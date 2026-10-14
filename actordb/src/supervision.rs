// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Per-fragment supervision.
//!
//! Every fragment runs under a [`FragmentSupervisor`](crate::supervision::FragmentSupervisor),
//! a ractor supervisor owning the fragment's background subsystem:
//!
//! - the [`KvActor`](crate::actor::KvActor) dispatcher and its worker pool
//!   (Phase 3), which self-supervises worker restarts;
//! - the [`Cleaner`](crate::supervision::Cleaner), which reaps TTL-expired keys on an interval;
//! - the [`Snapshotter`](crate::supervision::Snapshotter), which periodically flushes the write
//!   buffer so the durable store holds a fresh snapshot.
//!
//! A dead child is restarted in place without touching sibling fragments.
//! The supervisor also answers [`FragmentHealth`](crate::supervision::FragmentHealth) queries, feeding the
//! observability layer and (later) the Phase 8 placement controller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::actor::{now_ms, FragmentState, KvActor, KvArgs, KvMessage, VersionedValue};
use crate::shard::FragmentId;
use crate::store::KvStore;

/// Per-fragment health snapshot, served by the [`FragmentSupervisor`].
#[derive(Debug, Clone)]
pub struct FragmentHealth {
    /// Which fragment this reports on.
    pub fragment: FragmentId,
    /// Workers currently in the dispatch pool.
    pub worker_count: usize,
    /// Registered names of the dispatch pool workers.
    pub worker_names: Vec<String>,
    /// Restart counts per child (`dispatcher`, `cleaner`, `snapshotter`,
    /// `worker-<slot>`).
    pub restarts: HashMap<String, u64>,
    /// Keys reaped by the cleaner on its last sweep.
    pub last_reaped: usize,
}

/// Messages handled by the [`FragmentSupervisor`].
pub enum SupervisorMessage {
    /// Get the dispatcher's ref (for the node's fragment router).
    GetDispatcher {
        reply: RpcReplyPort<ActorRef<KvMessage>>,
    },
    /// Get the current health snapshot.
    GetHealth { reply: RpcReplyPort<FragmentHealth> },
}

/// Arguments to spawn a [`FragmentSupervisor`].
#[derive(Clone)]
pub struct SupervisorArgs {
    /// Which fragment this supervisor owns.
    pub fragment: FragmentId,
    /// Prefix for worker actor names (unique per supervisor).
    pub worker_prefix: String,
    /// Durable storage behind the cache. `None` = purely in-memory.
    pub store: Option<Arc<dyn KvStore>>,
    /// Worker actors in the dispatch pool.
    pub worker_count: usize,
    /// Flush the write buffer after this many buffered ops.
    pub flush_batch_size: usize,
    /// Flush the write buffer after this long, even if not full.
    pub flush_interval: Duration,
    /// How often the cleaner sweeps for expired keys.
    pub clean_interval: Duration,
    /// How often the snapshotter flushes to the store.
    pub snapshot_interval: Duration,
    /// How long tombstones are kept before garbage collection.
    pub tombstone_gc_age: Duration,
}

/// The per-fragment supervisor: owns the dispatcher, cleaner and
/// snapshotter, restarts dead children in place, and reports health.
pub struct FragmentSupervisor;

pub struct SupervisorState {
    fragment: FragmentId,
    worker_prefix: String,
    worker_count: usize,
    dispatcher: ActorRef<KvMessage>,
    cleaner: ActorRef<CleanerMessage>,
    snapshotter: ActorRef<SnapshotterMessage>,
    shared: Arc<FragmentState>,
    args: SupervisorArgs,
    restarts: HashMap<String, u64>,
    last_reaped: usize,
}

impl SupervisorState {
    /// Load the fragment's cache from the store. A corrupt store fails
    /// boot loudly rather than silently dropping data.
    async fn load_cache(
        store: &Option<Arc<dyn KvStore>>,
    ) -> Result<HashMap<String, VersionedValue>, ActorProcessingErr> {
        match store {
            Some(kvstore) => {
                let mut map = HashMap::new();
                for (k, v) in kvstore
                    .load()
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?
                {
                    let key = String::from_utf8(k)
                        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                    // Drop already-expired values at boot; the cleaner
                    // would reap them on its first sweep anyway.
                    if !v.is_expired(now_ms()) {
                        map.insert(key, v);
                    }
                }
                Ok(map)
            }
            None => Ok(HashMap::new()),
        }
    }

    fn bump_restart(&mut self, child: &str) {
        *self.restarts.entry(child.to_string()).or_insert(0) += 1;
    }
}

impl Actor for FragmentSupervisor {
    type Msg = SupervisorMessage;
    type State = SupervisorState;
    type Arguments = SupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let cache = SupervisorState::load_cache(&args.store).await?;
        let shared = Arc::new(FragmentState::new(
            cache,
            args.store.clone(),
            args.flush_batch_size,
            args.flush_interval,
        ));
        let cell = myself.get_cell();

        let (dispatcher, _h) = KvActor::spawn_linked(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: args.fragment,
                worker_prefix: args.worker_prefix.clone(),
                worker_count: args.worker_count,
            },
            cell.clone(),
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;

        let (cleaner, _h) = Cleaner::spawn_linked(
            None,
            Cleaner,
            CleanerArgs {
                shared: shared.clone(),
                interval: args.clean_interval,
                tombstone_gc_age: args.tombstone_gc_age,
            },
            cell.clone(),
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;

        let (snapshotter, _h) = Snapshotter::spawn_linked(
            None,
            Snapshotter,
            SnapshotterArgs {
                shared: shared.clone(),
                interval: args.snapshot_interval,
            },
            cell,
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;

        Ok(SupervisorState {
            fragment: args.fragment,
            worker_prefix: args.worker_prefix.clone(),
            worker_count: args.worker_count.max(1),
            dispatcher,
            cleaner,
            snapshotter,
            shared,
            args,
            restarts: HashMap::new(),
            last_reaped: 0,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Stop children; the dispatcher drains its pool and flushes.
        let _ = state.dispatcher.stop_and_wait(None, None).await;
        let _ = state.cleaner.stop_and_wait(None, None).await;
        let _ = state.snapshotter.stop_and_wait(None, None).await;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisorMessage::GetDispatcher { reply } => {
                let _ = reply.send(state.dispatcher.clone());
                Ok(())
            }
            SupervisorMessage::GetHealth { reply } => {
                let worker_names = (0..state.worker_count)
                    .map(|slot| format!("{}-worker-{slot}", state.worker_prefix))
                    .collect();
                let _ = reply.send(FragmentHealth {
                    fragment: state.fragment,
                    worker_count: state.worker_count,
                    worker_names,
                    restarts: state.restarts.clone(),
                    last_reaped: state.last_reaped,
                });
                Ok(())
            }
        }
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let dead = match &message {
            SupervisionEvent::ActorFailed(cell, _) => Some(cell.clone()),
            SupervisionEvent::ActorTerminated(cell, _, _) => Some(cell.clone()),
            _ => None,
        };
        if let Some(cell) = dead {
            let cell_id = cell.get_id();
            let myself_cell: ActorCell = myself.get_cell();
            if cell_id == state.dispatcher.get_id() {
                let (dispatcher, _h) = KvActor::spawn_linked(
                    None,
                    KvActor,
                    KvArgs {
                        shared: state.shared.clone(),
                        fragment: state.fragment,
                        worker_prefix: state.args.worker_prefix.clone(),
                        worker_count: state.args.worker_count,
                    },
                    myself_cell,
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                state.dispatcher = dispatcher;
                state.bump_restart("dispatcher");
            } else if cell_id == state.cleaner.get_id() {
                let (cleaner, _h) = Cleaner::spawn_linked(
                    None,
                    Cleaner,
                    CleanerArgs {
                        shared: state.shared.clone(),
                        interval: state.args.clean_interval,
                        tombstone_gc_age: state.args.tombstone_gc_age,
                    },
                    myself_cell,
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                state.cleaner = cleaner;
                state.bump_restart("cleaner");
            } else if cell_id == state.snapshotter.get_id() {
                let (snapshotter, _h) = Snapshotter::spawn_linked(
                    None,
                    Snapshotter,
                    SnapshotterArgs {
                        shared: state.shared.clone(),
                        interval: state.args.snapshot_interval,
                    },
                    myself_cell,
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                state.snapshotter = snapshotter;
                state.bump_restart("snapshotter");
            }
            // Unknown child (e.g. a worker reporting to the dispatcher,
            // not us): ignore.
        }
        Ok(())
    }
}

/// Messages for the [`Cleaner`].
pub enum CleanerMessage {
    /// Sweep for expired keys now.
    Sweep,
}

/// Background actor: periodically reaps TTL-expired keys from the
/// fragment's shared state. Reads already hide expired values; the
/// cleaner is memory hygiene. Tombstone aging plugs in here once
/// tombstones land (#13).
pub struct Cleaner;

#[derive(Clone)]
pub struct CleanerArgs {
    pub shared: Arc<FragmentState>,
    pub interval: Duration,
    /// Tombstones older than this are garbage-collected.
    pub tombstone_gc_age: Duration,
}

pub struct CleanerState {
    shared: Arc<FragmentState>,
    tombstone_gc_age: Duration,
}

impl Actor for Cleaner {
    type Msg = CleanerMessage;
    type State = CleanerState;
    type Arguments = CleanerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut ticker = tokio::time::interval(args.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                if myself.send_message(CleanerMessage::Sweep).is_err() {
                    break;
                }
            }
        });
        Ok(CleanerState {
            shared: args.shared,
            tombstone_gc_age: args.tombstone_gc_age,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CleanerMessage::Sweep => {
                let now = now_ms();
                state.shared.remove_expired(now).await;
                state
                    .shared
                    .gc_tombstones(now, state.tombstone_gc_age.as_millis() as u64)
                    .await;
                Ok(())
            }
        }
    }
}

/// Messages for the [`Snapshotter`].
pub enum SnapshotterMessage {
    /// Flush the write buffer to the store now.
    Snap,
}

/// Background actor: periodically flushes the fragment's write buffer,
/// so the durable store holds a fresh snapshot even under a trickle of
/// writes that never fills the batch.
pub struct Snapshotter;

#[derive(Clone)]
pub struct SnapshotterArgs {
    pub shared: Arc<FragmentState>,
    pub interval: Duration,
}

pub struct SnapshotterState {
    shared: Arc<FragmentState>,
}

impl Actor for Snapshotter {
    type Msg = SnapshotterMessage;
    type State = SnapshotterState;
    type Arguments = SnapshotterArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut ticker = tokio::time::interval(args.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                if myself.send_message(SnapshotterMessage::Snap).is_err() {
                    break;
                }
            }
        });
        Ok(SnapshotterState {
            shared: args.shared,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SnapshotterMessage::Snap => state.shared.flush().await,
        }
    }
}
