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
//! - the [`Cleaner`](crate::supervision::Cleaner), which materializes TTL-expiry tombstones on an interval;
//! - the [`Snapshotter`](crate::supervision::Snapshotter), which periodically flushes the write
//!   buffer so the durable store holds a fresh snapshot.
//!
//! A dead child is restarted in place without touching sibling fragments.
//! The supervisor also answers [`FragmentHealth`](crate::supervision::FragmentHealth) queries, feeding the
//! observability layer and (later) the Phase 8 placement controller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use ractor::rpc::CallResult;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::actor::{
    now_ms, FragmentState, HlcClock, KvActor, KvArgs, KvMessage, PoolConfig, VersionedValue,
};
use crate::shard::FragmentId;
use crate::store::KvStore;

/// Per-fragment health snapshot, served by the [`FragmentSupervisor`].
#[derive(Debug, Clone)]
pub struct FragmentHealth {
    /// Which fragment this reports on.
    pub fragment: FragmentId,
    /// Workers currently in the dispatch pools (high, default, background).
    pub worker_count: usize,
    /// Registered names of the dispatch pool workers.
    pub worker_names: Vec<String>,
    /// Restart counts per child (`dispatcher`, `cleaner`, `snapshotter`,
    /// `worker-<slot>`).
    pub restarts: HashMap<String, u64>,
    /// Legacy compatibility field. Always zero because the unsafe time-based
    /// tombstone collector has been removed.
    pub last_reaped: usize,
    /// Ops shed by backpressure, per pool (high, default, background).
    /// Explicit and counted, never silent (Phase 7).
    pub shed_counts: (u64, u64, u64),
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
    /// Node-wide clock seeded from the highest version loaded by every
    /// fragment before that fragment starts serving writes.
    pub hlc: Arc<Mutex<HlcClock>>,
    /// Worker pool configs by priority.
    pub default_pool: PoolConfig,
    pub high_pool: PoolConfig,
    pub background_pool: PoolConfig,
    /// Flush the write buffer after this many buffered ops.
    pub flush_batch_size: usize,
    /// A new write triggers a flush after this long, even if not full.
    /// The snapshotter separately flushes idle fragments.
    pub flush_interval: Duration,
    /// How often the cleaner sweeps for expired keys.
    pub clean_interval: Duration,
    /// How often the snapshotter flushes to the store.
    pub snapshot_interval: Duration,
    /// Legacy compatibility field. Ignored; node startup rejects `Some`.
    pub tombstone_gc_age: Option<Duration>,
    /// Legacy compatibility field. The removed collector no longer consults
    /// replication reset state.
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
}

/// The per-fragment supervisor: owns the dispatcher, cleaner and
/// snapshotter, restarts dead children in place, and reports health.
pub struct FragmentSupervisor;

pub struct SupervisorState {
    fragment: FragmentId,
    dispatcher: ActorRef<KvMessage>,
    cleaner: ActorRef<CleanerMessage>,
    snapshotter: ActorRef<SnapshotterMessage>,
    shared: Arc<FragmentState>,
    args: SupervisorArgs,
    restarts: HashMap<String, u64>,
}

impl SupervisorState {
    /// Load the fragment's cache from the store. A corrupt store fails
    /// boot loudly rather than silently dropping data.
    async fn load_cache(
        store: &Option<Arc<dyn KvStore>>,
    ) -> Result<(HashMap<String, VersionedValue>, Option<u64>), ActorProcessingErr> {
        match store {
            Some(kvstore) => {
                let mut map = HashMap::new();
                let mut expiry_updates = Vec::new();
                let mut high_water: Option<u64> = None;
                let now = now_ms();
                for (k, v) in kvstore
                    .load()
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?
                {
                    high_water = Some(
                        high_water
                            .map(|current| current.max(v.timestamp_ms))
                            .unwrap_or(v.timestamp_ms),
                    );
                    let key = String::from_utf8(k.clone())
                        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                    let value = if v.is_expired(now) {
                        let tombstone = v.expiry_tombstone();
                        expiry_updates.push((k, tombstone.clone()));
                        tombstone
                    } else {
                        v
                    };
                    map.insert(key, value);
                }
                // Persist boot-time expiry conversion before the fragment
                // serves reads or sync. Otherwise a crash could reload the
                // live value and later resurrect an older replica's data.
                if !expiry_updates.is_empty() {
                    kvstore
                        .write_batch(&expiry_updates)
                        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                }
                Ok((map, high_water))
            }
            None => Ok((HashMap::new(), None)),
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
        let (cache, persisted_high_water) = SupervisorState::load_cache(&args.store).await?;
        if let Some(timestamp_ms) = persisted_high_water {
            args.hlc
                .lock()
                .await
                .seed_persisted(timestamp_ms)
                .map_err(|error| ActorProcessingErr::from(error.to_string()))?;
        }
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
                default_pool: args.default_pool.clone(),
                high_pool: args.high_pool.clone(),
                background_pool: args.background_pool.clone(),
            },
            cell.clone(),
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;

        let (cleaner, _h) = Cleaner::spawn_linked(
            None,
            Cleaner,
            CleanerArgs {
                fragment: args.fragment,
                shared: shared.clone(),
                interval: args.clean_interval,
                tombstone_gc_age: args.tombstone_gc_age,
                reset_senders: args.reset_senders.clone(),
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
            dispatcher,
            cleaner,
            snapshotter,
            shared,
            args,
            restarts: HashMap::new(),
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
                // Worker names follow the dispatcher's naming:
                // {prefix}-{pool}-worker-{slot}.
                let mut worker_names = Vec::new();
                for (pool_cfg, suffix) in [
                    (&state.args.high_pool, "high"),
                    (&state.args.default_pool, "default"),
                    (&state.args.background_pool, "bg"),
                ] {
                    for slot in 0..pool_cfg.workers.max(1) {
                        worker_names.push(format!(
                            "{}-{suffix}-worker-{slot}",
                            state.args.worker_prefix
                        ));
                    }
                }
                // Query the dispatcher for per-pool shed counts.
                let shed_counts = match state
                    .dispatcher
                    .call(|reply| KvMessage::GetShedCounts { reply }, None)
                    .await
                {
                    Ok(CallResult::Success(counts)) => counts,
                    _ => (0, 0, 0),
                };
                let _ = reply.send(FragmentHealth {
                    fragment: state.fragment,
                    worker_count: worker_names.len(),
                    worker_names,
                    restarts: state.restarts.clone(),
                    last_reaped: 0,
                    shed_counts,
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
                        default_pool: state.args.default_pool.clone(),
                        high_pool: state.args.high_pool.clone(),
                        background_pool: state.args.background_pool.clone(),
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
                        fragment: state.args.fragment,
                        shared: state.shared.clone(),
                        interval: state.args.clean_interval,
                        tombstone_gc_age: state.args.tombstone_gc_age,
                        reset_senders: state.args.reset_senders.clone(),
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

/// Background actor: periodically converts TTL-expired values into
/// versioned tombstones. Reads already hide expired values, but retaining the
/// version floor prevents older replicas from resurrecting them.
pub struct Cleaner;

#[derive(Clone)]
pub struct CleanerArgs {
    /// Legacy compatibility field. The cleaner no longer uses fragment ids.
    pub fragment: FragmentId,
    pub shared: Arc<FragmentState>,
    pub interval: Duration,
    /// Legacy compatibility field. Ignored; tombstones are retained.
    pub tombstone_gc_age: Option<Duration>,
    /// Legacy compatibility field. Ignored because time-based collection was
    /// removed rather than guarded by transient sender state.
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
}

pub struct CleanerState {
    shared: Arc<FragmentState>,
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
                state.shared.expire_values(now).await?;
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
        // `interval()` yields its first tick immediately. Start at one full
        // interval so a write arriving just after startup is not flushed by a
        // delayed "initial" tick.
        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + args.interval, args.interval);
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
