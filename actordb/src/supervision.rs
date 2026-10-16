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

use futures::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use ractor::rpc::CallResult;
use ractor::{
    Actor, ActorCell, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort, SupervisionEvent,
};

use crate::actor::{
    now_ms, ExpiryPass, FragmentState, HlcClock, KvActor, KvArgs, KvMessage, PoolConfig,
    VersionedValue,
};
use crate::shard::FragmentId;
use crate::store::KvStore;
use crate::wire::check_versioned_value_budget;

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
    /// Close child admission, drain accepted work, materialize expirations,
    /// and flush the fragment while retaining its state for a failed retry.
    Quiesce {
        reply: RpcReplyPort<Result<(), FragmentShutdownError>>,
    },
}

/// A fragment could not reach its final local durability fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentShutdownError {
    /// A child actor could not be drained and joined.
    Actor(String),
    /// The fragment's retained persistence batch could not be committed.
    Storage(String),
}

impl std::fmt::Display for FragmentShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "actor drain failed: {error}"),
            Self::Storage(error) => write!(f, "final persistence flush failed: {error}"),
        }
    }
}

impl std::error::Error for FragmentShutdownError {}

/// A fragment could not construct its initial state from durable storage.
///
/// This concrete type is preserved inside ractor's `SpawnErr::StartupFailed`
/// so Node startup can distinguish durable-data failures from actor/framework
/// startup failures without parsing an error string.
#[derive(Debug)]
pub(crate) struct FragmentStartupStorageError(String);

impl FragmentStartupStorageError {
    fn boxed(error: impl std::fmt::Display) -> ActorProcessingErr {
        Box::new(Self(error.to_string()))
    }
}

impl std::fmt::Display for FragmentStartupStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FragmentStartupStorageError {}

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
    lifecycle: SupervisorLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SupervisorLifecycle {
    Running,
    Quiescing,
    Quiesced,
    Flushed,
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
                for (k, v) in kvstore.load().map_err(FragmentStartupStorageError::boxed)? {
                    high_water = Some(
                        high_water
                            .map(|current| current.max(v.timestamp_ms))
                            .unwrap_or(v.timestamp_ms),
                    );
                    let key =
                        String::from_utf8(k.clone()).map_err(FragmentStartupStorageError::boxed)?;
                    let value = if v.is_expired(now) {
                        let tombstone = v.expiry_tombstone();
                        expiry_updates.push((k, tombstone.clone()));
                        tombstone
                    } else {
                        v
                    };
                    check_versioned_value_budget(&key, &value).map_err(|error| {
                        let prefix: String = key.chars().take(64).collect();
                        FragmentStartupStorageError::boxed(format!(
                            "persisted record (key bytes {}, prefix {prefix:?}) cannot be replicated: {error}",
                            key.len()
                        ))
                    })?;
                    map.insert(key, value);
                }
                // Persist boot-time expiry conversion before the fragment
                // serves reads or sync. Otherwise a crash could reload the
                // live value and later resurrect an older replica's data.
                if !expiry_updates.is_empty() {
                    kvstore
                        .write_batch(&expiry_updates)
                        .map_err(FragmentStartupStorageError::boxed)?;
                }
                Ok((map, high_water))
            }
            None => Ok((HashMap::new(), None)),
        }
    }

    fn bump_restart(&mut self, child: &str) {
        *self.restarts.entry(child.to_string()).or_insert(0) += 1;
    }

    fn request_drain(cell: &ActorCell) {
        if cell.get_status() < ActorStatus::Draining {
            let _ = cell.drain();
        }
    }

    async fn wait_for_exit(cell: &ActorCell) -> Result<(), FragmentShutdownError> {
        cell.wait(None)
            .await
            .map_err(|error| FragmentShutdownError::Actor(error.to_string()))
    }

    /// Make every fragment mutator terminal before the authoritative flush.
    /// This is deliberately retryable: a cancelled RPC waiter does not cancel
    /// the actor handler, and a failed flush leaves `shared` plus its retained
    /// in-flight batch owned by this still-running supervisor.
    async fn quiesce(&mut self) -> Result<(), FragmentShutdownError> {
        if self.lifecycle < SupervisorLifecycle::Quiesced {
            self.lifecycle = SupervisorLifecycle::Quiescing;

            // Close both periodic producers before awaiting either one. Their
            // already-admitted work drains before the dispatcher/worker fence.
            Self::request_drain(&self.cleaner.get_cell());
            Self::request_drain(&self.snapshotter.get_cell());
            Self::wait_for_exit(&self.cleaner.get_cell()).await?;
            Self::wait_for_exit(&self.snapshotter.get_cell()).await?;

            Self::request_drain(&self.dispatcher.get_cell());
            Self::wait_for_exit(&self.dispatcher.get_cell()).await?;
            self.lifecycle = SupervisorLifecycle::Quiesced;
        }

        if self.lifecycle < SupervisorLifecycle::Flushed {
            // Catch a TTL that crossed its deadline after the cleaner's last
            // sweep, then explicitly retry any batch retained by a failed
            // dispatcher/post-stop or snapshot flush.
            self.shared
                .expire_values(now_ms())
                .await
                .map_err(|error| FragmentShutdownError::Storage(error.to_string()))?;
            self.shared
                .flush()
                .await
                .map_err(|error| FragmentShutdownError::Storage(error.to_string()))?;
            self.lifecycle = SupervisorLifecycle::Flushed;
        }
        Ok(())
    }
}

#[derive(Default)]
struct SupervisorStartupChildren {
    cells: Vec<ActorCell>,
}

impl SupervisorStartupChildren {
    fn track(&mut self, cell: ActorCell) {
        self.cells.push(cell);
    }

    async fn rollback(&mut self) {
        for cell in &self.cells {
            let _ = cell.drain();
        }
        for cell in self.cells.drain(..) {
            let _ = cell.wait(None).await;
        }
    }

    fn disarm(&mut self) {
        self.cells.clear();
    }
}

impl Drop for SupervisorStartupChildren {
    fn drop(&mut self) {
        // The ordinary error and panic paths await `rollback`. This fallback
        // closes admission if the surrounding startup future itself is
        // cancelled while a child is already running.
        for cell in &self.cells {
            let _ = cell.drain();
        }
    }
}

impl FragmentSupervisor {
    async fn build_state(
        myself: ActorRef<SupervisorMessage>,
        args: SupervisorArgs,
        children: &mut SupervisorStartupChildren,
    ) -> Result<SupervisorState, ActorProcessingErr> {
        let (cache, persisted_high_water) = SupervisorState::load_cache(&args.store).await?;
        if let Some(timestamp_ms) = persisted_high_water {
            args.hlc
                .lock()
                .await
                .seed_persisted(timestamp_ms)
                .map_err(FragmentStartupStorageError::boxed)?;
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
        children.track(dispatcher.get_cell());

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
        children.track(cleaner.get_cell());

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
        children.track(snapshotter.get_cell());

        Ok(SupervisorState {
            fragment: args.fragment,
            dispatcher,
            cleaner,
            snapshotter,
            shared,
            args,
            restarts: HashMap::new(),
            lifecycle: SupervisorLifecycle::Running,
        })
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
        let mut children = SupervisorStartupChildren::default();
        match AssertUnwindSafe(Self::build_state(myself, args, &mut children))
            .catch_unwind()
            .await
        {
            Ok(Ok(state)) => {
                children.disarm();
                Ok(state)
            }
            Ok(Err(error)) => {
                children.rollback().await;
                Err(error)
            }
            Err(panic) => {
                children.rollback().await;
                std::panic::resume_unwind(panic)
            }
        }
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Explicit Node shutdown calls this before stopping the supervisor and
        // receives the exact result. This fallback preserves ordering for
        // startup rollback and best-effort drop paths too.
        state
            .quiesce()
            .await
            .map_err(|error| ActorProcessingErr::from(error.to_string()))
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
            SupervisorMessage::Quiesce { reply } => {
                let result = state.quiesce().await;
                let _ = reply.send(result);
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
        // Planned child drains use normal termination events. Never turn one
        // into a restart, especially once the parent begins draining: ractor
        // rejects linking a new child to a draining parent.
        if state.lifecycle != SupervisorLifecycle::Running
            || myself.get_status() >= ActorStatus::Draining
        {
            return Ok(());
        }
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
#[non_exhaustive]
pub enum CleanerMessage {
    /// Perform a full sweep for expired keys now. Explicit manual messages
    /// are not covered by the automatic ticker's admission bound.
    Sweep,
    /// Internal, coalesced automatic pass. Direct callers use `Sweep`.
    #[doc(hidden)]
    AutomaticSweep(MaintenanceTrigger),
}

/// Opaque ownership of one admitted automatic maintenance job.
///
/// Public only because maintenance messages are public. The unique lease
/// moves through cleaner continuations; manual messages cannot release it.
#[doc(hidden)]
pub struct MaintenanceTrigger {
    pending: Arc<AtomicBool>,
}

impl Drop for MaintenanceTrigger {
    fn drop(&mut self) {
        self.pending.store(false, Ordering::Release);
    }
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
    ticker: TickerTask,
    pass: Option<ExpiryPass>,
}

struct TickerTask(Option<JoinHandle<()>>);

impl TickerTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    async fn stop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for TickerTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// At most one automatic job is queued or executing, including every page
/// of a cleaner pass. Skip alone would still enqueue ticks behind a stall.
fn coalesced_ticker<M: ractor::Message>(
    myself: ActorRef<M>,
    interval: Duration,
    immediate: bool,
    message: fn(MaintenanceTrigger) -> M,
) -> TickerTask {
    let start = tokio::time::Instant::now();
    let mut ticker =
        tokio::time::interval_at(if immediate { start } else { start + interval }, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let pending = Arc::new(AtomicBool::new(false));
    TickerTask::new(tokio::spawn(async move {
        loop {
            ticker.tick().await;
            if myself.get_status() >= ActorStatus::Draining {
                break;
            }
            if pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let trigger = MaintenanceTrigger {
                pending: pending.clone(),
            };
            if myself.send_message(message(trigger)).is_err() {
                break;
            }
        }
    }))
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
        let ticker = coalesced_ticker(myself, args.interval, true, CleanerMessage::AutomaticSweep);
        Ok(CleanerState {
            shared: args.shared,
            ticker,
            pass: None,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        state.ticker.stop().await;
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CleanerMessage::Sweep => {
                let now = now_ms();
                state.shared.expire_values(now).await?;
                Ok(())
            }
            CleanerMessage::AutomaticSweep(trigger) => {
                if myself.get_status() >= ActorStatus::Draining {
                    state.pass = None;
                    return Ok(());
                }
                let pass = state.pass.get_or_insert_with(|| ExpiryPass::new(now_ms()));
                let page = state.shared.expire_values_page(pass).await?;
                tracing::trace!(
                    visited = page.visited,
                    converted = page.converted,
                    work_bytes = page.work_bytes,
                    done = page.done,
                    "automatic expiry page"
                );
                if page.done || myself.get_status() >= ActorStatus::Draining {
                    state.pass = None;
                } else {
                    // A ready ractor mailbox can otherwise consume several
                    // pages before Tokio's cooperative budget expires. Yield
                    // with no data locks held and retain the unique admission
                    // lease; this is an opportunity, not a priority guarantee.
                    tokio::task::yield_now().await;
                    if myself.get_status() >= ActorStatus::Draining {
                        state.pass = None;
                    } else {
                        // Move, rather than opening a new slot between pages.
                        // Failed typed sends drop the lease with the message.
                        let _ = myself.send_message(CleanerMessage::AutomaticSweep(trigger));
                    }
                }
                Ok(())
            }
        }
    }
}

/// Messages for the [`Snapshotter`].
#[non_exhaustive]
pub enum SnapshotterMessage {
    /// Flush the write buffer to the store now. Explicit manual messages
    /// are not covered by the automatic ticker's admission bound.
    Snap,
    /// Internal, coalesced automatic flush. Direct callers use `Snap`.
    #[doc(hidden)]
    AutomaticSnap(MaintenanceTrigger),
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
    ticker: TickerTask,
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
        let ticker = coalesced_ticker(
            myself,
            args.interval,
            false,
            SnapshotterMessage::AutomaticSnap,
        );
        Ok(SnapshotterState {
            shared: args.shared,
            ticker,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        state.ticker.stop().await;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SnapshotterMessage::Snap => state.shared.flush().await,
            SnapshotterMessage::AutomaticSnap(_trigger) => state.shared.flush().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{ApplyOutcome, KvWorker, KvWorkerArgs, ReplicatedOp};
    use crate::store::StoreError;
    use ractor::registry::where_is;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex as StdMutex};

    #[derive(Default)]
    struct GatedStore {
        entered: AtomicBool,
        release: (StdMutex<bool>, Condvar),
        entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    }

    impl GatedStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    impl KvStore for GatedStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            drop(released);
            let mut entries = self.entries.lock().unwrap();
            for (key, value) in writes {
                entries.insert(key.clone(), value.clone());
            }
            Ok(())
        }
    }

    struct ReleaseMaintenanceStore(Arc<GatedStore>);

    impl Drop for ReleaseMaintenanceStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct DropSignalStore(Option<std::sync::mpsc::Sender<()>>);

    impl KvStore for DropSignalStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    impl Drop for DropSignalStore {
        fn drop(&mut self) {
            if let Some(dropped) = self.0.take() {
                let _ = dropped.send(());
            }
        }
    }

    enum TickTestMessage {
        Automatic(MaintenanceTrigger),
        Barrier(RpcReplyPort<()>),
    }

    struct TickTestActor;

    struct TickTestState {
        entered: Arc<std::sync::atomic::AtomicUsize>,
        gate: Arc<tokio::sync::Notify>,
        pending: Arc<StdMutex<Option<Arc<AtomicBool>>>>,
    }

    impl Actor for TickTestActor {
        type Msg = TickTestMessage;
        type State = TickTestState;
        type Arguments = TickTestState;

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                TickTestMessage::Automatic(trigger) => {
                    *state.pending.lock().unwrap() = Some(trigger.pending.clone());
                    state.entered.fetch_add(1, Ordering::SeqCst);
                    state.gate.notified().await;
                    drop(trigger);
                }
                TickTestMessage::Barrier(reply) => {
                    let _ = reply.send(());
                }
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn automatic_maintenance_ticks_coalesce_stalls_and_release_on_terminal_paths() {
        for immediate in [true, false] {
            let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Notify::new());
            let pending = Arc::new(StdMutex::new(None));
            let (actor, handle) = TickTestActor::spawn(
                None,
                TickTestActor,
                TickTestState {
                    entered: entered.clone(),
                    gate: gate.clone(),
                    pending: pending.clone(),
                },
            )
            .await
            .unwrap();
            let mut ticker = coalesced_ticker(
                actor.clone(),
                Duration::from_millis(2),
                immediate,
                TickTestMessage::Automatic,
            );
            tokio::time::advance(Duration::from_millis(2)).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while entered.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let lease = pending.lock().unwrap().clone().unwrap();
            assert!(lease.load(Ordering::Acquire));
            // Deterministic interval ticks while the actor handler is stalled. Stopping
            // the producer before release makes the following FIFO barrier
            // prove there is no hidden queued automatic backlog.
            for _ in 0..20 {
                tokio::time::advance(Duration::from_millis(2)).await;
                tokio::task::yield_now().await;
            }
            ticker.stop().await;
            gate.notify_one();
            assert!(matches!(
                actor
                    .call(TickTestMessage::Barrier, Some(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                CallResult::Success(())
            ));
            assert_eq!(entered.load(Ordering::SeqCst), 1);
            assert!(!lease.load(Ordering::Acquire));

            // Cancellation of an active handler retains no abandoned latch.
            lease.store(true, Ordering::Release);
            actor
                .send_message(TickTestMessage::Automatic(MaintenanceTrigger {
                    pending: lease.clone(),
                }))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while entered.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            actor.kill();
            handle.await.unwrap();
            assert!(!lease.load(Ordering::Acquire));
            lease.store(true, Ordering::Release);
            let result = actor.send_message(TickTestMessage::Automatic(MaintenanceTrigger {
                pending: lease.clone(),
            }));
            assert!(result.is_err());
            drop(result); // failed typed send owns the rejected message
            assert!(!lease.load(Ordering::Acquire));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn automatic_cleaner_stops_pages_on_drain_but_final_quiesce_sweeps_all() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseMaintenanceStore(store.clone());
        let values = (0..600)
            .map(|i| {
                let mut value = VersionedValue::new(vec![1], 10, "writer".into());
                value.expires_at_ms = Some(1);
                (format!("k{i:04}"), value)
            })
            .collect();
        let shared = Arc::new(FragmentState::new(
            values,
            Some(store.clone()),
            1,
            Duration::from_secs(3600),
        ));
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };
        let args = SupervisorArgs {
            fragment: FragmentId(0),
            worker_prefix: "paged-final-quiesce".into(),
            store: Some(store.clone()),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool.clone(),
            flush_batch_size: 1,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_millis(2),
            snapshot_interval: Duration::from_secs(3600),
            tombstone_gc_age: None,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        };
        let (dispatcher, _) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: args.fragment,
                worker_prefix: args.worker_prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let (cleaner, _) = Cleaner::spawn(
            None,
            Cleaner,
            CleanerArgs {
                fragment: args.fragment,
                shared: shared.clone(),
                interval: args.clean_interval,
                tombstone_gc_age: None,
                reset_senders: args.reset_senders.clone(),
            },
        )
        .await
        .unwrap();
        let (snapshotter, _) = Snapshotter::spawn(
            None,
            Snapshotter,
            SnapshotterArgs {
                shared: shared.clone(),
                interval: args.snapshot_interval,
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Do not depend on the timer driver while a synchronous store blocks
        // a Tokio worker. Coalesced repeated ticks are proven separately with
        // a paused-clock async gate; this gate proves drain and final fencing.
        cleaner.drain().unwrap();
        store.release();
        tokio::time::timeout(Duration::from_secs(2), cleaner.wait(None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store.entries.lock().unwrap().len(),
            256,
            "drain admitted another automatic page"
        );
        let mut state = SupervisorState {
            fragment: args.fragment,
            dispatcher,
            cleaner,
            snapshotter,
            shared,
            args,
            restarts: HashMap::new(),
            lifecycle: SupervisorLifecycle::Running,
        };
        state.quiesce().await.unwrap();
        let entries = store.entries.lock().unwrap();
        assert_eq!(entries.len(), 600);
        assert!(entries.values().all(|value| value.is_tombstone
            && value.timestamp_ms == 10
            && value.expires_at_ms == Some(1)));
        assert!(state.restarts.is_empty());
    }

    #[tokio::test]
    async fn automatic_cleaner_completes_multiple_pages_and_manual_snap_flushes_markers() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseMaintenanceStore(store.clone());
        store.release();
        let values = (0..600)
            .map(|i| {
                let mut value = VersionedValue::new(vec![1], 10, "writer".into());
                value.expires_at_ms = Some(1);
                (format!("k{i:04}"), value)
            })
            .collect();
        let shared = Arc::new(FragmentState::new(
            values,
            Some(store.clone()),
            usize::MAX,
            Duration::from_secs(3600),
        ));
        let (cleaner, _) = Cleaner::spawn(
            None,
            Cleaner,
            CleanerArgs {
                fragment: FragmentId(0),
                shared: shared.clone(),
                interval: Duration::from_secs(3600),
                tombstone_gc_age: None,
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while shared.snapshot_with_revision().await.1 != 600 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            store.entries.lock().unwrap().is_empty(),
            "idle buffer flushed before explicit snapshot"
        );
        assert_eq!(shared.expire_values(now_ms()).await.unwrap(), 0);
        cleaner.send_message(CleanerMessage::Sweep).unwrap();
        cleaner.drain().unwrap();
        cleaner.wait(None).await.unwrap();
        let (snapshotter, _) = Snapshotter::spawn(
            None,
            Snapshotter,
            SnapshotterArgs {
                shared,
                interval: Duration::from_secs(3600),
            },
        )
        .await
        .unwrap();
        snapshotter.send_message(SnapshotterMessage::Snap).unwrap();
        snapshotter.drain().unwrap();
        snapshotter.wait(None).await.unwrap();
        let entries = store.entries.lock().unwrap();
        assert_eq!(entries.len(), 600);
        assert!(entries
            .iter()
            .all(|(_, value)| value.is_tombstone && value.expires_at_ms == Some(1)));
    }

    #[test]
    fn fragment_shutdown_errors_identify_the_failed_phase() {
        assert_eq!(
            FragmentShutdownError::Actor("worker stopped".to_string()).to_string(),
            "actor drain failed: worker stopped"
        );
        assert_eq!(
            FragmentShutdownError::Storage("disk full".to_string()).to_string(),
            "final persistence flush failed: disk full"
        );
    }

    #[tokio::test]
    async fn failed_supervisor_start_joins_partial_worker_tree() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let collision_name = "failed-supervisor-default-worker-0".to_string();
        let (collision, _handle) = KvWorker::spawn(
            Some(collision_name.clone()),
            KvWorker,
            KvWorkerArgs {
                shared,
                pending: None,
            },
        )
        .await
        .unwrap();
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };

        let result = FragmentSupervisor::spawn(
            None,
            FragmentSupervisor,
            SupervisorArgs {
                fragment: FragmentId(0),
                worker_prefix: "failed-supervisor".to_string(),
                store: None,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
                flush_batch_size: 100,
                flush_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                tombstone_gc_age: None,
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .await;

        assert!(result.is_err());
        assert!(
            where_is("failed-supervisor-high-worker-0").is_none(),
            "failed dispatcher startup left an earlier worker running"
        );
        assert_eq!(
            where_is(collision_name).map(|cell| cell.get_id()),
            Some(collision.get_id()),
            "startup rollback stopped an actor it did not own"
        );
        collision.stop(None);
        let _ = collision.wait(None).await;
    }

    #[tokio::test]
    async fn failed_snapshotter_start_joins_completed_supervisor_children() {
        let (store_dropped, store_drop) = std::sync::mpsc::channel();
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };
        let result = FragmentSupervisor::spawn(
            None,
            FragmentSupervisor,
            SupervisorArgs {
                fragment: FragmentId(0),
                worker_prefix: "failed-snapshotter".to_string(),
                store: Some(Arc::new(DropSignalStore(Some(store_dropped)))),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
                flush_batch_size: 100,
                flush_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::ZERO,
                tombstone_gc_age: None,
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .await;

        assert!(result.is_err());
        assert!(
            store_drop.try_recv().is_ok(),
            "supervisor startup returned before completed children released their shared store"
        );
        for pool_name in ["high", "default", "bg"] {
            assert!(
                where_is(format!("failed-snapshotter-{pool_name}-worker-0")).is_none(),
                "failed supervisor startup left {pool_name} worker registered"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quiesce_drains_preaccepted_worker_messages_before_flushing() {
        let store = Arc::new(GatedStore::default());
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };
        let (supervisor, _handle) = FragmentSupervisor::spawn(
            None,
            FragmentSupervisor,
            SupervisorArgs {
                fragment: FragmentId(0),
                worker_prefix: "quiesce-drain-test".to_string(),
                store: Some(store.clone()),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
                flush_batch_size: 1,
                flush_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                tombstone_gc_age: None,
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .await
        .unwrap();
        let dispatcher = match supervisor
            .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
            .await
            .unwrap()
        {
            CallResult::Success(dispatcher) => dispatcher,
            other => panic!("dispatcher lookup failed: {other:?}"),
        };

        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::ApplyOp {
                op: ReplicatedOp {
                    key: "same-key".to_string(),
                    value: VersionedValue::new(b"first".to_vec(), 1, "test".to_string()),
                    seq: 1,
                    boot_id: 1,
                },
                priority: crate::actor::OpPriority::Default,
                is_replication: false,
                reply: Some(first_tx.into()),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (second_tx, second_rx) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::ApplyOp {
                op: ReplicatedOp {
                    key: "same-key".to_string(),
                    value: VersionedValue::new(b"second".to_vec(), 2, "test".to_string()),
                    seq: 2,
                    boot_id: 1,
                },
                priority: crate::actor::OpPriority::Default,
                is_replication: false,
                reply: Some(second_tx.into()),
            })
            .unwrap();

        let quiesce_supervisor = supervisor.clone();
        let quiesce = tokio::spawn(async move {
            quiesce_supervisor
                .call(|reply| SupervisorMessage::Quiesce { reply }, None)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !quiesce.is_finished(),
            "quiesce skipped blocked worker work"
        );

        store.release();
        assert_eq!(first_rx.await.unwrap().unwrap(), ApplyOutcome::Applied);
        assert_eq!(second_rx.await.unwrap().unwrap(), ApplyOutcome::Applied);
        match quiesce.await.unwrap().unwrap() {
            CallResult::Success(Ok(())) => {}
            other => panic!("quiesce failed: {other:?}"),
        }
        let health = match supervisor
            .call(|reply| SupervisorMessage::GetHealth { reply }, None)
            .await
            .unwrap()
        {
            CallResult::Success(health) => health,
            other => panic!("post-quiesce health failed: {other:?}"),
        };
        assert!(
            health.restarts.is_empty(),
            "planned child drains were counted or restarted"
        );
        let stopped_dispatcher = match supervisor
            .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
            .await
            .unwrap()
        {
            CallResult::Success(dispatcher) => dispatcher,
            other => panic!("post-quiesce dispatcher lookup failed: {other:?}"),
        };
        assert_eq!(stopped_dispatcher.get_id(), dispatcher.get_id());
        assert_eq!(stopped_dispatcher.get_status(), ActorStatus::Stopped);
        assert_eq!(
            store
                .entries
                .lock()
                .unwrap()
                .get(b"same-key".as_slice())
                .map(|value| value.value.clone()),
            Some(b"second".to_vec())
        );

        supervisor.stop(None);
        supervisor.wait(None).await.unwrap();
    }
}
