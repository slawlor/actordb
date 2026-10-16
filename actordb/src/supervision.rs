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
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use ractor::rpc::CallResult;
use ractor::{
    Actor, ActorCell, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort, SupervisionEvent,
};

use crate::actor::{
    now_ms, FragmentState, HlcClock, KvActor, KvArgs, KvMessage, PoolConfig, VersionedValue,
};
use crate::shard::FragmentId;
use crate::store::KvStore;
use crate::wire::check_versioned_value_budget;

/// Local, non-atomic fragment snapshot served by the [`FragmentSupervisor`].
/// A responding dispatcher and linked Running workers do not prove worker
/// responsiveness, cluster readiness, replication convergence, or durability.
#[derive(Debug, Clone)]
pub struct FragmentHealth {
    /// Which fragment this reports on.
    pub fragment: FragmentId,
    /// Linked dispatch-pool workers observed Running (high, default, background).
    pub worker_count: usize,
    /// Names of those exact dispatcher-owned Running worker actors.
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
    /// Get a local health snapshot. Missing/nonresponding dispatcher evidence
    /// closes the reply port rather than fabricating zero shed counters.
    /// The dispatcher probe uses this RPC's timeout, or five seconds when the
    /// caller supplies none. A queued expired/abandoned request is skipped.
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

async fn probe_health(
    dispatcher: &ActorRef<KvMessage>,
    args: &SupervisorArgs,
    restarts: &HashMap<String, u64>,
    timeout: Duration,
) -> Option<FragmentHealth> {
    if dispatcher.get_status() != ActorStatus::Running {
        return None;
    }
    let shed_counts = match dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, Some(timeout))
        .await
    {
        Ok(CallResult::Success(counts)) => counts,
        _ => return None,
    };
    if dispatcher.get_status() != ActorStatus::Running {
        return None;
    }
    // Inspect exact linked children, not the global name registry or configured
    // pool size. This is a status snapshot, not a probe of worker responsiveness.
    let live_names: HashSet<_> = dispatcher
        .get_children()
        .into_iter()
        .filter(|child| child.get_status() == ActorStatus::Running)
        .filter_map(|child| child.get_name())
        .collect();
    let mut worker_names = Vec::new();
    for (pool_cfg, suffix) in [
        (&args.high_pool, "high"),
        (&args.default_pool, "default"),
        (&args.background_pool, "bg"),
    ] {
        for slot in 0..pool_cfg.workers.max(1) {
            let name = format!("{}-{suffix}-worker-{slot}", args.worker_prefix);
            if live_names.contains(&name) {
                worker_names.push(name);
            }
        }
    }
    Some(FragmentHealth {
        fragment: args.fragment,
        worker_count: worker_names.len(),
        worker_names,
        restarts: restarts.clone(),
        last_reaped: 0,
        shed_counts,
    })
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
                if reply.is_closed() {
                    return Ok(());
                }
                let timeout = reply.get_timeout().unwrap_or(Duration::from_secs(5));
                if let Some(health) =
                    probe_health(&state.dispatcher, &state.args, &state.restarts, timeout).await
                {
                    let _ = reply.send(health);
                }
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
    ticker: TickerTask,
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
        let ticker = tokio::spawn(async move {
            loop {
                ticker.tick().await;
                if myself.send_message(CleanerMessage::Sweep).is_err() {
                    break;
                }
            }
        });
        Ok(CleanerState {
            shared: args.shared,
            ticker: TickerTask::new(ticker),
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
        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + args.interval, args.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let ticker = tokio::spawn(async move {
            loop {
                ticker.tick().await;
                if myself.send_message(SnapshotterMessage::Snap).is_err() {
                    break;
                }
            }
        });
        Ok(SnapshotterState {
            shared: args.shared,
            ticker: TickerTask::new(ticker),
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

    enum HealthDispatcherMode {
        Reply,
        DropReply,
        Block(Arc<tokio::sync::Notify>),
    }

    struct HealthDispatcher;

    impl Actor for HealthDispatcher {
        type Msg = KvMessage;
        type State = HealthDispatcherMode;
        type Arguments = HealthDispatcherMode;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if let KvMessage::GetShedCounts { reply } = message {
                match state {
                    HealthDispatcherMode::Reply => {}
                    HealthDispatcherMode::DropReply => return Ok(()),
                    HealthDispatcherMode::Block(release) => release.notified().await,
                }
                let _ = reply.send((7, 11, 13));
            }
            Ok(())
        }

        async fn handle_supervisor_evt(
            &self,
            _myself: ActorRef<Self::Msg>,
            _message: SupervisionEvent,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            // Keep a stopped child absent; this probe fixture intentionally
            // does not restart workers or terminate itself on child exit.
            Ok(())
        }
    }

    fn health_args(prefix: &str) -> SupervisorArgs {
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };
        SupervisorArgs {
            fragment: FragmentId(0),
            worker_prefix: prefix.to_string(),
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
        }
    }

    #[tokio::test]
    async fn health_counts_only_exact_owned_running_worker_names() {
        let args = health_args("health-owned-workers");
        let (dispatcher, _) =
            HealthDispatcher::spawn(None, HealthDispatcher, HealthDispatcherMode::Reply)
                .await
                .unwrap();
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let mut children = Vec::new();
        for suffix in ["high", "default", "bg"] {
            let worker_args = KvWorkerArgs {
                shared: shared.clone(),
                pending: None,
            };
            let name = Some(format!("{}-{suffix}-worker-0", args.worker_prefix));
            let (child, _) = if suffix == "default" {
                // Same configured name, but not owned by this dispatcher.
                KvWorker::spawn(name, KvWorker, worker_args).await.unwrap()
            } else {
                KvWorker::spawn_linked(name, KvWorker, worker_args, dispatcher.get_cell())
                    .await
                    .unwrap()
            };
            children.push(child);
        }
        children[2].stop(None);
        children[2].wait(None).await.unwrap();
        let restarts = HashMap::from([("dispatcher".to_string(), 3)]);
        let health = probe_health(&dispatcher, &args, &restarts, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(health.worker_count, 1);
        assert_eq!(
            health.worker_names,
            vec!["health-owned-workers-high-worker-0"]
        );
        assert_eq!(health.shed_counts, (7, 11, 13));
        assert_eq!(health.restarts, restarts);
        for child in children {
            child.stop(None);
            child.wait(None).await.unwrap();
        }
        dispatcher.stop(None);
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn health_dispatcher_missing_reply_stopped_and_blocked_are_unavailable() {
        let args = health_args("health-unavailable");
        let restarts = HashMap::new();
        let (missing_reply, _) =
            HealthDispatcher::spawn(None, HealthDispatcher, HealthDispatcherMode::DropReply)
                .await
                .unwrap();
        assert!(
            probe_health(&missing_reply, &args, &restarts, Duration::from_secs(1))
                .await
                .is_none()
        );
        missing_reply.stop(None);
        missing_reply.wait(None).await.unwrap();
        assert!(
            probe_health(&missing_reply, &args, &restarts, Duration::from_secs(1))
                .await
                .is_none()
        );

        let release = Arc::new(tokio::sync::Notify::new());
        let (blocked, _) = HealthDispatcher::spawn(
            None,
            HealthDispatcher,
            HealthDispatcherMode::Block(release.clone()),
        )
        .await
        .unwrap();
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            probe_health(&blocked, &args, &restarts, Duration::from_millis(20)),
        )
        .await
        .unwrap()
        .is_none());
        release.notify_one();
        blocked.stop(None);
        blocked.wait(None).await.unwrap();
    }

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

    struct ReleaseStore(Arc<GatedStore>);

    impl Drop for ReleaseStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn health_closed_ports_skip_and_blocked_dispatcher_does_not_pin_supervisor() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let mut args = health_args("health-blocked-real-dispatcher");
        args.store = Some(store.clone());
        args.flush_batch_size = 1;
        let (supervisor, _) = FragmentSupervisor::spawn(None, FragmentSupervisor, args)
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
        let (applied, mut apply_result) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::ApplyOp {
                op: ReplicatedOp {
                    key: "blocked".to_string(),
                    value: VersionedValue::new(b"value".to_vec(), 1, "writer".to_string()),
                    seq: 1,
                    boot_id: 1,
                },
                priority: crate::actor::OpPriority::Default,
                is_replication: false,
                reply: Some(applied.into()),
            })
            .unwrap();
        crate::common_test::periodic_async_check(
            || async { store.entered.load(Ordering::SeqCst) },
            Duration::from_secs(2),
        )
        .await;
        let (flushed, flush_result) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Flush {
                reply: flushed.into(),
            })
            .unwrap();

        // The dispatcher is held behind the store fence. Abandoned health
        // requests must not start an unnecessary five-second nested probe.
        let (abandoned, receiver) = tokio::sync::oneshot::channel();
        drop(receiver);
        supervisor
            .send_message(SupervisorMessage::GetHealth {
                reply: abandoned.into(),
            })
            .unwrap();
        assert!(matches!(
            supervisor
                .call(
                    |reply| SupervisorMessage::GetDispatcher { reply },
                    Some(Duration::from_millis(200)),
                )
                .await
                .unwrap(),
            CallResult::Success(_)
        ));
        assert!(matches!(
            supervisor
                .call(
                    |reply| SupervisorMessage::GetHealth { reply },
                    Some(Duration::from_millis(20)),
                )
                .await
                .unwrap(),
            CallResult::Timeout | CallResult::SenderError
        ));
        // Inner health timeout uses the caller's budget; the supervisor can
        // answer again while the actual dispatcher/store is still blocked.
        assert!(matches!(
            supervisor
                .call(
                    |reply| SupervisorMessage::GetDispatcher { reply },
                    Some(Duration::from_millis(200)),
                )
                .await
                .unwrap(),
            CallResult::Success(_)
        ));
        assert!(matches!(
            apply_result.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        store.release();
        assert_eq!(apply_result.await.unwrap().unwrap(), ApplyOutcome::Applied);
        assert!(flush_result.await.unwrap().is_ok());
        match supervisor
            .call(|reply| SupervisorMessage::Quiesce { reply }, None)
            .await
            .unwrap()
        {
            CallResult::Success(Ok(())) => {}
            other => panic!("quiesce failed: {other:?}"),
        }
        supervisor.stop(None);
        supervisor.wait(None).await.unwrap();
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
        let _release = ReleaseStore(store.clone());
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
        let health = match supervisor
            .call(|reply| SupervisorMessage::GetHealth { reply }, None)
            .await
            .unwrap()
        {
            CallResult::Success(health) => health,
            other => panic!("running health failed: {other:?}"),
        };
        assert_eq!(health.worker_count, 3);
        assert!(health.restarts.is_empty());

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
        let health = supervisor
            .call(|reply| SupervisorMessage::GetHealth { reply }, None)
            .await
            .unwrap();
        assert!(matches!(health, CallResult::SenderError));
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
        assert!(stopped_dispatcher.get_children().is_empty());
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
