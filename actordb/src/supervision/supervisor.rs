// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Fragment child ownership, startup rollback, health and quiescence.

use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use ractor::rpc::CallResult;
use ractor::{
    Actor, ActorCell, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort, SupervisionEvent,
};

use super::{
    Cleaner, CleanerArgs, CleanerMessage, Snapshotter, SnapshotterArgs, SnapshotterMessage,
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
/// The fragment, worker prefix and Node-wide HLC remain required. Omitted
/// tuning settings match [`crate::node::NodeConfig`] defaults.
#[derive(Clone, bon::Builder)]
pub struct SupervisorArgs {
    /// Which fragment this supervisor owns.
    pub fragment: FragmentId,
    /// Prefix for worker actor names (unique per supervisor).
    #[builder(into)]
    pub worker_prefix: String,
    /// Durable storage behind the cache. `None` = purely in-memory.
    pub store: Option<Arc<dyn KvStore>>,
    /// Node-wide clock seeded from the highest version loaded by every
    /// fragment before that fragment starts serving writes.
    pub hlc: Arc<Mutex<HlcClock>>,
    /// Worker pool configs by priority.
    #[builder(default = PoolConfig::default())]
    pub default_pool: PoolConfig,
    #[builder(default = PoolConfig::high_priority_defaults())]
    pub high_pool: PoolConfig,
    #[builder(default = PoolConfig::background_defaults())]
    pub background_pool: PoolConfig,
    /// Flush the write buffer after this many buffered ops.
    #[builder(default = DEFAULT_FLUSH_BATCH_SIZE)]
    pub flush_batch_size: usize,
    /// A new write triggers a flush after this long, even if not full.
    /// The snapshotter separately flushes idle fragments.
    #[builder(default = DEFAULT_FLUSH_INTERVAL)]
    pub flush_interval: Duration,
    /// How often the cleaner sweeps for expired keys.
    #[builder(default = DEFAULT_CLEAN_INTERVAL)]
    pub clean_interval: Duration,
    /// How often the snapshotter flushes to the store.
    #[builder(default = DEFAULT_SNAPSHOT_INTERVAL)]
    pub snapshot_interval: Duration,
    /// Legacy compatibility field. Ignored; node startup rejects `Some`.
    pub tombstone_gc_age: Option<Duration>,
    /// Legacy compatibility field. The removed collector no longer consults
    /// replication reset state.
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
}

pub(crate) const DEFAULT_FLUSH_BATCH_SIZE: usize = 1000;
pub(crate) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const DEFAULT_CLEAN_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);

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
            KvArgs::builder()
                .shared(shared.clone())
                .fragment(args.fragment)
                .worker_prefix(args.worker_prefix.clone())
                .default_pool(args.default_pool.clone())
                .high_pool(args.high_pool.clone())
                .background_pool(args.background_pool.clone())
                .build(),
            cell.clone(),
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        children.track(dispatcher.get_cell());

        let (cleaner, _h) = Cleaner::spawn_linked(
            None,
            Cleaner,
            CleanerArgs::builder()
                .fragment(args.fragment)
                .shared(shared.clone())
                .interval(args.clean_interval)
                .maybe_tombstone_gc_age(args.tombstone_gc_age)
                .reset_senders(args.reset_senders.clone())
                .build(),
            cell.clone(),
        )
        .await
        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        children.track(cleaner.get_cell());

        let (snapshotter, _h) = Snapshotter::spawn_linked(
            None,
            Snapshotter,
            SnapshotterArgs::builder()
                .shared(shared.clone())
                .interval(args.snapshot_interval)
                .build(),
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
                    KvArgs::builder()
                        .shared(state.shared.clone())
                        .fragment(state.fragment)
                        .worker_prefix(state.args.worker_prefix.clone())
                        .default_pool(state.args.default_pool.clone())
                        .high_pool(state.args.high_pool.clone())
                        .background_pool(state.args.background_pool.clone())
                        .build(),
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
                    CleanerArgs::builder()
                        .fragment(state.args.fragment)
                        .shared(state.shared.clone())
                        .interval(state.args.clean_interval)
                        .maybe_tombstone_gc_age(state.args.tombstone_gc_age)
                        .reset_senders(state.args.reset_senders.clone())
                        .build(),
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
                    SnapshotterArgs::builder()
                        .shared(state.shared.clone())
                        .interval(state.args.snapshot_interval)
                        .build(),
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

#[cfg(test)]
#[path = "tests/supervisor.rs"]
mod tests;
