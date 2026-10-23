// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Per-fragment lifecycle, child ownership, startup rollback, and health.

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
use crate::ttl_expiry::{ExpiryPublisher, PreparedExpiry};
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
/// tuning settings match [`crate::node::NodeConfig`] defaults. Use
/// [`SupervisorArgs::builder`] for construction: Node-owned callback attachment
/// is private, while fragment, worker prefix, and clock remain caller inputs.
#[derive(Clone, bon::Builder)]
pub struct SupervisorArgs {
    /// Which fragment this supervisor owns.
    pub fragment: FragmentId,
    /// Node-owned callback ingress; deliberately absent from the public builder.
    #[builder(skip = None)]
    expiry_publisher: Option<ExpiryPublisher>,
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
    /// Authoritative persistence backlog admission limits.
    #[builder(default)]
    pub persistence: crate::store::PersistenceConfig,
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
}

impl SupervisorArgs {
    /// Attach the Node-owned ingress after public builder construction.
    pub(crate) fn with_expiry_publisher(mut self, publisher: Option<ExpiryPublisher>) -> Self {
        self.expiry_publisher = publisher;
        self
    }
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

struct LoadedCache {
    values: HashMap<String, VersionedValue>,
    high_water: Option<actordb_protocol::RecordVersion>,
    expirations: Vec<PreparedExpiry>,
}

impl SupervisorState {
    /// Load the fragment's cache from the store. A corrupt store fails
    /// boot loudly rather than silently dropping data.
    async fn load_cache(
        store: &Option<Arc<dyn KvStore>>,
        fragment: FragmentId,
        publisher: &Option<ExpiryPublisher>,
        executor: &Arc<crate::store::executor::StorageExecutor>,
    ) -> Result<LoadedCache, ActorProcessingErr> {
        match store {
            Some(kvstore) => {
                let mut map = HashMap::new();
                let mut expiry_updates = Vec::new();
                let mut expirations = HashMap::new();
                let mut high_water: Option<actordb_protocol::RecordVersion> = None;
                let backend = kvstore.clone();
                let loaded = executor
                    .run(move || backend.load())
                    .await
                    .map_err(FragmentStartupStorageError::boxed)?;
                let now = now_ms();
                for (index, (raw_key, v)) in loaded.into_iter().enumerate() {
                    // Table partitions add a durable-only prefix around the
                    // logical namespace. The actor cache and replication
                    // protocol retain the logical key so table routing and
                    // co-partitioned fragment hashing remain deterministic.
                    let k = crate::table_key::strip_storage_prefix(&raw_key)
                        .unwrap_or_else(|| raw_key.clone());
                    high_water = Some(
                        high_water
                            .map(|current| current.max(v.version()))
                            .unwrap_or(v.version()),
                    );
                    let key =
                        String::from_utf8(k.clone()).map_err(FragmentStartupStorageError::boxed)?;
                    if map.contains_key(&key) {
                        let prefix: String = key.chars().take(64).collect();
                        return Err(FragmentStartupStorageError::boxed(format!(
                            "persisted dataset contains duplicate key ({} bytes, prefix {prefix:?}); load must return one winner per key", key.len()
                        )));
                    }
                    // Validate original records before expiry strips their payload.
                    check_versioned_value_budget(&key, &v)
                        .map_err(FragmentStartupStorageError::boxed)?;
                    let (value, previous_value) = if v.is_expired(now) {
                        let tombstone = v.expiry_tombstone();
                        expiry_updates.push((raw_key, tombstone.clone()));
                        (tombstone, if publisher.is_some() { Some(v) } else { None })
                    } else {
                        (v, None)
                    };
                    check_versioned_value_budget(&key, &value).map_err(|error| {
                        let prefix: String = key.chars().take(64).collect();
                        FragmentStartupStorageError::boxed(format!(
                            "persisted record (key bytes {}, prefix {prefix:?}) cannot be replicated: {error}",
                            key.len()
                        ))
                    })?;
                    if let (Some(publisher), Some(previous_value)) = (publisher, previous_value) {
                        if let Some(expiration) =
                            publisher.prepare_startup(fragment, &key, previous_value)
                        {
                            expirations.insert(key.clone(), (index, expiration));
                        }
                    }
                    map.insert(key, value);
                }
                // Persist boot-time expiry conversion before the fragment
                // serves reads or sync. Otherwise a crash could reload the
                // live value and later resurrect an older replica's data.
                if !expiry_updates.is_empty() {
                    let backend = kvstore.clone();
                    executor
                        .run(move || backend.write_batch(&expiry_updates))
                        .await
                        .map_err(FragmentStartupStorageError::boxed)?;
                }
                let mut expirations: Vec<_> = expirations.into_values().collect();
                expirations.sort_unstable_by_key(|(index, _)| *index);
                Ok(LoadedCache {
                    values: map,
                    high_water,
                    expirations: expirations.into_iter().map(|(_, event)| event).collect(),
                })
            }
            None => Ok(LoadedCache {
                values: HashMap::new(),
                high_water: None,
                expirations: Vec::new(),
            }),
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
        args.persistence
            .validate()
            .map_err(FragmentStartupStorageError::boxed)?;
        let loaded = SupervisorState::load_cache(
            &args.store,
            args.fragment,
            &args.expiry_publisher,
            &args.persistence.executor(),
        )
        .await?;
        if let Some(timestamp_ms) = loaded.high_water {
            args.hlc
                .lock()
                .await
                .seed_version(timestamp_ms)
                .map_err(FragmentStartupStorageError::boxed)?;
        }
        let shared = Arc::new(
            FragmentState::new(
                loaded.values,
                args.store.clone(),
                args.flush_batch_size,
                args.flush_interval,
            )
            .with_persistence_config(args.persistence.clone())
            .map_err(FragmentStartupStorageError::boxed)?
            .enable_partitions()
            .map_err(FragmentStartupStorageError::boxed)?
            .with_expiry_publisher(args.fragment, args.expiry_publisher.clone()),
        );
        // Publish only after boot-time normalization was validated and persisted
        // and the HLC accepted its version floor. Publish before starting cleaner
        // children, so their subsequent materializations cannot overtake these.
        for expiration in loaded.expirations {
            expiration.publish();
        }
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
mod tests {
    //! Supervisor regression groups retain access to their private runtime owner.

    use super::*;
    use crate::actor::maintenance_test_support::{
        GatedStore, ReleaseMaintenanceStore, ReleaseStore,
    };
    use crate::actor::{ApplyOutcome, KvWorker, KvWorkerArgs, ReplicatedOp};
    use crate::store::StoreError;
    use ractor::registry::where_is;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex as StdMutex;

    async fn wait_until_running(actors: &[ActorCell]) {
        // ractor spawn completes pre_start and linking, not the background
        // post_start transition. Fence exact physical actors before treating
        // their non-atomic health snapshot as a fully started test fixture.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = true;
                for actor in actors {
                    let status = actor.get_status();
                    assert!(
                        status < ActorStatus::Draining,
                        "actor {:?} stopped before becoming ready: {status:?}",
                        actor.get_name()
                    );
                    ready &= status == ActorStatus::Running;
                }
                if ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actor fixture did not finish startup");
    }

    async fn running_dispatcher(
        supervisor: &ActorRef<SupervisorMessage>,
        expected_workers: usize,
    ) -> ActorRef<KvMessage> {
        let dispatcher = match supervisor
            .call(
                |reply| SupervisorMessage::GetDispatcher { reply },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap()
        {
            CallResult::Success(dispatcher) => dispatcher,
            other => panic!("dispatcher lookup failed: {other:?}"),
        };
        let mut actors = dispatcher.get_children();
        assert_eq!(actors.len(), expected_workers);
        actors.push(dispatcher.get_cell());
        wait_until_running(&actors).await;
        dispatcher
    }

    mod lifecycle_configuration {
        //! Configured child startup, quiescence and real maintenance recovery.

        use super::*;

        #[tokio::test]
        async fn supervisor_starts_and_quiesces_owned_workers() {
            let (supervisor, handle) = FragmentSupervisor::spawn(
                None,
                FragmentSupervisor,
                SupervisorArgs::builder()
                    .fragment(FragmentId(0))
                    .worker_prefix("builder-owned-workers")
                    .hlc(Arc::new(Mutex::new(HlcClock::new())))
                    .build(),
            )
            .await
            .unwrap();
            let _dispatcher = running_dispatcher(&supervisor, 8).await;
            let CallResult::Success(health) = supervisor
                .call(
                    |reply| SupervisorMessage::GetHealth { reply },
                    Some(Duration::from_secs(2)),
                )
                .await
                .unwrap()
            else {
                panic!("builder defaults failed health probe")
            };
            assert_eq!(health.fragment, FragmentId(0));
            assert_eq!(health.worker_count, 8);
            assert_eq!(health.shed_counts, (0, 0, 0));
            assert!(matches!(
                supervisor
                    .call(
                        |reply| SupervisorMessage::Quiesce { reply },
                        Some(Duration::from_secs(2))
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(()))
            ));
            supervisor.drain().unwrap();
            handle.await.unwrap();
        }

        #[derive(Default)]
        struct RestartRecordingStore {
            entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
        }

        impl KvStore for RestartRecordingStore {
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self
                    .entries
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect())
            }

            fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                let mut entries = self.entries.lock().unwrap();
                for (key, value) in writes {
                    entries.insert(key.clone(), value.clone());
                }
                Ok(())
            }
        }

        struct StopRestartSupervisor(ActorCell);

        impl Drop for StopRestartSupervisor {
            fn drop(&mut self) {
                self.0.stop_children(None);
                self.0.kill();
            }
        }

        async fn restart_owned_maintenance_child<M: ractor::Message>(
            supervisor: &ActorRef<SupervisorMessage>,
            name: &str,
        ) -> ActorRef<M> {
            let original = supervisor
                .get_children()
                .into_iter()
                .find(|child| child.is_message_type_of::<M>() == Some(true))
                .unwrap();
            original.kill();
            tokio::time::timeout(Duration::from_secs(5), async {
                original.wait(None).await.unwrap();
                loop {
                    let replacement = supervisor.get_children().into_iter().find(|child| {
                        child.is_message_type_of::<M>() == Some(true)
                            && child.get_id() != original.get_id()
                            && child.get_status() == ActorStatus::Running
                    });
                    let health = supervisor
                        .call(
                            |reply| SupervisorMessage::GetHealth { reply },
                            Some(Duration::from_secs(2)),
                        )
                        .await
                        .unwrap();
                    if let (Some(replacement), CallResult::Success(health)) = (replacement, health)
                    {
                        if health.restarts.get(name) == Some(&1) {
                            return ActorRef::from(replacement);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("owned maintenance child did not restart")
        }

        #[tokio::test]
        async fn maintenance_restarts_preserve_shared_state_and_periodic_policy() {
            let store = Arc::new(RestartRecordingStore::default());
            let pool = |workers, queue_cap| {
                PoolConfig::builder()
                    .workers(workers)
                    .queue_cap(queue_cap)
                    .build()
            };
            let (supervisor, handle) = FragmentSupervisor::spawn(
                None,
                FragmentSupervisor,
                SupervisorArgs::builder()
                    .fragment(FragmentId(7))
                    .worker_prefix("builder-maintenance-restart")
                    .hlc(Arc::new(Mutex::new(HlcClock::new())))
                    .store(store.clone())
                    .high_pool(pool(3, 101))
                    .default_pool(pool(3, 103))
                    .background_pool(
                        PoolConfig::builder()
                            .workers(2)
                            .queue_cap(107)
                            .rate_limit_per_sec(7)
                            .build(),
                    )
                    .flush_batch_size(usize::MAX)
                    .flush_interval(Duration::from_secs(3600))
                    .clean_interval(Duration::from_millis(17))
                    .snapshot_interval(Duration::from_millis(11))
                    .build(),
            )
            .await
            .unwrap();
            let _stop = StopRestartSupervisor(supervisor.get_cell());
            let dispatcher = running_dispatcher(&supervisor, 8).await;
            let ordinary = VersionedValue::new(b"retained".to_vec(), 10, "writer".into());
            let apply = |key: &str, value: VersionedValue| ReplicatedOp {
                key: key.into(),
                value,
                seq: 1,
                boot_id: 1,
            };
            assert_eq!(
                dispatcher
                    .call(
                        |reply| KvMessage::ApplyOp {
                            op: apply("ordinary", ordinary.clone()),
                            priority: crate::actor::OpPriority::High,
                            is_replication: false,
                            reply: Some(reply)
                        },
                        Some(Duration::from_secs(2)),
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(ApplyOutcome::Applied))
            );

            // Identify the exact linked actor by its local message type, not a
            // global name, and join its old task before checking the replacement.
            let _cleaner =
                restart_owned_maintenance_child::<CleanerMessage>(&supervisor, "cleaner").await;
            let _snapshotter =
                restart_owned_maintenance_child::<SnapshotterMessage>(&supervisor, "snapshotter")
                    .await;
            let mut expiring = VersionedValue::new(b"temporary".to_vec(), 20, "writer".into());
            // Capture after both actors are running so their immediate startup
            // tick cannot already expire this value. The real wall clock, rather
            // than paused Tokio time, governs TTL; the store never blocks it.
            expiring.expires_at_ms = Some(now_ms() + 1000);
            let floor = expiring.expiry_tombstone();
            assert_eq!(
                dispatcher
                    .call(
                        |reply| KvMessage::ApplyOp {
                            op: apply("expiring", expiring.clone()),
                            priority: crate::actor::OpPriority::High,
                            is_replication: false,
                            reply: Some(reply)
                        },
                        Some(Duration::from_secs(2)),
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(ApplyOutcome::Applied))
            );

            // No manual Sweep/Snap and no write-trigger flush: both replacement
            // actors must retain their short configured intervals and shared cache
            // for the exact expiry floor to reach the original store within 5 s.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if store.entries.lock().unwrap().get(b"expiring".as_slice()) == Some(&floor) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("replacement maintenance actors lost state or periodic policy");
            assert_eq!(
                store.entries.lock().unwrap().get(b"ordinary".as_slice()),
                Some(&ordinary)
            );
            assert_eq!(
                dispatcher
                    .call(
                        |reply| KvMessage::Get {
                            key: "ordinary".into(),
                            priority: crate::actor::OpPriority::High,
                            reply
                        },
                        Some(Duration::from_secs(2)),
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(Some(ordinary)))
            );
            assert_eq!(
                dispatcher
                    .call(
                        |reply| KvMessage::Get {
                            key: "expiring".into(),
                            priority: crate::actor::OpPriority::High,
                            reply
                        },
                        Some(Duration::from_secs(2)),
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(None))
            );
            let CallResult::Success(health) = supervisor
                .call(
                    |reply| SupervisorMessage::GetHealth { reply },
                    Some(Duration::from_secs(2)),
                )
                .await
                .unwrap()
            else {
                panic!("restart health probe failed")
            };
            assert_eq!(health.fragment, FragmentId(7));
            assert_eq!(health.worker_count, 8);
            for (suffix, expected) in [("high", 3), ("default", 3), ("bg", 2)] {
                assert_eq!(
                    health
                        .worker_names
                        .iter()
                        .filter(|name| name.contains(&format!("-{suffix}-worker-")))
                        .count(),
                    expected
                );
            }
            assert_eq!(
                health.restarts,
                HashMap::from([("cleaner".into(), 1), ("snapshotter".into(), 1)])
            );
            assert!(matches!(
                supervisor
                    .call(
                        |reply| SupervisorMessage::Quiesce { reply },
                        Some(Duration::from_secs(2)),
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(()))
            ));
            assert!(
                supervisor.get_children().is_empty(),
                "planned quiesce restarted a child"
            );
            supervisor.drain().unwrap();
            handle.await.unwrap();
        }
    }
    mod health {
        //! Exact worker ownership and bounded local health evidence.

        use super::*;

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
                expiry_publisher: None,
                fragment: FragmentId(0),
                worker_prefix: prefix.to_string(),
                store: None,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
                flush_batch_size: 100,
                persistence: Default::default(),
                flush_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
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
            let mut actors: Vec<_> = children.iter().map(|child| child.get_cell()).collect();
            actors.push(dispatcher.get_cell());
            wait_until_running(&actors).await;
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
            wait_until_running(&[missing_reply.get_cell()]).await;
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
            wait_until_running(&[blocked.get_cell()]).await;
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
            let dispatcher = running_dispatcher(&supervisor, 3).await;
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
    }
    mod lifecycle {
        //! Startup rollback and authoritative post-quiescence flush ordering.

        use super::*;

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
                expiry_publisher: None,
                fragment: FragmentId(0),
                worker_prefix: "paged-final-quiesce".into(),
                store: Some(store.clone()),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool.clone(),
                flush_batch_size: 1,
                persistence: Default::default(),
                flush_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_millis(2),
                snapshot_interval: Duration::from_secs(3600),
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
                    expiry_publisher: None,
                    fragment: FragmentId(0),
                    worker_prefix: "failed-supervisor".to_string(),
                    store: None,
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    default_pool: pool.clone(),
                    high_pool: pool.clone(),
                    background_pool: pool,
                    flush_batch_size: 100,
                    persistence: Default::default(),
                    flush_interval: Duration::from_secs(3600),
                    clean_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::from_secs(3600),
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
                    expiry_publisher: None,
                    fragment: FragmentId(0),
                    worker_prefix: "failed-snapshotter".to_string(),
                    store: Some(Arc::new(DropSignalStore(Some(store_dropped)))),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    default_pool: pool.clone(),
                    high_pool: pool.clone(),
                    background_pool: pool,
                    flush_batch_size: 100,
                    persistence: Default::default(),
                    flush_interval: Duration::from_secs(3600),
                    clean_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::ZERO,
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
                    expiry_publisher: None,
                    fragment: FragmentId(0),
                    worker_prefix: "quiesce-drain-test".to_string(),
                    store: Some(store.clone()),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    default_pool: pool.clone(),
                    high_pool: pool.clone(),
                    background_pool: pool,
                    flush_batch_size: 1,
                    persistence: Default::default(),
                    flush_interval: Duration::from_secs(3600),
                    clean_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::from_secs(3600),
                },
            )
            .await
            .unwrap();
            let dispatcher = running_dispatcher(&supervisor, 3).await;
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
            // Draining is transient once the dispatcher empties its mailbox.
            // Stopping is stable here: post_stop must join the store-blocked
            // worker before the dispatcher can finish and become Stopped.
            tokio::time::timeout(Duration::from_secs(5), async {
                while dispatcher.get_status() < ActorStatus::Stopping {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("quiesce did not reach the dispatcher worker-join fence");
            assert_eq!(dispatcher.get_status(), ActorStatus::Stopping);
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
}

#[cfg(test)]
mod persistence_validation_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct StartupStore(AtomicUsize);
    impl KvStore for StartupStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, crate::store::StoreError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn write_batch(
            &self,
            _: &[(Vec<u8>, VersionedValue)],
        ) -> Result<(), crate::store::StoreError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn invalid_storage_slots_fail_before_supervisor_store_operations() {
        let store = Arc::new(StartupStore(AtomicUsize::new(0)));
        let args = SupervisorArgs::builder()
            .fragment(FragmentId(0))
            .worker_prefix("invalid-storage-slots")
            .hlc(Arc::new(Mutex::new(HlcClock::new())))
            .store(store.clone() as Arc<dyn KvStore>)
            .persistence(
                crate::store::PersistenceConfig::builder()
                    .storage_concurrency(0)
                    .build(),
            )
            .build();
        let started = tokio::time::timeout(
            Duration::from_secs(1),
            FragmentSupervisor::spawn(None, FragmentSupervisor, args),
        )
        .await
        .expect("invalid startup waited for an unusable storage executor");
        if let Ok((actor, handle)) = started {
            actor.stop(None);
            let _ = handle.await;
            panic!("zero storage slots were accepted");
        }
        assert_eq!(store.0.load(Ordering::Relaxed), 0);
    }
}
