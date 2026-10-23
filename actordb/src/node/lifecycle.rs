// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Operation admission, fragment routing, startup ownership, and shutdown.
//!
//! Shutdown first closes operation admission and freezes placement, then drains
//! admitted work/replication before stopping transport and fragment actors.
//! Startup guards retain partially started supervisors and exclusive store
//! attachment until join; cancellation must not release that ownership early.
//! Registry locks do not span supervisor RPCs, startup waits, or storage work.
//! The shutdown-progress mutex serializes the phase owner across joined work;
//! cancellation preserves that progress for the next shutdown caller.

use super::*;

/// Phase 5 senders, keyed by (fragment, peer node id).
pub(super) type SenderMap = Arc<Mutex<HashMap<(FragmentId, String), NodeSender>>>;

/// Sender state reports, keyed by (fragment, peer node id).
pub(super) type SenderReportMap =
    Arc<Mutex<HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>>>;

/// Per-fragment fence used while a durable handoff has detached its exact
/// sender set but still needs the local actor to prove that no later mutation
/// was admitted. The sender-map lock orders fence installation against sender
/// creation; the synchronous registry then keeps unrelated fragments free of
/// the handoff's joins and actor RPC.
#[derive(Default)]
pub(super) struct SenderFinalizationRegistry {
    pub(super) next_token: AtomicU64,
    pub(super) fragments: StdMutex<HashMap<FragmentId, u64>>,
}

impl SenderFinalizationRegistry {
    pub(super) fn seal(self: &Arc<Self>, fragment: FragmentId) -> Option<SenderFinalizationGuard> {
        let mut fragments = self.fragments.lock().unwrap();
        if fragments.contains_key(&fragment) {
            return None;
        }
        let token = self
            .next_token
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        fragments.insert(fragment, token);
        Some(SenderFinalizationGuard {
            fragment,
            token,
            registry: self.clone(),
        })
    }

    pub(super) fn is_sealed(&self, fragment: FragmentId) -> bool {
        self.fragments.lock().unwrap().contains_key(&fragment)
    }
}

pub(super) struct SenderFinalizationGuard {
    pub(super) fragment: FragmentId,
    pub(super) token: u64,
    pub(super) registry: Arc<SenderFinalizationRegistry>,
}

impl Drop for SenderFinalizationGuard {
    fn drop(&mut self) {
        let mut fragments = self.registry.fragments.lock().unwrap();
        if fragments.get(&self.fragment) == Some(&self.token) {
            fragments.remove(&self.fragment);
        }
    }
}

/// Join ownership for sender tasks detached from the active sender map by a
/// cancellable connection or placement task. Normal cleanup removes an entry
/// after joining it; cancellation leaves the entry here for Node shutdown.
#[derive(Default)]
pub(super) struct RetiringSenderRegistry {
    pub(super) senders: StdMutex<Vec<NodeSender>>,
}

impl RetiringSenderRegistry {
    pub(super) fn retire(self: &Arc<Self>, sender: NodeSender) -> RetiringSender {
        {
            let mut retiring = self.senders.lock().unwrap();
            if !retiring.iter().any(|current| current.same_task(&sender)) {
                retiring.push(sender.clone());
            }
        }
        sender.request_stop();
        RetiringSender {
            sender,
            registry: self.clone(),
        }
    }

    pub(super) fn remove(&self, sender: &NodeSender) {
        self.senders
            .lock()
            .unwrap()
            .retain(|current| !current.same_task(sender));
    }

    pub(super) fn snapshot(&self) -> Vec<NodeSender> {
        self.senders.lock().unwrap().clone()
    }

    pub(super) fn request_stop_all(&self) {
        for sender in self.senders.lock().unwrap().iter() {
            sender.request_stop();
        }
    }

    pub(super) fn clear(&self) {
        self.senders.lock().unwrap().clear();
    }
}

impl Drop for RetiringSenderRegistry {
    fn drop(&mut self) {
        for sender in self.senders.get_mut().unwrap().iter() {
            sender.request_stop();
        }
    }
}

pub(super) struct RetiringSender {
    pub(super) sender: NodeSender,
    pub(super) registry: Arc<RetiringSenderRegistry>,
}

impl RetiringSender {
    pub(super) async fn join(self) {
        self.sender.stop().await;
        self.registry.remove(&self.sender);
    }
}

pub(super) struct SenderEntryDeps<'a> {
    pub(super) config: &'a SenderConfig,
    pub(super) runtime: &'a SenderRuntimeOptions,
    pub(super) router: &'a FragmentRouter,
    pub(super) hlc: &'a Arc<Mutex<HlcClock>>,
    pub(super) senders: &'a SenderMap,
    pub(super) finalizing_senders: &'a Arc<SenderFinalizationRegistry>,
    pub(super) retiring_senders: &'a Arc<RetiringSenderRegistry>,
    pub(super) reports: &'a SenderReportMap,
    pub(super) peer_failures: &'a PeerFailureSignals,
    pub(super) operation_gate: &'a Arc<OperationGate>,
    pub(super) allow_quiescing: bool,
}

/// Atomically install the one replication sender allowed to own a
/// `(fragment, peer)` session. Placement and connection setup can race; a
/// check-then-spawn sequence would orphan one sender after it had already
/// emitted SessionStart or accepted mutations.
pub(super) async fn ensure_sender_entry(
    fragment: FragmentId,
    peer: &str,
    deps: SenderEntryDeps<'_>,
) {
    let key = (fragment, peer.to_string());
    let mut senders_guard = deps.senders.lock().await;
    if !deps.allow_quiescing && !deps.operation_gate.is_running() {
        return;
    }
    if deps.finalizing_senders.is_sealed(fragment) {
        return;
    }
    // A connection or placement tick can have queued this ensure call while a
    // loss transition committed. The drainer removes the router entry before
    // releasing the sender-map lock; re-check here so the stale ensure cannot
    // recreate a sender whose snapshot source has already been detached.
    if deps.router.route_for_fragment(fragment).is_none() {
        return;
    }
    let (replacing_stopped_sender, retiring_sender) = if let Some(sender) = senders_guard.get(&key)
    {
        if !sender.is_closed() {
            return;
        }
        let retiring = deps.retiring_senders.retire(sender.clone());
        senders_guard.remove(&key);
        deps.reports.lock().await.remove(&key);
        (true, Some(retiring))
    } else {
        (false, None)
    };
    let report = Arc::new(Mutex::new(SenderStateReport {
        state: "disconnected".to_string(),
        session_id: 0,
        buffered: 0,
        retained_bytes: 0,
        aggregate_retained_bytes: 0,
        in_flight: 0,
        last_ack_durability: None,
        ack_durability_mismatch: false,
        reconcile_rounds: 0,
    }));
    let peer_failures = deps.peer_failures.clone();
    let expected_peer = peer.to_string();
    let runtime = deps
        .runtime
        .clone()
        .with_queue_failure_handler(Arc::new(move |failed_tx| {
            peer_failures.signal(expected_peer.clone(), failed_tx);
        }));
    let sender = spawn_node_sender(
        fragment,
        peer.to_string(),
        deps.config.clone(),
        SenderDeps::builder()
            .router(deps.router.clone())
            .hlc(deps.hlc.clone())
            .state_report(report.clone())
            .build(),
        runtime,
    )
    .expect("node runtime options were validated before sender spawn");
    if replacing_stopped_sender {
        // The prior task may have died after accepting an already-applied
        // mutation. Start its replacement in snapshot recovery rather than
        // assuming the lost volatile queue was empty.
        sender.schedule_reset();
    }
    deps.reports.lock().await.insert(key.clone(), report);
    senders_guard.insert(key, sender);
    drop(senders_guard);
    if let Some(retiring) = retiring_sender {
        retiring.join().await;
    }
}

pub(super) const NODE_RUNNING: u8 = 0;

pub(super) const NODE_QUIESCING: u8 = 1;

pub(super) const NODE_STOPPED: u8 = 2;

/// One-way admission fence for public Node operations. Admission is a
/// lock-free check/increment/recheck sequence, so shutdown cannot miss a
/// racing operation and a cancelled operation releases its slot by RAII.
pub(super) struct OperationGate {
    pub(super) phase: AtomicU8,
    pub(super) active: AtomicUsize,
    pub(super) drained: Notify,
}

impl OperationGate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            phase: AtomicU8::new(NODE_RUNNING),
            active: AtomicUsize::new(0),
            drained: Notify::new(),
        })
    }

    pub(super) fn try_enter(self: &Arc<Self>) -> Result<OperationPermit, NodeError> {
        if self.phase.load(Ordering::SeqCst) != NODE_RUNNING {
            return Err(NodeError::ShuttingDown);
        }
        self.finish_enter_after_running_check()
    }

    pub(super) fn is_running(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == NODE_RUNNING
    }

    /// Complete admission after observing `NODE_RUNNING`. Shutdown may race
    /// that first observation, so publish the operation and then recheck the
    /// phase before allowing it to proceed.
    pub(super) fn finish_enter_after_running_check(
        self: &Arc<Self>,
    ) -> Result<OperationPermit, NodeError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.phase.load(Ordering::SeqCst) == NODE_RUNNING {
            return Ok(OperationPermit { gate: self.clone() });
        }
        self.release();
        Err(NodeError::ShuttingDown)
    }

    pub(super) fn begin_shutdown(&self) {
        let _ = self.phase.compare_exchange(
            NODE_RUNNING,
            NODE_QUIESCING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub(super) async fn wait_for_drain(&self) {
        loop {
            // Register before observing the count so the final release cannot
            // slip between the check and the wait.
            let drained = self.drained.notified();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            drained.await;
        }
    }

    pub(super) fn finish_shutdown(&self) {
        self.phase.store(NODE_STOPPED, Ordering::SeqCst);
        self.drained.notify_waiters();
    }

    pub(super) fn release(&self) {
        if self.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.drained.notify_waiters();
        }
    }
}

pub(super) struct OperationPermit {
    pub(super) gate: Arc<OperationGate>,
}

impl Drop for OperationPermit {
    fn drop(&mut self) {
        self.gate.release();
    }
}

/// Unique worker-name prefixes across supervisors in this process.
pub(super) static NEXT_WORKER_PREFIX: AtomicU64 = AtomicU64::new(0);

/// Routes keys to the fragment actors this node runs. Cloned into
/// every connection task so inbound ops land on the right fragment.
/// The actor map is shared and mutable: the Phase 8 placement driver
/// adds/removes fragment subtrees at runtime.
#[derive(Clone)]
pub struct FragmentRouter {
    pub(super) actors: Arc<RwLock<HashMap<FragmentId, FragmentRoute>>>,
    pub(super) next_generation: Arc<AtomicU64>,
    pub(super) fragment_count: usize,
    /// A mutation holds a shared lease from its serving decision through
    /// apply and replication fan-out. Placement revocation takes the exclusive
    /// lease before removing `serving`, closing the apply/enqueue teardown race.
    pub(super) mutation_gates: Arc<Vec<Arc<AsyncRwLock<()>>>>,
}

#[derive(Clone)]
pub(super) struct FragmentRoute {
    pub(super) generation: u64,
    pub(super) actor: ActorRef<KvMessage>,
}

/// Remove a newly published dynamic route if cancellation wins before its
/// dispatcher monitor is durably registered with the core task manager.
pub(super) struct RouteMonitorRegistrationGuard {
    pub(super) router: FragmentRouter,
    pub(super) fragment: FragmentId,
    pub(super) generation: u64,
    pub(super) armed: bool,
}

impl RouteMonitorRegistrationGuard {
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RouteMonitorRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.router
                .remove_if_generation(self.fragment, self.generation);
        }
    }
}

impl FragmentRouter {
    pub(crate) fn new(fragment_count: usize, actor_capacity: usize) -> Self {
        Self {
            actors: Arc::new(RwLock::new(HashMap::with_capacity(actor_capacity))),
            next_generation: Arc::new(AtomicU64::new(1)),
            fragment_count,
            mutation_gates: Arc::new(
                (0..fragment_count)
                    .map(|_| Arc::new(AsyncRwLock::new(())))
                    .collect(),
            ),
        }
    }

    pub(super) fn mutation_gate(&self, fragment: FragmentId) -> Arc<AsyncRwLock<()>> {
        self.mutation_gates
            .get(fragment.0 as usize)
            .expect("fragment was validated against fragment_count")
            .clone()
    }

    pub(super) fn route(&self, key: &str) -> Option<ActorRef<KvMessage>> {
        let frag = fragment_of(key, self.fragment_count);
        self.route_for_fragment(frag)
    }

    /// The actor running `fragment`, if this node runs it (includes
    /// fragments still bootstrapping under Phase 8).
    pub fn route_for_fragment(&self, fragment: FragmentId) -> Option<ActorRef<KvMessage>> {
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .map(|route| route.actor.clone())
    }

    pub(super) fn route_with_generation(
        &self,
        fragment: FragmentId,
    ) -> Option<(u64, ActorRef<KvMessage>)> {
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .map(|route| (route.generation, route.actor.clone()))
    }

    pub(crate) fn key_belongs_to(&self, fragment: FragmentId, key: &str) -> bool {
        fragment_of(key, self.fragment_count) == fragment
    }

    pub(super) fn fragment_ids(&self) -> Vec<u32> {
        self.actors.read().unwrap().keys().map(|f| f.0).collect()
    }

    /// Total fragments in the keyspace (`0..fragment_count`), whether or
    /// not this node serves them.
    pub(crate) fn fragment_count(&self) -> usize {
        self.fragment_count
    }

    /// Add (or replace) the actor for `fragment`.
    pub(super) fn insert(&self, fragment: FragmentId, actor: ActorRef<KvMessage>) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.actors
            .write()
            .unwrap()
            .insert(fragment, FragmentRoute { generation, actor });
        generation
    }

    /// Remove the actor for `fragment`.
    pub(super) fn remove(&self, fragment: FragmentId) {
        self.actors.write().unwrap().remove(&fragment);
    }

    /// Publish a supervisor replacement only while the exact subtree route
    /// that installed `expected` is still current. Placement may remove and
    /// re-add the same fragment while an old monitor is waking up; the
    /// generation check prevents that stale monitor from overwriting the new
    /// subtree (including an actor-id ABA after a very long process lifetime).
    pub(super) fn replace_if_current(
        &self,
        fragment: FragmentId,
        generation: u64,
        expected: &ActorRef<KvMessage>,
        replacement: ActorRef<KvMessage>,
    ) -> bool {
        let mut actors = self.actors.write().unwrap();
        let Some(route) = actors.get_mut(&fragment) else {
            return false;
        };
        if route.generation != generation || route.actor.get_id() != expected.get_id() {
            return false;
        }
        route.actor = replacement;
        true
    }

    pub(super) fn is_current(
        &self,
        fragment: FragmentId,
        generation: u64,
        expected: &ActorRef<KvMessage>,
    ) -> bool {
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .is_some_and(|route| {
                route.generation == generation && route.actor.get_id() == expected.get_id()
            })
    }

    pub(super) fn remove_if_generation(&self, fragment: FragmentId, generation: u64) {
        let mut actors = self.actors.write().unwrap();
        if actors
            .get(&fragment)
            .is_some_and(|route| route.generation == generation)
        {
            actors.remove(&fragment);
        }
    }

    #[cfg(test)]
    pub(super) async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        self.get_all_retained(None).await
    }

    pub(super) async fn get_all_retained(
        &self,
        retention: Option<Arc<dyn Send + Sync>>,
    ) -> Result<HashMap<String, VersionedValue>, NodeError> {
        // Clone the refs first; don't hold the lock across awaits.
        let actors: Vec<ActorRef<KvMessage>> = self
            .actors
            .read()
            .unwrap()
            .values()
            .map(|route| route.actor.clone())
            .collect();
        let mut all = HashMap::new();
        for actor in actors {
            let res = actor
                .call(
                    |reply| KvMessage::GetAll { reply }.with_optional_retention(retention.clone()),
                    Some(Duration::from_secs(30)),
                )
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?;
            match res {
                CallResult::Success(map) => all.extend(map),
                CallResult::Timeout => {
                    return Err(NodeError::Actor("get_all timed out".to_string()))
                }
                CallResult::SenderError => {
                    return Err(NodeError::Actor(
                        "get_all reply channel dropped".to_string(),
                    ))
                }
            }
        }
        Ok(all)
    }
}

pub(super) enum ShutdownProgress {
    Running,
    FencingReplication,
    DrainingReplication(Vec<ReplicationDrainWaiter>),
    StoppingTransport,
    QuiescingFragments,
    StoppingFragments,
    Stopped,
}

pub(super) struct ReplicationDrainWaiter {
    pub(super) target: ReplicationDrainTarget,
    pub(super) sender: NodeSender,
    pub(super) drain: NodeSenderDrain,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(expiry) = &self.ttl_expiry {
            expiry.request_abort();
        }
        // Async cleanup still belongs to `shutdown`, but never leave task
        // admission open merely because a caller dropped the handle.
        self.operation_gate.begin_shutdown();
        self.mutation_service.request_shutdown();
        self.embedded_operations.request_shutdown();
        self.placement_tasks.request_shutdown();
        self.tasks.request_shutdown();
        if let Ok(senders) = self.senders.try_lock() {
            for sender in senders.values() {
                sender.request_stop();
            }
        }
        self.retiring_senders.request_stop_all();
        self.supervisor_stops.stop_all();
        let store_binding = match self.store_binding.get_mut() {
            Ok(binding) => binding,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(binding) = store_binding.as_mut() {
            binding.abandon();
        }
    }
}

/// Spawn one fragment's supervised subtree (supervisor + dispatcher)
/// and return both. Shared by startup and the Phase 8 placement
/// driver (which spawns subtrees for newly assigned fragments at
/// runtime).
pub(super) async fn spawn_fragment_subtree_registered(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    hlc: &Arc<Mutex<HlcClock>>,
    registry: &Arc<SupervisorRegistry>,
    expiry_publisher: Option<ExpiryPublisher>,
) -> Result<(ActorRef<SupervisorMessage>, ActorRef<KvMessage>), NodeError> {
    // Unique per supervisor: tests run nodes in parallel in one
    // process, and the actor registry is process-global.
    let worker_prefix = format!(
        "{}-frag{}-{}",
        node_id,
        fragment.0,
        NEXT_WORKER_PREFIX.fetch_add(1, Ordering::SeqCst)
    );
    let store = config
        .store
        .clone()
        .map(|inner| Arc::new(PrefixedStore::new(inner, fragment)) as Arc<dyn KvStore>);
    let (supervisor, startup_handle) = ActorRuntime::<FragmentSupervisor>::spawn_instant(
        None,
        FragmentSupervisor,
        SupervisorArgs::builder()
            .fragment(fragment)
            .worker_prefix(worker_prefix)
            .maybe_store(store)
            .hlc(hlc.clone())
            .default_pool(config.default_pool.clone())
            .high_pool(config.high_pool.clone())
            .background_pool(config.background_pool.clone())
            .flush_batch_size(config.flush_batch_size)
            .persistence(config.persistence.clone())
            .flush_interval(config.flush_interval)
            .clean_interval(config.clean_interval)
            .snapshot_interval(config.snapshot_interval)
            .build()
            .with_expiry_publisher(expiry_publisher),
    )
    .map_err(|e| NodeError::Actor(e.to_string()))?;

    // Publish ownership before the first await. Dynamic placement tasks are
    // aborted at the start of Node shutdown; the independent startup observer
    // keeps pre_start alive and makes its completion (including failed-start
    // child cleanup) visible to the synchronous supervisor registry.
    let (startup_tx, startup_rx) = watch::channel(SupervisorStartupState::Starting);
    registry.insert(fragment, supervisor.clone(), startup_rx.clone());
    let startup_supervisor = supervisor.clone();
    drop(tokio::spawn(async move {
        let state = match startup_handle.await {
            Ok(Ok(_lifecycle_handle)) => SupervisorStartupState::Ready,
            Ok(Err(error)) => {
                cleanup_failed_supervisor_start(&startup_supervisor).await;
                SupervisorStartupState::Failed(SupervisorStartupFailure::from_spawn(error))
            }
            Err(error) => {
                cleanup_failed_supervisor_start(&startup_supervisor).await;
                SupervisorStartupState::Failed(SupervisorStartupFailure::Actor(format!(
                    "startup task failed: {error}"
                )))
            }
        };
        startup_tx.send_replace(state);
    }));

    match wait_for_supervisor_startup(startup_rx).await {
        Ok(SupervisorStartupState::Ready) => {}
        Ok(SupervisorStartupState::Failed(error)) => {
            registry.remove_if(fragment, &supervisor);
            return Err(error.into_node_error());
        }
        Ok(SupervisorStartupState::Starting) => unreachable!("startup wait returned pending"),
        Err(error) => {
            return Err(NodeError::Actor(error));
        }
    }
    let dispatcher_result = supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await;
    let dispatcher = match dispatcher_result {
        Ok(CallResult::Success(dispatcher)) => dispatcher,
        Ok(_) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove_if(fragment, &supervisor);
            return Err(NodeError::Actor(
                "supervisor dropped dispatcher reply".to_string(),
            ));
        }
        Err(error) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove_if(fragment, &supervisor);
            return Err(NodeError::Actor(error.to_string()));
        }
    };
    Ok((supervisor, dispatcher))
}

/// Supervisor pre_start performs the primary recursive rollback. This
/// observer-side sweep covers a framework-level startup failure outside that
/// hook and publishes failure only after any still-linked children exit.
pub(super) async fn cleanup_failed_supervisor_start(supervisor: &ActorRef<SupervisorMessage>) {
    let children = supervisor.get_children();
    for child in &children {
        let _ = child.drain();
    }
    for child in children {
        let _ = child.wait(None).await;
    }
}

/// Keep the router pointed at the dispatcher currently owned by one fragment
/// supervisor. The supervisor is the lifecycle authority and already replaces
/// a failed dispatcher in place; this monitor republishes that replacement to
/// every local and replication call site which resolves through the router.
///
/// Route generation plus actor identity make publication ABA-safe when a
/// dynamic placement loss/re-add races the old subtree's final failure event.
pub(super) async fn monitor_fragment_dispatcher(
    fragment: FragmentId,
    generation: u64,
    supervisor: ActorRef<SupervisorMessage>,
    router: FragmentRouter,
    mut current: ActorRef<KvMessage>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() || !router.is_current(fragment, generation, &current) {
            return;
        }

        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return;
            }
            _ = current.wait(None) => {}
        }

        let replacement = loop {
            if *shutdown.borrow() || !router.is_current(fragment, generation, &current) {
                return;
            }
            match supervisor
                .call(
                    |reply| SupervisorMessage::GetDispatcher { reply },
                    Some(RECONNECT_BASE),
                )
                .await
            {
                Ok(CallResult::Success(candidate)) if candidate.get_id() != current.get_id() => {
                    break candidate;
                }
                Ok(CallResult::Success(_)) | Ok(CallResult::Timeout) => {}
                Ok(CallResult::SenderError) | Err(_) => return,
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return;
                }
                _ = tokio::time::sleep(RECONNECT_BASE) => {}
            }
        };

        if !router.replace_if_current(fragment, generation, &current, replacement.clone()) {
            return;
        }
        tracing::warn!(
            fragment = fragment.0,
            previous_dispatcher = ?current.get_id(),
            replacement_dispatcher = ?replacement.get_id(),
            "republished restarted fragment dispatcher"
        );
        current = replacement;
    }
}

pub(super) async fn register_fragment_dispatcher_monitor(
    tasks: &Arc<RuntimeTasks>,
    fragment: FragmentId,
    generation: u64,
    supervisor: ActorRef<SupervisorMessage>,
    router: FragmentRouter,
    dispatcher: ActorRef<KvMessage>,
) -> bool {
    let shutdown = tasks.shutdown_signal();
    tasks
        .spawn(monitor_fragment_dispatcher(
            fragment, generation, supervisor, router, dispatcher, shutdown,
        ))
        .await
}

#[cfg(test)]
pub(super) async fn spawn_fragment_subtree(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    hlc: &Arc<Mutex<HlcClock>>,
) -> Result<(ActorRef<SupervisorMessage>, ActorRef<KvMessage>), NodeError> {
    let registry = Arc::new(SupervisorRegistry::new(1));
    let result =
        spawn_fragment_subtree_registered(node_id, fragment, config, hlc, &registry, None).await;
    if let Ok((supervisor, _)) = &result {
        registry.remove_if(fragment, supervisor);
    }
    result
}

/// Synchronous ownership of every self-running fragment supervisor. Its Drop
/// implementation closes the late-publication race when a Node is dropped
/// while a placement task is still unwinding.
pub(super) struct SupervisorRegistry {
    pub(super) supervisors: StdMutex<HashMap<FragmentId, Vec<RegisteredSupervisor>>>,
}

#[derive(Clone)]
pub(super) struct RegisteredSupervisor {
    pub(super) actor: ActorRef<SupervisorMessage>,
    pub(super) prepared: bool,
    pub(super) startup: watch::Receiver<SupervisorStartupState>,
}

#[derive(Clone, Debug)]
pub(super) enum SupervisorStartupState {
    Starting,
    Ready,
    Failed(SupervisorStartupFailure),
}

#[derive(Clone, Debug)]
pub(super) enum SupervisorStartupFailure {
    Actor(String),
    Storage(String),
}

impl SupervisorStartupFailure {
    pub(super) fn from_spawn(error: SpawnErr) -> Self {
        match error {
            SpawnErr::StartupFailed(error) => {
                match error.downcast::<FragmentStartupStorageError>() {
                    Ok(error) => Self::Storage(error.to_string()),
                    Err(error) => Self::Actor(SpawnErr::StartupFailed(error).to_string()),
                }
            }
            error => Self::Actor(error.to_string()),
        }
    }

    pub(super) fn into_node_error(self) -> NodeError {
        match self {
            Self::Actor(error) => NodeError::Actor(error),
            Self::Storage(error) => NodeError::Storage(error),
        }
    }
}

impl std::fmt::Display for SupervisorStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actor(error) | Self::Storage(error) => f.write_str(error),
        }
    }
}

pub(super) async fn wait_for_supervisor_startup(
    mut startup: watch::Receiver<SupervisorStartupState>,
) -> Result<SupervisorStartupState, String> {
    loop {
        let state = startup.borrow().clone();
        if !matches!(state, SupervisorStartupState::Starting) {
            return Ok(state);
        }
        startup
            .changed()
            .await
            .map_err(|_| "supervisor startup observer exited before completion".to_string())?;
    }
}

impl SupervisorRegistry {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            supervisors: StdMutex::new(HashMap::with_capacity(capacity)),
        }
    }

    pub(super) fn insert(
        &self,
        fragment: FragmentId,
        supervisor: ActorRef<SupervisorMessage>,
        startup: watch::Receiver<SupervisorStartupState>,
    ) {
        self.supervisors
            .lock()
            .unwrap()
            .entry(fragment)
            .or_default()
            .push(RegisteredSupervisor {
                actor: supervisor,
                prepared: false,
                startup,
            });
    }

    #[cfg(test)]
    pub(super) fn insert_ready(
        &self,
        fragment: FragmentId,
        supervisor: ActorRef<SupervisorMessage>,
    ) {
        let (_startup_tx, startup_rx) = watch::channel(SupervisorStartupState::Ready);
        self.insert(fragment, supervisor, startup_rx);
    }

    pub(super) fn get(&self, fragment: FragmentId) -> Option<RegisteredSupervisor> {
        self.supervisors
            .lock()
            .unwrap()
            .get(&fragment)
            .and_then(|entries| entries.last())
            .cloned()
    }

    pub(super) fn remove_if(
        &self,
        fragment: FragmentId,
        supervisor: &ActorRef<SupervisorMessage>,
    ) -> bool {
        let mut supervisors = self.supervisors.lock().unwrap();
        let mut removed = false;
        let mut empty = false;
        if let Some(entries) = supervisors.get_mut(&fragment) {
            let before = entries.len();
            entries.retain(|entry| entry.actor.get_id() != supervisor.get_id());
            removed = entries.len() != before;
            empty = entries.is_empty();
        }
        if empty {
            supervisors.remove(&fragment);
        }
        removed
    }

    pub(super) fn mark_prepared(
        &self,
        fragment: FragmentId,
        supervisor: &ActorRef<SupervisorMessage>,
    ) -> bool {
        let mut supervisors = self.supervisors.lock().unwrap();
        let Some(entries) = supervisors.get_mut(&fragment) else {
            return false;
        };
        let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.actor.get_id() == supervisor.get_id())
        else {
            return false;
        };
        entry.prepared = true;
        true
    }

    pub(super) fn snapshot(&self) -> Vec<(FragmentId, RegisteredSupervisor)> {
        self.supervisors
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(fragment, entries)| entries.iter().cloned().map(|entry| (*fragment, entry)))
            .collect()
    }

    pub(super) fn stop_all(&self) {
        for entries in self.supervisors.lock().unwrap().values() {
            for entry in entries {
                entry.actor.stop(None);
            }
        }
    }

    pub(super) fn clear(&self) {
        self.supervisors.lock().unwrap().clear();
    }

    pub(super) async fn stop_and_join_all(&self) {
        let supervisors = self.snapshot();
        for (_, entry) in &supervisors {
            entry.actor.stop(None);
        }
        for (_, entry) in supervisors {
            // pre_start may still own child actors. Its independent observer
            // publishes completion only after failed-start child cleanup.
            let _ = wait_for_supervisor_startup(entry.startup.clone()).await;
            entry.actor.stop(None);
            let _ = entry.actor.wait(None).await;
        }
        self.clear();
    }
}

impl Drop for SupervisorRegistry {
    fn drop(&mut self) {
        for entries in self.supervisors.get_mut().unwrap().values() {
            for entry in entries {
                entry.actor.stop(None);
            }
        }
    }
}

pub(super) async fn quiesce_fragment_supervisor(
    supervisor: &ActorRef<SupervisorMessage>,
) -> Result<(), FragmentShutdownError> {
    let result = supervisor
        .call(|reply| SupervisorMessage::Quiesce { reply }, None)
        .await
        .map_err(|error| {
            FragmentShutdownError::Actor(format!("quiesce request failed: {error}"))
        })?;
    match result {
        CallResult::Success(result) => result,
        CallResult::Timeout => Err(FragmentShutdownError::Actor(
            "quiesce timed out".to_string(),
        )),
        CallResult::SenderError => Err(FragmentShutdownError::Actor(
            "quiesce reply channel dropped".to_string(),
        )),
    }
}

/// Stops partially constructed fragment trees if startup fails or the startup
/// future is cancelled before ownership transfers to a returned Node.
pub(super) struct SupervisorStartupGuard {
    pub(super) supervisors: Vec<ActorRef<SupervisorMessage>>,
    pub(super) armed: bool,
}

impl SupervisorStartupGuard {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            supervisors: Vec::with_capacity(capacity),
            armed: true,
        }
    }

    pub(super) fn push(&mut self, supervisor: ActorRef<SupervisorMessage>) {
        self.supervisors.push(supervisor);
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
        self.supervisors.clear();
    }
}

impl Drop for SupervisorStartupGuard {
    fn drop(&mut self) {
        if self.armed {
            for supervisor in &self.supervisors {
                supervisor.stop(None);
            }
        }
    }
}

/// Owns a store binding until startup either transfers it to [`Node`] or
/// proves that every partially created store user has exited.
pub(super) struct StoreBindingStartupGuard {
    pub(super) binding: Option<StoreBinding>,
    pub(super) actors_may_exist: bool,
}

impl StoreBindingStartupGuard {
    pub(super) fn new(binding: Option<StoreBinding>) -> Self {
        Self {
            binding,
            actors_may_exist: false,
        }
    }

    pub(super) fn mark_actors_may_exist(&mut self) {
        self.actors_may_exist = true;
    }

    pub(super) fn allow_release_after_join(&mut self) {
        self.actors_may_exist = false;
    }

    pub(super) fn transfer_to_node(&mut self) -> Option<StoreBinding> {
        self.actors_may_exist = false;
        self.binding.take()
    }
}

impl Drop for StoreBindingStartupGuard {
    fn drop(&mut self) {
        if self.actors_may_exist {
            if let Some(binding) = self.binding.as_mut() {
                binding.abandon();
            }
        }
    }
}
impl Node {
    pub(super) fn admit_operation(&self) -> Result<OperationPermit, NodeError> {
        self.operation_gate.try_enter()
    }
    pub(super) async fn fail_startup_after_shutdown(&self, message: &str) -> NodeError {
        match self.shutdown().await {
            Ok(()) => NodeError::Actor(message.to_string()),
            Err(cleanup) => {
                NodeError::Actor(format!("{message}; startup cleanup also failed: {cleanup}"))
            }
        }
    }
    /// Start a node: spawn its [crate::actor::KvActor] and begin
    /// listening for peer connections. Binds `listen_addr` (use port 0
    /// for an ephemeral port) and returns the bound address via [Node::addr].
    /// `node_id` must contain 1 to 255 bytes.
    pub async fn start(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
    ) -> Result<Self, NodeError> {
        Self::start_with_options(node_id, listen_addr, config, NodeRuntimeOptions::default()).await
    }

    /// Resolve an asynchronous configuration provider and start the node with
    /// the resulting settings. Providers are resolved once before any socket
    /// or actor is created, so a failed dynamic lookup leaves no partial node.
    pub async fn start_with_provider<P>(
        node_id: String,
        listen_addr: SocketAddr,
        provider: &P,
    ) -> Result<Self, NodeError>
    where
        P: crate::configuration::ConfigurationProvider + ?Sized,
    {
        Self::start_with_provider_and_options(
            node_id,
            listen_addr,
            provider,
            NodeRuntimeOptions::default(),
        )
        .await
    }

    /// Resolve an asynchronous configuration provider with explicit runtime
    /// liveness and outbound-queue policy.
    pub async fn start_with_provider_and_options<P>(
        node_id: String,
        listen_addr: SocketAddr,
        provider: &P,
        runtime_options: NodeRuntimeOptions,
    ) -> Result<Self, NodeError>
    where
        P: crate::configuration::ConfigurationProvider + ?Sized,
    {
        let config = provider
            .resolve()
            .await
            .map_err(|error| NodeError::Actor(format!("configuration provider: {error}")))?;
        Self::start_with_options(node_id, listen_addr, config, runtime_options).await
    }

    /// Start a node with explicit runtime liveness and outbound-queue policy.
    /// Existing callers can continue using [`Node::start`] for the defaults.
    pub async fn start_with_options(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
        runtime_options: NodeRuntimeOptions,
    ) -> Result<Self, NodeError> {
        runtime_options.validate()?;
        let mut runtime_options = runtime_options;
        runtime_options.replication_retention =
            crate::retained::ByteBudget::new(runtime_options.replication_retained_byte_capacity);
        runtime_options.reconciliation_retention =
            crate::retained::ByteBudget::new(runtime_options.reconciliation_retained_byte_capacity);
        if config.ttl_expiry_handler.is_some() {
            config
                .ttl_expiry_options
                .validate()
                .map_err(NodeError::Actor)?;
        }
        if node_id.is_empty() || node_id.len() > MAX_WIRE_NODE_ID_BYTES {
            return Err(NodeError::Actor(format!(
                "node_id must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
            )));
        }
        if let Some(invalid_member) = config
            .members
            .keys()
            .find(|member_id| member_id.is_empty() || member_id.len() > MAX_WIRE_NODE_ID_BYTES)
        {
            return Err(NodeError::Actor(format!(
                "member node_id {invalid_member:?} must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
            )));
        }
        let dynamic = config.placement_controller.is_some();
        if let Some(recovery) = config.recovery.as_ref() {
            recovery
                .validate()
                .map_err(|error| NodeError::Codec(error.into()))?;
            if !dynamic || config.store.is_none() {
                return Err(NodeError::Codec(
                    "recovery requires dynamic placement and a durable store".into(),
                ));
            }
        }
        if config.fragment_count == 0 {
            return Err(NodeError::Actor(
                "fragment_count must be non-zero".to_string(),
            ));
        }
        if config.fragment_count > u32::MAX as usize {
            return Err(NodeError::Actor(format!(
                "fragment_count must not exceed {}",
                u32::MAX
            )));
        }
        config
            .replication
            .validate()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        config
            .replication
            .validate_node_ingress()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        // Validate before binding storage or transport. Maintenance actors
        // construct interval timers during startup and placement can construct
        // warning deadlines much later; neither may panic on user input.
        let timer_now = Instant::now();
        let warning_now = std::time::Instant::now();
        for (name, interval, enabled) in [
            ("clean_interval", config.clean_interval, true),
            ("snapshot_interval", config.snapshot_interval, true),
            (
                "placement_poll_interval",
                config.placement_poll_interval,
                dynamic,
            ),
            (
                "placement_drain_warn_interval",
                config.placement_drain_warn_interval,
                dynamic,
            ),
        ] {
            if enabled
                && (interval.is_zero()
                    || timer_now.checked_add(interval).is_none()
                    || warning_now.checked_add(interval).is_none())
            {
                return Err(NodeError::Actor(format!(
                    "{name} must be non-zero and representable by the runtime clock"
                )));
            }
        }

        if config
            .cluster_secret
            .as_ref()
            .is_some_and(|secret| secret.len() < 32)
        {
            return Err(NodeError::Actor(
                "cluster_secret must contain at least 32 bytes when configured".to_string(),
            ));
        }
        if config.cluster_secret.is_none() {
            tracing::warn!(
                "no cluster_secret configured: anyone with TCP reachability can join the peer mesh"
            );
        } else {
            tracing::warn!(
                "cluster_secret proves fresh cluster-key possession during handshake but does \
                 not protect post-handshake plaintext frames; use a protected custom Network \
                 transport when channel confidentiality or integrity is required"
            );
        }
        let mut member_addresses: HashMap<SocketAddr, &str> = HashMap::new();
        for (member_id, member_addr) in &config.members {
            if let Some(previous_id) = member_addresses.insert(*member_addr, member_id) {
                if previous_id != member_id {
                    return Err(NodeError::Actor(format!(
                        "members {previous_id} and {member_id} share address {member_addr}"
                    )));
                }
            }
        }
        // Initial placement: the controller's map in dynamic mode
        // (validated), the static fragment_map otherwise.
        let initial_placement: HashMap<FragmentId, Vec<String>> = if dynamic {
            let map = config
                .placement_controller
                .as_ref()
                .expect("checked above")
                .desired_placement();
            validate_placement(&map, config.fragment_count).map_err(NodeError::Actor)?;
            map
        } else {
            if !config.fragment_map.is_empty() {
                validate_placement(&config.fragment_map, config.fragment_count)
                    .map_err(NodeError::Actor)?;
            }
            config.fragment_map.clone()
        };

        let mut config = config;
        config.persistence = config.persistence.for_runtime().map_err(NodeError::Actor)?;
        let recovery = config.recovery.clone().map(|options| {
            Arc::new(RecoveryContext::new(
                options,
                node_id.clone(),
                initial_placement.clone(),
            ))
        });
        let mut sender_config = config.replication.clone();
        if recovery.is_some() {
            sender_config.required_ack_durability = ReplicationAckDurability::StableCommitted;
        }

        // Bind immutable storage identity before network or actor side effects.
        // The binding remains owned by Node until every fragment has stopped.
        let store_identity = StoreIdentity::new(node_id.clone(), config.fragment_count as u32);
        let store_binding = if let Some(store) = config.store.clone() {
            Some(
                config
                    .persistence
                    .executor()
                    .run(move || store.bind(&store_identity))
                    .await
                    .map_err(|error| NodeError::Storage(error.to_string()))?,
            )
        } else {
            None
        };
        let mut store_binding = StoreBindingStartupGuard::new(store_binding);

        // One actor per fragment. With persistence, each fragment gets its
        // own namespace via PrefixedStore; named tables add independent
        // cache/flush/iteration partitions over that fragment store while
        // retaining one backend handle and one replication channel set.
        // Fragments this node serves: from the placement map, or all of
        // them when no map is configured (single-node / legacy).
        let my_fragments: Vec<FragmentId> = if !dynamic && config.fragment_map.is_empty() {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .collect()
        } else {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .filter(|f| {
                    initial_placement
                        .get(f)
                        .map(|r| r.iter().any(|id| id == &node_id))
                        .unwrap_or(false)
                })
                .collect()
        };

        // Bind before creating any actors so a transport startup error has no
        // fragment trees or durable-store users to roll back.
        let (bound_addr, mut acceptor) = config
            .network
            .bind(listen_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;

        // One supervised subtree per served fragment. With persistence,
        // each fragment gets its own key namespace via PrefixedStore and
        // named tables receive independent logical store partitions. The supervisor owns the
        // dispatcher, cleaner and snapshotter; the router talks to the
        // dispatcher through the supervisor.
        let hlc = Arc::new(Mutex::new(
            HlcClock::with_offset(config.clock_skew_ms)
                .with_max_skew(config.max_clock_skew.as_millis().min(u64::MAX as u128) as u64),
        ));
        let supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>> =
            Arc::new(Mutex::new(HashMap::with_capacity(my_fragments.len())));
        let supervisor_stops = Arc::new(SupervisorRegistry::new(my_fragments.len()));
        let router = FragmentRouter::new(config.fragment_count, my_fragments.len());
        // Dynamic placement preloads assigned subtrees synchronously so a
        // corrupt durable record still fails Node::start. Authority is not
        // published until the placement driver reconciles each preload.
        let preloaded_fragments: Arc<StdMutex<HashSet<FragmentId>>> =
            Arc::new(StdMutex::new(if dynamic {
                my_fragments.iter().copied().collect()
            } else {
                HashSet::new()
            }));
        let serving: Arc<RwLock<HashSet<FragmentId>>> = Arc::new(RwLock::new(if dynamic {
            HashSet::new()
        } else {
            my_fragments.iter().copied().collect()
        }));
        let placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>> =
            Arc::new(Mutex::new(if dynamic {
                HashMap::new()
            } else {
                my_fragments
                    .iter()
                    .map(|f| (*f, FragmentPlacementState::Serving))
                    .collect()
            }));
        let tasks = RuntimeTasks::new(TASK_COMMAND_CAPACITY);
        let placement_tasks = RuntimeTasks::new(TASK_COMMAND_CAPACITY);
        let ttl_expiry = config.ttl_expiry_handler.as_ref().map(|handler| {
            ExpiryRuntime::start(
                node_id.clone(),
                handler.clone(),
                config.ttl_expiry_options.clone(),
            )
        });
        let expiry_publisher = ttl_expiry.as_ref().map(ExpiryRuntime::publisher);
        let mut startup_supervisors = SupervisorStartupGuard::new(my_fragments.len());
        for fragment in my_fragments {
            // From this point cancellation may leave a synchronously starting
            // actor alive. The binding must then remain fail-closed.
            store_binding.mark_actors_may_exist();
            let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
                &node_id,
                fragment,
                &config,
                &hlc,
                &supervisor_stops,
                expiry_publisher.clone(),
            )
            .await
            {
                Ok(subtree) => subtree,
                Err(error) => {
                    tasks.shutdown().await;
                    supervisor_stops.stop_and_join_all().await;
                    startup_supervisors.disarm();
                    config.persistence.executor().drain().await;
                    store_binding.allow_release_after_join();
                    return Err(error);
                }
            };
            startup_supervisors.push(supervisor.clone());
            supervisors
                .lock()
                .await
                .insert(fragment, supervisor.clone());
            let generation = router.insert(fragment, dispatcher.clone());
            if !register_fragment_dispatcher_monitor(
                &tasks,
                fragment,
                generation,
                supervisor,
                router.clone(),
                dispatcher,
            )
            .await
            {
                router.remove_if_generation(fragment, generation);
                tasks.shutdown().await;
                supervisor_stops.stop_and_join_all().await;
                startup_supervisors.disarm();
                config.persistence.executor().drain().await;
                store_binding.allow_release_after_join();
                return Err(NodeError::Actor(
                    "node runtime stopped during dispatcher-monitor startup".to_string(),
                ));
            }
        }

        let fragment_map: Arc<RwLock<HashMap<FragmentId, Vec<String>>>> =
            Arc::new(RwLock::new(initial_placement));
        let (membership_tx, membership_rx) = if dynamic {
            let (tx, rx) = watch::channel(0u64);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let (peer_changes, _) = watch::channel(0u64);
        let (dial_intent_changes, _) = watch::channel(0u64);
        let Some(read_service) = ReadService::start(
            &tasks,
            runtime_options.serving_read_queue_capacity,
            runtime_options.serving_read_key_byte_capacity,
        )
        .await
        else {
            tasks.shutdown().await;
            supervisor_stops.stop_and_join_all().await;
            startup_supervisors.disarm();
            config.persistence.executor().drain().await;
            store_binding.allow_release_after_join();
            return Err(NodeError::Actor(
                "node runtime stopped during serving read startup".to_string(),
            ));
        };
        let write_concerns = crate::write_concern::WriteConcernTracker::new(
            node_id.clone(),
            fragment_map.clone(),
            runtime_options.forwarded_mutation_capacity,
        );
        write_concerns.configure_regions(runtime_options.regions.as_ref().clone());
        if let Some(cap) = runtime_options.write_concern_acknowledgement_cap {
            write_concerns
                .set_acknowledgement_cap(cap)
                .expect("runtime options validated before startup");
        }
        let node = Self {
            recovery: recovery.clone(),
            node_id: node_id.clone(),
            boot_id: fresh_boot_id(),
            listen_addr: bound_addr,
            router: router.clone(),
            serving: serving.clone(),
            supervisors: supervisors.clone(),
            supervisor_stops: supervisor_stops.clone(),
            fragment_map: fragment_map.clone(),
            placement_states: placement_states.clone(),
            placement_controller: config.placement_controller.clone(),
            placement_poll_interval: config.placement_poll_interval,
            members: config.members.clone(),
            pending_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_batch_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_scans: Arc::new(StdMutex::new(HashMap::new())),
            pending_query_pages: Arc::new(StdMutex::new(HashMap::new())),
            query_admission: crate::query::QueryAdmission::with_runtime_capacity(
                runtime_options.query_runtime_byte_capacity,
            ),
            pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
            pending_relays: Arc::new(StdMutex::new(HashMap::new())),
            pending_mutation_relays: Arc::new(StdMutex::new(HashMap::new())),
            next_req_id: Arc::new(AtomicU64::new(1)),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            next_seq: Arc::new(Mutex::new(0)),
            hlc: hlc.clone(),
            senders: Arc::new(Mutex::new(HashMap::new())),
            finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_config,
            write_concerns: write_concerns.clone(),
            runtime_options: runtime_options.clone(),
            storage_executor: config.persistence.executor(),
            persistence_budget: config.persistence.aggregate(),
            sender_reports: Arc::new(Mutex::new(HashMap::new())),
            sync_entries_applied: Arc::new(AtomicU64::new(0)),
            membership_tx: membership_tx.clone(),
            cluster_secret: config.cluster_secret.clone(),
            placement_tasks,
            tasks,
            read_service,
            inbound_connections: Arc::new(Semaphore::new(
                runtime_options.inbound_connection_capacity,
            )),
            receive_byte_budget: Arc::new(Semaphore::new(runtime_options.receive_byte_capacity)),
            node_peer_queue_budget: NodePeerByteBudget::new(
                runtime_options.node_peer_queue_byte_capacity,
            ),
            relay_admission: RelayAdmission::new(
                runtime_options.relay_capacity,
                runtime_options.relay_key_byte_capacity,
            ),
            mutation_service: MutationService::new_with_tracker(
                runtime_options.forwarded_mutation_capacity,
                runtime_options.forwarded_mutation_byte_capacity,
                write_concerns,
            ),
            embedded_operations: EmbeddedOperations::new(
                runtime_options.embedded_operation_capacity,
                runtime_options.embedded_operation_byte_capacity,
            ),
            dial_intents: Arc::new(StdMutex::new(HashMap::new())),
            dial_intent_changes,
            peer_changes,
            operation_gate: OperationGate::new(),
            shutdown_state: Mutex::new(ShutdownProgress::Running),
            replication_drain_report: StdMutex::new(None),
            network: config.network.clone(),
            ttl_expiry,
            store_binding: StdMutex::new(store_binding.transfer_to_node()),
        };

        if !node
            .tasks
            .spawn(node.peer_failures.clone().run(node.peers.clone()))
            .await
        {
            return Err(node
                .fail_startup_after_shutdown(
                    "node runtime stopped during peer-failure worker startup",
                )
                .await);
        }

        // Phase 5: spawn a sender per (fragment, replica peer).
        // Legacy mode (no fragment map) spawns senders lazily on connect.
        for frag in router.fragment_ids() {
            let frag_id = FragmentId(frag);
            let replicas: Vec<String> = fragment_map
                .read()
                .unwrap()
                .get(&frag_id)
                .cloned()
                .unwrap_or_default();
            for peer in replicas {
                if peer != node_id {
                    node.ensure_sender(frag_id, &peer).await;
                }
            }
        }

        // Phase 8: the placement driver converges local state toward
        // the controller's desired placement.
        if dynamic {
            let deps = PlacementDriverDeps {
                recovery: recovery.clone(),
                node_id: node_id.clone(),
                controller: node.placement_controller.clone().expect("checked above"),
                poll_interval: node.placement_poll_interval,
                drain_warn_interval: config.placement_drain_warn_interval,
                fragment_count: config.fragment_count,
                subtree_config: config.clone(),
                router: router.clone(),
                serving: serving.clone(),
                placement: fragment_map.clone(),
                supervisors: supervisors.clone(),
                supervisor_stops: supervisor_stops.clone(),
                placement_states: placement_states.clone(),
                preloaded_fragments,
                placement_generation: Arc::new(AtomicU64::new(0)),
                senders: node.senders.clone(),
                finalizing_senders: node.finalizing_senders.clone(),
                retiring_senders: node.retiring_senders.clone(),
                sender_reports: node.sender_reports.clone(),
                sender_config: node.sender_config.clone(),
                sender_runtime: node
                    .runtime_options
                    .sender_options()
                    .with_write_concerns(node.write_concerns.clone())
                    .with_recovery(recovery.clone()),
                peers: node.peers.clone(),
                peer_failures: node.peer_failures.clone(),
                tasks: Arc::downgrade(&node.placement_tasks),
                dispatcher_tasks: Arc::downgrade(&node.tasks),
                operation_gate: node.operation_gate.clone(),
                hlc: hlc.clone(),
                expiry_publisher: node.ttl_expiry.as_ref().map(ExpiryRuntime::publisher),
            };
            let membership_rx = membership_rx.expect("checked above");
            if !node
                .placement_tasks
                .spawn(placement_driver(deps, membership_rx))
                .await
            {
                return Err(node
                    .fail_startup_after_shutdown(
                        "node runtime stopped during placement-driver startup",
                    )
                    .await);
            }
        }

        // Accept loop: every inbound connection gets a handler task.
        let ctx = node.connection_context();
        let tasks = Arc::downgrade(&node.tasks);
        let inbound_connections = node.inbound_connections.clone();
        if !node
            .tasks
            .spawn(async move {
                loop {
                    match acceptor.accept().await {
                        Ok(stream) => {
                            let permit = match inbound_connections.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    tracing::warn!(
                                        "inbound connection limit reached; dropping accepted socket"
                                    );
                                    continue;
                                }
                            };
                            let connection_ctx = ctx.clone();
                            let registered = match tasks.upgrade() {
                                Some(tasks) => {
                                    tasks
                                        .spawn(async move {
                                            let _ = handle_connection(
                                                connection_ctx,
                                                stream,
                                                /* we_dialed = */ false,
                                                None,
                                                Some(Arc::new(permit)),
                                            )
                                            .await;
                                        })
                                        .await
                                }
                                None => false,
                            };
                            if !registered {
                                break;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "peer accept failed; retrying");
                            tokio::time::sleep(RECONNECT_BASE).await;
                        }
                    }
                }
            })
            .await
        {
            return Err(node
                .fail_startup_after_shutdown("node runtime stopped during listener startup")
                .await);
        }

        // Configured dial intents start even when the peer is initially down;
        // unlike address-only `connect`, they retain the expected identity and
        // retry with bounded backoff from the first failure.
        for (id, addr) in &node.members {
            if id != &node.node_id
                && !node
                    .start_dial_supervisor(*addr, Some(id.clone()), None)
                    .await
            {
                return Err(node
                    .fail_startup_after_shutdown(
                        "node runtime stopped during member-dialer startup",
                    )
                    .await);
            }
        }

        startup_supervisors.disarm();
        Ok(node)
    }
    pub(super) async fn replication_drain_targets(&self) -> Vec<ReplicationDrainTarget> {
        let placement = self.fragment_map.read().unwrap().clone();
        let mut targets = if placement.is_empty() {
            // Legacy full-replication mode discovers peers from authenticated
            // connections. Its only truthful closed-world boundary is the
            // sender set already installed when admission and placement stop.
            self.senders
                .lock()
                .await
                .keys()
                .map(|(fragment, peer_id)| ReplicationDrainTarget {
                    fragment: *fragment,
                    peer_id: peer_id.clone(),
                })
                .collect()
        } else {
            let mut targets = Vec::new();
            for fragment in self.router.fragment_ids().into_iter().map(FragmentId) {
                if let Some(replicas) = placement.get(&fragment) {
                    targets.extend(
                        replicas
                            .iter()
                            .filter(|peer_id| *peer_id != &self.node_id)
                            .map(|peer_id| ReplicationDrainTarget {
                                fragment,
                                peer_id: peer_id.clone(),
                            }),
                    );
                }
            }
            targets
        };
        targets.sort();
        targets.dedup();
        targets
    }
    pub(super) async fn prepare_replication_drains(
        &self,
        targets: &[ReplicationDrainTarget],
    ) -> Result<Vec<ReplicationDrainWaiter>, String> {
        let sender_runtime = self
            .runtime_options
            .sender_options()
            .with_write_concerns(self.write_concerns.clone());
        let mut senders = Vec::with_capacity(targets.len());
        for target in targets {
            ensure_sender_entry(
                target.fragment,
                &target.peer_id,
                SenderEntryDeps {
                    config: &self.sender_config,
                    runtime: &sender_runtime,
                    router: &self.router,
                    hlc: &self.hlc,
                    senders: &self.senders,
                    finalizing_senders: &self.finalizing_senders,
                    retiring_senders: &self.retiring_senders,
                    reports: &self.sender_reports,
                    peer_failures: &self.peer_failures,
                    operation_gate: &self.operation_gate,
                    allow_quiescing: true,
                },
            )
            .await;

            let sender = self
                .senders
                .lock()
                .await
                .get(&(target.fragment, target.peer_id.clone()))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "no authoritative fragment sender is available for fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
            if sender.is_closed() {
                return Err(format!(
                    "replication sender stopped for fragment {} peer {}",
                    target.fragment, target.peer_id
                ));
            }
            let peers = self.peers.lock().await;
            if let Some(peer) = peers.get(&target.peer_id) {
                sender.connect_peer(peer.budgeted_sender()).map_err(|_| {
                    format!(
                        "replication sender stopped while reconnecting fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
            }
            drop(peers);
            senders.push((target.clone(), sender));
        }

        senders
            .into_iter()
            .map(|(target, sender)| {
                let drain = sender.request_drain().map_err(|_| {
                    format!(
                        "replication sender stopped while fencing fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
                Ok(ReplicationDrainWaiter {
                    target,
                    sender,
                    drain,
                })
            })
            .collect()
    }
    pub(super) async fn replication_drain_partition(
        &self,
        waiters: &[ReplicationDrainWaiter],
    ) -> Result<(Vec<ReplicationDrainTarget>, Vec<ReplicationDrainTarget>), String> {
        let senders = self.senders.lock().await;
        let mut completed = Vec::new();
        let mut pending = Vec::new();
        for waiter in waiters {
            let current = senders
                .get(&(waiter.target.fragment, waiter.target.peer_id.clone()))
                .ok_or_else(|| {
                    format!(
                        "replication sender disappeared for fragment {} peer {}",
                        waiter.target.fragment, waiter.target.peer_id
                    )
                })?;
            if !current.same_task(&waiter.sender) {
                return Err(format!(
                    "replication sender was replaced for fragment {} peer {}",
                    waiter.target.fragment, waiter.target.peer_id
                ));
            }
            if waiter.drain.is_complete() {
                completed.push(waiter.target.clone());
            } else if waiter.drain.is_closed() || waiter.sender.is_closed() {
                return Err(format!(
                    "replication sender stopped before draining fragment {} peer {}",
                    waiter.target.fragment, waiter.target.peer_id
                ));
            } else {
                pending.push(waiter.target.clone());
            }
        }
        Ok((completed, pending))
    }
    pub(super) async fn finish_local_shutdown(
        &self,
        progress: &mut ShutdownProgress,
    ) -> Result<(), NodeError> {
        if matches!(progress, ShutdownProgress::StoppingTransport) {
            self.mutation_service.request_shutdown();
            self.embedded_operations.request_shutdown();
            // Placement owns fragment and sender topology. Join it before the
            // general task set so no cancelled transition can publish work
            // after the transport/sender snapshot below.
            self.placement_tasks.shutdown().await;
            self.tasks.shutdown().await;
            // Transport cancellation closes RPC waiters, not accepted apply
            // ownership. Drain complete outcomes/fanout while every sender and
            // fragment actor is still alive. A canceled caller may resume this.
            self.mutation_service.shutdown().await;
            self.embedded_operations.shutdown().await;

            // Release every request/relay waiter retained by a cancelled
            // handler. Admitted remote operations then leave the public gate
            // promptly instead of waiting for their normal request deadline.
            self.pending_gets.lock().unwrap().clear();
            self.pending_batch_gets.lock().unwrap().clear();
            self.pending_mutations.lock().unwrap().clear();
            self.pending_relays.lock().unwrap().clear();
            self.pending_mutation_relays.lock().unwrap().clear();

            // A hard transport shutdown can cancel a handler before its normal
            // exact-channel cleanup tail. Clear every remaining peer entry
            // before stopping the long-lived replication senders.
            let peers: Vec<(String, PeerChannel)> = self.peers.lock().await.drain().collect();
            for (_, channel) in &peers {
                channel.cancel();
            }
            let disconnected_targets = {
                let senders = self.senders.lock().await;
                let mut targets = Vec::new();
                for (peer, channel) in &peers {
                    for ((_, sender_peer), sender) in senders.iter() {
                        if sender_peer == peer {
                            targets.push((sender.clone(), channel.raw_sender()));
                        }
                    }
                }
                targets
            };
            for (sender, peer_tx) in disconnected_targets {
                let _ = sender.send(SenderMsg::PeerDisconnected { peer_tx });
            }
            self.peer_changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });

            self.operation_gate.wait_for_drain().await;

            let senders: Vec<NodeSender> = self.senders.lock().await.values().cloned().collect();
            for sender in senders {
                sender.stop().await;
            }
            let retiring_senders = self.retiring_senders.snapshot();
            for sender in retiring_senders {
                sender.stop().await;
            }
            self.retiring_senders.clear();
            *progress = ShutdownProgress::QuiescingFragments;
        }

        if matches!(progress, ShutdownProgress::QuiescingFragments) {
            // Prepare every fragment before destroying any fragment state. A
            // partial store outage therefore leaves all successful fragments
            // quiesced and every failed buffer available for an exact retry.
            let supervisors = self.supervisor_stops.snapshot();
            let mut failures = Vec::new();
            let mut saw_storage_failure = false;
            for (fragment, entry) in supervisors {
                if entry.prepared {
                    continue;
                }
                let quiesce_result = match wait_for_supervisor_startup(entry.startup.clone()).await
                {
                    Ok(SupervisorStartupState::Ready) => {
                        quiesce_fragment_supervisor(&entry.actor).await
                    }
                    Ok(SupervisorStartupState::Failed(error)) => {
                        tracing::warn!(
                            fragment = fragment.0,
                            %error,
                            "fragment startup failed before shutdown; child cleanup completed"
                        );
                        Ok(())
                    }
                    Ok(SupervisorStartupState::Starting) => {
                        unreachable!("startup wait returned pending")
                    }
                    Err(error) => Err(FragmentShutdownError::Actor(error)),
                };
                match quiesce_result {
                    Ok(()) => {
                        if !self.supervisor_stops.mark_prepared(fragment, &entry.actor) {
                            failures.push(format!(
                                "fragment {fragment}: supervisor ownership changed during quiesce"
                            ));
                        }
                    }
                    Err(FragmentShutdownError::Storage(error)) => {
                        saw_storage_failure = true;
                        failures.push(format!("fragment {fragment}: {error}"));
                    }
                    Err(FragmentShutdownError::Actor(error)) => {
                        failures.push(format!("fragment {fragment}: {error}"));
                    }
                }
            }
            if !failures.is_empty() {
                let message = failures.join("; ");
                return if saw_storage_failure {
                    Err(NodeError::Storage(message))
                } else {
                    Err(NodeError::Actor(message))
                };
            }
            if self
                .supervisor_stops
                .snapshot()
                .iter()
                .any(|(_, entry)| !entry.prepared)
            {
                return Err(NodeError::Actor(
                    "fragment supervisor appeared after runtime task shutdown".to_string(),
                ));
            }
            // Persist this transition before sending the first stop signal. A
            // cancelled caller can then resume joins without asking a stopped
            // supervisor to prove durability again.
            *progress = ShutdownProgress::StoppingFragments;
        }

        if matches!(progress, ShutdownProgress::StoppingFragments) {
            // Every fragment's authoritative final sweep/fence is complete.
            // Drain notifications with a manager-owned deadline; cancellation
            // of this shutdown waiter does not detach or restart the drain.
            if let Some(expiry) = &self.ttl_expiry {
                expiry.shutdown().await;
            }
            let supervisors = self.supervisor_stops.snapshot();
            for (_, entry) in &supervisors {
                if matches!(&*entry.startup.borrow(), SupervisorStartupState::Ready) {
                    entry.actor.stop(None);
                }
            }
            for (_, entry) in supervisors {
                if matches!(&*entry.startup.borrow(), SupervisorStartupState::Ready) {
                    let _ = entry.actor.wait(None).await;
                }
            }
            self.supervisors.lock().await.clear();
            self.supervisor_stops.clear();
            self.storage_executor.drain().await;
            let store_binding = self.store_binding.lock().unwrap().take();
            drop(store_binding);
            self.operation_gate.finish_shutdown();
            *progress = ShutdownProgress::Stopped;
        }
        Ok(())
    }
    /// Shut the node down and report whether every local fragment reached its
    /// final persistence fence.
    ///
    /// Shutdown is one-way: it first rejects new public operations, closes and
    /// joins Node-owned runtime tasks, waits for already-admitted public work,
    /// and stops replication senders. It then quiesces every fragment before
    /// stopping any supervisor. A failed store flush leaves the fragment state
    /// and retryable batch owned by its supervisor; this method returns the
    /// error and a later call resumes shutdown. Only a successful return drops
    /// all fragment state and releases a configured RocksDB lock and live
    /// attachment.
    ///
    /// This local durability fence does not wait for peer replication ACKs;
    /// transport convergence has its own lifecycle policy. Calling this after
    /// a timed-out or cancelled replication drain explicitly abandons that
    /// remote guarantee and completes the local-only shutdown.
    pub async fn shutdown(&self) -> Result<(), NodeError> {
        let mut progress = self.shutdown_state.lock().await;
        if matches!(*progress, ShutdownProgress::Stopped) {
            return Ok(());
        }
        if matches!(
            *progress,
            ShutdownProgress::Running
                | ShutdownProgress::FencingReplication
                | ShutdownProgress::DrainingReplication(_)
        ) {
            self.operation_gate.begin_shutdown();
            *self.replication_drain_report.lock().unwrap() = None;
            *progress = ShutdownProgress::StoppingTransport;
        }
        self.finish_local_shutdown(&mut progress).await
    }
    /// Callback diagnostics, or `None` when registration is disabled. Counters
    /// describe local best-effort delivery and are readable after shutdown.
    pub fn ttl_expiry_diagnostics(&self) -> Option<ExpiryDiagnostics> {
        self.ttl_expiry.as_ref().map(ExpiryRuntime::diagnostics)
    }
    /// Fence locally admitted mutations, reconcile every frozen peer target
    /// through the configured ACK durability barrier, then complete the normal
    /// local persistence shutdown.
    ///
    /// The remote guarantee is a per-target LWW version floor: for each
    /// reported fragment/peer, the receiver acknowledged the fenced winner or
    /// a newer winner. It is not a global cache-equality, quorum, or fsync
    /// guarantee. `MemoryApplied` remains volatile; `StoreCommitted` means the
    /// receiver store accepted its ordinary flush, while `StableCommitted`
    /// additionally invokes its explicit stable-storage fence.
    ///
    /// The single timeout covers placement freeze, admitted-mutation drain,
    /// sender preparation, reconnect/reconciliation, and remote durability.
    /// Timeout or caller cancellation leaves the node quiesced with transport,
    /// senders, and fragment state alive; retrying this method resumes every
    /// issued exact sender generation. A sender task that exits is replaced and
    /// re-fenced from the still-authoritative fragment cache. [`Node::shutdown`]
    /// is the explicit local-only escape. Mutation responses already queued to
    /// a socket can still be lost, so clients must retry stamped operations
    /// idempotently.
    ///
    /// With explicit placement, targets are the configured remote replicas of
    /// every locally active fragment after placement tasks stop. Legacy empty-
    /// map mode can cover only sender targets already installed at the fence.
    pub async fn shutdown_with_replication_drain(
        &self,
        options: ReplicationDrainOptions,
    ) -> Result<ReplicationDrainReport, ReplicationDrainShutdownError> {
        if options.timeout.is_zero() {
            return Err(ReplicationDrainShutdownError::InvalidOptions(
                "timeout must be non-zero".to_string(),
            ));
        }
        let Some(deadline) = Instant::now().checked_add(options.timeout) else {
            return Err(ReplicationDrainShutdownError::InvalidOptions(
                "timeout is too large".to_string(),
            ));
        };

        let mut progress = tokio::time::timeout_at(deadline, self.shutdown_state.lock())
            .await
            .map_err(|_| ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::ShutdownCoordination,
                completed: Vec::new(),
                pending: Vec::new(),
            })?;
        let stored_report = || self.replication_drain_report.lock().unwrap().clone();
        if matches!(*progress, ShutdownProgress::Stopped) {
            return stored_report().ok_or_else(|| {
                ReplicationDrainShutdownError::Unavailable(
                    "node already completed a local-only shutdown".to_string(),
                )
            });
        }
        if stored_report().is_none()
            && matches!(
                *progress,
                ShutdownProgress::StoppingTransport
                    | ShutdownProgress::QuiescingFragments
                    | ShutdownProgress::StoppingFragments
            )
        {
            return Err(ReplicationDrainShutdownError::Unavailable(
                "local-only shutdown has already stopped replication".to_string(),
            ));
        }

        if matches!(*progress, ShutdownProgress::Running) {
            self.operation_gate.begin_shutdown();
            // Publish the topology freeze synchronously. Even an already-
            // expired deadline must not return while the placement manager
            // can still register a transition.
            self.placement_tasks.request_shutdown();
            *progress = ShutdownProgress::FencingReplication;
        }

        if matches!(*progress, ShutdownProgress::FencingReplication) {
            self.placement_tasks.request_shutdown();
            if tokio::time::timeout_at(deadline, self.placement_tasks.shutdown())
                .await
                .is_err()
            {
                return Err(ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::PlacementFreeze,
                    completed: Vec::new(),
                    pending: Vec::new(),
                });
            }
            if tokio::time::timeout_at(deadline, self.operation_gate.wait_for_drain())
                .await
                .is_err()
            {
                return Err(ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::MutationAdmissionDrain,
                    completed: Vec::new(),
                    pending: Vec::new(),
                });
            }

            let targets = tokio::time::timeout_at(deadline, self.replication_drain_targets())
                .await
                .map_err(|_| ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::TargetSnapshot,
                    completed: Vec::new(),
                    pending: Vec::new(),
                })?;
            let waiters =
                match tokio::time::timeout_at(deadline, self.prepare_replication_drains(&targets))
                    .await
                {
                    Ok(Ok(waiters)) => waiters,
                    Ok(Err(error)) => {
                        return Err(ReplicationDrainShutdownError::RetryableSender(error));
                    }
                    Err(_) => {
                        return Err(ReplicationDrainShutdownError::Timeout {
                            phase: ReplicationDrainPhase::SenderPreparation,
                            completed: Vec::new(),
                            pending: targets,
                        });
                    }
                };
            *progress = ShutdownProgress::DrainingReplication(waiters);
        }

        if let ShutdownProgress::DrainingReplication(waiters) = &*progress {
            loop {
                let all_targets: Vec<_> =
                    waiters.iter().map(|waiter| waiter.target.clone()).collect();
                let partition =
                    tokio::time::timeout_at(deadline, self.replication_drain_partition(waiters))
                        .await
                        .map_err(|_| ReplicationDrainShutdownError::Timeout {
                            phase: ReplicationDrainPhase::PeerReconciliation,
                            completed: Vec::new(),
                            pending: all_targets,
                        })?;
                let (completed, pending) = match partition {
                    Ok(partition) => partition,
                    Err(error) => {
                        // Exact-task correlation prevents a replacement from
                        // satisfying the old waiter. Return to preparation so
                        // a retry can install and fence a fresh sender from the
                        // still-authoritative cache.
                        *progress = ShutdownProgress::FencingReplication;
                        return Err(ReplicationDrainShutdownError::RetryableSender(format!(
                            "{error}; retry to replace and re-fence the sender"
                        )));
                    }
                };
                if pending.is_empty() {
                    let report = ReplicationDrainReport {
                        targets: completed,
                        durability: self.sender_config.required_ack_durability,
                    };
                    *self.replication_drain_report.lock().unwrap() = Some(report);
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(ReplicationDrainShutdownError::Timeout {
                        phase: ReplicationDrainPhase::PeerReconciliation,
                        completed,
                        pending,
                    });
                }
                tokio::time::sleep_until(deadline.min(deadline_after(Duration::from_millis(10))))
                    .await;
            }
            *progress = ShutdownProgress::StoppingTransport;
        }

        let report = stored_report().ok_or_else(|| {
            ReplicationDrainShutdownError::Unavailable(
                "replication drain completed without a retained report".to_string(),
            )
        })?;
        self.finish_local_shutdown(&mut progress)
            .await
            .map_err(|error| ReplicationDrainShutdownError::Local {
                error,
                replication: report.clone(),
            })?;
        Ok(report)
    }
    /// Local health snapshots from owned fragment supervisors, including any
    /// bootstrapping/draining fragment still owned by this node. The one
    /// configurable health deadline includes registry acquisition and every
    /// sequential probe. Missing serving supervisors, failed dispatchers, and
    /// timeouts return errors; unavailable counters are never replaced by zero.
    /// Worker names/counts describe linked actors observed Running, not worker
    /// responsiveness. This non-atomic snapshot is not a readiness, replication,
    /// or durability guarantee.
    pub async fn health(&self) -> Result<HashMap<FragmentId, FragmentHealth>, NodeError> {
        let _operation = self.admit_operation()?;
        let deadline = deadline_after(self.runtime_options.health_timeout);
        tokio::time::timeout_at(deadline, async {
            let mut supervisors: Vec<(FragmentId, ActorRef<SupervisorMessage>)> = self
                .supervisors
                .lock()
                .await
                .iter()
                .map(|(f, s)| (*f, s.clone()))
                .collect();
            let owned: HashSet<_> = supervisors.iter().map(|(fragment, _)| *fragment).collect();
            for fragment in self.serving.read().unwrap().iter() {
                if !owned.contains(fragment) {
                    return Err(NodeError::Actor(format!(
                        "health unavailable for fragment {fragment}: serving supervisor is missing"
                    )));
                }
            }
            supervisors.sort_unstable_by_key(|(fragment, _)| *fragment);
            let mut out = HashMap::with_capacity(supervisors.len());
            for (frag, supervisor) in supervisors {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(NodeError::Actor(
                        "health snapshot deadline expired".to_string(),
                    ));
                }
                let res = supervisor
                    .call(
                        |reply| SupervisorMessage::GetHealth { reply },
                        Some(remaining),
                    )
                    .await
                    .map_err(|error| {
                        NodeError::Actor(format!("health unavailable for fragment {frag}: {error}"))
                    })?;
                match res {
                    CallResult::Success(health) if health.fragment == frag => {
                        out.insert(frag, health);
                    }
                    _ => {
                        return Err(NodeError::Actor(format!(
                            "health unavailable for fragment {frag}: probe failed"
                        )))
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|_| NodeError::Actor("health snapshot deadline expired".to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::store::StoreError;
    use ractor::ActorProcessingErr;
    use std::sync::atomic::AtomicBool;
    use std::sync::Condvar;

    #[tokio::test]
    async fn nodes_serve_and_invalid_settings_fail_before_bind() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let error = match Node::start_with_options(
            "invalid-builder".to_string(),
            address,
            NodeConfig::builder().build(),
            NodeRuntimeOptions::builder()
                .health_timeout(Duration::ZERO)
                .build(),
        )
        .await
        {
            Err(error) => error,
            Ok(node) => {
                node.shutdown().await.unwrap();
                panic!("invalid health timeout accepted");
            }
        };
        assert!(matches!(error, NodeError::Actor(message) if message.contains("health timeout")));
        drop(listener);

        let node = Node::start_with_options(
            "bon-builder".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().fragment_count(2).build(),
            NodeRuntimeOptions::builder().build(),
        )
        .await
        .unwrap();
        node.put("builder-key".to_string(), b"value".to_vec())
            .await
            .unwrap();
        assert_eq!(
            node.get("builder-key".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"value".to_vec()
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_has_one_aggregate_deadline_and_passes_remaining_probe_budget() {
        let timeout = Duration::from_secs(1);
        let node = Node::start_with_options(
            "health-aggregate".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 2,
                ..Default::default()
            },
            NodeRuntimeOptions::default().with_health_timeout(timeout),
        )
        .await
        .unwrap();
        let mut replacements = HashMap::new();
        let mut observed = Vec::new();
        let release_second = Arc::new(Notify::new());
        for fragment in [FragmentId(0), FragmentId(1)] {
            let observed_timeout = Arc::new(StdMutex::new(None));
            let (actor, _) = DelayedHealthSupervisor::spawn(
                None,
                DelayedHealthSupervisor,
                DelayedHealthArgs {
                    fragment,
                    release: (fragment == FragmentId(1)).then(|| release_second.clone()),
                    observed_timeout: observed_timeout.clone(),
                },
            )
            .await
            .unwrap();
            replacements.insert(fragment, actor);
            observed.push(observed_timeout);
        }
        let originals = std::mem::replace(&mut *node.supervisors.lock().await, replacements);
        // First probe responds immediately; the second stays held. This avoids
        // requiring a timed first response to run within a tight CI margin.
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), node.health())
                .await
                .unwrap(),
            Err(NodeError::Actor(_))
        ));
        let first = observed[0].lock().unwrap().unwrap();
        let second = observed[1].lock().unwrap().unwrap();
        assert!(first <= timeout);
        assert!(second < first, "second fragment received a fresh timeout");
        let replacements = std::mem::replace(&mut *node.supervisors.lock().await, originals);
        release_second.notify_one();
        for actor in replacements.values() {
            actor.stop(None);
            actor.wait(None).await.unwrap();
        }
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_deadline_includes_registry_lock_and_missing_serving_supervisor_fails() {
        let mut node = Node::start_with_options(
            "health-registry".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::default().with_health_timeout(Duration::from_millis(20)),
        )
        .await
        .unwrap();
        let mut registry = node.supervisors.lock().await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), node.health()).await.unwrap(),
            Err(NodeError::Actor(error)) if error.contains("deadline")
        ));
        let supervisor = registry.remove(&FragmentId(0)).unwrap();
        drop(registry);
        assert!(matches!(
            node.health().await,
            Err(NodeError::Actor(error)) if error.contains("supervisor is missing")
        ));
        node.supervisors
            .lock()
            .await
            .insert(FragmentId(0), supervisor);
        node.runtime_options.health_timeout = Duration::from_secs(1);
        let health = node.health().await.unwrap();
        let fragment = &health[&FragmentId(0)];
        assert_eq!(fragment.worker_count, fragment.worker_names.len());
        assert_eq!(fragment.worker_count, 16);
        assert_eq!(fragment.shed_counts, (0, 0, 0));
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_stopped_registered_supervisor_reports_fragment_and_recovers() {
        let node = Node::start_with_options(
            "health-stopped-supervisor".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 2,
                ..Default::default()
            },
            NodeRuntimeOptions::default().with_health_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
        let fragment = FragmentId(1);
        let observed_timeout = Arc::new(StdMutex::new(None));
        let (stopped, _) = DelayedHealthSupervisor::spawn(
            None,
            DelayedHealthSupervisor,
            DelayedHealthArgs {
                fragment,
                release: None,
                observed_timeout: observed_timeout.clone(),
            },
        )
        .await
        .unwrap();
        stopped.stop(None);
        tokio::time::timeout(Duration::from_secs(5), stopped.wait(None))
            .await
            .unwrap()
            .unwrap();

        // Keep the exact stopped ref in the owned registry: this must exercise
        // failed RPC admission, not the missing-supervisor or probe-timeout path.
        let original = node
            .supervisors
            .lock()
            .await
            .insert(fragment, stopped.clone())
            .unwrap();
        let failed = tokio::time::timeout(Duration::from_secs(5), node.health()).await;

        // Restore ownership before any failure assertion, so cleanup still
        // reaches the original live supervisor even if the regression fires.
        let replaced = node
            .supervisors
            .lock()
            .await
            .insert(fragment, original)
            .unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(5), node.health()).await;
        let query = tokio::time::timeout(
            Duration::from_secs(5),
            node.get("health-recovered-query".to_string()),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(5), node.shutdown()).await;

        assert_eq!(replaced.get_id(), stopped.get_id());
        assert!(observed_timeout.lock().unwrap().is_none());
        assert!(matches!(
            failed.unwrap(),
            Err(NodeError::Actor(error))
                if error.starts_with(&format!("health unavailable for fragment {fragment}:"))
                    && !error.contains("probe failed")
        ));
        let recovered = recovered.unwrap().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[&fragment].worker_count, 16);
        assert_eq!(recovered[&fragment].shed_counts, (0, 0, 0));
        assert!(query.unwrap().unwrap().is_none());
        shutdown.unwrap().unwrap();
    }

    #[test]
    fn operation_admission_loses_a_race_with_shutdown() {
        let gate = OperationGate::new();

        // Model shutdown winning immediately after try_enter's initial
        // NODE_RUNNING observation but before it publishes the operation.
        gate.begin_shutdown();
        assert!(matches!(
            gate.finish_enter_after_running_check(),
            Err(NodeError::ShuttingDown)
        ));
        assert_eq!(gate.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn shutting_down_error_has_an_actionable_message() {
        assert_eq!(NodeError::ShuttingDown.to_string(), "node is shutting down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_subtree_start_remains_registered_until_startup_completes() {
        let store = Arc::new(BlockingLoadStore::default());
        let registry = Arc::new(SupervisorRegistry::new(1));
        let config = NodeConfig {
            store: Some(store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        };
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let spawn_registry = registry.clone();
        let spawn_hlc = hlc.clone();
        let spawn = tokio::spawn(async move {
            spawn_fragment_subtree_registered(
                "cancelled-start",
                FragmentId(0),
                &config,
                &spawn_hlc,
                &spawn_registry,
                None,
            )
            .await
        });

        let entered = tokio::time::timeout(Duration::from_secs(5), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if entered.is_err() {
            store.release();
            let _ = spawn.await;
            panic!("fragment startup never entered the store load");
        }

        let Some((fragment, entry)) = registry.snapshot().into_iter().next() else {
            store.release();
            let _ = spawn.await;
            panic!("starting supervisor was not synchronously registered");
        };
        let was_starting = matches!(&*entry.startup.borrow(), SupervisorStartupState::Starting);
        spawn.abort();
        let spawn_cancelled = spawn.await.unwrap_err().is_cancelled();

        let startup = tokio::spawn(wait_for_supervisor_startup(entry.startup.clone()));
        tokio::time::sleep(Duration::from_millis(25)).await;
        let wait_blocked = !startup.is_finished();
        store.release();
        let startup_state = tokio::time::timeout(Duration::from_secs(5), startup)
            .await
            .expect("startup observer did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(fragment, FragmentId(0));
        assert!(was_starting);
        assert!(spawn_cancelled);
        assert!(wait_blocked, "shutdown would not have waited for startup");
        assert!(matches!(startup_state, SupervisorStartupState::Ready));

        quiesce_fragment_supervisor(&entry.actor).await.unwrap();
        assert!(registry.mark_prepared(fragment, &entry.actor));
        entry.actor.stop(None);
        let _ = entry.actor.wait(None).await;
        assert!(registry.remove_if(fragment, &entry.actor));
        assert!(registry.snapshot().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_node_start_keeps_exclusive_store_fail_closed() {
        let store = Arc::new(BlockingExclusiveLoadStore::default());
        let config = || NodeConfig {
            store: Some(store.clone()),
            ..NodeConfig::default()
        };
        let startup = tokio::spawn(Node::start(
            "cancelled-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Node startup never entered the blocking store load");

        startup.abort();
        let join_error = match startup.await {
            Err(error) => error,
            Ok(_) => panic!("aborted Node startup completed normally"),
        };
        assert!(join_error.is_cancelled());
        let retry = Node::start(
            "cancelled-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await;
        assert!(matches!(retry, Err(NodeError::Storage(_))));
        assert!(store.attached.load(Ordering::SeqCst));

        store.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.exited.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled startup's store load did not exit");
    }

    #[tokio::test]
    async fn joined_node_start_failure_releases_exclusive_store() {
        let store = Arc::new(FailFirstLoadExclusiveStore::default());
        let config = || NodeConfig {
            store: Some(store.clone()),
            ..NodeConfig::default()
        };
        let first = Node::start(
            "retry-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await;
        assert!(first.is_err());
        assert!(!store.attached.load(Ordering::SeqCst));

        let second = Node::start(
            "retry-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await
        .expect("joined startup rollback did not release the store binding");
        second.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_restart_is_republished_to_the_fragment_router() {
        let node = Node::start(
            "dispatcher-republish".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 1,
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap();
        let fragment = FragmentId(0);
        node.put("before".to_string(), b"value".to_vec())
            .await
            .unwrap();

        let dead = node
            .router
            .route_for_fragment(fragment)
            .expect("fragment route missing before dispatcher failure");
        let dead_id = dead.get_id();
        dead.kill();

        let replacement = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(candidate) = node.router.route_for_fragment(fragment) {
                    if candidate.get_id() != dead_id {
                        break candidate;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor replacement was not republished");
        assert_ne!(replacement.get_id(), dead_id);
        assert_eq!(
            node.get("before".to_string())
                .await
                .unwrap()
                .expect("replacement dispatcher lost the shared cache")
                .value,
            b"value"
        );

        node.put("after".to_string(), b"replacement".to_vec())
            .await
            .unwrap();
        assert_eq!(
            node.get("after".to_string())
                .await
                .unwrap()
                .expect("replacement dispatcher lost shared state")
                .value,
            b"replacement"
        );

        let replacement_id = replacement.get_id();
        replacement.kill();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if node
                    .router
                    .route_for_fragment(fragment)
                    .is_some_and(|candidate| candidate.get_id() != replacement_id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("monitor did not continue across a second dispatcher restart");
        node.put("after-second".to_string(), b"replacement-2".to_vec())
            .await
            .unwrap();
        assert!(node.health().await.unwrap()[&fragment]
            .restarts
            .get("dispatcher")
            .is_some_and(|restarts| *restarts >= 2));

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_dispatcher_monitor_cannot_overwrite_a_readded_fragment() {
        let router = FragmentRouter::new(1, 1);
        let state = || BlockingPageState {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let (retired, retired_handle) = BlockingPageActor::spawn(None, BlockingPageActor, state())
            .await
            .unwrap();
        let retired_generation = router.insert(FragmentId(0), retired.clone());

        let (stale_replacement, stale_handle) =
            BlockingPageActor::spawn(None, BlockingPageActor, state())
                .await
                .unwrap();
        let requested = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (supervisor, supervisor_handle) = BlockingDispatcherSupervisor::spawn(
            None,
            BlockingDispatcherSupervisor,
            BlockingDispatcherSupervisorState {
                replacement: stale_replacement.clone(),
                requested: requested.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let monitor = tokio::spawn(monitor_fragment_dispatcher(
            FragmentId(0),
            retired_generation,
            supervisor.clone(),
            router.clone(),
            retired.clone(),
            shutdown_rx,
        ));

        retired.kill();
        tokio::time::timeout(Duration::from_secs(1), requested.notified())
            .await
            .expect("monitor did not request the supervisor replacement");

        // Placement replaces the entire subtree while the old monitor holds
        // a now-stale replacement response.
        router.remove(FragmentId(0));

        let (current, current_handle) = BlockingPageActor::spawn(None, BlockingPageActor, state())
            .await
            .unwrap();
        let current_generation = router.insert(FragmentId(0), current.clone());
        assert_ne!(retired_generation, current_generation);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), monitor)
            .await
            .expect("stale monitor did not exit")
            .unwrap();
        assert_eq!(
            router
                .route_for_fragment(FragmentId(0))
                .expect("readded route disappeared")
                .get_id(),
            current.get_id()
        );

        let _ = shutdown_tx.send(true);
        current.stop(None);
        stale_replacement.stop(None);
        supervisor.stop(None);
        retired_handle.await.unwrap();
        current_handle.await.unwrap();
        stale_handle.await.unwrap();
        supervisor_handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_sender_retirement_remains_owned_for_shutdown_join() {
        let sender = spawn_node_sender(
            FragmentId(0),
            "retiring-peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: test_sender_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let registry = Arc::new(RetiringSenderRegistry::default());
        let retirement = registry.retire(sender);
        drop(retirement); // model cancellation before the normal join path

        let retained = registry.snapshot();
        assert_eq!(retained.len(), 1);
        tokio::time::timeout(Duration::from_secs(1), retained[0].stop())
            .await
            .expect("retiring sender was not joinable after owner cancellation");
        registry.clear();
    }

    struct DelayedHealthSupervisor;

    impl Actor for DelayedHealthSupervisor {
        type Msg = SupervisorMessage;
        type State = DelayedHealthArgs;
        type Arguments = DelayedHealthArgs;

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
            if let SupervisorMessage::GetHealth { reply } = message {
                *state.observed_timeout.lock().unwrap() = reply.get_timeout();
                if let Some(release) = &state.release {
                    release.notified().await;
                }
                let _ = reply.send(FragmentHealth {
                    fragment: state.fragment,
                    worker_count: 0,
                    worker_names: Vec::new(),
                    restarts: HashMap::new(),
                    last_reaped: 0,
                    shed_counts: (7, 11, 13),
                });
            }
            Ok(())
        }
    }

    struct DelayedHealthArgs {
        fragment: FragmentId,
        release: Option<Arc<Notify>>,
        observed_timeout: Arc<StdMutex<Option<Duration>>>,
    }

    #[derive(Default)]
    struct BlockingLoadStore {
        entered: AtomicBool,
        release: (StdMutex<bool>, Condvar),
    }

    impl BlockingLoadStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    impl KvStore for BlockingLoadStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct BlockingExclusiveLoadStore {
        attached: Arc<AtomicBool>,
        entered: AtomicBool,
        exited: AtomicBool,
        release: (StdMutex<bool>, Condvar),
    }

    impl Default for BlockingExclusiveLoadStore {
        fn default() -> Self {
            Self {
                attached: Arc::new(AtomicBool::new(false)),
                entered: AtomicBool::new(false),
                exited: AtomicBool::new(false),
                release: (StdMutex::new(false), Condvar::new()),
            }
        }
    }

    impl BlockingExclusiveLoadStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    impl KvStore for BlockingExclusiveLoadStore {
        fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            self.attached
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| StoreError("exclusive store is already attached".to_string()))?;
            Ok(exclusive_test_binding(&self.attached))
        }

        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            self.exited.store(true, Ordering::SeqCst);
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct ExclusiveTestLease(Arc<AtomicBool>);

    impl Drop for ExclusiveTestLease {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    fn exclusive_test_binding(attached: &Arc<AtomicBool>) -> StoreBinding {
        let lease = ExclusiveTestLease(attached.clone());
        StoreBinding::managed(move || drop(lease))
    }

    struct FailFirstLoadExclusiveStore {
        attached: Arc<AtomicBool>,
        loads: AtomicUsize,
    }

    impl Default for FailFirstLoadExclusiveStore {
        fn default() -> Self {
            Self {
                attached: Arc::new(AtomicBool::new(false)),
                loads: AtomicUsize::new(0),
            }
        }
    }

    impl KvStore for FailFirstLoadExclusiveStore {
        fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            self.attached
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| StoreError("exclusive store is already attached".to_string()))?;
            Ok(exclusive_test_binding(&self.attached))
        }

        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            if self.loads.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut key = 0u32.to_be_bytes().to_vec();
                key.push(0xff);
                Ok(vec![(
                    key,
                    VersionedValue::new(Vec::new(), 1, "writer".to_string()),
                )])
            } else {
                Ok(Vec::new())
            }
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct BlockingDispatcherSupervisor;

    impl Actor for BlockingDispatcherSupervisor {
        type Msg = SupervisorMessage;
        type State = BlockingDispatcherSupervisorState;
        type Arguments = BlockingDispatcherSupervisorState;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            match message {
                SupervisorMessage::GetDispatcher { reply } => {
                    state.requested.notify_one();
                    state.release.notified().await;
                    let _ = reply.send(state.replacement.clone());
                }
                SupervisorMessage::GetHealth { reply } => {
                    let _ = reply.send(FragmentHealth {
                        fragment: FragmentId(0),
                        worker_count: 0,
                        worker_names: Vec::new(),
                        restarts: HashMap::new(),
                        last_reaped: 0,
                        shed_counts: (0, 0, 0),
                    });
                }
                SupervisorMessage::Quiesce { reply } => {
                    let _ = reply.send(Ok(()));
                }
            }
            Ok(())
        }
    }

    struct BlockingDispatcherSupervisorState {
        replacement: ActorRef<KvMessage>,
        requested: Arc<Notify>,
        release: Arc<Notify>,
    }
}
