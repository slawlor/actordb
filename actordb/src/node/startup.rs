//! Owned startup, live progress, and cancellation-safe handoff/cleanup.
use super::status::{NodeLifecycle, ShutdownStage};
use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartupStage {
    Validating,
    BindingStorage,
    BindingTransport,
    InitializingFragments,
    StartingRuntime,
    Ready,
    CleaningUp,
    Cancelled,
    Failed,
}
impl StartupStage {
    fn code(self) -> u8 {
        self as u8
    }
    fn from_code(code: u8) -> Self {
        match code {
            1 => Self::BindingStorage,
            2 => Self::BindingTransport,
            3 => Self::InitializingFragments,
            4 => Self::StartingRuntime,
            5 => Self::Ready,
            6 => Self::CleaningUp,
            7 => Self::Cancelled,
            8 => Self::Failed,
            _ => Self::Validating,
        }
    }
}

/// Startup remains owned after a cancelled waiter or dropped handle.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeStartupSnapshot {
    pub initial_readiness_established: bool,
    pub node_id: String,
    pub stage: StartupStage,
    pub lifecycle: NodeLifecycle,
    pub initialized_fragments: usize,
    pub required_fragments: usize,
    pub cancellation_requested: bool,
    pub cleanup_complete: bool,
    pub failure: Option<String>,
    pub observed_at: std::time::SystemTime,
    pub last_transition_at: std::time::SystemTime,
    pub revision: u64,
    pub changed_during_collection: bool,
}

pub(super) struct LifecycleObservation {
    pub(super) established: AtomicBool,
    pub(super) desired_count: AtomicUsize,
    initial_pending: StdMutex<HashSet<FragmentId>>,
    pub(super) startup_stage: AtomicU8,
    pub(super) initialized: AtomicUsize,
    pub(super) required: AtomicUsize,
    pub(super) startup_complete: AtomicBool,
    ready_lifecycle: AtomicU8,
    pub(super) cancelled: AtomicBool,
    pub(super) cleanup_complete: AtomicBool,
    pub(super) shutdown_stage: AtomicU8,
    pub(super) revision: AtomicU64,
    changed_at_ms: AtomicU64,
    pub(super) failure: StdMutex<Option<String>>,
    pub(super) changes: Notify,
}
impl LifecycleObservation {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            startup_stage: AtomicU8::new(0),
            established: AtomicBool::new(false),
            desired_count: AtomicUsize::new(0),
            initial_pending: StdMutex::new(HashSet::new()),
            initialized: AtomicUsize::new(0),
            required: AtomicUsize::new(0),
            startup_complete: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            cleanup_complete: AtomicBool::new(false),
            ready_lifecycle: AtomicU8::new(1),
            shutdown_stage: AtomicU8::new(0),
            revision: AtomicU64::new(0),
            changed_at_ms: AtomicU64::new(crate::actor::now_ms()),
            failure: StdMutex::new(None),
            changes: Notify::new(),
        })
    }
    pub(super) fn changed(&self) {
        self.changed_at_ms
            .store(crate::actor::now_ms(), Ordering::Release);
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.changes.notify_waiters();
    }
    pub(super) fn startup(&self, stage: StartupStage) {
        self.startup_stage.store(stage.code(), Ordering::Release);
        self.changed();
    }
    pub(super) fn initialized_fragment(&self) {
        self.initialized.fetch_add(1, Ordering::AcqRel);
        self.changed();
    }
    pub(super) fn initial_boundary(&self, fragments: &[FragmentId]) {
        *self.initial_pending.lock().unwrap() = fragments.iter().copied().collect();
        self.desired_count.store(fragments.len(), Ordering::Release);
    }
    pub(super) fn published_serving(&self, fragment: FragmentId) {
        self.initial_pending.lock().unwrap().remove(&fragment);
        self.establish_if_ready();
    }
    pub(super) fn desired(&self, desired: &HashSet<FragmentId>) {
        self.desired_count.store(desired.len(), Ordering::Release);
        self.initial_pending
            .lock()
            .unwrap()
            .retain(|fragment| desired.contains(fragment));
        self.establish_if_ready();
    }
    fn establish_if_ready(&self) {
        if self.startup_complete.load(Ordering::Acquire)
            && self.initial_pending.lock().unwrap().is_empty()
        {
            self.established.store(true, Ordering::Release);
        }
    }
    pub(super) fn initialization_done(&self) {
        self.startup_complete.store(true, Ordering::Release);
        self.establish_if_ready();
    }
    pub(super) fn ready(&self, lifecycle: NodeLifecycle) {
        self.ready_lifecycle
            .store(lifecycle as u8, Ordering::Release);
        self.startup(StartupStage::Ready);
    }
    pub(super) fn shutdown(&self, stage: ShutdownStage) {
        self.shutdown_stage.store(stage as u8, Ordering::Release);
        self.changed();
    }
    pub(super) fn fail(&self, error: &str) {
        *self.failure.lock().unwrap() = Some(error.chars().take(1024).collect());
        self.changed();
    }
    pub(super) fn clear_failure(&self) {
        *self.failure.lock().unwrap() = None;
        self.changed();
    }
    pub(super) fn observed_shutdown(&self) -> ShutdownStage {
        match self.shutdown_stage.load(Ordering::Acquire) {
            1 => ShutdownStage::ReplicationFence,
            2 => ShutdownStage::ReplicationDrain,
            3 => ShutdownStage::StoppingTransport,
            4 => ShutdownStage::QuiescingFragments,
            5 => ShutdownStage::StoppingFragments,
            6 => ShutdownStage::Stopped,
            _ => ShutdownStage::NotStarted,
        }
    }
    pub(super) fn snapshot(&self, node_id: &str) -> NodeStartupSnapshot {
        let revision = self.revision.load(Ordering::Acquire);
        let stage = StartupStage::from_code(self.startup_stage.load(Ordering::Acquire));
        let cancelled = self.cancelled.load(Ordering::Acquire);
        let failure = self.failure.try_lock().ok().and_then(|error| error.clone());
        let lifecycle = if stage == StartupStage::Failed {
            NodeLifecycle::Failed
        } else if stage == StartupStage::Cancelled {
            NodeLifecycle::Stopped
        } else if failure.is_some() {
            NodeLifecycle::Blocked
        } else if cancelled {
            NodeLifecycle::Draining
        } else if stage == StartupStage::Ready {
            match self.ready_lifecycle.load(Ordering::Acquire) {
                1 => NodeLifecycle::Initializing,
                3 => NodeLifecycle::Serving,
                7 => NodeLifecycle::Blocked,
                8 => NodeLifecycle::Failed,
                9 => NodeLifecycle::Unknown,
                _ => NodeLifecycle::Recovering,
            }
        } else if stage == StartupStage::Validating {
            NodeLifecycle::Starting
        } else {
            NodeLifecycle::Initializing
        };
        NodeStartupSnapshot {
            initial_readiness_established: self.established.load(Ordering::Acquire),
            node_id: node_id.to_owned(),
            stage,
            lifecycle,
            initialized_fragments: self.initialized.load(Ordering::Acquire),
            required_fragments: self.required.load(Ordering::Acquire),
            cancellation_requested: cancelled,
            cleanup_complete: self.cleanup_complete.load(Ordering::Acquire),
            failure,
            observed_at: std::time::SystemTime::now(),
            last_transition_at: std::time::UNIX_EPOCH
                + Duration::from_millis(self.changed_at_ms.load(Ordering::Acquire)),
            revision,
            changed_during_collection: revision != self.revision.load(Ordering::Acquire),
        }
    }
}

/// An owned startup manager. `wait` can be cancelled and resumed. Dropping or
/// cancelling this handle requests cleanup without aborting initialization.
pub struct NodeStartup {
    node_id: String,
    observation: Arc<LifecycleObservation>,
    outcome: Arc<StdMutex<Option<Result<Node, NodeError>>>>,
    cancel: watch::Sender<bool>,
    finished: Arc<AtomicBool>,
}
impl NodeStartup {
    pub fn snapshot(&self) -> NodeStartupSnapshot {
        let mut snapshot = self.observation.snapshot(&self.node_id);
        if snapshot.stage == StartupStage::Ready && !snapshot.cancellation_requested {
            if let Ok(outcome) = self.outcome.try_lock() {
                if let Some(Ok(node)) = outcome.as_ref() {
                    snapshot.lifecycle = node.status_snapshot().lifecycle;
                }
            }
        }
        snapshot
    }
    pub fn cancel(&self) {
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        self.observation.cancelled.store(true, Ordering::Release);
        self.observation.changed();
        self.cancel.send_replace(true);
    }
    pub async fn changed(&self, after_revision: u64) -> NodeStartupSnapshot {
        loop {
            let changed = self.observation.changes.notified();
            let snapshot = self.snapshot();
            if snapshot.revision != after_revision || self.finished.load(Ordering::Acquire) {
                return snapshot;
            }
            changed.await;
        }
    }
    /// Move the fully constructed node out only in the final synchronous poll;
    /// a cancelled waiting future leaves ownership with the startup manager.
    pub async fn wait(&mut self) -> Result<Node, NodeError> {
        loop {
            let changed = self.observation.changes.notified();
            if !self.observation.cancelled.load(Ordering::Acquire) {
                if let Some(result) = self.outcome.lock().unwrap().take() {
                    self.finished.store(true, Ordering::Release);
                    self.observation.changes.notify_waiters();
                    return result;
                }
            }
            if self.finished.load(Ordering::Acquire) {
                return Err(NodeError::Actor(
                    "startup was cancelled or its outcome was already taken".into(),
                ));
            }
            changed.await;
        }
    }
}
impl Drop for NodeStartup {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Acquire) {
            self.cancel();
        }
    }
}

impl Node {
    /// Start owned initialization immediately and expose progress before any
    /// socket, store binding or actor tree is ready. Requires a Tokio runtime.
    pub fn begin_start(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
    ) -> NodeStartup {
        Self::begin_start_with_options(node_id, listen_addr, config, NodeRuntimeOptions::default())
    }
    pub fn begin_start_with_options(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
        options: NodeRuntimeOptions,
    ) -> NodeStartup {
        let observation = LifecycleObservation::new();
        let outcome = Arc::new(StdMutex::new(None));
        let finished = Arc::new(AtomicBool::new(false));
        let (cancel, mut cancelled) = watch::channel(false);
        let handle = NodeStartup {
            node_id: node_id.clone(),
            observation: observation.clone(),
            outcome: outcome.clone(),
            cancel,
            finished: finished.clone(),
        };
        tokio::spawn(async move {
            let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                Self::start_observed(node_id, listen_addr, config, options, observation.clone()),
            ))
            .await
            .unwrap_or_else(|_| {
                Err(NodeError::UnknownOutcome(
                    "startup manager panicked; cleanup completion is unverified".into(),
                ))
            });
            if let Err(error) = &result {
                let stage =
                    StartupStage::from_code(observation.startup_stage.load(Ordering::Acquire));
                if matches!(
                    stage,
                    StartupStage::Validating
                        | StartupStage::BindingStorage
                        | StartupStage::BindingTransport
                ) || observation.observed_shutdown() == ShutdownStage::Stopped
                {
                    observation.cleanup_complete.store(true, Ordering::Release);
                }
                observation.fail(&error.to_string());
                observation.startup(StartupStage::Failed);
            }
            *outcome.lock().unwrap() = Some(result);
            observation.changes.notify_waiters();
            loop {
                let changed = observation.changes.notified();
                if finished.load(Ordering::Acquire) {
                    return;
                }
                if *cancelled.borrow() {
                    break;
                }
                tokio::select! { _ = changed => {}, _ = cancelled.changed() => {} }
            }
            let result = outcome.lock().unwrap().take();
            let cleanup_succeeded = if let Some(Ok(node)) = result {
                observation.startup(StartupStage::CleaningUp);
                loop {
                    match node.shutdown().await {
                        Ok(()) => break,
                        Err(error) => {
                            observation.fail(&error.to_string());
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                true
            } else {
                observation.cleanup_complete.load(Ordering::Acquire)
            };
            if cleanup_succeeded {
                observation.clear_failure();
                observation.cleanup_complete.store(true, Ordering::Release);
                observation.startup(StartupStage::Cancelled);
            }
            finished.store(true, Ordering::Release);
            observation.changes.notify_waiters();
        });
        handle
    }
}
