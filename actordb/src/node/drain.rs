// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! One-way application request draining and bounded local shutdown.
//!
//! Request draining leaves peer transport and fragment actors alive. Only
//! successful full shutdown proves that incoming replication apply has ended.

use super::lifecycle::{NODE_DRAINED, NODE_QUIESCING, NODE_RUNNING, NODE_STOPPED};
use super::*;

/// Observable node request lifecycle. Draining cannot be reversed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DrainPhase {
    Running,
    Draining,
    Drained,
    Stopped,
}

/// Identity of a node's one-way application admission cutoff. All retries
/// refer to the same incarnation; acceptance is the gate's successful recheck.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DrainCutoff {
    pub node_boot_id: u64,
}

/// Requested local boundary. Remote replication is a separate contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DrainDurability {
    /// Wait for execution and response ownership only.
    Applied,
    /// Also flush every local fragment through its configured store contract.
    #[default]
    ConfiguredStore,
    /// Require an explicit stable fence on every local fragment.
    StableCommitted,
}

/// Actual local evidence returned by a fragment's persistence fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LocalDurability {
    MemoryApplied,
    StoreCommitted,
    StableCommitted,
}

/// One completed local fence, captured after cutoff application work finishes.
/// The revision is an observed cache floor, not a global snapshot or writer lease.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct FragmentDrainFence {
    pub fragment: FragmentId,
    pub actor_generation: u64,
    pub revision: Option<u64>,
    pub durability: LocalDurability,
}

/// Approximate current ownership counts, readable without actor RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainStatus {
    phase: DrainPhase,
    active_requests: usize,
    active_replication_applies: usize,
    receive_apply_complete: bool,
    cutoff: Option<DrainCutoff>,
    active_persistence_fences: usize,
    active_storage_jobs: usize,
    awaiting_responses: usize,
}

/// Cumulative ownership/admission and bounded drain call observations.
/// Request counters count gate slots, not distinct client RPCs or successful writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainDiagnostics {
    accepted_requests: u64,
    completed_requests: u64,
    rejected_requests: u64,
    started_drains: u64,
    completed_drains: u64,
    timed_out_drains: u64,
    failed_drains: u64,
    cancelled_drains: u64,
    total_drain_duration: Duration,
}
impl DrainDiagnostics {
    pub fn accepted_requests(&self) -> u64 {
        self.accepted_requests
    }
    pub fn completed_requests(&self) -> u64 {
        self.completed_requests
    }
    pub fn rejected_requests(&self) -> u64 {
        self.rejected_requests
    }
    pub fn started_drains(&self) -> u64 {
        self.started_drains
    }
    pub fn completed_drains(&self) -> u64 {
        self.completed_drains
    }
    pub fn timed_out_drains(&self) -> u64 {
        self.timed_out_drains
    }
    pub fn failed_drains(&self) -> u64 {
        self.failed_drains
    }
    pub fn cancelled_drains(&self) -> u64 {
        self.cancelled_drains
    }
    pub fn total_drain_duration(&self) -> Duration {
        self.total_drain_duration
    }
}

struct DrainAttempt<'a> {
    gate: &'a OperationGate,
    started: std::time::Instant,
    reported: bool,
}
impl<'a> DrainAttempt<'a> {
    fn new(gate: &'a OperationGate) -> Self {
        gate.drain_started.fetch_add(1, Ordering::Relaxed);
        Self {
            gate,
            started: std::time::Instant::now(),
            reported: false,
        }
    }
    fn report(&mut self, result: &Result<DrainReport, DrainError>) {
        match result {
            Ok(_) => &self.gate.drain_completed,
            Err(DrainError::Timeout { .. }) => &self.gate.drain_timed_out,
            Err(_) => &self.gate.drain_failed,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.reported = true;
    }
}
impl Drop for DrainAttempt<'_> {
    fn drop(&mut self) {
        if !self.reported {
            self.gate.drain_cancelled.fetch_add(1, Ordering::Relaxed);
        }
        let nanos = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let mut total = self.gate.drain_duration_nanos.load(Ordering::Relaxed);
        while let Err(observed) = self.gate.drain_duration_nanos.compare_exchange_weak(
            total,
            total.saturating_add(nanos),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            total = observed;
        }
    }
}
impl DrainStatus {
    pub fn awaiting_responses(&self) -> usize {
        self.awaiting_responses
    }
    pub fn cutoff(&self) -> Option<DrainCutoff> {
        self.cutoff
    }
    /// Local drain fences still owned through their actual actor/store outcome.
    pub fn active_persistence_fences(&self) -> usize {
        self.active_persistence_fences
    }
    /// Active synchronous storage calls, including callers whose waiter left.
    pub fn active_storage_jobs(&self) -> usize {
        self.active_storage_jobs
    }
    pub fn phase(&self) -> DrainPhase {
        self.phase
    }
    /// Admitted request/actor ownership slots. A batch can own several slots.
    pub fn active_requests(&self) -> usize {
        self.active_requests
    }
    /// Decoded replication frames retained through sender/actor processing.
    pub fn active_replication_applies(&self) -> usize {
        self.active_replication_applies
    }
    /// True only after full shutdown joined receive/apply ownership.
    pub fn receive_apply_complete(&self) -> bool {
        self.receive_apply_complete
    }
}

/// One absolute timeout covering every stage of the call.
#[derive(Clone, Debug, bon::Builder)]
pub struct DrainOptions {
    #[builder(default = Duration::from_secs(30))]
    timeout: Duration,
    #[builder(default)]
    durability: DrainDurability,
}
impl Default for DrainOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl DrainOptions {
    pub fn with_durability(mut self, durability: DrainDurability) -> Self {
        self.durability = durability;
        self
    }
    pub fn durability(&self) -> DrainDurability {
        self.durability
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    fn deadline(&self) -> Result<Instant, DrainError> {
        Instant::now()
            .checked_add(self.timeout)
            .filter(|_| !self.timeout.is_zero())
            .ok_or_else(|| {
                DrainError::InvalidOptions("timeout must be nonzero and representable".into())
            })
    }
}

/// Evidence of the completed local boundary; contains no peer ACK guarantee.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainReport {
    status: DrainStatus,
    requested_durability: DrainDurability,
    local_fences: Vec<FragmentDrainFence>,
}
impl DrainReport {
    pub fn requested_durability(&self) -> DrainDurability {
        self.requested_durability
    }
    /// Actual per-fragment local persistence evidence. An execution-only drain
    /// has no persistence evidence; local shutdown alone never proves peer ACKs.
    pub fn local_fences(&self) -> &[FragmentDrainFence] {
        &self.local_fences
    }
    pub fn status(&self) -> DrainStatus {
        self.status
    }
    /// Whether actors, transport and local store ownership have been released.
    pub fn shutdown_completed(&self) -> bool {
        self.status.phase == DrainPhase::Stopped
    }
    /// Whether incoming receive/apply work has ended, including detached RPCs.
    pub fn receive_apply_complete(&self) -> bool {
        self.status.receive_apply_complete
    }
}

/// A bounded call can be retried with a fresh budget. Admission stays closed.
#[derive(Debug)]
#[non_exhaustive]
pub enum DrainError {
    InvalidOptions(String),
    Timeout {
        status: DrainStatus,
    },
    Local(NodeError),
    /// A persistence outcome failed. Admission stays closed and retry resumes
    /// the same selected durability rather than weakening its requirements.
    Persistence {
        fragment: FragmentId,
        message: String,
    },
    IncompatibleRetry(String),
}
impl std::fmt::Display for DrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidOptions(error) => write!(f, "invalid drain options: {error}"),
            Self::Timeout { status } => write!(
                f,
                "drain timed out during {:?}: {} request slots and {} replication frames remain",
                status.phase(),
                status.active_requests(),
                status.active_replication_applies()
            ),
            Self::Local(error) => write!(f, "local shutdown failed: {error}"),
            Self::Persistence { fragment, message } => {
                write!(
                    f,
                    "drain persistence fence for fragment {fragment} failed: {message}"
                )
            }
            Self::IncompatibleRetry(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for DrainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Local(error) => Some(error),
            _ => None,
        }
    }
}

impl Node {
    /// Application sessions retain this slot through response send or failure.
    pub(crate) fn admit_client_request(&self) -> Result<OperationPermit, NodeError> {
        self.admit_operation()
    }
    pub(crate) fn admit_client_request_for_table(
        &self,
        table: &str,
    ) -> Result<OperationPermit, NodeError> {
        if !self.table_metrics.enabled() {
            return self.admit_client_request();
        }
        match TableId::new(table) {
            Ok(table) => self.admit_table_operation(table.name()),
            Err(_) => self.admit_client_request(),
        }
    }
    /// Observe cumulative slot outcomes and elapsed bounded drain attempts.
    pub fn drain_diagnostics(&self) -> DrainDiagnostics {
        let gate = &self.operation_gate;
        DrainDiagnostics {
            accepted_requests: gate.accepted.load(Ordering::Relaxed),
            completed_requests: gate.completed.load(Ordering::Relaxed),
            rejected_requests: gate.rejected.load(Ordering::Relaxed),
            started_drains: gate.drain_started.load(Ordering::Relaxed),
            completed_drains: gate.drain_completed.load(Ordering::Relaxed),
            timed_out_drains: gate.drain_timed_out.load(Ordering::Relaxed),
            failed_drains: gate.drain_failed.load(Ordering::Relaxed),
            cancelled_drains: gate.drain_cancelled.load(Ordering::Relaxed),
            total_drain_duration: Duration::from_nanos(
                gate.drain_duration_nanos.load(Ordering::Relaxed),
            ),
        }
    }
    /// Atomically close embedded, table, SQL, scan and forwarded request
    /// admission, and request placement freeze. Idempotent and synchronous.
    /// Already admitted work keeps its completion owner. No resume is supported.
    pub fn begin_drain(&self) -> DrainStatus {
        self.operation_gate.begin_shutdown();
        self.placement_tasks.request_shutdown();
        self.drain_status()
    }

    /// Read approximate counts without waiting for the shutdown coordinator.
    pub fn drain_status(&self) -> DrainStatus {
        let phase = match self.operation_gate.phase.load(Ordering::SeqCst) {
            NODE_RUNNING => DrainPhase::Running,
            NODE_STOPPED => DrainPhase::Stopped,
            NODE_DRAINED => DrainPhase::Drained,
            _ => DrainPhase::Draining,
        };
        DrainStatus {
            phase,
            active_requests: self.operation_gate.active.load(Ordering::SeqCst),
            active_replication_applies: self.receive_apply_gate.active.load(Ordering::SeqCst),
            receive_apply_complete: self.receive_apply_gate.phase.load(Ordering::SeqCst)
                == NODE_STOPPED,
            cutoff: (phase != DrainPhase::Running).then_some(DrainCutoff {
                node_boot_id: self.boot_id,
            }),
            active_persistence_fences: self.local_drain.active(),
            active_storage_jobs: self.storage_executor.active(),
            awaiting_responses: self
                .operation_gate
                .awaiting_responses
                .load(Ordering::SeqCst),
        }
    }

    /// Close admission and wait for request completion, including cancelled
    /// waiters' actor work. Leaves peer transport/fragment actors alive; incoming
    /// replication and maintenance may still change data. By default this also
    /// waits for a configured local persistence fence on every hosted fragment;
    /// choose `StableCommitted` to require explicit stable storage, or `Applied`
    /// for execution only. This does not certify peer replication. Timeout or
    /// cancellation retains exact in-progress fence ownership for retry.
    pub async fn drain_requests(&self, options: DrainOptions) -> Result<DrainReport, DrainError> {
        let deadline = options.deadline()?;
        let mut attempt = DrainAttempt::new(&self.operation_gate);
        self.begin_drain();
        let draining = async {
            let _coordinator = self.shutdown_state.lock().await;
            self.local_drain.select(options.durability)?;
            self.placement_tasks.shutdown().await;
            self.operation_gate.wait_for_drain().await;
            // Query page ownership survives coordinator cancellation. Keep a
            // conservative resource check as well as its retained gate slot.
            while self.query_admission.diagnostics().reserved_runtime_bytes != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.mutation_service.shutdown().await;
            self.embedded_operations.shutdown().await;
            let local_fences = self.local_drain.complete(self, options.durability).await?;
            let _ = self.operation_gate.phase.compare_exchange(
                NODE_QUIESCING,
                NODE_DRAINED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            Ok(DrainReport {
                status: self.drain_status(),
                requested_durability: options.durability,
                local_fences,
            })
        };
        let result = match tokio::time::timeout_at(deadline, draining).await {
            Ok(result) => result,
            Err(_) => Err(DrainError::Timeout {
                status: self.drain_status(),
            }),
        };
        attempt.report(&result);
        result
    }

    /// Alias for [`Self::drain_requests`].
    pub async fn drain(&self, options: DrainOptions) -> Result<DrainReport, DrainError> {
        self.drain_requests(options).await
    }

    /// Complete local shutdown with one bounded budget. Success joins incoming
    /// replication apply before releasing fragment/store ownership and reaches
    /// every fragment's configured persistence fence. Peer ACKs are separate:
    /// use [`Self::shutdown_with_replication_drain`] when that guarantee is needed.
    pub async fn finish_shutdown(&self, options: DrainOptions) -> Result<DrainReport, DrainError> {
        let deadline = options.deadline()?;
        let mut attempt = DrainAttempt::new(&self.operation_gate);
        self.begin_drain();
        let stopping = async {
            let report = self.drain_requests(options.clone()).await?;
            self.shutdown().await.map_err(DrainError::Local)?;
            Ok(DrainReport {
                status: self.drain_status(),
                ..report
            })
        };
        let result = match tokio::time::timeout_at(deadline, stopping).await {
            Err(_) => Err(DrainError::Timeout {
                status: self.drain_status(),
            }),
            Ok(result) => result,
        };
        attempt.report(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{StoreBinding, StoreError};
    use std::sync::{atomic::AtomicBool, Condvar};

    struct BlockingStore {
        entered: Notify,
        released: (StdMutex<bool>, Condvar),
        writes: StdMutex<Vec<(Vec<u8>, VersionedValue)>>,
        detached: Arc<AtomicBool>,
        calls: AtomicUsize,
        fail: AtomicBool,
        stable: bool,
    }
    impl BlockingStore {
        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }
    impl KvStore for BlockingStore {
        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }
        fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            let detached = self.detached.clone();
            Ok(StoreBinding::managed(move || {
                detached.store(true, Ordering::SeqCst);
            }))
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(StoreError("controlled persistence failure".into()));
            }
            self.writes.lock().unwrap().extend_from_slice(writes);
            Ok(())
        }
        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            if self.stable {
                crate::store::StoreCommitDurability::Stable
            } else {
                crate::store::StoreCommitDurability::Buffered
            }
        }
        fn stable_fence(&self) -> Result<crate::store::StoreCommitDurability, StoreError> {
            if self.fail.load(Ordering::SeqCst) {
                Err(StoreError("controlled stable fence failure".into()))
            } else {
                Ok(self.commit_durability())
            }
        }
    }
    struct ReleaseStore(Arc<BlockingStore>);
    impl Drop for ReleaseStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct ApplyGate {
        actual: ActorRef<KvMessage>,
        entered: Notify,
        release: Semaphore,
    }
    struct GatedDispatcher;
    impl ractor::Actor for GatedDispatcher {
        type Msg = KvMessage;
        type State = Arc<ApplyGate>;
        type Arguments = Arc<ApplyGate>;
        async fn pre_start(
            &self,
            _: ActorRef<KvMessage>,
            state: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            Ok(state)
        }
        async fn handle(
            &self,
            _: ActorRef<KvMessage>,
            message: KvMessage,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            let (message, retention) = message.into_retained_parts();
            if matches!(
                &message,
                KvMessage::ApplyOp { .. }
                    | KvMessage::QueryPage { .. }
                    | KvMessage::GetBatchRetained(_)
            ) {
                state.entered.notify_one();
                state.release.acquire().await.unwrap().forget();
            }
            state
                .actual
                .cast(message.with_optional_retention(retention))
                .map_err(|error| ractor::ActorProcessingErr::from(error.to_string()))
        }
    }
    struct ReleaseApply(Arc<ApplyGate>);
    impl Drop for ReleaseApply {
        fn drop(&mut self) {
            self.0.release.add_permits(256);
        }
    }
    async fn gated_node() -> (Arc<Node>, Arc<ApplyGate>, ActorRef<KvMessage>) {
        let node = Arc::new(
            Node::start(
                "drain-owner".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        let gate = Arc::new(ApplyGate {
            actual: node.router.route_for_fragment(FragmentId(0)).unwrap(),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let (actor, _) = GatedDispatcher::spawn(None, GatedDispatcher, gate.clone())
            .await
            .unwrap();
        node.router.insert(FragmentId(0), actor.clone());
        (node, gate, actor)
    }

    #[tokio::test]
    async fn cancelled_write_and_cancelled_drain_keep_actual_apply_owned() {
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let writing = node.clone();
        let writer = tokio::spawn(async move { writing.put("held".into(), vec![9]).await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(node.drain_status().active_requests() > 0);
        let mut draining = Box::pin(node.drain_requests(DrainOptions::default()));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut draining)
                .await
                .is_err()
        );
        drop(draining);
        assert_eq!(node.drain_diagnostics().cancelled_drains(), 1);
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        assert_eq!(node.drain_diagnostics().timed_out_drains(), 1);
        gate.release.add_permits(1);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        let contents = node.router.get_all().await.unwrap();
        assert_eq!(contents[&Node::qualify_key("held".into())].value, vec![9]);
        let diagnostics = node.drain_diagnostics();
        assert_eq!(
            diagnostics.accepted_requests(),
            diagnostics.completed_requests()
        );
        assert_eq!(diagnostics.completed_drains(), 1);
        assert!(diagnostics.total_drain_duration() > Duration::ZERO);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn decoded_replication_batch_outlives_stream_and_shutdown_waiter() {
        use super::super::test_support::{complete_test_handshake, write_test_envelope};
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let (stream, mut peer) = tokio::io::duplex(16 * 1024);
        let context = node.connection_context();
        assert!(
            node.tasks
                .spawn(async move {
                    handle_connection(context, Box::pin(stream), false, None, None).await;
                })
                .await
        );
        complete_test_handshake(&mut peer, "replica").await;
        write_test_envelope(
            &mut peer,
            Envelope {
                msg: Some(EnvelopeMsg::SessionStart(crate::proto::SessionStart {
                    session_id: 7,
                    fragment: 0,
                })),
            },
        )
        .await;
        write_test_envelope(
            &mut peer,
            Envelope {
                msg: Some(EnvelopeMsg::ReplicateBatch(crate::proto::ReplicateBatch {
                    session_id: 7,
                    batch_seq: 1,
                    fragment: 0,
                    required_ack_level: ReplicationAckLevel::Applied as i32,
                    ops: vec![crate::proto::ReplicateOp {
                        key: Node::qualify_key("received".into()),
                        value: vec![8],
                        timestamp_ms: 1,
                        node_id: "replica".into(),
                        ..Default::default()
                    }],
                })),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        drop(peer);
        assert_eq!(node.drain_status().active_replication_applies(), 1);
        assert!(matches!(
            node.finish_shutdown(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        assert_eq!(node.drain_status().active_replication_applies(), 1);
        assert!(!node.drain_status().receive_apply_complete());
        gate.release.add_permits(1);
        let report = node.finish_shutdown(DrainOptions::default()).await.unwrap();
        assert!(report.receive_apply_complete());
        assert_eq!(report.status().active_replication_applies(), 0);
        assert!(node.receive_apply_gate.try_enter().is_err());
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_sql_waiter_keeps_actor_page_owned_until_request_drain() {
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let querying = node.clone();
        let query = tokio::spawn(async move { querying.query("SELECT key FROM kv").await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        assert!(node.query_diagnostics().reserved_runtime_bytes > 0);
        assert!(node.drain_status().active_requests() > 0);
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        gate.release.add_permits(1);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(node.query_diagnostics().reserved_runtime_bytes, 0);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn validated_reconcile_data_keeps_receive_lease_through_actual_actor_apply() {
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let sender = spawn_node_sender(
            FragmentId(0),
            "replica".into(),
            SenderConfig::default(),
            SenderDeps::builder()
                .router(node.router.clone())
                .hlc(node.hlc.clone())
                .state_report(Arc::new(Mutex::new(SenderStateReport {
                    state: "disconnected".into(),
                    session_id: 0,
                    buffered: 0,
                    retained_bytes: 0,
                    aggregate_retained_bytes: 0,
                    in_flight: 0,
                    last_ack_durability: None,
                    ack_durability_mismatch: false,
                    reconcile_rounds: 0,
                    progress: Default::default(),
                })))
                .build(),
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        node.senders
            .lock()
            .await
            .insert((FragmentId(0), "replica".into()), sender.clone());
        let (peer, mut outbound) = mpsc::channel(16);
        sender
            .send(SenderMsg::PeerConnected {
                peer: crate::replication::ReplicationPeer::new(peer),
            })
            .unwrap();
        sender.send(SenderMsg::ReconcileNow).unwrap();
        let mut session = 0;
        let round = loop {
            let frame = tokio::time::timeout(Duration::from_secs(2), outbound.recv())
                .await
                .unwrap()
                .unwrap();
            match Envelope::decode(frame.as_slice()).unwrap().msg {
                Some(EnvelopeMsg::SessionStart(start)) => session = start.session_id,
                Some(EnvelopeMsg::ReconcileRequest(request)) => break request.round_id,
                other => panic!("unexpected initial sender frame: {other:?}"),
            }
        };
        let key = Node::qualify_key("fetched".into());
        let value = VersionedValue::new(vec![4], 1, "replica".into());
        let (connection_valid, validation) = oneshot::channel();
        sender
            .send(SenderMsg::ReconcileResponse {
                session_id: session,
                round_id: round,
                page_seq: 0,
                next_after_key: key.clone(),
                snapshot_revision: 1,
                snapshot_time_ms: 10,
                done: true,
                restart: false,
                summaries: vec![KeySummary {
                    key: key.clone(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: "replica".into(),
                    is_tombstone: false,
                    content_digest: value.content_digest().to_vec(),
                }],
                connection_valid,
            })
            .unwrap();
        assert!(validation.await.unwrap());
        let fetch = loop {
            let frame = tokio::time::timeout(Duration::from_secs(2), outbound.recv())
                .await
                .unwrap()
                .unwrap();
            if let Some(EnvelopeMsg::ReconcileFetch(fetch)) =
                Envelope::decode(frame.as_slice()).unwrap().msg
            {
                break fetch;
            }
        };
        let (connection_valid, validation) = oneshot::channel();
        let retention: Arc<dyn Send + Sync> =
            Arc::new(node.receive_apply_gate.try_enter().unwrap());
        sender
            .send_retained(
                SenderMsg::ReconcileData {
                    session_id: session,
                    round_id: round,
                    chunk_seq: fetch.chunk_seq,
                    page_seq: 0,
                    done: true,
                    entries: vec![ReconcileEntry {
                        key,
                        value: value.value,
                        timestamp_ms: 1,
                        logical_counter: 0,
                        node_id: "replica".into(),
                        expires_at_ms: 0,
                        is_tombstone: false,
                    }],
                    connection_valid,
                },
                retention,
            )
            .unwrap();
        assert!(validation.await.unwrap());
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        assert_eq!(node.drain_status().active_replication_applies(), 1);
        assert!(matches!(
            node.finish_shutdown(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        assert_eq!(node.drain_status().active_replication_applies(), 1);
        gate.release.add_permits(1);
        let report = node.finish_shutdown(DrainOptions::default()).await.unwrap();
        assert!(report.receive_apply_complete());
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn bounded_shutdown_preserves_blocked_persistence_and_retry_ownership() {
        let store = Arc::new(BlockingStore {
            entered: Notify::new(),
            released: (StdMutex::new(false), Condvar::new()),
            writes: StdMutex::new(Vec::new()),
            detached: Arc::new(AtomicBool::new(false)),
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            stable: false,
        });
        let _release = ReleaseStore(store.clone());
        let node = Arc::new(
            Node::start(
                "drain-store".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig {
                    store: Some(store.clone()),
                    flush_batch_size: 10_000,
                    flush_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::from_secs(3600),
                    ..NodeConfig::default()
                },
            )
            .await
            .unwrap(),
        );
        node.put("held".into(), vec![7]).await.unwrap();
        let stopping = node.clone();
        let shutdown = tokio::spawn(async move {
            stopping
                .drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(100)))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        assert!(matches!(
            shutdown.await.unwrap(),
            Err(DrainError::Timeout { .. })
        ));
        let status = node.drain_status();
        assert_eq!(status.phase(), DrainPhase::Draining);
        assert_eq!(status.active_persistence_fences(), 1);
        assert_eq!(status.active_storage_jobs(), 1);
        assert!(status.cutoff().is_some());
        assert!(matches!(
            node.finish_shutdown(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        assert_eq!(
            store.calls.load(Ordering::SeqCst),
            1,
            "retry must wait the same owned fence"
        );
        assert!(!store.detached.load(Ordering::SeqCst));
        assert_ne!(node.drain_status().phase(), DrainPhase::Stopped);
        assert!(matches!(
            node.put("late".into(), vec![1]).await,
            Err(NodeError::ShuttingDown)
        ));
        store.release();
        let report = node.finish_shutdown(DrainOptions::default()).await.unwrap();
        assert!(report.shutdown_completed());
        assert!(report.receive_apply_complete());
        assert_eq!(
            report.local_fences()[0].durability,
            LocalDurability::StoreCommitted
        );
        assert_eq!(report.status().active_storage_jobs(), 0);
        assert_eq!(
            node.finish_shutdown(DrainOptions::default()).await.unwrap(),
            report
        );
        assert!(store.detached.load(Ordering::SeqCst));
        assert!(store
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|(_, value)| value.value == vec![7]));
    }

    fn controlled_store(stable: bool) -> Arc<BlockingStore> {
        Arc::new(BlockingStore {
            entered: Notify::new(),
            released: (StdMutex::new(false), Condvar::new()),
            writes: StdMutex::new(Vec::new()),
            detached: Arc::new(AtomicBool::new(false)),
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            stable,
        })
    }

    #[tokio::test]
    async fn accepted_write_blocked_in_persistence_survives_waiter_loss_and_metrics_match() {
        let store = controlled_store(true);
        let _release = ReleaseStore(store.clone());
        let node = Arc::new(
            Node::start(
                "durable-owned".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig {
                    store: Some(store.clone()),
                    flush_batch_size: 1,
                    flush_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::from_secs(3600),
                    ..NodeConfig::default()
                },
            )
            .await
            .unwrap(),
        );
        node.enable_table_metrics(&TableId::new("USERS").unwrap())
            .unwrap();
        let baseline_completed = node.storage_diagnostics().completed_jobs;
        let writing = node.clone();
        let writer = tokio::spawn(async move {
            writing
                .table("USERS")
                .unwrap()
                .put("private-key", vec![9])
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        assert!(!writer.is_finished());
        let cutoff = node.begin_drain().cutoff();
        writer.abort();
        let _ = writer.await;
        assert!(node.drain_status().active_requests() > 0);
        let options = DrainOptions::default().with_durability(DrainDurability::StableCommitted);
        assert!(matches!(
            node.drain_requests(options.clone().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        let metrics = node.metrics_snapshot();
        assert_eq!(metrics.status.drain.cutoff(), cutoff);
        assert!(metrics.status.drain.active_requests() > 0);
        assert_eq!(metrics.status.tables[0].active, 1);
        assert_eq!(metrics.status.storage.active_jobs, 1);
        assert_eq!(
            metrics.status.storage.completed_jobs, baseline_completed,
            "write is still blocked"
        );
        assert!(!store.detached.load(Ordering::SeqCst));
        store.release();
        let report = node.drain_requests(options.clone()).await.unwrap();
        assert_eq!(report.status().active_requests(), 0);
        assert_eq!(
            report.local_fences()[0].durability,
            LocalDurability::StableCommitted
        );
        assert_eq!(node.status_snapshot().tables[0].active, 0);
        assert!(store
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|(_, value)| value.value == vec![9]));
        node.finish_shutdown(options).await.unwrap();
        assert!(store.detached.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_store_drain_retries_without_weakening_or_replaying_application_write() {
        let store = controlled_store(true);
        store.release();
        store.fail.store(true, Ordering::SeqCst);
        let node = Node::start(
            "drain-failure".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                store: Some(store.clone()),
                flush_batch_size: 10_000,
                flush_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap();
        node.table("named")
            .unwrap()
            .put("key", vec![6])
            .await
            .unwrap();
        let options = DrainOptions::default().with_durability(DrainDurability::StableCommitted);
        assert!(matches!(
            node.drain_requests(options.clone()).await,
            Err(DrainError::Persistence { .. })
        ));
        assert!(node.status_snapshot().local_persistence_failure.is_some());
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_durability(DrainDurability::Applied))
                .await,
            Err(DrainError::IncompatibleRetry(_))
        ));
        assert!(!store.detached.load(Ordering::SeqCst));
        store.fail.store(false, Ordering::SeqCst);
        let report = node.drain_requests(options.clone()).await.unwrap();
        assert_eq!(
            report.local_fences()[0].durability,
            LocalDurability::StableCommitted
        );
        assert_eq!(store.writes.lock().unwrap().len(), 1);
        assert!(node.status_snapshot().local_persistence_failure.is_none());
        node.finish_shutdown(options).await.unwrap();
    }

    #[tokio::test]
    async fn stable_drain_without_store_reports_memory_not_fabricated_commit() {
        let node = Node::start(
            "memory-boundary".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        node.table("named")
            .unwrap()
            .put("key", vec![1])
            .await
            .unwrap();
        assert!(
            matches!(node.drain_requests(DrainOptions::default().with_durability(DrainDurability::StableCommitted)).await,
            Err(DrainError::Persistence { message, .. }) if message.contains("MemoryApplied"))
        );
        node.shutdown().await.unwrap();
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queued_writes_and_reads_keep_real_actor_ownership_after_all_waiters_leave() {
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let writing = node.clone();
        let first = tokio::spawn(async move { writing.put("first".into(), vec![1]).await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        let mut queued = Vec::new();
        for index in 0..3 {
            let writing = node.clone();
            queued.push(tokio::spawn(async move {
                writing.put(format!("queued-{index}"), vec![2]).await
            }));
        }
        let reading = node.clone();
        let read = tokio::spawn(async move { reading.get("first".into()).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while node
                .embedded_operation_diagnostics()
                .active_mutation_completions()
                < 4
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        node.begin_drain();
        first.abort();
        read.abort();
        let _ = first.await;
        let _ = read.await;
        for task in queued {
            task.abort();
            let _ = task.await;
        }
        assert!(node.drain_status().active_requests() >= 4);
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        gate.release.add_permits(256);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        let rows = node.router.get_all().await.unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(node.drain_status().active_requests(), 0);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_sdk_admission_race_accounts_every_applied_write_and_repeated_shutdown() {
        let node = Arc::new(
            Node::start(
                "admission-race".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        node.put("before".into(), vec![1]).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(65));
        let mut writes = Vec::new();
        for index in 0..64 {
            let writing = node.clone();
            let starting = barrier.clone();
            writes.push(tokio::spawn(async move {
                starting.wait().await;
                writing.put(format!("racing-{index}"), vec![2]).await
            }));
        }
        barrier.wait().await;
        let cutoff = node.begin_drain().cutoff();
        let mut applied = 1;
        for write in writes {
            match write.await.unwrap() {
                Ok(()) => applied += 1,
                Err(NodeError::ShuttingDown | NodeError::Backpressure(_)) => {}
                other => panic!("unexpected raced result: {other:?}"),
            }
        }
        let (a, b) = tokio::join!(
            node.drain_requests(DrainOptions::default()),
            node.drain_requests(DrainOptions::default())
        );
        assert_eq!(a.unwrap().status().cutoff(), cutoff);
        assert_eq!(b.unwrap().status().cutoff(), cutoff);
        assert_eq!(node.router.get_all().await.unwrap().len(), applied);
        let counters = node.drain_diagnostics();
        assert_eq!(counters.accepted_requests(), counters.completed_requests());
        assert!(matches!(
            node.put("after".into(), vec![3]).await,
            Err(NodeError::ShuttingDown)
        ));
        let (a, b) = tokio::join!(
            node.finish_shutdown(DrainOptions::default()),
            node.finish_shutdown(DrainOptions::default())
        );
        assert!(a.unwrap().shutdown_completed());
        assert!(b.unwrap().shutdown_completed());
        node.shutdown().await.unwrap();
        assert_eq!(node.drain_status().cutoff(), cutoff);
    }

    #[tokio::test]
    async fn cancelled_sdk_read_is_still_counted_until_the_actor_really_finishes() {
        let (node, gate, actor) = gated_node().await;
        let _release = ReleaseApply(gate.clone());
        let reading = node.clone();
        let read = tokio::spawn(async move { reading.get("missing".into()).await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        read.abort();
        let _ = read.await;
        assert!(node.drain_status().active_requests() > 0);
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        gate.release.add_permits(1);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(node.drain_status().active_requests(), 0);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn accepted_forwarded_response_stays_owned_while_the_real_socket_is_blocked() {
        use super::super::test_support::{complete_test_handshake, write_test_envelope};
        let node = Arc::new(
            Node::start(
                "response-owner".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        node.put("wide".into(), vec![4; 4096]).await.unwrap();
        let (stream, mut peer) = tokio::io::duplex(256);
        let context = node.connection_context();
        assert!(
            node.tasks
                .spawn(async move {
                    handle_connection(context, Box::pin(stream), false, None, None).await;
                })
                .await
        );
        complete_test_handshake(&mut peer, "caller").await;
        write_test_envelope(
            &mut peer,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: Node::qualify_key("wide".into()),
                    req_id: 42,
                    ..Default::default()
                })),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.drain_status().awaiting_responses() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        assert!(node.metrics_snapshot().status.drain.awaiting_responses() > 0);
        drop(peer);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(node.drain_status().awaiting_responses(), 0);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
    }

    #[tokio::test]
    async fn post_cutoff_forwarded_read_cannot_register_a_relay_on_a_routing_node() {
        use super::super::test_support::{complete_test_handshake, write_test_envelope};
        let node = Arc::new(
            Node::start(
                "relay".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder()
                    .fragment_map(HashMap::from([(FragmentId(0), vec!["owner".into()])]))
                    .build(),
            )
            .await
            .unwrap(),
        );
        let (owner, mut owner_data, _owner_responses) = super::super::tests::test_peer_channel(8);
        node.peers.lock().await.insert("owner".into(), owner);
        let (stream, mut peer) = tokio::io::duplex(4096);
        let context = node.connection_context();
        assert!(
            node.tasks
                .spawn(async move {
                    handle_connection(context, Box::pin(stream), false, None, None).await;
                })
                .await
        );
        complete_test_handshake(&mut peer, "caller").await;
        node.begin_drain();
        let rejected = node.drain_diagnostics().rejected_requests();
        write_test_envelope(
            &mut peer,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: Node::qualify_key("key".into()),
                    req_id: 17,
                    ..Default::default()
                })),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.drain_diagnostics().rejected_requests() == rejected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(node.pending_relays.lock().unwrap().is_empty());
        assert!(owner_data.try_recv().is_err());
        node.drain_requests(DrainOptions::default()).await.unwrap();
        drop(peer);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
    }

    #[tokio::test]
    async fn admission_cutoff_rejects_races_tables_queries_and_invalid_options_are_inert() {
        let node = Node::start(
            "drain-admission".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::ZERO))
                .await,
            Err(DrainError::InvalidOptions(_))
        ));
        assert_eq!(node.drain_status().phase(), DrainPhase::Running);
        node.begin_drain();
        assert!(matches!(
            node.operation_gate.finish_enter_after_running_check(),
            Err(NodeError::ShuttingDown)
        ));
        assert_eq!(node.drain_status().active_requests(), 0);
        assert!(matches!(
            node.get("late".into()).await,
            Err(NodeError::ShuttingDown)
        ));
        assert!(matches!(
            node.table("named").unwrap().put("late", vec![1]).await,
            Err(NodeError::ShuttingDown)
        ));
        assert!(matches!(
            node.query("SELECT key FROM actordb").await,
            Err(crate::query::QueryExecutionError::ShuttingDown)
        ));
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
        assert_eq!(node.begin_drain().phase(), DrainPhase::Stopped);
    }
}
