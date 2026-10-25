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

/// Approximate current ownership counts, readable without actor RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainStatus {
    phase: DrainPhase,
    active_requests: usize,
    active_replication_applies: usize,
    receive_apply_complete: bool,
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
}
impl Default for DrainOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl DrainOptions {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainReport {
    status: DrainStatus,
}
impl DrainReport {
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
    Timeout { status: DrainStatus },
    Local(NodeError),
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
        }
    }

    /// Close admission and wait for request completion, including cancelled
    /// waiters' actor work. Leaves peer transport/fragment actors alive; incoming
    /// replication and maintenance may still change data. This establishes no
    /// persistence or remote replication fence. Timeout/cancellation preserves
    /// owned work and can be retried. Call `finish_shutdown` for the full barrier.
    pub async fn drain_requests(&self, options: DrainOptions) -> Result<DrainReport, DrainError> {
        let deadline = options.deadline()?;
        let mut attempt = DrainAttempt::new(&self.operation_gate);
        self.begin_drain();
        let draining = async {
            let _coordinator = self.shutdown_state.lock().await;
            self.placement_tasks.shutdown().await;
            self.operation_gate.wait_for_drain().await;
            // Query page ownership survives coordinator cancellation. Keep a
            // conservative resource check as well as its retained gate slot.
            while self.query_admission.diagnostics().reserved_runtime_bytes != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.mutation_service.shutdown().await;
            self.embedded_operations.shutdown().await;
            let _ = self.operation_gate.phase.compare_exchange(
                NODE_QUIESCING,
                NODE_DRAINED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            DrainReport {
                status: self.drain_status(),
            }
        };
        let result = tokio::time::timeout_at(deadline, draining)
            .await
            .map_err(|_| DrainError::Timeout {
                status: self.drain_status(),
            });
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
        let result = match tokio::time::timeout_at(deadline, self.shutdown()).await {
            Err(_) => Err(DrainError::Timeout {
                status: self.drain_status(),
            }),
            Ok(Err(error)) => Err(DrainError::Local(error)),
            Ok(Ok(())) => Ok(DrainReport {
                status: self.drain_status(),
            }),
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
    }
    impl BlockingStore {
        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }
    impl KvStore for BlockingStore {
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
            self.entered.notify_one();
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
            self.writes.lock().unwrap().extend_from_slice(writes);
            Ok(())
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
                KvMessage::ApplyOp { .. } | KvMessage::QueryPage { .. }
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
            .send(SenderMsg::PeerConnected { peer_tx: peer })
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
                    entries: vec![SyncEntry {
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
        let report = node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(report.status().phase(), DrainPhase::Drained);
        assert!(!report.receive_apply_complete());
        let stopping = node.clone();
        let shutdown = tokio::spawn(async move {
            stopping
                .finish_shutdown(DrainOptions::default().with_timeout(Duration::from_millis(100)))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        assert!(matches!(
            shutdown.await.unwrap(),
            Err(DrainError::Timeout { .. })
        ));
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
        assert!(store.detached.load(Ordering::SeqCst));
        assert!(store
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|(_, value)| value.value == vec![7]));
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
