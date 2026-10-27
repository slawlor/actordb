//! Admission and completion ownership for embedded public reads and mutations.
//!
//! Count/byte leases precede dispatcher enqueue, and survive cancelled waiters.
//! Accepted mutation futures remain owned through apply and replication fanout.
//! Explicit shutdown drains; last-owner Drop aborts as Node's best-effort fallback.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

use super::NodeError;

pub(super) const EMBEDDED_OPERATION_CAPACITY: usize = 128;
pub(super) const EMBEDDED_OPERATION_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
type CompletionJob = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Approximate current embedded request admission use across one Node.
/// Values include actor/owner work after its user waiter times out or cancels.
/// Cache, backend, replication, and returned dataset allocations are separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedOperationDiagnostics {
    active_requests: usize,
    retained_request_bytes: usize,
    active_mutation_completions: usize,
}
impl EmbeddedOperationDiagnostics {
    /// Accepted point mutations owned through actual apply/fanout completion.
    /// A batch may have several completions sharing one request lease.
    pub fn active_mutation_completions(&self) -> usize {
        self.active_mutation_completions
    }

    /// Admitted requests whose ownership has not yet been released.
    pub fn active_requests(&self) -> usize {
        self.active_requests
    }
    /// Conservative request-storage charge retained by those requests.
    pub fn retained_request_bytes(&self) -> usize {
        self.retained_request_bytes
    }
}

pub(super) struct RequestRetention {
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    _operation: Option<super::OperationPermit>,
    /// Only points from an already-admitted aggregate may await completion
    /// capacity. External request admission remains nonwaiting.
    pub(super) pace_completions: bool,
}

pub(super) struct EmbeddedOperations {
    accepting: AtomicBool,
    capacity: usize,
    byte_capacity: usize,
    count: Arc<Semaphore>,
    completions: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    jobs: mpsc::Sender<CompletionJob>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    manager: StdMutex<Option<JoinHandle<()>>>,
    shutdown_lock: Mutex<()>,
}
impl EmbeddedOperations {
    pub(super) fn new(capacity: usize, byte_capacity: usize) -> Arc<Self> {
        let (jobs, receiver) = mpsc::channel(capacity);
        let (stop, stop_rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(false);
        let manager = tokio::spawn(run_manager(receiver, stop_rx, done_tx));
        Arc::new(Self {
            accepting: AtomicBool::new(true),
            capacity,
            byte_capacity,
            count: Arc::new(Semaphore::new(capacity)),
            completions: Arc::new(Semaphore::new(capacity)),
            bytes: Arc::new(Semaphore::new(byte_capacity)),
            jobs,
            stop,
            done,
            manager: StdMutex::new(Some(manager)),
            shutdown_lock: Mutex::new(()),
        })
    }
    pub(super) fn diagnostics(&self) -> EmbeddedOperationDiagnostics {
        EmbeddedOperationDiagnostics {
            active_requests: self.capacity.saturating_sub(self.count.available_permits()),
            retained_request_bytes: self
                .byte_capacity
                .saturating_sub(self.bytes.available_permits()),
            active_mutation_completions: self
                .capacity
                .saturating_sub(self.completions.available_permits()),
        }
    }
    pub(super) fn queued(&self) -> usize {
        self.jobs
            .max_capacity()
            .saturating_sub(self.jobs.capacity())
    }
    #[cfg(test)]
    pub(super) fn admit(&self, bytes: usize) -> Result<Arc<RequestRetention>, NodeError> {
        self.admit_owned(bytes, None)
    }
    pub(super) fn admit_owned(
        &self,
        bytes: usize,
        operation: Option<super::OperationPermit>,
    ) -> Result<Arc<RequestRetention>, NodeError> {
        self.admit_request(bytes, operation, false)
    }

    pub(super) fn admit_batch_owned(
        &self,
        bytes: usize,
        operation: Option<super::OperationPermit>,
    ) -> Result<Arc<RequestRetention>, NodeError> {
        self.admit_request(bytes, operation, true)
    }

    fn admit_request(
        &self,
        bytes: usize,
        operation: Option<super::OperationPermit>,
        pace_completions: bool,
    ) -> Result<Arc<RequestRetention>, NodeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(NodeError::ShuttingDown);
        }
        let count =
            self.count.clone().try_acquire_owned().map_err(|_| {
                NodeError::Backpressure("embedded operation capacity exhausted".into())
            })?;
        let bytes = u32::try_from(bytes).map_err(|_| {
            NodeError::Backpressure("embedded request exceeds retained-byte budget".into())
        })?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| {
                NodeError::Backpressure("embedded request retained-byte budget exhausted".into())
            })?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(NodeError::ShuttingDown);
        }
        Ok(Arc::new(RequestRetention {
            _count: count,
            _bytes: bytes,
            _operation: operation,
            pace_completions,
        }))
    }
    pub(super) fn submit(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), NodeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(NodeError::ShuttingDown);
        }
        // A successful nonwaiting send transfers ownership before the caller
        // can hit another await. Shutdown closes and drains this exact queue.
        let completion = self.completions.clone().try_acquire_owned().map_err(|_| {
            NodeError::Backpressure("embedded mutation completion capacity exhausted".into())
        })?;
        self.transfer(job, completion)
    }

    /// Pace only fanout belonging to a bounded, already-admitted batch. Each
    /// waiter owns no completion permit until FIFO admission succeeds; batches
    /// therefore cannot hold one another's permits while waiting for more.
    pub(super) async fn submit_paced(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        timeout: std::time::Duration,
    ) -> Result<(), NodeError> {
        let mut stopped = self.stop.subscribe();
        if !self.accepting.load(Ordering::Acquire) || *stopped.borrow() {
            return Err(NodeError::ShuttingDown);
        }
        let completion = tokio::select! {
            biased;
            _ = stopped.changed() => return Err(NodeError::ShuttingDown),
            permit = tokio::time::timeout(timeout, self.completions.clone().acquire_owned()) => {
                match permit {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => return Err(NodeError::ShuttingDown),
                    Err(_) => return Err(NodeError::Backpressure(
                        "embedded batch completion admission timed out".into(),
                    )),
                }
            }
        };
        self.transfer(job, completion)
    }

    fn transfer(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        completion: OwnedSemaphorePermit,
    ) -> Result<(), NodeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(NodeError::ShuttingDown);
        }
        // The queue and total completion capacity are equal. Owning this
        // permit leaves room for its queue entry, including the tiny interval
        // between a previous reply wakeup and that job's actual permit drop.
        self.jobs
            .try_send(Box::pin(async move {
                let _completion = completion;
                job.await;
            }))
            .map_err(|_| {
                NodeError::Backpressure("embedded mutation completion queue unavailable".into())
            })
    }
    pub(super) fn request_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.stop.send_replace(true);
    }
    pub(super) async fn shutdown(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
        self.request_shutdown();
        let mut done = self.done.clone();
        while !*done.borrow() {
            if done.changed().await.is_err() {
                break;
            }
        }
        let manager = self.manager.lock().unwrap().take();
        if let Some(manager) = manager {
            if let Err(error) = manager.await {
                tracing::warn!(%error, "embedded operation completion owner failed");
            }
        }
    }
}
impl Drop for EmbeddedOperations {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Some(manager) = self.manager.lock().unwrap().take() {
            manager.abort();
        }
    }
}

async fn run_manager(
    mut jobs: mpsc::Receiver<CompletionJob>,
    mut stop: watch::Receiver<bool>,
    done: watch::Sender<bool>,
) {
    struct PublishDone(watch::Sender<bool>);
    impl Drop for PublishDone {
        fn drop(&mut self) {
            self.0.send_replace(true);
        }
    }
    let _done = PublishDone(done);
    let mut owned = JoinSet::new();
    let mut draining = false;
    loop {
        tokio::select! {
            _ = stop.changed(), if !draining => {
                jobs.close();
                draining = true;
            }
            job = jobs.recv() => {
                match job {
                    Some(job) => { owned.spawn(job); }
                    None => break,
                }
            }
            result = owned.join_next(), if !owned.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::warn!(%error, "embedded mutation completion failed");
                }
            }
        }
    }
    while let Some(result) = owned.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "embedded mutation completion failed during drain");
        }
    }
}

pub(super) fn single_request_bytes(key: &String, value: Option<&Vec<u8>>) -> usize {
    // Original storage, one exact ApplyOp clone, routing/correlation key clones,
    // and fixed request/result headers. Charge capacity even for empty buffers.
    std::mem::size_of::<crate::actor::ReplicatedOp>()
        .saturating_add(std::mem::size_of::<crate::actor::VersionedValue>())
        .saturating_add(4 * std::mem::size_of::<String>())
        .saturating_add(key.capacity().saturating_mul(3))
        .saturating_add(value.map_or(0, |value| value.capacity().saturating_mul(2)))
        .saturating_add(2 * super::MAX_WIRE_NODE_ID_BYTES)
}
/// Concern tickets retain one additional exact operation for reset recovery.
pub(super) fn single_concern_request_bytes(key: &String, value: Option<&Vec<u8>>) -> usize {
    single_request_bytes(key, value)
        .saturating_add(std::mem::size_of::<crate::actor::ReplicatedOp>())
        .saturating_add(key.capacity())
        .saturating_add(value.map_or(0, Vec::capacity))
        .saturating_add(super::MAX_WIRE_NODE_ID_BYTES)
}
pub(super) fn batch_put_options_request_bytes(
    values: &Vec<(String, Vec<u8>, Option<std::time::Duration>)>,
) -> usize {
    std::mem::size_of::<Vec<(String, Vec<u8>, Option<std::time::Duration>)>>()
        .saturating_add(values.capacity().saturating_mul(std::mem::size_of::<(
            String,
            Vec<u8>,
            Option<std::time::Duration>,
        )>()))
        .saturating_add(values.iter().fold(0usize, |bytes, (key, value, _)| {
            bytes.saturating_add(single_request_bytes(key, Some(value)))
        }))
}
pub(super) fn batch_get_request_bytes(keys: &Vec<String>) -> usize {
    std::mem::size_of::<Vec<String>>()
        .saturating_add(
            keys.capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(keys.iter().fold(0usize, |bytes, key| {
            bytes.saturating_add(single_request_bytes(key, None))
        }))
}

pub(super) fn reject_batch<T, R>(items: Vec<T>, error: NodeError) -> Vec<Result<R, NodeError>> {
    items
        .into_iter()
        .map(|_| {
            Err(match &error {
                NodeError::ShuttingDown => NodeError::ShuttingDown,
                NodeError::Backpressure(reason) => NodeError::Backpressure(reason.clone()),
                _ => NodeError::Actor(error.to_string()),
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "embedded_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use ractor::{Actor, ActorProcessingErr, ActorRef};
    use std::time::Duration;
    use tokio::sync::Notify;

    use super::*;
    use crate::actor::KvMessage;
    use crate::node::{Node, NodeConfig, NodeRuntimeOptions};
    use crate::shard::FragmentId;

    struct Gate {
        actual: ActorRef<KvMessage>,
        entered: Notify,
        release: Semaphore,
        received: AtomicUsize,
    }
    struct GatedDispatcher;
    impl Actor for GatedDispatcher {
        type Msg = KvMessage;
        type State = Arc<Gate>;
        type Arguments = Arc<Gate>;
        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            gate: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(gate)
        }
        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            message: Self::Msg,
            gate: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if matches!(
                &message,
                KvMessage::ApplyOp { .. } | KvMessage::GetBatchRetained(_)
            ) {
                gate.received.fetch_add(1, Ordering::SeqCst);
                gate.entered.notify_one();
                gate.release.acquire().await.unwrap().forget();
            }
            gate.actual
                .cast(message)
                .map_err(|error| ActorProcessingErr::from(error.to_string()))
        }
    }
    struct ReleaseGate(Arc<Gate>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release.add_permits(256);
        }
    }
    async fn gated_node(
        runtime: NodeRuntimeOptions,
    ) -> (Arc<Node>, Arc<Gate>, ActorRef<KvMessage>) {
        let node = Arc::new(
            Node::start_with_options(
                "embedded-owner".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
                runtime,
            )
            .await
            .unwrap(),
        );
        let actual = node.router.route_for_fragment(FragmentId(0)).unwrap();
        let gate = Arc::new(Gate {
            actual,
            entered: Notify::new(),
            release: Semaphore::new(0),
            received: AtomicUsize::new(0),
        });
        let (actor, _) = GatedDispatcher::spawn(None, GatedDispatcher, gate.clone())
            .await
            .unwrap();
        node.router.insert(FragmentId(0), actor.clone());
        (node, gate, actor)
    }
    async fn cleanup(node: &Node, gate: &Gate, actor: &ActorRef<KvMessage>) {
        node.router.insert(FragmentId(0), gate.actual.clone());
        actor.stop(None);
        actor.wait(None).await.unwrap();
        node.shutdown().await.unwrap();
    }

    #[test]
    fn unknown_mutation_outcome_survives_peer_wire_and_write_concern_mapping() {
        let result = super::super::mutation_result(
            "key".into(),
            0,
            Err(NodeError::UnknownOutcome("still owned".into())),
        );
        assert_eq!(
            result.status,
            crate::proto::MutationStatus::UnknownOutcome as i32
        );
        super::super::validate_mutation_results(&["key".into()], std::slice::from_ref(&result))
            .unwrap();
        assert!(matches!(
            super::super::decode_mutation_result(result),
            Err(NodeError::UnknownOutcome(_))
        ));
        let error: crate::write_concern::WriteConcernError =
            NodeError::UnknownOutcome("still owned".into()).into();
        assert!(matches!(
            error.into_node_error(),
            NodeError::UnknownOutcome(_)
        ));
    }

    #[tokio::test]
    async fn cancelled_embedded_read_keeps_admission_while_dispatcher_owns_its_key() {
        let (node, gate, actor) = gated_node(
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(1)
                .build(),
        )
        .await;
        let _release = ReleaseGate(gate.clone());
        let reading = node.clone();
        let request = tokio::spawn(async move { reading.get("held".into()).await });
        gate.entered.notified().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(matches!(
            node.get("excess".into()).await,
            Err(NodeError::Backpressure(_))
        ));
        assert_eq!(gate.received.load(Ordering::SeqCst), 1);
        gate.release.add_permits(1);
        let released = tokio::time::timeout(
            Duration::from_secs(2),
            node.embedded_operations.count.acquire(),
        )
        .await
        .unwrap()
        .unwrap();
        drop(released);
        gate.release.add_permits(1);
        assert!(node.get("after".into()).await.unwrap().is_none());
        cleanup(&node, &gate, &actor).await;
    }

    #[tokio::test]
    async fn timed_out_mutation_remains_owned_and_shutdown_waits_for_actual_apply() {
        let (node, gate, actor) = gated_node(
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(1)
                .local_work_timeout(Duration::from_millis(20))
                .build(),
        )
        .await;
        let _release = ReleaseGate(gate.clone());
        let writing = node.clone();
        let request = tokio::spawn(async move { writing.put("held".into(), vec![1]).await });
        gate.entered.notified().await;
        assert!(matches!(
            request.await.unwrap(),
            Err(NodeError::UnknownOutcome(_))
        ));
        // The public request lease and the internal apply/completion lease
        // each own a gate slot after the waiter times out. Neither may be
        // released until the blocked actor actually completes.
        let held_owners = node.operation_gate.active.load(Ordering::SeqCst);
        assert_eq!(held_owners, 2);
        assert!(matches!(
            node.put("excess".into(), vec![2]).await,
            Err(NodeError::Backpressure(_))
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.operation_gate.active.load(Ordering::SeqCst) != held_owners {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(gate.received.load(Ordering::SeqCst), 1);
        let mut shutdown = Box::pin(node.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );
        assert_eq!(
            node.operation_gate.active.load(Ordering::SeqCst),
            held_owners
        );
        gate.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.operation_gate.active.load(Ordering::SeqCst), 0);
        assert_eq!(gate.received.load(Ordering::SeqCst), 1);
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_embedded_mutation_still_applies_and_replicates() {
        let (node, gate, actor) = gated_node(
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(1)
                .build(),
        )
        .await;
        let _release = ReleaseGate(gate.clone());
        let peer = Node::start(
            "embedded-replica".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        peer.connect(node.addr()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while node
                .sender_report(FragmentId(0), "embedded-replica")
                .await
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let writing = node.clone();
        let request = tokio::spawn(async move { writing.put("cancelled".into(), vec![7]).await });
        gate.entered.notified().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(matches!(
            node.put("excess".into(), vec![8]).await,
            Err(NodeError::Backpressure(_))
        ));
        gate.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if peer
                    .get("cancelled".into())
                    .await
                    .unwrap()
                    .is_some_and(|value| value.value == vec![7])
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            node.next_seq.lock().await.to_owned(),
            1,
            "rejected work must not consume a stamp"
        );
        cleanup(&node, &gate, &actor).await;
        peer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn timed_out_batch_cannot_accumulate_unbounded_mutation_completion_jobs() {
        let (node, gate, actor) = gated_node(
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(1)
                .local_work_timeout(Duration::from_millis(20))
                .build(),
        )
        .await;
        let _release = ReleaseGate(gate.clone());
        let writing = node.clone();
        let batch = tokio::spawn(async move {
            writing
                .put_multi(
                    (0..65)
                        .map(|index| (format!("key-{index}"), vec![1]))
                        .collect(),
                )
                .await
        });
        gate.entered.notified().await;
        let results = tokio::time::timeout(Duration::from_secs(2), batch)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(results.len(), 65);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(NodeError::UnknownOutcome(_))))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(NodeError::Backpressure(_))))
                .count(),
            64
        );
        assert_eq!(gate.received.load(Ordering::SeqCst), 1);
        let diagnostics = node.embedded_operation_diagnostics();
        assert_eq!(diagnostics.active_requests(), 1);
        assert_eq!(diagnostics.active_mutation_completions(), 1);
        assert!(diagnostics.retained_request_bytes() > 0);
        gate.release.add_permits(1);
        let permit = tokio::time::timeout(
            Duration::from_secs(2),
            node.embedded_operations.count.acquire(),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
        cleanup(&node, &gate, &actor).await;
    }

    #[tokio::test]
    async fn cancelled_batch_retains_only_its_already_accepted_bounded_prefix() {
        let (node, gate, actor) = gated_node(
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(2)
                .build(),
        )
        .await;
        let _release = ReleaseGate(gate.clone());
        let writing = node.clone();
        let batch = tokio::spawn(async move {
            writing
                .put_multi_with_priority(
                    (0..8)
                        .map(|index| (format!("key-{index}"), vec![index]))
                        .collect(),
                    crate::actor::OpPriority::Default,
                )
                .await
        });
        gate.entered.notified().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while node
                .embedded_operation_diagnostics()
                .active_mutation_completions()
                != 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        assert_eq!(node.embedded_operation_diagnostics().active_requests(), 1);
        assert_eq!(
            node.embedded_operation_diagnostics()
                .active_mutation_completions(),
            2
        );
        assert_eq!(*node.next_seq.lock().await, 2);
        assert!(matches!(
            node.put("external-excess".into(), vec![9]).await,
            Err(NodeError::Backpressure(_))
        ));
        assert_eq!(gate.received.load(Ordering::Acquire), 1);
        gate.release.add_permits(32);
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.embedded_operation_diagnostics().active_requests() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(gate.received.load(Ordering::Acquire), 2);
        for index in 0..8 {
            let value = node.get(format!("key-{index}")).await.unwrap();
            if index < 2 {
                assert_eq!(value.unwrap().value, [index]);
            } else {
                assert!(value.is_none(), "cancelled suffix was admitted");
            }
        }
        assert!(node.get("external-excess".into()).await.unwrap().is_none());
        cleanup(&node, &gate, &actor).await;
    }

    #[tokio::test]
    async fn embedded_byte_budget_charges_unused_string_and_vector_capacity_before_enqueue() {
        let node = Node::start_with_options(
            "embedded-capacity".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::builder()
                .embedded_operation_byte_capacity(1024)
                .build(),
        )
        .await
        .unwrap();
        let mut key = String::with_capacity(65_536);
        key.push('k');
        assert!(matches!(
            node.get(key).await,
            Err(NodeError::Backpressure(_))
        ));
        let mut batch = Vec::with_capacity(65_536);
        batch.push(("key".into(), vec![1]));
        assert!(matches!(
            node.put_multi(batch).await.as_slice(),
            [Err(NodeError::Backpressure(_))]
        ));
        assert_eq!(*node.next_seq.lock().await, 0);
        assert!(node.get("key".into()).await.unwrap().is_none());
        node.shutdown().await.unwrap();
    }
}
