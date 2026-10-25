// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Bounded ownership of serving forwarded mutation completion.
//!
//! The socket reader still awaits each response, preserving frame ordering.
//! Accepted requests outlive that waiter: apply, exact outcome handling, and
//! sender fanout cannot be canceled by a disconnected origin or transport
//! shutdown. Normal shutdown closes admission and drains without aborting.

use std::sync::{Arc, Mutex as StdMutex};

use ractor::ActorRef;
use tokio::sync::{
    mpsc, oneshot, watch, Mutex, OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore,
};
use tokio::task::{JoinHandle, JoinSet};

use crate::actor::{KvMessage, OpPriority, ReplicatedOp, VersionedValue};
use crate::proto::{BatchPut, ForwardedPut, MutationResponse};

use super::{
    apply_forwarded_mutation, priority_from_proto, unavailable_mutation_results, FragmentId,
    OperationPermit, PeerChannel, SenderMap,
};

pub(super) const FORWARDED_MUTATION_CAPACITY: usize = 128;
pub(super) const FORWARDED_MUTATION_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
const WORKERS_PER_PRIORITY: usize = 8;

struct MutationRetention {
    _peer_work: Option<Arc<dyn Send + Sync>>,
    _count: Option<OwnedSemaphorePermit>,
    _payload: OwnedSemaphorePermit,
}

pub(super) enum MutationRequest {
    Single(ForwardedPut),
    Batch(BatchPut),
}

impl MutationRequest {
    fn req_id(&self) -> u64 {
        match self {
            Self::Single(put) => put.req_id,
            Self::Batch(batch) => batch.req_id,
        }
    }

    fn priority(&self) -> OpPriority {
        priority_from_proto(match self {
            Self::Single(put) => put.priority,
            Self::Batch(batch) => batch.priority,
        })
    }

    fn into_keys(self) -> Vec<String> {
        match self {
            Self::Single(put) => vec![put.key],
            Self::Batch(batch) => batch.puts.into_iter().map(|put| put.key).collect(),
        }
    }

    /// Retained request storage plus the largest one-at-a-time ApplyOp clone
    /// and its correlation key. The batch's original backing Vec remains
    /// allocated during iteration; only the current item is cloned for RPC.
    fn payload_bytes(&self) -> usize {
        match self {
            Self::Single(put) => std::mem::size_of::<ForwardedPut>()
                .saturating_add(std::mem::size_of::<Vec<ForwardedPut>>())
                .saturating_add(put_buffer_bytes(put))
                .saturating_add(apply_clone_bytes(put)),
            Self::Batch(batch) => {
                let buffers = batch.puts.iter().fold(0usize, |bytes, put| {
                    bytes.saturating_add(put_buffer_bytes(put))
                });
                let largest_clone = batch.puts.iter().map(apply_clone_bytes).max().unwrap_or(0);
                std::mem::size_of::<BatchPut>()
                    .saturating_add(
                        batch
                            .puts
                            .capacity()
                            .saturating_mul(std::mem::size_of::<ForwardedPut>()),
                    )
                    .saturating_add(buffers)
                    .saturating_add(largest_clone)
            }
        }
    }
}

fn put_buffer_bytes(put: &ForwardedPut) -> usize {
    put.key
        .capacity()
        .saturating_add(put.value.capacity())
        .saturating_add(put.node_id.capacity())
        .saturating_add(put.write_concern.as_ref().map_or(0, Vec::capacity))
}

fn apply_clone_bytes(put: &ForwardedPut) -> usize {
    let bytes = std::mem::size_of::<ReplicatedOp>()
        .saturating_add(put.key.len())
        .saturating_add(put.value.len())
        .saturating_add(put.node_id.len())
        .saturating_add(std::mem::size_of::<String>())
        .saturating_add(put.key.len());
    if put.write_concern.is_some() {
        bytes.saturating_mul(2)
    } else {
        bytes
    }
}

pub(super) struct MutationJob {
    peer_work: Option<Arc<dyn Send + Sync>>,
    request: MutationRequest,
    actor: ActorRef<KvMessage>,
    senders: SenderMap,
    fragment: FragmentId,
    _operation: OperationPermit,
    _authority: Option<OwnedRwLockReadGuard<()>>,
    reply: oneshot::Sender<MutationCompletion>,
    retention: Option<Arc<MutationRetention>>,
}

pub(super) struct MutationCompletion {
    response: MutationResponse,
    // Include completed results still waiting for their reader's encoding in
    // admission. A dropped reader releases this only after apply/fanout ends.
    _retention: Option<Arc<MutationRetention>>,
    _operation: Option<OperationPermit>,
}

impl MutationCompletion {
    pub(super) fn send(self, tx: &PeerChannel) -> bool {
        if let Some(operation) = &self._operation {
            operation.mark_responding();
        }
        super::connection::send_envelope_retained(
            tx,
            crate::proto::Envelope {
                msg: Some(crate::proto::EnvelopeMsg::MutationResponse(self.response)),
            },
            Some(Arc::new((self._retention, self._operation))),
        )
    }
}

impl MutationJob {
    pub(super) fn with_peer_retention(mut self, retention: Option<Arc<dyn Send + Sync>>) -> Self {
        self.peer_work = retention;
        self
    }
    pub(super) fn new(
        request: MutationRequest,
        actor: ActorRef<KvMessage>,
        senders: SenderMap,
        fragment: FragmentId,
        operation: OperationPermit,
        authority: Option<OwnedRwLockReadGuard<()>>,
    ) -> (Self, oneshot::Receiver<MutationCompletion>) {
        let (reply, response) = oneshot::channel();
        (
            Self {
                peer_work: None,
                request,
                actor,
                senders,
                fragment,
                _operation: operation,
                _authority: authority,
                reply,
                retention: None,
            },
            response,
        )
    }

    fn reject(self, reason: &str) {
        let Self {
            peer_work: _peer_work,
            request,
            reply,
            retention,
            _operation,
            _authority,
            ..
        } = self;
        let req_id = request.req_id();
        let _ = reply.send(MutationCompletion {
            response: MutationResponse {
                req_id,
                results: unavailable_mutation_results(request.into_keys(), reason),
            },
            _retention: retention,
            _operation: Some(_operation),
        });
    }
}

/// This owner is deliberately outside abortive transport RuntimeTasks. It
/// closes and drains independently while actors and replication senders live.
pub(super) struct MutationService {
    accepting: StdMutex<bool>,
    lanes: [mpsc::Sender<MutationJob>; 3],
    count: Arc<Semaphore>,
    payload: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    manager: StdMutex<Option<JoinHandle<()>>>,
    shutdown_lock: Mutex<()>,
}

impl MutationService {
    pub(super) fn queued(&self) -> usize {
        self.lanes
            .iter()
            .map(|lane| lane.max_capacity().saturating_sub(lane.capacity()))
            .sum()
    }
    pub(super) fn admitted_apply_slots(&self) -> usize {
        self.lanes[0]
            .max_capacity()
            .saturating_sub(self.count.available_permits())
    }
    #[cfg(test)]
    pub(super) fn new(capacity: usize, byte_capacity: usize) -> Arc<Self> {
        Self::new_inner(capacity, byte_capacity, None)
    }

    pub(super) fn new_with_tracker(
        capacity: usize,
        byte_capacity: usize,
        tracker: Arc<crate::write_concern::WriteConcernTracker>,
    ) -> Arc<Self> {
        Self::new_inner(capacity, byte_capacity, Some(tracker))
    }

    fn new_inner(
        capacity: usize,
        byte_capacity: usize,
        tracker: Option<Arc<crate::write_concern::WriteConcernTracker>>,
    ) -> Arc<Self> {
        let (stop, stop_rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(false);
        let mut senders = Vec::with_capacity(3);
        let mut receivers = Vec::with_capacity(3);
        for _ in 0..3 {
            let (sender, receiver) = mpsc::channel(capacity);
            senders.push(sender);
            receivers.push(receiver);
        }
        let manager = tokio::spawn(run_manager(receivers, stop_rx, done_tx, tracker, capacity));
        Arc::new(Self {
            accepting: StdMutex::new(true),
            lanes: senders.try_into().expect("three mutation lanes"),
            count: Arc::new(Semaphore::new(capacity)),
            payload: Arc::new(Semaphore::new(byte_capacity)),
            stop,
            done,
            manager: StdMutex::new(Some(manager)),
            shutdown_lock: Mutex::new(()),
        })
    }

    /// Synchronous admission transfers the whole request or rejects before
    /// any actor send. Registration cannot detach an unowned accepted job.
    pub(super) fn submit(&self, mut job: MutationJob) {
        let Ok(bytes) = u32::try_from(job.request.payload_bytes()) else {
            job.reject("forwarded mutation exceeds completion payload budget");
            return;
        };
        let accepting = self.accepting.lock().unwrap();
        if !*accepting {
            drop(accepting);
            job.reject("forwarded mutation completion is shutting down");
            return;
        }
        let Ok(count) = self.count.clone().try_acquire_owned() else {
            drop(accepting);
            job.reject("forwarded mutation completion capacity exhausted");
            return;
        };
        let Ok(payload) = self.payload.clone().try_acquire_many_owned(bytes) else {
            drop(accepting);
            drop(count);
            job.reject("forwarded mutation completion payload budget exhausted");
            return;
        };
        job.retention = Some(Arc::new(MutationRetention {
            _peer_work: job.peer_work.clone(),
            _count: Some(count),
            _payload: payload,
        }));
        let lane = match job.request.priority() {
            OpPriority::High => 0,
            OpPriority::Default => 1,
            OpPriority::Background => 2,
        };
        let sent = self.lanes[lane].try_send(job);
        drop(accepting);
        if let Err(error) = sent {
            error
                .into_inner()
                .reject("forwarded mutation completion queue unavailable");
        }
    }

    pub(super) fn request_shutdown(&self) {
        *self.accepting.lock().unwrap() = false;
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
        // Keep join ownership until all workers finished. Canceling a waiting
        // shutdown caller cannot detach/abort accepted mutation completion.
        let manager = self.manager.lock().unwrap().take();
        if let Some(manager) = manager {
            if let Err(error) = manager.await {
                tracing::warn!(%error, "mutation completion owner failed");
            }
        }
    }
}

impl Drop for MutationService {
    fn drop(&mut self) {
        // Last-owner Drop remains best effort, like Node::drop. Only explicit
        // normal shutdown promises a non-aborting drain.
        self.request_shutdown();
        if let Some(manager) = self.manager.lock().unwrap().take() {
            manager.abort();
        }
    }
}

async fn run_manager(
    receivers: Vec<mpsc::Receiver<MutationJob>>,
    stop: watch::Receiver<bool>,
    done: watch::Sender<bool>,
    tracker: Option<Arc<crate::write_concern::WriteConcernTracker>>,
    capacity: usize,
) {
    struct PublishDone(watch::Sender<bool>);
    impl Drop for PublishDone {
        fn drop(&mut self) {
            self.0.send_replace(true);
        }
    }
    let _done = PublishDone(done);
    let (concern_tx, concern_rx) = mpsc::channel(capacity);
    let mut completion_owner = JoinSet::new();
    completion_owner.spawn(run_concern_completions(concern_rx));
    let mut workers = JoinSet::new();
    for receiver in receivers {
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..WORKERS_PER_PRIORITY {
            workers.spawn(run_worker(
                receiver.clone(),
                stop.clone(),
                tracker.clone(),
                concern_tx.clone(),
            ));
        }
    }
    drop(concern_tx);
    while let Some(result) = workers.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "mutation completion worker failed");
        }
    }
    while completion_owner.join_next().await.is_some() {}
}

struct ConcernCompletionJob {
    pending: crate::write_concern::PendingWrite,
    reply: oneshot::Sender<MutationCompletion>,
    completion: MutationCompletion,
    _operation: Arc<OperationPermit>,
}

impl ConcernCompletionJob {
    async fn complete(mut self) {
        let outcome = match self.pending.wait().await {
            Ok(receipt) => crate::write_concern::WriteConcernOutcome::Satisfied(receipt),
            Err(error) => error
                .outcome()
                .expect("acknowledgement wait returns timeout progress"),
        };
        self.completion.response.results[0].write_concern = Some(outcome.encode());
        let _ = self.reply.send(self.completion);
    }
}

async fn run_concern_completions(mut jobs: mpsc::Receiver<ConcernCompletionJob>) {
    let mut owned = JoinSet::new();
    while let Some(job) = jobs.recv().await {
        owned.spawn(job.complete());
        while let Some(result) = owned.try_join_next() {
            if let Err(error) = result {
                tracing::warn!(%error, "write concern completion failed");
            }
        }
    }
    while let Some(result) = owned.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "write concern completion failed");
        }
    }
}

async fn run_worker(
    jobs: Arc<Mutex<mpsc::Receiver<MutationJob>>>,
    mut stop: watch::Receiver<bool>,
    tracker: Option<Arc<crate::write_concern::WriteConcernTracker>>,
    concerns: mpsc::Sender<ConcernCompletionJob>,
) {
    loop {
        let job = {
            let mut jobs = jobs.lock().await;
            // Use the same snapshot for closing admission and enabling the
            // change branch. A second borrow could observe shutdown between
            // these decisions, disable the branch, and wait on an open queue.
            let stopping = *stop.borrow_and_update();
            receive_job(&mut jobs, &mut stop, stopping).await
        };
        let Some(job) = job else {
            return;
        };
        // Never race accepted apply/fanout against transport or node shutdown.
        if let Some(completion) = complete_job(job, tracker.as_ref()).await {
            if let Err(error) = concerns.try_send(completion) {
                // Admission bounds make fullness impossible during normal
                // operation; retain completion ownership on owner failure.
                error.into_inner().complete().await;
            }
        }
    }
}

async fn receive_job(
    jobs: &mut mpsc::Receiver<MutationJob>,
    stop: &mut watch::Receiver<bool>,
    stopping: bool,
) -> Option<MutationJob> {
    if stopping {
        jobs.close();
    }
    tokio::select! {
        biased;
        _ = stop.changed(), if !stopping => { jobs.close(); jobs.recv().await }
        job = jobs.recv() => job,
    }
}

async fn complete_job(
    job: MutationJob,
    tracker: Option<&Arc<crate::write_concern::WriteConcernTracker>>,
) -> Option<ConcernCompletionJob> {
    let MutationJob {
        peer_work,
        request,
        actor,
        senders,
        fragment,
        _operation,
        mut _authority,
        reply,
        mut retention,
    } = job;
    let req_id = request.req_id();
    let _operation = Arc::new(_operation);
    let priority = request.priority();
    let batch = matches!(&request, MutationRequest::Batch(_));
    let puts = match request {
        MutationRequest::Single(put) => vec![put],
        MutationRequest::Batch(batch) => batch.puts,
    };
    let mut results = Vec::with_capacity(puts.len());
    for (index, put) in puts.into_iter().enumerate() {
        let policy = put
            .write_concern
            .as_deref()
            .map(crate::write_concern::WriteConcernOptions::decode)
            .transpose();
        let policy = match policy {
            Ok(policy) if !batch || policy.is_none() => policy,
            Ok(_) => {
                let mut result = super::mutation_result(
                    put.key,
                    index,
                    Err(super::NodeError::Codec(
                        "batch write concerns are unsupported".into(),
                    )),
                );
                result.write_concern = Some(
                    crate::write_concern::WriteConcernOutcome::Unsupported(
                        "batch write concerns are unsupported".into(),
                    )
                    .encode(),
                );
                results.push(result);
                continue;
            }
            Err(error) => {
                let failure = crate::write_concern::WriteConcernError::from(error);
                let mut result = super::mutation_result(
                    put.key,
                    index,
                    Err(super::NodeError::Codec(failure.to_string())),
                );
                result.write_concern = failure.outcome().map(|outcome| outcome.encode());
                results.push(result);
                continue;
            }
        };
        let mut value = VersionedValue::new_version(
            put.value,
            actordb_protocol::RecordVersion::new(put.timestamp_ms, put.logical_counter),
            put.node_id,
        );
        value.expires_at_ms = (put.expires_at_ms != 0).then_some(put.expires_at_ms);
        value.is_tombstone = put.is_tombstone;
        let op = ReplicatedOp {
            key: put.key,
            value,
            seq: put.seq,
            boot_id: put.boot_id,
        };
        let pending = if let Some(policy) = policy.as_ref() {
            let prepared = tracker
                .ok_or_else(|| {
                    crate::write_concern::WriteConcernError::Unsupported(
                        "write concern receiver unavailable".into(),
                    )
                })
                .and_then(|tracker| tracker.prepare(fragment, &op, policy));
            match prepared {
                Ok(pending) => Some(pending),
                Err(failure) => {
                    let mut result = super::mutation_result(
                        op.key,
                        index,
                        Err(super::NodeError::Codec(failure.to_string())),
                    );
                    result.write_concern = failure.outcome().map(|outcome| outcome.encode());
                    results.push(result);
                    continue;
                }
            }
        } else {
            None
        };
        let mut result = apply_forwarded_mutation(
            &actor,
            &senders,
            fragment,
            op,
            priority,
            index,
            peer_work.clone(),
        )
        .await;
        if let Some(pending) = pending {
            // Replication fan-out has completed; ownership teardown must not
            // wait on client acknowledgement policy. Single writes only.
            drop(_authority.take());
            if matches!(
                crate::proto::MutationStatus::try_from(result.status),
                Ok(crate::proto::MutationStatus::Applied
                    | crate::proto::MutationStatus::AlreadyPresent)
            ) {
                pending.applied_locally();
                if pending.minimum_durability()
                    != crate::write_concern::WriteDurability::MemoryApplied
                {
                    let fence = super::flush_fence_retained(
                        &actor,
                        pending.minimum_durability().into(),
                        Some(Arc::new((_operation.clone(), peer_work.clone()))),
                    )
                    .await;
                    let failure = match fence {
                        Ok(durability) => {
                            let durability = durability.into();
                            pending.reached_local_durability(durability);
                            (durability < pending.minimum_durability())
                                .then(|| "local store did not provide requested fence".to_string())
                        }
                        Err(
                            error @ (super::NodeError::Actor(_)
                            | super::NodeError::UnknownOutcome(_)),
                        ) => {
                            result.write_concern = Some(
                                pending
                                    .fail_with(
                                        crate::write_concern::WriteConcernError::UnknownOutcome(
                                            error.to_string(),
                                        ),
                                    )
                                    .outcome()
                                    .expect("unknown outcome is encoded")
                                    .encode(),
                            );
                            results.push(result);
                            continue;
                        }
                        Err(error) => Some(error.to_string()),
                    };
                    if let Some(error) = failure {
                        result.write_concern = pending
                            .fail_durability(error)
                            .outcome()
                            .map(|outcome| outcome.encode());
                        results.push(result);
                        continue;
                    }
                }
                if pending.requires_wait() {
                    // ACK waiters have their own tracker admission. Release
                    // the apply slot so local-only writes can still enter;
                    // retain the payload lease through response encoding.
                    if let Some(lease) = retention.as_mut().and_then(Arc::get_mut) {
                        drop(lease._count.take());
                    }
                    return Some(ConcernCompletionJob {
                        pending,
                        reply,
                        completion: MutationCompletion {
                            response: MutationResponse {
                                req_id,
                                results: vec![result],
                            },
                            _retention: retention,
                            _operation: Some(_operation.as_ref().clone()),
                        },
                        _operation,
                    });
                }
                result.write_concern = Some(match pending.wait().await {
                    Ok(receipt) => {
                        crate::write_concern::WriteConcernOutcome::Satisfied(receipt).encode()
                    }
                    Err(error) => error
                        .outcome()
                        .expect("wait returns receipt timeout")
                        .encode(),
                });
            } else {
                if crate::proto::MutationStatus::try_from(result.status)
                    == Ok(crate::proto::MutationStatus::StorageError)
                {
                    result.write_concern = pending
                        .fail_with(crate::write_concern::WriteConcernError::UnknownOutcome(
                            format!(
                                "storage failed during apply; mutation may have applied: {}",
                                result.error
                            ),
                        ))
                        .outcome()
                        .map(|outcome| outcome.encode());
                } else {
                    pending.operation_failed();
                }
            }
        }
        results.push(result);
    }
    let _ = reply.send(MutationCompletion {
        response: MutationResponse { req_id, results },
        _retention: retention,
        _operation: Some(_operation.as_ref().clone()),
    });
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{FragmentState, KvWorker, KvWorkerArgs};
    use crate::common_test::periodic_async_check;
    use crate::proto::{Envelope, EnvelopeMsg, MutationStatus, Priority};
    use crate::replication::{
        spawn_node_sender, NodeSender, SenderConfig, SenderDeps, SenderRuntimeOptions,
        SenderStateReport,
    };
    use crate::store::{KvStore, StoreError};
    use prost::Message;
    use ractor::Actor;
    use std::collections::HashMap;
    use std::sync::Condvar;
    use std::time::Duration;
    use tokio::sync::{Notify, RwLock};

    use super::super::tests::{
        complete_test_handshake, read_test_envelope, test_connection_context, write_test_envelope,
    };
    use super::super::{
        handle_connection, FragmentRouter, HlcClock, Node, NodeConfig, OperationGate,
    };

    #[tokio::test]
    async fn concern_waits_do_not_occupy_forwarded_apply_worker_slots() {
        let (actor, shared) = worker(None).await;
        let tracker = crate::write_concern::WriteConcernTracker::new(
            "owner".into(),
            Arc::new(std::sync::RwLock::new(HashMap::from([(
                FragmentId(0),
                vec!["owner".into(), "peer".into()],
            )]))),
            16,
        );
        let service =
            MutationService::new_with_tracker(WORKERS_PER_PRIORITY, 128 * 1024, tracker.clone());
        let gate = OperationGate::new();
        let senders = Arc::new(Mutex::new(HashMap::new()));
        let mut responses = Vec::new();
        let mut operations = Vec::new();
        for index in 0..WORKERS_PER_PRIORITY {
            let mut request = put(index as u64 + 1, Priority::Default);
            request.key = format!("held-{index}");
            request.timestamp_ms = index as u64 + 1;
            request.write_concern = Some(
                crate::write_concern::WriteConcernOptions::builder()
                    .concern(crate::write_concern::WriteConcern::Count(2))
                    .timeout(Duration::from_secs(10))
                    .build()
                    .encode()
                    .unwrap(),
            );
            operations.push(ReplicatedOp {
                key: request.key.clone(),
                value: VersionedValue::new(
                    request.value.clone(),
                    request.timestamp_ms,
                    request.node_id.clone(),
                ),
                seq: request.seq,
                boot_id: request.boot_id,
            });
            let (job, response) = MutationJob::new(
                MutationRequest::Single(request),
                actor.clone(),
                senders.clone(),
                FragmentId(0),
                gate.try_enter().unwrap(),
                None,
            );
            service.submit(job);
            responses.push(response);
        }
        periodic_async_check(
            || {
                let shared = shared.clone();
                async move { shared.get_all().await.len() == WORKERS_PER_PRIORITY }
            },
            Duration::from_secs(2),
        )
        .await;
        let (job, plain) = MutationJob::new(
            MutationRequest::Single(put(100, Priority::Default)),
            actor.clone(),
            senders,
            FragmentId(0),
            gate.try_enter().unwrap(),
            None,
        );
        service.submit(job);
        let completion = tokio::time::timeout(Duration::from_secs(2), plain)
            .await
            .expect("plain write was blocked behind acknowledgement waits")
            .unwrap();
        assert_eq!(
            completion.response.results[0].status,
            MutationStatus::Applied as i32
        );
        tracker.acknowledge("peer", FragmentId(0), &operations);
        for response in responses {
            assert!(response.await.unwrap().response.results[0]
                .write_concern
                .is_some());
        }
        service.shutdown().await;
        actor.stop(None);
    }

    #[derive(Default)]
    struct GatedStore {
        entered: Notify,
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
            self.entered.notify_one();
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            self.entries.lock().unwrap().extend(writes.iter().cloned());
            Ok(())
        }
    }

    // Unblock the synchronous store even if an assertion panics, before the
    // runtime tries to join its worker threads.
    struct ReleaseStore(Arc<GatedStore>);
    impl Drop for ReleaseStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    fn put(req_id: u64, priority: Priority) -> ForwardedPut {
        ForwardedPut {
            key: "key".to_string(),
            value: b"value".to_vec(),
            timestamp_ms: 1,
            logical_counter: 0,
            node_id: "writer".to_string(),
            seq: req_id,
            boot_id: 7,
            priority: priority as i32,
            req_id,
            ..Default::default()
        }
    }

    async fn worker(store: Option<Arc<dyn KvStore>>) -> (ActorRef<KvMessage>, Arc<FragmentState>) {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            store,
            1,
            Duration::from_secs(3600),
        ));
        let (actor, _) = Actor::spawn(
            None,
            KvWorker,
            KvWorkerArgs {
                shared: shared.clone(),
                pending: None,
            },
        )
        .await
        .unwrap();
        (actor, shared)
    }

    async fn sender(
        actor: &ActorRef<KvMessage>,
    ) -> (NodeSender, SenderMap, Arc<Mutex<SenderStateReport>>) {
        let router = FragmentRouter::new(1, 1);
        router.insert(FragmentId(0), actor.clone());
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
            progress: Default::default(),
        }));
        let sender = spawn_node_sender(
            FragmentId(0),
            "replica".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                consistency_interval: Duration::from_secs(3600),
                ..Default::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let map = Arc::new(Mutex::new(HashMap::from([(
            (FragmentId(0), "replica".to_string()),
            sender.clone(),
        )])));
        (sender, map, report)
    }

    fn job(
        request: MutationRequest,
        actor: &ActorRef<KvMessage>,
        senders: SenderMap,
        gate: &Arc<OperationGate>,
    ) -> (MutationJob, oneshot::Receiver<MutationCompletion>) {
        MutationJob::new(
            request,
            actor.clone(),
            senders,
            FragmentId(0),
            gate.try_enter().unwrap(),
            None,
        )
    }

    async fn capacity_restored(service: &MutationService, count: usize, bytes: usize) {
        periodic_async_check(
            || async {
                service.count.available_permits() == count
                    && service.payload.available_permits() == bytes
            },
            Duration::from_secs(2),
        )
        .await;
    }

    async fn mutation_response(stream: &mut tokio::io::DuplexStream) -> MutationResponse {
        loop {
            if let Some(EnvelopeMsg::MutationResponse(response)) =
                read_test_envelope(stream).await.msg
            {
                return response;
            }
        }
    }

    fn assert_unavailable(response: &MutationResponse, req_id: u64, keys: &[&str], reason: &str) {
        assert_eq!(response.req_id, req_id);
        assert_eq!(response.results.len(), keys.len());
        for (index, (result, key)) in response.results.iter().zip(keys).enumerate() {
            assert_eq!(result.request_index, index as u32);
            assert_eq!(result.key.as_str(), *key);
            assert_eq!(result.status, MutationStatus::Unavailable as i32);
            assert!(result.error.contains(reason), "{}", result.error);
            assert_eq!(result.winner_timestamp_ms, 0);
            assert!(result.winner_node_id.is_empty());
            assert!(!result.winner_is_tombstone);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn production_batch_admission_rejection_preserves_duplicates_and_connection_health() {
        for count in [1, 4] {
            let store = Arc::new(GatedStore::default());
            let _release = ReleaseStore(store.clone());
            let (actor, state) = worker(Some(store.clone())).await;
            let router = FragmentRouter::new(1, 1);
            router.insert(FragmentId(0), actor.clone());
            let (mut context, tasks) = test_connection_context(
                router,
                Arc::new(std::sync::RwLock::new(std::collections::HashSet::from([
                    FragmentId(0),
                ]))),
                Arc::new(std::sync::RwLock::new(HashMap::new())),
                false,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
            )
            .await;
            let first = put(401, Priority::Background);
            let encoded = Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(first.clone())),
            }
            .encode_to_vec();
            let Some(EnvelopeMsg::ForwardedPut(decoded)) =
                Envelope::decode(encoded.as_slice()).unwrap().msg
            else {
                panic!("expected forwarded put")
            };
            // Match Prost's actual String/Vec capacities, rather than assuming
            // decoded buffers have the same capacity as the producer's input.
            let bytes = MutationRequest::Single(decoded).payload_bytes();
            let service = MutationService::new(count, bytes);
            context.mutation_service = service.clone();
            let gate = context.operation_gate.clone();
            let (mut active, active_socket) = tokio::io::duplex(4096);
            let active_reader = tokio::spawn(handle_connection(
                context.clone(),
                Box::pin(active_socket),
                false,
                None,
                None,
            ));
            complete_test_handshake(&mut active, "active-origin").await;
            write_test_envelope(
                &mut active,
                Envelope {
                    msg: Some(EnvelopeMsg::ForwardedPut(first)),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
                .await
                .unwrap();

            let (mut rejected, rejected_socket) = tokio::io::duplex(4096);
            let rejected_reader = tokio::spawn(handle_connection(
                context,
                Box::pin(rejected_socket),
                false,
                None,
                None,
            ));
            complete_test_handshake(&mut rejected, "rejected-origin").await;
            let keys = ["rejected", "rejected", "other"];
            let puts = keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    let mut put = put(410 + index as u64, Priority::Background);
                    put.key = (*key).to_string();
                    put.timestamp_ms = 10 + index as u64;
                    put.is_tombstone = index == 1;
                    if put.is_tombstone {
                        put.value.clear();
                    }
                    put.expires_at_ms = if index == 2 { u64::MAX } else { 0 };
                    put
                })
                .collect();
            write_test_envelope(
                &mut rejected,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                        req_id: 420,
                        priority: Priority::High as i32,
                        puts,
                    })),
                },
            )
            .await;
            let response = mutation_response(&mut rejected).await;
            let reason = if count == 1 {
                "capacity exhausted"
            } else {
                "payload budget exhausted"
            };
            assert_unavailable(&response, 420, &keys, reason);
            assert_eq!(service.count.available_permits(), count - 1);
            assert_eq!(service.payload.available_permits(), 0);
            for key in keys {
                assert!(state.get(key).await.is_none());
            }
            // Rejection must not kill or wedge the same physical reader. This
            // independent empty batch does not consume the saturated owner.
            write_test_envelope(
                &mut rejected,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                        req_id: 421,
                        priority: Priority::Default as i32,
                        puts: Vec::new(),
                    })),
                },
            )
            .await;
            let empty = mutation_response(&mut rejected).await;
            assert_eq!(empty.req_id, 421);
            assert!(empty.results.is_empty());

            store.release();
            let applied = mutation_response(&mut active).await;
            assert_eq!(applied.req_id, 401);
            assert_eq!(applied.results[0].status, MutationStatus::Applied as i32);
            capacity_restored(&service, count, bytes).await;
            gate.wait_for_drain().await;
            assert_eq!(store.entries.lock().unwrap().len(), 1);
            drop(active);
            drop(rejected);
            tokio::time::timeout(Duration::from_secs(2), active_reader)
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), rejected_reader)
                .await
                .unwrap()
                .unwrap();
            service.shutdown().await;
            tasks.shutdown().await;
            actor.stop(None);
        }
    }

    #[tokio::test]
    async fn closed_owner_rejects_whole_duplicate_batch_and_releases_authority() {
        let (actor, state) = worker(None).await;
        let service = MutationService::new(1, 1);
        service.shutdown().await;
        let gate = OperationGate::new();
        let authority = Arc::new(RwLock::new(()));
        let keys = ["key", "key", "suffix"];
        let request = MutationRequest::Batch(BatchPut {
            req_id: 430,
            priority: Priority::High as i32,
            puts: keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    let mut put = put(index as u64 + 1, Priority::Default);
                    put.key = (*key).to_string();
                    put
                })
                .collect(),
        });
        let (job, response) = MutationJob::new(
            request,
            actor.clone(),
            Arc::new(Mutex::new(HashMap::new())),
            FragmentId(0),
            gate.try_enter().unwrap(),
            Some(authority.clone().read_owned().await),
        );
        service.submit(job);
        let completion = response.await.unwrap();
        assert_unavailable(&completion.response, 430, &keys, "shutting down");
        // Rejected responses retain request ownership through send or drop.
        // Waiting for that same ownership while keeping the response alive
        // would make this fixture wait on itself indefinitely.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.wait_for_drain())
                .await
                .is_err()
        );
        drop(completion);
        tokio::time::timeout(Duration::from_secs(2), gate.wait_for_drain())
            .await
            .unwrap();
        drop(authority.write_owned().await);
        assert!(state.get_all().await.is_empty());
        capacity_restored(&service, 1, 1).await;
        actor.stop(None);
    }

    #[tokio::test]
    async fn production_mixed_fragment_and_hop_rejections_preserve_keys_and_reader() {
        let (actor, state) = worker(None).await;
        let router = FragmentRouter::new(2, 1);
        router.insert(FragmentId(0), actor.clone());
        let (context, tasks) = test_connection_context(
            router,
            Arc::new(std::sync::RwLock::new(std::collections::HashSet::from([
                FragmentId(0),
            ]))),
            Arc::new(std::sync::RwLock::new(HashMap::new())),
            false,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        let service = context.mutation_service.clone();
        let gate = context.operation_gate.clone();
        let serving_key = (0..100)
            .map(|index| format!("serving-{index}"))
            .find(|key| super::super::fragment_of(key, 2) == FragmentId(0))
            .unwrap();
        let remote_key = (0..100)
            .map(|index| format!("remote-{index}"))
            .find(|key| super::super::fragment_of(key, 2) == FragmentId(1))
            .unwrap();
        let (mut origin, socket) = tokio::io::duplex(4096);
        let reader = tokio::spawn(handle_connection(
            context,
            Box::pin(socket),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut origin, "validation-origin").await;
        let mixed_keys = [&serving_key[..], &remote_key[..], &serving_key[..]];
        let puts = mixed_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let mut put = put(440 + index as u64, Priority::Default);
                put.key = (*key).to_string();
                put
            })
            .collect();
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    req_id: 450,
                    priority: Priority::Default as i32,
                    puts,
                })),
            },
        )
        .await;
        assert_unavailable(
            &mutation_response(&mut origin).await,
            450,
            &mixed_keys,
            "multiple fragments",
        );
        let hop_keys = [&remote_key[..], &remote_key[..]];
        let puts = hop_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let mut put = put(460 + index as u64, Priority::Default);
                put.key = (*key).to_string();
                put.hops = if index == 0 {
                    0
                } else {
                    super::super::MAX_FORWARD_HOPS
                };
                put
            })
            .collect();
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    req_id: 470,
                    priority: Priority::Default as i32,
                    puts,
                })),
            },
        )
        .await;
        assert_unavailable(
            &mutation_response(&mut origin).await,
            470,
            &hop_keys,
            "hop limit",
        );
        assert!(state.get_all().await.is_empty());
        let mut valid = put(480, Priority::High);
        valid.key = serving_key.clone();
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(valid)),
            },
        )
        .await;
        let valid = mutation_response(&mut origin).await;
        assert_eq!(valid.req_id, 480);
        assert_eq!(valid.results[0].status, MutationStatus::Applied as i32);
        assert_eq!(state.get_all().await.len(), 1);
        assert!(state.get(&remote_key).await.is_none());
        gate.wait_for_drain().await;
        capacity_restored(
            &service,
            FORWARDED_MUTATION_CAPACITY,
            FORWARDED_MUTATION_BYTE_CAPACITY,
        )
        .await;
        drop(origin);
        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .unwrap()
            .unwrap();
        service.shutdown().await;
        tasks.shutdown().await;
        actor.stop(None);
    }

    #[test]
    fn payload_admission_charges_allocated_headers_buffers_and_largest_rpc_clone() {
        let mut first = put(1, Priority::Default);
        first.key.reserve(97);
        first.value.reserve(257);
        first.node_id.reserve(61);
        let single = MutationRequest::Single(first.clone());
        let MutationRequest::Single(single_put) = &single else {
            unreachable!()
        };
        assert_eq!(
            single.payload_bytes(),
            std::mem::size_of::<ForwardedPut>()
                + std::mem::size_of::<Vec<ForwardedPut>>()
                + put_buffer_bytes(single_put)
                + apply_clone_bytes(single_put)
        );
        let mut second = put(2, Priority::High);
        second.value = vec![5; 4096];
        let mut puts = Vec::with_capacity(8);
        puts.push(first);
        puts.push(second);
        let request = MutationRequest::Batch(BatchPut {
            puts,
            req_id: 3,
            priority: Priority::High as i32,
        });
        let MutationRequest::Batch(batch) = &request else {
            unreachable!()
        };
        assert_eq!(
            request.payload_bytes(),
            std::mem::size_of::<BatchPut>()
                + batch.puts.capacity() * std::mem::size_of::<ForwardedPut>()
                + batch.puts.iter().map(put_buffer_bytes).sum::<usize>()
                + batch.puts.iter().map(apply_clone_bytes).max().unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn canceled_apply_waiter_and_shutdown_keep_actor_sender_and_authority_owned() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let (actor, _) = worker(Some(store.clone())).await;
        let (sender, senders, report) = sender(&actor).await;
        let request = MutationRequest::Single(put(1, Priority::Default));
        let bytes = request.payload_bytes();
        let service = MutationService::new(1, bytes);
        let gate = OperationGate::new();
        let authority = Arc::new(RwLock::new(()));
        let (job, response) = MutationJob::new(
            request,
            actor.clone(),
            senders,
            FragmentId(0),
            gate.try_enter().unwrap(),
            Some(authority.clone().read_owned().await),
        );
        service.submit(job);
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        drop(response);
        gate.begin_shutdown();
        assert_eq!(service.count.available_permits(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), gate.wait_for_drain())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), authority.clone().write_owned())
                .await
                .is_err()
        );
        let closing = service.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        shutdown.abort();
        assert!(matches!(shutdown.await, Err(error) if error.is_cancelled()));
        assert_eq!(service.payload.available_permits(), 0);
        store.release();
        tokio::time::timeout(Duration::from_secs(2), service.shutdown())
            .await
            .unwrap();
        gate.wait_for_drain().await;
        let _write = authority.write_owned().await;
        capacity_restored(&service, 1, bytes).await;
        periodic_async_check(
            || async { report.lock().await.buffered == 1 },
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            store
                .entries
                .lock()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .timestamp_ms,
            1
        );
        sender.stop().await;
        actor.stop(None);
    }

    #[tokio::test]
    async fn canceled_fanout_waiter_retains_budget_until_actual_sender_handoff() {
        let (actor, state) = worker(None).await;
        let (sender, senders, report) = sender(&actor).await;
        let blocked_fanout = senders.lock().await;
        let request = MutationRequest::Single(put(1, Priority::Default));
        let bytes = request.payload_bytes();
        let service = MutationService::new(1, bytes);
        let gate = OperationGate::new();
        let (job, response) = job(request, &actor, senders.clone(), &gate);
        service.submit(job);
        periodic_async_check(
            || async { state.get("key").await.is_some() },
            Duration::from_secs(2),
        )
        .await;
        drop(response);
        gate.begin_shutdown();
        assert_eq!(service.count.available_permits(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), gate.wait_for_drain())
                .await
                .is_err()
        );
        let closing = service.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        drop(blocked_fanout);
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        gate.wait_for_drain().await;
        capacity_restored(&service, 1, bytes).await;
        periodic_async_check(
            || async { report.lock().await.buffered == 1 },
            Duration::from_secs(2),
        )
        .await;
        sender.stop().await;
        actor.stop(None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn aggregate_count_and_payload_saturation_reject_before_actual_actor_apply() {
        for count in [1, 4] {
            let store = Arc::new(GatedStore::default());
            let _release = ReleaseStore(store.clone());
            let (actor, state) = worker(Some(store.clone())).await;
            let senders = Arc::new(Mutex::new(HashMap::new()));
            let request = MutationRequest::Single(put(1, Priority::Background));
            let bytes = request.payload_bytes();
            let service = MutationService::new(count, bytes);
            let gate = OperationGate::new();
            let (first, first_response) = job(request, &actor, senders.clone(), &gate);
            service.submit(first);
            tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
                .await
                .unwrap();
            let mut rejected = put(2, Priority::High);
            rejected.key = "rejected".to_string();
            let (rejected, rejected_response) =
                job(MutationRequest::Single(rejected), &actor, senders, &gate);
            service.submit(rejected);
            let rejected = rejected_response.await.unwrap();
            assert_eq!(
                rejected.response.results[0].status,
                MutationStatus::Unavailable as i32
            );
            assert!(state.get("rejected").await.is_none());
            assert_eq!(service.count.available_permits(), count - 1);
            assert_eq!(service.payload.available_permits(), 0);
            store.release();
            let first = first_response.await.unwrap();
            assert_eq!(
                first.response.results[0].status,
                MutationStatus::Applied as i32
            );
            assert_eq!(service.count.available_permits(), count - 1);
            drop(first);
            service.shutdown().await;
            capacity_restored(&service, count, bytes).await;
            actor.stop(None);
        }
    }

    #[tokio::test]
    async fn whole_batch_completion_preserves_duplicate_order_and_terminal_outcomes() {
        let (actor, state) = worker(None).await;
        let (sender, senders, report) = sender(&actor).await;
        let mut first = put(31, Priority::Background);
        first.timestamp_ms = 3;
        let mut second = put(32, Priority::Background);
        second.timestamp_ms = 2;
        let request = MutationRequest::Batch(BatchPut {
            req_id: 99,
            priority: Priority::High as i32,
            puts: vec![first.clone(), second, first],
        });
        let bytes = request.payload_bytes();
        let service = MutationService::new(1, bytes);
        let gate = OperationGate::new();
        let (job, response) = job(request, &actor, senders, &gate);
        service.submit(job);
        let completion = response.await.unwrap();
        assert_eq!(completion.response.req_id, 99);
        let expected = [
            MutationStatus::Applied,
            MutationStatus::Superseded,
            MutationStatus::AlreadyPresent,
        ];
        for (index, (result, status)) in
            completion.response.results.iter().zip(expected).enumerate()
        {
            assert_eq!(result.request_index, index as u32);
            assert_eq!(result.key, "key");
            assert_eq!(result.status, status as i32);
        }
        assert_eq!(completion.response.results[1].winner_timestamp_ms, 3);
        assert_eq!(state.get("key").await.unwrap().timestamp_ms, 3);
        assert_eq!(service.count.available_permits(), 0);
        drop(completion);
        service.shutdown().await;
        capacity_restored(&service, 1, bytes).await;
        periodic_async_check(
            || async { report.lock().await.buffered == 2 },
            Duration::from_secs(2),
        )
        .await;
        sender.stop().await;
        actor.stop(None);
    }

    #[tokio::test]
    async fn unregistered_and_closed_owner_jobs_release_operation_and_authority() {
        let (actor, state) = worker(None).await;
        let gate = OperationGate::new();
        let authority = Arc::new(RwLock::new(()));
        let senders = Arc::new(Mutex::new(HashMap::new()));
        let (unregistered, abandoned) = MutationJob::new(
            MutationRequest::Single(put(1, Priority::Default)),
            actor.clone(),
            senders.clone(),
            FragmentId(0),
            gate.try_enter().unwrap(),
            Some(authority.clone().read_owned().await),
        );
        drop(unregistered);
        assert!(abandoned.await.is_err());
        gate.wait_for_drain().await;
        drop(authority.clone().write_owned().await);
        let service = MutationService::new(1, 4096);
        service.shutdown().await;
        let (closed, response) = MutationJob::new(
            MutationRequest::Single(put(2, Priority::High)),
            actor.clone(),
            senders,
            FragmentId(0),
            gate.try_enter().unwrap(),
            Some(authority.clone().read_owned().await),
        );
        service.submit(closed);
        let rejected = response.await.unwrap();
        assert_eq!(
            rejected.response.results[0].status,
            MutationStatus::Unavailable as i32
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.wait_for_drain())
                .await
                .is_err()
        );
        drop(rejected);
        tokio::time::timeout(Duration::from_secs(2), gate.wait_for_drain())
            .await
            .unwrap();
        drop(authority.write_owned().await);
        assert!(state.get("key").await.is_none());
        capacity_restored(&service, 1, 4096).await;
        actor.stop(None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn canceled_batch_waiter_finishes_suffix_and_queued_actor_work() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let (actor, state) = worker(Some(store.clone())).await;
        let (sender, senders, report) = sender(&actor).await;
        let mut suffix = put(2, Priority::Default);
        suffix.key = "suffix".to_string();
        let batch = MutationRequest::Batch(BatchPut {
            req_id: 9,
            priority: Priority::Default as i32,
            puts: vec![put(1, Priority::Default), suffix],
        });
        let mut queued = put(3, Priority::Default);
        queued.key = "queued".to_string();
        let queued = MutationRequest::Single(queued);
        let bytes = batch.payload_bytes() + queued.payload_bytes();
        let service = MutationService::new(2, bytes);
        let gate = OperationGate::new();
        let (batch, batch_response) = job(batch, &actor, senders.clone(), &gate);
        service.submit(batch);
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        let (queued, queued_response) = job(queued, &actor, senders, &gate);
        service.submit(queued);
        // All eight workers in the lane may dispatch, but this real actor is
        // blocked in the first store flush. Both accepted payloads stay owned.
        tokio::task::yield_now().await;
        drop(batch_response);
        drop(queued_response);
        gate.begin_shutdown();
        assert_eq!(service.count.available_permits(), 0);
        assert_eq!(service.payload.available_permits(), 0);
        let closing = service.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), gate.wait_for_drain())
                .await
                .is_err()
        );
        store.release();
        // Completion is a correctness check; allow shared CI scheduling time
        // after release while retaining the blocked-drain and final-state checks.
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .unwrap()
            .unwrap();
        gate.wait_for_drain().await;
        assert!(state.get("suffix").await.is_some());
        assert!(state.get("queued").await.is_some());
        assert_eq!(store.entries.lock().unwrap().len(), 3);
        capacity_restored(&service, 2, bytes).await;
        periodic_async_check(
            || async { report.lock().await.buffered == 3 },
            Duration::from_secs(2),
        )
        .await;
        sender.stop().await;
        actor.stop(None);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_after_receiver_snapshot_closes_an_empty_live_queue() {
        let (sender, mut jobs) = mpsc::channel(1);
        let (stop_sender, mut stop) = watch::channel(false);
        let stopping = *stop.borrow_and_update();
        assert!(!stopping);
        // Precisely the interleaving that used to disable the change branch
        // without closing the queue. Keep the producer alive while receiving.
        stop_sender.send_replace(true);
        let job = tokio::time::timeout(
            Duration::from_secs(1),
            receive_job(&mut jobs, &mut stop, stopping),
        )
        .await
        .unwrap();
        assert!(job.is_none());
        assert!(sender.is_closed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_drains_queued_jobs_beyond_execution_worker_count() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let (actor, state) = worker(Some(store.clone())).await;
        let requests: Vec<_> = (0..=WORKERS_PER_PRIORITY)
            .map(|index| {
                let mut put = put(index as u64 + 1, Priority::Default);
                put.key = format!("queued-{index}");
                MutationRequest::Single(put)
            })
            .collect();
        let count = requests.len();
        let bytes = requests.iter().map(MutationRequest::payload_bytes).sum();
        let service = MutationService::new(count, bytes);
        let gate = OperationGate::new();
        let senders = Arc::new(Mutex::new(HashMap::new()));
        let mut responses = Vec::new();
        for request in requests {
            let (job, response) = job(request, &actor, senders.clone(), &gate);
            service.submit(job);
            responses.push(response);
        }
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        // Eight execution workers are blocked on the real actor; the ninth
        // request must still occupy the service queue, not an actor mailbox.
        periodic_async_check(
            || async { service.lanes[1].capacity() == count - 1 },
            Duration::from_secs(2),
        )
        .await;
        drop(responses);
        gate.begin_shutdown();
        let closing = service.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), gate.wait_for_drain())
                .await
                .is_err()
        );
        assert_eq!(service.count.available_permits(), 0);
        store.release();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        gate.wait_for_drain().await;
        assert_eq!(state.get_all().await.len(), count);
        assert_eq!(store.entries.lock().unwrap().len(), count);
        capacity_restored(&service, count, bytes).await;
        actor.stop(None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn production_reader_preserves_single_before_batch_and_duplicate_positions() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let (actor, _) = worker(Some(store.clone())).await;
        let router = FragmentRouter::new(1, 1);
        router.insert(FragmentId(0), actor.clone());
        let (context, tasks) = test_connection_context(
            router,
            Arc::new(std::sync::RwLock::new(std::collections::HashSet::from([
                FragmentId(0),
            ]))),
            Arc::new(std::sync::RwLock::new(HashMap::new())),
            false,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        let service = context.mutation_service.clone();
        let (mut origin, socket) = tokio::io::duplex(4096);
        let reader = tokio::spawn(handle_connection(
            context,
            Box::pin(socket),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut origin, "origin").await;
        let first = put(81, Priority::Default);
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(first.clone())),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        let mut later = put(82, Priority::High);
        later.timestamp_ms = 2;
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    req_id: 83,
                    priority: Priority::High as i32,
                    puts: vec![first, later.clone(), later],
                })),
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        // The buffered high-priority batch must not enter another completion
        // lane while the preceding single put is awaiting its actor result.
        assert_eq!(
            service.count.available_permits(),
            FORWARDED_MUTATION_CAPACITY - 1
        );
        store.release();
        let single = loop {
            if let Some(EnvelopeMsg::MutationResponse(response)) =
                read_test_envelope(&mut origin).await.msg
            {
                break response;
            }
        };
        assert_eq!(single.req_id, 81);
        assert_eq!(single.results[0].status, MutationStatus::Applied as i32);
        let batch = loop {
            if let Some(EnvelopeMsg::MutationResponse(response)) =
                read_test_envelope(&mut origin).await.msg
            {
                break response;
            }
        };
        assert_eq!(batch.req_id, 83);
        for (index, (result, expected)) in batch
            .results
            .iter()
            .zip([
                MutationStatus::AlreadyPresent,
                MutationStatus::Applied,
                MutationStatus::AlreadyPresent,
            ])
            .enumerate()
        {
            assert_eq!(result.request_index, index as u32);
            assert_eq!(result.status, expected as i32);
            assert_eq!(result.key, "key");
        }
        drop(origin);
        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .unwrap()
            .unwrap();
        service.shutdown().await;
        tasks.shutdown().await;
        actor.stop(None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn node_shutdown_resumes_owned_forwarded_apply_before_actor_teardown() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseStore(store.clone());
        let node = Arc::new(
            Node::start(
                "completion-owner".to_string(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig {
                    store: Some(store.clone()),
                    flush_batch_size: 1,
                    flush_interval: Duration::from_secs(3600),
                    snapshot_interval: Duration::from_secs(3600),
                    clean_interval: Duration::from_secs(3600),
                    fragment_map: HashMap::from([(
                        FragmentId(0),
                        vec!["completion-owner".to_string(), "replica".to_string()],
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let (mut origin, socket) = tokio::io::duplex(4096);
        let context = node.connection_context();
        assert!(
            node.tasks
                .spawn(async move {
                    handle_connection(context, Box::pin(socket), false, None, None).await;
                })
                .await
        );
        complete_test_handshake(&mut origin, "origin").await;
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(put(71, Priority::Default))),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        let blocked_fanout = node.senders.lock().await;
        let closing = node.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        periodic_async_check(
            || async { !node.tasks.is_accepting() },
            Duration::from_secs(2),
        )
        .await;
        assert!(!shutdown.is_finished());
        shutdown.abort();
        assert!(matches!(shutdown.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            node.mutation_service.count.available_permits(),
            FORWARDED_MUTATION_CAPACITY - 1
        );
        assert!(node.router.route_for_fragment(FragmentId(0)).is_some());
        drop(origin);
        store.release();
        periodic_async_check(
            || async { !store.entries.lock().unwrap().is_empty() },
            Duration::from_secs(2),
        )
        .await;
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            node.operation_gate.wait_for_drain()
        )
        .await
        .is_err());
        let closing = node.clone();
        let resumed = tokio::spawn(async move { closing.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!resumed.is_finished());
        drop(blocked_fanout);
        tokio::time::timeout(Duration::from_secs(3), resumed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        capacity_restored(
            &node.mutation_service,
            FORWARDED_MUTATION_CAPACITY,
            FORWARDED_MUTATION_BYTE_CAPACITY,
        )
        .await;
        assert_eq!(
            store
                .entries
                .lock()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .timestamp_ms,
            1
        );
    }
}
