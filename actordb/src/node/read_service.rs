// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Node-owned, bounded serving reads outside the multiplexed socket reader.
//!
//! Each priority has an independent queue, key-byte allowance, and fixed worker
//! set. Admission ownership follows keys into actors after RPC cancellation.
//! Successful read-page construction is worker-bounded; admission failures use
//! the existing immediate reader response path. Encoded writer queues retain
//! their separate per-connection limits.

use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex, RwLock};

use ractor::{rpc::CallResult, ActorRef};
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::actor::{BatchReadPage, KvMessage, OpPriority, RetainedBatchRead};
use crate::proto::{BatchGet, BatchKeyValue, Envelope, EnvelopeMsg, ForwardedGet, GetResponse};
use crate::runtime_tasks::RuntimeTasks;
use crate::wire::{message_field_len, MAX_FRAME_BYTES};
use prost::Message;

use super::{
    batch_get_response_base_encoded_len, batch_get_response_envelope_encoded_len,
    send_batch_get_response, send_envelope, FragmentId, FragmentRouter, OperationGate,
    OperationPermit, PeerChannel, WIRE_BATCH_PAGE_BYTES,
};

pub(super) const READ_WORKERS_PER_PRIORITY: usize = 8;
pub(super) const READ_QUEUE_CAPACITY: usize = 64;
pub(super) const READ_KEY_BYTE_CAPACITY: usize = 64 * 1024 * 1024;

struct ReadRetention {
    _count: OwnedSemaphorePermit,
    _key_bytes: OwnedSemaphorePermit,
    _operation: Option<OperationPermit>,
}

struct ReadLane {
    jobs: mpsc::Sender<ReadJob>,
    count: Arc<Semaphore>,
    key_bytes: Arc<Semaphore>,
}

pub(super) struct ReadService {
    lanes: [ReadLane; 3],
    operation_gate: StdMutex<Option<Arc<OperationGate>>>,
}

pub(super) struct ReadRoute {
    pub(super) fragment: FragmentId,
    pub(super) generation: u64,
    pub(super) actor: ActorRef<KvMessage>,
    pub(super) router: FragmentRouter,
    pub(super) serving: Arc<RwLock<HashSet<FragmentId>>>,
}

impl ReadRoute {
    fn is_current(&self) -> bool {
        self.serving.read().unwrap().contains(&self.fragment)
            && self
                .router
                .is_current(self.fragment, self.generation, &self.actor)
    }
}

pub(super) enum ReadRequest {
    Single(ForwardedGet),
    Batch(BatchGet),
}

#[derive(Clone, Copy)]
enum ReadReply {
    Single(u64),
    Batch(u64),
}

impl ReadRequest {
    fn key_bytes(&self) -> usize {
        // Charge retained allocation capacity, not only visible UTF-8 length.
        // The single request becomes a one-key Vec when handed to the actor.
        match self {
            Self::Single(request) => std::mem::size_of::<Vec<String>>()
                .saturating_add(std::mem::size_of::<String>())
                .saturating_add(request.key.capacity()),
            Self::Batch(request) => std::mem::size_of::<Vec<String>>()
                .saturating_add(
                    request
                        .keys
                        .capacity()
                        .saturating_mul(std::mem::size_of::<String>()),
                )
                .saturating_add(
                    request
                        .keys
                        .iter()
                        .fold(0usize, |bytes, key| bytes.saturating_add(key.capacity())),
                ),
        }
    }

    fn into_keys(self) -> (ReadReply, Vec<String>) {
        match self {
            Self::Single(request) => (ReadReply::Single(request.req_id), vec![request.key]),
            Self::Batch(request) => (ReadReply::Batch(request.req_id), request.keys),
        }
    }

    fn unavailable(self, tx: &PeerChannel) -> bool {
        match self {
            Self::Single(_) => true, // GetResponse cannot express unavailability.
            Self::Batch(request) => send_batch_get_response(tx, request.req_id, Vec::new()),
        }
    }

    fn shed(self, tx: &PeerChannel) -> bool {
        match self {
            Self::Single(request) => send_envelope(
                tx,
                Envelope {
                    msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                        req_id: request.req_id,
                        shed: true,
                        ..GetResponse::default()
                    })),
                },
            ),
            Self::Batch(request) => {
                // One explicit shed position is an ordered prefix. Omitted
                // positions were not admitted and the origin resumes them.
                let results = request
                    .keys
                    .into_iter()
                    .take(1)
                    .map(|key| BatchKeyValue {
                        key,
                        shed: true,
                        ..BatchKeyValue::default()
                    })
                    .collect();
                send_batch_get_response(tx, request.req_id, results)
            }
        }
    }
}

pub(super) struct ReadJob {
    request: ReadRequest,
    priority: OpPriority,
    route: ReadRoute,
    tx: PeerChannel,
    deadline: Instant,
    retention: Option<Arc<ReadRetention>>,
}

impl ReadJob {
    pub(super) fn new(
        request: ReadRequest,
        priority: OpPriority,
        route: ReadRoute,
        tx: PeerChannel,
        deadline: Instant,
    ) -> Self {
        Self {
            request,
            priority,
            route,
            tx,
            deadline,
            retention: None,
        }
    }

    fn reject(self, shed: bool) -> bool {
        if *self.tx.cancel.borrow() {
            return false;
        }
        let sent = if shed {
            self.request.shed(&self.tx)
        } else {
            self.request.unavailable(&self.tx)
        };
        if !sent {
            self.tx.cancel();
        }
        sent
    }
}

impl ReadService {
    pub(super) fn bind_operation_gate(&self, gate: Arc<OperationGate>) {
        *self.operation_gate.lock().unwrap() = Some(gate);
    }
    pub(super) async fn start(
        tasks: &RuntimeTasks,
        queue_capacity: usize,
        key_byte_capacity: usize,
    ) -> Option<Arc<Self>> {
        let mut lanes = Vec::with_capacity(3);
        for _ in 0..3 {
            let (jobs, receiver) = mpsc::channel(queue_capacity);
            let receiver = Arc::new(Mutex::new(receiver));
            for _ in 0..READ_WORKERS_PER_PRIORITY {
                if !tasks
                    .spawn(run_read_worker(receiver.clone(), tasks.shutdown_signal()))
                    .await
                {
                    return None;
                }
            }
            lanes.push(ReadLane {
                jobs,
                // Include keys still owned by actors after their RPC waiter
                // disappears. They must not free admission for new requests.
                count: Arc::new(Semaphore::new(queue_capacity + READ_WORKERS_PER_PRIORITY)),
                key_bytes: Arc::new(Semaphore::new(key_byte_capacity)),
            });
        }
        Some(Arc::new(Self {
            lanes: lanes.try_into().ok().expect("three read lanes"),
            operation_gate: StdMutex::new(None),
        }))
    }

    /// Never wait on the socket reader. Only genuine capacity exhaustion
    /// reports shed; closed services and already-expired jobs are unavailable.
    pub(super) fn try_submit(&self, mut job: ReadJob) -> bool {
        let operation = match self.operation_gate.lock().unwrap().as_ref() {
            Some(gate) => match gate.try_enter() {
                Ok(operation) => Some(operation),
                Err(_) => return job.reject(false),
            },
            None => None,
        };
        if Instant::now() >= job.deadline {
            return job.reject(false);
        }
        let lane = &self.lanes[match job.priority {
            OpPriority::High => 0,
            OpPriority::Default => 1,
            OpPriority::Background => 2,
        }];
        if lane.jobs.is_closed() {
            return job.reject(false);
        }
        let count = match lane.count.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return job.reject(true),
        };
        let Ok(request_bytes) = u32::try_from(job.request.key_bytes()) else {
            return job.reject(true);
        };
        let key_bytes = match lane.key_bytes.clone().try_acquire_many_owned(request_bytes) {
            Ok(permit) => permit,
            Err(_) => return job.reject(true),
        };
        job.retention = Some(Arc::new(ReadRetention {
            _count: count,
            _key_bytes: key_bytes,
            _operation: operation,
        }));
        match lane.jobs.try_send(job) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(job)) => job.reject(true),
            Err(mpsc::error::TrySendError::Closed(job)) => job.reject(false),
        }
    }
}

async fn run_read_worker(
    jobs: Arc<Mutex<mpsc::Receiver<ReadJob>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let next = async { jobs.lock().await.recv().await };
        let job = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            job = next => match job { Some(job) => job, None => return },
        };
        serve_read(job, &mut shutdown).await;
    }
}

async fn serve_read(job: ReadJob, shutdown: &mut watch::Receiver<bool>) {
    if *job.tx.cancel.borrow() {
        return;
    }
    if Instant::now() >= job.deadline || !job.route.is_current() {
        job.reject(false);
        return;
    }
    let ReadJob {
        request,
        priority,
        route,
        tx,
        deadline,
        retention,
    } = job;
    // Keep one copy while result keys are in the reply port or being encoded;
    // the actor's copy survives cancellation until its request really drops.
    let retention = retention.expect("admitted read has retention ownership");
    let actor_retention = retention.clone();
    let (reply_kind, keys) = request.into_keys();
    let (base, target) = match reply_kind {
        ReadReply::Single(_) => (0, 0),
        ReadReply::Batch(req_id) => (
            batch_get_response_base_encoded_len(req_id),
            WIRE_BATCH_PAGE_BYTES,
        ),
    };
    let mut cancelled = tx.cancel.subscribe();
    if *cancelled.borrow() || *shutdown.borrow() {
        return;
    }
    let response = tokio::select! {
        biased;
        _ = shutdown.changed() => return,
        _ = cancelled.changed() => return,
        response = tokio::time::timeout_at(deadline, route.actor.call(
            move |reply| KvMessage::GetBatchRetained(RetainedBatchRead::new(
                keys, base, target, reply, priority, actor_retention,
            )),
            None,
        )) => response,
    };
    if *tx.cancel.borrow() || *shutdown.borrow() {
        return;
    }
    let sent = match response {
        Ok(Ok(CallResult::Success(page))) if Instant::now() < deadline && route.is_current() => {
            reply_kind.send_page_retained(&tx, page, Some(retention))
        }
        _ => reply_kind.unavailable(&tx),
    };
    if !sent {
        tx.cancel();
    }
}

impl ReadReply {
    fn unavailable(self, tx: &PeerChannel) -> bool {
        match self {
            Self::Single(_) => true,
            Self::Batch(req_id) => send_batch_get_response(tx, req_id, Vec::new()),
        }
    }

    fn send_page_retained(
        self,
        tx: &PeerChannel,
        page: BatchReadPage,
        retention: Option<Arc<dyn Send + Sync>>,
    ) -> bool {
        match self {
            Self::Single(req_id) => {
                if page.entries.len() != 1 {
                    return self.unavailable(tx);
                }
                let entry = page.entries.into_iter().next().unwrap();
                let shed = entry.result.is_err();
                let mut response = GetResponse {
                    req_id,
                    shed,
                    ..GetResponse::default()
                };
                if let Ok(Some(value)) = entry.result {
                    response.found = true;
                    response.value = value.value;
                    response.timestamp_ms = value.timestamp_ms;
                    response.logical_counter = value.logical_counter;
                    response.node_id = value.node_id;
                    response.expires_at_ms = value.expires_at_ms.unwrap_or(0);
                    response.is_tombstone = value.is_tombstone;
                }
                super::connection::send_envelope_retained(
                    tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::GetResponse(response)),
                    },
                    retention,
                )
            }
            Self::Batch(req_id) => {
                let mut results = Vec::new();
                let mut payload_len = batch_get_response_base_encoded_len(req_id);
                for entry in page.entries {
                    let shed = entry.result.is_err();
                    let mut result = BatchKeyValue {
                        key: entry.key,
                        shed,
                        ..BatchKeyValue::default()
                    };
                    if let Ok(Some(value)) = entry.result {
                        result.found = true;
                        result.value = value.value;
                        result.timestamp_ms = value.timestamp_ms;
                        result.logical_counter = value.logical_counter;
                        result.node_id = value.node_id;
                        result.expires_at_ms = value.expires_at_ms.unwrap_or(0);
                        result.is_tombstone = value.is_tombstone;
                    }
                    let next_payload = payload_len + message_field_len(2, result.encoded_len());
                    let response_len = batch_get_response_envelope_encoded_len(next_payload);
                    if response_len > MAX_FRAME_BYTES {
                        if results.is_empty() {
                            tracing::warn!(
                                req_id,
                                response_len,
                                "single batch read result exceeds the hard frame limit"
                            );
                            return false;
                        }
                        break;
                    }
                    payload_len = next_payload;
                    results.push(result);
                    if response_len > WIRE_BATCH_PAGE_BYTES {
                        break;
                    }
                }
                super::connection::send_envelope_retained(
                    tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchGetResponse(
                            crate::proto::BatchGetResponse { req_id, results },
                        )),
                    },
                    retention,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::test_peer_channel;
    use super::*;
    use crate::actor::BatchReadEntry;
    use ractor::Actor;
    use std::time::Duration;
    use tokio::sync::{oneshot, Notify};

    const KEY_BYTES: usize = std::mem::size_of::<Vec<String>>() + std::mem::size_of::<String>() + 3;

    #[test]
    fn read_storage_charge_includes_spare_capacity_and_headers() {
        let mut key = String::with_capacity(100);
        key.push_str("key");
        let key_capacity = key.capacity();
        let request = ReadRequest::Single(ForwardedGet {
            key,
            ..ForwardedGet::default()
        });
        assert_eq!(
            request.key_bytes(),
            std::mem::size_of::<Vec<String>>() + std::mem::size_of::<String>() + key_capacity
        );
        let mut keys = Vec::with_capacity(8);
        let mut key = String::with_capacity(100);
        key.push_str("key");
        let key_capacity = key.capacity();
        keys.push(key);
        let vector_capacity = keys.capacity();
        let request = ReadRequest::Batch(BatchGet {
            keys,
            ..BatchGet::default()
        });
        assert_eq!(
            request.key_bytes(),
            std::mem::size_of::<Vec<String>>()
                + vector_capacity * std::mem::size_of::<String>()
                + key_capacity
        );
    }

    struct HeldActor;
    struct HeldState {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Actor for HeldActor {
        type Msg = KvMessage;
        type State = HeldState;
        type Arguments = HeldState;
        async fn pre_start(
            &self,
            _: ActorRef<KvMessage>,
            state: HeldState,
        ) -> Result<HeldState, ractor::ActorProcessingErr> {
            Ok(state)
        }
        async fn handle(
            &self,
            _: ActorRef<KvMessage>,
            message: KvMessage,
            state: &mut HeldState,
        ) -> Result<(), ractor::ActorProcessingErr> {
            if let KvMessage::GetBatchRetained(request) = message {
                state.started.notify_one();
                state.release.notified().await;
                let entries = request
                    .keys
                    .into_iter()
                    .map(|key| BatchReadEntry {
                        key,
                        result: Ok(None),
                    })
                    .collect();
                let _ = request.reply.send(BatchReadPage { entries });
            }
            Ok(())
        }
    }

    async fn actor() -> (ActorRef<KvMessage>, Arc<Notify>, Arc<Notify>) {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, _) = Actor::spawn(
            None,
            HeldActor,
            HeldState {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        (actor, started, release)
    }

    fn job(
        actor: &ActorRef<KvMessage>,
        tx: &PeerChannel,
        req_id: u64,
        priority: OpPriority,
        timeout: Duration,
    ) -> ReadJob {
        let router = FragmentRouter::new(1, 0);
        router.insert(FragmentId(0), actor.clone());
        let (generation, actor) = router.route_with_generation(FragmentId(0)).unwrap();
        ReadJob::new(
            ReadRequest::Batch(BatchGet {
                req_id,
                keys: vec!["key".into()],
                priority: 0,
            }),
            priority,
            ReadRoute {
                fragment: FragmentId(0),
                generation,
                actor,
                router,
                serving: Arc::new(RwLock::new(HashSet::from([FragmentId(0)]))),
            },
            tx.clone(),
            Instant::now() + timeout,
        )
    }

    fn queued_service(
        capacity: usize,
        key_bytes: usize,
    ) -> (ReadService, Vec<mpsc::Receiver<ReadJob>>) {
        let mut receivers = Vec::new();
        let lanes = (0..3)
            .map(|_| {
                let (jobs, receiver) = mpsc::channel(capacity);
                receivers.push(receiver);
                ReadLane {
                    jobs,
                    count: Arc::new(Semaphore::new(capacity + READ_WORKERS_PER_PRIORITY)),
                    key_bytes: Arc::new(Semaphore::new(key_bytes)),
                }
            })
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        (
            ReadService {
                lanes,
                operation_gate: StdMutex::new(None),
            },
            receivers,
        )
    }

    async fn response(rx: &mut mpsc::Receiver<crate::wire::BudgetedFrame>) -> Envelope {
        let frame = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        Envelope::decode(frame.bytes()).unwrap()
    }

    #[tokio::test]
    async fn bounded_admission_sheds_ordered_prefix_and_isolates_priorities() {
        let (actor, _, _) = actor().await;
        let (tx, _data, mut responses) = test_peer_channel(8);
        let (service, _receivers) = queued_service(1, KEY_BYTES);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Background,
            Duration::from_secs(1)
        )));
        let mut rejected = job(
            &actor,
            &tx,
            2,
            OpPriority::Background,
            Duration::from_secs(1),
        );
        rejected.request = ReadRequest::Batch(BatchGet {
            req_id: 2,
            keys: vec!["first".into(), "second".into()],
            priority: 0,
        });
        assert!(service.try_submit(rejected));
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r))
            if r.req_id == 2 && r.results.len() == 1 && r.results[0].key == "first" && r.results[0].shed)
        );
        assert!(service.try_submit(job(
            &actor,
            &tx,
            3,
            OpPriority::High,
            Duration::from_secs(1)
        )));
        assert_eq!(service.lanes[0].key_bytes.available_permits(), 0);
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn blocked_background_execution_does_not_delay_high_priority_response() {
        let (background, started, release) = actor().await;
        let (high, _, high_release) = actor().await;
        let tasks = RuntimeTasks::new(8);
        let service = ReadService::start(&tasks, 1, KEY_BYTES).await.unwrap();
        let (tx, _data, mut responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &background,
            &tx,
            1,
            OpPriority::Background,
            Duration::from_secs(5)
        )));
        started.notified().await;
        high_release.notify_one();
        assert!(service.try_submit(job(&high, &tx, 2, OpPriority::High, Duration::from_secs(5))));
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r))
            if r.req_id == 2 && r.results.len() == 1 && !r.results[0].shed)
        );
        tx.cancel();
        tasks.shutdown().await;
        release.notify_one();
        background.stop_and_wait(None, None).await.unwrap();
        high.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn full_queue_sheds_single_read_without_waiting() {
        let (actor, _, _) = actor().await;
        // Both requests fit the byte gate: the second must fail on the queue,
        // not prematurely on byte exhaustion.
        let (service, _receivers) = queued_service(1, 2 * KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Default,
            Duration::from_secs(1)
        )));
        let mut request = job(&actor, &tx, 2, OpPriority::Default, Duration::from_secs(1));
        request.request = ReadRequest::Single(ForwardedGet {
            req_id: 2,
            key: "key".into(),
            priority: 0,
            hops: 0,
        });
        assert!(service.try_submit(request));
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::GetResponse(r)) if r.req_id == 2 && r.shed && !r.found)
        );
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn node_shutdown_drops_queued_keys_but_not_actor_owned_keys() {
        let (actor, started, release) = actor().await;
        let tasks = RuntimeTasks::new(8);
        let service = ReadService::start(&tasks, 1, 9 * KEY_BYTES).await.unwrap();
        let (tx, _data, _responses) = test_peer_channel(16);
        for req_id in 0..READ_WORKERS_PER_PRIORITY {
            assert!(service.try_submit(job(
                &actor,
                &tx,
                req_id as u64,
                OpPriority::Default,
                Duration::from_secs(10)
            )));
            tokio::time::timeout(Duration::from_secs(1), async {
                while service.lanes[1].jobs.capacity() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        started.notified().await;
        assert!(service.try_submit(job(
            &actor,
            &tx,
            9,
            OpPriority::Default,
            Duration::from_secs(10)
        )));
        assert_eq!(service.lanes[1].key_bytes.available_permits(), 0);
        tasks.shutdown().await;
        assert!(service.lanes[1].jobs.is_closed());
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert_eq!(service.lanes[1].count.available_permits(), 1);
        // Killing the actor drops its current and mailbox requests together.
        actor.kill();
        actor.wait(None).await.unwrap();
        assert_eq!(
            service.lanes[1].key_bytes.available_permits(),
            9 * KEY_BYTES
        );
        assert_eq!(service.lanes[1].count.available_permits(), 9);
        drop(release);
    }

    #[tokio::test]
    async fn cancelled_actor_request_retains_key_and_count_admission() {
        let (actor, started, release) = actor().await;
        let tasks = RuntimeTasks::new(8);
        let service = ReadService::start(&tasks, 1, KEY_BYTES).await.unwrap();
        let (tx, _data, _responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Default,
            Duration::from_secs(10)
        )));
        started.notified().await;
        tx.cancel();
        tokio::task::yield_now().await;
        assert_eq!(service.lanes[1].key_bytes.available_permits(), 0);
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY
        );
        tasks.shutdown().await;
        assert_eq!(service.lanes[1].key_bytes.available_permits(), 0);
        release.notify_one();
        actor.stop_and_wait(None, None).await.unwrap();
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY + 1
        );
    }

    #[tokio::test]
    async fn expired_queued_read_never_calls_actor_or_resets_deadline() {
        let (actor, started, _) = actor().await;
        let (service, mut receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Default,
            Duration::from_secs(1)
        )));
        let mut expired = receivers[1].recv().await.unwrap();
        expired.deadline = Instant::now();
        let (_stop, mut shutdown) = watch::channel(false);
        serve_read(expired, &mut shutdown).await;
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r)) if r.results.is_empty())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), started.notified())
                .await
                .is_err()
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn stale_route_cannot_publish_after_actor_reply() {
        let (actor, started, release) = actor().await;
        let (service, mut receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        let request = job(&actor, &tx, 1, OpPriority::Default, Duration::from_secs(5));
        let router = request.route.router.clone();
        assert!(service.try_submit(request));
        let request = receivers[1].recv().await.unwrap();
        let (done, completed) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let (_stop, mut shutdown) = watch::channel(false);
            serve_read(request, &mut shutdown).await;
            let _ = done.send(());
        });
        started.notified().await;
        router.insert(FragmentId(0), actor.clone());
        release.notify_one();
        completed.await.unwrap();
        worker.await.unwrap();
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r)) if r.results.is_empty())
        );
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_without_watch_receivers_skips_queued_actor_execution() {
        let (actor, started, _) = actor().await;
        let (service, mut receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Default,
            Duration::from_secs(1)
        )));
        // There is deliberately no subscribed cancellation receiver yet.
        tx.cancel();
        assert!(*tx.cancel.borrow());
        let queued = receivers[1].recv().await.unwrap();
        let (_stop, mut shutdown) = watch::channel(false);
        serve_read(queued, &mut shutdown).await;
        assert!(responses.try_recv().is_err());
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), started.notified())
                .await
                .is_err()
        );
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn count_exhaustion_includes_cancelled_actor_keys_with_spare_byte_budget() {
        let (actor, started, _) = actor().await;
        let tasks = RuntimeTasks::new(8);
        let capacity = READ_WORKERS_PER_PRIORITY + 1;
        let bytes = (capacity + 1) * KEY_BYTES;
        let service = ReadService::start(&tasks, 1, bytes).await.unwrap();
        let (old_tx, _old_data, _old_responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &old_tx,
            1,
            OpPriority::Default,
            Duration::from_secs(60)
        )));
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        old_tx.cancel();

        let (tx, _data, mut responses) = test_peer_channel(8);
        // The cancelled actor request still owns one admission. All eight
        // workers can now await later requests behind that same held actor.
        for req_id in 2..=capacity as u64 {
            assert!(service.try_submit(job(
                &actor,
                &tx,
                req_id,
                OpPriority::Default,
                Duration::from_secs(60)
            )));
            tokio::time::timeout(Duration::from_secs(2), async {
                while service.lanes[1].jobs.capacity() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(service.lanes[1].count.available_permits(), 0);
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert_eq!(service.lanes[1].jobs.capacity(), 1);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            99,
            OpPriority::Default,
            Duration::from_secs(60)
        )));
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r))
            if r.req_id == 99 && r.results.len() == 1 && r.results[0].key == "key" && r.results[0].shed)
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        tx.cancel();
        tasks.shutdown().await;
        assert_eq!(service.lanes[1].count.available_permits(), 0);
        actor.kill();
        actor.wait(None).await.unwrap();
        assert_eq!(service.lanes[1].count.available_permits(), capacity);
        assert_eq!(service.lanes[1].key_bytes.available_permits(), bytes);
    }

    #[tokio::test]
    async fn preexpired_single_admission_is_silent_and_does_not_consume_capacity() {
        let (actor, started, _) = actor().await;
        let (service, _receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        let mut request = job(&actor, &tx, 1, OpPriority::Default, Duration::ZERO);
        request.request = ReadRequest::Single(ForwardedGet {
            req_id: 1,
            key: "key".into(),
            ..ForwardedGet::default()
        });
        assert!(service.try_submit(request));
        assert!(
            responses.try_recv().is_err(),
            "unavailable single read fabricated a miss or shed reply"
        );
        assert!(!*tx.cancel.borrow());
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY + 1
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert_eq!(service.lanes[1].jobs.capacity(), 1);
        tx.cancel();
        assert!(!service.try_submit(job(&actor, &tx, 2, OpPriority::Default, Duration::ZERO)));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), started.notified())
                .await
                .is_err()
        );
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn stopped_actor_single_is_silent_and_releases_admission() {
        let (actor, _, _) = actor().await;
        actor.stop_and_wait(None, None).await.unwrap();
        let (service, mut receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, mut responses) = test_peer_channel(8);
        let mut request = job(&actor, &tx, 1, OpPriority::Default, Duration::from_secs(5));
        request.request = ReadRequest::Single(ForwardedGet {
            req_id: 1,
            key: "key".into(),
            ..ForwardedGet::default()
        });
        assert!(service.try_submit(request));
        let request = receivers[1].recv().await.unwrap();
        let (_stop, mut shutdown) = watch::channel(false);
        serve_read(request, &mut shutdown).await;
        assert!(
            responses.try_recv().is_err(),
            "stopped actor fabricated a not-found single reply"
        );
        assert!(!*tx.cancel.borrow());
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY + 1
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
    }

    #[tokio::test]
    async fn successful_read_response_lane_failure_closes_only_origin_and_releases_keys() {
        for batch in [false, true] {
            let (actor, _, release) = actor().await;
            let (service, mut receivers) = queued_service(1, KEY_BYTES);
            let (tx, _data, mut responses) = test_peer_channel(1);
            assert!(send_envelope(
                &tx,
                Envelope {
                    msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                        req_id: 99,
                        ..GetResponse::default()
                    }))
                }
            ));
            let (replacement, _replacement_data, mut replacement_responses) = test_peer_channel(1);
            let mut request = job(&actor, &tx, 1, OpPriority::Default, Duration::from_secs(5));
            if !batch {
                request.request = ReadRequest::Single(ForwardedGet {
                    req_id: 1,
                    key: "key".into(),
                    ..ForwardedGet::default()
                });
            }
            assert!(service.try_submit(request));
            let request = receivers[1].recv().await.unwrap();
            release.notify_one();
            let (_stop, mut shutdown) = watch::channel(false);
            serve_read(request, &mut shutdown).await;
            assert!(
                *tx.cancel.borrow(),
                "response rejection did not cancel its origin channel"
            );
            assert!(!*replacement.cancel.borrow());
            assert!(replacement_responses.try_recv().is_err());
            assert!(
                matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::GetResponse(r)) if r.req_id == 99)
            );
            assert!(
                responses.try_recv().is_err(),
                "failed read response leaked into saturated queue"
            );
            assert_eq!(
                service.lanes[1].count.available_permits(),
                READ_WORKERS_PER_PRIORITY + 1
            );
            assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
            actor.stop_and_wait(None, None).await.unwrap();
        }
    }

    #[tokio::test]
    async fn expired_rejection_response_failure_closes_channel_without_actor_admission() {
        let (actor, _, _) = actor().await;
        let (service, _receivers) = queued_service(1, KEY_BYTES);
        let (tx, _data, _responses) = test_peer_channel(1);
        assert!(send_envelope(
            &tx,
            Envelope {
                msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                    req_id: 99,
                    ..GetResponse::default()
                }))
            }
        ));
        assert!(!service.try_submit(job(&actor, &tx, 1, OpPriority::Default, Duration::ZERO)));
        assert!(*tx.cancel.borrow());
        assert_eq!(
            service.lanes[1].count.available_permits(),
            READ_WORKERS_PER_PRIORITY + 1
        );
        assert_eq!(service.lanes[1].key_bytes.available_permits(), KEY_BYTES);
        assert_eq!(service.lanes[1].jobs.capacity(), 1);
        actor.stop_and_wait(None, None).await.unwrap();
    }

    #[tokio::test]
    async fn closed_runtime_refuses_read_worker_registration() {
        let tasks = RuntimeTasks::new(8);
        tasks.shutdown().await;
        assert!(ReadService::start(&tasks, 1, KEY_BYTES).await.is_none());
    }

    #[tokio::test]
    async fn closed_service_is_unavailable_not_shed_even_when_budget_is_exhausted() {
        let (actor, _, _) = actor().await;
        let (service, receivers) = queued_service(1, 0);
        drop(receivers);
        let (tx, _data, mut responses) = test_peer_channel(8);
        assert!(service.try_submit(job(
            &actor,
            &tx,
            1,
            OpPriority::Default,
            Duration::from_secs(1)
        )));
        assert!(
            matches!(response(&mut responses).await.msg, Some(EnvelopeMsg::BatchGetResponse(r)) if r.results.is_empty())
        );
        actor.stop_and_wait(None, None).await.unwrap();
    }
}
