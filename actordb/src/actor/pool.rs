// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Priority configuration, token buckets, and managed worker admission.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::FutureExt;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, ActorStatus};
use tokio::sync::Mutex;

use super::fragment_state::FragmentState;
#[cfg(test)]
use super::pool_worker::{
    install_worker_get_gate, install_worker_startup_gate, take_worker_startup_gate,
};
use super::pool_worker::{PoolWorker, PoolWorkerArgs};
use super::protocol::KvMessage;
use crate::shard::hash_key;
/// Configuration for a worker pool.
/// Omitted builder settings use the normal-priority pool defaults.
#[derive(Debug, Clone, bon::Builder)]
pub struct PoolConfig {
    /// Number of worker actors.
    #[builder(default = 4)]
    pub workers: usize,
    /// Max active-plus-queued jobs accepted into this priority's workers
    /// before shedding. This does not bound the shared dispatcher mailbox.
    /// Shed ops are dropped and counted (explicit, not silent).
    #[builder(default = 10_000)]
    pub queue_cap: usize,
    /// Token-bucket rate limit (ops/sec). `None` = unlimited.
    pub rate_limit_per_sec: Option<u32>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl PoolConfig {
    pub(crate) fn high_priority_defaults() -> Self {
        Self::builder().workers(2).queue_cap(5_000).build()
    }

    pub(crate) fn background_defaults() -> Self {
        Self::builder()
            .workers(2)
            .queue_cap(5_000)
            .rate_limit_per_sec(1000)
            .build()
    }
}

/// Token-bucket rate limiter.
#[derive(Debug)]
pub struct TokenBucket {
    capacity: u32,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: u32) -> Self {
        Self {
            capacity: rate_per_sec.max(1),
            tokens: rate_per_sec as f64,
            refill_per_sec: rate_per_sec as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to take a token. Returns `true` if allowed.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity as f64);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub(super) fn release_pending(pending: &AtomicU64, count: u64) -> bool {
    let mut current = pending.load(Ordering::Relaxed);
    loop {
        debug_assert!(
            current >= count,
            "pending counter underflow while releasing {count} jobs"
        );
        if current < count {
            return false;
        }
        match pending.compare_exchange_weak(
            current,
            current - count,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

/// The worker index for `key`: sticky across calls within one pool, so a key's
/// operations at one priority always land on the same worker.
pub fn worker_idx(key: &str, worker_count: usize) -> usize {
    hash_key(key.as_bytes()) as usize % worker_count
}

/// Internal admission and lifecycle owner for a fragment's worker pool.
///
/// The pool spawns the workers, routes keys to them by sticky hash, and
/// drains them on shutdown. The dispatcher holds a pool and does nothing
/// else with worker lifecycle. It provides explicit backpressure: bounded queue,
/// token-bucket rate limiting, and counted sheds (never silent).
#[derive(Default)]
struct WorkerStartupChildren {
    cells: Vec<ActorCell>,
}

impl WorkerStartupChildren {
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

impl Drop for WorkerStartupChildren {
    fn drop(&mut self) {
        for cell in &self.cells {
            let _ = cell.drain();
        }
    }
}

struct WorkerSlot {
    worker: ActorRef<KvMessage>,
    pending: Arc<AtomicU64>,
}

pub(super) struct WorkerPool {
    workers: Vec<WorkerSlot>,
    config: PoolConfig,
    /// Jobs currently active or queued across this pool's workers.
    pub(super) pending: Arc<AtomicU64>,
    /// Ops shed due to queue cap or rate limit (explicit, counted).
    shed_count: Arc<AtomicU64>,
    rate_limiter: Option<Mutex<TokenBucket>>,
}

impl WorkerPool {
    /// Spawn workers sharing `shared` state, linked to `supervisor`
    /// so a dead worker raises a supervision event there. Workers are
    /// named `<prefix>-worker-<slot>` for registry lookup.
    pub(super) async fn spawn_linked(
        config: PoolConfig,
        shared: Arc<FragmentState>,
        supervisor: ActorCell,
        worker_prefix: &str,
    ) -> Result<Self, ActorProcessingErr> {
        let worker_count = config.workers.max(1);
        let pending = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(worker_count);
        let mut startup_children = WorkerStartupChildren::default();
        let startup = AssertUnwindSafe(async {
            for slot in 0..worker_count {
                let name = format!("{worker_prefix}-worker-{slot}");
                let slot_pending = Arc::new(AtomicU64::new(0));
                #[cfg(test)]
                let startup_gate = take_worker_startup_gate(&name);
                let (worker, _handle) = PoolWorker::spawn_linked(
                    Some(name),
                    PoolWorker,
                    PoolWorkerArgs {
                        shared: shared.clone(),
                        pool_pending: pending.clone(),
                        slot_pending: slot_pending.clone(),
                        #[cfg(test)]
                        startup_gate,
                    },
                    supervisor.clone(),
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                startup_children.track(worker.get_cell());
                workers.push(WorkerSlot {
                    worker,
                    pending: slot_pending,
                });
            }
            Ok::<(), ActorProcessingErr>(())
        })
        .catch_unwind()
        .await;
        match startup {
            Ok(Ok(())) => startup_children.disarm(),
            Ok(Err(error)) => {
                startup_children.rollback().await;
                return Err(error);
            }
            Err(panic) => {
                startup_children.rollback().await;
                std::panic::resume_unwind(panic);
            }
        }
        Ok(Self {
            workers,
            rate_limiter: config
                .rate_limit_per_sec
                .map(|r| Mutex::new(TokenBucket::new(r))),
            shed_count: Arc::new(AtomicU64::new(0)),
            pending,
            config,
        })
    }

    /// Spawn workers sharing `shared` state, unsupervised.
    #[cfg(test)]
    async fn spawn(
        config: PoolConfig,
        shared: Arc<FragmentState>,
    ) -> Result<Self, ActorProcessingErr> {
        // Unsupervised spawn: used only in tests. Production pools go
        // through `spawn_linked` so the dispatcher restarts dead workers.
        let worker_count = config.workers.max(1);
        let pending = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(worker_count);
        let mut startup_children = WorkerStartupChildren::default();
        let startup = AssertUnwindSafe(async {
            for _ in 0..worker_count {
                let slot_pending = Arc::new(AtomicU64::new(0));
                let (worker, _handle) = PoolWorker::spawn(
                    None,
                    PoolWorker,
                    PoolWorkerArgs {
                        shared: shared.clone(),
                        pool_pending: pending.clone(),
                        slot_pending: slot_pending.clone(),
                        startup_gate: None,
                    },
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                startup_children.track(worker.get_cell());
                workers.push(WorkerSlot {
                    worker,
                    pending: slot_pending,
                });
            }
            Ok::<(), ActorProcessingErr>(())
        })
        .catch_unwind()
        .await;
        match startup {
            Ok(Ok(())) => startup_children.disarm(),
            Ok(Err(error)) => {
                startup_children.rollback().await;
                return Err(error);
            }
            Err(panic) => {
                startup_children.rollback().await;
                std::panic::resume_unwind(panic);
            }
        }
        Ok(Self {
            workers,
            rate_limiter: config
                .rate_limit_per_sec
                .map(|r| Mutex::new(TokenBucket::new(r))),
            pending,
            shed_count: Arc::new(AtomicU64::new(0)),
            config,
        })
    }

    /// Find the pool slot holding `cell`, if any.
    pub(super) fn slot_of(&self, cell: &ActorCell) -> Option<usize> {
        self.workers
            .iter()
            .position(|slot| slot.worker.get_id() == cell.get_id())
    }

    /// Replace the worker at `slot` with `worker` (supervised restart).
    pub(super) fn replace(
        &mut self,
        slot: usize,
        worker: ActorRef<KvMessage>,
        pending: Arc<AtomicU64>,
    ) {
        if let Some(worker_slot) = self.workers.get_mut(slot) {
            *worker_slot = WorkerSlot { worker, pending };
        }
    }

    /// Try to reserve the exact sticky worker for an op. The reservation rolls
    /// both counters back unless it is successfully handed to that worker.
    pub(super) async fn try_reserve(&self, key: &str) -> Option<WorkerReservation<'_>> {
        // Rate limit first.
        if let Some(limiter) = &self.rate_limiter {
            if !limiter.lock().await.try_acquire() {
                self.shed_count.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
        // Queue cap.
        let pending = self.pending.fetch_add(1, Ordering::Relaxed);
        if pending >= self.config.queue_cap as u64 {
            release_pending(&self.pending, 1);
            self.shed_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.reserve_slot(key))
    }

    /// Number of ops shed by this pool.
    pub(super) fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }

    /// Reserve an op without cap/rate checks. Replication and reconciliation
    /// cannot be discarded after their sender has accepted responsibility,
    /// but they still participate in exact lifecycle accounting.
    pub(super) fn reserve_unchecked(&self, key: &str) -> WorkerReservation<'_> {
        self.pending.fetch_add(1, Ordering::Relaxed);
        self.reserve_slot(key)
    }

    fn reserve_slot(&self, key: &str) -> WorkerReservation<'_> {
        let idx = worker_idx(key, self.workers.len());
        self.workers[idx].pending.fetch_add(1, Ordering::Relaxed);
        WorkerReservation {
            pool: self,
            slot: idx,
            handed_off: false,
        }
    }

    /// Reclaim every active or queued job abandoned by one terminated worker.
    /// Supervision calls this only after actor termination, so that worker can
    /// no longer race a normal completion decrement.
    pub(super) fn reclaim_slot(&self, slot: usize) -> u64 {
        let abandoned = self.workers[slot].pending.swap(0, Ordering::AcqRel);
        release_pending(&self.pending, abandoned);
        abandoned
    }

    /// Close every worker mailbox, drain the work accepted before that fence,
    /// and wait for every worker to exit. Closing all mailboxes before waiting
    /// prevents one slow shard from leaving its siblings open to new work.
    pub(super) fn begin_shutdown(&self) {
        for slot in &self.workers {
            if slot.worker.get_status() < ActorStatus::Draining {
                let _ = slot.worker.drain();
            }
        }
    }

    pub(super) async fn join(&self) {
        for slot in &self.workers {
            let _ = slot.worker.wait(None).await;
        }
    }

    pub(super) async fn shutdown(&self) {
        self.begin_shutdown();
        self.join().await;
    }
}

pub(super) struct WorkerReservation<'a> {
    pool: &'a WorkerPool,
    slot: usize,
    handed_off: bool,
}

impl WorkerReservation<'_> {
    /// Hand the reservation and message to the selected worker. An immediate
    /// send failure drops the reply port and rolls the reservation back, so
    /// callers observe actor unavailability rather than false backpressure.
    pub(super) fn send(mut self, message: KvMessage) {
        if self.pool.workers[self.slot]
            .worker
            .send_message(message)
            .is_ok()
        {
            self.handed_off = true;
        }
    }
}

impl Drop for WorkerReservation<'_> {
    fn drop(&mut self) {
        if !self.handed_off {
            release_pending(&self.pool.workers[self.slot].pending, 1);
            release_pending(&self.pool.pending, 1);
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

#[cfg(test)]
mod tests {
    // Copyright (c) Sean Lawlor
    //
    // This source code is licensed under both the MIT license found in the
    // LICENSE-MIT file in the root directory of this source tree.

    //! Managed-pool admission, routing, and worker lifecycle regressions.

    use super::*;
    use crate::actor::pool_worker::take_worker_get_gate;
    use crate::actor::{
        BatchReadEntry, KvActor, KvArgs, OpPriority, ReplicatedOp, RetainedBatchRead,
        VersionedValue,
    };
    use crate::shard::FragmentId;
    use ractor::{registry::where_is, rpc::CallResult, ActorStatus, SupervisionEvent};
    use std::collections::HashMap;
    use std::time::Duration;
    #[test]
    fn pool_builder_preserves_defaults_and_optional_overrides() {
        let built = PoolConfig::builder().build();
        let default = PoolConfig::default();
        assert_eq!(
            (built.workers, built.queue_cap, built.rate_limit_per_sec),
            (
                default.workers,
                default.queue_cap,
                default.rate_limit_per_sec
            )
        );
        assert_eq!((built.workers, built.queue_cap), (4, 10_000));
        assert_eq!(built.rate_limit_per_sec, None);
        let limited = PoolConfig::builder()
            .workers(3)
            .queue_cap(17)
            .rate_limit_per_sec(9)
            .build();
        assert_eq!(
            (
                limited.workers,
                limited.queue_cap,
                limited.rate_limit_per_sec
            ),
            (3, 17, Some(9))
        );
        assert_eq!(
            PoolConfig::builder()
                .maybe_rate_limit_per_sec(None)
                .build()
                .rate_limit_per_sec,
            None
        );
    }
    static NEXT_PREFIX: AtomicU64 = AtomicU64::new(0);

    struct SupervisionProbe;

    impl Actor for SupervisionProbe {
        type Msg = ();
        type State = tokio::sync::mpsc::UnboundedSender<SupervisionEvent>;
        type Arguments = tokio::sync::mpsc::UnboundedSender<SupervisionEvent>;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            events: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(events)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            _message: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            Ok(())
        }

        async fn handle_supervisor_evt(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: SupervisionEvent,
            events: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            let _ = events.send(message);
            Ok(())
        }
    }

    fn unique_prefix(label: &str) -> String {
        format!("{label}-{}", NEXT_PREFIX.fetch_add(1, Ordering::Relaxed))
    }

    fn pool_config(queue_cap: usize, rate_limit_per_sec: Option<u32>) -> PoolConfig {
        PoolConfig {
            workers: 1,
            queue_cap,
            rate_limit_per_sec,
        }
    }

    fn op(key: &str, timestamp_ms: u64) -> ReplicatedOp {
        ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(
                timestamp_ms.to_be_bytes().to_vec(),
                timestamp_ms,
                "test-node".to_string(),
            ),
            seq: timestamp_ms,
            boot_id: 1,
        }
    }

    async fn wait_for(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    #[tokio::test]
    async fn standalone_worker_pool_releases_capacity_and_honors_rate_limit() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = WorkerPool::spawn(pool_config(1, None), shared.clone())
            .await
            .unwrap();

        pool.try_reserve("first")
            .await
            .expect("first job should be admitted")
            .send(KvMessage::ApplyOp {
                op: op("first", 1),
                priority: OpPriority::Default,
                is_replication: false,
                reply: None,
            });
        wait_for(|| pool.pending.load(Ordering::Relaxed) == 0).await;
        assert!(shared.get("first").await.is_some());

        let second = pool
            .try_reserve("second")
            .await
            .expect("completed work did not release capacity");
        drop(second);
        assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
        assert_eq!(pool.workers[0].pending.load(Ordering::Relaxed), 0);
        pool.shutdown().await;

        let rate_limited = WorkerPool::spawn(pool_config(1, Some(1)), shared)
            .await
            .unwrap();
        drop(
            rate_limited
                .try_reserve("token")
                .await
                .expect("initial rate-limit token should be available"),
        );
        assert!(
            rate_limited.try_reserve("limited").await.is_none(),
            "standalone pool ignored its configured rate limit"
        );
        assert_eq!(rate_limited.shed_count(), 1);
        rate_limited.shutdown().await;
    }

    #[tokio::test]
    async fn batch_read_releases_each_reservation_before_routing_the_next_key() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(1, None);
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: unique_prefix("batch-cap-one"),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();

        let page = dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["first".to_string(), "second".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
            .await
            .unwrap();
        let CallResult::Success(page) = page else {
            panic!("batch read did not complete");
        };
        assert_eq!(page.entries.len(), 2);
        assert!(page.entries.iter().all(|entry| entry.result == Ok(None)));
        let counts = dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 0, 0)));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn batch_read_stops_before_admitting_an_omitted_suffix() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        shared.apply(&op("first", 1)).await.unwrap();
        shared.apply(&op("second", 2)).await.unwrap();
        let pool = pool_config(8, Some(1));
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: unique_prefix("batch-lazy-admission"),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();

        let page = dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["first".to_string(), "second".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: 0,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
            .await
            .unwrap();
        let CallResult::Success(page) = page else {
            panic!("batch read did not complete");
        };
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, "first");
        assert!(matches!(page.entries[0].result, Ok(Some(_))));
        let counts = dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        assert_eq!(
            counts,
            CallResult::Success((0, 0, 0)),
            "omitted suffix consumed a rate-limit token"
        );

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn batch_read_rate_limit_is_charged_and_reported_per_key() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(8, Some(1));
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: unique_prefix("batch-rate-limit"),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();

        let page = dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["first".to_string(), "second".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
            .await
            .unwrap();
        let CallResult::Success(page) = page else {
            panic!("batch read did not complete");
        };
        assert_eq!(page.entries[0].result, Ok(None));
        assert!(page.entries[1].result.is_err());
        let counts = dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 1, 0)));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_background_batch_read_does_not_stall_high_priority_or_health() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(8, None);
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: unique_prefix("batch-priority-isolation"),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let gate = install_worker_get_gate("blocked-batch".to_string());
        let batch_dispatcher = dispatcher.clone();
        let batch = tokio::spawn(async move {
            batch_dispatcher
                .call(
                    |reply| KvMessage::GetBatch {
                        keys: vec!["blocked-batch".to_string(), "later".to_string()],
                        response_base_payload_len: 0,
                        response_soft_limit: usize::MAX,
                        reply,
                        priority: OpPriority::Background,
                    },
                    None,
                )
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("background batch did not reach its worker");

        let high = tokio::time::timeout(
            Duration::from_secs(2),
            dispatcher.call(
                |reply| KvMessage::Get {
                    key: "high".to_string(),
                    reply,
                    priority: OpPriority::High,
                },
                None,
            ),
        )
        .await
        .expect("background batch blocked high-priority dispatch")
        .unwrap();
        assert_eq!(high, CallResult::Success(Ok(None)));
        let counts = dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 0, 0)));

        gate.release.notify_one();
        let CallResult::Success(page) = batch.await.unwrap() else {
            panic!("background batch did not complete");
        };
        assert_eq!(page.entries.len(), 2);

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retained_batch_lease_survives_cancellation_until_sticky_worker_drops_keys() {
        for kill in [false, true] {
            let shared = Arc::new(FragmentState::new(
                HashMap::new(),
                None,
                100,
                Duration::from_secs(3600),
            ));
            let pool = pool_config(1, None);
            let prefix = unique_prefix("retained-batch");
            let key = format!("{prefix}-blocked");
            let suffix = format!("{prefix}-suffix");
            let gate = install_worker_get_gate(key.clone());
            let suffix_gate = install_worker_get_gate(suffix.clone());
            let (dispatcher, _) = KvActor::spawn(
                None,
                KvActor,
                KvArgs {
                    shared,
                    fragment: FragmentId(0),
                    worker_prefix: prefix.clone(),
                    default_pool: pool.clone(),
                    high_pool: pool.clone(),
                    background_pool: pool,
                },
            )
            .await
            .unwrap();
            let retention = Arc::new(());
            let retained = Arc::downgrade(&retention);
            let batch_dispatcher = dispatcher.clone();
            let batch = tokio::spawn(async move {
                batch_dispatcher
                    .call(
                        move |reply| {
                            KvMessage::GetBatchRetained(RetainedBatchRead::new(
                                vec![key, suffix],
                                0,
                                usize::MAX,
                                reply,
                                OpPriority::Default,
                                retention,
                            ))
                        },
                        None,
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
                .await
                .unwrap();
            batch.abort();
            assert!(batch.await.unwrap_err().is_cancelled());
            assert!(
                retained.upgrade().is_some(),
                "cancellation freed actor-owned admission"
            );
            if kill {
                where_is(format!("{prefix}-default-worker-0"))
                    .unwrap()
                    .kill();
            } else {
                gate.release.notify_one();
            }
            wait_for(|| retained.upgrade().is_none()).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), suffix_gate.entered.notified())
                    .await
                    .is_err(),
                "cancelled request admitted its untouched suffix"
            );
            take_worker_get_gate(&format!("{prefix}-suffix"));
            dispatcher.drain().unwrap();
            dispatcher.wait(None).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batch_read_worker_loss_closes_reply_and_reclaims_its_reservation() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(1, None);
        let prefix = unique_prefix("batch-worker-loss");
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let gate = install_worker_get_gate("blocked-batch-loss".to_string());
        let batch_dispatcher = dispatcher.clone();
        let batch = tokio::spawn(async move {
            batch_dispatcher
                .call(
                    |reply| KvMessage::GetBatch {
                        keys: vec!["blocked-batch-loss".to_string(), "later".to_string()],
                        response_base_payload_len: 0,
                        response_soft_limit: usize::MAX,
                        reply,
                        priority: OpPriority::Default,
                    },
                    None,
                )
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("batch did not reach the worker");

        let worker_name = format!("{prefix}-default-worker-0");
        let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
        let old_id = old_worker.get_id();
        old_worker.kill();
        wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
            .await;
        assert!(matches!(batch.await.unwrap(), CallResult::SenderError));

        let recovered = dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["recovered".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(recovered, CallResult::Success(_)));
        let counts = dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 0, 0)));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batch_read_orderly_drain_returns_the_admitted_prefix() {
        for key_count in [1, 2] {
            let shared = Arc::new(FragmentState::new(
                HashMap::new(),
                None,
                100,
                Duration::from_secs(3600),
            ));
            let pool = pool_config(1, None);
            let prefix = unique_prefix("batch-orderly-drain");
            let first = format!("{prefix}-first");
            let keys = vec![first.clone(), format!("{prefix}-suffix")];
            let (dispatcher, _handle) = KvActor::spawn(
                None,
                KvActor,
                KvArgs {
                    shared,
                    fragment: FragmentId(0),
                    worker_prefix: prefix,
                    default_pool: pool.clone(),
                    high_pool: pool.clone(),
                    background_pool: pool,
                },
            )
            .await
            .unwrap();
            let gate = install_worker_get_gate(first.clone());
            let (reply, response) = tokio::sync::oneshot::channel();
            dispatcher
                .send_message(KvMessage::GetBatch {
                    keys: keys.into_iter().take(key_count).collect(),
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply: reply.into(),
                    priority: OpPriority::Default,
                })
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
                .await
                .expect("batch did not enter its worker");

            dispatcher.drain().unwrap();
            gate.release.notify_one();
            let page = tokio::time::timeout(Duration::from_secs(2), response)
                .await
                .expect("admitted read did not finish during drain")
                .expect("orderly drain dropped the completed prefix");
            assert_eq!(
                page.entries,
                vec![BatchReadEntry {
                    key: first,
                    result: Ok(None),
                }]
            );
            tokio::time::timeout(Duration::from_secs(2), dispatcher.wait(None))
                .await
                .expect("batch prevented orderly worker drain")
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batch_read_cancellation_retains_admission_until_worker_completion() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(1, None);
        let prefix = unique_prefix("batch-cancelled");
        let first = format!("{prefix}-first");
        let suffix = format!("{prefix}-suffix");
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix,
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let gate = install_worker_get_gate(first.clone());
        let suffix_gate = install_worker_get_gate(suffix.clone());
        let (reply, response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::GetBatch {
                keys: vec![first, suffix.clone()],
                response_base_payload_len: 0,
                response_soft_limit: usize::MAX,
                reply: reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("batch did not enter its worker");
        drop(response);

        let read = || {
            dispatcher.call(
                |reply| KvMessage::Get {
                    key: "probe".to_string(),
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
        };
        assert!(matches!(read().await.unwrap(), CallResult::Success(Err(_))));
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(read().await.unwrap(), CallResult::Success(Ok(None))) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled batch did not release its completed reservation");
        assert!(
            take_worker_get_gate(&suffix).is_some(),
            "cancelled batch admitted its untouched suffix"
        );
        drop(suffix_gate);
        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn immediate_worker_send_failure_rolls_back_both_counters() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = WorkerPool::spawn(pool_config(1, None), shared)
            .await
            .unwrap();
        let reservation = pool
            .try_reserve("dead")
            .await
            .expect("job should reserve the empty pool");
        let slot = reservation.slot;
        let worker = pool.workers[slot].worker.clone();
        worker.stop(None);
        worker.wait(None).await.unwrap();

        let (reply, response) = tokio::sync::oneshot::channel();
        reservation.send(KvMessage::Get {
            key: "dead".to_string(),
            reply: reply.into(),
            priority: OpPriority::Default,
        });

        assert!(response.await.is_err(), "failed send fabricated a reply");
        assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
        assert_eq!(pool.workers[slot].pending.load(Ordering::Relaxed), 0);
        assert_eq!(pool.shed_count(), 0, "unavailability was counted as shed");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_get_holds_admission_until_worker_terminal() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = WorkerPool::spawn(pool_config(1, None), shared.clone())
            .await
            .unwrap();
        let cache_guard = shared.lock_cache_for_test().await;
        let (reply, response) = tokio::sync::oneshot::channel();
        pool.try_reserve("cancelled")
            .await
            .expect("first read should be admitted")
            .send(KvMessage::Get {
                key: "cancelled".to_string(),
                reply: reply.into(),
                priority: OpPriority::Default,
            });
        drop(response);

        assert_eq!(pool.pending.load(Ordering::Relaxed), 1);
        assert!(
            pool.try_reserve("next").await.is_none(),
            "caller cancellation released capacity before worker completion"
        );

        drop(cache_guard);
        wait_for(|| pool.pending.load(Ordering::Relaxed) == 0).await;
        drop(
            pool.try_reserve("next")
                .await
                .expect("worker completion did not release cancelled read"),
        );
        assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
        pool.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_kill_reclaims_active_and_queued_admissions() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let prefix = unique_prefix("reclaim-worker");
        let pool = pool_config(2, None);
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: FragmentId(0),
                worker_prefix: prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();

        // Hold the cache so one read is active and the next remains queued in
        // the same sticky worker. A third read proves both capacity slots are
        // reserved before the kill.
        let cache_guard = shared.lock_cache_for_test().await;
        let (first_reply, first_response) = tokio::sync::oneshot::channel();
        let (second_reply, second_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: "same-slot".to_string(),
                reply: first_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        dispatcher
            .send_message(KvMessage::Get {
                key: "same-slot".to_string(),
                reply: second_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        let shed = tokio::time::timeout(
            Duration::from_secs(1),
            dispatcher.call(
                |reply| KvMessage::Get {
                    key: "same-slot".to_string(),
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            ),
        )
        .await
        .expect("dispatcher blocked behind a worker read")
        .unwrap();
        assert!(matches!(shed, CallResult::Success(Err(_))));

        let worker_name = format!("{prefix}-default-worker-0");
        let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
        let old_id = old_worker.get_id();
        old_worker.kill();
        wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
            .await;
        assert!(first_response.await.is_err());
        assert!(second_response.await.is_err());

        // Keep the replacement blocked and fill the whole pool again. Both
        // replies must succeed after release; one leaked slot would shed one.
        let (fresh_one_reply, fresh_one_response) = tokio::sync::oneshot::channel();
        let (fresh_two_reply, fresh_two_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: "same-slot".to_string(),
                reply: fresh_one_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        dispatcher
            .send_message(KvMessage::Get {
                key: "same-slot".to_string(),
                reply: fresh_two_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();
        drop(cache_guard);
        assert!(fresh_one_response.await.unwrap().is_ok());
        assert!(fresh_two_response.await.unwrap().is_ok());

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_kill_reclaims_only_the_dead_slots_admissions() {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let prefix = unique_prefix("reclaim-one-slot");
        let pool = PoolConfig {
            workers: 2,
            queue_cap: 2,
            rate_limit_per_sec: None,
        };
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: FragmentId(0),
                worker_prefix: prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let key_for_slot = |wanted| {
            (0..1_000)
                .map(|candidate| format!("slot-key-{candidate}"))
                .find(|key| worker_idx(key, 2) == wanted)
                .expect("test keys should cover both worker slots")
        };
        let dead_key = key_for_slot(0);
        let healthy_key = key_for_slot(1);

        // Block one admitted read in each worker. The dispatcher health call
        // is a mailbox fence proving both reservations were handed off.
        let cache_guard = shared.lock_cache_for_test().await;
        let (dead_reply, dead_response) = tokio::sync::oneshot::channel();
        let (healthy_reply, healthy_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: dead_key.clone(),
                reply: dead_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        dispatcher
            .send_message(KvMessage::Get {
                key: healthy_key,
                reply: healthy_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        dispatcher
            .call(|reply| KvMessage::GetShedCounts { reply }, None)
            .await
            .unwrap();

        let worker_name = format!("{prefix}-default-worker-0");
        let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
        let old_id = old_worker.get_id();
        old_worker.kill();
        wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
            .await;
        assert!(dead_response.await.is_err());

        // Exactly one slot is now available. A fresh read for the replacement
        // consumes it; another read must shed while the healthy sibling still
        // owns its original reservation.
        let (fresh_reply, fresh_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: dead_key.clone(),
                reply: fresh_reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        let overflow = tokio::time::timeout(
            Duration::from_secs(2),
            dispatcher.call(
                |reply| KvMessage::Get {
                    key: dead_key,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            ),
        )
        .await;

        drop(cache_guard);
        assert!(healthy_response.await.unwrap().is_ok());
        assert!(fresh_response.await.unwrap().is_ok());
        assert!(matches!(
            overflow
                .expect("healthy sibling reservation was reclaimed")
                .unwrap(),
            CallResult::Success(Err(_))
        ));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_racing_worker_restart_still_drains_accepted_sibling_work() {
        let prefix = unique_prefix("restart-drain-race");
        let accepted_key = format!("{prefix}-accepted-before-drain");
        let shared = Arc::new(FragmentState::new(
            HashMap::from([(
                accepted_key.clone(),
                VersionedValue::new(b"value".to_vec(), 1, "test-node".to_string()),
            )]),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(8, None);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (probe, probe_handle) = SupervisionProbe::spawn(None, SupervisionProbe, event_tx)
            .await
            .unwrap();
        let (dispatcher, _handle) = KvActor::spawn_linked(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: FragmentId(0),
                worker_prefix: prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
            probe.get_cell(),
        )
        .await
        .unwrap();
        let dispatcher_id = dispatcher.get_id();

        // Keep accepted work active in a sibling pool. If the replacement
        // race turns into a dispatcher handler failure, ractor skips
        // `post_stop` and this worker is killed before it can reply.
        let accepted_gate = install_worker_get_gate(accepted_key.clone());
        let (accepted_reply, accepted_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: accepted_key.clone(),
                priority: OpPriority::High,
                reply: accepted_reply.into(),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), accepted_gate.entered.notified())
            .await
            .expect("accepted sibling work did not enter its worker");

        // Pause a default-worker replacement in pre_start. Draining the
        // dispatcher before releasing it makes ractor reject the new link,
        // deterministically exercising the status-check/spawn race.
        let worker_name = format!("{prefix}-default-worker-0");
        let startup_gate = install_worker_startup_gate(worker_name.clone());
        where_is(worker_name)
            .expect("default worker should be registered")
            .kill();
        tokio::time::timeout(Duration::from_secs(2), startup_gate.entered.notified())
            .await
            .expect("replacement did not enter pre_start");

        dispatcher.drain().unwrap();
        assert!(dispatcher.get_status() >= ActorStatus::Draining);
        startup_gate.release.notify_one();
        accepted_gate.release.notify_one();

        let accepted = tokio::time::timeout(Duration::from_secs(2), accepted_response)
            .await
            .expect("accepted sibling work did not finish during drain")
            .unwrap();
        assert!(matches!(accepted, Ok(Some(_))));
        tokio::time::timeout(Duration::from_secs(2), dispatcher.wait(None))
            .await
            .expect("dispatcher did not complete its orderly drain")
            .unwrap();
        let terminated_cleanly = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match event_rx.recv().await {
                    Some(SupervisionEvent::ActorTerminated(cell, _, _))
                        if cell.get_id() == dispatcher_id =>
                    {
                        break true;
                    }
                    Some(SupervisionEvent::ActorFailed(cell, _))
                        if cell.get_id() == dispatcher_id =>
                    {
                        break false;
                    }
                    Some(_) => {}
                    None => panic!("supervision probe stopped before dispatcher exit"),
                }
            }
        })
        .await
        .expect("supervisor did not observe dispatcher exit");
        assert!(
            terminated_cleanly,
            "replacement/drain race failed the dispatcher instead of terminating cleanly"
        );
        assert!(shared.get(&accepted_key).await.is_some());
        probe.stop(None);
        tokio::time::timeout(Duration::from_secs(2), probe_handle)
            .await
            .expect("supervision probe did not stop")
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_background_get_does_not_stall_high_priority_or_health() {
        let prefix = unique_prefix("get-hol");
        let blocked_key = format!("{prefix}-blocked");
        let shared = Arc::new(FragmentState::new(
            HashMap::from([(
                "hot".to_string(),
                VersionedValue::new(b"value".to_vec(), 1, "test-node".to_string()),
            )]),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(8, None);
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix,
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();

        let worker_gate = install_worker_get_gate(blocked_key.clone());
        let (background_reply, background_response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::Get {
                key: blocked_key,
                reply: background_reply.into(),
                priority: OpPriority::Background,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), worker_gate.entered.notified())
            .await
            .expect("background Get did not enter its worker");

        let health = tokio::time::timeout(
            Duration::from_secs(2),
            dispatcher.call(|reply| KvMessage::GetShedCounts { reply }, None),
        )
        .await;
        let high = tokio::time::timeout(
            Duration::from_secs(2),
            dispatcher.call(
                |reply| KvMessage::Get {
                    key: "hot".to_string(),
                    reply,
                    priority: OpPriority::High,
                },
                None,
            ),
        )
        .await;

        // Always release the worker before asserting so the old nested-RPC
        // behavior cannot strand the dispatcher after the timeout.
        worker_gate.release.notify_one();
        assert!(matches!(background_response.await.unwrap(), Ok(None)));
        assert!(matches!(
            health
                .expect("health query stalled behind a background worker")
                .unwrap(),
            CallResult::Success(_)
        ));
        assert!(matches!(
            high.expect("high-priority read stalled").unwrap(),
            CallResult::Success(Ok(Some(_)))
        ));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }
}
