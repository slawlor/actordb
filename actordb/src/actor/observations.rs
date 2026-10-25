//! Local bounded worker ownership observations; no payload/key metric labels.
use super::{KvMessage, OpPriority};
use crate::node::observability::{add_saturating, AtomicDurationSummary, DurationSummary};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);
const MAX_TIMED_QUEUED_JOBS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WorkerObservation {
    pub slot: usize,
    pub actor_id: String,
    pub incarnation: u64,
    pub state: ractor::ActorStatus,
    pub pending: u64,
    pub queued: u64,
    pub active: u64,
    pub accepted: u64,
    pub completed: u64,
    pub abandoned: u64,
    pub execution_duration: DurationSummary,
}
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PoolObservation {
    pub table: Option<String>,
    pub priority: OpPriority,
    pub incarnation: u64,
    pub queue_capacity: usize,
    pub worker_count: usize,
    pub workers: Vec<WorkerObservation>,
    pub workers_truncated: bool,
    pub pending: u64,
    pub queued: u64,
    pub active: u64,
    pub accepted: u64,
    pub completed: u64,
    pub abandoned: u64,
    pub shed_queue: u64,
    pub shed_rate: u64,
    pub shed_total: u64,
    pub timing_enabled: bool,
    pub oldest_queue_age: Option<Duration>,
    pub queue_duration: DurationSummary,
    pub execution_duration: DurationSummary,
}
#[doc(hidden)]
pub struct PoolObservationRequest {
    pub(crate) tables: Vec<String>,
    pub(crate) include_internal: bool,
    pub(crate) max_pools: usize,
    pub(crate) max_workers: usize,
    pub(crate) retention: Arc<dyn Send + Sync>,
}
#[derive(Debug, Clone, Default)]
pub struct PoolObservationPage {
    pub observed_at: Option<std::time::SystemTime>,
    pub pools: Vec<PoolObservation>,
    pub missing_tables: Vec<String>,
    pub pools_truncated: bool,
}
pub(super) struct WorkCounters {
    pub(super) incarnation: u64,
    pub(super) accepted: AtomicU64,
    pub(super) completed: AtomicU64,
    pub(super) abandoned: AtomicU64,
    pub(super) active: AtomicU64,
    pub(super) queued: AtomicU64,
    pub(super) execution: AtomicDurationSummary,
}
impl Default for WorkCounters {
    fn default() -> Self {
        Self {
            incarnation: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
            accepted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            active: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            execution: AtomicDurationSummary::default(),
        }
    }
}
pub(super) struct PoolCounters {
    pub(super) work: WorkCounters,
    pub(super) shed_queue: AtomicU64,
    pub(super) shed_rate: AtomicU64,
    pub(super) timing: Arc<AtomicBool>,
    queue_duration: AtomicDurationSummary,
    timed_queue: Mutex<BTreeMap<u64, Instant>>,
    next_id: AtomicU64,
}
impl PoolCounters {
    pub(super) fn new(timing: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            work: WorkCounters::default(),
            shed_queue: AtomicU64::new(0),
            shed_rate: AtomicU64::new(0),
            timing,
            queue_duration: AtomicDurationSummary::default(),
            timed_queue: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
        })
    }
    pub(super) fn oldest_queue_age(&self) -> Option<Duration> {
        if !self.timing.load(Ordering::Acquire) {
            return None;
        }
        let queue = self.timed_queue.try_lock().ok()?;
        if self.work.queued.load(Ordering::Acquire) as usize != queue.len() {
            return None;
        }
        Some(
            queue
                .values()
                .min()
                .map_or(Duration::ZERO, Instant::elapsed),
        )
    }
    pub(super) fn queue_duration(&self) -> DurationSummary {
        self.queue_duration.snapshot()
    }
}
#[doc(hidden)]
pub struct ObservedWorkerMessage {
    message: Box<KvMessage>,
    work: WorkerWork,
}
pub(crate) struct WorkerWork {
    pool: Arc<PoolCounters>,
    worker: Arc<WorkCounters>,
    queued_at: Option<Instant>,
    queue_id: Option<u64>,
    started_at: Option<Instant>,
    started: bool,
    completed: bool,
}
impl WorkerWork {
    pub(super) fn new(pool: Arc<PoolCounters>, worker: Arc<WorkCounters>) -> Self {
        let queued_at = pool.timing.load(Ordering::Acquire).then(Instant::now);
        add_saturating(&pool.work.accepted, 1);
        add_saturating(&worker.accepted, 1);
        pool.work.queued.fetch_add(1, Ordering::AcqRel);
        worker.queued.fetch_add(1, Ordering::AcqRel);
        let queue_id = queued_at.and_then(|instant| {
            let mut queue = pool.timed_queue.lock().unwrap();
            if queue.len() >= MAX_TIMED_QUEUED_JOBS {
                return None;
            }
            let id = pool.next_id.fetch_add(1, Ordering::Relaxed);
            queue.insert(id, instant);
            Some(id)
        });
        Self {
            pool,
            worker,
            queued_at,
            queue_id,
            started_at: None,
            started: false,
            completed: false,
        }
    }
    pub(super) fn wrap(self, message: KvMessage) -> KvMessage {
        KvMessage::ObservedWorker(ObservedWorkerMessage {
            message: Box::new(message),
            work: self,
        })
    }
    fn start(&mut self) {
        self.pool.work.queued.fetch_sub(1, Ordering::AcqRel);
        self.worker.queued.fetch_sub(1, Ordering::AcqRel);
        self.pool.work.active.fetch_add(1, Ordering::AcqRel);
        self.worker.active.fetch_add(1, Ordering::AcqRel);
        if let Some(id) = self.queue_id.take() {
            self.pool.timed_queue.lock().unwrap().remove(&id);
        }
        if let Some(queued) = self.queued_at {
            self.pool.queue_duration.observe(queued.elapsed());
            self.started_at = Some(Instant::now());
        }
        self.started = true;
    }
    pub(super) fn complete(&mut self) {
        self.completed = true;
    }
}
impl Drop for WorkerWork {
    fn drop(&mut self) {
        if let Some(id) = self.queue_id.take() {
            self.pool.timed_queue.lock().unwrap().remove(&id);
        }
        if self.started {
            self.pool.work.active.fetch_sub(1, Ordering::AcqRel);
            self.worker.active.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.pool.work.queued.fetch_sub(1, Ordering::AcqRel);
            self.worker.queued.fetch_sub(1, Ordering::AcqRel);
        }
        if self.completed {
            add_saturating(&self.pool.work.completed, 1);
            add_saturating(&self.worker.completed, 1);
        } else {
            add_saturating(&self.pool.work.abandoned, 1);
            add_saturating(&self.worker.abandoned, 1);
        }
        if let Some(started) = self.started_at {
            self.pool.work.execution.observe(started.elapsed());
            self.worker.execution.observe(started.elapsed());
        }
    }
}
impl KvMessage {
    pub(crate) fn into_observed_worker(self) -> (Self, Option<WorkerWork>) {
        if let Self::ObservedWorker(ObservedWorkerMessage { message, mut work }) = self {
            work.start();
            (*message, Some(work))
        } else {
            (self, None)
        }
    }
}
