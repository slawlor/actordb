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
mod tests;
