// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! The actor-first heart of actordb.
//!
//! A `KvActor` is the per-fragment dispatcher: it routes every operation
//! to one of its `KvWorker`s by `hash(key) % worker_count`. Sticky
//! dispatch preserves per-key ordering (a key's ops always land on the
//! same worker, whose mailbox is FIFO), while different keys execute
//! concurrently across workers.
//!
//! Workers share the fragment's state ([`FragmentState`](crate::actor::FragmentState)): one in-memory
//! cache, one persistence buffer, one [`KvStore`](crate::store::KvStore).
//! The cache is a `RwLock` — sticky dispatch means workers almost never
//! touch the same key, so contention is minimal. Replication is just
//! messages: a local write becomes a `ReplicatedOp` which the node's
//! transport layer forwards to the fragment's replica holders, and
//! inbound ops merge here with last-writer-wins. Because the merge is
//! idempotent and commutative, replicas converge without coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::rpc::CallResult;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

/// Priority for an operation. The dispatcher routes to separate worker
/// pools by priority, so bulk/background traffic can never starve
/// latency-sensitive ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OpPriority {
    /// Latency-sensitive reads/writes.
    High,
    /// Normal operations (default).
    #[default]
    Default,
    /// Bulk/scan traffic.
    Background,
}

/// Configuration for a worker pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of worker actors.
    pub workers: usize,
    /// Max pending ops before shedding. Shed ops are dropped and counted
    /// (explicit, not silent).
    pub queue_cap: usize,
    /// Token-bucket rate limit (ops/sec). `None` = unlimited.
    pub rate_limit_per_sec: Option<u32>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            queue_cap: 10_000,
            rate_limit_per_sec: None,
        }
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

use crate::shard::{hash_key, FragmentId};
use crate::store::KvStore;

/// A value stamped with its last-writer-wins version.
///
/// Versions are totally ordered by `(timestamp_ms, node_id)`: the higher
/// wall-clock timestamp wins, and the node id breaks ties deterministically
/// so every replica picks the same winner. `expires_at_ms` is an optional
/// wall-clock deadline (set via `put_with_ttl`); expired values are
/// invisible to reads and reaped by the fragment's cleaner.
///
/// A tombstone (`is_tombstone`) marks a deleted key. Tombstones participate
/// in LWW like writes: a tombstone with a higher timestamp shadows a live
/// value, and a live write with a higher timestamp resurrects the key.
/// On exact `(timestamp, node_id)` ties the tombstone wins (deletes are
/// explicit). Tombstones are shipped by sync and aged out by the cleaner
/// after `tombstone_gc_age`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub timestamp_ms: u64,
    pub node_id: String,
    /// Wall-clock expiry in ms since epoch. `None` = no expiry.
    pub expires_at_ms: Option<u64>,
    /// True if this is a deletion marker.
    pub is_tombstone: bool,
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, timestamp_ms: u64, node_id: String) -> Self {
        Self {
            value,
            timestamp_ms,
            node_id,
            expires_at_ms: None,
            is_tombstone: false,
        }
    }

    /// A tombstone for `node_id` at `timestamp_ms`.
    pub fn tombstone(timestamp_ms: u64, node_id: String) -> Self {
        Self {
            value: Vec::new(),
            timestamp_ms,
            node_id,
            expires_at_ms: None,
            is_tombstone: true,
        }
    }

    /// Returns `true` if `self` wins over `other` under last-writer-wins.
    /// Tombstones win exact `(timestamp, node_id)` ties.
    pub fn wins_over(&self, other: &VersionedValue) -> bool {
        (self.timestamp_ms, &self.node_id, self.is_tombstone)
            > (other.timestamp_ms, &other.node_id, other.is_tombstone)
    }

    /// Returns `true` if this value is past its expiry at `now_ms`.
    /// Tombstones never expire by TTL (they're aged by GC instead).
    pub fn is_expired(&self, now_ms: u64) -> bool {
        !self.is_tombstone && matches!(self.expires_at_ms, Some(exp) if now_ms >= exp)
    }
}

/// A single replicated write operation.
///
/// `seq` is a per-origin-node sequence number and `boot_id` a random
/// identifier minted at process start; together with the origin node id
/// they form the transport layer's dedup key to suppress re-broadcast
/// storms. Neither plays a role in conflict resolution.
#[derive(Debug, Clone)]
pub struct ReplicatedOp {
    pub key: String,
    pub value: VersionedValue,
    pub seq: u64,
    pub boot_id: u64,
}

/// Messages handled by the [KvActor].
pub enum KvMessage {
    /// Apply a write — local or replicated. Merged with last-writer-wins,
    /// so applying the same op twice is harmless (idempotent).
    ApplyOp {
        op: ReplicatedOp,
        priority: OpPriority,
    },
    /// Read a single key from the local copy. The reply is
    /// `Ok(value)` on success, `Err(shed_reason)` if the op was shed
    /// due to backpressure (never confused with "not found").
    Get {
        key: String,
        reply: RpcReplyPort<Result<Option<VersionedValue>, String>>,
        priority: OpPriority,
    },
    /// Read the full local copy (used for anti-entropy sync).
    GetAll {
        reply: RpcReplyPort<HashMap<String, VersionedValue>>,
    },
    /// Query per-pool shed counts (high, default, background).
    GetShedCounts {
        reply: RpcReplyPort<(u64, u64, u64)>,
    },
}

/// The fragment's shared state: one in-memory cache, one persistence
/// buffer, one [`KvStore`].
///
/// All `KvWorker`s of a fragment share this. Sticky dispatch routes each
/// key to one worker, so workers rarely contend on the same key; the
/// `RwLock` is a safety net, not a hot path.
///
/// Applied writes are buffered and flushed to the [`KvStore`]
/// in batches (see `NodeConfig` flush knobs); the cache is reloaded from
/// the store on boot, so a restarted node keeps its dataset. A crash
/// can lose the unflushed tail of the write buffer; the handshake sync
/// on reconnect reconciles it from peers.
pub struct FragmentState {
    /// In-memory cache of the fragment: the fast path for all reads.
    cache: RwLock<HashMap<String, VersionedValue>>,
    /// Optional durable layer behind the cache. `None` = no persistence.
    kvstore: Option<Arc<dyn KvStore>>,
    /// Buffered writes awaiting flush.
    flush: Mutex<FlushBuffer>,
    flush_batch_size: usize,
    flush_interval: Duration,
}

struct FlushBuffer {
    pending: Vec<(String, VersionedValue)>,
    last_flush: Instant,
}

impl FragmentState {
    pub fn new(
        cache: HashMap<String, VersionedValue>,
        kvstore: Option<Arc<dyn KvStore>>,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        Self {
            cache: RwLock::new(cache),
            kvstore,
            flush: Mutex::new(FlushBuffer {
                pending: Vec::new(),
                last_flush: Instant::now(),
            }),
            flush_batch_size,
            flush_interval,
        }
    }

    /// Merge an op into the cache, keeping whichever version wins.
    /// Returns `true` if the op replaced the stored version. Ops that
    /// are already expired are dropped — storing them would only wait
    /// for the cleaner to reap them.
    pub async fn apply(&self, op: &ReplicatedOp) -> bool {
        if op.value.is_expired(now_ms()) {
            return false;
        }
        let mut cache = self.cache.write().await;
        let replace = match cache.get(&op.key) {
            Some(existing) => op.value.wins_over(existing),
            None => true,
        };
        if replace {
            cache.insert(op.key.clone(), op.value.clone());
        }
        replace
    }

    /// Buffer an applied write, flushing to the store when the batch is
    /// full or the flush interval has elapsed. Called only for ops that
    /// won the merge. Memory is always updated first, so reads never
    /// wait for a flush. No-op when no store is configured. The store
    /// write happens without holding the buffer lock.
    pub async fn buffer(
        &self,
        key: &str,
        value: &VersionedValue,
    ) -> Result<(), ActorProcessingErr> {
        if self.kvstore.is_none() {
            return Ok(());
        }
        let batch = {
            let mut flush = self.flush.lock().await;
            flush.pending.push((key.to_string(), value.clone()));
            if flush.pending.len() >= self.flush_batch_size
                || flush.last_flush.elapsed() >= self.flush_interval
            {
                Some(flush.take_batch())
            } else {
                None
            }
        };
        if let Some(batch) = batch {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Write all buffered writes to the store in one batch.
    pub async fn flush(&self) -> Result<(), ActorProcessingErr> {
        let batch = {
            let mut flush = self.flush.lock().await;
            if flush.pending.is_empty() {
                return Ok(());
            }
            flush.take_batch()
        };
        self.write_batch(batch).await
    }

    async fn write_batch(
        &self,
        batch: Vec<(Vec<u8>, VersionedValue)>,
    ) -> Result<(), ActorProcessingErr> {
        if let Some(kvstore) = &self.kvstore {
            kvstore
                .write_batch(&batch)
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        }
        Ok(())
    }

    /// Read a key, treating expired values and tombstones as absent.
    pub async fn get(&self, key: &str) -> Option<VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .get(key)
            .filter(|v| !v.is_tombstone && !v.is_expired(now))
            .cloned()
    }

    /// Read the full cache, excluding expired values but *including*
    /// tombstones: sync must ship tombstones so deletes propagate.
    /// Callers serving user reads should filter tombstones out.
    pub async fn get_all(&self) -> HashMap<String, VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .iter()
            .filter(|(_, v)| !v.is_expired(now))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Remove tombstones older than `gc_age_ms`. Returns the count
    /// reaped. Only call once converged; a partitioned node rejoining
    /// with a pre-delete live value would resurrect the key.
    pub async fn gc_tombstones(&self, now_ms: u64, gc_age_ms: u64) -> usize {
        let mut cache = self.cache.write().await;
        let before = cache.len();
        cache.retain(|_, v| {
            if !v.is_tombstone {
                return true;
            }
            now_ms.saturating_sub(v.timestamp_ms) < gc_age_ms
        });
        before - cache.len()
    }

    /// Remove all expired keys. Returns the number reaped. Called by the
    /// fragment's cleaner; reads already hide expired values, so this is
    /// memory hygiene, not a correctness gate.
    pub async fn remove_expired(&self, now_ms: u64) -> usize {
        let mut cache = self.cache.write().await;
        let before = cache.len();
        cache.retain(|_, v| !v.is_expired(now_ms));
        before - cache.len()
    }
}

impl FlushBuffer {
    /// Drain the pending writes and stamp the flush time. Caller must
    /// write the batch without holding the buffer lock.
    fn take_batch(&mut self) -> Vec<(Vec<u8>, VersionedValue)> {
        let batch: Vec<(Vec<u8>, VersionedValue)> = self
            .pending
            .iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
            .collect();
        self.pending.clear();
        self.last_flush = Instant::now();
        batch
    }
}

/// Arguments to spawn a [KvActor].
#[derive(Clone)]
pub struct KvArgs {
    /// The fragment's shared state, created by the supervisor (which
    /// also hands it to the cleaner and snapshotter).
    pub shared: Arc<FragmentState>,
    /// Owning node id, for unique worker naming.
    /// Which fragment this actor serves. Used for debug output.
    pub fragment: FragmentId,
    /// Prefix for worker actor names (must be unique per dispatcher
    /// in the process; the supervisor generates it).
    pub worker_prefix: String,
    /// Worker pool configs by priority. Keys are sticky-routed by
    /// `hash(key) % pool.workers` within the selected pool.
    pub high_pool: PoolConfig,
    pub default_pool: PoolConfig,
    pub background_pool: PoolConfig,
}

/// A fragment's worker: applies ops and serves reads against the shared
/// [`FragmentState`]. One worker's mailbox is FIFO, and sticky dispatch
/// sends each key to one worker, so per-key ordering holds while
/// different keys execute concurrently.
pub struct KvWorker;

/// Arguments to spawn a [KvWorker].
#[derive(Clone)]
pub struct KvWorkerArgs {
    /// The fragment state this worker shares with its siblings.
    pub shared: Arc<FragmentState>,
    /// Pending-op counter for the pool (decremented after each op).
    /// `None` for unsupervised test workers.
    pub pending: Option<Arc<AtomicU64>>,
}

impl Actor for KvWorker {
    type Msg = KvMessage;
    type State = (Arc<FragmentState>, Option<Arc<AtomicU64>>);
    type Arguments = KvWorkerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok((args.shared, args.pending))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let result = match message {
            KvMessage::ApplyOp { op, priority: _ } => {
                // Memory first (read-your-write never waits for disk),
                // then buffer for the next batched flush. A DB error
                // fails the actor loudly rather than diverging silently.
                if state.0.apply(&op).await {
                    state.0.buffer(&op.key, &op.value).await?;
                }
                Ok(())
            }
            KvMessage::Get {
                key,
                reply,
                priority: _,
            } => {
                // The receiver may be gone (e.g. timed-out caller); that's fine.
                let _ = reply.send(Ok(state.0.get(&key).await));
                Ok(())
            }
            // GetAll is served by the dispatcher straight from shared state.
            KvMessage::GetAll { .. } => Ok(()),
            // Shed counts are a dispatcher-level query; workers don't track them.
            KvMessage::GetShedCounts { reply } => {
                let _ = reply.send((0, 0, 0));
                Ok(())
            }
        };
        // Release the pool's pending slot.
        if let Some(pending) = &state.1 {
            pending.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }
}

/// The worker index for `key`: sticky across calls, so a key's ops
/// always land on the same worker.
pub fn worker_idx(key: &str, worker_count: usize) -> usize {
    hash_key(key.as_bytes()) as usize % worker_count
}

/// Factory and lifecycle owner for a fragment's worker pool.
///
/// The pool spawns the workers, routes keys to them by sticky hash, and
/// drains them on shutdown. The dispatcher holds a pool and does nothing
/// else with worker lifecycle — this is the seam Phase 4's supervision
/// (health checks, restarts) will build on.
/// A pool of workers with explicit backpressure: bounded queue,
/// token-bucket rate limiting, and counted sheds (never silent).
pub struct WorkerPool {
    workers: Vec<ActorRef<KvMessage>>,
    config: PoolConfig,
    /// Ops currently pending (sent but not yet processed).
    pending: Arc<AtomicU64>,
    /// Ops shed due to queue cap or rate limit (explicit, counted).
    shed_count: Arc<AtomicU64>,
    rate_limiter: Option<Mutex<TokenBucket>>,
}

impl WorkerPool {
    /// Spawn workers sharing `shared` state, linked to `supervisor`
    /// so a dead worker raises a supervision event there. Workers are
    /// named `<prefix>-worker-<slot>` for registry lookup.
    pub async fn spawn_linked(
        config: PoolConfig,
        shared: Arc<FragmentState>,
        supervisor: ActorCell,
        worker_prefix: &str,
    ) -> Result<Self, ActorProcessingErr> {
        let worker_count = config.workers.max(1);
        let pending = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(worker_count);
        for slot in 0..worker_count {
            let name = format!("{worker_prefix}-worker-{slot}");
            let (worker, _handle) = KvWorker::spawn_linked(
                Some(name),
                KvWorker,
                KvWorkerArgs {
                    shared: shared.clone(),
                    pending: Some(pending.clone()),
                },
                supervisor.clone(),
            )
            .await
            .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
            workers.push(worker);
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
    pub async fn spawn(
        config: PoolConfig,
        shared: Arc<FragmentState>,
    ) -> Result<Self, ActorProcessingErr> {
        // Unsupervised spawn: used only in tests. Production pools go
        // through `spawn_linked` so the dispatcher restarts dead workers.
        let worker_count = config.workers.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (worker, _handle) = KvWorker::spawn(
                None,
                KvWorker,
                KvWorkerArgs {
                    shared: shared.clone(),
                    pending: None,
                },
            )
            .await
            .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
            workers.push(worker);
        }
        Ok(Self {
            workers,
            config,
            pending: Arc::new(AtomicU64::new(0)),
            shed_count: Arc::new(AtomicU64::new(0)),
            rate_limiter: None,
        })
    }

    /// Find the pool slot holding `cell`, if any.
    pub fn slot_of(&self, cell: &ActorCell) -> Option<usize> {
        self.workers
            .iter()
            .position(|w| w.get_id() == cell.get_id())
    }

    /// Replace the worker at `slot` with `worker` (supervised restart).
    pub fn replace(&mut self, slot: usize, worker: ActorRef<KvMessage>) {
        if let Some(w) = self.workers.get_mut(slot) {
            *w = worker;
        }
    }

    /// Try to admit an op: checks rate limit and queue cap.
    /// Returns `true` if admitted, `false` if shed (counted).
    pub async fn try_admit(&self) -> bool {
        // Rate limit first.
        if let Some(limiter) = &self.rate_limiter {
            if !limiter.lock().await.try_acquire() {
                self.shed_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
        // Queue cap.
        let pending = self.pending.fetch_add(1, Ordering::Relaxed);
        if pending >= self.config.queue_cap as u64 {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            self.shed_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Number of ops shed by this pool.
    pub fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }

    /// Sticky route: pick a worker by key hash.
    pub fn route(&self, key: &str) -> &ActorRef<KvMessage> {
        let idx = worker_idx(key, self.workers.len());
        &self.workers[idx]
    }

    /// Stop every worker, waiting for each. Call before flushing shared
    /// state so no new writes land mid-flush.
    pub async fn shutdown(&self) {
        for worker in &self.workers {
            let _ = worker.stop_and_wait(None, None).await;
        }
    }
}

/// The per-fragment dispatcher. One runs per fragment this node serves:
/// it owns the [`FragmentState`] and a [`WorkerPool`], and routes every
/// operation to a worker by sticky key hash. `GetAll` is served directly
/// from shared state.
pub struct KvActor;

/// The dispatcher's state: the worker pool and the state they share.
pub struct KvState {
    high_pool: WorkerPool,
    default_pool: WorkerPool,
    background_pool: WorkerPool,
    shared: Arc<FragmentState>,
    worker_prefix: String,
}

impl Actor for KvActor {
    type Msg = KvMessage;
    type State = KvState;
    type Arguments = KvArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // Workers are linked to the dispatcher: a dead worker raises a
        // supervision event here and gets replaced in-place.
        // Phase 7: three isolated pools by priority.
        let high_pool = WorkerPool::spawn_linked(
            args.high_pool.clone(),
            args.shared.clone(),
            myself.get_cell(),
            &format!("{}-high", args.worker_prefix),
        )
        .await?;
        let default_pool = WorkerPool::spawn_linked(
            args.default_pool.clone(),
            args.shared.clone(),
            myself.get_cell(),
            &format!("{}-default", args.worker_prefix),
        )
        .await?;
        let background_pool = WorkerPool::spawn_linked(
            args.background_pool.clone(),
            args.shared.clone(),
            myself.get_cell(),
            &format!("{}-bg", args.worker_prefix),
        )
        .await?;
        Ok(KvState {
            high_pool,
            default_pool,
            background_pool,
            shared: args.shared,
            worker_prefix: args.worker_prefix.clone(),
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Drain workers first so no new writes land mid-flush, then don't
        // leave buffered writes behind. (A kill -9 skips this; the
        // unflushed tail is recovered via peer sync on reconnect.)
        state.high_pool.shutdown().await;
        state.default_pool.shutdown().await;
        state.background_pool.shutdown().await;
        state.shared.flush().await
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp { op, priority } => {
                let pool = match priority {
                    OpPriority::High => &state.high_pool,
                    OpPriority::Default => &state.default_pool,
                    OpPriority::Background => &state.background_pool,
                };
                // Phase 7: admission control. Shed explicitly if the
                // pool is over its queue cap or rate limit.
                if !pool.try_admit().await {
                    // Shed: counted in pool.shed_count, not silent.
                    // For fire-and-forget ApplyOp, we just drop.
                    return Ok(());
                }
                pool.route(&op.key)
                    .send_message(KvMessage::ApplyOp { op, priority })
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                Ok(())
            }
            KvMessage::Get {
                key,
                reply,
                priority,
            } => {
                // Same sticky route as the write path, so a Get issued
                // after an ApplyOp at the same priority lands behind it
                // in the worker's FIFO mailbox — the read-your-write
                // barrier holds within a priority pool. Cross-priority
                // sequences make no ordering promise (separate pools).
                // The worker replies through the caller's reply channel
                // directly. On worker timeout/error the reply is dropped
                // and the caller sees a SenderError.
                let pool = match priority {
                    OpPriority::High => &state.high_pool,
                    OpPriority::Default => &state.default_pool,
                    OpPriority::Background => &state.background_pool,
                };
                if !pool.try_admit().await {
                    // Shed: explicit error, not "not found". The caller
                    // sees NodeError::Backpressure, distinct from Ok(None).
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".to_string()));
                    return Ok(());
                }
                let worker = pool.route(&key);
                if let Ok(CallResult::Success(value)) = worker
                    .call(
                        |r| KvMessage::Get {
                            key,
                            reply: r,
                            priority,
                        },
                        None,
                    )
                    .await
                {
                    let _ = reply.send(value);
                }
                Ok(())
            }
            KvMessage::GetAll { reply } => {
                let _ = reply.send(state.shared.get_all().await);
                Ok(())
            }
            KvMessage::GetShedCounts { reply } => {
                let counts = (
                    state.high_pool.shed_count(),
                    state.default_pool.shed_count(),
                    state.background_pool.shed_count(),
                );
                let _ = reply.send(counts);
                Ok(())
            }
        }
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // A dead worker is replaced in-place: the pool slot keeps its
        // index, so sticky routing is unaffected and no keys move.
        let dead = match &message {
            SupervisionEvent::ActorFailed(cell, _) => Some(cell.clone()),
            SupervisionEvent::ActorTerminated(cell, _, _) => Some(cell.clone()),
            _ => None,
        };
        if let Some(cell) = dead {
            // Check all three pools for the dead worker.
            let mut found = None;
            for (pool, suffix) in [
                (&mut state.high_pool, "high"),
                (&mut state.default_pool, "default"),
                (&mut state.background_pool, "bg"),
            ] {
                if let Some(slot) = pool.slot_of(&cell) {
                    found = Some((pool, slot, suffix));
                    break;
                }
            }
            if let Some((pool, slot, suffix)) = found {
                let name = format!("{}-{suffix}-worker-{slot}", state.worker_prefix);
                let (worker, _handle) = KvWorker::spawn_linked(
                    Some(name),
                    KvWorker,
                    KvWorkerArgs {
                        shared: state.shared.clone(),
                        pending: Some(pool.pending.clone()),
                    },
                    myself.get_cell(),
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                pool.replace(slot, worker);
            }
        }
        Ok(())
    }
}

/// Current wall-clock time in milliseconds since the unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Hybrid logical clock (simplified): monotonic per node, tracks the
/// max of wall-clock and observed remote timestamps.
///
/// Pure wall-clock LWW breaks under clock skew: a fast-clocked node's
/// writes shadow newer data from slow-clocked nodes, silently. HLC
/// keeps the `(timestamp, node_id)` tiebreak shape so the merge stays
/// idempotent and commutative, but guarantees per-node monotonicity and
/// moves forward when it sees a higher remote timestamp.
///
/// Timestamps are still wall-clock-meaningful when clocks agree; under
/// skew they degrade to logical ordering, which is the correct
/// tradeoff for LWW.
#[derive(Debug, Clone)]
pub struct HlcClock {
    last_ms: u64,
    /// Wall-clock offset in ms. Zero in production; tests use it to
    /// simulate clock skew between nodes.
    offset_ms: i64,
}

impl HlcClock {
    pub fn new() -> Self {
        Self::with_offset(0)
    }

    /// Create a clock whose wall-clock reads are shifted by `offset_ms`.
    /// Used to simulate clock skew in tests.
    pub fn with_offset(offset_ms: i64) -> Self {
        Self {
            last_ms: 0,
            offset_ms,
        }
    }

    fn wall(&self) -> u64 {
        let now = now_ms();
        if self.offset_ms >= 0 {
            now.saturating_add(self.offset_ms as u64)
        } else {
            now.saturating_sub(self.offset_ms.unsigned_abs())
        }
    }

    /// Timestamp for a local event: `max(wall, last + 1)`.
    pub fn tick(&mut self) -> u64 {
        let wall = self.wall();
        self.last_ms = wall.max(self.last_ms.saturating_add(1));
        self.last_ms
    }

    /// Observe a remote timestamp: `max(wall, last + 1, remote + 1)`.
    pub fn observe(&mut self, remote_ms: u64) {
        let wall = self.wall();
        self.last_ms = wall
            .max(self.last_ms.saturating_add(1))
            .max(remote_ms.saturating_add(1));
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_orders_by_timestamp_then_node_id() {
        let a = VersionedValue::new(vec![1], 100, "node-a".to_string());
        let b = VersionedValue::new(vec![2], 200, "node-a".to_string());
        let c = VersionedValue::new(vec![3], 200, "node-b".to_string());

        assert!(b.wins_over(&a));
        assert!(!a.wins_over(&b));
        // same timestamp: higher node id wins, deterministically
        assert!(c.wins_over(&b));
        assert!(!b.wins_over(&c));
        // identical versions never "win" — merge is a no-op
        assert!(!a.wins_over(&a));
    }
}

#[cfg(test)]
mod tombstone_hlc_tests {
    use super::*;

    #[test]
    fn tombstone_wins_exact_tie() {
        let ts = 1_000u64;
        let live = VersionedValue::new(b"v".to_vec(), ts, "node-a".to_string());
        let tomb = VersionedValue::tombstone(ts, "node-a".to_string());
        // Same (timestamp, node_id): tombstone wins the tie.
        assert!(tomb.wins_over(&live));
        assert!(!live.wins_over(&tomb));
        // Higher timestamp still beats the tombstone.
        let newer = VersionedValue::new(b"v2".to_vec(), ts + 1, "node-a".to_string());
        assert!(newer.wins_over(&tomb));
        assert!(!tomb.wins_over(&newer));
    }

    #[test]
    fn hlc_is_monotonic_and_observes_remote() {
        let mut hlc = HlcClock::new();
        let t1 = hlc.tick();
        let t2 = hlc.tick();
        assert!(t2 >= t1, "ticks must be monotonic");
        // Observing a far-future remote timestamp moves the clock past it.
        hlc.observe(t2 + 100_000);
        let t3 = hlc.tick();
        assert!(t3 > t2 + 100_000, "clock must move past observed remote");
        // Observing a stale timestamp never moves the clock backward.
        hlc.observe(1);
        let t4 = hlc.tick();
        assert!(t4 >= t3, "clock must never go backward");
    }
}
