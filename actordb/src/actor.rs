// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! The actor-first heart of actordb.
//!
//! A `KvActor` is the per-fragment dispatcher: it routes every operation
//! to one of its `KvWorker`s by `hash(key) % worker_count`. Sticky
//! dispatch preserves per-key ordering within a priority pool, while the
//! shared fragment state serializes conflicting writes across pools and
//! different keys execute concurrently across workers.
//!
//! Workers share the fragment's state ([`FragmentState`](crate::actor::FragmentState)): one in-memory
//! cache, one ordered persistence buffer, one [`KvStore`](crate::store::KvStore).
//! Cache merges and persistence enqueueing happen as one ordered operation,
//! including when two priority pools update the same key concurrently.
//! Replication is just
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
/// invisible to reads and converted to versioned tombstones by the fragment's
/// cleaner.
///
/// A tombstone (`is_tombstone`) marks a deleted key. Tombstones participate
/// in LWW like writes: a tombstone with a higher timestamp shadows a live
/// value, and a live write with a higher timestamp resurrects the key.
/// On exact `(timestamp, node_id)` ties the tombstone wins (deletes are
/// explicit). Tombstones are shipped by sync and retained until a future
/// causally safe garbage collector can prove that no replica needs them.
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

    /// Convert an expired live value into its deterministic deletion marker.
    ///
    /// The marker keeps the original LWW timestamp and writer identity, with
    /// the tombstone tie-break bit set. It therefore defeats versions older
    /// than the TTL write without incorrectly deleting a genuinely later
    /// concurrent put. The marker retains the original TTL deadline in
    /// `expires_at_ms`; tombstones themselves never expire through
    /// [`Self::is_expired`].
    pub fn expiry_tombstone(&self) -> Self {
        Self {
            value: Vec::new(),
            timestamp_ms: self.timestamp_ms,
            node_id: self.node_id.clone(),
            expires_at_ms: self.expires_at_ms,
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
    /// Tombstones never expire by TTL.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        !self.is_tombstone && matches!(self.expires_at_ms, Some(exp) if now_ms >= exp)
    }
}

/// A single replicated write operation.
///
/// `seq` is a per-origin-node sequence number and `boot_id` a random
/// identifier minted at process start. They are carried as origin metadata;
/// the current receiver does not deduplicate on them. Replay safety and
/// conflict resolution come from the value's LWW version.
#[derive(Debug, Clone)]
pub struct ReplicatedOp {
    pub key: String,
    pub value: VersionedValue,
    pub seq: u64,
    pub boot_id: u64,
}

/// Result of merging one exact version into a fragment.
///
/// Callers must use this result rather than infer success from a later read:
/// a concurrently present newer value does not prove that this operation was
/// ever applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// This operation won the LWW merge and changed the cache.
    Applied,
    /// The exact key/value/version was already present. Replaying it is an
    /// idempotent success.
    AlreadyPresent,
    /// A different version already wins the LWW merge.
    Superseded {
        timestamp_ms: u64,
        node_id: String,
        is_tombstone: bool,
    },
    /// The operation's TTL deadline had already passed; its deterministic
    /// expiry tombstone is now present or was already present.
    Expired,
}

/// Persistence level reached before acknowledging a replicated batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyDurability {
    /// The batch is visible in memory; no store flush is part of this fence.
    MemoryApplied,
    /// The configured [`KvStore`] accepted the fragment flush. This does not
    /// imply an fsync unless the store implementation provides that guarantee.
    StoreCommitted,
}

/// Failure before a fragment can produce an [`ApplyOutcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// Admission control rejected a client-originated mutation.
    Backpressure,
    /// Persistence failed while applying a batch-triggering mutation.
    Storage(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backpressure => write!(f, "fragment worker pool is at capacity"),
            Self::Storage(error) => write!(f, "persistence failed: {error}"),
        }
    }
}

impl std::error::Error for ApplyError {}

/// Messages handled by the [KvActor].
pub enum KvMessage {
    /// Apply a write — local or replicated. Merged with last-writer-wins,
    /// so applying the same op twice is harmless (idempotent).
    ///
    /// `is_replication`: true for ops arriving via acked replication
    /// (`ReplicateBatch`) or anti-entropy (`SyncResponse`, reconcile
    /// fetch). These bypass dispatcher admission control: the sender
    /// side is already bounded (`max_in_flight` + `buffer_capacity`),
    /// and shedding an acked op while the sender acks it would
    /// silently lose acknowledged writes (adversarial review #2).
    /// Client-forwarded writes (`ForwardedPut`, `BatchPut`) are NOT
    /// replication: they stay subject to admission control (and shed
    /// explicitly under backpressure).
    ApplyOp {
        op: ReplicatedOp,
        priority: OpPriority,
        is_replication: bool,
        /// Optional exact result. Callers that need an application or
        /// protocol acknowledgement supply this reply port.
        reply: Option<RpcReplyPort<Result<ApplyOutcome, ApplyError>>>,
    },
    /// Flush every persistence update currently ordered before this message.
    /// Replication uses this as an acknowledgement fence.
    Flush {
        reply: RpcReplyPort<Result<ApplyDurability, ApplyError>>,
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
    /// Read a reconciliation snapshot and the fragment-wide mutation
    /// revision captured under the same cache read lock.
    GetSnapshot {
        reply: RpcReplyPort<(HashMap<String, VersionedValue>, u64)>,
    },
    /// Read the current fragment-wide mutation revision.
    GetRevision { reply: RpcReplyPort<u64> },
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
    /// Advances for every winning cache mutation, including replicated
    /// writes and expiry conversion.
    revision: AtomicU64,
    /// Buffered writes awaiting flush.
    flush: Mutex<FlushBuffer>,
    /// Allows exactly one store write at a time for this fragment. The
    /// buffered batch remains recorded while the write is in progress, so a
    /// cancelled or failed flush can be retried without losing it.
    flush_in_progress: Mutex<()>,
    flush_batch_size: usize,
    flush_interval: Duration,
}

struct FlushBuffer {
    pending: Vec<(String, VersionedValue)>,
    /// The batch currently being attempted. It stays here until the store
    /// write succeeds; retries may therefore write the same versions again.
    in_flight: Option<Vec<(String, VersionedValue)>>,
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
            revision: AtomicU64::new(0),
            flush: Mutex::new(FlushBuffer {
                pending: Vec::new(),
                in_flight: None,
                last_flush: Instant::now(),
            }),
            flush_in_progress: Mutex::new(()),
            flush_batch_size,
            flush_interval,
        }
    }

    fn durability(&self) -> ApplyDurability {
        if self.kvstore.is_some() {
            ApplyDurability::StoreCommitted
        } else {
            ApplyDurability::MemoryApplied
        }
    }

    /// Merge an op into the cache and enqueue the same winning version for
    /// persistence as one ordered operation.
    ///
    /// Returns the exact merge outcome. Ops that are already expired install
    /// their deterministic expiry tombstone instead of disappearing. When
    /// persistence is configured, taking the flush-buffer lock before the
    /// cache lock ensures that concurrent
    /// workers cannot enqueue versions in a different order from the cache
    /// merge. This matters across priority pools, where the same key can be
    /// handled by different workers.
    pub async fn apply(&self, op: &ReplicatedOp) -> Result<ApplyOutcome, ActorProcessingErr> {
        let expired = op.value.is_expired(now_ms());
        let expiry_tombstone;
        let value = if expired {
            expiry_tombstone = op.value.expiry_tombstone();
            &expiry_tombstone
        } else {
            &op.value
        };

        if self.kvstore.is_none() {
            let mut cache = self.cache.write().await;
            let outcome = Self::merge_cache(&mut cache, &op.key, value);
            if outcome == ApplyOutcome::Applied {
                self.revision.fetch_add(1, Ordering::Release);
            }
            return Ok(Self::reported_outcome(outcome, expired));
        }

        let (outcome, should_flush) = {
            // Keep the winning cache mutation and its durable enqueue
            // adjacent, with no await point between them. Flushes may take a
            // batch before or after this critical section, never halfway
            // through it.
            let mut flush = self.flush.lock().await;
            let mut cache = self.cache.write().await;
            let outcome = Self::merge_cache(&mut cache, &op.key, value);
            if outcome != ApplyOutcome::Applied {
                return Ok(Self::reported_outcome(outcome, expired));
            }
            flush.pending.push((op.key.clone(), value.clone()));
            self.revision.fetch_add(1, Ordering::Release);
            let should_flush = flush.pending.len() >= self.flush_batch_size
                || flush.last_flush.elapsed() >= self.flush_interval;
            (outcome, should_flush)
        };

        if should_flush {
            self.flush().await?;
        }
        Ok(Self::reported_outcome(outcome, expired))
    }

    fn merge_cache(
        cache: &mut HashMap<String, VersionedValue>,
        key: &str,
        value: &VersionedValue,
    ) -> ApplyOutcome {
        match cache.get(key) {
            Some(existing) if existing == value => ApplyOutcome::AlreadyPresent,
            Some(existing) if !value.wins_over(existing) => ApplyOutcome::Superseded {
                timestamp_ms: existing.timestamp_ms,
                node_id: existing.node_id.clone(),
                is_tombstone: existing.is_tombstone,
            },
            _ => {
                cache.insert(key.to_string(), value.clone());
                ApplyOutcome::Applied
            }
        }
    }

    fn reported_outcome(outcome: ApplyOutcome, expired: bool) -> ApplyOutcome {
        if expired
            && matches!(
                &outcome,
                ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent
            )
        {
            ApplyOutcome::Expired
        } else {
            outcome
        }
    }

    /// Write the currently buffered writes to the store in one batch.
    ///
    /// At most one write is in progress per fragment. The authoritative copy
    /// of the batch remains in the buffer's in-flight slot until the write
    /// succeeds, so failure or cancellation cannot make a later flush forget
    /// it. Writes that arrive during the store call remain pending for the
    /// next flush.
    pub async fn flush(&self) -> Result<(), ActorProcessingErr> {
        let _single_flight = self.flush_in_progress.lock().await;
        let batch = {
            let mut flush = self.flush.lock().await;
            match flush.begin_batch() {
                Some(batch) => batch,
                None => return Ok(()),
            }
        };

        self.write_batch(&batch)?;
        self.flush.lock().await.commit_batch();
        Ok(())
    }

    fn write_batch(&self, batch: &[(String, VersionedValue)]) -> Result<(), ActorProcessingErr> {
        if let Some(kvstore) = &self.kvstore {
            // A busy key may appear several times in one batch. Send only
            // its LWW winner so backend implementations do not need to
            // define duplicate-key ordering within a batch.
            let mut coalesced: HashMap<Vec<u8>, VersionedValue> =
                HashMap::with_capacity(batch.len());
            for (key, value) in batch {
                let replace = match coalesced.get(key.as_bytes()) {
                    Some(existing) => value.wins_over(existing),
                    None => true,
                };
                if replace {
                    coalesced.insert(key.as_bytes().to_vec(), value.clone());
                }
            }
            let encoded: Vec<(Vec<u8>, VersionedValue)> = coalesced.into_iter().collect();
            kvstore
                .write_batch(&encoded)
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

    /// Read the full cache, including tombstones. Expired live values are
    /// synthesized as deterministic expiry tombstones even if the cleaner has
    /// not persisted their conversion yet, so synchronization never erases a
    /// version floor. Callers serving user reads should filter tombstones out.
    pub async fn get_all(&self) -> HashMap<String, VersionedValue> {
        self.snapshot_with_revision().await.0
    }

    /// Read a cache snapshot and its mutation revision atomically with
    /// respect to winning cache updates.
    pub async fn snapshot_with_revision(&self) -> (HashMap<String, VersionedValue>, u64) {
        let now = now_ms();
        let cache = self.cache.read().await;
        let snapshot = cache
            .iter()
            .map(|(key, value)| {
                let value = if value.is_expired(now) {
                    value.expiry_tombstone()
                } else {
                    value.clone()
                };
                (key.clone(), value)
            })
            .collect();
        let revision = self.revision.load(Ordering::Acquire);
        (snapshot, revision)
    }

    /// Read the mutation revision while holding the cache read lock.
    ///
    /// Writers advance the revision while holding the cache write lock, so
    /// taking the corresponding read lock makes this a fence against an
    /// in-progress winning mutation rather than a merely atomic observation.
    pub async fn current_revision(&self) -> u64 {
        let _cache = self.cache.read().await;
        self.revision.load(Ordering::Acquire)
    }

    /// Legacy compatibility shim for the removed time-based collector.
    ///
    /// This method intentionally retains every tombstone and returns zero.
    /// Elapsed time cannot prove that all replicas have durably observed a
    /// delete, so age-based removal can resurrect stale values.
    pub async fn gc_tombstones(&self, _now_ms: u64, _gc_age_ms: u64) -> usize {
        0
    }

    /// Convert all expired live values to durable expiry tombstones. Returns
    /// the number converted. Cache replacement and persistence enqueueing use
    /// the same lock order as normal writes, preserving their shared order.
    pub async fn expire_values(&self, now_ms: u64) -> Result<usize, ActorProcessingErr> {
        if self.kvstore.is_none() {
            let mut cache = self.cache.write().await;
            let mut converted = 0;
            for value in cache.values_mut() {
                if value.is_expired(now_ms) {
                    *value = value.expiry_tombstone();
                    converted += 1;
                }
            }
            if converted != 0 {
                self.revision.fetch_add(
                    u64::try_from(converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            return Ok(converted);
        }

        let (converted, should_flush) = {
            let mut flush = self.flush.lock().await;
            let mut cache = self.cache.write().await;
            let mut converted = 0;
            for (key, value) in cache.iter_mut() {
                if value.is_expired(now_ms) {
                    let tombstone = value.expiry_tombstone();
                    *value = tombstone.clone();
                    flush.pending.push((key.clone(), tombstone));
                    converted += 1;
                }
            }
            if converted != 0 {
                self.revision.fetch_add(
                    u64::try_from(converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            let should_flush = converted > 0
                && (flush.pending.len() >= self.flush_batch_size
                    || flush.last_flush.elapsed() >= self.flush_interval);
            (converted, should_flush)
        };

        if should_flush {
            self.flush().await?;
        }
        Ok(converted)
    }
}

impl FlushBuffer {
    /// Start or resume a flush attempt. The returned batch is a snapshot;
    /// its authoritative copy remains in `in_flight` until commit.
    ///
    /// Pending writes are appended to a failed/cancelled attempt before its
    /// retry, preserving the cache's winning-version order. The attempt time
    /// is stamped even on failure to avoid a hot retry loop against a sick
    /// store.
    fn begin_batch(&mut self) -> Option<Vec<(String, VersionedValue)>> {
        if self.in_flight.is_none() && self.pending.is_empty() {
            return None;
        }
        let pending = std::mem::take(&mut self.pending);
        self.in_flight.get_or_insert_with(Vec::new).extend(pending);
        self.last_flush = Instant::now();
        self.in_flight.clone()
    }

    /// Forget only the batch whose store write has completed. Writes that
    /// arrived during that call remain in `pending`.
    fn commit_batch(&mut self) {
        self.in_flight = None;
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
/// sends each key to one worker within a priority pool. Cross-pool LWW merges
/// are serialized by the shared state while different keys execute
/// concurrently.
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
            KvMessage::ApplyOp {
                op,
                priority: _,
                is_replication: _,
                reply,
            } => {
                // Cache merge and durable enqueue are one ordered operation.
                // A DB error fails the actor loudly rather than diverging
                // silently.
                match state.0.apply(&op).await {
                    Ok(outcome) => {
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(outcome));
                        }
                        Ok(())
                    }
                    Err(error) => {
                        if let Some(reply) = reply {
                            let _ = reply.send(Err(ApplyError::Storage(error.to_string())));
                        }
                        Err(error)
                    }
                }
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
            // Flush fences are handled by the dispatcher against the shared
            // state. They are never routed through a worker pool.
            KvMessage::Flush { reply } => {
                let _ = reply.send(Err(ApplyError::Storage(
                    "flush fence was routed to a worker".to_string(),
                )));
                Ok(())
            }
            // Snapshot queries are served by the dispatcher straight from
            // shared state.
            KvMessage::GetAll { .. }
            | KvMessage::GetSnapshot { .. }
            | KvMessage::GetRevision { .. } => Ok(()),
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

/// The worker index for `key`: sticky across calls within one pool, so a key's
/// operations at one priority always land on the same worker.
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

    /// Admit an op without cap/rate checks, only incrementing the
    /// pending counter. For replication-path ops that bypass admission
    /// control: the worker unconditionally releases one pending slot
    /// per op, so the increment must still happen to keep the
    /// accounting balanced (otherwise the counter underflows and the
    /// pool sheds everything).
    pub fn admit_unchecked(&self) {
        self.pending.fetch_add(1, Ordering::Relaxed);
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
            KvMessage::ApplyOp {
                op,
                priority,
                is_replication,
                reply,
            } => {
                let pool = match priority {
                    OpPriority::High => &state.high_pool,
                    OpPriority::Default => &state.default_pool,
                    OpPriority::Background => &state.background_pool,
                };
                // Phase 7: admission control. Shed explicitly if the
                // pool is over its queue cap or rate limit — unless this
                // is a replicated op: the replication sender is already
                // bounded, and shedding here while the sender acks would
                // silently lose acknowledged writes (adversarial #2).
                // Bypassed ops still take a pending slot (via
                // admit_unchecked) to keep the worker's release accounting
                // balanced.
                if is_replication {
                    pool.admit_unchecked();
                } else if !pool.try_admit().await {
                    // Shed: counted in pool.shed_count and returned to callers
                    // that requested an exact apply outcome.
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(ApplyError::Backpressure));
                    }
                    return Ok(());
                }
                pool.route(&op.key)
                    .send_message(KvMessage::ApplyOp {
                        op,
                        priority,
                        is_replication,
                        reply,
                    })
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                Ok(())
            }
            KvMessage::Get {
                key,
                reply,
                priority,
            } => {
                // Reads sticky-route within their selected priority pool.
                // Completed writes are already visible in the shared cache,
                // regardless of which pool applied them. The worker replies
                // through the caller's channel directly; on worker
                // timeout/error it is dropped and the caller sees a
                // SenderError.
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
            KvMessage::Flush { reply } => {
                let result = state
                    .shared
                    .flush()
                    .await
                    .map(|()| state.shared.durability())
                    .map_err(|error| ApplyError::Storage(error.to_string()));
                let _ = reply.send(result);
                // A failed fence keeps its in-flight buffer for retry. Report
                // the error to the protocol caller without killing the stable
                // dispatcher reference.
                Ok(())
            }
            KvMessage::GetAll { reply } => {
                let _ = reply.send(state.shared.get_all().await);
                Ok(())
            }
            KvMessage::GetSnapshot { reply } => {
                let _ = reply.send(state.shared.snapshot_with_revision().await);
                Ok(())
            }
            KvMessage::GetRevision { reply } => {
                let _ = reply.send(state.shared.current_revision().await);
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

/// A remote or persisted timestamp exceeds the configured future-skew bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockSkewError {
    pub timestamp_ms: u64,
    pub max_allowed_ms: u64,
}

impl std::fmt::Display for ClockSkewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "timestamp {} exceeds maximum allowed future time {}",
            self.timestamp_ms, self.max_allowed_ms
        )
    }
}

impl std::error::Error for ClockSkewError {}

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
    /// Maximum a remote or persisted timestamp may exceed our wall clock
    /// before it is rejected. Bounds `observe()` so a single far-future
    /// timestamp cannot permanently poison stored LWW versions.
    /// `u64::MAX` = unbounded (legacy behavior).
    max_skew_ms: u64,
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
            max_skew_ms: u64::MAX,
        }
    }

    /// Set the maximum accepted future skew for remote and persisted
    /// timestamps. Values beyond the bound are rejected, not clamped: storing
    /// the original value after clamping only the clock would still poison
    /// LWW ordering.
    pub fn with_max_skew(mut self, max_skew_ms: u64) -> Self {
        self.max_skew_ms = max_skew_ms;
        self
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

    /// Observe an accepted remote timestamp:
    /// `max(wall, last + 1, remote + 1)`.
    pub fn observe(&mut self, remote_ms: u64) -> Result<(), ClockSkewError> {
        let wall = self.wall();
        self.validate(remote_ms, wall)?;
        self.last_ms = wall
            .max(self.last_ms.saturating_add(1))
            .max(remote_ms.saturating_add(1));
        Ok(())
    }

    /// Seed the clock from a durable version before serving writes. This does
    /// not create an event; the next [`Self::tick`] advances beyond it.
    pub fn seed_persisted(&mut self, timestamp_ms: u64) -> Result<(), ClockSkewError> {
        let wall = self.wall();
        self.validate(timestamp_ms, wall)?;
        self.last_ms = self.last_ms.max(timestamp_ms);
        Ok(())
    }

    fn validate(&self, timestamp_ms: u64, wall_ms: u64) -> Result<(), ClockSkewError> {
        let max_allowed_ms = wall_ms.saturating_add(self.max_skew_ms);
        if timestamp_ms > max_allowed_ms {
            return Err(ClockSkewError {
                timestamp_ms,
                max_allowed_ms,
            });
        }
        Ok(())
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
        // Observing a far-future remote timestamp moves the clock past it
        // (unbounded by default, preserving legacy behavior).
        hlc.observe(t2 + 100_000).unwrap();
        let t3 = hlc.tick();
        assert!(t3 > t2 + 100_000, "clock must move past observed remote");
        // Observing a stale timestamp never moves the clock backward.
        hlc.observe(1).unwrap();
        let t4 = hlc.tick();
        assert!(t4 >= t3, "clock must never go backward");
    }

    #[test]
    fn hlc_rejects_far_future_remote_timestamps() {
        // Adversarial review #5: a far-future timestamp must not
        // permanently poison the clock.
        let mut hlc = HlcClock::new().with_max_skew(300_000); // 5 min
        let t1 = hlc.tick();
        // u64::MAX would previously drag the clock to u64::MAX forever.
        let error = hlc.observe(u64::MAX).unwrap_err();
        assert_eq!(error.timestamp_ms, u64::MAX);
        let t2 = hlc.tick();
        // Rejection leaves the clock near wall time instead of storing or
        // advancing to the poisonous timestamp.
        let wall = hlc.wall();
        assert!(
            t2 <= wall.saturating_add(10),
            "far-future remote must be rejected, got t2={t2} wall={wall}"
        );
        assert!(t2 >= t1, "clock must still be monotonic");
        // A moderately-ahead (within skew) remote still moves the clock.
        hlc.observe(wall + 60_000).unwrap();
        let t3 = hlc.tick();
        assert!(t3 > wall + 60_000, "in-skew remote must be observed");
    }

    #[test]
    fn hlc_seeds_from_persisted_versions() {
        let mut hlc = HlcClock::new().with_max_skew(300_000);
        let persisted = hlc.wall() + 60_000;
        hlc.seed_persisted(persisted).unwrap();
        assert!(
            hlc.tick() > persisted,
            "the first post-restart write must beat durable state"
        );
    }
}

#[cfg(test)]
mod flush_retention_tests {
    use super::*;
    use crate::store::{KvStore, StoreError};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Condvar, Mutex as StdMutex};
    use std::time::Duration;

    fn op(key: &str, value: VersionedValue) -> ReplicatedOp {
        ReplicatedOp {
            key: key.to_string(),
            value,
            seq: 0,
            boot_id: 0,
        }
    }

    /// A store that fails the first `failures` writes, then succeeds,
    /// recording everything it receives. Regression test for
    /// adversarial review #7 (failed flush must not discard data).
    struct FlakyStore {
        failures_left: StdMutex<usize>,
        written: StdMutex<Vec<(Vec<u8>, VersionedValue)>>,
    }

    impl KvStore for FlakyStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            {
                let mut left = self.failures_left.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    return Err(StoreError("injected failure".to_string()));
                }
            }
            self.written
                .lock()
                .unwrap()
                .extend(writes.iter().map(|(k, v)| (k.clone(), v.clone())));
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_flush_retains_batch_for_retry() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(1), // first write fails
            written: StdMutex::new(Vec::new()),
        });
        let state = FragmentState::new(
            HashMap::new(),
            Some(store.clone() as Arc<dyn KvStore>),
            2, // flush every 2 buffered ops
            Duration::from_secs(3600),
        );

        let v1 = VersionedValue::new(b"v1".to_vec(), 100, "n1".to_string());
        let v2 = VersionedValue::new(b"v2".to_vec(), 101, "n1".to_string());
        state.apply(&op("k1", v1)).await.unwrap();
        // Second buffered op triggers the flush, which fails.
        let err = state.apply(&op("k2", v2)).await.unwrap_err();
        assert!(
            err.to_string().contains("injected failure"),
            "expected injected failure, got {err}"
        );
        // Nothing reached the store.
        assert!(store.written.lock().unwrap().is_empty());

        // Retry: the retained batch must flush successfully now.
        state.flush().await.unwrap();
        let written_count = store.written.lock().unwrap().len();
        assert_eq!(written_count, 2, "retained batch must be retried");
        {
            let written = store.written.lock().unwrap();
            assert!(written.iter().any(|(k, _)| k == b"k1"));
            assert!(written.iter().any(|(k, _)| k == b"k2"));
        }

        // And the buffer is drained — a further flush is a no-op.
        state.flush().await.unwrap();
        assert_eq!(store.written.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn flush_coalesces_each_key_to_its_lww_winner() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let state = FragmentState::new(
            HashMap::new(),
            Some(store.clone() as Arc<dyn KvStore>),
            100,
            Duration::from_secs(3600),
        );

        let old = VersionedValue::new(b"old".to_vec(), 100, "n1".to_string());
        let winner = VersionedValue::new(b"winner".to_vec(), 300, "n1".to_string());
        let stale = VersionedValue::new(b"stale".to_vec(), 200, "n1".to_string());
        assert_eq!(
            state.apply(&op("key", old)).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            state.apply(&op("key", winner.clone())).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert!(matches!(
            state.apply(&op("key", stale)).await.unwrap(),
            ApplyOutcome::Superseded { .. }
        ));

        state.flush().await.unwrap();
        let written = store.written.lock().unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0], (b"key".to_vec(), winner));
    }

    #[tokio::test]
    async fn apply_reports_exact_lww_outcome() {
        let state = FragmentState::new(HashMap::new(), None, 100, Duration::from_secs(3600));
        let winner = VersionedValue::new(b"winner".to_vec(), 300, "n1".to_string());
        assert_eq!(
            state.apply(&op("key", winner.clone())).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            state.apply(&op("key", winner.clone())).await.unwrap(),
            ApplyOutcome::AlreadyPresent
        );

        let stale = VersionedValue::new(b"stale".to_vec(), 200, "n1".to_string());
        assert_eq!(
            state.apply(&op("key", stale)).await.unwrap(),
            ApplyOutcome::Superseded {
                timestamp_ms: winner.timestamp_ms,
                node_id: winner.node_id,
                is_tombstone: winner.is_tombstone,
            }
        );

        let mut expired = VersionedValue::new(b"expired".to_vec(), 400, "n1".to_string());
        expired.expires_at_ms = Some(1);
        assert_eq!(
            state.apply(&op("expired", expired.clone())).await.unwrap(),
            ApplyOutcome::Expired
        );
        let marker = state.get_all().await.remove("expired").unwrap();
        assert!(marker.is_tombstone);
        assert_eq!(marker.timestamp_ms, expired.timestamp_ms);
        assert_eq!(marker.node_id, expired.node_id);
        assert_eq!(marker.expires_at_ms, expired.expires_at_ms);
    }

    #[tokio::test]
    async fn cleaner_persists_expiry_marker_and_preserves_version_floor() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let mut expired = VersionedValue::new(b"ttl".to_vec(), 200, "n1".to_string());
        expired.expires_at_ms = Some(300);
        let state = FragmentState::new(
            HashMap::from([("key".to_string(), expired.clone())]),
            Some(store.clone() as Arc<dyn KvStore>),
            1,
            Duration::from_secs(3600),
        );

        assert_eq!(state.expire_values(300).await.unwrap(), 1);
        let marker = expired.expiry_tombstone();
        assert_eq!(
            store.written.lock().unwrap().as_slice(),
            &[(b"key".to_vec(), marker.clone())]
        );
        assert_eq!(state.get_all().await.get("key"), Some(&marker));

        let older = VersionedValue::new(b"old".to_vec(), 199, "n9".to_string());
        assert!(matches!(
            state.apply(&op("key", older)).await.unwrap(),
            ApplyOutcome::Superseded { .. }
        ));
        let newer = VersionedValue::new(b"new".to_vec(), 201, "n1".to_string());
        assert_eq!(
            state.apply(&op("key", newer.clone())).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(state.get("key").await, Some(newer));
    }

    #[tokio::test]
    async fn cleaner_sweeps_retain_existing_and_new_tombstones() {
        let existing = VersionedValue::tombstone(100, "n1".to_string());
        let mut expired = VersionedValue::new(b"ttl".to_vec(), 200, "n1".to_string());
        expired.expires_at_ms = Some(300);
        let state = FragmentState::new(
            HashMap::from([
                ("deleted".to_string(), existing.clone()),
                ("expired".to_string(), expired.clone()),
            ]),
            None,
            100,
            Duration::from_secs(3600),
        );

        assert_eq!(state.expire_values(300).await.unwrap(), 1);
        assert_eq!(state.expire_values(u64::MAX).await.unwrap(), 0);
        assert_eq!(state.gc_tombstones(u64::MAX, 0).await, 0);

        let values = state.get_all().await;
        assert_eq!(values.get("deleted"), Some(&existing));
        assert_eq!(values.get("expired"), Some(&expired.expiry_tombstone()));
    }

    /// Store whose first write waits until the test releases it. A second
    /// write, if allowed to overlap, completes first and exposes durable
    /// version regression deterministically.
    struct GatedStore {
        calls: AtomicUsize,
        release_first: (StdMutex<bool>, Condvar),
        stored: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    }

    impl GatedStore {
        fn release_first(&self) {
            let (released, wake) = &self.release_first;
            *released.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    impl KvStore for GatedStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(self
                .stored
                .lock()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if call == 0 {
                let (released, wake) = &self.release_first;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            }

            let mut stored = self.stored.lock().unwrap();
            for (key, value) in writes {
                stored.insert(key.clone(), value.clone());
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_flushes_are_serialized_and_preserve_latest_version() {
        let store = Arc::new(GatedStore {
            calls: AtomicUsize::new(0),
            release_first: (StdMutex::new(false), Condvar::new()),
            stored: StdMutex::new(HashMap::new()),
        });
        let state = Arc::new(FragmentState::new(
            HashMap::new(),
            Some(store.clone() as Arc<dyn KvStore>),
            100,
            Duration::from_secs(3600),
        ));

        let old = VersionedValue::new(b"old".to_vec(), 100, "n1".to_string());
        let new = VersionedValue::new(b"new".to_vec(), 200, "n1".to_string());
        state.apply(&op("key", old)).await.unwrap();

        let first_state = state.clone();
        let first = tokio::spawn(async move { first_state.flush().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.calls.load(AtomicOrdering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first flush did not reach the store");

        // The newer version is applied while the old batch is blocked in the
        // store. A second flush must wait rather than commit ahead of it.
        state.apply(&op("key", new.clone())).await.unwrap();
        let second_state = state.clone();
        let second = tokio::spawn(async move { second_state.flush().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            store.calls.load(AtomicOrdering::SeqCst),
            1,
            "a second store write overlapped the first"
        );

        store.release_first();
        tokio::time::timeout(Duration::from_secs(2), async {
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
        })
        .await
        .expect("serialized flushes did not complete");

        assert_eq!(
            store.stored.lock().unwrap().get(b"key".as_slice()),
            Some(&new),
            "durable state regressed behind the in-memory LWW winner"
        );
    }
}
