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

use std::collections::{BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
#[cfg(test)]
use ractor::rpc::CallResult;
use ractor::{
    Actor, ActorCell, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort, SupervisionEvent,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

/// Priority for an operation. The dispatcher routes to separate worker
/// pools by priority, so bulk/background work cannot occupy the workers or
/// worker-queue capacity reserved for latency-sensitive ops. Dispatcher
/// ingress remains a shared serialization point.
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
    /// Max active-plus-queued jobs accepted into this priority's workers
    /// before shedding. This does not bound the shared dispatcher mailbox.
    /// Shed ops are dropped and counted (explicit, not silent).
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
use crate::store::{KvStore, StoreCommitDurability};
use crate::wire::check_versioned_value_budget;

/// A value stamped with its last-writer-wins version.
///
/// Versions are totally ordered by `(timestamp_ms, node_id)`: the higher
/// wall-clock timestamp wins, and the node id breaks ties deterministically
/// so every replica picks the same winner. `expires_at_ms` is an optional
/// wall-clock deadline (set via `put_with_ttl`); expired values are
/// invisible to reads and converted to versioned tombstones by the fragment's
/// cleaner.
///
/// `node_id` is record provenance and must contain 1 to 255 UTF-8 bytes,
/// matching the cluster identity bound. Actor apply and startup validation
/// reject values outside that range without mutating state.
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

/// Version metadata for one key in an anti-entropy snapshot.
///
/// Values are intentionally omitted so callers can compare snapshots before
/// fetching only the keys whose versions differ. An expired live value is
/// represented as its deterministic expiry tombstone at the snapshot's
/// pinned wall-clock time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSummary {
    pub key: String,
    pub timestamp_ms: u64,
    pub node_id: String,
    pub is_tombstone: bool,
}

/// One lexicographically ordered page of fragment version summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryPage {
    pub summaries: Vec<SnapshotSummary>,
    /// Fragment mutation revision captured under the cache read lock.
    pub revision: u64,
    /// Wall-clock instant used to decide which TTL values are expired.
    pub snapshot_time_ms: u64,
    /// True when no keys remain after this page.
    pub done: bool,
    /// True when `expected_revision` did not match. In that case summaries
    /// are empty and the caller must restart from the beginning.
    pub restart: bool,
}

/// One memory-bounded page of exact values requested by reconciliation.
/// `next_index` is the first unexamined position in the caller's key vector;
/// absent keys still advance it and are omitted from `entries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedPage {
    pub entries: Vec<(String, VersionedValue)>,
    pub next_index: usize,
    /// At least one examined key was absent from the cache.
    pub missing: bool,
}

// Keep actor-owned summary clones bounded before the network layer adds
// protobuf metadata. A single individually valid summary may exceed the soft
// target, but admission control guarantees it still fits the hard wire limit.
const SUMMARY_PAGE_TARGET_BYTES: usize = 1024 * 1024;
const VERSIONED_PAGE_TARGET_BYTES: usize = 1024 * 1024;

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
    /// The configured [`KvStore`] completed its explicit stable-storage fence.
    /// The exact guarantee is backend-defined, but is stronger than merely
    /// accepting the write batch into a volatile or buffered layer.
    StableCommitted,
}

/// Failure before a fragment can produce an [`ApplyOutcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// Admission control rejected a client-originated mutation.
    Backpressure,
    /// The operation cannot be represented safely by the peer protocol.
    InvalidInput(String),
    /// Persistence failed while applying a batch-triggering mutation.
    Storage(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backpressure => write!(f, "fragment worker pool is at capacity"),
            Self::InvalidInput(error) => write!(f, "invalid mutation: {error}"),
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
    /// (`ReplicateBatch`) or anti-entropy (`ReconcileData`). These bypass
    /// dispatcher admission control: the sender
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
    /// Flush every persistence update ordered before this message, then ask the
    /// configured store for its explicit stable-storage fence. The existing
    /// [`KvMessage::Flush`] retains its buffered-store semantics.
    FlushStable {
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
    /// Read the full local copy for compatibility and diagnostics. The wire
    /// reconciler uses bounded summary pages and exact-key reads instead.
    GetAll {
        reply: RpcReplyPort<HashMap<String, VersionedValue>>,
    },
    /// Read a reconciliation snapshot and the fragment-wide mutation
    /// revision captured under the same cache read lock.
    GetSnapshot {
        reply: RpcReplyPort<(HashMap<String, VersionedValue>, u64)>,
    },
    /// Read at most `limit.max(1)` summaries in lexicographic key order,
    /// subject to an internal byte target. `after_key` is exclusive. A
    /// paginating caller passes the revision and pinned time returned by its
    /// first page on every subsequent request.
    GetSummaryPage {
        after_key: Option<String>,
        expected_revision: Option<u64>,
        snapshot_time_ms: Option<u64>,
        limit: usize,
        reply: RpcReplyPort<SummaryPage>,
    },
    /// Read one exact value, including tombstones. An expired live value is
    /// synthesized as a deterministic tombstone at `snapshot_time_ms`, or at
    /// the current wall-clock time when omitted.
    GetVersioned {
        key: String,
        snapshot_time_ms: Option<u64>,
        reply: RpcReplyPort<Option<VersionedValue>>,
    },
    /// Read a bounded page from one exact-key reconciliation request while
    /// taking the shared cache read lock only once per page. `keys` is shared
    /// across calls; `start_index` is the first position to examine.
    GetVersionedPage {
        keys: Arc<Vec<String>>,
        start_index: usize,
        snapshot_time_ms: Option<u64>,
        reply: RpcReplyPort<VersionedPage>,
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
/// can lose the unflushed tail of the write buffer; immediate correlated
/// reconciliation on reconnect repairs it from peers.
pub struct FragmentState {
    /// In-memory cache of the fragment: the fast path for all reads.
    cache: RwLock<FragmentCache>,
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

/// Values and their ordered key index. Both live under one lock so every
/// visible key is immediately available to ordered snapshot pagination.
struct FragmentCache {
    values: HashMap<String, VersionedValue>,
    ordered_keys: BTreeSet<String>,
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
        let ordered_keys = cache.keys().cloned().collect();
        Self {
            cache: RwLock::new(FragmentCache {
                values: cache,
                ordered_keys,
            }),
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
    pub async fn apply(&self, op: &ReplicatedOp) -> Result<ApplyOutcome, ApplyError> {
        check_versioned_value_budget(&op.key, &op.value).map_err(|error| {
            ApplyError::InvalidInput(format!(
                "key/value cannot fit every required peer frame: {error}"
            ))
        })?;
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
            self.flush()
                .await
                .map_err(|error| ApplyError::Storage(error.to_string()))?;
        }
        Ok(Self::reported_outcome(outcome, expired))
    }

    fn merge_cache(cache: &mut FragmentCache, key: &str, value: &VersionedValue) -> ApplyOutcome {
        if let Some(existing) = cache.values.get_mut(key) {
            if existing == value {
                return ApplyOutcome::AlreadyPresent;
            }
            if !value.wins_over(existing) {
                return ApplyOutcome::Superseded {
                    timestamp_ms: existing.timestamp_ms,
                    node_id: existing.node_id.clone(),
                    is_tombstone: existing.is_tombstone,
                };
            }
            *existing = value.clone();
            ApplyOutcome::Applied
        } else {
            let key = key.to_string();
            cache.ordered_keys.insert(key.clone());
            cache.values.insert(key, value.clone());
            ApplyOutcome::Applied
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

    /// Flush the fragment's ordered persistence buffer, then ask the backend
    /// for its strongest explicit stable-storage fence.
    ///
    /// Storeless fragments remain memory-only. A backend which reports a
    /// buffered fence preserves the ordinary [`ApplyDurability::StoreCommitted`]
    /// result; only an explicit stable fence yields
    /// [`ApplyDurability::StableCommitted`].
    pub async fn flush_stable(&self) -> Result<ApplyDurability, ActorProcessingErr> {
        self.flush().await?;
        let Some(kvstore) = &self.kvstore else {
            return Ok(ApplyDurability::MemoryApplied);
        };
        kvstore
            .stable_fence()
            .map(|durability| match durability {
                StoreCommitDurability::Buffered => ApplyDurability::StoreCommitted,
                StoreCommitDurability::Stable => ApplyDurability::StableCommitted,
            })
            .map_err(|error| ActorProcessingErr::from(error.to_string()))
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
            .values
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
            .values
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

    /// Read one lexicographically ordered page of version summaries.
    ///
    /// A caller starts with no expected revision or pinned time, then feeds
    /// both returned values into later pages. If a winning mutation occurred,
    /// the revision mismatch returns an empty restart page instead of mixing
    /// two logical snapshots.
    pub async fn summary_page(
        &self,
        after_key: Option<&str>,
        expected_revision: Option<u64>,
        snapshot_time_ms: Option<u64>,
        limit: usize,
    ) -> SummaryPage {
        use std::ops::Bound::{Excluded, Unbounded};

        let cache = self.cache.read().await;
        let revision = self.revision.load(Ordering::Acquire);
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        if expected_revision.is_some_and(|expected| expected != revision) {
            return SummaryPage {
                summaries: Vec::new(),
                revision,
                snapshot_time_ms,
                done: false,
                restart: true,
            };
        }

        let lower_bound = after_key
            .map(|key| Excluded(key.to_string()))
            .unwrap_or(Unbounded);
        let limit = limit.max(1);
        let keys = cache.ordered_keys.range((lower_bound, Unbounded));
        // Do not reserve from the fragment's total cardinality: callers may
        // use a very large count limit while the byte target still ends the
        // page after only a handful of metadata-heavy records.
        let mut summaries = Vec::with_capacity(limit.min(256));
        let mut payload_bytes = 0usize;
        let mut done = true;
        for key in keys {
            if summaries.len() == limit {
                done = false;
                break;
            }
            if let Some(value) = cache.values.get(key) {
                let summary_bytes = key.len().saturating_add(value.node_id.len());
                if !summaries.is_empty()
                    && payload_bytes.saturating_add(summary_bytes) > SUMMARY_PAGE_TARGET_BYTES
                {
                    done = false;
                    break;
                }
                summaries.push(Self::snapshot_summary(key, value, snapshot_time_ms));
                payload_bytes = payload_bytes.saturating_add(summary_bytes);
            }
        }

        SummaryPage {
            summaries,
            revision,
            snapshot_time_ms,
            done,
            restart: false,
        }
    }

    /// Read one value including tombstones at a pinned expiry time.
    pub async fn get_versioned(
        &self,
        key: &str,
        snapshot_time_ms: Option<u64>,
    ) -> Option<VersionedValue> {
        let cache = self.cache.read().await;
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        cache.values.get(key).map(|value| {
            if value.is_expired(snapshot_time_ms) {
                value.expiry_tombstone()
            } else {
                value.clone()
            }
        })
    }

    /// Read exact values in input order without cloning an arbitrary fetch
    /// chunk at once. Pages target 1 MiB of key/value metadata; one individually
    /// valid value may exceed that soft target but is returned alone.
    pub async fn get_versioned_page(
        &self,
        keys: &[String],
        start_index: usize,
        snapshot_time_ms: Option<u64>,
    ) -> VersionedPage {
        let cache = self.cache.read().await;
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        let mut next_index = start_index.min(keys.len());
        let mut payload_bytes = 0usize;
        let mut entries = Vec::new();
        let mut missing = false;
        while let Some(key) = keys.get(next_index) {
            let Some(value) = cache.values.get(key) else {
                missing = true;
                next_index += 1;
                continue;
            };
            let value = if value.is_expired(snapshot_time_ms) {
                value.expiry_tombstone()
            } else {
                value.clone()
            };
            let entry_bytes = key
                .len()
                .saturating_add(value.value.len())
                .saturating_add(value.node_id.len());
            if !entries.is_empty()
                && payload_bytes.saturating_add(entry_bytes) > VERSIONED_PAGE_TARGET_BYTES
            {
                break;
            }
            entries.push((key.clone(), value));
            payload_bytes = payload_bytes.saturating_add(entry_bytes);
            next_index += 1;
            if payload_bytes > VERSIONED_PAGE_TARGET_BYTES {
                break;
            }
        }
        VersionedPage {
            entries,
            next_index,
            missing,
        }
    }

    fn snapshot_summary(
        key: &str,
        value: &VersionedValue,
        snapshot_time_ms: u64,
    ) -> SnapshotSummary {
        SnapshotSummary {
            key: key.to_string(),
            timestamp_ms: value.timestamp_ms,
            node_id: value.node_id.clone(),
            is_tombstone: value.is_tombstone || value.is_expired(snapshot_time_ms),
        }
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
            for value in cache.values.values_mut() {
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
            for (key, value) in cache.values.iter_mut() {
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
        let result = handle_worker_message(&state.0, message).await;
        // Release the optional caller-owned pending slot. Production pools
        // use `PoolWorker`, below, which also tracks the exact worker slot so
        // supervision can reclaim queued work discarded by a failed worker.
        if let Some(pending) = &state.1 {
            release_pending(pending, 1);
        }
        result
    }
}

async fn handle_worker_message(
    shared: &Arc<FragmentState>,
    message: KvMessage,
) -> Result<(), ActorProcessingErr> {
    match message {
        KvMessage::ApplyOp {
            op,
            priority: _,
            is_replication: _,
            reply,
        } => {
            // Cache merge and durable enqueue are one ordered operation.
            // A DB error fails the actor loudly rather than diverging
            // silently.
            match shared.apply(&op).await {
                Ok(outcome) => {
                    if let Some(reply) = reply {
                        let _ = reply.send(Ok(outcome));
                    }
                    Ok(())
                }
                Err(error) => {
                    let fatal = match &error {
                        ApplyError::Storage(message) => {
                            Some(ActorProcessingErr::from(message.clone()))
                        }
                        ApplyError::Backpressure | ApplyError::InvalidInput(_) => None,
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(error));
                    }
                    match fatal {
                        Some(error) => Err(error),
                        None => Ok(()),
                    }
                }
            }
        }
        KvMessage::Get {
            key,
            reply,
            priority: _,
        } => {
            // The receiver may be gone (e.g. timed-out caller); that's fine.
            let _ = reply.send(Ok(shared.get(&key).await));
            Ok(())
        }
        // Flush fences are handled by the dispatcher against the shared
        // state. They are never routed through a worker pool.
        KvMessage::Flush { reply } | KvMessage::FlushStable { reply } => {
            let _ = reply.send(Err(ApplyError::Storage(
                "flush fence was routed to a worker".to_string(),
            )));
            Ok(())
        }
        // Snapshot queries are served by the dispatcher straight from
        // shared state.
        KvMessage::GetAll { .. }
        | KvMessage::GetSnapshot { .. }
        | KvMessage::GetSummaryPage { .. }
        | KvMessage::GetVersioned { .. }
        | KvMessage::GetVersionedPage { .. }
        | KvMessage::GetRevision { .. } => Ok(()),
        // Shed counts are a dispatcher-level query; workers don't track them.
        KvMessage::GetShedCounts { reply } => {
            let _ = reply.send((0, 0, 0));
            Ok(())
        }
    }
}

/// Worker used internally by [`WorkerPool`]. The pool-wide count enforces the
/// configured cap, while the slot-local count lets supervision reclaim only
/// the work abandoned by one failed worker.
struct PoolWorker;

struct PoolWorkerArgs {
    shared: Arc<FragmentState>,
    pool_pending: Arc<AtomicU64>,
    slot_pending: Arc<AtomicU64>,
    #[cfg(test)]
    startup_gate: Option<WorkerTestGate>,
}

#[cfg(test)]
#[derive(Clone, Default)]
struct WorkerTestGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static WORKER_STARTUP_GATES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<String, WorkerTestGate>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(test)]
static WORKER_GET_GATES: std::sync::LazyLock<std::sync::Mutex<HashMap<String, WorkerTestGate>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(test)]
fn install_worker_startup_gate(name: String) -> WorkerTestGate {
    let gate = WorkerTestGate::default();
    let replaced = WORKER_STARTUP_GATES
        .lock()
        .expect("worker startup gate registry poisoned")
        .insert(name, gate.clone());
    assert!(replaced.is_none(), "worker startup gate already installed");
    gate
}

#[cfg(test)]
fn take_worker_startup_gate(name: &str) -> Option<WorkerTestGate> {
    WORKER_STARTUP_GATES
        .lock()
        .expect("worker startup gate registry poisoned")
        .remove(name)
}

#[cfg(test)]
fn install_worker_get_gate(key: String) -> WorkerTestGate {
    let gate = WorkerTestGate::default();
    let replaced = WORKER_GET_GATES
        .lock()
        .expect("worker Get gate registry poisoned")
        .insert(key, gate.clone());
    assert!(replaced.is_none(), "worker Get gate already installed");
    gate
}

#[cfg(test)]
fn take_worker_get_gate(key: &str) -> Option<WorkerTestGate> {
    WORKER_GET_GATES
        .lock()
        .expect("worker Get gate registry poisoned")
        .remove(key)
}

impl Actor for PoolWorker {
    type Msg = KvMessage;
    type State = PoolWorkerArgs;
    type Arguments = PoolWorkerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        #[cfg(test)]
        if let Some(gate) = &args.startup_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(args)
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
                priority: _,
                is_replication: _,
                reply,
            } => {
                let result = state.shared.apply(&op).await;
                release_pool_reservation(state);
                match result {
                    Ok(outcome) => {
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(outcome));
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let fatal = match &error {
                            ApplyError::Storage(message) => {
                                Some(ActorProcessingErr::from(message.clone()))
                            }
                            ApplyError::Backpressure | ApplyError::InvalidInput(_) => None,
                        };
                        if let Some(reply) = reply {
                            let _ = reply.send(Err(error));
                        }
                        match fatal {
                            Some(error) => Err(error),
                            None => Ok(()),
                        }
                    }
                }
            }
            KvMessage::Get {
                key,
                reply,
                priority: _,
            } => {
                #[cfg(test)]
                if let Some(gate) = take_worker_get_gate(&key) {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
                let value = state.shared.get(&key).await;
                release_pool_reservation(state);
                let _ = reply.send(Ok(value));
                Ok(())
            }
            message => {
                // Dispatcher-only variants are never routed here. Keep the
                // public KvWorker's defensive behavior if an internal bug
                // does so, while still balancing the reservation.
                let result = handle_worker_message(&state.shared, message).await;
                release_pool_reservation(state);
                result
            }
        }
    }
}

fn release_pool_reservation(state: &PoolWorkerArgs) {
    // The order is deliberately the inverse of admission (slot, then pool),
    // preserving the conservative invariant that the pool total never falls
    // below the sum of its live slot counters. Nothing may await between the
    // two updates and the result reply is sent only after both complete.
    release_pending(&state.slot_pending, 1);
    release_pending(&state.pool_pending, 1);
}

fn release_pending(pending: &AtomicU64, count: u64) -> bool {
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

struct WorkerPool {
    workers: Vec<WorkerSlot>,
    config: PoolConfig,
    /// Jobs currently active or queued across this pool's workers.
    pending: Arc<AtomicU64>,
    /// Ops shed due to queue cap or rate limit (explicit, counted).
    shed_count: Arc<AtomicU64>,
    rate_limiter: Option<Mutex<TokenBucket>>,
}

impl WorkerPool {
    /// Spawn workers sharing `shared` state, linked to `supervisor`
    /// so a dead worker raises a supervision event there. Workers are
    /// named `<prefix>-worker-<slot>` for registry lookup.
    async fn spawn_linked(
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
    fn slot_of(&self, cell: &ActorCell) -> Option<usize> {
        self.workers
            .iter()
            .position(|slot| slot.worker.get_id() == cell.get_id())
    }

    /// Replace the worker at `slot` with `worker` (supervised restart).
    fn replace(&mut self, slot: usize, worker: ActorRef<KvMessage>, pending: Arc<AtomicU64>) {
        if let Some(worker_slot) = self.workers.get_mut(slot) {
            *worker_slot = WorkerSlot { worker, pending };
        }
    }

    /// Try to reserve the exact sticky worker for an op. The reservation rolls
    /// both counters back unless it is successfully handed to that worker.
    async fn try_reserve(&self, key: &str) -> Option<WorkerReservation<'_>> {
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
    fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }

    /// Reserve an op without cap/rate checks. Replication and reconciliation
    /// cannot be discarded after their sender has accepted responsibility,
    /// but they still participate in exact lifecycle accounting.
    fn reserve_unchecked(&self, key: &str) -> WorkerReservation<'_> {
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
    fn reclaim_slot(&self, slot: usize) -> u64 {
        let abandoned = self.workers[slot].pending.swap(0, Ordering::AcqRel);
        release_pending(&self.pending, abandoned);
        abandoned
    }

    /// Close every worker mailbox, drain the work accepted before that fence,
    /// and wait for every worker to exit. Closing all mailboxes before waiting
    /// prevents one slow shard from leaving its siblings open to new work.
    fn begin_shutdown(&self) {
        for slot in &self.workers {
            if slot.worker.get_status() < ActorStatus::Draining {
                let _ = slot.worker.drain();
            }
        }
    }

    async fn join(&self) {
        for slot in &self.workers {
            let _ = slot.worker.wait(None).await;
        }
    }

    async fn shutdown(&self) {
        self.begin_shutdown();
        self.join().await;
    }
}

struct WorkerReservation<'a> {
    pool: &'a WorkerPool,
    slot: usize,
    handed_off: bool,
}

impl WorkerReservation<'_> {
    /// Hand the reservation and message to the selected worker. An immediate
    /// send failure drops the reply port and rolls the reservation back, so
    /// callers observe actor unavailability rather than false backpressure.
    fn send(mut self, message: KvMessage) {
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

/// The per-fragment dispatcher. One runs per fragment this node serves:
/// it owns the [`FragmentState`] and an internal managed worker pool, and
/// routes every operation to a worker by sticky key hash. `GetAll` is served
/// directly from shared state.
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
        let mut pools = Vec::with_capacity(3);
        let startup = AssertUnwindSafe(async {
            pools.push(
                WorkerPool::spawn_linked(
                    args.high_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-high", args.worker_prefix),
                )
                .await?,
            );
            pools.push(
                WorkerPool::spawn_linked(
                    args.default_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-default", args.worker_prefix),
                )
                .await?,
            );
            pools.push(
                WorkerPool::spawn_linked(
                    args.background_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-bg", args.worker_prefix),
                )
                .await?,
            );
            Ok::<(), ActorProcessingErr>(())
        })
        .catch_unwind()
        .await;
        match startup {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                for pool in &pools {
                    pool.begin_shutdown();
                }
                for pool in &pools {
                    pool.join().await;
                }
                return Err(error);
            }
            Err(panic) => {
                for pool in &pools {
                    pool.begin_shutdown();
                }
                for pool in &pools {
                    pool.join().await;
                }
                std::panic::resume_unwind(panic);
            }
        }
        let mut pools = pools.into_iter();
        let high_pool = pools.next().expect("high pool was started");
        let default_pool = pools.next().expect("default pool was started");
        let background_pool = pools.next().expect("background pool was started");
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
        // Standalone KvActor users still receive the actor's historical
        // flush-on-stop contract. A supervising fragment keeps another Arc to
        // the shared state and performs an observable second fence, so a
        // failed attempt here retains its batch for the supervisor to retry.
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
                // Bypassed ops still reserve their exact sticky worker so
                // completion, immediate send failure, or worker death can
                // release the same accounting exactly once.
                let reservation = if is_replication {
                    Some(pool.reserve_unchecked(&op.key))
                } else {
                    pool.try_reserve(&op.key).await
                };
                let Some(reservation) = reservation else {
                    // Shed: counted in pool.shed_count and returned to callers
                    // that requested an exact apply outcome.
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(ApplyError::Backpressure));
                    }
                    return Ok(());
                };
                reservation.send(KvMessage::ApplyOp {
                    op,
                    priority,
                    is_replication,
                    reply,
                });
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
                let Some(reservation) = pool.try_reserve(&key).await else {
                    // Shed: explicit error, not "not found". The caller
                    // sees NodeError::Backpressure, distinct from Ok(None).
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".to_string()));
                    return Ok(());
                };
                // Hand the caller's reply port directly to the worker. The
                // dispatcher remains free to serve other pools, health, and
                // supervision while this read waits behind sticky-key work.
                reservation.send(KvMessage::Get {
                    key,
                    reply,
                    priority,
                });
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
            KvMessage::FlushStable { reply } => {
                let result = state
                    .shared
                    .flush_stable()
                    .await
                    .map_err(|error| ApplyError::Storage(error.to_string()));
                let _ = reply.send(result);
                // As with the ordinary flush fence, keep the stable dispatcher
                // reference alive so a backend failure can be retried.
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
            KvMessage::GetSummaryPage {
                after_key,
                expected_revision,
                snapshot_time_ms,
                limit,
                reply,
            } => {
                let page = state
                    .shared
                    .summary_page(
                        after_key.as_deref(),
                        expected_revision,
                        snapshot_time_ms,
                        limit,
                    )
                    .await;
                let _ = reply.send(page);
                Ok(())
            }
            KvMessage::GetVersioned {
                key,
                snapshot_time_ms,
                reply,
            } => {
                let _ = reply.send(state.shared.get_versioned(&key, snapshot_time_ms).await);
                Ok(())
            }
            KvMessage::GetVersionedPage {
                keys,
                start_index,
                snapshot_time_ms,
                reply,
            } => {
                let _ = reply.send(
                    state
                        .shared
                        .get_versioned_page(&keys, start_index, snapshot_time_ms)
                        .await,
                );
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
                pool.reclaim_slot(slot);
                // Planned worker drains emit the same termination events as
                // failures. Retire any remaining accounting above, but do not
                // link a replacement to a draining parent.
                if myself.get_status() >= ActorStatus::Draining {
                    return Ok(());
                }
                let name = format!("{}-{suffix}-worker-{slot}", state.worker_prefix);
                let slot_pending = Arc::new(AtomicU64::new(0));
                #[cfg(test)]
                let startup_gate = take_worker_startup_gate(&name);
                let replacement = PoolWorker::spawn_linked(
                    Some(name),
                    PoolWorker,
                    PoolWorkerArgs {
                        shared: state.shared.clone(),
                        pool_pending: pool.pending.clone(),
                        slot_pending: slot_pending.clone(),
                        #[cfg(test)]
                        startup_gate,
                    },
                    myself.get_cell(),
                )
                .await;
                match replacement {
                    Ok((worker, _handle)) => pool.replace(slot, worker, slot_pending),
                    // `drain()` changes the parent status immediately. It can
                    // race the await above, causing ractor to reject the new
                    // link after the earlier status check. That is an orderly
                    // retirement, not a dispatcher failure: returning an
                    // error would skip `post_stop` and kill sibling workers
                    // before their accepted mailboxes drain.
                    Err(_) if myself.get_status() >= ActorStatus::Draining => return Ok(()),
                    Err(error) => return Err(ActorProcessingErr::from(error.to_string())),
                }
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
    use crate::store::StoreError;

    struct StableFenceStore {
        reported: StoreCommitDurability,
        events: std::sync::Mutex<Vec<&'static str>>,
    }

    impl StableFenceStore {
        fn new(reported: StoreCommitDurability) -> Self {
            Self {
                reported,
                events: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl KvStore for StableFenceStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.events.lock().unwrap().push("write");
            Ok(())
        }

        fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
            self.events.lock().unwrap().push("fence");
            Ok(self.reported)
        }
    }

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

    fn test_state(cache: HashMap<String, VersionedValue>) -> FragmentState {
        FragmentState::new(cache, None, 100, Duration::from_secs(3600))
    }

    fn test_op(key: &str, value: VersionedValue) -> ReplicatedOp {
        ReplicatedOp {
            key: key.to_string(),
            value,
            seq: 0,
            boot_id: 0,
        }
    }

    #[tokio::test]
    async fn stable_flush_maps_store_guarantees_after_flushing_pending_writes() {
        for (reported, expected) in [
            (
                StoreCommitDurability::Buffered,
                ApplyDurability::StoreCommitted,
            ),
            (
                StoreCommitDurability::Stable,
                ApplyDurability::StableCommitted,
            ),
        ] {
            let store = Arc::new(StableFenceStore::new(reported));
            let state = FragmentState::new(
                HashMap::new(),
                Some(store.clone()),
                100,
                Duration::from_secs(3600),
            );
            state
                .apply(&test_op(
                    "stable",
                    VersionedValue::new(b"value".to_vec(), 1, "node".to_string()),
                ))
                .await
                .unwrap();

            assert_eq!(state.flush_stable().await.unwrap(), expected);
            assert_eq!(*store.events.lock().unwrap(), vec!["write", "fence"]);
        }

        let storeless = test_state(HashMap::new());
        assert_eq!(
            storeless.flush_stable().await.unwrap(),
            ApplyDurability::MemoryApplied
        );
    }

    #[tokio::test]
    async fn summary_pages_are_ordered_exclusive_and_revision_guarded() {
        let state = test_state(HashMap::from([
            (
                "zulu".to_string(),
                VersionedValue::new(vec![3], 30, "n1".to_string()),
            ),
            (
                "alpha".to_string(),
                VersionedValue::new(vec![1], 10, "n1".to_string()),
            ),
            (
                "charlie".to_string(),
                VersionedValue::new(vec![2], 20, "n1".to_string()),
            ),
        ]));

        let first = state.summary_page(None, None, Some(123), 2).await;
        assert_eq!(first.revision, 0);
        assert_eq!(first.snapshot_time_ms, 123);
        assert!(!first.done);
        assert!(!first.restart);
        assert_eq!(
            first
                .summaries
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "charlie"]
        );

        let second = state
            .summary_page(
                Some("charlie"),
                Some(first.revision),
                Some(first.snapshot_time_ms),
                2,
            )
            .await;
        assert!(second.done);
        assert!(!second.restart);
        assert_eq!(
            second
                .summaries
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["zulu"]
        );

        state
            .apply(&test_op(
                "bravo",
                VersionedValue::new(vec![4], 40, "n1".to_string()),
            ))
            .await
            .unwrap();
        let stale = state
            .summary_page(
                Some("charlie"),
                Some(first.revision),
                Some(first.snapshot_time_ms),
                2,
            )
            .await;
        assert!(stale.restart);
        assert!(!stale.done);
        assert!(stale.summaries.is_empty());
        assert_eq!(stale.revision, 1);

        let restarted = state.summary_page(None, None, Some(123), 10).await;
        assert_eq!(
            restarted
                .summaries
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "bravo", "charlie", "zulu"]
        );
        assert!(restarted.done);
    }

    #[tokio::test]
    async fn summary_cursor_can_advance_past_an_empty_key() {
        let state = test_state(HashMap::from([
            (
                String::new(),
                VersionedValue::new(vec![0], 1, "n1".to_string()),
            ),
            (
                "next".to_string(),
                VersionedValue::new(vec![1], 2, "n1".to_string()),
            ),
        ]));

        let first = state.summary_page(None, None, Some(123), 1).await;
        assert_eq!(first.summaries[0].key, "");
        assert!(!first.done);
        let second = state
            .summary_page(
                Some(""),
                Some(first.revision),
                Some(first.snapshot_time_ms),
                1,
            )
            .await;
        assert_eq!(second.summaries[0].key, "next");
        assert!(second.done);
    }

    #[tokio::test]
    async fn summary_pages_clamp_zero_limit_and_bound_cloned_metadata() {
        let large_suffix = "k".repeat(SUMMARY_PAGE_TARGET_BYTES / 2);
        let alpha = format!("alpha-{large_suffix}");
        let bravo = format!("bravo-{large_suffix}");
        let state = test_state(HashMap::from([
            (
                alpha.clone(),
                VersionedValue::new(Vec::new(), 1, "n".to_string()),
            ),
            (
                bravo.clone(),
                VersionedValue::new(Vec::new(), 2, "n".to_string()),
            ),
        ]));

        let first = state.summary_page(None, None, Some(123), 0).await;
        assert_eq!(first.summaries.len(), 1);
        assert_eq!(first.summaries[0].key, alpha);
        assert!(!first.done);

        let second = state
            .summary_page(
                Some(&alpha),
                Some(first.revision),
                Some(first.snapshot_time_ms),
                usize::MAX,
            )
            .await;
        assert_eq!(second.summaries.len(), 1);
        assert_eq!(second.summaries[0].key, bravo);
        assert!(second.done);
    }

    #[tokio::test]
    async fn summary_and_get_versioned_share_pinned_expiry_semantics() {
        let mut expiring = VersionedValue::new(b"value".to_vec(), 50, "n1".to_string());
        expiring.expires_at_ms = Some(100);
        let tombstone = VersionedValue::tombstone(60, "n2".to_string());
        let state = test_state(HashMap::from([
            ("expiring".to_string(), expiring.clone()),
            ("deleted".to_string(), tombstone.clone()),
        ]));

        let before = state.summary_page(None, None, Some(99), 10).await;
        assert!(
            !before
                .summaries
                .iter()
                .find(|summary| summary.key == "expiring")
                .unwrap()
                .is_tombstone
        );
        let at_expiry = state.summary_page(None, None, Some(100), 10).await;
        assert!(
            at_expiry
                .summaries
                .iter()
                .find(|summary| summary.key == "expiring")
                .unwrap()
                .is_tombstone
        );
        assert!(
            at_expiry
                .summaries
                .iter()
                .find(|summary| summary.key == "deleted")
                .unwrap()
                .is_tombstone
        );

        assert_eq!(
            state.get_versioned("expiring", Some(99)).await,
            Some(expiring.clone())
        );
        assert_eq!(
            state.get_versioned("deleted", Some(99)).await,
            Some(tombstone)
        );
        assert_eq!(state.get_versioned("missing", Some(99)).await, None);
        assert_eq!(
            state.get_versioned("expiring", Some(100)).await,
            Some(expiring.expiry_tombstone())
        );
    }

    #[tokio::test]
    async fn versioned_pages_bound_value_clones_and_advance_over_missing_keys() {
        let state = test_state(HashMap::from([
            (
                "a".to_string(),
                VersionedValue::new(vec![1; 600 * 1024], 1, "n".to_string()),
            ),
            (
                "b".to_string(),
                VersionedValue::new(vec![2; 600 * 1024], 2, "n".to_string()),
            ),
        ]));
        let keys = vec!["missing".to_string(), "a".to_string(), "b".to_string()];

        let first = state.get_versioned_page(&keys, 0, Some(u64::MAX)).await;
        assert!(first.missing);
        assert_eq!(first.next_index, 2);
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].0, "a");

        let second = state
            .get_versioned_page(&keys, first.next_index, Some(u64::MAX))
            .await;
        assert!(!second.missing);
        assert_eq!(second.next_index, keys.len());
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].0, "b");
    }

    #[tokio::test]
    async fn invalid_wire_record_apply_is_nonfatal_to_worker() {
        let shared = Arc::new(test_state(HashMap::new()));
        let (worker, handle) = KvWorker::spawn(
            None,
            KvWorker,
            KvWorkerArgs {
                shared: shared.clone(),
                pending: None,
            },
        )
        .await
        .unwrap();

        let oversized_key = "k".repeat(crate::wire::MAX_FRAME_BYTES / 2);
        let invalid = worker
            .call(
                |reply| KvMessage::ApplyOp {
                    op: test_op(
                        &oversized_key,
                        VersionedValue::new(Vec::new(), 1, "node".to_string()),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            invalid,
            CallResult::Success(Err(ApplyError::InvalidInput(_)))
        ));

        for node_id in [
            String::new(),
            "n".repeat(crate::wire::MAX_WIRE_NODE_ID_BYTES + 1),
        ] {
            let invalid = worker
                .call(
                    move |reply| KvMessage::ApplyOp {
                        op: test_op(
                            "invalid-provenance",
                            VersionedValue::new(Vec::new(), 2, node_id),
                        ),
                        priority: OpPriority::Default,
                        is_replication: false,
                        reply: Some(reply),
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(matches!(
                invalid,
                CallResult::Success(Err(ApplyError::InvalidInput(_)))
            ));
            assert!(shared.get("invalid-provenance").await.is_none());
        }

        let valid = worker
            .call(
                |reply| KvMessage::ApplyOp {
                    op: test_op(
                        "healthy",
                        VersionedValue::new(b"value".to_vec(), 3, "node".to_string()),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(valid, CallResult::Success(Ok(ApplyOutcome::Applied)));
        assert_eq!(
            shared.get("healthy").await.unwrap().value,
            b"value".to_vec()
        );

        worker.stop(None);
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod worker_pool_tests {
    use super::*;
    use ractor::registry::where_is;

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
        let cache_guard = shared.cache.write().await;
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
        let cache_guard = shared.cache.write().await;
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
        let cache_guard = shared.cache.write().await;
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
    async fn standalone_dispatcher_drain_flushes_buffered_writes() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            Some(store.clone() as Arc<dyn KvStore>),
            usize::MAX,
            Duration::from_secs(3600),
        ));
        let pool = PoolConfig {
            workers: 1,
            queue_cap: 16,
            rate_limit_per_sec: None,
        };
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: "standalone-drain-flush".to_string(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let value = VersionedValue::new(b"value".to_vec(), 100, "node".to_string());
        let result = dispatcher
            .call(
                |reply| KvMessage::ApplyOp {
                    op: op("key", value.clone()),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(result, CallResult::Success(Ok(ApplyOutcome::Applied)));
        assert!(store.written.lock().unwrap().is_empty());

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
        assert_eq!(
            store.written.lock().unwrap().as_slice(),
            &[(b"key".to_vec(), value)]
        );
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
