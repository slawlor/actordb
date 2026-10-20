// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Operation values, public messages, and retained batch-read protocol.

use std::collections::HashMap;
use std::sync::Arc;

use ractor::{ActorRef, RpcReplyPort};

use super::scan::{ScanMatch, ScanSpec};
use crate::wire::{batch_get_result_encoded_len, RepeatedFieldSizer};
use actordb_protocol::RecordVersion;
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

/// A value stamped with its last-writer-wins version.
///
/// Versions are totally ordered by `(timestamp_ms, logical_counter, node_id)`: the higher
/// physical/logical timestamp wins, and the node id breaks ties deterministically
/// so every replica picks the same winner. `expires_at_ms` is an optional
/// wall-clock deadline (set via `put_with_ttl`); expired values are
/// invisible to reads and converted to versioned tombstones by the fragment's
/// cleaner.
///
/// `node_id` is record provenance and must contain 1 to 255 UTF-8 bytes,
/// matching the cluster identity bound. Actor apply and startup validation
/// reject values outside that range without mutating state.
///
/// For each key, one `(timestamp_ms, logical_counter, node_id, is_tombstone)`
/// tuple must identify exactly one payload and expiry deadline. Custom writers
/// and restore tools must never reuse it for different contents. Node-generated
/// versions satisfy this when node identities are unique and persisted clock
/// floors are retained across restarts. Fragment apply rejects an equal tuple
/// with different contents as [`ApplyError::VersionConflict`]; anti-entropy
/// detects such divergence and refuses to certify convergence.
///
/// A tombstone (`is_tombstone`) marks a deleted key. Tombstones participate
/// in LWW like writes: a tombstone with a higher timestamp shadows a live
/// value, and a live write with a higher timestamp resurrects the key.
/// On exact `(timestamp, logical_counter, node_id)` ties the tombstone wins (deletes are
/// explicit). Tombstones are shipped by sync and retained until a future
/// causally safe garbage collector can prove that no replica needs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub timestamp_ms: u64,
    /// Logical component; independent of physical clock skew and TTL deadlines.
    pub logical_counter: u64,
    pub node_id: String,
    /// Wall-clock expiry in ms since epoch. `None` = no expiry. Zero is reserved
    /// for absent expiry in the wire and durable codecs; `Some(0)` is rejected.
    pub expires_at_ms: Option<u64>,
    /// True if this is a deletion marker.
    pub is_tombstone: bool,
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, timestamp_ms: u64, node_id: String) -> Self {
        Self::new_version(value, RecordVersion::from(timestamp_ms), node_id)
    }

    /// Construct a live value with a physical/logical timestamp.
    pub fn new_version(value: Vec<u8>, version: RecordVersion, node_id: String) -> Self {
        Self {
            value,
            timestamp_ms: version.timestamp_ms,
            logical_counter: version.logical_counter,
            node_id,
            expires_at_ms: None,
            is_tombstone: false,
        }
    }

    /// A tombstone for `node_id` at `timestamp_ms`.
    pub fn tombstone(timestamp_ms: u64, node_id: String) -> Self {
        Self::tombstone_version(RecordVersion::from(timestamp_ms), node_id)
    }

    /// Construct a deletion marker with a physical/logical timestamp.
    pub fn tombstone_version(version: RecordVersion, node_id: String) -> Self {
        Self {
            value: Vec::new(),
            timestamp_ms: version.timestamp_ms,
            logical_counter: version.logical_counter,
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
            logical_counter: self.logical_counter,
            node_id: self.node_id.clone(),
            expires_at_ms: self.expires_at_ms,
            is_tombstone: true,
        }
    }

    /// Whether both values have the same full LWW ordering tuple.
    ///
    /// Equal tuples must have equal payloads and expiry deadlines. An expiry
    /// conversion sets the tombstone bit, so it is a distinct ordered version.
    pub fn same_version(&self, other: &Self) -> bool {
        self.version() == other.version()
            && self.node_id == other.node_id
            && self.is_tombstone == other.is_tombstone
    }

    /// Digest of the payload and expiry deadline used in anti-entropy summaries.
    ///
    /// SHA-256 uses a domain prefix, payload length, and an explicit optional
    /// deadline tag so distinct field boundaries cannot produce identical input.
    /// This detects divergent contents; it does not resolve a reused version.
    pub(crate) fn content_digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"actordb-record-content-v1\0");
        digest.update((self.value.len() as u64).to_be_bytes());
        digest.update(&self.value);
        match self.expires_at_ms {
            Some(deadline) => {
                digest.update([1]);
                digest.update(deadline.to_be_bytes());
            }
            None => digest.update([0]),
        }
        digest.finalize().into()
    }

    /// Returns `true` if `self` wins over `other` under last-writer-wins.
    /// Tombstones win exact `(timestamp, logical_counter, node_id)` ties.
    pub fn wins_over(&self, other: &VersionedValue) -> bool {
        (self.version(), &self.node_id, self.is_tombstone)
            > (other.version(), &other.node_id, other.is_tombstone)
    }

    /// Original physical/logical timestamp, before the writer/tombstone tie-break.
    pub const fn version(&self) -> RecordVersion {
        RecordVersion::new(self.timestamp_ms, self.logical_counter)
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
/// fetching only the keys whose versions differ. `content_digest` also detects
/// invalid reuse of the same version for different payloads or TTL deadlines. An expired live value is
/// represented as its deterministic expiry tombstone at the snapshot's
/// pinned wall-clock time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSummary {
    /// SHA-256 of the payload and expiry deadline at the pinned snapshot time.
    pub content_digest: [u8; 32],
    pub key: String,
    pub timestamp_ms: u64,
    pub logical_counter: u64,
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

/// One position in an actor-routed batch read.
///
/// Entries remain in request order, including duplicate keys. `Err` is an
/// explicit per-key admission shed and is never confused with a missing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReadEntry {
    /// Requested key at this position.
    pub key: String,
    /// Visible value, explicit miss, or admission-shed error.
    pub result: Result<Option<VersionedValue>, String>,
}

/// One ordered, byte-bounded prefix returned by [`KvMessage::GetBatch`].
///
/// A non-empty request normally produces a non-empty prefix. Callers resume
/// any omitted suffix explicitly, just as the peer `BatchGet` protocol does.
/// Orderly dispatcher drain may end the page after its admitted key completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchReadPage {
    /// Contiguous request prefix in input order.
    pub entries: Vec<BatchReadEntry>,
}

/// Internal read request which retains admission ownership while actors hold
/// its keys. Public only because [`KvMessage`] is public; it has no public
/// constructor and does not change direct [`KvMessage::GetBatch`] calls.
#[doc(hidden)]
pub struct RetainedBatchRead {
    pub(crate) keys: Vec<String>,
    pub(crate) response_base_payload_len: usize,
    pub(crate) response_soft_limit: usize,
    pub(crate) reply: RpcReplyPort<BatchReadPage>,
    pub(crate) priority: OpPriority,
    pub(crate) retention: Arc<dyn Send + Sync>,
}

impl RetainedBatchRead {
    pub(crate) fn new(
        keys: Vec<String>,
        response_base_payload_len: usize,
        response_soft_limit: usize,
        reply: RpcReplyPort<BatchReadPage>,
        priority: OpPriority,
        retention: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            keys,
            response_base_payload_len,
            response_soft_limit,
            reply,
            priority,
            retention,
        }
    }
}

/// Opaque continuation passed between a fragment dispatcher and its exact
/// sticky worker. It is public only because [`KvMessage`] is public; direct
/// callers cannot construct it and should use [`KvMessage::GetBatch`].
#[doc(hidden)]
pub struct BatchReadContinuation {
    keys: Vec<String>,
    next_index: usize,
    pub(super) results: Vec<Result<Option<VersionedValue>, String>>,
    response_sizing: RepeatedFieldSizer,
    response_soft_limit: usize,
    pub(super) priority: OpPriority,
    pub(super) reply: RpcReplyPort<BatchReadPage>,
    pub(super) dispatcher: ActorRef<KvMessage>,
    _retention: Option<Arc<dyn Send + Sync>>,
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
        logical_counter: u64,
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
    /// The configured [`crate::store::KvStore`] accepted the fragment flush. This does not
    /// imply an fsync unless the store implementation provides that guarantee.
    StoreCommitted,
    /// The configured [`crate::store::KvStore`] completed its explicit stable-storage fence.
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
    /// The key already has this full LWW tuple with different payload or TTL.
    /// The existing value and persistence buffer are left unchanged.
    VersionConflict { key: String },
    /// Persistence failed while applying a batch-triggering mutation.
    Storage(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backpressure => write!(f, "fragment worker pool is at capacity"),
            Self::InvalidInput(error) => write!(f, "invalid mutation: {error}"),
            Self::Storage(error) => write!(f, "persistence failed: {error}"),
            Self::VersionConflict { key } => write!(
                f,
                "record version reused with different contents for key {key:?}"
            ),
        }
    }
}

impl std::error::Error for ApplyError {}

/// Opaque admission ownership attached to an actor message.
/// Public only because [`KvMessage`] is public. Only crate code constructs it.
#[doc(hidden)]
pub struct RetainedActorMessage {
    message: Box<KvMessage>,
    retention: Arc<dyn Send + Sync>,
}

/// Messages handled by [`crate::actor::KvActor`].
#[non_exhaustive]
pub enum KvMessage {
    /// Internal admission lease retained through dispatcher/worker completion.
    #[doc(hidden)]
    Retained(RetainedActorMessage),
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
    /// Scan the fragment's local records with a filter + transform spec.
    /// Routed to the background pool (non-sticky round-robin: scans have no
    /// key), with the same admission control as every other op — shed
    /// explicitly under backpressure, never confused with "no matches".
    /// Visibility matches [`crate::actor::FragmentState::get`]: tombstones
    /// and TTL-expired values are hidden. Results are in lexicographic key
    /// order, bounded by the spec's effective limit.
    Scan {
        spec: ScanSpec,
        reply: RpcReplyPort<Result<Vec<ScanMatch>, String>>,
    },
    /// Internal scan admission retained through background actor ownership.
    #[doc(hidden)]
    ScanRetained {
        spec: ScanSpec,
        retention: Arc<crate::query::QueryRetention>,
        reply: RpcReplyPort<Result<Vec<ScanMatch>, String>>,
    },
    /// Execute one bounded, revision-guarded query page on a background worker.
    QueryPage {
        work: crate::query::QueryPageWork,
        reply: RpcReplyPort<crate::proto::QueryPageResponse>,
    },
    /// Read an ordered prefix of keys through their normal sticky workers.
    /// Admission, rate limiting, and shed accounting remain per key. The
    /// dispatcher routes only the next key after the prior worker releases its
    /// reservation, so retained result memory stays byte-bounded.
    GetBatch {
        keys: Vec<String>,
        /// Encoded `BatchGetResponse` payload length before repeated results.
        /// The node supplies its request-id field here.
        response_base_payload_len: usize,
        /// Soft target for the complete encoded response envelope. One
        /// individually valid result may cross it and is returned alone or as
        /// the terminal entry in the prefix.
        response_soft_limit: usize,
        reply: RpcReplyPort<BatchReadPage>,
        priority: OpPriority,
    },
    /// Internal worker-to-dispatcher continuation for [`KvMessage::GetBatch`].
    #[doc(hidden)]
    ContinueBatchRead(BatchReadContinuation),
    /// Internal Node read admission ownership, retained through actor work.
    #[doc(hidden)]
    GetBatchRetained(RetainedBatchRead),
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

impl KvMessage {
    pub(crate) fn with_retention(self, retention: Arc<dyn Send + Sync>) -> Self {
        Self::Retained(RetainedActorMessage {
            message: Box::new(self),
            retention,
        })
    }
    pub(crate) fn with_optional_retention(self, retention: Option<Arc<dyn Send + Sync>>) -> Self {
        match retention {
            Some(retention) => self.with_retention(retention),
            None => self,
        }
    }
    pub(crate) fn into_retained_parts(self) -> (Self, Option<Arc<dyn Send + Sync>>) {
        let mut message = self;
        let mut combined: Option<Arc<dyn Send + Sync>> = None;
        while let Self::Retained(retained) = message {
            combined = Some(match combined {
                Some(previous) => Arc::new((previous, retained.retention)),
                None => retained.retention,
            });
            message = *retained.message;
        }
        (message, combined)
    }
}

impl BatchReadContinuation {
    pub(super) fn new(
        keys: Vec<String>,
        response_base_payload_len: usize,
        response_soft_limit: usize,
        priority: OpPriority,
        reply: RpcReplyPort<BatchReadPage>,
        dispatcher: ActorRef<KvMessage>,
    ) -> Self {
        let result_capacity = keys.len().min(256);
        Self {
            keys,
            next_index: 0,
            results: Vec::with_capacity(result_capacity),
            response_sizing: RepeatedFieldSizer::new(17, 2, response_base_payload_len),
            response_soft_limit,
            priority,
            reply,
            dispatcher,
            _retention: None,
        }
    }

    pub(super) fn with_optional_retention(
        mut self,
        retention: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        if let Some(retention) = retention {
            self._retention = Some(match self._retention.take() {
                Some(existing) => Arc::new((existing, retention)),
                None => retention,
            });
        }
        self
    }

    pub(super) fn with_retention(mut self, retention: Arc<dyn Send + Sync>) -> Self {
        self._retention = Some(retention);
        self
    }

    pub(super) fn current_key(&self) -> Option<&str> {
        self.keys.get(self.next_index).map(String::as_str)
    }

    pub(super) fn push(&mut self, result: Result<Option<VersionedValue>, String>) {
        let key = self
            .current_key()
            .expect("batch continuation must point at an input key");
        let result_payload_len = match &result {
            Ok(value) => batch_get_result_encoded_len(key, value.as_ref(), false),
            Err(_) => batch_get_result_encoded_len(key, None, true),
        };
        self.response_sizing.push(result_payload_len);
        self.results.push(result);
        self.next_index += 1;
    }

    pub(super) fn is_complete(&self) -> bool {
        self.next_index == self.keys.len()
            || (!self.results.is_empty()
                && self.response_sizing.encoded_len() > self.response_soft_limit)
    }

    pub(super) fn finish(mut self) {
        self.keys.truncate(self.results.len());
        let entries = self
            .keys
            .into_iter()
            .zip(self.results)
            .map(|(key, result)| BatchReadEntry { key, result })
            .collect();
        let _ = self.reply.send(BatchReadPage { entries });
    }

    pub(super) fn return_to_dispatcher(self) {
        if self.reply.is_closed() {
            return;
        }
        if self.is_complete() {
            self.finish();
            return;
        }
        let dispatcher = self.dispatcher.clone();
        if let Err(ractor::MessagingErr::SendErr(KvMessage::ContinueBatchRead(work))) =
            dispatcher.send_message(KvMessage::ContinueBatchRead(self))
        {
            // A drain can close admission between worker completion and this
            // send. Recover the owned message and return the completed prefix;
            // its untouched suffix was never reserved and remains retryable.
            work.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    //! LWW versions and tombstone ordering.

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
}
