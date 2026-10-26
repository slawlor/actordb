// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Phase 5: async sender/receiver replication.
//!
//! Replication moves off the write path. Per fragment, per replication
//! peer, a `ReplicationSender` (the task spawned by `spawn_bounded_sender`) task owns a state machine:
//!
//! ```text
//!   Disconnected ──PeerConnected──▶ Connected ⇄ (batching, acks)
//!        │                              │
//!        │                         buffer overflow
//!        │                              ▼
//!        └─────────────────────▶ BufferReset ──reconcile + ACK fence──▶ Connected
//! ```
//!
//! - **Connected**: dequeued mutations (writes + tombstoned deletes)
//!   accumulate in a bounded local buffer. A [`crate::node::Node`] feeds it
//!   through separate bounded data/control lanes plus persistent coalesced
//!   recovery signals. Standalone senders use those same bounded ingress
//!   lanes. The sender ships batches on
//!   a timer, keeps a bounded number of batches in flight, and drops acked
//!   records. Each sender↔receiver pairing gets a fresh `session_id`; the
//!   receiver discards batches from stale sessions.
//! - **Disconnected**: the peer is down; the buffer is retained for
//!   resume on reconnect.
//! - **BufferReset**: bounded ingress or the owned buffer overflowed. The
//!   overflowing command represents an already-applied mutation; a persistent
//!   overflow generation is recorded, the buffer is shed, and,
//!   if the transport is live, reconciliation starts immediately. Every
//!   round is correlated to its session and round id, fetched values are
//!   awaited locally, and a tracked empty batch fences outbound repairs
//!   before the reset completes.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::actor::{
    ApplyDurability, HlcClock, KvMessage, OpPriority, SnapshotSummary, VersionedValue,
};
use crate::network::PeerConnectionId;
use crate::node::FragmentRouter;
use crate::proto::{
    Envelope, EnvelopeMsg, KeySummary, ReconcileEntry, ReconcileFetch, ReconcileRequest,
    ReplicateBatch, ReplicateOp as ProtoReplicateOp, ReplicationAckLevel, SessionStart,
};
use crate::retained::{ByteBudget, BytePermit};
use crate::shard::FragmentId;
use crate::wire::{
    bytes_field_len, check_envelope_budget, check_replicated_op_budget, encode_envelope,
    varint_field_len, BudgetedFrame, BudgetedPeerTx, ReconcilePermit, ReconcileTicket,
    RepeatedFieldSizer, ANTI_ENTROPY_PAGE_BYTES, MAX_FRAME_BYTES, MAX_RECONCILE_FETCH_KEYS,
    MAX_RECONCILE_PAGE_ITEMS, MAX_WIRE_BATCH_ITEMS,
};

/// Prevent a permanently failing receiver store from becoming a tight
/// NACK/write/log loop. Full retry policy belongs with ACK timeouts and bounded
/// ingress; this floor makes the current explicit retry operationally safe.
const NACK_RETRY_DELAY: Duration = Duration::from_secs(1);

fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

type QueueFailureHandler = Arc<dyn Fn(PeerConnectionId) + Send + Sync>;

/// Runtime-only liveness policy for a replication sender.
///
/// Use [`spawn_bounded_sender`] to choose this sender's liveness policy.
/// The builder exposes public timeout policies, not the
/// Node's internal ingress capacity or transport-failure callback.
#[derive(Clone, bon::Builder)]
pub struct SenderRuntimeOptions {
    #[builder(skip = Arc::new(std::sync::atomic::AtomicBool::new(false)))]
    observation_timing: Arc<std::sync::atomic::AtomicBool>,
    #[builder(default = Duration::from_secs(30))]
    ack_timeout: Duration,
    #[builder(default = Duration::from_secs(30))]
    local_work_timeout: Duration,
    #[builder(default = Duration::from_secs(5))]
    queue_admission_timeout: Duration,
    #[builder(skip = 64)]
    control_channel_capacity: usize,
    #[builder(skip = None)]
    queue_failure_handler: Option<QueueFailureHandler>,
    #[builder(skip = None)]
    write_concerns: Option<Arc<crate::write_concern::WriteConcernTracker>>,
    #[builder(skip = ByteBudget::new(256 * 1024 * 1024))]
    aggregate_retention: Arc<ByteBudget>,
    #[builder(skip = ByteBudget::new(64 * 1024 * 1024))]
    reconciliation_result_budget: Arc<ByteBudget>,
    #[builder(skip)]
    pub(crate) recovery: Option<Arc<crate::node::recovery::RecoveryContext>>,
}

impl std::fmt::Debug for SenderRuntimeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SenderRuntimeOptions")
            .field("ack_timeout", &self.ack_timeout)
            .field("queue_admission_timeout", &self.queue_admission_timeout)
            .field("control_channel_capacity", &self.control_channel_capacity)
            .finish_non_exhaustive()
    }
}

impl Default for SenderRuntimeOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Invalid sender configuration passed to [`spawn_bounded_sender`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SenderOptionsError {
    /// A batch could never contain an operation.
    ZeroBatchSize,
    /// The batch timer cannot make progress.
    ZeroBatchInterval,
    /// No ordinary batch could ever be in flight.
    ZeroMaxInFlight,
    /// No operation could be buffered before reset.
    ZeroBufferCapacity,
    /// Retained operation byte allowance must be nonzero.
    ZeroRetainedBytes,
    /// The Node mutation lane exceeds Tokio's supported bound.
    BufferCapacityTooLarge,
    /// Periodic reconciliation would spin continuously.
    ZeroConsistencyInterval,
    /// A reconciliation phase would expire immediately.
    ZeroReconcileTimeout,
    /// An ordinary batch would expire immediately.
    ZeroAckTimeout,
    /// A local actor-work waiter must have a usable deadline.
    InvalidLocalWorkTimeout,
    /// Result allowance must cover one worst-case local summary and value page.
    InvalidReconciliationResultBudget,
    /// An outbound queue reservation would expire immediately.
    ZeroQueueAdmissionTimeout,
    /// The ordinary ACK timeout cannot be represented by Tokio's clock.
    AckTimeoutTooLarge,
    /// The queue timeout cannot be represented by Tokio's clock.
    QueueAdmissionTimeoutTooLarge,
    /// The Node-only control lane could never accept a protocol response.
    ZeroControlChannelCapacity,
    /// The Node-only control lane exceeds Tokio's supported bound.
    ControlChannelCapacityTooLarge,
    /// The batch interval cannot be represented by Tokio's clock.
    BatchIntervalTooLarge,
    /// The consistency interval cannot be represented by Tokio's clock.
    ConsistencyIntervalTooLarge,
    /// The reconcile timeout cannot be represented by Tokio's clock.
    ReconcileTimeoutTooLarge,
}

impl std::fmt::Display for SenderOptionsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBatchSize => write!(f, "replication batch size must be non-zero"),
            Self::ZeroBatchInterval => write!(f, "replication batch interval must be non-zero"),
            Self::ZeroMaxInFlight => write!(f, "replication max in-flight must be non-zero"),
            Self::ZeroRetainedBytes => {
                write!(f, "replication retained byte capacity must be non-zero")
            }
            Self::ZeroBufferCapacity => {
                write!(f, "replication buffer capacity must be non-zero")
            }
            Self::BufferCapacityTooLarge => {
                write!(f, "replication buffer capacity exceeds Tokio's maximum")
            }
            Self::ZeroConsistencyInterval => {
                write!(f, "replication consistency interval must be non-zero")
            }
            Self::ZeroReconcileTimeout => {
                write!(f, "replication reconcile timeout must be non-zero")
            }
            Self::InvalidReconciliationResultBudget => write!(
                f,
                "reconciliation result allowance must be at least four maximum frames"
            ),
            Self::InvalidLocalWorkTimeout => {
                write!(f, "local work timeout must be positive and representable")
            }
            Self::ZeroAckTimeout => write!(f, "replication ack timeout must be non-zero"),
            Self::ZeroQueueAdmissionTimeout => {
                write!(f, "replication queue admission timeout must be non-zero")
            }
            Self::AckTimeoutTooLarge => {
                write!(f, "replication ack timeout is too large")
            }
            Self::QueueAdmissionTimeoutTooLarge => {
                write!(f, "replication queue admission timeout is too large")
            }
            Self::ZeroControlChannelCapacity => {
                write!(f, "replication control channel capacity must be non-zero")
            }
            Self::ControlChannelCapacityTooLarge => write!(
                f,
                "replication control channel capacity exceeds Tokio's maximum"
            ),
            Self::BatchIntervalTooLarge => {
                write!(f, "replication batch interval is too large")
            }
            Self::ConsistencyIntervalTooLarge => {
                write!(f, "replication consistency interval is too large")
            }
            Self::ReconcileTimeoutTooLarge => {
                write!(f, "replication reconcile timeout is too large")
            }
        }
    }
}

impl std::error::Error for SenderOptionsError {}

impl SenderRuntimeOptions {
    pub(crate) fn with_observation_timing(
        mut self,
        timing: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.observation_timing = timing;
        self
    }
    /// Measure retained-operation queue ages for new Node sender submissions.
    pub fn with_timing_enabled(self, enabled: bool) -> Self {
        self.observation_timing.store(enabled, Ordering::Release);
        self
    }
    pub(crate) fn with_recovery(
        mut self,
        recovery: Option<Arc<crate::node::recovery::RecoveryContext>>,
    ) -> Self {
        self.recovery = recovery;
        self
    }

    /// Set the maximum wait for an acceptable ACK after an ordinary batch is
    /// admitted to the peer queue. Reconciliation barriers continue to use
    /// [`SenderConfig::reconcile_timeout`].
    pub fn with_ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Set the maximum wait to reserve one slot in the peer's outbound frame
    /// queue.
    pub fn with_queue_admission_timeout(mut self, timeout: Duration) -> Self {
        self.queue_admission_timeout = timeout;
        self
    }

    /// Current ordinary-batch ACK timeout.
    pub fn ack_timeout(&self) -> Duration {
        self.ack_timeout
    }

    /// Current peer-queue admission timeout.
    pub fn queue_admission_timeout(&self) -> Duration {
        self.queue_admission_timeout
    }

    pub(crate) fn with_control_channel_capacity(mut self, capacity: usize) -> Self {
        self.control_channel_capacity = capacity;
        self
    }

    pub(crate) fn with_write_concerns(
        mut self,
        tracker: Arc<crate::write_concern::WriteConcernTracker>,
    ) -> Self {
        self.write_concerns = Some(tracker);
        self
    }

    /// Bound one phase of local actor work; submitted storage calls retain ownership.
    pub fn with_local_work_timeout(mut self, timeout: Duration) -> Self {
        self.local_work_timeout = timeout;
        self
    }
    pub fn local_work_timeout(&self) -> Duration {
        self.local_work_timeout
    }

    /// Set aggregate local reconciliation page ownership. Minimum 64 MiB
    /// covers a worst-case summary cursor and value/protobuf page together.
    pub fn with_reconciliation_retained_byte_capacity(mut self, capacity: usize) -> Self {
        self.reconciliation_result_budget = ByteBudget::new(capacity);
        self
    }
    pub(crate) fn with_reconciliation_result_budget(mut self, budget: Arc<ByteBudget>) -> Self {
        self.reconciliation_result_budget = budget;
        self
    }

    pub(crate) fn with_aggregate_retention(mut self, budget: Arc<ByteBudget>) -> Self {
        self.aggregate_retention = budget;
        self
    }

    pub(crate) fn with_queue_failure_handler(mut self, handler: QueueFailureHandler) -> Self {
        self.queue_failure_handler = Some(handler);
        self
    }

    fn validate(&self) -> Result<(), SenderOptionsError> {
        if self.reconciliation_result_budget.limit() < 4 * MAX_FRAME_BYTES {
            return Err(SenderOptionsError::InvalidReconciliationResultBudget);
        }
        if self.local_work_timeout.is_zero()
            || Instant::now()
                .checked_add(self.local_work_timeout)
                .is_none()
        {
            return Err(SenderOptionsError::InvalidLocalWorkTimeout);
        }
        if self.ack_timeout.is_zero() {
            return Err(SenderOptionsError::ZeroAckTimeout);
        }
        if self.queue_admission_timeout.is_zero() {
            return Err(SenderOptionsError::ZeroQueueAdmissionTimeout);
        }
        if self.control_channel_capacity == 0 {
            return Err(SenderOptionsError::ZeroControlChannelCapacity);
        }
        if self.control_channel_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(SenderOptionsError::ControlChannelCapacityTooLarge);
        }
        let now = Instant::now();
        if now.checked_add(self.ack_timeout).is_none() {
            return Err(SenderOptionsError::AckTimeoutTooLarge);
        }
        if now.checked_add(self.queue_admission_timeout).is_none() {
            return Err(SenderOptionsError::QueueAdmissionTimeoutTooLarge);
        }
        Ok(())
    }
}

/// Extensible tuning for the replication sender. Construct with the Bon
/// builder or `Default`; public fields remain available for tuning and inspection.
///
/// ```compile_fail
/// use actordb::replication::SenderConfig;
/// let _ = SenderConfig { ..SenderConfig::default() };
/// ```
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct SenderConfig {
    /// Max ops per batch.
    #[builder(default = 100)]
    pub batch_size: usize,
    /// Ship a batch at least this often when the buffer is non-empty.
    #[builder(default = Duration::from_millis(100))]
    pub batch_interval: Duration,
    /// Max batches in flight awaiting acks.
    #[builder(default = 4)]
    pub max_in_flight: usize,
    /// Max buffered (unshipped) ops per sender. A [`crate::node::Node`] also
    /// uses this as the capacity of the preceding local-mutation ingress lane.
    /// Either overflow sheds queued history and enters `BufferReset`; recovery
    /// reads the authoritative fragment snapshot.
    #[builder(default = 10_000)]
    pub buffer_capacity: usize,
    /// Charged bytes retained through ingress, deque and ACK ownership.
    /// Saturation enters visible reset recovery from the fragment cache.
    #[builder(default = 64 * 1024 * 1024)]
    pub retained_byte_capacity: usize,
    /// How often a connected sender runs anti-entropy reconciliation
    /// to repair divergence.
    #[builder(default = Duration::from_secs(300))]
    pub consistency_interval: Duration,
    /// Maximum time to wait for one network reconciliation phase
    /// (summary response, fetched-data chunk, or durability barrier ACK).
    /// A timeout rotates the session and retries the round, so a connected
    /// but silent peer cannot strand bootstrap or overflow recovery forever.
    #[builder(default = Duration::from_secs(30))]
    pub reconcile_timeout: Duration,
    /// Minimum receiver fence that may retire an in-flight batch.
    /// `MemoryApplied` accepts any stronger acknowledgement level;
    /// `StoreCommitted` requires a receiver with a configured store, and
    /// `StableCommitted` additionally requires its explicit stable fence.
    #[builder(default = ReplicationAckDurability::MemoryApplied)]
    pub required_ack_durability: ReplicationAckDurability,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SenderConfig {
    pub(crate) fn validate(&self) -> Result<(), SenderOptionsError> {
        if self.batch_size == 0 {
            return Err(SenderOptionsError::ZeroBatchSize);
        }
        if self.batch_interval.is_zero() {
            return Err(SenderOptionsError::ZeroBatchInterval);
        }
        if self.max_in_flight == 0 {
            return Err(SenderOptionsError::ZeroMaxInFlight);
        }
        if self.retained_byte_capacity == 0 {
            return Err(SenderOptionsError::ZeroRetainedBytes);
        }
        if self.buffer_capacity == 0 {
            return Err(SenderOptionsError::ZeroBufferCapacity);
        }
        if self.buffer_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(SenderOptionsError::BufferCapacityTooLarge);
        }
        if self.consistency_interval.is_zero() {
            return Err(SenderOptionsError::ZeroConsistencyInterval);
        }
        if self.reconcile_timeout.is_zero() {
            return Err(SenderOptionsError::ZeroReconcileTimeout);
        }
        let now = Instant::now();
        if now.checked_add(self.batch_interval).is_none() {
            return Err(SenderOptionsError::BatchIntervalTooLarge);
        }
        if now.checked_add(self.consistency_interval).is_none() {
            return Err(SenderOptionsError::ConsistencyIntervalTooLarge);
        }
        if now.checked_add(self.reconcile_timeout).is_none() {
            return Err(SenderOptionsError::ReconcileTimeoutTooLarge);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SenderRetention {
    local: Arc<ByteBudget>,
    aggregate: Arc<ByteBudget>,
}
#[derive(Debug)]
struct RetentionLease {
    _local: BytePermit,
    _aggregate: BytePermit,
}
impl SenderRetention {
    fn reserve(&self, bytes: usize) -> Option<RetentionLease> {
        Some(RetentionLease {
            _local: self.local.reserve(bytes)?,
            _aggregate: self.aggregate.reserve(bytes)?,
        })
    }
    fn borrowed(&self, op: &crate::actor::ReplicatedOp) -> Option<RetainedOp> {
        // Reserve before fanout clones a payload for this replica.
        let lease = self.reserve(retained_borrowed_op_bytes(op))?;
        Some(RetainedOp {
            op: op.clone(),
            _lease: lease,
            enqueued_at: None,
        })
    }
}
fn retained_borrowed_op_bytes(op: &crate::actor::ReplicatedOp) -> usize {
    // Clones own only initialized data, not spare capacity of the caller.
    3usize.saturating_mul(
        256usize
            .saturating_add(op.key.len())
            .saturating_add(op.value.node_id.len())
            .saturating_add(op.value.value.len()),
    )
}
#[derive(Debug)]
struct RetainedOp {
    op: crate::actor::ReplicatedOp,
    _lease: RetentionLease,
    enqueued_at: Option<std::time::Instant>,
}
impl std::ops::Deref for RetainedOp {
    type Target = crate::actor::ReplicatedOp;
    fn deref(&self) -> &Self::Target {
        &self.op
    }
}
impl std::borrow::Borrow<crate::actor::ReplicatedOp> for RetainedOp {
    fn borrow(&self) -> &crate::actor::ReplicatedOp {
        &self.op
    }
}
impl std::ops::DerefMut for RetainedOp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}
struct RetainedControl {
    msg: SenderMsg,
    lease: RetentionLease,
    apply_retention: Option<Arc<dyn Send + Sync>>,
}
fn retained_control_bytes(msg: &SenderMsg) -> usize {
    let owned = match msg {
        SenderMsg::ReconcileResponse {
            summaries,
            next_after_key,
            ..
        } => next_after_key
            .capacity()
            .saturating_add(
                summaries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<KeySummary>()),
            )
            .saturating_add(summaries.iter().fold(0usize, |bytes, s| {
                bytes
                    .saturating_add(128)
                    .saturating_add(s.key.capacity())
                    .saturating_add(s.node_id.capacity())
                    .saturating_add(s.content_digest.capacity())
            })),
        SenderMsg::ReconcileData { entries, .. } => entries
            .capacity()
            .saturating_mul(std::mem::size_of::<ReconcileEntry>())
            .saturating_add(entries.iter().fold(0usize, |bytes, s| {
                bytes
                    .saturating_add(128)
                    .saturating_add(s.key.capacity())
                    .saturating_add(s.node_id.capacity())
                    .saturating_add(s.value.capacity())
            })),
        SenderMsg::Nack { error, .. } => error.capacity(),
        _ => return 0,
    };
    2usize.saturating_mul(256usize.saturating_add(owned))
}

/// Messages driving a `ReplicationSender` (the task spawned by [`spawn_bounded_sender`]).
pub enum SenderMsg {
    /// An already-applied mutation to replicate. Buffer-reset recovery reads
    /// the authoritative fragment snapshot, so callers must not enqueue an
    /// operation before its local merge has completed.
    Enqueue(crate::actor::ReplicatedOp),
    /// The peer (re)connected; start a new session on this channel.
    PeerConnected { peer: ReplicationPeer },
    /// One peer connection dropped. Ignore it if a newer channel has already
    /// replaced it; otherwise retain the buffer for reconnect.
    PeerDisconnected { connection_id: PeerConnectionId },
    /// The peer acked a batch after reaching the reported durability fence.
    Ack {
        session_id: u64,
        batch_seq: u64,
        durability: ReplicationAckDurability,
    },
    /// The peer could not apply or flush a batch. Requeue the exact stamped
    /// operations; LWW application makes retries idempotent.
    Nack {
        session_id: u64,
        batch_seq: u64,
        error: String,
    },
    /// Response to our reconcile request.
    ReconcileResponse {
        session_id: u64,
        round_id: u64,
        page_seq: u64,
        next_after_key: String,
        snapshot_revision: u64,
        snapshot_time_ms: u64,
        done: bool,
        restart: bool,
        summaries: Vec<KeySummary>,
        /// Tells the connection task whether the correlated frame passed
        /// timestamp validation. Stale frames return `true` without touching
        /// the HLC; an invalid current frame returns `false` so the connection
        /// is closed.
        connection_valid: oneshot::Sender<bool>,
    },
    /// Full values for keys we asked to fetch.
    ReconcileData {
        session_id: u64,
        round_id: u64,
        chunk_seq: u64,
        page_seq: u64,
        done: bool,
        entries: Vec<ReconcileEntry>,
        connection_valid: oneshot::Sender<bool>,
    },
    /// Trigger an anti-entropy reconcile round now (if connected).
    /// Used by the Phase 8 placement driver to bootstrap a newly
    /// assigned fragment without waiting for the periodic timer.
    ReconcileNow,
}

#[derive(Clone, Default)]
struct NodeSenderSignals {
    overflow_generation: u64,
    reconcile_generation: u64,
    drain: Option<NodeSenderDrainRequest>,
    peer_generation: u64,
    peer: Option<ReplicationPeer>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeSenderDrainRequest {
    generation: u64,
    required_ack_durability: ReplicationAckDurability,
}

/// One bounded standalone peer transport binding. Create once per physical
/// connection and clone for duplicate notifications; replacing the connection
/// creates a new identity even when the peer name remains unchanged.
/// The embedding owns the output receiver and its transport byte budget.
#[derive(Clone)]
pub struct ReplicationPeer {
    transport: SenderTransport,
}

#[derive(Clone)]
enum SenderTransport {
    Standalone {
        connection_id: PeerConnectionId,
        tx: mpsc::Sender<Vec<u8>>,
    },
    NodeBudgeted(BudgetedPeerTx),
}

impl ReplicationPeer {
    /// Bind a caller-owned bounded output queue to one fresh connection ID.
    /// Reuse clones of this handle for all notifications about that connection.
    ///
    /// ```
    /// use actordb::replication::{ReplicationPeer, SenderMsg};
    /// let (output, _frames) = tokio::sync::mpsc::channel(8);
    /// let peer = ReplicationPeer::new(output);
    /// let _connected = SenderMsg::PeerConnected { peer: peer.clone() };
    /// let _disconnected = SenderMsg::PeerDisconnected {
    ///     connection_id: peer.connection_id(),
    /// };
    /// ```
    pub fn new(tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            transport: SenderTransport::Standalone {
                connection_id: PeerConnectionId::new(),
                tx,
            },
        }
    }
    pub fn connection_id(&self) -> PeerConnectionId {
        self.id().clone()
    }
    fn id(&self) -> &PeerConnectionId {
        match &self.transport {
            SenderTransport::Standalone { connection_id, .. } => connection_id,
            SenderTransport::NodeBudgeted(peer) => &peer.connection_id,
        }
    }
    fn from_budgeted(peer: BudgetedPeerTx) -> Self {
        Self {
            transport: SenderTransport::NodeBudgeted(peer),
        }
    }
    fn node_transport(&self) -> Option<&BudgetedPeerTx> {
        match &self.transport {
            SenderTransport::NodeBudgeted(peer) => Some(peer),
            SenderTransport::Standalone { .. } => None,
        }
    }
}

struct NodeSenderIngress {
    retention: SenderRetention,
    reconciliation_result_budget: Arc<ByteBudget>,
    draining: StdMutex<bool>,
    pending_data: AtomicUsize,
    shed_operations: AtomicU64,
    reset_requests: AtomicU64,
    recovery_active: std::sync::atomic::AtomicBool,
    issued_overflow_generation: AtomicU64,
    settled_overflow_generation: AtomicU64,
}

impl Default for NodeSenderIngress {
    fn default() -> Self {
        Self {
            retention: SenderRetention {
                local: ByteBudget::new(64 * 1024 * 1024),
                aggregate: ByteBudget::new(256 * 1024 * 1024),
            },
            reconciliation_result_budget: ByteBudget::new(64 * 1024 * 1024),
            draining: StdMutex::new(false),
            pending_data: AtomicUsize::new(0),
            shed_operations: AtomicU64::new(0),
            reset_requests: AtomicU64::new(0),
            recovery_active: std::sync::atomic::AtomicBool::new(false),
            issued_overflow_generation: AtomicU64::new(0),
            settled_overflow_generation: AtomicU64::new(0),
        }
    }
}

struct NodeSenderDrainRuntime {
    completed: watch::Sender<u64>,
    ingress: Arc<NodeSenderIngress>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SenderIngressError {
    Full,
    Closed,
}

/// Bounded standalone ingress observations for this sender's lifetime.
/// Independent atomic/channel reads are approximate, not a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SenderIngressDiagnostics {
    /// Admitted mutations not yet incorporated into a published sender state.
    /// This includes a dequeued item while its state publication is pending.
    pub pending_operations: usize,
    pub queued_controls: usize,
    /// Payload charges across ingress, buffered operations, in-flight batches,
    /// and admitted protocol controls; not allocator resident bytes.
    pub retained_bytes: usize,
    pub reconciliation_retained_bytes: usize,
    /// Incoming applied-operation deltas shed at count or byte saturation.
    /// Does not count all prior buffered deltas discarded by a snapshot reset.
    /// Recovery reads their equal-or-newer authoritative LWW floors.
    pub shed_operations: u64,
    /// Requested snapshot resets, including control-payload saturation.
    pub reset_requests: u64,
    /// A requested reset has not obtained its correlated recovery ACK fence.
    /// Can remain true after stop: stopping is not successful delivery.
    pub recovery_pending: bool,
    pub closed: bool,
}

/// Bounded standalone replication ingress with independent control signals.
/// Locally merged mutations may be shed into visible snapshot reset recovery
/// on count or byte saturation; an applied write is never rejected afterwards.
#[derive(Clone)]
pub struct BoundedSender {
    inner: NodeSender,
}
impl BoundedSender {
    /// Submit an already-applied operation or protocol/lifecycle message.
    /// `Enqueue` returning `Ok` means bounded delta admission OR visible
    /// snapshot-reset recovery, not delivery or a durability acknowledgement.
    /// Saturated protocol controls return `Full` and must be retried or cause
    /// the embedding to close that connection.
    pub fn send(&self, message: SenderMsg) -> Result<(), SenderIngressError> {
        self.inner.send(message)
    }
    /// Current charged retained bytes including queued and in-flight payloads.
    pub fn retained_bytes(&self) -> usize {
        self.inner.ingress.retention.local.used()
    }
    /// Charged local result bytes across standalone senders sharing options.
    pub fn reconciliation_retained_bytes(&self) -> usize {
        self.inner.ingress.reconciliation_result_budget.used()
    }
    pub fn ingress_diagnostics(&self) -> SenderIngressDiagnostics {
        let ingress = &self.inner.ingress;
        SenderIngressDiagnostics {
            pending_operations: ingress.pending_data.load(Ordering::Acquire),
            queued_controls: self.inner.control_tx.max_capacity()
                - self.inner.control_tx.capacity(),
            retained_bytes: self.retained_bytes(),
            reconciliation_retained_bytes: self.reconciliation_retained_bytes(),
            shed_operations: ingress.shed_operations.load(Ordering::Relaxed),
            reset_requests: ingress.reset_requests.load(Ordering::Relaxed),
            recovery_pending: ingress.recovery_active.load(Ordering::Acquire)
                || ingress.issued_overflow_generation.load(Ordering::Acquire)
                    != ingress.settled_overflow_generation.load(Ordering::Acquire),
            closed: self.inner.is_closed(),
        }
    }
    /// Close admission, stop the sender and release its owned queue permits.
    /// This local cancellation does not certify remote delivery or durability.
    pub async fn stop(&self) {
        self.inner.stop().await;
    }
}

/// Spawn a standalone sender whose ingress and retained payloads are bounded.
/// Invalid count, byte, and timeout limits fail before spawning a task.
pub fn spawn_bounded_sender(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    options: SenderRuntimeOptions,
) -> Result<BoundedSender, SenderOptionsError> {
    spawn_node_sender(fragment, peer_id, config, deps, options).map(|inner| BoundedSender { inner })
}

/// Node-owned bounded ingress for one replication sender.
///
/// Locally applied mutations enter a bounded data lane,
/// protocol responses enter a separate bounded control lane, and coalescible
/// lifecycle/recovery signals remain available even when either lane is full.
#[derive(Clone)]
pub(crate) struct NodeSender {
    observation_timing: Arc<std::sync::atomic::AtomicBool>,
    data_tx: mpsc::Sender<RetainedOp>,
    control_tx: mpsc::Sender<RetainedControl>,
    signals_tx: watch::Sender<NodeSenderSignals>,
    ingress: Arc<NodeSenderIngress>,
    abort: AbortHandle,
    stopped: watch::Receiver<bool>,
    drain_completed: watch::Receiver<u64>,
    configured_ack_durability: ReplicationAckDurability,
    stop_lock: Arc<Mutex<()>>,
}

pub(crate) struct NodeSenderDrain {
    pub(crate) generation: u64,
    completed: watch::Receiver<u64>,
}

impl NodeSenderDrain {
    pub(crate) fn is_complete(&self) -> bool {
        *self.completed.borrow() >= self.generation
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.completed.has_changed().is_err()
    }
}

impl NodeSender {
    pub(crate) fn same_task(&self, other: &Self) -> bool {
        self.data_tx.same_channel(&other.data_tx)
    }

    #[cfg(test)]
    pub(crate) fn control_capacity(&self) -> usize {
        self.control_tx.capacity()
    }

    pub(crate) fn send(&self, msg: SenderMsg) -> Result<(), SenderIngressError> {
        if self.is_closed() {
            return Err(SenderIngressError::Closed);
        }
        match msg {
            SenderMsg::Enqueue(op) => self.send_data(op),
            SenderMsg::PeerConnected { peer } => self.set_peer(peer),
            SenderMsg::PeerDisconnected { connection_id } => {
                self.signals_tx.send_if_modified(|signals| {
                    if !signals
                        .peer
                        .as_ref()
                        .is_some_and(|current| current.id() == &connection_id)
                    {
                        return false;
                    }
                    signals.peer_generation = next_generation(signals.peer_generation);
                    signals.peer = None;
                    true
                });
                Ok(())
            }
            SenderMsg::ReconcileNow => {
                self.signals_tx.send_modify(|signals| {
                    signals.reconcile_generation = next_generation(signals.reconcile_generation);
                });
                Ok(())
            }
            control => self.send_control(control),
        }
    }

    /// Install a Node-owned transport together with its encoded-byte budget.
    /// Standalone senders use `ReplicationPeer` bounded output handles and
    /// caller-owned transport byte budgets; every Node connection uses this
    /// fully byte-gated path.
    pub(crate) fn connect_peer(&self, peer: BudgetedPeerTx) -> Result<(), SenderIngressError> {
        if self.is_closed() {
            return Err(SenderIngressError::Closed);
        }
        self.set_peer(ReplicationPeer::from_budgeted(peer))
    }

    fn set_peer(&self, peer: ReplicationPeer) -> Result<(), SenderIngressError> {
        self.signals_tx.send_if_modified(|signals| {
            if let Some(current) = signals.peer.as_ref() {
                if current.id() == peer.id() {
                    return false;
                }
            }
            signals.peer_generation = next_generation(signals.peer_generation);
            signals.peer = Some(peer);
            true
        });
        Ok(())
    }

    fn send_data(&self, op: crate::actor::ReplicatedOp) -> Result<(), SenderIngressError> {
        self.offer(&op)
    }

    /// Charge both sender and node byte allowances before cloning fanout data.
    pub(crate) fn offer(&self, op: &crate::actor::ReplicatedOp) -> Result<(), SenderIngressError> {
        let draining = self.ingress.draining.lock().unwrap();
        if *draining || self.is_closed() {
            return Err(SenderIngressError::Closed);
        }
        let Some(mut op) = self.ingress.retention.borrowed(op) else {
            crate::node::observability::add_saturating(&self.ingress.shed_operations, 1);
            tracing::warn!(
                retained_bytes = self.ingress.retention.local.used(),
                aggregate_retained_bytes = self.ingress.retention.aggregate.used(),
                "replication byte allowance saturated; scheduling reset"
            );
            self.schedule_reset();
            return Ok(());
        };
        op.enqueued_at = self
            .observation_timing
            .load(Ordering::Acquire)
            .then(std::time::Instant::now);
        self.ingress.pending_data.fetch_add(1, Ordering::AcqRel);
        let result = match self.data_tx.try_send(op) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.ingress.pending_data.fetch_sub(1, Ordering::AcqRel);
                crate::node::observability::add_saturating(&self.ingress.shed_operations, 1);
                self.schedule_reset();
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.ingress.pending_data.fetch_sub(1, Ordering::AcqRel);
                Err(SenderIngressError::Closed)
            }
        };
        drop(draining);
        result
    }

    fn send_control(&self, control: SenderMsg) -> Result<(), SenderIngressError> {
        self.send_control_retained(control, None)
    }

    /// Retain decoded receive ownership through queueing and actual actor apply.
    pub(crate) fn send_retained(
        &self,
        control: SenderMsg,
        retention: Arc<dyn Send + Sync>,
    ) -> Result<(), SenderIngressError> {
        if self.is_closed() {
            return Err(SenderIngressError::Closed);
        }
        self.send_control_retained(control, Some(retention))
    }

    fn send_control_retained(
        &self,
        control: SenderMsg,
        apply_retention: Option<Arc<dyn Send + Sync>>,
    ) -> Result<(), SenderIngressError> {
        let Some(lease) = self
            .ingress
            .retention
            .reserve(retained_control_bytes(&control))
        else {
            self.schedule_reset();
            return Err(SenderIngressError::Full);
        };
        match self.control_tx.try_send(RetainedControl {
            msg: control,
            lease,
            apply_retention,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(SenderIngressError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SenderIngressError::Closed),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.abort.is_finished() || self.data_tx.is_closed() || self.control_tx.is_closed()
    }

    pub(crate) fn schedule_reset(&self) {
        crate::node::observability::add_saturating(&self.ingress.reset_requests, 1);
        self.ingress.recovery_active.store(true, Ordering::Release);
        self.signals_tx.send_modify(|signals| {
            signals.overflow_generation = next_generation(signals.overflow_generation);
            // `send_modify` notifies receivers only after this closure. Keep
            // the placement/drain-visible issued watermark ordered before
            // that notification so no observer can see the signal while the
            // ingress still appears idle at the prior generation.
            self.ingress
                .issued_overflow_generation
                .store(signals.overflow_generation, Ordering::Release);
        });
    }

    /// Request a stable, correlated full-fragment reconciliation followed by
    /// the configured receiver durability barrier. The generation is private
    /// to this exact sender task, so replacing a sender cannot satisfy an old
    /// shutdown fence.
    pub(crate) fn request_drain(&self) -> Result<NodeSenderDrain, SenderIngressError> {
        self.request_drain_at(self.configured_ack_durability)
    }

    /// Request a drain using an explicit receiver durability fence. This is
    /// crate-private so placement can demand a stronger handoff certificate
    /// without changing the durability of ordinary replication batches.
    pub(crate) fn request_drain_at(
        &self,
        required_ack_durability: ReplicationAckDurability,
    ) -> Result<NodeSenderDrain, SenderIngressError> {
        // Serialize with data try_send so every accepted item is either ahead
        // of this fence or rejected after it. The task consumes the admitted
        // lane before taking its authoritative reconciliation snapshot.
        let mut draining = self.ingress.draining.lock().unwrap();
        if self.is_closed() {
            return Err(SenderIngressError::Closed);
        }
        *draining = true;
        let mut generation = 0;
        self.signals_tx.send_modify(|signals| {
            generation = next_generation(signals.drain.map_or(0, |drain| drain.generation));
            // A later request may satisfy an earlier waiter only if it keeps
            // the strongest outstanding durability requirement.
            let required_ack_durability = signals.drain.map_or(required_ack_durability, |drain| {
                drain.required_ack_durability.max(required_ack_durability)
            });
            signals.drain = Some(NodeSenderDrainRequest {
                generation,
                required_ack_durability,
            });
        });
        debug_assert_ne!(generation, 0);
        Ok(NodeSenderDrain {
            generation,
            completed: self.drain_completed.clone(),
        })
    }

    /// True only after every admitted data item has been reflected in a
    /// published sender state and every coalesced overflow has completed its
    /// correlated reset reconciliation.
    pub(crate) fn ingress_is_idle(&self) -> bool {
        let issued = self
            .ingress
            .issued_overflow_generation
            .load(Ordering::Acquire);
        self.ingress.pending_data.load(Ordering::Acquire) == 0
            && self
                .ingress
                .settled_overflow_generation
                .load(Ordering::Acquire)
                == issued
    }

    pub(crate) fn request_stop(&self) {
        self.abort.abort();
    }

    pub(crate) async fn stop(&self) {
        // Serialize concurrent placement/shutdown removal so they wait on the
        // same task completion instead of racing cloned watch receivers.
        let _guard = self.stop_lock.lock().await;
        self.request_stop();
        let mut stopped = self.stopped.clone();
        while !*stopped.borrow() {
            if stopped.changed().await.is_err() {
                break;
            }
        }
        // A pre-poll abort still drops the captured exit guard. Its watch
        // notification can precede the remaining captured inbox fields being
        // dropped, so also wait for actual task teardown before returning.
        while !self.abort.is_finished() {
            tokio::task::yield_now().await;
        }
    }
}

enum SenderInput {
    Data(RetainedOp, Arc<NodeSenderIngress>),
    Control(RetainedControl),
    Overflow(u64, Arc<NodeSenderIngress>),
    PeerChanged(Option<ReplicationPeer>),
    ReconcileNow,
    Drain(NodeSenderDrainRequest),
    Closed,
}

struct SenderInbox {
    data_rx: mpsc::Receiver<RetainedOp>,
    control_rx: mpsc::Receiver<RetainedControl>,
    signals_rx: watch::Receiver<NodeSenderSignals>,
    delivered_overflow_generation: u64,
    delivered_reconcile_generation: u64,
    delivered_drain_generation: u64,
    delivered_peer_generation: u64,
    ingress: Arc<NodeSenderIngress>,
}

impl SenderInbox {
    fn settle_overflow_through(&mut self, generation: u64) {
        self.delivered_overflow_generation = generation;
    }

    async fn recv(&mut self) -> SenderInput {
        let Self {
            data_rx,
            control_rx,
            signals_rx,
            delivered_overflow_generation,
            delivered_reconcile_generation,
            delivered_drain_generation,
            delivered_peer_generation,
            ingress,
        } = self;
        loop {
            // Compare and advance each latest-value field independently.
            // An unrelated update must never erase an overflow reset that
            // was observed but has not yet reached the state machine.
            let signals = signals_rx.borrow().clone();
            if signals.overflow_generation != *delivered_overflow_generation {
                *delivered_overflow_generation = signals.overflow_generation;
                return SenderInput::Overflow(signals.overflow_generation, ingress.clone());
            }
            if signals.peer_generation != *delivered_peer_generation {
                *delivered_peer_generation = signals.peer_generation;
                return SenderInput::PeerChanged(signals.peer);
            }
            if signals.reconcile_generation != *delivered_reconcile_generation {
                *delivered_reconcile_generation = signals.reconcile_generation;
                return SenderInput::ReconcileNow;
            }
            if let Some(drain) = signals
                .drain
                .filter(|drain| drain.generation != *delivered_drain_generation)
            {
                *delivered_drain_generation = drain.generation;
                return SenderInput::Drain(drain);
            }
            tokio::select! {
                biased;
                changed = signals_rx.changed() => {
                    if changed.is_err() {
                        return SenderInput::Closed;
                    }
                }
                control = control_rx.recv() => {
                    return control
                        .map(SenderInput::Control)
                        .unwrap_or(SenderInput::Closed);
                }
                data = data_rx.recv() => {
                    return data
                        .map(|op| SenderInput::Data(op, ingress.clone()))
                        .unwrap_or(SenderInput::Closed);
                }
            }
        }
    }
}

struct NodeSenderTaskExit(watch::Sender<bool>, Arc<NodeSenderIngress>);

impl Drop for NodeSenderTaskExit {
    fn drop(&mut self) {
        // The task can stop before queued data is selected. Dropping its
        // inbox releases those payloads; they are no longer pending ingress.
        self.1.pending_data.store(0, Ordering::Release);
        let _ = self.0.send(true);
    }
}

/// Receiver guarantee attached to the most recent accepted batch ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReplicationAckDurability {
    /// Every operation is visible in the receiver's in-memory cache. No store
    /// flush is required, even when the receiver has a configured store.
    MemoryApplied,
    /// The receiver applied every op and its configured store accepted a
    /// flush. This is not an fsync guarantee unless that store says so.
    StoreCommitted,
    /// The receiver applied every op and its configured store completed an
    /// explicit stable-storage fence.
    StableCommitted,
}

impl ReplicationAckDurability {
    fn proto_level(self) -> ReplicationAckLevel {
        match self {
            Self::MemoryApplied => ReplicationAckLevel::Applied,
            Self::StoreCommitted => ReplicationAckLevel::StoreCommitted,
            Self::StableCommitted => ReplicationAckLevel::StableCommitted,
        }
    }
}

/// A batch awaiting its ack.
struct InflightBatch {
    batch_seq: u64,
    ops: Vec<RetainedOp>,
    kind: InflightKind,
    required_ack_durability: ReplicationAckDurability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InflightKind {
    Streaming { ack_deadline: Instant },
    ReconcileBarrier,
}

#[derive(Debug)]
enum InflightAckResult {
    Missing,
    BelowRequired {
        required: ReplicationAckDurability,
    },
    Accepted {
        kind: InflightKind,
        ops: Vec<RetainedOp>,
    },
}

fn acknowledge_in_flight(
    connection: &mut ActiveConnection,
    batch_seq: u64,
    durability: ReplicationAckDurability,
) -> InflightAckResult {
    let Some(position) = connection
        .in_flight
        .iter()
        .position(|batch| batch.batch_seq == batch_seq)
    else {
        return InflightAckResult::Missing;
    };
    let required = connection.in_flight[position].required_ack_durability;
    if durability < required {
        return InflightAckResult::BelowRequired { required };
    }
    let batch = connection
        .in_flight
        .remove(position)
        .expect("in-flight batch position was just resolved");
    InflightAckResult::Accepted {
        kind: batch.kind,
        ops: batch.ops,
    }
}

struct ActiveConnection {
    peer_tx: SenderPeer,
    session_id: u64,
    session_ready: bool,
    next_batch_seq: u64,
    in_flight: VecDeque<InflightBatch>,
}

impl ActiveConnection {
    fn next_streaming_deadline(&self) -> Option<Instant> {
        self.in_flight
            .iter()
            .filter_map(|batch| match batch.kind {
                InflightKind::Streaming { ack_deadline } => Some(ack_deadline),
                InflightKind::ReconcileBarrier => None,
            })
            .min()
    }
}

#[derive(Clone)]
struct SenderPeer {
    peer: ReplicationPeer,
    queue_admission_timeout: Duration,
    local_work_timeout: Duration,
    reconciliation_result_budget: Arc<ByteBudget>,
    queue_failure_handler: Option<QueueFailureHandler>,
    recovery: Option<Arc<crate::node::recovery::RecoveryContext>>,
}

impl SenderPeer {
    fn new(peer: ReplicationPeer, options: &SenderRuntimeOptions) -> Self {
        Self {
            peer,
            queue_admission_timeout: options.queue_admission_timeout,
            local_work_timeout: options.local_work_timeout,
            reconciliation_result_budget: options.reconciliation_result_budget.clone(),
            queue_failure_handler: options.queue_failure_handler.clone(),
            recovery: options.recovery.clone(),
        }
    }

    fn same_connection(&self, other: &PeerConnectionId) -> bool {
        self.peer.id() == other
    }

    fn report_failure(&self) {
        if let Some(handler) = &self.queue_failure_handler {
            handler(self.peer.connection_id());
        }
    }

    fn must_disconnect_after_failure(&self) -> bool {
        (match &self.peer.transport {
            SenderTransport::Standalone { tx, .. } => tx.is_closed(),
            SenderTransport::NodeBudgeted(peer) => peer.tx.is_closed(),
        }) || self.queue_failure_handler.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryState {
    Streaming,
    Reset { epoch: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconcilePurpose {
    Routine,
    Reset {
        epoch: u64,
    },
    Drain {
        generation: u64,
        required_ack_durability: ReplicationAckDurability,
    },
}

impl ReconcilePurpose {
    fn required_ack_durability(
        self,
        configured: ReplicationAckDurability,
    ) -> ReplicationAckDurability {
        match self {
            Self::Drain {
                required_ack_durability,
                ..
            } => required_ack_durability,
            Self::Routine | Self::Reset { .. } => configured,
        }
    }
}

#[derive(Default)]
struct SenderDrainState {
    pending: Option<NodeSenderDrainRequest>,
    prepared_generation: Option<u64>,
    // Completion is a terminal certificate for this sender incarnation.
    // Placement either retires it or explicitly issues a newer drain; routine
    // reconciliation must not mutate the source behind the certificate.
    sealed_after_completion: bool,
}

impl SenderDrainState {
    fn request(&mut self, drain: NodeSenderDrainRequest) {
        self.pending = Some(drain);
        self.sealed_after_completion = false;
    }

    fn pending_preparation(&self) -> Option<NodeSenderDrainRequest> {
        self.pending
            .filter(|drain| self.prepared_generation != Some(drain.generation))
    }

    fn mark_prepared(&mut self, generation: u64) {
        self.prepared_generation = Some(generation);
    }

    fn prepared_purpose(&self) -> Option<ReconcilePurpose> {
        let drain = self.pending?;
        (self.prepared_generation == Some(drain.generation)).then_some(ReconcilePurpose::Drain {
            generation: drain.generation,
            required_ack_durability: drain.required_ack_durability,
        })
    }

    fn complete(&mut self) {
        self.pending = None;
        self.prepared_generation = None;
        self.sealed_after_completion = true;
    }

    fn routine_reconcile_allowed(&self) -> bool {
        self.pending.is_none() && !self.sealed_after_completion
    }
}

struct ReconcileSummaryState {
    next_page_seq: u64,
    next_fetch_chunk_seq: u64,
    after_key: String,
    remote_snapshot_revision: Option<u64>,
    remote_snapshot_time_ms: Option<u64>,
    remote_revision_stable: bool,
    local: ReconcileLocalCursor,
    changed_any: bool,
    summary_done: bool,
}

struct ReconcileLocalCursor {
    result_retention: Option<Arc<BytePermit>>,
    after_key: String,
    revision: Option<u64>,
    snapshot_time_ms: Option<u64>,
    revision_stable: bool,
    buffered: VecDeque<SnapshotSummary>,
    current: Option<SnapshotSummary>,
    done: bool,
}

enum ReconcilePhase {
    Response(ReconcileSummaryState),
    Data {
        summary: ReconcileSummaryState,
        remaining_chunks: VecDeque<Vec<String>>,
        next_chunk_seq: u64,
        expected_keys: HashSet<String>,
        received_keys: HashSet<String>,
        next_page_seq: u64,
    },
    Barrier {
        batch_seq: u64,
    },
}

struct ReconcileRound {
    session_id: u64,
    round_id: u64,
    purpose: ReconcilePurpose,
    phase: ReconcilePhase,
    phase_deadline: Instant,
    snapshot_revision: Option<u64>,
    /// Peer-wide credit retained for the complete round, including its
    /// durability barrier. Dropping or completing the round releases it.
    connection_permit: Option<ReconcilePermit>,
    /// Keep the remote summary charge while derived fetch-key chunks remain.
    control_retention: Option<RetentionLease>,
}

fn requeue_in_flight(in_flight: &mut VecDeque<InflightBatch>, buffer: &mut VecDeque<RetainedOp>) {
    // Pop newest batches first and push each batch in reverse so the final
    // buffer preserves original batch and operation order.
    while let Some(batch) = in_flight.pop_back() {
        for op in batch.ops.into_iter().rev() {
            buffer.push_front(op);
        }
    }
}

fn disconnect_transport(
    connection: &mut Option<ActiveConnection>,
    delivery: DeliveryState,
    buffer: &mut VecDeque<RetainedOp>,
) -> bool {
    if connection
        .as_ref()
        .is_some_and(|connection| !connection.peer_tx.must_disconnect_after_failure())
    {
        return false;
    }
    if let Some(mut connection) = connection.take() {
        if matches!(delivery, DeliveryState::Streaming) {
            requeue_in_flight(&mut connection.in_flight, buffer);
        }
    }
    true
}

/// A snapshot of a sender's state, for health checks and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SenderStateReport {
    pub progress: SenderProgress,
    /// "disconnected", "connected", or "buffer_reset".
    pub state: String,
    /// Current session id (0 if none).
    pub session_id: u64,
    /// Ops currently buffered.
    pub buffered: usize,
    /// Charged bytes across this sender ingress, queued and in-flight work.
    pub retained_bytes: usize,
    /// Charged bytes across all senders sharing the node allowance.
    pub aggregate_retained_bytes: usize,
    /// Batches in flight.
    pub in_flight: usize,
    /// Durability fence reported by the most recent well-formed ACK. This may
    /// be below that specific batch's requirement, in which case the batch
    /// remains in flight and the mismatch is observable here.
    pub last_ack_durability: Option<ReplicationAckDurability>,
    /// The most recent matching ACK was below its batch's minimum and did not
    /// retire that batch.
    pub ack_durability_mismatch: bool,
    /// Completed anti-entropy reconcile rounds. The Phase 8 placement
    /// driver polls this to detect bootstrap completion.
    pub reconcile_rounds: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReconciliationStage {
    AwaitingSummary,
    Fetching,
    AwaitingDurability,
    RetryBackoff,
}
/// Actual local sender observations. Fetch counts cover the current chunk,
/// not a measured fraction of remote dataset convergence.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct SenderProgress {
    pub incarnation: u64,
    pub observed_at: Option<std::time::SystemTime>,
    pub last_successful_ack_at: Option<std::time::SystemTime>,
    pub last_successful_ack_durability: Option<ReplicationAckDurability>,
    pub last_successful_reconcile_at: Option<std::time::SystemTime>,
    pub reconciliation_stage: Option<ReconciliationStage>,
    pub expected_fetch_keys: Option<usize>,
    pub received_fetch_keys: Option<usize>,
    pub timing_enabled: bool,
    pub oldest_retained_operation_age: Option<Duration>,
}

/// Shared dependencies for a sender task.
///
/// The builder requires the real router, node HLC, and state report.
#[derive(bon::Builder)]
pub struct SenderDeps {
    pub router: FragmentRouter,
    /// Node-wide HLC. Reconcile frames are observed here only after the sender
    /// has authenticated their exact session/round/phase correlation.
    pub hlc: Arc<Mutex<HlcClock>>,
    /// Where the sender publishes its state.
    pub state_report: Arc<Mutex<SenderStateReport>>,
}

pub(crate) fn spawn_node_sender(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    options: SenderRuntimeOptions,
) -> Result<NodeSender, SenderOptionsError> {
    config.validate()?;
    options.validate()?;
    let (data_tx, data_rx) = mpsc::channel(config.buffer_capacity);
    let (control_tx, control_rx) = mpsc::channel(options.control_channel_capacity);
    let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
    let ingress = Arc::new(NodeSenderIngress {
        retention: SenderRetention {
            local: ByteBudget::new(config.retained_byte_capacity),
            aggregate: options.aggregate_retention.clone(),
        },
        reconciliation_result_budget: options.reconciliation_result_budget.clone(),
        ..NodeSenderIngress::default()
    });
    let (stopped_tx, stopped) = watch::channel(false);
    let (drain_completed_tx, drain_completed) = watch::channel(0u64);
    let configured_ack_durability = config.required_ack_durability;
    let observation_timing = options.observation_timing.clone();
    let task_ingress = ingress.clone();
    let inbox = SenderInbox {
        data_rx,
        control_rx,
        signals_rx,
        delivered_overflow_generation: 0,
        delivered_reconcile_generation: 0,
        delivered_drain_generation: 0,
        delivered_peer_generation: 0,
        ingress: ingress.clone(),
    };
    let task_exit = NodeSenderTaskExit(stopped_tx, task_ingress.clone());
    let task = tokio::spawn(async move {
        let _exit = task_exit;
        sender_task(
            fragment,
            peer_id,
            config,
            deps,
            options,
            inbox,
            NodeSenderDrainRuntime {
                completed: drain_completed_tx,
                ingress: task_ingress,
            },
        )
        .await;
    });
    Ok(NodeSender {
        observation_timing,
        data_tx,
        control_tx,
        signals_tx,
        ingress,
        abort: task.abort_handle(),
        stopped,
        drain_completed,
        configured_ack_durability,
        stop_lock: Arc::new(Mutex::new(())),
    })
}

fn proto_op(op: &crate::actor::ReplicatedOp) -> ProtoReplicateOp {
    ProtoReplicateOp {
        key: op.key.clone(),
        value: op.value.value.clone(),
        timestamp_ms: op.value.timestamp_ms,
        logical_counter: op.value.logical_counter,
        node_id: op.value.node_id.clone(),
        seq: op.seq,
        boot_id: op.boot_id,
        expires_at_ms: op.value.expires_at_ms.unwrap_or(0),
        is_tombstone: op.value.is_tombstone,
    }
}

/// Exact Prost length of [`proto_op`] without cloning its owned payload.
fn proto_op_encoded_len(op: &crate::actor::ReplicatedOp) -> usize {
    bytes_field_len(1, op.key.len())
        .saturating_add(bytes_field_len(2, op.value.value.len()))
        .saturating_add(varint_field_len(3, op.value.timestamp_ms))
        .saturating_add(bytes_field_len(4, op.value.node_id.len()))
        .saturating_add(varint_field_len(5, op.seq))
        .saturating_add(varint_field_len(6, op.boot_id))
        .saturating_add(varint_field_len(7, op.value.expires_at_ms.unwrap_or(0)))
        .saturating_add(varint_field_len(8, u64::from(op.value.is_tombstone)))
        .saturating_add(varint_field_len(9, op.value.logical_counter))
}

fn take_streaming_batch(
    buffer: &mut VecDeque<RetainedOp>,
    max_items: usize,
    session_id: u64,
    batch_seq: u64,
    fragment: FragmentId,
    required_ack_level: i32,
) -> Result<(Vec<RetainedOp>, Vec<ProtoReplicateOp>), ()> {
    let mut ops = Vec::new();
    let mut batch = ReplicateBatch {
        session_id,
        batch_seq,
        fragment: fragment.0,
        ops: Vec::new(),
        required_ack_level,
    };
    let mut sizing = RepeatedFieldSizer::new(9, 4, batch.encoded_len());
    while ops.len() < max_items.min(MAX_WIRE_BATCH_ITEMS) {
        let Some(op) = buffer.pop_front() else { break };
        let item_len = proto_op_encoded_len(&op);
        let encoded_len = sizing.encoded_len_with(item_len);
        if encoded_len > MAX_FRAME_BYTES {
            buffer.push_front(op);
            if ops.is_empty() {
                return Err(());
            }
            break;
        }
        if encoded_len > ANTI_ENTROPY_PAGE_BYTES && !ops.is_empty() {
            buffer.push_front(op);
            break;
        }
        let encoded = proto_op(&op);
        debug_assert_eq!(encoded.encoded_len(), item_len);
        sizing.push(item_len);
        batch.ops.push(encoded);
        ops.push(op);
        if encoded_len > ANTI_ENTROPY_PAGE_BYTES {
            break;
        }
    }
    if ops.is_empty() {
        Err(())
    } else {
        Ok((ops, batch.ops))
    }
}

async fn send_envelope(
    peer: &SenderPeer,
    tracked_deadline: Option<Instant>,
    msg: EnvelopeMsg,
) -> bool {
    let envelope = Envelope { msg: Some(msg) };
    let encoded_len = match check_envelope_budget(&envelope, MAX_FRAME_BYTES) {
        Ok(encoded_len) => encoded_len,
        Err(error) => {
            tracing::warn!(%error, "rejecting oversized or invalid outbound replication frame");
            return false;
        }
    };
    let now = Instant::now();
    let queue_deadline = now.checked_add(peer.queue_admission_timeout).unwrap_or(now);
    let admission_deadline = tracked_deadline
        .map(|deadline| deadline.min(queue_deadline))
        .unwrap_or(queue_deadline);
    if Instant::now() >= admission_deadline {
        peer.report_failure();
        return false;
    }
    match &peer.peer.transport {
        SenderTransport::NodeBudgeted(outbound) => {
            let charged_len = encoded_len;
            let frame_permit =
                match tokio::time::timeout_at(admission_deadline, outbound.tx.reserve()).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) | Err(_) => {
                        peer.report_failure();
                        return false;
                    }
                };
            let producer_byte_permit = match tokio::time::timeout_at(
                admission_deadline,
                outbound
                    .producer_byte_budget
                    .clone()
                    .acquire_many_owned(charged_len as u32),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) | Err(_) => {
                    peer.report_failure();
                    return false;
                }
            };
            let byte_permit = match tokio::time::timeout_at(
                admission_deadline,
                outbound
                    .byte_budget
                    .clone()
                    .acquire_many_owned(charged_len as u32),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) | Err(_) => {
                    peer.report_failure();
                    return false;
                }
            };
            let global_permits = match outbound
                .acquire_global_producer(charged_len as u32, admission_deadline)
                .await
            {
                Ok(permits) => permits,
                Err(()) => {
                    peer.report_failure();
                    return false;
                }
            };
            if Instant::now() >= admission_deadline {
                peer.report_failure();
                return false;
            }
            let buf = match encode_envelope(&envelope) {
                Ok(buf) => buf,
                Err(error) => {
                    tracing::warn!(%error, "failed to encode preflighted outbound replication frame");
                    return false;
                }
            };
            frame_permit.send(
                BudgetedFrame::new_producer(buf, byte_permit, producer_byte_permit)
                    .with_global_permits(global_permits),
            );
            true
        }
        SenderTransport::Standalone { tx, .. } => {
            let frame_permit = match tokio::time::timeout_at(admission_deadline, tx.reserve()).await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) | Err(_) => {
                    peer.report_failure();
                    return false;
                }
            };
            if Instant::now() >= admission_deadline {
                peer.report_failure();
                return false;
            }
            let buf = match encode_envelope(&envelope) {
                Ok(buf) => buf,
                Err(error) => {
                    tracing::warn!(%error, "failed to encode preflighted outbound replication frame");
                    return false;
                }
            };
            frame_permit.send(buf);
            true
        }
    }
}

async fn sender_task(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    runtime: SenderRuntimeOptions,
    mut inbox: SenderInbox,
    node_drain: NodeSenderDrainRuntime,
) {
    let mut delivery = DeliveryState::Streaming;
    let mut connection: Option<ActiveConnection> = None;
    let retention = inbox.ingress.retention.clone();
    let mut buffer: VecDeque<RetainedOp> = VecDeque::new();
    // `interval()` ticks immediately once, which made the first batch race
    // sender startup instead of consistently observing `batch_interval`.
    let mut batch_timer =
        tokio::time::interval_at(deadline_after(config.batch_interval), config.batch_interval);
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consistency_timer = tokio::time::interval_at(
        deadline_after(config.consistency_interval),
        config.consistency_interval,
    );
    consistency_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut active_reconcile: Option<ReconcileRound> = None;
    let mut reconcile_credit_rx: Option<watch::Receiver<u64>> = None;
    let mut reconcile_ticket: Option<ReconcileTicket> = None;
    let mut next_round_id = 0_u64;
    let mut next_reset_epoch = 0_u64;
    let mut reconcile_rounds: u64 = 0;
    let mut last_ack_durability = None;
    let mut ack_durability_mismatch = false;
    let mut last_successful_ack_at = None;
    let mut last_successful_ack_durability = None;
    let mut last_successful_reconcile_at = None;
    let observation_incarnation = crate::node::observability::fresh_incarnation();
    let mut retry_not_before: Option<Instant> = None;
    let mut session_retry: Option<Instant> = None;
    let mut reconcile_retry: Option<(Instant, ReconcilePurpose)> = None;
    // Fresh routine reconciliation briefly drains existing ordinary batches
    // before it starts. A failed round clears this flag so buffered ordinary
    // replication takes precedence over an anti-entropy retry.
    let mut pause_new_streaming = false;
    // A fetched winner may have entered a configured local store's write
    // buffer before a flush failed. Carry that obligation across retry
    // rounds; ordinary no-fetch reconciliation on a storeless sender must not
    // acquire a new local durability requirement.
    let mut local_reconcile_flush_pending = false;
    let mut publish_data_completion: Option<Arc<NodeSenderIngress>> = None;
    let mut active_ingress_overflow: Option<(Arc<NodeSenderIngress>, u64)> = None;
    let mut drain_state = SenderDrainState::default();

    loop {
        if connection.is_none() {
            reconcile_credit_rx = None;
            reconcile_ticket = None;
        }
        if let Some(drain) = drain_state.pending_preparation() {
            let ingress = &node_drain.ingress;
            if ingress.pending_data.load(Ordering::Acquire) == 0 {
                // The cache is the authoritative source for Drain. Once every
                // pre-fence data item has left the ingress lane, a fresh
                // session makes delayed ACK/NACK/control frames harmless and
                // the correlated reconciliation subsumes buffered deltas and
                // every coalesced overflow reset.
                let issued_overflow = ingress.issued_overflow_generation.load(Ordering::Acquire);
                inbox.settle_overflow_through(issued_overflow);
                active_ingress_overflow = None;
                buffer.clear();
                active_reconcile = None;
                reconcile_retry = None;
                reconcile_ticket = None;
                retry_not_before = None;
                pause_new_streaming = false;
                delivery = DeliveryState::Streaming;

                let restarted = match connection.as_mut() {
                    Some(conn) => {
                        conn.in_flight.clear();
                        conn.session_id = rand_session_id();
                        conn.next_batch_seq = 0;
                        send_session_start(fragment, conn).await
                    }
                    None => true,
                };
                if restarted || disconnect_transport(&mut connection, delivery, &mut buffer) {
                    session_retry = None;
                } else {
                    session_retry = Some(deadline_after(NACK_RETRY_DELAY));
                }
                drain_state.mark_prepared(drain.generation);
            }
        }
        // A shutdown drain is stronger than ordinary queue emptiness: run a
        // fresh stable full reconciliation after the producer fence so an op
        // recovered from bounded-ingress overflow or a previously closed
        // sender is covered by the same correlated durability barrier.
        if let Some(purpose) = drain_state.prepared_purpose() {
            if matches!(delivery, DeliveryState::Streaming) && active_reconcile.is_none() {
                let scheduled = matches!(reconcile_retry, Some((_, current)) if current == purpose);
                if !scheduled {
                    reconcile_retry = Some((Instant::now(), purpose));
                }
                pause_new_streaming = true;
            }
        }
        // Publish state for health checks and tests.
        {
            let state = match delivery {
                DeliveryState::Reset { .. } => "buffer_reset",
                DeliveryState::Streaming
                    if connection
                        .as_ref()
                        .is_some_and(|connection| connection.session_ready) =>
                {
                    "connected"
                }
                DeliveryState::Streaming => "disconnected",
            };
            *deps.state_report.lock().await = SenderStateReport {
                progress: {
                    let timing_enabled = runtime.observation_timing.load(Ordering::Acquire);
                    let first = connection
                        .as_ref()
                        .and_then(|connection| connection.in_flight.front())
                        .and_then(|batch| batch.ops.first())
                        .or_else(|| buffer.front());
                    let pending_ingress = inbox.ingress.pending_data.load(Ordering::Acquire) != 0;
                    let oldest_retained_operation_age = if !timing_enabled {
                        None
                    } else {
                        match first {
                            Some(op) => op.enqueued_at.map(|at| at.elapsed()),
                            None if pending_ingress => None,
                            None => Some(Duration::ZERO),
                        }
                    };
                    let (reconciliation_stage, expected_fetch_keys, received_fetch_keys) =
                        match active_reconcile.as_ref().map(|round| &round.phase) {
                            Some(ReconcilePhase::Response(_)) => {
                                (Some(ReconciliationStage::AwaitingSummary), None, None)
                            }
                            Some(ReconcilePhase::Data {
                                expected_keys,
                                received_keys,
                                ..
                            }) => (
                                Some(ReconciliationStage::Fetching),
                                Some(expected_keys.len()),
                                Some(received_keys.len()),
                            ),
                            Some(ReconcilePhase::Barrier { .. }) => {
                                (Some(ReconciliationStage::AwaitingDurability), None, None)
                            }
                            None if reconcile_retry.is_some() => {
                                (Some(ReconciliationStage::RetryBackoff), None, None)
                            }
                            None => (None, None, None),
                        };
                    SenderProgress {
                        incarnation: observation_incarnation,
                        observed_at: Some(std::time::SystemTime::now()),
                        last_successful_ack_at,
                        last_successful_ack_durability,
                        last_successful_reconcile_at,
                        reconciliation_stage,
                        expected_fetch_keys,
                        received_fetch_keys,
                        timing_enabled,
                        oldest_retained_operation_age,
                    }
                },
                state: state.to_string(),
                session_id: connection.as_ref().map_or(0, |c| c.session_id),
                buffered: buffer.len(),
                retained_bytes: retention.local.used(),
                aggregate_retained_bytes: retention.aggregate.used(),
                in_flight: connection.as_ref().map_or(0, |c| c.in_flight.len()),
                last_ack_durability,
                ack_durability_mismatch,
                reconcile_rounds,
            };
        }
        // Keep a dequeued data item visible to placement drain until the
        // resulting buffer/reset state above has been published.
        if let Some(ingress) = publish_data_completion.take() {
            ingress.pending_data.fetch_sub(1, Ordering::AcqRel);
            if drain_state.pending.map(|drain| drain.generation) != drain_state.prepared_generation
                && ingress.pending_data.load(Ordering::Acquire) == 0
            {
                continue;
            }
        }
        let sender_wakeup = active_reconcile
            .as_ref()
            .map(|round| round.phase_deadline)
            .into_iter()
            .chain(
                reconcile_retry
                    .filter(|_| {
                        connection
                            .as_ref()
                            .is_some_and(|connection| connection.session_ready)
                    })
                    .map(|(deadline, _)| deadline),
            )
            .chain(session_retry.filter(|_| connection.is_some()))
            .chain(connection.iter().flat_map(|connection| {
                connection
                    .in_flight
                    .iter()
                    .filter_map(|batch| match batch.kind {
                        InflightKind::Streaming { ack_deadline } => Some(ack_deadline),
                        InflightKind::ReconcileBarrier => None,
                    })
            }))
            .min();
        tokio::select! {
            input = inbox.recv() => {
                let mut overflow = None;
                let mut retained_enqueue = None;
                let mut control_lease = None;
                let mut apply_retention = None;
                let msg = match input {
                    SenderInput::Control(control) => {
                        control_lease = Some(control.lease);
                        apply_retention = control.apply_retention;
                        Some(control.msg)
                    },
                    SenderInput::Data(op, ingress) => {
                        publish_data_completion = Some(ingress);
                        retained_enqueue = Some(op);
                        None
                    }
                    SenderInput::Overflow(generation, ingress) => {
                        overflow = Some((ingress, generation));
                        None
                    }
                    SenderInput::PeerChanged(Some(peer)) => {
                        Some(SenderMsg::PeerConnected { peer })
                    }
                    SenderInput::PeerChanged(None) => connection.as_ref().map(|current| {
                        SenderMsg::PeerDisconnected {
                            connection_id: current.peer_tx.peer.connection_id(),
                        }
                    }),
                    SenderInput::ReconcileNow => Some(SenderMsg::ReconcileNow),
                    SenderInput::Drain(drain) => {
                        drain_state.request(drain);
                        None
                    }
                    SenderInput::Closed => break,
                };
                let data_overflow = retained_enqueue.is_some() && buffer.len() >= config.buffer_capacity;
                if data_overflow {
                    crate::node::observability::add_saturating(&inbox.ingress.shed_operations, 1);
                    crate::node::observability::add_saturating(&inbox.ingress.reset_requests, 1);
                    inbox.ingress.recovery_active.store(true, Ordering::Release);
                }
                if overflow.is_some() || data_overflow {
                    if let Some(overflow) = overflow {
                        active_ingress_overflow = Some(overflow);
                    }
                    if let DeliveryState::Reset { epoch } = &mut delivery {
                        // The current round is still useful as a durability
                        // fence, but its epoch can no longer complete reset.
                        // Finish it, then take a fresh snapshot without
                        // needlessly rotating the healthy transport session.
                        buffer.clear();
                        *epoch = next_nonzero(&mut next_reset_epoch);
                        continue;
                    }
                    // Every shed command names an already-applied mutation.
                    // Discard queued deltas and recover the authoritative
                    // winner set through a newly correlated reset round.
                    buffer.clear();
                    let epoch = next_nonzero(&mut next_reset_epoch);
                    delivery = DeliveryState::Reset { epoch };
                    active_reconcile = None;
                    reconcile_retry = None;
                    pause_new_streaming = false;

                    // Invalidate old streaming batches before taking the
                    // reset snapshot. Delayed ACKs and reconcile responses
                    // from that session are then harmless.
                    let session_started = if let Some(conn) = connection.as_mut() {
                        conn.in_flight.clear();
                        conn.session_id = rand_session_id();
                        conn.next_batch_seq = 0;
                        send_session_start(fragment, conn).await
                    } else {
                        false
                    };
                    if session_started {
                        let conn = connection.as_ref().expect("checked above");
                        match begin_reconcile(
                            fragment,
                            ReconcilePurpose::Reset { epoch },
                            conn,
                            &mut reconcile_ticket,
                            &mut next_round_id,
                            config.reconcile_timeout,
                        )
                        .await
                        {
                            Ok(Some(round)) => active_reconcile = Some(round),
                            Ok(None) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    ReconcilePurpose::Reset { epoch },
                                ));
                            }
                            Err(()) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    ReconcilePurpose::Reset { epoch },
                                ));
                                disconnect_transport(
                                    &mut connection,
                                    delivery,
                                    &mut buffer,
                                );
                            }
                        }
                    } else if connection.is_some() {
                        reconcile_retry = Some((
                            Instant::now() + NACK_RETRY_DELAY,
                            ReconcilePurpose::Reset { epoch },
                        ));
                        if !disconnect_transport(
                            &mut connection,
                            delivery,
                            &mut buffer,
                        ) {
                            session_retry = Some(Instant::now() + NACK_RETRY_DELAY);
                        }
                    }
                    continue;
                }
                if let Some(op) = retained_enqueue { buffer.push_back(op); continue; }
                // Keep protocol payload permits until selected local work completes.
                let mut _control_lease = control_lease;
                let Some(msg) = msg else { continue };
                match msg {
                    SenderMsg::Enqueue(_) => unreachable!("mutations use the bounded data lane"),
                    SenderMsg::PeerConnected { peer } => {
                        if connection
                            .as_ref()
                            .is_some_and(|current| current.peer_tx.same_connection(peer.id()))
                        {
                            // Dynamic placement polls and duplicate
                            // notifications must not rotate live sessions.
                            continue;
                        }
                        reconcile_ticket = None;
                        if let Some(round) = active_reconcile.take() {
                            reconcile_retry = Some((Instant::now(), round.purpose));
                        }
                        if let Some(mut old) = connection.take() {
                            if matches!(delivery, DeliveryState::Streaming) {
                                if !old.in_flight.is_empty() {
                                    pause_new_streaming = false;
                                }
                                requeue_in_flight(&mut old.in_flight, &mut buffer);
                            }
                        }
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                        reconcile_credit_rx = peer.node_transport()
                            .map(BudgetedPeerTx::reconcile_change_receiver);
                        let mut conn = ActiveConnection {
                            peer_tx: SenderPeer::new(peer, &runtime),
                            session_id: rand_session_id(),
                            session_ready: false,
                            next_batch_seq: 0,
                            in_flight: VecDeque::new(),
                        };
                        if !send_session_start(fragment, &mut conn).await {
                            if conn.peer_tx.must_disconnect_after_failure() {
                                continue;
                            }
                            let retry_at = Instant::now() + NACK_RETRY_DELAY;
                            session_retry = Some(retry_at);
                            if let DeliveryState::Reset { epoch } = delivery {
                                reconcile_retry =
                                    Some((retry_at, ReconcilePurpose::Reset { epoch }));
                            }
                            connection = Some(conn);
                            continue;
                        }
                        session_retry = None;
                        let retry_due = reconcile_retry
                            .map(|(deadline, _)| Instant::now() >= deadline)
                            .unwrap_or(true);
                        let reconcile_purpose = match delivery {
                            DeliveryState::Reset { epoch } if retry_due => {
                                Some(ReconcilePurpose::Reset { epoch })
                            }
                            DeliveryState::Reset { .. } => None,
                            DeliveryState::Streaming
                                if retry_due
                                    && buffer.is_empty()
                                    && inbox.ingress.pending_data.load(Ordering::Acquire) == 0 =>
                            {
                                reconcile_retry.map(|(_, purpose)| purpose)
                            }
                            DeliveryState::Streaming => None,
                        };
                        if let Some(purpose) = reconcile_purpose {
                            match begin_reconcile(
                                fragment,
                                purpose,
                                &conn,
                                &mut reconcile_ticket,
                                &mut next_round_id,
                                config.reconcile_timeout,
                            )
                            .await
                            {
                                Ok(Some(round)) => {
                                    active_reconcile = Some(round);
                                    reconcile_retry = None;
                                    pause_new_streaming = false;
                                }
                                Ok(None) => {
                                    reconcile_retry = Some((
                                        deadline_after(NACK_RETRY_DELAY),
                                        purpose,
                                    ));
                                    pause_new_streaming = true;
                                }
                                Err(()) => {
                                    reconcile_retry = Some((
                                        deadline_after(NACK_RETRY_DELAY),
                                        purpose,
                                    ));
                                    pause_new_streaming = false;
                                    if !conn.peer_tx.must_disconnect_after_failure() {
                                        connection = Some(conn);
                                    }
                                    continue;
                                }
                            }
                        }
                        connection = Some(conn);
                    }
                    SenderMsg::PeerDisconnected { connection_id } => {
                        if connection
                            .as_ref()
                            .is_none_or(|current| !current.peer_tx.same_connection(&connection_id))
                        {
                            continue;
                        }
                        reconcile_ticket = None;
                        if let Some(round) = active_reconcile.take() {
                            reconcile_retry = Some((Instant::now(), round.purpose));
                        }
                        if let Some(mut conn) = connection.take() {
                            if matches!(delivery, DeliveryState::Streaming) {
                                if !conn.in_flight.is_empty() {
                                    pause_new_streaming = false;
                                }
                                requeue_in_flight(&mut conn.in_flight, &mut buffer);
                            }
                        }
                        session_retry = None;
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                    }
                    SenderMsg::Ack {
                        session_id: ack_session,
                        batch_seq,
                        durability,
                    } => {
                        let mut completed_round = None;
                        if let Some(conn) = connection.as_mut() {
                            if ack_session != conn.session_id {
                                continue;
                            }
                            match acknowledge_in_flight(conn, batch_seq, durability) {
                                InflightAckResult::Missing => {}
                                InflightAckResult::BelowRequired { required } => {
                                    last_ack_durability = Some(durability);
                                    ack_durability_mismatch = true;
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        batch_seq,
                                        ?durability,
                                        ?required,
                                        "replication ack did not meet the batch durability fence"
                                    );
                                }
                                InflightAckResult::Accepted { kind, ops } => {
                                    last_successful_ack_at = Some(std::time::SystemTime::now());
                                    last_successful_ack_durability = Some(durability);
                                    if let Some(tracker) = runtime.write_concerns.as_ref() {
                                        for op in &ops { tracker.acknowledge_at(&peer_id, fragment, std::slice::from_ref(&op.op), durability.into()); }
                                    }
                                    last_ack_durability = Some(durability);
                                    ack_durability_mismatch = false;
                                    let is_barrier = kind == InflightKind::ReconcileBarrier
                                        && active_reconcile.as_ref().is_some_and(|round| {
                                            round.session_id == ack_session
                                                && matches!(
                                                    round.phase,
                                                    ReconcilePhase::Barrier { batch_seq: seq }
                                                        if seq == batch_seq
                                                )
                                        });
                                    if is_barrier {
                                        completed_round = active_reconcile.take();
                                    }
                                }
                            }
                        }
                        if let Some(mut round) = completed_round {
                            reconcile_retry = None;
                            pause_new_streaming = false;
                            match round.purpose {
                                ReconcilePurpose::Routine | ReconcilePurpose::Drain { .. } => {
                                    let snapshot_is_current = match (
                                        round.snapshot_revision,
                                        fragment_revision(fragment, &deps.router, runtime.local_work_timeout).await,
                                    ) {
                                        (Some(expected), Ok(current)) => expected == current,
                                        _ => false,
                                    };
                                    if snapshot_is_current {
                                        reconcile_rounds = reconcile_rounds.saturating_add(1);
                                        last_successful_reconcile_at = Some(std::time::SystemTime::now());
                                        if let ReconcilePurpose::Drain { generation, .. } =
                                            round.purpose
                                        {
                                            // The fresh session and stable
                                            // barrier supersede every fenced
                                            // buffered delta. No producer can
                                            // enter this exact sender after
                                            // request_drain closed ingress.
                                            buffer.clear();
                                            drain_state.complete();
                                            let issued = node_drain
                                                .ingress
                                                .issued_overflow_generation
                                                .load(Ordering::Acquire);
                                            node_drain
                                                .ingress
                                                .settled_overflow_generation
                                                .store(issued, Ordering::Release);
                                            node_drain.ingress.recovery_active.store(false, Ordering::Release);
                                            node_drain.completed.send_modify(|current| {
                                                *current = (*current).max(generation);
                                            });
                                        }
                                    } else if let Some(conn) = connection.as_ref() {
                                        // Fetched winners or a concurrent
                                        // local mutation invalidate the
                                        // snapshot that chose the repairs.
                                        // Require a fresh stable round before
                                        // reporting routine/bootstrap
                                        // completion.
                                        drop(round.connection_permit.take());
                                        match begin_reconcile(
                                            fragment,
                                            round.purpose,
                                            conn,
                                            &mut reconcile_ticket,
                                            &mut next_round_id,
                                            config.reconcile_timeout,
                                        )
                                        .await
                                        {
                                            Ok(Some(next_round)) => {
                                                active_reconcile = Some(next_round)
                                            }
                                            Ok(None) => {
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    round.purpose,
                                                ));
                                            }
                                            Err(()) => {
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    round.purpose,
                                                ));
                                                disconnect_transport(
                                                    &mut connection,
                                                    delivery,
                                                    &mut buffer,
                                                );
                                            }
                                        }
                                    } else {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            round.purpose,
                                        ));
                                    }
                                }
                                ReconcilePurpose::Reset {
                                    epoch: completed_epoch,
                                } => {
                                    if !buffer.is_empty() {
                                        let deltas: Vec<_> = buffer.drain(..).collect();
                                        let send_result = match connection.as_ref() {
                                            Some(conn) => {
                                                send_repair_ops(fragment, conn, &deltas).await
                                            }
                                            None => Err(ReconcileError::Transport),
                                        };
                                        if send_result.is_err() {
                                            for op in deltas.into_iter().rev() {
                                                buffer.push_front(op);
                                            }
                                            disconnect_transport(
                                                &mut connection,
                                                delivery,
                                                &mut buffer,
                                            );
                                            reconcile_retry = Some((
                                                Instant::now() + NACK_RETRY_DELAY,
                                                round.purpose,
                                            ));
                                            continue;
                                        }
                                        // These deltas are now fenced, but a
                                        // bare revision cannot distinguish
                                        // them from unrelated concurrent
                                        // mutations. Require one fresh
                                        // no-change snapshot before clearing
                                        // reset.
                                        round.snapshot_revision = None;
                                        let Some(conn) = connection.as_mut() else {
                                            for op in deltas.into_iter().rev() {
                                                buffer.push_front(op);
                                            }
                                            continue;
                                        };
                                        let required_ack_durability = round
                                            .purpose
                                            .required_ack_durability(
                                                config.required_ack_durability,
                                            );
                                        match send_reconcile_barrier(
                                            fragment,
                                            required_ack_durability,
                                            conn,
                                        )
                                        .await
                                        {
                                            Ok(batch_seq) => {
                                                round.phase =
                                                    ReconcilePhase::Barrier { batch_seq };
                                                round.phase_deadline =
                                                    deadline_after(config.reconcile_timeout);
                                                active_reconcile = Some(round);
                                            }
                                            Err(()) => {
                                                for op in deltas.into_iter().rev() {
                                                    buffer.push_front(op);
                                                }
                                                disconnect_transport(
                                                    &mut connection,
                                                    delivery,
                                                    &mut buffer,
                                                );
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    round.purpose,
                                                ));
                                            }
                                        }
                                        continue;
                                    }

                                    let snapshot_is_current = match (
                                        round.snapshot_revision,
                                        fragment_revision(fragment, &deps.router, runtime.local_work_timeout).await,
                                    ) {
                                        (Some(expected), Ok(current)) => expected == current,
                                        _ => false,
                                    };
                                    match delivery {
                                        DeliveryState::Reset { epoch }
                                            if epoch == completed_epoch && snapshot_is_current =>
                                        {
                                            delivery = DeliveryState::Streaming;
                                            inbox.ingress.recovery_active.store(false, Ordering::Release);
                                            reconcile_rounds = reconcile_rounds.saturating_add(1);
                                            last_successful_reconcile_at = Some(std::time::SystemTime::now());
                                            if let Some((ingress, generation)) =
                                                active_ingress_overflow.take()
                                            {
                                                ingress
                                                    .settled_overflow_generation
                                                    .store(generation, Ordering::Release);
                                            }
                                        }
                                        DeliveryState::Reset { epoch } => {
                                            if let Some(conn) = connection.as_ref() {
                                                drop(round.connection_permit.take());
                                                match begin_reconcile(
                                                    fragment,
                                                    ReconcilePurpose::Reset { epoch },
                                                    conn,
                                                    &mut reconcile_ticket,
                                                    &mut next_round_id,
                                                    config.reconcile_timeout,
                                                )
                                                .await
                                                {
                                                    Ok(Some(round)) => {
                                                        active_reconcile = Some(round)
                                                    }
                                                    Ok(None) => {
                                                        reconcile_retry = Some((
                                                            Instant::now() + NACK_RETRY_DELAY,
                                                            ReconcilePurpose::Reset { epoch },
                                                        ));
                                                    }
                                                    Err(()) => {
                                                        reconcile_retry = Some((
                                                            Instant::now() + NACK_RETRY_DELAY,
                                                            ReconcilePurpose::Reset { epoch },
                                                        ));
                                                        disconnect_transport(
                                                            &mut connection,
                                                            delivery,
                                                            &mut buffer,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        DeliveryState::Streaming => {}
                                    }
                                }
                            }
                        }
                    }
                    SenderMsg::Nack {
                        session_id: nack_session,
                        batch_seq,
                        error,
                    } => {
                        let mut restart_session = false;
                        if let Some(conn) = connection.as_mut() {
                            if nack_session != conn.session_id {
                                continue;
                            }
                            if let Some(pos) = conn
                                .in_flight
                                .iter()
                                .position(|b| b.batch_seq == batch_seq)
                            {
                                if let Some(batch) = conn.in_flight.remove(pos) {
                                    let is_barrier = active_reconcile.as_ref().is_some_and(|round| {
                                        round.session_id == nack_session
                                            && matches!(
                                                round.phase,
                                                ReconcilePhase::Barrier { batch_seq: seq }
                                                    if seq == batch_seq
                                            )
                                    });
                                    if is_barrier {
                                        let purpose = active_reconcile
                                            .take()
                                            .map(|round| round.purpose)
                                            .unwrap_or(ReconcilePurpose::Routine);
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            purpose,
                                        ));
                                        // A reconciliation repair failure
                                        // poisons the receiver's current
                                        // session. Preserve unrelated
                                        // streaming batches, then mint a new
                                        // session before retrying the round.
                                        if matches!(delivery, DeliveryState::Streaming) {
                                            requeue_in_flight(
                                                &mut conn.in_flight,
                                                &mut buffer,
                                            );
                                        } else {
                                            conn.in_flight.clear();
                                        }
                                        conn.session_id = rand_session_id();
                                        conn.next_batch_seq = 0;
                                        restart_session = true;
                                    }
                                    for op in batch.ops.into_iter().rev() {
                                        buffer.push_front(op);
                                    }
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        batch_seq,
                                        %error,
                                        "replication batch rejected; queued for retry"
                                    );
                                    if !is_barrier {
                                        pause_new_streaming = false;
                                        retry_not_before = Some(Instant::now() + NACK_RETRY_DELAY);
                                    }
                                }
                            }
                        }
                        if restart_session {
                            let restarted = match connection.as_mut() {
                                Some(conn) => send_session_start(fragment, conn).await,
                                None => false,
                            };
                            if !restarted
                                && !disconnect_transport(
                                    &mut connection,
                                    delivery,
                                    &mut buffer,
                                )
                            {
                                session_retry = Some(Instant::now() + NACK_RETRY_DELAY);
                            }
                        }
                    }
                    SenderMsg::ReconcileNow => {
                        if drain_state.routine_reconcile_allowed()
                            && matches!(delivery, DeliveryState::Streaming)
                            && active_reconcile.is_none()
                        {
                            if let Some(conn) = connection.as_ref() {
                                if !conn.session_ready
                                    || !conn.in_flight.is_empty()
                                    || !buffer.is_empty()
                                    || inbox.ingress.pending_data.load(Ordering::Acquire) != 0
                                {
                                    reconcile_retry = Some((
                                        deadline_after(NACK_RETRY_DELAY),
                                        ReconcilePurpose::Routine,
                                    ));
                                    // Coalesced peer changes may be selected
                                    // before this request. Buffered replay or
                                    // admitted ingress must stream and reach
                                    // its ordinary ACK before routine repair.
                                    pause_new_streaming = buffer.is_empty()
                                        && inbox.ingress.pending_data.load(Ordering::Acquire) == 0;
                                    continue;
                                }
                                pause_new_streaming = false;
                                match begin_reconcile(
                                    fragment,
                                    ReconcilePurpose::Routine,
                                    conn,
                                    &mut reconcile_ticket,
                                    &mut next_round_id,
                                    config.reconcile_timeout,
                                )
                                .await
                                {
                                    Ok(Some(round)) => {
                                        active_reconcile = Some(round);
                                        reconcile_retry = None;
                                    }
                                    Ok(None) => {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            ReconcilePurpose::Routine,
                                        ));
                                        pause_new_streaming = true;
                                    }
                                    Err(()) => {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            ReconcilePurpose::Routine,
                                        ));
                                        pause_new_streaming = false;
                                        disconnect_transport(&mut connection, delivery, &mut buffer);
                                    }
                                }
                            } else {
                                // Preserve on-demand bootstrap across a
                                // connection race instead of silently dropping
                                // the request.
                                reconcile_retry =
                                    Some((Instant::now(), ReconcilePurpose::Routine));
                                pause_new_streaming = buffer.is_empty()
                                    && inbox.ingress.pending_data.load(Ordering::Acquire) == 0;
                            }
                        }
                    }
                    SenderMsg::ReconcileResponse {
                        session_id,
                        round_id,
                        page_seq,
                        next_after_key,
                        snapshot_revision,
                        snapshot_time_ms,
                        done,
                        restart,
                        summaries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().is_some_and(|round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(
                                    &round.phase,
                                    ReconcilePhase::Response(summary)
                                        if summary.next_page_seq == page_seq
                                )
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        let structurally_valid = active_reconcile.as_ref().is_some_and(|round| {
                            let ReconcilePhase::Response(summary) = &round.phase else {
                                return false;
                            };
                            if restart {
                                return summaries.is_empty() && !done;
                            }
                            let snapshot_matches = match (
                                summary.remote_snapshot_revision,
                                summary.remote_snapshot_time_ms,
                            ) {
                                (None, None) => page_seq == 0 && snapshot_time_ms != 0,
                                (Some(_), Some(snapshot_time)) => {
                                    snapshot_time == snapshot_time_ms
                                }
                                _ => false,
                            };
                            snapshot_matches
                                && reconcile_summaries_are_well_formed(
                                    fragment,
                                    &deps.router,
                                    summary.next_page_seq != 0,
                                    &summary.after_key,
                                    &summaries,
                                    &next_after_key,
                                    done,
                                )
                        });
                        if !structurally_valid {
                            tracing::warn!(
                                peer = %peer_id,
                                fragment = fragment.0,
                                round_id,
                                page_seq,
                                "rejecting malformed reconcile summary page"
                            );
                            let _ = connection_valid.send(false);
                            continue;
                        }
                        if !restart {
                            if let Some(timestamp_ms) =
                                summaries.iter().map(|summary| actordb_protocol::RecordVersion::new(summary.timestamp_ms, summary.logical_counter)).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe_version(timestamp_ms) {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        %error,
                                        "rejecting correlated reconcile response timestamp"
                                    );
                                    let _ = connection_valid.send(false);
                                    continue;
                                }
                            }
                        }
                        let _ = connection_valid.send(true);
                        let Some(mut round) = active_reconcile.take() else { continue };
                        round.control_retention = _control_lease.take();
                        if restart {
                            reconcile_retry =
                                Some((Instant::now() + NACK_RETRY_DELAY, round.purpose));
                            continue;
                        }
                        let ReconcilePhase::Response(mut summary) = round.phase else {
                            continue;
                        };
                        if summary.remote_snapshot_revision.is_none() {
                            summary.remote_snapshot_revision = Some(snapshot_revision);
                            summary.remote_snapshot_time_ms = Some(snapshot_time_ms);
                        } else if summary.remote_snapshot_revision != Some(snapshot_revision) {
                            summary.remote_revision_stable = false;
                        }
                        summary.next_page_seq = summary.next_page_seq.saturating_add(1);
                        summary.after_key = next_after_key;
                        summary.summary_done = done;

                        let page_result = match connection.as_ref() {
                            Some(conn) => {
                                reconcile_summary_page(
                                    fragment,
                                    &deps.router,
                                    conn,
                                    round.round_id,
                                    &mut summary,
                                    &summaries,
                                    ReconcilePageInput { done, retention: apply_retention.clone() },
                                )
                                .await
                            }
                            None => Err(ReconcileError::Transport),
                        };
                        let mut chunks = match page_result {
                            Ok(chunks) => chunks,
                            Err(error) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                if error == ReconcileError::Transport {
                                    disconnect_transport(
                                        &mut connection,
                                        delivery,
                                        &mut buffer,
                                    );
                                }
                                continue;
                            }
                        };
                        if let Some(keys) = chunks.pop_front() {
                            summary.changed_any = true;
                            let chunk_seq = summary.next_fetch_chunk_seq;
                            summary.next_fetch_chunk_seq =
                                summary.next_fetch_chunk_seq.saturating_add(1);
                            let expected_keys = keys.iter().cloned().collect();
                            let sent = match connection.as_ref() {
                                Some(conn) => {
                                    send_fetch(fragment, conn, round.round_id, chunk_seq, keys)
                                        .await
                                }
                                None => Err(ReconcileError::Transport),
                            };
                            if let Err(error) = sent {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                if error == ReconcileError::Transport {
                                    disconnect_transport(
                                        &mut connection,
                                        delivery,
                                        &mut buffer,
                                    );
                                }
                                continue;
                            }
                            round.phase = ReconcilePhase::Data {
                                summary,
                                remaining_chunks: chunks,
                                next_chunk_seq: chunk_seq,
                                expected_keys,
                                received_keys: HashSet::new(),
                                next_page_seq: 0,
                            };
                            round.phase_deadline = deadline_after(config.reconcile_timeout);
                            active_reconcile = Some(round);
                            continue;
                        }
                        if !summary.summary_done {
                            let sent = match connection.as_ref() {
                                Some(conn) => {
                                    send_summary_request(fragment, conn, round.round_id, &summary)
                                        .await
                                }
                                None => Err(ReconcileError::Transport),
                            };
                            if sent.is_ok() {
                                round.phase = ReconcilePhase::Response(summary);
                                round.phase_deadline = deadline_after(config.reconcile_timeout);
                                active_reconcile = Some(round);
                            } else {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                            continue;
                        }
                        round.snapshot_revision = if summary.changed_any
                            || !summary.remote_revision_stable
                            || !summary.local.revision_stable
                        {
                            None
                        } else {
                            summary.local.revision
                        };
                        // Reinitialize the moved phase before the barrier
                        // helper borrows the complete round.
                        round.phase = ReconcilePhase::Response(summary);
                        let barrier = match connection.as_mut() {
                            Some(conn) => {
                                enter_reconcile_barrier(
                                    fragment,
                                    &deps.router,
                                    &config,
                                    conn,
                                    &mut round,
                                    &mut local_reconcile_flush_pending,
                                )
                                .await
                            }
                            None => Err(ReconcileError::Transport),
                        };
                        match barrier {
                            Ok(()) => active_reconcile = Some(round),
                            Err(error) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                if error == ReconcileError::Transport {
                                    disconnect_transport(
                                        &mut connection,
                                        delivery,
                                        &mut buffer,
                                    );
                                }
                            }
                        }
                    }
                    SenderMsg::ReconcileData {
                        session_id,
                        round_id,
                        chunk_seq,
                        page_seq,
                        done,
                        entries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().is_some_and(|round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(
                                    &round.phase,
                                    ReconcilePhase::Data {
                                        next_chunk_seq,
                                        next_page_seq,
                                        ..
                                    } if *next_chunk_seq == chunk_seq && *next_page_seq == page_seq
                                )
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        let (expected_keys, received_keys) = match active_reconcile
                            .as_ref()
                            .map(|round| &round.phase)
                        {
                            Some(ReconcilePhase::Data {
                                expected_keys,
                                received_keys,
                                ..
                            }) => (expected_keys, received_keys),
                            _ => {
                                let _ = connection_valid.send(true);
                                continue;
                            }
                        };
                        let well_formed = (!entries.is_empty() || done)
                            && reconcile_entries_are_well_formed(
                            fragment,
                            &deps.router,
                            &entries,
                            expected_keys,
                            received_keys,
                        );
                        if !well_formed {
                            tracing::warn!(
                                peer = %peer_id,
                                fragment = fragment.0,
                                round_id,
                                chunk_seq,
                                page_seq,
                                "rejecting malformed reconcile data page"
                            );
                            let _ = connection_valid.send(false);
                            continue;
                        }
                        if well_formed {
                            if let Some(timestamp_ms) =
                                entries.iter().map(|entry| actordb_protocol::RecordVersion::new(entry.timestamp_ms, entry.logical_counter)).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe_version(timestamp_ms) {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        %error,
                                        "rejecting correlated reconcile data timestamp"
                                    );
                                    let _ = connection_valid.send(false);
                                    continue;
                                }
                            }
                        }
                        let _ = connection_valid.send(true);
                        let Some(mut round) = active_reconcile.take() else { continue };
                        let ReconcilePhase::Data {
                            mut summary,
                            mut remaining_chunks,
                            next_chunk_seq: current_chunk_seq,
                            expected_keys,
                            mut received_keys,
                            next_page_seq,
                        } = round.phase
                        else {
                            continue;
                        };
                        if round
                            .purpose
                            .required_ack_durability(config.required_ack_durability)
                            >= ReplicationAckDurability::StoreCommitted
                        {
                            // Apply is not transactional: an error or lost RPC
                            // reply may follow a successful prefix. Record the
                            // flush obligation before the first mutation.
                            local_reconcile_flush_pending = true;
                        }
                        received_keys.extend(entries.iter().map(|entry| entry.key.clone()));
                        if apply_reconcile_entries(fragment, &deps.router, entries, runtime.local_work_timeout, apply_retention.clone())
                            .await
                            .is_err()
                        {
                            reconcile_retry = Some((
                                Instant::now() + NACK_RETRY_DELAY,
                                round.purpose,
                            ));
                            continue;
                        }
                        if !done {
                            round.phase = ReconcilePhase::Data {
                                summary,
                                remaining_chunks,
                                next_chunk_seq: current_chunk_seq,
                                expected_keys,
                                received_keys,
                                next_page_seq: next_page_seq.saturating_add(1),
                            };
                            round.phase_deadline = deadline_after(config.reconcile_timeout);
                            active_reconcile = Some(round);
                            continue;
                        }
                        if received_keys != expected_keys {
                            tracing::warn!(
                                peer = %peer_id,
                                fragment = fragment.0,
                                expected = expected_keys.len(),
                                received = received_keys.len(),
                                "reconcile data terminated before every requested key arrived"
                            );
                            reconcile_retry = Some((
                                Instant::now() + NACK_RETRY_DELAY,
                                round.purpose,
                            ));
                            continue;
                        }
                        let Some(conn) = connection.as_mut() else { continue };
                        if let Some(keys) = remaining_chunks.pop_front() {
                            let next_seq = summary.next_fetch_chunk_seq;
                            summary.next_fetch_chunk_seq =
                                summary.next_fetch_chunk_seq.saturating_add(1);
                            let expected_keys = keys.iter().cloned().collect();
                            if send_fetch(fragment, conn, round_id, next_seq, keys)
                                .await
                                .is_err()
                            {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                                continue;
                            }
                            round.phase = ReconcilePhase::Data {
                                summary,
                                remaining_chunks,
                                next_chunk_seq: next_seq,
                                expected_keys,
                                received_keys: HashSet::new(),
                                next_page_seq: 0,
                            };
                            round.phase_deadline = deadline_after(config.reconcile_timeout);
                            active_reconcile = Some(round);
                            continue;
                        }
                        if !summary.summary_done {
                            if send_summary_request(fragment, conn, round.round_id, &summary)
                                .await
                                .is_err()
                            {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                                continue;
                            }
                            round.phase = ReconcilePhase::Response(summary);
                            round.phase_deadline = deadline_after(config.reconcile_timeout);
                            active_reconcile = Some(round);
                            continue;
                        }
                        // A fetched apply changes the fragment revision, but
                        // unrelated mutations may have raced with it. Force a
                        // no-fetch verification round after this barrier
                        // instead of rebasing to an ambiguous revision.
                        round.snapshot_revision = None;
                        round.phase = ReconcilePhase::Response(summary);
                        match enter_reconcile_barrier(
                            fragment,
                            &deps.router,
                            &config,
                            conn,
                            &mut round,
                            &mut local_reconcile_flush_pending,
                        )
                        .await
                        {
                            Ok(()) => active_reconcile = Some(round),
                            Err(error) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                if error == ReconcileError::Transport {
                                    disconnect_transport(
                                        &mut connection,
                                        delivery,
                                        &mut buffer,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            _ = batch_timer.tick() => {
                if matches!(delivery, DeliveryState::Reset { .. }) {
                    continue;
                }
                // Reset recovery may have discarded the original ingress
                // history. Live concern tickets still own the exact submitted
                // floor; retry it under its requested fence once streaming is
                // available. A superseding receiver winner is a valid floor.
                if connection.as_ref().is_some_and(|conn| conn.session_ready) && active_reconcile.is_none() {
                    if let Some(tracker) = runtime.write_concerns.as_ref() {
                        tracker.retry_operations(&peer_id, fragment, |operation| {
                            if buffer.len() >= config.buffer_capacity { return; }
                            let present = buffer.iter().any(|queued| queued.key == operation.key && queued.value == operation.value)
                                || connection.as_ref().is_some_and(|conn| conn.in_flight.iter().flat_map(|batch| &batch.ops)
                                    .any(|queued| queued.key == operation.key && queued.value == operation.value));
                            if !present {
                                if let Some(operation) = retention.borrowed(operation) { buffer.push_back(operation); }
                            }
                        });
                    }
                }
                let (sid, tx, tracked_deadline) = match connection.as_ref() {
                    Some(conn)
                        if !buffer.is_empty()
                            && conn.in_flight.len() < config.max_in_flight
                            && conn.session_ready
                            && active_reconcile.is_none()
                            && !pause_new_streaming
                            && retry_not_before
                                .map(|deadline| Instant::now() >= deadline)
                                .unwrap_or(true) =>
                    {
                        let tracked_deadline = conn
                            .next_streaming_deadline()
                            .into_iter()
                            .chain(active_reconcile.as_ref().map(|round| round.phase_deadline))
                            .min();
                        (conn.session_id, conn.peer_tx.clone(), tracked_deadline)
                    }
                    _ => continue,
                };
                let batch_seq = match connection.as_mut() {
                    Some(conn) => allocate_batch_seq(conn),
                    None => continue,
                };
                let required_ack_durability = runtime.write_concerns.as_ref().map_or(config.required_ack_durability,
                    |tracker| config.required_ack_durability.max(tracker.required_durability(&peer_id, fragment).into()));
                let required_ack_level = required_ack_durability.proto_level() as i32;
                let (ops, proto_ops) = match take_streaming_batch(
                    &mut buffer,
                    config.batch_size,
                    sid,
                    batch_seq,
                    fragment,
                    required_ack_level,
                ) {
                    Ok(batch) => batch,
                    Err(()) => {
                        tracing::error!(
                            peer = %peer_id,
                            fragment = fragment.0,
                            "buffered mutation cannot fit in one replication frame"
                        );
                        retry_not_before = Some(Instant::now() + NACK_RETRY_DELAY);
                        continue;
                    }
                };
                let ok = send_envelope(
                    &tx,
                    tracked_deadline,
                    EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                        session_id: sid,
                        batch_seq,
                        fragment: fragment.0,
                        ops: proto_ops,
                        required_ack_level,
                    }),
                )
                .await;
                if ok {
                    if let Some(conn) = connection.as_mut() {
                        conn.in_flight.push_back(InflightBatch {
                            batch_seq,
                            ops,
                            kind: InflightKind::Streaming {
                                ack_deadline: deadline_after(runtime.ack_timeout),
                            },
                            required_ack_durability,
                        });
                    }
                } else {
                    for op in ops.into_iter().rev() {
                        buffer.push_front(op);
                    }
                    retry_not_before = Some(Instant::now() + NACK_RETRY_DELAY);
                    if disconnect_transport(&mut connection, delivery, &mut buffer) {
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                        if let Some(round) = active_reconcile.take() {
                            reconcile_retry = Some((Instant::now(), round.purpose));
                        }
                    }
                }
            }
            _ = async {
                match sender_wakeup {
                    Some(deadline) => {
                        tokio::time::sleep_until(deadline).await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                let now = Instant::now();
                if session_retry.is_some_and(|deadline| now >= deadline) {
                    let restarted = match connection.as_mut() {
                        Some(conn) if !conn.session_ready => {
                            send_session_start(fragment, conn).await
                        }
                        Some(_) => true,
                        None => false,
                    };
                    if restarted || disconnect_transport(&mut connection, delivery, &mut buffer) {
                        session_retry = None;
                    } else {
                        session_retry = Some(Instant::now() + NACK_RETRY_DELAY);
                    }
                    continue;
                }
                if active_reconcile
                    .as_ref()
                    .is_some_and(|round| now >= round.phase_deadline)
                {
                    let round = active_reconcile.take().expect("checked above");
                    tracing::warn!(
                        peer = %peer_id,
                        fragment = fragment.0,
                        session_id = round.session_id,
                        round_id = round.round_id,
                        "reconciliation phase timed out; rotating session"
                    );
                    reconcile_retry = Some((now + NACK_RETRY_DELAY, round.purpose));
                    let restarted = if let Some(conn) = connection.as_mut() {
                        if matches!(delivery, DeliveryState::Streaming) {
                            requeue_in_flight(&mut conn.in_flight, &mut buffer);
                        } else {
                            conn.in_flight.clear();
                        }
                        conn.session_id = rand_session_id();
                        conn.next_batch_seq = 0;
                        send_session_start(fragment, conn).await
                    } else {
                        true
                    };
                    if !restarted
                        && !disconnect_transport(&mut connection, delivery, &mut buffer)
                    {
                        session_retry = Some(Instant::now() + NACK_RETRY_DELAY);
                    }
                    continue;
                }

                let ordinary_ack_timed_out = connection.as_ref().is_some_and(|conn| {
                    conn.in_flight.iter().any(|batch| {
                        matches!(
                            batch.kind,
                            InflightKind::Streaming { ack_deadline } if now >= ack_deadline
                        )
                    })
                });
                if ordinary_ack_timed_out {
                    pause_new_streaming = false;
                    let expired_session = connection.as_ref().map_or(0, |conn| conn.session_id);
                    tracing::warn!(
                        peer = %peer_id,
                        fragment = fragment.0,
                        session_id = expired_session,
                        "replication ACK timed out; rotating session and replaying in-flight batches"
                    );
                    if let Some(round) = active_reconcile.take() {
                        reconcile_retry = Some((now + NACK_RETRY_DELAY, round.purpose));
                    }
                    retry_not_before = Some(now + NACK_RETRY_DELAY);
                    let restarted = if let Some(conn) = connection.as_mut() {
                        requeue_in_flight(&mut conn.in_flight, &mut buffer);
                        conn.session_id = rand_session_id();
                        conn.next_batch_seq = 0;
                        send_session_start(fragment, conn).await
                    } else {
                        true
                    };
                    if !restarted
                        && !disconnect_transport(&mut connection, delivery, &mut buffer)
                    {
                        session_retry = Some(Instant::now() + NACK_RETRY_DELAY);
                    }
                    continue;
                }

                let Some((deadline, requested_purpose)) = reconcile_retry else {
                    continue;
                };
                if now < deadline {
                    continue;
                }
                let purpose = match delivery {
                    DeliveryState::Reset { epoch } => ReconcilePurpose::Reset { epoch },
                    DeliveryState::Streaming => requested_purpose,
                };
                if let Some(conn) = connection.as_ref() {
                    if !conn.session_ready || active_reconcile.is_some() {
                        continue;
                    }
                    if !conn.in_flight.is_empty()
                        || (!pause_new_streaming
                            && matches!(delivery, DeliveryState::Streaming)
                            && !buffer.is_empty())
                    {
                        reconcile_retry = Some((deadline_after(NACK_RETRY_DELAY), purpose));
                        continue;
                    }
                    pause_new_streaming = false;
                    match begin_reconcile(
                        fragment,
                        purpose,
                        conn,
                        &mut reconcile_ticket,
                        &mut next_round_id,
                        config.reconcile_timeout,
                    )
                    .await
                    {
                        Ok(Some(round)) => {
                            active_reconcile = Some(round);
                            reconcile_retry = None;
                        }
                        Ok(None) => {
                            reconcile_retry = Some((now + NACK_RETRY_DELAY, purpose));
                            pause_new_streaming = true;
                        }
                        Err(()) => {
                            reconcile_retry = Some((now + NACK_RETRY_DELAY, purpose));
                            pause_new_streaming = false;
                            disconnect_transport(&mut connection, delivery, &mut buffer);
                        }
                    }
                }
            }
            _ = async {
                match reconcile_credit_rx.as_mut() {
                    Some(changes) => {
                        let _ = changes.changed().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                if let Some((_, purpose)) = reconcile_retry {
                    reconcile_retry = Some((Instant::now(), purpose));
                }
            }
            _ = consistency_timer.tick() => {
                if drain_state.routine_reconcile_allowed()
                    && matches!(delivery, DeliveryState::Streaming)
                    && active_reconcile.is_none()
                    && reconcile_retry.is_none()
                {
                    if let Some(conn) = connection.as_ref() {
                        if !conn.session_ready {
                            continue;
                        }
                        if !conn.in_flight.is_empty() {
                            reconcile_retry = Some((
                                deadline_after(NACK_RETRY_DELAY),
                                ReconcilePurpose::Routine,
                            ));
                            pause_new_streaming = true;
                            continue;
                        }
                        pause_new_streaming = false;
                        match begin_reconcile(
                            fragment,
                            ReconcilePurpose::Routine,
                            conn,
                            &mut reconcile_ticket,
                            &mut next_round_id,
                            config.reconcile_timeout,
                        )
                        .await
                        {
                            Ok(Some(round)) => {
                                active_reconcile = Some(round);
                                reconcile_retry = None;
                            }
                            Ok(None) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    ReconcilePurpose::Routine,
                                ));
                                pause_new_streaming = true;
                            }
                            Err(()) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    ReconcilePurpose::Routine,
                                ));
                                pause_new_streaming = false;
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// A count ceiling complements the exact encoded-byte ceiling. Responders
/// stream values into byte-bounded data pages and never clone this whole set.
const RECONCILE_LOCAL_READ_CHUNK: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileError {
    Local,
    Transport,
    VersionConflict,
}

fn next_nonzero(counter: &mut u64) -> u64 {
    *counter = counter.wrapping_add(1);
    if *counter == 0 {
        *counter = 1;
    }
    *counter
}

fn next_generation(current: u64) -> u64 {
    let next = current.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}

fn allocate_batch_seq(connection: &mut ActiveConnection) -> u64 {
    let seq = connection.next_batch_seq;
    connection.next_batch_seq = if seq == u64::MAX - 1 { 0 } else { seq + 1 };
    seq
}

async fn send_session_start(fragment: FragmentId, connection: &mut ActiveConnection) -> bool {
    let sent = send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::SessionStart(SessionStart {
            session_id: connection.session_id,
            fragment: fragment.0,
        }),
    )
    .await;
    connection.session_ready = sent;
    sent
}

async fn begin_reconcile(
    fragment: FragmentId,
    purpose: ReconcilePurpose,
    connection: &ActiveConnection,
    reconcile_ticket: &mut Option<ReconcileTicket>,
    next_round_id: &mut u64,
    phase_timeout: Duration,
) -> Result<Option<ReconcileRound>, ()> {
    let connection_permit = match connection.peer_tx.peer.node_transport() {
        Some(peer) => match peer.try_acquire_reconcile(reconcile_ticket)? {
            Some(permit) => Some(permit),
            None => return Ok(None),
        },
        None => {
            *reconcile_ticket = None;
            None
        }
    };
    let round_id = next_nonzero(next_round_id);
    let (recovery_epoch, recovery_replica_digest) =
        connection.peer_tx.recovery.as_ref().map_or_else(
            || (Vec::new(), Vec::new()),
            |recovery| recovery.request_fields(fragment),
        );
    let ok = send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReconcileRequest(ReconcileRequest {
            fragment: fragment.0,
            session_id: connection.session_id,
            round_id,
            page_seq: 0,
            after_key: String::new(),
            snapshot_revision: 0,
            snapshot_time_ms: 0,
            recovery_epoch,
            recovery_replica_digest,
        }),
    )
    .await;
    if !ok {
        return Err(());
    }
    Ok(Some(ReconcileRound {
        session_id: connection.session_id,
        round_id,
        purpose,
        phase: ReconcilePhase::Response(ReconcileSummaryState {
            next_page_seq: 0,
            next_fetch_chunk_seq: 0,
            after_key: String::new(),
            remote_snapshot_revision: None,
            remote_snapshot_time_ms: None,
            remote_revision_stable: true,
            local: ReconcileLocalCursor {
                result_retention: None,
                after_key: String::new(),
                revision: None,
                snapshot_time_ms: None,
                revision_stable: true,
                buffered: VecDeque::new(),
                current: None,
                done: false,
            },
            changed_any: false,
            summary_done: false,
        }),
        phase_deadline: deadline_after(phase_timeout),
        snapshot_revision: None,
        connection_permit,
        control_retention: None,
    }))
}

async fn send_reconcile_barrier(
    fragment: FragmentId,
    required_ack_durability: ReplicationAckDurability,
    connection: &mut ActiveConnection,
) -> Result<u64, ()> {
    let batch_seq = allocate_batch_seq(connection);
    let ok = send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReplicateBatch(ReplicateBatch {
            session_id: connection.session_id,
            batch_seq,
            fragment: fragment.0,
            ops: Vec::new(),
            required_ack_level: required_ack_durability.proto_level() as i32,
        }),
    )
    .await;
    if !ok {
        return Err(());
    }
    connection.in_flight.push_back(InflightBatch {
        batch_seq,
        ops: Vec::new(),
        kind: InflightKind::ReconcileBarrier,
        required_ack_durability,
    });
    Ok(batch_seq)
}

async fn send_repair_ops(
    fragment: FragmentId,
    connection: &ActiveConnection,
    ops: &[impl std::borrow::Borrow<crate::actor::ReplicatedOp>],
) -> Result<(), ReconcileError> {
    let mut offset = 0;
    while offset < ops.len() {
        let mut batch = ReplicateBatch {
            session_id: connection.session_id,
            batch_seq: u64::MAX,
            fragment: fragment.0,
            ops: Vec::new(),
            // Untracked repairs are apply-only. The following tracked empty
            // barrier requests and reports the actual fence.
            required_ack_level: ReplicationAckLevel::Applied as i32,
        };
        let mut sizing = RepeatedFieldSizer::new(9, 4, batch.encoded_len());
        let mut consumed = 0;
        for op in ops[offset..].iter().take(100) {
            let op = op.borrow();
            let item_len = proto_op_encoded_len(op);
            let encoded_len = sizing.encoded_len_with(item_len);
            if encoded_len > MAX_FRAME_BYTES {
                break;
            }
            if encoded_len > ANTI_ENTROPY_PAGE_BYTES && !batch.ops.is_empty() {
                break;
            }
            let encoded = proto_op(op);
            debug_assert_eq!(encoded.encoded_len(), item_len);
            sizing.push(item_len);
            batch.ops.push(encoded);
            consumed += 1;
            if encoded_len > ANTI_ENTROPY_PAGE_BYTES {
                break;
            }
        }
        if consumed == 0 {
            tracing::warn!(
                fragment = fragment.0,
                key = %ops[offset].borrow().key,
                "reconciliation repair cannot fit in one wire frame"
            );
            return Err(ReconcileError::Local);
        }
        if !send_envelope(
            &connection.peer_tx,
            connection.next_streaming_deadline(),
            EnvelopeMsg::ReplicateBatch(batch),
        )
        .await
        {
            return Err(ReconcileError::Transport);
        }
        offset += consumed;
    }
    Ok(())
}

fn reconcile_summaries_are_well_formed(
    fragment: FragmentId,
    router: &FragmentRouter,
    has_cursor: bool,
    after_key: &str,
    summaries: &[KeySummary],
    next_after_key: &str,
    done: bool,
) -> bool {
    if summaries.len() > MAX_RECONCILE_PAGE_ITEMS
        || summaries.iter().any(|summary| {
            summary.content_digest.len() != 32 || !(1..=255).contains(&summary.node_id.len())
        })
    {
        return false;
    }
    if summaries.is_empty() {
        return done && next_after_key == after_key;
    }
    if (has_cursor && summaries[0].key.as_str() <= after_key)
        || summaries.windows(2).any(|pair| pair[0].key >= pair[1].key)
        || summaries
            .iter()
            .any(|summary| !router.key_belongs_to(fragment, &summary.key))
    {
        return false;
    }
    summaries
        .last()
        .is_some_and(|summary| summary.key == next_after_key)
}

fn reconcile_entries_are_well_formed(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: &[ReconcileEntry],
    expected_keys: &HashSet<String>,
    received_keys: &HashSet<String>,
) -> bool {
    if entries.len() > MAX_RECONCILE_PAGE_ITEMS || entries.len() > expected_keys.len() {
        return false;
    }
    let mut page_keys = HashSet::with_capacity(entries.len());
    entries.iter().all(|entry| {
        expected_keys.contains(&entry.key)
            && !received_keys.contains(&entry.key)
            && page_keys.insert(entry.key.as_str())
            && router.key_belongs_to(fragment, &entry.key)
    })
}

fn remote_summary_wins(remote: &KeySummary, local: &SnapshotSummary) -> bool {
    (
        remote.timestamp_ms,
        remote.logical_counter,
        &remote.node_id,
        remote.is_tombstone,
    ) > (
        local.timestamp_ms,
        local.logical_counter,
        &local.node_id,
        local.is_tombstone,
    )
}

fn local_summary_wins(local: &SnapshotSummary, remote: &KeySummary) -> bool {
    (
        local.timestamp_ms,
        local.logical_counter,
        &local.node_id,
        local.is_tombstone,
    ) > (
        remote.timestamp_ms,
        remote.logical_counter,
        &remote.node_id,
        remote.is_tombstone,
    )
}

async fn ensure_local_summary(
    fragment: FragmentId,
    router: &FragmentRouter,
    local: &mut ReconcileLocalCursor,
    deadline: Instant,
    result_budget: &Arc<ByteBudget>,
    retention: Option<Arc<dyn Send + Sync>>,
) -> Result<(), ReconcileError> {
    while local.current.is_none() {
        if let Some(summary) = local.buffered.pop_front() {
            local.current = Some(summary);
            return Ok(());
        }
        if local.done {
            return Ok(());
        }
        let actor = router
            .route_for_fragment(fragment)
            .ok_or(ReconcileError::Local)?;
        let result_retention = if let Some(retention) = &local.result_retention {
            retention.clone()
        } else {
            let retention = Arc::new(
                result_budget
                    .reserve(2 * MAX_FRAME_BYTES)
                    .ok_or(ReconcileError::Local)?,
            );
            local.result_retention = Some(retention.clone());
            retention
        };
        let first_page = local.revision.is_none();
        let page = match actor
            .call(
                |reply| {
                    KvMessage::GetSummaryPage {
                        after_key: (!first_page).then_some(local.after_key.clone()),
                        // Reconciliation is a bounded, monotonic scan rather
                        // than a lock-held snapshot. Drift is recorded below and
                        // forces a fresh verification pass after the barrier.
                        expected_revision: None,
                        snapshot_time_ms: local.snapshot_time_ms,
                        limit: RECONCILE_LOCAL_READ_CHUNK,
                        reply,
                    }
                    .with_retention(Arc::new((result_retention, retention.clone())))
                },
                Some(deadline.saturating_duration_since(Instant::now())),
            )
            .await
        {
            Ok(CallResult::Success(page)) => page,
            _ => return Err(ReconcileError::Local),
        };
        if page.restart
            || (page.summaries.is_empty() && !page.done)
            || (!first_page
                && page
                    .summaries
                    .first()
                    .is_some_and(|summary| summary.key <= local.after_key))
            || page
                .summaries
                .windows(2)
                .any(|pair| pair[0].key >= pair[1].key)
        {
            return Err(ReconcileError::Local);
        }
        match (local.revision, local.snapshot_time_ms) {
            (None, None) => {
                local.revision = Some(page.revision);
                local.snapshot_time_ms = Some(page.snapshot_time_ms);
            }
            (Some(revision), Some(snapshot_time_ms))
                if snapshot_time_ms == page.snapshot_time_ms =>
            {
                if revision != page.revision {
                    local.revision_stable = false;
                }
            }
            _ => return Err(ReconcileError::Local),
        }
        local.done = page.done;
        local.buffered.extend(page.summaries);
    }
    Ok(())
}

fn take_local_summary(local: &mut ReconcileLocalCursor) -> Option<SnapshotSummary> {
    let summary = local.current.take()?;
    local.after_key = summary.key.clone();
    Some(summary)
}

async fn send_repair_keys(
    fragment: FragmentId,
    router: &FragmentRouter,
    connection: &ActiveConnection,
    keys: Vec<String>,
    snapshot_time_ms: u64,
    deadline: Instant,
    retention: Option<Arc<dyn Send + Sync>>,
) -> Result<(), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    let result_retention = Arc::new(
        connection
            .peer_tx
            .reconciliation_result_budget
            .reserve(2 * MAX_FRAME_BYTES)
            .ok_or(ReconcileError::Local)?,
    );
    let keys = Arc::new(keys);
    let mut next_key_index = 0usize;
    let mut page = Vec::new();
    let sizing_batch = ReplicateBatch {
        session_id: connection.session_id,
        batch_seq: u64::MAX,
        fragment: fragment.0,
        ops: Vec::new(),
        required_ack_level: ReplicationAckLevel::Applied as i32,
    };
    let base_payload_len = sizing_batch.encoded_len();
    let mut sizing = RepeatedFieldSizer::new(9, 4, base_payload_len);
    while next_key_index < keys.len() {
        let request_keys = keys.clone();
        let request_retention = result_retention.clone();
        let authority_retention = retention.clone();
        let value_page = match actor
            .call(
                move |reply| {
                    KvMessage::GetVersionedPage {
                        keys: request_keys,
                        start_index: next_key_index,
                        snapshot_time_ms: Some(snapshot_time_ms),
                        reply,
                    }
                    .with_retention(Arc::new((request_retention, authority_retention)))
                },
                Some(deadline.saturating_duration_since(Instant::now())),
            )
            .await
        {
            Ok(CallResult::Success(value_page)) => value_page,
            _ => return Err(ReconcileError::Local),
        };
        if value_page.missing
            || value_page.next_index <= next_key_index
            || value_page.next_index > keys.len()
        {
            return Err(ReconcileError::Local);
        }
        next_key_index = value_page.next_index;
        for (key, value) in value_page.entries {
            let op = crate::actor::ReplicatedOp {
                key,
                value,
                seq: 0,
                boot_id: 0,
            };
            let item_len = proto_op_encoded_len(&op);
            let encoded_len = sizing.encoded_len_with(item_len);
            if encoded_len > MAX_FRAME_BYTES {
                if page.is_empty() {
                    tracing::warn!(
                        fragment = fragment.0,
                        key = %op.key,
                        "one reconciliation repair exceeds the hard frame limit"
                    );
                    return Err(ReconcileError::Local);
                }
                send_repair_ops(fragment, connection, &page).await?;
                page.clear();
                sizing.reset(base_payload_len);
                if sizing.encoded_len_with(item_len) > MAX_FRAME_BYTES {
                    tracing::warn!(
                        fragment = fragment.0,
                        key = %op.key,
                        "one reconciliation repair exceeds the hard frame limit"
                    );
                    return Err(ReconcileError::Local);
                }
            } else if encoded_len > ANTI_ENTROPY_PAGE_BYTES && !page.is_empty() {
                send_repair_ops(fragment, connection, &page).await?;
                page.clear();
                sizing.reset(base_payload_len);
            }
            sizing.push(item_len);
            page.push(op);
        }
        if !page.is_empty() {
            send_repair_ops(fragment, connection, &page).await?;
            page.clear();
            sizing.reset(base_payload_len);
        }
    }
    if !page.is_empty() {
        send_repair_ops(fragment, connection, &page).await?;
    }
    Ok(())
}

fn reconcile_fetch_chunks(
    fragment: FragmentId,
    session_id: u64,
    round_id: u64,
    keys: Vec<String>,
) -> Result<VecDeque<Vec<String>>, ReconcileError> {
    let mut chunks = VecDeque::new();
    let request = ReconcileFetch {
        fragment: fragment.0,
        keys: Vec::new(),
        session_id,
        round_id,
        chunk_seq: u64::MAX,
    };
    let base_payload_len = request.encoded_len();
    let mut sizing = RepeatedFieldSizer::new(13, 2, base_payload_len);
    let mut request_keys = Vec::new();
    for key in keys {
        let encoded_len = sizing.encoded_len_with(key.len());
        let next_count = request_keys.len() + 1;
        if encoded_len > MAX_FRAME_BYTES
            || ((encoded_len > ANTI_ENTROPY_PAGE_BYTES || next_count > MAX_RECONCILE_FETCH_KEYS)
                && !request_keys.is_empty())
        {
            if request_keys.is_empty() {
                return Err(ReconcileError::Local);
            }
            chunks.push_back(std::mem::take(&mut request_keys));
            sizing.reset(base_payload_len);
            if sizing.encoded_len_with(key.len()) > MAX_FRAME_BYTES {
                return Err(ReconcileError::Local);
            }
        }
        sizing.push(key.len());
        request_keys.push(key);
    }
    if !request_keys.is_empty() {
        chunks.push_back(request_keys);
    }
    Ok(chunks)
}

async fn send_summary_request(
    fragment: FragmentId,
    connection: &ActiveConnection,
    round_id: u64,
    summary: &ReconcileSummaryState,
) -> Result<(), ReconcileError> {
    let (recovery_epoch, recovery_replica_digest) =
        connection.peer_tx.recovery.as_ref().map_or_else(
            || (Vec::new(), Vec::new()),
            |recovery| recovery.request_fields(fragment),
        );
    if send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReconcileRequest(ReconcileRequest {
            fragment: fragment.0,
            session_id: connection.session_id,
            round_id,
            page_seq: summary.next_page_seq,
            after_key: summary.after_key.clone(),
            snapshot_revision: summary.remote_snapshot_revision.unwrap_or(0),
            snapshot_time_ms: summary.remote_snapshot_time_ms.unwrap_or(0),
            recovery_epoch,
            recovery_replica_digest,
        }),
    )
    .await
    {
        Ok(())
    } else {
        Err(ReconcileError::Transport)
    }
}

async fn send_fetch(
    fragment: FragmentId,
    connection: &ActiveConnection,
    round_id: u64,
    chunk_seq: u64,
    keys: Vec<String>,
) -> Result<(), ReconcileError> {
    if send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReconcileFetch(ReconcileFetch {
            fragment: fragment.0,
            keys,
            session_id: connection.session_id,
            round_id,
            chunk_seq,
        }),
    )
    .await
    {
        Ok(())
    } else {
        Err(ReconcileError::Transport)
    }
}

/// Merge one bounded peer summary page with bounded local summary pages.
/// Values are read only for local winners, one at a time, so neither side's
/// complete key or value set is materialized by reconciliation.
struct ReconcilePageInput {
    done: bool,
    retention: Option<Arc<dyn Send + Sync>>,
}

async fn reconcile_summary_page(
    fragment: FragmentId,
    router: &FragmentRouter,
    connection: &ActiveConnection,
    round_id: u64,
    summary: &mut ReconcileSummaryState,
    summaries: &[KeySummary],
    page: ReconcilePageInput,
) -> Result<VecDeque<Vec<String>>, ReconcileError> {
    let ReconcilePageInput { done, retention } = page;
    let deadline = deadline_after(connection.peer_tx.local_work_timeout);
    let mut fetch_keys = Vec::new();
    let mut repair_keys = Vec::new();
    let mut repair_key_bytes = 0usize;
    for remote in summaries {
        loop {
            ensure_local_summary(
                fragment,
                router,
                &mut summary.local,
                deadline,
                &connection.peer_tx.reconciliation_result_budget,
                retention.clone(),
            )
            .await?;
            match summary.local.current.as_ref() {
                Some(local) if local.key < remote.key => {
                    let local = take_local_summary(&mut summary.local).expect("matched Some");
                    repair_key_bytes = repair_key_bytes
                        .saturating_add(local.key.capacity())
                        .saturating_add(std::mem::size_of::<String>());
                    repair_keys.push(local.key);
                    if repair_keys.len() >= RECONCILE_LOCAL_READ_CHUNK
                        || repair_key_bytes >= ANTI_ENTROPY_PAGE_BYTES
                    {
                        summary.changed_any = true;
                        let snapshot_time_ms = summary
                            .local
                            .snapshot_time_ms
                            .ok_or(ReconcileError::Local)?;
                        send_repair_keys(
                            fragment,
                            router,
                            connection,
                            std::mem::take(&mut repair_keys),
                            snapshot_time_ms,
                            deadline,
                            retention.clone(),
                        )
                        .await?;
                        repair_key_bytes = 0;
                    }
                }
                Some(local) if local.key == remote.key => {
                    let local = take_local_summary(&mut summary.local).expect("matched Some");
                    if remote_summary_wins(remote, &local) {
                        fetch_keys.push(remote.key.clone());
                    } else if local_summary_wins(&local, remote) {
                        repair_key_bytes = repair_key_bytes
                            .saturating_add(remote.key.len())
                            .saturating_add(std::mem::size_of::<String>());
                        repair_keys.push(remote.key.clone());
                        if repair_keys.len() >= RECONCILE_LOCAL_READ_CHUNK
                            || repair_key_bytes >= ANTI_ENTROPY_PAGE_BYTES
                        {
                            summary.changed_any = true;
                            let snapshot_time_ms = summary
                                .local
                                .snapshot_time_ms
                                .ok_or(ReconcileError::Local)?;
                            send_repair_keys(
                                fragment,
                                router,
                                connection,
                                std::mem::take(&mut repair_keys),
                                snapshot_time_ms,
                                deadline,
                                retention.clone(),
                            )
                            .await?;
                            repair_key_bytes = 0;
                        }
                    } else if remote.content_digest.as_slice() != local.content_digest {
                        tracing::error!(fragment = fragment.0, key = %remote.key,
                            timestamp_ms = remote.timestamp_ms, logical_counter = remote.logical_counter,
                            writer = %remote.node_id, is_tombstone = remote.is_tombstone,
                            "reconciliation found a reused record version with conflicting contents");
                        return Err(ReconcileError::VersionConflict);
                    }
                    break;
                }
                Some(_) | None => {
                    fetch_keys.push(remote.key.clone());
                    break;
                }
            }
        }
    }
    if !repair_keys.is_empty() {
        summary.changed_any = true;
        let snapshot_time_ms = summary
            .local
            .snapshot_time_ms
            .ok_or(ReconcileError::Local)?;
        send_repair_keys(
            fragment,
            router,
            connection,
            std::mem::take(&mut repair_keys),
            snapshot_time_ms,
            deadline,
            retention.clone(),
        )
        .await?;
        repair_key_bytes = 0;
    }

    if done {
        loop {
            ensure_local_summary(
                fragment,
                router,
                &mut summary.local,
                deadline,
                &connection.peer_tx.reconciliation_result_budget,
                retention.clone(),
            )
            .await?;
            let Some(local) = take_local_summary(&mut summary.local) else {
                break;
            };
            repair_key_bytes = repair_key_bytes
                .saturating_add(local.key.capacity())
                .saturating_add(std::mem::size_of::<String>());
            repair_keys.push(local.key);
            if repair_keys.len() >= RECONCILE_LOCAL_READ_CHUNK
                || repair_key_bytes >= ANTI_ENTROPY_PAGE_BYTES
            {
                summary.changed_any = true;
                let snapshot_time_ms = summary
                    .local
                    .snapshot_time_ms
                    .ok_or(ReconcileError::Local)?;
                send_repair_keys(
                    fragment,
                    router,
                    connection,
                    std::mem::take(&mut repair_keys),
                    snapshot_time_ms,
                    deadline,
                    retention.clone(),
                )
                .await?;
                repair_key_bytes = 0;
            }
        }
        if !repair_keys.is_empty() {
            summary.changed_any = true;
            let snapshot_time_ms = summary
                .local
                .snapshot_time_ms
                .ok_or(ReconcileError::Local)?;
            send_repair_keys(
                fragment,
                router,
                connection,
                repair_keys,
                snapshot_time_ms,
                deadline,
                retention.clone(),
            )
            .await?;
        }
    }

    reconcile_fetch_chunks(fragment, connection.session_id, round_id, fetch_keys)
}

async fn apply_reconcile_entries(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: Vec<ReconcileEntry>,
    timeout: Duration,
    retention: Option<Arc<dyn Send + Sync>>,
) -> Result<(), ReconcileError> {
    let deadline = deadline_after(timeout);
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    for entry in entries {
        let mut value = VersionedValue::new_version(
            entry.value,
            actordb_protocol::RecordVersion::new(entry.timestamp_ms, entry.logical_counter),
            entry.node_id,
        );
        value.expires_at_ms = (entry.expires_at_ms != 0).then_some(entry.expires_at_ms);
        value.is_tombstone = entry.is_tombstone;
        let op = crate::actor::ReplicatedOp {
            key: entry.key,
            value,
            seq: 0,
            boot_id: 0,
        };
        if let Err(error) = check_replicated_op_budget(&op) {
            tracing::warn!(fragment = fragment.0, %error, "reconcile value exceeds wire budget");
            return Err(ReconcileError::Local);
        }
        match actor
            .call(
                |reply| {
                    KvMessage::ApplyOp {
                        op,
                        priority: OpPriority::Default,
                        is_replication: true,
                        reply: Some(reply),
                    }
                    .with_optional_retention(retention.clone())
                },
                Some(deadline.saturating_duration_since(Instant::now())),
            )
            .await
        {
            Ok(CallResult::Success(Ok(_))) => {}
            Ok(CallResult::Success(Err(error))) => {
                tracing::warn!(fragment = fragment.0, %error, "reconcile apply failed");
                return Err(ReconcileError::Local);
            }
            Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => {
                return Err(ReconcileError::Local);
            }
        }
    }
    Ok(())
}

async fn enter_reconcile_barrier(
    fragment: FragmentId,
    router: &FragmentRouter,
    config: &SenderConfig,
    connection: &mut ActiveConnection,
    round: &mut ReconcileRound,
    local_reconcile_flush_pending: &mut bool,
) -> Result<(), ReconcileError> {
    if *local_reconcile_flush_pending {
        flush_reconcile(fragment, router, connection.peer_tx.local_work_timeout).await?;
        *local_reconcile_flush_pending = false;
    }
    let required_ack_durability = round
        .purpose
        .required_ack_durability(config.required_ack_durability);
    let batch_seq = send_reconcile_barrier(fragment, required_ack_durability, connection)
        .await
        .map_err(|()| ReconcileError::Transport)?;
    round.phase = ReconcilePhase::Barrier { batch_seq };
    round.phase_deadline = deadline_after(config.reconcile_timeout);
    Ok(())
}

async fn fragment_revision(
    fragment: FragmentId,
    router: &FragmentRouter,
    timeout: Duration,
) -> Result<u64, ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor
        .call(|reply| KvMessage::GetRevision { reply }, Some(timeout))
        .await
    {
        Ok(CallResult::Success(revision)) => Ok(revision),
        Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => Err(ReconcileError::Local),
    }
}

async fn flush_reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
    timeout: Duration,
) -> Result<(), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor
        .call(|reply| KvMessage::Flush { reply }, Some(timeout))
        .await
    {
        Ok(CallResult::Success(Ok(
            ApplyDurability::StableCommitted
            | ApplyDurability::StoreCommitted
            | ApplyDurability::MemoryApplied,
        ))) => Ok(()),
        Ok(CallResult::Success(Err(error))) => {
            tracing::warn!(fragment = fragment.0, %error, "reconcile flush failed");
            Err(ReconcileError::Local)
        }
        Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => Err(ReconcileError::Local),
    }
}

fn rand_session_id() -> u64 {
    // Not cryptographic; just needs to differ across pairings.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    // Mix in a counter so rapid reconnects don't collide.
    static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .hash(&mut h);
    let id = h.finish();
    if id == 0 {
        1
    } else {
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::message_field_len;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn op(key: &str) -> crate::actor::ReplicatedOp {
        crate::actor::ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 0,
            boot_id: 0,
        }
    }

    fn retained(op: crate::actor::ReplicatedOp) -> RetainedOp {
        SenderRetention {
            local: ByteBudget::new(64 * 1024 * 1024),
            aggregate: ByteBudget::new(256 * 1024 * 1024),
        }
        .borrowed(&op)
        .unwrap()
    }

    fn state_report() -> Arc<Mutex<SenderStateReport>> {
        Arc::new(Mutex::new(SenderStateReport {
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
        }))
    }

    #[tokio::test]
    async fn public_bounded_sender_charges_wide_ingress_before_task_selection() {
        let mut wide = op("wide");
        wide.value.value = vec![7; 1024 * 1024];
        let charge = retained_borrowed_op_bytes(&wide);
        let sender = spawn_bounded_sender(
            FragmentId(0),
            "peer".into(),
            SenderConfig {
                retained_byte_capacity: charge,
                buffer_capacity: 100,
                ..Default::default()
            },
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: state_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        // No await occurs before these assertions on a current-thread
        // runtime, so the receiver has not selected either queued mutation.
        sender.send(SenderMsg::Enqueue(wide.clone())).unwrap();
        assert_eq!(sender.retained_bytes(), charge);
        assert_eq!(sender.inner.data_tx.capacity(), 99);
        sender.send(SenderMsg::Enqueue(wide)).unwrap();
        assert_eq!(sender.retained_bytes(), charge);
        assert_eq!(sender.inner.data_tx.capacity(), 99);
        assert_eq!(sender.inner.signals_tx.borrow().overflow_generation, 1);
        assert_eq!(sender.inner.ingress.pending_data.load(Ordering::Acquire), 1);
        let diagnostics = sender.ingress_diagnostics();
        assert_eq!(diagnostics.pending_operations, 1);
        assert_eq!(diagnostics.shed_operations, 1);
        assert_eq!(diagnostics.reset_requests, 1);
        assert_eq!(diagnostics.retained_bytes, charge);
        assert!(diagnostics.recovery_pending);
        sender.stop().await;
        assert_eq!(sender.retained_bytes(), 0);
        assert_eq!(sender.ingress_diagnostics().pending_operations, 0);
    }

    #[tokio::test]
    async fn retained_bytes_reset_before_count_saturation_and_release_on_stop() {
        let mut wide = op("wide");
        wide.value.value = vec![7; 128 * 1024];
        let charge = retained_borrowed_op_bytes(&wide);
        let aggregate = ByteBudget::new(charge);
        let report = state_report();
        let sender = spawn_node_sender(
            FragmentId(0),
            "peer".into(),
            SenderConfig {
                retained_byte_capacity: charge,
                buffer_capacity: 100,
                ..Default::default()
            },
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_aggregate_retention(aggregate.clone()),
        )
        .unwrap();
        sender.offer(&wide).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while report.lock().await.buffered != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(aggregate.used(), charge);
        // ACK/lifecycle controls remain admissible at the payload byte cap.
        sender
            .send(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 1,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();
        sender.offer(&wide).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while report.lock().await.state != "buffer_reset" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(report.lock().await.buffered, 0);
        sender.stop().await;
        assert_eq!(aggregate.used(), 0);
    }

    #[test]
    fn retention_survives_inflight_retry_and_releases_only_after_ack_ownership() {
        let value = op("key");
        let charge = retained_borrowed_op_bytes(&value);
        let retention = SenderRetention {
            local: ByteBudget::new(charge),
            aggregate: ByteBudget::new(charge),
        };
        let mut batches = VecDeque::from([InflightBatch {
            batch_seq: 1,
            ops: vec![retention.borrowed(&value).unwrap()],
            kind: InflightKind::Streaming {
                ack_deadline: Instant::now() + Duration::from_secs(1),
            },
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
        }]);
        let mut buffer = VecDeque::new();
        requeue_in_flight(&mut batches, &mut buffer);
        assert_eq!(retention.aggregate.used(), charge);
        let (tx, _rx) = mpsc::channel(1);
        let mut connection = ActiveConnection {
            peer_tx: SenderPeer::new(ReplicationPeer::new(tx), &SenderRuntimeOptions::default()),
            session_id: 1,
            session_ready: true,
            next_batch_seq: 2,
            in_flight: VecDeque::from([InflightBatch {
                batch_seq: 1,
                ops: buffer.drain(..).collect(),
                kind: InflightKind::Streaming {
                    ack_deadline: Instant::now() + Duration::from_secs(1),
                },
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
            }]),
        };
        assert!(matches!(
            acknowledge_in_flight(&mut connection, 1, ReplicationAckDurability::MemoryApplied),
            InflightAckResult::BelowRequired { .. }
        ));
        assert_eq!(retention.aggregate.used(), charge);
        let accepted =
            acknowledge_in_flight(&mut connection, 1, ReplicationAckDurability::StoreCommitted);
        assert_eq!(retention.aggregate.used(), charge);
        drop(accepted);
        assert_eq!(retention.local.used(), 0);
        assert_eq!(retention.aggregate.used(), 0);
    }

    #[test]
    fn queued_summary_control_charges_malformed_digest_spare_capacity() {
        let digest_capacity = 4 * 1024 * 1024;
        let (connection_valid, _) = oneshot::channel();
        let message = SenderMsg::ReconcileResponse {
            session_id: 1,
            round_id: 1,
            page_seq: 0,
            next_after_key: String::new(),
            snapshot_revision: 1,
            snapshot_time_ms: 1,
            done: true,
            restart: false,
            summaries: vec![KeySummary {
                key: "key".into(),
                timestamp_ms: 1,
                logical_counter: 0,
                node_id: "node".into(),
                is_tombstone: false,
                content_digest: Vec::with_capacity(digest_capacity),
            }],
            connection_valid,
        };
        assert!(retained_control_bytes(&message) >= 2 * digest_capacity);
    }

    #[test]
    fn fanout_ingress_charges_cloned_payload_not_unretained_spare_capacity() {
        let mut value = op("key");
        value.value.value = Vec::with_capacity(4096);
        value.value.value.push(1);
        let copied = retained_borrowed_op_bytes(&value);
        assert!(value.value.value.capacity() > value.value.value.len());
        let retention = SenderRetention {
            local: ByteBudget::new(copied),
            aggregate: ByteBudget::new(copied),
        };
        let clone = retention.borrowed(&value).unwrap();
        assert_eq!(clone.value.value.capacity(), 1);
        drop(clone);
        assert!(retention.borrowed(&value).is_some());
        assert_eq!(retention.local.used(), 0);
    }

    #[test]
    fn shared_sender_allowance_bounds_replica_fanout_before_cloning() {
        let mut value = op("wide");
        value.value.value = vec![9; 128 * 1024];
        let charge = retained_borrowed_op_bytes(&value);
        let aggregate = ByteBudget::new(charge);
        let first = SenderRetention {
            local: ByteBudget::new(charge * 2),
            aggregate: aggregate.clone(),
        };
        let second = SenderRetention {
            local: ByteBudget::new(charge * 2),
            aggregate: aggregate.clone(),
        };
        let owned = first.borrowed(&value).unwrap();
        assert!(second.borrowed(&value).is_none());
        assert_eq!(aggregate.used(), charge);
        drop(owned);
        assert!(second.borrowed(&value).is_some());
        assert_eq!(aggregate.used(), 0);
    }

    #[test]
    fn borrowed_replicate_op_length_matches_prost() {
        for (index, value_len) in [0, 1, 127, 128, 16_383, 16_384].into_iter().enumerate() {
            let value = VersionedValue {
                value: vec![7; value_len],
                timestamp_ms: if index % 2 == 0 { 0 } else { u64::MAX },
                logical_counter: 0,
                node_id: if index == 0 {
                    String::new()
                } else {
                    "node".repeat(index)
                },
                expires_at_ms: (index % 3 == 0).then_some(u64::MAX),
                is_tombstone: index % 2 != 0,
            };
            let op = crate::actor::ReplicatedOp {
                key: "k".repeat(value_len.min(128)),
                value,
                seq: if index % 2 == 0 { 0 } else { u64::MAX },
                boot_id: if index % 3 == 0 { 0 } else { u64::MAX },
            };
            assert_eq!(proto_op_encoded_len(&op), proto_op(&op).encoded_len());
        }
    }

    #[test]
    fn streaming_batch_soft_byte_split_preserves_buffer_order() {
        let mut first = op("first");
        first.value.value = vec![1; ANTI_ENTROPY_PAGE_BYTES / 2 + 1024];
        let mut second = op("second");
        second.value.value = vec![2; ANTI_ENTROPY_PAGE_BYTES / 2 + 1024];
        let third = op("third");
        let mut buffer = VecDeque::from([
            retained(first.clone()),
            retained(second.clone()),
            retained(third.clone()),
        ]);

        let (taken, encoded) = take_streaming_batch(
            &mut buffer,
            100,
            1,
            2,
            FragmentId(0),
            ReplicationAckLevel::Applied as i32,
        )
        .unwrap();

        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].key, first.key);
        assert_eq!(taken[0].value.value, first.value.value);
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].key, "first");
        assert_eq!(
            buffer.into_iter().map(|op| op.op.key).collect::<Vec<_>>(),
            vec![second.key, third.key]
        );
    }

    #[test]
    fn streaming_batch_preserves_the_wire_item_count_ceiling() {
        let mut buffer = (0..=MAX_WIRE_BATCH_ITEMS)
            .map(|index| retained(op(&format!("key-{index}"))))
            .collect::<VecDeque<_>>();

        let (taken, encoded) = take_streaming_batch(
            &mut buffer,
            usize::MAX,
            u64::MAX,
            u64::MAX,
            FragmentId(u32::MAX),
            ReplicationAckLevel::StableCommitted as i32,
        )
        .unwrap();

        assert_eq!(taken.len(), MAX_WIRE_BATCH_ITEMS);
        assert_eq!(encoded.len(), MAX_WIRE_BATCH_ITEMS);
        assert_eq!(buffer.len(), 1);
        assert_eq!(
            buffer.front().unwrap().key,
            format!("key-{MAX_WIRE_BATCH_ITEMS}")
        );
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: u64::MAX,
                batch_seq: u64::MAX,
                fragment: u32::MAX,
                ops: encoded,
                required_ack_level: ReplicationAckLevel::StableCommitted as i32,
            })),
        };
        assert!(envelope.encoded_len() <= ANTI_ENTROPY_PAGE_BYTES);
    }

    #[test]
    fn reconcile_fetches_batch_keys_without_exceeding_count_or_byte_bounds() {
        let chunks = reconcile_fetch_chunks(
            FragmentId(0),
            1,
            2,
            (0..600).map(|index| format!("key-{index:04}")).collect(),
        )
        .unwrap();
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![256, 256, 88]
        );

        let metadata_heavy = reconcile_fetch_chunks(
            FragmentId(0),
            1,
            2,
            (0..100)
                .map(|index| format!("{index:04}-{}", "k".repeat(20 * 1024)))
                .collect(),
        )
        .unwrap();
        assert!(metadata_heavy.len() > 1);
        assert!(metadata_heavy.iter().all(|keys| {
            keys.len() <= MAX_RECONCILE_FETCH_KEYS
                && message_field_len(
                    13,
                    ReconcileFetch {
                        fragment: 0,
                        keys: keys.clone(),
                        session_id: 1,
                        round_id: 2,
                        chunk_seq: u64::MAX,
                    }
                    .encoded_len(),
                ) <= ANTI_ENTROPY_PAGE_BYTES
        }));

        let large_key = "k".repeat(ANTI_ENTROPY_PAGE_BYTES + 1024);
        let singleton = reconcile_fetch_chunks(FragmentId(0), 1, 2, vec![large_key.clone()])
            .expect("one valid key may exceed the soft target");
        assert_eq!(singleton, VecDeque::from([vec![large_key]]));
        assert!(
            message_field_len(
                13,
                ReconcileFetch {
                    fragment: 0,
                    keys: singleton[0].clone(),
                    session_id: 1,
                    round_id: 2,
                    chunk_seq: u64::MAX,
                }
                .encoded_len(),
            ) > ANTI_ENTROPY_PAGE_BYTES
        );

        let empty_keys = reconcile_fetch_chunks(
            FragmentId(0),
            1,
            2,
            (0..=MAX_RECONCILE_FETCH_KEYS)
                .map(|_| String::new())
                .collect(),
        )
        .unwrap();
        assert_eq!(
            empty_keys.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![MAX_RECONCILE_FETCH_KEYS, 1]
        );
        assert!(empty_keys.iter().all(|keys| {
            message_field_len(
                13,
                ReconcileFetch {
                    fragment: 0,
                    keys: keys.clone(),
                    session_id: 1,
                    round_id: 2,
                    chunk_seq: u64::MAX,
                }
                .encoded_len(),
            ) <= MAX_FRAME_BYTES
        }));
    }

    #[test]
    fn summary_page_validation_distinguishes_empty_key_from_no_cursor() {
        let router = FragmentRouter::new(1, 0);
        let first = vec![KeySummary {
            content_digest: vec![0; 32],
            key: String::new(),
            timestamp_ms: 1,
            logical_counter: 0,
            node_id: "node".to_string(),
            is_tombstone: false,
        }];
        assert!(reconcile_summaries_are_well_formed(
            FragmentId(0),
            &router,
            false,
            "",
            &first,
            "",
            false,
        ));

        let second = vec![KeySummary {
            content_digest: vec![0; 32],
            key: "next".to_string(),
            timestamp_ms: 2,
            logical_counter: 0,
            node_id: "node".to_string(),
            is_tombstone: false,
        }];
        assert!(reconcile_summaries_are_well_formed(
            FragmentId(0),
            &router,
            true,
            "",
            &second,
            "next",
            true,
        ));
    }

    #[tokio::test]
    async fn equal_version_content_conflict_cannot_complete_reconciliation() {
        let router = FragmentRouter::new(1, 0);
        let (peer_tx, mut peer_rx) = mpsc::channel(2);
        let connection = ActiveConnection {
            peer_tx: SenderPeer::new(
                ReplicationPeer::new(peer_tx),
                &SenderRuntimeOptions::default(),
            ),
            session_id: 1,
            session_ready: true,
            next_batch_seq: 0,
            in_flight: VecDeque::new(),
        };
        for same_contents in [false, true] {
            let local = SnapshotSummary {
                key: "key".into(),
                timestamp_ms: 100,
                logical_counter: 9,
                node_id: "writer".into(),
                is_tombstone: false,
                content_digest: [1; 32],
            };
            let remote = KeySummary {
                key: local.key.clone(),
                timestamp_ms: local.timestamp_ms,
                logical_counter: local.logical_counter,
                node_id: local.node_id.clone(),
                is_tombstone: false,
                content_digest: vec![if same_contents { 1 } else { 2 }; 32],
            };
            let mut summary = ReconcileSummaryState {
                next_page_seq: 0,
                next_fetch_chunk_seq: 0,
                after_key: String::new(),
                remote_snapshot_revision: Some(0),
                remote_snapshot_time_ms: Some(100),
                remote_revision_stable: true,
                changed_any: false,
                summary_done: true,
                local: ReconcileLocalCursor {
                    result_retention: None,
                    after_key: String::new(),
                    revision: Some(0),
                    snapshot_time_ms: Some(100),
                    revision_stable: true,
                    buffered: VecDeque::new(),
                    current: Some(local),
                    done: true,
                },
            };
            let result = reconcile_summary_page(
                FragmentId(0),
                &router,
                &connection,
                1,
                &mut summary,
                &[remote],
                ReconcilePageInput {
                    done: true,
                    retention: None,
                },
            )
            .await;
            if same_contents {
                assert!(result.unwrap().is_empty());
            } else {
                assert_eq!(result.unwrap_err(), ReconcileError::VersionConflict);
            }
            assert!(!summary.changed_any);
            assert!(
                peer_rx.try_recv().is_err(),
                "a conflict must not select an arbitrary repair winner"
            );
        }
    }

    #[test]
    fn summary_page_requires_complete_content_digest_and_valid_writer() {
        let router = FragmentRouter::new(1, 0);
        let mut summary = KeySummary {
            key: "key".into(),
            timestamp_ms: 1,
            logical_counter: 0,
            node_id: "writer".into(),
            is_tombstone: false,
            content_digest: vec![0; 32],
        };
        for length in [0, 1, 31, 33] {
            summary.content_digest = vec![0; length];
            assert!(!reconcile_summaries_are_well_formed(
                FragmentId(0),
                &router,
                false,
                "",
                &[summary.clone()],
                "key",
                true
            ));
        }
        summary.content_digest = vec![0; 32];
        summary.node_id.clear();
        assert!(!reconcile_summaries_are_well_formed(
            FragmentId(0),
            &router,
            false,
            "",
            &[summary],
            "key",
            true
        ));
    }

    async fn wait_for_sender_report(
        report: &Arc<Mutex<SenderStateReport>>,
        predicate: impl Fn(&SenderStateReport) -> bool,
    ) -> SenderStateReport {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = report.lock().await.clone();
                if predicate(&state) {
                    return state;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender state did not converge")
    }

    #[test]
    fn sender_option_diagnostics_cover_node_ingress_limits() {
        let debug = format!("{:?}", SenderRuntimeOptions::default());
        assert!(debug.contains("control_channel_capacity: 64"));
        assert_eq!(
            SenderOptionsError::BufferCapacityTooLarge.to_string(),
            "replication buffer capacity exceeds Tokio's maximum"
        );
        assert_eq!(
            SenderOptionsError::ZeroControlChannelCapacity.to_string(),
            "replication control channel capacity must be non-zero"
        );
        assert_eq!(
            SenderOptionsError::ControlChannelCapacityTooLarge.to_string(),
            "replication control channel capacity exceeds Tokio's maximum"
        );
    }

    #[tokio::test]
    async fn node_sender_bounds_data_and_control_without_losing_signals() {
        let (data_tx, mut data_rx) = mpsc::channel(1);
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
        let ingress = Arc::new(NodeSenderIngress::default());
        let (stopped_tx, stopped) = watch::channel(false);
        let (_drain_completed_tx, drain_completed) = watch::channel(0u64);
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let task_ingress = ingress.clone();
        let task = tokio::spawn(async move {
            let _exit = NodeSenderTaskExit(stopped_tx, task_ingress);
            let _ = started_tx.send(());
            let _ = finish_rx.await;
        });
        let sender = NodeSender {
            observation_timing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            data_tx,
            control_tx,
            signals_tx,
            ingress,
            abort: task.abort_handle(),
            stopped,
            drain_completed,
            configured_ack_durability: ReplicationAckDurability::MemoryApplied,
            stop_lock: Arc::new(Mutex::new(())),
        };
        started_rx.await.unwrap();
        assert!(!task.is_finished());

        sender.send(SenderMsg::Enqueue(op("queued"))).unwrap();
        // This command is shed only after a persistent reset generation is
        // published. The caller does not block and the data lane stays at one.
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        sender
            .send(SenderMsg::Enqueue(op("coalesced-overflow")))
            .unwrap();
        assert_eq!(data_rx.len(), 1);
        assert_eq!(signals_rx.borrow().overflow_generation, 2);
        assert!(!sender.ingress_is_idle());

        sender
            .send(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 1,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();
        assert_eq!(control_rx.len(), 1);
        assert_eq!(
            sender.send(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 2,
                durability: ReplicationAckDurability::MemoryApplied,
            }),
            Err(SenderIngressError::Full)
        );

        let (old_peer, _old_rx) = mpsc::channel(1);
        let old_peer = ReplicationPeer::new(old_peer);
        let (new_peer, _new_rx) = mpsc::channel(1);
        let new_peer = ReplicationPeer::new(new_peer);
        sender
            .send(SenderMsg::PeerConnected {
                peer: old_peer.clone(),
            })
            .unwrap();
        sender
            .send(SenderMsg::PeerConnected {
                peer: new_peer.clone(),
            })
            .unwrap();
        sender
            .send(SenderMsg::PeerDisconnected {
                connection_id: old_peer.connection_id(),
            })
            .unwrap();
        sender.send(SenderMsg::ReconcileNow).unwrap();
        let signals = signals_rx.borrow().clone();
        assert!(signals
            .peer
            .is_some_and(|current| current.id() == new_peer.id()));
        assert_eq!(signals.reconcile_generation, 1);

        assert_eq!(data_rx.recv().await.unwrap().key, "queued");
        assert!(matches!(
            control_rx.recv().await,
            Some(RetainedControl {
                msg: SenderMsg::Ack { .. },
                ..
            })
        ));
        let pending_before_closed_send = sender.ingress.pending_data.load(Ordering::Acquire);
        let overflow_before_closed_send = sender.signals_tx.borrow().overflow_generation;
        drop(data_rx);
        assert_eq!(
            sender.send_data(op("closed-data")),
            Err(SenderIngressError::Closed)
        );
        assert_eq!(
            sender.ingress.pending_data.load(Ordering::Acquire),
            pending_before_closed_send
        );
        assert_eq!(
            sender.signals_tx.borrow().overflow_generation,
            overflow_before_closed_send
        );
        drop(control_rx);
        assert_eq!(
            sender.send_control(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 3,
                durability: ReplicationAckDurability::MemoryApplied,
            }),
            Err(SenderIngressError::Closed)
        );
        finish_tx.send(()).unwrap();
        task.await.unwrap();
        sender.stop().await;
        assert_eq!(
            sender.send(SenderMsg::ReconcileNow),
            Err(SenderIngressError::Closed)
        );
    }

    #[tokio::test]
    async fn placement_stable_drain_barrier_rejects_store_committed_ack() {
        let (data_tx, _data_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
        let ingress = Arc::new(NodeSenderIngress::default());
        let (stopped_tx, stopped) = watch::channel(false);
        let (_drain_completed_tx, drain_completed) = watch::channel(0u64);
        let (finish_tx, finish_rx) = oneshot::channel();
        let task_ingress = ingress.clone();
        let task = tokio::spawn(async move {
            let _exit = NodeSenderTaskExit(stopped_tx, task_ingress);
            let _ = finish_rx.await;
        });
        let sender = NodeSender {
            observation_timing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            data_tx,
            control_tx,
            signals_tx,
            ingress,
            abort: task.abort_handle(),
            stopped,
            drain_completed,
            configured_ack_durability: ReplicationAckDurability::MemoryApplied,
            stop_lock: Arc::new(Mutex::new(())),
        };

        let drain = sender
            .request_drain_at(ReplicationAckDurability::StableCommitted)
            .unwrap();
        let request = signals_rx.borrow().drain.expect("drain signal missing");
        assert_eq!(request.generation, drain.generation);
        assert_eq!(
            request.required_ack_durability,
            ReplicationAckDurability::StableCommitted
        );

        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        let mut connection = ActiveConnection {
            peer_tx: SenderPeer::new(
                ReplicationPeer::new(peer_tx),
                &SenderRuntimeOptions::default(),
            ),
            session_id: 7,
            session_ready: true,
            next_batch_seq: 0,
            in_flight: VecDeque::new(),
        };
        let purpose = ReconcilePurpose::Drain {
            generation: request.generation,
            required_ack_durability: request.required_ack_durability,
        };
        let batch_seq = send_reconcile_barrier(
            FragmentId(0),
            purpose.required_ack_durability(sender.configured_ack_durability),
            &mut connection,
        )
        .await
        .unwrap();
        let frame = peer_rx.recv().await.expect("stable barrier missing");
        let batch = match Envelope::decode(frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected stable ReplicateBatch barrier, got {other:?}"),
        };
        assert!(batch.ops.is_empty());
        assert_eq!(
            ReplicationAckLevel::try_from(batch.required_ack_level),
            Ok(ReplicationAckLevel::StableCommitted)
        );

        assert!(matches!(
            acknowledge_in_flight(
                &mut connection,
                batch_seq,
                ReplicationAckDurability::StoreCommitted,
            ),
            InflightAckResult::BelowRequired {
                required: ReplicationAckDurability::StableCommitted,
            }
        ));
        assert_eq!(connection.in_flight.len(), 1);
        assert!(!drain.is_complete());
        assert!(matches!(
            acknowledge_in_flight(
                &mut connection,
                batch_seq,
                ReplicationAckDurability::StableCommitted,
            ),
            InflightAckResult::Accepted {
                kind: InflightKind::ReconcileBarrier,
                ..
            }
        ));
        assert!(connection.in_flight.is_empty());

        finish_tx.send(()).unwrap();
        task.await.unwrap();
        sender.stop().await;
    }

    #[tokio::test]
    async fn completed_drain_suppresses_routine_tick_until_newer_drain_rearms() {
        let first = NodeSenderDrainRequest {
            generation: 1,
            required_ack_durability: ReplicationAckDurability::StableCommitted,
        };
        let mut drain_state = SenderDrainState::default();
        drain_state.request(first);
        drain_state.mark_prepared(first.generation);
        drain_state.complete();

        let consistency_interval = Duration::from_millis(10);
        let started = Instant::now();
        let mut consistency_timer =
            tokio::time::interval_at(started + consistency_interval, consistency_interval);
        consistency_timer.tick().await;
        assert!(Instant::now().duration_since(started) >= consistency_interval);

        let (peer_tx, mut peer_rx) = mpsc::channel(2);
        let connection = ActiveConnection {
            peer_tx: SenderPeer::new(
                ReplicationPeer::new(peer_tx),
                &SenderRuntimeOptions::default(),
            ),
            session_id: 41,
            session_ready: true,
            next_batch_seq: 0,
            in_flight: VecDeque::new(),
        };
        let mut reconcile_ticket = None;
        let mut next_round_id = 0;
        let mut active_reconcile = None;
        if drain_state.routine_reconcile_allowed() {
            active_reconcile = begin_reconcile(
                FragmentId(0),
                ReconcilePurpose::Routine,
                &connection,
                &mut reconcile_ticket,
                &mut next_round_id,
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        }
        assert!(active_reconcile.is_none());
        assert!(peer_rx.try_recv().is_err());

        let newer = NodeSenderDrainRequest {
            generation: 2,
            required_ack_durability: ReplicationAckDurability::StableCommitted,
        };
        drain_state.request(newer);
        assert!(drain_state.prepared_purpose().is_none());
        drain_state.mark_prepared(newer.generation);
        let purpose = drain_state
            .prepared_purpose()
            .expect("newer drain did not re-arm reconciliation");
        let round = begin_reconcile(
            FragmentId(0),
            purpose,
            &connection,
            &mut reconcile_ticket,
            &mut next_round_id,
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("newer drain did not start a round");
        assert_eq!(round.purpose, purpose);

        let frame = peer_rx.recv().await.expect("new drain request missing");
        let request = match Envelope::decode(frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => request,
            other => panic!("expected ReconcileRequest, got {other:?}"),
        };
        assert_eq!(request.session_id, connection.session_id);
        assert_eq!(request.round_id, round.round_id);
        assert_eq!(request.page_seq, 0);
    }

    #[tokio::test]
    async fn node_sender_delivers_each_sideband_field_across_unrelated_updates() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
        let ingress = Arc::new(NodeSenderIngress::default());
        let (first_peer, _first_peer_rx) = mpsc::channel(1);
        let first_peer = ReplicationPeer::new(first_peer);
        let (latest_peer, _latest_peer_rx) = mpsc::channel(1);
        let latest_peer = ReplicationPeer::new(latest_peer);
        let mut inbox = SenderInbox {
            data_rx,
            control_rx,
            signals_rx,
            delivered_overflow_generation: 0,
            delivered_reconcile_generation: 0,
            delivered_drain_generation: 0,
            delivered_peer_generation: 0,
            ingress: ingress.clone(),
        };

        signals_tx.send_modify(|signals| {
            signals.overflow_generation = 1;
            signals.peer_generation = 1;
            signals.peer = Some(first_peer);
            signals.reconcile_generation = 1;
        });
        assert!(matches!(
            inbox.recv().await,
            SenderInput::Overflow(1, event_ingress) if Arc::ptr_eq(&event_ingress, &ingress)
        ));

        // Updating only the peer after overflow delivery must not erase the
        // still-undelivered reconcile generation from the prior snapshot.
        signals_tx.send_modify(|signals| {
            signals.peer_generation = 2;
            signals.peer = Some(latest_peer.clone());
        });
        assert!(matches!(
            inbox.recv().await,
            SenderInput::PeerChanged(Some(peer)) if peer.id() == latest_peer.id()
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), inbox.recv()).await,
            Ok(SenderInput::ReconcileNow)
        ));
    }

    #[tokio::test]
    async fn node_sender_observes_signal_lane_close_and_reports_task_exit() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
        let mut inbox = SenderInbox {
            data_rx,
            control_rx,
            signals_rx,
            delivered_overflow_generation: 0,
            delivered_reconcile_generation: 0,
            delivered_drain_generation: 0,
            delivered_peer_generation: 0,
            ingress: Arc::new(NodeSenderIngress::default()),
        };
        drop(signals_tx);
        assert!(matches!(inbox.recv().await, SenderInput::Closed));
        drop(data_tx);
        drop(control_tx);

        let sender = spawn_node_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: state_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let mut stopped = sender.stopped.clone();
        drop(sender);
        tokio::time::timeout(Duration::from_secs(1), stopped.changed())
            .await
            .expect("sender task did not exit after every ingress handle closed")
            .expect("sender task dropped its exit signal without publishing completion");
        assert!(*stopped.borrow());
    }

    #[tokio::test]
    async fn full_node_data_ingress_forces_buffer_reset() {
        let report = state_report();
        let sender = spawn_node_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                buffer_capacity: 1,
                batch_interval: Duration::from_secs(3600),
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default()
                .with_queue_admission_timeout(Duration::from_millis(10))
                .with_control_channel_capacity(1),
        )
        .unwrap();

        sender.send(SenderMsg::Enqueue(op("queued"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if report.lock().await.state == "buffer_reset" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("overflow signal never reached the sender state machine");
        assert!(!sender.ingress_is_idle());
        let drain = sender.request_drain().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if report.lock().await.state != "buffer_reset" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain did not subsume the pending overflow reset");
        assert!(!drain.is_complete());
        assert!(
            !sender.ingress_is_idle(),
            "drain preparation published overflow settlement before remote proof"
        );
        sender.stop().await;
    }

    #[tokio::test]
    async fn standalone_sender_reports_owned_buffer_overflow_until_recovery_is_certified() {
        let report = state_report();
        let sender = spawn_bounded_sender(
            FragmentId(0),
            "peer".into(),
            SenderConfig::builder().buffer_capacity(1).build(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        sender.send(SenderMsg::Enqueue(op("buffered"))).unwrap();
        wait_for_sender_report(&report, |state| state.buffered == 1).await;
        assert_eq!(sender.ingress_diagnostics().reset_requests, 0);
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        wait_for_sender_report(&report, |state| state.state == "buffer_reset").await;
        let diagnostics = sender.ingress_diagnostics();
        assert_eq!(diagnostics.reset_requests, 1);
        assert_eq!(diagnostics.shed_operations, 1);
        assert!(diagnostics.recovery_pending);
        assert_eq!(diagnostics.retained_bytes, 0);
        sender.stop().await;
        assert!(sender.ingress_diagnostics().recovery_pending);
    }

    #[tokio::test]
    async fn standalone_sender_admission_is_bounded_and_controls_are_independent() {
        let sender = spawn_bounded_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig::builder().buffer_capacity(1).build(),
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: state_report(),
            },
            SenderRuntimeOptions::default().with_control_channel_capacity(1),
        )
        .unwrap();
        sender.send(SenderMsg::Enqueue(op("queued"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        sender
            .send(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 1,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();
        assert_eq!(
            sender.send(SenderMsg::Ack {
                session_id: 1,
                batch_seq: 2,
                durability: ReplicationAckDurability::MemoryApplied,
            }),
            Err(SenderIngressError::Full)
        );
        let diagnostics = sender.ingress_diagnostics();
        assert_eq!(diagnostics.pending_operations, 1);
        assert_eq!(diagnostics.queued_controls, 1);
        assert_eq!(diagnostics.shed_operations, 1);
        assert_eq!(diagnostics.reset_requests, 1);
        assert!(diagnostics.recovery_pending);
        assert!(!diagnostics.closed);
        sender.stop().await;
        let stopped = sender.ingress_diagnostics();
        assert_eq!(stopped.pending_operations, 0);
        assert_eq!(stopped.queued_controls, 0);
        assert_eq!(stopped.retained_bytes, 0);
        assert!(stopped.closed);
        assert!(
            stopped.recovery_pending,
            "local stop cannot certify a reset"
        );
        assert_eq!(
            sender.send(SenderMsg::Enqueue(op("after-stop"))),
            Err(SenderIngressError::Closed)
        );
    }

    #[test]
    fn requeue_in_flight_preserves_batches_ahead_of_newer_buffered_ops() {
        let mut in_flight = VecDeque::from([
            InflightBatch {
                batch_seq: 0,
                ops: vec![retained(op("a")), retained(op("b"))],
                kind: InflightKind::Streaming {
                    ack_deadline: Instant::now() + Duration::from_secs(1),
                },
                required_ack_durability: ReplicationAckDurability::MemoryApplied,
            },
            InflightBatch {
                batch_seq: 1,
                ops: vec![retained(op("c"))],
                kind: InflightKind::Streaming {
                    ack_deadline: Instant::now() + Duration::from_secs(1),
                },
                required_ack_durability: ReplicationAckDurability::MemoryApplied,
            },
        ]);
        let mut buffer = VecDeque::from([retained(op("d"))]);
        requeue_in_flight(&mut in_flight, &mut buffer);
        assert!(in_flight.is_empty());
        assert_eq!(
            buffer.into_iter().map(|op| op.op.key).collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
    }

    #[test]
    fn batch_sequence_allocator_never_emits_reserved_repair_sentinel() {
        let (tx, _rx) = mpsc::channel(1);
        let mut connection = ActiveConnection {
            peer_tx: SenderPeer::new(ReplicationPeer::new(tx), &SenderRuntimeOptions::default()),
            session_id: 1,
            session_ready: true,
            next_batch_seq: u64::MAX - 1,
            in_flight: VecDeque::new(),
        };
        assert_eq!(allocate_batch_seq(&mut connection), u64::MAX - 1);
        assert_eq!(allocate_batch_seq(&mut connection), 0);
        assert_eq!(allocate_batch_seq(&mut connection), 1);
        assert_eq!(next_generation(u64::MAX), 1);
    }

    #[test]
    fn invalid_sender_options_are_rejected() {
        let defaults = SenderRuntimeOptions::default();
        assert_eq!(defaults.ack_timeout(), Duration::from_secs(30));
        assert_eq!(defaults.queue_admission_timeout(), Duration::from_secs(5));
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_ack_timeout(Duration::ZERO)
                .validate(),
            Err(SenderOptionsError::ZeroAckTimeout)
        );
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_queue_admission_timeout(Duration::ZERO)
                .validate(),
            Err(SenderOptionsError::ZeroQueueAdmissionTimeout)
        );
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_ack_timeout(Duration::MAX)
                .validate(),
            Err(SenderOptionsError::AckTimeoutTooLarge)
        );
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_queue_admission_timeout(Duration::MAX)
                .validate(),
            Err(SenderOptionsError::QueueAdmissionTimeoutTooLarge)
        );
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_control_channel_capacity(0)
                .validate(),
            Err(SenderOptionsError::ZeroControlChannelCapacity)
        );
        assert_eq!(
            SenderRuntimeOptions::default()
                .with_control_channel_capacity(
                    tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1),
                )
                .validate(),
            Err(SenderOptionsError::ControlChannelCapacityTooLarge)
        );
        assert_eq!(
            SenderConfig {
                batch_size: 0,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroBatchSize)
        );
        assert_eq!(
            SenderConfig {
                batch_interval: Duration::ZERO,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroBatchInterval)
        );
        assert_eq!(
            SenderConfig {
                max_in_flight: 0,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroMaxInFlight)
        );
        assert_eq!(
            SenderConfig {
                buffer_capacity: 0,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroBufferCapacity)
        );
        let oversized_capacity = SenderConfig {
            buffer_capacity: tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1),
            ..SenderConfig::default()
        };
        assert_eq!(
            oversized_capacity.validate(),
            Err(SenderOptionsError::BufferCapacityTooLarge)
        );
        assert_eq!(
            SenderConfig {
                consistency_interval: Duration::ZERO,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroConsistencyInterval)
        );
        assert_eq!(
            SenderConfig {
                reconcile_timeout: Duration::ZERO,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ZeroReconcileTimeout)
        );
        assert_eq!(
            SenderConfig {
                batch_interval: Duration::MAX,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::BatchIntervalTooLarge)
        );
        assert_eq!(
            SenderConfig {
                consistency_interval: Duration::MAX,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ConsistencyIntervalTooLarge)
        );
        assert_eq!(
            SenderConfig {
                reconcile_timeout: Duration::MAX,
                ..SenderConfig::default()
            }
            .validate(),
            Err(SenderOptionsError::ReconcileTimeoutTooLarge)
        );
    }

    #[tokio::test]
    async fn replication_queue_timeout_reports_failure_without_admitting_frame() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(vec![1]).await.unwrap();
        let failed = Arc::new(AtomicBool::new(false));
        let observed = failed.clone();
        let options = SenderRuntimeOptions::default()
            .with_queue_admission_timeout(Duration::from_millis(10))
            .with_queue_failure_handler(Arc::new(move |_| {
                observed.store(true, Ordering::SeqCst);
            }));
        let peer = SenderPeer::new(ReplicationPeer::new(tx), &options);
        assert!(
            !send_envelope(
                &peer,
                None,
                EnvelopeMsg::SessionStart(SessionStart {
                    session_id: 1,
                    fragment: 0,
                }),
            )
            .await
        );
        assert!(failed.load(Ordering::SeqCst));
        assert_eq!(rx.recv().await, Some(vec![1]));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn node_replication_producers_share_one_raii_byte_budget() {
        let msg = || {
            EnvelopeMsg::SessionStart(SessionStart {
                session_id: 7,
                fragment: 0,
            })
        };
        let encoded_len = Envelope { msg: Some(msg()) }.encoded_len();
        let (frame_tx, mut frame_rx) = mpsc::channel(4);
        let (response_tx, _response_rx) = mpsc::channel(4);
        let identity = crate::network::PeerConnectionId::new();
        let budget = Arc::new(tokio::sync::Semaphore::new(encoded_len * 2));
        let producer_budget = Arc::new(tokio::sync::Semaphore::new(encoded_len * 2));
        let outbound = BudgetedPeerTx {
            node_byte_budget: crate::wire::NodePeerByteBudget::new(
                crate::wire::NODE_PEER_QUEUE_BYTE_CAPACITY,
            ),
            tx: frame_tx,
            response_tx,
            byte_budget: budget.clone(),
            producer_byte_budget: producer_budget,
            reconcile_gate: crate::wire::ReconcileGate::new(),
            connection_id: identity.clone(),
        };
        let options =
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(10));
        let first = SenderPeer::new(ReplicationPeer::from_budgeted(outbound.clone()), &options);
        let second = SenderPeer::new(ReplicationPeer::from_budgeted(outbound), &options);

        assert!(send_envelope(&first, None, msg()).await);
        assert!(send_envelope(&second, None, msg()).await);
        assert_eq!(budget.available_permits(), 0);
        assert!(!send_envelope(&first, None, msg()).await);
        assert_eq!(frame_rx.len(), 2);

        drop(frame_rx.recv().await.unwrap());
        assert_eq!(budget.available_permits(), encoded_len);
        assert!(send_envelope(&second, None, msg()).await);
    }

    #[tokio::test(start_paused = true)]
    async fn replication_peers_share_aggregate_encoded_body_admission() {
        let physical_max = crate::wire::MAX_FRAME_BYTES;
        let budget = crate::wire::NodePeerByteBudget::new(2 * physical_max);
        let msg = EnvelopeMsg::SessionStart(SessionStart {
            session_id: 7,
            fragment: 0,
        });
        let bytes = Envelope {
            msg: Some(msg.clone()),
        }
        .encoded_len();
        let held = budget
            .acquire_producer(
                (physical_max - bytes) as u32,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        let make_peer = || {
            let (tx, rx) = mpsc::channel(4);
            let (response_tx, _response_rx) = mpsc::channel(4);
            let identity = crate::network::PeerConnectionId::new();
            let outbound = BudgetedPeerTx {
                tx,
                response_tx,
                connection_id: identity.clone(),
                byte_budget: Arc::new(tokio::sync::Semaphore::new(4096)),
                producer_byte_budget: Arc::new(tokio::sync::Semaphore::new(4096)),
                node_byte_budget: budget.clone(),
                reconcile_gate: crate::wire::ReconcileGate::new(),
            };
            (
                SenderPeer::new(
                    ReplicationPeer::from_budgeted(outbound),
                    &SenderRuntimeOptions::default(),
                ),
                rx,
            )
        };
        let (a, mut a_data) = make_peer();
        let (b, mut b_data) = make_peer();
        assert!(send_envelope(&a, None, msg.clone()).await);
        let mut blocked = Box::pin(send_envelope(&b, None, msg));
        use std::future::Future;
        std::future::poll_fn(|cx| {
            assert!(blocked.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(b_data.len(), 0);
        drop(a_data.recv().await.unwrap());
        assert!(blocked.await);
        assert!(budget.diagnostics().saturation_events() > 0);
        drop(b_data.recv().await.unwrap());
        drop(held);
        assert_eq!(budget.diagnostics().admitted_bytes(), 0);
    }

    #[tokio::test]
    async fn node_reconciliation_rounds_share_one_connection_credit() {
        let (frame_tx, mut frame_rx) = mpsc::channel(4);
        let (response_tx, _response_rx) = mpsc::channel(4);
        let identity = crate::network::PeerConnectionId::new();
        let reconcile_gate = crate::wire::ReconcileGate::new();
        let outbound = BudgetedPeerTx {
            node_byte_budget: crate::wire::NodePeerByteBudget::new(
                crate::wire::NODE_PEER_QUEUE_BYTE_CAPACITY,
            ),
            tx: frame_tx,
            response_tx,
            byte_budget: Arc::new(tokio::sync::Semaphore::new(4096)),
            producer_byte_budget: Arc::new(tokio::sync::Semaphore::new(4096)),
            reconcile_gate: reconcile_gate.clone(),
            connection_id: identity.clone(),
        };
        let peer = SenderPeer::new(
            ReplicationPeer::from_budgeted(outbound),
            &SenderRuntimeOptions::default(),
        );
        let connection = ActiveConnection {
            peer_tx: peer,
            session_id: 7,
            session_ready: true,
            next_batch_seq: 0,
            in_flight: VecDeque::new(),
        };
        let mut first_round_id = 0;
        let mut first_ticket = None;
        let first = begin_reconcile(
            FragmentId(0),
            ReconcilePurpose::Routine,
            &connection,
            &mut first_ticket,
            &mut first_round_id,
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("first round did not acquire the peer credit");
        assert_eq!(reconcile_gate.available_permits(), 0);

        let mut second_round_id = 0;
        let mut second_ticket = None;
        assert!(
            begin_reconcile(
                FragmentId(1),
                ReconcilePurpose::Routine,
                &connection,
                &mut second_ticket,
                &mut second_round_id,
                Duration::from_secs(1),
            )
            .await
            .unwrap()
            .is_none(),
            "a second fragment bypassed the physical-connection credit"
        );

        drop(first);
        assert_eq!(reconcile_gate.available_permits(), 1);

        let mut incumbent_ticket = None;
        assert!(
            begin_reconcile(
                FragmentId(0),
                ReconcilePurpose::Routine,
                &connection,
                &mut incumbent_ticket,
                &mut first_round_id,
                Duration::from_secs(1),
            )
            .await
            .unwrap()
            .is_none(),
            "the completing fragment stole credit from an existing waiter"
        );
        let second = begin_reconcile(
            FragmentId(1),
            ReconcilePurpose::Routine,
            &connection,
            &mut second_ticket,
            &mut second_round_id,
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("the oldest waiter did not receive the released credit");
        assert_eq!(reconcile_gate.available_permits(), 0);
        drop(second);
        let incumbent = begin_reconcile(
            FragmentId(0),
            ReconcilePurpose::Routine,
            &connection,
            &mut incumbent_ticket,
            &mut first_round_id,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(incumbent.is_some());
        assert_eq!(frame_rx.len(), 3);
        while frame_rx.recv().await.is_some() {
            if frame_rx.is_empty() {
                break;
            }
        }
    }

    #[tokio::test]
    async fn live_overflow_handles_reset_reconcile_admission_failure() {
        let report = state_report();
        let sender = spawn_bounded_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(10)),
        )
        .unwrap();
        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender
            .send(SenderMsg::PeerConnected {
                peer: ReplicationPeer::new(peer_tx),
            })
            .unwrap();
        wait_for_sender_report(&report, |state| state.state == "connected").await;
        peer_rx.recv().await.expect("initial SessionStart missing");

        sender.send(SenderMsg::Enqueue(op("first"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        let reset = wait_for_sender_report(&report, |state| state.state == "buffer_reset").await;
        assert_ne!(reset.session_id, 0);
        let frame = peer_rx.recv().await.expect("reset SessionStart missing");
        match Envelope::decode(frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => {
                assert_eq!(session.session_id, reset.session_id);
            }
            other => panic!("expected reset SessionStart, got {other:?}"),
        }
        assert!(peer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn live_overflow_handles_reset_session_start_admission_failure() {
        let report = state_report();
        let sender = spawn_bounded_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(10)),
        )
        .unwrap();
        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender
            .send(SenderMsg::PeerConnected {
                peer: ReplicationPeer::new(peer_tx),
            })
            .unwrap();
        let connected = wait_for_sender_report(&report, |state| state.state == "connected").await;

        // Leave the initial SessionStart queued so the reset SessionStart
        // itself reaches the admission deadline while the transport stays live.
        sender.send(SenderMsg::Enqueue(op("first"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        let reset = wait_for_sender_report(&report, |state| state.state == "buffer_reset").await;
        assert_ne!(reset.session_id, 0);
        let frame = peer_rx.recv().await.expect("initial SessionStart missing");
        match Envelope::decode(frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => {
                assert_eq!(session.session_id, connected.session_id);
            }
            other => panic!("expected initial SessionStart, got {other:?}"),
        }
        assert!(peer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_replication_deadline_never_admits_to_an_empty_queue() {
        let (tx, mut rx) = mpsc::channel(1);
        let peer = SenderPeer::new(ReplicationPeer::new(tx), &SenderRuntimeOptions::default());
        let expired = Instant::now();
        tokio::task::yield_now().await;
        assert!(
            !send_envelope(
                &peer,
                Some(expired),
                EnvelopeMsg::SessionStart(SessionStart {
                    session_id: 1,
                    fragment: 0,
                }),
            )
            .await
        );
        assert!(rx.try_recv().is_err());
    }
}
