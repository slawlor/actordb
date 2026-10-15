// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Phase 5: async sender/receiver replication.
//!
//! Replication moves off the write path. Per fragment, per replication
//! peer, a `ReplicationSender` (the task spawned by `spawn_sender`) task owns a state machine:
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
//!   recovery signals. The public standalone constructors retain their
//!   source-compatible unbounded command channel. The sender ships batches on
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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ractor::rpc::CallResult;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::actor::{ApplyDurability, HlcClock, KvMessage, OpPriority, VersionedValue};
use crate::node::FragmentRouter;
use crate::proto::{
    Envelope, EnvelopeMsg, KeySummary, ReconcileFetch, ReconcileRequest, ReplicateBatch,
    ReplicateOp as ProtoReplicateOp, ReplicationAckLevel, SessionStart, SyncEntry,
};
use crate::shard::FragmentId;

/// Prevent a permanently failing receiver store from becoming a tight
/// NACK/write/log loop. Full retry policy belongs with ACK timeouts and bounded
/// ingress; this floor makes the current explicit retry operationally safe.
const NACK_RETRY_DELAY: Duration = Duration::from_secs(1);

fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

type QueueFailureHandler = Arc<dyn Fn(mpsc::Sender<Vec<u8>>) + Send + Sync>;

/// Runtime-only liveness policy for a replication sender.
///
/// These settings live outside [`SenderConfig`] so existing exhaustive
/// `SenderConfig` literals remain source-compatible. Use
/// [`spawn_sender_with_options`] to override the defaults.
#[derive(Clone)]
pub struct SenderRuntimeOptions {
    ack_timeout: Duration,
    queue_admission_timeout: Duration,
    control_channel_capacity: usize,
    queue_failure_handler: Option<QueueFailureHandler>,
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
        Self {
            ack_timeout: Duration::from_secs(30),
            queue_admission_timeout: Duration::from_secs(5),
            control_channel_capacity: 64,
            queue_failure_handler: None,
        }
    }
}

/// Invalid sender configuration passed to [`spawn_sender_with_options`].
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
    /// The Node mutation lane exceeds Tokio's supported bound.
    BufferCapacityTooLarge,
    /// Periodic reconciliation would spin continuously.
    ZeroConsistencyInterval,
    /// A reconciliation phase would expire immediately.
    ZeroReconcileTimeout,
    /// An ordinary batch would expire immediately.
    ZeroAckTimeout,
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

    pub(crate) fn with_queue_failure_handler(mut self, handler: QueueFailureHandler) -> Self {
        self.queue_failure_handler = Some(handler);
        self
    }

    fn validate(&self) -> Result<(), SenderOptionsError> {
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

/// Tuning knobs for the replication sender. All fields are public so
/// embedders can tune; `Default` is sane for tests and small clusters.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Max ops per batch.
    pub batch_size: usize,
    /// Ship a batch at least this often when the buffer is non-empty.
    pub batch_interval: Duration,
    /// Max batches in flight awaiting acks.
    pub max_in_flight: usize,
    /// Max buffered (unshipped) ops per sender. A [`crate::node::Node`] also
    /// uses this as the capacity of the preceding local-mutation ingress lane.
    /// Either overflow sheds queued history and enters `BufferReset`; recovery
    /// reads the authoritative fragment snapshot.
    pub buffer_capacity: usize,
    /// How often a connected sender runs anti-entropy reconciliation
    /// to repair divergence.
    pub consistency_interval: Duration,
    /// Maximum time to wait for one network reconciliation phase
    /// (summary response, fetched-data chunk, or durability barrier ACK).
    /// A timeout rotates the session and retries the round, so a connected
    /// but silent peer cannot strand bootstrap or overflow recovery forever.
    pub reconcile_timeout: Duration,
    /// Minimum receiver fence that may retire an in-flight batch.
    /// `MemoryApplied` accepts either acknowledgement level;
    /// `StoreCommitted` requires a receiver with a configured store.
    pub required_ack_durability: ReplicationAckDurability,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_interval: Duration::from_millis(100),
            max_in_flight: 4,
            buffer_capacity: 10_000,
            consistency_interval: Duration::from_secs(300),
            reconcile_timeout: Duration::from_secs(30),
            required_ack_durability: ReplicationAckDurability::MemoryApplied,
        }
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
        if self.buffer_capacity == 0 {
            return Err(SenderOptionsError::ZeroBufferCapacity);
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

    pub(crate) fn validate_node_ingress(&self) -> Result<(), SenderOptionsError> {
        if self.buffer_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(SenderOptionsError::BufferCapacityTooLarge);
        }
        Ok(())
    }
}

/// Messages driving a `ReplicationSender` (the task spawned by `spawn_sender`).
pub enum SenderMsg {
    /// An already-applied mutation to replicate. Buffer-reset recovery reads
    /// the authoritative fragment snapshot, so callers must not enqueue an
    /// operation before its local merge has completed.
    Enqueue(crate::actor::ReplicatedOp),
    /// The peer (re)connected; start a new session on this channel.
    PeerConnected { peer_tx: mpsc::Sender<Vec<u8>> },
    /// One peer connection dropped. Ignore it if a newer channel has already
    /// replaced it; otherwise retain the buffer for reconnect.
    PeerDisconnected { peer_tx: mpsc::Sender<Vec<u8>> },
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
        entries: Vec<SyncEntry>,
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
    peer_generation: u64,
    peer_tx: Option<mpsc::Sender<Vec<u8>>>,
}

#[derive(Default)]
struct NodeSenderIngress {
    pending_data: AtomicUsize,
    settled_overflow_generation: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeSenderSendError {
    Full,
    Closed,
}

/// Node-owned bounded ingress for one replication sender.
///
/// The public standalone sender constructors intentionally keep returning
/// their original unbounded Tokio sender for source compatibility. Nodes use
/// this handle instead: locally applied mutations enter a bounded data lane,
/// protocol responses enter a separate bounded control lane, and coalescible
/// lifecycle/recovery signals remain available even when either lane is full.
#[derive(Clone)]
pub(crate) struct NodeSender {
    data_tx: mpsc::Sender<crate::actor::ReplicatedOp>,
    control_tx: mpsc::Sender<SenderMsg>,
    signals_tx: watch::Sender<NodeSenderSignals>,
    ingress: Arc<NodeSenderIngress>,
    abort: AbortHandle,
    stopped: watch::Receiver<bool>,
    stop_lock: Arc<Mutex<()>>,
}

impl NodeSender {
    pub(crate) fn same_task(&self, other: &Self) -> bool {
        self.data_tx.same_channel(&other.data_tx)
    }

    #[cfg(test)]
    pub(crate) fn control_capacity(&self) -> usize {
        self.control_tx.capacity()
    }

    pub(crate) fn send(&self, msg: SenderMsg) -> Result<(), NodeSenderSendError> {
        if self.is_closed() {
            return Err(NodeSenderSendError::Closed);
        }
        match msg {
            SenderMsg::Enqueue(op) => self.send_data(op),
            SenderMsg::PeerConnected { peer_tx } => {
                self.signals_tx.send_if_modified(|signals| {
                    if signals
                        .peer_tx
                        .as_ref()
                        .map_or(false, |current| current.same_channel(&peer_tx))
                    {
                        return false;
                    }
                    signals.peer_generation = next_generation(signals.peer_generation);
                    signals.peer_tx = Some(peer_tx);
                    true
                });
                Ok(())
            }
            SenderMsg::PeerDisconnected { peer_tx } => {
                self.signals_tx.send_if_modified(|signals| {
                    if !signals
                        .peer_tx
                        .as_ref()
                        .map_or(false, |current| current.same_channel(&peer_tx))
                    {
                        return false;
                    }
                    signals.peer_generation = next_generation(signals.peer_generation);
                    signals.peer_tx = None;
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

    fn send_data(&self, op: crate::actor::ReplicatedOp) -> Result<(), NodeSenderSendError> {
        self.ingress.pending_data.fetch_add(1, Ordering::AcqRel);
        match self.data_tx.try_send(op) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.ingress.pending_data.fetch_sub(1, Ordering::AcqRel);
                self.schedule_reset();
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.ingress.pending_data.fetch_sub(1, Ordering::AcqRel);
                Err(NodeSenderSendError::Closed)
            }
        }
    }

    fn send_control(&self, control: SenderMsg) -> Result<(), NodeSenderSendError> {
        match self.control_tx.try_send(control) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(NodeSenderSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(NodeSenderSendError::Closed),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.abort.is_finished() || self.data_tx.is_closed() || self.control_tx.is_closed()
    }

    pub(crate) fn schedule_reset(&self) {
        self.signals_tx.send_modify(|signals| {
            signals.overflow_generation = next_generation(signals.overflow_generation);
        });
    }

    /// True only after every admitted data item has been reflected in a
    /// published sender state and every coalesced overflow has completed its
    /// correlated reset reconciliation.
    pub(crate) fn ingress_is_idle(&self) -> bool {
        let issued = self.signals_tx.borrow().overflow_generation;
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
    }
}

enum SenderInput {
    Message(SenderMsg),
    Data(crate::actor::ReplicatedOp, Arc<NodeSenderIngress>),
    Overflow(u64, Arc<NodeSenderIngress>),
    PeerChanged(Option<mpsc::Sender<Vec<u8>>>),
    ReconcileNow,
    Closed,
}

enum SenderInbox {
    Legacy(mpsc::UnboundedReceiver<SenderMsg>),
    Node {
        data_rx: mpsc::Receiver<crate::actor::ReplicatedOp>,
        control_rx: mpsc::Receiver<SenderMsg>,
        signals_rx: watch::Receiver<NodeSenderSignals>,
        delivered_overflow_generation: u64,
        delivered_reconcile_generation: u64,
        delivered_peer_generation: u64,
        ingress: Arc<NodeSenderIngress>,
    },
}

impl SenderInbox {
    async fn recv(&mut self) -> SenderInput {
        match self {
            Self::Legacy(rx) => rx
                .recv()
                .await
                .map(SenderInput::Message)
                .unwrap_or(SenderInput::Closed),
            Self::Node {
                data_rx,
                control_rx,
                signals_rx,
                delivered_overflow_generation,
                delivered_reconcile_generation,
                delivered_peer_generation,
                ingress,
            } => loop {
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
                    return SenderInput::PeerChanged(signals.peer_tx);
                }
                if signals.reconcile_generation != *delivered_reconcile_generation {
                    *delivered_reconcile_generation = signals.reconcile_generation;
                    return SenderInput::ReconcileNow;
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
                            .map(SenderInput::Message)
                            .unwrap_or(SenderInput::Closed);
                    }
                    data = data_rx.recv() => {
                        return data
                            .map(|op| SenderInput::Data(op, ingress.clone()))
                            .unwrap_or(SenderInput::Closed);
                    }
                }
            },
        }
    }
}

struct NodeSenderTaskExit(watch::Sender<bool>);

impl Drop for NodeSenderTaskExit {
    fn drop(&mut self) {
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
}

/// A batch awaiting its ack.
struct InflightBatch {
    batch_seq: u64,
    ops: Vec<crate::actor::ReplicatedOp>,
    kind: InflightKind,
}

#[derive(Clone, Copy)]
enum InflightKind {
    Streaming { ack_deadline: Instant },
    ReconcileBarrier,
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
    tx: mpsc::Sender<Vec<u8>>,
    queue_admission_timeout: Duration,
    queue_failure_handler: Option<QueueFailureHandler>,
}

impl SenderPeer {
    fn new(tx: mpsc::Sender<Vec<u8>>, options: &SenderRuntimeOptions) -> Self {
        Self {
            tx,
            queue_admission_timeout: options.queue_admission_timeout,
            queue_failure_handler: options.queue_failure_handler.clone(),
        }
    }

    fn same_channel(&self, other: &mpsc::Sender<Vec<u8>>) -> bool {
        self.tx.same_channel(other)
    }

    fn report_failure(&self) {
        if let Some(handler) = &self.queue_failure_handler {
            handler(self.tx.clone());
        }
    }

    fn must_disconnect_after_failure(&self) -> bool {
        self.tx.is_closed() || self.queue_failure_handler.is_some()
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
    Reset { epoch: u64 },
}

enum ReconcilePhase {
    Response,
    Data {
        remaining_chunks: VecDeque<Vec<String>>,
        next_chunk_seq: u64,
        expected_keys: HashSet<String>,
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
}

fn requeue_in_flight(
    in_flight: &mut VecDeque<InflightBatch>,
    buffer: &mut VecDeque<crate::actor::ReplicatedOp>,
) {
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
    buffer: &mut VecDeque<crate::actor::ReplicatedOp>,
) -> bool {
    if connection.as_ref().map_or(false, |connection| {
        !connection.peer_tx.must_disconnect_after_failure()
    }) {
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
    /// "disconnected", "connected", or "buffer_reset".
    pub state: String,
    /// Current session id (0 if none).
    pub session_id: u64,
    /// Ops currently buffered.
    pub buffered: usize,
    /// Batches in flight.
    pub in_flight: usize,
    /// Durability fence reported by the most recent well-formed ACK. This may
    /// be below `required_ack_durability`, in which case the batch remains in
    /// flight and the mismatch is observable here.
    pub last_ack_durability: Option<ReplicationAckDurability>,
    /// The most recent matching ACK was below the configured minimum and did
    /// not retire its batch.
    pub ack_durability_mismatch: bool,
    /// Completed anti-entropy reconcile rounds. The Phase 8 placement
    /// driver polls this to detect bootstrap completion.
    pub reconcile_rounds: u64,
}

/// Shared dependencies for a sender task.
pub struct SenderDeps {
    pub router: FragmentRouter,
    /// Node-wide HLC. Reconcile frames are observed here only after the sender
    /// has authenticated their exact session/round/phase correlation.
    pub hlc: Arc<Mutex<HlcClock>>,
    /// Legacy compatibility field. Ignored because tombstone collection no
    /// longer depends on transient sender reset state.
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
    /// Where the sender publishes its state.
    pub state_report: Arc<Mutex<SenderStateReport>>,
}

/// Spawn a replication sender for (`fragment`, `peer_id`).
///
/// This standalone compatibility API returns an unbounded command sender.
/// [`crate::node::Node`] uses an internal bounded ingress handle instead.
///
/// # Panics
///
/// Panics when `config` contains a zero or unrepresentable limit. Prefer
/// [`spawn_sender_with_options`] when configuration errors must be handled.
pub fn spawn_sender(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
) -> mpsc::UnboundedSender<SenderMsg> {
    spawn_sender_with_options(
        fragment,
        peer_id,
        config,
        deps,
        SenderRuntimeOptions::default(),
    )
    .expect("sender configuration must be valid")
}

/// Spawn a replication sender with explicit runtime liveness policy.
///
/// [`spawn_sender`] remains the source-compatible default entry point. This
/// variant validates both `config` and `options` before spawning the task and
/// retains the same unbounded command-channel return type.
pub fn spawn_sender_with_options(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    options: SenderRuntimeOptions,
) -> Result<mpsc::UnboundedSender<SenderMsg>, SenderOptionsError> {
    config.validate()?;
    options.validate()?;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(sender_task(
        fragment,
        peer_id,
        config,
        deps,
        options,
        SenderInbox::Legacy(rx),
    ));
    Ok(tx)
}

pub(crate) fn spawn_node_sender(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    options: SenderRuntimeOptions,
) -> Result<NodeSender, SenderOptionsError> {
    config.validate()?;
    config.validate_node_ingress()?;
    options.validate()?;
    let (data_tx, data_rx) = mpsc::channel(config.buffer_capacity);
    let (control_tx, control_rx) = mpsc::channel(options.control_channel_capacity);
    let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
    let ingress = Arc::new(NodeSenderIngress::default());
    let (stopped_tx, stopped) = watch::channel(false);
    let inbox = SenderInbox::Node {
        data_rx,
        control_rx,
        signals_rx,
        delivered_overflow_generation: 0,
        delivered_reconcile_generation: 0,
        delivered_peer_generation: 0,
        ingress: ingress.clone(),
    };
    let task = tokio::spawn(async move {
        let _exit = NodeSenderTaskExit(stopped_tx);
        sender_task(fragment, peer_id, config, deps, options, inbox).await;
    });
    Ok(NodeSender {
        data_tx,
        control_tx,
        signals_tx,
        ingress,
        abort: task.abort_handle(),
        stopped,
        stop_lock: Arc::new(Mutex::new(())),
    })
}

fn proto_op(op: &crate::actor::ReplicatedOp) -> ProtoReplicateOp {
    ProtoReplicateOp {
        key: op.key.clone(),
        value: op.value.value.clone(),
        timestamp_ms: op.value.timestamp_ms,
        node_id: op.value.node_id.clone(),
        seq: op.seq,
        boot_id: op.boot_id,
        expires_at_ms: op.value.expires_at_ms.unwrap_or(0),
        is_tombstone: op.value.is_tombstone,
    }
}

async fn send_envelope(
    peer: &SenderPeer,
    tracked_deadline: Option<Instant>,
    msg: EnvelopeMsg,
) -> bool {
    let mut buf = Vec::new();
    {
        use prost::Message;
        let env = Envelope { msg: Some(msg) };
        if env.encode(&mut buf).is_err() {
            return false;
        }
    }
    let now = Instant::now();
    let queue_deadline = now.checked_add(peer.queue_admission_timeout).unwrap_or(now);
    let admission_deadline = tracked_deadline
        .map(|deadline| deadline.min(queue_deadline))
        .unwrap_or(queue_deadline);
    if Instant::now() >= admission_deadline {
        peer.report_failure();
        return false;
    }
    match tokio::time::timeout_at(admission_deadline, peer.tx.reserve()).await {
        Ok(Ok(permit)) => {
            if Instant::now() >= admission_deadline {
                drop(permit);
                peer.report_failure();
                return false;
            }
            permit.send(buf);
            true
        }
        Ok(Err(_)) | Err(_) => {
            peer.report_failure();
            false
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
) {
    let mut delivery = DeliveryState::Streaming;
    let mut connection: Option<ActiveConnection> = None;
    let mut buffer: VecDeque<crate::actor::ReplicatedOp> = VecDeque::new();
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
    let mut next_round_id = 0_u64;
    let mut next_reset_epoch = 0_u64;
    let mut reconcile_rounds: u64 = 0;
    let mut last_ack_durability = None;
    let mut ack_durability_mismatch = false;
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

    loop {
        // Publish state for health checks and tests.
        {
            let state = match delivery {
                DeliveryState::Reset { .. } => "buffer_reset",
                DeliveryState::Streaming
                    if connection
                        .as_ref()
                        .map_or(false, |connection| connection.session_ready) =>
                {
                    "connected"
                }
                DeliveryState::Streaming => "disconnected",
            };
            *deps.state_report.lock().await = SenderStateReport {
                state: state.to_string(),
                session_id: connection.as_ref().map_or(0, |c| c.session_id),
                buffered: buffer.len(),
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
                            .map_or(false, |connection| connection.session_ready)
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
                let msg = match input {
                    SenderInput::Message(msg) => Some(msg),
                    SenderInput::Data(op, ingress) => {
                        publish_data_completion = Some(ingress);
                        Some(SenderMsg::Enqueue(op))
                    }
                    SenderInput::Overflow(generation, ingress) => {
                        overflow = Some((ingress, generation));
                        None
                    }
                    SenderInput::PeerChanged(Some(peer_tx)) => {
                        Some(SenderMsg::PeerConnected { peer_tx })
                    }
                    SenderInput::PeerChanged(None) => connection.as_ref().map(|current| {
                        SenderMsg::PeerDisconnected {
                            peer_tx: current.peer_tx.tx.clone(),
                        }
                    }),
                    SenderInput::ReconcileNow => Some(SenderMsg::ReconcileNow),
                    SenderInput::Closed => break,
                };
                let data_overflow = msg.as_ref().map_or(false, |msg| {
                    matches!(msg, SenderMsg::Enqueue(_))
                        && buffer.len() >= config.buffer_capacity
                });
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
                            &mut next_round_id,
                            config.reconcile_timeout,
                        )
                        .await
                        {
                            Ok(round) => active_reconcile = Some(round),
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
                let Some(msg) = msg else { continue };
                match msg {
                    SenderMsg::Enqueue(op) => {
                        // During reset these are post-snapshot deltas. If this
                        // bounded deque fills, the common branch above advances
                        // the reset epoch and takes a fresh snapshot.
                        buffer.push_back(op);
                    }
                    SenderMsg::PeerConnected { peer_tx: tx } => {
                        if connection
                            .as_ref()
                            .map_or(false, |current| current.peer_tx.same_channel(&tx))
                        {
                            // Dynamic placement polls and duplicate
                            // notifications must not rotate live sessions.
                            continue;
                        }
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
                        let mut conn = ActiveConnection {
                            peer_tx: SenderPeer::new(tx, &runtime),
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
                                    && (pause_new_streaming || buffer.is_empty()) =>
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
                                &mut next_round_id,
                                config.reconcile_timeout,
                            )
                            .await
                            {
                                Ok(round) => {
                                    active_reconcile = Some(round);
                                    reconcile_retry = None;
                                    pause_new_streaming = false;
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
                    SenderMsg::PeerDisconnected { peer_tx } => {
                        if connection
                            .as_ref()
                            .map_or(true, |current| !current.peer_tx.same_channel(&peer_tx))
                        {
                            continue;
                        }
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
                            if let Some(pos) = conn
                                .in_flight
                                .iter()
                                .position(|b| b.batch_seq == batch_seq)
                            {
                                last_ack_durability = Some(durability);
                                ack_durability_mismatch =
                                    durability < config.required_ack_durability;
                                if ack_durability_mismatch {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        batch_seq,
                                        ?durability,
                                        required = ?config.required_ack_durability,
                                        "replication ack did not meet the configured durability fence"
                                    );
                                } else {
                                    ack_durability_mismatch = false;
                                    conn.in_flight.remove(pos);
                                    let is_barrier = active_reconcile.as_ref().map_or(false, |round| {
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
                                ReconcilePurpose::Routine => {
                                    if round.snapshot_revision.is_some() {
                                        reconcile_rounds = reconcile_rounds.saturating_add(1);
                                    } else if let Some(conn) = connection.as_ref() {
                                        // Fetched winners (and reset deltas)
                                        // invalidate the snapshot that chose
                                        // them. The barrier proves those
                                        // writes were applied, but not that
                                        // the two sides now agree. Require a
                                        // fresh no-fetch round before
                                        // reporting routine/bootstrap
                                        // completion.
                                        match begin_reconcile(
                                            fragment,
                                            ReconcilePurpose::Routine,
                                            conn,
                                            &mut next_round_id,
                                            config.reconcile_timeout,
                                        )
                                        .await
                                        {
                                            Ok(next_round) => {
                                                active_reconcile = Some(next_round)
                                            }
                                            Err(()) => {
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    ReconcilePurpose::Routine,
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
                                            ReconcilePurpose::Routine,
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
                                        match send_reconcile_barrier(fragment, &config, conn).await {
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
                                        fragment_revision(fragment, &deps.router).await,
                                    ) {
                                        (Some(expected), Ok(current)) => expected == current,
                                        _ => false,
                                    };
                                    match delivery {
                                        DeliveryState::Reset { epoch }
                                            if epoch == completed_epoch && snapshot_is_current =>
                                        {
                                            delivery = DeliveryState::Streaming;
                                            reconcile_rounds = reconcile_rounds.saturating_add(1);
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
                                                match begin_reconcile(
                                                    fragment,
                                                    ReconcilePurpose::Reset { epoch },
                                                    conn,
                                                    &mut next_round_id,
                                                    config.reconcile_timeout,
                                                )
                                                .await
                                                {
                                                    Ok(round) => active_reconcile = Some(round),
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
                                    let is_barrier = active_reconcile.as_ref().map_or(false, |round| {
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
                        if matches!(delivery, DeliveryState::Streaming)
                            && active_reconcile.is_none()
                        {
                            if let Some(conn) = connection.as_ref() {
                                if !conn.session_ready || !conn.in_flight.is_empty() {
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
                                    &mut next_round_id,
                                    config.reconcile_timeout,
                                )
                                .await
                                {
                                    Ok(round) => {
                                        active_reconcile = Some(round);
                                        reconcile_retry = None;
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
                                pause_new_streaming = true;
                            }
                        }
                    }
                    SenderMsg::ReconcileResponse {
                        session_id,
                        round_id,
                        summaries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().map_or(false, |round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(round.phase, ReconcilePhase::Response)
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        if reconcile_summaries_are_well_formed(
                            fragment,
                            &deps.router,
                            &summaries,
                        ) {
                            if let Some(timestamp_ms) =
                                summaries.iter().map(|summary| summary.timestamp_ms).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe(timestamp_ms) {
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
                        let Some(conn) = connection.as_mut() else { continue };
                        match reconcile(
                            fragment,
                            &deps.router,
                            conn,
                            round.round_id,
                            summaries,
                        )
                        .await
                        {
                            Ok((
                                Some((
                                    remaining_chunks,
                                    expected_chunk_seq,
                                    expected_keys,
                                )),
                                snapshot_revision,
                            )) => {
                                round.snapshot_revision = Some(snapshot_revision);
                                round.phase = ReconcilePhase::Data {
                                    remaining_chunks,
                                    next_chunk_seq: expected_chunk_seq,
                                    expected_keys,
                                };
                                round.phase_deadline =
                                    deadline_after(config.reconcile_timeout);
                                active_reconcile = Some(round);
                            }
                            Ok((None, snapshot_revision)) => {
                                round.snapshot_revision = Some(snapshot_revision);
                                // A prior attempt may already have applied
                                // fetched winners to the cache and then failed
                                // its local store flush. The retry will see
                                // equal summaries and need no fetch, so fence
                                // local durability on every StoreCommitted
                                // completion path, not only after fresh data.
                                if local_reconcile_flush_pending {
                                    if flush_reconcile(fragment, &deps.router).await.is_err() {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            round.purpose,
                                        ));
                                        continue;
                                    }
                                    local_reconcile_flush_pending = false;
                                }
                                match send_reconcile_barrier(fragment, &config, conn).await {
                                    Ok(batch_seq) => {
                                        round.phase = ReconcilePhase::Barrier { batch_seq };
                                        round.phase_deadline =
                                            deadline_after(config.reconcile_timeout);
                                        active_reconcile = Some(round);
                                    }
                                    Err(()) => {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            round.purpose,
                                        ));
                                        disconnect_transport(&mut connection, delivery, &mut buffer);
                                    }
                                }
                            }
                            Err(ReconcileError::Local) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                            }
                            Err(ReconcileError::Transport) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                    SenderMsg::ReconcileData {
                        session_id,
                        round_id,
                        chunk_seq,
                        entries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().map_or(false, |round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(
                                    round.phase,
                                    ReconcilePhase::Data { next_chunk_seq, .. }
                                        if next_chunk_seq == chunk_seq
                                )
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        let expected_keys = match active_reconcile.as_ref().map(|round| &round.phase)
                        {
                            Some(ReconcilePhase::Data { expected_keys, .. }) => {
                                expected_keys
                            }
                            _ => {
                                let _ = connection_valid.send(true);
                                continue;
                            }
                        };
                        if reconcile_entries_are_exact(
                            fragment,
                            &deps.router,
                            &entries,
                            expected_keys,
                        ) {
                            if let Some(timestamp_ms) =
                                entries.iter().map(|entry| entry.timestamp_ms).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe(timestamp_ms) {
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
                            mut remaining_chunks,
                            next_chunk_seq,
                            expected_keys,
                        } = round.phase
                        else {
                            continue;
                        };
                        if config.required_ack_durability
                            == ReplicationAckDurability::StoreCommitted
                        {
                            // Apply is not transactional: an error or lost RPC
                            // reply may follow a successful prefix. Record the
                            // flush obligation before the first mutation.
                            local_reconcile_flush_pending = true;
                        }
                        if apply_reconcile_entries(
                            fragment,
                            &deps.router,
                            entries,
                            &expected_keys,
                        )
                            .await
                            .is_err()
                        {
                            reconcile_retry = Some((
                                Instant::now() + NACK_RETRY_DELAY,
                                round.purpose,
                            ));
                            continue;
                        }
                        let Some(conn) = connection.as_mut() else { continue };
                        if let Some(keys) = remaining_chunks.pop_front() {
                            let next_seq = next_chunk_seq.saturating_add(1);
                            let expected_keys = keys.iter().cloned().collect();
                            let ok = send_envelope(
                                &conn.peer_tx,
                                conn.next_streaming_deadline(),
                                EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                                    fragment: fragment.0,
                                    keys,
                                    session_id,
                                    round_id,
                                    chunk_seq: next_seq,
                                }),
                            )
                            .await;
                            if !ok {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                                continue;
                            }
                            round.phase = ReconcilePhase::Data {
                                remaining_chunks,
                                next_chunk_seq: next_seq,
                                expected_keys,
                            };
                            round.phase_deadline = deadline_after(config.reconcile_timeout);
                            active_reconcile = Some(round);
                            continue;
                        }
                        if local_reconcile_flush_pending {
                            if flush_reconcile(fragment, &deps.router).await.is_err() {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                continue;
                            }
                            local_reconcile_flush_pending = false;
                        }
                        // A fetched apply changes the fragment revision, but
                        // unrelated mutations may have raced with it. Force a
                        // no-fetch verification round after this barrier
                        // instead of rebasing to an ambiguous revision.
                        round.snapshot_revision = None;
                        match send_reconcile_barrier(fragment, &config, conn).await {
                            Ok(batch_seq) => {
                                round.phase = ReconcilePhase::Barrier { batch_seq };
                                round.phase_deadline =
                                    deadline_after(config.reconcile_timeout);
                                active_reconcile = Some(round);
                            }
                            Err(()) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                }
            }
            _ = batch_timer.tick() => {
                if matches!(delivery, DeliveryState::Reset { .. }) {
                    continue;
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
                let ops: Vec<_> = buffer
                    .drain(..config.batch_size.min(buffer.len()))
                    .collect();
                let proto_ops = ops.iter().map(proto_op).collect();
                let ok = send_envelope(
                    &tx,
                    tracked_deadline,
                    EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                        session_id: sid,
                        batch_seq,
                        fragment: fragment.0,
                        ops: proto_ops,
                        required_ack_level: match config.required_ack_durability {
                            ReplicationAckDurability::MemoryApplied => {
                                ReplicationAckLevel::Applied as i32
                            }
                            ReplicationAckDurability::StoreCommitted => {
                                ReplicationAckLevel::StoreCommitted as i32
                            }
                        },
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
                if session_retry.map_or(false, |deadline| now >= deadline) {
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
                    .map_or(false, |round| now >= round.phase_deadline)
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

                let ordinary_ack_timed_out = connection.as_ref().map_or(false, |conn| {
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
                        &mut next_round_id,
                        config.reconcile_timeout,
                    )
                    .await
                    {
                        Ok(round) => {
                            active_reconcile = Some(round);
                            reconcile_retry = None;
                        }
                        Err(()) => {
                            reconcile_retry = Some((now + NACK_RETRY_DELAY, purpose));
                            pause_new_streaming = false;
                            disconnect_transport(&mut connection, delivery, &mut buffer);
                        }
                    }
                }
            }
            _ = consistency_timer.tick() => {
                if matches!(delivery, DeliveryState::Streaming)
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
                            &mut next_round_id,
                            config.reconcile_timeout,
                        )
                        .await
                        {
                            Ok(round) => {
                                active_reconcile = Some(round);
                                reconcile_retry = None;
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

/// Keys per ReconcileFetch chunk. Bounds the memory of a single
/// ReconcileData message during bootstrap / anti-entropy on large
/// fragments (Phase 8 chunked bootstrap).
const RECONCILE_FETCH_CHUNK: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileError {
    Local,
    Transport,
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
    next_round_id: &mut u64,
    phase_timeout: Duration,
) -> Result<ReconcileRound, ()> {
    let round_id = next_nonzero(next_round_id);
    let ok = send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReconcileRequest(ReconcileRequest {
            fragment: fragment.0,
            session_id: connection.session_id,
            round_id,
        }),
    )
    .await;
    if !ok {
        return Err(());
    }
    Ok(ReconcileRound {
        session_id: connection.session_id,
        round_id,
        purpose,
        phase: ReconcilePhase::Response,
        phase_deadline: deadline_after(phase_timeout),
        snapshot_revision: None,
    })
}

async fn send_reconcile_barrier(
    fragment: FragmentId,
    config: &SenderConfig,
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
            required_ack_level: match config.required_ack_durability {
                ReplicationAckDurability::MemoryApplied => ReplicationAckLevel::Applied as i32,
                ReplicationAckDurability::StoreCommitted => {
                    ReplicationAckLevel::StoreCommitted as i32
                }
            },
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
    });
    Ok(batch_seq)
}

async fn send_repair_ops(
    fragment: FragmentId,
    connection: &ActiveConnection,
    ops: &[crate::actor::ReplicatedOp],
) -> Result<(), ReconcileError> {
    for chunk in ops.chunks(100) {
        let proto_ops = chunk.iter().map(proto_op).collect();
        if !send_envelope(
            &connection.peer_tx,
            connection.next_streaming_deadline(),
            EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: connection.session_id,
                batch_seq: u64::MAX,
                fragment: fragment.0,
                ops: proto_ops,
                // Untracked repairs are apply-only. The following tracked
                // empty barrier requests and reports the actual fence.
                required_ack_level: ReplicationAckLevel::Applied as i32,
            }),
        )
        .await
        {
            return Err(ReconcileError::Transport);
        }
    }
    Ok(())
}

fn reconcile_summaries_are_well_formed(
    fragment: FragmentId,
    router: &FragmentRouter,
    summaries: &[KeySummary],
) -> bool {
    let mut seen = HashSet::with_capacity(summaries.len());
    summaries.iter().all(|summary| {
        router.key_belongs_to(fragment, &summary.key) && seen.insert(summary.key.as_str())
    })
}

fn reconcile_entries_are_exact(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: &[SyncEntry],
    expected_keys: &HashSet<String>,
) -> bool {
    let received_keys: HashSet<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
    entries.len() == expected_keys.len()
        && received_keys.len() == entries.len()
        && entries.iter().all(|entry| {
            expected_keys.contains(&entry.key) && router.key_belongs_to(fragment, &entry.key)
        })
}

/// Run one reconciliation round: compare the peer's summaries against
/// local state, fetch keys where the peer wins, and ship keys where we
/// win.
///
/// Returns `None` when no fetch was needed (the round is complete).
/// Otherwise sends the first fetch chunk and returns the remaining
/// chunks; the caller sends them as each `ReconcileData` arrives.
///
/// LWW makes the whole exchange idempotent: if a shipped op no longer
/// wins by the time the peer applies it, the peer's merge rejects it.
async fn reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
    connection: &ActiveConnection,
    round_id: u64,
    summaries: Vec<KeySummary>,
) -> Result<(Option<(VecDeque<Vec<String>>, u64, HashSet<String>)>, u64), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    let (local, snapshot_revision): (HashMap<String, VersionedValue>, u64) = match actor
        .call(|reply| KvMessage::GetSnapshot { reply }, None)
        .await
    {
        Ok(CallResult::Success(snapshot)) => snapshot,
        _ => return Err(ReconcileError::Local),
    };

    let mut fetch_keys = Vec::new();
    let mut send_ops: Vec<crate::actor::ReplicatedOp> = Vec::new();
    let mut seen_remote = HashSet::new();

    for s in &summaries {
        if !router.key_belongs_to(fragment, &s.key) || !seen_remote.insert(s.key.clone()) {
            tracing::warn!(
                fragment = fragment.0,
                key = %s.key,
                "rejecting reconcile summary with wrong-fragment or duplicate key"
            );
            return Err(ReconcileError::Local);
        }
        let remote_v = VersionedValue {
            value: Vec::new(),
            timestamp_ms: s.timestamp_ms,
            node_id: s.node_id.clone(),
            expires_at_ms: None,
            is_tombstone: s.is_tombstone,
        };
        match local.get(&s.key) {
            Some(local_v) => {
                if remote_v.wins_over(local_v) {
                    fetch_keys.push(s.key.clone());
                } else if local_v.wins_over(&remote_v) {
                    send_ops.push(crate::actor::ReplicatedOp {
                        key: s.key.clone(),
                        value: local_v.clone(),
                        seq: 0,
                        boot_id: 0,
                    });
                }
            }
            None => {
                // Peer has it, we don't.
                fetch_keys.push(s.key.clone());
            }
        }
    }
    // Keys we have that the peer lacks.
    for (key, v) in &local {
        if !seen_remote.contains(key) {
            send_ops.push(crate::actor::ReplicatedOp {
                key: key.clone(),
                value: v.clone(),
                seq: 0,
                boot_id: 0,
            });
        }
    }

    send_repair_ops(fragment, connection, &send_ops).await?;

    if fetch_keys.is_empty() {
        return Ok((None, snapshot_revision));
    }
    // Chunk the fetch so a large fragment doesn't materialize as one
    // giant ReconcileData message: send the first chunk now, queue the
    // rest for the caller to send as each chunk's data arrives.
    let mut chunks: VecDeque<Vec<String>> = fetch_keys
        .chunks(RECONCILE_FETCH_CHUNK)
        .map(|c| c.to_vec())
        .collect();
    let first = chunks.pop_front().unwrap_or_default();
    let expected_keys = first.iter().cloned().collect();
    if !send_envelope(
        &connection.peer_tx,
        connection.next_streaming_deadline(),
        EnvelopeMsg::ReconcileFetch(ReconcileFetch {
            fragment: fragment.0,
            keys: first,
            session_id: connection.session_id,
            round_id,
            chunk_seq: 0,
        }),
    )
    .await
    {
        return Err(ReconcileError::Transport);
    }
    Ok((Some((chunks, 0, expected_keys)), snapshot_revision))
}

async fn apply_reconcile_entries(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: Vec<SyncEntry>,
    expected_keys: &HashSet<String>,
) -> Result<(), ReconcileError> {
    if !reconcile_entries_are_exact(fragment, router, &entries, expected_keys) {
        tracing::warn!(
            fragment = fragment.0,
            expected = expected_keys.len(),
            received = entries.len(),
            "rejecting incomplete, duplicate, unrequested, or wrong-fragment reconcile data"
        );
        return Err(ReconcileError::Local);
    }
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    for entry in entries {
        let mut value = VersionedValue::new(entry.value, entry.timestamp_ms, entry.node_id);
        value.expires_at_ms = (entry.expires_at_ms != 0).then_some(entry.expires_at_ms);
        value.is_tombstone = entry.is_tombstone;
        let op = crate::actor::ReplicatedOp {
            key: entry.key,
            value,
            seq: 0,
            boot_id: 0,
        };
        match actor
            .call(
                |reply| KvMessage::ApplyOp {
                    op,
                    priority: OpPriority::Default,
                    is_replication: true,
                    reply: Some(reply),
                },
                None,
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

async fn fragment_revision(
    fragment: FragmentId,
    router: &FragmentRouter,
) -> Result<u64, ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor
        .call(|reply| KvMessage::GetRevision { reply }, None)
        .await
    {
        Ok(CallResult::Success(revision)) => Ok(revision),
        Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => Err(ReconcileError::Local),
    }
}

async fn flush_reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
) -> Result<(), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor.call(|reply| KvMessage::Flush { reply }, None).await {
        Ok(CallResult::Success(Ok(
            ApplyDurability::StoreCommitted | ApplyDurability::MemoryApplied,
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
    use prost::Message as _;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn op(key: &str) -> crate::actor::ReplicatedOp {
        crate::actor::ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 0,
            boot_id: 0,
        }
    }

    fn state_report() -> Arc<Mutex<SenderStateReport>> {
        Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }))
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
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _exit = NodeSenderTaskExit(stopped_tx);
            let _ = started_tx.send(());
            let _ = finish_rx.await;
        });
        let sender = NodeSender {
            data_tx,
            control_tx,
            signals_tx,
            ingress,
            abort: task.abort_handle(),
            stopped,
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
            Err(NodeSenderSendError::Full)
        );

        let (old_peer, _old_rx) = mpsc::channel(1);
        let (new_peer, _new_rx) = mpsc::channel(1);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_peer.clone(),
            })
            .unwrap();
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: new_peer.clone(),
            })
            .unwrap();
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_peer })
            .unwrap();
        sender.send(SenderMsg::ReconcileNow).unwrap();
        let signals = signals_rx.borrow().clone();
        assert!(signals
            .peer_tx
            .map_or(false, |current| current.same_channel(&new_peer)));
        assert_eq!(signals.reconcile_generation, 1);

        assert_eq!(data_rx.recv().await.unwrap().key, "queued");
        assert!(matches!(
            control_rx.recv().await,
            Some(SenderMsg::Ack { .. })
        ));
        let pending_before_closed_send = sender.ingress.pending_data.load(Ordering::Acquire);
        let overflow_before_closed_send = sender.signals_tx.borrow().overflow_generation;
        drop(data_rx);
        assert_eq!(
            sender.send_data(op("closed-data")),
            Err(NodeSenderSendError::Closed)
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
            Err(NodeSenderSendError::Closed)
        );
        finish_tx.send(()).unwrap();
        task.await.unwrap();
        sender.stop().await;
        assert_eq!(
            sender.send(SenderMsg::ReconcileNow),
            Err(NodeSenderSendError::Closed)
        );
    }

    #[tokio::test]
    async fn node_sender_delivers_each_sideband_field_across_unrelated_updates() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (signals_tx, signals_rx) = watch::channel(NodeSenderSignals::default());
        let ingress = Arc::new(NodeSenderIngress::default());
        let (first_peer, _first_peer_rx) = mpsc::channel(1);
        let (latest_peer, _latest_peer_rx) = mpsc::channel(1);
        let mut inbox = SenderInbox::Node {
            data_rx,
            control_rx,
            signals_rx,
            delivered_overflow_generation: 0,
            delivered_reconcile_generation: 0,
            delivered_peer_generation: 0,
            ingress: ingress.clone(),
        };

        signals_tx.send_modify(|signals| {
            signals.overflow_generation = 1;
            signals.peer_generation = 1;
            signals.peer_tx = Some(first_peer);
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
            signals.peer_tx = Some(latest_peer.clone());
        });
        assert!(matches!(
            inbox.recv().await,
            SenderInput::PeerChanged(Some(peer)) if peer.same_channel(&latest_peer)
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
        let mut inbox = SenderInbox::Node {
            data_rx,
            control_rx,
            signals_rx,
            delivered_overflow_generation: 0,
            delivered_reconcile_generation: 0,
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
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
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
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
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
        sender.stop().await;
    }

    #[tokio::test]
    async fn standalone_sender_api_remains_unbounded() {
        fn assert_unbounded(_: &mpsc::UnboundedSender<SenderMsg>) {}
        let sender = spawn_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: crate::node::FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: state_report(),
            },
        );
        assert_unbounded(&sender);
        drop(sender);
        tokio::task::yield_now().await;
    }

    #[test]
    fn requeue_in_flight_preserves_batches_ahead_of_newer_buffered_ops() {
        let mut in_flight = VecDeque::from([
            InflightBatch {
                batch_seq: 0,
                ops: vec![op("a"), op("b")],
                kind: InflightKind::Streaming {
                    ack_deadline: Instant::now() + Duration::from_secs(1),
                },
            },
            InflightBatch {
                batch_seq: 1,
                ops: vec![op("c")],
                kind: InflightKind::Streaming {
                    ack_deadline: Instant::now() + Duration::from_secs(1),
                },
            },
        ]);
        let mut buffer = VecDeque::from([op("d")]);
        requeue_in_flight(&mut in_flight, &mut buffer);
        assert!(in_flight.is_empty());
        assert_eq!(
            buffer.into_iter().map(|op| op.key).collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
    }

    #[test]
    fn batch_sequence_allocator_never_emits_reserved_repair_sentinel() {
        let (tx, _rx) = mpsc::channel(1);
        let mut connection = ActiveConnection {
            peer_tx: SenderPeer::new(tx, &SenderRuntimeOptions::default()),
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
        let legacy_unbounded_capacity = SenderConfig {
            buffer_capacity: tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1),
            ..SenderConfig::default()
        };
        assert_eq!(legacy_unbounded_capacity.validate(), Ok(()));
        assert_eq!(
            legacy_unbounded_capacity.validate_node_ingress(),
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
        let peer = SenderPeer::new(tx, &options);
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
    async fn live_overflow_handles_reset_reconcile_admission_failure() {
        let report = state_report();
        let sender = spawn_sender_with_options(
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
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(10)),
        )
        .unwrap();
        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();
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
        let sender = spawn_sender_with_options(
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
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(10)),
        )
        .unwrap();
        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();
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
        let peer = SenderPeer::new(tx, &SenderRuntimeOptions::default());
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
