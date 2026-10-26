// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! A database node.
//!
//! Each node runs one supervised actor subtree per locally hosted fragment,
//! plus a Tokio TCP mesh to its peers. A client mutation is applied by one
//! serving fragment owner, which returns its exact outcome to the caller and
//! enqueues the winning version to per-fragment/per-peer replication senders.
//! Receivers report the durability they achieved after every operation is
//! accounted for and any requested flush is attempted: `MemoryApplied` after
//! cache application, or `StoreCommitted` after a configured store accepts the
//! flush, or `StableCommitted` after its explicit stable-storage fence. Senders
//! retire only ACKs that meet each batch's requested level.
//! Periodic anti-entropy repairs residual divergence.
//!
//! `seq` and the per-process `boot_id` are carried as propagation metadata.
//! Idempotency and conflict resolution use the value's LWW version rather than
//! deduplicating on those fields.
//!
//! The dialer side of each connection owns it: if the TCP stream drops,
//! it is redialed with exponential backoff until the node shuts down,
//! so transient network blips heal without operator intervention.
//!
//! Wire format: prost-encoded [`crate::proto::Envelope`]
//! frames, each prefixed with a 4-byte big-endian length.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
#[cfg(test)]
use ractor::Actor;
use ractor::{ActorRef, ActorRuntime, SpawnErr};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{
    mpsc, oneshot, watch, Mutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit,
    RwLock as AsyncRwLock, Semaphore,
};
use tokio::time::Instant;

use crate::actor::{
    ApplyDurability, ApplyError, ApplyOutcome, FragmentHealth, FragmentShutdownError,
    FragmentStartupStorageError, FragmentSupervisor, HlcClock, KvMessage, OpPriority, PoolConfig,
    ReplicatedOp, SupervisorArgs, SupervisorMessage, VersionedValue,
};
use crate::network::{BoxStream, Network, TcpNetwork};
use crate::placement::{
    validate_placement, FragmentPlacementState, PlacementController, PlacementReport,
};
use crate::proto::{
    BatchAck, BatchGet, BatchGetResponse, BatchKeyValue, BatchNack, BatchPut, Envelope,
    EnvelopeMsg, ForwardedGet, ForwardedPut, GetResponse, Hello, HelloAuth, KeySummary,
    MutationResponse, MutationResult, MutationStatus, Priority as ProtoPriority, ReconcileData,
    ReconcileEntry, ReconcileFetch, ReconcileResponse, ReplicationAckLevel,
};
use crate::replication::{
    spawn_node_sender, NodeSender, NodeSenderDrain, ReplicationAckDurability, SenderConfig,
    SenderDeps, SenderIngressError, SenderMsg, SenderRuntimeOptions, SenderStateReport,
};
use crate::runtime_tasks::RuntimeTasks;
use crate::shard::{fragment_of, FragmentId};
use crate::store::{
    KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, PrefixedStore, StoreBinding, StoreIdentity,
};
use crate::ttl_expiry::{
    ExpiryDiagnostics, ExpiryPublisher, ExpiryRuntime, TtlExpiryHandler, TtlExpiryOptions,
};
use crate::wire::{
    bytes_field_len, check_envelope_budget, check_record_budget, encode_envelope,
    message_field_len, preflight_envelope_decode, varint_field_len, BudgetedFrame, BudgetedPeerTx,
    NodePeerByteBudget, ReconcileGate, RepeatedFieldSizer, ANTI_ENTROPY_PAGE_BYTES,
    MAX_FRAME_BYTES, MAX_RECONCILE_FETCH_KEYS, MAX_RECONCILE_PAGE_ITEMS, MAX_WIRE_BATCH_ITEMS,
    MAX_WIRE_ERROR_BYTES, MAX_WIRE_NODE_ID_BYTES, NODE_PEER_QUEUE_BYTE_CAPACITY,
};

pub use crate::wire::PeerWriterDiagnostics;

#[cfg(test)]
mod peer_writer_budget_tests;
#[cfg(test)]
mod response_retention_tests;

mod connection;
use connection::{
    fresh_boot_id, handle_connection, send_envelope, try_send_envelope_immediate,
    try_send_envelope_until, try_send_envelope_with_admission_timeout, DialIntentState,
    PeerChannel, PeerFailureSignals, PeerMap,
};
#[cfg(test)]
use connection::{
    fresh_hello, run_bounded_peer_writer, try_send_envelope, validate_peer_hello, ConnContext,
    PeerWriterPipeline,
};

mod session;
use session::{
    run_reconcile_responder, InboundSession, InboundSessions, ReconcileResponderContext,
    ReconcileResponderJob, ReconcileResponseJob, ReconcileSummaryJob, ServedReconciles,
};

mod relay;
use relay::{
    cancel_connection_relays, relay_batch, relay_get, relay_put, remove_get_relay,
    remove_mutation_relay, send_batch_get_response, send_mutation_response,
    send_unavailable_mutation, unavailable_mutation_results, MutationRelayMap, ReforwardCtx,
    RelayAdmission, RelayMap,
};
#[cfg(test)]
use relay::{relay_key_storage_bytes, GetRelay};

mod placement;
use placement::{placement_driver, PlacementDriverDeps, BOOTSTRAP_TIMEOUT};

pub mod drain;
mod drain_persistence;
pub mod peer_authority;
use peer_authority::PeerAuthority;
pub use peer_authority::{
    AuthorizedPeerGeneration, PeerAuthorityOptions, PeerAuthoritySnapshot, PeerFenceError,
    PeerFenceOptions, PeerFencePhase, PeerFenceReport, PeerFenceStage, PeerFenceStatus,
};
mod lifecycle;
pub use drain::{
    DrainCutoff, DrainDiagnostics, DrainDurability, DrainError, DrainOptions, DrainPhase,
    DrainReport, DrainStatus, FragmentDrainFence, LocalDurability,
};
mod detailed_status;
mod membership;
pub mod metrics;
pub mod observability;
pub mod status;
pub use detailed_status::{
    DetailedObservation, DetailedSnapshotOptions, FragmentPoolObservation, PoolObservationFailure,
};
mod startup;
#[cfg(test)]
use lifecycle::spawn_fragment_subtree;
pub use lifecycle::FragmentRouter;
use lifecycle::{
    ensure_sender_entry, quiesce_fragment_supervisor, register_fragment_dispatcher_monitor,
    spawn_fragment_subtree_registered, OperationGate, OperationPermit, RetiringSenderRegistry,
    RouteMonitorRegistrationGuard, SenderEntryDeps, SenderFinalizationRegistry, SenderMap,
    SenderReportMap, ShutdownProgress, SupervisorRegistry,
};
pub use startup::{NodeStartup, NodeStartupSnapshot, StartupStage};

mod read_service;
use read_service::{
    ReadJob, ReadRequest, ReadRoute, ReadService, READ_KEY_BYTE_CAPACITY, READ_QUEUE_CAPACITY,
    READ_WORKERS_PER_PRIORITY,
};
mod embedded_operations;
pub use embedded_operations::EmbeddedOperationDiagnostics;
use embedded_operations::{
    EmbeddedOperations, RequestRetention, EMBEDDED_OPERATION_BYTE_CAPACITY,
    EMBEDDED_OPERATION_CAPACITY,
};

mod mutation_service;
mod write_concern;
use mutation_service::{
    MutationJob, MutationRequest, MutationService, FORWARDED_MUTATION_BYTE_CAPACITY,
    FORWARDED_MUTATION_CAPACITY,
};
mod query;
mod table;
pub use table::{Table, TableId};
pub(crate) mod recovery;
use recovery::RecoveryContext;
pub use recovery::RecoveryOptions;
pub mod scan;
pub use scan::{ScanError, MAX_SCAN_TOTAL_MATCHES};

/// Exact peer wire protocol version. Every current feature is mandatory;
/// older and newer versions are rejected rather than negotiated.
pub const WIRE_PROTOCOL_VERSION: u32 = 6;

/// Outbound queue depth per peer connection.
const PEER_CHANNEL_DEPTH: usize = 256;
/// Encoded bytes admitted to one peer's writer pipeline, including the frame
/// currently blocked on the socket.
const PEER_CHANNEL_BYTE_CAPACITY: usize = 2 * MAX_FRAME_BYTES;
/// Bound control preference so sustained replies cannot starve ordinary RPC
/// and replication traffic on the shared socket.
const PEER_RESPONSE_BURST_LIMIT: usize = 16;
/// Bytes unavailable to ordinary producers so every legal reader-generated
/// response can still enter the priority lane under load. Most aggregate
/// responses target one MiB, but one individually valid record may approach
/// the hard frame limit.
const PEER_RESPONSE_BYTE_RESERVE: usize = MAX_FRAME_BYTES;
const MIN_PEER_CHANNEL_BYTE_CAPACITY: usize = MAX_FRAME_BYTES + PEER_RESPONSE_BYTE_RESERVE;
/// Soft encoded-byte target for aggregate request and response frames. One
/// individually valid item may exceed this target up to `MAX_FRAME_BYTES`.
const WIRE_BATCH_PAGE_BYTES: usize = ANTI_ENTROPY_PAGE_BYTES;
/// Bound pull-request suffixes so a value-heavy batch cannot resend thousands
/// of key bytes quadratically while responses advance one item at a time.
const MAX_BATCH_GET_REQUEST_KEYS: usize = 256;
/// Bound local multi-key fan-out before requests reach fragment dispatcher
/// admission. Pool queue caps apply after dequeue, so issuing an unbounded
/// `join_all` would otherwise let one caller monopolize dispatcher ingress.
const MAX_LOCAL_BATCH_IN_FLIGHT: usize = 1024;
/// Protocol-control messages retained per fragment/peer sender. Mutation
/// ingress has its own lane sized by `SenderConfig::buffer_capacity`.
const REPLICATION_CONTROL_CHANNEL_DEPTH: usize = 64;
/// Concurrent inbound sockets admitted to handshake/connection processing.
const INBOUND_CONNECTION_CAPACITY: usize = 1024;
/// Node-wide raw receive bodies, including handshakes and outbound-dialed
/// connections. Four maximum frames fit without allowing the socket count to
/// multiply incomplete-body allocations into gigabytes.
const RECEIVE_BYTE_CAPACITY: usize = 4 * MAX_FRAME_BYTES;
const RELAY_CAPACITY: usize = 1024;
const RELAY_KEY_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
/// Total budget for both flights of the peer handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Internal task-registration queue. Registration applies backpressure and is
/// acknowledged before the caller treats a task as owned.
const TASK_COMMAND_CAPACITY: usize = 64;
const HANDSHAKE_CHALLENGE_BYTES: usize = 32;
const RECONCILE_SUMMARY_PAGE_ITEMS: usize = 256;
/// Fetch-response jobs retained per physical connection. Each job contains at
/// most one soft-page of advertised keys; overload fails the connection closed
/// so a peer can retry instead of blocking the sole socket reader.
const RECONCILE_RESPONSE_QUEUE_DEPTH: usize = 1;

/// Reconnect backoff: first retry after this, doubling to the cap.
const RECONNECT_BASE: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const REMOTE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const RECONCILE_DATA_ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);

fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

/// Pending single-key forwarded reads. The authenticated response source and
/// requested key are retained until completion so an unrelated peer cannot
/// win a request-id collision and found values can be checked against the
/// record wire bound before they affect the clock or caller.
type PendingGets = Arc<StdMutex<HashMap<u64, PendingGet>>>;
/// Pending batched forwarded reads. The expected connection-bound responder and ordered
/// key prefix are retained so a colliding or malformed reply cannot complete
/// another peer's request.
type PendingBatchGets = Arc<StdMutex<HashMap<u64, PendingBatchGet>>>;
/// Pending single or batched forwarded mutations. Results preserve request
/// order and cardinality.
type PendingMutations = Arc<StdMutex<HashMap<u64, PendingMutation>>>;

struct PendingMutation {
    expected_peer: String,
    expected_keys: Vec<String>,
    reply: oneshot::Sender<Vec<MutationResult>>,
}

struct PendingGet {
    expected_peer: String,
    expected_key: String,
    reply: oneshot::Sender<Result<Option<VersionedValue>, NodeError>>,
}

struct PendingBatchGet {
    expected_peer: String,
    expected_keys: Vec<String>,
    reply: oneshot::Sender<Result<Vec<BatchKeyValue>, NodeError>>,
}

/// Removes a pending request even if its future is cancelled while waiting
/// for outbound queue capacity or a peer response.
struct PendingMutationGuard {
    req_id: u64,
    pending: PendingMutations,
}

struct PendingGetGuard {
    req_id: u64,
    pending: PendingGets,
}

impl Drop for PendingGetGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.req_id);
    }
}

struct PendingBatchGetGuard {
    req_id: u64,
    pending: PendingBatchGets,
}

impl Drop for PendingBatchGetGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.req_id);
    }
}

impl Drop for PendingMutationGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.req_id);
    }
}

/// Max re-forward hops for a routed op. Bounds the chain when peers'
/// placement maps transiently disagree during migration.
const MAX_FORWARD_HOPS: u32 = 4;

/// Errors produced by [Node] operations.
#[derive(Debug)]
pub enum NodeError {
    Io(std::io::Error),
    Codec(String),
    Actor(String),
    Network(String),
    /// An accepted mutation outlived its waiter or lost its exact completion
    /// reply. It may have applied; do not retry it as a fresh write automatically.
    /// The node retains accepted completion through apply and replication handoff.
    UnknownOutcome(String),
    /// The operation was shed due to backpressure (queue cap or rate
    /// limit). The caller should retry with backoff or reduce load.
    /// Distinct from "not found" — a shed Get never returns Ok(None).
    Backpressure(String),
    /// Durable state failed to load, validate, or persist during startup; a
    /// durable timestamp could not seed the clock; or a store failed while
    /// applying a mutation or completing a requested durability fence such as
    /// shutdown.
    Storage(String),
    /// A different version already wins the LWW merge. The attempted write
    /// was not applied and is never reported as successful.
    Superseded {
        timestamp_ms: u64,
        logical_counter: u64,
        node_id: String,
        is_tombstone: bool,
    },
    /// A mutation reached the fragment after its TTL deadline. Its expiry
    /// tombstone was installed so older values cannot reappear.
    Expired,
    /// The node has begun its one-way shutdown fence and no longer admits
    /// application operations. Retry against another live node.
    ShuttingDown,
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Io(e) => write!(f, "io error: {e}"),
            NodeError::Codec(e) => write!(f, "codec error: {e}"),
            NodeError::Actor(e) => write!(f, "actor error: {e}"),
            NodeError::Network(e) => write!(f, "network error: {e}"),
            NodeError::UnknownOutcome(e) => write!(f, "mutation outcome unknown: {e}"),
            NodeError::Backpressure(e) => write!(f, "backpressure shed: {e}"),
            NodeError::Storage(e) => write!(f, "storage error: {e}"),
            NodeError::Superseded {
                timestamp_ms,
                logical_counter,
                node_id,
                is_tombstone,
            } => write!(
                f,
                "write was superseded by timestamp={timestamp_ms}:{logical_counter} node_id={node_id} tombstone={is_tombstone}"
            ),
            NodeError::Expired => write!(f, "write expired before it could be applied"),
            NodeError::ShuttingDown => write!(f, "node is shutting down"),
        }
    }
}

/// TTL deadlines are wall-clock milliseconds, not HLC version timestamps.
/// Zero is reserved for absent expiry by the wire and durable encodings.
fn ttl_expiry_deadline_ms(wall_ms: u64, ttl: Duration) -> Result<u64, NodeError> {
    let ttl_ms = u64::try_from(ttl.as_millis())
        .map_err(|_| NodeError::Codec("TTL duration exceeds u64 milliseconds".to_string()))?;
    wall_ms
        .checked_add(ttl_ms)
        .filter(|deadline| *deadline != 0)
        .ok_or_else(|| {
            NodeError::Codec("TTL expiry must fit a nonzero u64 wall-clock deadline".to_string())
        })
}

/// Map an [`OpPriority`] to its stable wire value (see `Priority` in
/// `meta.proto`).
fn priority_to_proto(p: OpPriority) -> i32 {
    match p {
        OpPriority::Default => ProtoPriority::Default as i32,
        OpPriority::High => ProtoPriority::High as i32,
        OpPriority::Background => ProtoPriority::Background as i32,
    }
}

/// Map a wire priority back to [`OpPriority`]. Unknown/absent values
/// degrade to `Default` (never misroute to a more aggressive pool).
fn priority_from_proto(v: i32) -> OpPriority {
    match v {
        x if x == ProtoPriority::High as i32 => OpPriority::High,
        x if x == ProtoPriority::Background as i32 => OpPriority::Background,
        _ => OpPriority::Default,
    }
}

/// Validate and observe one timestamp before any peer-supplied version can
/// affect routing, reconciliation, or stored LWW state. Invalid timestamps
/// terminate the connection at the call site; clamping only the local clock
/// while retaining the raw value would leave the key permanently poisoned.
async fn observe_remote_timestamp(
    hlc: &Arc<Mutex<HlcClock>>,
    version: actordb_protocol::RecordVersion,
    message: &str,
) -> bool {
    match hlc.lock().await.observe_version(version) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!("rejecting {message} with invalid timestamp: {error}");
            false
        }
    }
}

async fn call_apply(
    actor: &ActorRef<KvMessage>,
    op: ReplicatedOp,
    priority: OpPriority,
    is_replication: bool,
) -> Result<ApplyOutcome, NodeError> {
    call_apply_retained(actor, op, priority, is_replication, None).await
}

async fn call_apply_retained(
    actor: &ActorRef<KvMessage>,
    op: ReplicatedOp,
    priority: OpPriority,
    is_replication: bool,
    retention: Option<Arc<dyn Send + Sync>>,
) -> Result<ApplyOutcome, NodeError> {
    match actor
        .call(
            |reply| {
                KvMessage::ApplyOp {
                    op,
                    priority,
                    is_replication,
                    reply: Some(reply),
                }
                .with_optional_retention(retention)
            },
            None,
        )
        .await
        .map_err(|error| NodeError::Actor(error.to_string()))?
    {
        CallResult::Success(Ok(outcome)) => Ok(outcome),
        CallResult::Success(Err(ApplyError::Backpressure)) => Err(NodeError::Backpressure(
            "fragment worker pool is at capacity".to_string(),
        )),
        CallResult::Success(Err(ApplyError::InvalidInput(error))) => Err(NodeError::Codec(error)),
        CallResult::Success(Err(error @ ApplyError::VersionConflict { .. })) => {
            Err(NodeError::Codec(error.to_string()))
        }
        CallResult::Success(Err(ApplyError::Storage(error))) => Err(NodeError::Storage(error)),
        CallResult::Timeout => Err(NodeError::Actor("apply timed out".to_string())),
        CallResult::SenderError => Err(NodeError::Actor("apply reply channel dropped".to_string())),
    }
}

async fn flush_fence_retained(
    actor: &ActorRef<KvMessage>,
    required: ReplicationAckDurability,
    retention: Option<Arc<dyn Send + Sync>>,
) -> Result<ApplyDurability, NodeError> {
    let result = match required {
        ReplicationAckDurability::StableCommitted => {
            actor
                .call(
                    |reply| KvMessage::FlushStable { reply }.with_optional_retention(retention),
                    Some(Duration::from_secs(30)),
                )
                .await
        }
        ReplicationAckDurability::MemoryApplied | ReplicationAckDurability::StoreCommitted => {
            actor
                .call(
                    |reply| KvMessage::Flush { reply }.with_optional_retention(retention),
                    Some(Duration::from_secs(30)),
                )
                .await
        }
    };
    match result.map_err(|error| NodeError::Actor(error.to_string()))? {
        CallResult::Success(Ok(durability)) => Ok(durability),
        CallResult::Success(Err(ApplyError::Storage(error))) => Err(NodeError::Storage(error)),
        CallResult::Success(Err(ApplyError::Backpressure)) => Err(NodeError::Actor(
            "flush fence unexpectedly hit admission control".to_string(),
        )),
        CallResult::Success(Err(ApplyError::InvalidInput(error))) => Err(NodeError::Actor(
            format!("flush fence unexpectedly returned invalid input: {error}"),
        )),
        CallResult::Success(Err(error @ ApplyError::VersionConflict { .. })) => {
            Err(NodeError::Actor(error.to_string()))
        }
        CallResult::Timeout => Err(NodeError::Actor("flush fence timed out".to_string())),
        CallResult::SenderError => Err(NodeError::Actor(
            "flush fence reply channel dropped".to_string(),
        )),
    }
}

async fn enqueue_to_senders(senders: &SenderMap, frag: FragmentId, op: &ReplicatedOp) {
    // NodeSender clones are cheap task handles. Keep the node-global registry
    // lock to lookup only: a value can approach the wire frame limit, and its
    // per-replica clone plus bounded-mailbox admission must not serialize ACKs
    // or writes for every other fragment.
    let targets: Vec<NodeSender> = senders
        .lock()
        .await
        .iter()
        .filter(|((sender_frag, _), _)| *sender_frag == frag)
        .map(|(_, sender)| sender.clone())
        .collect();
    for sender in targets {
        let _ = sender.offer(op);
    }
}

async fn sender_task_is_current(
    senders: &SenderMap,
    key: &(FragmentId, String),
    sender: &NodeSender,
) -> bool {
    senders
        .lock()
        .await
        .get(key)
        .is_some_and(|current| current.same_task(sender))
}

fn truncate_wire_error(mut error: String) -> String {
    if error.len() <= MAX_WIRE_ERROR_BYTES {
        return error;
    }
    let mut end = MAX_WIRE_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error.truncate(end);
    error
}

/// Conservative encoded size for any result that can be produced for one
/// mutation key. Mutually exclusive error/winner fields are deliberately
/// counted together so a request chunk is safe for every outcome.
fn max_mutation_result_encoded_len(key_len: usize) -> usize {
    bytes_field_len(1, key_len)
        + varint_field_len(2, MutationStatus::Unavailable as u64)
        + bytes_field_len(3, MAX_WIRE_ERROR_BYTES)
        + varint_field_len(4, u64::MAX)
        + bytes_field_len(5, MAX_WIRE_NODE_ID_BYTES)
        + varint_field_len(6, 1)
        + varint_field_len(7, u64::from(u32::MAX))
}

fn batch_put_base_encoded_len(priority: i32) -> usize {
    BatchPut {
        puts: Vec::new(),
        priority,
        req_id: u64::MAX,
    }
    .encoded_len()
}

fn mutation_response_base_encoded_len() -> usize {
    MutationResponse {
        req_id: u64::MAX,
        results: Vec::new(),
    }
    .encoded_len()
}

fn batch_get_base_encoded_len(priority: i32) -> usize {
    BatchGet {
        req_id: u64::MAX,
        keys: Vec::new(),
        priority,
    }
    .encoded_len()
}

fn batch_get_response_base_encoded_len(req_id: u64) -> usize {
    BatchGetResponse {
        req_id,
        results: Vec::new(),
    }
    .encoded_len()
}

fn max_batch_get_non_value_result_encoded_len(key_len: usize) -> usize {
    bytes_field_len(1, key_len) + varint_field_len(8, 1)
}

fn batch_put_envelope_encoded_len(payload_len: usize) -> usize {
    message_field_len(15, payload_len)
}

fn batch_get_envelope_encoded_len(payload_len: usize) -> usize {
    message_field_len(16, payload_len)
}

fn batch_get_response_envelope_encoded_len(payload_len: usize) -> usize {
    message_field_len(17, payload_len)
}

fn mutation_response_envelope_encoded_len(payload_len: usize) -> usize {
    message_field_len(18, payload_len)
}

fn validate_batch_put_wire_bounds(batch: &BatchPut) -> Result<(), NodeError> {
    let request_len = batch_put_envelope_encoded_len(batch.encoded_len());
    let mut response_payload_len = mutation_response_base_encoded_len();
    for put in &batch.puts {
        response_payload_len +=
            message_field_len(2, max_mutation_result_encoded_len(put.key.len()));
    }
    let response_len = mutation_response_envelope_encoded_len(response_payload_len);
    if request_len > MAX_FRAME_BYTES || response_len > MAX_FRAME_BYTES {
        return Err(NodeError::Codec(format!(
            "batch put exceeds the hard request/response frame bound: request={request_len} response={response_len} limit={MAX_FRAME_BYTES}"
        )));
    }
    Ok(())
}

fn mutation_result(
    key: String,
    request_index: usize,
    result: Result<ApplyOutcome, NodeError>,
) -> MutationResult {
    let mut wire = MutationResult {
        key,
        status: MutationStatus::Applied as i32,
        error: String::new(),
        winner_timestamp_ms: 0,
        winner_logical_counter: 0,
        winner_node_id: String::new(),
        winner_is_tombstone: false,
        request_index: request_index as u32,

        write_concern: None,
    };
    match result {
        Ok(ApplyOutcome::Applied) => {}
        Ok(ApplyOutcome::AlreadyPresent) => {
            wire.status = MutationStatus::AlreadyPresent as i32;
        }
        Ok(ApplyOutcome::Superseded {
            timestamp_ms,
            logical_counter,
            node_id,
            is_tombstone,
        }) => {
            wire.status = MutationStatus::Superseded as i32;
            wire.winner_timestamp_ms = timestamp_ms;
            wire.winner_logical_counter = logical_counter;
            wire.winner_node_id = node_id;
            wire.winner_is_tombstone = is_tombstone;
        }
        Ok(ApplyOutcome::Expired) | Err(NodeError::Expired) => {
            wire.status = MutationStatus::Expired as i32;
        }
        Err(NodeError::Backpressure(error)) => {
            wire.status = MutationStatus::Backpressure as i32;
            wire.error = error;
        }
        Err(NodeError::Storage(error)) => {
            wire.status = MutationStatus::StorageError as i32;
            wire.error = error;
        }
        Err(NodeError::Superseded {
            timestamp_ms,
            logical_counter,
            node_id,
            is_tombstone,
        }) => {
            wire.status = MutationStatus::Superseded as i32;
            wire.winner_timestamp_ms = timestamp_ms;
            wire.winner_logical_counter = logical_counter;
            wire.winner_node_id = node_id;
            wire.winner_is_tombstone = is_tombstone;
        }
        Err(NodeError::UnknownOutcome(error)) => {
            wire.status = MutationStatus::UnknownOutcome as i32;
            wire.error = error;
        }
        Err(NodeError::Network(error)) => {
            wire.status = MutationStatus::Unavailable as i32;
            wire.error = error;
        }
        Err(error) => {
            wire.status = MutationStatus::ActorError as i32;
            wire.error = error.to_string();
        }
    }
    wire.error = truncate_wire_error(wire.error);
    wire
}

fn decode_mutation_result(result: MutationResult) -> Result<(), NodeError> {
    match MutationStatus::try_from(result.status) {
        Ok(MutationStatus::Applied | MutationStatus::AlreadyPresent) => Ok(()),
        Ok(MutationStatus::Superseded) => Err(NodeError::Superseded {
            timestamp_ms: result.winner_timestamp_ms,
            logical_counter: result.winner_logical_counter,
            node_id: result.winner_node_id,
            is_tombstone: result.winner_is_tombstone,
        }),
        Ok(MutationStatus::Expired) => Err(NodeError::Expired),
        Ok(MutationStatus::Backpressure) => Err(NodeError::Backpressure(result.error)),
        Ok(MutationStatus::StorageError) => Err(NodeError::Storage(result.error)),
        Ok(MutationStatus::ActorError) => Err(NodeError::Actor(result.error)),
        Ok(MutationStatus::Unavailable) => Err(NodeError::Network(result.error)),
        Ok(MutationStatus::UnknownOutcome) => Err(NodeError::UnknownOutcome(result.error)),
        Ok(MutationStatus::Unknown) => Err(NodeError::Codec(
            "peer returned an unspecified mutation status".to_string(),
        )),
        Err(_) => Err(NodeError::Codec(format!(
            "unknown mutation status {}",
            result.status
        ))),
    }
}

fn validate_mutation_results(
    expected_keys: &[String],
    results: &[MutationResult],
) -> Result<(), NodeError> {
    if results.len() != expected_keys.len() {
        return Err(NodeError::Codec(format!(
            "forwarded mutation returned {} results, expected {}",
            results.len(),
            expected_keys.len()
        )));
    }
    for (request_index, (expected_key, result)) in expected_keys.iter().zip(results).enumerate() {
        if result.request_index as usize != request_index || &result.key != expected_key {
            return Err(NodeError::Codec(format!(
                "forwarded mutation result {request_index} did not match its request"
            )));
        }
        if matches!(
            MutationStatus::try_from(result.status),
            Ok(MutationStatus::Unknown) | Err(_)
        ) {
            return Err(NodeError::Codec(format!(
                "forwarded mutation result {request_index} has unknown status {}",
                result.status
            )));
        }
        if result.error.len() > MAX_WIRE_ERROR_BYTES {
            return Err(NodeError::Codec(format!(
                "forwarded mutation result {request_index} exceeds the wire error limit"
            )));
        }
        match MutationStatus::try_from(result.status) {
            Ok(MutationStatus::Superseded) => {
                if !result.error.is_empty()
                    || result.winner_node_id.is_empty()
                    || result.winner_node_id.len() > MAX_WIRE_NODE_ID_BYTES
                {
                    return Err(NodeError::Codec(format!(
                        "forwarded mutation result {request_index} has invalid superseded fields"
                    )));
                }
            }
            Ok(
                MutationStatus::Backpressure
                | MutationStatus::StorageError
                | MutationStatus::ActorError
                | MutationStatus::Unavailable
                | MutationStatus::UnknownOutcome,
            ) => {
                if result.winner_timestamp_ms != 0
                    || result.winner_logical_counter != 0
                    || !result.winner_node_id.is_empty()
                    || result.winner_is_tombstone
                {
                    return Err(NodeError::Codec(format!(
                        "forwarded mutation result {request_index} has unexpected winner fields"
                    )));
                }
            }
            Ok(
                MutationStatus::Applied | MutationStatus::AlreadyPresent | MutationStatus::Expired,
            ) => {
                if !result.error.is_empty()
                    || result.winner_timestamp_ms != 0
                    || result.winner_logical_counter != 0
                    || !result.winner_node_id.is_empty()
                    || result.winner_is_tombstone
                {
                    return Err(NodeError::Codec(format!(
                        "forwarded mutation result {request_index} has unexpected fields"
                    )));
                }
            }
            Ok(MutationStatus::Unknown) | Err(_) => unreachable!("rejected above"),
        }
    }
    Ok(())
}

fn validate_batch_get_results(
    expected_keys: &[String],
    results: &[BatchKeyValue],
) -> Result<(), NodeError> {
    if results.len() > expected_keys.len() {
        return Err(NodeError::Codec(format!(
            "batch get returned {} results, requested at most {}",
            results.len(),
            expected_keys.len()
        )));
    }
    for (request_index, (expected_key, result)) in expected_keys.iter().zip(results).enumerate() {
        if &result.key != expected_key {
            return Err(NodeError::Codec(format!(
                "batch get result {request_index} did not match its request"
            )));
        }
        if result.shed {
            if result.found
                || !result.value.is_empty()
                || result.timestamp_ms != 0
                || result.logical_counter != 0
                || !result.node_id.is_empty()
                || result.expires_at_ms != 0
                || result.is_tombstone
            {
                return Err(NodeError::Codec(format!(
                    "batch get result {request_index} has invalid shed fields"
                )));
            }
        } else if result.found {
            validate_record_wire_budget(&result.key, result.value.len(), &result.node_id)?;
        } else if !result.value.is_empty()
            || result.timestamp_ms != 0
            || result.logical_counter != 0
            || !result.node_id.is_empty()
            || result.expires_at_ms != 0
            || result.is_tombstone
        {
            return Err(NodeError::Codec(format!(
                "batch get result {request_index} has invalid not-found fields"
            )));
        }
    }
    Ok(())
}

fn validate_get_response(expected_key: &str, response: &GetResponse) -> Result<(), NodeError> {
    if response.shed {
        if response.found
            || !response.value.is_empty()
            || response.timestamp_ms != 0
            || response.logical_counter != 0
            || !response.node_id.is_empty()
            || response.expires_at_ms != 0
            || response.is_tombstone
        {
            return Err(NodeError::Codec(
                "forwarded get response has invalid shed fields".to_string(),
            ));
        }
    } else if response.found {
        if response.is_tombstone {
            return Err(NodeError::Codec(
                "forwarded get response exposed a tombstone".to_string(),
            ));
        }
        validate_record_wire_budget(expected_key, response.value.len(), &response.node_id)?;
    } else if !response.value.is_empty()
        || response.timestamp_ms != 0
        || response.logical_counter != 0
        || !response.node_id.is_empty()
        || response.expires_at_ms != 0
        || response.is_tombstone
    {
        return Err(NodeError::Codec(
            "forwarded get response has invalid not-found fields".to_string(),
        ));
    }
    Ok(())
}

async fn apply_forwarded_mutation(
    actor: &ActorRef<KvMessage>,
    senders: &SenderMap,
    frag: FragmentId,
    op: ReplicatedOp,
    priority: OpPriority,
    request_index: usize,
    retention: Option<Arc<dyn Send + Sync>>,
) -> MutationResult {
    let key = op.key.clone();
    if let Err(error) = validate_replicated_op(&op) {
        return mutation_result(key, request_index, Err(error));
    }
    let result = match call_apply_retained(actor, op.clone(), priority, false, retention).await {
        // A dispatcher/worker disappearing is routing availability, not a
        // terminal mutation outcome. The stamped operation is idempotent and
        // may be retried at another configured holder.
        Err(NodeError::Actor(error)) => Err(NodeError::Network(error)),
        result => result,
    };
    if let Ok(outcome) = &result {
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent => {
                enqueue_to_senders(senders, frag, &op).await;
            }
            ApplyOutcome::Expired => {
                let mut expiry = op;
                expiry.value = expiry.value.expiry_tombstone();
                enqueue_to_senders(senders, frag, &expiry).await;
            }
            ApplyOutcome::Superseded { .. } => {}
        }
    }
    mutation_result(key, request_index, result)
}

fn validate_replicated_op(op: &ReplicatedOp) -> Result<(), NodeError> {
    validate_record_wire_budget(&op.key, op.value.value.len(), &op.value.node_id)
}

fn validate_record_wire_budget(
    key: &str,
    value_len: usize,
    node_id: &str,
) -> Result<(), NodeError> {
    check_record_budget(key.len(), value_len, node_id.len()).map_err(|error| {
        NodeError::Codec(format!(
            "key/value cannot fit every required peer frame: {error}"
        ))
    })
}

impl std::error::Error for NodeError {}

/// One exact fragment/peer sender covered by a replication-drained shutdown.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicationDrainTarget {
    fragment: FragmentId,
    peer_id: String,
}

impl ReplicationDrainTarget {
    /// Fragment whose fenced LWW versions were reconciled.
    pub fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Replica which acknowledged the configured durability barrier.
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }
}

/// Evidence returned after every frozen replication target acknowledged a
/// stable reconciliation. A successful call also completes local shutdown;
/// [`ReplicationDrainShutdownError::Local`] carries the same evidence when
/// the later local persistence fence fails.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationDrainReport {
    targets: Vec<ReplicationDrainTarget>,
    durability: ReplicationAckDurability,
}

impl ReplicationDrainReport {
    /// Exact sender targets included in the fence, sorted by fragment/peer.
    pub fn targets(&self) -> &[ReplicationDrainTarget] {
        &self.targets
    }

    /// Receiver fence required from every target.
    pub fn durability(&self) -> ReplicationAckDurability {
        self.durability
    }
}

/// Call-specific policy for [`Node::shutdown_with_replication_drain`].
///
/// The timeout is one absolute budget for freezing placement, waiting for
/// admitted mutations, reconnecting peers, reconciliation, and durability
/// barriers. Per-attempt ACK/reconciliation timeouts still govern retries
/// inside that total budget.
#[derive(Clone, Debug, bon::Builder)]
pub struct ReplicationDrainOptions {
    #[builder(default = Duration::from_secs(30))]
    timeout: Duration,
}

impl Default for ReplicationDrainOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ReplicationDrainOptions {
    /// Set the total replication-drain budget. Zero is rejected by the call.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Current total replication-drain budget.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Stage in which a bounded replication-drained shutdown exhausted its
/// call-wide deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReplicationDrainPhase {
    /// Waiting to acquire the node's shutdown coordinator.
    ShutdownCoordination,
    /// Stopping and joining the placement driver to freeze the target set.
    PlacementFreeze,
    /// Waiting for mutations admitted before the fence to reach a terminal state.
    MutationAdmissionDrain,
    /// Resolving the frozen fragment/peer target set.
    TargetSnapshot,
    /// Installing senders and issuing their exact drain generations.
    SenderPreparation,
    /// Reconnecting, reconciling, and waiting for receiver durability barriers.
    PeerReconciliation,
}

impl std::fmt::Display for ReplicationDrainPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ShutdownCoordination => "shutdown coordination",
            Self::PlacementFreeze => "placement freeze",
            Self::MutationAdmissionDrain => "mutation admission drain",
            Self::TargetSnapshot => "target snapshot",
            Self::SenderPreparation => "sender preparation",
            Self::PeerReconciliation => "peer reconciliation",
        };
        f.write_str(name)
    }
}

/// A replication-drained shutdown did not reach its advertised boundary.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReplicationDrainShutdownError {
    /// The call-specific policy is invalid.
    InvalidOptions(String),
    /// The absolute drain deadline elapsed. The node remains quiesced with
    /// transport and fragment state intact; retry this method or explicitly
    /// fall back to local-only [`Node::shutdown`].
    Timeout {
        phase: ReplicationDrainPhase,
        completed: Vec<ReplicationDrainTarget>,
        pending: Vec<ReplicationDrainTarget>,
    },
    /// An exact sender stopped or could not be prepared. The node remains
    /// quiesced with authoritative fragment state intact; retrying this method
    /// installs a replacement sender and establishes a fresh exact fence.
    RetryableSender(String),
    /// A local-only shutdown had already advanced beyond the point where a
    /// remote replication guarantee could still be established.
    Unavailable(String),
    /// Replication completed, but the subsequent local shutdown failed. The
    /// included report remains valid and a retry does not repeat remote work.
    Local {
        error: NodeError,
        replication: ReplicationDrainReport,
    },
}

impl std::fmt::Display for ReplicationDrainShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidOptions(error) => write!(f, "invalid replication drain options: {error}"),
            Self::Timeout {
                phase,
                completed,
                pending,
            } => write!(
                f,
                "replication drain timed out during {phase}: {} completed, {} pending",
                completed.len(),
                pending.len()
            ),
            Self::RetryableSender(error) => {
                write!(f, "replication drain sender must be retried: {error}")
            }
            Self::Unavailable(error) => write!(f, "replication drain unavailable: {error}"),
            Self::Local { error, .. } => {
                write!(f, "replication drained, but local shutdown failed: {error}")
            }
        }
    }
}

impl std::error::Error for ReplicationDrainShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Local { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for NodeError {
    fn from(e: std::io::Error) -> Self {
        NodeError::Io(e)
    }
}

impl From<prost::DecodeError> for NodeError {
    fn from(e: prost::DecodeError) -> Self {
        NodeError::Codec(e.to_string())
    }
}

/// A node in the database cluster.
pub struct Node {
    node_id: String,
    /// Random identifier minted at process start and carried as propagation
    /// metadata. Replay safety comes from the value's LWW version.
    boot_id: u64,
    listen_addr: SocketAddr,
    /// Routes keys to their fragment actors.
    router: FragmentRouter,
    /// Fragments this node authoritatively serves. In static mode this
    /// is fixed at startup; the Phase 8 placement driver mutates it.
    serving: Arc<RwLock<HashSet<FragmentId>>>,
    /// Per-fragment supervisors (own the dispatcher, cleaner,
    /// snapshotter). Stopped on shutdown. Shared with the Phase 8
    /// placement driver, which spawns/stops subtrees at runtime.
    supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>>,
    /// Synchronous stop/join ownership for both active and retiring fragment
    /// supervisors. Ractor actors are self-running after ActorRef drop, so
    /// this registry is also the reliable best-effort Node::drop stop path.
    supervisor_stops: Arc<SupervisorRegistry>,
    /// Placement: fragment -> replica node_ids. Static unless a
    /// placement controller is configured, in which case the driver
    /// keeps it converged with the controller.
    fragment_map: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    /// Per-fragment placement lifecycle (Phase 8; all Serving in
    /// static mode).
    placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>>,
    /// Shared generation of the authoritative placement map.
    placement_generation: Arc<AtomicU64>,
    /// Placement controller (Phase 8). `None` = static placement.
    placement_controller: Option<Arc<dyn PlacementController>>,
    /// How often the placement driver re-evaluates (Phase 8).
    placement_poll_interval: Duration,
    /// Static membership (see `NodeConfig::members`).
    members: HashMap<String, SocketAddr>,
    /// In-flight single-key forwarded reads.
    pending_gets: PendingGets,
    /// In-flight batched forwarded reads: req_id -> reply channel.
    pending_batch_gets: PendingBatchGets,
    /// In-flight distributed scan requests: req_id -> reply channel.
    pending_query_pages: query::PendingQueryPages,
    query_admission: Arc<crate::query::QueryAdmission>,
    /// In-flight single and batched forwarded mutation outcomes.
    pending_mutations: PendingMutations,
    /// Relayed forwarded-gets (Phase 8 re-forwarding).
    pending_relays: RelayMap,
    /// Relayed forwarded-mutation responses (Phase 8 re-forwarding).
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
    peers: PeerMap,
    peer_failures: PeerFailureSignals,
    /// Per-node sequence number for locally originated ops.
    next_seq: Arc<Mutex<u64>>,
    /// Hybrid logical clock for LWW timestamps.
    hlc: Arc<Mutex<HlcClock>>,
    /// Phase 5 senders, keyed by (fragment, peer node id).
    senders: SenderMap,
    /// Fragments whose exact sender set is detached for final handoff proof.
    finalizing_senders: Arc<SenderFinalizationRegistry>,
    /// Sender tasks removed by cancellable lifecycle work but not yet joined.
    retiring_senders: Arc<RetiringSenderRegistry>,
    /// Phase 5 sender tuning.
    sender_config: SenderConfig,
    pub(crate) write_concerns: Arc<crate::write_concern::WriteConcernTracker>,
    /// Transport and replication liveness policy.
    runtime_options: NodeRuntimeOptions,
    storage_executor: Arc<crate::store::executor::StorageExecutor>,
    persistence_budget: Arc<crate::retained::ByteBudget>,
    /// State reports for each sender, keyed by (fragment, peer).
    sender_reports: SenderReportMap,
    /// Coalesced wakeup for the placement driver on membership change (Phase 8).
    membership_tx: Option<watch::Sender<u64>>,
    /// Pre-shared cluster secret for the challenge-bound Hello transcript proof.
    /// This proves fresh possession during the handshake, but plaintext TCP
    /// still has no confidentiality or per-frame integrity.
    /// `None` = no group-membership proof.
    cluster_secret: Option<Vec<u8>>,
    /// Placement polling and gain/loss transitions are isolated so a drained
    /// shutdown can freeze sender topology without aborting peer transports.
    placement_tasks: Arc<RuntimeTasks>,
    tasks: Arc<RuntimeTasks>,
    read_service: Arc<ReadService>,
    inbound_connections: Arc<Semaphore>,
    receive_byte_budget: Arc<Semaphore>,
    node_peer_queue_budget: Arc<NodePeerByteBudget>,
    relay_admission: Arc<RelayAdmission>,
    mutation_service: Arc<MutationService>,
    embedded_operations: Arc<EmbeddedOperations>,
    dial_intents: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
    dial_intent_changes: watch::Sender<u64>,
    peer_changes: watch::Sender<u64>,
    operation_gate: Arc<OperationGate>,
    receive_apply_gate: Arc<OperationGate>,
    local_drain: Arc<drain_persistence::DrainPersistence>,
    table_metrics: observability::TableRegistry,
    peer_authority: Arc<PeerAuthority>,
    shutdown_state: Mutex<ShutdownProgress>,
    lifecycle_observation: Arc<startup::LifecycleObservation>,
    detail_admission: Arc<Semaphore>,
    pool_timing_enabled: Arc<std::sync::atomic::AtomicBool>,
    replication_drain_report: StdMutex<Option<ReplicationDrainReport>>,
    network: Arc<dyn Network>,
    ttl_expiry: Option<ExpiryRuntime>,
    /// Exclusive live attachment to the configured store. Clean shutdown
    /// releases it only after every fragment actor has stopped; cancellation
    /// and best-effort Drop leave managed backends fail-closed.
    store_binding: StdMutex<Option<StoreBinding>>,
    /// Observe the root store without extending its physical backend lifetime
    /// beyond shutdown. Callers retain an Arc for post-shutdown export.
    checkpoint_store: Option<Weak<dyn KvStore>>,
    recovery: Option<Arc<RecoveryContext>>,
}

/// Runtime liveness, queue policy, and region configuration for a [`Node`].
///
/// Configure runtime policies through this type’s builder or setters.
/// [`Node::start`] uses the defaults; pass an override to
/// [`Node::start_with_options`].
#[derive(Clone, Debug, bon::Builder)]
pub struct NodeRuntimeOptions {
    peer_authority: Option<PeerAuthorityOptions>,
    #[builder(skip = Arc::new(std::sync::atomic::AtomicBool::new(false)))]
    observation_timing: Arc<std::sync::atomic::AtomicBool>,
    #[builder(default = 256 * 1024 * 1024)]
    replication_retained_byte_capacity: usize,
    #[builder(default = Duration::from_secs(30))]
    local_work_timeout: Duration,
    #[builder(skip = crate::retained::ByteBudget::new(256 * 1024 * 1024))]
    replication_retention: Arc<crate::retained::ByteBudget>,
    #[builder(default = 64 * 1024 * 1024)]
    reconciliation_retained_byte_capacity: usize,
    #[builder(skip = crate::retained::ByteBudget::new(64 * 1024 * 1024))]
    reconciliation_retention: Arc<crate::retained::ByteBudget>,
    /// Operator-supplied node-to-region labels. Labels compare by exact bytes;
    /// they do not change placement or authenticate physical location.
    #[builder(default, into)]
    regions: Arc<HashMap<String, String>>,
    #[builder(default = PEER_CHANNEL_DEPTH)]
    peer_queue_frame_capacity: usize,
    #[builder(default = NODE_PEER_QUEUE_BYTE_CAPACITY)]
    node_peer_queue_byte_capacity: usize,
    #[builder(default = PEER_CHANNEL_BYTE_CAPACITY)]
    peer_queue_byte_capacity: usize,
    /// Aggregate conservative query/scan result and merge reservations.
    #[builder(default = crate::query::DEFAULT_QUERY_RUNTIME_BYTES)]
    query_runtime_byte_capacity: usize,
    #[builder(default = REPLICATION_CONTROL_CHANNEL_DEPTH)]
    replication_control_message_capacity: usize,
    #[builder(default = INBOUND_CONNECTION_CAPACITY)]
    inbound_connection_capacity: usize,
    #[builder(default = RECEIVE_BYTE_CAPACITY)]
    receive_byte_capacity: usize,
    #[builder(default = RELAY_CAPACITY)]
    relay_capacity: usize,
    #[builder(default = RELAY_KEY_BYTE_CAPACITY)]
    relay_key_byte_capacity: usize,
    #[builder(default = READ_QUEUE_CAPACITY)]
    serving_read_queue_capacity: usize,
    #[builder(default = READ_KEY_BYTE_CAPACITY)]
    serving_read_key_byte_capacity: usize,
    #[builder(default = EMBEDDED_OPERATION_CAPACITY)]
    embedded_operation_capacity: usize,
    #[builder(default = EMBEDDED_OPERATION_BYTE_CAPACITY)]
    embedded_operation_byte_capacity: usize,
    #[builder(default = FORWARDED_MUTATION_CAPACITY)]
    forwarded_mutation_capacity: usize,
    #[builder(default = FORWARDED_MUTATION_BYTE_CAPACITY)]
    forwarded_mutation_byte_capacity: usize,
    #[builder(default = REMOTE_RPC_TIMEOUT)]
    queue_admission_timeout: Duration,
    #[builder(default = RECONCILE_DATA_ADMISSION_TIMEOUT)]
    reconcile_data_admission_timeout: Duration,
    #[builder(default = REMOTE_RPC_TIMEOUT)]
    forwarded_attempt_timeout: Duration,
    #[builder(default = REMOTE_RPC_TIMEOUT)]
    connect_timeout: Duration,
    #[builder(default = HANDSHAKE_TIMEOUT)]
    handshake_timeout: Duration,
    #[builder(default = SOCKET_WRITE_TIMEOUT)]
    socket_read_timeout: Duration,
    #[builder(default = SOCKET_WRITE_TIMEOUT)]
    socket_write_timeout: Duration,
    #[builder(default = Duration::from_secs(30))]
    replication_ack_timeout: Duration,
    #[builder(default = Duration::from_secs(5))]
    health_timeout: Duration,
    /// Optional operational ceiling on acknowledgement counts for new writes.
    /// This weakens requested write concerns and is never applied automatically.
    write_concern_acknowledgement_cap: Option<u32>,
}

impl Default for NodeRuntimeOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl NodeRuntimeOptions {
    /// Opt into exact per-peer process authentication and hot generation fencing.
    /// This mode replaces cluster-secret proofs with independent process keys.
    pub fn with_peer_authority(mut self, authority: PeerAuthorityOptions) -> Self {
        self.peer_authority = Some(authority);
        self
    }
    /// Startup authority configuration. Use `Node::peer_fence_status` for hot
    /// transitions and the currently active authority revision/generations.
    pub fn peer_authority(&self) -> Option<&PeerAuthorityOptions> {
        self.peer_authority.as_ref()
    }
    /// Set aggregate encoded-body admission across every physical peer writer.
    /// Default 256 MiB; minimum 32 MiB + 144 bytes. Ordinary producers leave one maximum
    /// 16 MiB priority response reserved across the Node. This does not include
    /// protobuf objects, frame/task overhead, caches, or kernel socket buffers.
    pub fn with_node_peer_queue_byte_capacity(mut self, capacity: usize) -> Self {
        self.node_peer_queue_byte_capacity = capacity;
        self
    }
    /// Aggregate encoded-body allowance across the Node's peer writers.
    pub fn node_peer_queue_byte_capacity(&self) -> usize {
        self.node_peer_queue_byte_capacity
    }

    /// Set the node-wide query/scan execution byte allowance. Default 256 MiB.
    /// Each coordinator reserves its configured result plus merge limits before
    /// accepting work; incoming peer pages reserve a conservative 32 times their
    /// encoded page limit. Admission rejects immediately at capacity. Ownership
    /// follows actor work after waiter cancellation. Returned caller-owned results,
    /// cache/storage, raw receive frames, and encoded writer queues use separate
    /// budgets. Smaller nonzero allowances can reject otherwise legal queries.
    pub fn with_query_runtime_byte_capacity(mut self, capacity: usize) -> Self {
        self.query_runtime_byte_capacity = capacity;
        self
    }

    /// Current node-wide conservative query/scan execution byte allowance.
    pub fn query_runtime_byte_capacity(&self) -> usize {
        self.query_runtime_byte_capacity
    }

    /// Bound concurrent embedded point/batch requests before dispatcher enqueue.
    /// Default 128. Admission is nonwaiting and returns `NodeError::Backpressure`.
    /// Count ownership follows actor work and accepted mutation completion after
    /// caller cancellation. Query/scan and peer-serving reads have separate limits.
    pub fn with_embedded_operation_capacity(mut self, capacity: usize) -> Self {
        self.embedded_operation_capacity = capacity;
        self
    }
    /// Bound retained embedded request storage, including allocated buffer/vector
    /// capacities and conservative routing/ApplyOp clone overhead. Default 64 MiB.
    /// Cache/persistence/replication values and returned datasets are separate.
    pub fn with_embedded_operation_byte_capacity(mut self, capacity: usize) -> Self {
        self.embedded_operation_byte_capacity = capacity;
        self
    }
    /// Concurrent embedded requests allowed across all fragments and priorities.
    pub fn embedded_operation_capacity(&self) -> usize {
        self.embedded_operation_capacity
    }
    /// Retained embedded request-byte allowance across the Node.
    pub fn embedded_operation_byte_capacity(&self) -> usize {
        self.embedded_operation_byte_capacity
    }

    /// Set the node-to-region map used by region acknowledgement requirements.
    /// Node IDs and region labels must each contain 1 to 255 UTF-8 bytes.
    /// Startup validates supplied entries. Ordinary writes permit an empty or
    /// partial map; region-constrained writes additionally validate the serving
    /// replica set before applying their mutation.
    ///
    /// Configure consistent labels on every node that can receive mutations.
    /// Labels are operator assertions, not measured network locations, and do
    /// not influence replica placement.
    pub fn with_regions(mut self, regions: HashMap<String, String>) -> Self {
        self.regions = Arc::new(regions);
        self
    }

    /// Operator-supplied node-to-region labels.
    pub fn regions(&self) -> &HashMap<String, String> {
        &self.regions
    }

    /// Set the total number of frames buffered per peer connection. At least
    /// two slots are required so data and reader-generated responses each
    /// retain a bounded lane.
    pub fn with_peer_queue_frame_capacity(mut self, capacity: usize) -> Self {
        self.peer_queue_frame_capacity = capacity;
        self
    }

    /// Set the encoded-byte budget for one peer's outbound writer pipeline.
    /// It must fit the hard maximum frame plus the reserved response budget.
    pub fn with_peer_queue_byte_capacity(mut self, capacity: usize) -> Self {
        self.peer_queue_byte_capacity = capacity;
        self
    }

    /// Set the number of ACK/NACK/reconciliation responses retained per
    /// fragment/peer sender. Saturation closes the contributing connection so
    /// its session can replay instead of growing memory without bound.
    pub fn with_replication_control_message_capacity(mut self, capacity: usize) -> Self {
        self.replication_control_message_capacity = capacity;
        self
    }

    /// Set the maximum number of accepted sockets concurrently running the
    /// handshake or serving as inbound peer connections. Excess sockets are
    /// closed immediately after accept.
    pub fn with_inbound_connection_capacity(mut self, capacity: usize) -> Self {
        self.inbound_connection_capacity = capacity;
        self
    }

    /// Set the node-wide encoded-body receive budget across all physical
    /// connections, including handshakes and outbound-dialed peers. At least
    /// one maximum 16 MiB frame must fit. Bodies reserve space before allocation
    /// and retain it through their connection-handler turn. Exhaustion closes
    /// only the contributing connection without waiting for admission.
    ///
    /// This bounds raw frame bodies, not total heap use: protobuf decoding,
    /// actor/cache values, relay state, and caller results allocate separately.
    pub fn with_receive_byte_capacity(mut self, capacity: usize) -> Self {
        self.receive_byte_capacity = capacity;
        self
    }

    /// Set the node-wide limit on read and mutation relays, including pending
    /// timer registration/enqueue and timers which are still exiting. Default
    /// 1024. Admission does not wait: reads remain unavailable and mutations
    /// return explicit `Unavailable` when either relay budget is exhausted.
    pub fn with_relay_capacity(mut self, capacity: usize) -> Self {
        self.relay_capacity = capacity;
        self
    }

    /// Set the node-wide retained request-key storage budget for all relays.
    /// Default 64 MiB. Charges String buffer capacity and String/Vec storage,
    /// including unused vector capacity; it does not charge mutation payloads,
    /// writer buffers, caches, or fixed relay/timer/allocator overhead. The
    /// relay-count limit separately bounds that fixed overhead.
    pub fn with_relay_key_byte_capacity(mut self, capacity: usize) -> Self {
        self.relay_key_byte_capacity = capacity;
        self
    }

    /// Set queued serving reads per priority across this Node. Eight fixed
    /// workers per priority serve single/batch reads outside socket readers.
    /// Admission also counts actor-owned requests after RPC cancellation.
    pub fn with_serving_read_queue_capacity(mut self, capacity: usize) -> Self {
        self.serving_read_queue_capacity = capacity;
        self
    }

    /// Set retained decoded read-key bytes per priority across this Node.
    /// Charges String buffer capacities and Vec/String allocation headers.
    /// This budget is separate from raw receive bodies, returned values, and
    /// per-connection encoded writer queues. The minimum is 16 MiB so one
    /// maximum physical body always fits.
    pub fn with_serving_read_key_byte_capacity(mut self, capacity: usize) -> Self {
        self.serving_read_key_byte_capacity = capacity;
        self
    }

    /// Set the node-wide serving forwarded-mutation completion count, including
    /// queued/active single puts and batches and completed results awaiting
    /// their reader. Default 128. Admission never waits and rejects explicitly
    /// as `Unavailable` before apply. Execution has eight workers per priority,
    /// but the aggregate admission budget is shared across all priorities.
    pub fn with_forwarded_mutation_capacity(mut self, capacity: usize) -> Self {
        self.forwarded_mutation_capacity = capacity;
        self
    }

    /// Set retained serving forwarded-mutation request/RPC payload storage.
    /// Default 64 MiB. Charges allocated request String/Vec storage and buffers,
    /// plus the largest single ApplyOp clone and its correlation key. Does not
    /// charge raw frames, response-builder overhead, caches, sender copies,
    /// allocator overhead, or total heap. Nonzero smaller budgets may reject
    /// otherwise legal requests. Accepted work retains admission through apply,
    /// exact outcomes, fanout and reader encoding, even if its waiter disappears.
    pub fn with_forwarded_mutation_byte_capacity(mut self, capacity: usize) -> Self {
        self.forwarded_mutation_byte_capacity = capacity;
        self
    }

    /// Set one absolute maximum wait to reserve a peer queue slot and its
    /// exact encoded-byte permits.
    pub fn with_queue_admission_timeout(mut self, timeout: Duration) -> Self {
        self.queue_admission_timeout = timeout;
        self
    }

    /// Set the admission wait for each streamed reconciliation-data page.
    /// This is deliberately separate from ordinary control/RPC admission: a
    /// legal near-limit value may need to wait behind another near-limit frame
    /// while the socket writer is still within its own liveness deadline.
    pub fn with_reconcile_data_admission_timeout(mut self, timeout: Duration) -> Self {
        self.reconcile_data_admission_timeout = timeout;
        self
    }

    /// Set the total enqueue-plus-response budget for one forwarded owner
    /// attempt.
    pub fn with_forwarded_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.forwarded_attempt_timeout = timeout;
        self
    }

    /// Set the maximum duration of one outbound transport dial attempt.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the total deadline for the Hello and HelloAuth flights.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set the maximum time to receive a frame body after its length prefix.
    /// Idle established connections have no read deadline until a prefix
    /// arrives.
    pub fn with_socket_read_timeout(mut self, timeout: Duration) -> Self {
        self.socket_read_timeout = timeout;
        self
    }

    /// Set the maximum time allowed for one socket frame write.
    pub fn with_socket_write_timeout(mut self, timeout: Duration) -> Self {
        self.socket_write_timeout = timeout;
        self
    }

    /// Set the maximum wait for an acceptable ordinary replication ACK after
    /// queue admission.
    pub fn with_replication_ack_timeout(mut self, timeout: Duration) -> Self {
        self.replication_ack_timeout = timeout;
        self
    }

    /// Set one aggregate deadline for a local health snapshot, including
    /// supervisor-registry acquisition and every fragment probe. Default 5s.
    /// This does not measure cluster readiness, worker responsiveness, or
    /// durability. Unavailable evidence is returned as an error, not zeros.
    pub fn with_health_timeout(mut self, timeout: Duration) -> Self {
        self.health_timeout = timeout;
        self
    }

    /// Lower acknowledgement requirements for writes received by this node to
    /// at most `cap`. A cap must be positive; startup rejects zero. It never
    /// raises a requested threshold or removes any additional write requirement.
    ///
    /// Use only when intentionally accepting weaker replication assurance.
    pub fn with_write_concern_acknowledgement_cap(mut self, cap: u32) -> Self {
        self.write_concern_acknowledgement_cap = Some(cap);
        self
    }

    /// Restore requested acknowledgement counts in this startup configuration.
    pub fn without_write_concern_acknowledgement_cap(mut self) -> Self {
        self.write_concern_acknowledgement_cap = None;
        self
    }

    /// Optional operational ceiling on acknowledgement counts for new writes.
    pub fn write_concern_acknowledgement_cap(&self) -> Option<u32> {
        self.write_concern_acknowledgement_cap
    }

    /// Current per-peer outbound frame capacity.
    pub fn peer_queue_frame_capacity(&self) -> usize {
        self.peer_queue_frame_capacity
    }

    /// Current encoded-byte budget for one peer's outbound writer pipeline.
    pub fn peer_queue_byte_capacity(&self) -> usize {
        self.peer_queue_byte_capacity
    }

    /// Current per-sender protocol-control message capacity.
    pub fn replication_control_message_capacity(&self) -> usize {
        self.replication_control_message_capacity
    }

    /// Current concurrent inbound-connection limit.
    pub fn inbound_connection_capacity(&self) -> usize {
        self.inbound_connection_capacity
    }

    /// Current node-wide encoded-body receive budget.
    pub fn receive_byte_capacity(&self) -> usize {
        self.receive_byte_capacity
    }

    /// Current node-wide relay count limit.
    pub fn relay_capacity(&self) -> usize {
        self.relay_capacity
    }

    /// Current node-wide retained relay request-key storage budget.
    pub fn relay_key_byte_capacity(&self) -> usize {
        self.relay_key_byte_capacity
    }

    /// Current Node-wide queued serving-read capacity per priority.
    pub fn serving_read_queue_capacity(&self) -> usize {
        self.serving_read_queue_capacity
    }

    /// Current Node-wide retained decoded read-key byte capacity per priority.
    pub fn serving_read_key_byte_capacity(&self) -> usize {
        self.serving_read_key_byte_capacity
    }

    /// Current node-wide serving forwarded-mutation completion count limit.
    pub fn forwarded_mutation_capacity(&self) -> usize {
        self.forwarded_mutation_capacity
    }

    /// Current serving forwarded-mutation request/RPC payload storage budget.
    pub fn forwarded_mutation_byte_capacity(&self) -> usize {
        self.forwarded_mutation_byte_capacity
    }

    /// Current peer-queue admission timeout.
    pub fn queue_admission_timeout(&self) -> Duration {
        self.queue_admission_timeout
    }

    /// Current admission timeout for one reconciliation-data page.
    pub fn reconcile_data_admission_timeout(&self) -> Duration {
        self.reconcile_data_admission_timeout
    }

    /// Current total forwarded-attempt timeout.
    pub fn forwarded_attempt_timeout(&self) -> Duration {
        self.forwarded_attempt_timeout
    }

    /// Current outbound transport-dial timeout.
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Current total handshake timeout.
    pub fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    /// Current per-frame body-read timeout.
    pub fn socket_read_timeout(&self) -> Duration {
        self.socket_read_timeout
    }

    /// Current per-frame socket-write timeout.
    pub fn socket_write_timeout(&self) -> Duration {
        self.socket_write_timeout
    }

    /// Current ordinary replication ACK timeout.
    pub fn replication_ack_timeout(&self) -> Duration {
        self.replication_ack_timeout
    }

    /// Current aggregate local-health snapshot deadline.
    pub fn health_timeout(&self) -> Duration {
        self.health_timeout
    }

    /// Node-wide local reconciliation page allowance; minimum 64 MiB.
    pub fn reconciliation_retained_byte_capacity(&self) -> usize {
        self.reconciliation_retained_byte_capacity
    }
    /// Deadline for embedded actor read waiters and local reconciliation work.
    /// Accepted mutations retain completion ownership after their waiter ends.
    pub fn local_work_timeout(&self) -> Duration {
        self.local_work_timeout
    }
    pub fn with_local_work_timeout(mut self, timeout: Duration) -> Self {
        self.local_work_timeout = timeout;
        self
    }

    fn sender_options(&self) -> SenderRuntimeOptions {
        SenderRuntimeOptions::default()
            .with_observation_timing(self.observation_timing.clone())
            .with_aggregate_retention(self.replication_retention.clone())
            .with_reconciliation_result_budget(self.reconciliation_retention.clone())
            .with_local_work_timeout(self.local_work_timeout)
            .with_ack_timeout(self.replication_ack_timeout)
            .with_queue_admission_timeout(self.queue_admission_timeout)
            .with_control_channel_capacity(self.replication_control_message_capacity)
    }

    /// Current charged retained replication work across this node.
    pub fn replication_retained_bytes(&self) -> usize {
        self.replication_retention.used()
    }

    fn validate(&self) -> Result<(), NodeError> {
        if let Some(authority) = &self.peer_authority {
            authority.validate().map_err(NodeError::Codec)?;
        }
        if self.reconciliation_retained_byte_capacity < 4 * MAX_FRAME_BYTES {
            return Err(NodeError::Actor(
                "reconciliation result allowance must be at least 64 MiB".into(),
            ));
        }
        if self.local_work_timeout.is_zero()
            || Instant::now()
                .checked_add(self.local_work_timeout)
                .is_none()
        {
            return Err(NodeError::Actor(
                "local work timeout must be positive and representable".into(),
            ));
        }
        if self.replication_retained_byte_capacity == 0 {
            return Err(NodeError::Actor(
                "node replication byte allowance must be nonzero".into(),
            ));
        }
        for (node_id, region) in self.regions.iter() {
            if node_id.is_empty() || node_id.len() > MAX_WIRE_NODE_ID_BYTES {
                return Err(NodeError::Actor(format!(
                    "region map node_id must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
                )));
            }
            if region.is_empty() || region.len() > MAX_WIRE_NODE_ID_BYTES {
                return Err(NodeError::Actor(format!(
                    "region label must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
                )));
            }
        }
        if self.write_concern_acknowledgement_cap == Some(0) {
            return Err(NodeError::Actor(
                "write concern acknowledgement cap must be positive".to_string(),
            ));
        }
        for (name, capacity) in [
            (
                "query runtime byte capacity",
                self.query_runtime_byte_capacity,
            ),
            (
                "embedded operation capacity",
                self.embedded_operation_capacity,
            ),
            (
                "embedded operation byte capacity",
                self.embedded_operation_byte_capacity,
            ),
            ("relay capacity", self.relay_capacity),
            ("relay key byte capacity", self.relay_key_byte_capacity),
            (
                "forwarded mutation capacity",
                self.forwarded_mutation_capacity,
            ),
            (
                "forwarded mutation byte capacity",
                self.forwarded_mutation_byte_capacity,
            ),
        ] {
            if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
                return Err(NodeError::Actor(format!(
                    "{name} must be non-zero and at most {}",
                    Semaphore::MAX_PERMITS
                )));
            }
        }
        if self.embedded_operation_byte_capacity > u32::MAX as usize {
            return Err(NodeError::Actor(
                "embedded operation byte capacity exceeds u32 permit range".into(),
            ));
        }
        if self.serving_read_queue_capacity == 0
            || self
                .serving_read_queue_capacity
                .checked_add(READ_WORKERS_PER_PRIORITY)
                .is_none_or(|capacity| capacity > Semaphore::MAX_PERMITS)
        {
            return Err(NodeError::Actor(
                "serving read queue capacity is invalid".to_string(),
            ));
        }
        if self.serving_read_key_byte_capacity < MAX_FRAME_BYTES
            || self.serving_read_key_byte_capacity > Semaphore::MAX_PERMITS
        {
            return Err(NodeError::Actor(format!(
                "serving read key byte capacity must be between {MAX_FRAME_BYTES} and {}",
                Semaphore::MAX_PERMITS,
            )));
        }
        if self.peer_queue_frame_capacity < 2 {
            return Err(NodeError::Actor(
                "peer queue frame capacity must be at least 2".to_string(),
            ));
        }
        if self.node_peer_queue_byte_capacity < 2 * MAX_FRAME_BYTES
            || self.node_peer_queue_byte_capacity > u32::MAX as usize
            || self.node_peer_queue_byte_capacity > Semaphore::MAX_PERMITS
        {
            return Err(NodeError::Actor("node peer queue byte capacity must fit two maximum frames and the u32/semaphore permit range".into()));
        }
        if self.peer_queue_byte_capacity < MIN_PEER_CHANNEL_BYTE_CAPACITY {
            return Err(NodeError::Actor(format!(
                "peer queue byte capacity must be at least {MIN_PEER_CHANNEL_BYTE_CAPACITY}"
            )));
        }
        if self.replication_control_message_capacity == 0 {
            return Err(NodeError::Actor(
                "replication control message capacity must be non-zero".to_string(),
            ));
        }
        if self.inbound_connection_capacity == 0 {
            return Err(NodeError::Actor(
                "inbound connection capacity must be non-zero".to_string(),
            ));
        }
        if self.receive_byte_capacity < MAX_FRAME_BYTES {
            return Err(NodeError::Actor(format!(
                "receive byte capacity must be at least {MAX_FRAME_BYTES}"
            )));
        }
        if self.receive_byte_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "receive byte capacity exceeds maximum {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.peer_queue_frame_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "peer queue frame capacity exceeds maximum {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.peer_queue_byte_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "peer queue byte capacity exceeds maximum {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.replication_control_message_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "replication control message capacity exceeds maximum {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.inbound_connection_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "inbound connection capacity exceeds maximum {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.queue_admission_timeout.is_zero()
            || self.reconcile_data_admission_timeout.is_zero()
            || self.forwarded_attempt_timeout.is_zero()
            || self.connect_timeout.is_zero()
            || self.handshake_timeout.is_zero()
            || self.socket_read_timeout.is_zero()
            || self.socket_write_timeout.is_zero()
            || self.replication_ack_timeout.is_zero()
        {
            return Err(NodeError::Actor(
                "runtime transport and replication timeouts must be non-zero".to_string(),
            ));
        }
        let now = Instant::now();
        if [
            self.queue_admission_timeout,
            self.reconcile_data_admission_timeout,
            self.forwarded_attempt_timeout,
            self.connect_timeout,
            self.handshake_timeout,
            self.socket_read_timeout,
            self.socket_write_timeout,
            self.replication_ack_timeout,
        ]
        .iter()
        .any(|timeout| now.checked_add(*timeout).is_none())
        {
            return Err(NodeError::Actor(
                "runtime transport and replication timeouts must be representable".to_string(),
            ));
        }
        if self.health_timeout.is_zero()
            || Instant::now().checked_add(self.health_timeout).is_none()
        {
            return Err(NodeError::Actor(
                "health timeout must be non-zero and representable".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configuration for a [Node]. Use [`NodeConfig::builder`] to override only
/// the settings you need; omitted fields retain the documented defaults.
/// The non-exhaustive settings retain public fields for inspection and tuning;
/// construct them through the builder or [`Default`] to select validated settings.
/// This is the seed of the operator-facing
/// configuration surface (see the `NodeConfig` productionization issue).
///
/// ```
/// use actordb::actor::PoolConfig;
/// use actordb::node::{NodeConfig, NodeRuntimeOptions};
/// let config = NodeConfig::builder()
///     .high_pool(PoolConfig::builder().workers(4).queue_cap(5_000).build())
///     .maybe_store(None)
///     .build();
/// let runtime = NodeRuntimeOptions::builder()
///     .forwarded_mutation_capacity(256)
///     .forwarded_mutation_byte_capacity(128 * 1024 * 1024)
///     .build();
/// assert_eq!(config.background_pool.rate_limit_per_sec, Some(1000));
/// assert_eq!(runtime.forwarded_mutation_capacity(), 256);
/// ```
///
/// External construction uses the extensible builder:
///
/// ```compile_fail
/// use actordb::node::NodeConfig;
/// let _ = NodeConfig { ..NodeConfig::default() };
/// ```
///
/// Time-based tombstone collection is not a configuration option:
///
/// ```compile_fail
/// use actordb::node::NodeConfig;
/// let _ = NodeConfig::builder().maybe_tombstone_gc_age(None).build();
/// ```
#[derive(Clone, bon::Builder)]
#[non_exhaustive]
pub struct NodeConfig {
    #[builder(skip = Arc::new(std::sync::atomic::AtomicBool::new(false)))]
    pool_timing: Arc<std::sync::atomic::AtomicBool>,
    /// Explicit full-outage recovery authority for a complete durable dynamic
    /// replica set. Default None requires an already-serving bootstrap source.
    pub recovery: Option<RecoveryOptions>,
    /// Optional best-effort local TTL cleanup handler. Disabled by default;
    /// expired value payloads are not copied or retained for notifications then.
    /// Callbacks may run during successful fragment initialization. They do not
    /// establish durable/quorum delivery or replay and can precede a later startup
    /// failure. See [`crate::ttl_expiry`] for ordering, deduplication and cancellation.
    pub ttl_expiry_handler: Option<Arc<dyn TtlExpiryHandler>>,
    /// Bounded callback queue, batching and handler/drain deadlines. Validated
    /// when a handler is configured; unused when notifications are disabled.
    #[builder(default)]
    pub ttl_expiry_options: TtlExpiryOptions,
    /// Optional durable storage behind the node's in-memory cache.
    /// `None` (default) = no persistence; the node is purely in-memory.
    /// With the `rocksdb` feature, `NodeConfig::with_rocksdb` builds a
    /// config with the preferred RocksDB implementation, or bring your
    /// own [`KvStore`].
    pub store: Option<Arc<dyn KvStore>>,
    /// Network transport for the peer mesh. Defaults to plain TCP;
    /// see [`Network`].
    #[builder(default = Arc::new(TcpNetwork))]
    pub network: Arc<dyn Network>,
    /// Flush the store write buffer after this many buffered ops.
    /// Ignored when `store` is `None`.
    #[builder(default = crate::actor::DEFAULT_FLUSH_BATCH_SIZE)]
    pub flush_batch_size: usize,
    /// Per-fragment and aggregate node persistence backlog admission.
    #[builder(default)]
    pub persistence: crate::store::PersistenceConfig,
    /// Flush the store write buffer after this long, even if the batch
    /// isn't full (checked on each applied write). Ignored when `store`
    /// is `None`.
    ///
    /// Crash window: a `kill -9` can lose the unflushed tail, which is
    /// then recovered via peer sync on reconnect. Clean shutdowns
    /// always flush.
    #[builder(default = crate::actor::DEFAULT_FLUSH_INTERVAL)]
    pub flush_interval: Duration,
    /// Number of fragments the keyspace is partitioned into. Each
    /// fragment gets its own actor (and, with persistence, its own
    /// key namespace in the store). Default 1 = no sharding.
    ///
    /// Must be identical on every node: all nodes must agree on
    /// `fragment_of(key)`, or keys become unreachable.
    /// Must not exceed `u32::MAX`; the offered RocksDB backend binds this value
    /// into its persistent identity manifest.
    #[builder(default = 1)]
    pub fragment_count: usize,
    /// Static placement: fragment -> replica node_ids. Must be identical
    /// on every node. A non-empty map must cover exactly
    /// `0..fragment_count`; every replica list must be non-empty and
    /// duplicate-free, and every replica id must contain 1 to 255 bytes.
    /// Empty (default) = this node serves all fragments (single-node / full
    /// full replication).
    #[builder(default)]
    pub fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Static membership: node_id -> address, for dialing fragment owners.
    /// Every id must contain 1 to 255 bytes and addresses must be unique.
    /// [`Node::start`] registers a persistent dial supervisor for every
    /// non-local entry, so its expected id is enforced on the first
    /// authentication attempt and every retry.
    #[builder(default)]
    pub members: HashMap<String, SocketAddr>,
    /// Worker pool configs by priority. The default pool handles normal
    /// ops; high-priority is for latency-sensitive traffic; background
    /// is for bulk/scan (rate-limited).
    #[builder(default)]
    pub default_pool: PoolConfig,
    #[builder(default = PoolConfig::high_priority_defaults())]
    pub high_pool: PoolConfig,
    #[builder(default = PoolConfig::background_defaults())]
    pub background_pool: PoolConfig,
    /// Resident named-table worker pool sets per fragment, including KV.
    /// Fixed base pools are additional. Only idle sets are reclaimed; data,
    /// tombstones and query identities remain resident independently.
    /// Eviction recreates pool incarnations and pool-local token buckets;
    /// this is not a persistent logical-table or node-wide QPS limit.
    #[builder(default = 16)]
    pub table_pool_capacity: usize,
    /// How often each fragment's cleaner sweeps for TTL-expired keys.
    /// Default 30 seconds. Must be nonzero and representable by Tokio's clock.
    #[builder(default = crate::actor::DEFAULT_CLEAN_INTERVAL)]
    pub clean_interval: Duration,
    /// How often each fragment's snapshotter flushes the write buffer
    /// to the store. Default 60 seconds. Must be nonzero and representable by
    /// Tokio's clock, including for in-memory nodes.
    #[builder(default = crate::actor::DEFAULT_SNAPSHOT_INTERVAL)]
    pub snapshot_interval: Duration,
    /// Wall-clock offset (ms) applied to this node's HLC. Zero in
    /// production; tests use it to simulate clock skew. Hidden from
    /// docs: not a tuning knob.
    #[doc(hidden)]
    #[builder(default)]
    pub clock_skew_ms: i64,
    /// Maximum a remote or persisted timestamp may exceed this node's wall
    /// clock before the connection or durable startup is rejected. The raw
    /// out-of-policy value is never stored. Without this bound, one bad
    /// timestamp permanently poisons last-writer-wins cluster-wide. Default
    /// 5 minutes.
    #[builder(default = Duration::from_secs(300))]
    pub max_clock_skew: Duration,
    /// Pre-shared secret for mutual HMAC proofs over both peers' fresh
    /// challenges, identities, exact protocol versions, and connection
    /// roles. A captured handshake cannot be replayed against a new challenge.
    /// This is cluster-wide membership authentication, not per-node keys, and
    /// plaintext TCP still provides neither confidentiality nor integrity for
    /// post-handshake frames; use a protected custom [`Network`] transport
    /// when those properties matter. Must contain at least 32 independently
    /// generated, high-entropy random bytes and be identical on every node.
    /// A guessable password remains vulnerable to offline guessing from a
    /// captured transcript even though challenges prevent replay. `None`
    /// (default) sends no proof.
    pub cluster_secret: Option<Vec<u8>>,
    /// Phase 5 async replication tuning (batching, acks, buffers).
    #[builder(default)]
    pub replication: SenderConfig,
    /// Phase 8: dynamic placement controller. `None` (default) =
    /// static placement: the node's fragments are fixed at startup
    /// from `fragment_map`; every established replica session starts
    /// correlated reconciliation. Set to an `Arc<dyn PlacementController>` (e.g.
    /// [`ManualPlacement`](crate::placement::ManualPlacement)) to opt
    /// into dynamic placement: a driver task polls the controller,
    /// spawns/drains fragment subtrees at runtime, bootstraps newly
    /// assigned fragments via the same reconciler, and publishes only
    /// after an authoritative serving source completes verification.
    /// Membership-change callbacks are coalesced current-peer-set
    /// notifications, not a lossless ordered event stream.
    pub placement_controller: Option<Arc<dyn PlacementController>>,
    /// How often the Phase 8 placement driver re-evaluates desired
    /// placement. Ignored in static mode. Default 1 second.
    #[builder(default = Duration::from_secs(1))]
    pub placement_poll_interval: Duration,
    /// How often to warn while a revoked fragment cannot safely finish
    /// draining. This is an observability interval, not a destructive
    /// timeout: the old owner retains its recovery state until every sender
    /// has drained. Ignored in static mode. Default 10 seconds.
    #[builder(default = Duration::from_secs(10))]
    pub placement_drain_warn_interval: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(feature = "rocksdb")]
impl NodeConfig {
    /// Convenience: a config persisting to RocksDB at `path`, the
    /// preferred durable implementation. Ordinary batches use synchronous WAL
    /// commits; use [`NodeConfig::with_rocksdb_options`] for an explicit policy.
    pub fn with_rocksdb(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::store::StoreError> {
        Self::with_rocksdb_options(path, crate::store::RocksDbOptions::default())
    }

    /// Convenience: a config persisting to RocksDB with explicit open options.
    pub fn with_rocksdb_options(
        path: impl AsRef<std::path::Path>,
        options: crate::store::RocksDbOptions,
    ) -> Result<Self, crate::store::StoreError> {
        Ok(Self::builder()
            .store(Arc::new(crate::store::RocksDbStore::open_with_options(
                path, options,
            )?))
            .build())
    }
}

impl Node {
    /// Encode a public literal into KV. Internal qualification is selected by
    /// private entrypoint, never by recognizing authority in String syntax.
    fn qualify_key(key: String) -> String {
        let original_capacity = key.capacity();
        let mut encoded = crate::table_key::encode_table_key("KV", &key);
        encoded.reserve(original_capacity);
        encoded
    }

    /// Aggregate encoded-body reservations and saturation across all peers.
    /// Includes queued and active socket writes; excludes unencoded payloads,
    /// frame/task/allocator overhead, cache/storage, and kernel socket buffers.
    pub fn peer_writer_diagnostics(&self) -> PeerWriterDiagnostics {
        self.node_peer_queue_budget.diagnostics()
    }

    /// Current embedded request count and retained request-byte admission use.
    /// Includes actor/owner requests after cancelled or timed-out waiters.
    pub fn embedded_operation_diagnostics(&self) -> EmbeddedOperationDiagnostics {
        self.embedded_operations.diagnostics()
    }

    /// Current charged replication payload bytes across every local sender.
    pub fn replication_retained_bytes(&self) -> usize {
        self.runtime_options.replication_retention.used()
    }

    /// Charged local reconciliation page ownership across every sender.
    pub fn reconciliation_retained_bytes(&self) -> usize {
        self.runtime_options.reconciliation_retention.used()
    }
    /// Current charged authoritative persistence bytes across local fragments.
    pub fn persistence_retained_bytes(&self) -> usize {
        self.persistence_budget.used()
    }
    /// Submitted synchronous store jobs still owning execution slots.
    pub fn active_storage_jobs(&self) -> usize {
        self.storage_executor.active()
    }

    /// The fragment actor serving `key` (`None` if remote or not yet
    /// serving — e.g. still bootstrapping under Phase 8).
    fn fragment_actor(&self, key: &str) -> Option<ActorRef<KvMessage>> {
        let frag = fragment_of(key, self.router.fragment_count);
        if self.serving.read().unwrap().contains(&frag) {
            self.router.route(key)
        } else {
            None
        }
    }

    /// Ensure a Phase 5 sender exists for (`fragment`, `peer`).
    /// Idempotent; also used by the Phase 8 placement driver.
    async fn ensure_sender(&self, fragment: FragmentId, peer: &str) {
        ensure_sender_entry(
            fragment,
            peer,
            SenderEntryDeps {
                config: &self.sender_config,
                runtime: &self
                    .runtime_options
                    .sender_options()
                    .with_write_concerns(self.write_concerns.clone())
                    .with_recovery(self.recovery.clone()),
                router: &self.router,
                hlc: &self.hlc,
                senders: &self.senders,
                finalizing_senders: &self.finalizing_senders,
                retiring_senders: &self.retiring_senders,
                reports: &self.sender_reports,
                peer_failures: &self.peer_failures,
                operation_gate: &self.operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
    }

    /// Replica holders for `frag` in preference order: this node first
    /// (if it serves the fragment), then the others in map order. Empty
    /// when the fragment has no usable replica (misconfiguration), or
    /// when there is no placement map (everything is local).
    fn replica_order(&self, frag: FragmentId) -> Vec<String> {
        let map = self.fragment_map.read().unwrap();
        if map.is_empty() {
            return Vec::new(); // no map: everything is local
        }
        let Some(replicas) = map.get(&frag) else {
            return Vec::new();
        };
        // Prefer ourselves, but only while we actually serve the
        // fragment (Phase 8: a bootstrapping node forwards to the
        // previous owner instead of erroring).
        // (Load-aware selection is a later optimization.)
        let serves = replicas.iter().any(|id| id == &self.node_id)
            && self.serving.read().unwrap().contains(&frag);
        let mut order: Vec<String> = replicas
            .iter()
            .filter(|id| *id != &self.node_id)
            .cloned()
            .collect();
        if serves {
            order.insert(0, self.node_id.clone());
        }
        order
    }

    /// Outbound channel to `node_id`, if connected.
    async fn peer_sender(&self, node_id: &str) -> Option<PeerChannel> {
        self.peers.lock().await.get(node_id).cloned()
    }

    /// Write a key. The op is applied locally when this node serves the
    /// fragment; otherwise the exact stamped version is tried across the
    /// configured replica holders in placement order.
    ///
    /// Local and forwarded puts both wait for the serving fragment's exact
    /// apply outcome. An `Ok` means that version was applied or was already
    /// present at that apply point; admission, storage, expiry, and
    /// supersession are typed errors. `Ok` does not force a persistence flush
    /// or await asynchronous fan-out to the other replicas.
    pub async fn put(&self, key: String, value: Vec<u8>) -> Result<(), NodeError> {
        self.put_qualified_with_options(Self::qualify_key(key), value, None, OpPriority::Default)
            .await
    }

    /// Write with explicit priority (Phase 7). High priority is for
    /// latency-sensitive ops; background is for bulk (rate-limited).
    /// The priority is honored end-to-end: forwarded puts carry it on
    /// the wire and the owner applies them at the same priority.
    ///
    /// This waits for the serving worker's exact apply outcome, including when
    /// the fragment is remote. After it returns `Ok`, reads on the serving
    /// replica observe this version or a newer winner.
    pub async fn put_with_priority(
        &self,
        key: String,
        value: Vec<u8>,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        self.put_qualified_with_options(Self::qualify_key(key), value, None, priority)
            .await
    }

    /// Write with optional TTL and explicit priority, preserving one exact expiry deadline.
    pub async fn put_with_options(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        self.put_qualified_with_options(Self::qualify_key(key), value, ttl, priority)
            .await
    }

    pub(super) async fn put_qualified_with_options(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        let operation = self.admit_key_operation(&key)?;
        let retention = self.embedded_operations.admit_owned(
            embedded_operations::single_request_bytes(&key, Some(&value)),
            Some(operation.clone()),
        )?;
        self.put_with_ttl_and_priority(key, value, ttl, priority, retention)
            .await
    }

    /// Internal: put with TTL and priority. The priority is honored
    /// on both paths: local applies go to the priority's pool, and
    /// forwarded puts carry the priority on the wire so the owner
    /// applies them at the same priority.
    async fn put_with_ttl_and_priority(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
        retention: Arc<RequestRetention>,
    ) -> Result<(), NodeError> {
        // Capture and validate expiry before waiting for sequence/clock state
        // or constructing an operation. Invalid TTLs cannot consume a stamp,
        // reach a serving actor, or enqueue a forwarded mutation. None avoids
        // an extra wall-clock read on the ordinary non-TTL put path.
        let expires_at_ms = ttl
            .map(|ttl| ttl_expiry_deadline_ms(crate::actor::now_ms(), ttl))
            .transpose()?;
        let seq = {
            let mut guard = self.next_seq.lock().await;
            *guard += 1;
            *guard
        };
        let now = self
            .hlc
            .lock()
            .await
            .tick()
            .map_err(|error| NodeError::Codec(error.to_string()))?;
        let mut vv = VersionedValue::new_version(value, now, self.node_id.clone());
        vv.expires_at_ms = expires_at_ms;
        let op = ReplicatedOp {
            key,
            value: vv,
            seq,
            boot_id: self.boot_id,
        };
        self.apply_local_with_priority(op, priority, retention)
            .await
    }

    /// Write `value` under `key`, with an optional time-to-live. When
    /// `ttl` is set, expiry is anchored to the originating node's wall clock
    /// before sequence/clock waits and local or forwarded apply. HLC logical
    /// advancement does not extend the TTL. Durations have millisecond
    /// precision: sub-millisecond parts are truncated, and zero or a duration
    /// below one millisecond sets expiry to the captured origin wall time.
    /// Each serving/reading node evaluates expiry against its own wall clock;
    /// a slower clock can delay the expired apply outcome or read invisibility.
    /// `None` means no expiry.
    ///
    /// A duration or absolute expiry that cannot fit the nonzero `u64`
    /// millisecond wire/store deadline returns [`NodeError::Codec`] before
    /// consuming a sequence/HLC stamp or admitting a mutation. Normal public
    /// shutdown admission still takes precedence over input validation.
    ///
    /// Reads treat the value as absent after expiry, and the fragment's cleaner
    /// materializes and persists a versioned expiry tombstone on its next
    /// sweep so older replicas cannot resurrect it.
    ///
    /// Local and forwarded puts wait for the serving fragment's exact apply
    /// outcome before returning.
    pub async fn put_with_ttl(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), NodeError> {
        self.put_qualified_with_options(Self::qualify_key(key), value, ttl, OpPriority::Default)
            .await
    }

    /// Delete `key`: writes a tombstone that propagates to all replicas
    /// like a put. Reads treat tombstoned keys as absent; sync ships
    /// tombstones so deletes survive node restarts and re-joins. A later
    /// `put` with a higher timestamp resurrects the key.
    pub async fn delete(&self, key: String) -> Result<(), NodeError> {
        self.delete_qualified_with_priority(Self::qualify_key(key), OpPriority::Default)
            .await
    }

    /// Delete a key through the selected priority pool.
    pub async fn delete_with_priority(
        &self,
        key: String,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        self.delete_qualified_with_priority(Self::qualify_key(key), priority)
            .await
    }

    pub(super) async fn delete_qualified_with_priority(
        &self,
        key: String,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        let operation = self.admit_key_operation(&key)?;
        let retention = self.embedded_operations.admit_owned(
            embedded_operations::single_request_bytes(&key, None),
            Some(operation.clone()),
        )?;
        let seq = {
            let mut guard = self.next_seq.lock().await;
            *guard += 1;
            *guard
        };
        let now = self
            .hlc
            .lock()
            .await
            .tick()
            .map_err(|error| NodeError::Codec(error.to_string()))?;
        let op = ReplicatedOp {
            key: key.clone(),
            value: VersionedValue::tombstone_version(now, self.node_id.clone()),
            seq,
            boot_id: self.boot_id,
        };
        self.apply_local_with_priority(op, priority, retention)
            .await
    }

    async fn apply_local_with_priority(
        &self,
        op: ReplicatedOp,
        priority: OpPriority,
        retention: Arc<RequestRetention>,
    ) -> Result<(), NodeError> {
        self.apply_routed_with_concern(op, priority, None, retention)
            .await
            .map(|_| ())
            .map_err(crate::write_concern::WriteConcernError::into_node_error)
    }

    /// Read a key locally when this node serves the fragment; otherwise try
    /// the configured replica holders in placement order. An unavailable
    /// holder is distinct from a valid not-found response.
    pub async fn get(&self, key: String) -> Result<Option<VersionedValue>, NodeError> {
        self.get_qualified_with_priority(Self::qualify_key(key), OpPriority::Default)
            .await
    }

    /// Read a key with an explicit priority. High-priority reads go to
    /// the high pool (isolated from background bulk traffic); background
    /// reads go to the background pool (rate-limited, sheddable).
    ///
    /// Once a local [`put_with_priority`](Self::put_with_priority) returns,
    /// the shared fragment cache makes its result visible to reads in every
    /// priority pool.
    pub async fn get_with_priority(
        &self,
        key: String,
        priority: OpPriority,
    ) -> Result<Option<VersionedValue>, NodeError> {
        self.get_qualified_with_priority(Self::qualify_key(key), priority)
            .await
    }

    pub(super) async fn get_qualified_with_priority(
        &self,
        key: String,
        priority: OpPriority,
    ) -> Result<Option<VersionedValue>, NodeError> {
        let operation = self.admit_key_operation(&key)?;
        let retention = self.embedded_operations.admit_owned(
            embedded_operations::single_request_bytes(&key, None),
            Some(operation.clone()),
        )?;
        self.get_with_priority_admitted(key, priority, retention)
            .await
    }

    async fn get_with_priority_admitted(
        &self,
        key: String,
        priority: OpPriority,
        retention: Arc<RequestRetention>,
    ) -> Result<Option<VersionedValue>, NodeError> {
        if let Some(actor) = self.fragment_actor(&key) {
            let ownership: Arc<dyn Send + Sync> = Arc::new((retention, self.admit_operation()?));
            let res = actor
                .call(
                    |reply| {
                        KvMessage::GetBatchRetained(crate::actor::RetainedBatchRead::new(
                            vec![key],
                            0,
                            MAX_FRAME_BYTES,
                            reply,
                            priority,
                            ownership,
                        ))
                    },
                    Some(self.runtime_options.local_work_timeout()),
                )
                .await
                .map_err(|error| NodeError::Actor(error.to_string()))?;
            return match res {
                CallResult::Success(mut page) if page.entries.len() == 1 => page
                    .entries
                    .remove(0)
                    .result
                    .map_err(NodeError::Backpressure),
                CallResult::Success(_) => {
                    Err(NodeError::Actor("point read result cardinality".into()))
                }
                CallResult::Timeout => {
                    self.note_waiter_timeout();
                    Err(NodeError::Actor("get timed out".into()))
                }
                CallResult::SenderError => {
                    Err(NodeError::Actor("get reply channel dropped".into()))
                }
            };
        }

        // Remote fragment: retain the hop-bounded single-read protocol so a
        // stale holder can relay across temporarily disagreeing placement
        // maps. Transport, timeout, malformed-response, and actor-unavailable
        // silence retry the next holder; a valid not-found is terminal.
        let frag = fragment_of(&key, self.router.fragment_count);
        let replicas = self.replica_order(frag);
        if replicas.is_empty() {
            return Err(NodeError::Actor(format!("no replica for fragment {frag}")));
        }
        let wire_priority = priority_to_proto(priority);
        check_envelope_budget(
            &Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: key.clone(),
                    req_id: u64::MAX,
                    priority: wire_priority,
                    hops: MAX_FORWARD_HOPS,
                })),
            },
            MAX_FRAME_BYTES,
        )
        .map_err(|error| {
            NodeError::Codec(format!("get key cannot fit its request frame: {error}"))
        })?;

        let mut tried = Vec::with_capacity(replicas.len());
        for owner in replicas {
            tried.push(owner.clone());
            let Some(tx) = self.peer_sender(&owner).await else {
                continue;
            };
            let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
            let (resp_tx, resp_rx) = oneshot::channel();
            self.pending_gets.lock().unwrap().insert(
                req_id,
                PendingGet {
                    expected_peer: owner,
                    expected_key: key.clone(),
                    reply: resp_tx,
                },
            );
            let _pending_guard = PendingGetGuard {
                req_id,
                pending: self.pending_gets.clone(),
            };
            let attempt_deadline = deadline_after(self.runtime_options.forwarded_attempt_timeout);
            if !try_send_envelope_until(
                &tx,
                Envelope {
                    msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                        key: key.clone(),
                        req_id,
                        priority: wire_priority,
                        hops: 0,
                    })),
                },
                attempt_deadline,
            )
            .await
            {
                continue;
            }
            let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
            match response {
                Ok(Ok(Ok(value))) => return Ok(value),
                Ok(Ok(Err(error @ NodeError::Backpressure(_)))) => return Err(error),
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => continue,
            }
        }
        Err(NodeError::Network(format!(
            "no replica answered get for fragment {frag} (tried {})",
            tried.join(",")
        )))
    }

    /// Read the full local copy across all fragments (debugging / tests).
    /// Request count admission is bounded; use bounded queries/scans for
    /// production reads.
    pub async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let operation = self.admit_table_operation("KV")?;
        let retention = self
            .embedded_operations
            .admit_owned(std::mem::size_of::<KvMessage>(), Some(operation.clone()))?;
        let ownership: Arc<dyn Send + Sync> = Arc::new((retention, self.admit_operation()?));
        let all = tokio::time::timeout(
            self.runtime_options.local_work_timeout(),
            self.router.get_all_retained(Some(ownership)),
        )
        .await
        .map_err(|_| {
            self.note_waiter_timeout();
            NodeError::Actor("get_all local work timed out".into())
        })??;
        // Hide tombstones from user reads (sync needs them, users don't), and
        // present the built-in KV table's keys in its user-facing form.
        let mut displayed = HashMap::new();
        for (key, value) in all.into_iter().filter(|(_, value)| !value.is_tombstone) {
            let key = crate::table_key::decode_table_key("KV", &key).unwrap_or(key);
            if displayed.insert(key, value).is_some() {
                return Err(NodeError::Codec(
                    "ambiguous all-table display key; use per-table queries".into(),
                ));
            }
        }
        Ok(displayed)
    }

    /// Put multiple keys with an explicit priority (Phase 7). Bulk
    /// callers should prefer [`OpPriority::Background`]; the priority is
    /// carried on the wire and the owner applies the batch at the same
    /// priority, so fan-out preserves traffic isolation end-to-end.
    ///
    /// Each key is hashed to its fragment; keys are grouped by fragment
    /// and each fragment's keys are sent in exact-byte/count-bounded `BatchPut` messages,
    /// failing over across replica holders (self first) until each position
    /// has a terminal outcome or holders are exhausted. Returns per-key
    /// results in input order.
    ///
    /// Local and remote keys await an exact per-key apply outcome. Remote
    /// batches use the configured timeout (5 seconds by default) per replica
    /// attempt and fail over idempotently when a connection drops or no
    /// response arrives.
    pub async fn put_multi_with_priority(
        &self,
        kvs: Vec<(String, Vec<u8>)>,
        priority: OpPriority,
    ) -> Vec<Result<(), NodeError>> {
        let kvs: Vec<(String, Vec<u8>)> = kvs
            .into_iter()
            .map(|(key, value)| (Self::qualify_key(key), value))
            .collect();
        self.put_multi_qualified_with_priority(kvs, priority).await
    }

    pub(super) async fn put_multi_qualified_with_priority(
        &self,
        kvs: Vec<(String, Vec<u8>)>,
        priority: OpPriority,
    ) -> Vec<Result<(), NodeError>> {
        let operation = match self.admit_batch_operation(kvs.iter().map(|(key, _)| key.as_str())) {
            Ok(operation) => operation,
            Err(_) => {
                return kvs
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self.embedded_operations.admit_batch_owned(
            embedded_operations::batch_put_request_bytes(&kvs),
            Some(operation.clone()),
        ) {
            Ok(retention) => retention,
            Err(error) => return embedded_operations::reject_batch(kvs, error),
        };
        self.put_multi_with_priority_admitted(kvs, priority, retention)
            .await
    }

    async fn put_multi_with_priority_admitted(
        &self,
        kvs: Vec<(String, Vec<u8>)>,
        priority: OpPriority,
        retention: Arc<RequestRetention>,
    ) -> Vec<Result<(), NodeError>> {
        // Partition into local ops and remote groups (by fragment).
        let mut local: Vec<(usize, String, Vec<u8>)> = Vec::new();
        let mut remote: HashMap<FragmentId, Vec<(usize, String, Vec<u8>)>> = HashMap::new();
        let mut results: Vec<Option<Result<(), NodeError>>> =
            (0..kvs.len()).map(|_| None).collect();

        for (i, (key, value)) in kvs.into_iter().enumerate() {
            if self.fragment_actor(&key).is_some() {
                local.push((i, key, value));
            } else {
                let frag = fragment_of(&key, self.router.fragment_count);
                if self.replica_order(frag).is_empty() {
                    results[i] = Some(Err(NodeError::Actor(format!(
                        "no replica for fragment {frag}"
                    ))));
                } else {
                    remote.entry(frag).or_default().push((i, key, value));
                }
            }
        }

        // Bound calls before dispatcher admission: a pool queue cap covers
        // active/worker-queued work, not an arbitrary backlog already sitting
        // in the shared dispatcher mailbox.
        let mut local = local.into_iter();
        let local_window =
            MAX_LOCAL_BATCH_IN_FLIGHT.min(self.runtime_options.embedded_operation_capacity());
        loop {
            let chunk: Vec<_> = local.by_ref().take(local_window).collect();
            if chunk.is_empty() {
                break;
            }
            let local_futs = chunk.into_iter().map(|(i, key, value)| {
                let this = self;
                let retention = retention.clone();
                async move {
                    let r = this
                        .put_with_ttl_and_priority(key, value, None, priority, retention)
                        .await;
                    (i, r)
                }
            });
            for (i, r) in futures::future::join_all(local_futs).await {
                results[i] = Some(r);
            }
        }

        // Remote puts: exact-byte- and count-bounded BatchPut chunks per
        // fragment. Unavailable positions fail over independently while
        // completed positions retain their exact per-key outcomes.
        // The request and a conservative worst-case response are both sized
        // incrementally, avoiding quadratic protobuf rescans.
        let wire_priority = priority_to_proto(priority);
        for (frag, items) in remote {
            let mut remaining = VecDeque::new();
            for (index, key, value) in items {
                if let Err(error) = validate_record_wire_budget(&key, value.len(), &self.node_id) {
                    results[index] = Some(Err(error));
                    continue;
                }
                let seq = {
                    let mut guard = self.next_seq.lock().await;
                    *guard += 1;
                    *guard
                };
                let version = match self.hlc.lock().await.tick() {
                    Ok(version) => version,
                    Err(error) => {
                        results[index] = Some(Err(NodeError::Codec(error.to_string())));
                        continue;
                    }
                };
                remaining.push_back((
                    index,
                    ForwardedPut {
                        key,
                        value,
                        timestamp_ms: version.timestamp_ms,
                        logical_counter: version.logical_counter,
                        node_id: self.node_id.clone(),
                        seq,
                        boot_id: self.boot_id,
                        expires_at_ms: 0,
                        is_tombstone: false,
                        priority: wire_priority,
                        hops: 0,
                        req_id: 0,

                        write_concern: None,
                    },
                ));
            }

            while !remaining.is_empty() {
                let mut chunk = Vec::new();
                let mut request_payload_len = batch_put_base_encoded_len(wire_priority);
                let mut response_payload_len = mutation_response_base_encoded_len();
                while chunk.len() < MAX_WIRE_BATCH_ITEMS {
                    let Some((index, put)) = remaining.pop_front() else {
                        break;
                    };
                    let next_request_payload =
                        request_payload_len + message_field_len(1, put.encoded_len());
                    let next_response_payload = response_payload_len
                        + message_field_len(2, max_mutation_result_encoded_len(put.key.len()));
                    let request_len = batch_put_envelope_encoded_len(next_request_payload);
                    let response_len =
                        mutation_response_envelope_encoded_len(next_response_payload);

                    if !chunk.is_empty()
                        && (request_len > WIRE_BATCH_PAGE_BYTES
                            || response_len > WIRE_BATCH_PAGE_BYTES)
                    {
                        remaining.push_front((index, put));
                        break;
                    }
                    if request_len > MAX_FRAME_BYTES || response_len > MAX_FRAME_BYTES {
                        results[index] = Some(Err(NodeError::Codec(format!(
                            "batch put item cannot fit its request/response frames: request={request_len} response={response_len} limit={MAX_FRAME_BYTES}"
                        ))));
                        continue;
                    }
                    request_payload_len = next_request_payload;
                    response_payload_len = next_response_payload;
                    chunk.push((index, put));
                }
                if chunk.is_empty() {
                    continue;
                }

                // Keep original input positions alongside exact stamped puts.
                // A valid response may finish only part of the batch; retry
                // its unavailable positions without replaying terminal ones.
                let mut unresolved = chunk;
                let mut tried: Vec<String> = Vec::new();
                for owner in self.replica_order(frag) {
                    tried.push(owner.clone());
                    let Some(tx) = self.peer_sender(&owner).await else {
                        continue;
                    };
                    let expected_keys: Vec<String> =
                        unresolved.iter().map(|(_, put)| put.key.clone()).collect();
                    let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
                    let (resp_tx, resp_rx) = oneshot::channel();
                    self.pending_mutations.lock().unwrap().insert(
                        req_id,
                        PendingMutation {
                            expected_peer: owner.clone(),
                            expected_keys: expected_keys.clone(),
                            reply: resp_tx,
                        },
                    );
                    let _pending_guard = PendingMutationGuard {
                        req_id,
                        pending: self.pending_mutations.clone(),
                    };
                    let attempt_deadline =
                        deadline_after(self.runtime_options.forwarded_attempt_timeout);
                    if !try_send_envelope_until(
                        &tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                                puts: unresolved.iter().map(|(_, put)| put.clone()).collect(),
                                priority: wire_priority,
                                req_id,
                            })),
                        },
                        attempt_deadline,
                    )
                    .await
                    {
                        continue;
                    }
                    let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
                    match response {
                        Ok(Ok(wire_results))
                            if validate_mutation_results(&expected_keys, &wire_results).is_ok() =>
                        {
                            // Validate the entire response before completing
                            // any position. A stable filter preserves duplicate
                            // keys and order; response indexes restart at zero
                            // for the next holder's reduced request.
                            let mut unavailable = Vec::new();
                            for ((index, put), result) in unresolved.into_iter().zip(wire_results) {
                                if MutationStatus::try_from(result.status)
                                    == Ok(MutationStatus::Unavailable)
                                {
                                    unavailable.push((index, put));
                                } else {
                                    results[index] = Some(decode_mutation_result(result));
                                }
                            }
                            unresolved = unavailable;
                            if unresolved.is_empty() {
                                break;
                            }
                        }
                        // A timeout or malformed/dropped response may follow a
                        // successful apply. Retrying the exact stamped versions
                        // at another holder is safe under the idempotent merge.
                        _ => {}
                    }
                }
                for (index, _) in unresolved {
                    results[index] = Some(Err(NodeError::Network(format!(
                        "no replica acknowledged batch put for fragment {frag} (tried {})",
                        tried.join(",")
                    ))));
                }
            }
        }

        results.into_iter().map(|r| r.unwrap()).collect()
    }

    /// Put multiple keys. Each key is hashed to its fragment; keys are
    /// grouped by fragment and each fragment's keys are sent in exact-byte/count-bounded
    /// `BatchPut` messages, failing over across replica holders.
    /// Returns per-key results in input order.
    ///
    /// Bulk puts run at [`OpPriority::Background`]; see
    /// [`put_multi_with_priority`](Self::put_multi_with_priority) for an
    /// explicit priority. Local and remote keys both return exact per-key
    /// outcomes from the serving fragment.
    pub async fn put_multi(&self, kvs: Vec<(String, Vec<u8>)>) -> Vec<Result<(), NodeError>> {
        self.put_multi_with_priority(kvs, OpPriority::Background)
            .await
    }

    /// Get multiple keys with an explicit priority (Phase 7). Bulk
    /// callers should prefer [`OpPriority::Background`]; the priority is
    /// carried on the wire and the owner serves the batch at the same
    /// priority, so fan-out preserves traffic isolation end-to-end.
    ///
    /// Each key is hashed to its fragment; keys are grouped by fragment
    /// and each fragment's keys are fetched in exact-byte/count-bounded `BatchGet`
    /// pull requests. Large replies advance as validated ordered prefixes.
    /// Returns per-key results in input order.
    ///
    /// Remote reads use the configured timeout (5 seconds by default) per
    /// replica attempt and fail over to the next replica holder when the preferred owner is
    /// unreachable or stops responding; a dead owner fails only its
    /// keys, and only when no replica holder answers. Errors are
    /// classified: `NodeError::Actor` for misconfiguration (no replica
    /// listed for a fragment), `NodeError::Network` for connectivity
    /// failures (not connected, timed out, all replicas unreachable),
    /// `NodeError::Backpressure` when the (local or remote) pool shed
    /// the read — never confused with "not found".
    pub async fn get_multi_with_priority(
        &self,
        keys: Vec<String>,
        priority: OpPriority,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        let keys: Vec<String> = keys.into_iter().map(Self::qualify_key).collect();
        self.get_multi_qualified_with_priority(keys, priority).await
    }

    pub(super) async fn get_multi_qualified_with_priority(
        &self,
        keys: Vec<String>,
        priority: OpPriority,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        let operation = match self.admit_batch_operation(keys.iter().map(String::as_str)) {
            Ok(operation) => operation,
            Err(_) => {
                return keys
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self.embedded_operations.admit_owned(
            embedded_operations::batch_get_request_bytes(&keys),
            Some(operation.clone()),
        ) {
            Ok(retention) => retention,
            Err(error) => return embedded_operations::reject_batch(keys, error),
        };
        self.get_multi_with_priority_admitted(keys, priority, retention)
            .await
    }

    async fn get_multi_with_priority_admitted(
        &self,
        keys: Vec<String>,
        priority: OpPriority,
        retention: Arc<RequestRetention>,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        let mut local: Vec<(usize, String)> = Vec::new();
        let mut remote: HashMap<FragmentId, Vec<(usize, String)>> = HashMap::new();
        let mut results: Vec<Option<Result<Option<VersionedValue>, NodeError>>> =
            (0..keys.len()).map(|_| None).collect();

        for (i, key) in keys.into_iter().enumerate() {
            if self.fragment_actor(&key).is_some() {
                local.push((i, key));
            } else {
                let frag = fragment_of(&key, self.router.fragment_count);
                if self.replica_order(frag).is_empty() {
                    results[i] = Some(Err(NodeError::Actor(format!(
                        "no replica for fragment {frag}"
                    ))));
                } else {
                    remote.entry(frag).or_default().push((i, key));
                }
            }
        }

        // As with local puts, bound the pre-admission dispatcher backlog.
        let mut local = local.into_iter();
        loop {
            let chunk: Vec<_> = local.by_ref().take(MAX_LOCAL_BATCH_IN_FLIGHT).collect();
            if chunk.is_empty() {
                break;
            }
            let local_futs = chunk.into_iter().map(|(i, key)| {
                let this = self;
                let retention = retention.clone();
                async move {
                    let r = this
                        .get_with_priority_admitted(key, priority, retention)
                        .await;
                    (i, r)
                }
            });
            for (i, r) in futures::future::join_all(local_futs).await {
                results[i] = Some(r);
            }
        }

        // Remote gets: byte- and count-bounded requests. A response is an
        // ordered prefix that stops after crossing the soft byte target;
        // omitted suffixes are pulled under fresh request ids instead of
        // becoming false misses.
        let wire_priority = priority_to_proto(priority);
        let singleton_response_base = batch_get_response_base_encoded_len(u64::MAX);
        for (frag, items) in remote {
            let mut remaining: VecDeque<_> = items.into_iter().collect();
            while !remaining.is_empty() {
                let mut chunk = Vec::new();
                let mut request_payload_len = batch_get_base_encoded_len(wire_priority);
                while chunk.len() < MAX_BATCH_GET_REQUEST_KEYS {
                    let Some((index, key)) = remaining.pop_front() else {
                        break;
                    };
                    let next_request_payload =
                        request_payload_len + message_field_len(2, key.len());
                    let request_len = batch_get_envelope_encoded_len(next_request_payload);
                    // A missing read may be shed instead. Reserve the explicit
                    // `shed=true` bit so either non-value outcome fits alone.
                    let missing_result_len = max_batch_get_non_value_result_encoded_len(key.len());
                    let response_payload_len =
                        singleton_response_base + message_field_len(2, missing_result_len);
                    let response_len =
                        batch_get_response_envelope_encoded_len(response_payload_len);
                    if !chunk.is_empty() && request_len > WIRE_BATCH_PAGE_BYTES {
                        remaining.push_front((index, key));
                        break;
                    }
                    if request_len > MAX_FRAME_BYTES || response_len > MAX_FRAME_BYTES {
                        results[index] = Some(Err(NodeError::Codec(format!(
                            "batch get key cannot fit its request/response frames: request={request_len} response={response_len} limit={MAX_FRAME_BYTES}"
                        ))));
                        continue;
                    }
                    request_payload_len = next_request_payload;
                    chunk.push((index, key));
                }
                if chunk.is_empty() {
                    continue;
                }

                let mut offset = 0usize;
                let mut tried = Vec::new();
                for owner in self.replica_order(frag) {
                    if offset == chunk.len() {
                        break;
                    }
                    tried.push(owner.clone());
                    let Some(tx) = self.peer_sender(&owner).await else {
                        continue;
                    };
                    // One absolute deadline covers every prefix page attempted
                    // at this owner. A peer cannot stretch a bounded owner
                    // attempt indefinitely by returning one key at a time.
                    let attempt_deadline =
                        deadline_after(self.runtime_options.forwarded_attempt_timeout);
                    while offset < chunk.len() && Instant::now() < attempt_deadline {
                        let keys: Vec<String> =
                            chunk[offset..].iter().map(|(_, key)| key.clone()).collect();
                        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
                        let (resp_tx, resp_rx) = oneshot::channel();
                        self.pending_batch_gets.lock().unwrap().insert(
                            req_id,
                            PendingBatchGet {
                                expected_peer: owner.clone(),
                                expected_keys: keys.clone(),
                                reply: resp_tx,
                            },
                        );
                        let _pending_guard = PendingBatchGetGuard {
                            req_id,
                            pending: self.pending_batch_gets.clone(),
                        };
                        if !try_send_envelope_until(
                            &tx,
                            Envelope {
                                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                                    req_id,
                                    keys: keys.clone(),
                                    priority: wire_priority,
                                })),
                            },
                            attempt_deadline,
                        )
                        .await
                        {
                            break;
                        }
                        let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
                        let Ok(Ok(Ok(kvs))) = response else {
                            break;
                        };
                        // Empty is an explicit unavailable/non-serving reply.
                        // It must make no progress and therefore tries the next
                        // holder rather than turning any key into not-found.
                        if kvs.is_empty() {
                            break;
                        }
                        let page_len = kvs.len();
                        for ((index, key), kv) in chunk[offset..offset + page_len].iter().zip(kvs) {
                            let result = if kv.shed {
                                Err(NodeError::Backpressure(format!(
                                    "batch get for key {key} shed by remote"
                                )))
                            } else if kv.found {
                                let mut value = VersionedValue::new_version(
                                    kv.value,
                                    actordb_protocol::RecordVersion::new(
                                        kv.timestamp_ms,
                                        kv.logical_counter,
                                    ),
                                    kv.node_id,
                                );
                                value.expires_at_ms =
                                    (kv.expires_at_ms != 0).then_some(kv.expires_at_ms);
                                value.is_tombstone = kv.is_tombstone;
                                Ok(if value.is_tombstone {
                                    None
                                } else {
                                    Some(value)
                                })
                            } else {
                                Ok(None)
                            };
                            results[*index] = Some(result);
                        }
                        offset += page_len;
                    }
                }
                if offset < chunk.len() {
                    for (index, _) in &chunk[offset..] {
                        results[*index] = Some(Err(NodeError::Network(format!(
                            "no replica answered batch get for fragment {frag} (tried {})",
                            tried.join(",")
                        ))));
                    }
                }
            }
        }

        results.into_iter().map(|r| r.unwrap()).collect()
    }

    /// Get multiple keys. Each key is hashed to its fragment; keys are
    /// grouped by fragment and each fragment's keys are fetched in exact-byte/count-bounded
    /// `BatchGet` pull requests, failing over across replica holders.
    /// Returns per-key results in input order.
    ///
    /// Bulk gets run at [`OpPriority::Background`]; see
    /// [`get_multi_with_priority`](Self::get_multi_with_priority) for an
    /// explicit priority. Remote reads use the configured timeout (5 seconds
    /// by default) per replica attempt; a dead owner fails only its keys, and
    /// only when no replica holder answers.
    pub async fn get_multi(
        &self,
        keys: Vec<String>,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        self.get_multi_with_priority(keys, OpPriority::Background)
            .await
    }

    /// This node's id.
    pub fn id(&self) -> &str {
        &self.node_id
    }

    /// Test hook: the sender's state report for (`fragment`, `peer`),
    /// if the sender exists. Hidden from docs.
    #[doc(hidden)]
    pub async fn sender_report(
        &self,
        fragment: FragmentId,
        peer: &str,
    ) -> Option<SenderStateReport> {
        let key = (fragment, peer.to_string());
        loop {
            let report = self.sender_reports.lock().await.get(&key).cloned()?;
            let snapshot = report.lock().await.clone();
            let reports = self.sender_reports.lock().await;
            match reports.get(&key) {
                Some(current) if Arc::ptr_eq(current, &report) => return Some(snapshot),
                Some(_) => continue,
                None => return None,
            }
        }
    }

    /// The address this node is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Number of currently connected peers.
    pub async fn peer_count(&self) -> usize {
        self.peers.lock().await.len()
    }
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::test_support::*;
    pub(super) use super::test_support::{
        complete_test_handshake, read_test_envelope, test_connection_context, test_peer_channel,
        write_test_envelope,
    };
    use super::*;
    use crate::store::StoreError;
    use ractor::ActorProcessingErr;

    #[tokio::test]
    async fn embedded_get_has_an_explicit_local_actor_wait_deadline() {
        let node = Node::start_with_options(
            "local-deadline".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::builder()
                .local_work_timeout(Duration::from_millis(20))
                .build(),
        )
        .await
        .unwrap();
        let original = node.router.route_for_fragment(FragmentId(0)).unwrap();
        let (stalled, handle) = StalledLocalActor::spawn(None, StalledLocalActor, ())
            .await
            .unwrap();
        node.router.insert(FragmentId(0), stalled.clone());
        let result = tokio::time::timeout(Duration::from_secs(2), node.get("key".into()))
            .await
            .unwrap();
        assert!(matches!(result,Err(NodeError::Actor(message)) if message.contains("timed out")));
        node.router.insert(FragmentId(0), original);
        stalled.stop(None);
        handle.await.unwrap();
        node.shutdown().await.unwrap();
    }

    #[test]
    fn ttl_expiry_deadline_preserves_millisecond_precision_and_exact_boundaries() {
        assert_eq!(ttl_expiry_deadline_ms(1000, Duration::ZERO).unwrap(), 1000);
        assert_eq!(
            ttl_expiry_deadline_ms(1000, Duration::from_nanos(999_999)).unwrap(),
            1000
        );
        assert_eq!(
            ttl_expiry_deadline_ms(1000, Duration::from_nanos(1_999_999)).unwrap(),
            1001
        );
        assert_eq!(
            ttl_expiry_deadline_ms(1000, Duration::from_millis(u64::MAX - 1000)).unwrap(),
            u64::MAX
        );
        assert_eq!(
            ttl_expiry_deadline_ms(u64::MAX, Duration::ZERO).unwrap(),
            u64::MAX
        );
        assert_eq!(
            ttl_expiry_deadline_ms(0, Duration::from_millis(1)).unwrap(),
            1
        );
    }

    #[test]
    fn ttl_expiry_deadline_rejects_truncation_overflow_and_absent_expiry_sentinel() {
        // 2^64 + 5000 milliseconds: the old cast silently shortened this to 5s.
        let wrapping = Duration::new(18_446_744_073_709_556, 616_000_000);
        assert_eq!(wrapping.as_millis(), u64::MAX as u128 + 5001);
        for ttl in [wrapping, Duration::MAX, Duration::from_millis(u64::MAX)] {
            assert!(matches!(
                ttl_expiry_deadline_ms(1000, ttl),
                Err(NodeError::Codec(_))
            ));
        }
        assert!(matches!(
            ttl_expiry_deadline_ms(u64::MAX, Duration::from_millis(1)),
            Err(NodeError::Codec(_))
        ));
        for ttl in [Duration::ZERO, Duration::from_nanos(999_999)] {
            assert!(matches!(
                ttl_expiry_deadline_ms(0, ttl),
                Err(NodeError::Codec(_))
            ));
        }
    }

    #[tokio::test]
    async fn invalid_ttl_rejects_before_sequence_clock_or_store_and_node_stays_healthy() {
        let store = Arc::new(TtlRecordingStore::default());
        let node = Node::start(
            "ttl-invalid".into(),
            "127.0.0.1:0".parse().unwrap(),
            ttl_store_config(&store),
        )
        .await
        .unwrap();
        let sequence = node.next_seq.lock().await;
        let mut clock = node.hlc.lock().await;
        let future = crate::actor::now_ms() + 120_000;
        clock
            .seed_version(actordb_protocol::RecordVersion::new(future, 0))
            .unwrap();
        let next_request = node.next_req_id.load(Ordering::Relaxed);
        for ttl in [
            Duration::MAX,
            Duration::from_millis(u64::MAX),
            Duration::new(18_446_744_073_709_556, 616_000_000),
        ] {
            // Holding both mutation-state locks proves rejection is before
            // their acquisition, rather than merely after a stamp is consumed.
            assert!(matches!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    node.put_with_ttl("invalid".into(), b"value".to_vec(), Some(ttl))
                )
                .await
                .unwrap(),
                Err(NodeError::Codec(_))
            ));
        }
        assert_eq!(*sequence, 0);
        assert!(clock.tick().unwrap() > actordb_protocol::RecordVersion::from(future));
        assert_eq!(node.next_req_id.load(Ordering::Relaxed), next_request);
        drop(clock);
        drop(sequence);
        assert!(node.get_all().await.unwrap().is_empty());
        assert!(store.load().unwrap().is_empty());
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        node.put("healthy".into(), b"ok".to_vec()).await.unwrap();
        assert_eq!(
            node.get("healthy".into()).await.unwrap().unwrap().value,
            b"ok"
        );
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        node.shutdown().await.unwrap();
        assert!(matches!(
            node.put_with_ttl("after-shutdown".into(), Vec::new(), Some(Duration::MAX))
                .await,
            Err(NodeError::ShuttingDown)
        ));
    }

    #[tokio::test]
    async fn local_ttl_uses_wall_time_and_retains_future_hlc_expiry_provenance_on_restart() {
        let store = Arc::new(TtlRecordingStore::default());
        let config = ttl_store_config(&store);
        let node = Node::start(
            "ttl-local".into(),
            "127.0.0.1:0".parse().unwrap(),
            config.clone(),
        )
        .await
        .unwrap();
        let future = crate::actor::now_ms() + 120_000;
        node.hlc
            .lock()
            .await
            .seed_version(actordb_protocol::RecordVersion::new(future, 0))
            .unwrap();
        node.put_with_ttl("permanent".into(), b"value".to_vec(), None)
            .await
            .unwrap();
        let permanent = node.get("permanent".into()).await.unwrap().unwrap();
        assert_eq!(
            permanent.version(),
            actordb_protocol::RecordVersion::new(future, 1)
        );
        assert_eq!(permanent.expires_at_ms, None);

        let before = crate::actor::now_ms();
        node.put_with_ttl(
            "expiring".into(),
            b"value".to_vec(),
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap();
        let after = crate::actor::now_ms();
        let expiring = node.get("expiring".into()).await.unwrap().unwrap();
        assert_eq!(
            expiring.version(),
            actordb_protocol::RecordVersion::new(future, 2)
        );
        assert!((before + 60_000..=after + 60_000).contains(&expiring.expires_at_ms.unwrap()));
        assert!(expiring.expires_at_ms.unwrap() < expiring.timestamp_ms);

        let mut markers = Vec::new();
        for (index, ttl) in [Duration::ZERO, Duration::from_nanos(999_999)]
            .into_iter()
            .enumerate()
        {
            let key = format!("immediate-{index}");
            let before = crate::actor::now_ms();
            assert!(matches!(
                node.put_with_ttl(key.clone(), b"value".to_vec(), Some(ttl))
                    .await,
                Err(NodeError::Expired)
            ));
            let after = crate::actor::now_ms();
            assert!(node.get(key.clone()).await.unwrap().is_none());
            let marker_key = crate::table_key::encode_table_key("KV", &key);
            let marker = node
                .router
                .get_all()
                .await
                .unwrap()
                .remove(&marker_key)
                .unwrap();
            assert!(marker.is_tombstone);
            assert_eq!(
                marker.version(),
                actordb_protocol::RecordVersion::new(future, 3 + index as u64)
            );
            assert_eq!(marker.node_id, "ttl-local");
            assert!((before..=after).contains(&marker.expires_at_ms.unwrap()));
            markers.push((key, marker));
        }
        node.shutdown().await.unwrap();
        let restarted = Node::start("ttl-local".into(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap();
        let values = restarted.router.get_all().await.unwrap();
        assert_eq!(
            values[&crate::table_key::encode_table_key("KV", "permanent")],
            permanent
        );
        assert_eq!(
            values[&crate::table_key::encode_table_key("KV", "expiring")],
            expiring
        );
        for (key, marker) in markers {
            let qualified = crate::table_key::encode_table_key("KV", &key);
            assert_eq!(values[&qualified], marker);
            assert!(restarted.get(key).await.unwrap().is_none());
        }
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn forwarded_ttl_preserves_origin_wall_deadline_and_hlc_stamp() {
        let store = Arc::new(TtlRecordingStore::default());
        let placement = HashMap::from([(FragmentId(0), vec!["ttl-owner".to_string()])]);
        let owner = Node::start(
            "ttl-owner".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_map: placement.clone(),
                ..ttl_store_config(&store)
            },
        )
        .await
        .unwrap();
        let caller = Node::start(
            "ttl-caller".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_map: placement,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        caller.connect(owner.addr()).await.unwrap();
        // connect() registers an owned handler; authentication/peer
        // publication completes asynchronously before forwarding is usable.
        tokio::time::timeout(Duration::from_secs(2), async {
            while caller.peer_sender("ttl-owner").await.is_none()
                || owner.peer_sender("ttl-caller").await.is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let future = crate::actor::now_ms() + 120_000;
        let mut clock = caller.hlc.lock().await;
        clock
            .seed_version(actordb_protocol::RecordVersion::new(future, 0))
            .unwrap();
        let next_request = caller.next_req_id.load(Ordering::Relaxed);
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                caller.put_with_ttl(
                    "invalid-remote".into(),
                    b"value".to_vec(),
                    Some(Duration::MAX)
                )
            )
            .await
            .unwrap(),
            Err(NodeError::Codec(_))
        ));
        assert_eq!(*caller.next_seq.lock().await, 0);
        assert_eq!(caller.next_req_id.load(Ordering::Relaxed), next_request);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        drop(clock);
        assert!(owner.get_all().await.unwrap().is_empty());
        assert!(store.load().unwrap().is_empty());

        let before = crate::actor::now_ms();
        caller
            .put_with_ttl(
                "forwarded".into(),
                b"value".to_vec(),
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        let after = crate::actor::now_ms();
        let value = owner.get("forwarded".into()).await.unwrap().unwrap();
        assert_eq!(
            value.version(),
            actordb_protocol::RecordVersion::new(future, 1)
        );
        assert_eq!(value.node_id, "ttl-caller");
        assert!((before + 60_000..=after + 60_000).contains(&value.expires_at_ms.unwrap()));
        assert!(value.expires_at_ms.unwrap() < value.timestamp_ms);

        let before = crate::actor::now_ms();
        assert!(matches!(
            caller
                .put_with_ttl(
                    "immediate-remote".into(),
                    b"value".to_vec(),
                    Some(Duration::ZERO)
                )
                .await,
            Err(NodeError::Expired)
        ));
        let after = crate::actor::now_ms();
        let marker_key = crate::table_key::encode_table_key("KV", "immediate-remote");
        let marker = owner
            .router
            .get_all()
            .await
            .unwrap()
            .remove(&marker_key)
            .unwrap();
        assert!(marker.is_tombstone);
        assert_eq!(marker.node_id, "ttl-caller");
        assert_eq!(
            marker.version(),
            actordb_protocol::RecordVersion::new(future, 2)
        );
        assert!((before..=after).contains(&marker.expires_at_ms.unwrap()));
        caller.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
        let persisted = store.load().unwrap();
        assert!(persisted.iter().any(|(_, stored)| stored == &value));
        assert!(persisted.iter().any(|(_, stored)| stored == &marker));
    }

    #[tokio::test]
    async fn ttl_deadline_includes_waiting_for_the_origin_clock() {
        let node = Node::start(
            "ttl-wait".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        let clock = node.hlc.lock().await;
        let mut request = Box::pin(node.put_with_ttl(
            "waited".into(),
            b"value".to_vec(),
            Some(Duration::from_millis(5)),
        ));
        assert!(futures::poll!(request.as_mut()).is_pending());
        // The deadline has been captured, and the request is held at the HLC
        // mutex. Waiting here must consume TTL rather than restart it later.
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(clock);
        assert!(matches!(request.await, Err(NodeError::Expired)));
        assert!(node.get("waited".into()).await.unwrap().is_none());
        let marker_key = crate::table_key::encode_table_key("KV", "waited");
        let marker = node
            .router
            .get_all()
            .await
            .unwrap()
            .remove(&marker_key)
            .unwrap();
        assert!(marker.is_tombstone);
        assert!(marker.expires_at_ms.unwrap() < marker.timestamp_ms);
        node.shutdown().await.unwrap();
    }

    #[test]
    fn mutation_responses_require_exact_cardinality_order_and_indices() {
        let expected = vec!["a".to_string(), "b".to_string()];
        assert!(validate_mutation_results(&expected, &[result("a", 0), result("b", 1)]).is_ok());
        assert!(validate_mutation_results(&expected, &[result("a", 0)]).is_err());
        assert!(validate_mutation_results(&expected, &[result("b", 0), result("a", 1)]).is_err());
        assert!(validate_mutation_results(&expected, &[result("a", 1), result("b", 0)]).is_err());
    }

    #[test]
    fn batch_get_prefix_validation_is_positional_and_preserves_duplicates() {
        fn missing(key: &str) -> BatchKeyValue {
            BatchKeyValue {
                key: key.to_string(),
                found: false,
                value: Vec::new(),
                timestamp_ms: 0,
                logical_counter: 0,
                node_id: String::new(),
                expires_at_ms: 0,
                is_tombstone: false,
                shed: false,
            }
        }

        let expected = vec!["dup".to_string(), "missing".to_string(), "dup".to_string()];
        assert!(validate_batch_get_results(&expected, &[]).is_ok());
        assert!(
            validate_batch_get_results(&expected, &[missing("dup"), missing("missing")]).is_ok()
        );
        assert!(
            validate_batch_get_results(&expected, &[missing("missing"), missing("dup")]).is_err()
        );
        assert!(validate_batch_get_results(
            &expected,
            &[
                missing("dup"),
                missing("missing"),
                missing("dup"),
                missing("extra"),
            ]
        )
        .is_err());
    }

    #[test]
    fn aggregate_batch_length_accounting_matches_prost() {
        let puts = vec![
            ForwardedPut {
                key: String::new(),
                value: Vec::new(),
                timestamp_ms: 1,
                logical_counter: 0,
                node_id: "node".to_string(),
                seq: 1,
                boot_id: 1,
                expires_at_ms: 0,
                is_tombstone: false,
                priority: ProtoPriority::Default as i32,
                hops: 0,
                req_id: 0,

                write_concern: None,
            },
            ForwardedPut {
                key: "k".repeat(127),
                value: vec![7; 128],
                timestamp_ms: u64::MAX,
                logical_counter: 0,
                node_id: "node".to_string(),
                seq: u64::MAX,
                boot_id: u64::MAX,
                expires_at_ms: u64::MAX,
                is_tombstone: true,
                priority: ProtoPriority::Default as i32,
                hops: 0,
                req_id: 0,

                write_concern: None,
            },
        ];
        let priority = ProtoPriority::Default as i32;
        let put_payload = batch_put_base_encoded_len(priority)
            + puts
                .iter()
                .map(|put| message_field_len(1, put.encoded_len()))
                .sum::<usize>();
        let put_envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                puts,
                priority,
                req_id: u64::MAX,
            })),
        };
        assert_eq!(
            batch_put_envelope_encoded_len(put_payload),
            put_envelope.encoded_len()
        );

        let keys = vec![String::new(), "k".repeat(127), "k".repeat(128)];
        let get_payload = batch_get_base_encoded_len(priority)
            + keys
                .iter()
                .map(|key| message_field_len(2, key.len()))
                .sum::<usize>();
        let get_envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: u64::MAX,
                keys,
                priority,
            })),
        };
        assert_eq!(
            batch_get_envelope_encoded_len(get_payload),
            get_envelope.encoded_len()
        );

        let shed = BatchKeyValue {
            key: "shed".repeat(32),
            found: false,
            value: Vec::new(),
            timestamp_ms: 0,
            logical_counter: 0,
            node_id: String::new(),
            expires_at_ms: 0,
            is_tombstone: false,
            shed: true,
        };
        assert_eq!(
            max_batch_get_non_value_result_encoded_len(shed.key.len()),
            shed.encoded_len()
        );
        let shed_payload = batch_get_response_base_encoded_len(u64::MAX)
            + message_field_len(2, shed.encoded_len());
        let shed_envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: u64::MAX,
                results: vec![shed],
            })),
        };
        assert_eq!(
            batch_get_response_envelope_encoded_len(shed_payload),
            shed_envelope.encoded_len()
        );

        let key = "result".to_string();
        let worst = MutationResult {
            key: key.clone(),
            status: MutationStatus::Unavailable as i32,
            error: "e".repeat(MAX_WIRE_ERROR_BYTES),
            winner_timestamp_ms: u64::MAX,
            winner_logical_counter: 0,
            winner_node_id: "n".repeat(MAX_WIRE_NODE_ID_BYTES),
            winner_is_tombstone: true,
            request_index: u32::MAX,

            write_concern: None,
        };
        assert_eq!(
            max_mutation_result_encoded_len(key.len()),
            worst.encoded_len()
        );
        let response_payload =
            mutation_response_base_encoded_len() + message_field_len(2, worst.encoded_len());
        let response = Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: u64::MAX,
                results: vec![worst],
            })),
        };
        assert_eq!(
            mutation_response_envelope_encoded_len(response_payload),
            response.encoded_len()
        );
    }

    #[test]
    fn batch_get_single_key_preflight_reserves_the_shed_bit_at_hard_boundary() {
        let priority = ProtoPriority::Default as i32;
        let fits = |key_len: usize| {
            let request_payload =
                batch_get_base_encoded_len(priority) + message_field_len(2, key_len);
            let response_payload = batch_get_response_base_encoded_len(u64::MAX)
                + message_field_len(2, max_batch_get_non_value_result_encoded_len(key_len));
            batch_get_envelope_encoded_len(request_payload) <= MAX_FRAME_BYTES
                && batch_get_response_envelope_encoded_len(response_payload) <= MAX_FRAME_BYTES
        };
        let mut low = 0usize;
        let mut high = MAX_FRAME_BYTES;
        while low < high {
            let midpoint = low + (high - low).div_ceil(2);
            if fits(midpoint) {
                low = midpoint;
            } else {
                high = midpoint - 1;
            }
        }
        assert!(fits(low));
        assert!(!fits(low + 1));
    }

    #[test]
    fn wire_errors_are_utf8_safely_bounded() {
        let truncated = truncate_wire_error("🦀".repeat(MAX_WIRE_ERROR_BYTES));
        assert!(truncated.len() <= MAX_WIRE_ERROR_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert_eq!(truncate_wire_error("short".to_string()), "short");
    }

    #[test]
    fn runtime_regions_validate_utf8_byte_lengths() {
        for (node_id, region) in [
            (String::new(), "west".to_string()),
            ("n".repeat(256), "west".to_string()),
            ("node".to_string(), String::new()),
            ("node".to_string(), "r".repeat(256)),
            ("node".to_string(), "🦀".repeat(64)),
        ] {
            let options =
                NodeRuntimeOptions::default().with_regions(HashMap::from([(node_id, region)]));
            assert!(options.validate().is_err());
        }
        let options = NodeRuntimeOptions::builder()
            .regions(HashMap::from([("n".repeat(255), "r".repeat(255))]))
            .build();
        assert!(options.validate().is_ok());
        assert!(NodeRuntimeOptions::default().validate().is_ok());
    }

    #[tokio::test]
    async fn invalid_region_config_fails_before_bind() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let options = NodeRuntimeOptions::default()
            .with_regions(HashMap::from([("node".to_string(), String::new())]));
        let result = Node::start_with_options(
            "node".to_string(),
            listener.local_addr().unwrap(),
            NodeConfig::default(),
            options,
        )
        .await;
        assert!(matches!(result, Err(NodeError::Actor(error)) if error.contains("region label")));
    }

    #[test]
    fn runtime_options_reject_zero_write_concern_acknowledgement_cap() {
        assert!(NodeRuntimeOptions::default()
            .with_write_concern_acknowledgement_cap(0)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_write_concern_acknowledgement_cap(0)
            .without_write_concern_acknowledgement_cap()
            .validate()
            .is_ok());
    }

    #[test]
    fn runtime_options_reject_unrepresentable_limits() {
        for capacity in [0, Semaphore::MAX_PERMITS.saturating_add(1)] {
            assert!(NodeRuntimeOptions::default()
                .with_relay_capacity(capacity)
                .validate()
                .is_err());
            assert!(NodeRuntimeOptions::default()
                .with_relay_key_byte_capacity(capacity)
                .validate()
                .is_err());
            assert!(NodeRuntimeOptions::default()
                .with_forwarded_mutation_capacity(capacity)
                .validate()
                .is_err());
            assert!(NodeRuntimeOptions::default()
                .with_forwarded_mutation_byte_capacity(capacity)
                .validate()
                .is_err());
        }
        assert!(NodeRuntimeOptions::default()
            .with_relay_capacity(1)
            .with_relay_key_byte_capacity(1)
            .validate()
            .is_ok());
        for options in [
            NodeRuntimeOptions::default().with_serving_read_queue_capacity(0),
            NodeRuntimeOptions::default().with_serving_read_queue_capacity(usize::MAX),
            NodeRuntimeOptions::default().with_serving_read_key_byte_capacity(MAX_FRAME_BYTES - 1),
            NodeRuntimeOptions::default().with_serving_read_key_byte_capacity(usize::MAX),
        ] {
            assert!(options.validate().is_err());
        }
        assert!(NodeRuntimeOptions::default()
            .with_forwarded_mutation_capacity(1)
            .with_forwarded_mutation_byte_capacity(1)
            .validate()
            .is_ok());
        assert!(NodeRuntimeOptions::default()
            .with_socket_write_timeout(Duration::MAX)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_peer_queue_frame_capacity(tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1))
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_peer_queue_frame_capacity(1)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_peer_queue_byte_capacity(MIN_PEER_CHANNEL_BYTE_CAPACITY - 1)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_peer_queue_byte_capacity(MIN_PEER_CHANNEL_BYTE_CAPACITY)
            .validate()
            .is_ok());
        assert!(NodeRuntimeOptions::default()
            .with_replication_control_message_capacity(0)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_inbound_connection_capacity(0)
            .validate()
            .is_err());
        for capacity in [0, MAX_FRAME_BYTES - 1, Semaphore::MAX_PERMITS + 1] {
            assert!(NodeRuntimeOptions::default()
                .with_receive_byte_capacity(capacity)
                .validate()
                .is_err());
        }
        assert!(NodeRuntimeOptions::default()
            .with_receive_byte_capacity(MAX_FRAME_BYTES)
            .validate()
            .is_ok());
        assert!(NodeRuntimeOptions::default()
            .with_replication_control_message_capacity(
                tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1),
            )
            .validate()
            .is_err());
        assert!(
            NodeRuntimeOptions::default()
                .with_inbound_connection_capacity(
                    tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1),
                )
                .validate()
                .is_err()
        );
        for options in [
            NodeRuntimeOptions::default().with_connect_timeout(Duration::ZERO),
            NodeRuntimeOptions::default().with_handshake_timeout(Duration::ZERO),
            NodeRuntimeOptions::default().with_socket_read_timeout(Duration::ZERO),
            NodeRuntimeOptions::default().with_reconcile_data_admission_timeout(Duration::ZERO),
        ] {
            assert!(options.validate().is_err());
        }
        for timeout in [Duration::ZERO, Duration::MAX] {
            assert!(NodeRuntimeOptions::default()
                .with_health_timeout(timeout)
                .validate()
                .is_err());
        }
    }

    #[test]
    fn peer_hello_requires_exact_current_protocol() {
        let mut hello = fresh_hello("peer").unwrap();
        assert_eq!(validate_peer_hello("local", Some("peer"), &hello), Ok(()));
        for version in (0..WIRE_PROTOCOL_VERSION).chain([WIRE_PROTOCOL_VERSION + 1]) {
            hello.protocol_version = version;
            assert_eq!(
                validate_peer_hello("local", Some("peer"), &hello),
                Err("peer must use the exact current wire protocol version"),
            );
        }
    }

    #[tokio::test]
    async fn batch_actor_timeout_returns_unavailable_without_stalling_shared_connection() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(2, 0);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, _actor_handle) = ractor::Actor::spawn(
            None,
            BlockingPageActor,
            BlockingPageState {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        router.insert(fragment, actor.clone());
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (mut ctx, runtime_tasks) = test_connection_context(
            router,
            Arc::new(RwLock::new(HashSet::from([fragment]))),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers.clone(),
        )
        .await;
        ctx.runtime_options =
            NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(50));
        let (mut client, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut client, "peer").await;

        let key_for_fragment = |fragment| {
            (0..1000)
                .map(|i| format!("batch-timeout-{i}"))
                .find(|key| fragment_of(key, 2) == fragment)
                .unwrap()
        };
        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 41,
                    keys: vec![key_for_fragment(fragment)],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("batch read did not reach the blocked actor");
        let response =
            tokio::time::timeout(Duration::from_secs(1), read_test_envelope(&mut client))
                .await
                .expect("batch actor deadline did not release the connection reader");
        assert!(matches!(
            response.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 41,
                results,
            })) if results.is_empty()
        ));

        // Keep the original actor blocked. A request for an independently
        // non-serving fragment must still traverse both halves of this same
        // multiplexed connection after the timed-out batch.
        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 42,
                    keys: vec![key_for_fragment(FragmentId(1))],
                    priority: ProtoPriority::High as i32,
                })),
            },
        )
        .await;
        let probe = tokio::time::timeout(Duration::from_secs(1), read_test_envelope(&mut client))
            .await
            .expect("shared connection stalled after the batch actor timeout");
        assert!(matches!(
            probe.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 42,
                results,
            })) if results.is_empty()
        ));
        assert!(peers.lock().await.contains_key("peer"));

        release.notify_one();
        actor.stop_and_wait(None, None).await.unwrap();
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection did not stop after its stream closed")
            .unwrap();
        runtime_tasks.shutdown().await;
    }

    #[tokio::test]
    async fn dynamic_mutations_at_routing_boundaries_fail_closed() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (ctx, runtime_tasks) = test_connection_context(
            router,
            serving.clone(),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers.clone(),
        )
        .await;
        let (mut client, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut client, "peer").await;

        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                    key: "hop-limit".to_string(),
                    value: b"value".to_vec(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: "peer".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: ProtoPriority::Default as i32,
                    hops: MAX_FORWARD_HOPS,
                    req_id: 11,

                    write_concern: None,
                })),
            },
        )
        .await;
        let hop_envelope = read_test_envelope(&mut client).await;
        assert!(matches!(
            hop_envelope.msg,
            Some(EnvelopeMsg::MutationResponse(ref response))
                if response.req_id == 11
                    && response.results.len() == 1
                    && response.results[0].status == MutationStatus::Unavailable as i32
        ));

        // This state is an invariant violation, but the defensive path must
        // fail the whole batch instead of applying without an owned actor.
        serving.write().unwrap().insert(fragment);
        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    puts: vec![ForwardedPut {
                        key: "missing-actor".to_string(),
                        value: b"value".to_vec(),
                        timestamp_ms: 2,
                        logical_counter: 0,
                        node_id: "peer".to_string(),
                        seq: 2,
                        boot_id: 1,
                        expires_at_ms: 0,
                        is_tombstone: false,
                        priority: ProtoPriority::Default as i32,
                        hops: 0,
                        req_id: 12,

                        write_concern: None,
                    }],
                    priority: ProtoPriority::Default as i32,
                    req_id: 12,
                })),
            },
        )
        .await;
        let batch_envelope = read_test_envelope(&mut client).await;
        assert!(matches!(
            batch_envelope.msg,
            Some(EnvelopeMsg::MutationResponse(ref response))
                if response.req_id == 12
                    && response.results.len() == 1
                    && response.results[0].status == MutationStatus::Unavailable as i32
        ));

        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                    key: "missing-single-actor".to_string(),
                    value: b"value".to_vec(),
                    timestamp_ms: 3,
                    logical_counter: 0,
                    node_id: "peer".to_string(),
                    seq: 3,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: ProtoPriority::Default as i32,
                    hops: 0,
                    req_id: 13,

                    write_concern: None,
                })),
            },
        )
        .await;
        let single_envelope = read_test_envelope(&mut client).await;
        assert!(matches!(
            single_envelope.msg,
            Some(EnvelopeMsg::MutationResponse(ref response))
                if response.req_id == 13
                    && response.results.len() == 1
                    && response.results[0].status == MutationStatus::Unavailable as i32
        ));

        // Legacy ForwardedGet cannot express unavailable. A missing actor
        // must therefore produce no false not-found response. The following
        // batch request is a connection-order barrier.
        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: "missing-read-actor".to_string(),
                    req_id: 14,
                    priority: ProtoPriority::Default as i32,
                    hops: 0,
                })),
            },
        )
        .await;
        write_test_envelope(
            &mut client,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 15,
                    keys: vec!["missing-read-actor".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut client).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 15,
                results,
            })) if results.is_empty()
        ));

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection did not close after its stream was dropped")
            .unwrap();
        assert!(peers.lock().await.is_empty());
        runtime_tasks.shutdown().await;
    }

    #[test]
    fn unspecified_wire_statuses_are_never_success() {
        let unknown = MutationResult {
            status: MutationStatus::Unknown as i32,
            ..result("key", 0)
        };
        assert!(
            validate_mutation_results(&["key".to_string()], std::slice::from_ref(&unknown))
                .is_err()
        );
        assert!(matches!(
            decode_mutation_result(unknown),
            Err(NodeError::Codec(_))
        ));
        assert_eq!(
            ReplicationAckLevel::try_from(0),
            Ok(ReplicationAckLevel::Unknown)
        );
    }

    #[tokio::test]
    async fn stopped_forwarded_mutation_actor_is_unavailable() {
        let fragment = FragmentId(0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, actor) = spawn_fragment_subtree(
            "stopped-forwarded-actor",
            fragment,
            &NodeConfig::default(),
            &hlc,
        )
        .await
        .unwrap();
        let _ = supervisor.stop_and_wait(None, None).await;
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));

        let result = apply_forwarded_mutation(
            &actor,
            &senders,
            fragment,
            ReplicatedOp {
                key: "key".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 1, "origin".to_string()),
                seq: 1,
                boot_id: 1,
            },
            OpPriority::Default,
            0,
            None,
        )
        .await;
        assert_eq!(
            MutationStatus::try_from(result.status),
            Ok(MutationStatus::Unavailable)
        );
    }

    #[test]
    fn scalar_get_response_shapes_fail_closed() {
        let not_found = GetResponse {
            req_id: 1,
            found: false,
            value: Vec::new(),
            timestamp_ms: 0,
            logical_counter: 0,
            node_id: String::new(),
            expires_at_ms: 0,
            is_tombstone: false,
            shed: false,
        };
        assert!(validate_get_response("key", &not_found).is_ok());

        let found = GetResponse {
            found: true,
            value: b"value".to_vec(),
            timestamp_ms: 7,
            logical_counter: 0,
            node_id: "owner".to_string(),
            ..not_found.clone()
        };
        assert!(validate_get_response("key", &found).is_ok());

        let shed = GetResponse {
            shed: true,
            ..not_found.clone()
        };
        assert!(validate_get_response("key", &shed).is_ok());

        for malformed in [
            GetResponse {
                shed: true,
                found: true,
                ..not_found.clone()
            },
            GetResponse {
                found: true,
                node_id: String::new(),
                ..not_found.clone()
            },
            GetResponse {
                value: b"ghost".to_vec(),
                ..not_found.clone()
            },
            GetResponse {
                is_tombstone: true,
                ..not_found
            },
        ] {
            assert!(validate_get_response("key", &malformed).is_err());
        }
    }

    #[test]
    fn dropping_a_pending_mutation_waiter_removes_its_registry_entry() {
        let pending: PendingMutations = Arc::new(StdMutex::new(HashMap::new()));
        let (reply, _response) = oneshot::channel();
        pending.lock().unwrap().insert(
            7,
            PendingMutation {
                expected_peer: "peer".to_string(),
                expected_keys: vec!["key".to_string()],
                reply,
            },
        );
        {
            let _guard = PendingMutationGuard {
                req_id: 7,
                pending: pending.clone(),
            };
        }
        assert!(pending.lock().unwrap().is_empty());
    }

    #[test]
    fn dropping_pending_read_waiters_removes_their_registry_entries() {
        let pending_gets: PendingGets = Arc::new(StdMutex::new(HashMap::new()));
        let (reply, _response) = oneshot::channel();
        pending_gets.lock().unwrap().insert(
            3,
            PendingGet {
                expected_peer: "peer".to_string(),
                expected_key: "key".to_string(),
                reply,
            },
        );
        {
            let _guard = PendingGetGuard {
                req_id: 3,
                pending: pending_gets.clone(),
            };
        }
        assert!(pending_gets.lock().unwrap().is_empty());

        let pending_batch_gets: PendingBatchGets = Arc::new(StdMutex::new(HashMap::new()));
        let (batch_reply, _batch_response) = oneshot::channel();
        pending_batch_gets.lock().unwrap().insert(
            4,
            PendingBatchGet {
                expected_peer: "peer".to_string(),
                expected_keys: vec!["key".to_string()],
                reply: batch_reply,
            },
        );
        {
            let _guard = PendingBatchGetGuard {
                req_id: 4,
                pending: pending_batch_gets.clone(),
            };
        }
        assert!(pending_batch_gets.lock().unwrap().is_empty());
    }

    struct StalledLocalActor;

    impl Actor for StalledLocalActor {
        type Msg = KvMessage;
        type State = Vec<KvMessage>;
        type Arguments = ();
        async fn pre_start(
            &self,
            _: ActorRef<KvMessage>,
            _: (),
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(Vec::new())
        }
        async fn handle(
            &self,
            _: ActorRef<KvMessage>,
            message: KvMessage,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            state.push(message);
            Ok(())
        }
    }

    #[derive(Default)]
    struct TtlRecordingStore {
        values: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
        writes: AtomicUsize,
    }

    impl KvStore for TtlRecordingStore {
        fn bind(
            &self,
            _identity: &crate::store::StoreIdentity,
        ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
            Ok(crate::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            crate::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<crate::store::StoreCommitDurability, crate::store::StoreError> {
            Ok(self.commit_durability())
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.values.lock().unwrap().extend(writes.iter().cloned());
            Ok(())
        }
    }

    fn ttl_store_config(store: &Arc<TtlRecordingStore>) -> NodeConfig {
        NodeConfig {
            store: Some(store.clone()),
            flush_batch_size: 1,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            ..Default::default()
        }
    }
}
