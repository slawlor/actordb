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
//! flush. Senders retire only ACKs that meet their requested level.
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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
use ractor::{Actor, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::{AbortHandle, JoinSet};

use crate::actor::{
    ApplyDurability, ApplyError, ApplyOutcome, HlcClock, KvMessage, OpPriority, PoolConfig,
    ReplicatedOp, VersionedValue,
};
use crate::network::{BoxStream, Network, TcpNetwork};
use crate::placement::{
    validate_placement, FragmentPlacementState, PlacementController, PlacementReport,
};
use crate::proto::{
    BatchAck, BatchGet, BatchGetResponse, BatchKeyValue, BatchNack, BatchPut, Envelope,
    EnvelopeMsg, ForwardedGet, ForwardedPut, GetResponse, Hello, KeySummary, MutationResponse,
    MutationResult, MutationStatus, Priority as ProtoPriority, ReconcileData, ReconcileResponse,
    ReplicationAckLevel, SyncEntry, SyncResponse,
};
use crate::replication::{
    spawn_sender, ReplicationAckDurability, SenderConfig, SenderDeps, SenderMsg, SenderStateReport,
};
use crate::shard::{fragment_of, FragmentId};
use crate::store::{KvStore, PrefixedStore};
use crate::supervision::{FragmentHealth, FragmentSupervisor, SupervisorArgs, SupervisorMessage};

/// Maximum wire frame size (16 MiB). Anything larger is treated as a
/// corrupt stream and the connection is dropped.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Outbound queue depth per peer connection.
const PEER_CHANNEL_DEPTH: usize = 256;

/// Reconnect backoff: first retry after this, doubling to the cap.
const RECONNECT_BASE: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const REMOTE_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// peer node_id -> outbound frame queue
type PeerMap = Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>;
/// Phase 5 senders, keyed by (fragment, peer node id).
type SenderMap = Arc<Mutex<HashMap<(FragmentId, String), mpsc::UnboundedSender<SenderMsg>>>>;
/// Sender state reports, keyed by (fragment, peer node id).
type SenderReportMap = Arc<Mutex<HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>>>;
/// Senders currently in BufferReset, keyed by (fragment, peer).
type ResetSenderSet = Arc<Mutex<HashMap<(FragmentId, String), u64>>>;
/// Relayed forwarded-gets: relay_req_id -> (origin peer_tx, origin req_id).
type RelayMap = Arc<Mutex<HashMap<u64, (mpsc::Sender<Vec<u8>>, u64)>>>;
/// Relayed forwarded mutations: relay req_id -> (origin peer_tx, origin req_id).
type MutationRelayMap = Arc<Mutex<HashMap<u64, MutationRelay>>>;

struct SenderEntryDeps<'a> {
    config: &'a SenderConfig,
    router: &'a FragmentRouter,
    hlc: &'a Arc<Mutex<HlcClock>>,
    reset_senders: &'a ResetSenderSet,
    senders: &'a SenderMap,
    reports: &'a SenderReportMap,
}

/// Atomically install the one replication sender allowed to own a
/// `(fragment, peer)` session. Placement and connection setup can race; a
/// check-then-spawn sequence would orphan one sender after it had already
/// emitted SessionStart or accepted mutations.
async fn ensure_sender_entry(fragment: FragmentId, peer: &str, deps: SenderEntryDeps<'_>) {
    let key = (fragment, peer.to_string());
    let mut senders_guard = deps.senders.lock().await;
    if senders_guard.contains_key(&key) {
        return;
    }
    let report = Arc::new(Mutex::new(SenderStateReport {
        state: "disconnected".to_string(),
        session_id: 0,
        buffered: 0,
        in_flight: 0,
        last_ack_durability: None,
        ack_durability_mismatch: false,
        reconcile_rounds: 0,
    }));
    let sender = spawn_sender(
        fragment,
        peer.to_string(),
        deps.config.clone(),
        SenderDeps {
            router: deps.router.clone(),
            hlc: deps.hlc.clone(),
            reset_senders: deps.reset_senders.clone(),
            state_report: report.clone(),
        },
    );
    deps.reports.lock().await.insert(key.clone(), report);
    senders_guard.insert(key, sender);
}

struct MutationRelay {
    expected_peer: String,
    expected_keys: Vec<String>,
    origin_tx: mpsc::Sender<Vec<u8>>,
    origin_req_id: u64,
    timeout_cancel: oneshot::Sender<()>,
}

/// Pending single-key forwarded reads: req_id -> reply channel. The
/// reply is `Ok(value)` or `Err(shed_reason)` if the remote shed the
/// read due to backpressure.
type PendingGets =
    Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Option<VersionedValue>, String>>>>>;
/// Pending batched forwarded reads: req_id -> reply channel.
type PendingBatchGets = Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<BatchKeyValue>>>>>;
/// Pending single or batched forwarded mutations. Results preserve request
/// order and cardinality.
type PendingMutations = Arc<StdMutex<HashMap<u64, PendingMutation>>>;

struct PendingMutation {
    expected_peer: String,
    expected_keys: Vec<String>,
    reply: oneshot::Sender<Vec<MutationResult>>,
}

/// Removes a pending request even if its future is cancelled while waiting
/// for outbound queue capacity or a peer response.
struct PendingMutationGuard {
    req_id: u64,
    pending: PendingMutations,
}

impl Drop for PendingMutationGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.req_id);
    }
}

/// Unique worker-name prefixes across supervisors in this process.
static NEXT_WORKER_PREFIX: AtomicU64 = AtomicU64::new(0);

/// Everything a connection task needs. Cloned per connection.
#[derive(Clone)]
struct ConnContext {
    node_id: String,
    router: FragmentRouter,
    /// Fragments this node authoritatively serves (Phase 8: a
    /// bootstrapping fragment has an actor in the router but is not
    /// yet serving).
    serving: Arc<RwLock<HashSet<FragmentId>>>,
    /// Authoritative placement map (Phase 8: updated by the placement
    /// driver; static otherwise). Used to re-forward ops for fragments
    /// this node no longer serves.
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    /// When true, the handshake skips the full-state SyncRequest: the
    /// Phase 5 reconciler owns anti-entropy.
    dynamic_placement: bool,
    pending_gets: PendingGets,
    pending_batch_gets: PendingBatchGets,
    pending_mutations: PendingMutations,
    /// Relayed forwarded-gets: relay_req_id -> (origin peer_tx, origin req_id).
    pending_relays: RelayMap,
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
    peers: PeerMap,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
    hlc: Arc<Mutex<HlcClock>>,
    senders: SenderMap,
    sender_config: SenderConfig,
    reset_senders: ResetSenderSet,
    sender_reports: SenderReportMap,
    /// Full-state sync entries applied (observability for the Phase 8
    /// rolling-restart test: must stay 0 when dynamic placement owns
    /// anti-entropy).
    sync_entries_applied: Arc<AtomicU64>,
    /// Notify the placement driver on peer join/leave. `None` in
    /// static mode.
    membership_tx: Option<mpsc::UnboundedSender<()>>,
    /// Pre-shared cluster secret for handshake authentication.
    /// `None` = unauthenticated mesh.
    cluster_secret: Option<Vec<u8>>,
}

/// Max re-forward hops for a routed op. Bounds the chain when peers'
/// placement maps transiently disagree during migration.
const MAX_FORWARD_HOPS: u32 = 4;

/// Routes keys to the fragment actors this node runs. Cloned into
/// every connection task so inbound ops land on the right fragment.
/// The actor map is shared and mutable: the Phase 8 placement driver
/// adds/removes fragment subtrees at runtime.
#[derive(Clone)]
pub struct FragmentRouter {
    actors: Arc<RwLock<HashMap<FragmentId, ActorRef<KvMessage>>>>,
    fragment_count: usize,
}

impl FragmentRouter {
    fn route(&self, key: &str) -> Option<ActorRef<KvMessage>> {
        let frag = fragment_of(key, self.fragment_count);
        self.route_for_fragment(frag)
    }

    /// The actor running `fragment`, if this node runs it (includes
    /// fragments still bootstrapping under Phase 8).
    pub fn route_for_fragment(&self, fragment: FragmentId) -> Option<ActorRef<KvMessage>> {
        self.actors.read().unwrap().get(&fragment).cloned()
    }

    pub(crate) fn key_belongs_to(&self, fragment: FragmentId, key: &str) -> bool {
        fragment_of(key, self.fragment_count) == fragment
    }

    fn fragment_ids(&self) -> Vec<u32> {
        self.actors.read().unwrap().keys().map(|f| f.0).collect()
    }

    /// Add (or replace) the actor for `fragment`.
    fn insert(&self, fragment: FragmentId, actor: ActorRef<KvMessage>) {
        self.actors.write().unwrap().insert(fragment, actor);
    }

    /// Remove the actor for `fragment`.
    fn remove(&self, fragment: FragmentId) {
        self.actors.write().unwrap().remove(&fragment);
    }

    async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        // Clone the refs first; don't hold the lock across awaits.
        let actors: Vec<ActorRef<KvMessage>> =
            self.actors.read().unwrap().values().cloned().collect();
        let mut all = HashMap::new();
        for actor in actors {
            let res = actor
                .call(|reply| KvMessage::GetAll { reply }, None)
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?;
            match res {
                CallResult::Success(map) => all.extend(map),
                CallResult::Timeout => {
                    return Err(NodeError::Actor("get_all timed out".to_string()))
                }
                CallResult::SenderError => {
                    return Err(NodeError::Actor(
                        "get_all reply channel dropped".to_string(),
                    ))
                }
            }
        }
        Ok(all)
    }
}

/// Errors produced by [Node] operations.
#[derive(Debug)]
pub enum NodeError {
    Io(std::io::Error),
    Codec(String),
    Actor(String),
    Network(String),
    /// The operation was shed due to backpressure (queue cap or rate
    /// limit). The caller should retry with backoff or reduce load.
    /// Distinct from "not found" — a shed Get never returns Ok(None).
    Backpressure(String),
    /// A store failed while applying a mutation.
    Storage(String),
    /// A different version already wins the LWW merge. The attempted write
    /// was not applied and is never reported as successful.
    Superseded {
        timestamp_ms: u64,
        node_id: String,
        is_tombstone: bool,
    },
    /// A mutation reached the fragment after its TTL deadline. Its expiry
    /// tombstone was installed so older values cannot reappear.
    Expired,
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Io(e) => write!(f, "io error: {e}"),
            NodeError::Codec(e) => write!(f, "codec error: {e}"),
            NodeError::Actor(e) => write!(f, "actor error: {e}"),
            NodeError::Network(e) => write!(f, "network error: {e}"),
            NodeError::Backpressure(e) => write!(f, "backpressure shed: {e}"),
            NodeError::Storage(e) => write!(f, "storage error: {e}"),
            NodeError::Superseded {
                timestamp_ms,
                node_id,
                is_tombstone,
            } => write!(
                f,
                "write was superseded by timestamp={timestamp_ms} node_id={node_id} tombstone={is_tombstone}"
            ),
            NodeError::Expired => write!(f, "write expired before it could be applied"),
        }
    }
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
    timestamp_ms: u64,
    message: &str,
) -> bool {
    match hlc.lock().await.observe(timestamp_ms) {
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
    match actor
        .call(
            |reply| KvMessage::ApplyOp {
                op,
                priority,
                is_replication,
                reply: Some(reply),
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
        CallResult::Success(Err(ApplyError::Storage(error))) => Err(NodeError::Storage(error)),
        CallResult::Timeout => Err(NodeError::Actor("apply timed out".to_string())),
        CallResult::SenderError => Err(NodeError::Actor("apply reply channel dropped".to_string())),
    }
}

async fn flush_fence(actor: &ActorRef<KvMessage>) -> Result<ApplyDurability, NodeError> {
    match actor
        .call(|reply| KvMessage::Flush { reply }, None)
        .await
        .map_err(|error| NodeError::Actor(error.to_string()))?
    {
        CallResult::Success(Ok(durability)) => Ok(durability),
        CallResult::Success(Err(ApplyError::Storage(error))) => Err(NodeError::Storage(error)),
        CallResult::Success(Err(ApplyError::Backpressure)) => Err(NodeError::Actor(
            "flush fence unexpectedly hit admission control".to_string(),
        )),
        CallResult::Timeout => Err(NodeError::Actor("flush fence timed out".to_string())),
        CallResult::SenderError => Err(NodeError::Actor(
            "flush fence reply channel dropped".to_string(),
        )),
    }
}

async fn enqueue_to_senders(senders: &SenderMap, frag: FragmentId, op: &ReplicatedOp) {
    let senders = senders.lock().await;
    for ((sender_frag, _peer), sender) in senders.iter() {
        if *sender_frag == frag {
            let _ = sender.send(SenderMsg::Enqueue(op.clone()));
        }
    }
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
        winner_node_id: String::new(),
        winner_is_tombstone: false,
        request_index: request_index as u32,
    };
    match result {
        Ok(ApplyOutcome::Applied) => {}
        Ok(ApplyOutcome::AlreadyPresent) => {
            wire.status = MutationStatus::AlreadyPresent as i32;
        }
        Ok(ApplyOutcome::Superseded {
            timestamp_ms,
            node_id,
            is_tombstone,
        }) => {
            wire.status = MutationStatus::Superseded as i32;
            wire.winner_timestamp_ms = timestamp_ms;
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
            node_id,
            is_tombstone,
        }) => {
            wire.status = MutationStatus::Superseded as i32;
            wire.winner_timestamp_ms = timestamp_ms;
            wire.winner_node_id = node_id;
            wire.winner_is_tombstone = is_tombstone;
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
    wire
}

fn decode_mutation_result(result: MutationResult) -> Result<(), NodeError> {
    match MutationStatus::try_from(result.status) {
        Ok(MutationStatus::Applied | MutationStatus::AlreadyPresent) => Ok(()),
        Ok(MutationStatus::Superseded) => Err(NodeError::Superseded {
            timestamp_ms: result.winner_timestamp_ms,
            node_id: result.winner_node_id,
            is_tombstone: result.winner_is_tombstone,
        }),
        Ok(MutationStatus::Expired) => Err(NodeError::Expired),
        Ok(MutationStatus::Backpressure) => Err(NodeError::Backpressure(result.error)),
        Ok(MutationStatus::StorageError) => Err(NodeError::Storage(result.error)),
        Ok(MutationStatus::ActorError) => Err(NodeError::Actor(result.error)),
        Ok(MutationStatus::Unavailable) => Err(NodeError::Network(result.error)),
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
) -> MutationResult {
    let key = op.key.clone();
    let result = call_apply(actor, op.clone(), priority, false).await;
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

impl std::error::Error for NodeError {}

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
    /// Placement: fragment -> replica node_ids. Static unless a
    /// placement controller is configured, in which case the driver
    /// keeps it converged with the controller.
    fragment_map: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    /// Per-fragment placement lifecycle (Phase 8; all Serving in
    /// static mode).
    placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>>,
    /// Placement controller (Phase 8). `None` = static placement.
    placement_controller: Option<Arc<dyn PlacementController>>,
    /// How often the placement driver re-evaluates (Phase 8).
    placement_poll_interval: Duration,
    /// Static membership (see `NodeConfig::members`).
    members: HashMap<String, SocketAddr>,
    /// In-flight forwarded reads: req_id -> reply channel.
    pending_gets: PendingGets,
    /// In-flight batched forwarded reads: req_id -> reply channel.
    pending_batch_gets: PendingBatchGets,
    /// In-flight single and batched forwarded mutation outcomes.
    pending_mutations: PendingMutations,
    /// Relayed forwarded-gets (Phase 8 re-forwarding).
    pending_relays: RelayMap,
    /// Relayed forwarded-mutation responses (Phase 8 re-forwarding).
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
    peers: PeerMap,
    /// Per-node sequence number for locally originated ops.
    next_seq: Arc<Mutex<u64>>,
    /// Hybrid logical clock for LWW timestamps.
    hlc: Arc<Mutex<HlcClock>>,
    /// Phase 5 senders, keyed by (fragment, peer node id).
    senders: SenderMap,
    /// Senders currently in BufferReset, keyed by (fragment, peer).
    /// The cleaner blocks tombstone GC while this is non-empty for the
    /// fragment.
    reset_senders: ResetSenderSet,
    /// Phase 5 sender tuning.
    sender_config: SenderConfig,
    /// State reports for each sender, keyed by (fragment, peer).
    sender_reports: SenderReportMap,
    /// Full-state sync entries applied via SyncResponse (observability;
    /// stays 0 when dynamic placement owns anti-entropy).
    sync_entries_applied: Arc<AtomicU64>,
    /// Wake the placement driver on membership change (Phase 8).
    membership_tx: Option<mpsc::UnboundedSender<()>>,
    /// Pre-shared cluster secret for handshake authentication.
    /// `None` = unauthenticated mesh.
    cluster_secret: Option<Vec<u8>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
    network: Arc<dyn Network>,
}

/// Mint a fresh boot id. Wall-clock nanos mixed with the pid: two boots
/// can never share a nanosecond on the same machine, so collisions are
/// not a practical concern (and a collision would only risk a dropped
/// duplicate, never corruption — LWW still guards the merge).
fn fresh_boot_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

/// Spawn one fragment's supervised subtree (supervisor + dispatcher)
/// and return both. Shared by startup and the Phase 8 placement
/// driver (which spawns subtrees for newly assigned fragments at
/// runtime).
async fn spawn_fragment_subtree(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    reset_senders: &Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
    hlc: &Arc<Mutex<HlcClock>>,
) -> Result<(ActorRef<SupervisorMessage>, ActorRef<KvMessage>), NodeError> {
    // Unique per supervisor: tests run nodes in parallel in one
    // process, and the actor registry is process-global.
    let worker_prefix = format!(
        "{}-frag{}-{}",
        node_id,
        fragment.0,
        NEXT_WORKER_PREFIX.fetch_add(1, Ordering::SeqCst)
    );
    let store = config
        .store
        .clone()
        .map(|inner| Arc::new(PrefixedStore::new(inner, fragment)) as Arc<dyn KvStore>);
    let (supervisor, _sup_handle) = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs {
            fragment,
            worker_prefix,
            store,
            hlc: hlc.clone(),
            tombstone_gc_age: config.tombstone_gc_age,
            reset_senders: reset_senders.clone(),
            default_pool: config.default_pool.clone(),
            high_pool: config.high_pool.clone(),
            background_pool: config.background_pool.clone(),
            flush_batch_size: config.flush_batch_size,
            flush_interval: config.flush_interval,
            clean_interval: config.clean_interval,
            snapshot_interval: config.snapshot_interval,
        },
    )
    .await
    .map_err(|e| NodeError::Actor(e.to_string()))?;
    let dispatcher = match supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await
        .map_err(|e| NodeError::Actor(e.to_string()))?
    {
        CallResult::Success(d) => d,
        _ => {
            return Err(NodeError::Actor(
                "supervisor dropped dispatcher reply".to_string(),
            ))
        }
    };
    Ok((supervisor, dispatcher))
}

/// Configuration for a [Node]. Start with `NodeConfig::default()` and
/// set the fields you need. This is the seed of the operator-facing
/// configuration surface (see the `NodeConfig` productionization issue).
#[derive(Clone)]
pub struct NodeConfig {
    /// Optional durable storage behind the node's in-memory cache.
    /// `None` (default) = no persistence; the node is purely in-memory.
    /// With the `rocksdb` feature, `NodeConfig::with_rocksdb` builds a
    /// config with the preferred RocksDB implementation, or bring your
    /// own [`KvStore`].
    pub store: Option<Arc<dyn KvStore>>,
    /// Network transport for the peer mesh. Defaults to plain TCP;
    /// see [`Network`].
    pub network: Arc<dyn Network>,
    /// Flush the store write buffer after this many buffered ops.
    /// Ignored when `store` is `None`.
    pub flush_batch_size: usize,
    /// Flush the store write buffer after this long, even if the batch
    /// isn't full (checked on each applied write). Ignored when `store`
    /// is `None`.
    ///
    /// Crash window: a `kill -9` can lose the unflushed tail, which is
    /// then recovered via peer sync on reconnect. Clean shutdowns
    /// always flush.
    pub flush_interval: Duration,
    /// Number of fragments the keyspace is partitioned into. Each
    /// fragment gets its own actor (and, with persistence, its own
    /// key namespace in the store). Default 1 = no sharding.
    ///
    /// Must be identical on every node: all nodes must agree on
    /// `fragment_of(key)`, or keys become unreachable.
    pub fragment_count: usize,
    /// Static placement: fragment -> replica node_ids. Must be identical
    /// on every node. Empty (default) = this node serves all fragments
    /// (single-node / legacy full replication).
    pub fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Static membership: node_id -> address, for dialing fragment
    /// owners. `Node::start` best-effort dials all members.
    pub members: HashMap<String, SocketAddr>,
    /// Worker pool configs by priority. The default pool handles normal
    /// ops; high-priority is for latency-sensitive traffic; background
    /// is for bulk/scan (rate-limited).
    pub default_pool: PoolConfig,
    pub high_pool: PoolConfig,
    pub background_pool: PoolConfig,
    /// How often each fragment's cleaner sweeps for TTL-expired keys.
    /// Default 30 seconds.
    pub clean_interval: Duration,
    /// How often each fragment's snapshotter flushes the write buffer
    /// to the store. Default 60 seconds.
    pub snapshot_interval: Duration,
    /// Optional age after which the cleaner removes tombstones from memory.
    /// `None` (default) retains them indefinitely. Time-based GC is unsafe:
    /// sender buffers, receiver crashes, and restart can lose the convergence
    /// evidence required to prevent resurrection. Replicated deployments
    /// should leave this disabled.
    pub tombstone_gc_age: Option<Duration>,
    /// Wall-clock offset (ms) applied to this node's HLC. Zero in
    /// production; tests use it to simulate clock skew. Hidden from
    /// docs: not a tuning knob.
    #[doc(hidden)]
    pub clock_skew_ms: i64,
    /// Maximum a remote or persisted timestamp may exceed this node's wall
    /// clock before the connection or durable startup is rejected. The raw
    /// out-of-policy value is never stored. Without this bound, one bad
    /// timestamp permanently poisons last-writer-wins cluster-wide. Default
    /// 5 minutes.
    pub max_clock_skew: Duration,
    /// Pre-shared cluster secret for handshake authentication. When
    /// set, every `Hello` carries an HMAC-SHA256(secret, node_id) tag
    /// which the receiver verifies; mismatches drop the connection,
    /// and any non-`Hello` message arriving before a completed
    /// handshake is ignored. `None` (default) = unauthenticated mesh
    /// (anyone with TCP reachability can join); a warning is logged
    /// at startup. Must be identical on every node.
    pub cluster_secret: Option<Vec<u8>>,
    /// Phase 5 async replication tuning (batching, acks, buffers).
    pub replication: SenderConfig,
    /// Phase 8: dynamic placement controller. `None` (default) =
    /// static placement: the node's fragments are fixed at startup
    /// from `fragment_map` and the full-state handshake sync runs as
    /// before. Set to an `Arc<dyn PlacementController>` (e.g.
    /// [`ManualPlacement`](crate::placement::ManualPlacement)) to opt
    /// into dynamic placement: a driver task polls the controller,
    /// spawns/drains fragment subtrees at runtime, bootstraps newly
    /// assigned fragments via the Phase 5 reconciler, and skips the
    /// full-state handshake sync (the reconciler owns anti-entropy).
    pub placement_controller: Option<Arc<dyn PlacementController>>,
    /// How often the Phase 8 placement driver re-evaluates desired
    /// placement. Ignored in static mode. Default 1 second.
    pub placement_poll_interval: Duration,
    /// How often to warn while a revoked fragment cannot safely finish
    /// draining. This is an observability interval, not a destructive
    /// timeout: the old owner retains its recovery state until every sender
    /// has drained. Ignored in static mode. Default 10 seconds.
    pub placement_drain_warn_interval: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            store: None,
            network: Arc::new(TcpNetwork),
            flush_batch_size: 1000,
            flush_interval: Duration::from_millis(100),
            fragment_count: 1,
            fragment_map: HashMap::new(),
            members: HashMap::new(),
            default_pool: PoolConfig {
                workers: 4,
                queue_cap: 10_000,
                rate_limit_per_sec: None,
            },
            high_pool: PoolConfig {
                workers: 2,
                queue_cap: 5_000,
                rate_limit_per_sec: None,
            },
            background_pool: PoolConfig {
                workers: 2,
                queue_cap: 5_000,
                rate_limit_per_sec: Some(1000),
            },
            clean_interval: Duration::from_secs(30),
            snapshot_interval: Duration::from_secs(60),
            tombstone_gc_age: None,
            clock_skew_ms: 0,
            max_clock_skew: Duration::from_secs(300),
            cluster_secret: None,
            replication: SenderConfig::default(),
            placement_controller: None,
            placement_poll_interval: Duration::from_secs(1),
            placement_drain_warn_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(feature = "rocksdb")]
impl NodeConfig {
    /// Convenience: a config persisting to RocksDB at `path`, the
    /// preferred durable implementation.
    pub fn with_rocksdb(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::store::StoreError> {
        Ok(Self {
            store: Some(Arc::new(crate::store::RocksDbStore::open(path)?)),
            ..Self::default()
        })
    }
}

impl Node {
    /// Start a node: spawn its [crate::actor::KvActor] and begin
    /// listening for peer connections. Binds `listen_addr` (use port 0
    /// for an ephemeral port) and returns the bound address via [Node::addr].
    pub async fn start(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
    ) -> Result<Self, NodeError> {
        let dynamic = config.placement_controller.is_some();
        if config.fragment_count == 0 {
            return Err(NodeError::Actor(
                "fragment_count must be non-zero".to_string(),
            ));
        }
        if config.replication.batch_size == 0 {
            return Err(NodeError::Actor(
                "replication.batch_size must be non-zero".to_string(),
            ));
        }
        if config.replication.max_in_flight == 0 {
            return Err(NodeError::Actor(
                "replication.max_in_flight must be non-zero".to_string(),
            ));
        }
        if config.replication.batch_interval.is_zero()
            || config.replication.consistency_interval.is_zero()
            || config.replication.reconcile_timeout.is_zero()
        {
            return Err(NodeError::Actor(
                "replication intervals and timeouts must be non-zero".to_string(),
            ));
        }
        if dynamic
            && (config.placement_poll_interval.is_zero()
                || config.placement_drain_warn_interval.is_zero())
        {
            return Err(NodeError::Actor(
                "dynamic placement intervals must be non-zero".to_string(),
            ));
        }
        if config.cluster_secret.is_none() {
            tracing::warn!(
                "no cluster_secret configured: the peer mesh is unauthenticated; \
                 anyone with TCP reachability can join. Set \
                 NodeConfig.cluster_secret in production."
            );
        }
        if let Some(age) = config.tombstone_gc_age {
            tracing::warn!(
                ?age,
                "unsafe time-based tombstone GC is enabled; sender buffering, \
                 receiver crashes, restart, or partitions can resurrect deleted \
                 or expired data"
            );
        }
        // Initial placement: the controller's map in dynamic mode
        // (validated), the static fragment_map otherwise.
        let initial_placement: HashMap<FragmentId, Vec<String>> = if dynamic {
            let map = config
                .placement_controller
                .as_ref()
                .expect("checked above")
                .desired_placement();
            validate_placement(&map, config.fragment_count).map_err(NodeError::Actor)?;
            map
        } else {
            if !config.fragment_map.is_empty() {
                for i in 0..config.fragment_count {
                    let frag = FragmentId(i as u32);
                    match config.fragment_map.get(&frag) {
                        Some(replicas) if !replicas.is_empty() => {}
                        _ => {
                            return Err(NodeError::Actor(format!(
                                "fragment_map leaves {frag} with no replicas"
                            )))
                        }
                    }
                }
            }
            config.fragment_map.clone()
        };

        // One actor per fragment. With persistence, each fragment gets
        // its own key namespace via PrefixedStore, sharing the one
        // underlying store.
        // Fragments this node serves: from the placement map, or all of
        // them when no map is configured (single-node / legacy).
        let my_fragments: Vec<FragmentId> = if !dynamic && config.fragment_map.is_empty() {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .collect()
        } else {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .filter(|f| {
                    initial_placement
                        .get(f)
                        .map(|r| r.iter().any(|id| id == &node_id))
                        .unwrap_or(false)
                })
                .collect()
        };

        // One supervised subtree per served fragment. With persistence,
        // each fragment gets its own key namespace via PrefixedStore,
        // sharing the one underlying store. The supervisor owns the
        // dispatcher, cleaner and snapshotter; the router talks to the
        // dispatcher through the supervisor.
        let reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let hlc = Arc::new(Mutex::new(
            HlcClock::with_offset(config.clock_skew_ms)
                .with_max_skew(config.max_clock_skew.as_millis().min(u64::MAX as u128) as u64),
        ));
        let supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>> =
            Arc::new(Mutex::new(HashMap::with_capacity(my_fragments.len())));
        let router = FragmentRouter {
            actors: Arc::new(RwLock::new(HashMap::with_capacity(my_fragments.len()))),
            fragment_count: config.fragment_count,
        };
        let serving: Arc<RwLock<HashSet<FragmentId>>> =
            Arc::new(RwLock::new(my_fragments.iter().copied().collect()));
        let placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>> =
            Arc::new(Mutex::new(
                my_fragments
                    .iter()
                    .map(|f| (*f, FragmentPlacementState::Serving))
                    .collect(),
            ));
        for fragment in my_fragments {
            let (supervisor, dispatcher) =
                spawn_fragment_subtree(&node_id, fragment, &config, &reset_senders, &hlc).await?;
            supervisors.lock().await.insert(fragment, supervisor);
            router.insert(fragment, dispatcher);
        }

        let (bound_addr, mut acceptor) = config
            .network
            .bind(listen_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;

        let fragment_map: Arc<RwLock<HashMap<FragmentId, Vec<String>>>> =
            Arc::new(RwLock::new(initial_placement));
        let (membership_tx, membership_rx) = if dynamic {
            let (tx, rx) = mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let node = Self {
            node_id: node_id.clone(),
            boot_id: fresh_boot_id(),
            listen_addr: bound_addr,
            router: router.clone(),
            serving: serving.clone(),
            supervisors: supervisors.clone(),
            fragment_map: fragment_map.clone(),
            placement_states: placement_states.clone(),
            placement_controller: config.placement_controller.clone(),
            placement_poll_interval: config.placement_poll_interval,
            members: config.members.clone(),
            pending_gets: Arc::new(Mutex::new(HashMap::new())),
            pending_batch_gets: Arc::new(Mutex::new(HashMap::new())),
            pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
            pending_relays: Arc::new(Mutex::new(HashMap::new())),
            pending_mutation_relays: Arc::new(Mutex::new(HashMap::new())),
            next_req_id: Arc::new(AtomicU64::new(1)),
            peers: Arc::new(Mutex::new(HashMap::new())),
            next_seq: Arc::new(Mutex::new(0)),
            hlc: hlc.clone(),
            senders: Arc::new(Mutex::new(HashMap::new())),
            reset_senders: reset_senders.clone(),
            sender_config: config.replication.clone(),
            sender_reports: Arc::new(Mutex::new(HashMap::new())),
            sync_entries_applied: Arc::new(AtomicU64::new(0)),
            membership_tx: membership_tx.clone(),
            cluster_secret: config.cluster_secret.clone(),
            tasks: Arc::new(Mutex::new(Vec::new())),
            network: config.network.clone(),
        };

        // Phase 5: spawn a sender per (fragment, replica peer).
        // Legacy mode (no fragment map) spawns senders lazily on connect.
        for frag in router.fragment_ids() {
            let frag_id = FragmentId(frag);
            let replicas: Vec<String> = fragment_map
                .read()
                .unwrap()
                .get(&frag_id)
                .cloned()
                .unwrap_or_default();
            for peer in replicas {
                if peer != node_id {
                    node.ensure_sender(frag_id, &peer).await;
                }
            }
        }

        // Phase 8: the placement driver converges local state toward
        // the controller's desired placement.
        if dynamic {
            let deps = PlacementDriverDeps {
                node_id: node_id.clone(),
                controller: node.placement_controller.clone().expect("checked above"),
                poll_interval: node.placement_poll_interval,
                drain_warn_interval: config.placement_drain_warn_interval,
                fragment_count: config.fragment_count,
                subtree_config: config.clone(),
                router: router.clone(),
                serving: serving.clone(),
                placement: fragment_map.clone(),
                supervisors: supervisors.clone(),
                placement_states: placement_states.clone(),
                senders: node.senders.clone(),
                sender_reports: node.sender_reports.clone(),
                reset_senders: reset_senders.clone(),
                sender_config: node.sender_config.clone(),
                peers: node.peers.clone(),
                hlc: hlc.clone(),
            };
            let membership_rx = membership_rx.expect("checked above");
            let h = tokio::spawn(placement_driver(deps, membership_rx));
            node.tasks.lock().await.push(h.abort_handle());
        }

        // Accept loop: every inbound connection gets a handler task.
        let ctx = ConnContext {
            node_id: node.node_id.clone(),
            router: node.router.clone(),
            serving: node.serving.clone(),
            placement: node.fragment_map.clone(),
            dynamic_placement: dynamic,
            pending_gets: node.pending_gets.clone(),
            pending_batch_gets: node.pending_batch_gets.clone(),
            pending_mutations: node.pending_mutations.clone(),
            pending_relays: node.pending_relays.clone(),
            pending_mutation_relays: node.pending_mutation_relays.clone(),
            next_req_id: node.next_req_id.clone(),
            peers: node.peers.clone(),
            tasks: node.tasks.clone(),
            hlc: node.hlc.clone(),
            senders: node.senders.clone(),
            sender_config: node.sender_config.clone(),
            reset_senders: node.reset_senders.clone(),
            sender_reports: node.sender_reports.clone(),
            sync_entries_applied: node.sync_entries_applied.clone(),
            membership_tx: node.membership_tx.clone(),
            cluster_secret: node.cluster_secret.clone(),
        };
        let accept_task = tokio::spawn(async move {
            while let Ok(stream) = acceptor.accept().await {
                let h = tokio::spawn(handle_connection(
                    ctx.clone(),
                    stream,
                    /* we_dialed = */ false,
                ));
                ctx.tasks.lock().await.push(h.abort_handle());
            }
        });
        node.tasks.lock().await.push(accept_task.abort_handle());

        // Best-effort mesh: dial every known member. Whoever starts
        // later dials us back, so the mesh converges without manual
        // connect() calls.
        for (id, addr) in &node.members {
            if id != &node.node_id {
                let _ = node.connect(*addr).await;
            }
        }

        Ok(node)
    }

    /// Dial a peer and run the handshake. After the handshake both sides
    /// exchange full state, so a late joiner converges immediately.
    ///
    /// The dial is tried once: an unreachable peer returns an error.
    /// Once connected, the dialer side owns the connection — if it
    /// drops, it is redialed with exponential backoff until
    /// [Node::shutdown].
    pub async fn connect(&self, peer_addr: SocketAddr) -> Result<(), NodeError> {
        let stream = self
            .network
            .connect(peer_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;
        let ctx = ConnContext {
            node_id: self.node_id.clone(),
            router: self.router.clone(),
            serving: self.serving.clone(),
            placement: self.fragment_map.clone(),
            dynamic_placement: self.placement_controller.is_some(),
            pending_gets: self.pending_gets.clone(),
            pending_batch_gets: self.pending_batch_gets.clone(),
            pending_mutations: self.pending_mutations.clone(),
            pending_relays: self.pending_relays.clone(),
            pending_mutation_relays: self.pending_mutation_relays.clone(),
            next_req_id: self.next_req_id.clone(),
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            hlc: self.hlc.clone(),
            senders: self.senders.clone(),
            sender_config: self.sender_config.clone(),
            reset_senders: self.reset_senders.clone(),
            sender_reports: self.sender_reports.clone(),
            sync_entries_applied: self.sync_entries_applied.clone(),
            membership_tx: self.membership_tx.clone(),
            cluster_secret: self.cluster_secret.clone(),
        };
        let network = self.network.clone();
        let h = tokio::spawn(async move {
            // Run the initial connection to completion...
            handle_connection(ctx.clone(), stream, /* we_dialed = */ true).await;
            // ...then redial forever. Only the dialer side redials; the
            // listener side just waits for the next inbound connection.
            // This task is aborted by shutdown() with all the rest.
            reconnect_loop(ctx, network, peer_addr).await;
        });
        self.tasks.lock().await.push(h.abort_handle());
        Ok(())
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
                router: &self.router,
                hlc: &self.hlc,
                reset_senders: &self.reset_senders,
                senders: &self.senders,
                reports: &self.sender_reports,
            },
        )
        .await;
    }

    /// Pick a replica holder for `frag`, preferring this node. Returns
    /// `None` if the fragment has no usable replica (misconfiguration).
    fn pick_replica(&self, frag: FragmentId) -> Option<String> {
        self.replica_order(frag).into_iter().next()
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
    async fn peer_sender(&self, node_id: &str) -> Option<mpsc::Sender<Vec<u8>>> {
        self.peers.lock().await.get(node_id).cloned()
    }

    /// Enqueue an op for asynchronous replication to every sender serving
    /// `frag`. The client write path does not await those replica ACKs; each
    /// sender retains its own batch through the configured receiver fence.
    async fn enqueue_for_replication(&self, frag: FragmentId, op: &ReplicatedOp) {
        // Ensure senders exist for all current replication targets.
        // (Static peers got senders at startup; legacy-mode peers get
        // them on connect.)
        enqueue_to_senders(&self.senders, frag, op).await;
    }

    /// Write a key. The op is routed in one hop to a node serving the
    /// key's fragment: applied locally if we serve it, otherwise
    /// forwarded to a replica holder.
    ///
    /// Local and forwarded puts both wait for the serving fragment's exact
    /// apply outcome. An `Ok` means that version was applied or was already
    /// present at that apply point; admission, storage, expiry, and
    /// supersession are typed errors. `Ok` does not force a persistence flush
    /// or await asynchronous fan-out to the other replicas.
    pub async fn put(&self, key: String, value: Vec<u8>) -> Result<(), NodeError> {
        self.put_with_ttl(key, value, None).await
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
        self.put_with_ttl_and_priority(key, value, None, priority)
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
    ) -> Result<(), NodeError> {
        let seq = {
            let mut guard = self.next_seq.lock().await;
            *guard += 1;
            *guard
        };
        let now = self.hlc.lock().await.tick();
        let mut vv = VersionedValue::new(value, now, self.node_id.clone());
        vv.expires_at_ms = ttl.map(|d| now + d.as_millis() as u64);
        let op = ReplicatedOp {
            key,
            value: vv,
            seq,
            boot_id: self.boot_id,
        };
        self.apply_local_with_priority(op, priority).await
    }

    /// Write `value` under `key`, with an optional time-to-live. When
    /// `ttl` is set, the key expires `ttl` after this call (wall-clock):
    /// reads treat it as absent past expiry, and the fragment's cleaner
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
        self.put_with_ttl_and_priority(key, value, ttl, OpPriority::Default)
            .await
    }

    /// Delete `key`: writes a tombstone that propagates to all replicas
    /// like a put. Reads treat tombstoned keys as absent; sync ships
    /// tombstones so deletes survive node restarts and re-joins. A later
    /// `put` with a higher timestamp resurrects the key.
    pub async fn delete(&self, key: String) -> Result<(), NodeError> {
        let seq = {
            let mut guard = self.next_seq.lock().await;
            *guard += 1;
            *guard
        };
        let now = self.hlc.lock().await.tick();
        let op = ReplicatedOp {
            key: key.clone(),
            value: VersionedValue::tombstone(now, self.node_id.clone()),
            seq,
            boot_id: self.boot_id,
        };
        self.apply_local(op).await
    }

    /// Apply a locally-originated op, await its exact merge outcome, then
    /// replicate only an operation that was applied or already present.
    async fn apply_local(&self, op: ReplicatedOp) -> Result<(), NodeError> {
        self.apply_local_with_priority(op, OpPriority::Default)
            .await
    }

    async fn apply_local_with_priority(
        &self,
        op: ReplicatedOp,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        let frag = fragment_of(&op.key, self.router.fragment_count);
        if let Some(actor) = self.fragment_actor(&op.key) {
            let outcome = call_apply(&actor, op.clone(), priority, false).await?;

            match outcome {
                ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent => {}
                ApplyOutcome::Superseded {
                    timestamp_ms,
                    node_id,
                    is_tombstone,
                } => {
                    return Err(NodeError::Superseded {
                        timestamp_ms,
                        node_id,
                        is_tombstone,
                    })
                }
                ApplyOutcome::Expired => {
                    let mut expiry = op.clone();
                    expiry.value = expiry.value.expiry_tombstone();
                    self.enqueue_for_replication(frag, &expiry).await;
                    return Err(NodeError::Expired);
                }
            }

            self.enqueue_for_replication(frag, &op).await;
            return Ok(());
        }

        // Remote fragment: forward to a replica holder in one hop.
        let owner = self
            .pick_replica(frag)
            .ok_or_else(|| NodeError::Actor(format!("no replica for fragment {frag}")))?;
        let tx = self.peer_sender(&owner).await.ok_or_else(|| {
            NodeError::Network(format!("not connected to fragment owner {owner}"))
        })?;
        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let expected_key = op.key.clone();
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending_mutations.lock().unwrap().insert(
            req_id,
            PendingMutation {
                expected_peer: owner.clone(),
                expected_keys: vec![expected_key.clone()],
                reply: resp_tx,
            },
        );
        let _pending_guard = PendingMutationGuard {
            req_id,
            pending: self.pending_mutations.clone(),
        };
        let response = tokio::time::timeout(REMOTE_RPC_TIMEOUT, async {
            if !try_send_envelope(
                &tx,
                Envelope {
                    msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                        key: op.key,
                        value: op.value.value,
                        timestamp_ms: op.value.timestamp_ms,
                        node_id: op.value.node_id,
                        seq: op.seq,
                        boot_id: op.boot_id,
                        expires_at_ms: op.value.expires_at_ms.unwrap_or(0),
                        is_tombstone: op.value.is_tombstone,
                        priority: priority_to_proto(priority),
                        hops: 0,
                        req_id,
                    })),
                },
            )
            .await
            {
                return Err(NodeError::Network(format!(
                    "connection to fragment owner {owner} closed while sending mutation"
                )));
            }
            resp_rx
                .await
                .map_err(|_| NodeError::Actor("forwarded mutation reply dropped".to_string()))
        })
        .await;
        match response {
            Ok(Ok(mut results)) => {
                validate_mutation_results(std::slice::from_ref(&expected_key), &results)?;
                let result = results.remove(0);
                decode_mutation_result(result)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(NodeError::Network(format!(
                "forwarded mutation to {owner} timed out"
            ))),
        }
    }

    /// Read a key. Routed in one hop to a node serving the key's
    /// fragment: read locally if we serve it, otherwise forwarded to a
    /// replica holder with the reply awaited.
    pub async fn get(&self, key: String) -> Result<Option<VersionedValue>, NodeError> {
        self.get_with_priority(key, OpPriority::Default).await
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
        if let Some(actor) = self.fragment_actor(&key) {
            let res = actor
                .call(
                    |reply| KvMessage::Get {
                        key,
                        reply,
                        priority,
                    },
                    None,
                )
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?;
            return match res {
                CallResult::Success(Ok(value)) => Ok(value),
                CallResult::Success(Err(shed)) => Err(NodeError::Backpressure(shed)),
                CallResult::Timeout => Err(NodeError::Actor("get timed out".to_string())),
                CallResult::SenderError => {
                    Err(NodeError::Actor("get reply channel dropped".to_string()))
                }
            };
        }

        // Remote fragment: forward the read, await the reply. The
        // read executes at the caller's priority on the remote owner.
        let frag = fragment_of(&key, self.router.fragment_count);
        let owner = self
            .pick_replica(frag)
            .ok_or_else(|| NodeError::Actor(format!("no replica for fragment {frag}")))?;
        let tx = self.peer_sender(&owner).await.ok_or_else(|| {
            NodeError::Network(format!("not connected to fragment owner {owner}"))
        })?;
        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending_gets.lock().await.insert(req_id, resp_tx);
        send_envelope(
            &tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key,
                    req_id,
                    priority: priority_to_proto(priority),
                    hops: 0,
                })),
            },
        )
        .await;
        // Bounded wait: a dead owner must not hang the caller forever.
        match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(shed))) => Err(NodeError::Backpressure(shed)),
            Ok(Err(_)) => Err(NodeError::Actor("forwarded get dropped".to_string())),
            Err(_) => {
                self.pending_gets.lock().await.remove(&req_id);
                Err(NodeError::Network(format!(
                    "forwarded get to {owner} timed out"
                )))
            }
        }
    }

    /// Read the full local copy across all fragments (debugging / tests).
    pub async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let all = self.router.get_all().await?;
        // Hide tombstones from user reads (sync needs them, users don't).
        Ok(all.into_iter().filter(|(_, v)| !v.is_tombstone).collect())
    }

    /// Put multiple keys with an explicit priority (Phase 7). Bulk
    /// callers should prefer [`OpPriority::Background`]; the priority is
    /// carried on the wire and the owner applies the batch at the same
    /// priority, so fan-out preserves traffic isolation end-to-end.
    ///
    /// Each key is hashed to its fragment; keys are grouped by fragment
    /// and each fragment's keys are sent with one `BatchPut` message,
    /// failing over across replica holders (self first) until one
    /// accepts the batch. Returns per-key results in input order.
    ///
    /// Local and remote keys await an exact per-key apply outcome. Remote
    /// batches have a 5s timeout per replica attempt and fail over
    /// idempotently when a connection drops or no response arrives.
    pub async fn put_multi_with_priority(
        &self,
        kvs: Vec<(String, Vec<u8>)>,
        priority: OpPriority,
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

        // Local puts run concurrently, each awaiting its exact apply outcome
        // at the requested priority.
        let local_futs = local.into_iter().map(|(i, key, value)| {
            let this = self;
            async move {
                let r = this.put_with_priority(key, value, priority).await;
                (i, r)
            }
        });
        for (i, r) in futures::future::join_all(local_futs).await {
            results[i] = Some(r);
        }

        // Remote puts: one BatchPut per fragment, failing over across
        // replica holders until one accepts the batch.
        let wire_priority = priority_to_proto(priority);
        for (frag, items) in remote {
            // Build the batch once, with HLC timestamps.
            let mut puts = Vec::with_capacity(items.len());
            let mut indices = Vec::with_capacity(items.len());
            for (i, key, value) in items {
                let seq = {
                    let mut guard = self.next_seq.lock().await;
                    *guard += 1;
                    *guard
                };
                let now = self.hlc.lock().await.tick();
                puts.push(ForwardedPut {
                    key,
                    value,
                    timestamp_ms: now,
                    node_id: self.node_id.clone(),
                    seq,
                    boot_id: self.boot_id,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: wire_priority,
                    hops: 0,
                    req_id: 0,
                });
                indices.push(i);
            }
            let expected_keys: Vec<String> = puts.iter().map(|put| put.key.clone()).collect();
            let mut done = false;
            let mut tried: Vec<String> = Vec::new();
            for owner in self.replica_order(frag) {
                tried.push(owner.clone());
                let Some(tx) = self.peer_sender(&owner).await else {
                    continue;
                };
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
                let response = tokio::time::timeout(REMOTE_RPC_TIMEOUT, async {
                    if !try_send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                                puts: puts.clone(),
                                priority: wire_priority,
                                req_id,
                            })),
                        },
                    )
                    .await
                    {
                        return Err(());
                    }
                    resp_rx.await.map_err(|_| ())
                })
                .await;
                match response {
                    Ok(Ok(wire_results))
                        if validate_mutation_results(&expected_keys, &wire_results).is_ok() =>
                    {
                        // A stale/non-serving holder or failed relay returns a
                        // typed routing failure for every key. Retry the exact
                        // stamped batch at the next configured replica.
                        if wire_results.iter().all(|result| {
                            MutationStatus::try_from(result.status)
                                == Ok(MutationStatus::Unavailable)
                        }) {
                            continue;
                        }
                        for (index, result) in indices.iter().zip(wire_results) {
                            results[*index] = Some(decode_mutation_result(result));
                        }
                        done = true;
                        break;
                    }
                    // A timeout or malformed/dropped response may follow a
                    // successful apply. Retrying the exact stamped versions
                    // at another holder is safe under the idempotent merge.
                    _ => {}
                }
            }
            if !done {
                for i in indices {
                    results[i] = Some(Err(NodeError::Network(format!(
                        "no replica acknowledged batch put for fragment {frag} (tried {})",
                        tried.join(",")
                    ))));
                }
            }
        }

        results.into_iter().map(|r| r.unwrap()).collect()
    }

    /// Put multiple keys. Each key is hashed to its fragment; keys are
    /// grouped by fragment and each fragment's keys are sent with one
    /// `BatchPut` message, failing over across replica holders.
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
    /// and each fragment's keys are fetched with one `BatchGet` message.
    /// Returns per-key results in input order.
    ///
    /// Remote reads have a 5s timeout per replica attempt and fail over
    /// to the next replica holder when the preferred owner is
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

        // Local gets: concurrent, at the requested priority.
        let local_futs = local.into_iter().map(|(i, key)| {
            let this = self;
            async move {
                let r = this.get_with_priority(key, priority).await;
                (i, r)
            }
        });
        for (i, r) in futures::future::join_all(local_futs).await {
            results[i] = Some(r);
        }

        // Remote gets: one BatchGet per fragment, failing over across
        // replica holders on timeout or dropped replies.
        let wire_priority = priority_to_proto(priority);
        for (frag, items) in remote {
            let keys: Vec<String> = items.iter().map(|(_, k)| k.clone()).collect();
            let indices: Vec<usize> = items.iter().map(|(i, _)| *i).collect();
            let mut done = false;
            let mut tried: Vec<String> = Vec::new();
            for owner in self.replica_order(frag) {
                tried.push(owner.clone());
                let Some(tx) = self.peer_sender(&owner).await else {
                    continue;
                };
                let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
                let (resp_tx, resp_rx) = oneshot::channel();
                self.pending_batch_gets.lock().await.insert(req_id, resp_tx);
                send_envelope(
                    &tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                            req_id,
                            keys: keys.clone(),
                            priority: wire_priority,
                        })),
                    },
                )
                .await;
                match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
                    Ok(Ok(kvs)) => {
                        // Map back by key.
                        let by_key: HashMap<String, BatchKeyValue> =
                            kvs.into_iter().map(|kv| (kv.key.clone(), kv)).collect();
                        for (i, key) in indices.iter().zip(&keys) {
                            match by_key.get(key) {
                                Some(kv) if kv.shed => {
                                    results[*i] = Some(Err(NodeError::Backpressure(format!(
                                        "batch get for key {key} shed by remote"
                                    ))));
                                }
                                Some(kv) if kv.found => {
                                    let mut v = VersionedValue::new(
                                        kv.value.clone(),
                                        kv.timestamp_ms,
                                        kv.node_id.clone(),
                                    );
                                    v.expires_at_ms = if kv.expires_at_ms == 0 {
                                        None
                                    } else {
                                        Some(kv.expires_at_ms)
                                    };
                                    v.is_tombstone = kv.is_tombstone;
                                    // Tombstones read as absent.
                                    results[*i] =
                                        Some(Ok(if v.is_tombstone { None } else { Some(v) }));
                                }
                                _ => results[*i] = Some(Ok(None)),
                            }
                        }
                        done = true;
                        break;
                    }
                    // Dropped reply or timeout: clean up and fail over
                    // to the next replica. A late response for a
                    // removed req_id is ignored by the handler.
                    _ => {
                        self.pending_batch_gets.lock().await.remove(&req_id);
                    }
                }
            }
            if !done {
                for i in indices {
                    results[i] = Some(Err(NodeError::Network(format!(
                        "no replica answered batch get for fragment {frag} (tried {})",
                        tried.join(",")
                    ))));
                }
            }
        }

        results.into_iter().map(|r| r.unwrap()).collect()
    }

    /// Get multiple keys. Each key is hashed to its fragment; keys are
    /// grouped by fragment and each fragment's keys are fetched with one
    /// `BatchGet` message, failing over across replica holders.
    /// Returns per-key results in input order.
    ///
    /// Bulk gets run at [`OpPriority::Background`]; see
    /// [`get_multi_with_priority`](Self::get_multi_with_priority) for an
    /// explicit priority. Remote reads have a 5s timeout per replica
    /// attempt; a dead owner fails only its keys, and only when no
    /// replica holder answers.
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
        let reports = self.sender_reports.lock().await;
        match reports.get(&(fragment, peer.to_string())) {
            Some(r) => Some(r.lock().await.clone()),
            None => None,
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

    /// Shut the node down: abort all connection tasks and stop every fragment
    /// supervisor. Stopping the actors drops their state, which releases the
    /// RocksDB lock (when persistence is enabled) so the database can
    /// be reopened — e.g. by a restarted node on the same path.
    pub async fn shutdown(&self) {
        let mut tasks = self.tasks.lock().await;
        for t in tasks.drain(..) {
            t.abort();
        }
        // Stop the supervisors; each drains its dispatcher pool (which
        // flushes) and its background actors. Failures just mean an
        // actor was already dead.
        let supervisors: Vec<ActorRef<SupervisorMessage>> =
            self.supervisors.lock().await.values().cloned().collect();
        for supervisor in supervisors {
            let _ = supervisor.stop_and_wait(None, None).await;
        }
    }

    /// Health snapshots for every fragment this node serves.
    pub async fn health(&self) -> Result<HashMap<FragmentId, FragmentHealth>, NodeError> {
        let supervisors: Vec<(FragmentId, ActorRef<SupervisorMessage>)> = self
            .supervisors
            .lock()
            .await
            .iter()
            .map(|(f, s)| (*f, s.clone()))
            .collect();
        let mut out = HashMap::with_capacity(supervisors.len());
        for (frag, supervisor) in &supervisors {
            let res = supervisor
                .call(|reply| SupervisorMessage::GetHealth { reply }, None)
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?;
            match res {
                CallResult::Success(h) => {
                    out.insert(*frag, h);
                }
                _ => return Err(NodeError::Actor("health check failed".to_string())),
            }
        }
        Ok(out)
    }

    /// Phase 8 placement report: this node's authoritative placement
    /// map plus per-fragment lifecycle state (Serving, Bootstrapping,
    /// Draining, Stopped).
    pub async fn placement_report(&self) -> PlacementReport {
        let placement = self.fragment_map.read().unwrap().clone();
        let states = self.placement_states.lock().await.clone();
        PlacementReport {
            placement,
            states,
            dynamic: self.placement_controller.is_some(),
        }
    }

    /// Full-state sync entries applied via SyncResponse since start.
    /// Stays 0 when dynamic placement is enabled (the handshake skips
    /// the full-state sync; the Phase 5 reconciler owns anti-entropy).
    pub fn sync_entries_applied(&self) -> u64 {
        self.sync_entries_applied.load(Ordering::Relaxed)
    }
}

/// Everything the Phase 8 placement driver needs. All shared
/// ownership; `Clone` for spawning per-fragment gain/lose tasks.
#[derive(Clone)]
struct PlacementDriverDeps {
    node_id: String,
    controller: Arc<dyn PlacementController>,
    poll_interval: Duration,
    drain_warn_interval: Duration,
    fragment_count: usize,
    subtree_config: NodeConfig,
    router: FragmentRouter,
    serving: Arc<RwLock<HashSet<FragmentId>>>,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>>,
    placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>>,
    senders: SenderMap,
    sender_reports: SenderReportMap,
    reset_senders: ResetSenderSet,
    sender_config: SenderConfig,
    peers: PeerMap,
    hlc: Arc<Mutex<HlcClock>>,
}

impl PlacementDriverDeps {
    /// Idempotent sender spawn for (`fragment`, `peer`).
    async fn ensure_sender(&self, fragment: FragmentId, peer: &str) {
        ensure_sender_entry(
            fragment,
            peer,
            SenderEntryDeps {
                config: &self.sender_config,
                router: &self.router,
                hlc: &self.hlc,
                reset_senders: &self.reset_senders,
                senders: &self.senders,
                reports: &self.sender_reports,
            },
        )
        .await;
    }

    /// Completed reconcile rounds for (`fragment`, `peer`), if the
    /// sender exists.
    async fn sender_rounds(&self, fragment: FragmentId, peer: &str) -> Option<u64> {
        let r = {
            let reports = self.sender_reports.lock().await;
            reports.get(&(fragment, peer.to_string())).cloned()?
        };
        let guard = r.lock().await;
        Some(guard.reconcile_rounds)
    }
}

/// Phase 8 placement driver: polls the controller and converges local
/// state (fragment subtrees, senders, routing) toward the desired
/// placement. Also wakes on membership change.
async fn placement_driver(
    deps: PlacementDriverDeps,
    mut membership_rx: mpsc::UnboundedReceiver<()>,
) {
    // Structured ownership matters here: gain/loss transitions may wait on
    // remote peers. Dropping the driver must abort those children instead of
    // leaving detached tasks holding fragment state after node shutdown.
    let mut transitions = JoinSet::new();
    // Converge immediately at startup; then on interval / membership.
    placement_tick(&deps, &mut transitions).await;
    let mut interval = tokio::time::interval(deps.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                placement_tick(&deps, &mut transitions).await;
            }
            msg = membership_rx.recv() => {
                if msg.is_none() {
                    break; // node shutting down
                }
                let members: Vec<String> =
                    deps.peers.lock().await.keys().cloned().collect();
                deps.controller.on_membership_change(&members);
                placement_tick(&deps, &mut transitions).await;
            }
            completed = transitions.join_next(), if !transitions.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "placement transition task failed");
                }
            }
        }
    }
}

/// One driver tick: publish the controller's map as the authoritative
/// routing table, then gain/lose fragments to match.
async fn placement_tick(deps: &PlacementDriverDeps, transitions: &mut JoinSet<()>) {
    let desired = deps.controller.desired_placement();
    if let Err(e) = validate_placement(&desired, deps.fragment_count) {
        eprintln!("[placement] invalid desired placement ({e}); keeping current");
        return;
    }
    // The controller's map is the routing table.
    *deps.placement.write().unwrap() = desired.clone();

    let mine_desired: HashSet<FragmentId> = desired
        .iter()
        .filter(|(_, r)| r.iter().any(|id| id == &deps.node_id))
        .map(|(f, _)| *f)
        .collect();
    let (mine_current, in_flight): (HashSet<FragmentId>, HashSet<FragmentId>) = {
        let states = deps.placement_states.lock().await;
        let mut current = HashSet::new();
        let mut flight = HashSet::new();
        for (f, s) in states.iter() {
            match s {
                FragmentPlacementState::Stopped => {}
                FragmentPlacementState::Bootstrapping | FragmentPlacementState::Draining => {
                    current.insert(*f);
                    flight.insert(*f);
                }
                FragmentPlacementState::Serving => {
                    current.insert(*f);
                }
            }
        }
        (current, flight)
    };

    // Gains: mark Bootstrapping synchronously so the next tick doesn't
    // spawn a duplicate.
    for f in mine_desired.difference(&mine_current) {
        deps.placement_states
            .lock()
            .await
            .insert(*f, FragmentPlacementState::Bootstrapping);
        let d = deps.clone();
        let frag = *f;
        transitions.spawn(async move { gain_fragment(&d, frag).await });
    }
    // Losses (skip ones already draining).
    for f in mine_current.difference(&mine_desired) {
        if in_flight.contains(f) {
            continue;
        }
        deps.placement_states
            .lock()
            .await
            .insert(*f, FragmentPlacementState::Draining);
        let d = deps.clone();
        let frag = *f;
        transitions.spawn(async move { lose_fragment(&d, frag).await });
    }
    // Steady state: keep senders aligned with the replica sets.
    for f in mine_desired.intersection(&mine_current) {
        if in_flight.contains(f) {
            continue;
        }
        ensure_fragment_senders(deps, *f, &desired).await;
    }
}

/// Keep the sender set for a served fragment aligned with its replica
/// set: spawn missing senders, (re)connect them, prune senders for
/// peers that are no longer replicas.
async fn ensure_fragment_senders(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    desired: &HashMap<FragmentId, Vec<String>>,
) {
    let replicas: Vec<String> = desired
        .get(&fragment)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|p| p != &deps.node_id)
        .collect();
    for peer in &replicas {
        deps.ensure_sender(fragment, peer).await;
    }
    // (Re)connect senders whose peers are currently connected. (The
    // Hello handler does this for new connections; the driver covers
    // senders spawned after the connection was established.)
    {
        let peers = deps.peers.lock().await;
        let senders = deps.senders.lock().await;
        for peer in &replicas {
            if let (Some(tx), Some(sender)) =
                (peers.get(peer), senders.get(&(fragment, peer.clone())))
            {
                let _ = sender.send(SenderMsg::PeerConnected {
                    peer_tx: tx.clone(),
                });
            }
        }
    }
    // Prune senders for peers that are no longer replicas. Their
    // buffered ops were fanned out to the remaining senders too, and
    // the peer's copy is not authoritative.
    let stale: Vec<(FragmentId, String)> = deps
        .senders
        .lock()
        .await
        .keys()
        .filter(|(f, p)| *f == fragment && !replicas.contains(p))
        .cloned()
        .collect();
    if !stale.is_empty() {
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let mut reset = deps.reset_senders.lock().await;
        for key in stale {
            senders.remove(&key);
            reports.remove(&key);
            reset.remove(&key);
        }
    }
}

/// Bootstrap timeout: bound the wait for a source peer / reconcile
/// round. On timeout the fragment still goes Serving (availability
/// over consistency); LWW + periodic reconcile converge it later.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);

/// Gain a fragment: spawn the subtree, bootstrap state from a replica
/// holder via the Phase 5 reconciler (chunked, bounded), then go
/// Serving. The fragment is NOT authoritative until bootstrap
/// completes (or times out): local reads/writes forward to the
/// previous owner meanwhile.
async fn gain_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let (supervisor, dispatcher) = match spawn_fragment_subtree(
        &deps.node_id,
        fragment,
        &deps.subtree_config,
        &deps.reset_senders,
        &deps.hlc,
    )
    .await
    {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[placement] failed to spawn subtree for {fragment}: {e}");
            // Drop the Bootstrapping mark so the next tick retries.
            deps.placement_states.lock().await.remove(&fragment);
            return;
        }
    };
    deps.supervisors.lock().await.insert(fragment, supervisor);
    // The actor exists in the router now (so the reconciler can merge
    // into it), but the fragment is not serving yet.
    deps.router.insert(fragment, dispatcher);

    let replicas: Vec<String> = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .cloned()
        .unwrap_or_default();
    let sources: Vec<String> = replicas
        .iter()
        .filter(|p| *p != &deps.node_id)
        .cloned()
        .collect();
    for peer in &replicas {
        if peer != &deps.node_id {
            deps.ensure_sender(fragment, peer).await;
        }
    }
    // (Re)connect the senders whose peers are up.
    {
        let peers = deps.peers.lock().await;
        let senders = deps.senders.lock().await;
        for peer in &sources {
            if let (Some(tx), Some(sender)) =
                (peers.get(peer), senders.get(&(fragment, peer.clone())))
            {
                let _ = sender.send(SenderMsg::PeerConnected {
                    peer_tx: tx.clone(),
                });
            }
        }
    }

    // Bootstrap: wait for a source peer, trigger an on-demand
    // reconcile round, and wait for it to complete.
    let bootstrapped = bootstrap_from(deps, fragment, &sources).await;
    if !bootstrapped {
        eprintln!("[placement] bootstrap for {fragment} timed out; serving anyway");
    }
    deps.serving.write().unwrap().insert(fragment);
    deps.placement_states
        .lock()
        .await
        .insert(fragment, FragmentPlacementState::Serving);
    deps.controller.on_fragment_ready(fragment);
}

/// Run one reconcile round against a source holder and wait for it to
/// complete. Returns false on timeout / no source.
async fn bootstrap_from(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    sources: &[String],
) -> bool {
    let start = std::time::Instant::now();
    // Wait for a source peer to be connected.
    let source = loop {
        {
            let peers = deps.peers.lock().await;
            if let Some(s) = sources.iter().find(|p| peers.contains_key(*p)) {
                break s.clone();
            }
        }
        if start.elapsed() > BOOTSTRAP_TIMEOUT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let before = deps.sender_rounds(fragment, &source).await.unwrap_or(0);
    {
        let senders = deps.senders.lock().await;
        match senders.get(&(fragment, source.clone())) {
            Some(tx) => {
                let _ = tx.send(SenderMsg::ReconcileNow);
            }
            None => return false,
        }
    }
    // Wait for the round to complete.
    loop {
        if let Some(rounds) = deps.sender_rounds(fragment, &source).await {
            if rounds > before {
                return true;
            }
        }
        if start.elapsed() > BOOTSTRAP_TIMEOUT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Lose a fragment: stop serving new local ops (they forward to the
/// new owner), drain replication senders, then stop the subtree.
async fn lose_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    // Leave the serving set first: new local ops forward to the new
    // owner instead of enqueueing here.
    deps.serving.write().unwrap().remove(&fragment);
    // Keep senders to the current replicas so queued ops drain out.
    let replicas: Vec<String> = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .cloned()
        .unwrap_or_default();
    for peer in &replicas {
        if peer != &deps.node_id {
            deps.ensure_sender(fragment, peer).await;
        }
    }
    {
        let peers = deps.peers.lock().await;
        let senders = deps.senders.lock().await;
        for ((f, p), tx) in senders.iter() {
            if *f == fragment {
                if let Some(peer_tx) = peers.get(p) {
                    let _ = tx.send(SenderMsg::PeerConnected {
                        peer_tx: peer_tx.clone(),
                    });
                }
            }
        }
    }
    // Wait until every copy has actually drained. A slow or partitioned peer
    // leaves this fragment in Draining; operators may repair placement or
    // connectivity without the old owner destroying its recovery state.
    let mut next_warning = std::time::Instant::now() + deps.drain_warn_interval;
    loop {
        let still_revoked = deps
            .placement
            .read()
            .unwrap()
            .get(&fragment)
            .map(|replicas| !replicas.iter().any(|node| node == &deps.node_id))
            .unwrap_or(true);
        if !still_revoked {
            // The controller reversed the move while it was draining. Restore
            // local service instead of letting the stale transition remove a
            // newly authoritative fragment.
            deps.serving.write().unwrap().insert(fragment);
            deps.placement_states
                .lock()
                .await
                .insert(fragment, FragmentPlacementState::Serving);
            return;
        }
        let drained = {
            let reports = deps.sender_reports.lock().await;
            let mut ok = true;
            for ((f, _), r) in reports.iter() {
                if *f == fragment {
                    let rep = r.lock().await;
                    if rep.buffered != 0 || rep.in_flight != 0 || rep.state == "buffer_reset" {
                        ok = false;
                        break;
                    }
                }
            }
            ok
        };
        if drained {
            break;
        }
        if std::time::Instant::now() >= next_warning {
            tracing::warn!(
                fragment = fragment.0,
                "fragment remains Draining while replication recovery is incomplete"
            );
            next_warning = std::time::Instant::now() + deps.drain_warn_interval;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A placement reversal can race the final report poll.
    let still_revoked = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .map(|replicas| !replicas.iter().any(|node| node == &deps.node_id))
        .unwrap_or(true);
    if !still_revoked {
        deps.serving.write().unwrap().insert(fragment);
        deps.placement_states
            .lock()
            .await
            .insert(fragment, FragmentPlacementState::Serving);
        return;
    }
    // Tear down: stop the supervisor, drop the actor and the senders.
    if let Some(sup) = deps.supervisors.lock().await.remove(&fragment) {
        let _ = sup.stop_and_wait(None, None).await;
    }
    deps.router.remove(fragment);
    {
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let mut reset = deps.reset_senders.lock().await;
        let keys: Vec<(FragmentId, String)> = senders
            .keys()
            .filter(|(f, _)| *f == fragment)
            .cloned()
            .collect();
        for key in keys {
            senders.remove(&key);
            reports.remove(&key);
            reset.remove(&key);
        }
    }
    deps.placement_states
        .lock()
        .await
        .insert(fragment, FragmentPlacementState::Stopped);
}

/// Drive one peer connection: handshake, then a read loop.
///
/// `we_dialed` distinguishes the dialer (sends `Hello` immediately) from
/// the listener (replies to the dialer's `Hello`).
///
/// The writer task is registered in `tasks` alongside everything else:
/// aborting the read loop alone must not orphan it, or the socket's
/// write half stays open and the peer never observes the disconnect.
async fn handle_connection(ctx: ConnContext, stream: BoxStream, we_dialed: bool) {
    let ConnContext {
        node_id,
        router,
        serving,
        placement,
        dynamic_placement,
        pending_gets,
        pending_batch_gets,
        pending_mutations,
        pending_relays,
        pending_mutation_relays,
        next_req_id,
        peers,
        tasks,
        hlc,
        senders,
        sender_config,
        reset_senders,
        sender_reports,
        sync_entries_applied,
        membership_tx,
        cluster_secret,
    } = ctx;
    // Phase 5: current session per (peer, fragment) on this connection.
    // Sessions are per-connection: a new connection means a new pairing.
    let sessions: Arc<Mutex<HashMap<(String, FragmentId), u64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    // An untracked reconciliation repair has no individual ACK. Remember its
    // first failure so the following tracked empty barrier NACKs instead of
    // falsely declaring the session converged. A fresh SessionStart clears
    // the poison and lets the sender retry the complete round.
    let mut reconcile_failures: HashMap<(String, FragmentId), (u64, String)> = HashMap::new();
    let (mut reader, writer) = tokio::io::split(stream);
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(PEER_CHANNEL_DEPTH);

    // Writer task: owns the socket's write half, drains the queue.
    let writer_task = tokio::spawn(async move {
        let mut w = writer;
        while let Some(frame) = rx.recv().await {
            if w.write_u32(frame.len() as u32).await.is_err() {
                break;
            }
            if w.write_all(&frame).await.is_err() {
                break;
            }
        }
    });
    tasks.lock().await.push(writer_task.abort_handle());

    // The dialer introduces itself first.
    if we_dialed {
        send_hello(&tx, &node_id, cluster_secret.as_deref()).await;
    }

    // Phase 8: shared re-forward context (migration safety).
    let rfctx = ReforwardCtx {
        node_id: node_id.clone(),
        placement: placement.clone(),
        peers: peers.clone(),
        pending_relays: pending_relays.clone(),
        pending_mutation_relays: pending_mutation_relays.clone(),
        next_req_id: next_req_id.clone(),
    };

    let mut peer_node_id: Option<String> = None;
    // Length-delimited framing: 4-byte big-endian length prefix.
    while let Ok(n) = reader.read_u32().await {
        let len = n as usize;
        if len > MAX_FRAME_BYTES {
            break; // corrupt stream; drop the connection
        }
        let mut buf = vec![0u8; len];
        if reader.read_exact(&mut buf).await.is_err() {
            break;
        }
        let envelope = match Envelope::decode(&buf[..]) {
            Ok(e) => e,
            Err(_) => break,
        };

        // Gate: only Hello may be processed before the handshake
        // completes. Any other message arriving pre-handshake is
        // ignored — an unauthenticated peer must not be able to issue
        // reads, writes, or sync requests (adversarial review #3).
        if peer_node_id.is_none() && !matches!(envelope.msg, Some(EnvelopeMsg::Hello(_))) {
            tracing::debug!("ignoring pre-handshake non-Hello message");
            continue;
        }

        match envelope.msg {
            Some(EnvelopeMsg::Hello(Hello {
                node_id: peer,
                auth_tag,
            })) => {
                // A connection has exactly one immutable peer identity. A
                // second Hello could otherwise replace `peer_node_id`, churn
                // every sender session, and leave the old peer-map entry
                // pointing at this connection.
                if peer_node_id.is_some() {
                    tracing::warn!(
                        claimed_node_id = %peer,
                        "rejecting second Hello on an established connection"
                    );
                    break;
                }
                // Reject Hellos claiming our own id (misconfiguration or
                // impersonation attempt).
                if peer == node_id {
                    tracing::warn!("rejecting Hello claiming our own node_id; dropping connection");
                    break;
                }
                // Authenticate the handshake when a cluster secret is
                // configured. Without it, anyone with TCP reachability
                // can join the mesh (adversarial review #3, #4).
                if let Some(secret) = cluster_secret.as_deref() {
                    if !verify_hello_auth_tag(secret, &peer, &auth_tag) {
                        tracing::warn!(
                            "Hello auth failed for claimed node_id={peer}; dropping connection"
                        );
                        break;
                    }
                }
                peer_node_id = Some(peer.clone());
                peers.lock().await.insert(peer.clone(), tx.clone());
                if !we_dialed {
                    // The listener answers the handshake...
                    send_hello(&tx, &node_id, cluster_secret.as_deref()).await;
                }
                // ...and both sides then pull the fragments they serve
                // for anti-entropy — unless dynamic placement is on, in
                // which case the Phase 5 reconciler owns anti-entropy
                // (delta-only; no full-state copy).
                if !dynamic_placement {
                    send_sync_request(&tx, router.fragment_ids()).await;
                }
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx.clone() {
                    let _ = mtx.send(());
                }
                // Phase 5: (re)connect the senders for this peer. Legacy
                // mode (no fragment map) spawns senders lazily here.
                for frag_id in router.fragment_ids() {
                    let frag = FragmentId(frag_id);
                    let is_replica = {
                        let map = placement.read().unwrap();
                        map.is_empty()
                            || map
                                .get(&frag)
                                .map(|replicas| replicas.iter().any(|id| id == &peer))
                                .unwrap_or(false)
                    };
                    if is_replica {
                        ensure_sender_entry(
                            frag,
                            &peer,
                            SenderEntryDeps {
                                config: &sender_config,
                                router: &router,
                                hlc: &hlc,
                                reset_senders: &reset_senders,
                                senders: &senders,
                                reports: &sender_reports,
                            },
                        )
                        .await;
                    }
                }
                {
                    // Connection handlers for the same claimed peer can race.
                    // Serialize the winner check with peer replacement and
                    // disconnect notification so an older Hello cannot publish
                    // its channel after a newer connection won the map entry.
                    let peers_guard = peers.lock().await;
                    if !peers_guard
                        .get(&peer)
                        .map_or(false, |current| current.same_channel(&tx))
                    {
                        continue;
                    }
                    let senders_guard = senders.lock().await;
                    for ((_, sender_peer), sender_tx) in senders_guard.iter() {
                        if sender_peer == &peer {
                            let _ = sender_tx.send(SenderMsg::PeerConnected {
                                peer_tx: tx.clone(),
                            });
                        }
                    }
                }
            }
            Some(EnvelopeMsg::SessionStart(s)) => {
                // New sender↔receiver pairing: adopt the session id.
                // Batches from older sessions are discarded.
                // Zero is protobuf's absent/default value and must not create
                // a session that legacy, uncorrelated messages can match.
                if s.session_id == 0 {
                    tracing::warn!(
                        fragment = s.fragment,
                        "ignoring SessionStart with default session id"
                    );
                    continue;
                }
                if let Some(peer) = peer_node_id.clone() {
                    let key = (peer, FragmentId(s.fragment));
                    let previous = sessions.lock().await.insert(key.clone(), s.session_id);
                    if previous != Some(s.session_id) {
                        reconcile_failures.remove(&key);
                    }
                }
            }
            Some(EnvelopeMsg::ReplicateBatch(b)) => {
                // Validate the session and the whole batch before applying
                // anything. An ACK is sent only after every exact outcome is
                // known and the sender's requested fence is reached.
                let peer = match peer_node_id.clone() {
                    Some(p) => p,
                    None => continue,
                };
                let frag = FragmentId(b.fragment);
                let session_key = (peer.clone(), frag);
                let current = sessions.lock().await.get(&session_key).copied();
                if current != Some(b.session_id) {
                    continue; // stale session
                }
                let tracked = b.batch_seq != u64::MAX;
                let requested_ack = if tracked {
                    match ReplicationAckLevel::try_from(b.required_ack_level) {
                        Ok(ReplicationAckLevel::Applied) => {
                            Some(ReplicationAckDurability::MemoryApplied)
                        }
                        Ok(ReplicationAckLevel::StoreCommitted) => {
                            Some(ReplicationAckDurability::StoreCommitted)
                        }
                        Ok(ReplicationAckLevel::Unknown) | Err(_) => None,
                    }
                } else {
                    None
                };
                let prior_reconcile_failure = reconcile_failures
                    .get(&session_key)
                    .filter(|(session_id, _)| *session_id == b.session_id)
                    .map(|(_, error)| error.clone());
                let invalid_key = b
                    .ops
                    .iter()
                    .find(|op| fragment_of(&op.key, router.fragment_count) != frag)
                    .map(|op| op.key.clone());
                let actor = router.route_for_fragment(frag);
                let mut failure = prior_reconcile_failure.or_else(|| {
                    match (tracked, requested_ack, invalid_key, actor.as_ref()) {
                        (true, None, _, _) => Some(
                            "tracked replication batch omitted a valid ACK requirement".to_string(),
                        ),
                        (_, _, Some(key), _) => Some(format!(
                            "replication key {key:?} does not belong to declared fragment {frag}"
                        )),
                        (_, _, None, None) => Some(format!(
                            "receiver does not host declared replication fragment {frag}"
                        )),
                        (_, _, None, Some(_)) => None,
                    }
                });

                if failure.is_none() {
                    if let Some(timestamp_ms) = b.ops.iter().map(|op| op.timestamp_ms).max() {
                        if !observe_remote_timestamp(&hlc, timestamp_ms, "ReplicateBatch").await {
                            break;
                        }
                    }
                }

                if failure.is_none() {
                    let actor = actor.as_ref().expect("validated actor");
                    for op in b.ops {
                        let replicated = ReplicatedOp {
                            key: op.key,
                            value: {
                                let mut v =
                                    VersionedValue::new(op.value, op.timestamp_ms, op.node_id);
                                v.expires_at_ms = if op.expires_at_ms == 0 {
                                    None
                                } else {
                                    Some(op.expires_at_ms)
                                };
                                v.is_tombstone = op.is_tombstone;
                                v
                            },
                            seq: op.seq,
                            boot_id: op.boot_id,
                        };
                        if let Err(error) =
                            call_apply(actor, replicated, OpPriority::Default, true).await
                        {
                            failure = Some(error.to_string());
                            break;
                        }
                    }
                }

                let durability = if failure.is_none() {
                    match (tracked, requested_ack) {
                        (false, _) | (true, Some(ReplicationAckDurability::MemoryApplied)) => {
                            Some(ApplyDurability::MemoryApplied)
                        }
                        (true, Some(ReplicationAckDurability::StoreCommitted)) => {
                            match flush_fence(actor.as_ref().expect("validated actor")).await {
                                Ok(durability) => Some(durability),
                                Err(error) => {
                                    failure = Some(error.to_string());
                                    None
                                }
                            }
                        }
                        (true, None) => None,
                    }
                } else {
                    None
                };

                if tracked {
                    let response = if let Some(error) = failure {
                        EnvelopeMsg::BatchNack(BatchNack {
                            session_id: b.session_id,
                            batch_seq: b.batch_seq,
                            fragment: b.fragment,
                            error,
                        })
                    } else {
                        let level = match durability.expect("successful durability fence") {
                            ApplyDurability::MemoryApplied => ReplicationAckLevel::Applied,
                            ApplyDurability::StoreCommitted => ReplicationAckLevel::StoreCommitted,
                        };
                        EnvelopeMsg::BatchAck(BatchAck {
                            session_id: b.session_id,
                            batch_seq: b.batch_seq,
                            fragment: b.fragment,
                            level: level as i32,
                        })
                    };
                    send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(response),
                        },
                    )
                    .await;
                } else if let Some(error) = failure {
                    reconcile_failures
                        .entry(session_key)
                        .or_insert_with(|| (b.session_id, error.clone()));
                    tracing::warn!(
                        peer = %peer,
                        fragment = frag.0,
                        %error,
                        "reconciliation batch failed"
                    );
                }
            }
            Some(EnvelopeMsg::BatchAck(a)) => {
                // Route the ack to the sender for (fragment, peer).
                if let Some(peer) = peer_node_id.clone() {
                    let durability = match ReplicationAckLevel::try_from(a.level) {
                        Ok(ReplicationAckLevel::Applied) => ReplicationAckDurability::MemoryApplied,
                        Ok(ReplicationAckLevel::StoreCommitted) => {
                            ReplicationAckDurability::StoreCommitted
                        }
                        Ok(ReplicationAckLevel::Unknown) | Err(_) => {
                            tracing::warn!(
                                peer = %peer,
                                fragment = a.fragment,
                                level = a.level,
                                "ignoring replication ack with unknown durability"
                            );
                            continue;
                        }
                    };
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(a.fragment), peer)) {
                        let _ = tx.send(SenderMsg::Ack {
                            session_id: a.session_id,
                            batch_seq: a.batch_seq,
                            durability,
                        });
                    }
                }
            }
            Some(EnvelopeMsg::BatchNack(n)) => {
                if let Some(peer) = peer_node_id.clone() {
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(n.fragment), peer)) {
                        let _ = tx.send(SenderMsg::Nack {
                            session_id: n.session_id,
                            batch_seq: n.batch_seq,
                            error: n.error,
                        });
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileRequest(r)) => {
                // Peer wants our (timestamp, tombstone) summaries.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(r.fragment);
                let current = sessions.lock().await.get(&(peer, frag)).copied();
                // Both identifiers are nonzero so old senders, whose newly
                // added scalar fields decode to defaults, fail closed.
                if r.session_id == 0 || r.round_id == 0 || current != Some(r.session_id) {
                    tracing::debug!(
                        fragment = r.fragment,
                        session_id = r.session_id,
                        round_id = r.round_id,
                        "ignoring stale or uncorrelated reconcile request"
                    );
                    continue;
                }
                if let Some(actor) = router.route_for_fragment(frag) {
                    if let Ok(CallResult::Success(map)) =
                        actor.call(|reply| KvMessage::GetAll { reply }, None).await
                    {
                        let summaries = map
                            .into_iter()
                            .map(|(key, v): (String, VersionedValue)| KeySummary {
                                key,
                                timestamp_ms: v.timestamp_ms,
                                node_id: v.node_id,
                                is_tombstone: v.is_tombstone,
                            })
                            .collect();
                        send_envelope(
                            &tx,
                            Envelope {
                                msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                                    fragment: r.fragment,
                                    summaries,
                                    session_id: r.session_id,
                                    round_id: r.round_id,
                                })),
                            },
                        )
                        .await;
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileFetch(f)) => {
                // Peer wants full values for these keys.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(f.fragment);
                let current = sessions.lock().await.get(&(peer, frag)).copied();
                if f.session_id == 0 || f.round_id == 0 || current != Some(f.session_id) {
                    tracing::debug!(
                        fragment = f.fragment,
                        session_id = f.session_id,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "ignoring stale or uncorrelated reconcile fetch"
                    );
                    continue;
                }
                if let Some(actor) = router.route_for_fragment(frag) {
                    if let Ok(CallResult::Success(map)) =
                        actor.call(|reply| KvMessage::GetAll { reply }, None).await
                    {
                        let entries = f
                            .keys
                            .into_iter()
                            .filter_map(|key| {
                                map.get(&key).map(|v| SyncEntry {
                                    key: key.clone(),
                                    value: v.value.clone(),
                                    timestamp_ms: v.timestamp_ms,
                                    node_id: v.node_id.clone(),
                                    expires_at_ms: v.expires_at_ms.unwrap_or(0),
                                    is_tombstone: v.is_tombstone,
                                })
                            })
                            .collect();
                        send_envelope(
                            &tx,
                            Envelope {
                                msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                                    fragment: f.fragment,
                                    entries,
                                    session_id: f.session_id,
                                    round_id: f.round_id,
                                    chunk_seq: f.chunk_seq,
                                })),
                            },
                        )
                        .await;
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileResponse(r)) => {
                // Zero is the protobuf default for peers that do not speak
                // the correlated reconcile protocol. Reject it before it can
                // influence the HLC or reach a sender.
                if r.session_id == 0 || r.round_id == 0 {
                    tracing::debug!(
                        fragment = r.fragment,
                        session_id = r.session_id,
                        round_id = r.round_id,
                        "ignoring uncorrelated reconcile response"
                    );
                    continue;
                }
                // The sender owns exact session/round/phase correlation. Let
                // it validate correlation before this frame may advance the
                // HLC; stale responses must be side-effect free.
                if let Some(peer) = peer_node_id.clone() {
                    let sender = senders
                        .lock()
                        .await
                        .get(&(FragmentId(r.fragment), peer))
                        .cloned();
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        if tx
                            .send(SenderMsg::ReconcileResponse {
                                session_id: r.session_id,
                                round_id: r.round_id,
                                summaries: r.summaries,
                                connection_valid,
                            })
                            .is_err()
                        {
                            break;
                        }
                        if !matches!(
                            tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await,
                            Ok(Ok(true))
                        ) {
                            break;
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileData(d)) => {
                if d.session_id == 0 || d.round_id == 0 {
                    tracing::debug!(
                        fragment = d.fragment,
                        session_id = d.session_id,
                        round_id = d.round_id,
                        chunk_seq = d.chunk_seq,
                        "ignoring uncorrelated reconcile data"
                    );
                    continue;
                }
                if let Some(peer) = peer_node_id.clone() {
                    let sender = senders
                        .lock()
                        .await
                        .get(&(FragmentId(d.fragment), peer))
                        .cloned();
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        if tx
                            .send(SenderMsg::ReconcileData {
                                session_id: d.session_id,
                                round_id: d.round_id,
                                chunk_seq: d.chunk_seq,
                                entries: d.entries,
                                connection_valid,
                            })
                            .is_err()
                        {
                            break;
                        }
                        if !matches!(
                            tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await,
                            Ok(Ok(true))
                        ) {
                            break;
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedPut(p)) => {
                if !observe_remote_timestamp(&hlc, p.timestamp_ms, "ForwardedPut").await {
                    break;
                }
                let frag = fragment_of(&p.key, router.fragment_count);
                let is_serving = serving.read().unwrap().contains(&frag);
                if is_serving {
                    let result = match router.route_for_fragment(frag) {
                        Some(actor) => {
                            let replicated = ReplicatedOp {
                                key: p.key.clone(),
                                value: {
                                    let mut v = VersionedValue::new(
                                        p.value.clone(),
                                        p.timestamp_ms,
                                        p.node_id.clone(),
                                    );
                                    v.expires_at_ms =
                                        (p.expires_at_ms != 0).then_some(p.expires_at_ms);
                                    v.is_tombstone = p.is_tombstone;
                                    v
                                },
                                seq: p.seq,
                                boot_id: p.boot_id,
                            };
                            apply_forwarded_mutation(
                                &actor,
                                &senders,
                                frag,
                                replicated,
                                priority_from_proto(p.priority),
                                0,
                            )
                            .await
                        }
                        None => mutation_result(
                            p.key.clone(),
                            0,
                            Err(NodeError::Actor(format!(
                                "serving fragment {frag} has no actor"
                            ))),
                        ),
                    };
                    send_mutation_response(
                        &tx,
                        MutationResponse {
                            req_id: p.req_id,
                            results: vec![result],
                        },
                    )
                    .await;
                } else if p.hops < MAX_FORWARD_HOPS {
                    relay_put(p, frag, &tx, &peer_node_id, &rfctx).await;
                } else {
                    send_unavailable_mutation(
                        &tx,
                        p.req_id,
                        vec![p.key],
                        "forward hop limit reached",
                    )
                    .await;
                }
            }
            Some(EnvelopeMsg::ForwardedGet(g)) => {
                // We serve this fragment: read it and reply on this
                // connection. (A bootstrapping node is not yet
                // authoritative, so it relays instead of serving
                // possibly-stale state.)
                let frag = fragment_of(&g.key, router.fragment_count);
                let is_serving = serving.read().unwrap().contains(&frag);
                if !is_serving {
                    if g.hops < MAX_FORWARD_HOPS {
                        relay_get(&g, frag, &tx, &peer_node_id, &rfctx).await;
                    }
                    continue;
                }
                // Serve at the originator's priority (Phase 7).
                let get_priority = priority_from_proto(g.priority);
                let (value, shed) = match router.route(&g.key) {
                    Some(actor) => {
                        match actor
                            .call(
                                |reply| KvMessage::Get {
                                    key: g.key.clone(),
                                    reply,
                                    priority: get_priority,
                                },
                                None,
                            )
                            .await
                        {
                            Ok(CallResult::Success(Ok(v))) => (v, false),
                            Ok(CallResult::Success(Err(_))) => (None, true),
                            _ => (None, false),
                        }
                    }
                    None => (None, false),
                };
                let (found, bytes, timestamp_ms, origin, expires_at_ms, is_tombstone) = match value
                {
                    Some(v) => (
                        true,
                        v.value,
                        v.timestamp_ms,
                        v.node_id,
                        v.expires_at_ms.unwrap_or(0),
                        v.is_tombstone,
                    ),
                    None => (false, Vec::new(), 0, String::new(), 0, false),
                };
                send_envelope(
                    &tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                            req_id: g.req_id,
                            found,
                            value: bytes,
                            timestamp_ms,
                            node_id: origin,
                            expires_at_ms,
                            is_tombstone,
                            shed,
                        })),
                    },
                )
                .await;
            }
            Some(EnvelopeMsg::GetResponse(r)) => {
                if r.found && !observe_remote_timestamp(&hlc, r.timestamp_ms, "GetResponse").await {
                    break;
                }
                // A relayed read completing: forward the response to the
                // origin peer with its original req_id.
                if let Some((origin_tx, orig_id)) = pending_relays.lock().await.remove(&r.req_id) {
                    send_envelope(
                        &origin_tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                                req_id: orig_id,
                                ..r
                            })),
                        },
                    )
                    .await;
                    continue;
                }
                if let Some(resp_tx) = pending_gets.lock().await.remove(&r.req_id) {
                    if r.shed {
                        let _ = resp_tx.send(Err(
                            "shed: remote pool at queue cap or rate limit".to_string()
                        ));
                    } else {
                        let value = r.found.then(|| {
                            let mut v = VersionedValue::new(
                                r.value.clone(),
                                r.timestamp_ms,
                                r.node_id.clone(),
                            );
                            v.expires_at_ms = if r.expires_at_ms == 0 {
                                None
                            } else {
                                Some(r.expires_at_ms)
                            };
                            v
                        });
                        let _ = resp_tx.send(Ok(value));
                    }
                }
            }
            Some(EnvelopeMsg::MutationResponse(mut response)) => {
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let expected_keys = {
                    let relays = pending_mutation_relays.lock().await;
                    relays.get(&response.req_id).and_then(|relay| {
                        (relay.expected_peer == peer).then(|| relay.expected_keys.clone())
                    })
                }
                .or_else(|| {
                    let pending = pending_mutations.lock().unwrap();
                    pending.get(&response.req_id).and_then(|request| {
                        (request.expected_peer == peer).then(|| request.expected_keys.clone())
                    })
                });
                let Some(expected_keys) = expected_keys else {
                    continue;
                };
                if let Err(error) = validate_mutation_results(&expected_keys, &response.results) {
                    tracing::warn!(
                        peer = %peer,
                        req_id = response.req_id,
                        %error,
                        "ignoring malformed mutation response"
                    );
                    continue;
                }
                if let Some(timestamp_ms) = response
                    .results
                    .iter()
                    .filter(|result| {
                        MutationStatus::try_from(result.status) == Ok(MutationStatus::Superseded)
                    })
                    .map(|result| result.winner_timestamp_ms)
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "MutationResponse").await {
                        break;
                    }
                }

                let relay = {
                    let mut relays = pending_mutation_relays.lock().await;
                    if matches!(
                        relays.get(&response.req_id),
                        Some(relay) if relay.expected_peer == peer
                    ) {
                        relays.remove(&response.req_id)
                    } else {
                        None
                    }
                };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    response.req_id = relay.origin_req_id;
                    send_mutation_response(&relay.origin_tx, response).await;
                    continue;
                }

                let pending = {
                    let mut pending = pending_mutations.lock().unwrap();
                    if matches!(
                        pending.get(&response.req_id),
                        Some(request) if request.expected_peer == peer
                    ) {
                        pending.remove(&response.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(response.results);
                }
            }
            Some(EnvelopeMsg::BatchPut(bp)) => {
                let batch_priority = priority_from_proto(bp.priority);
                if let Some(timestamp_ms) = bp.puts.iter().map(|p| p.timestamp_ms).max() {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchPut").await {
                        break;
                    }
                }
                let keys: Vec<String> = bp.puts.iter().map(|p| p.key.clone()).collect();
                let Some(frag) = bp
                    .puts
                    .first()
                    .map(|p| fragment_of(&p.key, router.fragment_count))
                else {
                    send_mutation_response(
                        &tx,
                        MutationResponse {
                            req_id: bp.req_id,
                            results: Vec::new(),
                        },
                    )
                    .await;
                    continue;
                };
                if bp
                    .puts
                    .iter()
                    .any(|p| fragment_of(&p.key, router.fragment_count) != frag)
                {
                    send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        keys,
                        "batch contains keys from multiple fragments",
                    )
                    .await;
                    continue;
                }

                let is_serving = serving.read().unwrap().contains(&frag);
                if !is_serving {
                    let max_hops = bp.puts.iter().map(|p| p.hops).max().unwrap_or(0);
                    if max_hops < MAX_FORWARD_HOPS {
                        relay_batch(bp, frag, &tx, &peer_node_id, &rfctx).await;
                    } else {
                        send_unavailable_mutation(
                            &tx,
                            bp.req_id,
                            keys,
                            "forward hop limit reached",
                        )
                        .await;
                    }
                    continue;
                }

                let Some(actor) = router.route_for_fragment(frag) else {
                    send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        keys,
                        "serving fragment has no actor",
                    )
                    .await;
                    continue;
                };
                let req_id = bp.req_id;
                let mut results = Vec::with_capacity(bp.puts.len());
                for (request_index, p) in bp.puts.into_iter().enumerate() {
                    let replicated = ReplicatedOp {
                        key: p.key,
                        value: {
                            let mut v = VersionedValue::new(p.value, p.timestamp_ms, p.node_id);
                            v.expires_at_ms = (p.expires_at_ms != 0).then_some(p.expires_at_ms);
                            v.is_tombstone = p.is_tombstone;
                            v
                        },
                        seq: p.seq,
                        boot_id: p.boot_id,
                    };
                    results.push(
                        apply_forwarded_mutation(
                            &actor,
                            &senders,
                            frag,
                            replicated,
                            batch_priority,
                            request_index,
                        )
                        .await,
                    );
                }
                send_mutation_response(&tx, MutationResponse { req_id, results }).await;
            }
            Some(EnvelopeMsg::BatchGet(bg)) => {
                // Phase 6: serve a batch of reads. Phase 7: the batch
                // carries one priority applied to every get.
                let batch_priority = priority_from_proto(bg.priority);
                let mut results = Vec::with_capacity(bg.keys.len());
                for key in bg.keys {
                    let (value, shed) = match router.route(&key) {
                        Some(actor) => match actor
                            .call(
                                |reply| KvMessage::Get {
                                    key: key.clone(),
                                    reply,
                                    priority: batch_priority,
                                },
                                None,
                            )
                            .await
                        {
                            Ok(CallResult::Success(Ok(v))) => (v, false),
                            Ok(CallResult::Success(Err(_))) => (None, true),
                            _ => (None, false),
                        },
                        None => (None, false),
                    };
                    let (found, bytes, timestamp_ms, node_id, expires_at_ms, is_tombstone) =
                        match value {
                            Some(v) => (
                                true,
                                v.value,
                                v.timestamp_ms,
                                v.node_id,
                                v.expires_at_ms.unwrap_or(0),
                                v.is_tombstone,
                            ),
                            None => (false, Vec::new(), 0, String::new(), 0, false),
                        };
                    results.push(BatchKeyValue {
                        key,
                        found,
                        value: bytes,
                        timestamp_ms,
                        node_id,
                        expires_at_ms,
                        is_tombstone,
                        shed,
                    });
                }
                send_envelope(
                    &tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                            req_id: bg.req_id,
                            results,
                        })),
                    },
                )
                .await;
            }
            Some(EnvelopeMsg::BatchGetResponse(r)) => {
                if let Some(timestamp_ms) = r
                    .results
                    .iter()
                    .filter(|result| result.found)
                    .map(|result| result.timestamp_ms)
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchGetResponse").await {
                        break;
                    }
                }
                if let Some(resp_tx) = pending_batch_gets.lock().await.remove(&r.req_id) {
                    let _ = resp_tx.send(r.results);
                }
            }
            Some(EnvelopeMsg::SyncRequest(req)) => {
                // Send only the requested fragments we serve. An empty
                // request means "everything you hold".
                let wanted: Vec<FragmentId> = if req.fragments.is_empty() {
                    router.actors.read().unwrap().keys().copied().collect()
                } else {
                    req.fragments
                        .iter()
                        .map(|f| FragmentId(*f))
                        .filter(|f| router.actors.read().unwrap().contains_key(f))
                        .collect()
                };
                let mut all = HashMap::new();
                for frag in wanted {
                    if let Some(actor) = router.route_for_fragment(frag) {
                        if let Ok(CallResult::Success(map)) =
                            actor.call(|reply| KvMessage::GetAll { reply }, None).await
                        {
                            all.extend(map);
                        }
                    }
                }
                {
                    let entries = all
                        .into_iter()
                        .map(|(key, v)| SyncEntry {
                            key,
                            value: v.value,
                            timestamp_ms: v.timestamp_ms,
                            node_id: v.node_id,
                            expires_at_ms: v.expires_at_ms.unwrap_or(0),
                            is_tombstone: v.is_tombstone,
                        })
                        .collect();
                    send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::SyncResponse(SyncResponse { entries })),
                        },
                    )
                    .await;
                }
            }
            Some(EnvelopeMsg::SyncResponse(resp)) => {
                // Merge-only: sync never re-broadcasts (the gossiper
                // already has these ops; everyone else will get them
                // through normal gossip or their own sync).
                if let Some(timestamp_ms) = resp.entries.iter().map(|e| e.timestamp_ms).max() {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "SyncResponse").await {
                        break;
                    }
                }
                sync_entries_applied.fetch_add(resp.entries.len() as u64, Ordering::Relaxed);
                for e in resp.entries {
                    let op = ReplicatedOp {
                        key: e.key,
                        value: {
                            let mut v = VersionedValue::new(e.value, e.timestamp_ms, e.node_id);
                            v.expires_at_ms = if e.expires_at_ms == 0 {
                                None
                            } else {
                                Some(e.expires_at_ms)
                            };
                            v.is_tombstone = e.is_tombstone;
                            v
                        },
                        seq: 0,     // seq/boot_id are propagation concerns;
                        boot_id: 0, // sync is not gossip
                    };
                    if let Some(actor) = router.route(&op.key) {
                        let _ = actor.send_message(KvMessage::ApplyOp {
                            op,
                            priority: OpPriority::Background,
                            is_replication: true,
                            reply: None,
                        });
                    }
                }
            }
            None => {}
        }
    }

    // Connection dropped: forget the peer and stop the writer.
    if let Some(peer) = peer_node_id {
        let mut guard = peers.lock().await;
        // Only remove if it's still our entry (a newer connection may
        // have replaced it).
        if let Some(tx_existing) = guard.get(&peer) {
            if tx_existing.same_channel(&tx) {
                guard.remove(&peer);
                // Phase 5: the senders retain their buffers for resume.
                // Only notify if we were the current connection.
                let senders_guard = senders.lock().await;
                for ((_, p), sender_tx) in senders_guard.iter() {
                    if p == &peer {
                        let _ = sender_tx.send(SenderMsg::PeerDisconnected {
                            peer_tx: tx.clone(),
                        });
                    }
                }
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx {
                    let _ = mtx.send(());
                }
            }
        }
    }
    writer_task.abort();
}

/// Shared context for re-forwarding ops during migration (Phase 8).
/// A node that receives a routed op for a fragment it no longer
/// serves re-forwards it to the current owner instead of dropping it.
struct ReforwardCtx {
    node_id: String,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    peers: PeerMap,
    pending_relays: RelayMap,
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
}

/// The current owner for a re-forward: not us, and not the peer the op
/// arrived from (loop protection; `hops` is the backstop).
async fn reforward_target(
    ctx: &ReforwardCtx,
    peer_node_id: &Option<String>,
    frag: FragmentId,
) -> Option<(String, mpsc::Sender<Vec<u8>>)> {
    let replicas = ctx.placement.read().unwrap().get(&frag).cloned()?;
    let peers = ctx.peers.lock().await;
    for owner in replicas {
        if owner == ctx.node_id || peer_node_id.as_deref() == Some(owner.as_str()) {
            continue;
        }
        if let Some(tx) = peers.get(&owner) {
            return Some((owner, tx.clone()));
        }
    }
    None
}

fn unavailable_mutation_results(keys: Vec<String>, reason: &str) -> Vec<MutationResult> {
    keys.into_iter()
        .enumerate()
        .map(|(request_index, key)| {
            mutation_result(
                key,
                request_index,
                Err(NodeError::Network(reason.to_string())),
            )
        })
        .collect()
}

async fn send_unavailable_mutation(
    tx: &mpsc::Sender<Vec<u8>>,
    req_id: u64,
    keys: Vec<String>,
    reason: &str,
) {
    send_mutation_response(
        tx,
        MutationResponse {
            req_id,
            results: unavailable_mutation_results(keys, reason),
        },
    )
    .await;
}

async fn send_mutation_response(tx: &mpsc::Sender<Vec<u8>>, response: MutationResponse) -> bool {
    tokio::time::timeout(
        REMOTE_RPC_TIMEOUT,
        try_send_envelope(
            tx,
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(response)),
            },
        ),
    )
    .await
    .unwrap_or(false)
}

async fn register_mutation_relay(
    relays: MutationRelayMap,
    relay_id: u64,
    expected_peer: String,
    expected_keys: Vec<String>,
    origin_tx: mpsc::Sender<Vec<u8>>,
    origin_req_id: u64,
    timeout_after: Duration,
) {
    let (timeout_cancel, cancelled) = oneshot::channel();
    let timeout_keys = expected_keys.clone();
    relays.lock().await.insert(
        relay_id,
        MutationRelay {
            expected_peer,
            expected_keys,
            origin_tx,
            origin_req_id,
            timeout_cancel,
        },
    );

    // Register before spawning the timer. The cancellation sender is already
    // in the map, so every registered relay has exactly one live deadline.
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(timeout_after) => {
                let relay = { relays.lock().await.remove(&relay_id) };
                if let Some(relay) = relay {
                    send_unavailable_mutation(
                        &relay.origin_tx,
                        relay.origin_req_id,
                        timeout_keys,
                        "forwarded mutation relay timed out",
                    )
                    .await;
                }
            }
            _ = cancelled => {}
        }
    });
}

/// Relay a write to the fragment's current owner and preserve the exact
/// response path. A fresh local request id prevents collisions between
/// independent upstream peers.
async fn relay_put(
    mut p: ForwardedPut,
    frag: FragmentId,
    origin_tx: &mpsc::Sender<Vec<u8>>,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
) {
    let original_req_id = p.req_id;
    let keys = vec![p.key.clone()];
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        send_unavailable_mutation(
            origin_tx,
            original_req_id,
            keys,
            "no connected owner available for mutation relay",
        )
        .await;
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    register_mutation_relay(
        ctx.pending_mutation_relays.clone(),
        relay_id,
        owner,
        keys.clone(),
        origin_tx.clone(),
        original_req_id,
        REMOTE_RPC_TIMEOUT,
    )
    .await;
    p.req_id = relay_id;
    p.hops += 1;
    let sent = tokio::time::timeout(
        REMOTE_RPC_TIMEOUT,
        try_send_envelope(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(p)),
            },
        ),
    )
    .await
    .unwrap_or(false);
    if !sent {
        let relay = { ctx.pending_mutation_relays.lock().await.remove(&relay_id) };
        if let Some(relay) = relay {
            let _ = relay.timeout_cancel.send(());
            send_unavailable_mutation(
                &relay.origin_tx,
                relay.origin_req_id,
                keys,
                "mutation relay connection closed while sending",
            )
            .await;
        }
    }
}

/// Relay a whole single-fragment batch with the same response guarantees.
async fn relay_batch(
    mut bp: BatchPut,
    frag: FragmentId,
    origin_tx: &mpsc::Sender<Vec<u8>>,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
) {
    let original_req_id = bp.req_id;
    let keys: Vec<String> = bp.puts.iter().map(|put| put.key.clone()).collect();
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        send_unavailable_mutation(
            origin_tx,
            original_req_id,
            keys,
            "no connected owner available for batch relay",
        )
        .await;
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    register_mutation_relay(
        ctx.pending_mutation_relays.clone(),
        relay_id,
        owner,
        keys.clone(),
        origin_tx.clone(),
        original_req_id,
        REMOTE_RPC_TIMEOUT,
    )
    .await;
    bp.req_id = relay_id;
    for p in bp.puts.iter_mut() {
        p.hops += 1;
    }
    let sent = tokio::time::timeout(
        REMOTE_RPC_TIMEOUT,
        try_send_envelope(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(bp)),
            },
        ),
    )
    .await
    .unwrap_or(false);
    if !sent {
        let relay = { ctx.pending_mutation_relays.lock().await.remove(&relay_id) };
        if let Some(relay) = relay {
            let _ = relay.timeout_cancel.send(());
            send_unavailable_mutation(
                &relay.origin_tx,
                relay.origin_req_id,
                keys,
                "batch relay connection closed while sending",
            )
            .await;
        }
    }
}

/// Relay a read for a fragment we don't serve to the current owner,
/// remembering where to send the response. Hop-limited and never
/// bounced back to the peer it arrived from.
async fn relay_get(
    g: &ForwardedGet,
    frag: FragmentId,
    origin_tx: &mpsc::Sender<Vec<u8>>,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
) {
    let Some((_owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    ctx.pending_relays
        .lock()
        .await
        .insert(relay_id, (origin_tx.clone(), g.req_id));
    send_envelope(
        &owner_tx,
        Envelope {
            msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                key: g.key.clone(),
                req_id: relay_id,
                // Preserve the originator's priority across the relay.
                priority: g.priority,
                hops: g.hops + 1,
            })),
        },
    )
    .await;
}

/// Redial `addr` until a connection sticks, with exponential backoff.
/// Each successful dial is run to completion (i.e. until it drops),
/// then the loop redials. Runs until the task is aborted by shutdown.
async fn reconnect_loop(ctx: ConnContext, network: Arc<dyn Network>, addr: SocketAddr) {
    let mut backoff = RECONNECT_BASE;
    loop {
        tokio::time::sleep(backoff + jitter(backoff)).await;
        match network.connect(addr).await {
            Ok(stream) => {
                backoff = RECONNECT_BASE;
                handle_connection(ctx.clone(), stream, /* we_dialed = */ true).await;
                // Dropped: loop around and redial.
            }
            Err(_) => {
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
    }
}

/// Up to 50% jitter from clock nanos (no rng dependency for this),
/// so a network-wide blip doesn't make every node redial in lockstep.
fn jitter(backoff: Duration) -> Duration {
    let half_ms = backoff.as_millis() as u64 / 2;
    if half_ms == 0 {
        return Duration::ZERO;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(nanos % (half_ms + 1))
}

/// Legacy gossip: re-send an op to all peers except the connection it
/// arrived on. Only used when no fragment_map is configured; the sharded
/// path fans out from the coordinator instead.
async fn send_hello(tx: &mpsc::Sender<Vec<u8>>, node_id: &str, cluster_secret: Option<&[u8]>) {
    send_envelope(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: node_id.to_string(),
                auth_tag: cluster_secret
                    .map(|s| hello_auth_tag(s, node_id))
                    .unwrap_or_default(),
            })),
        },
    )
    .await;
}

/// HMAC-SHA256(secret, node_id): proves knowledge of the cluster
/// secret without revealing it. Compared in constant time.
fn hello_auth_tag(secret: &[u8], node_id: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(node_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time verification of a Hello's auth tag.
fn verify_hello_auth_tag(secret: &[u8], node_id: &str, tag: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    let expected = hello_auth_tag(secret, node_id);
    expected.as_slice().ct_eq(tag).into()
}

async fn send_sync_request(tx: &mpsc::Sender<Vec<u8>>, fragments: Vec<u32>) {
    send_envelope(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::SyncRequest(crate::proto::SyncRequest {
                fragments,
            })),
        },
    )
    .await;
}

async fn send_envelope(tx: &mpsc::Sender<Vec<u8>>, envelope: Envelope) {
    let _ = try_send_envelope(tx, envelope).await;
}

async fn try_send_envelope(tx: &mpsc::Sender<Vec<u8>>, envelope: Envelope) -> bool {
    let mut frame = Vec::new();
    if envelope.encode(&mut frame).is_err() {
        return false;
    }
    tx.send(frame).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(key: &str, request_index: u32) -> MutationResult {
        MutationResult {
            key: key.to_string(),
            status: MutationStatus::Applied as i32,
            error: String::new(),
            winner_timestamp_ms: 0,
            winner_node_id: String::new(),
            winner_is_tombstone: false,
            request_index,
        }
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

    #[tokio::test]
    async fn mutation_relay_timeout_removes_entry_and_replies_unavailable() {
        let relays: MutationRelayMap = Arc::new(Mutex::new(HashMap::new()));
        let (origin_tx, mut origin_rx) = mpsc::channel(1);
        register_mutation_relay(
            relays.clone(),
            17,
            "owner".to_string(),
            vec!["key".to_string()],
            origin_tx,
            9,
            Duration::from_millis(10),
        )
        .await;
        assert!(relays.lock().await.contains_key(&17));

        let bytes = tokio::time::timeout(Duration::from_secs(1), origin_rx.recv())
            .await
            .expect("relay timeout did not reply")
            .expect("relay response channel closed");
        let envelope = Envelope::decode(bytes.as_slice()).expect("invalid relay response");
        let Some(EnvelopeMsg::MutationResponse(response)) = envelope.msg else {
            panic!("expected mutation response");
        };
        assert_eq!(response.req_id, 9);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].key, "key");
        assert_eq!(
            MutationStatus::try_from(response.results[0].status),
            Ok(MutationStatus::Unavailable)
        );
        assert!(relays.lock().await.is_empty());
    }

    #[tokio::test]
    async fn completed_mutation_relay_cancels_timeout() {
        let relays: MutationRelayMap = Arc::new(Mutex::new(HashMap::new()));
        let (origin_tx, mut origin_rx) = mpsc::channel(1);
        register_mutation_relay(
            relays.clone(),
            17,
            "owner".to_string(),
            vec!["key".to_string()],
            origin_tx,
            9,
            Duration::from_millis(10),
        )
        .await;
        let relay = relays.lock().await.remove(&17).expect("relay missing");
        let _ = relay.timeout_cancel.send(());

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(origin_rx.try_recv().is_err());
        assert!(relays.lock().await.is_empty());
    }

    #[tokio::test]
    async fn stale_peer_disconnect_does_not_clear_replacement_sender_channel() {
        let router = FragmentRouter {
            actors: Arc::new(RwLock::new(HashMap::new())),
            fragment_count: 1,
        };
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
        );
        let (old_tx, mut old_rx) = mpsc::channel(4);
        let (new_tx, mut new_rx) = mpsc::channel(4);

        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_tx.clone(),
            })
            .unwrap();
        let old_session = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected old SessionStart, got {other:?}"),
        };
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: new_tx.clone(),
            })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(old_session, new_session);

        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "connected" && state.session_id == new_session
        })
        .await;

        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: new_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;
    }

    async fn common_wait_for_sender_report(
        report: &Arc<Mutex<SenderStateReport>>,
        predicate: impl Fn(&SenderStateReport) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = report.lock().await;
                if predicate(&state) {
                    return;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender state did not converge");
    }
}
