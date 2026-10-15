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
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
use ractor::{Actor, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{
    mpsc, oneshot, watch, Mutex, Notify, OwnedSemaphorePermit, RwLock as AsyncRwLock, Semaphore,
};
use tokio::time::Instant;

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
    EnvelopeMsg, ForwardedGet, ForwardedPut, GetResponse, Hello, HelloAuth, KeySummary,
    MutationResponse, MutationResult, MutationStatus, Priority as ProtoPriority, ReconcileData,
    ReconcileResponse, ReplicationAckLevel, SyncEntry, SyncResponse,
};
use crate::replication::{
    spawn_node_sender, NodeSender, NodeSenderSendError, ReplicationAckDurability, SenderConfig,
    SenderDeps, SenderMsg, SenderRuntimeOptions, SenderStateReport,
};
use crate::runtime_tasks::RuntimeTasks;
use crate::shard::{fragment_of, FragmentId};
use crate::store::{KvStore, PrefixedStore};
use crate::supervision::{FragmentHealth, FragmentSupervisor, SupervisorArgs, SupervisorMessage};

/// Maximum wire frame size (16 MiB). Anything larger is treated as a
/// corrupt stream and the connection is dropped.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Current peer wire protocol version. Hello ranges must include this value.
/// Version 2 is wire-incompatible with version 1 and also changed the public
/// generated protocol types (`Hello::challenge` and `EnvelopeMsg::HelloAuth`).
pub const WIRE_PROTOCOL_VERSION: u32 = 2;
/// The peer returns exact forwarded-mutation outcomes.
pub const CAPABILITY_MUTATION_RESULTS: u64 = 1 << 0;
/// Replication ACKs carry an explicit receiver durability fence.
pub const CAPABILITY_DURABILITY_ACKS: u64 = 1 << 1;
/// Reconciliation frames carry session/round/chunk correlation.
pub const CAPABILITY_CORRELATED_RECONCILIATION: u64 = 1 << 2;
/// The handshake uses independent fresh challenges and mutual transcript
/// proofs before publishing either side of a connection.
pub const CAPABILITY_CHALLENGE_AUTH: u64 = 1 << 3;
/// Capabilities required by this binary before it installs a peer connection.
pub const REQUIRED_WIRE_CAPABILITIES: u64 = CAPABILITY_MUTATION_RESULTS
    | CAPABILITY_DURABILITY_ACKS
    | CAPABILITY_CORRELATED_RECONCILIATION
    | CAPABILITY_CHALLENGE_AUTH;

/// Outbound queue depth per peer connection.
const PEER_CHANNEL_DEPTH: usize = 256;
/// Protocol-control messages retained per fragment/peer sender. Mutation
/// ingress has its own lane sized by `SenderConfig::buffer_capacity`.
const REPLICATION_CONTROL_CHANNEL_DEPTH: usize = 64;
/// Concurrent inbound sockets admitted to handshake/connection processing.
const INBOUND_CONNECTION_CAPACITY: usize = 1024;
/// Total budget for both flights of the peer handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Internal task-registration queue. Registration applies backpressure and is
/// acknowledged before the caller treats a task as owned.
const TASK_COMMAND_CAPACITY: usize = 64;
const HANDSHAKE_CHALLENGE_BYTES: usize = 32;

/// Reconnect backoff: first retry after this, doubling to the cap.
const RECONNECT_BASE: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const REMOTE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

/// One live connection's bounded outbound queue plus a cancellation signal
/// shared by every producer and the socket reader/writer tasks.
#[derive(Clone)]
struct PeerChannel {
    tx: mpsc::Sender<Vec<u8>>,
    cancel: watch::Sender<bool>,
    queue_admission_timeout: Duration,
    rank: Option<ConnectionRank>,
}

impl PeerChannel {
    fn same_channel(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }

    fn same_sender(&self, other: &mpsc::Sender<Vec<u8>>) -> bool {
        self.tx.same_channel(other)
    }

    fn raw_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.tx.clone()
    }

    fn cancel(&self) {
        let _ = self.cancel.send(true);
    }
}

/// Both endpoints derive the same ordering from the authenticated transcript.
/// A connection dialed by the lexicographically smaller node is preferred;
/// the transcript digest breaks ties between duplicate sockets in the same
/// direction without depending on arrival order at either endpoint.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ConnectionRank {
    preferred_direction: bool,
    transcript_id: [u8; 32],
}

/// peer node_id -> outbound frame queue
type PeerMap = Arc<Mutex<HashMap<String, PeerChannel>>>;
type PeerFailurePending = Arc<StdMutex<HashMap<String, Vec<mpsc::Sender<Vec<u8>>>>>>;

/// Synchronous signal path from replication sender callbacks to one Node-owned
/// async worker. Duplicate signals for the same physical channel coalesce, but
/// distinct stale/current channels are retained so one cannot overwrite the
/// other before the worker performs exact-channel cancellation.
#[derive(Clone, Default)]
struct PeerFailureSignals {
    pending: PeerFailurePending,
    notify: Arc<Notify>,
}

impl PeerFailureSignals {
    fn signal(&self, peer: String, failed_tx: mpsc::Sender<Vec<u8>>) {
        let mut pending = self.pending.lock().unwrap();
        let channels = pending.entry(peer).or_default();
        if !channels
            .iter()
            .any(|channel| channel.same_channel(&failed_tx))
        {
            channels.push(failed_tx);
        }
        drop(pending);
        self.notify.notify_one();
    }

    async fn run(self, peers: PeerMap) {
        loop {
            self.notify.notified().await;
            let failures = std::mem::take(&mut *self.pending.lock().unwrap());
            for (peer, failed_channels) in failures {
                let current = peers.lock().await.get(&peer).cloned();
                if let Some(current) = current {
                    if failed_channels
                        .iter()
                        .any(|failed_tx| current.same_sender(failed_tx))
                    {
                        current.cancel();
                    }
                }
            }
        }
    }
}

struct DialIntentGuard {
    address: SocketAddr,
    registry: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
    changes: watch::Sender<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DialIntentState {
    Pending,
    Active,
}

impl DialIntentGuard {
    fn reserve(
        registry: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
        changes: watch::Sender<u64>,
        address: SocketAddr,
    ) -> Option<Self> {
        let mut intents = registry.lock().unwrap();
        if intents.contains_key(&address) {
            return None;
        }
        intents.insert(address, DialIntentState::Pending);
        drop(intents);
        Some(Self {
            address,
            registry,
            changes,
        })
    }

    fn activate(&self) {
        if let Some(state) = self.registry.lock().unwrap().get_mut(&self.address) {
            *state = DialIntentState::Active;
        }
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

impl Drop for DialIntentGuard {
    fn drop(&mut self) {
        self.registry.lock().unwrap().remove(&self.address);
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}
/// Phase 5 senders, keyed by (fragment, peer node id).
type SenderMap = Arc<Mutex<HashMap<(FragmentId, String), NodeSender>>>;
/// Sender state reports, keyed by (fragment, peer node id).
type SenderReportMap = Arc<Mutex<HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>>>;

/// Join ownership for sender tasks detached from the active sender map by a
/// cancellable connection or placement task. Normal cleanup removes an entry
/// after joining it; cancellation leaves the entry here for Node shutdown.
#[derive(Default)]
struct RetiringSenderRegistry {
    senders: StdMutex<Vec<NodeSender>>,
}

impl RetiringSenderRegistry {
    fn retire(self: &Arc<Self>, sender: NodeSender) -> RetiringSender {
        {
            let mut retiring = self.senders.lock().unwrap();
            if !retiring.iter().any(|current| current.same_task(&sender)) {
                retiring.push(sender.clone());
            }
        }
        sender.request_stop();
        RetiringSender {
            sender,
            registry: self.clone(),
        }
    }

    fn remove(&self, sender: &NodeSender) {
        self.senders
            .lock()
            .unwrap()
            .retain(|current| !current.same_task(sender));
    }

    fn snapshot(&self) -> Vec<NodeSender> {
        self.senders.lock().unwrap().clone()
    }

    fn request_stop_all(&self) {
        for sender in self.senders.lock().unwrap().iter() {
            sender.request_stop();
        }
    }

    fn clear(&self) {
        self.senders.lock().unwrap().clear();
    }
}

impl Drop for RetiringSenderRegistry {
    fn drop(&mut self) {
        for sender in self.senders.get_mut().unwrap().iter() {
            sender.request_stop();
        }
    }
}

struct RetiringSender {
    sender: NodeSender,
    registry: Arc<RetiringSenderRegistry>,
}

impl RetiringSender {
    async fn join(self) {
        self.sender.stop().await;
        self.registry.remove(&self.sender);
    }
}

/// Relayed forwarded-gets keyed by their locally allocated request id.
type RelayMap = Arc<Mutex<HashMap<u64, GetRelay>>>;
/// Relayed forwarded mutations: relay req_id -> (origin peer_tx, origin req_id).
type MutationRelayMap = Arc<Mutex<HashMap<u64, MutationRelay>>>;

struct SenderEntryDeps<'a> {
    config: &'a SenderConfig,
    runtime: &'a SenderRuntimeOptions,
    router: &'a FragmentRouter,
    hlc: &'a Arc<Mutex<HlcClock>>,
    senders: &'a SenderMap,
    retiring_senders: &'a Arc<RetiringSenderRegistry>,
    reports: &'a SenderReportMap,
    peer_failures: &'a PeerFailureSignals,
}

/// Atomically install the one replication sender allowed to own a
/// `(fragment, peer)` session. Placement and connection setup can race; a
/// check-then-spawn sequence would orphan one sender after it had already
/// emitted SessionStart or accepted mutations.
async fn ensure_sender_entry(fragment: FragmentId, peer: &str, deps: SenderEntryDeps<'_>) {
    let key = (fragment, peer.to_string());
    let mut senders_guard = deps.senders.lock().await;
    // A connection or placement tick can have queued this ensure call while a
    // loss transition committed. The drainer removes the router entry before
    // releasing the sender-map lock; re-check here so the stale ensure cannot
    // recreate a sender whose snapshot source has already been detached.
    if deps.router.route_for_fragment(fragment).is_none() {
        return;
    }
    let (replacing_stopped_sender, retiring_sender) = if let Some(sender) = senders_guard.get(&key)
    {
        if !sender.is_closed() {
            return;
        }
        let retiring = deps.retiring_senders.retire(sender.clone());
        senders_guard.remove(&key);
        deps.reports.lock().await.remove(&key);
        (true, Some(retiring))
    } else {
        (false, None)
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
    let peer_failures = deps.peer_failures.clone();
    let expected_peer = peer.to_string();
    let runtime = deps
        .runtime
        .clone()
        .with_queue_failure_handler(Arc::new(move |failed_tx| {
            peer_failures.signal(expected_peer.clone(), failed_tx);
        }));
    let sender = spawn_node_sender(
        fragment,
        peer.to_string(),
        deps.config.clone(),
        SenderDeps {
            router: deps.router.clone(),
            hlc: deps.hlc.clone(),
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
            state_report: report.clone(),
        },
        runtime,
    )
    .expect("node runtime options were validated before sender spawn");
    if replacing_stopped_sender {
        // The prior task may have died after accepting an already-applied
        // mutation. Start its replacement in snapshot recovery rather than
        // assuming the lost volatile queue was empty.
        sender.schedule_reset();
    }
    deps.reports.lock().await.insert(key.clone(), report);
    senders_guard.insert(key, sender);
    drop(senders_guard);
    if let Some(retiring) = retiring_sender {
        retiring.join().await;
    }
}

struct MutationRelay {
    expected_peer: String,
    expected_keys: Vec<String>,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    timeout_cancel: oneshot::Sender<()>,
}

struct MutationRelayRegistration {
    expected_peer: String,
    expected_keys: Vec<String>,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    deadline: Instant,
}

struct GetRelay {
    origin_tx: PeerChannel,
    origin_req_id: u64,
    timeout_cancel: oneshot::Sender<()>,
}

/// Pending single-key forwarded reads: req_id -> reply channel. The
/// reply is `Ok(value)` or `Err(shed_reason)` if the remote shed the
/// read due to backpressure.
type PendingGets =
    Arc<StdMutex<HashMap<u64, oneshot::Sender<Result<Option<VersionedValue>, String>>>>>;
/// Pending batched forwarded reads: req_id -> reply channel.
type PendingBatchGets = Arc<StdMutex<HashMap<u64, oneshot::Sender<Vec<BatchKeyValue>>>>>;
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
    peer_failures: PeerFailureSignals,
    tasks: Weak<RuntimeTasks>,
    /// Monotonic wake-up signal for dial supervisors waiting behind another
    /// physical connection to the same authenticated peer.
    peer_changes: watch::Sender<u64>,
    hlc: Arc<Mutex<HlcClock>>,
    senders: SenderMap,
    retiring_senders: Arc<RetiringSenderRegistry>,
    sender_config: SenderConfig,
    sender_runtime: SenderRuntimeOptions,
    runtime_options: NodeRuntimeOptions,
    sender_reports: SenderReportMap,
    /// Full-state sync entries applied (observability for the Phase 8
    /// rolling-restart test: must stay 0 when dynamic placement owns
    /// anti-entropy).
    sync_entries_applied: Arc<AtomicU64>,
    /// Notify the placement driver on peer join/leave. `None` in
    /// static mode.
    membership_tx: Option<watch::Sender<u64>>,
    /// Pre-shared cluster secret for fresh, challenge-bound mutual proofs.
    /// This proves group-key possession, not individual identity, and does not
    /// protect post-handshake plaintext frames. `None` = no membership proof.
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
    /// A mutation holds a shared lease from its serving decision through
    /// apply and replication fan-out. Placement revocation takes the exclusive
    /// lease before removing `serving`, closing the apply/enqueue teardown race.
    mutation_gates: Arc<Vec<Arc<AsyncRwLock<()>>>>,
}

impl FragmentRouter {
    pub(crate) fn new(fragment_count: usize, actor_capacity: usize) -> Self {
        Self {
            actors: Arc::new(RwLock::new(HashMap::with_capacity(actor_capacity))),
            fragment_count,
            mutation_gates: Arc::new(
                (0..fragment_count)
                    .map(|_| Arc::new(AsyncRwLock::new(())))
                    .collect(),
            ),
        }
    }

    fn mutation_gate(&self, fragment: FragmentId) -> Arc<AsyncRwLock<()>> {
        self.mutation_gates
            .get(fragment.0 as usize)
            .expect("fragment was validated against fragment_count")
            .clone()
    }

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
    peer_failures: PeerFailureSignals,
    /// Per-node sequence number for locally originated ops.
    next_seq: Arc<Mutex<u64>>,
    /// Hybrid logical clock for LWW timestamps.
    hlc: Arc<Mutex<HlcClock>>,
    /// Phase 5 senders, keyed by (fragment, peer node id).
    senders: SenderMap,
    /// Sender tasks removed by cancellable lifecycle work but not yet joined.
    retiring_senders: Arc<RetiringSenderRegistry>,
    /// Phase 5 sender tuning.
    sender_config: SenderConfig,
    /// Transport and replication liveness policy.
    runtime_options: NodeRuntimeOptions,
    /// State reports for each sender, keyed by (fragment, peer).
    sender_reports: SenderReportMap,
    /// Full-state sync entries applied via SyncResponse (observability;
    /// stays 0 when dynamic placement owns anti-entropy).
    sync_entries_applied: Arc<AtomicU64>,
    /// Coalesced wakeup for the placement driver on membership change (Phase 8).
    membership_tx: Option<watch::Sender<u64>>,
    /// Pre-shared cluster secret for the challenge-bound Hello transcript proof.
    /// This proves fresh possession during the handshake, but plaintext TCP
    /// still has no confidentiality or per-frame integrity.
    /// `None` = no group-membership proof.
    cluster_secret: Option<Vec<u8>>,
    tasks: Arc<RuntimeTasks>,
    inbound_connections: Arc<Semaphore>,
    dial_intents: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
    dial_intent_changes: watch::Sender<u64>,
    peer_changes: watch::Sender<u64>,
    shutdown_state: Mutex<bool>,
    network: Arc<dyn Network>,
}

impl Drop for Node {
    fn drop(&mut self) {
        // Async cleanup still belongs to `shutdown`, but never leave task
        // admission open merely because a caller dropped the handle.
        self.tasks.request_shutdown();
        if let Ok(senders) = self.senders.try_lock() {
            for sender in senders.values() {
                sender.request_stop();
            }
        }
        self.retiring_senders.request_stop_all();
        self.supervisor_stops.stop_all();
    }
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

async fn run_peer_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        let frame = tokio::select! {
            changed = cancel_rx.changed() => {
                let _ = changed;
                break;
            }
            frame = rx.recv() => {
                let Some(frame) = frame else { break };
                frame
            }
        };
        let write = async {
            writer.write_u32(frame.len() as u32).await?;
            writer.write_all(&frame).await
        };
        let result = tokio::select! {
            changed = cancel_rx.changed() => {
                let _ = changed;
                break;
            }
            result = tokio::time::timeout(socket_write_timeout, write) => result,
        };
        if !matches!(result, Ok(Ok(()))) {
            break;
        }
    }
    let _ = cancel_tx.send(true);
}

/// Spawn one fragment's supervised subtree (supervisor + dispatcher)
/// and return both. Shared by startup and the Phase 8 placement
/// driver (which spawns subtrees for newly assigned fragments at
/// runtime).
async fn spawn_fragment_subtree_registered(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    hlc: &Arc<Mutex<HlcClock>>,
    registry: &Arc<SupervisorRegistry>,
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
            default_pool: config.default_pool.clone(),
            high_pool: config.high_pool.clone(),
            background_pool: config.background_pool.clone(),
            flush_batch_size: config.flush_batch_size,
            flush_interval: config.flush_interval,
            clean_interval: config.clean_interval,
            snapshot_interval: config.snapshot_interval,
            tombstone_gc_age: config.tombstone_gc_age,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        },
    )
    .await
    .map_err(|e| NodeError::Actor(e.to_string()))?;
    registry.insert(fragment, supervisor.clone());
    let mut startup_guard = SupervisorStartupGuard::new(1);
    startup_guard.push(supervisor.clone());
    let dispatcher_result = supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await;
    let dispatcher = match dispatcher_result {
        Ok(CallResult::Success(dispatcher)) => dispatcher,
        Ok(_) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove(fragment);
            startup_guard.disarm();
            return Err(NodeError::Actor(
                "supervisor dropped dispatcher reply".to_string(),
            ));
        }
        Err(error) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove(fragment);
            startup_guard.disarm();
            return Err(NodeError::Actor(error.to_string()));
        }
    };
    startup_guard.disarm();
    Ok((supervisor, dispatcher))
}

#[cfg(test)]
async fn spawn_fragment_subtree(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    hlc: &Arc<Mutex<HlcClock>>,
) -> Result<(ActorRef<SupervisorMessage>, ActorRef<KvMessage>), NodeError> {
    let registry = Arc::new(SupervisorRegistry::new(1));
    let result = spawn_fragment_subtree_registered(node_id, fragment, config, hlc, &registry).await;
    if result.is_ok() {
        registry.remove(fragment);
    }
    result
}

/// Synchronous ownership of every self-running fragment supervisor. Its Drop
/// implementation closes the late-publication race when a Node is dropped
/// while a placement task is still unwinding.
struct SupervisorRegistry {
    supervisors: StdMutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>,
}

impl SupervisorRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            supervisors: StdMutex::new(HashMap::with_capacity(capacity)),
        }
    }

    fn insert(&self, fragment: FragmentId, supervisor: ActorRef<SupervisorMessage>) {
        self.supervisors
            .lock()
            .unwrap()
            .insert(fragment, supervisor);
    }

    fn remove(&self, fragment: FragmentId) {
        self.supervisors.lock().unwrap().remove(&fragment);
    }

    fn snapshot(&self) -> Vec<ActorRef<SupervisorMessage>> {
        self.supervisors.lock().unwrap().values().cloned().collect()
    }

    fn stop_all(&self) {
        for supervisor in self.supervisors.lock().unwrap().values() {
            supervisor.stop(None);
        }
    }

    fn clear(&self) {
        self.supervisors.lock().unwrap().clear();
    }
}

impl Drop for SupervisorRegistry {
    fn drop(&mut self) {
        for supervisor in self.supervisors.get_mut().unwrap().values() {
            supervisor.stop(None);
        }
    }
}

/// Stops partially constructed fragment trees if startup fails or the startup
/// future is cancelled before ownership transfers to a returned Node.
struct SupervisorStartupGuard {
    supervisors: Vec<ActorRef<SupervisorMessage>>,
    armed: bool,
}

impl SupervisorStartupGuard {
    fn new(capacity: usize) -> Self {
        Self {
            supervisors: Vec::with_capacity(capacity),
            armed: true,
        }
    }

    fn push(&mut self, supervisor: ActorRef<SupervisorMessage>) {
        self.supervisors.push(supervisor);
    }

    async fn rollback(&mut self) {
        if !self.armed {
            return;
        }
        for supervisor in &self.supervisors {
            supervisor.stop(None);
        }
        for supervisor in self.supervisors.drain(..) {
            let _ = supervisor.wait(None).await;
        }
        self.armed = false;
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.supervisors.clear();
    }
}

impl Drop for SupervisorStartupGuard {
    fn drop(&mut self) {
        if self.armed {
            for supervisor in &self.supervisors {
                supervisor.stop(None);
            }
        }
    }
}

/// Runtime liveness and queue policy for a [`Node`].
///
/// This is separate from [`NodeConfig`] so adding transport policy does not
/// break existing exhaustive `NodeConfig` literals. [`Node::start`] uses the
/// defaults; pass an override to [`Node::start_with_options`].
#[derive(Clone, Debug)]
pub struct NodeRuntimeOptions {
    peer_queue_frame_capacity: usize,
    replication_control_message_capacity: usize,
    inbound_connection_capacity: usize,
    queue_admission_timeout: Duration,
    forwarded_attempt_timeout: Duration,
    connect_timeout: Duration,
    handshake_timeout: Duration,
    socket_read_timeout: Duration,
    socket_write_timeout: Duration,
    replication_ack_timeout: Duration,
}

impl Default for NodeRuntimeOptions {
    fn default() -> Self {
        Self {
            peer_queue_frame_capacity: PEER_CHANNEL_DEPTH,
            replication_control_message_capacity: REPLICATION_CONTROL_CHANNEL_DEPTH,
            inbound_connection_capacity: INBOUND_CONNECTION_CAPACITY,
            queue_admission_timeout: REMOTE_RPC_TIMEOUT,
            forwarded_attempt_timeout: REMOTE_RPC_TIMEOUT,
            connect_timeout: REMOTE_RPC_TIMEOUT,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            socket_read_timeout: SOCKET_WRITE_TIMEOUT,
            socket_write_timeout: SOCKET_WRITE_TIMEOUT,
            replication_ack_timeout: Duration::from_secs(30),
        }
    }
}

impl NodeRuntimeOptions {
    /// Set the number of frames buffered per peer connection.
    pub fn with_peer_queue_frame_capacity(mut self, capacity: usize) -> Self {
        self.peer_queue_frame_capacity = capacity;
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

    /// Set the maximum wait to reserve a peer queue slot.
    pub fn with_queue_admission_timeout(mut self, timeout: Duration) -> Self {
        self.queue_admission_timeout = timeout;
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

    /// Current per-peer outbound frame capacity.
    pub fn peer_queue_frame_capacity(&self) -> usize {
        self.peer_queue_frame_capacity
    }

    /// Current per-sender protocol-control message capacity.
    pub fn replication_control_message_capacity(&self) -> usize {
        self.replication_control_message_capacity
    }

    /// Current concurrent inbound-connection limit.
    pub fn inbound_connection_capacity(&self) -> usize {
        self.inbound_connection_capacity
    }

    /// Current peer-queue admission timeout.
    pub fn queue_admission_timeout(&self) -> Duration {
        self.queue_admission_timeout
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

    fn sender_options(&self) -> SenderRuntimeOptions {
        SenderRuntimeOptions::default()
            .with_ack_timeout(self.replication_ack_timeout)
            .with_queue_admission_timeout(self.queue_admission_timeout)
            .with_control_channel_capacity(self.replication_control_message_capacity)
    }

    fn validate(&self) -> Result<(), NodeError> {
        if self.peer_queue_frame_capacity == 0 {
            return Err(NodeError::Actor(
                "peer queue frame capacity must be non-zero".to_string(),
            ));
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
        if self.peer_queue_frame_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(NodeError::Actor(format!(
                "peer queue frame capacity exceeds maximum {}",
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
        Ok(())
    }
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
    /// on every node. A non-empty map must cover exactly
    /// `0..fragment_count`; every replica list must be non-empty and
    /// duplicate-free, and every replica id must contain 1 to 255 bytes.
    /// Empty (default) = this node serves all fragments (single-node / legacy
    /// full replication).
    pub fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Static membership: node_id -> address, for dialing fragment owners.
    /// Every id must contain 1 to 255 bytes and addresses must be unique.
    /// [`Node::start`] registers a persistent dial supervisor for every
    /// non-local entry, so its expected id is enforced on the first
    /// authentication attempt and every retry.
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
    /// Reserved compatibility field for the removed time-based tombstone
    /// collector. Must be `None`; [`Node::start`] rejects every `Some` value
    /// because age alone cannot prove that no replica can resurrect a deleted
    /// value.
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
    /// Pre-shared secret for mutual HMAC proofs over both peers' fresh
    /// challenges, identities, protocol ranges, capabilities, and connection
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
    /// Membership-change callbacks are coalesced current-peer-set
    /// notifications, not a lossless ordered event stream.
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
    /// `node_id` must contain 1 to 255 bytes.
    pub async fn start(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
    ) -> Result<Self, NodeError> {
        Self::start_with_options(node_id, listen_addr, config, NodeRuntimeOptions::default()).await
    }

    /// Start a node with explicit runtime liveness and outbound-queue policy.
    /// Existing callers can continue using [`Node::start`] for the defaults.
    pub async fn start_with_options(
        node_id: String,
        listen_addr: SocketAddr,
        config: NodeConfig,
        runtime_options: NodeRuntimeOptions,
    ) -> Result<Self, NodeError> {
        runtime_options.validate()?;
        if node_id.is_empty() || node_id.len() > 255 {
            return Err(NodeError::Actor(
                "node_id must contain 1 to 255 bytes".to_string(),
            ));
        }
        if let Some(invalid_member) = config
            .members
            .keys()
            .find(|member_id| member_id.is_empty() || member_id.len() > 255)
        {
            return Err(NodeError::Actor(format!(
                "member node_id {invalid_member:?} must contain 1 to 255 bytes"
            )));
        }
        let dynamic = config.placement_controller.is_some();
        if config.fragment_count == 0 {
            return Err(NodeError::Actor(
                "fragment_count must be non-zero".to_string(),
            ));
        }
        config
            .replication
            .validate()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        config
            .replication
            .validate_node_ingress()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        if dynamic
            && (config.placement_poll_interval.is_zero()
                || config.placement_drain_warn_interval.is_zero())
        {
            return Err(NodeError::Actor(
                "dynamic placement intervals must be non-zero".to_string(),
            ));
        }
        if config.tombstone_gc_age.is_some() {
            return Err(NodeError::Actor(
                "tombstone_gc_age is unsupported because time alone cannot prove replica convergence; it must be None"
                    .to_string(),
            ));
        }
        if config
            .cluster_secret
            .as_ref()
            .map_or(false, |secret| secret.len() < 32)
        {
            return Err(NodeError::Actor(
                "cluster_secret must contain at least 32 bytes when configured".to_string(),
            ));
        }
        if config.cluster_secret.is_none() {
            tracing::warn!(
                "no cluster_secret configured: anyone with TCP reachability can join the peer mesh"
            );
        } else {
            tracing::warn!(
                "cluster_secret proves fresh cluster-key possession during handshake but does \
                 not protect post-handshake plaintext frames; use a protected custom Network \
                 transport when channel confidentiality or integrity is required"
            );
        }
        let mut member_addresses: HashMap<SocketAddr, &str> = HashMap::new();
        for (member_id, member_addr) in &config.members {
            if let Some(previous_id) = member_addresses.insert(*member_addr, member_id) {
                if previous_id != member_id {
                    return Err(NodeError::Actor(format!(
                        "members {previous_id} and {member_id} share address {member_addr}"
                    )));
                }
            }
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
                validate_placement(&config.fragment_map, config.fragment_count)
                    .map_err(NodeError::Actor)?;
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

        // Bind before creating any actors so a transport startup error has no
        // fragment trees or durable-store users to roll back.
        let (bound_addr, mut acceptor) = config
            .network
            .bind(listen_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;

        // One supervised subtree per served fragment. With persistence,
        // each fragment gets its own key namespace via PrefixedStore,
        // sharing the one underlying store. The supervisor owns the
        // dispatcher, cleaner and snapshotter; the router talks to the
        // dispatcher through the supervisor.
        let hlc = Arc::new(Mutex::new(
            HlcClock::with_offset(config.clock_skew_ms)
                .with_max_skew(config.max_clock_skew.as_millis().min(u64::MAX as u128) as u64),
        ));
        let supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>> =
            Arc::new(Mutex::new(HashMap::with_capacity(my_fragments.len())));
        let supervisor_stops = Arc::new(SupervisorRegistry::new(my_fragments.len()));
        let router = FragmentRouter::new(config.fragment_count, my_fragments.len());
        let serving: Arc<RwLock<HashSet<FragmentId>>> =
            Arc::new(RwLock::new(my_fragments.iter().copied().collect()));
        let placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>> =
            Arc::new(Mutex::new(
                my_fragments
                    .iter()
                    .map(|f| (*f, FragmentPlacementState::Serving))
                    .collect(),
            ));
        let mut startup_supervisors = SupervisorStartupGuard::new(my_fragments.len());
        for fragment in my_fragments {
            let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
                &node_id,
                fragment,
                &config,
                &hlc,
                &supervisor_stops,
            )
            .await
            {
                Ok(subtree) => subtree,
                Err(error) => {
                    startup_supervisors.rollback().await;
                    return Err(error);
                }
            };
            startup_supervisors.push(supervisor.clone());
            supervisors.lock().await.insert(fragment, supervisor);
            router.insert(fragment, dispatcher);
        }

        let fragment_map: Arc<RwLock<HashMap<FragmentId, Vec<String>>>> =
            Arc::new(RwLock::new(initial_placement));
        let (membership_tx, membership_rx) = if dynamic {
            let (tx, rx) = watch::channel(0u64);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let (peer_changes, _) = watch::channel(0u64);
        let (dial_intent_changes, _) = watch::channel(0u64);
        let tasks = RuntimeTasks::new(TASK_COMMAND_CAPACITY);
        let node = Self {
            node_id: node_id.clone(),
            boot_id: fresh_boot_id(),
            listen_addr: bound_addr,
            router: router.clone(),
            serving: serving.clone(),
            supervisors: supervisors.clone(),
            supervisor_stops: supervisor_stops.clone(),
            fragment_map: fragment_map.clone(),
            placement_states: placement_states.clone(),
            placement_controller: config.placement_controller.clone(),
            placement_poll_interval: config.placement_poll_interval,
            members: config.members.clone(),
            pending_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_batch_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
            pending_relays: Arc::new(Mutex::new(HashMap::new())),
            pending_mutation_relays: Arc::new(Mutex::new(HashMap::new())),
            next_req_id: Arc::new(AtomicU64::new(1)),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            next_seq: Arc::new(Mutex::new(0)),
            hlc: hlc.clone(),
            senders: Arc::new(Mutex::new(HashMap::new())),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_config: config.replication.clone(),
            runtime_options: runtime_options.clone(),
            sender_reports: Arc::new(Mutex::new(HashMap::new())),
            sync_entries_applied: Arc::new(AtomicU64::new(0)),
            membership_tx: membership_tx.clone(),
            cluster_secret: config.cluster_secret.clone(),
            tasks,
            inbound_connections: Arc::new(Semaphore::new(
                runtime_options.inbound_connection_capacity,
            )),
            dial_intents: Arc::new(StdMutex::new(HashMap::new())),
            dial_intent_changes,
            peer_changes,
            shutdown_state: Mutex::new(false),
            network: config.network.clone(),
        };

        if !node
            .tasks
            .spawn(node.peer_failures.clone().run(node.peers.clone()))
            .await
        {
            node.shutdown().await;
            return Err(NodeError::Actor(
                "node runtime stopped during peer-failure worker startup".to_string(),
            ));
        }

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
                supervisor_stops: supervisor_stops.clone(),
                placement_states: placement_states.clone(),
                placement_generation: Arc::new(AtomicU64::new(0)),
                senders: node.senders.clone(),
                retiring_senders: node.retiring_senders.clone(),
                sender_reports: node.sender_reports.clone(),
                sender_config: node.sender_config.clone(),
                sender_runtime: node.runtime_options.sender_options(),
                peers: node.peers.clone(),
                peer_failures: node.peer_failures.clone(),
                tasks: Arc::downgrade(&node.tasks),
                hlc: hlc.clone(),
            };
            let membership_rx = membership_rx.expect("checked above");
            if !node
                .tasks
                .spawn(placement_driver(deps, membership_rx))
                .await
            {
                node.shutdown().await;
                return Err(NodeError::Actor(
                    "node runtime stopped during placement-driver startup".to_string(),
                ));
            }
        }

        // Accept loop: every inbound connection gets a handler task.
        let ctx = node.connection_context();
        let tasks = Arc::downgrade(&node.tasks);
        let inbound_connections = node.inbound_connections.clone();
        if !node
            .tasks
            .spawn(async move {
                loop {
                    match acceptor.accept().await {
                        Ok(stream) => {
                            let permit = match inbound_connections.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    tracing::warn!(
                                        "inbound connection limit reached; dropping accepted socket"
                                    );
                                    continue;
                                }
                            };
                            let connection_ctx = ctx.clone();
                            let registered = match tasks.upgrade() {
                                Some(tasks) => {
                                    tasks
                                        .spawn(async move {
                                            let _ = handle_connection(
                                                connection_ctx,
                                                stream,
                                                /* we_dialed = */ false,
                                                None,
                                                Some(Arc::new(permit)),
                                            )
                                            .await;
                                        })
                                        .await
                                }
                                None => false,
                            };
                            if !registered {
                                break;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "peer accept failed; retrying");
                            tokio::time::sleep(RECONNECT_BASE).await;
                        }
                    }
                }
            })
            .await
        {
            node.shutdown().await;
            return Err(NodeError::Actor(
                "node runtime stopped during listener startup".to_string(),
            ));
        }

        // Configured dial intents start even when the peer is initially down;
        // unlike address-only `connect`, they retain the expected identity and
        // retry with bounded backoff from the first failure.
        for (id, addr) in &node.members {
            if id != &node.node_id
                && !node
                    .start_dial_supervisor(*addr, Some(id.clone()), None)
                    .await
            {
                node.shutdown().await;
                return Err(NodeError::Actor(
                    "node runtime stopped during member-dialer startup".to_string(),
                ));
            }
        }

        startup_supervisors.disarm();
        Ok(node)
    }

    /// Dial a peer and start a supervised connection. After authentication,
    /// both sides exchange full state, so a late joiner converges immediately.
    ///
    /// The initial dial is tried once: an unreachable peer returns an error.
    /// This method returns `Ok` only after that transport dial succeeds and a
    /// Node-owned supervisor owns the stream; authentication completes
    /// asynchronously. Concurrent calls for the same address wait on the
    /// pending initial dial instead of reporting success early, then share the
    /// same active supervisor. After its first successful authentication, an
    /// address-only supervisor pins that peer id and rejects a different id on
    /// every later reconnect. Dropped connections are redialed with
    /// exponential backoff until [Node::shutdown].
    pub async fn connect(&self, peer_addr: SocketAddr) -> Result<(), NodeError> {
        if !self.tasks.is_accepting() {
            return Err(NodeError::Network("node is shutting down".to_string()));
        }
        let mut shutdown = self.tasks.shutdown_signal();
        let intent = loop {
            // Subscribe before inspecting the registry so activation/removal
            // cannot be lost between the check and the wait.
            let mut changes = self.dial_intent_changes.subscribe();
            let state = self.dial_intents.lock().unwrap().get(&peer_addr).copied();
            match state {
                Some(DialIntentState::Active) => return Ok(()),
                Some(DialIntentState::Pending) => {
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            let _ = changed;
                            return Err(NodeError::Network("node is shutting down".to_string()));
                        }
                        changed = changes.changed() => {
                            if changed.is_err() {
                                return Err(NodeError::Network("node is shutting down".to_string()));
                            }
                        }
                    }
                }
                None => {
                    if let Some(intent) = DialIntentGuard::reserve(
                        self.dial_intents.clone(),
                        self.dial_intent_changes.clone(),
                        peer_addr,
                    ) {
                        break intent;
                    }
                }
            }
        };
        if *shutdown.borrow() {
            return Err(NodeError::Network("node is shutting down".to_string()));
        }
        let stream = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return Err(NodeError::Network("node is shutting down".to_string()));
            }
            result = tokio::time::timeout(
                self.runtime_options.connect_timeout,
                self.network.connect(peer_addr),
            ) => {
                result
                    .map_err(|_| NodeError::Network("peer connection attempt timed out".to_string()))?
                    .map_err(|e| NodeError::Network(e.to_string()))?
            }
        };
        if self
            .start_reserved_dial_supervisor(intent, None, Some(stream))
            .await
        {
            Ok(())
        } else {
            Err(NodeError::Network("node is shutting down".to_string()))
        }
    }

    fn connection_context(&self) -> ConnContext {
        ConnContext {
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
            peer_failures: self.peer_failures.clone(),
            tasks: Arc::downgrade(&self.tasks),
            peer_changes: self.peer_changes.clone(),
            hlc: self.hlc.clone(),
            senders: self.senders.clone(),
            retiring_senders: self.retiring_senders.clone(),
            sender_config: self.sender_config.clone(),
            sender_runtime: self.runtime_options.sender_options(),
            runtime_options: self.runtime_options.clone(),
            sender_reports: self.sender_reports.clone(),
            sync_entries_applied: self.sync_entries_applied.clone(),
            membership_tx: self.membership_tx.clone(),
            cluster_secret: self.cluster_secret.clone(),
        }
    }

    async fn start_dial_supervisor(
        &self,
        peer_addr: SocketAddr,
        expected_peer_id: Option<String>,
        initial_stream: Option<BoxStream>,
    ) -> bool {
        let Some(intent) = DialIntentGuard::reserve(
            self.dial_intents.clone(),
            self.dial_intent_changes.clone(),
            peer_addr,
        ) else {
            return true;
        };
        self.start_reserved_dial_supervisor(intent, expected_peer_id, initial_stream)
            .await
    }

    async fn start_reserved_dial_supervisor(
        &self,
        intent: DialIntentGuard,
        expected_peer_id: Option<String>,
        initial_stream: Option<BoxStream>,
    ) -> bool {
        let peer_addr = intent.address;
        let ctx = self.connection_context();
        let network = self.network.clone();
        self.tasks
            .spawn(async move {
                let intent_guard = intent;
                intent_guard.activate();
                dial_supervisor(ctx, network, peer_addr, expected_peer_id, initial_stream).await;
                drop(intent_guard);
            })
            .await
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
                runtime: &self.runtime_options.sender_options(),
                router: &self.router,
                hlc: &self.hlc,
                senders: &self.senders,
                retiring_senders: &self.retiring_senders,
                reports: &self.sender_reports,
                peer_failures: &self.peer_failures,
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
    async fn peer_sender(&self, node_id: &str) -> Option<PeerChannel> {
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
        let mutation_lease = if self.placement_controller.is_some() {
            Some(self.router.mutation_gate(frag).read_owned().await)
        } else {
            None
        };
        let local_actor = if self.serving.read().unwrap().contains(&frag) {
            self.router.route_for_fragment(frag)
        } else {
            None
        };
        if let Some(actor) = local_actor {
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
        drop(mutation_lease);

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
        let attempt_deadline = deadline_after(self.runtime_options.forwarded_attempt_timeout);
        if !try_send_envelope_until(
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
            attempt_deadline,
        )
        .await
        {
            return Err(NodeError::Network(format!(
                "connection to fragment owner {owner} closed or timed out while sending mutation"
            )));
        }
        let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
        match response {
            Ok(Ok(mut results)) => {
                validate_mutation_results(std::slice::from_ref(&expected_key), &results)?;
                let result = results.remove(0);
                decode_mutation_result(result)
            }
            Ok(Err(_)) => Err(NodeError::Actor(
                "forwarded mutation reply dropped".to_string(),
            )),
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
        self.pending_gets.lock().unwrap().insert(req_id, resp_tx);
        let _pending_guard = PendingGetGuard {
            req_id,
            pending: self.pending_gets.clone(),
        };
        let attempt_deadline = deadline_after(self.runtime_options.forwarded_attempt_timeout);
        if !try_send_envelope_until(
            &tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key,
                    req_id,
                    priority: priority_to_proto(priority),
                    hops: 0,
                })),
            },
            attempt_deadline,
        )
        .await
        {
            return Err(NodeError::Network(format!(
                "connection to fragment owner {owner} closed or timed out while sending read"
            )));
        }
        let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
        match response {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(shed))) => Err(NodeError::Backpressure(shed)),
            Ok(Err(_)) => Err(NodeError::Actor("forwarded get dropped".to_string())),
            Err(_) => Err(NodeError::Network(format!(
                "forwarded get to {owner} timed out"
            ))),
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
    /// batches use the configured timeout (5 seconds by default) per replica
    /// attempt and fail over idempotently when a connection drops or no
    /// response arrives.
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
                let attempt_deadline =
                    deadline_after(self.runtime_options.forwarded_attempt_timeout);
                if !try_send_envelope_until(
                    &tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                            puts: puts.clone(),
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
                self.pending_batch_gets
                    .lock()
                    .unwrap()
                    .insert(req_id, resp_tx);
                let _pending_guard = PendingBatchGetGuard {
                    req_id,
                    pending: self.pending_batch_gets.clone(),
                };
                let attempt_deadline =
                    deadline_after(self.runtime_options.forwarded_attempt_timeout);
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
                    continue;
                }
                let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
                if let Ok(Ok(kvs)) = response {
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
                                results[*i] = Some(Ok(if v.is_tombstone { None } else { Some(v) }));
                            }
                            _ => results[*i] = Some(Ok(None)),
                        }
                    }
                    done = true;
                    break;
                }
                // A dropped reply or timeout falls through to the next
                // replica. The guard removes this req_id, so a late response
                // is ignored by the handler.
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

    /// Shut the node down: close task admission, abort and join all Node-owned
    /// runtime tasks, disconnect senders, and stop every fragment supervisor.
    /// Stopping the actors drops their state, which releases the RocksDB lock
    /// (when persistence is enabled) so the database can be reopened — e.g. by
    /// a restarted node on the same path. Repeated calls are idempotent.
    pub async fn shutdown(&self) {
        let mut stopped = self.shutdown_state.lock().await;
        if *stopped {
            return;
        }
        self.tasks.shutdown().await;

        // Release every request/relay waiter retained by a cancelled handler.
        self.pending_gets.lock().unwrap().clear();
        self.pending_batch_gets.lock().unwrap().clear();
        self.pending_mutations.lock().unwrap().clear();
        self.pending_relays.lock().await.clear();
        self.pending_mutation_relays.lock().await.clear();

        // A hard shutdown can cancel a handler before its normal exact-channel
        // cleanup tail. Clear every remaining current entry explicitly before
        // stopping the long-lived replication senders.
        let peers: Vec<(String, PeerChannel)> = self.peers.lock().await.drain().collect();
        for (_, channel) in &peers {
            channel.cancel();
        }
        {
            let senders = self.senders.lock().await;
            for (peer, channel) in &peers {
                for ((_, sender_peer), sender) in senders.iter() {
                    if sender_peer == peer {
                        let _ = sender.send(SenderMsg::PeerDisconnected {
                            peer_tx: channel.raw_sender(),
                        });
                    }
                }
            }
        }
        self.peer_changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });

        let senders: Vec<NodeSender> = self.senders.lock().await.values().cloned().collect();
        for sender in senders {
            sender.stop().await;
        }
        let retiring_senders = self.retiring_senders.snapshot();
        for sender in retiring_senders {
            sender.stop().await;
        }
        self.retiring_senders.clear();
        // Two-phase and retry-safe: signal every active/retiring supervisor
        // before awaiting any one of them. `wait` remains valid after a prior
        // stop or a cancelled earlier shutdown waiter.
        let supervisors = self.supervisor_stops.snapshot();
        for supervisor in &supervisors {
            supervisor.stop(None);
        }
        for supervisor in supervisors {
            let _ = supervisor.wait(None).await;
        }
        self.supervisor_stops.clear();
        *stopped = true;
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
    supervisor_stops: Arc<SupervisorRegistry>,
    placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>>,
    /// Increments only when the published placement map actually changes.
    /// A global generation is conservative: unrelated fragment movement can
    /// retry an in-flight bootstrap, but a P -> Q -> P ABA cannot publish work
    /// completed against the first P.
    placement_generation: Arc<AtomicU64>,
    senders: SenderMap,
    retiring_senders: Arc<RetiringSenderRegistry>,
    sender_reports: SenderReportMap,
    sender_config: SenderConfig,
    sender_runtime: SenderRuntimeOptions,
    peers: PeerMap,
    peer_failures: PeerFailureSignals,
    tasks: Weak<RuntimeTasks>,
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
                runtime: &self.sender_runtime,
                router: &self.router,
                hlc: &self.hlc,
                senders: &self.senders,
                retiring_senders: &self.retiring_senders,
                reports: &self.sender_reports,
                peer_failures: &self.peer_failures,
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
async fn placement_driver(deps: PlacementDriverDeps, mut membership_rx: watch::Receiver<u64>) {
    // Converge immediately at startup; then on interval / membership.
    placement_tick(&deps).await;
    let mut interval = tokio::time::interval(deps.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                placement_tick(&deps).await;
            }
            changed = membership_rx.changed() => {
                if changed.is_err() {
                    break; // node shutting down
                }
                let members: Vec<String> =
                    deps.peers.lock().await.keys().cloned().collect();
                deps.controller.on_membership_change(&members);
                placement_tick(&deps).await;
            }
        }
    }
}

/// One driver tick: publish the controller's map as the authoritative
/// routing table, then gain/lose fragments to match.
async fn placement_tick(deps: &PlacementDriverDeps) {
    let desired = deps.controller.desired_placement();
    if let Err(e) = validate_placement(&desired, deps.fragment_count) {
        eprintln!("[placement] invalid desired placement ({e}); keeping current");
        return;
    }
    let mine_desired: HashSet<FragmentId> = desired
        .iter()
        .filter(|(_, r)| r.iter().any(|id| id == &deps.node_id))
        .map(|(f, _)| *f)
        .collect();

    // `placement_states` is also the transition commit mutex. Publishing a
    // controller map and marking its gain/loss transitions in one critical
    // section prevents a stale drainer from validating the old map and then
    // tearing down state after a re-add has already been published.
    let (mine_current, in_flight, transitions): (
        HashSet<FragmentId>,
        HashSet<FragmentId>,
        Vec<(FragmentId, bool)>,
    ) = {
        let mut states = deps.placement_states.lock().await;
        // Canonical lock order is placement_states -> placement. The sync map
        // guard never crosses an await.
        let mut published = deps.placement.write().unwrap();
        if *published != desired {
            *published = desired.clone();
            deps.placement_generation.fetch_add(1, Ordering::AcqRel);
        }
        drop(published);
        let mut current = HashSet::new();
        let mut flight = HashSet::new();
        let mut transitions = Vec::new();
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

        // Mark transitions before spawning so the next tick cannot create a
        // duplicate while the child task is waiting to be scheduled.
        for f in mine_desired.difference(&current) {
            states.insert(*f, FragmentPlacementState::Bootstrapping);
            transitions.push((*f, true));
        }
        for f in current.difference(&mine_desired) {
            if flight.contains(f) {
                continue;
            }
            states.insert(*f, FragmentPlacementState::Draining);
            transitions.push((*f, false));
        }
        (current, flight, transitions)
    };

    // Register outside the placement-state commit mutex. The Node task manager
    // owns and joins transitions directly, so aborting the polling driver
    // cannot detach a gain/loss child during shutdown.
    for (fragment, gaining) in transitions {
        let transition_deps = deps.clone();
        let transition = async move {
            if gaining {
                gain_fragment(&transition_deps, fragment).await;
            } else {
                lose_fragment(&transition_deps, fragment).await;
            }
        };
        let registered = match deps.tasks.upgrade() {
            Some(tasks) => tasks.spawn(transition).await,
            None => false,
        };
        if !registered {
            tracing::debug!(
                fragment = fragment.0,
                "node stopped before placement transition registered"
            );
        }
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
                    peer_tx: tx.raw_sender(),
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
        let mut retiring = Vec::with_capacity(stale.len());
        for key in stale {
            if let Some(sender) = senders.get(&key).cloned() {
                retiring.push(deps.retiring_senders.retire(sender));
                senders.remove(&key);
            }
            reports.remove(&key);
        }
        drop(reports);
        drop(senders);
        for sender in retiring {
            sender.join().await;
        }
    }
}

/// Bootstrap timeout: bound the wait for a source peer / reconcile
/// round. On timeout the fragment still goes Serving (availability
/// over consistency); LWW + periodic reconcile converge it later.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FragmentGainCommit {
    Serving,
    Retry,
    Revoked,
}

/// Commit a completed gain only if the exact replica set which was reconciled
/// is still current. `placement_states` is the publication mutex used by
/// placement ticks, so this check and the Serving transition are atomic with
/// both revocation and replica-set replacement.
async fn commit_fragment_gain(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    expected_replicas: &[String],
    expected_generation: u64,
) -> FragmentGainCommit {
    let mut states = deps.placement_states.lock().await;
    let current = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .cloned()
        .unwrap_or_default();
    if !current.iter().any(|node| node == &deps.node_id) {
        states.insert(fragment, FragmentPlacementState::Draining);
        return FragmentGainCommit::Revoked;
    }
    if current != expected_replicas
        || deps.placement_generation.load(Ordering::Acquire) != expected_generation
    {
        states.insert(fragment, FragmentPlacementState::Bootstrapping);
        return FragmentGainCommit::Retry;
    }

    deps.serving.write().unwrap().insert(fragment);
    states.insert(fragment, FragmentPlacementState::Serving);
    FragmentGainCommit::Serving
}

/// Align replication senders and reconcile against one placement snapshot.
/// If placement changes while reconciliation is in flight, repeat against the
/// new replica set before publishing the fragment as authoritative.
async fn converge_fragment_gain(deps: &PlacementDriverDeps, fragment: FragmentId) -> bool {
    loop {
        let (desired, replicas, generation) = {
            let mut states = deps.placement_states.lock().await;
            let desired = deps.placement.read().unwrap().clone();
            let generation = deps.placement_generation.load(Ordering::Acquire);
            let replicas = desired.get(&fragment).cloned().unwrap_or_default();
            if !replicas.iter().any(|node| node == &deps.node_id) {
                states.insert(fragment, FragmentPlacementState::Draining);
                return false;
            }
            states.insert(fragment, FragmentPlacementState::Bootstrapping);
            (desired, replicas, generation)
        };

        ensure_fragment_senders(deps, fragment, &desired).await;
        let sources: Vec<String> = replicas
            .iter()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        if !sources.is_empty() && !bootstrap_from(deps, fragment, &sources).await {
            tracing::warn!(
                fragment = fragment.0,
                "fragment bootstrap timed out; serving retained state if placement is unchanged"
            );
        }

        match commit_fragment_gain(deps, fragment, &replicas, generation).await {
            FragmentGainCommit::Serving => return true,
            FragmentGainCommit::Retry => continue,
            FragmentGainCommit::Revoked => return false,
        }
    }
}

/// Gain a fragment: spawn the subtree, bootstrap state from a replica
/// holder via the Phase 5 reconciler (chunked, bounded), then go
/// Serving. The fragment is NOT authoritative until bootstrap
/// completes (or times out): local reads/writes forward to the
/// previous owner meanwhile.
async fn gain_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
        &deps.node_id,
        fragment,
        &deps.subtree_config,
        &deps.hlc,
        &deps.supervisor_stops,
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
    let mut startup_supervisor = SupervisorStartupGuard::new(1);
    startup_supervisor.push(supervisor.clone());
    deps.supervisors.lock().await.insert(fragment, supervisor);
    startup_supervisor.disarm();
    // The actor exists in the router now (so the reconciler can merge
    // into it), but the fragment is not serving yet.
    deps.router.insert(fragment, dispatcher);

    if converge_fragment_gain(deps, fragment).await {
        deps.controller.on_fragment_ready(fragment);
    } else {
        lose_fragment(deps, fragment).await;
    }
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

async fn fragment_sender_set_is_drained(
    fragment: FragmentId,
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> bool {
    for (key @ (sender_fragment, _), sender) in senders {
        if *sender_fragment != fragment {
            continue;
        }
        if sender.is_closed() || !sender.ingress_is_idle() {
            return false;
        }
        let Some(report) = reports.get(key) else {
            return false;
        };
        let report = report.lock().await;
        if report.buffered != 0 || report.in_flight != 0 || report.state == "buffer_reset" {
            return false;
        }
    }
    // Treat an orphan report as non-drained. It indicates that a concurrent
    // lifecycle transition has not committed its exact sender/report set yet.
    reports
        .keys()
        .filter(|(report_fragment, _)| *report_fragment == fragment)
        .all(|key| senders.contains_key(key))
}

async fn restore_fragment_if_readded(deps: &PlacementDriverDeps, fragment: FragmentId) -> bool {
    // Remain non-authoritative while the sender set is realigned and the
    // retained subtree catches up on writes forwarded during Draining.
    let restored = converge_fragment_gain(deps, fragment).await;
    if restored {
        deps.controller.on_fragment_ready(fragment);
    }
    restored
}

/// Lose a fragment: stop serving new local ops (they forward to the
/// new owner), drain replication senders, then stop the subtree.
async fn lose_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    // First wait for every mutation which already chose this owner to finish
    // both its local apply and sender fan-out. New mutations queue behind this
    // exclusive lease, observe `serving` removed, and forward to the new owner.
    let mutation_gate = deps.router.mutation_gate(fragment);
    let mutation_revocation = mutation_gate.write_owned().await;
    deps.serving.write().unwrap().remove(&fragment);
    drop(mutation_revocation);
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
                        peer_tx: peer_tx.raw_sender(),
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
        if restore_fragment_if_readded(deps, fragment).await {
            // The controller reversed the move while it was draining. Restore
            // local service instead of letting the stale transition remove a
            // newly authoritative fragment.
            return;
        }

        let drained = {
            let senders = deps.senders.lock().await;
            let reports = deps.sender_reports.lock().await;
            fragment_sender_set_is_drained(fragment, &senders, &reports).await
        };
        if drained {
            // Final teardown commit. `placement_states` serializes this
            // validation with placement publication. Keeping the sender map
            // locked through router removal also forces already-queued ensure
            // calls to re-check the now-absent snapshot source.
            let mut states = deps.placement_states.lock().await;
            let still_revoked = deps
                .placement
                .read()
                .unwrap()
                .get(&fragment)
                .map_or(true, |replicas| {
                    !replicas.iter().any(|node| node == &deps.node_id)
                });
            if !still_revoked {
                // A re-add raced the final drain check. Keep the subtree
                // non-serving and let the top of the loop realign senders and
                // reconcile before publishing it again.
                states.insert(fragment, FragmentPlacementState::Bootstrapping);
                drop(states);
                continue;
            }

            // Fence every dynamic mutation path before the destructive commit.
            // New readers wait until the router entry is gone and then fail or
            // forward without touching the retired actor.
            let mutation_commit = deps.router.mutation_gate(fragment).write_owned().await;
            let mut senders = deps.senders.lock().await;
            let mut reports = deps.sender_reports.lock().await;
            if !fragment_sender_set_is_drained(fragment, &senders, &reports).await {
                drop(reports);
                drop(senders);
                drop(mutation_commit);
                drop(states);
                continue;
            }

            let keys: Vec<(FragmentId, String)> = senders
                .keys()
                .filter(|(sender_fragment, _)| *sender_fragment == fragment)
                .cloned()
                .collect();
            let mut retiring_senders = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(sender) = senders.get(&key).cloned() {
                    retiring_senders.push(deps.retiring_senders.retire(sender));
                    senders.remove(&key);
                }
                reports.remove(&key);
            }

            // Detach every admission/recreation path before the first teardown
            // wait. A queued ensure call cannot acquire `senders` until after
            // the router is absent, and an inbound mutation cannot acquire its
            // shared lease until after the same point.
            let supervisor = deps.supervisors.lock().await.remove(&fragment);
            if let Some(supervisor) = &supervisor {
                // The synchronous registry retains join ownership if this
                // transition is cancelled after the stop signal.
                supervisor.stop(None);
            }
            deps.router.remove(fragment);
            drop(reports);
            drop(senders);
            drop(mutation_commit);
            drop(states);

            for sender in retiring_senders {
                sender.join().await;
            }
            if let Some(supervisor) = supervisor {
                let _ = supervisor.wait(None).await;
                deps.supervisor_stops.remove(fragment);
            }
            deps.placement_states
                .lock()
                .await
                .insert(fragment, FragmentPlacementState::Stopped);
            return;
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
}

/// Drive one peer connection: handshake, then a read loop.
///
/// `we_dialed` distinguishes the dialer (sends `Hello` immediately) from
/// the listener (replies to the dialer's `Hello`).
///
/// The writer is registered as a sibling in the Node task manager. Cancelling
/// the handler therefore cannot detach the socket's write half from shutdown.
#[derive(Debug)]
enum HandshakeState {
    AwaitingHello,
    AwaitingAuth { peer: Hello },
    Established { peer: String },
}

impl HandshakeState {
    fn is_established(&self) -> bool {
        matches!(self, Self::Established { .. })
    }

    fn established_peer_id(&self) -> Option<&str> {
        match self {
            Self::Established { peer } => Some(peer),
            Self::AwaitingHello | Self::AwaitingAuth { .. } => None,
        }
    }
}

#[derive(Debug)]
struct ConnectionOutcome {
    peer_id: Option<String>,
    was_established: bool,
}

async fn read_connection_frame<R>(
    reader: &mut R,
    cancel: &mut watch::Receiver<bool>,
    established: bool,
    handshake_deadline: Instant,
    frame_body_timeout: Duration,
) -> Option<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let prefix = if established {
        tokio::select! {
            changed = cancel.changed() => {
                let _ = changed;
                return None;
            }
            result = reader.read_u32() => result.ok()?,
        }
    } else {
        tokio::select! {
            changed = cancel.changed() => {
                let _ = changed;
                return None;
            }
            _ = tokio::time::sleep_until(handshake_deadline) => return None,
            result = reader.read_u32() => result.ok()?,
        }
    };
    let len = prefix as usize;
    let max_len = if established {
        MAX_FRAME_BYTES
    } else {
        4 * 1024
    };
    if len > max_len {
        return None;
    }

    let mut frame = vec![0u8; len];
    let body_deadline = if established {
        deadline_after(frame_body_timeout)
    } else {
        handshake_deadline
    };
    tokio::select! {
        changed = cancel.changed() => {
            let _ = changed;
            None
        }
        _ = tokio::time::sleep_until(body_deadline) => None,
        result = reader.read_exact(&mut frame) => result.ok().map(|_| frame),
    }
}

fn validate_peer_hello(
    local_node_id: &str,
    expected_peer_id: Option<&str>,
    hello: &Hello,
) -> Result<(), &'static str> {
    if hello.node_id.is_empty() || hello.node_id.len() > 255 {
        return Err("peer node_id is empty or exceeds 255 bytes");
    }
    if hello.node_id == local_node_id {
        return Err("peer claimed the local node_id");
    }
    if expected_peer_id.map_or(false, |expected| expected != hello.node_id) {
        return Err("peer identity did not match the configured member");
    }
    if !hello.auth_tag.is_empty() {
        return Err("peer used the retired replayable Hello auth_tag");
    }
    if hello.challenge.len() != HANDSHAKE_CHALLENGE_BYTES
        || hello.challenge.iter().all(|byte| *byte == 0)
    {
        return Err("peer challenge is missing, invalid, or all-zero");
    }
    if hello.min_protocol_version == 0
        || hello.min_protocol_version > WIRE_PROTOCOL_VERSION
        || hello.max_protocol_version < WIRE_PROTOCOL_VERSION
    {
        return Err("peer wire-version range is incompatible");
    }
    if REQUIRED_WIRE_CAPABILITIES & !hello.capabilities != 0 {
        return Err("peer omitted required wire capabilities");
    }
    Ok(())
}

async fn handle_connection(
    ctx: ConnContext,
    stream: BoxStream,
    we_dialed: bool,
    expected_peer_id: Option<String>,
    connection_lease: Option<Arc<OwnedSemaphorePermit>>,
) -> ConnectionOutcome {
    let handshake_deadline = deadline_after(ctx.runtime_options.handshake_timeout);
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
        peer_failures,
        tasks,
        peer_changes,
        hlc,
        senders,
        retiring_senders,
        sender_config,
        sender_runtime,
        runtime_options,
        sender_reports,
        sync_entries_applied,
        membership_tx,
        cluster_secret,
    } = ctx;
    let local_hello = match fresh_hello(&node_id) {
        Ok(hello) => hello,
        Err(error) => {
            tracing::error!(%error, "failed to generate a handshake challenge");
            return ConnectionOutcome {
                peer_id: None,
                was_established: false,
            };
        }
    };
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
    let (raw_tx, rx) = mpsc::channel::<Vec<u8>>(runtime_options.peer_queue_frame_capacity);
    let (cancel, _) = watch::channel(false);
    let mut tx = PeerChannel {
        tx: raw_tx,
        cancel: cancel.clone(),
        queue_admission_timeout: runtime_options.queue_admission_timeout,
        rank: None,
    };
    let writer_cancel = cancel.subscribe();
    // Subscribe before the writer or handshake can signal cancellation;
    // subscribing after an early failure would mark that value as already
    // observed and leave the read side waiting forever.
    let mut read_cancel = cancel.subscribe();
    let writer_cancel_tx = cancel.clone();
    let socket_write_timeout = runtime_options.socket_write_timeout;

    // The writer is independently registered with the Node task manager. The
    // context carries only a Weak reference, avoiding a manager→handler→manager
    // ownership cycle while still making shutdown join both halves.
    let writer_registered = match tasks.upgrade() {
        Some(tasks) => {
            let writer_lease = connection_lease.clone();
            tokio::time::timeout_at(
                handshake_deadline,
                tasks.spawn(async move {
                    let _writer_lease = writer_lease;
                    run_peer_writer(
                        writer,
                        rx,
                        writer_cancel,
                        writer_cancel_tx,
                        socket_write_timeout,
                    )
                    .await;
                }),
            )
            .await
            .unwrap_or(false)
        }
        None => false,
    };
    if !writer_registered {
        return ConnectionOutcome {
            peer_id: None,
            was_established: false,
        };
    }

    // Both endpoints contribute a fresh challenge before either can prove the
    // ordered transcript. Every required handshake enqueue is checked.
    if !send_hello_until(&tx, local_hello.clone(), handshake_deadline).await {
        tx.cancel();
        return ConnectionOutcome {
            peer_id: None,
            was_established: false,
        };
    }

    // Phase 8: shared re-forward context (migration safety).
    let rfctx = ReforwardCtx {
        node_id: node_id.clone(),
        placement: placement.clone(),
        peers: peers.clone(),
        tasks: tasks.clone(),
        pending_relays: pending_relays.clone(),
        pending_mutation_relays: pending_mutation_relays.clone(),
        next_req_id: next_req_id.clone(),
        forwarded_attempt_timeout: runtime_options.forwarded_attempt_timeout,
    };

    let mut handshake = HandshakeState::AwaitingHello;
    let mut authenticated_peer_id = None;
    let mut was_established = false;
    // Length-delimited framing: 4-byte big-endian length prefix.
    loop {
        let Some(buf) = read_connection_frame(
            &mut reader,
            &mut read_cancel,
            handshake.is_established(),
            handshake_deadline,
            runtime_options.socket_read_timeout,
        )
        .await
        else {
            break;
        };
        let envelope = match Envelope::decode(&buf[..]) {
            Ok(e) => e,
            Err(_) => break,
        };
        let peer_node_id = handshake.established_peer_id().map(str::to_owned);

        // A small, absolute-deadline handshake accepts only its two flights.
        // Unexpected application traffic fails closed instead of allowing an
        // unauthenticated peer to consume decode/loop work until timeout.
        if !handshake.is_established()
            && !matches!(
                envelope.msg,
                Some(EnvelopeMsg::Hello(_)) | Some(EnvelopeMsg::HelloAuth(_))
            )
        {
            tracing::warn!("rejecting application frame before handshake completion");
            break;
        }

        match envelope.msg {
            Some(EnvelopeMsg::Hello(peer_hello)) => {
                if !matches!(handshake, HandshakeState::AwaitingHello) {
                    tracing::warn!("rejecting duplicate or post-handshake Hello");
                    break;
                }
                if let Err(reason) =
                    validate_peer_hello(&node_id, expected_peer_id.as_deref(), &peer_hello)
                {
                    tracing::warn!(claimed_node_id = %peer_hello.node_id, %reason, "rejecting Hello");
                    break;
                }
                let (dialer, listener) = if we_dialed {
                    (&local_hello, &peer_hello)
                } else {
                    (&peer_hello, &local_hello)
                };
                let auth_tag = cluster_secret
                    .as_deref()
                    .map(|secret| handshake_auth_tag(secret, we_dialed, dialer, listener))
                    .unwrap_or_default();
                if !send_hello_auth_until(&tx, auth_tag, handshake_deadline).await {
                    break;
                }
                handshake = HandshakeState::AwaitingAuth { peer: peer_hello };
            }
            Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) => {
                let peer_hello = match &handshake {
                    HandshakeState::AwaitingAuth { peer } => peer.clone(),
                    HandshakeState::AwaitingHello | HandshakeState::Established { .. } => {
                        tracing::warn!("rejecting out-of-order or duplicate HelloAuth");
                        break;
                    }
                };
                let (dialer, listener) = if we_dialed {
                    (&local_hello, &peer_hello)
                } else {
                    (&peer_hello, &local_hello)
                };
                if !verify_handshake_auth(
                    cluster_secret.as_deref(),
                    !we_dialed,
                    dialer,
                    listener,
                    &auth_tag,
                ) {
                    tracing::warn!(
                        claimed_node_id = %peer_hello.node_id,
                        "challenge-bound handshake proof failed"
                    );
                    break;
                }

                let peer = peer_hello.node_id.clone();
                authenticated_peer_id = Some(peer.clone());
                tx.rank = Some(connection_rank(dialer, listener));
                let (won_election, previous) = {
                    let mut peer_guard = peers.lock().await;
                    match peer_guard.get(&peer) {
                        Some(current) if current.same_channel(&tx) => (true, None),
                        Some(current) if current.rank.as_ref() >= tx.rank.as_ref() => (false, None),
                        Some(_) => (true, peer_guard.insert(peer.clone(), tx.clone())),
                        None => {
                            peer_guard.insert(peer.clone(), tx.clone());
                            (true, None)
                        }
                    }
                };
                if !won_election {
                    tracing::debug!(%peer, "authenticated duplicate connection lost election");
                    break;
                }
                if let Some(previous) = previous {
                    previous.cancel();
                }
                peer_changes.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
                handshake = HandshakeState::Established { peer: peer.clone() };
                was_established = true;

                // Both sides then pull the fragments they serve
                // for anti-entropy — unless dynamic placement is on, in
                // which case the Phase 5 reconciler owns anti-entropy
                // (delta-only; no full-state copy).
                if !dynamic_placement {
                    send_sync_request(&tx, router.fragment_ids()).await;
                }
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx.clone() {
                    mtx.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
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
                    // Dynamic sender creation is placement-owned. Letting a
                    // connection handler create one from an earlier placement
                    // snapshot can reinsert a revoked peer after a gain has
                    // aligned and pruned its exact sender set. The placement
                    // path also reconnects newly-created senders to live peers;
                    // this handler only notifies senders that already exist.
                    if is_replica && !dynamic_placement {
                        ensure_sender_entry(
                            frag,
                            &peer,
                            SenderEntryDeps {
                                config: &sender_config,
                                runtime: &sender_runtime,
                                router: &router,
                                hlc: &hlc,
                                senders: &senders,
                                retiring_senders: &retiring_senders,
                                reports: &sender_reports,
                                peer_failures: &peer_failures,
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
                        break;
                    }
                    let senders_guard = senders.lock().await;
                    for ((_, sender_peer), sender_tx) in senders_guard.iter() {
                        if sender_peer == &peer {
                            let _ = sender_tx.send(SenderMsg::PeerConnected {
                                peer_tx: tx.raw_sender(),
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
                let fragment = FragmentId(s.fragment);
                // Placement controllers are observed independently. A source
                // can therefore publish the overlap and start this session
                // just before the destination has attached its actor. Adopt
                // the bounded transport session now; batches arriving before
                // actor installation receive a NACK below and are retried.
                // Requiring an actor here would silently lose the only
                // SessionStart and stall the sender until its ACK timeout.
                if fragment.0 as usize >= router.fragment_count {
                    tracing::warn!(
                        fragment = s.fragment,
                        "ignoring SessionStart for an invalid fragment"
                    );
                    continue;
                }
                if let Some(peer) = peer_node_id.clone() {
                    let key = (peer, fragment);
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
                // Dynamic placement teardown takes the exclusive side of this
                // lease before detaching the actor. Hold the shared side
                // through apply, durability fencing, and ACK enqueue so no
                // mutation can be acknowledged during destructive teardown.
                let _mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
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
                        if tx
                            .send(SenderMsg::Ack {
                                session_id: a.session_id,
                                batch_seq: a.batch_seq,
                                durability,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            Some(EnvelopeMsg::BatchNack(n)) => {
                if let Some(peer) = peer_node_id.clone() {
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(n.fragment), peer)) {
                        if tx
                            .send(SenderMsg::Nack {
                                session_id: n.session_id,
                                batch_seq: n.batch_seq,
                                error: n.error,
                            })
                            .is_err()
                        {
                            break;
                        }
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
                    let sender_key = (FragmentId(r.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileResponse {
                            session_id: r.session_id,
                            round_id: r.round_id,
                            summaries: r.summaries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            // Placement may retire the sender after the map
                            // lookup above. That is a stale local consumer,
                            // not a protocol failure on this multiplexed peer
                            // connection.
                            Err(NodeSenderSendError::Closed) => continue,
                            Err(NodeSenderSendError::Full) => break,
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => break,
                            // The sender can be retired after accepting the
                            // message but before answering its oneshot.
                            Ok(Err(_)) => continue,
                            Err(_) => {
                                // A live sender that stops validating control
                                // traffic is unhealthy. A removed/replaced
                                // sender merely lost a placement race.
                                let still_current = senders
                                    .lock()
                                    .await
                                    .get(&sender_key)
                                    .map_or(false, |current| current.same_task(&tx));
                                if still_current {
                                    break;
                                }
                                continue;
                            }
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
                    let sender_key = (FragmentId(d.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileData {
                            session_id: d.session_id,
                            round_id: d.round_id,
                            chunk_seq: d.chunk_seq,
                            entries: d.entries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            Err(NodeSenderSendError::Closed) => continue,
                            Err(NodeSenderSendError::Full) => break,
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => break,
                            Ok(Err(_)) => continue,
                            Err(_) => {
                                let still_current = senders
                                    .lock()
                                    .await
                                    .get(&sender_key)
                                    .map_or(false, |current| current.same_task(&tx));
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedPut(p)) => {
                if !observe_remote_timestamp(&hlc, p.timestamp_ms, "ForwardedPut").await {
                    break;
                }
                let frag = fragment_of(&p.key, router.fragment_count);
                let mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
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
                    drop(mutation_lease);
                    send_mutation_response(
                        &tx,
                        MutationResponse {
                            req_id: p.req_id,
                            results: vec![result],
                        },
                    )
                    .await;
                } else if p.hops < MAX_FORWARD_HOPS {
                    drop(mutation_lease);
                    relay_put(p, frag, &tx, &peer_node_id, &rfctx).await;
                } else {
                    drop(mutation_lease);
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
                let relay = { pending_relays.lock().await.remove(&r.req_id) };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    send_envelope(
                        &relay.origin_tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                                req_id: relay.origin_req_id,
                                ..r
                            })),
                        },
                    )
                    .await;
                    continue;
                }
                if let Some(resp_tx) = pending_gets.lock().unwrap().remove(&r.req_id) {
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

                let mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
                let is_serving = serving.read().unwrap().contains(&frag);
                if !is_serving {
                    drop(mutation_lease);
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
                    drop(mutation_lease);
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
                drop(mutation_lease);
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
                if let Some(resp_tx) = pending_batch_gets.lock().unwrap().remove(&r.req_id) {
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
                let mut sync_failed = false;
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
                        match call_apply(&actor, op, OpPriority::Background, true).await {
                            Ok(_) => {
                                sync_entries_applied.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "full-state sync apply failed; closing connection for retry"
                                );
                                sync_failed = true;
                                break;
                            }
                        }
                    }
                }
                if sync_failed {
                    break;
                }
            }
            None => {}
        }
    }

    // Connection dropped: forget the peer only if this exact physical channel
    // is still current. Legacy full-replication peers are discovered at
    // runtime, so reap their per-fragment senders on disconnect rather than
    // letting sequential claimed identities grow the sender maps forever.
    // The next handshake's mandatory full-state exchange covers writes made
    // while that peer is absent. Explicit placement maps retain sender buffers
    // because their peer cardinality is configured and bounded.
    let mut disconnected_senders = Vec::new();
    if let Some(peer) = handshake.established_peer_id().map(str::to_owned) {
        let mut guard = peers.lock().await;
        // Only remove if it's still our entry (a newer connection may
        // have replaced it).
        if let Some(tx_existing) = guard.get(&peer) {
            if tx_existing.same_channel(&tx) {
                guard.remove(&peer);
                let legacy_full_replication = placement.read().unwrap().is_empty();
                let mut senders_guard = senders.lock().await;
                if legacy_full_replication {
                    let keys: Vec<(FragmentId, String)> = senders_guard
                        .keys()
                        .filter(|(_, sender_peer)| sender_peer == &peer)
                        .cloned()
                        .collect();
                    let mut reports_guard = sender_reports.lock().await;
                    for key in keys {
                        if let Some(sender) = senders_guard.get(&key).cloned() {
                            disconnected_senders.push(retiring_senders.retire(sender));
                            senders_guard.remove(&key);
                        }
                        reports_guard.remove(&key);
                    }
                } else {
                    // Explicitly placed senders retain their buffers for
                    // resume. Only notify if this was the current channel.
                    for ((_, sender_peer), sender_tx) in senders_guard.iter() {
                        if sender_peer == &peer {
                            let _ = sender_tx.send(SenderMsg::PeerDisconnected {
                                peer_tx: tx.raw_sender(),
                            });
                        }
                    }
                }
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx {
                    mtx.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
                }
                peer_changes.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
            }
        }
    }
    tx.cancel();
    for sender in disconnected_senders {
        sender.join().await;
    }
    ConnectionOutcome {
        peer_id: authenticated_peer_id,
        was_established,
    }
}

/// Shared context for re-forwarding ops during migration (Phase 8).
/// A node that receives a routed op for a fragment it no longer
/// serves re-forwards it to the current owner instead of dropping it.
struct ReforwardCtx {
    node_id: String,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    peers: PeerMap,
    tasks: Weak<RuntimeTasks>,
    pending_relays: RelayMap,
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
    forwarded_attempt_timeout: Duration,
}

/// The current owner for a re-forward: not us, and not the peer the op
/// arrived from (loop protection; `hops` is the backstop).
async fn reforward_target(
    ctx: &ReforwardCtx,
    peer_node_id: &Option<String>,
    frag: FragmentId,
) -> Option<(String, PeerChannel)> {
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

async fn send_unavailable_mutation(tx: &PeerChannel, req_id: u64, keys: Vec<String>, reason: &str) {
    send_mutation_response(
        tx,
        MutationResponse {
            req_id,
            results: unavailable_mutation_results(keys, reason),
        },
    )
    .await;
}

async fn send_mutation_response(tx: &PeerChannel, response: MutationResponse) -> bool {
    try_send_envelope(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(response)),
        },
    )
    .await
}

async fn register_mutation_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: MutationRelayMap,
    relay_id: u64,
    registration: MutationRelayRegistration,
) {
    let MutationRelayRegistration {
        expected_peer,
        expected_keys,
        origin_tx,
        origin_req_id,
        deadline,
    } = registration;
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
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                let relay = { timeout_relays.lock().await.remove(&relay_id) };
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
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tasks.spawn(timeout_task).await,
        None => false,
    };
    if !registered {
        relays.lock().await.remove(&relay_id);
    }
}

async fn register_get_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: RelayMap,
    relay_id: u64,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    deadline: Instant,
) {
    let (timeout_cancel, cancelled) = oneshot::channel();
    relays.lock().await.insert(
        relay_id,
        GetRelay {
            origin_tx,
            origin_req_id,
            timeout_cancel,
        },
    );
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                timeout_relays.lock().await.remove(&relay_id);
            }
            _ = cancelled => {}
        }
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tasks.spawn(timeout_task).await,
        None => false,
    };
    if !registered {
        relays.lock().await.remove(&relay_id);
    }
}

/// Relay a write to the fragment's current owner and preserve the exact
/// response path. A fresh local request id prevents collisions between
/// independent upstream peers.
async fn relay_put(
    mut p: ForwardedPut,
    frag: FragmentId,
    origin_tx: &PeerChannel,
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
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_keys: keys.clone(),
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
        },
    )
    .await;
    p.req_id = relay_id;
    p.hops += 1;
    let sent = try_send_envelope_until(
        &owner_tx,
        Envelope {
            msg: Some(EnvelopeMsg::ForwardedPut(p)),
        },
        attempt_deadline,
    )
    .await;
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
    origin_tx: &PeerChannel,
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
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_keys: keys.clone(),
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
        },
    )
    .await;
    bp.req_id = relay_id;
    for p in bp.puts.iter_mut() {
        p.hops += 1;
    }
    let sent = try_send_envelope_until(
        &owner_tx,
        Envelope {
            msg: Some(EnvelopeMsg::BatchPut(bp)),
        },
        attempt_deadline,
    )
    .await;
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
    origin_tx: &PeerChannel,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
) {
    let Some((_owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    register_get_relay(
        &ctx.tasks,
        ctx.pending_relays.clone(),
        relay_id,
        origin_tx.clone(),
        g.req_id,
        attempt_deadline,
    )
    .await;
    let sent = try_send_envelope_until(
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
        attempt_deadline,
    )
    .await;
    if !sent {
        if let Some(relay) = ctx.pending_relays.lock().await.remove(&relay_id) {
            let _ = relay.timeout_cancel.send(());
        }
    }
}

/// Own one outbound dial intent until shutdown. Authentication/protocol
/// failures back off just like transport failures. A duplicate loser parks on
/// peer-map changes while the elected connection is healthy instead of
/// continuously rotating replication sessions.
async fn dial_supervisor(
    ctx: ConnContext,
    network: Arc<dyn Network>,
    addr: SocketAddr,
    mut expected_peer_id: Option<String>,
    mut initial_stream: Option<BoxStream>,
) {
    let mut backoff = RECONNECT_BASE;
    loop {
        let stream = match initial_stream.take() {
            Some(stream) => stream,
            None => match tokio::time::timeout(
                ctx.runtime_options.connect_timeout,
                network.connect(addr),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::debug!(%addr, %error, "peer dial failed");
                    tokio::time::sleep(backoff + jitter(backoff)).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    continue;
                }
                Err(_) => {
                    tracing::debug!(%addr, "peer dial timed out");
                    tokio::time::sleep(backoff + jitter(backoff)).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    continue;
                }
            },
        };

        let outcome = handle_connection(
            ctx.clone(),
            stream,
            /* we_dialed = */ true,
            expected_peer_id.clone(),
            None,
        )
        .await;

        // Address-only `connect` pins the first authenticated identity for
        // every later reconnect. Otherwise an address reused by a different
        // node could make the dial intent look healthy while the original
        // routing/replication target remained permanently absent.
        if expected_peer_id.is_none() {
            expected_peer_id.clone_from(&outcome.peer_id);
        }

        if let Some(peer) = outcome.peer_id.as_deref() {
            let mut changes = ctx.peer_changes.subscribe();
            let winner_is_live = ctx.peers.lock().await.contains_key(peer);
            if winner_is_live {
                while ctx.peers.lock().await.contains_key(peer) {
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
                backoff = RECONNECT_BASE;
                continue;
            }
        }

        if outcome.was_established {
            backoff = RECONNECT_BASE;
        }
        tokio::time::sleep(backoff + jitter(backoff)).await;
        if !outcome.was_established {
            backoff = (backoff * 2).min(RECONNECT_MAX);
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
fn fresh_hello(node_id: &str) -> Result<Hello, getrandom::Error> {
    let mut challenge = vec![0u8; HANDSHAKE_CHALLENGE_BYTES];
    getrandom::getrandom(&mut challenge)?;
    Ok(Hello {
        node_id: node_id.to_string(),
        auth_tag: Vec::new(),
        min_protocol_version: WIRE_PROTOCOL_VERSION,
        max_protocol_version: WIRE_PROTOCOL_VERSION,
        capabilities: REQUIRED_WIRE_CAPABILITIES,
        challenge,
    })
}

async fn send_hello_until(tx: &PeerChannel, hello: Hello, deadline: Instant) -> bool {
    try_send_envelope_until(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(hello)),
        },
        deadline,
    )
    .await
}

async fn send_hello_auth_until(tx: &PeerChannel, auth_tag: Vec<u8>, deadline: Instant) -> bool {
    try_send_envelope_until(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })),
        },
        deadline,
    )
    .await
}

/// Canonical manual encoding used by both HMAC and connection election. It is
/// deliberately independent of protobuf encoding and binds both advertised
/// offers plus the selected protocol/capabilities.
fn handshake_transcript(dialer: &Hello, listener: &Hello) -> Vec<u8> {
    fn append_hello(out: &mut Vec<u8>, role: &[u8], hello: &Hello) {
        out.extend_from_slice(&(role.len() as u64).to_be_bytes());
        out.extend_from_slice(role);
        out.extend_from_slice(&(hello.node_id.len() as u64).to_be_bytes());
        out.extend_from_slice(hello.node_id.as_bytes());
        out.extend_from_slice(&hello.min_protocol_version.to_be_bytes());
        out.extend_from_slice(&hello.max_protocol_version.to_be_bytes());
        out.extend_from_slice(&hello.capabilities.to_be_bytes());
        out.extend_from_slice(&(hello.challenge.len() as u64).to_be_bytes());
        out.extend_from_slice(&hello.challenge);
    }

    let mut transcript = Vec::with_capacity(256);
    transcript.extend_from_slice(b"actordb-handshake-transcript-v2\0");
    append_hello(&mut transcript, b"dialer", dialer);
    append_hello(&mut transcript, b"listener", listener);
    transcript.extend_from_slice(&WIRE_PROTOCOL_VERSION.to_be_bytes());
    transcript.extend_from_slice(&(dialer.capabilities & listener.capabilities).to_be_bytes());
    transcript
}

fn handshake_auth_tag(
    secret: &[u8],
    prover_is_dialer: bool,
    dialer: &Hello,
    listener: &Hello,
) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(b"actordb-handshake-proof-v2\0");
    mac.update(if prover_is_dialer {
        b"dialer"
    } else {
        b"listener"
    });
    mac.update(&handshake_transcript(dialer, listener));
    mac.finalize().into_bytes().to_vec()
}

fn verify_handshake_auth(
    secret: Option<&[u8]>,
    prover_is_dialer: bool,
    dialer: &Hello,
    listener: &Hello,
    tag: &[u8],
) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    match secret {
        Some(secret) => {
            if tag.len() != 32 {
                return false;
            }
            // `verify_slice` is constant-time.
            let mut verifier =
                Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
            verifier.update(b"actordb-handshake-proof-v2\0");
            verifier.update(if prover_is_dialer {
                b"dialer"
            } else {
                b"listener"
            });
            verifier.update(&handshake_transcript(dialer, listener));
            verifier.verify_slice(tag).is_ok()
        }
        None => tag.is_empty(),
    }
}

fn connection_rank(dialer: &Hello, listener: &Hello) -> ConnectionRank {
    use sha2::{Digest, Sha256};

    let transcript_id: [u8; 32] = Sha256::digest(handshake_transcript(dialer, listener)).into();
    ConnectionRank {
        preferred_direction: dialer.node_id < listener.node_id,
        transcript_id,
    }
}

async fn send_sync_request(tx: &PeerChannel, fragments: Vec<u32>) {
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

async fn send_envelope(tx: &PeerChannel, envelope: Envelope) {
    let _ = try_send_envelope(tx, envelope).await;
}

async fn try_send_envelope(tx: &PeerChannel, envelope: Envelope) -> bool {
    try_send_envelope_with_deadline(tx, envelope, None).await
}

async fn try_send_envelope_until(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Instant,
) -> bool {
    try_send_envelope_with_deadline(tx, envelope, Some(attempt_deadline)).await
}

async fn try_send_envelope_with_deadline(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Option<Instant>,
) -> bool {
    let mut frame = Vec::new();
    if envelope.encode(&mut frame).is_err() {
        return false;
    }
    let queue_deadline = deadline_after(tx.queue_admission_timeout);
    let admission_deadline = attempt_deadline
        .map(|deadline| deadline.min(queue_deadline))
        .unwrap_or(queue_deadline);
    if Instant::now() >= admission_deadline {
        tx.cancel();
        return false;
    }
    match tokio::time::timeout_at(admission_deadline, tx.tx.reserve()).await {
        Ok(Ok(permit)) => {
            if Instant::now() >= admission_deadline {
                drop(permit);
                tx.cancel();
                return false;
            }
            permit.send(frame);
            true
        }
        Ok(Err(_)) | Err(_) => {
            tx.cancel();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::spawn_sender_with_options;

    fn test_peer_channel(capacity: usize) -> (PeerChannel, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (cancel, _) = watch::channel(false);
        (
            PeerChannel {
                tx,
                cancel,
                queue_admission_timeout: Duration::from_millis(10),
                rank: None,
            },
            rx,
        )
    }

    fn test_sender_report() -> Arc<Mutex<SenderStateReport>> {
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

    fn test_placement_deps(
        router: FragmentRouter,
        serving: Arc<RwLock<HashSet<FragmentId>>>,
        placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
        supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>>,
        senders: SenderMap,
        sender_reports: SenderReportMap,
    ) -> PlacementDriverDeps {
        PlacementDriverDeps {
            node_id: "node-a".to_string(),
            controller: Arc::new(crate::placement::StaticPlacement::new(HashMap::new())),
            poll_interval: Duration::from_secs(3600),
            drain_warn_interval: Duration::from_secs(3600),
            fragment_count: 1,
            subtree_config: NodeConfig::default(),
            router,
            serving,
            placement,
            supervisors,
            supervisor_stops: Arc::new(SupervisorRegistry::new(1)),
            placement_states: Arc::new(Mutex::new(HashMap::from([(
                FragmentId(0),
                FragmentPlacementState::Draining,
            )]))),
            placement_generation: Arc::new(AtomicU64::new(0)),
            senders,
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_reports,
            sender_config: SenderConfig::default(),
            sender_runtime: SenderRuntimeOptions::default(),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            tasks: Weak::new(),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
        }
    }

    fn test_connection_context(
        router: FragmentRouter,
        serving: Arc<RwLock<HashSet<FragmentId>>>,
        placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
        dynamic_placement: bool,
        senders: SenderMap,
        sender_reports: SenderReportMap,
        peers: PeerMap,
    ) -> (ConnContext, Arc<RuntimeTasks>) {
        let tasks = RuntimeTasks::new(8);
        let (peer_changes, _) = watch::channel(0u64);
        (
            ConnContext {
                node_id: "local".to_string(),
                router,
                serving,
                placement,
                dynamic_placement,
                pending_gets: Arc::new(StdMutex::new(HashMap::new())),
                pending_batch_gets: Arc::new(StdMutex::new(HashMap::new())),
                pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
                pending_relays: Arc::new(Mutex::new(HashMap::new())),
                pending_mutation_relays: Arc::new(Mutex::new(HashMap::new())),
                next_req_id: Arc::new(AtomicU64::new(1)),
                peers,
                peer_failures: PeerFailureSignals::default(),
                tasks: Arc::downgrade(&tasks),
                peer_changes,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                senders,
                retiring_senders: Arc::new(RetiringSenderRegistry::default()),
                sender_config: SenderConfig::default(),
                sender_runtime: SenderRuntimeOptions::default(),
                runtime_options: NodeRuntimeOptions::default(),
                sender_reports,
                sync_entries_applied: Arc::new(AtomicU64::new(0)),
                membership_tx: None,
                cluster_secret: None,
            },
            tasks,
        )
    }

    async fn complete_test_handshake(stream: &mut tokio::io::DuplexStream, peer_id: &str) {
        assert!(matches!(
            read_test_envelope(stream).await.msg,
            Some(EnvelopeMsg::Hello(_))
        ));
        write_test_envelope(
            stream,
            Envelope {
                msg: Some(EnvelopeMsg::Hello(Hello {
                    node_id: peer_id.to_string(),
                    auth_tag: Vec::new(),
                    min_protocol_version: WIRE_PROTOCOL_VERSION,
                    max_protocol_version: WIRE_PROTOCOL_VERSION,
                    capabilities: REQUIRED_WIRE_CAPABILITIES,
                    challenge: vec![7; HANDSHAKE_CHALLENGE_BYTES],
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(stream).await.msg,
            Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) if auth_tag.is_empty()
        ));
        write_test_envelope(
            stream,
            Envelope {
                msg: Some(EnvelopeMsg::HelloAuth(HelloAuth {
                    auth_tag: Vec::new(),
                })),
            },
        )
        .await;
    }

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

    async fn write_test_envelope(stream: &mut tokio::io::DuplexStream, envelope: Envelope) {
        let frame = envelope.encode_to_vec();
        stream.write_u32(frame.len() as u32).await.unwrap();
        stream.write_all(&frame).await.unwrap();
    }

    async fn read_test_envelope(stream: &mut tokio::io::DuplexStream) -> Envelope {
        tokio::time::timeout(Duration::from_secs(1), async {
            let len = stream.read_u32().await.unwrap() as usize;
            let mut frame = vec![0; len];
            stream.read_exact(&mut frame).await.unwrap();
            Envelope::decode(frame.as_slice()).unwrap()
        })
        .await
        .expect("timed out waiting for test protocol frame")
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
    fn runtime_option_defaults_are_explicit_and_source_compatible() {
        let options = NodeRuntimeOptions::default();
        assert_eq!(options.peer_queue_frame_capacity(), 256);
        assert_eq!(options.replication_control_message_capacity(), 64);
        assert_eq!(options.inbound_connection_capacity(), 1024);
        assert_eq!(options.queue_admission_timeout(), Duration::from_secs(5));
        assert_eq!(options.forwarded_attempt_timeout(), Duration::from_secs(5));
        assert_eq!(options.connect_timeout(), Duration::from_secs(5));
        assert_eq!(options.handshake_timeout(), Duration::from_secs(5));
        assert_eq!(options.socket_read_timeout(), Duration::from_secs(30));
        assert_eq!(options.socket_write_timeout(), Duration::from_secs(30));
        assert_eq!(options.replication_ack_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn runtime_options_reject_unrepresentable_limits() {
        assert!(NodeRuntimeOptions::default()
            .with_socket_write_timeout(Duration::MAX)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_peer_queue_frame_capacity(tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1))
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_replication_control_message_capacity(0)
            .validate()
            .is_err());
        assert!(NodeRuntimeOptions::default()
            .with_inbound_connection_capacity(0)
            .validate()
            .is_err());
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
        ] {
            assert!(options.validate().is_err());
        }
    }

    #[test]
    fn challenge_hmac_binds_both_offers_roles_and_freshness() {
        let dialer = Hello {
            node_id: "dialer".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        let listener = Hello {
            node_id: "listener".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![2; HANDSHAKE_CHALLENGE_BYTES],
        };
        let authenticated = handshake_auth_tag(b"secret", true, &dialer, &listener);
        assert!(verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &listener,
            &authenticated,
        ));
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            false,
            &dialer,
            &listener,
            &authenticated,
        ));
        let mut fresh_listener = listener.clone();
        fresh_listener.challenge[0] ^= 1;
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &fresh_listener,
            &authenticated,
        ));

        let mut tampered_dialer = dialer.clone();
        tampered_dialer.capabilities ^= CAPABILITY_MUTATION_RESULTS;
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &tampered_dialer,
            &listener,
            &authenticated,
        ));
        let reflected = handshake_auth_tag(b"secret", false, &listener, &dialer);
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &listener,
            &reflected,
        ));
    }

    #[test]
    fn each_hello_uses_a_fresh_nonzero_os_challenge() {
        let first = fresh_hello("node").unwrap();
        let second = fresh_hello("node").unwrap();
        assert_eq!(first.challenge.len(), HANDSHAKE_CHALLENGE_BYTES);
        assert_eq!(second.challenge.len(), HANDSHAKE_CHALLENGE_BYTES);
        assert!(first.challenge.iter().any(|byte| *byte != 0));
        assert!(second.challenge.iter().any(|byte| *byte != 0));
        assert_ne!(first.challenge, second.challenge);
    }

    #[test]
    fn connection_rank_prefers_one_crossed_dial_and_totally_orders_duplicates() {
        let a = Hello {
            node_id: "a".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        let b = Hello {
            node_id: "b".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![2; HANDSHAKE_CHALLENGE_BYTES],
        };

        let canonical = connection_rank(&a, &b);
        let reverse = connection_rank(&b, &a);
        assert!(canonical.preferred_direction);
        assert!(!reverse.preferred_direction);
        assert!(canonical > reverse);

        let mut second_b = b.clone();
        second_b.challenge[0] ^= 1;
        assert_ne!(canonical, connection_rank(&a, &second_b));
    }

    #[tokio::test]
    async fn saturated_ack_and_nack_control_lanes_close_the_connection() {
        for nack in [false, true] {
            let fragment = FragmentId(0);
            let report = test_sender_report();
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    reset_senders: Arc::new(Mutex::new(HashMap::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            sender
                .send(SenderMsg::Ack {
                    session_id: 1,
                    batch_seq: 1,
                    durability: ReplicationAckDurability::MemoryApplied,
                })
                .unwrap();

            let senders = Arc::new(Mutex::new(HashMap::from([(
                (fragment, "peer".to_string()),
                sender.clone(),
            )])));
            let sender_reports = Arc::new(Mutex::new(HashMap::from([(
                (fragment, "peer".to_string()),
                report.clone(),
            )])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders,
                sender_reports,
                peers.clone(),
            );
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if peers.lock().await.contains_key("peer") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("peer Hello was not installed");

            let response = if nack {
                EnvelopeMsg::BatchNack(BatchNack {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    error: "retry".to_string(),
                })
            } else {
                EnvelopeMsg::BatchAck(BatchAck {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    level: ReplicationAckLevel::Applied as i32,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(response),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("saturated sender control lane did not close the connection")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            runtime_tasks.shutdown().await;

            drop(report_guard);
            sender.stop().await;
        }
    }

    #[tokio::test]
    async fn retiring_reconcile_consumer_does_not_close_shared_connection() {
        for data in [false, true] {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            // Keep the sender from consuming the reconcile control message so
            // the test can retire it at the exact admission boundary.
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    reset_senders: Arc::new(Mutex::new(HashMap::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports =
                Arc::new(Mutex::new(HashMap::from([(key.clone(), report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders.clone(),
                sender_reports,
                peers.clone(),
            );
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if peers.lock().await.contains_key("peer") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("peer Hello was not installed");

            let reconcile = if data {
                EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                })
            } else {
                EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while sender.control_capacity() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("reconcile frame was not admitted to the sender");

            let retired = senders.lock().await.remove(&key).unwrap();
            retired.request_stop();
            drop(report_guard);

            // A per-fragment consumer disappearing is a local placement race,
            // not grounds to discard the multiplexed connection. Exercise a
            // subsequent request to prove that both read and write halves are
            // still usable.
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::SyncRequest(crate::proto::SyncRequest {
                        fragments: Vec::new(),
                    })),
                },
            )
            .await;
            loop {
                if matches!(
                    read_test_envelope(&mut client).await.msg,
                    Some(EnvelopeMsg::SyncResponse(_))
                ) {
                    break;
                }
            }
            assert!(peers.lock().await.contains_key("peer"));

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
            retired.stop().await;
        }
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
        );
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
                    node_id: "peer".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: ProtoPriority::Default as i32,
                    hops: MAX_FORWARD_HOPS,
                    req_id: 11,
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
                        node_id: "peer".to_string(),
                        seq: 2,
                        boot_id: 1,
                        expires_at_ms: 0,
                        is_tombstone: false,
                        priority: ProtoPriority::Default as i32,
                        hops: 0,
                        req_id: 12,
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

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection did not close after its stream was dropped")
            .unwrap();
        assert!(peers.lock().await.is_empty());
        runtime_tasks.shutdown().await;
    }

    #[tokio::test]
    async fn dynamic_connection_handler_does_not_create_placement_senders() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) = spawn_fragment_subtree(
            "dynamic-sender-ownership-test",
            fragment,
            &NodeConfig::default(),
            &hlc,
        )
        .await
        .unwrap();
        router.insert(fragment, dispatcher);

        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["local".to_string(), "peer".to_string()],
        )])));
        let (mut ctx, runtime_tasks) = test_connection_context(
            router.clone(),
            Arc::new(RwLock::new(HashSet::from([fragment]))),
            placement,
            true,
            senders.clone(),
            reports.clone(),
            peers.clone(),
        );
        ctx.node_id = "local".to_string();

        let (mut client, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut client, "peer").await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !peers.lock().await.contains_key("peer") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dynamic peer was not authenticated");

        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("dynamic connection did not finish")
            .unwrap();
        runtime_tasks.shutdown().await;
        router.remove(fragment);
        supervisor.stop(None);
        let _ = supervisor.wait(None).await;
    }

    #[tokio::test]
    async fn legacy_disconnected_peer_identities_reap_their_senders() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) = spawn_fragment_subtree(
            "legacy-sender-reap-test",
            fragment,
            &NodeConfig::default(),
            &hlc,
        )
        .await
        .unwrap();
        router.insert(fragment, dispatcher);

        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (ctx, runtime_tasks) = test_connection_context(
            router.clone(),
            Arc::new(RwLock::new(HashSet::from([fragment]))),
            Arc::new(RwLock::new(HashMap::new())),
            false,
            senders.clone(),
            reports.clone(),
            peers.clone(),
        );

        for index in 0..3 {
            let peer = format!("ephemeral-peer-{index}");
            let (mut client, server) = tokio::io::duplex(16 * 1024);
            let connection = tokio::spawn(handle_connection(
                ctx.clone(),
                Box::pin(server),
                false,
                None,
                None,
            ));
            complete_test_handshake(&mut client, &peer).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while !senders.lock().await.contains_key(&(fragment, peer.clone())) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("legacy sender was not created");

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("legacy connection did not finish")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            assert!(senders.lock().await.is_empty());
            assert!(reports.lock().await.is_empty());
        }

        runtime_tasks.shutdown().await;
        router.remove(fragment);
        supervisor.stop(None);
        let _ = supervisor.wait(None).await;
    }

    #[tokio::test]
    async fn placement_revocation_waits_for_admitted_mutation_lease() {
        let router = FragmentRouter::new(1, 0);
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let admitted = router.mutation_gate(fragment).read_owned().await;
        let revocation_gate = router.mutation_gate(fragment);
        let revoked_serving = serving.clone();
        let revoke = tokio::spawn(async move {
            let exclusive = revocation_gate.write_owned().await;
            revoked_serving.write().unwrap().remove(&fragment);
            drop(exclusive);
        });
        tokio::task::yield_now().await;
        assert!(!revoke.is_finished());
        assert!(serving.read().unwrap().contains(&fragment));

        drop(admitted);
        revoke.await.unwrap();
        assert!(!serving.read().unwrap().contains(&fragment));
        let _late_mutation = router.mutation_gate(fragment).read_owned().await;
        assert!(!serving.read().unwrap().contains(&fragment));
    }

    #[tokio::test]
    async fn placement_drain_rechecks_reset_required_sender_as_one_map_snapshot() {
        let fragment = FragmentId(0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        sender.schedule_reset();
        let key = (fragment, "peer".to_string());
        let senders = HashMap::from([(key.clone(), sender.clone())]);
        let reports = HashMap::from([(key, report)]);

        assert!(!fragment_sender_set_is_drained(fragment, &senders, &reports).await);
        sender.stop().await;
    }

    #[tokio::test]
    async fn completed_bootstrap_cannot_publish_after_placement_revocation() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["node-b".to_string()],
        )])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_states
            .lock()
            .await
            .insert(fragment, FragmentPlacementState::Bootstrapping);

        let expected = vec!["node-a".to_string(), "node-b".to_string()];
        assert_eq!(
            commit_fragment_gain(&deps, fragment, &expected, 0).await,
            FragmentGainCommit::Revoked
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Draining)
        );
    }

    #[tokio::test]
    async fn completed_bootstrap_retries_after_replica_set_replacement() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let current = vec!["node-a".to_string(), "node-c".to_string()];
        let placement = Arc::new(RwLock::new(HashMap::from([(fragment, current.clone())])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_states
            .lock()
            .await
            .insert(fragment, FragmentPlacementState::Bootstrapping);

        let stale = vec!["node-a".to_string(), "node-b".to_string()];
        assert_eq!(
            commit_fragment_gain(&deps, fragment, &stale, 0).await,
            FragmentGainCommit::Retry
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Bootstrapping)
        );

        assert_eq!(
            commit_fragment_gain(&deps, fragment, &current, 0).await,
            FragmentGainCommit::Serving
        );
        assert!(serving.read().unwrap().contains(&fragment));
    }

    #[tokio::test]
    async fn completed_bootstrap_retries_after_placement_aba() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let replicas = vec!["node-a".to_string(), "node-b".to_string()];
        let placement = Arc::new(RwLock::new(HashMap::from([(fragment, replicas.clone())])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_generation.store(2, Ordering::Release);

        assert_eq!(
            commit_fragment_gain(&deps, fragment, &replicas, 0).await,
            FragmentGainCommit::Retry
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Bootstrapping)
        );
    }

    #[tokio::test]
    async fn stopped_sender_entry_is_replaced_in_snapshot_recovery() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let config = NodeConfig::default();
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("replace-sender-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peer_failures = PeerFailureSignals::default();
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let sender_config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
            },
        )
        .await;
        let original = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .unwrap();
        original.stop().await;

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
            },
        )
        .await;
        let replacement = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .unwrap();
        assert!(!replacement.is_closed());
        assert!(!replacement.ingress_is_idle());
        assert_eq!(reports.lock().await.len(), 1);

        replacement.stop().await;
        router.remove(fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn drain_set_ignores_other_fragments_and_requires_a_matching_report() {
        let target = FragmentId(0);
        let other = FragmentId(1);
        let other_report = test_sender_report();
        let other_sender = spawn_node_sender(
            other,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(2, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: other_report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let other_key = (other, "peer".to_string());
        let mut senders = HashMap::from([(other_key.clone(), other_sender.clone())]);
        let reports = HashMap::from([(other_key, other_report)]);
        assert!(fragment_sender_set_is_drained(target, &senders, &reports).await);

        let target_sender = spawn_node_sender(
            target,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(2, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: test_sender_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        senders.insert((target, "peer".to_string()), target_sender.clone());
        assert!(!fragment_sender_set_is_drained(target, &senders, &reports).await);

        target_sender.stop().await;
        other_sender.stop().await;
    }

    #[tokio::test]
    async fn final_teardown_commit_restores_a_concurrently_readded_fragment() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("readd-at-commit-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let supervisors = Arc::new(Mutex::new(HashMap::from([(fragment, supervisor.clone())])));
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let report = test_sender_report();
        let report_guard = report.lock().await;
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: router.clone(),
                hlc: hlc.clone(),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let key = (fragment, "peer".to_string());
        let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement.clone(),
            supervisors.clone(),
            senders.clone(),
            reports,
        );
        let loss_deps = deps.clone();
        let loss = tokio::spawn(async move {
            lose_fragment(&loss_deps, fragment).await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let revoked = !serving.read().unwrap().contains(&fragment);
                let first_lock_is_held = senders.try_lock().is_err();
                tokio::task::yield_now().await;
                let second_lock_is_held = senders.try_lock().is_err();
                if revoked && first_lock_is_held && second_lock_is_held {
                    break;
                }
            }
        })
        .await
        .expect("loss transition did not reach its preliminary drain check");
        {
            let _states = deps.placement_states.lock().await;
            placement
                .write()
                .unwrap()
                .insert(fragment, vec!["node-a".to_string()]);
        }
        drop(report_guard);
        tokio::time::timeout(Duration::from_secs(1), loss)
            .await
            .expect("re-added fragment did not leave teardown")
            .unwrap();

        assert!(serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Serving)
        );
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.contains_key(&fragment));
        sender.stop().await;
        router.remove(fragment);
        supervisors.lock().await.remove(&fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn final_teardown_commit_retries_when_sender_state_changes() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("sender-change-at-commit-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let supervisors = Arc::new(Mutex::new(HashMap::from([(fragment, supervisor.clone())])));
        let mutation_gate = router.mutation_gate(fragment);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let report = test_sender_report();
        let report_guard = report.lock().await;
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: router.clone(),
                hlc: hlc.clone(),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let key = (fragment, "peer".to_string());
        let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement.clone(),
            supervisors.clone(),
            senders.clone(),
            reports,
        );
        let loss_deps = deps.clone();
        let loss = tokio::spawn(async move {
            lose_fragment(&loss_deps, fragment).await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let revoked = !serving.read().unwrap().contains(&fragment);
                let first_lock_is_held = senders.try_lock().is_err();
                tokio::task::yield_now().await;
                let second_lock_is_held = senders.try_lock().is_err();
                if revoked && first_lock_is_held && second_lock_is_held {
                    break;
                }
            }
        })
        .await
        .expect("loss transition did not reach its preliminary drain check");
        let admitted = mutation_gate.read_owned().await;
        drop(report_guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if deps.placement_states.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loss transition did not reach its final commit fence");

        sender.schedule_reset();
        let publish_states = deps.placement_states.clone();
        let publish_placement = placement.clone();
        let (publisher_started_tx, publisher_started_rx) = oneshot::channel();
        let publisher = tokio::spawn(async move {
            let _ = publisher_started_tx.send(());
            let _states = publish_states.lock().await;
            publish_placement
                .write()
                .unwrap()
                .insert(fragment, vec!["node-a".to_string()]);
        });
        publisher_started_rx.await.unwrap();
        drop(admitted);
        tokio::time::timeout(Duration::from_secs(1), publisher)
            .await
            .expect("re-add publication remained blocked behind teardown")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), loss)
            .await
            .expect("teardown did not retry after sender state changed")
            .unwrap();

        assert!(serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Serving)
        );
        assert!(!senders
            .lock()
            .await
            .contains_key(&(fragment, "peer".to_string())));
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.contains_key(&fragment));
        sender.stop().await;
        router.remove(fragment);
        supervisors.lock().await.remove(&fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn completed_teardown_without_registered_supervisor_marks_stopped() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("teardown-supervisor-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let supervisors = Arc::new(Mutex::new(HashMap::new()));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement,
            supervisors.clone(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );

        tokio::time::timeout(Duration::from_secs(1), lose_fragment(&deps, fragment))
            .await
            .expect("drained fragment teardown did not complete");
        assert!(!serving.read().unwrap().contains(&fragment));
        assert!(router.route_for_fragment(fragment).is_none());
        assert!(supervisors.lock().await.is_empty());
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Stopped)
        );
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn stale_sender_ensure_refuses_a_removed_router_fragment() {
        let router = FragmentRouter::new(1, 0);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peer_failures = PeerFailureSignals::default();
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));

        ensure_sender_entry(
            FragmentId(0),
            "peer",
            SenderEntryDeps {
                config: &config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
            },
        )
        .await;

        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());
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

    #[test]
    fn dropping_pending_read_waiters_removes_registry_entries() {
        let pending_gets: PendingGets = Arc::new(StdMutex::new(HashMap::new()));
        let (get_reply, _get_response) = oneshot::channel();
        pending_gets.lock().unwrap().insert(3, get_reply);
        {
            let _guard = PendingGetGuard {
                req_id: 3,
                pending: pending_gets.clone(),
            };
        }
        assert!(pending_gets.lock().unwrap().is_empty());

        let pending_batch_gets: PendingBatchGets = Arc::new(StdMutex::new(HashMap::new()));
        let (batch_reply, _batch_response) = oneshot::channel();
        pending_batch_gets.lock().unwrap().insert(4, batch_reply);
        {
            let _guard = PendingBatchGetGuard {
                req_id: 4,
                pending: pending_batch_gets.clone(),
            };
        }
        assert!(pending_batch_gets.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn full_peer_queue_times_out_cancels_and_does_not_admit_frame() {
        let (peer, mut rx) = test_peer_channel(1);
        let mut cancelled = peer.cancel.subscribe();
        let envelope = || Envelope { msg: None };
        assert!(try_send_envelope(&peer, envelope()).await);
        assert!(!try_send_envelope(&peer, envelope()).await);
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("connection was not cancelled")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn forwarded_attempt_deadline_caps_queue_admission_wait() {
        let (mut peer, mut rx) = test_peer_channel(1);
        peer.queue_admission_timeout = Duration::from_secs(1);
        assert!(try_send_envelope(&peer, Envelope { msg: None }).await);
        let started = Instant::now();
        assert!(
            !try_send_envelope_until(
                &peer,
                Envelope { msg: None },
                started + Duration::from_millis(10),
            )
            .await
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_forwarded_attempt_never_admits_to_an_empty_queue() {
        let (peer, mut rx) = test_peer_channel(1);
        let expired = Instant::now();
        tokio::task::yield_now().await;
        assert!(
            !try_send_envelope_until(&peer, Envelope { msg: None }, expired).await,
            "an expired total attempt budget must fail before reserving capacity"
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn closed_peer_queue_fails_and_cancels_connection() {
        let (peer, rx) = test_peer_channel(1);
        let mut cancelled = peer.cancel.subscribe();
        drop(rx);
        assert!(!try_send_envelope(&peer, Envelope { msg: None }).await);
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("connection was not cancelled")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
    }

    #[tokio::test]
    async fn stale_peer_failure_cannot_overwrite_current_channel_failure() {
        let (current, _current_rx) = test_peer_channel(1);
        let (stale, _stale_rx) = test_peer_channel(1);
        let mut cancelled = current.cancel.subscribe();
        let peers = Arc::new(Mutex::new(HashMap::from([(
            "peer".to_string(),
            current.clone(),
        )])));
        let failures = PeerFailureSignals::default();

        // Queue both before the worker starts so the stale signal is
        // guaranteed to arrive last. A single-value map used to lose the
        // current-channel failure here and leave replication disconnected.
        failures.signal("peer".to_string(), current.raw_sender());
        failures.signal("peer".to_string(), stale.raw_sender());
        let worker = tokio::spawn(failures.run(peers));

        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("current connection failure was overwritten")
            .expect("current cancellation sender dropped");
        assert!(*cancelled.borrow());
        worker.abort();
        let _ = worker.await;
    }

    #[tokio::test]
    async fn cancelled_sender_retirement_remains_owned_for_shutdown_join() {
        let sender = spawn_node_sender(
            FragmentId(0),
            "retiring-peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: test_sender_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let registry = Arc::new(RetiringSenderRegistry::default());
        let retirement = registry.retire(sender);
        drop(retirement); // model cancellation before the normal join path

        let retained = registry.snapshot();
        assert_eq!(retained.len(), 1);
        tokio::time::timeout(Duration::from_secs(1), retained[0].stop())
            .await
            .expect("retiring sender was not joinable after owner cancellation");
        registry.clear();
    }

    #[tokio::test]
    async fn socket_write_timeout_signals_connection_cancellation() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (cancel_tx, mut cancelled) = watch::channel(false);
        frame_tx.send(vec![0; 1024]).await.unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_millis(10),
        ));
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("writer did not signal cancellation")
            .expect("writer cancellation sender dropped");
        assert!(*cancelled.borrow());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn connection_cancellation_interrupts_a_blocked_socket_write() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        frame_tx.send(vec![0; 1024]).await.unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            cancel_tx.subscribe(),
            cancel_tx.clone(),
            Duration::from_secs(30),
        ));
        tokio::task::yield_now().await;
        let _ = cancel_tx.send(true);

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled writer waited for the socket write timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn mutation_relay_timeout_removes_entry_and_replies_unavailable() {
        let relays: MutationRelayMap = Arc::new(Mutex::new(HashMap::new()));
        let (origin_tx, mut origin_rx) = test_peer_channel(1);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(10),
            },
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
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn completed_mutation_relay_cancels_timeout() {
        let relays: MutationRelayMap = Arc::new(Mutex::new(HashMap::new()));
        let (origin_tx, mut origin_rx) = test_peer_channel(1);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(10),
            },
        )
        .await;
        let relay = relays.lock().await.remove(&17).expect("relay missing");
        let _ = relay.timeout_cancel.send(());

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(origin_rx.try_recv().is_err());
        assert!(relays.lock().await.is_empty());
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn read_relay_timeout_removes_registry_entry() {
        let relays: RelayMap = Arc::new(Mutex::new(HashMap::new()));
        let (origin_tx, _origin_rx) = test_peer_channel(1);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        register_get_relay(
            &task_ref,
            relays.clone(),
            23,
            origin_tx,
            11,
            Instant::now() + Duration::from_millis(10),
        )
        .await;
        assert!(relays.lock().await.contains_key(&23));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if relays.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay deadline did not remove the entry");
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn stale_peer_disconnect_does_not_clear_replacement_sender_channel() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = crate::replication::spawn_sender(
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

    #[tokio::test]
    async fn reset_sender_retries_reconcile_after_live_queue_admission_timeout() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(20)),
        )
        .expect("sender options are valid");
        let op = |key: &str| ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 0,
            boot_id: 0,
        };

        sender.send(SenderMsg::Enqueue(op("first"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "buffer_reset" && state.session_id == 0
        })
        .await;

        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();

        // The SessionStart fills the only queue slot. The sender publishes
        // this session only after the immediate ReconcileRequest admission
        // has timed out against the still-live, full channel.
        common_wait_for_sender_report(&report, |state| {
            state.state == "buffer_reset" && state.session_id != 0
        })
        .await;
        let session_id = report.lock().await.session_id;
        let session_frame = peer_rx.recv().await.expect("SessionStart frame missing");
        match Envelope::decode(session_frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => {
                assert_eq!(session.session_id, session_id);
                assert_eq!(session.fragment, 0);
            }
            other => panic!("expected SessionStart, got {other:?}"),
        }
        assert!(peer_rx.try_recv().is_err());

        assert!(
            tokio::time::timeout(Duration::from_millis(500), peer_rx.recv())
                .await
                .is_err(),
            "reconcile retried before the retry floor"
        );
        let reconcile_frame = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("sender did not retry reconciliation")
            .expect("peer channel closed");
        match Envelope::decode(reconcile_frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, session_id);
                assert_ne!(request.round_id, 0);
                assert_eq!(request.fragment, 0);
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn routine_reconcile_disconnect_drains_buffer_before_retry() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
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
            other => panic!("expected SessionStart, got {other:?}"),
        };

        sender.send(SenderMsg::ReconcileNow).unwrap();
        let old_round = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, old_session);
                request.round_id
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        };

        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "buffered-during-reconcile".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 1, "node".to_string()),
                seq: 7,
                boot_id: 11,
            }))
            .unwrap();
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "disconnected" && state.buffered == 1
        })
        .await;

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let batch = tokio::time::timeout(Duration::from_secs(1), new_rx.recv())
            .await
            .expect("buffered mutation was not replayed")
            .expect("replacement peer channel closed");
        let batch = match Envelope::decode(batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(batch.session_id, new_session);
        assert_eq!(batch.ops.len(), 1);
        assert_eq!(batch.ops[0].key, "buffered-during-reconcile");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), new_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the ordinary batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: new_session,
                batch_seq: batch.batch_seq,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("routine reconciliation was not retried after the batch ACK")
            .expect("replacement peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, 0);
                assert_ne!(request.round_id, old_round);
            }
            other => panic!("expected ReconcileRequest after batch ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_requeued_batch_on_reconnect() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
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
            other => panic!("expected SessionStart, got {other:?}"),
        };

        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "requeued-for-reconnect".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 17, "node".to_string()),
                seq: 23,
                boot_id: 29,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), old_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("old peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, old_session);
        assert_eq!(first_batch.ops.len(), 1);

        // ReconcileNow observes the in-flight batch and enters the fresh-drain
        // pause. Disconnecting before its ACK must turn that batch back into
        // replay work, rather than preserving a pause that skips the replay.
        sender.send(SenderMsg::ReconcileNow).unwrap();
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "disconnected" && state.buffered == 1 && state.in_flight == 0
        })
        .await;
        assert!(old_rx.try_recv().is_err());

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let replay = tokio::time::timeout(Duration::from_secs(1), new_rx.recv())
            .await
            .expect("requeued batch was not replayed")
            .expect("replacement peer channel closed");
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(replay.session_id, new_session);
        assert_eq!(replay.fragment, first_batch.fragment);
        assert_eq!(replay.required_ack_level, first_batch.required_ack_level);
        assert_eq!(replay.ops, first_batch.ops);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), new_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the replayed batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: new_session,
                batch_seq: replay.batch_seq,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("routine reconciliation was not resumed after replay ACK")
            .expect("replacement peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, 0);
            }
            other => panic!("expected ReconcileRequest after replay ACK, got {other:?}"),
        }
    }

    #[derive(Clone, Copy)]
    enum FreshPauseRequeue {
        Nack,
        AckTimeout,
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_nacked_batch() {
        assert_fresh_pause_replays_before_reconcile(FreshPauseRequeue::Nack).await;
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_ack_timed_out_batch() {
        assert_fresh_pause_replays_before_reconcile(FreshPauseRequeue::AckTimeout).await;
    }

    async fn assert_fresh_pause_replays_before_reconcile(trigger: FreshPauseRequeue) {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let required_ack_durability = match trigger {
            FreshPauseRequeue::Nack => ReplicationAckDurability::MemoryApplied,
            FreshPauseRequeue::AckTimeout => ReplicationAckDurability::StoreCommitted,
        };
        let runtime = match trigger {
            FreshPauseRequeue::Nack => SenderRuntimeOptions::default(),
            // The weak ACK below is an observable FIFO barrier proving that
            // ReconcileNow was handled before this deliberately short expiry.
            FreshPauseRequeue::AckTimeout => {
                SenderRuntimeOptions::default().with_ack_timeout(Duration::from_secs(1))
            }
        };
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability,
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            runtime,
        )
        .expect("sender options are valid");

        let (peer_tx, mut peer_rx) = mpsc::channel(4);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();
        let first_session = match Envelope::decode(peer_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };

        let key = match trigger {
            FreshPauseRequeue::Nack => "requeued-after-nack",
            FreshPauseRequeue::AckTimeout => "requeued-after-ack-timeout",
        };
        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: key.to_string(),
                value: VersionedValue::new(b"value".to_vec(), 31, "node".to_string()),
                seq: 37,
                boot_id: 41,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), peer_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, first_session);
        assert_eq!(first_batch.ops.len(), 1);

        sender.send(SenderMsg::ReconcileNow).unwrap();
        let replay_session = match trigger {
            FreshPauseRequeue::Nack => {
                sender
                    .send(SenderMsg::Nack {
                        session_id: first_session,
                        batch_seq: first_batch.batch_seq,
                        error: "injected rejection".to_string(),
                    })
                    .unwrap();
                common_wait_for_sender_report(&report, |state| {
                    state.buffered == 1 && state.in_flight == 0
                })
                .await;
                first_session
            }
            FreshPauseRequeue::AckTimeout => {
                sender
                    .send(SenderMsg::Ack {
                        session_id: first_session,
                        batch_seq: first_batch.batch_seq,
                        durability: ReplicationAckDurability::MemoryApplied,
                    })
                    .unwrap();
                common_wait_for_sender_report(&report, |state| {
                    state.in_flight == 1 && state.ack_durability_mismatch
                })
                .await;
                let session_frame = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
                    .await
                    .expect("ACK timeout did not rotate the session")
                    .expect("peer channel closed");
                let session_id = match Envelope::decode(session_frame.as_slice()).unwrap().msg {
                    Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
                    other => panic!("expected replacement SessionStart, got {other:?}"),
                };
                assert_ne!(session_id, first_session);
                session_id
            }
        };

        let replay = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("ordinary batch was not replayed")
            .expect("peer channel closed");
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(replay.session_id, replay_session);
        assert_eq!(replay.fragment, first_batch.fragment);
        assert_eq!(replay.required_ack_level, first_batch.required_ack_level);
        assert_eq!(replay.ops, first_batch.ops);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the replayed batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: replay_session,
                batch_seq: replay.batch_seq,
                durability: required_ack_durability,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("routine reconciliation was not resumed after replay ACK")
            .expect("peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, replay_session);
                assert_ne!(request.round_id, 0);
            }
            other => panic!("expected ReconcileRequest after replay ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reconnect_preserves_ordinary_nack_retry_floor() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
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
            other => panic!("expected SessionStart, got {other:?}"),
        };
        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "nack-floor".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 43, "node".to_string()),
                seq: 47,
                boot_id: 53,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), old_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("old peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, old_session);

        let nack_sent_at = Instant::now();
        sender
            .send(SenderMsg::Nack {
                session_id: old_session,
                batch_seq: first_batch.batch_seq,
                error: "injected rejection".to_string(),
            })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.buffered == 1 && state.in_flight == 0)
            .await;
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let early_deadline = nack_sent_at + Duration::from_millis(800);
        if Instant::now() < early_deadline {
            assert!(
                tokio::time::timeout_at(early_deadline, new_rx.recv())
                    .await
                    .is_err(),
                "reconnect bypassed the NACK retry floor"
            );
        }
        let replay = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("ordinary batch was not replayed after the retry floor")
            .expect("replacement peer channel closed");
        assert!(nack_sent_at.elapsed() >= Duration::from_millis(900));
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected replayed ReplicateBatch, got {other:?}"),
        };
        assert_eq!(replay.session_id, new_session);
        assert_eq!(replay.ops, first_batch.ops);
    }

    #[tokio::test]
    async fn reconnect_preserves_reconcile_timeout_retry_floor() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_millis(50),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (timed_out_tx, mut timed_out_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: timed_out_tx.clone(),
            })
            .unwrap();
        let initial_session = match Envelope::decode(timed_out_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };
        sender.send(SenderMsg::ReconcileNow).unwrap();
        let initial_round = match Envelope::decode(timed_out_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, initial_session);
                request.round_id
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        };

        let rotated = tokio::time::timeout(Duration::from_secs(1), timed_out_rx.recv())
            .await
            .expect("reconcile timeout did not rotate the session")
            .expect("peer channel closed");
        let rotated_session = match Envelope::decode(rotated.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected rotated SessionStart, got {other:?}"),
        };
        assert_ne!(rotated_session, initial_session);
        let retry_floor_observed_at = Instant::now();

        sender
            .send(SenderMsg::PeerDisconnected {
                peer_tx: timed_out_tx,
            })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;
        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, rotated_session);

        let early_deadline = retry_floor_observed_at + Duration::from_millis(800);
        if Instant::now() < early_deadline {
            assert!(
                tokio::time::timeout_at(early_deadline, new_rx.recv())
                    .await
                    .is_err(),
                "reconnect bypassed the reconcile retry floor"
            );
        }
        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("reconciliation was not retried after the retry floor")
            .expect("replacement peer channel closed");
        assert!(retry_floor_observed_at.elapsed() >= Duration::from_millis(900));
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, initial_round);
            }
            other => panic!("expected delayed ReconcileRequest, got {other:?}"),
        }
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
