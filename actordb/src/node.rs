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
    ReconcileFetch, ReconcileResponse, ReplicationAckLevel, SyncEntry,
};
use crate::replication::{
    spawn_node_sender, NodeSender, NodeSenderDrain, NodeSenderSendError, ReplicationAckDurability,
    SenderConfig, SenderDeps, SenderMsg, SenderRuntimeOptions, SenderStateReport,
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
pub(crate) mod recovery;
use recovery::RecoveryContext;
pub use recovery::RecoveryOptions;
pub mod scan;
pub use scan::{ScanError, MAX_SCAN_TOTAL_MATCHES};

/// Current peer wire protocol version. Hello ranges must include this value.
/// Version 4 carries independent physical/logical record timestamps. Older
/// peers cannot preserve their ordering and are rejected during negotiation.
/// It retains v3 byte-bounded, cursor-correlated reconciliation.
pub const WIRE_PROTOCOL_VERSION: u32 = 4;
/// The peer returns exact forwarded-mutation outcomes.
pub const CAPABILITY_MUTATION_RESULTS: u64 = 1 << 0;
/// Replication ACKs carry an explicit receiver durability fence.
pub const CAPABILITY_DURABILITY_ACKS: u64 = 1 << 1;
/// Reconciliation frames carry session/round/chunk correlation.
pub const CAPABILITY_CORRELATED_RECONCILIATION: u64 = 1 << 2;
/// The handshake uses independent fresh challenges and mutual transcript
/// proofs before publishing either side of a connection.
pub const CAPABILITY_CHALLENGE_AUTH: u64 = 1 << 3;
/// Anti-entropy uses stable cursor pages with exact encoded-byte bounds.
pub const CAPABILITY_PAGED_ANTI_ENTROPY: u64 = 1 << 4;
/// Aggregate batch requests and replies are exact-byte-bounded. Batch-get
/// replies may contain an ordered prefix that the caller resumes explicitly.
pub const CAPABILITY_BYTE_BOUNDED_BATCHES: u64 = 1 << 5;
/// Replication ACKs can report an explicit stable-storage fence independently
/// of ordinary store-commit policy.
pub const CAPABILITY_STABLE_DURABILITY_ACKS: u64 = 1 << 6;
/// Optional distributed scan support.
pub const CAPABILITY_SCAN: u64 = 1 << 7;
/// Optional revision-guarded, bounded SQL query pages.
pub const CAPABILITY_QUERY_PAGES: u64 = 1 << 8;
/// Forwarded writes carry explicit acknowledgement policy and progress.
pub const CAPABILITY_WRITE_CONCERNS: u64 = 1 << 9;
/// Summaries carry content digests and reject equal-version divergent contents.
pub const CAPABILITY_RECORD_CONTENT_DIGEST: u64 = 1 << 10;
/// Capabilities required by this binary before it installs a peer connection.
pub const REQUIRED_WIRE_CAPABILITIES: u64 = CAPABILITY_MUTATION_RESULTS
    | CAPABILITY_DURABILITY_ACKS
    | CAPABILITY_CORRELATED_RECONCILIATION
    | CAPABILITY_CHALLENGE_AUTH
    | CAPABILITY_PAGED_ANTI_ENTROPY
    | CAPABILITY_BYTE_BOUNDED_BATCHES
    | CAPABILITY_STABLE_DURABILITY_ACKS
    | CAPABILITY_WRITE_CONCERNS
    | CAPABILITY_RECORD_CONTENT_DIGEST;

/// Outbound queue depth per peer connection.
const PEER_CHANNEL_DEPTH: usize = 256;
/// Encoded bytes admitted to one peer's writer pipeline, including the frame
/// currently blocked on the socket.
const PEER_CHANNEL_BYTE_CAPACITY: usize = 32 * 1024 * 1024;
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
const RECEIVE_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
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

/// One live connection's bounded outbound queue plus a cancellation signal
/// shared by every producer and the socket reader/writer tasks.
#[derive(Clone)]
struct PeerChannel {
    outbound: BudgetedPeerTx,
    cancel: watch::Sender<bool>,
    queue_admission_timeout: Duration,
    rank: Option<ConnectionRank>,
    capabilities: u64,
}

struct ConnectionCancelGuard(PeerChannel);

impl Drop for ConnectionCancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl PeerChannel {
    fn same_channel(&self, other: &Self) -> bool {
        self.outbound.same_channel(&other.outbound)
    }

    fn same_sender(&self, other: &mpsc::Sender<Vec<u8>>) -> bool {
        self.outbound.identity.same_channel(other)
    }

    fn raw_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.outbound.identity.clone()
    }

    fn budgeted_sender(&self) -> BudgetedPeerTx {
        self.outbound.clone()
    }

    fn cancel(&self) {
        // Wake producers blocked on the encoded-byte gate immediately; this
        // budget belongs only to this physical connection and is never reused.
        self.outbound.close();
        // Cancellation is terminal, including for relays which subscribe after
        // the reader exited and the last previous receiver was dropped.
        self.cancel.send_replace(true);
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

/// Per-fragment fence used while a durable handoff has detached its exact
/// sender set but still needs the local actor to prove that no later mutation
/// was admitted. The sender-map lock orders fence installation against sender
/// creation; the synchronous registry then keeps unrelated fragments free of
/// the handoff's joins and actor RPC.
#[derive(Default)]
struct SenderFinalizationRegistry {
    next_token: AtomicU64,
    fragments: StdMutex<HashMap<FragmentId, u64>>,
}

impl SenderFinalizationRegistry {
    fn seal(self: &Arc<Self>, fragment: FragmentId) -> Option<SenderFinalizationGuard> {
        let mut fragments = self.fragments.lock().unwrap();
        if fragments.contains_key(&fragment) {
            return None;
        }
        let token = self
            .next_token
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        fragments.insert(fragment, token);
        Some(SenderFinalizationGuard {
            fragment,
            token,
            registry: self.clone(),
        })
    }

    fn is_sealed(&self, fragment: FragmentId) -> bool {
        self.fragments.lock().unwrap().contains_key(&fragment)
    }
}

struct SenderFinalizationGuard {
    fragment: FragmentId,
    token: u64,
    registry: Arc<SenderFinalizationRegistry>,
}

impl Drop for SenderFinalizationGuard {
    fn drop(&mut self) {
        let mut fragments = self.registry.fragments.lock().unwrap();
        if fragments.get(&self.fragment) == Some(&self.token) {
            fragments.remove(&self.fragment);
        }
    }
}

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
type RelayMap = Arc<StdMutex<HashMap<u64, GetRelay>>>;
/// Relayed forwarded mutations: relay req_id -> (origin peer_tx, origin req_id).
type MutationRelayMap = Arc<StdMutex<HashMap<u64, MutationRelay>>>;

struct SenderEntryDeps<'a> {
    config: &'a SenderConfig,
    runtime: &'a SenderRuntimeOptions,
    router: &'a FragmentRouter,
    hlc: &'a Arc<Mutex<HlcClock>>,
    senders: &'a SenderMap,
    finalizing_senders: &'a Arc<SenderFinalizationRegistry>,
    retiring_senders: &'a Arc<RetiringSenderRegistry>,
    reports: &'a SenderReportMap,
    peer_failures: &'a PeerFailureSignals,
    operation_gate: &'a Arc<OperationGate>,
    allow_quiescing: bool,
}

/// Atomically install the one replication sender allowed to own a
/// `(fragment, peer)` session. Placement and connection setup can race; a
/// check-then-spawn sequence would orphan one sender after it had already
/// emitted SessionStart or accepted mutations.
async fn ensure_sender_entry(fragment: FragmentId, peer: &str, deps: SenderEntryDeps<'_>) {
    let key = (fragment, peer.to_string());
    let mut senders_guard = deps.senders.lock().await;
    if !deps.allow_quiescing && !deps.operation_gate.is_running() {
        return;
    }
    if deps.finalizing_senders.is_sealed(fragment) {
        return;
    }
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
        retained_bytes: 0,
        aggregate_retained_bytes: 0,
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
        SenderDeps::builder()
            .router(deps.router.clone())
            .hlc(deps.hlc.clone())
            .state_report(report.clone())
            .build(),
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
    expected_tx: PeerChannel,
    expected_keys: Vec<String>,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    timeout_cancel: oneshot::Sender<()>,
    /// Keep shutdown admission open until the downstream attempt reaches a
    /// response, timeout, or send failure. Response bytes may still be lost
    /// after enqueue; the fence covers mutation completion, not socket flush.
    _operation: OperationPermit,
    _admission: Arc<RelayLease>,
}

struct MutationRelayRegistration {
    expected_peer: String,
    expected_tx: PeerChannel,
    expected_keys: Vec<String>,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    deadline: Instant,
    operation: OperationPermit,
}

struct GetRelay {
    expected_peer: String,
    expected_tx: PeerChannel,
    expected_key: String,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    timeout_cancel: oneshot::Sender<()>,
    _admission: Arc<RelayLease>,
}

struct GetRelayRegistration {
    expected_peer: String,
    expected_tx: PeerChannel,
    expected_key: String,
    origin_tx: PeerChannel,
    origin_req_id: u64,
    deadline: Instant,
}

struct RelayAdmission {
    slots: Arc<Semaphore>,
    key_bytes: Arc<Semaphore>,
}

struct RelayLease {
    _slot: OwnedSemaphorePermit,
    _keys: OwnedSemaphorePermit,
}

impl RelayAdmission {
    fn new(capacity: usize, key_byte_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            key_bytes: Arc::new(Semaphore::new(key_byte_capacity)),
        })
    }

    fn try_reserve(&self, key_bytes: usize) -> Option<Arc<RelayLease>> {
        let key_bytes = u32::try_from(key_bytes).ok()?;
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let keys = self
            .key_bytes
            .clone()
            .try_acquire_many_owned(key_bytes)
            .ok()?;
        Some(Arc::new(RelayLease {
            _slot: slot,
            _keys: keys,
        }))
    }
}

fn relay_key_storage_bytes(capacity: usize) -> usize {
    std::mem::size_of::<String>().saturating_add(capacity)
}

fn relay_keys_storage_bytes(keys: &[String], capacity: usize) -> usize {
    keys.iter().fold(
        std::mem::size_of::<Vec<String>>()
            .saturating_add(capacity.saturating_mul(std::mem::size_of::<String>())),
        |bytes, key| bytes.saturating_add(key.capacity()),
    )
}

enum RelayRegistry {
    Get(RelayMap),
    Mutation(MutationRelayMap),
}

/// Synchronous rollback covers timer registration and outbound admission.
/// The timer also owns this lease, so its lifetime cannot outlive admission.
struct RelayRegistrationGuard {
    registry: RelayRegistry,
    relay_id: u64,
    admission: Arc<RelayLease>,
    origin_tx: PeerChannel,
    armed: bool,
}

impl RelayRegistrationGuard {
    fn disarm(mut self) {
        self.armed = false;
    }

    async fn enqueue(&self, owner_tx: &PeerChannel, envelope: Envelope, deadline: Instant) -> bool {
        let mut owner_cancel = owner_tx.cancel.subscribe();
        let mut origin_cancel = self.origin_tx.cancel.subscribe();
        if *owner_cancel.borrow() || *origin_cancel.borrow() {
            return false;
        }
        tokio::select! {
            _ = owner_cancel.changed() => false,
            _ = origin_cancel.changed() => false,
            sent = try_send_envelope_until(owner_tx, envelope, deadline) => sent,
        }
    }
}

impl Drop for RelayRegistrationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match &self.registry {
            RelayRegistry::Get(relays) => {
                drop(remove_get_relay(relays, self.relay_id, &self.admission));
            }
            RelayRegistry::Mutation(relays) => {
                if let Some(relay) = remove_mutation_relay(relays, self.relay_id, &self.admission) {
                    fail_mutation_relay(relay, "mutation relay canceled before enqueue");
                }
            }
        }
    }
}

fn remove_get_relay(relays: &RelayMap, id: u64, admission: &Arc<RelayLease>) -> Option<GetRelay> {
    let mut relays = relays.lock().unwrap();
    if relays
        .get(&id)
        .is_some_and(|relay| Arc::ptr_eq(&relay._admission, admission))
    {
        relays.remove(&id)
    } else {
        None
    }
}

fn remove_mutation_relay(
    relays: &MutationRelayMap,
    id: u64,
    admission: &Arc<RelayLease>,
) -> Option<MutationRelay> {
    let mut relays = relays.lock().unwrap();
    if relays
        .get(&id)
        .is_some_and(|relay| Arc::ptr_eq(&relay._admission, admission))
    {
        relays.remove(&id)
    } else {
        None
    }
}

fn fail_mutation_relay(relay: MutationRelay, reason: &str) {
    let MutationRelay {
        origin_tx,
        origin_req_id,
        expected_keys,
        timeout_cancel,
        _operation,
        _admission,
        ..
    } = relay;
    let _ = timeout_cancel.send(());
    if !*origin_tx.cancel.borrow() {
        let _ = send_unavailable_mutation(&origin_tx, origin_req_id, expected_keys, reason);
    }
}

fn cancel_connection_relays(channel: &PeerChannel, reads: &RelayMap, mutations: &MutationRelayMap) {
    let reads = {
        let mut reads = reads.lock().unwrap();
        let ids: Vec<_> = reads
            .iter()
            .filter_map(|(id, relay)| {
                (relay.origin_tx.same_channel(channel) || relay.expected_tx.same_channel(channel))
                    .then_some(*id)
            })
            .collect();
        ids.into_iter()
            .filter_map(|id| reads.remove(&id))
            .collect::<Vec<_>>()
    };
    drop(reads);
    let mutations = {
        let mut mutations = mutations.lock().unwrap();
        let ids: Vec<_> = mutations
            .iter()
            .filter_map(|(id, relay)| {
                (relay.origin_tx.same_channel(channel) || relay.expected_tx.same_channel(channel))
                    .then_some(*id)
            })
            .collect();
        ids.into_iter()
            .filter_map(|id| mutations.remove(&id))
            .collect::<Vec<_>>()
    };
    for relay in mutations {
        fail_mutation_relay(relay, "mutation relay connection closed");
    }
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
/// Pending distributed scan requests: req_id -> reply channel. The expected
/// responder is retained so a colliding reply cannot complete another
/// peer's scan.
type PendingScans = Arc<StdMutex<HashMap<u64, PendingScan>>>;

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

struct PendingScan {
    expected_peer: String,
    reply: oneshot::Sender<Result<Vec<crate::proto::ScanMatchProto>, String>>,
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

const NODE_RUNNING: u8 = 0;
const NODE_QUIESCING: u8 = 1;
const NODE_STOPPED: u8 = 2;

/// One-way admission fence for public Node operations. Admission is a
/// lock-free check/increment/recheck sequence, so shutdown cannot miss a
/// racing operation and a cancelled operation releases its slot by RAII.
struct OperationGate {
    phase: AtomicU8,
    active: AtomicUsize,
    drained: Notify,
}

impl OperationGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            phase: AtomicU8::new(NODE_RUNNING),
            active: AtomicUsize::new(0),
            drained: Notify::new(),
        })
    }

    fn try_enter(self: &Arc<Self>) -> Result<OperationPermit, NodeError> {
        if self.phase.load(Ordering::SeqCst) != NODE_RUNNING {
            return Err(NodeError::ShuttingDown);
        }
        self.finish_enter_after_running_check()
    }

    fn is_running(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == NODE_RUNNING
    }

    /// Complete admission after observing `NODE_RUNNING`. Shutdown may race
    /// that first observation, so publish the operation and then recheck the
    /// phase before allowing it to proceed.
    fn finish_enter_after_running_check(self: &Arc<Self>) -> Result<OperationPermit, NodeError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.phase.load(Ordering::SeqCst) == NODE_RUNNING {
            return Ok(OperationPermit { gate: self.clone() });
        }
        self.release();
        Err(NodeError::ShuttingDown)
    }

    fn begin_shutdown(&self) {
        let _ = self.phase.compare_exchange(
            NODE_RUNNING,
            NODE_QUIESCING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    async fn wait_for_drain(&self) {
        loop {
            // Register before observing the count so the final release cannot
            // slip between the check and the wait.
            let drained = self.drained.notified();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            drained.await;
        }
    }

    fn finish_shutdown(&self) {
        self.phase.store(NODE_STOPPED, Ordering::SeqCst);
        self.drained.notify_waiters();
    }

    fn release(&self) {
        if self.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.drained.notify_waiters();
        }
    }
}

struct OperationPermit {
    gate: Arc<OperationGate>,
}

impl Drop for OperationPermit {
    fn drop(&mut self) {
        self.gate.release();
    }
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
#[cfg(test)]
#[derive(Clone)]
struct SenderSnapshotHook {
    captured: Arc<Notify>,
    resume: Arc<Notify>,
}

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
    /// Whether placement lifecycles are controller-driven. Both modes use
    /// the same protocol-v3 reconciler for anti-entropy.
    dynamic_placement: bool,
    pending_gets: PendingGets,
    pending_batch_gets: PendingBatchGets,
    /// In-flight distributed scan requests (coordinator side).
    pending_scans: PendingScans,
    pending_query_pages: query::PendingQueryPages,
    query_admission: Arc<crate::query::QueryAdmission>,
    pending_mutations: PendingMutations,
    /// Relayed forwarded-gets: relay_req_id -> (origin peer_tx, origin req_id).
    pending_relays: RelayMap,
    pending_mutation_relays: MutationRelayMap,
    next_req_id: Arc<AtomicU64>,
    peers: PeerMap,
    peer_failures: PeerFailureSignals,
    tasks: Weak<RuntimeTasks>,
    read_service: Arc<ReadService>,
    operation_gate: Arc<OperationGate>,
    /// Monotonic wake-up signal for dial supervisors waiting behind another
    /// physical connection to the same authenticated peer.
    peer_changes: watch::Sender<u64>,
    hlc: Arc<Mutex<HlcClock>>,
    senders: SenderMap,
    finalizing_senders: Arc<SenderFinalizationRegistry>,
    retiring_senders: Arc<RetiringSenderRegistry>,
    sender_config: SenderConfig,
    sender_runtime: SenderRuntimeOptions,
    runtime_options: NodeRuntimeOptions,
    receive_byte_budget: Arc<Semaphore>,
    node_peer_queue_budget: Arc<NodePeerByteBudget>,
    relay_admission: Arc<RelayAdmission>,
    mutation_service: Arc<MutationService>,
    sender_reports: SenderReportMap,
    /// Notify the placement driver on peer join/leave. `None` in
    /// static mode.
    membership_tx: Option<watch::Sender<u64>>,
    /// Pre-shared cluster secret for fresh, challenge-bound mutual proofs.
    /// This proves group-key possession, not individual identity, and does not
    /// protect post-handshake plaintext frames. `None` = no membership proof.
    cluster_secret: Option<Vec<u8>>,
    #[cfg(test)]
    sender_snapshot_hook: Option<SenderSnapshotHook>,
    recovery: Option<Arc<RecoveryContext>>,
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
    actors: Arc<RwLock<HashMap<FragmentId, FragmentRoute>>>,
    next_generation: Arc<AtomicU64>,
    fragment_count: usize,
    /// A mutation holds a shared lease from its serving decision through
    /// apply and replication fan-out. Placement revocation takes the exclusive
    /// lease before removing `serving`, closing the apply/enqueue teardown race.
    mutation_gates: Arc<Vec<Arc<AsyncRwLock<()>>>>,
}

#[derive(Clone)]
struct FragmentRoute {
    generation: u64,
    actor: ActorRef<KvMessage>,
}

/// Remove a newly published dynamic route if cancellation wins before its
/// dispatcher monitor is durably registered with the core task manager.
struct RouteMonitorRegistrationGuard {
    router: FragmentRouter,
    fragment: FragmentId,
    generation: u64,
    armed: bool,
}

impl RouteMonitorRegistrationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RouteMonitorRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.router
                .remove_if_generation(self.fragment, self.generation);
        }
    }
}

impl FragmentRouter {
    pub(crate) fn new(fragment_count: usize, actor_capacity: usize) -> Self {
        Self {
            actors: Arc::new(RwLock::new(HashMap::with_capacity(actor_capacity))),
            next_generation: Arc::new(AtomicU64::new(1)),
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
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .map(|route| route.actor.clone())
    }

    fn route_with_generation(&self, fragment: FragmentId) -> Option<(u64, ActorRef<KvMessage>)> {
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .map(|route| (route.generation, route.actor.clone()))
    }

    pub(crate) fn key_belongs_to(&self, fragment: FragmentId, key: &str) -> bool {
        fragment_of(key, self.fragment_count) == fragment
    }

    fn fragment_ids(&self) -> Vec<u32> {
        self.actors.read().unwrap().keys().map(|f| f.0).collect()
    }

    /// Total fragments in the keyspace (`0..fragment_count`), whether or
    /// not this node serves them.
    pub(crate) fn fragment_count(&self) -> usize {
        self.fragment_count
    }

    /// Add (or replace) the actor for `fragment`.
    fn insert(&self, fragment: FragmentId, actor: ActorRef<KvMessage>) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.actors
            .write()
            .unwrap()
            .insert(fragment, FragmentRoute { generation, actor });
        generation
    }

    /// Remove the actor for `fragment`.
    fn remove(&self, fragment: FragmentId) {
        self.actors.write().unwrap().remove(&fragment);
    }

    /// Publish a supervisor replacement only while the exact subtree route
    /// that installed `expected` is still current. Placement may remove and
    /// re-add the same fragment while an old monitor is waking up; the
    /// generation check prevents that stale monitor from overwriting the new
    /// subtree (including an actor-id ABA after a very long process lifetime).
    fn replace_if_current(
        &self,
        fragment: FragmentId,
        generation: u64,
        expected: &ActorRef<KvMessage>,
        replacement: ActorRef<KvMessage>,
    ) -> bool {
        let mut actors = self.actors.write().unwrap();
        let Some(route) = actors.get_mut(&fragment) else {
            return false;
        };
        if route.generation != generation || route.actor.get_id() != expected.get_id() {
            return false;
        }
        route.actor = replacement;
        true
    }

    fn is_current(
        &self,
        fragment: FragmentId,
        generation: u64,
        expected: &ActorRef<KvMessage>,
    ) -> bool {
        self.actors
            .read()
            .unwrap()
            .get(&fragment)
            .is_some_and(|route| {
                route.generation == generation && route.actor.get_id() == expected.get_id()
            })
    }

    fn remove_if_generation(&self, fragment: FragmentId, generation: u64) {
        let mut actors = self.actors.write().unwrap();
        if actors
            .get(&fragment)
            .is_some_and(|route| route.generation == generation)
        {
            actors.remove(&fragment);
        }
    }

    #[cfg(test)]
    async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        self.get_all_retained(None).await
    }

    async fn get_all_retained(
        &self,
        retention: Option<Arc<dyn Send + Sync>>,
    ) -> Result<HashMap<String, VersionedValue>, NodeError> {
        // Clone the refs first; don't hold the lock across awaits.
        let actors: Vec<ActorRef<KvMessage>> = self
            .actors
            .read()
            .unwrap()
            .values()
            .map(|route| route.actor.clone())
            .collect();
        let mut all = HashMap::new();
        for actor in actors {
            let res = actor
                .call(
                    |reply| KvMessage::GetAll { reply }.with_optional_retention(retention.clone()),
                    Some(Duration::from_secs(30)),
                )
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
        CallResult::Success(Err(ApplyError::InvalidInput(error))) => Err(NodeError::Codec(error)),
        CallResult::Success(Err(error @ ApplyError::VersionConflict { .. })) => {
            Err(NodeError::Codec(error.to_string()))
        }
        CallResult::Success(Err(ApplyError::Storage(error))) => Err(NodeError::Storage(error)),
        CallResult::Timeout => Err(NodeError::Actor("apply timed out".to_string())),
        CallResult::SenderError => Err(NodeError::Actor("apply reply channel dropped".to_string())),
    }
}

async fn flush_fence(
    actor: &ActorRef<KvMessage>,
    required: ReplicationAckDurability,
) -> Result<ApplyDurability, NodeError> {
    let result = match required {
        ReplicationAckDurability::StableCommitted => {
            actor
                .call(
                    |reply| KvMessage::FlushStable { reply },
                    Some(Duration::from_secs(30)),
                )
                .await
        }
        ReplicationAckDurability::MemoryApplied | ReplicationAckDurability::StoreCommitted => {
            actor
                .call(
                    |reply| KvMessage::Flush { reply },
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

fn validate_legacy_get_response(
    expected_key: &str,
    response: &GetResponse,
) -> Result<(), NodeError> {
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
) -> MutationResult {
    let key = op.key.clone();
    if let Err(error) = validate_replicated_op(&op) {
        return mutation_result(key, request_index, Err(error));
    }
    let result = match call_apply(actor, op.clone(), priority, false).await {
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
    pending_scans: PendingScans,
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
    /// Compatibility counter for the retired full-state sync protocol.
    /// Protocol v3 leaves it at zero; sender reconciliation metrics report
    /// anti-entropy progress.
    sync_entries_applied: Arc<AtomicU64>,
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
    shutdown_state: Mutex<ShutdownProgress>,
    replication_drain_report: StdMutex<Option<ReplicationDrainReport>>,
    network: Arc<dyn Network>,
    ttl_expiry: Option<ExpiryRuntime>,
    /// Exclusive live attachment to the configured store. Clean shutdown
    /// releases it only after every fragment actor has stopped; cancellation
    /// and best-effort Drop leave managed backends fail-closed.
    store_binding: StdMutex<Option<StoreBinding>>,
    recovery: Option<Arc<RecoveryContext>>,
}

enum ShutdownProgress {
    Running,
    FencingReplication,
    DrainingReplication(Vec<ReplicationDrainWaiter>),
    StoppingTransport,
    QuiescingFragments,
    StoppingFragments,
    Stopped,
}

struct ReplicationDrainWaiter {
    target: ReplicationDrainTarget,
    sender: NodeSender,
    drain: NodeSenderDrain,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(expiry) = &self.ttl_expiry {
            expiry.request_abort();
        }
        // Async cleanup still belongs to `shutdown`, but never leave task
        // admission open merely because a caller dropped the handle.
        self.operation_gate.begin_shutdown();
        self.mutation_service.request_shutdown();
        self.embedded_operations.request_shutdown();
        self.placement_tasks.request_shutdown();
        self.tasks.request_shutdown();
        if let Ok(senders) = self.senders.try_lock() {
            for sender in senders.values() {
                sender.request_stop();
            }
        }
        self.retiring_senders.request_stop_all();
        self.supervisor_stops.stop_all();
        let store_binding = match self.store_binding.get_mut() {
            Ok(binding) => binding,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(binding) = store_binding.as_mut() {
            binding.abandon();
        }
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

#[cfg(test)]
async fn run_peer_writer<W>(
    writer: W,
    rx: mpsc::Receiver<BudgetedFrame>,
    byte_budget: Arc<Semaphore>,
    cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let (response_tx, response_rx) = mpsc::channel(1);
    drop(response_tx);
    let producer_byte_budget = Arc::new(Semaphore::new(byte_budget.available_permits()));
    run_bounded_peer_writer(
        writer,
        PeerWriterPipeline {
            data_rx: rx,
            response_rx,
            byte_budget,
            producer_byte_budget,
        },
        cancel_rx,
        cancel_tx,
        socket_write_timeout,
    )
    .await;
}

struct PeerWriterExitGuard {
    byte_budget: Arc<Semaphore>,
    producer_byte_budget: Arc<Semaphore>,
    cancel: watch::Sender<bool>,
}

struct PeerWriterPipeline {
    data_rx: mpsc::Receiver<BudgetedFrame>,
    response_rx: mpsc::Receiver<BudgetedFrame>,
    byte_budget: Arc<Semaphore>,
    producer_byte_budget: Arc<Semaphore>,
}

impl Drop for PeerWriterExitGuard {
    fn drop(&mut self) {
        self.byte_budget.close();
        self.producer_byte_budget.close();
        let _ = self.cancel.send(true);
    }
}

async fn run_bounded_peer_writer<W>(
    writer: W,
    pipeline: PeerWriterPipeline,
    cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let PeerWriterPipeline {
        data_rx,
        response_rx,
        byte_budget,
        producer_byte_budget,
    } = pipeline;
    let _exit_guard = PeerWriterExitGuard {
        byte_budget,
        producer_byte_budget,
        cancel: cancel_tx.clone(),
    };
    run_queued_peer_writer(
        writer,
        data_rx,
        response_rx,
        cancel_rx,
        cancel_tx.clone(),
        socket_write_timeout,
    )
    .await;
    let _ = cancel_tx.send(true);
}

async fn run_queued_peer_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<BudgetedFrame>,
    mut response_rx: mpsc::Receiver<BudgetedFrame>,
    mut cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let mut data_open = true;
    let mut response_open = true;
    let mut response_burst = 0usize;
    'writer: while data_open || response_open {
        let (frame, is_response) = loop {
            if !data_open && !response_open {
                break 'writer;
            }
            if *cancel_rx.borrow() {
                break 'writer;
            }
            if data_open && response_burst >= PEER_RESPONSE_BURST_LIMIT {
                match rx.try_recv() {
                    Ok(frame) => break (frame, false),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => data_open = false,
                }
            }
            tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    let _ = changed;
                    break 'writer;
                }
                frame = response_rx.recv(), if response_open => {
                    match frame {
                        Some(frame) => break (frame, true),
                        None => response_open = false,
                    }
                }
                frame = rx.recv(), if data_open => {
                    match frame {
                        Some(frame) => break (frame, false),
                        None => data_open = false,
                    }
                }
            }
        };
        if is_response {
            response_burst = response_burst.saturating_add(1);
        } else {
            response_burst = 0;
        }
        if frame.len() > MAX_FRAME_BYTES || u32::try_from(frame.len()).is_err() {
            tracing::warn!(
                encoded_len = frame.len(),
                "closing peer transport before writing an oversized frame"
            );
            break;
        }
        let write = async {
            writer.write_u32(frame.len() as u32).await?;
            writer.write_all(frame.bytes()).await?;
            writer.flush().await
        };
        let result = tokio::select! {
            changed = cancel_rx.changed() => {
                let _ = changed;
                None
            }
            result = tokio::time::timeout(socket_write_timeout, write) => Some(result),
        };
        if !matches!(result, Some(Ok(Ok(())))) {
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
    expiry_publisher: Option<ExpiryPublisher>,
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
    let (supervisor, startup_handle) = ActorRuntime::<FragmentSupervisor>::spawn_instant(
        None,
        FragmentSupervisor,
        SupervisorArgs::builder()
            .fragment(fragment)
            .worker_prefix(worker_prefix)
            .maybe_store(store)
            .hlc(hlc.clone())
            .default_pool(config.default_pool.clone())
            .high_pool(config.high_pool.clone())
            .background_pool(config.background_pool.clone())
            .flush_batch_size(config.flush_batch_size)
            .persistence(config.persistence.clone())
            .flush_interval(config.flush_interval)
            .clean_interval(config.clean_interval)
            .snapshot_interval(config.snapshot_interval)
            .build()
            .with_expiry_publisher(expiry_publisher),
    )
    .map_err(|e| NodeError::Actor(e.to_string()))?;

    // Publish ownership before the first await. Dynamic placement tasks are
    // aborted at the start of Node shutdown; the independent startup observer
    // keeps pre_start alive and makes its completion (including failed-start
    // child cleanup) visible to the synchronous supervisor registry.
    let (startup_tx, startup_rx) = watch::channel(SupervisorStartupState::Starting);
    registry.insert(fragment, supervisor.clone(), startup_rx.clone());
    let startup_supervisor = supervisor.clone();
    drop(tokio::spawn(async move {
        let state = match startup_handle.await {
            Ok(Ok(_lifecycle_handle)) => SupervisorStartupState::Ready,
            Ok(Err(error)) => {
                cleanup_failed_supervisor_start(&startup_supervisor).await;
                SupervisorStartupState::Failed(SupervisorStartupFailure::from_spawn(error))
            }
            Err(error) => {
                cleanup_failed_supervisor_start(&startup_supervisor).await;
                SupervisorStartupState::Failed(SupervisorStartupFailure::Actor(format!(
                    "startup task failed: {error}"
                )))
            }
        };
        startup_tx.send_replace(state);
    }));

    match wait_for_supervisor_startup(startup_rx).await {
        Ok(SupervisorStartupState::Ready) => {}
        Ok(SupervisorStartupState::Failed(error)) => {
            registry.remove_if(fragment, &supervisor);
            return Err(error.into_node_error());
        }
        Ok(SupervisorStartupState::Starting) => unreachable!("startup wait returned pending"),
        Err(error) => {
            return Err(NodeError::Actor(error));
        }
    }
    let dispatcher_result = supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await;
    let dispatcher = match dispatcher_result {
        Ok(CallResult::Success(dispatcher)) => dispatcher,
        Ok(_) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove_if(fragment, &supervisor);
            return Err(NodeError::Actor(
                "supervisor dropped dispatcher reply".to_string(),
            ));
        }
        Err(error) => {
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            registry.remove_if(fragment, &supervisor);
            return Err(NodeError::Actor(error.to_string()));
        }
    };
    Ok((supervisor, dispatcher))
}

/// Supervisor pre_start performs the primary recursive rollback. This
/// observer-side sweep covers a framework-level startup failure outside that
/// hook and publishes failure only after any still-linked children exit.
async fn cleanup_failed_supervisor_start(supervisor: &ActorRef<SupervisorMessage>) {
    let children = supervisor.get_children();
    for child in &children {
        let _ = child.drain();
    }
    for child in children {
        let _ = child.wait(None).await;
    }
}

/// Keep the router pointed at the dispatcher currently owned by one fragment
/// supervisor. The supervisor is the lifecycle authority and already replaces
/// a failed dispatcher in place; this monitor republishes that replacement to
/// every local and replication call site which resolves through the router.
///
/// Route generation plus actor identity make publication ABA-safe when a
/// dynamic placement loss/re-add races the old subtree's final failure event.
async fn monitor_fragment_dispatcher(
    fragment: FragmentId,
    generation: u64,
    supervisor: ActorRef<SupervisorMessage>,
    router: FragmentRouter,
    mut current: ActorRef<KvMessage>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() || !router.is_current(fragment, generation, &current) {
            return;
        }

        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return;
            }
            _ = current.wait(None) => {}
        }

        let replacement = loop {
            if *shutdown.borrow() || !router.is_current(fragment, generation, &current) {
                return;
            }
            match supervisor
                .call(
                    |reply| SupervisorMessage::GetDispatcher { reply },
                    Some(RECONNECT_BASE),
                )
                .await
            {
                Ok(CallResult::Success(candidate)) if candidate.get_id() != current.get_id() => {
                    break candidate;
                }
                Ok(CallResult::Success(_)) | Ok(CallResult::Timeout) => {}
                Ok(CallResult::SenderError) | Err(_) => return,
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return;
                }
                _ = tokio::time::sleep(RECONNECT_BASE) => {}
            }
        };

        if !router.replace_if_current(fragment, generation, &current, replacement.clone()) {
            return;
        }
        tracing::warn!(
            fragment = fragment.0,
            previous_dispatcher = ?current.get_id(),
            replacement_dispatcher = ?replacement.get_id(),
            "republished restarted fragment dispatcher"
        );
        current = replacement;
    }
}

async fn register_fragment_dispatcher_monitor(
    tasks: &Arc<RuntimeTasks>,
    fragment: FragmentId,
    generation: u64,
    supervisor: ActorRef<SupervisorMessage>,
    router: FragmentRouter,
    dispatcher: ActorRef<KvMessage>,
) -> bool {
    let shutdown = tasks.shutdown_signal();
    tasks
        .spawn(monitor_fragment_dispatcher(
            fragment, generation, supervisor, router, dispatcher, shutdown,
        ))
        .await
}

#[cfg(test)]
async fn spawn_fragment_subtree(
    node_id: &str,
    fragment: FragmentId,
    config: &NodeConfig,
    hlc: &Arc<Mutex<HlcClock>>,
) -> Result<(ActorRef<SupervisorMessage>, ActorRef<KvMessage>), NodeError> {
    let registry = Arc::new(SupervisorRegistry::new(1));
    let result =
        spawn_fragment_subtree_registered(node_id, fragment, config, hlc, &registry, None).await;
    if let Ok((supervisor, _)) = &result {
        registry.remove_if(fragment, supervisor);
    }
    result
}

/// Synchronous ownership of every self-running fragment supervisor. Its Drop
/// implementation closes the late-publication race when a Node is dropped
/// while a placement task is still unwinding.
struct SupervisorRegistry {
    supervisors: StdMutex<HashMap<FragmentId, Vec<RegisteredSupervisor>>>,
}

#[derive(Clone)]
struct RegisteredSupervisor {
    actor: ActorRef<SupervisorMessage>,
    prepared: bool,
    startup: watch::Receiver<SupervisorStartupState>,
}

#[derive(Clone, Debug)]
enum SupervisorStartupState {
    Starting,
    Ready,
    Failed(SupervisorStartupFailure),
}

#[derive(Clone, Debug)]
enum SupervisorStartupFailure {
    Actor(String),
    Storage(String),
}

impl SupervisorStartupFailure {
    fn from_spawn(error: SpawnErr) -> Self {
        match error {
            SpawnErr::StartupFailed(error) => {
                match error.downcast::<FragmentStartupStorageError>() {
                    Ok(error) => Self::Storage(error.to_string()),
                    Err(error) => Self::Actor(SpawnErr::StartupFailed(error).to_string()),
                }
            }
            error => Self::Actor(error.to_string()),
        }
    }

    fn into_node_error(self) -> NodeError {
        match self {
            Self::Actor(error) => NodeError::Actor(error),
            Self::Storage(error) => NodeError::Storage(error),
        }
    }
}

impl std::fmt::Display for SupervisorStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actor(error) | Self::Storage(error) => f.write_str(error),
        }
    }
}

async fn wait_for_supervisor_startup(
    mut startup: watch::Receiver<SupervisorStartupState>,
) -> Result<SupervisorStartupState, String> {
    loop {
        let state = startup.borrow().clone();
        if !matches!(state, SupervisorStartupState::Starting) {
            return Ok(state);
        }
        startup
            .changed()
            .await
            .map_err(|_| "supervisor startup observer exited before completion".to_string())?;
    }
}

impl SupervisorRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            supervisors: StdMutex::new(HashMap::with_capacity(capacity)),
        }
    }

    fn insert(
        &self,
        fragment: FragmentId,
        supervisor: ActorRef<SupervisorMessage>,
        startup: watch::Receiver<SupervisorStartupState>,
    ) {
        self.supervisors
            .lock()
            .unwrap()
            .entry(fragment)
            .or_default()
            .push(RegisteredSupervisor {
                actor: supervisor,
                prepared: false,
                startup,
            });
    }

    #[cfg(test)]
    fn insert_ready(&self, fragment: FragmentId, supervisor: ActorRef<SupervisorMessage>) {
        let (_startup_tx, startup_rx) = watch::channel(SupervisorStartupState::Ready);
        self.insert(fragment, supervisor, startup_rx);
    }

    fn get(&self, fragment: FragmentId) -> Option<RegisteredSupervisor> {
        self.supervisors
            .lock()
            .unwrap()
            .get(&fragment)
            .and_then(|entries| entries.last())
            .cloned()
    }

    fn remove_if(&self, fragment: FragmentId, supervisor: &ActorRef<SupervisorMessage>) -> bool {
        let mut supervisors = self.supervisors.lock().unwrap();
        let mut removed = false;
        let mut empty = false;
        if let Some(entries) = supervisors.get_mut(&fragment) {
            let before = entries.len();
            entries.retain(|entry| entry.actor.get_id() != supervisor.get_id());
            removed = entries.len() != before;
            empty = entries.is_empty();
        }
        if empty {
            supervisors.remove(&fragment);
        }
        removed
    }

    fn mark_prepared(
        &self,
        fragment: FragmentId,
        supervisor: &ActorRef<SupervisorMessage>,
    ) -> bool {
        let mut supervisors = self.supervisors.lock().unwrap();
        let Some(entries) = supervisors.get_mut(&fragment) else {
            return false;
        };
        let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.actor.get_id() == supervisor.get_id())
        else {
            return false;
        };
        entry.prepared = true;
        true
    }

    fn snapshot(&self) -> Vec<(FragmentId, RegisteredSupervisor)> {
        self.supervisors
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(fragment, entries)| entries.iter().cloned().map(|entry| (*fragment, entry)))
            .collect()
    }

    fn stop_all(&self) {
        for entries in self.supervisors.lock().unwrap().values() {
            for entry in entries {
                entry.actor.stop(None);
            }
        }
    }

    fn clear(&self) {
        self.supervisors.lock().unwrap().clear();
    }

    async fn stop_and_join_all(&self) {
        let supervisors = self.snapshot();
        for (_, entry) in &supervisors {
            entry.actor.stop(None);
        }
        for (_, entry) in supervisors {
            // pre_start may still own child actors. Its independent observer
            // publishes completion only after failed-start child cleanup.
            let _ = wait_for_supervisor_startup(entry.startup.clone()).await;
            entry.actor.stop(None);
            let _ = entry.actor.wait(None).await;
        }
        self.clear();
    }
}

impl Drop for SupervisorRegistry {
    fn drop(&mut self) {
        for entries in self.supervisors.get_mut().unwrap().values() {
            for entry in entries {
                entry.actor.stop(None);
            }
        }
    }
}

async fn quiesce_fragment_supervisor(
    supervisor: &ActorRef<SupervisorMessage>,
) -> Result<(), FragmentShutdownError> {
    let result = supervisor
        .call(|reply| SupervisorMessage::Quiesce { reply }, None)
        .await
        .map_err(|error| {
            FragmentShutdownError::Actor(format!("quiesce request failed: {error}"))
        })?;
    match result {
        CallResult::Success(result) => result,
        CallResult::Timeout => Err(FragmentShutdownError::Actor(
            "quiesce timed out".to_string(),
        )),
        CallResult::SenderError => Err(FragmentShutdownError::Actor(
            "quiesce reply channel dropped".to_string(),
        )),
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

/// Owns a store binding until startup either transfers it to [`Node`] or
/// proves that every partially created store user has exited.
struct StoreBindingStartupGuard {
    binding: Option<StoreBinding>,
    actors_may_exist: bool,
}

impl StoreBindingStartupGuard {
    fn new(binding: Option<StoreBinding>) -> Self {
        Self {
            binding,
            actors_may_exist: false,
        }
    }

    fn mark_actors_may_exist(&mut self) {
        self.actors_may_exist = true;
    }

    fn allow_release_after_join(&mut self) {
        self.actors_may_exist = false;
    }

    fn transfer_to_node(&mut self) -> Option<StoreBinding> {
        self.actors_may_exist = false;
        self.binding.take()
    }
}

impl Drop for StoreBindingStartupGuard {
    fn drop(&mut self) {
        if self.actors_may_exist {
            if let Some(binding) = self.binding.as_mut() {
                binding.abandon();
            }
        }
    }
}

/// Runtime liveness, queue policy, and region configuration for a [`Node`].
///
/// Configure runtime policies through this type’s builder or setters.
/// [`Node::start`] uses the defaults; pass an override to
/// [`Node::start_with_options`].
#[derive(Clone, Debug, bon::Builder)]
pub struct NodeRuntimeOptions {
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
    /// Set aggregate encoded-body admission across every physical peer writer.
    /// Default 256 MiB; minimum 32 MiB. Ordinary producers leave one maximum
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
    /// per-connection encoded writer queues. The minimum is 16 MiB.
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
/// construct them through the builder or [`Default`] so added options remain compatible.
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
    /// Empty (default) = this node serves all fragments (single-node / legacy
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

    fn admit_operation(&self) -> Result<OperationPermit, NodeError> {
        self.operation_gate.try_enter()
    }

    async fn fail_startup_after_shutdown(&self, message: &str) -> NodeError {
        match self.shutdown().await {
            Ok(()) => NodeError::Actor(message.to_string()),
            Err(cleanup) => {
                NodeError::Actor(format!("{message}; startup cleanup also failed: {cleanup}"))
            }
        }
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
        let mut runtime_options = runtime_options;
        runtime_options.replication_retention =
            crate::retained::ByteBudget::new(runtime_options.replication_retained_byte_capacity);
        runtime_options.reconciliation_retention =
            crate::retained::ByteBudget::new(runtime_options.reconciliation_retained_byte_capacity);
        if config.ttl_expiry_handler.is_some() {
            config
                .ttl_expiry_options
                .validate()
                .map_err(NodeError::Actor)?;
        }
        if node_id.is_empty() || node_id.len() > MAX_WIRE_NODE_ID_BYTES {
            return Err(NodeError::Actor(format!(
                "node_id must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
            )));
        }
        if let Some(invalid_member) = config
            .members
            .keys()
            .find(|member_id| member_id.is_empty() || member_id.len() > MAX_WIRE_NODE_ID_BYTES)
        {
            return Err(NodeError::Actor(format!(
                "member node_id {invalid_member:?} must contain 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
            )));
        }
        let dynamic = config.placement_controller.is_some();
        if let Some(recovery) = config.recovery.as_ref() {
            recovery
                .validate()
                .map_err(|error| NodeError::Codec(error.into()))?;
            if !dynamic || config.store.is_none() {
                return Err(NodeError::Codec(
                    "recovery requires dynamic placement and a durable store".into(),
                ));
            }
        }
        if config.fragment_count == 0 {
            return Err(NodeError::Actor(
                "fragment_count must be non-zero".to_string(),
            ));
        }
        if config.fragment_count > u32::MAX as usize {
            return Err(NodeError::Actor(format!(
                "fragment_count must not exceed {}",
                u32::MAX
            )));
        }
        config
            .replication
            .validate()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        config
            .replication
            .validate_node_ingress()
            .map_err(|error| NodeError::Actor(error.to_string()))?;
        // Validate before binding storage or transport. Maintenance actors
        // construct interval timers during startup and placement can construct
        // warning deadlines much later; neither may panic on user input.
        let timer_now = Instant::now();
        let warning_now = std::time::Instant::now();
        for (name, interval, enabled) in [
            ("clean_interval", config.clean_interval, true),
            ("snapshot_interval", config.snapshot_interval, true),
            (
                "placement_poll_interval",
                config.placement_poll_interval,
                dynamic,
            ),
            (
                "placement_drain_warn_interval",
                config.placement_drain_warn_interval,
                dynamic,
            ),
        ] {
            if enabled
                && (interval.is_zero()
                    || timer_now.checked_add(interval).is_none()
                    || warning_now.checked_add(interval).is_none())
            {
                return Err(NodeError::Actor(format!(
                    "{name} must be non-zero and representable by the runtime clock"
                )));
            }
        }

        if config
            .cluster_secret
            .as_ref()
            .is_some_and(|secret| secret.len() < 32)
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

        let mut config = config;
        config.persistence = config.persistence.for_runtime().map_err(NodeError::Actor)?;
        let recovery = config.recovery.clone().map(|options| {
            Arc::new(RecoveryContext::new(
                options,
                node_id.clone(),
                initial_placement.clone(),
            ))
        });
        let mut sender_config = config.replication.clone();
        if recovery.is_some() {
            sender_config.required_ack_durability = ReplicationAckDurability::StableCommitted;
        }

        // Bind immutable storage identity before network or actor side effects.
        // The binding remains owned by Node until every fragment has stopped.
        let store_identity = StoreIdentity::new(node_id.clone(), config.fragment_count as u32);
        let store_binding = if let Some(store) = config.store.clone() {
            Some(
                config
                    .persistence
                    .executor()
                    .run(move || store.bind(&store_identity))
                    .await
                    .map_err(|error| NodeError::Storage(error.to_string()))?,
            )
        } else {
            None
        };
        let mut store_binding = StoreBindingStartupGuard::new(store_binding);

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
        // Dynamic placement preloads assigned subtrees synchronously so a
        // corrupt durable record still fails Node::start. Authority is not
        // published until the placement driver reconciles each preload.
        let preloaded_fragments: Arc<StdMutex<HashSet<FragmentId>>> =
            Arc::new(StdMutex::new(if dynamic {
                my_fragments.iter().copied().collect()
            } else {
                HashSet::new()
            }));
        let serving: Arc<RwLock<HashSet<FragmentId>>> = Arc::new(RwLock::new(if dynamic {
            HashSet::new()
        } else {
            my_fragments.iter().copied().collect()
        }));
        let placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>> =
            Arc::new(Mutex::new(if dynamic {
                HashMap::new()
            } else {
                my_fragments
                    .iter()
                    .map(|f| (*f, FragmentPlacementState::Serving))
                    .collect()
            }));
        let tasks = RuntimeTasks::new(TASK_COMMAND_CAPACITY);
        let placement_tasks = RuntimeTasks::new(TASK_COMMAND_CAPACITY);
        let ttl_expiry = config.ttl_expiry_handler.as_ref().map(|handler| {
            ExpiryRuntime::start(
                node_id.clone(),
                handler.clone(),
                config.ttl_expiry_options.clone(),
            )
        });
        let expiry_publisher = ttl_expiry.as_ref().map(ExpiryRuntime::publisher);
        let mut startup_supervisors = SupervisorStartupGuard::new(my_fragments.len());
        for fragment in my_fragments {
            // From this point cancellation may leave a synchronously starting
            // actor alive. The binding must then remain fail-closed.
            store_binding.mark_actors_may_exist();
            let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
                &node_id,
                fragment,
                &config,
                &hlc,
                &supervisor_stops,
                expiry_publisher.clone(),
            )
            .await
            {
                Ok(subtree) => subtree,
                Err(error) => {
                    tasks.shutdown().await;
                    supervisor_stops.stop_and_join_all().await;
                    startup_supervisors.disarm();
                    config.persistence.executor().drain().await;
                    store_binding.allow_release_after_join();
                    return Err(error);
                }
            };
            startup_supervisors.push(supervisor.clone());
            supervisors
                .lock()
                .await
                .insert(fragment, supervisor.clone());
            let generation = router.insert(fragment, dispatcher.clone());
            if !register_fragment_dispatcher_monitor(
                &tasks,
                fragment,
                generation,
                supervisor,
                router.clone(),
                dispatcher,
            )
            .await
            {
                router.remove_if_generation(fragment, generation);
                tasks.shutdown().await;
                supervisor_stops.stop_and_join_all().await;
                startup_supervisors.disarm();
                config.persistence.executor().drain().await;
                store_binding.allow_release_after_join();
                return Err(NodeError::Actor(
                    "node runtime stopped during dispatcher-monitor startup".to_string(),
                ));
            }
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
        let Some(read_service) = ReadService::start(
            &tasks,
            runtime_options.serving_read_queue_capacity,
            runtime_options.serving_read_key_byte_capacity,
        )
        .await
        else {
            tasks.shutdown().await;
            supervisor_stops.stop_and_join_all().await;
            startup_supervisors.disarm();
            config.persistence.executor().drain().await;
            store_binding.allow_release_after_join();
            return Err(NodeError::Actor(
                "node runtime stopped during serving read startup".to_string(),
            ));
        };
        let write_concerns = crate::write_concern::WriteConcernTracker::new(
            node_id.clone(),
            fragment_map.clone(),
            runtime_options.forwarded_mutation_capacity,
        );
        write_concerns.configure_regions(runtime_options.regions.as_ref().clone());
        if let Some(cap) = runtime_options.write_concern_acknowledgement_cap {
            write_concerns
                .set_acknowledgement_cap(cap)
                .expect("runtime options validated before startup");
        }
        let node = Self {
            recovery: recovery.clone(),
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
            pending_scans: Arc::new(StdMutex::new(HashMap::new())),
            pending_query_pages: Arc::new(StdMutex::new(HashMap::new())),
            query_admission: crate::query::QueryAdmission::with_runtime_capacity(
                runtime_options.query_runtime_byte_capacity,
            ),
            pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
            pending_relays: Arc::new(StdMutex::new(HashMap::new())),
            pending_mutation_relays: Arc::new(StdMutex::new(HashMap::new())),
            next_req_id: Arc::new(AtomicU64::new(1)),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            next_seq: Arc::new(Mutex::new(0)),
            hlc: hlc.clone(),
            senders: Arc::new(Mutex::new(HashMap::new())),
            finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_config,
            write_concerns: write_concerns.clone(),
            runtime_options: runtime_options.clone(),
            storage_executor: config.persistence.executor(),
            persistence_budget: config.persistence.aggregate(),
            sender_reports: Arc::new(Mutex::new(HashMap::new())),
            sync_entries_applied: Arc::new(AtomicU64::new(0)),
            membership_tx: membership_tx.clone(),
            cluster_secret: config.cluster_secret.clone(),
            placement_tasks,
            tasks,
            read_service,
            inbound_connections: Arc::new(Semaphore::new(
                runtime_options.inbound_connection_capacity,
            )),
            receive_byte_budget: Arc::new(Semaphore::new(runtime_options.receive_byte_capacity)),
            node_peer_queue_budget: NodePeerByteBudget::new(
                runtime_options.node_peer_queue_byte_capacity,
            ),
            relay_admission: RelayAdmission::new(
                runtime_options.relay_capacity,
                runtime_options.relay_key_byte_capacity,
            ),
            mutation_service: MutationService::new_with_tracker(
                runtime_options.forwarded_mutation_capacity,
                runtime_options.forwarded_mutation_byte_capacity,
                write_concerns,
            ),
            embedded_operations: EmbeddedOperations::new(
                runtime_options.embedded_operation_capacity,
                runtime_options.embedded_operation_byte_capacity,
            ),
            dial_intents: Arc::new(StdMutex::new(HashMap::new())),
            dial_intent_changes,
            peer_changes,
            operation_gate: OperationGate::new(),
            shutdown_state: Mutex::new(ShutdownProgress::Running),
            replication_drain_report: StdMutex::new(None),
            network: config.network.clone(),
            ttl_expiry,
            store_binding: StdMutex::new(store_binding.transfer_to_node()),
        };

        if !node
            .tasks
            .spawn(node.peer_failures.clone().run(node.peers.clone()))
            .await
        {
            return Err(node
                .fail_startup_after_shutdown(
                    "node runtime stopped during peer-failure worker startup",
                )
                .await);
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
                recovery: recovery.clone(),
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
                preloaded_fragments,
                placement_generation: Arc::new(AtomicU64::new(0)),
                senders: node.senders.clone(),
                finalizing_senders: node.finalizing_senders.clone(),
                retiring_senders: node.retiring_senders.clone(),
                sender_reports: node.sender_reports.clone(),
                sender_config: node.sender_config.clone(),
                sender_runtime: node
                    .runtime_options
                    .sender_options()
                    .with_write_concerns(node.write_concerns.clone())
                    .with_recovery(recovery.clone()),
                peers: node.peers.clone(),
                peer_failures: node.peer_failures.clone(),
                tasks: Arc::downgrade(&node.placement_tasks),
                dispatcher_tasks: Arc::downgrade(&node.tasks),
                operation_gate: node.operation_gate.clone(),
                hlc: hlc.clone(),
                expiry_publisher: node.ttl_expiry.as_ref().map(ExpiryRuntime::publisher),
            };
            let membership_rx = membership_rx.expect("checked above");
            if !node
                .placement_tasks
                .spawn(placement_driver(deps, membership_rx))
                .await
            {
                return Err(node
                    .fail_startup_after_shutdown(
                        "node runtime stopped during placement-driver startup",
                    )
                    .await);
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
            return Err(node
                .fail_startup_after_shutdown("node runtime stopped during listener startup")
                .await);
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
                return Err(node
                    .fail_startup_after_shutdown(
                        "node runtime stopped during member-dialer startup",
                    )
                    .await);
            }
        }

        startup_supervisors.disarm();
        Ok(node)
    }

    /// Dial a peer and start a supervised connection. After authentication,
    /// both sides start paged reconciliation, so a late joiner converges
    /// without constructing a monolithic full-state frame.
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
        let _operation = self.admit_operation()?;
        if !self.tasks.is_accepting() {
            return Err(NodeError::ShuttingDown);
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
            recovery: self.recovery.clone(),
            node_id: self.node_id.clone(),
            router: self.router.clone(),
            serving: self.serving.clone(),
            placement: self.fragment_map.clone(),
            dynamic_placement: self.placement_controller.is_some(),
            pending_gets: self.pending_gets.clone(),
            pending_batch_gets: self.pending_batch_gets.clone(),
            pending_scans: self.pending_scans.clone(),
            pending_query_pages: self.pending_query_pages.clone(),
            query_admission: self.query_admission.clone(),
            pending_mutations: self.pending_mutations.clone(),
            pending_relays: self.pending_relays.clone(),
            pending_mutation_relays: self.pending_mutation_relays.clone(),
            next_req_id: self.next_req_id.clone(),
            peers: self.peers.clone(),
            peer_failures: self.peer_failures.clone(),
            tasks: Arc::downgrade(&self.tasks),
            read_service: self.read_service.clone(),
            operation_gate: self.operation_gate.clone(),
            peer_changes: self.peer_changes.clone(),
            hlc: self.hlc.clone(),
            senders: self.senders.clone(),
            finalizing_senders: self.finalizing_senders.clone(),
            retiring_senders: self.retiring_senders.clone(),
            sender_config: self.sender_config.clone(),
            sender_runtime: self
                .runtime_options
                .sender_options()
                .with_write_concerns(self.write_concerns.clone())
                .with_recovery(self.recovery.clone()),
            runtime_options: self.runtime_options.clone(),
            receive_byte_budget: self.receive_byte_budget.clone(),
            node_peer_queue_budget: self.node_peer_queue_budget.clone(),
            relay_admission: self.relay_admission.clone(),
            mutation_service: self.mutation_service.clone(),
            sender_reports: self.sender_reports.clone(),
            membership_tx: self.membership_tx.clone(),
            cluster_secret: self.cluster_secret.clone(),
            #[cfg(test)]
            sender_snapshot_hook: None,
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
        let _operation = self.admit_operation()?;
        let retention =
            self.embedded_operations
                .admit(embedded_operations::single_request_bytes(
                    &key,
                    Some(&value),
                ))?;
        self.put_with_ttl_and_priority(key, value, None, OpPriority::Default, retention)
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
        let _operation = self.admit_operation()?;
        let retention =
            self.embedded_operations
                .admit(embedded_operations::single_request_bytes(
                    &key,
                    Some(&value),
                ))?;
        self.put_with_ttl_and_priority(key, value, None, priority, retention)
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
        let _operation = self.admit_operation()?;
        let retention =
            self.embedded_operations
                .admit(embedded_operations::single_request_bytes(
                    &key,
                    Some(&value),
                ))?;
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
        let _operation = self.admit_operation()?;
        let retention =
            self.embedded_operations
                .admit(embedded_operations::single_request_bytes(
                    &key,
                    Some(&value),
                ))?;
        self.put_with_ttl_and_priority(key, value, ttl, OpPriority::Default, retention)
            .await
    }

    /// Delete `key`: writes a tombstone that propagates to all replicas
    /// like a put. Reads treat tombstoned keys as absent; sync ships
    /// tombstones so deletes survive node restarts and re-joins. A later
    /// `put` with a higher timestamp resurrects the key.
    pub async fn delete(&self, key: String) -> Result<(), NodeError> {
        self.delete_with_priority(key, OpPriority::Default).await
    }

    /// Delete a key through the selected priority pool.
    pub async fn delete_with_priority(
        &self,
        key: String,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        let _operation = self.admit_operation()?;
        let retention = self
            .embedded_operations
            .admit(embedded_operations::single_request_bytes(&key, None))?;
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
        let _operation = self.admit_operation()?;
        let retention = self
            .embedded_operations
            .admit(embedded_operations::single_request_bytes(&key, None))?;
        self.get_with_priority_admitted(key, OpPriority::Default, retention)
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
        let _operation = self.admit_operation()?;
        let retention = self
            .embedded_operations
            .admit(embedded_operations::single_request_bytes(&key, None))?;
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
                CallResult::Timeout => Err(NodeError::Actor("get timed out".into())),
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
    /// Request count admission is bounded, but this legacy debug API clones the
    /// whole local dataset. Use bounded queries/scans for production reads.
    pub async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let _operation = self.admit_operation()?;
        let retention = self
            .embedded_operations
            .admit(std::mem::size_of::<KvMessage>())?;
        let ownership: Arc<dyn Send + Sync> = Arc::new((retention, self.admit_operation()?));
        let all = tokio::time::timeout(
            self.runtime_options.local_work_timeout(),
            self.router.get_all_retained(Some(ownership)),
        )
        .await
        .map_err(|_| NodeError::Actor("get_all local work timed out".into()))??;
        // Hide tombstones from user reads (sync needs them, users don't).
        Ok(all.into_iter().filter(|(_, v)| !v.is_tombstone).collect())
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
        let _operation = match self.admit_operation() {
            Ok(operation) => operation,
            Err(_) => {
                return kvs
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self
            .embedded_operations
            .admit(embedded_operations::batch_put_request_bytes(&kvs))
        {
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
        loop {
            let chunk: Vec<_> = local.by_ref().take(MAX_LOCAL_BATCH_IN_FLIGHT).collect();
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
        let _operation = match self.admit_operation() {
            Ok(operation) => operation,
            Err(_) => {
                return kvs
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self
            .embedded_operations
            .admit(embedded_operations::batch_put_request_bytes(&kvs))
        {
            Ok(retention) => retention,
            Err(error) => return embedded_operations::reject_batch(kvs, error),
        };
        self.put_multi_with_priority_admitted(kvs, OpPriority::Background, retention)
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
        let _operation = match self.admit_operation() {
            Ok(operation) => operation,
            Err(_) => {
                return keys
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self
            .embedded_operations
            .admit(embedded_operations::batch_get_request_bytes(&keys))
        {
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
        let _operation = match self.admit_operation() {
            Ok(operation) => operation,
            Err(_) => {
                return keys
                    .into_iter()
                    .map(|_| Err(NodeError::ShuttingDown))
                    .collect()
            }
        };
        let retention = match self
            .embedded_operations
            .admit(embedded_operations::batch_get_request_bytes(&keys))
        {
            Ok(retention) => retention,
            Err(error) => return embedded_operations::reject_batch(keys, error),
        };
        self.get_multi_with_priority_admitted(keys, OpPriority::Background, retention)
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

    async fn replication_drain_targets(&self) -> Vec<ReplicationDrainTarget> {
        let placement = self.fragment_map.read().unwrap().clone();
        let mut targets = if placement.is_empty() {
            // Legacy full-replication mode discovers peers from authenticated
            // connections. Its only truthful closed-world boundary is the
            // sender set already installed when admission and placement stop.
            self.senders
                .lock()
                .await
                .keys()
                .map(|(fragment, peer_id)| ReplicationDrainTarget {
                    fragment: *fragment,
                    peer_id: peer_id.clone(),
                })
                .collect()
        } else {
            let mut targets = Vec::new();
            for fragment in self.router.fragment_ids().into_iter().map(FragmentId) {
                if let Some(replicas) = placement.get(&fragment) {
                    targets.extend(
                        replicas
                            .iter()
                            .filter(|peer_id| *peer_id != &self.node_id)
                            .map(|peer_id| ReplicationDrainTarget {
                                fragment,
                                peer_id: peer_id.clone(),
                            }),
                    );
                }
            }
            targets
        };
        targets.sort();
        targets.dedup();
        targets
    }

    async fn prepare_replication_drains(
        &self,
        targets: &[ReplicationDrainTarget],
    ) -> Result<Vec<ReplicationDrainWaiter>, String> {
        let sender_runtime = self
            .runtime_options
            .sender_options()
            .with_write_concerns(self.write_concerns.clone());
        let mut senders = Vec::with_capacity(targets.len());
        for target in targets {
            ensure_sender_entry(
                target.fragment,
                &target.peer_id,
                SenderEntryDeps {
                    config: &self.sender_config,
                    runtime: &sender_runtime,
                    router: &self.router,
                    hlc: &self.hlc,
                    senders: &self.senders,
                    finalizing_senders: &self.finalizing_senders,
                    retiring_senders: &self.retiring_senders,
                    reports: &self.sender_reports,
                    peer_failures: &self.peer_failures,
                    operation_gate: &self.operation_gate,
                    allow_quiescing: true,
                },
            )
            .await;

            let sender = self
                .senders
                .lock()
                .await
                .get(&(target.fragment, target.peer_id.clone()))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "no authoritative fragment sender is available for fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
            if sender.is_closed() {
                return Err(format!(
                    "replication sender stopped for fragment {} peer {}",
                    target.fragment, target.peer_id
                ));
            }
            let peers = self.peers.lock().await;
            if let Some(peer) = peers.get(&target.peer_id) {
                sender.connect_peer(peer.budgeted_sender()).map_err(|_| {
                    format!(
                        "replication sender stopped while reconnecting fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
            }
            drop(peers);
            senders.push((target.clone(), sender));
        }

        senders
            .into_iter()
            .map(|(target, sender)| {
                let drain = sender.request_drain().map_err(|_| {
                    format!(
                        "replication sender stopped while fencing fragment {} peer {}",
                        target.fragment, target.peer_id
                    )
                })?;
                Ok(ReplicationDrainWaiter {
                    target,
                    sender,
                    drain,
                })
            })
            .collect()
    }

    async fn replication_drain_partition(
        &self,
        waiters: &[ReplicationDrainWaiter],
    ) -> Result<(Vec<ReplicationDrainTarget>, Vec<ReplicationDrainTarget>), String> {
        let senders = self.senders.lock().await;
        let mut completed = Vec::new();
        let mut pending = Vec::new();
        for waiter in waiters {
            let current = senders
                .get(&(waiter.target.fragment, waiter.target.peer_id.clone()))
                .ok_or_else(|| {
                    format!(
                        "replication sender disappeared for fragment {} peer {}",
                        waiter.target.fragment, waiter.target.peer_id
                    )
                })?;
            if !current.same_task(&waiter.sender) {
                return Err(format!(
                    "replication sender was replaced for fragment {} peer {}",
                    waiter.target.fragment, waiter.target.peer_id
                ));
            }
            if waiter.drain.is_complete() {
                completed.push(waiter.target.clone());
            } else if waiter.drain.is_closed() || waiter.sender.is_closed() {
                return Err(format!(
                    "replication sender stopped before draining fragment {} peer {}",
                    waiter.target.fragment, waiter.target.peer_id
                ));
            } else {
                pending.push(waiter.target.clone());
            }
        }
        Ok((completed, pending))
    }

    async fn finish_local_shutdown(
        &self,
        progress: &mut ShutdownProgress,
    ) -> Result<(), NodeError> {
        if matches!(progress, ShutdownProgress::StoppingTransport) {
            self.mutation_service.request_shutdown();
            self.embedded_operations.request_shutdown();
            // Placement owns fragment and sender topology. Join it before the
            // general task set so no cancelled transition can publish work
            // after the transport/sender snapshot below.
            self.placement_tasks.shutdown().await;
            self.tasks.shutdown().await;
            // Transport cancellation closes RPC waiters, not accepted apply
            // ownership. Drain complete outcomes/fanout while every sender and
            // fragment actor is still alive. A canceled caller may resume this.
            self.mutation_service.shutdown().await;
            self.embedded_operations.shutdown().await;

            // Release every request/relay waiter retained by a cancelled
            // handler. Admitted remote operations then leave the public gate
            // promptly instead of waiting for their normal request deadline.
            self.pending_gets.lock().unwrap().clear();
            self.pending_batch_gets.lock().unwrap().clear();
            self.pending_mutations.lock().unwrap().clear();
            self.pending_relays.lock().unwrap().clear();
            self.pending_mutation_relays.lock().unwrap().clear();

            // A hard transport shutdown can cancel a handler before its normal
            // exact-channel cleanup tail. Clear every remaining peer entry
            // before stopping the long-lived replication senders.
            let peers: Vec<(String, PeerChannel)> = self.peers.lock().await.drain().collect();
            for (_, channel) in &peers {
                channel.cancel();
            }
            let disconnected_targets = {
                let senders = self.senders.lock().await;
                let mut targets = Vec::new();
                for (peer, channel) in &peers {
                    for ((_, sender_peer), sender) in senders.iter() {
                        if sender_peer == peer {
                            targets.push((sender.clone(), channel.raw_sender()));
                        }
                    }
                }
                targets
            };
            for (sender, peer_tx) in disconnected_targets {
                let _ = sender.send(SenderMsg::PeerDisconnected { peer_tx });
            }
            self.peer_changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });

            self.operation_gate.wait_for_drain().await;

            let senders: Vec<NodeSender> = self.senders.lock().await.values().cloned().collect();
            for sender in senders {
                sender.stop().await;
            }
            let retiring_senders = self.retiring_senders.snapshot();
            for sender in retiring_senders {
                sender.stop().await;
            }
            self.retiring_senders.clear();
            *progress = ShutdownProgress::QuiescingFragments;
        }

        if matches!(progress, ShutdownProgress::QuiescingFragments) {
            // Prepare every fragment before destroying any fragment state. A
            // partial store outage therefore leaves all successful fragments
            // quiesced and every failed buffer available for an exact retry.
            let supervisors = self.supervisor_stops.snapshot();
            let mut failures = Vec::new();
            let mut saw_storage_failure = false;
            for (fragment, entry) in supervisors {
                if entry.prepared {
                    continue;
                }
                let quiesce_result = match wait_for_supervisor_startup(entry.startup.clone()).await
                {
                    Ok(SupervisorStartupState::Ready) => {
                        quiesce_fragment_supervisor(&entry.actor).await
                    }
                    Ok(SupervisorStartupState::Failed(error)) => {
                        tracing::warn!(
                            fragment = fragment.0,
                            %error,
                            "fragment startup failed before shutdown; child cleanup completed"
                        );
                        Ok(())
                    }
                    Ok(SupervisorStartupState::Starting) => {
                        unreachable!("startup wait returned pending")
                    }
                    Err(error) => Err(FragmentShutdownError::Actor(error)),
                };
                match quiesce_result {
                    Ok(()) => {
                        if !self.supervisor_stops.mark_prepared(fragment, &entry.actor) {
                            failures.push(format!(
                                "fragment {fragment}: supervisor ownership changed during quiesce"
                            ));
                        }
                    }
                    Err(FragmentShutdownError::Storage(error)) => {
                        saw_storage_failure = true;
                        failures.push(format!("fragment {fragment}: {error}"));
                    }
                    Err(FragmentShutdownError::Actor(error)) => {
                        failures.push(format!("fragment {fragment}: {error}"));
                    }
                }
            }
            if !failures.is_empty() {
                let message = failures.join("; ");
                return if saw_storage_failure {
                    Err(NodeError::Storage(message))
                } else {
                    Err(NodeError::Actor(message))
                };
            }
            if self
                .supervisor_stops
                .snapshot()
                .iter()
                .any(|(_, entry)| !entry.prepared)
            {
                return Err(NodeError::Actor(
                    "fragment supervisor appeared after runtime task shutdown".to_string(),
                ));
            }
            // Persist this transition before sending the first stop signal. A
            // cancelled caller can then resume joins without asking a stopped
            // supervisor to prove durability again.
            *progress = ShutdownProgress::StoppingFragments;
        }

        if matches!(progress, ShutdownProgress::StoppingFragments) {
            // Every fragment's authoritative final sweep/fence is complete.
            // Drain notifications with a manager-owned deadline; cancellation
            // of this shutdown waiter does not detach or restart the drain.
            if let Some(expiry) = &self.ttl_expiry {
                expiry.shutdown().await;
            }
            let supervisors = self.supervisor_stops.snapshot();
            for (_, entry) in &supervisors {
                if matches!(&*entry.startup.borrow(), SupervisorStartupState::Ready) {
                    entry.actor.stop(None);
                }
            }
            for (_, entry) in supervisors {
                if matches!(&*entry.startup.borrow(), SupervisorStartupState::Ready) {
                    let _ = entry.actor.wait(None).await;
                }
            }
            self.supervisors.lock().await.clear();
            self.supervisor_stops.clear();
            self.storage_executor.drain().await;
            let store_binding = self.store_binding.lock().unwrap().take();
            drop(store_binding);
            self.operation_gate.finish_shutdown();
            *progress = ShutdownProgress::Stopped;
        }
        Ok(())
    }

    /// Shut the node down and report whether every local fragment reached its
    /// final persistence fence.
    ///
    /// Shutdown is one-way: it first rejects new public operations, closes and
    /// joins Node-owned runtime tasks, waits for already-admitted public work,
    /// and stops replication senders. It then quiesces every fragment before
    /// stopping any supervisor. A failed store flush leaves the fragment state
    /// and retryable batch owned by its supervisor; this method returns the
    /// error and a later call resumes shutdown. Only a successful return drops
    /// all fragment state and releases a configured RocksDB lock and live
    /// attachment.
    ///
    /// This local durability fence does not wait for peer replication ACKs;
    /// transport convergence has its own lifecycle policy. Calling this after
    /// a timed-out or cancelled replication drain explicitly abandons that
    /// remote guarantee and completes the local-only shutdown.
    pub async fn shutdown(&self) -> Result<(), NodeError> {
        let mut progress = self.shutdown_state.lock().await;
        if matches!(*progress, ShutdownProgress::Stopped) {
            return Ok(());
        }
        if matches!(
            *progress,
            ShutdownProgress::Running
                | ShutdownProgress::FencingReplication
                | ShutdownProgress::DrainingReplication(_)
        ) {
            self.operation_gate.begin_shutdown();
            *self.replication_drain_report.lock().unwrap() = None;
            *progress = ShutdownProgress::StoppingTransport;
        }
        self.finish_local_shutdown(&mut progress).await
    }

    /// Callback diagnostics, or `None` when registration is disabled. Counters
    /// describe local best-effort delivery and are readable after shutdown.
    pub fn ttl_expiry_diagnostics(&self) -> Option<ExpiryDiagnostics> {
        self.ttl_expiry.as_ref().map(ExpiryRuntime::diagnostics)
    }

    /// Fence locally admitted mutations, reconcile every frozen peer target
    /// through the configured ACK durability barrier, then complete the normal
    /// local persistence shutdown.
    ///
    /// The remote guarantee is a per-target LWW version floor: for each
    /// reported fragment/peer, the receiver acknowledged the fenced winner or
    /// a newer winner. It is not a global cache-equality, quorum, or fsync
    /// guarantee. `MemoryApplied` remains volatile; `StoreCommitted` means the
    /// receiver store accepted its ordinary flush, while `StableCommitted`
    /// additionally invokes its explicit stable-storage fence.
    ///
    /// The single timeout covers placement freeze, admitted-mutation drain,
    /// sender preparation, reconnect/reconciliation, and remote durability.
    /// Timeout or caller cancellation leaves the node quiesced with transport,
    /// senders, and fragment state alive; retrying this method resumes every
    /// issued exact sender generation. A sender task that exits is replaced and
    /// re-fenced from the still-authoritative fragment cache. [`Node::shutdown`]
    /// is the explicit local-only escape. Mutation responses already queued to
    /// a socket can still be lost, so clients must retry stamped operations
    /// idempotently.
    ///
    /// With explicit placement, targets are the configured remote replicas of
    /// every locally active fragment after placement tasks stop. Legacy empty-
    /// map mode can cover only sender targets already installed at the fence.
    pub async fn shutdown_with_replication_drain(
        &self,
        options: ReplicationDrainOptions,
    ) -> Result<ReplicationDrainReport, ReplicationDrainShutdownError> {
        if options.timeout.is_zero() {
            return Err(ReplicationDrainShutdownError::InvalidOptions(
                "timeout must be non-zero".to_string(),
            ));
        }
        let Some(deadline) = Instant::now().checked_add(options.timeout) else {
            return Err(ReplicationDrainShutdownError::InvalidOptions(
                "timeout is too large".to_string(),
            ));
        };

        let mut progress = tokio::time::timeout_at(deadline, self.shutdown_state.lock())
            .await
            .map_err(|_| ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::ShutdownCoordination,
                completed: Vec::new(),
                pending: Vec::new(),
            })?;
        let stored_report = || self.replication_drain_report.lock().unwrap().clone();
        if matches!(*progress, ShutdownProgress::Stopped) {
            return stored_report().ok_or_else(|| {
                ReplicationDrainShutdownError::Unavailable(
                    "node already completed a local-only shutdown".to_string(),
                )
            });
        }
        if stored_report().is_none()
            && matches!(
                *progress,
                ShutdownProgress::StoppingTransport
                    | ShutdownProgress::QuiescingFragments
                    | ShutdownProgress::StoppingFragments
            )
        {
            return Err(ReplicationDrainShutdownError::Unavailable(
                "local-only shutdown has already stopped replication".to_string(),
            ));
        }

        if matches!(*progress, ShutdownProgress::Running) {
            self.operation_gate.begin_shutdown();
            // Publish the topology freeze synchronously. Even an already-
            // expired deadline must not return while the placement manager
            // can still register a transition.
            self.placement_tasks.request_shutdown();
            *progress = ShutdownProgress::FencingReplication;
        }

        if matches!(*progress, ShutdownProgress::FencingReplication) {
            self.placement_tasks.request_shutdown();
            if tokio::time::timeout_at(deadline, self.placement_tasks.shutdown())
                .await
                .is_err()
            {
                return Err(ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::PlacementFreeze,
                    completed: Vec::new(),
                    pending: Vec::new(),
                });
            }
            if tokio::time::timeout_at(deadline, self.operation_gate.wait_for_drain())
                .await
                .is_err()
            {
                return Err(ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::MutationAdmissionDrain,
                    completed: Vec::new(),
                    pending: Vec::new(),
                });
            }

            let targets = tokio::time::timeout_at(deadline, self.replication_drain_targets())
                .await
                .map_err(|_| ReplicationDrainShutdownError::Timeout {
                    phase: ReplicationDrainPhase::TargetSnapshot,
                    completed: Vec::new(),
                    pending: Vec::new(),
                })?;
            let waiters =
                match tokio::time::timeout_at(deadline, self.prepare_replication_drains(&targets))
                    .await
                {
                    Ok(Ok(waiters)) => waiters,
                    Ok(Err(error)) => {
                        return Err(ReplicationDrainShutdownError::RetryableSender(error));
                    }
                    Err(_) => {
                        return Err(ReplicationDrainShutdownError::Timeout {
                            phase: ReplicationDrainPhase::SenderPreparation,
                            completed: Vec::new(),
                            pending: targets,
                        });
                    }
                };
            *progress = ShutdownProgress::DrainingReplication(waiters);
        }

        if let ShutdownProgress::DrainingReplication(waiters) = &*progress {
            loop {
                let all_targets: Vec<_> =
                    waiters.iter().map(|waiter| waiter.target.clone()).collect();
                let partition =
                    tokio::time::timeout_at(deadline, self.replication_drain_partition(waiters))
                        .await
                        .map_err(|_| ReplicationDrainShutdownError::Timeout {
                            phase: ReplicationDrainPhase::PeerReconciliation,
                            completed: Vec::new(),
                            pending: all_targets,
                        })?;
                let (completed, pending) = match partition {
                    Ok(partition) => partition,
                    Err(error) => {
                        // Exact-task correlation prevents a replacement from
                        // satisfying the old waiter. Return to preparation so
                        // a retry can install and fence a fresh sender from the
                        // still-authoritative cache.
                        *progress = ShutdownProgress::FencingReplication;
                        return Err(ReplicationDrainShutdownError::RetryableSender(format!(
                            "{error}; retry to replace and re-fence the sender"
                        )));
                    }
                };
                if pending.is_empty() {
                    let report = ReplicationDrainReport {
                        targets: completed,
                        durability: self.sender_config.required_ack_durability,
                    };
                    *self.replication_drain_report.lock().unwrap() = Some(report);
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(ReplicationDrainShutdownError::Timeout {
                        phase: ReplicationDrainPhase::PeerReconciliation,
                        completed,
                        pending,
                    });
                }
                tokio::time::sleep_until(deadline.min(deadline_after(Duration::from_millis(10))))
                    .await;
            }
            *progress = ShutdownProgress::StoppingTransport;
        }

        let report = stored_report().ok_or_else(|| {
            ReplicationDrainShutdownError::Unavailable(
                "replication drain completed without a retained report".to_string(),
            )
        })?;
        self.finish_local_shutdown(&mut progress)
            .await
            .map_err(|error| ReplicationDrainShutdownError::Local {
                error,
                replication: report.clone(),
            })?;
        Ok(report)
    }

    /// Local health snapshots from owned fragment supervisors, including any
    /// bootstrapping/draining fragment still owned by this node. The one
    /// configurable health deadline includes registry acquisition and every
    /// sequential probe. Missing serving supervisors, failed dispatchers, and
    /// timeouts return errors; unavailable counters are never replaced by zero.
    /// Worker names/counts describe linked actors observed Running, not worker
    /// responsiveness. This non-atomic snapshot is not a readiness, replication,
    /// or durability guarantee.
    pub async fn health(&self) -> Result<HashMap<FragmentId, FragmentHealth>, NodeError> {
        let _operation = self.admit_operation()?;
        let deadline = deadline_after(self.runtime_options.health_timeout);
        tokio::time::timeout_at(deadline, async {
            let mut supervisors: Vec<(FragmentId, ActorRef<SupervisorMessage>)> = self
                .supervisors
                .lock()
                .await
                .iter()
                .map(|(f, s)| (*f, s.clone()))
                .collect();
            let owned: HashSet<_> = supervisors.iter().map(|(fragment, _)| *fragment).collect();
            for fragment in self.serving.read().unwrap().iter() {
                if !owned.contains(fragment) {
                    return Err(NodeError::Actor(format!(
                        "health unavailable for fragment {fragment}: serving supervisor is missing"
                    )));
                }
            }
            supervisors.sort_unstable_by_key(|(fragment, _)| *fragment);
            let mut out = HashMap::with_capacity(supervisors.len());
            for (frag, supervisor) in supervisors {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(NodeError::Actor(
                        "health snapshot deadline expired".to_string(),
                    ));
                }
                let res = supervisor
                    .call(
                        |reply| SupervisorMessage::GetHealth { reply },
                        Some(remaining),
                    )
                    .await
                    .map_err(|error| {
                        NodeError::Actor(format!("health unavailable for fragment {frag}: {error}"))
                    })?;
                match res {
                    CallResult::Success(health) if health.fragment == frag => {
                        out.insert(frag, health);
                    }
                    _ => {
                        return Err(NodeError::Actor(format!(
                            "health unavailable for fragment {frag}: probe failed"
                        )))
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|_| NodeError::Actor("health snapshot deadline expired".to_string()))?
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

    /// Compatibility counter for the retired `SyncResponse` protocol.
    /// Protocol v3 uses correlated reconciliation for every placement mode,
    /// so this always remains zero.
    pub fn sync_entries_applied(&self) -> u64 {
        self.sync_entries_applied.load(Ordering::Relaxed)
    }
}

/// Everything the Phase 8 placement driver needs. All shared
/// ownership; `Clone` for spawning per-fragment gain/lose tasks.
#[derive(Clone)]
struct PlacementDriverDeps {
    expiry_publisher: Option<ExpiryPublisher>,
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
    /// Subtrees loaded synchronously during dynamic startup but not yet
    /// reconciled or published as authoritative. The first placement tick
    /// consumes this set into explicit gain/loss transitions.
    preloaded_fragments: Arc<StdMutex<HashSet<FragmentId>>>,
    /// Increments only when the published placement map actually changes.
    /// A global generation is conservative: unrelated fragment movement can
    /// retry an in-flight bootstrap, but a P -> Q -> P ABA cannot publish work
    /// completed against the first P.
    placement_generation: Arc<AtomicU64>,
    senders: SenderMap,
    finalizing_senders: Arc<SenderFinalizationRegistry>,
    retiring_senders: Arc<RetiringSenderRegistry>,
    sender_reports: SenderReportMap,
    sender_config: SenderConfig,
    sender_runtime: SenderRuntimeOptions,
    peers: PeerMap,
    peer_failures: PeerFailureSignals,
    /// Polling and gain/loss transitions, stopped to freeze placement before
    /// a replication-drained shutdown snapshots targets.
    tasks: Weak<RuntimeTasks>,
    /// Dispatcher restart monitors are core actor lifecycle work and must stay
    /// alive while the frozen senders reconcile.
    dispatcher_tasks: Weak<RuntimeTasks>,
    operation_gate: Arc<OperationGate>,
    hlc: Arc<Mutex<HlcClock>>,
    recovery: Option<Arc<RecoveryContext>>,
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
    let (mine_current, in_flight, transitions) = {
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

        // Dynamic startup loaded these subtrees synchronously for durable
        // validation but deliberately withheld authority. Consume each one
        // exactly once into a transition which adopts (rather than replaces)
        // the existing actor tree. A controller revocation before this first
        // tick becomes a loss transition instead of leaving an orphan.
        let preloaded = std::mem::take(&mut *deps.preloaded_fragments.lock().unwrap());
        for fragment in preloaded {
            current.insert(fragment);
            flight.insert(fragment);
            if mine_desired.contains(&fragment) {
                states.insert(fragment, FragmentPlacementState::Bootstrapping);
                transitions.push((fragment, true, true));
            } else {
                states.insert(fragment, FragmentPlacementState::Draining);
                transitions.push((fragment, false, true));
            }
        }

        // Mark transitions before spawning so the next tick cannot create a
        // duplicate while the child task is waiting to be scheduled.
        for f in mine_desired.difference(&current) {
            states.insert(*f, FragmentPlacementState::Bootstrapping);
            transitions.push((*f, true, false));
        }
        for f in current.difference(&mine_desired) {
            if flight.contains(f) {
                continue;
            }
            states.insert(*f, FragmentPlacementState::Draining);
            transitions.push((*f, false, false));
        }
        (current, flight, transitions)
    };

    // Register outside the placement-state commit mutex. The placement task manager
    // owns and joins transitions directly, so aborting the polling driver
    // cannot detach a gain/loss child during shutdown.
    for (fragment, gaining, preloaded) in transitions {
        let transition_deps = deps.clone();
        let transition = async move {
            if gaining {
                gain_fragment(&transition_deps, fragment, preloaded).await;
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
        let targets: Vec<(NodeSender, BudgetedPeerTx)> = {
            let senders = deps.senders.lock().await;
            replicas
                .iter()
                .filter_map(|peer| {
                    Some((
                        senders.get(&(fragment, peer.clone()))?.clone(),
                        peers.get(peer)?.budgeted_sender(),
                    ))
                })
                .collect()
        };
        // Retain PeerMap's winner authority until every sender has observed
        // this channel, but do not retain the unrelated sender registry while
        // each task updates its own connection state.
        for (sender, peer_tx) in targets {
            let _ = sender.connect_peer(peer_tx);
        }
    }
    // Prune senders for peers that are no longer replicas. Their
    // buffered ops were fanned out to the remaining senders too, and
    // the peer's copy is not authoritative.
    let retiring = {
        // Placement publication is the authority for pruning. Re-read it
        // under its commit mutex so an older convergence pass cannot delete a
        // sender which a newer replica set has just made current.
        let _states = deps.placement_states.lock().await;
        let current_replicas: HashSet<String> = deps
            .placement
            .read()
            .unwrap()
            .get(&fragment)
            .into_iter()
            .flatten()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let stale: Vec<(FragmentId, String)> = senders
            .keys()
            .filter(|(sender_fragment, peer)| {
                *sender_fragment == fragment && !current_replicas.contains(peer)
            })
            .cloned()
            .collect();
        let mut retiring = Vec::with_capacity(stale.len());
        for key in &stale {
            if let Some(sender) = senders.remove(key) {
                retiring.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(key);
        }
        retiring
    };
    for sender in retiring {
        sender.join().await;
    }
}

/// Bootstrap timeout: bound the wait for a serving source peer and verified
/// reconcile round. Timeout keeps the fragment non-authoritative; the
/// placement loop can retry the gain later.
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

        if deps
            .recovery
            .as_ref()
            .is_some_and(|recovery| !recovery.matches_placement(fragment, &replicas))
        {
            tracing::warn!(
                fragment = fragment.0,
                "recovery placement changed; explicit new recovery authority required"
            );
            return false;
        }
        ensure_fragment_senders(deps, fragment, &desired).await;
        let sources: Vec<String> = replicas
            .iter()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        let recovery_fence = if deps.recovery.is_some() {
            recovery::bootstrap_all(deps, fragment, &sources).await
        } else {
            None
        };
        let bootstrapped = if deps.recovery.is_some() {
            recovery_fence.is_some()
        } else {
            sources.is_empty() || bootstrap_from(deps, fragment, &sources).await
        };
        if !bootstrapped {
            tracing::warn!(
                fragment = fragment.0,
                "fragment bootstrap timed out; refusing to publish incomplete state"
            );
            return false;
        }

        match commit_fragment_gain(deps, fragment, &replicas, generation).await {
            FragmentGainCommit::Serving => return true,
            FragmentGainCommit::Retry => continue,
            FragmentGainCommit::Revoked => return false,
        }
    }
}

/// Gain a fragment: adopt a synchronously preloaded startup subtree or spawn a
/// new one, bootstrap state from a replica holder via the bounded reconciler,
/// then go Serving. The fragment is not authoritative until bootstrap
/// completes: local reads/writes forward to a serving owner meanwhile.
async fn gain_fragment(deps: &PlacementDriverDeps, fragment: FragmentId, preloaded: bool) {
    if preloaded {
        let actor_loaded = deps.router.route_for_fragment(fragment).is_some();
        let supervisor_loaded = deps.supervisors.lock().await.contains_key(&fragment);
        if !actor_loaded || !supervisor_loaded {
            tracing::error!(
                fragment = fragment.0,
                actor_loaded,
                supervisor_loaded,
                "dynamic startup preload was incomplete"
            );
            deps.placement_states.lock().await.remove(&fragment);
            return;
        }
    } else {
        let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
            &deps.node_id,
            fragment,
            &deps.subtree_config,
            &deps.hlc,
            &deps.supervisor_stops,
            deps.expiry_publisher.clone(),
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
        deps.supervisors
            .lock()
            .await
            .insert(fragment, supervisor.clone());
        // The actor exists in the router now (so the reconciler can merge
        // into it), but the fragment is not serving yet.
        let generation = deps.router.insert(fragment, dispatcher.clone());
        let mut route_registration = RouteMonitorRegistrationGuard {
            router: deps.router.clone(),
            fragment,
            generation,
            armed: true,
        };
        let monitor_registered = match deps.dispatcher_tasks.upgrade() {
            Some(tasks) => {
                register_fragment_dispatcher_monitor(
                    &tasks,
                    fragment,
                    generation,
                    supervisor.clone(),
                    deps.router.clone(),
                    dispatcher,
                )
                .await
            }
            None => false,
        };
        if !monitor_registered {
            drop(route_registration);
            deps.supervisors.lock().await.remove(&fragment);
            deps.placement_states
                .lock()
                .await
                .insert(fragment, FragmentPlacementState::Draining);
            match quiesce_fragment_supervisor(&supervisor).await {
                Ok(()) if deps.supervisor_stops.mark_prepared(fragment, &supervisor) => {
                    supervisor.stop(None);
                    let _ = supervisor.wait(None).await;
                    deps.supervisor_stops.remove_if(fragment, &supervisor);
                    deps.placement_states.lock().await.remove(&fragment);
                }
                Ok(()) => {
                    tracing::error!(
                        fragment = fragment.0,
                        "startup rollback lost exact supervisor ownership"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "startup rollback retained a fragment with an incomplete final flush"
                    );
                }
            }
            return;
        }
        route_registration.disarm();
    }

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
    let mut baselines = HashMap::<String, u64>::new();
    // Trigger every connected candidate rather than trusting replica order.
    // A simultaneously bootstrapping replica is deliberately non-authoritative
    // and will reject the request; any already-serving holder may complete it.
    loop {
        let connected: Vec<String> = {
            let peers = deps.peers.lock().await;
            sources
                .iter()
                .filter(|source| peers.contains_key(*source))
                .cloned()
                .collect()
        };
        for source in connected {
            if baselines.contains_key(&source) {
                continue;
            }
            let Some(before) = deps.sender_rounds(fragment, &source).await else {
                continue;
            };
            let sender = deps
                .senders
                .lock()
                .await
                .get(&(fragment, source.clone()))
                .cloned();
            if let Some(sender) = sender {
                if sender.send(SenderMsg::ReconcileNow).is_ok() {
                    baselines.insert(source, before);
                }
            }
        }
        for (source, before) in &baselines {
            if deps
                .sender_rounds(fragment, source)
                .await
                .is_some_and(|rounds| rounds > *before)
            {
                return true;
            }
        }
        if start.elapsed() > BOOTSTRAP_TIMEOUT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone)]
struct FragmentSenderSnapshot {
    key: (FragmentId, String),
    sender: NodeSender,
    report: Arc<Mutex<SenderStateReport>>,
}

struct DrainedFragmentSenderSnapshot {
    entries: Vec<FragmentSenderSnapshot>,
    _report_guards: Vec<OwnedMutexGuard<SenderStateReport>>,
}

fn snapshot_fragment_sender_set(
    fragment: FragmentId,
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> Option<Vec<FragmentSenderSnapshot>> {
    let mut entries: Vec<FragmentSenderSnapshot> = senders
        .iter()
        .filter(|((sender_fragment, _), _)| *sender_fragment == fragment)
        .map(|(key, sender)| {
            Some(FragmentSenderSnapshot {
                key: key.clone(),
                sender: sender.clone(),
                report: reports.get(key)?.clone(),
            })
        })
        .collect::<Option<_>>()?;
    let report_count = reports
        .keys()
        .filter(|(report_fragment, _)| *report_fragment == fragment)
        .count();
    if report_count != entries.len() {
        return None;
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    Some(entries)
}

async fn lock_drained_fragment_sender_snapshot(
    entries: Vec<FragmentSenderSnapshot>,
) -> Option<DrainedFragmentSenderSnapshot> {
    if entries
        .iter()
        .any(|entry| entry.sender.is_closed() || !entry.sender.ingress_is_idle())
    {
        return None;
    }
    let mut report_guards = Vec::with_capacity(entries.len());
    for entry in &entries {
        report_guards.push(entry.report.clone().lock_owned().await);
    }
    if entries
        .iter()
        .any(|entry| entry.sender.is_closed() || !entry.sender.ingress_is_idle())
        || report_guards.iter().any(|report| {
            report.buffered != 0 || report.in_flight != 0 || report.state == "buffer_reset"
        })
    {
        return None;
    }
    Some(DrainedFragmentSenderSnapshot {
        entries,
        _report_guards: report_guards,
    })
}

fn fragment_sender_snapshot_is_current(
    fragment: FragmentId,
    snapshot: &[FragmentSenderSnapshot],
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> bool {
    senders
        .keys()
        .filter(|(sender_fragment, _)| *sender_fragment == fragment)
        .count()
        == snapshot.len()
        && reports
            .keys()
            .filter(|(report_fragment, _)| *report_fragment == fragment)
            .count()
            == snapshot.len()
        && snapshot.iter().all(|entry| {
            senders
                .get(&entry.key)
                .is_some_and(|sender| sender.same_task(&entry.sender))
                && reports
                    .get(&entry.key)
                    .is_some_and(|report| Arc::ptr_eq(report, &entry.report))
        })
}

#[cfg(test)]
async fn fragment_sender_set_is_drained(
    fragment: FragmentId,
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> bool {
    let Some(snapshot) = snapshot_fragment_sender_set(fragment, senders, reports) else {
        return false;
    };
    lock_drained_fragment_sender_snapshot(snapshot)
        .await
        .is_some()
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

struct FragmentHandoffDrain {
    peer: String,
    sender: NodeSender,
    drain: NodeSenderDrain,
}

fn fragment_placement_snapshot(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> (HashMap<FragmentId, Vec<String>>, Vec<String>, u64) {
    let desired = deps.placement.read().unwrap().clone();
    let replicas = desired.get(&fragment).cloned().unwrap_or_default();
    let generation = deps.placement_generation.load(Ordering::Acquire);
    (desired, replicas, generation)
}

fn fragment_placement_matches(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    expected_replicas: &[String],
    expected_generation: u64,
) -> bool {
    deps.placement
        .read()
        .unwrap()
        .get(&fragment)
        .is_some_and(|replicas| replicas == expected_replicas)
        && deps.placement_generation.load(Ordering::Acquire) == expected_generation
}

async fn routed_fragment_revision(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> Result<u64, String> {
    let actor = deps
        .router
        .route_for_fragment(fragment)
        .ok_or_else(|| "fragment route is absent".to_string())?;
    match actor
        .call(
            |reply| KvMessage::GetRevision { reply },
            Some(REMOTE_RPC_TIMEOUT),
        )
        .await
        .map_err(|error| error.to_string())?
    {
        CallResult::Success(revision) => Ok(revision),
        CallResult::Timeout => Err("fragment revision query timed out".to_string()),
        CallResult::SenderError => Err("fragment revision reply channel dropped".to_string()),
    }
}

/// Remove and join every sender for one fragment. Placement handoff drains
/// permanently seal sender ingress, so any retry or re-add must install fresh
/// sender tasks before it can publish the fragment again.
async fn retire_fragment_senders(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let retiring = {
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let keys: Vec<(FragmentId, String)> = senders
            .keys()
            .filter(|(sender_fragment, _)| *sender_fragment == fragment)
            .cloned()
            .collect();
        let mut retiring = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(sender) = senders.remove(&key) {
                retiring.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(&key);
        }
        retiring
    };
    for sender in retiring {
        sender.join().await;
    }
}

async fn restore_fragment_after_handoff_cancelled(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> bool {
    retire_fragment_senders(deps, fragment).await;
    restore_fragment_if_readded(deps, fragment).await
}

async fn finish_fragment_prefix_retirement(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    supervisor: ActorRef<SupervisorMessage>,
    expected_replicas: Vec<String>,
    expected_generation: u64,
) {
    // The router is irreversibly detached, but the registry keeps the
    // quiesced cache and retryable persistence batch alive. Flush before
    // deletion so no later actor write can recreate the retired namespace.
    let mut next_flush_warning = std::time::Instant::now();
    loop {
        match quiesce_fragment_supervisor(&supervisor).await {
            Ok(()) => {
                if deps.supervisor_stops.mark_prepared(fragment, &supervisor) {
                    break;
                }
                tracing::error!(
                    fragment = fragment.0,
                    "fragment quiesced but supervisor ownership changed"
                );
                return;
            }
            Err(error) => {
                if std::time::Instant::now() >= next_flush_warning {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "fragment remains Draining while its final local flush is incomplete"
                    );
                    next_flush_warning = std::time::Instant::now() + deps.drain_warn_interval;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    let base_store = deps
        .subtree_config
        .store
        .as_ref()
        .expect("prefix retirement requires a configured store")
        .clone();
    let fragment_store = Arc::new(PrefixedStore::new(base_store, fragment));
    let mut next_delete_warning = std::time::Instant::now();
    loop {
        // Placement publication uses this same mutex. Holding it through the
        // synchronous, idempotent delete makes the final revoked-map check and
        // namespace mutation one atomic lifecycle decision.
        let states = deps.placement_states.clone().lock_owned().await;
        if !fragment_placement_matches(deps, fragment, &expected_replicas, expected_generation) {
            tracing::info!(
                fragment = fragment.0,
                "retaining quiesced fragment prefix because placement changed after handoff"
            );
            drop(states);
            break;
        }
        let backend = fragment_store.clone();
        let deleted = deps
            .subtree_config
            .persistence
            .executor()
            .run(move || {
                let _placement_guard = states;
                backend.delete_prefix(&[])
            })
            .await;
        match deleted {
            Ok(PrefixDeleteOutcome::Deleted) => {
                tracing::info!(fragment = fragment.0, "retired durable fragment prefix");
                break;
            }
            Ok(PrefixDeleteOutcome::Retained) => {
                tracing::info!(
                    fragment = fragment.0,
                    "store retained fragment prefix after retirement request"
                );
                break;
            }
            Err(error) => {
                if std::time::Instant::now() >= next_delete_warning {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "fragment remains Draining while durable prefix deletion is incomplete"
                    );
                    next_delete_warning = std::time::Instant::now() + deps.drain_warn_interval;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    supervisor.stop(None);
    let _ = supervisor.wait(None).await;
    if !deps.supervisor_stops.remove_if(fragment, &supervisor) {
        tracing::error!(
            fragment = fragment.0,
            "fragment stopped but exact supervisor ownership was lost"
        );
        return;
    }
    deps.placement_states
        .lock()
        .await
        .insert(fragment, FragmentPlacementState::Stopped);
}

async fn lose_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let prefix_delete_support = deps
        .subtree_config
        .store
        .as_ref()
        .map_or(PrefixDeleteSupport::Retained, |store| {
            store.prefix_delete_support()
        });
    match prefix_delete_support {
        PrefixDeleteSupport::Supported => {
            lose_fragment_with_prefix_retirement(deps, fragment).await
        }
        PrefixDeleteSupport::Retained => lose_fragment_retaining_prefix(deps, fragment).await,
    }
}

/// Retire a persisted fragment only after every replacement replica proves a
/// stable LWW floor from one unchanged source revision. Placement changes,
/// fetched winners, or sender replacement invalidate the attempt and rebuild
/// its sealed sender set from scratch. TTL cleanup may independently
/// materialize an equivalent tombstone after that revision pin.
async fn lose_fragment_with_prefix_retirement(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let mutation_gate = deps.router.mutation_gate(fragment);
    let mutation_revocation = mutation_gate.write_owned().await;
    deps.serving.write().unwrap().remove(&fragment);
    drop(mutation_revocation);

    let mut next_warning = std::time::Instant::now() + deps.drain_warn_interval;
    loop {
        let (desired, replicas, generation) = fragment_placement_snapshot(deps, fragment);
        if replicas.iter().any(|node| node == &deps.node_id) {
            if restore_fragment_after_handoff_cancelled(deps, fragment).await {
                return;
            }
            continue;
        }
        let targets: Vec<String> = replicas
            .iter()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        if targets.is_empty() {
            if std::time::Instant::now() >= next_warning {
                tracing::warn!(
                    fragment = fragment.0,
                    "fragment remains Draining because no replacement replica can certify handoff"
                );
                next_warning = std::time::Instant::now() + deps.drain_warn_interval;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        ensure_fragment_senders(deps, fragment, &desired).await;
        let baseline_revision = match routed_fragment_revision(deps, fragment).await {
            Ok(revision) => revision,
            Err(error) => {
                tracing::warn!(fragment = fragment.0, %error, "cannot read handoff baseline");
                retire_fragment_senders(deps, fragment).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let target_senders: Option<Vec<(String, NodeSender)>> = {
            let senders = deps.senders.lock().await;
            targets
                .iter()
                .map(|peer| {
                    senders
                        .get(&(fragment, peer.clone()))
                        .cloned()
                        .map(|sender| (peer.clone(), sender))
                })
                .collect()
        };
        let Some(target_senders) = target_senders else {
            retire_fragment_senders(deps, fragment).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let mut waiters = Vec::with_capacity(target_senders.len());
        for (peer, sender) in target_senders {
            let Ok(drain) = sender.request_drain_at(ReplicationAckDurability::StableCommitted)
            else {
                waiters.clear();
                break;
            };
            waiters.push(FragmentHandoffDrain {
                peer,
                sender,
                drain,
            });
        }
        if waiters.len() != targets.len() {
            retire_fragment_senders(deps, fragment).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let certified = loop {
            if !fragment_placement_matches(deps, fragment, &replicas, generation) {
                break false;
            }
            if waiters.iter().all(|waiter| waiter.drain.is_complete()) {
                break true;
            }
            if waiters
                .iter()
                .any(|waiter| waiter.drain.is_closed() || waiter.sender.is_closed())
            {
                break false;
            }
            if std::time::Instant::now() >= next_warning {
                let pending: Vec<&str> = waiters
                    .iter()
                    .filter(|waiter| !waiter.drain.is_complete())
                    .map(|waiter| waiter.peer.as_str())
                    .collect();
                tracing::warn!(
                    fragment = fragment.0,
                    ?pending,
                    "fragment remains Draining while stable handoff is incomplete"
                );
                next_warning = std::time::Instant::now() + deps.drain_warn_interval;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        if !certified {
            if replicas.iter().any(|node| node == &deps.node_id)
                && restore_fragment_after_handoff_cancelled(deps, fragment).await
            {
                return;
            }
            retire_fragment_senders(deps, fragment).await;
            continue;
        }

        // Point-of-no-return commit for this attempt. Fence only this
        // fragment's mutations, then atomically seal and detach the exact
        // certified sender generation. The seal is ordered by SenderMap, so a
        // queued ensure cannot recreate a task while the map is unlocked for
        // joins and the final actor revision query.
        let mutation_commit = deps.router.mutation_gate(fragment).write_owned().await;
        let states = deps.placement_states.lock().await;
        if !fragment_placement_matches(deps, fragment, &replicas, generation) {
            drop(states);
            drop(mutation_commit);
            retire_fragment_senders(deps, fragment).await;
            continue;
        }
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let exact_senders = senders
            .iter()
            .filter(|((sender_fragment, _), _)| *sender_fragment == fragment)
            .count()
            == waiters.len()
            && waiters.iter().all(|waiter| {
                waiter.drain.is_complete()
                    && senders
                        .get(&(fragment, waiter.peer.clone()))
                        .is_some_and(|sender| sender.same_task(&waiter.sender))
                    && reports.contains_key(&(fragment, waiter.peer.clone()))
            })
            && reports
                .keys()
                .filter(|(report_fragment, _)| *report_fragment == fragment)
                .count()
                == waiters.len();
        if !exact_senders {
            drop(reports);
            drop(senders);
            drop(mutation_commit);
            drop(states);
            retire_fragment_senders(deps, fragment).await;
            continue;
        }
        let Some(finalization) = deps.finalizing_senders.seal(fragment) else {
            drop(reports);
            drop(senders);
            drop(states);
            drop(mutation_commit);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };

        let mut retiring_senders = Vec::with_capacity(waiters.len());
        for waiter in &waiters {
            let key = (fragment, waiter.peer.clone());
            if let Some(sender) = senders.remove(&key) {
                retiring_senders.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(&key);
        }
        drop(reports);
        drop(senders);
        drop(states);

        for sender in retiring_senders {
            sender.join().await;
        }
        let final_revision = routed_fragment_revision(deps, fragment).await;
        if final_revision != Ok(baseline_revision) {
            tracing::debug!(
                fragment = fragment.0,
                baseline_revision,
                ?final_revision,
                "fragment changed during handoff; retrying every replacement replica"
            );
            drop(finalization);
            drop(mutation_commit);
            continue;
        }

        // Placement publication remained available during the potentially
        // slow joins/RPC. Revalidate its exact generation before committing
        // route removal; P -> Q -> P is rejected by placement_generation.
        let states = deps.placement_states.lock().await;
        if !fragment_placement_matches(deps, fragment, &replicas, generation) {
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            continue;
        }
        let senders = deps.senders.lock().await;
        let reports = deps.sender_reports.lock().await;
        let sender_set_remains_detached = !senders
            .keys()
            .any(|(sender_fragment, _)| *sender_fragment == fragment)
            && !reports
                .keys()
                .any(|(report_fragment, _)| *report_fragment == fragment);
        if !sender_set_remains_detached {
            drop(reports);
            drop(senders);
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            continue;
        }

        let mut active_supervisors = deps.supervisors.lock().await;
        let active_supervisor = active_supervisors.get(&fragment).cloned();
        let registered_supervisor = deps.supervisor_stops.get(fragment);
        let ownership_matches = match (&active_supervisor, &registered_supervisor) {
            (Some(active), Some(registered)) => active.get_id() == registered.actor.get_id(),
            _ => false,
        };
        if !ownership_matches {
            drop(active_supervisors);
            drop(reports);
            drop(senders);
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            tracing::error!(
                fragment = fragment.0,
                "refusing to destroy fragment without exact supervisor ownership"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let supervisor = active_supervisors
            .remove(&fragment)
            .expect("ownership checked above");
        deps.router.remove(fragment);
        drop(active_supervisors);
        drop(reports);
        drop(senders);
        drop(states);
        drop(finalization);
        drop(mutation_commit);

        finish_fragment_prefix_retirement(deps, fragment, supervisor, replicas, generation).await;
        return;
    }
}

/// Lose a fragment: stop serving new local ops (they forward to the
/// new owner), drain replication senders, then stop the subtree.
async fn lose_fragment_retaining_prefix(deps: &PlacementDriverDeps, fragment: FragmentId) {
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
        let targets: Vec<(NodeSender, BudgetedPeerTx)> = deps
            .senders
            .lock()
            .await
            .iter()
            .filter_map(|((sender_fragment, peer), sender)| {
                (*sender_fragment == fragment)
                    .then(|| {
                        peers
                            .get(peer)
                            .map(|tx| (sender.clone(), tx.budgeted_sender()))
                    })
                    .flatten()
            })
            .collect();
        for (sender, peer_tx) in targets {
            let _ = sender.connect_peer(peer_tx);
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

        let sender_snapshot = {
            let senders = deps.senders.lock().await;
            let reports = deps.sender_reports.lock().await;
            snapshot_fragment_sender_set(fragment, &senders, &reports)
        };
        let drained = match sender_snapshot {
            Some(snapshot) => lock_drained_fragment_sender_snapshot(snapshot)
                .await
                .is_some(),
            None => false,
        };
        if drained {
            // Final teardown commit. `placement_states` serializes this
            // validation with placement publication. Acquire the per-fragment
            // mutation fence before any global registry, then await report
            // state without those registries held.
            let mutation_commit = deps.router.mutation_gate(fragment).write_owned().await;
            let final_snapshot = {
                let senders = deps.senders.lock().await;
                let reports = deps.sender_reports.lock().await;
                snapshot_fragment_sender_set(fragment, &senders, &reports)
            };
            let Some(final_snapshot) = final_snapshot else {
                drop(mutation_commit);
                continue;
            };
            let Some(drained) = lock_drained_fragment_sender_snapshot(final_snapshot).await else {
                drop(mutation_commit);
                continue;
            };

            let mut states = deps.placement_states.lock().await;
            let still_revoked = deps
                .placement
                .read()
                .unwrap()
                .get(&fragment)
                .is_none_or(|replicas| !replicas.iter().any(|node| node == &deps.node_id));
            if !still_revoked {
                // A re-add raced the final drain check. Keep the subtree
                // non-serving and let the top of the loop realign senders and
                // reconcile before publishing it again.
                states.insert(fragment, FragmentPlacementState::Bootstrapping);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                continue;
            }

            let mut senders = deps.senders.lock().await;
            let mut reports = deps.sender_reports.lock().await;
            if !fragment_sender_snapshot_is_current(fragment, &drained.entries, &senders, &reports)
            {
                drop(reports);
                drop(senders);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                continue;
            }

            // Detach every admission/recreation path before the first teardown
            // wait. A queued ensure call cannot acquire `senders` until after
            // the router is absent, and an inbound mutation cannot acquire its
            // shared lease until after the same point.
            let mut active_supervisors = deps.supervisors.lock().await;
            let active_supervisor = active_supervisors.get(&fragment).cloned();
            let registered_supervisor = deps.supervisor_stops.get(fragment);
            let ownership_matches = match (&active_supervisor, &registered_supervisor) {
                (Some(active), Some(registered)) => active.get_id() == registered.actor.get_id(),
                _ => false,
            };
            if !ownership_matches {
                drop(active_supervisors);
                drop(reports);
                drop(senders);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                tracing::error!(
                    fragment = fragment.0,
                    "refusing to destroy fragment without exact supervisor ownership"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let supervisor = active_supervisors
                .remove(&fragment)
                .expect("ownership checked above");
            let mut retiring_senders = Vec::with_capacity(drained.entries.len());
            for entry in &drained.entries {
                if let Some(sender) = senders.remove(&entry.key) {
                    retiring_senders.push(deps.retiring_senders.retire(sender));
                }
                reports.remove(&entry.key);
            }
            drop(active_supervisors);
            deps.router.remove(fragment);
            drop(reports);
            drop(senders);
            drop(states);
            drop(drained);
            drop(mutation_commit);

            for sender in retiring_senders {
                sender.join().await;
            }

            // The router is now irreversibly detached, but the registry keeps
            // the quiesced cache and retryable persistence batch alive. A
            // store outage leaves placement visibly Draining; a re-add waits
            // for this teardown to finish and starts a fresh, durable subtree.
            let mut next_flush_warning = std::time::Instant::now();
            loop {
                match quiesce_fragment_supervisor(&supervisor).await {
                    Ok(()) => {
                        if deps.supervisor_stops.mark_prepared(fragment, &supervisor) {
                            break;
                        }
                        tracing::error!(
                            fragment = fragment.0,
                            "fragment quiesced but supervisor ownership changed"
                        );
                        return;
                    }
                    Err(error) => {
                        if std::time::Instant::now() >= next_flush_warning {
                            tracing::warn!(
                                fragment = fragment.0,
                                %error,
                                "fragment remains Draining while its final local flush is incomplete"
                            );
                            next_flush_warning =
                                std::time::Instant::now() + deps.drain_warn_interval;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            if !deps.supervisor_stops.remove_if(fragment, &supervisor) {
                tracing::error!(
                    fragment = fragment.0,
                    "fragment stopped but exact supervisor ownership was lost"
                );
                return;
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

struct ServedReconcileState {
    session_id: u64,
    round_id: u64,
    next_page_seq: u64,
    next_after_key: String,
    snapshot_revision: u64,
    snapshot_time_ms: u64,
    next_fetch_chunk_seq: u64,
    advertised_keys: HashSet<String>,
    /// Subtree incarnation which produced the summaries/fetch authorization.
    /// Dispatcher restarts preserve this generation because they share state;
    /// destructive placement remove/re-add cycles allocate a new one.
    route_generation: u64,
    recovery_epoch: Vec<u8>,
    recovery_replica_digest: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InboundSession {
    session_id: u64,
    generation: u64,
}

type InboundSessions = Arc<Mutex<HashMap<(String, FragmentId), InboundSession>>>;

struct ReconcileResponseJob {
    peer: String,
    session: InboundSession,
    actor: ActorRef<KvMessage>,
    fetch: ReconcileFetch,
    snapshot_time_ms: u64,
}

/// Keep the largest ordered summary prefix which obeys the soft page target,
/// allowing one individually valid summary to exceed it up to the hard frame
/// limit. Cursor replacement makes prefix sizes non-monotonic (a later key can
/// be shorter), so every prefix is evaluated once instead of stopping at the
/// first overflow or repeatedly rescanning while popping a suffix.
fn fit_reconcile_response_page(
    response: &mut ReconcileResponse,
    after_key: &str,
    source_done: bool,
) -> Result<usize, usize> {
    let original_len = response.summaries.len();
    let base_payload_len = ReconcileResponse {
        fragment: response.fragment,
        summaries: Vec::new(),
        session_id: response.session_id,
        round_id: response.round_id,
        page_seq: response.page_seq,
        next_after_key: String::new(),
        snapshot_revision: response.snapshot_revision,
        snapshot_time_ms: response.snapshot_time_ms,
        done: false,
        restart: response.restart,
    }
    .encoded_len();

    if original_len == 0 {
        response.next_after_key = after_key.to_string();
        response.done = source_done;
        let payload_len = base_payload_len
            .saturating_add(bytes_field_len(6, response.next_after_key.len()))
            .saturating_add(varint_field_len(9, u64::from(response.done)));
        let encoded_len = message_field_len(12, payload_len);
        return if encoded_len <= MAX_FRAME_BYTES {
            Ok(encoded_len)
        } else {
            Err(encoded_len)
        };
    }

    let mut summaries_payload_len = 0usize;
    let mut singleton_encoded_len = 0usize;
    let mut largest_fitting = None;
    for (index, summary) in response.summaries.iter().enumerate() {
        summaries_payload_len =
            summaries_payload_len.saturating_add(message_field_len(2, summary.encoded_len()));
        let count = index + 1;
        let done = source_done && count == original_len;
        let payload_len = base_payload_len
            .saturating_add(summaries_payload_len)
            .saturating_add(bytes_field_len(6, summary.key.len()))
            .saturating_add(varint_field_len(9, u64::from(done)));
        let encoded_len = message_field_len(12, payload_len);
        if count == 1 {
            singleton_encoded_len = encoded_len;
        }
        if encoded_len <= ANTI_ENTROPY_PAGE_BYTES || (count == 1 && encoded_len <= MAX_FRAME_BYTES)
        {
            largest_fitting = Some((count, encoded_len));
        }
    }

    let Some((keep, encoded_len)) = largest_fitting else {
        return Err(singleton_encoded_len);
    };
    response.summaries.truncate(keep);
    response.next_after_key = response
        .summaries
        .last()
        .expect("a non-empty prefix was selected")
        .key
        .clone();
    response.done = source_done && keep == original_len;
    debug_assert_eq!(message_field_len(12, response.encoded_len()), encoded_len);
    Ok(encoded_len)
}

/// Payload base for a data page, conservatively reserving the terminal
/// `done=true` field before any entries are admitted.
fn reconcile_data_reserved_base_len(page: &ReconcileData) -> usize {
    debug_assert!(!page.done);
    page.encoded_len().saturating_add(varint_field_len(7, 1))
}

/// Read an authorized fetch through actor-owned memory-bounded value pages and
/// emit exact wire-byte-bounded pages. The peer writer's byte semaphore bounds
/// encoded pages waiting on the socket.
async fn send_reconcile_data_pages(
    actor: &ActorRef<KvMessage>,
    tx: &PeerChannel,
    fetch: ReconcileFetch,
    snapshot_time_ms: u64,
    admission_timeout: Duration,
) -> bool {
    let ReconcileFetch {
        fragment,
        session_id,
        round_id,
        chunk_seq,
        keys,
    } = fetch;
    let keys = Arc::new(keys);
    let mut next_key_index = 0usize;
    let mut page = ReconcileData {
        fragment,
        entries: Vec::new(),
        session_id,
        round_id,
        chunk_seq,
        page_seq: 0,
        done: false,
    };
    // Reserve the terminal flag on every candidate. Non-terminal pages encode
    // two bytes smaller, while the final page cannot cross the soft target
    // merely because `done=true` is set after the last value arrives.
    let mut base_payload_len = reconcile_data_reserved_base_len(&page);
    let mut sizing = RepeatedFieldSizer::new(14, 2, base_payload_len);

    while next_key_index < keys.len() {
        let request_keys = keys.clone();
        let value_page = match actor
            .call(
                move |reply| KvMessage::GetVersionedPage {
                    keys: request_keys,
                    start_index: next_key_index,
                    snapshot_time_ms: Some(snapshot_time_ms),
                    reply,
                },
                None,
            )
            .await
        {
            Ok(CallResult::Success(value_page)) => value_page,
            _ => return false,
        };
        if value_page.next_index <= next_key_index || value_page.next_index > keys.len() {
            return false;
        }
        next_key_index = value_page.next_index;
        for (key, value) in value_page.entries {
            let entry = SyncEntry {
                key,
                value: value.value,
                timestamp_ms: value.timestamp_ms,
                logical_counter: value.logical_counter,
                node_id: value.node_id,
                expires_at_ms: value.expires_at_ms.unwrap_or(0),
                is_tombstone: value.is_tombstone,
            };

            let item_len = entry.encoded_len();
            let encoded_len = sizing.encoded_len_with(item_len);
            if encoded_len > MAX_FRAME_BYTES && page.entries.is_empty() {
                tracing::warn!(
                    fragment,
                    round_id,
                    chunk_seq,
                    encoded_len,
                    "one reconcile value exceeds the hard frame limit"
                );
                return false;
            }
            if encoded_len > MAX_FRAME_BYTES
                || (encoded_len > ANTI_ENTROPY_PAGE_BYTES && !page.entries.is_empty())
            {
                let next_page_seq = page.page_seq.saturating_add(1);
                if !try_send_envelope_with_admission_timeout(
                    tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(page)),
                    },
                    admission_timeout,
                )
                .await
                {
                    return false;
                }
                page = ReconcileData {
                    fragment,
                    entries: Vec::new(),
                    session_id,
                    round_id,
                    chunk_seq,
                    page_seq: next_page_seq,
                    done: false,
                };
                base_payload_len = reconcile_data_reserved_base_len(&page);
                sizing.reset(base_payload_len);
                if sizing.encoded_len_with(item_len) > MAX_FRAME_BYTES {
                    tracing::warn!(
                        fragment,
                        round_id,
                        chunk_seq,
                        encoded_len = sizing.encoded_len_with(item_len),
                        "one reconcile value exceeds the hard frame limit"
                    );
                    return false;
                }
            }
            sizing.push(item_len);
            page.entries.push(entry);
        }
    }

    page.done = true;
    try_send_envelope_with_admission_timeout(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(page)),
        },
        admission_timeout,
    )
    .await
}

/// Stream fetch responses outside the connection reader. Keeping outbound
/// byte admission off that reader is essential: two peers may fetch large
/// divergent values simultaneously, and each reader must remain available to
/// drain the other peer's data while its own writer is backpressured.
async fn run_reconcile_responder(
    mut jobs: mpsc::Receiver<ReconcileResponseJob>,
    tx: PeerChannel,
    sessions: InboundSessions,
    mut session_changes: watch::Receiver<u64>,
    admission_timeout: Duration,
) {
    let mut cancelled = tx.cancel.subscribe();
    'jobs: loop {
        if *cancelled.borrow() {
            return;
        }
        let job = tokio::select! {
            changed = cancelled.changed() => {
                let _ = changed;
                return;
            }
            job = jobs.recv() => match job {
                Some(job) => job,
                None => return,
            }
        };
        let session_key = (job.peer.clone(), FragmentId(job.fetch.fragment));
        if sessions.lock().await.get(&session_key).copied() != Some(job.session) {
            continue;
        }

        let response = send_reconcile_data_pages(
            &job.actor,
            &tx,
            job.fetch,
            job.snapshot_time_ms,
            admission_timeout,
        );
        tokio::pin!(response);
        loop {
            tokio::select! {
                changed = cancelled.changed() => {
                    let _ = changed;
                    return;
                }
                changed = session_changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if sessions.lock().await.get(&session_key).copied() != Some(job.session) {
                        // Dropping the response future cancels an in-progress
                        // actor RPC or outbound admission wait for the stale
                        // session without disturbing the replacement session.
                        continue 'jobs;
                    }
                }
                sent = &mut response => {
                    if !sent {
                        tx.cancel();
                        return;
                    }
                    continue 'jobs;
                }
            }
        }
    }
}

/// The byte permit follows the raw allocation through decoding and the full
/// inline handler turn. Decoded or forwarded copies are separate allocations.
struct ReceivedFrame {
    bytes: Vec<u8>,
    _byte_permit: OwnedSemaphorePermit,
}

async fn read_connection_frame<R>(
    reader: &mut R,
    cancel: &mut watch::Receiver<bool>,
    receive_byte_budget: &Arc<Semaphore>,
    established: bool,
    handshake_deadline: Instant,
    frame_body_timeout: Duration,
) -> Option<ReceivedFrame>
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

    let byte_permit = match receive_byte_budget.clone().try_acquire_many_owned(prefix) {
        Ok(permit) => permit,
        Err(_) => {
            // Waiting here could stop the only reader which can drain the
            // opposite direction. No frame is decoded or applied on failure.
            tracing::warn!(len, "closing connection at node receive byte capacity");
            return None;
        }
    };
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
        result = reader.read_exact(&mut frame) => result.ok().map(|_| ReceivedFrame {
            bytes: frame,
            _byte_permit: byte_permit,
        }),
    }
}

fn validate_peer_hello(
    local_node_id: &str,
    expected_peer_id: Option<&str>,
    hello: &Hello,
) -> Result<(), &'static str> {
    if hello.node_id.is_empty() || hello.node_id.len() > MAX_WIRE_NODE_ID_BYTES {
        return Err("peer node_id is empty or exceeds 255 bytes");
    }
    if hello.node_id == local_node_id {
        return Err("peer claimed the local node_id");
    }
    if expected_peer_id.is_some_and(|expected| expected != hello.node_id) {
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
        recovery,
        node_id,
        router,
        serving,
        placement,
        dynamic_placement,
        pending_gets,
        pending_batch_gets,
        pending_scans,
        pending_query_pages,
        query_admission,
        pending_mutations,
        pending_relays,
        pending_mutation_relays,
        next_req_id,
        peers,
        peer_failures,
        tasks,
        read_service,
        operation_gate,
        peer_changes,
        hlc,
        senders,
        finalizing_senders,
        retiring_senders,
        sender_config,
        sender_runtime,
        runtime_options,
        receive_byte_budget,
        node_peer_queue_budget,
        relay_admission,
        mutation_service,
        sender_reports,
        membership_tx,
        cluster_secret,
        #[cfg(test)]
        sender_snapshot_hook,
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
    let sessions: InboundSessions = Arc::new(Mutex::new(HashMap::new()));
    let (session_changes, session_change_rx) = watch::channel(0u64);
    let mut next_session_generation = 1u64;
    // An untracked reconciliation repair has no individual ACK. Remember its
    // first failure so the following tracked empty barrier NACKs instead of
    // falsely declaring the session converged. A fresh SessionStart clears
    // the poison and lets the sender retry the complete round.
    let mut reconcile_failures: HashMap<(String, FragmentId), (u64, String)> = HashMap::new();
    // Bounded responder-side authorization. Only the exact next cursor page
    // and keys advertised by the most recent page may be fetched.
    let mut served_reconciles: HashMap<(String, FragmentId), ServedReconcileState> = HashMap::new();
    let (mut reader, writer) = tokio::io::split(stream);
    let socket_write_timeout = runtime_options.socket_write_timeout;
    let peer_queue_frame_capacity = runtime_options.peer_queue_frame_capacity;
    let peer_queue_byte_capacity = runtime_options.peer_queue_byte_capacity;
    // Split the configured frame budget between ordinary producer traffic and
    // reader-generated responses. The writer drains responses first, avoiding
    // the symmetric full-queue deadlock where both peers need to emit an ACK
    // before either can make progress.
    let response_frame_capacity = peer_queue_frame_capacity / 2;
    let producer_frame_capacity = peer_queue_frame_capacity - response_frame_capacity;
    let (frame_tx, rx) = mpsc::channel::<BudgetedFrame>(producer_frame_capacity);
    let (response_tx, response_rx) = mpsc::channel::<BudgetedFrame>(response_frame_capacity);
    // `SenderMsg::PeerDisconnected` is public and historically identifies a
    // transport with Sender<Vec<u8>>. Keep a live, unsent compatibility token;
    // Node frames exclusively use the budgeted channel above.
    let (identity, _identity_rx) = mpsc::channel::<Vec<u8>>(1);
    let (cancel, _) = watch::channel(false);
    let byte_budget = Arc::new(Semaphore::new(peer_queue_byte_capacity));
    let producer_byte_budget = Arc::new(Semaphore::new(
        peer_queue_byte_capacity - PEER_RESPONSE_BYTE_RESERVE,
    ));
    let mut tx = PeerChannel {
        outbound: BudgetedPeerTx {
            node_byte_budget: node_peer_queue_budget,
            tx: frame_tx,
            response_tx,
            byte_budget: byte_budget.clone(),
            producer_byte_budget: producer_byte_budget.clone(),
            reconcile_gate: ReconcileGate::new(),
            identity,
        },
        cancel: cancel.clone(),
        queue_admission_timeout: runtime_options.queue_admission_timeout,
        rank: None,
        capabilities: REQUIRED_WIRE_CAPABILITIES | CAPABILITY_SCAN | CAPABILITY_QUERY_PAGES,
    };
    let writer_cancel = cancel.subscribe();
    // Abort/early-return must cancel jobs bound to this exact physical socket.
    let _connection_cancel = ConnectionCancelGuard(tx.clone());
    // Subscribe before the writer or handshake can signal cancellation;
    // subscribing after an early failure would mark that value as already
    // observed and leave the read side waiting forever.
    let mut read_cancel = cancel.subscribe();
    let writer_cancel_tx = cancel.clone();
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
                    run_bounded_peer_writer(
                        writer,
                        PeerWriterPipeline {
                            data_rx: rx,
                            response_rx,
                            byte_budget,
                            producer_byte_budget,
                        },
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

    // Fetch responses can span multiple near-limit frames. Produce them in a
    // bounded connection-owned worker so the reader always keeps consuming
    // the opposite direction and cannot participate in a bidirectional
    // socket/byte-budget deadlock.
    let (reconcile_response_tx, reconcile_response_rx) =
        mpsc::channel(RECONCILE_RESPONSE_QUEUE_DEPTH);
    let responder_registered = match tasks.upgrade() {
        Some(tasks) => {
            let responder_tx = tx.clone();
            let admission_timeout = runtime_options.reconcile_data_admission_timeout;
            tokio::time::timeout_at(
                handshake_deadline,
                tasks.spawn(run_reconcile_responder(
                    reconcile_response_rx,
                    responder_tx,
                    sessions.clone(),
                    session_change_rx,
                    admission_timeout,
                )),
            )
            .await
            .unwrap_or(false)
        }
        None => false,
    };
    if !responder_registered {
        tx.cancel();
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
        relay_admission,
    };

    let mut handshake = HandshakeState::AwaitingHello;
    let mut authenticated_peer_id = None;
    let mut was_established = false;
    // Length-delimited framing: 4-byte big-endian length prefix.
    'connection: loop {
        let Some(buf) = read_connection_frame(
            &mut reader,
            &mut read_cancel,
            &receive_byte_budget,
            handshake.is_established(),
            handshake_deadline,
            runtime_options.socket_read_timeout,
        )
        .await
        else {
            break;
        };
        if let Err(error) = preflight_envelope_decode(&buf.bytes) {
            tracing::warn!(%error, "rejecting inbound peer frame before protobuf allocation");
            break;
        }
        let envelope = match Envelope::decode(&buf.bytes[..]) {
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
                tx.capabilities = peer_hello.capabilities;
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
                    cancel_connection_relays(&previous, &pending_relays, &pending_mutation_relays);
                }
                peer_changes.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
                handshake = HandshakeState::Established { peer: peer.clone() };
                was_established = true;

                // Both static and dynamic placement use the same correlated,
                // byte-bounded reconciliation protocol. The legacy
                // SyncRequest/SyncResponse exchange is deliberately retired:
                // keeping a second monolithic bootstrap path would re-open
                // the frame and memory bounds enforced by protocol v3.
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
                                finalizing_senders: &finalizing_senders,
                                retiring_senders: &retiring_senders,
                                reports: &sender_reports,
                                peer_failures: &peer_failures,
                                operation_gate: &operation_gate,
                                allow_quiescing: false,
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
                        .is_some_and(|current| current.same_channel(&tx))
                    {
                        break;
                    }
                    let peer_senders: Vec<NodeSender> = senders
                        .lock()
                        .await
                        .iter()
                        .filter(|((_, sender_peer), _)| sender_peer == &peer)
                        .map(|(_, sender)| sender.clone())
                        .collect();
                    for sender_tx in peer_senders {
                        let _ = sender_tx.connect_peer(tx.budgeted_sender());
                        if !dynamic_placement {
                            let _ = sender_tx.send(SenderMsg::ReconcileNow);
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
                    let changed = {
                        let mut sessions = sessions.lock().await;
                        if sessions
                            .get(&key)
                            .is_some_and(|session| session.session_id == s.session_id)
                        {
                            false
                        } else {
                            let generation = next_session_generation;
                            next_session_generation =
                                next_session_generation.wrapping_add(1).max(1);
                            sessions.insert(
                                key.clone(),
                                InboundSession {
                                    session_id: s.session_id,
                                    generation,
                                },
                            );
                            true
                        }
                    };
                    if changed {
                        reconcile_failures.remove(&key);
                        served_reconciles.remove(&key);
                        session_changes.send_modify(|generation| {
                            *generation = generation.wrapping_add(1);
                        });
                    }
                }
            }
            Some(EnvelopeMsg::ReplicateBatch(b)) => {
                if b.ops.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(item_count = b.ops.len(), "replication batch is too large");
                    break;
                }
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
                if current.map(|session| session.session_id) != Some(b.session_id) {
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
                let reconcile_barrier = tracked && b.ops.is_empty();
                let requested_ack = if tracked {
                    match ReplicationAckLevel::try_from(b.required_ack_level) {
                        Ok(ReplicationAckLevel::Applied) => {
                            Some(ReplicationAckDurability::MemoryApplied)
                        }
                        Ok(ReplicationAckLevel::StoreCommitted) => {
                            Some(ReplicationAckDurability::StoreCommitted)
                        }
                        Ok(ReplicationAckLevel::StableCommitted) => {
                            Some(ReplicationAckDurability::StableCommitted)
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
                let route = router.route_with_generation(frag);
                let actor = route.as_ref().map(|(_, actor)| actor.clone());
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

                if failure.is_none() && reconcile_barrier {
                    let source_authorized = serving.read().unwrap().contains(&frag)
                        || served_reconciles.get(&session_key).is_some_and(|state| {
                            recovery.as_ref().is_some_and(|authority| {
                                authority.authorizes(
                                    frag,
                                    &peer,
                                    &state.recovery_epoch,
                                    &state.recovery_replica_digest,
                                    &placement.read().unwrap(),
                                )
                            })
                        });
                    let same_serving_incarnation = source_authorized
                        && route.as_ref().is_some_and(|(generation, _)| {
                            served_reconciles.get(&session_key).is_some_and(|state| {
                                state.session_id == b.session_id
                                    && state.route_generation == *generation
                            })
                        });
                    if !same_serving_incarnation {
                        failure = Some(format!(
                            "reconciliation barrier no longer matches the serving incarnation of fragment {frag}"
                        ));
                    }
                }

                if failure.is_none() {
                    if let Some(timestamp_ms) = b
                        .ops
                        .iter()
                        .map(|op| {
                            actordb_protocol::RecordVersion::new(
                                op.timestamp_ms,
                                op.logical_counter,
                            )
                        })
                        .max()
                    {
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
                                let mut v = VersionedValue::new_version(
                                    op.value,
                                    actordb_protocol::RecordVersion::new(
                                        op.timestamp_ms,
                                        op.logical_counter,
                                    ),
                                    op.node_id,
                                );
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
                        if let Err(error) = validate_replicated_op(&replicated) {
                            failure = Some(error.to_string());
                            break;
                        }
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
                        (
                            true,
                            Some(
                                required @ (ReplicationAckDurability::StoreCommitted
                                | ReplicationAckDurability::StableCommitted),
                            ),
                        ) => {
                            match flush_fence(actor.as_ref().expect("validated actor"), required)
                                .await
                            {
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

                if reconcile_barrier {
                    // The empty tracked batch is the reconcile durability
                    // barrier. Its ACK or NACK ends this responder round, so
                    // no fetch authorization may survive into a retry.
                    served_reconciles.remove(&session_key);
                }

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
                            ApplyDurability::StableCommitted => {
                                ReplicationAckLevel::StableCommitted
                            }
                        };
                        EnvelopeMsg::BatchAck(BatchAck {
                            session_id: b.session_id,
                            batch_seq: b.batch_seq,
                            fragment: b.fragment,
                            level: level as i32,
                        })
                    };
                    if !send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(response),
                        },
                    ) {
                        break;
                    }
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
                        Ok(ReplicationAckLevel::StableCommitted) => {
                            ReplicationAckDurability::StableCommitted
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
                    let sender_key = (FragmentId(a.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        if tx
                            .send(SenderMsg::Ack {
                                session_id: a.session_id,
                                batch_seq: a.batch_seq,
                                durability,
                            })
                            .is_err()
                        {
                            let still_current =
                                sender_task_is_current(&senders, &sender_key, &tx).await;
                            if still_current {
                                break;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::BatchNack(n)) => {
                if let Some(peer) = peer_node_id.clone() {
                    let sender_key = (FragmentId(n.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        if tx
                            .send(SenderMsg::Nack {
                                session_id: n.session_id,
                                batch_seq: n.batch_seq,
                                error: n.error,
                            })
                            .is_err()
                        {
                            let still_current =
                                sender_task_is_current(&senders, &sender_key, &tx).await;
                            if still_current {
                                break;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileRequest(r)) => {
                // Peer wants one stable, ordered page of version summaries.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(r.fragment);
                let current = sessions.lock().await.get(&(peer.clone(), frag)).copied();
                // Both identifiers are nonzero so old senders, whose newly
                // added scalar fields decode to defaults, fail closed.
                if r.session_id == 0
                    || r.round_id == 0
                    || current.map(|session| session.session_id) != Some(r.session_id)
                {
                    tracing::debug!(
                        fragment = r.fragment,
                        session_id = r.session_id,
                        round_id = r.round_id,
                        "ignoring stale or uncorrelated reconcile request"
                    );
                    continue;
                }
                let recovery_authorized = recovery.as_ref().is_some_and(|authority| {
                    authority.authorizes(
                        frag,
                        &peer,
                        &r.recovery_epoch,
                        &r.recovery_replica_digest,
                        &placement.read().unwrap(),
                    )
                });
                if !serving.read().unwrap().contains(&frag) && !recovery_authorized {
                    // Independent placement observers can legitimately race:
                    // the serving replica may start its routine round before
                    // this node has finished bootstrapping the same fragment.
                    // Ignore that round so its sender times out/retries, while
                    // preserving the multiplexed connection for this node's
                    // reverse bootstrap and for unrelated fragments.
                    tracing::debug!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        "ignoring reconciliation for a non-serving fragment"
                    );
                    continue;
                }
                let first_page = r.page_seq == 0;
                let reconcile_key = (peer.clone(), frag);
                if (first_page
                    && (!r.after_key.is_empty()
                        || r.snapshot_revision != 0
                        || r.snapshot_time_ms != 0))
                    || (!first_page && r.snapshot_time_ms == 0)
                {
                    tracing::warn!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        page_seq = r.page_seq,
                        "rejecting malformed reconcile page request"
                    );
                    break;
                }
                let route = router.route_with_generation(frag);
                if !first_page
                    && !served_reconciles
                        .get(&reconcile_key)
                        .zip(route.as_ref())
                        .is_some_and(|(state, (generation, _))| {
                            state.route_generation == *generation
                        })
                {
                    // A legitimate placement remove/re-add can invalidate a
                    // paged round between frames. Drop its authorization and
                    // let the correlated sender rotate/retry without closing
                    // the multiplexed connection.
                    served_reconciles.remove(&reconcile_key);
                    continue;
                }
                if !first_page
                    && !served_reconciles.get(&reconcile_key).is_some_and(|state| {
                        state.session_id == r.session_id
                            && state.round_id == r.round_id
                            && state.next_page_seq == r.page_seq
                            && state.next_after_key == r.after_key
                            && state.snapshot_revision == r.snapshot_revision
                            && state.snapshot_time_ms == r.snapshot_time_ms
                    })
                {
                    tracing::warn!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        page_seq = r.page_seq,
                        "rejecting unexpected reconcile continuation"
                    );
                    break;
                }
                if let Some((route_generation, actor)) = route {
                    if let Ok(CallResult::Success(page)) = actor
                        .call(
                            |reply| KvMessage::GetSummaryPage {
                                after_key: (!first_page).then_some(r.after_key.clone()),
                                // Do not hold or clone a fragment-wide
                                // snapshot across network turns. Revision
                                // drift is returned to the requester, which
                                // completes this bounded pass and then runs a
                                // mutation-free verification pass.
                                expected_revision: None,
                                snapshot_time_ms: (!first_page).then_some(r.snapshot_time_ms),
                                limit: RECONCILE_SUMMARY_PAGE_ITEMS,
                                reply,
                            },
                            None,
                        )
                        .await
                    {
                        let summaries: Vec<KeySummary> = page
                            .summaries
                            .into_iter()
                            .map(|summary| KeySummary {
                                key: summary.key,
                                timestamp_ms: summary.timestamp_ms,
                                logical_counter: summary.logical_counter,
                                node_id: summary.node_id,
                                is_tombstone: summary.is_tombstone,
                                content_digest: summary.content_digest.to_vec(),
                            })
                            .collect();
                        let mut response = ReconcileResponse {
                            fragment: r.fragment,
                            summaries,
                            session_id: r.session_id,
                            round_id: r.round_id,
                            page_seq: r.page_seq,
                            next_after_key: String::new(),
                            snapshot_revision: page.revision,
                            snapshot_time_ms: page.snapshot_time_ms,
                            done: false,
                            restart: page.restart,
                        };
                        if let Err(encoded_len) =
                            fit_reconcile_response_page(&mut response, &r.after_key, page.done)
                        {
                            tracing::warn!(
                                peer = %peer,
                                fragment = r.fragment,
                                round_id = r.round_id,
                                page_seq = r.page_seq,
                                encoded_len,
                                "one reconcile summary exceeds the hard frame limit"
                            );
                            break 'connection;
                        }
                        let (
                            snapshot_revision,
                            snapshot_time_ms,
                            next_fetch_chunk_seq,
                            served_route_generation,
                        ) = if first_page {
                            (
                                response.snapshot_revision,
                                response.snapshot_time_ms,
                                0,
                                route_generation,
                            )
                        } else {
                            let state = served_reconciles
                                .get(&reconcile_key)
                                .expect("validated continuation");
                            (
                                state.snapshot_revision,
                                state.snapshot_time_ms,
                                state.next_fetch_chunk_seq,
                                state.route_generation,
                            )
                        };
                        let served_state = (!response.restart).then(|| ServedReconcileState {
                            recovery_epoch: r.recovery_epoch.clone(),
                            recovery_replica_digest: r.recovery_replica_digest.clone(),
                            session_id: response.session_id,
                            round_id: response.round_id,
                            next_page_seq: response.page_seq.saturating_add(1),
                            next_after_key: response.next_after_key.clone(),
                            snapshot_revision,
                            snapshot_time_ms,
                            next_fetch_chunk_seq,
                            advertised_keys: response
                                .summaries
                                .iter()
                                .map(|summary| summary.key.clone())
                                .collect(),
                            route_generation: served_route_generation,
                        });
                        let envelope = Envelope {
                            msg: Some(EnvelopeMsg::ReconcileResponse(response)),
                        };
                        if !try_send_envelope_immediate(&tx, envelope) {
                            break 'connection;
                        }
                        if let Some(state) = served_state {
                            served_reconciles.insert(reconcile_key.clone(), state);
                        } else {
                            served_reconciles.remove(&reconcile_key);
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileFetch(f)) => {
                // Peer wants full values for a bounded key batch. Read values
                // incrementally and stream exact-byte-bounded pages so neither
                // the cache lock nor this connection owns an unbounded clone.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(f.fragment);
                let current = sessions.lock().await.get(&(peer.clone(), frag)).copied();
                if f.session_id == 0
                    || f.round_id == 0
                    || current.map(|session| session.session_id) != Some(f.session_id)
                {
                    tracing::debug!(
                        fragment = f.fragment,
                        session_id = f.session_id,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "ignoring stale or uncorrelated reconcile fetch"
                    );
                    continue;
                }
                let recovery_authorized =
                    served_reconciles
                        .get(&(peer.clone(), frag))
                        .is_some_and(|state| {
                            recovery.as_ref().is_some_and(|authority| {
                                authority.authorizes(
                                    frag,
                                    &peer,
                                    &state.recovery_epoch,
                                    &state.recovery_replica_digest,
                                    &placement.read().unwrap(),
                                )
                            })
                        });
                if !serving.read().unwrap().contains(&frag) && !recovery_authorized {
                    // A fragment can lose authority between the summary and
                    // fetch phases. Let the correlated sender expire and retry
                    // without tearing down other traffic on this connection.
                    tracing::debug!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "ignoring reconciliation fetch for a non-serving fragment"
                    );
                    continue;
                }
                if f.keys.is_empty() || f.keys.len() > MAX_RECONCILE_FETCH_KEYS {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        key_count = f.keys.len(),
                        "rejecting reconcile fetch outside the key-count bound"
                    );
                    break;
                }
                let mut unique_keys = HashSet::with_capacity(f.keys.len());
                if f.keys.iter().any(|key| {
                    !router.key_belongs_to(frag, key) || !unique_keys.insert(key.as_str())
                }) {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "rejecting duplicate or wrong-fragment reconcile fetch"
                    );
                    break;
                }
                let reconcile_key = (peer.clone(), frag);
                let Some((route_generation, actor)) = router.route_with_generation(frag) else {
                    served_reconciles.remove(&reconcile_key);
                    continue;
                };
                if served_reconciles
                    .get(&reconcile_key)
                    .is_some_and(|state| state.route_generation != route_generation)
                {
                    // A legitimate remove/re-add invalidates an otherwise
                    // authorized fetch without making the shared connection
                    // malicious. An absent authorization, by contrast, falls
                    // through to the strict correlation check below and
                    // closes the connection.
                    served_reconciles.remove(&reconcile_key);
                    continue;
                }
                let snapshot_time_ms =
                    served_reconciles.get_mut(&reconcile_key).and_then(|state| {
                        if state.session_id != f.session_id
                            || state.round_id != f.round_id
                            || state.next_fetch_chunk_seq != f.chunk_seq
                            || !f.keys.iter().all(|key| state.advertised_keys.contains(key))
                        {
                            return None;
                        }
                        for key in &f.keys {
                            state.advertised_keys.remove(key);
                        }
                        state.next_fetch_chunk_seq = state.next_fetch_chunk_seq.saturating_add(1);
                        Some(state.snapshot_time_ms)
                    });
                let Some(snapshot_time_ms) = snapshot_time_ms else {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "rejecting unadvertised or out-of-order reconcile fetch"
                    );
                    break;
                };
                let job = ReconcileResponseJob {
                    peer: peer.clone(),
                    session: current.expect("validated reconcile fetch session"),
                    actor,
                    fetch: f,
                    snapshot_time_ms,
                };
                if reconcile_response_tx.try_send(job).is_err() {
                    tracing::warn!(
                        peer = %peer,
                        fragment = frag.0,
                        "reconcile response queue is unavailable or at capacity"
                    );
                    break;
                }
            }
            Some(EnvelopeMsg::ReconcileResponse(r)) => {
                if r.summaries.len() > MAX_RECONCILE_PAGE_ITEMS {
                    tracing::warn!(
                        item_count = r.summaries.len(),
                        "reconcile summary page is too large"
                    );
                    break;
                }
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
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileResponse {
                            session_id: r.session_id,
                            round_id: r.round_id,
                            page_seq: r.page_seq,
                            next_after_key: r.next_after_key,
                            snapshot_revision: r.snapshot_revision,
                            snapshot_time_ms: r.snapshot_time_ms,
                            done: r.done,
                            restart: r.restart,
                            summaries: r.summaries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            // Placement may retire the sender after the map
                            // lookup above. That is a stale local consumer,
                            // not a protocol failure on this multiplexed peer
                            // connection.
                            Err(NodeSenderSendError::Closed) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(NodeSenderSendError::Full) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            // The sender can be retired after accepting the
                            // message but before answering its oneshot. Only a
                            // failure from the still-current task makes this
                            // connection unusable.
                            Ok(Err(_)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => {
                                // A live sender that stops validating control
                                // traffic is unhealthy. A removed/replaced
                                // sender merely lost a placement race.
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
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
                if d.entries.len() > MAX_RECONCILE_PAGE_ITEMS {
                    tracing::warn!(
                        item_count = d.entries.len(),
                        "reconcile data page is too large"
                    );
                    break;
                }
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
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileData {
                            session_id: d.session_id,
                            round_id: d.round_id,
                            chunk_seq: d.chunk_seq,
                            page_seq: d.page_seq,
                            done: d.done,
                            entries: d.entries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            Err(NodeSenderSendError::Closed) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(NodeSenderSendError::Full) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Ok(Err(_)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
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
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(NodeError::ShuttingDown) => {
                        if !send_unavailable_mutation(
                            &tx,
                            p.req_id,
                            vec![p.key],
                            "node is shutting down",
                        ) {
                            break;
                        }
                        continue;
                    }
                    Err(_) => unreachable!("operation admission has one failure mode"),
                };
                if let Err(error) = validate_record_wire_budget(&p.key, p.value.len(), &p.node_id) {
                    tracing::warn!(%error, "rejecting irreducible forwarded mutation");
                    break;
                }
                if !observe_remote_timestamp(
                    &hlc,
                    actordb_protocol::RecordVersion::new(p.timestamp_ms, p.logical_counter),
                    "ForwardedPut",
                )
                .await
                {
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
                    let Some(actor) = router.route_for_fragment(frag) else {
                        drop(mutation_lease);
                        if !send_unavailable_mutation(
                            &tx,
                            p.req_id,
                            vec![p.key],
                            &format!("serving fragment {frag} has no actor"),
                        ) {
                            break;
                        }
                        continue;
                    };
                    let concerned = p.write_concern.is_some();
                    let (job, completion) = MutationJob::new(
                        MutationRequest::Single(p),
                        actor,
                        senders.clone(),
                        frag,
                        operation,
                        mutation_lease,
                    );
                    mutation_service.submit(job);
                    if concerned {
                        let response_tx = tx.clone();
                        if let Some(tasks) = tasks.upgrade() {
                            if tasks
                                .spawn(async move {
                                    if let Ok(completion) = completion.await {
                                        completion.send(&response_tx);
                                    }
                                })
                                .await
                            {
                                continue;
                            }
                        }
                        break;
                    }
                    // Preserve connection frame order. Cancellation of this
                    // waiter never cancels the owned apply/outcome/fanout job.
                    let Ok(completion) = completion.await else {
                        break;
                    };
                    if !completion.send(&tx) {
                        break;
                    }
                } else if p.hops < MAX_FORWARD_HOPS {
                    drop(mutation_lease);
                    relay_put(p, frag, &tx, &peer_node_id, &rfctx, operation).await;
                } else {
                    drop(mutation_lease);
                    if !send_unavailable_mutation(
                        &tx,
                        p.req_id,
                        vec![p.key],
                        "forward hop limit reached",
                    ) {
                        break;
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedGet(g)) => {
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
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
                let Some((generation, actor)) = router.route_with_generation(frag) else {
                    // The legacy response cannot express unavailability.
                    continue;
                };
                let priority = priority_from_proto(g.priority);
                if !read_service.try_submit(ReadJob::new(
                    ReadRequest::Single(g),
                    priority,
                    ReadRoute {
                        fragment: frag,
                        generation,
                        actor,
                        router: router.clone(),
                        serving: serving.clone(),
                    },
                    tx.clone(),
                    deadline,
                )) {
                    break;
                }
            }
            Some(EnvelopeMsg::GetResponse(r)) => {
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                // Correlate the authenticated response source before it may
                // remove a waiter or advance the local clock.
                let relay_validation = {
                    let relays = pending_relays.lock().unwrap();
                    relays.get(&r.req_id).and_then(|relay| {
                        (relay.expected_peer == peer && relay.expected_tx.same_channel(&tx)).then(
                            || {
                                (
                                    relay._admission.clone(),
                                    validate_legacy_get_response(&relay.expected_key, &r),
                                )
                            },
                        )
                    })
                };
                let pending_expected_key = {
                    let pending = pending_gets.lock().unwrap();
                    pending.get(&r.req_id).and_then(|request| {
                        (request.expected_peer == peer).then(|| request.expected_key.clone())
                    })
                };
                let relay_admission = relay_validation
                    .as_ref()
                    .map(|(admission, _)| admission.clone());
                let Some(validation) = relay_validation.map(|(_, result)| result).or_else(|| {
                    pending_expected_key
                        .as_deref()
                        .map(|key| validate_legacy_get_response(key, &r))
                }) else {
                    continue;
                };
                if let Err(error) = validation {
                    tracing::warn!(
                        peer = %peer,
                        req_id = r.req_id,
                        %error,
                        "rejecting malformed forwarded-get response"
                    );
                    if let Some(admission) = &relay_admission {
                        if let Some(relay) = remove_get_relay(&pending_relays, r.req_id, admission)
                        {
                            let _ = relay.timeout_cancel.send(());
                        }
                    } else if let Some(pending) = pending_gets.lock().unwrap().remove(&r.req_id) {
                        let _ = pending.reply.send(Err(error));
                    }
                    break;
                }
                if r.found
                    && !observe_remote_timestamp(
                        &hlc,
                        actordb_protocol::RecordVersion::new(r.timestamp_ms, r.logical_counter),
                        "GetResponse",
                    )
                    .await
                {
                    break;
                }
                // A relayed read completing: forward the response to the
                // origin peer with its original req_id.
                let relay = if let Some(admission) = &relay_admission {
                    remove_get_relay(&pending_relays, r.req_id, admission)
                } else {
                    None
                };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    let _ = send_envelope(
                        &relay.origin_tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                                req_id: relay.origin_req_id,
                                ..r
                            })),
                        },
                    );
                    continue;
                }
                let pending = if pending_expected_key.is_some() {
                    pending_gets.lock().unwrap().remove(&r.req_id)
                } else {
                    None
                };
                if let Some(pending) = pending {
                    if r.shed {
                        let _ = pending.reply.send(Err(NodeError::Backpressure(
                            "remote pool at queue cap or rate limit".to_string(),
                        )));
                    } else if !r.found || r.is_tombstone {
                        let _ = pending.reply.send(Ok(None));
                    } else {
                        let mut value = VersionedValue::new_version(
                            r.value,
                            actordb_protocol::RecordVersion::new(r.timestamp_ms, r.logical_counter),
                            r.node_id,
                        );
                        value.expires_at_ms = (r.expires_at_ms != 0).then_some(r.expires_at_ms);
                        let _ = pending.reply.send(Ok(Some(value)));
                    }
                }
            }
            Some(EnvelopeMsg::MutationResponse(mut response)) => {
                if response.results.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = response.results.len(),
                        "mutation response is too large"
                    );
                    break;
                }
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let relay_validation = {
                    let relays = pending_mutation_relays.lock().unwrap();
                    relays.get(&response.req_id).and_then(|relay| {
                        (relay.expected_peer == peer && relay.expected_tx.same_channel(&tx)).then(
                            || {
                                (
                                    relay._admission.clone(),
                                    validate_mutation_results(
                                        &relay.expected_keys,
                                        &response.results,
                                    ),
                                )
                            },
                        )
                    })
                };
                let relay_admission = relay_validation
                    .as_ref()
                    .map(|(admission, _)| admission.clone());
                let validation = relay_validation.map(|(_, result)| result).or_else(|| {
                    let pending = pending_mutations.lock().unwrap();
                    pending.get(&response.req_id).and_then(|request| {
                        (request.expected_peer == peer).then(|| {
                            validate_mutation_results(&request.expected_keys, &response.results)
                        })
                    })
                });
                let Some(validation) = validation else {
                    continue;
                };
                if let Err(error) = validation {
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
                    .map(|result| {
                        actordb_protocol::RecordVersion::new(
                            result.winner_timestamp_ms,
                            result.winner_logical_counter,
                        )
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "MutationResponse").await {
                        break;
                    }
                }

                let relay = if let Some(admission) = &relay_admission {
                    remove_mutation_relay(&pending_mutation_relays, response.req_id, admission)
                } else {
                    None
                };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    response.req_id = relay.origin_req_id;
                    let _ = send_mutation_response(&relay.origin_tx, response);
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
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(NodeError::ShuttingDown) => {
                        let keys = bp.puts.into_iter().map(|put| put.key).collect();
                        if !send_unavailable_mutation(&tx, bp.req_id, keys, "node is shutting down")
                        {
                            break;
                        }
                        continue;
                    }
                    Err(_) => unreachable!("operation admission has one failure mode"),
                };
                if bp.puts.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = bp.puts.len(),
                        "forwarded mutation batch is too large"
                    );
                    break;
                }
                if let Err(error) = validate_batch_put_wire_bounds(&bp) {
                    tracing::warn!(%error, "rejecting improperly bounded batch put");
                    break;
                }
                if let Some(error) = bp.puts.iter().find_map(|put| {
                    validate_record_wire_budget(&put.key, put.value.len(), &put.node_id).err()
                }) {
                    tracing::warn!(%error, "rejecting irreducible item in batch put");
                    break;
                }
                if let Some(timestamp_ms) = bp
                    .puts
                    .iter()
                    .map(|p| {
                        actordb_protocol::RecordVersion::new(p.timestamp_ms, p.logical_counter)
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchPut").await {
                        break;
                    }
                }
                let Some(frag) = bp
                    .puts
                    .first()
                    .map(|p| fragment_of(&p.key, router.fragment_count))
                else {
                    if !send_mutation_response(
                        &tx,
                        MutationResponse {
                            req_id: bp.req_id,
                            results: Vec::new(),
                        },
                    ) {
                        break;
                    }
                    continue;
                };
                if bp
                    .puts
                    .iter()
                    .any(|p| fragment_of(&p.key, router.fragment_count) != frag)
                {
                    if !send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        bp.puts.into_iter().map(|put| put.key).collect(),
                        "batch contains keys from multiple fragments",
                    ) {
                        break;
                    }
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
                        relay_batch(bp, frag, &tx, &peer_node_id, &rfctx, operation).await;
                    } else {
                        if !send_unavailable_mutation(
                            &tx,
                            bp.req_id,
                            bp.puts.into_iter().map(|put| put.key).collect(),
                            "forward hop limit reached",
                        ) {
                            break;
                        }
                    }
                    continue;
                }

                let Some(actor) = router.route_for_fragment(frag) else {
                    drop(mutation_lease);
                    if !send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        bp.puts.into_iter().map(|put| put.key).collect(),
                        "serving fragment has no actor",
                    ) {
                        break;
                    }
                    continue;
                };
                let (job, completion) = MutationJob::new(
                    MutationRequest::Batch(bp),
                    actor,
                    senders.clone(),
                    frag,
                    operation,
                    mutation_lease,
                );
                mutation_service.submit(job);
                let Ok(completion) = completion.await else {
                    break;
                };
                if !completion.send(&tx) {
                    break;
                }
            }
            Some(EnvelopeMsg::BatchGet(bg)) => {
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
                if bg.keys.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = bg.keys.len(),
                        "forwarded read batch is too large"
                    );
                    break;
                }
                let req_id = bg.req_id;
                let Some(frag) = bg
                    .keys
                    .first()
                    .map(|key| fragment_of(key, router.fragment_count))
                else {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                };
                if bg
                    .keys
                    .iter()
                    .any(|key| fragment_of(key, router.fragment_count) != frag)
                {
                    tracing::warn!(req_id, "batch get contains keys from multiple fragments");
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                }
                if !serving.read().unwrap().contains(&frag) {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                }
                let Some((generation, actor)) = router.route_with_generation(frag) else {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                };
                let priority = priority_from_proto(bg.priority);
                if !read_service.try_submit(ReadJob::new(
                    ReadRequest::Batch(bg),
                    priority,
                    ReadRoute {
                        fragment: frag,
                        generation,
                        actor,
                        router: router.clone(),
                        serving: serving.clone(),
                    },
                    tx.clone(),
                    deadline,
                )) {
                    break;
                }
            }
            Some(EnvelopeMsg::BatchGetResponse(r)) => {
                if r.results.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(item_count = r.results.len(), "read response is too large");
                    break;
                }
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let validation = {
                    let pending = pending_batch_gets.lock().unwrap();
                    pending.get(&r.req_id).and_then(|request| {
                        (request.expected_peer == peer)
                            .then(|| validate_batch_get_results(&request.expected_keys, &r.results))
                    })
                };
                let Some(validation) = validation else {
                    continue;
                };
                if let Err(error) = validation {
                    tracing::warn!(
                        peer = %peer,
                        req_id = r.req_id,
                        %error,
                        "rejecting malformed batch-get response"
                    );
                    if let Some(pending) = pending_batch_gets.lock().unwrap().remove(&r.req_id) {
                        let _ = pending.reply.send(Err(error));
                    }
                    break;
                }
                if let Some(timestamp_ms) = r
                    .results
                    .iter()
                    .filter(|result| result.found)
                    .map(|result| {
                        actordb_protocol::RecordVersion::new(
                            result.timestamp_ms,
                            result.logical_counter,
                        )
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchGetResponse").await {
                        break;
                    }
                }
                let pending = {
                    let mut pending = pending_batch_gets.lock().unwrap();
                    if matches!(
                        pending.get(&r.req_id),
                        Some(request) if request.expected_peer == peer
                    ) {
                        pending.remove(&r.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(Ok(r.results));
                }
            }
            Some(EnvelopeMsg::SyncRequest(_)) | Some(EnvelopeMsg::SyncResponse(_)) => {
                // Protocol v3 deliberately has one anti-entropy engine. A
                // negotiated v3 peer that sends a legacy full-sync frame is
                // either malformed or has falsely advertised capabilities;
                // fail closed instead of rebuilding an unbounded response.
                tracing::warn!("closing peer that sent a retired full-state sync frame");
                break;
            }
            Some(EnvelopeMsg::QueryPageRequest(req)) => {
                if tx.capabilities & CAPABILITY_QUERY_PAGES == 0 {
                    break;
                }
                let retention = match query::reserve_inbound(&query_admission, &req) {
                    Ok(retention) => retention,
                    Err(error) => {
                        let page = crate::query::page_error(
                            &req,
                            crate::proto::QueryPageStatus::Backpressure,
                            error.to_string(),
                        );
                        let _ = try_send_envelope_until(
                            &tx,
                            crate::query::page_envelope(page),
                            deadline_after(runtime_options.queue_admission_timeout),
                        )
                        .await;
                        continue;
                    }
                };
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(_) => {
                        let page = crate::query::page_error(
                            &req,
                            crate::proto::QueryPageStatus::Unavailable,
                            "node is shutting down",
                        );
                        let _ = try_send_envelope_until(
                            &tx,
                            crate::query::page_envelope(page),
                            deadline_after(runtime_options.queue_admission_timeout),
                        )
                        .await;
                        continue;
                    }
                };
                let Some(tasks) = tasks.upgrade() else {
                    break;
                };
                let channel = tx.clone();
                let router = router.clone();
                let serving = serving.clone();
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
                if !tasks
                    .spawn(async move {
                        let _operation = operation;
                        query::serve_query_page(channel, router, serving, req, retention, deadline)
                            .await;
                    })
                    .await
                {
                    break;
                }
            }
            Some(EnvelopeMsg::QueryPageResponse(response)) => {
                if tx.capabilities & CAPABILITY_QUERY_PAGES == 0 {
                    break;
                }
                let pending = {
                    let mut pending = pending_query_pages.lock().unwrap();
                    if pending
                        .get(&response.req_id)
                        .is_some_and(|request| request.channel.same_channel(&tx))
                    {
                        pending.remove(&response.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(response);
                }
            }
            Some(EnvelopeMsg::ScanRequest(req)) => {
                // Serve the scan without blocking the connection read
                // loop: the spawned task runs the fragment actor call and
                // replies with a single bounded ScanResponse.
                let tx = tx.clone();
                let serving = serving.clone();
                let router = router.clone();
                if tx.capabilities & CAPABILITY_SCAN == 0 {
                    break;
                }
                let retention = match query_admission
                    .reserve_work(req.encoded_len().saturating_add(1024), 48 * 1024 * 1024)
                {
                    Ok(retention) => retention,
                    Err(error) => {
                        scan::send_scan_response(&tx, req.req_id, Vec::new(), error.to_string())
                            .await;
                        continue;
                    }
                };
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(_) => {
                        scan::send_scan_response(
                            &tx,
                            req.req_id,
                            Vec::new(),
                            "node is shutting down".into(),
                        )
                        .await;
                        continue;
                    }
                };
                let Some(tasks) = tasks.upgrade() else {
                    break;
                };
                if !tasks
                    .spawn(async move {
                        let _operation = operation;
                        scan::serve_scan_request(tx, serving, router, req, retention).await;
                    })
                    .await
                {
                    break;
                }
            }
            Some(EnvelopeMsg::ScanResponse(resp)) => {
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let pending = {
                    let mut pending = pending_scans.lock().unwrap();
                    match pending.get(&resp.req_id) {
                        Some(request) if request.expected_peer == peer => {
                            pending.remove(&resp.req_id)
                        }
                        _ => None,
                    }
                };
                if let Some(pending) = pending {
                    let result = if resp.error.is_empty() {
                        Ok(resp.matches)
                    } else {
                        Err(resp.error)
                    };
                    let _ = pending.reply.send(result);
                }
            }
            None => {}
        }
    }

    tx.cancel();
    pending_query_pages
        .lock()
        .unwrap()
        .retain(|_, request| !request.channel.same_channel(&tx));
    // Connection dropped: forget the peer only if this exact physical channel
    // is still current. Legacy full-replication peers are discovered at
    // runtime, so reap their per-fragment senders on disconnect rather than
    // letting sequential claimed identities grow the sender maps forever.
    // The next established session's immediate correlated reconciliation
    // covers writes made while that peer is absent. Explicit placement maps
    // retain sender buffers because their peer cardinality is configured and
    // bounded.
    // End relay ownership before asynchronous peer/sender cleanup can wait on
    // another task. This physical channel's replacement is never affected.
    tx.cancel();
    cancel_connection_relays(&tx, &pending_relays, &pending_mutation_relays);
    let mut disconnected_senders = Vec::new();
    if let Some(peer) = handshake.established_peer_id().map(str::to_owned) {
        let mut guard = peers.lock().await;
        // Only remove if it's still our entry (a newer connection may
        // have replaced it).
        if let Some(tx_existing) = guard.get(&peer) {
            if tx_existing.same_channel(&tx) {
                guard.remove(&peer);
                let legacy_full_replication = placement.read().unwrap().is_empty();
                if legacy_full_replication && operation_gate.is_running() {
                    let mut senders_guard = senders.lock().await;
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
                    // Explicitly placed senders, plus legacy senders already
                    // captured by a shutdown fence, retain their recovery
                    // state for reconnect. Only notify if this was the
                    // current channel.
                    let peer_senders: Vec<NodeSender> = senders
                        .lock()
                        .await
                        .iter()
                        .filter(|((_, sender_peer), _)| sender_peer == &peer)
                        .map(|(_, sender)| sender.clone())
                        .collect();
                    for sender_tx in peer_senders {
                        let _ = sender_tx.send(SenderMsg::PeerDisconnected {
                            peer_tx: tx.raw_sender(),
                        });
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
    drop(reconcile_response_tx);
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
    relay_admission: Arc<RelayAdmission>,
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

fn send_unavailable_mutation(
    tx: &PeerChannel,
    req_id: u64,
    keys: Vec<String>,
    reason: &str,
) -> bool {
    send_mutation_response(
        tx,
        MutationResponse {
            req_id,
            results: unavailable_mutation_results(keys, reason),
        },
    )
}

fn send_mutation_response(tx: &PeerChannel, response: MutationResponse) -> bool {
    try_send_envelope_immediate(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(response)),
        },
    )
}

fn send_batch_get_response(tx: &PeerChannel, req_id: u64, results: Vec<BatchKeyValue>) -> bool {
    try_send_envelope_immediate(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id,
                results,
            })),
        },
    )
}

async fn register_mutation_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: MutationRelayMap,
    admission: &Arc<RelayAdmission>,
    relay_id: u64,
    registration: MutationRelayRegistration,
) -> Option<RelayRegistrationGuard> {
    let lease = admission.try_reserve(relay_keys_storage_bytes(
        &registration.expected_keys,
        registration.expected_keys.capacity(),
    ));
    let Some(lease) = lease else {
        let MutationRelayRegistration {
            origin_tx,
            origin_req_id,
            expected_keys,
            operation: _operation,
            ..
        } = registration;
        let _ = send_unavailable_mutation(
            &origin_tx,
            origin_req_id,
            expected_keys,
            "mutation relay admission exhausted",
        );
        return None;
    };
    let MutationRelayRegistration {
        expected_peer,
        expected_tx,
        expected_keys,
        origin_tx,
        origin_req_id,
        deadline,
        operation,
    } = registration;
    let (timeout_cancel, cancelled) = oneshot::channel();
    let mut owner_cancel = expected_tx.cancel.subscribe();
    let mut origin_cancel = origin_tx.cancel.subscribe();
    let guard = RelayRegistrationGuard {
        registry: RelayRegistry::Mutation(relays.clone()),
        relay_id,
        admission: lease.clone(),
        origin_tx: origin_tx.clone(),
        armed: true,
    };
    let relay = MutationRelay {
        expected_peer,
        expected_tx,
        expected_keys,
        origin_tx,
        origin_req_id,
        timeout_cancel,
        _operation: operation,
        _admission: lease.clone(),
    };
    if *owner_cancel.borrow() || *origin_cancel.borrow() || Instant::now() >= deadline {
        fail_mutation_relay(
            relay,
            "mutation relay connection closed before registration",
        );
        return None;
    }
    {
        let mut entries = relays.lock().unwrap();
        if entries.contains_key(&relay_id) {
            drop(entries);
            fail_mutation_relay(relay, "mutation relay request id collision");
            return None;
        }
        entries.insert(relay_id, relay);
    }

    // Register before spawning the timer. The cancellation sender is already
    // in the map, so every registered relay has exactly one live deadline.
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        // Admission is retained until the timer future itself exits, not just
        // until its map entry is removed. Completed/canceled timers are bounded.
        let _timer_admission = lease;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "forwarded mutation relay timed out");
                }
            }
            _ = owner_cancel.changed() => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "mutation relay owner connection closed");
                }
            }
            _ = origin_cancel.changed() => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "mutation relay origin connection closed");
                }
            }
            _ = cancelled => {}
        }
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tokio::time::timeout_at(deadline, tasks.spawn(timeout_task))
            .await
            .unwrap_or(false),
        None => false,
    };
    registered.then_some(guard)
}

async fn register_get_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: RelayMap,
    admission: &Arc<RelayAdmission>,
    relay_id: u64,
    registration: GetRelayRegistration,
) -> Option<RelayRegistrationGuard> {
    let lease = admission.try_reserve(relay_key_storage_bytes(
        registration.expected_key.capacity(),
    ))?;
    let GetRelayRegistration {
        expected_peer,
        expected_tx,
        expected_key,
        origin_tx,
        origin_req_id,
        deadline,
    } = registration;
    let (timeout_cancel, cancelled) = oneshot::channel();
    let mut owner_cancel = expected_tx.cancel.subscribe();
    let mut origin_cancel = origin_tx.cancel.subscribe();
    if *owner_cancel.borrow() || *origin_cancel.borrow() || Instant::now() >= deadline {
        return None;
    }
    let guard = RelayRegistrationGuard {
        registry: RelayRegistry::Get(relays.clone()),
        relay_id,
        admission: lease.clone(),
        origin_tx: origin_tx.clone(),
        armed: true,
    };
    {
        let mut entries = relays.lock().unwrap();
        if entries.contains_key(&relay_id) {
            return None;
        }
        entries.insert(
            relay_id,
            GetRelay {
                expected_peer,
                expected_tx,
                expected_key,
                origin_tx,
                origin_req_id,
                timeout_cancel,
                _admission: lease.clone(),
            },
        );
    }
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        let _timer_admission = lease;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = owner_cancel.changed() => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = origin_cancel.changed() => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = cancelled => {}
        }
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tokio::time::timeout_at(deadline, tasks.spawn(timeout_task))
            .await
            .unwrap_or(false),
        None => false,
    };
    registered.then_some(guard)
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
    operation: OperationPermit,
) {
    let original_req_id = p.req_id;
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "no connected owner available for mutation relay",
        );
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let extra = match p
        .write_concern
        .as_deref()
        .map(crate::write_concern::WriteConcernOptions::decode)
        .transpose()
    {
        Ok(options) => options.map_or(Duration::ZERO, |options| options.timeout()),
        Err(error) => {
            let failure = crate::write_concern::WriteConcernError::from(error);
            let mut result = mutation_result(p.key, 0, Err(NodeError::Codec(failure.to_string())));
            result.write_concern = failure.outcome().map(|outcome| outcome.encode());
            let _ = send_mutation_response(
                origin_tx,
                MutationResponse {
                    req_id: original_req_id,
                    results: vec![result],
                },
            );
            return;
        }
    };
    let Some(timeout) = ctx.forwarded_attempt_timeout.checked_add(extra) else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "relay concern timeout exceeds range",
        );
        return;
    };
    let Some(attempt_deadline) = Instant::now().checked_add(timeout) else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "relay concern timeout exceeds range",
        );
        return;
    };
    let keys = vec![p.key.clone()];
    let Some(registration) = register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_keys: keys,
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
            operation,
        },
    )
    .await
    else {
        return;
    };
    p.req_id = relay_id;
    p.hops += 1;
    let sent = registration
        .enqueue(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(p)),
            },
            attempt_deadline,
        )
        .await;
    if !sent {
        drop(registration);
    } else {
        registration.disarm();
    }
}

/// Relay a whole single-fragment batch with the same response guarantees.
async fn relay_batch(
    mut bp: BatchPut,
    frag: FragmentId,
    origin_tx: &PeerChannel,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
    operation: OperationPermit,
) {
    let original_req_id = bp.req_id;
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            bp.puts.into_iter().map(|put| put.key).collect(),
            "no connected owner available for batch relay",
        );
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    let keys: Vec<String> = bp.puts.iter().map(|put| put.key.clone()).collect();
    let Some(registration) = register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_keys: keys,
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
            operation,
        },
    )
    .await
    else {
        return;
    };
    bp.req_id = relay_id;
    for p in bp.puts.iter_mut() {
        p.hops += 1;
    }
    let sent = registration
        .enqueue(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(bp)),
            },
            attempt_deadline,
        )
        .await;
    if !sent {
        drop(registration);
    } else {
        registration.disarm();
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
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    let Some(registration) = register_get_relay(
        &ctx.tasks,
        ctx.pending_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        GetRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_key: g.key.clone(),
            origin_tx: origin_tx.clone(),
            origin_req_id: g.req_id,
            deadline: attempt_deadline,
        },
    )
    .await
    else {
        return;
    };
    let sent = registration
        .enqueue(
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
        drop(registration);
    } else {
        registration.disarm();
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
    getrandom::fill(&mut challenge)?;
    Ok(Hello {
        node_id: node_id.to_string(),
        auth_tag: Vec::new(),
        min_protocol_version: WIRE_PROTOCOL_VERSION,
        max_protocol_version: WIRE_PROTOCOL_VERSION,
        capabilities: REQUIRED_WIRE_CAPABILITIES | CAPABILITY_SCAN | CAPABILITY_QUERY_PAGES,
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
    use hmac::{Hmac, KeyInit, Mac};
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
    use hmac::{Hmac, KeyInit, Mac};
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

fn send_envelope(tx: &PeerChannel, envelope: Envelope) -> bool {
    try_send_envelope_immediate(tx, envelope)
}

/// Admit a reader-generated response without ever parking the sole socket
/// reader behind its own writer. The writer drains this reserved lane before
/// ordinary producer traffic. Saturation cancels the physical connection;
/// correlated senders then retry, while the reader remains available to drain
/// frames that let the opposite writer make progress.
fn try_send_envelope_immediate(tx: &PeerChannel, envelope: Envelope) -> bool {
    let encoded_len = match check_envelope_budget(&envelope, MAX_FRAME_BYTES) {
        Ok(encoded_len) => encoded_len,
        Err(error) => {
            tracing::warn!(%error, "rejecting oversized or invalid outbound peer frame");
            return false;
        }
    };
    let frame_permit = match tx.outbound.response_tx.try_reserve() {
        Ok(permit) => permit,
        Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let byte_permit = match tx
        .outbound
        .byte_budget
        .clone()
        .try_acquire_many_owned(encoded_len as u32)
    {
        Ok(permit) => permit,
        Err(_) => {
            drop(frame_permit);
            tx.cancel();
            return false;
        }
    };
    let global_permits = match tx
        .outbound
        .node_byte_budget
        .try_response(encoded_len as u32)
    {
        Ok(permits) => permits,
        Err(()) => {
            tx.cancel();
            return false;
        }
    };
    let frame = match encode_envelope(&envelope) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(%error, "failed to encode preflighted outbound peer frame");
            return false;
        }
    };
    frame_permit
        .send(BudgetedFrame::new_response(frame, byte_permit).with_global_permits(global_permits));
    true
}

#[cfg(test)]
async fn try_send_envelope(tx: &PeerChannel, envelope: Envelope) -> bool {
    try_send_envelope_with_deadline(tx, envelope, None, tx.queue_admission_timeout).await
}

async fn try_send_envelope_until(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Instant,
) -> bool {
    try_send_envelope_with_deadline(
        tx,
        envelope,
        Some(attempt_deadline),
        tx.queue_admission_timeout,
    )
    .await
}

async fn try_send_envelope_with_admission_timeout(
    tx: &PeerChannel,
    envelope: Envelope,
    admission_timeout: Duration,
) -> bool {
    try_send_envelope_with_deadline(tx, envelope, None, admission_timeout).await
}

async fn try_send_envelope_with_deadline(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Option<Instant>,
    admission_timeout: Duration,
) -> bool {
    let encoded_len = match check_envelope_budget(&envelope, MAX_FRAME_BYTES) {
        Ok(encoded_len) => encoded_len,
        Err(error) => {
            tracing::warn!(%error, "rejecting oversized or invalid outbound peer frame");
            return false;
        }
    };
    let queue_deadline = deadline_after(admission_timeout);
    let admission_deadline = attempt_deadline
        .map(|deadline| deadline.min(queue_deadline))
        .unwrap_or(queue_deadline);
    if Instant::now() >= admission_deadline {
        tx.cancel();
        return false;
    }
    let frame_permit =
        match tokio::time::timeout_at(admission_deadline, tx.outbound.tx.reserve()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                tx.cancel();
                return false;
            }
        };
    let producer_byte_permit = match tokio::time::timeout_at(
        admission_deadline,
        tx.outbound
            .producer_byte_budget
            .clone()
            .acquire_many_owned(encoded_len as u32),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) | Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let byte_permit = match tokio::time::timeout_at(
        admission_deadline,
        tx.outbound
            .byte_budget
            .clone()
            .acquire_many_owned(encoded_len as u32),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) | Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let global_permits = match tx
        .outbound
        .acquire_global_producer(encoded_len as u32, admission_deadline)
        .await
    {
        Ok(permits) => permits,
        Err(()) => {
            tx.cancel();
            return false;
        }
    };
    if Instant::now() >= admission_deadline {
        tx.cancel();
        return false;
    }
    let frame = match encode_envelope(&envelope) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(%error, "failed to encode preflighted outbound peer frame");
            return false;
        }
    };
    frame_permit.send(
        BudgetedFrame::new_producer(frame, byte_permit, producer_byte_permit)
            .with_global_permits(global_permits),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ReconcileRequest, SessionStart};
    use crate::replication::spawn_sender_with_options;
    use crate::store::StoreError;
    use ractor::ActorProcessingErr;
    use std::sync::atomic::AtomicBool;
    use std::sync::Condvar;

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

    #[tokio::test]
    async fn nodes_serve_and_invalid_settings_fail_before_bind() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let error = match Node::start_with_options(
            "invalid-builder".to_string(),
            address,
            NodeConfig::builder().build(),
            NodeRuntimeOptions::builder()
                .health_timeout(Duration::ZERO)
                .build(),
        )
        .await
        {
            Err(error) => error,
            Ok(node) => {
                node.shutdown().await.unwrap();
                panic!("invalid health timeout accepted");
            }
        };
        assert!(matches!(error, NodeError::Actor(message) if message.contains("health timeout")));
        drop(listener);

        let node = Node::start_with_options(
            "bon-builder".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().fragment_count(2).build(),
            NodeRuntimeOptions::builder().build(),
        )
        .await
        .unwrap();
        node.put("builder-key".to_string(), b"value".to_vec())
            .await
            .unwrap();
        assert_eq!(
            node.get("builder-key".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"value".to_vec()
        );
        node.shutdown().await.unwrap();
    }

    struct DelayedHealthSupervisor;

    struct DelayedHealthArgs {
        fragment: FragmentId,
        release: Option<Arc<Notify>>,
        observed_timeout: Arc<StdMutex<Option<Duration>>>,
    }

    impl Actor for DelayedHealthSupervisor {
        type Msg = SupervisorMessage;
        type State = DelayedHealthArgs;
        type Arguments = DelayedHealthArgs;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if let SupervisorMessage::GetHealth { reply } = message {
                *state.observed_timeout.lock().unwrap() = reply.get_timeout();
                if let Some(release) = &state.release {
                    release.notified().await;
                }
                let _ = reply.send(FragmentHealth {
                    fragment: state.fragment,
                    worker_count: 0,
                    worker_names: Vec::new(),
                    restarts: HashMap::new(),
                    last_reaped: 0,
                    shed_counts: (7, 11, 13),
                });
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn health_has_one_aggregate_deadline_and_passes_remaining_probe_budget() {
        let timeout = Duration::from_secs(1);
        let node = Node::start_with_options(
            "health-aggregate".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 2,
                ..Default::default()
            },
            NodeRuntimeOptions::default().with_health_timeout(timeout),
        )
        .await
        .unwrap();
        let mut replacements = HashMap::new();
        let mut observed = Vec::new();
        let release_second = Arc::new(Notify::new());
        for fragment in [FragmentId(0), FragmentId(1)] {
            let observed_timeout = Arc::new(StdMutex::new(None));
            let (actor, _) = DelayedHealthSupervisor::spawn(
                None,
                DelayedHealthSupervisor,
                DelayedHealthArgs {
                    fragment,
                    release: (fragment == FragmentId(1)).then(|| release_second.clone()),
                    observed_timeout: observed_timeout.clone(),
                },
            )
            .await
            .unwrap();
            replacements.insert(fragment, actor);
            observed.push(observed_timeout);
        }
        let originals = std::mem::replace(&mut *node.supervisors.lock().await, replacements);
        // First probe responds immediately; the second stays held. This avoids
        // requiring a timed first response to run within a tight CI margin.
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), node.health())
                .await
                .unwrap(),
            Err(NodeError::Actor(_))
        ));
        let first = observed[0].lock().unwrap().unwrap();
        let second = observed[1].lock().unwrap().unwrap();
        assert!(first <= timeout);
        assert!(second < first, "second fragment received a fresh timeout");
        let replacements = std::mem::replace(&mut *node.supervisors.lock().await, originals);
        release_second.notify_one();
        for actor in replacements.values() {
            actor.stop(None);
            actor.wait(None).await.unwrap();
        }
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_deadline_includes_registry_lock_and_missing_serving_supervisor_fails() {
        let mut node = Node::start_with_options(
            "health-registry".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::default().with_health_timeout(Duration::from_millis(20)),
        )
        .await
        .unwrap();
        let mut registry = node.supervisors.lock().await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), node.health()).await.unwrap(),
            Err(NodeError::Actor(error)) if error.contains("deadline")
        ));
        let supervisor = registry.remove(&FragmentId(0)).unwrap();
        drop(registry);
        assert!(matches!(
            node.health().await,
            Err(NodeError::Actor(error)) if error.contains("supervisor is missing")
        ));
        node.supervisors
            .lock()
            .await
            .insert(FragmentId(0), supervisor);
        node.runtime_options.health_timeout = Duration::from_secs(1);
        let health = node.health().await.unwrap();
        let fragment = &health[&FragmentId(0)];
        assert_eq!(fragment.worker_count, fragment.worker_names.len());
        assert_eq!(fragment.worker_count, 8);
        assert_eq!(fragment.shed_counts, (0, 0, 0));
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_stopped_registered_supervisor_reports_fragment_and_recovers() {
        let node = Node::start_with_options(
            "health-stopped-supervisor".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 2,
                ..Default::default()
            },
            NodeRuntimeOptions::default().with_health_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
        let fragment = FragmentId(1);
        let observed_timeout = Arc::new(StdMutex::new(None));
        let (stopped, _) = DelayedHealthSupervisor::spawn(
            None,
            DelayedHealthSupervisor,
            DelayedHealthArgs {
                fragment,
                release: None,
                observed_timeout: observed_timeout.clone(),
            },
        )
        .await
        .unwrap();
        stopped.stop(None);
        tokio::time::timeout(Duration::from_secs(5), stopped.wait(None))
            .await
            .unwrap()
            .unwrap();

        // Keep the exact stopped ref in the owned registry: this must exercise
        // failed RPC admission, not the missing-supervisor or probe-timeout path.
        let original = node
            .supervisors
            .lock()
            .await
            .insert(fragment, stopped.clone())
            .unwrap();
        let failed = tokio::time::timeout(Duration::from_secs(5), node.health()).await;

        // Restore ownership before any failure assertion, so cleanup still
        // reaches the original live supervisor even if the regression fires.
        let replaced = node
            .supervisors
            .lock()
            .await
            .insert(fragment, original)
            .unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(5), node.health()).await;
        let query = tokio::time::timeout(
            Duration::from_secs(5),
            node.get("health-recovered-query".to_string()),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(5), node.shutdown()).await;

        assert_eq!(replaced.get_id(), stopped.get_id());
        assert!(observed_timeout.lock().unwrap().is_none());
        assert!(matches!(
            failed.unwrap(),
            Err(NodeError::Actor(error))
                if error.starts_with(&format!("health unavailable for fragment {fragment}:"))
                    && !error.contains("probe failed")
        ));
        let recovered = recovered.unwrap().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[&fragment].worker_count, 8);
        assert_eq!(recovered[&fragment].shed_counts, (0, 0, 0));
        assert!(query.unwrap().unwrap().is_none());
        shutdown.unwrap().unwrap();
    }

    #[derive(Default)]
    struct BlockingLoadStore {
        entered: AtomicBool,
        release: (StdMutex<bool>, Condvar),
    }

    impl BlockingLoadStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    impl KvStore for BlockingLoadStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct BlockingExclusiveLoadStore {
        attached: Arc<AtomicBool>,
        entered: AtomicBool,
        exited: AtomicBool,
        release: (StdMutex<bool>, Condvar),
    }

    struct ExclusiveTestLease(Arc<AtomicBool>);

    impl Drop for ExclusiveTestLease {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    fn exclusive_test_binding(attached: &Arc<AtomicBool>) -> StoreBinding {
        let lease = ExclusiveTestLease(attached.clone());
        StoreBinding::managed(move || drop(lease))
    }

    impl Default for BlockingExclusiveLoadStore {
        fn default() -> Self {
            Self {
                attached: Arc::new(AtomicBool::new(false)),
                entered: AtomicBool::new(false),
                exited: AtomicBool::new(false),
                release: (StdMutex::new(false), Condvar::new()),
            }
        }
    }

    impl BlockingExclusiveLoadStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    impl KvStore for BlockingExclusiveLoadStore {
        fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            self.attached
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| StoreError("exclusive store is already attached".to_string()))?;
            Ok(exclusive_test_binding(&self.attached))
        }

        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            self.exited.store(true, Ordering::SeqCst);
            Ok(Vec::new())
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct FailFirstLoadExclusiveStore {
        attached: Arc<AtomicBool>,
        loads: AtomicUsize,
    }

    impl Default for FailFirstLoadExclusiveStore {
        fn default() -> Self {
            Self {
                attached: Arc::new(AtomicBool::new(false)),
                loads: AtomicUsize::new(0),
            }
        }
    }

    impl KvStore for FailFirstLoadExclusiveStore {
        fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            self.attached
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| StoreError("exclusive store is already attached".to_string()))?;
            Ok(exclusive_test_binding(&self.attached))
        }

        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            if self.loads.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut key = 0u32.to_be_bytes().to_vec();
                key.push(0xff);
                Ok(vec![(
                    key,
                    VersionedValue::new(Vec::new(), 1, "writer".to_string()),
                )])
            } else {
                Ok(Vec::new())
            }
        }

        fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            Ok(())
        }
    }

    #[test]
    fn operation_admission_loses_a_race_with_shutdown() {
        let gate = OperationGate::new();

        // Model shutdown winning immediately after try_enter's initial
        // NODE_RUNNING observation but before it publishes the operation.
        gate.begin_shutdown();
        assert!(matches!(
            gate.finish_enter_after_running_check(),
            Err(NodeError::ShuttingDown)
        ));
        assert_eq!(gate.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn shutting_down_error_has_an_actionable_message() {
        assert_eq!(NodeError::ShuttingDown.to_string(), "node is shutting down");
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

    #[derive(Default)]
    struct TtlRecordingStore {
        values: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
        writes: AtomicUsize,
    }

    impl KvStore for TtlRecordingStore {
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
        clock.seed_persisted(future).unwrap();
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
        node.hlc.lock().await.seed_persisted(future).unwrap();
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
            let marker = node.router.get_all().await.unwrap().remove(&key).unwrap();
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
        assert_eq!(values["permanent"], permanent);
        assert_eq!(values["expiring"], expiring);
        for (key, marker) in markers {
            assert_eq!(values[&key], marker);
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
        clock.seed_persisted(future).unwrap();
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
        let marker = owner
            .router
            .get_all()
            .await
            .unwrap()
            .remove("immediate-remote")
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
        let marker = node
            .router
            .get_all()
            .await
            .unwrap()
            .remove("waited")
            .unwrap();
        assert!(marker.is_tombstone);
        assert!(marker.expires_at_ms.unwrap() < marker.timestamp_ms);
        node.shutdown().await.unwrap();
    }

    struct BlockingPageActor;

    struct BlockingPageState {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Actor for BlockingPageActor {
        type Msg = KvMessage;
        type State = BlockingPageState;
        type Arguments = BlockingPageState;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            if let KvMessage::GetVersionedPage { reply, .. } = message {
                state.started.notify_one();
                state.release.notified().await;
                drop(reply);
            } else if let KvMessage::GetBatch { reply, .. } = message {
                state.started.notify_one();
                state.release.notified().await;
                drop(reply);
            } else if let KvMessage::GetBatchRetained(request) = message {
                state.started.notify_one();
                state.release.notified().await;
                drop(request);
            }
            Ok(())
        }
    }

    struct BlockingDispatcherSupervisor;

    struct BlockingDispatcherSupervisorState {
        replacement: ActorRef<KvMessage>,
        requested: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Actor for BlockingDispatcherSupervisor {
        type Msg = SupervisorMessage;
        type State = BlockingDispatcherSupervisorState;
        type Arguments = BlockingDispatcherSupervisorState;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            match message {
                SupervisorMessage::GetDispatcher { reply } => {
                    state.requested.notify_one();
                    state.release.notified().await;
                    let _ = reply.send(state.replacement.clone());
                }
                SupervisorMessage::GetHealth { reply } => {
                    let _ = reply.send(FragmentHealth {
                        fragment: FragmentId(0),
                        worker_count: 0,
                        worker_names: Vec::new(),
                        restarts: HashMap::new(),
                        last_reaped: 0,
                        shed_counts: (0, 0, 0),
                    });
                }
                SupervisorMessage::Quiesce { reply } => {
                    let _ = reply.send(Ok(()));
                }
            }
            Ok(())
        }
    }

    pub(super) fn test_peer_channel(
        capacity: usize,
    ) -> (
        PeerChannel,
        mpsc::Receiver<BudgetedFrame>,
        mpsc::Receiver<BudgetedFrame>,
    ) {
        let (tx, rx) = mpsc::channel(capacity);
        let (response_tx, response_rx) = mpsc::channel(capacity);
        let (identity, _) = mpsc::channel(1);
        let (cancel, _) = watch::channel(false);
        (
            PeerChannel {
                outbound: BudgetedPeerTx {
                    node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                    tx,
                    response_tx,
                    byte_budget: Arc::new(Semaphore::new(PEER_CHANNEL_BYTE_CAPACITY)),
                    producer_byte_budget: Arc::new(Semaphore::new(
                        PEER_CHANNEL_BYTE_CAPACITY - PEER_RESPONSE_BYTE_RESERVE,
                    )),
                    reconcile_gate: ReconcileGate::new(),
                    identity,
                },
                cancel,
                queue_admission_timeout: Duration::from_millis(10),
                rank: None,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
            },
            rx,
            response_rx,
        )
    }

    async fn test_budgeted_frame(bytes: Vec<u8>, budget: &Arc<Semaphore>) -> BudgetedFrame {
        let permit = budget
            .clone()
            .acquire_many_owned(bytes.len() as u32)
            .await
            .unwrap();
        BudgetedFrame::new_response(bytes, permit)
    }

    fn test_sender_report() -> Arc<Mutex<SenderStateReport>> {
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
        let supervisor_stops = Arc::new(SupervisorRegistry::new(1));
        if let Ok(active) = supervisors.try_lock() {
            for (fragment, supervisor) in active.iter() {
                supervisor_stops.insert_ready(*fragment, supervisor.clone());
            }
        }
        PlacementDriverDeps {
            expiry_publisher: None,
            recovery: None,
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
            supervisor_stops,
            placement_states: Arc::new(Mutex::new(HashMap::from([(
                FragmentId(0),
                FragmentPlacementState::Draining,
            )]))),
            preloaded_fragments: Arc::new(StdMutex::new(HashSet::new())),
            placement_generation: Arc::new(AtomicU64::new(0)),
            senders,
            finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_reports,
            sender_config: SenderConfig::default(),
            sender_runtime: SenderRuntimeOptions::default(),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            tasks: Weak::new(),
            dispatcher_tasks: Weak::new(),
            operation_gate: OperationGate::new(),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
        }
    }

    pub(super) async fn test_connection_context(
        router: FragmentRouter,
        serving: Arc<RwLock<HashSet<FragmentId>>>,
        placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
        dynamic_placement: bool,
        senders: SenderMap,
        sender_reports: SenderReportMap,
        peers: PeerMap,
    ) -> (ConnContext, Arc<RuntimeTasks>) {
        let tasks = RuntimeTasks::new(8);
        let read_service = ReadService::start(&tasks, READ_QUEUE_CAPACITY, READ_KEY_BYTE_CAPACITY)
            .await
            .expect("test read service started");
        let (peer_changes, _) = watch::channel(0u64);
        (
            ConnContext {
                recovery: None,
                node_id: "local".to_string(),
                router,
                serving,
                placement,
                dynamic_placement,
                pending_gets: Arc::new(StdMutex::new(HashMap::new())),
                pending_batch_gets: Arc::new(StdMutex::new(HashMap::new())),
                pending_scans: Arc::new(StdMutex::new(HashMap::new())),
                pending_query_pages: Arc::new(StdMutex::new(HashMap::new())),
                query_admission: crate::query::QueryAdmission::new(),
                pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
                pending_relays: Arc::new(StdMutex::new(HashMap::new())),
                pending_mutation_relays: Arc::new(StdMutex::new(HashMap::new())),
                next_req_id: Arc::new(AtomicU64::new(1)),
                peers,
                peer_failures: PeerFailureSignals::default(),
                tasks: Arc::downgrade(&tasks),
                read_service,
                operation_gate: OperationGate::new(),
                peer_changes,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                senders,
                finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
                retiring_senders: Arc::new(RetiringSenderRegistry::default()),
                sender_config: SenderConfig::default(),
                sender_runtime: SenderRuntimeOptions::default(),
                runtime_options: NodeRuntimeOptions::default(),
                receive_byte_budget: Arc::new(Semaphore::new(RECEIVE_BYTE_CAPACITY)),
                node_peer_queue_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                relay_admission: RelayAdmission::new(RELAY_CAPACITY, RELAY_KEY_BYTE_CAPACITY),
                mutation_service: MutationService::new(
                    FORWARDED_MUTATION_CAPACITY,
                    FORWARDED_MUTATION_BYTE_CAPACITY,
                ),
                sender_reports,
                membership_tx: None,
                cluster_secret: None,
                sender_snapshot_hook: None,
            },
            tasks,
        )
    }

    pub(super) async fn complete_test_handshake(
        stream: &mut tokio::io::DuplexStream,
        peer_id: &str,
    ) {
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
            winner_logical_counter: 0,
            winner_node_id: String::new(),
            winner_is_tombstone: false,
            request_index,

            write_concern: None,
        }
    }

    pub(super) async fn write_test_envelope(
        stream: &mut tokio::io::DuplexStream,
        envelope: Envelope,
    ) {
        let frame = envelope.encode_to_vec();
        stream.write_u32(frame.len() as u32).await.unwrap();
        stream.write_all(&frame).await.unwrap();
    }

    pub(super) async fn read_test_envelope(stream: &mut tokio::io::DuplexStream) -> Envelope {
        tokio::time::timeout(Duration::from_secs(1), async {
            let len = stream.read_u32().await.unwrap() as usize;
            let mut frame = vec![0; len];
            stream.read_exact(&mut frame).await.unwrap();
            Envelope::decode(frame.as_slice()).unwrap()
        })
        .await
        .expect("timed out waiting for test protocol frame")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_subtree_start_remains_registered_until_startup_completes() {
        let store = Arc::new(BlockingLoadStore::default());
        let registry = Arc::new(SupervisorRegistry::new(1));
        let config = NodeConfig {
            store: Some(store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        };
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let spawn_registry = registry.clone();
        let spawn_hlc = hlc.clone();
        let spawn = tokio::spawn(async move {
            spawn_fragment_subtree_registered(
                "cancelled-start",
                FragmentId(0),
                &config,
                &spawn_hlc,
                &spawn_registry,
                None,
            )
            .await
        });

        let entered = tokio::time::timeout(Duration::from_secs(5), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if entered.is_err() {
            store.release();
            let _ = spawn.await;
            panic!("fragment startup never entered the store load");
        }

        let Some((fragment, entry)) = registry.snapshot().into_iter().next() else {
            store.release();
            let _ = spawn.await;
            panic!("starting supervisor was not synchronously registered");
        };
        let was_starting = matches!(&*entry.startup.borrow(), SupervisorStartupState::Starting);
        spawn.abort();
        let spawn_cancelled = spawn.await.unwrap_err().is_cancelled();

        let startup = tokio::spawn(wait_for_supervisor_startup(entry.startup.clone()));
        tokio::time::sleep(Duration::from_millis(25)).await;
        let wait_blocked = !startup.is_finished();
        store.release();
        let startup_state = tokio::time::timeout(Duration::from_secs(5), startup)
            .await
            .expect("startup observer did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(fragment, FragmentId(0));
        assert!(was_starting);
        assert!(spawn_cancelled);
        assert!(wait_blocked, "shutdown would not have waited for startup");
        assert!(matches!(startup_state, SupervisorStartupState::Ready));

        quiesce_fragment_supervisor(&entry.actor).await.unwrap();
        assert!(registry.mark_prepared(fragment, &entry.actor));
        entry.actor.stop(None);
        let _ = entry.actor.wait(None).await;
        assert!(registry.remove_if(fragment, &entry.actor));
        assert!(registry.snapshot().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_node_start_keeps_exclusive_store_fail_closed() {
        let store = Arc::new(BlockingExclusiveLoadStore::default());
        let config = || NodeConfig {
            store: Some(store.clone()),
            ..NodeConfig::default()
        };
        let startup = tokio::spawn(Node::start(
            "cancelled-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Node startup never entered the blocking store load");

        startup.abort();
        let join_error = match startup.await {
            Err(error) => error,
            Ok(_) => panic!("aborted Node startup completed normally"),
        };
        assert!(join_error.is_cancelled());
        let retry = Node::start(
            "cancelled-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await;
        assert!(matches!(retry, Err(NodeError::Storage(_))));
        assert!(store.attached.load(Ordering::SeqCst));

        store.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.exited.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled startup's store load did not exit");
    }

    #[tokio::test]
    async fn joined_node_start_failure_releases_exclusive_store() {
        let store = Arc::new(FailFirstLoadExclusiveStore::default());
        let config = || NodeConfig {
            store: Some(store.clone()),
            ..NodeConfig::default()
        };
        let first = Node::start(
            "retry-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await;
        assert!(first.is_err());
        assert!(!store.attached.load(Ordering::SeqCst));

        let second = Node::start(
            "retry-node-start".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config(),
        )
        .await
        .expect("joined startup rollback did not release the store binding");
        second.shutdown().await.unwrap();
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

    fn reference_fit_reconcile_response_page(
        response: &mut ReconcileResponse,
        after_key: &str,
        source_done: bool,
    ) -> Result<usize, usize> {
        let original_len = response.summaries.len();
        loop {
            response.next_after_key = response
                .summaries
                .last()
                .map(|summary| summary.key.clone())
                .unwrap_or_else(|| after_key.to_string());
            response.done = source_done && response.summaries.len() == original_len;
            let encoded_len = message_field_len(12, response.encoded_len());
            if encoded_len > MAX_FRAME_BYTES && response.summaries.len() <= 1 {
                return Err(encoded_len);
            }
            if encoded_len <= ANTI_ENTROPY_PAGE_BYTES
                || (response.summaries.len() <= 1 && encoded_len <= MAX_FRAME_BYTES)
            {
                return Ok(encoded_len);
            }
            response.summaries.pop().expect("non-singleton page");
        }
    }

    fn assert_reconcile_response_fit_matches_reference(
        response: ReconcileResponse,
        after_key: &str,
        source_done: bool,
    ) -> ReconcileResponse {
        let mut expected = response.clone();
        let expected_len =
            reference_fit_reconcile_response_page(&mut expected, after_key, source_done);
        let mut actual = response;
        let actual_len = fit_reconcile_response_page(&mut actual, after_key, source_done);
        assert_eq!(actual_len, expected_len);
        assert_eq!(actual, expected);
        if let Ok(encoded_len) = actual_len {
            assert_eq!(
                encoded_len,
                Envelope {
                    msg: Some(EnvelopeMsg::ReconcileResponse(actual.clone()))
                }
                .encoded_len()
            );
        }
        actual
    }

    #[test]
    fn reconcile_response_linear_fit_matches_prior_boundary_semantics() {
        let base = ReconcileResponse {
            fragment: u32::MAX,
            summaries: Vec::new(),
            session_id: u64::MAX,
            round_id: u64::MAX,
            page_seq: 128,
            next_after_key: String::new(),
            snapshot_revision: u64::MAX,
            snapshot_time_ms: u64::MAX,
            done: false,
            restart: true,
        };
        let empty = assert_reconcile_response_fit_matches_reference(base.clone(), "cursor", true);
        assert!(empty.summaries.is_empty());
        assert_eq!(empty.next_after_key, "cursor");
        assert!(empty.done);

        let summary = |key: String| KeySummary {
            content_digest: vec![0; 32],
            key,
            timestamp_ms: u64::MAX,
            logical_counter: 0,
            node_id: "node".to_string(),
            is_tombstone: true,
        };
        let singleton = assert_reconcile_response_fit_matches_reference(
            ReconcileResponse {
                summaries: vec![summary(format!("b{}", "x".repeat(600 * 1024)))],
                ..base.clone()
            },
            "a",
            true,
        );
        assert_eq!(singleton.summaries.len(), 1);
        assert!(
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(singleton.clone()))
            }
            .encoded_len()
                > ANTI_ENTROPY_PAGE_BYTES
        );
        assert!(singleton.done);

        let fixed_key = |prefix: char| format!("{prefix}{}", "x".repeat(300 * 1024));
        let truncated = assert_reconcile_response_fit_matches_reference(
            ReconcileResponse {
                summaries: vec![
                    summary(fixed_key('a')),
                    summary(fixed_key('b')),
                    summary(fixed_key('c')),
                ],
                ..base.clone()
            },
            "",
            true,
        );
        assert_eq!(truncated.summaries.len(), 2);
        assert!(!truncated.done);

        let long_middle = format!("b{}", "x".repeat(600 * 1024));
        let non_monotonic = ReconcileResponse {
            summaries: vec![
                summary("a".to_string()),
                summary(long_middle),
                summary("c".to_string()),
            ],
            ..base
        };
        let mut first_two = non_monotonic.clone();
        first_two.summaries.truncate(2);
        first_two.next_after_key = first_two.summaries[1].key.clone();
        assert!(
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(first_two))
            }
            .encoded_len()
                > ANTI_ENTROPY_PAGE_BYTES
        );
        let fitted = assert_reconcile_response_fit_matches_reference(non_monotonic, "", true);
        assert_eq!(fitted.summaries.len(), 3);
        assert_eq!(fitted.next_after_key, "c");
        assert!(fitted.done);

        let mut oversized = ReconcileResponse {
            summaries: vec![summary("z".repeat(MAX_FRAME_BYTES / 2))],
            ..ReconcileResponse::default()
        };
        assert!(fit_reconcile_response_page(&mut oversized, "", true).is_err());
    }

    #[test]
    fn reconcile_data_reserves_terminal_flag_across_sequence_boundaries() {
        for page_seq in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut page = ReconcileData {
                fragment: u32::MAX,
                entries: Vec::new(),
                session_id: u64::MAX,
                round_id: u64::MAX,
                chunk_seq: u64::MAX,
                page_seq,
                done: false,
            };
            let base_payload_len = reconcile_data_reserved_base_len(&page);
            let mut sizing = RepeatedFieldSizer::new(14, 2, base_payload_len);
            for value_len in [0, 1, 127, 128, 16_383, 16_384] {
                let entry = SyncEntry {
                    key: format!("key-{value_len}"),
                    value: vec![7; value_len],
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "node".to_string(),
                    expires_at_ms: u64::MAX,
                    is_tombstone: true,
                };
                let item_len = entry.encoded_len();
                let predicted = sizing.encoded_len_with(item_len);
                page.entries.push(entry);
                let mut terminal = page.clone();
                terminal.done = true;
                assert_eq!(
                    predicted,
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(terminal))
                    }
                    .encoded_len(),
                    "page_seq={page_seq}, value_len={value_len}"
                );
                assert_eq!(sizing.push(item_len), predicted);
                assert!(
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(page.clone()))
                    }
                    .encoded_len()
                        <= predicted
                );
            }
        }
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

    #[tokio::test]
    async fn receive_byte_permit_follows_completed_frame_and_releases_on_eof() {
        let budget = Arc::new(Semaphore::new(8));
        let (_cancel, mut cancelled) = watch::channel(false);
        let (mut client, mut reader) = tokio::io::duplex(64);
        client.write_u32(4).await.unwrap();
        client.write_all(&[1, 2, 3, 4]).await.unwrap();
        let frame = read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(frame.bytes, [1, 2, 3, 4]);
        assert_eq!(budget.available_permits(), 4);
        drop(frame);
        assert_eq!(budget.available_permits(), 8);

        // Empty bodies consume no bytes, but still follow normal framing.
        client.write_u32(0).await.unwrap();
        let empty = read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(empty.bytes.is_empty());
        drop(empty);
        assert_eq!(budget.available_permits(), 8);

        client.write_u32(8).await.unwrap();
        client.write_all(&[1, 2]).await.unwrap();
        drop(client);
        assert!(read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .is_none());
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn receive_byte_permit_releases_on_timeout_cancel_and_task_abort() {
        for failure in 0..3 {
            let budget = Arc::new(Semaphore::new(8));
            let (cancel, mut cancelled) = watch::channel(false);
            let (mut client, mut reader) = tokio::io::duplex(64);
            client.write_u32(8).await.unwrap();
            client.write_all(&[1]).await.unwrap();
            let read_budget = budget.clone();
            let read = tokio::spawn(async move {
                read_connection_frame(
                    &mut reader,
                    &mut cancelled,
                    &read_budget,
                    true,
                    Instant::now(),
                    if failure == 0 {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_secs(10)
                    },
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                while budget.available_permits() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("incomplete body did not reserve its bytes");
            match failure {
                0 => assert!(read.await.unwrap().is_none()),
                1 => {
                    cancel.send(true).unwrap();
                    assert!(read.await.unwrap().is_none());
                }
                _ => {
                    read.abort();
                    assert!(matches!(read.await, Err(error) if error.is_cancelled()));
                }
            }
            assert_eq!(budget.available_permits(), 8);
        }
    }

    #[tokio::test]
    async fn receive_byte_admission_checks_handshake_and_hard_limits_first() {
        for (established, len) in [(false, 4097), (true, MAX_FRAME_BYTES + 1)] {
            let budget = Arc::new(Semaphore::new(8));
            let (_cancel, mut cancelled) = watch::channel(false);
            let (mut client, mut reader) = tokio::io::duplex(64);
            client.write_u32(len as u32).await.unwrap();
            assert!(read_connection_frame(
                &mut reader,
                &mut cancelled,
                &budget,
                established,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .await
            .is_none());
            assert_eq!(budget.available_permits(), 8);
        }
    }

    #[tokio::test]
    async fn receive_byte_admission_is_shared_and_saturation_is_connection_local() {
        let capacity = 2 * MAX_FRAME_BYTES;
        let budget = Arc::new(Semaphore::new(capacity));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (mut ctx, runtime_tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers.clone(),
        )
        .await;
        ctx.receive_byte_budget = budget.clone();
        ctx.runtime_options = NodeRuntimeOptions::default()
            .with_receive_byte_capacity(capacity)
            .with_socket_read_timeout(Duration::from_secs(10));
        let mut connections = Vec::new();
        for (peer, we_dialed) in [
            ("held-accepted", false),
            ("held-dialed", true),
            ("healthy", false),
            ("shed", false),
        ] {
            let (mut client, server) = tokio::io::duplex(4096);
            let handler = tokio::spawn(handle_connection(
                ctx.clone(),
                Box::pin(server),
                we_dialed,
                we_dialed.then(|| peer.to_string()),
                None,
            ));
            complete_test_handshake(&mut client, peer).await;
            connections.push((client, handler));
        }
        let [
            (mut accepted, accepted_handler),
            (mut dialed, dialed_handler),
            (mut healthy, healthy_handler),
            (mut shed, shed_handler),
        ]: [_; 4] = connections.try_into().ok().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while peers.lock().await.len() != 4 || budget.available_permits() != capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handshake frames retained receive permits");

        // Accepted and outbound-dialed handlers each announce an incomplete
        // maximum frame. Their reservations must share the one node budget.
        accepted.write_u32(MAX_FRAME_BYTES as u32).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() != MAX_FRAME_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        dialed.write_u32(MAX_FRAME_BYTES as u32).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // No body follows: rejection must happen at admission, before
        // allocation/read/decode, without waiting for the held frames.
        shed.write_u32(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), shed_handler)
            .await
            .expect("receive saturation waited for body bytes")
            .unwrap();
        let mut byte = [0];
        assert_eq!(shed.read(&mut byte).await.unwrap(), 0);
        assert_eq!(budget.available_permits(), 0);
        assert!(peers.lock().await.contains_key("healthy"));
        assert!(!peers.lock().await.contains_key("shed"));

        drop(accepted);
        tokio::time::timeout(Duration::from_secs(1), accepted_handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), MAX_FRAME_BYTES);
        write_test_envelope(
            &mut healthy,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 71,
                    keys: vec!["probe".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut healthy).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse { req_id: 71, results }))
                if results.is_empty()
        ));

        let (mut replacement, server) = tokio::io::duplex(4096);
        let replacement_handler =
            tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut replacement, "held-accepted").await;
        write_test_envelope(
            &mut replacement,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 72,
                    keys: vec!["replacement-probe".to_string()],
                    priority: ProtoPriority::High as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut replacement).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse { req_id: 72, results }))
                if results.is_empty()
        ));

        drop(dialed);
        drop(healthy);
        drop(replacement);
        for handler in [dialed_handler, healthy_handler, replacement_handler] {
            tokio::time::timeout(Duration::from_secs(1), handler)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(budget.available_permits(), capacity);
        assert!(peers.lock().await.is_empty());
        runtime_tasks.shutdown().await;
    }

    #[tokio::test]
    async fn dispatcher_restart_is_republished_to_the_fragment_router() {
        let node = Node::start(
            "dispatcher-republish".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_count: 1,
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap();
        let fragment = FragmentId(0);
        node.put("before".to_string(), b"value".to_vec())
            .await
            .unwrap();

        let dead = node
            .router
            .route_for_fragment(fragment)
            .expect("fragment route missing before dispatcher failure");
        let dead_id = dead.get_id();
        dead.kill();

        let replacement = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(candidate) = node.router.route_for_fragment(fragment) {
                    if candidate.get_id() != dead_id {
                        break candidate;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor replacement was not republished");
        assert_ne!(replacement.get_id(), dead_id);
        assert_eq!(
            node.get("before".to_string())
                .await
                .unwrap()
                .expect("replacement dispatcher lost the shared cache")
                .value,
            b"value"
        );

        node.put("after".to_string(), b"replacement".to_vec())
            .await
            .unwrap();
        assert_eq!(
            node.get("after".to_string())
                .await
                .unwrap()
                .expect("replacement dispatcher lost shared state")
                .value,
            b"replacement"
        );

        let replacement_id = replacement.get_id();
        replacement.kill();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if node
                    .router
                    .route_for_fragment(fragment)
                    .is_some_and(|candidate| candidate.get_id() != replacement_id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("monitor did not continue across a second dispatcher restart");
        node.put("after-second".to_string(), b"replacement-2".to_vec())
            .await
            .unwrap();
        assert!(node.health().await.unwrap()[&fragment]
            .restarts
            .get("dispatcher")
            .is_some_and(|restarts| *restarts >= 2));

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_dispatcher_monitor_cannot_overwrite_a_readded_fragment() {
        let router = FragmentRouter::new(1, 1);
        let state = || BlockingPageState {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let (retired, retired_handle) = BlockingPageActor::spawn(None, BlockingPageActor, state())
            .await
            .unwrap();
        let retired_generation = router.insert(FragmentId(0), retired.clone());

        let (stale_replacement, stale_handle) =
            BlockingPageActor::spawn(None, BlockingPageActor, state())
                .await
                .unwrap();
        let requested = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (supervisor, supervisor_handle) = BlockingDispatcherSupervisor::spawn(
            None,
            BlockingDispatcherSupervisor,
            BlockingDispatcherSupervisorState {
                replacement: stale_replacement.clone(),
                requested: requested.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let monitor = tokio::spawn(monitor_fragment_dispatcher(
            FragmentId(0),
            retired_generation,
            supervisor.clone(),
            router.clone(),
            retired.clone(),
            shutdown_rx,
        ));

        retired.kill();
        tokio::time::timeout(Duration::from_secs(1), requested.notified())
            .await
            .expect("monitor did not request the supervisor replacement");

        // Placement replaces the entire subtree while the old monitor holds
        // a now-stale replacement response.
        router.remove(FragmentId(0));

        let (current, current_handle) = BlockingPageActor::spawn(None, BlockingPageActor, state())
            .await
            .unwrap();
        let current_generation = router.insert(FragmentId(0), current.clone());
        assert_ne!(retired_generation, current_generation);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), monitor)
            .await
            .expect("stale monitor did not exit")
            .unwrap();
        assert_eq!(
            router
                .route_for_fragment(FragmentId(0))
                .expect("readded route disappeared")
                .get_id(),
            current.get_id()
        );

        let _ = shutdown_tx.send(true);
        current.stop(None);
        stale_replacement.stop(None);
        supervisor.stop(None);
        retired_handle.await.unwrap();
        current_handle.await.unwrap();
        stale_handle.await.unwrap();
        supervisor_handle.await.unwrap();
    }

    #[test]
    fn challenge_hmac_binds_both_offers_roles_and_freshness() {
        let dialer = Hello {
            node_id: "dialer".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: 0x7f, // Fixed offer for the independent transcript vector.
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        let listener = Hello {
            node_id: "listener".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: 0x7f, // Fixed offer for the independent transcript vector.
            challenge: vec![2; HANDSHAKE_CHALLENGE_BYTES],
        };
        let authenticated = handshake_auth_tag(b"secret", true, &dialer, &listener);
        // Independently computed SHA-256/HMAC vectors pin the existing wire
        // transcript across crypto dependency upgrades (including proof roles).
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            hex(&authenticated),
            "c8481f4b79b30974a6593ea01a69abe9c06cf2612456898d08361d612e14ad1f"
        );
        assert_eq!(
            hex(&handshake_auth_tag(b"secret", false, &dialer, &listener)),
            "caebe45abdee3c01b2f91d9b572d014bf7179368e5ebe5a66dc3a8a1bd162b4c"
        );
        assert_eq!(
            hex(&connection_rank(&dialer, &listener).transcript_id),
            "51fb4b81d53d2ea3fec5121c3ac214123af25fab134cc4b50013b28cfbf39d5e"
        );
        assert!(verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &listener,
            &authenticated,
        ));
        assert!(!verify_handshake_auth(
            Some(b"different secret"),
            true,
            &dialer,
            &listener,
            &authenticated,
        ));
        for malformed in [&authenticated[..31], &[0; 33][..]] {
            assert!(!verify_handshake_auth(
                Some(b"secret"),
                true,
                &dialer,
                &listener,
                malformed,
            ));
        }
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
    fn clock_only_v4_peer_is_rejected_before_digest_reconciliation() {
        let mut hello = Hello {
            node_id: "peer".into(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        assert_eq!(validate_peer_hello("local", Some("peer"), &hello), Ok(()));
        hello.capabilities &= !CAPABILITY_RECORD_CONTENT_DIGEST;
        assert_eq!(
            validate_peer_hello("local", Some("peer"), &hello),
            Err("peer omitted required wire capabilities")
        );
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
            )
            .await;
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
            )
            .await;
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
                    page_seq: 0,
                    done: true,
                })
            } else {
                EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
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
            let probe_req_id = 99;
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: probe_req_id,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            loop {
                if let Some(EnvelopeMsg::BatchGetResponse(response)) =
                    read_test_envelope(&mut client).await.msg
                {
                    assert_eq!(response.req_id, probe_req_id);
                    // This dynamic node does not serve the fragment, so an
                    // empty unavailable prefix is the safe reply. It still
                    // proves both halves of the shared connection survived.
                    assert!(response.results.is_empty());
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
    async fn saturated_retired_sender_does_not_close_shared_connection() {
        for frame_kind in 0..4 {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
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

            let replacement_report = test_sender_report();
            let replacement = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: replacement_report.clone(),
                },
                SenderRuntimeOptions::default(),
            )
            .unwrap();
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports =
                Arc::new(Mutex::new(HashMap::from([(key.clone(), report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (mut ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders.clone(),
                sender_reports.clone(),
                peers.clone(),
            )
            .await;
            let captured = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            ctx.sender_snapshot_hook = Some(SenderSnapshotHook {
                captured: captured.clone(),
                resume: resume.clone(),
            });
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            let stale_control = match frame_kind {
                0 => EnvelopeMsg::BatchAck(BatchAck {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    level: ReplicationAckLevel::Applied as i32,
                }),
                1 => EnvelopeMsg::BatchNack(BatchNack {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    error: "retry".to_string(),
                }),
                2 => EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                    page_seq: 0,
                    done: true,
                }),
                3 => EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
                }),
                _ => unreachable!(),
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(stale_control),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), captured.notified())
                .await
                .expect("connection did not snapshot the old sender");
            senders
                .lock()
                .await
                .insert(key.clone(), replacement.clone());
            sender_reports.lock().await.insert(key, replacement_report);
            resume.notify_one();

            let probe_req_id = 100;
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: probe_req_id,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            loop {
                if let Some(EnvelopeMsg::BatchGetResponse(response)) =
                    read_test_envelope(&mut client).await.msg
                {
                    assert_eq!(response.req_id, probe_req_id);
                    break;
                }
            }
            assert!(peers.lock().await.contains_key("peer"));

            drop(report_guard);
            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
            sender.stop().await;
            replacement.stop().await;
        }
    }

    #[tokio::test]
    async fn current_failed_reconcile_consumer_closes_for_replacement() {
        for (data, fail_after_admission) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            let mut report_guard = Some(report.lock().await);
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            if !fail_after_admission {
                drop(report_guard.take());
                sender.stop().await;
            }
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders,
                sender_reports,
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            let reconcile = if data {
                EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                    page_seq: 0,
                    done: true,
                })
            } else {
                EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;
            if fail_after_admission {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while sender.control_capacity() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("reconcile frame was not admitted to the current sender");
                sender.request_stop();
                drop(report_guard.take());
            }

            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("failed current reconcile consumer did not close the connection")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            runtime_tasks.shutdown().await;
            sender.stop().await;
        }
    }

    #[tokio::test]
    async fn non_serving_reconcile_races_do_not_close_shared_connection() {
        for fetch in [false, true] {
            let fragment = FragmentId(0);
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::from([(
                    fragment,
                    vec!["local".to_string(), "peer".to_string()],
                )]))),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                        session_id: 1,
                        fragment: fragment.0,
                    })),
                },
            )
            .await;
            let reconcile = if fetch {
                EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                    fragment: fragment.0,
                    keys: vec!["connection-probe".to_string()],
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 0,
                })
            } else {
                EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                    fragment: fragment.0,
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    after_key: String::new(),
                    snapshot_revision: 0,
                    snapshot_time_ms: 0,

                    recovery_epoch: Vec::new(),
                    recovery_replica_digest: Vec::new(),
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;

            // A request for another multiplexed protocol proves that the
            // honest placement race did not sacrifice either connection half.
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: 99,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            assert!(matches!(
                read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                    req_id: 99,
                    results,
                })) if results.is_empty()
            ));
            assert!(peers.lock().await.contains_key("peer"));

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
        }
    }

    #[tokio::test]
    async fn blocked_serving_reads_do_not_stall_pending_replies_on_same_connection() {
        for batch in [false, true] {
            let fragment = FragmentId(0);
            let router = FragmentRouter::new(1, 0);
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let (actor, _) = ractor::Actor::spawn(
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
            let (mut ctx, runtime_tasks) = test_connection_context(
                router,
                Arc::new(RwLock::new(HashSet::from([fragment]))),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
            )
            .await;
            ctx.runtime_options = NodeRuntimeOptions::default()
                .with_forwarded_attempt_timeout(Duration::from_secs(10));
            let receive_byte_budget = ctx.receive_byte_budget.clone();
            let (reply, completed) = oneshot::channel();
            ctx.pending_gets.lock().unwrap().insert(
                99,
                PendingGet {
                    expected_peer: "peer".into(),
                    expected_key: "reply-key".into(),
                    reply,
                },
            );
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            let request = if batch {
                EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 1,
                    keys: vec!["held".into()],
                    priority: ProtoPriority::Background as i32,
                })
            } else {
                EnvelopeMsg::ForwardedGet(ForwardedGet {
                    req_id: 1,
                    key: "held".into(),
                    priority: ProtoPriority::Background as i32,
                    hops: 0,
                })
            };
            write_test_envelope(&mut client, Envelope { msg: Some(request) }).await;
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .unwrap();
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                        req_id: 99,
                        ..GetResponse::default()
                    })),
                },
            )
            .await;
            assert!(tokio::time::timeout(Duration::from_secs(1), completed)
                .await
                .expect("serving read blocked a pending reply on its physical connection")
                .unwrap()
                .unwrap()
                .is_none());
            tokio::time::timeout(Duration::from_secs(1), async {
                while receive_byte_budget.available_permits() != RECEIVE_BYTE_CAPACITY {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("decoded read job retained the raw receive body permit");
            // Cancel while the actor still owns the request, rather than
            // relying on its ten-second deadline to release the socket reader.
            connection.abort();
            assert!(connection.await.unwrap_err().is_cancelled());
            release.notify_one();
            actor.stop_and_wait(None, None).await.unwrap();
            runtime_tasks.shutdown().await;
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
        )
        .await;
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
        )
        .await;

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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let sender_config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let operation_gate = OperationGate::new();

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
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
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
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
    async fn stopped_drain_sender_returns_to_retryable_preparation() {
        let fragment = FragmentId(0);
        let node = Node::start(
            "drain-replacement-source".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_map: HashMap::from([(
                    fragment,
                    vec![
                        "drain-replacement-source".to_string(),
                        "absent-target".to_string(),
                    ],
                )]),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap();
        node.put("replacement-fence".to_string(), b"value".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::PeerReconciliation,
                ..
            })
        ));

        let stopped = node
            .senders
            .lock()
            .await
            .get(&(fragment, "absent-target".to_string()))
            .cloned()
            .unwrap();
        stopped.stop().await;
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::RetryableSender(_))
        ));

        // The failed exact waiter moved the coordinator back to preparation.
        // A retry replaces the dead task and waits on a new generation rather
        // than returning the same permanent Unavailable error.
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::PeerReconciliation,
                ..
            })
        ));
        node.shutdown().await.unwrap();
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
    async fn report_wait_releases_global_registries_and_restores_a_readded_fragment() {
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
            while serving.read().unwrap().contains(&fragment) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loss transition did not revoke serving");
        assert!(
            !loss.is_finished(),
            "locked sender report must delay teardown"
        );
        let senders_guard = tokio::time::timeout(Duration::from_millis(100), senders.lock())
            .await
            .expect("sender registry stayed locked across a report wait");
        drop(senders_guard);
        let reports_guard =
            tokio::time::timeout(Duration::from_millis(100), deps.sender_reports.lock())
                .await
                .expect("report registry stayed locked across an entry report wait");
        drop(reports_guard);
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
    async fn final_teardown_waits_on_only_the_fragment_mutation_fence() {
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
            while serving.read().unwrap().contains(&fragment) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loss transition did not revoke serving");
        let admitted = mutation_gate.read_owned().await;
        drop(report_guard);
        let publish_states = deps.placement_states.clone();
        let publish_placement = placement.clone();
        let publisher = tokio::spawn(async move {
            let _states = publish_states.lock().await;
            publish_placement
                .write()
                .unwrap()
                .insert(fragment, vec!["node-a".to_string()]);
        });
        tokio::time::timeout(Duration::from_secs(1), publisher)
            .await
            .expect("placement publication was blocked by a fragment mutation fence")
            .unwrap();
        drop(admitted);
        tokio::time::timeout(Duration::from_secs(1), loss)
            .await
            .expect("teardown did not restore the re-added fragment")
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
    async fn teardown_without_registered_supervisor_stays_draining() {
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

        assert!(
            tokio::time::timeout(Duration::from_millis(100), lose_fragment(&deps, fragment))
                .await
                .is_err(),
            "teardown claimed completion without lifecycle ownership"
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.is_empty());
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Draining)
        );
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn stale_sender_ensure_refuses_a_removed_router_fragment() {
        let router = FragmentRouter::new(1, 0);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peer_failures = PeerFailureSignals::default();
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let operation_gate = OperationGate::new();

        ensure_sender_entry(
            FragmentId(0),
            "peer",
            SenderEntryDeps {
                config: &config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;

        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());
    }

    #[tokio::test]
    async fn finalizing_fragment_refuses_sender_recreation_until_the_seal_drops() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let config = NodeConfig::default();
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("sealed-sender-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let peer_failures = PeerFailureSignals::default();
        let sender_config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let operation_gate = OperationGate::new();
        let seal = {
            let _registry_order = senders.lock().await;
            finalizing_senders.seal(fragment).unwrap()
        };

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());

        drop(seal);
        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        let sender = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .expect("sender was not recreated after finalization ended");

        sender.stop().await;
        router.remove(fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn sender_snapshot_identity_rejects_same_key_replacement() {
        let fragment = FragmentId(0);
        let key = (fragment, "peer".to_string());
        let report = test_sender_report();
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let replacement_report = test_sender_report();
        let replacement = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: replacement_report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let senders: SenderMap =
            Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::from([(key.clone(), report)])));
        assert!(sender_task_is_current(&senders, &key, &sender).await);
        let snapshot = {
            let senders = senders.lock().await;
            let reports = reports.lock().await;
            snapshot_fragment_sender_set(fragment, &senders, &reports).unwrap()
        };

        senders
            .lock()
            .await
            .insert(key.clone(), replacement.clone());
        reports.lock().await.insert(key.clone(), replacement_report);
        assert!(!sender_task_is_current(&senders, &key, &sender).await);
        let drained = lock_drained_fragment_sender_snapshot(snapshot)
            .await
            .expect("detached empty sender should be drainable");
        {
            let senders = senders.lock().await;
            let reports = reports.lock().await;
            assert!(!fragment_sender_snapshot_is_current(
                fragment,
                &drained.entries,
                &senders,
                &reports,
            ));
        }
        drop(drained);

        sender.stop().await;
        replacement.stop().await;
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
        )
        .await;
        assert_eq!(
            MutationStatus::try_from(result.status),
            Ok(MutationStatus::Unavailable)
        );
    }

    #[test]
    fn legacy_get_response_shapes_fail_closed() {
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
        assert!(validate_legacy_get_response("key", &not_found).is_ok());

        let found = GetResponse {
            found: true,
            value: b"value".to_vec(),
            timestamp_ms: 7,
            logical_counter: 0,
            node_id: "owner".to_string(),
            ..not_found.clone()
        };
        assert!(validate_legacy_get_response("key", &found).is_ok());

        let shed = GetResponse {
            shed: true,
            ..not_found.clone()
        };
        assert!(validate_legacy_get_response("key", &shed).is_ok());

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
            assert!(validate_legacy_get_response("key", &malformed).is_err());
        }
    }

    #[tokio::test]
    async fn direct_and_relayed_get_responses_are_pinned_to_the_expected_peer() {
        let pending_gets: PendingGets = Arc::new(StdMutex::new(HashMap::new()));
        let relays: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (mut ctx, runtime_tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers,
        )
        .await;
        ctx.pending_gets = pending_gets.clone();
        ctx.pending_relays = relays.clone();
        let (mut owner, owner_server) = tokio::io::duplex(4096);
        let owner_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(owner_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        let expected_tx = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(tx) = ctx.peers.lock().await.get("owner").cloned() {
                    break tx;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (pending_reply, pending_response) = oneshot::channel();
        pending_gets.lock().unwrap().insert(
            31,
            PendingGet {
                expected_peer: "owner".to_string(),
                expected_key: "key".to_string(),
                reply: pending_reply,
            },
        );
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (timeout_cancel, _timeout_cancelled) = oneshot::channel();
        relays.lock().unwrap().insert(
            32,
            GetRelay {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_key: "key".to_string(),
                origin_tx,
                origin_req_id: 9,
                timeout_cancel,
                _admission: ctx
                    .relay_admission
                    .try_reserve(relay_key_storage_bytes(3))
                    .unwrap(),
            },
        );

        let (mut attacker, attacker_server) = tokio::io::duplex(4096);
        let attacker_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(attacker_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut attacker, "attacker").await;
        let not_found_response = |req_id| Envelope {
            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id,
                found: false,
                value: Vec::new(),
                timestamp_ms: 0,
                logical_counter: 0,
                node_id: String::new(),
                expires_at_ms: 0,
                is_tombstone: false,
                shed: false,
            })),
        };
        for req_id in [31, 32] {
            write_test_envelope(&mut attacker, not_found_response(req_id)).await;
        }
        write_test_envelope(
            &mut attacker,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 33,
                    keys: vec!["barrier".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut attacker).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 33,
                results,
            })) if results.is_empty()
        ));
        assert!(pending_gets.lock().unwrap().contains_key(&31));
        assert!(relays.lock().unwrap().contains_key(&32));
        assert!(origin_response_rx.try_recv().is_err());

        for req_id in [31, 32] {
            write_test_envelope(&mut owner, not_found_response(req_id)).await;
        }
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 34,
                    keys: vec!["barrier".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut owner).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 34,
                results,
            })) if results.is_empty()
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), pending_response)
                .await
                .expect("pending read response timed out")
                .unwrap()
                .is_ok()
        );
        let relayed = tokio::time::timeout(Duration::from_secs(1), origin_response_rx.recv())
            .await
            .expect("relay response timed out")
            .expect("relay response missing");
        assert!(matches!(
            Envelope::decode(relayed.bytes()).unwrap().msg,
            Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id: 9,
                found: false,
                ..
            }))
        ));
        assert!(pending_gets.lock().unwrap().is_empty());
        assert!(relays.lock().unwrap().is_empty());

        drop(attacker);
        drop(owner);
        tokio::time::timeout(Duration::from_secs(1), attacker_connection)
            .await
            .expect("attacker connection did not stop")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), owner_connection)
            .await
            .expect("owner connection did not stop")
            .unwrap();
        runtime_tasks.shutdown().await;
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

    #[tokio::test]
    async fn full_peer_queue_times_out_cancels_and_does_not_admit_frame() {
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
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
    async fn reader_response_saturation_cancels_without_waiting() {
        let (peer, _rx, mut response_rx) = test_peer_channel(1);
        let envelope = || Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: 1,
                batch_seq: 1,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        };
        assert!(try_send_envelope_immediate(&peer, envelope()));
        let mut cancelled = peer.cancel.subscribe();
        assert!(!try_send_envelope_immediate(&peer, envelope()));
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("saturated reader response did not cancel the connection")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
        assert!(response_rx.recv().await.is_some());
        assert!(response_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_producer_share_still_admits_a_near_limit_response() {
        let (peer, _rx, mut response_rx) = test_peer_channel(1);
        let _producer_permit = peer
            .outbound
            .producer_byte_budget
            .clone()
            .acquire_many_owned(MAX_FRAME_BYTES as u32)
            .await
            .unwrap();
        let _shared_producer_permit = peer
            .outbound
            .byte_budget
            .clone()
            .acquire_many_owned(MAX_FRAME_BYTES as u32)
            .await
            .unwrap();
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: u64::MAX,
                results: vec![BatchKeyValue {
                    key: "large".to_string(),
                    found: true,
                    value: vec![7; MAX_FRAME_BYTES - 128],
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "node".to_string(),
                    expires_at_ms: u64::MAX,
                    is_tombstone: false,
                    shed: false,
                }],
            })),
        };
        assert!(envelope.encoded_len() > MAX_FRAME_BYTES - 256);
        assert!(envelope.encoded_len() <= MAX_FRAME_BYTES);
        assert!(try_send_envelope_immediate(&peer, envelope));
        assert!(response_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn session_replacement_cancels_in_progress_reconcile_response() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, actor_handle) = BlockingPageActor::spawn(
            None,
            BlockingPageActor,
            BlockingPageState {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        let (peer, _frames, _responses) = test_peer_channel(1);
        let session_key = ("peer".to_string(), FragmentId(0));
        let original_session = InboundSession {
            session_id: 7,
            generation: 1,
        };
        let sessions: InboundSessions = Arc::new(Mutex::new(HashMap::from([(
            session_key.clone(),
            original_session,
        )])));
        let (session_changes, session_change_rx) = watch::channel(0u64);
        let (jobs, job_rx) = mpsc::channel(1);
        let responder = tokio::spawn(run_reconcile_responder(
            job_rx,
            peer,
            sessions.clone(),
            session_change_rx,
            Duration::from_secs(30),
        ));
        jobs.send(ReconcileResponseJob {
            peer: "peer".to_string(),
            session: original_session,
            actor,
            fetch: ReconcileFetch {
                fragment: 0,
                keys: vec!["key".to_string()],
                session_id: original_session.session_id,
                round_id: 1,
                chunk_seq: 0,
            },
            snapshot_time_ms: 1,
        })
        .await
        .unwrap();
        started.notified().await;

        sessions.lock().await.insert(
            session_key,
            InboundSession {
                session_id: 8,
                generation: 2,
            },
        );
        session_changes.send_modify(|generation| *generation += 1);
        drop(jobs);
        tokio::time::timeout(Duration::from_secs(1), responder)
            .await
            .expect("stale responder remained blocked on its actor RPC")
            .unwrap();

        release.notify_one();
        actor_handle.abort();
        let _ = actor_handle.await;
    }

    #[tokio::test]
    async fn oversized_outbound_frame_is_rejected_before_queue_admission() {
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec!["x".repeat(MAX_FRAME_BYTES)],
                priority: ProtoPriority::Default as i32,
            })),
        };

        assert!(!try_send_envelope(&peer, envelope).await);
        assert!(rx.try_recv().is_err());
        assert!(try_send_envelope(&peer, Envelope { msg: None }).await);
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn forwarded_attempt_deadline_caps_queue_admission_wait() {
        let (mut peer, mut rx, _response_rx) = test_peer_channel(1);
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
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
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
        let (peer, rx, _response_rx) = test_peer_channel(1);
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
        let (current, _current_rx, _current_response_rx) = test_peer_channel(1);
        let (stale, _stale_rx, _stale_response_rx) = test_peer_channel(1);
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
    async fn peer_writer_prioritizes_reader_responses_over_queued_data() {
        let (mut reader, writer) = tokio::io::duplex(64);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(2));
        let producer_byte_budget = Arc::new(Semaphore::new(1));

        let producer_permit = producer_byte_budget.clone().acquire_owned().await.unwrap();
        let data_permit = byte_budget.clone().acquire_owned().await.unwrap();
        frame_tx
            .send(BudgetedFrame::new_producer(
                vec![1],
                data_permit,
                producer_permit,
            ))
            .await
            .unwrap();
        let response_permit = byte_budget.clone().acquire_owned().await.unwrap();
        response_tx
            .send(BudgetedFrame::new_response(vec![2], response_permit))
            .await
            .unwrap();
        drop(frame_tx);
        drop(response_tx);

        let task = tokio::spawn(run_bounded_peer_writer(
            writer,
            PeerWriterPipeline {
                data_rx: frame_rx,
                response_rx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
            },
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(1),
        ));
        for expected in [2, 1] {
            assert_eq!(reader.read_u32().await.unwrap(), 1);
            assert_eq!(reader.read_u8().await.unwrap(), expected);
        }
        task.await.unwrap();
        assert!(byte_budget.is_closed());
        assert!(producer_byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 2);
        assert_eq!(producer_byte_budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn peer_writer_bounds_response_bursts_to_preserve_data_progress() {
        let response_count = PEER_RESPONSE_BURST_LIMIT + 1;
        let (mut reader, writer) = tokio::io::duplex(256);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(response_count);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(response_count + 1));
        let producer_byte_budget = Arc::new(Semaphore::new(1));

        let producer_permit = producer_byte_budget.clone().acquire_owned().await.unwrap();
        let data_permit = byte_budget.clone().acquire_owned().await.unwrap();
        frame_tx
            .send(BudgetedFrame::new_producer(
                vec![1],
                data_permit,
                producer_permit,
            ))
            .await
            .unwrap();
        for _ in 0..response_count {
            let permit = byte_budget.clone().acquire_owned().await.unwrap();
            response_tx
                .send(BudgetedFrame::new_response(vec![2], permit))
                .await
                .unwrap();
        }
        drop(frame_tx);
        drop(response_tx);

        let task = tokio::spawn(run_bounded_peer_writer(
            writer,
            PeerWriterPipeline {
                data_rx: frame_rx,
                response_rx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
            },
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(1),
        ));
        for _ in 0..PEER_RESPONSE_BURST_LIMIT {
            assert_eq!(reader.read_u32().await.unwrap(), 1);
            assert_eq!(reader.read_u8().await.unwrap(), 2);
        }
        assert_eq!(reader.read_u32().await.unwrap(), 1);
        assert_eq!(reader.read_u8().await.unwrap(), 1);
        task.await.unwrap();
        assert_eq!(byte_budget.available_permits(), response_count + 1);
        assert_eq!(producer_byte_budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn socket_write_timeout_signals_connection_cancellation() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (cancel_tx, mut cancelled) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(1024));
        frame_tx
            .send(test_budgeted_frame(vec![0; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
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
        let byte_budget = Arc::new(Semaphore::new(1024));
        frame_tx
            .send(test_budgeted_frame(vec![0; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
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
        assert!(byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 1024);
    }

    #[tokio::test]
    async fn aborting_peer_writer_releases_current_and_queued_byte_permits() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(2);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(2048));
        frame_tx
            .send(test_budgeted_frame(vec![1; 1024], &byte_budget).await)
            .await
            .unwrap();
        frame_tx
            .send(test_budgeted_frame(vec![2; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(30),
        ));
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 2048);
    }

    #[tokio::test]
    async fn peer_byte_budget_backpressures_producers_before_frame_allocation() {
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec!["bounded".repeat(32)],
                priority: ProtoPriority::Default as i32,
            })),
        };
        let encoded_len = envelope.encoded_len();
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let (response_tx, _response_rx) = mpsc::channel(8);
        let (identity, _) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(encoded_len * 3));
        let producer_byte_budget = Arc::new(Semaphore::new(encoded_len * 2));
        let peer = PeerChannel {
            outbound: BudgetedPeerTx {
                node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                tx: frame_tx,
                response_tx,
                byte_budget,
                producer_byte_budget,
                reconcile_gate: ReconcileGate::new(),
                identity,
            },
            cancel: cancel_tx,
            queue_admission_timeout: Duration::from_millis(10),
            rank: None,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
        };

        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert!(!try_send_envelope(&peer, envelope).await);
        assert_eq!(frame_rx.len(), 2);
        assert_eq!(peer.outbound.tx.capacity(), 6);
    }

    #[tokio::test]
    async fn reader_responses_use_reserved_byte_capacity() {
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: 1,
                batch_seq: 1,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        };
        let encoded_len = envelope.encoded_len();
        let (frame_tx, mut frame_rx) = mpsc::channel(1);
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let (identity, _) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(encoded_len * 2));
        let producer_byte_budget = Arc::new(Semaphore::new(encoded_len));
        let peer = PeerChannel {
            outbound: BudgetedPeerTx {
                node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                tx: frame_tx,
                response_tx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
                reconcile_gate: ReconcileGate::new(),
                identity,
            },
            cancel: cancel_tx,
            queue_admission_timeout: Duration::from_millis(10),
            rank: None,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
        };

        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert_eq!(producer_byte_budget.available_permits(), 0);
        assert_eq!(byte_budget.available_permits(), encoded_len);
        assert!(try_send_envelope_immediate(&peer, envelope));
        assert_eq!(byte_budget.available_permits(), 0);
        drop(response_rx.recv().await.unwrap());
        assert_eq!(byte_budget.available_permits(), encoded_len);
        assert_eq!(producer_byte_budget.available_permits(), 0);
        drop(frame_rx.recv().await.unwrap());
        assert_eq!(byte_budget.available_permits(), encoded_len * 2);
        assert_eq!(producer_byte_budget.available_permits(), encoded_len);
    }

    #[tokio::test(start_paused = true)]
    async fn mutation_relay_timeout_removes_entry_and_replies_unavailable() {
        let relays: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let operation_gate = OperationGate::new();
        let operation = operation_gate.try_enter().unwrap();
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            &admission,
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(50),
                operation,
            },
        )
        .await
        .unwrap()
        .disarm();
        assert!(relays.lock().unwrap().contains_key(&17));
        operation_gate.begin_shutdown();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), operation_gate.wait_for_drain())
                .await
                .is_err(),
            "relay released mutation admission before response or timeout"
        );

        tokio::time::advance(Duration::from_millis(50)).await;
        let bytes = tokio::time::timeout(Duration::from_secs(1), origin_response_rx.recv())
            .await
            .expect("relay timeout did not reply")
            .expect("relay response channel closed");
        let envelope = Envelope::decode(bytes.bytes()).expect("invalid relay response");
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
        assert!(relays.lock().unwrap().is_empty());
        tokio::time::timeout(Duration::from_secs(1), operation_gate.wait_for_drain())
            .await
            .expect("relay timeout did not release mutation admission");
        tasks.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn completed_mutation_relay_cancels_timeout() {
        let relays: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let operation_gate = OperationGate::new();
        let operation = operation_gate.try_enter().unwrap();
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            &admission,
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(10),
                operation,
            },
        )
        .await
        .unwrap()
        .disarm();
        let relay = relays.lock().unwrap().remove(&17).expect("relay missing");
        let MutationRelay {
            timeout_cancel,
            _operation: operation,
            _admission: relay_admission,
            ..
        } = relay;
        let _ = timeout_cancel.send(());
        operation_gate.begin_shutdown();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), operation_gate.wait_for_drain())
                .await
                .is_err(),
            "removed relay released mutation admission before response handling finished"
        );
        drop(operation);
        drop(relay_admission);
        tokio::time::timeout(Duration::from_secs(1), operation_gate.wait_for_drain())
            .await
            .expect("completed relay did not release mutation admission");

        tokio::time::advance(Duration::from_millis(30)).await;
        // A removed entry alone does not prove that its canceled timer exited.
        // The timer retains the sole admission slot until its future drops.
        let _timer_exited = tokio::time::timeout(Duration::from_secs(1), admission.slots.acquire())
            .await
            .expect("completed relay timer did not release admission")
            .expect("relay admission closed");
        assert!(origin_response_rx.try_recv().is_err());
        assert!(relays.lock().unwrap().is_empty());
        tasks.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn read_relay_timeout_removes_registry_entry() {
        let relays: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, _origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        register_get_relay(
            &task_ref,
            relays.clone(),
            &admission,
            23,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_key: "key".to_string(),
                origin_tx,
                origin_req_id: 11,
                deadline: Instant::now() + Duration::from_millis(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        assert!(relays.lock().unwrap().contains_key(&23));
        tokio::time::advance(Duration::from_millis(10)).await;
        let _timer_exited = tokio::time::timeout(Duration::from_secs(1), admission.slots.acquire())
            .await
            .expect("relay deadline timer did not release admission")
            .expect("relay admission closed");
        assert!(relays.lock().unwrap().is_empty());
        tasks.shutdown().await;
    }

    async fn wait_for_relay_admission(admission: &RelayAdmission, slots: usize, bytes: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while admission.slots.available_permits() != slots
                || admission.key_bytes.available_permits() != bytes
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay admission was not reclaimed after timer exit");
    }

    #[tokio::test]
    async fn origin_cancellation_reclaims_relays_without_connection_cleanup() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(1);
        let gate = OperationGate::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".into(),
                expected_tx: owner.clone(),
                expected_key: "key".into(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                deadline,
            },
        )
        .await
        .unwrap()
        .disarm();
        register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".into(),
                expected_tx: owner.clone(),
                expected_keys: vec!["key".into(), "key".into()],
                origin_tx: origin.clone(),
                origin_req_id: 12,
                deadline,
                operation: gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap()
        .disarm();
        assert_eq!(admission.slots.available_permits(), 0);
        gate.begin_shutdown();
        let mut drain = Box::pin(gate.wait_for_drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());

        // The reader can be aborted before reaching its cleanup tail. Only
        // the registered timers observe this origin's terminal cancellation.
        origin.cancel();
        wait_for_relay_admission(&admission, 2, 1024).await;
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("origin cancellation retained the mutation operation fence");
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        assert!(origin_responses.try_recv().is_err());
        assert!(
            !*owner.cancel.borrow(),
            "origin loss canceled a healthy owner"
        );
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_cleanup_is_pinned_to_exact_owner_origin_and_registration_generation() {
        let tasks = RuntimeTasks::new(8);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(8, 4096);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (old_owner, _old_owner_rx, _old_owner_responses) = test_peer_channel(1);
        let (new_owner, _new_owner_rx, _new_owner_responses) = test_peer_channel(1);
        let (old_origin, _old_origin_rx, mut old_origin_responses) = test_peer_channel(4);
        let (new_origin, _new_origin_rx, mut new_origin_responses) = test_peer_channel(4);
        let gate = OperationGate::new();
        for (id, owner, origin) in [
            (1, &old_owner, &new_origin),
            (2, &new_owner, &new_origin),
            (3, &new_owner, &old_origin),
        ] {
            register_get_relay(
                &task_ref,
                reads.clone(),
                &admission,
                id,
                GetRelayRegistration {
                    expected_peer: "same-owner-id".to_string(),
                    expected_tx: owner.clone(),
                    expected_key: "key".to_string(),
                    origin_tx: origin.clone(),
                    origin_req_id: id + 100,
                    deadline: Instant::now() + Duration::from_secs(10),
                },
            )
            .await
            .unwrap()
            .disarm();
            register_mutation_relay(
                &task_ref,
                mutations.clone(),
                &admission,
                id,
                MutationRelayRegistration {
                    expected_peer: "same-owner-id".to_string(),
                    expected_tx: owner.clone(),
                    expected_keys: vec!["key".to_string()],
                    origin_tx: origin.clone(),
                    origin_req_id: id + 200,
                    deadline: Instant::now() + Duration::from_secs(10),
                    operation: gate.try_enter().unwrap(),
                },
            )
            .await
            .unwrap()
            .disarm();
        }
        // Old physical owner cleanup must not clear replacement work which
        // carries the exact same authenticated peer id.
        old_owner.cancel();
        cancel_connection_relays(&old_owner, &reads, &mutations);
        assert!(!reads.lock().unwrap().contains_key(&1));
        assert!(!mutations.lock().unwrap().contains_key(&1));
        assert_eq!(reads.lock().unwrap().len(), 2);
        assert_eq!(mutations.lock().unwrap().len(), 2);
        let unavailable = new_origin_responses.try_recv().unwrap();
        assert!(matches!(Envelope::decode(unavailable.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 201 && response.results[0].status == MutationStatus::Unavailable as i32));
        old_origin.cancel();
        cancel_connection_relays(&old_origin, &reads, &mutations);
        assert_eq!(reads.lock().unwrap().len(), 1);
        assert_eq!(mutations.lock().unwrap().len(), 1);
        assert!(reads.lock().unwrap().contains_key(&2));
        assert!(mutations.lock().unwrap().contains_key(&2));
        assert!(old_origin_responses.try_recv().is_err());
        cancel_connection_relays(&old_owner, &reads, &mutations);
        assert!(reads.lock().unwrap().contains_key(&2));
        assert!(mutations.lock().unwrap().contains_key(&2));

        // A dropped registration guard/timer may race request-id reuse. It
        // must compare its lease generation rather than delete a new entry.
        let stale_guard = register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            4,
            GetRelayRegistration {
                expected_peer: "same-owner-id".to_string(),
                expected_tx: new_owner.clone(),
                expected_key: "old".to_string(),
                origin_tx: new_origin.clone(),
                origin_req_id: 104,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap();
        let removed = reads.lock().unwrap().remove(&4).unwrap();
        drop(removed);
        register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            4,
            GetRelayRegistration {
                expected_peer: "same-owner-id".to_string(),
                expected_tx: new_owner.clone(),
                expected_key: "new".to_string(),
                origin_tx: new_origin,
                origin_req_id: 105,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        drop(stale_guard);
        assert_eq!(reads.lock().unwrap().get(&4).unwrap().origin_req_id, 105);
        new_owner.cancel();
        // Exercise timer-owned disconnect cleanup, even if the reader cannot
        // reach its own tail. No explicit map cleanup for this final channel.
        wait_for_relay_admission(&admission, 8, 4096).await;
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_responses_ignore_same_peer_on_a_different_physical_channel() {
        let (ctx, tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        let (mut owner, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        let physical_owner = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(tx) = ctx.peers.lock().await.get("owner").cloned() {
                    break tx;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (different_physical_owner, _other_rx, _other_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(4);
        register_get_relay(
            &ctx.tasks,
            ctx.pending_relays.clone(),
            &ctx.relay_admission,
            11,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: different_physical_owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 101,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        register_mutation_relay(
            &ctx.tasks,
            ctx.pending_mutation_relays.clone(),
            &ctx.relay_admission,
            12,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: different_physical_owner.clone(),
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 102,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: ctx.operation_gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap()
        .disarm();
        let read_response = Envelope {
            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id: 11,
                found: false,
                value: vec![],
                timestamp_ms: 0,
                logical_counter: 0,
                node_id: String::new(),
                expires_at_ms: 0,
                is_tombstone: false,
                shed: false,
            })),
        };
        let mutation_response = Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: 12,
                results: vec![result("key", 0)],
            })),
        };
        for barrier in [201, 202] {
            write_test_envelope(&mut owner, read_response.clone()).await;
            write_test_envelope(&mut owner, mutation_response.clone()).await;
            write_test_envelope(
                &mut owner,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: barrier,
                        keys: vec![],
                        priority: 0,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut owner).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == barrier));
            if barrier == 201 {
                assert!(ctx.pending_relays.lock().unwrap().contains_key(&11));
                assert!(ctx
                    .pending_mutation_relays
                    .lock()
                    .unwrap()
                    .contains_key(&12));
                assert!(origin_responses.try_recv().is_err());
                ctx.pending_relays
                    .lock()
                    .unwrap()
                    .get_mut(&11)
                    .unwrap()
                    .expected_tx = physical_owner.clone();
                ctx.pending_mutation_relays
                    .lock()
                    .unwrap()
                    .get_mut(&12)
                    .unwrap()
                    .expected_tx = physical_owner.clone();
            }
        }
        assert!(
            matches!(Envelope::decode(origin_responses.try_recv().unwrap().bytes()).unwrap().msg,
            Some(EnvelopeMsg::GetResponse(response)) if response.req_id == 101)
        );
        assert!(
            matches!(Envelope::decode(origin_responses.try_recv().unwrap().bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 102)
        );
        wait_for_relay_admission(
            &ctx.relay_admission,
            RELAY_CAPACITY,
            RELAY_KEY_BYTE_CAPACITY,
        )
        .await;
        drop(owner);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .unwrap()
            .unwrap();
        tasks.shutdown().await;
    }

    #[test]
    fn relay_key_admission_charges_empty_keys_and_unused_storage_and_rolls_back() {
        let mut keys = Vec::with_capacity(8);
        keys.push(String::with_capacity(64));
        keys.push(String::new());
        let bytes = relay_keys_storage_bytes(&keys, keys.capacity());
        assert_eq!(
            bytes,
            std::mem::size_of::<Vec<String>>()
                + keys.capacity() * std::mem::size_of::<String>()
                + 64
        );
        assert_eq!(
            relay_key_storage_bytes(64),
            std::mem::size_of::<String>() + 64
        );
        let admission = RelayAdmission::new(2, bytes);
        assert!(admission.try_reserve(bytes + 1).is_none());
        assert_eq!(
            admission.slots.available_permits(),
            2,
            "failed byte admission leaked a count slot"
        );
        let lease = admission.try_reserve(bytes).unwrap();
        assert!(admission.try_reserve(1).is_none());
        assert_eq!(admission.slots.available_permits(), 1);
        let timer_lease = lease.clone();
        drop(lease);
        assert_eq!(
            admission.key_bytes.available_permits(),
            0,
            "timer lifetime must retain admission"
        );
        drop(timer_lease);
        assert_eq!(admission.slots.available_permits(), 2);
        assert_eq!(admission.key_bytes.available_permits(), bytes);
    }

    #[tokio::test]
    async fn pre_canceled_owner_rejects_relay_registration_without_waiting() {
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let (owner, _owner_rx, _owner_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(1);
        let gate = OperationGate::new();
        // No cancellation receivers exist yet: terminal cancellation must be
        // sticky for a relay which starts after the old reader has exited.
        owner.cancel();
        assert!(*owner.cancel.subscribe().borrow());
        assert!(register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                deadline: Instant::now() + Duration::from_secs(10),
            }
        )
        .await
        .is_none());
        assert!(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            }
        )
        .await
        .is_none());
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_responses.try_recv().unwrap();
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 12 && response.results[0].status == MutationStatus::Unavailable as i32));
        assert_eq!(admission.slots.available_permits(), 2);
        assert_eq!(admission.key_bytes.available_permits(), 1024);
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_relay_registration_before_timer_ack_reclaims_both_types() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let gate = OperationGate::new();
        // Current-thread Tokio cannot run its manager between this one poll
        // and Drop: both registry insertion and queued timer registration have
        // happened, but the timer acknowledgment has not.
        let mut read = Box::pin(register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        ));
        assert!(futures::poll!(read.as_mut()).is_pending());
        assert_eq!(reads.lock().unwrap().len(), 1);
        drop(read);
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(
            admission.slots.available_permits(),
            1,
            "queued timer lost its admission lease"
        );
        let mut mutation = Box::pin(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            },
        ));
        assert!(futures::poll!(mutation.as_mut()).is_pending());
        assert_eq!(mutations.lock().unwrap().len(), 1);
        drop(mutation);
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_response_rx
            .try_recv()
            .expect("canceled mutation registration must reply unavailable");
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 12 && response.results[0].status == MutationStatus::Unavailable as i32));
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        wait_for_relay_admission(&admission, 2, 1024).await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_enqueue_cancellation_and_registration_failure_release_admission() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(1, 1024);
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_response_rx) = test_peer_channel(4);
        let gate = OperationGate::new();
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec![],
                priority: 0,
            })),
        };
        assert!(
            try_send_envelope_until(
                &owner,
                envelope.clone(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
        );
        let registration = register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            1,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_keys: vec!["key".to_string()],
                origin_tx: origin.clone(),
                origin_req_id: 11,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap();
        let mut enqueue = Box::pin(async move {
            assert!(
                registration
                    .enqueue(&owner, envelope, Instant::now() + Duration::from_secs(10))
                    .await
            );
            registration.disarm();
        });
        assert!(futures::poll!(enqueue.as_mut()).is_pending());
        drop(enqueue);
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_response_rx.try_recv().unwrap();
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 11 && response.results[0].status == MutationStatus::Unavailable as i32));
        wait_for_relay_admission(&admission, 1, 1024).await;
        // A stopped task owner rejects registration; no map/timer/operation
        // may remain and the downstream frame must never be enqueued.
        tasks.shutdown().await;
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        assert!(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            }
        )
        .await
        .is_none());
        assert!(mutations.lock().unwrap().is_empty());
        assert!(origin_response_rx.try_recv().is_ok());
        wait_for_relay_admission(&admission, 1, 1024).await;
        gate.begin_shutdown();
        gate.wait_for_drain().await;
    }

    #[tokio::test]
    async fn batch_relay_preserves_duplicate_positions_stamps_and_terminal_capacity() {
        let (mut ctx, tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::from([(
                FragmentId(0),
                vec!["owner".to_string()],
            )]))),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        ctx.relay_admission = RelayAdmission::new(1, 1024);
        ctx.runtime_options = ctx
            .runtime_options
            .with_forwarded_attempt_timeout(Duration::from_secs(10));
        let (mut owner, owner_server) = tokio::io::duplex(4096);
        let owner_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(owner_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        // This response proves the owner handshake published its exact channel
        // before the origin requests a relay through it.
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 300,
                    ..Default::default()
                })),
            },
        )
        .await;
        assert!(matches!(read_test_envelope(&mut owner).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == 300));
        let (mut origin, origin_server) = tokio::io::duplex(4096);
        let origin_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(origin_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut origin, "origin").await;
        let request = BatchPut {
            req_id: 400,
            priority: ProtoPriority::High as i32,
            puts: ["duplicate", "duplicate", "other"]
                .into_iter()
                .enumerate()
                .map(|(index, key)| ForwardedPut {
                    key: key.into(),
                    value: if index == 2 {
                        Vec::new()
                    } else {
                        vec![index as u8]
                    },
                    timestamp_ms: index as u64 + 1,
                    logical_counter: 0,
                    node_id: format!("origin-{index}"),
                    seq: index as u64 + 11,
                    boot_id: index as u64 + 21,
                    expires_at_ms: index as u64 * 10,
                    is_tombstone: index == 2,
                    priority: ProtoPriority::Background as i32,
                    hops: index as u32,
                    req_id: index as u64 + 500,

                    write_concern: None,
                })
                .collect(),
        };
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(request.clone())),
            },
        )
        .await;
        let Some(EnvelopeMsg::BatchPut(forwarded)) = read_test_envelope(&mut owner).await.msg
        else {
            panic!("owner did not receive the forwarded batch");
        };
        assert_ne!(forwarded.req_id, request.req_id);
        let mut expected = request.clone();
        expected.req_id = forwarded.req_id;
        for put in &mut expected.puts {
            put.hops += 1;
        }
        assert_eq!(
            forwarded, expected,
            "relay changed operation stamps or positions"
        );
        assert_eq!(ctx.pending_mutation_relays.lock().unwrap().len(), 1);
        assert_eq!(ctx.relay_admission.slots.available_permits(), 0);
        assert!(ctx.relay_admission.key_bytes.available_permits() < 1024);
        ctx.operation_gate.begin_shutdown();
        let mut drain = Box::pin(ctx.operation_gate.wait_for_drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());

        let mut superseded = result("duplicate", 1);
        superseded.status = MutationStatus::Superseded as i32;
        superseded.winner_timestamp_ms = 4;
        superseded.winner_node_id = "winner".into();
        superseded.winner_is_tombstone = true;
        let mut unavailable = result("other", 2);
        unavailable.status = MutationStatus::Unavailable as i32;
        unavailable.error = "downstream unavailable".into();
        let results = vec![result("duplicate", 0), superseded, unavailable];
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                    req_id: forwarded.req_id,
                    results: results.clone(),
                })),
            },
        )
        .await;
        assert_eq!(
            read_test_envelope(&mut origin).await.msg,
            Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: request.req_id,
                results,
            })),
            "relay changed duplicate positions or downstream statuses",
        );
        wait_for_relay_admission(&ctx.relay_admission, 1, 1024).await;
        assert!(ctx.pending_mutation_relays.lock().unwrap().is_empty());
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("terminal batch response retained the operation fence");
        drop(origin);
        drop(owner);
        for connection in [origin_connection, owner_connection] {
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .unwrap()
                .unwrap();
        }
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn silent_owner_ingress_is_bounded_by_shared_relay_count_and_key_bytes() {
        for bound_by_bytes in [false, true] {
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (owner, mut owner_rx, _owner_response_rx) = test_peer_channel(8);
            peers.lock().await.insert("owner".to_string(), owner);
            let (mut ctx, tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::from([(
                    FragmentId(0),
                    vec!["owner".to_string()],
                )]))),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                peers,
            )
            .await;
            let slots = if bound_by_bytes { 8 } else { 2 };
            let bytes = if bound_by_bytes {
                relay_key_storage_bytes(3) + relay_keys_storage_bytes(&["key".to_string()], 1)
            } else {
                1024
            };
            ctx.relay_admission = RelayAdmission::new(slots, bytes);
            ctx.runtime_options = ctx
                .runtime_options
                .with_forwarded_attempt_timeout(Duration::from_millis(500));
            let admission = ctx.relay_admission.clone();
            let reads = ctx.pending_relays.clone();
            let mutations = ctx.pending_mutation_relays.clone();
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "origin").await;
            let read = |req_id| Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: "key".to_string(),
                    req_id,
                    priority: ProtoPriority::High as i32,
                    hops: 0,
                })),
            };
            let put = |req_id| Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                    key: "key".to_string(),
                    value: b"value".to_vec(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: "origin".to_string(),
                    seq: req_id,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: ProtoPriority::Default as i32,
                    hops: 0,
                    req_id,

                    write_concern: None,
                })),
            };
            write_test_envelope(&mut client, read(10)).await;
            write_test_envelope(&mut client, put(11)).await;
            let forwarded_read = tokio::time::timeout(Duration::from_secs(1), owner_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(Envelope::decode(forwarded_read.bytes()).unwrap().msg,
                Some(EnvelopeMsg::ForwardedGet(g)) if g.req_id != 10 && g.priority == ProtoPriority::High as i32 && g.hops == 1)
            );
            let forwarded_put = tokio::time::timeout(Duration::from_secs(1), owner_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(Envelope::decode(forwarded_put.bytes()).unwrap().msg,
                Some(EnvelopeMsg::ForwardedPut(p)) if p.req_id != 11 && p.timestamp_ms == 1 && p.seq == 11 && p.hops == 1)
            );
            for req_id in 20..40 {
                write_test_envelope(&mut client, read(req_id)).await;
                write_test_envelope(&mut client, put(req_id + 100)).await;
                assert!(matches!(read_test_envelope(&mut client).await.msg,
                    Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == req_id + 100 && response.results[0].status == MutationStatus::Unavailable as i32));
                assert_eq!(reads.lock().unwrap().len(), 1);
                assert_eq!(mutations.lock().unwrap().len(), 1);
            }
            assert!(
                owner_rx.try_recv().is_err(),
                "rejected relays reached the owner"
            );
            let batch_put = ForwardedPut {
                key: "key".to_string(),
                value: b"value".to_vec(),
                timestamp_ms: 2,
                logical_counter: 0,
                node_id: "origin".to_string(),
                seq: 400,
                boot_id: 1,
                req_id: 400,
                ..Default::default()
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                        req_id: 400,
                        puts: vec![batch_put.clone(), batch_put],
                        priority: ProtoPriority::High as i32,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 400 && response.results.len() == 2
                    && response.results.iter().enumerate().all(|(index, result)| result.key == "key" && result.request_index as usize == index && result.status == MutationStatus::Unavailable as i32)));
            // Barrier proves the saturated peer reader remains usable and no
            // unavailable legacy read was converted into a false miss.
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: 300,
                        keys: vec![],
                        priority: 0,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == 300 && response.results.is_empty()));
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 11 && response.results[0].status == MutationStatus::Unavailable as i32));
            wait_for_relay_admission(&admission, slots, bytes).await;
            assert!(reads.lock().unwrap().is_empty());
            assert!(mutations.lock().unwrap().is_empty());
            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .unwrap()
                .unwrap();
            tasks.shutdown().await;
        }
    }

    #[tokio::test]
    async fn stale_peer_disconnect_does_not_clear_replacement_sender_channel() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
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
        // Measure from before admission, not a possibly delayed receipt of
        // SessionStart after the sender has already begun its retry floor.
        let reconcile_started_at = Instant::now();
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

        let early_deadline = reconcile_started_at + Duration::from_millis(800);
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
        assert!(reconcile_started_at.elapsed() >= Duration::from_millis(900));
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
