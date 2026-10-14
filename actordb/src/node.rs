// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! A database node.
//!
//! Each node runs one [crate::actor::KvActor] holding the node's
//! *local copy* of the keyspace, plus a Tokio TCP mesh to its peers.
//! Writes are disseminated with **epidemic broadcast**: a local write is
//! applied to the local actor and gossiped to every peer; each peer
//! applies it and re-gossips it once, tracked by
//! `(origin node_id, boot_id, seq)`
//! so the flood terminates. Inbound writes merge into the local copy
//! with last-writer-wins, which is idempotent and commutative — so all
//! replicas converge without any coordination, even across restarts and
//! late joiners (a joining node pulls full state with a sync exchange
//! during the handshake).
//!
//! `boot_id` is minted fresh at every process start: a restarted node
//! reuses its `node_id` but its sequence counter starts back at zero,
//! so without it peers would mistake the new boot's first writes for
//! replays of pre-restart ops and silently drop them.
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
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
use ractor::{Actor, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::AbortHandle;

use crate::actor::{HlcClock, KvMessage, OpPriority, PoolConfig, ReplicatedOp, VersionedValue};
use crate::network::{BoxStream, Network, TcpNetwork};
use crate::proto::{
    BatchAck, BatchGet, BatchGetResponse, BatchKeyValue, BatchPut, Envelope, EnvelopeMsg,
    ForwardedGet, ForwardedPut, GetResponse, Hello, KeySummary, Priority as ProtoPriority,
    ReconcileData, ReconcileResponse, SyncEntry, SyncResponse,
};
use crate::replication::{spawn_sender, SenderConfig, SenderDeps, SenderMsg, SenderStateReport};
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

/// peer node_id -> outbound frame queue
type PeerMap = Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>;
/// Phase 5 senders, keyed by (fragment, peer node id).
type SenderMap = Arc<Mutex<HashMap<(FragmentId, String), mpsc::UnboundedSender<SenderMsg>>>>;
/// Sender state reports, keyed by (fragment, peer node id).
type SenderReportMap = Arc<Mutex<HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>>>;
/// Senders currently in BufferReset, keyed by (fragment, peer).
type ResetSenderSet = Arc<Mutex<HashSet<(FragmentId, String)>>>;

/// Pending single-key forwarded reads: req_id -> reply channel. The
/// reply is `Ok(value)` or `Err(shed_reason)` if the remote shed the
/// read due to backpressure.
type PendingGets =
    Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Option<VersionedValue>, String>>>>>;
/// Pending batched forwarded reads: req_id -> reply channel.
type PendingBatchGets = Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<BatchKeyValue>>>>>;

/// Unique worker-name prefixes across supervisors in this process.
static NEXT_WORKER_PREFIX: AtomicU64 = AtomicU64::new(0);

/// Everything a connection task needs. Cloned per connection.
#[derive(Clone)]
struct ConnContext {
    node_id: String,
    router: FragmentRouter,
    pending_gets: PendingGets,
    pending_batch_gets: PendingBatchGets,
    peers: PeerMap,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
    hlc: Arc<Mutex<HlcClock>>,
    senders: SenderMap,
    sender_config: SenderConfig,
    reset_senders: ResetSenderSet,
    sender_reports: SenderReportMap,
}

/// Routes keys to the fragment actors this node serves. Cloned into
/// every connection task so inbound ops land on the right fragment.
/// Returns `None` for fragments this node doesn't serve.
#[derive(Clone)]
pub struct FragmentRouter {
    actors: HashMap<FragmentId, ActorRef<KvMessage>>,
    fragment_count: usize,
}

impl FragmentRouter {
    fn route(&self, key: &str) -> Option<&ActorRef<KvMessage>> {
        let frag = fragment_of(key, self.fragment_count);
        self.actors.get(&frag)
    }

    /// The actor serving `fragment`, if this node serves it.
    pub fn route_for_fragment(&self, fragment: FragmentId) -> Option<&ActorRef<KvMessage>> {
        self.actors.get(&fragment)
    }

    fn fragment_ids(&self) -> Vec<u32> {
        self.actors.keys().map(|f| f.0).collect()
    }

    async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let mut all = HashMap::new();
        for actor in self.actors.values() {
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
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Io(e) => write!(f, "io error: {e}"),
            NodeError::Codec(e) => write!(f, "codec error: {e}"),
            NodeError::Actor(e) => write!(f, "actor error: {e}"),
            NodeError::Network(e) => write!(f, "network error: {e}"),
            NodeError::Backpressure(e) => write!(f, "backpressure shed: {e}"),
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
    /// Random identifier minted at process start; joins the gossip
    /// dedup key so a restarted node is not mistaken for a replay.
    boot_id: u64,
    listen_addr: SocketAddr,
    /// Routes keys to their fragment actors.
    router: FragmentRouter,
    /// Per-fragment supervisors (own the dispatcher, cleaner,
    /// snapshotter). Stopped on shutdown.
    supervisors: HashMap<FragmentId, ActorRef<SupervisorMessage>>,
    /// Static placement (see `NodeConfig::fragment_map`).
    fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Static membership (see `NodeConfig::members`).
    members: HashMap<String, SocketAddr>,
    /// In-flight forwarded reads: req_id -> reply channel.
    pending_gets: PendingGets,
    /// In-flight batched forwarded reads: req_id -> reply channel.
    pending_batch_gets: PendingBatchGets,
    next_req_id: AtomicU64,
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
    /// How long tombstones are kept before the cleaner garbage-collects
    /// them. Must exceed the longest expected partition: a node
    /// rejoining after its tombstones were GC'd can resurrect deleted
    /// keys. Default 1 hour.
    pub tombstone_gc_age: Duration,
    /// Wall-clock offset (ms) applied to this node's HLC. Zero in
    /// production; tests use it to simulate clock skew. Hidden from
    /// docs: not a tuning knob.
    #[doc(hidden)]
    pub clock_skew_ms: i64,
    /// Phase 5 async replication tuning (batching, acks, buffers).
    pub replication: SenderConfig,
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
            tombstone_gc_age: Duration::from_secs(3600),
            clock_skew_ms: 0,
            replication: SenderConfig::default(),
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
        if config.fragment_count == 0 {
            return Err(NodeError::Actor(
                "fragment_count must be non-zero".to_string(),
            ));
        }
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

        // One actor per fragment. With persistence, each fragment gets
        // its own key namespace via PrefixedStore, sharing the one
        // underlying store.
        // Fragments this node serves: from the map, or all of them when
        // no map is configured (single-node / legacy).
        let my_fragments: Vec<FragmentId> = if config.fragment_map.is_empty() {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .collect()
        } else {
            (0..config.fragment_count)
                .map(|i| FragmentId(i as u32))
                .filter(|f| {
                    config
                        .fragment_map
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
        let reset_senders: Arc<Mutex<HashSet<(FragmentId, String)>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let mut supervisors = HashMap::with_capacity(my_fragments.len());
        let mut actors = HashMap::with_capacity(my_fragments.len());
        for fragment in my_fragments {
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
            supervisors.insert(fragment, supervisor);
            actors.insert(fragment, dispatcher);
        }

        let (bound_addr, mut acceptor) = config
            .network
            .bind(listen_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;

        let router = FragmentRouter {
            actors,
            fragment_count: config.fragment_count,
        };
        let node = Self {
            node_id: node_id.clone(),
            boot_id: fresh_boot_id(),
            listen_addr: bound_addr,
            router: router.clone(),
            supervisors,
            fragment_map: config.fragment_map.clone(),
            members: config.members.clone(),
            pending_gets: Arc::new(Mutex::new(HashMap::new())),
            pending_batch_gets: Arc::new(Mutex::new(HashMap::new())),
            next_req_id: AtomicU64::new(1),
            peers: Arc::new(Mutex::new(HashMap::new())),
            next_seq: Arc::new(Mutex::new(0)),
            hlc: Arc::new(Mutex::new(HlcClock::with_offset(config.clock_skew_ms))),
            senders: Arc::new(Mutex::new(HashMap::new())),
            reset_senders: reset_senders.clone(),
            sender_config: config.replication.clone(),
            sender_reports: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
            network: config.network.clone(),
        };

        // Phase 5: spawn a sender per (fragment, static replica peer).
        // Legacy mode (no fragment map) spawns senders lazily on connect.
        for frag in router.fragment_ids() {
            let frag_id = FragmentId(frag);
            if let Some(replicas) = config.fragment_map.get(&frag_id) {
                for peer in replicas {
                    if peer != &node_id {
                        let report = Arc::new(Mutex::new(SenderStateReport {
                            state: "disconnected".to_string(),
                            session_id: 0,
                            buffered: 0,
                            in_flight: 0,
                        }));
                        let tx = spawn_sender(
                            frag_id,
                            peer.clone(),
                            config.replication.clone(),
                            SenderDeps {
                                router: router.clone(),
                                reset_senders: reset_senders.clone(),
                                state_report: report.clone(),
                            },
                        );
                        node.senders
                            .lock()
                            .await
                            .insert((frag_id, peer.clone()), tx);
                        node.sender_reports
                            .lock()
                            .await
                            .insert((frag_id, peer.clone()), report);
                    }
                }
            }
        }

        // Accept loop: every inbound connection gets a handler task.
        let ctx = ConnContext {
            node_id: node.node_id.clone(),
            router: node.router.clone(),
            pending_gets: node.pending_gets.clone(),
            pending_batch_gets: node.pending_batch_gets.clone(),
            peers: node.peers.clone(),
            tasks: node.tasks.clone(),
            hlc: node.hlc.clone(),
            senders: node.senders.clone(),
            sender_config: node.sender_config.clone(),
            reset_senders: node.reset_senders.clone(),
            sender_reports: node.sender_reports.clone(),
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
            pending_gets: self.pending_gets.clone(),
            pending_batch_gets: self.pending_batch_gets.clone(),
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            hlc: self.hlc.clone(),
            senders: self.senders.clone(),
            sender_config: self.sender_config.clone(),
            reset_senders: self.reset_senders.clone(),
            sender_reports: self.sender_reports.clone(),
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

    /// The fragment actor serving `key` (`None` if remote).
    fn fragment_actor(&self, key: &str) -> Option<&ActorRef<KvMessage>> {
        self.router.route(key)
    }

    /// Pick a replica holder for `frag`, preferring this node. Returns
    /// `None` if the fragment has no usable replica (misconfiguration).
    fn pick_replica(&self, frag: FragmentId) -> Option<String> {
        self.replica_order(frag).into_iter().next()
    }

    /// Replica holders for `frag` in preference order: this node first
    /// (if it holds the fragment), then the others in map order. Empty
    /// when the fragment has no usable replica (misconfiguration), or
    /// when there is no placement map (everything is local).
    fn replica_order(&self, frag: FragmentId) -> Vec<String> {
        if self.fragment_map.is_empty() {
            return Vec::new(); // no map: everything is local
        }
        let Some(replicas) = self.fragment_map.get(&frag) else {
            return Vec::new();
        };
        // Prefer ourselves; otherwise map order.
        // (Load-aware selection is a later optimization.)
        let mut order: Vec<String> = replicas
            .iter()
            .filter(|id| *id != &self.node_id)
            .cloned()
            .collect();
        if replicas.iter().any(|id| id == &self.node_id) {
            order.insert(0, self.node_id.clone());
        }
        order
    }

    /// Outbound channel to `node_id`, if connected.
    async fn peer_sender(&self, node_id: &str) -> Option<mpsc::Sender<Vec<u8>>> {
        self.peers.lock().await.get(node_id).cloned()
    }

    /// Fan out an op to the other replica holders of `frag`.
    /// Fire-and-forget: a dead peer is skipped (its data is recovered
    /// via sync when it returns).
    /// Phase 5: enqueue `op` for async replication to every sender
    /// serving `frag`. Returns immediately; the senders batch, ship,
    /// and ack off the write path.
    async fn enqueue_for_replication(&self, frag: FragmentId, op: &ReplicatedOp) {
        // Ensure senders exist for all current replication targets.
        // (Static peers got senders at startup; legacy-mode peers get
        // them on connect.)
        let senders = self.senders.lock().await;
        for ((f, _peer), tx) in senders.iter() {
            if *f == frag {
                // Unbounded channel: the sender's internal buffer is the
                // bound (overflow → BufferReset). A dead sender is dropped
                // lazily.
                let _ = tx.send(SenderMsg::Enqueue(op.clone()));
            }
        }
    }

    /// Write a key locally and gossip it to all peers.
    ///
    /// This is read-your-write: when `put` returns, the op has been
    /// applied to the local copy (and, with persistence enabled,
    /// written to disk). The gossip to peers is fire-and-forget.
    /// Write a key. The op is routed in one hop to a node serving the
    /// key's fragment: applied locally if we serve it, otherwise
    /// forwarded to a replica holder.
    ///
    /// Local puts are read-your-write: when `put` returns, the op has
    /// been applied to the local copy. Forwarded puts are fire-and-forget
    /// to the owner (at-least-once; the owner replicates to the other
    /// holders).
    pub async fn put(&self, key: String, value: Vec<u8>) -> Result<(), NodeError> {
        self.put_with_ttl(key, value, None).await
    }

    /// Write with explicit priority (Phase 7). High priority is for
    /// latency-sensitive ops; background is for bulk (rate-limited).
    /// The priority is honored end-to-end: forwarded puts carry it on
    /// the wire and the owner applies them at the same priority.
    ///
    /// Read-your-write holds within a priority: the barrier read runs
    /// in the write's pool, so a `get_with_priority` at the *same*
    /// priority sees this write. A cross-priority read (e.g. background
    /// write, default-priority get) is NOT ordered behind the write —
    /// the pools are independent and make no cross-pool ordering
    /// promise.
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
    /// reaps it on its next sweep.
    ///
    /// Local puts are read-your-write: when `put_with_ttl` returns, the
    /// op has been applied to the local copy. Forwarded puts are
    /// fire-and-forget to the owner (at-least-once; the owner replicates
    /// to the other holders).
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

    /// Apply a locally-originated op: mark seen, route to the fragment
    /// actor, barrier on read-your-write, then replicate.
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
            actor
                .send_message(KvMessage::ApplyOp {
                    op: op.clone(),
                    priority,
                })
                .map_err(|e| NodeError::Actor(e.to_string()))?;

            // Barrier: the actor drains its mailbox in FIFO order, so this
            // `call` returns only after the ApplyOp above has been applied.
            // (A `get` on the same key doubles as the read-your-write check.)
            // For tombstones the get returns None — the barrier still holds.
            // The barrier read uses the write's priority: it must land in
            // the same pool, or a cross-pool read would not be ordered
            // behind the write.
            match actor
                .call(
                    |reply| KvMessage::Get {
                        key: op.key.clone(),
                        reply,
                        priority,
                    },
                    None,
                )
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?
            {
                CallResult::Success(Ok(_)) => {}
                CallResult::Success(Err(shed)) => {
                    return Err(NodeError::Backpressure(format!("put barrier shed: {shed}")))
                }
                CallResult::Timeout => return Err(NodeError::Actor("put timed out".to_string())),
                CallResult::SenderError => {
                    return Err(NodeError::Actor("put reply channel dropped".to_string()))
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
        send_envelope(
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
                })),
            },
        )
        .await;
        Ok(())
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
    /// Read-your-write pairs with [`put_with_priority`](Self::put_with_priority)
    /// at the same priority. A high-priority read is not ordered behind
    /// a background write to the same key.
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

    /// Read with explicit priority (Phase 7).
    /// Read the full local copy across all fragments (debugging / tests).
    pub async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let all = self.router.get_all().await?;
        // Hide tombstones from user reads (sync needs them, users don't).
        Ok(all.into_iter().filter(|(_, v)| !v.is_tombstone).collect())
    }

    /// Put multiple keys. Each key is hashed to its fragment; keys are
    /// grouped by destination node and each remote node gets one
    /// `BatchPut` message. Returns per-key results in input order.
    ///
    /// Local keys are read-your-write (like `put`); remote keys are
    /// fire-and-forget to the owner (like `put`). A remote key fails if
    /// its owner is not connected.
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
    /// Local keys are read-your-write (like `put`); remote keys are
    /// fire-and-forget to the owner (like `put`): a remote key fails
    /// only when no replica holder is connected
    /// (`NodeError::Network`), or the fragment has no usable replica
    /// (`NodeError::Actor`, misconfiguration).
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

        // Local puts: run concurrently, each with its own barrier, at
        // the requested priority.
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
                });
                indices.push(i);
            }
            let mut sent = false;
            let mut tried: Vec<String> = Vec::new();
            for owner in self.replica_order(frag) {
                tried.push(owner.clone());
                let Some(tx) = self.peer_sender(&owner).await else {
                    continue;
                };
                send_envelope(
                    &tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                            puts: puts.clone(),
                            priority: wire_priority,
                        })),
                    },
                )
                .await;
                for i in &indices {
                    results[*i] = Some(Ok(()));
                }
                sent = true;
                break;
            }
            if !sent {
                for i in indices {
                    results[i] = Some(Err(NodeError::Network(format!(
                        "no connected replica for fragment {frag} (tried {})",
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
    /// explicit priority. Local keys are read-your-write (like `put`);
    /// remote keys are fire-and-forget to the owner (like `put`).
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

    /// Stop all networking tasks. The actor is left running; dropping the
    /// process ends it.
    /// Shut the node down: abort all connection tasks and stop the
    /// actor. Stopping the actor drops its state, which releases the
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
        for supervisor in self.supervisors.values() {
            let _ = supervisor.stop_and_wait(None, None).await;
        }
    }

    /// Health snapshots for every fragment this node serves.
    pub async fn health(&self) -> Result<HashMap<FragmentId, FragmentHealth>, NodeError> {
        let mut out = HashMap::with_capacity(self.supervisors.len());
        for (frag, supervisor) in &self.supervisors {
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
        pending_gets,
        pending_batch_gets,
        peers,
        tasks,
        hlc,
        senders,
        sender_config,
        reset_senders,
        sender_reports,
    } = ctx;
    // Phase 5: current session per (peer, fragment) on this connection.
    // Sessions are per-connection: a new connection means a new pairing.
    let sessions: Arc<Mutex<HashMap<(String, FragmentId), u64>>> =
        Arc::new(Mutex::new(HashMap::new()));
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
        send_hello(&tx, &node_id).await;
    }

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

        match envelope.msg {
            Some(EnvelopeMsg::Hello(Hello { node_id: peer })) => {
                peer_node_id = Some(peer.clone());
                peers.lock().await.insert(peer.clone(), tx.clone());
                if !we_dialed {
                    // The listener answers the handshake...
                    send_hello(&tx, &node_id).await;
                }
                // ...and both sides then pull the fragments they serve
                // for anti-entropy.
                send_sync_request(&tx, router.fragment_ids()).await;
                // Phase 5: (re)connect the senders for this peer. Legacy
                // mode (no fragment map) spawns senders lazily here.
                {
                    let mut senders_guard = senders.lock().await;
                    for frag_id in router.fragment_ids() {
                        let frag = FragmentId(frag_id);
                        let key = (frag, peer.clone());
                        if !senders_guard.contains_key(&key) {
                            let report = Arc::new(Mutex::new(SenderStateReport {
                                state: "disconnected".to_string(),
                                session_id: 0,
                                buffered: 0,
                                in_flight: 0,
                            }));
                            let sender_tx = spawn_sender(
                                frag,
                                peer.clone(),
                                sender_config.clone(),
                                SenderDeps {
                                    router: router.clone(),
                                    reset_senders: reset_senders.clone(),
                                    state_report: report.clone(),
                                },
                            );
                            sender_reports.lock().await.insert(key.clone(), report);
                            senders_guard.insert(key.clone(), sender_tx);
                        }
                    }
                    for ((frag, p), sender_tx) in senders_guard.iter() {
                        if p == &peer {
                            let _ = sender_tx.send(SenderMsg::PeerConnected {
                                peer_tx: tx.clone(),
                            });
                            let _ = frag; // silence unused
                        }
                    }
                }
            }
            Some(EnvelopeMsg::SessionStart(s)) => {
                // New sender↔receiver pairing: adopt the session id.
                // Batches from older sessions are discarded.
                if let Some(peer) = peer_node_id.clone() {
                    sessions
                        .lock()
                        .await
                        .insert((peer, FragmentId(s.fragment)), s.session_id);
                }
            }
            Some(EnvelopeMsg::ReplicateBatch(b)) => {
                // Phase 5: validate the session, apply via LWW (idempotent),
                // and ack. Stale sessions are discarded.
                let peer = match peer_node_id.clone() {
                    Some(p) => p,
                    None => continue,
                };
                let frag = FragmentId(b.fragment);
                let current = sessions.lock().await.get(&(peer.clone(), frag)).copied();
                if current != Some(b.session_id) {
                    continue; // stale session
                }
                for op in b.ops {
                    hlc.lock().await.observe(op.timestamp_ms);
                    let replicated = ReplicatedOp {
                        key: op.key.clone(),
                        value: {
                            let mut v = VersionedValue::new(
                                op.value.clone(),
                                op.timestamp_ms,
                                op.node_id.clone(),
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
                    // u64::MAX marks reconciliation batches (not ack-tracked);
                    // they still apply via LWW.
                    if let Some(actor) = router.route(&replicated.key) {
                        let _ = actor.send_message(KvMessage::ApplyOp {
                            op: replicated,
                            priority: OpPriority::Default,
                        });
                    }
                }
                // Ack unless this was a reconciliation batch.
                if b.batch_seq != u64::MAX {
                    send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                                session_id: b.session_id,
                                batch_seq: b.batch_seq,
                                fragment: b.fragment,
                            })),
                        },
                    )
                    .await;
                }
            }
            Some(EnvelopeMsg::BatchAck(a)) => {
                // Route the ack to the sender for (fragment, peer).
                if let Some(peer) = peer_node_id.clone() {
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(a.fragment), peer)) {
                        let _ = tx.send(SenderMsg::Ack {
                            session_id: a.session_id,
                            batch_seq: a.batch_seq,
                        });
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileRequest(r)) => {
                // Peer wants our (timestamp, tombstone) summaries.
                let frag = FragmentId(r.fragment);
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
                                })),
                            },
                        )
                        .await;
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileFetch(f)) => {
                // Peer wants full values for these keys.
                let frag = FragmentId(f.fragment);
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
                                })),
                            },
                        )
                        .await;
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileResponse(r)) => {
                // Route to the sender for (fragment, peer).
                if let Some(peer) = peer_node_id.clone() {
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(r.fragment), peer)) {
                        let _ = tx.send(SenderMsg::ReconcileResponse {
                            summaries: r.summaries,
                        });
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileData(d)) => {
                // Route to the sender for (fragment, peer).
                if let Some(peer) = peer_node_id.clone() {
                    let senders = senders.lock().await;
                    if let Some(tx) = senders.get(&(FragmentId(d.fragment), peer)) {
                        let _ = tx.send(SenderMsg::ReconcileData { entries: d.entries });
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedPut(p)) => {
                hlc.lock().await.observe(p.timestamp_ms);
                // We own this fragment: apply as coordinator, then
                // enqueue for async replication to the other holders.
                let replicated = ReplicatedOp {
                    key: p.key.clone(),
                    value: {
                        let mut v =
                            VersionedValue::new(p.value.clone(), p.timestamp_ms, p.node_id.clone());
                        v.expires_at_ms = if p.expires_at_ms == 0 {
                            None
                        } else {
                            Some(p.expires_at_ms)
                        };
                        v.is_tombstone = p.is_tombstone;
                        v
                    },
                    seq: p.seq,
                    boot_id: p.boot_id,
                };
                let frag = fragment_of(&replicated.key, router.fragment_count);
                if let Some(actor) = router.route(&replicated.key) {
                    let _ = actor.send_message(KvMessage::ApplyOp {
                        op: replicated.clone(),
                        priority: priority_from_proto(p.priority),
                    });
                    // Phase 5: async fan-out via senders.
                    let senders = senders.lock().await;
                    for ((f, _peer), tx) in senders.iter() {
                        if *f == frag {
                            let _ = tx.send(SenderMsg::Enqueue(replicated.clone()));
                        }
                    }
                }
            }
            Some(EnvelopeMsg::BatchPut(bp)) => {
                // Phase 6: apply each put as coordinator, enqueue for
                // async replication. Phase 7: the batch carries one
                // priority applied to every put.
                let batch_priority = priority_from_proto(bp.priority);
                hlc.lock()
                    .await
                    .observe(bp.puts.iter().map(|p| p.timestamp_ms).max().unwrap_or(0));
                for p in bp.puts {
                    let replicated = ReplicatedOp {
                        key: p.key.clone(),
                        value: {
                            let mut v = VersionedValue::new(
                                p.value.clone(),
                                p.timestamp_ms,
                                p.node_id.clone(),
                            );
                            v.expires_at_ms = if p.expires_at_ms == 0 {
                                None
                            } else {
                                Some(p.expires_at_ms)
                            };
                            v.is_tombstone = p.is_tombstone;
                            v
                        },
                        seq: p.seq,
                        boot_id: p.boot_id,
                    };
                    let frag = fragment_of(&replicated.key, router.fragment_count);
                    if let Some(actor) = router.route(&replicated.key) {
                        let _ = actor.send_message(KvMessage::ApplyOp {
                            op: replicated.clone(),
                            priority: batch_priority,
                        });
                        let senders = senders.lock().await;
                        for ((f, _peer), tx) in senders.iter() {
                            if *f == frag {
                                let _ = tx.send(SenderMsg::Enqueue(replicated.clone()));
                            }
                        }
                    }
                }
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
                if let Some(resp_tx) = pending_batch_gets.lock().await.remove(&r.req_id) {
                    let _ = resp_tx.send(r.results);
                }
            }
            Some(EnvelopeMsg::ForwardedGet(g)) => {
                // We own this fragment: read it and reply on this
                // connection, at the originator's priority.
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
            Some(EnvelopeMsg::SyncRequest(req)) => {
                // Send only the requested fragments we serve. An empty
                // request means "everything you hold".
                let wanted: Vec<FragmentId> = if req.fragments.is_empty() {
                    router.actors.keys().copied().collect()
                } else {
                    req.fragments
                        .iter()
                        .map(|f| FragmentId(*f))
                        .filter(|f| router.actors.contains_key(f))
                        .collect()
                };
                let mut all = HashMap::new();
                for frag in wanted {
                    if let Some(actor) = router.actors.get(&frag) {
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
                for e in resp.entries {
                    hlc.lock().await.observe(e.timestamp_ms);
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
                        let _ = sender_tx.send(SenderMsg::PeerDisconnected);
                    }
                }
            }
        }
    }
    writer_task.abort();
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
async fn send_hello(tx: &mpsc::Sender<Vec<u8>>, node_id: &str) {
    send_envelope(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: node_id.to_string(),
            })),
        },
    )
    .await;
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
    let mut frame = Vec::new();
    if envelope.encode(&mut frame).is_ok() {
        let _ = tx.send(frame).await;
    }
}
