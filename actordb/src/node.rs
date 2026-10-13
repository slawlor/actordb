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

use crate::actor::{now_ms, KvActor, KvArgs, KvMessage, ReplicatedOp, VersionedValue};
use crate::network::{BoxStream, Network, TcpNetwork};
use crate::proto::{
    Envelope, EnvelopeMsg, ForwardedGet, ForwardedPut, GetResponse, Hello,
    ReplicateOp as ProtoReplicateOp, SyncEntry, SyncResponse,
};
use crate::shard::{fragment_of, FragmentId};
use crate::store::{KvStore, PrefixedStore};

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

/// Everything a connection task needs. Cloned per connection.
#[derive(Clone)]
struct ConnContext {
    node_id: String,
    router: FragmentRouter,
    fragment_map: HashMap<FragmentId, Vec<String>>,
    pending_gets: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<VersionedValue>>>>>,
    peers: PeerMap,
    seen_ops: Arc<Mutex<HashSet<(String, u64, u64)>>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

/// Routes keys to the fragment actors this node serves. Cloned into
/// every connection task so inbound ops land on the right fragment.
/// Returns `None` for fragments this node doesn't serve.
#[derive(Clone)]
struct FragmentRouter {
    actors: HashMap<FragmentId, ActorRef<KvMessage>>,
    fragment_count: usize,
}

impl FragmentRouter {
    fn route(&self, key: &str) -> Option<&ActorRef<KvMessage>> {
        let frag = fragment_of(key, self.fragment_count);
        self.actors.get(&frag)
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
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Io(e) => write!(f, "io error: {e}"),
            NodeError::Codec(e) => write!(f, "codec error: {e}"),
            NodeError::Actor(e) => write!(f, "actor error: {e}"),
            NodeError::Network(e) => write!(f, "network error: {e}"),
        }
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
    /// Static placement (see `NodeConfig::fragment_map`).
    fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Static membership (see `NodeConfig::members`).
    members: HashMap<String, SocketAddr>,
    /// In-flight forwarded reads: req_id -> reply channel.
    pending_gets: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<VersionedValue>>>>>,
    next_req_id: AtomicU64,
    peers: PeerMap,
    /// (origin node_id, boot_id, seq) of every op already applied, to
    /// stop re-broadcast storms. The actor's LWW merge is idempotent,
    /// so this is purely a propagation concern.
    seen_ops: Arc<Mutex<HashSet<(String, u64, u64)>>>,
    /// Per-node sequence number for locally originated ops.
    next_seq: Arc<Mutex<u64>>,
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
    /// With the `rocksdb` feature, [`NodeConfig::with_rocksdb`] builds a
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

        // One actor per served fragment. With persistence, each fragment
        // gets its own key namespace via PrefixedStore, sharing the one
        // underlying store.
        let mut actors = HashMap::with_capacity(my_fragments.len());
        for fragment in my_fragments {
            let store = config
                .store
                .clone()
                .map(|inner| Arc::new(PrefixedStore::new(inner, fragment)) as Arc<dyn KvStore>);
            let (actor, _actor_handle) = KvActor::spawn(
                None,
                KvActor,
                KvArgs {
                    store,
                    fragment,
                    flush_batch_size: config.flush_batch_size,
                    flush_interval: config.flush_interval,
                },
            )
            .await
            .map_err(|e| NodeError::Actor(e.to_string()))?;
            actors.insert(fragment, actor);
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
            fragment_map: config.fragment_map.clone(),
            members: config.members.clone(),
            pending_gets: Arc::new(Mutex::new(HashMap::new())),
            next_req_id: AtomicU64::new(1),
            peers: Arc::new(Mutex::new(HashMap::new())),
            seen_ops: Arc::new(Mutex::new(HashSet::new())),
            next_seq: Arc::new(Mutex::new(0)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            network: config.network.clone(),
        };

        // Accept loop: every inbound connection gets a handler task.
        let ctx = ConnContext {
            node_id: node.node_id.clone(),
            router: node.router.clone(),
            fragment_map: node.fragment_map.clone(),
            pending_gets: node.pending_gets.clone(),
            peers: node.peers.clone(),
            seen_ops: node.seen_ops.clone(),
            tasks: node.tasks.clone(),
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
            fragment_map: self.fragment_map.clone(),
            pending_gets: self.pending_gets.clone(),
            peers: self.peers.clone(),
            seen_ops: self.seen_ops.clone(),
            tasks: self.tasks.clone(),
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
        if self.fragment_map.is_empty() {
            return None; // no map: everything is local
        }
        let replicas = self.fragment_map.get(&frag)?;
        // Prefer ourselves; otherwise the first listed replica.
        // (Load-aware selection is a later optimization.)
        if replicas.iter().any(|id| id == &self.node_id) {
            Some(self.node_id.clone())
        } else {
            replicas.first().cloned()
        }
    }

    /// Outbound channel to `node_id`, if connected.
    async fn peer_sender(&self, node_id: &str) -> Option<mpsc::Sender<Vec<u8>>> {
        self.peers.lock().await.get(node_id).cloned()
    }

    /// Fan out an op to the other replica holders of `frag`.
    /// Fire-and-forget: a dead peer is skipped (its data is recovered
    /// via sync when it returns).
    async fn replicate(&self, frag: FragmentId, op: &ReplicatedOp, exclude: Option<&str>) {
        replicate_to_holders(
            &self.node_id,
            &self.fragment_map,
            &self.peers,
            frag,
            op,
            exclude,
        )
        .await;
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
        let seq = {
            let mut guard = self.next_seq.lock().await;
            *guard += 1;
            *guard
        };
        let op = ReplicatedOp {
            key: key.clone(),
            value: VersionedValue::new(value, now_ms(), self.node_id.clone()),
            seq,
            boot_id: self.boot_id,
        };
        // Mark locally originated ops as seen so inbound echoes terminate.
        self.seen_ops
            .lock()
            .await
            .insert((self.node_id.clone(), self.boot_id, seq));

        let frag = fragment_of(&key, self.router.fragment_count);
        if let Some(actor) = self.fragment_actor(&op.key) {
            actor
                .send_message(KvMessage::ApplyOp(op.clone()))
                .map_err(|e| NodeError::Actor(e.to_string()))?;

            // Barrier: the actor drains its mailbox in FIFO order, so this
            // `call` returns only after the ApplyOp above has been applied.
            // (A `get` on the same key doubles as the read-your-write check.)
            match actor
                .call(|reply| KvMessage::Get { key, reply }, None)
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?
            {
                CallResult::Success(_) => {}
                CallResult::Timeout => return Err(NodeError::Actor("put timed out".to_string())),
                CallResult::SenderError => {
                    return Err(NodeError::Actor("put reply channel dropped".to_string()))
                }
            }

            self.replicate(frag, &op, None).await;
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
        if let Some(actor) = self.fragment_actor(&key) {
            let res = actor
                .call(|reply| KvMessage::Get { key, reply }, None)
                .await
                .map_err(|e| NodeError::Actor(e.to_string()))?;
            return match res {
                CallResult::Success(value) => Ok(value),
                CallResult::Timeout => Err(NodeError::Actor("get timed out".to_string())),
                CallResult::SenderError => {
                    Err(NodeError::Actor("get reply channel dropped".to_string()))
                }
            };
        }

        // Remote fragment: forward the read, await the reply.
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
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet { key, req_id })),
            },
        )
        .await;
        // Bounded wait: a dead owner must not hang the caller forever.
        match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(Ok(value)) => Ok(value),
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
        self.router.get_all().await
    }

    /// This node's id.
    pub fn id(&self) -> &str {
        &self.node_id
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
        // The handlers never block, so this returns promptly. Failures
        // just mean an actor was already dead.
        for actor in self.router.actors.values() {
            let _ = actor.stop_and_wait(None, None).await;
        }
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
        fragment_map,
        pending_gets,
        peers,
        seen_ops,
        tasks,
    } = ctx;
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
                peers.lock().await.insert(peer, tx.clone());
                if !we_dialed {
                    // The listener answers the handshake...
                    send_hello(&tx, &node_id).await;
                }
                // ...and both sides then pull the fragments they serve
                // for anti-entropy.
                send_sync_request(&tx, router.fragment_ids()).await;
            }
            Some(EnvelopeMsg::Replicate(op)) => {
                let id = (op.node_id.clone(), op.boot_id, op.seq);
                let is_new = seen_ops.lock().await.insert(id);
                if !is_new {
                    continue; // already applied
                }
                let replicated = ReplicatedOp {
                    key: op.key.clone(),
                    value: VersionedValue::new(
                        op.value.clone(),
                        op.timestamp_ms,
                        op.node_id.clone(),
                    ),
                    seq: op.seq,
                    boot_id: op.boot_id,
                };
                // Apply the op. In legacy mode (no fragment map) we also
                // re-gossip it, so multi-hop topologies converge. In
                // sharded mode the coordinator fans out to all holders
                // directly — never re-forward here. Ops for fragments we
                // don't serve are dropped (misrouted or stale placement).
                if let Some(actor) = router.route(&replicated.key) {
                    if actor.send_message(KvMessage::ApplyOp(replicated)).is_ok()
                        && fragment_map.is_empty()
                    {
                        rebroadcast(&peers, &tx, &op).await;
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedPut(p)) => {
                // We own this fragment: apply as coordinator, then fan
                // out to the other replica holders (excluding the sender).
                let replicated = ReplicatedOp {
                    key: p.key.clone(),
                    value: VersionedValue::new(p.value.clone(), p.timestamp_ms, p.node_id.clone()),
                    seq: p.seq,
                    boot_id: p.boot_id,
                };
                let frag = fragment_of(&replicated.key, router.fragment_count);
                if let Some(actor) = router.route(&replicated.key) {
                    let _ = actor.send_message(KvMessage::ApplyOp(replicated.clone()));
                    seen_ops
                        .lock()
                        .await
                        .insert((p.node_id.clone(), p.boot_id, p.seq));
                    replicate_to_holders(
                        &node_id,
                        &fragment_map,
                        &peers,
                        frag,
                        &replicated,
                        peer_node_id.as_deref(),
                    )
                    .await;
                }
            }
            Some(EnvelopeMsg::ForwardedGet(g)) => {
                // We own this fragment: read it and reply on this
                // connection.
                let value = match router.route(&g.key) {
                    Some(actor) => {
                        match actor
                            .call(
                                |reply| KvMessage::Get {
                                    key: g.key.clone(),
                                    reply,
                                },
                                None,
                            )
                            .await
                        {
                            Ok(CallResult::Success(v)) => v,
                            _ => None,
                        }
                    }
                    None => None,
                };
                let (found, bytes, timestamp_ms, origin) = match value {
                    Some(v) => (true, v.value, v.timestamp_ms, v.node_id),
                    None => (false, Vec::new(), 0, String::new()),
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
                        })),
                    },
                )
                .await;
            }
            Some(EnvelopeMsg::GetResponse(r)) => {
                if let Some(resp_tx) = pending_gets.lock().await.remove(&r.req_id) {
                    let value = r.found.then(|| {
                        VersionedValue::new(r.value.clone(), r.timestamp_ms, r.node_id.clone())
                    });
                    let _ = resp_tx.send(value);
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
                    let op = ReplicatedOp {
                        key: e.key,
                        value: VersionedValue::new(e.value, e.timestamp_ms, e.node_id),
                        seq: 0,     // seq/boot_id are propagation concerns;
                        boot_id: 0, // sync is not gossip
                    };
                    if let Some(actor) = router.route(&op.key) {
                        let _ = actor.send_message(KvMessage::ApplyOp(op));
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
async fn rebroadcast(peers: &PeerMap, arrived_on: &mpsc::Sender<Vec<u8>>, op: &ProtoReplicateOp) {
    let mut frame = Vec::new();
    if (Envelope {
        msg: Some(EnvelopeMsg::Replicate(op.clone())),
    })
    .encode(&mut frame)
    .is_err()
    {
        return;
    }
    let peers = peers.lock().await;
    for tx in peers.values() {
        if !tx.same_channel(arrived_on) {
            let _ = tx.send(frame.clone()).await;
        }
    }
}

/// Fan out an op to the other replica holders of `frag`, excluding
/// `exclude` (typically the node the op arrived from). Fire-and-forget.
async fn replicate_to_holders(
    node_id: &str,
    fragment_map: &HashMap<FragmentId, Vec<String>>,
    peers: &PeerMap,
    frag: FragmentId,
    op: &ReplicatedOp,
    exclude: Option<&str>,
) {
    let targets: Vec<String> = match fragment_map.get(&frag) {
        Some(replicas) => replicas
            .iter()
            .filter(|r| r.as_str() != node_id && Some(r.as_str()) != exclude)
            .cloned()
            .collect(),
        // No map: legacy — replicate to every connected peer.
        None => peers
            .lock()
            .await
            .keys()
            .filter(|k| Some(k.as_str()) != exclude)
            .cloned()
            .collect(),
    };
    if targets.is_empty() {
        return;
    }
    let proto_op = ProtoReplicateOp {
        key: op.key.clone(),
        value: op.value.value.clone(),
        timestamp_ms: op.value.timestamp_ms,
        node_id: op.value.node_id.clone(),
        seq: op.seq,
        boot_id: op.boot_id,
    };
    let peers = peers.lock().await;
    for target in targets {
        if let Some(tx) = peers.get(&target) {
            send_envelope(
                tx,
                Envelope {
                    msg: Some(EnvelopeMsg::Replicate(proto_op.clone())),
                },
            )
            .await;
        }
    }
}

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
