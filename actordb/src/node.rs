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
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as ProstMessage;
use ractor::rpc::CallResult;
use ractor::{Actor, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tokio::task::AbortHandle;

use crate::actor::{now_ms, KvActor, KvArgs, KvMessage, ReplicatedOp, VersionedValue};
use crate::network::{BoxStream, Network, TcpNetwork};
use crate::proto::{
    Envelope, EnvelopeMsg, Hello, ReplicateOp as ProtoReplicateOp, SyncEntry, SyncResponse,
};
use crate::store::{KvStore, MemoryStore};

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
    actor: ActorRef<KvMessage>,
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
    /// Durable storage for the node's local copy. Defaults to a no-op
    /// in-memory store; see [`KvStore`]. With the `rocksdb` feature,
    /// use [`NodeConfig::with_rocksdb`] for RocksDB persistence.
    pub store: Arc<dyn KvStore>,
    /// Network transport for the peer mesh. Defaults to plain TCP;
    /// see [`Network`].
    pub network: Arc<dyn Network>,
    /// Flush the store write buffer after this many buffered ops.
    /// Ignored by the in-memory store.
    pub flush_batch_size: usize,
    /// Flush the store write buffer after this long, even if the batch
    /// isn't full (checked on each applied write). Ignored by the
    /// in-memory store.
    ///
    /// Crash window: a `kill -9` can lose the unflushed tail, which is
    /// then recovered via peer sync on reconnect. Clean shutdowns
    /// always flush.
    pub flush_interval: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            store: Arc::new(MemoryStore),
            network: Arc::new(TcpNetwork),
            flush_batch_size: 1000,
            flush_interval: Duration::from_millis(100),
        }
    }
}

#[cfg(feature = "rocksdb")]
impl NodeConfig {
    /// Convenience: a config persisting to RocksDB at `path`.
    pub fn with_rocksdb(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::store::StoreError> {
        Ok(Self {
            store: Arc::new(crate::store::RocksDbStore::open(path)?),
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
        let (actor, _actor_handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                store: config.store,
                flush_batch_size: config.flush_batch_size,
                flush_interval: config.flush_interval,
            },
        )
        .await
        .map_err(|e| NodeError::Actor(e.to_string()))?;

        let (bound_addr, mut acceptor) = config
            .network
            .bind(listen_addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;

        let node = Self {
            node_id: node_id.clone(),
            boot_id: fresh_boot_id(),
            listen_addr: bound_addr,
            actor: actor.clone(),
            peers: Arc::new(Mutex::new(HashMap::new())),
            seen_ops: Arc::new(Mutex::new(HashSet::new())),
            next_seq: Arc::new(Mutex::new(0)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            network: config.network.clone(),
        };

        // Accept loop: every inbound connection gets a handler task.
        let (nid, act, peers, seen, tasks) = (
            node.node_id.clone(),
            node.actor.clone(),
            node.peers.clone(),
            node.seen_ops.clone(),
            node.tasks.clone(),
        );
        let accept_task = tokio::spawn(async move {
            while let Ok(stream) = acceptor.accept().await {
                let h = tokio::spawn(handle_connection(
                    nid.clone(),
                    act.clone(),
                    peers.clone(),
                    seen.clone(),
                    tasks.clone(),
                    stream,
                    /* we_dialed = */ false,
                ));
                tasks.lock().await.push(h.abort_handle());
            }
        });
        node.tasks.lock().await.push(accept_task.abort_handle());

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
        let (node_id, actor, peers, seen_ops, tasks, network) = (
            self.node_id.clone(),
            self.actor.clone(),
            self.peers.clone(),
            self.seen_ops.clone(),
            self.tasks.clone(),
            self.network.clone(),
        );
        let h = tokio::spawn(async move {
            // Run the initial connection to completion...
            handle_connection(
                node_id.clone(),
                actor.clone(),
                peers.clone(),
                seen_ops.clone(),
                tasks.clone(),
                stream,
                /* we_dialed = */ true,
            )
            .await;
            // ...then redial forever. Only the dialer side redials; the
            // listener side just waits for the next inbound connection.
            // This task is aborted by shutdown() with all the rest.
            reconnect_loop(node_id, actor, peers, seen_ops, tasks, network, peer_addr).await;
        });
        self.tasks.lock().await.push(h.abort_handle());
        Ok(())
    }

    /// Write a key locally and gossip it to all peers.
    ///
    /// This is read-your-write: when `put` returns, the op has been
    /// applied to the local copy (and, with persistence enabled,
    /// written to disk). The gossip to peers is fire-and-forget.
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

        self.actor
            .send_message(KvMessage::ApplyOp(op.clone()))
            .map_err(|e| NodeError::Actor(e.to_string()))?;

        // Barrier: the actor drains its mailbox in FIFO order, so this
        // `call` returns only after the ApplyOp above has been applied.
        // (A `get` on the same key doubles as the read-your-write check.)
        match self
            .actor
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

        self.broadcast(op).await;
        Ok(())
    }

    /// Read a key from the local copy. This is a local read — it reflects
    /// whatever has converged here so far (eventual consistency).
    pub async fn get(&self, key: String) -> Result<Option<VersionedValue>, NodeError> {
        let res = self
            .actor
            .call(|reply| KvMessage::Get { key, reply }, None)
            .await
            .map_err(|e| NodeError::Actor(e.to_string()))?;
        match res {
            CallResult::Success(value) => Ok(value),
            CallResult::Timeout => Err(NodeError::Actor("get timed out".to_string())),
            CallResult::SenderError => {
                Err(NodeError::Actor("get reply channel dropped".to_string()))
            }
        }
    }

    /// Read the full local copy (debugging / tests).
    pub async fn get_all(&self) -> Result<HashMap<String, VersionedValue>, NodeError> {
        let res = self
            .actor
            .call(|reply| KvMessage::GetAll { reply }, None)
            .await
            .map_err(|e| NodeError::Actor(e.to_string()))?;
        match res {
            CallResult::Success(all) => Ok(all),
            CallResult::Timeout => Err(NodeError::Actor("get_all timed out".to_string())),
            CallResult::SenderError => Err(NodeError::Actor(
                "get_all reply channel dropped".to_string(),
            )),
        }
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
        // just mean the actor was already dead.
        let _ = self.actor.stop_and_wait(None, None).await;
    }

    /// Gossip an op to every connected peer. Fire-and-forget: a dead peer
    /// is dropped from the set.
    async fn broadcast(&self, op: ReplicatedOp) {
        let proto_op = ProtoReplicateOp {
            key: op.key,
            value: op.value.value,
            timestamp_ms: op.value.timestamp_ms,
            node_id: op.value.node_id,
            seq: op.seq,
            boot_id: op.boot_id,
        };
        let mut frame = Vec::new();
        // Encoding into a Vec cannot realistically fail; skip on error.
        if (Envelope {
            msg: Some(EnvelopeMsg::Replicate(proto_op)),
        })
        .encode(&mut frame)
        .is_err()
        {
            return;
        }

        let mut dead = Vec::new();
        {
            let peers = self.peers.lock().await;
            for (peer_id, tx) in peers.iter() {
                if tx.send(frame.clone()).await.is_err() {
                    dead.push(peer_id.clone());
                }
            }
        }
        if !dead.is_empty() {
            let mut peers = self.peers.lock().await;
            for peer_id in dead {
                peers.remove(&peer_id);
            }
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
async fn handle_connection(
    node_id: String,
    actor: ActorRef<KvMessage>,
    peers: PeerMap,
    seen_ops: Arc<Mutex<HashSet<(String, u64, u64)>>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
    stream: BoxStream,
    we_dialed: bool,
) {
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
                // ...and both sides then pull full state for anti-entropy.
                send_sync_request(&tx).await;
            }
            Some(EnvelopeMsg::Replicate(op)) => {
                let id = (op.node_id.clone(), op.boot_id, op.seq);
                let is_new = seen_ops.lock().await.insert(id);
                if !is_new {
                    continue; // already applied; kills re-broadcast storms
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
                if actor
                    .send_message(KvMessage::ApplyOp(replicated.clone()))
                    .is_ok()
                {
                    // Epidemic step: pass it on to our other peers.
                    rebroadcast(&peers, &tx, &op).await;
                }
            }
            Some(EnvelopeMsg::SyncRequest(_)) => {
                if let Ok(CallResult::Success(all)) =
                    actor.call(|reply| KvMessage::GetAll { reply }, None).await
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
                    let _ = actor.send_message(KvMessage::ApplyOp(op));
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
async fn reconnect_loop(
    node_id: String,
    actor: ActorRef<KvMessage>,
    peers: PeerMap,
    seen_ops: Arc<Mutex<HashSet<(String, u64, u64)>>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
    network: Arc<dyn Network>,
    addr: SocketAddr,
) {
    let mut backoff = RECONNECT_BASE;
    loop {
        tokio::time::sleep(backoff + jitter(backoff)).await;
        match network.connect(addr).await {
            Ok(stream) => {
                backoff = RECONNECT_BASE;
                handle_connection(
                    node_id.clone(),
                    actor.clone(),
                    peers.clone(),
                    seen_ops.clone(),
                    tasks.clone(),
                    stream,
                    /* we_dialed = */ true,
                )
                .await;
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

/// Re-gossip an op to all peers except the connection it arrived on.
/// (Sending it back too would be harmless — dedup terminates the flood —
/// but there's no point paying for the bytes.)
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

async fn send_sync_request(tx: &mpsc::Sender<Vec<u8>>) {
    send_envelope(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::SyncRequest(crate::proto::SyncRequest {})),
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
