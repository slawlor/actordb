#[path = "common/content_digest.rs"]
mod content_digest;
use content_digest::content_digest;

// Integration tests: multi-node replication over real TCP on localhost.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use actordb::actor::{now_ms, VersionedValue};
use actordb::network::{BindFuture, ConnectFuture, Network, NetworkError, TcpNetwork};
use actordb::node::{
    Node, NodeConfig, NodeError, NodeRuntimeOptions, ReplicationDrainOptions,
    ReplicationDrainPhase, ReplicationDrainShutdownError, REQUIRED_WIRE_CAPABILITIES,
    WIRE_PROTOCOL_VERSION,
};
use actordb::proto::{Envelope, EnvelopeMsg, Hello, HelloAuth};
use actordb::store::{
    KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, StoreCommitDurability, StoreError,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct CountingNetwork {
    connect_attempts: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct GatedFailNetwork {
    connect_attempts: Arc<AtomicUsize>,
    first_entered: Arc<tokio::sync::Notify>,
    release_first: Arc<tokio::sync::Notify>,
}

impl Network for CountingNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let tcp = TcpNetwork;
            tcp.bind(addr).await
        })
    }

    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        self.connect_attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let tcp = TcpNetwork;
            tcp.connect(addr).await
        })
    }
}

impl Network for GatedFailNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let tcp = TcpNetwork;
            tcp.bind(addr).await
        })
    }

    fn connect(&self, _addr: SocketAddr) -> ConnectFuture<'_> {
        let attempt = self.connect_attempts.fetch_add(1, Ordering::SeqCst);
        let first_entered = self.first_entered.clone();
        let release_first = self.release_first.clone();
        Box::pin(async move {
            if attempt == 0 {
                first_entered.notify_one();
                release_first.notified().await;
            }
            Err(NetworkError("injected dial failure".to_string()))
        })
    }
}

#[derive(Default)]
struct PreloadedStore {
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl PreloadedStore {
    fn insert_fragment(&self, fragment: u32, key: &str, value: VersionedValue) {
        let mut prefixed = fragment.to_be_bytes().to_vec();
        prefixed.extend_from_slice(key.as_bytes());
        self.entries.lock().unwrap().insert(prefixed, value);
    }

    fn insert_fragment_zero(&self, key: &str, value: VersionedValue) {
        self.insert_fragment(0, key, value);
    }

    fn get_fragment_zero(&self, key: &str) -> Option<VersionedValue> {
        let mut prefixed = 0u32.to_be_bytes().to_vec();
        prefixed.extend_from_slice(key.as_bytes());
        self.entries.lock().unwrap().get(&prefixed).cloned()
    }
}

impl KvStore for PreloadedStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

#[derive(Default)]
struct AlwaysFailStore;

impl KvStore for AlwaysFailStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Err(StoreError("injected permanent failure".to_string()))
    }
}

#[derive(Default)]
struct FailOnceStore {
    attempts: AtomicUsize,
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl KvStore for FailOnceStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StoreError("injected transient failure".to_string()));
        }
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

struct ToggleFailStore {
    failing: AtomicBool,
    attempts: AtomicUsize,
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

#[derive(Default)]
struct RetirementStore {
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    delete_attempts: AtomicUsize,
    fail_delete: AtomicBool,
}

impl RetirementStore {
    fn fragment_is_empty(&self, fragment: u32) -> bool {
        let prefix = fragment.to_be_bytes();
        self.entries
            .lock()
            .unwrap()
            .keys()
            .all(|key| !key.starts_with(&prefix))
    }
}

impl KvStore for RetirementStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Stable
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Stable)
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Supported
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        self.delete_attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(StoreError("injected prefix deletion failure".to_string()));
        }
        self.entries
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with(prefix));
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

impl Default for ToggleFailStore {
    fn default() -> Self {
        Self {
            failing: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
            entries: StdMutex::new(HashMap::new()),
        }
    }
}

impl KvStore for ToggleFailStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            return Err(StoreError("injected switchable failure".to_string()));
        }
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

struct SelectiveFailStore {
    failing_fragment: u32,
    failing: AtomicBool,
    attempts: [AtomicUsize; 2],
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl SelectiveFailStore {
    fn new(failing_fragment: u32) -> Self {
        Self {
            failing_fragment,
            failing: AtomicBool::new(true),
            attempts: [AtomicUsize::new(0), AtomicUsize::new(0)],
            entries: StdMutex::new(HashMap::new()),
        }
    }

    fn attempts_for(&self, fragment: u32) -> usize {
        self.attempts[fragment as usize].load(Ordering::SeqCst)
    }
}

impl KvStore for SelectiveFailStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let fragment = writes
            .first()
            .and_then(|(key, _)| key.get(..4))
            .and_then(|prefix| prefix.try_into().ok())
            .map(u32::from_be_bytes)
            .expect("prefixed store supplied an empty or malformed batch");
        assert!(
            writes
                .iter()
                .all(|(key, _)| key.starts_with(&fragment.to_be_bytes())),
            "a prefixed-store batch crossed fragment boundaries"
        );
        self.attempts[fragment as usize].fetch_add(1, Ordering::SeqCst);
        if fragment == self.failing_fragment && self.failing.load(Ordering::SeqCst) {
            return Err(StoreError("injected fragment-local failure".to_string()));
        }
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

#[derive(Default)]
struct GatedStore {
    entered: AtomicBool,
    release: (StdMutex<bool>, Condvar),
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl GatedStore {
    fn release_on_drop(self: &Arc<Self>) -> GatedStoreReleaseGuard {
        GatedStoreReleaseGuard(self.clone())
    }

    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}

// A failed assertion or cancelled test future must unblock synchronous store
// calls before Tokio tears down its runtime. Aborting the actor task cannot
// interrupt a thread already waiting on this Condvar.
struct GatedStoreReleaseGuard(Arc<GatedStore>);

impl Drop for GatedStoreReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl KvStore for GatedStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.entered.store(true, Ordering::SeqCst);
        let mut released = self.release.0.lock().unwrap();
        while !*released {
            released = self.release.1.wait(released).unwrap();
        }
        drop(released);
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

#[test]
fn gated_store_release_guard_unblocks_a_write_on_unwind() {
    let store = Arc::new(GatedStore::default());
    let value = VersionedValue::new(b"retained".to_vec(), 1, "test".to_string());
    let writer_store = store.clone();
    let writer_value = value.clone();
    let (done, completion) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result = writer_store.write_batch(&[(b"key".to_vec(), writer_value)]);
        done.send(result).unwrap();
    });

    let injected_failure = AtomicBool::new(false);
    let unwind = std::panic::catch_unwind(|| {
        let _release_on_drop = store.release_on_drop();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !store.entered.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "writer did not enter gate"
            );
            std::thread::yield_now();
        }
        assert!(completion.try_recv().is_err(), "write bypassed the gate");
        injected_failure.store(true, Ordering::SeqCst);
        panic!("injected failure while the store write is blocked");
    });
    assert!(unwind.is_err());
    assert!(injected_failure.load(Ordering::SeqCst));
    completion
        .recv_timeout(Duration::from_secs(2))
        .expect("unwind did not release the blocked store write")
        .unwrap();
    writer.join().unwrap();
    assert_eq!(store.load().unwrap(), vec![(b"key".to_vec(), value)]);
}

async fn test_node(id: &str) -> Node {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(id.to_string(), addr, NodeConfig::default())
        .await
        .expect("node failed to start")
}

async fn test_node_with_config(id: &str, config: NodeConfig) -> Node {
    Node::start(id.to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("node failed to start")
}

/// Wait until the handshake has completed on both sides.
async fn wait_for_peers(a: &Node, b: &Node) {
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
}

async fn send_frame(stream: &mut tokio::net::TcpStream, envelope: Envelope) {
    let mut encoded = Vec::new();
    envelope.encode(&mut encoded).unwrap();
    stream.write_u32(encoded.len() as u32).await.unwrap();
    stream.write_all(&encoded).await.unwrap();
    stream.flush().await.unwrap();
}

async fn recv_frame(stream: &mut tokio::net::TcpStream) -> Envelope {
    tokio::time::timeout(Duration::from_secs(5), async {
        let len = stream.read_u32().await.unwrap() as usize;
        let mut encoded = vec![0; len];
        stream.read_exact(&mut encoded).await.unwrap();
        Envelope::decode(encoded.as_slice()).unwrap()
    })
    .await
    .expect("timed out waiting for protocol frame")
}

async fn recv_batch_put(stream: &mut tokio::net::TcpStream) -> actordb::proto::BatchPut {
    loop {
        if let Some(EnvelopeMsg::BatchPut(batch)) = recv_frame(stream).await.msg {
            return batch;
        }
    }
}

fn batch_put_response(
    batch: &actordb::proto::BatchPut,
    statuses: &[actordb::proto::MutationStatus],
) -> Envelope {
    use actordb::proto::{MutationResponse, MutationResult, MutationStatus};

    assert_eq!(batch.puts.len(), statuses.len());
    let results = batch
        .puts
        .iter()
        .zip(statuses)
        .enumerate()
        .map(|(request_index, (put, status))| {
            let mut result = MutationResult {
                key: put.key.clone(),
                status: *status as i32,
                error: String::new(),
                winner_timestamp_ms: 0,
                winner_logical_counter: 0,
                winner_node_id: String::new(),
                winner_is_tombstone: false,
                request_index: request_index as u32,

                write_concern: None,
            };
            match status {
                MutationStatus::Superseded => {
                    result.winner_timestamp_ms = put.timestamp_ms + 1;
                    result.winner_node_id = "newer-writer".to_string();
                    result.winner_is_tombstone = true;
                }
                MutationStatus::Backpressure
                | MutationStatus::StorageError
                | MutationStatus::ActorError
                | MutationStatus::Unavailable => {
                    result.error = format!("injected {status:?}");
                }
                _ => {}
            }
            result
        })
        .collect();
    Envelope {
        msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
            req_id: batch.req_id,
            results,
        })),
    }
}

fn complete_reconcile_response(
    request: &actordb::proto::ReconcileRequest,
    summaries: Vec<actordb::proto::KeySummary>,
) -> actordb::proto::ReconcileResponse {
    let next_after_key = summaries
        .last()
        .map(|summary| summary.key.clone())
        .unwrap_or_else(|| request.after_key.clone());
    actordb::proto::ReconcileResponse {
        fragment: request.fragment,
        summaries,
        session_id: request.session_id,
        round_id: request.round_id,
        page_seq: request.page_seq,
        next_after_key,
        snapshot_revision: if request.snapshot_revision == 0 {
            1
        } else {
            request.snapshot_revision
        },
        snapshot_time_ms: if request.snapshot_time_ms == 0 {
            now_ms().max(1)
        } else {
            request.snapshot_time_ms
        },
        done: true,
        restart: false,
    }
}

fn complete_reconcile_data(
    fetch: &actordb::proto::ReconcileFetch,
    entries: Vec<actordb::proto::SyncEntry>,
) -> actordb::proto::ReconcileData {
    actordb::proto::ReconcileData {
        fragment: fetch.fragment,
        entries,
        session_id: fetch.session_id,
        round_id: fetch.round_id,
        chunk_seq: fetch.chunk_seq,
        page_seq: 0,
        done: true,
    }
}

/// Receive every untracked repair before the following tracked barrier and
/// return the summaries an applying raw peer would advertise on verification.
async fn recv_reconcile_repairs_and_barrier(
    stream: &mut tokio::net::TcpStream,
) -> (
    Vec<actordb::proto::KeySummary>,
    actordb::proto::ReplicateBatch,
) {
    let mut summaries = Vec::new();
    loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(stream).await.msg {
            if batch.batch_seq == u64::MAX {
                summaries.extend(batch.ops.into_iter().map(|op| actordb::proto::KeySummary {
                    content_digest: content_digest(&op.value, op.expires_at_ms),
                    key: op.key,
                    timestamp_ms: op.timestamp_ms,
                    logical_counter: op.logical_counter,
                    node_id: op.node_id,
                    is_tombstone: op.is_tombstone,
                }));
            } else {
                summaries.sort_by(|left, right| left.key.cmp(&right.key));
                return (summaries, batch);
            }
        }
    }
}

/// Finish the empty protocol-v3 reconcile that static senders start as soon
/// as a raw peer connects. Tests that exercise later streaming/ACK behavior
/// must not leave this bootstrap round blocking the sender state machine.
async fn complete_empty_reconcile(stream: &mut tokio::net::TcpStream, session_id: u64) {
    loop {
        match recv_frame(stream).await.msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) if request.session_id == session_id => {
                send_frame(
                    stream,
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                            &request,
                            Vec::new(),
                        ))),
                    },
                )
                .await;
            }
            Some(EnvelopeMsg::ReplicateBatch(batch))
                if batch.session_id == session_id
                    && batch.batch_seq != u64::MAX
                    && batch.ops.is_empty() =>
            {
                send_frame(
                    stream,
                    Envelope {
                        msg: Some(EnvelopeMsg::BatchAck(actordb::proto::BatchAck {
                            session_id: batch.session_id,
                            batch_seq: batch.batch_seq,
                            fragment: batch.fragment,
                            level: batch.required_ack_level,
                        })),
                    },
                )
                .await;
                return;
            }
            _ => {}
        }
    }
}

async fn recv_session_start(stream: &mut tokio::net::TcpStream) -> actordb::proto::SessionStart {
    loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(stream).await.msg {
            return session;
        }
    }
}

async fn recv_reconcile_request(
    stream: &mut tokio::net::TcpStream,
    session_id: u64,
) -> actordb::proto::ReconcileRequest {
    loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(stream).await.msg {
            if request.session_id == session_id {
                return request;
            }
        }
    }
}

async fn send_reconcile_response(
    stream: &mut tokio::net::TcpStream,
    request: &actordb::proto::ReconcileRequest,
    summaries: Vec<actordb::proto::KeySummary>,
) {
    send_frame(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                request, summaries,
            ))),
        },
    )
    .await;
}

async fn ack_replication_batch(
    stream: &mut tokio::net::TcpStream,
    batch: &actordb::proto::ReplicateBatch,
    session_id: u64,
    level: actordb::proto::ReplicationAckLevel,
) {
    send_frame(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(actordb::proto::BatchAck {
                session_id,
                batch_seq: batch.batch_seq,
                fragment: batch.fragment,
                level: level as i32,
            })),
        },
    )
    .await;
}

async fn raw_replication_drain_source(
    source_id: &str,
    peer_id: &str,
    replication: actordb::replication::SenderConfig,
) -> (Arc<Node>, tokio::net::TcpStream, u64) {
    use actordb::shard::FragmentId;

    let source = Arc::new(
        test_node_with_config(
            source_id,
            NodeConfig {
                fragment_map: HashMap::from([(
                    FragmentId(0),
                    vec![source_id.to_string(), peer_id.to_string()],
                )]),
                replication,
                ..NodeConfig::default()
            },
        )
        .await,
    );
    let mut stream = tokio::net::TcpStream::connect(source.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, peer_id).await;
    let initial_session = recv_session_start(&mut stream).await;
    assert_eq!(initial_session.fragment, 0);
    complete_empty_reconcile(&mut stream, initial_session.session_id).await;
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), peer_id)
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1 && report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;
    (source, stream, initial_session.session_id)
}

/// Complete the protocol-v3 mutual handshake used by raw TCP peers in these
/// tests. With no cluster secret configured, the second flight is explicit
/// but carries an empty proof.
async fn complete_no_secret_handshake(stream: &mut tokio::net::TcpStream, peer_id: &str) {
    let server_hello = match recv_frame(stream).await.msg {
        Some(EnvelopeMsg::Hello(hello)) => hello,
        other => panic!("expected server Hello, got {other:?}"),
    };
    assert_eq!(server_hello.min_protocol_version, WIRE_PROTOCOL_VERSION);
    assert_eq!(server_hello.max_protocol_version, WIRE_PROTOCOL_VERSION);
    assert_eq!(
        server_hello.capabilities & REQUIRED_WIRE_CAPABILITIES,
        REQUIRED_WIRE_CAPABILITIES
    );
    assert_eq!(server_hello.challenge.len(), 32);

    send_frame(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: peer_id.to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x5a; 32],
            })),
        },
    )
    .await;

    match recv_frame(stream).await.msg {
        Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) => assert!(auth_tag.is_empty()),
        other => panic!("expected server HelloAuth, got {other:?}"),
    }
    send_frame(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth {
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
}

#[tokio::test]
async fn single_node_put_get() {
    let n = test_node("solo").await;

    n.put("k1".to_string(), b"v1".to_vec()).await.unwrap();

    let got = n
        .get("k1".to_string())
        .await
        .unwrap()
        .expect("key should exist");
    assert_eq!(got.value, b"v1");
    assert_eq!(got.node_id, "solo");
    assert!(n.get("missing".to_string()).await.unwrap().is_none());

    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_zero_sized_replication_and_placement_config_is_rejected() {
    use actordb::node::NodeError;
    use actordb::placement::ManualPlacement;
    use actordb::shard::FragmentId;

    let mut batch_zero = NodeConfig::default();
    batch_zero.replication.batch_size = 0;
    let mut in_flight_zero = NodeConfig::default();
    in_flight_zero.replication.max_in_flight = 0;
    let mut reconcile_zero = NodeConfig::default();
    reconcile_zero.replication.reconcile_timeout = Duration::ZERO;
    let empty_cluster_secret = NodeConfig {
        cluster_secret: Some(Vec::new()),
        ..NodeConfig::default()
    };
    let weak_cluster_secret = NodeConfig {
        cluster_secret: Some(vec![7; 31]),
        ..NodeConfig::default()
    };

    for (id, config) in [
        ("invalid-batch", batch_zero),
        ("invalid-in-flight", in_flight_zero),
        ("invalid-reconcile", reconcile_zero),
        ("invalid-empty-cluster-secret", empty_cluster_secret),
        ("invalid-weak-cluster-secret", weak_cluster_secret),
    ] {
        assert!(matches!(
            Node::start(id.to_string(), "127.0.0.1:0".parse().unwrap(), config).await,
            Err(NodeError::Actor(_))
        ));
    }

    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["invalid-placement".to_string()],
    )])));
    let placement_zero = NodeConfig {
        placement_controller: Some(controller),
        placement_poll_interval: Duration::ZERO,
        ..NodeConfig::default()
    };
    assert!(matches!(
        Node::start(
            "invalid-placement".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            placement_zero,
        )
        .await,
        Err(NodeError::Actor(_))
    ));

    #[cfg(target_pointer_width = "64")]
    {
        let too_many_fragments = NodeConfig {
            fragment_count: u32::MAX as usize + 1,
            ..NodeConfig::default()
        };
        assert!(matches!(
            Node::start(
                "invalid-fragment-count".to_string(),
                "127.0.0.1:0".parse().unwrap(),
                too_many_fragments,
            )
            .await,
            Err(NodeError::Actor(_))
        ));
    }

    // Runtime liveness policy is validated before the network bind. Holding
    // this address open distinguishes the intended Actor error from a later
    // bind failure.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied_addr = occupied.local_addr().unwrap();
    for invalid_id in [String::new(), "x".repeat(256)] {
        assert!(matches!(
            Node::start(invalid_id, occupied_addr, NodeConfig::default()).await,
            Err(NodeError::Actor(_))
        ));
    }
    for invalid_member in [String::new(), "x".repeat(256)] {
        assert!(matches!(
            Node::start(
                "invalid-member-id".to_string(),
                occupied_addr,
                NodeConfig {
                    members: HashMap::from([(invalid_member, "127.0.0.1:9".parse().unwrap())]),
                    ..NodeConfig::default()
                },
            )
            .await,
            Err(NodeError::Actor(_))
        ));
    }
    assert!(matches!(
        Node::start(
            "invalid-static-placement-id".to_string(),
            occupied_addr,
            NodeConfig {
                fragment_map: HashMap::from([(
                    FragmentId(0),
                    vec!["invalid-static-placement-id".to_string(), String::new()],
                )]),
                ..NodeConfig::default()
            },
        )
        .await,
        Err(NodeError::Actor(_))
    ));
    let invalid_runtime_options = [
        (
            "invalid-peer-queue-capacity",
            NodeRuntimeOptions::default().with_peer_queue_frame_capacity(0),
        ),
        (
            "invalid-replication-control-capacity",
            NodeRuntimeOptions::default().with_replication_control_message_capacity(0),
        ),
        (
            "invalid-inbound-connection-capacity",
            NodeRuntimeOptions::default().with_inbound_connection_capacity(0),
        ),
        (
            "invalid-queue-admission-timeout",
            NodeRuntimeOptions::default().with_queue_admission_timeout(Duration::ZERO),
        ),
        (
            "invalid-forwarded-attempt-timeout",
            NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::ZERO),
        ),
        (
            "invalid-connect-timeout",
            NodeRuntimeOptions::default().with_connect_timeout(Duration::ZERO),
        ),
        (
            "invalid-handshake-timeout",
            NodeRuntimeOptions::default().with_handshake_timeout(Duration::ZERO),
        ),
        (
            "invalid-socket-read-timeout",
            NodeRuntimeOptions::default().with_socket_read_timeout(Duration::ZERO),
        ),
        (
            "invalid-socket-write-timeout",
            NodeRuntimeOptions::default().with_socket_write_timeout(Duration::ZERO),
        ),
        (
            "invalid-replication-ack-timeout",
            NodeRuntimeOptions::default().with_replication_ack_timeout(Duration::ZERO),
        ),
    ];
    for (id, options) in invalid_runtime_options {
        assert!(matches!(
            Node::start_with_options(
                id.to_string(),
                occupied_addr,
                NodeConfig::default(),
                options,
            )
            .await,
            Err(NodeError::Actor(_))
        ));
    }
}

#[tokio::test]
async fn invalid_maintenance_intervals_are_rejected_before_binding() {
    use actordb::placement::ManualPlacement;
    use actordb::shard::FragmentId;

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap();
    for invalid in [Duration::ZERO, Duration::MAX] {
        for field in [
            "clean_interval",
            "snapshot_interval",
            "placement_poll_interval",
            "placement_drain_warn_interval",
        ] {
            let mut config = NodeConfig::default();
            match field {
                "clean_interval" => config.clean_interval = invalid,
                "snapshot_interval" => config.snapshot_interval = invalid,
                "placement_poll_interval" => config.placement_poll_interval = invalid,
                "placement_drain_warn_interval" => config.placement_drain_warn_interval = invalid,
                _ => unreachable!(),
            }
            if field.starts_with("placement_") {
                config.placement_controller = Some(Arc::new(ManualPlacement::new(HashMap::from(
                    [(FragmentId(0), vec!["invalid-maintenance".to_string()])],
                ))));
            }
            let result = Node::start("invalid-maintenance".to_string(), addr, config).await;
            assert!(
                matches!(result, Err(NodeError::Actor(ref message)) if message.contains(field)),
                "{field}={invalid:?} was not rejected before transport bind"
            );
        }
    }
}

#[tokio::test]
async fn static_placement_ignores_unused_placement_intervals() {
    let node = Node::start(
        "static-unused-intervals".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            placement_poll_interval: Duration::ZERO,
            placement_drain_warn_interval: Duration::MAX,
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    node.put("valid".into(), vec![1]).await.unwrap();
    assert_eq!(
        node.get("valid".into()).await.unwrap().unwrap().value,
        vec![1]
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn time_based_tombstone_gc_is_rejected_before_binding() {
    use actordb::node::NodeError;

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap();
    let config = NodeConfig {
        tombstone_gc_age: Some(Duration::from_secs(24 * 60 * 60)),
        ..NodeConfig::default()
    };

    assert!(matches!(
        Node::start("unsafe-gc".to_string(), addr, config).await,
        Err(NodeError::Actor(message))
            if message
                == "tombstone_gc_age is unsupported because time alone cannot prove replica convergence; it must be None"
    ));
}

#[tokio::test]
async fn legacy_tombstone_gc_api_remains_source_compatible_and_safe() {
    use actordb::actor::{FragmentState, HlcClock, PoolConfig};
    use actordb::shard::FragmentId;
    use actordb::supervision::{CleanerArgs, FragmentHealth, SupervisorArgs};
    use tokio::sync::Mutex;

    let reset_senders = Arc::new(Mutex::new(HashMap::new()));
    let shared = Arc::new(FragmentState::new(
        HashMap::from([(
            "deleted".to_string(),
            VersionedValue::tombstone(1, "node".to_string()),
        )]),
        None,
        100,
        Duration::from_secs(1),
    ));
    let cleaner = CleanerArgs {
        fragment: FragmentId(0),
        shared: shared.clone(),
        interval: Duration::from_secs(1),
        tombstone_gc_age: Some(Duration::ZERO),
        reset_senders: reset_senders.clone(),
    };
    let supervisor = SupervisorArgs {
        expiry_publisher: None,
        fragment: FragmentId(0),
        worker_prefix: "compat".to_string(),
        store: None,
        hlc: Arc::new(Mutex::new(HlcClock::new())),
        default_pool: PoolConfig::default(),
        high_pool: PoolConfig::default(),
        background_pool: PoolConfig::default(),
        flush_batch_size: 100,
        flush_interval: Duration::from_secs(1),
        clean_interval: Duration::from_secs(1),
        snapshot_interval: Duration::from_secs(1),
        tombstone_gc_age: None,
        reset_senders,
    };
    let health = FragmentHealth {
        fragment: FragmentId(0),
        worker_count: 0,
        worker_names: Vec::new(),
        restarts: HashMap::new(),
        last_reaped: 0,
        shed_counts: (0, 0, 0),
    };
    assert_eq!(shared.gc_tombstones(u64::MAX, 0).await, 0);
    assert!(shared.get_all().await.contains_key("deleted"));
    assert_eq!(health.last_reaped, 0);
    let _ = (cleaner, supervisor);
}

#[tokio::test]
async fn two_nodes_replicate() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("hello".to_string(), b"world".to_vec()).await.unwrap();

    common::wait_for(
        || async {
            b.get("hello".to_string()).await.unwrap().map(|v| v.value) == Some(b"world".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_drained_shutdown_fences_an_immediate_write() {
    use actordb::replication::ReplicationAckDurability;
    use actordb::shard::FragmentId;

    let a = test_node("drain-source").await;
    let b = test_node("drain-target").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "drain-target")
                .await
                .is_some()
        },
        Duration::from_secs(5),
    )
    .await;

    a.put("drain-immediate".to_string(), b"fenced".to_vec())
        .await
        .unwrap();
    let report = a
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();

    assert_eq!(report.durability(), ReplicationAckDurability::MemoryApplied);
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].fragment(), FragmentId(0));
    assert_eq!(report.targets()[0].peer_id(), "drain-target");
    assert_eq!(
        b.get("drain-immediate".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"fenced".to_vec())
    );
    assert!(matches!(
        a.put("after-fence".to_string(), Vec::new()).await,
        Err(NodeError::ShuttingDown)
    ));
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn drain_requires_no_change_verification_round() {
    use actordb::proto::ReplicationAckLevel;
    use actordb::replication::SenderConfig;

    let peer_id = "raw-stable-drain-peer";
    let (source, mut stream, initial_session_id) = raw_replication_drain_source(
        "stable-drain-source",
        peer_id,
        SenderConfig {
            batch_interval: Duration::from_secs(3600),
            consistency_interval: Duration::from_secs(3600),
            reconcile_timeout: Duration::from_secs(5),
            ..SenderConfig::default()
        },
    )
    .await;
    source
        .put("stable-drain-key".to_string(), b"fenced".to_vec())
        .await
        .unwrap();

    let drain_source = source.clone();
    let drain = tokio::spawn(async move {
        drain_source
            .shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(10)),
            )
            .await
    });

    let drain_session = recv_session_start(&mut stream).await;
    assert_ne!(drain_session.session_id, initial_session_id);
    let first_request = recv_reconcile_request(&mut stream, drain_session.session_id).await;
    send_reconcile_response(&mut stream, &first_request, Vec::new()).await;
    let (repaired_summaries, first_barrier) = recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert_eq!(repaired_summaries.len(), 1);
    assert_eq!(repaired_summaries[0].key, "stable-drain-key");
    ack_replication_batch(
        &mut stream,
        &first_barrier,
        drain_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let verification_request = recv_reconcile_request(&mut stream, drain_session.session_id).await;
    assert_ne!(verification_request.round_id, first_request.round_id);
    assert!(
        !drain.is_finished(),
        "the repair round's first barrier completed the shutdown drain"
    );
    send_reconcile_response(&mut stream, &verification_request, repaired_summaries).await;
    let (verification_repairs, verification_barrier) =
        recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(verification_repairs.is_empty());
    assert!(
        !drain.is_finished(),
        "the drain completed before its stable barrier ACK"
    );
    ack_replication_batch(
        &mut stream,
        &verification_barrier,
        drain_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let report = tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("stable drain did not finish after its verification ACK")
        .unwrap()
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].peer_id(), peer_id);
}

#[tokio::test]
async fn drain_ignores_stale_old_session_ack_after_rotation() {
    use actordb::proto::{KeySummary, ReplicationAckLevel};
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let peer_id = "raw-stale-drain-peer";
    let (source, mut stream, old_session_id) = raw_replication_drain_source(
        "stale-drain-source",
        peer_id,
        SenderConfig {
            batch_size: 1,
            batch_interval: Duration::from_millis(10),
            consistency_interval: Duration::from_secs(3600),
            reconcile_timeout: Duration::from_secs(5),
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
            ..SenderConfig::default()
        },
    )
    .await;
    source
        .put("stale-drain-key".to_string(), b"fenced".to_vec())
        .await
        .unwrap();

    let ordinary_batch = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.session_id == old_session_id
                && batch.batch_seq != u64::MAX
                && !batch.ops.is_empty()
            {
                break batch;
            }
        }
    };
    let remote_summaries: Vec<KeySummary> = ordinary_batch
        .ops
        .iter()
        .map(|op| KeySummary {
            content_digest: content_digest(&op.value, op.expires_at_ms),
            key: op.key.clone(),
            timestamp_ms: op.timestamp_ms,
            logical_counter: op.logical_counter,
            node_id: op.node_id.clone(),
            is_tombstone: op.is_tombstone,
        })
        .collect();
    ack_replication_batch(
        &mut stream,
        &ordinary_batch,
        old_session_id,
        ReplicationAckLevel::StoreCommitted,
    )
    .await;
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), peer_id)
                .await
                .is_some_and(|report| report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;

    let drain_source = source.clone();
    let drain = tokio::spawn(async move {
        drain_source
            .shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(10)),
            )
            .await
    });
    let drain_session = recv_session_start(&mut stream).await;
    assert_ne!(drain_session.session_id, old_session_id);
    let request = recv_reconcile_request(&mut stream, drain_session.session_id).await;
    send_reconcile_response(&mut stream, &request, remote_summaries).await;
    let (repairs, barrier) = recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(repairs.is_empty());

    // Reuse the live barrier sequence with the expired session. Follow it with
    // a weak ACK on the current session: observing that mismatch proves the
    // sender consumed both FIFO controls without accepting the stale one.
    ack_replication_batch(
        &mut stream,
        &barrier,
        old_session_id,
        ReplicationAckLevel::StoreCommitted,
    )
    .await;
    ack_replication_batch(
        &mut stream,
        &barrier,
        drain_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), peer_id)
                .await
                .is_some_and(|report| {
                    report.session_id == drain_session.session_id
                        && report.in_flight == 1
                        && report.ack_durability_mismatch
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        !drain.is_finished(),
        "an ACK from the pre-drain session completed the rotated drain"
    );

    ack_replication_batch(
        &mut stream,
        &barrier,
        drain_session.session_id,
        ReplicationAckLevel::StoreCommitted,
    )
    .await;
    let report = tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("current-session durability ACK did not finish the drain")
        .unwrap()
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].peer_id(), peer_id);
}

#[tokio::test]
async fn drain_disconnect_reconnect_retries_same_generation() {
    use actordb::proto::ReplicationAckLevel;
    use actordb::replication::SenderConfig;

    let peer_id = "raw-reconnect-drain-peer";
    let (source, mut first_stream, initial_session_id) = raw_replication_drain_source(
        "reconnect-drain-source",
        peer_id,
        SenderConfig {
            batch_interval: Duration::from_secs(3600),
            consistency_interval: Duration::from_secs(3600),
            reconcile_timeout: Duration::from_secs(5),
            ..SenderConfig::default()
        },
    )
    .await;
    source
        .put("reconnect-drain-key".to_string(), b"fenced".to_vec())
        .await
        .unwrap();

    let drain_source = source.clone();
    let drain = tokio::spawn(async move {
        drain_source
            .shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(10)),
            )
            .await
    });
    let interrupted_session = recv_session_start(&mut first_stream).await;
    assert_ne!(interrupted_session.session_id, initial_session_id);
    let _interrupted_request =
        recv_reconcile_request(&mut first_stream, interrupted_session.session_id).await;
    drop(first_stream);
    common::wait_for(
        || async { source.peer_count().await == 0 },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        !drain.is_finished(),
        "disconnect completed or discarded the pending drain generation"
    );

    let mut replacement_stream = tokio::net::TcpStream::connect(source.addr()).await.unwrap();
    complete_no_secret_handshake(&mut replacement_stream, peer_id).await;
    let replacement_session = recv_session_start(&mut replacement_stream).await;
    assert_ne!(
        replacement_session.session_id,
        interrupted_session.session_id
    );
    let first_request =
        recv_reconcile_request(&mut replacement_stream, replacement_session.session_id).await;
    send_reconcile_response(&mut replacement_stream, &first_request, Vec::new()).await;
    let (repaired_summaries, first_barrier) =
        recv_reconcile_repairs_and_barrier(&mut replacement_stream).await;
    assert_eq!(repaired_summaries.len(), 1);
    assert_eq!(repaired_summaries[0].key, "reconnect-drain-key");
    ack_replication_batch(
        &mut replacement_stream,
        &first_barrier,
        replacement_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let verification_request =
        recv_reconcile_request(&mut replacement_stream, replacement_session.session_id).await;
    send_reconcile_response(
        &mut replacement_stream,
        &verification_request,
        repaired_summaries,
    )
    .await;
    let (verification_repairs, verification_barrier) =
        recv_reconcile_repairs_and_barrier(&mut replacement_stream).await;
    assert!(verification_repairs.is_empty());
    ack_replication_batch(
        &mut replacement_stream,
        &verification_barrier,
        replacement_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let report = tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("the original drain generation did not survive reconnect")
        .unwrap()
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].peer_id(), peer_id);
}

#[tokio::test]
async fn manual_placement_drain_freezes_target_and_keeps_replication_runtime_alive() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::ReplicationAckLevel;
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let fragment = FragmentId(0);
    let source_id = "dynamic-drain-source";
    let peer_id = "dynamic-drain-peer";
    let initial_placement = HashMap::from([(fragment, vec![source_id.to_string()])]);
    let controller = Arc::new(ManualPlacement::new(initial_placement.clone()));
    let source = Arc::new(
        test_node_with_config(
            source_id,
            NodeConfig {
                placement_controller: Some(controller.clone()),
                placement_poll_interval: Duration::from_millis(10),
                replication: SenderConfig {
                    batch_interval: Duration::from_secs(3600),
                    consistency_interval: Duration::from_secs(3600),
                    reconcile_timeout: Duration::from_secs(5),
                    ..SenderConfig::default()
                },
                ..NodeConfig::default()
            },
        )
        .await,
    );
    common::wait_for(
        || async {
            let report = source.placement_report().await;
            report.placement == initial_placement
                && report.states.get(&fragment) == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    source
        .put("dynamic-drain-key".to_string(), b"fenced".to_vec())
        .await
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(source.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, peer_id).await;
    common::wait_for(
        || async { source.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    let frozen_placement =
        HashMap::from([(fragment, vec![source_id.to_string(), peer_id.to_string()])]);
    controller.set_placement(frozen_placement.clone());
    common::wait_for(
        || async {
            source.placement_report().await.placement == frozen_placement
                && source
                    .sender_report(fragment, peer_id)
                    .await
                    .is_some_and(|report| report.session_id != 0)
        },
        Duration::from_secs(5),
    )
    .await;
    let pre_drain_session = recv_session_start(&mut stream).await;

    let drain_source = source.clone();
    let drain = tokio::spawn(async move {
        drain_source
            .shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(10)),
            )
            .await
    });
    let drain_session = recv_session_start(&mut stream).await;
    assert_ne!(drain_session.session_id, pre_drain_session.session_id);
    let first_request = recv_reconcile_request(&mut stream, drain_session.session_id).await;

    // A drain request is issued only after the placement task manager has
    // stopped. Move the controller back to source-only and give several poll
    // intervals a chance to expose an accidentally live placement driver.
    controller.set_placement(initial_placement.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(controller.placement(), initial_placement);
    let frozen_report = source.placement_report().await;
    assert!(frozen_report.dynamic);
    assert_eq!(frozen_report.placement, frozen_placement);
    assert_eq!(
        frozen_report.states.get(&fragment),
        Some(&FragmentPlacementState::Serving)
    );
    assert_eq!(source.peer_count().await, 1);
    assert!(!drain.is_finished());

    // The connection and fragment dispatcher remain supervised after the
    // placement plane freezes: the raw peer can drive reconciliation and the
    // dispatcher supplies the locally authoritative winner as a repair.
    send_reconcile_response(&mut stream, &first_request, Vec::new()).await;
    let (repaired_summaries, first_barrier) = recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert_eq!(repaired_summaries.len(), 1);
    assert_eq!(repaired_summaries[0].key, "dynamic-drain-key");
    ack_replication_batch(
        &mut stream,
        &first_barrier,
        drain_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let verification_request = recv_reconcile_request(&mut stream, drain_session.session_id).await;
    send_reconcile_response(&mut stream, &verification_request, repaired_summaries).await;
    let (verification_repairs, verification_barrier) =
        recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(verification_repairs.is_empty());
    ack_replication_batch(
        &mut stream,
        &verification_barrier,
        drain_session.session_id,
        ReplicationAckLevel::Applied,
    )
    .await;

    let report = tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("dynamic placement drain did not finish after the peer fence")
        .unwrap()
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].fragment(), fragment);
    assert_eq!(report.targets()[0].peer_id(), peer_id);
}

#[tokio::test]
async fn single_replica_drain_reports_no_remote_targets() {
    use actordb::shard::FragmentId;

    let node = test_node_with_config(
        "single-replica",
        NodeConfig {
            fragment_map: HashMap::from([(FragmentId(0), vec!["single-replica".to_string()])]),
            ..NodeConfig::default()
        },
    )
    .await;
    assert!(matches!(
        node.shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::ZERO)
        )
        .await,
        Err(ReplicationDrainShutdownError::InvalidOptions(_))
    ));
    node.put("still-running".to_string(), b"value".to_vec())
        .await
        .unwrap();

    let report = node
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert!(report.targets().is_empty());
}

#[tokio::test]
async fn replication_drain_timeout_retries_the_fenced_overflow_snapshot() {
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let placement = HashMap::from([(
        FragmentId(0),
        vec!["overflow-source".to_string(), "overflow-target".to_string()],
    )]);
    let config = || NodeConfig {
        fragment_map: placement.clone(),
        replication: SenderConfig {
            buffer_capacity: 1,
            batch_interval: Duration::from_secs(3600),
            consistency_interval: Duration::from_secs(3600),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let a = test_node_with_config("overflow-source", config()).await;
    let b = test_node_with_config("overflow-target", config()).await;

    for index in 0..25 {
        a.put(
            format!("overflow-{index}"),
            format!("value-{index}").into_bytes(),
        )
        .await
        .unwrap();
    }
    let first = a
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    match first {
        ReplicationDrainShutdownError::Timeout {
            phase,
            completed,
            pending,
        } => {
            assert_eq!(phase, ReplicationDrainPhase::PeerReconciliation);
            assert!(completed.is_empty());
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].fragment(), FragmentId(0));
            assert_eq!(pending[0].peer_id(), "overflow-target");
        }
        other => panic!("unexpected drain result: {other}"),
    }
    assert!(matches!(
        a.put("post-timeout".to_string(), Vec::new()).await,
        Err(NodeError::ShuttingDown)
    ));

    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    let report = a
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    for index in 0..25 {
        assert_eq!(
            b.get(format!("overflow-{index}"))
                .await
                .unwrap()
                .map(|value| value.value),
            Some(format!("value-{index}").into_bytes())
        );
    }
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_shutdown_abandons_a_timed_out_replication_drain() {
    use actordb::shard::FragmentId;

    let node = test_node_with_config(
        "local-escape",
        NodeConfig {
            fragment_map: HashMap::from([(
                FragmentId(0),
                vec!["local-escape".to_string(), "absent-peer".to_string()],
            )]),
            ..NodeConfig::default()
        },
    )
    .await;
    node.put("local-only".to_string(), b"value".to_vec())
        .await
        .unwrap();
    assert!(matches!(
        node.shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_millis(50))
        )
        .await,
        Err(ReplicationDrainShutdownError::Timeout { .. })
    ));
    tokio::time::timeout(Duration::from_secs(1), node.shutdown())
        .await
        .expect("local-only shutdown waited for the absent peer")
        .unwrap();
}

#[tokio::test]
async fn concurrent_replication_drain_callers_keep_independent_deadlines() {
    use actordb::shard::FragmentId;

    let placement = HashMap::from([(
        FragmentId(0),
        vec![
            "concurrent-source".to_string(),
            "concurrent-target".to_string(),
        ],
    )]);
    let source = Arc::new(
        test_node_with_config(
            "concurrent-source",
            NodeConfig {
                fragment_map: placement.clone(),
                ..NodeConfig::default()
            },
        )
        .await,
    );
    let target = test_node_with_config(
        "concurrent-target",
        NodeConfig {
            fragment_map: placement,
            ..NodeConfig::default()
        },
    )
    .await;
    source
        .put("concurrent-fence".to_string(), b"value".to_vec())
        .await
        .unwrap();

    let first_source = source.clone();
    let first = tokio::spawn(async move {
        first_source
            .shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(30)),
            )
            .await
    });
    common::wait_for(
        || async {
            matches!(
                source.get("concurrent-fence".to_string()).await,
                Err(NodeError::ShuttingDown)
            )
        },
        Duration::from_secs(5),
    )
    .await;

    let started = std::time::Instant::now();
    let second = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_millis(25)),
        )
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(matches!(
        second,
        ReplicationDrainShutdownError::Timeout {
            phase: ReplicationDrainPhase::ShutdownCoordination,
            ..
        }
    ));

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;
    let report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(
        target
            .get("concurrent-fence".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );
    target.shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_drain_honors_store_committed_receiver_fence() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let target_store = Arc::new(PreloadedStore::default());
    let source = test_node_with_config(
        "durable-source",
        NodeConfig {
            replication: SenderConfig {
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
                ..SenderConfig::default()
            },
            ..NodeConfig::default()
        },
    )
    .await;
    let target = test_node_with_config(
        "durable-target",
        NodeConfig {
            store: Some(target_store.clone()),
            flush_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        },
    )
    .await;
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), "durable-target")
                .await
                .is_some()
        },
        Duration::from_secs(5),
    )
    .await;

    source
        .put("store-fence".to_string(), b"committed".to_vec())
        .await
        .unwrap();
    let report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(
        report.durability(),
        ReplicationAckDurability::StoreCommitted
    );
    assert_eq!(
        target_store
            .get_fragment_zero("store-fence")
            .map(|value| value.value),
        Some(b"committed".to_vec())
    );
    target.shutdown().await.unwrap();
}

#[tokio::test]
async fn store_committed_drain_rejects_a_storeless_receiver_ack() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let placement = HashMap::from([(
        FragmentId(0),
        vec!["strict-source".to_string(), "storeless-target".to_string()],
    )]);
    let source = test_node_with_config(
        "strict-source",
        NodeConfig {
            fragment_map: placement.clone(),
            replication: SenderConfig {
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
                ..SenderConfig::default()
            },
            ..NodeConfig::default()
        },
    )
    .await;
    let target = test_node_with_config(
        "storeless-target",
        NodeConfig {
            fragment_map: placement,
            ..NodeConfig::default()
        },
    )
    .await;
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;
    source
        .put("strict-fence".to_string(), b"value".to_vec())
        .await
        .unwrap();

    let error = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    match error {
        ReplicationDrainShutdownError::Timeout {
            phase: ReplicationDrainPhase::PeerReconciliation,
            completed,
            pending,
        } => {
            assert!(completed.is_empty());
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].peer_id(), "storeless-target");
        }
        other => panic!("unexpected drain result: {other}"),
    }
    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn store_committed_drain_persists_to_the_offered_rocksdb_backend() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;
    use actordb::store::{PrefixedStore, RocksDbStore};

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("drain-target");
    let placement = HashMap::from([(
        FragmentId(0),
        vec!["rocks-source".to_string(), "rocks-target".to_string()],
    )]);
    let source = test_node_with_config(
        "rocks-source",
        NodeConfig {
            fragment_map: placement.clone(),
            replication: SenderConfig {
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
                ..SenderConfig::default()
            },
            ..NodeConfig::default()
        },
    )
    .await;
    let target_store = Arc::new(RocksDbStore::open(&database).unwrap());
    let target_config = NodeConfig {
        store: Some(target_store.clone()),
        fragment_map: placement.clone(),
        flush_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let target = test_node_with_config("rocks-target", target_config).await;
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;

    source
        .put("rocks-drain".to_string(), b"durable".to_vec())
        .await
        .unwrap();
    let report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(
        report.durability(),
        ReplicationAckDurability::StoreCommitted
    );
    let persisted = PrefixedStore::new(target_store.clone(), FragmentId(0))
        .load()
        .unwrap();
    assert!(persisted.iter().any(|(key, value)| {
        key == b"rocks-drain" && value.value == b"durable" && !value.is_tombstone
    }));
    target.shutdown().await.unwrap();
    drop(target);
    drop(target_store);

    let mut restart_config = NodeConfig::with_rocksdb(&database).unwrap();
    restart_config.fragment_map = placement;
    let restarted = test_node_with_config("rocks-target", restart_config).await;
    assert_eq!(
        restarted
            .get("rocks-drain".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"durable".to_vec())
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_report_survives_a_later_local_flush_failure() {
    use actordb::shard::FragmentId;

    let placement = HashMap::from([(
        FragmentId(0),
        vec!["flush-source".to_string(), "flush-target".to_string()],
    )]);
    let source_store = Arc::new(ToggleFailStore::default());
    let source = test_node_with_config(
        "flush-source",
        NodeConfig {
            fragment_map: placement.clone(),
            store: Some(source_store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        },
    )
    .await;
    let target = test_node_with_config(
        "flush-target",
        NodeConfig {
            fragment_map: placement,
            ..NodeConfig::default()
        },
    )
    .await;
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;
    source
        .put("flush-after-drain".to_string(), b"retained".to_vec())
        .await
        .unwrap();

    let first_report = match source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap_err()
    {
        ReplicationDrainShutdownError::Local { error, replication } => {
            assert!(matches!(error, NodeError::Storage(_)));
            replication
        }
        other => panic!("unexpected drain result: {other}"),
    };
    assert_eq!(first_report.targets().len(), 1);
    assert_eq!(
        target
            .get("flush-after-drain".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"retained".to_vec())
    );

    // The source has already stopped transport after completing the remote
    // fence. Remove the target entirely: the retry must use the retained
    // report and perform only the failed local persistence phase.
    target.shutdown().await.unwrap();
    source_store.failing.store(false, Ordering::SeqCst);
    let retried_report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert_eq!(retried_report, first_report);
}

#[tokio::test]
async fn three_node_mesh_converges() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    let c = test_node("c").await;

    // Full mesh: Phase 5 replication is direct sender→receiver (no
    // gossip multi-hop), so every pair needs a connection.
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    c.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async {
            a.peer_count().await == 2 && b.peer_count().await == 2 && c.peer_count().await == 2
        },
        Duration::from_secs(5),
    )
    .await;

    a.put("from-a".to_string(), b"1".to_vec()).await.unwrap();
    b.put("from-b".to_string(), b"2".to_vec()).await.unwrap();
    c.put("from-c".to_string(), b"3".to_vec()).await.unwrap();

    for n in [&a, &b, &c] {
        common::wait_for(
            || async {
                let all = n.get_all().await.unwrap();
                all.get("from-a").map(|v| v.value.clone()) == Some(b"1".to_vec())
                    && all.get("from-b").map(|v| v.value.clone()) == Some(b"2".to_vec())
                    && all.get("from-c").map(|v| v.value.clone()) == Some(b"3".to_vec())
            },
            Duration::from_secs(5),
        )
        .await;
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn lww_last_write_wins_everywhere() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("race".to_string(), b"first".to_vec()).await.unwrap();
    // Ensure the second write gets a strictly later timestamp.
    tokio::time::sleep(Duration::from_millis(50)).await;
    b.put("race".to_string(), b"second".to_vec()).await.unwrap();

    // Both replicas must agree on the later write.
    for n in [&a, &b] {
        common::wait_for(
            || async {
                n.get("race".to_string()).await.unwrap().map(|v| v.value)
                    == Some(b"second".to_vec())
            },
            Duration::from_secs(5),
        )
        .await;
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn restarted_node_writes_are_accepted() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("k".to_string(), b"v1".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"v1".to_vec()) },
        Duration::from_secs(5),
    )
    .await;

    // Simulate a restart of a: same node_id, but fresh process state —
    // the sequence counter is back at zero.
    a.shutdown().await.unwrap();
    let a2 = test_node("a").await;
    a2.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a2.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    // Ensure the post-restart write gets a strictly later timestamp.
    tokio::time::sleep(Duration::from_millis(50)).await;
    a2.put("k".to_string(), b"v2".to_vec()).await.unwrap();

    // b must accept the new boot's writes. With the old (node_id, seq)
    // dedup key, b would have dropped this as a replay of the
    // pre-restart op and the cluster would have silently diverged.
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"v2".to_vec()) },
        Duration::from_secs(5),
    )
    .await;
    a2.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_connection_redials() {
    // Fixed port so the dialer can find the peer again after a restart.
    let addr: SocketAddr = "127.0.0.1:41071".parse().unwrap();
    let a = Node::start("a".to_string(), addr, NodeConfig::default())
        .await
        .expect("node a failed to start");
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    a.put("k".to_string(), b"v1".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"v1".to_vec()) },
        Duration::from_secs(5),
    )
    .await;

    // Kill a outright. b's dialer-side supervisor must notice the drop
    // and redial once a is back on the same address.
    a.shutdown().await.unwrap();
    // Give the OS a moment to release the port.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a2 = Node::start("a".to_string(), addr, NodeConfig::default())
        .await
        .expect("node a failed to restart");

    common::wait_for(
        || async { a2.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(15),
    )
    .await;

    // Writes flow again over the re-established connection (and the new
    // boot's ops are accepted — see restarted_node_writes_are_accepted).
    tokio::time::sleep(Duration::from_millis(50)).await;
    a2.put("k".to_string(), b"v2".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"v2".to_vec()) },
        Duration::from_secs(5),
    )
    .await;

    a2.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_sender_reap_recovers_offline_writes_and_tombstones_by_full_sync() {
    let a = test_node("legacy-a").await;
    let b = test_node("legacy-b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("delete-while-away".to_string(), b"old".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            b.get("delete-while-away".to_string())
                .await
                .unwrap()
                .is_some()
        },
        Duration::from_secs(5),
    )
    .await;
    b.shutdown().await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 0 },
        Duration::from_secs(2),
    )
    .await;

    a.put("written-while-away".to_string(), b"new".to_vec())
        .await
        .unwrap();
    a.delete("delete-while-away".to_string()).await.unwrap();

    let b2 = test_node("legacy-b").await;
    b2.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b2).await;
    common::wait_for(
        || async {
            b2.get("written-while-away".to_string())
                .await
                .unwrap()
                .map(|value| value.value)
                == Some(b"new".to_vec())
                && b2
                    .get("delete-while-away".to_string())
                    .await
                    .unwrap()
                    .is_none()
        },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b2.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_member_retries_initial_failure_and_rejects_wrong_identity() {
    // Reserve an ephemeral address, release it, and start the node while no
    // listener exists. The configured dial intent must survive that first
    // refusal and connect once the peer appears.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = probe.local_addr().unwrap();
    drop(probe);

    let node = Node::start(
        "configured-dialer".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            members: HashMap::from([("expected-peer".to_string(), peer_addr)]),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;

    let listener = tokio::net::TcpListener::bind(peer_addr).await.unwrap();
    let (mut wrong_peer, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("configured member was not retried after its initial refusal")
        .unwrap();
    assert!(matches!(
        recv_frame(&mut wrong_peer).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send_frame(
        &mut wrong_peer,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "wrong-peer".to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x31; 32],
            })),
        },
    )
    .await;
    let mut remainder = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        wrong_peer.read_to_end(&mut remainder),
    )
    .await
    .expect("wrong configured member identity was not rejected")
    .unwrap();
    assert_eq!(node.peer_count().await, 0);

    let (mut expected_peer, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("identity failure did not leave the configured dialer supervised")
        .unwrap();
    complete_no_secret_handshake(&mut expected_peer, "expected-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(3),
    )
    .await;

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn address_only_connect_pins_the_first_authenticated_identity() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let node = test_node("identity-pinning-dialer").await;
    node.connect(peer_addr).await.unwrap();

    let (mut first_peer, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("initial dial did not arrive")
        .unwrap();
    complete_no_secret_handshake(&mut first_peer, "peer-b").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(2),
    )
    .await;
    drop(first_peer);
    common::wait_for(
        || async { node.peer_count().await == 0 },
        Duration::from_secs(2),
    )
    .await;

    let (mut wrong_replacement, _) =
        tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("dial supervisor did not retry after disconnect")
            .unwrap();
    assert!(matches!(
        recv_frame(&mut wrong_replacement).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send_frame(
        &mut wrong_replacement,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "peer-c".to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x42; 32],
            })),
        },
    )
    .await;
    let mut remainder = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        wrong_replacement.read_to_end(&mut remainder),
    )
    .await
    .expect("address reuse by a different identity was accepted")
    .unwrap();
    assert_eq!(node.peer_count().await, 0);

    let (mut original_identity, _) =
        tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("identity mismatch stopped the supervised retries")
            .unwrap();
    complete_no_secret_handshake(&mut original_identity, "peer-b").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(2),
    )
    .await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn simultaneous_dials_elect_one_stable_connection_without_redial_churn() {
    let attempts_a = Arc::new(AtomicUsize::new(0));
    let attempts_b = Arc::new(AtomicUsize::new(0));
    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            network: Arc::new(CountingNetwork {
                connect_attempts: attempts_a.clone(),
            }),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let b = Node::start(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            network: Arc::new(CountingNetwork {
                connect_attempts: attempts_b.clone(),
            }),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();

    let (a_result, b_result) = tokio::join!(a.connect(b.addr()), b.connect(a.addr()));
    a_result.unwrap();
    b_result.unwrap();
    wait_for_peers(&a, &b).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(attempts_a.load(Ordering::SeqCst), 1);
    assert_eq!(attempts_b.load(Ordering::SeqCst), 1);
    assert_eq!(a.peer_count().await, 1);
    assert_eq!(b.peer_count().await, 1);

    a.put("crossed-dial".to_string(), b"stable".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            b.get("crossed-dial".to_string())
                .await
                .unwrap()
                .map(|value| value.value)
                == Some(b"stable".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_connect_calls_share_one_transport_dial_intent() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let node = Node::start(
        "single-dial-intent".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            network: Arc::new(CountingNetwork {
                connect_attempts: attempts.clone(),
            }),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();

    let (first, second, third, fourth) = tokio::join!(
        node.connect(peer_addr),
        node.connect(peer_addr),
        node.connect(peer_addr),
        node.connect(peer_addr),
    );
    first.unwrap();
    second.unwrap();
    third.unwrap();
    fourth.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let (mut peer, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("reserved dial did not reach the listener")
        .unwrap();
    complete_no_secret_handshake(&mut peer, "single-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(2),
    )
    .await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_connect_waiter_never_reports_false_success_after_owner_failure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let node = Arc::new(
        Node::start(
            "failed-single-dial".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                network: Arc::new(GatedFailNetwork {
                    connect_attempts: attempts.clone(),
                    first_entered: first_entered.clone(),
                    release_first: release_first.clone(),
                }),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    let peer_addr = "127.0.0.1:9".parse().unwrap();

    let first_node = node.clone();
    let first = tokio::spawn(async move { first_node.connect(peer_addr).await });
    first_entered.notified().await;
    let second_node = node.clone();
    let second = tokio::spawn(async move { second_node.connect(peer_addr).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !second.is_finished(),
        "duplicate caller reported success while the owning dial was pending"
    );

    release_first.notify_one();
    assert!(first.await.unwrap().is_err());
    assert!(second.await.unwrap().is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // Both failed attempts released their reservations; a later caller can
    // become the owner instead of observing a permanently stuck Pending entry.
    assert!(node.connect(peer_addr).await.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_connect_owner_releases_pending_intent_to_waiter() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let node = Arc::new(
        Node::start(
            "cancelled-single-dial".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                network: Arc::new(GatedFailNetwork {
                    connect_attempts: attempts.clone(),
                    first_entered: first_entered.clone(),
                    release_first,
                }),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    let peer_addr = "127.0.0.1:9".parse().unwrap();

    let first_node = node.clone();
    let first = tokio::spawn(async move { first_node.connect(peer_addr).await });
    first_entered.notified().await;

    let second_node = node.clone();
    let second = tokio::spawn(async move { second_node.connect(peer_addr).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!second.is_finished());

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(second.await.unwrap().is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_joiner_converges_via_immediate_reconciliation() {
    use actordb::shard::FragmentId;

    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("early".to_string(), b"data".to_vec()).await.unwrap();
    common::wait_for(
        || async {
            b.get("early".to_string()).await.unwrap().map(|v| v.value) == Some(b"data".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    // C joins after the data exists and only ever talks to B. Protocol v3
    // starts correlated reconciliation immediately after the peer session.
    let c = test_node("c").await;
    c.connect(b.addr()).await.unwrap();

    common::wait_for(
        || async {
            c.get("early".to_string()).await.unwrap().map(|v| v.value) == Some(b"data".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            c.sender_report(FragmentId(0), "b")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1)
        },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconciliation_paginates_summaries_and_fetches_without_restart_storms() {
    use actordb::shard::FragmentId;

    const KEY_COUNT: usize = 300;
    const VALUE_BYTES: usize = 8 * 1024;

    let source = test_node("paged-source").await;
    for index in 0..KEY_COUNT {
        source
            .put(
                format!("paged-{index:04}"),
                vec![(index % 251) as u8; VALUE_BYTES],
            )
            .await
            .unwrap();
    }

    let target = test_node("paged-target").await;
    target.connect(source.addr()).await.unwrap();
    wait_for_peers(&source, &target).await;

    common::wait_for(
        || async {
            for index in 0..KEY_COUNT {
                let expected = vec![(index % 251) as u8; VALUE_BYTES];
                if target
                    .get(format!("paged-{index:04}"))
                    .await
                    .unwrap()
                    .map(|value| value.value)
                    != Some(expected)
                {
                    return false;
                }
            }
            true
        },
        Duration::from_secs(20),
    )
    .await;
    common::wait_for(
        || async {
            target
                .sender_report(FragmentId(0), "paged-source")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1)
        },
        Duration::from_secs(20),
    )
    .await;

    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}

#[tokio::test]
async fn bidirectional_large_reconciliation_keeps_connection_reader_live() {
    use actordb::replication::SenderConfig;
    use actordb::shard::{fragment_of, FragmentId};

    // Two data pages cannot coexist in the minimum legal producer byte budget
    // (one maximum 16 MiB frame; the other 16 MiB is response-only).
    // Both peers therefore need their connection reader to drain the opposite
    // direction while their responder waits to admit its second page. More
    // fragments than the responder queue depth also prove that the shared
    // connection credit schedules honest work instead of disconnecting a
    // simultaneous startup burst.
    const VALUE_BYTES: usize = 8 * 1024 * 1024;
    const MIN_WRITER_BYTES: usize = 32 * 1024 * 1024;
    const FRAGMENT_COUNT: usize = 10;

    fn key_for_fragment(prefix: &str, fragment: FragmentId) -> String {
        for nonce in 0u64.. {
            let key = format!("{prefix}-{}-{nonce}", fragment.0);
            if fragment_of(&key, FRAGMENT_COUNT) == fragment {
                return key;
            }
        }
        unreachable!()
    }

    let timestamp = now_ms();
    let store_a = Arc::new(PreloadedStore::default());
    let store_b = Arc::new(PreloadedStore::default());
    let mut large_a_keys = Vec::new();
    let mut large_b_keys = Vec::new();
    for index in 0..2 {
        let key_a = key_for_fragment(&format!("large-a-{index}"), FragmentId(0));
        let key_b = key_for_fragment(&format!("large-b-{index}"), FragmentId(0));
        store_a.insert_fragment(
            0,
            &key_a,
            VersionedValue::new(vec![b'a'; VALUE_BYTES], timestamp, "large-a".to_string()),
        );
        store_b.insert_fragment(
            0,
            &key_b,
            VersionedValue::new(vec![b'b'; VALUE_BYTES], timestamp, "large-b".to_string()),
        );
        large_a_keys.push(key_a);
        large_b_keys.push(key_b);
    }

    let mut small_a_keys = Vec::new();
    let mut small_b_keys = Vec::new();
    for fragment in 1..FRAGMENT_COUNT {
        let fragment = FragmentId(fragment as u32);
        let key_a = key_for_fragment("small-a", fragment);
        let key_b = key_for_fragment("small-b", fragment);
        store_a.insert_fragment(
            fragment.0,
            &key_a,
            VersionedValue::new(vec![b'a'], timestamp, "large-a".to_string()),
        );
        store_b.insert_fragment(
            fragment.0,
            &key_b,
            VersionedValue::new(vec![b'b'], timestamp, "large-b".to_string()),
        );
        small_a_keys.push(key_a);
        small_b_keys.push(key_b);
    }

    let fragment_map: HashMap<FragmentId, Vec<String>> = (0..FRAGMENT_COUNT)
        .map(|fragment| {
            (
                FragmentId(fragment as u32),
                vec!["large-a".to_string(), "large-b".to_string()],
            )
        })
        .collect();
    let replication = SenderConfig {
        consistency_interval: Duration::from_secs(3600),
        reconcile_timeout: Duration::from_secs(20),
        ..SenderConfig::default()
    };
    let runtime = NodeRuntimeOptions::default()
        .with_peer_queue_byte_capacity(MIN_WRITER_BYTES)
        .with_reconcile_data_admission_timeout(Duration::from_secs(20));

    let a = Node::start_with_options(
        "large-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(store_a),
            fragment_count: FRAGMENT_COUNT,
            fragment_map: fragment_map.clone(),
            replication: replication.clone(),
            ..NodeConfig::default()
        },
        runtime.clone(),
    )
    .await
    .unwrap();
    let b = Node::start_with_options(
        "large-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(store_b),
            fragment_count: FRAGMENT_COUNT,
            fragment_map,
            replication,
            ..NodeConfig::default()
        },
        runtime,
    )
    .await
    .unwrap();

    a.connect(b.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    common::wait_for(
        || async {
            for fragment in 0..FRAGMENT_COUNT {
                let fragment = FragmentId(fragment as u32);
                if !a
                    .sender_report(fragment, "large-b")
                    .await
                    .is_some_and(|report| report.reconcile_rounds >= 1)
                    || !b
                        .sender_report(fragment, "large-a")
                        .await
                        .is_some_and(|report| report.reconcile_rounds >= 1)
                {
                    return false;
                }
            }
            true
        },
        Duration::from_secs(10),
    )
    .await;

    for (node, key, expected) in large_b_keys
        .iter()
        .map(|key| (&a, key, b'b'))
        .chain(large_a_keys.iter().map(|key| (&b, key, b'a')))
    {
        let value = node
            .get(key.clone())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{key} did not reconcile"));
        assert_eq!(value.value.len(), VALUE_BYTES);
        assert!(value.value.iter().all(|byte| *byte == expected));
    }
    for (node, keys, expected) in [(&a, &small_b_keys, b'b'), (&b, &small_a_keys, b'a')] {
        for key in keys {
            assert_eq!(
                node.get(key.clone()).await.unwrap().unwrap().value,
                vec![expected]
            );
        }
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconcile_cursor_continues_after_revision_drift_for_verified_multi_pass() {
    use actordb::proto::{EnvelopeMsg, ReconcileRequest, SessionStart};

    let node = test_node("paged-revision-source").await;
    for index in 0..300 {
        node.put(format!("key-{index:04}"), vec![index as u8])
            .await
            .unwrap();
    }

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "paged-revision-peer").await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id: 77,
                fragment: 0,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 0,
                session_id: 77,
                round_id: 9,
                page_seq: 0,
                after_key: String::new(),
                snapshot_revision: 0,
                snapshot_time_ms: 0,
            })),
        },
    )
    .await;
    let first = loop {
        if let Some(EnvelopeMsg::ReconcileResponse(response)) = recv_frame(&mut stream).await.msg {
            if response.session_id == 77 && response.round_id == 9 {
                break response;
            }
        }
    };
    assert!(!first.done);
    assert!(!first.restart);

    node.put("zzzz-after-cursor".to_string(), b"new".to_vec())
        .await
        .unwrap();
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 0,
                session_id: 77,
                round_id: 9,
                page_seq: 1,
                after_key: first.next_after_key,
                snapshot_revision: first.snapshot_revision,
                snapshot_time_ms: first.snapshot_time_ms,
            })),
        },
    )
    .await;
    let second = loop {
        if let Some(EnvelopeMsg::ReconcileResponse(response)) = recv_frame(&mut stream).await.msg {
            if response.session_id == 77 && response.round_id == 9 && response.page_seq == 1 {
                break response;
            }
        }
    };
    assert!(!second.restart);
    assert_ne!(second.snapshot_revision, first.snapshot_revision);
    assert_eq!(second.snapshot_time_ms, first.snapshot_time_ms);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconcile_barrier_revokes_prior_fetch_authorization() {
    use actordb::proto::{
        BatchAck, EnvelopeMsg, ReconcileFetch, ReconcileRequest, ReplicateBatch,
        ReplicationAckLevel, SessionStart,
    };

    let node = test_node("barrier-auth-source").await;
    node.put("authorized-before-barrier".to_string(), b"value".to_vec())
        .await
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "barrier-auth-peer").await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id: 77,
                fragment: 0,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 0,
                session_id: 77,
                round_id: 9,
                page_seq: 0,
                after_key: String::new(),
                snapshot_revision: 0,
                snapshot_time_ms: 0,
            })),
        },
    )
    .await;
    let response = loop {
        if let Some(EnvelopeMsg::ReconcileResponse(response)) = recv_frame(&mut stream).await.msg {
            if response.session_id == 77 && response.round_id == 9 {
                break response;
            }
        }
    };
    let advertised_key = response
        .summaries
        .first()
        .expect("source did not advertise its key")
        .key
        .clone();

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: 77,
                batch_seq: 1,
                fragment: 0,
                ops: Vec::new(),
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    loop {
        if let Some(EnvelopeMsg::BatchAck(BatchAck {
            session_id: 77,
            batch_seq: 1,
            ..
        })) = recv_frame(&mut stream).await.msg
        {
            break;
        }
    }

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                fragment: 0,
                keys: vec![advertised_key],
                session_id: 77,
                round_id: 9,
                chunk_seq: 0,
            })),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut prefix = [0u8; 4];
            if stream.read_exact(&mut prefix).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(prefix) as usize;
            let mut body = vec![0u8; len];
            if stream.read_exact(&mut body).await.is_err() {
                break;
            }
        }
    })
    .await
    .expect("post-barrier fetch did not close the connection");

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconcile_barrier_rejects_a_replaced_fragment_incarnation() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        BatchNack, EnvelopeMsg, ReconcileRequest, ReplicateBatch, ReplicationAckLevel, SessionStart,
    };
    use actordb::shard::FragmentId;

    let fragment = FragmentId(0);
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        fragment,
        vec!["incarnation-node".to_string()],
    )])));
    let node = test_node_with_config(
        "incarnation-node",
        NodeConfig {
            placement_controller: Some(controller.clone()),
            placement_poll_interval: Duration::from_millis(10),
            placement_drain_warn_interval: Duration::from_millis(10),
            ..NodeConfig::default()
        },
    )
    .await;
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "incarnation-peer").await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id: 77,
                fragment: 0,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 0,
                session_id: 77,
                round_id: 9,
                page_seq: 0,
                after_key: String::new(),
                snapshot_revision: 0,
                snapshot_time_ms: 0,
            })),
        },
    )
    .await;
    loop {
        if let Some(EnvelopeMsg::ReconcileResponse(response)) = recv_frame(&mut stream).await.msg {
            if response.session_id == 77 && response.round_id == 9 {
                break;
            }
        }
    }

    // Destroy the subtree which served the summaries, then create a new
    // in-memory incarnation at the same fragment id before the old barrier.
    controller.set_placement(HashMap::from([(
        fragment,
        vec!["replacement-owner".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(5),
    )
    .await;
    controller.set_placement(HashMap::from([(
        fragment,
        vec!["incarnation-node".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: 77,
                batch_seq: 1,
                fragment: 0,
                ops: Vec::new(),
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    loop {
        if let Some(EnvelopeMsg::BatchNack(BatchNack {
            session_id: 77,
            batch_seq: 1,
            error,
            ..
        })) = recv_frame(&mut stream).await.msg
        {
            assert!(error.contains("serving incarnation"));
            break;
        }
    }

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_immediate_reconciliation_is_not_counted_and_retries() {
    use actordb::proto::{EnvelopeMsg, KeySummary, SyncEntry};
    use actordb::shard::FragmentId;

    let node = Node::start(
        "sync-failure-receiver".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(Arc::new(AlwaysFailStore)),
            flush_batch_size: 1,
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-sync-source").await;
    let request = loop {
        match recv_frame(&mut stream).await.msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => break request,
            Some(EnvelopeMsg::SessionStart(_)) => {}
            _ => {}
        }
    };
    let summary = KeySummary {
        content_digest: content_digest(b"value", 0),
        key: "failed-reconcile".to_string(),
        timestamp_ms: now_ms(),
        logical_counter: 0,
        node_id: "raw-sync-source".to_string(),
        is_tombstone: false,
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &request,
                vec![summary.clone()],
            ))),
        },
    )
    .await;
    let fetch = loop {
        if let Some(EnvelopeMsg::ReconcileFetch(fetch)) = recv_frame(&mut stream).await.msg {
            break fetch;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(complete_reconcile_data(
                &fetch,
                vec![SyncEntry {
                    key: summary.key,
                    value: b"value".to_vec(),
                    timestamp_ms: summary.timestamp_ms,
                    logical_counter: summary.logical_counter,
                    node_id: "raw-sync-source".to_string(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
            ))),
        },
    )
    .await;
    let retry = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(retry)) = recv_frame(&mut stream).await.msg {
            if retry.round_id != request.round_id {
                break retry;
            }
        }
    };
    assert_eq!(retry.session_id, request.session_id);
    assert_eq!(node.peer_count().await, 1);
    assert_eq!(
        node.sender_report(FragmentId(0), "raw-sync-source")
            .await
            .unwrap()
            .reconcile_rounds,
        0
    );
    assert!(matches!(node.shutdown().await, Err(NodeError::Storage(_))));
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn persistence_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("actordb");

    // Write with persistence on, shut down cleanly, restart with no
    // peers: the full dataset must come back from disk alone.
    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(db_path.clone()).unwrap(),
    )
    .await
    .expect("node failed to start");

    a.put("k1".to_string(), b"v1".to_vec()).await.unwrap();
    a.put("k2".to_string(), b"v2".to_vec()).await.unwrap();
    a.shutdown().await.unwrap();

    let a2 = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(db_path).unwrap(),
    )
    .await
    .expect("node failed to restart");

    for (k, v) in [("k1", b"v1".as_slice()), ("k2", b"v2".as_slice())] {
        let got = a2
            .get(k.to_string())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{k} missing after restart"));
        assert_eq!(got.value, v);
        assert_eq!(got.node_id, "a");
    }
    a2.shutdown().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_manifest_rejects_wrong_identity_before_network_bind() {
    use actordb::node::NodeError;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("manifest");
    let original = Node::start(
        "manifest-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(&db_path).unwrap(),
    )
    .await
    .unwrap();
    original.shutdown().await.unwrap();
    drop(original);

    // The occupied address proves manifest validation runs before transport
    // bind: a mismatched database must report Storage, not Network.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied_addr = occupied.local_addr().unwrap();
    let wrong_node = Node::start(
        "manifest-b".to_string(),
        occupied_addr,
        NodeConfig::with_rocksdb(&db_path).unwrap(),
    )
    .await;
    assert!(matches!(wrong_node, Err(NodeError::Storage(_))));
    drop(occupied);

    let mut wrong_count = NodeConfig::with_rocksdb(&db_path).unwrap();
    wrong_count.fragment_count = 2;
    let wrong_count = Node::start(
        "manifest-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        wrong_count,
    )
    .await;
    assert!(matches!(wrong_count, Err(NodeError::Storage(_))));

    // Failed mismatches never rewrite or poison the original identity.
    let reopened = Node::start(
        "manifest-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(&db_path).unwrap(),
    )
    .await
    .unwrap();
    reopened.shutdown().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_store_allows_only_one_live_node_attachment() {
    use actordb::node::NodeError;
    use actordb::store::RocksDbStore;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
    let config = || NodeConfig {
        store: Some(store.clone()),
        ..NodeConfig::default()
    };
    let first = Node::start(
        "shared-store".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();
    let second = Node::start(
        "shared-store".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await;
    assert!(matches!(second, Err(NodeError::Storage(_))));

    first.shutdown().await.unwrap();
    let replacement = Node::start(
        "shared-store".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();
    replacement.shutdown().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn persistence_keeps_gossiped_winner() {
    // The write-through path must persist gossip-received ops too, and
    // after a restart the node must hold the LWW *winner*, then keep
    // converging when it reconnects.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("actordb");

    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(db_path.clone()).unwrap(),
    )
    .await
    .expect("node failed to start");
    let b = test_node("b").await;
    a.connect(b.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Strict causal order: b only writes after seeing a's value, so
    // b's timestamp is definitely later and its value must win.
    a.put("k".to_string(), b"from-a".to_vec()).await.unwrap();
    common::wait_for(
        || async {
            b.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"from-a".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;
    b.put("k".to_string(), b"from-b".to_vec()).await.unwrap();
    common::wait_for(
        || async {
            a.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"from-b".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    // Restart a with no peers: disk must hold the winner.
    a.shutdown().await.unwrap();
    let a2 = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::with_rocksdb(db_path).unwrap(),
    )
    .await
    .expect("node failed to restart");
    let got = a2
        .get("k".to_string())
        .await
        .unwrap()
        .expect("k missing after restart");
    assert_eq!(got.value, b"from-b");

    // Reconnecting still converges: b's newer write wins again.
    a2.connect(b.addr()).await.unwrap();
    b.put("k".to_string(), b"from-b2".to_vec()).await.unwrap();
    common::wait_for(
        || async {
            a2.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"from-b2".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    a2.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn persistence_flushes_on_shutdown() {
    // With a batch size and interval that never trigger mid-test, the
    // shutdown flush is the only thing persisting these writes.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("actordb");
    let config = || {
        let mut cfg = NodeConfig::with_rocksdb(db_path.clone()).unwrap();
        cfg.flush_batch_size = 1_000_000;
        cfg.flush_interval = Duration::from_secs(3600);
        cfg
    };

    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config())
        .await
        .expect("node failed to start");
    a.put("k".to_string(), b"v".to_vec()).await.unwrap();
    a.shutdown().await.unwrap();

    let a2 = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config())
        .await
        .expect("node failed to restart");
    let got = a2
        .get("k".to_string())
        .await
        .unwrap()
        .expect("k missing after restart");
    assert_eq!(got.value, b"v");
    a2.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_flush_failure_is_retryable_across_all_fragments() {
    use actordb::shard::{fragment_of, FragmentId};

    let store = Arc::new(ToggleFailStore::default());
    let config = || NodeConfig {
        fragment_count: 2,
        store: Some(store.clone()),
        flush_batch_size: usize::MAX,
        flush_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let node = Node::start(
        "retry-shutdown".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();

    let mut keys = HashMap::new();
    for index in 0..10_000 {
        let key = format!("shutdown-fragment-{index}");
        let fragment = fragment_of(&key, 2);
        keys.entry(fragment).or_insert(key);
        if keys.len() == 2 {
            break;
        }
    }
    for fragment in [FragmentId(0), FragmentId(1)] {
        let key = keys.get(&fragment).expect("missing key for fragment");
        node.put(key.clone(), format!("value-{}", fragment.0).into_bytes())
            .await
            .unwrap();
    }

    let first = node.shutdown().await.unwrap_err();
    assert!(matches!(first, NodeError::Storage(_)));
    assert_eq!(store.attempts.load(Ordering::SeqCst), 4);
    assert!(matches!(
        node.get(keys[&FragmentId(0)].clone()).await,
        Err(NodeError::ShuttingDown)
    ));

    store.failing.store(false, Ordering::SeqCst);
    node.shutdown().await.unwrap();
    assert_eq!(store.attempts.load(Ordering::SeqCst), 6);

    let restarted = Node::start(
        "retry-shutdown".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();
    for fragment in [FragmentId(0), FragmentId(1)] {
        let key = keys[&fragment].clone();
        let value = restarted.get(key).await.unwrap().unwrap();
        assert_eq!(value.value, format!("value-{}", fragment.0).into_bytes());
    }
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_retry_skips_fragments_that_already_reached_the_durability_fence() {
    use actordb::shard::{fragment_of, FragmentId};

    let store = Arc::new(SelectiveFailStore::new(0));
    let config = || NodeConfig {
        fragment_count: 2,
        store: Some(store.clone()),
        flush_batch_size: usize::MAX,
        flush_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let node = Node::start(
        "partial-retry-shutdown".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();

    let mut keys = HashMap::new();
    for index in 0..10_000 {
        let key = format!("partial-shutdown-fragment-{index}");
        let fragment = fragment_of(&key, 2);
        keys.entry(fragment).or_insert(key);
        if keys.len() == 2 {
            break;
        }
    }
    for fragment in [FragmentId(0), FragmentId(1)] {
        node.put(
            keys[&fragment].clone(),
            format!("value-{}", fragment.0).into_bytes(),
        )
        .await
        .unwrap();
    }

    assert!(matches!(node.shutdown().await, Err(NodeError::Storage(_))));
    let failed_attempts = store.attempts_for(0);
    let prepared_attempts = store.attempts_for(1);
    assert!(failed_attempts > 0);
    assert!(prepared_attempts > 0);

    store.failing.store(false, Ordering::SeqCst);
    node.shutdown().await.unwrap();
    assert!(store.attempts_for(0) > failed_attempts);
    assert_eq!(
        store.attempts_for(1),
        prepared_attempts,
        "retry flushed a fragment that had already completed its durability fence"
    );

    let restarted = Node::start(
        "partial-retry-shutdown".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
    )
    .await
    .unwrap();
    for fragment in [FragmentId(0), FragmentId(1)] {
        let value = restarted
            .get(keys[&fragment].clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.value, format!("value-{}", fragment.0).into_bytes());
    }
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_shutdown_waiter_resumes_retained_final_flush() {
    let store = Arc::new(GatedStore::default());
    let _release_on_drop = store.release_on_drop();
    let node = Arc::new(
        Node::start(
            "cancelled-shutdown".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                store: Some(store.clone()),
                flush_batch_size: usize::MAX,
                flush_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    node.put("cancelled-flush".to_string(), b"retained".to_vec())
        .await
        .unwrap();

    let shutdown_node = node.clone();
    let first = tokio::spawn(async move { shutdown_node.shutdown().await });
    common::wait_for(
        || async { store.entered.load(Ordering::SeqCst) },
        Duration::from_secs(5),
    )
    .await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(matches!(
        node.get("cancelled-flush".to_string()).await,
        Err(NodeError::ShuttingDown)
    ));

    store.release();
    node.shutdown().await.unwrap();
    let mut persisted_key = 0u32.to_be_bytes().to_vec();
    persisted_key.extend_from_slice(b"cancelled-flush");
    assert_eq!(
        store
            .entries
            .lock()
            .unwrap()
            .get(&persisted_key)
            .cloned()
            .map(|value| value.value),
        Some(b"retained".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_rejects_new_calls_and_waits_for_an_admitted_put() {
    use actordb::actor::OpPriority;

    let store = Arc::new(GatedStore::default());
    let _release_on_drop = store.release_on_drop();
    let node = Arc::new(
        Node::start(
            "operation-fence".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                store: Some(store.clone()),
                flush_batch_size: 1,
                flush_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                clean_interval: Duration::from_secs(3600),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap(),
    );

    let put_node = node.clone();
    let put = tokio::spawn(async move {
        put_node
            .put("admitted-before-shutdown".to_string(), b"value".to_vec())
            .await
    });
    common::wait_for(
        || async { store.entered.load(Ordering::SeqCst) },
        Duration::from_secs(5),
    )
    .await;

    let shutdown_node = node.clone();
    let shutdown = tokio::spawn(async move { shutdown_node.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match node.health().await {
                Err(NodeError::ShuttingDown) => break,
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected health error: {error}"),
            }
        }
    })
    .await
    .expect("shutdown did not close public operation admission");
    assert!(!shutdown.is_finished(), "shutdown skipped an admitted put");

    assert!(matches!(
        node.connect("127.0.0.1:9".parse().unwrap()).await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.put("rejected-put".to_string(), b"value".to_vec())
            .await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.put_with_priority(
            "rejected-priority-put".to_string(),
            b"value".to_vec(),
            OpPriority::High,
        )
        .await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.put_with_ttl(
            "rejected-ttl-put".to_string(),
            b"value".to_vec(),
            Some(Duration::from_secs(1)),
        )
        .await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.delete("rejected-delete".to_string()).await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.get("rejected-get".to_string()).await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(
        node.get_with_priority("rejected-priority-get".to_string(), OpPriority::High)
            .await,
        Err(NodeError::ShuttingDown)
    ));
    assert!(matches!(node.get_all().await, Err(NodeError::ShuttingDown)));

    let put_results = node
        .put_multi(vec![("rejected-batch-put".to_string(), b"value".to_vec())])
        .await;
    assert!(matches!(
        put_results.as_slice(),
        [Err(NodeError::ShuttingDown)]
    ));
    let priority_put_results = node
        .put_multi_with_priority(
            vec![("rejected-priority-batch-put".to_string(), b"value".to_vec())],
            OpPriority::High,
        )
        .await;
    assert!(matches!(
        priority_put_results.as_slice(),
        [Err(NodeError::ShuttingDown)]
    ));
    let get_results = node.get_multi(vec!["rejected-batch-get".to_string()]).await;
    assert!(matches!(
        get_results.as_slice(),
        [Err(NodeError::ShuttingDown)]
    ));
    let priority_get_results = node
        .get_multi_with_priority(
            vec!["rejected-priority-batch-get".to_string()],
            OpPriority::High,
        )
        .await;
    assert!(matches!(
        priority_get_results.as_slice(),
        [Err(NodeError::ShuttingDown)]
    ));

    store.release();
    put.await.unwrap().unwrap();
    shutdown.await.unwrap().unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_materializes_expiry_before_the_final_flush() {
    let store = Arc::new(PreloadedStore::default());
    let node = Node::start(
        "shutdown-expiry".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    node.put_with_ttl(
        "expires-at-shutdown".to_string(),
        b"short-lived".to_vec(),
        Some(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    node.shutdown().await.unwrap();
    let persisted = store
        .get_fragment_zero("expires-at-shutdown")
        .expect("shutdown did not persist expiry tombstone");
    assert!(persisted.is_tombstone);
    assert!(persisted.value.is_empty());
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn memory_store_exercises_persistence_plumbing() {
    // The MemoryStore test double runs the full store path (load,
    // buffer, batch flush, shutdown flush) without a database.
    use actordb::store::MemoryStore;
    use std::sync::Arc;

    let config = NodeConfig {
        store: Some(Arc::new(MemoryStore)),
        flush_batch_size: 1,
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("node failed to start");
    a.put("k".to_string(), b"v".to_vec()).await.unwrap();
    let got = a.get("k".to_string()).await.unwrap().expect("k missing");
    assert_eq!(got.value, b"v");
    a.shutdown().await.unwrap();
}

#[tokio::test]
async fn ttl_expiry_is_persisted_as_a_versioned_tombstone() {
    let store = Arc::new(PreloadedStore::default());
    let config = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: 1,
        clean_interval: Duration::from_millis(10),
        ..NodeConfig::default()
    };
    let node = Node::start(
        "ttl-durable".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .expect("node failed to start");

    node.put_with_ttl(
        "ephemeral".to_string(),
        b"value".to_vec(),
        Some(Duration::from_millis(40)),
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            store
                .get_fragment_zero("ephemeral")
                .is_some_and(|value| value.is_tombstone)
        },
        Duration::from_secs(5),
    )
    .await;

    let marker = store.get_fragment_zero("ephemeral").unwrap();
    assert!(marker.is_tombstone);
    assert!(marker.expires_at_ms.is_some());
    assert!(node.get("ephemeral".to_string()).await.unwrap().is_none());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn ttl_expiry_does_not_resurrect_an_older_partitioned_value() {
    let config = NodeConfig {
        clean_interval: Duration::from_millis(10),
        ..NodeConfig::default()
    };
    let a = Node::start("ttl-a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("node a failed to start");
    let b = test_node("ttl-b").await;

    // b's disconnected value predates a's TTL write. Once a's value expires,
    // its marker must still beat b's old value when the partition heals.
    b.put("ttl-race".to_string(), b"old".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    a.put_with_ttl(
        "ttl-race".to_string(),
        b"temporary".to_vec(),
        Some(Duration::from_millis(40)),
    )
    .await
    .unwrap();

    common::wait_for(
        || async { a.get("ttl-race".to_string()).await.unwrap().is_none() },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        b.get("ttl-race".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"old".to_vec())
    );

    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    common::wait_for(
        || async {
            a.get("ttl-race".to_string()).await.unwrap().is_none()
                && b.get("ttl-race".to_string()).await.unwrap().is_none()
        },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_rewrites_expired_durable_values_as_tombstones() {
    let store = Arc::new(PreloadedStore::default());
    let timestamp = now_ms().saturating_sub(100);
    let mut expired = VersionedValue::new(b"stale".to_vec(), timestamp, "old".to_string());
    expired.expires_at_ms = Some(now_ms().saturating_sub(1));
    store.insert_fragment_zero("expired-at-boot", expired.clone());
    let config = NodeConfig {
        store: Some(store.clone()),
        ..NodeConfig::default()
    };

    let node = Node::start(
        "ttl-restart".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .expect("node failed to start");
    assert!(node
        .get("expired-at-boot".to_string())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store.get_fragment_zero("expired-at-boot"),
        Some(expired.expiry_tombstone())
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn persisted_version_seeds_clock_before_first_write() {
    let store = Arc::new(PreloadedStore::default());
    let persisted_timestamp = now_ms() + 60_000;
    store.insert_fragment_zero(
        "key",
        VersionedValue::new_version(
            b"persisted".to_vec(),
            actordb::actor::RecordVersion::new(persisted_timestamp, 1_000_000),
            "previous-boot".to_string(),
        ),
    );
    let config = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: 1,
        ..NodeConfig::default()
    };

    let node = Node::start(
        "node".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
    )
    .await
    .expect("node failed to start from an in-policy durable timestamp");
    node.put("key".to_string(), b"new".to_vec())
        .await
        .expect("first post-restart write was not applied");

    let current = node
        .get("key".to_string())
        .await
        .unwrap()
        .expect("key missing");
    assert_eq!(current.value, b"new");
    assert_eq!(
        current.version(),
        actordb::actor::RecordVersion::new(persisted_timestamp, 1_000_001)
    );
    node.shutdown().await.unwrap();
    let restarted = Node::start("node".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    restarted
        .put("key".into(), b"after-restart".to_vec())
        .await
        .unwrap();
    let next = restarted.get("key".into()).await.unwrap().unwrap();
    assert!(next.version() > current.version());
    assert_eq!(next.timestamp_ms, persisted_timestamp);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_rejects_out_of_policy_persisted_timestamp() {
    let store = Arc::new(PreloadedStore::default());
    store.insert_fragment_zero(
        "poisoned",
        VersionedValue::new(b"bad".to_vec(), u64::MAX, "old-peer".to_string()),
    );
    let config = NodeConfig {
        store: Some(store),
        ..NodeConfig::default()
    };

    match Node::start("node".to_string(), "127.0.0.1:0".parse().unwrap(), config).await {
        Ok(node) => {
            node.shutdown().await.unwrap();
            panic!("node accepted a poisoned durable timestamp");
        }
        Err(NodeError::Storage(error)) => assert!(
            error.contains("exceeds maximum allowed"),
            "unexpected storage startup error: {error}"
        ),
        Err(error) => panic!("durable timestamp failure was not classified as Storage: {error}"),
    }
}

#[tokio::test]
async fn startup_rejects_invalid_persisted_provenance() {
    for node_id in [String::new(), "x".repeat(256)] {
        let store = Arc::new(PreloadedStore::default());
        store.insert_fragment_zero("poisoned", VersionedValue::new(b"bad".to_vec(), 1, node_id));
        let config = NodeConfig {
            store: Some(store),
            ..NodeConfig::default()
        };

        match Node::start("node".to_string(), "127.0.0.1:0".parse().unwrap(), config).await {
            Ok(node) => {
                node.shutdown().await.unwrap();
                panic!("node accepted invalid durable provenance");
            }
            Err(NodeError::Storage(error)) => assert!(
                error.contains("record node id is"),
                "unexpected storage startup error: {error}"
            ),
            Err(error) => {
                panic!("durable provenance failure was not classified as Storage: {error}")
            }
        }
    }
}

#[tokio::test]
async fn fragments_route_and_isolate() {
    // Phase 1: one node, 4 fragments. Keys hash to their fragments;
    // every key stays retrievable and get_all sees the union.
    use actordb::shard::fragment_of;
    use std::collections::HashSet;

    let config = NodeConfig {
        fragment_count: 4,
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .expect("node failed to start");

    let mut frags_seen = HashSet::new();
    for i in 0..50 {
        let key = format!("key-{i}");
        frags_seen.insert(fragment_of(&key, 4).0);
        a.put(key.clone(), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    // 50 keys over 4 fragments: all fragments should be hit.
    assert_eq!(frags_seen.len(), 4, "not all fragments hit");

    for i in 0..50 {
        let key = format!("key-{i}");
        let got = a.get(key).await.unwrap().expect("key missing");
        assert_eq!(got.value, format!("v{i}").into_bytes());
    }

    let all = a.get_all().await.unwrap();
    assert_eq!(all.len(), 50);
    a.shutdown().await.unwrap();
}

/// Phase 2: disjoint fragments. Each node serves 2 of 6 fragments.
/// A put for a key owned by another node routes there in one hop.
#[tokio::test]
async fn disjoint_fragments_route_to_owner() {
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;

    let frag_count: usize = 6;
    // Node a: frags 0,1. Node b: frags 2,3. Node c: frags 4,5.
    let mut fmap: HashMap<FragmentId, Vec<String>> = HashMap::new();
    for i in 0..frag_count {
        let owner = match i as u32 {
            0 | 1 => "a",
            2 | 3 => "b",
            _ => "c",
        };
        fmap.insert(FragmentId(i as u32), vec![owner.to_string()]);
    }

    let mk = || NodeConfig {
        fragment_count: frag_count,
        fragment_map: fmap.clone(),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    let c = Node::start("c".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    // Full mesh (nodes started without members; connect manually).
    a.connect(b.addr()).await.unwrap();
    a.connect(c.addr()).await.unwrap();
    b.connect(c.addr()).await.unwrap();
    common::wait_for(
        || async {
            a.peer_count().await == 2 && b.peer_count().await == 2 && c.peer_count().await == 2
        },
        Duration::from_secs(5),
    )
    .await;

    // Find a key owned by c (frags 4,5), put it via a.
    let key_c = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| matches!(fragment_of(k, frag_count).0, 4 | 5))
        .unwrap();
    a.put(key_c.clone(), b"via-a".to_vec()).await.unwrap();

    // c (the owner) has it; a does NOT (it doesn't serve that fragment).
    common::wait_for(
        || async {
            c.get(key_c.clone()).await.unwrap().map(|v| v.value) == Some(b"via-a".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;
    // a doesn't hold it locally (not the owner)...
    assert!(!a.get_all().await.unwrap().contains_key(&key_c));

    // ...but a can READ it via forwarded get.
    let v = a.get(key_c.clone()).await.unwrap().unwrap();
    assert_eq!(v.value, b"via-a".to_vec());

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

/// Phase 2: replicated fragment. A put on one replica fans out to the
/// other holder.
#[tokio::test]
async fn replica_fanout_reaches_other_holder() {
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;

    let frag_count: usize = 4;
    let mut fmap: HashMap<FragmentId, Vec<String>> = HashMap::new();
    // Frag 0 replicated on a and b; others single-homed.
    fmap.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
    fmap.insert(FragmentId(1), vec!["a".to_string()]);
    fmap.insert(FragmentId(2), vec!["b".to_string()]);
    fmap.insert(FragmentId(3), vec!["c".to_string()]);

    let mk = || NodeConfig {
        fragment_count: frag_count,
        fragment_map: fmap.clone(),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    a.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    // Key in frag 0, put via a. Both replicas should converge.
    let key = (0..1000)
        .map(|i| format!("r{i}"))
        .find(|k| fragment_of(k, frag_count) == FragmentId(0))
        .unwrap();
    a.put(key.clone(), b"rep".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get(key.clone()).await.unwrap().map(|v| v.value) == Some(b"rep".to_vec()) },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Phase 3: the worker pool handles concurrent keys; per-key ordering
/// is preserved by sticky dispatch.
#[tokio::test]
async fn worker_pool_concurrent_keys_and_ordering() {
    use std::sync::Arc;

    let config = NodeConfig {
        default_pool: actordb::actor::PoolConfig {
            workers: 8,
            ..Default::default()
        },
        ..NodeConfig::default()
    };
    let a = Arc::new(
        Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap(),
    );

    // 50 concurrent puts to distinct keys.
    let mut handles = Vec::new();
    for i in 0..50 {
        let a_clone = a.clone();
        handles.push(tokio::spawn(async move {
            a_clone
                .put(format!("ck-{i}"), format!("v{i}").into_bytes())
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    for i in 0..50 {
        let got = a.get(format!("ck-{i}")).await.unwrap().unwrap();
        assert_eq!(got.value, format!("v{i}").into_bytes());
    }

    // Sequential puts to one key land in order (distinct timestamps).
    for i in 0..10 {
        a.put("ordered".to_string(), format!("w{i}").into_bytes())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let got = a.get("ordered".to_string()).await.unwrap().unwrap();
    assert_eq!(got.value, b"w9".to_vec());

    a.shutdown().await.unwrap();
}

/// Phase 4: TTL expiry — a key with a TTL disappears without manual
/// intervention (reads hide it past expiry; the cleaner materializes its
/// tombstone).
#[tokio::test]
async fn ttl_expiry_removes_key() {
    let store = Arc::new(PreloadedStore::default());
    let config = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: 1,
        clean_interval: Duration::from_millis(50),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    // A non-TTL key must survive the same automatic maintenance pass.
    a.put("perm".to_string(), b"forever".to_vec())
        .await
        .unwrap();
    let result = a
        .put_with_ttl(
            "temp".to_string(),
            b"data".to_vec(),
            Some(Duration::from_millis(100)),
        )
        .await;
    // Admission may legitimately finish after the TTL on a busy runner.
    assert!(
        matches!(&result, Ok(()) | Err(NodeError::Expired)),
        "unexpected TTL put result: {result:?}"
    );
    // Public get_all hides tombstones. Inspect the actual persisted marker so
    // synthesized read-time expiry alone cannot satisfy this condition.
    common::wait_for(
        || async {
            store
                .get_fragment_zero("temp")
                .is_some_and(|value| value.is_tombstone)
                && a.get("temp".to_string()).await.unwrap().is_none()
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        a.get("perm".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"forever".to_vec())
    );

    a.shutdown().await.unwrap();
}

/// Phase 4: killing a worker triggers a supervised restart; the
/// fragment keeps serving and sibling fragments are unaffected.
#[tokio::test]
async fn worker_kill_triggers_supervised_restart() {
    use ractor::registry::where_is;

    let config = NodeConfig {
        fragment_count: 2,
        default_pool: actordb::actor::PoolConfig {
            workers: 2,
            ..Default::default()
        },
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    // Find a key on fragment 0, worker 0.
    let mut target_key = String::new();
    for i in 0..100 {
        let k = format!("wkill-{i}");
        if actordb::shard::fragment_of(&k, 2) == actordb::shard::FragmentId(0)
            && actordb::actor::worker_idx(&k, 2) == 0
        {
            target_key = k;
            break;
        }
    }
    assert!(!target_key.is_empty());

    a.put(target_key.clone(), b"v1".to_vec()).await.unwrap();
    assert!(a.get(target_key.clone()).await.unwrap().is_some());

    // Kill default worker 0 of fragment 0, matching the target key's route.
    let health = a.health().await.unwrap();
    let frag0_health = &health[&actordb::shard::FragmentId(0)];
    let name: ractor::ActorName = frag0_health
        .worker_names
        .iter()
        .find(|name| name.ends_with("-default-worker-0"))
        .expect("default worker 0 should be running")
        .clone();
    let worker = where_is(name.clone()).expect("worker should be registered");
    let killed_id = worker.get_id();
    worker.kill();

    common::wait_for(
        || async {
            where_is(name.clone()).is_some_and(|replacement| {
                replacement.get_id() != killed_id
                    && replacement.get_status() == ractor::ActorStatus::Running
            })
        },
        Duration::from_secs(5),
    )
    .await;

    // Fragment 0 still serves (new worker in place).
    a.put(target_key.clone(), b"v2".to_vec()).await.unwrap();
    let got = a.get(target_key.clone()).await.unwrap().unwrap();
    assert_eq!(got.value, b"v2".to_vec());

    // Sibling fragment (1) unaffected.
    let mut frag1_key = String::new();
    for i in 0..100 {
        let k = format!("sib-{i}");
        if actordb::shard::fragment_of(&k, 2) == actordb::shard::FragmentId(1) {
            frag1_key = k;
            break;
        }
    }
    a.put(frag1_key.clone(), b"sib".to_vec()).await.unwrap();
    assert!(a.get(frag1_key).await.unwrap().is_some());

    // Health reports the fragment alive.
    let health = a.health().await.unwrap();
    assert!(health.contains_key(&actordb::shard::FragmentId(0)));

    a.shutdown().await.unwrap();
}

/// Tombstones: delete hides the key locally and the tombstone
/// propagates to replicas via streaming replication and reconciliation.
#[tokio::test]
async fn delete_propagates_to_replicas() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("gone".to_string(), b"v".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get("gone".to_string()).await.unwrap().is_some() },
        Duration::from_secs(5),
    )
    .await;

    b.delete("gone".to_string()).await.unwrap();
    // Deleted on the deleter immediately...
    assert!(b.get("gone".to_string()).await.unwrap().is_none());
    // ...and on the replica once the tombstone replicates.
    common::wait_for(
        || async { a.get("gone".to_string()).await.unwrap().is_none() },
        Duration::from_secs(5),
    )
    .await;
    // Tombstones are hidden from get_all too.
    assert!(!a.get_all().await.unwrap().contains_key("gone"));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Tombstones: a delete survives node restart (tombstone is durable)
/// and a late joiner learns the delete via immediate reconciliation.
#[tokio::test]
async fn delete_survives_restart_and_rejoin() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("k".to_string(), b"v".to_vec()).await.unwrap();
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().is_some() },
        Duration::from_secs(5),
    )
    .await;
    a.delete("k".to_string()).await.unwrap();
    common::wait_for(
        || async { b.get("k".to_string()).await.unwrap().is_none() },
        Duration::from_secs(5),
    )
    .await;

    // A fresh node joining after the delete learns the tombstone from
    // sync and does not resurrect the key.
    let c = test_node("c").await;
    c.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { c.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.get("k".to_string()).await.unwrap().is_none());

    // A put after the delete resurrects the key with a higher timestamp.
    b.put("k".to_string(), b"v2".to_vec()).await.unwrap();
    common::wait_for(
        || async { a.get("k".to_string()).await.unwrap().map(|v| v.value) == Some(b"v2".to_vec()) },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

/// HLC (#14): one node with a +5min clock skew must not permanently
/// shadow the slow node's writes. After observing the fast node's
/// timestamp the slow node's HLC jumps forward, so its next write
/// wins everywhere. With wall-clock LWW this test fails: the fast
/// node's write would win forever.
#[tokio::test]
async fn hlc_orders_writes_under_clock_skew() {
    let skewed_config = NodeConfig {
        clock_skew_ms: 300_000, // +5 minutes
        ..NodeConfig::default()
    };
    let fast = Node::start(
        "fast".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        skewed_config,
    )
    .await
    .unwrap();
    let slow = test_node("slow").await;
    slow.connect(fast.addr()).await.unwrap();
    wait_for_peers(&slow, &fast).await;

    // Slow node writes first.
    slow.put("sk".to_string(), b"slow-1".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            fast.get("sk".to_string()).await.unwrap().map(|v| v.value) == Some(b"slow-1".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    // Fast node (clock +5min) overwrites: its timestamp is huge, so it
    // wins — correctly, since it really is the later write.
    fast.put("sk".to_string(), b"fast-1".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            slow.get("sk".to_string()).await.unwrap().map(|v| v.value) == Some(b"fast-1".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    // Slow node writes again. Its HLC has observed fast's timestamp,
    // so this write's timestamp exceeds it and wins on both nodes.
    slow.put("sk".to_string(), b"slow-2".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            slow.get("sk".to_string()).await.unwrap().map(|v| v.value) == Some(b"slow-2".to_vec())
                && fast.get("sk".to_string()).await.unwrap().map(|v| v.value)
                    == Some(b"slow-2".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    slow.shutdown().await.unwrap();
    fast.shutdown().await.unwrap();
}

/// Phase 5: kill a peer mid-replication. The sender's buffer overflows,
/// it enters `buffer_reset`, and on reconnect it reconciles — no
/// divergence.
#[tokio::test]
async fn sender_buffer_reset_and_reconcile() {
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let fragment_map = HashMap::from([(FragmentId(0), vec!["a".to_string(), "b".to_string()])]);
    let config = NodeConfig {
        fragment_map: fragment_map.clone(),
        replication: SenderConfig {
            batch_size: 5,
            batch_interval: Duration::from_millis(50),
            max_in_flight: 2,
            buffer_capacity: 10, // small: overflow quickly
            consistency_interval: Duration::from_secs(3600), // disable periodic
            reconcile_timeout: Duration::from_secs(30),
            required_ack_durability: actordb::replication::ReplicationAckDurability::MemoryApplied,
        },
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let b = Node::start(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map: fragment_map.clone(),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Sanity: replication works while connected.
    a.put("k0".to_string(), b"v0".to_vec()).await.unwrap();
    common::wait_for(
        || async {
            b.get("k0".to_string()).await.unwrap().map(|v| v.value) == Some(b"v0".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    // Kill b. a's sender buffers; overflow → buffer_reset.
    b.shutdown().await.unwrap();
    for i in 1..30 {
        a.put(format!("k{i}"), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    // The sender should have entered buffer_reset (buffer shed).
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "b")
                .await
                .map(|r| r.state == "buffer_reset")
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    // Restart b and reconnect. Reconciliation should converge everything.
    let b2 = Node::start(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    b2.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async {
            let all = b2.get_all().await.unwrap();
            (1..30).all(|i| {
                all.get(&format!("k{i}")).map(|v| v.value.clone())
                    == Some(format!("v{i}").into_bytes())
            })
        },
        Duration::from_secs(10),
    )
    .await;

    a.shutdown().await.unwrap();
    b2.shutdown().await.unwrap();
}

/// A live transport must recover immediately after sender-buffer overflow.
/// The old implementation entered `buffer_reset` but only started
/// reconciliation after a future reconnect, so a static topology diverged
/// forever while both peers remained healthy.
#[tokio::test]
async fn live_connected_overflow_reconciles_without_reconnect() {
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        replication: SenderConfig {
            batch_size: 100,
            batch_interval: Duration::from_secs(3600),
            max_in_flight: 1,
            buffer_capacity: 2,
            consistency_interval: Duration::from_secs(3600),
            reconcile_timeout: Duration::from_secs(30),
            required_ack_durability: actordb::replication::ReplicationAckDurability::MemoryApplied,
        },
        ..NodeConfig::default()
    };
    let a = Node::start(
        "live-reset-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    let b = test_node("live-reset-b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    let original_session = a
        .sender_report(FragmentId(0), "live-reset-b")
        .await
        .expect("sender should exist")
        .session_id;
    assert_ne!(original_session, 0);

    let writes: Vec<_> = (0..30)
        .map(|i| (format!("live-reset-{i}"), format!("value-{i}").into_bytes()))
        .collect();
    let results = a.put_multi(writes.clone()).await;
    assert!(results.iter().all(Result::is_ok));

    common::wait_for(
        || async {
            let report = a.sender_report(FragmentId(0), "live-reset-b").await;
            let all = b.get_all().await.unwrap();
            let converged = writes.iter().all(|(key, value)| {
                all.get(key).map(|v| v.value.as_slice()) == Some(value.as_slice())
            });
            report
                .map(|r| {
                    r.state == "connected"
                        && r.reconcile_rounds >= 1
                        && r.session_id != original_session
                        && converged
                })
                .unwrap_or(false)
        },
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(a.peer_count().await, 1);
    assert_eq!(b.peer_count().await, 1);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Unchanged dynamic-placement polls reuse the current peer channel. They
/// must not mint a new replication session and invalidate slow in-flight
/// batches on every poll.
#[tokio::test]
async fn unchanged_placement_polls_preserve_sender_session() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut placement = HashMap::new();
    placement.insert(FragmentId(0), vec!["stable-a".to_string()]);
    let controller = Arc::new(ManualPlacement::new(placement));
    let make_config = || NodeConfig {
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(25),
        replication: SenderConfig {
            consistency_interval: Duration::from_secs(3600),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let a = Node::start(
        "stable-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        make_config(),
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            a.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["stable-a".to_string(), "stable-b".to_string()],
    )]));
    let b = Node::start(
        "stable-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        make_config(),
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    common::wait_for(
        || async {
            b.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "stable-b")
                .await
                .map(|report| report.session_id != 0)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;
    let session = a
        .sender_report(FragmentId(0), "stable-b")
        .await
        .unwrap()
        .session_id;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        a.sender_report(FragmentId(0), "stable-b")
            .await
            .unwrap()
            .session_id,
        session,
        "steady placement polls rotated a live sender session"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// A response from an older or fabricated reconcile round must not complete
/// a newer buffer reset. Completion requires the exact round and a tracked
/// ACK fence after outbound repair frames.
#[tokio::test]
async fn stale_reconcile_round_cannot_complete_live_reset() {
    use actordb::proto::{BatchAck, Envelope, EnvelopeMsg, KeySummary, ReplicationAckLevel};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec!["reset-sender".to_string(), "raw-reset-peer".to_string()],
    );
    let node = Node::start(
        "reset-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(30),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-reset-peer").await;

    // Drain handshake frames through the initial SessionStart. Protocol v3
    // also starts a routine reconciliation immediately; after the overflow
    // below, wait specifically for the request on the rotated reset session.
    let initial_session_id = loop {
        if let Some(EnvelopeMsg::SessionStart(start)) = recv_frame(&mut stream).await.msg {
            break start.session_id;
        }
    };

    for i in 0..4 {
        node.put(
            format!("round-key-{i}"),
            format!("round-value-{i}").into_bytes(),
        )
        .await
        .unwrap();
    }

    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.session_id != initial_session_id {
                break request;
            }
        }
    };
    let session_id = request.session_id;
    let round_id = request.round_id;
    assert_ne!(session_id, 0);
    assert_ne!(round_id, 0);

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse({
                // If the stale round were accepted, this remote-only winner
                // would immediately produce a ReconcileFetch. The valid
                // response below is deliberately different.
                let summaries = vec![KeySummary {
                    content_digest: content_digest(b"", 0),
                    key: "stale-round-only".to_string(),
                    // A stale frame must be discarded before HLC validation;
                    // this value would close the connection if observed.
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "raw-reset-peer".to_string(),
                    is_tombstone: false,
                }];
                let mut response = complete_reconcile_response(&request, summaries);
                response.round_id = round_id.saturating_add(1);
                response
            })),
        },
    )
    .await;
    let quiet_deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        match tokio::time::timeout_at(quiet_deadline, recv_frame(&mut stream)).await {
            Err(_) => break,
            Ok(Envelope {
                msg: Some(EnvelopeMsg::SessionStart(start)),
            }) if start.session_id == session_id && start.fragment == 0 => {
                // The reset SessionStart and first reconciliation request use
                // separate transport queues, so the already-enqueued start
                // may be observed second. It is not a reaction to the stale
                // response; any other protocol output still fails the test.
            }
            Ok(unexpected) => {
                panic!("stale response triggered protocol output: {unexpected:?}")
            }
        }
    }
    let report = node
        .sender_report(FragmentId(0), "raw-reset-peer")
        .await
        .unwrap();
    assert_eq!(report.state, "buffer_reset");
    assert_eq!(report.reconcile_rounds, 0);

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &request,
                Vec::new(),
            ))),
        },
    )
    .await;

    let (repair_summaries, barrier) = recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(
        !repair_summaries.is_empty(),
        "expected local winners before the barrier"
    );
    assert!(barrier.ops.is_empty(), "completion fence must be empty");
    assert_eq!(barrier.session_id, session_id);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id,
                batch_seq: barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;

    // The first snapshot started at the initial overflow. Later Enqueue
    // messages advanced the reset epoch, so even a valid barrier cannot clear
    // the guard; the sender must run a second correlated round.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let report = node
        .sender_report(FragmentId(0), "raw-reset-peer")
        .await
        .unwrap();
    assert_eq!(report.state, "buffer_reset");
    assert_eq!(report.reconcile_rounds, 0);

    let second_request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            assert_eq!(request.session_id, session_id);
            break request;
        }
    };
    let second_round_id = second_request.round_id;
    assert_ne!(second_round_id, round_id);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &second_request,
                repair_summaries,
            ))),
        },
    )
    .await;
    let (verification_repairs, second_barrier) =
        recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(verification_repairs.is_empty());
    assert!(second_barrier.ops.is_empty());
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id,
                batch_seq: second_barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;

    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-reset-peer")
                .await
                .map(|report| report.state == "connected" && report.reconcile_rounds == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// A bootstrap round that fetched remote winners has only fenced those
/// writes when its first barrier is acknowledged. It must run one fresh
/// no-fetch comparison before placement may advertise the fragment as
/// Serving.
#[tokio::test]
async fn placement_bootstrap_fetch_requires_verification_round() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, KeySummary, ReplicationAckLevel, SyncEntry,
    };
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["raw-bootstrap-source".to_string()],
    )])));
    let node = Node::start(
        "bootstrap-target".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            placement_controller: Some(controller.clone()),
            placement_poll_interval: Duration::from_millis(25),
            replication: SenderConfig {
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                // This node is deliberately storeless. StoreCommitted is a
                // requirement on remote batch receivers; fetching a winner
                // locally must remain a valid in-memory bootstrap.
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
                ..SenderConfig::default()
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-bootstrap-source").await;

    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec![
            "bootstrap-target".to_string(),
            "raw-bootstrap-source".to_string(),
        ],
    )]));

    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    assert_eq!(
        node.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Bootstrapping)
    );

    let summary = KeySummary {
        content_digest: content_digest(b"fetched", 0),
        key: "bootstrap-fetched".to_string(),
        timestamp_ms: now_ms(),
        logical_counter: 0,
        node_id: "raw-bootstrap-source".to_string(),
        is_tombstone: false,
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &first,
                vec![summary.clone()],
            ))),
        },
    )
    .await;
    let fetch = loop {
        if let Some(EnvelopeMsg::ReconcileFetch(fetch)) = recv_frame(&mut stream).await.msg {
            break fetch;
        }
    };
    assert_eq!(fetch.keys, vec![summary.key.clone()]);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(complete_reconcile_data(
                &fetch,
                vec![SyncEntry {
                    key: summary.key.clone(),
                    value: b"fetched".to_vec(),
                    timestamp_ms: summary.timestamp_ms,
                    logical_counter: summary.logical_counter,
                    node_id: summary.node_id.clone(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
            ))),
        },
    )
    .await;
    let first_barrier = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: first.session_id,
                batch_seq: first_barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::StoreCommitted as i32,
            })),
        },
    )
    .await;

    let second = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    assert_ne!(second.round_id, first.round_id);
    assert_eq!(
        node.sender_report(FragmentId(0), "raw-bootstrap-source")
            .await
            .unwrap()
            .reconcile_rounds,
        0,
        "the fetched-data barrier was reported as completed bootstrap"
    );
    assert_eq!(
        node.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Bootstrapping)
    );

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &second,
                vec![summary],
            ))),
        },
    )
    .await;
    let second_barrier = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: second.session_id,
                batch_seq: second_barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::StoreCommitted as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
                && node
                    .sender_report(FragmentId(0), "raw-bootstrap-source")
                    .await
                    .map(|report| report.reconcile_rounds == 1)
                    .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// Reconciliation liveness is independent of the normal batch timer. A live
/// peer that silently withholds a phase response must cause a session rotation
/// and a fresh correlated round within the configured phase timeout.
#[tokio::test]
async fn silent_reconcile_phase_times_out_and_retries() {
    use actordb::proto::EnvelopeMsg;
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec!["timeout-sender".to_string(), "raw-timeout-peer".to_string()],
    );
    let node = Node::start(
        "timeout-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_millis(100),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-timeout-peer").await;
    let initial_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            break session.session_id;
        }
    };

    node.put("timeout-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("timeout-b".to_string(), b"b".to_vec())
        .await
        .unwrap();

    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    assert_eq!(first.session_id, initial_session);
    let rotated_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            if session.session_id != first.session_id {
                break session.session_id;
            }
        }
    };
    let retry = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.session_id == rotated_session {
                break request;
            }
        }
    };
    assert_ne!(retry.round_id, first.round_id);
    assert_eq!(retry.fragment, 0);
    assert_eq!(
        node.sender_report(FragmentId(0), "raw-timeout-peer")
            .await
            .unwrap()
            .state,
        "buffer_reset"
    );

    node.shutdown().await.unwrap();
}

/// Reset completion is fenced by the fragment-wide mutation revision, not
/// only by local sender Enqueue events. An inbound replicated write racing
/// after the snapshot must force another verification round.
#[tokio::test]
async fn inbound_mutation_after_snapshot_invalidates_reset_completion() {
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel,
    };
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec![
            "revision-sender".to_string(),
            "raw-revision-peer".to_string(),
        ],
    );
    let node = Node::start(
        "revision-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-revision-peer").await;
    let initial_session_id = loop {
        if let Some(EnvelopeMsg::SessionStart(start)) = recv_frame(&mut stream).await.msg {
            break start.session_id;
        }
    };

    node.put("revision-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("revision-b".to_string(), b"b".to_vec())
        .await
        .unwrap();
    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.session_id != initial_session_id {
                break request;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &request,
                Vec::new(),
            ))),
        },
    )
    .await;
    let barrier = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };

    // Mutate the fragment through the inbound replication path. That path
    // does not enqueue into this outbound sender, so only the fragment
    // revision fence can detect the race.
    let inbound_session = 4242;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(actordb::proto::SessionStart {
                session_id: inbound_session,
                fragment: 0,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: inbound_session,
                batch_seq: 7,
                fragment: 0,
                ops: vec![ReplicateOp {
                    key: "inbound-race".to_string(),
                    value: b"new".to_vec(),
                    timestamp_ms: now_ms(),
                    logical_counter: 0,
                    node_id: "raw-revision-peer".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id,
                batch_seq: 7,
                ..
            })) if session_id == inbound_session
        ) {
            break;
        }
    }

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: request.session_id,
                batch_seq: barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    let verification = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(next)) = recv_frame(&mut stream).await.msg {
            if next.session_id == request.session_id && next.round_id != request.round_id {
                break next;
            }
        }
    };
    assert_ne!(verification.round_id, request.round_id);
    let report = node
        .sender_report(FragmentId(0), "raw-revision-peer")
        .await
        .unwrap();
    assert_eq!(report.state, "buffer_reset");
    assert_eq!(report.reconcile_rounds, 0);

    node.shutdown().await.unwrap();
}

/// Fetched reconciliation data is an exact response contract: a prematurely
/// completed page retries the round, while an unrequested entry is a protocol
/// violation that closes the connection. Neither can be partially applied and
/// then certified by a barrier.
#[tokio::test]
async fn malformed_reconcile_data_is_rejected_without_partial_apply() {
    use actordb::proto::{Envelope, EnvelopeMsg, KeySummary, SyncEntry};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec!["data-sender".to_string(), "raw-data-peer".to_string()],
    );
    let node = Node::start(
        "data-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-data-peer").await;
    let initial_session_id = loop {
        if let Some(EnvelopeMsg::SessionStart(start)) = recv_frame(&mut stream).await.msg {
            break start.session_id;
        }
    };
    node.put("data-a".to_string(), b"a".to_vec()).await.unwrap();
    node.put("data-b".to_string(), b"b".to_vec()).await.unwrap();

    let remote_timestamp = now_ms();
    let summary = KeySummary {
        content_digest: content_digest(b"", 0),
        key: "remote-fetch".to_string(),
        timestamp_ms: remote_timestamp,
        logical_counter: 0,
        node_id: "raw-data-peer".to_string(),
        is_tombstone: false,
    };
    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.session_id != initial_session_id {
                break request;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &first,
                vec![summary.clone()],
            ))),
        },
    )
    .await;
    let first_fetch = loop {
        if let Some(EnvelopeMsg::ReconcileFetch(fetch)) = recv_frame(&mut stream).await.msg {
            break fetch;
        }
    };
    assert_eq!(first_fetch.keys, ["remote-fetch"]);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(complete_reconcile_data(
                &first_fetch,
                Vec::new(),
            ))),
        },
    )
    .await;

    let second = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.round_id != first.round_id {
                break request;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &second,
                vec![summary],
            ))),
        },
    )
    .await;
    let second_fetch = loop {
        if let Some(EnvelopeMsg::ReconcileFetch(fetch)) = recv_frame(&mut stream).await.msg {
            break fetch;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(complete_reconcile_data(
                &second_fetch,
                vec![SyncEntry {
                    key: "unrequested-key".to_string(),
                    value: b"bad".to_vec(),
                    // Malformed/unrequested data is rejected before HLC
                    // observation, so it cannot poison or tear down the live
                    // connection with an otherwise invalid timestamp.
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "raw-data-peer".to_string(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
            ))),
        },
    )
    .await;
    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut remainder))
        .await
        .expect("protocol-invalid reconcile data did not close the connection")
        .unwrap();
    assert!(node
        .get("remote-fetch".to_string())
        .await
        .unwrap()
        .is_none());
    assert!(node
        .get("unrequested-key".to_string())
        .await
        .unwrap()
        .is_none());
    let report = node
        .sender_report(FragmentId(0), "raw-data-peer")
        .await
        .unwrap();
    assert_eq!(report.state, "buffer_reset");
    assert_eq!(report.reconcile_rounds, 0);

    node.shutdown().await.unwrap();
}

/// Phase 5: batches from a stale session are discarded. We replay a
/// batch with a bogus session id over a raw TCP connection; the
/// receiver must drop it.
#[tokio::test]
async fn stale_session_batches_discarded() {
    use actordb::proto::{Envelope, EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel};

    let b = test_node("b").await;

    // Open a raw peer connection and send a batch without first establishing
    // a fragment session. Using a unique peer identity avoids racing the
    // node's intentional replacement of duplicate peer connections.
    let mut stream = tokio::net::TcpStream::connect(b.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-stale-peer").await;

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: 0xDEADBEEF, // bogus: no such session
                batch_seq: 0,
                fragment: 0,
                ops: vec![ReplicateOp {
                    key: "evil".to_string(),
                    value: b"bad".to_vec(),
                    timestamp_ms: u64::MAX, // would win any LWW...
                    logical_counter: 0,
                    node_id: "raw-stale-peer".to_string(),
                    seq: 999,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;

    // The batch must NOT be applied, despite the huge timestamp.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(b.get("evil".to_string()).await.unwrap().is_none());

    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_start_for_out_of_range_fragment_is_not_admitted() {
    use actordb::proto::{
        EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel, SessionStart,
    };

    let node = test_node("session-bound-receiver").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "unhosted-session-peer").await;
    let initial_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            break session.session_id;
        }
    };
    complete_empty_reconcile(&mut stream, initial_session).await;

    let invalid_fragment = u32::MAX;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id: 77,
                fragment: invalid_fragment,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: 77,
                batch_seq: 1,
                fragment: invalid_fragment,
                ops: vec![ReplicateOp {
                    key: "never-admitted".to_string(),
                    value: b"bad".to_vec(),
                    timestamp_ms: now_ms(),
                    logical_counter: 0,
                    node_id: "unhosted-session-peer".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), recv_frame(&mut stream))
            .await
            .is_err()
    );
    assert!(node
        .get("never-admitted".to_string())
        .await
        .unwrap()
        .is_none());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_started_before_actor_installation_survives_placement_race() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel, SessionStart,
    };
    use actordb::shard::FragmentId;

    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["early-session-peer".to_string()],
    )])));
    let node = Node::start(
        "early-session-target".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            placement_controller: Some(controller.clone()),
            placement_poll_interval: Duration::from_millis(25),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "early-session-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    let session_id = 77;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id,
                fragment: 0,
            })),
        },
    )
    .await;
    let batch = ReplicateBatch {
        session_id,
        batch_seq: 1,
        fragment: 0,
        ops: vec![ReplicateOp {
            key: "early-session-key".to_string(),
            value: b"accepted-after-install".to_vec(),
            timestamp_ms: now_ms(),
            logical_counter: 0,
            node_id: "early-session-peer".to_string(),
            seq: 1,
            boot_id: 1,
            expires_at_ms: 0,
            is_tombstone: false,
        }],
        required_ack_level: ReplicationAckLevel::Applied as i32,
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(batch.clone())),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::BatchNack(nack))
                if nack.session_id == session_id && nack.batch_seq == batch.batch_seq
        ) {
            break;
        }
    }
    assert_eq!(node.peer_count().await, 1);

    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["early-session-target".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    // The receiver retained the bounded session while placement caught up,
    // so replay does not need another SessionStart and succeeds immediately.
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(batch.clone())),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::BatchAck(ack))
                if ack.session_id == session_id && ack.batch_seq == batch.batch_seq
        ) {
            break;
        }
    }
    assert_eq!(
        node.get(batch.ops[0].key.clone())
            .await
            .unwrap()
            .unwrap()
            .value,
        batch.ops[0].value
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_session_rejects_far_future_timestamp() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel, SessionStart,
    };
    let node = test_node("receiver").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-peer").await;
    let frames = [
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                session_id: 42,
                fragment: 0,
            })),
        },
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: 42,
                batch_seq: 1,
                fragment: 0,
                ops: vec![ReplicateOp {
                    key: "poisoned".to_string(),
                    value: b"bad".to_vec(),
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "raw-peer".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    ];
    for frame in frames {
        send_frame(&mut stream, frame).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(node.get("poisoned".to_string()).await.unwrap().is_none());
    node.shutdown().await.unwrap();
}

/// Phase 6: multi-key put/get spanning fragments and nodes.
#[tokio::test]
async fn multi_key_fanout() {
    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Put 20 keys via a; they hash across the (single) fragment and
    // replicate to b via the async sender.
    let kvs: Vec<(String, Vec<u8>)> = (0..20)
        .map(|i| (format!("mk{i}"), format!("mv{i}").into_bytes()))
        .collect();
    let results = a.put_multi(kvs.clone()).await;
    assert!(results.iter().all(|r| r.is_ok()));

    // All keys readable from b via get_multi (tests remote batch path).
    let keys: Vec<String> = kvs.iter().map(|(k, _)| k.clone()).collect();
    common::wait_for(
        || async {
            let res = b.get_multi(keys.clone()).await;
            res.iter().all(|r| matches!(r, Ok(Some(_))))
        },
        Duration::from_secs(5),
    )
    .await;
    let res = b.get_multi(keys.clone()).await;
    for ((k, v), r) in kvs.iter().zip(res) {
        match r {
            Ok(Some(got)) => {
                assert_eq!(&got.value, v, "key {k}");
            }
            other => panic!("key {k}: unexpected {other:?}"),
        }
    }

    // Results are in input order.
    let res = a.get_multi(keys).await;
    assert_eq!(res.len(), 20);

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_batches_split_at_wire_item_limit_without_dropping_connection() {
    use actordb::actor::OpPriority;
    use actordb::shard::FragmentId;

    const ITEM_COUNT: usize = 4097;
    let fragment_map = HashMap::from([(FragmentId(0), vec!["batch-owner".to_string()])]);
    let config = || NodeConfig {
        fragment_count: 1,
        fragment_map: fragment_map.clone(),
        ..NodeConfig::default()
    };
    let owner = test_node_with_config("batch-owner", config()).await;
    let client = Node::start_with_options(
        "batch-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    client.connect(owner.addr()).await.unwrap();
    wait_for_peers(&client, &owner).await;

    let writes: Vec<_> = (0..ITEM_COUNT)
        .map(|index| {
            (
                format!("count-bounded-{index}"),
                index.to_be_bytes().to_vec(),
            )
        })
        .collect();
    let put_results = client
        .put_multi_with_priority(writes.clone(), OpPriority::Default)
        .await;
    assert_eq!(put_results.len(), ITEM_COUNT);
    assert!(
        put_results.iter().all(Result::is_ok),
        "first batch failure: {:?}",
        put_results.iter().find(|result| result.is_err())
    );
    assert_eq!(client.peer_count().await, 1);

    let keys: Vec<_> = writes.iter().map(|(key, _)| key.clone()).collect();
    let get_results = client
        .get_multi_with_priority(keys, OpPriority::Default)
        .await;
    assert_eq!(get_results.len(), ITEM_COUNT);
    for ((_, expected), result) in writes.iter().zip(get_results) {
        assert_eq!(result.unwrap().unwrap().value, *expected);
    }
    assert_eq!(client.peer_count().await, 1);

    client.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_batches_split_at_encoded_byte_limits_without_false_misses() {
    use actordb::actor::OpPriority;
    use actordb::shard::FragmentId;

    let fragment_map = HashMap::from([(FragmentId(0), vec!["byte-owner".to_string()])]);
    let config = || NodeConfig {
        fragment_count: 1,
        fragment_map: fragment_map.clone(),
        ..NodeConfig::default()
    };
    let owner = test_node_with_config("byte-owner", config()).await;
    let client = Node::start_with_options(
        "byte-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    client.connect(owner.addr()).await.unwrap();
    wait_for_peers(&client, &owner).await;

    // Each record fits alone. Combined put requests cross the 1 MiB target,
    // while the first and second get results cross the 16 MiB hard limit and
    // therefore require an ordered prefix pull. Repeating the first key pins
    // positional duplicate handling across pages.
    let small = vec![b's'; 768 * 1024];
    let large = vec![b'l'; 15 * 1024 * 1024 + 512 * 1024];
    let writes = vec![
        ("byte-small".to_string(), small.clone()),
        ("byte-large".to_string(), large.clone()),
    ];
    let put_results = client
        .put_multi_with_priority(writes, OpPriority::Default)
        .await;
    assert!(
        put_results.iter().all(Result::is_ok),
        "byte-bounded put failed: {put_results:?}"
    );
    assert_eq!(client.peer_count().await, 1);

    let get_results = client
        .get_multi_with_priority(
            vec![
                "byte-small".to_string(),
                "byte-large".to_string(),
                "byte-small".to_string(),
                "byte-missing".to_string(),
            ],
            OpPriority::Default,
        )
        .await;
    assert_eq!(get_results.len(), 4);
    assert_eq!(
        get_results[0].as_ref().unwrap().as_ref().unwrap().value,
        small
    );
    assert_eq!(
        get_results[1].as_ref().unwrap().as_ref().unwrap().value,
        large
    );
    assert_eq!(
        get_results[2].as_ref().unwrap().as_ref().unwrap().value,
        get_results[0].as_ref().unwrap().as_ref().unwrap().value
    );
    assert!(get_results[3].as_ref().unwrap().is_none());
    assert_eq!(client.peer_count().await, 1);

    client.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

/// Phase 6: partial failure + replica failover — one replica down,
/// reads fail over to the surviving replica; errors are reported
/// per-key only when all replicas are unreachable.
#[tokio::test]
async fn multi_key_partial_failure() {
    use actordb::shard::{fragment_of, FragmentId};

    // Two fragments; a serves fragment 0, b+c serve fragment 1 (b
    // preferred).
    let mut fragment_map = std::collections::HashMap::new();
    fragment_map.insert(FragmentId(0), vec!["a".to_string()]);
    fragment_map.insert(FragmentId(1), vec!["b".to_string(), "c".to_string()]);
    let mk_config = |map: std::collections::HashMap<FragmentId, Vec<String>>| NodeConfig {
        fragment_count: 2,
        fragment_map: map,
        ..NodeConfig::default()
    };
    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        mk_config(fragment_map.clone()),
    )
    .await
    .unwrap();
    let b = Node::start(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        mk_config(fragment_map.clone()),
    )
    .await
    .unwrap();
    let c = Node::start(
        "c".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        mk_config(fragment_map),
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    // b and c replicate fragment 1 to each other; connect them directly.
    c.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    // Find keys for each fragment.
    let mut keys_f0 = Vec::new();
    let mut keys_f1 = Vec::new();
    for i in 0..50 {
        let k = format!("pk{i}");
        if fragment_of(&k, 2) == FragmentId(0) {
            keys_f0.push(k);
        } else {
            keys_f1.push(k);
        }
        if keys_f0.len() >= 3 && keys_f1.len() >= 3 {
            break;
        }
    }
    assert!(!keys_f0.is_empty() && !keys_f1.is_empty());

    // Put all keys via a. Fragment-1 keys go to b (preferred owner)
    // and replicate to c via the async sender.
    let mut all_keys = keys_f0.clone();
    all_keys.extend(keys_f1.clone());
    let kvs: Vec<(String, Vec<u8>)> = all_keys
        .iter()
        .map(|k| (k.clone(), format!("v{k}").into_bytes()))
        .collect();
    let results = a.put_multi(kvs).await;
    assert!(results.iter().all(|r| r.is_ok()));
    // Wait until c has the fragment-1 keys via replication.
    common::wait_for(
        || async {
            let res = c.get_multi(keys_f1.clone()).await;
            res.iter().all(|r| matches!(r, Ok(Some(_))))
        },
        Duration::from_secs(10),
    )
    .await;

    // Kill b (preferred owner). Fragment-1 reads must fail over to c
    // and still succeed; fragment-0 keys stay local.
    b.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let results = a.get_multi(all_keys.clone()).await;
    assert_eq!(results.len(), all_keys.len());
    for (i, k) in keys_f0.iter().enumerate() {
        assert!(results[i].is_ok(), "local key {k} should not error");
    }
    for (j, k) in keys_f1.iter().enumerate() {
        let i = keys_f0.len() + j;
        match &results[i] {
            Ok(Some(v)) => assert_eq!(
                v.value,
                format!("v{k}").into_bytes(),
                "failover read of {k} returned wrong value"
            ),
            other => panic!("remote key {k} should fail over to c, got {other:?}"),
        }
    }

    let single_key = keys_f1[0].clone();
    assert_eq!(
        a.get(single_key.clone())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(format!("v{single_key}").into_bytes()),
        "single read did not skip the disconnected preferred owner"
    );
    a.put(single_key.clone(), b"single-failover".to_vec())
        .await
        .expect("single write did not skip the disconnected preferred owner");
    assert_eq!(
        c.get(single_key).await.unwrap().map(|value| value.value),
        Some(b"single-failover".to_vec())
    );

    // Kill c too. Now no replica of fragment 1 is reachable: its keys
    // must error per-key while fragment-0 keys still succeed.
    c.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let results = a.get_multi(all_keys.clone()).await;
    for (i, k) in keys_f0.iter().enumerate() {
        assert!(results[i].is_ok(), "local key {k} should not error");
    }
    for (j, k) in keys_f1.iter().enumerate() {
        let i = keys_f0.len() + j;
        assert!(
            results[i].is_err(),
            "remote key {k} should error when all replicas are down"
        );
    }

    a.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_operations_fail_over_after_a_stale_owner_returns_unavailable() {
    use actordb::shard::FragmentId;

    let map = |owners: &[&str]| {
        HashMap::from([(
            FragmentId(0),
            owners.iter().map(|owner| (*owner).to_string()).collect(),
        )])
    };
    let config = |fragment_map| NodeConfig {
        fragment_map,
        ..NodeConfig::default()
    };
    let a = Node::start_with_options(
        "stale-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["stale-b", "stale-c"])),
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let b = Node::start(
        "stale-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["stale-c"])),
    )
    .await
    .unwrap();
    let c = Node::start(
        "stale-c".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["stale-c"])),
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    let results = a
        .put_multi(vec![("stale-batch".to_string(), b"value".to_vec())])
        .await;
    assert!(results[0].is_ok(), "failover result: {:?}", results[0]);
    assert_eq!(
        c.get("stale-batch".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    a.put("stale-single".to_string(), b"single".to_vec())
        .await
        .expect("single put did not fail over after unavailable");
    assert_eq!(
        c.get("stale-single".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"single".to_vec())
    );

    // The live but non-serving first owner returns an empty unavailable
    // BatchGetResponse. The caller must try the next owner instead of treating
    // the omitted key as a miss.
    let reads = a.get_multi(vec!["stale-batch".to_string()]).await;
    assert_eq!(reads.len(), 1);
    assert_eq!(
        reads[0].as_ref().unwrap().as_ref().unwrap().value,
        b"value".to_vec()
    );
    assert_eq!(
        a.get("stale-single".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"single".to_vec()),
        "single get treated an unavailable preferred owner as not-found"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn mixed_batch_put_retries_only_unavailable_positions_with_exact_stamps() {
    use actordb::actor::OpPriority;
    use actordb::proto::{MutationStatus, Priority};
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        fragment_map: HashMap::from([(
            FragmentId(0),
            vec![
                "subset-first".to_string(),
                "subset-second".to_string(),
                "subset-third".to_string(),
            ],
        )]),
        ..NodeConfig::default()
    };
    let client = Node::start_with_options(
        "subset-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let mut first = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut first, "subset-first").await;
    let mut second = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut second, "subset-second").await;
    let mut third = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut third, "subset-third").await;
    common::wait_for(
        || async { client.peer_count().await == 3 },
        Duration::from_secs(5),
    )
    .await;

    let input = vec![
        ("duplicate".to_string(), b"older".to_vec()),
        ("retry-a".to_string(), b"a".to_vec()),
        ("storage".to_string(), b"storage".to_vec()),
        ("duplicate".to_string(), b"newer".to_vec()),
        ("backpressure".to_string(), b"backpressure".to_vec()),
        ("retry-b".to_string(), b"b".to_vec()),
        ("expired".to_string(), b"expired".to_vec()),
        ("actor-error".to_string(), b"actor-error".to_vec()),
    ];
    let exchange = async {
        let original = recv_batch_put(&mut first).await;
        assert_eq!(original.puts.len(), 8);
        assert_eq!(original.priority, Priority::High as i32);
        assert!(original.puts.iter().all(|put| {
            put.priority == Priority::High as i32
                && put.node_id == "subset-client"
                && put.seq != 0
                && put.boot_id != 0
                && put.req_id == 0
        }));
        assert_eq!(original.puts[0].key, original.puts[3].key);
        assert_ne!(original.puts[0].value, original.puts[3].value);
        send_frame(
            &mut first,
            batch_put_response(
                &original,
                &[
                    MutationStatus::Applied,
                    MutationStatus::Unavailable,
                    MutationStatus::StorageError,
                    MutationStatus::Unavailable,
                    MutationStatus::Backpressure,
                    MutationStatus::Unavailable,
                    MutationStatus::Expired,
                    MutationStatus::ActorError,
                ],
            ),
        )
        .await;

        let second_batch = recv_batch_put(&mut second).await;
        assert_ne!(original.req_id, second_batch.req_id);
        assert_eq!(second_batch.priority, original.priority);
        assert_eq!(
            second_batch.puts,
            vec![
                original.puts[1].clone(),
                original.puts[3].clone(),
                original.puts[5].clone(),
            ],
            "retry must preserve exact mutations and only unavailable positions"
        );
        // Response indexes are 0, 1, 2 in this filtered request, rather than
        // the original indexes 1, 3, 5. The duplicate's other position has
        // already completed and must never be retransmitted.
        send_frame(
            &mut second,
            batch_put_response(
                &second_batch,
                &[
                    MutationStatus::AlreadyPresent,
                    MutationStatus::Superseded,
                    MutationStatus::Unavailable,
                ],
            ),
        )
        .await;

        let third_batch = recv_batch_put(&mut third).await;
        assert_ne!(original.req_id, third_batch.req_id);
        assert_ne!(second_batch.req_id, third_batch.req_id);
        assert_eq!(third_batch.priority, original.priority);
        assert_eq!(third_batch.puts, vec![original.puts[5].clone()]);
        send_frame(
            &mut third,
            batch_put_response(&third_batch, &[MutationStatus::Applied]),
        )
        .await;
        original.puts[3].timestamp_ms + 1
    };
    let (winner_timestamp_ms, results) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            exchange,
            client.put_multi_with_priority(input, OpPriority::High)
        )
    })
    .await
    .expect("mixed batch-put subset exchange timed out");
    assert_eq!(results.len(), 8);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(matches!(&results[2], Err(NodeError::Storage(_))));
    assert!(matches!(
        &results[3],
        Err(NodeError::Superseded {
            timestamp_ms,
            logical_counter,
            node_id,
            is_tombstone: true,
        }) if *timestamp_ms == winner_timestamp_ms && node_id == "newer-writer"
    ));
    assert!(matches!(&results[4], Err(NodeError::Backpressure(_))));
    assert!(results[5].is_ok());
    assert!(matches!(&results[6], Err(NodeError::Expired)));
    assert!(matches!(&results[7], Err(NodeError::Actor(_))));

    drop(first);
    drop(second);
    drop(third);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn mixed_batch_put_malformed_subset_exhaustion_preserves_terminal_results() {
    use actordb::proto::MutationStatus;
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        fragment_map: HashMap::from([(
            FragmentId(0),
            vec!["partial-first".to_string(), "partial-second".to_string()],
        )]),
        ..NodeConfig::default()
    };
    let client = Node::start_with_options(
        "partial-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let mut first = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut first, "partial-first").await;
    let mut second = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut second, "partial-second").await;
    common::wait_for(
        || async { client.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    let input = vec![
        ("completed".to_string(), b"done".to_vec()),
        ("unresolved-a".to_string(), b"a".to_vec()),
        ("storage".to_string(), b"storage".to_vec()),
        ("unresolved-b".to_string(), b"b".to_vec()),
        ("backpressure".to_string(), b"backpressure".to_vec()),
    ];
    let exchange = async {
        let original = recv_batch_put(&mut first).await;
        send_frame(
            &mut first,
            batch_put_response(
                &original,
                &[
                    MutationStatus::Applied,
                    MutationStatus::Unavailable,
                    MutationStatus::StorageError,
                    MutationStatus::Unavailable,
                    MutationStatus::Backpressure,
                ],
            ),
        )
        .await;
        let retry = recv_batch_put(&mut second).await;
        assert_ne!(original.req_id, retry.req_id);
        assert_eq!(retry.priority, original.priority);
        assert_eq!(
            retry.puts,
            vec![original.puts[1].clone(), original.puts[3].clone()]
        );

        let mut malformed =
            batch_put_response(&retry, &[MutationStatus::Applied, MutationStatus::Applied]);
        let Some(EnvelopeMsg::MutationResponse(response)) = malformed.msg.as_mut() else {
            unreachable!();
        };
        // The first result looks valid, but the second uses its original
        // position rather than position 1 of the filtered request. The whole
        // response must be ignored, without committing its first success.
        response.results[1].request_index = 3;
        send_frame(&mut second, malformed).await;
        // Withhold a valid response so the final holder's attempt expires.
    };
    let ((), results) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(exchange, client.put_multi(input))
    })
    .await
    .expect("partial batch-put exhaustion exchange timed out");
    assert_eq!(results.len(), 5);
    assert!(results[0].is_ok());
    assert!(matches!(&results[1], Err(NodeError::Network(_))));
    assert!(matches!(&results[2], Err(NodeError::Storage(_))));
    assert!(matches!(&results[3], Err(NodeError::Network(_))));
    assert!(matches!(&results[4], Err(NodeError::Backpressure(_))));

    drop(first);
    drop(second);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn timed_out_single_put_retries_the_exact_stamped_operation() {
    use actordb::proto::{MutationResponse, MutationResult, MutationStatus};
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        fragment_map: HashMap::from([(
            FragmentId(0),
            vec!["silent-first".to_string(), "responding-second".to_string()],
        )]),
        ..NodeConfig::default()
    };
    let client = Node::start_with_options(
        "retry-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();

    let mut first = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut first, "silent-first").await;
    let mut second = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut second, "responding-second").await;
    common::wait_for(
        || async { client.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    let capture = async {
        let first_put = loop {
            if let Some(EnvelopeMsg::ForwardedPut(put)) = recv_frame(&mut first).await.msg {
                break put;
            }
        };
        // Deliberately do not answer the preferred holder. The second attempt
        // must carry the same version even though the first may have applied.
        let second_put = loop {
            if let Some(EnvelopeMsg::ForwardedPut(put)) = recv_frame(&mut second).await.msg {
                break put;
            }
        };
        send_frame(
            &mut second,
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                    req_id: second_put.req_id,
                    results: vec![MutationResult {
                        key: second_put.key.clone(),
                        status: MutationStatus::Applied as i32,
                        error: String::new(),
                        winner_timestamp_ms: 0,
                        winner_logical_counter: 0,
                        winner_node_id: String::new(),
                        winner_is_tombstone: false,
                        request_index: 0,

                        write_concern: None,
                    }],
                })),
            },
        )
        .await;
        (first_put, second_put)
    };
    let ((mut first_put, second_put), result) =
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                capture,
                client.put("retry-key".to_string(), b"value".to_vec())
            )
        })
        .await
        .expect("single-put retry exchange timed out");
    result.expect("single put did not retry after the first response timed out");

    assert_ne!(first_put.req_id, second_put.req_id);
    first_put.req_id = second_put.req_id;
    assert_eq!(first_put, second_put, "retry changed the stamped mutation");

    drop(first);
    drop(second);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn timed_out_single_get_tries_the_next_holder() {
    use actordb::actor::OpPriority;
    use actordb::proto::{GetResponse, Priority};
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        fragment_map: HashMap::from([(
            FragmentId(0),
            vec!["silent-reader".to_string(), "responding-reader".to_string()],
        )]),
        ..NodeConfig::default()
    };
    let client = Node::start_with_options(
        "read-retry-client".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();

    let mut first = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut first, "silent-reader").await;
    let mut second = tokio::net::TcpStream::connect(client.addr()).await.unwrap();
    complete_no_secret_handshake(&mut second, "responding-reader").await;
    common::wait_for(
        || async { client.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    let exchange = async {
        let first_get = loop {
            if let Some(EnvelopeMsg::ForwardedGet(get)) = recv_frame(&mut first).await.msg {
                break get;
            }
        };
        let second_get = loop {
            if let Some(EnvelopeMsg::ForwardedGet(get)) = recv_frame(&mut second).await.msg {
                break get;
            }
        };
        send_frame(
            &mut first,
            Envelope {
                msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                    req_id: first_get.req_id,
                    found: false,
                    value: Vec::new(),
                    timestamp_ms: 0,
                    logical_counter: 0,
                    node_id: String::new(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                    shed: false,
                })),
            },
        )
        .await;
        send_frame(
            &mut second,
            Envelope {
                msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                    req_id: second_get.req_id,
                    found: true,
                    value: b"value".to_vec(),
                    timestamp_ms: 7,
                    logical_counter: 0,
                    node_id: "responding-reader".to_string(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                    shed: false,
                })),
            },
        )
        .await;
        (first_get, second_get)
    };
    let ((first_get, second_get), result) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            exchange,
            client.get_with_priority("retry-key".to_string(), OpPriority::High)
        )
    })
    .await
    .expect("single-get retry exchange timed out");
    assert_eq!(
        result.unwrap().map(|value| value.value),
        Some(b"value".to_vec())
    );
    assert_ne!(first_get.req_id, second_get.req_id);
    assert_eq!(first_get.key, second_get.key);
    assert_eq!(first_get.priority, Priority::High as i32);
    assert_eq!(second_get.priority, Priority::High as i32);
    assert_eq!(first_get.hops, 0);
    assert_eq!(second_get.hops, 0);

    let shed_exchange = async {
        let request = loop {
            if let Some(EnvelopeMsg::ForwardedGet(get)) = recv_frame(&mut first).await.msg {
                break get;
            }
        };
        send_frame(
            &mut first,
            Envelope {
                msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                    req_id: request.req_id,
                    found: false,
                    value: Vec::new(),
                    timestamp_ms: 0,
                    logical_counter: 0,
                    node_id: String::new(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                    shed: true,
                })),
            },
        )
        .await;
        request
    };
    let (shed_request, shed_result) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            shed_exchange,
            client.get_with_priority("shed-key".to_string(), OpPriority::Background)
        )
    })
    .await
    .expect("single-get shed exchange timed out");
    assert_eq!(shed_request.priority, Priority::Background as i32);
    assert!(matches!(shed_result, Err(NodeError::Backpressure(_))));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), recv_frame(&mut second))
            .await
            .is_err(),
        "a typed backpressure response incorrectly failed over"
    );

    drop(first);
    drop(second);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_relay_scans_past_the_origin_to_a_connected_owner() {
    use actordb::shard::FragmentId;

    let map = |owners: &[&str]| {
        HashMap::from([(
            FragmentId(0),
            owners.iter().map(|owner| (*owner).to_string()).collect(),
        )])
    };
    let config = |fragment_map| NodeConfig {
        fragment_map,
        ..NodeConfig::default()
    };
    let a = Node::start(
        "relay-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["relay-b"])),
    )
    .await
    .unwrap();
    let b = Node::start(
        "relay-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["relay-a", "relay-c"])),
    )
    .await
    .unwrap();
    let c = Node::start(
        "relay-c".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["relay-c"])),
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    c.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { b.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;

    a.put("relayed-write".to_string(), b"value".to_vec())
        .await
        .unwrap();
    assert_eq!(
        c.get("relayed-write".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );
    assert_eq!(
        a.get("relayed-write".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec()),
        "single read did not re-forward across disagreeing placement maps"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

/// Concurrent background bulk writes preserve high-priority read results.
/// Held-pool unit tests prove isolation; shared-runner latency is a benchmark,
/// not a correctness assertion (40 observations cannot establish a p99 SLO).
#[tokio::test]
async fn background_bulk_writes_preserve_high_priority_reads() {
    use actordb::actor::{OpPriority, PoolConfig};
    use std::sync::Arc;

    let config = NodeConfig {
        background_pool: PoolConfig {
            workers: 1,
            queue_cap: 100_000,
            rate_limit_per_sec: None,
        },
        high_pool: PoolConfig {
            workers: 1,
            queue_cap: 100_000,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let n = Arc::new(
        Node::start("sat".to_string(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap(),
    );
    n.put("hot".to_string(), b"v".to_vec()).await.unwrap();

    // Run both workloads to completion instead of aborting a measurement task.
    let flood = {
        let n = n.clone();
        tokio::spawn(async move {
            for i in 0..200u32 {
                let kvs: Vec<(String, Vec<u8>)> = (0..25)
                    .map(|j| (format!("bg{i}-{j}"), vec![0u8; 32]))
                    .collect();
                let results = n.put_multi(kvs).await;
                assert!(
                    results.iter().all(Result::is_ok),
                    "bulk write failed: {results:?}"
                );
            }
        })
    };

    for _ in 0..40 {
        let r = n
            .get_with_priority("hot".to_string(), OpPriority::High)
            .await;
        assert_eq!(r.unwrap().map(|value| value.value), Some(b"v".to_vec()));
    }
    flood.await.unwrap();

    n.shutdown().await.unwrap();
}

/// Phase 7: a queue-cap hit is explicit, counted, and surfaced. A shed
/// Get returns `NodeError::Backpressure` (never `Ok(None)`), the shed
/// is counted per pool, and the count is visible via `health()`.
#[tokio::test]
async fn queue_cap_shed_is_explicit_and_counted() {
    use actordb::actor::PoolConfig;
    use actordb::node::NodeError;

    // Degenerate config: the default pool sheds everything (cap 0), so
    // every op deterministically exercises the shed path.
    let config = NodeConfig {
        default_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let n = Node::start("shed".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    // A shed Get is an explicit Backpressure error, not "not found".
    let r = n.get("k".to_string()).await;
    assert!(
        matches!(r, Err(NodeError::Backpressure(_))),
        "shed get should be Backpressure, got {r:?}"
    );

    // A shed put surfaces via its barrier read as well.
    let r = n.put("k".to_string(), b"v".to_vec()).await;
    assert!(
        matches!(r, Err(NodeError::Backpressure(_))),
        "shed put should be Backpressure, got {r:?}"
    );

    // Sheds are counted per pool and visible via the health API.
    let health = n.health().await.unwrap();
    let default_shed: u64 = health.values().map(|h| h.shed_counts.1).sum();
    assert!(
        default_shed >= 2,
        "expected shed counts to reflect the shed ops, got {default_shed}"
    );

    n.shutdown().await.unwrap();
}

/// Phase 7: forwarded priority is preserved end-to-end. A background
/// batch sent to a remote owner is applied at background priority
/// there — observable via the remote's per-pool shed counters.
#[tokio::test]
async fn forwarded_priority_preserved_end_to_end() {
    use actordb::actor::{OpPriority, PoolConfig};
    use actordb::node::NodeError;
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;

    // Two fragments: a serves 0, b serves 1. b's background pool sheds
    // everything; its default pool is healthy.
    let mut fragment_map = HashMap::new();
    fragment_map.insert(FragmentId(0), vec!["a".to_string()]);
    fragment_map.insert(FragmentId(1), vec!["b".to_string()]);
    let mk = |map: HashMap<FragmentId, Vec<String>>, mut cfg: NodeConfig| {
        cfg.fragment_count = 2;
        cfg.fragment_map = map;
        cfg
    };
    let config_b = NodeConfig {
        background_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        mk(fragment_map.clone(), NodeConfig::default()),
    )
    .await
    .unwrap();
    let b = Node::start(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        mk(fragment_map, config_b),
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // A key in fragment 1 (owned by b).
    let key = (0..100)
        .map(|i| format!("fp{i}"))
        .find(|k| fragment_of(k, 2) == FragmentId(1))
        .unwrap();

    // Background batch from a: b applies it at background priority →
    // shed → counted in b's background pool.
    let r = a
        .put_multi_with_priority(vec![(key.clone(), b"v".to_vec())], OpPriority::Background)
        .await;
    assert!(
        matches!(&r[0], Err(NodeError::Backpressure(_))),
        "remote shed must be returned exactly, got {:?}",
        r[0]
    );
    common::wait_for(
        || async {
            b.health()
                .await
                .unwrap()
                .values()
                .map(|h| h.shed_counts.2)
                .sum::<u64>()
                > 0
        },
        Duration::from_secs(5),
    )
    .await;

    // Default-priority batch lands in b's default pool (not shed): the
    // key is applied and the default shed counter stays zero.
    let r = a
        .put_multi_with_priority(vec![(key.clone(), b"v2".to_vec())], OpPriority::Default)
        .await;
    assert!(r[0].is_ok());
    common::wait_for(
        || async { b.get(key.clone()).await.unwrap().is_some() },
        Duration::from_secs(5),
    )
    .await;
    let default_shed: u64 = b
        .health()
        .await
        .unwrap()
        .values()
        .map(|h| h.shed_counts.0)
        .sum();
    assert_eq!(
        default_shed, 0,
        "default-priority batch must not shed on b's healthy default pool"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_batch_read_reports_per_key_shed_and_preserves_high_priority_pool() {
    use actordb::actor::{OpPriority, PoolConfig};
    use actordb::shard::FragmentId;

    let config = NodeConfig {
        fragment_map: HashMap::from([(FragmentId(0), vec!["batch-read-owner".to_string()])]),
        background_pool: PoolConfig {
            workers: 1,
            queue_cap: 1,
            rate_limit_per_sec: Some(1),
        },
        ..NodeConfig::default()
    };
    let origin = Node::start(
        "batch-read-origin".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
    )
    .await
    .unwrap();
    let owner = Node::start(
        "batch-read-owner".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    let key = "remote-batch-duplicate".to_string();
    owner
        .put_with_priority(key.clone(), b"value".to_vec(), OpPriority::Default)
        .await
        .unwrap();
    owner.connect(origin.addr()).await.unwrap();
    wait_for_peers(&origin, &owner).await;

    // The origin owns no fragments, so both duplicate positions traverse the
    // real BatchGet responder. Only the first may consume the owner's one
    // background token; the second must be explicit shed, never a false miss.
    let background = origin.get_multi(vec![key.clone(), key.clone()]).await;
    assert_eq!(background.len(), 2);
    assert_eq!(
        background[0].as_ref().unwrap().as_ref().unwrap().value,
        b"value".to_vec()
    );
    assert!(
        matches!(&background[1], Err(NodeError::Backpressure(_))),
        "second duplicate should return remote shed, got {:?}",
        background[1]
    );

    // The same connection must still serve a High-priority aggregate read
    // through the separate high pool while the background token is exhausted.
    let high = origin
        .get_multi_with_priority(vec![key.clone(), key], OpPriority::High)
        .await;
    assert_eq!(high.len(), 2);
    for result in high {
        assert_eq!(result.unwrap().unwrap().value, b"value".to_vec());
    }
    let health = owner.health().await.unwrap();
    assert_eq!(health[&FragmentId(0)].shed_counts, (0, 0, 1));
    assert_eq!(origin.peer_count().await, 1);
    assert_eq!(owner.peer_count().await, 1);

    origin.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn forwarded_single_put_returns_remote_backpressure() {
    use actordb::actor::PoolConfig;
    use actordb::node::NodeError;
    use actordb::shard::{fragment_of, FragmentId};

    let mut fragment_map = HashMap::new();
    fragment_map.insert(FragmentId(0), vec!["a".to_string()]);
    fragment_map.insert(FragmentId(1), vec!["b".to_string()]);
    let config_a = NodeConfig {
        fragment_count: 2,
        fragment_map: fragment_map.clone(),
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        default_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..config_a.clone()
    };

    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config_a)
        .await
        .unwrap();
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), config_b)
        .await
        .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    let key = (0..100)
        .map(|i| format!("remote-shed-{i}"))
        .find(|key| fragment_of(key, 2) == FragmentId(1))
        .unwrap();

    let result = a.put(key.clone(), b"value".to_vec()).await;
    assert!(
        matches!(result, Err(NodeError::Backpressure(_))),
        "remote shed must be returned exactly, got {result:?}"
    );
    assert!(!b.get_all().await.unwrap().contains_key(&key));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn forwarded_put_returns_remote_storage_failure() {
    use actordb::node::NodeError;
    use actordb::shard::{fragment_of, FragmentId};

    let mut fragment_map = HashMap::new();
    fragment_map.insert(FragmentId(0), vec!["a".to_string()]);
    fragment_map.insert(FragmentId(1), vec!["b".to_string()]);
    let config_a = NodeConfig {
        fragment_count: 2,
        fragment_map: fragment_map.clone(),
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        store: Some(Arc::new(AlwaysFailStore)),
        flush_batch_size: 1,
        ..config_a.clone()
    };

    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config_a)
        .await
        .unwrap();
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), config_b)
        .await
        .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    let key = (0..100)
        .map(|i| format!("remote-store-{i}"))
        .find(|key| fragment_of(key, 2) == FragmentId(1))
        .unwrap();

    let result = a.put(key, b"value".to_vec()).await;
    assert!(
        matches!(result, Err(NodeError::Storage(_))),
        "remote storage failure must be returned exactly, got {result:?}"
    );

    a.shutdown().await.unwrap();
    assert!(matches!(b.shutdown().await, Err(NodeError::Storage(_))));
}

#[tokio::test]
async fn simultaneous_fragment_gains_bootstrap_only_from_serving_source() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::shard::FragmentId;

    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["source-a".to_string()],
    )])));
    let config = || NodeConfig {
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(25),
        ..NodeConfig::default()
    };
    let a = test_node_with_config("source-a", config()).await;
    let b = test_node_with_config("joining-b", config()).await;
    let c = test_node_with_config("joining-c", config()).await;
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    c.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async {
            a.peer_count().await == 2 && b.peer_count().await == 2 && c.peer_count().await == 2
        },
        Duration::from_secs(5),
    )
    .await;

    a.put("authoritative-before-gain".to_string(), b"value".to_vec())
        .await
        .unwrap();
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec![
            "joining-b".to_string(),
            "joining-c".to_string(),
            "source-a".to_string(),
        ],
    )]));

    common::wait_for(
        || async {
            b.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
                && c.placement_report().await.states.get(&FragmentId(0))
                    == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(20),
    )
    .await;
    for node in [&b, &c] {
        assert_eq!(
            node.get("authoritative-before-gain".to_string())
                .await
                .unwrap()
                .map(|value| value.value),
            Some(b"value".to_vec())
        );
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

/// Phase 8: migrate a fragment between two live nodes under continuous
/// writes. Zero lost writes; reads stay correct throughout.
#[tokio::test]
async fn placement_migration_no_lost_writes() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let frag_count: usize = 2;
    let mut initial = HashMap::new();
    initial.insert(FragmentId(0), vec!["a".to_string()]);
    initial.insert(FragmentId(1), vec!["a".to_string()]);
    let controller = Arc::new(ManualPlacement::new(initial));

    let mk = || NodeConfig {
        fragment_count: frag_count,
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(100),
        replication: SenderConfig {
            consistency_interval: Duration::from_millis(200),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let a = Arc::new(
        Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
            .await
            .unwrap(),
    );
    let b = Arc::new(
        Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
            .await
            .unwrap(),
    );
    a.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;

    // Keys homed on fragment 0 (the migrating fragment).
    let keys: Vec<String> = (0..200)
        .map(|i| format!("mig-key-{i}"))
        .filter(|k| fragment_of(k, frag_count) == FragmentId(0))
        .take(12)
        .collect();
    assert!(keys.len() >= 2, "need at least two keys in fragment 0");

    // Seed initial values via A.
    for k in &keys {
        a.put(k.clone(), b"v0".to_vec()).await.unwrap();
    }

    // Continuous writer via A (single writer, so HLC timestamps respect
    // write order). Records the latest acknowledged value per key.
    let expected: Arc<tokio::sync::Mutex<HashMap<String, Vec<u8>>>> =
        Arc::new(tokio::sync::Mutex::new(
            keys.iter()
                .map(|key| (key.clone(), b"v0".to_vec()))
                .collect(),
        ));
    let attempted: Arc<tokio::sync::Mutex<HashMap<String, u64>>> = Arc::new(
        tokio::sync::Mutex::new(keys.iter().map(|key| (key.clone(), 0)).collect()),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let a = a.clone();
        let expected = expected.clone();
        let attempted = attempted.clone();
        let stop = stop.clone();
        let keys = keys.clone();
        tokio::spawn(async move {
            let mut round: u64 = 1;
            while !stop.load(Ordering::Relaxed) {
                for k in &keys {
                    let v = format!("v{round}").into_bytes();
                    attempted.lock().await.insert(k.clone(), round);
                    // A may forward to B once it stops serving; the write
                    // still lands.
                    if a.put(k.clone(), v.clone()).await.is_ok() {
                        expected.lock().await.insert(k.clone(), v);
                    }
                }
                round += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };

    // Let the writer run, then start the migration: overlap [A, B].
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut overlap = HashMap::new();
    overlap.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
    overlap.insert(FragmentId(1), vec!["a".to_string()]);
    controller.set_placement(overlap);

    // Wait until B serves fragment 0 (bootstrap complete).
    let bootstrap_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let report = b.placement_report().await;
        if report.states.get(&FragmentId(0)) == Some(&FragmentPlacementState::Serving) {
            break;
        }
        if tokio::time::Instant::now() >= bootstrap_deadline {
            panic!(
                "B did not finish bootstrap: b_placement={report:?}, a_placement={:?}, a_to_b={:?}, b_to_a={:?}, peer_counts=({}, {})",
                a.placement_report().await,
                a.sender_report(FragmentId(0), "b").await,
                b.sender_report(FragmentId(0), "a").await,
                a.peer_count().await,
                b.peer_count().await,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Reads during the overlap: every value must be one the writer
    // actually wrote (well-formed, and not from the future).
    for k in &keys {
        let got = b.get(k.clone()).await.unwrap().expect("key missing");
        let s = String::from_utf8(got.value.clone()).unwrap();
        assert!(s.starts_with('v'), "unexpected value {s} for {k}");
        let round: u64 = s[1..].parse().expect("value should be v<round>");
        let max_attempted_round = attempted.lock().await[k];
        assert!(
            round <= max_attempted_round,
            "read from the future for {k}: {s}"
        );
    }

    // Revoke A: [B] only.
    let mut final_map = HashMap::new();
    final_map.insert(FragmentId(0), vec!["b".to_string()]);
    final_map.insert(FragmentId(1), vec!["a".to_string()]);
    controller.set_placement(final_map);

    // Wait until A drops fragment 0.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let report = a.placement_report().await;
        if report.states.get(&FragmentId(0)) == Some(&FragmentPlacementState::Stopped) {
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            panic!(
                "A did not finish drain: a_placement={report:?}, b_placement={:?}, a_to_b={:?}, b_to_a={:?}, peer_counts=({}, {})",
                b.placement_report().await,
                a.sender_report(FragmentId(0), "b").await,
                b.sender_report(FragmentId(0), "a").await,
                a.peer_count().await,
                b.peer_count().await,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop the writer; B must retain at least the final acknowledged value of
    // every key. A response can be lost after B applied a later attempt, so a
    // value newer than the last acknowledged one is valid but must still be a
    // value this writer actually attempted.
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    let expected = expected.lock().await.clone();
    let attempted = attempted.lock().await.clone();
    assert_eq!(expected.len(), keys.len());
    let convergence_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut mismatches = Vec::new();
        for (key, expected_value) in &expected {
            let actual = b.get(key.clone()).await.unwrap().map(|value| value.value);
            let acknowledged_round = std::str::from_utf8(expected_value)
                .unwrap()
                .strip_prefix('v')
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let max_attempted_round = attempted[key];
            let actual_round = actual
                .as_deref()
                .and_then(|value| std::str::from_utf8(value).ok())
                .and_then(|value| value.strip_prefix('v'))
                .and_then(|round| round.parse::<u64>().ok());
            if !matches!(actual_round, Some(round) if round >= acknowledged_round && round <= max_attempted_round)
            {
                mismatches.push((key.clone(), acknowledged_round, max_attempted_round, actual));
            }
        }
        if mismatches.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= convergence_deadline {
            panic!(
                "migrated values did not converge: mismatches={mismatches:?}, a_placement={:?}, b_placement={:?}, a_to_b={:?}, b_to_a={:?}",
                a.placement_report().await,
                b.placement_report().await,
                a.sender_report(FragmentId(0), "b").await,
                b.sender_report(FragmentId(0), "a").await,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Once A has retired the fragment, a grouped write must travel through
    // B's dynamic BatchPut mutation lease as one ownership decision.
    let migrated_batch = vec![
        (keys[0].clone(), b"batch-after-move-0".to_vec()),
        (keys[1].clone(), b"batch-after-move-1".to_vec()),
    ];
    let batch_results = a.put_multi(migrated_batch.clone()).await;
    assert!(
        batch_results.iter().all(Result::is_ok),
        "post-migration batch failed: results={batch_results:?}, a_placement={:?}, b_placement={:?}, peer_counts=({}, {})",
        a.placement_report().await,
        b.placement_report().await,
        a.peer_count().await,
        b.peer_count().await,
    );
    for (key, expected_value) in migrated_batch {
        assert_eq!(
            b.get(key).await.unwrap().map(|value| value.value),
            Some(expected_value)
        );
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// RocksDB prefix retirement is destructive only after the replacement
/// replica completes an exact stable-storage handoff. Reopening the retired
/// source must not expose any fragment data, while reopening the destination
/// must recover the migrated value from its stable handoff.
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn placement_retires_rocksdb_prefix_after_stable_handoff() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;
    use actordb::store::{PrefixedStore, RocksDbStore, StoreIdentity};

    let fragment = FragmentId(0);
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        fragment,
        vec!["rocks-source".to_string()],
    )])));
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();

    let config = |path: &std::path::Path| {
        let mut config = NodeConfig::with_rocksdb(path).unwrap();
        config.placement_controller = Some(controller.clone());
        config.placement_poll_interval = Duration::from_millis(25);
        config.placement_drain_warn_interval = Duration::from_millis(100);
        config.snapshot_interval = Duration::from_secs(3600);
        config.clean_interval = Duration::from_secs(3600);
        config.replication = SenderConfig {
            batch_interval: Duration::from_millis(10),
            consistency_interval: Duration::from_millis(100),
            ..SenderConfig::default()
        };
        config
    };

    let source = Node::start(
        "rocks-source".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(source_dir.path()),
    )
    .await
    .unwrap();
    let target = Node::start(
        "rocks-target".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(target_dir.path()),
    )
    .await
    .unwrap();
    source.connect(target.addr()).await.unwrap();
    common::wait_for(
        || async { source.peer_count().await == 1 && target.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            source.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    source
        .put("durable-migration-key".to_string(), b"value".to_vec())
        .await
        .unwrap();
    controller.set_placement(HashMap::from([(
        fragment,
        vec!["rocks-source".to_string(), "rocks-target".to_string()],
    )]));
    common::wait_for(
        || async {
            target.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(20),
    )
    .await;

    controller.set_placement(HashMap::from([(
        fragment,
        vec!["rocks-target".to_string()],
    )]));
    common::wait_for(
        || async {
            source.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(
        target
            .get("durable-migration-key".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    source.shutdown().await.unwrap();
    drop(source);
    let reopened = Arc::new(RocksDbStore::open(source_dir.path()).unwrap());
    let _binding = reopened
        .bind(&StoreIdentity::new("rocks-source".to_string(), 1))
        .unwrap();
    let retired_fragment = PrefixedStore::new(reopened, fragment);
    assert!(retired_fragment.load().unwrap().is_empty());

    target.shutdown().await.unwrap();
    drop(target);
    let reopened_target = Arc::new(RocksDbStore::open(target_dir.path()).unwrap());
    let _target_binding = reopened_target
        .bind(&StoreIdentity::new("rocks-target".to_string(), 1))
        .unwrap();
    let durable_target = PrefixedStore::new(reopened_target, fragment);
    assert_eq!(
        durable_target
            .load()
            .unwrap()
            .into_iter()
            .find(|(key, _)| key == b"durable-migration-key")
            .map(|(_, value)| value.value),
        Some(b"value".to_vec())
    );
}

/// An ambiguous prefix-delete failure keeps the retired subtree owned and the
/// lifecycle visibly Draining. A re-add after router detach retains the prefix
/// and starts a fresh subtree; a later revocation retries deletion and may
/// publish Stopped only after the backend confirms it.
#[tokio::test]
async fn placement_prefix_delete_failure_retries_before_stopped() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let fragment = FragmentId(0);
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        fragment,
        vec!["delete-source".to_string()],
    )])));
    let source_store = Arc::new(RetirementStore::default());
    source_store.fail_delete.store(true, Ordering::SeqCst);
    let target_store = Arc::new(RetirementStore::default());
    let config = |store: Arc<RetirementStore>| NodeConfig {
        store: Some(store),
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(25),
        placement_drain_warn_interval: Duration::from_millis(50),
        snapshot_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        replication: SenderConfig {
            batch_interval: Duration::from_millis(10),
            consistency_interval: Duration::from_millis(100),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let source = Node::start(
        "delete-source".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(source_store.clone()),
    )
    .await
    .unwrap();
    let target = Node::start(
        "delete-target".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(target_store),
    )
    .await
    .unwrap();
    source.connect(target.addr()).await.unwrap();
    common::wait_for(
        || async { source.peer_count().await == 1 && target.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            source.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    source
        .put("delete-retry-key".to_string(), b"value".to_vec())
        .await
        .unwrap();

    let overlap = HashMap::from([(
        fragment,
        vec!["delete-source".to_string(), "delete-target".to_string()],
    )]);
    let target_only = HashMap::from([(fragment, vec!["delete-target".to_string()])]);
    controller.set_placement(overlap.clone());
    common::wait_for(
        || async {
            target.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(20),
    )
    .await;
    controller.set_placement(target_only.clone());
    common::wait_for(
        || async {
            source_store.delete_attempts.load(Ordering::SeqCst) != 0
                && source.placement_report().await.states.get(&fragment)
                    == Some(&FragmentPlacementState::Draining)
        },
        Duration::from_secs(20),
    )
    .await;
    assert!(!source_store.fragment_is_empty(fragment.0));

    // The old subtree is already detached here. Changing placement must make
    // the failing delete retain its namespace, stop, and bootstrap a fresh
    // serving subtree instead of reviving the quiesced cache.
    controller.set_placement(overlap);
    common::wait_for(
        || async {
            source.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(20),
    )
    .await;
    assert!(!source_store.fragment_is_empty(fragment.0));
    assert_eq!(
        source
            .get("delete-retry-key".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    source_store.fail_delete.store(false, Ordering::SeqCst);
    controller.set_placement(target_only);
    common::wait_for(
        || async {
            source.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(source_store.fragment_is_empty(fragment.0));
    assert_eq!(
        target
            .get("delete-retry-key".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}

/// A sender awaiting overflow reconciliation is not drained merely because
/// its ordinary buffer and in-flight batch count are both zero. Placement
/// revocation must keep the fragment subtree alive past the warning threshold
/// and tear it down only after reset actually completes.
#[tokio::test]
async fn placement_drain_waits_for_buffer_reset() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{BatchAck, Envelope, EnvelopeMsg, ReplicationAckLevel};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let initial = HashMap::from([(FragmentId(0), vec!["drain-a".to_string()])]);
    let controller = Arc::new(ManualPlacement::new(initial));
    let node = Node::start(
        "drain-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            placement_controller: Some(controller.clone()),
            placement_poll_interval: Duration::from_millis(25),
            placement_drain_warn_interval: Duration::from_millis(100),
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(30),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-drain-peer").await;
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["drain-a".to_string(), "raw-drain-peer".to_string()],
    )]));

    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-drain-peer")
                .await
                .map(|report| report.session_id != 0)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;
    node.put("drain-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("drain-b".to_string(), b"b".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-drain-peer")
                .await
                .map(|report| report.state == "buffer_reset")
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["raw-drain-peer".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Draining)
        },
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        node.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Draining),
        "placement warning threshold destructively timed out the drain"
    );
    assert_eq!(
        node.sender_report(FragmentId(0), "raw-drain-peer")
            .await
            .unwrap()
            .state,
        "buffer_reset"
    );

    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &request,
                Vec::new(),
            ))),
        },
    )
    .await;
    let (repair_summaries, barrier) = recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(!repair_summaries.is_empty());
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: request.session_id,
                batch_seq: barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    let verification_request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &verification_request,
                repair_summaries,
            ))),
        },
    )
    .await;
    let (verification_repairs, verification_barrier) =
        recv_reconcile_repairs_and_barrier(&mut stream).await;
    assert!(verification_repairs.is_empty());
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: verification_request.session_id,
                batch_seq: verification_barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        node.sender_report(FragmentId(0), "raw-drain-peer")
            .await
            .is_none(),
        "completed drain retained its sender"
    );

    node.shutdown().await.unwrap();
}

/// Once placement has detached a fragment, a failed final flush keeps the
/// retained subtree non-authoritative. Re-adding the node cannot revive that
/// quiesced cache; recovery must finish the flush, stop the old subtree, and
/// start a fresh one from the durable value.
#[tokio::test]
async fn placement_readd_waits_for_failed_final_flush_and_reloads() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::shard::FragmentId;

    let fragment = FragmentId(0);
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        fragment,
        vec!["flush-readd-a".to_string()],
    )])));
    let store = Arc::new(ToggleFailStore::default());
    let node = Node::start(
        "flush-readd-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_count: 1,
            placement_controller: Some(controller.clone()),
            placement_poll_interval: Duration::from_millis(200),
            placement_drain_warn_interval: Duration::from_millis(50),
            store: Some(store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    let original_workers = node.health().await.unwrap()[&fragment].worker_names.clone();

    node.put("flush-readd-key".to_string(), b"durable".to_vec())
        .await
        .unwrap();
    let attempts_before_loss = store.attempts.load(Ordering::SeqCst);
    controller.set_placement(HashMap::from([(fragment, vec!["other-owner".to_string()])]));
    common::wait_for(
        || async {
            store.attempts.load(Ordering::SeqCst) > attempts_before_loss
                && node.placement_report().await.states.get(&fragment)
                    == Some(&FragmentPlacementState::Draining)
        },
        Duration::from_secs(5),
    )
    .await;

    controller.set_placement(HashMap::from([(
        fragment,
        vec!["flush-readd-a".to_string()],
    )]));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        node.placement_report().await.states.get(&fragment),
        Some(&FragmentPlacementState::Draining),
        "re-add revived a detached fragment whose final flush still failed"
    );

    store.failing.store(false, Ordering::SeqCst);
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&fragment)
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    let restarted_workers = node.health().await.unwrap()[&fragment].worker_names.clone();
    assert_ne!(original_workers, restarted_workers);
    assert_eq!(
        node.get("flush-readd-key".to_string())
            .await
            .unwrap()
            .unwrap()
            .value,
        b"durable".to_vec()
    );
    node.shutdown().await.unwrap();
}

/// A placement reversal cancels an in-progress loss transition. The stale
/// drainer must not tear down a fragment that the controller made
/// authoritative again while BufferReset was still recovering.
#[tokio::test]
async fn placement_readd_while_draining_cancels_teardown() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{BatchAck, Envelope, EnvelopeMsg, ReplicationAckLevel};
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let overlap = HashMap::from([(
        FragmentId(0),
        vec!["readd-a".to_string(), "raw-readd-peer".to_string()],
    )]);
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["readd-a".to_string()],
    )])));
    let retirement_store = Arc::new(RetirementStore::default());
    let node = Node::start(
        "readd-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            placement_controller: Some(controller.clone()),
            store: Some(retirement_store),
            placement_poll_interval: Duration::from_millis(25),
            placement_drain_warn_interval: Duration::from_millis(100),
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability:
                    actordb::replication::ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-readd-peer").await;
    controller.set_placement(overlap.clone());
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-readd-peer")
                .await
                .map(|report| report.session_id != 0)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.put("readd-first".to_string(), b"first".to_vec())
        .await
        .unwrap();
    node.put("readd-second".to_string(), b"second".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-readd-peer")
                .await
                .map(|report| report.state == "buffer_reset")
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["raw-readd-peer".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Draining)
        },
        Duration::from_secs(5),
    )
    .await;
    let mut readded_stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut readded_stream, "raw-readded-peer").await;
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["readd-a".to_string(), "raw-readded-peer".to_string()],
    )]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Bootstrapping)
        },
        Duration::from_secs(5),
    )
    .await;

    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) =
            recv_frame(&mut readded_stream).await.msg
        {
            break request;
        }
    };
    send_frame(
        &mut readded_stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &request,
                Vec::new(),
            ))),
        },
    )
    .await;
    let (repair_summaries, barrier) = recv_reconcile_repairs_and_barrier(&mut readded_stream).await;
    assert!(!repair_summaries.is_empty());
    send_frame(
        &mut readded_stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: request.session_id,
                batch_seq: barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    let verification_request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) =
            recv_frame(&mut readded_stream).await.msg
        {
            break request;
        }
    };
    send_frame(
        &mut readded_stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &verification_request,
                repair_summaries,
            ))),
        },
    )
    .await;
    let (verification_repairs, verification_barrier) =
        recv_reconcile_repairs_and_barrier(&mut readded_stream).await;
    assert!(verification_repairs.is_empty());
    send_frame(
        &mut readded_stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: verification_request.session_id,
                batch_seq: verification_barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        node.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Serving),
        "stale drain tore down a fragment after placement re-added it"
    );
    assert_eq!(
        node.get("readd-second".to_string())
            .await
            .unwrap()
            .unwrap()
            .value,
        b"second".to_vec()
    );

    node.shutdown().await.unwrap();
}

/// Phase 8: rolling restart with dynamic placement. The survivor keeps
/// serving while the restarted node catches up through correlated,
/// byte-bounded reconciliation.
#[tokio::test]
async fn placement_rolling_restart_delta_reconcile_only() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;
    use std::sync::Arc;

    let frag_count: usize = 2;
    let mut map = HashMap::new();
    map.insert(FragmentId(0), vec!["a".to_string()]);
    map.insert(FragmentId(1), vec!["a".to_string()]);
    let controller = Arc::new(ManualPlacement::new(map));

    let mk = || NodeConfig {
        fragment_count: frag_count,
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(100),
        replication: SenderConfig {
            consistency_interval: Duration::from_millis(200),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };

    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    common::wait_for(
        || async {
            let report = a.placement_report().await;
            (0..frag_count).all(|fragment| {
                report.states.get(&FragmentId(fragment as u32))
                    == Some(&FragmentPlacementState::Serving)
            })
        },
        Duration::from_secs(5),
    )
    .await;
    controller.set_placement(HashMap::from([
        (FragmentId(0), vec!["a".to_string(), "b".to_string()]),
        (FragmentId(1), vec!["a".to_string(), "b".to_string()]),
    ]));
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    a.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            let report = b.placement_report().await;
            (0..frag_count).all(|fragment| {
                report.states.get(&FragmentId(fragment as u32))
                    == Some(&FragmentPlacementState::Serving)
            })
        },
        Duration::from_secs(10),
    )
    .await;

    // Write data and let it converge to both replicas.
    for i in 0..50 {
        a.put(format!("rk-{i}"), format!("rv-{i}").into_bytes())
            .await
            .unwrap();
    }
    common::wait_for(
        || async {
            let all = b.get_all().await.unwrap();
            (0..50).all(|i| {
                all.get(&format!("rk-{i}")).map(|v| v.value.clone())
                    == Some(format!("rv-{i}").into_bytes())
            })
        },
        Duration::from_secs(10),
    )
    .await;

    // Restart A: same node_id, fresh (empty) state.
    a.shutdown().await.unwrap();
    // The surviving replica keeps serving throughout.
    assert_eq!(
        b.get("rk-0".to_string())
            .await
            .unwrap()
            .expect("b should serve during a's restart")
            .value,
        b"rv-0".to_vec()
    );
    b.put("restart-only".to_string(), b"newer".to_vec())
        .await
        .unwrap();

    let a2 = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();

    let restart_fragment = fragment_of("restart-only", frag_count);
    common::wait_for(
        || async {
            a2.placement_report().await.states.get(&restart_fragment)
                == Some(&FragmentPlacementState::Bootstrapping)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        a2.get("restart-only".to_string()).await.is_err(),
        "a restarted replica served its preloaded state before reconciliation"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        a2.placement_report().await.states.get(&restart_fragment),
        Some(&FragmentPlacementState::Bootstrapping),
        "an offline restarted replica became authoritative without a source"
    );

    a2.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a2.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(10),
    )
    .await;

    // A2 must converge to the full dataset through the same v3 reconciliation
    // protocol used by static placement before either fragment is published.
    common::wait_for(
        || async {
            let report = a2.placement_report().await;
            (0..frag_count).all(|fragment| {
                report.states.get(&FragmentId(fragment as u32))
                    == Some(&FragmentPlacementState::Serving)
            })
        },
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(
        a2.get("restart-only".to_string())
            .await
            .unwrap()
            .expect("verified restart delta missing")
            .value,
        b"newer".to_vec()
    );
    let all = a2.get_all().await.unwrap();
    assert!((0..50).all(|i| {
        all.get(&format!("rk-{i}")).map(|v| v.value.clone()) == Some(format!("rv-{i}").into_bytes())
    }));
    assert_eq!(
        a2.sync_entries_applied(),
        0,
        "protocol-v3 placement bootstrap used the retired sync path"
    );

    a2.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Regression test for adversarial review #1: a shed write must never
/// be acked `Ok`. With a zero-cap pool every ApplyOp sheds; every `put`
/// must surface `Backpressure`, never `Ok`.
#[tokio::test]
async fn shed_write_never_acked_ok() {
    use actordb::actor::PoolConfig;
    use actordb::node::NodeError;

    // Degenerate: cap 0 sheds every op deterministically.
    let config = NodeConfig {
        default_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let n = Node::start(
        "shed-ok".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();

    for i in 0..20 {
        let key = format!("sw-{i}");
        let r = n.put(key.clone(), b"v".to_vec()).await;
        assert!(
            matches!(r, Err(NodeError::Backpressure(_))),
            "shed put must be Backpressure, not Ok: got {r:?} for {key}"
        );
        // And the key must not exist (nothing was applied).
        let got = n.get(key.clone()).await;
        // The get itself sheds (cap 0) → Backpressure, not Ok(None).
        assert!(
            matches!(got, Err(NodeError::Backpressure(_))),
            "shed get must be Backpressure, got {got:?}"
        );
    }

    n.shutdown().await.unwrap();
}

/// Shed deletes must surface as `Backpressure`, not masquerade as
/// successful deletes (which also read back as `None` via the
/// tombstone-filtering `get`).
#[tokio::test]
async fn shed_delete_never_acked_ok() {
    use actordb::actor::PoolConfig;
    use actordb::node::NodeError;

    // Degenerate: cap 0 sheds every op deterministically.
    let config = NodeConfig {
        default_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let n = Node::start(
        "shed-del".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();

    for i in 0..20 {
        let key = format!("sd-{i}");
        let r = n.delete(key.clone()).await;
        assert!(
            matches!(r, Err(NodeError::Backpressure(_))),
            "shed delete must be Backpressure, not Ok: got {r:?} for {key}"
        );
    }

    n.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_limit_sheds_excess_and_handshake_timeout_releases_capacity() {
    let node = Node::start_with_options(
        "bounded-handshakes".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default()
            .with_inbound_connection_capacity(1)
            .with_handshake_timeout(Duration::from_millis(150)),
    )
    .await
    .unwrap();

    let mut held = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut held).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));

    let mut shed = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), shed.read(&mut byte))
        .await
        .expect("excess inbound connection was not shed")
        .unwrap();
    assert_eq!(read, 0);

    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), held.read_to_end(&mut remainder))
        .await
        .expect("silent pre-auth connection outlived its absolute deadline")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut admitted = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut admitted, "admitted-after-timeout").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(2),
    )
    .await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn pre_auth_frames_have_a_small_cap_and_one_absolute_body_deadline() {
    let node = Node::start_with_options(
        "pre-auth-bounds".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_handshake_timeout(Duration::from_millis(150)),
    )
    .await
    .unwrap();

    let mut oversized = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut oversized).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    oversized.write_u32(4097).await.unwrap();
    oversized.flush().await.unwrap();
    let mut byte = [0u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), oversized.read(&mut byte))
            .await
            .expect("oversized pre-auth prefix did not close the socket")
            .unwrap(),
        0
    );

    let mut partial = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut partial).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    partial.write_u32(32).await.unwrap();
    partial.write_all(&[1]).await.unwrap();
    partial.flush().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), partial.read(&mut byte))
            .await
            .expect("partial pre-auth body outlived the handshake deadline")
            .unwrap(),
        0
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn hello_auth_second_flight_uses_the_original_handshake_deadline() {
    let node = Node::start_with_options(
        "second-flight-deadline".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_handshake_timeout(Duration::from_millis(600)),
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    tokio::time::sleep(Duration::from_millis(350)).await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "slow-second-flight".to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x6b; 32],
            })),
        },
    )
    .await;
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::HelloAuth(_))
    ));

    let mut byte = [0u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(450), stream.read(&mut byte))
            .await
            .expect("HelloAuth received a fresh handshake timeout")
            .unwrap(),
        0
    );
    assert_eq!(node.peer_count().await, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn established_idle_is_allowed_but_partial_frame_body_times_out() {
    let node = Node::start_with_options(
        "established-read-timeout".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_socket_read_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "partial-frame-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(2),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(node.peer_count().await, 1, "idle peer hit a prefix timeout");

    stream.write_u32(32).await.unwrap();
    stream.write_all(&[1]).await.unwrap();
    stream.flush().await.unwrap();
    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut remainder))
        .await
        .expect("partial established frame body outlived its read deadline")
        .unwrap();
    common::wait_for(
        || async { node.peer_count().await == 0 },
        Duration::from_secs(2),
    )
    .await;
    node.shutdown().await.unwrap();
}

/// A ForwardedPut arriving before the mutual handshake completes must close
/// the socket and must never be applied.
#[tokio::test]
async fn pre_handshake_forwarded_put_ignored() {
    use actordb::proto::{Envelope, EnvelopeMsg, ForwardedPut};

    let a = test_node("a").await;

    // Consume the server's eager Hello, but do not answer it.
    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                key: "evil".to_string(),
                value: b"evil-val".to_vec(),
                timestamp_ms: 1,
                logical_counter: 0,
                node_id: "attacker".to_string(),
                expires_at_ms: 0,
                is_tombstone: false,
                seq: 0,
                boot_id: 0,
                priority: 0,
                hops: 0,
                req_id: 1,

                write_concern: None,
            })),
        },
    )
    .await;
    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut remainder))
        .await
        .expect("pre-auth application frame did not close the socket")
        .unwrap();

    let got = a.get("evil".to_string()).await.unwrap();
    assert!(
        got.is_none(),
        "pre-handshake ForwardedPut was applied! got {got:?}"
    );

    a.shutdown().await.unwrap();
}

/// With a cluster secret configured, a valid Hello followed by an invalid
/// challenge-bound proof must drop the connection before peer publication.
#[tokio::test]
async fn hello_auth_mismatch_drops_connection() {
    let config = NodeConfig {
        cluster_secret: Some(vec![0x91; 32]),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "b".to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x6b; 32],
            })),
        },
    )
    .await;
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) if auth_tag.len() == 32
    ));
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth {
                auth_tag: vec![0; 32],
            })),
        },
    )
    .await;

    // The server must close the connection: read returns 0 (EOF).
    let mut read_buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut read_buf))
        .await
        .expect("timed out waiting for close")
        .unwrap();
    assert_eq!(n, 0, "expected connection to be closed on auth failure");

    // And "b" must not be a peer.
    assert_eq!(a.peer_count().await, 0, "unauthenticated peer was accepted");

    a.shutdown().await.unwrap();
}

/// A peer must explicitly negotiate the wire version and every semantic bit
/// relied on by mutation, batching, and replication acknowledgements.
/// Fieldless legacy Hellos and partial capability sets fail closed before peer
/// installation.
#[tokio::test]
async fn incompatible_hello_capabilities_are_rejected() {
    use actordb::node::{CAPABILITY_BYTE_BOUNDED_BATCHES, CAPABILITY_STABLE_DURABILITY_ACKS};
    use actordb::proto::{EnvelopeMsg, Hello};

    let node = test_node("capability-server").await;
    let incompatible = [
        Hello {
            node_id: "legacy-peer".to_string(),
            ..Hello::default()
        },
        Hello {
            node_id: "partial-peer".to_string(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES & !CAPABILITY_BYTE_BOUNDED_BATCHES,
            ..Hello::default()
        },
        Hello {
            node_id: "no-stable-fence-peer".to_string(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES & !CAPABILITY_STABLE_DURABILITY_ACKS,
            ..Hello::default()
        },
    ];
    for hello in incompatible {
        let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
        assert!(matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::Hello(_))
        ));
        send_frame(
            &mut stream,
            Envelope {
                msg: Some(EnvelopeMsg::Hello(hello)),
            },
        )
        .await;
        let mut read_buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut read_buf))
            .await
            .expect("incompatible Hello connection was not closed")
            .unwrap();
        assert_eq!(read, 0);
    }
    assert_eq!(node.peer_count().await, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn retired_full_state_sync_frames_close_protocol_v3_connection() {
    use actordb::proto::{EnvelopeMsg, SyncRequest};

    let node = test_node("retired-sync-server").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "retired-sync-peer").await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SyncRequest(SyncRequest { fragments: vec![0] })),
        },
    )
    .await;

    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut remainder))
        .await
        .expect("retired sync frame did not close the connection")
        .unwrap();
    common::wait_for(
        || async { node.peer_count().await == 0 },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// Regression test for adversarial review #4: a Hello claiming our own
/// node_id must be rejected.
#[tokio::test]
async fn hello_claiming_self_id_rejected() {
    let a = test_node("a").await;

    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    assert!(matches!(
        recv_frame(&mut stream).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "a".to_string(), // our own id!
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x7c; 32],
            })),
        },
    )
    .await;

    let mut read_buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut read_buf))
        .await
        .expect("timed out waiting for close")
        .unwrap();
    assert_eq!(n, 0, "expected connection to be closed on self-id Hello");

    a.shutdown().await.unwrap();
}

/// Peer identity is immutable for the life of a connection. Accepting a
/// second Hello used to let one socket replace its identity and rotate every
/// sender session while leaving stale peer-map entries behind.
#[tokio::test]
async fn second_hello_closes_established_connection() {
    let node = test_node("hello-server").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "first-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "second-peer".to_string(),
                auth_tag: Vec::new(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![0x8d; 32],
            })),
        },
    )
    .await;

    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut remainder))
        .await
        .expect("server did not close after a second Hello")
        .unwrap();
    common::wait_for(
        || async { node.peer_count().await == 0 },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// Replaying SessionStart with the same id must not clear a failed-repair
/// latch. Otherwise an empty tracked barrier could ACK immediately after an
/// untracked repair failed and falsely certify convergence.
#[tokio::test]
async fn same_session_replay_does_not_clear_reconcile_failure() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, ReplicateBatch, ReplicateOp, ReplicationAckLevel, SessionStart,
    };

    let node = Node::start(
        "poison-receiver".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(Arc::new(AlwaysFailStore)),
            flush_batch_size: 1,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-poison-sender").await;

    let session_id = 77;
    let start = || Envelope {
        msg: Some(EnvelopeMsg::SessionStart(SessionStart {
            session_id,
            fragment: 0,
        })),
    };
    send_frame(&mut stream, start()).await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id,
                batch_seq: u64::MAX,
                fragment: 0,
                ops: vec![ReplicateOp {
                    key: "failed-repair".to_string(),
                    value: b"value".to_vec(),
                    timestamp_ms: now_ms(),
                    logical_counter: 0,
                    node_id: "raw-poison-sender".to_string(),
                    seq: 1,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    // Same-id replay is not a new session and cannot erase the failure.
    send_frame(&mut stream, start()).await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id,
                batch_seq: 9,
                fragment: 0,
                ops: Vec::new(),
                required_ack_level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;

    let nack = loop {
        if let Some(EnvelopeMsg::BatchNack(nack)) = recv_frame(&mut stream).await.msg {
            if nack.session_id == session_id && nack.batch_seq == 9 {
                break nack;
            }
        }
    };
    assert!(nack.error.contains("injected permanent failure"));

    assert!(matches!(node.shutdown().await, Err(NodeError::Storage(_))));
}

/// Positive case: two nodes with the same cluster secret complete the
/// handshake and replicate.
#[tokio::test]
async fn cluster_secret_handshake_succeeds() {
    let secret = vec![0xa5; 32];
    let config_a = NodeConfig {
        cluster_secret: Some(secret.clone()),
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        cluster_secret: Some(secret),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config_a)
        .await
        .unwrap();
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), config_b)
        .await
        .unwrap();

    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Both peers authenticated: they see each other.
    assert_eq!(a.peer_count().await, 1);
    assert_eq!(b.peer_count().await, 1);

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Regression test for adversarial review #2: replicated ops must not
/// be shed under backpressure. The receiver's dispatcher is saturated
/// (cap 0 sheds all local ops), but replication bypasses admission —
/// otherwise the sender would ack batches that were never applied.
#[tokio::test]
async fn replication_not_shed_under_backpressure() {
    use actordb::actor::PoolConfig;

    let a = test_node("a").await;
    // B sheds every local op (cap 0), but replication must still land.
    let config_b = NodeConfig {
        default_pool: PoolConfig {
            workers: 1,
            queue_cap: 0,
            rate_limit_per_sec: None,
        },
        ..NodeConfig::default()
    };
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), config_b)
        .await
        .unwrap();

    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // A writes; the batch replicates to B.
    a.put("repl-k".to_string(), b"repl-v".to_vec())
        .await
        .unwrap();

    // B must converge despite its saturated pool: get_all bypasses
    // admission, so we can observe the replicated state directly.
    common::wait_for(
        || async {
            b.get_all()
                .await
                .unwrap()
                .get("repl-k")
                .map(|v| v.value.clone())
                == Some(b"repl-v".to_vec())
        },
        Duration::from_secs(15),
    )
    .await;

    // And the replicated op must not have been counted as shed.
    let health = b.health().await.unwrap();
    let shed: u64 = health.values().map(|h| h.shed_counts.1).sum();
    assert_eq!(
        shed, 0,
        "replicated op was shed under backpressure (ack would be a lie)"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_applied_ack_does_not_force_a_receiver_store_flush() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(GatedStore::default());
    let _release_on_drop = store.release_on_drop();
    let config_a = NodeConfig {
        replication: SenderConfig {
            batch_size: 1,
            batch_interval: Duration::from_millis(10),
            required_ack_durability: ReplicationAckDurability::MemoryApplied,
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: usize::MAX,
        flush_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let a = Node::start(
        "memory-ack-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_a,
    )
    .await
    .unwrap();
    let b = Node::start(
        "memory-ack-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_b,
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("memory-ack".to_string(), b"value".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            b.get("memory-ack".to_string())
                .await
                .unwrap()
                .map(|value| value.value)
                == Some(b"value".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "memory-ack-b")
                .await
                .map(|report| {
                    report.in_flight == 0
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        !store.entered.load(Ordering::SeqCst),
        "MemoryApplied unexpectedly forced a store flush"
    );
    assert_eq!(
        b.get("memory-ack".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    // Clean shutdown flushes the buffered write, so release the test store
    // only after proving that the ACK itself did not enter it.
    store.release();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_ack_waits_for_the_receiver_store_fence() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(GatedStore::default());
    let _release_on_drop = store.release_on_drop();
    let config_a = NodeConfig {
        replication: SenderConfig {
            batch_size: 1,
            batch_interval: Duration::from_millis(10),
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: 1_000_000,
        // Observe the explicit replication ACK fence rather than an apply's
        // age-triggered flush after a slow handshake under parallel load.
        flush_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let a = Node::start(
        "ack-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_a,
    )
    .await
    .unwrap();
    let b = Node::start(
        "ack-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_b,
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "ack-b")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1 && report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;
    let baseline_ack = a
        .sender_report(FragmentId(0), "ack-b")
        .await
        .unwrap()
        .last_ack_durability;

    a.put("ack-gated".to_string(), b"value".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async { store.entered.load(Ordering::SeqCst) },
        Duration::from_secs(5),
    )
    .await;
    let blocked = a
        .sender_report(FragmentId(0), "ack-b")
        .await
        .expect("sender report missing");
    assert_eq!(blocked.last_ack_durability, baseline_ack);
    assert_eq!(blocked.in_flight, 1, "batch retired before store fence");

    store.release();
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "ack-b")
                .await
                .map(|report| {
                    report.in_flight == 0
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::StoreCommitted)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        b.get("ack-gated".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_rejects_an_ack_below_the_configured_durability() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let config_a = NodeConfig {
        replication: SenderConfig {
            batch_size: 1,
            batch_interval: Duration::from_millis(10),
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let a = Node::start(
        "durability-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_a,
    )
    .await
    .unwrap();
    let b = test_node("durability-b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Protocol v3 gates streaming on the reconciliation barrier. A receiver
    // without a durable store can only acknowledge MemoryApplied, so that
    // initial StoreCommitted barrier itself must remain outstanding.
    common::wait_for(
        || async {
            a.sender_report(FragmentId(0), "durability-b")
                .await
                .map(|report| {
                    report.in_flight == 1
                        && report.ack_durability_mismatch
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        a.sender_report(FragmentId(0), "durability-b")
            .await
            .unwrap()
            .reconcile_rounds,
        0
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// An admitted ordinary batch cannot remain in flight forever. A silent peer
/// forces a fresh sender session, and every still-unacknowledged stamped op is
/// replayed only after the retry floor. ACKs from the expired session remain
/// harmless even when the new session reuses the same batch sequence number.
#[tokio::test]
async fn ordinary_ack_timeout_rotates_session_and_replays_exact_batch() {
    use actordb::proto::{BatchAck, EnvelopeMsg, ReplicationAckLevel};
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let ack_timeout = Duration::from_secs(2);

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec![
            "ack-timeout-sender".to_string(),
            "raw-ack-timeout-peer".to_string(),
        ],
    );
    let node = Node::start_with_options(
        "ack-timeout-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 1,
                batch_interval: Duration::from_millis(10),
                max_in_flight: 1,
                buffer_capacity: 100,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability: ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
        NodeRuntimeOptions::default()
            .with_replication_ack_timeout(ack_timeout)
            .with_queue_admission_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-ack-timeout-peer").await;
    let initial_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            break session.session_id;
        }
    };
    complete_empty_reconcile(&mut stream, initial_session).await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-ack-timeout-peer")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1 && report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;

    // Receipt can lag sender admission. Start before the write so scheduler
    // delays consuming the first batch cannot falsely shorten this interval.
    let write_started_at = tokio::time::Instant::now();
    node.put("ack-timeout-key".to_string(), b"value".to_vec())
        .await
        .unwrap();
    let first = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    assert_eq!(first.session_id, initial_session);
    assert_eq!(first.ops.len(), 1);
    assert_eq!(first.ops[0].key, "ack-timeout-key");

    // Withhold the ACK. The timeout invalidates the whole old session before
    // any replay, so a delayed old-session ACK can never certify the retry.
    let replay_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            if session.session_id != initial_session {
                break session.session_id;
            }
        }
    };
    let replay = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX && batch.session_id == replay_session {
                break batch;
            }
        }
    };
    assert!(
        write_started_at.elapsed() >= ack_timeout + Duration::from_millis(900),
        "timed-out batch replayed without the one-second retry floor"
    );
    assert_eq!(
        replay.ops, first.ops,
        "replay changed the stamped operation"
    );
    assert_eq!(replay.required_ack_level, first.required_ack_level);

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: first.session_id,
                batch_seq: first.batch_seq,
                fragment: first.fragment,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let pending = node
        .sender_report(FragmentId(0), "raw-ack-timeout-peer")
        .await
        .unwrap();
    assert_eq!(pending.session_id, replay_session);
    assert_eq!(pending.in_flight, 1, "stale ACK retired the replay");

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: replay.session_id,
                batch_seq: replay.batch_seq,
                fragment: replay.fragment,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-ack-timeout-peer")
                .await
                .map(|report| {
                    report.session_id == replay_session
                        && report.in_flight == 0
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// ACKs may arrive out of order. Retiring a newer batch must neither refresh
/// the older batch's deadline nor cause the already-ACKed operation to be
/// replayed when that older deadline rotates the session.
#[tokio::test]
async fn older_batch_timeout_replays_only_the_unacknowledged_batch() {
    use actordb::proto::{BatchAck, EnvelopeMsg, ReplicationAckLevel};
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec![
            "out-of-order-ack-sender".to_string(),
            "raw-out-of-order-ack-peer".to_string(),
        ],
    );
    let node = Node::start_with_options(
        "out-of-order-ack-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 1,
                batch_interval: Duration::from_millis(10),
                max_in_flight: 2,
                buffer_capacity: 100,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability: ReplicationAckDurability::MemoryApplied,
            },
            ..NodeConfig::default()
        },
        NodeRuntimeOptions::default()
            .with_replication_ack_timeout(Duration::from_secs(2))
            .with_queue_admission_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-out-of-order-ack-peer").await;
    let initial_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            break session.session_id;
        }
    };
    complete_empty_reconcile(&mut stream, initial_session).await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-out-of-order-ack-peer")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1 && report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;

    node.put("older-unacked".to_string(), b"first".to_vec())
        .await
        .unwrap();
    node.put("newer-acked".to_string(), b"second".to_vec())
        .await
        .unwrap();
    let first = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    let second = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    assert_eq!(first.session_id, initial_session);
    assert_eq!(second.session_id, initial_session);
    assert_eq!(first.ops.len(), 1);
    assert_eq!(second.ops.len(), 1);
    assert_eq!(first.ops[0].key, "older-unacked");
    assert_eq!(second.ops[0].key, "newer-acked");

    // Retire only the newer batch and synchronize on the sender report before
    // allowing the older batch's original admission deadline to expire.
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: second.session_id,
                batch_seq: second.batch_seq,
                fragment: second.fragment,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-out-of-order-ack-peer")
                .await
                .map(|report| {
                    report.session_id == initial_session
                        && report.in_flight == 1
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(1),
    )
    .await;

    let replay_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            if session.session_id != initial_session {
                break session.session_id;
            }
        }
    };
    let replay = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX && batch.session_id == replay_session {
                break batch;
            }
        }
    };
    assert_eq!(replay.ops, first.ops);
    assert_ne!(replay.ops, second.ops);

    // With max_in_flight=2, an erroneously requeued second op would be emitted
    // on the next 10ms batch tick without waiting for this replay's ACK.
    if let Ok(unexpected) =
        tokio::time::timeout(Duration::from_millis(100), recv_frame(&mut stream)).await
    {
        panic!("retired newer batch produced protocol output: {unexpected:?}");
    }

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: replay.session_id,
                batch_seq: replay.batch_seq,
                fragment: replay.fragment,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-out-of-order-ack-peer")
                .await
                .map(|report| report.session_id == replay_session && report.in_flight == 0)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

/// A correlated ACK below the requested durability is not a keepalive. It
/// leaves the original admission deadline intact and eventually forces the
/// same fail-closed session rotation and exact replay as a missing ACK.
#[tokio::test]
async fn weak_ack_does_not_prevent_timeout_and_exact_replay() {
    use actordb::proto::{BatchAck, EnvelopeMsg, ReplicationAckLevel};
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let ack_timeout = Duration::from_secs(2);

    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec![
            "weak-ack-sender".to_string(),
            "raw-weak-ack-peer".to_string(),
        ],
    );
    let node = Node::start_with_options(
        "weak-ack-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            fragment_map,
            replication: SenderConfig {
                batch_size: 1,
                batch_interval: Duration::from_millis(10),
                max_in_flight: 1,
                buffer_capacity: 100,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
            },
            ..NodeConfig::default()
        },
        NodeRuntimeOptions::default()
            .with_replication_ack_timeout(ack_timeout)
            .with_queue_admission_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-weak-ack-peer").await;
    let initial_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            break session.session_id;
        }
    };
    complete_empty_reconcile(&mut stream, initial_session).await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-weak-ack-peer")
                .await
                .is_some_and(|report| report.reconcile_rounds >= 1 && report.in_flight == 0)
        },
        Duration::from_secs(5),
    )
    .await;

    let write_started_at = tokio::time::Instant::now();
    node.put("weak-ack-key".to_string(), b"value".to_vec())
        .await
        .unwrap();
    let first = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    assert_eq!(first.session_id, initial_session);
    assert_eq!(
        first.required_ack_level,
        ReplicationAckLevel::StoreCommitted as i32
    );

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: first.session_id,
                batch_seq: first.batch_seq,
                fragment: first.fragment,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-weak-ack-peer")
                .await
                .map(|report| {
                    report.session_id == initial_session
                        && report.in_flight == 1
                        && report.ack_durability_mismatch
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::MemoryApplied)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(1),
    )
    .await;

    let replay_session = loop {
        if let Some(EnvelopeMsg::SessionStart(session)) = recv_frame(&mut stream).await.msg {
            if session.session_id != initial_session {
                break session.session_id;
            }
        }
    };
    let replay = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX && batch.session_id == replay_session {
                break batch;
            }
        }
    };
    assert!(
        write_started_at.elapsed() >= ack_timeout + Duration::from_millis(900),
        "weak ACK reset the deadline or bypassed the retry floor"
    );
    assert_eq!(replay.ops, first.ops, "weak-ACK replay changed the op");

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: replay.session_id,
                batch_seq: replay.batch_seq,
                fragment: replay.fragment,
                level: ReplicationAckLevel::StoreCommitted as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-weak-ack-peer")
                .await
                .map(|report| {
                    report.session_id == replay_session
                        && report.in_flight == 0
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::StoreCommitted)
                        && !report.ack_durability_mismatch
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_nack_retries_the_exact_batch_after_store_failure() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(FailOnceStore::default());
    let config_a = NodeConfig {
        replication: SenderConfig {
            batch_size: 1,
            batch_interval: Duration::from_millis(10),
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    };
    let config_b = NodeConfig {
        store: Some(store.clone()),
        flush_batch_size: 1_000_000,
        ..NodeConfig::default()
    };
    let a = Node::start(
        "retry-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_a,
    )
    .await
    .unwrap();
    let b = Node::start(
        "retry-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config_b,
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    a.put("retry-key".to_string(), b"retry-value".to_vec())
        .await
        .unwrap();
    common::wait_for(
        || async {
            let acked = a
                .sender_report(FragmentId(0), "retry-b")
                .await
                .map(|report| {
                    report.in_flight == 0
                        && report.last_ack_durability
                            == Some(ReplicationAckDurability::StoreCommitted)
                })
                .unwrap_or(false);
            acked && store.attempts.load(Ordering::SeqCst) >= 2
        },
        Duration::from_secs(5),
    )
    .await;
    let mut prefixed_key = 0u32.to_be_bytes().to_vec();
    prefixed_key.extend_from_slice(b"retry-key");
    assert_eq!(
        store
            .entries
            .lock()
            .unwrap()
            .get(&prefixed_key)
            .map(|value| value.value.clone()),
        Some(b"retry-value".to_vec())
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// A failed untracked reconciliation repair must poison that receiver
/// session. The following empty barrier may not ACK and falsely certify the
/// missing tail of the repair batch; the sender must rotate its session and
/// reconcile again.
#[tokio::test]
async fn failed_reconcile_repair_cannot_be_hidden_by_barrier_ack() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(FailOnceStore::default());
    let sender_config = NodeConfig {
        replication: SenderConfig {
            batch_size: 100,
            batch_interval: Duration::from_secs(3600),
            max_in_flight: 1,
            buffer_capacity: 1,
            consistency_interval: Duration::from_secs(3600),
            reconcile_timeout: Duration::from_secs(5),
            required_ack_durability: ReplicationAckDurability::StoreCommitted,
        },
        ..NodeConfig::default()
    };
    let receiver_config = NodeConfig {
        store: Some(store.clone()),
        // Force the first repair op to encounter the injected store failure
        // before the rest of that untracked repair batch can be applied.
        flush_batch_size: 1,
        flush_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let a = Node::start(
        "repair-sender".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        sender_config,
    )
    .await
    .unwrap();
    let b = Node::start(
        "repair-receiver".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        receiver_config,
    )
    .await
    .unwrap();
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    let writes: Vec<_> = (0..4)
        .map(|i| (format!("repair-{i}"), format!("value-{i}").into_bytes()))
        .collect();
    let results = a.put_multi(writes.clone()).await;
    assert!(results.iter().all(Result::is_ok));

    common::wait_for(
        || async {
            let converged = b.get_all().await.unwrap();
            let all_present = writes.iter().all(|(key, value)| {
                converged.get(key).map(|stored| stored.value.as_slice()) == Some(value.as_slice())
            });
            a.sender_report(FragmentId(0), "repair-receiver")
                .await
                .map(|report| {
                    report.state == "connected"
                        && report.reconcile_rounds >= 1
                        && report.in_flight == 0
                        && all_present
                })
                .unwrap_or(false)
        },
        Duration::from_secs(10),
    )
    .await;
    assert!(
        store.attempts.load(Ordering::SeqCst) >= 2,
        "repair failure was not retried through a fresh session"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// A failed local StoreCommitted fence remains required even when the retry
/// sees the fetched winner already present in cache and therefore needs no
/// second fetch. The remote barrier alone cannot certify requester durability.
#[tokio::test]
async fn fetched_reconcile_value_retries_failed_local_store_fence() {
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, KeySummary, ReplicationAckLevel, SyncEntry,
    };
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(FailOnceStore::default());
    let mut fragment_map = HashMap::new();
    fragment_map.insert(
        FragmentId(0),
        vec!["fetch-store".to_string(), "raw-fetch-peer".to_string()],
    );
    let node = Node::start(
        "fetch-store".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            store: Some(store.clone()),
            flush_batch_size: usize::MAX,
            flush_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            fragment_map,
            replication: SenderConfig {
                batch_size: 100,
                batch_interval: Duration::from_secs(3600),
                max_in_flight: 1,
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability: ReplicationAckDurability::StoreCommitted,
            },
            ..NodeConfig::default()
        },
    )
    .await
    .unwrap();
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "raw-fetch-peer").await;
    let initial_session_id = loop {
        if let Some(EnvelopeMsg::SessionStart(start)) = recv_frame(&mut stream).await.msg {
            break start.session_id;
        }
    };
    node.put("fetch-local-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("fetch-local-b".to_string(), b"b".to_vec())
        .await
        .unwrap();

    let remote_timestamp = now_ms();
    let summary = KeySummary {
        content_digest: content_digest(b"durable", 0),
        key: "fetched-durable".to_string(),
        timestamp_ms: remote_timestamp,
        logical_counter: 0,
        node_id: "raw-fetch-peer".to_string(),
        is_tombstone: false,
    };
    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.session_id != initial_session_id {
                break request;
            }
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &first,
                vec![summary.clone()],
            ))),
        },
    )
    .await;
    let mut remote_summaries = vec![summary.clone()];
    let fetch = loop {
        match recv_frame(&mut stream).await.msg {
            Some(EnvelopeMsg::ReconcileFetch(fetch)) => break fetch,
            Some(EnvelopeMsg::ReplicateBatch(batch)) if batch.batch_seq == u64::MAX => {
                remote_summaries.extend(batch.ops.into_iter().map(|op| KeySummary {
                    content_digest: content_digest(&op.value, op.expires_at_ms),
                    key: op.key,
                    timestamp_ms: op.timestamp_ms,
                    logical_counter: op.logical_counter,
                    node_id: op.node_id,
                    is_tombstone: op.is_tombstone,
                }));
            }
            _ => {}
        }
    };
    remote_summaries.sort_by(|left, right| left.key.cmp(&right.key));
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(complete_reconcile_data(
                &fetch,
                vec![SyncEntry {
                    key: summary.key.clone(),
                    value: b"durable".to_vec(),
                    timestamp_ms: summary.timestamp_ms,
                    logical_counter: summary.logical_counter,
                    node_id: summary.node_id.clone(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
            ))),
        },
    )
    .await;

    let second = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.round_id != first.round_id {
                break request;
            }
        }
    };
    assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(complete_reconcile_response(
                &second,
                remote_summaries,
            ))),
        },
    )
    .await;
    let barrier = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq != u64::MAX {
                break batch;
            }
        }
    };
    assert!(barrier.ops.is_empty());
    assert!(
        store.attempts.load(Ordering::SeqCst) >= 2,
        "no-fetch retry skipped the failed local flush"
    );
    let mut durable_key = 0u32.to_be_bytes().to_vec();
    durable_key.extend_from_slice(b"fetched-durable");
    assert_eq!(
        store
            .entries
            .lock()
            .unwrap()
            .get(&durable_key)
            .map(|value| value.value.clone()),
        Some(b"durable".to_vec())
    );
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: second.session_id,
                batch_seq: barrier.batch_seq,
                fragment: 0,
                level: ReplicationAckLevel::StoreCommitted as i32,
            })),
        },
    )
    .await;
    common::wait_for(
        || async {
            node.sender_report(FragmentId(0), "raw-fetch-peer")
                .await
                .map(|report| report.state == "connected" && report.reconcile_rounds == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    )
    .await;

    node.shutdown().await.unwrap();
}
