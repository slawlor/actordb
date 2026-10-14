// Integration tests: multi-node replication over real TCP on localhost.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use actordb::actor::{now_ms, VersionedValue};
use actordb::node::{Node, NodeConfig};
use actordb::proto::Envelope;
use actordb::store::{KvStore, StoreError};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct PreloadedStore {
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl PreloadedStore {
    fn insert_fragment_zero(&self, key: &str, value: VersionedValue) {
        let mut prefixed = 0u32.to_be_bytes().to_vec();
        prefixed.extend_from_slice(key.as_bytes());
        self.entries.lock().unwrap().insert(prefixed, value);
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

#[derive(Default)]
struct GatedStore {
    entered: AtomicBool,
    release: (StdMutex<bool>, Condvar),
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl GatedStore {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
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

async fn test_node(id: &str) -> Node {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(id.to_string(), addr, NodeConfig::default())
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

    n.shutdown().await;
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

    for (id, config) in [
        ("invalid-batch", batch_zero),
        ("invalid-in-flight", in_flight_zero),
        ("invalid-reconcile", reconcile_zero),
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
    use actordb::replication::SenderDeps;
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
    fn legacy_sender_field_is_public(deps: &SenderDeps) {
        let _ = &deps.reset_senders;
    }

    assert_eq!(shared.gc_tombstones(u64::MAX, 0).await, 0);
    assert!(shared.get_all().await.contains_key("deleted"));
    assert_eq!(health.last_reaped, 0);
    let _ = (cleaner, supervisor, legacy_sender_field_is_public);
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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
    a.shutdown().await;
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

    a2.shutdown().await;
    b.shutdown().await;
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
    a.shutdown().await;
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

    a2.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn late_joiner_receives_full_state() {
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

    // c joins after the data exists and only ever talks to b.
    // The handshake sync must bring it fully up to date.
    let c = test_node("c").await;
    c.connect(b.addr()).await.unwrap();

    common::wait_for(
        || async {
            c.get("early".to_string()).await.unwrap().map(|v| v.value) == Some(b"data".to_vec())
        },
        Duration::from_secs(5),
    )
    .await;

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
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
    a.shutdown().await;

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
    a2.shutdown().await;
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
    a.shutdown().await;
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

    a2.shutdown().await;
    b.shutdown().await;
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
    a.shutdown().await;

    let a2 = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config())
        .await
        .expect("node failed to restart");
    let got = a2
        .get("k".to_string())
        .await
        .unwrap()
        .expect("k missing after restart");
    assert_eq!(got.value, b"v");
    a2.shutdown().await;
}

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
    a.shutdown().await;
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
    node.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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
    node.shutdown().await;
}

#[tokio::test]
async fn persisted_version_seeds_clock_before_first_write() {
    let store = Arc::new(PreloadedStore::default());
    let persisted_timestamp = now_ms() + 60_000;
    store.insert_fragment_zero(
        "key",
        VersionedValue::new(
            b"persisted".to_vec(),
            persisted_timestamp,
            "previous-boot".to_string(),
        ),
    );
    let config = NodeConfig {
        store: Some(store),
        flush_batch_size: 1,
        ..NodeConfig::default()
    };

    let node = Node::start("node".to_string(), "127.0.0.1:0".parse().unwrap(), config)
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
    assert!(current.timestamp_ms > persisted_timestamp);
    node.shutdown().await;
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
            node.shutdown().await;
            panic!("node accepted a poisoned durable timestamp");
        }
        Err(error) => assert!(
            error.to_string().contains("exceeds maximum allowed"),
            "unexpected startup error: {error}"
        ),
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
    a.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
}

/// Phase 4: TTL expiry — a key with a TTL disappears without manual
/// intervention (reads hide it past expiry; the cleaner materializes its
/// tombstone).
#[tokio::test]
async fn ttl_expiry_removes_key() {
    let config = NodeConfig {
        clean_interval: Duration::from_millis(50),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    a.put_with_ttl(
        "temp".to_string(),
        b"data".to_vec(),
        Some(Duration::from_millis(150)),
    )
    .await
    .unwrap();
    // Visible before expiry.
    assert!(a.get("temp".to_string()).await.unwrap().is_some());

    // Past TTL + cleaner sweep: gone.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(a.get("temp".to_string()).await.unwrap().is_none());

    // A key without TTL survives.
    a.put("perm".to_string(), b"forever".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(a.get("perm".to_string()).await.unwrap().is_some());

    a.shutdown().await;
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

    // Kill worker 0 of fragment 0 (name via health report).
    let health = a.health().await.unwrap();
    let frag0_health = &health[&actordb::shard::FragmentId(0)];
    let name: ractor::ActorName = frag0_health.worker_names[0].clone();
    let worker = where_is(name).expect("worker should be registered");
    worker.kill();

    // Wait for the supervised restart.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Fragment 0 still serves (new worker in place).
    a.put(target_key.clone(), b"v2".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
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

    a.shutdown().await;
}

/// Tombstones: delete hides the key locally and the tombstone
/// propagates to replicas via replication and handshake sync.
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

    a.shutdown().await;
    b.shutdown().await;
}

/// Tombstones: a delete survives node restart (tombstone is durable)
/// and a late joiner learns the delete via handshake sync.
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

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
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

    slow.shutdown().await;
    fast.shutdown().await;
}

/// Phase 5: kill a peer mid-replication. The sender's buffer overflows,
/// it enters `buffer_reset`, and on reconnect it reconciles — no
/// divergence.
#[tokio::test]
async fn sender_buffer_reset_and_reconcile() {
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let config = NodeConfig {
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
    let b = test_node("b").await;
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
    b.shutdown().await;
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
        NodeConfig::default(),
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

    a.shutdown().await;
    b2.shutdown().await;
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
    a.shutdown().await;
    b.shutdown().await;
}

/// Unchanged dynamic-placement polls reuse the current peer channel. They
/// must not mint a new replication session and invalidate slow in-flight
/// batches on every poll.
#[tokio::test]
async fn unchanged_placement_polls_preserve_sender_session() {
    use actordb::placement::ManualPlacement;
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let mut placement = HashMap::new();
    placement.insert(
        FragmentId(0),
        vec!["stable-a".to_string(), "stable-b".to_string()],
    );
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

    a.shutdown().await;
    b.shutdown().await;
}

/// A response from an older or fabricated reconcile round must not complete
/// a newer buffer reset. Completion requires the exact round and a tracked
/// ACK fence after outbound repair frames.
#[tokio::test]
async fn stale_reconcile_round_cannot_complete_live_reset() {
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, KeySummary, ReconcileResponse, ReplicationAckLevel,
    };
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-reset-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;

    // Drain handshake/sync frames through the initial SessionStart.
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }

    for i in 0..4 {
        node.put(
            format!("round-key-{i}"),
            format!("round-value-{i}").into_bytes(),
        )
        .await
        .unwrap();
    }

    let (session_id, round_id) = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break (request.session_id, request.round_id);
        }
    };
    assert_ne!(session_id, 0);
    assert_ne!(round_id, 0);

    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                // If the stale round were accepted, this remote-only winner
                // would immediately produce a ReconcileFetch. The valid
                // response below is deliberately different.
                summaries: vec![KeySummary {
                    key: "stale-round-only".to_string(),
                    // A stale frame must be discarded before HLC validation;
                    // this value would close the connection if observed.
                    timestamp_ms: u64::MAX,
                    node_id: "raw-reset-peer".to_string(),
                    is_tombstone: false,
                }],
                session_id,
                round_id: round_id.saturating_add(1),
            })),
        },
    )
    .await;
    if let Ok(unexpected) =
        tokio::time::timeout(Duration::from_millis(100), recv_frame(&mut stream)).await
    {
        panic!("stale response triggered protocol output: {unexpected:?}");
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
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: Vec::new(),
                session_id,
                round_id,
            })),
        },
    )
    .await;

    let mut saw_repair = false;
    let barrier = loop {
        if let Some(EnvelopeMsg::ReplicateBatch(batch)) = recv_frame(&mut stream).await.msg {
            if batch.batch_seq == u64::MAX {
                saw_repair = true;
            } else {
                break batch;
            }
        }
    };
    assert!(saw_repair, "expected local winners before the barrier");
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

    let second_round_id = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            assert_eq!(request.session_id, session_id);
            break request.round_id;
        }
    };
    assert_ne!(second_round_id, round_id);
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: Vec::new(),
                session_id,
                round_id: second_round_id,
            })),
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

    node.shutdown().await;
}

/// A bootstrap round that fetched remote winners has only fenced those
/// writes when its first barrier is acknowledged. It must run one fresh
/// no-fetch comparison before placement may advertise the fragment as
/// Serving.
#[tokio::test]
async fn placement_bootstrap_fetch_requires_verification_round() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, KeySummary, ReconcileData, ReconcileResponse,
        ReplicationAckLevel, SyncEntry,
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-bootstrap-source".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;

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
        key: "bootstrap-fetched".to_string(),
        timestamp_ms: now_ms(),
        node_id: "raw-bootstrap-source".to_string(),
        is_tombstone: false,
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary.clone()],
                session_id: first.session_id,
                round_id: first.round_id,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 0,
                entries: vec![SyncEntry {
                    key: summary.key.clone(),
                    value: b"fetched".to_vec(),
                    timestamp_ms: summary.timestamp_ms,
                    node_id: summary.node_id.clone(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                session_id: first.session_id,
                round_id: first.round_id,
                chunk_seq: fetch.chunk_seq,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary],
                session_id: second.session_id,
                round_id: second.round_id,
            })),
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

    node.shutdown().await;
}

/// Reconciliation liveness is independent of the normal batch timer. A live
/// peer that silently withholds a phase response must cause a session rotation
/// and a fresh correlated round within the configured phase timeout.
#[tokio::test]
async fn silent_reconcile_phase_times_out_and_retries() {
    use actordb::proto::{Envelope, EnvelopeMsg, Hello};
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-timeout-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }

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

    node.shutdown().await;
}

/// Reset completion is fenced by the fragment-wide mutation revision, not
/// only by local sender Enqueue events. An inbound replicated write racing
/// after the snapshot must force another verification round.
#[tokio::test]
async fn inbound_mutation_after_snapshot_invalidates_reset_completion() {
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, ReconcileResponse, ReplicateBatch, ReplicateOp,
        ReplicationAckLevel,
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-revision-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }

    node.put("revision-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("revision-b".to_string(), b"b".to_vec())
        .await
        .unwrap();
    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: Vec::new(),
                session_id: request.session_id,
                round_id: request.round_id,
            })),
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

    node.shutdown().await;
}

/// Fetched reconciliation data is an exact response contract: missing or
/// unrequested entries cannot be partially applied and then certified by a
/// barrier. Each malformed chunk aborts the round and schedules a retry.
#[tokio::test]
async fn malformed_reconcile_data_is_rejected_and_retried() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, Hello, KeySummary, ReconcileData, ReconcileResponse, SyncEntry,
    };
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-data-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }
    node.put("data-a".to_string(), b"a".to_vec()).await.unwrap();
    node.put("data-b".to_string(), b"b".to_vec()).await.unwrap();

    let remote_timestamp = now_ms();
    let summary = KeySummary {
        key: "remote-fetch".to_string(),
        timestamp_ms: remote_timestamp,
        node_id: "raw-data-peer".to_string(),
        is_tombstone: false,
    };
    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary.clone()],
                session_id: first.session_id,
                round_id: first.round_id,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 0,
                entries: Vec::new(),
                session_id: first.session_id,
                round_id: first.round_id,
                chunk_seq: first_fetch.chunk_seq,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary],
                session_id: second.session_id,
                round_id: second.round_id,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 0,
                entries: vec![SyncEntry {
                    key: "unrequested-key".to_string(),
                    value: b"bad".to_vec(),
                    // Malformed/unrequested data is rejected before HLC
                    // observation, so it cannot poison or tear down the live
                    // connection with an otherwise invalid timestamp.
                    timestamp_ms: u64::MAX,
                    node_id: "raw-data-peer".to_string(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                session_id: second.session_id,
                round_id: second.round_id,
                chunk_seq: second_fetch.chunk_seq,
            })),
        },
    )
    .await;
    let third = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            if request.round_id != second.round_id {
                break request;
            }
        }
    };
    assert_eq!(third.session_id, first.session_id);
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

    node.shutdown().await;
}

/// Phase 5: batches from a stale session are discarded. We replay a
/// batch with a bogus session id over a raw TCP connection; the
/// receiver must drop it.
#[tokio::test]
async fn stale_session_batches_discarded() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, Hello, ReplicateBatch, ReplicateOp, ReplicationAckLevel,
    };
    use tokio::io::AsyncWriteExt;

    let a = test_node("a").await;
    let b = test_node("b").await;
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;

    // Hijack: open a raw TCP connection to b, claim to be "a", and send
    // a batch with a session id b never issued. It must be discarded.
    let mut stream = tokio::net::TcpStream::connect(b.addr()).await.unwrap();
    let mut hello_buf = Vec::new();
    Envelope {
        msg: Some(EnvelopeMsg::Hello(Hello {
            node_id: "a".to_string(),
            auth_tag: Vec::new(),
        })),
    }
    .encode(&mut hello_buf)
    .unwrap();
    stream.write_u32(hello_buf.len() as u32).await.unwrap();
    stream.write_all(&hello_buf).await.unwrap();

    // Give b a moment to process the hello.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut batch_buf = Vec::new();
    Envelope {
        msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
            session_id: 0xDEADBEEF, // bogus: no such session
            batch_seq: 0,
            fragment: 0,
            ops: vec![ReplicateOp {
                key: "evil".to_string(),
                value: b"bad".to_vec(),
                timestamp_ms: u64::MAX, // would win any LWW...
                node_id: "a".to_string(),
                seq: 999,
                boot_id: 1,
                expires_at_ms: 0,
                is_tombstone: false,
            }],
            required_ack_level: ReplicationAckLevel::Applied as i32,
        })),
    }
    .encode(&mut batch_buf)
    .unwrap();
    stream.write_u32(batch_buf.len() as u32).await.unwrap();
    stream.write_all(&batch_buf).await.unwrap();

    // The batch must NOT be applied, despite the huge timestamp.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(b.get("evil".to_string()).await.unwrap().is_none());

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn current_session_rejects_far_future_timestamp() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, Hello, ReplicateBatch, ReplicateOp, ReplicationAckLevel,
        SessionStart,
    };
    use tokio::io::AsyncWriteExt;

    let node = test_node("receiver").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    let frames = [
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
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
        let mut encoded = Vec::new();
        frame.encode(&mut encoded).unwrap();
        stream.write_u32(encoded.len() as u32).await.unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    stream.flush().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(node.get("poisoned".to_string()).await.unwrap().is_none());
    node.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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
    b.shutdown().await;
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

    // Kill c too. Now no replica of fragment 1 is reachable: its keys
    // must error per-key while fragment-0 keys still succeed.
    c.shutdown().await;
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

    a.shutdown().await;
}

#[tokio::test]
async fn batch_put_fails_over_after_a_stale_owner_returns_unavailable() {
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
        "stale-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(map(&["stale-b", "stale-c"])),
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

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// Phase 7: saturating the background pool must not degrade
/// high-priority read latency — the pools are isolated.
#[tokio::test]
async fn background_saturation_leaves_high_priority_p99_unaffected() {
    use actordb::actor::{OpPriority, PoolConfig};
    use std::sync::Arc;
    use std::time::Instant;

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

    // Flood the background pool with bulk writes for the duration of
    // the measurement.
    let flood = {
        let n = n.clone();
        tokio::spawn(async move {
            for i in 0..200u32 {
                let kvs: Vec<(String, Vec<u8>)> = (0..25)
                    .map(|j| (format!("bg{i}-{j}"), vec![0u8; 32]))
                    .collect();
                let _ = n.put_multi(kvs).await;
            }
        })
    };

    // High-priority reads stay fast while the background pool is
    // saturated: separate pool, separate worker, separate counters.
    let mut latencies = Vec::with_capacity(40);
    for _ in 0..40 {
        let t = Instant::now();
        let r = n
            .get_with_priority("hot".to_string(), OpPriority::High)
            .await;
        latencies.push(t.elapsed());
        assert!(r.unwrap().is_some(), "hot key should read back");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    flood.abort();

    latencies.sort();
    let p99 = latencies[(latencies.len() * 99) / 100];
    assert!(
        p99 < Duration::from_millis(500),
        "high-priority p99 {p99:?} degraded under background saturation"
    );

    n.shutdown().await;
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

    n.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
}
/// Phase 8: migrate a fragment between two live nodes under continuous
/// writes. Zero lost writes; reads stay correct throughout.
#[tokio::test]
async fn placement_migration_no_lost_writes() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::replication::SenderConfig;
    use actordb::shard::{fragment_of, FragmentId};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    assert!(!keys.is_empty(), "need keys in fragment 0");

    // Seed initial values via A.
    for k in &keys {
        a.put(k.clone(), b"v0".to_vec()).await.unwrap();
    }

    // Continuous writer via A (single writer, so HLC timestamps respect
    // write order). Records the latest acknowledged value per key.
    let expected: Arc<tokio::sync::Mutex<HashMap<String, Vec<u8>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let max_round = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let a = a.clone();
        let expected = expected.clone();
        let max_round = max_round.clone();
        let stop = stop.clone();
        let keys = keys.clone();
        tokio::spawn(async move {
            let mut round: u64 = 1;
            while !stop.load(Ordering::Relaxed) {
                for k in &keys {
                    let v = format!("v{round}").into_bytes();
                    // A may forward to B once it stops serving; the write
                    // still lands.
                    if a.put(k.clone(), v.clone()).await.is_ok() {
                        expected.lock().await.insert(k.clone(), v);
                        max_round.store(round, Ordering::Relaxed);
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
    common::wait_for(
        || async {
            b.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(20),
    )
    .await;

    // Reads during the overlap: every value must be one the writer
    // actually wrote (well-formed, and not from the future).
    for k in &keys {
        let got = b.get(k.clone()).await.unwrap().expect("key missing");
        let s = String::from_utf8(got.value.clone()).unwrap();
        assert!(s.starts_with('v'), "unexpected value {s} for {k}");
        let round: u64 = s[1..].parse().expect("value should be v<round>");
        assert!(
            round <= max_round.load(Ordering::Relaxed),
            "read from the future for {k}: {s}"
        );
    }

    // Revoke A: [B] only.
    let mut final_map = HashMap::new();
    final_map.insert(FragmentId(0), vec!["b".to_string()]);
    final_map.insert(FragmentId(1), vec!["a".to_string()]);
    controller.set_placement(final_map);

    // Wait until A drops fragment 0.
    common::wait_for(
        || async {
            a.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Stopped)
        },
        Duration::from_secs(20),
    )
    .await;

    // Stop the writer; B must converge to the final value of every key:
    // zero lost writes.
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    let expected = expected.lock().await.clone();
    assert!(!expected.is_empty());
    common::wait_for(
        || async {
            for (k, v) in &expected {
                match b.get(k.clone()).await.unwrap().map(|x| x.value) {
                    Some(got) if got == *v => {}
                    _ => return false,
                }
            }
            true
        },
        Duration::from_secs(15),
    )
    .await;

    a.shutdown().await;
    b.shutdown().await;
}

/// A sender awaiting overflow reconciliation is not drained merely because
/// its ordinary buffer and in-flight batch count are both zero. Placement
/// revocation must keep the fragment subtree alive past the warning threshold
/// and tear it down only after reset actually completes.
#[tokio::test]
async fn placement_drain_waits_for_buffer_reset() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, ReconcileResponse, ReplicationAckLevel,
    };
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let initial = HashMap::from([(
        FragmentId(0),
        vec!["drain-a".to_string(), "raw-drain-peer".to_string()],
    )]);
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
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-drain-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;

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
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: Vec::new(),
                session_id: request.session_id,
                round_id: request.round_id,
            })),
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

    node.shutdown().await;
}

/// A placement reversal cancels an in-progress loss transition. The stale
/// drainer must not tear down a fragment that the controller made
/// authoritative again while BufferReset was still recovering.
#[tokio::test]
async fn placement_readd_while_draining_cancels_teardown() {
    use actordb::placement::{FragmentPlacementState, ManualPlacement};
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, ReconcileResponse, ReplicationAckLevel,
    };
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;

    let overlap = HashMap::from([(
        FragmentId(0),
        vec!["readd-a".to_string(), "raw-readd-peer".to_string()],
    )]);
    let controller = Arc::new(ManualPlacement::new(overlap.clone()));
    let node = Node::start(
        "readd-a".to_string(),
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-readd-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
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
    controller.set_placement(overlap);
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(5),
    )
    .await;

    let request = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: Vec::new(),
                session_id: request.session_id,
                round_id: request.round_id,
            })),
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

    node.shutdown().await;
}

/// Phase 8: rolling restart with dynamic placement. The survivor keeps
/// serving; the restarted node catches up via delta reconciliation
/// only — no full-state resync.
#[tokio::test]
async fn placement_rolling_restart_delta_reconcile_only() {
    use actordb::placement::ManualPlacement;
    use actordb::replication::SenderConfig;
    use actordb::shard::FragmentId;
    use std::collections::HashMap;
    use std::sync::Arc;

    let frag_count: usize = 2;
    let mut map = HashMap::new();
    map.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
    map.insert(FragmentId(1), vec!["a".to_string(), "b".to_string()]);
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
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    a.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
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
    a.shutdown().await;
    // The surviving replica keeps serving throughout.
    assert_eq!(
        b.get("rk-0".to_string())
            .await
            .unwrap()
            .expect("b should serve during a's restart")
            .value,
        b"rv-0".to_vec()
    );
    let a2 = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .unwrap();
    a2.connect(b.addr()).await.unwrap();
    common::wait_for(
        || async { a2.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(10),
    )
    .await;

    // A2 must converge to the full dataset...
    common::wait_for(
        || async {
            let all = a2.get_all().await.unwrap();
            (0..50).all(|i| {
                all.get(&format!("rk-{i}")).map(|v| v.value.clone())
                    == Some(format!("rv-{i}").into_bytes())
            })
        },
        Duration::from_secs(15),
    )
    .await;

    // ...without any full-state resync: the handshake skips SyncRequest
    // in dynamic mode, so all catch-up came from the Phase 5 delta
    // reconciler.
    assert_eq!(
        a2.sync_entries_applied(),
        0,
        "restarted node used full-state sync instead of delta reconciliation"
    );

    a2.shutdown().await;
    b.shutdown().await;
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

    n.shutdown().await;
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

    n.shutdown().await;
}

/// Regression test for adversarial review #3: a ForwardedPut arriving
/// before any Hello (no completed handshake) must be ignored, not applied.
#[tokio::test]
async fn pre_handshake_forwarded_put_ignored() {
    use actordb::proto::{Envelope, EnvelopeMsg, ForwardedPut};
    use tokio::io::AsyncWriteExt;

    let a = test_node("a").await;

    // Raw TCP, no Hello: send a ForwardedPut directly.
    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    let mut buf = Vec::new();
    Envelope {
        msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
            key: "evil".to_string(),
            value: b"evil-val".to_vec(),
            timestamp_ms: 1,
            node_id: "attacker".to_string(),
            expires_at_ms: 0,
            is_tombstone: false,
            seq: 0,
            boot_id: 0,
            priority: 0, // PRIORITY_DEFAULT
            hops: 0,
            req_id: 1,
        })),
    }
    .encode(&mut buf)
    .unwrap();
    stream.write_u32(buf.len() as u32).await.unwrap();
    stream.write_all(&buf).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // The key must not exist: the pre-handshake write was dropped.
    let got = a.get("evil".to_string()).await.unwrap();
    assert!(
        got.is_none(),
        "pre-handshake ForwardedPut was applied! got {got:?}"
    );

    a.shutdown().await;
}

/// Regression test for adversarial review #3: with a cluster secret
/// configured, a Hello with a bad auth tag must drop the connection.
#[tokio::test]
async fn hello_auth_mismatch_drops_connection() {
    use actordb::proto::{Envelope, EnvelopeMsg, Hello};

    let config = NodeConfig {
        cluster_secret: Some(b"correct-secret".to_vec()),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();

    // Wrong secret: HMAC won't verify.
    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    let mut buf = Vec::new();
    Envelope {
        msg: Some(EnvelopeMsg::Hello(Hello {
            node_id: "b".to_string(),
            auth_tag: b"wrong-tag".to_vec(),
        })),
    }
    .encode(&mut buf)
    .unwrap();
    stream.write_u32(buf.len() as u32).await.unwrap();
    stream.write_all(&buf).await.unwrap();

    // The server must close the connection: read returns 0 (EOF).
    let mut read_buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut read_buf))
        .await
        .expect("timed out waiting for close")
        .unwrap();
    assert_eq!(n, 0, "expected connection to be closed on auth failure");

    // And "b" must not be a peer.
    assert_eq!(a.peer_count().await, 0, "unauthenticated peer was accepted");

    a.shutdown().await;
}

/// Regression test for adversarial review #4: a Hello claiming our own
/// node_id must be rejected.
#[tokio::test]
async fn hello_claiming_self_id_rejected() {
    use actordb::proto::{Envelope, EnvelopeMsg, Hello};

    let a = test_node("a").await;

    let mut stream = tokio::net::TcpStream::connect(a.addr()).await.unwrap();
    let mut buf = Vec::new();
    Envelope {
        msg: Some(EnvelopeMsg::Hello(Hello {
            node_id: "a".to_string(), // our own id!
            auth_tag: Vec::new(),
        })),
    }
    .encode(&mut buf)
    .unwrap();
    stream.write_u32(buf.len() as u32).await.unwrap();
    stream.write_all(&buf).await.unwrap();

    let mut read_buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut read_buf))
        .await
        .expect("timed out waiting for close")
        .unwrap();
    assert_eq!(n, 0, "expected connection to be closed on self-id Hello");

    a.shutdown().await;
}

/// Peer identity is immutable for the life of a connection. Accepting a
/// second Hello used to let one socket replace its identity and rotate every
/// sender session while leaving stale peer-map entries behind.
#[tokio::test]
async fn second_hello_closes_established_connection() {
    use actordb::proto::{Envelope, EnvelopeMsg, Hello};

    let node = test_node("hello-server").await;
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    for peer in ["first-peer", "second-peer"] {
        let mut encoded = Vec::new();
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: peer.to_string(),
                auth_tag: Vec::new(),
            })),
        }
        .encode(&mut encoded)
        .unwrap();
        stream.write_u32(encoded.len() as u32).await.unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    stream.flush().await.unwrap();

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

    node.shutdown().await;
}

/// Replaying SessionStart with the same id must not clear a failed-repair
/// latch. Otherwise an empty tracked barrier could ACK immediately after an
/// untracked repair failed and falsely certify convergence.
#[tokio::test]
async fn same_session_replay_does_not_clear_reconcile_failure() {
    use actordb::proto::{
        Envelope, EnvelopeMsg, Hello, ReplicateBatch, ReplicateOp, ReplicationAckLevel,
        SessionStart,
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-poison-sender".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;

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

    node.shutdown().await;
}

/// Positive case: two nodes with the same cluster secret complete the
/// handshake and replicate.
#[tokio::test]
async fn cluster_secret_handshake_succeeds() {
    let secret = b"shared-secret".to_vec();
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_applied_ack_does_not_force_a_receiver_store_flush() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(GatedStore::default());
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
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_ack_waits_for_the_receiver_store_fence() {
    use actordb::replication::{ReplicationAckDurability, SenderConfig};
    use actordb::shard::FragmentId;

    let store = Arc::new(GatedStore::default());
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
    assert_eq!(blocked.last_ack_durability, None);
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.put("memory-only-target".to_string(), b"value".to_vec())
        .await
        .unwrap();
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
        b.get("memory-only-target".to_string())
            .await
            .unwrap()
            .map(|value| value.value),
        Some(b"value".to_vec())
    );

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
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

    a.shutdown().await;
    b.shutdown().await;
}

/// A failed local StoreCommitted fence remains required even when the retry
/// sees the fetched winner already present in cache and therefore needs no
/// second fetch. The remote barrier alone cannot certify requester durability.
#[tokio::test]
async fn fetched_reconcile_value_retries_failed_local_store_fence() {
    use actordb::proto::{
        BatchAck, Envelope, EnvelopeMsg, Hello, KeySummary, ReconcileData, ReconcileResponse,
        ReplicationAckLevel, SyncEntry,
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
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: "raw-fetch-peer".to_string(),
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
    loop {
        if matches!(
            recv_frame(&mut stream).await.msg,
            Some(EnvelopeMsg::SessionStart(_))
        ) {
            break;
        }
    }
    node.put("fetch-local-a".to_string(), b"a".to_vec())
        .await
        .unwrap();
    node.put("fetch-local-b".to_string(), b"b".to_vec())
        .await
        .unwrap();

    let remote_timestamp = now_ms();
    let summary = KeySummary {
        key: "fetched-durable".to_string(),
        timestamp_ms: remote_timestamp,
        node_id: "raw-fetch-peer".to_string(),
        is_tombstone: false,
    };
    let first = loop {
        if let Some(EnvelopeMsg::ReconcileRequest(request)) = recv_frame(&mut stream).await.msg {
            break request;
        }
    };
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary.clone()],
                session_id: first.session_id,
                round_id: first.round_id,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 0,
                entries: vec![SyncEntry {
                    key: summary.key.clone(),
                    value: b"durable".to_vec(),
                    timestamp_ms: summary.timestamp_ms,
                    node_id: summary.node_id.clone(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                session_id: first.session_id,
                round_id: first.round_id,
                chunk_seq: fetch.chunk_seq,
            })),
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
            msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                summaries: vec![summary],
                session_id: second.session_id,
                round_id: second.round_id,
            })),
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

    node.shutdown().await;
}
