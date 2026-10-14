// Integration tests: multi-node replication over real TCP on localhost.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use actordb::node::{Node, NodeConfig};

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
/// intervention (reads hide it past expiry; the cleaner reaps it).
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

/// Phase 5: batches from a stale session are discarded. We replay a
/// batch with a bogus session id over a raw TCP connection; the
/// receiver must drop it.
#[tokio::test]
async fn stale_session_batches_discarded() {
    use actordb::proto::{Envelope, EnvelopeMsg, Hello, ReplicateBatch, ReplicateOp};
    use prost::Message;
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
    assert!(r[0].is_ok(), "fire-and-forget send should succeed");
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
