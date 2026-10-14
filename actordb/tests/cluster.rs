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

    // Star topology: b and c dial a. Epidemic broadcast converges
    // everything everywhere despite no direct b<->c link.
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async {
            a.peer_count().await == 2 && b.peer_count().await == 1 && c.peer_count().await == 1
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
        worker_count: 8,
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
        worker_count: 2,
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
