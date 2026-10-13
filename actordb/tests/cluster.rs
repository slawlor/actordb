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

#[tokio::test]
async fn persistence_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("actordb");

    // Write with persistence on, shut down cleanly, restart with no
    // peers: the full dataset must come back from disk alone.
    let a = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            db_path: Some(db_path.clone()),
            ..NodeConfig::default()
        },
    )
    .await
    .expect("node failed to start");

    a.put("k1".to_string(), b"v1".to_vec()).await.unwrap();
    a.put("k2".to_string(), b"v2".to_vec()).await.unwrap();
    a.shutdown().await;

    let a2 = Node::start(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig {
            db_path: Some(db_path),
            ..NodeConfig::default()
        },
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
        NodeConfig {
            db_path: Some(db_path.clone()),
            ..NodeConfig::default()
        },
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
        NodeConfig {
            db_path: Some(db_path),
            ..NodeConfig::default()
        },
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

#[tokio::test]
async fn persistence_flushes_on_shutdown() {
    // With a batch size and interval that never trigger mid-test, the
    // shutdown flush is the only thing persisting these writes.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("actordb");
    let config = || NodeConfig {
        db_path: Some(db_path.clone()),
        flush_batch_size: 1_000_000,
        flush_interval: Duration::from_secs(3600),
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
