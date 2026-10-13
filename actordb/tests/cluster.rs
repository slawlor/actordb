// Integration tests: multi-node replication over real TCP on localhost.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use actordb::node::Node;

async fn test_node(id: &str) -> Node {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(id.to_string(), addr)
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
