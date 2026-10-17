// Integration tests: Node::scan over local fragments.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use actordb::actor::{KeyMatcher, ScanSpec, ValueTransform};
use actordb::node::scan::ScanError;
use actordb::node::{Node, NodeConfig, NodeError};
use actordb::shard::{fragment_of, FragmentId};

mod common;

async fn test_node(id: &str, config: NodeConfig) -> Node {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(id.to_string(), addr, config)
        .await
        .expect("node failed to start")
}

fn scan_all() -> ScanSpec {
    ScanSpec {
        matcher: KeyMatcher::All,
        transform: ValueTransform::Identity,
        limit: 0,
        offset: 0,
    }
}

#[tokio::test]
async fn node_scan_finds_all_keys_across_fragments() {
    let node = test_node(
        "scan-basic",
        NodeConfig {
            fragment_count: 4,
            ..NodeConfig::default()
        },
    )
    .await;

    // 40 keys spread across all 4 fragments by the key hash.
    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..40 {
        let key = format!("key-{i:03}");
        let value = format!("value-{i}").into_bytes();
        node.put(key.clone(), value.clone()).await.unwrap();
        expected.insert(key, value);
    }

    let matches = node.scan(&scan_all()).await.expect("scan failed");
    assert_eq!(matches.len(), 40);
    let found: HashMap<String, Vec<u8>> = matches.into_iter().map(|m| (m.key, m.value)).collect();
    assert_eq!(found, expected, "every key exactly once with its value");

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_prefix_matcher_and_transforms() {
    let node = test_node("scan-filter", NodeConfig::default()).await;

    for i in 0..10 {
        node.put(format!("user:{i}"), format!("name-{i}").into_bytes())
            .await
            .unwrap();
        node.put(format!("order:{i}"), format!("item-{i}").into_bytes())
            .await
            .unwrap();
    }

    // Prefix + KeyOnly.
    let matches = node
        .scan(&ScanSpec {
            matcher: KeyMatcher::Prefix(b"user:".to_vec()),
            transform: ValueTransform::KeyOnly,
            limit: 0,
            offset: 0,
        })
        .await
        .expect("prefix scan failed");
    assert_eq!(matches.len(), 10);
    assert!(matches.iter().all(|m| m.key.starts_with("user:")));
    assert!(matches.iter().all(|m| m.value.is_empty()));

    // PrefixBytes transform.
    let matches = node
        .scan(&ScanSpec {
            matcher: KeyMatcher::Prefix(b"order:".to_vec()),
            transform: ValueTransform::PrefixBytes(4),
            limit: 0,
            offset: 0,
        })
        .await
        .expect("prefix-bytes scan failed");
    assert_eq!(matches.len(), 10);
    assert!(matches.iter().all(|m| m.value == b"item"));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_hides_tombstones_and_expired() {
    let node = test_node("scan-visibility", NodeConfig::default()).await;

    node.put("live".to_string(), b"v".to_vec()).await.unwrap();
    node.put("dead".to_string(), b"v".to_vec()).await.unwrap();
    node.delete("dead".to_string()).await.unwrap();

    // Expiry can race admission on a loaded or instrumented runner. Both an
    // applied write and an explicitly expired write must disappear from scans.
    let result = node
        .put_with_ttl(
            "old".to_string(),
            b"v".to_vec(),
            Some(Duration::from_millis(100)),
        )
        .await;
    assert!(
        matches!(result, Ok(()) | Err(NodeError::Expired)),
        "unexpected TTL result: {result:?}"
    );
    common::wait_for(
        || async { node.get("old".to_string()).await.unwrap().is_none() },
        Duration::from_secs(5),
    )
    .await;

    let matches = node.scan(&scan_all()).await.expect("scan failed");
    let keys: Vec<&str> = matches.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["live"]);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_sharded_placement_errors_on_uncovered_fragments() {
    use actordb::node::scan::MAX_SCAN_TOTAL_MATCHES;
    let _ = MAX_SCAN_TOTAL_MATCHES;

    // Sharded placement: this node serves only fragment 0 of 2.
    let placement: HashMap<FragmentId, Vec<String>> = HashMap::from([
        (FragmentId(0), vec!["shard-a".to_string()]),
        (FragmentId(1), vec!["shard-b".to_string()]),
    ]);
    let node = test_node(
        "shard-a",
        NodeConfig {
            fragment_count: 2,
            fragment_map: placement,
            ..NodeConfig::default()
        },
    )
    .await;

    // Write a key that hashes to the locally-held fragment 0 (a key
    // hashing to fragment 1 would try to forward to the absent shard-b).
    let mut key = String::from("k0");
    for i in 0..1000 {
        key = format!("k{i}");
        if fragment_of(&key, 2) == FragmentId(0) {
            break;
        }
    }
    assert_eq!(fragment_of(&key, 2), FragmentId(0));
    node.put(key, b"v".to_vec()).await.unwrap();

    match node.scan(&scan_all()).await {
        Err(ScanError::FragmentsUnavailable(missing)) => {
            assert_eq!(missing, vec![FragmentId(1)]);
        }
        other => panic!("expected FragmentsUnavailable, got {other:?}"),
    }

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_sharded_placement_succeeds_when_fully_held() {
    // Same sharded map, but this node holds both fragments.
    let placement: HashMap<FragmentId, Vec<String>> = HashMap::from([
        (FragmentId(0), vec!["shard-full".to_string()]),
        (FragmentId(1), vec!["shard-full".to_string()]),
    ]);
    let node = test_node(
        "shard-full",
        NodeConfig {
            fragment_count: 2,
            fragment_map: placement,
            ..NodeConfig::default()
        },
    )
    .await;

    node.put("k1".to_string(), b"v".to_vec()).await.unwrap();
    node.put("k2".to_string(), b"v".to_vec()).await.unwrap();

    let matches = node.scan(&scan_all()).await.expect("scan failed");
    assert_eq!(matches.len(), 2);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_error_converts_to_node_error() {
    use actordb::node::NodeError;
    let err: NodeError = ScanError::FragmentsUnavailable(vec![FragmentId(3)]).into();
    assert!(matches!(err, NodeError::Actor(_)));
    let err: NodeError = ScanError::Backpressure("shed".to_string()).into();
    assert!(matches!(err, NodeError::Backpressure(_)));
    let err: NodeError = ScanError::ShuttingDown.into();
    assert!(matches!(err, NodeError::ShuttingDown));
}

async fn wait_for_peers(a: &Node, b: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if a.peer_count().await == 1 && b.peer_count().await == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("peers did not connect");
}

/// Two nodes, four fragments: a serves 0,1 and b serves 2,3. The
/// coordinator (a) scans local fragments in-process and forwards the
/// rest to b over the wire.
async fn two_node_cluster() -> (Node, Node) {
    let placement: HashMap<FragmentId, Vec<String>> = HashMap::from([
        (FragmentId(0), vec!["scan-a".to_string()]),
        (FragmentId(1), vec!["scan-a".to_string()]),
        (FragmentId(2), vec!["scan-b".to_string()]),
        (FragmentId(3), vec!["scan-b".to_string()]),
    ]);
    let config = || NodeConfig {
        fragment_count: 4,
        fragment_map: placement.clone(),
        ..NodeConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let a = Node::start("scan-a".to_string(), addr, config())
        .await
        .expect("node a failed to start");
    let b = Node::start("scan-b".to_string(), addr, config())
        .await
        .expect("node b failed to start");
    b.connect(a.addr()).await.unwrap();
    wait_for_peers(&a, &b).await;
    (a, b)
}

/// Find `count` keys hashing to `fragment` (fragment_count = 4).
fn keys_for_fragment(fragment: u32, count: usize, tag: &str) -> Vec<String> {
    use actordb::shard::fragment_of;
    let mut keys = Vec::new();
    let mut i = 0u64;
    while keys.len() < count {
        let key = format!("{tag}-{i:06}");
        if fragment_of(&key, 4) == FragmentId(fragment) {
            keys.push(key);
        }
        i += 1;
    }
    keys
}

#[tokio::test]
async fn node_scan_forwards_remote_fragments_to_holders() {
    let (a, b) = two_node_cluster().await;

    // 10 keys per fragment, written directly on the serving node.
    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for frag in 0..4u32 {
        let keys = keys_for_fragment(frag, 10, "fwd");
        let node = if frag < 2 { &a } else { &b };
        for key in &keys {
            let value = format!("value-{key}").into_bytes();
            node.put(key.clone(), value.clone()).await.unwrap();
            expected.insert(key.clone(), value);
        }
    }

    // Coordinator a: fragments 0,1 local; 2,3 forwarded to b.
    let matches = a.scan(&scan_all()).await.expect("scan failed");
    let found: HashMap<String, Vec<u8>> = matches.into_iter().map(|m| (m.key, m.value)).collect();
    assert_eq!(found, expected, "every key exactly once across holders");

    // And from the other side: b coordinates, a serves 0,1 remotely.
    let matches = b.scan(&scan_all()).await.expect("scan failed");
    assert_eq!(matches.len(), 40);

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_scan_fails_over_to_next_holder() {
    // Fragment 0 has two holders: a (down) and b (live). The coordinator
    // c must fail over from a to b.
    let placement: HashMap<FragmentId, Vec<String>> = HashMap::from([
        (
            FragmentId(0),
            vec!["fail-a".to_string(), "fail-b".to_string()],
        ),
        (FragmentId(1), vec!["fail-c".to_string()]),
    ]);
    let config = || NodeConfig {
        fragment_count: 2,
        fragment_map: placement.clone(),
        ..NodeConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // Note: fail-a is never started; fail-b holds fragment 0's replica.
    let b = Node::start("fail-b".to_string(), addr, config())
        .await
        .unwrap();
    let c = Node::start("fail-c".to_string(), addr, config())
        .await
        .unwrap();
    b.connect(c.addr()).await.unwrap();
    wait_for_peers(&b, &c).await;

    // Key hashing to fragment 0 (held by fail-b).
    use actordb::shard::fragment_of;
    let mut key = String::from("fk0");
    for i in 0..1000 {
        key = format!("fk{i}");
        if fragment_of(&key, 2) == FragmentId(0) {
            break;
        }
    }
    assert_eq!(fragment_of(&key, 2), FragmentId(0));
    b.put(key.clone(), b"v".to_vec()).await.unwrap();

    // c coordinates: fragment 1 local, fragment 0 via fail-a (dead) then
    // fail-b (live).
    let matches = c.scan(&scan_all()).await.expect("scan failed");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].key, key);

    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}
