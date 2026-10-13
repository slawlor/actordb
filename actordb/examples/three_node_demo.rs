// Three-node demo: spin up nodes on localhost, mesh them, write from
// each node, and print every node's local copy once gossip converges.
//
// Run with: cargo run --example three_node_demo

use std::time::Duration;

use actordb::node::{Node, NodeConfig};

#[tokio::main]
async fn main() {
    let a = Node::start(
        "node-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    let b = Node::start(
        "node-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    let c = Node::start(
        "node-c".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();

    // Star topology: b and c dial a. There is no direct b<->c link;
    // epidemic broadcast still converges everything everywhere.
    b.connect(a.addr()).await.unwrap();
    c.connect(a.addr()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    a.put("greeting".to_string(), b"hello from a".to_vec())
        .await
        .unwrap();
    b.put("counter".to_string(), b"42".to_vec()).await.unwrap();
    // Later write to the same key wins (last-writer-wins).
    tokio::time::sleep(Duration::from_millis(100)).await;
    c.put("greeting".to_string(), b"hello from c".to_vec())
        .await
        .unwrap();

    // Give gossip a moment to converge.
    tokio::time::sleep(Duration::from_secs(2)).await;

    for n in [&a, &b, &c] {
        println!("--- {} ({}) ---", n.id(), n.addr());
        let mut all: Vec<_> = n.get_all().await.unwrap().into_iter().collect();
        all.sort_by(|x, y| x.0.cmp(&y.0));
        for (key, v) in all {
            println!(
                "  {key} = {:?}  (ts={} origin={})",
                String::from_utf8_lossy(&v.value),
                v.timestamp_ms,
                v.node_id,
            );
        }
    }

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}
