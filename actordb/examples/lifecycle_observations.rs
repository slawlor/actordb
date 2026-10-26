//! Owned startup plus bounded backend-neutral operational observations.
use actordb::node::metrics::MetricPoint;
use actordb::node::{DetailedSnapshotOptions, Node, NodeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut startup = Node::begin_start(
        "example".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::default(),
    );
    println!("starting: {:?}", startup.snapshot().stage);
    let node = startup.wait().await?;
    node.enable_metrics();
    node.put("hello".into(), b"world".to_vec()).await?;
    let snapshot = node
        .collect_metrics_detailed(
            DetailedSnapshotOptions::default(),
            &|point: &MetricPoint| {
                println!(
                    "{} {:?}: {:?}",
                    point.descriptor.name, point.scope, point.value
                );
            },
        )
        .await?;
    println!(
        "lifecycle {:?}, complete {}",
        snapshot.status.lifecycle,
        snapshot.status.is_complete()
    );
    node.shutdown().await?;
    Ok(())
}
