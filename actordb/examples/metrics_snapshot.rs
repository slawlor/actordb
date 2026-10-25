//! Record an owned, bounded observation without selecting a metrics backend.
//! Run: cargo run -p actordb --example metrics_snapshot

use std::sync::Mutex;

use actordb::node::metrics::{MetricPoint, MetricValue, MetricsSink};
use actordb::node::{Node, NodeConfig};

#[derive(Default)]
struct RecordingAdapter(Mutex<Vec<MetricPoint>>);

impl MetricsSink for RecordingAdapter {
    fn record(&self, point: &MetricPoint) {
        // This sample retains one collection only. A real adapter replaces
        // cumulative observations or derives deltas; it never blindly adds
        // each observed total to a backend counter.
        self.0.lock().unwrap().push(point.clone());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::start(
        "metrics-example".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::builder().fragment_count(1).build(),
    )
    .await?;
    node.put("example".into(), vec![1, 2, 3]).await?;
    let adapter = RecordingAdapter::default();
    let snapshot = node.collect_metrics(&adapter);
    println!(
        "node={} incarnation={} readiness={:?} complete={}",
        snapshot.status.node_id,
        snapshot.status.boot_id,
        snapshot.status.readiness,
        snapshot.status.observation.is_complete(),
    );
    for point in adapter.0.lock().unwrap().iter() {
        let value = match point.value {
            MetricValue::Counter(total) => format!("cumulative {total}"),
            MetricValue::Gauge(current) => format!("current {current}"),
            MetricValue::Distribution(summary) => format!(
                "samples={} sum={} max={}",
                summary.count, summary.sum, summary.max,
            ),
            _ => "unsupported value kind".into(),
        };
        println!(
            "{} {:?}: {} {:?}",
            point.descriptor.name, point.scope, value, point.descriptor.unit
        );
    }
    node.shutdown().await?;
    Ok(())
}
