//! Forward versioned TTL notifications to a bounded application worker.
//! Run: cargo run -p actordb --example ttl_expiry_callbacks

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actordb::node::{Node, NodeConfig};
use actordb::ttl_expiry::{expiry_handler_fn, ExpiryBatch, ExpiryCallbackError, TtlExpiryOptions};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This index is application state; the database chooses no recipients,
    // ownership policy, fanout mechanism, or application payload encoding.
    let index = Arc::new(Mutex::new(HashMap::<String, (u64, String)>::new()));
    let (sender, mut receiver) = mpsc::channel::<ExpiryBatch>(8);
    let (expired, observed_expiry) = oneshot::channel();
    let worker_index = index.clone();
    let mut workers = JoinSet::new();
    workers.spawn(async move {
        let mut expired = Some(expired);
        while let Some(batch) = receiver.recv().await {
            for event in batch.events {
                let mut index = worker_index.lock().await;
                // Atomically compare and clear in application state. A delayed
                // event or replica duplicate must never remove a newer version.
                let same_version = index.get(&event.key).is_some_and(|(timestamp, writer)| {
                    (*timestamp, writer.as_str()) == event.original_version()
                });
                if same_version {
                    index.remove(&event.key);
                    println!("Applied expiry {} from {}", event.id, batch.local_node_id);
                    if let Some(expired) = expired.take() {
                        let _ = expired.send(());
                    }
                }
            }
        }
    });

    let handler = expiry_handler_fn(move |batch: ExpiryBatch| {
        let sender = sender.clone();
        async move {
            // The application channel is independently bounded. This handler
            // reports saturation; ActorDB records the error without retrying.
            sender
                .try_send(batch)
                .map_err(|_| ExpiryCallbackError::new("application worker unavailable or full"))
        }
    });
    let config = NodeConfig::builder()
        .ttl_expiry_handler(Arc::new(handler))
        .ttl_expiry_options(
            TtlExpiryOptions::builder()
                .max_batch_delay(Duration::from_millis(10))
                .build(),
        )
        .clean_interval(Duration::from_millis(25))
        .build();
    let node = Node::start("expiry-example".into(), "127.0.0.1:0".parse()?, config).await?;
    node.put_with_ttl(
        "item".into(),
        vec![0xff, 0, 0x80],
        Some(Duration::from_secs(1)),
    )
    .await?;
    let value = node
        .get("item".into())
        .await?
        .ok_or_else(|| ExpiryCallbackError::new("example record expired before indexing"))?;
    index
        .lock()
        .await
        .insert("item".into(), (value.timestamp_ms, value.node_id));
    tokio::time::timeout(Duration::from_secs(5), observed_expiry).await??;
    assert!(index.lock().await.is_empty());
    node.shutdown().await?;
    println!("{:?}", node.ttl_expiry_diagnostics().unwrap());
    // Node shutdown releases the handler's sender. The application explicitly
    // joins its own worker; JoinSet also aborts it if the example exits early.
    while let Some(result) = workers.join_next().await {
        result?;
    }
    Ok(())
}
