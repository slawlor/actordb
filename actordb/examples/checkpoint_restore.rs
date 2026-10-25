//! Run with --features rocksdb --example checkpoint_restore -- SOURCE TARGET BUNDLE

#[cfg(feature = "rocksdb")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use actordb::checkpoint::{CheckpointImporter, CheckpointOptions};
    use actordb::node::{Node, NodeConfig};
    use actordb::store::{RocksDbStore, StoreIdentity};
    use std::sync::Arc;

    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 3 {
        println!("usage: checkpoint_restore SOURCE_STORE TARGET_STORE CHECKPOINT_BUNDLE");
        return Ok(());
    }
    let source = Arc::new(RocksDbStore::open(&arguments[0])?);
    let node = Node::start(
        "checkpoint-source".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::builder()
            .fragment_count(1)
            .store(source)
            .build(),
    )
    .await?;
    node.table("Example")?
        .put("opaque", vec![0xff, 0, 0x80])
        .await?;
    node.shutdown().await?;
    let manifest = node
        .export_checkpoint(&arguments[2], CheckpointOptions::default())
        .await?;
    let target = Arc::new(RocksDbStore::open(&arguments[1])?);
    let import_target = target.clone();
    let bundle = arguments[2].clone();
    let receipt = tokio::task::spawn_blocking(move || {
        CheckpointImporter::open(
            import_target,
            StoreIdentity::new("checkpoint-target".into(), 1),
            bundle,
        )?
        .finish()
    })
    .await??;
    let restored = Node::start(
        "checkpoint-target".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::builder()
            .fragment_count(1)
            .store(target)
            .build(),
    )
    .await?;
    let value = restored.table("Example")?.get("opaque").await?.unwrap();
    assert_eq!(value.value, vec![0xff, 0, 0x80]);
    println!(
        "imported {} records from {} into {}; historical writer={}",
        manifest.records(),
        receipt.source().node_id(),
        receipt.target().node_id(),
        value.node_id
    );
    restored.shutdown().await?;
    Ok(())
}

#[cfg(not(feature = "rocksdb"))]
fn main() {
    println!("checkpoint_restore requires --features rocksdb");
}
