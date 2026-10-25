//! Run a volatile local write and inspect its certified processing floor.
//! See docs/write-concerns.md for store/stable policies and durable backends.
use actordb::node::{Node, NodeConfig};
use actordb::write_concern::WriteConcernOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::start(
        "example".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::default(),
    )
    .await?;
    let receipt = node
        .put_with_write_concern(
            "hello".into(),
            b"world".to_vec(),
            WriteConcernOptions::builder()
                .require_evidence(true)
                .build(),
        )
        .await?;
    let evidence = receipt.evidence().expect("required evidence");
    let operation = evidence.operation();
    println!(
        "fragment {} version {:?} from {}",
        operation.fragment(),
        operation.version(),
        operation.originating_node()
    );
    for acknowledgement in evidence.acknowledgements() {
        println!(
            "{} reached {:?}",
            acknowledgement.node(),
            acknowledgement.durability()
        );
    }
    node.shutdown().await?;
    Ok(())
}
