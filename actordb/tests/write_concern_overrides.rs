mod common;

use std::collections::HashMap;
use std::time::Duration;

use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::shard::FragmentId;
use actordb::write_concern::{WriteConcern, WriteConcernError, WriteConcernOptions};

fn config() -> NodeConfig {
    NodeConfig::builder()
        .fragment_map(HashMap::from([(
            FragmentId(0),
            vec!["receiver".into(), "offline-b".into(), "offline-c".into()],
        )]))
        .build()
}

fn majority() -> WriteConcernOptions {
    WriteConcernOptions::builder()
        .concern(WriteConcern::Majority)
        .timeout(Duration::from_millis(20))
        .build()
}

#[tokio::test]
async fn startup_cap_runtime_validation_and_clear_change_subsequent_writes() {
    let receiver = Node::start_with_options(
        "receiver".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        NodeRuntimeOptions::default().with_write_concern_acknowledgement_cap(1),
    )
    .await
    .unwrap();
    let receipt = receiver
        .put_with_write_concern("startup".into(), vec![1], majority())
        .await
        .unwrap();
    assert_eq!(receipt.requested_acknowledgements(), 2);
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert!(receipt.was_overridden());
    assert_eq!(receiver.write_concern_acknowledgement_cap(), Some(1));
    assert!(matches!(
        receiver.set_write_concern_acknowledgement_cap(0),
        Err(WriteConcernError::InvalidPolicy(_))
    ));
    assert_eq!(receiver.write_concern_acknowledgement_cap(), Some(1));

    receiver.clear_write_concern_acknowledgement_cap();
    let Err(WriteConcernError::TimedOut(receipt)) = receiver
        .put_with_write_concern("cleared".into(), vec![2], majority())
        .await
    else {
        panic!("cleared cap must restore the requested majority");
    };
    assert_eq!(receipt.required_acknowledgements(), 2);
    assert!(!receipt.was_overridden());
    receiver.shutdown().await.unwrap();
}

#[tokio::test]
async fn forwarded_write_uses_receiving_node_cap_and_returns_its_receipt() {
    let receiver = Node::start("receiver".into(), "127.0.0.1:0".parse().unwrap(), config())
        .await
        .unwrap();
    let caller = Node::start("caller".into(), "127.0.0.1:0".parse().unwrap(), config())
        .await
        .unwrap();
    caller.connect(receiver.addr()).await.unwrap();
    common::wait_for(
        || async { caller.peer_count().await == 1 },
        Duration::from_secs(3),
    )
    .await;

    receiver.set_write_concern_acknowledgement_cap(1).unwrap();
    let receipt = caller
        .put_with_write_concern("forwarded-receiver-cap".into(), vec![1], majority())
        .await
        .unwrap();
    assert_eq!(receipt.receiving_node(), "receiver");
    assert_eq!(receipt.requested_acknowledgements(), 2);
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert!(receipt.was_overridden());

    receiver.clear_write_concern_acknowledgement_cap();
    caller.set_write_concern_acknowledgement_cap(1).unwrap();
    let Err(WriteConcernError::TimedOut(receipt)) = caller
        .put_with_write_concern("forwarded-caller-cap".into(), vec![2], majority())
        .await
    else {
        panic!("the caller's cap must not weaken the receiving node's threshold");
    };
    assert_eq!(receipt.receiving_node(), "receiver");
    assert_eq!(receipt.required_acknowledgements(), 2);
    assert!(!receipt.was_overridden());
    caller.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
