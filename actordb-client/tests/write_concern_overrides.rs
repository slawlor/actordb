use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::shard::FragmentId;
use actordb_client::{
    Client, ClientOptions, WriteConcern, WriteConcernClientError, WriteConcernOptions,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn sdk_receipt_discloses_receiving_node_acknowledgement_override() {
    let node = Arc::new(
        Node::start_with_options(
            "sdk-receiver".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .fragment_map(HashMap::from([(
                    FragmentId(0),
                    vec![
                        "sdk-receiver".into(),
                        "offline-b".into(),
                        "offline-c".into(),
                    ],
                )]))
                .build(),
            NodeRuntimeOptions::default().with_write_concern_acknowledgement_cap(1),
        )
        .await
        .unwrap(),
    );
    let mut server = ClientServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        ClientServerOptions::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap();
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Majority)
        .build();
    let receipt = client
        .put_with_write_concern("overridden", vec![1], options.clone())
        .await
        .unwrap();
    assert_eq!(receipt.receiving_node(), "sdk-receiver");
    assert_eq!(receipt.replica_count(), Some(3));
    assert_eq!(receipt.requested_acknowledgements(), 2);
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert_eq!(receipt.acknowledged_nodes(), &["sdk-receiver"]);
    assert!(receipt.was_overridden());

    let strict = options.clone().with_strict_acknowledgements(true);
    assert!(matches!(
        client
            .put_with_write_concern("strict-rejected", vec![9], strict.clone())
            .await,
        Err(WriteConcernClientError::InvalidPolicy(_))
    ));
    assert!(client.get("strict-rejected").await.unwrap().is_none());
    assert!(matches!(
        client.delete_with_write_concern("overridden", strict).await,
        Err(WriteConcernClientError::InvalidPolicy(_))
    ));
    assert_eq!(
        client.get("overridden").await.unwrap().unwrap().bytes,
        vec![1]
    );

    let receipt = client
        .delete_with_write_concern("overridden", options)
        .await
        .unwrap();
    assert_eq!(receipt.requested_acknowledgements(), 2);
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert!(receipt.was_overridden());
    assert!(client.get("overridden").await.unwrap().is_none());

    let receipt = client
        .put_with_write_concern("local", vec![2], WriteConcernOptions::default())
        .await
        .unwrap();
    assert_eq!(receipt.requested_acknowledgements(), 1);
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert!(!receipt.was_overridden());

    client.close().await;
    server.shutdown().await;
    node.shutdown().await.unwrap();
}
