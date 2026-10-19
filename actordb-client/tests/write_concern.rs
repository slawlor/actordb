use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use actordb_client::{
    Client, ClientOptions, WriteConcern, WriteConcernClientError, WriteConcernOptions,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn client_receives_partial_timeout_and_keeps_connection_for_later_operations() {
    let reservations: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: Vec<_> = reservations
        .iter()
        .map(|r| r.local_addr().unwrap())
        .collect();
    let ids = vec![
        "sdk-concern-a".to_string(),
        "sdk-concern-b".to_string(),
        "sdk-concern-c".to_string(),
    ];
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(FragmentId(0), ids.clone())]))
        .members(ids.iter().cloned().zip(addresses.iter().copied()).collect())
        .build();
    let mut reservations = reservations;
    drop(reservations.remove(0));
    let node = Arc::new(
        Node::start(ids[0].clone(), addresses[0], config)
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
    let client = Client::connect(
        server.addr(),
        ClientOptions {
            timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Majority)
        .timeout(Duration::from_millis(350))
        .build();
    let error = client
        .put_with_write_concern("wait", vec![1], options)
        .await
        .unwrap_err();
    let WriteConcernClientError::TimedOut(receipt) = error else {
        panic!("expected policy timeout, not a transport timeout: {error:?}")
    };
    assert_eq!(receipt.acknowledged_nodes(), &[ids[0].as_str()]);
    assert_eq!(receipt.required_acknowledgements(), 2);
    assert_eq!(client.get("wait").await.unwrap().unwrap().bytes, vec![1]);
    let deleted = client
        .delete_with_write_concern("wait", WriteConcernOptions::default())
        .await
        .unwrap();
    assert_eq!(deleted.required_acknowledgements(), 1);
    assert!(client.get("wait").await.unwrap().is_none());
    client.close().await;
    server.shutdown().await;
    node.shutdown().await.unwrap();
}
