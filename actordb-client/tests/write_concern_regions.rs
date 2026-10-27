#![cfg(feature = "grpc")]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb_client::{Client, ClientOptions, WriteConcernClientError, WriteConcernOptions};
use actordb_client::{RpcCallOptions, RpcClient, RpcError, RpcFuture, RpcReply};
use actordb_protocol::client as wire;
use actordb_protocol::write_concern::{WriteConcernOutcome, WriteConcernReceipt};

async fn node(id: &str, regions: HashMap<String, String>) -> Arc<Node> {
    Arc::new(
        Node::start_with_options(
            id.into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .fragment_map(HashMap::from([(
                    FragmentId(0),
                    vec!["sdk-region-a".into(), "sdk-region-b".into()],
                )]))
                .replication(SenderConfig::builder().batch_size(1).build())
                .build(),
            NodeRuntimeOptions::default().with_regions(regions),
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn sdk_routed_put_and_delete_preserve_region_receipts() {
    let regions = HashMap::from([
        ("sdk-region-a".into(), "west".into()),
        ("sdk-region-b".into(), "east".into()),
        ("sdk-region-origin".into(), "east".into()),
    ]);
    let a = node("sdk-region-a", regions.clone()).await;
    let b = node("sdk-region-b", regions.clone()).await;
    let origin = node("sdk-region-origin", regions).await;
    b.connect(a.addr()).await.unwrap();
    origin.connect(a.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while a.peer_count().await != 2
            || b.peer_count().await != 1
            || origin.peer_count().await != 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut server = GrpcServer::bind(
        origin.clone(),
        "127.0.0.1:0".parse().unwrap(),
        GrpcServerOptions::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    let options = WriteConcernOptions::default().with_require_other_region(true);
    let receipt = client
        .put_with_write_concern("sdk-region-key", vec![1], options.clone())
        .await
        .unwrap();
    assert_eq!(receipt.receiving_node(), "sdk-region-a");
    assert_eq!(receipt.receiving_region(), Some("west"));
    assert_eq!(
        receipt.outside_region_acknowledged_nodes(),
        &["sdk-region-b"]
    );
    assert_eq!(
        b.get("sdk-region-key".into()).await.unwrap().unwrap().value,
        vec![1]
    );
    let receipt = client
        .delete_with_write_concern("sdk-region-key", options)
        .await
        .unwrap();
    assert!(receipt.other_region_satisfied());
    assert!(b.get("sdk-region-key".into()).await.unwrap().is_none());
    client.close().await;
    server.shutdown().await.unwrap();
    origin.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

struct BadRegion(bool);
impl RpcClient for BadRegion {
    fn call(
        &self,
        _: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        Box::pin(async move {
            let mut receipt =
                WriteConcernReceipt::new("region-mock".into(), 2, 1, 1, vec!["region-mock".into()])
                    .unwrap();
            if self.0 {
                receipt = receipt
                    .with_region_progress("west".into(), Vec::new())
                    .unwrap();
            }
            Ok(RpcReply::new(wire::Response {
                mutations: vec![wire::MutationResult {
                    key: "key".into(),
                    error: None,
                    write_concern: Some(WriteConcernOutcome::Satisfied(receipt).encode()),
                }],
                ..Default::default()
            }))
        })
    }
}
#[tokio::test]
async fn sdk_rejects_success_without_requested_region_progress() {
    for malformed in [false, true] {
        let client =
            Client::from_rpc(Arc::new(BadRegion(malformed)), ClientOptions::default()).unwrap();
        assert!(matches!(
            client
                .put_with_write_concern(
                    "key",
                    vec![1],
                    WriteConcernOptions::default().with_require_other_region(true)
                )
                .await,
            Err(WriteConcernClientError::UnknownOutcome(_))
        ));
        client.close().await;
    }
}
