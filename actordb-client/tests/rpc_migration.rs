//! Contract tests shared by the default RPC adapter and vendor implementations.
use actordb::node::{Node, NodeConfig};
use actordb::rpc::NodeRpcService;
use actordb_client::{
    Client, ClientError, ClientOptions, RpcCallOptions, RpcClient, RpcError, RpcFuture, RpcReply,
};
use actordb_protocol::{
    client as wire,
    rpc::{RpcRequest, RpcService},
};
use std::sync::Arc;
use std::time::Duration;

struct DirectRpc(Arc<NodeRpcService>);
impl RpcClient for DirectRpc {
    fn call(
        &self,
        request: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        let request = (*request).clone();
        Box::pin(async move { self.0.call(RpcRequest::new(request)).await })
    }
}
async fn node() -> Arc<Node> {
    Arc::new(
        Node::start(
            "rpc-test".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    )
}
async fn catalog(client: &Client) {
    client.put("same", vec![0, 255, 7]).await.unwrap();
    assert_eq!(
        client.get("same").await.unwrap().unwrap().bytes,
        [0, 255, 7]
    );
    assert!(client
        .put_multi(vec![("dup".into(), vec![1]), ("dup".into(), vec![2])])
        .await
        .unwrap()
        .iter()
        .all(Result::is_ok));
    let reads = client
        .get_multi(vec!["dup".into(), "absent".into(), "dup".into()])
        .await
        .unwrap();
    assert_eq!(reads[0].as_ref().unwrap().as_ref().unwrap().bytes, [2]);
    assert!(reads[1].as_ref().unwrap().is_none());
    assert_eq!(reads[2].as_ref().unwrap().as_ref().unwrap().bytes, [2]);
    let table = client.table("USERS").unwrap();
    table.put("same", vec![3]).await.unwrap();
    assert_eq!(
        client.get("same").await.unwrap().unwrap().bytes,
        [0, 255, 7]
    );
    assert_eq!(table.get("same").await.unwrap().unwrap().bytes, [3]);
    assert_eq!(
        table
            .query("SELECT COUNT(*) FROM USERS")
            .await
            .unwrap()
            .rows[0][0],
        actordb_client::QueryValue::Integer(1)
    );
    client.delete("dup").await.unwrap();
    assert!(client.get("dup").await.unwrap().is_none());
    assert_eq!(client.inspect().await.unwrap().peer_count, 0);
}
#[tokio::test]
async fn custom_rpc_works_without_any_grpc_or_stream_handshake() {
    let node = node().await;
    let service = Arc::new(NodeRpcService::new(node.clone(), Default::default()).unwrap());
    let client = Client::from_rpc(
        Arc::new(DirectRpc(service.clone())),
        ClientOptions::default(),
    )
    .unwrap();
    catalog(&client).await;
    client.close().await;
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    node.put("still-running".into(), vec![1]).await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_reply_retains_service_and_node_drain_ownership_until_dropped() {
    let node = node().await;
    let service = NodeRpcService::new(
        node.clone(),
        actordb::rpc::RpcServiceOptions::builder()
            .max_in_flight_requests(1)
            .build(),
    )
    .unwrap();
    let request = || {
        RpcRequest::new(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::Get(wire::Get {
                key: "missing".into(),
                ..Default::default()
            })),
        })
    };
    let reply = service.call(request()).await.unwrap();
    assert_eq!(node.drain_status().active_requests(), 1);
    let rejected = service.call(request()).await.unwrap();
    assert_eq!(
        rejected.error.as_ref().unwrap().code,
        wire::ErrorCode::Unavailable as i32
    );
    assert!(matches!(
        node.drain_requests(
            actordb::node::DrainOptions::default().with_timeout(Duration::from_millis(20))
        )
        .await,
        Err(actordb::node::DrainError::Timeout { .. })
    ));
    drop(reply);
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    assert_eq!(node.drain_status().active_requests(), 0);
    node.finish_shutdown(Default::default()).await.unwrap();
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn grpc_catalog_matches_the_custom_trait_adapter() {
    let node = node().await;
    let mut server = actordb::grpc::GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    catalog(&client).await;
    client.close().await;
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn unary_grpc_rejects_extra_messages_before_node_admission() {
    use actordb_protocol::grpc::{BoundedCodec, Payload};
    let node = node().await;
    let mut server = actordb::grpc::GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", server.addr()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.unwrap();
    let calls = tokio_stream::iter(["first", "second"].map(|key| {
        Payload::new(Arc::new(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::Put(wire::Put {
                key: key.into(),
                value: vec![1],
                ..Default::default()
            })),
        }))
    }));
    let path =
        tonic::codegen::http::uri::PathAndQuery::from_static(actordb_protocol::RPC_CALL_PATH);
    let response = grpc
        .client_streaming(
            tonic::Request::new(calls),
            path,
            BoundedCodec::<wire::Request, wire::Response>::default(),
        )
        .await;
    assert!(
        response.is_err(),
        "a unary method accepted a streaming body"
    );
    assert_eq!(node.drain_diagnostics().accepted_requests(), 0);
    assert!(node.get("first".into()).await.unwrap().is_none());
    assert!(node.get("second".into()).await.unwrap().is_none());
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

struct GatedRpc {
    entered: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Notify>,
}
impl RpcClient for GatedRpc {
    fn call(
        &self,
        request: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        Box::pin(async move {
            self.entered.wait().await;
            self.release.notified().await;
            let Some(wire::request::Operation::Get(get)) = &request.operation else {
                panic!("read")
            };
            Ok(RpcReply::new(wire::Response {
                reads: vec![wire::ReadResult {
                    key: get.key.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}
#[tokio::test]
async fn calls_execute_concurrently_and_close_cancels_waiters_without_session_state() {
    let entered = Arc::new(tokio::sync::Barrier::new(5));
    let rpc = Arc::new(GatedRpc {
        entered: entered.clone(),
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let client = Client::from_rpc(rpc, ClientOptions::default()).unwrap();
    let mut calls = Vec::new();
    for i in 0..4 {
        let client = client.clone();
        calls.push(tokio::spawn(
            async move { client.get(format!("k{i}")).await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(2), entered.wait())
        .await
        .expect("RPCs must overlap, not serialize on a socket");
    client.close().await;
    for call in calls {
        assert!(matches!(call.await.unwrap(), Err(ClientError::Closed)));
    }
}
