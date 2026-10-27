#![cfg(feature = "grpc")]

use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb::rpc::RpcServiceOptions;
use actordb_client::{Client, ClientOptions};
use actordb_protocol::{client as wire, MAX_RPC_MESSAGE_BYTES, RPC_CALL_PATH};
use bytes::Bytes;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::http;

fn options(timeout: Duration) -> GrpcServerOptions {
    GrpcServerOptions {
        service: RpcServiceOptions::builder().timeout(timeout).build(),
        max_decoded_request_bytes: MAX_RPC_MESSAGE_BYTES,
        receive_memory_bytes: 4 * MAX_RPC_MESSAGE_BYTES,
        ..Default::default()
    }
}

fn framed(request: wire::Request) -> Bytes {
    let encoded = request.encode_to_vec();
    let mut frame = Vec::with_capacity(encoded.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    frame.extend(encoded);
    frame.into()
}

fn request(addr: std::net::SocketAddr) -> http::Request<()> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://{addr}{RPC_CALL_PATH}"))
        .header("content-type", "application/grpc")
        .body(())
        .unwrap()
}

#[tokio::test]
async fn unfinished_bodies_release_all_receive_slots_without_client_deadlines() {
    let node = Arc::new(
        Node::start(
            "receive-slots".into(),
            "127.0.0.1:0".parse().unwrap(),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        options(Duration::from_millis(500)),
    )
    .await
    .unwrap();
    let tcp = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    let (mut connection, driver) = h2::client::handshake(tcp).await.unwrap();
    let driver = tokio::spawn(async move {
        let _ = driver.await;
    });
    let before = node.drain_diagnostics().accepted_requests();
    let mut blocked = Vec::new();
    for _ in 0..4 {
        blocked.push(
            connection
                .send_request(request(server.addr()), false)
                .unwrap(),
        );
    }
    let (excess, mut body) = connection
        .send_request(request(server.addr()), false)
        .unwrap();
    body.send_data(
        framed(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::Get(wire::Get {
                key: "excess".into(),
                ..Default::default()
            })),
        }),
        true,
    )
    .unwrap();
    let rejected = tokio::time::timeout(Duration::from_secs(2), excess)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rejected.headers().get("grpc-status").unwrap(), "8");
    for (response, _body) in blocked {
        let response = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "4");
    }
    assert_eq!(node.drain_diagnostics().accepted_requests(), before);
    // A timed-out body retires only that stream, not the shared connection.
    let (ready, mut body) = connection
        .send_request(request(server.addr()), false)
        .unwrap();
    body.send_data(
        framed(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::Get(wire::Get {
                key: "same-connection".into(),
                ..Default::default()
            })),
        }),
        true,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut response = ready.await.unwrap().into_body();
        while let Some(data) = response.data().await {
            let data = data.unwrap();
            response
                .flow_control()
                .release_capacity(data.len())
                .unwrap();
        }
        assert_eq!(
            response
                .trailers()
                .await
                .unwrap()
                .unwrap()
                .get("grpc-status")
                .unwrap(),
            "0"
        );
    })
    .await
    .unwrap();
    let client = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    assert!(client.get("released").await.unwrap().is_none());
    client.close().await;
    drop(connection);
    driver.abort();
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn decoded_message_without_end_stream_cannot_extend_receive_deadline_or_apply() {
    let node = Arc::new(
        Node::start(
            "receive-trailers".into(),
            "127.0.0.1:0".parse().unwrap(),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        options(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let tcp = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    let (mut connection, driver) = h2::client::handshake(tcp).await.unwrap();
    let driver = tokio::spawn(async move {
        let _ = driver.await;
    });
    let mut headers = request(server.addr());
    headers
        .headers_mut()
        .insert("grpc-timeout", "100S".parse().unwrap());
    let (response, mut body) = connection.send_request(headers, false).unwrap();
    body.send_data(
        framed(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::Put(wire::Put {
                key: "not-admitted".into(),
                value: vec![1],
                ..Default::default()
            })),
        }),
        false,
    )
    .unwrap();
    let before = node.drain_diagnostics().accepted_requests();
    let response = tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.headers().get("grpc-status").unwrap(), "4");
    assert_eq!(node.drain_diagnostics().accepted_requests(), before);
    assert!(node.get("not-admitted".into()).await.unwrap().is_none());
    drop(connection);
    driver.abort();
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_body_does_not_deadline_an_accepted_slow_mutation() {
    use actordb::actor::VersionedValue;
    use actordb::store::{KvStore, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity};
    struct SlowStore;
    impl KvStore for SlowStore {
        fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            Ok(StoreBinding::unmanaged())
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(vec![])
        }
        fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(vec![])
        }
        fn commit_durability(&self) -> StoreCommitDurability {
            StoreCommitDurability::Buffered
        }
        fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
            Ok(StoreCommitDurability::Buffered)
        }
        fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            std::thread::sleep(Duration::from_millis(600));
            Ok(())
        }
    }
    let node = Arc::new(
        Node::start(
            "receive-owned-mutation".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .store(Arc::new(SlowStore))
                .flush_batch_size(1)
                .build(),
        )
        .await
        .unwrap(),
    );
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        options(Duration::from_millis(200)),
    )
    .await
    .unwrap();
    let client = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    client.put("owned", vec![9]).await.unwrap();
    assert_eq!(client.get("owned").await.unwrap().unwrap().bytes, vec![9]);
    client.close().await;
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn custom_service_binding_validates_receive_timeout_before_listening() {
    use actordb_protocol::rpc::{RpcError, RpcFuture, RpcReply, RpcRequest, RpcService};
    struct Never;
    impl RpcService for Never {
        fn call(&self, _: RpcRequest) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
            Box::pin(std::future::pending())
        }
    }
    for timeout in [Duration::ZERO, Duration::MAX] {
        assert!(GrpcServer::bind_service(
            Arc::new(Never),
            "127.0.0.1:0".parse().unwrap(),
            options(timeout)
        )
        .await
        .is_err());
    }
}
