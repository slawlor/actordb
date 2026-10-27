#![cfg(feature = "grpc")]
use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{
    grpc::{GrpcClient, GrpcClientOptions},
    Client, ClientError, ClientOptions,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

async fn node() -> Arc<Node> {
    Arc::new(
        Node::start(
            "grpc-auth".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    )
}
#[tokio::test]
async fn grpc_metadata_acl_is_enforced_before_node_admission() {
    let node = node().await;
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        GrpcServerOptions {
            authorize: Some(Arc::new(|metadata, _, request| {
                if metadata.get("authorization").and_then(|v| v.to_str().ok())
                    != Some("Bearer test-token")
                {
                    return Err(tonic::Status::unauthenticated("missing RPC credential"));
                }
                if request.table != "KV" {
                    return Err(tonic::Status::permission_denied("table ACL"));
                }
                Ok(())
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let before = node.drain_diagnostics().accepted_requests();
    let anonymous = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        anonymous.put("denied", vec![1]).await,
        Err(ClientError::Transport(_))
    ));
    assert_eq!(node.drain_diagnostics().accepted_requests(), before);
    anonymous.close().await;
    let mut options = GrpcClientOptions::default();
    options
        .metadata
        .insert("authorization", "Bearer test-token".parse().unwrap());
    assert!(!format!("{options:?}").contains("test-token"));
    let rpc = GrpcClient::connect(
        format!("http://{}", server.addr()),
        options,
        std::time::Duration::from_secs(2),
    )
    .await
    .unwrap();
    let client = Client::from_rpc(Arc::new(rpc), ClientOptions::default()).unwrap();
    client.put("allowed", vec![7]).await.unwrap();
    let before = node.drain_diagnostics().accepted_requests();
    assert!(client
        .table("USERS")
        .unwrap()
        .put("denied", vec![2])
        .await
        .is_err());
    assert_eq!(node.drain_diagnostics().accepted_requests(), before);
    assert!(node
        .table("USERS")
        .unwrap()
        .get("denied")
        .await
        .unwrap()
        .is_none());
    assert_eq!(node.peer_count().await, 0);
    client.close().await;
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn caller_provisioned_mtls_authenticates_in_grpc_and_idle_tls_does_not_block() {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = generated.cert.pem();
    let key = generated.signing_key.serialize_pem();
    let identity = Identity::from_pem(pem.clone(), key);
    let root = Certificate::from_pem(pem.clone());
    let node = node().await;
    let saw_certificate = Arc::new(AtomicBool::new(false));
    let seen = saw_certificate.clone();
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        GrpcServerOptions {
            tls: Some(
                ServerTlsConfig::new()
                    .identity(identity.clone())
                    .client_ca_root(root.clone()),
            ),
            authorize: Some(Arc::new(move |_, extensions, _| {
                let authenticated = extensions
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .and_then(|info| info.peer_certs())
                    .is_some_and(|certs| !certs.is_empty());
                seen.store(authenticated, Ordering::SeqCst);
                if authenticated {
                    Ok(())
                } else {
                    Err(tonic::Status::unauthenticated("client certificate"))
                }
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stalled = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
    let channel = Endpoint::from_shared(format!("https://{}", server.addr()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(root.clone())
                .identity(identity.clone())
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = Client::from_rpc(
        Arc::new(GrpcClient::from_channel(
            channel,
            GrpcClientOptions::default(),
        )),
        ClientOptions::default(),
    )
    .unwrap();
    client.put("encrypted", vec![9; 32768]).await.unwrap();
    assert_eq!(
        client.get("encrypted").await.unwrap().unwrap().bytes,
        vec![9; 32768]
    );
    assert!(saw_certificate.load(Ordering::SeqCst));
    assert_eq!(node.peer_count().await, 0);
    for (name, ca) in [
        ("wrong-host", root.clone()),
        (
            "localhost",
            Certificate::from_pem(
                rcgen::generate_simple_self_signed(vec!["localhost".into()])
                    .unwrap()
                    .cert
                    .pem(),
            ),
        ),
    ] {
        assert!(Endpoint::from_shared(format!("https://{}", server.addr()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(ca)
                    .identity(identity.clone())
                    .domain_name(name)
            )
            .unwrap()
            .connect()
            .await
            .is_err());
    }
    // A valid server trust root alone is not a client identity.
    let without_cert = Endpoint::from_shared(format!("https://{}", server.addr()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(root)
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await;
    if let Ok(channel) = without_cert {
        let unauthenticated = Client::from_rpc(
            Arc::new(GrpcClient::from_channel(
                channel,
                GrpcClientOptions::default(),
            )),
            ClientOptions::default(),
        )
        .unwrap();
        assert!(unauthenticated
            .put("not-authenticated", vec![1])
            .await
            .is_err());
        unauthenticated.close().await;
    }
    assert!(node
        .get("not-authenticated".into())
        .await
        .unwrap()
        .is_none());
    drop(stalled);
    client.close().await;
    server.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
