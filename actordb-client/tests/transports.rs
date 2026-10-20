mod common;
#[path = "../examples/common/tls.rs"]
mod tls;
#[path = "common/transport.rs"]
mod transport;

use std::sync::Arc;
use std::time::Duration;

use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::network::{AcceptFuture, Acceptor, Network, NetworkError};
use actordb::node::{Node, NodeConfig, NodeError};
use actordb_client::{Client, ClientError, ClientOptions, QueryValue};
use tls::TlsNetwork;
use tokio::io::{AsyncReadExt, BufStream};
use transport::MemoryNetwork;

const TIMEOUT: Duration = Duration::from_secs(10);

async fn node() -> Arc<Node> {
    Arc::new(
        Node::start(
            "custom-sdk".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    )
}

struct FailOnceAcceptor {
    inner: Box<dyn Acceptor>,
    failed: bool,
}

impl Acceptor for FailOnceAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        if !self.failed {
            self.failed = true;
            Box::pin(async { Err(NetworkError("transient accept failure".into())) })
        } else {
            self.inner.accept()
        }
    }
}

#[tokio::test]
async fn custom_acceptor_and_buffered_client_preserve_authentication_and_catalog() {
    tokio::time::timeout(TIMEOUT, async {
        let node = node().await;
        let network = MemoryNetwork::default();
        let (addr, acceptor) = network.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let credential = vec![42; 32];
        let mut server = ClientServer::from_acceptor(
            node.clone(),
            addr,
            Box::new(FailOnceAcceptor {
                inner: acceptor,
                failed: false,
            }),
            ClientServerOptions {
                credential: Some(credential.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(server.addr(), addr);
        assert!(matches!(
            Client::from_stream(
                network.connect(addr).await.unwrap(),
                ClientOptions::default()
            )
            .await,
            Err(ClientError::Authentication) | Err(ClientError::Transport(_))
        ));
        let client = Client::from_stream(
            network.connect(addr).await.unwrap(),
            ClientOptions::builder()
                .maybe_credential(Some(credential))
                .build(),
        )
        .await
        .unwrap();
        // Small frames require a flush; large frames exercise partial I/O.
        client.put("small", b"hello".to_vec()).await.unwrap();
        client.put("large", vec![3; 32768]).await.unwrap();
        assert_eq!(client.get("small").await.unwrap().unwrap().bytes, b"hello");
        assert_eq!(
            client.get("large").await.unwrap().unwrap().bytes,
            vec![3; 32768]
        );
        assert_eq!(
            client.query("SELECT COUNT(*) FROM kv").await.unwrap().rows,
            vec![vec![QueryValue::Integer(2)]]
        );
        assert_eq!(client.inspect().await.unwrap().peer_count, 0);
        client.delete("small").await.unwrap();
        assert!(client.get("small").await.unwrap().is_none());
        client.close().await;
        assert!(matches!(
            client.get("large").await,
            Err(ClientError::Closed)
        ));
        server.shutdown().await;
        assert!(network.connect(addr).await.is_err());
        node.put("still-running".into(), vec![1]).await.unwrap();
        node.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn custom_listener_shutdown_releases_authenticated_and_idle_streams() {
    tokio::time::timeout(TIMEOUT, async {
        let node = node().await;
        let network = Arc::new(MemoryNetwork::default());
        let mut server = ClientServer::bind_with_network(
            node.clone(),
            "127.0.0.1:0".parse().unwrap(),
            network.clone(),
            ClientServerOptions::default(),
        )
        .await
        .unwrap();
        let client = Client::from_stream(
            network.connect(server.addr()).await.unwrap(),
            ClientOptions::default(),
        )
        .await
        .unwrap();
        let mut idle = network.connect(server.addr()).await.unwrap();
        server.shutdown().await;
        assert_eq!(idle.read(&mut [0]).await.unwrap(), 0);
        assert!(client.get("missing").await.is_err());
        let (_, replacement) = network.bind(server.addr()).await.unwrap();
        drop(replacement);
        node.put("alive".into(), vec![1]).await.unwrap();
        node.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_options_release_injected_resources_before_startup() {
    tokio::time::timeout(TIMEOUT, async {
        let (stream, mut remote) = tokio::io::duplex(1024);
        assert!(matches!(
            Client::from_stream(
                stream,
                ClientOptions::builder().timeout(Duration::ZERO).build()
            )
            .await,
            Err(ClientError::Invalid(_))
        ));
        assert_eq!(remote.read(&mut [0]).await.unwrap(), 0);
        let node = node().await;
        let network = Arc::new(MemoryNetwork::default());
        let (addr, acceptor) = network.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(ClientServer::from_acceptor(
            node.clone(),
            addr,
            acceptor,
            ClientServerOptions {
                max_connections: 0,
                ..Default::default()
            }
        )
        .is_err());
        let (_, replacement) = network.bind(addr).await.unwrap();
        drop(replacement);
        assert!(ClientServer::bind_with_network(
            node.clone(),
            addr,
            network.clone(),
            ClientServerOptions {
                timeout: Duration::ZERO,
                ..Default::default()
            }
        )
        .await
        .is_err());
        let (_, replacement) = network.bind(addr).await.unwrap();
        drop(replacement);
        // The existing plain TCP API preserves concrete I/O startup errors.
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(matches!(
            ClientServer::bind(node.clone(), occupied.local_addr().unwrap(), ClientServerOptions::default()).await,
            Err(NodeError::Io(error)) if error.kind() == std::io::ErrorKind::AddrInUse
        ));
        node.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn injected_client_handshake_timeout_and_cancellation_close_streams() {
    tokio::time::timeout(TIMEOUT, async {
        let (stream, mut remote) = tokio::io::duplex(1024);
        assert!(matches!(
            Client::from_stream(
                BufStream::new(stream),
                ClientOptions::builder()
                    .timeout(Duration::from_millis(20))
                    .build()
            )
            .await,
            Err(ClientError::Timeout)
        ));
        let mut sent = Vec::new();
        remote.read_to_end(&mut sent).await.unwrap();
        assert!(
            !sent.is_empty(),
            "the buffered Hello must have been flushed"
        );
        let (stream, mut remote) = tokio::io::duplex(1024);
        let pending = tokio::spawn(Client::from_stream(stream, ClientOptions::default()));
        let length = remote.read_u32().await.unwrap();
        let mut body = vec![0; length as usize];
        remote.read_exact(&mut body).await.unwrap();
        pending.abort();
        let Err(error) = pending.await else {
            panic!("handshake completed before cancellation");
        };
        assert!(error.is_cancelled());
        assert_eq!(remote.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn caller_configured_mtls_supports_application_traffic_and_peer_replication() {
    tokio::time::timeout(TIMEOUT, async {
        let (server_tls, client_tls) = tls::localhost_configs().unwrap();
        let network = Arc::new(TlsNetwork::new(
            server_tls,
            client_tls,
            "localhost".try_into().unwrap(),
            TIMEOUT,
        ));
        let config = NodeConfig::builder().network(network.clone()).build();
        let a = Arc::new(
            Node::start(
                "tls-a".into(),
                "127.0.0.1:0".parse().unwrap(),
                config.clone(),
            )
            .await
            .unwrap(),
        );
        let b = Node::start("tls-b".into(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap();
        b.connect(a.addr()).await.unwrap();
        common::wait_for(
            || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
            TIMEOUT,
        )
        .await;
        let credential = vec![73; 32];
        let mut server = ClientServer::bind_with_network(
            a.clone(),
            "127.0.0.1:0".parse().unwrap(),
            network.clone(),
            ClientServerOptions {
                credential: Some(credential.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // An unfinished inbound TLS handshake must not block a healthy client.
        let mut stalled = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
        assert!(Client::from_stream(
            network.connect(server.addr()).await.unwrap(),
            ClientOptions::default()
        )
        .await
        .is_err());
        let client = Client::from_stream(
            network.connect(server.addr()).await.unwrap(),
            ClientOptions::builder()
                .maybe_credential(Some(credential))
                .build(),
        )
        .await
        .unwrap();
        client.put("encrypted", vec![9; 32768]).await.unwrap();
        assert_eq!(
            client.get("encrypted").await.unwrap().unwrap().bytes,
            vec![9; 32768]
        );
        common::wait_for(
            || async {
                b.get_all()
                    .await
                    .unwrap()
                    .get("encrypted")
                    .is_some_and(|v| v.value == vec![9; 32768])
            },
            TIMEOUT,
        )
        .await;
        assert_eq!(
            a.peer_count().await,
            1,
            "application clients never become peers"
        );
        client.close().await;
        server.shutdown().await;
        // Shutdown cancels the partial TLS handshake and frees the listener.
        assert_eq!(stalled.read(&mut [0]).await.unwrap(), 0);
        let replacement = tokio::net::TcpListener::bind(server.addr()).await.unwrap();
        drop(replacement);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tls_validates_server_names_and_trust_without_stopping_the_listener() {
    tokio::time::timeout(TIMEOUT, async {
        let node = node().await;
        let (server_tls, client_tls) = tls::localhost_configs().unwrap();
        let network = Arc::new(TlsNetwork::new(
            server_tls.clone(),
            client_tls.clone(),
            "localhost".try_into().unwrap(),
            TIMEOUT,
        ));
        let mut server = ClientServer::bind_with_network(
            node.clone(),
            "127.0.0.1:0".parse().unwrap(),
            network.clone(),
            ClientServerOptions::default(),
        )
        .await
        .unwrap();
        let wrong_name = TlsNetwork::new(
            server_tls.clone(),
            client_tls,
            "wrong.example".try_into().unwrap(),
            TIMEOUT,
        );
        assert!(wrong_name.connect(server.addr()).await.is_err());
        let (_, unrelated_roots) = tls::localhost_configs().unwrap();
        let untrusted = TlsNetwork::new(
            server_tls,
            unrelated_roots,
            "localhost".try_into().unwrap(),
            TIMEOUT,
        );
        assert!(untrusted.connect(server.addr()).await.is_err());
        let client = Client::from_stream(
            network.connect(server.addr()).await.unwrap(),
            ClientOptions::default(),
        )
        .await
        .unwrap();
        client.put("healthy", vec![1]).await.unwrap();
        client.close().await;
        server.shutdown().await;
        node.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
