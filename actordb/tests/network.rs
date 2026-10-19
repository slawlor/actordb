mod common;
#[path = "common/transport.rs"]
mod transport;

use std::sync::Arc;
use std::time::Duration;

use actordb::network::Network;
use actordb::node::{Node, NodeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use transport::MemoryNetwork;

const TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn buffered_transport_preserves_bytes_eof_and_listener_ownership() {
    tokio::time::timeout(TIMEOUT, async {
        let network = MemoryNetwork::default();
        let (addr, mut acceptor) = network.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(network.bind(addr).await.is_err());
        // Cancelling a pending accept leaves the listener usable.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), acceptor.accept())
                .await
                .is_err()
        );
        let mut dialed = network.connect(addr).await.unwrap();
        let mut accepted = acceptor.accept().await.unwrap();
        let payload: Vec<u8> = (0..65536).map(|i| i as u8).collect();
        let ((), received) = tokio::join!(
            async {
                dialed.write_all(&payload).await.unwrap();
                dialed.flush().await.unwrap();
                dialed.shutdown().await.unwrap();
            },
            async {
                let mut received = Vec::new();
                accepted.read_to_end(&mut received).await.unwrap();
                received
            }
        );
        assert_eq!(received, payload);
        drop(acceptor);
        assert!(network.connect(addr).await.is_err());
        let (_, replacement) = network.bind(addr).await.unwrap();
        drop(replacement);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn mesh_replicates_and_reconnects_over_buffered_non_tcp_streams() {
    tokio::time::timeout(TIMEOUT, async {
        let network = Arc::new(MemoryNetwork::default());
        let config = NodeConfig::builder().network(network.clone()).build();
        let a = Node::start(
            "buffered-a".into(),
            "127.0.0.1:0".parse().unwrap(),
            config.clone(),
        )
        .await
        .unwrap();
        let b = Node::start(
            "buffered-b".into(),
            "127.0.0.1:0".parse().unwrap(),
            config.clone(),
        )
        .await
        .unwrap();
        let addr = b.addr();
        a.connect(addr).await.unwrap();
        common::wait_for(
            || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
            TIMEOUT,
        )
        .await;
        a.put("from-a".into(), vec![7; 32768]).await.unwrap();
        b.put("from-b".into(), vec![8; 32768]).await.unwrap();
        common::wait_for(
            || async {
                b.get_all()
                    .await
                    .unwrap()
                    .get("from-a")
                    .is_some_and(|v| v.value == vec![7; 32768])
                    && a.get_all()
                        .await
                        .unwrap()
                        .get("from-b")
                        .is_some_and(|v| v.value == vec![8; 32768])
            },
            TIMEOUT,
        )
        .await;
        b.shutdown().await.unwrap();
        common::wait_for(|| async { a.peer_count().await == 0 }, TIMEOUT).await;
        a.put("offline".into(), b"catch up".to_vec()).await.unwrap();
        let b = Node::start("buffered-b".into(), addr, config)
            .await
            .unwrap();
        common::wait_for(
            || async {
                b.get_all()
                    .await
                    .unwrap()
                    .get("offline")
                    .is_some_and(|v| v.value == b"catch up")
            },
            TIMEOUT,
        )
        .await;
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        // Node shutdown drops its acceptor, permitting another bind.
        let (_, replacement) = network.bind(addr).await.unwrap();
        drop(replacement);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn node_shutdown_cancels_idle_custom_streams_and_pending_accepts() {
    tokio::time::timeout(TIMEOUT, async {
        let network = Arc::new(MemoryNetwork::default());
        let node = Node::start(
            "idle".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().network(network.clone()).build(),
        )
        .await
        .unwrap();
        let mut idle = network.connect(node.addr()).await.unwrap();
        // The peer Hello proves this connection was accepted; never answer it.
        let length = idle.read_u32().await.unwrap();
        let mut body = vec![0; length as usize];
        idle.read_exact(&mut body).await.unwrap();
        node.shutdown().await.unwrap();
        assert_eq!(idle.read(&mut [0]).await.unwrap(), 0);
        let (_, replacement) = network.bind(node.addr()).await.unwrap();
        drop(replacement);
    })
    .await
    .unwrap();
}
