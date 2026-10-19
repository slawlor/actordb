//! Caller-configured mTLS for both the peer mesh and application listener.
//! Run: cargo run -p actordb-client --example tls_demo

#[path = "common/tls.rs"]
mod tls;

use std::sync::Arc;
use std::time::Duration;

use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::network::Network;
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientOptions};
use tls::TlsNetwork;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (server_tls, client_tls) = tls::localhost_configs()?;
    let network = Arc::new(TlsNetwork::new(
        server_tls,
        client_tls,
        "localhost".try_into()?,
        Duration::from_secs(5),
    ));
    let config = NodeConfig::builder().network(network.clone()).build();
    let a = Arc::new(Node::start("tls-a".into(), "127.0.0.1:0".parse()?, config.clone()).await?);
    let b = Node::start("tls-b".into(), "127.0.0.1:0".parse()?, config).await?;
    b.connect(a.addr()).await?;
    let credential = {
        let mut credential = vec![0; 32];
        getrandom::fill(&mut credential)?;
        credential
    };
    let mut listener = ClientServer::bind_with_network(
        a.clone(),
        "127.0.0.1:0".parse()?,
        network.clone(),
        ClientServerOptions {
            credential: Some(credential.clone()),
            ..Default::default()
        },
    )
    .await?;
    let stream = network.connect(listener.addr()).await?;
    let client = Client::from_stream(
        stream,
        ClientOptions {
            credential: Some(credential),
            ..Default::default()
        },
    )
    .await?;
    client.put("greeting", b"hello over mTLS".to_vec()).await?;
    // Check b's local copy, so a forwarded get cannot mask failed replication.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if b.get_all()
                .await?
                .get("greeting")
                .is_some_and(|v| v.value == b"hello over mTLS")
            {
                return Ok::<_, actordb::node::NodeError>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    println!("Application write and peer replication completed over caller-configured mTLS.");
    client.close().await;
    listener.shutdown().await;
    a.shutdown().await?;
    b.shutdown().await?;
    Ok(())
}
