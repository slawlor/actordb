//! Caller-configured mTLS for the peer mesh and the separate gRPC application adapter.
//! Run: cargo run -p actordb-client --example tls_demo

#[path = "common/tls.rs"]
mod tls;

use std::sync::Arc;
use std::time::Duration;

use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{
    grpc::{GrpcClient, GrpcClientOptions},
    Client, ClientOptions,
};
use tls::TlsNetwork;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

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
    // Demo-only credentials; real hosts provision distinct certificates and CAs.
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let root = Certificate::from_pem(generated.cert.pem());
    let identity = Identity::from_pem(generated.cert.pem(), generated.signing_key.serialize_pem());
    let mut listener = GrpcServer::bind(
        a.clone(),
        "127.0.0.1:0".parse()?,
        GrpcServerOptions {
            tls: Some(
                ServerTlsConfig::new()
                    .identity(identity.clone())
                    .client_ca_root(root.clone()),
            ),
            ..Default::default()
        },
    )
    .await?;
    let channel = Endpoint::from_shared(format!("https://{}", listener.addr()))?
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(root)
                .identity(identity)
                .domain_name("localhost"),
        )?
        .connect()
        .await?;
    let client = Client::from_rpc(
        Arc::new(GrpcClient::from_channel(
            channel,
            GrpcClientOptions::default(),
        )),
        ClientOptions::default(),
    )?;
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
    listener.shutdown().await?;
    a.shutdown().await?;
    b.shutdown().await?;
    Ok(())
}
