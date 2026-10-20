#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::Duration;

use actordb::network::TlsNetwork;
use actordb::node::{Node, NodeConfig};
use rcgen::generate_simple_self_signed;
use tokio_rustls::rustls::{
    pki_types::PrivatePkcs8KeyDer, pki_types::ServerName, server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};

fn localhost_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let generated = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = generated.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .unwrap();
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert.clone()], key.clone_key().into())
        .unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![cert], key.into())
        .unwrap();
    (Arc::new(server), Arc::new(client))
}

#[tokio::test]
async fn built_in_tls_network_encrypts_and_authenticates_peer_traffic() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (server, client) = localhost_configs();
        let network = Arc::new(
            TlsNetwork::new(
                server,
                client,
                ServerName::try_from("localhost").unwrap(),
                Duration::from_secs(2),
            )
            .unwrap(),
        );
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
        a.put("encrypted".into(), b"value".to_vec()).await.unwrap();
        loop {
            if b.get_all()
                .await
                .unwrap()
                .get("encrypted")
                .is_some_and(|value| value.value == b"value")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn built_in_tls_network_rejects_an_unbounded_handshake_timeout() {
    let (server, client) = localhost_configs();
    let error = TlsNetwork::new(
        server,
        client,
        ServerName::try_from("localhost").unwrap(),
        Duration::ZERO,
    )
    .unwrap_err();
    assert!(error.to_string().contains("timeout"));
}
