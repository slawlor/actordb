#[path = "../../actordb/tests/common/evidence_store.rs"]
#[allow(dead_code)]
mod evidence_store;
use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use actordb_client::{
    Client, ClientOptions, PutOptions, WriteConcernClientError, WriteConcernOptions,
    WriteDurability,
};
use evidence_store::EvidenceStore;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn sdk_forwarded_put_delete_and_local_fence_failure_preserve_certified_evidence() {
    let store = EvidenceStore::new(true);
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(
            FragmentId(0),
            vec!["receiver".into(), "offline-b".into(), "offline-c".into()],
        )]))
        .flush_batch_size(10_000)
        .flush_interval(Duration::from_secs(3600))
        .snapshot_interval(Duration::from_secs(3600))
        .build();
    let mut receiving_config = config.clone();
    receiving_config.store = Some(store.clone());
    let receiver = Node::start(
        "receiver".into(),
        "127.0.0.1:0".parse().unwrap(),
        receiving_config,
    )
    .await
    .unwrap();
    let gateway = Arc::new(
        Node::start("gateway".into(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap(),
    );
    gateway.connect(receiver.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while gateway.peer_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut server = ClientServer::bind(
        gateway.clone(),
        "127.0.0.1:0".parse().unwrap(),
        ClientServerOptions::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap()
        .table("orders")
        .unwrap();
    let stable = WriteConcernOptions::builder()
        .minimum_durability(WriteDurability::StableCommitted)
        .require_evidence(true)
        .build();
    let receipt = client
        .put_with_write_concern_options(
            "key",
            vec![7],
            PutOptions::builder().ttl(Duration::from_secs(10)).build(),
            stable.clone(),
        )
        .await
        .unwrap();
    assert_eq!(receipt.receiving_node(), "receiver");
    let evidence = receipt.evidence().unwrap();
    assert_eq!(
        evidence.durability_at("receiver"),
        Some(WriteDurability::StableCommitted)
    );
    assert_eq!(evidence.operation().originating_node(), "gateway");
    assert!(evidence.operation().expires_at_ms().is_some());
    evidence
        .operation()
        .validate_application_write("ORDERS", "key", &[7], false)
        .unwrap();
    let deleted = client
        .delete_with_write_concern("key", stable.clone())
        .await
        .unwrap();
    assert!(deleted.evidence().unwrap().operation().is_tombstone());
    assert!(client.get("key").await.unwrap().is_none());

    store.fail_write.store(true, Ordering::SeqCst);
    let Err(WriteConcernClientError::DurabilityFailed { receipt, .. }) = client
        .put_with_write_concern("failed-fence", vec![9], stable)
        .await
    else {
        panic!("SDK must preserve known applied fence failure")
    };
    assert_eq!(
        receipt.evidence().unwrap().durability_at("receiver"),
        Some(WriteDurability::MemoryApplied)
    );
    assert_eq!(
        client.get("failed-fence").await.unwrap().unwrap().bytes,
        vec![9]
    );
    store.fail_write.store(false, Ordering::SeqCst);
    client.close().await;
    server.shutdown().await;
    gateway.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
