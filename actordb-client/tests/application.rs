#![cfg(feature = "grpc")]
use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientError, ClientOptions, Priority, PutOptions, QueryValue};
use actordb_protocol::client as wire;
use std::sync::Arc;
use std::time::Duration;

async fn server(options: GrpcServerOptions) -> (Arc<Node>, GrpcServer) {
    let node = Arc::new(
        Node::start(
            "sdk-node".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    );
    let server = GrpcServer::bind(node.clone(), "127.0.0.1:0".parse().unwrap(), options)
        .await
        .unwrap();
    (node, server)
}

#[tokio::test]
async fn full_application_catalog_preserves_peer_membership_and_node_lifecycle() {
    let (node, mut server) = server(GrpcServerOptions::default()).await;
    let client = Client::connect(
        format!("http://{}", server.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(node.peer_count().await, 0);
    client
        .put_with_options(
            "item:1",
            b"123".to_vec(),
            PutOptions::builder()
                .maybe_ttl(Some(Duration::from_secs(60)))
                .priority(Priority::High)
                .build(),
        )
        .await
        .unwrap();
    let value = client.get("item:1").await.unwrap().unwrap();
    assert_eq!(value.bytes, b"123");
    assert_eq!(value.node_id, "sdk-node");
    assert!(value.timestamp_ms > 0);
    assert!(value.expires_at_ms.is_some());
    let writes = client
        .put_multi(vec![
            ("dup".into(), b"old".to_vec()),
            ("dup".into(), b"new".to_vec()),
            ("item:2".into(), b"456".to_vec()),
        ])
        .await
        .unwrap();
    assert_eq!(writes.len(), 3);
    assert!(writes.into_iter().all(|result| result.is_ok()));
    let reads = client
        .get_multi(vec!["dup".into(), "missing".into(), "dup".into()])
        .await
        .unwrap();
    assert_eq!(reads.len(), 3);
    assert_eq!(reads[0].as_ref().unwrap().as_ref().unwrap().bytes, b"new");
    assert!(reads[1].as_ref().unwrap().is_none());
    assert_eq!(reads[2].as_ref().unwrap().as_ref().unwrap().bytes, b"new");
    assert!(client.get_multi(Vec::new()).await.unwrap().is_empty());
    assert!(client.put_multi(Vec::new()).await.unwrap().is_empty());
    let query = client
        .query_with_params(
            "SELECT key, value FROM kv WHERE key = ?",
            &[QueryValue::Text("dup".into())],
        )
        .await
        .unwrap();
    assert_eq!(
        query.rows,
        vec![vec![
            QueryValue::Text("dup".into()),
            QueryValue::Bytes(b"new".to_vec())
        ]]
    );
    assert_eq!(
        client.query("SELECT COUNT(*) FROM kv").await.unwrap().rows,
        vec![vec![QueryValue::Integer(3)]]
    );
    let scan = client
        .scan(wire::Scan {
            matcher_kind: 1,
            prefix: b"item:".to_vec(),
            transform_kind: 2,
            prefix_bytes: 1,
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(scan.len(), 2);
    assert_eq!(scan[0].value, b"1");
    client
        .delete_with_priority("dup", Priority::High)
        .await
        .unwrap();
    assert!(client.get("dup").await.unwrap().is_none());
    assert!(
        matches!(client.put_with_ttl("expired", b"x".to_vec(), Some(Duration::ZERO)).await, Err(ClientError::Server(error)) if error.code == wire::ErrorCode::Expired as i32)
    );
    assert!(client.get("expired").await.unwrap().is_none());
    let inspection = client.inspect().await.unwrap();
    assert_eq!(inspection.node_id, "sdk-node");
    assert_eq!(inspection.peer_count, 0);
    assert_eq!(inspection.health.len(), 1);

    // Table views share the RPC adapter; no second socket is
    // opened for the users table.
    let users = client.table("users").unwrap();
    users.put("item:1", b"table-value".to_vec()).await.unwrap();
    assert_eq!(
        users.get("item:1").await.unwrap().unwrap().bytes,
        b"table-value"
    );
    assert_eq!(client.get("item:1").await.unwrap().unwrap().bytes, b"123");
    assert_eq!(
        users
            .query("SELECT COUNT(*) FROM users")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    client.close().await;
    assert_eq!(node.peer_count().await, 0);
    server.shutdown().await.unwrap();
    node.put("still-running".into(), b"ok".to_vec())
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_queries_match_embedded_results_and_keep_the_sdk_connection_usable() {
    let (node, mut listener) = server(GrpcServerOptions::default()).await;
    let client = Client::connect(
        format!("http://{}", listener.addr()),
        ClientOptions::default(),
    )
    .await
    .unwrap();
    client.put("plain", b"opaque\0\xff".to_vec()).await.unwrap();
    client
        .put_with_ttl("ttl", b"ttl".to_vec(), Some(Duration::from_secs(3600)))
        .await
        .unwrap();
    client.put("deleted", b"gone".to_vec()).await.unwrap();
    client.delete("deleted").await.unwrap();
    for projection in [
        "timestamp_ms",
        "logical_counter",
        "node_id",
        "expires_at_ms",
        "is_tombstone",
    ] {
        for key in ["plain", "ttl", "deleted"] {
            let sql =
                format!("SELECT {projection} FROM kv INCLUDING TOMBSTONES WHERE key = '{key}'");
            let embedded = node.query(&sql).await.unwrap();
            let remote = client.query(&sql).await.unwrap();
            assert_eq!(remote, embedded, "{sql}");
            assert_eq!(remote.rows.len(), 1);
            assert_eq!(
                client.get("plain").await.unwrap().unwrap().bytes,
                b"opaque\0\xff"
            );
        }
    }
    let sql = "SELECT key, value, timestamp_ms, logical_counter, node_id, expires_at_ms, is_tombstone, frag, OCTET_LENGTH(value), PREFIX_BYTES(value, ?) FROM kv INCLUDING TOMBSTONES WHERE timestamp_ms >= ? AND logical_counter >= ? AND node_id = ? AND is_tombstone >= ? AND STARTS_WITH(key, ?)";
    let params = [
        QueryValue::Integer(2),
        QueryValue::Integer(0),
        QueryValue::Integer(0),
        QueryValue::Text("sdk-node".into()),
        QueryValue::Integer(0),
        QueryValue::Text("".into()),
    ];
    assert_eq!(
        client.query_with_params(sql, &params).await.unwrap(),
        node.query_with_params(sql, &params).await.unwrap()
    );
    let expiry_sql = "SELECT key, expires_at_ms FROM kv WHERE expires_at_ms >= ?";
    assert_eq!(
        client
            .query_with_params(expiry_sql, &[QueryValue::Integer(1)])
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    let tombstones = client.query("SELECT key, timestamp_ms, logical_counter, node_id, expires_at_ms, is_tombstone FROM kv INCLUDING TOMBSTONES WHERE is_tombstone = 1").await.unwrap();
    assert_eq!(tombstones.rows.len(), 1);
    assert_eq!(tombstones.rows[0][0], QueryValue::Text("deleted".into()));
    assert_eq!(tombstones.rows[0][5], QueryValue::Integer(1));
    assert_eq!(
        client
            .query("SELECT expires_at_ms FROM kv WHERE key = 'plain'")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Null]]
    );
    // Server-rejected SQL/ranges must not retire a valid application socket.
    for (sql, params) in [
        (
            "SELECT timestamp_ms FROM kv WHERE timestamp_ms >= ?",
            vec![QueryValue::Integer(-1)],
        ),
        (
            "SELECT logical_counter FROM kv WHERE logical_counter = ?",
            vec![QueryValue::Integer(u64::MAX as i128 + 1)],
        ),
        (
            "SELECT is_tombstone FROM kv WHERE is_tombstone = ?",
            vec![QueryValue::Integer(2)],
        ),
        ("SELECT unknown_metadata FROM kv", vec![]),
    ] {
        assert!(matches!(
            client.query_with_params(sql, &params).await,
            Err(ClientError::Server(error)) if error.code == wire::ErrorCode::Invalid as i32
        ));
        assert!(client.get("plain").await.unwrap().is_some());
    }
    client.close().await;
    listener.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_shutdown_drains_canceled_client_mutation_before_returning() {
    use actordb::actor::VersionedValue;
    use actordb::store::{KvStore, StoreError};
    struct BlockingStore {
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        gate: std::sync::Condvar,
    }
    impl KvStore for BlockingStore {
        fn bind(
            &self,
            _identity: &actordb::store::StoreIdentity,
        ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
            Ok(actordb::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, actordb::actor::VersionedValue)>, actordb::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> actordb::store::StoreCommitDurability {
            actordb::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<actordb::store::StoreCommitDurability, actordb::store::StoreError> {
            Ok(self.commit_durability())
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            if !writes.is_empty() {
                self.entered.notify_one();
                let mut released = self.released.lock().unwrap();
                while !*released {
                    released = self.gate.wait(released).unwrap();
                }
            }
            Ok(())
        }
    }
    let store = Arc::new(BlockingStore {
        entered: tokio::sync::Notify::new(),
        released: std::sync::Mutex::new(false),
        gate: std::sync::Condvar::new(),
    });
    struct Release(Arc<BlockingStore>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.released.lock().unwrap() = true;
            self.0.gate.notify_all();
        }
    }
    let _release = Release(store.clone());
    let config = NodeConfig::builder()
        .store(store.clone())
        .flush_batch_size(1)
        .build();
    let node = Arc::new(
        Node::start(
            "drain-origin".into(),
            "127.0.0.1:0".parse().unwrap(),
            config,
        )
        .await
        .unwrap(),
    );
    let replica = Node::start(
        "drain-replica".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    node.connect(replica.addr()).await.unwrap();
    let mut server = GrpcServer::bind(
        node.clone(),
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
    let active = client.clone();
    let write = tokio::spawn(async move { active.put("retained", b"accepted".to_vec()).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    write.abort();
    let _ = write.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), server.shutdown())
            .await
            .is_err()
    );
    // Canceling the waiter retains the join handle for a subsequent retry.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), server.shutdown())
            .await
            .is_err()
    );
    *store.released.lock().unwrap() = true;
    store.gate.notify_all();
    tokio::time::timeout(Duration::from_secs(5), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.get("retained".into()).await.unwrap().unwrap().value,
        b"accepted"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if replica.get("retained".into()).await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.shutdown().await.unwrap();
    replica.shutdown().await.unwrap();
}
