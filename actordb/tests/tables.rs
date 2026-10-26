use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig};
use actordb::store::{KvStore, StoreError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RecordingStore {
    records: Mutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl KvStore for RecordingStore {
    fn bind(
        &self,
        _identity: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
        Ok(actordb::store::StoreBinding::unmanaged())
    }

    fn load_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, actordb::actor::VersionedValue)>, actordb::store::StoreError> {
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
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.records.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
}

async fn node() -> Node {
    Node::start(
        "table-node".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn logical_tables_share_a_node_but_isolate_keys_and_queries() {
    let node = node().await;
    let users = node.table("users").unwrap();
    let events = node.table("events").unwrap();

    users.put("same", b"user-value".to_vec()).await.unwrap();
    events.put("same", b"event-value".to_vec()).await.unwrap();

    assert_eq!(
        users.get("same").await.unwrap().unwrap().value,
        b"user-value"
    );
    assert_eq!(
        events.get("same").await.unwrap().unwrap().value,
        b"event-value"
    );

    let users_result = users.query("SELECT key, value FROM users").await.unwrap();
    assert_eq!(users_result.rows.len(), 1);
    assert_eq!(
        users_result.rows[0][0],
        actordb_query::QueryValue::Text("same".into())
    );
    assert_eq!(
        users_result.rows[0][1],
        actordb_query::QueryValue::Bytes(b"user-value".to_vec())
    );

    let events_result = events
        .query("SELECT value FROM events WHERE key = 'same'")
        .await
        .unwrap();
    assert_eq!(events_result.rows.len(), 1);
    assert_eq!(
        events_result.rows[0][0],
        actordb_query::QueryValue::Bytes(b"event-value".to_vec())
    );

    assert!(node.get("same".into()).await.unwrap().is_none());
}

#[tokio::test]
async fn table_partitions_reload_from_disjoint_durable_prefixes() {
    let store = Arc::new(RecordingStore::default());
    let mut config = NodeConfig::default();
    config.store = Some(store.clone());
    let node = Node::start(
        "table-persist".into(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    node.table("users")
        .unwrap()
        .put("same", b"user".to_vec())
        .await
        .unwrap();
    node.table("events")
        .unwrap()
        .put("same", b"event".to_vec())
        .await
        .unwrap();
    node.shutdown().await.unwrap();

    {
        let records = store.records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records
            .keys()
            .all(|key| key[4..].starts_with(b"\x1dactordb.table.store.v1:")));
    }

    let mut config = NodeConfig::default();
    config.store = Some(store);
    let node = Node::start(
        "table-persist".into(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    assert_eq!(
        node.table("users")
            .unwrap()
            .get("same")
            .await
            .unwrap()
            .unwrap()
            .value,
        b"user"
    );
    assert_eq!(
        node.table("events")
            .unwrap()
            .get("same")
            .await
            .unwrap()
            .unwrap()
            .value,
        b"event"
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_table_reads_queries_and_scans_do_not_create_workers_across_fragments() {
    use actordb::actor::{KeyMatcher, ScanSpec, ValueTransform};
    use actordb::node::DetailedSnapshotOptions;
    let node = Node::start(
        "empty-tables".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(4)
            .table_pool_capacity(2)
            .build(),
    )
    .await
    .unwrap();
    let names: Vec<_> = (0..32).map(|index| format!("EMPTY_{index}")).collect();
    for name in &names {
        let table = node.table(name.clone()).unwrap();
        assert!(table.get("absent").await.unwrap().is_none());
        assert!(table
            .get_multi(vec!["absent".into(), "duplicate".into()])
            .await
            .iter()
            .all(|value| matches!(value, Ok(None))));
        assert!(table
            .query(&format!("SELECT key FROM {name}"))
            .await
            .unwrap()
            .rows
            .is_empty());
        assert!(table
            .scan(&ScanSpec {
                matcher: KeyMatcher::All,
                transform: ValueTransform::KeyOnly,
                limit: 1,
                offset: 0
            })
            .await
            .unwrap()
            .is_empty());
    }
    for chunk in names.chunks(4) {
        futures::future::join_all(chunk.iter().map(|name| {
            let node = &node;
            async move {
                let table = node.table(name.clone()).unwrap();
                assert!(table.get("concurrent-miss").await.unwrap().is_none());
                assert!(table
                    .query(&format!("SELECT key FROM {name}"))
                    .await
                    .unwrap()
                    .rows
                    .is_empty());
            }
        }))
        .await;
    }
    let status = node
        .status_snapshot_detailed(
            DetailedSnapshotOptions::builder()
                .tables(names)
                .include_internal(false)
                .max_pools(256)
                .max_workers(1024)
                .build(),
        )
        .await
        .unwrap();
    assert!(
        status.pools.is_empty(),
        "unknown tables have no worker sets"
    );
    assert_eq!(
        node.status_snapshot().readiness,
        actordb::node::status::Readiness::Ready
    );
    assert_eq!(node.query_diagnostics().reserved_runtime_bytes, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_worker_cache_preserves_durable_tables_and_tombstones_across_restart() {
    use actordb::node::DetailedSnapshotOptions;
    let store = Arc::new(RecordingStore::default());
    let names: Vec<_> = (0..24).map(|index| format!("DATA_{index}")).collect();
    for boot in 0..2 {
        let node = Node::start(
            "cached-durable-tables".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .store(store.clone())
                .table_pool_capacity(2)
                .build(),
        )
        .await
        .unwrap();
        for (index, name) in names.iter().enumerate() {
            let table = node.table(name.clone()).unwrap();
            if boot == 0 {
                table.put("live", vec![index as u8]).await.unwrap();
                table.put("deleted", vec![7]).await.unwrap();
                table.delete("deleted").await.unwrap();
                table
                    .put_with_options(
                        "ttl",
                        vec![8],
                        Some(std::time::Duration::from_secs(3600)),
                        actordb::actor::OpPriority::Default,
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(
                table.get("live").await.unwrap().unwrap().value,
                vec![index as u8]
            );
            assert!(table.get("deleted").await.unwrap().is_none());
            assert_eq!(
                table
                    .query(&format!("SELECT COUNT(*) FROM {name} INCLUDING TOMBSTONES"))
                    .await
                    .unwrap()
                    .rows,
                vec![vec![actordb_query::QueryValue::Integer(3)]]
            );
            assert!(table
                .get("ttl")
                .await
                .unwrap()
                .unwrap()
                .expires_at_ms
                .is_some());
            let status = node
                .status_snapshot_detailed(
                    DetailedSnapshotOptions::builder()
                        .tables(names.clone())
                        .include_internal(false)
                        .max_pools(256)
                        .max_workers(1024)
                        .build(),
                )
                .await
                .unwrap();
            assert!(
                status.pools.len() <= 6,
                "at most two table sets per fragment"
            );
            assert!(
                status
                    .pools
                    .iter()
                    .map(|entry| entry.pool.worker_count)
                    .sum::<usize>()
                    <= 16
            );
        }
        assert_eq!(
            node.status_snapshot().readiness,
            actordb::node::status::Readiness::Ready
        );
        node.shutdown().await.unwrap();
    }
    assert_eq!(store.records.lock().unwrap().len(), 72);
}

#[tokio::test]
async fn table_worker_capacity_validates_before_node_startup() {
    for capacity in [0, 4097] {
        assert!(Node::start(
            "bad-table-capacity".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().table_pool_capacity(capacity).build()
        )
        .await
        .is_err());
    }
}
