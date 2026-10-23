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
