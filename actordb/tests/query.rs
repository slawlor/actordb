//! Query behavior through public Node APIs, including real peer forwarding.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig, NodeError};
use actordb::query::QueryOptions;
use actordb::shard::{fragment_of, FragmentId};
use actordb::store::{KvStore, StoreError};
use actordb_query::{parse, QueryValue};

mod common;

// Load a large, deterministic fixture without timing the write/replication path.
struct LoadedStore(Vec<(Vec<u8>, VersionedValue)>);

impl KvStore for LoadedStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self.0.clone())
    }

    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .0
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
}

fn loaded_store(count: usize, fragments: usize) -> Arc<dyn KvStore> {
    let timestamp = actordb::actor::now_ms().saturating_sub(1_000);
    Arc::new(LoadedStore(
        (0..count)
            .map(|index| {
                let key = format!("record:{index:06}");
                let mut stored_key = fragment_of(&key, fragments).0.to_be_bytes().to_vec();
                stored_key.extend_from_slice(key.as_bytes());
                (
                    stored_key,
                    VersionedValue::new(b"123".to_vec(), timestamp, "fixture".into()),
                )
            })
            .collect(),
    ))
}

async fn start(id: &str, config: NodeConfig) -> Node {
    Node::start(id.into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap()
}

#[tokio::test]
async fn aggregate_exhausts_input_beyond_both_legacy_scan_caps() {
    let count = 110_003;
    let node = start(
        "query-complete",
        NodeConfig {
            fragment_count: 4,
            store: Some(loaded_store(count, 4)),
            ..NodeConfig::default()
        },
    )
    .await;
    let result = node
        .query("SELECT COUNT(*), SUM(OCTET_LENGTH(value)), SUM(CAST(value AS BIGINT)) FROM kv")
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            QueryValue::Integer(count as i128),
            QueryValue::Integer(3 * count as i128),
            QueryValue::Integer(123 * count as i128),
        ]]
    );
    let page = node
        .query("SELECT key FROM kv ORDER BY key ASC LIMIT 7 OFFSET 25")
        .await
        .unwrap();
    assert_eq!(
        page.rows,
        (25..32)
            .map(|index| vec![QueryValue::Text(format!("record:{index:06}"))])
            .collect::<Vec<_>>()
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn fragment_scope_and_key_equality_do_not_contact_unavailable_fragments() {
    let node = start(
        "query-scoped",
        NodeConfig {
            fragment_count: 2,
            fragment_map: HashMap::from([
                (FragmentId(0), vec!["query-scoped".into()]),
                (FragmentId(1), vec!["missing-holder".into()]),
            ]),
            ..NodeConfig::default()
        },
    )
    .await;
    let key = (0..1_000)
        .map(|index| format!("local:{index}"))
        .find(|key| fragment_of(key, 2) == FragmentId(0))
        .unwrap();
    node.put(key.clone(), b"live".to_vec()).await.unwrap();
    assert_eq!(
        node.query("SELECT COUNT(*) FROM kv WHERE frag IN (0)")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    assert_eq!(
        node.query_with_params(
            "SELECT key, frag FROM kv WHERE key = ?",
            &[QueryValue::Text(key.clone())],
        )
        .await
        .unwrap()
        .rows,
        vec![vec![QueryValue::Text(key), QueryValue::Integer(0)]]
    );
    assert!(node.query("SELECT COUNT(*) FROM kv").await.is_err());
    assert!(node
        .query("SELECT COUNT(*) FROM kv WHERE frag = 2")
        .await
        .is_err());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_merges_local_and_remote_fragment_views_in_global_key_order() {
    let placement = HashMap::from([
        (FragmentId(0), vec!["query-a".into()]),
        (FragmentId(1), vec!["query-b".into()]),
        (FragmentId(2), vec!["query-a".into()]),
        (FragmentId(3), vec!["query-b".into()]),
    ]);
    let config = || NodeConfig {
        fragment_count: 4,
        fragment_map: placement.clone(),
        ..NodeConfig::default()
    };
    let a = start("query-a", config()).await;
    let b = start("query-b", config()).await;
    b.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    let mut keys = Vec::new();
    for index in 0..80 {
        let key = format!("user:{index:03}");
        let owner = if fragment_of(&key, 4).0.is_multiple_of(2) {
            &a
        } else {
            &b
        };
        owner.put(key.clone(), b"42".to_vec()).await.unwrap();
        keys.push(key);
    }
    let query = "SELECT key FROM kv WHERE STARTS_WITH(key, 'user:') LIMIT 13 OFFSET 9";
    let expected = keys[9..22]
        .iter()
        .map(|key| vec![QueryValue::Text(key.clone())])
        .collect::<Vec<_>>();
    assert_eq!(a.query(query).await.unwrap().rows, expected);
    assert_eq!(b.query(query).await.unwrap().rows, expected);
    assert_eq!(
        a.query("SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(80), QueryValue::Integer(80 * 42)]]
    );
    a.shutdown().await.unwrap();
    assert!(b.query("SELECT COUNT(*) FROM kv").await.is_err());
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn queries_hide_deletes_and_expiry_and_preserve_empty_unicode_keys() {
    let node = start("query-visibility", NodeConfig::default()).await;
    for key in ["", "α", "deleted"] {
        node.put(key.into(), b"value".to_vec()).await.unwrap();
    }
    node.delete("deleted".into()).await.unwrap();
    let expired = node
        .put_with_ttl("expired".into(), b"value".to_vec(), Some(Duration::ZERO))
        .await;
    assert!(matches!(expired, Ok(()) | Err(NodeError::Expired)));
    let result = node.query("SELECT key FROM kv").await.unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![QueryValue::Text(String::new())],
            vec![QueryValue::Text("α".into())],
        ]
    );
    assert!(node
        .query("SELECT key FROM kv LIMIT 0")
        .await
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        node.query("SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM kv WHERE key = 'absent'")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(0), QueryValue::Null]]
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_budgets_and_bad_numeric_values_fail_explicitly() {
    let node = start(
        "query-budget",
        NodeConfig {
            fragment_count: 1,
            store: Some(loaded_store(10, 1)),
            ..NodeConfig::default()
        },
    )
    .await;
    let plan = parse("SELECT COUNT(*) FROM kv").unwrap();
    let mut scoped = plan.clone();
    scoped.fragments = Some(vec![0, 0]);
    assert_eq!(
        node.query_plan(&scoped).await.unwrap().rows,
        vec![vec![QueryValue::Integer(10)]]
    );
    assert!(node
        .query_plan_with_options(
            &plan,
            QueryOptions {
                max_scanned_rows: 2,
                ..QueryOptions::default()
            },
        )
        .await
        .is_err());
    node.put("wide".into(), vec![0; 4_096]).await.unwrap();
    assert!(node
        .query_plan_with_options(
            &parse("SELECT value FROM kv WHERE key = 'wide'").unwrap(),
            QueryOptions {
                page_bytes: 1_024,
                ..QueryOptions::default()
            },
        )
        .await
        .is_err());
    assert!(node
        .query("SELECT SUM(CAST(value AS BIGINT)) FROM kv WHERE key = 'wide'")
        .await
        .is_err());
    // A failed query does not close serving admission.
    assert_eq!(
        node.get("wide".into()).await.unwrap().unwrap().value.len(),
        4_096
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn selected_key_ranges_charge_only_selected_index_work() {
    let node = start(
        "query-seek",
        NodeConfig {
            fragment_count: 1,
            store: Some(loaded_store(10_000, 1)),
            ..NodeConfig::default()
        },
    )
    .await;
    for (sql, expected) in [
        (
            "SELECT key FROM kv WHERE key = 'record:009999'",
            vec!["record:009999"],
        ),
        (
            "SELECT key FROM kv WHERE key >= 'record:009997' AND key < 'record:009999'",
            vec!["record:009997", "record:009998"],
        ),
        (
            "SELECT key FROM kv WHERE key = 'record:009999' AND key < 'record:000000'",
            vec![],
        ),
    ] {
        let result = node
            .query_plan_with_options(
                &parse(sql).unwrap(),
                QueryOptions {
                    max_scanned_rows: 3,
                    page_visits: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            result.rows,
            expected
                .into_iter()
                .map(|key| vec![QueryValue::Text(key.into())])
                .collect::<Vec<_>>()
        );
    }
    let aggregate = node
        .query("SELECT COUNT(*) FROM kv WHERE key > 'z' AND key < 'a'")
        .await
        .unwrap();
    assert_eq!(aggregate.rows, vec![vec![QueryValue::Integer(0)]]);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn many_fragment_heads_merge_in_global_key_order() {
    let node = start(
        "query-heap",
        NodeConfig {
            fragment_count: 32,
            store: Some(loaded_store(200, 32)),
            ..Default::default()
        },
    )
    .await;
    let result = node
        .query("SELECT key FROM kv ORDER BY key ASC LIMIT 100 OFFSET 50")
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        (50..150)
            .map(|index| vec![QueryValue::Text(format!("record:{index:06}"))])
            .collect::<Vec<_>>()
    );
    node.shutdown().await.unwrap();
}
