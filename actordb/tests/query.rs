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
    fn bind(
        &self,
        _identity: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
        Ok(actordb::store::StoreBinding::unmanaged())
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
                let user_key = format!("record:{index:06}");
                let encoded_key = user_key
                    .bytes()
                    .flat_map(|byte| {
                        [
                            b"0123456789abcdef"[(byte >> 4) as usize],
                            b"0123456789abcdef"[(byte & 0x0f) as usize],
                        ]
                    })
                    .map(char::from)
                    .collect::<String>();
                let key = format!("\u{1e}actordb.table.v1:4b56:{encoded_key}");
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
        NodeConfig::builder()
            .fragment_count(4)
            .maybe_store(Some(loaded_store(count, 4)))
            .build(),
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
async fn merge_accounting_keeps_offset_limits_sparse_pages_and_budget_failures_exact() {
    let count = 2_048;
    let node = start(
        "query-accounting",
        NodeConfig::builder()
            .fragment_count(64)
            .store(loaded_store(count, 64))
            .build(),
    )
    .await;
    let mut plan = parse("SELECT key FROM kv ORDER BY key ASC LIMIT 31 OFFSET 277").unwrap();
    plan.projections = vec![actordb_query::Projection::Key; 32];
    let options = QueryOptions {
        max_buffer_bytes: 8 * 1024 * 1024,
        max_result_bytes: 16 * 1024 * 1024,
        ..Default::default()
    };
    let result = node
        .query_plan_with_options(&plan, options.clone())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        (277..308)
            .map(|index| vec![QueryValue::Text(format!("record:{index:06}")); 32])
            .collect::<Vec<_>>()
    );
    // No row matches, but metadata filtering must still exhaust and advance
    // through sparse continuations rather than treating an empty page as EOF.
    let sparse = parse("SELECT key FROM kv WHERE logical_counter = 77").unwrap();
    assert!(node
        .query_plan_with_options(&sparse, options.clone())
        .await
        .unwrap()
        .rows
        .is_empty());
    let aggregate = node
        .query_plan_with_options(&parse("SELECT COUNT(*) FROM kv").unwrap(), options.clone())
        .await
        .unwrap();
    assert_eq!(
        aggregate.rows,
        vec![vec![QueryValue::Integer(count as i128)]]
    );
    let exhausted = QueryOptions {
        max_buffer_bytes: 32 * 1024,
        ..options
    };
    assert!(matches!(
        node.query_plan_with_options(&plan, exhausted).await,
        Err(actordb::query::QueryExecutionError::BudgetExceeded(_))
    ));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn sql_projects_durable_record_metadata() {
    let node = start(
        "query-metadata",
        NodeConfig::builder().fragment_count(1).build(),
    )
    .await;
    node.put("metadata-key".into(), b"opaque payload".to_vec())
        .await
        .unwrap();
    let result = node
        .query(
            "SELECT timestamp_ms, logical_counter, node_id, expires_at_ms, is_tombstone \
             FROM kv WHERE key = 'metadata-key'",
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0][0], QueryValue::Integer(value) if value > 0));
    assert!(matches!(result.rows[0][1], QueryValue::Integer(value) if value >= 0));
    assert!(matches!(result.rows[0][2], QueryValue::Text(ref value) if value == "query-metadata"));
    assert_eq!(result.rows[0][3], QueryValue::Null);
    assert_eq!(result.rows[0][4], QueryValue::Integer(0));
    node.put("live-key".into(), b"still live".to_vec())
        .await
        .unwrap();
    assert_eq!(
        node.query("SELECT key FROM kv WHERE is_tombstone = 1")
            .await
            .unwrap()
            .rows,
        Vec::<Vec<QueryValue>>::new()
    );
    node.delete("metadata-key".into()).await.unwrap();
    let tombstones = node
        .query(
            "SELECT key, is_tombstone, node_id FROM kv INCLUDING TOMBSTONES \
             WHERE is_tombstone = 1 AND node_id = 'query-metadata'",
        )
        .await
        .unwrap();
    assert_eq!(
        tombstones.rows,
        vec![vec![
            QueryValue::Text("metadata-key".into()),
            QueryValue::Integer(1),
            QueryValue::Text("query-metadata".into()),
        ]]
    );
    assert_eq!(
        node.query("SELECT key, is_tombstone FROM kv INCLUDING TOMBSTONES")
            .await
            .unwrap()
            .rows,
        vec![
            vec![QueryValue::Text("live-key".into()), QueryValue::Integer(0)],
            vec![
                QueryValue::Text("metadata-key".into()),
                QueryValue::Integer(1)
            ],
        ]
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn fragment_scope_and_key_equality_do_not_contact_unavailable_fragments() {
    let node = start(
        "query-scoped",
        NodeConfig::builder()
            .fragment_count(2)
            .fragment_map(HashMap::from([
                (FragmentId(0), vec!["query-scoped".into()]),
                (FragmentId(1), vec!["missing-holder".into()]),
            ]))
            .build(),
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
    let config = || {
        NodeConfig::builder()
            .fragment_count(4)
            .fragment_map(placement.clone())
            .build()
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
    let metadata = a
        .query("SELECT key, node_id FROM kv WHERE STARTS_WITH(key, 'user:')")
        .await
        .unwrap();
    assert_eq!(metadata.rows.len(), keys.len());
    for (row, key) in metadata.rows.iter().zip(&keys) {
        let expected_owner = if fragment_of(key, 4).0.is_multiple_of(2) {
            "query-a"
        } else {
            "query-b"
        };
        assert_eq!(row[0], QueryValue::Text(key.clone()));
        assert_eq!(row[1], QueryValue::Text(expected_owner.into()));
    }
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
        NodeConfig::builder()
            .fragment_count(1)
            .maybe_store(Some(loaded_store(10, 1)))
            .build(),
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
        NodeConfig::builder()
            .fragment_count(1)
            .store(loaded_store(10_000, 1))
            .build(),
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
    for key in ["雪", "雪:1", "雲"] {
        node.put(key.into(), b"123".to_vec()).await.unwrap();
    }
    for (sql, expected) in [
        (
            "SELECT key FROM kv WHERE STARTS_WITH(key, X'e99b')",
            vec!["雪", "雪:1", "雲"],
        ),
        ("SELECT key FROM kv WHERE STARTS_WITH(key, X'ff')", vec![]),
        (
            "SELECT key FROM kv WHERE STARTS_WITH(key, X'e9') AND key < 'a'",
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
        NodeConfig::builder()
            .fragment_count(32)
            .store(loaded_store(200, 32))
            .build(),
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

#[tokio::test]
async fn runtime_allowance_is_configurable_and_visible_in_diagnostics() {
    use actordb::node::NodeRuntimeOptions;
    let node = Node::start_with_options(
        "query-runtime-budget".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_query_runtime_byte_capacity(1024 * 1024),
    )
    .await
    .unwrap();
    let plan = parse("SELECT COUNT(*) FROM kv").unwrap();
    assert!(matches!(
        node.query_plan(&plan).await,
        Err(actordb::query::QueryExecutionError::Backpressure(_))
    ));
    let result = node
        .query_plan_with_options(
            &plan,
            QueryOptions {
                max_result_bytes: 16 * 1024,
                max_buffer_bytes: 128 * 1024,
                page_bytes: 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![QueryValue::Integer(0)]]);
    let diagnostics = node.query_diagnostics();
    assert_eq!(diagnostics.runtime_byte_capacity, 1024 * 1024);
    assert_eq!(diagnostics.reserved_runtime_bytes, 0);
    assert_eq!(diagnostics.admission_rejections, 1);
    node.shutdown().await.unwrap();
    assert!(Node::start_with_options(
        "query-zero".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_query_runtime_byte_capacity(0)
    )
    .await
    .is_err());
}
