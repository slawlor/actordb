use actordb::actor::{KeyMatcher, OpPriority, ScanSpec, ValueTransform};
use actordb::node::{Node, NodeConfig};
use actordb::shard::{fragment_of_literal, FragmentId};
use actordb::write_concern::{WriteConcern, WriteConcernOptions};
use actordb_query::{QueryValue, MAX_QUERY_EXPRESSIONS, MAX_QUERY_LITERAL_BYTES};
use std::collections::HashMap;
use std::time::Duration;

const RESERVED: &str = "\u{001e}actordb.table.v1:5553455253:76696374696d";
async fn node(id: &str, config: NodeConfig) -> Node {
    Node::start(id.into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap()
}
async fn connect(gateway: &Node, holder: &Node) {
    gateway.connect(holder.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while gateway.peer_count().await != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn local_policy() -> WriteConcernOptions {
    WriteConcernOptions::builder()
        .concern(WriteConcern::Local)
        .require_evidence(true)
        .build()
}

async fn literal_checks(node: &Node) {
    node.table("USERS")
        .unwrap()
        .put("victim", vec![100])
        .await
        .unwrap();
    let keys = [
        RESERVED.to_owned(),
        "\0".into(),
        "雪/é🦀".into(),
        "".into(),
        "\u{001e}actordb.table.v1:4b56:78".into(),
    ];
    for (index, key) in keys.iter().enumerate() {
        node.put_with_options(key.clone(), vec![index as u8], None, OpPriority::High)
            .await
            .unwrap();
        assert_eq!(
            node.get(key.clone()).await.unwrap().unwrap().value,
            vec![index as u8]
        );
        assert_eq!(
            node.table("KV")
                .unwrap()
                .get(key)
                .await
                .unwrap()
                .unwrap()
                .value,
            vec![index as u8]
        );
    }
    assert_eq!(
        node.table("USERS")
            .unwrap()
            .get("victim")
            .await
            .unwrap()
            .unwrap()
            .value,
        [100]
    );
    node.put_with_ttl(RESERVED.into(), vec![13], Some(Duration::from_secs(60)))
        .await
        .unwrap();
    assert!(node
        .table("KV")
        .unwrap()
        .get(RESERVED)
        .await
        .unwrap()
        .unwrap()
        .expires_at_ms
        .is_some());
    node.delete(RESERVED.into()).await.unwrap();
    assert!(node
        .table("KV")
        .unwrap()
        .get(RESERVED)
        .await
        .unwrap()
        .is_none());
    let writes = node
        .put_multi(vec![
            (RESERVED.into(), vec![7]),
            ("雪/é🦀".into(), vec![8]),
            (RESERVED.into(), vec![9]),
        ])
        .await;
    assert!(writes.iter().all(Result::is_ok));
    let reads = node
        .get_multi(vec![RESERVED.into(), "雪/é🦀".into(), RESERVED.into()])
        .await;
    assert_eq!(
        reads
            .into_iter()
            .map(|value| value.unwrap().unwrap().value)
            .collect::<Vec<_>>(),
        vec![vec![9], vec![8], vec![9]]
    );
    let receipt = node
        .put_with_write_concern(RESERVED.into(), vec![11], local_policy())
        .await
        .unwrap();
    let via_table = node
        .table("KV")
        .unwrap()
        .put_with_write_concern_options(
            RESERVED,
            vec![12],
            None,
            OpPriority::Default,
            local_policy(),
        )
        .await
        .unwrap();
    assert_eq!(
        receipt.evidence().unwrap().operation().key_digest(),
        via_table.evidence().unwrap().operation().key_digest()
    );
    assert_eq!(
        node.table("USERS")
            .unwrap()
            .get("victim")
            .await
            .unwrap()
            .unwrap()
            .value,
        [100]
    );
    node.delete_with_write_concern(RESERVED.into(), local_policy())
        .await
        .unwrap();
    assert!(node.get(RESERVED.into()).await.unwrap().is_none());
    assert!(node
        .table("KV")
        .unwrap()
        .get(RESERVED)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        node.table("USERS")
            .unwrap()
            .get("victim")
            .await
            .unwrap()
            .unwrap()
            .value,
        [100]
    );
    node.delete_with_priority("雪/é🦀".into(), OpPriority::Background)
        .await
        .unwrap();
    assert!(node
        .table("KV")
        .unwrap()
        .get("雪/é🦀")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn literal_node_and_table_kv_keys_round_trip_locally() {
    let node = node(
        "literal-local",
        NodeConfig::builder().fragment_count(4).build(),
    )
    .await;
    literal_checks(&node).await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn literal_keys_batches_and_concerns_round_trip_through_forwarding() {
    let map = (0..4)
        .map(|id| (FragmentId(id), vec!["literal-holder".into()]))
        .collect();
    let config = NodeConfig::builder()
        .fragment_count(4)
        .fragment_map(map)
        .build();
    let holder = node("literal-holder", config.clone()).await;
    let gateway = node("literal-gateway", config).await;
    connect(&gateway, &holder).await;
    literal_checks(&gateway).await;
    gateway.shutdown().await.unwrap();
    holder.shutdown().await.unwrap();
}

async fn scan_checks(node: &Node) {
    let table = node.table("USERS").unwrap();
    for key in ["a", "ab", "b", "雪"] {
        table.put(key, key.as_bytes().to_vec()).await.unwrap();
    }
    node.put("not-a-user".into(), vec![99]).await.unwrap();
    for transform in [
        ValueTransform::Identity,
        ValueTransform::KeyOnly,
        ValueTransform::PrefixBytes(1),
    ] {
        let rows = table
            .scan(&ScanSpec {
                matcher: KeyMatcher::All,
                transform: transform.clone(),
                limit: 2,
                offset: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(),
            vec!["ab", "b"]
        );
        let expected = match transform {
            ValueTransform::Identity => vec![b"ab".to_vec(), b"b".to_vec()],
            ValueTransform::KeyOnly => vec![vec![], vec![]],
            _ => vec![b"a".to_vec(), b"b".to_vec()],
        };
        assert_eq!(
            rows.into_iter().map(|row| row.value).collect::<Vec<_>>(),
            expected
        );
    }
    let prefix = table
        .scan(&ScanSpec {
            matcher: KeyMatcher::Prefix(b"a".to_vec()),
            transform: ValueTransform::KeyOnly,
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        prefix
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "ab"]
    );
    assert_eq!(
        table
            .query("SELECT key FROM users WHERE STARTS_WITH(key, 'a')")
            .await
            .unwrap()
            .rows,
        vec![
            vec![QueryValue::Text("a".into())],
            vec![QueryValue::Text("ab".into())]
        ]
    );
}

#[tokio::test]
async fn named_scan_transforms_and_pagination_match_sql_locally_and_remotely() {
    let local = node("scan-local", NodeConfig::default()).await;
    scan_checks(&local).await;
    local.shutdown().await.unwrap();
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(FragmentId(0), vec!["scan-holder".into()])]))
        .build();
    let holder = node("scan-holder", config.clone()).await;
    let gateway = node("scan-gateway", config).await;
    connect(&gateway, &holder).await;
    scan_checks(&gateway).await;
    gateway.shutdown().await.unwrap();
    holder.shutdown().await.unwrap();
}

async fn boundary_checks(node: &Node) {
    let key = "x".repeat(MAX_QUERY_LITERAL_BYTES);
    node.put(key.clone(), vec![7]).await.unwrap();
    let params = [QueryValue::Text(key.clone())];
    assert_eq!(
        node.query_with_params("SELECT key FROM kv WHERE key = ?", &params)
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Text(key.clone())]]
    );
    let repeated_sql = format!(
        "SELECT key FROM kv WHERE {}",
        std::iter::repeat_n("key = ?", MAX_QUERY_EXPRESSIONS)
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let repeated_params = vec![QueryValue::Text(key.clone()); MAX_QUERY_EXPRESSIONS];
    let bound = actordb_query::parse_with_params(&repeated_sql, &repeated_params).unwrap();
    assert!(bound.to_sql().len() > actordb_query::MAX_SQL_BYTES);
    assert!(
        actordb_query::parse(&bound.to_sql()).is_err(),
        "raw API SQL remains bounded"
    );
    assert_eq!(
        node.query_with_params(&repeated_sql, &repeated_params)
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Text(key.clone())]]
    );
    assert_eq!(
        node.query_plan(&bound).await.unwrap().rows,
        vec![vec![QueryValue::Text(key.clone())]]
    );
    let prefix = [QueryValue::Bytes(key.as_bytes().to_vec())];
    assert_eq!(
        node.query_with_params("SELECT COUNT(*) FROM kv WHERE STARTS_WITH(key, ?)", &prefix)
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    let clauses = std::iter::repeat_n("is_tombstone = 0", MAX_QUERY_EXPRESSIONS)
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT COUNT(*) FROM kv WHERE {clauses}");
    assert_eq!(
        node.query(&sql).await.unwrap().rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    let mut plan = actordb_query::parse(&sql).unwrap();
    plan.fragments = Some(vec![fragment_of_literal(&key, 1).0]);
    assert_eq!(
        actordb_query::parse(&plan.to_sql())
            .unwrap()
            .metadata_predicates
            .len(),
        32
    );
    assert_eq!(
        node.query_plan(&plan).await.unwrap().rows,
        vec![vec![QueryValue::Integer(1)]]
    );
}

#[tokio::test]
async fn advertised_query_boundaries_survive_namespace_and_peer_canonicalization() {
    let local = node("boundary-local", NodeConfig::default()).await;
    boundary_checks(&local).await;
    local.shutdown().await.unwrap();
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(
            FragmentId(0),
            vec!["boundary-holder".into()],
        )]))
        .build();
    let holder = node("boundary-holder", config.clone()).await;
    let gateway = node("boundary-gateway", config).await;
    connect(&gateway, &holder).await;
    boundary_checks(&gateway).await;
    gateway.shutdown().await.unwrap();
    holder.shutdown().await.unwrap();
}

#[tokio::test]
async fn diagnostic_get_all_preserves_all_tables_and_reports_ambiguous_display() {
    let node = node("all-table-display", NodeConfig::default()).await;
    node.table("USERS")
        .unwrap()
        .put("victim", vec![1])
        .await
        .unwrap();
    node.put("plain".into(), vec![2]).await.unwrap();
    assert_eq!(node.get_all().await.unwrap().len(), 2);
    node.put(RESERVED.into(), vec![3]).await.unwrap();
    assert!(node.get_all().await.is_err());
    assert_eq!(node.get(RESERVED.into()).await.unwrap().unwrap().value, [3]);
    assert_eq!(
        node.table("USERS")
            .unwrap()
            .get("victim")
            .await
            .unwrap()
            .unwrap()
            .value,
        [1]
    );
    node.shutdown().await.unwrap();
}
