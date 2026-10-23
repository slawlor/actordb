use actordb::node::{Node, NodeConfig};

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

    let users_result = users.query("SELECT key, value FROM kv").await.unwrap();
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
        .query("SELECT value FROM kv WHERE key = 'same'")
        .await
        .unwrap();
    assert_eq!(events_result.rows.len(), 1);
    assert_eq!(
        events_result.rows[0][0],
        actordb_query::QueryValue::Bytes(b"event-value".to_vec())
    );

    assert!(node.get("same".into()).await.unwrap().is_none());
}
