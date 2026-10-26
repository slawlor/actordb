use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use actordb_client::{Client, ClientOptions, QueryValue};
use actordb_protocol::client::Scan;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn sdk_named_scan_and_query_limits_preserve_namespaces_through_remote_holder() {
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(
            FragmentId(0),
            vec!["sdk-namespace-holder".into()],
        )]))
        .build();
    let holder = Node::start(
        "sdk-namespace-holder".into(),
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
    )
    .await
    .unwrap();
    let gateway = Arc::new(
        Node::start(
            "sdk-namespace-gateway".into(),
            "127.0.0.1:0".parse().unwrap(),
            config,
        )
        .await
        .unwrap(),
    );
    gateway.connect(holder.addr()).await.unwrap();
    let mut server = ClientServer::bind(
        gateway.clone(),
        "127.0.0.1:0".parse().unwrap(),
        ClientServerOptions::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap();
    let users = client.table("USERS").unwrap();
    users.put("a", b"abc".to_vec()).await.unwrap();
    users.put("ab", b"def".to_vec()).await.unwrap();
    users.put("b", b"ghi".to_vec()).await.unwrap();
    client.put("not-a-user", vec![99]).await.unwrap();
    for (transform_kind, prefix_bytes, expected) in [
        (0, 0, b"def".to_vec()),
        (1, 0, vec![]),
        (2, 1, b"d".to_vec()),
    ] {
        let rows = users
            .scan(Scan {
                matcher_kind: 0,
                prefix: vec![],
                transform_kind,
                prefix_bytes,
                limit: 1,
                offset: 1,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "ab");
        assert_eq!(rows[0].value, expected);
    }
    let rows = users
        .scan(Scan {
            matcher_kind: 1,
            prefix: b"a".to_vec(),
            transform_kind: 1,
            prefix_bytes: 0,
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(),
        vec!["a", "ab"]
    );
    let key = "x".repeat(actordb_query::MAX_QUERY_LITERAL_BYTES);
    users.put(&key, vec![7]).await.unwrap();
    let repeated_sql = format!(
        "SELECT key FROM users WHERE {}",
        std::iter::repeat_n("key = ?", 32)
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let repeated_params = vec![QueryValue::Text(key.clone()); 32];
    assert_eq!(
        users
            .query_with_params(&repeated_sql, &repeated_params)
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Text(key.clone())]]
    );
    assert_eq!(
        users
            .query_with_params(
                "SELECT key FROM users WHERE key = ?",
                &[QueryValue::Text(key.clone())]
            )
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Text(key.clone())]]
    );
    assert_eq!(
        users
            .query_with_params(
                "SELECT COUNT(*) FROM users WHERE STARTS_WITH(key, ?)",
                &[QueryValue::Bytes(key.as_bytes().to_vec())]
            )
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    let clauses = std::iter::repeat_n("is_tombstone = 0", 32)
        .collect::<Vec<_>>()
        .join(" AND ");
    assert_eq!(
        users
            .query(&format!("SELECT COUNT(*) FROM users WHERE {clauses}"))
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(4)]]
    );
    let literal = "\u{001e}actordb.table.v1:5553455253:61";
    client.put(literal, vec![11]).await.unwrap();
    assert_eq!(
        gateway.get(literal.into()).await.unwrap().unwrap().value,
        [11]
    );
    assert_eq!(users.get("a").await.unwrap().unwrap().bytes, b"abc");
    let literal_scan = client
        .scan(Scan {
            matcher_kind: 1,
            prefix: literal.as_bytes().to_vec(),
            transform_kind: 1,
            prefix_bytes: 0,
            limit: 1,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(literal_scan[0].key, literal);
    client.close().await;
    server.shutdown().await;
    gateway.shutdown().await.unwrap();
    holder.shutdown().await.unwrap();
}
