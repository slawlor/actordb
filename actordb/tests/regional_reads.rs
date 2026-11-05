mod common;

use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::shard::FragmentId;
use std::collections::HashMap;
use std::time::Duration;

async fn node(id: &str, regions: HashMap<String, String>) -> Node {
    Node::start_with_options(
        id.into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_map(HashMap::from([(
                FragmentId(0),
                vec!["east".into(), "west".into()],
            )]))
            .build(),
        NodeRuntimeOptions::default().with_regions(regions),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn all_read_paths_prefer_same_region_then_fall_back() {
    let regions = HashMap::from([
        ("east".into(), "east".into()),
        ("west".into(), "west".into()),
        ("ingress".into(), "west".into()),
    ]);
    let east = node("east", regions.clone()).await;
    let west = node("west", regions.clone()).await;
    let ingress = node("ingress", regions).await;
    // Replicas are deliberately disconnected from each other: distinct local
    // values identify which holder answered without timing assertions.
    east.put("key".into(), b"east".to_vec()).await.unwrap();
    west.put("key".into(), b"west".to_vec()).await.unwrap();
    ingress.connect(east.addr()).await.unwrap();
    ingress.connect(west.addr()).await.unwrap();
    common::wait_for(
        || async { ingress.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        ingress.get("key".into()).await.unwrap().unwrap().value,
        b"west"
    );
    let batch = ingress.get_multi(vec!["key".into()]).await;
    assert_eq!(batch[0].as_ref().unwrap().as_ref().unwrap().value, b"west");
    let rows = ingress.query("SELECT value FROM kv").await.unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![actordb_query::QueryValue::Bytes(b"west".to_vec())]]
    );
    let scan = ingress
        .scan(&actordb::actor::ScanSpec {
            matcher: actordb::actor::KeyMatcher::All,
            transform: actordb::actor::ValueTransform::Identity,
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(scan[0].value, b"west");
    // A serving node remains preferred even when its remote peers are earlier
    // in the shared fragment map.
    assert_eq!(
        west.get("key".into()).await.unwrap().unwrap().value,
        b"west"
    );
    west.shutdown().await.unwrap();
    common::wait_for(
        || async { ingress.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        ingress.get("key".into()).await.unwrap().unwrap().value,
        b"east"
    );
    assert_eq!(
        ingress.query("SELECT value FROM kv").await.unwrap().rows,
        vec![vec![actordb_query::QueryValue::Bytes(b"east".to_vec())]]
    );
    ingress.shutdown().await.unwrap();
    east.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_origin_label_preserves_map_order() {
    let regions = HashMap::from([
        ("east".into(), "east".into()),
        ("west".into(), "west".into()),
    ]);
    let east = node("east", regions.clone()).await;
    let west = node("west", regions.clone()).await;
    let ingress = node("ingress", regions).await;
    east.put("key".into(), b"east".to_vec()).await.unwrap();
    west.put("key".into(), b"west".to_vec()).await.unwrap();
    ingress.connect(east.addr()).await.unwrap();
    ingress.connect(west.addr()).await.unwrap();
    common::wait_for(
        || async { ingress.peer_count().await == 2 },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        ingress.get("key".into()).await.unwrap().unwrap().value,
        b"east"
    );
    ingress.shutdown().await.unwrap();
    east.shutdown().await.unwrap();
    west.shutdown().await.unwrap();
}
