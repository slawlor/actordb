mod common;

use std::collections::HashMap;
use std::time::Duration;

use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb::write_concern::{WriteConcern, WriteConcernError, WriteConcernOptions};

async fn node(id: &str, replicas: &[&str], regions: HashMap<String, String>) -> Node {
    Node::start_with_options(
        id.into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_map(HashMap::from([(
                FragmentId(0),
                replicas.iter().map(|id| (*id).to_string()).collect(),
            )]))
            .replication(SenderConfig::builder().batch_size(1).build())
            .build(),
        NodeRuntimeOptions::default().with_regions(regions),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn missing_or_impossible_regions_reject_put_and_delete_before_apply() {
    for regions in [
        HashMap::from([("region-invalid".into(), "west".into())]),
        HashMap::from([
            ("region-invalid".into(), "west".into()),
            ("region-invalid-peer".into(), "west".into()),
        ]),
    ] {
        let node = node(
            "region-invalid",
            &["region-invalid", "region-invalid-peer"],
            regions,
        )
        .await;
        let options = WriteConcernOptions::default().with_require_other_region(true);
        assert!(matches!(
            node.put_with_write_concern("absent".into(), vec![1], options.clone())
                .await,
            Err(WriteConcernError::InvalidPolicy(_))
        ));
        assert!(node.get("absent".into()).await.unwrap().is_none());
        node.put("existing".into(), vec![2]).await.unwrap();
        assert!(matches!(
            node.delete_with_write_concern("existing".into(), options)
                .await,
            Err(WriteConcernError::InvalidPolicy(_))
        ));
        assert_eq!(
            node.get("existing".into()).await.unwrap().unwrap().value,
            vec![2]
        );
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn count_one_times_out_without_other_region_then_succeeds_when_peer_connects() {
    let replicas = ["region-a", "region-b", "region-c"];
    let regions = HashMap::from([
        (replicas[0].into(), "west".into()),
        (replicas[1].into(), "west".into()),
        (replicas[2].into(), "east".into()),
    ]);
    let a = node(replicas[0], &replicas, regions.clone()).await;
    let b = node(replicas[1], &replicas, regions.clone()).await;
    let c = node(replicas[2], &replicas, regions).await;
    b.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 1 && b.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Count(1))
        .require_other_region(true)
        .timeout(Duration::from_millis(300))
        .build();
    let Err(WriteConcernError::TimedOut(receipt)) = a
        .put_with_write_concern("regional-timeout".into(), vec![1], options)
        .await
    else {
        panic!("a same-region peer must not satisfy the region requirement");
    };
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert_eq!(receipt.receiving_region(), Some("west"));
    assert!(!receipt.other_region_satisfied());
    assert!(a.get("regional-timeout".into()).await.unwrap().is_some());
    common::wait_for(
        || async { b.get("regional-timeout".into()).await.unwrap().is_some() },
        Duration::from_secs(5),
    )
    .await;

    c.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async { a.peer_count().await == 2 && c.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    let options = WriteConcernOptions::default().with_require_other_region(true);
    let receipt = a
        .put_with_write_concern("regional-success".into(), vec![2], options.clone())
        .await
        .unwrap();
    assert_eq!(receipt.required_acknowledgements(), 1);
    assert_eq!(receipt.outside_region_acknowledged_nodes(), &["region-c"]);
    let receipt = a
        .delete_with_write_concern("regional-success".into(), options)
        .await
        .unwrap();
    assert!(receipt.other_region_satisfied());
    assert!(c.get("regional-success".into()).await.unwrap().is_none());
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn routed_write_uses_serving_receiver_region_and_returns_region_receipt() {
    let replicas = ["routed-region-a", "routed-region-b"];
    let regions = HashMap::from([
        (replicas[0].into(), "west".into()),
        (replicas[1].into(), "east".into()),
        ("routed-region-origin".into(), "east".into()),
    ]);
    let a = node(replicas[0], &replicas, regions.clone()).await;
    let b = node(replicas[1], &replicas, regions.clone()).await;
    let origin = node("routed-region-origin", &replicas, regions).await;
    b.connect(a.addr()).await.unwrap();
    origin.connect(a.addr()).await.unwrap();
    common::wait_for(
        || async {
            a.peer_count().await == 2 && b.peer_count().await == 1 && origin.peer_count().await == 1
        },
        Duration::from_secs(5),
    )
    .await;
    let receipt = origin
        .put_with_write_concern(
            "routed-region".into(),
            vec![3],
            WriteConcernOptions::default().with_require_other_region(true),
        )
        .await
        .unwrap();
    assert_eq!(receipt.receiving_node(), replicas[0]);
    assert_eq!(receipt.receiving_region(), Some("west"));
    assert_eq!(receipt.outside_region_acknowledged_nodes(), &[replicas[1]]);
    assert_eq!(
        b.get("routed-region".into()).await.unwrap().unwrap().value,
        vec![3]
    );
    origin.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
