//! Public bounded metrics and node observation contracts.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::node::drain::{DrainOptions, DrainPhase};
use actordb::node::metrics::{MetricKind, MetricScope, MetricValue};
use actordb::node::status::{Readiness, ReplicationPurpose, SnapshotLimits};
use actordb::node::{Node, NodeConfig, NodeError, ReplicationDrainOptions};
use actordb::placement::{FragmentPlacementState, ManualPlacement};
use actordb::shard::FragmentId;
use actordb::ttl_expiry::{expiry_handler_fn, TtlExpiryOptions};

fn one_fragment() -> NodeConfig {
    NodeConfig::builder().fragment_count(1).build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collectors_reenter_during_writes_and_observe_drain_and_stop() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let node = Arc::new(
            Node::start(
                "metrics".into(),
                "127.0.0.1:0".parse().unwrap(),
                one_fragment(),
            )
            .await
            .unwrap(),
        );
        let written = Arc::new(AtomicUsize::new(0));
        let writer_node = node.clone();
        let writer_count = written.clone();
        let writer = tokio::spawn(async move {
            for index in 0..200 {
                match writer_node
                    .put(format!("private-key-{index}"), vec![0xff; 1024])
                    .await
                {
                    Ok(()) => {
                        writer_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(NodeError::ShuttingDown) => break,
                    Err(error) => panic!("unexpected write failure: {error}"),
                }
                tokio::task::yield_now().await;
            }
        });
        common::wait_for(
            || async { written.load(Ordering::Relaxed) > 0 },
            Duration::from_secs(5),
        )
        .await;
        for _ in 0..20 {
            let calls = AtomicUsize::new(0);
            let snapshot = node.collect_metrics(&|point: &actordb::node::metrics::MetricPoint| {
                // This read uses the same placement and sender locks as collection.
                // Successful complete placement demonstrates callbacks own no locks.
                assert!(!node.status_snapshot().observation.placement_unavailable);
                let _ = node.embedded_operation_diagnostics();
                assert_eq!(point.descriptor.kind, point.value.kind());
                match &point.scope {
                    MetricScope::Node | MetricScope::Fragment { .. } => {}
                    MetricScope::FragmentPeer { peer_id, .. } => {
                        assert!(!peer_id.contains("private-key"))
                    }
                    _ => panic!("unexpected scope"),
                }
                calls.fetch_add(1, Ordering::Relaxed);
            });
            assert_eq!(calls.load(Ordering::Relaxed), snapshot.points.len());
            assert_eq!(snapshot.status.readiness, Readiness::Ready);
            assert!(snapshot
                .points
                .iter()
                .all(|point| !point.descriptor.name.contains("private-key")));
            tokio::task::yield_now().await;
        }
        let beginning = node.begin_drain();
        let draining = node.metrics_snapshot();
        assert_eq!(draining.status.drain.phase(), beginning.phase());
        assert_eq!(draining.status.readiness, Readiness::NotReady);
        let report = node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(report.status().phase(), DrainPhase::Drained);
        let drained = node.metrics_snapshot();
        assert_eq!(drained.status.drain.phase(), report.status().phase());
        assert_eq!(drained.status.drain.active_requests(), 0);
        assert_eq!(drained.status.readiness, Readiness::NotReady);
        assert!(matches!(
            node.put("closed".into(), vec![]).await,
            Err(NodeError::ShuttingDown)
        ));
        writer.await.unwrap();
        let stopped_report = node.finish_shutdown(DrainOptions::default()).await.unwrap();
        let stopped = node.collect_metrics(&|_: &actordb::node::metrics::MetricPoint| {
            assert_eq!(node.drain_status().phase(), DrainPhase::Stopped);
        });
        assert_eq!(stopped.status.drain, stopped_report.status());
        assert!(stopped.status.drain.receive_apply_complete());
        assert_eq!(stopped.status.readiness, Readiness::NotReady);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_partial_snapshots_preserve_known_readiness_without_inventing_missing_capacity() {
    let config = NodeConfig::builder().fragment_count(2).build();
    let node = Node::start("bounded".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let snapshot = node.metrics_snapshot_with_limits(SnapshotLimits::new(1, 0));
    assert_eq!(snapshot.status.fragments.len(), 1);
    // The declared initial boundary was established before collection. One
    // observed running authoritative route proves availability even though
    // this output limit cannot report every fragment or exact live capacity.
    assert_eq!(snapshot.status.readiness, Readiness::Ready);
    assert!(snapshot.status.initial_readiness_established);
    assert_eq!(snapshot.status.available_fragments, None);
    assert!(snapshot.status.observation.fragments_truncated);
    assert!(!snapshot.status.observation.is_complete());
    assert!(snapshot
        .points
        .iter()
        .any(|point| point.descriptor.name == "actordb.node.ready"
            && point.value == MetricValue::Gauge(1)));
    assert!(!snapshot
        .points
        .iter()
        .any(|point| point.descriptor.name == "actordb.lifecycle.available_fragments"));
    // With no route observation, availability is genuinely unknown; it must
    // not be replaced with a ready=false/zero capacity measurement.
    let unobserved = node.metrics_snapshot_with_limits(SnapshotLimits::new(0, 0));
    assert_eq!(unobserved.status.readiness, Readiness::Unknown);
    assert!(unobserved.status.fragments.is_empty());
    assert!(!unobserved
        .points
        .iter()
        .any(|point| point.descriptor.name == "actordb.node.ready"
            || point.descriptor.name == "actordb.lifecycle.available_fragments"));
    assert!(!snapshot
        .points
        .iter()
        .any(|point| point.descriptor.name.starts_with("actordb.expiry.")));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_catchup_is_not_background_replication_and_progress_is_unknown() {
    let placement = HashMap::from([(
        FragmentId(0),
        vec!["catching-up".into(), "existing-owner".into()],
    )]);
    let controller = Arc::new(ManualPlacement::new(placement));
    let config = NodeConfig::builder()
        .fragment_count(1)
        .placement_controller(controller)
        .placement_poll_interval(Duration::from_millis(10))
        .build();
    let node = Node::start("catching-up".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    common::wait_for(
        || async {
            let status = node.status_snapshot();
            status.fragments.first().is_some_and(|fragment| {
                fragment.placement_state == Some(FragmentPlacementState::Bootstrapping)
            }) && status
                .replication
                .iter()
                .any(|sender| sender.purpose == ReplicationPurpose::InitialCatchUp)
        },
        Duration::from_secs(5),
    )
    .await;
    let status = node.status_snapshot();
    assert_eq!(status.readiness, Readiness::NotReady);
    assert!(status.fragments.iter().all(|fragment| !fragment.serving));
    for sender in &status.replication {
        assert_eq!(sender.purpose, ReplicationPurpose::InitialCatchUp);
        assert!(sender.last_ack_durability.is_none());
        let original = node
            .sender_report(sender.fragment, &sender.peer_id)
            .await
            .unwrap();
        assert_eq!(sender.completed_reconcile_rounds, original.reconcile_rounds);
        assert_eq!(sender.buffered_operations, original.buffered);
    }
    node.shutdown().await.unwrap();

    let map = HashMap::from([(FragmentId(0), vec!["serving".into(), "offline-peer".into()])]);
    let node = Node::start(
        "serving".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .fragment_map(map)
            .build(),
    )
    .await
    .unwrap();
    common::wait_for(
        || async {
            node.status_snapshot()
                .replication
                .iter()
                .any(|sender| sender.purpose == ReplicationPurpose::Background)
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(node.status_snapshot().readiness, Readiness::Ready);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn placement_generation_and_completed_replication_fence_match_reports() {
    let map = HashMap::from([(FragmentId(0), vec!["generation".into()])]);
    let controller = Arc::new(ManualPlacement::new(map));
    let config = NodeConfig::builder()
        .fragment_count(1)
        .placement_controller(controller.clone())
        .placement_poll_interval(Duration::from_millis(10))
        .build();
    let node = Node::start("generation".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    common::wait_for(
        || async { node.status_snapshot().readiness == Readiness::Ready },
        Duration::from_secs(5),
    )
    .await;
    let before = node.status_snapshot().placement_generation.unwrap();
    controller.set_placement(HashMap::from([(
        FragmentId(0),
        vec!["generation".into(), "new-replica".into()],
    )]));
    common::wait_for(
        || async {
            node.status_snapshot()
                .placement_generation
                .is_some_and(|generation| generation > before)
        },
        Duration::from_secs(5),
    )
    .await;
    let status = node.status_snapshot();
    let report = node.placement_report().await;
    assert_eq!(
        status.fragments[0].replicas,
        report.placement[&FragmentId(0)]
    );
    assert_eq!(
        status.fragments[0].placement_state,
        report.states.get(&FragmentId(0)).copied()
    );
    node.shutdown().await.unwrap();

    // A node with no remote targets still returns exact completed fence evidence.
    let node = Node::start(
        "fence".into(),
        "127.0.0.1:0".parse().unwrap(),
        one_fragment(),
    )
    .await
    .unwrap();
    node.put("record".into(), vec![1]).await.unwrap();
    let report = node
        .shutdown_with_replication_drain(ReplicationDrainOptions::default())
        .await
        .unwrap();
    assert_eq!(
        node.status_snapshot().completed_replication_drain.as_ref(),
        Some(&report)
    );

    let replicas = HashMap::from([(
        FragmentId(0),
        vec!["fence-source".into(), "fence-replica".into()],
    )]);
    let config = NodeConfig::builder()
        .fragment_count(1)
        .fragment_map(replicas)
        .build();
    let source = Node::start(
        "fence-source".into(),
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
    )
    .await
    .unwrap();
    let replica = Node::start(
        "fence-replica".into(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    source.connect(replica.addr()).await.unwrap();
    common::wait_for(
        || async { source.peer_count().await == 1 && replica.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    source.put("fenced".into(), vec![7]).await.unwrap();
    let report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(report.targets().len(), 1);
    assert_eq!(report.targets()[0].peer_id(), replica.id());
    assert_eq!(
        source
            .status_snapshot()
            .completed_replication_drain
            .as_ref(),
        Some(&report)
    );
    let capped = source.status_snapshot_with_limits(SnapshotLimits::new(1, 0));
    assert!(capped.completed_replication_drain.is_none());
    assert!(capped.observation.drain_report_truncated);
    assert_eq!(
        replica.get("fenced".into()).await.unwrap().unwrap().value,
        vec![7]
    );
    replica.shutdown().await.unwrap();
}

#[tokio::test]
async fn cumulative_counters_and_distribution_summaries_are_not_reset_by_collection() {
    let config = NodeConfig::builder()
        .fragment_count(1)
        .ttl_expiry_handler(Arc::new(expiry_handler_fn(|_| async { Ok(()) })))
        .ttl_expiry_options(
            TtlExpiryOptions::builder()
                .max_batch_delay(Duration::from_millis(1))
                .build(),
        )
        .clean_interval(Duration::from_millis(5))
        .build();
    let node = Node::start("ttl-metrics".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    node.put_with_ttl("expiring".into(), vec![42], Some(Duration::from_millis(10)))
        .await
        .unwrap();
    common::wait_for(
        || async { node.ttl_expiry_diagnostics().unwrap().delivered_events > 0 },
        Duration::from_secs(5),
    )
    .await;
    node.shutdown().await.unwrap();
    let first = node.metrics_snapshot();
    let second = node.metrics_snapshot();
    for point in first
        .points
        .iter()
        .filter(|point| point.descriptor.kind != MetricKind::Gauge)
    {
        let next = second
            .points
            .iter()
            .find(|next| next.descriptor == point.descriptor && next.scope == point.scope)
            .unwrap();
        assert_eq!(point.value, next.value);
        if let MetricValue::Distribution(summary) = point.value {
            if point.descriptor.name.starts_with("actordb.expiry.") {
                assert!(
                    summary.count > 0,
                    "the expiry workflow must produce its measured latency samples"
                );
            } else if summary.count == 0 {
                assert_eq!((summary.sum, summary.max), (0, 0));
            }
            assert!(summary.sum >= summary.max);
        }
    }
    assert_eq!(first.status.boot_id, second.status.boot_id);
    assert!(!first.status.request_timing_enabled);
    assert!(!first.status.storage.timing_enabled);
    for summary in [
        first.status.request_duration,
        first.status.storage.queue_wait,
        first.status.storage.execution_time,
    ] {
        assert_eq!(
            (summary.count, summary.sum_nanos, summary.max_nanos),
            (0, 0, 0)
        );
    }
    for name in [
        "actordb.request.owned_duration",
        "actordb.storage.queue_wait",
        "actordb.storage.execution_time",
    ] {
        assert!(
            !first
                .points
                .iter()
                .any(|point| point.descriptor.name == name),
            "disabled optional timing must not be exported as an observed measurement"
        );
    }
    assert!(first.points.iter().any(|point| point.descriptor.name
        == "actordb.expiry.delivered_events"
        && point.value == MetricValue::Counter(1)));
}
