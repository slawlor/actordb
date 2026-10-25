mod common;
#[path = "common/evidence_store.rs"]
mod evidence_store;
use actordb::node::{DrainError, DrainOptions, Node, NodeConfig, NodeRuntimeOptions};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb::write_concern::{
    WriteConcern, WriteConcernError, WriteConcernOptions, WriteDurability,
};
use evidence_store::{EvidenceStore, ReleaseGate, StableGate};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn config(store: Arc<EvidenceStore>, replicas: Vec<String>) -> NodeConfig {
    NodeConfig::builder()
        .store(store)
        .fragment_map(HashMap::from([(FragmentId(0), replicas)]))
        .flush_batch_size(10_000)
        .flush_interval(Duration::from_secs(3600))
        .snapshot_interval(Duration::from_secs(3600))
        .replication(
            SenderConfig::builder()
                .batch_size(1)
                .batch_interval(Duration::from_millis(10))
                .build(),
        )
        .build()
}
fn policy(durability: WriteDurability, concern: WriteConcern) -> WriteConcernOptions {
    WriteConcernOptions::builder()
        .concern(concern)
        .minimum_durability(durability)
        .require_evidence(true)
        .timeout(Duration::from_secs(2))
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_stable_floor_survives_sender_buffer_reset_and_session_recovery() {
    let local_store = EvidenceStore::new(true);
    let remote_store = EvidenceStore::new(true);
    let replicas = vec![
        "reset-local".into(),
        "reset-remote".into(),
        "offline".into(),
    ];
    let mut local_config = config(local_store, replicas.clone());
    local_config.replication = SenderConfig::builder()
        .batch_size(1)
        .buffer_capacity(1)
        .max_in_flight(1)
        .batch_interval(Duration::from_millis(5))
        .build();
    let local = Arc::new(
        Node::start(
            "reset-local".into(),
            "127.0.0.1:0".parse().unwrap(),
            local_config,
        )
        .await
        .unwrap(),
    );
    let remote = Node::start(
        "reset-remote".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(remote_store.clone(), replicas),
    )
    .await
    .unwrap();
    local.connect(remote.addr()).await.unwrap();
    common::wait_for(
        || async {
            local
                .sender_report(FragmentId(0), "reset-remote")
                .await
                .is_some_and(|report| report.state == "connected")
        },
        Duration::from_secs(3),
    )
    .await;
    let gate = StableGate::new();
    let _release = ReleaseGate(gate.clone());
    *remote_store.gate.lock().unwrap() = Some(gate.clone());
    let writer_node = local.clone();
    let writer = tokio::spawn(async move {
        writer_node
            .put_with_write_concern(
                "floor".into(),
                vec![5],
                WriteConcernOptions::builder()
                    .concern(WriteConcern::Majority)
                    .minimum_durability(WriteDurability::StableCommitted)
                    .timeout(Duration::from_secs(5))
                    .build(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    for index in 0..12 {
        local.put(format!("noise-{index}"), vec![1]).await.unwrap();
    }
    common::wait_for(
        || async {
            local
                .sender_report(FragmentId(0), "reset-remote")
                .await
                .is_some_and(|report| report.state == "buffer_reset")
        },
        Duration::from_secs(3),
    )
    .await;
    assert!(!writer.is_finished());
    gate.release();
    let receipt = tokio::time::timeout(Duration::from_secs(6), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.evidence().unwrap().durability_at("reset-remote"),
        Some(WriteDurability::StableCommitted)
    );
    assert_eq!(
        remote.get("floor".into()).await.unwrap().unwrap().value,
        vec![5]
    );
    local.shutdown().await.unwrap();
    remote.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_evidence_distinguishes_enqueue_buffered_commit_and_explicit_stable_fence() {
    let store = EvidenceStore::new(true);
    let node = Node::start(
        "local".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(store.clone(), vec!["local".into()]),
    )
    .await
    .unwrap();
    for (key, durability) in [
        ("memory", WriteDurability::MemoryApplied),
        ("buffered", WriteDurability::StoreCommitted),
        ("stable", WriteDurability::StableCommitted),
    ] {
        let receipt = node
            .put_with_write_concern(key.into(), vec![7], policy(durability, WriteConcern::Local))
            .await
            .unwrap();
        let evidence = receipt.evidence().unwrap();
        assert_eq!(evidence.durability_at("local"), Some(durability));
        evidence
            .operation()
            .validate_application_write("KV", key, &[7], false)
            .unwrap();
        let value = node.get(key.into()).await.unwrap().unwrap();
        assert_eq!(
            evidence.operation().version().timestamp_ms,
            value.timestamp_ms
        );
        assert_eq!(
            evidence.operation().version().logical_counter,
            value.logical_counter
        );
        assert_eq!(evidence.eligible_nodes(), &["local"]);
        assert_eq!(evidence.replica_set_digest().len(), 32);
        if durability == WriteDurability::MemoryApplied {
            assert_eq!(store.commits.load(Ordering::SeqCst), 0);
        }
        if durability == WriteDurability::StoreCommitted {
            assert!(store.commits.load(Ordering::SeqCst) > 0);
            assert_eq!(store.fences.load(Ordering::SeqCst), 0);
        }
    }
    assert!(store.fences.load(Ordering::SeqCst) > 0);
    assert_eq!(node.write_concern_diagnostics().satisfied, 3);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn buffered_only_or_failed_local_fence_never_claims_stable_and_preserves_applied_progress() {
    let store = EvidenceStore::new(false);
    let node = Node::start(
        "failure".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(store.clone(), vec!["failure".into()]),
    )
    .await
    .unwrap();
    let stable = policy(WriteDurability::StableCommitted, WriteConcern::Local);
    let Err(WriteConcernError::DurabilityFailed { receipt, .. }) = node
        .put_with_write_concern("buffered-only".into(), vec![1], stable.clone())
        .await
    else {
        panic!("buffered store cannot certify stable completion")
    };
    assert_eq!(
        receipt.evidence().unwrap().durability_at("failure"),
        Some(WriteDurability::StoreCommitted)
    );
    assert_eq!(
        node.get("buffered-only".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![1]
    );

    store.stable.store(true, Ordering::SeqCst);
    for (key, write_failure) in [("commit-failed", true), ("fence-failed", false)] {
        store.fail_write.store(write_failure, Ordering::SeqCst);
        store.fail_fence.store(!write_failure, Ordering::SeqCst);
        let Err(WriteConcernError::DurabilityFailed { receipt, .. }) = node
            .put_with_write_concern(key.into(), vec![2], stable.clone())
            .await
        else {
            panic!("local persistence failure must preserve an applied receipt")
        };
        assert_eq!(
            receipt.evidence().unwrap().durability_at("failure"),
            Some(WriteDurability::MemoryApplied)
        );
        assert_eq!(node.get(key.into()).await.unwrap().unwrap().value, vec![2]);
    }
    store.fail_write.store(false, Ordering::SeqCst);
    store.fail_fence.store(false, Ordering::SeqCst);
    node.put_with_write_concern("retry".into(), vec![3], stable)
        .await
        .unwrap();
    assert_eq!(node.write_concern_diagnostics().durability_failed, 3);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_buffered_ack_cannot_satisfy_stable_request_then_actual_stable_ack_succeeds() {
    let local_store = EvidenceStore::new(true);
    let remote_store = EvidenceStore::new(false);
    let replicas = vec![
        "local-stable".into(),
        "remote-buffered".into(),
        "offline".into(),
    ];
    let local = Node::start(
        "local-stable".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(local_store, replicas.clone()),
    )
    .await
    .unwrap();
    let remote = Node::start(
        "remote-buffered".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(remote_store.clone(), replicas),
    )
    .await
    .unwrap();
    local.connect(remote.addr()).await.unwrap();
    common::wait_for(
        || async { local.peer_count().await == 1 && remote.peer_count().await == 1 },
        Duration::from_secs(3),
    )
    .await;
    let stable = policy(WriteDurability::StableCommitted, WriteConcern::Majority)
        .with_strict_acknowledgements(true);
    let Err(WriteConcernError::TimedOut(receipt)) = local
        .put_with_write_concern("cannot-stabilize".into(), vec![1], stable.clone())
        .await
    else {
        panic!("buffered remote must not satisfy requested stable threshold")
    };
    assert_eq!(receipt.acknowledged_nodes(), &["local-stable"]);
    assert_eq!(
        receipt.evidence().unwrap().durability_at("local-stable"),
        Some(WriteDurability::StableCommitted)
    );
    assert!(remote
        .get("cannot-stabilize".into())
        .await
        .unwrap()
        .is_some());
    remote_store.stable.store(true, Ordering::SeqCst);
    let receipt = local
        .put_with_write_concern("can-stabilize".into(), vec![2], stable)
        .await
        .unwrap();
    assert_eq!(
        receipt.evidence().unwrap().durability_at("remote-buffered"),
        Some(WriteDurability::StableCommitted)
    );
    assert_eq!(receipt.required_acknowledgements(), 2);
    local.shutdown().await.unwrap();
    remote.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_caller_deadline_keeps_actual_stable_fence_owned_until_drain() {
    let store = EvidenceStore::new(true);
    let gate = StableGate::new();
    let _release = ReleaseGate(gate.clone());
    *store.gate.lock().unwrap() = Some(gate.clone());
    let node = Arc::new(
        Node::start_with_options(
            "held-fence".into(),
            "127.0.0.1:0".parse().unwrap(),
            config(store.clone(), vec!["held-fence".into()]),
            NodeRuntimeOptions::default().with_local_work_timeout(Duration::from_millis(25)),
        )
        .await
        .unwrap(),
    );
    let writing = node.clone();
    let writer = tokio::spawn(async move {
        writing
            .put_with_write_concern(
                "held".into(),
                vec![9],
                WriteConcernOptions::builder()
                    .minimum_durability(WriteDurability::StableCommitted)
                    .timeout(Duration::from_millis(10))
                    .build(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    let Err(WriteConcernError::UnknownOutcome(_)) = writer.await.unwrap() else {
        panic!("lost caller deadline cannot certify unfinished fence")
    };
    assert!(node.drain_status().active_requests() > 0);
    assert!(matches!(
        node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
            .await,
        Err(DrainError::Timeout { .. })
    ));
    gate.release();
    node.drain_requests(DrainOptions::default()).await.unwrap();
    assert_eq!(node.drain_status().active_requests(), 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_stable_write_waiter_leaves_successful_completion_and_fence_owned() {
    let store = EvidenceStore::new(true);
    let gate = StableGate::new();
    let _release = ReleaseGate(gate.clone());
    *store.gate.lock().unwrap() = Some(gate.clone());
    let node = Arc::new(
        Node::start(
            "cancel-fence".into(),
            "127.0.0.1:0".parse().unwrap(),
            config(store, vec!["cancel-fence".into()]),
        )
        .await
        .unwrap(),
    );
    let writing = node.clone();
    let writer = tokio::spawn(async move {
        writing
            .put_with_write_concern(
                "held".into(),
                vec![9],
                policy(WriteDurability::StableCommitted, WriteConcern::Local),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert!(matches!(
        node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
            .await,
        Err(DrainError::Timeout { .. })
    ));
    gate.release();
    node.drain_requests(DrainOptions::default()).await.unwrap();
    let diagnostics = node.write_concern_diagnostics();
    assert_eq!(diagnostics.active, 0);
    assert_eq!(diagnostics.satisfied, 1);
    assert_eq!(diagnostics.cancelled, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn persistence_error_during_apply_preserves_uncertainty_instead_of_fabricating_a_receipt() {
    let store = EvidenceStore::new(true);
    let mut config = config(store.clone(), vec!["apply-failed".into()]);
    config.flush_batch_size = 1;
    let node = Node::start(
        "apply-failed".into(),
        "127.0.0.1:0".parse().unwrap(),
        config,
    )
    .await
    .unwrap();
    store.fail_write.store(true, Ordering::SeqCst);
    assert!(matches!(
        node.put_with_write_concern(
            "uncertain".into(),
            vec![8],
            policy(WriteDurability::StableCommitted, WriteConcern::Local)
        )
        .await,
        Err(WriteConcernError::UnknownOutcome(_))
    ));
    assert_eq!(
        node.get("uncertain".into()).await.unwrap().unwrap().value,
        vec![8]
    );
    assert_eq!(node.write_concern_diagnostics().unknown_outcomes, 1);
    store.fail_write.store(false, Ordering::SeqCst);
    node.shutdown().await.unwrap();
}
