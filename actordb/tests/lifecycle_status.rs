mod common;
use actordb::actor::VersionedValue;
use actordb::node::status::{FragmentActivity, NodeLifecycle, Readiness};
use actordb::node::{Node, NodeConfig, StartupStage};
use actordb::placement::{FragmentPlacementState, ManualPlacement};
use actordb::shard::{fragment_of, FragmentId};
use actordb::store::{KvStore, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
#[tokio::test]
async fn replication_reports_real_success_timestamps_and_enabled_queue_ages() {
    let config = NodeConfig::builder()
        .fragment_map(HashMap::from([(
            FragmentId(0),
            vec!["ack-source".into(), "ack-peer".into()],
        )]))
        .build();
    let source = Node::start(
        "ack-source".into(),
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
    )
    .await
    .unwrap();
    let peer = Node::start("ack-peer".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    source.enable_metrics();
    source.connect(peer.addr()).await.unwrap();
    source.put("observed-ack".into(), vec![5]).await.unwrap();
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), "ack-peer")
                .await
                .is_some_and(|report| {
                    report.progress.last_successful_ack_at.is_some()
                        && report.progress.last_successful_reconcile_at.is_some()
                        && report.buffered == 0
                        && report.in_flight == 0
                })
        },
        Duration::from_secs(3),
    )
    .await;
    let report = source
        .sender_report(FragmentId(0), "ack-peer")
        .await
        .unwrap();
    assert_eq!(
        report.progress.last_successful_ack_durability,
        Some(actordb::replication::ReplicationAckDurability::MemoryApplied)
    );
    assert!(report.progress.incarnation != 0);
    assert!(report.progress.observed_at.is_some());
    assert!(report.progress.timing_enabled);
    assert_eq!(
        report.progress.oldest_retained_operation_age,
        Some(Duration::ZERO)
    );
    assert_eq!(
        peer.get("observed-ack".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![5]
    );
    let snapshot = source.status_snapshot();
    assert_eq!(snapshot.lifecycle, NodeLifecycle::Serving);
    assert!(snapshot.replication[0].lag.is_none());
    source.shutdown().await.unwrap();
    peer.shutdown().await.unwrap();
}

struct LoadingStore {
    entered: Notify,
    released: (Mutex<bool>, Condvar),
    detached: Arc<AtomicBool>,
}
impl LoadingStore {
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
impl KvStore for LoadingStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        let detached = self.detached.clone();
        Ok(StoreBinding::managed(move || {
            detached.store(true, Ordering::SeqCst);
        }))
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.entered.notify_one();
        let mut released = self.released.0.lock().unwrap();
        while !*released {
            released = self.released.1.wait(released).unwrap();
        }
        Ok(Vec::new())
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Stable)
    }
}
struct Release(Arc<LoadingStore>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[tokio::test]
async fn explicit_recovery_reports_missing_candidates_without_premature_serving() {
    let store = Arc::new(LoadingStore {
        entered: Notify::new(),
        released: (Mutex::new(true), Condvar::new()),
        detached: Arc::new(AtomicBool::new(false)),
    });
    let controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["recovering".into(), "missing".into()],
    )])));
    let node = Node::start(
        "recovering".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .store(store)
            .placement_controller(controller)
            .recovery(actordb::node::RecoveryOptions::new([1; 32]).unwrap())
            .build(),
    )
    .await
    .unwrap();
    let status = node.status_snapshot();
    assert_eq!(status.lifecycle, NodeLifecycle::Recovering);
    assert_eq!(status.readiness, Readiness::NotReady);
    assert!(!status.initial_readiness_established);
    assert_eq!(status.fragments[0].missing_peers, &["missing"]);
    node.shutdown().await.unwrap();
}

struct ShutdownStore {
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
impl ShutdownStore {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
impl KvStore for ShutdownStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.entered.notify_one();
        let mut released = self.release.0.lock().unwrap();
        while !*released {
            released = self.release.1.wait(released).unwrap();
        }
        Ok(())
    }
}
struct ReleaseShutdown(Arc<ShutdownStore>);
impl Drop for ReleaseShutdown {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[tokio::test]
async fn active_shutdown_stage_remains_observable_through_lock_ownership_and_cancellation() {
    let store = Arc::new(ShutdownStore {
        entered: Notify::new(),
        release: (Mutex::new(false), Condvar::new()),
    });
    let _release = ReleaseShutdown(store.clone());
    let node = Arc::new(
        Node::start(
            "shutdown-stage".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .store(store.clone())
                .flush_batch_size(10_000)
                .flush_interval(Duration::from_secs(3600))
                .snapshot_interval(Duration::from_secs(3600))
                .build(),
        )
        .await
        .unwrap(),
    );
    node.put("pending".into(), vec![1]).await.unwrap();
    let stopping = node.clone();
    let shutdown = tokio::spawn(async move { stopping.shutdown().await });
    tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
        .await
        .unwrap();
    let status = node.status_snapshot();
    assert_eq!(status.lifecycle, NodeLifecycle::Stopping);
    assert_eq!(
        status.shutdown_stage,
        actordb::node::status::ShutdownStage::QuiescingFragments
    );
    assert!(status.storage.active_jobs > 0);
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    assert_eq!(node.status_snapshot().shutdown_stage, status.shutdown_stage);
    store.release();
    node.shutdown().await.unwrap();
    assert_eq!(
        node.status_snapshot().shutdown_stage,
        actordb::node::status::ShutdownStage::Stopped
    );
}

#[tokio::test]
async fn owned_startup_reports_load_and_keeps_cleanup_owned_after_cancelled_waiter() {
    let store = Arc::new(LoadingStore {
        entered: Notify::new(),
        released: (Mutex::new(false), Condvar::new()),
        detached: Arc::new(AtomicBool::new(false)),
    });
    let _release = Release(store.clone());
    let mut startup = Node::begin_start(
        "startup-status".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder().store(store.clone()).build(),
    );
    assert_eq!(startup.snapshot().lifecycle, NodeLifecycle::Starting);
    tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
        .await
        .unwrap();
    let snapshot = startup.snapshot();
    assert_eq!(snapshot.stage, StartupStage::InitializingFragments);
    assert_eq!(snapshot.lifecycle, NodeLifecycle::Initializing);
    assert_eq!(snapshot.initialized_fragments, 0);
    assert_eq!(snapshot.required_fragments, 1);
    assert!(!snapshot.initial_readiness_established);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), startup.wait())
            .await
            .is_err()
    );
    startup.cancel();
    assert_eq!(startup.snapshot().lifecycle, NodeLifecycle::Draining);
    assert!(!store.detached.load(Ordering::SeqCst));
    store.release();
    assert!(tokio::time::timeout(Duration::from_secs(3), startup.wait())
        .await
        .unwrap()
        .is_err());
    let snapshot = startup.snapshot();
    assert_eq!(snapshot.stage, StartupStage::Cancelled);
    assert!(snapshot.cleanup_complete);
    assert!(store.detached.load(Ordering::SeqCst));
}

#[tokio::test]
async fn startup_validation_failure_is_observable_and_cancel_after_handoff_is_inert() {
    let mut failed = Node::begin_start(
        "failed-startup".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder().fragment_count(0).build(),
    );
    assert!(failed.wait().await.is_err());
    let snapshot = failed.snapshot();
    assert_eq!(snapshot.lifecycle, NodeLifecycle::Failed);
    assert!(snapshot.failure.is_some());
    assert!(snapshot.cleanup_complete);
    let mut startup = Node::begin_start(
        "handoff".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    );
    let node = startup.wait().await.unwrap();
    startup.cancel();
    assert!(!startup.snapshot().cancellation_requested);
    assert_eq!(node.status_snapshot().lifecycle, NodeLifecycle::Serving);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn established_serving_boundary_survives_background_gain_and_fresh_learner_stays_offline() {
    let controller = Arc::new(ManualPlacement::new(HashMap::from([
        (FragmentId(0), vec!["established".into()]),
        (FragmentId(1), vec!["offline".into()]),
    ])));
    let node = Node::start(
        "established".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(2)
            .placement_controller(controller.clone())
            .placement_poll_interval(Duration::from_millis(20))
            .build(),
    )
    .await
    .unwrap();
    // Authority events establish the boundary even if nobody collected status.
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
        },
        Duration::from_secs(3),
    )
    .await;
    controller.set_placement(HashMap::from([
        (FragmentId(0), vec!["established".into()]),
        (FragmentId(1), vec!["established".into(), "offline".into()]),
    ]));
    common::wait_for(
        || async {
            node.placement_report().await.states.get(&FragmentId(1))
                == Some(&FragmentPlacementState::Bootstrapping)
        },
        Duration::from_secs(3),
    )
    .await;
    let status = node.status_snapshot();
    assert!(status.initial_readiness_established);
    assert_eq!(status.readiness, Readiness::Ready);
    assert_eq!(status.lifecycle, NodeLifecycle::Serving);
    assert!(status.capacity_partial);
    assert_eq!(status.desired_local_fragments, 2);
    assert_eq!(status.available_fragments, Some(1));
    assert!(!status.fragments[1].serving);
    assert!(matches!(
        status.fragments[1].activity,
        FragmentActivity::Acquiring | FragmentActivity::Stalled
    ));
    let key = (0..100)
        .map(|index| format!("key-{index}"))
        .find(|key| fragment_of(key, 2) == FragmentId(0))
        .unwrap();
    node.put(key.clone(), vec![7]).await.unwrap();
    assert_eq!(node.get(key).await.unwrap().unwrap().value, vec![7]);
    let learner_controller = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["learner".into(), "offline".into()],
    )])));
    let learner = Node::start(
        "learner".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .placement_controller(learner_controller)
            .build(),
    )
    .await
    .unwrap();
    let status = learner.status_snapshot();
    assert!(!status.initial_readiness_established);
    assert_eq!(status.readiness, Readiness::NotReady);
    assert_eq!(status.lifecycle, NodeLifecycle::Initializing);
    assert!(!status.fragments[0].serving);
    assert_eq!(status.fragments[0].missing_peers, &["offline"]);
    learner.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
