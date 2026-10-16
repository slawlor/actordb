// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Startup rollback and authoritative post-quiescence flush ordering.

use super::*;

struct DropSignalStore(Option<std::sync::mpsc::Sender<()>>);

impl KvStore for DropSignalStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
}

impl Drop for DropSignalStore {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.take() {
            let _ = dropped.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_cleaner_stops_pages_on_drain_but_final_quiesce_sweeps_all() {
    let store = Arc::new(GatedStore::default());
    let _release = ReleaseMaintenanceStore(store.clone());
    let values = (0..600)
        .map(|i| {
            let mut value = VersionedValue::new(vec![1], 10, "writer".into());
            value.expires_at_ms = Some(1);
            (format!("k{i:04}"), value)
        })
        .collect();
    let shared = Arc::new(FragmentState::new(
        values,
        Some(store.clone()),
        1,
        Duration::from_secs(3600),
    ));
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };
    let args = SupervisorArgs {
        fragment: FragmentId(0),
        worker_prefix: "paged-final-quiesce".into(),
        store: Some(store.clone()),
        hlc: Arc::new(Mutex::new(HlcClock::new())),
        default_pool: pool.clone(),
        high_pool: pool.clone(),
        background_pool: pool.clone(),
        flush_batch_size: 1,
        flush_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_millis(2),
        snapshot_interval: Duration::from_secs(3600),
        tombstone_gc_age: None,
        reset_senders: Arc::new(Mutex::new(HashMap::new())),
    };
    let (dispatcher, _) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared: shared.clone(),
            fragment: args.fragment,
            worker_prefix: args.worker_prefix.clone(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let (cleaner, _) = Cleaner::spawn(
        None,
        Cleaner,
        CleanerArgs {
            fragment: args.fragment,
            shared: shared.clone(),
            interval: args.clean_interval,
            tombstone_gc_age: None,
            reset_senders: args.reset_senders.clone(),
        },
    )
    .await
    .unwrap();
    let (snapshotter, _) = Snapshotter::spawn(
        None,
        Snapshotter,
        SnapshotterArgs {
            shared: shared.clone(),
            interval: args.snapshot_interval,
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Do not depend on the timer driver while a synchronous store blocks
    // a Tokio worker. Coalesced repeated ticks are proven separately with
    // a paused-clock async gate; this gate proves drain and final fencing.
    cleaner.drain().unwrap();
    store.release();
    tokio::time::timeout(Duration::from_secs(2), cleaner.wait(None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.entries.lock().unwrap().len(),
        256,
        "drain admitted another automatic page"
    );
    let mut state = SupervisorState {
        fragment: args.fragment,
        dispatcher,
        cleaner,
        snapshotter,
        shared,
        args,
        restarts: HashMap::new(),
        lifecycle: SupervisorLifecycle::Running,
    };
    state.quiesce().await.unwrap();
    let entries = store.entries.lock().unwrap();
    assert_eq!(entries.len(), 600);
    assert!(entries.values().all(|value| value.is_tombstone
        && value.timestamp_ms == 10
        && value.expires_at_ms == Some(1)));
    assert!(state.restarts.is_empty());
}

#[test]
fn fragment_shutdown_errors_identify_the_failed_phase() {
    assert_eq!(
        FragmentShutdownError::Actor("worker stopped".to_string()).to_string(),
        "actor drain failed: worker stopped"
    );
    assert_eq!(
        FragmentShutdownError::Storage("disk full".to_string()).to_string(),
        "final persistence flush failed: disk full"
    );
}

#[tokio::test]
async fn failed_supervisor_start_joins_partial_worker_tree() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let collision_name = "failed-supervisor-default-worker-0".to_string();
    let (collision, _handle) = KvWorker::spawn(
        Some(collision_name.clone()),
        KvWorker,
        KvWorkerArgs {
            shared,
            pending: None,
        },
    )
    .await
    .unwrap();
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };

    let result = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs {
            fragment: FragmentId(0),
            worker_prefix: "failed-supervisor".to_string(),
            store: None,
            hlc: Arc::new(Mutex::new(HlcClock::new())),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
            flush_batch_size: 100,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            tombstone_gc_age: None,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        },
    )
    .await;

    assert!(result.is_err());
    assert!(
        where_is("failed-supervisor-high-worker-0").is_none(),
        "failed dispatcher startup left an earlier worker running"
    );
    assert_eq!(
        where_is(collision_name).map(|cell| cell.get_id()),
        Some(collision.get_id()),
        "startup rollback stopped an actor it did not own"
    );
    collision.stop(None);
    let _ = collision.wait(None).await;
}

#[tokio::test]
async fn failed_snapshotter_start_joins_completed_supervisor_children() {
    let (store_dropped, store_drop) = std::sync::mpsc::channel();
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };
    let result = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs {
            fragment: FragmentId(0),
            worker_prefix: "failed-snapshotter".to_string(),
            store: Some(Arc::new(DropSignalStore(Some(store_dropped)))),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
            flush_batch_size: 100,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::ZERO,
            tombstone_gc_age: None,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        },
    )
    .await;

    assert!(result.is_err());
    assert!(
        store_drop.try_recv().is_ok(),
        "supervisor startup returned before completed children released their shared store"
    );
    for pool_name in ["high", "default", "bg"] {
        assert!(
            where_is(format!("failed-snapshotter-{pool_name}-worker-0")).is_none(),
            "failed supervisor startup left {pool_name} worker registered"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesce_drains_preaccepted_worker_messages_before_flushing() {
    let store = Arc::new(GatedStore::default());
    let _release = ReleaseStore(store.clone());
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };
    let (supervisor, _handle) = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs {
            fragment: FragmentId(0),
            worker_prefix: "quiesce-drain-test".to_string(),
            store: Some(store.clone()),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
            flush_batch_size: 1,
            flush_interval: Duration::from_secs(3600),
            clean_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            tombstone_gc_age: None,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        },
    )
    .await
    .unwrap();
    let dispatcher = match supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await
        .unwrap()
    {
        CallResult::Success(dispatcher) => dispatcher,
        other => panic!("dispatcher lookup failed: {other:?}"),
    };
    let health = match supervisor
        .call(|reply| SupervisorMessage::GetHealth { reply }, None)
        .await
        .unwrap()
    {
        CallResult::Success(health) => health,
        other => panic!("running health failed: {other:?}"),
    };
    assert_eq!(health.worker_count, 3);
    assert!(health.restarts.is_empty());

    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::ApplyOp {
            op: ReplicatedOp {
                key: "same-key".to_string(),
                value: VersionedValue::new(b"first".to_vec(), 1, "test".to_string()),
                seq: 1,
                boot_id: 1,
            },
            priority: crate::actor::OpPriority::Default,
            is_replication: false,
            reply: Some(first_tx.into()),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::ApplyOp {
            op: ReplicatedOp {
                key: "same-key".to_string(),
                value: VersionedValue::new(b"second".to_vec(), 2, "test".to_string()),
                seq: 2,
                boot_id: 1,
            },
            priority: crate::actor::OpPriority::Default,
            is_replication: false,
            reply: Some(second_tx.into()),
        })
        .unwrap();

    let quiesce_supervisor = supervisor.clone();
    let quiesce = tokio::spawn(async move {
        quiesce_supervisor
            .call(|reply| SupervisorMessage::Quiesce { reply }, None)
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !quiesce.is_finished(),
        "quiesce skipped blocked worker work"
    );

    store.release();
    assert_eq!(first_rx.await.unwrap().unwrap(), ApplyOutcome::Applied);
    assert_eq!(second_rx.await.unwrap().unwrap(), ApplyOutcome::Applied);
    match quiesce.await.unwrap().unwrap() {
        CallResult::Success(Ok(())) => {}
        other => panic!("quiesce failed: {other:?}"),
    }
    let health = supervisor
        .call(|reply| SupervisorMessage::GetHealth { reply }, None)
        .await
        .unwrap();
    assert!(matches!(health, CallResult::SenderError));
    let stopped_dispatcher = match supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await
        .unwrap()
    {
        CallResult::Success(dispatcher) => dispatcher,
        other => panic!("post-quiesce dispatcher lookup failed: {other:?}"),
    };
    assert_eq!(stopped_dispatcher.get_id(), dispatcher.get_id());
    assert_eq!(stopped_dispatcher.get_status(), ActorStatus::Stopped);
    assert!(stopped_dispatcher.get_children().is_empty());
    assert_eq!(
        store
            .entries
            .lock()
            .unwrap()
            .get(b"same-key".as_slice())
            .map(|value| value.value.clone()),
        Some(b"second".to_vec())
    );

    supervisor.stop(None);
    supervisor.wait(None).await.unwrap();
}
