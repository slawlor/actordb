// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Builder defaults, explicit settings and real child recovery.

use super::*;

#[tokio::test]
async fn supervision_builders_omit_settings_without_inventing_shared_dependencies() {
    let hlc = Arc::new(Mutex::new(HlcClock::new()));
    let make = || {
        SupervisorArgs::builder()
            .fragment(FragmentId(7))
            .worker_prefix("builder-supervisor")
            .hlc(hlc.clone())
            .build()
    };
    let args = make();
    let other = make();
    let defaults = crate::node::NodeConfig::default();
    assert!(Arc::ptr_eq(&args.hlc, &hlc));
    assert_eq!(args.fragment, FragmentId(7));
    assert_eq!(args.worker_prefix, "builder-supervisor");
    assert!(args.store.is_none());
    assert!(args.tombstone_gc_age.is_none());
    assert_eq!(args.flush_batch_size, defaults.flush_batch_size);
    assert_eq!(args.flush_interval, defaults.flush_interval);
    assert_eq!(args.clean_interval, defaults.clean_interval);
    assert_eq!(args.snapshot_interval, defaults.snapshot_interval);
    for (actual, expected) in [
        (&args.high_pool, &defaults.high_pool),
        (&args.default_pool, &defaults.default_pool),
        (&args.background_pool, &defaults.background_pool),
    ] {
        assert_eq!(
            (actual.workers, actual.queue_cap, actual.rate_limit_per_sec),
            (
                expected.workers,
                expected.queue_cap,
                expected.rate_limit_per_sec
            )
        );
    }
    assert!(!Arc::ptr_eq(&args.reset_senders, &other.reset_senders));
    args.reset_senders
        .lock()
        .await
        .insert((FragmentId(7), "peer".into()), 1);
    assert!(other.reset_senders.lock().await.is_empty());

    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        1000,
        Duration::from_millis(100),
    ));
    let make_cleaner = || {
        CleanerArgs::builder()
            .fragment(FragmentId(7))
            .shared(shared.clone())
            .build()
    };
    let cleaner = make_cleaner();
    assert!(Arc::ptr_eq(&cleaner.shared, &shared));
    assert_eq!(cleaner.fragment, FragmentId(7));
    assert_eq!(cleaner.interval, defaults.clean_interval);
    assert!(cleaner.tombstone_gc_age.is_none());
    assert!(!Arc::ptr_eq(
        &cleaner.reset_senders,
        &make_cleaner().reset_senders
    ));
    assert!(!Arc::ptr_eq(&cleaner.reset_senders, &args.reset_senders));
    let snapshotter = SnapshotterArgs::builder().shared(shared.clone()).build();
    assert!(Arc::ptr_eq(&snapshotter.shared, &shared));
    assert_eq!(snapshotter.interval, defaults.snapshot_interval);
}

#[tokio::test]
async fn supervision_builders_preserve_explicit_settings_and_legacy_overrides() {
    let hlc = Arc::new(Mutex::new(HlcClock::new()));
    let store: Arc<dyn KvStore> = Arc::new(GatedStore::default());
    let resets = Arc::new(Mutex::new(HashMap::from([(
        (FragmentId(8), "peer".into()),
        9,
    )])));
    let custom_pool = || {
        PoolConfig::builder()
            .workers(1)
            .queue_cap(3)
            .rate_limit_per_sec(5)
            .build()
    };
    let legacy_age = Duration::from_secs(6);
    let args = SupervisorArgs::builder()
        .fragment(FragmentId(8))
        .worker_prefix(String::from("builder-overrides"))
        .hlc(hlc.clone())
        .store(store.clone())
        .high_pool(custom_pool())
        .default_pool(custom_pool())
        .background_pool(custom_pool())
        .flush_batch_size(7)
        .flush_interval(Duration::from_millis(8))
        .clean_interval(Duration::from_secs(9))
        .snapshot_interval(Duration::from_secs(10))
        .tombstone_gc_age(legacy_age)
        .reset_senders(resets.clone())
        .build();
    assert!(Arc::ptr_eq(args.store.as_ref().unwrap(), &store));
    assert!(Arc::ptr_eq(&args.hlc, &hlc));
    assert!(Arc::ptr_eq(&args.reset_senders, &resets));
    assert_eq!(args.tombstone_gc_age, Some(legacy_age));
    assert_eq!(args.flush_batch_size, 7);
    assert_eq!(args.flush_interval, Duration::from_millis(8));
    assert_eq!(args.clean_interval, Duration::from_secs(9));
    assert_eq!(args.snapshot_interval, Duration::from_secs(10));
    for pool in [args.high_pool, args.default_pool, args.background_pool] {
        assert_eq!(
            (pool.workers, pool.queue_cap, pool.rate_limit_per_sec),
            (1, 3, Some(5))
        );
    }
    let no_store = SupervisorArgs::builder()
        .fragment(FragmentId(8))
        .worker_prefix("no-store")
        .hlc(hlc)
        .maybe_store(None)
        .maybe_tombstone_gc_age(None)
        .build();
    assert!(no_store.store.is_none());
    assert!(no_store.tombstone_gc_age.is_none());
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        1000,
        Duration::from_millis(100),
    ));
    let cleaner = CleanerArgs::builder()
        .fragment(FragmentId(8))
        .shared(shared.clone())
        .interval(Duration::from_secs(11))
        .maybe_tombstone_gc_age(Some(legacy_age))
        .reset_senders(resets.clone())
        .build();
    assert_eq!(cleaner.interval, Duration::from_secs(11));
    assert_eq!(cleaner.tombstone_gc_age, Some(legacy_age));
    assert!(Arc::ptr_eq(&cleaner.reset_senders, &resets));
    let snapshotter = SnapshotterArgs::builder()
        .shared(shared)
        .interval(Duration::from_secs(12))
        .build();
    assert_eq!(snapshotter.interval, Duration::from_secs(12));
}

#[tokio::test]
async fn supervisor_builder_defaults_start_and_quiesce_real_owned_workers() {
    let (supervisor, handle) = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs::builder()
            .fragment(FragmentId(0))
            .worker_prefix("builder-owned-workers")
            .hlc(Arc::new(Mutex::new(HlcClock::new())))
            .build(),
    )
    .await
    .unwrap();
    let CallResult::Success(health) = supervisor
        .call(
            |reply| SupervisorMessage::GetHealth { reply },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    else {
        panic!("builder defaults failed health probe")
    };
    assert_eq!(health.fragment, FragmentId(0));
    assert_eq!(health.worker_count, 8);
    assert_eq!(health.shed_counts, (0, 0, 0));
    assert!(matches!(
        supervisor
            .call(
                |reply| SupervisorMessage::Quiesce { reply },
                Some(Duration::from_secs(2))
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(()))
    ));
    supervisor.drain().unwrap();
    handle.await.unwrap();
}

#[derive(Default)]
struct RestartRecordingStore {
    entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl KvStore for RestartRecordingStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

struct StopRestartSupervisor(ActorCell);

impl Drop for StopRestartSupervisor {
    fn drop(&mut self) {
        self.0.stop_children(None);
        self.0.kill();
    }
}

async fn restart_owned_maintenance_child<M: ractor::Message>(
    supervisor: &ActorRef<SupervisorMessage>,
    name: &str,
) -> ActorRef<M> {
    let original = supervisor
        .get_children()
        .into_iter()
        .find(|child| child.is_message_type_of::<M>() == Some(true))
        .unwrap();
    original.kill();
    tokio::time::timeout(Duration::from_secs(5), async {
        original.wait(None).await.unwrap();
        loop {
            let replacement = supervisor.get_children().into_iter().find(|child| {
                child.is_message_type_of::<M>() == Some(true)
                    && child.get_id() != original.get_id()
                    && child.get_status() == ActorStatus::Running
            });
            let health = supervisor
                .call(
                    |reply| SupervisorMessage::GetHealth { reply },
                    Some(Duration::from_secs(2)),
                )
                .await
                .unwrap();
            if let (Some(replacement), CallResult::Success(health)) = (replacement, health) {
                if health.restarts.get(name) == Some(&1) {
                    return ActorRef::from(replacement);
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owned maintenance child did not restart")
}

#[tokio::test]
async fn maintenance_child_builder_restarts_preserve_shared_state_and_periodic_policy() {
    let store = Arc::new(RestartRecordingStore::default());
    let pool = |workers, queue_cap| {
        PoolConfig::builder()
            .workers(workers)
            .queue_cap(queue_cap)
            .build()
    };
    let (supervisor, handle) = FragmentSupervisor::spawn(
        None,
        FragmentSupervisor,
        SupervisorArgs::builder()
            .fragment(FragmentId(7))
            .worker_prefix("builder-maintenance-restart")
            .hlc(Arc::new(Mutex::new(HlcClock::new())))
            .store(store.clone())
            .high_pool(pool(3, 101))
            .default_pool(pool(3, 103))
            .background_pool(
                PoolConfig::builder()
                    .workers(2)
                    .queue_cap(107)
                    .rate_limit_per_sec(7)
                    .build(),
            )
            .flush_batch_size(usize::MAX)
            .flush_interval(Duration::from_secs(3600))
            .clean_interval(Duration::from_millis(17))
            .snapshot_interval(Duration::from_millis(11))
            .build(),
    )
    .await
    .unwrap();
    let _stop = StopRestartSupervisor(supervisor.get_cell());
    let CallResult::Success(dispatcher) = supervisor
        .call(
            |reply| SupervisorMessage::GetDispatcher { reply },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    else {
        panic!("dispatcher lookup failed")
    };
    let ordinary = VersionedValue::new(b"retained".to_vec(), 10, "writer".into());
    let apply = |key: &str, value: VersionedValue| ReplicatedOp {
        key: key.into(),
        value,
        seq: 1,
        boot_id: 1,
    };
    assert_eq!(
        dispatcher
            .call(
                |reply| KvMessage::ApplyOp {
                    op: apply("ordinary", ordinary.clone()),
                    priority: crate::actor::OpPriority::High,
                    is_replication: false,
                    reply: Some(reply)
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(ApplyOutcome::Applied))
    );

    // Identify the exact linked actor by its local message type, not a
    // global name, and join its old task before checking the replacement.
    let _cleaner = restart_owned_maintenance_child::<CleanerMessage>(&supervisor, "cleaner").await;
    let _snapshotter =
        restart_owned_maintenance_child::<SnapshotterMessage>(&supervisor, "snapshotter").await;
    let mut expiring = VersionedValue::new(b"temporary".to_vec(), 20, "writer".into());
    // Capture after both actors are running so their immediate startup
    // tick cannot already expire this value. The real wall clock, rather
    // than paused Tokio time, governs TTL; the store never blocks it.
    expiring.expires_at_ms = Some(now_ms() + 1000);
    let floor = expiring.expiry_tombstone();
    assert_eq!(
        dispatcher
            .call(
                |reply| KvMessage::ApplyOp {
                    op: apply("expiring", expiring.clone()),
                    priority: crate::actor::OpPriority::High,
                    is_replication: false,
                    reply: Some(reply)
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(ApplyOutcome::Applied))
    );

    // No manual Sweep/Snap and no write-trigger flush: both replacement
    // actors must retain their short configured intervals and shared cache
    // for the exact expiry floor to reach the original store within 5 s.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store.entries.lock().unwrap().get(b"expiring".as_slice()) == Some(&floor) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("replacement maintenance actors lost state or periodic policy");
    assert_eq!(
        store.entries.lock().unwrap().get(b"ordinary".as_slice()),
        Some(&ordinary)
    );
    assert_eq!(
        dispatcher
            .call(
                |reply| KvMessage::Get {
                    key: "ordinary".into(),
                    priority: crate::actor::OpPriority::High,
                    reply
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(Some(ordinary)))
    );
    assert_eq!(
        dispatcher
            .call(
                |reply| KvMessage::Get {
                    key: "expiring".into(),
                    priority: crate::actor::OpPriority::High,
                    reply
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(None))
    );
    let CallResult::Success(health) = supervisor
        .call(
            |reply| SupervisorMessage::GetHealth { reply },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    else {
        panic!("restart health probe failed")
    };
    assert_eq!(health.fragment, FragmentId(7));
    assert_eq!(health.worker_count, 8);
    for (suffix, expected) in [("high", 3), ("default", 3), ("bg", 2)] {
        assert_eq!(
            health
                .worker_names
                .iter()
                .filter(|name| name.contains(&format!("-{suffix}-worker-")))
                .count(),
            expected
        );
    }
    assert_eq!(
        health.restarts,
        HashMap::from([("cleaner".into(), 1), ("snapshotter".into(), 1)])
    );
    assert!(matches!(
        supervisor
            .call(
                |reply| SupervisorMessage::Quiesce { reply },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(()))
    ));
    assert!(
        supervisor.get_children().is_empty(),
        "planned quiesce restarted a child"
    );
    supervisor.drain().unwrap();
    handle.await.unwrap();
}
