// Copyright (c) Sean Lawlor
//
// This source code is licensed under the MIT license in the workspace root.

//! Downstream-facing compatibility checks for the actor module facades.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use actordb::actor::{self, FragmentState, HlcClock, KvArgs, KvWorkerArgs, PoolConfig};
use actordb::shard::FragmentId;
use actordb::supervision::{self, CleanerArgs, SnapshotterArgs, SupervisorArgs};
use ractor::Actor;
use tokio::sync::Mutex;

#[test]
fn builder_types_keep_their_public_paths() {
    // Bon's default typestate modules are private anonymous APIs. Public
    // builder types and inferred setter chains must work without exposing them.
    let _: actor::PoolConfigBuilder = PoolConfig::builder();
    let _: actor::KvArgsBuilder = KvArgs::builder();
    let _: actor::KvWorkerArgsBuilder = KvWorkerArgs::builder();
    let _: supervision::SupervisorArgsBuilder = SupervisorArgs::builder();
    let _: supervision::CleanerArgsBuilder = CleanerArgs::builder();
    let _: supervision::SnapshotterArgsBuilder = SnapshotterArgs::builder();
    let _: actor::SupervisorArgsBuilder = actor::SupervisorArgs::builder();
    let _: actor::CleanerArgsBuilder = actor::CleanerArgs::builder();
    let _: actor::SnapshotterArgsBuilder = actor::SnapshotterArgs::builder();
}

#[test]
fn arguments_build_through_the_facades_with_existing_defaults() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        1000,
        Duration::from_millis(100),
    ));
    let args = KvArgs::builder()
        .shared(shared.clone())
        .fragment(FragmentId(0))
        .worker_prefix("facade-test")
        .build();
    assert_eq!(args.default_pool.workers, 4);
    assert_eq!(args.high_pool.workers, 2);
    assert_eq!(args.background_pool.rate_limit_per_sec, Some(1000));

    let worker = KvWorkerArgs::builder().shared(shared.clone()).build();
    assert!(worker.pending.is_none());
    let cleaner = CleanerArgs::builder()
        .shared(shared.clone())
        .fragment(FragmentId(0))
        .build();
    assert_eq!(cleaner.interval, Duration::from_secs(30));
    assert!(cleaner.tombstone_gc_age.is_none());
    let snapshotter = SnapshotterArgs::builder().shared(shared).build();
    assert_eq!(snapshotter.interval, Duration::from_secs(60));

    let supervisor = SupervisorArgs::builder()
        .fragment(FragmentId(0))
        .worker_prefix("facade-test")
        .hlc(Arc::new(Mutex::new(HlcClock::new())))
        .build();
    assert_eq!(supervisor.flush_batch_size, 1000);
    assert_eq!(supervisor.flush_interval, Duration::from_millis(100));
    assert_eq!(supervisor.clean_interval, cleaner.interval);
    assert_eq!(supervisor.snapshot_interval, snapshotter.interval);
    assert!(supervisor.store.is_none());
}

#[test]
fn actor_associated_state_types_keep_their_public_paths() {
    fn state_is<A: Actor<State = S>, S: Send + 'static>() {}

    state_is::<actor::KvActor, actor::KvState>();
    state_is::<actor::KvWorker, (Arc<FragmentState>, Option<Arc<AtomicU64>>)>();
    state_is::<supervision::FragmentSupervisor, supervision::SupervisorState>();
    state_is::<supervision::Cleaner, supervision::CleanerState>();
    state_is::<supervision::Snapshotter, supervision::SnapshotterState>();
    // Cross-facade associated states prove these are aliases, not wrapper actors.
    state_is::<actor::FragmentSupervisor, supervision::SupervisorState>();
    state_is::<actor::Cleaner, supervision::CleanerState>();
    state_is::<actor::Snapshotter, supervision::SnapshotterState>();
    state_is::<supervision::FragmentSupervisor, actor::SupervisorState>();
    state_is::<supervision::Cleaner, actor::CleanerState>();
    state_is::<supervision::Snapshotter, actor::SnapshotterState>();
}

#[test]
fn canonical_and_compatibility_facades_export_identical_types() {
    fn same_type<A: 'static, B: 'static>() {
        assert_eq!(std::any::TypeId::of::<A>(), std::any::TypeId::of::<B>());
    }

    same_type::<actor::SupervisorArgs, supervision::SupervisorArgs>();
    same_type::<actor::CleanerArgs, supervision::CleanerArgs>();
    same_type::<actor::SnapshotterArgs, supervision::SnapshotterArgs>();
    same_type::<actor::SupervisorMessage, supervision::SupervisorMessage>();
    same_type::<actor::CleanerMessage, supervision::CleanerMessage>();
    same_type::<actor::SnapshotterMessage, supervision::SnapshotterMessage>();
    same_type::<actor::FragmentHealth, supervision::FragmentHealth>();
    same_type::<actor::FragmentShutdownError, supervision::FragmentShutdownError>();
    same_type::<actor::MaintenanceTrigger, supervision::MaintenanceTrigger>();
}
