// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Placement isolation and cancellation at actual storage admission/completion.

use super::tests::test_placement_deps;
use super::*;
use crate::store::{PersistenceConfig, StoreCommitDurability, StoreError};
use std::sync::Condvar;

#[derive(Default)]
struct StorageLatch {
    entered: AtomicUsize,
    released: StdMutex<bool>,
    wake: Condvar,
}

impl StorageLatch {
    fn block(&self) {
        self.entered.fetch_add(1, Ordering::Release);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

// A test failure must not strand a spawn_blocking call and hang runtime exit.
struct ReleaseStorage(Arc<StorageLatch>);
impl Drop for ReleaseStorage {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Default)]
struct GatedRetirementStore {
    deletion: Arc<StorageLatch>,
    fail_delete: AtomicUsize,
    prefixes: StdMutex<Vec<Vec<u8>>>,
}

impl KvStore for GatedRetirementStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Stable
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Stable)
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Supported
    }
    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        self.prefixes.lock().unwrap().push(prefix.to_vec());
        self.deletion.block();
        if self
            .fail_delete
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(StoreError("injected ambiguous delete failure".into()));
        }
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

struct QuiescedSupervisor;
impl Actor for QuiescedSupervisor {
    type Msg = SupervisorMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        _: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        _: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        if let SupervisorMessage::Quiesce { reply } = message {
            let _ = reply.send(Ok(()));
        }
        Ok(())
    }
}

async fn retirement_fixture() -> (
    PlacementDriverDeps,
    ActorRef<SupervisorMessage>,
    Arc<crate::placement::ManualPlacement>,
    Arc<GatedRetirementStore>,
) {
    let retiring = FragmentId(0);
    let gained = FragmentId(1);
    let revoked = FragmentId(2);
    let map = HashMap::from([
        (retiring, vec!["node-b".to_string()]),
        (gained, vec!["node-b".to_string()]),
        (revoked, vec!["node-a".to_string()]),
    ]);
    let (supervisor, _) = Actor::spawn(None, QuiescedSupervisor, ()).await.unwrap();
    let mut deps = test_placement_deps(
        FragmentRouter::new(3, 0),
        Arc::new(RwLock::new(HashSet::from([revoked]))),
        Arc::new(RwLock::new(map.clone())),
        Arc::new(Mutex::new(HashMap::from([(retiring, supervisor.clone())]))),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(Mutex::new(HashMap::new())),
    );
    // Production detaches active routing before entering prefix finalization;
    // only the exact shutdown registry keeps this supervisor alive.
    deps.supervisors.lock().await.remove(&retiring);
    deps.fragment_count = 3;
    deps.subtree_config.fragment_count = 3;
    deps.subtree_config.persistence = PersistenceConfig::builder().storage_concurrency(1).build();
    let store = Arc::new(GatedRetirementStore::default());
    deps.subtree_config.store = Some(store.clone());
    deps.placement_states
        .lock()
        .await
        .insert(revoked, FragmentPlacementState::Serving);
    let controller = Arc::new(crate::placement::ManualPlacement::new(map));
    deps.controller = controller.clone();
    (deps, supervisor, controller, store)
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("storage test condition timed out");
}

async fn publish_readd_and_unrelated_changes(
    deps: &PlacementDriverDeps,
    controller: &crate::placement::ManualPlacement,
) {
    controller.set_placement(HashMap::from([
        (FragmentId(0), vec!["node-a".to_string()]),
        (FragmentId(1), vec!["node-a".to_string()]),
        (FragmentId(2), vec!["node-b".to_string()]),
    ]));
    tokio::time::timeout(Duration::from_millis(250), placement_tick(deps))
        .await
        .expect("unrelated map publication stalled behind storage");
    let placement = deps.placement.read().unwrap().clone();
    assert_eq!(placement[&FragmentId(0)], ["node-b"]);
    assert_eq!(placement[&FragmentId(1)], ["node-a"]);
    assert_eq!(placement[&FragmentId(2)], ["node-b"]);
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Draining)
    );
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(2)),
        Some(&FragmentPlacementState::Draining)
    );
    let generation = deps.placement_generation.load(Ordering::Acquire);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(250),
            commit_fragment_gain(deps, FragmentId(1), &["node-a".into()], generation),
        )
        .await
        .expect("unrelated Serving commit stalled behind storage"),
        FragmentGainCommit::Serving
    );
    assert!(deps.serving.read().unwrap().contains(&FragmentId(1)));
}

#[tokio::test]
async fn queued_and_running_prefix_delete_only_defer_their_own_fragment() {
    let (deps, supervisor, controller, store) = retirement_fixture().await;
    let release_delete = ReleaseStorage(store.deletion.clone());
    let executor = deps.subtree_config.persistence.executor();
    let occupied = Arc::new(StorageLatch::default());
    let release_slot = ReleaseStorage(occupied.clone());
    let slot_executor = executor.clone();
    let slot_latch = occupied.clone();
    let slot = tokio::spawn(async move {
        slot_executor
            .run(move || {
                slot_latch.block();
                Ok(())
            })
            .await
    });
    wait_until(|| occupied.entered.load(Ordering::Acquire) == 1).await;
    let retirement_deps = deps.clone();
    let retirement_supervisor = supervisor.clone();
    let retirement = tokio::spawn(async move {
        finish_fragment_prefix_retirement(
            &retirement_deps,
            FragmentId(0),
            retirement_supervisor,
            vec!["node-b".into()],
            0,
        )
        .await;
    });
    wait_until(|| {
        deps.prefix_retirements.is_sealed(FragmentId(0)) && executor.diagnostics().queued_jobs == 1
    })
    .await;
    publish_readd_and_unrelated_changes(&deps, &controller).await;
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 0);

    drop(release_slot);
    slot.await.unwrap().unwrap();
    wait_until(|| store.deletion.entered.load(Ordering::Acquire) == 1).await;
    publish_readd_and_unrelated_changes(&deps, &controller).await;
    assert!(!retirement.is_finished());
    assert!(deps.supervisor_stops.get(FragmentId(0)).is_some());

    drop(release_delete);
    retirement.await.unwrap();
    executor.drain().await;
    assert!(!deps.prefix_retirements.is_sealed(FragmentId(0)));
    assert_eq!(
        store.prefixes.lock().unwrap().as_slice(),
        &[vec![0, 0, 0, 0]]
    );
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Stopped)
    );
    assert!(deps.supervisor_stops.get(FragmentId(0)).is_none());
    placement_tick(&deps).await;
    assert_eq!(deps.placement.read().unwrap()[&FragmentId(0)], ["node-a"]);
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Bootstrapping)
    );
}

#[tokio::test]
async fn cancelled_running_delete_keeps_its_fragment_sealed_until_backend_completion() {
    let (deps, supervisor, controller, store) = retirement_fixture().await;
    let release_delete = ReleaseStorage(store.deletion.clone());
    let executor = deps.subtree_config.persistence.executor();
    let retirement_deps = deps.clone();
    let retirement_supervisor = supervisor.clone();
    let retirement = tokio::spawn(async move {
        finish_fragment_prefix_retirement(
            &retirement_deps,
            FragmentId(0),
            retirement_supervisor,
            vec!["node-b".into()],
            0,
        )
        .await;
    });
    wait_until(|| store.deletion.entered.load(Ordering::Acquire) == 1).await;
    retirement.abort();
    assert!(retirement.await.unwrap_err().is_cancelled());
    publish_readd_and_unrelated_changes(&deps, &controller).await;
    assert!(deps.prefix_retirements.is_sealed(FragmentId(0)));
    assert!(deps.supervisor_stops.get(FragmentId(0)).is_some());

    let drain_executor = executor.clone();
    let drain = tokio::spawn(async move { drain_executor.drain().await });
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    drop(release_delete);
    drain.await.unwrap();
    placement_tick(&deps).await;
    assert_eq!(deps.placement.read().unwrap()[&FragmentId(0)], ["node-a"]);
    // The surviving exact supervisor and Draining state still prevent a new
    // subtree, even after the backend drops its seal. Shutdown can join it.
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Draining)
    );
    finish_fragment_prefix_retirement(&deps, FragmentId(0), supervisor, vec!["node-b".into()], 0)
        .await;
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn cancelled_queued_delete_never_starts_and_readd_waits_for_old_supervisor() {
    let (deps, supervisor, controller, store) = retirement_fixture().await;
    let executor = deps.subtree_config.persistence.executor();
    let occupied = Arc::new(StorageLatch::default());
    let release_slot = ReleaseStorage(occupied.clone());
    let slot_executor = executor.clone();
    let slot_latch = occupied.clone();
    let slot = tokio::spawn(async move {
        slot_executor
            .run(move || {
                slot_latch.block();
                Ok(())
            })
            .await
    });
    wait_until(|| occupied.entered.load(Ordering::Acquire) == 1).await;
    let retirement_deps = deps.clone();
    let retirement_supervisor = supervisor.clone();
    let retirement = tokio::spawn(async move {
        finish_fragment_prefix_retirement(
            &retirement_deps,
            FragmentId(0),
            retirement_supervisor,
            vec!["node-b".into()],
            0,
        )
        .await;
    });
    wait_until(|| executor.diagnostics().queued_jobs == 1).await;
    retirement.abort();
    assert!(retirement.await.unwrap_err().is_cancelled());
    assert!(!deps.prefix_retirements.is_sealed(FragmentId(0)));
    controller.set_placement(HashMap::from([
        (FragmentId(0), vec!["node-a".into()]),
        (FragmentId(1), vec!["node-b".into()]),
        (FragmentId(2), vec!["node-a".into()]),
    ]));
    placement_tick(&deps).await;
    assert_eq!(deps.placement.read().unwrap()[&FragmentId(0)], ["node-a"]);
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Draining)
    );
    finish_fragment_prefix_retirement(&deps, FragmentId(0), supervisor, vec!["node-b".into()], 0)
        .await;
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 0);
    drop(release_slot);
    slot.await.unwrap().unwrap();
    executor.drain().await;
}

#[tokio::test]
async fn ambiguous_delete_failure_allows_readd_but_never_retries_a_stale_generation() {
    let (deps, supervisor, controller, store) = retirement_fixture().await;
    let release_delete = ReleaseStorage(store.deletion.clone());
    store.fail_delete.store(1, Ordering::Release);
    let retirement_deps = deps.clone();
    let retirement = tokio::spawn(async move {
        finish_fragment_prefix_retirement(
            &retirement_deps,
            FragmentId(0),
            supervisor,
            vec!["node-b".into()],
            0,
        )
        .await;
    });
    wait_until(|| store.deletion.entered.load(Ordering::Acquire) == 1).await;
    publish_readd_and_unrelated_changes(&deps, &controller).await;
    drop(release_delete);
    wait_until(|| !deps.prefix_retirements.is_sealed(FragmentId(0))).await;
    placement_tick(&deps).await;
    retirement.await.unwrap();
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 1);
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Stopped)
    );
}

#[tokio::test]
async fn unchanged_authority_retries_ambiguous_deletion_idempotently() {
    let (deps, supervisor, _, store) = retirement_fixture().await;
    store.fail_delete.store(2, Ordering::Release);
    store.deletion.release();
    finish_fragment_prefix_retirement(&deps, FragmentId(0), supervisor, vec!["node-b".into()], 0)
        .await;
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 3);
    assert!(store
        .prefixes
        .lock()
        .unwrap()
        .iter()
        .all(|prefix| prefix == &[0, 0, 0, 0]));
    assert!(!deps.prefix_retirements.is_sealed(FragmentId(0)));
    assert_eq!(
        deps.placement_states.lock().await.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Stopped)
    );
}

#[tokio::test]
async fn placement_aba_before_delete_never_mutates_the_prefix() {
    let (deps, supervisor, _, store) = retirement_fixture().await;
    deps.placement_generation.store(2, Ordering::Release);
    finish_fragment_prefix_retirement(&deps, FragmentId(0), supervisor, vec!["node-b".into()], 0)
        .await;
    assert_eq!(store.deletion.entered.load(Ordering::Acquire), 0);
    assert!(!deps.prefix_retirements.is_sealed(FragmentId(0)));
}

#[tokio::test(start_paused = true)]
#[tracing_test::traced_test]
async fn recovery_mismatch_waits_without_sender_churn_and_shutdown_cancels_it() {
    let (mut deps, supervisor, _, _) = retirement_fixture().await;
    let fragment = FragmentId(0);
    deps.placement
        .write()
        .unwrap()
        .insert(fragment, vec!["node-a".into()]);
    deps.recovery = Some(Arc::new(RecoveryContext::new(
        recovery::RecoveryOptions::new([1; 32]).unwrap(),
        "node-a".into(),
        HashMap::from([(fragment, vec!["node-a".into(), "node-b".into()])]),
    )));
    let tasks = RuntimeTasks::new(8);
    let gain_deps = deps.clone();
    assert!(
        tasks
            .spawn(async move {
                assert!(!converge_fragment_gain(&gain_deps, fragment).await);
            })
            .await
    );
    tokio::task::yield_now().await;
    // The old implementation returned immediately, causing its caller to
    // repeatedly retire and recreate senders. The gain must stay blocked.
    assert_eq!(
        deps.placement_states.lock().await.get(&fragment),
        Some(&FragmentPlacementState::Bootstrapping)
    );
    assert!(logs_contain("recovery_placement_mismatch"));
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(!deps.serving.read().unwrap().contains(&fragment));
    assert!(deps.senders.lock().await.is_empty());
    logs_assert(|lines| {
        let warnings = lines
            .iter()
            .filter(|line| line.contains("recovery_placement_mismatch"))
            .count();
        if warnings != 1 {
            return Err(format!("unchanged mismatch logged {warnings} warnings"));
        }
        Ok(())
    });
    tokio::time::timeout(Duration::from_millis(1), tasks.shutdown())
        .await
        .expect("blocked recovery gain delayed shutdown");
    supervisor.stop_and_wait(None, None).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn recovery_mismatch_wakes_on_revocation_with_bounded_delay() {
    let (mut deps, supervisor, _, _) = retirement_fixture().await;
    let fragment = FragmentId(0);
    deps.placement
        .write()
        .unwrap()
        .insert(fragment, vec!["node-a".into()]);
    deps.recovery = Some(Arc::new(RecoveryContext::new(
        recovery::RecoveryOptions::new([1; 32]).unwrap(),
        "node-a".into(),
        HashMap::from([(fragment, vec!["node-a".into(), "node-b".into()])]),
    )));
    let gain_deps = deps.clone();
    let gain = tokio::spawn(async move { converge_fragment_gain(&gain_deps, fragment).await });
    tokio::task::yield_now().await;
    assert!(!gain.is_finished());
    deps.placement
        .write()
        .unwrap()
        .insert(fragment, vec!["node-b".into()]);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(!gain.await.unwrap());
    assert_eq!(
        deps.placement_states.lock().await.get(&fragment),
        Some(&FragmentPlacementState::Draining)
    );
    supervisor.stop_and_wait(None, None).await.unwrap();
}
