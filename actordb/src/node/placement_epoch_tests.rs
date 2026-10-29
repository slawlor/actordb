// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Stable fragment transitions progress during actual unrelated publication.
use super::tests::test_placement_deps;
use super::*;
use crate::placement::ManualPlacement;
use crate::store::{StoreCommitDurability, StoreError};

#[tokio::test]
async fn unrelated_published_churn_keeps_gain_and_handoff_snapshot_current() {
    let stable = FragmentId(0);
    let moving = FragmentId(1);
    let replicas = vec!["node-a".into()];
    let placement = Arc::new(RwLock::new(HashMap::from([
        (stable, replicas.clone()),
        (moving, vec!["node-b".into()]),
    ])));
    let mut deps = test_placement_deps(
        FragmentRouter::new(2, 0),
        Arc::new(RwLock::new(HashSet::new())),
        placement,
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(Mutex::new(Default::default())),
        Arc::new(Mutex::new(HashMap::new())),
    );
    deps.fragment_count = 2;
    deps.placement_states
        .lock()
        .await
        .insert(stable, FragmentPlacementState::Bootstrapping);
    let controller = Arc::new(ManualPlacement::new(deps.placement.read().unwrap().clone()));
    deps.controller = controller.clone();
    let (_, pinned, generation) = fragment_placement_snapshot(&deps, stable);
    for index in 0..100 {
        controller.set_placement(HashMap::from([
            (stable, replicas.clone()),
            (moving, vec![format!("remote-{index}")]),
        ]));
        placement_tick(&deps).await;
        assert!(fragment_placement_matches(
            &deps, stable, &pinned, generation
        ));
    }
    assert_eq!(deps.placement_generation.load(Ordering::Acquire), 100);
    assert_eq!(deps.fragment_generation(stable), 0);
    assert_eq!(deps.fragment_generation(moving), 100);
    assert_eq!(
        commit_fragment_gain(&deps, stable, &pinned, generation).await,
        FragmentGainCommit::Serving
    );
}

#[derive(Default)]
struct StableRetirementStore {
    values: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    deleted: AtomicUsize,
}
impl KvStore for StableRetirementStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Stable
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Stable)
    }
    fn write_batch(&self, values: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.values.lock().unwrap().extend(values.iter().cloned());
        Ok(())
    }
    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Supported
    }
    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        self.values
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with(prefix));
        self.deleted.fetch_add(1, Ordering::Release);
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

async fn wait_for(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stable fragment transition stalled during unrelated churn");
}

struct ChurnAbort(tokio::task::AbortHandle);
impl Drop for ChurnAbort {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn churn(
    controller: Arc<ManualPlacement>,
    replicas: Vec<String>,
) -> (tokio::task::JoinHandle<()>, ChurnAbort) {
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(5));
        let mut index = 0usize;
        loop {
            interval.tick().await;
            controller.set_placement(HashMap::from([
                (FragmentId(0), replicas.clone()),
                (FragmentId(1), vec![format!("remote-{}", index % 2)]),
            ]));
            index += 1;
        }
    });
    let abort = ChurnAbort(task.abort_handle());
    (task, abort)
}

#[tokio::test]
async fn actual_gain_and_durable_retirement_finish_while_unrelated_fragment_keeps_moving() {
    let controller = Arc::new(ManualPlacement::new(HashMap::from([
        (FragmentId(0), vec!["epoch-source".into()]),
        (FragmentId(1), vec!["remote-0".into()]),
    ])));
    let source_store = Arc::new(StableRetirementStore::default());
    let target_store = Arc::new(StableRetirementStore::default());
    let config = |store: Arc<dyn KvStore>| NodeConfig {
        store: Some(store),
        fragment_count: 2,
        placement_controller: Some(controller.clone()),
        placement_poll_interval: Duration::from_millis(5),
        snapshot_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        replication: SenderConfig::builder()
            .batch_interval(Duration::from_millis(5))
            .consistency_interval(Duration::from_secs(3600))
            .build(),
        ..NodeConfig::default()
    };
    let source = Node::start(
        "epoch-source".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(source_store.clone()),
    )
    .await
    .unwrap();
    let target = Node::start(
        "epoch-target".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(target_store.clone()),
    )
    .await
    .unwrap();
    wait_for(async || {
        source.placement_report().await.states.get(&FragmentId(0))
            == Some(&FragmentPlacementState::Serving)
    })
    .await;
    let values: Vec<_> = (0..1000)
        .map(|index| format!("key-{index}"))
        .filter(|key| crate::shard::fragment_of_literal(key, 2) == FragmentId(0))
        .take(64)
        .map(|key| (key, vec![8; 4096]))
        .collect();
    assert_eq!(values.len(), 64);
    let results = source
        .put_multi_with_priority(values.clone(), OpPriority::Default)
        .await;
    assert!(results.iter().all(Result::is_ok));
    source.connect(target.addr()).await.unwrap();
    wait_for(async || source.peer_count().await == 1 && target.peer_count().await == 1).await;
    let gain_generation = target.placement_generation.load(Ordering::Acquire);
    let (moving, abort) = churn(
        controller.clone(),
        vec!["epoch-source".into(), "epoch-target".into()],
    );
    wait_for(async || {
        target.placement_generation.load(Ordering::Acquire) > gain_generation + 3
            && target.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
    })
    .await;
    assert_eq!(
        target
            .get(values[0].0.clone())
            .await
            .unwrap()
            .unwrap()
            .value,
        values[0].1
    );
    moving.abort();
    let _ = moving.await;
    drop(abort);
    let retirement_generation = source.placement_generation.load(Ordering::Acquire);
    let (moving, abort) = churn(controller, vec!["epoch-target".into()]);
    wait_for(async || {
        source.placement_generation.load(Ordering::Acquire) > retirement_generation + 3
            && source.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Stopped)
    })
    .await;
    assert_eq!(source_store.deleted.load(Ordering::Acquire), 1);
    assert!(source_store
        .load_prefix(&0u32.to_be_bytes())
        .unwrap()
        .is_empty());
    assert!(!target_store
        .load_prefix(&0u32.to_be_bytes())
        .unwrap()
        .is_empty());
    assert_eq!(
        target
            .get(values[63].0.clone())
            .await
            .unwrap()
            .unwrap()
            .value,
        values[63].1
    );
    moving.abort();
    let _ = moving.await;
    drop(abort);
    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}
