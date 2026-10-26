// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Fragment admission remains shared across independently ordered tables.
use super::flush_tests::FlakyStore;
use super::*;
use crate::actor::maintenance_test_support::{GatedStore, ReleaseMaintenanceStore};
use crate::actor::{KvActor, KvArgs, KvMessage, OpPriority, PoolConfig};
use crate::store::{StoreBinding, StoreError, StoreIdentity};
use ractor::{rpc::CallResult, Actor};
use std::sync::atomic::AtomicBool;
use std::sync::Mutex as StdMutex;

fn key(table: &str, key: &str) -> String {
    crate::table_key::encode_table_key(table, key)
}
fn op(table: &str, key_text: &str, version: u64, bytes: usize) -> ReplicatedOp {
    ReplicatedOp {
        key: key(table, key_text),
        value: VersionedValue::new(vec![version as u8; bytes], version, "writer".into()),
        seq: version,
        boot_id: 1,
    }
}
fn store() -> Arc<FlakyStore> {
    Arc::new(FlakyStore {
        failures_left: StdMutex::new(0),
        written: StdMutex::new(Vec::new()),
    })
}
fn state(
    store: Arc<dyn KvStore>,
    config: PersistenceConfig,
    configure_first: bool,
) -> FragmentState {
    let state = FragmentState::new(
        HashMap::new(),
        Some(store),
        usize::MAX,
        Duration::from_secs(3600),
    );
    if configure_first {
        state
            .with_persistence_config(config)
            .unwrap()
            .enable_partitions()
            .unwrap()
    } else {
        state
            .enable_partitions()
            .unwrap()
            .with_persistence_config(config)
            .unwrap()
    }
}
async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn named_tables_share_exact_fragment_record_and_byte_boundaries() {
    for configure_first in [false, true] {
        for record_limited in [false, true] {
            let first = op("A", "one", 1, 256);
            let charge = persistence_record_bytes(&first.key, &first.value);
            let config = PersistenceConfig::builder()
                .max_records(if record_limited { 1 } else { 8 })
                .max_bytes(if record_limited { 2048 } else { charge })
                .node_max_bytes(65536)
                .build();
            let state = state(store(), config, configure_first);
            assert_eq!(state.apply(&first).await.unwrap(), ApplyOutcome::Applied);
            for table in ["B", "C", "D"] {
                let denied = op(table, "one", 1, 256);
                let before = state.current_revision().await;
                assert_eq!(state.apply(&denied).await, Err(ApplyError::Backpressure));
                assert!(state.get(&denied.key).await.is_none());
                assert_eq!(state.current_revision().await, before);
                assert_eq!(state.persistence_backlog().await, (1, charge));
            }
            state.flush().await.unwrap();
            assert_eq!(state.persistence_backlog().await, (0, 0));
            assert_eq!(
                state.apply(&op("B", "one", 2, 256)).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
    }
}

#[tokio::test]
async fn preloaded_unpartitioned_standalone_actor_and_direct_base_share_custom_caps() {
    let values = HashMap::from([
        (
            key("KV", "loaded"),
            VersionedValue::new(vec![1], 1, "writer".into()),
        ),
        (
            key("A", "loaded"),
            VersionedValue::new(vec![2], 1, "writer".into()),
        ),
    ]);
    let state = FragmentState::new(values, Some(store()), usize::MAX, Duration::from_secs(3600))
        .with_persistence_config(
            PersistenceConfig::builder()
                .max_records(1)
                .max_bytes(2048)
                .node_max_bytes(65536)
                .build(),
        )
        .unwrap();
    assert!(!state.partitioned);
    assert!(state
        .table_children
        .read()
        .await
        .values()
        .all(|child| Arc::ptr_eq(&child.persistence, &state.persistence)));
    let base = ReplicatedOp {
        key: "base".into(),
        value: VersionedValue::new(vec![3; 256], 2, "writer".into()),
        seq: 2,
        boot_id: 1,
    };
    state.apply(&base).await.unwrap();
    let shared = Arc::new(state);
    let pool = PoolConfig::builder().workers(1).queue_cap(16).build();
    let (actor, _) = Actor::spawn(
        None,
        KvActor,
        KvArgs::builder()
            .shared(shared.clone())
            .fragment(crate::shard::FragmentId(0))
            .worker_prefix("shared-preloaded-caps")
            .default_pool(pool.clone())
            .high_pool(pool.clone())
            .background_pool(pool)
            .build(),
    )
    .await
    .unwrap();
    for table in ["KV", "A"] {
        let operation = op(table, "denied", 3, 256);
        let result = actor
            .call(
                |reply| KvMessage::ApplyOp {
                    op: operation,
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(result, CallResult::Success(Err(ApplyError::Backpressure)));
        assert!(shared.get_from_actor(&key(table, "denied")).await.is_none());
    }
    assert_eq!(
        shared.persistence_backlog().await,
        (1, persistence_record_bytes(&base.key, &base.value))
    );
    assert_eq!(
        shared
            .get_from_actor(&key("KV", "loaded"))
            .await
            .unwrap()
            .value,
        [1]
    );
    assert_eq!(
        shared
            .get_from_actor(&key("A", "loaded"))
            .await
            .unwrap()
            .value,
        [2]
    );
    actor.drain().unwrap();
    actor.wait(None).await.unwrap();
    assert_eq!(shared.persistence_backlog().await, (0, 0));
}

#[tokio::test]
async fn shared_node_budget_resize_failure_rolls_back_fragment_charge_and_record_slots() {
    let a = op("A", "one", 1, 128);
    let b = op("B", "one", 1, 128);
    let a_bytes = persistence_record_bytes(&a.key, &a.value);
    let b_bytes = persistence_record_bytes(&b.key, &b.value);
    let config = PersistenceConfig::builder()
        .max_records(2)
        .max_bytes(8192)
        .node_max_bytes(a_bytes + b_bytes)
        .build();
    let first = state(store(), config.clone(), true);
    let second = state(store(), config.clone(), false);
    first.apply(&a).await.unwrap();
    second.apply(&b).await.unwrap();
    assert_eq!(config.aggregate().used(), a_bytes + b_bytes);
    let denied = op("A", "one", 2, 256);
    let before = first.current_revision().await;
    assert_eq!(first.apply(&denied).await, Err(ApplyError::Backpressure));
    assert_eq!(first.current_revision().await, before);
    assert_eq!(first.get(&a.key).await.unwrap().timestamp_ms, 1);
    assert_eq!(first.persistence_backlog().await, (1, a_bytes));
    assert_eq!(config.aggregate().used(), a_bytes + b_bytes);
    let additional = op("C", "one", 2, 1);
    assert_eq!(
        first.apply(&additional).await,
        Err(ApplyError::Backpressure)
    );
    assert_eq!(first.persistence_backlog().await, (1, a_bytes));
    first.apply(&op("A", "one", 3, 64)).await.unwrap();
    second.apply(&op("B", "one", 2, 192)).await.unwrap();
    assert_eq!(first.persistence_backlog().await.0, 1);
    assert_eq!(second.persistence_backlog().await.0, 1);
    assert_eq!(config.aggregate().used(), a_bytes + b_bytes);
    first.flush().await.unwrap();
    second.flush().await.unwrap();
    assert_eq!(config.aggregate().used(), 0);
}

#[tokio::test]
async fn concurrent_table_admission_cannot_oversubscribe_fragment_records() {
    let shared = Arc::new(
        state(
            store(),
            PersistenceConfig::builder()
                .max_records(3)
                .max_bytes(65536)
                .node_max_bytes(131072)
                .build(),
            true,
        )
        .with_table_pool_capacity(64)
        .unwrap(),
    );
    let outcomes = futures::future::join_all((0..32).map(|index| {
        let shared = shared.clone();
        async move { shared.apply(&op(&format!("T{index}"), "one", 1, 1)).await }
    }))
    .await;
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(ApplyOutcome::Applied)))
            .count(),
        3
    );
    assert!(outcomes.iter().all(|outcome| matches!(
        outcome,
        Ok(ApplyOutcome::Applied) | Err(ApplyError::Backpressure)
    )));
    assert_eq!(shared.persistence_backlog().await.0, 3);
    shared.flush().await.unwrap();
    assert_eq!(shared.persistence_backlog().await, (0, 0));
}

struct PartitionGatedStore {
    gate: Arc<GatedStore>,
    fail_first: AtomicBool,
}
impl KvStore for PartitionGatedStore {
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
        StoreCommitDurability::Buffered
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Buffered)
    }
    fn write_batch(&self, values: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if values
            .iter()
            .any(|(key, _)| key.starts_with(&crate::table_key::table_storage_prefix("A")))
        {
            self.gate.write_batch(values)?;
            if self.fail_first.swap(false, Ordering::AcqRel) {
                return Err(StoreError("ambiguous A failure".into()));
            }
        } else {
            self.gate
                .entries
                .lock()
                .unwrap()
                .extend(values.iter().cloned());
        }
        Ok(())
    }
}

#[tokio::test]
async fn active_and_pending_same_key_keep_two_slots_until_cancelled_failed_owner_restores() {
    let gate = Arc::new(GatedStore::default());
    let release = ReleaseMaintenanceStore(gate.clone());
    let shared = Arc::new(state(
        Arc::new(PartitionGatedStore {
            gate: gate.clone(),
            fail_first: AtomicBool::new(true),
        }),
        PersistenceConfig::builder()
            .max_records(2)
            .max_bytes(8192)
            .node_max_bytes(16384)
            .build(),
        true,
    ));
    let first = op("A", "one", 1, 128);
    shared.apply(&first).await.unwrap();
    let child = shared.table_state("A").await.unwrap();
    let flushing = child.clone();
    let flush = tokio::spawn(async move { flushing.flush_local().await });
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    let newer = op("A", "one", 2, 128);
    shared.apply(&newer).await.unwrap();
    {
        let flush = child.flush.lock().await;
        assert_eq!(
            flush.in_flight.as_ref().unwrap()[&first.key]
                .value
                .timestamp_ms,
            1
        );
        assert_eq!(flush.pending[&first.key].value.timestamp_ms, 2);
    }
    let bytes = persistence_record_bytes(&first.key, &first.value)
        + persistence_record_bytes(&newer.key, &newer.value);
    assert_eq!(shared.persistence_backlog().await, (2, bytes));
    flush.abort();
    assert!(flush.await.unwrap_err().is_cancelled());
    assert_eq!(
        shared.apply(&op("B", "one", 1, 128)).await,
        Err(ApplyError::Backpressure)
    );
    assert_eq!(shared.persistence_backlog().await, (2, bytes));
    drop(release);
    shared.executor.drain().await;
    assert_eq!(
        shared.persistence_backlog().await,
        (1, persistence_record_bytes(&newer.key, &newer.value))
    );
    assert_eq!(shared.get(&newer.key).await.unwrap().timestamp_ms, 2);
    shared.apply(&op("B", "one", 1, 128)).await.unwrap();
    shared.flush().await.unwrap();
    assert_eq!(shared.persistence_backlog().await, (0, 0));
    assert!(gate
        .entries
        .lock()
        .unwrap()
        .values()
        .any(|value| value.timestamp_ms == 2));
}

#[tokio::test]
async fn independent_table_flushes_progress_while_another_partition_owns_storage() {
    let gate = Arc::new(GatedStore::default());
    let release = ReleaseMaintenanceStore(gate.clone());
    let shared = Arc::new(state(
        Arc::new(PartitionGatedStore {
            gate: gate.clone(),
            fail_first: AtomicBool::new(false),
        }),
        PersistenceConfig::builder()
            .max_records(2)
            .max_bytes(8192)
            .node_max_bytes(16384)
            .storage_concurrency(2)
            .build(),
        true,
    ));
    shared.apply(&op("A", "one", 1, 128)).await.unwrap();
    shared.apply(&op("B", "one", 1, 128)).await.unwrap();
    let a = shared.table_state("A").await.unwrap();
    let flushing = a.clone();
    let first = tokio::spawn(async move { flushing.flush_local().await });
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    let b = shared.table_state("B").await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), b.flush_local())
        .await
        .expect("B flush serialized behind A")
        .unwrap();
    assert!(!first.is_finished());
    assert_eq!(shared.persistence_backlog().await.0, 1);
    drop(release);
    first.await.unwrap().unwrap();
    shared.executor.drain().await;
    assert_eq!(shared.persistence_backlog().await, (0, 0));
}
