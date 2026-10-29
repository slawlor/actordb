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

#[tokio::test]
async fn explicit_expiry_retries_dirty_other_table_before_reserving_a_marker() {
    for record_limited in [false, true] {
        let backend = store();
        let dirty = op("B", "one", 1, 128);
        let charge = persistence_record_bytes(&dirty.key, &dirty.value);
        let mut expiring = VersionedValue::new(vec![9; 128], 1, "writer".into());
        expiring.expires_at_ms = Some(100);
        let state = FragmentState::new(
            HashMap::from([(key("A", "due"), expiring.clone())]),
            Some(backend.clone()),
            usize::MAX,
            Duration::from_secs(3600),
        )
        .with_persistence_config(
            PersistenceConfig::builder()
                .max_records(if record_limited { 1 } else { 8 })
                .max_bytes(if record_limited { 8192 } else { charge })
                .node_max_bytes(65536)
                .build(),
        )
        .unwrap()
        .enable_partitions()
        .unwrap();
        state.apply(&dirty).await.unwrap();
        let revision = state.current_revision().await;
        *backend.failures_left.lock().unwrap() = 1;
        assert!(state.expire_values(100).await.is_err());
        assert_eq!(state.current_revision().await, revision);
        assert_eq!(state.persistence_backlog().await, (1, charge));
        assert_eq!(
            state
                .get_versioned(&key("A", "due"), Some(99))
                .await
                .unwrap(),
            expiring
        );
        let a = state.table_state("A").await.unwrap();
        assert!(!a.cache.read().unwrap().values[&key("A", "due")].is_tombstone);
        assert_eq!(state.expire_values(100).await.unwrap(), 1);
        state.flush().await.unwrap();
        assert_eq!(state.persistence_backlog().await, (0, 0));
        assert_eq!(
            state
                .get_versioned(&key("A", "due"), Some(99))
                .await
                .unwrap(),
            expiring.expiry_tombstone()
        );
        assert!(state.current_revision().await > revision);
    }
}

#[tokio::test]
async fn healthy_shutdown_retry_releases_other_tables_shared_slot_and_emits_expiry_once() {
    use crate::node::{Node, NodeConfig};
    use crate::ttl_expiry::{expiry_handler_fn, ExpiryCallbackError};
    use std::sync::atomic::AtomicUsize;
    for record_limited in [false, true] {
        let backend = store();
        let identity = if record_limited {
            "record-expiry-retry"
        } else {
            "bytes-expiry-retry"
        };
        let dirty = ReplicatedOp {
            key: key("B", "one"),
            value: VersionedValue::new(vec![8; 128], 1, identity.into()),
            seq: 0,
            boot_id: 1,
        };
        let charge = persistence_record_bytes(&dirty.key, &dirty.value);
        let delivered = Arc::new(AtomicUsize::new(0));
        let delivered_handler = delivered.clone();
        let handler = Arc::new(expiry_handler_fn(move |batch| {
            let delivered = delivered_handler.clone();
            async move {
                delivered.fetch_add(batch.events.len(), Ordering::AcqRel);
                Ok::<(), ExpiryCallbackError>(())
            }
        }));
        let config = NodeConfig::builder()
            .store(backend.clone())
            .flush_batch_size(usize::MAX)
            .snapshot_interval(Duration::from_secs(3600))
            .clean_interval(Duration::from_secs(3600))
            .persistence(
                PersistenceConfig::builder()
                    .max_records(if record_limited { 1 } else { 8 })
                    .max_bytes(if record_limited { 8192 } else { charge })
                    .node_max_bytes(65536)
                    .build(),
            )
            .ttl_expiry_handler(handler)
            .build();
        let node = Node::start(identity.into(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap();
        // Let the initial immediate Cleaner/Snapshotter ticks settle before
        // constructing the deliberately dirty cross-table shutdown state.
        tokio::time::sleep(Duration::from_millis(50)).await;
        node.table("A")
            .unwrap()
            .put_with_write_concern_options(
                "due",
                vec![9; 128],
                Some(Duration::from_millis(50)),
                OpPriority::Default,
                crate::write_concern::WriteConcernOptions::default()
                    .with_minimum_durability(crate::write_concern::WriteDurability::StoreCommitted),
            )
            .await
            .unwrap();
        let original = backend
            .written
            .lock()
            .unwrap()
            .iter()
            .find(|(physical, _)| {
                physical
                    .windows(key("A", "due").len())
                    .any(|part| part == key("A", "due").as_bytes())
            })
            .unwrap()
            .1
            .clone();
        node.table("B")
            .unwrap()
            .put("one", vec![8; 128])
            .await
            .unwrap();
        let deadline = original.expires_at_ms.unwrap();
        tokio::time::sleep(Duration::from_millis(
            deadline.saturating_sub(now_ms()).saturating_add(1),
        ))
        .await;
        *backend.failures_left.lock().unwrap() = 100;
        assert!(
            tokio::time::timeout(Duration::from_secs(2), node.shutdown())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(delivered.load(Ordering::Acquire), 0);
        *backend.failures_left.lock().unwrap() = 0;
        tokio::time::timeout(Duration::from_secs(2), node.shutdown())
            .await
            .expect("healthy retry starved A behind dirty B")
            .unwrap();
        assert_eq!(delivered.load(Ordering::Acquire), 1);
        let written = backend.written.lock().unwrap();
        let marker = written
            .iter()
            .rev()
            .find(|(physical, _)| {
                physical
                    .windows(key("A", "due").len())
                    .any(|part| part == key("A", "due").as_bytes())
            })
            .unwrap()
            .1
            .clone();
        assert_eq!(marker, original.expiry_tombstone());
        assert!(written.iter().any(|(physical, value)| physical
            .ends_with(key("B", "one").as_bytes())
            && value.value == [8; 128]));
    }
}

#[tokio::test]
async fn manual_pass_finishes_multiple_due_rows_and_tables_at_one_record_capacity() {
    let backend = store();
    let values = ["A", "B", "C"]
        .into_iter()
        .flat_map(|table| {
            (0..4).map(move |index| {
                let mut value = VersionedValue::new(vec![9; 128], 1, "writer".into());
                value.expires_at_ms = Some(100);
                (key(table, &format!("old-{index}")), value)
            })
        })
        .collect();
    let shared = FragmentState::new(
        values,
        Some(backend.clone()),
        usize::MAX,
        Duration::from_secs(3600),
    )
    .with_persistence_config(
        PersistenceConfig::builder()
            .max_records(1)
            .max_bytes(8192)
            .node_max_bytes(65536)
            .build(),
    )
    .unwrap()
    .enable_partitions()
    .unwrap();
    assert_eq!(shared.expire_values(100).await.unwrap(), 12);
    assert_eq!(shared.expire_values(100).await.unwrap(), 0);
    assert_eq!(shared.persistence_backlog().await, (0, 0));
    assert_eq!(backend.written.lock().unwrap().len(), 12);
    assert!(backend
        .written
        .lock()
        .unwrap()
        .iter()
        .all(|(_, value)| value.is_tombstone));
}

#[tokio::test]
async fn manual_saturation_retry_keeps_original_deadline_and_table_quotas() {
    let deadline = now_ms() + 10000;
    let values = (0..3)
        .map(|index| {
            let mut value = VersionedValue::new(vec![9; 128], 1, "writer".into());
            value.expires_at_ms = Some(deadline);
            (key("A", &format!("old-{index}")), value)
        })
        .collect();
    let shared = FragmentState::new(values, Some(store()), usize::MAX, Duration::from_secs(3600))
        .with_persistence_config(
            PersistenceConfig::builder()
                .max_records(1)
                .max_bytes(8192)
                .node_max_bytes(65536)
                .build(),
        )
        .unwrap()
        .enable_partitions()
        .unwrap();
    let mut pass = ExpiryPass::new(deadline);
    pass.retry_saturation = true;
    let first = shared
        .expire_values_page_partitioned(&mut pass)
        .await
        .unwrap();
    assert_eq!(first.converted, 1);
    assert!(first.saturated);
    assert!(!first.done);
    for table in ["A", "B"] {
        let mut operation = op(table, "z-new", 2, 128);
        operation.value.expires_at_ms = Some(deadline);
        shared.apply(&operation).await.unwrap();
        shared.flush_from_actor().await.unwrap();
    }
    let mut converted = first.converted;
    for _ in 0..4 {
        let page = shared
            .expire_values_page_partitioned(&mut pass)
            .await
            .unwrap();
        converted += page.converted;
        if page.done {
            break;
        }
    }
    assert!(
        pass.partition_done,
        "insert churn extended the captured pass quota"
    );
    assert_eq!(converted, 3);
    for table in ["A", "B"] {
        let child = shared.table_state(table).await.unwrap();
        assert!(!child.cache.read().unwrap().values[&key(table, "z-new")].is_tombstone);
    }
    shared.flush_from_actor().await.unwrap();
    assert_eq!(shared.expire_values(deadline).await.unwrap(), 2);
}

#[tokio::test]
async fn irreducible_manual_marker_returns_without_repeated_retry_spin() {
    let mut due = VersionedValue::new(vec![9], 1, "writer".into());
    due.expires_at_ms = Some(100);
    let shared = FragmentState::new(
        HashMap::from([(key("A", "due"), due)]),
        Some(store()),
        usize::MAX,
        Duration::from_secs(3600),
    )
    .with_persistence_config(
        PersistenceConfig::builder()
            .max_records(1)
            .max_bytes(1)
            .node_max_bytes(65536)
            .build(),
    )
    .unwrap()
    .enable_partitions()
    .unwrap();
    let revision = shared.current_revision().await;
    let result = tokio::time::timeout(Duration::from_millis(200), shared.expire_values(100))
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(shared.current_revision().await, revision);
    assert_eq!(shared.persistence_backlog().await, (0, 0));
}

#[tokio::test]
async fn standalone_managed_actor_fences_and_drain_flush_every_unpartitioned_child() {
    let backend = store();
    let shared = Arc::new(
        FragmentState::new(
            HashMap::from([
                (
                    key("KV", "loaded"),
                    VersionedValue::new(vec![1], 1, "writer".into()),
                ),
                (
                    key("A", "loaded"),
                    VersionedValue::new(vec![2], 1, "writer".into()),
                ),
            ]),
            Some(backend.clone()),
            usize::MAX,
            Duration::from_secs(3600),
        )
        .with_persistence_config(
            PersistenceConfig::builder()
                .max_records(1)
                .max_bytes(2048)
                .node_max_bytes(65536)
                .build(),
        )
        .unwrap(),
    );
    assert!(!shared.partitioned);
    let pool = PoolConfig::builder().workers(1).queue_cap(16).build();
    let (actor, _) = Actor::spawn(
        None,
        KvActor,
        KvArgs::builder()
            .shared(shared.clone())
            .fragment(crate::shard::FragmentId(0))
            .worker_prefix("unpartitioned-managed-fences")
            .default_pool(pool.clone())
            .high_pool(pool.clone())
            .background_pool(pool)
            .build(),
    )
    .await
    .unwrap();
    for (table, name, version, stable) in [("KV", "one", 2, false), ("A", "one", 3, true)] {
        let result = actor
            .call(
                |reply| KvMessage::ApplyOp {
                    op: op(table, name, version, 128),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(result, CallResult::Success(Ok(ApplyOutcome::Applied)));
        assert_eq!(shared.persistence_backlog().await.0, 1);
        let result = actor
            .call(
                |reply| {
                    if stable {
                        KvMessage::FlushStable { reply }
                    } else {
                        KvMessage::Flush { reply }
                    }
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            CallResult::Success(Ok(ApplyDurability::StoreCommitted))
        );
        assert_eq!(shared.persistence_backlog().await, (0, 0));
    }
    assert_eq!(
        actor
            .call(
                |reply| KvMessage::ApplyOp {
                    op: op("A", "drain", 4, 128),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                Some(Duration::from_secs(2))
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(ApplyOutcome::Applied))
    );
    assert_eq!(shared.persistence_backlog().await.0, 1);
    actor.drain().unwrap();
    actor.wait(None).await.unwrap();
    assert_eq!(shared.persistence_backlog().await, (0, 0));
    assert_eq!(backend.written.lock().unwrap().len(), 3);
    assert!(backend
        .written
        .lock()
        .unwrap()
        .iter()
        .any(
            |(physical, value)| physical.ends_with(key("A", "drain").as_bytes())
                && value.timestamp_ms == 4
        ));
}

#[tokio::test]
async fn standalone_child_fence_cancellation_and_failure_keep_cap_until_actual_completion() {
    for cancelled in [false, true] {
        let gate = Arc::new(GatedStore::default());
        let release = ReleaseMaintenanceStore(gate.clone());
        let shared = Arc::new(
            FragmentState::new(
                HashMap::new(),
                Some(Arc::new(PartitionGatedStore {
                    gate: gate.clone(),
                    fail_first: AtomicBool::new(!cancelled),
                })),
                usize::MAX,
                Duration::from_secs(3600),
            )
            .with_persistence_config(
                PersistenceConfig::builder()
                    .max_records(1)
                    .max_bytes(2048)
                    .node_max_bytes(65536)
                    .build(),
            )
            .unwrap(),
        );
        let pool = PoolConfig::builder().workers(1).queue_cap(16).build();
        let (actor, _) = Actor::spawn(
            None,
            KvActor,
            KvArgs::builder()
                .shared(shared.clone())
                .fragment(crate::shard::FragmentId(0))
                .worker_prefix(format!("standalone-fence-{cancelled}"))
                .default_pool(pool.clone())
                .high_pool(pool.clone())
                .background_pool(pool)
                .build(),
        )
        .await
        .unwrap();
        assert_eq!(
            actor
                .call(
                    |reply| KvMessage::ApplyOp {
                        op: op("A", "one", 1, 128),
                        priority: OpPriority::Default,
                        is_replication: false,
                        reply: Some(reply),
                    },
                    Some(Duration::from_secs(2))
                )
                .await
                .unwrap(),
            CallResult::Success(Ok(ApplyOutcome::Applied))
        );
        let fencing = actor.clone();
        let fence = tokio::spawn(async move {
            fencing
                .call(
                    |reply| KvMessage::Flush { reply },
                    Some(Duration::from_secs(30)),
                )
                .await
        });
        wait_until(|| gate.entered.load(Ordering::Acquire)).await;
        assert_eq!(shared.persistence_backlog().await.0, 1);
        assert_eq!(
            shared.apply_from_actor(&op("B", "one", 1, 128)).await,
            Err(ApplyError::Backpressure)
        );
        assert!(gate.entries.lock().unwrap().is_empty());
        if cancelled {
            fence.abort();
            assert!(fence.await.unwrap_err().is_cancelled());
            assert_eq!(shared.persistence_backlog().await.0, 1);
            drop(release);
            shared.executor.drain().await;
        } else {
            drop(release);
            assert!(matches!(
                fence.await.unwrap().unwrap(),
                CallResult::Success(Err(ApplyError::Storage(_)))
            ));
            assert_eq!(shared.persistence_backlog().await.0, 1);
            assert_eq!(actor.get_status(), ractor::ActorStatus::Running);
            assert_eq!(
                actor
                    .call(
                        |reply| KvMessage::Flush { reply },
                        Some(Duration::from_secs(2))
                    )
                    .await
                    .unwrap(),
                CallResult::Success(Ok(ApplyDurability::StoreCommitted))
            );
        }
        assert_eq!(shared.persistence_backlog().await, (0, 0));
        assert_eq!(
            shared
                .get_from_actor(&key("A", "one"))
                .await
                .unwrap()
                .timestamp_ms,
            1
        );
        assert!(shared.get_from_actor(&key("B", "one")).await.is_none());
        actor.drain().unwrap();
        actor.wait(None).await.unwrap();
        assert_eq!(gate.entries.lock().unwrap().len(), 1);
    }
}
