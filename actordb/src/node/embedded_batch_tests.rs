// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Internal batch pacing without changing external nonwaiting admission.
use super::*;
use crate::actor::{OpPriority, VersionedValue};
use crate::node::{Node, NodeConfig, NodeRuntimeOptions};
use crate::store::{KvStore, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

struct SlowStore {
    delay: Duration,
    writes: AtomicUsize,
    values: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}
impl KvStore for SlowStore {
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
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        std::thread::sleep(self.delay);
        self.writes.fetch_add(1, Ordering::Release);
        self.values.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
}

async fn batch_node(capacity: usize, slow_store: bool) -> Arc<Node> {
    let mut config = NodeConfig {
        flush_batch_size: 1,
        snapshot_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    if slow_store {
        config.store = Some(Arc::new(SlowStore {
            delay: Duration::from_millis(1),
            writes: AtomicUsize::new(0),
            values: StdMutex::new(HashMap::new()),
        }));
    }
    Arc::new(
        Node::start_with_options(
            format!("batch-pacing-{capacity}-{slow_store}"),
            "127.0.0.1:0".parse().unwrap(),
            config,
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(capacity)
                .build(),
        )
        .await
        .unwrap(),
    )
}

async fn settle(node: &Node) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let diagnostics = node.embedded_operation_diagnostics();
            if diagnostics.active_requests() == 0 && diagnostics.active_mutation_completions() == 0
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_1024_point_batches_do_not_shed_their_own_completion_capacity() {
    for slow_store in [false, true] {
        for capacity in [1, 2, 8, 128] {
            let node = batch_node(capacity, slow_store).await;
            let values: Vec<_> = (0..1024)
                .map(|index| (format!("key-{index:04}"), vec![(index % 251) as u8]))
                .collect();
            let results = tokio::time::timeout(
                Duration::from_secs(10),
                node.put_multi_with_priority(values.clone(), OpPriority::Default),
            )
            .await
            .unwrap();
            assert_eq!(results.len(), values.len());
            assert!(
                results.iter().all(Result::is_ok),
                "capacity={capacity}, slow_store={slow_store}: {:?}",
                results
                    .iter()
                    .filter(|result| result.is_err())
                    .collect::<Vec<_>>()
            );
            settle(&node).await;
            for (key, expected) in [values[0].clone(), values[511].clone(), values[1023].clone()] {
                assert_eq!(node.get(key).await.unwrap().unwrap().value, expected);
            }
            assert_eq!(
                node.embedded_operation_diagnostics()
                    .active_mutation_completions(),
                0
            );
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn competing_batches_progress_fairly_without_reserved_per_batch_completion_slots() {
    for capacity in [2, 8] {
        let node = batch_node(capacity, true).await;
        let batch_a = (0..128)
            .map(|index| (format!("a-{index}"), vec![1]))
            .collect();
        let batch_b = (0..128)
            .map(|index| (format!("b-{index}"), vec![2]))
            .collect();
        let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                node.put_multi_with_priority(batch_a, OpPriority::Default),
                node.put_multi_with_priority(batch_b, OpPriority::Default)
            )
        })
        .await
        .unwrap();
        assert_eq!(a.len(), 128);
        assert_eq!(b.len(), 128);
        assert!(a.iter().all(Result::is_ok));
        assert!(b.iter().all(Result::is_ok));
        settle(&node).await;
        assert_eq!(node.get("a-127".into()).await.unwrap().unwrap().value, [1]);
        assert_eq!(node.get("b-127".into()).await.unwrap().unwrap().value, [2]);
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn duplicate_input_positions_keep_ordered_results_and_increasing_version_floor() {
    for capacity in [1, 2, 8] {
        let node = batch_node(capacity, true).await;
        let results = node
            .put_multi_with_priority(
                (0..32)
                    .map(|index| ("same-key".into(), vec![index]))
                    .collect(),
                OpPriority::Default,
            )
            .await;
        assert_eq!(results.len(), 32);
        assert!(results
            .iter()
            .all(|result| result.is_ok() || matches!(result, Err(NodeError::Superseded { .. }))));
        settle(&node).await;
        let winner = node.get("same-key".into()).await.unwrap().unwrap();
        assert_eq!(winner.value, [31]);
        node.put("same-key".into(), vec![32]).await.unwrap();
        let later = node.get("same-key".into()).await.unwrap().unwrap();
        assert!(later.wins_over(&winner));
        assert_eq!(later.value, [32]);
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn paced_wait_is_fifo_while_external_point_and_request_admission_remain_nonwaiting() {
    let operations = EmbeddedOperations::new(2, 4096);
    let mut initial = operations
        .completions
        .clone()
        .acquire_many_owned(2)
        .await
        .unwrap();
    let other_slot = initial.split(1).unwrap();
    let count = operations
        .count
        .clone()
        .acquire_many_owned(2)
        .await
        .unwrap();
    assert!(matches!(
        operations.admit_batch_owned(1, None),
        Err(NodeError::Backpressure(_))
    ));
    assert!(matches!(
        operations.admit_owned(1, None),
        Err(NodeError::Backpressure(_))
    ));
    drop(count);
    let retained_bytes = operations
        .bytes
        .clone()
        .acquire_many_owned(4096)
        .await
        .unwrap();
    assert!(matches!(
        operations.admit_batch_owned(1, None),
        Err(NodeError::Backpressure(_))
    ));
    assert!(matches!(
        operations.admit_owned(1, None),
        Err(NodeError::Backpressure(_))
    ));
    assert_eq!(operations.diagnostics().active_requests(), 0);
    drop(retained_bytes);
    let ran = Arc::new(AtomicUsize::new(0));
    let (release_first, first_release) = oneshot::channel();
    let (first_running, first_started) = oneshot::channel();
    let first_operations = operations.clone();
    let first_ran = ran.clone();
    let first = tokio::spawn(async move {
        first_operations
            .submit_paced(
                async move {
                    first_ran.fetch_add(1, Ordering::AcqRel);
                    let _ = first_running.send(());
                    let _ = first_release.await;
                },
                Duration::from_secs(1),
            )
            .await
    });
    tokio::task::yield_now().await;
    let second_operations = operations.clone();
    let second_ran = ran.clone();
    let second = tokio::spawn(async move {
        second_operations
            .submit_paced(
                async move {
                    second_ran.fetch_add(1, Ordering::AcqRel);
                },
                Duration::from_secs(1),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert!(matches!(
        operations.submit(async {}),
        Err(NodeError::Backpressure(_))
    ));
    drop(initial);
    first.await.unwrap().unwrap();
    first_started.await.unwrap();
    assert!(
        !second.is_finished(),
        "a later FIFO waiter bypassed the first running job"
    );
    release_first.send(()).unwrap();
    second.await.unwrap().unwrap();
    drop(other_slot);
    operations.shutdown().await;
    assert_eq!(ran.load(Ordering::Acquire), 2);
    assert_eq!(operations.diagnostics().active_mutation_completions(), 0);
}

#[tokio::test]
async fn blocked_paced_admission_times_out_or_cancels_without_submitting_work() {
    let operations = EmbeddedOperations::new(1, 4096);
    let held = operations
        .completions
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let ran_after_timeout = ran.clone();
    assert!(matches!(
        operations
            .submit_paced(
                async move {
                    ran_after_timeout.store(true, Ordering::Release);
                },
                Duration::from_millis(10)
            )
            .await,
        Err(NodeError::Backpressure(_))
    ));
    let request = operations.admit_batch_owned(1, None).unwrap();
    let submitting = operations.clone();
    let ran_after_cancel = ran.clone();
    let waiting = tokio::spawn(async move {
        submitting
            .submit_paced(
                async move {
                    let _request = request;
                    ran_after_cancel.store(true, Ordering::Release);
                },
                Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(operations.diagnostics().active_requests(), 1);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(operations.diagnostics().active_requests(), 0);
    assert!(!ran.load(Ordering::Acquire));
    assert_eq!(operations.queued(), 0);
    drop(held);
    operations.shutdown().await;
}

#[tokio::test]
async fn shutdown_wakes_unsubmitted_waiters_and_joins_accepted_paced_work() {
    let operations = EmbeddedOperations::new(1, 4096);
    let started = Arc::new(Notify::new());
    let started_job = started.clone();
    let (release, released) = oneshot::channel();
    operations
        .submit_paced(
            async move {
                started_job.notify_one();
                let _ = released.await;
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    started.notified().await;
    let ran = Arc::new(AtomicBool::new(false));
    let waiting_operations = operations.clone();
    let waiting_ran = ran.clone();
    let waiter = tokio::spawn(async move {
        waiting_operations
            .submit_paced(
                async move {
                    waiting_ran.store(true, Ordering::Release);
                },
                Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;
    let shutdown_operations = operations.clone();
    let shutdown = tokio::spawn(async move { shutdown_operations.shutdown().await });
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .unwrap()
            .unwrap(),
        Err(NodeError::ShuttingDown)
    ));
    assert!(!shutdown.is_finished());
    assert!(!ran.load(Ordering::Acquire));
    release.send(()).unwrap();
    shutdown.await.unwrap();
    assert_eq!(operations.diagnostics().active_mutation_completions(), 0);
}
