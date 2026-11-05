mod common;

use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::replication::{RegionalReplicationOptions, ReplicationAckDurability, SenderConfig};
use actordb::shard::{fragment_of_literal, FragmentId};
use actordb::store::{KvStore, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
struct FenceGate {
    blocked: Mutex<bool>,
    released: Condvar,
    active: AtomicUsize,
}
impl FenceGate {
    fn arm(&self) {
        *self.blocked.lock().unwrap() = true;
    }
    fn release(&self) {
        *self.blocked.lock().unwrap() = false;
        self.released.notify_all();
    }
    fn fence(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
        let mut blocked = self.blocked.lock().unwrap();
        while *blocked {
            blocked = self.released.wait(blocked).unwrap();
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct UnblockOnDrop(Arc<FenceGate>);
impl Drop for UnblockOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct TestStore {
    gate: Arc<FenceGate>,
    values: Mutex<HashMap<Vec<u8>, VersionedValue>>,
}
impl KvStore for TestStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .load()?
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect())
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.values.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }
    // Test-only stable boundary: deliberately held to exercise ACK ownership.
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.gate.fence();
        Ok(StoreCommitDurability::Stable)
    }
}

fn config(store: Option<Arc<dyn KvStore>>) -> NodeConfig {
    NodeConfig::builder()
        .fragment_count(4)
        .fragment_map(
            (0..4)
                .map(|id| {
                    (
                        FragmentId(id),
                        vec![
                            "source".into(),
                            "local".into(),
                            "remote-a".into(),
                            "remote-b".into(),
                        ],
                    )
                })
                .collect(),
        )
        .maybe_store(store)
        .build()
}
fn key_for(fragment: u32) -> String {
    (0..)
        .map(|i| format!("regional-{fragment}-{i}"))
        .find(|key| fragment_of_literal(key, 4) == FragmentId(fragment))
        .unwrap()
}
async fn all_visible(node: &Node, keys: &[String]) -> bool {
    for key in keys {
        if node.get(key.clone()).await.ok().flatten().is_none() {
            return false;
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wan_batches_and_rounds_share_caps_across_peers_without_blocking_local_replication() {
    let gate = Arc::new(FenceGate::default());
    let _release_on_failure = UnblockOnDrop(gate.clone());
    let regions = HashMap::from([
        ("source".into(), "west".into()),
        ("local".into(), "west".into()),
        ("remote-a".into(), "east".into()),
        ("remote-b".into(), "north".into()),
    ]);
    let profile = |durability| {
        SenderConfig::builder()
            .batch_size(1)
            .max_in_flight(4)
            .batch_interval(Duration::from_millis(1))
            .required_ack_durability(durability)
            .build()
    };
    let runtime = NodeRuntimeOptions::builder()
        .regions(regions)
        .regional_replication(
            RegionalReplicationOptions::builder()
                .in_region(profile(ReplicationAckDurability::MemoryApplied))
                .cross_region(profile(ReplicationAckDurability::StableCommitted))
                .cross_region_max_in_flight(1)
                .cross_region_max_reconcile_rounds(1)
                .build(),
        )
        .build();
    let source = Node::start_with_options(
        "source".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(None),
        runtime,
    )
    .await
    .unwrap();
    let local = Node::start("local".into(), "127.0.0.1:0".parse().unwrap(), config(None))
        .await
        .unwrap();
    let store = || {
        Arc::new(TestStore {
            gate: gate.clone(),
            values: Mutex::new(HashMap::new()),
        }) as Arc<dyn KvStore>
    };
    let remote_a = Node::start(
        "remote-a".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(Some(store())),
    )
    .await
    .unwrap();
    let remote_b = Node::start(
        "remote-b".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(Some(store())),
    )
    .await
    .unwrap();
    gate.arm();
    for node in [&local, &remote_a, &remote_b] {
        node.connect(source.addr()).await.unwrap();
    }
    common::wait_for(
        || async {
            let usage = source.regional_replication_usage().unwrap();
            usage.active_reconcile_rounds == 1
                && usage.waiting_reconcile_senders > 0
                && gate.active.load(Ordering::SeqCst) == 1
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        source
            .regional_replication_usage()
            .unwrap()
            .in_flight_batches,
        0,
        "reconciliation barriers must use the separate round budget"
    );
    common::wait_for(
        || async {
            source
                .sender_report(FragmentId(0), "local")
                .await
                .is_some_and(|r| r.reconcile_rounds > 0)
        },
        Duration::from_secs(5),
    )
    .await;
    gate.release();
    common::wait_for(
        || async {
            for fragment in 0..4 {
                for peer in ["remote-a", "remote-b"] {
                    if !source
                        .sender_report(FragmentId(fragment), peer)
                        .await
                        .is_some_and(|r| r.reconcile_rounds > 0)
                    {
                        return false;
                    }
                }
            }
            let usage = source.regional_replication_usage().unwrap();
            usage.active_reconcile_rounds == 0 && usage.waiting_reconcile_senders == 0
        },
        Duration::from_secs(5),
    )
    .await;
    gate.arm();
    let keys: Vec<String> = (0..4).map(key_for).collect();
    for key in &keys {
        source.put(key.clone(), vec![7]).await.unwrap();
    }
    common::wait_for(
        || async {
            let usage = source.regional_replication_usage().unwrap();
            usage.in_flight_batches == 1
                && usage.waiting_batch_senders > 0
                && gate.active.load(Ordering::SeqCst) == 1
        },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(|| all_visible(&local, &keys), Duration::from_secs(5)).await;
    assert_eq!(
        source
            .regional_replication_usage()
            .unwrap()
            .in_flight_batches,
        1
    );
    gate.release();
    common::wait_for(
        || async { all_visible(&remote_a, &keys).await && all_visible(&remote_b, &keys).await },
        Duration::from_secs(5),
    )
    .await;
    common::wait_for(
        || async {
            let usage = source.regional_replication_usage().unwrap();
            usage.in_flight_batches == 0 && usage.waiting_batch_senders == 0
        },
        Duration::from_secs(5),
    )
    .await;
    // Local shutdown must still process sender controls while WAN capacity is
    // saturated; it releases current sender ownership without waiting for a
    // remote stable fence that belongs to another Node.
    gate.arm();
    for key in &keys {
        source.put(key.clone(), vec![8]).await.unwrap();
    }
    common::wait_for(
        || async {
            let usage = source.regional_replication_usage().unwrap();
            usage.in_flight_batches == 1 && usage.waiting_batch_senders > 0
        },
        Duration::from_secs(5),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), source.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source
            .regional_replication_usage()
            .unwrap()
            .in_flight_batches,
        0
    );
    assert_eq!(
        source
            .regional_replication_usage()
            .unwrap()
            .waiting_batch_senders,
        0
    );
    gate.release();
    for node in [local, remote_a, remote_b] {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_regional_profiles_fail_before_node_startup() {
    let invalid = RegionalReplicationOptions::builder()
        .cross_region(SenderConfig::builder().max_in_flight(0).build())
        .build();
    assert!(Node::start_with_options(
        "invalid".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_regional_replication(invalid)
    )
    .await
    .is_err());
}
