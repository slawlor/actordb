//! Explicit full-outage recovery preserves the union of durable version floors.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig, RecoveryOptions};
use actordb::placement::{FragmentPlacementState, ManualPlacement};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb::store::{KvStore, StoreCommitDurability, StoreError};

#[derive(Default)]
struct StableFixture(
    Mutex<HashMap<Vec<u8>, VersionedValue>>,
    std::sync::atomic::AtomicBool,
);
impl StableFixture {
    fn insert(&self, key: &str, value: VersionedValue) {
        let encoded: String = key
            .as_bytes()
            .iter()
            .flat_map(|byte| {
                let digits = b"0123456789abcdef";
                [digits[(byte >> 4) as usize], digits[(byte & 0x0f) as usize]]
            })
            .map(char::from)
            .collect();
        let logical = format!("\u{001e}actordb.table.v1:4b56:{encoded}");
        let physical = format!("\u{001d}actordb.table.store.v1:4b56:{logical}");
        let key = [0u32.to_be_bytes().as_slice(), physical.as_bytes()].concat();
        self.0.lock().unwrap().insert(key, value);
    }
}
impl KvStore for StableFixture {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.0.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        if self.1.load(std::sync::atomic::Ordering::Acquire) {
            Err(StoreError("fixture stable fence failed".into()))
        } else {
            Ok(StoreCommitDurability::Stable)
        }
    }
}

fn config(
    store: Arc<StableFixture>,
    placement: Arc<ManualPlacement>,
    epoch: Option<[u8; 32]>,
) -> NodeConfig {
    NodeConfig::builder()
        .store(store)
        .placement_controller(placement)
        .maybe_recovery(epoch.map(|epoch| RecoveryOptions::new(epoch).unwrap()))
        .flush_batch_size(1)
        .clean_interval(Duration::from_secs(3600))
        .snapshot_interval(Duration::from_secs(3600))
        .replication(
            SenderConfig::builder()
                .batch_interval(Duration::from_millis(1))
                .build(),
        )
        .build()
}

async fn start(id: &str, config: NodeConfig) -> Node {
    Node::start(id.into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap()
}

#[tokio::test]
async fn all_restarted_replicas_recover_union_and_tombstones_before_serving() {
    let placement = Arc::new(ManualPlacement::new(HashMap::from([(
        FragmentId(0),
        vec!["a".into(), "b".into()],
    )])));
    let a_store = Arc::new(StableFixture::default());
    let b_store = Arc::new(StableFixture::default());
    a_store.insert(
        "a-only",
        VersionedValue::new(b"a".to_vec(), 100, "a".into()),
    );
    a_store.insert("deleted", VersionedValue::tombstone(200, "a".into()));
    a_store.insert(
        "winner",
        VersionedValue::new(b"old".to_vec(), 100, "a".into()),
    );
    b_store.insert(
        "b-only",
        VersionedValue::new(b"b".to_vec(), 100, "b".into()),
    );
    b_store.insert(
        "deleted",
        VersionedValue::new(b"stale".to_vec(), 150, "b".into()),
    );
    b_store.insert(
        "winner",
        VersionedValue::new(b"new".to_vec(), 101, "b".into()),
    );
    let a = start(
        "a",
        config(a_store.clone(), placement.clone(), Some([7; 32])),
    )
    .await;
    let b = start("b", config(b_store.clone(), placement, Some([7; 32]))).await;
    assert_ne!(
        a.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Serving)
    );
    assert_ne!(
        b.placement_report().await.states.get(&FragmentId(0)),
        Some(&FragmentPlacementState::Serving)
    );
    a.connect(b.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if a.placement_report().await.states.get(&FragmentId(0))
                == Some(&FragmentPlacementState::Serving)
                && b.placement_report().await.states.get(&FragmentId(0))
                    == Some(&FragmentPlacementState::Serving)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for node in [&a, &b] {
        assert_eq!(
            node.get("a-only".into()).await.unwrap().unwrap().value,
            b"a"
        );
        assert_eq!(
            node.get("b-only".into()).await.unwrap().unwrap().value,
            b"b"
        );
        assert_eq!(
            node.get("winner".into()).await.unwrap().unwrap().value,
            b"new"
        );
        assert!(node.get("deleted".into()).await.unwrap().is_none());
    }
    for store in [a_store, b_store] {
        let durable = store.load().unwrap();
        assert_eq!(durable.len(), 4);
        assert!(durable
            .iter()
            .any(|(key, value)| key.ends_with(b"64656c65746564")
                && value.is_tombstone
                && value.timestamp_ms == 200));
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn unapproved_or_mismatched_recovery_epochs_cannot_certify_nonserving_copies() {
    for epochs in [(None, None), (Some([1; 32]), Some([2; 32]))] {
        let placement = Arc::new(ManualPlacement::new(HashMap::from([(
            FragmentId(0),
            vec!["a".into(), "b".into()],
        )])));
        let a = start(
            "a",
            config(
                Arc::new(StableFixture::default()),
                placement.clone(),
                epochs.0,
            ),
        )
        .await;
        let b = start(
            "b",
            config(Arc::new(StableFixture::default()), placement, epochs.1),
        )
        .await;
        a.connect(b.addr()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while a.peer_count().await != 1 || b.peer_count().await != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        for node in [&a, &b] {
            assert_ne!(
                node.placement_report().await.states.get(&FragmentId(0)),
                Some(&FragmentPlacementState::Serving)
            );
        }
        tokio::time::resume();
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn missing_replica_or_failed_stable_fence_prevents_recovery_authority() {
    for missing in [true, false] {
        let mut replicas = vec!["a".into(), "b".into()];
        if missing {
            replicas.push("missing".into());
        }
        let placement = Arc::new(ManualPlacement::new(HashMap::from([(
            FragmentId(0),
            replicas,
        )])));
        let a_store = Arc::new(StableFixture::default());
        a_store
            .1
            .store(!missing, std::sync::atomic::Ordering::Release);
        let a = start("a", config(a_store, placement.clone(), Some([9; 32]))).await;
        let b = start(
            "b",
            config(Arc::new(StableFixture::default()), placement, Some([9; 32])),
        )
        .await;
        a.connect(b.addr()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while a.peer_count().await != 1 || b.peer_count().await != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_ne!(
            a.placement_report().await.states.get(&FragmentId(0)),
            Some(&FragmentPlacementState::Serving)
        );
        assert_ne!(
            b.placement_report().await.states.get(&FragmentId(0)),
            Some(&FragmentPlacementState::Serving)
        );
        tokio::time::resume();
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
