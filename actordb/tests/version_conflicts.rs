use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use actordb::actor::{
    now_ms, ApplyError, ApplyOutcome, FragmentState, RecordVersion, ReplicatedOp, VersionedValue,
};
use actordb::node::{Node, NodeConfig, NodeError};
use actordb::store::{KvStore, StoreError};

#[derive(Default)]
struct RecordingStore {
    entries: Mutex<Vec<(Vec<u8>, VersionedValue)>>,
    writes: AtomicUsize,
}
impl KvStore for RecordingStore {
    fn bind(
        &self,
        _identity: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
        Ok(actordb::store::StoreBinding::unmanaged())
    }

    fn load_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, actordb::actor::VersionedValue)>, actordb::store::StoreError> {
        // This fixture owns a bounded in-memory dataset, not a production prefix scan.
        Ok(self
            .load()?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect())
    }

    fn commit_durability(&self) -> actordb::store::StoreCommitDurability {
        actordb::store::StoreCommitDurability::Buffered
    }

    fn stable_fence(
        &self,
    ) -> Result<actordb::store::StoreCommitDurability, actordb::store::StoreError> {
        Ok(self.commit_durability())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self.entries.lock().unwrap().clone())
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut entries = self.entries.lock().unwrap();
        for (key, value) in writes {
            entries.retain(|(existing, _)| existing != key);
            entries.push((key.clone(), value.clone()));
        }
        Ok(())
    }
}
fn op(value: VersionedValue) -> ReplicatedOp {
    ReplicatedOp {
        key: "key".into(),
        value,
        seq: 1,
        boot_id: 1,
    }
}
fn state(store: Option<Arc<dyn KvStore>>) -> FragmentState {
    FragmentState::new(HashMap::new(), store, 100, Duration::from_secs(3600))
}
fn live() -> VersionedValue {
    VersionedValue::new_version(
        vec![1, 2],
        RecordVersion::new(now_ms(), 17),
        "writer".into(),
    )
}

#[tokio::test]
async fn reused_versions_reject_payload_and_ttl_changes_before_cache_or_persistence() {
    for durable in [false, true] {
        let store = Arc::new(RecordingStore::default());
        let state = state(durable.then(|| store.clone() as Arc<dyn KvStore>));
        let first = op(live());
        assert_eq!(state.apply(&first).await.unwrap(), ApplyOutcome::Applied);
        let revision = state.current_revision().await;
        let summary = state.summary_page(None, None, None, 10).await;
        for conflict in [
            VersionedValue {
                value: vec![9],
                ..first.value.clone()
            },
            VersionedValue {
                expires_at_ms: Some(now_ms() + 60_000),
                ..first.value.clone()
            },
        ] {
            assert_eq!(
                state.apply(&op(conflict)).await,
                Err(ApplyError::VersionConflict { key: "key".into() })
            );
            assert_eq!(state.get("key").await, Some(first.value.clone()));
            assert_eq!(state.current_revision().await, revision);
            assert_eq!(
                state
                    .summary_page(None, None, Some(summary.snapshot_time_ms), 10)
                    .await,
                summary
            );
        }
        let mut replay = first.clone();
        replay.seq = 22;
        replay.boot_id = 88;
        assert_eq!(
            state.apply(&replay).await.unwrap(),
            ApplyOutcome::AlreadyPresent
        );
        state.flush().await.unwrap();
        if durable {
            assert_eq!(
                store.entries.lock().unwrap().as_slice(),
                &[(b"key".to_vec(), first.value)]
            );
            assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn opposite_order_conflicts_remain_explicit_and_summaries_detect_divergence() {
    let a = state(None);
    let b = state(None);
    let first = op(live());
    let second = op(VersionedValue {
        value: vec![9],
        ..first.value.clone()
    });
    a.apply(&first).await.unwrap();
    b.apply(&second).await.unwrap();
    assert!(matches!(
        a.apply(&second).await,
        Err(ApplyError::VersionConflict { .. })
    ));
    assert!(matches!(
        b.apply(&first).await,
        Err(ApplyError::VersionConflict { .. })
    ));
    let a = a
        .summary_page(None, None, None, 1)
        .await
        .summaries
        .remove(0);
    let b = b
        .summary_page(None, None, None, 1)
        .await
        .summaries
        .remove(0);
    assert_eq!(
        (a.timestamp_ms, a.logical_counter, a.node_id, a.is_tombstone),
        (b.timestamp_ms, b.logical_counter, b.node_id, b.is_tombstone)
    );
    assert_ne!(a.content_digest, b.content_digest);
}

#[tokio::test]
async fn deletes_expiry_and_later_versions_still_converge() {
    let a = state(None);
    let b = state(None);
    let mut value = live();
    value.expires_at_ms = Some(now_ms() + 60_000);
    let first = op(value.clone());
    a.apply(&first).await.unwrap();
    b.apply(&first).await.unwrap();
    let deadline = value.expires_at_ms.unwrap();
    // One replica synthesizes expiry in its summary, the other installs it.
    let expected = a
        .summary_page(None, None, Some(deadline), 1)
        .await
        .summaries;
    b.expire_values(deadline).await.unwrap();
    assert_eq!(
        b.summary_page(None, None, Some(deadline), 1)
            .await
            .summaries,
        expected
    );
    a.apply(&op(value.expiry_tombstone())).await.unwrap();
    assert_eq!(
        a.summary_page(None, None, Some(deadline), 1)
            .await
            .summaries,
        expected
    );
    // Replay of the live version cannot resurrect either tombstone.
    assert!(matches!(
        a.apply(&first).await.unwrap(),
        ApplyOutcome::Superseded { .. }
    ));
    assert!(matches!(
        b.apply(&first).await.unwrap(),
        ApplyOutcome::Superseded { .. }
    ));
    let mut later = value;
    later.logical_counter += 1;
    later.expires_at_ms = None;
    let later = op(later);
    a.apply(&later).await.unwrap();
    b.apply(&later).await.unwrap();
    let delete = op(VersionedValue::tombstone_version(
        later.value.version(),
        later.value.node_id,
    ));
    a.apply(&delete).await.unwrap();
    b.apply(&delete).await.unwrap();
    assert_eq!(
        a.get_versioned("key", Some(deadline)).await,
        b.get_versioned("key", Some(deadline)).await
    );
    assert!(a.get("key").await.is_none());
}

#[tokio::test]
async fn startup_rejects_duplicate_persisted_keys_without_overwriting_store() {
    let first = live();
    let physical_key = [0u32.to_be_bytes().as_slice(), b"key"].concat();
    let store = Arc::new(RecordingStore {
        entries: Mutex::new(vec![
            (physical_key.clone(), first.clone()),
            (
                physical_key,
                VersionedValue {
                    value: vec![9],
                    ..first
                },
            ),
        ]),
        ..Default::default()
    });
    let result = Node::start(
        "restored".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder().store(store.clone()).build(),
    )
    .await;
    assert!(matches!(result, Err(NodeError::Storage(error)) if error.contains("duplicate key")));
    assert_eq!(store.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn zero_expiry_is_rejected_before_lossy_peer_encoding() {
    let state = state(None);
    let mut value = live();
    value.expires_at_ms = Some(0);
    assert!(matches!(
        state.apply(&op(value)).await,
        Err(ApplyError::InvalidInput(_))
    ));
    assert_eq!(state.current_revision().await, 0);
    assert!(state.get("key").await.is_none());
}
