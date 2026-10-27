//! Flush ownership and namespace delegation without transient full-value copies.
use super::*;
use crate::store::{PrefixedStore, StoreBinding, StoreError, StoreIdentity};
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct BorrowingBackend {
    payload: AtomicUsize,
    fail: AtomicBool,
    calls: AtomicUsize,
    expected_key: Vec<u8>,
}
impl KvStore for BorrowingBackend {
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
        panic!("flush must dispatch its borrowed view")
    }
    fn write_batch_borrowed(
        &self,
        prefix: &[u8],
        writes: &[(&[u8], &VersionedValue)],
    ) -> Result<(), StoreError> {
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].1.value.as_ptr() as usize,
            self.payload.load(Ordering::Acquire)
        );
        assert_eq!([prefix, writes[0].0].concat(), self.expected_key);
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail.swap(false, Ordering::AcqRel) {
            Err(StoreError("controlled borrowed write failure".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn named_flush_borrows_authoritative_payload_and_retains_exact_failure_retry() {
    let key = crate::table_key::encode_table_key("USERS", "wide");
    let backend = Arc::new(BorrowingBackend {
        payload: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
        expected_key: [
            0u32.to_be_bytes().as_slice(),
            crate::table_key::table_storage_prefix("USERS").as_slice(),
            key.as_bytes(),
        ]
        .concat(),
    });
    let store = Arc::new(PrefixedStore::new(backend.clone(), FragmentId(0)));
    let state = FragmentState::new(
        HashMap::new(),
        Some(store),
        usize::MAX,
        Duration::from_secs(3600),
    )
    .enable_partitions()
    .unwrap();
    let operation = ReplicatedOp {
        key: key.clone(),
        value: VersionedValue::new(vec![7; 8 * 1024 * 1024], 10, "writer".into()),
        seq: 1,
        boot_id: 1,
    };
    assert_eq!(
        state.apply(&operation).await.unwrap(),
        ApplyOutcome::Applied
    );
    let child = state
        .table_children
        .read()
        .await
        .get("USERS")
        .unwrap()
        .clone();
    let pointer = child.flush.lock().await.pending[&key].value.value.as_ptr() as usize;
    backend.payload.store(pointer, Ordering::Release);
    let charge = state.persistence.aggregate.used();
    assert!(charge > 16 * 1024 * 1024);
    assert!(state.flush_stable().await.is_err());
    assert_eq!(state.persistence.aggregate.used(), charge);
    assert_eq!(state.get(&key).await.unwrap().value, operation.value.value);
    assert_eq!(
        state.flush_stable().await.unwrap(),
        ApplyDurability::StableCommitted
    );
    assert_eq!(state.persistence.aggregate.used(), 0);
    assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
}
