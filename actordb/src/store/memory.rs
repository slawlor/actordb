// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! No-op storage test double for exercising persistence plumbing.
//!
//! [`MemoryStore`] records nothing and provides no durability. Storeless
//! production nodes should use `NodeConfig.store = None` instead.

use crate::actor::VersionedValue;

use super::{KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, StoreCommitDurability, StoreError};
use super::{StoreBinding, StoreIdentity};

/// Test double for [`KvStore`]: records nothing, loads nothing.
///
/// Exists so tests can exercise the actor's persistence plumbing
/// (buffering, batching, flush-on-shutdown) without a database.
/// Not for production use — a node that doesn't need durability
/// should use `store: None`.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryStore;

impl KvStore for MemoryStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }

    fn write_batch_borrowed(
        &self,
        _prefix: &[u8],
        _writes: &[(&[u8], &VersionedValue)],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Buffered)
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Retained
    }

    fn delete_prefix(&self, _prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_is_a_noop() {
        let s = MemoryStore;
        assert_eq!(s.commit_durability(), StoreCommitDurability::Buffered);
        assert_eq!(s.stable_fence().unwrap(), StoreCommitDurability::Buffered);
        assert_eq!(s.prefix_delete_support(), PrefixDeleteSupport::Retained);
        assert!(s.load().unwrap().is_empty());
        s.write_batch(&[(
            b"k".to_vec(),
            VersionedValue::new(vec![1], 1, "n".to_string()),
        )])
        .unwrap();
        assert!(s.load().unwrap().is_empty());
    }
}
