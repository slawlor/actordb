// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Fragment namespace adapter for any [`KvStore`].
//!
//! [`PrefixedStore`] delegates backend capabilities while translating logical
//! keys to the shared four-byte fragment prefix format. It is a fragment view,
//! not a backend that can be attached directly to a node.

use std::sync::Arc;

use crate::actor::VersionedValue;
use crate::shard::FragmentId;

use super::{
    KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, StoreBinding, StoreCommitDurability,
    StoreError, StoreIdentity,
};

/// A [`KvStore`] view namespaced to one fragment.
///
/// Keys are prefixed with the fragment id (`frag_id` as big-endian
/// u32) before hitting the inner store, and the prefix is stripped on
/// load. Lets N fragment actors share one underlying store (e.g. one
/// RocksDB) without key collisions, and makes per-fragment operations
/// such as bounded load and durable namespace deletion possible.
pub struct PrefixedStore {
    inner: Arc<dyn KvStore>,
    prefix: [u8; 4],
}

/// A table-local durable view over a fragment store.
///
/// Each table receives its own physical prefix and therefore its own cache,
/// flush buffer, and ordered iteration group while retaining the node's
/// shared backend handle and replication transport.
pub struct TableStore {
    inner: Arc<dyn KvStore>,
    prefix: Vec<u8>,
}

impl TableStore {
    /// Create a table partition over an already fragment-scoped store.
    pub fn new(inner: Arc<dyn KvStore>, table: &str) -> Self {
        Self {
            inner,
            prefix: crate::table_key::table_storage_prefix(table),
        }
    }

    fn prefixed(&self, key: &[u8]) -> Vec<u8> {
        let mut prefixed = Vec::with_capacity(self.prefix.len() + key.len());
        prefixed.extend_from_slice(&self.prefix);
        prefixed.extend_from_slice(key);
        prefixed
    }
}

impl KvStore for TableStore {
    fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Err(StoreError(
            "TableStore is a table view and cannot be attached as a Node base store".to_string(),
        ))
    }

    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_prefix(&[])
    }

    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        let physical_prefix = self.prefixed(prefix);
        let mut out = Vec::new();
        for (key, value) in self.inner.load_prefix(&physical_prefix)? {
            if let Some(stripped) = key.strip_prefix(self.prefix.as_slice()) {
                out.push((stripped.to_vec(), value));
            }
        }
        Ok(out)
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        self.inner.commit_durability()
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.inner.stable_fence()
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let prefixed: Vec<_> = writes
            .iter()
            .map(|(key, value)| (self.prefixed(key), value.clone()))
            .collect();
        self.inner.write_batch(&prefixed)
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        self.inner.prefix_delete_support()
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        self.inner.delete_prefix(&self.prefixed(prefix))
    }
}

impl PrefixedStore {
    /// Wrap `inner`, namespacing all keys to `fragment`.
    pub fn new(inner: Arc<dyn KvStore>, fragment: FragmentId) -> Self {
        Self {
            inner,
            prefix: fragment.0.to_be_bytes(),
        }
    }
}

impl KvStore for PrefixedStore {
    fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Err(StoreError(
            "PrefixedStore is a fragment view and cannot be attached as a Node base store"
                .to_string(),
        ))
    }

    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_prefix(&[])
    }

    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        let mut physical_prefix = self.prefix.to_vec();
        physical_prefix.extend_from_slice(prefix);
        let mut out = Vec::new();
        for (key, value) in self.inner.load_prefix(&physical_prefix)? {
            if let Some(stripped) = key.strip_prefix(self.prefix.as_slice()) {
                out.push((stripped.to_vec(), value));
            }
        }
        Ok(out)
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        self.inner.commit_durability()
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.inner.stable_fence()
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let prefixed: Vec<(Vec<u8>, VersionedValue)> = writes
            .iter()
            .map(|(k, v)| {
                let mut buf = Vec::with_capacity(4 + k.len());
                buf.extend_from_slice(&self.prefix);
                buf.extend_from_slice(k);
                (buf, v.clone())
            })
            .collect();
        self.inner.write_batch(&prefixed)
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        self.inner.prefix_delete_support()
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        let mut prefixed = Vec::with_capacity(self.prefix.len() + prefix.len());
        prefixed.extend_from_slice(&self.prefix);
        prefixed.extend_from_slice(prefix);
        self.inner.delete_prefix(&prefixed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_store_isolates_fragments() {
        use crate::shard::FragmentId;
        use std::sync::Arc;

        // A recording inner store: captures what the wrapper writes.
        struct Rec {
            written: std::sync::Mutex<Vec<(Vec<u8>, VersionedValue)>>,
        }
        impl KvStore for Rec {
            fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
                Ok(StoreBinding::unmanaged())
            }
            fn load_prefix(
                &self,
                prefix: &[u8],
            ) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self
                    .load()?
                    .into_iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .collect())
            }
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self.written.lock().unwrap().clone())
            }
            fn commit_durability(&self) -> StoreCommitDurability {
                StoreCommitDurability::Stable
            }
            fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
                Ok(StoreCommitDurability::Stable)
            }
            fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                self.written
                    .lock()
                    .unwrap()
                    .extend(writes.iter().map(|(k, v)| (k.clone(), v.clone())));
                Ok(())
            }
            fn prefix_delete_support(&self) -> PrefixDeleteSupport {
                PrefixDeleteSupport::Supported
            }
            fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
                self.written
                    .lock()
                    .unwrap()
                    .retain(|(key, _)| !key.starts_with(prefix));
                Ok(PrefixDeleteOutcome::Deleted)
            }
        }

        let inner = Arc::new(Rec {
            written: std::sync::Mutex::new(Vec::new()),
        });
        let f0 = PrefixedStore::new(inner.clone(), FragmentId(0));
        let f1 = PrefixedStore::new(inner.clone(), FragmentId(1));

        assert_eq!(f0.commit_durability(), StoreCommitDurability::Stable);
        assert_eq!(f0.stable_fence().unwrap(), StoreCommitDurability::Stable);
        assert_eq!(f0.prefix_delete_support(), PrefixDeleteSupport::Supported);

        let v = VersionedValue::new(b"x".to_vec(), 1, "n".to_string());
        f0.write_batch(&[(b"key".to_vec(), v.clone())]).unwrap();
        f1.write_batch(&[(b"key".to_vec(), v.clone())]).unwrap();

        // Same logical key, different fragments -> different stored keys.
        let written = inner.written.lock().unwrap();
        assert_eq!(written.len(), 2);
        assert_ne!(written[0].0, written[1].0);
        assert!(written[0].0.starts_with(&0u32.to_be_bytes()));
        assert!(written[1].0.starts_with(&1u32.to_be_bytes()));
        drop(written);

        // Each fragment loads only its own keys, prefix stripped.
        assert_eq!(f0.load().unwrap().len(), 1);
        assert_eq!(f0.load().unwrap()[0].0, b"key");
        assert_eq!(f1.load().unwrap().len(), 1);
        assert_eq!(f1.load().unwrap()[0].0, b"key");

        assert_eq!(f0.delete_prefix(&[]).unwrap(), PrefixDeleteOutcome::Deleted);
        assert!(f0.load().unwrap().is_empty());
        assert_eq!(f1.load().unwrap().len(), 1);
        assert!(f1
            .bind(&StoreIdentity::new("node".to_string(), 2))
            .err()
            .expect("a PrefixedStore was accepted as a Node base store")
            .to_string()
            .contains("cannot be attached as a Node base store"));
    }

    #[test]
    fn table_store_partitions_physical_iteration() {
        use std::sync::Mutex;

        struct Rec {
            written: Mutex<Vec<(Vec<u8>, VersionedValue)>>,
        }
        impl KvStore for Rec {
            fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
                Ok(StoreBinding::unmanaged())
            }
            fn commit_durability(&self) -> StoreCommitDurability {
                StoreCommitDurability::Buffered
            }
            fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
                Ok(StoreCommitDurability::Buffered)
            }
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self.written.lock().unwrap().clone())
            }
            fn load_prefix(
                &self,
                prefix: &[u8],
            ) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self
                    .written
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .cloned()
                    .collect())
            }
            fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                self.written.lock().unwrap().extend_from_slice(writes);
                Ok(())
            }
        }

        let inner = Arc::new(Rec {
            written: Mutex::new(Vec::new()),
        });
        let users = TableStore::new(inner.clone(), "USERS");
        let events = TableStore::new(inner.clone(), "EVENTS");
        let value = VersionedValue::new(b"value".to_vec(), 1, "node".to_string());
        users
            .write_batch(&[(b"physical-user-key".to_vec(), value.clone())])
            .unwrap();
        events
            .write_batch(&[(b"physical-event-key".to_vec(), value)])
            .unwrap();

        assert_eq!(users.load().unwrap().len(), 1);
        assert_eq!(users.load().unwrap()[0].0, b"physical-user-key");
        assert_eq!(events.load().unwrap().len(), 1);
        assert_eq!(events.load().unwrap()[0].0, b"physical-event-key");
        assert_eq!(inner.load().unwrap().len(), 2);
    }
}
