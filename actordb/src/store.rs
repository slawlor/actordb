// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Pluggable durable storage for a node's local copy.
//!
//! A [`KvStore`](crate::store::KvStore) persists the batches the [`KvActor`](crate::actor::KvActor)
//! flushes and reloads them on boot. Persistence itself is optional:
//! `NodeConfig.store` is `None` by default (a pure in-memory node).
//! Enable the `rocksdb` cargo feature for `RocksDbStore`, the
//! preferred implementation, or bring your own. [`MemoryStore`](crate::store::MemoryStore) is a
//! test double for exercising the persistence plumbing without a
//! database.

use std::fmt;
use std::sync::Arc;

use crate::actor::VersionedValue;
use crate::shard::FragmentId;
#[cfg(feature = "rocksdb")]
use crate::wire::{check_node_id_budget, check_record_budget};

/// Error from a [`KvStore`] operation.
#[derive(Debug)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "store error: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

/// Durable storage for a node's local copy.
///
/// Implementors must be thread-safe: different fragments can call the same
/// underlying store concurrently. `load` runs once at actor boot;
/// `write_batch` runs on every flush (see `NodeConfig` flush knobs). Writes
/// for one fragment are serialized, but a batch may be retried after a
/// failure or cancelled caller, so implementations must be idempotent for
/// the same key/value versions.
///
/// Keys are raw bytes. The actor works with `String` keys and converts
/// at the boundary (failing boot loudly on non-UTF-8 data); the store
/// itself never interprets key bytes.
///
/// Every persisted [`VersionedValue::node_id`] must contain 1 to 255 UTF-8
/// bytes. Node actors validate this before calling the store and again while
/// loading, but direct store users must preserve the same invariant.
pub trait KvStore: Send + Sync + 'static {
    /// Load the full backing dataset. The default [`KvStore::load_prefix`]
    /// implementation filters this result during fragment startup.
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    /// Load only keys beginning with `prefix`. The default preserves source
    /// compatibility for custom stores; stores with ordered iteration should
    /// override it to avoid a full-dataset scan for every fragment.
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .load()?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect())
    }
    /// Atomically persist a batch of winning writes. Each key occurs at most
    /// once in a batch.
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
}

/// Test double for [`KvStore`]: records nothing, loads nothing.
///
/// Exists so tests can exercise the actor's persistence plumbing
/// (buffering, batching, flush-on-shutdown) without a database.
/// Not for production use — a node that doesn't need durability
/// should use `store: None`.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryStore;

impl KvStore for MemoryStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
}

/// A [`KvStore`] view namespaced to one fragment.
///
/// Keys are prefixed with the fragment id (`frag_id` as big-endian
/// u32) before hitting the inner store, and the prefix is stripped on
/// load. Lets N fragment actors share one underlying store (e.g. one
/// RocksDB) without key collisions, and makes per-fragment operations
/// (drop, migrate) possible later via prefix scans.
pub struct PrefixedStore {
    inner: Arc<dyn KvStore>,
    prefix: [u8; 4],
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
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        let mut out = Vec::new();
        for (key, value) in self.inner.load_prefix(&self.prefix)? {
            if let Some(stripped) = key.strip_prefix(self.prefix.as_slice()) {
                out.push((stripped.to_vec(), value));
            }
        }
        Ok(out)
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
}

/// RocksDB-backed store. Available with the `rocksdb` cargo feature.
///
/// Values are encoded as `[timestamp_ms: u64 BE][expires_at_ms: u64 BE, 0 = none][is_tombstone: u8][node_id_len: u32 BE][node_id][value]`.
#[cfg(feature = "rocksdb")]
pub struct RocksDbStore {
    db: rocksdb::DB,
}

#[cfg(feature = "rocksdb")]
impl RocksDbStore {
    /// Open (creating if missing) the RocksDB at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(|e| StoreError(e.to_string()))?;
        let db = rocksdb::DB::open_default(path).map_err(|e| StoreError(e.to_string()))?;
        Ok(Self { db })
    }

    fn encode(v: &VersionedValue) -> Vec<u8> {
        let mut buf = Vec::with_capacity(21 + v.node_id.len() + v.value.len());
        buf.extend_from_slice(&v.timestamp_ms.to_be_bytes());
        // 0 = no expiry.
        buf.extend_from_slice(&v.expires_at_ms.unwrap_or(0).to_be_bytes());
        buf.push(v.is_tombstone as u8);
        buf.extend_from_slice(&(v.node_id.len() as u32).to_be_bytes());
        buf.extend_from_slice(v.node_id.as_bytes());
        buf.extend_from_slice(&v.value);
        buf
    }

    fn decode(bytes: &[u8]) -> Result<VersionedValue, String> {
        if bytes.len() < 21 {
            return Err(format!("value too short: {} bytes", bytes.len()));
        }
        let timestamp_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let expires_raw = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let expires_at_ms = if expires_raw == 0 {
            None
        } else {
            Some(expires_raw)
        };
        let is_tombstone = bytes[16] != 0;
        let id_len = u32::from_be_bytes(bytes[17..21].try_into().unwrap()) as usize;
        if bytes.len() < 21 + id_len {
            return Err(format!(
                "node_id overruns value: id_len={id_len} total={}",
                bytes.len()
            ));
        }
        let node_id = String::from_utf8(bytes[21..21 + id_len].to_vec())
            .map_err(|e| format!("node_id not UTF-8: {e}"))?;
        let value = bytes[21 + id_len..].to_vec();
        Ok(VersionedValue {
            value,
            timestamp_ms,
            node_id,
            expires_at_ms,
            is_tombstone,
        })
    }

    fn load_matching(
        &self,
        prefix: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        let mode = prefix.map_or(rocksdb::IteratorMode::Start, |prefix| {
            rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward)
        });
        let prefix_len = prefix.map_or(0, <[u8]>::len);
        let mut out = Vec::new();
        // Validate each raw record before decoding or retaining another owned
        // copy. Prefix iteration also prevents one full-DB scan per fragment.
        for item in self.db.iterator(mode) {
            let (key, encoded) = item.map_err(|error| StoreError(error.to_string()))?;
            if prefix.map_or(false, |prefix| !key.starts_with(prefix)) {
                break;
            }
            let logical_key = key.get(prefix_len..).ok_or_else(|| {
                StoreError("RocksDB key is shorter than its requested prefix".to_string())
            })?;
            if encoded.len() < 21 {
                return Err(StoreError(format!(
                    "persisted value too short: {} bytes",
                    encoded.len()
                )));
            }
            let node_len = u32::from_be_bytes(encoded[17..21].try_into().unwrap()) as usize;
            let value_offset = 21usize.checked_add(node_len).ok_or_else(|| {
                StoreError("persisted node id length overflowed usize".to_string())
            })?;
            if encoded.len() < value_offset {
                return Err(StoreError(format!(
                    "persisted node id overruns value: id_len={node_len} total={}",
                    encoded.len()
                )));
            }
            check_record_budget(logical_key.len(), encoded.len() - value_offset, node_len)
                .map_err(|error| {
                    StoreError(format!(
                        "persisted record (key bytes {}) cannot be replicated: {error}",
                        logical_key.len()
                    ))
                })?;
            let value = Self::decode(&encoded).map_err(StoreError)?;
            out.push((key.to_vec(), value));
        }
        Ok(out)
    }
}

#[cfg(feature = "rocksdb")]
impl KvStore for RocksDbStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_matching(None)
    }

    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_matching(Some(prefix))
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in writes {
            check_node_id_budget(value.node_id.len()).map_err(|error| {
                StoreError(format!(
                    "refusing to persist record with invalid provenance: {error}"
                ))
            })?;
            batch.put(key, Self::encode(value));
        }
        self.db.write(batch).map_err(|e| StoreError(e.to_string()))
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
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(self.written.lock().unwrap().clone())
            }
            fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                self.written
                    .lock()
                    .unwrap()
                    .extend(writes.iter().map(|(k, v)| (k.clone(), v.clone())));
                Ok(())
            }
        }

        let inner = Arc::new(Rec {
            written: std::sync::Mutex::new(Vec::new()),
        });
        let f0 = PrefixedStore::new(inner.clone(), FragmentId(0));
        let f1 = PrefixedStore::new(inner.clone(), FragmentId(1));

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
    }

    #[test]
    fn memory_store_is_a_noop() {
        let s = MemoryStore;
        assert!(s.load().unwrap().is_empty());
        s.write_batch(&[(
            b"k".to_vec(),
            VersionedValue::new(vec![1], 1, "n".to_string()),
        )])
        .unwrap();
        assert!(s.load().unwrap().is_empty());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_value_encoding_roundtrip() {
        let v = VersionedValue::new(vec![1, 2, 3], 123456789, "node-a".to_string());
        assert_eq!(RocksDbStore::decode(&RocksDbStore::encode(&v)).unwrap(), v);
        let v = VersionedValue::new(vec![], 0, "n".to_string());
        assert_eq!(RocksDbStore::decode(&RocksDbStore::encode(&v)).unwrap(), v);
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_decode_rejects_garbage() {
        assert!(RocksDbStore::decode(&[]).is_err());
        assert!(RocksDbStore::decode(&[0; 11]).is_err());
        let mut bad = vec![0u8; 12];
        bad[8..12].copy_from_slice(&10u32.to_be_bytes());
        bad.extend_from_slice(b"abc");
        assert!(RocksDbStore::decode(&bad).is_err());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_prefix_load_rejects_irreducible_record_before_decode() {
        use crate::wire::MAX_FRAME_BYTES;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let mut raw_key = 0u32.to_be_bytes().to_vec();
        raw_key.extend(std::iter::repeat(b'k').take(MAX_FRAME_BYTES / 2));
        let encoded = RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, "node".to_string()));
        store.db.put(raw_key, encoded).unwrap();

        let error = PrefixedStore::new(store, FragmentId(0))
            .load()
            .expect_err("oversized persisted key was accepted");
        assert!(error.to_string().contains("cannot be replicated"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_rejects_invalid_provenance_before_persisting() {
        use crate::wire::MAX_WIRE_NODE_ID_BYTES;

        let dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        for node_id in [String::new(), "n".repeat(MAX_WIRE_NODE_ID_BYTES + 1)] {
            let error = store
                .write_batch(&[(
                    b"invalid".to_vec(),
                    VersionedValue::new(Vec::new(), 1, node_id),
                )])
                .expect_err("invalid provenance was persisted");
            assert!(error.to_string().contains("invalid provenance"));
        }
        assert!(store.load().unwrap().is_empty());

        store
            .db
            .put(
                b"legacy-invalid",
                RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, String::new())),
            )
            .unwrap();
        let error = store
            .load()
            .expect_err("invalid legacy provenance was loaded");
        assert!(error.to_string().contains("record node id is 0 bytes"));
    }
}
