// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Pluggable durable storage for a node's local copy.
//!
//! A [`KvStore`] persists the batches the [`KvActor`](crate::actor::KvActor)
//! flushes and reloads them on boot. Persistence itself is optional:
//! `NodeConfig.store` is `None` by default (a pure in-memory node).
//! Enable the `rocksdb` cargo feature for [`RocksDbStore`], the
//! preferred implementation, or bring your own. [`MemoryStore`] is a
//! test double for exercising the persistence plumbing without a
//! database.

use std::collections::HashMap;
use std::fmt;

use crate::actor::VersionedValue;

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
/// Implementors must be thread-safe: the actor calls these from its
/// message handler. `load` runs once at actor boot; `write_batch` runs
/// on every flush (see `NodeConfig` flush knobs).
pub trait KvStore: Send + Sync + 'static {
    /// Load the full dataset. Called once at actor boot.
    fn load(&self) -> Result<HashMap<String, VersionedValue>, StoreError>;
    /// Persist a batch of winning writes.
    fn write_batch(&self, writes: &[(String, VersionedValue)]) -> Result<(), StoreError>;
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
    fn load(&self) -> Result<HashMap<String, VersionedValue>, StoreError> {
        Ok(HashMap::new())
    }

    fn write_batch(&self, _writes: &[(String, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
}

/// RocksDB-backed store. Available with the `rocksdb` cargo feature.
///
/// Values are encoded as `[timestamp_ms: u64 BE][node_id_len: u32 BE][node_id][value]`.
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
        let mut buf = Vec::with_capacity(12 + v.node_id.len() + v.value.len());
        buf.extend_from_slice(&v.timestamp_ms.to_be_bytes());
        buf.extend_from_slice(&(v.node_id.len() as u32).to_be_bytes());
        buf.extend_from_slice(v.node_id.as_bytes());
        buf.extend_from_slice(&v.value);
        buf
    }

    fn decode(bytes: &[u8]) -> Result<VersionedValue, String> {
        if bytes.len() < 12 {
            return Err(format!("value too short: {} bytes", bytes.len()));
        }
        let timestamp_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let id_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if bytes.len() < 12 + id_len {
            return Err(format!(
                "node_id overruns value: id_len={id_len} total={}",
                bytes.len()
            ));
        }
        let node_id = String::from_utf8(bytes[12..12 + id_len].to_vec())
            .map_err(|e| format!("node_id not UTF-8: {e}"))?;
        let value = bytes[12 + id_len..].to_vec();
        Ok(VersionedValue {
            value,
            timestamp_ms,
            node_id,
        })
    }
}

#[cfg(feature = "rocksdb")]
impl KvStore for RocksDbStore {
    fn load(&self) -> Result<HashMap<String, VersionedValue>, StoreError> {
        let mut store = HashMap::new();
        // Corrupt or unreadable entries fail the boot loudly rather
        // than silently dropping data.
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (k, v) = item.map_err(|e| StoreError(e.to_string()))?;
            let key = String::from_utf8(k.to_vec()).map_err(|e| StoreError(e.to_string()))?;
            let value = Self::decode(&v).map_err(StoreError)?;
            store.insert(key, value);
        }
        Ok(store)
    }

    fn write_batch(&self, writes: &[(String, VersionedValue)]) -> Result<(), StoreError> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in writes {
            batch.put(key.as_bytes(), Self::encode(value));
        }
        self.db.write(batch).map_err(|e| StoreError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_is_a_noop() {
        let s = MemoryStore;
        assert!(s.load().unwrap().is_empty());
        s.write_batch(&[(
            "k".to_string(),
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
        let v = VersionedValue::new(vec![], 0, String::new());
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
}
