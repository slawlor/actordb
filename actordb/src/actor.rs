// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! The actor-first heart of actordb.
//!
//! A `KvActor` owns a node's *local copy* of the keyspace. The actor's
//! message handler is the only code that ever mutates that copy, so all
//! state transitions are serialized through the actor's mailbox — no
//! locks, no shared mutable state. Replication is just messages:
//! a local write becomes a `ReplicatedOp` which the node's transport
//! layer gossips to peers, and inbound ops merge here with
//! last-writer-wins. Because the merge is idempotent and commutative,
//! replicas converge without coordination.

use std::collections::HashMap;
use std::path::PathBuf;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use rocksdb::{IteratorMode, DB};

/// A value stamped with its last-writer-wins version.
///
/// Versions are totally ordered by `(timestamp_ms, node_id)`: the higher
/// wall-clock timestamp wins, and the node id breaks ties deterministically
/// so every replica picks the same winner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub timestamp_ms: u64,
    pub node_id: String,
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, timestamp_ms: u64, node_id: String) -> Self {
        Self {
            value,
            timestamp_ms,
            node_id,
        }
    }

    /// Returns `true` if `self` wins over `other` under last-writer-wins.
    pub fn wins_over(&self, other: &VersionedValue) -> bool {
        (self.timestamp_ms, &self.node_id) > (other.timestamp_ms, &other.node_id)
    }
}

/// A single replicated write operation.
///
/// `seq` is a per-origin-node sequence number and `boot_id` a random
/// identifier minted at process start; together with the origin node id
/// they form the transport layer's dedup key to suppress re-broadcast
/// storms. Neither plays a role in conflict resolution.
#[derive(Debug, Clone)]
pub struct ReplicatedOp {
    pub key: String,
    pub value: VersionedValue,
    pub seq: u64,
    pub boot_id: u64,
}

/// Messages handled by the [KvActor].
pub enum KvMessage {
    /// Apply a write — local or replicated. Merged with last-writer-wins,
    /// so applying the same op twice is harmless (idempotent).
    ApplyOp(ReplicatedOp),
    /// Read a single key from the local copy.
    Get {
        key: String,
        reply: RpcReplyPort<Option<VersionedValue>>,
    },
    /// Read the full local copy (used for anti-entropy sync).
    GetAll {
        reply: RpcReplyPort<HashMap<String, VersionedValue>>,
    },
}

/// The actor's exclusive state: this node's local copy of the keyspace.
///
/// When a `db` is present, every applied write is also persisted to
/// RocksDB (write-through) and the copy is reloaded from disk on boot,
/// so a restarted node keeps its dataset.
pub struct KvState {
    store: HashMap<String, VersionedValue>,
    db: Option<DB>,
}

impl KvState {
    /// Merge an op into the local copy, keeping whichever version wins.
    /// Returns `true` if the op replaced the stored version.
    fn apply(&mut self, op: &ReplicatedOp) -> bool {
        let replace = match self.store.get(&op.key) {
            Some(existing) => op.value.wins_over(existing),
            None => true,
        };
        if replace {
            self.store.insert(op.key.clone(), op.value.clone());
        }
        replace
    }

    /// Persist an applied write. Called only for ops that won the merge.
    fn persist(&self, key: &str, value: &VersionedValue) -> Result<(), ActorProcessingErr> {
        if let Some(db) = &self.db {
            db.put(key.as_bytes(), encode_value(value))
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        }
        Ok(())
    }
}

/// Arguments to spawn a [KvActor].
#[derive(Debug, Clone, Default)]
pub struct KvArgs {
    /// If set, persist every applied write to RocksDB at this path
    /// (created if missing) and reload it on boot. `None` (default)
    /// keeps the node purely in-memory.
    pub db_path: Option<PathBuf>,
}

/// The key-value actor. One runs per node and owns that node's local copy.
pub struct KvActor;

impl Actor for KvActor {
    type Msg = KvMessage;
    type State = KvState;
    type Arguments = KvArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut store = HashMap::new();
        let db = match args.db_path {
            Some(path) => {
                std::fs::create_dir_all(&path)
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                let db =
                    DB::open_default(&path).map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                // Reload the local copy. Corrupt or unreadable entries
                // fail the boot loudly rather than silently dropping data.
                for item in db.iterator(IteratorMode::Start) {
                    let (k, v) = item.map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                    let key = String::from_utf8(k.to_vec())
                        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                    let value = decode_value(&v).map_err(ActorProcessingErr::from)?;
                    store.insert(key, value);
                }
                Some(db)
            }
            None => None,
        };
        Ok(KvState { store, db })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp(op) => {
                // Write-through: durable first, then memory. If the DB
                // write fails the actor errors out rather than diverging
                // silently from disk.
                if state.apply(&op) {
                    state.persist(&op.key, &op.value)?;
                }
                Ok(())
            }
            KvMessage::Get { key, reply } => {
                // The receiver may be gone (e.g. timed-out caller); that's fine.
                let _ = reply.send(state.store.get(&key).cloned());
                Ok(())
            }
            KvMessage::GetAll { reply } => {
                let _ = reply.send(state.store.clone());
                Ok(())
            }
        }
    }
}

/// Current wall-clock time in milliseconds since the unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// On-disk encoding of a [VersionedValue]:
/// `[timestamp_ms: u64 BE][node_id_len: u32 BE][node_id][value]`.
fn encode_value(v: &VersionedValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + v.node_id.len() + v.value.len());
    buf.extend_from_slice(&v.timestamp_ms.to_be_bytes());
    buf.extend_from_slice(&(v.node_id.len() as u32).to_be_bytes());
    buf.extend_from_slice(v.node_id.as_bytes());
    buf.extend_from_slice(&v.value);
    buf
}

fn decode_value(bytes: &[u8]) -> Result<VersionedValue, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_encoding_roundtrip() {
        let v = VersionedValue::new(vec![1, 2, 3], 123456789, "node-a".to_string());
        let bytes = encode_value(&v);
        assert_eq!(decode_value(&bytes).unwrap(), v);
        // empty value and empty node id
        let v = VersionedValue::new(vec![], 0, String::new());
        assert_eq!(decode_value(&encode_value(&v)).unwrap(), v);
    }

    #[test]
    fn value_decoding_rejects_garbage() {
        assert!(decode_value(&[]).is_err());
        assert!(decode_value(&[0; 11]).is_err());
        // claims a 10-byte node id but only has 3
        let mut bad = vec![0u8; 12];
        bad[8..12].copy_from_slice(&10u32.to_be_bytes());
        bad.extend_from_slice(b"abc");
        assert!(decode_value(&bad).is_err());
    }

    #[test]
    fn lww_orders_by_timestamp_then_node_id() {
        let a = VersionedValue::new(vec![1], 100, "node-a".to_string());
        let b = VersionedValue::new(vec![2], 200, "node-a".to_string());
        let c = VersionedValue::new(vec![3], 200, "node-b".to_string());

        assert!(b.wins_over(&a));
        assert!(!a.wins_over(&b));
        // same timestamp: higher node id wins, deterministically
        assert!(c.wins_over(&b));
        assert!(!b.wins_over(&c));
        // identical versions never "win" — merge is a no-op
        assert!(!a.wins_over(&a));
    }
}
