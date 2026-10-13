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
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::shard::FragmentId;
use crate::store::KvStore;

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
/// Applied writes are buffered and flushed to the [`KvStore`] in
/// batches (see `NodeConfig` flush knobs); the copy is reloaded from
/// the store on boot, so a restarted node keeps its dataset. A crash
/// can lose the unflushed tail of the write buffer; the handshake sync
/// on reconnect reconciles it from peers.
pub struct KvState {
    /// In-memory cache of the local copy: the fast path for all reads.
    store: HashMap<String, VersionedValue>,
    /// Optional durable layer behind the cache. `None` = no persistence.
    kvstore: Option<Arc<dyn KvStore>>,
    pending: Vec<(String, VersionedValue)>,
    last_flush: Instant,
    flush_batch_size: usize,
    flush_interval: Duration,
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

    /// Buffer an applied write, flushing to the store when the batch is
    /// full or the flush interval has elapsed. Called only for ops that
    /// won the merge. Memory is always updated first, so reads never
    /// wait for a flush. No-op when no store is configured.
    fn buffer(&mut self, key: &str, value: &VersionedValue) -> Result<(), ActorProcessingErr> {
        if self.kvstore.is_none() {
            return Ok(());
        }
        self.pending.push((key.to_string(), value.clone()));
        if self.pending.len() >= self.flush_batch_size
            || self.last_flush.elapsed() >= self.flush_interval
        {
            self.flush()?;
        }
        Ok(())
    }

    /// Write the buffered batch to the store in one batch.
    fn flush(&mut self) -> Result<(), ActorProcessingErr> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Some(kvstore) = &self.kvstore {
            let batch: Vec<(Vec<u8>, VersionedValue)> = self
                .pending
                .iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
                .collect();
            kvstore
                .write_batch(&batch)
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
            self.pending.clear();
            self.last_flush = Instant::now();
        }
        Ok(())
    }
}

/// Arguments to spawn a [KvActor].
#[derive(Clone)]
pub struct KvArgs {
    /// Optional durable storage behind the in-memory cache. `None`
    /// (default) = no persistence; the node is purely in-memory.
    pub store: Option<Arc<dyn KvStore>>,
    /// Which fragment this actor serves. Used for store namespacing
    /// and debug output.
    pub fragment: FragmentId,
    /// Flush the write buffer after this many buffered ops.
    pub flush_batch_size: usize,
    /// Flush the write buffer after this long, even if the batch isn't
    /// full. Checked on each applied write.
    pub flush_interval: Duration,
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
        // Reload the in-memory cache from the store, if any. A corrupt
        // store fails the boot loudly rather than silently dropping data.
        let store: HashMap<String, VersionedValue> = match &args.store {
            Some(kvstore) => {
                let mut map = HashMap::new();
                for (k, v) in kvstore
                    .load()
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?
                {
                    let key = String::from_utf8(k)
                        .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                    map.insert(key, v);
                }
                map
            }
            None => HashMap::new(),
        };
        Ok(KvState {
            store,
            kvstore: args.store,
            pending: Vec::new(),
            last_flush: Instant::now(),
            flush_batch_size: args.flush_batch_size,
            flush_interval: args.flush_interval,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Clean shutdown: don't leave buffered writes behind.
        // (A kill -9 skips this; the unflushed tail is recovered via
        // peer sync on reconnect.)
        state.flush()
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp(op) => {
                // Memory first (read-your-write never waits for disk),
                // then buffer for the next batched flush. A DB error
                // fails the actor loudly rather than diverging silently.
                if state.apply(&op) {
                    state.buffer(&op.key, &op.value)?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
