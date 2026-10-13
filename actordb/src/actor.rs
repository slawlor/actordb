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

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

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
/// `seq` is a per-origin-node sequence number used by the transport layer
/// to suppress re-broadcast storms; it plays no role in conflict
/// resolution.
#[derive(Debug, Clone)]
pub struct ReplicatedOp {
    pub key: String,
    pub value: VersionedValue,
    pub seq: u64,
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
pub struct KvState {
    store: HashMap<String, VersionedValue>,
}

impl KvState {
    /// Merge an op into the local copy, keeping whichever version wins.
    fn apply(&mut self, op: ReplicatedOp) {
        let replace = match self.store.get(&op.key) {
            Some(existing) => op.value.wins_over(existing),
            None => true,
        };
        if replace {
            self.store.insert(op.key, op.value);
        }
    }
}

/// Arguments to spawn a [KvActor]. Kept for future use (e.g. persistence
/// directory); the node id currently travels on each operation instead.
pub struct KvArgs {}

/// The key-value actor. One runs per node and owns that node's local copy.
pub struct KvActor;

impl Actor for KvActor {
    type Msg = KvMessage;
    type State = KvState;
    type Arguments = KvArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(KvState {
            store: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp(op) => {
                state.apply(op);
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
