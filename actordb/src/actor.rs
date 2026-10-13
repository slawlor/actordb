// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! The actor-first heart of actordb.
//!
//! A `KvActor` is the per-fragment dispatcher: it routes every operation
//! to one of its `KvWorker`s by `hash(key) % worker_count`. Sticky
//! dispatch preserves per-key ordering (a key's ops always land on the
//! same worker, whose mailbox is FIFO), while different keys execute
//! concurrently across workers.
//!
//! Workers share the fragment's state ([`FragmentState`](crate::actor::FragmentState)): one in-memory
//! cache, one persistence buffer, one [`KvStore`](crate::store::KvStore).
//! The cache is a `RwLock` — sticky dispatch means workers almost never
//! touch the same key, so contention is minimal. Replication is just
//! messages: a local write becomes a `ReplicatedOp` which the node's
//! transport layer forwards to the fragment's replica holders, and
//! inbound ops merge here with last-writer-wins. Because the merge is
//! idempotent and commutative, replicas converge without coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::rpc::CallResult;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::sync::{Mutex, RwLock};

use crate::shard::{hash_key, FragmentId};
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

/// The fragment's shared state: one in-memory cache, one persistence
/// buffer, one [`KvStore`].
///
/// All `KvWorker`s of a fragment share this. Sticky dispatch routes each
/// key to one worker, so workers rarely contend on the same key; the
/// `RwLock` is a safety net, not a hot path.
///
/// Applied writes are buffered and flushed to the [`KvStore`]
/// in batches (see `NodeConfig` flush knobs); the cache is reloaded from
/// the store on boot, so a restarted node keeps its dataset. A crash
/// can lose the unflushed tail of the write buffer; the handshake sync
/// on reconnect reconciles it from peers.
pub struct FragmentState {
    /// In-memory cache of the fragment: the fast path for all reads.
    cache: RwLock<HashMap<String, VersionedValue>>,
    /// Optional durable layer behind the cache. `None` = no persistence.
    kvstore: Option<Arc<dyn KvStore>>,
    /// Buffered writes awaiting flush.
    flush: Mutex<FlushBuffer>,
    flush_batch_size: usize,
    flush_interval: Duration,
}

struct FlushBuffer {
    pending: Vec<(String, VersionedValue)>,
    last_flush: Instant,
}

impl FragmentState {
    fn new(
        cache: HashMap<String, VersionedValue>,
        kvstore: Option<Arc<dyn KvStore>>,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        Self {
            cache: RwLock::new(cache),
            kvstore,
            flush: Mutex::new(FlushBuffer {
                pending: Vec::new(),
                last_flush: Instant::now(),
            }),
            flush_batch_size,
            flush_interval,
        }
    }

    /// Merge an op into the cache, keeping whichever version wins.
    /// Returns `true` if the op replaced the stored version.
    async fn apply(&self, op: &ReplicatedOp) -> bool {
        let mut cache = self.cache.write().await;
        let replace = match cache.get(&op.key) {
            Some(existing) => op.value.wins_over(existing),
            None => true,
        };
        if replace {
            cache.insert(op.key.clone(), op.value.clone());
        }
        replace
    }

    /// Buffer an applied write, flushing to the store when the batch is
    /// full or the flush interval has elapsed. Called only for ops that
    /// won the merge. Memory is always updated first, so reads never
    /// wait for a flush. No-op when no store is configured. The store
    /// write happens without holding the buffer lock.
    async fn buffer(&self, key: &str, value: &VersionedValue) -> Result<(), ActorProcessingErr> {
        if self.kvstore.is_none() {
            return Ok(());
        }
        let batch = {
            let mut flush = self.flush.lock().await;
            flush.pending.push((key.to_string(), value.clone()));
            if flush.pending.len() >= self.flush_batch_size
                || flush.last_flush.elapsed() >= self.flush_interval
            {
                Some(flush.take_batch())
            } else {
                None
            }
        };
        if let Some(batch) = batch {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Write all buffered writes to the store in one batch.
    async fn flush(&self) -> Result<(), ActorProcessingErr> {
        let batch = {
            let mut flush = self.flush.lock().await;
            if flush.pending.is_empty() {
                return Ok(());
            }
            flush.take_batch()
        };
        self.write_batch(batch).await
    }

    async fn write_batch(
        &self,
        batch: Vec<(Vec<u8>, VersionedValue)>,
    ) -> Result<(), ActorProcessingErr> {
        if let Some(kvstore) = &self.kvstore {
            kvstore
                .write_batch(&batch)
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<VersionedValue> {
        self.cache.read().await.get(key).cloned()
    }

    async fn get_all(&self) -> HashMap<String, VersionedValue> {
        self.cache.read().await.clone()
    }
}

impl FlushBuffer {
    /// Drain the pending writes and stamp the flush time. Caller must
    /// write the batch without holding the buffer lock.
    fn take_batch(&mut self) -> Vec<(Vec<u8>, VersionedValue)> {
        let batch: Vec<(Vec<u8>, VersionedValue)> = self
            .pending
            .iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
            .collect();
        self.pending.clear();
        self.last_flush = Instant::now();
        batch
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
    /// Number of worker actors in this fragment's pool. Keys are
    /// sticky-routed by `hash(key) % worker_count`.
    pub worker_count: usize,
    /// Flush the write buffer after this many buffered ops.
    pub flush_batch_size: usize,
    /// Flush the write buffer after this long, even if the batch isn't
    /// full. Checked on each applied write.
    pub flush_interval: Duration,
}

/// A fragment's worker: applies ops and serves reads against the shared
/// [`FragmentState`]. One worker's mailbox is FIFO, and sticky dispatch
/// sends each key to one worker, so per-key ordering holds while
/// different keys execute concurrently.
pub struct KvWorker;

/// Arguments to spawn a [KvWorker].
#[derive(Clone)]
pub struct KvWorkerArgs {
    /// The fragment state this worker shares with its siblings.
    pub shared: Arc<FragmentState>,
}

impl Actor for KvWorker {
    type Msg = KvMessage;
    type State = Arc<FragmentState>;
    type Arguments = KvWorkerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args.shared)
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
                if state.apply(&op).await {
                    state.buffer(&op.key, &op.value).await?;
                }
                Ok(())
            }
            KvMessage::Get { key, reply } => {
                // The receiver may be gone (e.g. timed-out caller); that's fine.
                let _ = reply.send(state.get(&key).await);
                Ok(())
            }
            // GetAll is served by the dispatcher straight from shared state.
            KvMessage::GetAll { .. } => Ok(()),
        }
    }
}

/// The worker index for `key`: sticky across calls.
fn worker_idx(key: &str, worker_count: usize) -> usize {
    hash_key(key.as_bytes()) as usize % worker_count
}

/// The per-fragment dispatcher. One runs per fragment this node serves:
/// it owns the [`FragmentState`], spawns the worker pool, and routes
/// every operation to a worker by sticky key hash. `GetAll` is served
/// directly from shared state.
pub struct KvActor;

/// The dispatcher's state: the worker pool and the state they share.
pub struct KvState {
    workers: Vec<ActorRef<KvMessage>>,
    shared: Arc<FragmentState>,
}

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
        let cache: HashMap<String, VersionedValue> = match &args.store {
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
        let shared = Arc::new(FragmentState::new(
            cache,
            args.store,
            args.flush_batch_size,
            args.flush_interval,
        ));
        let worker_count = args.worker_count.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (worker, _handle) = KvWorker::spawn(
                None,
                KvWorker,
                KvWorkerArgs {
                    shared: shared.clone(),
                },
            )
            .await
            .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
            workers.push(worker);
        }
        Ok(KvState { workers, shared })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Stop workers first so no new writes land mid-flush, then don't
        // leave buffered writes behind. (A kill -9 skips this; the
        // unflushed tail is recovered via peer sync on reconnect.)
        for worker in &state.workers {
            let _ = worker.stop_and_wait(None, None).await;
        }
        state.shared.flush().await
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp(op) => {
                let worker = &state.workers[worker_idx(&op.key, state.workers.len())];
                worker
                    .send_message(KvMessage::ApplyOp(op))
                    .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                Ok(())
            }
            KvMessage::Get { key, reply } => {
                // Same sticky route as the write path, so a Get issued
                // after an ApplyOp lands behind it in the worker's FIFO
                // mailbox — the read-your-write barrier still holds.
                // The worker replies through the caller's reply channel
                // directly. On worker timeout/error the reply is dropped
                // and the caller sees a SenderError.
                let worker = &state.workers[worker_idx(&key, state.workers.len())];
                if let Ok(CallResult::Success(value)) = worker
                    .call(|r| KvMessage::Get { key, reply: r }, None)
                    .await
                {
                    let _ = reply.send(value);
                }
                Ok(())
            }
            KvMessage::GetAll { reply } => {
                let _ = reply.send(state.shared.get_all().await);
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
