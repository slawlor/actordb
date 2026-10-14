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
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use tokio::sync::{Mutex, RwLock};

use crate::shard::{hash_key, FragmentId};
use crate::store::KvStore;

/// A value stamped with its last-writer-wins version.
///
/// Versions are totally ordered by `(timestamp_ms, node_id)`: the higher
/// wall-clock timestamp wins, and the node id breaks ties deterministically
/// so every replica picks the same winner. `expires_at_ms` is an optional
/// wall-clock deadline (set via `put_with_ttl`); expired values are
/// invisible to reads and reaped by the fragment's cleaner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub timestamp_ms: u64,
    pub node_id: String,
    /// Wall-clock expiry in ms since epoch. `None` = no expiry.
    pub expires_at_ms: Option<u64>,
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, timestamp_ms: u64, node_id: String) -> Self {
        Self {
            value,
            timestamp_ms,
            node_id,
            expires_at_ms: None,
        }
    }

    /// Returns `true` if `self` wins over `other` under last-writer-wins.
    pub fn wins_over(&self, other: &VersionedValue) -> bool {
        (self.timestamp_ms, &self.node_id) > (other.timestamp_ms, &other.node_id)
    }

    /// Returns `true` if this value is past its expiry at `now_ms`.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        matches!(self.expires_at_ms, Some(exp) if now_ms >= exp)
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
    pub fn new(
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
    /// Returns `true` if the op replaced the stored version. Ops that
    /// are already expired are dropped — storing them would only wait
    /// for the cleaner to reap them.
    pub async fn apply(&self, op: &ReplicatedOp) -> bool {
        if op.value.is_expired(now_ms()) {
            return false;
        }
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
    pub async fn buffer(
        &self,
        key: &str,
        value: &VersionedValue,
    ) -> Result<(), ActorProcessingErr> {
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
    pub async fn flush(&self) -> Result<(), ActorProcessingErr> {
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

    /// Read a key, treating expired values as absent.
    pub async fn get(&self, key: &str) -> Option<VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .get(key)
            .filter(|v| !v.is_expired(now))
            .cloned()
    }

    /// Read the full cache, excluding expired values.
    pub async fn get_all(&self) -> HashMap<String, VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .iter()
            .filter(|(_, v)| !v.is_expired(now))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Remove all expired keys. Returns the number reaped. Called by the
    /// fragment's cleaner; reads already hide expired values, so this is
    /// memory hygiene, not a correctness gate.
    pub async fn remove_expired(&self, now_ms: u64) -> usize {
        let mut cache = self.cache.write().await;
        let before = cache.len();
        cache.retain(|_, v| !v.is_expired(now_ms));
        before - cache.len()
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
    /// The fragment's shared state, created by the supervisor (which
    /// also hands it to the cleaner and snapshotter).
    pub shared: Arc<FragmentState>,
    /// Owning node id, for unique worker naming.
    /// Which fragment this actor serves. Used for debug output.
    pub fragment: FragmentId,
    /// Prefix for worker actor names (must be unique per dispatcher
    /// in the process; the supervisor generates it).
    pub worker_prefix: String,
    /// Number of worker actors in this fragment's pool. Keys are
    /// sticky-routed by `hash(key) % worker_count`.
    pub worker_count: usize,
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

/// The worker index for `key`: sticky across calls, so a key's ops
/// always land on the same worker.
pub fn worker_idx(key: &str, worker_count: usize) -> usize {
    hash_key(key.as_bytes()) as usize % worker_count
}

/// Factory and lifecycle owner for a fragment's worker pool.
///
/// The pool spawns the workers, routes keys to them by sticky hash, and
/// drains them on shutdown. The dispatcher holds a pool and does nothing
/// else with worker lifecycle — this is the seam Phase 4's supervision
/// (health checks, restarts) will build on.
pub struct WorkerPool {
    workers: Vec<ActorRef<KvMessage>>,
}

impl WorkerPool {
    /// Spawn `worker_count` workers sharing `shared` state, linked to
    /// `supervisor` so a dead worker raises a supervision event there.
    /// Workers are named `frag-<fragment>-worker-<slot>` for registry
    /// lookup (tests, debugging).
    pub async fn spawn_linked(
        worker_count: usize,
        shared: Arc<FragmentState>,
        supervisor: ActorCell,
        worker_prefix: &str,
    ) -> Result<Self, ActorProcessingErr> {
        let worker_count = worker_count.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        for slot in 0..worker_count {
            let name = format!("{worker_prefix}-worker-{slot}");
            let (worker, _handle) = KvWorker::spawn_linked(
                Some(name),
                KvWorker,
                KvWorkerArgs {
                    shared: shared.clone(),
                },
                supervisor.clone(),
            )
            .await
            .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
            workers.push(worker);
        }
        Ok(Self { workers })
    }

    /// Spawn `worker_count` workers sharing `shared` state, unsupervised.
    pub async fn spawn(
        worker_count: usize,
        shared: Arc<FragmentState>,
    ) -> Result<Self, ActorProcessingErr> {
        // Unsupervised spawn: used only in tests. Production pools go
        // through `spawn_linked` so the dispatcher restarts dead workers.
        let worker_count = worker_count.max(1);
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
        Ok(Self { workers })
    }

    /// The worker serving `key` (sticky: `hash(key) % worker_count`).
    pub fn route(&self, key: &str) -> &ActorRef<KvMessage> {
        &self.workers[worker_idx(key, self.workers.len())]
    }

    /// Find the pool slot holding `cell`, if any.
    pub fn slot_of(&self, cell: &ActorCell) -> Option<usize> {
        self.workers
            .iter()
            .position(|w| w.get_id() == cell.get_id())
    }

    /// Replace the worker at `slot` with `worker` (supervised restart).
    pub fn replace(&mut self, slot: usize, worker: ActorRef<KvMessage>) {
        if let Some(w) = self.workers.get_mut(slot) {
            *w = worker;
        }
    }

    /// Stop every worker, waiting for each. Call before flushing shared
    /// state so no new writes land mid-flush.
    pub async fn shutdown(&self) {
        for worker in &self.workers {
            let _ = worker.stop_and_wait(None, None).await;
        }
    }
}

/// The per-fragment dispatcher. One runs per fragment this node serves:
/// it owns the [`FragmentState`] and a [`WorkerPool`], and routes every
/// operation to a worker by sticky key hash. `GetAll` is served directly
/// from shared state.
pub struct KvActor;

/// The dispatcher's state: the worker pool and the state they share.
pub struct KvState {
    pool: WorkerPool,
    shared: Arc<FragmentState>,
    worker_prefix: String,
}

impl Actor for KvActor {
    type Msg = KvMessage;
    type State = KvState;
    type Arguments = KvArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // Workers are linked to the dispatcher: a dead worker raises a
        // supervision event here and gets replaced in-place.
        let pool = WorkerPool::spawn_linked(
            args.worker_count,
            args.shared.clone(),
            myself.get_cell(),
            &args.worker_prefix,
        )
        .await?;
        Ok(KvState {
            pool,
            shared: args.shared,
            worker_prefix: args.worker_prefix.clone(),
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Drain workers first so no new writes land mid-flush, then don't
        // leave buffered writes behind. (A kill -9 skips this; the
        // unflushed tail is recovered via peer sync on reconnect.)
        state.pool.shutdown().await;
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
                state
                    .pool
                    .route(&op.key)
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
                let worker = state.pool.route(&key);
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

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // A dead worker is replaced in-place: the pool slot keeps its
        // index, so sticky routing is unaffected and no keys move.
        let dead = match &message {
            SupervisionEvent::ActorFailed(cell, _) => Some(cell.clone()),
            SupervisionEvent::ActorTerminated(cell, _, _) => Some(cell.clone()),
            _ => None,
        };
        if let Some(cell) = dead {
            if let Some(slot) = state.pool.slot_of(&cell) {
                let name = format!("{}-worker-{slot}", state.worker_prefix);
                let (worker, _handle) = KvWorker::spawn_linked(
                    Some(name),
                    KvWorker,
                    KvWorkerArgs {
                        shared: state.shared.clone(),
                    },
                    myself.get_cell(),
                )
                .await
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
                state.pool.replace(slot, worker);
            }
        }
        Ok(())
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
