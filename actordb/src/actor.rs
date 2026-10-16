// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! The actor-first heart of actordb.
//!
//! A `KvActor` is the per-fragment dispatcher: it routes every operation
//! to one of its `KvWorker`s by `hash(key) % worker_count`. Sticky
//! dispatch preserves per-key ordering within a priority pool, while the
//! shared fragment state serializes conflicting writes across pools and
//! different keys execute concurrently across workers.
//! `GetBatch` keeps one aggregate reply while lazily routing each ordered key
//! through that same admission and sticky-worker path; it never clones or
//! reserves an omitted byte-page suffix.
//!
//! Workers share the fragment's state ([`FragmentState`](crate::actor::FragmentState)): one in-memory
//! cache, one ordered persistence buffer, one [`KvStore`](crate::store::KvStore).
//! Cache merges and persistence enqueueing happen as one ordered operation,
//! including when two priority pools update the same key concurrently.
//! Replication is just
//! messages: a local write becomes a `ReplicatedOp` which the node's
//! transport layer forwards to the fragment's replica holders, and
//! inbound ops merge here with last-writer-wins. Because the merge is
//! idempotent and commutative, replicas converge without coordination.

mod clock;
mod fragment_state;
mod kv_actor;
mod kv_worker;
mod pool;
mod pool_worker;
mod protocol;

pub use clock::{now_ms, ClockSkewError, HlcClock};
pub use fragment_state::FragmentState;
pub use kv_actor::{KvActor, KvArgs, KvArgsBuilder, KvState};
pub use kv_worker::{KvWorker, KvWorkerArgs, KvWorkerArgsBuilder};
pub use pool::{worker_idx, PoolConfig, PoolConfigBuilder, TokenBucket};
pub use protocol::{
    ApplyDurability, ApplyError, ApplyOutcome, BatchReadContinuation, BatchReadEntry,
    BatchReadPage, KvMessage, OpPriority, ReplicatedOp, RetainedBatchRead, SnapshotSummary,
    SummaryPage, VersionedPage, VersionedValue,
};

pub(crate) use fragment_state::ExpiryPass;

#[cfg(test)]
mod test_support;
