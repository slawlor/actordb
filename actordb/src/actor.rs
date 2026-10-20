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
//! idempotent and commutative for unique record versions, replicas converge
//! without coordination. Reusing a full version for different contents is
//! rejected explicitly; see [`crate::actor::VersionedValue`].
//!
//! This is also the canonical facade for the fragment supervisor, cleaner,
//! and snapshotter. Their previous [`crate::supervision`] imports remain
//! available as compatibility reexports of the same types.

mod cleaner;
mod clock;
mod fragment;
mod fragment_state;
mod kv_actor;
mod kv_worker;
mod maintenance;
mod pool;
mod pool_worker;
mod protocol;
mod scan;
mod snapshotter;

pub use actordb_protocol::RecordVersion;
pub use cleaner::{Cleaner, CleanerArgs, CleanerArgsBuilder, CleanerMessage, CleanerState};
pub use clock::ClockError;
pub use clock::{now_ms, ClockSkewError, HlcClock};
pub use fragment::{
    FragmentHealth, FragmentShutdownError, FragmentSupervisor, SupervisorArgs,
    SupervisorArgsBuilder, SupervisorMessage, SupervisorState,
};
pub use fragment_state::FragmentState;
pub use kv_actor::{KvActor, KvArgs, KvArgsBuilder, KvState};
pub use kv_worker::{KvWorker, KvWorkerArgs, KvWorkerArgsBuilder};
pub use maintenance::MaintenanceTrigger;
pub use pool::{worker_idx, PoolConfig, PoolConfigBuilder, TokenBucket};
pub use protocol::{
    ApplyDurability, ApplyError, ApplyOutcome, BatchReadContinuation, BatchReadEntry,
    BatchReadPage, KvMessage, OpPriority, ReplicatedOp, RetainedActorMessage, RetainedBatchRead,
    SnapshotSummary, SummaryPage, VersionedPage, VersionedValue,
};
pub use scan::{KeyMatcher, ScanMatch, ScanSpec, ValueTransform, DEFAULT_SCAN_LIMIT};
pub use snapshotter::{
    Snapshotter, SnapshotterArgs, SnapshotterArgsBuilder, SnapshotterMessage, SnapshotterState,
};

pub(crate) use fragment::{
    FragmentStartupStorageError, DEFAULT_CLEAN_INTERVAL, DEFAULT_FLUSH_BATCH_SIZE,
    DEFAULT_FLUSH_INTERVAL, DEFAULT_SNAPSHOT_INTERVAL,
};
pub(crate) use fragment_state::ExpiryPass;

#[cfg(test)]
mod maintenance_test_support {
    //! Test-only synchronous store gates shared by the owning actor regressions.

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex as StdMutex};

    use crate::actor::VersionedValue;
    use crate::store::{KvStore, StoreError};

    #[derive(Default)]
    pub(super) struct GatedStore {
        pub(super) entered: AtomicBool,
        release: (StdMutex<bool>, Condvar),
        pub(super) entries: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    }

    impl GatedStore {
        pub(super) fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }

    pub(super) struct ReleaseStore(pub(super) Arc<GatedStore>);

    impl Drop for ReleaseStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl KvStore for GatedStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            drop(released);
            let mut entries = self.entries.lock().unwrap();
            for (key, value) in writes {
                entries.insert(key.clone(), value.clone());
            }
            Ok(())
        }
    }

    pub(super) struct ReleaseMaintenanceStore(pub(super) Arc<GatedStore>);

    impl Drop for ReleaseMaintenanceStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }
}
#[cfg(test)]
mod test_support {
    //! Small fixtures shared by actor-owned tests.

    use std::collections::HashMap;
    use std::time::Duration;

    use super::{FragmentState, ReplicatedOp, VersionedValue};
    pub(super) fn test_state(cache: HashMap<String, VersionedValue>) -> FragmentState {
        FragmentState::new(cache, None, 100, Duration::from_secs(3600))
    }

    pub(super) fn test_op(key: &str, value: VersionedValue) -> ReplicatedOp {
        ReplicatedOp {
            key: key.to_string(),
            value,
            seq: 0,
            boot_id: 0,
        }
    }
}
