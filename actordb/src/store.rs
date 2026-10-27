// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Pluggable durable storage for a node's local copy.
//!
//! A [`KvStore`](crate::store::KvStore) persists the batches the [`KvActor`](crate::actor::KvActor)
//! flushes and reloads them on boot. Persistence itself is optional:
//! `NodeConfig.store` is `None` by default (a pure in-memory node).
//! The default crate includes no bundled storage backend. Enable the `rocksdb`
//! cargo feature for `RocksDbStore`, the preferred durable implementation, or
//! bring your own. The independent `memory` feature enables `MemoryStore`, a
//! no-op test double for exercising the persistence plumbing without a database.
//! Neither feature enables the other; the Node's volatile actor cache does not
//! depend on either backend.
//!
//! Shared contracts, identities, capability types, and backend options live in
//! this parent module. Each implementation has its own child module:
//! the `memory` feature enables the test double in `store::memory`,
//! [`prefixed`](crate::store::prefixed) contains the fragment namespace adapter,
//! and the `rocksdb` feature enables the offered RocksDB
//! backend in `store::rocksdb`. Existing `store::*` type imports remain available
//! through reexports when the corresponding feature is enabled. `KvStore` and
//! the `PrefixedStore` namespace wrapper remain available without a backend.
//!
//! A caller-supplied store works with the backend-free default crate:
//!
//! ```
//! use actordb::{actor::VersionedValue, shard::FragmentId};
//! use actordb::store::{KvStore, PrefixedStore, StoreError, StoreBinding, StoreIdentity, StoreCommitDurability};
//! use std::sync::Arc;
//!
//! struct CallerStore;
//! impl KvStore for CallerStore {
//!     fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> { Ok(StoreBinding::unmanaged()) }
//!     fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> { Ok(Vec::new()) }
//!     fn commit_durability(&self) -> StoreCommitDurability { StoreCommitDurability::Buffered }
//!     fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> { Ok(StoreCommitDurability::Buffered) }
//!     fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
//!         Ok(Vec::new())
//!     }
//!     fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
//!         Ok(())
//!     }
//! }
//! let root: Arc<dyn KvStore> = Arc::new(CallerStore);
//! let fragment = PrefixedStore::new(root, FragmentId(0));
//! assert!(fragment.load().unwrap().is_empty());
//! ```

#![cfg_attr(
    not(feature = "memory"),
    doc = r#"
Without `memory`, neither the root reexport nor the backend module is public:

```compile_fail
use actordb::store::MemoryStore;
```

```compile_fail
use actordb::store::memory::MemoryStore;
```
"#
)]
#![cfg_attr(
    not(feature = "rocksdb"),
    doc = r#"
Without `rocksdb`, neither the root reexport nor the backend module is public:

```compile_fail
use actordb::store::RocksDbStore;
```

```compile_fail
use actordb::store::rocksdb::RocksDbStore;
```

RocksDB open options and Node's convenience constructor are also opt-in:

```compile_fail
use actordb::store::RocksDbOptions;
```

```compile_fail
let config = actordb::node::NodeConfig::with_rocksdb("./data/node-a");
```
"#
)]

use std::fmt;

use crate::actor::VersionedValue;

pub(crate) mod executor;
#[cfg(feature = "memory")]
pub mod memory;
pub mod prefixed;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "memory")]
pub use memory::MemoryStore;
pub use prefixed::{PrefixedStore, TableStore};
#[cfg(feature = "rocksdb")]
pub use rocksdb::RocksDbStore;

/// Admission limits for authoritative persistence work.
///
/// A pending winner replaces the previous pending value for its key. Active
/// store batches remain immutable and charged until completion. Byte charges
/// include a second batch representation used by synchronous backends.
#[derive(Clone, Debug, bon::Builder)]
pub struct PersistenceConfig {
    /// Maximum retained pending and active records per fragment.
    #[builder(default = 10_000)]
    max_records: usize,
    /// Maximum charged retained bytes per fragment.
    #[builder(default = 64 * 1024 * 1024)]
    max_bytes: usize,
    /// Maximum charged retained bytes across this node's fragments.
    #[builder(default = 256 * 1024 * 1024)]
    node_max_bytes: usize,

    /// Maximum concurrent synchronous storage jobs across the node.
    #[builder(default = 4)]
    storage_concurrency: usize,
    #[builder(skip = std::sync::Arc::new(std::sync::OnceLock::new()))]
    resources: std::sync::Arc<std::sync::OnceLock<PersistenceResources>>,
}
#[derive(Debug)]
struct PersistenceResources {
    aggregate: std::sync::Arc<crate::retained::ByteBudget>,
    executor: std::sync::Arc<executor::StorageExecutor>,
}
impl Default for PersistenceConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl PersistenceConfig {
    fn resources(&self) -> &PersistenceResources {
        self.resources.get_or_init(|| PersistenceResources {
            aggregate: crate::retained::ByteBudget::new(self.node_max_bytes),
            executor: executor::StorageExecutor::new(self.storage_concurrency),
        })
    }
    pub(crate) fn aggregate(&self) -> std::sync::Arc<crate::retained::ByteBudget> {
        self.resources().aggregate.clone()
    }
    pub(crate) fn executor(&self) -> std::sync::Arc<executor::StorageExecutor> {
        self.resources().executor.clone()
    }

    pub fn storage_concurrency(&self) -> usize {
        self.storage_concurrency
    }
    /// Submitted storage jobs still owning backend execution slots.
    pub fn active_storage_jobs(&self) -> usize {
        self.resources().executor.active()
    }
    pub fn max_records(&self) -> usize {
        self.max_records
    }
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
    pub fn node_max_bytes(&self) -> usize {
        self.node_max_bytes
    }
    /// Current charged bytes for this runtime's persistence work.
    pub fn retained_bytes(&self) -> usize {
        self.resources().aggregate.used()
    }
    /// Validate nonzero admission and supported executor concurrency limits.
    pub fn validate(&self) -> Result<(), String> {
        if self.storage_concurrency == 0
            || self.storage_concurrency > u32::MAX as usize
            || self.storage_concurrency > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err("storage concurrency must fit a positive u32".into());
        }
        if self.max_records == 0 || self.max_bytes == 0 || self.node_max_bytes == 0 {
            return Err("persistence admission limits must be nonzero".to_string());
        }
        Ok(())
    }

    pub(crate) fn for_runtime(&self) -> Result<Self, String> {
        self.validate()?;
        let mut runtime = self.clone();
        runtime.resources = std::sync::Arc::new(std::sync::OnceLock::new());
        Ok(runtime)
    }
}

/// Error from a [`KvStore`] operation.
#[derive(Debug)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "store error: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

/// Immutable identity of one node's durable keyspace.
///
/// A backend may persist this identity and reject a later attachment under a
/// different node id or fragment count. The offered RocksDB backend does so:
/// changing either value without an explicit migration can otherwise hide
/// prefixed records or mount one node's local copy as another node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    node_id: String,
    fragment_count: u32,
}

impl StoreIdentity {
    /// Describe the node and fragment layout which will use a store.
    pub fn new(node_id: String, fragment_count: u32) -> Self {
        Self {
            node_id,
            fragment_count,
        }
    }

    /// Node id permanently associated with the store.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Fragment count permanently associated with the store.
    pub fn fragment_count(&self) -> u32 {
        self.fragment_count
    }
}

/// RAII ownership returned when a node attaches to a store.
///
/// Every backend implements an explicit [`KvStore::bind`] policy. RocksDB
/// uses a managed binding to reject
/// two live nodes sharing one `RocksDbStore` instance; dropping the binding
/// releases that in-process attachment.
pub struct StoreBinding {
    release: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl StoreBinding {
    /// Whether this backend supplied explicit managed attachment ownership.
    /// Checkpoint-capable root stores must provide an exclusive managed lease.
    pub fn is_managed(&self) -> bool {
        self.release.is_some()
    }
    /// Construct a binding with no release action.
    ///
    /// This is useful to custom stores which validate [`StoreIdentity`] but do
    /// not need to own an exclusive backend lease.
    pub fn unmanaged() -> Self {
        Self { release: None }
    }

    /// Construct a binding which runs `release` when it is cleanly dropped.
    ///
    /// Custom stores can use this to own an exclusive backend lease for the
    /// lifetime of a Node. actordb may deliberately abandon the callback when
    /// asynchronous Node startup or shutdown is cancelled and actor exit
    /// cannot be proven; in that case the callback and its captures are
    /// retained until process exit so an RAII capture cannot release the lease.
    pub fn managed(release: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            release: Some(Box::new(release)),
        }
    }

    /// Keep a managed backend fail-closed when its owner cannot prove that all
    /// users have stopped. Explicit Node shutdown releases normally; best-
    /// effort Drop abandons the release callback so the same live store object
    /// cannot be reattached while old actor tasks may still be exiting.
    pub(crate) fn abandon(&mut self) {
        if let Some(release) = self.release.take() {
            // Dropping a closure also drops its captures. A custom backend may
            // capture an RAII lease whose Drop releases exclusivity, so merely
            // discarding the callback would violate fail-closed cancellation.
            // There is no later join point which can safely release it; retain
            // the lease until process exit instead.
            std::mem::forget(release);
        }
    }
}

impl Drop for StoreBinding {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

/// Whether a backend opts into lifecycle-driven prefix deletion.
///
/// The conservative default is [`PrefixDeleteSupport::Retained`], so adding
/// prefix retirement to actordb does not cause existing custom stores to begin
/// receiving destructive calls without an explicit implementation choice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PrefixDeleteSupport {
    /// Keep retired namespaces. Callers must not invoke `delete_prefix` as an
    /// automatic lifecycle side effect.
    #[default]
    Retained,
    /// The backend supports quiesced, durable, idempotent prefix deletion.
    Supported,
}

/// Crash durability of an ordinary successful [`KvStore::write_batch`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StoreCommitDurability {
    /// The backend accepted the write, but an operating-system crash or power
    /// loss may still discard it.
    #[default]
    Buffered,
    /// The backend reports that the write reached its stable crash-recovery
    /// boundary before returning.
    Stable,
}

/// Actual synchronous storage ownership and optional monotonic timing.
/// Counters belong to `scope_id`, the shared PersistenceConfig executor
/// incarnation, which can be deliberately reused across successive nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageDiagnostics {
    pub scope_id: u64,
    pub queued_jobs: usize,
    pub active_jobs: usize,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub timing_enabled: bool,
    pub queue_wait: crate::node::observability::DurationSummary,
    pub execution_time: crate::node::observability::DurationSummary,
}

/// Result of asking a store to remove a key prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixDeleteOutcome {
    /// The prefix was durably made invisible before the call returned.
    Deleted,
    /// The backend retained the prefix because durable namespace cleanup is
    /// unsupported or deliberately disabled.
    Retained,
}

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
/// loading, but direct store users must preserve the same invariant. For each
/// key, a full LWW tuple must identify one payload and expiry deadline; direct
/// writes and restore tools must not replace its contents while retaining its
/// version. Use a fresh version for edited data and preserve clock floors.
/// Node startup rejects duplicate keys rather than choosing a row by load order.
/// Digests are derived from loaded winners in memory and require no persisted
/// digest metadata.
pub trait KvStore: Send + Sync + 'static {
    /// Optional durable snapshot and atomic resumable import capability.
    /// No full-dataset `load` fallback is used for checkpoint transfer.
    fn checkpoint_backend(&self) -> Option<&dyn crate::checkpoint::CheckpointBackend> {
        None
    }
    /// Attach one live node and validate any backend identity metadata.
    ///
    /// Implement the backend's explicit identity/exclusivity policy. A wrapper
    /// delegates and retains the returned ownership. Choosing unmanaged
    /// attachment is an explicit backend decision, never a hidden default.
    fn bind(&self, identity: &StoreIdentity) -> Result<StoreBinding, StoreError>;
    /// Load the full backing dataset, with exactly one current winner per key.
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    /// Load only the requested namespace. Implement prefix-specific storage
    /// access; there is no implicit full-dataset load/filter fallback.
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    /// Crash durability provided by an ordinary successful `write_batch`.
    ///
    /// Declare the actual backend completion contract; wrappers delegate it.
    fn commit_durability(&self) -> StoreCommitDurability;
    /// Force writes completed before this call through the backend's strongest
    /// available crash-recovery fence.
    ///
    /// Callers flush actor-side buffers first. Implement a real fence and
    /// report its actual durability, or explicitly return unsupported/error.
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError>;
    /// Atomically persist a batch of winning writes. Each key occurs at most
    /// once in a batch.
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
    /// Atomically persist a borrowed winning batch, prepending `prefix` to
    /// every key exactly once. Namespace wrappers combine their prefixes and
    /// delegate without copying values or materializing intermediate keys.
    ///
    /// The default materializes one owned backend batch and calls
    /// [`Self::write_batch`], so existing custom stores need no changes.
    /// Backends may override this to encode directly from the borrowed view.
    /// All data must be consumed synchronously before returning; durability
    /// and failure semantics are identical to `write_batch`.
    fn write_batch_borrowed(
        &self,
        prefix: &[u8],
        writes: &[(&[u8], &VersionedValue)],
    ) -> Result<(), StoreError> {
        let owned: Vec<_> = writes
            .iter()
            .map(|(key, value)| {
                let mut physical = Vec::with_capacity(prefix.len().saturating_add(key.len()));
                physical.extend_from_slice(prefix);
                physical.extend_from_slice(key);
                (physical, (*value).clone())
            })
            .collect();
        self.write_batch(&owned)
    }
    /// Declare whether lifecycle code may invoke [`KvStore::delete_prefix`].
    ///
    /// Backends retain namespaces unless they explicitly opt in. Opting in promises
    /// that `delete_prefix` obeys its quiescence, durability, and idempotency
    /// contract; the returned [`PrefixDeleteOutcome`] remains authoritative for
    /// each call.
    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Retained
    }
    /// Durably remove keys beginning with `prefix` after their writers have
    /// been quiesced. The operation must be idempotent because lifecycle work
    /// can retry it after an ambiguous failure.
    ///
    /// Unsupported deletion retains data. Returning
    /// [`PrefixDeleteOutcome::Deleted`] promises that a successful reopen will
    /// not expose any pre-call key in this prefix.
    fn delete_prefix(&self, _prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        Ok(PrefixDeleteOutcome::Retained)
    }
}

/// Crash policy for successful RocksDB write batches.
///
/// Both modes keep the write-ahead log enabled. The distinction is whether a
/// successful call forces that log through the operating-system buffer cache.
#[cfg(feature = "rocksdb")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RocksDbDurability {
    /// Append to the WAL without synchronously flushing it to stable media.
    AsyncWal,
    /// Synchronously flush the WAL before reporting success.
    #[default]
    SyncWal,
}

/// Options used when opening the offered RocksDB backend.
#[cfg(feature = "rocksdb")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bon::Builder)]
pub struct RocksDbOptions {
    #[builder(default = RocksDbDurability::SyncWal)]
    durability: RocksDbDurability,
}

#[cfg(feature = "rocksdb")]
impl Default for RocksDbOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(feature = "rocksdb")]
impl RocksDbOptions {
    /// Select the WAL completion policy for ordinary data batches.
    pub fn with_durability(mut self, durability: RocksDbDurability) -> Self {
        self.durability = durability;
        self
    }

    /// Current ordinary-write durability policy.
    pub fn durability(&self) -> RocksDbDurability {
        self.durability
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_admission_rejects_zero_executor_slots() {
        assert!(PersistenceConfig::builder()
            .storage_concurrency(0)
            .build()
            .validate()
            .is_err());
        assert!(PersistenceConfig::default().validate().is_ok());
    }

    #[test]
    fn custom_store_declares_durability_and_retains_unsupported_deletion() {
        struct CustomStore;

        impl KvStore for CustomStore {
            fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
                Ok(StoreBinding::unmanaged())
            }
            fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(Vec::new())
            }
            fn commit_durability(&self) -> StoreCommitDurability {
                StoreCommitDurability::Buffered
            }
            fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
                Ok(StoreCommitDurability::Buffered)
            }
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                Ok(Vec::new())
            }

            fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                Ok(())
            }
        }

        let store = CustomStore;
        assert_eq!(store.commit_durability(), StoreCommitDurability::Buffered);
        assert_eq!(
            store.stable_fence().unwrap(),
            StoreCommitDurability::Buffered
        );
        assert_eq!(store.prefix_delete_support(), PrefixDeleteSupport::Retained);
    }
}
