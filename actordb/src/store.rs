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
#[cfg(feature = "rocksdb")]
use crate::shard::fragment_of;
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
/// Custom stores receive an unmanaged binding from the default
/// [`KvStore::bind`] implementation. RocksDB uses a managed binding to reject
/// two live nodes sharing one `RocksDbStore` instance; dropping the binding
/// releases that in-process attachment.
pub struct StoreBinding {
    release: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl StoreBinding {
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

/// Result of asking a store to remove a key prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixDeleteOutcome {
    /// The prefix was durably made invisible before the call returned.
    Deleted,
    /// The backend retained the prefix. This is the source-compatible default
    /// for custom stores which have not implemented durable namespace cleanup.
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
/// loading, but direct store users must preserve the same invariant.
pub trait KvStore: Send + Sync + 'static {
    /// Attach one live node and validate any backend identity metadata.
    ///
    /// The default is an unmanaged no-op so existing custom implementations
    /// remain source compatible. Backends which persist multiple fragment
    /// namespaces should bind at least the node id and fragment count. A
    /// wrapper around a backend which requires binding must delegate this call
    /// and retain the returned ownership rather than relying on the default.
    fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
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
    /// Crash durability provided by an ordinary successful `write_batch`.
    ///
    /// Existing custom stores default to [`StoreCommitDurability::Buffered`].
    /// A wrapper must delegate this method if the wrapped backend offers a
    /// stronger guarantee.
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }
    /// Force writes completed before this call through the backend's strongest
    /// available crash-recovery fence.
    ///
    /// Callers flush actor-side buffers before invoking this method. The default
    /// performs no additional work and reports [`KvStore::commit_durability`],
    /// preserving compatibility for custom stores. Implementations may override
    /// it when an explicit fence can strengthen buffered ordinary writes.
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(self.commit_durability())
    }
    /// Atomically persist a batch of winning writes. Each key occurs at most
    /// once in a batch.
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
    /// Declare whether lifecycle code may invoke [`KvStore::delete_prefix`].
    ///
    /// Existing custom stores retain namespaces by default. Opting in promises
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
    /// Existing custom stores retain data by default. Returning
    /// [`PrefixDeleteOutcome::Deleted`] promises that a successful reopen will
    /// not expose any pre-call key in this prefix.
    fn delete_prefix(&self, _prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        Ok(PrefixDeleteOutcome::Retained)
    }
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

    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Buffered)
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Retained
    }

    fn delete_prefix(&self, _prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

/// A [`KvStore`] view namespaced to one fragment.
///
/// Keys are prefixed with the fragment id (`frag_id` as big-endian
/// u32) before hitting the inner store, and the prefix is stripped on
/// load. Lets N fragment actors share one underlying store (e.g. one
/// RocksDB) without key collisions, and makes per-fragment operations
/// such as bounded load and durable namespace deletion possible.
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
    fn bind(&self, _identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Err(StoreError(
            "PrefixedStore is a fragment view and cannot be attached as a Node base store"
                .to_string(),
        ))
    }

    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        let mut out = Vec::new();
        for (key, value) in self.inner.load_prefix(&self.prefix)? {
            if let Some(stripped) = key.strip_prefix(self.prefix.as_slice()) {
                out.push((stripped.to_vec(), value));
            }
        }
        Ok(out)
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        self.inner.commit_durability()
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.inner.stable_fence()
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

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        self.inner.prefix_delete_support()
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        let mut prefixed = Vec::with_capacity(self.prefix.len() + prefix.len());
        prefixed.extend_from_slice(&self.prefix);
        prefixed.extend_from_slice(prefix);
        self.inner.delete_prefix(&prefixed)
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RocksDbOptions {
    durability: RocksDbDurability,
    adopt_legacy_manifest: bool,
}

#[cfg(feature = "rocksdb")]
impl RocksDbOptions {
    /// Select the WAL completion policy for ordinary data batches.
    pub fn with_durability(mut self, durability: RocksDbDurability) -> Self {
        self.durability = durability;
        self
    }

    /// Explicitly allow a non-empty pre-manifest database to be adopted by the
    /// first [`StoreIdentity`] supplied to [`KvStore::bind`].
    ///
    /// This is a one-time migration escape hatch. A legacy database contains
    /// no trustworthy original node id, so callers must verify the path and
    /// take a backup before enabling it. Existing, corrupt, or mismatched
    /// manifests are never overwritten.
    pub fn with_legacy_manifest_adoption(mut self, allow: bool) -> Self {
        self.adopt_legacy_manifest = allow;
        self
    }

    /// Current ordinary-write durability policy.
    pub fn durability(&self) -> RocksDbDurability {
        self.durability
    }

    /// Whether this open may adopt a non-empty pre-manifest database.
    pub fn legacy_manifest_adoption(&self) -> bool {
        self.adopt_legacy_manifest
    }
}

/// RocksDB-backed store. Available with the `rocksdb` cargo feature.
///
/// Values are encoded as `[timestamp_ms: u64 BE][expires_at_ms: u64 BE, 0 = none][is_tombstone: u8][node_id_len: u32 BE][node_id][value]`.
/// Fragment data lives in the default column family; an independent metadata
/// column family holds the node/layout manifest. Data operations require a
/// live [`StoreBinding`], and physical keys must begin with their four-byte
/// fragment id followed by the UTF-8 logical key.
#[cfg(feature = "rocksdb")]
pub struct RocksDbStore {
    db: rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
    options: RocksDbOptions,
    manifest_lock: std::sync::Mutex<()>,
    attached: Arc<std::sync::atomic::AtomicBool>,
    attached_identity: Arc<std::sync::Mutex<Option<StoreIdentity>>>,
    ordinary_write_options: rocksdb::WriteOptions,
    sync_write_options: rocksdb::WriteOptions,
}

#[cfg(feature = "rocksdb")]
const ROCKSDB_METADATA_CF: &str = "actordb-metadata";
#[cfg(feature = "rocksdb")]
const ROCKSDB_MANIFEST_KEY: &[u8] = b"manifest";
#[cfg(feature = "rocksdb")]
const ROCKSDB_MANIFEST_MAGIC: &[u8; 8] = b"ACTORDB\0";
#[cfg(feature = "rocksdb")]
const ROCKSDB_MANIFEST_VERSION: u32 = 1;
#[cfg(feature = "rocksdb")]
const ROCKSDB_DATA_FORMAT_VERSION: u32 = 1;
#[cfg(feature = "rocksdb")]
const ROCKSDB_PARTITIONER_VERSION: u32 = 1;

#[cfg(feature = "rocksdb")]
impl RocksDbStore {
    /// Open (creating if missing) the RocksDB at `path`.
    ///
    /// A non-empty default-CF-only legacy database is rejected without adding
    /// the metadata column family. Use [`RocksDbOptions`] for explicit adoption.
    /// The metadata column family is created only after a successful
    /// [`KvStore::bind`] validation.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        Self::open_with_options(path, RocksDbOptions::default())
    }

    /// Open RocksDB with an explicit WAL durability and legacy-manifest policy.
    /// Unknown column families are rejected instead of silently ignored.
    pub fn open_with_options(
        path: impl AsRef<std::path::Path>,
        options: RocksDbOptions,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(|error| StoreError(error.to_string()))?;
        let current_path = path.join("CURRENT");
        let has_current = match std::fs::symlink_metadata(&current_path) {
            Ok(metadata) if metadata.file_type().is_file() => true,
            Ok(_) => {
                return Err(StoreError(format!(
                    "RocksDB CURRENT path is not a regular file: {}",
                    current_path.display()
                )))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut entries = std::fs::read_dir(path)
                    .map_err(|read_error| StoreError(read_error.to_string()))?;
                if entries
                    .next()
                    .transpose()
                    .map_err(|read_error| StoreError(read_error.to_string()))?
                    .is_some()
                {
                    return Err(StoreError(format!(
                        "refusing to initialize non-empty RocksDB directory without CURRENT: {}",
                        path.display()
                    )));
                }
                false
            }
            Err(error) => {
                return Err(StoreError(format!(
                    "cannot inspect RocksDB CURRENT file {}: {error}",
                    current_path.display()
                )))
            }
        };
        let mut db_options = rocksdb::Options::default();
        db_options.create_if_missing(true);
        let db = if has_current {
            let column_families = rocksdb::DB::list_cf(&rocksdb::Options::default(), path)
                .map_err(|error| StoreError(error.to_string()))?;
            if !column_families
                .iter()
                .any(|name| name == rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            {
                return Err(StoreError(
                    "RocksDB is missing its default column family".to_string(),
                ));
            }
            if let Some(unknown) = column_families.iter().find(|name| {
                name.as_str() != rocksdb::DEFAULT_COLUMN_FAMILY_NAME
                    && name.as_str() != ROCKSDB_METADATA_CF
            }) {
                return Err(StoreError(format!(
                    "RocksDB contains unsupported column family {unknown:?}"
                )));
            }

            if column_families
                .iter()
                .any(|name| name == ROCKSDB_METADATA_CF)
            {
                let descriptors = column_families.into_iter().map(|name| {
                    rocksdb::ColumnFamilyDescriptor::new(name, rocksdb::Options::default())
                });
                rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open_cf_descriptors(
                    &db_options,
                    path,
                    descriptors,
                )
                .map_err(|error| StoreError(error.to_string()))?
            } else {
                // Probe an old default-CF-only database while holding its lock.
                // A strict open of non-empty legacy data must not create a
                // metadata CF and thereby prevent rollback to the old binary.
                let db =
                    rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open(&db_options, path)
                        .map_err(|error| StoreError(error.to_string()))?;
                let has_legacy_data = db
                    .iterator(rocksdb::IteratorMode::Start)
                    .next()
                    .transpose()
                    .map_err(|error| StoreError(error.to_string()))?
                    .is_some();
                if has_legacy_data && !options.adopt_legacy_manifest {
                    return Err(StoreError(
                        "non-empty RocksDB has no actordb manifest; reopen with explicit legacy manifest adoption after verifying and backing up the database"
                            .to_string(),
                    ));
                }
                db
            }
        } else {
            rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open(&db_options, path)
                .map_err(|error| StoreError(error.to_string()))?
        };
        let mut ordinary_write_options = rocksdb::WriteOptions::default();
        ordinary_write_options.set_sync(matches!(options.durability, RocksDbDurability::SyncWal));
        let mut sync_write_options = rocksdb::WriteOptions::default();
        sync_write_options.set_sync(true);
        Ok(Self {
            db,
            options,
            manifest_lock: std::sync::Mutex::new(()),
            attached: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            attached_identity: Arc::new(std::sync::Mutex::new(None)),
            ordinary_write_options,
            sync_write_options,
        })
    }

    /// Ordinary-write durability selected when this database was opened.
    pub fn durability(&self) -> RocksDbDurability {
        self.options.durability
    }

    fn write_options(&self, force_sync: bool) -> &rocksdb::WriteOptions {
        if force_sync {
            &self.sync_write_options
        } else {
            &self.ordinary_write_options
        }
    }

    fn metadata_cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, StoreError> {
        self.db
            .cf_handle(ROCKSDB_METADATA_CF)
            .ok_or_else(|| StoreError("RocksDB metadata column family is missing".to_string()))
    }

    fn encode_manifest(identity: &StoreIdentity) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(28 + identity.node_id.len());
        encoded.extend_from_slice(ROCKSDB_MANIFEST_MAGIC);
        encoded.extend_from_slice(&ROCKSDB_MANIFEST_VERSION.to_be_bytes());
        encoded.extend_from_slice(&ROCKSDB_DATA_FORMAT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&ROCKSDB_PARTITIONER_VERSION.to_be_bytes());
        encoded.extend_from_slice(&identity.fragment_count.to_be_bytes());
        encoded.extend_from_slice(&(identity.node_id.len() as u32).to_be_bytes());
        encoded.extend_from_slice(identity.node_id.as_bytes());
        encoded
    }

    fn decode_manifest(encoded: &[u8]) -> Result<StoreIdentity, StoreError> {
        const HEADER_LEN: usize = 28;
        if encoded.len() < HEADER_LEN {
            return Err(StoreError(format!(
                "RocksDB manifest is truncated: {} bytes",
                encoded.len()
            )));
        }
        if &encoded[..8] != ROCKSDB_MANIFEST_MAGIC {
            return Err(StoreError("RocksDB manifest has invalid magic".to_string()));
        }
        let manifest_version = u32::from_be_bytes(encoded[8..12].try_into().unwrap());
        if manifest_version != ROCKSDB_MANIFEST_VERSION {
            return Err(StoreError(format!(
                "unsupported RocksDB manifest version {manifest_version} (expected {ROCKSDB_MANIFEST_VERSION})"
            )));
        }
        let data_version = u32::from_be_bytes(encoded[12..16].try_into().unwrap());
        if data_version != ROCKSDB_DATA_FORMAT_VERSION {
            return Err(StoreError(format!(
                "unsupported RocksDB data format version {data_version} (expected {ROCKSDB_DATA_FORMAT_VERSION})"
            )));
        }
        let partitioner_version = u32::from_be_bytes(encoded[16..20].try_into().unwrap());
        if partitioner_version != ROCKSDB_PARTITIONER_VERSION {
            return Err(StoreError(format!(
                "unsupported RocksDB partitioner version {partitioner_version} (expected {ROCKSDB_PARTITIONER_VERSION})"
            )));
        }
        let fragment_count = u32::from_be_bytes(encoded[20..24].try_into().unwrap());
        let node_id_len = u32::from_be_bytes(encoded[24..28].try_into().unwrap()) as usize;
        let expected_len = HEADER_LEN.checked_add(node_id_len).ok_or_else(|| {
            StoreError("RocksDB manifest node id length overflowed usize".to_string())
        })?;
        if encoded.len() != expected_len {
            return Err(StoreError(format!(
                "RocksDB manifest length mismatch: declared node id {node_id_len} bytes, total {}",
                encoded.len()
            )));
        }
        let node_id = String::from_utf8(encoded[HEADER_LEN..].to_vec()).map_err(|error| {
            StoreError(format!("RocksDB manifest node id is not UTF-8: {error}"))
        })?;
        check_node_id_budget(node_id.len()).map_err(|error| {
            StoreError(format!("RocksDB manifest has invalid node id: {error}"))
        })?;
        if fragment_count == 0 {
            return Err(StoreError(
                "RocksDB manifest fragment count is zero".to_string(),
            ));
        }
        Ok(StoreIdentity::new(node_id, fragment_count))
    }

    fn validate_legacy_records(&self, identity: &StoreIdentity) -> Result<(), StoreError> {
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, encoded) = item.map_err(|error| StoreError(error.to_string()))?;
            let prefix: [u8; 4] = key
                .get(..4)
                .ok_or_else(|| {
                    StoreError(format!(
                        "legacy RocksDB key is shorter than the 4-byte fragment prefix: {} bytes",
                        key.len()
                    ))
                })?
                .try_into()
                .unwrap();
            Self::preflight_encoded_record(key.len() - 4, &encoded)?;
            let fragment = u32::from_be_bytes(prefix);
            if fragment >= identity.fragment_count {
                return Err(StoreError(format!(
                    "legacy RocksDB key belongs to fragment {fragment}, outside configured count {}",
                    identity.fragment_count
                )));
            }
            let logical_key = std::str::from_utf8(&key[4..]).map_err(|error| {
                StoreError(format!("legacy RocksDB logical key is not UTF-8: {error}"))
            })?;
            let expected = fragment_of(logical_key, identity.fragment_count as usize);
            if expected.0 != fragment {
                return Err(StoreError(format!(
                    "legacy RocksDB key is stored under fragment {fragment} but maps to fragment {} for configured count {}",
                    expected.0, identity.fragment_count
                )));
            }
            Self::decode(&encoded).map_err(StoreError)?;
        }
        Ok(())
    }

    fn bind_manifest(&self, identity: &StoreIdentity) -> Result<(), StoreError> {
        check_node_id_budget(identity.node_id.len()).map_err(|error| {
            StoreError(format!(
                "requested RocksDB identity has invalid node id: {error}"
            ))
        })?;
        if identity.fragment_count == 0 {
            return Err(StoreError(
                "requested RocksDB identity has zero fragments".to_string(),
            ));
        }
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| StoreError("RocksDB manifest lock was poisoned".to_string()))?;
        if let Some(metadata) = self.db.cf_handle(ROCKSDB_METADATA_CF) {
            for item in self.db.iterator_cf(&metadata, rocksdb::IteratorMode::Start) {
                let (key, _) = item.map_err(|error| StoreError(error.to_string()))?;
                if key.as_ref() != ROCKSDB_MANIFEST_KEY {
                    return Err(StoreError(format!(
                        "RocksDB metadata column family contains an unsupported key ({} bytes)",
                        key.len()
                    )));
                }
            }
            if let Some(encoded) = self
                .db
                .get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
                .map_err(|error| StoreError(error.to_string()))?
            {
                let actual = Self::decode_manifest(&encoded)?;
                if &actual != identity {
                    return Err(StoreError(format!(
                        "RocksDB identity mismatch: stored node {:?} with {} fragments, requested node {:?} with {} fragments",
                        actual.node_id,
                        actual.fragment_count,
                        identity.node_id,
                        identity.fragment_count
                    )));
                }
                return Ok(());
            }
        }

        let has_legacy_data = self
            .db
            .iterator(rocksdb::IteratorMode::Start)
            .next()
            .transpose()
            .map_err(|error| StoreError(error.to_string()))?
            .is_some();
        if has_legacy_data && !self.options.adopt_legacy_manifest {
            return Err(StoreError(
                "non-empty RocksDB has no actordb manifest; reopen with explicit legacy manifest adoption after verifying and backing up the database"
                    .to_string(),
            ));
        }
        if has_legacy_data {
            self.validate_legacy_records(identity)?;
        }

        // Do not change a legacy database's column-family layout until its
        // complete contents have been validated against the requested
        // identity. This preserves rollback after a failed adoption attempt.
        if self.db.cf_handle(ROCKSDB_METADATA_CF).is_none() {
            self.db
                .create_cf(ROCKSDB_METADATA_CF, &rocksdb::Options::default())
                .map_err(|error| StoreError(error.to_string()))?;
        }
        let metadata = self.metadata_cf()?;

        // Identity metadata is always synchronously committed. Losing a
        // manifest while retaining its data would make a later mount
        // ambiguous even when ordinary batches use AsyncWal.
        self.db
            .put_cf_opt(
                &metadata,
                ROCKSDB_MANIFEST_KEY,
                Self::encode_manifest(identity),
                &self.sync_write_options,
            )
            .map_err(|error| StoreError(error.to_string()))
    }

    fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut end = prefix.to_vec();
        for index in (0..end.len()).rev() {
            if end[index] != u8::MAX {
                end[index] += 1;
                end.truncate(index + 1);
                return Some(end);
            }
        }
        None
    }

    fn attached_identity(&self) -> Result<StoreIdentity, StoreError> {
        self.attached_identity
            .lock()
            .map_err(|_| StoreError("RocksDB attachment lock was poisoned".to_string()))?
            .clone()
            .ok_or_else(|| {
                StoreError("RocksDB data operation requires a live StoreBinding".to_string())
            })
    }

    fn validate_fragment_prefix(
        key_or_prefix: &[u8],
        identity: &StoreIdentity,
    ) -> Result<u32, StoreError> {
        let prefix: [u8; 4] = key_or_prefix
            .get(..4)
            .ok_or_else(|| {
                StoreError(format!(
                    "RocksDB data key/prefix is shorter than the 4-byte fragment prefix: {} bytes",
                    key_or_prefix.len()
                ))
            })?
            .try_into()
            .unwrap();
        let fragment = u32::from_be_bytes(prefix);
        if fragment >= identity.fragment_count {
            return Err(StoreError(format!(
                "RocksDB data key/prefix belongs to fragment {fragment}, outside bound count {}",
                identity.fragment_count
            )));
        }
        Ok(fragment)
    }

    fn validate_physical_key<'a>(
        key: &'a [u8],
        identity: &StoreIdentity,
    ) -> Result<&'a str, StoreError> {
        let fragment = Self::validate_fragment_prefix(key, identity)?;
        let logical_key = std::str::from_utf8(&key[4..])
            .map_err(|error| StoreError(format!("RocksDB logical key is not UTF-8: {error}")))?;
        let expected = fragment_of(logical_key, identity.fragment_count as usize);
        if expected.0 != fragment {
            return Err(StoreError(format!(
                "RocksDB key is stored under fragment {fragment} but maps to fragment {} for bound count {}",
                expected.0, identity.fragment_count
            )));
        }
        Ok(logical_key)
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

    fn encoded_record_layout(bytes: &[u8]) -> Result<(bool, usize), String> {
        if bytes.len() < 21 {
            return Err(format!("value too short: {} bytes", bytes.len()));
        }
        let is_tombstone = match bytes[16] {
            0 => false,
            1 => true,
            flag => return Err(format!("invalid tombstone flag {flag}; expected 0 or 1")),
        };
        let node_id_len = u32::from_be_bytes(bytes[17..21].try_into().unwrap()) as usize;
        let value_offset = 21usize
            .checked_add(node_id_len)
            .ok_or_else(|| "node_id length overflowed usize".to_string())?;
        if bytes.len() < value_offset {
            return Err(format!(
                "node_id overruns value: id_len={node_id_len} total={}",
                bytes.len()
            ));
        }
        Ok((is_tombstone, value_offset))
    }

    fn preflight_encoded_record(logical_key_len: usize, bytes: &[u8]) -> Result<(), StoreError> {
        let (_, value_offset) = Self::encoded_record_layout(bytes).map_err(StoreError)?;
        let node_id_len = value_offset - 21;
        check_record_budget(logical_key_len, bytes.len() - value_offset, node_id_len).map_err(
            |error| {
                StoreError(format!(
                    "persisted record (key bytes {logical_key_len}) cannot be replicated: {error}"
                ))
            },
        )
    }

    fn decode(bytes: &[u8]) -> Result<VersionedValue, String> {
        let (is_tombstone, value_offset) = Self::encoded_record_layout(bytes)?;
        let timestamp_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let expires_raw = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let expires_at_ms = if expires_raw == 0 {
            None
        } else {
            Some(expires_raw)
        };
        let node_id = std::str::from_utf8(&bytes[21..value_offset])
            .map_err(|error| format!("node_id not UTF-8: {error}"))?
            .to_owned();
        let value = bytes[value_offset..].to_vec();
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
        let identity = self.attached_identity()?;
        if let Some(prefix) = prefix {
            Self::validate_fragment_prefix(prefix, &identity)?;
        }
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.fill_cache(false);
        let mode = if let Some(prefix) = prefix {
            let end = Self::prefix_end(prefix).ok_or_else(|| {
                StoreError("RocksDB prefix has no finite lexicographic upper bound".to_string())
            })?;
            read_options.set_iterate_lower_bound(prefix.to_vec());
            read_options.set_iterate_upper_bound(end);
            rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward)
        } else {
            rocksdb::IteratorMode::Start
        };
        let mut out = Vec::new();
        // Validate each raw record before decoding or retaining another owned
        // copy. Prefix iteration also prevents one full-DB scan per fragment.
        for item in self.db.iterator_opt(mode, read_options) {
            let (key, encoded) = item.map_err(|error| StoreError(error.to_string()))?;
            if prefix.is_some_and(|prefix| !key.starts_with(prefix)) {
                break;
            }
            let logical_key = Self::validate_physical_key(&key, &identity)?;
            Self::preflight_encoded_record(logical_key.len(), &encoded)?;
            let value = Self::decode(&encoded).map_err(StoreError)?;
            out.push((key.to_vec(), value));
        }
        Ok(out)
    }
}

#[cfg(feature = "rocksdb")]
impl KvStore for RocksDbStore {
    fn bind(&self, identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        use std::sync::atomic::Ordering;

        self.attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                StoreError("RocksDB store is already attached to a live actordb node".to_string())
            })?;
        if let Err(error) = self.bind_manifest(identity) {
            self.attached.store(false, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = self
            .attached_identity
            .lock()
            .map(|mut attached_identity| *attached_identity = Some(identity.clone()))
        {
            self.attached.store(false, Ordering::Release);
            return Err(StoreError(format!(
                "RocksDB attachment lock was poisoned: {error}"
            )));
        }
        let attached = self.attached.clone();
        let attached_identity = self.attached_identity.clone();
        Ok(StoreBinding::managed(move || {
            if let Ok(mut identity) = attached_identity.lock() {
                *identity = None;
            }
            attached.store(false, Ordering::Release);
        }))
    }

    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_matching(None)
    }

    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.load_matching(Some(prefix))
    }

    fn commit_durability(&self) -> StoreCommitDurability {
        match self.options.durability {
            RocksDbDurability::AsyncWal => StoreCommitDurability::Buffered,
            RocksDbDurability::SyncWal => StoreCommitDurability::Stable,
        }
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.attached_identity()?;
        self.db
            .flush_wal(true)
            .map_err(|error| StoreError(error.to_string()))?;
        Ok(StoreCommitDurability::Stable)
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let identity = self.attached_identity()?;
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in writes {
            let logical_key = Self::validate_physical_key(key, &identity)?;
            check_record_budget(logical_key.len(), value.value.len(), value.node_id.len())
                .map_err(|error| {
                    StoreError(format!(
                        "refusing to persist record which cannot be replicated: {error}"
                    ))
                })?;
            batch.put(key, Self::encode(value));
        }
        self.db
            .write_opt(batch, self.write_options(false))
            .map_err(|e| StoreError(e.to_string()))
    }

    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        PrefixDeleteSupport::Supported
    }

    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        let identity = self.attached_identity()?;
        Self::validate_fragment_prefix(prefix, &identity)?;
        let end = Self::prefix_end(prefix).ok_or_else(|| {
            StoreError("RocksDB prefix has no finite lexicographic upper bound".to_string())
        })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_range(prefix, &end);
        // Namespace retirement is always synchronous even when ordinary data
        // batches use AsyncWal: placement must never publish deletion before
        // the range tombstone can survive an operating-system crash.
        self.db
            .write_opt(batch, self.write_options(true))
            .map_err(|error| StoreError(error.to_string()))?;
        Ok(PrefixDeleteOutcome::Deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "rocksdb")]
    fn directory_snapshot(path: &std::path::Path) -> Vec<(std::ffi::OsString, Option<Vec<u8>>)> {
        let mut entries: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let contents = entry
                    .file_type()
                    .unwrap()
                    .is_file()
                    .then(|| std::fs::read(entry.path()).unwrap());
                (entry.file_name(), contents)
            })
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

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
            fn commit_durability(&self) -> StoreCommitDurability {
                StoreCommitDurability::Stable
            }
            fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
                Ok(StoreCommitDurability::Stable)
            }
            fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                self.written
                    .lock()
                    .unwrap()
                    .extend(writes.iter().map(|(k, v)| (k.clone(), v.clone())));
                Ok(())
            }
            fn prefix_delete_support(&self) -> PrefixDeleteSupport {
                PrefixDeleteSupport::Supported
            }
            fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
                self.written
                    .lock()
                    .unwrap()
                    .retain(|(key, _)| !key.starts_with(prefix));
                Ok(PrefixDeleteOutcome::Deleted)
            }
        }

        let inner = Arc::new(Rec {
            written: std::sync::Mutex::new(Vec::new()),
        });
        let f0 = PrefixedStore::new(inner.clone(), FragmentId(0));
        let f1 = PrefixedStore::new(inner.clone(), FragmentId(1));

        assert_eq!(f0.commit_durability(), StoreCommitDurability::Stable);
        assert_eq!(f0.stable_fence().unwrap(), StoreCommitDurability::Stable);
        assert_eq!(f0.prefix_delete_support(), PrefixDeleteSupport::Supported);

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

        assert_eq!(f0.delete_prefix(&[]).unwrap(), PrefixDeleteOutcome::Deleted);
        assert!(f0.load().unwrap().is_empty());
        assert_eq!(f1.load().unwrap().len(), 1);
        assert!(f1
            .bind(&StoreIdentity::new("node".to_string(), 2))
            .err()
            .expect("a PrefixedStore was accepted as a Node base store")
            .to_string()
            .contains("cannot be attached as a Node base store"));
    }

    #[test]
    fn memory_store_is_a_noop() {
        let s = MemoryStore;
        assert_eq!(s.commit_durability(), StoreCommitDurability::Buffered);
        assert_eq!(s.stable_fence().unwrap(), StoreCommitDurability::Buffered);
        assert_eq!(s.prefix_delete_support(), PrefixDeleteSupport::Retained);
        assert!(s.load().unwrap().is_empty());
        s.write_batch(&[(
            b"k".to_vec(),
            VersionedValue::new(vec![1], 1, "n".to_string()),
        )])
        .unwrap();
        assert!(s.load().unwrap().is_empty());
    }

    #[test]
    fn custom_store_capabilities_default_to_retention_and_buffering() {
        struct CustomStore;

        impl KvStore for CustomStore {
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

        let mut invalid_tombstone =
            RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, "node".to_string()));
        invalid_tombstone[16] = 2;
        assert!(RocksDbStore::decode(&invalid_tombstone)
            .unwrap_err()
            .contains("invalid tombstone flag 2"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_prefix_load_rejects_irreducible_record_before_decode() {
        use crate::wire::MAX_FRAME_BYTES;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let _binding = store
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .unwrap();
        let mut raw_key = 0u32.to_be_bytes().to_vec();
        raw_key.resize(4 + MAX_FRAME_BYTES / 2, b'k');
        let encoded = RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, "node".to_string()));
        let error = store
            .write_batch(&[(
                raw_key.clone(),
                VersionedValue::new(Vec::new(), 1, "node".to_string()),
            )])
            .expect_err("an oversized direct RocksDB write was accepted");
        assert!(error.to_string().contains("cannot be replicated"));
        store.db.put(raw_key, encoded).unwrap();

        let error = PrefixedStore::new(store, FragmentId(0))
            .load()
            .expect_err("oversized persisted key was accepted");
        assert!(error.to_string().contains("cannot be replicated"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_rejects_invalid_tombstone_on_load_and_legacy_adoption() {
        let mut encoded = RocksDbStore::encode(&VersionedValue::new(
            b"value".to_vec(),
            1,
            "writer".to_string(),
        ));
        encoded[16] = 2;
        let mut key = 0u32.to_be_bytes().to_vec();
        key.extend_from_slice(b"key");

        let normal_dir = tempfile::tempdir().unwrap();
        let normal = RocksDbStore::open(normal_dir.path()).unwrap();
        let _binding = normal
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .unwrap();
        normal.db.put(&key, &encoded).unwrap();
        let error = normal
            .load()
            .expect_err("an invalid persisted tombstone flag was accepted");
        assert!(error.to_string().contains("invalid tombstone flag 2"));

        let legacy_dir = tempfile::tempdir().unwrap();
        {
            let legacy = rocksdb::DB::open_default(legacy_dir.path()).unwrap();
            legacy.put(&key, &encoded).unwrap();
        }
        let adopting = RocksDbStore::open_with_options(
            legacy_dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .unwrap();
        let error = adopting
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .err()
            .expect("legacy adoption accepted an invalid tombstone flag");
        assert!(error.to_string().contains("invalid tombstone flag 2"));
        assert!(adopting.db.cf_handle(ROCKSDB_METADATA_CF).is_none());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_legacy_adoption_preflights_wire_budget_before_decode() {
        use crate::wire::MAX_FRAME_BYTES;

        let dir = tempfile::tempdir().unwrap();
        let mut key = 0u32.to_be_bytes().to_vec();
        key.extend_from_slice(b"key");
        let mut encoded = vec![0; 21 + 6 + MAX_FRAME_BYTES];
        encoded[17..21].copy_from_slice(&6u32.to_be_bytes());
        encoded[21..27].copy_from_slice(b"writer");
        encoded[21] = 0xff;
        {
            let legacy = rocksdb::DB::open_default(dir.path()).unwrap();
            legacy.put(key, encoded).unwrap();
        }

        let adopting = RocksDbStore::open_with_options(
            dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .unwrap();
        let error = adopting
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .err()
            .expect("an irreducibly large legacy record was adopted");
        assert!(error.to_string().contains("cannot be replicated"));
        assert!(!error.to_string().contains("not UTF-8"));
        assert!(adopting.db.cf_handle(ROCKSDB_METADATA_CF).is_none());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_rejects_invalid_provenance_before_persisting() {
        use crate::wire::MAX_WIRE_NODE_ID_BYTES;

        let dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let _binding = store
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .unwrap();
        let mut key = 0u32.to_be_bytes().to_vec();
        key.extend_from_slice(b"invalid");
        for node_id in [String::new(), "n".repeat(MAX_WIRE_NODE_ID_BYTES + 1)] {
            let error = store
                .write_batch(&[(key.clone(), VersionedValue::new(Vec::new(), 1, node_id))])
                .expect_err("invalid provenance was persisted");
            assert!(error.to_string().contains("cannot be replicated"));
        }
        assert!(store.load().unwrap().is_empty());

        let mut corrupt_key = 0u32.to_be_bytes().to_vec();
        corrupt_key.extend_from_slice(b"legacy-invalid");
        store
            .db
            .put(
                corrupt_key,
                RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, String::new())),
            )
            .unwrap();
        let error = store
            .load()
            .expect_err("invalid legacy provenance was loaded");
        assert!(error.to_string().contains("record node id is 0 bytes"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_rejects_misrouted_writes_and_persisted_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let _binding = store
            .bind(&StoreIdentity::new("node".to_string(), 2))
            .unwrap();
        let logical_key = (0..1000)
            .map(|index| format!("misrouted-{index}"))
            .find(|key| fragment_of(key, 2) == FragmentId(1))
            .unwrap();
        let mut wrong_physical_key = 0u32.to_be_bytes().to_vec();
        wrong_physical_key.extend_from_slice(logical_key.as_bytes());
        let value = VersionedValue::new(b"value".to_vec(), 1, "writer".to_string());

        let error = store
            .write_batch(&[(wrong_physical_key.clone(), value.clone())])
            .expect_err("a key was persisted under the wrong fragment");
        assert!(error.to_string().contains("maps to fragment 1"));

        store
            .db
            .put(wrong_physical_key, RocksDbStore::encode(&value))
            .unwrap();
        let error = PrefixedStore::new(store, FragmentId(0))
            .load()
            .expect_err("a persisted key under the wrong fragment was loaded");
        assert!(error.to_string().contains("maps to fragment 1"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_options_default_to_sync_wal() {
        let dir = tempfile::tempdir().unwrap();
        let identity = StoreIdentity::new("node".to_string(), 1);
        let store = RocksDbStore::open(dir.path()).unwrap();
        assert_eq!(store.durability(), RocksDbDurability::SyncWal);
        assert_eq!(store.commit_durability(), StoreCommitDurability::Stable);
        assert_eq!(
            store.prefix_delete_support(),
            PrefixDeleteSupport::Supported
        );
        let binding = store.bind(&identity).unwrap();
        assert_eq!(store.stable_fence().unwrap(), StoreCommitDurability::Stable);
        drop(binding);
        drop(store);

        let store = RocksDbStore::open_with_options(
            dir.path(),
            RocksDbOptions::default().with_durability(RocksDbDurability::AsyncWal),
        )
        .unwrap();
        assert_eq!(store.durability(), RocksDbDurability::AsyncWal);
        assert_eq!(store.commit_durability(), StoreCommitDurability::Buffered);
        assert_eq!(
            store.prefix_delete_support(),
            PrefixDeleteSupport::Supported
        );
        let _binding = store.bind(&identity).unwrap();
        let mut key = 0u32.to_be_bytes().to_vec();
        key.extend_from_slice(b"key");
        store
            .write_batch(&[(
                key,
                VersionedValue::new(b"value".to_vec(), 1, "writer".to_string()),
            )])
            .unwrap();
        assert_eq!(store.stable_fence().unwrap(), StoreCommitDurability::Stable);
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_missing_current_never_initializes_a_nonempty_directory() {
        let sentinel_dir = tempfile::tempdir().unwrap();
        std::fs::write(sentinel_dir.path().join("orphan.sst"), b"sentinel").unwrap();
        let before = directory_snapshot(sentinel_dir.path());
        let error = RocksDbStore::open(sentinel_dir.path())
            .err()
            .expect("a nonempty directory without CURRENT was initialized");
        assert!(error.to_string().contains("without CURRENT"));
        assert_eq!(directory_snapshot(sentinel_dir.path()), before);

        let damaged_dir = tempfile::tempdir().unwrap();
        {
            let db = rocksdb::DB::open_default(damaged_dir.path()).unwrap();
            db.put(b"key", b"value").unwrap();
        }
        std::fs::rename(
            damaged_dir.path().join("CURRENT"),
            damaged_dir.path().join("CURRENT.saved"),
        )
        .unwrap();
        let before = directory_snapshot(damaged_dir.path());
        let error = RocksDbStore::open_with_options(
            damaged_dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .err()
        .expect("a damaged RocksDB without CURRENT was reinitialized");
        assert!(error.to_string().contains("without CURRENT"));
        assert_eq!(directory_snapshot(damaged_dir.path()), before);
        assert!(!damaged_dir.path().join("CURRENT").exists());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_manifest_binds_identity_and_live_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let identity = StoreIdentity::new("node-a".to_string(), 4);

        let binding = store.bind(&identity).unwrap();
        let error = store
            .bind(&identity)
            .err()
            .expect("a second live attachment was accepted");
        assert!(error.to_string().contains("already attached"));
        drop(binding);

        drop(store.bind(&identity).unwrap());
        for mismatch in [
            StoreIdentity::new("node-b".to_string(), 4),
            StoreIdentity::new("node-a".to_string(), 5),
        ] {
            let error = store
                .bind(&mismatch)
                .err()
                .expect("a mismatched identity was accepted");
            assert!(error.to_string().contains("identity mismatch"));
        }
        drop(store);

        let reopened = RocksDbStore::open(dir.path()).unwrap();
        drop(reopened.bind(&identity).unwrap());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_manifest_attachment_is_atomic_across_threads() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Barrier;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let start = Arc::new(Barrier::new(3));
        let attempted = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let successes = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let store = store.clone();
            let start = start.clone();
            let attempted = attempted.clone();
            let release = release.clone();
            let successes = successes.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                let binding = store.bind(&StoreIdentity::new("node".to_string(), 1)).ok();
                if binding.is_some() {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                attempted.wait();
                release.wait();
                drop(binding);
            }));
        }
        start.wait();
        attempted.wait();
        assert_eq!(successes.load(Ordering::SeqCst), 1);
        release.wait();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_manifest_rejects_corruption_without_overwriting_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let identity = StoreIdentity::new("node".to_string(), 1);
        drop(store.bind(&identity).unwrap());
        let metadata = store.metadata_cf().unwrap();
        let mut future = RocksDbStore::encode_manifest(&identity);
        future[8..12].copy_from_slice(&(ROCKSDB_MANIFEST_VERSION + 1).to_be_bytes());
        store
            .db
            .put_cf(&metadata, ROCKSDB_MANIFEST_KEY, &future)
            .unwrap();

        let error = store
            .bind(&identity)
            .err()
            .expect("a future manifest was accepted");
        assert!(error.to_string().contains("unsupported RocksDB manifest"));
        assert_eq!(
            store
                .db
                .get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
                .unwrap()
                .unwrap(),
            future
        );
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_nonempty_legacy_database_requires_explicit_adoption() {
        let dir = tempfile::tempdir().unwrap();
        let identity = StoreIdentity::new("legacy-owner".to_string(), 2);
        let logical_key = (0..1000)
            .map(|index| format!("legacy-{index}"))
            .find(|key| fragment_of(key, 2) == FragmentId(1))
            .unwrap();
        let key = {
            let mut key = 1u32.to_be_bytes().to_vec();
            key.extend_from_slice(logical_key.as_bytes());
            key
        };
        {
            let legacy = rocksdb::DB::open_default(dir.path()).unwrap();
            legacy
                .put(
                    &key,
                    RocksDbStore::encode(&VersionedValue::new(
                        b"value".to_vec(),
                        1,
                        "writer".to_string(),
                    )),
                )
                .unwrap();
        }
        let error = RocksDbStore::open(dir.path())
            .err()
            .expect("non-empty legacy database was silently mutated and opened");
        assert!(error
            .to_string()
            .contains("explicit legacy manifest adoption"));
        assert_eq!(
            rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap(),
            vec![rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string()]
        );
        drop(rocksdb::DB::open_default(dir.path()).unwrap());

        let adopting = RocksDbStore::open_with_options(
            dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .unwrap();
        assert!(adopting.db.cf_handle(ROCKSDB_METADATA_CF).is_none());
        let binding = adopting.bind(&identity).unwrap();
        assert!(adopting.db.cf_handle(ROCKSDB_METADATA_CF).is_some());
        assert_eq!(adopting.load().unwrap()[0].0, key);
        drop(binding);
        drop(adopting);

        let strict = RocksDbStore::open(dir.path()).unwrap();
        drop(strict.bind(&identity).unwrap());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_legacy_adoption_rejects_wrong_partition_mapping_without_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let logical_key = (0..1000)
            .map(|index| format!("mismatch-{index}"))
            .find(|key| fragment_of(key, 2) == FragmentId(1))
            .unwrap();
        let mut physical_key = 0u32.to_be_bytes().to_vec();
        physical_key.extend_from_slice(logical_key.as_bytes());
        {
            let legacy = rocksdb::DB::open_default(dir.path()).unwrap();
            legacy
                .put(
                    physical_key,
                    RocksDbStore::encode(&VersionedValue::new(
                        b"value".to_vec(),
                        1,
                        "writer".to_string(),
                    )),
                )
                .unwrap();
        }

        let adopting = RocksDbStore::open_with_options(
            dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .unwrap();
        let error = adopting
            .bind(&StoreIdentity::new("legacy-owner".to_string(), 2))
            .err()
            .expect("legacy data was adopted under an incompatible fragment count");
        assert!(error.to_string().contains("maps to fragment 1"));
        assert!(adopting.db.cf_handle(ROCKSDB_METADATA_CF).is_none());
        drop(adopting);

        let retry = RocksDbStore::open_with_options(
            dir.path(),
            RocksDbOptions::default().with_legacy_manifest_adoption(true),
        )
        .unwrap();
        drop(
            retry
                .bind(&StoreIdentity::new("legacy-owner".to_string(), 1))
                .unwrap(),
        );
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_rejects_unknown_column_families_and_metadata_keys() {
        let unknown_cf_dir = tempfile::tempdir().unwrap();
        {
            let mut db = rocksdb::DB::open_default(unknown_cf_dir.path()).unwrap();
            db.create_cf("future-data", &rocksdb::Options::default())
                .unwrap();
        }
        let error = RocksDbStore::open(unknown_cf_dir.path())
            .err()
            .expect("unknown column family was silently accepted");
        assert!(error.to_string().contains("unsupported column family"));

        let unknown_metadata_dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(unknown_metadata_dir.path()).unwrap();
        store
            .db
            .create_cf(ROCKSDB_METADATA_CF, &rocksdb::Options::default())
            .unwrap();
        let metadata = store.metadata_cf().unwrap();
        store.db.put_cf(&metadata, b"future-key", b"value").unwrap();
        let error = store
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .err()
            .expect("unknown metadata was silently accepted");
        assert!(error.to_string().contains("unsupported key"));
        assert!(store
            .db
            .get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
            .unwrap()
            .is_none());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_prefix_delete_is_isolated_idempotent_and_persistent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let identity = StoreIdentity::new("node".to_string(), 2);
        let binding = store.bind(&identity).unwrap();
        let fragment_zero = PrefixedStore::new(store.clone(), FragmentId(0));
        let fragment_one = PrefixedStore::new(store.clone(), FragmentId(1));
        let value = VersionedValue::new(b"value".to_vec(), 1, "node".to_string());
        let fragment_zero_keys: Vec<Vec<u8>> = (0..1000)
            .map(|index| format!("zero-{index}"))
            .filter(|key| fragment_of(key, 2) == FragmentId(0))
            .take(2)
            .map(String::into_bytes)
            .collect();
        let fragment_one_key = (0..1000)
            .map(|index| format!("one-{index}"))
            .find(|key| fragment_of(key, 2) == FragmentId(1))
            .unwrap()
            .into_bytes();
        fragment_zero
            .write_batch(&[
                (fragment_zero_keys[0].clone(), value.clone()),
                (fragment_zero_keys[1].clone(), value.clone()),
            ])
            .unwrap();
        fragment_one
            .write_batch(&[(fragment_one_key, value)])
            .unwrap();

        assert_eq!(
            fragment_zero.delete_prefix(&[]).unwrap(),
            PrefixDeleteOutcome::Deleted
        );
        assert_eq!(
            fragment_zero.delete_prefix(&[]).unwrap(),
            PrefixDeleteOutcome::Deleted
        );
        assert!(fragment_zero.load().unwrap().is_empty());
        assert_eq!(fragment_one.load().unwrap().len(), 1);
        drop(fragment_zero);
        drop(fragment_one);
        drop(binding);
        drop(store);

        let reopened = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let _binding = reopened.bind(&identity).unwrap();
        assert!(PrefixedStore::new(reopened.clone(), FragmentId(0))
            .load()
            .unwrap()
            .is_empty());
        assert_eq!(
            PrefixedStore::new(reopened, FragmentId(1))
                .load()
                .unwrap()
                .len(),
            1
        );
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_prefix_delete_rejects_invalid_prefixes_without_scanning() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let _binding = store
            .bind(&StoreIdentity::new("node".to_string(), 1))
            .unwrap();
        let value = VersionedValue::new(b"value".to_vec(), 1, "node".to_string());
        let fragment = PrefixedStore::new(store.clone(), FragmentId(0));
        fragment.write_batch(&[(b"key".to_vec(), value)]).unwrap();
        assert!(store.delete_prefix(&[]).is_err());
        assert!(store.delete_prefix(&[0xff]).is_err());
        assert!(store.delete_prefix(&[0xff; 4]).is_err());
        assert_eq!(fragment.load().unwrap().len(), 1);
    }
}
