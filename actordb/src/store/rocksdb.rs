// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Offered RocksDB backend, enabled by the `rocksdb` Cargo feature.
//!
//! [`RocksDbStore`] owns the engine, record codec, identity manifest, attachment
//! lease, and durable prefix operations. Shared storage contracts and opening
//! policies stay in the parent [`crate::store`] module.

use std::sync::Arc;

use ::rocksdb as rocksdb_engine;

use crate::actor::VersionedValue;
use crate::shard::fragment_of;
use crate::wire::{check_node_id_budget, check_record_budget};

use super::{
    KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, RocksDbDurability, RocksDbOptions,
    StoreBinding, StoreCommitDurability, StoreError, StoreIdentity,
};

/// RocksDB-backed store. Available with the `rocksdb` cargo feature.
///
/// Values use format3 only: `[ACTVAL03][physical: u64 BE][logical: u64 BE]\n/// [expiry: u64 BE, 0 = none][tombstone: u8][writer_len: u32 BE][writer][value]`.
/// Fragment data lives in the default column family; an independent metadata
/// column family holds the node/layout manifest. Data operations require a
/// live [`StoreBinding`], and physical keys must begin with their four-byte
/// fragment id followed by a UTF-8 logical key, or a validated physical table
/// prefix and its matching encoded logical table key. Record wire admission
/// measures the logical key, not additional durable namespace bytes.
pub struct RocksDbStore {
    db: Arc<rocksdb_engine::DBWithThreadMode<rocksdb_engine::MultiThreaded>>,
    options: RocksDbOptions,
    manifest_lock: std::sync::Mutex<()>,
    attached: Arc<std::sync::atomic::AtomicBool>,
    attached_identity: Arc<std::sync::Mutex<Option<StoreIdentity>>>,
    ordinary_write_options: rocksdb_engine::WriteOptions,
    sync_write_options: rocksdb_engine::WriteOptions,
}

const ROCKSDB_METADATA_CF: &str = "actordb-metadata";
const ROCKSDB_MANIFEST_KEY: &[u8] = b"manifest";
const ROCKSDB_MANIFEST_MAGIC: &[u8; 8] = b"ACTORDB\0";
const ROCKSDB_MANIFEST_VERSION: u32 = 3;
const ROCKSDB_DATA_FORMAT_VERSION: u32 = 3;
const ROCKSDB_PARTITIONER_VERSION: u32 = 1;
const ROCKSDB_FORMAT_KEY: &[u8] = b"format";
const ROCKSDB_RECORD_MAGIC: &[u8; 8] = b"ACTVAL03";

mod checkpoint;

struct ReadOnlyLogs(std::path::PathBuf);
impl Drop for ReadOnlyLogs {
    fn drop(&mut self) {
        if let Ok(entries) = std::fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        let _ = std::fs::remove_dir(&self.0);
    }
}

impl RocksDbStore {
    /// Open (creating if missing) the RocksDB at `path`.
    ///
    /// Existing databases must carry format3 metadata and records. Older and
    /// unmanifested databases are rejected by a read-only preflight before any
    /// writable open. Fresh initialization never adopts existing data.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        Self::open_with_options(path, RocksDbOptions::default())
    }

    /// Open RocksDB with an explicit WAL durability policy.
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
        let mut db_options = rocksdb_engine::Options::default();
        db_options.create_if_missing(true);
        let db = if has_current {
            Self::preflight_existing(path)?;
            let descriptors = [
                rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME,
                ROCKSDB_METADATA_CF,
            ]
            .into_iter()
            .map(|name| {
                rocksdb_engine::ColumnFamilyDescriptor::new(
                    name,
                    rocksdb_engine::Options::default(),
                )
            });
            rocksdb_engine::DBWithThreadMode::<rocksdb_engine::MultiThreaded>::open_cf_descriptors(
                &db_options,
                path,
                descriptors,
            )
            .map_err(|error| StoreError(error.to_string()))?
        } else {
            db_options.create_missing_column_families(true);
            let descriptors = [
                rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME,
                ROCKSDB_METADATA_CF,
            ]
            .into_iter()
            .map(|name| {
                rocksdb_engine::ColumnFamilyDescriptor::new(
                    name,
                    rocksdb_engine::Options::default(),
                )
            });
            let db = rocksdb_engine::DBWithThreadMode::<rocksdb_engine::MultiThreaded>::open_cf_descriptors(&db_options, path, descriptors)
                .map_err(|error| StoreError(error.to_string()))?;
            let metadata = db
                .cf_handle(ROCKSDB_METADATA_CF)
                .expect("fresh metadata CF");
            let mut write = rocksdb_engine::WriteOptions::default();
            write.set_sync(true);
            db.put_cf_opt(&metadata, ROCKSDB_FORMAT_KEY, Self::format_bytes(), &write)
                .map_err(|error| StoreError(error.to_string()))?;
            drop(metadata);
            db
        };
        let mut ordinary_write_options = rocksdb_engine::WriteOptions::default();
        ordinary_write_options.set_sync(matches!(options.durability, RocksDbDurability::SyncWal));
        let mut sync_write_options = rocksdb_engine::WriteOptions::default();
        sync_write_options.set_sync(true);
        Ok(Self {
            db: Arc::new(db),
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

    fn write_options(&self, force_sync: bool) -> &rocksdb_engine::WriteOptions {
        if force_sync {
            &self.sync_write_options
        } else {
            &self.ordinary_write_options
        }
    }

    fn metadata_cf(&self) -> Result<Arc<rocksdb_engine::BoundColumnFamily<'_>>, StoreError> {
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

    fn format_bytes() -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[..4].copy_from_slice(&ROCKSDB_MANIFEST_VERSION.to_be_bytes());
        bytes[4..8].copy_from_slice(&ROCKSDB_DATA_FORMAT_VERSION.to_be_bytes());
        bytes[8..].copy_from_slice(&ROCKSDB_PARTITIONER_VERSION.to_be_bytes());
        bytes
    }
    fn validate_metadata(
        db: &rocksdb_engine::DBWithThreadMode<rocksdb_engine::MultiThreaded>,
    ) -> Result<Option<StoreIdentity>, StoreError> {
        let metadata = db.cf_handle(ROCKSDB_METADATA_CF).ok_or_else(|| {
            StoreError("unmanifested RocksDB is unsupported by fresh format3".into())
        })?;
        if db
            .get_cf(&metadata, ROCKSDB_FORMAT_KEY)
            .map_err(|error| StoreError(error.to_string()))?
            .as_deref()
            != Some(Self::format_bytes().as_slice())
        {
            return Err(StoreError(
                "RocksDB lacks exact format3 initialization metadata".into(),
            ));
        }
        for item in db.iterator_cf(&metadata, rocksdb_engine::IteratorMode::Start) {
            let (key, _) = item.map_err(|error| StoreError(error.to_string()))?;
            if key.as_ref() != ROCKSDB_FORMAT_KEY
                && key.as_ref() != ROCKSDB_MANIFEST_KEY
                && key.as_ref() != checkpoint::IMPORT_KEY
                && key.as_ref() != checkpoint::GENERATION_KEY
                && !key.starts_with(checkpoint::PROVENANCE_PREFIX)
            {
                return Err(StoreError(
                    "RocksDB metadata contains an unsupported key".into(),
                ));
            }
        }
        db.get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
            .map_err(|error| StoreError(error.to_string()))?
            .map(|encoded| Self::decode_manifest(&encoded))
            .transpose()
    }
    fn preflight_existing(path: &std::path::Path) -> Result<(), StoreError> {
        let names = rocksdb_engine::DB::list_cf(&rocksdb_engine::Options::default(), path)
            .map_err(|error| StoreError(error.to_string()))?;
        if names.iter().any(|name| {
            name != rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME && name != ROCKSDB_METADATA_CF
        }) {
            return Err(StoreError(
                "RocksDB contains unsupported column family".into(),
            ));
        }
        if !names.iter().any(|name| name == ROCKSDB_METADATA_CF) {
            return Err(StoreError(
                "unmanifested RocksDB is unsupported by fresh format3".into(),
            ));
        }
        // Read-only RocksDB may create an informational log. Redirect it to
        // an owned temporary directory, leaving every existing DB file intact.
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|error| StoreError(error.to_string()))?;
        let suffix = nonce
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let logs =
            ReadOnlyLogs(std::env::temp_dir().join(format!("actordb-format-probe-{suffix}")));
        std::fs::create_dir(&logs.0).map_err(|error| StoreError(error.to_string()))?;
        let mut options = rocksdb_engine::Options::default();
        options.set_db_log_dir(&logs.0);
        options.set_log_level(rocksdb_engine::LogLevel::Fatal);
        let db = rocksdb_engine::DBWithThreadMode::<rocksdb_engine::MultiThreaded>::open_cf_for_read_only(&options, path, names, false).map_err(|error| StoreError(error.to_string()))?;
        let identity = Self::validate_metadata(&db)?;
        for item in db.iterator(rocksdb_engine::IteratorMode::Start) {
            let (key, encoded) = item.map_err(|error| StoreError(error.to_string()))?;
            let identity = identity
                .as_ref()
                .ok_or_else(|| StoreError("unbound format3 database contains records".into()))?;
            let logical = Self::validate_physical_key(&key, identity)?;
            Self::preflight_encoded_record(logical.len(), &encoded)?;
            let (_, start, end) = Self::encoded_record_layout(&encoded).map_err(StoreError)?;
            std::str::from_utf8(&encoded[start..end])
                .map_err(|error| StoreError(format!("record writer is not UTF-8: {error}")))?;
        }
        Ok(())
    }
    fn bind_manifest(&self, identity: &StoreIdentity) -> Result<(), StoreError> {
        check_node_id_budget(identity.node_id.len())
            .map_err(|error| StoreError(error.to_string()))?;
        if identity.fragment_count == 0 {
            return Err(StoreError("requested identity has zero fragments".into()));
        }
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|_| StoreError("manifest lock poisoned".into()))?;
        let metadata = self.metadata_cf()?;
        if self
            .db
            .get_cf(&metadata, checkpoint::IMPORT_KEY)
            .map_err(|error| StoreError(error.to_string()))?
            .is_some()
        {
            return Err(StoreError(
                "checkpoint import is incomplete; resume exact import before startup".into(),
            ));
        }
        if let Some(actual) = Self::validate_metadata(&self.db)? {
            if actual != *identity {
                return Err(StoreError(format!(
                    "RocksDB identity mismatch: stored {:?}, requested {:?}",
                    actual, identity
                )));
            }
            return Ok(());
        }
        if self
            .db
            .iterator(rocksdb_engine::IteratorMode::Start)
            .next()
            .transpose()
            .map_err(|error| StoreError(error.to_string()))?
            .is_some()
        {
            return Err(StoreError(
                "unbound format3 database contains records".into(),
            ));
        }
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
        let physical_key = std::str::from_utf8(&key[4..])
            .map_err(|error| StoreError(format!("RocksDB logical key is not UTF-8: {error}")))?;
        let logical_key = crate::table_key::logical_record_key(physical_key)
            .map_err(|error| StoreError(error.into()))?;
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
        let mut bytes = Vec::with_capacity(37 + v.node_id.len() + v.value.len());
        bytes.extend_from_slice(ROCKSDB_RECORD_MAGIC);
        bytes.extend_from_slice(&v.timestamp_ms.to_be_bytes());
        bytes.extend_from_slice(&v.logical_counter.to_be_bytes());
        bytes.extend_from_slice(&v.expires_at_ms.unwrap_or(0).to_be_bytes());
        bytes.push(u8::from(v.is_tombstone));
        bytes.extend_from_slice(&(v.node_id.len() as u32).to_be_bytes());
        bytes.extend_from_slice(v.node_id.as_bytes());
        bytes.extend_from_slice(&v.value);
        bytes
    }
    fn encoded_record_layout(bytes: &[u8]) -> Result<(bool, usize, usize), String> {
        if bytes.len() < 37 || bytes.get(..8) != Some(ROCKSDB_RECORD_MAGIC.as_slice()) {
            return Err("unsupported record format; expected ACTVAL03".into());
        }
        let tombstone = match bytes[32] {
            0 => false,
            1 => true,
            flag => return Err(format!("invalid tombstone flag {flag}")),
        };
        let length = u32::from_be_bytes(bytes[33..37].try_into().unwrap()) as usize;
        let end = 37usize
            .checked_add(length)
            .ok_or_else(|| "writer length overflow".to_owned())?;
        if end > bytes.len() {
            return Err("writer overruns record".into());
        }
        Ok((tombstone, 37, end))
    }
    fn preflight_encoded_record(logical_key_len: usize, bytes: &[u8]) -> Result<(), StoreError> {
        let (_, start, end) = Self::encoded_record_layout(bytes).map_err(StoreError)?;
        check_record_budget(logical_key_len, bytes.len() - end, end - start)
            .map_err(|error| StoreError(format!("persisted record cannot be replicated: {error}")))
    }
    fn decode(bytes: &[u8]) -> Result<VersionedValue, String> {
        let (is_tombstone, start, end) = Self::encoded_record_layout(bytes)?;
        let expires = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        Ok(VersionedValue {
            timestamp_ms: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            logical_counter: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            expires_at_ms: (expires != 0).then_some(expires),
            is_tombstone,
            node_id: std::str::from_utf8(&bytes[start..end])
                .map_err(|error| error.to_string())?
                .to_owned(),
            value: bytes[end..].to_vec(),
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
        let mut read_options = rocksdb_engine::ReadOptions::default();
        read_options.fill_cache(false);
        let mode = if let Some(prefix) = prefix {
            let end = Self::prefix_end(prefix).ok_or_else(|| {
                StoreError("RocksDB prefix has no finite lexicographic upper bound".to_string())
            })?;
            read_options.set_iterate_lower_bound(prefix.to_vec());
            read_options.set_iterate_upper_bound(end);
            rocksdb_engine::IteratorMode::From(prefix, rocksdb_engine::Direction::Forward)
        } else {
            rocksdb_engine::IteratorMode::Start
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

impl KvStore for RocksDbStore {
    fn checkpoint_backend(&self) -> Option<&dyn crate::checkpoint::CheckpointBackend> {
        Some(self)
    }
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
        let mut batch = rocksdb_engine::WriteBatch::default();
        for (key, value) in writes {
            if value.expires_at_ms == Some(0) {
                return Err(StoreError(
                    "zero expiry is reserved by record format3".into(),
                ));
            }
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
        let mut batch = rocksdb_engine::WriteBatch::default();
        batch.delete_range(prefix, &end);
        let metadata = self.metadata_cf()?;
        let revision = checkpoint::provenance_generation(&self.db)?
            .checked_add(1)
            .ok_or_else(|| StoreError("checkpoint provenance generation exhausted".into()))?;
        batch.put_cf(
            &metadata,
            checkpoint::GENERATION_KEY,
            revision.to_be_bytes(),
        );
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

    use crate::shard::FragmentId;
    use crate::store::PrefixedStore;

    fn largest_admitted_payload(key_len: usize, writer_len: usize) -> usize {
        let mut low = 0;
        let mut high = crate::wire::MAX_FRAME_BYTES;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if check_record_budget(key_len, middle, writer_len).is_ok() {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        low
    }

    #[tokio::test]
    async fn admitted_record_boundaries_roundtrip_native_nodes_and_checkpoints() {
        use crate::checkpoint::{CheckpointImporter, CheckpointOptions, MAX_CHUNK_BYTES};
        use crate::node::{Node, NodeConfig};
        for (table, writer, key_boundary) in [
            ("KV".to_string(), "writer".to_string(), false),
            ("T".repeat(255), "w".repeat(255), false),
            ("T".repeat(255), "writer".to_string(), true),
        ] {
            let mut user_len = 1;
            if key_boundary {
                let namespace_len = crate::table_key::table_prefix(&table).len();
                let mut low = 0;
                let mut high = crate::wire::MAX_FRAME_BYTES / 2;
                while low < high {
                    let middle = low + (high - low).div_ceil(2);
                    if check_record_budget(namespace_len + middle * 2, 0, writer.len()).is_ok() {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                user_len = low;
            }
            let user_key = "k".repeat(user_len);
            let logical = crate::table_key::encode_table_key(&table, &user_key);
            let payload_len = largest_admitted_payload(logical.len(), writer.len());
            if key_boundary {
                assert!(check_record_budget(logical.len() + 2, 0, writer.len()).is_err());
            }
            let payload = vec![7; payload_len];
            assert!(check_record_budget(logical.len(), payload_len, writer.len()).is_ok());
            assert!(check_record_budget(logical.len(), payload_len + 1, writer.len()).is_err());
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("source");
            let store = Arc::new(RocksDbStore::open(&path).unwrap());
            let node = Node::start(
                writer.clone(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder()
                    .store(store.clone())
                    .flush_batch_size(1)
                    .build(),
            )
            .await
            .unwrap();
            node.table(&table)
                .unwrap()
                .put(user_key.clone(), payload.clone())
                .await
                .unwrap();
            assert_eq!(
                node.table(&table)
                    .unwrap()
                    .get(user_key.clone())
                    .await
                    .unwrap()
                    .map(|record| record.value),
                Some(payload.clone())
            );
            node.shutdown().await.unwrap();
            let bundle = directory.path().join("bundle");
            let manifest = node
                .export_checkpoint(&bundle, CheckpointOptions::new(MAX_CHUNK_BYTES, 1).unwrap())
                .await
                .unwrap();
            assert_eq!(manifest.records(), 1);
            drop(node);
            drop(store);
            let reopened = Arc::new(RocksDbStore::open(&path).unwrap());
            let node = Node::start(
                writer.clone(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder().store(reopened).build(),
            )
            .await
            .unwrap();
            assert_eq!(
                node.table(&table)
                    .unwrap()
                    .get(user_key.clone())
                    .await
                    .unwrap()
                    .map(|record| record.value),
                Some(payload.clone())
            );
            node.shutdown().await.unwrap();
            let target = Arc::new(RocksDbStore::open(directory.path().join("target")).unwrap());
            let receipt = CheckpointImporter::open(
                target.clone(),
                StoreIdentity::new("restored".into(), 1),
                &bundle,
            )
            .unwrap()
            .finish()
            .unwrap();
            assert_eq!(receipt.records(), 1);
            let restored = Node::start(
                "restored".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder().store(target.clone()).build(),
            )
            .await
            .unwrap();
            assert_eq!(
                restored
                    .table(&table)
                    .unwrap()
                    .get(user_key)
                    .await
                    .unwrap()
                    .map(|record| record.value),
                Some(payload)
            );
            restored.shutdown().await.unwrap();
            let binding = target
                .bind(&StoreIdentity::new("restored".into(), 1))
                .unwrap();
            let records = target.load().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].1.node_id, writer);
            drop(binding);
        }
    }

    #[test]
    fn mismatched_physical_namespaces_reject_writes_and_reopen_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let identity = StoreIdentity::new("owner".into(), 1);
        let value = VersionedValue::new(vec![1], 1, "writer".into());
        let malformed = [
            format!(
                "{}{}",
                String::from_utf8(crate::table_key::table_storage_prefix("A")).unwrap(),
                crate::table_key::encode_table_key("B", "k")
            ),
            format!(
                "{}{}ff",
                String::from_utf8(crate::table_key::table_storage_prefix("A")).unwrap(),
                crate::table_key::table_prefix("A")
            ),
        ];
        for (index, key) in malformed.into_iter().enumerate() {
            let path = directory.path().join(format!("case-{index}"));
            let store = RocksDbStore::open(&path).unwrap();
            let binding = store.bind(&identity).unwrap();
            let mut physical = 0u32.to_be_bytes().to_vec();
            physical.extend_from_slice(key.as_bytes());
            assert!(store
                .write_batch(&[(physical.clone(), value.clone())])
                .is_err());
            assert!(store.load().unwrap().is_empty());
            assert!(crate::checkpoint::CheckpointRecord {
                key: physical.clone(),
                value: value.clone()
            }
            .validate(&identity)
            .is_err());
            // Inject malformed persisted bytes only into this owned test DB.
            store
                .db
                .put(&physical, RocksDbStore::encode(&value))
                .unwrap();
            drop(binding);
            drop(store);
            let before = directory_snapshot(&path);
            assert!(RocksDbStore::open(&path).is_err());
            assert_eq!(directory_snapshot(&path), before);
        }
    }

    #[test]
    fn old_storage_formats_are_rejected_without_changing_any_database_file() {
        for (manifest_version, record_version) in [(1u32, 1u32), (1, 2), (3, 2)] {
            let dir = tempfile::tempdir().unwrap();
            let identity = StoreIdentity::new("owner".into(), 1);
            {
                let store = RocksDbStore::open(dir.path()).unwrap();
                let binding = store.bind(&identity).unwrap();
                let mut manifest = RocksDbStore::encode_manifest(&identity);
                manifest[8..12].copy_from_slice(&manifest_version.to_be_bytes());
                manifest[12..16].copy_from_slice(&record_version.to_be_bytes());
                store
                    .db
                    .put_cf(
                        &store.metadata_cf().unwrap(),
                        ROCKSDB_MANIFEST_KEY,
                        manifest,
                    )
                    .unwrap();
                drop(binding);
            }
            let before = directory_snapshot(dir.path());
            assert!(RocksDbStore::open(dir.path()).is_err());
            assert_eq!(directory_snapshot(dir.path()), before);
        }
        let dir = tempfile::tempdir().unwrap();
        {
            let old = rocksdb_engine::DB::open_default(dir.path()).unwrap();
            old.put(b"old", b"record").unwrap();
        }
        let before = directory_snapshot(dir.path());
        assert!(RocksDbStore::open(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn old_records_inside_new_manifest_are_rejected_before_writable_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = RocksDbStore::open(dir.path()).unwrap();
            let binding = store.bind(&StoreIdentity::new("owner".into(), 1)).unwrap();
            let mut old = Vec::new();
            old.extend(42u64.to_be_bytes());
            old.extend(0u64.to_be_bytes());
            old.push(128);
            old.extend(5u32.to_be_bytes());
            old.extend(7u64.to_be_bytes());
            old.extend(b"owner");
            old.extend(b"payload");
            store
                .db
                .put([0u32.to_be_bytes().as_slice(), b"key"].concat(), old)
                .unwrap();
            drop(binding);
        }
        let before = directory_snapshot(dir.path());
        assert!(RocksDbStore::open(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn fresh_unbound_initialization_reopens_and_record3_preserves_every_stamp_field() {
        let dir = tempfile::tempdir().unwrap();
        drop(RocksDbStore::open(dir.path()).unwrap());
        let store = RocksDbStore::open(dir.path()).unwrap();
        let binding = store.bind(&StoreIdentity::new("owner".into(), 1)).unwrap();
        let mut value = VersionedValue::new(b"opaque\xff".to_vec(), 42, "historical".into());
        value.logical_counter = 99;
        value.is_tombstone = true;
        value.expires_at_ms = Some(100);
        let key = [0u32.to_be_bytes().as_slice(), b"key"].concat();
        store.write_batch(&[(key.clone(), value.clone())]).unwrap();
        drop(binding);
        drop(store);
        let reopened = RocksDbStore::open(dir.path()).unwrap();
        let _binding = reopened
            .bind(&StoreIdentity::new("owner".into(), 1))
            .unwrap();
        assert_eq!(reopened.load().unwrap(), vec![(key, value)]);
    }

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
    fn rocksdb_value_encoding_roundtrip() {
        let v = VersionedValue::new(vec![1, 2, 3], 123456789, "node-a".to_string());
        assert_eq!(RocksDbStore::decode(&RocksDbStore::encode(&v)).unwrap(), v);
        let v = VersionedValue::new(vec![], 0, "n".to_string());
        assert_eq!(RocksDbStore::decode(&RocksDbStore::encode(&v)).unwrap(), v);
    }

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
        invalid_tombstone[32] = 2;
        assert!(RocksDbStore::decode(&invalid_tombstone)
            .unwrap_err()
            .contains("invalid tombstone flag 2"));
    }

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

    #[test]
    fn rocksdb_rejects_invalid_tombstone_on_load() {
        let mut encoded = RocksDbStore::encode(&VersionedValue::new(
            b"value".to_vec(),
            1,
            "writer".to_string(),
        ));
        encoded[32] = 2;
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
    }

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
        corrupt_key.extend_from_slice(b"invalid");
        store
            .db
            .put(
                corrupt_key,
                RocksDbStore::encode(&VersionedValue::new(Vec::new(), 1, String::new())),
            )
            .unwrap();
        let error = store.load().expect_err("invalid provenance was loaded");
        assert!(error.to_string().contains("record node id is 0 bytes"));
    }

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
            let db = rocksdb_engine::DB::open_default(damaged_dir.path()).unwrap();
            db.put(b"key", b"value").unwrap();
        }
        std::fs::rename(
            damaged_dir.path().join("CURRENT"),
            damaged_dir.path().join("CURRENT.saved"),
        )
        .unwrap();
        let before = directory_snapshot(damaged_dir.path());
        let error = RocksDbStore::open_with_options(damaged_dir.path(), RocksDbOptions::default())
            .err()
            .expect("a damaged RocksDB without CURRENT was reinitialized");
        assert!(error.to_string().contains("without CURRENT"));
        assert_eq!(directory_snapshot(damaged_dir.path()), before);
        assert!(!damaged_dir.path().join("CURRENT").exists());
    }

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

    #[test]
    fn rocksdb_rejects_unknown_column_families_and_metadata_keys() {
        let unknown_cf_dir = tempfile::tempdir().unwrap();
        {
            let mut db = rocksdb_engine::DB::open_default(unknown_cf_dir.path()).unwrap();
            db.create_cf("future-data", &rocksdb_engine::Options::default())
                .unwrap();
        }
        let error = RocksDbStore::open(unknown_cf_dir.path())
            .err()
            .expect("unknown column family was silently accepted");
        assert!(error.to_string().contains("unsupported column family"));

        let unknown_metadata_dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(unknown_metadata_dir.path()).unwrap();
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
