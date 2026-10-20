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
/// Values are encoded as `[timestamp_ms: u64 BE][expires_at_ms: u64 BE, 0 = none][is_tombstone: u8][node_id_len: u32 BE][node_id][value]`.
/// Fragment data lives in the default column family; an independent metadata
/// column family holds the node/layout manifest. Data operations require a
/// live [`StoreBinding`], and physical keys must begin with their four-byte
/// fragment id followed by the UTF-8 logical key.
pub struct RocksDbStore {
    db: rocksdb_engine::DBWithThreadMode<rocksdb_engine::MultiThreaded>,
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
const ROCKSDB_MANIFEST_VERSION: u32 = 1;
const ROCKSDB_DATA_FORMAT_VERSION: u32 = 2;
const ROCKSDB_PARTITIONER_VERSION: u32 = 1;

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
        let mut db_options = rocksdb_engine::Options::default();
        db_options.create_if_missing(true);
        let db = if has_current {
            let column_families =
                rocksdb_engine::DB::list_cf(&rocksdb_engine::Options::default(), path)
                    .map_err(|error| StoreError(error.to_string()))?;
            if !column_families
                .iter()
                .any(|name| name == rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME)
            {
                return Err(StoreError(
                    "RocksDB is missing its default column family".to_string(),
                ));
            }
            if let Some(unknown) = column_families.iter().find(|name| {
                name.as_str() != rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME
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
                // Probe an old default-CF-only database while holding its lock.
                // A strict open of non-empty legacy data must not create a
                // metadata CF and thereby prevent rollback to the old binary.
                let db = rocksdb_engine::DBWithThreadMode::<rocksdb_engine::MultiThreaded>::open(
                    &db_options,
                    path,
                )
                .map_err(|error| StoreError(error.to_string()))?;
                let has_legacy_data = db
                    .iterator(rocksdb_engine::IteratorMode::Start)
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
            rocksdb_engine::DBWithThreadMode::<rocksdb_engine::MultiThreaded>::open(
                &db_options,
                path,
            )
            .map_err(|error| StoreError(error.to_string()))?
        };
        let mut ordinary_write_options = rocksdb_engine::WriteOptions::default();
        ordinary_write_options.set_sync(matches!(options.durability, RocksDbDurability::SyncWal));
        let mut sync_write_options = rocksdb_engine::WriteOptions::default();
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
        if ![1, ROCKSDB_DATA_FORMAT_VERSION].contains(&data_version) {
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
        for item in self.db.iterator(rocksdb_engine::IteratorMode::Start) {
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
            for item in self
                .db
                .iterator_cf(&metadata, rocksdb_engine::IteratorMode::Start)
            {
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
                if u32::from_be_bytes(encoded[12..16].try_into().unwrap()) == 1 {
                    self.validate_legacy_records(identity)?;
                    self.db
                        .put_cf_opt(
                            &metadata,
                            ROCKSDB_MANIFEST_KEY,
                            Self::encode_manifest(identity),
                            &self.sync_write_options,
                        )
                        .map_err(|error| StoreError(error.to_string()))?;
                }
                return Ok(());
            }
        }

        let has_legacy_data = self
            .db
            .iterator(rocksdb_engine::IteratorMode::Start)
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
                .create_cf(ROCKSDB_METADATA_CF, &rocksdb_engine::Options::default())
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
        let mut buf = Vec::with_capacity(29 + v.node_id.len() + v.value.len());
        buf.extend_from_slice(&v.timestamp_ms.to_be_bytes());
        // 0 = no expiry.
        buf.extend_from_slice(&v.expires_at_ms.unwrap_or(0).to_be_bytes());
        buf.push(0x80 | v.is_tombstone as u8);
        buf.extend_from_slice(&(v.node_id.len() as u32).to_be_bytes());
        buf.extend_from_slice(&v.logical_counter.to_be_bytes());
        buf.extend_from_slice(v.node_id.as_bytes());
        buf.extend_from_slice(&v.value);
        buf
    }

    fn encoded_record_layout(bytes: &[u8]) -> Result<(bool, usize, usize), String> {
        if bytes.len() < 21 {
            return Err(format!("value too short: {} bytes", bytes.len()));
        }
        let is_tombstone = match bytes[16] {
            0 | 128 => false,
            1 | 129 => true,
            flag => return Err(format!("invalid tombstone flag {flag}; expected 0 or 1")),
        };
        let node_id_len = u32::from_be_bytes(bytes[17..21].try_into().unwrap()) as usize;
        let node_offset = if bytes[16] & 0x80 != 0 {
            29usize
        } else {
            21usize
        };
        let value_offset = node_offset
            .checked_add(node_id_len)
            .ok_or_else(|| "node_id length overflowed usize".to_string())?;
        if bytes.len() < value_offset {
            return Err(format!(
                "node_id overruns value: id_len={node_id_len} total={}",
                bytes.len()
            ));
        }
        Ok((is_tombstone, node_offset, value_offset))
    }

    fn preflight_encoded_record(logical_key_len: usize, bytes: &[u8]) -> Result<(), StoreError> {
        let (_, node_offset, value_offset) =
            Self::encoded_record_layout(bytes).map_err(StoreError)?;
        let node_id_len = value_offset - node_offset;
        check_record_budget(logical_key_len, bytes.len() - value_offset, node_id_len).map_err(
            |error| {
                StoreError(format!(
                    "persisted record (key bytes {logical_key_len}) cannot be replicated: {error}"
                ))
            },
        )
    }

    fn decode(bytes: &[u8]) -> Result<VersionedValue, String> {
        let (is_tombstone, node_offset, value_offset) = Self::encoded_record_layout(bytes)?;
        let logical_counter = if node_offset == 29 {
            u64::from_be_bytes(bytes[21..29].try_into().unwrap())
        } else {
            0
        };
        let timestamp_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let expires_raw = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let expires_at_ms = if expires_raw == 0 {
            None
        } else {
            Some(expires_raw)
        };
        let node_id = std::str::from_utf8(&bytes[node_offset..value_offset])
            .map_err(|error| format!("node_id not UTF-8: {error}"))?
            .to_owned();
        let value = bytes[value_offset..].to_vec();
        Ok(VersionedValue {
            value,
            timestamp_ms,
            logical_counter,
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
    fn format_one_records_upgrade_and_preserve_logical_restart_versions() {
        let dir = tempfile::tempdir().unwrap();
        let identity = StoreIdentity::new("owner".into(), 1);
        let key = [0u32.to_be_bytes().as_slice(), b"legacy"].concat();
        let old = VersionedValue::new(b"old".to_vec(), 42, "owner".into());
        {
            let store = RocksDbStore::open(dir.path()).unwrap();
            let binding = store.bind(&identity).unwrap();
            let mut legacy = RocksDbStore::encode(&old);
            legacy.drain(21..29);
            legacy[16] &= 0x7f;
            store.db.put(&key, legacy).unwrap();
            let mut manifest = RocksDbStore::encode_manifest(&identity);
            manifest[12..16].copy_from_slice(&1u32.to_be_bytes());
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
        let store = RocksDbStore::open(dir.path()).unwrap();
        let binding = store.bind(&identity).unwrap();
        assert_eq!(store.load().unwrap(), vec![(key.clone(), old)]);
        let manifest = store
            .db
            .get_cf(&store.metadata_cf().unwrap(), ROCKSDB_MANIFEST_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(u32::from_be_bytes(manifest[12..16].try_into().unwrap()), 2);
        let new = VersionedValue::new_version(
            b"new".to_vec(),
            actordb_protocol::RecordVersion::new(42, 1_000_000),
            "owner".into(),
        );
        store.write_batch(&[(key.clone(), new.clone())]).unwrap();
        drop(binding);
        drop(store);
        let reopened = RocksDbStore::open(dir.path()).unwrap();
        let _binding = reopened.bind(&identity).unwrap();
        assert_eq!(reopened.load().unwrap(), vec![(key, new)]);
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
        invalid_tombstone[16] = 2;
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
            let legacy = rocksdb_engine::DB::open_default(legacy_dir.path()).unwrap();
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
            let legacy = rocksdb_engine::DB::open_default(dir.path()).unwrap();
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
            let legacy = rocksdb_engine::DB::open_default(dir.path()).unwrap();
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
            rocksdb_engine::DB::list_cf(&rocksdb_engine::Options::default(), dir.path()).unwrap(),
            vec![rocksdb_engine::DEFAULT_COLUMN_FAMILY_NAME.to_string()]
        );
        drop(rocksdb_engine::DB::open_default(dir.path()).unwrap());

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
            let legacy = rocksdb_engine::DB::open_default(dir.path()).unwrap();
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
        store
            .db
            .create_cf(ROCKSDB_METADATA_CF, &rocksdb_engine::Options::default())
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
