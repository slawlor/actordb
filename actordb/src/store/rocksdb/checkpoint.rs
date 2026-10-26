//! Native immutable snapshots and atomic stable import cursors.

use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::Ordering;

use super::*;
use crate::checkpoint::VerifiedCheckpointChunk;
use crate::checkpoint::{
    CheckpointBackend, CheckpointError, CheckpointImport, CheckpointManifest, CheckpointReader,
    CheckpointRecord, ImportProgress, ImportReceipt,
};

pub(super) const IMPORT_KEY: &[u8] = b"checkpoint-import";
pub(super) const PROVENANCE_PREFIX: &[u8] = b"checkpoint-provenance/";
pub(super) const GENERATION_KEY: &[u8] = b"checkpoint-generation";
type Engine = rocksdb_engine::DBWithThreadMode<rocksdb_engine::MultiThreaded>;

pub(super) fn provenance_generation(db: &Engine) -> Result<u64, StoreError> {
    let Some(metadata) = db.cf_handle(ROCKSDB_METADATA_CF) else {
        return Ok(0);
    };
    let Some(encoded) = db
        .get_cf(&metadata, GENERATION_KEY)
        .map_err(|error| StoreError(error.to_string()))?
    else {
        return Ok(0);
    };
    Ok(u64::from_be_bytes(encoded.as_slice().try_into().map_err(
        |_| StoreError("invalid checkpoint provenance generation".into()),
    )?))
}

fn provenance_key(digest: &[u8; 32]) -> Vec<u8> {
    let mut key = PROVENANCE_PREFIX.to_vec();
    key.extend(digest);
    key
}

struct SnapshotReader {
    db: Engine,
    identity: StoreIdentity,
}
impl CheckpointReader for SnapshotReader {
    fn page(
        &mut self,
        after: Option<&[u8]>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<CheckpointRecord>, CheckpointError> {
        let mode = after.map_or(rocksdb_engine::IteratorMode::Start, |after| {
            rocksdb_engine::IteratorMode::From(after, rocksdb_engine::Direction::Forward)
        });
        let mut options = rocksdb_engine::ReadOptions::default();
        options.fill_cache(false);
        let mut out = Vec::new();
        let mut bytes = 0;
        for item in self.db.iterator_opt(mode, options) {
            let (key, encoded) = item.map_err(|error| StoreError(error.to_string()))?;
            if after.is_some_and(|after| key.as_ref() <= after) {
                continue;
            }
            let logical = RocksDbStore::validate_physical_key(&key, &self.identity)?;
            RocksDbStore::preflight_encoded_record(logical.len(), &encoded)?;
            let record = CheckpointRecord {
                key: key.to_vec(),
                value: RocksDbStore::decode(&encoded).map_err(StoreError)?,
            };
            record.validate(&self.identity)?;
            let charge = record.encoded_size();
            if charge > max_bytes {
                if out.is_empty() {
                    return Err(CheckpointError::BudgetExceeded(
                        "individual record exceeds selected chunk size".into(),
                    ));
                }
                break;
            }
            if bytes + charge > max_bytes || out.len() == max_records {
                break;
            }
            bytes += charge;
            out.push(record);
        }
        Ok(out)
    }
}

struct ImportSession {
    db: Arc<Engine>,
    attached: Arc<std::sync::atomic::AtomicBool>,
    identity_state: Arc<std::sync::Mutex<Option<StoreIdentity>>>,
    target: StoreIdentity,
    manifest: CheckpointManifest,
    current: ImportProgress,
}
impl Drop for ImportSession {
    fn drop(&mut self) {
        if let Ok(mut identity) = self.identity_state.lock() {
            *identity = None;
        }
        self.attached.store(false, Ordering::Release);
    }
}
impl ImportSession {
    fn metadata(&self) -> Result<Arc<rocksdb_engine::BoundColumnFamily<'_>>, CheckpointError> {
        self.db
            .cf_handle(ROCKSDB_METADATA_CF)
            .ok_or_else(|| CheckpointError::Invalid("missing import metadata".into()))
    }
    fn sync_write(&self, batch: rocksdb_engine::WriteBatch) -> Result<(), CheckpointError> {
        let mut options = rocksdb_engine::WriteOptions::default();
        options.set_sync(true);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| StoreError(error.to_string()))?;
        Ok(())
    }
}
impl CheckpointImport for ImportSession {
    fn progress(&self) -> Result<ImportProgress, CheckpointError> {
        Ok(self.current.clone())
    }
    fn apply(&mut self, chunk: &VerifiedCheckpointChunk) -> Result<(), CheckpointError> {
        let previous = chunk.previous();
        let next = chunk.next();
        let records = chunk.records();
        if previous != &self.current
            || next.manifest_digest != self.manifest.digest()
            || next.next_chunk != previous.next_chunk + 1
            || previous.complete
        {
            return Err(CheckpointError::Invalid(
                "import transaction cursor changed".into(),
            ));
        }
        if previous.checked_next(&self.manifest, records)? != *next {
            return Err(CheckpointError::Invalid(
                "backend transaction does not match canonical chunk/hash commitment".into(),
            ));
        }
        let metadata = self.metadata()?;
        let persisted = self
            .db
            .get_cf(&metadata, IMPORT_KEY)
            .map_err(|error| StoreError(error.to_string()))?
            .ok_or_else(|| CheckpointError::Invalid("missing incomplete import marker".into()))?;
        if ImportProgress::decode(&persisted)? != *previous {
            return Err(CheckpointError::Invalid(
                "durable import cursor changed".into(),
            ));
        }
        let mut batch = rocksdb_engine::WriteBatch::default();
        for record in records {
            record.validate(&self.target)?;
            if let Some(existing) = self
                .db
                .get(&record.key)
                .map_err(|error| StoreError(error.to_string()))?
            {
                let existing = RocksDbStore::decode(&existing).map_err(StoreError)?;
                if record.value.same_version(&existing) && record.value != existing {
                    return Err(CheckpointError::Invalid(
                        "equal LWW version has divergent checkpoint contents".into(),
                    ));
                }
                if (
                    record.value.version(),
                    &record.value.node_id,
                    record.value.is_tombstone,
                ) <= (existing.version(), &existing.node_id, existing.is_tombstone)
                {
                    continue;
                }
            }
            batch.put(&record.key, RocksDbStore::encode(&record.value));
        }
        batch.put_cf(&metadata, IMPORT_KEY, next.encode());
        self.sync_write(batch)?;
        drop(metadata);
        self.current = next.clone();
        Ok(())
    }
    fn finish(
        &mut self,
        receipt: &ImportReceipt,
    ) -> Result<StoreCommitDurability, CheckpointError> {
        if receipt.target() != &self.target
            || receipt.source() != self.manifest.source()
            || receipt.manifest_digest() != &self.manifest.digest()
            || receipt.root_hash() != self.manifest.root_hash()
            || receipt.records() != self.manifest.records()
            || self.current.next_chunk != self.manifest.chunks()
            || self.current.records != self.manifest.records()
            || self.current.previous_hash != *self.manifest.root_hash()
        {
            return Err(CheckpointError::Invalid(
                "cannot finalize different/incomplete import".into(),
            ));
        }
        let metadata = self.metadata()?;
        let mut batch = rocksdb_engine::WriteBatch::default();
        let mut encoded = provenance_generation(&self.db)?.to_be_bytes().to_vec();
        encoded.extend(receipt.encode());
        batch.put_cf(
            &metadata,
            provenance_key(receipt.manifest_digest()),
            encoded,
        );
        batch.delete_cf(&metadata, IMPORT_KEY);
        self.sync_write(batch)?;
        drop(metadata);
        self.current.complete = true;
        Ok(StoreCommitDurability::Stable)
    }
}

impl CheckpointBackend for RocksDbStore {
    fn create_snapshot(&self, directory: &Path) -> Result<(), CheckpointError> {
        self.attached_identity()?;
        if directory.exists() {
            return Err(CheckpointError::Invalid(
                "snapshot destination already exists".into(),
            ));
        }
        let mut random = [0; 16];
        getrandom::fill(&mut random)
            .map_err(|error| CheckpointError::Invalid(error.to_string()))?;
        let name = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let parent = directory
            .parent()
            .ok_or_else(|| CheckpointError::Invalid("snapshot path lacks parent".into()))?;
        let staging = parent.join(format!("snapshot-staging-{name}"));
        rocksdb_engine::checkpoint::Checkpoint::new(&*self.db)
            .map_err(|error| StoreError(error.to_string()))?
            .create_checkpoint(&staging)
            .map_err(|error| StoreError(error.to_string()))?;
        for entry in fs::read_dir(&staging)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                File::open(entry.path())?.sync_all()?;
            }
        }
        File::open(&staging)?.sync_all()?;
        fs::rename(&staging, directory)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
    fn open_snapshot(
        &self,
        directory: &Path,
        identity: &StoreIdentity,
    ) -> Result<Box<dyn CheckpointReader>, CheckpointError> {
        RocksDbStore::preflight_existing(directory)?;
        let cf_names = rocksdb_engine::DB::list_cf(&rocksdb_engine::Options::default(), directory)
            .map_err(|error| StoreError(error.to_string()))?;
        let descriptors = cf_names.into_iter().map(|name| {
            rocksdb_engine::ColumnFamilyDescriptor::new(name, rocksdb_engine::Options::default())
        });
        // The engine's OS-backed LOCK also serializes exporters for one
        // bundle across processes; recovery releases it after a crash. No
        // record-writing API is exposed through the owned reader.
        let db = Engine::open_cf_descriptors(
            &rocksdb_engine::Options::default(),
            directory,
            descriptors,
        )
        .map_err(|error| StoreError(error.to_string()))?;
        let metadata = db
            .cf_handle(ROCKSDB_METADATA_CF)
            .ok_or_else(|| CheckpointError::Invalid("frozen snapshot lacks manifest".into()))?;
        let encoded = db
            .get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
            .map_err(|error| StoreError(error.to_string()))?
            .ok_or_else(|| CheckpointError::Invalid("frozen snapshot lacks identity".into()))?;
        if RocksDbStore::decode_manifest(&encoded)? != *identity {
            return Err(CheckpointError::Invalid(
                "snapshot belongs to different identity".into(),
            ));
        }
        drop(metadata);
        Ok(Box::new(SnapshotReader {
            db,
            identity: identity.clone(),
        }))
    }
    fn begin_import(
        &self,
        target: &StoreIdentity,
        manifest: &CheckpointManifest,
    ) -> Result<Box<dyn CheckpointImport>, CheckpointError> {
        crate::checkpoint::validate_identity(target)?;
        if target.fragment_count() != manifest.source().fragment_count() {
            return Err(CheckpointError::Invalid(
                "import changes fragment count".into(),
            ));
        }
        self.attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                CheckpointError::Invalid("target has a live binding/import owner".into())
            })?;
        let mut session = ImportSession {
            db: self.db.clone(),
            attached: self.attached.clone(),
            identity_state: self.attached_identity.clone(),
            target: target.clone(),
            manifest: manifest.clone(),
            current: ImportProgress::initial(manifest),
        };
        let existing = self
            .db
            .cf_handle(ROCKSDB_METADATA_CF)
            .map(|metadata| self.db.get_cf(&metadata, IMPORT_KEY))
            .transpose()
            .map_err(|error| StoreError(error.to_string()))?
            .flatten();
        if let Some(progress) = existing {
            let progress = ImportProgress::decode(&progress)?;
            if progress.manifest_digest != manifest.digest() {
                return Err(CheckpointError::Invalid(
                    "a different import is incomplete".into(),
                ));
            }
            let metadata = self.metadata_cf()?;
            let encoded = self
                .db
                .get_cf(&metadata, ROCKSDB_MANIFEST_KEY)
                .map_err(|error| StoreError(error.to_string()))?
                .ok_or_else(|| {
                    CheckpointError::Invalid("incomplete import lacks target identity".into())
                })?;
            if Self::decode_manifest(&encoded)? != *target {
                return Err(CheckpointError::Invalid(
                    "resume changes target identity".into(),
                ));
            }
            session.current = progress;
        } else {
            self.bind_manifest(target)?;
            if let Some(receipt) = self.import_evidence(&manifest.digest())? {
                if receipt.target() != target
                    || receipt.source() != manifest.source()
                    || receipt.root_hash() != manifest.root_hash()
                {
                    return Err(CheckpointError::Invalid(
                        "completed import evidence differs".into(),
                    ));
                }
                session.current.next_chunk = manifest.chunks();
                session.current.records = manifest.records();
                session.current.previous_hash = *manifest.root_hash();
                session.current.complete = true;
            } else {
                let metadata = self.metadata_cf()?;
                let mut batch = rocksdb_engine::WriteBatch::default();
                batch.put_cf(&metadata, IMPORT_KEY, session.current.encode());
                session.sync_write(batch)?;
            }
        }
        // Import owns the engine through its private session, not ordinary
        // data-operation admission. Keeping attached_identity absent rejects
        // public loads/writes/deletes/fences through another shared Arc while
        // partial chunks are present; the durable marker also rejects bind.
        Ok(Box::new(session))
    }
    fn import_evidence(&self, digest: &[u8; 32]) -> Result<Option<ImportReceipt>, CheckpointError> {
        let Some(metadata) = self.db.cf_handle(ROCKSDB_METADATA_CF) else {
            return Ok(None);
        };
        let encoded = self
            .db
            .get_cf(&metadata, provenance_key(digest))
            .map_err(|error| StoreError(error.to_string()))?;
        let Some(encoded) = encoded else {
            return Ok(None);
        };
        let generation = u64::from_be_bytes(
            encoded
                .get(..8)
                .ok_or_else(|| CheckpointError::Invalid("truncated provenance generation".into()))?
                .try_into()
                .unwrap(),
        );
        if generation != provenance_generation(&self.db)? {
            return Ok(None);
        }
        Ok(Some(ImportReceipt::decode(&encoded[8..])?))
    }
}
