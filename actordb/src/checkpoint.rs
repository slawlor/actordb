//! Stable, bounded checkpoint bundles with crash-resumable export/import.
//!
//! Bundles preserve physical keys and complete LWW provenance. They certify a
//! local stable store copy, not authority to discard unavailable replicas.
//! All filesystem/backend methods are synchronous; use a blocking executor
//! when calling them from an async application. No `load()` fallback is used.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::actor::VersionedValue;
use crate::shard::fragment_of;
use crate::store::{KvStore, StoreCommitDurability, StoreError, StoreIdentity};

pub(crate) mod format;
use format::{hash, Decoder, Encoder};

/// Hard limit for one encoded checkpoint chunk, independent of record count.
pub const MAX_CHUNK_BYTES: usize = 32 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 2 * 1024 * 1024;
const CHUNK_HEADER_BYTES: usize = 8 + 8 + 32 + 4;
const FORMAT_MAGIC: &[u8; 8] = b"ACTCP003";

#[derive(Debug)]
#[non_exhaustive]
pub enum CheckpointError {
    Unsupported,
    /// The stopped Node no longer owns its physical backend. Retain the
    /// caller's store Arc through post-shutdown export, or open it offline.
    BackendReleased,
    Invalid(String),
    Store(StoreError),
    Io(std::io::Error),
    /// A record cannot fit the chosen byte limit. Retry with a larger limit
    /// when creating a new export; a begun export keeps its original limits.
    BudgetExceeded(String),
}
impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => f.write_str("store does not support durable bounded checkpoints"),
            Self::BackendReleased => f.write_str("checkpoint backend was released by shutdown; retain the store Arc for export or reopen it offline"),
            Self::Invalid(message) => write!(f, "invalid checkpoint: {message}"),
            Self::Store(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::BudgetExceeded(message) => write!(f, "checkpoint budget exceeded: {message}"),
        }
    }
}
impl std::error::Error for CheckpointError {}
impl From<std::io::Error> for CheckpointError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<StoreError> for CheckpointError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// One raw durable record, including the four-byte fragment and physical
/// named-table namespace. Values are never decoded as application text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointRecord {
    pub key: Vec<u8>,
    pub value: VersionedValue,
}
impl CheckpointRecord {
    /// Exact size in the canonical transfer codec.
    pub fn encoded_size(&self) -> usize {
        4 + self.key.len()
            + 16
            + 4
            + self.value.node_id.len()
            + 1
            + usize::from(self.value.expires_at_ms.is_some()) * 8
            + 1
            + 4
            + self.value.value.len()
    }
    pub fn validate(&self, identity: &StoreIdentity) -> Result<(), CheckpointError> {
        let fragment = self
            .key
            .get(..4)
            .ok_or_else(|| CheckpointError::Invalid("physical key lacks fragment prefix".into()))?;
        let fragment = u32::from_be_bytes(fragment.try_into().unwrap());
        let physical = std::str::from_utf8(&self.key[4..])
            .map_err(|_| CheckpointError::Invalid("physical key is not UTF-8".into()))?;
        if fragment >= identity.fragment_count()
            || fragment_of(physical, identity.fragment_count() as usize).0 != fragment
        {
            return Err(CheckpointError::Invalid(
                "physical key has a different fragment layout".into(),
            ));
        }
        crate::wire::check_record_budget(
            physical.len(),
            self.value.value.len(),
            self.value.node_id.len(),
        )
        .map_err(|error| CheckpointError::Invalid(error.to_string()))?;
        if self.value.expires_at_ms == Some(0) {
            return Err(CheckpointError::Invalid(
                "zero expiry is reserved by the durable/wire codecs".into(),
            ));
        }
        if physical.starts_with("\u{001d}actordb.table.store.v1:") {
            let logical =
                crate::table_key::strip_storage_prefix(&self.key[4..]).ok_or_else(|| {
                    CheckpointError::Invalid("invalid physical table namespace".into())
                })?;
            let logical = std::str::from_utf8(&logical)
                .map_err(|_| CheckpointError::Invalid("table key is not UTF-8".into()))?;
            let table = crate::table_key::table_name(logical).ok_or_else(|| {
                CheckpointError::Invalid("physical table record lacks logical namespace".into())
            })?;
            if !self.key[4..].starts_with(&crate::table_key::table_storage_prefix(&table))
                || crate::table_key::decode_table_key(&table, logical).is_none()
            {
                return Err(CheckpointError::Invalid(
                    "physical/logical table identities disagree".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Backend-created immutable durable snapshot. Each page must be in strictly
/// increasing physical-key order, exclusive of `after`, and fit both limits.
pub trait CheckpointReader: Send {
    fn page(
        &mut self,
        after: Option<&[u8]>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<CheckpointRecord>, CheckpointError>;
}

/// Optional backend contract. Implementations must persist the immutable
/// snapshot before returning, never emulate paging with a full `load`, and
/// atomically commit import records together with exact progress. An incomplete
/// import must block ordinary `KvStore::bind` across process restart.
pub trait CheckpointBackend: Send + Sync {
    /// Called with an exclusive source store binding after a stable fence.
    fn create_snapshot(&self, directory: &Path) -> Result<(), CheckpointError>;
    fn open_snapshot(
        &self,
        directory: &Path,
        identity: &StoreIdentity,
    ) -> Result<Box<dyn CheckpointReader>, CheckpointError>;
    /// Acquire an exclusive target lease. An exact existing import may resume;
    /// a different concurrent/incomplete import must be rejected.
    fn begin_import(
        &self,
        target: &StoreIdentity,
        manifest: &CheckpointManifest,
    ) -> Result<Box<dyn CheckpointImport>, CheckpointError>;
    /// Currently retained stable import evidence; prefix retirement or other
    /// destructive changes must invalidate evidence whose floor was removed.
    fn import_evidence(
        &self,
        manifest_digest: &[u8; 32],
    ) -> Result<Option<ImportReceipt>, CheckpointError>;
}

/// Backend import ownership. Dropping releases only its exclusive in-process
/// lease; the durable incomplete marker remains and blocks serving.
pub trait CheckpointImport: Send {
    fn progress(&self) -> Result<ImportProgress, CheckpointError>;
    /// Merge LWW winners, reject equal-version divergent contents, and persist
    /// `next` atomically with those writes through stable storage. The exact
    /// previous progress must match; duplicate retries are idempotent.
    fn apply(&mut self, chunk: &VerifiedCheckpointChunk) -> Result<(), CheckpointError>;
    /// Publish completion only after all chunks match the manifest and all
    /// target winners plus provenance metadata are stable.
    fn finish(&mut self, receipt: &ImportReceipt)
        -> Result<StoreCommitDurability, CheckpointError>;
}

/// Opaque pre-apply authentication evidence. Only the importer can construct
/// this after matching data against the manifest-committed immutable digest
/// index. Public backend callers cannot manufacture it from arbitrary rows.
#[derive(Debug, Clone)]
pub struct VerifiedCheckpointChunk {
    previous: ImportProgress,
    next: ImportProgress,
    records: Vec<CheckpointRecord>,
}
impl VerifiedCheckpointChunk {
    pub fn previous(&self) -> &ImportProgress {
        &self.previous
    }
    pub fn next(&self) -> &ImportProgress {
        &self.next
    }
    pub fn records(&self) -> &[CheckpointRecord] {
        &self.records
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportProgress {
    pub manifest_digest: [u8; 32],
    pub next_chunk: u64,
    pub records: u64,
    pub previous_hash: [u8; 32],
    pub last_key: Option<Vec<u8>>,
    pub complete: bool,
}
impl ImportProgress {
    pub fn initial(manifest: &CheckpointManifest) -> Self {
        Self {
            manifest_digest: manifest.digest(),
            next_chunk: 0,
            records: 0,
            previous_hash: manifest.initial_hash(),
            last_key: None,
            complete: false,
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Encoder::default();
        out.digest(&self.manifest_digest);
        out.u64(self.next_chunk);
        out.u64(self.records);
        out.digest(&self.previous_hash);
        out.u8(u8::from(self.complete));
        match &self.last_key {
            Some(key) => {
                out.u8(1);
                out.bytes(key);
            }
            None => out.u8(0),
        }
        out.0
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        let mut input = Decoder::new(bytes);
        let manifest_digest = input.digest()?;
        let next_chunk = input.u64()?;
        let records = input.u64()?;
        let previous_hash = input.digest()?;
        let complete = match input.u8()? {
            0 => false,
            1 => true,
            _ => {
                return Err(CheckpointError::Invalid(
                    "invalid progress completion flag".into(),
                ))
            }
        };
        let last_key = match input.u8()? {
            0 => None,
            1 => Some(input.bytes(MAX_CHUNK_BYTES)?),
            _ => {
                return Err(CheckpointError::Invalid(
                    "invalid progress cursor flag".into(),
                ))
            }
        };
        input.finish()?;
        Ok(Self {
            manifest_digest,
            next_chunk,
            records,
            previous_hash,
            last_key,
            complete,
        })
    }
    /// Reconstruct and validate one canonical chunk's exact next cursor.
    /// Backends use this before committing so direct capability callers cannot
    /// claim an unrelated root for arbitrary imported records.
    pub fn checked_next(
        &self,
        manifest: &CheckpointManifest,
        records: &[CheckpointRecord],
    ) -> Result<Self, CheckpointError> {
        if self.complete
            || self.manifest_digest != manifest.digest()
            || self.next_chunk >= manifest.chunks
            || records.is_empty()
            || records.len() > manifest.options.max_chunk_records
        {
            return Err(CheckpointError::Invalid(
                "chunk does not follow the manifest/cursor".into(),
            ));
        }
        let mut encoded = Encoder::default();
        encoded.0.extend(FORMAT_MAGIC);
        encoded.u64(self.next_chunk);
        encoded.digest(&self.previous_hash);
        encoded.u32(records.len() as u32);
        let mut last = self.last_key.as_deref();
        for record in records {
            record.validate(manifest.source())?;
            if last.is_some_and(|last| record.key.as_slice() <= last) {
                return Err(CheckpointError::Invalid(
                    "chunk records are not strictly ordered".into(),
                ));
            }
            last = Some(&record.key);
            encoded.record(record);
            if encoded.0.len() > manifest.options.max_chunk_bytes {
                return Err(CheckpointError::BudgetExceeded(
                    "canonical chunk exceeds manifest limit".into(),
                ));
            }
        }
        let next = Self {
            manifest_digest: self.manifest_digest,
            next_chunk: self.next_chunk + 1,
            records: self
                .records
                .checked_add(records.len() as u64)
                .ok_or_else(|| CheckpointError::Invalid("import count overflow".into()))?,
            previous_hash: hash(&encoded.0),
            last_key: records.last().map(|record| record.key.clone()),
            complete: false,
        };
        if next.records > manifest.records
            || (next.next_chunk == manifest.chunks
                && (next.records != manifest.records || next.previous_hash != manifest.root_hash))
        {
            return Err(CheckpointError::Invalid(
                "chunk cannot certify final manifest commitment".into(),
            ));
        }
        Ok(next)
    }
}

/// Stable target evidence returned by a completed validated import. It retains
/// historical source identity without granting that writer transport trust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReceipt {
    target: StoreIdentity,
    source: StoreIdentity,
    manifest_digest: [u8; 32],
    root_hash: [u8; 32],
    records: u64,
}
/// An actual target backend and its returned stable import receipt.
pub struct CheckpointReplicaObservation<'a> {
    pub store: &'a dyn KvStore,
    pub receipt: &'a ImportReceipt,
}
/// Verified stable copies of this exact checkpoint at every requested target.
/// This is separate from historical replication ACKs and process authority.
#[derive(Debug, Clone)]
pub struct CheckpointReplicaCertificate {
    manifest_digest: [u8; 32],
    targets: Vec<ImportReceipt>,
}
impl CheckpointReplicaCertificate {
    pub fn manifest_digest(&self) -> &[u8; 32] {
        &self.manifest_digest
    }
    pub fn targets(&self) -> &[ImportReceipt] {
        &self.targets
    }
    pub fn collect(
        manifest: &CheckpointManifest,
        expected: &[StoreIdentity],
        observations: &[CheckpointReplicaObservation<'_>],
    ) -> Result<Self, CheckpointError> {
        if expected.is_empty() || expected.len() > 256 || expected.len() != observations.len() {
            return Err(CheckpointError::Invalid(
                "replica certificate needs every bounded expected target".into(),
            ));
        }
        let mut targets = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for observation in observations {
            let receipt = observation.receipt;
            if !expected.contains(receipt.target())
                || !seen.insert(receipt.target().node_id())
                || receipt.source() != manifest.source()
                || receipt.manifest_digest() != &manifest.digest()
                || receipt.root_hash() != manifest.root_hash()
                || receipt.records() != manifest.records()
                || observation
                    .store
                    .checkpoint_backend()
                    .ok_or(CheckpointError::Unsupported)?
                    .import_evidence(receipt.manifest_digest())?
                    .as_ref()
                    != Some(receipt)
            {
                return Err(CheckpointError::Invalid(
                    "target does not retain this exact stable checkpoint floor".into(),
                ));
            }
            targets.push(receipt.clone());
        }
        for observation in observations {
            if observation
                .store
                .checkpoint_backend()
                .ok_or(CheckpointError::Unsupported)?
                .import_evidence(observation.receipt.manifest_digest())?
                .as_ref()
                != Some(observation.receipt)
            {
                return Err(CheckpointError::Invalid(
                    "target floor changed during certificate collection".into(),
                ));
            }
        }
        targets.sort_by(|a, b| a.target().node_id().cmp(b.target().node_id()));
        Ok(Self {
            manifest_digest: manifest.digest(),
            targets,
        })
    }
}
impl ImportReceipt {
    pub fn target(&self) -> &StoreIdentity {
        &self.target
    }
    pub fn source(&self) -> &StoreIdentity {
        &self.source
    }
    pub fn manifest_digest(&self) -> &[u8; 32] {
        &self.manifest_digest
    }
    pub fn root_hash(&self) -> &[u8; 32] {
        &self.root_hash
    }
    pub fn records(&self) -> u64 {
        self.records
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Encoder::default();
        out.identity(&self.target);
        out.identity(&self.source);
        out.digest(&self.manifest_digest);
        out.digest(&self.root_hash);
        out.u64(self.records);
        out.0
    }
    /// Decode persisted evidence, checking its bounded canonical shape. Hashes
    /// detect corruption, not an untrusted party's authorization or signatures.
    pub fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        let mut input = Decoder::new(bytes);
        let target = input.identity()?;
        let source = input.identity()?;
        let manifest_digest = input.digest()?;
        let root_hash = input.digest()?;
        let records = input.u64()?;
        input.finish()?;
        Ok(Self {
            target,
            source,
            manifest_digest,
            root_hash,
            records,
        })
    }
}

/// Evidence attached by `Node::export_checkpoint` only when that same node
/// completed a remote replication-drained shutdown. Independent of local
/// stable storage, and never synthesized for an unavailable member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteReplicationEvidence {
    pub(crate) source_id: String,
    pub(crate) targets: Vec<(crate::shard::FragmentId, String)>,
    pub(crate) durability: crate::replication::ReplicationAckDurability,
}
impl RemoteReplicationEvidence {
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn targets(&self) -> &[(crate::shard::FragmentId, String)] {
        &self.targets
    }
    pub fn durability(&self) -> crate::replication::ReplicationAckDurability {
        self.durability
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointOptions {
    max_chunk_bytes: usize,
    max_chunk_records: usize,
    pub(crate) remote: Option<RemoteReplicationEvidence>,
}
impl Default for CheckpointOptions {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 1024 * 1024,
            max_chunk_records: 256,
            remote: None,
        }
    }
}
impl CheckpointOptions {
    pub fn new(max_chunk_bytes: usize, max_chunk_records: usize) -> Result<Self, CheckpointError> {
        if !(CHUNK_HEADER_BYTES + 1..=MAX_CHUNK_BYTES).contains(&max_chunk_bytes)
            || !(1..=4096).contains(&max_chunk_records)
        {
            return Err(CheckpointError::Invalid(
                "chunk byte/record limits outside supported bounds".into(),
            ));
        }
        Ok(Self {
            max_chunk_bytes,
            max_chunk_records,
            remote: None,
        })
    }
    pub fn max_chunk_bytes(&self) -> usize {
        self.max_chunk_bytes
    }
    pub fn max_chunk_records(&self) -> usize {
        self.max_chunk_records
    }
    fn encode(&self, out: &mut Encoder) {
        out.u32(self.max_chunk_bytes as u32);
        out.u32(self.max_chunk_records as u32);
        match &self.remote {
            None => out.u8(0),
            Some(remote) => {
                out.u8(1);
                out.string(&remote.source_id);
                out.u8(match remote.durability {
                    crate::replication::ReplicationAckDurability::MemoryApplied => 0,
                    crate::replication::ReplicationAckDurability::StoreCommitted => 1,
                    crate::replication::ReplicationAckDurability::StableCommitted => 2,
                });
                out.u32(remote.targets.len() as u32);
                for (fragment, peer) in &remote.targets {
                    out.u32(fragment.0);
                    out.string(peer);
                }
            }
        }
    }
    fn decode(input: &mut Decoder<'_>) -> Result<Self, CheckpointError> {
        let bytes = input.u32()? as usize;
        let records = input.u32()? as usize;
        let mut result = Self::new(bytes, records)?;
        result.remote = match input.u8()? {
            0 => None,
            1 => {
                let source_id = input.string(255)?;
                let durability = match input.u8()? {
                    0 => crate::replication::ReplicationAckDurability::MemoryApplied,
                    1 => crate::replication::ReplicationAckDurability::StoreCommitted,
                    2 => crate::replication::ReplicationAckDurability::StableCommitted,
                    _ => {
                        return Err(CheckpointError::Invalid(
                            "invalid remote evidence durability".into(),
                        ))
                    }
                };
                let count = input.u32()? as usize;
                if count > 4096 {
                    return Err(CheckpointError::BudgetExceeded(
                        "too many remote targets".into(),
                    ));
                }
                let mut targets = Vec::with_capacity(count);
                for _ in 0..count {
                    targets.push((crate::shard::FragmentId(input.u32()?), input.string(255)?));
                }
                Some(RemoteReplicationEvidence {
                    source_id,
                    targets,
                    durability,
                })
            }
            _ => return Err(CheckpointError::Invalid("invalid evidence flag".into())),
        };
        Ok(result)
    }
}

/// Final constant-sized transfer description (apart from optional bounded ACK
/// evidence). Its root commits to ordered chunks and every raw record field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    source: StoreIdentity,
    options: CheckpointOptions,
    chunks: u64,
    records: u64,
    root_hash: [u8; 32],
    hashes_hash: [u8; 32],
}
impl CheckpointManifest {
    pub fn source(&self) -> &StoreIdentity {
        &self.source
    }
    pub fn options(&self) -> &CheckpointOptions {
        &self.options
    }
    pub fn chunks(&self) -> u64 {
        self.chunks
    }
    pub fn records(&self) -> u64 {
        self.records
    }
    pub fn root_hash(&self) -> &[u8; 32] {
        &self.root_hash
    }
    pub fn chunk_index_hash(&self) -> &[u8; 32] {
        &self.hashes_hash
    }
    pub fn remote_replication(&self) -> Option<&RemoteReplicationEvidence> {
        self.options.remote.as_ref()
    }
    /// Every export requires a positive local stable fence.
    pub fn local_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Stable
    }
    pub fn digest(&self) -> [u8; 32] {
        hash(&self.encode())
    }
    fn initial_hash(&self) -> [u8; 32] {
        let mut out = Encoder::default();
        out.0.extend(FORMAT_MAGIC);
        out.identity(&self.source);
        self.options.encode(&mut out);
        hash(&out.0)
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Encoder::default();
        out.0.extend(FORMAT_MAGIC);
        out.identity(&self.source);
        self.options.encode(&mut out);
        out.u64(self.chunks);
        out.u64(self.records);
        out.digest(&self.root_hash);
        out.digest(&self.hashes_hash);
        out.0
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(CheckpointError::BudgetExceeded("manifest metadata".into()));
        }
        let mut input = Decoder::new(bytes);
        if input.take(8)? != FORMAT_MAGIC {
            return Err(CheckpointError::Invalid(
                "unsupported checkpoint format".into(),
            ));
        }
        let source = input.identity()?;
        let options = CheckpointOptions::decode(&mut input)?;
        if options
            .remote
            .as_ref()
            .is_some_and(|remote| remote.source_id != source.node_id())
        {
            return Err(CheckpointError::Invalid(
                "remote evidence belongs to a different source".into(),
            ));
        }
        if let Some(remote) = &options.remote {
            if remote.targets.iter().any(|(fragment, peer)| {
                fragment.0 >= source.fragment_count() || peer.is_empty() || peer == source.node_id()
            }) || remote.targets.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(CheckpointError::Invalid(
                    "remote evidence targets are invalid, duplicated or unordered".into(),
                ));
            }
        }
        let chunks = input.u64()?;
        let records = input.u64()?;
        let root_hash = input.digest()?;
        let hashes_hash = input.digest()?;
        input.finish()?;
        Ok(Self {
            source,
            options,
            chunks,
            records,
            root_hash,
            hashes_hash,
        })
    }
    pub fn read(directory: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        Self::decode(&read_bounded(
            &directory.as_ref().join("manifest"),
            MAX_METADATA_BYTES,
        )?)
    }
}

pub(crate) fn validate_identity(identity: &StoreIdentity) -> Result<(), CheckpointError> {
    if identity.fragment_count() == 0
        || identity.node_id().is_empty()
        || identity.node_id().len() > 255
    {
        return Err(CheckpointError::Invalid(
            "invalid node identity or fragment count".into(),
        ));
    }
    Ok(())
}
pub(crate) fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, CheckpointError> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(CheckpointError::BudgetExceeded(
            "file exceeds bounded format size".into(),
        ));
    }
    let mut out = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut out)?;
    if out.len() > maximum {
        return Err(CheckpointError::BudgetExceeded(
            "file grew beyond limit".into(),
        ));
    }
    Ok(out)
}
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CheckpointError> {
    let temporary = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| CheckpointError::Invalid("file lacks parent directory".into()))?,
    )?
    .sync_all()?;
    Ok(())
}
fn chunk_path(directory: &Path, index: u64) -> PathBuf {
    directory.join(format!("chunk-{index:020}"))
}

/// Export cursor persisted after each durable chunk. Reopening an interrupted
/// export reads the same immutable backend snapshot, including after source
/// restart or later source writes. Chunks are independent bounded files.
pub struct CheckpointExporter {
    directory: PathBuf,
    reader: Box<dyn CheckpointReader>,
    manifest: CheckpointManifest,
    last_key: Option<Vec<u8>>,
    complete: bool,
}
impl CheckpointExporter {
    /// Resume the exact persisted source, limits and evidence after process
    /// restart. The immutable snapshot is reopened, never recaptured.
    pub fn resume(
        store: Arc<dyn KvStore>,
        source: StoreIdentity,
        directory: impl AsRef<Path>,
    ) -> Result<Self, CheckpointError> {
        let bytes = read_bounded(
            &directory.as_ref().join("export-state"),
            MAX_CHUNK_BYTES + MAX_METADATA_BYTES,
        )?;
        let mut input = Decoder::new(&bytes);
        let manifest = CheckpointManifest::decode(&input.bytes(MAX_METADATA_BYTES)?)?;
        if manifest.source != source {
            return Err(CheckpointError::Invalid(
                "export resume changes source identity".into(),
            ));
        }
        Self::open(store, source, directory, manifest.options)
    }
    pub fn open(
        store: Arc<dyn KvStore>,
        source: StoreIdentity,
        directory: impl AsRef<Path>,
        options: CheckpointOptions,
    ) -> Result<Self, CheckpointError> {
        validate_identity(&source)?;
        let backend = store
            .checkpoint_backend()
            .ok_or(CheckpointError::Unsupported)?;
        let directory = directory.as_ref().to_path_buf();
        let snapshot = directory.join("snapshot");
        let state_path = directory.join("export-state");
        let binding = if snapshot.exists() && state_path.exists() {
            None
        } else {
            let binding = store.bind(&source)?;
            if !binding.is_managed() {
                return Err(CheckpointError::Invalid(
                    "checkpoint snapshots require a managed exclusive source binding".into(),
                ));
            }
            if store.stable_fence()? != StoreCommitDurability::Stable {
                return Err(CheckpointError::Invalid(
                    "export requires a positive stable local fence".into(),
                ));
            }
            Some(binding)
        };
        fs::create_dir_all(&directory)?;
        let (manifest, last_key, complete) = if state_path.exists() {
            let state = read_bounded(&state_path, MAX_CHUNK_BYTES + MAX_METADATA_BYTES)?;
            let mut input = Decoder::new(&state);
            let manifest = CheckpointManifest::decode(&input.bytes(MAX_METADATA_BYTES)?)?;
            if manifest.source != source || manifest.options != options {
                return Err(CheckpointError::Invalid(
                    "export retry changes source or limits/evidence".into(),
                ));
            }
            let last_key = match input.u8()? {
                0 => None,
                1 => Some(input.bytes(MAX_CHUNK_BYTES)?),
                _ => return Err(CheckpointError::Invalid("invalid export cursor".into())),
            };
            input.finish()?;
            let complete = directory.join("manifest").exists();
            if complete && CheckpointManifest::read(&directory)? != manifest {
                return Err(CheckpointError::Invalid(
                    "completed manifest differs from export state".into(),
                ));
            }
            (manifest, last_key, complete)
        } else {
            if directory.join("manifest").exists() || directory.join("snapshot").exists() {
                return Err(CheckpointError::Invalid(
                    "export directory has no resumable state".into(),
                ));
            }
            let mut manifest = CheckpointManifest {
                source,
                options,
                chunks: 0,
                records: 0,
                root_hash: [0; 32],
                hashes_hash: [0; 32],
            };
            manifest.root_hash = manifest.initial_hash();
            // Persist initial state before snapshot creation, so an interrupted
            // creation can be distinguished from a completed immutable snapshot.
            write_export_state(&directory, &manifest, None)?;
            (manifest, None, false)
        };
        if !snapshot.exists() {
            backend.create_snapshot(&snapshot)?;
        }
        if !directory.join("snapshot-ready").exists() {
            // A backend publishes its frozen directory atomically. Reopening
            // certifies that a crash between directory publication and this
            // small ready marker did not leave an incomplete snapshot.
            drop(backend.open_snapshot(&snapshot, &manifest.source)?);
            atomic_write(&directory.join("snapshot-ready"), &manifest.initial_hash())?;
        }
        if read_bounded(&directory.join("snapshot-ready"), 32)? != manifest.initial_hash() {
            return Err(CheckpointError::Invalid(
                "snapshot identity/evidence mismatch".into(),
            ));
        }
        let reader = backend.open_snapshot(&snapshot, &manifest.source)?;
        drop(binding);
        Ok(Self {
            directory,
            reader,
            manifest,
            last_key,
            complete,
        })
    }
    /// Copy at most one bounded chunk. `None` means the completed manifest is
    /// already durable. Every successful call can be retried after restart.
    pub fn next_chunk(&mut self) -> Result<Option<u64>, CheckpointError> {
        if self.complete {
            return Ok(None);
        }
        let records = self.reader.page(
            self.last_key.as_deref(),
            self.manifest.options.max_chunk_records,
            self.manifest.options.max_chunk_bytes - CHUNK_HEADER_BYTES,
        )?;
        if records.is_empty() {
            let mut hashes = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.directory.join("chunk-hashes"))?;
            if hashes.metadata()?.len()
                != self
                    .manifest
                    .chunks
                    .checked_mul(32)
                    .ok_or_else(|| CheckpointError::Invalid("chunk index length overflow".into()))?
            {
                return Err(CheckpointError::Invalid(
                    "export hash index differs from durable cursor".into(),
                ));
            }
            hashes.sync_all()?;
            self.manifest.hashes_hash = streaming_hash(&mut hashes, None)?;
            write_export_state(&self.directory, &self.manifest, self.last_key.as_deref())?;
            atomic_write(&self.directory.join("manifest"), &self.manifest.encode())?;
            self.complete = true;
            return Ok(None);
        }
        let mut previous = self.last_key.as_deref();
        if records.len() > self.manifest.options.max_chunk_records {
            return Err(CheckpointError::BudgetExceeded(
                "backend returned too many records".into(),
            ));
        }
        let mut out = Encoder::default();
        out.0.extend(FORMAT_MAGIC);
        out.u64(self.manifest.chunks);
        out.digest(&self.manifest.root_hash);
        out.u32(records.len() as u32);
        for record in &records {
            record.validate(&self.manifest.source)?;
            if previous.is_some_and(|previous| record.key.as_slice() <= previous) {
                return Err(CheckpointError::Invalid(
                    "snapshot page is not strictly ordered".into(),
                ));
            }
            previous = Some(&record.key);
            out.record(record);
            if out.0.len() > self.manifest.options.max_chunk_bytes {
                return Err(CheckpointError::BudgetExceeded(
                    "backend returned oversized chunk".into(),
                ));
            }
        }
        let index = self.manifest.chunks;
        let path = chunk_path(&self.directory, index);
        if path.exists() {
            if read_bounded(&path, self.manifest.options.max_chunk_bytes)? != out.0 {
                return Err(CheckpointError::Invalid(
                    "retry chunk differs from immutable snapshot".into(),
                ));
            }
        } else {
            atomic_write(&path, &out.0)?;
        }
        let digest = hash(&out.0);
        let mut hashes = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join("chunk-hashes"))?;
        let offset = index
            .checked_mul(32)
            .ok_or_else(|| CheckpointError::Invalid("chunk index length overflow".into()))?;
        let length = hashes.metadata()?.len();
        if length > offset && length <= offset + 32 {
            validate_index_prefix(&self.directory, &mut hashes, &self.manifest)?;
            hashes.set_len(offset)?;
        }
        if length >= offset && length <= offset + 32 {
            hashes.seek(SeekFrom::Start(offset))?;
            hashes.write_all(&digest)?;
            hashes.sync_all()?;
        } else {
            return Err(CheckpointError::Invalid(
                "export hash index has a different cursor".into(),
            ));
        }
        self.manifest.root_hash = digest;
        self.manifest.chunks += 1;
        self.manifest.records = self
            .manifest
            .records
            .checked_add(records.len() as u64)
            .ok_or_else(|| CheckpointError::Invalid("record count overflow".into()))?;
        self.last_key = records.last().map(|record| record.key.clone());
        write_export_state(&self.directory, &self.manifest, self.last_key.as_deref())?;
        Ok(Some(index))
    }
    pub fn finish(mut self) -> Result<CheckpointManifest, CheckpointError> {
        while self.next_chunk()?.is_some() {}
        Ok(self.manifest)
    }
}
fn write_export_state(
    directory: &Path,
    manifest: &CheckpointManifest,
    last_key: Option<&[u8]>,
) -> Result<(), CheckpointError> {
    let mut out = Encoder::default();
    out.bytes(&manifest.encode());
    match last_key {
        Some(key) => {
            out.u8(1);
            out.bytes(key);
        }
        None => out.u8(0),
    }
    atomic_write(&directory.join("export-state"), &out.0)
}

/// Resumable import cursor. The backend keeps records inaccessible to Node
/// startup until `finish` publishes the fully validated stable provenance.
pub struct CheckpointImporter {
    directory: PathBuf,
    manifest: CheckpointManifest,
    target: StoreIdentity,
    backend: Box<dyn CheckpointImport>,
    verified_hashes: File,
}
impl CheckpointImporter {
    pub fn open(
        store: Arc<dyn KvStore>,
        target: StoreIdentity,
        directory: impl AsRef<Path>,
    ) -> Result<Self, CheckpointError> {
        validate_identity(&target)?;
        let directory = directory.as_ref().to_path_buf();
        let manifest = CheckpointManifest::read(&directory)?;
        if target.fragment_count() != manifest.source.fragment_count() {
            return Err(CheckpointError::Invalid(
                "checkpoint import cannot change fragment count/partitioner".into(),
            ));
        }
        let verified_hashes = verify_bundle(&directory, &manifest)?;
        let backend = store
            .checkpoint_backend()
            .ok_or(CheckpointError::Unsupported)?
            .begin_import(&target, &manifest)?;
        Ok(Self {
            directory,
            manifest,
            target,
            backend,
            verified_hashes,
        })
    }
    pub fn progress(&self) -> Result<ImportProgress, CheckpointError> {
        self.backend.progress()
    }
    /// Validate and durably merge one chunk, checking its exact hash chain,
    /// record order, physical namespace, and full record budgets.
    pub fn next_chunk(&mut self) -> Result<bool, CheckpointError> {
        let previous = self.backend.progress()?;
        if previous.manifest_digest != self.manifest.digest()
            || previous.next_chunk > self.manifest.chunks
        {
            return Err(CheckpointError::Invalid(
                "backend import progress belongs to different manifest".into(),
            ));
        }
        if previous.complete || previous.next_chunk == self.manifest.chunks {
            return Ok(false);
        }
        let bytes = read_bounded(
            &chunk_path(&self.directory, previous.next_chunk),
            self.manifest.options.max_chunk_bytes,
        )?;
        self.verified_hashes.seek(SeekFrom::Start(
            previous
                .next_chunk
                .checked_mul(32)
                .ok_or_else(|| CheckpointError::Invalid("hash index overflow".into()))?,
        ))?;
        let mut expected_hash = [0; 32];
        self.verified_hashes.read_exact(&mut expected_hash)?;
        if hash(&bytes) != expected_hash {
            return Err(CheckpointError::Invalid(
                "chunk changed after full bundle validation".into(),
            ));
        }
        let mut input = Decoder::new(&bytes);
        if input.take(8)? != FORMAT_MAGIC
            || input.u64()? != previous.next_chunk
            || input.digest()? != previous.previous_hash
        {
            return Err(CheckpointError::Invalid(
                "chunk index/hash chain differs from import cursor".into(),
            ));
        }
        let count = input.u32()? as usize;
        if count == 0 || count > self.manifest.options.max_chunk_records {
            return Err(CheckpointError::Invalid(
                "invalid chunk record count".into(),
            ));
        }
        let mut records = Vec::with_capacity(count);
        let mut last = previous.last_key.clone();
        for _ in 0..count {
            let record = input.record()?;
            record.validate(&self.manifest.source)?;
            if last
                .as_deref()
                .is_some_and(|last| record.key.as_slice() <= last)
            {
                return Err(CheckpointError::Invalid(
                    "duplicate or unordered checkpoint key".into(),
                ));
            }
            last = Some(record.key.clone());
            records.push(record);
        }
        input.finish()?;
        let next = ImportProgress {
            manifest_digest: previous.manifest_digest,
            next_chunk: previous.next_chunk + 1,
            records: previous
                .records
                .checked_add(count as u64)
                .ok_or_else(|| CheckpointError::Invalid("record count overflow".into()))?,
            previous_hash: hash(&bytes),
            last_key: records.last().map(|record| record.key.clone()),
            complete: false,
        };
        if next.records > self.manifest.records
            || (next.next_chunk == self.manifest.chunks
                && (next.records != self.manifest.records
                    || next.previous_hash != self.manifest.root_hash))
        {
            return Err(CheckpointError::Invalid(
                "final chunk fails manifest record/hash commitment".into(),
            ));
        }
        self.backend.apply(&VerifiedCheckpointChunk {
            previous,
            next,
            records,
        })?;
        Ok(true)
    }
    pub fn finish(mut self) -> Result<ImportReceipt, CheckpointError> {
        while self.next_chunk()? {}
        let progress = self.backend.progress()?;
        if progress.next_chunk != self.manifest.chunks
            || progress.records != self.manifest.records
            || progress.previous_hash != self.manifest.root_hash
        {
            return Err(CheckpointError::Invalid(
                "incomplete checkpoint cannot be published".into(),
            ));
        }
        let receipt = ImportReceipt {
            target: self.target,
            source: self.manifest.source.clone(),
            manifest_digest: self.manifest.digest(),
            root_hash: self.manifest.root_hash,
            records: self.manifest.records,
        };
        if self.backend.finish(&receipt)? != StoreCommitDurability::Stable {
            return Err(CheckpointError::Invalid(
                "import backend did not certify stable completion".into(),
            ));
        }
        Ok(receipt)
    }
}

// Verify the manifest-committed digest index with bounded memory. Data chunks
// may arrive later: each one is checked against this immutable index before
// apply, while the incomplete backend marker keeps partial data non-serving.
fn verify_bundle(directory: &Path, manifest: &CheckpointManifest) -> Result<File, CheckpointError> {
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| CheckpointError::Invalid(error.to_string()))?;
    let name = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let path = std::env::temp_dir().join(format!("actordb-verified-{name}"));
    let mut index = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    // An open unlinked file remains owned by this session and needs no shared
    // path or cleanup race. Each restart regenerates it from the root hash.
    fs::remove_file(path)?;
    let mut source = File::open(directory.join("chunk-hashes"))?;
    let expected_bytes = manifest
        .chunks
        .checked_mul(32)
        .ok_or_else(|| CheckpointError::Invalid("hash index size overflow".into()))?;
    if !source.metadata()?.is_file() || source.metadata()?.len() != expected_bytes {
        return Err(CheckpointError::Invalid(
            "chunk digest index does not match manifest count".into(),
        ));
    }
    if streaming_hash(&mut source, Some(&mut index))? != manifest.hashes_hash {
        return Err(CheckpointError::Invalid(
            "chunk digest index does not match manifest hash".into(),
        ));
    }
    index.seek(SeekFrom::Start(0))?;
    Ok(index)
}

fn streaming_hash(
    source: &mut File,
    mut copy: Option<&mut File>,
) -> Result<[u8; 32], CheckpointError> {
    source.seek(SeekFrom::Start(0))?;
    let mut remaining = source.metadata()?.len();
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    while remaining != 0 {
        let limit = remaining.min(buffer.len() as u64) as usize;
        let count = source.read(&mut buffer[..limit])?;
        if count == 0 {
            return Err(CheckpointError::Invalid(
                "chunk hash index was truncated during verification".into(),
            ));
        }
        remaining -= count as u64;
        digest.update(&buffer[..count]);
        if let Some(copy) = &mut copy {
            copy.write_all(&buffer[..count])?;
        }
    }
    if source.read(&mut buffer[..1])? != 0 {
        return Err(CheckpointError::Invalid(
            "chunk hash index grew during verification".into(),
        ));
    }
    Ok(digest.finalize().into())
}

fn validate_index_prefix(
    directory: &Path,
    index: &mut File,
    manifest: &CheckpointManifest,
) -> Result<(), CheckpointError> {
    index.seek(SeekFrom::Start(0))?;
    let mut previous = manifest.initial_hash();
    let mut records = 0u64;
    for chunk in 0..manifest.chunks {
        let bytes = read_bounded(
            &chunk_path(directory, chunk),
            manifest.options.max_chunk_bytes,
        )?;
        let mut input = Decoder::new(&bytes);
        if input.take(8)? != FORMAT_MAGIC || input.u64()? != chunk || input.digest()? != previous {
            return Err(CheckpointError::Invalid(
                "committed export prefix is corrupt".into(),
            ));
        }
        records = records
            .checked_add(input.u32()? as u64)
            .ok_or_else(|| CheckpointError::Invalid("export prefix count overflow".into()))?;
        previous = hash(&bytes);
        let mut expected = [0; 32];
        index.read_exact(&mut expected)?;
        if previous != expected {
            return Err(CheckpointError::Invalid(
                "committed hash index prefix is corrupt".into(),
            ));
        }
    }
    if previous != manifest.root_hash || records != manifest.records {
        return Err(CheckpointError::Invalid(
            "committed export prefix differs from durable cursor".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_bundle_manifest_is_rejected_without_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest");
        for magic in [b"ACTCP001", b"ACTCP002"] {
            std::fs::write(&path, magic).unwrap();
            let before = std::fs::read(&path).unwrap();
            assert!(CheckpointManifest::read(dir.path()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }
    #[test]
    fn unsupported_backend_never_uses_full_load_or_creates_transfer_files() {
        struct Legacy;
        impl KvStore for Legacy {
            fn bind(&self, _: &StoreIdentity) -> Result<crate::store::StoreBinding, StoreError> {
                Ok(crate::store::StoreBinding::unmanaged())
            }
            fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                panic!("unsupported checkpoint must not load namespaces")
            }
            fn commit_durability(&self) -> StoreCommitDurability {
                StoreCommitDurability::Buffered
            }
            fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
                Ok(StoreCommitDurability::Buffered)
            }
            fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
                panic!("unbounded load fallback must never run")
            }
            fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
                Ok(())
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let bundle = directory.path().join("unsupported");
        assert!(matches!(
            CheckpointExporter::open(
                Arc::new(Legacy),
                StoreIdentity::new("legacy".into(), 1),
                &bundle,
                CheckpointOptions::default()
            ),
            Err(CheckpointError::Unsupported)
        ));
        assert!(!bundle.exists());
        assert!(CheckpointOptions::new(MAX_CHUNK_BYTES + 1, 1).is_err());
        assert!(CheckpointOptions::new(100, 4097).is_err());
    }
}
