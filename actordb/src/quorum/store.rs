//! Atomic stable protocol storage, including promises and accepted proposals.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use fs2::FileExt;
use prost::Message;
use sha2::{Digest, Sha256};

use super::{Manifest, QuorumError};
use crate::shard::FragmentId;
use crate::store::StoreBinding;

const MAGIC: &[u8; 8] = b"AQSNAP01";
const HARD_LIMIT: usize = 16 * 1024 * 1024;

/// A stable, atomic replacement store for protocol state. Unlike `KvStore`,
/// every successful save MUST survive process/OS crashes before returning.
/// An ambiguous failure must fail closed until the store is reopened.
pub trait QuorumStore: Send + Sync + 'static {
    /// Validate durable identity and exclusively attach one live node.
    fn bind(&self, manifest: &Manifest) -> Result<StoreBinding, QuorumError>;
    fn load(&self, fragment: FragmentId) -> Result<Option<Vec<u8>>, QuorumError>;
    /// Atomically replace all protocol state, including fencing promises.
    fn save(&self, fragment: FragmentId, state: &[u8]) -> Result<(), QuorumError>;
}

/// Offered filesystem backend. Uses checksummed files, atomic rename, file
/// fsync, directory fsync, and an exclusive OS lock. It requires a local
/// filesystem providing those guarantees; it is not a `KvStore` adapter.
pub struct FileQuorumStore {
    path: PathBuf,
    _lock: File,
    attached: Arc<AtomicBool>,
    failed: AtomicBool,
    writes: Mutex<()>,
}

fn storage(error: impl std::fmt::Display) -> QuorumError {
    QuorumError::Storage(error.to_string())
}

impl FileQuorumStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, QuorumError> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path).map_err(storage)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("lock"))
            .map_err(storage)?;
        lock.try_lock_exclusive().map_err(storage)?;
        Ok(Self {
            path,
            _lock: lock,
            attached: Arc::new(AtomicBool::new(false)),
            failed: AtomicBool::new(false),
            writes: Mutex::new(()),
        })
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>, QuorumError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(storage("store requires reopening after a failed write"));
        }
        let mut file = match File::open(self.path.join(name)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let length = usize::try_from(file.metadata().map_err(storage)?.len()).map_err(storage)?;
        if !(40..=HARD_LIMIT + 40).contains(&length) {
            return Err(storage("invalid quorum file length"));
        }
        let mut bytes = Vec::with_capacity(length);
        Read::by_ref(&mut file)
            .take((HARD_LIMIT + 41) as u64)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if bytes.len() != length
            || &bytes[..8] != MAGIC
            || Sha256::digest(&bytes[40..]).as_slice() != &bytes[8..40]
        {
            return Err(storage("corrupt quorum file"));
        }
        Ok(Some(bytes.split_off(40)))
    }

    fn replace(&self, name: &str, bytes: &[u8]) -> Result<(), QuorumError> {
        if bytes.len() > HARD_LIMIT {
            return Err(storage("quorum file exceeds hard limit"));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(storage("store requires reopening after a failed write"));
        }
        let _writer = self.writes.lock().map_err(storage)?;
        let result = (|| {
            let temp = self.path.join(format!("{name}.pending"));
            let mut file = File::create(&temp).map_err(storage)?;
            file.write_all(MAGIC).map_err(storage)?;
            file.write_all(&Sha256::digest(bytes)).map_err(storage)?;
            file.write_all(bytes).map_err(storage)?;
            file.sync_all().map_err(storage)?;
            std::fs::rename(temp, self.path.join(name)).map_err(storage)?;
            File::open(&self.path)
                .map_err(storage)?
                .sync_all()
                .map_err(storage)
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
}

impl QuorumStore for FileQuorumStore {
    fn bind(&self, manifest: &Manifest) -> Result<StoreBinding, QuorumError> {
        if self
            .attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(storage("quorum store already attached"));
        }
        let result = (|| {
            match self.read("manifest")? {
                Some(bytes) => {
                    let stored = Manifest::decode(bytes.as_slice()).map_err(storage)?;
                    if stored != *manifest {
                        return Err(storage(
                            "quorum store identity or genesis placement mismatch",
                        ));
                    }
                }
                None => {
                    // A missing identity must never turn existing protocol files into genesis.
                    for entry in std::fs::read_dir(&self.path).map_err(storage)? {
                        let name = entry.map_err(storage)?.file_name();
                        if name != "lock" && name != "manifest.pending" {
                            return Err(storage("nonempty quorum store has no manifest"));
                        }
                    }
                    for (index, membership) in manifest.genesis.iter().enumerate() {
                        self.replace(
                            &format!("fragment-{index}"),
                            &super::state::genesis(
                                membership.clone(),
                                FragmentId(index as u32),
                                &manifest.cluster_id,
                            )
                            .encode_to_vec(),
                        )?;
                    }
                    self.replace("manifest", &manifest.encode_to_vec())?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.attached.store(false, Ordering::Release);
            return Err(error);
        }
        let attached = self.attached.clone();
        Ok(StoreBinding::managed(move || {
            attached.store(false, Ordering::Release)
        }))
    }

    fn load(&self, fragment: FragmentId) -> Result<Option<Vec<u8>>, QuorumError> {
        if !self.attached.load(Ordering::Acquire) {
            return Err(storage("quorum store is not bound"));
        }
        self.read(&format!("fragment-{}", fragment.0))
    }

    fn save(&self, fragment: FragmentId, state: &[u8]) -> Result<(), QuorumError> {
        if !self.attached.load(Ordering::Acquire) {
            return Err(storage("quorum store is not bound"));
        }
        self.replace(&format!("fragment-{}", fragment.0), state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            cluster_id: "test".into(),
            node_id: "a".into(),
            genesis: vec![super::super::Membership {
                voters: vec!["a".into()],
                next_voters: vec![],
            }],
            policy: vec![1; 32],
        }
    }

    #[test]
    fn stable_replacement_identity_lock_and_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let store = FileQuorumStore::open(directory.path()).unwrap();
        assert!(FileQuorumStore::open(directory.path()).is_err());
        let binding = store.bind(&manifest()).unwrap();
        assert!(store.bind(&manifest()).is_err());
        store.save(FragmentId(0), b"first").unwrap();
        store.save(FragmentId(0), b"second").unwrap();
        drop(binding);
        drop(store);
        let store = FileQuorumStore::open(directory.path()).unwrap();
        let mut wrong = manifest();
        wrong.node_id = "b".into();
        assert!(store.bind(&wrong).is_err());
        let _binding = store.bind(&manifest()).unwrap();
        assert_eq!(store.load(FragmentId(0)).unwrap().unwrap(), b"second");
        std::fs::write(directory.path().join("fragment-0"), b"corrupt").unwrap();
        assert!(store.load(FragmentId(0)).is_err());
    }
}
