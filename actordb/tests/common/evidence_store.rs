//! Controllable real buffered commits and explicit stable fences for concern tests.
use actordb::actor::VersionedValue;
use actordb::store::{KvStore, StoreCommitDurability, StoreError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::Notify;

pub struct StableGate {
    pub entered: Notify,
    released: (Mutex<bool>, Condvar),
}
impl StableGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: (Mutex::new(false), Condvar::new()),
        })
    }
    pub fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
    fn wait(&self) {
        self.entered.notify_one();
        let mut released = self.released.0.lock().unwrap();
        while !*released {
            released = self.released.1.wait(released).unwrap();
        }
    }
}
pub struct ReleaseGate(pub Arc<StableGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub struct EvidenceStore {
    pub stable: AtomicBool,
    pub fail_write: AtomicBool,
    pub fail_fence: AtomicBool,
    pub commits: AtomicU64,
    pub fences: AtomicU64,
    pub gate: Mutex<Option<Arc<StableGate>>>,
    values: Mutex<HashMap<Vec<u8>, VersionedValue>>,
}
impl EvidenceStore {
    pub fn new(stable: bool) -> Arc<Self> {
        Arc::new(Self {
            stable: AtomicBool::new(stable),
            fail_write: AtomicBool::new(false),
            fail_fence: AtomicBool::new(false),
            commits: AtomicU64::new(0),
            fences: AtomicU64::new(0),
            gate: Mutex::new(None),
            values: Mutex::new(HashMap::new()),
        })
    }
}
impl KvStore for EvidenceStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn write_batch(&self, values: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if self.fail_write.load(Ordering::SeqCst) {
            return Err(StoreError("injected commit failure".into()));
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.values.lock().unwrap().extend(values.iter().cloned());
        Ok(())
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.fences.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.gate.lock().unwrap().clone() {
            gate.wait();
        }
        if self.fail_fence.load(Ordering::SeqCst) {
            return Err(StoreError("injected stable fence failure".into()));
        }
        Ok(if self.stable.load(Ordering::SeqCst) {
            StoreCommitDurability::Stable
        } else {
            StoreCommitDurability::Buffered
        })
    }
}
