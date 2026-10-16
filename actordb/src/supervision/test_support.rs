// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

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
