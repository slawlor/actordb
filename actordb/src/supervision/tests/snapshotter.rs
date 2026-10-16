// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Automatic flush persistence and lease rearming under a paused timer.

use super::*;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use crate::actor::{ApplyOutcome, ReplicatedOp, VersionedValue};
use crate::store::{KvStore, StoreError};

type SnapshotWrites = Vec<(Vec<u8>, VersionedValue)>;

#[derive(Default)]
struct RecordingSnapshotStore {
    batches: StdMutex<Vec<SnapshotWrites>>,
}

impl KvStore for RecordingSnapshotStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.batches.lock().unwrap().push(writes.to_vec());
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn automatic_snapshot_flushes_buffered_values_and_rearms_after_completion() {
    let store = Arc::new(RecordingSnapshotStore::default());
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        Some(store.clone()),
        usize::MAX,
        Duration::from_secs(3600),
    ));
    let first = VersionedValue::new(b"first".to_vec(), 10, "writer".into());
    assert_eq!(
        shared
            .apply(&ReplicatedOp {
                key: "first".into(),
                value: first.clone(),
                seq: 1,
                boot_id: 1,
            })
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    let interval = Duration::from_millis(10);
    let (snapshotter, handle) = Snapshotter::spawn(
        None,
        Snapshotter,
        SnapshotterArgs {
            shared: shared.clone(),
            interval,
        },
    )
    .await
    .unwrap();
    // Neither buffered apply nor the ticker's startup may flush. Let the
    // actual actor/ticker run without advancing to their first deadline.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(store.batches.lock().unwrap().is_empty());
    tokio::time::advance(interval).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        if store.batches.lock().unwrap().len() == 1 {
            break;
        }
    }
    assert_eq!(
        *store.batches.lock().unwrap(),
        vec![vec![(b"first".to_vec(), first.clone())]]
    );

    // A second buffered expiry floor needs a later automatic trigger:
    // persisting the first batch must have released the coalescing lease.
    let mut expired = VersionedValue::new(b"expired".to_vec(), 20, "writer".into());
    expired.expires_at_ms = Some(1);
    let floor = expired.expiry_tombstone();
    assert_eq!(
        shared
            .apply(&ReplicatedOp {
                key: "second".into(),
                value: expired,
                seq: 2,
                boot_id: 1,
            })
            .await
            .unwrap(),
        ApplyOutcome::Expired
    );
    assert_eq!(store.batches.lock().unwrap().len(), 1);
    tokio::time::advance(interval).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        if store.batches.lock().unwrap().len() == 2 {
            break;
        }
    }
    assert_eq!(
        *store.batches.lock().unwrap(),
        vec![
            vec![(b"first".to_vec(), first.clone())],
            vec![(b"second".to_vec(), floor.clone())],
        ]
    );
    assert_eq!(shared.get("first").await, Some(first));
    assert_eq!(shared.get("second").await, None);
    assert_eq!(
        shared.snapshot_with_revision().await.0.get("second"),
        Some(&floor)
    );
    snapshotter.drain().unwrap();
    handle.await.unwrap();
    tokio::time::advance(interval * 2).await;
    assert_eq!(store.batches.lock().unwrap().len(), 2);
}
