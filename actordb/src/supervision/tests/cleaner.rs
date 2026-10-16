// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Paged automatic expiry and explicit snapshot persistence.

use super::*;
use crate::actor::VersionedValue;
use crate::supervision::test_support::{GatedStore, ReleaseMaintenanceStore};
use crate::supervision::{Snapshotter, SnapshotterArgs, SnapshotterMessage};

#[tokio::test]
async fn automatic_cleaner_completes_multiple_pages_and_manual_snap_flushes_markers() {
    let store = Arc::new(GatedStore::default());
    let _release = ReleaseMaintenanceStore(store.clone());
    store.release();
    let values = (0..600)
        .map(|i| {
            let mut value = VersionedValue::new(vec![1], 10, "writer".into());
            value.expires_at_ms = Some(1);
            (format!("k{i:04}"), value)
        })
        .collect();
    let shared = Arc::new(FragmentState::new(
        values,
        Some(store.clone()),
        usize::MAX,
        Duration::from_secs(3600),
    ));
    let (cleaner, _) = Cleaner::spawn(
        None,
        Cleaner,
        CleanerArgs {
            fragment: FragmentId(0),
            shared: shared.clone(),
            interval: Duration::from_secs(3600),
            tombstone_gc_age: None,
            reset_senders: Arc::new(Mutex::new(HashMap::new())),
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while shared.snapshot_with_revision().await.1 != 600 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        store.entries.lock().unwrap().is_empty(),
        "idle buffer flushed before explicit snapshot"
    );
    assert_eq!(shared.expire_values(now_ms()).await.unwrap(), 0);
    cleaner.send_message(CleanerMessage::Sweep).unwrap();
    cleaner.drain().unwrap();
    cleaner.wait(None).await.unwrap();
    let (snapshotter, _) = Snapshotter::spawn(
        None,
        Snapshotter,
        SnapshotterArgs {
            shared,
            interval: Duration::from_secs(3600),
        },
    )
    .await
    .unwrap();
    snapshotter.send_message(SnapshotterMessage::Snap).unwrap();
    snapshotter.drain().unwrap();
    snapshotter.wait(None).await.unwrap();
    let entries = store.entries.lock().unwrap();
    assert_eq!(entries.len(), 600);
    assert!(entries
        .iter()
        .all(|(_, value)| value.is_tombstone && value.expires_at_ms == Some(1)));
}
