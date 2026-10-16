// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Shared-state expiry, snapshots, and storage-fence regressions.

use super::flush_tests::FlakyStore;
use super::*;
use crate::actor::test_support::{test_op, test_state};
use crate::store::StoreError;
use std::sync::Mutex as StdMutex;
struct StableFenceStore {
    reported: StoreCommitDurability,
    events: std::sync::Mutex<Vec<&'static str>>,
}

impl StableFenceStore {
    fn new(reported: StoreCommitDurability) -> Self {
        Self {
            reported,
            events: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl KvStore for StableFenceStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, _writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.events.lock().unwrap().push("write");
        Ok(())
    }

    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.events.lock().unwrap().push("fence");
        Ok(self.reported)
    }
}
fn expiring_value(payload: Vec<u8>, deadline: u64) -> VersionedValue {
    let mut value = VersionedValue::new(payload, 10, "writer".to_string());
    value.expires_at_ms = Some(deadline);
    value
}

#[tokio::test]
async fn expiry_pages_bound_visits_even_for_sparse_non_ttl_cache_and_empty_key() {
    let mut values = HashMap::with_capacity(100_000);
    values.insert(String::new(), VersionedValue::new(vec![], 1, "n".into()));
    for i in 0..600 {
        values.insert(
            format!("k{i:04}"),
            VersionedValue::new(vec![], 1, "n".into()),
        );
    }
    let state = test_state(values);
    let mut pass = ExpiryPass::new(100);
    let first = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!(
        (first.visited, first.converted, first.done),
        (256, 0, false)
    );
    assert_eq!(pass.after_key.as_deref(), Some("k0254"));
    let second = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!(
        (second.visited, second.converted, second.done),
        (256, 0, false)
    );
    let third = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!((third.visited, third.converted, third.done), (89, 0, true));
    assert_eq!(state.revision.load(Ordering::Acquire), 0);
    assert_eq!(
        state.expire_values_page(&mut pass).await.unwrap().visited,
        0
    );

    let empty = test_state(HashMap::new());
    let page = empty
        .expire_values_page(&mut ExpiryPass::new(100))
        .await
        .unwrap();
    assert_eq!(
        (page.visited, page.converted, page.work_bytes, page.done),
        (0, 0, 0, true)
    );
    let empty_key = test_state(HashMap::from([(
        String::new(),
        expiring_value(vec![1], 100),
    )]));
    let mut pass = ExpiryPass::new(100);
    let page = empty_key.expire_values_page(&mut pass).await.unwrap();
    assert_eq!((page.visited, page.converted, page.done), (1, 1, true));
    assert_eq!(pass.after_key.as_deref(), Some(""));
}

#[tokio::test]
async fn expiry_pages_soft_bytes_charge_capacity_and_allow_one_terminal_record() {
    let mut payload = Vec::with_capacity(EXPIRY_PAGE_WORK_BYTES + 17);
    payload.push(1);
    let oversized = expiring_value(payload, 100);
    let oversized_charge = expiry_record_work_bytes("a", &oversized, true, false);
    let state = test_state(HashMap::from([
        ("a".into(), oversized),
        ("z".into(), expiring_value(vec![1], 100)),
    ]));
    let mut pass = ExpiryPass::new(100);
    let page = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!((page.visited, page.converted, page.done), (1, 1, false));
    assert_eq!(
        page.work_bytes,
        oversized_charge + std::mem::size_of::<String>() + 1
    );
    assert!(page.work_bytes >= EXPIRY_PAGE_WORK_BYTES);
    assert_eq!(
        state.expire_values_page(&mut pass).await.unwrap().visited,
        1
    );

    let payload = vec![1; EXPIRY_PAGE_WORK_BYTES];
    let state = test_state(HashMap::from([
        ("a".into(), expiring_value(vec![1], 100)),
        ("b".into(), expiring_value(payload, 100)),
        ("c".into(), expiring_value(vec![1], 100)),
    ]));
    let mut pass = ExpiryPass::new(100);
    let page = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!((page.visited, page.converted, page.done), (2, 2, false));
    assert_eq!(pass.after_key.as_deref(), Some("b"));
    assert_eq!(
        state.expire_values_page(&mut pass).await.unwrap().visited,
        1
    );
}

#[tokio::test]
async fn expiry_pages_publish_each_revision_and_recheck_current_winners_at_pinned_time() {
    let values = (0..520)
        .map(|i| (format!("k{i:04}"), expiring_value(vec![1], 100)))
        .collect();
    let state = test_state(values);
    let mut pass = ExpiryPass::new(100);
    let first = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!(first.converted, 256);
    let first_revision = state.revision.load(Ordering::Acquire);
    let winner = VersionedValue::new(vec![2], 11, "writer".into());
    // This future expiry is not expired at the pass's pinned instant.
    let mut future = expiring_value(vec![3], u64::MAX);
    future.timestamp_ms = 11;
    let winner_op = test_op("k0256", winner.clone());
    let future_op = test_op("k0257", future);
    // Register concurrent mutations ahead of the next page on Tokio's
    // fair write-lock queue, rather than relying on scheduling sleeps.
    let cache_gate = state.cache.write().await;
    let mut apply_winner = Box::pin(state.apply(&winner_op));
    let mut apply_future = Box::pin(state.apply(&future_op));
    assert!(futures::poll!(apply_winner.as_mut()).is_pending());
    assert!(futures::poll!(apply_future.as_mut()).is_pending());
    let mut next_page = Box::pin(state.expire_values_page(&mut pass));
    assert!(futures::poll!(next_page.as_mut()).is_pending());
    drop(cache_gate);
    assert_eq!(apply_winner.await.unwrap(), ApplyOutcome::Applied);
    assert_eq!(apply_future.await.unwrap(), ApplyOutcome::Applied);
    let before_second = state.revision.load(Ordering::Acquire);
    let second = next_page.await.unwrap();
    assert_eq!((second.visited, second.converted), (256, 254));
    assert_eq!(state.revision.load(Ordering::Acquire), before_second + 254);
    assert_eq!(state.cache.read().await.values.get("k0256"), Some(&winner));
    assert!(!state.cache.read().await.values["k0257"].is_tombstone);
    let summary = state
        .summary_page(Some("k0255"), Some(first_revision), Some(100), 10)
        .await;
    assert!(summary.restart);
    assert!(state.expire_values_page(&mut pass).await.unwrap().done);
}

#[tokio::test]
async fn expiry_pages_initial_quota_and_high_water_make_insert_churn_finite() {
    let values = (0..300)
        .map(|i| (format!("k{i:04}"), expiring_value(vec![1], 100)))
        .collect();
    let state = test_state(values);
    let mut pass = ExpiryPass::new(100);
    assert_eq!(
        state.expire_values_page(&mut pass).await.unwrap().visited,
        256
    );
    // Use current live future-TTL values to avoid apply's immediate expiry
    // materialization, then move their deadline under the shared cache lock.
    for key in ["a-behind", "k0255-new", "z-beyond"] {
        state
            .apply(&test_op(key, expiring_value(vec![2], u64::MAX)))
            .await
            .unwrap();
        state
            .cache
            .write()
            .await
            .values
            .get_mut(key)
            .unwrap()
            .expires_at_ms = Some(100);
    }
    let page = state.expire_values_page(&mut pass).await.unwrap();
    assert_eq!((page.visited, page.converted, page.done), (44, 44, true));
    let cache = state.cache.read().await;
    assert!(cache.values["k0255-new"].is_tombstone);
    assert!(!cache.values["a-behind"].is_tombstone);
    assert!(!cache.values["z-beyond"].is_tombstone);
    assert!(
        !cache.values["k0299"].is_tombstone,
        "insert consumed one original visit"
    );
    drop(cache);
    // Public full sweep remains authoritative and returns its full count.
    assert_eq!(state.expire_values(100).await.unwrap(), 3);
}

#[tokio::test]
async fn expiry_pages_failed_flush_retains_marker_and_newer_durable_winner() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(1),
        written: StdMutex::new(Vec::new()),
    });
    let value = expiring_value(vec![1], 100);
    let state = FragmentState::new(
        HashMap::from([("key".into(), value.clone())]),
        Some(store.clone()),
        1,
        Duration::from_secs(3600),
    );
    let mut pass = ExpiryPass::new(100);
    assert!(state
        .expire_values_page(&mut pass)
        .await
        .unwrap_err()
        .to_string()
        .contains("injected failure"));
    assert_eq!(pass.after_key.as_deref(), Some("key"));
    assert_eq!(
        state.cache.read().await.values["key"],
        value.expiry_tombstone()
    );
    assert_eq!(state.revision.load(Ordering::Acquire), 1);
    assert!(state.flush.lock().await.in_flight.is_some());
    let winner = VersionedValue::new(vec![2], 11, "writer".into());
    state.apply(&test_op("key", winner.clone())).await.unwrap();
    assert_eq!(
        store.written.lock().unwrap().as_slice(),
        &[(b"key".to_vec(), winner)]
    );
    assert!(state.expire_values_page(&mut pass).await.unwrap().done);
    state.flush().await.unwrap();
    assert_eq!(store.written.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn expiry_pages_cancelled_flush_wait_retains_pending_and_allows_safe_rescan() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(0),
        written: StdMutex::new(Vec::new()),
    });
    let value = expiring_value(vec![1], 100);
    let state = Arc::new(FragmentState::new(
        HashMap::from([("key".into(), value)]),
        Some(store.clone()),
        1,
        Duration::from_secs(3600),
    ));
    let flush_fence = state.flush_in_progress.lock().await;
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        task_state
            .expire_values_page(&mut ExpiryPass::new(100))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.revision.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.flush.lock().await.pending.len(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(flush_fence);
    let page = state
        .expire_values_page(&mut ExpiryPass::new(100))
        .await
        .unwrap();
    assert_eq!((page.visited, page.converted, page.done), (1, 0, true));
    let winner = VersionedValue::new(vec![2], 11, "writer".into());
    state.apply(&test_op("key", winner.clone())).await.unwrap();
    assert_eq!(
        store.written.lock().unwrap().as_slice(),
        &[(b"key".to_vec(), winner)]
    );
}

#[tokio::test]
async fn stable_flush_maps_store_guarantees_after_flushing_pending_writes() {
    for (reported, expected) in [
        (
            StoreCommitDurability::Buffered,
            ApplyDurability::StoreCommitted,
        ),
        (
            StoreCommitDurability::Stable,
            ApplyDurability::StableCommitted,
        ),
    ] {
        let store = Arc::new(StableFenceStore::new(reported));
        let state = FragmentState::new(
            HashMap::new(),
            Some(store.clone()),
            100,
            Duration::from_secs(3600),
        );
        state
            .apply(&test_op(
                "stable",
                VersionedValue::new(b"value".to_vec(), 1, "node".to_string()),
            ))
            .await
            .unwrap();

        assert_eq!(state.flush_stable().await.unwrap(), expected);
        assert_eq!(*store.events.lock().unwrap(), vec!["write", "fence"]);
    }

    let storeless = test_state(HashMap::new());
    assert_eq!(
        storeless.flush_stable().await.unwrap(),
        ApplyDurability::MemoryApplied
    );
}

#[tokio::test]
async fn summary_pages_are_ordered_exclusive_and_revision_guarded() {
    let state = test_state(HashMap::from([
        (
            "zulu".to_string(),
            VersionedValue::new(vec![3], 30, "n1".to_string()),
        ),
        (
            "alpha".to_string(),
            VersionedValue::new(vec![1], 10, "n1".to_string()),
        ),
        (
            "charlie".to_string(),
            VersionedValue::new(vec![2], 20, "n1".to_string()),
        ),
    ]));

    let first = state.summary_page(None, None, Some(123), 2).await;
    assert_eq!(first.revision, 0);
    assert_eq!(first.snapshot_time_ms, 123);
    assert!(!first.done);
    assert!(!first.restart);
    assert_eq!(
        first
            .summaries
            .iter()
            .map(|summary| summary.key.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "charlie"]
    );

    let second = state
        .summary_page(
            Some("charlie"),
            Some(first.revision),
            Some(first.snapshot_time_ms),
            2,
        )
        .await;
    assert!(second.done);
    assert!(!second.restart);
    assert_eq!(
        second
            .summaries
            .iter()
            .map(|summary| summary.key.as_str())
            .collect::<Vec<_>>(),
        vec!["zulu"]
    );

    state
        .apply(&test_op(
            "bravo",
            VersionedValue::new(vec![4], 40, "n1".to_string()),
        ))
        .await
        .unwrap();
    let stale = state
        .summary_page(
            Some("charlie"),
            Some(first.revision),
            Some(first.snapshot_time_ms),
            2,
        )
        .await;
    assert!(stale.restart);
    assert!(!stale.done);
    assert!(stale.summaries.is_empty());
    assert_eq!(stale.revision, 1);

    let restarted = state.summary_page(None, None, Some(123), 10).await;
    assert_eq!(
        restarted
            .summaries
            .iter()
            .map(|summary| summary.key.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "bravo", "charlie", "zulu"]
    );
    assert!(restarted.done);
}

#[tokio::test]
async fn summary_cursor_can_advance_past_an_empty_key() {
    let state = test_state(HashMap::from([
        (
            String::new(),
            VersionedValue::new(vec![0], 1, "n1".to_string()),
        ),
        (
            "next".to_string(),
            VersionedValue::new(vec![1], 2, "n1".to_string()),
        ),
    ]));

    let first = state.summary_page(None, None, Some(123), 1).await;
    assert_eq!(first.summaries[0].key, "");
    assert!(!first.done);
    let second = state
        .summary_page(
            Some(""),
            Some(first.revision),
            Some(first.snapshot_time_ms),
            1,
        )
        .await;
    assert_eq!(second.summaries[0].key, "next");
    assert!(second.done);
}

#[tokio::test]
async fn summary_pages_clamp_zero_limit_and_bound_cloned_metadata() {
    let large_suffix = "k".repeat(SUMMARY_PAGE_TARGET_BYTES / 2);
    let alpha = format!("alpha-{large_suffix}");
    let bravo = format!("bravo-{large_suffix}");
    let state = test_state(HashMap::from([
        (
            alpha.clone(),
            VersionedValue::new(Vec::new(), 1, "n".to_string()),
        ),
        (
            bravo.clone(),
            VersionedValue::new(Vec::new(), 2, "n".to_string()),
        ),
    ]));

    let first = state.summary_page(None, None, Some(123), 0).await;
    assert_eq!(first.summaries.len(), 1);
    assert_eq!(first.summaries[0].key, alpha);
    assert!(!first.done);

    let second = state
        .summary_page(
            Some(&alpha),
            Some(first.revision),
            Some(first.snapshot_time_ms),
            usize::MAX,
        )
        .await;
    assert_eq!(second.summaries.len(), 1);
    assert_eq!(second.summaries[0].key, bravo);
    assert!(second.done);
}

#[tokio::test]
async fn summary_and_get_versioned_share_pinned_expiry_semantics() {
    let mut expiring = VersionedValue::new(b"value".to_vec(), 50, "n1".to_string());
    expiring.expires_at_ms = Some(100);
    let tombstone = VersionedValue::tombstone(60, "n2".to_string());
    let state = test_state(HashMap::from([
        ("expiring".to_string(), expiring.clone()),
        ("deleted".to_string(), tombstone.clone()),
    ]));

    let before = state.summary_page(None, None, Some(99), 10).await;
    assert!(
        !before
            .summaries
            .iter()
            .find(|summary| summary.key == "expiring")
            .unwrap()
            .is_tombstone
    );
    let at_expiry = state.summary_page(None, None, Some(100), 10).await;
    assert!(
        at_expiry
            .summaries
            .iter()
            .find(|summary| summary.key == "expiring")
            .unwrap()
            .is_tombstone
    );
    assert!(
        at_expiry
            .summaries
            .iter()
            .find(|summary| summary.key == "deleted")
            .unwrap()
            .is_tombstone
    );

    assert_eq!(
        state.get_versioned("expiring", Some(99)).await,
        Some(expiring.clone())
    );
    assert_eq!(
        state.get_versioned("deleted", Some(99)).await,
        Some(tombstone)
    );
    assert_eq!(state.get_versioned("missing", Some(99)).await, None);
    assert_eq!(
        state.get_versioned("expiring", Some(100)).await,
        Some(expiring.expiry_tombstone())
    );
}

#[tokio::test]
async fn versioned_pages_bound_value_clones_and_advance_over_missing_keys() {
    let state = test_state(HashMap::from([
        (
            "a".to_string(),
            VersionedValue::new(vec![1; 600 * 1024], 1, "n".to_string()),
        ),
        (
            "b".to_string(),
            VersionedValue::new(vec![2; 600 * 1024], 2, "n".to_string()),
        ),
    ]));
    let keys = vec!["missing".to_string(), "a".to_string(), "b".to_string()];

    let first = state.get_versioned_page(&keys, 0, Some(u64::MAX)).await;
    assert!(first.missing);
    assert_eq!(first.next_index, 2);
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].0, "a");

    let second = state
        .get_versioned_page(&keys, first.next_index, Some(u64::MAX))
        .await;
    assert!(!second.missing);
    assert_eq!(second.next_index, keys.len());
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].0, "b");
}
