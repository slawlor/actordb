//! Bounded metadata pages and exact cache/revision publication regressions.

use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Condvar, Mutex as StdMutex};
use tokio::sync::Notify;

pub(super) struct SnapshotPause {
    pub(super) captured: Notify,
    pub(super) resume: Notify,
}

// Thread-local measurement excludes unrelated concurrently running tests.
// Count requested bytes, not allocator slack, live heap, or process RSS.
thread_local! {
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}
struct Allocator;
fn charge(bytes: usize) {
    let _ = ALLOCATED.try_with(|value| {
        if let Some(previous) = value.get() {
            value.set(Some(previous.saturating_add(bytes)));
        }
    });
}
// Test instrumentation delegates every allocator operation to System with
// unchanged pointer/layout contracts; it does not change production allocation.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            charge(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, bytes) };
        if !pointer.is_null() {
            charge(bytes);
        }
        pointer
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
struct AllocationScope(Option<usize>);
impl AllocationScope {
    fn start() -> Self {
        Self(ALLOCATED.with(|value| value.replace(Some(0))))
    }
    fn finish(mut self) -> usize {
        ALLOCATED.with(|value| {
            let bytes = value.get().unwrap();
            value.set(self.0.take());
            bytes
        })
    }
}
impl Drop for AllocationScope {
    fn drop(&mut self) {
        ALLOCATED.with(|value| value.set(self.0.take()));
    }
}

fn key(table: &str, literal: &str) -> String {
    crate::table_key::encode_table_key(table, literal)
}
fn value(version: u64) -> VersionedValue {
    VersionedValue::new(vec![version as u8], version, "writer".into())
}
fn op(key: String, value: VersionedValue) -> ReplicatedOp {
    ReplicatedOp {
        key,
        value,
        seq: 0,
        boot_id: 0,
    }
}
fn partitioned(cache: HashMap<String, VersionedValue>) -> Arc<FragmentState> {
    Arc::new(
        FragmentState::new(cache, None, usize::MAX, Duration::from_secs(3600))
            .enable_partitions()
            .unwrap(),
    )
}

#[tokio::test]
async fn partitioned_pages_seek_physical_order_and_fence_every_winning_table_mutation() {
    let mut keys = vec![String::new(), "plain".into()];
    for table in ["A", "AB", "A0", "KV", "Z"] {
        keys.extend([key(table, ""), key(table, "Unicode-雪\0")]);
    }
    let state = partitioned(keys.iter().map(|key| (key.clone(), value(1))).collect());
    // Table-name order is not physical-key order: the delimiter follows a
    // shorter hex table name after the next table's hexadecimal byte.
    assert!("A" < "AB");
    assert!(key("A", "") > key("AB", ""));
    keys.sort();
    let first = state.summary_page(None, None, Some(100), 2).await;
    let mut actual = first
        .summaries
        .iter()
        .map(|row| row.key.clone())
        .collect::<Vec<_>>();
    let mut last = actual.last().cloned();
    let mut done = first.done;
    while !done {
        let page = state
            .summary_page(last.as_deref(), Some(first.revision), Some(100), 2)
            .await;
        assert!(!page.restart);
        assert_eq!(page.revision, first.revision);
        actual.extend(page.summaries.into_iter().map(|row| row.key));
        last = actual.last().cloned();
        done = page.done;
    }
    assert_eq!(actual, keys);
    let changed = key("AB", "new");
    assert_eq!(
        state.apply(&op(changed.clone(), value(2))).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(state.current_revision().await, first.revision + 1);
    let restart = state
        .summary_page(last.as_deref(), Some(first.revision), Some(100), 2)
        .await;
    assert!(restart.restart);
    assert!(restart.summaries.is_empty());
    assert_eq!(
        state.apply(&op(changed.clone(), value(2))).await.unwrap(),
        ApplyOutcome::AlreadyPresent
    );
    assert!(matches!(
        state.apply(&op(changed, value(1))).await.unwrap(),
        ApplyOutcome::Superseded { .. }
    ));
    assert_eq!(state.current_revision().await, first.revision + 1);
    let snapshot = state.snapshot_with_revision().await;
    let page = state
        .summary_page(None, Some(snapshot.1), None, usize::MAX)
        .await;
    assert!(page.done && !page.restart);
    for row in page.summaries {
        assert_eq!(row.content_digest, snapshot.0[&row.key].content_digest());
    }
}

#[tokio::test]
async fn summary_and_fetch_share_pinned_ttl_before_after_deadline_and_cleanup() {
    let a = key("A", "a");
    let b = key("A", "b");
    let mut expiring = value(50);
    expiring.expires_at_ms = Some(100);
    let state = partitioned(HashMap::from([
        (a.clone(), value(1)),
        (b.clone(), expiring),
    ]));
    let first = state.summary_page(None, None, Some(99), 1).await;
    let continuation = state
        .summary_page(Some(&a), Some(first.revision), Some(99), 1)
        .await;
    assert!(!continuation.restart);
    for time in [99, 100, 101] {
        let page = state
            .summary_page(Some(&a), Some(first.revision), Some(time), 1)
            .await;
        let fetched = state.get_versioned(&b, Some(time)).await.unwrap();
        assert_eq!(page.summaries[0].is_tombstone, fetched.is_tombstone);
        assert_eq!(page.summaries[0].content_digest, fetched.content_digest());
    }
    assert!(!continuation.summaries[0].is_tombstone);
    assert_eq!(state.expire_values(100).await.unwrap(), 1);
    assert_eq!(state.current_revision().await, first.revision + 1);
    assert!(
        state
            .summary_page(Some(&a), Some(first.revision), Some(99), 1)
            .await
            .restart
    );
    // Cleaner materialization discards the original payload, so both APIs now
    // return the same retained tombstone even for a formerly-live pinned time.
    let page = state.summary_page(Some(&a), None, Some(99), 1).await;
    let fetched = state.get_versioned(&b, Some(99)).await.unwrap();
    assert!(fetched.is_tombstone);
    assert_eq!(page.summaries[0].content_digest, fetched.content_digest());
}

#[tokio::test]
async fn continuation_crossing_wall_deadline_without_cleaner_stays_at_its_pinned_time() {
    let a = key("KV", "a");
    let b = key("KV", "b");
    let mut expiring = value(1);
    let deadline = now_ms() + 40;
    expiring.expires_at_ms = Some(deadline);
    let state = partitioned(HashMap::from([
        (a.clone(), value(1)),
        (b.clone(), expiring),
    ]));
    let first = state.summary_page(None, None, Some(deadline - 1), 1).await;
    while now_ms() <= deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let next = state
        .summary_page(
            Some(&a),
            Some(first.revision),
            Some(first.snapshot_time_ms),
            1,
        )
        .await;
    let fetched = state
        .get_versioned(&b, Some(first.snapshot_time_ms))
        .await
        .unwrap();
    assert!(!next.restart && !next.summaries[0].is_tombstone);
    assert_eq!(next.summaries[0].content_digest, fetched.content_digest());
}

#[tokio::test]
async fn summaries_and_revision_do_not_wait_for_any_child_cache_or_copy_payloads() {
    for (records, value_bytes, many_tables) in [
        (512, 128 * 1024, false),
        (512, 128 * 1024, true),
        (4096, 8, true),
    ] {
        let state = partitioned(
            (0..records)
                .map(|index| {
                    (
                        if many_tables {
                            key(&format!("T{index:04}"), "key")
                        } else {
                            key("KV", &format!("key-{index:06}"))
                        },
                        VersionedValue::new(vec![7; value_bytes], 1, "writer".into()),
                    )
                })
                .collect(),
        );
        let child = state
            .table_children
            .read()
            .await
            .values()
            .next()
            .unwrap()
            .clone();
        let _cache = child.cache.write().await;
        let scope = AllocationScope::start();
        let started = Instant::now();
        let page = state.summary_page(None, None, Some(1), 1).await;
        let allocated = scope.finish();
        assert_eq!(page.summaries.len(), 1);
        assert!(allocated < 32 * 1024, "one row allocated {allocated} bytes");
        eprintln!("bounded summary: records={records}, value_bytes={value_bytes}, many_tables={many_tables}, rows=1, allocation_requested_bytes={allocated}, elapsed={:?} (not RSS/SLO)", started.elapsed());
        let scope = AllocationScope::start();
        let mismatch = state.summary_page(None, Some(1), Some(1), usize::MAX).await;
        assert_eq!(state.current_revision().await, 0);
        let allocated = scope.finish();
        assert!(mismatch.restart && mismatch.summaries.is_empty());
        assert_eq!(
            allocated, 0,
            "revision mismatch visited data or allocated metadata"
        );
    }
}

#[tokio::test]
async fn direct_snapshot_values_and_revision_stay_atomic_across_paused_clone() {
    let state = Arc::new(FragmentState::new(
        HashMap::from([("key".into(), value(0))]),
        None,
        100,
        Duration::from_secs(60),
    ));
    let pause = Arc::new(SnapshotPause {
        captured: Notify::new(),
        resume: Notify::new(),
    });
    *state.snapshot_pause.lock().unwrap() = Some(pause.clone());
    let reading = state.clone();
    let snapshot = tokio::spawn(async move { reading.snapshot_with_revision().await });
    pause.captured.notified().await;
    let mutation = op("key".into(), value(1));
    let write = state.apply(&mutation);
    tokio::pin!(write);
    assert!(futures::poll!(&mut write).is_pending());
    assert_eq!(state.revision.load(Ordering::Acquire), 0);
    pause.resume.notify_one();
    let (values, revision) = snapshot.await.unwrap();
    assert_eq!(values["key"].timestamp_ms, revision);
    assert_eq!(revision, 0);
    write.await.unwrap();
    let (values, revision) = state.snapshot_with_revision().await;
    assert_eq!(values["key"].timestamp_ms, revision);
    assert_eq!(revision, 1);
}

#[tokio::test]
async fn full_partitioned_snapshot_fences_other_tables_but_summary_stays_nonblocking() {
    let a = key("A", "key");
    let b = key("B", "key");
    let state = partitioned(HashMap::from([
        (a.clone(), value(0)),
        (b.clone(), value(0)),
    ]));
    let b_child = state.table_children.read().await.get("B").unwrap().clone();
    let b_cache = b_child.cache.write().await;
    let reading = state.clone();
    let snapshot = tokio::spawn(async move { reading.snapshot_with_revision().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.snapshot_fence.try_read().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mutation = op(a.clone(), value(1));
    let write = state.apply(&mutation);
    tokio::pin!(write);
    assert!(futures::poll!(&mut write).is_pending());
    assert_eq!(state.current_revision().await, 0);
    assert_eq!(
        state
            .summary_page(None, None, Some(1), 2)
            .await
            .summaries
            .len(),
        2
    );
    drop(b_cache);
    let (values, revision) = snapshot.await.unwrap();
    assert_eq!(revision, 0);
    assert_eq!(values[&a].timestamp_ms, 0);
    assert_eq!(values[&b].timestamp_ms, 0);
    write.await.unwrap();
    let (values, revision) = state.snapshot_with_revision().await;
    assert_eq!(revision, 1);
    assert_eq!(values[&a].timestamp_ms, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_direct_and_cross_table_snapshots_pair_exact_mutation_counts() {
    for partitioned_mode in [false, true] {
        let keys = if partitioned_mode {
            vec![key("A", "key"), key("B", "key")]
        } else {
            vec!["key".into()]
        };
        let mut state = FragmentState::new(
            keys.iter().map(|key| (key.clone(), value(0))).collect(),
            None,
            100,
            Duration::from_secs(60),
        );
        if partitioned_mode {
            state = state.enable_partitions().unwrap();
        }
        let state = Arc::new(state);
        let mut writers = Vec::new();
        for key in keys {
            let writing = state.clone();
            writers.push(tokio::spawn(async move {
                for version in 1..=10_000 {
                    writing
                        .apply(&op(key.clone(), value(version)))
                        .await
                        .unwrap();
                    if version % 8 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        while writers.iter().any(|writer| !writer.is_finished()) {
            let (values, revision) = state.snapshot_with_revision().await;
            let versions: u64 = values.values().map(|value| value.timestamp_ms).sum();
            assert_eq!(versions, revision);
            tokio::task::yield_now().await;
        }
        for writer in writers {
            writer.await.unwrap();
        }
        let (values, revision) = state.snapshot_with_revision().await;
        assert_eq!(
            values.values().map(|value| value.timestamp_ms).sum::<u64>(),
            revision
        );
    }
}

#[tokio::test]
async fn standalone_preloaded_child_view_does_not_change_direct_base_revision() {
    let managed = key("KV", "managed");
    let state = FragmentState::new(
        HashMap::from([("plain".into(), value(0)), (managed.clone(), value(0))]),
        None,
        100,
        Duration::from_secs(60),
    );
    state
        .apply_from_actor(&op(managed, value(1)))
        .await
        .unwrap();
    assert_eq!(state.current_revision().await, 0);
    let snapshot = state.snapshot_with_revision().await;
    assert_eq!(snapshot.0.len(), 1);
    assert_eq!(snapshot.1, 0);
    assert_eq!(
        state
            .summary_page(None, None, Some(1), 100)
            .await
            .summaries
            .len(),
        1
    );
    state.apply(&op("plain".into(), value(1))).await.unwrap();
    assert_eq!(state.current_revision().await, 1);
}

struct GatedStore {
    entered: Notify,
    release: StdMutex<bool>,
    wake: Condvar,
    first: AtomicBool,
    calls: AtomicUsize,
}
impl GatedStore {
    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
struct Release(Arc<GatedStore>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl KvStore for GatedStore {
    fn bind(
        &self,
        _: &crate::store::StoreIdentity,
    ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
        Ok(crate::store::StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, crate::store::StoreError> {
        Ok(Vec::new())
    }
    fn load_prefix(
        &self,
        _: &[u8],
    ) -> Result<Vec<(Vec<u8>, VersionedValue)>, crate::store::StoreError> {
        Ok(Vec::new())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, crate::store::StoreError> {
        Ok(self.commit_durability())
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), crate::store::StoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.first.swap(false, Ordering::Relaxed) {
            self.entered.notify_one();
            let mut released = self.release.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
            return Err(crate::store::StoreError(
                "controlled first flush failure".into(),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn failed_or_cancelled_auto_flush_cannot_delay_or_undo_cache_revision_publication() {
    for cancel in [false, true] {
        let store = Arc::new(GatedStore {
            entered: Notify::new(),
            release: StdMutex::new(false),
            wake: Condvar::new(),
            first: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        let _release = Release(store.clone());
        let state = Arc::new(
            FragmentState::new(
                HashMap::new(),
                Some(store.clone()),
                1,
                Duration::from_secs(3600),
            )
            .enable_partitions()
            .unwrap(),
        );
        let writing = state.clone();
        let stored_key = key("A", "key");
        let mutation = op(stored_key.clone(), value(1));
        let apply = tokio::spawn(async move { writing.apply(&mutation).await });
        store.entered.notified().await;
        assert!(
            state.snapshot_fence.try_write().is_ok(),
            "backend I/O retained publication fence"
        );
        let page = state.summary_page(None, None, Some(1), 1).await;
        assert_eq!(page.revision, 1);
        assert_eq!(page.summaries.len(), 1);
        let snapshot = state.snapshot_with_revision().await;
        assert_eq!(snapshot.1, page.revision);
        assert_eq!(
            snapshot.0[&stored_key].content_digest(),
            page.summaries[0].content_digest
        );
        if cancel {
            apply.abort();
            assert!(apply.await.unwrap_err().is_cancelled());
        } else {
            store.release();
            assert!(matches!(apply.await.unwrap(), Err(ApplyError::Storage(_))));
        }
        store.release();
        state.flush().await.unwrap();
        assert_eq!(state.current_revision().await, 1);
        assert_eq!(state.summary_page(None, None, Some(1), 1).await.revision, 1);
        assert!(store.calls.load(Ordering::Relaxed) >= 2);
    }
}
