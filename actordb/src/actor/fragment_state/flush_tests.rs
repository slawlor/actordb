// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Ordered flush retention and final expiry-fence regressions.

use super::*;
use crate::actor::{KvActor, KvArgs, KvMessage, OpPriority, PoolConfig};
use crate::shard::FragmentId;
use crate::store::{KvStore, StoreError};
use ractor::{rpc::CallResult, Actor};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::Duration;
fn op(key: &str, value: VersionedValue) -> ReplicatedOp {
    ReplicatedOp {
        key: key.to_string(),
        value,
        seq: 0,
        boot_id: 0,
    }
}

/// A store that fails the first `failures` writes, then succeeds,
/// recording everything it receives. Regression test for
/// adversarial review #7 (failed flush must not discard data).
pub(super) struct FlakyStore {
    pub(super) failures_left: StdMutex<usize>,
    pub(super) written: StdMutex<Vec<(Vec<u8>, VersionedValue)>>,
}

impl KvStore for FlakyStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        {
            let mut left = self.failures_left.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                return Err(StoreError("injected failure".to_string()));
            }
        }
        self.written
            .lock()
            .unwrap()
            .extend(writes.iter().map(|(k, v)| (k.clone(), v.clone())));
        Ok(())
    }
}

#[tokio::test]
async fn standalone_dispatcher_drain_flushes_buffered_writes() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(0),
        written: StdMutex::new(Vec::new()),
    });
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        Some(store.clone() as Arc<dyn KvStore>),
        usize::MAX,
        Duration::from_secs(3600),
    ));
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: "standalone-drain-flush".to_string(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let value = VersionedValue::new(b"value".to_vec(), 100, "node".to_string());
    let result = dispatcher
        .call(
            |reply| KvMessage::ApplyOp {
                op: op("key", value.clone()),
                priority: OpPriority::Default,
                is_replication: false,
                reply: Some(reply),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, CallResult::Success(Ok(ApplyOutcome::Applied)));
    assert!(store.written.lock().unwrap().is_empty());

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
    assert_eq!(
        store.written.lock().unwrap().as_slice(),
        &[(b"key".to_vec(), value)]
    );
}

#[tokio::test]
async fn failed_flush_retains_batch_for_retry() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(1), // first write fails
        written: StdMutex::new(Vec::new()),
    });
    let state = FragmentState::new(
        HashMap::new(),
        Some(store.clone() as Arc<dyn KvStore>),
        2, // flush every 2 buffered ops
        Duration::from_secs(3600),
    );

    let v1 = VersionedValue::new(b"v1".to_vec(), 100, "n1".to_string());
    let v2 = VersionedValue::new(b"v2".to_vec(), 101, "n1".to_string());
    state.apply(&op("k1", v1)).await.unwrap();
    // Second buffered op triggers the flush, which fails.
    let err = state.apply(&op("k2", v2)).await.unwrap_err();
    assert!(
        err.to_string().contains("injected failure"),
        "expected injected failure, got {err}"
    );
    // Nothing reached the store.
    assert!(store.written.lock().unwrap().is_empty());

    // Retry: the retained batch must flush successfully now.
    state.flush().await.unwrap();
    let written_count = store.written.lock().unwrap().len();
    assert_eq!(written_count, 2, "retained batch must be retried");
    {
        let written = store.written.lock().unwrap();
        assert!(written.iter().any(|(k, _)| k == b"k1"));
        assert!(written.iter().any(|(k, _)| k == b"k2"));
    }

    // And the buffer is drained — a further flush is a no-op.
    state.flush().await.unwrap();
    assert_eq!(store.written.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn flush_coalesces_each_key_to_its_lww_winner() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(0),
        written: StdMutex::new(Vec::new()),
    });
    let state = FragmentState::new(
        HashMap::new(),
        Some(store.clone() as Arc<dyn KvStore>),
        100,
        Duration::from_secs(3600),
    );

    let old = VersionedValue::new(b"old".to_vec(), 100, "n1".to_string());
    let winner = VersionedValue::new(b"winner".to_vec(), 300, "n1".to_string());
    let stale = VersionedValue::new(b"stale".to_vec(), 200, "n1".to_string());
    assert_eq!(
        state.apply(&op("key", old)).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(&op("key", winner.clone())).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        state.apply(&op("key", stale)).await.unwrap(),
        ApplyOutcome::Superseded { .. }
    ));

    state.flush().await.unwrap();
    let written = store.written.lock().unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0], (b"key".to_vec(), winner));
}

#[tokio::test]
async fn apply_reports_exact_lww_outcome() {
    let state = FragmentState::new(HashMap::new(), None, 100, Duration::from_secs(3600));
    let winner = VersionedValue::new(b"winner".to_vec(), 300, "n1".to_string());
    assert_eq!(
        state.apply(&op("key", winner.clone())).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(&op("key", winner.clone())).await.unwrap(),
        ApplyOutcome::AlreadyPresent
    );

    let stale = VersionedValue::new(b"stale".to_vec(), 200, "n1".to_string());
    assert_eq!(
        state.apply(&op("key", stale)).await.unwrap(),
        ApplyOutcome::Superseded {
            timestamp_ms: winner.timestamp_ms,
            node_id: winner.node_id,
            is_tombstone: winner.is_tombstone,
        }
    );

    let mut expired = VersionedValue::new(b"expired".to_vec(), 400, "n1".to_string());
    expired.expires_at_ms = Some(1);
    assert_eq!(
        state.apply(&op("expired", expired.clone())).await.unwrap(),
        ApplyOutcome::Expired
    );
    let marker = state.get_all().await.remove("expired").unwrap();
    assert!(marker.is_tombstone);
    assert_eq!(marker.timestamp_ms, expired.timestamp_ms);
    assert_eq!(marker.node_id, expired.node_id);
    assert_eq!(marker.expires_at_ms, expired.expires_at_ms);
}

#[tokio::test]
async fn cleaner_persists_expiry_marker_and_preserves_version_floor() {
    let store = Arc::new(FlakyStore {
        failures_left: StdMutex::new(0),
        written: StdMutex::new(Vec::new()),
    });
    let mut expired = VersionedValue::new(b"ttl".to_vec(), 200, "n1".to_string());
    expired.expires_at_ms = Some(300);
    let state = FragmentState::new(
        HashMap::from([("key".to_string(), expired.clone())]),
        Some(store.clone() as Arc<dyn KvStore>),
        1,
        Duration::from_secs(3600),
    );

    assert_eq!(state.expire_values(300).await.unwrap(), 1);
    let marker = expired.expiry_tombstone();
    assert_eq!(
        store.written.lock().unwrap().as_slice(),
        &[(b"key".to_vec(), marker.clone())]
    );
    assert_eq!(state.get_all().await.get("key"), Some(&marker));

    let older = VersionedValue::new(b"old".to_vec(), 199, "n9".to_string());
    assert!(matches!(
        state.apply(&op("key", older)).await.unwrap(),
        ApplyOutcome::Superseded { .. }
    ));
    let newer = VersionedValue::new(b"new".to_vec(), 201, "n1".to_string());
    assert_eq!(
        state.apply(&op("key", newer.clone())).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(state.get("key").await, Some(newer));
}

#[tokio::test]
async fn cleaner_sweeps_retain_existing_and_new_tombstones() {
    let existing = VersionedValue::tombstone(100, "n1".to_string());
    let mut expired = VersionedValue::new(b"ttl".to_vec(), 200, "n1".to_string());
    expired.expires_at_ms = Some(300);
    let state = FragmentState::new(
        HashMap::from([
            ("deleted".to_string(), existing.clone()),
            ("expired".to_string(), expired.clone()),
        ]),
        None,
        100,
        Duration::from_secs(3600),
    );

    assert_eq!(state.expire_values(300).await.unwrap(), 1);
    assert_eq!(state.expire_values(u64::MAX).await.unwrap(), 0);
    assert_eq!(state.gc_tombstones(u64::MAX, 0).await, 0);

    let values = state.get_all().await;
    assert_eq!(values.get("deleted"), Some(&existing));
    assert_eq!(values.get("expired"), Some(&expired.expiry_tombstone()));
}

/// Store whose first write waits until the test releases it. A second
/// write, if allowed to overlap, completes first and exposes durable
/// version regression deterministically.
struct GatedStore {
    calls: AtomicUsize,
    release_first: (StdMutex<bool>, Condvar),
    stored: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl GatedStore {
    fn release_first(&self) {
        let (released, wake) = &self.release_first;
        *released.lock().unwrap() = true;
        wake.notify_all();
    }
}

impl KvStore for GatedStore {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        if call == 0 {
            let (released, wake) = &self.release_first;
            let mut released = released.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }

        let mut stored = self.stored.lock().unwrap();
        for (key, value) in writes {
            stored.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_flushes_are_serialized_and_preserve_latest_version() {
    let store = Arc::new(GatedStore {
        calls: AtomicUsize::new(0),
        release_first: (StdMutex::new(false), Condvar::new()),
        stored: StdMutex::new(HashMap::new()),
    });
    let state = Arc::new(FragmentState::new(
        HashMap::new(),
        Some(store.clone() as Arc<dyn KvStore>),
        100,
        Duration::from_secs(3600),
    ));

    let old = VersionedValue::new(b"old".to_vec(), 100, "n1".to_string());
    let new = VersionedValue::new(b"new".to_vec(), 200, "n1".to_string());
    state.apply(&op("key", old)).await.unwrap();

    let first_state = state.clone();
    let first = tokio::spawn(async move { first_state.flush().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.calls.load(AtomicOrdering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first flush did not reach the store");

    // The newer version is applied while the old batch is blocked in the
    // store. A second flush must wait rather than commit ahead of it.
    state.apply(&op("key", new.clone())).await.unwrap();
    let second_state = state.clone();
    let second = tokio::spawn(async move { second_state.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        store.calls.load(AtomicOrdering::SeqCst),
        1,
        "a second store write overlapped the first"
    );

    store.release_first();
    tokio::time::timeout(Duration::from_secs(2), async {
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    })
    .await
    .expect("serialized flushes did not complete");

    assert_eq!(
        store.stored.lock().unwrap().get(b"key".as_slice()),
        Some(&new),
        "durable state regressed behind the in-memory LWW winner"
    );
}
