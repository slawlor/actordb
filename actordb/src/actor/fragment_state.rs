// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Shared cache, ordered persistence, expiry, and snapshot state.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::ActorProcessingErr;
use tokio::sync::{Mutex, RwLock};

use super::clock::now_ms;
use super::protocol::{
    ApplyDurability, ApplyError, ApplyOutcome, ReplicatedOp, SnapshotSummary, SummaryPage,
    VersionedPage, VersionedValue,
};
use crate::store::{KvStore, StoreCommitDurability};
use crate::wire::check_versioned_value_budget;

// Actor-owned summary clones stay bounded before wire metadata is added.
const SUMMARY_PAGE_TARGET_BYTES: usize = 1024 * 1024;
const VERSIONED_PAGE_TARGET_BYTES: usize = 1024 * 1024;
/// The fragment's shared state: one in-memory cache, one persistence
/// buffer, one [`KvStore`].
///
/// Workers, the dispatcher, and background maintenance share this cache.
/// The lock serializes winning merges with the ordered index and snapshot
/// revision; periodic expiry releases it between bounded pages.
///
/// Applied writes are buffered and flushed to the [`KvStore`]
/// in batches (see `NodeConfig` flush knobs); the cache is reloaded from
/// the store on boot, so a restarted node keeps its dataset. A crash
/// can lose the unflushed tail of the write buffer; immediate correlated
/// reconciliation on reconnect repairs it from peers.
pub struct FragmentState {
    /// In-memory cache of the fragment: the fast path for all reads.
    cache: RwLock<FragmentCache>,
    /// Optional durable layer behind the cache. `None` = no persistence.
    kvstore: Option<Arc<dyn KvStore>>,
    /// Advances for every winning cache mutation, including replicated
    /// writes and expiry conversion.
    revision: AtomicU64,
    /// Buffered writes awaiting flush.
    flush: Mutex<FlushBuffer>,
    /// Allows exactly one store write at a time for this fragment. The
    /// buffered batch remains recorded while the write is in progress, so a
    /// cancelled or failed flush can be retried without losing it.
    flush_in_progress: Mutex<()>,
    flush_batch_size: usize,
    flush_interval: Duration,
}

/// Values and their ordered key index. Both live under one lock so every
/// visible key is immediately available to ordered snapshot pagination.
struct FragmentCache {
    values: HashMap<String, VersionedValue>,
    ordered_keys: BTreeSet<String>,
}

pub(crate) const EXPIRY_PAGE_VISIT_LIMIT: usize = 256;
pub(crate) const EXPIRY_PAGE_WORK_BYTES: usize = 1024 * 1024;

/// A weakly consistent, finite periodic pass. The first page captures its
/// ordered high-water key and cardinality under the same cache write lock.
pub(crate) struct ExpiryPass {
    now_ms: u64,
    initialized: bool,
    remaining: usize,
    after_key: Option<String>,
    high_water: Option<String>,
}

impl ExpiryPass {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            initialized: false,
            remaining: 0,
            after_key: None,
            high_water: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExpiryPage {
    pub(crate) visited: usize,
    pub(crate) converted: usize,
    pub(crate) work_bytes: usize,
    pub(crate) done: bool,
}

fn expiry_record_work_bytes(
    key: &str,
    value: &VersionedValue,
    expired: bool,
    durable: bool,
) -> usize {
    // A conservative work charge, not a heap or wall-clock bound: inspected
    // metadata, cursor-key cloning, and expired payload/buffer destruction.
    let mut bytes = std::mem::size_of::<(String, VersionedValue)>()
        .saturating_add(key.len())
        .saturating_add(value.node_id.capacity());
    if expired {
        bytes = bytes
            .saturating_add(value.value.capacity())
            .saturating_add(value.node_id.len());
        if durable {
            bytes = bytes
                .saturating_add(std::mem::size_of::<(String, VersionedValue)>())
                .saturating_add(key.len())
                .saturating_add(value.node_id.len());
        }
    }
    bytes
}

struct FlushBuffer {
    pending: Vec<(String, VersionedValue)>,
    /// The batch currently being attempted. It stays here until the store
    /// write succeeds; retries may therefore write the same versions again.
    in_flight: Option<Vec<(String, VersionedValue)>>,
    last_flush: Instant,
}

impl FragmentState {
    pub fn new(
        cache: HashMap<String, VersionedValue>,
        kvstore: Option<Arc<dyn KvStore>>,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let ordered_keys = cache.keys().cloned().collect();
        Self {
            cache: RwLock::new(FragmentCache {
                values: cache,
                ordered_keys,
            }),
            kvstore,
            revision: AtomicU64::new(0),
            flush: Mutex::new(FlushBuffer {
                pending: Vec::new(),
                in_flight: None,
                last_flush: Instant::now(),
            }),
            flush_in_progress: Mutex::new(()),
            flush_batch_size,
            flush_interval,
        }
    }

    pub(super) fn durability(&self) -> ApplyDurability {
        if self.kvstore.is_some() {
            ApplyDurability::StoreCommitted
        } else {
            ApplyDurability::MemoryApplied
        }
    }

    /// Merge an op into the cache and enqueue the same winning version for
    /// persistence as one ordered operation.
    ///
    /// Returns the exact merge outcome. Ops that are already expired install
    /// their deterministic expiry tombstone instead of disappearing. When
    /// persistence is configured, taking the flush-buffer lock before the
    /// cache lock ensures that concurrent
    /// workers cannot enqueue versions in a different order from the cache
    /// merge. This matters across priority pools, where the same key can be
    /// handled by different workers.
    pub async fn apply(&self, op: &ReplicatedOp) -> Result<ApplyOutcome, ApplyError> {
        check_versioned_value_budget(&op.key, &op.value).map_err(|error| {
            ApplyError::InvalidInput(format!(
                "key/value cannot fit every required peer frame: {error}"
            ))
        })?;
        let expired = op.value.is_expired(now_ms());
        let expiry_tombstone;
        let value = if expired {
            expiry_tombstone = op.value.expiry_tombstone();
            &expiry_tombstone
        } else {
            &op.value
        };

        if self.kvstore.is_none() {
            let mut cache = self.cache.write().await;
            let outcome = Self::merge_cache(&mut cache, &op.key, value);
            if outcome == ApplyOutcome::Applied {
                self.revision.fetch_add(1, Ordering::Release);
            }
            return Ok(Self::reported_outcome(outcome, expired));
        }

        let (outcome, should_flush) = {
            // Keep the winning cache mutation and its durable enqueue
            // adjacent, with no await point between them. Flushes may take a
            // batch before or after this critical section, never halfway
            // through it.
            let mut flush = self.flush.lock().await;
            let mut cache = self.cache.write().await;
            let outcome = Self::merge_cache(&mut cache, &op.key, value);
            if outcome != ApplyOutcome::Applied {
                return Ok(Self::reported_outcome(outcome, expired));
            }
            flush.pending.push((op.key.clone(), value.clone()));
            self.revision.fetch_add(1, Ordering::Release);
            let should_flush = flush.pending.len() >= self.flush_batch_size
                || flush.last_flush.elapsed() >= self.flush_interval;
            (outcome, should_flush)
        };

        if should_flush {
            self.flush()
                .await
                .map_err(|error| ApplyError::Storage(error.to_string()))?;
        }
        Ok(Self::reported_outcome(outcome, expired))
    }

    fn merge_cache(cache: &mut FragmentCache, key: &str, value: &VersionedValue) -> ApplyOutcome {
        if let Some(existing) = cache.values.get_mut(key) {
            if existing == value {
                return ApplyOutcome::AlreadyPresent;
            }
            if !value.wins_over(existing) {
                return ApplyOutcome::Superseded {
                    timestamp_ms: existing.timestamp_ms,
                    node_id: existing.node_id.clone(),
                    is_tombstone: existing.is_tombstone,
                };
            }
            *existing = value.clone();
            ApplyOutcome::Applied
        } else {
            let key = key.to_string();
            cache.ordered_keys.insert(key.clone());
            cache.values.insert(key, value.clone());
            ApplyOutcome::Applied
        }
    }

    fn reported_outcome(outcome: ApplyOutcome, expired: bool) -> ApplyOutcome {
        if expired
            && matches!(
                &outcome,
                ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent
            )
        {
            ApplyOutcome::Expired
        } else {
            outcome
        }
    }

    /// Write the currently buffered writes to the store in one batch.
    ///
    /// At most one write is in progress per fragment. The authoritative copy
    /// of the batch remains in the buffer's in-flight slot until the write
    /// succeeds, so failure or cancellation cannot make a later flush forget
    /// it. Writes that arrive during the store call remain pending for the
    /// next flush.
    pub async fn flush(&self) -> Result<(), ActorProcessingErr> {
        let _single_flight = self.flush_in_progress.lock().await;
        let batch = {
            let mut flush = self.flush.lock().await;
            match flush.begin_batch() {
                Some(batch) => batch,
                None => return Ok(()),
            }
        };

        self.write_batch(&batch)?;
        self.flush.lock().await.commit_batch();
        Ok(())
    }

    /// Flush the fragment's ordered persistence buffer, then ask the backend
    /// for its strongest explicit stable-storage fence.
    ///
    /// Storeless fragments remain memory-only. A backend which reports a
    /// buffered fence preserves the ordinary [`ApplyDurability::StoreCommitted`]
    /// result; only an explicit stable fence yields
    /// [`ApplyDurability::StableCommitted`].
    pub async fn flush_stable(&self) -> Result<ApplyDurability, ActorProcessingErr> {
        self.flush().await?;
        let Some(kvstore) = &self.kvstore else {
            return Ok(ApplyDurability::MemoryApplied);
        };
        kvstore
            .stable_fence()
            .map(|durability| match durability {
                StoreCommitDurability::Buffered => ApplyDurability::StoreCommitted,
                StoreCommitDurability::Stable => ApplyDurability::StableCommitted,
            })
            .map_err(|error| ActorProcessingErr::from(error.to_string()))
    }

    fn write_batch(&self, batch: &[(String, VersionedValue)]) -> Result<(), ActorProcessingErr> {
        if let Some(kvstore) = &self.kvstore {
            // A busy key may appear several times in one batch. Send only
            // its LWW winner so backend implementations do not need to
            // define duplicate-key ordering within a batch.
            let mut coalesced: HashMap<Vec<u8>, VersionedValue> =
                HashMap::with_capacity(batch.len());
            for (key, value) in batch {
                let replace = match coalesced.get(key.as_bytes()) {
                    Some(existing) => value.wins_over(existing),
                    None => true,
                };
                if replace {
                    coalesced.insert(key.as_bytes().to_vec(), value.clone());
                }
            }
            let encoded: Vec<(Vec<u8>, VersionedValue)> = coalesced.into_iter().collect();
            kvstore
                .write_batch(&encoded)
                .map_err(|e| ActorProcessingErr::from(e.to_string()))?;
        }
        Ok(())
    }

    /// Read a key, treating expired values and tombstones as absent.
    pub async fn get(&self, key: &str) -> Option<VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .values
            .get(key)
            .filter(|v| !v.is_tombstone && !v.is_expired(now))
            .cloned()
    }

    /// Read the full cache, including tombstones. Expired live values are
    /// synthesized as deterministic expiry tombstones even if the cleaner has
    /// not persisted their conversion yet, so synchronization never erases a
    /// version floor. Callers serving user reads should filter tombstones out.
    pub async fn get_all(&self) -> HashMap<String, VersionedValue> {
        self.snapshot_with_revision().await.0
    }

    /// Read a cache snapshot and its mutation revision atomically with
    /// respect to winning cache updates.
    pub async fn snapshot_with_revision(&self) -> (HashMap<String, VersionedValue>, u64) {
        let now = now_ms();
        let cache = self.cache.read().await;
        let snapshot = cache
            .values
            .iter()
            .map(|(key, value)| {
                let value = if value.is_expired(now) {
                    value.expiry_tombstone()
                } else {
                    value.clone()
                };
                (key.clone(), value)
            })
            .collect();
        let revision = self.revision.load(Ordering::Acquire);
        (snapshot, revision)
    }

    /// Read one lexicographically ordered page of version summaries.
    ///
    /// A caller starts with no expected revision or pinned time, then feeds
    /// both returned values into later pages. If a winning mutation occurred,
    /// the revision mismatch returns an empty restart page instead of mixing
    /// two logical snapshots.
    pub async fn summary_page(
        &self,
        after_key: Option<&str>,
        expected_revision: Option<u64>,
        snapshot_time_ms: Option<u64>,
        limit: usize,
    ) -> SummaryPage {
        use std::ops::Bound::{Excluded, Unbounded};

        let cache = self.cache.read().await;
        let revision = self.revision.load(Ordering::Acquire);
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        if expected_revision.is_some_and(|expected| expected != revision) {
            return SummaryPage {
                summaries: Vec::new(),
                revision,
                snapshot_time_ms,
                done: false,
                restart: true,
            };
        }

        let lower_bound = after_key
            .map(|key| Excluded(key.to_string()))
            .unwrap_or(Unbounded);
        let limit = limit.max(1);
        let keys = cache.ordered_keys.range((lower_bound, Unbounded));
        // Do not reserve from the fragment's total cardinality: callers may
        // use a very large count limit while the byte target still ends the
        // page after only a handful of metadata-heavy records.
        let mut summaries = Vec::with_capacity(limit.min(256));
        let mut payload_bytes = 0usize;
        let mut done = true;
        for key in keys {
            if summaries.len() == limit {
                done = false;
                break;
            }
            if let Some(value) = cache.values.get(key) {
                let summary_bytes = key.len().saturating_add(value.node_id.len());
                if !summaries.is_empty()
                    && payload_bytes.saturating_add(summary_bytes) > SUMMARY_PAGE_TARGET_BYTES
                {
                    done = false;
                    break;
                }
                summaries.push(Self::snapshot_summary(key, value, snapshot_time_ms));
                payload_bytes = payload_bytes.saturating_add(summary_bytes);
            }
        }

        SummaryPage {
            summaries,
            revision,
            snapshot_time_ms,
            done,
            restart: false,
        }
    }

    /// Read one value including tombstones at a pinned expiry time.
    pub async fn get_versioned(
        &self,
        key: &str,
        snapshot_time_ms: Option<u64>,
    ) -> Option<VersionedValue> {
        let cache = self.cache.read().await;
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        cache.values.get(key).map(|value| {
            if value.is_expired(snapshot_time_ms) {
                value.expiry_tombstone()
            } else {
                value.clone()
            }
        })
    }

    /// Read exact values in input order without cloning an arbitrary fetch
    /// chunk at once. Pages target 1 MiB of key/value metadata; one individually
    /// valid value may exceed that soft target but is returned alone.
    pub async fn get_versioned_page(
        &self,
        keys: &[String],
        start_index: usize,
        snapshot_time_ms: Option<u64>,
    ) -> VersionedPage {
        let cache = self.cache.read().await;
        let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
        let mut next_index = start_index.min(keys.len());
        let mut payload_bytes = 0usize;
        let mut entries = Vec::new();
        let mut missing = false;
        while let Some(key) = keys.get(next_index) {
            let Some(value) = cache.values.get(key) else {
                missing = true;
                next_index += 1;
                continue;
            };
            let value = if value.is_expired(snapshot_time_ms) {
                value.expiry_tombstone()
            } else {
                value.clone()
            };
            let entry_bytes = key
                .len()
                .saturating_add(value.value.len())
                .saturating_add(value.node_id.len());
            if !entries.is_empty()
                && payload_bytes.saturating_add(entry_bytes) > VERSIONED_PAGE_TARGET_BYTES
            {
                break;
            }
            entries.push((key.clone(), value));
            payload_bytes = payload_bytes.saturating_add(entry_bytes);
            next_index += 1;
            if payload_bytes > VERSIONED_PAGE_TARGET_BYTES {
                break;
            }
        }
        VersionedPage {
            entries,
            next_index,
            missing,
        }
    }

    fn snapshot_summary(
        key: &str,
        value: &VersionedValue,
        snapshot_time_ms: u64,
    ) -> SnapshotSummary {
        SnapshotSummary {
            key: key.to_string(),
            timestamp_ms: value.timestamp_ms,
            node_id: value.node_id.clone(),
            is_tombstone: value.is_tombstone || value.is_expired(snapshot_time_ms),
        }
    }

    /// Read the mutation revision while holding the cache read lock.
    ///
    /// Writers advance the revision while holding the cache write lock, so
    /// taking the corresponding read lock makes this a fence against an
    /// in-progress winning mutation rather than a merely atomic observation.
    pub async fn current_revision(&self) -> u64 {
        let _cache = self.cache.read().await;
        self.revision.load(Ordering::Acquire)
    }

    /// Legacy compatibility shim for the removed time-based collector.
    ///
    /// This method intentionally retains every tombstone and returns zero.
    /// Elapsed time cannot prove that all replicas have durably observed a
    /// delete, so age-based removal can resurrect stale values.
    pub async fn gc_tombstones(&self, _now_ms: u64, _gc_age_ms: u64) -> usize {
        0
    }

    /// Convert all expired live values to durable expiry tombstones. Returns
    /// the number converted. Cache replacement and persistence enqueueing use
    /// the same lock order as normal writes, preserving their shared order.
    pub async fn expire_values(&self, now_ms: u64) -> Result<usize, ActorProcessingErr> {
        if self.kvstore.is_none() {
            let mut cache = self.cache.write().await;
            let mut converted = 0;
            for value in cache.values.values_mut() {
                if value.is_expired(now_ms) {
                    *value = value.expiry_tombstone();
                    converted += 1;
                }
            }
            if converted != 0 {
                self.revision.fetch_add(
                    u64::try_from(converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            return Ok(converted);
        }

        let (converted, should_flush) = {
            let mut flush = self.flush.lock().await;
            let mut cache = self.cache.write().await;
            let mut converted = 0;
            for (key, value) in cache.values.iter_mut() {
                if value.is_expired(now_ms) {
                    let tombstone = value.expiry_tombstone();
                    *value = tombstone.clone();
                    flush.pending.push((key.clone(), tombstone));
                    converted += 1;
                }
            }
            if converted != 0 {
                self.revision.fetch_add(
                    u64::try_from(converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            let should_flush = converted > 0
                && (flush.pending.len() >= self.flush_batch_size
                    || flush.last_flush.elapsed() >= self.flush_interval);
            (converted, should_flush)
        };

        if should_flush {
            self.flush().await?;
        }
        Ok(converted)
    }

    /// Periodic maintenance only; authoritative shutdown still uses the full
    /// public sweep. Every visited current value counts, including non-TTL
    /// records. One terminal record may cross the soft work-byte target.
    pub(crate) async fn expire_values_page(
        &self,
        pass: &mut ExpiryPass,
    ) -> Result<ExpiryPage, ActorProcessingErr> {
        let (page, should_flush) = {
            // Preserve normal apply's lock order and adjacent cache/enqueue/
            // revision publication. No await occurs after the cache is locked.
            let mut flush = if self.kvstore.is_some() {
                Some(self.flush.lock().await)
            } else {
                None
            };
            let mut cache = self.cache.write().await;
            let mut initial_work = 0usize;
            if !pass.initialized {
                pass.remaining = cache.ordered_keys.len();
                pass.high_water = cache.ordered_keys.last().cloned();
                if let Some(high_water) = &pass.high_water {
                    // Charge this single pass-metadata clone to the first
                    // visited record, including an oversized first record.
                    initial_work = std::mem::size_of::<String>().saturating_add(high_water.len());
                }
                pass.initialized = true;
            }
            let mut page = ExpiryPage {
                visited: 0,
                converted: 0,
                work_bytes: 0,
                done: pass.remaining == 0,
            };
            let mut next_cursor = None;
            let mut exhausted = false;
            if let Some(high_water) = pass.high_water.as_deref().filter(|_| !page.done) {
                use std::ops::Bound::{Excluded, Included, Unbounded};
                let lower = pass.after_key.as_deref().map_or(Unbounded, Excluded);
                let FragmentCache {
                    values,
                    ordered_keys,
                } = &mut *cache;
                let mut keys = ordered_keys.range::<str, _>((lower, Included(high_water)));
                let limit = pass.remaining.min(EXPIRY_PAGE_VISIT_LIMIT);
                while page.visited < limit
                    && (page.visited == 0 || page.work_bytes < EXPIRY_PAGE_WORK_BYTES)
                {
                    let Some(key) = keys.next() else {
                        exhausted = true;
                        break;
                    };
                    let value = values.get_mut(key).expect("ordered cache key has a value");
                    let expired = value.is_expired(pass.now_ms);
                    let charge = expiry_record_work_bytes(key, value, expired, flush.is_some())
                        .saturating_add(std::mem::take(&mut initial_work));
                    page.work_bytes = page.work_bytes.saturating_add(charge);
                    page.visited += 1;
                    next_cursor = Some(key.clone());
                    if expired {
                        let tombstone = value.expiry_tombstone();
                        if let Some(flush) = &mut flush {
                            *value = tombstone.clone();
                            flush.pending.push((key.clone(), tombstone));
                        } else {
                            *value = tombstone;
                        }
                        page.converted += 1;
                    }
                }
            }
            pass.remaining -= page.visited;
            if let Some(cursor) = next_cursor {
                pass.after_key = Some(cursor);
            }
            page.done |= exhausted
                || pass.remaining == 0
                || pass.after_key.as_deref() == pass.high_water.as_deref();
            if page.done {
                pass.remaining = 0;
            }
            if page.converted != 0 {
                self.revision.fetch_add(
                    u64::try_from(page.converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            let should_flush = page.converted > 0
                && flush.as_ref().is_some_and(|flush| {
                    flush.pending.len() >= self.flush_batch_size
                        || flush.last_flush.elapsed() >= self.flush_interval
                });
            (page, should_flush)
        };
        if should_flush {
            self.flush().await?;
        }
        Ok(page)
    }
}

impl FlushBuffer {
    /// Start or resume a flush attempt. The returned batch is a snapshot;
    /// its authoritative copy remains in `in_flight` until commit.
    ///
    /// Pending writes are appended to a failed/cancelled attempt before its
    /// retry, preserving the cache's winning-version order. The attempt time
    /// is stamped even on failure to avoid a hot retry loop against a sick
    /// store.
    fn begin_batch(&mut self) -> Option<Vec<(String, VersionedValue)>> {
        if self.in_flight.is_none() && self.pending.is_empty() {
            return None;
        }
        let pending = std::mem::take(&mut self.pending);
        self.in_flight.get_or_insert_with(Vec::new).extend(pending);
        self.last_flush = Instant::now();
        self.in_flight.clone()
    }

    /// Forget only the batch whose store write has completed. Writes that
    /// arrived during that call remain in `pending`.
    fn commit_batch(&mut self) {
        self.in_flight = None;
    }
}

#[cfg(test)]
impl FragmentState {
    // Test-only contention gates retain the private cache guard without
    // exposing cache representation to sibling actor modules.
    pub(super) async fn lock_cache_for_test(&self) -> impl Sized + '_ {
        self.cache.write().await
    }
}

#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod maintenance_diagnostics;
#[cfg(test)]
mod tests;
