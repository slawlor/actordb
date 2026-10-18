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
use super::scan::{KeyMatcher, ScanMatch, ScanSpec, ValueTransform};
use crate::store::{KvStore, StoreCommitDurability};
use crate::wire::check_versioned_value_budget;

// Actor-owned summary clones stay bounded before wire metadata is added.
static NEXT_QUERY_INCARNATION: AtomicU64 = AtomicU64::new(1);

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
    query_incarnation: u64,
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
            query_incarnation: NEXT_QUERY_INCARNATION.fetch_add(1, Ordering::Relaxed),
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

    /// Scan the fragment's local cache, returning transformed matches.
    ///
    /// Single fused pass under one read lock: filter (key matcher, then
    /// [`get`](Self::get)-style visibility — tombstones and TTL-expired
    /// values are hidden) and transform happen inline, and only matches are
    /// materialized. Never clone-then-filter.
    ///
    /// Matches are returned in lexicographic key order (the fragment's
    /// ordered key index), so [`ScanSpec::offset`] pagination is stable as
    /// long as the key set does not change between pages. At most
    /// [`ScanSpec::effective_limit`](crate::actor::ScanSpec::effective_limit)
    /// matches are returned.
    ///
    /// The read lock is held for the whole pass: concurrent writes to this
    /// fragment pause for the scan's duration (reads do not). Keep `limit`
    /// tight; chunked scans are a future follow-up.
    pub async fn scan(&self, spec: &ScanSpec) -> Vec<ScanMatch> {
        let now = now_ms();
        let limit = spec.effective_limit();
        let cache = self.cache.read().await;
        let mut out = Vec::new();
        let mut skipped: u64 = 0;
        // Lexicographic order via the ordered key index: deterministic
        // results, stable offset pagination.
        for key in cache.ordered_keys.iter() {
            let Some(value) = cache.values.get(key) else {
                continue;
            };
            // Same visibility as `get`: tombstones and expired values are
            // not matches.
            if value.is_tombstone || value.is_expired(now) {
                continue;
            }
            let matched = match &spec.matcher {
                KeyMatcher::All => true,
                KeyMatcher::Prefix(prefix) => key.as_bytes().starts_with(prefix),
            };
            if !matched {
                continue;
            }
            if skipped < spec.offset {
                skipped += 1;
                continue;
            }
            let transformed = match spec.transform {
                ValueTransform::Identity => value.value.clone(),
                ValueTransform::KeyOnly => Vec::new(),
                ValueTransform::PrefixBytes(n) => {
                    value.value[..value.value.len().min(n as usize)].to_vec()
                }
            };
            out.push(ScanMatch {
                key: key.clone(),
                value: transformed,
            });
            if out.len() as u64 >= limit {
                break;
            }
        }
        out
    }

    /// Read a revision-guarded page through the background worker pool.
    /// Cursor progression includes filtered and invisible records, so sparse
    /// predicates cannot turn a page into an unbounded cache-lock pass.
    pub async fn query_page(
        &self,
        work: &crate::query::QueryPageWork,
    ) -> crate::proto::QueryPageResponse {
        use crate::proto::{QueryPageRow, QueryPageStatus};
        use crate::query::{
            page_encoded_len, page_error, value_to_cell, MAX_QUERY_PAGE_BYTES, QUERY_PAGE_VISITS,
            QUERY_WORK_BYTES,
        };
        use actordb_query::{AggregateInput, Projection, QueryAccumulator};
        use std::ops::Bound::{Excluded, Unbounded};

        let req = &work.request;
        let plan = &work.plan;
        if let Err(error) = plan.validate() {
            return page_error(req, QueryPageStatus::Invalid, error.to_string());
        }
        let max_rows = if req.max_rows == 0 {
            QUERY_PAGE_VISITS
        } else {
            req.max_rows as usize
        };
        let max_visits = if req.max_visits == 0 {
            QUERY_PAGE_VISITS
        } else {
            req.max_visits as usize
        };
        let max_bytes = if req.max_bytes == 0 {
            1024 * 1024
        } else {
            req.max_bytes as usize
        };
        if max_rows > QUERY_PAGE_VISITS
            || max_visits > QUERY_PAGE_VISITS
            || !(1024..=MAX_QUERY_PAGE_BYTES).contains(&max_bytes)
        {
            return page_error(req, QueryPageStatus::Invalid, "invalid query page limits");
        }
        if !req.continuation
            && (!req.after_key.is_empty()
                || req.revision != 0
                || req.snapshot_time_ms != 0
                || req.incarnation != 0
                || req.verify_only)
        {
            return page_error(
                req,
                QueryPageStatus::Invalid,
                "initial page contains continuation state",
            );
        }
        if req.continuation && (req.snapshot_time_ms == 0 || req.incarnation == 0) {
            return page_error(
                req,
                QueryPageStatus::Invalid,
                "continuation lacks pinned identity/time",
            );
        }
        let cache = self.cache.read().await;
        let revision = self.revision.load(Ordering::Acquire);
        if req.continuation
            && (req.revision != revision || req.incarnation != self.query_incarnation)
        {
            return page_error(
                req,
                QueryPageStatus::Changed,
                "fragment changed between query pages",
            );
        }
        let snapshot_time_ms = if req.continuation {
            req.snapshot_time_ms
        } else {
            now_ms()
        };
        let mut page = crate::proto::QueryPageResponse {
            req_id: req.req_id,
            fragment: req.fragment,
            rows: Vec::new(),
            next_after_key: req.after_key.clone(),
            done: true,
            revision,
            snapshot_time_ms,
            incarnation: self.query_incarnation,
            status: QueryPageStatus::Ok as i32,
            ..Default::default()
        };
        if req.verify_only || !plan.matches_fragment(req.fragment) {
            return page;
        }
        let lower = if req.continuation {
            Excluded(req.after_key.clone())
        } else {
            Unbounded
        };
        let mut keys = cache.ordered_keys.range((lower, Unbounded)).peekable();
        let mut accumulator = QueryAccumulator::new(plan);
        while let Some(key) = keys.peek().copied() {
            if page.visited as usize == max_visits
                || (!plan.is_aggregate() && page.rows.len() == max_rows)
            {
                page.done = false;
                break;
            }
            let Some(value) = cache.values.get(key) else {
                keys.next();
                continue;
            };
            let matched =
                !value.is_tombstone && !value.is_expired(snapshot_time_ms) && plan.matches_key(key);
            // Charge actual projected/parsed work, not an untouched full value
            // for COUNT or OCTET_LENGTH. A wide parsed BIGINT/projection still
            // has a strict per-turn work cap.
            let value_work = if matched {
                plan.projections
                    .iter()
                    .map(|projection| match projection {
                        Projection::Value => value.value.len(),
                        Projection::PrefixBytes(n) => value.value.len().min(*n as usize),
                        Projection::Aggregate {
                            input: AggregateInput::BigInt,
                            ..
                        } => value.value.len(),
                        _ => 0,
                    })
                    .sum::<usize>()
            } else {
                0
            };
            let record_work = key
                .len()
                .saturating_mul(2)
                .saturating_add(value_work)
                .saturating_add(128);
            if record_work > QUERY_WORK_BYTES {
                return page_error(
                    req,
                    QueryPageStatus::Budget,
                    "one record exceeds query work/page bytes; use a narrower projection",
                );
            }
            if page.work_bytes.saturating_add(record_work as u64) > QUERY_WORK_BYTES as u64 {
                page.done = false;
                break;
            }
            if matched && !plan.is_aggregate() {
                let row_wire_bytes = crate::query::projected_row_wire_bytes(
                    plan,
                    key,
                    value.value.len(),
                    req.fragment,
                );
                let predicted = page_encoded_len(&page)
                    .saturating_add(crate::wire::message_field_len(3, row_wire_bytes))
                    .saturating_add(crate::wire::bytes_field_len(4, key.len()))
                    .saturating_add(32);
                if predicted > max_bytes {
                    if page.visited == 0 {
                        return page_error(
                            req,
                            QueryPageStatus::Budget,
                            "projected row exceeds encoded query page budget",
                        );
                    }
                    page.done = false;
                    break;
                }
                let cells = match plan.project(key, &value.value, req.fragment) {
                    Ok(values) => values.into_iter().map(value_to_cell).collect(),
                    Err(error) => {
                        return page_error(req, QueryPageStatus::Invalid, error.to_string())
                    }
                };
                page.rows.push(QueryPageRow {
                    key: key.clone(),
                    cells,
                });
                let old_cursor = std::mem::replace(&mut page.next_after_key, key.clone());
                if page_encoded_len(&page).saturating_add(32) > max_bytes {
                    page.rows.pop();
                    page.next_after_key = old_cursor;
                    if page.visited == 0 {
                        return page_error(
                            req,
                            QueryPageStatus::Budget,
                            "projected row exceeds encoded query page budget",
                        );
                    }
                    page.done = false;
                    break;
                }
            } else {
                page.next_after_key = key.clone();
                if matched {
                    if let Err(error) = accumulator.push(key, &value.value, req.fragment) {
                        return page_error(req, QueryPageStatus::Invalid, error.to_string());
                    }
                }
            }
            page.visited += 1;
            page.work_bytes += record_work as u64;
            keys.next();
        }
        if plan.is_aggregate() {
            let result = accumulator.finish();
            page.rows = result
                .rows
                .into_iter()
                .map(|row| QueryPageRow {
                    key: String::new(),
                    cells: row.into_iter().map(value_to_cell).collect(),
                })
                .collect();
        }
        if page_encoded_len(&page) > max_bytes {
            return page_error(
                req,
                QueryPageStatus::Budget,
                "query page cursor/result exceeds encoded budget",
            );
        }
        page
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
mod flush_tests {
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
}
#[cfg(test)]
mod maintenance_diagnostics {
    //! Opt-in, descriptive latency samples, not a CI performance assertion.
    //! Full-sweep baseline uses the retained public primitive; paged mode runs the
    //! actual Cleaner. Both contend with actual dispatcher/worker foreground RPCs.

    use super::*;
    use crate::actor::{
        Cleaner, CleanerArgs, KvActor, KvArgs, KvMessage, OpPriority, PoolConfig, ReplicatedOp,
    };
    use crate::shard::FragmentId;
    use ractor::rpc::CallResult;
    use ractor::Actor;
    use tokio::sync::Barrier;

    const MAX_SAMPLES_PER_CLASS: usize = 20_000;

    #[derive(Clone, Copy, Debug)]
    enum Mode {
        FullSweep,
        AutomaticCleaner,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual release diagnostic; no shared-runner latency threshold"]
    async fn maintenance_foreground_latency_distribution() {
        eprintln!("mode,keys,map_capacity,ttl_percent,materialization_us,total_through_drain_us,class,operations,stored_samples,p50_us,p95_us,p99_us,max_us");
        for cardinality in [10_000usize, 100_000] {
            for capacity_multiple in [1usize, 8] {
                for ttl_percent in [0usize, 1, 100] {
                    for mode in [Mode::FullSweep, Mode::AutomaticCleaner] {
                        measure_case(cardinality, capacity_multiple, ttl_percent, mode).await;
                    }
                }
            }
        }
    }

    async fn measure_case(
        cardinality: usize,
        capacity_multiple: usize,
        ttl_percent: usize,
        mode: Mode,
    ) {
        let mut values = HashMap::with_capacity(cardinality * capacity_multiple + 4);
        for i in 0..cardinality {
            let mut value = VersionedValue::new(vec![1; 32], 10, "seed".into());
            if i < cardinality * ttl_percent / 100 {
                value.expires_at_ms = Some(1);
            }
            values.insert(format!("k{i:08}"), value);
        }
        // A last ordered witness makes even the near-zero-expiry pass observable.
        // This is not a literal zero-expiry dataset: one witness always expires.
        let mut witness = VersionedValue::new(vec![1; 32], 10, "seed".into());
        witness.expires_at_ms = Some(1);
        values.insert("zz-witness".into(), witness);
        let classes = [
            ("high-hit", OpPriority::High, false, "foreground-high"),
            (
                "default-hit",
                OpPriority::Default,
                false,
                "foreground-default",
            ),
            (
                "overwrite",
                OpPriority::Default,
                true,
                "foreground-overwrite",
            ),
        ];
        for (_, _, _, key) in classes {
            values.insert(
                key.into(),
                VersionedValue::new(vec![1; 32], 10, "seed".into()),
            );
        }
        let map_capacity = values.capacity();
        let shared = Arc::new(FragmentState::new(
            values,
            None,
            usize::MAX,
            Duration::from_secs(3600),
        ));
        let pool = PoolConfig {
            workers: 2,
            queue_cap: 128,
            rate_limit_per_sec: None,
        };
        let (dispatcher, _) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared: shared.clone(),
                fragment: FragmentId(0),
                worker_prefix: format!(
                    "maintenance-diag-{cardinality}-{capacity_multiple}-{ttl_percent}-{mode:?}"
                ),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let start_gate = Arc::new(Barrier::new(4));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut foreground = Vec::new();
        for (label, priority, write, key) in classes {
            let dispatcher = dispatcher.clone();
            let start_gate = start_gate.clone();
            let stop = stop.clone();
            foreground.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(MAX_SAMPLES_PER_CLASS);
                let mut operations = 0usize;
                start_gate.wait().await;
                while !stop.load(Ordering::Acquire) {
                    let started = Instant::now();
                    if write {
                        let outcome = dispatcher
                            .call(
                                |reply| KvMessage::ApplyOp {
                                    op: ReplicatedOp {
                                        key: key.into(),
                                        value: VersionedValue::new(
                                            vec![2; 32],
                                            100 + operations as u64,
                                            "foreground".into(),
                                        ),
                                        seq: 0,
                                        boot_id: 0,
                                    },
                                    priority,
                                    is_replication: false,
                                    reply: Some(reply),
                                },
                                Some(Duration::from_secs(5)),
                            )
                            .await
                            .unwrap();
                        assert!(matches!(
                            outcome,
                            CallResult::Success(Ok(ApplyOutcome::Applied))
                        ));
                    } else {
                        let outcome = dispatcher
                            .call(
                                |reply| KvMessage::Get {
                                    key: key.into(),
                                    priority,
                                    reply,
                                },
                                Some(Duration::from_secs(5)),
                            )
                            .await
                            .unwrap();
                        assert!(matches!(outcome, CallResult::Success(Ok(Some(_)))));
                    }
                    if samples.len() < MAX_SAMPLES_PER_CLASS {
                        samples
                            .push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    }
                    operations += 1;
                }
                (label, operations, samples)
            }));
        }
        start_gate.wait().await;
        let started = Instant::now();
        let mut full_sweep = None;
        let mut cleaner = None;
        match mode {
            Mode::FullSweep => {
                let shared = shared.clone();
                full_sweep = Some(tokio::spawn(async move {
                    shared.expire_values(now_ms()).await.unwrap()
                }));
            }
            Mode::AutomaticCleaner => {
                cleaner = Some(
                    Cleaner::spawn(
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
                    .unwrap()
                    .0,
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if shared.cache.read().await.values["zz-witness"].is_tombstone {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let materialization = started.elapsed();
        stop.store(true, Ordering::Release);
        if let Some(full_sweep) = full_sweep {
            assert_eq!(
                full_sweep.await.unwrap(),
                cardinality * ttl_percent / 100 + 1
            );
        }
        if let Some(cleaner) = cleaner {
            cleaner.drain().unwrap();
            cleaner.wait(None).await.unwrap();
        }
        let mut results = Vec::new();
        for task in foreground {
            results.push(task.await.unwrap());
        }
        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
        let total = started.elapsed();
        for (label, operations, mut samples) in results {
            samples.sort_unstable();
            let quantile = |percent: usize| {
                // One blocked RPC is a useful maximum observation, not evidence
                // for a meaningful tail distribution. Report sparse tails as NA.
                if (percent == 99 && samples.len() < 100) || (percent == 95 && samples.len() < 20) {
                    return "na".into();
                }
                samples
                    .get(samples.len().saturating_sub(1) * percent / 100)
                    .map(|ns| format!("{:.3}", *ns as f64 / 1000.0))
                    .unwrap_or_else(|| "na".into())
            };
            eprintln!("{mode:?},{cardinality},{map_capacity},{ttl_percent},{},{},{label},{operations},{},{},{},{},{}",
                materialization.as_micros(), total.as_micros(), samples.len(), quantile(50), quantile(95), quantile(99), quantile(100));
        }
    }
}
#[cfg(test)]
mod tests {
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
}

#[cfg(test)]
mod query_page_tests {
    use super::*;
    use crate::proto::{QueryPageRequest, QueryPageStatus};
    use crate::query::QueryPageWork;

    fn query_state(keys: impl IntoIterator<Item = String>) -> FragmentState {
        FragmentState::new(
            keys.into_iter()
                .map(|key| (key, VersionedValue::new(b"123".to_vec(), 1, "n".into())))
                .collect(),
            None,
            1000,
            Duration::from_secs(60),
        )
    }
    fn work(sql: &str, visits: u32) -> QueryPageWork {
        QueryPageWork::new(
            Arc::new(actordb_query::parse(sql).unwrap()),
            QueryPageRequest {
                req_id: 7,
                max_visits: visits,
                max_rows: 256,
                max_bytes: 1024 * 1024,
                ..Default::default()
            },
        )
    }
    fn resume(work: &mut QueryPageWork, page: &crate::proto::QueryPageResponse) {
        work.request.continuation = true;
        work.request.after_key = page.next_after_key.clone();
        work.request.revision = page.revision;
        work.request.snapshot_time_ms = page.snapshot_time_ms;
        work.request.incarnation = page.incarnation;
    }

    #[tokio::test]
    async fn cancelled_query_waiter_retains_actor_admission_until_page_finishes() {
        use crate::actor::{KvMessage, KvWorker, KvWorkerArgs};
        use ractor::Actor;
        let state = Arc::new(query_state(["a".into()]));
        let admission = crate::query::QueryAdmission::new();
        let retention = admission.reserve(1024).unwrap();
        let weak = Arc::downgrade(&retention);
        let (worker, handle) = Actor::spawn(
            None,
            KvWorker,
            KvWorkerArgs {
                shared: state.clone(),
                pending: None,
            },
        )
        .await
        .unwrap();
        let cache = state.cache.write().await;
        let (reply, response) = tokio::sync::oneshot::channel();
        let mut work = work("SELECT key FROM kv", 1);
        work.retention = Some(retention);
        worker
            .cast(KvMessage::QueryPage {
                work,
                reply: reply.into(),
            })
            .unwrap();
        // Let the actor enter the read-lock wait, then cancel only its caller.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(response);
        assert!(weak.upgrade().is_some());
        drop(cache);
        crate::common_test::periodic_check(|| weak.upgrade().is_none(), Duration::from_secs(2))
            .await;
        assert!(weak.upgrade().is_none());
        worker.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn sparse_query_pages_advance_through_nonmatches() {
        let state = query_state((0..600).map(|n| format!("a{n:04}")).chain(["z-hit".into()]));
        let mut work = work("SELECT key FROM kv WHERE STARTS_WITH(key, 'z')", 256);
        let first = state.query_page(&work).await;
        assert_eq!(first.status, QueryPageStatus::Ok as i32);
        assert_eq!(first.visited, 256);
        assert!(!first.done);
        assert!(first.rows.is_empty());
        resume(&mut work, &first);
        let second = state.query_page(&work).await;
        assert_eq!(second.visited, 256);
        assert!(second.next_after_key > first.next_after_key);
        assert!(!second.done);
        resume(&mut work, &second);
        let third = state.query_page(&work).await;
        assert!(third.done);
        assert_eq!(third.visited, 89);
        assert_eq!(third.rows.len(), 1);
        assert_eq!(third.rows[0].key, "z-hit");
    }

    #[tokio::test]
    async fn query_continuation_rejects_mutations_and_new_incarnations() {
        let state = query_state(["a".into(), "b".into()]);
        let mut work = work("SELECT key FROM kv", 1);
        let first = state.query_page(&work).await;
        resume(&mut work, &first);
        let replacement = query_state(["a".into(), "b".into()]);
        assert_eq!(
            replacement.query_page(&work).await.status,
            QueryPageStatus::Changed as i32
        );
        state
            .apply(&ReplicatedOp {
                key: "b".into(),
                value: VersionedValue::new(b"456".to_vec(), 2, "n".into()),
                seq: 1,
                boot_id: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            state.query_page(&work).await.status,
            QueryPageStatus::Changed as i32
        );
    }

    #[tokio::test]
    async fn query_pages_reject_oversized_rows_but_count_wide_values() {
        let state = FragmentState::new(
            HashMap::from([(
                "wide".into(),
                VersionedValue::new(vec![1; 2 * 1024 * 1024], 1, "n".into()),
            )]),
            None,
            1000,
            Duration::from_secs(60),
        );
        let projected = state
            .query_page(&work("SELECT key, value FROM kv", 256))
            .await;
        assert_eq!(projected.status, QueryPageStatus::Budget as i32);
        assert!(projected.rows.is_empty());
        let count = state
            .query_page(&work(
                "SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM kv",
                256,
            ))
            .await;
        assert_eq!(count.status, QueryPageStatus::Ok as i32);
        assert!(count.done);
        assert_eq!(
            count.rows[0]
                .cells
                .iter()
                .cloned()
                .map(crate::query::cell_to_value)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![
                actordb_query::QueryValue::Integer(1),
                actordb_query::QueryValue::Integer(2 * 1024 * 1024)
            ]
        );
    }

    #[tokio::test]
    async fn query_page_bytes_stop_before_cloning_a_wide_projection() {
        let state = FragmentState::new(
            HashMap::from([
                ("a".into(), VersionedValue::new(vec![1; 32], 1, "n".into())),
                (
                    "b".into(),
                    VersionedValue::new(vec![2; 2048], 1, "n".into()),
                ),
            ]),
            None,
            1000,
            Duration::from_secs(60),
        );
        let mut work = work("SELECT key, value FROM kv", 256);
        work.request.max_bytes = 1024;
        let page = state.query_page(&work).await;
        assert_eq!(page.status, QueryPageStatus::Ok as i32);
        assert_eq!(page.visited, 1);
        assert!(!page.done);
        assert_eq!(page.rows[0].key, "a");
        resume(&mut work, &page);
        let oversized = state.query_page(&work).await;
        assert_eq!(oversized.status, QueryPageStatus::Budget as i32);
        assert!(oversized.rows.is_empty());
    }
}
