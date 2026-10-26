// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Shared cache, ordered persistence, expiry, and snapshot state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use ractor::ActorProcessingErr;
use tokio::sync::{Mutex, RwLock};

use super::clock::now_ms;
use super::protocol::{
    ApplyDurability, ApplyError, ApplyOutcome, ReplicatedOp, SnapshotSummary, SummaryPage,
    VersionedPage, VersionedValue,
};
use super::scan::{KeyMatcher, ScanMatch, ScanSpec};
use crate::retained::{ByteBudget, BytePermit};
use crate::shard::FragmentId;
use crate::store::{KvStore, PersistenceConfig, StoreCommitDurability, TableStore};
use crate::ttl_expiry::ExpiryPublisher;
use crate::wire::check_versioned_value_budget;

// Actor-owned summary clones stay bounded before wire metadata is added.
static NEXT_QUERY_INCARNATION: AtomicU64 = AtomicU64::new(1);

const SUMMARY_PAGE_TARGET_BYTES: usize = 1024 * 1024;
const VERSIONED_PAGE_TARGET_BYTES: usize = 1024 * 1024;

fn query_page_limits(work: &crate::query::QueryPageWork) -> Result<(usize, usize, usize), String> {
    use crate::query::{MAX_QUERY_PAGE_BYTES, QUERY_PAGE_VISITS};
    let req = &work.request;
    if let Err(error) = work.plan.validate_with_limits(work.validation_limits) {
        return Err(error.to_string());
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
        return Err("invalid query page limits".into());
    }
    if !req.continuation
        && (!req.after_key.is_empty()
            || req.revision != 0
            || req.snapshot_time_ms != 0
            || req.incarnation != 0
            || req.verify_only)
    {
        return Err("initial page contains continuation state".into());
    }
    if req.continuation && (req.snapshot_time_ms == 0 || req.incarnation == 0) {
        return Err("continuation lacks pinned identity/time".into());
    }
    Ok((max_rows, max_visits, max_bytes))
}
/// Shared fragment/cache state. Named-table cache and ordered flush partitions
/// share one fragment-wide persistence admission allowance and node executor.
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
    /// Fragment-wide metadata publication, shared with every data child. No
    /// payloads are retained here; summary paging never acquires child locks.
    summary_index: Arc<StdRwLock<SummaryIndex>>,
    /// Winning cache publication takes the shared side; the explicitly full
    /// diagnostic snapshot takes the exclusive side. Never held through I/O.
    snapshot_fence: Arc<RwLock<()>>,
    #[cfg(test)]
    snapshot_pause: std::sync::Mutex<Option<Arc<summary_revision_tests::SnapshotPause>>>,
    query_incarnation: u64,
    /// Buffered writes awaiting flush.
    flush: Arc<Mutex<FlushBuffer>>,
    executor: Arc<crate::store::executor::StorageExecutor>,
    persistence: Arc<FragmentPersistenceResources>,
    /// Allows exactly one store write at a time for this cache/table partition. The
    /// buffered batch remains recorded while the write is in progress, so a
    /// cancelled or failed flush can be retried without losing it.
    flush_in_progress: Arc<Mutex<()>>,
    flush_batch_size: usize,
    flush_interval: Duration,
    expiry_publisher: Option<(FragmentId, ExpiryPublisher)>,
    /// Independently cached and persisted table partitions. Table children
    /// are created lazily and share fragment admission, backend/executor and snapshot metadata.
    table_children: Arc<RwLock<BTreeMap<String, Arc<FragmentState>>>>,
    partitioned: bool,
    persistence_template: Option<PersistenceConfig>,
    pool_timing: Arc<std::sync::atomic::AtomicBool>,
    table_pool_capacity: usize,
    /// Cold namespaces with no proven record yet. Bounded admission protects
    /// metadata while snapshots/backend owners postpone safe reclamation.
    empty_tables: std::sync::Mutex<BTreeSet<String>>,
}

/// Values and their ordered key index. Both live under one lock so every
/// visible key is immediately available to ordered snapshot pagination.
struct FragmentCache {
    values: HashMap<String, VersionedValue>,
    ordered_keys: BTreeSet<String>,
    /// Hash wide payloads once before mutation locks; summaries only copy hashes.
    content_digests: HashMap<String, CachedDigests>,
    // Exactly one deadline per live TTL winner. Replacements remove the old
    // entry eagerly, so hot-key updates cannot accumulate stale heap entries.
    expiry_deadlines: BTreeSet<(u64, String)>,
}

#[derive(Clone, Copy)]
struct CachedDigests {
    live: [u8; 32],
    expired: [u8; 32],
}

#[derive(Default)]
struct SummaryIndex {
    values: BTreeMap<String, SummaryValue>,
    revision: u64,
}

struct SummaryValue {
    timestamp_ms: u64,
    logical_counter: u64,
    node_id: String,
    expires_at_ms: Option<u64>,
    is_tombstone: bool,
    digests: CachedDigests,
}

impl SummaryValue {
    fn new(value: &VersionedValue, digests: CachedDigests) -> Self {
        Self {
            timestamp_ms: value.timestamp_ms,
            logical_counter: value.logical_counter,
            node_id: value.node_id.clone(),
            expires_at_ms: value.expires_at_ms,
            is_tombstone: value.is_tombstone,
            digests,
        }
    }

    fn summary(&self, key: &str, snapshot_time_ms: u64) -> SnapshotSummary {
        let expired = !self.is_tombstone
            && self
                .expires_at_ms
                .is_some_and(|deadline| deadline <= snapshot_time_ms);
        SnapshotSummary {
            key: key.to_string(),
            timestamp_ms: self.timestamp_ms,
            logical_counter: self.logical_counter,
            node_id: self.node_id.clone(),
            is_tombstone: self.is_tombstone || expired,
            content_digest: if expired {
                self.digests.expired
            } else {
                self.digests.live
            },
        }
    }
}
impl CachedDigests {
    fn new(value: &VersionedValue) -> Self {
        Self {
            live: value.content_digest(),
            expired: value.expiry_tombstone().content_digest(),
        }
    }
    fn expire(&mut self) {
        self.live = self.expired;
    }
}

pub(crate) const EXPIRY_PAGE_VISIT_LIMIT: usize = 256;
pub(crate) const EXPIRY_PAGE_WORK_BYTES: usize = 1024 * 1024;

/// A weakly consistent, finite pass over deadlines due at a pinned time.
/// The initial index cardinality bounds insert churn without scanning keys.
pub(crate) struct ExpiryPass {
    now_ms: u64,
    initialized: bool,
    remaining: usize,
    after_deadline: Option<(u64, String)>,
    root_done: bool,
    tables_remaining: Option<usize>,
    after_table: Option<String>,
    active_table: Option<Arc<FragmentState>>,
    partition_done: bool,
    /// Explicit full passes can release shared admission between attempts.
    /// Automatic pages keep their finite skip-on-saturation behavior.
    retry_saturation: bool,
}

impl ExpiryPass {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            initialized: false,
            remaining: 0,
            after_deadline: None,
            root_done: false,
            tables_remaining: None,
            after_table: None,
            active_table: None,
            partition_done: false,
            retry_saturation: false,
        }
    }

    fn reset_deadlines(&mut self) {
        self.initialized = false;
        self.remaining = 0;
        self.after_deadline = None;
    }
}

#[derive(Debug)]
pub(crate) struct ExpiryPage {
    pub(crate) visited: usize,
    pub(crate) converted: usize,
    pub(crate) work_bytes: usize,
    pub(crate) done: bool,
    saturated: bool,
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

struct PendingWrite {
    value: VersionedValue,
    local: BytePermit,
    aggregate: BytePermit,
    /// Pending and immutable active representations each retain one record.
    _record: BytePermit,
}

struct FragmentPersistenceResources {
    records: Arc<ByteBudget>,
    bytes: Arc<ByteBudget>,
    aggregate: Arc<ByteBudget>,
    executor: Arc<crate::store::executor::StorageExecutor>,
}
impl FragmentPersistenceResources {
    fn new(config: &PersistenceConfig) -> Arc<Self> {
        Arc::new(Self {
            records: ByteBudget::new(config.max_records()),
            bytes: ByteBudget::new(config.max_bytes()),
            aggregate: config.aggregate(),
            executor: config.executor(),
        })
    }
}
struct FlushBuffer {
    pending: HashMap<String, PendingWrite>,
    /// Immutable authoritative ownership during an ordered store call.
    in_flight: Option<Arc<HashMap<String, PendingWrite>>>,
    local_budget: Arc<ByteBudget>,
    aggregate_budget: Arc<ByteBudget>,
    record_budget: Arc<ByteBudget>,
    last_flush: Instant,
    failed: bool,
}

fn persistence_record_bytes(key: &str, value: &VersionedValue) -> usize {
    // Both authoritative and backend-owned encoded representations; hash
    // buckets, String/Vec headers, permits, and allocator slack are charged.
    2usize.saturating_mul(
        256usize
            .saturating_add(key.len())
            .saturating_add(value.value.len())
            .saturating_add(value.node_id.len()),
    )
}

impl FragmentState {
    pub fn new(
        cache: HashMap<String, VersionedValue>,
        kvstore: Option<Arc<dyn KvStore>>,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let mut unqualified = HashMap::new();
        let mut table_caches: HashMap<String, HashMap<String, VersionedValue>> = HashMap::new();
        for (key, value) in cache {
            if let Some(table) = crate::table_key::table_name(&key) {
                table_caches.entry(table).or_default().insert(key, value);
            } else {
                unqualified.insert(key, value);
            }
        }
        let mut state = Self::new_local(
            unqualified,
            kvstore.clone(),
            flush_batch_size,
            flush_interval,
        );
        let children = table_caches
            .into_iter()
            .map(|(table, cache)| {
                let store = kvstore.as_ref().map(|inner| {
                    Arc::new(TableStore::new(inner.clone(), &table)) as Arc<dyn KvStore>
                });
                let mut child = Self::new_local(cache, store, flush_batch_size, flush_interval);
                child
                    .share_persistence(state.persistence.clone())
                    .expect("startup table persistence is unshared and empty");
                (table, Arc::new(child.with_summary_state(&state)))
            })
            .collect();
        state.table_children = Arc::new(RwLock::new(children));
        state
    }

    fn new_local(
        cache: HashMap<String, VersionedValue>,
        kvstore: Option<Arc<dyn KvStore>>,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let ordered_keys = cache.keys().cloned().collect();
        let content_digests: HashMap<String, CachedDigests> = cache
            .iter()
            .map(|(key, value)| (key.clone(), CachedDigests::new(value)))
            .collect();
        let summary_values = cache
            .iter()
            .map(|(key, value)| (key.clone(), SummaryValue::new(value, content_digests[key])))
            .collect();
        let expiry_deadlines = cache
            .iter()
            .filter_map(|(key, value)| {
                (!value.is_tombstone)
                    .then_some(value.expires_at_ms)
                    .flatten()
                    .map(|deadline| (deadline, key.clone()))
            })
            .collect();
        let persistence = FragmentPersistenceResources::new(&PersistenceConfig::default());
        Self {
            cache: RwLock::new(FragmentCache {
                values: cache,
                ordered_keys,
                content_digests,
                expiry_deadlines,
            }),
            kvstore,
            revision: AtomicU64::new(0),
            summary_index: Arc::new(StdRwLock::new(SummaryIndex {
                values: summary_values,
                revision: 0,
            })),
            snapshot_fence: Arc::new(RwLock::new(())),
            #[cfg(test)]
            snapshot_pause: std::sync::Mutex::new(None),
            query_incarnation: NEXT_QUERY_INCARNATION.fetch_add(1, Ordering::Relaxed),
            flush: Arc::new(Mutex::new(FlushBuffer {
                pending: HashMap::new(),
                in_flight: None,
                local_budget: persistence.bytes.clone(),
                aggregate_budget: persistence.aggregate.clone(),
                record_budget: persistence.records.clone(),
                last_flush: Instant::now(),
                failed: false,
            })),
            executor: persistence.executor.clone(),
            persistence,
            flush_in_progress: Arc::new(Mutex::new(())),
            flush_batch_size,
            flush_interval,
            expiry_publisher: None,
            table_children: Arc::new(RwLock::new(BTreeMap::new())),
            partitioned: false,
            persistence_template: None,
            pool_timing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            table_pool_capacity: 16,
            empty_tables: std::sync::Mutex::new(BTreeSet::new()),
        }
    }

    /// Called only for an unshared, newly constructed child. Move metadata,
    /// never payloads, into the root's index before publishing that child.
    fn with_summary_state(self, root: &Self) -> Self {
        self.with_summary_handles(root.summary_index.clone(), root.snapshot_fence.clone())
    }

    fn with_summary_handles(
        mut self,
        summary_index: Arc<StdRwLock<SummaryIndex>>,
        snapshot_fence: Arc<RwLock<()>>,
    ) -> Self {
        let own = Arc::get_mut(&mut self.summary_index)
            .expect("new child metadata is not shared")
            .get_mut()
            .unwrap();
        summary_index
            .write()
            .unwrap()
            .values
            .extend(std::mem::take(&mut own.values));
        self.summary_index = summary_index;
        self.snapshot_fence = snapshot_fence;
        self
    }

    /// Configure retained persistence admission before sharing or applying work.
    /// Rejections from subsequent writes return [`ApplyError::Backpressure`]
    /// before cache mutation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits, shared work, or a retained backlog.
    pub fn with_persistence_config(mut self, config: PersistenceConfig) -> Result<Self, String> {
        config.validate()?;
        self.configure_persistence(&config)?;
        self.persistence_template = Some(config.clone());
        {
            let children = Arc::get_mut(&mut self.table_children)
                .ok_or_else(|| "table partitions are already shared".to_string())?
                .get_mut();
            for child in children.values_mut() {
                let child = Arc::get_mut(child)
                    .ok_or_else(|| "table partition is already shared".to_string())?;
                child.share_persistence(self.persistence.clone())?;
                child.persistence_template = Some(config.clone());
            }
        }
        Ok(self)
    }

    pub(crate) fn with_pool_timing(mut self, timing: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.pool_timing = timing.clone();
        for child in Arc::get_mut(&mut self.table_children)
            .expect("unshared startup table registry")
            .get_mut()
            .values_mut()
        {
            Arc::get_mut(child)
                .expect("unshared startup table state")
                .pool_timing = timing.clone();
        }
        self
    }
    pub(crate) fn pool_timing_enabled(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.pool_timing.clone()
    }
    /// Bound idle-reclaimable named-table worker sets when this state is
    /// attached to a KvActor. Includes KV; fixed base workers are additional.
    /// Data/version floors are not evicted. Configure before starting actors.
    /// Recreated pools start fresh local counters/token buckets. This also
    /// bounds pending-empty metadata retained while safe reclamation waits.
    pub fn with_table_pool_capacity(mut self, capacity: usize) -> Result<Self, String> {
        if !(1..=4096).contains(&capacity) {
            return Err("table_pool_capacity must be between 1 and 4096".into());
        }
        if self.empty_tables.get_mut().unwrap().len() > capacity {
            return Err("pending empty table ownership exceeds the requested capacity".into());
        }
        self.table_pool_capacity = capacity;
        Ok(self)
    }
    pub(crate) fn table_pool_capacity(&self) -> usize {
        self.table_pool_capacity
    }
    fn configure_persistence(&mut self, config: &PersistenceConfig) -> Result<(), String> {
        self.share_persistence(FragmentPersistenceResources::new(config))
    }

    fn share_persistence(
        &mut self,
        resources: Arc<FragmentPersistenceResources>,
    ) -> Result<(), String> {
        let flush = Arc::get_mut(&mut self.flush)
            .ok_or_else(|| "persistence work is already shared".to_string())?
            .get_mut();
        if flush.record_count() != 0 {
            return Err("persistence configuration requires an empty backlog".to_string());
        }
        flush.local_budget = resources.bytes.clone();
        flush.record_budget = resources.records.clone();
        flush.aggregate_budget = resources.aggregate.clone();
        self.executor = resources.executor.clone();
        self.persistence = resources;
        Ok(())
    }

    /// Charged authoritative pending/active bytes and retained records.
    pub async fn persistence_backlog(&self) -> (usize, usize) {
        (
            self.persistence.records.used(),
            self.persistence.bytes.used(),
        )
    }

    pub(crate) fn with_expiry_publisher(
        mut self,
        fragment: FragmentId,
        publisher: Option<ExpiryPublisher>,
    ) -> Self {
        self.expiry_publisher = publisher.clone().map(|publisher| (fragment, publisher));
        if self.partitioned {
            let children = Arc::get_mut(&mut self.table_children)
                .expect("table partitions are not concurrently shared during startup")
                .get_mut();
            for child in children.values_mut() {
                let child = Arc::get_mut(child).expect("table partition is already shared");
                child.expiry_publisher = publisher.clone().map(|publisher| (fragment, publisher));
            }
        }
        self
    }

    /// Enable the table-partition router for a live node fragment.
    pub(crate) fn enable_partitions(mut self) -> Result<Self, String> {
        // Boot loads the backend before the actor tree exists. Move already
        // loaded named records into their child caches now, otherwise the
        // first query would correctly route to an empty child while the
        // parent still held the durable records.
        let mut grouped = HashMap::<String, HashMap<String, VersionedValue>>::new();
        {
            let cache = self.cache.get_mut();
            let mut retained: HashMap<String, VersionedValue> = HashMap::new();
            for (key, value) in std::mem::take(&mut cache.values) {
                if let Some(table) = crate::table_key::table_name(&key) {
                    grouped.entry(table).or_default().insert(key, value);
                } else {
                    retained.insert(key, value);
                }
            }
            cache.ordered_keys = retained.keys().cloned().collect();
            cache.content_digests = retained
                .iter()
                .map(|(key, value)| (key.clone(), CachedDigests::new(value)))
                .collect();
            cache.expiry_deadlines = retained
                .iter()
                .filter_map(|(key, value)| {
                    (!value.is_tombstone)
                        .then_some(value.expires_at_ms)
                        .flatten()
                        .map(|deadline| (deadline, key.clone()))
                })
                .collect();
            cache.values = retained;
        }
        let summary_index = self.summary_index.clone();
        let snapshot_fence = self.snapshot_fence.clone();
        {
            let children = Arc::get_mut(&mut self.table_children)
                .expect("table partitions are not concurrently shared during startup")
                .get_mut();
            for (table, values) in grouped {
                let store = self.kvstore.as_ref().map(|inner| {
                    Arc::new(TableStore::new(inner.clone(), &table)) as Arc<dyn KvStore>
                });
                let mut child =
                    Self::new_local(values, store, self.flush_batch_size, self.flush_interval)
                        .with_summary_handles(summary_index.clone(), snapshot_fence.clone());
                child.pool_timing = self.pool_timing.clone();
                child.table_pool_capacity = self.table_pool_capacity;
                child.expiry_publisher = self.expiry_publisher.clone();
                child.share_persistence(self.persistence.clone())?;
                child.persistence_template = self.persistence_template.clone();
                children.insert(table, Arc::new(child));
            }
        }
        self.partitioned = true;
        {
            let children = Arc::get_mut(&mut self.table_children)
                .ok_or_else(|| "table partitions are already shared".to_string())?
                .get_mut();
            for child in children.values_mut() {
                let child = Arc::get_mut(child)
                    .ok_or_else(|| "table partition is already shared".to_string())?;
                child.share_persistence(self.persistence.clone())?;
                child.persistence_template = self.persistence_template.clone();
            }
        }
        Ok(self)
    }

    async fn table_state(&self, table: &str) -> Result<Arc<FragmentState>, ApplyError> {
        if let Some(state) = self.table_children.read().await.get(table).cloned() {
            return Ok(state);
        }
        self.reclaim_empty_tables().await;
        let mut children = self.table_children.write().await;
        if let Some(state) = children.get(table).cloned() {
            return Ok(state);
        }
        let mut empty = self.empty_tables.lock().unwrap();
        if table != "KV" && empty.len() >= self.table_pool_capacity {
            return Err(ApplyError::Backpressure);
        }
        let store = self
            .kvstore
            .as_ref()
            .map(|inner| Arc::new(TableStore::new(inner.clone(), table)) as Arc<dyn KvStore>);
        let mut state = Self::new_local(
            HashMap::new(),
            store,
            self.flush_batch_size,
            self.flush_interval,
        );
        state.expiry_publisher = self.expiry_publisher.clone();
        state.pool_timing = self.pool_timing.clone();
        state.table_pool_capacity = self.table_pool_capacity;
        state
            .share_persistence(self.persistence.clone())
            .expect("new table persistence is unshared and empty");
        state.persistence_template = self.persistence_template.clone();
        let state = Arc::new(state.with_summary_state(self));
        if table != "KV" {
            empty.insert(table.to_string());
        }
        children.insert(table.to_string(), state.clone());
        Ok(state)
    }

    /// Prepare the fixed KV partition during actor startup. Dynamic named
    /// workers retain the root and resolve data children during execution.
    pub(crate) async fn table_state_for_actor(&self, table: &str) -> Arc<FragmentState> {
        debug_assert_eq!(
            table, "KV",
            "only fixed KV startup may create a table without mutation admission"
        );
        self.table_state(table)
            .await
            .expect("fixed KV partition is exempt from pending-empty admission")
    }

    /// Managed read paths never create unknown table cache entries. KV is
    /// the fixed built-in namespace, independent of direct state-view mode.
    pub(crate) async fn has_table_for_actor(&self, table: &str) -> bool {
        if table == "KV" {
            return true;
        }
        let child = self.table_children.read().await.get(table).cloned();
        let Some(child) = child else {
            return false;
        };
        let present = !child.cache.read().await.values.is_empty();
        drop(child);
        if !present {
            self.prune_empty_table(table).await;
        }
        present
    }

    /// Never remove a data/version floor, or a child retained by executing
    /// work. Worker sets retain the root rather than a speculative empty child.
    pub(crate) async fn prune_empty_table(&self, table: &str) {
        if table == "KV" {
            return;
        }
        if !self.empty_tables.lock().unwrap().contains(table) {
            return;
        }
        let mut children = self.table_children.write().await;
        let mut empty_tables = self.empty_tables.lock().unwrap();
        let has_data = children.get(table).is_some_and(|child| {
            child
                .cache
                .try_read()
                .is_ok_and(|cache| !cache.values.is_empty())
        });
        if has_data || !children.contains_key(table) {
            empty_tables.remove(table);
            return;
        }
        let empty = children.get(table).is_some_and(|child| {
            Arc::strong_count(child) == 1
                && child.cache.try_read().is_ok_and(|cache| {
                    cache.values.is_empty()
                        && cache.ordered_keys.is_empty()
                        && cache.content_digests.is_empty()
                        && cache.expiry_deadlines.is_empty()
                })
                && child
                    .flush
                    .try_lock()
                    .is_ok_and(|flush| flush.record_count() == 0 && flush.in_flight.is_none())
                && child.flush_in_progress.try_lock().is_ok()
        });
        if empty {
            children.remove(table);
            empty_tables.remove(table);
        }
    }

    /// Retry only bounded cold-name candidates, independent of data cardinality.
    pub(crate) async fn reclaim_empty_tables(&self) {
        let pending: Vec<_> = self.empty_tables.lock().unwrap().iter().cloned().collect();
        for table in pending {
            self.prune_empty_table(&table).await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn cached_table_count(&self) -> usize {
        self.table_children.read().await.len()
    }

    fn publish_expiry(&self, key: &str, previous_value: VersionedValue) {
        if let Some((fragment, publisher)) = &self.expiry_publisher {
            publisher.offer(*fragment, key, previous_value);
        }
    }

    fn install_expiry(
        &self,
        value: &mut VersionedValue,
        tombstone: VersionedValue,
    ) -> Option<VersionedValue> {
        if self.expiry_publisher.is_some() {
            Some(std::mem::replace(value, tombstone))
        } else {
            *value = tombstone;
            None
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
    /// handled by different workers. Incoming TTL is evaluated after these
    /// locks are acquired; normalization is not a cleanup callback event.
    ///
    /// An equal full LWW tuple with a different payload or expiry deadline is a
    /// [`ApplyError::VersionConflict`]. This rejects the operation before changing
    /// the cache, revision, or persistence buffer; it never selects a winner by
    /// arrival order. Custom writers must preserve [`VersionedValue`]'s unique
    /// version invariant.
    pub async fn apply(&self, op: &ReplicatedOp) -> Result<ApplyOutcome, ApplyError> {
        if self.partitioned {
            return self.apply_from_actor(op).await;
        }
        self.apply_local(op).await
    }

    /// Managed actors resolve encoded table keys even for a standalone public
    /// unpartitioned root. Direct FragmentState users retain base-cache semantics.
    pub(crate) async fn apply_from_actor(
        &self,
        op: &ReplicatedOp,
    ) -> Result<ApplyOutcome, ApplyError> {
        if let Some(table) = crate::table_key::table_name(&op.key) {
            let child = self.table_state(&table).await?;
            let result = child.apply_local(op).await;
            drop(child);
            self.prune_empty_table(&table).await;
            return result;
        }
        self.apply_local(op).await
    }

    async fn apply_local(&self, op: &ReplicatedOp) -> Result<ApplyOutcome, ApplyError> {
        check_versioned_value_budget(&op.key, &op.value).map_err(|error| {
            ApplyError::InvalidInput(format!(
                "key/value cannot fit every required peer frame: {error}"
            ))
        })?;
        // Hash the raw payload before lock waits; TTL selection only copies
        // its cached live/expired digests inside the mutation critical section.
        let digests = CachedDigests::new(&op.value);
        if self.kvstore.is_none() {
            let _publication = self.snapshot_fence.read().await;
            let mut cache = self.cache.write().await;
            Self::check_version_conflict(&cache, &op.key, &op.value)?;
            let expired = op.value.is_expired(now_ms());
            let tombstone = expired.then(|| op.value.expiry_tombstone());
            let value = tombstone.as_ref().unwrap_or(&op.value);
            let mut selected_digests = digests;
            if expired {
                selected_digests.expire();
            }
            let outcome = Self::merge_cache(&mut cache, &op.key, value, selected_digests)?;
            if outcome == ApplyOutcome::Applied {
                self.publish_summary(&op.key, value, selected_digests);
                self.revision.fetch_add(1, Ordering::Release);
            }
            return Ok(Self::reported_outcome(outcome, expired));
        }

        let (outcome, expired, should_flush) = {
            // Preserve raw and normalized conflict checks before persistence
            // admission. Nothing mutates or awaits after acquiring both locks
            // until the winning merge, durable enqueue, and revision agree.
            let mut flush = self.flush.lock().await;
            let _publication = self.snapshot_fence.read().await;
            let mut cache = self.cache.write().await;
            Self::check_version_conflict(&cache, &op.key, &op.value)?;
            let expired = op.value.is_expired(now_ms());
            let tombstone = expired.then(|| op.value.expiry_tombstone());
            let value = tombstone.as_ref().unwrap_or(&op.value);
            let mut selected_digests = digests;
            if expired {
                selected_digests.expire();
                Self::check_version_conflict(&cache, &op.key, value)?;
            }
            let winning = cache
                .values
                .get(&op.key)
                .is_none_or(|existing| value != existing && value.wins_over(existing));
            if winning && !flush.enqueue(&op.key, value) {
                tracing::warn!(
                    records = flush.record_count(),
                    retained_bytes = flush.local_budget.used(),
                    "rejecting mutation at persistence admission limit"
                );
                return Err(ApplyError::Backpressure);
            }
            let outcome = Self::merge_cache(&mut cache, &op.key, value, selected_digests)?;
            if outcome != ApplyOutcome::Applied {
                return Ok(Self::reported_outcome(outcome, expired));
            }
            self.publish_summary(&op.key, value, selected_digests);
            self.revision.fetch_add(1, Ordering::Release);
            let should_flush = flush.should_flush(self.flush_batch_size, self.flush_interval);
            (outcome, expired, should_flush)
        };
        if should_flush {
            self.flush()
                .await
                .map_err(|error| ApplyError::Storage(error.to_string()))?;
        }
        Ok(Self::reported_outcome(outcome, expired))
    }

    /// Called synchronously while cache.write and the publication fence are
    /// held, before exposing the winning cache update or awaiting persistence.
    fn publish_summary(&self, key: &str, value: &VersionedValue, digests: CachedDigests) {
        let mut index = self.summary_index.write().unwrap();
        index
            .values
            .insert(key.to_string(), SummaryValue::new(value, digests));
        index.revision = index.revision.wrapping_add(1);
    }

    fn check_version_conflict(
        cache: &FragmentCache,
        key: &str,
        value: &VersionedValue,
    ) -> Result<(), ApplyError> {
        if cache
            .values
            .get(key)
            .is_some_and(|existing| existing.same_version(value) && existing != value)
        {
            return Err(ApplyError::VersionConflict {
                key: key.to_string(),
            });
        }
        Ok(())
    }

    fn merge_cache(
        cache: &mut FragmentCache,
        key: &str,
        value: &VersionedValue,
        digests: CachedDigests,
    ) -> Result<ApplyOutcome, ApplyError> {
        if let Some(existing) = cache.values.get_mut(key) {
            if existing.same_version(value) {
                return if existing == value {
                    Ok(ApplyOutcome::AlreadyPresent)
                } else {
                    Err(ApplyError::VersionConflict {
                        key: key.to_string(),
                    })
                };
            }
            if !value.wins_over(existing) {
                return Ok(ApplyOutcome::Superseded {
                    timestamp_ms: existing.timestamp_ms,
                    logical_counter: existing.logical_counter,
                    node_id: existing.node_id.clone(),
                    is_tombstone: existing.is_tombstone,
                });
            }
            if !existing.is_tombstone {
                if let Some(deadline) = existing.expires_at_ms {
                    cache.expiry_deadlines.remove(&(deadline, key.to_owned()));
                }
            }
            if !value.is_tombstone {
                if let Some(deadline) = value.expires_at_ms {
                    cache.expiry_deadlines.insert((deadline, key.to_owned()));
                }
            }
            *existing = value.clone();
            cache.content_digests.insert(key.to_string(), digests);
            Ok(ApplyOutcome::Applied)
        } else {
            let key = key.to_string();
            cache.ordered_keys.insert(key.clone());
            cache.content_digests.insert(key.clone(), digests);
            if !value.is_tombstone {
                if let Some(deadline) = value.expires_at_ms {
                    cache.expiry_deadlines.insert((deadline, key.clone()));
                }
            }
            cache.values.insert(key, value.clone());
            Ok(ApplyOutcome::Applied)
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
    /// At most one write is in progress per table/cache partition. Partitions
    /// share the fragment record/byte allowance and node executor. The authoritative copy
    /// of the batch remains in the buffer's in-flight slot until the write
    /// succeeds, so failure or cancellation cannot make a later flush forget
    /// it. Writes that arrive during the store call remain pending for the
    /// next flush.
    pub async fn flush(&self) -> Result<(), ActorProcessingErr> {
        if self.partitioned {
            return self.flush_from_actor().await;
        }
        self.reclaim_empty_tables().await;
        self.flush_local().await
    }

    /// Managed actors resolve child tables even when public direct state
    /// operations deliberately retain unpartitioned base-cache semantics.
    pub(crate) async fn flush_from_actor(&self) -> Result<(), ActorProcessingErr> {
        self.reclaim_empty_tables().await;
        self.flush_local().await?;
        let children = self.table_children.read().await.clone();
        for child in children.values() {
            child.flush_local().await?;
        }
        drop(children);
        self.reclaim_empty_tables().await;
        Ok(())
    }

    async fn flush_local(&self) -> Result<(), ActorProcessingErr> {
        self.flush_owned(false).await.map(|_| ())
    }

    /// Flush and complete the backend's strongest stable-storage fence under
    /// the same ordering guard. The accepted operation remains owned when its
    /// waiter is canceled; safe teardown still waits for backend completion.
    pub async fn flush_stable(&self) -> Result<ApplyDurability, ActorProcessingErr> {
        if self.partitioned {
            return self.flush_stable_from_actor().await;
        }
        self.reclaim_empty_tables().await;
        self.flush_stable_local().await
    }

    pub(crate) async fn flush_stable_from_actor(
        &self,
    ) -> Result<ApplyDurability, ActorProcessingErr> {
        self.reclaim_empty_tables().await;
        let mut durability = self.flush_stable_local().await?;
        let children = self.table_children.read().await.clone();
        for child in children.values() {
            durability = match (durability, child.flush_stable_local().await?) {
                (ApplyDurability::MemoryApplied, _) | (_, ApplyDurability::MemoryApplied) => {
                    ApplyDurability::MemoryApplied
                }
                (ApplyDurability::StableCommitted, ApplyDurability::StableCommitted) => {
                    ApplyDurability::StableCommitted
                }
                _ => ApplyDurability::StoreCommitted,
            };
        }
        drop(children);
        self.reclaim_empty_tables().await;
        Ok(durability)
    }

    async fn flush_stable_local(&self) -> Result<ApplyDurability, ActorProcessingErr> {
        self.flush_owned(true).await
    }

    async fn flush_owned(&self, stable: bool) -> Result<ApplyDurability, ActorProcessingErr> {
        let ordering = self.flush_in_progress.clone();
        let guard = ordering.clone().lock_owned().await;
        let Some(kvstore) = self.kvstore.clone() else {
            return Ok(ApplyDurability::MemoryApplied);
        };
        let batch = self.flush.lock().await.begin_batch();
        if batch.is_none() && !stable {
            return Ok(ApplyDurability::StoreCommitted);
        }
        let flush = self.flush.clone();
        let executor = self.executor.clone();
        let (reply, completion) = tokio::sync::oneshot::channel();
        // Registration is synchronous after taking the authoritative batch.
        // The executor retains and reaps this owner. The ordering guard is
        // captured by the blocking call itself, including runtime teardown.
        let owner_executor = executor.clone();
        executor.spawn_owner(async move {
            let completed = owner_executor
                .run(move || {
                    // Keep the ordering guard outside the unwind boundary.
                    // A panicking backend must not let another flush replace
                    // the authoritative batch before this owner restores it.
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if let Some(batch) = &batch {
                            let encoded: Vec<_> = batch
                                .iter()
                                .map(|(key, write)| (key.as_bytes().to_vec(), write.value.clone()))
                                .collect();
                            kvstore.write_batch(&encoded)?;
                        }
                        if stable {
                            Ok(match kvstore.stable_fence()? {
                                StoreCommitDurability::Buffered => ApplyDurability::StoreCommitted,
                                StoreCommitDurability::Stable => ApplyDurability::StableCommitted,
                            })
                        } else {
                            Ok(ApplyDurability::StoreCommitted)
                        }
                    }))
                    .unwrap_or_else(|_| {
                        Err(crate::store::StoreError("storage backend panicked".into()))
                    });
                    drop(batch);
                    Ok((result, guard))
                })
                .await;
            let result = match completed {
                Ok((result, _ordering_guard)) => {
                    let mut flush = flush.lock().await;
                    if result.is_ok() {
                        flush.commit_batch();
                    } else {
                        flush.restore_batch();
                        flush.failed = true;
                        tracing::warn!(
                            records = flush.record_count(),
                            retained_bytes = flush.local_budget.used(),
                            "retaining persistence winners after storage failure"
                        );
                    }
                    result
                }
                Err(error) => {
                    // A canceled/not-started blocking job can drop its guard.
                    // Reacquire ordering before touching any batch: a newer
                    // flush may have started while this owner was waking.
                    let _ordering_guard = ordering.lock_owned().await;
                    let mut flush = flush.lock().await;
                    flush.restore_batch();
                    flush.failed = true;
                    Err(error)
                }
            };
            let _ = reply.send(result);
        });
        completion
            .await
            .map_err(|error| {
                ActorProcessingErr::from(format!("storage completion owner stopped: {error}"))
            })?
            .map_err(|error| ActorProcessingErr::from(error.to_string()))
    }

    /// Read a key, treating expired values and tombstones as absent.
    pub async fn get(&self, key: &str) -> Option<VersionedValue> {
        if self.partitioned {
            return self.get_from_actor(key).await;
        }
        self.get_local(key).await
    }

    pub(crate) async fn get_from_actor(&self, key: &str) -> Option<VersionedValue> {
        if let Some(table) = crate::table_key::table_name(key) {
            let child = self.table_children.read().await.get(&table).cloned()?;
            let value = child.get_local(key).await;
            drop(child);
            if value.is_none() {
                self.prune_empty_table(&table).await;
            }
            return value;
        }
        self.get_local(key).await
    }

    async fn get_local(&self, key: &str) -> Option<VersionedValue> {
        let now = now_ms();
        self.cache
            .read()
            .await
            .values
            .get(key)
            .filter(|v| !v.is_tombstone && !v.is_expired(now))
            .cloned()
    }

    /// Scan through bounded, revision-guarded pages in lexicographic order.
    ///
    /// The per-fragment offset and effective limit are preserved. Cache locks
    /// are released between pages. The complete scan fails on a concurrent
    /// mutation, one million visited records, or a 16 MiB result/work budget;
    /// partial results are never returned. Use key/prefix transforms for wide
    /// values. Already expired values and tombstones remain invisible.
    pub async fn scan(&self, spec: &ScanSpec) -> Result<Vec<ScanMatch>, String> {
        if self.partitioned {
            return self.scan_from_actor(spec).await;
        }
        self.scan_local(spec).await
    }

    pub(crate) async fn scan_from_actor(&self, spec: &ScanSpec) -> Result<Vec<ScanMatch>, String> {
        if matches!(spec.matcher, KeyMatcher::Prefix(_)) {
            // A table-scoped scan carries the table namespace in its
            // prefix. Route it to that table's independent cache/index.
            if let KeyMatcher::Prefix(prefix) = &spec.matcher {
                if let Ok(prefix) = std::str::from_utf8(prefix) {
                    if let Some(table) = crate::table_key::table_name(prefix) {
                        let child = self.table_children.read().await.get(&table).cloned();
                        if let Some(child) = child {
                            return child.scan_local_encoded(spec, Some(&table)).await;
                        }
                        // Preserve validation even for an absent relation.
                        let mut plan = spec.query_plan(None);
                        plan.table = table;
                        crate::query::validate_physical_plan(&plan)
                            .map_err(|error| error.to_string())?;
                        return Ok(Vec::new());
                    }
                }
            }
        }
        self.scan_local(spec).await
    }

    async fn scan_local(&self, spec: &ScanSpec) -> Result<Vec<ScanMatch>, String> {
        self.scan_local_encoded(spec, None).await
    }

    async fn scan_local_encoded(
        &self,
        spec: &ScanSpec,
        table: Option<&str>,
    ) -> Result<Vec<ScanMatch>, String> {
        use crate::proto::{QueryPageRequest, QueryPageStatus};
        use crate::query::{cell_to_value, QueryOptions, QueryPageWork};
        use actordb_query::QueryValue;
        let mut plan = spec.query_plan(None);
        if let Some(table) = table {
            plan.table = table.into();
        }
        let plan = Arc::new(plan);
        let options = QueryOptions::default();
        let mut req = QueryPageRequest {
            max_rows: 256,
            max_bytes: options.page_bytes as u32,
            max_visits: options.page_visits as u32,
            ..Default::default()
        };
        let mut out = Vec::new();
        let mut skipped = 0u64;
        let mut visited = 0u64;
        let mut retained = 0usize;
        // Reserve framing metadata before constructing owning result copies.
        let mut encoded = 4096usize;
        loop {
            let remaining = options.max_scanned_rows.saturating_sub(visited);
            if remaining == 0 {
                return Err("scan visited record budget exceeded".into());
            }
            req.max_visits = (options.page_visits as u64).min(remaining) as u32;
            req.max_rows = (spec
                .effective_limit()
                .saturating_sub(out.len() as u64)
                .saturating_add(spec.offset.saturating_sub(skipped)))
            .min(256) as u32;
            // Page projection clones remain bounded in addition to the retained
            // reply; reject a projected row before copying a full wide value.
            let work = if table.is_some() {
                QueryPageWork::physical(plan.clone(), req.clone())
                    .map_err(|error| error.to_string())?
            } else {
                QueryPageWork::new(plan.clone(), req.clone())
            };
            let page = self.query_page(&work).await;
            if page.status != QueryPageStatus::Ok as i32 {
                return Err(page.error);
            }
            visited += u64::from(page.visited);
            for row in page.rows {
                if skipped < spec.offset {
                    skipped += 1;
                    continue;
                }
                let mut cells = row.cells.into_iter().map(cell_to_value);
                let Some(Ok(QueryValue::Text(key))) = cells.next() else {
                    return Err("scan key projection".into());
                };
                let value = match cells.next() {
                    Some(Ok(QueryValue::Bytes(bytes))) => bytes,
                    None => Vec::new(),
                    _ => return Err("scan value projection".into()),
                };
                retained = retained
                    .saturating_add(key.capacity())
                    .saturating_add(value.capacity());
                let row_wire = crate::wire::bytes_field_len(1, key.len())
                    .saturating_add(crate::wire::bytes_field_len(2, value.len()));
                encoded = encoded.saturating_add(crate::wire::message_field_len(2, row_wire));
                // Vec growth can allocate up to twice the next cardinality.
                let storage = retained.saturating_add(
                    (out.len() + 1)
                        .saturating_mul(2)
                        .saturating_mul(std::mem::size_of::<ScanMatch>()),
                );
                if storage > options.max_result_bytes || encoded > crate::wire::MAX_FRAME_BYTES {
                    return Err("scan result byte budget exceeded".into());
                }
                out.push(ScanMatch { key, value });
            }
            if page.done || out.len() as u64 == spec.effective_limit() {
                // Fence the final accepted contribution, including a truncated
                // page. Holding no lock across awaits avoids long read stalls.
                req.continuation = true;
                req.revision = page.revision;
                req.snapshot_time_ms = page.snapshot_time_ms;
                req.incarnation = page.incarnation;
                req.after_key = page.next_after_key;
                req.verify_only = true;
                let work = if table.is_some() {
                    QueryPageWork::physical(plan, req).map_err(|error| error.to_string())?
                } else {
                    QueryPageWork::new(plan, req)
                };
                let verify = self.query_page(&work).await;
                if verify.status != QueryPageStatus::Ok as i32 {
                    return Err(verify.error);
                }
                return Ok(out);
            }
            req.continuation = true;
            req.revision = page.revision;
            req.snapshot_time_ms = page.snapshot_time_ms;
            req.incarnation = page.incarnation;
            req.after_key = page.next_after_key;
            tokio::task::yield_now().await;
        }
    }

    /// Read a revision-guarded page through the background worker pool.
    /// Cursor progression includes filtered and invisible records, so sparse
    /// predicates cannot turn a page into an unbounded cache-lock pass.
    pub async fn query_page(
        &self,
        work: &crate::query::QueryPageWork,
    ) -> crate::proto::QueryPageResponse {
        if self.partitioned {
            return self.query_page_from_actor(work).await;
        }
        self.query_page_local(work).await
    }

    pub(crate) async fn query_page_from_actor(
        &self,
        work: &crate::query::QueryPageWork,
    ) -> crate::proto::QueryPageResponse {
        let table = work.plan.table.to_ascii_uppercase();
        let child = self.table_children.read().await.get(&table).cloned();
        if let Some(child) = child {
            if table == "KV" || !child.cache.read().await.values.is_empty() {
                return child.query_page_local(work).await;
            }
            drop(child);
            self.prune_empty_table(&table).await;
        }
        self.query_page_absent(work)
    }

    /// An unknown relation has a stable empty identity, not a cached entry.
    /// A later first record selects its child incarnation and invalidates an
    /// old absence continuation. Other tables' mutations do not invalidate it.
    fn query_page_absent(
        &self,
        work: &crate::query::QueryPageWork,
    ) -> crate::proto::QueryPageResponse {
        use crate::proto::{QueryPageRow, QueryPageStatus};
        use crate::query::{page_encoded_len, page_error, value_to_cell};
        let (_, _, max_bytes) = match query_page_limits(work) {
            Ok(limits) => limits,
            Err(error) => return page_error(&work.request, QueryPageStatus::Invalid, error),
        };
        let req = &work.request;
        if req.continuation && (req.revision != 0 || req.incarnation != self.query_incarnation) {
            return page_error(
                req,
                QueryPageStatus::Changed,
                "table changed since its absence snapshot",
            );
        }
        let mut page = crate::proto::QueryPageResponse {
            req_id: req.req_id,
            fragment: req.fragment,
            next_after_key: req.after_key.clone(),
            done: true,
            revision: 0,
            incarnation: self.query_incarnation,
            snapshot_time_ms: if req.continuation {
                req.snapshot_time_ms
            } else {
                now_ms()
            },
            status: QueryPageStatus::Ok as i32,
            ..Default::default()
        };
        if !req.verify_only && work.plan.matches_fragment(req.fragment) && work.plan.is_aggregate()
        {
            page.rows = actordb_query::QueryAccumulator::new_with_limits(
                &work.plan,
                work.validation_limits,
            )
            .finish()
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
                "empty aggregate exceeds query page budget",
            );
        }
        page
    }

    async fn query_page_local(
        &self,
        work: &crate::query::QueryPageWork,
    ) -> crate::proto::QueryPageResponse {
        use crate::proto::{QueryPageRow, QueryPageStatus};
        use crate::query::{page_encoded_len, page_error, value_to_cell, QUERY_WORK_BYTES};
        use actordb_query::{AggregateInput, Projection, QueryAccumulator};
        let req = &work.request;
        let plan = &work.plan;
        let (max_rows, max_visits, max_bytes) = match query_page_limits(work) {
            Ok(limits) => limits,
            Err(error) => return page_error(req, QueryPageStatus::Invalid, error),
        };
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
        let bounds = crate::query::query_key_bounds(
            plan,
            req.continuation.then_some(req.after_key.as_str()),
        )
        .unwrap_or_else(|| {
            (
                std::ops::Bound::Included(String::new()),
                std::ops::Bound::Excluded(String::new()),
            )
        });
        let mut keys = cache.ordered_keys.range(bounds).peekable();
        let mut accumulator = QueryAccumulator::new_with_limits(plan, work.validation_limits);
        let empty_metadata = actordb_query::RecordMetadata::default();
        let needs_metadata = plan.needs_metadata();
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
            let metadata = needs_metadata.then(|| actordb_query::RecordMetadata {
                timestamp_ms: value.timestamp_ms,
                logical_counter: value.logical_counter,
                node_id: value.node_id.clone(),
                expires_at_ms: value.expires_at_ms,
                is_tombstone: value.is_tombstone,
            });
            let metadata_ref = metadata.as_ref().unwrap_or(&empty_metadata);
            let matched = (!value.is_tombstone || plan.include_tombstones)
                && (!value.is_expired(snapshot_time_ms) || value.is_tombstone)
                && plan.matches_key(key)
                && plan.matches_metadata(metadata_ref);
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
                        Projection::NodeId => value.node_id.len(),
                        _ => 0,
                    })
                    .sum::<usize>()
            } else {
                0
            };
            let metadata_work = if needs_metadata {
                value.node_id.len()
            } else {
                0
            };
            let record_work = key
                .len()
                .saturating_mul(2)
                .saturating_add(value_work)
                .saturating_add(metadata_work)
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
                    metadata_ref,
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
                let cells =
                    match plan.project_with_metadata(key, &value.value, req.fragment, metadata_ref)
                    {
                        Ok(values) => values.into_iter().map(value_to_cell).collect(),
                        Err(error) => {
                            return page_error(req, QueryPageStatus::Invalid, error.to_string());
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
                    if let Err(error) = accumulator.push_with_metadata(
                        key,
                        &value.value,
                        req.fragment,
                        metadata_ref,
                    ) {
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
    ///
    /// A partitioned live fragment includes every table and excludes cache
    /// publication while cloning this explicitly full diagnostic snapshot.
    /// It can allocate the resident dataset and delay writers; reconciliation
    /// uses bounded [`Self::summary_page`] instead. A standalone unpartitioned
    /// state retains its direct base-cache view and local mutation revision.
    pub async fn snapshot_with_revision(&self) -> (HashMap<String, VersionedValue>, u64) {
        self.reclaim_empty_tables().await;
        if self.partitioned {
            let result = {
                // Only this explicitly full diagnostic operation excludes
                // cross-table cache publication. Never fence backend I/O.
                let _snapshot = self.snapshot_fence.write().await;
                let snapshot_time_ms = now_ms();
                let mut snapshot = self.snapshot_local(snapshot_time_ms).await.0;
                let children = self.table_children.read().await.clone();
                for child in children.values() {
                    snapshot.extend(child.snapshot_local(snapshot_time_ms).await.0);
                }
                let revision = self.summary_index.read().unwrap().revision;
                (snapshot, revision)
            };
            self.reclaim_empty_tables().await;
            return result;
        }
        self.snapshot_local(now_ms()).await
    }

    async fn snapshot_local(
        &self,
        snapshot_time_ms: u64,
    ) -> (HashMap<String, VersionedValue>, u64) {
        let cache = self.cache.read().await;
        let snapshot = cache
            .values
            .iter()
            .map(|(key, value)| {
                let value = if value.is_expired(snapshot_time_ms) {
                    value.expiry_tombstone()
                } else {
                    value.clone()
                };
                (key.clone(), value)
            })
            .collect();
        #[cfg(test)]
        {
            let pause = self.snapshot_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.captured.notify_one();
                pause.resume.notified().await;
            }
        }
        // Winning writers advance this counter before releasing cache.write.
        // Pair it with values under the same read guard, never after cloning.
        (snapshot, self.revision.load(Ordering::Acquire))
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
        if self.partitioned {
            return self
                .summary_page_partitioned(after_key, expected_revision, snapshot_time_ms, limit)
                .await;
        }
        self.summary_page_local(after_key, expected_revision, snapshot_time_ms, limit)
            .await
    }

    async fn summary_page_partitioned(
        &self,
        after_key: Option<&str>,
        expected_revision: Option<u64>,
        snapshot_time_ms: Option<u64>,
        limit: usize,
    ) -> SummaryPage {
        use std::ops::Bound::{Excluded, Unbounded};

        let index = self.summary_index.read().unwrap();
        let revision = index.revision;
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
        // Physical-key ordering is not table-name ordering (prefix delimiters
        // matter). The shared index directly seeks the caller's exact cursor.
        let lower = after_key.map_or(Unbounded, Excluded);
        let limit = limit.max(1);
        let mut summaries = Vec::with_capacity(limit.min(256));
        let mut payload_bytes = 0usize;
        let mut done = true;
        for (key, value) in index.values.range::<str, _>((lower, Unbounded)) {
            if summaries.len() == limit {
                done = false;
                break;
            }
            let summary_bytes = key
                .len()
                .saturating_add(value.node_id.len())
                .saturating_add(32);
            if !summaries.is_empty()
                && payload_bytes.saturating_add(summary_bytes) > SUMMARY_PAGE_TARGET_BYTES
            {
                done = false;
                break;
            }
            summaries.push(value.summary(key, snapshot_time_ms));
            payload_bytes = payload_bytes.saturating_add(summary_bytes);
        }
        SummaryPage {
            summaries,
            revision,
            snapshot_time_ms,
            done,
            restart: false,
        }
    }

    async fn summary_page_local(
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
                let summary_bytes = key
                    .len()
                    .saturating_add(value.node_id.len())
                    .saturating_add(32);
                if !summaries.is_empty()
                    && payload_bytes.saturating_add(summary_bytes) > SUMMARY_PAGE_TARGET_BYTES
                {
                    done = false;
                    break;
                }
                summaries.push(Self::snapshot_summary(
                    key,
                    value,
                    cache.content_digests[key],
                    snapshot_time_ms,
                ));
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
        if self.partitioned {
            return self.get_versioned_from_actor(key, snapshot_time_ms).await;
        }
        self.get_versioned_local(key, snapshot_time_ms).await
    }

    pub(crate) async fn get_versioned_from_actor(
        &self,
        key: &str,
        snapshot_time_ms: Option<u64>,
    ) -> Option<VersionedValue> {
        if let Some(table) = crate::table_key::table_name(key) {
            let child = self.table_children.read().await.get(&table).cloned()?;
            let value = child.get_versioned_local(key, snapshot_time_ms).await;
            drop(child);
            if value.is_none() {
                self.prune_empty_table(&table).await;
            }
            return value;
        }
        self.get_versioned_local(key, snapshot_time_ms).await
    }

    async fn get_versioned_local(
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
        if self.partitioned {
            let snapshot_time_ms = snapshot_time_ms.unwrap_or_else(now_ms);
            let mut next_index = start_index.min(keys.len());
            let mut payload_bytes = 0usize;
            let mut entries = Vec::new();
            let mut missing = false;
            while let Some(key) = keys.get(next_index) {
                let Some(value) = self.get_versioned(key, Some(snapshot_time_ms)).await else {
                    missing = true;
                    next_index += 1;
                    continue;
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
            return VersionedPage {
                entries,
                next_index,
                missing,
            };
        }
        self.get_versioned_page_local(keys, start_index, snapshot_time_ms)
            .await
    }

    async fn get_versioned_page_local(
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
        digests: CachedDigests,
        snapshot_time_ms: u64,
    ) -> SnapshotSummary {
        SnapshotSummary {
            content_digest: if value.is_expired(snapshot_time_ms) {
                digests.expired
            } else {
                digests.live
            },
            key: key.to_string(),
            timestamp_ms: value.timestamp_ms,
            logical_counter: value.logical_counter,
            node_id: value.node_id.clone(),
            is_tombstone: value.is_tombstone || value.is_expired(snapshot_time_ms),
        }
    }

    /// Read the mutation revision at the cache publication boundary.
    ///
    /// Writers advance the revision while holding the cache write lock, so
    /// taking the corresponding read lock fences local cache publication.
    /// Partitioned fragments read the shared metadata revision without walking
    /// tables or waiting for child caches/backend work. Metadata and revision
    /// are published together before any winning cache lock is released.
    pub async fn current_revision(&self) -> u64 {
        if self.partitioned {
            return self.summary_index.read().unwrap().revision;
        }
        self.current_revision_local().await
    }

    async fn current_revision_local(&self) -> u64 {
        let _cache = self.cache.read().await;
        self.revision.load(Ordering::Acquire)
    }

    /// Convert all due live values to durable expiry tombstones using the
    /// deadline index. Non-TTL values and retained tombstones are never visited.
    /// Locks are released between bounded pages; concurrent writes may change
    /// the finite pass. Persistence admission failure keeps the deadline indexed
    /// and returns an error so shutdown can retry its final expiry fence.
    pub async fn expire_values(&self, now_ms: u64) -> Result<usize, ActorProcessingErr> {
        if self.partitioned {
            return self.expire_values_from_actor(now_ms).await;
        }
        self.reclaim_empty_tables().await;
        self.expire_values_local(now_ms).await
    }

    pub(crate) async fn expire_values_from_actor(
        &self,
        now_ms: u64,
    ) -> Result<usize, ActorProcessingErr> {
        self.reclaim_empty_tables().await;
        {
            // This is an explicitly full/manual or terminal pass, not one
            // bounded automatic Cleaner page. A different dirty table can
            // own the shared allowance needed for this table's first expiry
            // marker. Retry those authoritative batches before expiry, or a
            // healthy shutdown retry could forever saturate the empty table
            // without reaching the failed dirty partition's final flush.
            self.flush_from_actor().await?;
            let mut pass = ExpiryPass::new(now_ms);
            pass.retry_saturation = true;
            let mut converted = 0usize;
            let mut retried_without_conversion = false;
            loop {
                let page = self.expire_values_page_partitioned(&mut pass).await?;
                converted = converted.saturating_add(page.converted);
                if page.saturated {
                    // Retry the same deadline after freeing other tables'
                    // pending/active charges. An irreducible marker or ongoing
                    // contention gets one zero-progress retry, never a spin.
                    if page.converted == 0 && retried_without_conversion {
                        return Err(ActorProcessingErr::from(
                            "expiry persistence admission remains saturated after full flush",
                        ));
                    }
                    retried_without_conversion = page.converted == 0;
                    // The bounded page may already have flushed its own
                    // converted markers. Do not walk every table again for
                    // each such page when no shared backlog remains.
                    if self.persistence.records.used() != 0 || self.persistence.bytes.used() != 0 {
                        self.flush_from_actor().await?;
                    }
                } else {
                    retried_without_conversion = false;
                }
                if page.done {
                    self.flush_from_actor().await?;
                    self.reclaim_empty_tables().await;
                    return Ok(converted);
                }
                tokio::task::yield_now().await;
            }
        }
    }

    async fn expire_values_local(&self, now_ms: u64) -> Result<usize, ActorProcessingErr> {
        let mut pass = ExpiryPass::new(now_ms);
        let mut converted = 0usize;
        let mut saturated = false;
        loop {
            let page = self.expire_values_page_local(&mut pass).await?;
            converted = converted.saturating_add(page.converted);
            saturated |= page.saturated;
            if page.done {
                break;
            }
            tokio::task::yield_now().await;
        }
        if saturated {
            return Err(ActorProcessingErr::from(
                "expiry persistence admission saturated; retry remaining expirations",
            ));
        }
        Ok(converted)
    }

    /// Visit at most 256 deadlines/table entries across the entire fragment.
    /// Table traversal also consumes the aggregate work allowance, so empty
    /// namespaces cannot turn one automatic message into an unbounded scan.
    /// One terminal record may cross the soft work-byte target. The pass pins
    /// its expiry time and retains at most one child across messages.
    pub(crate) async fn expire_values_page(
        &self,
        pass: &mut ExpiryPass,
    ) -> Result<ExpiryPage, ActorProcessingErr> {
        if !self.partitioned {
            return self.expire_values_page_local(pass).await;
        }
        self.expire_values_page_partitioned(pass).await
    }

    async fn expire_values_page_partitioned(
        &self,
        pass: &mut ExpiryPass,
    ) -> Result<ExpiryPage, ActorProcessingErr> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut page = ExpiryPage {
            visited: 0,
            converted: 0,
            work_bytes: 0,
            done: pass.partition_done,
            saturated: false,
        };
        if pass.partition_done {
            return Ok(page);
        }
        if pass.tables_remaining.is_none() {
            pass.tables_remaining = Some(self.table_children.read().await.len());
        }
        while page.visited < EXPIRY_PAGE_VISIT_LIMIT && page.work_bytes < EXPIRY_PAGE_WORK_BYTES {
            let next = if !pass.root_done {
                self.expire_values_page_local_budget(
                    pass,
                    EXPIRY_PAGE_VISIT_LIMIT - page.visited,
                    EXPIRY_PAGE_WORK_BYTES - page.work_bytes,
                )
                .await?
            } else {
                if pass.active_table.is_none() {
                    if pass.tables_remaining == Some(0) {
                        pass.partition_done = true;
                        break;
                    }
                    let next = {
                        let children = self.table_children.read().await;
                        let lower = pass.after_table.as_ref().map_or(Unbounded, Excluded);
                        children
                            .range::<String, _>((lower, Unbounded))
                            .next()
                            .map(|(name, child)| (name.clone(), child.clone()))
                    };
                    let Some((name, child)) = next else {
                        pass.partition_done = true;
                        break;
                    };
                    page.visited += 1;
                    page.work_bytes = page.work_bytes.saturating_add(
                        std::mem::size_of::<(String, Arc<FragmentState>)>()
                            .saturating_add(name.capacity()),
                    );
                    pass.after_table = Some(name);
                    pass.active_table = Some(child);
                    *pass
                        .tables_remaining
                        .as_mut()
                        .expect("table quota initialized") -= 1;
                    if page.visited == EXPIRY_PAGE_VISIT_LIMIT
                        || page.work_bytes >= EXPIRY_PAGE_WORK_BYTES
                    {
                        break;
                    }
                }
                let child = pass.active_table.as_ref().expect("selected child").clone();
                child
                    .expire_values_page_local_budget(
                        pass,
                        EXPIRY_PAGE_VISIT_LIMIT - page.visited,
                        EXPIRY_PAGE_WORK_BYTES - page.work_bytes,
                    )
                    .await?
            };
            page.visited += next.visited;
            page.converted = page.converted.saturating_add(next.converted);
            page.work_bytes = page.work_bytes.saturating_add(next.work_bytes);
            page.saturated |= next.saturated;
            if next.done {
                if pass.root_done {
                    pass.active_table = None;
                } else {
                    pass.root_done = true;
                }
                pass.reset_deadlines();
            }
            if next.saturated && pass.retry_saturation {
                // The explicit driver can free another table's shared charge
                // outside the bounded page before retrying this same deadline.
                break;
            }
            if pass.root_done && pass.active_table.is_none() && pass.tables_remaining == Some(0) {
                pass.partition_done = true;
                break;
            }
        }
        page.done = pass.partition_done;
        Ok(page)
    }

    async fn expire_values_page_local(
        &self,
        pass: &mut ExpiryPass,
    ) -> Result<ExpiryPage, ActorProcessingErr> {
        self.expire_values_page_local_budget(pass, EXPIRY_PAGE_VISIT_LIMIT, EXPIRY_PAGE_WORK_BYTES)
            .await
    }

    async fn expire_values_page_local_budget(
        &self,
        pass: &mut ExpiryPass,
        visit_limit: usize,
        work_bytes: usize,
    ) -> Result<ExpiryPage, ActorProcessingErr> {
        use std::ops::Bound::{Excluded, Unbounded};
        let (page, should_flush) = {
            // Shared ordering with apply: reserve persistence before installing
            // a tombstone, then publish its revision/callback without an await.
            let mut flush = if self.kvstore.is_some() {
                Some(self.flush.lock().await)
            } else {
                None
            };
            let _publication = self.snapshot_fence.read().await;
            let mut cache = self.cache.write().await;
            let mut summary_index = self.summary_index.write().unwrap();
            if !pass.initialized {
                pass.remaining = cache.expiry_deadlines.len();
                pass.initialized = true;
            }
            let mut page = ExpiryPage {
                visited: 0,
                converted: 0,
                work_bytes: 0,
                done: pass.remaining == 0,
                saturated: false,
            };
            let limit = pass.remaining.min(visit_limit);
            while page.visited < limit && (page.visited == 0 || page.work_bytes < work_bytes) {
                let lower = pass.after_deadline.as_ref().map_or(Unbounded, Excluded);
                let Some(entry) = cache
                    .expiry_deadlines
                    .range((lower, Unbounded))
                    .next()
                    .filter(|(deadline, _)| *deadline <= pass.now_ms)
                    .cloned()
                else {
                    page.done = true;
                    break;
                };
                let (deadline, key) = &entry;
                let value = cache
                    .values
                    .get_mut(key)
                    .expect("indexed deadline has a value");
                debug_assert_eq!(value.expires_at_ms, Some(*deadline));
                debug_assert!(value.is_expired(pass.now_ms));
                let charge = expiry_record_work_bytes(key, value, true, flush.is_some())
                    .saturating_add(std::mem::size_of::<(u64, String)>());
                page.work_bytes = page.work_bytes.saturating_add(charge);
                page.visited += 1;
                let tombstone = value.expiry_tombstone();
                if flush
                    .as_mut()
                    .is_some_and(|flush| !flush.enqueue(key, &tombstone))
                {
                    page.saturated = true;
                    if pass.retry_saturation {
                        // The explicit caller will flush shared dirty batches
                        // outside this bounded page and retry this exact key.
                        // A failed attempt charges work but consumes no cursor
                        // or original deadline quota.
                        break;
                    }
                    pass.after_deadline = Some(entry);
                    continue;
                }
                let previous_value = self.install_expiry(value, tombstone);
                let summary = summary_index
                    .values
                    .get_mut(key)
                    .expect("cache summary exists");
                summary.is_tombstone = value.is_tombstone;
                summary.expires_at_ms = value.expires_at_ms;
                summary.digests.expire();
                cache
                    .content_digests
                    .get_mut(key)
                    .expect("cache digest exists")
                    .expire();
                if let Some(previous_value) = previous_value {
                    self.publish_expiry(key, previous_value);
                }
                cache.expiry_deadlines.remove(&entry);
                pass.after_deadline = Some(entry);
                page.converted += 1;
            }
            let consumed = page.visited - usize::from(page.saturated && pass.retry_saturation);
            pass.remaining -= consumed;
            page.done |= pass.remaining == 0;
            // A quota larger than the due set must finish on this page, even
            // when the page ends exactly at a visit/work boundary.
            if !page.done {
                let lower = pass.after_deadline.as_ref().map_or(Unbounded, Excluded);
                page.done = cache
                    .expiry_deadlines
                    .range((lower, Unbounded))
                    .next()
                    .is_none_or(|(deadline, _)| *deadline > pass.now_ms);
            }
            if page.done {
                pass.remaining = 0;
            }
            if page.converted != 0 {
                summary_index.revision = summary_index
                    .revision
                    .wrapping_add(u64::try_from(page.converted).unwrap_or(u64::MAX));
                self.revision.fetch_add(
                    u64::try_from(page.converted).unwrap_or(u64::MAX),
                    Ordering::Release,
                );
            }
            let should_flush = page.saturated
                || (page.converted > 0
                    && flush.as_ref().is_some_and(|flush| {
                        flush.should_flush(self.flush_batch_size, self.flush_interval)
                    }));
            (page, should_flush)
        };
        if should_flush {
            self.flush_local().await?;
        }
        Ok(page)
    }
}

#[cfg(test)]
#[path = "summary_revision_tests.rs"]
mod summary_revision_tests;

impl FlushBuffer {
    fn should_flush(&self, batch_size: usize, interval: Duration) -> bool {
        let elapsed = self.last_flush.elapsed();
        elapsed >= interval || (!self.failed && self.record_count() >= batch_size)
    }
    fn record_count(&self) -> usize {
        self.pending.len() + self.in_flight.as_ref().map_or(0, |batch| batch.len())
    }
    /// Reserve only the growth delta when replacing a pending winner. Failed
    /// admission leaves the old value and both accounting reservations intact.
    fn enqueue(&mut self, key: &str, value: &VersionedValue) -> bool {
        let bytes = persistence_record_bytes(key, value);
        if let Some(existing) = self.pending.get_mut(key) {
            let previous = existing.local.bytes();
            if !existing.local.resize(bytes) {
                return false;
            }
            if !existing.aggregate.resize(bytes) {
                assert!(existing.local.resize(previous));
                return false;
            }
            existing.value = value.clone();
            return true;
        }
        let Some(record) = self.record_budget.reserve(1) else {
            return false;
        };
        let Some(local) = self.local_budget.reserve(bytes) else {
            return false;
        };
        let Some(aggregate) = self.aggregate_budget.reserve(bytes) else {
            return false;
        };
        self.pending.insert(
            key.to_string(),
            PendingWrite {
                value: value.clone(),
                local,
                aggregate,
                _record: record,
            },
        );
        true
    }
    fn restore_batch(&mut self) {
        if let Some(batch) = self.in_flight.take() {
            let batch = Arc::try_unwrap(batch)
                .unwrap_or_else(|_| panic!("active persistence batch still owned"));
            for (key, write) in batch {
                match self.pending.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if write.value.wins_over(&entry.get().value) {
                            entry.insert(write);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(write);
                    }
                }
            }
        }
    }
    fn begin_batch(&mut self) -> Option<Arc<HashMap<String, PendingWrite>>> {
        // An abandoned waiter may leave a completed attempt recorded. The
        // fragment ordering mutex proves there is no active store call here.
        self.restore_batch();
        if self.pending.is_empty() {
            return None;
        }
        let batch = Arc::new(std::mem::take(&mut self.pending));
        self.in_flight = Some(batch.clone());
        self.last_flush = Instant::now();
        Some(batch)
    }
    fn commit_batch(&mut self) {
        self.failed = false;
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
#[path = "fragment_persistence_tests.rs"]
mod persistence_limit_tests;

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
        fn bind(
            &self,
            _identity: &crate::store::StoreIdentity,
        ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
            Ok(crate::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            crate::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<crate::store::StoreCommitDurability, crate::store::StoreError> {
            Ok(self.commit_durability())
        }
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
    async fn failing_store_coalesces_hot_key_and_rejects_before_cache_mutation() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(1000),
            written: StdMutex::new(Vec::new()),
        });
        let config = PersistenceConfig::builder()
            .max_records(2)
            .max_bytes(4096)
            .node_max_bytes(4096)
            .build()
            .for_runtime()
            .unwrap();
        let state = FragmentState::new(
            HashMap::new(),
            Some(store.clone()),
            1,
            Duration::from_secs(3600),
        )
        .with_persistence_config(config)
        .unwrap();
        for version in 1..=100 {
            let value = VersionedValue::new(vec![1; 256], version, "node".into());
            let applied = state.apply(&op("hot", value)).await;
            assert!(applied.is_ok() || matches!(applied, Err(ApplyError::Storage(_))));
            assert!(state.flush().await.is_err());
            assert_eq!(state.persistence_backlog().await.0, 1);
            assert!(state.persistence_backlog().await.1 <= 4096);
        }
        assert!(matches!(
            state
                .apply(&op(
                    "other",
                    VersionedValue::new(vec![2; 256], 101, "node".into())
                ))
                .await,
            Ok(ApplyOutcome::Applied) | Err(ApplyError::Storage(_))
        ));
        let before = state.current_revision().await;
        assert_eq!(
            state
                .apply(&op(
                    "rejected",
                    VersionedValue::new(vec![3; 256], 102, "node".into())
                ))
                .await,
            Err(ApplyError::Backpressure)
        );
        assert!(state.get("rejected").await.is_none());
        assert_eq!(state.current_revision().await, before);
        assert_eq!(
            state
                .apply(&op(
                    "hot",
                    VersionedValue::new(vec![4; 2048], 103, "node".into())
                ))
                .await,
            Err(ApplyError::Backpressure)
        );
        assert_eq!(state.get("hot").await.unwrap().timestamp_ms, 100);
        *store.failures_left.lock().unwrap() = 0;
        state.flush().await.unwrap();
        assert_eq!(state.persistence_backlog().await, (0, 0));
        let written = store.written.lock().unwrap();
        assert_eq!(written.len(), 2);
        assert!(written
            .iter()
            .any(|(key, value)| key == b"hot" && value.timestamp_ms == 100));
    }

    #[test]
    fn low_level_state_rejects_zero_storage_slots_before_async_work() {
        let configured = FragmentState::new(HashMap::new(), None, 1, Duration::from_secs(1))
            .with_persistence_config(PersistenceConfig::builder().storage_concurrency(0).build());
        assert!(configured.is_err());
    }

    #[tokio::test]
    async fn raw_expired_version_conflict_never_enters_persistence_backlog() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let mut original = VersionedValue::new(vec![1], 10, "writer".into());
        original.expires_at_ms = Some(1);
        let state = FragmentState::new(
            HashMap::from([("key".into(), original.clone())]),
            Some(store),
            100,
            Duration::from_secs(3600),
        );
        let mut conflict = original.clone();
        conflict.value = vec![2];
        assert!(matches!(
            state.apply(&op("key", conflict)).await,
            Err(ApplyError::VersionConflict { .. })
        ));
        assert_eq!(state.persistence_backlog().await, (0, 0));
        let cache = state.cache.read().await;
        assert_eq!(cache.values["key"], original);
        assert_eq!(cache.content_digests["key"].live, original.content_digest());
        assert_eq!(state.revision.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn saturated_expiry_keeps_live_cached_digest_until_marker_admission() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let mut expiring = VersionedValue::new(vec![7], 10, "writer".into());
        expiring.expires_at_ms = Some(500);
        let config = PersistenceConfig::builder().max_records(1).build();
        let state = FragmentState::new(
            HashMap::from([("ttl".into(), expiring.clone())]),
            Some(store),
            usize::MAX,
            Duration::from_secs(3600),
        )
        .with_persistence_config(config)
        .unwrap();
        state
            .apply(&op(
                "occupied",
                VersionedValue::new(vec![1], 20, "writer".into()),
            ))
            .await
            .unwrap();
        let page = state
            .expire_values_page(&mut ExpiryPass::new(500))
            .await
            .unwrap();
        assert_eq!(page.converted, 0);
        let cache = state.cache.read().await;
        assert_eq!(cache.values["ttl"], expiring);
        assert_eq!(cache.content_digests["ttl"].live, expiring.content_digest());
    }

    #[tokio::test]
    async fn aggregate_persistence_budget_bounds_multiple_fragments() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let value = VersionedValue::new(vec![1; 256], 1, "node".into());
        let charge = persistence_record_bytes("key", &value);
        let config = PersistenceConfig::builder()
            .max_bytes(charge * 2)
            .node_max_bytes(charge)
            .build()
            .for_runtime()
            .unwrap();
        let first = FragmentState::new(
            HashMap::new(),
            Some(store.clone()),
            100,
            Duration::from_secs(3600),
        )
        .with_persistence_config(config.clone())
        .unwrap();
        let second =
            FragmentState::new(HashMap::new(), Some(store), 100, Duration::from_secs(3600))
                .with_persistence_config(config.clone())
                .unwrap();
        first.apply(&op("key", value.clone())).await.unwrap();
        assert_eq!(
            second.apply(&op("key", value.clone())).await,
            Err(ApplyError::Backpressure)
        );
        first.flush().await.unwrap();
        second.apply(&op("key", value)).await.unwrap();
        assert_eq!(config.retained_bytes(), charge);
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
                logical_counter: winner.logical_counter,
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
        fn bind(
            &self,
            _identity: &crate::store::StoreIdentity,
        ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
            Ok(crate::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            crate::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<crate::store::StoreCommitDurability, crate::store::StoreError> {
            Ok(self.commit_durability())
        }
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

    struct GatedPanicStore {
        calls: AtomicUsize,
        release: (StdMutex<bool>, Condvar),
        stored: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
    }
    impl KvStore for GatedPanicStore {
        fn bind(
            &self,
            _identity: &crate::store::StoreIdentity,
        ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
            Ok(crate::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            crate::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<crate::store::StoreCommitDurability, crate::store::StoreError> {
            Ok(self.commit_durability())
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            if self.calls.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                let (released, wake) = &self.release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
                drop(released);
                panic!("gated backend panic");
            }
            self.stored.lock().unwrap().extend(writes.iter().cloned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn backend_panic_retains_ordering_until_authoritative_batch_is_restored() {
        let store = Arc::new(GatedPanicStore {
            calls: AtomicUsize::new(0),
            release: (StdMutex::new(false), Condvar::new()),
            stored: StdMutex::new(HashMap::new()),
        });
        let config = PersistenceConfig::builder().storage_concurrency(1).build();
        let state = Arc::new(
            FragmentState::new(
                HashMap::new(),
                Some(store.clone()),
                100,
                Duration::from_secs(3600),
            )
            .with_persistence_config(config.clone())
            .unwrap(),
        );
        let winner = VersionedValue::new(vec![7], 100, "node".into());
        state.apply(&op("key", winner.clone())).await.unwrap();
        let writing = state.clone();
        let writer = tokio::spawn(async move { writing.flush().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.calls.load(AtomicOrdering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let buffer = state.flush.lock().await;
        *store.release.0.lock().unwrap() = true;
        store.release.1.notify_all();
        tokio::time::timeout(Duration::from_secs(2), async {
            while config.active_storage_jobs() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            state.flush_in_progress.try_lock().is_err(),
            "backend panic released ordering before restoring its batch"
        );
        assert!(config.executor().owned_tasks() >= 1);
        drop(buffer);
        let error = tokio::time::timeout(Duration::from_secs(2), writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("storage backend panicked"));
        assert_eq!(state.persistence_backlog().await.0, 1);
        state.flush().await.unwrap();
        config.executor().drain().await;
        assert_eq!(state.persistence_backlog().await, (0, 0));
        assert_eq!(
            store.stored.lock().unwrap().get(b"key".as_slice()),
            Some(&winner)
        );
        assert_eq!(config.executor().owned_tasks(), 0);
    }

    #[tokio::test]
    async fn blocked_storage_keeps_current_thread_live_and_cancelled_flush_ordered() {
        let store = Arc::new(GatedStore {
            calls: AtomicUsize::new(0),
            release_first: (StdMutex::new(false), Condvar::new()),
            stored: StdMutex::new(HashMap::new()),
        });
        let config = PersistenceConfig::builder().storage_concurrency(1).build();
        let state = Arc::new(
            FragmentState::new(
                HashMap::new(),
                Some(store.clone()),
                100,
                Duration::from_secs(3600),
            )
            .with_persistence_config(config.clone())
            .unwrap(),
        );
        let old = VersionedValue::new(vec![1], 100, "node".into());
        let winner = VersionedValue::new(vec![2], 200, "node".into());
        state.apply(&op("key", old)).await.unwrap();
        let first_state = state.clone();
        let waiter = tokio::spawn(async move { first_state.flush().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.calls.load(AtomicOrdering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(config.active_storage_jobs(), 1);
        // This coroutine runs on the same current-thread runtime while the
        // synchronous store call is blocked on a separate executor thread.
        state.apply(&op("key", winner.clone())).await.unwrap();
        let next_state = state.clone();
        let next = tokio::spawn(async move { next_state.flush_stable().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(store.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(!next.is_finished());
        store.release_first();
        tokio::time::timeout(Duration::from_secs(2), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(state.persistence_backlog().await, (0, 0));
        assert_eq!(
            store.stored.lock().unwrap().get(b"key".as_slice()),
            Some(&winner)
        );
        assert_eq!(config.active_storage_jobs(), 0);
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
        eprintln!(
            "mode,keys,map_capacity,ttl_percent,materialization_us,total_through_drain_us,class,operations,stored_samples,p50_us,p95_us,p99_us,max_us"
        );
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
            eprintln!(
                "{mode:?},{cardinality},{map_capacity},{ttl_percent},{},{},{label},{operations},{},{},{},{},{}",
                materialization.as_micros(),
                total.as_micros(),
                samples.len(),
                quantile(50),
                quantile(95),
                quantile(99),
                quantile(100)
            );
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
        fn bind(
            &self,
            _identity: &crate::store::StoreIdentity,
        ) -> Result<crate::store::StoreBinding, crate::store::StoreError> {
            Ok(crate::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, crate::actor::VersionedValue)>, crate::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> crate::store::StoreCommitDurability {
            crate::store::StoreCommitDurability::Buffered
        }
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
    async fn expiry_pages_ignore_non_ttl_tombstones_and_future_deadlines() {
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
            (first.visited, first.converted, first.work_bytes, first.done),
            (0, 0, 0, true)
        );
        assert_eq!(state.revision.load(Ordering::Acquire), 0);
        assert_eq!(
            state.expire_values_page(&mut pass).await.unwrap().visited,
            0
        );
        let tombstones = (0..10_000)
            .map(|i| {
                (
                    format!("deleted:{i}"),
                    VersionedValue::tombstone(1, "n".into()),
                )
            })
            .collect();
        let state = test_state(tombstones);
        assert_eq!(
            state
                .expire_values_page(&mut ExpiryPass::new(u64::MAX))
                .await
                .unwrap()
                .visited,
            0
        );
        let state = test_state(HashMap::from([(
            "future".into(),
            expiring_value(vec![1], 101),
        )]));
        assert_eq!(
            state
                .expire_values_page(&mut ExpiryPass::new(100))
                .await
                .unwrap()
                .visited,
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
        assert_eq!(
            pass.after_deadline.as_ref().map(|(_, key)| key.as_str()),
            Some("")
        );
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
            oversized_charge + std::mem::size_of::<(u64, String)>()
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
        assert_eq!(
            pass.after_deadline.as_ref().map(|(_, key)| key.as_str()),
            Some("b")
        );
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
        assert_eq!((second.visited, second.converted), (256, 256));
        assert_eq!(state.revision.load(Ordering::Acquire), before_second + 256);
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
            let mut cache = state.cache.write().await;
            cache.expiry_deadlines.remove(&(u64::MAX, key.to_owned()));
            cache.values.get_mut(key).unwrap().expires_at_ms = Some(100);
            let digests = CachedDigests::new(&cache.values[key]);
            cache.content_digests.insert(key.to_owned(), digests);
            cache.expiry_deadlines.insert((100, key.to_owned()));
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
        assert_eq!(
            pass.after_deadline.as_ref().map(|(_, key)| key.as_str()),
            Some("key")
        );
        assert_eq!(
            state.cache.read().await.values["key"],
            value.expiry_tombstone()
        );
        assert_eq!(state.revision.load(Ordering::Acquire), 1);
        assert!(state.flush.lock().await.pending.contains_key("key"));
        let winner = VersionedValue::new(vec![2], 11, "writer".into());
        state.apply(&test_op("key", winner.clone())).await.unwrap();
        state.flush().await.unwrap();
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
        assert_eq!((page.visited, page.converted, page.done), (0, 0, true));
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
        let admission = crate::query::QueryAdmission::with_runtime_capacity(64 * 1024);
        let retention = admission.reserve_work(1024, 64 * 1024).unwrap();
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
        assert_eq!(admission.diagnostics().reserved_runtime_bytes, 64 * 1024);
        assert!(admission.reserve_work(1024, 1).is_err());
        drop(cache);
        crate::common_test::periodic_check(|| weak.upgrade().is_none(), Duration::from_secs(2))
            .await;
        assert!(weak.upgrade().is_none());
        assert_eq!(admission.diagnostics().reserved_runtime_bytes, 0);
        assert!(admission.reserve_work(1024, 64 * 1024).is_ok());
        worker.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn sparse_query_pages_seek_past_nonmatching_prefixes() {
        let state = query_state((0..600).map(|n| format!("a{n:04}")).chain(["z-hit".into()]));
        let work = work("SELECT key FROM kv WHERE STARTS_WITH(key, 'z')", 256);
        let page = state.query_page(&work).await;
        assert_eq!(page.status, QueryPageStatus::Ok as i32);
        assert!(page.done);
        assert_eq!(page.visited, 1);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].key, "z-hit");
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

#[cfg(test)]
mod expiry_callback_tests {
    use super::flush_tests::FlakyStore;
    use super::*;
    use crate::actor::test_support::test_op;
    use crate::ttl_expiry::{
        expiry_handler_fn, ExpiryBatch, ExpiryCallbackError, ExpiryRuntime, TtlExpiryOptions,
    };
    use std::sync::Mutex as StdMutex;
    use tokio::sync::{mpsc, Notify};

    fn expiring(version: u64) -> VersionedValue {
        let mut value = VersionedValue::new(vec![0xff, 0, 0x80], version, "writer".into());
        value.expires_at_ms = Some(100);
        value
    }

    fn fixture(
        node: &str,
        cache: HashMap<String, VersionedValue>,
        store: Option<Arc<dyn KvStore>>,
    ) -> (
        FragmentState,
        ExpiryRuntime,
        mpsc::Receiver<ExpiryBatch>,
        Arc<AtomicU64>,
    ) {
        let (sender, receiver) = mpsc::channel(1024);
        let handler = expiry_handler_fn(move |batch| {
            let sender = sender.clone();
            async move {
                sender
                    .try_send(batch)
                    .map_err(|_| ExpiryCallbackError::new("test receiver full"))
            }
        });
        let clock = Arc::new(AtomicU64::new(200));
        let clock_handler = clock.clone();
        let runtime = ExpiryRuntime::start_with_clock(
            node.into(),
            Arc::new(handler),
            TtlExpiryOptions::builder().batch_size(1).build(),
            Arc::new(move || clock_handler.load(Ordering::SeqCst)),
        );
        let state = FragmentState::new(cache, store, usize::MAX, Duration::from_secs(3600))
            .with_expiry_publisher(FragmentId(7), Some(runtime.publisher()));
        (state, runtime, receiver, clock)
    }

    #[tokio::test(start_paused = true)]
    async fn reads_and_synthesized_tombstones_are_silent_and_cleanup_reports_original_once() {
        let original = expiring(1);
        let pointer = original.value.as_ptr() as usize;
        let (state, runtime, mut output, _) =
            fixture("node", HashMap::from([("key".into(), original)]), None);
        assert!(state.get("key").await.is_none());
        assert!(
            state
                .get_versioned("key", Some(100))
                .await
                .unwrap()
                .is_tombstone
        );
        let (snapshot, revision) = state.snapshot_with_revision().await;
        assert!(snapshot["key"].is_tombstone);
        assert_eq!(revision, 0);
        assert!(!state.cache.read().await.values["key"].is_tombstone);
        assert_eq!(runtime.diagnostics().enqueued_events, 0);
        assert_eq!(state.expire_values(100).await.unwrap(), 1);
        assert_eq!(state.expire_values(100).await.unwrap(), 0);
        runtime.shutdown().await;
        let batch = output.try_recv().unwrap();
        let event = &batch.events[0];
        assert_eq!(event.fragment, FragmentId(7));
        assert_eq!(event.key, "key");
        assert_eq!(event.previous_value.value, [0xff, 0, 0x80]);
        assert_eq!(event.previous_value.value.as_ptr() as usize, pointer);
        assert!(!event.previous_value.is_tombstone);
        assert_eq!(
            event.original_version(),
            (actordb_protocol::RecordVersion::from(1u64), "writer")
        );
        assert_eq!(event.expires_at_ms, 100);
        assert_eq!(event.observed_at_ms, 200);
        assert_eq!(
            state.get_all().await["key"],
            event.previous_value.expiry_tombstone()
        );
        assert_eq!(runtime.diagnostics().delivered_events, 1);
        assert!(output.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_callbacks_materialize_the_same_tombstone_without_a_publisher() {
        let original = expiring(1);
        let tombstone = original.expiry_tombstone();
        let state = FragmentState::new(
            HashMap::from([("key".into(), original)]),
            None,
            100,
            Duration::from_secs(3600),
        );
        assert!(state.expiry_publisher.is_none());
        assert_eq!(state.expire_values(100).await.unwrap(), 1);
        assert_eq!(state.get_all().await["key"], tombstone);
        assert_eq!(state.expire_values(100).await.unwrap(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_overwrite_and_winning_or_losing_already_expired_apply_are_not_cleanup() {
        for kind in 0..4 {
            let (state, runtime, mut output, _) =
                fixture("node", HashMap::from([("key".into(), expiring(1))]), None);
            let incoming = match kind {
                0 => VersionedValue::tombstone(2, "writer".into()),
                1 => VersionedValue::new(b"replacement".to_vec(), 2, "writer".into()),
                2 => {
                    let mut v = expiring(2);
                    v.expires_at_ms = Some(1);
                    v
                }
                _ => {
                    let mut v = expiring(0);
                    v.expires_at_ms = Some(1);
                    v
                }
            };
            let result = state.apply(&test_op("key", incoming)).await.unwrap();
            if kind == 3 {
                assert!(matches!(result, ApplyOutcome::Superseded { .. }));
                // The losing replicated update is silent. Only expiring the
                // existing winning live version now produces an event.
                assert_eq!(runtime.diagnostics().enqueued_events, 0);
                assert_eq!(state.expire_values(100).await.unwrap(), 1);
            } else {
                if kind == 2 {
                    assert_eq!(result, ApplyOutcome::Expired);
                }
                assert_eq!(state.expire_values(100).await.unwrap(), 0);
            }
            runtime.shutdown().await;
            assert_eq!(runtime.diagnostics().delivered_events, u64::from(kind == 3));
            if kind == 3 {
                assert_eq!(
                    output.try_recv().unwrap().events[0].original_version(),
                    (actordb_protocol::RecordVersion::from(1u64), "writer")
                );
            } else {
                assert!(output.try_recv().is_err());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn replacement_and_ttl_extension_race_with_cleanup_under_the_mutation_lock() {
        for durable in [false, true] {
            for cleanup_first in [false, true] {
                for extends_ttl in [false, true] {
                    let store = Arc::new(FlakyStore {
                        failures_left: StdMutex::new(0),
                        written: StdMutex::new(Vec::new()),
                    });
                    let (state, runtime, mut output, _) = fixture(
                        "node",
                        HashMap::from([("key".into(), expiring(1))]),
                        durable.then(|| store.clone() as Arc<dyn KvStore>),
                    );
                    let mut newer = VersionedValue::new(b"newer".to_vec(), 2, "writer".into());
                    if extends_ttl {
                        newer.expires_at_ms = Some(u64::MAX);
                    }
                    let op = test_op("key", newer.clone());
                    let lock = state.cache.write().await;
                    let mut apply = Box::pin(state.apply(&op));
                    let mut cleanup = Box::pin(state.expire_values(100));
                    if cleanup_first {
                        assert!(futures::poll!(&mut cleanup).is_pending());
                        assert!(futures::poll!(&mut apply).is_pending());
                    } else {
                        assert!(futures::poll!(&mut apply).is_pending());
                        assert!(futures::poll!(&mut cleanup).is_pending());
                    }
                    drop(lock);
                    if cleanup_first {
                        assert_eq!(cleanup.await.unwrap(), 1);
                        assert_eq!(apply.await.unwrap(), ApplyOutcome::Applied);
                    } else {
                        assert_eq!(apply.await.unwrap(), ApplyOutcome::Applied);
                        assert_eq!(cleanup.await.unwrap(), 0);
                    }
                    assert_eq!(state.get_all().await["key"], newer);
                    state.flush().await.unwrap();
                    runtime.shutdown().await;
                    assert_eq!(
                        runtime.diagnostics().delivered_events,
                        u64::from(cleanup_first)
                    );
                    if cleanup_first {
                        let event = output.try_recv().unwrap().events.remove(0);
                        assert_eq!(
                            event.original_version(),
                            (actordb_protocol::RecordVersion::from(1u64), "writer")
                        );
                        // Callback delivery occurs after the newer version was
                        // installed. A consumer must compare versions first.
                        assert!(newer.wins_over(&event.previous_value));
                    } else {
                        assert!(output.try_recv().is_err());
                    }
                    if durable {
                        assert_eq!(store.written.lock().unwrap().last().unwrap().1, newer);
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn paged_cleanup_preserves_fragment_order_and_snapshot_only_reads_stay_silent() {
        let cache = (0..600)
            .map(|i| (format!("k{i:04}"), expiring(i)))
            .collect();
        let (state, runtime, mut output, _) = fixture("node", cache, None);
        let mut pass = ExpiryPass::new(100);
        let mut converted = 0;
        loop {
            let page = state.expire_values_page(&mut pass).await.unwrap();
            converted += page.converted;
            if page.done {
                break;
            }
        }
        assert_eq!(converted, 600);
        assert_eq!(state.expire_values(100).await.unwrap(), 0);
        state.snapshot_with_revision().await;
        runtime.shutdown().await;
        let mut keys = Vec::new();
        while let Ok(batch) = output.try_recv() {
            keys.extend(batch.events.into_iter().map(|e| e.key));
        }
        assert_eq!(
            keys,
            (0..600).map(|i| format!("k{i:04}")).collect::<Vec<_>>()
        );
        assert_eq!(runtime.diagnostics().delivered_events, 600);
    }

    #[tokio::test(start_paused = true)]
    async fn replica_copies_of_the_same_version_generate_the_same_identity() {
        let original = {
            let mut value = expiring(42);
            value.expires_at_ms = Some(u64::MAX);
            value
        };
        let (a, runtime_a, mut events_a, clock_a) = fixture("replica-a", HashMap::new(), None);
        let (b, runtime_b, mut events_b, clock_b) = fixture("replica-b", HashMap::new(), None);
        let op = test_op("replicated", original.clone());
        assert_eq!(a.apply(&op).await.unwrap(), ApplyOutcome::Applied);
        assert_eq!(b.apply(&op).await.unwrap(), ApplyOutcome::Applied);
        clock_a.store(u64::MAX, Ordering::SeqCst);
        clock_b.store(u64::MAX, Ordering::SeqCst);
        assert_eq!(a.expire_values(u64::MAX).await.unwrap(), 1);
        assert_eq!(b.expire_values(u64::MAX).await.unwrap(), 1);
        runtime_a.shutdown().await;
        runtime_b.shutdown().await;
        let batch_a = events_a.try_recv().unwrap();
        let batch_b = events_b.try_recv().unwrap();
        assert_ne!(batch_a.local_node_id, batch_b.local_node_id);
        assert_eq!(batch_a.events[0].id, batch_b.events[0].id);
        assert_eq!(batch_a.events[0].previous_value, original);
        assert_eq!(batch_b.events[0].previous_value, original);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_persistence_does_not_replay_the_local_transition_callback() {
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(1),
            written: StdMutex::new(Vec::new()),
        });
        let (mut state, runtime, mut output, _) = fixture(
            "node",
            HashMap::from([("key".into(), expiring(1))]),
            Some(store.clone()),
        );
        state.flush_batch_size = 1;
        assert!(state.expire_values(100).await.is_err());
        assert!(state.get_all().await["key"].is_tombstone);
        assert!(store.written.lock().unwrap().is_empty());
        assert_eq!(state.expire_values(100).await.unwrap(), 0);
        state.flush().await.unwrap();
        runtime.shutdown().await;
        assert_eq!(
            output.try_recv().unwrap().events[0].original_version(),
            (actordb_protocol::RecordVersion::from(1u64), "writer")
        );
        assert_eq!(runtime.diagnostics().delivered_events, 1);
        assert_eq!(
            store.written.lock().unwrap()[0].1,
            expiring(1).expiry_tombstone()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn queue_saturation_never_prevents_cleanup_or_revision_publication() {
        let gate = Arc::new(Notify::new());
        let gate_handler = gate.clone();
        let handler = expiry_handler_fn(move |_batch| {
            let gate = gate_handler.clone();
            async move {
                gate.notified().await;
                Ok(())
            }
        });
        let runtime = ExpiryRuntime::start_with_clock(
            "node".into(),
            Arc::new(handler),
            TtlExpiryOptions::builder()
                .queue_event_capacity(1)
                .batch_size(1)
                .build(),
            Arc::new(|| 200),
        );
        let cache = (0..100).map(|i| (format!("k{i}"), expiring(i))).collect();
        let state = FragmentState::new(cache, None, 100, Duration::from_secs(3600))
            .with_expiry_publisher(FragmentId(0), Some(runtime.publisher()));
        assert_eq!(state.expire_values(100).await.unwrap(), 100);
        assert!(state.get_all().await.values().all(|v| v.is_tombstone));
        assert_eq!(state.snapshot_with_revision().await.1, 100);
        assert_eq!(runtime.diagnostics().dropped_events, 99);
        gate.notify_one();
        runtime.shutdown().await;
        assert_eq!(runtime.diagnostics().delivered_events, 1);
    }
}

#[cfg(test)]
mod apply_expiry_contention_tests {
    use super::flush_tests::FlakyStore;
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn apply_normalizes_ttl_after_cache_and_flush_contention() {
        for durable in [false, true] {
            for contend_on_flush in [false, true] {
                if contend_on_flush && !durable {
                    continue;
                }
                let store = Arc::new(FlakyStore {
                    failures_left: StdMutex::new(0),
                    written: StdMutex::new(Vec::new()),
                });
                let state = FragmentState::new(
                    HashMap::new(),
                    durable.then(|| store.clone() as Arc<dyn KvStore>),
                    usize::MAX,
                    Duration::from_secs(3600),
                );
                let cache_guard = if contend_on_flush {
                    None
                } else {
                    Some(state.cache.write().await)
                };
                let flush_guard = if contend_on_flush {
                    Some(state.flush.lock().await)
                } else {
                    None
                };
                let mut value = VersionedValue::new(b"payload".to_vec(), now_ms(), "n1".into());
                let deadline = now_ms() + 30;
                value.expires_at_ms = Some(deadline);
                let op = ReplicatedOp {
                    key: "ttl".into(),
                    value: value.clone(),
                    seq: 0,
                    boot_id: 0,
                };
                let apply = state.apply(&op);
                tokio::pin!(apply);
                assert!(futures::poll!(&mut apply).is_pending());
                while now_ms() < deadline {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                drop(cache_guard);
                drop(flush_guard);
                assert_eq!(apply.await.unwrap(), ApplyOutcome::Expired);
                assert_eq!(
                    state.get_versioned("ttl", None).await,
                    Some(value.expiry_tombstone())
                );
                {
                    let cache = state.cache.read().await;
                    assert!(cache.values["ttl"].is_tombstone);
                    assert!(cache.expiry_deadlines.is_empty());
                    assert_eq!(
                        cache.content_digests["ttl"].live,
                        value.expiry_tombstone().content_digest()
                    );
                }
                assert_eq!(state.expire_values(now_ms()).await.unwrap(), 0);
                if durable {
                    state.flush().await.unwrap();
                    assert_eq!(
                        store.written.lock().unwrap().as_slice(),
                        &[(b"ttl".to_vec(), value.expiry_tombstone())]
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn conflicting_ttl_crossing_cache_wait_leaves_cache_index_and_backlog_unchanged() {
        for durable in [false, true] {
            let store = Arc::new(FlakyStore {
                failures_left: StdMutex::new(0),
                written: StdMutex::new(Vec::new()),
            });
            let mut original = VersionedValue::new(b"original".to_vec(), now_ms(), "n1".into());
            let deadline = now_ms() + 30;
            original.expires_at_ms = Some(deadline);
            let state = FragmentState::new(
                HashMap::from([("ttl".into(), original.clone())]),
                durable.then(|| store.clone() as Arc<dyn KvStore>),
                usize::MAX,
                Duration::from_secs(3600),
            );
            let cache_guard = state.cache.write().await;
            let mut conflicting = original.clone();
            conflicting.value = b"conflicting".to_vec();
            let op = ReplicatedOp {
                key: "ttl".into(),
                value: conflicting,
                seq: 0,
                boot_id: 0,
            };
            let apply = state.apply(&op);
            tokio::pin!(apply);
            assert!(futures::poll!(&mut apply).is_pending());
            while now_ms() < deadline {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            drop(cache_guard);
            assert!(matches!(
                apply.await,
                Err(ApplyError::VersionConflict { .. })
            ));
            assert_eq!(state.persistence_backlog().await, (0, 0));
            let cache = state.cache.read().await;
            assert_eq!(cache.values["ttl"], original);
            assert_eq!(cache.content_digests["ttl"].live, original.content_digest());
            assert_eq!(cache.expiry_deadlines.len(), 1);
            assert!(cache
                .expiry_deadlines
                .contains(&(deadline, "ttl".to_owned())));
            assert_eq!(state.revision.load(Ordering::Acquire), 0);
            assert!(store.written.lock().unwrap().is_empty());
        }
    }
}

#[cfg(test)]
mod expiry_index_tests {
    use super::*;
    use crate::actor::test_support::test_op;

    #[tokio::test]
    async fn deadline_index_tracks_only_live_winners_without_hot_key_growth() {
        let state = FragmentState::new(HashMap::new(), None, 100, Duration::from_secs(1));
        for version in 1..=500 {
            let mut value = VersionedValue::new(vec![1], version, "n".into());
            value.expires_at_ms = Some(u64::MAX - version);
            state.apply(&test_op("hot", value)).await.unwrap();
            assert_eq!(state.cache.read().await.expiry_deadlines.len(), 1);
        }
        let older = VersionedValue::new(vec![2], 1, "n".into());
        assert!(matches!(
            state.apply(&test_op("hot", older)).await.unwrap(),
            ApplyOutcome::Superseded { .. }
        ));
        assert_eq!(state.cache.read().await.expiry_deadlines.len(), 1);
        let deleted = VersionedValue::tombstone(501, "n".into());
        state.apply(&test_op("hot", deleted)).await.unwrap();
        assert!(state.cache.read().await.expiry_deadlines.is_empty());
        assert_eq!(state.expire_values(u64::MAX).await.unwrap(), 0);
        assert!(state.cache.read().await.values["hot"].is_tombstone);
    }

    #[tokio::test]
    async fn persistence_saturation_retains_due_deadline_for_retry() {
        use super::flush_tests::FlakyStore;
        use crate::store::PersistenceConfig;
        use std::sync::Mutex as StdMutex;
        let store = Arc::new(FlakyStore {
            failures_left: StdMutex::new(0),
            written: StdMutex::new(Vec::new()),
        });
        let mut due = VersionedValue::new(vec![1], 1, "n".into());
        due.expires_at_ms = Some(100);
        let initial_digest = due.content_digest();
        let state = FragmentState::new(
            HashMap::from([("due".into(), due)]),
            Some(store),
            usize::MAX,
            Duration::from_secs(3600),
        )
        .with_persistence_config(PersistenceConfig::builder().max_records(1).build())
        .unwrap();
        state
            .apply(&test_op(
                "live",
                VersionedValue::new(vec![2], 2, "n".into()),
            ))
            .await
            .unwrap();
        assert!(state.expire_values(100).await.is_err());
        assert_eq!(state.cache.read().await.expiry_deadlines.len(), 1);
        assert!(!state.cache.read().await.values["due"].is_tombstone);
        assert_eq!(
            state.cache.read().await.content_digests["due"].live,
            initial_digest
        );
        assert_eq!(state.expire_values(100).await.unwrap(), 1);
        assert!(state.cache.read().await.expiry_deadlines.is_empty());
        assert!(state.cache.read().await.values["due"].is_tombstone);
        let cache = state.cache.read().await;
        assert_eq!(
            cache.content_digests["due"].live,
            cache.values["due"].content_digest()
        );
    }
}

#[cfg(test)]
mod empty_table_reclamation_tests {
    use super::*;
    use crate::actor::table_runtime_tests::{actor, apply, stop};

    #[tokio::test]
    async fn deferred_empty_owner_survives_failure_and_eviction_then_reclaims_on_unrelated_work() {
        let (shared, actor) = actor(1, "deferred-empty-owner").await;
        // Model a snapshot retaining the just-created cold child through both
        // failed mutation completion and subsequent idle worker eviction.
        let held = shared.table_state("HELD").await.unwrap();
        assert!(matches!(
            apply(&actor, "HELD", 1, false, "").await,
            Err(ApplyError::InvalidInput(_))
        ));
        assert!(matches!(
            apply(&actor, "OTHER", 1, false, "").await,
            Err(ApplyError::Backpressure)
        ));
        assert_eq!(shared.cached_table_count().await, 2);
        assert_eq!(shared.empty_tables.lock().unwrap().len(), 1);
        drop(held);
        assert!(matches!(
            apply(&actor, "OTHER", 1, false, "").await,
            Err(ApplyError::InvalidInput(_))
        ));
        assert_eq!(shared.cached_table_count().await, 1);
        assert!(shared.empty_tables.lock().unwrap().is_empty());
        stop(actor).await;
    }

    #[tokio::test]
    async fn pending_backend_and_fence_ownership_prevent_pruning_then_maintenance_retries() {
        let state = FragmentState::new(Default::default(), None, 100, Duration::from_secs(3600))
            .with_table_pool_capacity(1)
            .unwrap()
            .enable_partitions()
            .unwrap();
        let child = state.table_state("BACKLOG").await.unwrap();
        assert!(child
            .flush
            .lock()
            .await
            .enqueue("pending", &VersionedValue::new(vec![1], 1, "writer".into())));
        drop(child);
        state.prune_empty_table("BACKLOG").await;
        assert_eq!(
            state.cached_table_count().await,
            1,
            "cache-empty cannot discard a persistence owner"
        );
        let child = state.table_children.read().await["BACKLOG"].clone();
        child.flush.lock().await.pending.clear();
        let ordering = child.flush_in_progress.clone().lock_owned().await;
        drop(child);
        state.prune_empty_table("BACKLOG").await;
        assert_eq!(
            state.cached_table_count().await,
            1,
            "active fence ownership is retained"
        );
        drop(ordering);
        state.flush().await.unwrap();
        assert_eq!(
            state.cached_table_count().await,
            0,
            "unrelated maintenance retries deferred cleanup"
        );
    }
}

#[cfg(test)]
mod partitioned_expiry_tests {
    use super::*;

    fn value(deadline: Option<u64>, bytes: usize) -> VersionedValue {
        let mut value = VersionedValue::new(vec![1; bytes], 10, "writer".into());
        value.expires_at_ms = deadline;
        value
    }

    fn state(values: HashMap<String, VersionedValue>) -> FragmentState {
        FragmentState::new(values, None, 100, Duration::from_secs(3600))
            .enable_partitions()
            .unwrap()
    }

    #[tokio::test]
    async fn named_expiry_pages_share_one_budget_and_keep_the_pinned_time() {
        let mut values = HashMap::new();
        for index in 0..600 {
            values.insert(
                crate::table_key::encode_table_key("DUE", &format!("k{index:04}")),
                value(Some(100), 32),
            );
        }
        let future = crate::table_key::encode_table_key("FUTURE", "still-live");
        values.insert(future.clone(), value(Some(101), 32));
        let state = state(values);
        let mut pass = ExpiryPass::new(100);
        let mut converted = 0;
        let mut pages = 0;
        loop {
            let page = state.expire_values_page(&mut pass).await.unwrap();
            assert!(page.visited <= EXPIRY_PAGE_VISIT_LIMIT);
            assert!(page.converted <= page.visited);
            assert!(page.work_bytes < EXPIRY_PAGE_WORK_BYTES);
            if pages == 0 {
                assert_eq!((page.visited, page.converted, page.done), (256, 255, false));
                assert!(page.work_bytes > 0);
            }
            converted += page.converted;
            pages += 1;
            assert!(pages <= 4);
            if page.done {
                break;
            }
        }
        assert_eq!(converted, 600);
        assert_eq!(pages, 3);
        let children = state.table_children.read().await;
        assert!(!children["FUTURE"].cache.read().await.values[&future].is_tombstone);
        assert!(children["DUE"]
            .cache
            .read()
            .await
            .values
            .values()
            .all(|v| v.is_tombstone));
        drop(children);
        let done = state.expire_values_page(&mut pass).await.unwrap();
        assert_eq!((done.visited, done.converted, done.done), (0, 0, true));
    }

    #[tokio::test]
    async fn work_bytes_are_aggregate_across_parent_and_child_locks() {
        let state = state(HashMap::from([
            ("parent".into(), value(Some(100), 600_000)),
            (
                crate::table_key::encode_table_key("A", "large"),
                value(Some(100), 600_000),
            ),
            (
                crate::table_key::encode_table_key("B", "small"),
                value(Some(100), 1),
            ),
        ]));
        let mut pass = ExpiryPass::new(100);
        let first = state.expire_values_page(&mut pass).await.unwrap();
        assert_eq!((first.visited, first.converted, first.done), (3, 2, false));
        assert!(first.work_bytes > EXPIRY_PAGE_WORK_BYTES);
        assert!(first.work_bytes < EXPIRY_PAGE_WORK_BYTES + 601_000);
        let next = state.expire_values_page(&mut pass).await.unwrap();
        assert_eq!((next.visited, next.converted, next.done), (2, 1, true));
    }

    #[tokio::test]
    async fn table_walk_is_paged_even_when_every_table_has_no_due_deadline() {
        let values = (0..1000)
            .map(|index| {
                (
                    crate::table_key::encode_table_key(&format!("T{index:04}"), "live"),
                    value(None, 1),
                )
            })
            .collect();
        let state = state(values);
        let mut pass = ExpiryPass::new(100);
        let mut visits = 0;
        let mut pages = 0;
        loop {
            let page = state.expire_values_page(&mut pass).await.unwrap();
            assert!(page.visited <= EXPIRY_PAGE_VISIT_LIMIT);
            assert_eq!(page.converted, 0);
            assert!(page.work_bytes > 0);
            visits += page.visited;
            pages += 1;
            assert!(pages <= 4);
            if page.done {
                break;
            }
        }
        assert_eq!((visits, pages), (1000, 4));
    }

    #[tokio::test]
    async fn table_insert_churn_cannot_extend_the_original_pass_quota() {
        let values = (0..300)
            .map(|index| {
                (
                    crate::table_key::encode_table_key(&format!("T{index:04}"), "live"),
                    value(None, 1),
                )
            })
            .collect();
        let state = state(values);
        let mut pass = ExpiryPass::new(100);
        let first = state.expire_values_page(&mut pass).await.unwrap();
        assert_eq!((first.visited, first.done), (256, false));
        for index in 0..100 {
            state
                .apply(&ReplicatedOp {
                    key: crate::table_key::encode_table_key(&format!("Z{index:04}"), "new"),
                    value: value(None, 1),
                    seq: 0,
                    boot_id: 0,
                })
                .await
                .unwrap();
        }
        let last = state.expire_values_page(&mut pass).await.unwrap();
        assert_eq!((last.visited, last.done), (44, true));
        assert!(pass.active_table.is_none());
        assert_eq!(state.table_children.read().await.len(), 400);
    }
}
