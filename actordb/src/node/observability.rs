// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Cheap cumulative observations with deliberately bounded table scope.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use super::{Node, NodeError, TableId};

static INCARNATION_SALT: OnceLock<u64> = OnceLock::new();
static NEXT_INCARNATION: AtomicU64 = AtomicU64::new(1);
/// Process-scoped random prefix plus sequence avoids counter/reset collisions
/// across SDK restarts. This is observation metadata, never authentication.
pub(crate) fn fresh_incarnation() -> u64 {
    let salt = *INCARNATION_SALT.get_or_init(|| {
        let mut bytes = [0; 8];
        if getrandom::fill(&mut bytes).is_ok() {
            u64::from_be_bytes(bytes)
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |time| time.as_nanos() as u64)
                ^ ((std::process::id() as u64) << 32)
        }
    });
    let mut identity = salt.wrapping_add(NEXT_INCARNATION.fetch_add(1, Ordering::Relaxed));
    if identity == 0 {
        identity = salt.wrapping_add(NEXT_INCARNATION.fetch_add(1, Ordering::Relaxed));
    }
    identity
}

/// Cumulative monotonic duration observations; no quantiles are inferred.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DurationSummary {
    pub count: u64,
    pub sum_nanos: u64,
    pub max_nanos: u64,
}

#[derive(Default, Debug)]
pub(crate) struct AtomicDurationSummary {
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}
impl AtomicDurationSummary {
    pub(crate) fn observe(&self, elapsed: std::time::Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        add_saturating(&self.count, 1);
        add_saturating(&self.sum, nanos);
        self.max.fetch_max(nanos, Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> DurationSummary {
        DurationSummary {
            count: self.count.load(Ordering::Relaxed),
            sum_nanos: self.sum.load(Ordering::Relaxed),
            max_nanos: self.max.load(Ordering::Relaxed),
        }
    }
}

pub(crate) fn add_saturating(counter: &AtomicU64, amount: u64) {
    let mut observed = counter.load(Ordering::Relaxed);
    while let Err(current) = counter.compare_exchange_weak(
        observed,
        observed.saturating_add(amount),
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        observed = current;
    }
}

/// Explicitly registered table admission ownership. Counts are ownership
/// slots, not distinct client calls; completion includes terminal errors.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableMetricsSnapshot {
    pub table: String,
    pub accepted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub active: usize,
    pub duration: DurationSummary,
}

/// Bounded queue/ownership observations. Worker queues are reported separately
/// by detailed pool snapshots; these values describe node admission pipelines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RequestQueueDiagnostics {
    pub queued_embedded_mutations: usize,
    pub queued_forwarded_mutations: usize,
    pub forwarded_apply_slots: usize,
    pub queued_forwarded_reads: usize,
    pub forwarded_read_slots: usize,
    pub awaiting_responses: usize,
}

#[derive(Default)]
pub(super) struct TableCounters {
    accepted: AtomicU64,
    completed: AtomicU64,
    rejected: AtomicU64,
    active: AtomicUsize,
    duration: AtomicDurationSummary,
}
impl TableCounters {
    pub(super) fn rejected(&self) {
        add_saturating(&self.rejected, 1);
    }
    pub(super) fn acquire(self: &Arc<Self>) -> TableObservation {
        add_saturating(&self.accepted, 1);
        self.active.fetch_add(1, Ordering::Relaxed);
        TableObservation {
            counters: self.clone(),
            started: Instant::now(),
        }
    }
    fn snapshot(&self, table: String) -> TableMetricsSnapshot {
        TableMetricsSnapshot {
            table,
            accepted: self.accepted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            duration: self.duration.snapshot(),
        }
    }
}

pub(super) struct TableObservation {
    counters: Arc<TableCounters>,
    started: Instant,
}
impl Drop for TableObservation {
    fn drop(&mut self) {
        self.counters.duration.observe(self.started.elapsed());
        add_saturating(&self.counters.completed, 1);
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Table labels are installed deliberately, never learned from user keys.
/// Once installed they remain for the node incarnation, bounded to 256 names.
pub(super) struct TableRegistry {
    enabled: AtomicBool,
    tables: RwLock<HashMap<String, Arc<TableCounters>>>,
}
impl TableRegistry {
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
    pub(super) fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            tables: RwLock::new(HashMap::new()),
        }
    }
    fn register(&self, table: &TableId) -> Result<(), NodeError> {
        let mut tables = self.tables.write().unwrap();
        if !tables.contains_key(table.name()) {
            if tables.len() >= 256 {
                return Err(NodeError::Backpressure(
                    "256 registered metric table scopes".into(),
                ));
            }
            tables.insert(table.name().to_owned(), Arc::new(TableCounters::default()));
        }
        self.enabled.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) fn scope(&self, name: &str) -> Option<Arc<TableCounters>> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }
        self.tables.read().unwrap().get(name).cloned()
    }
    pub(super) fn snapshot(&self, limit: usize) -> Option<(Vec<TableMetricsSnapshot>, bool)> {
        let tables = self.tables.try_read().ok()?;
        let mut names = tables.keys().collect::<Vec<_>>();
        names.sort();
        let truncated = names.len() > limit;
        Some((
            names
                .into_iter()
                .take(limit)
                .map(|name| tables[name].snapshot(name.clone()))
                .collect(),
            truncated,
        ))
    }
}

impl Node {
    pub fn queue_diagnostics(&self) -> RequestQueueDiagnostics {
        RequestQueueDiagnostics {
            queued_embedded_mutations: self.embedded_operations.queued(),
            queued_forwarded_mutations: self.mutation_service.queued(),
            forwarded_apply_slots: self.mutation_service.admitted_apply_slots(),
            queued_forwarded_reads: self.read_service.queued(),
            forwarded_read_slots: self.read_service.admitted_slots(),
            awaiting_responses: self
                .operation_gate
                .awaiting_responses
                .load(Ordering::SeqCst),
        }
    }
    pub fn storage_diagnostics(&self) -> crate::store::StorageDiagnostics {
        self.storage_executor.diagnostics()
    }
    /// Enable request/storage timing for future work. Existing cheap lifecycle
    /// counters are always available; enabling does not invent earlier samples.
    pub fn enable_metrics(&self) {
        self.pool_timing_enabled.store(true, Ordering::Release);
        self.operation_gate
            .timing_enabled
            .store(true, Ordering::Release);
        self.storage_executor.enable_timing();
    }

    /// Include a deliberate table label in observations for future work.
    /// Registration is idempotent and bounded to 256 identities per node.
    pub fn enable_table_metrics(&self, table: &TableId) -> Result<(), NodeError> {
        self.table_metrics.register(table)?;
        self.enable_metrics();
        Ok(())
    }

    pub(super) fn admit_table_operation(
        &self,
        table: &str,
    ) -> Result<super::OperationPermit, NodeError> {
        let counters = self.table_metrics.scope(table);
        match self.admit_operation() {
            Ok(operation) => Ok(operation.with_table(counters.map(|scope| scope.acquire()))),
            Err(error) => {
                if let Some(scope) = counters {
                    scope.rejected();
                }
                Err(error)
            }
        }
    }

    pub(super) fn admit_key_operation(
        &self,
        key: &str,
    ) -> Result<super::OperationPermit, NodeError> {
        if !self.table_metrics.enabled() {
            return self.admit_operation();
        }
        self.admit_table_operation(crate::table_key::table_name(key).as_deref().unwrap_or("KV"))
    }

    pub(super) fn admit_batch_operation<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> Result<super::OperationPermit, NodeError> {
        if !self.table_metrics.enabled() {
            return self.admit_operation();
        }
        let mut keys = keys.into_iter();
        let Some(first) = keys.next() else {
            return self.admit_operation();
        };
        let name = crate::table_key::table_name(first);
        let table = name.as_deref().unwrap_or("KV");
        if keys.all(|key| crate::table_key::table_name(key).as_deref().unwrap_or("KV") == table) {
            self.admit_table_operation(table)
        } else {
            // A mixed-table low-level batch has node scope; do not mislabel it.
            self.admit_operation()
        }
    }

    pub(crate) fn note_waiter_timeout(&self) {
        add_saturating(&self.operation_gate.waiter_timeouts, 1);
    }
}
