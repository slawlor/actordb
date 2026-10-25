//! Bounded SQL query execution policy and background page primitives.
//!
//! Queries read each selected fragment at a pinned TTL time and revision. A
//! mutation between pages fails with [`crate::query::QueryExecutionError::QueryChanged`];
//! this is a checked per-fragment read, not a distributed snapshot transaction.

use crate::retained::{ByteBudget, BytePermit};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb_query::{QueryPlan, QueryValue};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::proto::meta::query_cell;
use crate::proto::{
    Envelope, EnvelopeMsg, QueryCell, QueryPageRequest, QueryPageResponse, QueryPageRow,
    QueryPageStatus,
};

/// Execution limits. Exhaustion fails the whole query; results are never silently truncated.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// Absolute deadline for the complete query including queue admission.
    pub timeout: Duration,
    /// Maximum retained projected result storage, including collection headers.
    pub max_result_bytes: usize,
    /// Maximum retained merge pages and fragment state storage.
    pub max_buffer_bytes: usize,
    /// Maximum visited records, including nonmatches and invisible records.
    pub max_scanned_rows: u64,
    /// Maximum encoded page body. Must be between 1024 and 16 MiB minus 4096.
    pub page_bytes: usize,
    /// Maximum records visited under one cache lock; between 1 and 256.
    pub page_visits: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            max_result_bytes: 16 * 1024 * 1024,
            max_buffer_bytes: 32 * 1024 * 1024,
            max_scanned_rows: 1_000_000,
            page_bytes: 1024 * 1024,
            page_visits: 256,
        }
    }
}

/// A query failed before a complete, validated result could be returned.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueryExecutionError {
    /// Syntax, parameter, or plan validation failure.
    InvalidQuery(String),
    /// A selected fragment has no usable serving holder.
    FragmentsUnavailable(Vec<u32>),
    /// The pinned fragment state or physical holder changed during execution.
    QueryChanged(u32),
    /// Count, work, encoded page, merge memory, or result budget was exhausted.
    BudgetExceeded(String),
    /// Background pool or node query admission rejected the operation.
    Backpressure(String),
    /// The complete query deadline elapsed.
    Timeout,
    /// The node has closed operation admission.
    ShuttingDown,
    /// A holder returned an invalid page or the actor RPC failed.
    Protocol(String),
}

impl fmt::Display for QueryExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidQuery(e) => write!(f, "invalid query: {e}"),
            Self::FragmentsUnavailable(ids) => write!(f, "query fragments unavailable: {ids:?}"),
            Self::QueryChanged(id) => write!(f, "fragment {id} changed during query"),
            Self::BudgetExceeded(e) => write!(f, "query budget exceeded: {e}"),
            Self::Backpressure(e) => write!(f, "query admission rejected: {e}"),
            Self::Timeout => write!(f, "query deadline elapsed"),
            Self::ShuttingDown => write!(f, "node is shutting down"),
            Self::Protocol(e) => write!(f, "invalid query page: {e}"),
        }
    }
}
impl std::error::Error for QueryExecutionError {}
impl From<actordb_query::QueryError> for QueryExecutionError {
    fn from(error: actordb_query::QueryError) -> Self {
        Self::InvalidQuery(error.to_string())
    }
}

pub(crate) const MAX_QUERY_PAGE_BYTES: usize = crate::wire::MAX_FRAME_BYTES - 4096;
pub(crate) const QUERY_PAGE_VISITS: usize = 256;
pub(crate) const QUERY_WORK_BYTES: usize = 1024 * 1024;

pub(crate) const DEFAULT_QUERY_RUNTIME_BYTES: usize = 256 * 1024 * 1024;

/// Admission counters for active query/scan execution. These are conservative
/// reservations, not RSS measurements or storage retained by returned callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryDiagnostics {
    /// Configured node-wide allowance for results, merge pages, and active work.
    pub runtime_byte_capacity: usize,
    /// Bytes reserved through active coordinators and actor work.
    pub reserved_runtime_bytes: usize,
    /// Number of query/scan admissions rejected at any count/byte limit.
    pub admission_rejections: u64,
}

pub(crate) struct QueryAdmission {
    jobs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    runtime: Arc<ByteBudget>,
    runtime_capacity: usize,
    rejected: AtomicU64,
}

/// Admission ownership retained by actor work after its waiting future is cancelled.
pub struct QueryRetention {
    _job: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    _runtime: BytePermit,
    _operation: Option<Arc<dyn Send + Sync>>,
}
impl QueryAdmission {
    #[cfg(test)]
    pub(crate) fn new() -> Arc<Self> {
        Self::with_runtime_capacity(DEFAULT_QUERY_RUNTIME_BYTES)
    }
    pub(crate) fn with_runtime_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            jobs: Arc::new(Semaphore::new(32)),
            bytes: Arc::new(Semaphore::new(32 * 1024 * 1024)),
            runtime: ByteBudget::new(capacity),
            runtime_capacity: capacity,
            rejected: AtomicU64::new(0),
        })
    }
    pub(crate) fn diagnostics(&self) -> QueryDiagnostics {
        QueryDiagnostics {
            runtime_byte_capacity: self.runtime_capacity,
            reserved_runtime_bytes: self.runtime.used(),
            admission_rejections: self.rejected.load(Ordering::Relaxed),
        }
    }
    #[cfg(test)]
    pub(crate) fn reserve_work(
        &self,
        bytes: usize,
        runtime_bytes: usize,
    ) -> Result<Arc<QueryRetention>, QueryExecutionError> {
        self.reserve_work_retained(bytes, runtime_bytes, None)
    }

    /// Keep application admission ownership with actor page work even when
    /// the query coordinator or its caller is cancelled.
    pub(crate) fn reserve_work_retained(
        &self,
        bytes: usize,
        runtime_bytes: usize,
        operation: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Arc<QueryRetention>, QueryExecutionError> {
        // Try-acquire only: blocked callers never accumulate owning request
        // payloads behind an unbounded semaphore waiter list.
        let result = (|| {
            let bytes = u32::try_from(bytes.max(1))
                .map_err(|_| QueryExecutionError::BudgetExceeded("query request storage".into()))?;
            let job =
                self.jobs.clone().try_acquire_owned().map_err(|_| {
                    QueryExecutionError::Backpressure("32 active query jobs".into())
                })?;
            let bytes = self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes)
                .map_err(|_| QueryExecutionError::Backpressure("query request bytes".into()))?;
            let runtime = self.runtime.reserve(runtime_bytes).ok_or_else(|| {
                tracing::warn!(
                    reserved_bytes = self.runtime.used(),
                    requested_bytes = runtime_bytes,
                    capacity_bytes = self.runtime_capacity,
                    "query runtime admission rejected"
                );
                QueryExecutionError::Backpressure("shared query runtime bytes".into())
            })?;
            Ok(Arc::new(QueryRetention {
                _job: job,
                _bytes: bytes,
                _runtime: runtime,
                _operation: operation,
            }))
        })();
        if result.is_err() {
            let mut previous = self.rejected.load(Ordering::Relaxed);
            while let Err(current) = self.rejected.compare_exchange_weak(
                previous,
                previous.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                previous = current;
            }
        }
        result
    }
}

/// Background actor page request. Construct through [`QueryPageWork::new`].
pub struct QueryPageWork {
    pub(crate) plan: Arc<QueryPlan>,
    pub(crate) request: QueryPageRequest,
    pub(crate) retention: Option<Arc<QueryRetention>>,
}
impl QueryPageWork {
    /// Construct a low-level bounded page request. Runtime validates every limit.
    pub fn new(plan: Arc<QueryPlan>, request: QueryPageRequest) -> Self {
        Self {
            plan,
            request,
            retention: None,
        }
    }
}

pub(crate) fn value_to_cell(value: QueryValue) -> QueryCell {
    let value = match value {
        QueryValue::Null => query_cell::Value::Null(true),
        QueryValue::Text(v) => query_cell::Value::Text(v),
        QueryValue::Bytes(v) => query_cell::Value::Bytes(v),
        QueryValue::Integer(v) => query_cell::Value::Integer(v.to_string()),
    };
    QueryCell { value: Some(value) }
}
pub(crate) fn cell_to_value(cell: QueryCell) -> Result<QueryValue, QueryExecutionError> {
    match cell.value {
        Some(query_cell::Value::Null(true)) => Ok(QueryValue::Null),
        Some(query_cell::Value::Text(v)) => Ok(QueryValue::Text(v)),
        Some(query_cell::Value::Bytes(v)) => Ok(QueryValue::Bytes(v)),
        Some(query_cell::Value::Integer(v)) => {
            let n = v
                .parse::<i128>()
                .map_err(|_| QueryExecutionError::Protocol("invalid i128".into()))?;
            if n.to_string() != v {
                return Err(QueryExecutionError::Protocol("noncanonical i128".into()));
            }
            Ok(QueryValue::Integer(n))
        }
        _ => Err(QueryExecutionError::Protocol(
            "missing or invalid query cell".into(),
        )),
    }
}

pub(crate) fn page_error(
    req: &QueryPageRequest,
    status: QueryPageStatus,
    error: impl Into<String>,
) -> QueryPageResponse {
    let mut error = error.into();
    while error.len() > 1024 {
        error.pop();
    }
    QueryPageResponse {
        req_id: req.req_id,
        fragment: req.fragment,
        status: status as i32,
        error,
        ..Default::default()
    }
}

pub(crate) fn page_encoded_len(page: &QueryPageResponse) -> usize {
    // One Envelope field with the actual response payload. Avoid allocating/copying rows.
    let payload = page.encoded_len();
    crate::wire::message_field_len(24, payload)
}

pub(crate) fn row_storage(row: &QueryPageRow) -> usize {
    row.key
        .capacity()
        .saturating_add(std::mem::size_of::<QueryPageRow>())
        .saturating_add(
            row.cells
                .capacity()
                .saturating_mul(std::mem::size_of::<QueryCell>()),
        )
        .saturating_add(
            row.cells
                .iter()
                .map(|c| match &c.value {
                    Some(query_cell::Value::Text(v) | query_cell::Value::Integer(v)) => {
                        v.capacity()
                    }
                    Some(query_cell::Value::Bytes(v)) => v.capacity(),
                    _ => 0,
                })
                .sum::<usize>(),
        )
}
pub(crate) fn result_row_storage(row: &[QueryValue]) -> usize {
    std::mem::size_of::<Vec<QueryValue>>()
        .saturating_add(row.len().saturating_mul(std::mem::size_of::<QueryValue>()))
        .saturating_add(
            row.iter()
                .map(|v| match v {
                    QueryValue::Text(v) => v.capacity(),
                    QueryValue::Bytes(v) => v.capacity(),
                    _ => 0,
                })
                .sum::<usize>(),
        )
}

pub(crate) fn page_envelope(page: QueryPageResponse) -> Envelope {
    Envelope {
        msg: Some(EnvelopeMsg::QueryPageResponse(page)),
    }
}

/// Exact projected row size before cloning value buffers. The enclosing page
/// adds the cursor and fixed response metadata separately.
pub(crate) fn projected_row_wire_bytes(
    plan: &QueryPlan,
    key: &str,
    value_len: usize,
    fragment: u32,
    metadata: &actordb_query::RecordMetadata,
) -> usize {
    use crate::wire::{bytes_field_len, message_field_len};
    use actordb_query::Projection;
    let mut bytes = bytes_field_len(1, key.len());
    for projection in &plan.projections {
        let cell = match projection {
            Projection::Key => bytes_field_len(2, key.len()),
            Projection::Value => bytes_field_len(3, value_len),
            Projection::PrefixBytes(n) => bytes_field_len(3, value_len.min(*n as usize)),
            Projection::Fragment => bytes_field_len(4, fragment.to_string().len()),
            Projection::OctetLength => bytes_field_len(4, value_len.to_string().len()),
            Projection::TimestampMs => bytes_field_len(4, metadata.timestamp_ms.to_string().len()),
            Projection::LogicalCounter => {
                bytes_field_len(4, metadata.logical_counter.to_string().len())
            }
            Projection::NodeId => bytes_field_len(2, metadata.node_id.len()),
            Projection::ExpiresAtMs => metadata
                .expires_at_ms
                .map_or(2, |value| bytes_field_len(4, value.to_string().len())),
            Projection::Tombstone => bytes_field_len(4, 1),
            Projection::Aggregate { .. } => 0,
        };
        bytes = bytes.saturating_add(message_field_len(2, cell));
    }
    bytes
}

/// Ordered index bounds implied by all conjunctive key predicates. `None`
/// means the intersection is empty, including impossible binary prefixes.
pub(crate) fn query_key_bounds(
    plan: &QueryPlan,
    after: Option<&str>,
) -> Option<(std::ops::Bound<String>, std::ops::Bound<String>)> {
    use actordb_query::KeyPredicate;
    use std::ops::Bound::{self, Excluded, Included, Unbounded};
    let mut lower = Unbounded;
    let mut upper = Unbounded;
    fn tighten(current: &mut Bound<String>, candidate: Bound<String>, lower: bool) {
        let (key, exclusive) = match &candidate {
            Included(key) => (key, false),
            Excluded(key) => (key, true),
            Unbounded => return,
        };
        let replace = match &*current {
            Unbounded => true,
            Included(old) | Excluded(old) => {
                let ordering = key.cmp(old);
                (if lower {
                    ordering.is_gt()
                } else {
                    ordering.is_lt()
                }) || (ordering.is_eq() && exclusive)
            }
        };
        if replace {
            *current = candidate;
        }
    }
    for predicate in &plan.predicates {
        match predicate {
            KeyPredicate::Equal(key) => {
                tighten(&mut lower, Included(key.clone()), true);
                tighten(&mut upper, Included(key.clone()), false);
            }
            KeyPredicate::GreaterThan(key) => tighten(&mut lower, Excluded(key.clone()), true),
            KeyPredicate::GreaterOrEqual(key) => tighten(&mut lower, Included(key.clone()), true),
            KeyPredicate::LessThan(key) => tighten(&mut upper, Excluded(key.clone()), false),
            KeyPredicate::LessOrEqual(key) => tighten(&mut upper, Included(key.clone()), false),
            KeyPredicate::Prefix(prefix) => {
                tighten(&mut lower, Included(utf8_ceiling(prefix)?), true);
                // Bytes beginning with prefix form [prefix, successor(prefix)).
                let mut successor = prefix.clone();
                while successor.last() == Some(&255) {
                    successor.pop();
                }
                if let Some(last) = successor.last_mut() {
                    *last += 1;
                    if let Some(end) = utf8_ceiling(&successor) {
                        tighten(&mut upper, Excluded(end), false);
                    }
                }
            }
        }
    }
    if let Some(after) = after {
        tighten(&mut lower, Excluded(after.to_owned()), true);
    }
    match (&lower, &upper) {
        (Included(a), Included(b)) if a > b => None,
        (Included(a) | Excluded(a), Included(b) | Excluded(b))
            if a >= b && (a != b || !matches!((&lower, &upper), (Included(_), Included(_)))) =>
        {
            None
        }
        _ => Some((lower, upper)),
    }
}

/// Smallest valid UTF-8 string at or above an arbitrary byte sequence.
/// UTF-8 scalar encodings sort in scalar order, allowing a bounded binary
/// search at the first invalid/incomplete scalar rather than a cache scan.
fn utf8_ceiling(bytes: &[u8]) -> Option<String> {
    let error = match std::str::from_utf8(bytes) {
        Ok(text) => return Some(text.to_owned()),
        Err(error) => error,
    };
    let mut prefix = std::str::from_utf8(&bytes[..error.valid_up_to()])
        .ok()?
        .to_owned();
    let suffix = &bytes[error.valid_up_to()..];
    let scalar =
        |index: u32| char::from_u32(if index < 0xd800 { index } else { index + 0x800 }).unwrap();
    let mut low = 0u32;
    let mut high = 0x110000 - 0x800;
    while low < high {
        let mid = low + (high - low) / 2;
        let mut encoded = [0; 4];
        if scalar(mid).encode_utf8(&mut encoded).as_bytes() < suffix {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low < 0x110000 - 0x800 {
        prefix.push(scalar(low));
        return Some(prefix);
    }
    // No scalar can finish this prefix; carry into the previous scalar.
    while let Some(last) = prefix.pop() {
        if last != char::MAX {
            let mut next = last as u32 + 1;
            if next == 0xd800 {
                next = 0xe000;
            }
            prefix.push(char::from_u32(next)?);
            return Some(prefix);
        }
    }
    None
}

#[cfg(test)]
mod key_bound_tests {
    use super::*;
    use actordb_query::{KeyPredicate, Projection};
    use std::collections::BTreeSet;

    #[test]
    fn conjunctive_bounds_select_exact_range_and_binary_prefixes() {
        let keys: BTreeSet<String> = ["", "a", "aa", "ab", "az", "b", "é", "雪", "𐀀", "\u{10ffff}"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let cases = vec![
            vec![KeyPredicate::Equal("az".into())],
            vec![
                KeyPredicate::GreaterThan("aa".into()),
                KeyPredicate::LessOrEqual("az".into()),
            ],
            vec![
                KeyPredicate::GreaterOrEqual("a".into()),
                KeyPredicate::LessThan("a".into()),
            ],
            vec![
                KeyPredicate::Equal("a".into()),
                KeyPredicate::Equal("b".into()),
            ],
            vec![KeyPredicate::Prefix(vec![])],
            vec![
                KeyPredicate::Prefix(b"a".to_vec()),
                KeyPredicate::LessThan("ab".into()),
            ],
            vec![KeyPredicate::Prefix("雪".as_bytes().to_vec())],
            vec![KeyPredicate::Prefix(vec![0xe9])],
            vec![KeyPredicate::Prefix(vec![0xf0, 0x90])],
            vec![KeyPredicate::Prefix(vec![0xff])],
            vec![KeyPredicate::Prefix(vec![0xc0])],
            vec![KeyPredicate::Prefix(vec![0xe9, 0x00])],
            vec![KeyPredicate::Prefix(vec![0xf4, 0x8f, 0xbf, 0xbf, 0xff])],
            vec![
                KeyPredicate::Prefix(b"a".to_vec()),
                KeyPredicate::Prefix(b"b".to_vec()),
            ],
        ];
        for predicates in cases {
            let plan = QueryPlan {
                table: "KV".into(),
                projections: vec![Projection::Key],
                predicates,
                metadata_predicates: Vec::new(),
                fragments: None,
                limit: None,
                offset: 0,
                include_tombstones: false,
            };
            for after in [None, Some(""), Some("aa"), Some("雪"), Some("\u{10ffff}")] {
                let expected: Vec<_> = keys
                    .iter()
                    .filter(|key| {
                        plan.matches_key(key) && after.is_none_or(|after| key.as_str() > after)
                    })
                    .cloned()
                    .collect();
                let actual: Vec<_> = query_key_bounds(&plan, after)
                    .map(|bounds| {
                        keys.range(bounds)
                            .filter(|key| plan.matches_key(key))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                assert_eq!(actual, expected, "{:?} after {:?}", plan.predicates, after);
            }
        }
    }
}

#[cfg(test)]
mod runtime_admission_tests {
    use super::*;

    #[test]
    fn aggregate_runtime_admission_rejects_without_waiting_and_rolls_back() {
        let admission = QueryAdmission::new();
        let mut active = Vec::new();
        let per_query = 48 * 1024 * 1024;
        for _ in 0..5 {
            active.push(admission.reserve_work(1024, per_query).unwrap());
        }
        assert_eq!(
            admission.diagnostics().reserved_runtime_bytes,
            5 * per_query
        );
        for _ in 0..100 {
            assert!(matches!(
                admission.reserve_work(1024, per_query),
                Err(QueryExecutionError::Backpressure(_))
            ));
        }
        assert_eq!(admission.diagnostics().admission_rejections, 100);
        active.pop();
        assert!(admission.reserve_work(1024, per_query).is_ok());
        drop(active);
        assert_eq!(admission.diagnostics().reserved_runtime_bytes, 0);
        // Failed runtime acquisition must not leak request/count permits.
        let tiny = QueryAdmission::with_runtime_capacity(1);
        for _ in 0..40 {
            assert!(tiny.reserve_work(1, 2).is_err());
        }
        assert!(tiny.reserve_work(1, 1).is_ok());
    }
}
