//! Bounded SQL query execution policy and background page primitives.
//!
//! Queries read each selected fragment at a pinned TTL time and revision. A
//! mutation between pages fails with [`crate::query::QueryExecutionError::QueryChanged`];
//! this is a checked per-fragment read, not a distributed snapshot transaction.

use std::fmt;
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

pub(crate) struct QueryAdmission {
    jobs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

/// Admission ownership retained by actor work after its waiting future is cancelled.
pub struct QueryRetention {
    _job: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
impl QueryAdmission {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Arc::new(Semaphore::new(32)),
            bytes: Arc::new(Semaphore::new(32 * 1024 * 1024)),
        })
    }
    pub(crate) fn reserve(&self, bytes: usize) -> Result<Arc<QueryRetention>, QueryExecutionError> {
        let bytes = u32::try_from(bytes.max(1))
            .map_err(|_| QueryExecutionError::BudgetExceeded("query request storage".into()))?;
        let job = self
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryExecutionError::Backpressure("32 active query jobs".into()))?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| QueryExecutionError::Backpressure("query request bytes".into()))?;
        Ok(Arc::new(QueryRetention {
            _job: job,
            _bytes: bytes,
        }))
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
            Projection::Aggregate { .. } => 0,
        };
        bytes = bytes.saturating_add(message_field_len(2, cell));
    }
    bytes
}
