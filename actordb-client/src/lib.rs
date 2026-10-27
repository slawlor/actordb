//! Transport-neutral ActorDB SDK. The optional default gRPC adapter provides
//! unary requests; custom RPC frameworks implement `RpcClient` instead.
//! Requests may execute concurrently. No mutation is transparently retried:
//! a lost reply after dispatch remains `ClientError::UnknownOutcome`.

use actordb_protocol::{client as wire, MAX_RPC_MESSAGE_BYTES};
use prost::Message;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Semaphore};
use tokio::time::{timeout_at, Instant};

pub use actordb_protocol::meta::{Priority, ScanMatch};
pub use actordb_protocol::rpc::{RpcCallOptions, RpcClient, RpcError, RpcFuture, RpcReply};
pub use actordb_query::{QueryResult, QueryValue};
pub use wire::Scan as ScanOptions;
pub use wire::{Inspection, Value};
#[cfg(feature = "grpc")]
pub mod grpc;

mod write_concern;
pub use actordb_protocol::write_concern::{
    WriteAcknowledgement, WriteConcern, WriteConcernEvidence, WriteConcernOptions,
    WriteConcernReceipt, WriteDurability, WriteOperationEvidence,
};
pub use write_concern::WriteConcernClientError;

/// RPC admission and materialization limits. Authentication is adapter-owned.
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct ClientOptions {
    #[builder(default = "KV".to_string(), into)]
    pub table: String,
    #[builder(default = Duration::from_secs(30))]
    pub timeout: Duration,
    /// Queued plus active RPC requests sharing this client.
    #[builder(default = 64)]
    pub max_queued_requests: usize,
    #[builder(default = 32 * 1024 * 1024)]
    pub max_queued_bytes: usize,
    /// Per-response raw/decoded/portable-conversion allowance.
    #[builder(default = actordb_protocol::DEFAULT_MAX_DECODED_RESPONSE_BYTES)]
    pub max_decoded_response_bytes: usize,
    /// Aggregate reservations for response materialization, not caller-retained
    /// values or allocator RSS. Defaults allow four concurrent 64 MiB RPCs.
    #[builder(default = 256 * 1024 * 1024)]
    pub max_response_memory_bytes: usize,
}
impl Default for ClientOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct PutOptions {
    pub ttl: Option<Duration>,
    #[builder(default)]
    pub priority: Priority,
}

#[derive(Debug)]
pub enum ClientError {
    Invalid(String),
    Timeout,
    Closed,
    Overloaded,
    Transport(String),
    Protocol(String),
    Server(wire::Error),
    UnknownOutcome(String),
}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(e) => write!(f, "invalid request: {e}"),
            Self::Timeout => f.write_str("RPC deadline exceeded"),
            Self::Closed => f.write_str("client closed"),
            Self::Overloaded => f.write_str("RPC admission full"),
            Self::Transport(e) => write!(f, "RPC transport: {e}"),
            Self::Protocol(e) => write!(f, "RPC protocol: {e}"),
            Self::Server(e) => write!(f, "server error {}: {}", e.code, e.message),
            Self::UnknownOutcome(e) => write!(f, "mutation outcome unknown: {e}"),
        }
    }
}
impl std::error::Error for ClientError {}

struct Inner {
    rpc: Mutex<Option<Arc<dyn RpcClient>>>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    responses: Arc<Semaphore>,
    options: ClientOptions,
    closing: watch::Sender<bool>,
}
type OwnedResponse = RpcReply;
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
    table: String,
}

impl Client {
    /// Construct a client for any unary RPC adapter. No authentication, stream,
    /// handshake, or framework-specific types are involved in this factory.
    pub fn from_rpc(rpc: Arc<dyn RpcClient>, options: ClientOptions) -> Result<Self, ClientError> {
        Self::validate_options(&options)?;
        let table = options.table.clone();
        Ok(Self {
            inner: Arc::new(Inner {
                rpc: Mutex::new(Some(rpc)),
                count: Arc::new(Semaphore::new(options.max_queued_requests)),
                bytes: Arc::new(Semaphore::new(options.max_queued_bytes)),
                responses: Arc::new(Semaphore::new(options.max_response_memory_bytes)),
                closing: watch::channel(false).0,
                options,
            }),
            table,
        })
    }
    fn validate_options(options: &ClientOptions) -> Result<(), ClientError> {
        if options.table.is_empty()
            || options.table.len() > 255
            || options.timeout.is_zero()
            || Instant::now().checked_add(options.timeout).is_none()
            || options.max_queued_requests == 0
            || options.max_queued_requests > Semaphore::MAX_PERMITS
            || options.max_queued_bytes < MAX_RPC_MESSAGE_BYTES
            || options.max_queued_bytes > u32::MAX as usize
            || options.max_queued_bytes > Semaphore::MAX_PERMITS
            || options.max_decoded_response_bytes < MAX_RPC_MESSAGE_BYTES
            || options.max_decoded_response_bytes > u32::MAX as usize
            || options.max_response_memory_bytes < options.max_decoded_response_bytes
            || options.max_response_memory_bytes > u32::MAX as usize
            || options.max_response_memory_bytes > Semaphore::MAX_PERMITS
        {
            return Err(ClientError::Invalid(
                "RPC table, deadline, or admission bounds".into(),
            ));
        }
        Ok(())
    }
    /// Share the RPC adapter and all admission budgets with a table-scoped view.
    pub fn table(&self, table: impl Into<String>) -> Result<Self, ClientError> {
        let table = table.into();
        if table.is_empty() || table.len() > 255 {
            return Err(ClientError::Invalid("table name length".into()));
        }
        Ok(Self {
            inner: self.inner.clone(),
            table,
        })
    }
    async fn request(
        &self,
        operation: wire::request::Operation,
        mutation: bool,
    ) -> Result<OwnedResponse, ClientError> {
        self.request_with_timeout(operation, mutation, Duration::ZERO)
            .await
    }
    async fn request_with_timeout(
        &self,
        operation: wire::request::Operation,
        mutation: bool,
        extra: Duration,
    ) -> Result<OwnedResponse, ClientError> {
        let request_timeout = self
            .inner
            .options
            .timeout
            .checked_add(extra)
            .ok_or_else(|| ClientError::Invalid("timeout too large".into()))?;
        let deadline = Instant::now()
            .checked_add(request_timeout)
            .ok_or_else(|| ClientError::Invalid("timeout too large".into()))?;
        let _count = self.inner.count.try_acquire().map_err(|_| {
            if self.inner.count.is_closed() {
                ClientError::Closed
            } else {
                ClientError::Overloaded
            }
        })?;
        let request = Arc::new(wire::Request {
            operation: Some(operation),
            table: self.table.clone(),
        });
        let length = request.encoded_len();
        if length > MAX_RPC_MESSAGE_BYTES {
            return Err(ClientError::Invalid(
                "RPC request exceeds byte limit".into(),
            ));
        }
        let _bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(length as u32)
            .map_err(|_| {
                if self.inner.bytes.is_closed() {
                    ClientError::Closed
                } else {
                    ClientError::Overloaded
                }
            })?;
        let mut closing = self.inner.closing.subscribe();
        let cancelled = async {
            if !*closing.borrow_and_update() {
                let _ = closing.changed().await;
            }
        };
        tokio::pin!(cancelled);
        // Reserve the full per-call bound before framework buffering/decoding.
        // Calls run concurrently within the aggregate reservation, not behind
        // a shared socket lock or a single response-materialization owner.
        let response_owner = tokio::select! {
            biased;
            _ = &mut cancelled => return Err(ClientError::Closed),
            owner = timeout_at(deadline, self.inner.responses.clone()
                .acquire_many_owned(self.inner.options.max_decoded_response_bytes as u32)) =>
                owner.map_err(|_| ClientError::Timeout)?.map_err(|_| ClientError::Closed)?,
        };
        let rpc = self
            .inner
            .rpc
            .lock()
            .unwrap()
            .clone()
            .ok_or(ClientError::Closed)?;
        let response_shape = response_shape(&request)?;
        let call_options = RpcCallOptions {
            deadline: deadline.into_std(),
            max_decoded_response_bytes: self.inner.options.max_decoded_response_bytes,
            response_shape,
        };
        let mut dispatched = false;
        let exchange = async {
            dispatched = true;
            let mut reply = rpc
                .call(request.clone(), call_options)
                .await
                .map_err(|error| match error {
                    RpcError::NotDispatched(message) => ClientError::Transport(message),
                    RpcError::Failure(message) => {
                        if mutation {
                            ClientError::UnknownOutcome(message)
                        } else {
                            ClientError::Transport(message)
                        }
                    }
                    error => {
                        if mutation {
                            ClientError::UnknownOutcome(error.to_string())
                        } else {
                            ClientError::Transport(error.to_string())
                        }
                    }
                })?;
            reply.retain(response_owner);
            if reply.encoded_len() > MAX_RPC_MESSAGE_BYTES {
                return Err(if mutation {
                    ClientError::UnknownOutcome("oversized RPC reply".into())
                } else {
                    ClientError::Protocol("oversized RPC reply".into())
                });
            }
            validate_response(&request, &reply).map_err(|error| {
                if mutation {
                    ClientError::UnknownOutcome(error.to_string())
                } else {
                    error
                }
            })?;
            if let Some(error) = reply.error.clone() {
                Err(server_error(error))
            } else {
                Ok(reply)
            }
        };
        let result = tokio::select! {
            biased;
            _ = &mut cancelled => Err(ClientError::Closed),
            result = timeout_at(deadline, exchange) => result.unwrap_or(Err(ClientError::Timeout)),
        };
        if mutation
            && dispatched
            && matches!(result, Err(ClientError::Timeout | ClientError::Closed))
        {
            Err(ClientError::UnknownOutcome(result.unwrap_err().to_string()))
        } else {
            result
        }
    }
    /// Close this client and its table/clone views, cancelling local RPC waiters.
    /// Accepted server mutations remain owned by the service. Closing this SDK
    /// never shuts down the Node or a separately shared custom adapter.
    pub async fn close(&self) {
        self.inner.count.close();
        self.inner.bytes.close();
        self.inner.responses.close();
        self.inner.closing.send_replace(true);
        self.inner.rpc.lock().unwrap().take();
    }
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<Value>, ClientError> {
        self.get_with_priority(key, Priority::Default).await
    }
    pub async fn get_with_priority(
        &self,
        key: impl Into<String>,
        priority: Priority,
    ) -> Result<Option<Value>, ClientError> {
        let mut response = self
            .request(
                wire::request::Operation::Get(wire::Get {
                    key: key.into(),
                    priority: priority as i32,
                }),
                false,
            )
            .await?;
        let mut results = decode_reads(std::mem::take(&mut response.reads));
        if results.len() != 1 {
            return Err(ClientError::Protocol(
                "single get result cardinality".into(),
            ));
        }
        results.remove(0)
    }
    pub async fn put(&self, key: impl Into<String>, value: Vec<u8>) -> Result<(), ClientError> {
        self.put_with_options(key, value, PutOptions::default())
            .await
    }
    pub async fn put_with_options(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<(), ClientError> {
        let ttl_ms = options
            .ttl
            .map(|ttl| {
                u64::try_from(ttl.as_millis())
                    .map_err(|_| ClientError::Invalid("TTL exceeds u64 milliseconds".into()))
            })
            .transpose()?;
        self.single_mutation(wire::request::Operation::Put(wire::Put {
            key: key.into(),
            value,
            ttl_ms,
            priority: options.priority as i32,

            write_concern: None,
        }))
        .await
    }
    pub async fn put_with_ttl(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ClientError> {
        self.put_with_options(key, value, PutOptions::builder().maybe_ttl(ttl).build())
            .await
    }
    pub async fn put_with_priority(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        priority: Priority,
    ) -> Result<(), ClientError> {
        self.put_with_options(key, value, PutOptions::builder().priority(priority).build())
            .await
    }
    pub async fn delete(&self, key: impl Into<String>) -> Result<(), ClientError> {
        self.delete_with_priority(key, Priority::Default).await
    }
    pub async fn delete_with_priority(
        &self,
        key: impl Into<String>,
        priority: Priority,
    ) -> Result<(), ClientError> {
        self.single_mutation(wire::request::Operation::Delete(wire::Delete {
            key: key.into(),
            priority: priority as i32,

            write_concern: None,
        }))
        .await
    }
    async fn single_mutation(
        &self,
        operation: wire::request::Operation,
    ) -> Result<(), ClientError> {
        let mut response = self.request(operation, true).await?;
        let mut results = decode_mutations(std::mem::take(&mut response.mutations));
        if results.len() != 1 {
            return Err(ClientError::UnknownOutcome(
                "single mutation result cardinality".into(),
            ));
        }
        results.remove(0)
    }
    pub async fn get_multi(
        &self,
        keys: Vec<String>,
    ) -> Result<Vec<Result<Option<Value>, ClientError>>, ClientError> {
        self.get_multi_with_priority(keys, Priority::Background)
            .await
    }
    pub async fn get_multi_with_priority(
        &self,
        keys: Vec<String>,
        priority: Priority,
    ) -> Result<Vec<Result<Option<Value>, ClientError>>, ClientError> {
        if keys.len() > actordb_protocol::MAX_BATCH_ITEMS {
            return Err(ClientError::Invalid("too many batch keys".into()));
        }
        let count = keys.len();
        let mut response = self
            .request(
                wire::request::Operation::BatchGet(wire::BatchGet {
                    keys,
                    priority: priority as i32,
                }),
                false,
            )
            .await?;
        if response.reads.len() != count {
            return Err(ClientError::Protocol("batch get cardinality".into()));
        }
        Ok(decode_reads(std::mem::take(&mut response.reads)))
    }
    pub async fn put_multi(
        &self,
        values: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<Result<(), ClientError>>, ClientError> {
        self.put_multi_with_priority(values, Priority::Background)
            .await
    }
    pub async fn put_multi_with_priority(
        &self,
        values: Vec<(String, Vec<u8>)>,
        priority: Priority,
    ) -> Result<Vec<Result<(), ClientError>>, ClientError> {
        if values.len() > actordb_protocol::MAX_BATCH_ITEMS {
            return Err(ClientError::Invalid("too many batch items".into()));
        }
        let count = values.len();
        let puts = values
            .into_iter()
            .map(|(key, value)| wire::Put {
                key,
                value,
                ttl_ms: None,
                priority: priority as i32,

                write_concern: None,
            })
            .collect();
        let mut response = self
            .request(
                wire::request::Operation::BatchPut(wire::BatchPut {
                    puts,
                    priority: priority as i32,
                }),
                true,
            )
            .await?;
        if response.mutations.len() != count {
            return Err(ClientError::UnknownOutcome(
                "batch mutation cardinality".into(),
            ));
        }
        Ok(decode_mutations(std::mem::take(&mut response.mutations)))
    }
    pub async fn query(&self, sql: &str) -> Result<QueryResult, ClientError> {
        self.query_with_params(sql, &[]).await
    }
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: &[QueryValue],
    ) -> Result<QueryResult, ClientError> {
        if params.len() > actordb_protocol::MAX_BATCH_ITEMS {
            return Err(ClientError::Invalid("too many query parameters".into()));
        }
        if sql.len() > actordb_query::MAX_SQL_BYTES
            || params.iter().any(|value| match value {
                QueryValue::Text(value) => value.len() > actordb_query::MAX_QUERY_LITERAL_BYTES,
                QueryValue::Bytes(value) => value.len() > actordb_query::MAX_QUERY_LITERAL_BYTES,
                _ => false,
            })
        {
            return Err(ClientError::Invalid(
                "SQL or parameter exceeds query limits".into(),
            ));
        }
        let parameter_bytes =
            params
                .iter()
                .try_fold(sql.len().saturating_add(64), |size, value| {
                    let bytes = match value {
                        QueryValue::Text(value) => value.len(),
                        QueryValue::Bytes(value) => value.len(),
                        _ => 40,
                    };
                    size.checked_add(bytes.saturating_add(16))
                });
        if parameter_bytes.is_none_or(|bytes| bytes > MAX_RPC_MESSAGE_BYTES) {
            return Err(ClientError::Invalid(
                "query parameters exceed frame limit".into(),
            ));
        }
        let params = params.iter().map(encode_cell).collect();
        let mut response = self
            .request(
                wire::request::Operation::Query(wire::Query {
                    sql: sql.into(),
                    params,
                }),
                false,
            )
            .await?;
        let result = response
            .query
            .take()
            .ok_or_else(|| ClientError::Protocol("missing query result".into()))?;
        Ok(QueryResult {
            columns: result.columns,
            rows: result
                .rows
                .into_iter()
                .map(|row| row.cells.into_iter().map(decode_cell).collect())
                .collect::<Result<_, _>>()?,
        })
    }
    /// Local health and placement inspection of the contacted Node, not readiness.
    pub async fn inspect(&self) -> Result<Inspection, ClientError> {
        let mut response = self
            .request(wire::request::Operation::Inspect(wire::Inspect {}), false)
            .await?;
        response
            .inspection
            .take()
            .ok_or_else(|| ClientError::Protocol("missing inspection".into()))
    }
    /// Existing count-bounded scan API. Limits/offsets apply independently per fragment.
    pub async fn scan(
        &self,
        spec: wire::Scan,
    ) -> Result<Vec<actordb_protocol::meta::ScanMatch>, ClientError> {
        let mut response = self
            .request(wire::request::Operation::Scan(spec), false)
            .await?;
        Ok(std::mem::take(&mut response.scans))
    }
}

fn decode_reads(results: Vec<wire::ReadResult>) -> Vec<Result<Option<Value>, ClientError>> {
    results
        .into_iter()
        .map(|item| match item.error {
            Some(error) => Err(ClientError::Server(error)),
            None => Ok(item.value),
        })
        .collect()
}
fn decode_mutations(results: Vec<wire::MutationResult>) -> Vec<Result<(), ClientError>> {
    results
        .into_iter()
        .map(|item| match item.error {
            Some(error) => Err(server_error(error)),
            None => Ok(()),
        })
        .collect()
}
fn server_error(error: wire::Error) -> ClientError {
    if error.code == wire::ErrorCode::UnknownOutcome as i32 {
        ClientError::UnknownOutcome(error.message)
    } else {
        ClientError::Server(error)
    }
}

fn response_shape(
    request: &wire::Request,
) -> Result<actordb_protocol::RpcResponseShape, ClientError> {
    let mut shape = actordb_protocol::RpcResponseShape::from_request(request);
    if let Some(wire::request::Operation::Query(query)) = &request.operation {
        let params: Vec<_> = query
            .params
            .iter()
            .cloned()
            .map(decode_cell)
            .collect::<Result<_, _>>()?;
        // Invalid caller SQL retains the existing server-error behavior. It
        // does not relax structural or decoded-byte limits on the response.
        if let Ok(plan) = actordb_query::parse_with_params(&query.sql, &params) {
            shape = shape
                .with_query_shape(plan.projections.len(), plan.limit, plan.is_aggregate())
                .map_err(|error| ClientError::Invalid(error.into()))?;
        }
    }
    Ok(shape)
}

fn validate_response(
    request: &wire::Request,
    response: &wire::Response,
) -> Result<(), ClientError> {
    use wire::request::Operation;
    if response.error.is_some() {
        return Ok(());
    }
    let invalid =
        || ClientError::Protocol("response shape or ordered keys do not match request".into());
    let read_keys: Vec<&str> = match &request.operation {
        Some(Operation::Get(get)) => vec![&get.key],
        Some(Operation::BatchGet(batch)) => batch.keys.iter().map(String::as_str).collect(),
        _ => Vec::new(),
    };
    let mutation_keys: Vec<&str> = match &request.operation {
        Some(Operation::Put(put)) => vec![&put.key],
        Some(Operation::Delete(delete)) => vec![&delete.key],
        Some(Operation::BatchPut(batch)) => batch.puts.iter().map(|put| put.key.as_str()).collect(),
        _ => Vec::new(),
    };
    if response.reads.len() != read_keys.len()
        || response.reads.iter().zip(read_keys).any(|(result, key)| {
            result.key != key || (result.value.is_some() && result.error.is_some())
        })
        || response.mutations.len() != mutation_keys.len()
        || response
            .mutations
            .iter()
            .zip(mutation_keys)
            .any(|(result, key)| result.key != key)
    {
        return Err(invalid());
    }
    if response.query.is_some() != matches!(request.operation, Some(Operation::Query(_)))
        || response.inspection.is_some() != matches!(request.operation, Some(Operation::Inspect(_)))
        || (!response.scans.is_empty() && !matches!(request.operation, Some(Operation::Scan(_))))
    {
        return Err(invalid());
    }
    if let (Some(result), Some(Operation::Query(query))) = (&response.query, &request.operation) {
        let params: Vec<_> = query
            .params
            .iter()
            .cloned()
            .map(decode_cell)
            .collect::<Result<_, _>>()?;
        let plan = actordb_query::parse_with_params(&query.sql, &params).map_err(|_| invalid())?;
        if result.columns != plan.columns()
            || result
                .rows
                .iter()
                .any(|row| row.cells.len() != result.columns.len())
        {
            return Err(invalid());
        }
        if (plan.is_aggregate() && result.rows.len() != 1)
            || plan
                .limit
                .is_some_and(|limit| result.rows.len() as u64 > limit)
        {
            return Err(invalid());
        }
        for row in &result.rows {
            for (cell, projection) in row.cells.iter().zip(&plan.projections) {
                use actordb_query::Projection;
                let value = decode_cell(cell.clone())?;
                let valid = projection.accepts_value(&value)
                    && match (projection, &value) {
                        (Projection::Fragment, QueryValue::Integer(value)) => u32::try_from(*value)
                            .is_ok_and(|fragment| plan.matches_fragment(fragment)),
                        (Projection::NodeId, QueryValue::Text(node_id)) => {
                            !node_id.is_empty() && node_id.len() <= 255
                        }
                        _ => true,
                    };
                if !valid {
                    return Err(invalid());
                }
            }
        }
    }
    Ok(())
}
fn encode_cell(value: &QueryValue) -> actordb_protocol::meta::QueryCell {
    use actordb_protocol::meta::query_cell::Value as Cell;
    actordb_protocol::meta::QueryCell {
        value: Some(match value {
            QueryValue::Null => Cell::Null(true),
            QueryValue::Text(v) => Cell::Text(v.clone()),
            QueryValue::Bytes(v) => Cell::Bytes(v.clone()),
            QueryValue::Integer(v) => Cell::Integer(v.to_string()),
        }),
    }
}
fn decode_cell(cell: actordb_protocol::meta::QueryCell) -> Result<QueryValue, ClientError> {
    use actordb_protocol::meta::query_cell::Value as Cell;
    match cell.value {
        Some(Cell::Null(true)) => Ok(QueryValue::Null),
        Some(Cell::Text(v)) => Ok(QueryValue::Text(v)),
        Some(Cell::Bytes(v)) => Ok(QueryValue::Bytes(v)),
        Some(Cell::Integer(v)) => {
            let integer: i128 = v
                .parse()
                .map_err(|_| ClientError::Protocol("invalid decimal integer".into()))?;
            if integer.to_string() != v {
                return Err(ClientError::Protocol("noncanonical decimal integer".into()));
            }
            Ok(QueryValue::Integer(integer))
        }
        _ => Err(ClientError::Protocol("invalid query cell".into())),
    }
}
