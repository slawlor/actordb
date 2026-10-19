//! Async Rust companion for ActorDB's separate application listener.
//!
//! Clones share one TCP connection. Requests are serialized on that connection;
//! frame count, encoded bytes and total deadlines bound queued work. Use separate
//! Clients for parallel connections. Closing this handle never shuts down a Node.
//! Writes are never transparently retried: a lost reply after dispatch is an
//! [`ClientError::UnknownOutcome`], because repeating a write creates a new version.

use actordb_protocol::{
    client as wire, CLIENT_CAPABILITIES, CLIENT_PROTOCOL_VERSION, MAX_FRAME_BYTES,
};
use prost::Message;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{timeout_at, Instant};

pub use actordb_protocol::meta::{Priority, ScanMatch};
pub use actordb_query::{QueryResult, QueryValue};
pub use wire::Scan as ScanOptions;
pub use wire::{Inspection, Value};

mod write_concern;
pub use actordb_protocol::write_concern::{WriteConcern, WriteConcernOptions, WriteConcernReceipt};
pub use write_concern::WriteConcernClientError;

/// Connection and queue bounds. Credentials are independent of cluster secrets.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub identity: String,
    pub credential: Option<Vec<u8>>,
    pub timeout: Duration,
    pub max_queued_requests: usize,
    pub max_queued_bytes: usize,
}
impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            identity: "rust-client".into(),
            credential: None,
            timeout: Duration::from_secs(30),
            max_queued_requests: 64,
            max_queued_bytes: 32 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    pub ttl: Option<Duration>,
    pub priority: Priority,
}

#[derive(Debug)]
pub enum ClientError {
    Invalid(String),
    Authentication,
    Timeout,
    Closed,
    /// The configured request-count or encoded-byte queue is full.
    Overloaded,
    Transport(std::io::Error),
    Protocol(String),
    Server(wire::Error),
    /// The server may have applied the mutation. No automatic retry was made.
    UnknownOutcome(String),
}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(e) => write!(f, "invalid request: {e}"),
            Self::Authentication => write!(f, "application authentication failed"),
            Self::Timeout => write!(f, "request deadline exceeded"),
            Self::Closed => write!(f, "client connection closed"),
            Self::Overloaded => write!(f, "client request queue is full"),
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Protocol(e) => write!(f, "protocol: {e}"),
            Self::Server(e) => write!(f, "server error {}: {}", e.code, e.message),
            Self::UnknownOutcome(e) => write!(f, "mutation outcome unknown: {e}"),
        }
    }
}
impl std::error::Error for ClientError {}
impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Transport(e)
    }
}

struct Inner {
    stream: Mutex<Option<TcpStream>>,
    next_id: AtomicU64,
    count: Semaphore,
    bytes: Arc<Semaphore>,
    options: ClientOptions,
    capabilities: u64,
}
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub async fn connect(addr: SocketAddr, options: ClientOptions) -> Result<Self, ClientError> {
        if options.identity.is_empty()
            || options.identity.len() > 255
            || options.timeout.is_zero()
            || options.max_queued_requests == 0
            || options.max_queued_requests > Semaphore::MAX_PERMITS
            || options.max_queued_bytes < MAX_FRAME_BYTES
            || options.max_queued_bytes > u32::MAX as usize
            || options.max_queued_bytes > Semaphore::MAX_PERMITS
            || options
                .credential
                .as_ref()
                .is_some_and(|key| key.len() < 32)
        {
            return Err(ClientError::Invalid(
                "identity, timeout, queue bounds or credential length".into(),
            ));
        }
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .ok_or_else(|| ClientError::Invalid("timeout too large".into()))?;
        let (stream, capabilities) = timeout_at(deadline, async {
            let mut stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            let mut challenge = vec![0; 32];
            getrandom::fill(&mut challenge).map_err(|e| ClientError::Protocol(e.to_string()))?;
            let hello = wire::Hello {
                identity: options.identity.clone(),
                challenge,
                min_version: CLIENT_PROTOCOL_VERSION,
                max_version: CLIENT_PROTOCOL_VERSION,
                capabilities: CLIENT_CAPABILITIES,
            };
            send_frame(
                &mut stream,
                &wire::Frame {
                    message: Some(wire::frame::Message::Hello(hello.clone())),
                },
            )
            .await?;
            let server = match read_frame(&mut stream, actordb_protocol::MAX_HANDSHAKE_BYTES)
                .await?
                .message
            {
                Some(wire::frame::Message::Hello(hello))
                    if actordb_protocol::valid_client_hello(&hello) =>
                {
                    hello
                }
                _ => return Err(ClientError::Authentication),
            };
            let proof = options
                .credential
                .as_deref()
                .map(|key| actordb_protocol::client_auth_proof(key, true, &hello, &server))
                .unwrap_or_default();
            send_frame(
                &mut stream,
                &wire::Frame {
                    message: Some(wire::frame::Message::Auth(wire::Auth { proof })),
                },
            )
            .await?;
            match read_frame(&mut stream, actordb_protocol::MAX_HANDSHAKE_BYTES)
                .await?
                .message
            {
                Some(wire::frame::Message::Auth(auth))
                    if actordb_protocol::verify_client_auth(
                        options.credential.as_deref(),
                        false,
                        &hello,
                        &server,
                        &auth.proof,
                    ) => {}
                _ => return Err(ClientError::Authentication),
            }
            Ok((stream, hello.capabilities & server.capabilities))
        })
        .await
        .map_err(|_| ClientError::Timeout)??;
        Ok(Self {
            inner: Arc::new(Inner {
                stream: Mutex::new(Some(stream)),
                next_id: AtomicU64::new(1),
                count: Semaphore::new(options.max_queued_requests),
                bytes: Arc::new(Semaphore::new(options.max_queued_bytes)),
                options,
                capabilities,
            }),
        })
    }

    async fn request(
        &self,
        operation: wire::request::Operation,
        mutation: bool,
    ) -> Result<wire::Response, ClientError> {
        self.request_with_timeout(operation, mutation, Duration::ZERO)
            .await
    }

    async fn request_with_timeout(
        &self,
        operation: wire::request::Operation,
        mutation: bool,
        extra: Duration,
    ) -> Result<wire::Response, ClientError> {
        let request_timeout = self
            .inner
            .options
            .timeout
            .checked_add(extra)
            .ok_or_else(|| ClientError::Invalid("timeout too large".into()))?;
        let deadline = Instant::now()
            .checked_add(request_timeout)
            .ok_or_else(|| ClientError::Invalid("timeout too large".into()))?;
        // Admission is nonwaiting, so excess callers never retain payloads in
        // an unaccounted semaphore waiter queue.
        let _count = self.inner.count.try_acquire().map_err(|_| {
            if self.inner.count.is_closed() {
                ClientError::Closed
            } else {
                ClientError::Overloaded
            }
        })?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = wire::Frame {
            message: Some(wire::frame::Message::Request(wire::Request {
                id,
                operation: Some(operation),
            })),
        };
        let length = frame.encoded_len();
        if length > MAX_FRAME_BYTES {
            return Err(ClientError::Invalid("request exceeds frame limit".into()));
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
        let mut guard = timeout_at(deadline, self.inner.stream.lock())
            .await
            .map_err(|_| ClientError::Timeout)?;
        // Taking ownership makes cancellation close the socket. A partial
        // prefix/body can never be reused by the next queued request.
        let mut stream = guard.take().ok_or(ClientError::Closed)?;
        let mut dispatched = false;
        let result = timeout_at(deadline, async {
            // Mark before write: even a failing write can transmit a complete request.
            dispatched = true;
            send_frame(&mut stream, &frame).await?;
            match read_frame(&mut stream, MAX_FRAME_BYTES).await?.message {
                Some(wire::frame::Message::Response(response)) if response.id == id => {
                    validate_response(&frame, &response)?;
                    Ok(response)
                }
                _ => Err(ClientError::Protocol(
                    "unexpected response or request id".into(),
                )),
            }
        })
        .await
        .unwrap_or(Err(ClientError::Timeout));
        match result {
            Ok(response) => {
                *guard = Some(stream);
                if let Some(error) = response.error.clone() {
                    Err(server_error(error))
                } else {
                    Ok(response)
                }
            }
            Err(error) => {
                *guard = None;
                if mutation && dispatched {
                    Err(ClientError::UnknownOutcome(error.to_string()))
                } else {
                    Err(error)
                }
            }
        }
    }

    pub async fn close(&self) {
        self.inner.count.close();
        self.inner.bytes.close();
        if let Some(mut stream) = self.inner.stream.lock().await.take() {
            let _ = stream.shutdown().await;
        }
    }
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<Value>, ClientError> {
        self.get_with_priority(key, Priority::Default).await
    }
    pub async fn get_with_priority(
        &self,
        key: impl Into<String>,
        priority: Priority,
    ) -> Result<Option<Value>, ClientError> {
        let response = self
            .request(
                wire::request::Operation::Get(wire::Get {
                    key: key.into(),
                    priority: priority as i32,
                }),
                false,
            )
            .await?;
        let mut results = decode_reads(response.reads);
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
        self.put_with_options(
            key,
            value,
            PutOptions {
                ttl,
                ..PutOptions::default()
            },
        )
        .await
    }
    pub async fn put_with_priority(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        priority: Priority,
    ) -> Result<(), ClientError> {
        self.put_with_options(
            key,
            value,
            PutOptions {
                priority,
                ..PutOptions::default()
            },
        )
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
        let response = self.request(operation, true).await?;
        let mut results = decode_mutations(response.mutations);
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
        let response = self
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
        Ok(decode_reads(response.reads))
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
        let response = self
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
        Ok(decode_mutations(response.mutations))
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
        if parameter_bytes.is_none_or(|bytes| bytes > MAX_FRAME_BYTES) {
            return Err(ClientError::Invalid(
                "query parameters exceed frame limit".into(),
            ));
        }
        if self.inner.capabilities & actordb_protocol::CLIENT_CAPABILITY_QUERY == 0 {
            return Err(ClientError::Invalid(
                "server does not support queries".into(),
            ));
        }
        let params = params.iter().map(encode_cell).collect();
        let response = self
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
        self.request(wire::request::Operation::Inspect(wire::Inspect {}), false)
            .await?
            .inspection
            .ok_or_else(|| ClientError::Protocol("missing inspection".into()))
    }
    /// Existing count-bounded scan API. Limits/offsets apply independently per fragment.
    pub async fn scan(
        &self,
        spec: wire::Scan,
    ) -> Result<Vec<actordb_protocol::meta::ScanMatch>, ClientError> {
        if self.inner.capabilities & actordb_protocol::CLIENT_CAPABILITY_SCAN == 0 {
            return Err(ClientError::Invalid("server does not support scans".into()));
        }
        Ok(self
            .request(wire::request::Operation::Scan(spec), false)
            .await?
            .scans)
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

fn validate_response(frame: &wire::Frame, response: &wire::Response) -> Result<(), ClientError> {
    use wire::request::Operation;
    let Some(wire::frame::Message::Request(request)) = &frame.message else {
        unreachable!()
    };
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
                use actordb_protocol::meta::query_cell::Value;
                use actordb_query::Projection;
                let value = decode_cell(cell.clone())?;
                let valid = match (projection, &cell.value, &value) {
                    (Projection::Key, Some(Value::Text(_)), _) => true,
                    (Projection::Value, Some(Value::Bytes(_)), _) => true,
                    (Projection::PrefixBytes(count), Some(Value::Bytes(bytes)), _) => {
                        bytes.len() <= *count as usize
                    }
                    (Projection::Fragment, _, QueryValue::Integer(value)) => {
                        u32::try_from(*value).is_ok_and(|fragment| plan.matches_fragment(fragment))
                    }
                    (Projection::OctetLength, _, QueryValue::Integer(value)) => {
                        u64::try_from(*value).is_ok()
                    }
                    (
                        Projection::Aggregate {
                            function: actordb_query::AggregateFunction::Count,
                            ..
                        },
                        _,
                        QueryValue::Integer(value),
                    ) => *value >= 0 && *value <= u64::MAX as i128,
                    (
                        Projection::Aggregate {
                            function: actordb_query::AggregateFunction::Count,
                            ..
                        },
                        _,
                        _,
                    ) => false,
                    (
                        Projection::Aggregate {
                            input: actordb_query::AggregateInput::OctetLength,
                            ..
                        },
                        _,
                        QueryValue::Integer(value),
                    ) => *value >= 0,
                    (
                        Projection::Aggregate {
                            function:
                                actordb_query::AggregateFunction::Min
                                | actordb_query::AggregateFunction::Max,
                            input: actordb_query::AggregateInput::BigInt,
                        },
                        _,
                        QueryValue::Integer(value),
                    ) => i64::try_from(*value).is_ok(),
                    (
                        Projection::Aggregate { .. },
                        _,
                        QueryValue::Integer(_) | QueryValue::Null,
                    ) => true,
                    _ => false,
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

async fn send_frame(stream: &mut TcpStream, frame: &wire::Frame) -> Result<(), ClientError> {
    if frame.encoded_len() > MAX_FRAME_BYTES {
        return Err(ClientError::Invalid("frame exceeds limit".into()));
    }
    let bytes = frame.encode_to_vec();
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}
async fn read_frame(stream: &mut TcpStream, max: usize) -> Result<wire::Frame, ClientError> {
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > max {
        return Err(ClientError::Protocol("invalid frame length".into()));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    actordb_protocol::preflight_client_frame(&bytes)
        .map_err(|e| ClientError::Protocol(e.into()))?;
    wire::Frame::decode(bytes.as_slice()).map_err(|e| ClientError::Protocol(e.to_string()))
}
