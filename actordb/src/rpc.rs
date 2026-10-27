//! Framework-neutral application service for any unary RPC adapter.
//! Authentication and authorization are intentionally absent. Peer networking
//! and actor execution are independent of this service.

use crate::actor::{OpPriority, VersionedValue};
use crate::node::{Node, NodeError, Table};
pub use actordb_protocol::rpc::{RpcError, RpcFuture, RpcReply, RpcRequest, RpcService};
use actordb_protocol::{client as wire, MAX_RPC_MESSAGE_BYTES};
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct RpcServiceOptions {
    #[builder(default = 64)]
    pub max_in_flight_requests: usize,
    #[builder(default = 64 * 1024 * 1024)]
    pub max_request_bytes: usize,
    #[builder(default = Duration::from_secs(30))]
    pub timeout: Duration,
}
impl Default for RpcServiceOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}
struct Inner {
    node: Arc<Node>,
    options: RpcServiceOptions,
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    idle: Arc<Notify>,
}
#[derive(Clone)]
pub struct NodeRpcService {
    inner: Arc<Inner>,
}
struct RequestLease {
    count: Option<tokio::sync::OwnedSemaphorePermit>,
    bytes: Option<tokio::sync::OwnedSemaphorePermit>,
    idle: Arc<Notify>,
}
impl Drop for RequestLease {
    fn drop(&mut self) {
        self.bytes.take();
        self.count.take();
        self.idle.notify_waiters();
    }
}
impl NodeRpcService {
    pub fn new(node: Arc<Node>, options: RpcServiceOptions) -> Result<Self, NodeError> {
        if options.max_in_flight_requests == 0
            || options.max_in_flight_requests > Semaphore::MAX_PERMITS
            || options.max_request_bytes < MAX_RPC_MESSAGE_BYTES
            || options.max_request_bytes > u32::MAX as usize
            || options.max_request_bytes > Semaphore::MAX_PERMITS
            || options.timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(options.timeout)
                .is_none()
        {
            return Err(NodeError::Codec("invalid RPC service limits".into()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                requests: Arc::new(Semaphore::new(options.max_in_flight_requests)),
                bytes: Arc::new(Semaphore::new(options.max_request_bytes)),
                idle: Arc::new(Notify::new()),
                node,
                options,
            }),
        })
    }
    /// Stop service admission and join request/response ownership, not the Node.
    /// A timed-out caller may retry shutdown; admitted mutation jobs stay owned.
    pub async fn shutdown(&self, budget: Duration) -> Result<(), NodeError> {
        self.inner.requests.close();
        tokio::time::timeout(budget, async {
            loop {
                let idle = self.inner.idle.notified();
                if self.inner.requests.available_permits()
                    == self.inner.options.max_in_flight_requests
                {
                    break;
                }
                idle.await;
            }
        })
        .await
        .map_err(|_| NodeError::Actor("RPC shutdown still owns requests".into()))
    }
}
fn rejected(code: wire::ErrorCode, message: &str) -> RpcReply {
    RpcReply::new(wire::Response {
        error: Some(error(code, message)),
        ..Default::default()
    })
}
impl RpcService for NodeRpcService {
    fn call(&self, envelope: RpcRequest) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            // Frameworks must bound materialization before constructing this
            // envelope. The core adds count/encoded-byte admission and semantic
            // validation, without interpreting transport/authentication state.
            let (request, receive_ownership) = envelope.into_parts();
            if request.table.is_empty() || request.table.len() > 255 || request.operation.is_none()
            {
                return Ok(rejected(
                    wire::ErrorCode::Invalid,
                    "table and operation required",
                ));
            }
            let size = request.encoded_len();
            if size > MAX_RPC_MESSAGE_BYTES {
                return Ok(rejected(
                    wire::ErrorCode::ResourceLimit,
                    "RPC request exceeds byte limit",
                ));
            }
            let count = match inner.requests.clone().try_acquire_owned() {
                Ok(count) => count,
                Err(_) => {
                    return Ok(rejected(
                        wire::ErrorCode::Unavailable,
                        "RPC service admission closed or full",
                    ))
                }
            };
            let mut lease = RequestLease {
                count: Some(count),
                bytes: None,
                idle: inner.idle.clone(),
            };
            let bytes = match inner.bytes.clone().try_acquire_many_owned(size as u32) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Ok(rejected(
                        wire::ErrorCode::Backpressure,
                        "RPC request byte admission full",
                    ))
                }
            };
            lease.bytes = Some(bytes);
            let operation = match inner.node.admit_client_request_for_table(&request.table) {
                Ok(operation) => operation,
                Err(_) => {
                    return Ok(rejected(
                        wire::ErrorCode::Unavailable,
                        "node is shutting down",
                    ))
                }
            };
            let mutation = matches!(
                request.operation,
                Some(
                    wire::request::Operation::Put(_)
                        | wire::request::Operation::Delete(_)
                        | wire::request::Operation::BatchPut(_)
                )
            );
            let (mut send, receive) = tokio::sync::oneshot::channel();
            // This detached *owned* task holds all admission leases and the
            // Node operation permit. Dropping an HTTP/Thrift waiter cannot
            // interrupt an accepted mutation's apply and fan-out.
            tokio::spawn(async move {
                let _receive_ownership = receive_ownership;
                let mut response = if mutation {
                    dispatch(&inner.node, request).await
                } else {
                    let result = tokio::select! {
                        biased;
                        _ = send.closed() => return,
                        result = tokio::time::timeout(inner.options.timeout, dispatch(&inner.node, request)) => result,
                    };
                    match result {
                        Ok(response) => response,
                        Err(_) => wire::Response {
                            error: Some(error(
                                wire::ErrorCode::Unavailable,
                                "RPC read deadline elapsed",
                            )),
                            ..Default::default()
                        },
                    }
                };
                if response.encoded_len() > MAX_RPC_MESSAGE_BYTES
                    || !response_counts_valid(&response)
                {
                    response = wire::Response {
                        error: Some(error(
                            if mutation {
                                wire::ErrorCode::UnknownOutcome
                            } else {
                                wire::ErrorCode::ResourceLimit
                            },
                            "complete RPC response exceeds byte or count limit",
                        )),
                        ..Default::default()
                    };
                }
                operation.mark_responding();
                let mut reply = RpcReply::new(response);
                reply.retain(operation);
                reply.retain(lease);
                let _ = send.send(reply);
            });
            receive
                .await
                .map_err(|_| RpcError::Failure("RPC execution owner failed".into()))
        })
    }
}

fn response_counts_valid(response: &wire::Response) -> bool {
    response.reads.len() <= actordb_protocol::MAX_BATCH_ITEMS
        && response.mutations.len() <= actordb_protocol::MAX_BATCH_ITEMS
        && response.scans.len() <= 100_000
        && response.query.as_ref().is_none_or(|query| {
            query.columns.len() <= 32
                && query.rows.len() <= 100_000
                && query.rows.iter().all(|row| row.cells.len() <= 32)
        })
        && response.inspection.as_ref().is_none_or(|inspection| {
            inspection.health.len() <= 100_000
                && inspection.placement.len() <= 100_000
                && inspection.health.iter().all(|health| {
                    health.worker_names.len() <= actordb_protocol::MAX_BATCH_ITEMS
                        && health.restarts.len() <= actordb_protocol::MAX_BATCH_ITEMS
                })
                && inspection
                    .placement
                    .iter()
                    .all(|row| row.replica_ids.len() <= actordb_protocol::MAX_BATCH_ITEMS)
        })
}

enum DispatchTarget<'a> {
    Table(Table<'a>),
}

impl DispatchTarget<'_> {
    async fn get(
        &self,
        key: String,
        priority: crate::actor::OpPriority,
    ) -> Result<Option<VersionedValue>, NodeError> {
        let Self::Table(table) = self;
        table.get_with_priority(key, priority).await
    }

    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: crate::actor::OpPriority,
    ) -> Result<(), NodeError> {
        let Self::Table(table) = self;
        table.put_with_options(key, value, ttl, priority).await
    }

    async fn put_with_concern(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: crate::actor::OpPriority,
        options: crate::write_concern::WriteConcernOptions,
    ) -> Result<crate::write_concern::WriteConcernReceipt, crate::write_concern::WriteConcernError>
    {
        let Self::Table(table) = self;
        table
            .put_with_write_concern_options(key, value, ttl, priority, options)
            .await
    }

    async fn delete(
        &self,
        key: String,
        priority: crate::actor::OpPriority,
    ) -> Result<(), NodeError> {
        let Self::Table(table) = self;
        table.delete_with_priority(key, priority).await
    }

    async fn delete_with_concern(
        &self,
        key: String,
        priority: crate::actor::OpPriority,
        options: crate::write_concern::WriteConcernOptions,
    ) -> Result<crate::write_concern::WriteConcernReceipt, crate::write_concern::WriteConcernError>
    {
        let Self::Table(table) = self;
        table
            .delete_with_write_concern_options(key, priority, options)
            .await
    }

    async fn query(
        &self,
        sql: &str,
        params: &[actordb_query::QueryValue],
    ) -> Result<actordb_query::QueryResult, crate::query::QueryExecutionError> {
        let Self::Table(table) = self;
        table.query_with_params(sql, params).await
    }

    async fn scan(
        &self,
        spec: &crate::actor::ScanSpec,
    ) -> Result<Vec<crate::actor::ScanMatch>, crate::node::ScanError> {
        let Self::Table(table) = self;
        table.scan(spec).await
    }
}

async fn dispatch(node: &Node, request: wire::Request) -> wire::Response {
    use wire::request::Operation;
    let mut response = wire::Response {
        ..Default::default()
    };
    let target = match node.table(request.table.clone()) {
        Ok(table) => DispatchTarget::Table(table),
        Err(failure) => {
            response.error = Some(error(wire::ErrorCode::Invalid, &failure.to_string()));
            return response;
        }
    };
    let priority_is_valid = match &request.operation {
        Some(wire::request::Operation::Get(get)) => {
            actordb_protocol::meta::Priority::try_from(get.priority).is_ok()
        }
        Some(wire::request::Operation::Put(put)) => {
            actordb_protocol::meta::Priority::try_from(put.priority).is_ok()
        }
        Some(wire::request::Operation::Delete(delete)) => {
            actordb_protocol::meta::Priority::try_from(delete.priority).is_ok()
        }
        Some(wire::request::Operation::BatchGet(batch)) => {
            actordb_protocol::meta::Priority::try_from(batch.priority).is_ok()
        }
        Some(wire::request::Operation::BatchPut(batch)) => {
            actordb_protocol::meta::Priority::try_from(batch.priority).is_ok()
                && batch
                    .puts
                    .iter()
                    .all(|put| actordb_protocol::meta::Priority::try_from(put.priority).is_ok())
        }
        _ => true,
    };
    if !priority_is_valid {
        response.error = Some(error(wire::ErrorCode::Invalid, "unknown priority"));
        return response;
    }
    match request.operation {
        Some(Operation::Get(get)) => {
            let result = target.get(get.key.clone(), priority(get.priority)).await;
            response.reads.push(read_result(get.key, result));
        }
        Some(Operation::Put(put)) => {
            if let Some(encoded) = put.write_concern.as_deref() {
                let result = match crate::write_concern::WriteConcernOptions::decode(encoded) {
                    Ok(options) => {
                        target
                            .put_with_concern(
                                put.key.clone(),
                                put.value,
                                put.ttl_ms.map(Duration::from_millis),
                                priority(put.priority),
                                options,
                            )
                            .await
                    }
                    Err(error) => Err(error.into()),
                };
                response
                    .mutations
                    .push(concern_mutation_result(put.key, result));
                return response;
            }
            let result = target
                .put(
                    put.key.clone(),
                    put.value,
                    put.ttl_ms.map(Duration::from_millis),
                    priority(put.priority),
                )
                .await;
            response.mutations.push(mutation_result(put.key, result));
        }
        Some(Operation::Delete(delete)) => {
            if let Some(encoded) = delete.write_concern.as_deref() {
                let result = match crate::write_concern::WriteConcernOptions::decode(encoded) {
                    Ok(options) => {
                        target
                            .delete_with_concern(
                                delete.key.clone(),
                                priority(delete.priority),
                                options,
                            )
                            .await
                    }
                    Err(error) => Err(error.into()),
                };
                response
                    .mutations
                    .push(concern_mutation_result(delete.key, result));
                return response;
            }
            let result = target
                .delete(delete.key.clone(), priority(delete.priority))
                .await;
            response.mutations.push(mutation_result(delete.key, result));
        }
        Some(Operation::BatchGet(batch)) => {
            if batch.keys.len() > actordb_protocol::MAX_BATCH_ITEMS {
                response.error = Some(error(wire::ErrorCode::ResourceLimit, "too many batch keys"));
            } else {
                for key in batch.keys {
                    let result = target.get(key.clone(), priority(batch.priority)).await;
                    response.reads.push(read_result(key, result));
                    if response.encoded_len() + 16 > MAX_RPC_MESSAGE_BYTES {
                        response.reads.clear();
                        response.error = Some(error(
                            wire::ErrorCode::ResourceLimit,
                            "batch result exceeds frame limit",
                        ));
                        break;
                    }
                }
            }
        }
        Some(Operation::BatchPut(batch)) => {
            if batch.puts.iter().any(|put| put.write_concern.is_some()) {
                response.error = Some(error(
                    wire::ErrorCode::Unsupported,
                    "batch write concerns are not supported",
                ));
                return response;
            }
            if batch.puts.len() > actordb_protocol::MAX_BATCH_ITEMS {
                response.error = Some(error(wire::ErrorCode::ResourceLimit, "too many batch puts"));
            } else {
                // Apply in input order, retaining duplicate positions. The batch
                // is not a transaction: results describe each individual write.
                for put in batch.puts {
                    let result = target
                        .put(
                            put.key.clone(),
                            put.value,
                            put.ttl_ms.map(Duration::from_millis),
                            priority(batch.priority),
                        )
                        .await;
                    response.mutations.push(mutation_result(put.key, result));
                }
            }
        }
        Some(Operation::Query(query)) => {
            let params = query
                .params
                .into_iter()
                .map(decode_cell)
                .collect::<Result<Vec<_>, _>>();
            match params {
                Err(message) => response.error = Some(error(wire::ErrorCode::Invalid, message)),
                Ok(params) => match target.query(&query.sql, &params).await {
                    Ok(result) => {
                        response.query = Some(wire::QueryResult {
                            columns: result.columns,
                            rows: result
                                .rows
                                .into_iter()
                                .map(|cells| wire::QueryRow {
                                    cells: cells.into_iter().map(encode_cell).collect(),
                                })
                                .collect(),
                        })
                    }
                    Err(failure) => response.error = Some(query_error(failure)),
                },
            }
        }
        Some(Operation::Inspect(_)) => match node.health().await {
            Err(failure) => response.error = Some(node_error(failure)),
            Ok(health) => {
                let placement = node.placement_report().await;
                let mut ids: HashSet<_> = placement
                    .placement
                    .keys()
                    .chain(placement.states.keys())
                    .copied()
                    .collect();
                ids.extend(health.keys().copied());
                let mut placement_rows: Vec<_> = ids
                    .into_iter()
                    .map(|fragment| wire::Placement {
                        fragment: fragment.0,
                        replica_ids: placement
                            .placement
                            .get(&fragment)
                            .cloned()
                            .unwrap_or_default(),
                        state: placement
                            .states
                            .get(&fragment)
                            .map(|state| format!("{state:?}"))
                            .unwrap_or_else(|| {
                                if health.contains_key(&fragment) {
                                    "Serving".into()
                                } else {
                                    "NotOwned".into()
                                }
                            }),
                    })
                    .collect();
                placement_rows.sort_by_key(|row| row.fragment);
                let mut health: Vec<_> = health
                    .into_values()
                    .map(|item| wire::FragmentHealth {
                        fragment: item.fragment.0,
                        worker_count: item.worker_count as u64,
                        worker_names: item.worker_names,
                        restarts: item.restarts,
                        shed_high: item.shed_counts.0,
                        shed_default: item.shed_counts.1,
                        shed_background: item.shed_counts.2,
                    })
                    .collect();
                health.sort_by_key(|row| row.fragment);
                response.inspection = Some(wire::Inspection {
                    node_id: node.id().into(),
                    peer_addr: node.addr().to_string(),
                    peer_count: node.peer_count().await as u64,
                    health,
                    placement: placement_rows,
                    dynamic: placement.dynamic,
                });
            }
        },
        Some(Operation::Scan(scan)) => {
            let request = actordb_protocol::meta::ScanRequest {
                matcher_kind: scan.matcher_kind,
                matcher_arg: scan.prefix,
                transform_kind: scan.transform_kind,
                transform_arg: scan.prefix_bytes,
                limit: scan.limit,
                offset: scan.offset,
                ..Default::default()
            };
            match crate::actor::ScanSpec::from_scan_request(&request) {
                Err(message) => response.error = Some(error(wire::ErrorCode::Invalid, &message)),
                Ok(spec) => match target.scan(&spec).await {
                    Ok(matches) => {
                        response.scans = matches.into_iter().map(|item| item.to_proto()).collect()
                    }
                    Err(failure) => {
                        response.error =
                            Some(error(wire::ErrorCode::Unavailable, &failure.to_string()))
                    }
                },
            }
        }
        None => {
            response.error = Some(error(
                wire::ErrorCode::Unsupported,
                "missing or unsupported operation",
            ))
        }
    }
    response
}
fn priority(value: i32) -> OpPriority {
    match actordb_protocol::meta::Priority::try_from(value) {
        Ok(actordb_protocol::meta::Priority::High) => OpPriority::High,
        Ok(actordb_protocol::meta::Priority::Background) => OpPriority::Background,
        _ => OpPriority::Default,
    }
}
fn read_result(key: String, result: Result<Option<VersionedValue>, NodeError>) -> wire::ReadResult {
    match result {
        Ok(value) => wire::ReadResult {
            key,
            value: value.map(|value| wire::Value {
                bytes: value.value,
                timestamp_ms: value.timestamp_ms,
                logical_counter: value.logical_counter,
                node_id: value.node_id,
                expires_at_ms: value.expires_at_ms,
            }),
            error: None,
        },
        Err(failure) => wire::ReadResult {
            key,
            value: None,
            error: Some(node_error(failure)),
        },
    }
}
fn mutation_result(key: String, result: Result<(), NodeError>) -> wire::MutationResult {
    wire::MutationResult {
        key,
        error: result.err().map(|failure| {
            if matches!(
                failure,
                NodeError::Io(_) | NodeError::Network(_) | NodeError::Actor(_)
            ) {
                error(wire::ErrorCode::UnknownOutcome, &failure.to_string())
            } else {
                node_error(failure)
            }
        }),

        write_concern: None,
    }
}

fn concern_mutation_result(
    key: String,
    result: Result<
        crate::write_concern::WriteConcernReceipt,
        crate::write_concern::WriteConcernError,
    >,
) -> wire::MutationResult {
    use crate::write_concern::{WriteConcernError, WriteConcernOutcome};
    match result {
        Ok(receipt) => wire::MutationResult {
            key,
            error: None,
            write_concern: Some(WriteConcernOutcome::Satisfied(receipt).encode()),
        },
        Err(WriteConcernError::Operation(error)) => mutation_result(key, Err(error)),
        Err(failure) => {
            let encoded = failure.outcome().map(|outcome| outcome.encode());
            let code = match failure {
                WriteConcernError::TimedOut(_) | WriteConcernError::UnknownOutcome(_) => {
                    wire::ErrorCode::UnknownOutcome
                }
                WriteConcernError::Backpressure => wire::ErrorCode::Backpressure,
                WriteConcernError::DurabilityFailed { .. } => wire::ErrorCode::Storage,
                WriteConcernError::Unsupported(_) => wire::ErrorCode::Unsupported,
                _ => wire::ErrorCode::Invalid,
            };
            wire::MutationResult {
                key,
                error: Some(error(code, &failure.to_string())),
                write_concern: encoded,
            }
        }
    }
}
fn error(code: wire::ErrorCode, message: &str) -> wire::Error {
    wire::Error {
        code: code as i32,
        message: message.chars().take(1024).collect(),
        ..Default::default()
    }
}
fn node_error(failure: NodeError) -> wire::Error {
    use wire::ErrorCode;
    let code = match &failure {
        NodeError::Io(_) | NodeError::Actor(_) | NodeError::Network(_) => ErrorCode::Unavailable,
        NodeError::UnknownOutcome(_) => ErrorCode::UnknownOutcome,
        NodeError::Codec(_) => ErrorCode::Invalid,
        NodeError::Backpressure(_) => ErrorCode::Backpressure,
        NodeError::Storage(_) => ErrorCode::Storage,
        NodeError::Superseded { .. } => ErrorCode::Superseded,
        NodeError::Expired => ErrorCode::Expired,
        NodeError::ShuttingDown => ErrorCode::ShuttingDown,
    };
    let mut result = error(code, &failure.to_string());
    if let NodeError::Superseded {
        timestamp_ms,
        logical_counter,
        node_id,
        is_tombstone,
    } = failure
    {
        result.winner_timestamp_ms = timestamp_ms;
        result.winner_logical_counter = logical_counter;
        result.winner_node_id = node_id;
        result.winner_is_tombstone = is_tombstone;
    }
    result
}
fn query_error(failure: crate::query::QueryExecutionError) -> wire::Error {
    use crate::query::QueryExecutionError;
    let code = match failure {
        QueryExecutionError::InvalidQuery(_) => wire::ErrorCode::Invalid,
        QueryExecutionError::BudgetExceeded(_) => wire::ErrorCode::ResourceLimit,
        QueryExecutionError::Backpressure(_) => wire::ErrorCode::Backpressure,
        QueryExecutionError::QueryChanged(_) => wire::ErrorCode::QueryChanged,
        QueryExecutionError::ShuttingDown => wire::ErrorCode::ShuttingDown,
        _ => wire::ErrorCode::Unavailable,
    };
    error(code, &failure.to_string())
}
fn encode_cell(value: actordb_query::QueryValue) -> actordb_protocol::meta::QueryCell {
    use actordb_protocol::meta::query_cell::Value;
    actordb_protocol::meta::QueryCell {
        value: Some(match value {
            actordb_query::QueryValue::Null => Value::Null(true),
            actordb_query::QueryValue::Text(v) => Value::Text(v),
            actordb_query::QueryValue::Bytes(v) => Value::Bytes(v),
            actordb_query::QueryValue::Integer(v) => Value::Integer(v.to_string()),
        }),
    }
}
fn decode_cell(
    value: actordb_protocol::meta::QueryCell,
) -> Result<actordb_query::QueryValue, &'static str> {
    use actordb_protocol::meta::query_cell::Value;
    match value.value {
        Some(Value::Null(true)) => Ok(actordb_query::QueryValue::Null),
        Some(Value::Text(v)) => Ok(actordb_query::QueryValue::Text(v)),
        Some(Value::Bytes(v)) => Ok(actordb_query::QueryValue::Bytes(v)),
        Some(Value::Integer(v)) => {
            let integer: i128 = v.parse().map_err(|_| "invalid integer parameter")?;
            if integer.to_string() != v {
                return Err("noncanonical integer parameter");
            }
            Ok(actordb_query::QueryValue::Integer(integer))
        }
        _ => Err("invalid parameter"),
    }
}
