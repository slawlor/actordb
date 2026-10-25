//! Opt-in application listener composed around an existing [`crate::node::Node`].
//!
//! Application clients never join peer membership. Credentials are separate
//! from cluster secrets. TCP authentication proves possession but does not
//! encrypt traffic; supply a protected [`Network`](crate::network::Network)
//! transport when needed.
//! Listener shutdown closes application sessions and leaves the Node running.

use crate::actor::{OpPriority, VersionedValue};
use crate::network::{Acceptor, BoxStream, Network};
use crate::node::{Node, NodeError, Table};
use actordb_protocol::{
    client as wire, CLIENT_CAPABILITIES, CLIENT_PROTOCOL_VERSION, MAX_FRAME_BYTES,
};
use prost::Message;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

/// Resource limits and optional application credential. Never reuse a cluster key.
#[derive(Clone, Debug)]
pub struct ClientServerOptions {
    pub credential: Option<Vec<u8>>,
    pub max_connections: usize,
    pub max_in_flight_requests: usize,
    pub receive_bytes: usize,
    pub timeout: Duration,
}
impl Default for ClientServerOptions {
    fn default() -> Self {
        Self {
            credential: None,
            max_connections: 64,
            max_in_flight_requests: 8,
            receive_bytes: 64 * 1024 * 1024,
            timeout: Duration::from_secs(30),
        }
    }
}
/// Owned application listener. Drop requests cancellation; [`Self::shutdown`]
/// additionally joins the listener and all application connection tasks.
pub struct ClientServer {
    addr: SocketAddr,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl ClientServer {
    /// Bind a plain TCP application listener.
    pub async fn bind(
        node: Arc<Node>,
        addr: SocketAddr,
        options: ClientServerOptions,
    ) -> Result<Self, NodeError> {
        Self::validate_options(&options)?;
        let (addr, acceptor) = crate::network::bind_tcp(addr).await?;
        Self::from_acceptor(node, addr, acceptor, options)
    }

    /// Bind an application listener using a caller-configured transport.
    ///
    /// The transport supplies byte streams; the application protocol and its
    /// credentials remain independent of peer membership and cluster secrets.
    /// Transport operations must obey [`Network`]'s cancellation and ownership
    /// contract. TLS configuration and certificate validation belong to the caller.
    pub async fn bind_with_network(
        node: Arc<Node>,
        addr: SocketAddr,
        network: Arc<dyn Network>,
        options: ClientServerOptions,
    ) -> Result<Self, NodeError> {
        Self::validate_options(&options)?;
        let (addr, acceptor) = network
            .bind(addr)
            .await
            .map_err(|e| NodeError::Network(e.to_string()))?;
        Self::from_acceptor(node, addr, acceptor, options)
    }

    /// Serve application connections from an existing acceptor.
    ///
    /// `addr` is the listener address reported by [`Self::addr`]. Ownership of
    /// the acceptor and every accepted stream transfers to this server, including
    /// on validation failure. Shutdown cancels pending accepts, releases the
    /// listener, and joins application sessions without shutting down the Node.
    pub fn from_acceptor(
        node: Arc<Node>,
        addr: SocketAddr,
        mut acceptor: Box<dyn Acceptor>,
        options: ClientServerOptions,
    ) -> Result<Self, NodeError> {
        Self::validate_options(&options)?;
        let (cancel, mut cancellation) = watch::channel(false);
        let connections = Arc::new(Semaphore::new(options.max_connections));
        let requests = Arc::new(Semaphore::new(options.max_in_flight_requests));
        let bytes = Arc::new(Semaphore::new(options.receive_bytes));
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    _ = cancellation.changed() => break,
                    Some(_) = sessions.join_next(), if !sessions.is_empty() => {},
                    accepted = acceptor.accept() => {
                        let stream = match accepted {
                            Ok(stream) => stream,
                            Err(error) => {
                                tracing::warn!(%error, "application accept failed; retrying");
                                tokio::select! {
                                    _ = cancellation.changed() => break,
                                    _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                                }
                                continue;
                            }
                        };
                        let Ok(permit) = connections.clone().try_acquire_owned() else { continue; };
                        let node = node.clone(); let options = options.clone();
                        let bytes = bytes.clone(); let requests = requests.clone();
                        let mut cancellation = cancellation.clone();
                        sessions.spawn(async move {
                            let _connection_permit = permit;
                            serve_connection(node, stream, options, bytes, requests, &mut cancellation).await;
                        });
                    }
                }
            }
            // Release the listener before joining accepted mutation work.
            drop(acceptor);
            // Idle/read-only work cancels promptly. Accepted mutations own
            // their apply and fan-out until completion, even after stream loss.
            while sessions.join_next().await.is_some() {}
        });
        Ok(Self {
            addr,
            cancel,
            task: Some(task),
        })
    }

    fn validate_options(options: &ClientServerOptions) -> Result<(), NodeError> {
        if options.max_connections == 0
            || options.max_connections > Semaphore::MAX_PERMITS
            || options.max_in_flight_requests == 0
            || options.max_in_flight_requests > Semaphore::MAX_PERMITS
            || options.timeout.is_zero()
            || options.receive_bytes < MAX_FRAME_BYTES
            || options.receive_bytes > u32::MAX as usize
            || options.receive_bytes > Semaphore::MAX_PERMITS
            || options
                .credential
                .as_ref()
                .is_some_and(|key| key.len() < 32)
            || tokio::time::Instant::now()
                .checked_add(options.timeout)
                .is_none()
        {
            return Err(NodeError::Codec(
                "invalid application listener limits or credential".into(),
            ));
        }
        Ok(())
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub async fn shutdown(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task.take();
    }
}
impl Drop for ClientServer {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

async fn serve_connection(
    node: Arc<Node>,
    mut stream: BoxStream,
    options: ClientServerOptions,
    bytes: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    cancellation: &mut watch::Receiver<bool>,
) {
    let handshake = async {
        let client = match receive(&mut stream, actordb_protocol::MAX_HANDSHAKE_BYTES, &bytes)
            .await?
            .0
            .message
        {
            Some(wire::frame::Message::Hello(hello))
                if actordb_protocol::valid_client_hello(&hello) =>
            {
                hello
            }
            _ => return None,
        };
        let mut challenge = vec![0; 32];
        getrandom::fill(&mut challenge).ok()?;
        let server = wire::Hello {
            identity: node.id().into(),
            challenge,
            min_version: CLIENT_PROTOCOL_VERSION,
            max_version: CLIENT_PROTOCOL_VERSION,
            capabilities: CLIENT_CAPABILITIES,
        };
        send(
            &mut stream,
            wire::Frame {
                message: Some(wire::frame::Message::Hello(server.clone())),
            },
        )
        .await?;
        let auth = match receive(&mut stream, actordb_protocol::MAX_HANDSHAKE_BYTES, &bytes)
            .await?
            .0
            .message
        {
            Some(wire::frame::Message::Auth(auth)) => auth,
            _ => return None,
        };
        if !actordb_protocol::verify_client_auth(
            options.credential.as_deref(),
            true,
            &client,
            &server,
            &auth.proof,
        ) {
            return None;
        }
        let proof = options
            .credential
            .as_deref()
            .map(|key| actordb_protocol::client_auth_proof(key, false, &client, &server))
            .unwrap_or_default();
        send(
            &mut stream,
            wire::Frame {
                message: Some(wire::frame::Message::Auth(wire::Auth { proof })),
            },
        )
        .await?;
        Some(client.capabilities & server.capabilities)
    };
    let completed = tokio::select! {
        _ = cancellation.changed() => None,
        result = tokio::time::timeout(options.timeout.min(Duration::from_secs(5)), handshake) => result.ok().flatten(),
    };
    let Some(capabilities) = completed else {
        return;
    };
    loop {
        if *cancellation.borrow() {
            return;
        }
        // Idle connections occupy no receive bytes. Body and request work have
        // deadlines; listener cancellation interrupts the idle prefix read.
        let first = tokio::select! {
            _ = cancellation.changed() => return,
            result = stream.read_u8() => match result { Ok(first) => first, _ => return },
        };
        let mut prefix = [first, 0, 0, 0];
        tokio::select! {
            _ = cancellation.changed() => return,
            result = tokio::time::timeout(options.timeout, stream.read_exact(&mut prefix[1..])) => if !matches!(result, Ok(Ok(_))) { return; },
        }
        let length = u32::from_be_bytes(prefix);
        if length == 0 || length as usize > MAX_FRAME_BYTES {
            return;
        }
        let Ok(permit) = bytes.clone().try_acquire_many_owned(length) else {
            return;
        };
        let mut body = vec![0; length as usize];
        tokio::select! {
            _ = cancellation.changed() => return,
            result = tokio::time::timeout(options.timeout, stream.read_exact(&mut body)) => {
                if !matches!(result, Ok(Ok(_))) { return; }
            }
        }
        if actordb_protocol::preflight_client_frame(&body).is_err() {
            return;
        }
        let request = match wire::Frame::decode(body.as_slice())
            .ok()
            .and_then(|frame| frame.message)
        {
            Some(wire::frame::Message::Request(request)) if request.id != 0 => request,
            _ => return,
        };
        drop(body);
        let _request_permit = tokio::select! {
            _ = cancellation.changed() => return,
            result = tokio::time::timeout(options.timeout, requests.acquire()) => match result { Ok(Ok(permit)) => permit, _ => return },
        };
        if *cancellation.borrow() {
            return;
        }
        let optional_capability = match request.operation {
            Some(wire::request::Operation::Put(ref put)) if put.write_concern.is_some() => {
                actordb_protocol::CLIENT_CAPABILITY_WRITE_CONCERNS
            }
            Some(wire::request::Operation::Delete(ref delete))
                if delete.write_concern.is_some() =>
            {
                actordb_protocol::CLIENT_CAPABILITY_WRITE_CONCERNS
            }
            Some(wire::request::Operation::Query(_)) => actordb_protocol::CLIENT_CAPABILITY_QUERY,
            Some(wire::request::Operation::Scan(_)) => actordb_protocol::CLIENT_CAPABILITY_SCAN,
            _ => 0,
        };
        if optional_capability & capabilities != optional_capability {
            let response = wire::Response {
                id: request.id,
                error: Some(error(
                    wire::ErrorCode::Unsupported,
                    "operation capability was not negotiated",
                )),
                ..Default::default()
            };
            if !matches!(
                tokio::time::timeout(
                    options.timeout,
                    send(
                        &mut stream,
                        wire::Frame {
                            message: Some(wire::frame::Message::Response(response))
                        }
                    )
                )
                .await,
                Ok(Some(()))
            ) {
                return;
            }
            continue;
        }
        let mutation = matches!(
            request.operation,
            Some(
                wire::request::Operation::Put(_)
                    | wire::request::Operation::Delete(_)
                    | wire::request::Operation::BatchPut(_)
            )
        );
        // Node admission includes the response's ownership on this independent
        // application listener, including a write blocked on the client socket.
        let response_ownership = node.admit_client_request();
        let mut response = if response_ownership.is_err() {
            wire::Response {
                id: request.id,
                error: Some(error(wire::ErrorCode::Unavailable, "node is shutting down")),
                ..Default::default()
            }
        } else if mutation {
            // This session is tracked by the listener and never aborted while
            // it owns an accepted mutation. Client deadlines may expire, but
            // apply/outcome/fan-out completion remains owned and gets joined.
            dispatch(&node, request).await
        } else {
            tokio::select! {
                _ = cancellation.changed() => return,
                result = tokio::time::timeout(options.timeout, dispatch(&node, request)) => match result { Ok(response) => response, Err(_) => return },
            }
        };
        if *cancellation.borrow() {
            return;
        }
        if response.encoded_len() + 16 > MAX_FRAME_BYTES || !response_counts_valid(&response) {
            response = wire::Response {
                id: response.id,
                error: Some(error(
                    if mutation {
                        wire::ErrorCode::UnknownOutcome
                    } else {
                        wire::ErrorCode::ResourceLimit
                    },
                    "complete response exceeds byte or count limit; reduce batch/query/scan size",
                )),
                ..Default::default()
            };
        }
        let result = tokio::time::timeout(
            options.timeout,
            send(
                &mut stream,
                wire::Frame {
                    message: Some(wire::frame::Message::Response(response)),
                },
            ),
        )
        .await;
        drop(response_ownership);
        drop(permit);
        if !matches!(result, Ok(Some(()))) {
            return;
        }
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
        id: request.id,
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
                    if response.encoded_len() + 16 > MAX_FRAME_BYTES {
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
                                .map(|cells| actordb_protocol::meta::QueryPageRow {
                                    key: String::new(),
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
                        last_reaped: item.last_reaped as u64,
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
async fn send(stream: &mut BoxStream, frame: wire::Frame) -> Option<()> {
    if frame.encoded_len() > MAX_FRAME_BYTES {
        return None;
    }
    let bytes = frame.encode_to_vec();
    stream.write_u32(bytes.len() as u32).await.ok()?;
    stream.write_all(&bytes).await.ok()?;
    stream.flush().await.ok()?;
    Some(())
}
async fn receive(
    stream: &mut BoxStream,
    max: usize,
    bytes: &Arc<Semaphore>,
) -> Option<(wire::Frame, tokio::sync::OwnedSemaphorePermit)> {
    let length = stream.read_u32().await.ok()?;
    if length == 0 || length as usize > max {
        return None;
    }
    let permit = bytes.clone().try_acquire_many_owned(length).ok()?;
    let mut body = vec![0; length as usize];
    stream.read_exact(&mut body).await.ok()?;
    actordb_protocol::preflight_client_frame(&body).ok()?;
    Some((wire::Frame::decode(body.as_slice()).ok()?, permit))
}

#[cfg(test)]
mod outcome_mapping_tests {
    use super::*;
    #[test]
    fn embedded_unknown_mutation_remains_unknown_in_application_response() {
        let result = mutation_result(
            "key".into(),
            Err(NodeError::UnknownOutcome("still owned".into())),
        );
        assert_eq!(
            result.error.unwrap().code,
            wire::ErrorCode::UnknownOutcome as i32
        );
    }

    async fn application_session(
        node: Arc<Node>,
    ) -> (BoxStream, JoinHandle<()>, watch::Sender<bool>) {
        let (server, client) = tokio::io::duplex(256);
        let (cancel, mut cancelled) = watch::channel(false);
        let task = tokio::spawn(async move {
            serve_connection(
                node,
                Box::pin(server),
                ClientServerOptions::default(),
                Arc::new(Semaphore::new(MAX_FRAME_BYTES)),
                Arc::new(Semaphore::new(8)),
                &mut cancelled,
            )
            .await;
        });
        let mut client: BoxStream = Box::pin(client);
        let bytes = Arc::new(Semaphore::new(MAX_FRAME_BYTES));
        send(
            &mut client,
            wire::Frame {
                message: Some(wire::frame::Message::Hello(wire::Hello {
                    identity: "drain-client".into(),
                    challenge: vec![1; 32],
                    min_version: CLIENT_PROTOCOL_VERSION,
                    max_version: CLIENT_PROTOCOL_VERSION,
                    capabilities: CLIENT_CAPABILITIES,
                })),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            receive(&mut client, MAX_FRAME_BYTES, &bytes)
                .await
                .unwrap()
                .0
                .message,
            Some(wire::frame::Message::Hello(_))
        ));
        send(
            &mut client,
            wire::Frame {
                message: Some(wire::frame::Message::Auth(wire::Auth { proof: Vec::new() })),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            receive(&mut client, MAX_FRAME_BYTES, &bytes)
                .await
                .unwrap()
                .0
                .message,
            Some(wire::frame::Message::Auth(_))
        ));
        (client, task, cancel)
    }

    #[tokio::test]
    async fn request_drain_waits_for_accepted_application_response_socket_failure() {
        use crate::node::{DrainError, DrainOptions, NodeConfig};
        let node = Arc::new(
            Node::start(
                "response-drain".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        node.table("kv")
            .unwrap()
            .put("wide", vec![3; 4096])
            .await
            .unwrap();
        let (mut client, session, _cancel) = application_session(node.clone()).await;
        send(
            &mut client,
            wire::Frame {
                message: Some(wire::frame::Message::Request(wire::Request {
                    id: 1,
                    table: "kv".into(),
                    operation: Some(wire::request::Operation::Get(wire::Get {
                        key: "wide".into(),
                        ..Default::default()
                    })),
                })),
            },
        )
        .await
        .unwrap();
        // The complete value cannot fit in the unconsumed socket. Wait until
        // the Node/table/actor read slots leave only response ownership.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let diagnostics = node.drain_diagnostics();
                if diagnostics.accepted_requests() >= 4
                    && node.drain_status().active_requests() == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            node.drain_requests(DrainOptions::default().with_timeout(Duration::from_millis(20)))
                .await,
            Err(DrainError::Timeout { .. })
        ));
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .unwrap()
            .unwrap();
        node.drain_requests(DrainOptions::default()).await.unwrap();
        assert_eq!(node.drain_status().active_requests(), 0);
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
    }

    #[tokio::test]
    async fn post_cutoff_application_request_returns_unavailable_without_admission() {
        use crate::node::{DrainOptions, NodeConfig};
        let node = Arc::new(
            Node::start(
                "rejected-drain".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        let (mut client, session, _cancel) = application_session(node.clone()).await;
        node.begin_drain();
        send(
            &mut client,
            wire::Frame {
                message: Some(wire::frame::Message::Request(wire::Request {
                    id: 2,
                    table: "kv".into(),
                    operation: Some(wire::request::Operation::Get(wire::Get {
                        key: "late".into(),
                        ..Default::default()
                    })),
                })),
            },
        )
        .await
        .unwrap();
        let bytes = Arc::new(Semaphore::new(MAX_FRAME_BYTES));
        let response = receive(&mut client, MAX_FRAME_BYTES, &bytes)
            .await
            .unwrap()
            .0;
        assert!(
            matches!(response.message, Some(wire::frame::Message::Response(wire::Response {
            error: Some(wire::Error { code, .. }), ..
        })) if code == wire::ErrorCode::Unavailable as i32)
        );
        assert_eq!(node.drain_status().active_requests(), 0);
        assert!(node.drain_diagnostics().rejected_requests() > 0);
        node.drain_requests(DrainOptions::default()).await.unwrap();
        drop(client);
        session.await.unwrap();
        node.finish_shutdown(DrainOptions::default()).await.unwrap();
    }
}
