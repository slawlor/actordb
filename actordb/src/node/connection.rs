// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Peer connection identity, handshake, framing, writer ownership, and redial.
//!
//! Connection publication holds only the peer map lock; actor/store RPCs run
//! after releasing it. Physical channel identity pins pending replies and
//! replacement cleanup. The task manager owns both socket halves; cancellation
//! closes the channel before sender/relay cleanup and never detaches writers.
//! Legacy disconnect acquires peers, senders, then sender reports in that order;
//! the registries are released before waiting for retiring sender joins.

use super::*;

/// One live connection's bounded outbound queue plus a cancellation signal
/// shared by every producer and the socket reader/writer tasks.
#[derive(Clone)]
pub(super) struct PeerChannel {
    pub(super) outbound: BudgetedPeerTx,
    pub(super) cancel: watch::Sender<bool>,
    pub(super) queue_admission_timeout: Duration,
    pub(super) rank: Option<ConnectionRank>,
    pub(super) capabilities: u64,
}

pub(super) struct ConnectionCancelGuard(pub(super) PeerChannel);

impl Drop for ConnectionCancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl PeerChannel {
    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.outbound.same_channel(&other.outbound)
    }

    pub(super) fn same_sender(&self, other: &mpsc::Sender<Vec<u8>>) -> bool {
        self.outbound.identity.same_channel(other)
    }

    pub(super) fn raw_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.outbound.identity.clone()
    }

    pub(super) fn budgeted_sender(&self) -> BudgetedPeerTx {
        self.outbound.clone()
    }

    pub(super) fn cancel(&self) {
        // Wake producers blocked on the encoded-byte gate immediately; this
        // budget belongs only to this physical connection and is never reused.
        self.outbound.close();
        // Cancellation is terminal, including for relays which subscribe after
        // the reader exited and the last previous receiver was dropped.
        self.cancel.send_replace(true);
    }
}

/// Both endpoints derive the same ordering from the authenticated transcript.
/// A connection dialed by the lexicographically smaller node is preferred;
/// the transcript digest breaks ties between duplicate sockets in the same
/// direction without depending on arrival order at either endpoint.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ConnectionRank {
    pub(super) preferred_direction: bool,
    pub(super) transcript_id: [u8; 32],
}

/// peer node_id -> outbound frame queue
pub(super) type PeerMap = Arc<Mutex<HashMap<String, PeerChannel>>>;

pub(super) type PeerFailurePending = Arc<StdMutex<HashMap<String, Vec<mpsc::Sender<Vec<u8>>>>>>;

/// Synchronous signal path from replication sender callbacks to one Node-owned
/// async worker. Duplicate signals for the same physical channel coalesce, but
/// distinct stale/current channels are retained so one cannot overwrite the
/// other before the worker performs exact-channel cancellation.
#[derive(Clone, Default)]
pub(super) struct PeerFailureSignals {
    pub(super) pending: PeerFailurePending,
    pub(super) notify: Arc<Notify>,
}

impl PeerFailureSignals {
    pub(super) fn signal(&self, peer: String, failed_tx: mpsc::Sender<Vec<u8>>) {
        let mut pending = self.pending.lock().unwrap();
        let channels = pending.entry(peer).or_default();
        if !channels
            .iter()
            .any(|channel| channel.same_channel(&failed_tx))
        {
            channels.push(failed_tx);
        }
        drop(pending);
        self.notify.notify_one();
    }

    pub(super) async fn run(self, peers: PeerMap) {
        loop {
            self.notify.notified().await;
            let failures = std::mem::take(&mut *self.pending.lock().unwrap());
            for (peer, failed_channels) in failures {
                let current = peers.lock().await.get(&peer).cloned();
                if let Some(current) = current {
                    if failed_channels
                        .iter()
                        .any(|failed_tx| current.same_sender(failed_tx))
                    {
                        current.cancel();
                    }
                }
            }
        }
    }
}

pub(super) struct DialIntentGuard {
    pub(super) address: SocketAddr,
    pub(super) registry: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
    pub(super) changes: watch::Sender<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DialIntentState {
    Pending,
    Active,
}

impl DialIntentGuard {
    pub(super) fn reserve(
        registry: Arc<StdMutex<HashMap<SocketAddr, DialIntentState>>>,
        changes: watch::Sender<u64>,
        address: SocketAddr,
    ) -> Option<Self> {
        let mut intents = registry.lock().unwrap();
        if intents.contains_key(&address) {
            return None;
        }
        intents.insert(address, DialIntentState::Pending);
        drop(intents);
        Some(Self {
            address,
            registry,
            changes,
        })
    }

    pub(super) fn activate(&self) {
        if let Some(state) = self.registry.lock().unwrap().get_mut(&self.address) {
            *state = DialIntentState::Active;
        }
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

impl Drop for DialIntentGuard {
    fn drop(&mut self) {
        self.registry.lock().unwrap().remove(&self.address);
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

/// Everything a connection task needs. Cloned per connection.
#[cfg(test)]
#[derive(Clone)]
pub(super) struct SenderSnapshotHook {
    pub(super) captured: Arc<Notify>,
    pub(super) resume: Arc<Notify>,
}

#[derive(Clone)]
pub(super) struct ConnContext {
    pub(super) node_id: String,
    pub(super) router: FragmentRouter,
    /// Fragments this node authoritatively serves (Phase 8: a
    /// bootstrapping fragment has an actor in the router but is not
    /// yet serving).
    pub(super) serving: Arc<RwLock<HashSet<FragmentId>>>,
    /// Authoritative placement map (Phase 8: updated by the placement
    /// driver; static otherwise). Used to re-forward ops for fragments
    /// this node no longer serves.
    pub(super) placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    /// Whether placement lifecycles are controller-driven. Both modes use
    /// the same protocol-v3 reconciler for anti-entropy.
    pub(super) dynamic_placement: bool,
    pub(super) pending_gets: PendingGets,
    pub(super) pending_batch_gets: PendingBatchGets,
    /// In-flight distributed scan requests (coordinator side).
    pub(super) pending_scans: PendingScans,
    pub(super) pending_query_pages: query::PendingQueryPages,
    pub(super) query_admission: Arc<crate::query::QueryAdmission>,
    pub(super) pending_mutations: PendingMutations,
    /// Relayed forwarded-gets: relay_req_id -> (origin peer_tx, origin req_id).
    pub(super) pending_relays: RelayMap,
    pub(super) pending_mutation_relays: MutationRelayMap,
    pub(super) next_req_id: Arc<AtomicU64>,
    pub(super) peers: PeerMap,
    pub(super) peer_failures: PeerFailureSignals,
    pub(super) tasks: Weak<RuntimeTasks>,
    pub(super) read_service: Arc<ReadService>,
    pub(super) operation_gate: Arc<OperationGate>,
    /// Monotonic wake-up signal for dial supervisors waiting behind another
    /// physical connection to the same authenticated peer.
    pub(super) peer_changes: watch::Sender<u64>,
    pub(super) hlc: Arc<Mutex<HlcClock>>,
    pub(super) senders: SenderMap,
    pub(super) finalizing_senders: Arc<SenderFinalizationRegistry>,
    pub(super) retiring_senders: Arc<RetiringSenderRegistry>,
    pub(super) sender_config: SenderConfig,
    pub(super) sender_runtime: SenderRuntimeOptions,
    pub(super) runtime_options: NodeRuntimeOptions,
    pub(super) receive_byte_budget: Arc<Semaphore>,
    pub(super) node_peer_queue_budget: Arc<NodePeerByteBudget>,
    pub(super) relay_admission: Arc<RelayAdmission>,
    pub(super) mutation_service: Arc<MutationService>,
    pub(super) sender_reports: SenderReportMap,
    /// Notify the placement driver on peer join/leave. `None` in
    /// static mode.
    pub(super) membership_tx: Option<watch::Sender<u64>>,
    /// Pre-shared cluster secret for fresh, challenge-bound mutual proofs.
    /// This proves group-key possession, not individual identity, and does not
    /// protect post-handshake plaintext frames. `None` = no membership proof.
    pub(super) cluster_secret: Option<Vec<u8>>,
    #[cfg(test)]
    pub(super) sender_snapshot_hook: Option<SenderSnapshotHook>,
    pub(super) recovery: Option<Arc<RecoveryContext>>,
}

/// Mint a fresh boot id. Wall-clock nanos mixed with the pid: two boots
/// can never share a nanosecond on the same machine, so collisions are
/// not a practical concern (and a collision would only risk a dropped
/// duplicate, never corruption — LWW still guards the merge).
pub(super) fn fresh_boot_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

#[cfg(test)]
pub(super) async fn run_peer_writer<W>(
    writer: W,
    rx: mpsc::Receiver<BudgetedFrame>,
    byte_budget: Arc<Semaphore>,
    cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let (response_tx, response_rx) = mpsc::channel(1);
    drop(response_tx);
    let producer_byte_budget = Arc::new(Semaphore::new(byte_budget.available_permits()));
    run_bounded_peer_writer(
        writer,
        PeerWriterPipeline {
            data_rx: rx,
            response_rx,
            byte_budget,
            producer_byte_budget,
        },
        cancel_rx,
        cancel_tx,
        socket_write_timeout,
    )
    .await;
}

pub(super) struct PeerWriterExitGuard {
    pub(super) byte_budget: Arc<Semaphore>,
    pub(super) producer_byte_budget: Arc<Semaphore>,
    pub(super) cancel: watch::Sender<bool>,
}

pub(super) struct PeerWriterPipeline {
    pub(super) data_rx: mpsc::Receiver<BudgetedFrame>,
    pub(super) response_rx: mpsc::Receiver<BudgetedFrame>,
    pub(super) byte_budget: Arc<Semaphore>,
    pub(super) producer_byte_budget: Arc<Semaphore>,
}

impl Drop for PeerWriterExitGuard {
    fn drop(&mut self) {
        self.byte_budget.close();
        self.producer_byte_budget.close();
        let _ = self.cancel.send(true);
    }
}

pub(super) async fn run_bounded_peer_writer<W>(
    writer: W,
    pipeline: PeerWriterPipeline,
    cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let PeerWriterPipeline {
        data_rx,
        response_rx,
        byte_budget,
        producer_byte_budget,
    } = pipeline;
    let _exit_guard = PeerWriterExitGuard {
        byte_budget,
        producer_byte_budget,
        cancel: cancel_tx.clone(),
    };
    run_queued_peer_writer(
        writer,
        data_rx,
        response_rx,
        cancel_rx,
        cancel_tx.clone(),
        socket_write_timeout,
    )
    .await;
    let _ = cancel_tx.send(true);
}

pub(super) async fn run_queued_peer_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<BudgetedFrame>,
    mut response_rx: mpsc::Receiver<BudgetedFrame>,
    mut cancel_rx: watch::Receiver<bool>,
    cancel_tx: watch::Sender<bool>,
    socket_write_timeout: Duration,
) where
    W: AsyncWrite + Unpin,
{
    let mut data_open = true;
    let mut response_open = true;
    let mut response_burst = 0usize;
    'writer: while data_open || response_open {
        let (frame, is_response) = loop {
            if !data_open && !response_open {
                break 'writer;
            }
            if *cancel_rx.borrow() {
                break 'writer;
            }
            if data_open && response_burst >= PEER_RESPONSE_BURST_LIMIT {
                match rx.try_recv() {
                    Ok(frame) => break (frame, false),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => data_open = false,
                }
            }
            tokio::select! {
                biased;
                changed = cancel_rx.changed() => {
                    let _ = changed;
                    break 'writer;
                }
                frame = response_rx.recv(), if response_open => {
                    match frame {
                        Some(frame) => break (frame, true),
                        None => response_open = false,
                    }
                }
                frame = rx.recv(), if data_open => {
                    match frame {
                        Some(frame) => break (frame, false),
                        None => data_open = false,
                    }
                }
            }
        };
        if is_response {
            response_burst = response_burst.saturating_add(1);
        } else {
            response_burst = 0;
        }
        if frame.len() > MAX_FRAME_BYTES || u32::try_from(frame.len()).is_err() {
            tracing::warn!(
                encoded_len = frame.len(),
                "closing peer transport before writing an oversized frame"
            );
            break;
        }
        let write = async {
            writer.write_u32(frame.len() as u32).await?;
            writer.write_all(frame.bytes()).await?;
            writer.flush().await
        };
        let result = tokio::select! {
            changed = cancel_rx.changed() => {
                let _ = changed;
                None
            }
            result = tokio::time::timeout(socket_write_timeout, write) => Some(result),
        };
        if !matches!(result, Some(Ok(Ok(())))) {
            break;
        }
    }
    let _ = cancel_tx.send(true);
}

/// Drive one peer connection: handshake, then a read loop.
///
/// `we_dialed` distinguishes the dialer (sends `Hello` immediately) from
/// the listener (replies to the dialer's `Hello`).
///
/// The writer is registered as a sibling in the Node task manager. Cancelling
/// the handler therefore cannot detach the socket's write half from shutdown.
#[derive(Debug)]
pub(super) enum HandshakeState {
    AwaitingHello,
    AwaitingAuth { peer: Hello },
    Established { peer: String },
}

impl HandshakeState {
    pub(super) fn is_established(&self) -> bool {
        matches!(self, Self::Established { .. })
    }

    pub(super) fn established_peer_id(&self) -> Option<&str> {
        match self {
            Self::Established { peer } => Some(peer),
            Self::AwaitingHello | Self::AwaitingAuth { .. } => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct ConnectionOutcome {
    pub(super) peer_id: Option<String>,
    pub(super) was_established: bool,
}

/// The byte permit follows the raw allocation through decoding and the full
/// inline handler turn. Decoded or forwarded copies are separate allocations.
pub(super) struct ReceivedFrame {
    pub(super) bytes: Vec<u8>,
    pub(super) _byte_permit: OwnedSemaphorePermit,
}

pub(super) async fn read_connection_frame<R>(
    reader: &mut R,
    cancel: &mut watch::Receiver<bool>,
    receive_byte_budget: &Arc<Semaphore>,
    established: bool,
    handshake_deadline: Instant,
    frame_body_timeout: Duration,
) -> Option<ReceivedFrame>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let prefix = if established {
        tokio::select! {
            changed = cancel.changed() => {
                let _ = changed;
                return None;
            }
            result = reader.read_u32() => result.ok()?,
        }
    } else {
        tokio::select! {
            changed = cancel.changed() => {
                let _ = changed;
                return None;
            }
            _ = tokio::time::sleep_until(handshake_deadline) => return None,
            result = reader.read_u32() => result.ok()?,
        }
    };
    let len = prefix as usize;
    let max_len = if established {
        MAX_FRAME_BYTES
    } else {
        4 * 1024
    };
    if len > max_len {
        return None;
    }

    let byte_permit = match receive_byte_budget.clone().try_acquire_many_owned(prefix) {
        Ok(permit) => permit,
        Err(_) => {
            // Waiting here could stop the only reader which can drain the
            // opposite direction. No frame is decoded or applied on failure.
            tracing::warn!(len, "closing connection at node receive byte capacity");
            return None;
        }
    };
    let mut frame = vec![0u8; len];
    let body_deadline = if established {
        deadline_after(frame_body_timeout)
    } else {
        handshake_deadline
    };
    tokio::select! {
        changed = cancel.changed() => {
            let _ = changed;
            None
        }
        _ = tokio::time::sleep_until(body_deadline) => None,
        result = reader.read_exact(&mut frame) => result.ok().map(|_| ReceivedFrame {
            bytes: frame,
            _byte_permit: byte_permit,
        }),
    }
}

pub(super) fn validate_peer_hello(
    local_node_id: &str,
    expected_peer_id: Option<&str>,
    hello: &Hello,
) -> Result<(), &'static str> {
    if hello.node_id.is_empty() || hello.node_id.len() > MAX_WIRE_NODE_ID_BYTES {
        return Err("peer node_id is empty or exceeds 255 bytes");
    }
    if hello.node_id == local_node_id {
        return Err("peer claimed the local node_id");
    }
    if expected_peer_id.is_some_and(|expected| expected != hello.node_id) {
        return Err("peer identity did not match the configured member");
    }
    if !hello.auth_tag.is_empty() {
        return Err("peer used the retired replayable Hello auth_tag");
    }
    if hello.challenge.len() != HANDSHAKE_CHALLENGE_BYTES
        || hello.challenge.iter().all(|byte| *byte == 0)
    {
        return Err("peer challenge is missing, invalid, or all-zero");
    }
    if hello.min_protocol_version == 0
        || hello.min_protocol_version > WIRE_PROTOCOL_VERSION
        || hello.max_protocol_version < WIRE_PROTOCOL_VERSION
    {
        return Err("peer wire-version range is incompatible");
    }
    if REQUIRED_WIRE_CAPABILITIES & !hello.capabilities != 0 {
        return Err("peer omitted required wire capabilities");
    }
    Ok(())
}

pub(super) async fn handle_connection(
    ctx: ConnContext,
    stream: BoxStream,
    we_dialed: bool,
    expected_peer_id: Option<String>,
    connection_lease: Option<Arc<OwnedSemaphorePermit>>,
) -> ConnectionOutcome {
    let handshake_deadline = deadline_after(ctx.runtime_options.handshake_timeout);
    let ConnContext {
        recovery,
        node_id,
        router,
        serving,
        placement,
        dynamic_placement,
        pending_gets,
        pending_batch_gets,
        pending_scans,
        pending_query_pages,
        query_admission,
        pending_mutations,
        pending_relays,
        pending_mutation_relays,
        next_req_id,
        peers,
        peer_failures,
        tasks,
        read_service,
        operation_gate,
        peer_changes,
        hlc,
        senders,
        finalizing_senders,
        retiring_senders,
        sender_config,
        sender_runtime,
        runtime_options,
        receive_byte_budget,
        node_peer_queue_budget,
        relay_admission,
        mutation_service,
        sender_reports,
        membership_tx,
        cluster_secret,
        #[cfg(test)]
        sender_snapshot_hook,
    } = ctx;
    let local_hello = match fresh_hello(&node_id) {
        Ok(hello) => hello,
        Err(error) => {
            tracing::error!(%error, "failed to generate a handshake challenge");
            return ConnectionOutcome {
                peer_id: None,
                was_established: false,
            };
        }
    };
    // Phase 5: current session per (peer, fragment) on this connection.
    // Sessions are per-connection: a new connection means a new pairing.
    let sessions: InboundSessions = Arc::new(Mutex::new(HashMap::new()));
    let (session_changes, session_change_rx) = watch::channel(0u64);
    let mut next_session_generation = 1u64;
    // An untracked reconciliation repair has no individual ACK. Remember its
    // first failure so the following tracked empty barrier NACKs instead of
    // falsely declaring the session converged. A fresh SessionStart clears
    // the poison and lets the sender retry the complete round.
    let mut reconcile_failures: HashMap<(String, FragmentId), (u64, String)> = HashMap::new();
    // Bounded responder-side authorization. Only the exact next cursor page
    // and keys advertised by the most recent page may be fetched.
    let mut served_reconciles: HashMap<(String, FragmentId), ServedReconcileState> = HashMap::new();
    let (mut reader, writer) = tokio::io::split(stream);
    let socket_write_timeout = runtime_options.socket_write_timeout;
    let peer_queue_frame_capacity = runtime_options.peer_queue_frame_capacity;
    let peer_queue_byte_capacity = runtime_options.peer_queue_byte_capacity;
    // Split the configured frame budget between ordinary producer traffic and
    // reader-generated responses. The writer drains responses first, avoiding
    // the symmetric full-queue deadlock where both peers need to emit an ACK
    // before either can make progress.
    let response_frame_capacity = peer_queue_frame_capacity / 2;
    let producer_frame_capacity = peer_queue_frame_capacity - response_frame_capacity;
    let (frame_tx, rx) = mpsc::channel::<BudgetedFrame>(producer_frame_capacity);
    let (response_tx, response_rx) = mpsc::channel::<BudgetedFrame>(response_frame_capacity);
    // `SenderMsg::PeerDisconnected` is public and historically identifies a
    // transport with Sender<Vec<u8>>. Keep a live, unsent compatibility token;
    // Node frames exclusively use the budgeted channel above.
    let (identity, _identity_rx) = mpsc::channel::<Vec<u8>>(1);
    let (cancel, _) = watch::channel(false);
    let byte_budget = Arc::new(Semaphore::new(peer_queue_byte_capacity));
    let producer_byte_budget = Arc::new(Semaphore::new(
        peer_queue_byte_capacity - PEER_RESPONSE_BYTE_RESERVE,
    ));
    let mut tx = PeerChannel {
        outbound: BudgetedPeerTx {
            node_byte_budget: node_peer_queue_budget,
            tx: frame_tx,
            response_tx,
            byte_budget: byte_budget.clone(),
            producer_byte_budget: producer_byte_budget.clone(),
            reconcile_gate: ReconcileGate::new(),
            identity,
        },
        cancel: cancel.clone(),
        queue_admission_timeout: runtime_options.queue_admission_timeout,
        rank: None,
        capabilities: REQUIRED_WIRE_CAPABILITIES
            | CAPABILITY_SCAN
            | CAPABILITY_QUERY_PAGES
            | CAPABILITY_QUERY_METADATA,
    };
    let writer_cancel = cancel.subscribe();
    // Abort/early-return must cancel jobs bound to this exact physical socket.
    let _connection_cancel = ConnectionCancelGuard(tx.clone());
    // Subscribe before the writer or handshake can signal cancellation;
    // subscribing after an early failure would mark that value as already
    // observed and leave the read side waiting forever.
    let mut read_cancel = cancel.subscribe();
    let writer_cancel_tx = cancel.clone();
    // The writer is independently registered with the Node task manager. The
    // context carries only a Weak reference, avoiding a manager→handler→manager
    // ownership cycle while still making shutdown join both halves.
    let writer_registered = match tasks.upgrade() {
        Some(tasks) => {
            let writer_lease = connection_lease.clone();
            tokio::time::timeout_at(
                handshake_deadline,
                tasks.spawn(async move {
                    let _writer_lease = writer_lease;
                    run_bounded_peer_writer(
                        writer,
                        PeerWriterPipeline {
                            data_rx: rx,
                            response_rx,
                            byte_budget,
                            producer_byte_budget,
                        },
                        writer_cancel,
                        writer_cancel_tx,
                        socket_write_timeout,
                    )
                    .await;
                }),
            )
            .await
            .unwrap_or(false)
        }
        None => false,
    };
    if !writer_registered {
        return ConnectionOutcome {
            peer_id: None,
            was_established: false,
        };
    }

    // Fetch responses can span multiple near-limit frames. Produce them in a
    // bounded connection-owned worker so the reader always keeps consuming
    // the opposite direction and cannot participate in a bidirectional
    // socket/byte-budget deadlock.
    let (reconcile_response_tx, reconcile_response_rx) =
        mpsc::channel(RECONCILE_RESPONSE_QUEUE_DEPTH);
    let responder_registered = match tasks.upgrade() {
        Some(tasks) => {
            let responder_tx = tx.clone();
            let admission_timeout = runtime_options.reconcile_data_admission_timeout;
            tokio::time::timeout_at(
                handshake_deadline,
                tasks.spawn(run_reconcile_responder(
                    reconcile_response_rx,
                    responder_tx,
                    sessions.clone(),
                    session_change_rx,
                    admission_timeout,
                )),
            )
            .await
            .unwrap_or(false)
        }
        None => false,
    };
    if !responder_registered {
        tx.cancel();
        return ConnectionOutcome {
            peer_id: None,
            was_established: false,
        };
    }

    // Both endpoints contribute a fresh challenge before either can prove the
    // ordered transcript. Every required handshake enqueue is checked.
    if !send_hello_until(&tx, local_hello.clone(), handshake_deadline).await {
        tx.cancel();
        return ConnectionOutcome {
            peer_id: None,
            was_established: false,
        };
    }

    // Phase 8: shared re-forward context (migration safety).
    let rfctx = ReforwardCtx {
        node_id: node_id.clone(),
        placement: placement.clone(),
        peers: peers.clone(),
        tasks: tasks.clone(),
        pending_relays: pending_relays.clone(),
        pending_mutation_relays: pending_mutation_relays.clone(),
        next_req_id: next_req_id.clone(),
        forwarded_attempt_timeout: runtime_options.forwarded_attempt_timeout,
        relay_admission,
    };

    let mut handshake = HandshakeState::AwaitingHello;
    let mut authenticated_peer_id = None;
    let mut was_established = false;
    // Length-delimited framing: 4-byte big-endian length prefix.
    'connection: loop {
        let Some(buf) = read_connection_frame(
            &mut reader,
            &mut read_cancel,
            &receive_byte_budget,
            handshake.is_established(),
            handshake_deadline,
            runtime_options.socket_read_timeout,
        )
        .await
        else {
            break;
        };
        if let Err(error) = preflight_envelope_decode(&buf.bytes) {
            tracing::warn!(%error, "rejecting inbound peer frame before protobuf allocation");
            break;
        }
        let envelope = match Envelope::decode(&buf.bytes[..]) {
            Ok(e) => e,
            Err(_) => break,
        };
        let peer_node_id = handshake.established_peer_id().map(str::to_owned);

        // A small, absolute-deadline handshake accepts only its two flights.
        // Unexpected application traffic fails closed instead of allowing an
        // unauthenticated peer to consume decode/loop work until timeout.
        if !handshake.is_established()
            && !matches!(
                envelope.msg,
                Some(EnvelopeMsg::Hello(_)) | Some(EnvelopeMsg::HelloAuth(_))
            )
        {
            tracing::warn!("rejecting application frame before handshake completion");
            break;
        }

        match envelope.msg {
            Some(EnvelopeMsg::Hello(peer_hello)) => {
                if !matches!(handshake, HandshakeState::AwaitingHello) {
                    tracing::warn!("rejecting duplicate or post-handshake Hello");
                    break;
                }
                if let Err(reason) =
                    validate_peer_hello(&node_id, expected_peer_id.as_deref(), &peer_hello)
                {
                    tracing::warn!(claimed_node_id = %peer_hello.node_id, %reason, "rejecting Hello");
                    break;
                }
                let (dialer, listener) = if we_dialed {
                    (&local_hello, &peer_hello)
                } else {
                    (&peer_hello, &local_hello)
                };
                let auth_tag = cluster_secret
                    .as_deref()
                    .map(|secret| handshake_auth_tag(secret, we_dialed, dialer, listener))
                    .unwrap_or_default();
                if !send_hello_auth_until(&tx, auth_tag, handshake_deadline).await {
                    break;
                }
                handshake = HandshakeState::AwaitingAuth { peer: peer_hello };
            }
            Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) => {
                let peer_hello = match &handshake {
                    HandshakeState::AwaitingAuth { peer } => peer.clone(),
                    HandshakeState::AwaitingHello | HandshakeState::Established { .. } => {
                        tracing::warn!("rejecting out-of-order or duplicate HelloAuth");
                        break;
                    }
                };
                let (dialer, listener) = if we_dialed {
                    (&local_hello, &peer_hello)
                } else {
                    (&peer_hello, &local_hello)
                };
                if !verify_handshake_auth(
                    cluster_secret.as_deref(),
                    !we_dialed,
                    dialer,
                    listener,
                    &auth_tag,
                ) {
                    tracing::warn!(
                        claimed_node_id = %peer_hello.node_id,
                        "challenge-bound handshake proof failed"
                    );
                    break;
                }

                let peer = peer_hello.node_id.clone();
                authenticated_peer_id = Some(peer.clone());
                tx.rank = Some(connection_rank(dialer, listener));
                tx.capabilities = peer_hello.capabilities;
                let (won_election, previous) = {
                    let mut peer_guard = peers.lock().await;
                    match peer_guard.get(&peer) {
                        Some(current) if current.same_channel(&tx) => (true, None),
                        Some(current) if current.rank.as_ref() >= tx.rank.as_ref() => (false, None),
                        Some(_) => (true, peer_guard.insert(peer.clone(), tx.clone())),
                        None => {
                            peer_guard.insert(peer.clone(), tx.clone());
                            (true, None)
                        }
                    }
                };
                if !won_election {
                    tracing::debug!(%peer, "authenticated duplicate connection lost election");
                    break;
                }
                if let Some(previous) = previous {
                    previous.cancel();
                    cancel_connection_relays(&previous, &pending_relays, &pending_mutation_relays);
                }
                peer_changes.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
                handshake = HandshakeState::Established { peer: peer.clone() };
                was_established = true;

                // Both static and dynamic placement use the same correlated,
                // byte-bounded reconciliation protocol. The legacy
                // SyncRequest/SyncResponse exchange is deliberately retired:
                // keeping a second monolithic bootstrap path would re-open
                // the frame and memory bounds enforced by protocol v3.
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx.clone() {
                    mtx.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
                }
                // Phase 5: (re)connect the senders for this peer. Legacy
                // mode (no fragment map) spawns senders lazily here.
                for frag_id in router.fragment_ids() {
                    let frag = FragmentId(frag_id);
                    let is_replica = {
                        let map = placement.read().unwrap();
                        map.is_empty()
                            || map
                                .get(&frag)
                                .map(|replicas| replicas.iter().any(|id| id == &peer))
                                .unwrap_or(false)
                    };
                    // Dynamic sender creation is placement-owned. Letting a
                    // connection handler create one from an earlier placement
                    // snapshot can reinsert a revoked peer after a gain has
                    // aligned and pruned its exact sender set. The placement
                    // path also reconnects newly-created senders to live peers;
                    // this handler only notifies senders that already exist.
                    if is_replica && !dynamic_placement {
                        ensure_sender_entry(
                            frag,
                            &peer,
                            SenderEntryDeps {
                                config: &sender_config,
                                runtime: &sender_runtime,
                                router: &router,
                                hlc: &hlc,
                                senders: &senders,
                                finalizing_senders: &finalizing_senders,
                                retiring_senders: &retiring_senders,
                                reports: &sender_reports,
                                peer_failures: &peer_failures,
                                operation_gate: &operation_gate,
                                allow_quiescing: false,
                            },
                        )
                        .await;
                    }
                }
                {
                    // Connection handlers for the same claimed peer can race.
                    // Serialize the winner check with peer replacement and
                    // disconnect notification so an older Hello cannot publish
                    // its channel after a newer connection won the map entry.
                    let peers_guard = peers.lock().await;
                    if !peers_guard
                        .get(&peer)
                        .is_some_and(|current| current.same_channel(&tx))
                    {
                        break;
                    }
                    let peer_senders: Vec<NodeSender> = senders
                        .lock()
                        .await
                        .iter()
                        .filter(|((_, sender_peer), _)| sender_peer == &peer)
                        .map(|(_, sender)| sender.clone())
                        .collect();
                    for sender_tx in peer_senders {
                        let _ = sender_tx.connect_peer(tx.budgeted_sender());
                        if !dynamic_placement {
                            let _ = sender_tx.send(SenderMsg::ReconcileNow);
                        }
                    }
                }
            }
            Some(EnvelopeMsg::SessionStart(s)) => {
                // New sender↔receiver pairing: adopt the session id.
                // Batches from older sessions are discarded.
                // Zero is protobuf's absent/default value and must not create
                // a session that legacy, uncorrelated messages can match.
                if s.session_id == 0 {
                    tracing::warn!(
                        fragment = s.fragment,
                        "ignoring SessionStart with default session id"
                    );
                    continue;
                }
                let fragment = FragmentId(s.fragment);
                // Placement controllers are observed independently. A source
                // can therefore publish the overlap and start this session
                // just before the destination has attached its actor. Adopt
                // the bounded transport session now; batches arriving before
                // actor installation receive a NACK below and are retried.
                // Requiring an actor here would silently lose the only
                // SessionStart and stall the sender until its ACK timeout.
                if fragment.0 as usize >= router.fragment_count {
                    tracing::warn!(
                        fragment = s.fragment,
                        "ignoring SessionStart for an invalid fragment"
                    );
                    continue;
                }
                if let Some(peer) = peer_node_id.clone() {
                    let key = (peer, fragment);
                    let changed = {
                        let mut sessions = sessions.lock().await;
                        if sessions
                            .get(&key)
                            .is_some_and(|session| session.session_id == s.session_id)
                        {
                            false
                        } else {
                            let generation = next_session_generation;
                            next_session_generation =
                                next_session_generation.wrapping_add(1).max(1);
                            sessions.insert(
                                key.clone(),
                                InboundSession {
                                    session_id: s.session_id,
                                    generation,
                                },
                            );
                            true
                        }
                    };
                    if changed {
                        reconcile_failures.remove(&key);
                        served_reconciles.remove(&key);
                        session_changes.send_modify(|generation| {
                            *generation = generation.wrapping_add(1);
                        });
                    }
                }
            }
            Some(EnvelopeMsg::ReplicateBatch(b)) => {
                if b.ops.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(item_count = b.ops.len(), "replication batch is too large");
                    break;
                }
                // Validate the session and the whole batch before applying
                // anything. An ACK is sent only after every exact outcome is
                // known and the sender's requested fence is reached.
                let peer = match peer_node_id.clone() {
                    Some(p) => p,
                    None => continue,
                };
                let frag = FragmentId(b.fragment);
                let session_key = (peer.clone(), frag);
                let current = sessions.lock().await.get(&session_key).copied();
                if current.map(|session| session.session_id) != Some(b.session_id) {
                    continue; // stale session
                }
                // Dynamic placement teardown takes the exclusive side of this
                // lease before detaching the actor. Hold the shared side
                // through apply, durability fencing, and ACK enqueue so no
                // mutation can be acknowledged during destructive teardown.
                let _mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
                let tracked = b.batch_seq != u64::MAX;
                let reconcile_barrier = tracked && b.ops.is_empty();
                let requested_ack = if tracked {
                    match ReplicationAckLevel::try_from(b.required_ack_level) {
                        Ok(ReplicationAckLevel::Applied) => {
                            Some(ReplicationAckDurability::MemoryApplied)
                        }
                        Ok(ReplicationAckLevel::StoreCommitted) => {
                            Some(ReplicationAckDurability::StoreCommitted)
                        }
                        Ok(ReplicationAckLevel::StableCommitted) => {
                            Some(ReplicationAckDurability::StableCommitted)
                        }
                        Ok(ReplicationAckLevel::Unknown) | Err(_) => None,
                    }
                } else {
                    None
                };
                let prior_reconcile_failure = reconcile_failures
                    .get(&session_key)
                    .filter(|(session_id, _)| *session_id == b.session_id)
                    .map(|(_, error)| error.clone());
                let invalid_key = b
                    .ops
                    .iter()
                    .find(|op| fragment_of(&op.key, router.fragment_count) != frag)
                    .map(|op| op.key.clone());
                let route = router.route_with_generation(frag);
                let actor = route.as_ref().map(|(_, actor)| actor.clone());
                let mut failure = prior_reconcile_failure.or_else(|| {
                    match (tracked, requested_ack, invalid_key, actor.as_ref()) {
                        (true, None, _, _) => Some(
                            "tracked replication batch omitted a valid ACK requirement".to_string(),
                        ),
                        (_, _, Some(key), _) => Some(format!(
                            "replication key {key:?} does not belong to declared fragment {frag}"
                        )),
                        (_, _, None, None) => Some(format!(
                            "receiver does not host declared replication fragment {frag}"
                        )),
                        (_, _, None, Some(_)) => None,
                    }
                });

                if failure.is_none() && reconcile_barrier {
                    let source_authorized = serving.read().unwrap().contains(&frag)
                        || served_reconciles.get(&session_key).is_some_and(|state| {
                            recovery.as_ref().is_some_and(|authority| {
                                authority.authorizes(
                                    frag,
                                    &peer,
                                    &state.recovery_epoch,
                                    &state.recovery_replica_digest,
                                    &placement.read().unwrap(),
                                )
                            })
                        });
                    let same_serving_incarnation = source_authorized
                        && route.as_ref().is_some_and(|(generation, _)| {
                            served_reconciles.get(&session_key).is_some_and(|state| {
                                state.session_id == b.session_id
                                    && state.route_generation == *generation
                            })
                        });
                    if !same_serving_incarnation {
                        failure = Some(format!(
                            "reconciliation barrier no longer matches the serving incarnation of fragment {frag}"
                        ));
                    }
                }

                if failure.is_none() {
                    if let Some(timestamp_ms) = b
                        .ops
                        .iter()
                        .map(|op| {
                            actordb_protocol::RecordVersion::new(
                                op.timestamp_ms,
                                op.logical_counter,
                            )
                        })
                        .max()
                    {
                        if !observe_remote_timestamp(&hlc, timestamp_ms, "ReplicateBatch").await {
                            break;
                        }
                    }
                }

                if failure.is_none() {
                    let actor = actor.as_ref().expect("validated actor");
                    for op in b.ops {
                        let replicated = ReplicatedOp {
                            key: op.key,
                            value: {
                                let mut v = VersionedValue::new_version(
                                    op.value,
                                    actordb_protocol::RecordVersion::new(
                                        op.timestamp_ms,
                                        op.logical_counter,
                                    ),
                                    op.node_id,
                                );
                                v.expires_at_ms = if op.expires_at_ms == 0 {
                                    None
                                } else {
                                    Some(op.expires_at_ms)
                                };
                                v.is_tombstone = op.is_tombstone;
                                v
                            },
                            seq: op.seq,
                            boot_id: op.boot_id,
                        };
                        if let Err(error) = validate_replicated_op(&replicated) {
                            failure = Some(error.to_string());
                            break;
                        }
                        if let Err(error) =
                            call_apply(actor, replicated, OpPriority::Default, true).await
                        {
                            failure = Some(error.to_string());
                            break;
                        }
                    }
                }

                let durability = if failure.is_none() {
                    match (tracked, requested_ack) {
                        (false, _) | (true, Some(ReplicationAckDurability::MemoryApplied)) => {
                            Some(ApplyDurability::MemoryApplied)
                        }
                        (
                            true,
                            Some(
                                required @ (ReplicationAckDurability::StoreCommitted
                                | ReplicationAckDurability::StableCommitted),
                            ),
                        ) => {
                            match flush_fence(actor.as_ref().expect("validated actor"), required)
                                .await
                            {
                                Ok(durability) => Some(durability),
                                Err(error) => {
                                    failure = Some(error.to_string());
                                    None
                                }
                            }
                        }
                        (true, None) => None,
                    }
                } else {
                    None
                };

                if reconcile_barrier {
                    // The empty tracked batch is the reconcile durability
                    // barrier. Its ACK or NACK ends this responder round, so
                    // no fetch authorization may survive into a retry.
                    served_reconciles.remove(&session_key);
                }

                if tracked {
                    let response = if let Some(error) = failure {
                        EnvelopeMsg::BatchNack(BatchNack {
                            session_id: b.session_id,
                            batch_seq: b.batch_seq,
                            fragment: b.fragment,
                            error,
                        })
                    } else {
                        let level = match durability.expect("successful durability fence") {
                            ApplyDurability::MemoryApplied => ReplicationAckLevel::Applied,
                            ApplyDurability::StoreCommitted => ReplicationAckLevel::StoreCommitted,
                            ApplyDurability::StableCommitted => {
                                ReplicationAckLevel::StableCommitted
                            }
                        };
                        EnvelopeMsg::BatchAck(BatchAck {
                            session_id: b.session_id,
                            batch_seq: b.batch_seq,
                            fragment: b.fragment,
                            level: level as i32,
                        })
                    };
                    if !send_envelope(
                        &tx,
                        Envelope {
                            msg: Some(response),
                        },
                    ) {
                        break;
                    }
                } else if let Some(error) = failure {
                    reconcile_failures
                        .entry(session_key)
                        .or_insert_with(|| (b.session_id, error.clone()));
                    tracing::warn!(
                        peer = %peer,
                        fragment = frag.0,
                        %error,
                        "reconciliation batch failed"
                    );
                }
            }
            Some(EnvelopeMsg::BatchAck(a)) => {
                // Route the ack to the sender for (fragment, peer).
                if let Some(peer) = peer_node_id.clone() {
                    let durability = match ReplicationAckLevel::try_from(a.level) {
                        Ok(ReplicationAckLevel::Applied) => ReplicationAckDurability::MemoryApplied,
                        Ok(ReplicationAckLevel::StoreCommitted) => {
                            ReplicationAckDurability::StoreCommitted
                        }
                        Ok(ReplicationAckLevel::StableCommitted) => {
                            ReplicationAckDurability::StableCommitted
                        }
                        Ok(ReplicationAckLevel::Unknown) | Err(_) => {
                            tracing::warn!(
                                peer = %peer,
                                fragment = a.fragment,
                                level = a.level,
                                "ignoring replication ack with unknown durability"
                            );
                            continue;
                        }
                    };
                    let sender_key = (FragmentId(a.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        if tx
                            .send(SenderMsg::Ack {
                                session_id: a.session_id,
                                batch_seq: a.batch_seq,
                                durability,
                            })
                            .is_err()
                        {
                            let still_current =
                                sender_task_is_current(&senders, &sender_key, &tx).await;
                            if still_current {
                                break;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::BatchNack(n)) => {
                if let Some(peer) = peer_node_id.clone() {
                    let sender_key = (FragmentId(n.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        if tx
                            .send(SenderMsg::Nack {
                                session_id: n.session_id,
                                batch_seq: n.batch_seq,
                                error: n.error,
                            })
                            .is_err()
                        {
                            let still_current =
                                sender_task_is_current(&senders, &sender_key, &tx).await;
                            if still_current {
                                break;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileRequest(r)) => {
                // Peer wants one stable, ordered page of version summaries.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(r.fragment);
                let current = sessions.lock().await.get(&(peer.clone(), frag)).copied();
                // Both identifiers are nonzero so old senders, whose newly
                // added scalar fields decode to defaults, fail closed.
                if r.session_id == 0
                    || r.round_id == 0
                    || current.map(|session| session.session_id) != Some(r.session_id)
                {
                    tracing::debug!(
                        fragment = r.fragment,
                        session_id = r.session_id,
                        round_id = r.round_id,
                        "ignoring stale or uncorrelated reconcile request"
                    );
                    continue;
                }
                let recovery_authorized = recovery.as_ref().is_some_and(|authority| {
                    authority.authorizes(
                        frag,
                        &peer,
                        &r.recovery_epoch,
                        &r.recovery_replica_digest,
                        &placement.read().unwrap(),
                    )
                });
                if !serving.read().unwrap().contains(&frag) && !recovery_authorized {
                    // Independent placement observers can legitimately race:
                    // the serving replica may start its routine round before
                    // this node has finished bootstrapping the same fragment.
                    // Ignore that round so its sender times out/retries, while
                    // preserving the multiplexed connection for this node's
                    // reverse bootstrap and for unrelated fragments.
                    tracing::debug!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        "ignoring reconciliation for a non-serving fragment"
                    );
                    continue;
                }
                let first_page = r.page_seq == 0;
                let reconcile_key = (peer.clone(), frag);
                if (first_page
                    && (!r.after_key.is_empty()
                        || r.snapshot_revision != 0
                        || r.snapshot_time_ms != 0))
                    || (!first_page && r.snapshot_time_ms == 0)
                {
                    tracing::warn!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        page_seq = r.page_seq,
                        "rejecting malformed reconcile page request"
                    );
                    break;
                }
                let route = router.route_with_generation(frag);
                if !first_page
                    && !served_reconciles
                        .get(&reconcile_key)
                        .zip(route.as_ref())
                        .is_some_and(|(state, (generation, _))| {
                            state.route_generation == *generation
                        })
                {
                    // A legitimate placement remove/re-add can invalidate a
                    // paged round between frames. Drop its authorization and
                    // let the correlated sender rotate/retry without closing
                    // the multiplexed connection.
                    served_reconciles.remove(&reconcile_key);
                    continue;
                }
                if !first_page
                    && !served_reconciles.get(&reconcile_key).is_some_and(|state| {
                        state.session_id == r.session_id
                            && state.round_id == r.round_id
                            && state.next_page_seq == r.page_seq
                            && state.next_after_key == r.after_key
                            && state.snapshot_revision == r.snapshot_revision
                            && state.snapshot_time_ms == r.snapshot_time_ms
                    })
                {
                    tracing::warn!(
                        peer = %peer,
                        fragment = r.fragment,
                        round_id = r.round_id,
                        page_seq = r.page_seq,
                        "rejecting unexpected reconcile continuation"
                    );
                    break;
                }
                if let Some((route_generation, actor)) = route {
                    if let Ok(CallResult::Success(page)) = actor
                        .call(
                            |reply| KvMessage::GetSummaryPage {
                                after_key: (!first_page).then_some(r.after_key.clone()),
                                // Do not hold or clone a fragment-wide
                                // snapshot across network turns. Revision
                                // drift is returned to the requester, which
                                // completes this bounded pass and then runs a
                                // mutation-free verification pass.
                                expected_revision: None,
                                snapshot_time_ms: (!first_page).then_some(r.snapshot_time_ms),
                                limit: RECONCILE_SUMMARY_PAGE_ITEMS,
                                reply,
                            },
                            None,
                        )
                        .await
                    {
                        let summaries: Vec<KeySummary> = page
                            .summaries
                            .into_iter()
                            .map(|summary| KeySummary {
                                key: summary.key,
                                timestamp_ms: summary.timestamp_ms,
                                logical_counter: summary.logical_counter,
                                node_id: summary.node_id,
                                is_tombstone: summary.is_tombstone,
                                content_digest: summary.content_digest.to_vec(),
                            })
                            .collect();
                        let mut response = ReconcileResponse {
                            fragment: r.fragment,
                            summaries,
                            session_id: r.session_id,
                            round_id: r.round_id,
                            page_seq: r.page_seq,
                            next_after_key: String::new(),
                            snapshot_revision: page.revision,
                            snapshot_time_ms: page.snapshot_time_ms,
                            done: false,
                            restart: page.restart,
                        };
                        if let Err(encoded_len) =
                            fit_reconcile_response_page(&mut response, &r.after_key, page.done)
                        {
                            tracing::warn!(
                                peer = %peer,
                                fragment = r.fragment,
                                round_id = r.round_id,
                                page_seq = r.page_seq,
                                encoded_len,
                                "one reconcile summary exceeds the hard frame limit"
                            );
                            break 'connection;
                        }
                        let (
                            snapshot_revision,
                            snapshot_time_ms,
                            next_fetch_chunk_seq,
                            served_route_generation,
                        ) = if first_page {
                            (
                                response.snapshot_revision,
                                response.snapshot_time_ms,
                                0,
                                route_generation,
                            )
                        } else {
                            let state = served_reconciles
                                .get(&reconcile_key)
                                .expect("validated continuation");
                            (
                                state.snapshot_revision,
                                state.snapshot_time_ms,
                                state.next_fetch_chunk_seq,
                                state.route_generation,
                            )
                        };
                        let served_state = (!response.restart).then(|| ServedReconcileState {
                            recovery_epoch: r.recovery_epoch.clone(),
                            recovery_replica_digest: r.recovery_replica_digest.clone(),
                            session_id: response.session_id,
                            round_id: response.round_id,
                            next_page_seq: response.page_seq.saturating_add(1),
                            next_after_key: response.next_after_key.clone(),
                            snapshot_revision,
                            snapshot_time_ms,
                            next_fetch_chunk_seq,
                            advertised_keys: response
                                .summaries
                                .iter()
                                .map(|summary| summary.key.clone())
                                .collect(),
                            route_generation: served_route_generation,
                        });
                        let envelope = Envelope {
                            msg: Some(EnvelopeMsg::ReconcileResponse(response)),
                        };
                        if !try_send_envelope_immediate(&tx, envelope) {
                            break 'connection;
                        }
                        if let Some(state) = served_state {
                            served_reconciles.insert(reconcile_key.clone(), state);
                        } else {
                            served_reconciles.remove(&reconcile_key);
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileFetch(f)) => {
                // Peer wants full values for a bounded key batch. Read values
                // incrementally and stream exact-byte-bounded pages so neither
                // the cache lock nor this connection owns an unbounded clone.
                let peer = match peer_node_id.clone() {
                    Some(peer) => peer,
                    None => continue,
                };
                let frag = FragmentId(f.fragment);
                let current = sessions.lock().await.get(&(peer.clone(), frag)).copied();
                if f.session_id == 0
                    || f.round_id == 0
                    || current.map(|session| session.session_id) != Some(f.session_id)
                {
                    tracing::debug!(
                        fragment = f.fragment,
                        session_id = f.session_id,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "ignoring stale or uncorrelated reconcile fetch"
                    );
                    continue;
                }
                let recovery_authorized =
                    served_reconciles
                        .get(&(peer.clone(), frag))
                        .is_some_and(|state| {
                            recovery.as_ref().is_some_and(|authority| {
                                authority.authorizes(
                                    frag,
                                    &peer,
                                    &state.recovery_epoch,
                                    &state.recovery_replica_digest,
                                    &placement.read().unwrap(),
                                )
                            })
                        });
                if !serving.read().unwrap().contains(&frag) && !recovery_authorized {
                    // A fragment can lose authority between the summary and
                    // fetch phases. Let the correlated sender expire and retry
                    // without tearing down other traffic on this connection.
                    tracing::debug!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "ignoring reconciliation fetch for a non-serving fragment"
                    );
                    continue;
                }
                if f.keys.is_empty() || f.keys.len() > MAX_RECONCILE_FETCH_KEYS {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        key_count = f.keys.len(),
                        "rejecting reconcile fetch outside the key-count bound"
                    );
                    break;
                }
                let mut unique_keys = HashSet::with_capacity(f.keys.len());
                if f.keys.iter().any(|key| {
                    !router.key_belongs_to(frag, key) || !unique_keys.insert(key.as_str())
                }) {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "rejecting duplicate or wrong-fragment reconcile fetch"
                    );
                    break;
                }
                let reconcile_key = (peer.clone(), frag);
                let Some((route_generation, actor)) = router.route_with_generation(frag) else {
                    served_reconciles.remove(&reconcile_key);
                    continue;
                };
                if served_reconciles
                    .get(&reconcile_key)
                    .is_some_and(|state| state.route_generation != route_generation)
                {
                    // A legitimate remove/re-add invalidates an otherwise
                    // authorized fetch without making the shared connection
                    // malicious. An absent authorization, by contrast, falls
                    // through to the strict correlation check below and
                    // closes the connection.
                    served_reconciles.remove(&reconcile_key);
                    continue;
                }
                let snapshot_time_ms =
                    served_reconciles.get_mut(&reconcile_key).and_then(|state| {
                        if state.session_id != f.session_id
                            || state.round_id != f.round_id
                            || state.next_fetch_chunk_seq != f.chunk_seq
                            || !f.keys.iter().all(|key| state.advertised_keys.contains(key))
                        {
                            return None;
                        }
                        for key in &f.keys {
                            state.advertised_keys.remove(key);
                        }
                        state.next_fetch_chunk_seq = state.next_fetch_chunk_seq.saturating_add(1);
                        Some(state.snapshot_time_ms)
                    });
                let Some(snapshot_time_ms) = snapshot_time_ms else {
                    tracing::warn!(
                        peer = %peer,
                        fragment = f.fragment,
                        round_id = f.round_id,
                        chunk_seq = f.chunk_seq,
                        "rejecting unadvertised or out-of-order reconcile fetch"
                    );
                    break;
                };
                let job = ReconcileResponseJob {
                    peer: peer.clone(),
                    session: current.expect("validated reconcile fetch session"),
                    actor,
                    fetch: f,
                    snapshot_time_ms,
                };
                if reconcile_response_tx.try_send(job).is_err() {
                    tracing::warn!(
                        peer = %peer,
                        fragment = frag.0,
                        "reconcile response queue is unavailable or at capacity"
                    );
                    break;
                }
            }
            Some(EnvelopeMsg::ReconcileResponse(r)) => {
                if r.summaries.len() > MAX_RECONCILE_PAGE_ITEMS {
                    tracing::warn!(
                        item_count = r.summaries.len(),
                        "reconcile summary page is too large"
                    );
                    break;
                }
                // Zero is the protobuf default for peers that do not speak
                // the correlated reconcile protocol. Reject it before it can
                // influence the HLC or reach a sender.
                if r.session_id == 0 || r.round_id == 0 {
                    tracing::debug!(
                        fragment = r.fragment,
                        session_id = r.session_id,
                        round_id = r.round_id,
                        "ignoring uncorrelated reconcile response"
                    );
                    continue;
                }
                // The sender owns exact session/round/phase correlation. Let
                // it validate correlation before this frame may advance the
                // HLC; stale responses must be side-effect free.
                if let Some(peer) = peer_node_id.clone() {
                    let sender_key = (FragmentId(r.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileResponse {
                            session_id: r.session_id,
                            round_id: r.round_id,
                            page_seq: r.page_seq,
                            next_after_key: r.next_after_key,
                            snapshot_revision: r.snapshot_revision,
                            snapshot_time_ms: r.snapshot_time_ms,
                            done: r.done,
                            restart: r.restart,
                            summaries: r.summaries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            // Placement may retire the sender after the map
                            // lookup above. That is a stale local consumer,
                            // not a protocol failure on this multiplexed peer
                            // connection.
                            Err(NodeSenderSendError::Closed) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(NodeSenderSendError::Full) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            // The sender can be retired after accepting the
                            // message but before answering its oneshot. Only a
                            // failure from the still-current task makes this
                            // connection unusable.
                            Ok(Err(_)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => {
                                // A live sender that stops validating control
                                // traffic is unhealthy. A removed/replaced
                                // sender merely lost a placement race.
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ReconcileData(d)) => {
                if d.entries.len() > MAX_RECONCILE_PAGE_ITEMS {
                    tracing::warn!(
                        item_count = d.entries.len(),
                        "reconcile data page is too large"
                    );
                    break;
                }
                if d.session_id == 0 || d.round_id == 0 {
                    tracing::debug!(
                        fragment = d.fragment,
                        session_id = d.session_id,
                        round_id = d.round_id,
                        chunk_seq = d.chunk_seq,
                        "ignoring uncorrelated reconcile data"
                    );
                    continue;
                }
                if let Some(peer) = peer_node_id.clone() {
                    let sender_key = (FragmentId(d.fragment), peer);
                    let sender = senders.lock().await.get(&sender_key).cloned();
                    #[cfg(test)]
                    if sender.is_some() {
                        if let Some(hook) = sender_snapshot_hook.as_ref() {
                            hook.captured.notify_one();
                            hook.resume.notified().await;
                        }
                    }
                    if let Some(tx) = sender {
                        let (connection_valid, validation) = oneshot::channel();
                        match tx.send(SenderMsg::ReconcileData {
                            session_id: d.session_id,
                            round_id: d.round_id,
                            chunk_seq: d.chunk_seq,
                            page_seq: d.page_seq,
                            done: d.done,
                            entries: d.entries,
                            connection_valid,
                        }) {
                            Ok(()) => {}
                            Err(NodeSenderSendError::Closed) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(NodeSenderSendError::Full) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                        match tokio::time::timeout(REMOTE_RPC_TIMEOUT, validation).await {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Ok(Err(_)) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => {
                                let still_current =
                                    sender_task_is_current(&senders, &sender_key, &tx).await;
                                if still_current {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedPut(p)) => {
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(NodeError::ShuttingDown) => {
                        if !send_unavailable_mutation(
                            &tx,
                            p.req_id,
                            vec![p.key],
                            "node is shutting down",
                        ) {
                            break;
                        }
                        continue;
                    }
                    Err(_) => unreachable!("operation admission has one failure mode"),
                };
                if let Err(error) = validate_record_wire_budget(&p.key, p.value.len(), &p.node_id) {
                    tracing::warn!(%error, "rejecting irreducible forwarded mutation");
                    break;
                }
                if !observe_remote_timestamp(
                    &hlc,
                    actordb_protocol::RecordVersion::new(p.timestamp_ms, p.logical_counter),
                    "ForwardedPut",
                )
                .await
                {
                    break;
                }
                let frag = fragment_of(&p.key, router.fragment_count);
                let mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
                let is_serving = serving.read().unwrap().contains(&frag);
                if is_serving {
                    let Some(actor) = router.route_for_fragment(frag) else {
                        drop(mutation_lease);
                        if !send_unavailable_mutation(
                            &tx,
                            p.req_id,
                            vec![p.key],
                            &format!("serving fragment {frag} has no actor"),
                        ) {
                            break;
                        }
                        continue;
                    };
                    let concerned = p.write_concern.is_some();
                    let (job, completion) = MutationJob::new(
                        MutationRequest::Single(p),
                        actor,
                        senders.clone(),
                        frag,
                        operation,
                        mutation_lease,
                    );
                    mutation_service.submit(job);
                    if concerned {
                        let response_tx = tx.clone();
                        if let Some(tasks) = tasks.upgrade() {
                            if tasks
                                .spawn(async move {
                                    if let Ok(completion) = completion.await {
                                        completion.send(&response_tx);
                                    }
                                })
                                .await
                            {
                                continue;
                            }
                        }
                        break;
                    }
                    // Preserve connection frame order. Cancellation of this
                    // waiter never cancels the owned apply/outcome/fanout job.
                    let Ok(completion) = completion.await else {
                        break;
                    };
                    if !completion.send(&tx) {
                        break;
                    }
                } else if p.hops < MAX_FORWARD_HOPS {
                    drop(mutation_lease);
                    relay_put(p, frag, &tx, &peer_node_id, &rfctx, operation).await;
                } else {
                    drop(mutation_lease);
                    if !send_unavailable_mutation(
                        &tx,
                        p.req_id,
                        vec![p.key],
                        "forward hop limit reached",
                    ) {
                        break;
                    }
                }
            }
            Some(EnvelopeMsg::ForwardedGet(g)) => {
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
                // We serve this fragment: read it and reply on this
                // connection. (A bootstrapping node is not yet
                // authoritative, so it relays instead of serving
                // possibly-stale state.)
                let frag = fragment_of(&g.key, router.fragment_count);
                let is_serving = serving.read().unwrap().contains(&frag);
                if !is_serving {
                    if g.hops < MAX_FORWARD_HOPS {
                        relay_get(&g, frag, &tx, &peer_node_id, &rfctx).await;
                    }
                    continue;
                }
                let Some((generation, actor)) = router.route_with_generation(frag) else {
                    // The legacy response cannot express unavailability.
                    continue;
                };
                let priority = priority_from_proto(g.priority);
                if !read_service.try_submit(ReadJob::new(
                    ReadRequest::Single(g),
                    priority,
                    ReadRoute {
                        fragment: frag,
                        generation,
                        actor,
                        router: router.clone(),
                        serving: serving.clone(),
                    },
                    tx.clone(),
                    deadline,
                )) {
                    break;
                }
            }
            Some(EnvelopeMsg::GetResponse(r)) => {
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                // Correlate the authenticated response source before it may
                // remove a waiter or advance the local clock.
                let relay_validation = {
                    let relays = pending_relays.lock().unwrap();
                    relays.get(&r.req_id).and_then(|relay| {
                        (relay.expected_peer == peer && relay.expected_tx.same_channel(&tx)).then(
                            || {
                                (
                                    relay._admission.clone(),
                                    validate_legacy_get_response(&relay.expected_key, &r),
                                )
                            },
                        )
                    })
                };
                let pending_expected_key = {
                    let pending = pending_gets.lock().unwrap();
                    pending.get(&r.req_id).and_then(|request| {
                        (request.expected_peer == peer).then(|| request.expected_key.clone())
                    })
                };
                let relay_admission = relay_validation
                    .as_ref()
                    .map(|(admission, _)| admission.clone());
                let Some(validation) = relay_validation.map(|(_, result)| result).or_else(|| {
                    pending_expected_key
                        .as_deref()
                        .map(|key| validate_legacy_get_response(key, &r))
                }) else {
                    continue;
                };
                if let Err(error) = validation {
                    tracing::warn!(
                        peer = %peer,
                        req_id = r.req_id,
                        %error,
                        "rejecting malformed forwarded-get response"
                    );
                    if let Some(admission) = &relay_admission {
                        if let Some(relay) = remove_get_relay(&pending_relays, r.req_id, admission)
                        {
                            let _ = relay.timeout_cancel.send(());
                        }
                    } else if let Some(pending) = pending_gets.lock().unwrap().remove(&r.req_id) {
                        let _ = pending.reply.send(Err(error));
                    }
                    break;
                }
                if r.found
                    && !observe_remote_timestamp(
                        &hlc,
                        actordb_protocol::RecordVersion::new(r.timestamp_ms, r.logical_counter),
                        "GetResponse",
                    )
                    .await
                {
                    break;
                }
                // A relayed read completing: forward the response to the
                // origin peer with its original req_id.
                let relay = if let Some(admission) = &relay_admission {
                    remove_get_relay(&pending_relays, r.req_id, admission)
                } else {
                    None
                };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    let _ = send_envelope(
                        &relay.origin_tx,
                        Envelope {
                            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                                req_id: relay.origin_req_id,
                                ..r
                            })),
                        },
                    );
                    continue;
                }
                let pending = if pending_expected_key.is_some() {
                    pending_gets.lock().unwrap().remove(&r.req_id)
                } else {
                    None
                };
                if let Some(pending) = pending {
                    if r.shed {
                        let _ = pending.reply.send(Err(NodeError::Backpressure(
                            "remote pool at queue cap or rate limit".to_string(),
                        )));
                    } else if !r.found || r.is_tombstone {
                        let _ = pending.reply.send(Ok(None));
                    } else {
                        let mut value = VersionedValue::new_version(
                            r.value,
                            actordb_protocol::RecordVersion::new(r.timestamp_ms, r.logical_counter),
                            r.node_id,
                        );
                        value.expires_at_ms = (r.expires_at_ms != 0).then_some(r.expires_at_ms);
                        let _ = pending.reply.send(Ok(Some(value)));
                    }
                }
            }
            Some(EnvelopeMsg::MutationResponse(mut response)) => {
                if response.results.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = response.results.len(),
                        "mutation response is too large"
                    );
                    break;
                }
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let relay_validation = {
                    let relays = pending_mutation_relays.lock().unwrap();
                    relays.get(&response.req_id).and_then(|relay| {
                        (relay.expected_peer == peer && relay.expected_tx.same_channel(&tx)).then(
                            || {
                                (
                                    relay._admission.clone(),
                                    validate_mutation_results(
                                        &relay.expected_keys,
                                        &response.results,
                                    ),
                                )
                            },
                        )
                    })
                };
                let relay_admission = relay_validation
                    .as_ref()
                    .map(|(admission, _)| admission.clone());
                let validation = relay_validation.map(|(_, result)| result).or_else(|| {
                    let pending = pending_mutations.lock().unwrap();
                    pending.get(&response.req_id).and_then(|request| {
                        (request.expected_peer == peer).then(|| {
                            validate_mutation_results(&request.expected_keys, &response.results)
                        })
                    })
                });
                let Some(validation) = validation else {
                    continue;
                };
                if let Err(error) = validation {
                    tracing::warn!(
                        peer = %peer,
                        req_id = response.req_id,
                        %error,
                        "ignoring malformed mutation response"
                    );
                    continue;
                }
                if let Some(timestamp_ms) = response
                    .results
                    .iter()
                    .filter(|result| {
                        MutationStatus::try_from(result.status) == Ok(MutationStatus::Superseded)
                    })
                    .map(|result| {
                        actordb_protocol::RecordVersion::new(
                            result.winner_timestamp_ms,
                            result.winner_logical_counter,
                        )
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "MutationResponse").await {
                        break;
                    }
                }

                let relay = if let Some(admission) = &relay_admission {
                    remove_mutation_relay(&pending_mutation_relays, response.req_id, admission)
                } else {
                    None
                };
                if let Some(relay) = relay {
                    let _ = relay.timeout_cancel.send(());
                    response.req_id = relay.origin_req_id;
                    let _ = send_mutation_response(&relay.origin_tx, response);
                    continue;
                }

                let pending = {
                    let mut pending = pending_mutations.lock().unwrap();
                    if matches!(
                        pending.get(&response.req_id),
                        Some(request) if request.expected_peer == peer
                    ) {
                        pending.remove(&response.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(response.results);
                }
            }
            Some(EnvelopeMsg::BatchPut(bp)) => {
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(NodeError::ShuttingDown) => {
                        let keys = bp.puts.into_iter().map(|put| put.key).collect();
                        if !send_unavailable_mutation(&tx, bp.req_id, keys, "node is shutting down")
                        {
                            break;
                        }
                        continue;
                    }
                    Err(_) => unreachable!("operation admission has one failure mode"),
                };
                if bp.puts.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = bp.puts.len(),
                        "forwarded mutation batch is too large"
                    );
                    break;
                }
                if let Err(error) = validate_batch_put_wire_bounds(&bp) {
                    tracing::warn!(%error, "rejecting improperly bounded batch put");
                    break;
                }
                if let Some(error) = bp.puts.iter().find_map(|put| {
                    validate_record_wire_budget(&put.key, put.value.len(), &put.node_id).err()
                }) {
                    tracing::warn!(%error, "rejecting irreducible item in batch put");
                    break;
                }
                if let Some(timestamp_ms) = bp
                    .puts
                    .iter()
                    .map(|p| {
                        actordb_protocol::RecordVersion::new(p.timestamp_ms, p.logical_counter)
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchPut").await {
                        break;
                    }
                }
                let Some(frag) = bp
                    .puts
                    .first()
                    .map(|p| fragment_of(&p.key, router.fragment_count))
                else {
                    if !send_mutation_response(
                        &tx,
                        MutationResponse {
                            req_id: bp.req_id,
                            results: Vec::new(),
                        },
                    ) {
                        break;
                    }
                    continue;
                };
                if bp
                    .puts
                    .iter()
                    .any(|p| fragment_of(&p.key, router.fragment_count) != frag)
                {
                    if !send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        bp.puts.into_iter().map(|put| put.key).collect(),
                        "batch contains keys from multiple fragments",
                    ) {
                        break;
                    }
                    continue;
                }

                let mutation_lease = if dynamic_placement {
                    Some(router.mutation_gate(frag).read_owned().await)
                } else {
                    None
                };
                let is_serving = serving.read().unwrap().contains(&frag);
                if !is_serving {
                    drop(mutation_lease);
                    let max_hops = bp.puts.iter().map(|p| p.hops).max().unwrap_or(0);
                    if max_hops < MAX_FORWARD_HOPS {
                        relay_batch(bp, frag, &tx, &peer_node_id, &rfctx, operation).await;
                    } else {
                        if !send_unavailable_mutation(
                            &tx,
                            bp.req_id,
                            bp.puts.into_iter().map(|put| put.key).collect(),
                            "forward hop limit reached",
                        ) {
                            break;
                        }
                    }
                    continue;
                }

                let Some(actor) = router.route_for_fragment(frag) else {
                    drop(mutation_lease);
                    if !send_unavailable_mutation(
                        &tx,
                        bp.req_id,
                        bp.puts.into_iter().map(|put| put.key).collect(),
                        "serving fragment has no actor",
                    ) {
                        break;
                    }
                    continue;
                };
                let (job, completion) = MutationJob::new(
                    MutationRequest::Batch(bp),
                    actor,
                    senders.clone(),
                    frag,
                    operation,
                    mutation_lease,
                );
                mutation_service.submit(job);
                let Ok(completion) = completion.await else {
                    break;
                };
                if !completion.send(&tx) {
                    break;
                }
            }
            Some(EnvelopeMsg::BatchGet(bg)) => {
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
                if bg.keys.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(
                        item_count = bg.keys.len(),
                        "forwarded read batch is too large"
                    );
                    break;
                }
                let req_id = bg.req_id;
                let Some(frag) = bg
                    .keys
                    .first()
                    .map(|key| fragment_of(key, router.fragment_count))
                else {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                };
                if bg
                    .keys
                    .iter()
                    .any(|key| fragment_of(key, router.fragment_count) != frag)
                {
                    tracing::warn!(req_id, "batch get contains keys from multiple fragments");
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                }
                if !serving.read().unwrap().contains(&frag) {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                }
                let Some((generation, actor)) = router.route_with_generation(frag) else {
                    if !send_batch_get_response(&tx, req_id, Vec::new()) {
                        break;
                    }
                    continue;
                };
                let priority = priority_from_proto(bg.priority);
                if !read_service.try_submit(ReadJob::new(
                    ReadRequest::Batch(bg),
                    priority,
                    ReadRoute {
                        fragment: frag,
                        generation,
                        actor,
                        router: router.clone(),
                        serving: serving.clone(),
                    },
                    tx.clone(),
                    deadline,
                )) {
                    break;
                }
            }
            Some(EnvelopeMsg::BatchGetResponse(r)) => {
                if r.results.len() > MAX_WIRE_BATCH_ITEMS {
                    tracing::warn!(item_count = r.results.len(), "read response is too large");
                    break;
                }
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let validation = {
                    let pending = pending_batch_gets.lock().unwrap();
                    pending.get(&r.req_id).and_then(|request| {
                        (request.expected_peer == peer)
                            .then(|| validate_batch_get_results(&request.expected_keys, &r.results))
                    })
                };
                let Some(validation) = validation else {
                    continue;
                };
                if let Err(error) = validation {
                    tracing::warn!(
                        peer = %peer,
                        req_id = r.req_id,
                        %error,
                        "rejecting malformed batch-get response"
                    );
                    if let Some(pending) = pending_batch_gets.lock().unwrap().remove(&r.req_id) {
                        let _ = pending.reply.send(Err(error));
                    }
                    break;
                }
                if let Some(timestamp_ms) = r
                    .results
                    .iter()
                    .filter(|result| result.found)
                    .map(|result| {
                        actordb_protocol::RecordVersion::new(
                            result.timestamp_ms,
                            result.logical_counter,
                        )
                    })
                    .max()
                {
                    if !observe_remote_timestamp(&hlc, timestamp_ms, "BatchGetResponse").await {
                        break;
                    }
                }
                let pending = {
                    let mut pending = pending_batch_gets.lock().unwrap();
                    if matches!(
                        pending.get(&r.req_id),
                        Some(request) if request.expected_peer == peer
                    ) {
                        pending.remove(&r.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(Ok(r.results));
                }
            }
            Some(EnvelopeMsg::SyncRequest(_)) | Some(EnvelopeMsg::SyncResponse(_)) => {
                // Protocol v3 deliberately has one anti-entropy engine. A
                // negotiated v3 peer that sends a legacy full-sync frame is
                // either malformed or has falsely advertised capabilities;
                // fail closed instead of rebuilding an unbounded response.
                tracing::warn!("closing peer that sent a retired full-state sync frame");
                break;
            }
            Some(EnvelopeMsg::QueryPageRequest(req)) => {
                if tx.capabilities & CAPABILITY_QUERY_PAGES == 0 {
                    break;
                }
                let retention = match query::reserve_inbound(&query_admission, &req) {
                    Ok(retention) => retention,
                    Err(error) => {
                        let page = crate::query::page_error(
                            &req,
                            crate::proto::QueryPageStatus::Backpressure,
                            error.to_string(),
                        );
                        let _ = try_send_envelope_until(
                            &tx,
                            crate::query::page_envelope(page),
                            deadline_after(runtime_options.queue_admission_timeout),
                        )
                        .await;
                        continue;
                    }
                };
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(_) => {
                        let page = crate::query::page_error(
                            &req,
                            crate::proto::QueryPageStatus::Unavailable,
                            "node is shutting down",
                        );
                        let _ = try_send_envelope_until(
                            &tx,
                            crate::query::page_envelope(page),
                            deadline_after(runtime_options.queue_admission_timeout),
                        )
                        .await;
                        continue;
                    }
                };
                let Some(tasks) = tasks.upgrade() else {
                    break;
                };
                let channel = tx.clone();
                let router = router.clone();
                let serving = serving.clone();
                let deadline = deadline_after(runtime_options.forwarded_attempt_timeout);
                if !tasks
                    .spawn(async move {
                        let _operation = operation;
                        query::serve_query_page(channel, router, serving, req, retention, deadline)
                            .await;
                    })
                    .await
                {
                    break;
                }
            }
            Some(EnvelopeMsg::QueryPageResponse(response)) => {
                if tx.capabilities & CAPABILITY_QUERY_PAGES == 0 {
                    break;
                }
                let pending = {
                    let mut pending = pending_query_pages.lock().unwrap();
                    if pending
                        .get(&response.req_id)
                        .is_some_and(|request| request.channel.same_channel(&tx))
                    {
                        pending.remove(&response.req_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.reply.send(response);
                }
            }
            Some(EnvelopeMsg::ScanRequest(req)) => {
                // Serve the scan without blocking the connection read
                // loop: the spawned task runs the fragment actor call and
                // replies with a single bounded ScanResponse.
                let tx = tx.clone();
                let serving = serving.clone();
                let router = router.clone();
                if tx.capabilities & CAPABILITY_SCAN == 0 {
                    break;
                }
                let retention = match query_admission
                    .reserve_work(req.encoded_len().saturating_add(1024), 48 * 1024 * 1024)
                {
                    Ok(retention) => retention,
                    Err(error) => {
                        scan::send_scan_response(&tx, req.req_id, Vec::new(), error.to_string())
                            .await;
                        continue;
                    }
                };
                let operation = match operation_gate.try_enter() {
                    Ok(operation) => operation,
                    Err(_) => {
                        scan::send_scan_response(
                            &tx,
                            req.req_id,
                            Vec::new(),
                            "node is shutting down".into(),
                        )
                        .await;
                        continue;
                    }
                };
                let Some(tasks) = tasks.upgrade() else {
                    break;
                };
                if !tasks
                    .spawn(async move {
                        let _operation = operation;
                        scan::serve_scan_request(tx, serving, router, req, retention).await;
                    })
                    .await
                {
                    break;
                }
            }
            Some(EnvelopeMsg::ScanResponse(resp)) => {
                let Some(peer) = peer_node_id.clone() else {
                    continue;
                };
                let pending = {
                    let mut pending = pending_scans.lock().unwrap();
                    match pending.get(&resp.req_id) {
                        Some(request) if request.expected_peer == peer => {
                            pending.remove(&resp.req_id)
                        }
                        _ => None,
                    }
                };
                if let Some(pending) = pending {
                    let result = if resp.error.is_empty() {
                        Ok(resp.matches)
                    } else {
                        Err(resp.error)
                    };
                    let _ = pending.reply.send(result);
                }
            }
            None => {}
        }
    }

    tx.cancel();
    pending_query_pages
        .lock()
        .unwrap()
        .retain(|_, request| !request.channel.same_channel(&tx));
    // Connection dropped: forget the peer only if this exact physical channel
    // is still current. Legacy full-replication peers are discovered at
    // runtime, so reap their per-fragment senders on disconnect rather than
    // letting sequential claimed identities grow the sender maps forever.
    // The next established session's immediate correlated reconciliation
    // covers writes made while that peer is absent. Explicit placement maps
    // retain sender buffers because their peer cardinality is configured and
    // bounded.
    // End relay ownership before asynchronous peer/sender cleanup can wait on
    // another task. This physical channel's replacement is never affected.
    tx.cancel();
    cancel_connection_relays(&tx, &pending_relays, &pending_mutation_relays);
    let mut disconnected_senders = Vec::new();
    if let Some(peer) = handshake.established_peer_id().map(str::to_owned) {
        let mut guard = peers.lock().await;
        // Only remove if it's still our entry (a newer connection may
        // have replaced it).
        if let Some(tx_existing) = guard.get(&peer) {
            if tx_existing.same_channel(&tx) {
                guard.remove(&peer);
                let legacy_full_replication = placement.read().unwrap().is_empty();
                if legacy_full_replication && operation_gate.is_running() {
                    let mut senders_guard = senders.lock().await;
                    let keys: Vec<(FragmentId, String)> = senders_guard
                        .keys()
                        .filter(|(_, sender_peer)| sender_peer == &peer)
                        .cloned()
                        .collect();
                    let mut reports_guard = sender_reports.lock().await;
                    for key in keys {
                        if let Some(sender) = senders_guard.get(&key).cloned() {
                            disconnected_senders.push(retiring_senders.retire(sender));
                            senders_guard.remove(&key);
                        }
                        reports_guard.remove(&key);
                    }
                } else {
                    // Explicitly placed senders, plus legacy senders already
                    // captured by a shutdown fence, retain their recovery
                    // state for reconnect. Only notify if this was the
                    // current channel.
                    let peer_senders: Vec<NodeSender> = senders
                        .lock()
                        .await
                        .iter()
                        .filter(|((_, sender_peer), _)| sender_peer == &peer)
                        .map(|(_, sender)| sender.clone())
                        .collect();
                    for sender_tx in peer_senders {
                        let _ = sender_tx.send(SenderMsg::PeerDisconnected {
                            peer_tx: tx.raw_sender(),
                        });
                    }
                }
                // Phase 8: wake the placement driver so the controller
                // sees the membership change promptly.
                if let Some(mtx) = membership_tx {
                    mtx.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
                }
                peer_changes.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
            }
        }
    }
    drop(reconcile_response_tx);
    for sender in disconnected_senders {
        sender.join().await;
    }
    ConnectionOutcome {
        peer_id: authenticated_peer_id,
        was_established,
    }
}

/// Own one outbound dial intent until shutdown. Authentication/protocol
/// failures back off just like transport failures. A duplicate loser parks on
/// peer-map changes while the elected connection is healthy instead of
/// continuously rotating replication sessions.
pub(super) async fn dial_supervisor(
    ctx: ConnContext,
    network: Arc<dyn Network>,
    addr: SocketAddr,
    mut expected_peer_id: Option<String>,
    mut initial_stream: Option<BoxStream>,
) {
    let mut backoff = RECONNECT_BASE;
    loop {
        let stream = match initial_stream.take() {
            Some(stream) => stream,
            None => match tokio::time::timeout(
                ctx.runtime_options.connect_timeout,
                network.connect(addr),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::debug!(%addr, %error, "peer dial failed");
                    tokio::time::sleep(backoff + jitter(backoff)).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    continue;
                }
                Err(_) => {
                    tracing::debug!(%addr, "peer dial timed out");
                    tokio::time::sleep(backoff + jitter(backoff)).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    continue;
                }
            },
        };

        let outcome = handle_connection(
            ctx.clone(),
            stream,
            /* we_dialed = */ true,
            expected_peer_id.clone(),
            None,
        )
        .await;

        // Address-only `connect` pins the first authenticated identity for
        // every later reconnect. Otherwise an address reused by a different
        // node could make the dial intent look healthy while the original
        // routing/replication target remained permanently absent.
        if expected_peer_id.is_none() {
            expected_peer_id.clone_from(&outcome.peer_id);
        }

        if let Some(peer) = outcome.peer_id.as_deref() {
            let mut changes = ctx.peer_changes.subscribe();
            let winner_is_live = ctx.peers.lock().await.contains_key(peer);
            if winner_is_live {
                while ctx.peers.lock().await.contains_key(peer) {
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
                backoff = RECONNECT_BASE;
                continue;
            }
        }

        if outcome.was_established {
            backoff = RECONNECT_BASE;
        }
        tokio::time::sleep(backoff + jitter(backoff)).await;
        if !outcome.was_established {
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }
}

/// Up to 50% jitter from clock nanos (no rng dependency for this),
/// so a network-wide blip doesn't make every node redial in lockstep.
pub(super) fn jitter(backoff: Duration) -> Duration {
    let half_ms = backoff.as_millis() as u64 / 2;
    if half_ms == 0 {
        return Duration::ZERO;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(nanos % (half_ms + 1))
}

/// Legacy gossip: re-send an op to all peers except the connection it
/// arrived on. Only used when no fragment_map is configured; the sharded
/// path fans out from the coordinator instead.
pub(super) fn fresh_hello(node_id: &str) -> Result<Hello, getrandom::Error> {
    let mut challenge = vec![0u8; HANDSHAKE_CHALLENGE_BYTES];
    getrandom::fill(&mut challenge)?;
    Ok(Hello {
        node_id: node_id.to_string(),
        auth_tag: Vec::new(),
        min_protocol_version: WIRE_PROTOCOL_VERSION,
        max_protocol_version: WIRE_PROTOCOL_VERSION,
        capabilities: REQUIRED_WIRE_CAPABILITIES
            | CAPABILITY_SCAN
            | CAPABILITY_QUERY_PAGES
            | CAPABILITY_QUERY_METADATA,
        challenge,
    })
}

pub(super) async fn send_hello_until(tx: &PeerChannel, hello: Hello, deadline: Instant) -> bool {
    try_send_envelope_until(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(hello)),
        },
        deadline,
    )
    .await
}

pub(super) async fn send_hello_auth_until(
    tx: &PeerChannel,
    auth_tag: Vec<u8>,
    deadline: Instant,
) -> bool {
    try_send_envelope_until(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })),
        },
        deadline,
    )
    .await
}

/// Canonical manual encoding used by both HMAC and connection election. It is
/// deliberately independent of protobuf encoding and binds both advertised
/// offers plus the selected protocol/capabilities.
pub(super) fn handshake_transcript(dialer: &Hello, listener: &Hello) -> Vec<u8> {
    fn append_hello(out: &mut Vec<u8>, role: &[u8], hello: &Hello) {
        out.extend_from_slice(&(role.len() as u64).to_be_bytes());
        out.extend_from_slice(role);
        out.extend_from_slice(&(hello.node_id.len() as u64).to_be_bytes());
        out.extend_from_slice(hello.node_id.as_bytes());
        out.extend_from_slice(&hello.min_protocol_version.to_be_bytes());
        out.extend_from_slice(&hello.max_protocol_version.to_be_bytes());
        out.extend_from_slice(&hello.capabilities.to_be_bytes());
        out.extend_from_slice(&(hello.challenge.len() as u64).to_be_bytes());
        out.extend_from_slice(&hello.challenge);
    }

    let mut transcript = Vec::with_capacity(256);
    transcript.extend_from_slice(b"actordb-handshake-transcript-v2\0");
    append_hello(&mut transcript, b"dialer", dialer);
    append_hello(&mut transcript, b"listener", listener);
    transcript.extend_from_slice(&WIRE_PROTOCOL_VERSION.to_be_bytes());
    transcript.extend_from_slice(&(dialer.capabilities & listener.capabilities).to_be_bytes());
    transcript
}

pub(super) fn handshake_auth_tag(
    secret: &[u8],
    prover_is_dialer: bool,
    dialer: &Hello,
    listener: &Hello,
) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(b"actordb-handshake-proof-v2\0");
    mac.update(if prover_is_dialer {
        b"dialer"
    } else {
        b"listener"
    });
    mac.update(&handshake_transcript(dialer, listener));
    mac.finalize().into_bytes().to_vec()
}

pub(super) fn verify_handshake_auth(
    secret: Option<&[u8]>,
    prover_is_dialer: bool,
    dialer: &Hello,
    listener: &Hello,
    tag: &[u8],
) -> bool {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    match secret {
        Some(secret) => {
            if tag.len() != 32 {
                return false;
            }
            // `verify_slice` is constant-time.
            let mut verifier =
                Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
            verifier.update(b"actordb-handshake-proof-v2\0");
            verifier.update(if prover_is_dialer {
                b"dialer"
            } else {
                b"listener"
            });
            verifier.update(&handshake_transcript(dialer, listener));
            verifier.verify_slice(tag).is_ok()
        }
        None => tag.is_empty(),
    }
}

pub(super) fn connection_rank(dialer: &Hello, listener: &Hello) -> ConnectionRank {
    use sha2::{Digest, Sha256};

    let transcript_id: [u8; 32] = Sha256::digest(handshake_transcript(dialer, listener)).into();
    ConnectionRank {
        preferred_direction: dialer.node_id < listener.node_id,
        transcript_id,
    }
}

pub(super) fn send_envelope(tx: &PeerChannel, envelope: Envelope) -> bool {
    try_send_envelope_immediate(tx, envelope)
}

/// Admit a reader-generated response without ever parking the sole socket
/// reader behind its own writer. The writer drains this reserved lane before
/// ordinary producer traffic. Saturation cancels the physical connection;
/// correlated senders then retry, while the reader remains available to drain
/// frames that let the opposite writer make progress.
pub(super) fn try_send_envelope_immediate(tx: &PeerChannel, envelope: Envelope) -> bool {
    let encoded_len = match check_envelope_budget(&envelope, MAX_FRAME_BYTES) {
        Ok(encoded_len) => encoded_len,
        Err(error) => {
            tracing::warn!(%error, "rejecting oversized or invalid outbound peer frame");
            return false;
        }
    };
    let frame_permit = match tx.outbound.response_tx.try_reserve() {
        Ok(permit) => permit,
        Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let byte_permit = match tx
        .outbound
        .byte_budget
        .clone()
        .try_acquire_many_owned(encoded_len as u32)
    {
        Ok(permit) => permit,
        Err(_) => {
            drop(frame_permit);
            tx.cancel();
            return false;
        }
    };
    let global_permits = match tx
        .outbound
        .node_byte_budget
        .try_response(encoded_len as u32)
    {
        Ok(permits) => permits,
        Err(()) => {
            tx.cancel();
            return false;
        }
    };
    let frame = match encode_envelope(&envelope) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(%error, "failed to encode preflighted outbound peer frame");
            return false;
        }
    };
    frame_permit
        .send(BudgetedFrame::new_response(frame, byte_permit).with_global_permits(global_permits));
    true
}

#[cfg(test)]
pub(super) async fn try_send_envelope(tx: &PeerChannel, envelope: Envelope) -> bool {
    try_send_envelope_with_deadline(tx, envelope, None, tx.queue_admission_timeout).await
}

pub(super) async fn try_send_envelope_until(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Instant,
) -> bool {
    try_send_envelope_with_deadline(
        tx,
        envelope,
        Some(attempt_deadline),
        tx.queue_admission_timeout,
    )
    .await
}

pub(super) async fn try_send_envelope_with_admission_timeout(
    tx: &PeerChannel,
    envelope: Envelope,
    admission_timeout: Duration,
) -> bool {
    try_send_envelope_with_deadline(tx, envelope, None, admission_timeout).await
}

pub(super) async fn try_send_envelope_with_deadline(
    tx: &PeerChannel,
    envelope: Envelope,
    attempt_deadline: Option<Instant>,
    admission_timeout: Duration,
) -> bool {
    let encoded_len = match check_envelope_budget(&envelope, MAX_FRAME_BYTES) {
        Ok(encoded_len) => encoded_len,
        Err(error) => {
            tracing::warn!(%error, "rejecting oversized or invalid outbound peer frame");
            return false;
        }
    };
    let queue_deadline = deadline_after(admission_timeout);
    let admission_deadline = attempt_deadline
        .map(|deadline| deadline.min(queue_deadline))
        .unwrap_or(queue_deadline);
    if Instant::now() >= admission_deadline {
        tx.cancel();
        return false;
    }
    let frame_permit =
        match tokio::time::timeout_at(admission_deadline, tx.outbound.tx.reserve()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                tx.cancel();
                return false;
            }
        };
    let producer_byte_permit = match tokio::time::timeout_at(
        admission_deadline,
        tx.outbound
            .producer_byte_budget
            .clone()
            .acquire_many_owned(encoded_len as u32),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) | Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let byte_permit = match tokio::time::timeout_at(
        admission_deadline,
        tx.outbound
            .byte_budget
            .clone()
            .acquire_many_owned(encoded_len as u32),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) | Err(_) => {
            tx.cancel();
            return false;
        }
    };
    let global_permits = match tx
        .outbound
        .acquire_global_producer(encoded_len as u32, admission_deadline)
        .await
    {
        Ok(permits) => permits,
        Err(()) => {
            tx.cancel();
            return false;
        }
    };
    if Instant::now() >= admission_deadline {
        tx.cancel();
        return false;
    }
    let frame = match encode_envelope(&envelope) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(%error, "failed to encode preflighted outbound peer frame");
            return false;
        }
    };
    frame_permit.send(
        BudgetedFrame::new_producer(frame, byte_permit, producer_byte_permit)
            .with_global_permits(global_permits),
    );
    true
}
impl Node {
    /// Dial a peer and start a supervised connection. After authentication,
    /// both sides start paged reconciliation, so a late joiner converges
    /// without constructing a monolithic full-state frame.
    ///
    /// The initial dial is tried once: an unreachable peer returns an error.
    /// This method returns `Ok` only after that transport dial succeeds and a
    /// Node-owned supervisor owns the stream; authentication completes
    /// asynchronously. Concurrent calls for the same address wait on the
    /// pending initial dial instead of reporting success early, then share the
    /// same active supervisor. After its first successful authentication, an
    /// address-only supervisor pins that peer id and rejects a different id on
    /// every later reconnect. Dropped connections are redialed with
    /// exponential backoff until [Node::shutdown].
    pub async fn connect(&self, peer_addr: SocketAddr) -> Result<(), NodeError> {
        let _operation = self.admit_operation()?;
        if !self.tasks.is_accepting() {
            return Err(NodeError::ShuttingDown);
        }
        let mut shutdown = self.tasks.shutdown_signal();
        let intent = loop {
            // Subscribe before inspecting the registry so activation/removal
            // cannot be lost between the check and the wait.
            let mut changes = self.dial_intent_changes.subscribe();
            let state = self.dial_intents.lock().unwrap().get(&peer_addr).copied();
            match state {
                Some(DialIntentState::Active) => return Ok(()),
                Some(DialIntentState::Pending) => {
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            let _ = changed;
                            return Err(NodeError::Network("node is shutting down".to_string()));
                        }
                        changed = changes.changed() => {
                            if changed.is_err() {
                                return Err(NodeError::Network("node is shutting down".to_string()));
                            }
                        }
                    }
                }
                None => {
                    if let Some(intent) = DialIntentGuard::reserve(
                        self.dial_intents.clone(),
                        self.dial_intent_changes.clone(),
                        peer_addr,
                    ) {
                        break intent;
                    }
                }
            }
        };
        if *shutdown.borrow() {
            return Err(NodeError::Network("node is shutting down".to_string()));
        }
        let stream = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return Err(NodeError::Network("node is shutting down".to_string()));
            }
            result = tokio::time::timeout(
                self.runtime_options.connect_timeout,
                self.network.connect(peer_addr),
            ) => {
                result
                    .map_err(|_| NodeError::Network("peer connection attempt timed out".to_string()))?
                    .map_err(|e| NodeError::Network(e.to_string()))?
            }
        };
        if self
            .start_reserved_dial_supervisor(intent, None, Some(stream))
            .await
        {
            Ok(())
        } else {
            Err(NodeError::Network("node is shutting down".to_string()))
        }
    }
    pub(super) fn connection_context(&self) -> ConnContext {
        ConnContext {
            recovery: self.recovery.clone(),
            node_id: self.node_id.clone(),
            router: self.router.clone(),
            serving: self.serving.clone(),
            placement: self.fragment_map.clone(),
            dynamic_placement: self.placement_controller.is_some(),
            pending_gets: self.pending_gets.clone(),
            pending_batch_gets: self.pending_batch_gets.clone(),
            pending_scans: self.pending_scans.clone(),
            pending_query_pages: self.pending_query_pages.clone(),
            query_admission: self.query_admission.clone(),
            pending_mutations: self.pending_mutations.clone(),
            pending_relays: self.pending_relays.clone(),
            pending_mutation_relays: self.pending_mutation_relays.clone(),
            next_req_id: self.next_req_id.clone(),
            peers: self.peers.clone(),
            peer_failures: self.peer_failures.clone(),
            tasks: Arc::downgrade(&self.tasks),
            read_service: self.read_service.clone(),
            operation_gate: self.operation_gate.clone(),
            peer_changes: self.peer_changes.clone(),
            hlc: self.hlc.clone(),
            senders: self.senders.clone(),
            finalizing_senders: self.finalizing_senders.clone(),
            retiring_senders: self.retiring_senders.clone(),
            sender_config: self.sender_config.clone(),
            sender_runtime: self
                .runtime_options
                .sender_options()
                .with_write_concerns(self.write_concerns.clone())
                .with_recovery(self.recovery.clone()),
            runtime_options: self.runtime_options.clone(),
            receive_byte_budget: self.receive_byte_budget.clone(),
            node_peer_queue_budget: self.node_peer_queue_budget.clone(),
            relay_admission: self.relay_admission.clone(),
            mutation_service: self.mutation_service.clone(),
            sender_reports: self.sender_reports.clone(),
            membership_tx: self.membership_tx.clone(),
            cluster_secret: self.cluster_secret.clone(),
            #[cfg(test)]
            sender_snapshot_hook: None,
        }
    }
    pub(super) async fn start_dial_supervisor(
        &self,
        peer_addr: SocketAddr,
        expected_peer_id: Option<String>,
        initial_stream: Option<BoxStream>,
    ) -> bool {
        let Some(intent) = DialIntentGuard::reserve(
            self.dial_intents.clone(),
            self.dial_intent_changes.clone(),
            peer_addr,
        ) else {
            return true;
        };
        self.start_reserved_dial_supervisor(intent, expected_peer_id, initial_stream)
            .await
    }
    pub(super) async fn start_reserved_dial_supervisor(
        &self,
        intent: DialIntentGuard,
        expected_peer_id: Option<String>,
        initial_stream: Option<BoxStream>,
    ) -> bool {
        let peer_addr = intent.address;
        let ctx = self.connection_context();
        let network = self.network.clone();
        self.tasks
            .spawn(async move {
                let intent_guard = intent;
                intent_guard.activate();
                dial_supervisor(ctx, network, peer_addr, expected_peer_id, initial_stream).await;
                drop(intent_guard);
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::proto::ReconcileRequest;
    use crate::proto::SessionStart;
    use crate::replication::spawn_sender_with_options;

    #[tokio::test]
    async fn receive_byte_permit_follows_completed_frame_and_releases_on_eof() {
        let budget = Arc::new(Semaphore::new(8));
        let (_cancel, mut cancelled) = watch::channel(false);
        let (mut client, mut reader) = tokio::io::duplex(64);
        client.write_u32(4).await.unwrap();
        client.write_all(&[1, 2, 3, 4]).await.unwrap();
        let frame = read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(frame.bytes, [1, 2, 3, 4]);
        assert_eq!(budget.available_permits(), 4);
        drop(frame);
        assert_eq!(budget.available_permits(), 8);

        // Empty bodies consume no bytes, but still follow normal framing.
        client.write_u32(0).await.unwrap();
        let empty = read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(empty.bytes.is_empty());
        drop(empty);
        assert_eq!(budget.available_permits(), 8);

        client.write_u32(8).await.unwrap();
        client.write_all(&[1, 2]).await.unwrap();
        drop(client);
        assert!(read_connection_frame(
            &mut reader,
            &mut cancelled,
            &budget,
            true,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .is_none());
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn receive_byte_permit_releases_on_timeout_cancel_and_task_abort() {
        for failure in 0..3 {
            let budget = Arc::new(Semaphore::new(8));
            let (cancel, mut cancelled) = watch::channel(false);
            let (mut client, mut reader) = tokio::io::duplex(64);
            client.write_u32(8).await.unwrap();
            client.write_all(&[1]).await.unwrap();
            let read_budget = budget.clone();
            let read = tokio::spawn(async move {
                read_connection_frame(
                    &mut reader,
                    &mut cancelled,
                    &read_budget,
                    true,
                    Instant::now(),
                    if failure == 0 {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_secs(10)
                    },
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                while budget.available_permits() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("incomplete body did not reserve its bytes");
            match failure {
                0 => assert!(read.await.unwrap().is_none()),
                1 => {
                    cancel.send(true).unwrap();
                    assert!(read.await.unwrap().is_none());
                }
                _ => {
                    read.abort();
                    assert!(matches!(read.await, Err(error) if error.is_cancelled()));
                }
            }
            assert_eq!(budget.available_permits(), 8);
        }
    }

    #[tokio::test]
    async fn receive_byte_admission_checks_handshake_and_hard_limits_first() {
        for (established, len) in [(false, 4097), (true, MAX_FRAME_BYTES + 1)] {
            let budget = Arc::new(Semaphore::new(8));
            let (_cancel, mut cancelled) = watch::channel(false);
            let (mut client, mut reader) = tokio::io::duplex(64);
            client.write_u32(len as u32).await.unwrap();
            assert!(read_connection_frame(
                &mut reader,
                &mut cancelled,
                &budget,
                established,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .await
            .is_none());
            assert_eq!(budget.available_permits(), 8);
        }
    }

    #[tokio::test]
    async fn receive_byte_admission_is_shared_and_saturation_is_connection_local() {
        let capacity = 2 * MAX_FRAME_BYTES;
        let budget = Arc::new(Semaphore::new(capacity));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (mut ctx, runtime_tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers.clone(),
        )
        .await;
        ctx.receive_byte_budget = budget.clone();
        ctx.runtime_options = NodeRuntimeOptions::default()
            .with_receive_byte_capacity(capacity)
            .with_socket_read_timeout(Duration::from_secs(10));
        let mut connections = Vec::new();
        for (peer, we_dialed) in [
            ("held-accepted", false),
            ("held-dialed", true),
            ("healthy", false),
            ("shed", false),
        ] {
            let (mut client, server) = tokio::io::duplex(4096);
            let handler = tokio::spawn(handle_connection(
                ctx.clone(),
                Box::pin(server),
                we_dialed,
                we_dialed.then(|| peer.to_string()),
                None,
            ));
            complete_test_handshake(&mut client, peer).await;
            connections.push((client, handler));
        }
        let [
            (mut accepted, accepted_handler),
            (mut dialed, dialed_handler),
            (mut healthy, healthy_handler),
            (mut shed, shed_handler),
        ]: [_; 4] = connections.try_into().ok().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while peers.lock().await.len() != 4 || budget.available_permits() != capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handshake frames retained receive permits");

        // Accepted and outbound-dialed handlers each announce an incomplete
        // maximum frame. Their reservations must share the one node budget.
        accepted.write_u32(MAX_FRAME_BYTES as u32).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() != MAX_FRAME_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        dialed.write_u32(MAX_FRAME_BYTES as u32).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // No body follows: rejection must happen at admission, before
        // allocation/read/decode, without waiting for the held frames.
        shed.write_u32(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), shed_handler)
            .await
            .expect("receive saturation waited for body bytes")
            .unwrap();
        let mut byte = [0];
        assert_eq!(shed.read(&mut byte).await.unwrap(), 0);
        assert_eq!(budget.available_permits(), 0);
        assert!(peers.lock().await.contains_key("healthy"));
        assert!(!peers.lock().await.contains_key("shed"));

        drop(accepted);
        tokio::time::timeout(Duration::from_secs(1), accepted_handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), MAX_FRAME_BYTES);
        write_test_envelope(
            &mut healthy,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 71,
                    keys: vec!["probe".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut healthy).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse { req_id: 71, results }))
                if results.is_empty()
        ));

        let (mut replacement, server) = tokio::io::duplex(4096);
        let replacement_handler =
            tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut replacement, "held-accepted").await;
        write_test_envelope(
            &mut replacement,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 72,
                    keys: vec!["replacement-probe".to_string()],
                    priority: ProtoPriority::High as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut replacement).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse { req_id: 72, results }))
                if results.is_empty()
        ));

        drop(dialed);
        drop(healthy);
        drop(replacement);
        for handler in [dialed_handler, healthy_handler, replacement_handler] {
            tokio::time::timeout(Duration::from_secs(1), handler)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(budget.available_permits(), capacity);
        assert!(peers.lock().await.is_empty());
        runtime_tasks.shutdown().await;
    }

    #[test]
    fn challenge_hmac_binds_both_offers_roles_and_freshness() {
        let dialer = Hello {
            node_id: "dialer".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: 0x7f, // Fixed offer for the independent transcript vector.
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        let listener = Hello {
            node_id: "listener".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: 0x7f, // Fixed offer for the independent transcript vector.
            challenge: vec![2; HANDSHAKE_CHALLENGE_BYTES],
        };
        let authenticated = handshake_auth_tag(b"secret", true, &dialer, &listener);
        // Independently computed SHA-256/HMAC vectors pin the existing wire
        // transcript across crypto dependency upgrades (including proof roles).
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            hex(&authenticated),
            "c8481f4b79b30974a6593ea01a69abe9c06cf2612456898d08361d612e14ad1f"
        );
        assert_eq!(
            hex(&handshake_auth_tag(b"secret", false, &dialer, &listener)),
            "caebe45abdee3c01b2f91d9b572d014bf7179368e5ebe5a66dc3a8a1bd162b4c"
        );
        assert_eq!(
            hex(&connection_rank(&dialer, &listener).transcript_id),
            "51fb4b81d53d2ea3fec5121c3ac214123af25fab134cc4b50013b28cfbf39d5e"
        );
        assert!(verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &listener,
            &authenticated,
        ));
        assert!(!verify_handshake_auth(
            Some(b"different secret"),
            true,
            &dialer,
            &listener,
            &authenticated,
        ));
        for malformed in [&authenticated[..31], &[0; 33][..]] {
            assert!(!verify_handshake_auth(
                Some(b"secret"),
                true,
                &dialer,
                &listener,
                malformed,
            ));
        }
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            false,
            &dialer,
            &listener,
            &authenticated,
        ));
        let mut fresh_listener = listener.clone();
        fresh_listener.challenge[0] ^= 1;
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &fresh_listener,
            &authenticated,
        ));

        let mut tampered_dialer = dialer.clone();
        tampered_dialer.capabilities ^= CAPABILITY_MUTATION_RESULTS;
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &tampered_dialer,
            &listener,
            &authenticated,
        ));
        let reflected = handshake_auth_tag(b"secret", false, &listener, &dialer);
        assert!(!verify_handshake_auth(
            Some(b"secret"),
            true,
            &dialer,
            &listener,
            &reflected,
        ));
    }

    #[test]
    fn each_hello_uses_a_fresh_nonzero_os_challenge() {
        let first = fresh_hello("node").unwrap();
        let second = fresh_hello("node").unwrap();
        assert_eq!(first.challenge.len(), HANDSHAKE_CHALLENGE_BYTES);
        assert_eq!(second.challenge.len(), HANDSHAKE_CHALLENGE_BYTES);
        assert!(first.challenge.iter().any(|byte| *byte != 0));
        assert!(second.challenge.iter().any(|byte| *byte != 0));
        assert_ne!(first.challenge, second.challenge);
    }

    #[test]
    fn connection_rank_prefers_one_crossed_dial_and_totally_orders_duplicates() {
        let a = Hello {
            node_id: "a".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![1; HANDSHAKE_CHALLENGE_BYTES],
        };
        let b = Hello {
            node_id: "b".to_string(),
            auth_tag: Vec::new(),
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
            challenge: vec![2; HANDSHAKE_CHALLENGE_BYTES],
        };

        let canonical = connection_rank(&a, &b);
        let reverse = connection_rank(&b, &a);
        assert!(canonical.preferred_direction);
        assert!(!reverse.preferred_direction);
        assert!(canonical > reverse);

        let mut second_b = b.clone();
        second_b.challenge[0] ^= 1;
        assert_ne!(canonical, connection_rank(&a, &second_b));
    }

    #[tokio::test]
    async fn saturated_ack_and_nack_control_lanes_close_the_connection() {
        for nack in [false, true] {
            let fragment = FragmentId(0);
            let report = test_sender_report();
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            sender
                .send(SenderMsg::Ack {
                    session_id: 1,
                    batch_seq: 1,
                    durability: ReplicationAckDurability::MemoryApplied,
                })
                .unwrap();

            let senders = Arc::new(Mutex::new(HashMap::from([(
                (fragment, "peer".to_string()),
                sender.clone(),
            )])));
            let sender_reports = Arc::new(Mutex::new(HashMap::from([(
                (fragment, "peer".to_string()),
                report.clone(),
            )])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders,
                sender_reports,
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if peers.lock().await.contains_key("peer") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("peer Hello was not installed");

            let response = if nack {
                EnvelopeMsg::BatchNack(BatchNack {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    error: "retry".to_string(),
                })
            } else {
                EnvelopeMsg::BatchAck(BatchAck {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    level: ReplicationAckLevel::Applied as i32,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(response),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("saturated sender control lane did not close the connection")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            runtime_tasks.shutdown().await;

            drop(report_guard);
            sender.stop().await;
        }
    }

    #[tokio::test]
    async fn retiring_reconcile_consumer_does_not_close_shared_connection() {
        for data in [false, true] {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            // Keep the sender from consuming the reconcile control message so
            // the test can retire it at the exact admission boundary.
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports =
                Arc::new(Mutex::new(HashMap::from([(key.clone(), report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders.clone(),
                sender_reports,
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if peers.lock().await.contains_key("peer") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("peer Hello was not installed");

            let reconcile = if data {
                EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                    page_seq: 0,
                    done: true,
                })
            } else {
                EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while sender.control_capacity() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("reconcile frame was not admitted to the sender");

            let retired = senders.lock().await.remove(&key).unwrap();
            retired.request_stop();
            drop(report_guard);

            // A per-fragment consumer disappearing is a local placement race,
            // not grounds to discard the multiplexed connection. Exercise a
            // subsequent request to prove that both read and write halves are
            // still usable.
            let probe_req_id = 99;
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: probe_req_id,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            loop {
                if let Some(EnvelopeMsg::BatchGetResponse(response)) =
                    read_test_envelope(&mut client).await.msg
                {
                    assert_eq!(response.req_id, probe_req_id);
                    // This dynamic node does not serve the fragment, so an
                    // empty unavailable prefix is the safe reply. It still
                    // proves both halves of the shared connection survived.
                    assert!(response.results.is_empty());
                    break;
                }
            }
            assert!(peers.lock().await.contains_key("peer"));

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
            retired.stop().await;
        }
    }

    #[tokio::test]
    async fn saturated_retired_sender_does_not_close_shared_connection() {
        for frame_kind in 0..4 {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            let report_guard = report.lock().await;
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            sender
                .send(SenderMsg::Ack {
                    session_id: 1,
                    batch_seq: 1,
                    durability: ReplicationAckDurability::MemoryApplied,
                })
                .unwrap();

            let replacement_report = test_sender_report();
            let replacement = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: replacement_report.clone(),
                },
                SenderRuntimeOptions::default(),
            )
            .unwrap();
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports =
                Arc::new(Mutex::new(HashMap::from([(key.clone(), report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (mut ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders.clone(),
                sender_reports.clone(),
                peers.clone(),
            )
            .await;
            let captured = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            ctx.sender_snapshot_hook = Some(SenderSnapshotHook {
                captured: captured.clone(),
                resume: resume.clone(),
            });
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            let stale_control = match frame_kind {
                0 => EnvelopeMsg::BatchAck(BatchAck {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    level: ReplicationAckLevel::Applied as i32,
                }),
                1 => EnvelopeMsg::BatchNack(BatchNack {
                    session_id: 1,
                    batch_seq: 2,
                    fragment: fragment.0,
                    error: "retry".to_string(),
                }),
                2 => EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                    page_seq: 0,
                    done: true,
                }),
                3 => EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
                }),
                _ => unreachable!(),
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(stale_control),
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), captured.notified())
                .await
                .expect("connection did not snapshot the old sender");
            senders
                .lock()
                .await
                .insert(key.clone(), replacement.clone());
            sender_reports.lock().await.insert(key, replacement_report);
            resume.notify_one();

            let probe_req_id = 100;
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: probe_req_id,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            loop {
                if let Some(EnvelopeMsg::BatchGetResponse(response)) =
                    read_test_envelope(&mut client).await.msg
                {
                    assert_eq!(response.req_id, probe_req_id);
                    break;
                }
            }
            assert!(peers.lock().await.contains_key("peer"));

            drop(report_guard);
            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
            sender.stop().await;
            replacement.stop().await;
        }
    }

    #[tokio::test]
    async fn current_failed_reconcile_consumer_closes_for_replacement() {
        for (data, fail_after_admission) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let fragment = FragmentId(0);
            let key = (fragment, "peer".to_string());
            let report = test_sender_report();
            let mut report_guard = Some(report.lock().await);
            let sender = spawn_node_sender(
                fragment,
                "peer".to_string(),
                SenderConfig::default(),
                SenderDeps {
                    router: FragmentRouter::new(1, 0),
                    hlc: Arc::new(Mutex::new(HlcClock::new())),
                    state_report: report.clone(),
                },
                SenderRuntimeOptions::default().with_control_channel_capacity(1),
            )
            .unwrap();
            if !fail_after_admission {
                drop(report_guard.take());
                sender.stop().await;
            }
            let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
            let sender_reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                senders,
                sender_reports,
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            let reconcile = if data {
                EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: fragment.0,
                    entries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 1,
                    page_seq: 0,
                    done: true,
                })
            } else {
                EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: fragment.0,
                    summaries: Vec::new(),
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    next_after_key: String::new(),
                    snapshot_revision: 1,
                    snapshot_time_ms: 1,
                    done: true,
                    restart: false,
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;
            if fail_after_admission {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while sender.control_capacity() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("reconcile frame was not admitted to the current sender");
                sender.request_stop();
                drop(report_guard.take());
            }

            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("failed current reconcile consumer did not close the connection")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            runtime_tasks.shutdown().await;
            sender.stop().await;
        }
    }

    #[tokio::test]
    async fn non_serving_reconcile_races_do_not_close_shared_connection() {
        for fetch in [false, true] {
            let fragment = FragmentId(0);
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (ctx, runtime_tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::from([(
                    fragment,
                    vec!["local".to_string(), "peer".to_string()],
                )]))),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                peers.clone(),
            )
            .await;
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;

            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                        session_id: 1,
                        fragment: fragment.0,
                    })),
                },
            )
            .await;
            let reconcile = if fetch {
                EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                    fragment: fragment.0,
                    keys: vec!["connection-probe".to_string()],
                    session_id: 1,
                    round_id: 1,
                    chunk_seq: 0,
                })
            } else {
                EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                    fragment: fragment.0,
                    session_id: 1,
                    round_id: 1,
                    page_seq: 0,
                    after_key: String::new(),
                    snapshot_revision: 0,
                    snapshot_time_ms: 0,

                    recovery_epoch: Vec::new(),
                    recovery_replica_digest: Vec::new(),
                })
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(reconcile),
                },
            )
            .await;

            // A request for another multiplexed protocol proves that the
            // honest placement race did not sacrifice either connection half.
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: 99,
                        keys: vec!["connection-probe".to_string()],
                        priority: ProtoPriority::Default as i32,
                    })),
                },
            )
            .await;
            assert!(matches!(
                read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                    req_id: 99,
                    results,
                })) if results.is_empty()
            ));
            assert!(peers.lock().await.contains_key("peer"));

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("connection did not stop after its stream closed")
                .unwrap();
            runtime_tasks.shutdown().await;
        }
    }

    #[tokio::test]
    async fn blocked_serving_reads_do_not_stall_pending_replies_on_same_connection() {
        for batch in [false, true] {
            let fragment = FragmentId(0);
            let router = FragmentRouter::new(1, 0);
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let (actor, _) = ractor::Actor::spawn(
                None,
                BlockingPageActor,
                BlockingPageState {
                    started: started.clone(),
                    release: release.clone(),
                },
            )
            .await
            .unwrap();
            router.insert(fragment, actor.clone());
            let (mut ctx, runtime_tasks) = test_connection_context(
                router,
                Arc::new(RwLock::new(HashSet::from([fragment]))),
                Arc::new(RwLock::new(HashMap::new())),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
            )
            .await;
            ctx.runtime_options = NodeRuntimeOptions::default()
                .with_forwarded_attempt_timeout(Duration::from_secs(10));
            let receive_byte_budget = ctx.receive_byte_budget.clone();
            let (reply, completed) = oneshot::channel();
            ctx.pending_gets.lock().unwrap().insert(
                99,
                PendingGet {
                    expected_peer: "peer".into(),
                    expected_key: "reply-key".into(),
                    reply,
                },
            );
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "peer").await;
            let request = if batch {
                EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 1,
                    keys: vec!["held".into()],
                    priority: ProtoPriority::Background as i32,
                })
            } else {
                EnvelopeMsg::ForwardedGet(ForwardedGet {
                    req_id: 1,
                    key: "held".into(),
                    priority: ProtoPriority::Background as i32,
                    hops: 0,
                })
            };
            write_test_envelope(&mut client, Envelope { msg: Some(request) }).await;
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .unwrap();
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                        req_id: 99,
                        ..GetResponse::default()
                    })),
                },
            )
            .await;
            assert!(tokio::time::timeout(Duration::from_secs(1), completed)
                .await
                .expect("serving read blocked a pending reply on its physical connection")
                .unwrap()
                .unwrap()
                .is_none());
            tokio::time::timeout(Duration::from_secs(1), async {
                while receive_byte_budget.available_permits() != RECEIVE_BYTE_CAPACITY {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("decoded read job retained the raw receive body permit");
            // Cancel while the actor still owns the request, rather than
            // relying on its ten-second deadline to release the socket reader.
            connection.abort();
            assert!(connection.await.unwrap_err().is_cancelled());
            release.notify_one();
            actor.stop_and_wait(None, None).await.unwrap();
            runtime_tasks.shutdown().await;
        }
    }

    #[tokio::test]
    async fn dynamic_connection_handler_does_not_create_placement_senders() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) = spawn_fragment_subtree(
            "dynamic-sender-ownership-test",
            fragment,
            &NodeConfig::default(),
            &hlc,
        )
        .await
        .unwrap();
        router.insert(fragment, dispatcher);

        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["local".to_string(), "peer".to_string()],
        )])));
        let (mut ctx, runtime_tasks) = test_connection_context(
            router.clone(),
            Arc::new(RwLock::new(HashSet::from([fragment]))),
            placement,
            true,
            senders.clone(),
            reports.clone(),
            peers.clone(),
        )
        .await;
        ctx.node_id = "local".to_string();

        let (mut client, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
        complete_test_handshake(&mut client, "peer").await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !peers.lock().await.contains_key("peer") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dynamic peer was not authenticated");

        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("dynamic connection did not finish")
            .unwrap();
        runtime_tasks.shutdown().await;
        router.remove(fragment);
        supervisor.stop(None);
        let _ = supervisor.wait(None).await;
    }

    #[tokio::test]
    async fn legacy_disconnected_peer_identities_reap_their_senders() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) = spawn_fragment_subtree(
            "legacy-sender-reap-test",
            fragment,
            &NodeConfig::default(),
            &hlc,
        )
        .await
        .unwrap();
        router.insert(fragment, dispatcher);

        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (ctx, runtime_tasks) = test_connection_context(
            router.clone(),
            Arc::new(RwLock::new(HashSet::from([fragment]))),
            Arc::new(RwLock::new(HashMap::new())),
            false,
            senders.clone(),
            reports.clone(),
            peers.clone(),
        )
        .await;

        for index in 0..3 {
            let peer = format!("ephemeral-peer-{index}");
            let (mut client, server) = tokio::io::duplex(16 * 1024);
            let connection = tokio::spawn(handle_connection(
                ctx.clone(),
                Box::pin(server),
                false,
                None,
                None,
            ));
            complete_test_handshake(&mut client, &peer).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while !senders.lock().await.contains_key(&(fragment, peer.clone())) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("legacy sender was not created");

            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .expect("legacy connection did not finish")
                .unwrap();
            assert!(peers.lock().await.is_empty());
            assert!(senders.lock().await.is_empty());
            assert!(reports.lock().await.is_empty());
        }

        runtime_tasks.shutdown().await;
        router.remove(fragment);
        supervisor.stop(None);
        let _ = supervisor.wait(None).await;
    }

    #[tokio::test]
    async fn direct_and_relayed_get_responses_are_pinned_to_the_expected_peer() {
        let pending_gets: PendingGets = Arc::new(StdMutex::new(HashMap::new()));
        let relays: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
        let (mut ctx, runtime_tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            peers,
        )
        .await;
        ctx.pending_gets = pending_gets.clone();
        ctx.pending_relays = relays.clone();
        let (mut owner, owner_server) = tokio::io::duplex(4096);
        let owner_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(owner_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        let expected_tx = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(tx) = ctx.peers.lock().await.get("owner").cloned() {
                    break tx;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (pending_reply, pending_response) = oneshot::channel();
        pending_gets.lock().unwrap().insert(
            31,
            PendingGet {
                expected_peer: "owner".to_string(),
                expected_key: "key".to_string(),
                reply: pending_reply,
            },
        );
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (timeout_cancel, _timeout_cancelled) = oneshot::channel();
        relays.lock().unwrap().insert(
            32,
            GetRelay {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_key: "key".to_string(),
                origin_tx,
                origin_req_id: 9,
                timeout_cancel,
                _admission: ctx
                    .relay_admission
                    .try_reserve(relay_key_storage_bytes(3))
                    .unwrap(),
            },
        );

        let (mut attacker, attacker_server) = tokio::io::duplex(4096);
        let attacker_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(attacker_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut attacker, "attacker").await;
        let not_found_response = |req_id| Envelope {
            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id,
                found: false,
                value: Vec::new(),
                timestamp_ms: 0,
                logical_counter: 0,
                node_id: String::new(),
                expires_at_ms: 0,
                is_tombstone: false,
                shed: false,
            })),
        };
        for req_id in [31, 32] {
            write_test_envelope(&mut attacker, not_found_response(req_id)).await;
        }
        write_test_envelope(
            &mut attacker,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 33,
                    keys: vec!["barrier".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut attacker).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 33,
                results,
            })) if results.is_empty()
        ));
        assert!(pending_gets.lock().unwrap().contains_key(&31));
        assert!(relays.lock().unwrap().contains_key(&32));
        assert!(origin_response_rx.try_recv().is_err());

        for req_id in [31, 32] {
            write_test_envelope(&mut owner, not_found_response(req_id)).await;
        }
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 34,
                    keys: vec!["barrier".to_string()],
                    priority: ProtoPriority::Default as i32,
                })),
            },
        )
        .await;
        assert!(matches!(
            read_test_envelope(&mut owner).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: 34,
                results,
            })) if results.is_empty()
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), pending_response)
                .await
                .expect("pending read response timed out")
                .unwrap()
                .is_ok()
        );
        let relayed = tokio::time::timeout(Duration::from_secs(1), origin_response_rx.recv())
            .await
            .expect("relay response timed out")
            .expect("relay response missing");
        assert!(matches!(
            Envelope::decode(relayed.bytes()).unwrap().msg,
            Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id: 9,
                found: false,
                ..
            }))
        ));
        assert!(pending_gets.lock().unwrap().is_empty());
        assert!(relays.lock().unwrap().is_empty());

        drop(attacker);
        drop(owner);
        tokio::time::timeout(Duration::from_secs(1), attacker_connection)
            .await
            .expect("attacker connection did not stop")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), owner_connection)
            .await
            .expect("owner connection did not stop")
            .unwrap();
        runtime_tasks.shutdown().await;
    }

    #[tokio::test]
    async fn full_peer_queue_times_out_cancels_and_does_not_admit_frame() {
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
        let mut cancelled = peer.cancel.subscribe();
        let envelope = || Envelope { msg: None };
        assert!(try_send_envelope(&peer, envelope()).await);
        assert!(!try_send_envelope(&peer, envelope()).await);
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("connection was not cancelled")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reader_response_saturation_cancels_without_waiting() {
        let (peer, _rx, mut response_rx) = test_peer_channel(1);
        let envelope = || Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: 1,
                batch_seq: 1,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        };
        assert!(try_send_envelope_immediate(&peer, envelope()));
        let mut cancelled = peer.cancel.subscribe();
        assert!(!try_send_envelope_immediate(&peer, envelope()));
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("saturated reader response did not cancel the connection")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
        assert!(response_rx.recv().await.is_some());
        assert!(response_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_producer_share_still_admits_a_near_limit_response() {
        let (peer, _rx, mut response_rx) = test_peer_channel(1);
        let _producer_permit = peer
            .outbound
            .producer_byte_budget
            .clone()
            .acquire_many_owned(MAX_FRAME_BYTES as u32)
            .await
            .unwrap();
        let _shared_producer_permit = peer
            .outbound
            .byte_budget
            .clone()
            .acquire_many_owned(MAX_FRAME_BYTES as u32)
            .await
            .unwrap();
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id: u64::MAX,
                results: vec![BatchKeyValue {
                    key: "large".to_string(),
                    found: true,
                    value: vec![7; MAX_FRAME_BYTES - 128],
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "node".to_string(),
                    expires_at_ms: u64::MAX,
                    is_tombstone: false,
                    shed: false,
                }],
            })),
        };
        assert!(envelope.encoded_len() > MAX_FRAME_BYTES - 256);
        assert!(envelope.encoded_len() <= MAX_FRAME_BYTES);
        assert!(try_send_envelope_immediate(&peer, envelope));
        assert!(response_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn oversized_outbound_frame_is_rejected_before_queue_admission() {
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec!["x".repeat(MAX_FRAME_BYTES)],
                priority: ProtoPriority::Default as i32,
            })),
        };

        assert!(!try_send_envelope(&peer, envelope).await);
        assert!(rx.try_recv().is_err());
        assert!(try_send_envelope(&peer, Envelope { msg: None }).await);
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn forwarded_attempt_deadline_caps_queue_admission_wait() {
        let (mut peer, mut rx, _response_rx) = test_peer_channel(1);
        peer.queue_admission_timeout = Duration::from_secs(1);
        assert!(try_send_envelope(&peer, Envelope { msg: None }).await);
        let started = Instant::now();
        assert!(
            !try_send_envelope_until(
                &peer,
                Envelope { msg: None },
                started + Duration::from_millis(10),
            )
            .await
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_forwarded_attempt_never_admits_to_an_empty_queue() {
        let (peer, mut rx, _response_rx) = test_peer_channel(1);
        let expired = Instant::now();
        tokio::task::yield_now().await;
        assert!(
            !try_send_envelope_until(&peer, Envelope { msg: None }, expired).await,
            "an expired total attempt budget must fail before reserving capacity"
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn closed_peer_queue_fails_and_cancels_connection() {
        let (peer, rx, _response_rx) = test_peer_channel(1);
        let mut cancelled = peer.cancel.subscribe();
        drop(rx);
        assert!(!try_send_envelope(&peer, Envelope { msg: None }).await);
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("connection was not cancelled")
            .expect("cancellation sender dropped");
        assert!(*cancelled.borrow());
    }

    #[tokio::test]
    async fn stale_peer_failure_cannot_overwrite_current_channel_failure() {
        let (current, _current_rx, _current_response_rx) = test_peer_channel(1);
        let (stale, _stale_rx, _stale_response_rx) = test_peer_channel(1);
        let mut cancelled = current.cancel.subscribe();
        let peers = Arc::new(Mutex::new(HashMap::from([(
            "peer".to_string(),
            current.clone(),
        )])));
        let failures = PeerFailureSignals::default();

        // Queue both before the worker starts so the stale signal is
        // guaranteed to arrive last. A single-value map used to lose the
        // current-channel failure here and leave replication disconnected.
        failures.signal("peer".to_string(), current.raw_sender());
        failures.signal("peer".to_string(), stale.raw_sender());
        let worker = tokio::spawn(failures.run(peers));

        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("current connection failure was overwritten")
            .expect("current cancellation sender dropped");
        assert!(*cancelled.borrow());
        worker.abort();
        let _ = worker.await;
    }

    #[tokio::test]
    async fn peer_writer_prioritizes_reader_responses_over_queued_data() {
        let (mut reader, writer) = tokio::io::duplex(64);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(2));
        let producer_byte_budget = Arc::new(Semaphore::new(1));

        let producer_permit = producer_byte_budget.clone().acquire_owned().await.unwrap();
        let data_permit = byte_budget.clone().acquire_owned().await.unwrap();
        frame_tx
            .send(BudgetedFrame::new_producer(
                vec![1],
                data_permit,
                producer_permit,
            ))
            .await
            .unwrap();
        let response_permit = byte_budget.clone().acquire_owned().await.unwrap();
        response_tx
            .send(BudgetedFrame::new_response(vec![2], response_permit))
            .await
            .unwrap();
        drop(frame_tx);
        drop(response_tx);

        let task = tokio::spawn(run_bounded_peer_writer(
            writer,
            PeerWriterPipeline {
                data_rx: frame_rx,
                response_rx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
            },
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(1),
        ));
        for expected in [2, 1] {
            assert_eq!(reader.read_u32().await.unwrap(), 1);
            assert_eq!(reader.read_u8().await.unwrap(), expected);
        }
        task.await.unwrap();
        assert!(byte_budget.is_closed());
        assert!(producer_byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 2);
        assert_eq!(producer_byte_budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn peer_writer_bounds_response_bursts_to_preserve_data_progress() {
        let response_count = PEER_RESPONSE_BURST_LIMIT + 1;
        let (mut reader, writer) = tokio::io::duplex(256);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(response_count);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(response_count + 1));
        let producer_byte_budget = Arc::new(Semaphore::new(1));

        let producer_permit = producer_byte_budget.clone().acquire_owned().await.unwrap();
        let data_permit = byte_budget.clone().acquire_owned().await.unwrap();
        frame_tx
            .send(BudgetedFrame::new_producer(
                vec![1],
                data_permit,
                producer_permit,
            ))
            .await
            .unwrap();
        for _ in 0..response_count {
            let permit = byte_budget.clone().acquire_owned().await.unwrap();
            response_tx
                .send(BudgetedFrame::new_response(vec![2], permit))
                .await
                .unwrap();
        }
        drop(frame_tx);
        drop(response_tx);

        let task = tokio::spawn(run_bounded_peer_writer(
            writer,
            PeerWriterPipeline {
                data_rx: frame_rx,
                response_rx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
            },
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(1),
        ));
        for _ in 0..PEER_RESPONSE_BURST_LIMIT {
            assert_eq!(reader.read_u32().await.unwrap(), 1);
            assert_eq!(reader.read_u8().await.unwrap(), 2);
        }
        assert_eq!(reader.read_u32().await.unwrap(), 1);
        assert_eq!(reader.read_u8().await.unwrap(), 1);
        task.await.unwrap();
        assert_eq!(byte_budget.available_permits(), response_count + 1);
        assert_eq!(producer_byte_budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn socket_write_timeout_signals_connection_cancellation() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (cancel_tx, mut cancelled) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(1024));
        frame_tx
            .send(test_budgeted_frame(vec![0; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_millis(10),
        ));
        tokio::time::timeout(Duration::from_secs(1), cancelled.changed())
            .await
            .expect("writer did not signal cancellation")
            .expect("writer cancellation sender dropped");
        assert!(*cancelled.borrow());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn connection_cancellation_interrupts_a_blocked_socket_write() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(1024));
        frame_tx
            .send(test_budgeted_frame(vec![0; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
            cancel_tx.subscribe(),
            cancel_tx.clone(),
            Duration::from_secs(30),
        ));
        tokio::task::yield_now().await;
        let _ = cancel_tx.send(true);

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled writer waited for the socket write timeout")
            .unwrap();
        assert!(byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 1024);
    }

    #[tokio::test]
    async fn aborting_peer_writer_releases_current_and_queued_byte_permits() {
        let (_reader, writer) = tokio::io::duplex(1);
        let (frame_tx, frame_rx) = mpsc::channel(2);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(2048));
        frame_tx
            .send(test_budgeted_frame(vec![1; 1024], &byte_budget).await)
            .await
            .unwrap();
        frame_tx
            .send(test_budgeted_frame(vec![2; 1024], &byte_budget).await)
            .await
            .unwrap();
        let task = tokio::spawn(run_peer_writer(
            writer,
            frame_rx,
            byte_budget.clone(),
            cancel_tx.subscribe(),
            cancel_tx,
            Duration::from_secs(30),
        ));
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(byte_budget.is_closed());
        assert_eq!(byte_budget.available_permits(), 2048);
    }

    #[tokio::test]
    async fn peer_byte_budget_backpressures_producers_before_frame_allocation() {
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec!["bounded".repeat(32)],
                priority: ProtoPriority::Default as i32,
            })),
        };
        let encoded_len = envelope.encoded_len();
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let (response_tx, _response_rx) = mpsc::channel(8);
        let (identity, _) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(encoded_len * 3));
        let producer_byte_budget = Arc::new(Semaphore::new(encoded_len * 2));
        let peer = PeerChannel {
            outbound: BudgetedPeerTx {
                node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                tx: frame_tx,
                response_tx,
                byte_budget,
                producer_byte_budget,
                reconcile_gate: ReconcileGate::new(),
                identity,
            },
            cancel: cancel_tx,
            queue_admission_timeout: Duration::from_millis(10),
            rank: None,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
        };

        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert!(!try_send_envelope(&peer, envelope).await);
        assert_eq!(frame_rx.len(), 2);
        assert_eq!(peer.outbound.tx.capacity(), 6);
    }

    #[tokio::test]
    async fn reader_responses_use_reserved_byte_capacity() {
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: 1,
                batch_seq: 1,
                fragment: 0,
                level: ReplicationAckLevel::Applied as i32,
            })),
        };
        let encoded_len = envelope.encoded_len();
        let (frame_tx, mut frame_rx) = mpsc::channel(1);
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let (identity, _) = mpsc::channel(1);
        let (cancel_tx, _) = watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(encoded_len * 2));
        let producer_byte_budget = Arc::new(Semaphore::new(encoded_len));
        let peer = PeerChannel {
            outbound: BudgetedPeerTx {
                node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                tx: frame_tx,
                response_tx,
                byte_budget: byte_budget.clone(),
                producer_byte_budget: producer_byte_budget.clone(),
                reconcile_gate: ReconcileGate::new(),
                identity,
            },
            cancel: cancel_tx,
            queue_admission_timeout: Duration::from_millis(10),
            rank: None,
            capabilities: REQUIRED_WIRE_CAPABILITIES,
        };

        assert!(try_send_envelope(&peer, envelope.clone()).await);
        assert_eq!(producer_byte_budget.available_permits(), 0);
        assert_eq!(byte_budget.available_permits(), encoded_len);
        assert!(try_send_envelope_immediate(&peer, envelope));
        assert_eq!(byte_budget.available_permits(), 0);
        drop(response_rx.recv().await.unwrap());
        assert_eq!(byte_budget.available_permits(), encoded_len);
        assert_eq!(producer_byte_budget.available_permits(), 0);
        drop(frame_rx.recv().await.unwrap());
        assert_eq!(byte_budget.available_permits(), encoded_len * 2);
        assert_eq!(producer_byte_budget.available_permits(), encoded_len);
    }

    #[tokio::test]
    async fn stale_peer_disconnect_does_not_clear_replacement_sender_channel() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = crate::replication::spawn_sender(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
        );
        let (old_tx, mut old_rx) = mpsc::channel(4);
        let (new_tx, mut new_rx) = mpsc::channel(4);

        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_tx.clone(),
            })
            .unwrap();
        let old_session = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected old SessionStart, got {other:?}"),
        };
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: new_tx.clone(),
            })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(old_session, new_session);

        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "connected" && state.session_id == new_session
        })
        .await;

        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: new_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;
    }

    #[tokio::test]
    async fn reset_sender_retries_reconcile_after_live_queue_admission_timeout() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                buffer_capacity: 1,
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default().with_queue_admission_timeout(Duration::from_millis(20)),
        )
        .expect("sender options are valid");
        let op = |key: &str| ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 0,
            boot_id: 0,
        };

        sender.send(SenderMsg::Enqueue(op("first"))).unwrap();
        sender.send(SenderMsg::Enqueue(op("overflow"))).unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "buffer_reset" && state.session_id == 0
        })
        .await;

        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();

        // The SessionStart fills the only queue slot. The sender publishes
        // this session only after the immediate ReconcileRequest admission
        // has timed out against the still-live, full channel.
        common_wait_for_sender_report(&report, |state| {
            state.state == "buffer_reset" && state.session_id != 0
        })
        .await;
        let session_id = report.lock().await.session_id;
        let session_frame = peer_rx.recv().await.expect("SessionStart frame missing");
        match Envelope::decode(session_frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => {
                assert_eq!(session.session_id, session_id);
                assert_eq!(session.fragment, 0);
            }
            other => panic!("expected SessionStart, got {other:?}"),
        }
        assert!(peer_rx.try_recv().is_err());

        assert!(
            tokio::time::timeout(Duration::from_millis(500), peer_rx.recv())
                .await
                .is_err(),
            "reconcile retried before the retry floor"
        );
        let reconcile_frame = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("sender did not retry reconciliation")
            .expect("peer channel closed");
        match Envelope::decode(reconcile_frame.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, session_id);
                assert_ne!(request.round_id, 0);
                assert_eq!(request.fragment, 0);
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn routine_reconcile_disconnect_drains_buffer_before_retry() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_tx.clone(),
            })
            .unwrap();
        let old_session = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };

        sender.send(SenderMsg::ReconcileNow).unwrap();
        let old_round = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, old_session);
                request.round_id
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        };

        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "buffered-during-reconcile".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 1, "node".to_string()),
                seq: 7,
                boot_id: 11,
            }))
            .unwrap();
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "disconnected" && state.buffered == 1
        })
        .await;

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let batch = tokio::time::timeout(Duration::from_secs(1), new_rx.recv())
            .await
            .expect("buffered mutation was not replayed")
            .expect("replacement peer channel closed");
        let batch = match Envelope::decode(batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(batch.session_id, new_session);
        assert_eq!(batch.ops.len(), 1);
        assert_eq!(batch.ops[0].key, "buffered-during-reconcile");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), new_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the ordinary batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: new_session,
                batch_seq: batch.batch_seq,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("routine reconciliation was not retried after the batch ACK")
            .expect("replacement peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, 0);
                assert_ne!(request.round_id, old_round);
            }
            other => panic!("expected ReconcileRequest after batch ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_requeued_batch_on_reconnect() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_tx.clone(),
            })
            .unwrap();
        let old_session = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };

        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "requeued-for-reconnect".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 17, "node".to_string()),
                seq: 23,
                boot_id: 29,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), old_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("old peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, old_session);
        assert_eq!(first_batch.ops.len(), 1);

        // ReconcileNow observes the in-flight batch and enters the fresh-drain
        // pause. Disconnecting before its ACK must turn that batch back into
        // replay work, rather than preserving a pause that skips the replay.
        sender.send(SenderMsg::ReconcileNow).unwrap();
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| {
            state.state == "disconnected" && state.buffered == 1 && state.in_flight == 0
        })
        .await;
        assert!(old_rx.try_recv().is_err());

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let replay = tokio::time::timeout(Duration::from_secs(1), new_rx.recv())
            .await
            .expect("requeued batch was not replayed")
            .expect("replacement peer channel closed");
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(replay.session_id, new_session);
        assert_eq!(replay.fragment, first_batch.fragment);
        assert_eq!(replay.required_ack_level, first_batch.required_ack_level);
        assert_eq!(replay.ops, first_batch.ops);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), new_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the replayed batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: new_session,
                batch_seq: replay.batch_seq,
                durability: ReplicationAckDurability::MemoryApplied,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("routine reconciliation was not resumed after replay ACK")
            .expect("replacement peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, 0);
            }
            other => panic!("expected ReconcileRequest after replay ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_nacked_batch() {
        assert_fresh_pause_replays_before_reconcile(FreshPauseRequeue::Nack).await;
    }

    #[tokio::test]
    async fn fresh_reconcile_pause_does_not_skip_ack_timed_out_batch() {
        assert_fresh_pause_replays_before_reconcile(FreshPauseRequeue::AckTimeout).await;
    }

    #[tokio::test]
    async fn reconnect_preserves_ordinary_nack_retry_floor() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (old_tx, mut old_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: old_tx.clone(),
            })
            .unwrap();
        let old_session = match Envelope::decode(old_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };
        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: "nack-floor".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 43, "node".to_string()),
                seq: 47,
                boot_id: 53,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), old_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("old peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, old_session);

        let nack_sent_at = Instant::now();
        sender
            .send(SenderMsg::Nack {
                session_id: old_session,
                batch_seq: first_batch.batch_seq,
                error: "injected rejection".to_string(),
            })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.buffered == 1 && state.in_flight == 0)
            .await;
        sender
            .send(SenderMsg::PeerDisconnected { peer_tx: old_tx })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;

        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, old_session);

        let early_deadline = nack_sent_at + Duration::from_millis(800);
        if Instant::now() < early_deadline {
            assert!(
                tokio::time::timeout_at(early_deadline, new_rx.recv())
                    .await
                    .is_err(),
                "reconnect bypassed the NACK retry floor"
            );
        }
        let replay = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("ordinary batch was not replayed after the retry floor")
            .expect("replacement peer channel closed");
        assert!(nack_sent_at.elapsed() >= Duration::from_millis(900));
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected replayed ReplicateBatch, got {other:?}"),
        };
        assert_eq!(replay.session_id, new_session);
        assert_eq!(replay.ops, first_batch.ops);
    }

    #[tokio::test]
    async fn reconnect_preserves_reconcile_timeout_retry_floor() {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_secs(3600),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_millis(50),
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .expect("sender options are valid");

        let (timed_out_tx, mut timed_out_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected {
                peer_tx: timed_out_tx.clone(),
            })
            .unwrap();
        let initial_session = match Envelope::decode(timed_out_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };
        // Measure from before admission, not a possibly delayed receipt of
        // SessionStart after the sender has already begun its retry floor.
        let reconcile_started_at = Instant::now();
        sender.send(SenderMsg::ReconcileNow).unwrap();
        let initial_round = match Envelope::decode(timed_out_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, initial_session);
                request.round_id
            }
            other => panic!("expected ReconcileRequest, got {other:?}"),
        };

        let rotated = tokio::time::timeout(Duration::from_secs(1), timed_out_rx.recv())
            .await
            .expect("reconcile timeout did not rotate the session")
            .expect("peer channel closed");
        let rotated_session = match Envelope::decode(rotated.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected rotated SessionStart, got {other:?}"),
        };
        assert_ne!(rotated_session, initial_session);

        sender
            .send(SenderMsg::PeerDisconnected {
                peer_tx: timed_out_tx,
            })
            .unwrap();
        common_wait_for_sender_report(&report, |state| state.state == "disconnected").await;
        let (new_tx, mut new_rx) = mpsc::channel(4);
        sender
            .send(SenderMsg::PeerConnected { peer_tx: new_tx })
            .unwrap();
        let new_session = match Envelope::decode(new_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected replacement SessionStart, got {other:?}"),
        };
        assert_ne!(new_session, rotated_session);

        let early_deadline = reconcile_started_at + Duration::from_millis(800);
        if Instant::now() < early_deadline {
            assert!(
                tokio::time::timeout_at(early_deadline, new_rx.recv())
                    .await
                    .is_err(),
                "reconnect bypassed the reconcile retry floor"
            );
        }
        let retry = tokio::time::timeout(Duration::from_secs(2), new_rx.recv())
            .await
            .expect("reconciliation was not retried after the retry floor")
            .expect("replacement peer channel closed");
        assert!(reconcile_started_at.elapsed() >= Duration::from_millis(900));
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, new_session);
                assert_ne!(request.round_id, initial_round);
            }
            other => panic!("expected delayed ReconcileRequest, got {other:?}"),
        }
    }

    async fn test_budgeted_frame(bytes: Vec<u8>, budget: &Arc<Semaphore>) -> BudgetedFrame {
        let permit = budget
            .clone()
            .acquire_many_owned(bytes.len() as u32)
            .await
            .unwrap();
        BudgetedFrame::new_response(bytes, permit)
    }

    #[derive(Clone, Copy)]
    enum FreshPauseRequeue {
        Nack,
        AckTimeout,
    }

    async fn assert_fresh_pause_replays_before_reconcile(trigger: FreshPauseRequeue) {
        let router = FragmentRouter::new(1, 0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let required_ack_durability = match trigger {
            FreshPauseRequeue::Nack => ReplicationAckDurability::MemoryApplied,
            FreshPauseRequeue::AckTimeout => ReplicationAckDurability::StoreCommitted,
        };
        let runtime = match trigger {
            FreshPauseRequeue::Nack => SenderRuntimeOptions::default(),
            // The weak ACK below is an observable FIFO barrier proving that
            // ReconcileNow was handled before this deliberately short expiry.
            FreshPauseRequeue::AckTimeout => {
                SenderRuntimeOptions::default().with_ack_timeout(Duration::from_secs(1))
            }
        };
        let sender = spawn_sender_with_options(
            FragmentId(0),
            "peer".to_string(),
            SenderConfig {
                batch_interval: Duration::from_millis(10),
                consistency_interval: Duration::from_secs(3600),
                reconcile_timeout: Duration::from_secs(5),
                required_ack_durability,
                ..SenderConfig::default()
            },
            SenderDeps {
                router,
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            runtime,
        )
        .expect("sender options are valid");

        let (peer_tx, mut peer_rx) = mpsc::channel(4);
        sender.send(SenderMsg::PeerConnected { peer_tx }).unwrap();
        let first_session = match Envelope::decode(peer_rx.recv().await.unwrap().as_slice())
            .unwrap()
            .msg
        {
            Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
            other => panic!("expected SessionStart, got {other:?}"),
        };

        let key = match trigger {
            FreshPauseRequeue::Nack => "requeued-after-nack",
            FreshPauseRequeue::AckTimeout => "requeued-after-ack-timeout",
        };
        sender
            .send(SenderMsg::Enqueue(ReplicatedOp {
                key: key.to_string(),
                value: VersionedValue::new(b"value".to_vec(), 31, "node".to_string()),
                seq: 37,
                boot_id: 41,
            }))
            .unwrap();
        let first_batch = tokio::time::timeout(Duration::from_secs(1), peer_rx.recv())
            .await
            .expect("ordinary batch was not sent")
            .expect("peer channel closed");
        let first_batch = match Envelope::decode(first_batch.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch, got {other:?}"),
        };
        assert_eq!(first_batch.session_id, first_session);
        assert_eq!(first_batch.ops.len(), 1);

        sender.send(SenderMsg::ReconcileNow).unwrap();
        let replay_session = match trigger {
            FreshPauseRequeue::Nack => {
                sender
                    .send(SenderMsg::Nack {
                        session_id: first_session,
                        batch_seq: first_batch.batch_seq,
                        error: "injected rejection".to_string(),
                    })
                    .unwrap();
                common_wait_for_sender_report(&report, |state| {
                    state.buffered == 1 && state.in_flight == 0
                })
                .await;
                first_session
            }
            FreshPauseRequeue::AckTimeout => {
                sender
                    .send(SenderMsg::Ack {
                        session_id: first_session,
                        batch_seq: first_batch.batch_seq,
                        durability: ReplicationAckDurability::MemoryApplied,
                    })
                    .unwrap();
                common_wait_for_sender_report(&report, |state| {
                    state.in_flight == 1 && state.ack_durability_mismatch
                })
                .await;
                let session_frame = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
                    .await
                    .expect("ACK timeout did not rotate the session")
                    .expect("peer channel closed");
                let session_id = match Envelope::decode(session_frame.as_slice()).unwrap().msg {
                    Some(EnvelopeMsg::SessionStart(session)) => session.session_id,
                    other => panic!("expected replacement SessionStart, got {other:?}"),
                };
                assert_ne!(session_id, first_session);
                session_id
            }
        };

        let replay = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("ordinary batch was not replayed")
            .expect("peer channel closed");
        let replay = match Envelope::decode(replay.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReplicateBatch(batch)) => batch,
            other => panic!("expected ReplicateBatch before reconcile retry, got {other:?}"),
        };
        assert_eq!(replay.session_id, replay_session);
        assert_eq!(replay.fragment, first_batch.fragment);
        assert_eq!(replay.required_ack_level, first_batch.required_ack_level);
        assert_eq!(replay.ops, first_batch.ops);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer_rx.recv())
                .await
                .is_err(),
            "reconciliation retried before the replayed batch was ACKed"
        );
        sender
            .send(SenderMsg::Ack {
                session_id: replay_session,
                batch_seq: replay.batch_seq,
                durability: required_ack_durability,
            })
            .unwrap();

        let retry = tokio::time::timeout(Duration::from_secs(2), peer_rx.recv())
            .await
            .expect("routine reconciliation was not resumed after replay ACK")
            .expect("peer channel closed");
        match Envelope::decode(retry.as_slice()).unwrap().msg {
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                assert_eq!(request.session_id, replay_session);
                assert_ne!(request.round_id, 0);
            }
            other => panic!("expected ReconcileRequest after replay ACK, got {other:?}"),
        }
    }

    async fn common_wait_for_sender_report(
        report: &Arc<Mutex<SenderStateReport>>,
        predicate: impl Fn(&SenderStateReport) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = report.lock().await;
                if predicate(&state) {
                    return;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender state did not converge");
    }
}
