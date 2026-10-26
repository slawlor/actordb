// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Shared test-only channels, actors, and placement fixtures.
//! Every helper is confined to the node subtree.

use super::*;

pub(super) struct BlockingPageActor;

impl Actor for BlockingPageActor {
    type Msg = KvMessage;
    type State = BlockingPageState;
    type Arguments = BlockingPageState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        if let KvMessage::GetVersionedPage { reply, .. } = message {
            state.started.notify_one();
            state.release.notified().await;
            drop(reply);
        } else if let KvMessage::GetBatch { reply, .. } = message {
            state.started.notify_one();
            state.release.notified().await;
            drop(reply);
        } else if let KvMessage::GetBatchRetained(request) = message {
            state.started.notify_one();
            state.release.notified().await;
            drop(request);
        }
        Ok(())
    }
}

pub(super) struct BlockingPageState {
    pub(super) started: Arc<Notify>,
    pub(super) release: Arc<Notify>,
}

pub(super) fn test_peer_channel(
    capacity: usize,
) -> (
    PeerChannel,
    mpsc::Receiver<BudgetedFrame>,
    mpsc::Receiver<BudgetedFrame>,
) {
    let (tx, rx) = mpsc::channel(capacity);
    let (response_tx, response_rx) = mpsc::channel(capacity);
    let identity = crate::network::PeerConnectionId::new();
    let (cancel, _) = watch::channel(false);
    (
        PeerChannel {
            work_retention: None,
            outbound: BudgetedPeerTx {
                node_byte_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
                tx,
                response_tx,
                byte_budget: Arc::new(Semaphore::new(PEER_CHANNEL_BYTE_CAPACITY)),
                producer_byte_budget: Arc::new(Semaphore::new(
                    PEER_CHANNEL_BYTE_CAPACITY - PEER_RESPONSE_BYTE_RESERVE,
                )),
                reconcile_gate: ReconcileGate::new(),
                connection_id: identity,
            },
            cancel,
            queue_admission_timeout: Duration::from_millis(10),
            rank: None,
        },
        rx,
        response_rx,
    )
}

pub(super) fn test_sender_report() -> Arc<Mutex<SenderStateReport>> {
    Arc::new(Mutex::new(SenderStateReport {
        state: "disconnected".to_string(),
        session_id: 0,
        buffered: 0,
        retained_bytes: 0,
        aggregate_retained_bytes: 0,
        in_flight: 0,
        last_ack_durability: None,
        ack_durability_mismatch: false,
        reconcile_rounds: 0,
        progress: Default::default(),
    }))
}

pub(super) async fn test_connection_context(
    router: FragmentRouter,
    serving: Arc<RwLock<HashSet<FragmentId>>>,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    dynamic_placement: bool,
    senders: SenderMap,
    sender_reports: SenderReportMap,
    peers: PeerMap,
) -> (ConnContext, Arc<RuntimeTasks>) {
    let tasks = RuntimeTasks::new(8);
    let read_service = ReadService::start(&tasks, READ_QUEUE_CAPACITY, READ_KEY_BYTE_CAPACITY)
        .await
        .expect("test read service started");
    let (peer_changes, _) = watch::channel(0u64);
    (
        ConnContext {
            recovery: None,
            node_id: "local".to_string(),
            router,
            serving,
            placement,
            dynamic_placement,
            pending_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_batch_gets: Arc::new(StdMutex::new(HashMap::new())),
            pending_scans: Arc::new(StdMutex::new(HashMap::new())),
            pending_query_pages: Arc::new(StdMutex::new(HashMap::new())),
            query_admission: crate::query::QueryAdmission::new(),
            pending_mutations: Arc::new(StdMutex::new(HashMap::new())),
            pending_relays: Arc::new(StdMutex::new(HashMap::new())),
            pending_mutation_relays: Arc::new(StdMutex::new(HashMap::new())),
            next_req_id: Arc::new(AtomicU64::new(1)),
            peers,
            peer_failures: PeerFailureSignals::default(),
            tasks: Arc::downgrade(&tasks),
            read_service,
            operation_gate: OperationGate::new(),
            receive_apply_gate: OperationGate::new(),
            peer_authority: PeerAuthority::new("local".into(), None),
            peer_changes,
            hlc: Arc::new(Mutex::new(HlcClock::new())),
            senders,
            finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_config: SenderConfig::default(),
            sender_runtime: SenderRuntimeOptions::default(),
            runtime_options: NodeRuntimeOptions::default(),
            receive_byte_budget: Arc::new(Semaphore::new(RECEIVE_BYTE_CAPACITY)),
            node_peer_queue_budget: NodePeerByteBudget::new(NODE_PEER_QUEUE_BYTE_CAPACITY),
            relay_admission: RelayAdmission::new(RELAY_CAPACITY, RELAY_KEY_BYTE_CAPACITY),
            mutation_service: MutationService::new(
                FORWARDED_MUTATION_CAPACITY,
                FORWARDED_MUTATION_BYTE_CAPACITY,
            ),
            sender_reports,
            membership_tx: None,
            cluster_secret: None,
            sender_snapshot_hook: None,
        },
        tasks,
    )
}

pub(super) async fn complete_test_handshake(stream: &mut tokio::io::DuplexStream, peer_id: &str) {
    assert!(matches!(
        read_test_envelope(stream).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    write_test_envelope(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: peer_id.to_string(),
                protocol_version: WIRE_PROTOCOL_VERSION,
                challenge: vec![7; HANDSHAKE_CHALLENGE_BYTES],
                process_generation: 0,
            })),
        },
    )
    .await;
    assert!(matches!(
        read_test_envelope(stream).await.msg,
        Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag })) if auth_tag.is_empty()
    ));
    write_test_envelope(
        stream,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth {
                auth_tag: Vec::new(),
            })),
        },
    )
    .await;
}

pub(super) fn result(key: &str, request_index: u32) -> MutationResult {
    MutationResult {
        key: key.to_string(),
        status: MutationStatus::Applied as i32,
        error: String::new(),
        winner_timestamp_ms: 0,
        winner_logical_counter: 0,
        winner_node_id: String::new(),
        winner_is_tombstone: false,
        request_index,

        write_concern: None,
    }
}

pub(super) async fn write_test_envelope(stream: &mut tokio::io::DuplexStream, envelope: Envelope) {
    let frame = envelope.encode_to_vec();
    stream.write_u32(frame.len() as u32).await.unwrap();
    stream.write_all(&frame).await.unwrap();
}

pub(super) async fn read_test_envelope(stream: &mut tokio::io::DuplexStream) -> Envelope {
    tokio::time::timeout(Duration::from_secs(1), async {
        let len = stream.read_u32().await.unwrap() as usize;
        let mut frame = vec![0; len];
        stream.read_exact(&mut frame).await.unwrap();
        Envelope::decode(frame.as_slice()).unwrap()
    })
    .await
    .expect("timed out waiting for test protocol frame")
}
