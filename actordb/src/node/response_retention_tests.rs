// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Actual socket backpressure must retain request and peer-era ownership.

use super::connection::fresh_hello;
use super::*;

async fn send(peer: &mut tokio::io::DuplexStream, message: EnvelopeMsg) {
    let bytes = Envelope { msg: Some(message) }.encode_to_vec();
    peer.write_u32(bytes.len() as u32).await.unwrap();
    peer.write_all(&bytes).await.unwrap();
}
async fn receive(peer: &mut tokio::io::DuplexStream) -> Envelope {
    let length = peer.read_u32().await.unwrap();
    let mut bytes = vec![0; length as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    Envelope::decode(bytes.as_slice()).unwrap()
}
async fn fixture() -> (
    Arc<Node>,
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
) {
    fixture_with_options(NodeRuntimeOptions::default()).await
}

async fn fixture_with_options(
    options: NodeRuntimeOptions,
) -> (
    Arc<Node>,
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
) {
    let node = Arc::new(
        Node::start_with_options(
            "response-holder".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_map: HashMap::from([(FragmentId(0), vec!["response-holder".into()])]),
                ..Default::default()
            },
            options,
        )
        .await
        .unwrap(),
    );
    node.put("row".into(), vec![7; 4096]).await.unwrap();
    let (server, mut peer) = tokio::io::duplex(256);
    let context = node.connection_context();
    let session = tokio::spawn(async move {
        handle_connection(context, Box::pin(server), false, None, None).await;
    });
    assert!(matches!(
        receive(&mut peer).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send(
        &mut peer,
        EnvelopeMsg::Hello(fresh_hello("observer").unwrap()),
    )
    .await;
    assert!(matches!(
        receive(&mut peer).await.msg,
        Some(EnvelopeMsg::HelloAuth(_))
    ));
    send(
        &mut peer,
        EnvelopeMsg::HelloAuth(HelloAuth {
            auth_tag: Vec::new(),
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while node.peer_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    (node, peer, session)
}
fn execution_drain() -> drain::DrainOptions {
    drain::DrainOptions::default().with_durability(drain::DrainDurability::Applied)
}
async fn blocked_response(peer: &mut tokio::io::DuplexStream) {
    let length = tokio::time::timeout(Duration::from_secs(3), peer.read_u32())
        .await
        .unwrap()
        .unwrap();
    assert!(
        length > 256,
        "response must exceed the unconsumed socket buffer, got {length}"
    );
}
async fn finish(node: &Node, peer: tokio::io::DuplexStream, session: tokio::task::JoinHandle<()>) {
    drop(peer);
    tokio::time::timeout(Duration::from_secs(3), session)
        .await
        .unwrap()
        .unwrap();
    node.drain_requests(execution_drain()).await.unwrap();
    assert_eq!(node.drain_status().active_requests(), 0);
    assert_eq!(node.drain_status().awaiting_responses(), 0);
    assert_eq!(node.peer_fence_status().outstanding_frames(), 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn actual_sql_and_scan_responses_block_drain_until_socket_failure() {
    for scan in [false, true] {
        let (node, mut peer, session) = fixture().await;
        let request = if scan {
            EnvelopeMsg::ScanRequest(crate::proto::ScanRequest {
                req_id: 1,
                fragment: 0,
                matcher_kind: 0,
                transform_kind: 0,
                limit: 1,
                ..Default::default()
            })
        } else {
            EnvelopeMsg::QueryPageRequest(crate::proto::QueryPageRequest {
                req_id: 1,
                fragment: 0,
                sql: "SELECT key, value FROM kv".into(),
                max_bytes: 8192,
                max_rows: 16,
                max_visits: 32,
                ..Default::default()
            })
        };
        send(&mut peer, request).await;
        blocked_response(&mut peer).await;
        assert_eq!(node.drain_status().awaiting_responses(), 1);
        assert_eq!(node.drain_status().active_requests(), 1);
        assert_eq!(node.peer_fence_status().outstanding_frames(), 1);
        assert!(node.query_diagnostics().reserved_runtime_bytes > 0);
        assert!(matches!(
            node.drain_requests(execution_drain().with_timeout(Duration::from_millis(20)))
                .await,
            Err(drain::DrainError::Timeout { .. })
        ));
        assert_eq!(node.drain_status().active_requests(), 1);
        finish(&node, peer, session).await;
    }
}

struct DropReadReply;
impl ractor::Actor for DropReadReply {
    type Msg = KvMessage;
    type State = ();
    type Arguments = ();
    async fn pre_start(
        &self,
        _: ActorRef<KvMessage>,
        _: (),
    ) -> Result<(), ractor::ActorProcessingErr> {
        Ok(())
    }
    async fn handle(
        &self,
        _: ActorRef<KvMessage>,
        message: KvMessage,
        _: &mut (),
    ) -> Result<(), ractor::ActorProcessingErr> {
        drop(message);
        Ok(())
    }
}

struct BlockRead {
    started: Notify,
    release: Semaphore,
}
struct BlockReadReply;
impl ractor::Actor for BlockReadReply {
    type Msg = KvMessage;
    type State = Arc<BlockRead>;
    type Arguments = Arc<BlockRead>;
    async fn pre_start(
        &self,
        _: ActorRef<KvMessage>,
        state: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(state)
    }
    async fn handle(
        &self,
        _: ActorRef<KvMessage>,
        message: KvMessage,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        state.started.notify_one();
        state.release.acquire().await.unwrap().forget();
        drop(message);
        Ok(())
    }
}
struct ReleaseRead(Arc<BlockRead>);
impl Drop for ReleaseRead {
    fn drop(&mut self) {
        self.0.release.add_permits(16);
    }
}

async fn block_large_read(node: &Node, peer: &mut tokio::io::DuplexStream) {
    send(
        peer,
        EnvelopeMsg::ForwardedGet(ForwardedGet {
            req_id: 1,
            key: Node::qualify_key("row".into()),
            ..Default::default()
        }),
    )
    .await;
    blocked_response(peer).await;
    assert_eq!(node.drain_status().active_requests(), 1);
}
async fn wait_queued(node: &Node, producer: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let channel = node.peers.lock().await.get("observer").cloned().unwrap();
            let queue = if producer {
                &channel.outbound.tx
            } else {
                &channel.outbound.response_tx
            };
            if queue.capacity() < queue.max_capacity()
                && node.drain_status().awaiting_responses() == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn queued_sql_scan_and_admission_error_pages_keep_both_leases() {
    for error in 0..3 {
        let (node, mut peer, session) = fixture().await;
        block_large_read(&node, &mut peer).await;
        let request = match error {
            0 => EnvelopeMsg::QueryPageRequest(crate::proto::QueryPageRequest {
                req_id: 2,
                fragment: 0,
                sql: "not SQL".into(),
                max_bytes: 8192,
                ..Default::default()
            }),
            1 => EnvelopeMsg::ScanRequest(crate::proto::ScanRequest {
                req_id: 2,
                fragment: 0,
                matcher_kind: 99,
                ..Default::default()
            }),
            _ => EnvelopeMsg::QueryPageRequest(crate::proto::QueryPageRequest {
                req_id: 2,
                fragment: 0,
                sql: "SELECT key FROM kv".into(),
                max_bytes: 1,
                ..Default::default()
            }),
        };
        send(&mut peer, request).await;
        wait_queued(&node, true).await;
        assert_eq!(node.drain_status().active_requests(), 2);
        assert_eq!(node.peer_fence_status().outstanding_frames(), 2);
        assert!(matches!(
            node.drain_requests(execution_drain().with_timeout(Duration::from_millis(20)))
                .await,
            Err(drain::DrainError::Timeout { .. })
        ));
        finish(&node, peer, session).await;
    }
}

#[tokio::test]
async fn batch_read_actor_error_response_retains_request_until_socket_failure() {
    let (node, mut peer, session) = fixture().await;
    block_large_read(&node, &mut peer).await;
    let actual = node.router.route_for_fragment(FragmentId(0)).unwrap();
    let (dropper, _) = DropReadReply::spawn(None, DropReadReply, ()).await.unwrap();
    node.router.insert(FragmentId(0), dropper.clone());
    send(
        &mut peer,
        EnvelopeMsg::BatchGet(BatchGet {
            req_id: 2,
            keys: vec![Node::qualify_key("missing".into())],
            ..Default::default()
        }),
    )
    .await;
    wait_queued(&node, false).await;
    assert_eq!(node.drain_status().active_requests(), 2);
    assert_eq!(node.peer_fence_status().outstanding_frames(), 2);
    assert!(matches!(
        node.drain_requests(execution_drain().with_timeout(Duration::from_millis(20)))
            .await,
        Err(drain::DrainError::Timeout { .. })
    ));
    node.router.insert(FragmentId(0), actual);
    dropper.stop(None);
    dropper.wait(None).await.unwrap();
    finish(&node, peer, session).await;
}

#[tokio::test]
async fn timed_out_batch_read_keeps_actor_and_error_response_owned_independently() {
    let (node, mut peer, session) = fixture_with_options(
        NodeRuntimeOptions::default().with_forwarded_attempt_timeout(Duration::from_millis(250)),
    )
    .await;
    block_large_read(&node, &mut peer).await;
    let actual = node.router.route_for_fragment(FragmentId(0)).unwrap();
    let state = Arc::new(BlockRead {
        started: Notify::new(),
        release: Semaphore::new(0),
    });
    let _release = ReleaseRead(state.clone());
    let (blocker, _) = BlockReadReply::spawn(None, BlockReadReply, state.clone())
        .await
        .unwrap();
    node.router.insert(FragmentId(0), blocker.clone());
    send(
        &mut peer,
        EnvelopeMsg::BatchGet(BatchGet {
            req_id: 2,
            keys: vec![Node::qualify_key("missing".into())],
            ..Default::default()
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), state.started.notified())
        .await
        .unwrap();
    wait_queued(&node, false).await; // The actor waiter expired, while its work remains blocked.
    assert_eq!(node.drain_status().active_requests(), 2);
    assert_eq!(node.peer_fence_status().outstanding_frames(), 2);
    assert!(matches!(
        node.drain_requests(execution_drain().with_timeout(Duration::from_millis(20)))
            .await,
        Err(drain::DrainError::Timeout { .. })
    ));
    drop(peer);
    tokio::time::timeout(Duration::from_secs(3), session)
        .await
        .unwrap()
        .unwrap();
    // Both queued frames failed delivery, but the timed-out actor still owns
    // the second request. A stream teardown cannot certify execution completion.
    assert!(matches!(
        node.drain_requests(execution_drain().with_timeout(Duration::from_millis(20)))
            .await,
        Err(drain::DrainError::Timeout { .. })
    ));
    assert_eq!(node.drain_status().active_requests(), 1);
    state.release.add_permits(1);
    node.drain_requests(execution_drain()).await.unwrap();
    node.router.insert(FragmentId(0), actual);
    blocker.stop(None);
    blocker.wait(None).await.unwrap();
    node.shutdown().await.unwrap();
}
