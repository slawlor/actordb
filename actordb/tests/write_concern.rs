use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use actordb::node::{Node, NodeConfig};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb::write_concern::{WriteConcern, WriteConcernError, WriteConcernOptions};

static NEXT_CLUSTER: AtomicU64 = AtomicU64::new(0);
struct Plan {
    ids: Vec<String>,
    addresses: Vec<SocketAddr>,
    reservations: Vec<Option<TcpListener>>,
    config: NodeConfig,
}
impl Plan {
    fn new(count: usize, first_replica: usize) -> Self {
        let id = NEXT_CLUSTER.fetch_add(1, Ordering::Relaxed);
        let ids: Vec<_> = (0..count).map(|i| format!("concern-{id}-{i}")).collect();
        let reservations: Vec<_> = (0..count)
            .map(|_| Some(TcpListener::bind("127.0.0.1:0").unwrap()))
            .collect();
        let addresses: Vec<_> = reservations
            .iter()
            .map(|l| l.as_ref().unwrap().local_addr().unwrap())
            .collect();
        let config = NodeConfig::builder()
            .fragment_map(HashMap::from([(
                FragmentId(0),
                ids[first_replica..].to_vec(),
            )]))
            .members(ids.iter().cloned().zip(addresses.iter().copied()).collect())
            .replication(
                SenderConfig::builder()
                    .batch_interval(Duration::from_millis(20))
                    .build(),
            )
            .build();
        Self {
            ids,
            addresses,
            reservations,
            config,
        }
    }
    async fn start(&mut self, index: usize) -> Node {
        drop(self.reservations[index].take());
        Node::start(
            self.ids[index].clone(),
            self.addresses[index],
            self.config.clone(),
        )
        .await
        .unwrap()
    }
    async fn all(&mut self) -> Vec<Node> {
        let mut nodes = Vec::new();
        for i in 0..self.ids.len() {
            nodes.push(self.start(i).await);
        }
        wait(|| async {
            let mut ready = true;
            for node in &nodes {
                ready &= node.peer_count().await == nodes.len() - 1;
            }
            ready
        })
        .await;
        nodes
    }
}
async fn wait<F, Fut>(condition: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
fn concern(level: WriteConcern) -> WriteConcernOptions {
    WriteConcernOptions::builder()
        .concern(level)
        .timeout(Duration::from_secs(2))
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_default_does_not_wait_and_timeout_does_not_undo_or_stop_replication() {
    let mut plan = Plan::new(3, 0);
    let first = plan.start(0).await;
    first.put("local".into(), vec![1]).await.unwrap();
    let receipt = first
        .put_with_write_concern(
            "explicit-local".into(),
            vec![2],
            concern(WriteConcern::Local),
        )
        .await
        .unwrap();
    assert_eq!(receipt.acknowledged_nodes(), &[first.id()]);
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Majority)
        .timeout(Duration::from_millis(100))
        .build();
    let error = first
        .put_with_write_concern("timed-out".into(), vec![3], options)
        .await
        .unwrap_err();
    let WriteConcernError::TimedOut(progress) = error else {
        panic!("expected timeout")
    };
    assert_eq!(progress.acknowledged_nodes(), &[first.id()]);
    assert_eq!(progress.required_acknowledgements(), 2);
    assert_eq!(
        first.get("timed-out".into()).await.unwrap().unwrap().value,
        vec![3]
    );
    let second = plan.start(1).await;
    wait(|| async {
        second
            .get("timed-out".into())
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counts_and_majority_include_receiver_and_only_require_threshold() {
    let mut plan = Plan::new(5, 0);
    let nodes = plan.all().await;
    let two = nodes[0]
        .put_with_write_concern("count".into(), vec![1], concern(WriteConcern::Count(2)))
        .await
        .unwrap();
    assert_eq!(two.required_acknowledgements(), 2);
    assert!(two.acknowledged_nodes().len() >= 2);
    assert!(two
        .acknowledged_nodes()
        .iter()
        .any(|id| id == nodes[0].id()));
    let majority = nodes[0]
        .put_with_write_concern("majority".into(), vec![2], concern(WriteConcern::Majority))
        .await
        .unwrap();
    assert_eq!(majority.required_acknowledgements(), 3);
    assert_eq!(majority.replica_count(), Some(5));
    assert!(majority.acknowledged_nodes().len() >= 3);
    let deleted = nodes[0]
        .delete_with_write_concern("majority".into(), concern(WriteConcern::Majority))
        .await
        .unwrap();
    assert!(deleted.acknowledged_nodes().len() >= 3);
    for id in deleted.acknowledged_nodes() {
        let node = nodes.iter().find(|node| node.id() == id).unwrap();
        assert!(node.get("majority".into()).await.unwrap().is_none());
    }
    assert!(matches!(
        nodes[0]
            .put_with_write_concern("invalid".into(), vec![4], concern(WriteConcern::Count(4)))
            .await,
        Err(WriteConcernError::InvalidPolicy(_))
    ));
    assert!(nodes[0].get("invalid".into()).await.unwrap().is_none());
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_write_counts_the_serving_receiver_not_ingress() {
    let mut plan = Plan::new(4, 1);
    let nodes = plan.all().await;
    let receipt = nodes[0]
        .put_with_write_concern("routed".into(), vec![1], concern(WriteConcern::Majority))
        .await
        .unwrap();
    assert_eq!(receipt.replica_count(), Some(3));
    assert_eq!(receipt.required_acknowledgements(), 2);
    assert_ne!(receipt.receiving_node(), nodes[0].id());
    assert!(!receipt
        .acknowledged_nodes()
        .iter()
        .any(|id| id == nodes[0].id()));
    let deleted = nodes[0]
        .delete_with_write_concern("routed".into(), concern(WriteConcern::Count(2)))
        .await
        .unwrap();
    assert_eq!(deleted.required_acknowledgements(), 2);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn nonlocal_concern_requires_configured_replicas_before_application() {
    let id = NEXT_CLUSTER.fetch_add(1, Ordering::Relaxed);
    let node = Node::start(
        format!("concern-unplaced-{id}"),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        node.put_with_write_concern("missing".into(), vec![1], concern(WriteConcern::Majority))
            .await,
        Err(WriteConcernError::InvalidPolicy(_))
    ));
    assert!(node.get("missing".into()).await.unwrap().is_none());
    node.put_with_write_concern("local".into(), vec![1], concern(WriteConcern::Count(1)))
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[tracing_test::traced_test]
async fn forwarded_concern_accepts_replication_ack_on_the_same_peer_connection() {
    use actordb::node::{REQUIRED_WIRE_CAPABILITIES, WIRE_PROTOCOL_VERSION};
    use actordb::proto::{
        Envelope, EnvelopeMsg, ForwardedPut, Hello, HelloAuth, ReplicationAckLevel,
    };
    use prost::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn send(stream: &mut tokio::net::TcpStream, envelope: Envelope) {
        let bytes = envelope.encode_to_vec();
        stream.write_u32(bytes.len() as u32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    }
    async fn receive(stream: &mut tokio::net::TcpStream) -> Envelope {
        let length = stream.read_u32().await.unwrap() as usize;
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await.unwrap();
        Envelope::decode(bytes.as_slice()).unwrap()
    }
    let mut plan = Plan::new(2, 0);
    let node = plan.start(0).await;
    let mut peer = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    assert!(matches!(
        receive(&mut peer).await.msg,
        Some(EnvelopeMsg::Hello(_))
    ));
    send(
        &mut peer,
        Envelope {
            msg: Some(EnvelopeMsg::Hello(Hello {
                node_id: plan.ids[1].clone(),
                min_protocol_version: WIRE_PROTOCOL_VERSION,
                max_protocol_version: WIRE_PROTOCOL_VERSION,
                capabilities: REQUIRED_WIRE_CAPABILITIES,
                challenge: vec![1; 32],
                auth_tag: vec![],
            })),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut peer).await.msg,
        Some(EnvelopeMsg::HelloAuth(_))
    ));
    send(
        &mut peer,
        Envelope {
            msg: Some(EnvelopeMsg::HelloAuth(HelloAuth { auth_tag: vec![] })),
        },
    )
    .await;
    let timestamp = actordb::actor::now_ms();
    send(
        &mut peer,
        Envelope {
            msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                key: "same-connection".into(),
                value: vec![1],
                timestamp_ms: timestamp,
                logical_counter: 0,
                node_id: plan.ids[1].clone(),
                seq: 1,
                boot_id: 1,
                req_id: 41,
                write_concern: Some(concern(WriteConcern::Count(2)).encode().unwrap()),
                ..Default::default()
            })),
        },
    )
    .await;
    let mut summaries = HashMap::new();
    let receipt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match receive(&mut peer).await.msg {
                Some(EnvelopeMsg::ReplicateBatch(batch)) => {
                    for op in batch.ops {
                        summaries.insert(
                            op.key.clone(),
                            actordb::proto::KeySummary {
                                key: op.key,
                                timestamp_ms: op.timestamp_ms,
                                logical_counter: op.logical_counter,
                                node_id: op.node_id,
                                is_tombstone: op.is_tombstone,
                            },
                        );
                    }
                    if batch.batch_seq != u64::MAX {
                        send(
                            &mut peer,
                            Envelope {
                                msg: Some(EnvelopeMsg::BatchAck(actordb::proto::BatchAck {
                                    session_id: batch.session_id,
                                    batch_seq: batch.batch_seq,
                                    fragment: batch.fragment,
                                    level: ReplicationAckLevel::Applied as i32,
                                })),
                            },
                        )
                        .await;
                    }
                }
                Some(EnvelopeMsg::ReconcileRequest(request)) => {
                    let mut values: Vec<_> = summaries
                        .values()
                        .filter(|entry| entry.key > request.after_key)
                        .cloned()
                        .collect();
                    values.sort_by(|a, b| a.key.cmp(&b.key));
                    let next_after_key = values
                        .last()
                        .map_or(request.after_key.clone(), |entry| entry.key.clone());
                    send(
                        &mut peer,
                        Envelope {
                            msg: Some(EnvelopeMsg::ReconcileResponse(
                                actordb::proto::ReconcileResponse {
                                    fragment: request.fragment,
                                    session_id: request.session_id,
                                    round_id: request.round_id,
                                    page_seq: request.page_seq,
                                    summaries: values,
                                    next_after_key,
                                    done: true,
                                    snapshot_revision: summaries.len() as u64,
                                    snapshot_time_ms: actordb::actor::now_ms(),
                                    ..Default::default()
                                },
                            )),
                        },
                    )
                    .await;
                }
                Some(EnvelopeMsg::MutationResponse(mut response)) if response.req_id == 41 => {
                    let result = response.results.remove(0);
                    return actordb_protocol::write_concern::WriteConcernOutcome::decode(
                        result.write_concern.as_deref().unwrap(),
                    )
                    .unwrap();
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    let actordb_protocol::write_concern::WriteConcernOutcome::Satisfied(receipt) = receipt else {
        panic!("same-stream ACK was not processed")
    };
    assert_eq!(receipt.required_acknowledgements(), 2);
    assert_eq!(receipt.acknowledged_nodes().len(), 2);
    drop(peer);
    node.shutdown().await.unwrap();
}
