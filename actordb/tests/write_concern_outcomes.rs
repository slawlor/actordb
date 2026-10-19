//! Lost replies preserve uncertainty across subsequent routing attempts.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::proto::{Envelope, EnvelopeMsg, MutationStatus};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use actordb::write_concern::{
    WriteConcern, WriteConcernError, WriteConcernOptions, WriteConcernOutcome,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

static NEXT_CASE: AtomicU64 = AtomicU64::new(0);

/// Forward real peer traffic, observing or dropping only the selected write's
/// response. The receiving Node performs its ordinary stamped apply and fanout.
struct ReplyProxy {
    address: SocketAddr,
    observed: Arc<Notify>,
    task: JoinHandle<()>,
}

impl ReplyProxy {
    async fn start(receiver: SocketAddr, key: &'static str, drop_reply: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let observed = Arc::new(Notify::new());
        let signal = observed.clone();
        let task = tokio::spawn(async move {
            let (origin, _) = listener.accept().await.unwrap();
            let receiver = TcpStream::connect(receiver).await.unwrap();
            let (mut origin_read, mut origin_write) = origin.into_split();
            let (mut receiver_read, mut receiver_write) = receiver.into_split();
            let requests = tokio::io::copy(&mut origin_read, &mut receiver_write);
            let responses = async {
                loop {
                    let Ok(length) = receiver_read.read_u32().await else {
                        return;
                    };
                    let mut bytes = vec![0; length as usize];
                    if receiver_read.read_exact(&mut bytes).await.is_err() {
                        return;
                    }
                    let envelope = Envelope::decode(bytes.as_slice()).unwrap();
                    if let Some(EnvelopeMsg::MutationResponse(response)) = envelope.msg {
                        if let Some(result) =
                            response.results.iter().find(|result| result.key == key)
                        {
                            let outcome = WriteConcernOutcome::decode(
                                result.write_concern.as_deref().unwrap(),
                            )
                            .unwrap();
                            if drop_reply {
                                assert!(matches!(
                                    MutationStatus::try_from(result.status),
                                    Ok(MutationStatus::Applied | MutationStatus::AlreadyPresent)
                                ));
                                assert!(matches!(outcome, WriteConcernOutcome::Satisfied(_)));
                                signal.notify_one();
                                continue;
                            }
                            assert!(matches!(outcome, WriteConcernOutcome::InvalidPolicy(_)));
                            signal.notify_one();
                        }
                    }
                    if origin_write.write_u32(length).await.is_err()
                        || origin_write.write_all(&bytes).await.is_err()
                    {
                        return;
                    }
                }
            };
            tokio::select! { _ = requests => {}, _ = responses => {} }
        });
        Self {
            address,
            observed,
            task,
        }
    }

    async fn assert_observed(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.observed.notified())
            .await
            .unwrap();
    }
}

impl Drop for ReplyProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    origin: Node,
    receiver: Node,
    fallback: Option<Node>,
    lost_reply: ReplyProxy,
    rejection: Option<ReplyProxy>,
}

fn config(replicas: Vec<String>) -> NodeConfig {
    NodeConfig::builder()
        .fragment_map(HashMap::from([(FragmentId(0), replicas)]))
        .replication(SenderConfig::builder().batch_size(1).build())
        .build()
}

impl Fixture {
    async fn start(key: &'static str, rejecting_fallback: bool) -> Self {
        let case = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let receiver_id = format!("outcomes-{case}-receiver");
        let fallback_id = format!("outcomes-{case}-fallback");
        let replicas = vec![receiver_id.clone(), fallback_id.clone()];
        let receiver = Node::start(
            receiver_id,
            "127.0.0.1:0".parse().unwrap(),
            config(replicas.clone()),
        )
        .await
        .unwrap();
        // A fallback with a smaller replica set rejects Count(2) before apply.
        // This models stale routing across a replica-set/configuration change.
        let fallback = if rejecting_fallback {
            let node = Node::start(
                fallback_id,
                "127.0.0.1:0".parse().unwrap(),
                config(vec![replicas[1].clone()]),
            )
            .await
            .unwrap();
            node.connect(receiver.addr()).await.unwrap();
            Some(node)
        } else {
            None
        };
        let lost_reply = ReplyProxy::start(receiver.addr(), key, true).await;
        let rejection = if let Some(node) = &fallback {
            Some(ReplyProxy::start(node.addr(), key, false).await)
        } else {
            None
        };
        let origin = Node::start_with_options(
            format!("outcomes-{case}-origin"),
            "127.0.0.1:0".parse().unwrap(),
            config(replicas),
            NodeRuntimeOptions::default()
                .with_forwarded_attempt_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap();
        origin.connect(lost_reply.address).await.unwrap();
        if let Some(proxy) = &rejection {
            origin.connect(proxy.address).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            let expected = if rejecting_fallback { 2 } else { 1 };
            while origin.peer_count().await != expected
                || receiver.peer_count().await != expected
                || if let Some(node) = &fallback {
                    node.peer_count().await != 2
                } else {
                    false
                }
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Self {
            origin,
            receiver,
            fallback,
            lost_reply,
            rejection,
        }
    }

    async fn assert_applied(&self, key: &str) {
        self.lost_reply.assert_observed().await;
        let value = self.receiver.get(key.into()).await.unwrap().unwrap();
        assert_eq!(value.value, vec![9]);
        assert_eq!(value.node_id, self.origin.id());
    }

    async fn shutdown(self) {
        self.origin.shutdown().await.unwrap();
        self.receiver.shutdown().await.unwrap();
        if let Some(node) = self.fallback {
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_applied_reply_then_no_connected_fallback_returns_unknown_outcome() {
    let key = "outcomes-lost-no-fallback";
    let fixture = Fixture::start(key, false).await;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.origin.put_with_write_concern(
            key.into(),
            vec![9],
            WriteConcernOptions::builder()
                .timeout(Duration::from_millis(300))
                .build(),
        ),
    )
    .await
    .unwrap();
    fixture.assert_applied(key).await;
    assert!(
        matches!(result, Err(WriteConcernError::UnknownOutcome(_))),
        "{result:?}"
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_applied_reply_then_preapply_rejection_preserves_unknown_outcome() {
    let key = "outcomes-lost-then-rejected";
    let fixture = Fixture::start(key, true).await;
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Count(2))
        .timeout(Duration::from_millis(300))
        .build();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .origin
            .put_with_write_concern(key.into(), vec![9], options),
    )
    .await
    .unwrap();
    fixture.assert_applied(key).await;
    fixture.rejection.as_ref().unwrap().assert_observed().await;
    assert!(
        matches!(result, Err(WriteConcernError::UnknownOutcome(_))),
        "{result:?}"
    );
    fixture.shutdown().await;
}
