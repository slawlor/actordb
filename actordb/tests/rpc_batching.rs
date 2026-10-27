//! Real actor/TCP contract and frame-count regressions for generic RPC batching.
use actordb::actor::PoolConfig;
use actordb::network::{BindFuture, BoxStream, ConnectFuture, Network, TcpNetwork};
use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::rpc::{NodeRpcService, RpcReply, RpcRequest, RpcService};
use actordb::shard::FragmentId;
use actordb_protocol::client as wire;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct CountingNetwork(
    Arc<AtomicUsize>,
    Option<(std::net::SocketAddr, Arc<AtomicBool>)>,
);
struct CountingStream {
    stream: BoxStream,
    writes: Arc<AtomicUsize>,
    drop_replies: Option<Arc<AtomicBool>>,
}
impl AsyncRead for CountingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self
            .drop_replies
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            let mut scratch = [0; 16384];
            let mut discarded = ReadBuf::new(&mut scratch);
            match self.stream.as_mut().poll_read(cx, &mut discarded) {
                Poll::Ready(Ok(())) if !discarded.filled().is_empty() => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                result => result,
            }
        } else {
            self.stream.as_mut().poll_read(cx, out)
        }
    }
}
impl AsyncWrite for CountingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = self.stream.as_mut().poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Ok(_))) {
            self.writes.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.stream.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.stream.as_mut().poll_shutdown(cx)
    }
}
impl Network for CountingNetwork {
    fn bind(&self, addr: std::net::SocketAddr) -> BindFuture<'_> {
        Box::pin(async move { TcpNetwork.bind(addr).await })
    }
    fn connect(&self, addr: std::net::SocketAddr) -> ConnectFuture<'_> {
        let writes = self.0.clone();
        let drop_replies = self
            .1
            .as_ref()
            .filter(|(target, _)| *target == addr)
            .map(|(_, flag)| flag.clone());
        Box::pin(async move {
            Ok(Box::pin(CountingStream {
                stream: TcpNetwork.connect(addr).await?,
                writes,
                drop_replies,
            }) as BoxStream)
        })
    }
}
async fn call(
    service: &NodeRpcService,
    table: &str,
    operation: wire::request::Operation,
) -> RpcReply {
    service
        .call(RpcRequest::new(wire::Request {
            table: table.into(),
            operation: Some(operation),
        }))
        .await
        .unwrap()
}
fn gets(keys: Vec<String>) -> wire::request::Operation {
    wire::request::Operation::BatchGet(wire::BatchGet { keys, priority: 0 })
}
fn puts(values: Vec<wire::Put>) -> wire::request::Operation {
    wire::request::Operation::BatchPut(wire::BatchPut {
        puts: values,
        priority: 0,
    })
}
fn put(key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> wire::Put {
    wire::Put {
        key: key.into(),
        value,
        ttl_ms,
        ..Default::default()
    }
}
fn config(map: HashMap<FragmentId, Vec<String>>) -> NodeConfig {
    NodeConfig::builder()
        .fragment_count(map.len())
        .fragment_map(map)
        .background_pool(PoolConfig::builder().workers(2).build())
        .build()
}
async fn connected(origin: &Node, owner: &Node) {
    origin.connect(owner.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while origin.peer_count().await != 1 || owner.peer_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_bulk_uses_one_peer_frame_and_preserves_ordered_duplicates_and_namespaces() {
    let map = HashMap::from([(FragmentId(0), vec!["batch-owner".into()])]);
    let owner = Node::start(
        "batch-owner".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(map.clone()),
    )
    .await
    .unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let mut cfg = config(map);
    cfg.network = Arc::new(CountingNetwork(writes.clone(), None));
    let origin = Arc::new(
        Node::start("batch-router".into(), "127.0.0.1:0".parse().unwrap(), cfg)
            .await
            .unwrap(),
    );
    connected(&origin, &owner).await;
    let service = NodeRpcService::new(origin.clone(), Default::default()).unwrap();
    let keys: Vec<_> = (0..64).map(|i| format!("key-{i}")).collect();
    writes.store(0, Ordering::Relaxed);
    let response = call(
        &service,
        "USERS",
        puts(keys.iter().map(|key| put(key, vec![7], None)).collect()),
    )
    .await;
    assert!(response.error.is_none());
    assert!(response
        .mutations
        .iter()
        .all(|result| result.error.is_none()));
    assert_eq!(
        writes.load(Ordering::Relaxed),
        2,
        "64 puts must use one length/body peer frame"
    );
    drop(response);
    let mut requested = keys.clone();
    requested.push(keys[3].clone());
    requested.push("missing".into());
    writes.store(0, Ordering::Relaxed);
    let response = call(&service, "USERS", gets(requested.clone())).await;
    assert!(response.error.is_none());
    assert_eq!(response.reads.len(), requested.len());
    assert_eq!(
        response
            .reads
            .iter()
            .map(|row| &row.key)
            .collect::<Vec<_>>(),
        requested.iter().collect::<Vec<_>>()
    );
    assert!(response.reads[..65].iter().all(|row| row
        .value
        .as_ref()
        .is_some_and(|value| value.bytes == vec![7])));
    assert!(response.reads[65].value.is_none());
    assert_eq!(writes.load(Ordering::Relaxed), 2);
    drop(response);
    let literal = "\u{1e}actordb.table.v1:4b56:61";
    let response = call(
        &service,
        "USERS",
        puts(vec![
            put(literal, vec![9], None),
            put("dup", vec![1], None),
            put("dup", vec![2], None),
        ]),
    )
    .await;
    assert!(response
        .mutations
        .iter()
        .all(|result| result.error.is_none()));
    drop(response);
    assert_eq!(
        owner
            .table("USERS")
            .unwrap()
            .get(literal)
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![9]
    );
    assert_eq!(
        owner
            .table("USERS")
            .unwrap()
            .get("dup")
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![2]
    );
    assert!(owner.get("dup".into()).await.unwrap().is_none());
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    origin.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn ttl_failover_replays_exact_stamps_and_expiry_then_reads_the_surviving_holder() {
    let map = HashMap::from([(FragmentId(0), vec!["ttl-first".into(), "ttl-second".into()])]);
    let first = Node::start(
        "ttl-first".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(map.clone()),
    )
    .await
    .unwrap();
    let second = Node::start(
        "ttl-second".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(map.clone()),
    )
    .await
    .unwrap();
    let dropping = Arc::new(AtomicBool::new(false));
    let mut cfg = config(map);
    cfg.network = Arc::new(CountingNetwork(
        Arc::new(AtomicUsize::new(0)),
        Some((first.addr(), dropping.clone())),
    ));
    let origin = Arc::new(
        Node::start_with_options(
            "ttl-failover".into(),
            "127.0.0.1:0".parse().unwrap(),
            cfg,
            NodeRuntimeOptions::default()
                .with_forwarded_attempt_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap(),
    );
    origin.connect(first.addr()).await.unwrap();
    origin.connect(second.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while origin.peer_count().await != 2
            || first.peer_count().await != 1
            || second.peer_count().await != 1
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let service = NodeRpcService::new(origin.clone(), Default::default()).unwrap();
    dropping.store(true, Ordering::Release);
    let response = call(
        &service,
        "KV",
        puts(vec![
            put("a", vec![1], Some(60000)),
            put("b", vec![2], Some(60000)),
        ]),
    )
    .await;
    assert!(response.error.is_none());
    assert!(response.mutations.iter().all(|row| row.error.is_none()));
    drop(response);
    for key in ["a", "b"] {
        let original = first.get(key.into()).await.unwrap().unwrap();
        let replay = second.get(key.into()).await.unwrap().unwrap();
        assert_eq!(original, replay);
        assert!(original.expires_at_ms.is_some());
    }
    let response = call(
        &service,
        "KV",
        gets(vec!["b".into(), "missing".into(), "a".into()]),
    )
    .await;
    assert!(response.error.is_none());
    assert_eq!(response.reads[0].value.as_ref().unwrap().bytes, vec![2]);
    assert!(response.reads[1].value.is_none());
    assert_eq!(response.reads[2].value.as_ref().unwrap().bytes, vec![1]);
    drop(response);
    dropping.store(false, Ordering::Release);
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    origin.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_and_remote_wide_reads_fail_completely_and_release_page_reservations() {
    for remote in [false, true] {
        let map = HashMap::from([(FragmentId(0), vec!["wide-owner".into()])]);
        let owner = Arc::new(
            Node::start(
                "wide-owner".into(),
                "127.0.0.1:0".parse().unwrap(),
                config(map.clone()),
            )
            .await
            .unwrap(),
        );
        let origin = if remote {
            Arc::new(
                Node::start(
                    "wide-router".into(),
                    "127.0.0.1:0".parse().unwrap(),
                    config(map),
                )
                .await
                .unwrap(),
            )
        } else {
            owner.clone()
        };
        if remote {
            connected(&origin, &owner).await;
        }
        owner
            .put("a".into(), vec![1; 9 * 1024 * 1024])
            .await
            .unwrap();
        owner
            .put("b".into(), vec![2; 9 * 1024 * 1024])
            .await
            .unwrap();
        let service = NodeRpcService::new(origin.clone(), Default::default()).unwrap();
        let response = call(
            &service,
            "KV",
            gets(vec!["a".into(), "b".into(), "a".into()]),
        )
        .await;
        assert_eq!(
            response.error.as_ref().unwrap().code,
            wire::ErrorCode::ResourceLimit as i32
        );
        assert!(response.reads.is_empty());
        drop(response);
        assert_eq!(origin.query_diagnostics().reserved_runtime_bytes, 0);
        let response = call(&service, "KV", gets(vec!["a".into()])).await;
        assert_eq!(
            response.reads[0].value.as_ref().unwrap().bytes.len(),
            9 * 1024 * 1024
        );
        assert_eq!(
            origin.query_diagnostics().reserved_runtime_bytes,
            64 * 1024 * 1024
        );
        drop(response);
        assert_eq!(origin.query_diagnostics().reserved_runtime_bytes, 0);
        service.shutdown(Duration::from_secs(2)).await.unwrap();
        origin.shutdown().await.unwrap();
        if remote {
            owner.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn ttl_results_and_duplicate_waves_match_local_and_remote_actor_semantics() {
    for remote in [false, true] {
        let map = HashMap::from([(FragmentId(0), vec!["ttl-owner".into()])]);
        let owner = Arc::new(
            Node::start(
                "ttl-owner".into(),
                "127.0.0.1:0".parse().unwrap(),
                config(map.clone()),
            )
            .await
            .unwrap(),
        );
        let origin = if remote {
            Arc::new(
                Node::start(
                    "ttl-router".into(),
                    "127.0.0.1:0".parse().unwrap(),
                    config(map),
                )
                .await
                .unwrap(),
            )
        } else {
            owner.clone()
        };
        if remote {
            connected(&origin, &owner).await;
        }
        let service = NodeRpcService::new(origin.clone(), Default::default()).unwrap();
        let response = call(
            &service,
            "KV",
            puts(vec![
                put("ttl", vec![1], Some(60000)),
                put("expired", vec![2], Some(0)),
                put("invalid", vec![3], Some(u64::MAX)),
                put("dup", vec![4], Some(60000)),
                put("dup", vec![5], None),
            ]),
        )
        .await;
        assert!(response.error.is_none());
        assert!(response.mutations[0].error.is_none());
        assert_eq!(
            response.mutations[1].error.as_ref().unwrap().code,
            wire::ErrorCode::Expired as i32
        );
        assert_eq!(
            response.mutations[2].error.as_ref().unwrap().code,
            wire::ErrorCode::Invalid as i32
        );
        assert!(response.mutations[3..]
            .iter()
            .all(|result| result.error.is_none()));
        drop(response);
        assert!(owner
            .get("ttl".into())
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms
            .is_some());
        assert!(owner.get("expired".into()).await.unwrap().is_none());
        assert!(owner.get("invalid".into()).await.unwrap().is_none());
        let duplicate = owner.get("dup".into()).await.unwrap().unwrap();
        assert_eq!(duplicate.value, vec![5]);
        assert!(duplicate.expires_at_ms.is_none());
        service.shutdown(Duration::from_secs(2)).await.unwrap();
        origin.shutdown().await.unwrap();
        if remote {
            owner.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn bounded_waves_do_not_shed_their_own_low_completion_capacity() {
    let node = Arc::new(
        Node::start_with_options(
            "batch-capacity".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::builder()
                .embedded_operation_capacity(1)
                .build(),
        )
        .await
        .unwrap(),
    );
    let service = NodeRpcService::new(node.clone(), Default::default()).unwrap();
    let mut values: Vec<_> = (0..300)
        .map(|i| put(&format!("key-{i}"), vec![1], None))
        .collect();
    values.extend([put("dup", vec![1], None), put("dup", vec![2], None)]);
    let response = call(&service, "KV", puts(values)).await;
    assert!(response.error.is_none());
    assert_eq!(response.mutations.len(), 302);
    assert!(response.mutations.iter().all(|row| row.error.is_none()));
    drop(response);
    assert_eq!(
        node.get("dup".into()).await.unwrap().unwrap().value,
        vec![2]
    );
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_mutation_reply_does_not_skip_later_admitted_waves() {
    let node = Arc::new(
        Node::start(
            "reply-overflow".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .default_pool(PoolConfig::builder().rate_limit_per_sec(0).build())
                .build(),
        )
        .await
        .unwrap(),
    );
    let service = NodeRpcService::new(node.clone(), Default::default()).unwrap();
    let values = (0..4096)
        .map(|index| {
            let key = format!("{index:04}{}", "k".repeat(4076));
            put(&key, Vec::new(), None)
        })
        .collect();
    let response = call(&service, "KV", puts(values)).await;
    assert_eq!(
        response.error.as_ref().unwrap().code,
        wire::ErrorCode::UnknownOutcome as i32
    );
    assert!(response.mutations.is_empty());
    drop(response);
    assert_eq!(
        node.health()
            .await
            .unwrap()
            .values()
            .map(|health| health.shed_counts.1)
            .sum::<u64>(),
        4096
    );
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    node.shutdown().await.unwrap();
}
