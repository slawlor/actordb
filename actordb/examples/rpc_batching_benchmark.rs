//! Fixed loopback dataset; run the identical example on baseline and candidate.
//! cargo run -p actordb --no-default-features --release --example rpc_batching_benchmark -- after
use actordb::actor::PoolConfig;
use actordb::network::{BindFuture, BoxStream, ConnectFuture, Network, TcpNetwork};
use actordb::node::{Node, NodeConfig};
use actordb::rpc::{NodeRpcService, RpcRequest, RpcService};
use actordb::shard::FragmentId;
use actordb_protocol::client as wire;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct Counter(Arc<AtomicUsize>);
struct Stream {
    inner: BoxStream,
    writes: Arc<AtomicUsize>,
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_read(cx, out)
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = self.inner.as_mut().poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Ok(_))) {
            self.writes.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}
impl Network for Counter {
    fn bind(&self, addr: std::net::SocketAddr) -> BindFuture<'_> {
        Box::pin(async move { TcpNetwork.bind(addr).await })
    }
    fn connect(&self, addr: std::net::SocketAddr) -> ConnectFuture<'_> {
        let writes = self.0.clone();
        Box::pin(async move {
            Ok(Box::pin(Stream {
                inner: TcpNetwork.connect(addr).await?,
                writes,
            }) as BoxStream)
        })
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let label = std::env::args().nth(1).unwrap_or_else(|| "run".into());
    println!("label,rows,operation,trial,micros,peer_write_polls");
    let map = HashMap::from([(FragmentId(0), vec!["bench-owner".into()])]);
    let cfg = || {
        NodeConfig::builder()
            .fragment_map(map.clone())
            .default_pool(PoolConfig::builder().workers(4).build())
            .background_pool(PoolConfig::builder().workers(2).build())
            .build()
    };
    let owner = Arc::new(
        Node::start("bench-owner".into(), "127.0.0.1:0".parse().unwrap(), cfg())
            .await
            .unwrap(),
    );
    let writes = Arc::new(AtomicUsize::new(0));
    let mut options = cfg();
    options.network = Arc::new(Counter(writes.clone()));
    let router = Arc::new(
        Node::start(
            "bench-router".into(),
            "127.0.0.1:0".parse().unwrap(),
            options,
        )
        .await
        .unwrap(),
    );
    router.connect(owner.addr()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while router.peer_count().await != 1 || owner.peer_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let remote = NodeRpcService::new(router.clone(), Default::default()).unwrap();
    let local = NodeRpcService::new(owner.clone(), Default::default()).unwrap();
    for count in [64, 256, 1024, 4096] {
        let keys: Vec<_> = (0..count).map(|i| format!("bench:{i:04}")).collect();
        let setup: Vec<_> = keys.iter().map(|key| (key.clone(), vec![7; 16])).collect();
        assert!(owner.put_multi(setup).await.iter().all(Result::is_ok));
        for operation in ["remote_get", "remote_put", "local_get"] {
            for trial in 0..4 {
                let operation_data = if operation == "remote_put" {
                    wire::request::Operation::BatchPut(wire::BatchPut {
                        puts: keys
                            .iter()
                            .map(|key| wire::Put {
                                key: key.clone(),
                                value: vec![7; 16],
                                ..Default::default()
                            })
                            .collect(),
                        priority: 0,
                    })
                } else {
                    wire::request::Operation::BatchGet(wire::BatchGet {
                        keys: keys.clone(),
                        priority: 0,
                    })
                };
                let request = RpcRequest::new(wire::Request {
                    table: "KV".into(),
                    operation: Some(operation_data),
                });
                writes.store(0, Ordering::Relaxed);
                let begin = Instant::now();
                let response = if operation == "local_get" {
                    local.call(request).await.unwrap()
                } else {
                    remote.call(request).await.unwrap()
                };
                let micros = begin.elapsed().as_micros();
                let polls = writes.load(Ordering::Relaxed);
                assert!(response.error.is_none());
                if operation == "remote_put" {
                    assert_eq!(response.mutations.len(), count);
                    assert!(response.mutations.iter().all(|row| row.error.is_none()));
                } else {
                    assert_eq!(response.reads.len(), count);
                    assert!(response.reads.iter().all(|row| row
                        .value
                        .as_ref()
                        .is_some_and(|value| value.bytes == vec![7; 16])));
                }
                drop(response);
                if trial != 0 {
                    println!("{label},{count},{operation},{trial},{micros},{polls}");
                }
            }
        }
    }
    remote.shutdown(Duration::from_secs(2)).await.unwrap();
    local.shutdown(Duration::from_secs(2)).await.unwrap();
    router.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}
