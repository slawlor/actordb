//! Exact bulk outcome classification across real TCP lost-reply/failover faults.
use actordb::network::{BindFuture, BoxStream, ConnectFuture, Network, TcpNetwork};
use actordb::node::{Node, NodeConfig, NodeError, NodeRuntimeOptions};
use actordb::shard::FragmentId;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct DropReplies {
    target: std::net::SocketAddr,
    enabled: Arc<AtomicBool>,
}
struct ReplyFilter {
    stream: BoxStream,
    enabled: Arc<AtomicBool>,
}
impl AsyncRead for ReplyFilter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.enabled.load(Ordering::Acquire) {
            return self.stream.as_mut().poll_read(cx, out);
        }
        let mut scratch = [0; 16_384];
        let mut discarded = ReadBuf::new(&mut scratch);
        match self.stream.as_mut().poll_read(cx, &mut discarded) {
            Poll::Ready(Ok(())) if !discarded.filled().is_empty() => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            result => result,
        }
    }
}
impl AsyncWrite for ReplyFilter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.stream.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.stream.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.stream.as_mut().poll_shutdown(cx)
    }
}
impl Network for DropReplies {
    fn bind(&self, addr: std::net::SocketAddr) -> BindFuture<'_> {
        Box::pin(async move { TcpNetwork.bind(addr).await })
    }
    fn connect(&self, addr: std::net::SocketAddr) -> ConnectFuture<'_> {
        let enabled = self.enabled.clone();
        let filtered = addr == self.target;
        Box::pin(async move {
            let stream = TcpNetwork.connect(addr).await?;
            Ok(if filtered {
                Box::pin(ReplyFilter { stream, enabled }) as BoxStream
            } else {
                stream
            })
        })
    }
}

async fn wait_for(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lost_bulk_ack_reports_unknown_but_uncontacted_owner_remains_network_error() {
    let map = HashMap::from([(FragmentId(0), vec!["bulk-owner".into()])]);
    let owner = Node::start(
        "bulk-owner".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder().fragment_map(map.clone()).build(),
    )
    .await
    .unwrap();
    let enabled = Arc::new(AtomicBool::new(false));
    let origin = Node::start_with_options(
        "bulk-origin".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_map(map)
            .network(Arc::new(DropReplies {
                target: owner.addr(),
                enabled: enabled.clone(),
            }))
            .build(),
        NodeRuntimeOptions::builder()
            .forwarded_attempt_timeout(Duration::from_millis(200))
            .build(),
    )
    .await
    .unwrap();
    let unsent = origin.put_multi(vec![("unsent".into(), vec![0])]).await;
    assert!(matches!(&unsent[0], Err(NodeError::Network(_))));
    origin.connect(owner.addr()).await.unwrap();
    wait_for(async || origin.peer_count().await == 1 && owner.peer_count().await == 1).await;
    origin.put("baseline".into(), vec![0]).await.unwrap();
    enabled.store(true, Ordering::Release);
    let results = origin
        .table("USERS")
        .unwrap()
        .put_multi(vec![
            ("duplicate".into(), vec![1]),
            ("duplicate".into(), vec![2]),
            ("other".into(), vec![3]),
        ])
        .await;
    assert_eq!(results.len(), 3);
    assert!(
        results
            .iter()
            .all(|result| matches!(result, Err(NodeError::UnknownOutcome(_)))),
        "{results:?}"
    );
    assert_eq!(
        owner
            .table("USERS")
            .unwrap()
            .get("duplicate")
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![2]
    );
    assert_eq!(
        owner
            .table("USERS")
            .unwrap()
            .get("other")
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![3]
    );
    enabled.store(false, Ordering::Release);
    origin.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_first_ack_preserves_each_position_across_success_and_later_rejection() {
    let map = HashMap::from([(FragmentId(0), vec!["bulk-first".into(), "bulk-next".into()])]);
    let first = Arc::new(
        Node::start(
            "bulk-first".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().fragment_map(map.clone()).build(),
        )
        .await
        .unwrap(),
    );
    let next = Node::start(
        "bulk-next".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder().fragment_map(map.clone()).build(),
    )
    .await
    .unwrap();
    let enabled = Arc::new(AtomicBool::new(false));
    let origin = Arc::new(
        Node::start_with_options(
            "bulk-failover".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .fragment_map(map)
                .network(Arc::new(DropReplies {
                    target: first.addr(),
                    enabled: enabled.clone(),
                }))
                .build(),
            NodeRuntimeOptions::builder()
                .forwarded_attempt_timeout(Duration::from_millis(500))
                .build(),
        )
        .await
        .unwrap(),
    );
    origin.connect(first.addr()).await.unwrap();
    origin.connect(next.addr()).await.unwrap();
    wait_for(async || {
        origin.peer_count().await == 2
            && first.peer_count().await == 1
            && next.peer_count().await == 1
    })
    .await;
    enabled.store(true, Ordering::Release);
    let writing = origin.clone();
    let job = tokio::spawn(async move {
        writing
            .put_multi(vec![
                ("reject".into(), vec![1]),
                ("duplicate".into(), vec![2]),
                ("duplicate".into(), vec![3]),
            ])
            .await
    });
    wait_for(async || {
        first
            .get("duplicate".into())
            .await
            .ok()
            .flatten()
            .is_some_and(|value| value.value == vec![3])
    })
    .await;
    // First has applied the batch. A later fresh write on the other holder
    // makes only the first original position lose its exact-stamp retry.
    tokio::time::sleep(Duration::from_millis(2)).await;
    next.put("reject".into(), vec![9]).await.unwrap();
    let results = job.await.unwrap();
    assert_eq!(results.len(), 3);
    assert!(
        matches!(&results[0], Err(NodeError::UnknownOutcome(_))),
        "{results:?}"
    );
    assert!(results[1].is_ok() && results[2].is_ok(), "{results:?}");
    assert_eq!(
        first.get("reject".into()).await.unwrap().unwrap().value,
        vec![1]
    );
    assert_eq!(
        next.get("reject".into()).await.unwrap().unwrap().value,
        vec![9]
    );
    assert_eq!(
        next.get("duplicate".into()).await.unwrap().unwrap().value,
        vec![3]
    );
    enabled.store(false, Ordering::Release);
    origin.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    next.shutdown().await.unwrap();
}
