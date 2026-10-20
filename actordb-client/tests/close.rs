use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use actordb_client::{Client, ClientError, ClientOptions};
use actordb_protocol::{client as wire, CLIENT_CAPABILITIES, CLIENT_PROTOCOL_VERSION};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

#[derive(Default)]
struct State {
    dropped: AtomicBool,
    shutdown_started: AtomicBool,
    block_writes: AtomicBool,
    write_started: AtomicBool,
}

struct ControlledStream {
    stream: DuplexStream,
    state: Arc<State>,
}
impl Drop for ControlledStream {
    fn drop(&mut self) {
        self.state.dropped.store(true, Ordering::SeqCst);
    }
}
impl AsyncRead for ControlledStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for ControlledStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.state.write_started.store(true, Ordering::SeqCst);
        if self.state.block_writes.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.state.shutdown_started.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}

async fn receive(remote: &mut DuplexStream) -> wire::Frame {
    let length = remote.read_u32().await.unwrap();
    let mut body = vec![0; length as usize];
    remote.read_exact(&mut body).await.unwrap();
    wire::Frame::decode(body.as_slice()).unwrap()
}
async fn send(remote: &mut DuplexStream, message: wire::frame::Message) {
    let body = wire::Frame {
        message: Some(message),
    }
    .encode_to_vec();
    remote.write_u32(body.len() as u32).await.unwrap();
    remote.write_all(&body).await.unwrap();
}
async fn client() -> (Client, DuplexStream, Arc<State>) {
    let (stream, mut remote) = tokio::io::duplex(1024);
    let state = Arc::new(State::default());
    let handshake = async {
        assert!(matches!(
            receive(&mut remote).await.message,
            Some(wire::frame::Message::Hello(_))
        ));
        send(
            &mut remote,
            wire::frame::Message::Hello(wire::Hello {
                identity: "server".into(),
                challenge: vec![1; 32],
                min_version: CLIENT_PROTOCOL_VERSION,
                max_version: CLIENT_PROTOCOL_VERSION,
                capabilities: CLIENT_CAPABILITIES,
            }),
        )
        .await;
        assert!(matches!(
            receive(&mut remote).await.message,
            Some(wire::frame::Message::Auth(_))
        ));
        send(
            &mut remote,
            wire::frame::Message::Auth(wire::Auth { proof: vec![] }),
        )
        .await;
    };
    let (client, ()) = tokio::join!(
        Client::from_stream(
            ControlledStream {
                stream,
                state: state.clone(),
            },
            ClientOptions {
                timeout: Duration::from_secs(1),
                ..Default::default()
            }
        ),
        handshake
    );
    (client.unwrap(), remote, state)
}
async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn close_drops_transport_at_total_shutdown_deadline() {
    let (client, _remote, state) = client().await;
    let close = client.close();
    tokio::pin!(close);
    assert_pending(close.as_mut()).await;
    assert!(state.shutdown_started.load(Ordering::SeqCst));
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_pending(close.as_mut()).await;
    assert!(!state.dropped.load(Ordering::SeqCst));
    tokio::time::advance(Duration::from_millis(1)).await;
    close.await;
    assert!(state.dropped.load(Ordering::SeqCst));
    assert!(matches!(
        client.get("closed").await,
        Err(ClientError::Closed)
    ));
    client.close().await;
}

#[tokio::test(start_paused = true)]
async fn close_cancels_sent_mutation_and_queued_requests() {
    let (client, mut remote, state) = client().await;
    let active = client.put("active", vec![1]);
    tokio::pin!(active);
    assert_pending(active.as_mut()).await;
    assert!(matches!(
        receive(&mut remote).await.message,
        Some(wire::frame::Message::Request(_))
    ));
    let queued = client.put("queued", vec![2]);
    tokio::pin!(queued);
    assert_pending(queued.as_mut()).await;
    let ((), active, queued) = tokio::join!(client.close(), active, queued);
    assert!(matches!(active, Err(ClientError::UnknownOutcome(_))));
    assert!(matches!(queued, Err(ClientError::Closed)));
    assert!(state.dropped.load(Ordering::SeqCst));
    assert!(!state.shutdown_started.load(Ordering::SeqCst));
    assert_eq!(remote.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn close_cancels_backpressured_mutation_without_waiting_for_peer() {
    let (client, _remote, state) = client().await;
    state.block_writes.store(true, Ordering::SeqCst);
    state.write_started.store(false, Ordering::SeqCst);
    let active = client.put("blocked", vec![1]);
    tokio::pin!(active);
    assert_pending(active.as_mut()).await;
    assert!(state.write_started.load(Ordering::SeqCst));
    let ((), result) = tokio::join!(client.close(), active);
    assert!(matches!(result, Err(ClientError::UnknownOutcome(_))));
    assert!(state.dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn close_cancels_active_read_with_closed_outcome() {
    let (client, _remote, state) = client().await;
    let active = client.get("missing");
    tokio::pin!(active);
    assert_pending(active.as_mut()).await;
    let ((), result) = tokio::join!(client.close(), active);
    assert!(matches!(result, Err(ClientError::Closed)));
    assert!(state.dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn zero_budget_and_cancelled_close_drop_owned_transport() {
    let (first, _remote, first_state) = client().await;
    first.close_with_timeout(Duration::ZERO).await;
    assert!(first_state.dropped.load(Ordering::SeqCst));
    assert!(!first_state.shutdown_started.load(Ordering::SeqCst));
    let (second, _remote, second_state) = client().await;
    let mut close = Box::pin(second.close());
    assert_pending(close.as_mut()).await;
    drop(close);
    assert!(second_state.dropped.load(Ordering::SeqCst));
    second.close().await;
}
