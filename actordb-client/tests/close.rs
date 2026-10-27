use actordb_client::{
    Client, ClientError, ClientOptions, RpcCallOptions, RpcClient, RpcError, RpcFuture, RpcReply,
};
use actordb_protocol::client as wire;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

struct PendingRpc {
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}
impl RpcClient for PendingRpc {
    fn call(
        &self,
        _: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}
fn fixture() -> (Client, Arc<AtomicUsize>, Arc<tokio::sync::Notify>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let client = Client::from_rpc(
        Arc::new(PendingRpc {
            calls: calls.clone(),
            entered: entered.clone(),
        }),
        ClientOptions::builder()
            .max_queued_requests(2)
            .max_response_memory_bytes(actordb_protocol::DEFAULT_MAX_DECODED_RESPONSE_BYTES)
            .timeout(Duration::from_secs(2))
            .build(),
    )
    .unwrap();
    (client, calls, entered)
}
#[tokio::test]
async fn close_marks_sent_mutation_unknown_but_unsent_queued_read_closed() {
    let (client, calls, entered) = fixture();
    let active = client.clone();
    let write = tokio::spawn(async move { active.put("accepted", vec![1]).await });
    entered.notified().await;
    let queued = client.clone();
    let read = tokio::spawn(async move { queued.get("queued").await });
    tokio::task::yield_now().await;
    client.close().await;
    assert!(matches!(
        write.await.unwrap(),
        Err(ClientError::UnknownOutcome(_))
    ));
    assert!(matches!(read.await.unwrap(), Err(ClientError::Closed)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn close_cancels_active_read_and_shared_table_views() {
    let (client, _, entered) = fixture();
    let table = client.table("users").unwrap();
    let reading = tokio::spawn(async move { table.get("waiting").await });
    entered.notified().await;
    client.close().await;
    assert!(matches!(reading.await.unwrap(), Err(ClientError::Closed)));
    assert!(matches!(
        client.get("after").await,
        Err(ClientError::Closed)
    ));
}
#[tokio::test]
async fn cancel_reclaims_admission_without_retiring_a_shared_rpc_adapter() {
    let (client, calls, entered) = fixture();
    let active = client.clone();
    let reading = tokio::spawn(async move { active.get("cancelled").await });
    entered.notified().await;
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    let next = client.clone();
    let reading = tokio::spawn(async move { next.get("next").await });
    entered.notified().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    client.close().await;
    assert!(matches!(reading.await.unwrap(), Err(ClientError::Closed)));
}
