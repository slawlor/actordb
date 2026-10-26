//! Deterministic aggregate peer-body ownership and liveness regressions.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::Notify;

use super::*;
use crate::wire::MAX_FRAME_BYTES;

fn envelope() -> Envelope {
    Envelope {
        msg: Some(EnvelopeMsg::BatchAck(BatchAck {
            session_id: 1,
            batch_seq: 1,
            fragment: 0,
            level: ReplicationAckLevel::Applied as i32,
        })),
    }
}
fn peer(
    budget: &Arc<NodePeerByteBudget>,
) -> (
    PeerChannel,
    mpsc::Receiver<BudgetedFrame>,
    mpsc::Receiver<BudgetedFrame>,
) {
    let (mut peer, data, responses) = super::tests::test_peer_channel(4);
    peer.outbound.node_byte_budget = budget.clone();
    (peer, data, responses)
}
async fn pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn aggregate_saturation_across_peers_releases_only_dropped_bodies() {
    let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
    let bytes = envelope().encoded_len();
    // Reserve most producer capacity without allocating a wide body. The two
    // actual small frames below share the remaining aggregate allowance.
    let held = budget
        .acquire_producer(
            (MAX_FRAME_BYTES - 2 * bytes) as u32,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let (a, mut a_data, _a_responses) = peer(&budget);
    let (b, mut b_data, _b_responses) = peer(&budget);
    assert!(try_send_envelope(&a, envelope()).await);
    assert!(try_send_envelope(&b, envelope()).await);
    assert_eq!(
        budget.diagnostics().producer_admitted_bytes(),
        MAX_FRAME_BYTES
    );
    let mut blocked = Box::pin(try_send_envelope(&a, envelope()));
    pending(blocked.as_mut()).await;
    assert_eq!(a_data.len(), 1);
    assert_eq!(b_data.len(), 1);
    assert!(budget.diagnostics().saturation_events() > 0);
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(!blocked.await);
    assert!(*a.cancel.borrow());
    // Cancellation alone does not free buffers still owned by this queue.
    assert_eq!(budget.diagnostics().admitted_bytes(), MAX_FRAME_BYTES);
    drop(a_data.recv().await.unwrap());
    assert_eq!(
        budget.diagnostics().admitted_bytes(),
        MAX_FRAME_BYTES - bytes
    );
    assert!(try_send_envelope(&b, envelope()).await);
    drop(held);
    drop(b_data.recv().await.unwrap());
    drop(b_data.recv().await.unwrap());
    assert_eq!(budget.diagnostics().admitted_bytes(), 0);
    assert_eq!(budget.diagnostics().producer_admitted_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn aggregate_producer_saturation_preserves_priority_response_reserve() {
    let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
    let held = budget
        .acquire_producer(
            MAX_FRAME_BYTES as u32,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let (a, a_data, _a_responses) = peer(&budget);
    let (b, _b_data, mut b_responses) = peer(&budget);
    let mut blocked = Box::pin(try_send_envelope(&a, envelope()));
    pending(blocked.as_mut()).await;
    assert_eq!(a_data.len(), 0);
    assert!(try_send_envelope_immediate(&b, envelope()));
    assert_eq!(
        budget.diagnostics().admitted_bytes(),
        MAX_FRAME_BYTES + envelope().encoded_len()
    );
    drop(b_responses.recv().await.unwrap());
    drop(blocked);
    assert_eq!(a.outbound.tx.capacity(), 4);
    assert_eq!(
        a.outbound.producer_byte_budget.available_permits(),
        MAX_FRAME_BYTES
    );
    assert_eq!(budget.diagnostics().admitted_bytes(), MAX_FRAME_BYTES);
    drop(held);
    assert_eq!(budget.diagnostics().admitted_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_shared_total_wait_releases_partial_global_admission() {
    let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
    let first_response = budget.try_response(MAX_FRAME_BYTES as u32).unwrap();
    let second_response = budget.try_response(MAX_FRAME_BYTES as u32).unwrap();
    let (a, a_data, _a_responses) = peer(&budget);
    let mut blocked = Box::pin(try_send_envelope(&a, envelope()));
    pending(blocked.as_mut()).await;
    assert_eq!(a_data.len(), 0);
    assert_eq!(
        budget.diagnostics().producer_admitted_bytes(),
        envelope().encoded_len()
    );
    drop(blocked);
    assert_eq!(budget.diagnostics().producer_admitted_bytes(), 0);
    assert_eq!(a.outbound.tx.capacity(), 4);
    drop(first_response);
    drop(second_response);
    assert_eq!(budget.diagnostics().admitted_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn closing_one_writer_wakes_its_global_waiter_and_keeps_shared_gates_open() {
    let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
    let held = budget
        .acquire_producer(
            MAX_FRAME_BYTES as u32,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let (a, a_data, _a_responses) = peer(&budget);
    let (b, mut b_data, _b_responses) = peer(&budget);
    let mut blocked = Box::pin(try_send_envelope(&a, envelope()));
    pending(blocked.as_mut()).await;
    drop(a_data);
    assert!(!blocked.await);
    assert!(*a.cancel.borrow());
    drop(held);
    assert!(try_send_envelope(&b, envelope()).await);
    drop(b_data.recv().await.unwrap());
    assert_eq!(budget.diagnostics().admitted_bytes(), 0);
}

struct PendingWriter {
    entered: Arc<Notify>,
}
impl AsyncWrite for PendingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.entered.notify_one();
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn writer_cancel_and_abort_release_queued_and_active_global_body_permits() {
    for abort in [false, true] {
        let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
        let (a, data, responses) = peer(&budget);
        assert!(try_send_envelope(&a, envelope()).await);
        assert!(try_send_envelope(&a, envelope()).await);
        assert!(try_send_envelope_immediate(&a, envelope()));
        let entered = Arc::new(Notify::new());
        let writer = tokio::spawn(run_bounded_peer_writer(
            PendingWriter {
                entered: entered.clone(),
            },
            PeerWriterPipeline {
                data_rx: data,
                response_rx: responses,
                byte_budget: a.outbound.byte_budget.clone(),
                producer_byte_budget: a.outbound.producer_byte_budget.clone(),
            },
            a.cancel.subscribe(),
            a.cancel.clone(),
            Duration::from_secs(30),
        ));
        entered.notified().await;
        assert_eq!(
            budget.diagnostics().admitted_bytes(),
            3 * envelope().encoded_len()
        );
        if abort {
            writer.abort();
            assert!(writer.await.unwrap_err().is_cancelled());
        } else {
            a.cancel.send_replace(true);
            writer.await.unwrap();
        }
        assert_eq!(budget.diagnostics().admitted_bytes(), 0);
        assert_eq!(budget.diagnostics().producer_admitted_bytes(), 0);
        let (b, mut b_data, _b_responses) = peer(&budget);
        assert!(try_send_envelope(&b, envelope()).await);
        drop(b_data.recv().await.unwrap());
    }
}

#[test]
fn aggregate_writer_budget_validation_rejects_impossible_limits() {
    assert!(NodeRuntimeOptions::builder()
        .node_peer_queue_byte_capacity(2 * MAX_FRAME_BYTES - 1)
        .build()
        .validate()
        .is_err());
    assert!(NodeRuntimeOptions::builder()
        .node_peer_queue_byte_capacity(2 * MAX_FRAME_BYTES)
        .build()
        .validate()
        .is_ok());
    if let Some(too_large) = (u32::MAX as usize).checked_add(1) {
        assert!(NodeRuntimeOptions::builder()
            .node_peer_queue_byte_capacity(too_large)
            .build()
            .validate()
            .is_err());
    }
    assert!(NodeRuntimeOptions::builder()
        .node_peer_queue_byte_capacity(Semaphore::MAX_PERMITS + 1)
        .build()
        .validate()
        .is_err());
}

#[tokio::test]
async fn producer_and_response_charge_encoded_body_until_queue_release() {
    let budget = NodePeerByteBudget::new(2 * MAX_FRAME_BYTES);
    let (channel, mut data, mut responses) = peer(&budget);
    let bytes = envelope().encoded_len();
    assert!(try_send_envelope(&channel, envelope()).await);
    assert!(try_send_envelope_immediate(&channel, envelope()));
    assert_eq!(budget.diagnostics().admitted_bytes(), 2 * (bytes));
    assert_eq!(budget.diagnostics().producer_admitted_bytes(), bytes);
    let frame = data.recv().await.unwrap();
    assert_eq!(frame.len(), bytes);
    drop(frame);
    assert_eq!(budget.diagnostics().admitted_bytes(), bytes);
    drop(responses.recv().await.unwrap());
    assert_eq!(budget.diagnostics().admitted_bytes(), 0);
}
