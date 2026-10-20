// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Public low-level fragment worker and its request handlers.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use super::fragment_state::FragmentState;
use super::pool::release_pending;
use super::protocol::{ApplyError, BatchReadEntry, BatchReadPage, KvMessage};
use crate::wire::{batch_get_result_encoded_len, RepeatedFieldSizer};
/// A fragment's worker: applies ops and serves reads against the shared
/// [`FragmentState`]. One worker's mailbox is FIFO, and sticky dispatch
/// sends each key to one worker within a priority pool. Cross-pool LWW merges
/// are serialized by the shared state while different keys execute
/// concurrently.
pub struct KvWorker;

/// Arguments to spawn a [KvWorker].
#[derive(Clone, bon::Builder)]
pub struct KvWorkerArgs {
    /// The fragment state this worker shares with its siblings.
    pub shared: Arc<FragmentState>,
    /// Pending-op counter for the pool (decremented after each op).
    /// `None` for unsupervised test workers.
    pub pending: Option<Arc<AtomicU64>>,
}

impl Actor for KvWorker {
    type Msg = KvMessage;
    type State = (Arc<FragmentState>, Option<Arc<AtomicU64>>);
    type Arguments = KvWorkerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok((args.shared, args.pending))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let result = handle_worker_message(&state.0, message).await;
        // Release the optional caller-owned pending slot. Production pools
        // use `PoolWorker`, which also tracks the exact worker slot so
        // supervision can reclaim queued work discarded by a failed worker.
        if let Some(pending) = &state.1 {
            release_pending(pending, 1);
        }
        result
    }
}

async fn serve_worker_batch(
    shared: &FragmentState,
    keys: Vec<String>,
    response_base_payload_len: usize,
    response_soft_limit: usize,
    reply: RpcReplyPort<BatchReadPage>,
) {
    let mut entries = Vec::with_capacity(keys.len().min(256));
    let mut response_sizing = RepeatedFieldSizer::new(17, 2, response_base_payload_len);
    for key in keys {
        let value = shared.get(&key).await;
        response_sizing.push(batch_get_result_encoded_len(&key, value.as_ref(), false));
        entries.push(BatchReadEntry {
            key,
            result: Ok(value),
        });
        if response_sizing.encoded_len() > response_soft_limit {
            break;
        }
    }
    let _ = reply.send(BatchReadPage { entries });
}

pub(super) async fn handle_worker_message(
    shared: &Arc<FragmentState>,
    message: KvMessage,
) -> Result<(), ActorProcessingErr> {
    match message {
        KvMessage::ApplyOp {
            op,
            priority: _,
            is_replication: _,
            reply,
        } => {
            // Cache merge and durable enqueue are one ordered operation.
            // A DB error fails the actor loudly rather than diverging
            // silently.
            match shared.apply(&op).await {
                Ok(outcome) => {
                    if let Some(reply) = reply {
                        let _ = reply.send(Ok(outcome));
                    }
                    Ok(())
                }
                Err(error) => {
                    let fatal = match &error {
                        ApplyError::Storage(message) => {
                            Some(ActorProcessingErr::from(message.clone()))
                        }
                        ApplyError::Backpressure | ApplyError::InvalidInput(_) => None,
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(error));
                    }
                    match fatal {
                        Some(error) => Err(error),
                        None => Ok(()),
                    }
                }
            }
        }
        KvMessage::Get {
            key,
            reply,
            priority: _,
        } => {
            // The receiver may be gone (e.g. timed-out caller); that's fine.
            let _ = reply.send(Ok(shared.get(&key).await));
            Ok(())
        }
        KvMessage::Scan { spec, reply } => {
            // Worker-executed (unlike the dispatcher-served GetAll family
            // below): the scan runs against shared state inside the worker's
            // message handler. The receiver may be gone; that's fine.
            let _ = reply.send(Ok(shared.scan(&spec).await));
            Ok(())
        }
        KvMessage::ScanRetained {
            spec,
            retention,
            reply,
        } => {
            let _retention = retention;
            if !reply.is_closed() {
                let _ = reply.send(Ok(shared.scan(&spec).await));
            }
            Ok(())
        }
        KvMessage::QueryPage { work, reply } => {
            if !reply.is_closed() {
                let _retention = &work.retention;
                let _ = reply.send(shared.query_page(&work).await);
            }
            Ok(())
        }
        KvMessage::GetBatch {
            keys,
            response_base_payload_len,
            response_soft_limit,
            reply,
            priority: _,
        } => {
            // `KvWorker` remains a public low-level actor. It has no managed
            // pool admission, so serve the same ordered byte-bounded shape
            // directly. Production `KvActor` routes one admitted key at a
            // time through `ContinueBatchRead` below.
            serve_worker_batch(
                shared,
                keys,
                response_base_payload_len,
                response_soft_limit,
                reply,
            )
            .await;
            Ok(())
        }
        KvMessage::GetBatchRetained(request) => {
            let _retention = request.retention;
            serve_worker_batch(
                shared,
                request.keys,
                request.response_base_payload_len,
                request.response_soft_limit,
                request.reply,
            )
            .await;
            Ok(())
        }
        KvMessage::ContinueBatchRead(mut work) => {
            let Some(key) = work.current_key() else {
                work.finish();
                return Ok(());
            };
            let value = shared.get(key).await;
            work.push(Ok(value));
            work.return_to_dispatcher();
            Ok(())
        }
        // Flush fences are handled by the dispatcher against the shared
        // state. They are never routed through a worker pool.
        KvMessage::Flush { reply } | KvMessage::FlushStable { reply } => {
            let _ = reply.send(Err(ApplyError::Storage(
                "flush fence was routed to a worker".to_string(),
            )));
            Ok(())
        }
        // Snapshot queries are served by the dispatcher straight from
        // shared state.
        KvMessage::GetAll { .. }
        | KvMessage::GetSnapshot { .. }
        | KvMessage::GetSummaryPage { .. }
        | KvMessage::GetVersioned { .. }
        | KvMessage::GetVersionedPage { .. }
        | KvMessage::GetRevision { .. } => Ok(()),
        // Shed counts are a dispatcher-level query; workers don't track them.
        KvMessage::GetShedCounts { reply } => {
            let _ = reply.send((0, 0, 0));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    // Copyright (c) Sean Lawlor
    //
    // This source code is licensed under both the MIT license found in the
    // LICENSE-MIT file in the root directory of this source tree.

    //! Low-level worker APIs, retained reads, and input rejection.

    use super::*;
    use crate::actor::test_support::{test_op, test_state};
    use crate::actor::{ApplyOutcome, OpPriority, ReplicatedOp, RetainedBatchRead, VersionedValue};
    use ractor::rpc::CallResult;
    use std::collections::HashMap;
    use std::time::Duration;
    #[tokio::test]
    async fn worker_without_pending_counter_serves_operations() {
        let shared = Arc::new(test_state(HashMap::new()));
        let (worker, handle) = KvWorker::spawn(
            None,
            KvWorker,
            KvWorkerArgs::builder().shared(shared.clone()).build(),
        )
        .await
        .unwrap();
        let value = VersionedValue::new(b"built".to_vec(), 10, "writer".into());
        assert_eq!(
            worker
                .call(
                    |reply| KvMessage::ApplyOp {
                        op: ReplicatedOp {
                            key: "built".into(),
                            value: value.clone(),
                            seq: 1,
                            boot_id: 1
                        },
                        priority: OpPriority::Default,
                        is_replication: false,
                        reply: Some(reply),
                    },
                    Some(Duration::from_secs(2))
                )
                .await
                .unwrap(),
            CallResult::Success(Ok(ApplyOutcome::Applied))
        );
        assert_eq!(shared.get("built").await, Some(value));
        worker.drain().unwrap();
        handle.await.unwrap();
    }
    #[tokio::test]
    async fn direct_worker_batch_read_preserves_positions_and_exact_response_boundary() {
        use crate::proto::{BatchGetResponse, BatchKeyValue, Envelope, EnvelopeMsg};
        use prost::Message;

        let value = VersionedValue::new(vec![7; 128], 128, "n".to_string());
        let shared = Arc::new(test_state(HashMap::from([(
            "present".to_string(),
            value.clone(),
        )])));
        let (worker, handle) = KvWorker::spawn(
            None,
            KvWorker,
            KvWorkerArgs {
                shared,
                pending: None,
            },
        )
        .await
        .unwrap();

        // Measure with Prost, including the nonzero request id and nested
        // envelope, rather than the actor's incremental sizing helper.
        let response = BatchGetResponse {
            req_id: u64::MAX,
            results: Vec::new(),
        };
        let response_base_payload_len = response.encoded_len();
        let mut singleton = response;
        singleton.results.push(BatchKeyValue {
            key: "present".to_string(),
            found: true,
            value: value.value.clone(),
            timestamp_ms: value.timestamp_ms,
            logical_counter: value.logical_counter,
            node_id: value.node_id.clone(),
            ..BatchKeyValue::default()
        });
        let singleton_response_len = Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(singleton)),
        }
        .encoded_len();
        let keys = vec![
            "present".to_string(),
            "m".to_string(),
            "present".to_string(),
            "m".to_string(),
        ];
        let read = |keys: Vec<String>, response_soft_limit| {
            worker.call(
                move |reply| KvMessage::GetBatch {
                    keys,
                    response_base_payload_len,
                    response_soft_limit,
                    reply,
                    priority: OpPriority::Background,
                },
                Some(Duration::from_secs(2)),
            )
        };

        let present = BatchReadEntry {
            key: "present".to_string(),
            result: Ok(Some(value)),
        };
        let missing = BatchReadEntry {
            key: "m".to_string(),
            result: Ok(None),
        };
        let CallResult::Success(below_boundary) = read(keys.clone(), singleton_response_len - 1)
            .await
            .unwrap()
        else {
            panic!("direct worker did not return the crossing singleton");
        };
        assert_eq!(below_boundary.entries, vec![present.clone()]);

        // Equality does not end the page: the next miss crosses the target
        // and is included once. The duplicate positions remain in the suffix.
        let CallResult::Success(first) = read(keys.clone(), singleton_response_len).await.unwrap()
        else {
            panic!("direct worker did not return the first prefix");
        };
        assert_eq!(first.entries, vec![present.clone(), missing.clone()]);
        let suffix = keys.into_iter().skip(first.entries.len()).collect();
        let CallResult::Success(second) = read(suffix, singleton_response_len).await.unwrap()
        else {
            panic!("direct worker did not return the resumed suffix");
        };
        assert_eq!(second.entries, vec![present.clone(), missing]);

        let retention = Arc::new(());
        let released = Arc::downgrade(&retention);
        let retained = worker
            .call(
                move |reply| {
                    KvMessage::GetBatchRetained(RetainedBatchRead::new(
                        vec!["present".to_string(), "m".to_string()],
                        response_base_payload_len,
                        singleton_response_len - 1,
                        reply,
                        OpPriority::Background,
                        retention,
                    ))
                },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(
            retained,
            CallResult::Success(BatchReadPage {
                entries: vec![present]
            })
        );

        let empty = read(Vec::new(), 0).await.unwrap();
        assert_eq!(
            empty,
            CallResult::Success(BatchReadPage {
                entries: Vec::new()
            })
        );
        assert!(
            released.upgrade().is_none(),
            "low-level worker retained a completed request lease"
        );
        worker.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_wire_record_apply_is_nonfatal_to_worker() {
        let shared = Arc::new(test_state(HashMap::new()));
        let (worker, handle) = KvWorker::spawn(
            None,
            KvWorker,
            KvWorkerArgs {
                shared: shared.clone(),
                pending: None,
            },
        )
        .await
        .unwrap();

        let oversized_key = "k".repeat(crate::wire::MAX_FRAME_BYTES / 2);
        let invalid = worker
            .call(
                |reply| KvMessage::ApplyOp {
                    op: test_op(
                        &oversized_key,
                        VersionedValue::new(Vec::new(), 1, "node".to_string()),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            invalid,
            CallResult::Success(Err(ApplyError::InvalidInput(_)))
        ));

        for node_id in [
            String::new(),
            "n".repeat(crate::wire::MAX_WIRE_NODE_ID_BYTES + 1),
        ] {
            let invalid = worker
                .call(
                    move |reply| KvMessage::ApplyOp {
                        op: test_op(
                            "invalid-provenance",
                            VersionedValue::new(Vec::new(), 2, node_id),
                        ),
                        priority: OpPriority::Default,
                        is_replication: false,
                        reply: Some(reply),
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(matches!(
                invalid,
                CallResult::Success(Err(ApplyError::InvalidInput(_)))
            ));
            assert!(shared.get("invalid-provenance").await.is_none());
        }

        let valid = worker
            .call(
                |reply| KvMessage::ApplyOp {
                    op: test_op(
                        "healthy",
                        VersionedValue::new(b"value".to_vec(), 3, "node".to_string()),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(valid, CallResult::Success(Ok(ApplyOutcome::Applied)));
        assert_eq!(
            shared.get("healthy").await.unwrap().value,
            b"value".to_vec()
        );

        worker.stop(None);
        handle.await.unwrap();
    }
}
