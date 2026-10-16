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
mod tests;
