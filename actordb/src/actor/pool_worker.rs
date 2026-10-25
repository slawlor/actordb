// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Private managed worker with exact pool and generation reservations.

#[cfg(test)]
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef};

use super::fragment_state::FragmentState;
use super::kv_worker::handle_worker_message;
use super::pool::release_pending;
use super::protocol::{ApplyError, KvMessage};
/// Worker used internally by [`super::pool::WorkerPool`]. The pool-wide count enforces the
/// configured cap, while the slot-local count lets supervision reclaim only
/// the work abandoned by one failed worker.
pub(super) struct PoolWorker;

pub(super) struct PoolWorkerArgs {
    pub(super) shared: Arc<FragmentState>,
    pub(super) pool_pending: Arc<AtomicU64>,
    pub(super) slot_pending: Arc<AtomicU64>,
    #[cfg(test)]
    pub(super) startup_gate: Option<WorkerTestGate>,
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct WorkerTestGate {
    pub(crate) entered: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static WORKER_STARTUP_GATES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<String, WorkerTestGate>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(test)]
static WORKER_GET_GATES: std::sync::LazyLock<std::sync::Mutex<HashMap<String, WorkerTestGate>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(test)]
pub(super) fn install_worker_startup_gate(name: String) -> WorkerTestGate {
    let gate = WorkerTestGate::default();
    let replaced = WORKER_STARTUP_GATES
        .lock()
        .expect("worker startup gate registry poisoned")
        .insert(name, gate.clone());
    assert!(replaced.is_none(), "worker startup gate already installed");
    gate
}

#[cfg(test)]
pub(super) fn take_worker_startup_gate(name: &str) -> Option<WorkerTestGate> {
    WORKER_STARTUP_GATES
        .lock()
        .expect("worker startup gate registry poisoned")
        .remove(name)
}

#[cfg(test)]
pub(crate) fn install_worker_get_gate(key: String) -> WorkerTestGate {
    let gate = WorkerTestGate::default();
    let replaced = WORKER_GET_GATES
        .lock()
        .expect("worker Get gate registry poisoned")
        .insert(key, gate.clone());
    assert!(replaced.is_none(), "worker Get gate already installed");
    gate
}

#[cfg(test)]
pub(super) fn take_worker_get_gate(key: &str) -> Option<WorkerTestGate> {
    WORKER_GET_GATES
        .lock()
        .expect("worker Get gate registry poisoned")
        .remove(key)
}

impl Actor for PoolWorker {
    type Msg = KvMessage;
    type State = PoolWorkerArgs;
    type Arguments = PoolWorkerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        #[cfg(test)]
        if let Some(gate) = &args.startup_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (message, mut observation) = message.into_observed_worker();
        let result = async {
            let (message, _retention) = message.into_retained_parts();
            match message {
                KvMessage::ApplyOp {
                    op,
                    priority: _,
                    is_replication: _,
                    reply,
                } => {
                    let result = state.shared.apply(&op).await;
                    release_pool_reservation(state);
                    match result {
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
                                ApplyError::Backpressure
                                | ApplyError::InvalidInput(_)
                                | ApplyError::VersionConflict { .. } => None,
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
                    #[cfg(test)]
                    if let Some(gate) = take_worker_get_gate(&key) {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                    let value = state.shared.get(&key).await;
                    release_pool_reservation(state);
                    let _ = reply.send(Ok(value));
                    Ok(())
                }
                KvMessage::ContinueBatchRead(mut work) => {
                    let Some(key) = work.current_key() else {
                        release_pool_reservation(state);
                        work.finish();
                        return Ok(());
                    };
                    #[cfg(test)]
                    if let Some(gate) = take_worker_get_gate(key) {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                    let value = state.shared.get(key).await;
                    // Release before the dispatcher admits the next key. At a
                    // queue-cap boundary, forwarding first would make completed
                    // work look pending and spuriously shed the continuation.
                    release_pool_reservation(state);
                    work.push(Ok(value));
                    work.return_to_dispatcher();
                    Ok(())
                }
                message => {
                    // Dispatcher-only variants are never routed here. Keep the
                    // public KvWorker's defensive behavior if an internal bug
                    // does so, while still balancing the reservation.
                    let result = handle_worker_message(&state.shared, message).await;
                    release_pool_reservation(state);
                    result
                }
            }
        }
        .await;
        if let Some(observation) = observation.as_mut() {
            observation.complete();
        }
        result
    }
}

fn release_pool_reservation(state: &PoolWorkerArgs) {
    // The order is deliberately the inverse of admission (slot, then pool),
    // preserving the conservative invariant that the pool total never falls
    // below the sum of its live slot counters. Nothing may await between the
    // two updates and the result reply is sent only after both complete.
    release_pending(&state.slot_pending, 1);
    release_pending(&state.pool_pending, 1);
}
