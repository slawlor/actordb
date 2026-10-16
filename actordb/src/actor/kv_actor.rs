// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Per-fragment dispatcher and supervision of priority worker pools.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures::FutureExt;
use ractor::{Actor, ActorProcessingErr, ActorRef, ActorStatus, SupervisionEvent};

use super::fragment_state::FragmentState;
use super::pool::{PoolConfig, WorkerPool};
#[cfg(test)]
use super::pool_worker::take_worker_startup_gate;
use super::pool_worker::{PoolWorker, PoolWorkerArgs};
use super::protocol::{ApplyError, BatchReadContinuation, KvMessage, OpPriority};
use crate::shard::FragmentId;
/// Arguments to spawn a [KvActor].
/// State, fragment identity and the unique worker prefix remain required;
/// omitted pool settings match [`crate::node::NodeConfig`] defaults.
///
/// ```compile_fail
/// use actordb::{actor::{FragmentState, KvArgs}, shard::FragmentId};
/// use std::{collections::HashMap, sync::Arc, time::Duration};
/// let shared = Arc::new(FragmentState::new(
///     HashMap::new(), None, 1000, Duration::from_millis(100),
/// ));
/// // A builder cannot invent the dispatcher's unique worker names.
/// let args = KvArgs::builder().shared(shared).fragment(FragmentId(0)).build();
/// ```
#[derive(Clone, bon::Builder)]
pub struct KvArgs {
    /// The fragment's shared state, created by the supervisor (which
    /// also hands it to the cleaner and snapshotter).
    pub shared: Arc<FragmentState>,
    /// Owning node id, for unique worker naming.
    /// Which fragment this actor serves. Used for debug output.
    pub fragment: FragmentId,
    /// Prefix for worker actor names (must be unique per dispatcher
    /// in the process; the supervisor generates it).
    #[builder(into)]
    pub worker_prefix: String,
    /// Worker pool configs by priority. Keys are sticky-routed by
    /// `hash(key) % pool.workers` within the selected pool.
    #[builder(default = PoolConfig::high_priority_defaults())]
    pub high_pool: PoolConfig,
    #[builder(default = PoolConfig::default())]
    pub default_pool: PoolConfig,
    #[builder(default = PoolConfig::background_defaults())]
    pub background_pool: PoolConfig,
}

/// The per-fragment dispatcher. One runs per fragment this node serves:
/// it owns the [`FragmentState`] and an internal managed worker pool, and
/// routes every operation to a worker by sticky key hash. `GetAll` is served
/// directly from shared state.
pub struct KvActor;

/// The dispatcher's state: the worker pool and the state they share.
pub struct KvState {
    high_pool: WorkerPool,
    default_pool: WorkerPool,
    background_pool: WorkerPool,
    shared: Arc<FragmentState>,
    worker_prefix: String,
}

impl KvState {
    fn pool(&self, priority: OpPriority) -> &WorkerPool {
        match priority {
            OpPriority::High => &self.high_pool,
            OpPriority::Default => &self.default_pool,
            OpPriority::Background => &self.background_pool,
        }
    }
}

async fn route_batch_read(mut work: BatchReadContinuation, state: &KvState) {
    loop {
        if work.reply.is_closed() {
            return;
        }
        if work.is_complete()
            || (!work.results.is_empty() && work.dispatcher.get_status() >= ActorStatus::Draining)
        {
            work.finish();
            return;
        }
        let pool = state.pool(work.priority);
        let reservation = {
            let key = work
                .current_key()
                .expect("incomplete batch continuation must have a key");
            pool.try_reserve(key).await
        };
        let Some(reservation) = reservation else {
            work.push(Err("shed: pool at queue cap or rate limit".to_string()));
            continue;
        };
        reservation.send(KvMessage::ContinueBatchRead(work));
        return;
    }
}

impl Actor for KvActor {
    type Msg = KvMessage;
    type State = KvState;
    type Arguments = KvArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // Workers are linked to the dispatcher: a dead worker raises a
        // supervision event here and gets replaced in-place.
        // Phase 7: three isolated pools by priority.
        let mut pools = Vec::with_capacity(3);
        let startup = AssertUnwindSafe(async {
            pools.push(
                WorkerPool::spawn_linked(
                    args.high_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-high", args.worker_prefix),
                )
                .await?,
            );
            pools.push(
                WorkerPool::spawn_linked(
                    args.default_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-default", args.worker_prefix),
                )
                .await?,
            );
            pools.push(
                WorkerPool::spawn_linked(
                    args.background_pool.clone(),
                    args.shared.clone(),
                    myself.get_cell(),
                    &format!("{}-bg", args.worker_prefix),
                )
                .await?,
            );
            Ok::<(), ActorProcessingErr>(())
        })
        .catch_unwind()
        .await;
        match startup {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                for pool in &pools {
                    pool.begin_shutdown();
                }
                for pool in &pools {
                    pool.join().await;
                }
                return Err(error);
            }
            Err(panic) => {
                for pool in &pools {
                    pool.begin_shutdown();
                }
                for pool in &pools {
                    pool.join().await;
                }
                std::panic::resume_unwind(panic);
            }
        }
        let mut pools = pools.into_iter();
        let high_pool = pools.next().expect("high pool was started");
        let default_pool = pools.next().expect("default pool was started");
        let background_pool = pools.next().expect("background pool was started");
        Ok(KvState {
            high_pool,
            default_pool,
            background_pool,
            shared: args.shared,
            worker_prefix: args.worker_prefix.clone(),
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Standalone KvActor users still receive the actor's historical
        // flush-on-stop contract. A supervising fragment keeps another Arc to
        // the shared state and performs an observable second fence, so a
        // failed attempt here retains its batch for the supervisor to retry.
        state.high_pool.shutdown().await;
        state.default_pool.shutdown().await;
        state.background_pool.shutdown().await;
        state.shared.flush().await
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            KvMessage::ApplyOp {
                op,
                priority,
                is_replication,
                reply,
            } => {
                let pool = match priority {
                    OpPriority::High => &state.high_pool,
                    OpPriority::Default => &state.default_pool,
                    OpPriority::Background => &state.background_pool,
                };
                // Phase 7: admission control. Shed explicitly if the
                // pool is over its queue cap or rate limit — unless this
                // is a replicated op: the replication sender is already
                // bounded, and shedding here while the sender acks would
                // silently lose acknowledged writes (adversarial #2).
                // Bypassed ops still reserve their exact sticky worker so
                // completion, immediate send failure, or worker death can
                // release the same accounting exactly once.
                let reservation = if is_replication {
                    Some(pool.reserve_unchecked(&op.key))
                } else {
                    pool.try_reserve(&op.key).await
                };
                let Some(reservation) = reservation else {
                    // Shed: counted in pool.shed_count and returned to callers
                    // that requested an exact apply outcome.
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(ApplyError::Backpressure));
                    }
                    return Ok(());
                };
                reservation.send(KvMessage::ApplyOp {
                    op,
                    priority,
                    is_replication,
                    reply,
                });
                Ok(())
            }
            KvMessage::Get {
                key,
                reply,
                priority,
            } => {
                // Reads sticky-route within their selected priority pool.
                // Completed writes are already visible in the shared cache,
                // regardless of which pool applied them. The worker replies
                // through the caller's channel directly; on worker
                // timeout/error it is dropped and the caller sees a
                // SenderError.
                let pool = match priority {
                    OpPriority::High => &state.high_pool,
                    OpPriority::Default => &state.default_pool,
                    OpPriority::Background => &state.background_pool,
                };
                let Some(reservation) = pool.try_reserve(&key).await else {
                    // Shed: explicit error, not "not found". The caller
                    // sees NodeError::Backpressure, distinct from Ok(None).
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".to_string()));
                    return Ok(());
                };
                // Hand the caller's reply port directly to the worker. The
                // dispatcher remains free to serve other pools, health, and
                // supervision while this read waits behind sticky-key work.
                reservation.send(KvMessage::Get {
                    key,
                    reply,
                    priority,
                });
                Ok(())
            }
            KvMessage::GetBatch {
                keys,
                response_base_payload_len,
                response_soft_limit,
                reply,
                priority,
            } => {
                route_batch_read(
                    BatchReadContinuation::new(
                        keys,
                        response_base_payload_len,
                        response_soft_limit,
                        priority,
                        reply,
                        myself,
                    ),
                    state,
                )
                .await;
                Ok(())
            }
            KvMessage::ContinueBatchRead(work) => {
                route_batch_read(work, state).await;
                Ok(())
            }
            KvMessage::GetBatchRetained(request) => {
                route_batch_read(
                    BatchReadContinuation::new(
                        request.keys,
                        request.response_base_payload_len,
                        request.response_soft_limit,
                        request.priority,
                        request.reply,
                        myself,
                    )
                    .with_retention(request.retention),
                    state,
                )
                .await;
                Ok(())
            }
            KvMessage::Flush { reply } => {
                let result = state
                    .shared
                    .flush()
                    .await
                    .map(|()| state.shared.durability())
                    .map_err(|error| ApplyError::Storage(error.to_string()));
                let _ = reply.send(result);
                // A failed fence keeps its in-flight buffer for retry. Report
                // the error to the protocol caller without killing the stable
                // dispatcher reference.
                Ok(())
            }
            KvMessage::FlushStable { reply } => {
                let result = state
                    .shared
                    .flush_stable()
                    .await
                    .map_err(|error| ApplyError::Storage(error.to_string()));
                let _ = reply.send(result);
                // As with the ordinary flush fence, keep the stable dispatcher
                // reference alive so a backend failure can be retried.
                Ok(())
            }
            KvMessage::GetAll { reply } => {
                let _ = reply.send(state.shared.get_all().await);
                Ok(())
            }
            KvMessage::GetSnapshot { reply } => {
                let _ = reply.send(state.shared.snapshot_with_revision().await);
                Ok(())
            }
            KvMessage::GetSummaryPage {
                after_key,
                expected_revision,
                snapshot_time_ms,
                limit,
                reply,
            } => {
                let page = state
                    .shared
                    .summary_page(
                        after_key.as_deref(),
                        expected_revision,
                        snapshot_time_ms,
                        limit,
                    )
                    .await;
                let _ = reply.send(page);
                Ok(())
            }
            KvMessage::GetVersioned {
                key,
                snapshot_time_ms,
                reply,
            } => {
                let _ = reply.send(state.shared.get_versioned(&key, snapshot_time_ms).await);
                Ok(())
            }
            KvMessage::GetVersionedPage {
                keys,
                start_index,
                snapshot_time_ms,
                reply,
            } => {
                let _ = reply.send(
                    state
                        .shared
                        .get_versioned_page(&keys, start_index, snapshot_time_ms)
                        .await,
                );
                Ok(())
            }
            KvMessage::GetRevision { reply } => {
                let _ = reply.send(state.shared.current_revision().await);
                Ok(())
            }
            KvMessage::GetShedCounts { reply } => {
                let counts = (
                    state.high_pool.shed_count(),
                    state.default_pool.shed_count(),
                    state.background_pool.shed_count(),
                );
                let _ = reply.send(counts);
                Ok(())
            }
        }
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // A dead worker is replaced in-place: the pool slot keeps its
        // index, so sticky routing is unaffected and no keys move.
        let dead = match &message {
            SupervisionEvent::ActorFailed(cell, _) => Some(cell.clone()),
            SupervisionEvent::ActorTerminated(cell, _, _) => Some(cell.clone()),
            _ => None,
        };
        if let Some(cell) = dead {
            // Check all three pools for the dead worker.
            let mut found = None;
            for (pool, suffix) in [
                (&mut state.high_pool, "high"),
                (&mut state.default_pool, "default"),
                (&mut state.background_pool, "bg"),
            ] {
                if let Some(slot) = pool.slot_of(&cell) {
                    found = Some((pool, slot, suffix));
                    break;
                }
            }
            if let Some((pool, slot, suffix)) = found {
                pool.reclaim_slot(slot);
                // Planned worker drains emit the same termination events as
                // failures. Retire any remaining accounting above, but do not
                // link a replacement to a draining parent.
                if myself.get_status() >= ActorStatus::Draining {
                    return Ok(());
                }
                let name = format!("{}-{suffix}-worker-{slot}", state.worker_prefix);
                let slot_pending = Arc::new(AtomicU64::new(0));
                #[cfg(test)]
                let startup_gate = take_worker_startup_gate(&name);
                let replacement = PoolWorker::spawn_linked(
                    Some(name),
                    PoolWorker,
                    PoolWorkerArgs {
                        shared: state.shared.clone(),
                        pool_pending: pool.pending.clone(),
                        slot_pending: slot_pending.clone(),
                        #[cfg(test)]
                        startup_gate,
                    },
                    myself.get_cell(),
                )
                .await;
                match replacement {
                    Ok((worker, _handle)) => pool.replace(slot, worker, slot_pending),
                    // `drain()` changes the parent status immediately. It can
                    // race the await above, causing ractor to reject the new
                    // link after the earlier status check. That is an orderly
                    // retirement, not a dispatcher failure: returning an
                    // error would skip `post_stop` and kill sibling workers
                    // before their accepted mailboxes drain.
                    Err(_) if myself.get_status() >= ActorStatus::Draining => return Ok(()),
                    Err(error) => return Err(ActorProcessingErr::from(error.to_string())),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Copyright (c) Sean Lawlor
    //
    // This source code is licensed under both the MIT license found in the
    // LICENSE-MIT file in the root directory of this source tree.

    //! Dispatcher argument defaults and caller-owned dependencies.

    use super::*;
    use crate::actor::test_support::test_state;
    use crate::actor::KvWorkerArgs;
    use std::collections::HashMap;
    #[test]
    fn actor_args_builders_retain_dependencies_and_priority_defaults() {
        let shared = Arc::new(test_state(HashMap::new()));
        let args = KvArgs::builder()
            .shared(shared.clone())
            .fragment(FragmentId(7))
            .worker_prefix("builder-defaults")
            .build();
        assert!(Arc::ptr_eq(&args.shared, &shared));
        assert_eq!(args.fragment, FragmentId(7));
        assert_eq!(args.worker_prefix, "builder-defaults");
        for (pool, expected) in [
            (&args.default_pool, (4, 10_000, None)),
            (&args.high_pool, (2, 5_000, None)),
            (&args.background_pool, (2, 5_000, Some(1000))),
        ] {
            assert_eq!(
                (pool.workers, pool.queue_cap, pool.rate_limit_per_sec),
                expected
            );
        }
        let custom = || PoolConfig::builder().workers(1).queue_cap(2).build();
        let overridden = KvArgs::builder()
            .shared(shared.clone())
            .fragment(FragmentId(8))
            .worker_prefix(String::from("builder-overrides"))
            .high_pool(custom())
            .default_pool(custom())
            .background_pool(custom())
            .build();
        for pool in [
            overridden.high_pool,
            overridden.default_pool,
            overridden.background_pool,
        ] {
            assert_eq!(
                (pool.workers, pool.queue_cap, pool.rate_limit_per_sec),
                (1, 2, None)
            );
        }
        let worker = KvWorkerArgs::builder().shared(shared.clone()).build();
        assert!(Arc::ptr_eq(&worker.shared, &shared));
        assert!(worker.pending.is_none());
        let pending = Arc::new(AtomicU64::new(0));
        let worker = KvWorkerArgs::builder()
            .shared(shared.clone())
            .pending(pending.clone())
            .build();
        assert!(Arc::ptr_eq(worker.pending.as_ref().unwrap(), &pending));
        assert!(KvWorkerArgs::builder()
            .shared(shared)
            .maybe_pending(None)
            .build()
            .pending
            .is_none());
    }
}
