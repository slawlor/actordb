// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Per-fragment dispatcher and supervision of priority worker pools.

use std::collections::HashMap;
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
    table_pools: HashMap<String, TablePools>,
    shared: Arc<FragmentState>,
    worker_prefix: String,
    high_config: PoolConfig,
    default_config: PoolConfig,
    background_config: PoolConfig,
}

struct TablePools {
    high_pool: WorkerPool,
    default_pool: WorkerPool,
    background_pool: WorkerPool,
    shared: Arc<FragmentState>,
}

impl KvState {
    fn base_pool(&self, priority: OpPriority) -> &WorkerPool {
        match priority {
            OpPriority::High => &self.high_pool,
            OpPriority::Default => &self.default_pool,
            OpPriority::Background => &self.background_pool,
        }
    }

    fn base_pool_mut(&mut self, priority: OpPriority) -> &mut WorkerPool {
        match priority {
            OpPriority::High => &mut self.high_pool,
            OpPriority::Default => &mut self.default_pool,
            OpPriority::Background => &mut self.background_pool,
        }
    }

    fn table_pool(&self, table: &str, priority: OpPriority) -> &WorkerPool {
        let pools = self
            .table_pools
            .get(table)
            .expect("table CRUD pools must be initialized before routing");
        match priority {
            OpPriority::High => &pools.high_pool,
            OpPriority::Default => &pools.default_pool,
            OpPriority::Background => &pools.background_pool,
        }
    }

    fn table_pool_mut(&mut self, table: &str, priority: OpPriority) -> &mut WorkerPool {
        let pools = self
            .table_pools
            .get_mut(table)
            .expect("table CRUD pools must be initialized before routing");
        match priority {
            OpPriority::High => &mut pools.high_pool,
            OpPriority::Default => &mut pools.default_pool,
            OpPriority::Background => &mut pools.background_pool,
        }
    }

    fn pool(&self, table: Option<&str>, priority: OpPriority) -> &WorkerPool {
        table.map_or_else(
            || self.base_pool(priority),
            |table| self.table_pool(table, priority),
        )
    }

    fn pool_mut(&mut self, table: Option<&str>, priority: OpPriority) -> &mut WorkerPool {
        match table {
            Some(table) => self.table_pool_mut(table, priority),
            None => self.base_pool_mut(priority),
        }
    }

    async fn ensure_table_pools(
        &mut self,
        table: &str,
        myself: &ActorRef<KvMessage>,
    ) -> Result<(), ActorProcessingErr> {
        if self.table_pools.contains_key(table) {
            return Ok(());
        }
        let shared = self.shared.table_state_for_actor(table).await;
        let prefix = format!("{}-table-{}", self.worker_prefix, table);
        let pools = spawn_pools(
            self.high_config.clone(),
            self.default_config.clone(),
            self.background_config.clone(),
            shared.clone(),
            myself,
            &prefix,
        )
        .await?;
        self.table_pools.insert(
            table.to_string(),
            TablePools {
                high_pool: pools.0,
                default_pool: pools.1,
                background_pool: pools.2,
                shared,
            },
        );
        Ok(())
    }
}

fn table_for_key(key: &str) -> Option<String> {
    crate::table_key::table_name(key)
}

fn table_for_scan(spec: &crate::actor::ScanSpec) -> Option<String> {
    match &spec.matcher {
        crate::actor::KeyMatcher::Prefix(prefix) => std::str::from_utf8(prefix)
            .ok()
            .and_then(crate::table_key::table_name),
        crate::actor::KeyMatcher::All => None,
    }
}

async fn spawn_pools(
    high_config: PoolConfig,
    default_config: PoolConfig,
    background_config: PoolConfig,
    shared: Arc<FragmentState>,
    supervisor: &ActorRef<KvMessage>,
    worker_prefix: &str,
) -> Result<(WorkerPool, WorkerPool, WorkerPool), ActorProcessingErr> {
    let mut pools = Vec::with_capacity(3);
    let startup = AssertUnwindSafe(async {
        pools.push(
            WorkerPool::spawn_linked(
                high_config,
                shared.clone(),
                supervisor.get_cell(),
                &format!("{worker_prefix}-high"),
            )
            .await?,
        );
        pools.push(
            WorkerPool::spawn_linked(
                default_config,
                shared.clone(),
                supervisor.get_cell(),
                &format!("{worker_prefix}-default"),
            )
            .await?,
        );
        pools.push(
            WorkerPool::spawn_linked(
                background_config,
                shared,
                supervisor.get_cell(),
                &format!("{worker_prefix}-bg"),
            )
            .await?,
        );
        Ok::<(), ActorProcessingErr>(())
    })
    .catch_unwind()
    .await;
    match startup {
        Ok(Ok(())) => {
            let mut pools = pools.into_iter();
            Ok((
                pools.next().expect("high pool was started"),
                pools.next().expect("default pool was started"),
                pools.next().expect("background pool was started"),
            ))
        }
        Ok(Err(error)) => {
            for pool in &pools {
                pool.begin_shutdown();
            }
            for pool in &pools {
                pool.join().await;
            }
            Err(error)
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
}

async fn route_batch_read(mut work: BatchReadContinuation, state: &mut KvState) {
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
        let table = work.current_key().and_then(table_for_key);
        if let Some(table) = table.as_deref() {
            if state
                .ensure_table_pools(table, &work.dispatcher)
                .await
                .is_err()
            {
                work.push(Err("table CRUD pool unavailable".to_string()));
                continue;
            }
        }
        let pool = state.pool(table.as_deref(), work.priority);
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
        // supervision event here and gets replaced in-place. Named tables get
        // independent pool sets; the base pools remain only for internal
        // messages that do not carry a table namespace.
        let (high_pool, default_pool, background_pool) = spawn_pools(
            args.high_pool.clone(),
            args.default_pool.clone(),
            args.background_pool.clone(),
            args.shared.clone(),
            &myself,
            &args.worker_prefix,
        )
        .await?;
        // `KV` is the public default table for the Node API. Start
        // its CRUD pools with the fragment so the first application request
        // does not pay dynamic actor-creation latency (custom tables remain
        // lazy and are created on first use).
        let kv_shared = args.shared.table_state_for_actor("KV").await;
        let (kv_high_pool, kv_default_pool, kv_background_pool) = spawn_pools(
            args.high_pool.clone(),
            args.default_pool.clone(),
            args.background_pool.clone(),
            kv_shared.clone(),
            &myself,
            &format!("{}-table-KV", args.worker_prefix),
        )
        .await?;
        let mut table_pools = HashMap::new();
        table_pools.insert(
            "KV".to_string(),
            TablePools {
                high_pool: kv_high_pool,
                default_pool: kv_default_pool,
                background_pool: kv_background_pool,
                shared: kv_shared,
            },
        );
        Ok(KvState {
            high_pool,
            default_pool,
            background_pool,
            table_pools,
            shared: args.shared,
            worker_prefix: args.worker_prefix.clone(),
            high_config: args.high_pool,
            default_config: args.default_pool,
            background_config: args.background_pool,
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
        for pools in state.table_pools.values_mut() {
            pools.high_pool.shutdown().await;
            pools.default_pool.shutdown().await;
            pools.background_pool.shutdown().await;
        }
        state.shared.flush().await
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (message, admission_retention) = message.into_retained_parts();
        match message {
            KvMessage::ApplyOp {
                op,
                priority,
                is_replication,
                reply,
            } => {
                let table = table_for_key(&op.key);
                if let Some(table_name) = table.as_deref() {
                    state.ensure_table_pools(table_name, &myself).await?;
                }
                let pool = state.pool(table.as_deref(), priority);
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
                reservation.send(
                    KvMessage::ApplyOp {
                        op,
                        priority,
                        is_replication,
                        reply,
                    }
                    .with_optional_retention(admission_retention.clone()),
                );
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
                let table = table_for_key(&key);
                if let Some(table_name) = table.as_deref() {
                    state.ensure_table_pools(table_name, &myself).await?;
                }
                let pool = state.pool(table.as_deref(), priority);
                let Some(reservation) = pool.try_reserve(&key).await else {
                    // Shed: explicit error, not "not found". The caller
                    // sees NodeError::Backpressure, distinct from Ok(None).
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".to_string()));
                    return Ok(());
                };
                // Hand the caller's reply port directly to the worker. The
                // dispatcher remains free to serve other pools, health, and
                // supervision while this read waits behind sticky-key work.
                reservation.send(
                    KvMessage::Get {
                        key,
                        reply,
                        priority,
                    }
                    .with_optional_retention(admission_retention.clone()),
                );
                Ok(())
            }
            KvMessage::Scan { spec, reply } => {
                // Scans ride the background pool ("bulk/scan traffic") so
                // they can never starve latency-sensitive ops. Non-sticky
                // routing: a scan has no key, so spread concurrent scans
                // round-robin across background workers. Shed explicitly
                // under backpressure — never confused with "no matches".
                let table = table_for_scan(&spec);
                if let Some(table_name) = table.as_deref() {
                    state.ensure_table_pools(table_name, &myself).await?;
                }
                let pool = state.pool(table.as_deref(), OpPriority::Background);
                let Some(reservation) = pool.try_reserve_round_robin().await else {
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".to_string()));
                    return Ok(());
                };
                reservation.send(
                    KvMessage::Scan { spec, reply }
                        .with_optional_retention(admission_retention.clone()),
                );
                Ok(())
            }
            KvMessage::ScanRetained {
                spec,
                retention,
                reply,
            } => {
                if reply.is_closed() {
                    return Ok(());
                }
                let table = table_for_scan(&spec);
                if let Some(table_name) = table.as_deref() {
                    state.ensure_table_pools(table_name, &myself).await?;
                }
                let pool = state.pool(table.as_deref(), OpPriority::Background);
                let Some(reservation) = pool.try_reserve_round_robin().await else {
                    let _ = reply.send(Err("shed: pool at queue cap or rate limit".into()));
                    return Ok(());
                };
                reservation.send(
                    KvMessage::ScanRetained {
                        spec,
                        retention,
                        reply,
                    }
                    .with_optional_retention(admission_retention.clone()),
                );
                Ok(())
            }
            KvMessage::QueryPage { work, reply } => {
                if reply.is_closed() {
                    return Ok(());
                }
                let table = Some(work.plan.table.to_ascii_uppercase());
                state
                    .ensure_table_pools(table.as_deref().unwrap(), &myself)
                    .await?;
                let pool = state.pool(table.as_deref(), OpPriority::Background);
                let Some(reservation) = pool.try_reserve_round_robin().await else {
                    let _ = reply.send(crate::query::page_error(
                        &work.request,
                        crate::proto::QueryPageStatus::Backpressure,
                        "background pool at queue cap or rate limit",
                    ));
                    return Ok(());
                };
                reservation.send(
                    KvMessage::QueryPage { work, reply }
                        .with_optional_retention(admission_retention.clone()),
                );
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
                    )
                    .with_optional_retention(admission_retention.clone()),
                    state,
                )
                .await;
                Ok(())
            }
            KvMessage::ContinueBatchRead(work) => {
                route_batch_read(
                    work.with_optional_retention(admission_retention.clone()),
                    state,
                )
                .await;
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
                    .with_retention(request.retention)
                    .with_optional_retention(admission_retention.clone()),
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
            KvMessage::Retained(_) => unreachable!("retained message was unwrapped"),
            KvMessage::GetShedCounts { reply } => {
                let mut counts = (
                    state.high_pool.shed_count(),
                    state.default_pool.shed_count(),
                    state.background_pool.shed_count(),
                );
                for pools in state.table_pools.values() {
                    counts.0 += pools.high_pool.shed_count();
                    counts.1 += pools.default_pool.shed_count();
                    counts.2 += pools.background_pool.shed_count();
                }
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
            // Locate the owning pool without holding a mutable borrow across
            // the replacement actor's async startup.
            let mut found = None;
            for (priority, suffix) in [
                (OpPriority::High, "high"),
                (OpPriority::Default, "default"),
                (OpPriority::Background, "bg"),
            ] {
                if let Some(slot) = state.base_pool(priority).slot_of(&cell) {
                    found = Some((None, priority, slot, suffix.to_string()));
                    break;
                }
            }
            if found.is_none() {
                for table in state.table_pools.keys() {
                    for (priority, suffix) in [
                        (OpPriority::High, "high"),
                        (OpPriority::Default, "default"),
                        (OpPriority::Background, "bg"),
                    ] {
                        if let Some(slot) = state.table_pool(table, priority).slot_of(&cell) {
                            found = Some((Some(table.clone()), priority, slot, suffix.to_string()));
                            break;
                        }
                    }
                    if found.is_some() {
                        break;
                    }
                }
            }
            if let Some((table, priority, slot, suffix)) = found {
                let shared = table
                    .as_deref()
                    .and_then(|name| state.table_pools.get(name).map(|p| p.shared.clone()))
                    .unwrap_or_else(|| state.shared.clone());
                let (shared, pending) = {
                    let pool = state.pool_mut(table.as_deref(), priority);
                    pool.reclaim_slot(slot);
                    (shared, pool.pending.clone())
                };
                // Planned worker drains emit the same termination events as
                // failures. Retire any remaining accounting above, but do not
                // link a replacement to a draining parent.
                if myself.get_status() >= ActorStatus::Draining {
                    return Ok(());
                }
                let name = match table.as_deref() {
                    Some(table) => format!(
                        "{}-table-{table}-{suffix}-worker-{slot}",
                        state.worker_prefix
                    ),
                    None => format!("{}-{suffix}-worker-{slot}", state.worker_prefix),
                };
                let slot_pending = Arc::new(AtomicU64::new(0));
                #[cfg(test)]
                let startup_gate = take_worker_startup_gate(&name);
                let replacement = PoolWorker::spawn_linked(
                    Some(name),
                    PoolWorker,
                    PoolWorkerArgs {
                        shared,
                        pool_pending: pending,
                        slot_pending: slot_pending.clone(),
                        #[cfg(test)]
                        startup_gate,
                    },
                    myself.get_cell(),
                )
                .await;
                match replacement {
                    Ok((worker, _handle)) => state.pool_mut(table.as_deref(), priority).replace(
                        slot,
                        worker,
                        slot_pending,
                    ),
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
mod retention_tests {
    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

    use super::*;
    use crate::actor::test_support::{test_op, test_state};
    use crate::actor::VersionedValue;
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn named_tables_get_independent_crud_worker_sets() {
        let shared = Arc::new(test_state(Default::default()));
        let (actor, handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs::builder()
                .shared(shared)
                .fragment(FragmentId(0))
                .worker_prefix("table-pools".to_string())
                .build(),
        )
        .await
        .unwrap();

        let key = crate::table_key::encode_table_key("USERS", "id");
        let outcome = actor
            .call(
                move |reply| KvMessage::ApplyOp {
                    op: test_op(
                        &key,
                        VersionedValue::new(b"value".to_vec(), 1, "writer".into()),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ractor::rpc::CallResult::Success(Ok(_))));

        // The default config starts 2 high + 4 default + 2 background
        // workers for the eagerly prepared KV table. USERS adds a second
        // complete set under the same lightweight dispatcher/control plane.
        assert_eq!(actor.get_children().len(), 24);
        actor.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_waiters_retain_admission_until_dispatcher_or_worker_work_finishes() {
        for summary in [false, true] {
            let shared = Arc::new(test_state(Default::default()));
            let (actor, handle) = KvActor::spawn(
                None,
                KvActor,
                KvArgs::builder()
                    .shared(shared.clone())
                    .fragment(FragmentId(0))
                    .worker_prefix(format!("retained-{}", summary))
                    .build(),
            )
            .await
            .unwrap();
            let cache = shared.lock_cache_for_test().await;
            let admission = Arc::new(Semaphore::new(1));
            let retention: Arc<dyn Send + Sync> =
                Arc::new(admission.clone().acquire_owned().await.unwrap());
            if summary {
                let mut pending = Box::pin(actor.call(
                    move |reply| {
                        KvMessage::GetSummaryPage {
                            after_key: None,
                            expected_revision: None,
                            snapshot_time_ms: None,
                            limit: 1,
                            reply,
                        }
                        .with_retention(retention)
                    },
                    None,
                ));
                std::future::poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(pending);
            } else {
                let mut pending = Box::pin(actor.call(
                    move |reply| {
                        KvMessage::Get {
                            key: "key".into(),
                            priority: OpPriority::Default,
                            reply,
                        }
                        .with_retention(retention)
                    },
                    None,
                ));
                std::future::poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(pending);
            }
            assert_eq!(admission.available_permits(), 0);
            drop(cache);
            let permit = tokio::time::timeout(Duration::from_secs(2), admission.acquire())
                .await
                .expect("actor-owned admission was not released")
                .unwrap();
            drop(permit);
            actor.stop(None);
            handle.await.unwrap();
        }
    }
}
