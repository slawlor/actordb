//! Bounded actor observations performed only after registry locks are released.
use super::status::{NodeStatus, SnapshotLimits};
use super::*;
use crate::actor::observations::{PoolObservation, PoolObservationRequest};

#[derive(Debug, Clone, bon::Builder)]
pub struct DetailedSnapshotOptions {
    #[builder(default)]
    limits: SnapshotLimits,
    #[builder(default = Duration::from_secs(1))]
    timeout: Duration,
    #[builder(default = 256)]
    max_pools: usize,
    #[builder(default = 1024)]
    max_workers: usize,
    #[builder(default = true)]
    include_internal: bool,
    /// Empty chooses KV plus explicitly registered table metric scopes.
    #[builder(default)]
    tables: Vec<String>,
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::OpPriority;
    use ractor::{Actor, ActorProcessingErr, ActorRef};

    fn selected(status: &NodeStatus) -> &PoolObservation {
        &status
            .pools
            .iter()
            .find(|observed| {
                observed.pool.table.as_deref() == Some("KV")
                    && observed.pool.priority == OpPriority::Default
            })
            .unwrap()
            .pool
    }
    #[tokio::test]
    async fn rate_shedding_is_distinct_from_queue_capacity_shedding() {
        let node = Node::start(
            "rate-observation".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .default_pool(
                    crate::actor::PoolConfig::builder()
                        .workers(1)
                        .queue_cap(10)
                        .rate_limit_per_sec(1)
                        .build(),
                )
                .build(),
        )
        .await
        .unwrap();
        node.get("first".into()).await.unwrap();
        assert!(node.get("limited".into()).await.is_err());
        let status = node
            .status_snapshot_detailed(DetailedSnapshotOptions::default())
            .await
            .unwrap();
        assert_eq!(selected(&status).shed_rate, 1);
        assert_eq!(selected(&status).shed_queue, 0);
        node.shutdown().await.unwrap();
    }
    struct Release(Arc<Notify>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    #[tokio::test]
    async fn detailed_pool_observations_count_real_queued_active_shed_and_completed_work() {
        let node = Arc::new(
            Node::start(
                "pool-observations".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder()
                    .default_pool(
                        crate::actor::PoolConfig::builder()
                            .workers(1)
                            .queue_cap(2)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap(),
        );
        let gate = crate::actor::install_worker_get_gate(Node::qualify_key("blocked".into()));
        let _release = Release(gate.release.clone());
        let reading = node.clone();
        let blocked = tokio::spawn(async move { reading.get("blocked".into()).await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        let reading = node.clone();
        let queued = tokio::spawn(async move { reading.get("queued".into()).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = node
                    .status_snapshot_detailed(DetailedSnapshotOptions::default())
                    .await
                    .unwrap();
                if selected(&status).queued == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(node.get("shed".into()).await.is_err());
        let status = node
            .status_snapshot_detailed(DetailedSnapshotOptions::default())
            .await
            .unwrap();
        let pool = selected(&status);
        assert_eq!(pool.active, 1);
        assert_eq!(pool.queued, 1);
        assert_eq!(pool.completed, 0);
        assert_eq!(pool.shed_queue, 1);
        assert_eq!(pool.workers[0].active, 1);
        assert!(pool.oldest_queue_age.is_none());
        node.enable_metrics();
        let status = node
            .status_snapshot_detailed(DetailedSnapshotOptions::default())
            .await
            .unwrap();
        assert!(
            selected(&status).oldest_queue_age.is_none(),
            "old untimed jobs cannot gain invented ages"
        );
        blocked.abort();
        assert!(blocked.await.unwrap_err().is_cancelled());
        assert_eq!(
            selected(
                &node
                    .status_snapshot_detailed(DetailedSnapshotOptions::default())
                    .await
                    .unwrap()
            )
            .completed,
            0
        );
        gate.release.notify_one();
        queued.await.unwrap().unwrap();
        let status = node
            .status_snapshot_detailed(DetailedSnapshotOptions::default())
            .await
            .unwrap();
        assert_eq!(selected(&status).active, 0);
        assert_eq!(selected(&status).completed, 2);

        let gate = crate::actor::install_worker_get_gate(Node::qualify_key("timed".into()));
        let _release = Release(gate.release.clone());
        let reading = node.clone();
        let timed = tokio::spawn(async move { reading.get("timed".into()).await });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        let reading = node.clone();
        let queued = tokio::spawn(async move { reading.get("timed-queued".into()).await });
        let status = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = node
                    .status_snapshot_detailed(DetailedSnapshotOptions::default())
                    .await
                    .unwrap();
                if selected(&status).queued == 1 {
                    break status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(selected(&status).oldest_queue_age.is_some());
        assert!(selected(&status).queue_duration.count >= 1);
        gate.release.notify_one();
        timed.await.unwrap().unwrap();
        queued.await.unwrap().unwrap();
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn detailed_limits_and_collectors_preserve_owned_bounded_data_without_locks() {
        let node = Node::start(
            "detail-collector".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        node.enable_metrics();
        node.put("one".into(), vec![1]).await.unwrap();
        let limited = node
            .status_snapshot_detailed(
                DetailedSnapshotOptions::builder()
                    .max_pools(2)
                    .max_workers(1)
                    .build(),
            )
            .await
            .unwrap();
        assert_eq!(limited.pools.len(), 2);
        assert!(
            limited
                .pools
                .iter()
                .map(|entry| entry.pool.workers.len())
                .sum::<usize>()
                <= 1
        );
        assert!(limited.detailed.unwrap().pools_truncated);
        let snapshot = node
            .collect_metrics_detailed(
                DetailedSnapshotOptions::default(),
                &|_: &super::super::metrics::MetricPoint| {
                    assert!(node.shutdown_state.try_lock().is_ok());
                    assert!(node.sender_reports.try_lock().is_ok());
                    assert!(node.peers.try_lock().is_ok());
                    assert!(node.router.actors.try_read().is_ok());
                    let _reentered = node.status_snapshot();
                },
            )
            .await
            .unwrap();
        assert!(snapshot
            .points
            .iter()
            .any(|point| point.descriptor.name == "actordb.pool.completed"
                && point.source_incarnation.is_some()));
        node.shutdown().await.unwrap();
    }

    struct GateDispatcher;
    struct GateState {
        actual: ActorRef<KvMessage>,
        entered: Arc<Notify>,
        release: Arc<Semaphore>,
    }
    impl Actor for GateDispatcher {
        type Msg = KvMessage;
        type State = GateState;
        type Arguments = GateState;
        async fn pre_start(
            &self,
            _: ActorRef<KvMessage>,
            args: GateState,
        ) -> Result<GateState, ActorProcessingErr> {
            Ok(args)
        }
        async fn handle(
            &self,
            _: ActorRef<KvMessage>,
            message: KvMessage,
            state: &mut GateState,
        ) -> Result<(), ActorProcessingErr> {
            if matches!(&message, KvMessage::ObservePools { .. }) {
                state.entered.notify_one();
                state.release.acquire().await.unwrap().forget();
            }
            state
                .actual
                .cast(message)
                .map_err(|error| ActorProcessingErr::from(error.to_string()))
        }
    }
    #[tokio::test]
    async fn cancelled_detailed_actor_calls_retain_capacity_and_report_timeouts_without_fake_zeroes(
    ) {
        let node = Node::start(
            "detail-timeout".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        let actual = node.router.route_for_fragment(FragmentId(0)).unwrap();
        let release = Arc::new(Semaphore::new(0));
        let entered = Arc::new(Notify::new());
        let (gate, _) = GateDispatcher::spawn(
            None,
            GateDispatcher,
            GateState {
                actual,
                entered,
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        node.router.insert(FragmentId(0), gate.clone());
        let options = DetailedSnapshotOptions::builder()
            .timeout(Duration::from_millis(5))
            .build();
        for _ in 0..16 {
            let status = node
                .status_snapshot_detailed(options.clone())
                .await
                .unwrap();
            assert!(status.pools.is_empty());
            assert!(status
                .detailed
                .unwrap()
                .failures
                .iter()
                .any(|(_, failure)| *failure == PoolObservationFailure::ActorTimeout));
        }
        assert_eq!(node.detail_admission.available_permits(), 0);
        let busy = node.status_snapshot_detailed(options).await.unwrap();
        assert_eq!(
            busy.detailed.unwrap().failures[0].1,
            PoolObservationFailure::AdmissionBusy
        );
        release.add_permits(32);
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.detail_admission.available_permits() != 16 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.stop(None);
        gate.wait(None).await.unwrap();
        node.shutdown().await.unwrap();
    }
}
impl Default for DetailedSnapshotOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PoolObservationFailure {
    RegistryContended,
    ActorUnavailable,
    ActorTimeout,
    BudgetExpired,
    AdmissionBusy,
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FragmentPoolObservation {
    pub fragment: FragmentId,
    pub pool: PoolObservation,
    pub observed_at: std::time::SystemTime,
}
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DetailedObservation {
    pub attempted_fragments: usize,
    pub failures: Vec<(Option<FragmentId>, PoolObservationFailure)>,
    pub missing_tables: Vec<(FragmentId, String)>,
    pub pools_truncated: bool,
    pub workers_truncated: bool,
    pub tables_truncated: bool,
}
impl DetailedObservation {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
            && !self.pools_truncated
            && !self.workers_truncated
            && !self.tables_truncated
    }
}
impl Node {
    /// Query deliberate actor/table scopes under one absolute deadline. At most
    /// 16 detailed calls own actor work, including cancelled RPC waiters.
    /// Partial/contended/timed-out data is explicit; no unknown queue is zeroed.
    pub async fn status_snapshot_detailed(
        &self,
        options: DetailedSnapshotOptions,
    ) -> Result<NodeStatus, NodeError> {
        let started = std::time::Instant::now();
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .filter(|_| !options.timeout.is_zero())
            .ok_or_else(|| {
                NodeError::Codec("detail timeout must be nonzero and representable".into())
            })?;
        if options.max_pools > 4096 || options.max_workers > 4096 || options.tables.len() > 256 {
            return Err(NodeError::Codec(
                "detail output limits exceed 4096 pools/workers or 256 table scopes".into(),
            ));
        }
        let mut status = self.status_snapshot_with_limits(options.limits);
        let lifecycle_revision = status.lifecycle_revision;
        let mut detail = DetailedObservation::default();
        let permit = match self.detail_admission.clone().try_acquire_owned() {
            Ok(permit) => Arc::new(permit),
            Err(_) => {
                detail
                    .failures
                    .push((None, PoolObservationFailure::AdmissionBusy));
                status.detailed = Some(detail);
                return Ok(status);
            }
        };
        let mut tables = if options.tables.is_empty() {
            let mut tables = vec!["KV".to_owned()];
            tables.extend(status.tables.iter().map(|table| table.table.clone()));
            tables
        } else {
            options
                .tables
                .iter()
                .map(|name| super::TableId::new(name.clone()).map(|table| table.name().to_owned()))
                .collect::<Result<Vec<_>, _>>()?
        };
        tables.sort();
        tables.dedup();
        detail.tables_truncated = tables.len() > options.limits.max_tables();
        tables.truncate(options.limits.max_tables());
        let actors = match self.router.actors.try_read() {
            Ok(actors) => (0..self
                .router
                .fragment_count
                .min(options.limits.max_fragments()))
                .filter_map(|index| {
                    let fragment = FragmentId(index as u32);
                    actors
                        .get(&fragment)
                        .map(|route| (fragment, route.actor.clone()))
                })
                .collect::<Vec<_>>(),
            Err(_) => {
                detail
                    .failures
                    .push((None, PoolObservationFailure::RegistryContended));
                Vec::new()
            }
        };
        let mut remaining_pools = options.max_pools;
        let mut remaining_workers = options.max_workers;
        for (fragment, actor) in actors {
            if remaining_pools == 0 {
                detail.pools_truncated = true;
                break;
            }
            if Instant::now() >= deadline {
                detail
                    .failures
                    .push((Some(fragment), PoolObservationFailure::BudgetExpired));
                break;
            }
            detail.attempted_fragments += 1;
            let request = PoolObservationRequest {
                tables: tables.clone(),
                include_internal: options.include_internal,
                max_pools: remaining_pools,
                max_workers: remaining_workers,
                retention: permit.clone(),
            };
            let result = tokio::time::timeout_at(
                deadline,
                actor.call(|reply| KvMessage::ObservePools { request, reply }, None),
            )
            .await;
            match result {
                Ok(Ok(CallResult::Success(page))) => {
                    if page.observed_at.is_none()
                        || page.pools.len() > remaining_pools
                        || page
                            .pools
                            .iter()
                            .map(|pool| pool.workers.len())
                            .sum::<usize>()
                            > remaining_workers
                        || page.missing_tables.len() > tables.len()
                    {
                        detail
                            .failures
                            .push((Some(fragment), PoolObservationFailure::ActorUnavailable));
                        continue;
                    }
                    detail.pools_truncated |= page.pools_truncated;
                    for pool in page.pools {
                        remaining_pools = remaining_pools.saturating_sub(1);
                        remaining_workers = remaining_workers.saturating_sub(pool.workers.len());
                        detail.workers_truncated |= pool.workers_truncated;
                        status.pools.push(FragmentPoolObservation {
                            fragment,
                            pool,
                            observed_at: page.observed_at.expect("validated observation metadata"),
                        });
                    }
                    detail.missing_tables.extend(
                        page.missing_tables
                            .into_iter()
                            .map(|table| (fragment, table)),
                    );
                }
                Err(_) | Ok(Ok(CallResult::Timeout)) => detail
                    .failures
                    .push((Some(fragment), PoolObservationFailure::ActorTimeout)),
                _ => detail
                    .failures
                    .push((Some(fragment), PoolObservationFailure::ActorUnavailable)),
            }
        }
        status.observation.lifecycle_changed |=
            lifecycle_revision != self.lifecycle_observation.revision.load(Ordering::Acquire);
        status.collection_duration = started.elapsed();
        status.detailed = Some(detail);
        Ok(status)
    }
}
