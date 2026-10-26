//! Bounded observations of lifecycle, serving authority, and replication.
//!
//! No actor RPC, backend call, or asynchronous lock wait is made by collection.
//! A snapshot is an observation, not a write or durability fence. Check its
//! completeness and change indicators before using it to sequence placement.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

use super::drain::{DrainPhase, DrainStatus};
use super::{Node, ReplicationDrainReport};
use crate::placement::FragmentPlacementState;
use crate::replication::ReplicationAckDurability;
use crate::shard::FragmentId;
/// Actual lifecycle phase; replication activity can coexist with Serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeLifecycle {
    Starting,
    Initializing,
    Recovering,
    Serving,
    Draining,
    Stopping,
    Stopped,
    Blocked,
    Failed,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleBlocker {
    MissingSources {
        fragment: FragmentId,
        peers: Vec<String>,
    },
    AwaitingAuthority {
        fragment: FragmentId,
    },
    SnapshotUnavailable,
    LocalFailure(String),
    RuntimeStopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FragmentActivity {
    Acquiring,
    Serving,
    Transferring,
    Retiring,
    Stalled,
    NotAssigned,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationActivity {
    Active,
    Steady,
    WaitingForPeer,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObservationFreshness {
    Fresh,
    Stale,
    Unknown,
}

/// Maximum fragment and sender records returned by one observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    max_fragments: usize,
    max_senders: usize,
    max_tables: usize,
    stale_after: Duration,
}

impl SnapshotLimits {
    /// Set output limits, clamped to 4,096 records per collection category.
    /// Zero requests only aggregate lifecycle information.
    pub fn new(max_fragments: usize, max_senders: usize) -> Self {
        Self {
            max_fragments: max_fragments.min(4_096),
            max_senders: max_senders.min(4_096),
            max_tables: 64,
            stale_after: Duration::from_secs(5),
        }
    }

    /// Maximum number of fragment records.
    pub fn max_fragments(&self) -> usize {
        self.max_fragments
    }

    /// Maximum number of replication sender records.
    pub fn max_senders(&self) -> usize {
        self.max_senders
    }
    pub fn with_max_tables(mut self, max_tables: usize) -> Self {
        self.max_tables = max_tables.min(256);
        self
    }
    pub fn max_tables(&self) -> usize {
        self.max_tables
    }
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self::new(256, 256)
    }
}

/// Whether this node can currently accept work for its assigned fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Readiness {
    /// The declared initial authority boundary is established, admission is
    /// open, and at least one authoritative route remains available. Later
    /// acquisitions can coexist with serving. Routing-only roles can be ready.
    Ready,
    /// Admission is closed, or at least one assigned fragment cannot serve.
    NotReady,
    /// Observation was incomplete or changed during collection.
    Unknown,
}

/// Last atomically published stage of full shutdown, including active calls.
/// Request-only drain does not advance this coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShutdownStage {
    NotStarted,
    ReplicationFence,
    ReplicationDrain,
    StoppingTransport,
    QuiescingFragments,
    StoppingFragments,
    Stopped,
}

/// One fragment's actual local serving authority and observed placement.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FragmentStatus {
    pub activity: FragmentActivity,
    pub missing_peers: Vec<String>,
    pub missing_peers_unknown: bool,
    pub replication_active: bool,
    pub blocker: Option<LifecycleBlocker>,
    pub fragment: FragmentId,
    /// Replica order in the node's authoritative routing map.
    pub replicas: Vec<String>,
    pub assigned_locally: bool,
    /// Absent when a dynamic gain has not yet installed a lifecycle state.
    pub placement_state: Option<FragmentPlacementState>,
    /// Local serving-set membership together with a published actor route.
    /// This does not override the node's admission fence.
    pub serving: bool,
    /// A published route has a running dispatcher, including during catch-up.
    pub route_available: bool,
}

/// Typed connection/session state from the sender's existing report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SenderConnectionState {
    Connected,
    Disconnected,
    /// The sender must rebuild its delivery state from authoritative data.
    ResetRequired,
    /// A newer sender state was not recognized by this observer.
    Unknown,
}

/// What a sender's reconciliation is being used for on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationPurpose {
    /// A newly assigned fragment is not yet authoritative.
    InitialCatchUp,
    /// Replication/anti-entropy for an already serving local fragment.
    Background,
    /// Local placement has been revoked and the fragment is being retired.
    PlacementDrain,
    /// Fragment observation was unavailable or did not establish its purpose.
    Unknown,
}

/// One sender observed without waiting for its report lock.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplicationStatus {
    pub incarnation: u64,
    pub activity: ReplicationActivity,
    pub observed_at: Option<SystemTime>,
    pub freshness: ObservationFreshness,
    pub last_successful_ack_at: Option<SystemTime>,
    pub last_successful_ack_durability: Option<ReplicationAckDurability>,
    pub last_successful_reconcile_at: Option<SystemTime>,
    pub reconciliation_stage: Option<crate::replication::ReconciliationStage>,
    pub expected_fetch_keys: Option<usize>,
    pub received_fetch_keys: Option<usize>,
    pub timing_enabled: bool,
    pub oldest_retained_operation_age: Option<Duration>,
    pub fragment: FragmentId,
    pub peer_id: String,
    pub connection: SenderConnectionState,
    pub purpose: ReplicationPurpose,
    /// Observation metadata, not a metric label or a durability guarantee.
    pub session_id: u64,
    pub buffered_operations: usize,
    pub retained_bytes: usize,
    pub in_flight_batches: usize,
    /// Lifetime of this sender instance; resets when a sender is replaced.
    pub completed_reconcile_rounds: u64,
    /// Most recent ACK's actual receiver fence, not a target's pending fence.
    pub last_ack_durability: Option<ReplicationAckDurability>,
    pub ack_durability_mismatch: bool,
}

/// Which sections could not be fully observed without waiting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotObservation {
    pub retirement_unavailable: bool,
    pub peers_unavailable: bool,
    pub stale_sender_reports: usize,
    pub lifecycle_reason_unavailable: bool,
    pub table_metrics_unavailable: bool,
    pub table_metrics_truncated: bool,
    pub placement_unavailable: bool,
    pub fragments_truncated: bool,
    /// At most 64 replica IDs are copied for each fragment.
    pub replicas_truncated: bool,
    pub senders_unavailable: bool,
    pub senders_truncated: bool,
    pub unavailable_sender_reports: usize,
    pub drain_report_unavailable: bool,
    pub drain_report_truncated: bool,
    /// Published placement generation changed while other sections were read.
    pub placement_changed: bool,
    /// Admission/drain phase changed while other sections were read.
    pub lifecycle_changed: bool,
}

impl SnapshotObservation {
    /// Every requested section was sampled completely and no detected phase
    /// or placement change occurred. Independent gauges still are approximate.
    pub fn is_complete(&self) -> bool {
        !self.retirement_unavailable
            && !self.peers_unavailable
            && self.stale_sender_reports == 0
            && !self.lifecycle_reason_unavailable
            && !self.placement_unavailable
            && !self.table_metrics_unavailable
            && !self.table_metrics_truncated
            && !self.fragments_truncated
            && !self.replicas_truncated
            && !self.senders_unavailable
            && !self.senders_truncated
            && self.unavailable_sender_reports == 0
            && !self.drain_report_unavailable
            && !self.drain_report_truncated
            && !self.placement_changed
            && !self.lifecycle_changed
    }
}

/// Owned, bounded node observation. Reading this value never touches the node.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeStatus {
    pub initial_readiness_established: bool,
    pub desired_local_fragments: usize,
    /// Published authority count; available dispatcher count is separate.
    pub serving_fragments: Option<usize>,
    /// Exact only when every published serving route fit the observation budget.
    pub available_fragments: Option<usize>,
    pub capacity_partial: bool,
    pub startup: super::NodeStartupSnapshot,
    pub lifecycle_blocker: Option<LifecycleBlocker>,
    pub pools: Vec<super::FragmentPoolObservation>,
    pub detailed: Option<super::DetailedObservation>,
    pub lifecycle: NodeLifecycle,
    pub lifecycle_reason: Option<String>,
    pub lifecycle_revision: u64,
    pub node_id: String,
    /// Process incarnation, useful for detecting cumulative metric resets.
    pub boot_id: u64,
    pub observed_at: SystemTime,
    pub collection_duration: Duration,
    pub drain: DrainStatus,
    pub completed_local_persistence_fences: usize,
    pub local_persistence_failure: Option<(FragmentId, String)>,
    pub storage: crate::store::StorageDiagnostics,
    pub request_timing_enabled: bool,
    pub request_duration: super::observability::DurationSummary,
    pub request_waiter_timeouts: u64,
    pub tables: Vec<super::observability::TableMetricsSnapshot>,
    pub queues: super::observability::RequestQueueDiagnostics,
    pub write_concerns: crate::write_concern::WriteConcernDiagnostics,
    pub peer_fence: super::PeerFenceStatus,
    /// Conservative query/scan coordinator and actor-owned byte reservations.
    /// Request draining also waits for this ownership to finish.
    pub query_reserved_runtime_bytes: usize,
    /// Atomically published coordinator stage, readable during active calls.
    pub shutdown_stage: ShutdownStage,
    pub readiness: Readiness,
    pub dynamic_placement: bool,
    /// Shared authoritative map generation, sampled with the placement map.
    pub placement_generation: Option<u64>,
    pub fragment_count: usize,
    pub fragments: Vec<FragmentStatus>,
    pub replication: Vec<ReplicationStatus>,
    /// Exact completed shutdown fence evidence; absent before such a fence.
    pub completed_replication_drain: Option<ReplicationDrainReport>,
    pub observation: SnapshotObservation,
}

impl Node {
    /// Observe node lifecycle, placement and replication using default limits.
    /// Contended report locks are marked unavailable; collection never waits
    /// for shutdown, actor execution, reconciliation or storage.
    pub fn status_snapshot(&self) -> NodeStatus {
        self.status_snapshot_with_limits(SnapshotLimits::default())
    }

    /// Observe a bounded subset of fragments and senders. Fragment sampling
    /// uses ascending IDs; sender sampling is unspecified when truncated.
    pub fn status_snapshot_with_limits(&self, limits: SnapshotLimits) -> NodeStatus {
        let started = Instant::now();
        let observed_at = SystemTime::now();
        let lifecycle = self.drain_status();
        let lifecycle_revision = self.lifecycle_observation.revision.load(Ordering::Acquire);
        let mut observation = SnapshotObservation::default();
        let tables = match self.table_metrics.snapshot(limits.max_tables) {
            Some((tables, truncated)) => {
                observation.table_metrics_truncated = truncated;
                tables
            }
            None => {
                observation.table_metrics_unavailable = true;
                Vec::new()
            }
        };
        let shutdown_stage = self.lifecycle_observation.observed_shutdown();
        let mut fragments = Vec::new();
        let mut placement_generation = None;
        let mut readiness = Readiness::Unknown;
        let established = self
            .lifecycle_observation
            .established
            .load(Ordering::Acquire);
        let desired_count = self
            .lifecycle_observation
            .desired_count
            .load(Ordering::Acquire);
        let mut serving_count = None;
        let mut available_count = None;

        // This is the placement transition lock order. All acquisitions are
        // nonwaiting and all guards leave this block before sender collection.
        if let (Ok(states), Ok(placement), Ok(serving), Ok(routes)) = (
            self.placement_states.try_lock(),
            self.fragment_map.try_read(),
            self.serving.try_read(),
            self.router.actors.try_read(),
        ) {
            placement_generation = Some(self.placement_generation.load(Ordering::Acquire));
            observation.fragments_truncated = self.router.fragment_count > limits.max_fragments;
            let mut missing_serving = false;
            for index in 0..self.router.fragment_count.min(limits.max_fragments) {
                let fragment = FragmentId(index as u32);
                let all_replicas = placement.get(&fragment).map(Vec::as_slice).unwrap_or(&[]);
                observation.replicas_truncated |= all_replicas.len() > 64;
                let replicas = all_replicas.iter().take(64).cloned().collect();
                let assigned_locally = (placement.is_empty()
                    && self.placement_controller.is_none())
                    || all_replicas.iter().any(|peer| peer == &self.node_id);
                let route_available = routes
                    .get(&fragment)
                    .is_some_and(|route| route.actor.get_status() == ractor::ActorStatus::Running);
                let is_serving = serving.contains(&fragment) && route_available;
                missing_serving |= assigned_locally && !is_serving;
                fragments.push(FragmentStatus {
                    activity: match states.get(&fragment) {
                        Some(FragmentPlacementState::Serving) if is_serving => {
                            FragmentActivity::Serving
                        }
                        Some(FragmentPlacementState::Draining) if route_available => {
                            FragmentActivity::Transferring
                        }
                        Some(FragmentPlacementState::Draining) => FragmentActivity::Retiring,
                        _ if assigned_locally => FragmentActivity::Acquiring,
                        _ => FragmentActivity::NotAssigned,
                    },
                    missing_peers: Vec::new(),
                    missing_peers_unknown: false,
                    replication_active: false,
                    blocker: None,
                    fragment,
                    replicas,
                    assigned_locally,
                    placement_state: states.get(&fragment).copied(),
                    serving: is_serving,
                    route_available,
                });
            }
            serving_count = Some(serving.len());
            let probe_count = serving.len().min(limits.max_fragments);
            let available = serving
                .iter()
                .take(probe_count)
                .filter(|fragment| {
                    routes.get(fragment).is_some_and(|route| {
                        route.actor.get_status() == ractor::ActorStatus::Running
                    })
                })
                .count();
            if probe_count == serving.len() {
                available_count = Some(available);
            }
            readiness = if !established {
                Readiness::NotReady
            } else if available > 0 || desired_count == 0 {
                Readiness::Ready
            } else if probe_count < serving.len()
                || observation.fragments_truncated && !missing_serving
            {
                Readiness::Unknown
            } else {
                Readiness::NotReady
            };
        } else {
            observation.placement_unavailable = true;
        }
        if let Ok(peers) = self.peers.try_lock() {
            for fragment in &mut fragments {
                fragment.missing_peers = fragment
                    .replicas
                    .iter()
                    .filter(|peer| {
                        *peer != &self.node_id
                            && !peers
                                .get(*peer)
                                .is_some_and(|channel| !*channel.cancel.borrow())
                    })
                    .cloned()
                    .collect();
                fragment.missing_peers_unknown = observation.replicas_truncated;
                let acquiring = fragment.activity == FragmentActivity::Acquiring
                    && fragment.placement_state != Some(FragmentPlacementState::Serving);
                let remote_count = fragment
                    .replicas
                    .iter()
                    .filter(|peer| *peer != &self.node_id)
                    .count();
                if acquiring
                    && !fragment.missing_peers_unknown
                    && !fragment.missing_peers.is_empty()
                    && (self.recovery.is_some() || fragment.missing_peers.len() == remote_count)
                {
                    fragment.activity = FragmentActivity::Stalled;
                    fragment.blocker = Some(LifecycleBlocker::MissingSources {
                        fragment: fragment.fragment,
                        peers: fragment.missing_peers.clone(),
                    });
                } else if acquiring {
                    fragment.blocker = Some(LifecycleBlocker::AwaitingAuthority {
                        fragment: fragment.fragment,
                    });
                }
            }
        } else {
            observation.peers_unavailable = true;
            for fragment in &mut fragments {
                fragment.missing_peers_unknown = true;
            }
        }
        if let Ok(sealed) = self.finalizing_senders.fragments.try_lock() {
            for fragment in &mut fragments {
                if sealed.contains_key(&fragment.fragment) {
                    fragment.activity = FragmentActivity::Retiring;
                }
            }
        } else {
            observation.retirement_unavailable = true;
        }

        let reports = match self.sender_reports.try_lock() {
            Ok(reports) => {
                observation.senders_truncated = reports.len() > limits.max_senders;
                reports
                    .iter()
                    .take(limits.max_senders)
                    .map(|(key, report)| (key.clone(), report.clone()))
                    .collect::<Vec<_>>()
            }
            Err(_) => {
                observation.senders_unavailable = true;
                Vec::new()
            }
        };
        let mut replication = Vec::with_capacity(reports.len());
        for ((fragment, peer_id), report) in reports {
            let Ok(report_guard) = report.try_lock() else {
                observation.unavailable_sender_reports += 1;
                continue;
            };
            let report_snapshot = report_guard.clone();
            drop(report_guard);
            // Replacement can reset a sender's counters while collection is
            // running. Do not publish an old instance as the current sender.
            let current = self.sender_reports.try_lock().is_ok_and(|reports| {
                reports
                    .get(&(fragment, peer_id.clone()))
                    .is_some_and(|current| std::sync::Arc::ptr_eq(current, &report))
            });
            if !current {
                observation.unavailable_sender_reports += 1;
                continue;
            }
            let purpose = fragments
                .binary_search_by_key(&fragment, |state| state.fragment)
                .ok()
                .map(|index| &fragments[index])
                .map(|state| match state.placement_state {
                    Some(FragmentPlacementState::Bootstrapping) => {
                        ReplicationPurpose::InitialCatchUp
                    }
                    Some(FragmentPlacementState::Draining) => ReplicationPurpose::PlacementDrain,
                    _ if state.serving => ReplicationPurpose::Background,
                    _ => ReplicationPurpose::Unknown,
                })
                .unwrap_or(ReplicationPurpose::Unknown);
            replication.push(ReplicationStatus {
                incarnation: report_snapshot.progress.incarnation,
                activity: if report_snapshot.progress.observed_at.is_none() {
                    ReplicationActivity::Unknown
                } else if report_snapshot.state == "disconnected" {
                    ReplicationActivity::WaitingForPeer
                } else if report_snapshot.progress.reconciliation_stage.is_some()
                    || report_snapshot.buffered != 0
                    || report_snapshot.in_flight != 0
                {
                    ReplicationActivity::Active
                } else {
                    ReplicationActivity::Steady
                },
                observed_at: report_snapshot.progress.observed_at,
                freshness: match report_snapshot
                    .progress
                    .observed_at
                    .and_then(|at| at.elapsed().ok())
                {
                    Some(age) if age <= limits.stale_after => ObservationFreshness::Fresh,
                    Some(_) => {
                        observation.stale_sender_reports += 1;
                        ObservationFreshness::Stale
                    }
                    None => {
                        observation.unavailable_sender_reports += 1;
                        ObservationFreshness::Unknown
                    }
                },
                last_successful_ack_at: report_snapshot.progress.last_successful_ack_at,
                last_successful_ack_durability: report_snapshot
                    .progress
                    .last_successful_ack_durability,
                last_successful_reconcile_at: report_snapshot.progress.last_successful_reconcile_at,
                reconciliation_stage: report_snapshot.progress.reconciliation_stage,
                expected_fetch_keys: report_snapshot.progress.expected_fetch_keys,
                received_fetch_keys: report_snapshot.progress.received_fetch_keys,
                timing_enabled: report_snapshot.progress.timing_enabled,
                oldest_retained_operation_age: report_snapshot
                    .progress
                    .oldest_retained_operation_age,
                fragment,
                peer_id,
                connection: match report_snapshot.state.as_str() {
                    "connected" => SenderConnectionState::Connected,
                    "disconnected" => SenderConnectionState::Disconnected,
                    "buffer_reset" => SenderConnectionState::ResetRequired,
                    _ => SenderConnectionState::Unknown,
                },
                purpose,
                session_id: report_snapshot.session_id,
                buffered_operations: report_snapshot.buffered,
                retained_bytes: report_snapshot.retained_bytes,
                in_flight_batches: report_snapshot.in_flight,
                completed_reconcile_rounds: report_snapshot.reconcile_rounds,
                last_ack_durability: report_snapshot.last_ack_durability,
                ack_durability_mismatch: report_snapshot.ack_durability_mismatch,
            });
        }
        replication.sort_by(|left, right| {
            (left.fragment, &left.peer_id).cmp(&(right.fragment, &right.peer_id))
        });
        for fragment in &mut fragments {
            fragment.replication_active = replication.iter().any(|sender| {
                sender.fragment == fragment.fragment
                    && (sender.activity == ReplicationActivity::Active
                        || sender.buffered_operations != 0
                        || sender.in_flight_batches != 0)
            });
        }
        let completed_replication_drain = match self.replication_drain_report.try_lock() {
            Ok(report) => {
                if report
                    .as_ref()
                    .is_some_and(|report| report.targets().len() > limits.max_senders)
                {
                    observation.drain_report_truncated = true;
                    None
                } else {
                    report.clone()
                }
            }
            Err(_) => {
                observation.drain_report_unavailable = true;
                None
            }
        };
        observation.placement_changed = placement_generation.is_some_and(|generation| {
            generation != self.placement_generation.load(Ordering::Acquire)
        });
        let lifecycle_after = self.drain_status();
        observation.lifecycle_changed = lifecycle.phase() != lifecycle_after.phase()
            || lifecycle_revision != self.lifecycle_observation.revision.load(Ordering::Acquire);
        if lifecycle.phase() != DrainPhase::Running
            || lifecycle_after.phase() != DrainPhase::Running
        {
            readiness = Readiness::NotReady;
        } else if observation.lifecycle_changed || observation.placement_changed {
            readiness = Readiness::Unknown;
        }
        let failure = match self.lifecycle_observation.failure.try_lock() {
            Ok(failure) => failure.clone(),
            Err(_) => {
                observation.lifecycle_reason_unavailable = true;
                None
            }
        };
        let blocked_fragment = fragments
            .iter()
            .find(|fragment| fragment.activity == FragmentActivity::Stalled);
        let node_lifecycle = match lifecycle.phase() {
            DrainPhase::Stopped => NodeLifecycle::Stopped,
            _ if failure.is_some() => NodeLifecycle::Blocked,
            DrainPhase::Draining | DrainPhase::Drained
                if shutdown_stage != ShutdownStage::NotStarted =>
            {
                NodeLifecycle::Stopping
            }
            DrainPhase::Draining | DrainPhase::Drained => NodeLifecycle::Draining,
            _ if !self
                .lifecycle_observation
                .startup_complete
                .load(Ordering::Acquire) =>
            {
                NodeLifecycle::Initializing
            }
            _ if !self.tasks.is_accepting() => NodeLifecycle::Failed,
            _ if !established => {
                if self.recovery.is_some() {
                    NodeLifecycle::Recovering
                } else {
                    NodeLifecycle::Initializing
                }
            }
            _ if readiness == Readiness::Ready => NodeLifecycle::Serving,
            _ if readiness == Readiness::Unknown => NodeLifecycle::Unknown,
            _ if blocked_fragment.is_some() => NodeLifecycle::Blocked,
            _ => NodeLifecycle::Recovering,
        };
        let lifecycle_blocker = if let Some(failure) = &failure {
            Some(LifecycleBlocker::LocalFailure(failure.clone()))
        } else if !self.tasks.is_accepting() && lifecycle.phase() == DrainPhase::Running {
            Some(LifecycleBlocker::RuntimeStopped)
        } else if node_lifecycle == NodeLifecycle::Unknown {
            Some(LifecycleBlocker::SnapshotUnavailable)
        } else if node_lifecycle != NodeLifecycle::Serving {
            blocked_fragment.and_then(|fragment| fragment.blocker.clone())
        } else {
            None
        };
        let mut startup = self.lifecycle_observation.snapshot(&self.node_id);
        startup.lifecycle = node_lifecycle;
        NodeStatus {
            initial_readiness_established: established,
            desired_local_fragments: desired_count,
            serving_fragments: serving_count,
            available_fragments: available_count,
            capacity_partial: available_count.is_none_or(|available| available < desired_count),
            startup,
            lifecycle_blocker,
            pools: Vec::new(),
            detailed: None,
            lifecycle: node_lifecycle,
            lifecycle_reason: failure,
            lifecycle_revision,
            node_id: self.node_id.clone(),
            boot_id: self.boot_id,
            observed_at,
            collection_duration: started.elapsed(),
            drain: lifecycle,
            completed_local_persistence_fences: self.local_drain.completed(),
            local_persistence_failure: self.local_drain.failure(),
            storage: self.storage_diagnostics(),
            request_timing_enabled: self.operation_gate.timing_enabled.load(Ordering::Acquire),
            request_duration: self.operation_gate.request_duration.snapshot(),
            request_waiter_timeouts: self.operation_gate.waiter_timeouts.load(Ordering::Relaxed),
            tables,
            queues: self.queue_diagnostics(),
            write_concerns: self.write_concern_diagnostics(),
            peer_fence: self.peer_fence_status(),
            query_reserved_runtime_bytes: self.query_admission.diagnostics().reserved_runtime_bytes,
            shutdown_stage,
            readiness,
            dynamic_placement: self.placement_controller.is_some(),
            placement_generation,
            fragment_count: self.router.fragment_count,
            fragments,
            replication,
            completed_replication_drain,
            observation,
        }
    }
}
impl NodeStatus {
    /// Age of this owned observation, not estimated remote LWW data lag.
    pub fn age(&self) -> Option<Duration> {
        self.observed_at.elapsed().ok()
    }
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.age().is_none_or(|age| age > max_age)
    }
    pub fn is_complete(&self) -> bool {
        self.observation.is_complete()
            && self
                .detailed
                .as_ref()
                .is_none_or(super::DetailedObservation::is_complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeConfig;
    #[tokio::test]
    async fn source_staleness_and_retirement_fences_are_explicit_without_changing_serving_to_replication(
    ) {
        let node = Node::start(
            "stale-status".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap();
        let old = SystemTime::now() - Duration::from_secs(60);
        let report = crate::replication::SenderStateReport {
            state: "connected".into(),
            session_id: 1,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            reconcile_rounds: 2,
            last_ack_durability: Some(ReplicationAckDurability::StableCommitted),
            ack_durability_mismatch: false,
            progress: crate::replication::SenderProgress {
                observed_at: Some(old),
                last_successful_ack_at: Some(old),
                last_successful_reconcile_at: Some(old),
                last_successful_ack_durability: Some(ReplicationAckDurability::StableCommitted),
                reconciliation_stage: Some(crate::replication::ReconciliationStage::RetryBackoff),
                ..Default::default()
            },
        };
        node.sender_reports.lock().await.insert(
            (FragmentId(0), "peer".into()),
            std::sync::Arc::new(tokio::sync::Mutex::new(report)),
        );
        let mut status = node.status_snapshot();
        assert_eq!(status.lifecycle, NodeLifecycle::Serving);
        assert_eq!(status.replication[0].freshness, ObservationFreshness::Stale);
        assert_eq!(status.observation.stale_sender_reports, 1);
        assert_eq!(
            status.replication[0].purpose,
            ReplicationPurpose::Background
        );
        assert!(!status.is_complete());
        status.observed_at = old;
        assert!(status.is_stale(Duration::from_secs(1)));
        node.placement_states
            .lock()
            .await
            .insert(FragmentId(0), FragmentPlacementState::Draining);
        node.serving.write().unwrap().remove(&FragmentId(0));
        assert_eq!(
            node.status_snapshot().fragments[0].activity,
            FragmentActivity::Transferring
        );
        let retirement = node.finalizing_senders.seal(FragmentId(0)).unwrap();
        assert_eq!(
            node.status_snapshot().fragments[0].activity,
            FragmentActivity::Retiring
        );
        drop(retirement);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn contended_sections_are_reported_without_waiting_and_legacy_routes_are_required() {
        let node = Node::start(
            "status-locks".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder().fragment_count(1).build(),
        )
        .await
        .unwrap();
        {
            let _placement_owner = node.placement_states.lock().await;
            let status = node.status_snapshot();
            assert!(status.observation.placement_unavailable);
            assert_eq!(status.readiness, Readiness::Unknown);
            assert!(status.fragments.is_empty());
        }
        {
            let _sender_owner = node.sender_reports.lock().await;
            let status = node.status_snapshot();
            assert!(status.observation.senders_unavailable);
            assert!(status.replication.is_empty());
        }
        {
            let _shutdown_owner = node.shutdown_state.lock().await;
            let status = node.status_snapshot();
            assert_eq!(status.shutdown_stage, ShutdownStage::NotStarted);
        }
        let status = node.status_snapshot();
        assert!(status.fragments[0].assigned_locally);
        assert_eq!(status.readiness, Readiness::Ready);
        node.router.actors.write().unwrap().remove(&FragmentId(0));
        let status = node.status_snapshot();
        assert_eq!(status.readiness, Readiness::NotReady);
        assert!(!status.fragments[0].route_available);
        node.shutdown().await.unwrap();
    }
}
