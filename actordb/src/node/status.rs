//! Bounded observations of lifecycle, serving authority, and replication.
//!
//! No actor RPC, backend call, or asynchronous lock wait is made by collection.
//! A snapshot is an observation, not a write or durability fence. Check its
//! completeness and change indicators before using it to sequence placement.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

use super::drain::{DrainPhase, DrainStatus};
use super::lifecycle::ShutdownProgress;
use super::{Node, ReplicationDrainReport};
use crate::placement::FragmentPlacementState;
use crate::replication::ReplicationAckDurability;
use crate::shard::FragmentId;

/// Maximum fragment and sender records returned by one observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    max_fragments: usize,
    max_senders: usize,
    max_tables: usize,
}

impl SnapshotLimits {
    /// Set output limits, clamped to 4,096 records per collection category.
    /// Zero requests only aggregate lifecycle information.
    pub fn new(max_fragments: usize, max_senders: usize) -> Self {
        Self {
            max_fragments: max_fragments.min(4_096),
            max_senders: max_senders.min(4_096),
            max_tables: 64,
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
    /// Admission is open and every assigned fragment has a serving route.
    /// A routing node with no locally assigned fragments can be ready.
    Ready,
    /// Admission is closed, or at least one assigned fragment cannot serve.
    NotReady,
    /// Observation was incomplete or changed during collection.
    Unknown,
}

/// Last safely observable stage of full shutdown. Request-only drain does not
/// advance this coordinator. A running coordinator is reported unavailable.
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
    /// Catch-up percentage is not measured by the reconciliation protocol.
    /// An absent value must not be interpreted as zero or complete.
    pub catch_up_fraction: Option<f64>,
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
    /// No replication time/byte lag estimate is currently measured.
    pub lag: Option<Duration>,
}

/// Which sections could not be fully observed without waiting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotObservation {
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
    pub shutdown_stage_unavailable: bool,
    /// Published placement generation changed while other sections were read.
    pub placement_changed: bool,
    /// Admission/drain phase changed while other sections were read.
    pub lifecycle_changed: bool,
}

impl SnapshotObservation {
    /// Every requested section was sampled completely and no detected phase
    /// or placement change occurred. Independent gauges still are approximate.
    pub fn is_complete(&self) -> bool {
        !self.placement_unavailable
            && !self.table_metrics_unavailable
            && !self.table_metrics_truncated
            && !self.fragments_truncated
            && !self.replicas_truncated
            && !self.senders_unavailable
            && !self.senders_truncated
            && self.unavailable_sender_reports == 0
            && !self.drain_report_unavailable
            && !self.drain_report_truncated
            && !self.shutdown_stage_unavailable
            && !self.placement_changed
            && !self.lifecycle_changed
    }
}

/// Owned, bounded node observation. Reading this value never touches the node.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeStatus {
    pub node_id: String,
    /// Process incarnation, useful for detecting cumulative metric resets.
    pub boot_id: u64,
    pub observed_at: SystemTime,
    pub collection_duration: Duration,
    pub lifecycle: DrainStatus,
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
    /// Absent while a shutdown call owns the coordinator; no wait is made.
    pub shutdown_stage: Option<ShutdownStage>,
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
        let shutdown_stage = match self.shutdown_state.try_lock() {
            Ok(progress) => Some(match &*progress {
                ShutdownProgress::Running => ShutdownStage::NotStarted,
                ShutdownProgress::FencingReplication => ShutdownStage::ReplicationFence,
                ShutdownProgress::DrainingReplication(_) => ShutdownStage::ReplicationDrain,
                ShutdownProgress::StoppingTransport => ShutdownStage::StoppingTransport,
                ShutdownProgress::QuiescingFragments => ShutdownStage::QuiescingFragments,
                ShutdownProgress::StoppingFragments => ShutdownStage::StoppingFragments,
                ShutdownProgress::Stopped => ShutdownStage::Stopped,
            }),
            Err(_) => {
                observation.shutdown_stage_unavailable = true;
                None
            }
        };
        let mut fragments = Vec::new();
        let mut placement_generation = None;
        let mut readiness = Readiness::Unknown;

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
                    fragment,
                    replicas,
                    assigned_locally,
                    placement_state: states.get(&fragment).copied(),
                    serving: is_serving,
                    route_available,
                    catch_up_fraction: None,
                });
            }
            readiness = if missing_serving {
                Readiness::NotReady
            } else if observation.fragments_truncated {
                Readiness::Unknown
            } else {
                Readiness::Ready
            };
        } else {
            observation.placement_unavailable = true;
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
                lag: None,
            });
        }
        replication.sort_by(|left, right| {
            (left.fragment, &left.peer_id).cmp(&(right.fragment, &right.peer_id))
        });
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
        observation.lifecycle_changed = lifecycle.phase() != lifecycle_after.phase();
        if lifecycle.phase() != DrainPhase::Running
            || lifecycle_after.phase() != DrainPhase::Running
        {
            readiness = Readiness::NotReady;
        } else if observation.lifecycle_changed || observation.placement_changed {
            readiness = Readiness::Unknown;
        }
        NodeStatus {
            node_id: self.node_id.clone(),
            boot_id: self.boot_id,
            observed_at,
            collection_duration: started.elapsed(),
            lifecycle,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeConfig;

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
            assert!(status.observation.shutdown_stage_unavailable);
            assert!(status.shutdown_stage.is_none());
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
