//! Backend-neutral pull metrics over existing bounded diagnostics.
//!
//! Counters and distribution summaries are cumulative for the descriptor's
//! source lifetime. Node scopes use `boot_id`; shared storage scopes use
//! `status.storage.scope_id`. Gauges are current approximate
//! observations. Collection does not install callbacks on the request path.
//! Exporters should replace cumulative values, or compute deltas themselves;
//! adding each observed counter value to a backend counter double counts it.

use super::drain::DrainPhase;
use super::status::{NodeStatus, Readiness, SnapshotLimits};
use super::Node;
use crate::shard::FragmentId;

/// How an exported value behaves across observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricKind {
    /// Monotonic cumulative amount for a node incarnation; never reset on read.
    Counter,
    /// Current observed value; can increase or decrease.
    Gauge,
    /// Cumulative count/sum/max of observations, not raw samples or quantiles.
    Distribution,
}

/// Units for metric values (distribution count is always a sample count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricUnit {
    Bytes,
    Requests,
    Operations,
    Batches,
    Events,
    Frames,
    Milliseconds,
    Nanoseconds,
    /// Zero or one.
    Boolean,
}

/// Counter reset identity is metadata rather than an unbounded metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricLifetime {
    NodeIncarnation,
    /// Shared PersistenceConfig resources may outlive/reappear on another node.
    PersistenceResources,
}

/// Stable metric name and meaning, independent of the recording backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetricDescriptor {
    pub name: &'static str,
    pub description: &'static str,
    pub kind: MetricKind,
    pub unit: MetricUnit,
    pub lifetime: MetricLifetime,
}

/// Bounded operational scopes. Keys, values, request IDs, session IDs and
/// error strings never become metric labels.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricScope {
    Node,
    /// A table name deliberately registered through `enable_table_metrics`.
    Table {
        table: String,
    },
    Fragment {
        fragment: FragmentId,
    },
    FragmentPeer {
        fragment: FragmentId,
        peer_id: String,
    },
}

/// Aggregate distribution evidence already measured by ActorDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DistributionSummary {
    pub count: u64,
    pub sum: u64,
    pub max: u64,
}

/// The kind of a value determines how a recording backend should handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricValue {
    Counter(u64),
    Gauge(u64),
    Distribution(DistributionSummary),
}

impl MetricValue {
    pub fn kind(&self) -> MetricKind {
        match self {
            Self::Counter(_) => MetricKind::Counter,
            Self::Gauge(_) => MetricKind::Gauge,
            Self::Distribution(_) => MetricKind::Distribution,
        }
    }
}

/// One owned observation suitable for an exporter to record or retain.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetricPoint {
    pub descriptor: &'static MetricDescriptor,
    pub scope: MetricScope,
    pub value: MetricValue,
}

/// Owned bounded points plus the status observation they were built from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    pub status: NodeStatus,
    pub points: Vec<MetricPoint>,
}

/// Recording adapter. Called synchronously only after collection has released
/// all node/report locks. A sink can safely re-enter node diagnostic APIs.
/// Exporter latency/panics belong to the caller; no background exporter task
/// or backend runtime is selected by ActorDB.
pub trait MetricsSink {
    fn record(&self, point: &MetricPoint);
}

impl<F: Fn(&MetricPoint)> MetricsSink for F {
    fn record(&self, point: &MetricPoint) {
        self(point);
    }
}

impl MetricsSnapshot {
    /// Send an already collected observation to a backend adapter.
    pub fn record(&self, sink: &(impl MetricsSink + ?Sized)) {
        for point in &self.points {
            sink.record(point);
        }
    }
}

macro_rules! metric {
    ($constant:ident, $name:literal, $kind:ident, $unit:ident, $description:literal) => {
        metric!(
            $constant,
            $name,
            $kind,
            $unit,
            $description,
            NodeIncarnation
        );
    };
    ($constant:ident, $name:literal, $kind:ident, $unit:ident, $description:literal, $lifetime:ident) => {
        const $constant: MetricDescriptor = MetricDescriptor {
            name: $name,
            description: $description,
            kind: MetricKind::$kind,
            unit: MetricUnit::$unit,
            lifetime: MetricLifetime::$lifetime,
        };
    };
}

metric!(
    REQUEST_TIMEOUTS,
    "actordb.request.waiter_timeouts",
    Counter,
    Requests,
    "Expired request waiters; actual execution can remain outstanding."
);
metric!(
    REQUEST_DURATION,
    "actordb.request.owned_duration",
    Distribution,
    Nanoseconds,
    "Time through final request ownership release; includes queue/execution and terminal errors."
);
metric!(
    LOCAL_FENCES_ACTIVE,
    "actordb.drain.active_persistence_fences",
    Gauge,
    Operations,
    "Owned local persistence drain fences not yet terminal."
);
metric!(
    LOCAL_FENCES_COMPLETED,
    "actordb.drain.completed_persistence_fences",
    Counter,
    Operations,
    "Successful local drain persistence fences; not peer evidence."
);
metric!(
    STORAGE_QUEUED,
    "actordb.storage.queued_jobs",
    Gauge,
    Operations,
    "Backend jobs waiting for a bounded executor slot.",
    PersistenceResources
);
metric!(
    STORAGE_COMPLETED,
    "actordb.storage.completed_jobs",
    Counter,
    Operations,
    "Actual backend calls completed, including failures.",
    PersistenceResources
);
metric!(
    STORAGE_FAILED,
    "actordb.storage.failed_jobs",
    Counter,
    Events,
    "Actual failed backend calls, including panics.",
    PersistenceResources
);
metric!(
    STORAGE_QUEUE_WAIT,
    "actordb.storage.queue_wait",
    Distribution,
    Nanoseconds,
    "Monotonic executor queue wait for admitted backend calls.",
    PersistenceResources
);
metric!(
    STORAGE_EXECUTION,
    "actordb.storage.execution_time",
    Distribution,
    Nanoseconds,
    "Actual backend execution duration through return or panic.",
    PersistenceResources
);
metric!(
    TABLE_ACCEPTED,
    "actordb.table.accepted_slots",
    Counter,
    Requests,
    "Accepted ownership slots for a deliberately registered table."
);
metric!(
    TABLE_COMPLETED,
    "actordb.table.completed_slots",
    Counter,
    Requests,
    "Registered table ownership released through terminal execution/response."
);
metric!(
    TABLE_REJECTED,
    "actordb.table.rejected_slots",
    Counter,
    Requests,
    "Registered table lifecycle admission rejections."
);
metric!(
    TABLE_ACTIVE,
    "actordb.table.active_slots",
    Gauge,
    Requests,
    "Registered table slots still owning request or response work."
);
metric!(
    TABLE_DURATION,
    "actordb.table.owned_duration",
    Distribution,
    Nanoseconds,
    "Registered table slot duration through final ownership release."
);

metric!(
    ADMISSION_OPEN,
    "actordb.node.admission_open",
    Gauge,
    Boolean,
    "One while public operation admission is open."
);
metric!(
    READY,
    "actordb.node.ready",
    Gauge,
    Boolean,
    "Observed readiness; omitted when unknown."
);
metric!(
    ACTIVE_REQUESTS,
    "actordb.node.active_requests",
    Gauge,
    Operations,
    "Admitted request/actor ownership slots; batches may own several slots."
);
metric!(
    RECEIVE_APPLIES,
    "actordb.node.active_replication_applies",
    Gauge,
    Frames,
    "Decoded replication frames retained through actor/sender processing."
);
metric!(
    EMBEDDED_REQUESTS,
    "actordb.embedded.active_requests",
    Gauge,
    Requests,
    "Embedded request leases, including work after waiter cancellation."
);
metric!(
    EMBEDDED_BYTES,
    "actordb.embedded.retained_request_bytes",
    Gauge,
    Bytes,
    "Conservative admitted embedded request storage charge."
);
metric!(
    EMBEDDED_COMPLETIONS,
    "actordb.embedded.mutation_completions",
    Gauge,
    Operations,
    "Embedded mutation owners waiting for actual apply and fanout completion."
);
metric!(
    PEER_BYTES,
    "actordb.peer_writer.admitted_bytes",
    Gauge,
    Bytes,
    "Encoded peer body reservations, queued frames and active writes."
);
metric!(
    PEER_CAPACITY,
    "actordb.peer_writer.capacity_bytes",
    Gauge,
    Bytes,
    "Aggregate encoded peer body admission capacity."
);
metric!(
    PEER_PRODUCER_BYTES,
    "actordb.peer_writer.producer_admitted_bytes",
    Gauge,
    Bytes,
    "Encoded ordinary producer body admission, excluding priority replies."
);
metric!(
    PEER_SATURATIONS,
    "actordb.peer_writer.saturation_events",
    Counter,
    Events,
    "Failed immediate aggregate byte acquisitions, including attempts later admitted by waiting."
);
metric!(
    REPLICATION_BYTES,
    "actordb.replication.retained_bytes",
    Gauge,
    Bytes,
    "Charged replication payload bytes across all local senders."
);
metric!(
    RECONCILIATION_BYTES,
    "actordb.reconciliation.retained_bytes",
    Gauge,
    Bytes,
    "Charged sender reconciliation page bytes."
);
metric!(
    PERSISTENCE_BYTES,
    "actordb.persistence.retained_bytes",
    Gauge,
    Bytes,
    "Charged authoritative persistence ownership bytes."
);
metric!(
    STORAGE_JOBS,
    "actordb.storage.active_jobs",
    Gauge,
    Operations,
    "Submitted synchronous storage jobs still holding an execution slot."
);
metric!(QUERY_RUNTIME_BYTES, "actordb.query.reserved_runtime_bytes", Gauge, Bytes, "Query/scan result, merge page and actor work byte reservations, including work after caller cancellation.");
metric!(
    QUERY_RUNTIME_CAPACITY,
    "actordb.query.runtime_capacity_bytes",
    Gauge,
    Bytes,
    "Node-wide configured query/scan runtime byte allowance."
);
metric!(
    QUERY_REJECTIONS,
    "actordb.query.admission_rejections",
    Counter,
    Events,
    "Query/scan admissions rejected at a count or byte limit."
);
metric!(
    ACCEPTED_REQUESTS,
    "actordb.admission.accepted_slots",
    Counter,
    Operations,
    "Cumulative accepted request/actor gate slots, not unique user requests."
);
metric!(
    COMPLETED_REQUESTS,
    "actordb.admission.completed_slots",
    Counter,
    Operations,
    "Cumulative released admitted request/actor gate slots; not successful writes."
);
metric!(
    REJECTED_REQUESTS,
    "actordb.admission.rejected_slots",
    Counter,
    Operations,
    "Cumulative operation gate admission attempts rejected by the lifecycle fence."
);
metric!(
    DRAIN_STARTED,
    "actordb.drain.started_calls",
    Counter,
    Events,
    "Bounded local drain/shutdown calls started with valid options."
);
metric!(
    DRAIN_COMPLETED,
    "actordb.drain.completed_calls",
    Counter,
    Events,
    "Bounded local drain/shutdown calls returning success."
);
metric!(
    DRAIN_TIMEOUTS,
    "actordb.drain.timed_out_calls",
    Counter,
    Events,
    "Bounded local drain/shutdown calls exhausting their total budget."
);
metric!(
    DRAIN_FAILURES,
    "actordb.drain.failed_calls",
    Counter,
    Events,
    "Bounded local drain/shutdown calls returning a local error."
);
metric!(
    DRAIN_CANCELLED,
    "actordb.drain.cancelled_calls",
    Counter,
    Events,
    "Bounded local drain/shutdown futures dropped before a terminal return."
);
metric!(DRAIN_DURATION, "actordb.drain.total_duration_ms", Counter, Milliseconds, "Cumulative monotonic elapsed time of terminal or cancelled bounded local drain/shutdown calls.");
metric!(
    FRAGMENT_SERVING,
    "actordb.fragment.serving",
    Gauge,
    Boolean,
    "Local serving membership and published actor route, independent of node admission."
);
metric!(
    FRAGMENT_ASSIGNED,
    "actordb.fragment.assigned",
    Gauge,
    Boolean,
    "Authoritative placement assigns this fragment to the local node."
);
metric!(
    SENDER_BUFFERED,
    "actordb.sender.buffered_operations",
    Gauge,
    Operations,
    "Operations currently buffered in this sender."
);
metric!(
    SENDER_BYTES,
    "actordb.sender.retained_bytes",
    Gauge,
    Bytes,
    "Charged bytes across sender ingress, queued and in-flight work."
);
metric!(
    SENDER_IN_FLIGHT,
    "actordb.sender.in_flight_batches",
    Gauge,
    Batches,
    "Current in-flight replication batches."
);
metric!(
    SENDER_ROUNDS,
    "actordb.sender.completed_reconcile_rounds",
    Gauge,
    Operations,
    "Completed rounds for this sender instance; replacement resets this gauge."
);
metric!(
    SENDER_ACK_MISMATCH,
    "actordb.sender.ack_durability_mismatch",
    Gauge,
    Boolean,
    "Most recent matching ACK was below its batch requirement."
);
metric!(
    EXPIRY_RETAINED_EVENTS,
    "actordb.expiry.retained_events",
    Gauge,
    Events,
    "Queued and in-flight retained expiry notifications."
);
metric!(
    EXPIRY_RETAINED_BYTES,
    "actordb.expiry.retained_bytes",
    Gauge,
    Bytes,
    "Queued and in-flight retained expiry notification bytes."
);
metric!(
    EXPIRY_QUEUED_EVENTS,
    "actordb.expiry.queued_events",
    Gauge,
    Events,
    "Notifications waiting for callback dispatch."
);
metric!(
    EXPIRY_QUEUED_BYTES,
    "actordb.expiry.queued_bytes",
    Gauge,
    Bytes,
    "Charged queued expiry notification bytes."
);
metric!(
    EXPIRY_IN_FLIGHT_EVENTS,
    "actordb.expiry.in_flight_events",
    Gauge,
    Events,
    "Notifications in a running callback."
);
metric!(
    EXPIRY_IN_FLIGHT_BYTES,
    "actordb.expiry.in_flight_bytes",
    Gauge,
    Bytes,
    "Charged bytes in a running expiry callback."
);
metric!(
    EXPIRY_ENQUEUED,
    "actordb.expiry.enqueued_events",
    Counter,
    Events,
    "Expiry events admitted to callback ownership."
);
metric!(
    EXPIRY_BATCHES,
    "actordb.expiry.dispatched_batches",
    Counter,
    Batches,
    "Expiry callback batches dispatched."
);
metric!(
    EXPIRY_DELIVERED,
    "actordb.expiry.delivered_events",
    Counter,
    Events,
    "Events in callbacks returning success; not recipient acknowledgements."
);
metric!(
    EXPIRY_FAILED,
    "actordb.expiry.failed_events",
    Counter,
    Events,
    "Events in failed callback batches."
);
metric!(
    EXPIRY_ABANDONED,
    "actordb.expiry.abandoned_events",
    Counter,
    Events,
    "Events abandoned by callback shutdown or cancellation."
);
metric!(
    EXPIRY_DROPPED,
    "actordb.expiry.dropped_events",
    Counter,
    Events,
    "Notifications dropped before callback admission."
);
metric!(
    EXPIRY_DROP_COUNT,
    "actordb.expiry.dropped_event_capacity",
    Counter,
    Events,
    "Notifications dropped due to the event count limit."
);
metric!(
    EXPIRY_DROP_BYTES,
    "actordb.expiry.dropped_byte_capacity",
    Counter,
    Events,
    "Notifications dropped due to the retained byte limit."
);
metric!(
    EXPIRY_DROP_CLOSED,
    "actordb.expiry.dropped_closed",
    Counter,
    Events,
    "Notifications dropped because callback admission was closed."
);
metric!(
    EXPIRY_ERRORS,
    "actordb.expiry.handler_errors",
    Counter,
    Events,
    "Expiry callbacks returning an error."
);
metric!(
    EXPIRY_PANICS,
    "actordb.expiry.handler_panics",
    Counter,
    Events,
    "Expiry callback panics."
);
metric!(
    EXPIRY_TIMEOUTS,
    "actordb.expiry.handler_timeouts",
    Counter,
    Events,
    "Expiry callback timeouts."
);
metric!(EXPIRY_TO_DISPATCH, "actordb.expiry.expiry_to_dispatch", Distribution, Milliseconds, "Cumulative wall-clock expiry-to-dispatch summary; backwards clock differences saturate at zero.");
metric!(
    QUEUE_TO_DISPATCH,
    "actordb.expiry.queue_to_dispatch",
    Distribution,
    Milliseconds,
    "Cumulative monotonic callback queue residence summary."
);

metric!(
    QUEUED_EMBEDDED,
    "actordb.request.queued_embedded_mutations",
    Gauge,
    Requests,
    "Mutations queued in the embedded completion manager."
);
metric!(
    QUEUED_FORWARDED_WRITES,
    "actordb.request.queued_forwarded_mutations",
    Gauge,
    Requests,
    "Forwarded writes queued before completion-worker execution."
);
metric!(
    FORWARDED_APPLY_SLOTS,
    "actordb.request.forwarded_apply_slots",
    Gauge,
    Requests,
    "Forwarded apply admission use; concern waiters are separate."
);
metric!(
    QUEUED_FORWARDED_READS,
    "actordb.request.queued_forwarded_reads",
    Gauge,
    Requests,
    "Forwarded reads queued before actor handoff."
);
metric!(
    FORWARDED_READ_SLOTS,
    "actordb.request.forwarded_read_slots",
    Gauge,
    Requests,
    "Forwarded read slots retained through actual actor/response ownership."
);
metric!(
    AWAITING_RESPONSES,
    "actordb.request.awaiting_responses",
    Gauge,
    Requests,
    "Accepted request response slots held through socket write or terminal drop."
);
metric!(
    CONCERN_ACCEPTED,
    "actordb.write_concern.accepted",
    Counter,
    Requests,
    "Accepted captured write-concern tickets."
);
metric!(
    CONCERN_SATISFIED,
    "actordb.write_concern.satisfied",
    Counter,
    Requests,
    "Tickets satisfied by validated acknowledgement/durability evidence."
);
metric!(
    CONCERN_TIMEOUTS,
    "actordb.write_concern.timed_out",
    Counter,
    Requests,
    "Concern deadlines expired with explicit partial progress."
);
metric!(
    CONCERN_DURABILITY_FAILURES,
    "actordb.write_concern.durability_failed",
    Counter,
    Events,
    "Required local durability was not achieved."
);
metric!(
    CONCERN_OPERATION_FAILURES,
    "actordb.write_concern.operation_failed",
    Counter,
    Events,
    "Operation failed/superseded before concern success."
);
metric!(
    CONCERN_UNKNOWN,
    "actordb.write_concern.unknown_outcomes",
    Counter,
    Events,
    "Lost or uncertain operation/fence outcomes."
);
metric!(
    CONCERN_REJECTED,
    "actordb.write_concern.rejected",
    Counter,
    Requests,
    "Rejected write-concern policies or waiter admission."
);
metric!(
    CONCERN_CANCELLED,
    "actordb.write_concern.cancelled",
    Counter,
    Requests,
    "Concern waiter cancellations, not execution completion."
);
metric!(
    CONCERN_ACTIVE,
    "actordb.write_concern.active",
    Gauge,
    Requests,
    "Captured concern tickets still owned."
);
metric!(
    CONCERN_WAIT,
    "actordb.write_concern.wait_duration",
    Distribution,
    Nanoseconds,
    "Actual concern wait duration through terminal waiter outcome."
);
metric!(
    PEER_ACCEPTED,
    "actordb.peer.admitted_frames",
    Counter,
    Frames,
    "Authenticated decoded frames accepted by receiver authority."
);
metric!(
    PEER_COMPLETED,
    "actordb.peer.completed_frames",
    Counter,
    Frames,
    "Receiver frame ownership terminally released after actor/store/response work."
);
metric!(
    PEER_REJECTED,
    "actordb.peer.rejected_frames",
    Counter,
    Frames,
    "Receiver authority frame rejections."
);
metric!(
    PEER_HANDSHAKES_REJECTED,
    "actordb.peer.rejected_handshakes",
    Counter,
    Events,
    "Rejected process authentication/authority handshakes."
);
metric!(
    PEER_PENDING_FRAMES,
    "actordb.peer.outstanding_fence_frames",
    Gauge,
    Frames,
    "Old authority frame ownership not yet drained."
);
metric!(
    PEER_PENDING_PERSISTENCE,
    "actordb.peer.outstanding_fence_persistence",
    Gauge,
    Operations,
    "Old authority persistence fence work not yet terminal."
);

impl Node {
    /// Collect metrics with default fragment/sender limits.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics_snapshot_with_limits(SnapshotLimits::default())
    }

    /// Collect bounded operational observations. Unavailable sections are
    /// omitted and described by `status.observation`, never replaced by zero.
    /// Gauges and atomic cumulative summaries are approximate independent
    /// reads, rather than a transaction across subsystems.
    pub fn metrics_snapshot_with_limits(&self, limits: SnapshotLimits) -> MetricsSnapshot {
        let status = self.status_snapshot_with_limits(limits);
        let mut points = Vec::new();
        let mut node_point = |descriptor: &'static MetricDescriptor, value| {
            points.push(MetricPoint {
                descriptor,
                scope: MetricScope::Node,
                value,
            });
        };
        node_point(
            &ADMISSION_OPEN,
            MetricValue::Gauge(u64::from(status.lifecycle.phase() == DrainPhase::Running)),
        );
        match status.readiness {
            Readiness::Ready => node_point(&READY, MetricValue::Gauge(1)),
            Readiness::NotReady => node_point(&READY, MetricValue::Gauge(0)),
            Readiness::Unknown => {}
        }
        node_point(
            &ACTIVE_REQUESTS,
            MetricValue::Gauge(status.lifecycle.active_requests() as u64),
        );
        node_point(
            &RECEIVE_APPLIES,
            MetricValue::Gauge(status.lifecycle.active_replication_applies() as u64),
        );
        node_point(
            &REQUEST_TIMEOUTS,
            MetricValue::Counter(status.request_waiter_timeouts),
        );
        node_point(
            &LOCAL_FENCES_ACTIVE,
            MetricValue::Gauge(status.lifecycle.active_persistence_fences() as u64),
        );
        node_point(
            &LOCAL_FENCES_COMPLETED,
            MetricValue::Counter(status.completed_local_persistence_fences as u64),
        );
        for (descriptor, value) in [
            (
                &STORAGE_QUEUED,
                MetricValue::Gauge(status.storage.queued_jobs as u64),
            ),
            (
                &STORAGE_COMPLETED,
                MetricValue::Counter(status.storage.completed_jobs),
            ),
            (
                &STORAGE_FAILED,
                MetricValue::Counter(status.storage.failed_jobs),
            ),
        ] {
            node_point(descriptor, value);
        }
        if status.request_timing_enabled {
            node_point(
                &REQUEST_DURATION,
                MetricValue::Distribution(DistributionSummary {
                    count: status.request_duration.count,
                    sum: status.request_duration.sum_nanos,
                    max: status.request_duration.max_nanos,
                }),
            );
        }
        if status.storage.timing_enabled {
            for (descriptor, duration) in [
                (&STORAGE_QUEUE_WAIT, status.storage.queue_wait),
                (&STORAGE_EXECUTION, status.storage.execution_time),
            ] {
                node_point(
                    descriptor,
                    MetricValue::Distribution(DistributionSummary {
                        count: duration.count,
                        sum: duration.sum_nanos,
                        max: duration.max_nanos,
                    }),
                );
            }
        }
        for (descriptor, value) in [
            (&QUEUED_EMBEDDED, status.queues.queued_embedded_mutations),
            (
                &QUEUED_FORWARDED_WRITES,
                status.queues.queued_forwarded_mutations,
            ),
            (&FORWARDED_APPLY_SLOTS, status.queues.forwarded_apply_slots),
            (
                &QUEUED_FORWARDED_READS,
                status.queues.queued_forwarded_reads,
            ),
            (&FORWARDED_READ_SLOTS, status.queues.forwarded_read_slots),
            (&AWAITING_RESPONSES, status.lifecycle.awaiting_responses()),
        ] {
            node_point(descriptor, MetricValue::Gauge(value as u64));
        }
        for (descriptor, value) in [
            (&CONCERN_ACCEPTED, status.write_concerns.accepted),
            (&CONCERN_SATISFIED, status.write_concerns.satisfied),
            (&CONCERN_TIMEOUTS, status.write_concerns.timed_out),
            (
                &CONCERN_DURABILITY_FAILURES,
                status.write_concerns.durability_failed,
            ),
            (
                &CONCERN_OPERATION_FAILURES,
                status.write_concerns.operation_failed,
            ),
            (&CONCERN_UNKNOWN, status.write_concerns.unknown_outcomes),
            (&CONCERN_REJECTED, status.write_concerns.rejected),
            (&CONCERN_CANCELLED, status.write_concerns.cancelled),
            (&PEER_ACCEPTED, status.peer_fence.accepted_frames()),
            (&PEER_COMPLETED, status.peer_fence.completed_frames()),
            (&PEER_REJECTED, status.peer_fence.rejected_frames()),
            (
                &PEER_HANDSHAKES_REJECTED,
                status.peer_fence.rejected_handshakes(),
            ),
        ] {
            node_point(descriptor, MetricValue::Counter(value));
        }
        node_point(
            &CONCERN_ACTIVE,
            MetricValue::Gauge(status.write_concerns.active),
        );
        node_point(
            &CONCERN_WAIT,
            MetricValue::Distribution(DistributionSummary {
                count: status.write_concerns.wait_count,
                sum: status.write_concerns.wait_nanos,
                max: status.write_concerns.max_wait_nanos,
            }),
        );
        node_point(
            &PEER_PENDING_FRAMES,
            MetricValue::Gauge(status.peer_fence.outstanding_frames() as u64),
        );
        node_point(
            &PEER_PENDING_PERSISTENCE,
            MetricValue::Gauge(status.peer_fence.outstanding_persistence() as u64),
        );
        let drain = self.drain_diagnostics();
        for (descriptor, value) in [
            (&ACCEPTED_REQUESTS, drain.accepted_requests()),
            (&COMPLETED_REQUESTS, drain.completed_requests()),
            (&REJECTED_REQUESTS, drain.rejected_requests()),
            (&DRAIN_STARTED, drain.started_drains()),
            (&DRAIN_COMPLETED, drain.completed_drains()),
            (&DRAIN_TIMEOUTS, drain.timed_out_drains()),
            (&DRAIN_FAILURES, drain.failed_drains()),
            (&DRAIN_CANCELLED, drain.cancelled_drains()),
            (
                &DRAIN_DURATION,
                drain
                    .total_drain_duration()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            ),
        ] {
            node_point(descriptor, MetricValue::Counter(value));
        }
        let embedded = self.embedded_operation_diagnostics();
        node_point(
            &EMBEDDED_REQUESTS,
            MetricValue::Gauge(embedded.active_requests() as u64),
        );
        node_point(
            &EMBEDDED_BYTES,
            MetricValue::Gauge(embedded.retained_request_bytes() as u64),
        );
        node_point(
            &EMBEDDED_COMPLETIONS,
            MetricValue::Gauge(embedded.active_mutation_completions() as u64),
        );
        let peer = self.peer_writer_diagnostics();
        node_point(
            &PEER_BYTES,
            MetricValue::Gauge(peer.admitted_bytes() as u64),
        );
        node_point(
            &PEER_CAPACITY,
            MetricValue::Gauge(peer.capacity_bytes() as u64),
        );
        node_point(
            &PEER_PRODUCER_BYTES,
            MetricValue::Gauge(peer.producer_admitted_bytes() as u64),
        );
        node_point(
            &PEER_SATURATIONS,
            MetricValue::Counter(peer.saturation_events()),
        );
        node_point(
            &REPLICATION_BYTES,
            MetricValue::Gauge(self.replication_retained_bytes() as u64),
        );
        node_point(
            &RECONCILIATION_BYTES,
            MetricValue::Gauge(self.reconciliation_retained_bytes() as u64),
        );
        node_point(
            &PERSISTENCE_BYTES,
            MetricValue::Gauge(self.persistence_retained_bytes() as u64),
        );
        node_point(
            &STORAGE_JOBS,
            MetricValue::Gauge(self.active_storage_jobs() as u64),
        );
        let query = self.query_admission.diagnostics();
        node_point(
            &QUERY_RUNTIME_BYTES,
            MetricValue::Gauge(query.reserved_runtime_bytes as u64),
        );
        node_point(
            &QUERY_RUNTIME_CAPACITY,
            MetricValue::Gauge(query.runtime_byte_capacity as u64),
        );
        node_point(
            &QUERY_REJECTIONS,
            MetricValue::Counter(query.admission_rejections),
        );
        if let Some(expiry) = self.ttl_expiry_diagnostics() {
            for (descriptor, value) in [
                (&EXPIRY_RETAINED_EVENTS, expiry.retained_events),
                (&EXPIRY_RETAINED_BYTES, expiry.retained_bytes),
                (&EXPIRY_QUEUED_EVENTS, expiry.queued_events),
                (&EXPIRY_QUEUED_BYTES, expiry.queued_bytes),
                (&EXPIRY_IN_FLIGHT_EVENTS, expiry.in_flight_events),
                (&EXPIRY_IN_FLIGHT_BYTES, expiry.in_flight_bytes),
            ] {
                node_point(descriptor, MetricValue::Gauge(value as u64));
            }
            for (descriptor, value) in [
                (&EXPIRY_ENQUEUED, expiry.enqueued_events),
                (&EXPIRY_BATCHES, expiry.dispatched_batches),
                (&EXPIRY_DELIVERED, expiry.delivered_events),
                (&EXPIRY_FAILED, expiry.failed_events),
                (&EXPIRY_ABANDONED, expiry.abandoned_events),
                (&EXPIRY_DROPPED, expiry.dropped_events),
                (&EXPIRY_DROP_COUNT, expiry.dropped_event_capacity),
                (&EXPIRY_DROP_BYTES, expiry.dropped_byte_capacity),
                (&EXPIRY_DROP_CLOSED, expiry.dropped_closed),
                (&EXPIRY_ERRORS, expiry.handler_errors),
                (&EXPIRY_PANICS, expiry.handler_panics),
                (&EXPIRY_TIMEOUTS, expiry.handler_timeouts),
            ] {
                node_point(descriptor, MetricValue::Counter(value));
            }
            for (descriptor, sum, max) in [
                (
                    &EXPIRY_TO_DISPATCH,
                    expiry.expiry_to_dispatch_total_ms,
                    expiry.expiry_to_dispatch_max_ms,
                ),
                (
                    &QUEUE_TO_DISPATCH,
                    expiry.queue_to_dispatch_total_ms,
                    expiry.queue_to_dispatch_max_ms,
                ),
            ] {
                node_point(
                    descriptor,
                    MetricValue::Distribution(DistributionSummary {
                        count: expiry.latency_samples,
                        sum,
                        max,
                    }),
                );
            }
        }
        for table in &status.tables {
            for (descriptor, value) in [
                (&TABLE_ACCEPTED, MetricValue::Counter(table.accepted)),
                (&TABLE_COMPLETED, MetricValue::Counter(table.completed)),
                (&TABLE_REJECTED, MetricValue::Counter(table.rejected)),
                (&TABLE_ACTIVE, MetricValue::Gauge(table.active as u64)),
                (
                    &TABLE_DURATION,
                    MetricValue::Distribution(DistributionSummary {
                        count: table.duration.count,
                        sum: table.duration.sum_nanos,
                        max: table.duration.max_nanos,
                    }),
                ),
            ] {
                points.push(MetricPoint {
                    descriptor,
                    scope: MetricScope::Table {
                        table: table.table.clone(),
                    },
                    value,
                });
            }
        }
        for fragment in &status.fragments {
            for (descriptor, value) in [
                (&FRAGMENT_SERVING, fragment.serving),
                (&FRAGMENT_ASSIGNED, fragment.assigned_locally),
            ] {
                points.push(MetricPoint {
                    descriptor,
                    scope: MetricScope::Fragment {
                        fragment: fragment.fragment,
                    },
                    value: MetricValue::Gauge(u64::from(value)),
                });
            }
        }
        for sender in &status.replication {
            for (descriptor, value) in [
                (&SENDER_BUFFERED, sender.buffered_operations as u64),
                (&SENDER_BYTES, sender.retained_bytes as u64),
                (&SENDER_IN_FLIGHT, sender.in_flight_batches as u64),
                (&SENDER_ROUNDS, sender.completed_reconcile_rounds),
                (
                    &SENDER_ACK_MISMATCH,
                    u64::from(sender.ack_durability_mismatch),
                ),
            ] {
                points.push(MetricPoint {
                    descriptor,
                    scope: MetricScope::FragmentPeer {
                        fragment: sender.fragment,
                        peer_id: sender.peer_id.clone(),
                    },
                    value: MetricValue::Gauge(value),
                });
            }
        }
        MetricsSnapshot { status, points }
    }

    /// Snapshot first, then invoke exporter callbacks without any node locks.
    /// Returns the exact observation sent to the sink, including omissions.
    pub fn collect_metrics(&self, sink: &(impl MetricsSink + ?Sized)) -> MetricsSnapshot {
        let snapshot = self.metrics_snapshot();
        snapshot.record(sink);
        snapshot
    }
}
