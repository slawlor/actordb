//! Per-write processing acknowledgements on the normal asynchronous sender path.
//!
//! Strict acknowledgement policies reject an operational cap that would lower
//! the requested count before local apply admission. Placement, eligibility,
//! region labels, and the accepted count remain frozen for each waiter.
//! Acknowledgements describe processing of a mutation and preservation of an
//! equal-or-newer LWW winner, with configured storage durability. They do not
//! establish global ordering or guarantee that the submitted value stays live.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use sha2::{Digest, Sha256};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

mod overrides;
use overrides::effective_acknowledgement_count;

use crate::actor::{ApplyDurability, ReplicatedOp};
use crate::node::NodeError;
use crate::shard::FragmentId;
pub use actordb_protocol::write_concern::{
    WriteAcknowledgement, WriteConcern, WriteConcernEvidence, WriteConcernOptions,
    WriteConcernOutcome, WriteConcernPolicyError, WriteConcernReceipt, WriteDurability,
    WriteOperationEvidence, MAX_WRITE_CONCERN_REPLICAS,
};
mod regions;
use regions::RegionRequirement;

#[derive(Debug)]
#[non_exhaustive]
pub enum WriteConcernError {
    Operation(NodeError),
    InvalidPolicy(String),
    Unsupported(String),
    /// The update applied locally; the required acknowledgements did not arrive.
    TimedOut(WriteConcernReceipt),
    /// Apply completed; the explicit local durability fence failed or was unsupported.
    DurabilityFailed {
        receipt: Box<WriteConcernReceipt>,
        error: String,
    },
    Backpressure,
    /// A reply was lost or malformed. The update may have applied.
    UnknownOutcome(String),
}

impl fmt::Display for WriteConcernError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => error.fmt(f),
            Self::InvalidPolicy(error) | Self::Unsupported(error) | Self::UnknownOutcome(error) => {
                f.write_str(error)
            }
            Self::TimedOut(receipt) => write!(
                f,
                "write applied at {}; received {} of {} required acknowledgements",
                receipt.receiving_node(),
                receipt.acknowledged_nodes().len(),
                receipt.required_acknowledgements()
            ),
            Self::Backpressure => f.write_str("write concern waiter capacity exhausted"),
            Self::DurabilityFailed { error, .. } => {
                write!(f, "write applied but durability fence failed: {error}")
            }
        }
    }
}
impl std::error::Error for WriteConcernError {}
impl From<NodeError> for WriteConcernError {
    fn from(e: NodeError) -> Self {
        match e {
            NodeError::UnknownOutcome(error) => Self::UnknownOutcome(error),
            error => Self::Operation(error),
        }
    }
}
impl From<WriteConcernPolicyError> for WriteConcernError {
    fn from(e: WriteConcernPolicyError) -> Self {
        match e {
            WriteConcernPolicyError::Invalid(e) => Self::InvalidPolicy(e),
            WriteConcernPolicyError::Unsupported(e) => Self::Unsupported(e),
            _ => Self::Unsupported(e.to_string()),
        }
    }
}

impl WriteConcernError {
    pub(crate) fn into_node_error(self) -> NodeError {
        match self {
            Self::Operation(error) => error,
            Self::UnknownOutcome(error) => NodeError::UnknownOutcome(error),
            error => NodeError::Network(error.to_string()),
        }
    }
    pub(crate) fn outcome(&self) -> Option<WriteConcernOutcome> {
        match self {
            Self::InvalidPolicy(e) => Some(WriteConcernOutcome::InvalidPolicy(e.clone())),
            Self::Unsupported(e) => Some(WriteConcernOutcome::Unsupported(e.clone())),
            Self::TimedOut(receipt) => Some(WriteConcernOutcome::TimedOut(receipt.clone())),
            Self::DurabilityFailed { receipt, error } => {
                Some(WriteConcernOutcome::DurabilityFailed {
                    receipt: receipt.as_ref().clone(),
                    error: error.clone(),
                })
            }
            Self::Backpressure => Some(WriteConcernOutcome::Backpressure),
            Self::UnknownOutcome(error) => Some(WriteConcernOutcome::UnknownOutcome(error.clone())),
            _ => None,
        }
    }
}

pub(crate) fn decode_outcome(
    bytes: &[u8],
    options: &WriteConcernOptions,
) -> Result<WriteConcernReceipt, WriteConcernError> {
    match WriteConcernOutcome::decode(bytes).map_err(|e| {
        WriteConcernError::UnknownOutcome(format!("invalid write concern receipt: {e}"))
    })? {
        WriteConcernOutcome::Satisfied(receipt) => {
            receipt
                .validate_for(options, true)
                .map_err(|e| WriteConcernError::UnknownOutcome(e.to_string()))?;
            Ok(receipt)
        }
        WriteConcernOutcome::TimedOut(receipt) => {
            receipt
                .validate_for(options, false)
                .map_err(|e| WriteConcernError::UnknownOutcome(e.to_string()))?;
            Err(WriteConcernError::TimedOut(receipt))
        }
        WriteConcernOutcome::InvalidPolicy(e) => Err(WriteConcernError::InvalidPolicy(e)),
        WriteConcernOutcome::Unsupported(e) => Err(WriteConcernError::Unsupported(e)),
        WriteConcernOutcome::Backpressure => Err(WriteConcernError::Backpressure),
        WriteConcernOutcome::UnknownOutcome(error) => Err(WriteConcernError::UnknownOutcome(error)),
        WriteConcernOutcome::DurabilityFailed { receipt, error } => {
            receipt
                .validate_for(options, false)
                .map_err(|e| WriteConcernError::UnknownOutcome(e.to_string()))?;
            Err(WriteConcernError::DurabilityFailed {
                receipt: Box::new(receipt),
                error,
            })
        }
        _ => Err(WriteConcernError::UnknownOutcome(
            "unsupported write concern response".into(),
        )),
    }
}

#[derive(Debug)]
pub(crate) struct WriteConcernPlan {
    pub(crate) receiver: String,
    pub(crate) replica_count: u32,
    pub(crate) requested: u32,
    pub(crate) required: u32,
    pub(crate) eligible: HashSet<String>,
    pub(crate) options: WriteConcernOptions,
    region: Option<RegionRequirement>,
}
impl WriteConcernPlan {
    pub(crate) fn requires_wait(&self) -> bool {
        self.required > 1 || self.region.is_some()
    }
    fn complete(&self, nodes: &BTreeSet<String>) -> bool {
        nodes.len() >= self.required as usize
            && self
                .region
                .as_ref()
                .is_none_or(|region| region.satisfied_by(nodes))
    }
    fn receipt(&self, nodes: &BTreeSet<String>) -> WriteConcernReceipt {
        let receipt = WriteConcernReceipt::new(
            self.receiver.clone(),
            self.replica_count,
            self.requested,
            self.required,
            nodes.iter().cloned().collect(),
        )
        .expect("validated plan and distinct eligible acknowledgements");
        match &self.region {
            Some(region) => receipt
                .with_region_progress(
                    region.receiver_region().to_string(),
                    nodes
                        .iter()
                        .filter(|node| region.outside_nodes().contains(*node))
                        .cloned()
                        .collect(),
                )
                .expect("region progress contains distinct acknowledged eligible nodes"),
            None => receipt,
        }
    }
}

#[derive(Debug, Default)]
struct Progress {
    nodes: BTreeSet<String>,
    durability: BTreeMap<String, WriteDurability>,
    deadline: Option<Instant>,
    complete: bool,
}
#[derive(Debug)]
struct Ticket {
    plan: WriteConcernPlan,
    fragment: FragmentId,
    op: ReplicatedOp,
    certificate: WriteOperationEvidence,
    created: Instant,
    progress: Mutex<Progress>,
    changed: Notify,
}

#[derive(Debug)]
pub(crate) struct WriteConcernTracker {
    pub(crate) receiver: String,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    capacity: Arc<Semaphore>,
    acknowledgement_cap: AtomicU32,
    pending: Mutex<HashMap<[u8; 32], Vec<Arc<Ticket>>>>,
    regions: RwLock<HashMap<String, String>>,
    diagnostics: WriteConcernCounters,
}

/// Monotonic per-node write-concern preparation/completion counters. No key labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct WriteConcernDiagnostics {
    pub accepted: u64,
    pub satisfied: u64,
    pub timed_out: u64,
    pub durability_failed: u64,
    pub operation_failed: u64,
    pub unknown_outcomes: u64,
    pub rejected: u64,
    pub cancelled: u64,
    pub active: u64,
    pub wait_count: u64,
    pub wait_nanos: u64,
    pub max_wait_nanos: u64,
}
#[derive(Debug, Default)]
struct WriteConcernCounters {
    accepted: AtomicU64,
    satisfied: AtomicU64,
    timed_out: AtomicU64,
    durability_failed: AtomicU64,
    operation_failed: AtomicU64,
    unknown_outcomes: AtomicU64,
    rejected: AtomicU64,
    cancelled: AtomicU64,
    active: AtomicU64,
    wait_count: AtomicU64,
    wait_nanos: AtomicU64,
    max_wait_nanos: AtomicU64,
}
impl From<ApplyDurability> for WriteDurability {
    fn from(durability: ApplyDurability) -> Self {
        match durability {
            ApplyDurability::MemoryApplied => Self::MemoryApplied,
            ApplyDurability::StoreCommitted => Self::StoreCommitted,
            ApplyDurability::StableCommitted => Self::StableCommitted,
        }
    }
}
impl From<WriteDurability> for crate::replication::ReplicationAckDurability {
    fn from(durability: WriteDurability) -> Self {
        match durability {
            WriteDurability::MemoryApplied => Self::MemoryApplied,
            WriteDurability::StoreCommitted => Self::StoreCommitted,
            WriteDurability::StableCommitted => Self::StableCommitted,
            _ => Self::StableCommitted,
        }
    }
}
impl From<crate::replication::ReplicationAckDurability> for WriteDurability {
    fn from(durability: crate::replication::ReplicationAckDurability) -> Self {
        match durability {
            crate::replication::ReplicationAckDurability::MemoryApplied => Self::MemoryApplied,
            crate::replication::ReplicationAckDurability::StoreCommitted => Self::StoreCommitted,
            crate::replication::ReplicationAckDurability::StableCommitted => Self::StableCommitted,
        }
    }
}

impl WriteConcernTracker {
    pub(crate) fn new(
        receiver: String,
        placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
        capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            receiver,
            placement,
            capacity: Arc::new(Semaphore::new(capacity)),
            acknowledgement_cap: AtomicU32::new(0),
            pending: Mutex::new(HashMap::new()),
            regions: RwLock::new(HashMap::new()),
            diagnostics: WriteConcernCounters::default(),
        })
    }
    pub(crate) fn diagnostics(&self) -> WriteConcernDiagnostics {
        let d = &self.diagnostics;
        WriteConcernDiagnostics {
            accepted: d.accepted.load(Ordering::Relaxed),
            satisfied: d.satisfied.load(Ordering::Relaxed),
            timed_out: d.timed_out.load(Ordering::Relaxed),
            durability_failed: d.durability_failed.load(Ordering::Relaxed),
            operation_failed: d.operation_failed.load(Ordering::Relaxed),
            unknown_outcomes: d.unknown_outcomes.load(Ordering::Relaxed),
            rejected: d.rejected.load(Ordering::Relaxed),
            cancelled: d.cancelled.load(Ordering::Relaxed),
            active: d.active.load(Ordering::Relaxed),
            wait_count: d.wait_count.load(Ordering::Relaxed),
            wait_nanos: d.wait_nanos.load(Ordering::Relaxed),
            max_wait_nanos: d.max_wait_nanos.load(Ordering::Relaxed),
        }
    }
    /// Promote the actual outgoing batch fence while eligible tickets await this peer.
    pub(crate) fn required_durability(&self, peer: &str, fragment: FragmentId) -> WriteDurability {
        self.pending
            .lock()
            .unwrap()
            .values()
            .flatten()
            .filter(|ticket| ticket.fragment == fragment && ticket.plan.eligible.contains(peer))
            .map(|ticket| ticket.plan.options.minimum_durability())
            .max()
            .unwrap_or_default()
    }
    /// Bounded retries of the submitted floor survive ingress/history resets.
    /// The sender reserves its own byte budget before cloning each operation.
    pub(crate) fn retry_operations(
        &self,
        peer: &str,
        fragment: FragmentId,
        mut offer: impl FnMut(&ReplicatedOp),
    ) {
        for ticket in self.pending.lock().unwrap().values().flatten() {
            let progress = ticket.progress.lock().unwrap();
            if ticket.fragment == fragment
                && ticket.plan.eligible.contains(peer)
                && !progress.complete
                && !progress.nodes.contains(peer)
                && progress
                    .deadline
                    .is_some_and(|deadline| deadline > Instant::now())
            {
                offer(&ticket.op);
            }
        }
    }
    pub(crate) fn configure_regions(&self, regions: HashMap<String, String>) {
        *self.regions.write().unwrap() = regions;
    }
    pub(crate) fn set_acknowledgement_cap(&self, cap: u32) -> Result<(), WriteConcernError> {
        if cap == 0 {
            return Err(WriteConcernError::InvalidPolicy(
                "write concern acknowledgement cap must be positive".into(),
            ));
        }
        self.acknowledgement_cap.store(cap, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn clear_acknowledgement_cap(&self) {
        self.acknowledgement_cap.store(0, Ordering::Relaxed);
    }

    pub(crate) fn acknowledgement_cap(&self) -> Option<u32> {
        NonZeroU32::new(self.acknowledgement_cap.load(Ordering::Relaxed)).map(NonZeroU32::get)
    }

    pub(crate) fn plan(
        &self,
        fragment: FragmentId,
        options: &WriteConcernOptions,
    ) -> Result<WriteConcernPlan, WriteConcernError> {
        self.compute_plan(fragment, options).inspect_err(|_| {
            self.diagnostics.rejected.fetch_add(1, Ordering::Relaxed);
        })
    }
    fn compute_plan(
        &self,
        fragment: FragmentId,
        options: &WriteConcernOptions,
    ) -> Result<WriteConcernPlan, WriteConcernError> {
        options.validate()?;
        let placement = self.placement.read().unwrap();
        let replicas = placement.get(&fragment);
        let replica_count = replicas.map_or(0, Vec::len);
        if replica_count > MAX_WRITE_CONCERN_REPLICAS {
            return Err(WriteConcernError::InvalidPolicy(
                "replica set exceeds write concern limit".into(),
            ));
        }
        let requested = options.concern().required_acknowledgements(replica_count)?;
        // Freeze this operational override into the plan. Existing waiters keep
        // their captured count even when the node's cap changes or is cleared.
        let required = effective_acknowledgement_count(
            requested,
            NonZeroU32::new(self.acknowledgement_cap.load(Ordering::Relaxed)),
        );
        if options.strict_acknowledgements() && required < requested {
            return Err(WriteConcernError::InvalidPolicy(format!(
                "operational acknowledgement cap of {required} cannot satisfy strict requested count of {requested}"
            )));
        }
        let eligible: HashSet<_> = replicas
            .cloned()
            .unwrap_or_else(|| vec![self.receiver.clone()])
            .into_iter()
            .collect();
        if !eligible.contains(&self.receiver) {
            return Err(NodeError::Network(
                "receiving node no longer belongs to the replica set".into(),
            )
            .into());
        }
        let region = if options.require_other_region() {
            Some(
                RegionRequirement::resolve(
                    &self.receiver,
                    &eligible,
                    &self.regions.read().unwrap(),
                )
                .map_err(WriteConcernError::InvalidPolicy)?,
            )
        } else {
            None
        };
        Ok(WriteConcernPlan {
            receiver: self.receiver.clone(),
            replica_count: replica_count as u32,
            requested,
            required,
            eligible,
            options: options.clone(),
            region,
        })
    }
    pub(crate) fn prepare(
        self: &Arc<Self>,
        fragment: FragmentId,
        op: &ReplicatedOp,
        options: &WriteConcernOptions,
    ) -> Result<PendingWrite, WriteConcernError> {
        let plan = self.plan(fragment, options)?;
        let permit = if plan.requires_wait() {
            Some(self.capacity.clone().try_acquire_owned().map_err(|_| {
                self.diagnostics.rejected.fetch_add(1, Ordering::Relaxed);
                WriteConcernError::Backpressure
            })?)
        } else {
            None
        };
        let key = plan.requires_wait().then(|| fingerprint(fragment, op));
        let ticket = Arc::new(Ticket {
            plan,
            fragment,
            op: op.clone(),
            certificate: operation_certificate(fragment, op),
            created: Instant::now(),
            progress: Mutex::new(Progress::default()),
            changed: Notify::new(),
        });
        self.diagnostics.accepted.fetch_add(1, Ordering::Relaxed);
        self.diagnostics.active.fetch_add(1, Ordering::Relaxed);
        if let Some(key) = key {
            self.pending
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .push(ticket.clone());
        }
        Ok(PendingWrite {
            tracker: self.clone(),
            key,
            ticket,
            _permit: permit,
            terminal: false,
        })
    }
    /// Called only after session, batch identity, apply, and the sender's
    /// configured durability fence have all been validated.
    #[cfg(test)]
    pub(crate) fn acknowledge(&self, peer: &str, fragment: FragmentId, ops: &[ReplicatedOp]) {
        self.acknowledge_at(peer, fragment, ops, WriteDurability::MemoryApplied);
    }
    pub(crate) fn acknowledge_at(
        &self,
        peer: &str,
        fragment: FragmentId,
        ops: &[ReplicatedOp],
        durability: WriteDurability,
    ) {
        if peer == self.receiver || self.pending.lock().unwrap().is_empty() {
            return;
        }
        for op in ops {
            let pending = self.pending.lock().unwrap();
            for ticket in pending.values().flatten().filter(|ticket| {
                ticket.fragment == fragment
                    && ticket.op.key == op.key
                    && (op.value == ticket.op.value || op.value.wins_over(&ticket.op.value))
            }) {
                let mut progress = ticket.progress.lock().unwrap();
                if progress.complete
                    || durability < ticket.plan.options.minimum_durability()
                    || !ticket.plan.eligible.contains(peer)
                    || progress
                        .deadline
                        .is_some_and(|deadline| Instant::now() > deadline)
                {
                    continue;
                }
                let inserted = progress.nodes.insert(peer.to_string());
                if inserted
                    || progress
                        .durability
                        .get(peer)
                        .is_some_and(|previous| durability > *previous)
                {
                    progress.durability.insert(peer.to_string(), durability);
                    // A peer ACK cannot stand in for local application.
                    progress.complete =
                        progress.nodes.contains(&ticket.plan.receiver)
                            && progress.durability.get(&ticket.plan.receiver).is_some_and(
                                |level| *level >= ticket.plan.options.minimum_durability(),
                            )
                            && ticket.plan.complete(&progress.nodes);
                    ticket.changed.notify_waiters();
                }
            }
        }
    }
}

pub(crate) struct PendingWrite {
    tracker: Arc<WriteConcernTracker>,
    key: Option<[u8; 32]>,
    ticket: Arc<Ticket>,
    _permit: Option<OwnedSemaphorePermit>,
    terminal: bool,
}
impl PendingWrite {
    pub(crate) fn fail_with(mut self, error: WriteConcernError) -> WriteConcernError {
        self.terminal = true;
        if matches!(error, WriteConcernError::UnknownOutcome(_)) {
            self.tracker
                .diagnostics
                .unknown_outcomes
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.tracker
                .diagnostics
                .operation_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        error
    }
    pub(crate) fn operation_failed(mut self) {
        self.terminal = true;
        self.tracker
            .diagnostics
            .operation_failed
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn minimum_durability(&self) -> WriteDurability {
        self.ticket.plan.options.minimum_durability()
    }
    fn receipt(&self, progress: &Progress) -> WriteConcernReceipt {
        self.ticket
            .plan
            .receipt(&progress.nodes)
            .with_evidence(
                WriteConcernEvidence::new(
                    self.ticket.certificate.clone(),
                    self.ticket.plan.eligible.iter().cloned().collect(),
                    progress
                        .durability
                        .iter()
                        .map(|(node, level)| WriteAcknowledgement::new(node.clone(), *level))
                        .collect(),
                )
                .expect("validated evidence"),
            )
            .expect("evidence matches plan")
            .with_requested_durability(self.minimum_durability())
            .expect("captured policy has evidence")
    }
    pub(crate) fn requires_wait(&self) -> bool {
        self.ticket.plan.requires_wait()
    }
    pub(crate) fn applied_locally(&self) {
        let mut progress = self.ticket.progress.lock().unwrap();
        progress.nodes.insert(self.ticket.plan.receiver.clone());
        progress.durability.insert(
            self.ticket.plan.receiver.clone(),
            WriteDurability::MemoryApplied,
        );
        progress.deadline = Some(Instant::now() + self.ticket.plan.options.timeout());
        progress.complete = self.ticket.plan.options.minimum_durability()
            == WriteDurability::MemoryApplied
            && self.ticket.plan.complete(&progress.nodes);
        self.ticket.changed.notify_waiters();
    }
    pub(crate) fn reached_local_durability(&self, durability: WriteDurability) {
        let mut progress = self.ticket.progress.lock().unwrap();
        progress
            .durability
            .insert(self.ticket.plan.receiver.clone(), durability);
        progress.complete = durability >= self.minimum_durability()
            && self.ticket.plan.complete(&progress.nodes)
            && progress
                .deadline
                .is_some_and(|deadline| Instant::now() <= deadline);
        self.ticket.changed.notify_waiters();
    }
    pub(crate) fn fail_durability(mut self, error: String) -> WriteConcernError {
        let receipt = self.receipt(&self.ticket.progress.lock().unwrap());
        self.terminal = true;
        self.tracker
            .diagnostics
            .durability_failed
            .fetch_add(1, Ordering::Relaxed);
        WriteConcernError::DurabilityFailed {
            receipt: Box::new(receipt),
            error,
        }
    }
    pub(crate) async fn wait(mut self) -> Result<WriteConcernReceipt, WriteConcernError> {
        loop {
            let changed = self.ticket.changed.notified();
            let deadline = {
                let progress = self.ticket.progress.lock().unwrap();
                if progress.complete {
                    self.terminal = true;
                    self.tracker
                        .diagnostics
                        .satisfied
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(self.receipt(&progress));
                }
                progress
                    .deadline
                    .expect("local application precedes acknowledgement wait")
            };
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let progress = self.ticket.progress.lock().unwrap();
                let receipt = self.receipt(&progress);
                self.terminal = true;
                return if progress.complete {
                    self.tracker
                        .diagnostics
                        .satisfied
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(receipt)
                } else {
                    self.tracker
                        .diagnostics
                        .timed_out
                        .fetch_add(1, Ordering::Relaxed);
                    Err(WriteConcernError::TimedOut(receipt))
                };
            }
        }
    }
}
impl Drop for PendingWrite {
    fn drop(&mut self) {
        let stats = &self.tracker.diagnostics;
        stats.active.fetch_sub(1, Ordering::Relaxed);
        if !self.terminal {
            stats.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        let nanos = self
            .ticket
            .created
            .elapsed()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        stats.wait_count.fetch_add(1, Ordering::Relaxed);
        let _ = stats
            .wait_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sum| {
                Some(sum.saturating_add(nanos))
            });
        stats.max_wait_nanos.fetch_max(nanos, Ordering::Relaxed);
        if let Some(key) = self.key {
            let mut pending = self.tracker.pending.lock().unwrap();
            if let Some(tickets) = pending.get_mut(&key) {
                tickets.retain(|ticket| !Arc::ptr_eq(ticket, &self.ticket));
                if tickets.is_empty() {
                    pending.remove(&key);
                }
            }
        }
    }
}

pub(crate) fn operation_certificate(
    fragment: FragmentId,
    op: &ReplicatedOp,
) -> WriteOperationEvidence {
    WriteOperationEvidence::new(
        fragment.0,
        &op.key,
        &op.value.value,
        actordb_protocol::RecordVersion::new(op.value.timestamp_ms, op.value.logical_counter),
        op.value.node_id.clone(),
        op.value.is_tombstone,
        op.value.expires_at_ms,
    )
    .expect("validated operation")
}

fn fingerprint(fragment: FragmentId, op: &ReplicatedOp) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(fragment.0.to_be_bytes());
    digest.update((op.key.len() as u64).to_be_bytes());
    digest.update(op.key.as_bytes());
    digest.update(op.value.timestamp_ms.to_be_bytes());
    digest.update(op.value.logical_counter.to_be_bytes());
    digest.update((op.value.node_id.len() as u64).to_be_bytes());
    digest.update(op.value.node_id.as_bytes());
    digest.update([
        u8::from(op.value.is_tombstone),
        u8::from(op.value.expires_at_ms.is_some()),
    ]);
    digest.update(op.value.expires_at_ms.unwrap_or(0).to_be_bytes());
    digest.update(&op.value.value);
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::VersionedValue;
    #[tokio::test]
    async fn only_actual_eligible_fences_certify_frozen_operation_floor_and_set() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .minimum_durability(WriteDurability::StableCommitted)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();
        tracker.acknowledge_at(
            "b",
            FragmentId(0),
            std::slice::from_ref(&op),
            WriteDurability::StoreCommitted,
        );
        assert_eq!(pending.ticket.progress.lock().unwrap().nodes.len(), 1);
        pending.reached_local_durability(WriteDurability::StableCommitted);
        let captured = pending.ticket.plan.eligible.clone();
        tracker
            .placement
            .write()
            .unwrap()
            .insert(FragmentId(0), vec!["a".into(), "new".into()]);
        let mut offered = 0;
        tracker.retry_operations("b", FragmentId(0), |retry| {
            assert_eq!(retry.value, op.value);
            offered += 1;
        });
        assert_eq!(offered, 1);
        let mut newer = op.clone();
        newer.value.timestamp_ms += 1;
        tracker.acknowledge_at(
            "new",
            FragmentId(0),
            &[newer.clone()],
            WriteDurability::StableCommitted,
        );
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge_at(
            "b",
            FragmentId(0),
            &[newer],
            WriteDurability::StableCommitted,
        );
        let receipt = pending.wait().await.unwrap();
        let evidence = receipt.evidence().unwrap();
        assert_eq!(
            evidence.operation(),
            &operation_certificate(FragmentId(0), &op)
        );
        assert_eq!(
            evidence
                .eligible_nodes()
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
            captured
        );
        assert_eq!(
            evidence.durability_at("b"),
            Some(WriteDurability::StableCommitted)
        );
        assert_eq!(tracker.diagnostics().satisfied, 1);
        assert_eq!(tracker.diagnostics().active, 0);
    }
    fn op() -> ReplicatedOp {
        ReplicatedOp {
            key: "key".into(),
            value: VersionedValue::new(vec![1], 1, "a".into()),
            seq: 1,
            boot_id: 1,
        }
    }
    fn tracker() -> Arc<WriteConcernTracker> {
        WriteConcernTracker::new(
            "a".into(),
            Arc::new(RwLock::new(HashMap::from([(
                FragmentId(0),
                vec!["a".into(), "b".into(), "c".into()],
            )]))),
            1,
        )
    }
    fn region_tracker() -> Arc<WriteConcernTracker> {
        let tracker = tracker();
        tracker.configure_regions(HashMap::from([
            ("a".into(), "west".into()),
            ("b".into(), "west".into()),
            ("c".into(), "east".into()),
        ]));
        tracker
    }
    #[tokio::test]
    async fn acknowledgement_cap_does_not_remove_other_region_requirement() {
        let tracker = region_tracker();
        tracker.set_acknowledgement_cap(1).unwrap();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(2))
            .require_other_region(true)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        assert!(pending.key.is_some());
        assert_eq!(pending.ticket.plan.required, 1);
        assert!(matches!(
            tracker.prepare(FragmentId(0), &op, &options),
            Err(WriteConcernError::Backpressure)
        ));
        pending.applied_locally();
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge("b", FragmentId(0), std::slice::from_ref(&op));
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge("c", FragmentId(0), &[op]);
        let receipt = pending.wait().await.unwrap();
        assert_eq!(receipt.requested_acknowledgements(), 2);
        assert_eq!(receipt.required_acknowledgements(), 1);
        assert!(receipt.was_overridden());
        assert!(receipt.other_region_satisfied());
        assert_eq!(receipt.outside_region_acknowledged_nodes(), &["c"]);
    }

    #[tokio::test]
    async fn region_constraint_waits_after_numeric_threshold_and_ignores_unmatched_acks() {
        let tracker = region_tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(2))
            .require_other_region(true)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();
        tracker.acknowledge("b", FragmentId(0), std::slice::from_ref(&op));
        tracker.acknowledge("b", FragmentId(0), std::slice::from_ref(&op));
        tracker.acknowledge("ineligible", FragmentId(0), std::slice::from_ref(&op));
        let mut stale = op.clone();
        stale.value.timestamp_ms -= 1;
        tracker.acknowledge("c", FragmentId(0), &[stale]);
        tracker.acknowledge("c", FragmentId(1), std::slice::from_ref(&op));
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge("c", FragmentId(0), &[op]);
        let receipt = pending.wait().await.unwrap();
        assert_eq!(receipt.acknowledged_nodes(), &["a", "b", "c"]);
        assert_eq!(receipt.outside_region_acknowledged_nodes(), &["c"]);
        assert!(receipt.other_region_satisfied());
    }
    #[tokio::test]
    async fn local_region_constraint_registers_waiter_and_preserves_early_peer_ack() {
        let tracker = region_tracker();
        let op = op();
        let options = WriteConcernOptions::default().with_require_other_region(true);
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        assert!(pending.key.is_some());
        assert!(matches!(
            tracker.prepare(FragmentId(0), &op, &options),
            Err(WriteConcernError::Backpressure)
        ));
        tracker.acknowledge("c", FragmentId(0), &[op]);
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        pending.applied_locally();
        let receipt = pending.wait().await.unwrap();
        assert_eq!(receipt.required_acknowledgements(), 1);
        assert_eq!(receipt.acknowledged_nodes(), &["a", "c"]);
        assert_eq!(receipt.receiving_region(), Some("west"));
    }
    #[test]
    fn routed_decoder_rejects_receipt_that_omits_requested_region() {
        let options = WriteConcernOptions::default().with_require_other_region(true);
        let receipt = WriteConcernReceipt::new("a".into(), 3, 1, 1, vec!["a".into()]).unwrap();
        let bytes = WriteConcernOutcome::Satisfied(receipt).encode();
        assert!(matches!(
            decode_outcome(&bytes, &options),
            Err(WriteConcernError::UnknownOutcome(_))
        ));
    }
    #[test]
    fn maximum_legal_key_and_region_receipt_fit_mutation_reply_frame() {
        use crate::proto::{
            Envelope, EnvelopeMsg, MutationResponse, MutationResult, MutationStatus,
        };
        use crate::wire::{check_envelope_budget, check_record_budget, MAX_FRAME_BYTES};

        let mut low = 0;
        let mut high = MAX_FRAME_BYTES;
        while low < high {
            let midpoint = low + (high - low).div_ceil(2);
            if check_record_budget(midpoint, 0, 255).is_ok() {
                low = midpoint;
            } else {
                high = midpoint - 1;
            }
        }
        assert!(check_record_budget(low, 0, 255).is_ok());
        assert!(check_record_budget(low + 1, 0, 255).is_err());
        let ids: Vec<_> = (0..MAX_WRITE_CONCERN_REPLICAS)
            .map(|id| format!("{id:04}{}", "n".repeat(251)))
            .collect();
        let receipt = WriteConcernReceipt::new(
            ids[0].clone(),
            MAX_WRITE_CONCERN_REPLICAS as u32,
            1,
            1,
            ids.clone(),
        )
        .unwrap()
        .with_region_progress("r".repeat(255), ids[1..].to_vec())
        .unwrap()
        .with_evidence(
            WriteConcernEvidence::new(
                WriteOperationEvidence::new(
                    0,
                    &"k".repeat(low),
                    &[],
                    actordb_protocol::RecordVersion::new(1, 0),
                    ids[0].clone(),
                    false,
                    None,
                )
                .unwrap(),
                ids.clone(),
                ids.iter()
                    .map(|id| {
                        WriteAcknowledgement::new(id.clone(), WriteDurability::StableCommitted)
                    })
                    .collect(),
            )
            .unwrap(),
        )
        .unwrap();
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: u64::MAX,
                results: vec![MutationResult {
                    key: "k".repeat(low),
                    status: MutationStatus::Applied as i32,
                    write_concern: Some(WriteConcernOutcome::Satisfied(receipt).encode()),
                    ..Default::default()
                }],
            })),
        };
        check_envelope_budget(&envelope, MAX_FRAME_BYTES).unwrap();
    }
    #[test]
    fn acknowledgement_cap_never_raises_or_legalizes_requested_counts() {
        let tracker = tracker();
        tracker.set_acknowledgement_cap(3).unwrap();
        let count_one = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(1))
            .build();
        let plan = tracker.plan(FragmentId(0), &count_one).unwrap();
        assert_eq!((plan.requested, plan.required), (1, 1));
        assert!(!plan.requires_wait());

        tracker.set_acknowledgement_cap(1).unwrap();
        let invalid = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(3))
            .build();
        assert!(matches!(
            tracker.plan(FragmentId(0), &invalid),
            Err(WriteConcernError::InvalidPolicy(_))
        ));
        assert!(matches!(
            tracker.set_acknowledgement_cap(0),
            Err(WriteConcernError::InvalidPolicy(_))
        ));
        assert_eq!(tracker.acknowledgement_cap(), Some(1));
    }

    #[tokio::test]
    async fn strict_rejection_does_not_consume_waiter_capacity_or_change_captured_policy() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .strict_acknowledgements(true)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        tracker.set_acknowledgement_cap(1).unwrap();
        assert!(matches!(
            tracker.prepare(FragmentId(0), &op, &options),
            Err(WriteConcernError::InvalidPolicy(_))
        ));
        pending.applied_locally();
        assert_eq!(pending.ticket.plan.required, 2);
        assert_eq!(tracker.diagnostics().rejected, 1);
        tracker.acknowledge("b", FragmentId(0), std::slice::from_ref(&op));
        let receipt = pending.wait().await.unwrap();
        assert!(!receipt.was_overridden());

        tracker.clear_acknowledgement_cap();
        assert!(tracker.prepare(FragmentId(0), &op, &options).is_ok());
        tracker.set_acknowledgement_cap(2).unwrap();
        assert!(tracker.plan(FragmentId(0), &options).is_ok());
    }

    #[tokio::test]
    async fn eligibility_remains_frozen_after_placement_changes() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .strict_acknowledgements(true)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        tracker.placement.write().unwrap().insert(
            FragmentId(0),
            vec!["a".into(), "new-b".into(), "new-c".into()],
        );
        pending.applied_locally();
        tracker.acknowledge("new-b", FragmentId(0), std::slice::from_ref(&op));
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge("b", FragmentId(0), &[op]);
        assert_eq!(
            pending.wait().await.unwrap().acknowledged_nodes(),
            &["a", "b"]
        );
    }

    #[tokio::test]
    async fn acknowledgement_cap_changes_only_new_plans_and_reports_effective_threshold() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();

        tracker.set_acknowledgement_cap(1).unwrap();
        let newer = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        newer.applied_locally();
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        assert_eq!(pending.ticket.plan.required, 2);
        let receipt = newer.wait().await.unwrap();
        assert_eq!(receipt.requested_acknowledgements(), 2);
        assert_eq!(receipt.required_acknowledgements(), 1);
        assert!(receipt.was_overridden());
        assert_eq!(receipt.acknowledged_nodes(), &["a"]);

        tracker.clear_acknowledgement_cap();
        assert_eq!(tracker.acknowledgement_cap(), None);
        assert_eq!(tracker.plan(FragmentId(0), &options).unwrap().required, 2);
        tracker.acknowledge("b", FragmentId(0), &[op]);
        let receipt = pending.wait().await.unwrap();
        assert_eq!(receipt.required_acknowledgements(), 2);
        assert!(!receipt.was_overridden());
    }

    #[tokio::test]
    async fn distinct_matching_processing_acks_satisfy_count() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(2))
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();
        tracker.acknowledge("outside", FragmentId(0), std::slice::from_ref(&op));
        let mut other = op.clone();
        other.value.value = vec![2];
        tracker.acknowledge("b", FragmentId(0), &[other]);
        assert!(!pending.ticket.progress.lock().unwrap().complete);
        tracker.acknowledge("b", FragmentId(0), std::slice::from_ref(&op));
        tracker.acknowledge("b", FragmentId(0), &[op]);
        assert_eq!(
            pending.wait().await.unwrap().acknowledged_nodes(),
            &["a", "b"]
        );
    }
    #[tokio::test]
    async fn timeout_keeps_local_progress_and_releases_capacity() {
        let tracker = tracker();
        let op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .timeout(std::time::Duration::from_millis(5))
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();
        assert!(matches!(
            tracker.prepare(FragmentId(0), &op, &options),
            Err(WriteConcernError::Backpressure)
        ));
        let Err(WriteConcernError::TimedOut(receipt)) = pending.wait().await else {
            panic!("expected timeout")
        };
        assert_eq!(receipt.acknowledged_nodes(), &["a"]);
        assert!(tracker.prepare(FragmentId(0), &op, &options).is_ok());
    }
    #[test]
    fn cancelled_waiter_is_removed_and_snapshot_replay_matches_exact_value() {
        let tracker = tracker();
        let mut op = op();
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .build();
        let pending = tracker.prepare(FragmentId(0), &op, &options).unwrap();
        pending.applied_locally();
        op.seq = 0;
        op.boot_id = 0;
        tracker.acknowledge("b", FragmentId(0), &[op]);
        assert!(pending.ticket.progress.lock().unwrap().complete);
        drop(pending);
        assert!(tracker.pending.lock().unwrap().is_empty());
    }
}
