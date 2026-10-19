//! Per-write processing acknowledgements on the normal asynchronous sender path.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use sha2::{Digest, Sha256};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::actor::ReplicatedOp;
use crate::node::NodeError;
use crate::shard::FragmentId;
pub use actordb_protocol::write_concern::{
    WriteConcern, WriteConcernOptions, WriteConcernOutcome, WriteConcernPolicyError,
    WriteConcernReceipt, MAX_WRITE_CONCERN_REPLICAS,
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
        }
    }
}
impl std::error::Error for WriteConcernError {}
impl From<NodeError> for WriteConcernError {
    fn from(e: NodeError) -> Self {
        Self::Operation(e)
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
            error => NodeError::Network(error.to_string()),
        }
    }
    pub(crate) fn outcome(&self) -> Option<WriteConcernOutcome> {
        match self {
            Self::InvalidPolicy(e) => Some(WriteConcernOutcome::InvalidPolicy(e.clone())),
            Self::Unsupported(e) => Some(WriteConcernOutcome::Unsupported(e.clone())),
            Self::TimedOut(receipt) => Some(WriteConcernOutcome::TimedOut(receipt.clone())),
            Self::Backpressure => Some(WriteConcernOutcome::Backpressure),
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
    deadline: Option<Instant>,
    complete: bool,
}
#[derive(Debug)]
struct Ticket {
    plan: WriteConcernPlan,
    progress: Mutex<Progress>,
    changed: Notify,
}

#[derive(Debug)]
pub(crate) struct WriteConcernTracker {
    pub(crate) receiver: String,
    placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    capacity: Arc<Semaphore>,
    pending: Mutex<HashMap<[u8; 32], Vec<Arc<Ticket>>>>,
    regions: RwLock<HashMap<String, String>>,
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
            pending: Mutex::new(HashMap::new()),
            regions: RwLock::new(HashMap::new()),
        })
    }
    pub(crate) fn configure_regions(&self, regions: HashMap<String, String>) {
        *self.regions.write().unwrap() = regions;
    }
    pub(crate) fn plan(
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
            required: requested,
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
            Some(
                self.capacity
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| WriteConcernError::Backpressure)?,
            )
        } else {
            None
        };
        let key = plan.requires_wait().then(|| fingerprint(fragment, op));
        let ticket = Arc::new(Ticket {
            plan,
            progress: Mutex::new(Progress::default()),
            changed: Notify::new(),
        });
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
        })
    }
    /// Called only after session, batch identity, apply, and the sender's
    /// configured durability fence have all been validated.
    pub(crate) fn acknowledge(&self, peer: &str, fragment: FragmentId, ops: &[ReplicatedOp]) {
        if peer == self.receiver || self.pending.lock().unwrap().is_empty() {
            return;
        }
        for op in ops {
            let key = fingerprint(fragment, op);
            let pending = self.pending.lock().unwrap();
            if let Some(tickets) = pending.get(&key) {
                for ticket in tickets {
                    let mut progress = ticket.progress.lock().unwrap();
                    if progress.complete
                        || !ticket.plan.eligible.contains(peer)
                        || progress
                            .deadline
                            .is_some_and(|deadline| Instant::now() > deadline)
                    {
                        continue;
                    }
                    if progress.nodes.insert(peer.to_string()) {
                        // A peer ACK cannot stand in for local application.
                        progress.complete = progress.nodes.contains(&ticket.plan.receiver)
                            && ticket.plan.complete(&progress.nodes);
                        ticket.changed.notify_waiters();
                    }
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
}
impl PendingWrite {
    pub(crate) fn requires_wait(&self) -> bool {
        self.ticket.plan.requires_wait()
    }
    pub(crate) fn applied_locally(&self) {
        let mut progress = self.ticket.progress.lock().unwrap();
        progress.nodes.insert(self.ticket.plan.receiver.clone());
        progress.deadline = Some(Instant::now() + self.ticket.plan.options.timeout());
        progress.complete = self.ticket.plan.complete(&progress.nodes);
        self.ticket.changed.notify_waiters();
    }
    pub(crate) async fn wait(self) -> Result<WriteConcernReceipt, WriteConcernError> {
        loop {
            let changed = self.ticket.changed.notified();
            let deadline = {
                let progress = self.ticket.progress.lock().unwrap();
                if progress.complete {
                    return Ok(self.ticket.plan.receipt(&progress.nodes));
                }
                progress
                    .deadline
                    .expect("local application precedes acknowledgement wait")
            };
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let progress = self.ticket.progress.lock().unwrap();
                let receipt = self.ticket.plan.receipt(&progress.nodes);
                return if progress.complete {
                    Ok(receipt)
                } else {
                    Err(WriteConcernError::TimedOut(receipt))
                };
            }
        }
    }
}
impl Drop for PendingWrite {
    fn drop(&mut self) {
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

fn fingerprint(fragment: FragmentId, op: &ReplicatedOp) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(fragment.0.to_be_bytes());
    digest.update((op.key.len() as u64).to_be_bytes());
    digest.update(op.key.as_bytes());
    digest.update(op.value.timestamp_ms.to_be_bytes());
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
        stale.value.timestamp_ms += 1;
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
