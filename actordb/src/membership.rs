//! Resumable staged membership changes driven by an application controller.
//!
//! A transition preserves every old holder in its joint map, requires each
//! old local floor and stable import evidence at every final target, and waits
//! for actual serving observations before retirement. Permanent source loss
//! and process-authority fencing are separate explicit application decisions.
//! This protocol supplies validation and durable progress, not consensus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::checkpoint::format::{Decoder, Encoder};
use crate::checkpoint::{CheckpointError, CheckpointManifest, ImportReceipt};
use crate::placement::{validate_placement, ManualPlacement, PlacementController};
use crate::shard::FragmentId;

const MAX_STATE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) mod native;
pub use native::{MembershipFenceObservation, MembershipShutdownObservation};
pub type MembershipMap = HashMap<FragmentId, Vec<String>>;

#[derive(Debug)]
#[non_exhaustive]
pub enum MembershipError {
    Invalid(String),
    Checkpoint(CheckpointError),
    InsufficientEvidence(String),
}
impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(error) => write!(f, "invalid membership transition: {error}"),
            Self::Checkpoint(error) => error.fmt(f),
            Self::InsufficientEvidence(error) => {
                write!(f, "membership evidence incomplete: {error}")
            }
        }
    }
}
impl std::error::Error for MembershipError {}
impl From<CheckpointError> for MembershipError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MembershipStage {
    Prepared,
    Joint,
    ReadyToRetire,
    Complete,
}

/// A fresh live-node observation produced by Node, independent of imported
/// stable floors. Boot and placement generation are retained for operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipReady {
    pub(crate) node_id: String,
    pub(crate) boot_id: u64,
    pub(crate) placement_generation: u64,
    pub(crate) joint_digest: [u8; 32],
    pub(crate) epoch: [u8; 32],
    pub(crate) authority_digest: [u8; 32],
    pub(crate) native_revision: Option<u64>,
}
impl MembershipReady {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub fn boot_id(&self) -> u64 {
        self.boot_id
    }
    pub fn placement_generation(&self) -> u64 {
        self.placement_generation
    }
    pub fn epoch(&self) -> &[u8; 32] {
        &self.epoch
    }
    pub fn authority_digest(&self) -> &[u8; 32] {
        &self.authority_digest
    }
    pub fn native_authority_revision(&self) -> Option<u64> {
        self.native_revision
    }
}

/// Exact, separately authorized missing-source disposition. Constructing this
/// means the application accepts that an unavailable floor cannot be proved;
/// it is not checkpoint evidence or a fabricated handoff receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermanentSourceLoss {
    epoch: [u8; 32],
    missing: BTreeSet<String>,
    decision_id: [u8; 32],
}
impl PermanentSourceLoss {
    pub fn authorize(
        epoch: [u8; 32],
        missing: impl IntoIterator<Item = String>,
        decision_id: [u8; 32],
    ) -> Result<Self, MembershipError> {
        let missing = missing.into_iter().collect::<BTreeSet<_>>();
        if epoch == [0; 32]
            || decision_id == [0; 32]
            || missing.is_empty()
            || missing.len() > 256
            || missing.iter().any(|id| id.is_empty() || id.len() > 255)
        {
            return Err(MembershipError::Invalid("loss authorization needs a fresh epoch, explicit members and nonzero external decision ID".into()));
        }
        Ok(Self {
            epoch,
            missing,
            decision_id,
        })
    }
    pub fn missing_sources(&self) -> &BTreeSet<String> {
        &self.missing
    }
    pub fn decision_id(&self) -> &[u8; 32] {
        &self.decision_id
    }
}

/// Application-certified completed fence and its exact expected process
/// generations. `decision_id` refers to external durable authorization/evidence,
/// not a process-local revision or a checkpoint hash. Native peer fences may
/// supply this evidence once the application has fenced every affected node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipAuthority {
    epoch: [u8; 32],
    generations: BTreeMap<String, u64>,
    decision_id: [u8; 32],
    pub(crate) native_receivers: BTreeMap<String, NativeMembershipReceiver>,
    pub(crate) native_retired: BTreeMap<String, NativeRetiredReceiver>,
}
/// Exact local SDK receiver fence evidence. Revisions are process-local.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMembershipReceiver {
    pub node_boot_id: u64,
    pub authority_revision: u64,
}
/// Full local SDK shutdown, distinct from remote receiver authority revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeRetiredReceiver {
    pub node_boot_id: u64,
    pub local_fence_count: u32,
}
impl MembershipAuthority {
    pub fn digest(&self) -> [u8; 32] {
        let mut out = Encoder::default();
        out.digest(&self.epoch);
        encode_authority(&mut out, Some(self));
        Sha256::digest(&out.0).into()
    }
    pub fn certify_external(
        epoch: [u8; 32],
        generations: BTreeMap<String, u64>,
        decision_id: [u8; 32],
    ) -> Result<Self, MembershipError> {
        if epoch == [0; 32]
            || decision_id == [0; 32]
            || generations.is_empty()
            || generations.len() > 256
            || generations.keys().any(|id| id.is_empty() || id.len() > 255)
            || generations.values().any(|generation| *generation == 0)
        {
            return Err(MembershipError::Invalid("authority needs a fresh epoch, bounded positive process generations, and explicit external decision ID".into()));
        }
        Ok(Self {
            epoch,
            generations,
            decision_id,
            native_receivers: BTreeMap::new(),
            native_retired: BTreeMap::new(),
        })
    }
    pub fn generations(&self) -> &BTreeMap<String, u64> {
        &self.generations
    }
    pub fn decision_id(&self) -> &[u8; 32] {
        &self.decision_id
    }
    pub fn native_receivers(&self) -> &BTreeMap<String, NativeMembershipReceiver> {
        &self.native_receivers
    }
    pub fn retired_sdk_shutdowns(&self) -> &BTreeMap<String, NativeRetiredReceiver> {
        &self.native_retired
    }
    pub fn is_native(&self) -> bool {
        !self.native_receivers.is_empty()
    }
    fn matches(&self, epoch: &[u8; 32], map: &MembershipMap) -> bool {
        &self.epoch == epoch
            && self.generations.keys().cloned().collect::<BTreeSet<_>>()
                == map.values().flatten().cloned().collect()
    }
}

/// Durable transition coordinator. Its file stores proposal, source manifest
/// generations, target import evidence and explicit authorizations. Live
/// readiness is deliberately re-observed after every coordinator restart.
pub struct MembershipTransition {
    path: PathBuf,
    epoch: [u8; 32],
    current: MembershipMap,
    desired: MembershipMap,
    minimum_replicas: usize,
    stage: MembershipStage,
    sources: BTreeMap<String, CheckpointManifest>,
    imports: BTreeMap<(String, String), ImportReceipt>,
    ready: BTreeMap<String, MembershipReady>,
    loss: Option<PermanentSourceLoss>,
    joint_authority: Option<MembershipAuthority>,
    final_authority: Option<MembershipAuthority>,
}
impl MembershipTransition {
    pub fn create(
        path: impl AsRef<Path>,
        epoch: [u8; 32],
        current: MembershipMap,
        desired: MembershipMap,
        minimum_replicas: usize,
    ) -> Result<Self, MembershipError> {
        validate_maps(epoch, &current, &desired, minimum_replicas)?;
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            return Err(MembershipError::Invalid(
                "transition path already exists; reopen its exact durable proposal".into(),
            ));
        }
        let result = Self {
            path,
            epoch,
            current,
            desired,
            minimum_replicas,
            stage: MembershipStage::Prepared,
            sources: BTreeMap::new(),
            imports: BTreeMap::new(),
            ready: BTreeMap::new(),
            loss: None,
            joint_authority: None,
            final_authority: None,
        };
        result.persist()?;
        Ok(result)
    }
    pub fn reopen(path: impl AsRef<Path>) -> Result<Self, MembershipError> {
        let path = path.as_ref().to_path_buf();
        let bytes = crate::checkpoint::read_bounded(&path, MAX_STATE_BYTES)?;
        if bytes.len() < 32 {
            return Err(MembershipError::Invalid(
                "truncated transition state".into(),
            ));
        }
        let (body, digest) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != digest {
            return Err(MembershipError::Invalid(
                "transition state checksum mismatch".into(),
            ));
        }
        let mut input = Decoder::new(body);
        let version = input.take(8)?;
        if version != b"ACTMEM04" {
            return Err(MembershipError::Invalid(
                "unsupported transition state".into(),
            ));
        }
        let epoch = input.digest()?;
        let minimum_replicas = input.u32()? as usize;
        let stage = match input.u8()? {
            0 => MembershipStage::Prepared,
            1 => MembershipStage::Joint,
            2 => MembershipStage::ReadyToRetire,
            3 => MembershipStage::Complete,
            _ => return Err(MembershipError::Invalid("unknown transition stage".into())),
        };
        let current = decode_map(&mut input)?;
        let desired = decode_map(&mut input)?;
        validate_maps(epoch, &current, &desired, minimum_replicas)?;
        let count = input.u32()? as usize;
        if count > 256 {
            return Err(MembershipError::Invalid(
                "too many transition sources".into(),
            ));
        }
        let mut sources = BTreeMap::new();
        for _ in 0..count {
            let manifest = CheckpointManifest::decode(&input.bytes(2 * 1024 * 1024)?)?;
            if sources
                .insert(manifest.source().node_id().to_owned(), manifest)
                .is_some()
            {
                return Err(MembershipError::Invalid(
                    "duplicate transition source".into(),
                ));
            }
        }
        let count = input.u32()? as usize;
        if count > 65_536 {
            return Err(MembershipError::Invalid("too many import receipts".into()));
        }
        let mut imports = BTreeMap::new();
        for _ in 0..count {
            let receipt = ImportReceipt::decode(&input.bytes(2048)?)?;
            let key = (
                receipt.target().node_id().to_owned(),
                receipt.source().node_id().to_owned(),
            );
            if imports.insert(key, receipt).is_some() {
                return Err(MembershipError::Invalid(
                    "duplicate transition import".into(),
                ));
            }
        }
        let loss = match input.u8()? {
            0 => None,
            1 => {
                let decision_id = input.digest()?;
                let count = input.u32()? as usize;
                if count == 0 || count > 256 {
                    return Err(MembershipError::Invalid(
                        "invalid missing-source count".into(),
                    ));
                }
                let mut missing = BTreeSet::new();
                for _ in 0..count {
                    missing.insert(input.string(255)?);
                }
                Some(PermanentSourceLoss::authorize(epoch, missing, decision_id)?)
            }
            _ => return Err(MembershipError::Invalid("invalid source-loss flag".into())),
        };
        let joint_authority = decode_authority(&mut input, epoch)?;
        let final_authority = decode_authority(&mut input, epoch)?;
        input.finish()?;
        let mut result = Self {
            path,
            epoch,
            current,
            desired,
            minimum_replicas,
            stage,
            sources,
            imports,
            ready: BTreeMap::new(),
            loss,
            joint_authority,
            final_authority,
        };
        result.validate_evidence()?;
        // Process/revision readiness must be reacquired after restart. A
        // committed final intent stays idempotently publishable.
        if result.stage == MembershipStage::ReadyToRetire {
            result.stage = MembershipStage::Joint;
            result.persist()?;
        }
        Ok(result)
    }
    pub fn epoch(&self) -> &[u8; 32] {
        &self.epoch
    }
    pub fn stage(&self) -> MembershipStage {
        self.stage
    }
    pub fn current(&self) -> &MembershipMap {
        &self.current
    }
    pub fn desired(&self) -> &MembershipMap {
        &self.desired
    }
    pub fn minimum_replicas(&self) -> usize {
        self.minimum_replicas
    }
    pub fn joint(&self) -> MembershipMap {
        self.current
            .iter()
            .map(|(fragment, old)| {
                let mut holders = old
                    .iter()
                    .filter(|id| {
                        self.loss
                            .as_ref()
                            .is_none_or(|loss| !loss.missing.contains(*id))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for id in &self.desired[fragment] {
                    if !holders.contains(id) {
                        holders.push(id.clone());
                    }
                }
                (*fragment, holders)
            })
            .collect()
    }
    pub fn joint_digest(&self) -> [u8; 32] {
        map_digest(&self.joint())
    }
    pub fn source_manifests(&self) -> impl Iterator<Item = &CheckpointManifest> {
        self.sources.values()
    }
    pub fn import_receipts(&self) -> impl Iterator<Item = &ImportReceipt> {
        self.imports.values()
    }
    pub fn missing_source_authorization(&self) -> Option<&PermanentSourceLoss> {
        self.loss.as_ref()
    }
    pub fn joint_authority(&self) -> Option<&MembershipAuthority> {
        self.joint_authority.as_ref()
    }
    pub fn final_authority(&self) -> Option<&MembershipAuthority> {
        self.final_authority.as_ref()
    }
    pub fn readiness_authority_digest(&self) -> Option<[u8; 32]> {
        self.final_authority
            .as_ref()
            .or(self.joint_authority.as_ref())
            .map(MembershipAuthority::digest)
    }
    /// Sources required for every changed fragment, including retained holders.
    pub fn required_sources(&self) -> BTreeSet<String> {
        self.current
            .iter()
            .filter(|(fragment, replicas)| *replicas != &self.desired[fragment])
            .flat_map(|(_, replicas)| replicas.iter().cloned())
            .collect()
    }
    pub fn target_nodes(&self) -> BTreeSet<String> {
        self.current
            .iter()
            .filter(|(fragment, replicas)| *replicas != &self.desired[fragment])
            .flat_map(|(fragment, _)| self.desired[fragment].iter().cloned())
            .collect()
    }
    pub fn record_source(&mut self, manifest: CheckpointManifest) -> Result<(), MembershipError> {
        if self.stage == MembershipStage::Complete {
            return Err(MembershipError::Invalid(
                "completed transition evidence is immutable".into(),
            ));
        }
        let id = manifest.source().node_id().to_owned();
        if !self.required_sources().contains(&id)
            || manifest.source().fragment_count() as usize != self.current.len()
            || self
                .loss
                .as_ref()
                .is_some_and(|loss| loss.missing.contains(&id))
        {
            return Err(MembershipError::Invalid(
                "checkpoint source is outside the captured old holder set/layout".into(),
            ));
        }
        if self
            .sources
            .get(&id)
            .is_some_and(|prior| prior != &manifest)
        {
            return Err(MembershipError::Invalid(
                "source changed checkpoint floor within one transition".into(),
            ));
        }
        if self.sources.get(&id) == Some(&manifest) {
            return Ok(());
        }
        if self.encoded_size_estimate() + manifest.encode().len() + 4 > MAX_STATE_BYTES {
            return Err(MembershipError::Invalid(
                "source evidence exceeds bounded transition metadata".into(),
            ));
        }
        self.sources.insert(id, manifest);
        self.ready.clear();
        if self.stage == MembershipStage::ReadyToRetire {
            self.stage = MembershipStage::Joint;
        }
        self.persist()
    }
    pub fn record_import(&mut self, receipt: ImportReceipt) -> Result<(), MembershipError> {
        if self.stage == MembershipStage::Complete {
            return Err(MembershipError::Invalid(
                "completed transition evidence is immutable".into(),
            ));
        }
        let source = self
            .sources
            .get(receipt.source().node_id())
            .ok_or_else(|| {
                MembershipError::InsufficientEvidence(
                    "source checkpoint has not been qualified".into(),
                )
            })?;
        if source.source() != receipt.source()
            || source.digest() != *receipt.manifest_digest()
            || source.root_hash() != receipt.root_hash()
            || source.records() != receipt.records()
            || receipt.target().fragment_count() as usize != self.current.len()
            || !self.target_nodes().contains(receipt.target().node_id())
        {
            return Err(MembershipError::Invalid(
                "target import is not the qualified source floor".into(),
            ));
        }
        let key = (
            receipt.target().node_id().to_owned(),
            receipt.source().node_id().to_owned(),
        );
        if self.imports.get(&key) == Some(&receipt) {
            return Ok(());
        }
        if self.encoded_size_estimate() + receipt.encode().len() + 4 > MAX_STATE_BYTES {
            return Err(MembershipError::Invalid(
                "import evidence exceeds bounded transition metadata".into(),
            ));
        }
        self.imports.insert(
            (
                receipt.target().node_id().to_owned(),
                receipt.source().node_id().to_owned(),
            ),
            receipt,
        );
        self.ready.clear();
        if self.stage == MembershipStage::ReadyToRetire {
            self.stage = MembershipStage::Joint;
        }
        self.persist()
    }
    /// Explicit application acceptance of permanent data-source loss. This
    /// alone does not elect a writer: a separately fenced joint authority and
    /// stable target copies are still mandatory before final publication.
    pub fn authorize_permanent_loss(
        &mut self,
        authorization: PermanentSourceLoss,
    ) -> Result<(), MembershipError> {
        if self.stage != MembershipStage::Prepared
            || authorization.epoch != self.epoch
            || !authorization.missing.is_subset(&self.required_sources())
            || authorization
                .missing
                .iter()
                .any(|id| self.sources.contains_key(id) || self.target_nodes().contains(id))
        {
            return Err(MembershipError::Invalid(
                "loss decision does not match missing retiring holders in prepared proposal".into(),
            ));
        }
        self.loss = Some(authorization);
        self.joint_authority = None;
        self.final_authority = None;
        self.ready.clear();
        self.persist()
    }
    /// Record the application's completed authority fence. The nonzero
    /// decision ID identifies externally persisted fencing evidence; it is
    /// deliberately separate from snapshots and source qualification. Native
    /// hot peer-policy fencing can be used by the application before this call.
    pub fn record_authority_fence(
        &mut self,
        authority: MembershipAuthority,
    ) -> Result<(), MembershipError> {
        if !authority.matches(&self.epoch, &self.joint()) || self.stage == MembershipStage::Complete
        {
            return Err(MembershipError::Invalid(
                "authority fence does not match this epoch and exact joint membership".into(),
            ));
        }
        if self.joint_authority.as_ref() == Some(&authority) {
            return Ok(());
        }
        if self.joint_authority.is_some() {
            return Err(MembershipError::Invalid("joint authority decision is immutable; use a fresh transition epoch for changed process authority".into()));
        }
        self.joint_authority = Some(authority);
        self.final_authority = None;
        self.ready.clear();
        if self.stage == MembershipStage::ReadyToRetire {
            self.stage = MembershipStage::Joint;
        }
        self.persist()
    }
    /// Record completed final process fencing separately from joint overlap.
    /// Retired processes must be absent; retained generations may keep their
    /// generations or advance them, never roll back. This is an operational
    /// policy gate; authenticating and revoking credentials belongs to the app.
    pub fn record_final_authority_fence(
        &mut self,
        authority: MembershipAuthority,
    ) -> Result<(), MembershipError> {
        if self.stage != MembershipStage::Joint && self.stage != MembershipStage::ReadyToRetire {
            return Err(MembershipError::Invalid(
                "final authority requires a published joint stage".into(),
            ));
        }
        let joint = self.joint_authority.as_ref().ok_or_else(|| {
            MembershipError::InsufficientEvidence("joint authority has not been fenced".into())
        })?;
        if !authority.matches(&self.epoch, &self.desired) || self.stage == MembershipStage::Complete
        {
            return Err(MembershipError::Invalid(
                "final authority differs from exact desired holder set/epoch".into(),
            ));
        }
        if self.final_authority.as_ref() == Some(&authority) {
            return Ok(());
        }
        if self.final_authority.is_some() {
            return Err(MembershipError::Invalid("final authority decision is immutable; use a fresh transition epoch for changed process authority".into()));
        }
        let previous_authority = joint;
        if joint.is_native() && authority.is_native() {
            let retired = joint
                .generations
                .keys()
                .filter(|id| !authority.generations.contains_key(*id))
                .cloned()
                .collect::<BTreeSet<_>>();
            if authority
                .native_retired
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != retired
                || authority.native_retired.iter().any(|(id, receiver)| {
                    joint
                        .native_receivers
                        .get(id)
                        .is_none_or(|previous| previous.node_boot_id != receiver.node_boot_id)
                })
            {
                return Err(MembershipError::InsufficientEvidence(
                    "retired shutdowns do not match the SDK processes certified in joint authority"
                        .into(),
                ));
            }
        }
        for (id, generation) in &authority.generations {
            let previous = previous_authority.generations.get(id).ok_or_else(|| {
                MembershipError::Invalid(
                    "final policy contains a process absent from the joint stage".into(),
                )
            })?;
            if generation < previous {
                return Err(MembershipError::Invalid(
                    "retained process generation rolled back".into(),
                ));
            }
        }
        self.final_authority = Some(authority);
        self.ready.clear();
        self.stage = MembershipStage::Joint;
        self.persist()
    }
    /// Idempotently publish the overlap map to the supplied supported manual
    /// controller. A stale controller/proposal is rejected before mutation.
    pub fn publish_joint(&mut self, controller: &ManualPlacement) -> Result<(), MembershipError> {
        let observed = controller.desired_placement();
        let joint = self.joint();
        if self.stage == MembershipStage::Complete
            || (map_digest(&observed) != map_digest(&self.current)
                && map_digest(&observed) != map_digest(&joint))
        {
            return Err(MembershipError::Invalid(
                "controller changed outside this transition".into(),
            ));
        }
        if self.loss.is_some() && self.joint_authority.is_none() {
            return Err(MembershipError::InsufficientEvidence(
                "permanent loss requires explicit authority fencing before changing the holder set"
                    .into(),
            ));
        }
        self.stage = MembershipStage::Joint;
        self.persist()?;
        controller.set_placement(joint);
        Ok(())
    }
    pub fn record_ready(&mut self, evidence: MembershipReady) -> Result<(), MembershipError> {
        if self.stage != MembershipStage::Joint
            || !self.target_nodes().contains(&evidence.node_id)
            || evidence.joint_digest != self.joint_digest()
            || evidence.epoch != self.epoch
            || Some(evidence.authority_digest) != self.readiness_authority_digest()
            || self
                .final_authority
                .as_ref()
                .or(self.joint_authority.as_ref())
                .is_some_and(|authority| {
                    authority.is_native()
                        && authority
                            .native_receivers
                            .get(&evidence.node_id)
                            .is_none_or(|receiver| {
                                receiver.node_boot_id != evidence.boot_id
                                    || Some(receiver.authority_revision) != evidence.native_revision
                            })
                })
        {
            return Err(MembershipError::Invalid(
                "live readiness belongs to another joint proposal or target".into(),
            ));
        }
        self.ready.insert(evidence.node_id.clone(), evidence);
        if self.check_retirement().is_ok() {
            self.stage = MembershipStage::ReadyToRetire;
        }
        self.persist()
    }
    pub fn check_retirement(&self) -> Result<(), MembershipError> {
        if self.joint_authority.is_none() || self.final_authority.is_none() {
            return Err(MembershipError::InsufficientEvidence(
                "joint and final authority fences must both be recorded".into(),
            ));
        }
        for source in self.required_sources() {
            if self
                .loss
                .as_ref()
                .is_some_and(|loss| loss.missing.contains(&source))
            {
                continue;
            }
            let manifest = self.sources.get(&source).ok_or_else(|| {
                MembershipError::InsufficientEvidence(format!(
                    "old holder {source} has no stable checkpoint"
                ))
            })?;
            for target in self.target_nodes() {
                if !self.imports.contains_key(&(target.clone(), source.clone())) {
                    return Err(MembershipError::InsufficientEvidence(format!(
                        "target {target} has not imported source {source}"
                    )));
                }
                if manifest.source().fragment_count() as usize != self.current.len() {
                    return Err(MembershipError::Invalid("source layout changed".into()));
                }
            }
        }
        if !self
            .target_nodes()
            .iter()
            .all(|target| self.ready.contains_key(target))
        {
            return Err(MembershipError::InsufficientEvidence(
                "final targets have not all reported live joint serving readiness".into(),
            ));
        }
        Ok(())
    }
    /// Publish the final map only after complete source/target/fence/readiness
    /// evidence. Durable final intent allows exact retry after controller crash.
    pub fn publish_final(&mut self, controller: &ManualPlacement) -> Result<(), MembershipError> {
        let observed = controller.desired_placement();
        if self.stage == MembershipStage::Complete {
            if map_digest(&observed) != self.joint_digest()
                && map_digest(&observed) != map_digest(&self.desired)
                && map_digest(&observed) != map_digest(&self.current)
            {
                return Err(MembershipError::Invalid(
                    "controller diverged after final intent".into(),
                ));
            }
        } else {
            self.check_retirement()?;
            if map_digest(&observed) != self.joint_digest() {
                return Err(MembershipError::Invalid(
                    "controller is not at the captured joint map".into(),
                ));
            }
            self.stage = MembershipStage::Complete;
            self.persist()?;
        }
        controller.set_placement(self.desired.clone());
        Ok(())
    }
    fn validate_evidence(&self) -> Result<(), MembershipError> {
        if self
            .joint_authority
            .as_ref()
            .is_some_and(|authority| !authority.matches(&self.epoch, &self.joint()))
            || self
                .final_authority
                .as_ref()
                .is_some_and(|authority| !authority.matches(&self.epoch, &self.desired))
        {
            return Err(MembershipError::Invalid(
                "persisted authority differs from captured maps".into(),
            ));
        }
        for (source, manifest) in &self.sources {
            if source != manifest.source().node_id()
                || !self.required_sources().contains(source)
                || manifest.source().fragment_count() as usize != self.current.len()
            {
                return Err(MembershipError::Invalid(
                    "persisted source evidence is invalid".into(),
                ));
            }
        }
        for ((target, source), receipt) in &self.imports {
            let manifest = self
                .sources
                .get(source)
                .ok_or_else(|| MembershipError::Invalid("persisted import has no source".into()))?;
            if receipt.target().node_id() != target
                || receipt.source() != manifest.source()
                || receipt.manifest_digest() != &manifest.digest()
                || !self.target_nodes().contains(target)
            {
                return Err(MembershipError::Invalid(
                    "persisted import evidence is invalid".into(),
                ));
            }
        }
        Ok(())
    }
    fn persist(&self) -> Result<(), MembershipError> {
        if self.encoded_size_estimate() > MAX_STATE_BYTES {
            return Err(MembershipError::Invalid(
                "transition metadata exceeds supported bound".into(),
            ));
        }
        let mut out = Encoder::default();
        out.0.extend(b"ACTMEM04");
        out.digest(&self.epoch);
        out.u32(self.minimum_replicas as u32);
        out.u8(match self.stage {
            MembershipStage::Prepared => 0,
            MembershipStage::Joint => 1,
            MembershipStage::ReadyToRetire => 2,
            MembershipStage::Complete => 3,
        });
        encode_map(&mut out, &self.current);
        encode_map(&mut out, &self.desired);
        out.u32(self.sources.len() as u32);
        for manifest in self.sources.values() {
            out.bytes(&manifest.encode());
        }
        out.u32(self.imports.len() as u32);
        for receipt in self.imports.values() {
            out.bytes(&receipt.encode());
        }
        match &self.loss {
            None => out.u8(0),
            Some(loss) => {
                out.u8(1);
                out.digest(&loss.decision_id);
                out.u32(loss.missing.len() as u32);
                for source in &loss.missing {
                    out.string(source);
                }
            }
        }
        encode_authority(&mut out, self.joint_authority.as_ref());
        encode_authority(&mut out, self.final_authority.as_ref());
        let digest: [u8; 32] = Sha256::digest(&out.0).into();
        out.digest(&digest);
        if out.0.len() > MAX_STATE_BYTES {
            return Err(MembershipError::Invalid(
                "transition evidence exceeds supported bounded metadata".into(),
            ));
        }
        crate::checkpoint::atomic_write(&self.path, &out.0)?;
        Ok(())
    }
    fn encoded_size_estimate(&self) -> usize {
        // Includes upper bounds for both authority registries and explicit
        // loss decisions so new evidence cannot grow a buffer past the cap.
        4096 + map_size(&self.current)
            + map_size(&self.desired)
            + self
                .sources
                .values()
                .map(|manifest| manifest.encode().len() + 4)
                .sum::<usize>()
            + self
                .imports
                .values()
                .map(|receipt| receipt.encode().len() + 4)
                .sum::<usize>()
            + self.joint_authority.as_ref().map_or(0, |authority| {
                let mut out = Encoder::default();
                encode_authority(&mut out, Some(authority));
                out.0.len()
            })
            + self.final_authority.as_ref().map_or(0, |authority| {
                let mut out = Encoder::default();
                encode_authority(&mut out, Some(authority));
                out.0.len()
            })
            + self.loss.as_ref().map_or(0, |loss| {
                loss.missing.iter().map(|id| id.len() + 4).sum::<usize>()
            })
    }
}

fn map_size(map: &MembershipMap) -> usize {
    4 + map
        .values()
        .map(|replicas| 8 + replicas.iter().map(|id| id.len() + 4).sum::<usize>())
        .sum::<usize>()
}

fn encode_authority(out: &mut Encoder, authority: Option<&MembershipAuthority>) {
    match authority {
        None => out.u8(0),
        Some(authority) => {
            out.u8(1);
            out.digest(&authority.decision_id);
            out.u32(authority.generations.len() as u32);
            for (id, identity) in &authority.generations {
                out.string(id);
                out.u64(*identity);
            }
            out.u32(authority.native_receivers.len() as u32);
            for (id, receiver) in &authority.native_receivers {
                out.string(id);
                out.u64(receiver.node_boot_id);
                out.u64(receiver.authority_revision);
            }
            out.u32(authority.native_retired.len() as u32);
            for (id, receiver) in &authority.native_retired {
                out.string(id);
                out.u64(receiver.node_boot_id);
                out.u32(receiver.local_fence_count);
            }
        }
    }
}
fn decode_authority(
    input: &mut Decoder<'_>,
    epoch: [u8; 32],
) -> Result<Option<MembershipAuthority>, MembershipError> {
    match input.u8()? {
        0 => Ok(None),
        1 => {
            let decision = input.digest()?;
            let count = input.u32()? as usize;
            if count == 0 || count > 256 {
                return Err(MembershipError::Invalid(
                    "invalid persisted authority count".into(),
                ));
            }
            let mut generations = BTreeMap::new();
            for _ in 0..count {
                let id = input.string(255)?;
                let identity = input.u64()?;
                if generations.insert(id, identity).is_some() {
                    return Err(MembershipError::Invalid(
                        "duplicate persisted process identity".into(),
                    ));
                }
            }
            let mut authority =
                MembershipAuthority::certify_external(epoch, generations, decision)?;
            {
                let count = input.u32()? as usize;
                if count > 256 {
                    return Err(MembershipError::Invalid("too many native receivers".into()));
                }
                for _ in 0..count {
                    let id = input.string(255)?;
                    let receiver = NativeMembershipReceiver {
                        node_boot_id: input.u64()?,
                        authority_revision: input.u64()?,
                    };
                    if authority.native_receivers.insert(id, receiver).is_some() {
                        return Err(MembershipError::Invalid("duplicate native receiver".into()));
                    }
                }
                let count = input.u32()? as usize;
                if count > 256 {
                    return Err(MembershipError::Invalid(
                        "too many retired receivers".into(),
                    ));
                }
                for _ in 0..count {
                    let id = input.string(255)?;
                    let receiver = NativeRetiredReceiver {
                        node_boot_id: input.u64()?,
                        local_fence_count: input.u32()?,
                    };
                    if authority.native_retired.insert(id, receiver).is_some() {
                        return Err(MembershipError::Invalid(
                            "duplicate retired receiver".into(),
                        ));
                    }
                }
                if !authority.native_receivers.is_empty()
                    && authority.native_receivers.keys().collect::<BTreeSet<_>>()
                        != authority.generations.keys().collect()
                {
                    return Err(MembershipError::Invalid(
                        "native evidence does not cover exact registry".into(),
                    ));
                }
            }
            Ok(Some(authority))
        }
        _ => Err(MembershipError::Invalid(
            "invalid persisted authority flag".into(),
        )),
    }
}

fn validate_maps(
    epoch: [u8; 32],
    current: &MembershipMap,
    desired: &MembershipMap,
    minimum: usize,
) -> Result<(), MembershipError> {
    if epoch == [0; 32]
        || current.is_empty()
        || current.len() > 4096
        || current.len() != desired.len()
        || minimum == 0
        || minimum > 64
    {
        return Err(MembershipError::Invalid(
            "fresh epoch, fixed fragment layout and positive bounded replica minimum required"
                .into(),
        ));
    }
    validate_placement(current, current.len()).map_err(MembershipError::Invalid)?;
    validate_placement(desired, current.len()).map_err(MembershipError::Invalid)?;
    if map_size(current) + map_size(desired) + 4096 > MAX_STATE_BYTES {
        return Err(MembershipError::Invalid(
            "captured maps exceed bounded transition metadata".into(),
        ));
    }
    if current
        .values()
        .chain(desired.values())
        .any(|holders| holders.len() > 64)
        || desired.values().any(|holders| holders.len() < minimum)
        || current
            .values()
            .chain(desired.values())
            .flatten()
            .collect::<BTreeSet<_>>()
            .len()
            > 256
    {
        return Err(MembershipError::Invalid(
            "proposal violates replica minimum or bounded membership limits".into(),
        ));
    }
    for (fragment, old) in current {
        if old
            .iter()
            .chain(&desired[fragment])
            .collect::<BTreeSet<_>>()
            .len()
            > 64
        {
            return Err(MembershipError::Invalid(
                "joint map exceeds 64 holders per fragment".into(),
            ));
        }
    }
    Ok(())
}
fn encode_map(out: &mut Encoder, map: &MembershipMap) {
    out.u32(map.len() as u32);
    for (fragment, replicas) in map.iter().collect::<BTreeMap<_, _>>() {
        out.u32(fragment.0);
        out.u32(replicas.len() as u32);
        for replica in replicas {
            out.string(replica);
        }
    }
}
fn decode_map(input: &mut Decoder<'_>) -> Result<MembershipMap, MembershipError> {
    let count = input.u32()? as usize;
    if count == 0 || count > 4096 {
        return Err(MembershipError::Invalid(
            "invalid persisted fragment count".into(),
        ));
    }
    let mut map = HashMap::new();
    for _ in 0..count {
        let fragment = FragmentId(input.u32()?);
        let count = input.u32()? as usize;
        if count == 0 || count > 64 {
            return Err(MembershipError::Invalid(
                "invalid persisted replica count".into(),
            ));
        }
        let mut replicas = Vec::with_capacity(count);
        for _ in 0..count {
            replicas.push(input.string(255)?);
        }
        if map.insert(fragment, replicas).is_some() {
            return Err(MembershipError::Invalid(
                "duplicate persisted fragment".into(),
            ));
        }
    }
    Ok(map)
}
pub(crate) fn map_digest(map: &MembershipMap) -> [u8; 32] {
    let mut out = Encoder::default();
    encode_map(&mut out, map);
    Sha256::digest(&out.0).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fresh_journal_rejects_old_headers_without_modification_and_roundtrips_generations() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("transition");
        let current = HashMap::from([(FragmentId(0), vec!["a".into(), "b".into()])]);
        let desired = HashMap::from([(FragmentId(0), vec!["b".into(), "c".into()])]);
        let mut transition =
            MembershipTransition::create(&path, [7; 32], current.clone(), desired, 2).unwrap();
        let registry = [("a".into(), 1), ("b".into(), 2), ("c".into(), 1)]
            .into_iter()
            .collect();
        transition
            .record_authority_fence(
                MembershipAuthority::certify_external([7; 32], registry, [10; 32]).unwrap(),
            )
            .unwrap();
        transition
            .publish_joint(&ManualPlacement::new(current))
            .unwrap();
        let reopened = MembershipTransition::reopen(&path).unwrap();
        assert_eq!(reopened.joint_authority().unwrap().generations()["b"], 2);
        let original = std::fs::read(&path).unwrap();
        for header in [b"ACTMEM02", b"ACTMEM03"] {
            let mut obsolete = original.clone();
            obsolete[..8].copy_from_slice(header);
            let offset = obsolete.len() - 32;
            let digest: [u8; 32] = Sha256::digest(&obsolete[..offset]).into();
            obsolete[offset..].copy_from_slice(&digest);
            std::fs::write(&path, &obsolete).unwrap();
            assert!(MembershipTransition::reopen(&path).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), obsolete);
        }
    }
}
