//! Node-backed stable checkpoint and live membership evidence.

use std::path::Path;

use crate::checkpoint::{
    CheckpointError, CheckpointExporter, CheckpointManifest, CheckpointOptions,
    RemoteReplicationEvidence,
};
use crate::membership::{
    map_digest, MembershipError, MembershipReady, MembershipStage, MembershipTransition,
};

use super::drain::DrainPhase;
use super::status::{Readiness, SnapshotLimits};
use super::*;

impl Node {
    /// Export after completed full local shutdown. The backend must provide
    /// durable immutable paging and a positive stable fence; configured-store
    /// completion alone is not interpreted as stable media. A completed remote
    /// replication fence is attached separately when this node actually has it.
    /// Cancellation leaves the blocking exporter owning its work and leases.
    pub async fn export_checkpoint(
        &self,
        directory: impl AsRef<Path>,
        mut options: CheckpointOptions,
    ) -> Result<CheckpointManifest, CheckpointError> {
        let status = self.drain_status();
        if status.phase() != DrainPhase::Stopped || !status.receive_apply_complete() {
            return Err(CheckpointError::Invalid(
                "checkpoint export requires completed full local shutdown".into(),
            ));
        }
        let store = self
            .checkpoint_store
            .clone()
            .ok_or(CheckpointError::Unsupported)?;
        let identity = StoreIdentity::new(self.node_id.clone(), self.router.fragment_count as u32);
        let directory = directory.as_ref().to_path_buf();
        let resume = directory.join("export-state").exists();
        if directory.join("manifest").exists() || resume {
            // Retry with the original evidence/limits. A new process cannot
            // synthesize an ACK report it no longer owns, but the already
            // frozen bundle's evidence remains part of its exact manifest.
            if directory.join("manifest").exists() {
                let prior = CheckpointManifest::read(&directory)?;
                if prior.source() != &identity
                    || prior.options().max_chunk_bytes() != options.max_chunk_bytes()
                    || prior.options().max_chunk_records() != options.max_chunk_records()
                {
                    return Err(CheckpointError::Invalid(
                        "export retry changes source or limits".into(),
                    ));
                }
                options = prior.options().clone();
            }
        } else if let Some(report) = self.replication_drain_report.lock().unwrap().as_ref() {
            if report.targets().len() > 4096 {
                return Err(CheckpointError::BudgetExceeded(
                    "remote fence evidence exceeds target limit".into(),
                ));
            }
            options.remote = Some(RemoteReplicationEvidence {
                source_id: self.node_id.clone(),
                targets: report
                    .targets()
                    .iter()
                    .map(|target| (target.fragment(), target.peer_id().to_owned()))
                    .collect(),
                durability: report.durability(),
            });
        }
        tokio::task::spawn_blocking(move || {
            if resume {
                CheckpointExporter::resume(store, identity, directory)?.finish()
            } else {
                CheckpointExporter::open(store, identity, directory, options)?.finish()
            }
        })
        .await
        .map_err(|error| CheckpointError::Invalid(format!("checkpoint worker failed: {error}")))?
    }

    /// Certify actual joint placement serving after every qualified source
    /// floor was durably imported into this exact target store. Imported writer
    /// IDs remain historical record origins, never peer credentials.
    pub fn certify_membership_ready(
        &self,
        transition: &MembershipTransition,
    ) -> Result<MembershipReady, MembershipError> {
        if transition.stage() != MembershipStage::Joint
            || !transition.target_nodes().contains(&self.node_id)
        {
            return Err(MembershipError::Invalid(
                "node is not a final target in an active joint transition".into(),
            ));
        }
        let store = self.checkpoint_store.as_ref().ok_or_else(|| {
            MembershipError::InsufficientEvidence(
                "target has no durable checkpoint-capable store".into(),
            )
        })?;
        let backend = store
            .checkpoint_backend()
            .ok_or(CheckpointError::Unsupported)?;
        let mut verified_receipts = Vec::new();
        for source in transition.required_sources() {
            if transition
                .missing_source_authorization()
                .is_some_and(|loss| loss.missing_sources().contains(&source))
            {
                continue;
            }
            let receipt = transition
                .import_receipts()
                .find(|receipt| {
                    receipt.target().node_id() == self.node_id
                        && receipt.source().node_id() == source
                })
                .ok_or_else(|| {
                    MembershipError::InsufficientEvidence(format!(
                        "target lacks qualified import from {source}"
                    ))
                })?;
            if backend.import_evidence(receipt.manifest_digest())?.as_ref() != Some(receipt) {
                return Err(MembershipError::InsufficientEvidence(format!(
                    "target no longer retains the imported floor from {source}"
                )));
            }
            verified_receipts.push(receipt);
        }
        let status = self.status_snapshot_with_limits(SnapshotLimits::new(4096, 4096));
        if status.readiness != Readiness::Ready || !status.observation.is_complete() {
            return Err(MembershipError::InsufficientEvidence(
                "joint placement is not completely observed and serving".into(),
            ));
        }
        let observed = status
            .fragments
            .iter()
            .map(|fragment| (fragment.fragment, fragment.replicas.clone()))
            .collect::<HashMap<_, _>>();
        if map_digest(&observed) != transition.joint_digest() {
            return Err(MembershipError::InsufficientEvidence(
                "node is serving a different placement generation/proposal".into(),
            ));
        }
        for receipt in verified_receipts {
            if backend.import_evidence(receipt.manifest_digest())?.as_ref() != Some(receipt) {
                return Err(MembershipError::InsufficientEvidence(
                    "imported floor changed while serving readiness was observed".into(),
                ));
            }
        }
        Ok(MembershipReady {
            node_id: self.node_id.clone(),
            boot_id: self.boot_id,
            placement_generation: status.placement_generation.ok_or_else(|| {
                MembershipError::InsufficientEvidence("placement generation unavailable".into())
            })?,
            joint_digest: transition.joint_digest(),
            epoch: *transition.epoch(),
            authority_digest: transition.readiness_authority_digest().ok_or_else(|| {
                MembershipError::InsufficientEvidence(
                    "readiness requires an explicitly fenced authority".into(),
                )
            })?,
        })
    }
}
