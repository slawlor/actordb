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
use super::peer_authority::PeerFencePhase;
use super::status::{NodeLifecycle, Readiness, SnapshotLimits, SnapshotObservation};
use super::*;

impl Node {
    /// Export after completed full local shutdown. The backend must provide
    /// durable immutable paging and a positive stable fence; configured-store
    /// completion alone is not interpreted as stable media. A completed remote
    /// replication fence is attached separately when this node actually has it.
    /// Cancellation leaves the blocking exporter owning its work and leases.
    /// Retain the caller's store Arc through this call: shutdown releases Node's
    /// own backend ownership, and an expired handle returns `BackendReleased`.
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
            .as_ref()
            .ok_or(CheckpointError::Unsupported)?
            .upgrade()
            .ok_or(CheckpointError::BackendReleased)?;
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
        let authority = transition
            .final_authority()
            .or(transition.joint_authority())
            .ok_or_else(|| {
                MembershipError::InsufficientEvidence(
                    "membership has no explicit authority decision".into(),
                )
            })?;
        let captured_authority = self.peer_authority_snapshot();
        let captured_peer_phase = self.peer_fence_status().phase();
        if let Some(snapshot) = &captured_authority {
            if snapshot.receiver_id() != self.node_id
                || snapshot.node_boot_id() != self.boot_id
                || snapshot.phase() != PeerFencePhase::Ready
                || !crate::membership::native::matches_snapshot(authority, snapshot)
            {
                return Err(MembershipError::InsufficientEvidence(
                    "current native receiver policy does not match the exact transition policy"
                        .into(),
                ));
            }
            if authority.is_native()
                && authority
                    .native_receivers()
                    .get(&self.node_id)
                    .is_none_or(|receiver| {
                        receiver.node_boot_id != self.boot_id
                            || receiver.authority_revision != snapshot.revision()
                    })
            {
                return Err(MembershipError::InsufficientEvidence(
                    "native transition fence belongs to another boot/revision".into(),
                ));
            }
        } else if authority.is_native() {
            return Err(MembershipError::InsufficientEvidence(
                "native evidence requires a configured receiver-generation policy".into(),
            ));
        } else if captured_peer_phase != PeerFencePhase::Unconfigured {
            return Err(MembershipError::InsufficientEvidence(
                "external-only readiness is unavailable during native fencing/shutdown".into(),
            ));
        }
        let store = self
            .checkpoint_store
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
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
        if status.readiness != Readiness::Ready
            || status.lifecycle != NodeLifecycle::Serving
            || !placement_observation_complete(&status.observation)
        {
            return Err(MembershipError::InsufficientEvidence(
                "joint placement is not completely observed and serving".into(),
            ));
        }
        if status.fragments.iter().any(|fragment| {
            fragment.assigned_locally
                && (fragment.placement_state != Some(FragmentPlacementState::Serving)
                    || !fragment.serving
                    || !fragment.route_available)
        }) {
            return Err(MembershipError::InsufficientEvidence(
                "every assigned joint fragment must have completed Serving placement".into(),
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
        if self.placement_generation.load(Ordering::Acquire)
            != status.placement_generation.unwrap_or(u64::MAX)
            || self.peer_authority_snapshot() != captured_authority
            || self.peer_fence_status().phase() != captured_peer_phase
            || self.drain_status().phase() != DrainPhase::Running
            || self.lifecycle_observation.revision.load(Ordering::Acquire)
                != status.lifecycle_revision
        {
            return Err(MembershipError::InsufficientEvidence(
                "placement or native authority changed during certification".into(),
            ));
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
            native_revision: captured_authority.map(|snapshot| snapshot.revision()),
        })
    }
}

fn placement_observation_complete(observation: &SnapshotObservation) -> bool {
    !observation.placement_unavailable
        && !observation.fragments_truncated
        && !observation.replicas_truncated
        && !observation.placement_changed
        && !observation.lifecycle_changed
        && !observation.lifecycle_reason_unavailable
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn telemetry_limits_do_not_make_complete_placement_ineligible() {
        let mut observation = SnapshotObservation {
            table_metrics_truncated: true,
            table_metrics_unavailable: true,
            senders_truncated: true,
            senders_unavailable: true,
            unavailable_sender_reports: 100,
            stale_sender_reports: 100,
            ..Default::default()
        };
        assert!(placement_observation_complete(&observation));
        observation.fragments_truncated = true;
        assert!(!placement_observation_complete(&observation));
    }
}
