//! Native SDK reports, separate from distributed application authorization.

use super::*;
use crate::node::{
    DrainPhase, DrainReport, Node, PeerAuthoritySnapshot, PeerFencePhase, PeerFenceReport,
};

/// One live SDK receiver and the completed hot fence returned by that receiver.
pub struct MembershipFenceObservation<'a> {
    pub node: &'a Node,
    pub report: &'a PeerFenceReport,
}
/// A retiring SDK process and its completed full local shutdown report.
pub struct MembershipShutdownObservation<'a> {
    pub node: &'a Node,
    pub report: &'a DrainReport,
}

impl MembershipAuthority {
    /// Certify every joint receiver's genuine completed/current native policy.
    /// The application still supplies its own epoch and distributed decision ID.
    pub fn certify_native_joint(
        epoch: [u8; 32],
        joint: &MembershipMap,
        observations: &[MembershipFenceObservation<'_>],
        decision_id: [u8; 32],
    ) -> Result<Self, MembershipError> {
        certify_receivers(epoch, joint, observations, decision_id)
    }
    /// Certify all final live receivers and separately verify every retired SDK
    /// process reached full shutdown. Peer revocation is not shutdown evidence.
    pub fn certify_native_final(
        epoch: [u8; 32],
        joint: &MembershipMap,
        desired: &MembershipMap,
        observations: &[MembershipFenceObservation<'_>],
        retired: &[MembershipShutdownObservation<'_>],
        decision_id: [u8; 32],
    ) -> Result<Self, MembershipError> {
        let mut result = certify_receivers(epoch, desired, observations, decision_id)?;
        let expected = node_ids(joint)
            .difference(&node_ids(desired))
            .cloned()
            .collect::<BTreeSet<_>>();
        for observation in retired {
            let node = observation.node;
            let report = observation.report;
            let current = node.drain_status();
            let boot = node.status_snapshot().boot_id;
            if !expected.contains(node.id())
                || !report.shutdown_completed()
                || !report.receive_apply_complete()
                || current.phase() != DrainPhase::Stopped
                || !current.receive_apply_complete()
                || report
                    .status()
                    .cutoff()
                    .is_none_or(|cutoff| cutoff.node_boot_id != boot)
                || result.native_retired.contains_key(node.id())
            {
                return Err(MembershipError::InsufficientEvidence(
                    "retired SDK process lacks matching full shutdown evidence".into(),
                ));
            }
            result.native_retired.insert(
                node.id().to_owned(),
                NativeRetiredReceiver {
                    node_boot_id: boot,
                    local_fence_count: report.local_fences().len() as u32,
                },
            );
        }
        if result
            .native_retired
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected
        {
            return Err(MembershipError::InsufficientEvidence(
                "every retired SDK receiver needs full shutdown evidence".into(),
            ));
        }
        Ok(result)
    }
}

pub(crate) fn matches_snapshot(
    authority: &MembershipAuthority,
    snapshot: &PeerAuthoritySnapshot,
) -> bool {
    if snapshot.phase() != PeerFencePhase::Ready {
        return false;
    }
    let Some(local) = authority.generations().get(snapshot.receiver_id()) else {
        return false;
    };
    *local == snapshot.local_generation()
        && snapshot.peer_generations().len() + 1 == authority.generations().len()
        && snapshot
            .peer_generations()
            .iter()
            .all(|(id, generation)| authority.generations().get(id) == Some(generation))
        && !snapshot
            .peer_generations()
            .contains_key(snapshot.receiver_id())
}
fn node_ids(map: &MembershipMap) -> BTreeSet<String> {
    map.values().flatten().cloned().collect()
}
fn certify_receivers(
    epoch: [u8; 32],
    map: &MembershipMap,
    observations: &[MembershipFenceObservation<'_>],
    decision: [u8; 32],
) -> Result<MembershipAuthority, MembershipError> {
    let expected = node_ids(map);
    if observations.is_empty() || observations.len() != expected.len() || expected.len() > 256 {
        return Err(MembershipError::InsufficientEvidence(
            "native fence reports must cover every exact live receiver".into(),
        ));
    }
    let mut snapshots = Vec::new();
    let mut receivers = BTreeMap::new();
    let mut registry = None;
    for observation in observations {
        let snapshot = observation.node.peer_authority_snapshot().ok_or_else(|| {
            MembershipError::InsufficientEvidence(
                "external-only authority is not a native SDK receiver fence".into(),
            )
        })?;
        if snapshot.phase() != PeerFencePhase::Ready
            || snapshot.receiver_id() != observation.node.id()
            || &snapshot != observation.report.public_authority()
            || !expected.contains(snapshot.receiver_id())
        {
            return Err(MembershipError::InsufficientEvidence(
                "SDK fence report is stale, pending, or from another node incarnation/policy"
                    .into(),
            ));
        }
        let mut identities = snapshot
            .peer_generations()
            .iter()
            .map(|(id, generation)| (id.clone(), *generation))
            .collect::<BTreeMap<_, _>>();
        identities.insert(
            snapshot.receiver_id().to_owned(),
            snapshot.local_generation(),
        );
        if identities.keys().cloned().collect::<BTreeSet<_>>() != expected
            || registry.as_ref().is_some_and(|prior| prior != &identities)
        {
            return Err(MembershipError::InsufficientEvidence(
                "live receiver generation policies disagree with exact membership".into(),
            ));
        }
        registry = Some(identities);
        if receivers
            .insert(
                snapshot.receiver_id().to_owned(),
                NativeMembershipReceiver {
                    node_boot_id: snapshot.node_boot_id(),
                    authority_revision: snapshot.revision(),
                },
            )
            .is_some()
        {
            return Err(MembershipError::Invalid(
                "duplicate native receiver evidence".into(),
            ));
        }
        snapshots.push(snapshot);
    }
    for (observation, captured) in observations.iter().zip(snapshots) {
        if observation.node.peer_authority_snapshot().as_ref() != Some(&captured) {
            return Err(MembershipError::InsufficientEvidence(
                "native policy changed during certification".into(),
            ));
        }
    }
    let mut authority = MembershipAuthority::certify_external(epoch, registry.unwrap(), decision)?;
    authority.native_receivers = receivers;
    Ok(authority)
}
