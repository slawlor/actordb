//! Durability errors must never become successful in-memory evidence or cutover.

use super::journal::faults::{inject, Stage, STAGES};
use super::*;
use crate::store::StoreIdentity;

const EPOCH: [u8; 32] = [7; 32];

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Source,
    Import,
    Loss,
    Authority,
    FinalAuthority,
    Ready,
    Joint,
    Final,
}
const MUTATIONS: [Mutation; 8] = [
    Mutation::Source,
    Mutation::Import,
    Mutation::Loss,
    Mutation::Authority,
    Mutation::FinalAuthority,
    Mutation::Ready,
    Mutation::Joint,
    Mutation::Final,
];

fn map(ids: &[&str]) -> MembershipMap {
    HashMap::from([(FragmentId(0), ids.iter().map(|id| (*id).into()).collect())])
}

// These codec fixtures exercise journal transactions without a store feature.
// Real exported/imported floors and native receiver reports are independently
// exercised by the checkpoints and native_membership integration suites.
fn manifest(id: &str) -> CheckpointManifest {
    let mut out = Encoder::default();
    out.0.extend(b"ACTCP003");
    out.identity(&StoreIdentity::new(id.into(), 1));
    out.u32(1024 * 1024);
    out.u32(256);
    out.u8(0);
    let root: [u8; 32] = Sha256::digest(&out.0).into();
    out.u64(0);
    out.u64(0);
    out.digest(&root);
    out.digest(&Sha256::digest([]).into());
    CheckpointManifest::decode(&out.0).unwrap()
}
fn receipt(id: &str) -> ImportReceipt {
    let source = manifest(id);
    let mut out = Encoder::default();
    out.identity(&StoreIdentity::new("b".into(), 1));
    out.identity(source.source());
    out.digest(&source.digest());
    out.digest(source.root_hash());
    out.u64(source.records());
    ImportReceipt::decode(&out.0).unwrap()
}
fn authority(final_policy: bool, native: bool) -> MembershipAuthority {
    let ids: &[&str] = if final_policy { &["b"] } else { &["a", "b"] };
    let mut authority = MembershipAuthority::certify_external(
        EPOCH,
        ids.iter().map(|id| ((*id).into(), 2)).collect(),
        if final_policy { [9; 32] } else { [8; 32] },
    )
    .unwrap();
    if native {
        authority.native_receivers = ids
            .iter()
            .map(|id| {
                (
                    (*id).into(),
                    NativeMembershipReceiver {
                        node_boot_id: if *id == "a" { 10 } else { 11 },
                        authority_revision: 3,
                    },
                )
            })
            .collect();
        if final_policy {
            authority.native_retired.insert(
                "a".into(),
                NativeRetiredReceiver {
                    node_boot_id: 10,
                    local_fence_count: 1,
                },
            );
        }
    }
    authority
}
fn ready(transition: &MembershipTransition) -> MembershipReady {
    MembershipReady {
        node_id: "b".into(),
        boot_id: 11,
        placement_generation: 4,
        joint_digest: transition.joint_digest(),
        epoch: EPOCH,
        authority_digest: transition.readiness_authority_digest().unwrap(),
        native_revision: transition
            .final_authority()
            .or(transition.joint_authority())
            .unwrap()
            .is_native()
            .then_some(3),
    }
}
fn apply(
    mutation: Mutation,
    transition: &mut MembershipTransition,
    controller: &ManualPlacement,
    native: bool,
) -> Result<(), MembershipError> {
    match mutation {
        Mutation::Source => transition.record_source(manifest("a")),
        Mutation::Import => transition.record_import(receipt("a")),
        Mutation::Loss => transition.authorize_permanent_loss(
            PermanentSourceLoss::authorize(EPOCH, ["a".into()], [6; 32]).unwrap(),
        ),
        Mutation::Authority => transition.record_authority_fence(authority(false, native)),
        Mutation::FinalAuthority => {
            transition.record_final_authority_fence(authority(true, native))
        }
        Mutation::Ready => transition.record_ready(ready(transition)),
        Mutation::Joint => transition.publish_joint(controller),
        Mutation::Final => transition.publish_final(controller),
    }
}
fn fixture(
    path: &Path,
    mutation: Mutation,
    native: bool,
) -> (MembershipTransition, ManualPlacement) {
    let desired = if matches!(mutation, Mutation::Joint) {
        map(&["b", "c"])
    } else {
        map(&["b"])
    };
    let mut transition =
        MembershipTransition::create(path, EPOCH, map(&["a", "b"]), desired, 1).unwrap();
    let controller = ManualPlacement::new(transition.current().clone());
    if matches!(mutation, Mutation::Import) {
        transition.record_source(manifest("a")).unwrap();
    }
    if matches!(mutation, Mutation::Loss) {
        transition.record_source(manifest("b")).unwrap();
        transition
            .record_authority_fence(authority(false, native))
            .unwrap();
    }
    if matches!(
        mutation,
        Mutation::FinalAuthority | Mutation::Ready | Mutation::Final
    ) {
        for id in ["a", "b"] {
            transition.record_source(manifest(id)).unwrap();
            transition.record_import(receipt(id)).unwrap();
        }
        transition
            .record_authority_fence(authority(false, native))
            .unwrap();
        transition.publish_joint(&controller).unwrap();
        if matches!(mutation, Mutation::FinalAuthority) {
            // Adding final authority clears already acquired joint-policy
            // readiness only when that invalidation is durably committed.
            transition.record_ready(ready(&transition)).unwrap();
        }
    }
    if matches!(mutation, Mutation::Ready | Mutation::Final) {
        transition
            .record_final_authority_fence(authority(true, native))
            .unwrap();
    }
    if matches!(mutation, Mutation::Final) {
        transition.record_ready(ready(&transition)).unwrap();
    }
    (transition, controller)
}
fn memory(transition: &MembershipTransition) -> String {
    format!(
        "{:?}/{:?}/{:?}/{:?}/{:?}/{:?}/{:?}/{:?}",
        transition.stage,
        transition.sources,
        transition.imports,
        transition.loss,
        transition.joint_authority,
        transition.final_authority,
        transition.ready,
        transition.joint_digest(),
    )
}

#[test]
fn every_journal_mutation_preserves_memory_and_controller_after_every_io_failure() {
    for native in [false, true] {
        for mutation in MUTATIONS {
            for stage in STAGES {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("transition");
                let (mut transition, controller) = fixture(&path, mutation, native);
                let committed = memory(&transition);
                let old_file = std::fs::read(&path).unwrap();
                let old_map = controller.placement();
                // Two failed identical calls must still attempt persistence;
                // immutable evidence fast paths cannot turn failure into Ok.
                for attempt in 0..2 {
                    let _fault = inject(&path, stage);
                    assert!(
                        apply(mutation, &mut transition, &controller, native).is_err(),
                        "{mutation:?}/{stage:?}/native={native}/attempt={attempt}"
                    );
                    assert_eq!(memory(&transition), committed);
                    assert_eq!(controller.placement(), old_map);
                    if stage != Stage::ParentSync {
                        assert_eq!(std::fs::read(&path).unwrap(), old_file);
                    } else {
                        assert_ne!(
                            std::fs::read(&path).unwrap(),
                            old_file,
                            "parent-fsync fault must follow actual candidate rename"
                        );
                    }
                }
                apply(mutation, &mut transition, &controller, native).unwrap();
                assert_ne!(memory(&transition), committed);
                if matches!(mutation, Mutation::Joint | Mutation::Final) {
                    assert_ne!(controller.placement(), old_map);
                }
                if !matches!(mutation, Mutation::Ready) {
                    // Normal immutable evidence/publication retries remain supported.
                    apply(mutation, &mut transition, &controller, native).unwrap();
                }
                MembershipTransition::reopen(&path).unwrap();
            }
        }
    }
}

#[test]
fn reopening_a_renamed_candidate_requires_a_fresh_durable_write_for_every_mutation() {
    for mutation in MUTATIONS {
        for reopen_failure in STAGES {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("transition");
            let (mut transition, controller) = fixture(&path, mutation, true);
            let old_map = controller.placement();
            {
                let _fault = inject(&path, Stage::ParentSync);
                assert!(apply(mutation, &mut transition, &controller, true).is_err());
            }
            {
                let _fault = inject(&path, reopen_failure);
                assert!(MembershipTransition::reopen(&path).is_err());
                assert_eq!(controller.placement(), old_map);
            }
            let mut recovered = MembershipTransition::reopen(&path).unwrap();
            if matches!(mutation, Mutation::Ready) {
                assert_eq!(recovered.stage(), MembershipStage::Joint);
                assert!(recovered.ready.is_empty());
            }
            apply(mutation, &mut recovered, &controller, true).unwrap();
        }
    }
}

#[test]
fn creation_failures_support_exact_create_or_reopen_recovery() {
    for stage in STAGES {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("transition");
        {
            let _fault = inject(&path, stage);
            assert!(
                MembershipTransition::create(&path, EPOCH, map(&["a", "b"]), map(&["b"]), 1)
                    .is_err()
            );
        }
        let recovered = if stage == Stage::ParentSync {
            assert!(path.is_file());
            assert!(
                MembershipTransition::create(&path, EPOCH, map(&["a", "b"]), map(&["b"]), 1)
                    .is_err()
            );
            MembershipTransition::reopen(&path).unwrap()
        } else {
            assert!(!path.exists());
            MembershipTransition::create(&path, EPOCH, map(&["a", "b"]), map(&["b"]), 1).unwrap()
        };
        assert_eq!(recovered.stage(), MembershipStage::Prepared);
        assert_eq!(std::fs::read(&path).unwrap()[..8], *b"ACTMEM04");
    }
}

#[test]
fn completed_replay_requires_sync_but_not_new_readiness_or_retired_processes() {
    for stage in STAGES {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("transition");
        let (mut transition, controller) = fixture(&path, Mutation::Final, true);
        transition.publish_final(&controller).unwrap();
        let mut recovered = MembershipTransition::reopen(&path).unwrap();
        assert_eq!(recovered.stage(), MembershipStage::Complete);
        assert!(recovered.ready.is_empty());
        let restarted_controller = ManualPlacement::new(recovered.current().clone());
        for _ in 0..2 {
            let _fault = inject(&path, stage);
            assert!(recovered.publish_final(&restarted_controller).is_err());
            assert_eq!(restarted_controller.placement(), *recovered.current());
            assert_eq!(recovered.stage(), MembershipStage::Complete);
        }
        recovered.publish_final(&restarted_controller).unwrap();
        assert_eq!(restarted_controller.placement(), *recovered.desired());
    }
}

#[test]
fn unwritable_journal_cannot_be_bypassed_by_same_object_final_retry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("transition");
    let backup = directory.path().join("committed-backup");
    let (mut transition, controller) = fixture(&path, Mutation::Final, true);
    std::fs::rename(&path, &backup).unwrap();
    std::fs::create_dir(&path).unwrap();
    for _ in 0..2 {
        assert!(transition.publish_final(&controller).is_err());
        assert_eq!(transition.stage(), MembershipStage::ReadyToRetire);
        assert_eq!(controller.placement(), *transition.current());
    }
    assert_eq!(
        MembershipTransition::reopen(&backup).unwrap().stage(),
        MembershipStage::Joint
    );
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(&backup, &path).unwrap();
    transition.publish_final(&controller).unwrap();
    assert_eq!(controller.placement(), *transition.desired());
}

#[test]
fn durable_reopen_preserves_m04_bytes_and_rejected_journals_remain_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("transition");
    let (mut transition, controller) = fixture(&path, Mutation::Final, true);
    transition.publish_final(&controller).unwrap();
    let original = std::fs::read(&path).unwrap();
    let recovered = MembershipTransition::reopen(&path).unwrap();
    assert_eq!(recovered.stage(), MembershipStage::Complete);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    for header in [b"ACTMEM02", b"ACTMEM03"] {
        let mut obsolete = original.clone();
        obsolete[..8].copy_from_slice(header);
        let offset = obsolete.len() - 32;
        let digest: [u8; 32] = Sha256::digest(&obsolete[..offset]).into();
        obsolete[offset..].copy_from_slice(&digest);
        std::fs::write(&path, &obsolete).unwrap();
        assert!(MembershipTransition::reopen(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), obsolete);
        assert!(!path.with_extension("pending").exists());
    }
    let mut corrupt = original;
    corrupt[20] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(MembershipTransition::reopen(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
    assert!(!path.with_extension("pending").exists());
}

#[test]
fn fault_scope_restores_previous_hook_on_unwind_and_is_path_local() {
    let directory = tempfile::tempdir().unwrap();
    let outer = directory.path().join("outer");
    let inner = directory.path().join("inner");
    let _outer_fault = inject(&outer, Stage::Create);
    assert!(std::panic::catch_unwind(|| {
        let _inner_fault = inject(&inner, Stage::Create);
        panic!("exercise fault-guard unwind");
    })
    .is_err());
    journal::atomic_write(&inner, b"unaffected").unwrap();
    assert!(journal::atomic_write(&outer, b"fails").is_err());
}
