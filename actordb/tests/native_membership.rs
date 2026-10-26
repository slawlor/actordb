#![cfg(feature = "rocksdb")]
mod common;
use actordb::checkpoint::{
    CheckpointImporter, CheckpointOptions, CheckpointReplicaCertificate,
    CheckpointReplicaObservation,
};
use actordb::membership::{
    MembershipAuthority, MembershipFenceObservation, MembershipShutdownObservation,
    MembershipStage, MembershipTransition,
};
use actordb::node::{
    DrainOptions, Node, NodeConfig, NodeRuntimeOptions, PeerAuthorityOptions, PeerFenceOptions,
    PeerFenceReport, RecoveryOptions,
};
use actordb::placement::{FragmentPlacementState, ManualPlacement, PlacementController};
use actordb::shard::FragmentId;
use actordb::store::{KvStore, RocksDbStore, StoreIdentity};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn policy(id: &str, ids: &[String], generation: u64) -> PeerAuthorityOptions {
    PeerAuthorityOptions::new(
        generation,
        ids.iter()
            .filter(|peer| peer.as_str() != id)
            .map(|peer| (peer.clone(), generation))
            .collect(),
    )
}
fn map(ids: &[&str]) -> HashMap<FragmentId, Vec<String>> {
    HashMap::from([(FragmentId(0), ids.iter().map(|id| (*id).into()).collect())])
}

async fn workflow(old: &[&str], desired: &[&str]) {
    let directory = tempfile::tempdir().unwrap();
    let epoch = [9; 32];
    let controller = Arc::new(ManualPlacement::new(map(old)));
    let mut transition = MembershipTransition::create(
        directory.path().join("transition"),
        epoch,
        map(old),
        map(desired),
        desired.len(),
    )
    .unwrap();
    let mut stores = HashMap::new();
    let mut bundles = HashMap::new();
    let mut manifests = HashMap::new();
    for id in old {
        let store = Arc::new(RocksDbStore::open(directory.path().join(id)).unwrap());
        let source = Node::start(
            (*id).into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .fragment_count(1)
                .store(store.clone())
                .build(),
        )
        .await
        .unwrap();
        source
            .put((*id).into(), id.as_bytes().to_vec())
            .await
            .unwrap();
        source.shutdown().await.unwrap();
        let bundle = directory.path().join(format!("bundle-{id}"));
        let manifest = source
            .export_checkpoint(&bundle, CheckpointOptions::default())
            .await
            .unwrap();
        transition.record_source(manifest.clone()).unwrap();
        stores.insert((*id).to_owned(), store);
        bundles.insert((*id).to_owned(), bundle);
        manifests.insert((*id).to_owned(), manifest);
    }
    transition.publish_joint(&controller).unwrap();
    let mut receipts = Vec::new();
    for target in desired {
        let store = stores
            .entry((*target).to_owned())
            .or_insert_with(|| Arc::new(RocksDbStore::open(directory.path().join(target)).unwrap()))
            .clone();
        for source in old {
            let receipt = CheckpointImporter::open(
                store.clone(),
                StoreIdentity::new((*target).into(), 1),
                &bundles[*source],
            )
            .unwrap()
            .finish()
            .unwrap();
            transition.record_import(receipt.clone()).unwrap();
            receipts.push(receipt);
        }
    }
    let first = old[0];
    let observed = receipts
        .iter()
        .filter(|receipt| receipt.source().node_id() == first)
        .map(|receipt| CheckpointReplicaObservation {
            store: &*stores[receipt.target().node_id()],
            receipt,
        })
        .collect::<Vec<_>>();
    let expected = desired
        .iter()
        .map(|id| StoreIdentity::new((*id).into(), 1))
        .collect::<Vec<_>>();
    let certificate =
        CheckpointReplicaCertificate::collect(&manifests[first], &expected, &observed).unwrap();
    assert_eq!(certificate.targets().len(), desired.len());
    assert!(
        CheckpointReplicaCertificate::collect(&manifests[first], &expected, &observed[..0])
            .is_err()
    );
    let joint_ids = transition.joint()[&FragmentId(0)].clone();
    let mut nodes = HashMap::new();
    for (id, store) in &stores {
        let config = NodeConfig::builder()
            .fragment_count(1)
            .store(store.clone())
            .placement_controller(controller.clone())
            .placement_poll_interval(Duration::from_millis(10))
            .recovery(RecoveryOptions::new(epoch).unwrap())
            .build();
        nodes.insert(
            id.clone(),
            Node::start_with_options(
                id.clone(),
                "127.0.0.1:0".parse().unwrap(),
                config,
                NodeRuntimeOptions::default().with_peer_authority(policy(id, &joint_ids, 1)),
            )
            .await
            .unwrap(),
        );
    }
    for (index, id) in joint_ids.iter().enumerate() {
        for peer in joint_ids.iter().skip(index + 1) {
            nodes[id].connect(nodes[peer].addr()).await.unwrap();
        }
    }
    for node in nodes.values() {
        common::wait_for(
            || async {
                node.placement_report().await.states.get(&FragmentId(0))
                    == Some(&FragmentPlacementState::Serving)
            },
            Duration::from_secs(10),
        )
        .await;
    }
    let mut joint_reports = HashMap::<String, PeerFenceReport>::new();
    for id in &joint_ids {
        joint_reports.insert(
            id.clone(),
            nodes[id]
                .advance_peer_authority(policy(id, &joint_ids, 2), PeerFenceOptions::default())
                .await
                .unwrap(),
        );
    }
    let observations = joint_ids
        .iter()
        .map(|id| MembershipFenceObservation {
            node: &nodes[id],
            report: &joint_reports[id],
        })
        .collect::<Vec<_>>();
    assert!(MembershipAuthority::certify_native_joint(
        epoch,
        &transition.joint(),
        &observations[..0],
        [1; 32]
    )
    .is_err());
    let native_joint = MembershipAuthority::certify_native_joint(
        epoch,
        &transition.joint(),
        &observations,
        [1; 32],
    )
    .unwrap();
    assert!(native_joint.is_native());
    transition.record_authority_fence(native_joint).unwrap();
    for index in 0..65 {
        let table = nodes[desired[0]]
            .table(format!("metric_scope_{index}"))
            .unwrap();
        nodes[desired[0]].enable_table_metrics(table.id()).unwrap();
    }
    assert!(
        nodes[desired[0]]
            .status_snapshot()
            .observation
            .table_metrics_truncated
    );
    let mut old_ready = Vec::new();
    for target in desired {
        common::wait_for(
            || async { nodes[*target].certify_membership_ready(&transition).is_ok() },
            Duration::from_secs(5),
        )
        .await;
        let ready = nodes[*target]
            .certify_membership_ready(&transition)
            .unwrap();
        old_ready.push(ready.clone());
        transition.record_ready(ready).unwrap();
    }
    let mut wrong_registry = transition.joint_authority().unwrap().generations().clone();
    let changed_peer = joint_ids
        .iter()
        .find(|id| id.as_str() != desired[0])
        .unwrap();
    wrong_registry.insert(changed_peer.clone(), 99);
    let mut wrong = MembershipTransition::create(
        directory.path().join("wrong"),
        [10; 32],
        map(old),
        map(desired),
        desired.len(),
    )
    .unwrap();
    wrong
        .record_authority_fence(
            MembershipAuthority::certify_external([10; 32], wrong_registry, [8; 32]).unwrap(),
        )
        .unwrap();
    wrong.publish_joint(&controller).unwrap();
    assert!(nodes[desired[0]]
        .certify_membership_ready(&wrong)
        .unwrap_err()
        .to_string()
        .contains("native receiver policy"));
    assert!(wrong.record_ready(old_ready[0].clone()).is_err());
    let final_ids = desired
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<Vec<_>>();
    let mut shutdowns = HashMap::new();
    for id in old.iter().filter(|id| !desired.contains(id)) {
        shutdowns.insert(
            (*id).to_owned(),
            nodes[*id]
                .finish_shutdown(DrainOptions::default())
                .await
                .unwrap(),
        );
    }
    let mut final_reports = HashMap::new();
    for id in &final_ids {
        nodes[id]
            .begin_peer_fence(policy(id, &final_ids, 3))
            .unwrap();
        assert!(nodes[id].certify_membership_ready(&transition).is_err());
        final_reports.insert(
            id.clone(),
            nodes[id]
                .advance_peer_authority(policy(id, &final_ids, 3), PeerFenceOptions::default())
                .await
                .unwrap(),
        );
    }
    let observations = final_ids
        .iter()
        .map(|id| MembershipFenceObservation {
            node: &nodes[id],
            report: &final_reports[id],
        })
        .collect::<Vec<_>>();
    let retired = shutdowns
        .iter()
        .map(|(id, report)| MembershipShutdownObservation {
            node: &nodes[id],
            report,
        })
        .collect::<Vec<_>>();
    assert!(MembershipAuthority::certify_native_final(
        epoch,
        &transition.joint(),
        transition.desired(),
        &observations,
        &[],
        [2; 32]
    )
    .is_err());
    let final_authority = MembershipAuthority::certify_native_final(
        epoch,
        &transition.joint(),
        transition.desired(),
        &observations,
        &retired,
        [2; 32],
    )
    .unwrap();
    transition
        .record_final_authority_fence(final_authority)
        .unwrap();
    assert_eq!(transition.stage(), MembershipStage::Joint);
    for ready in old_ready {
        assert!(transition.record_ready(ready).is_err());
    }
    for target in desired {
        transition
            .record_ready(
                nodes[*target]
                    .certify_membership_ready(&transition)
                    .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(transition.stage(), MembershipStage::ReadyToRetire);
    transition.publish_final(&controller).unwrap();
    assert_eq!(controller.desired_placement(), map(desired));
    for target in desired {
        for source in old {
            assert_eq!(
                nodes[*target]
                    .get((*source).into())
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                source.as_bytes()
            );
        }
    }
    let reopened = MembershipTransition::reopen(directory.path().join("transition")).unwrap();
    assert!(reopened.final_authority().unwrap().is_native());
    assert_eq!(
        reopened
            .final_authority()
            .unwrap()
            .retired_sdk_shutdowns()
            .len(),
        shutdowns.len()
    );
    // A stopped/restarted target cannot reuse the previous boot's native report.
    let target = desired[0];
    nodes[target].shutdown().await.unwrap();
    let binding = stores[target]
        .bind(&StoreIdentity::new(target.into(), 1))
        .unwrap();
    stores[target].delete_prefix(&[0; 4]).unwrap();
    drop(binding);
    assert!(
        CheckpointReplicaCertificate::collect(&manifests[first], &expected, &observed).is_err()
    );
    let restarted = Node::start_with_options(
        target.into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(stores[target].clone())
            .fragment_map(map(desired))
            .build(),
        NodeRuntimeOptions::default().with_peer_authority(policy(target, &final_ids, 3)),
    )
    .await
    .unwrap();
    let stale = [MembershipFenceObservation {
        node: &restarted,
        report: &final_reports[target],
    }];
    assert!(
        MembershipAuthority::certify_native_joint(epoch, &map(&[target]), &stale, [3; 32]).is_err()
    );
    restarted.shutdown().await.unwrap();
    for (id, node) in &nodes {
        if id != target {
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn native_fixed_count_replacement() {
    workflow(&["a", "b"], &["b", "c"]).await;
}
#[tokio::test]
async fn native_staged_shrink() {
    workflow(&["a", "b", "c"], &["c"]).await;
}

#[tokio::test]
async fn enabling_generation_fence_never_certifies_external_readiness() {
    let directory = tempfile::tempdir().unwrap();
    let epoch = [11; 32];
    let source_store = Arc::new(RocksDbStore::open(directory.path().join("a")).unwrap());
    let source = Node::start(
        "a".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(source_store.clone())
            .build(),
    )
    .await
    .unwrap();
    source.put("key".into(), vec![1]).await.unwrap();
    source.shutdown().await.unwrap();
    let bundle = directory.path().join("bundle");
    let manifest = source
        .export_checkpoint(&bundle, CheckpointOptions::default())
        .await
        .unwrap();
    let target = Arc::new(RocksDbStore::open(directory.path().join("b")).unwrap());
    let receipt =
        CheckpointImporter::open(target.clone(), StoreIdentity::new("b".into(), 1), &bundle)
            .unwrap()
            .finish()
            .unwrap();
    let mut transition = MembershipTransition::create(
        directory.path().join("transition"),
        epoch,
        map(&["a"]),
        map(&["b"]),
        1,
    )
    .unwrap();
    transition.record_source(manifest).unwrap();
    transition.record_import(receipt).unwrap();
    let controller = ManualPlacement::new(map(&["a"]));
    transition.publish_joint(&controller).unwrap();
    let ids = vec!["a".to_owned(), "b".to_owned()];
    let registry = ids.iter().map(|id| (id.clone(), 1)).collect();
    transition
        .record_authority_fence(
            MembershipAuthority::certify_external(epoch, registry, [11; 32]).unwrap(),
        )
        .unwrap();
    let node = Node::start(
        "b".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(target)
            .fragment_map(transition.joint())
            .build(),
    )
    .await
    .unwrap();
    assert!(node.certify_membership_ready(&transition).is_ok());
    let next = policy("b", &ids, 1);
    node.begin_peer_fence(next.clone()).unwrap();
    assert!(node.peer_authority_snapshot().is_none());
    assert_eq!(
        node.peer_fence_status().phase(),
        actordb::node::PeerFencePhase::Fencing
    );
    assert!(node
        .certify_membership_ready(&transition)
        .unwrap_err()
        .to_string()
        .contains("native fencing"));
    node.advance_peer_authority(next, PeerFenceOptions::default())
        .await
        .unwrap();
    assert!(node.certify_membership_ready(&transition).is_ok());
    node.shutdown().await.unwrap();
}
