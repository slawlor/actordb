//! Run with --features rocksdb --example staged_membership -- FRESH_DIRECTORY replace|shrink
//! The application owns external decisions, TLS/PSK revocation and allocation.

#[cfg(feature = "rocksdb")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use actordb::checkpoint::{
        CheckpointImporter, CheckpointOptions, CheckpointReplicaCertificate,
        CheckpointReplicaObservation,
    };
    use actordb::membership::{
        MembershipAuthority, MembershipFenceObservation, MembershipShutdownObservation,
        MembershipTransition,
    };
    use actordb::node::{
        DrainOptions, Node, NodeConfig, NodeRuntimeOptions, PeerAuthorityOptions, PeerFenceOptions,
        RecoveryOptions,
    };
    use actordb::placement::ManualPlacement;
    use actordb::shard::FragmentId;
    use actordb::store::{RocksDbStore, StoreIdentity};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 || !["replace", "shrink"].contains(&args[1].as_str()) {
        println!("usage: staged_membership FRESH_DIRECTORY replace|shrink");
        return Ok(());
    }
    let base = std::path::PathBuf::from(&args[0]);
    std::fs::create_dir_all(&base)?;
    let old = if args[1] == "replace" {
        vec!["a", "b"]
    } else {
        vec!["a", "b", "c"]
    };
    let final_ids = if args[1] == "replace" {
        vec!["b", "c"]
    } else {
        vec!["c"]
    };
    let map = |ids: &[&str]| {
        HashMap::from([(
            FragmentId(0),
            ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>(),
        )])
    };
    let mut epoch = [0; 32];
    getrandom::fill(&mut epoch)?;
    let mut transition = MembershipTransition::create(
        base.join("transition"),
        epoch,
        map(&old),
        map(&final_ids),
        final_ids.len(),
    )?;
    let controller = Arc::new(ManualPlacement::new(map(&old)));
    let mut stores = HashMap::new();
    let mut bundles = HashMap::new();
    let mut manifests = HashMap::new();
    for id in &old {
        let store = Arc::new(RocksDbStore::open(base.join(id))?);
        let source = Node::start(
            (*id).into(),
            "127.0.0.1:0".parse()?,
            NodeConfig::builder()
                .fragment_count(1)
                .store(store.clone())
                .build(),
        )
        .await?;
        source.put((*id).into(), id.as_bytes().to_vec()).await?;
        source.shutdown().await?;
        let bundle = base.join(format!("bundle-{id}"));
        let manifest = source
            .export_checkpoint(&bundle, CheckpointOptions::default())
            .await?;
        transition.record_source(manifest.clone())?;
        stores.insert((*id).to_owned(), store);
        bundles.insert((*id).to_owned(), bundle);
        manifests.insert((*id).to_owned(), manifest);
    }
    transition.publish_joint(&controller)?;
    let mut receipts = Vec::new();
    for target in &final_ids {
        if !stores.contains_key(*target) {
            stores.insert(
                (*target).to_owned(),
                Arc::new(RocksDbStore::open(base.join(target))?),
            );
        }
        let store = stores[*target].clone();
        for source in &old {
            let receipt = CheckpointImporter::open(
                store.clone(),
                StoreIdentity::new((*target).into(), 1),
                &bundles[*source],
            )?
            .finish()?;
            transition.record_import(receipt.clone())?;
            receipts.push(receipt);
        }
    }
    let observations = receipts
        .iter()
        .filter(|receipt| receipt.source().node_id() == old[0])
        .map(|receipt| CheckpointReplicaObservation {
            store: &*stores[receipt.target().node_id()],
            receipt,
        })
        .collect::<Vec<_>>();
    let expected = final_ids
        .iter()
        .map(|id| StoreIdentity::new((*id).into(), 1))
        .collect::<Vec<_>>();
    let copies =
        CheckpointReplicaCertificate::collect(&manifests[old[0]], &expected, &observations)?;
    println!(
        "{} exact stable checkpoint copies verified",
        copies.targets().len()
    );
    let joint_ids = transition.joint()[&FragmentId(0)].clone();
    let policy = |id: &str, ids: &[String], generation| {
        PeerAuthorityOptions::new(
            generation,
            ids.iter()
                .filter(|peer| peer.as_str() != id)
                .map(|peer| (peer.clone(), generation))
                .collect(),
        )
    };
    let mut nodes = HashMap::new();
    for (id, store) in &stores {
        let config = NodeConfig::builder()
            .fragment_count(1)
            .store(store.clone())
            .placement_controller(controller.clone())
            .placement_poll_interval(Duration::from_millis(25))
            .recovery(RecoveryOptions::new(epoch)?)
            .build();
        nodes.insert(
            id.clone(),
            Node::start_with_options(
                id.clone(),
                "127.0.0.1:0".parse()?,
                config,
                NodeRuntimeOptions::default().with_peer_authority(policy(id, &joint_ids, 1)),
            )
            .await?,
        );
    }
    for (index, id) in joint_ids.iter().enumerate() {
        for peer in joint_ids.iter().skip(index + 1) {
            nodes[id].connect(nodes[peer].addr()).await?;
        }
    }
    for node in nodes.values() {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if node.placement_report().await.states.get(&FragmentId(0))
                    == Some(&actordb::placement::FragmentPlacementState::Serving)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await?;
    }
    let mut joint_reports = HashMap::new();
    for id in &joint_ids {
        joint_reports.insert(
            id.clone(),
            nodes[id]
                .advance_peer_authority(policy(id, &joint_ids, 2), PeerFenceOptions::default())
                .await?,
        );
    }
    let reports = joint_ids
        .iter()
        .map(|id| MembershipFenceObservation {
            node: &nodes[id],
            report: &joint_reports[id],
        })
        .collect::<Vec<_>>();
    transition.record_authority_fence(MembershipAuthority::certify_native_joint(
        epoch,
        &transition.joint(),
        &reports,
        [1; 32],
    )?)?;
    let desired = final_ids
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<Vec<_>>();
    let mut shutdowns = HashMap::new();
    for id in &joint_ids {
        if !desired.contains(id) {
            shutdowns.insert(
                id.clone(),
                nodes[id].finish_shutdown(DrainOptions::default()).await?,
            );
        }
    }
    let mut final_reports = HashMap::new();
    for id in &desired {
        final_reports.insert(
            id.clone(),
            nodes[id]
                .advance_peer_authority(policy(id, &desired, 3), PeerFenceOptions::default())
                .await?,
        );
    }
    let reports = desired
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
    transition.record_final_authority_fence(MembershipAuthority::certify_native_final(
        epoch,
        &transition.joint(),
        transition.desired(),
        &reports,
        &retired,
        [2; 32],
    )?)?;
    for id in &desired {
        let ready = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(ready) = nodes[id].certify_membership_ready(&transition) {
                    break ready;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await?;
        transition.record_ready(ready)?;
    }
    transition.publish_final(&controller)?;
    println!(
        "final membership {:?}; every source floor retained",
        desired
    );
    for node in nodes.values() {
        node.shutdown().await?;
    }
    Ok(())
}
#[cfg(not(feature = "rocksdb"))]
fn main() {
    println!("staged_membership requires --features rocksdb");
}
