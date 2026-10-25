#![cfg(feature = "rocksdb")]

mod common;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actordb::actor::VersionedValue;
use actordb::checkpoint::{
    CheckpointExporter, CheckpointImporter, CheckpointManifest, CheckpointOptions,
};
use actordb::membership::{
    MembershipAuthority, MembershipPeerIdentity, MembershipStage, MembershipTransition,
    PermanentSourceLoss,
};
use actordb::node::{Node, NodeConfig, RecoveryOptions, ReplicationDrainOptions};
use actordb::placement::{ManualPlacement, PlacementController};
use actordb::shard::FragmentId;
use actordb::store::{KvStore, RocksDbStore, StoreIdentity};

fn identity(id: &str) -> StoreIdentity {
    StoreIdentity::new(id.into(), 1)
}
fn authority(
    epoch: [u8; 32],
    map: &HashMap<FragmentId, Vec<String>>,
    decision: u8,
) -> MembershipAuthority {
    let identities = map
        .values()
        .flatten()
        .map(|id| {
            let mut key = [0; 32];
            key[..id.len()].copy_from_slice(id.as_bytes());
            (id.clone(), MembershipPeerIdentity::new(1, key).unwrap())
        })
        .collect();
    MembershipAuthority::certify_external(epoch, identities, [decision; 32]).unwrap()
}
fn physical(table: &str, key: &str) -> Vec<u8> {
    let hex = |value: &str| {
        value
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let mut result = vec![0; 4];
    result.extend(
        format!(
            "\u{001d}actordb.table.store.v1:{}:\u{001e}actordb.table.v1:{}:{}",
            hex(table),
            hex(table),
            hex(key)
        )
        .as_bytes(),
    );
    result
}
fn populate(store: &RocksDbStore, id: &str, records: &[(Vec<u8>, VersionedValue)]) {
    let binding = store.bind(&identity(id)).unwrap();
    store.write_batch(records).unwrap();
    store.stable_fence().unwrap();
    drop(binding);
}
fn export(store: Arc<RocksDbStore>, id: &str, directory: &Path) -> CheckpointManifest {
    CheckpointExporter::open(
        store,
        identity(id),
        directory,
        CheckpointOptions::new(2048, 1).unwrap(),
    )
    .unwrap()
    .finish()
    .unwrap()
}

#[test]
fn export_resume_uses_immutable_snapshot_and_import_restart_blocks_serving_until_complete() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source");
    let source = Arc::new(RocksDbStore::open(&source_path).unwrap());
    let mut live = VersionedValue::new(vec![0, 0xff, 0x80], 10, "historical-removed-writer".into());
    live.logical_counter = 99;
    live.expires_at_ms = Some(u64::MAX);
    let mut expired = live.clone();
    expired.expires_at_ms = Some(1);
    expired.is_tombstone = true;
    expired.value.clear();
    let records = vec![
        (physical("KV", "live"), live.clone()),
        (physical("Other", "live"), live.clone()),
        (physical("KV", "dead"), expired.clone()),
    ];
    populate(&source, "old-owner", &records);
    let bundle = directory.path().join("bundle");
    let mut exporter = CheckpointExporter::open(
        source.clone(),
        identity("old-owner"),
        &bundle,
        CheckpointOptions::new(2048, 1).unwrap(),
    )
    .unwrap();
    assert_eq!(exporter.next_chunk().unwrap(), Some(0));
    drop(exporter);
    let mut hashes = fs::read(bundle.join("chunk-hashes")).unwrap();
    hashes.extend([1, 2, 3, 4, 5]);
    fs::write(bundle.join("chunk-hashes"), hashes).unwrap();
    // Later source activity cannot silently change an interrupted export.
    populate(
        &source,
        "old-owner",
        &[(
            physical("KV", "later"),
            VersionedValue::new(vec![9], 500, "new-write".into()),
        )],
    );
    fs::remove_file(bundle.join("snapshot-ready")).unwrap();
    drop(source);
    let source = Arc::new(RocksDbStore::open(&source_path).unwrap());
    // Resuming a durable snapshot needs no new live-source binding.
    let live_source_binding = source.bind(&identity("old-owner")).unwrap();
    let manifest = CheckpointExporter::resume(source, identity("old-owner"), &bundle)
        .unwrap()
        .finish()
        .unwrap();
    drop(live_source_binding);
    assert_eq!(manifest.records(), 3);
    assert_eq!(manifest.chunks(), 3);
    assert!(manifest.remote_replication().is_none());
    for chunk in 0..manifest.chunks() {
        assert!(
            fs::metadata(bundle.join(format!("chunk-{chunk:020}")))
                .unwrap()
                .len()
                <= 2048
        );
    }

    let target_path = directory.path().join("target");
    let target = Arc::new(RocksDbStore::open(&target_path).unwrap());
    let mut importer =
        CheckpointImporter::open(target.clone(), identity("replacement"), &bundle).unwrap();
    assert!(importer.next_chunk().unwrap());
    assert_eq!(importer.progress().unwrap().next_chunk, 1);
    assert!(target.load().is_err());
    assert!(target
        .write_batch(&[(physical("KV", "intruder"), live.clone())])
        .is_err());
    assert!(target.delete_prefix(&[0; 4]).is_err());
    assert!(target.stable_fence().is_err());
    drop(importer);
    assert!(target.bind(&identity("replacement")).is_err());
    assert!(target.load().is_err());
    assert!(target
        .write_batch(&[(physical("KV", "intruder"), live.clone())])
        .is_err());
    assert!(target.delete_prefix(&[0; 4]).is_err());
    drop(target);
    let target = Arc::new(RocksDbStore::open(&target_path).unwrap());
    assert!(target.bind(&identity("replacement")).is_err());
    let importer =
        CheckpointImporter::open(target.clone(), identity("replacement"), &bundle).unwrap();
    assert_eq!(importer.progress().unwrap().next_chunk, 1);
    let receipt = importer.finish().unwrap();
    assert_eq!(receipt.source().node_id(), "old-owner");
    assert_eq!(receipt.target().node_id(), "replacement");
    let binding = target.bind(&identity("replacement")).unwrap();
    let loaded = target
        .load()
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();
    for (key, value) in &records {
        assert_eq!(loaded.get(key), Some(value));
    }
    assert_eq!(loaded.len(), 3);
    drop(binding);
    // Exact completed imports can be retried without rewriting versions.
    assert_eq!(
        CheckpointImporter::open(target.clone(), identity("replacement"), &bundle)
            .unwrap()
            .finish()
            .unwrap(),
        receipt
    );
}

#[test]
fn partial_transfer_advances_verified_chunks_and_blocks_serving_until_finished() {
    let directory = tempfile::tempdir().unwrap();
    let source = Arc::new(RocksDbStore::open(directory.path().join("source")).unwrap());
    populate(
        &source,
        "source",
        &[
            (
                physical("KV", "a"),
                VersionedValue::new(vec![1], 1, "writer".into()),
            ),
            (
                physical("KV", "b"),
                VersionedValue::new(vec![2], 2, "writer".into()),
            ),
        ],
    );
    let bundle = directory.path().join("bundle");
    let manifest = export(source, "source", &bundle);
    let transfer = directory.path().join("transfer");
    fs::create_dir(&transfer).unwrap();
    fs::copy(bundle.join("manifest"), transfer.join("manifest")).unwrap();
    fs::copy(bundle.join("chunk-hashes"), transfer.join("chunk-hashes")).unwrap();
    fs::copy(
        bundle.join("chunk-00000000000000000000"),
        transfer.join("chunk-00000000000000000000"),
    )
    .unwrap();
    let target = Arc::new(RocksDbStore::open(directory.path().join("target")).unwrap());
    let mut importer =
        CheckpointImporter::open(target.clone(), identity("target"), &transfer).unwrap();
    assert!(importer.next_chunk().unwrap());
    assert_eq!(importer.progress().unwrap().next_chunk, 1);
    assert!(importer.next_chunk().is_err()); // Second data chunk has not arrived.
    drop(importer);
    assert!(target.bind(&identity("target")).is_err());
    fs::copy(
        bundle.join("chunk-00000000000000000001"),
        transfer.join("chunk-00000000000000000001"),
    )
    .unwrap();
    let chunk = transfer.join("chunk-00000000000000000001");
    let original = fs::read(&chunk).unwrap();
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&chunk, &corrupt).unwrap();
    let mut importer =
        CheckpointImporter::open(target.clone(), identity("target"), &transfer).unwrap();
    assert!(importer.next_chunk().is_err()); // Bad digest is rejected before apply.
    assert_eq!(importer.progress().unwrap().next_chunk, 1);
    drop(importer);
    assert!(target.bind(&identity("target")).is_err());
    fs::write(chunk, original).unwrap();
    let hash_path = transfer.join("chunk-hashes");
    let original_hashes = fs::read(&hash_path).unwrap();
    let mut bad_hashes = original_hashes.clone();
    bad_hashes[0] ^= 1;
    fs::write(&hash_path, bad_hashes).unwrap();
    let untouched = Arc::new(RocksDbStore::open(directory.path().join("untouched")).unwrap());
    assert!(CheckpointImporter::open(untouched.clone(), identity("untouched"), &transfer).is_err());
    let binding = untouched.bind(&identity("untouched")).unwrap();
    assert!(untouched.load().unwrap().is_empty());
    drop(binding);
    fs::write(hash_path, original_hashes).unwrap();
    assert!(CheckpointImporter::open(
        target.clone(),
        StoreIdentity::new("target".into(), 2),
        &transfer
    )
    .is_err());
    let receipt = CheckpointImporter::open(target.clone(), identity("target"), &transfer)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(receipt.manifest_digest(), &manifest.digest());
    assert!(target.bind(&identity("other-target")).is_err());
}

#[test]
fn union_preserves_tombstone_order_and_equal_version_conflicts_keep_import_incomplete() {
    let directory = tempfile::tempdir().unwrap();
    let mut bundles = Vec::new();
    for (id, tombstone) in [("a", false), ("b", true)] {
        let store = Arc::new(RocksDbStore::open(directory.path().join(id)).unwrap());
        let mut value = VersionedValue::new(vec![1], 10, "same-origin".into());
        value.logical_counter = 8;
        value.expires_at_ms = Some(100);
        value.is_tombstone = tombstone;
        if tombstone {
            value.value.clear();
        }
        populate(&store, id, &[(physical("KV", "key"), value)]);
        let bundle = directory.path().join(format!("bundle-{id}"));
        export(store, id, &bundle);
        bundles.push(bundle);
    }
    let target = Arc::new(RocksDbStore::open(directory.path().join("target")).unwrap());
    for bundle in &bundles {
        CheckpointImporter::open(target.clone(), identity("target"), bundle)
            .unwrap()
            .finish()
            .unwrap();
    }
    let binding = target.bind(&identity("target")).unwrap();
    assert!(target.load().unwrap()[0].1.is_tombstone);
    drop(binding);
    let conflict = Arc::new(RocksDbStore::open(directory.path().join("conflict")).unwrap());
    let mut value = VersionedValue::new(vec![2], 10, "same-origin".into());
    value.logical_counter = 8;
    value.expires_at_ms = Some(100);
    populate(&conflict, "conflict", &[(physical("KV", "key"), value)]);
    let mut importer =
        CheckpointImporter::open(conflict.clone(), identity("conflict"), &bundles[0]).unwrap();
    assert!(importer.next_chunk().is_err());
    assert_eq!(importer.progress().unwrap().next_chunk, 0);
    drop(importer);
    assert!(conflict.bind(&identity("conflict")).is_err());
    assert!(CheckpointImporter::open(conflict, identity("conflict"), &bundles[1]).is_err());
}

#[tokio::test]
async fn imported_historical_origins_survive_new_identity_and_node_export_requires_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let source = Arc::new(RocksDbStore::open(directory.path().join("source")).unwrap());
    let config = NodeConfig::builder()
        .fragment_count(1)
        .store(source)
        .build();
    let node = Node::start("old-writer".into(), "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    node.table("Other")
        .unwrap()
        .put("opaque", vec![0xff, 0, 0x80])
        .await
        .unwrap();
    let bundle = directory.path().join("bundle");
    assert!(node
        .export_checkpoint(&bundle, CheckpointOptions::default())
        .await
        .is_err());
    node.shutdown().await.unwrap();
    let manifest = node
        .export_checkpoint(&bundle, CheckpointOptions::default())
        .await
        .unwrap();
    assert!(manifest.remote_replication().is_none());
    let target = Arc::new(RocksDbStore::open(directory.path().join("new-store")).unwrap());
    let mut importer =
        CheckpointImporter::open(target.clone(), identity("new-writer"), &bundle).unwrap();
    assert!(importer.next_chunk().unwrap());
    drop(importer);
    assert!(Node::start(
        "new-writer".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(target.clone())
            .build()
    )
    .await
    .is_err());
    CheckpointImporter::open(target.clone(), identity("new-writer"), &bundle)
        .unwrap()
        .finish()
        .unwrap();
    let node = Node::start(
        "new-writer".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(target)
            .build(),
    )
    .await
    .unwrap();
    let table = node.table("Other").unwrap();
    let historical = table.get("opaque").await.unwrap().unwrap();
    assert_eq!(historical.node_id, "old-writer");
    assert_eq!(historical.value, vec![0xff, 0, 0x80]);
    table.put("opaque", vec![7]).await.unwrap();
    let current = table.get("opaque").await.unwrap().unwrap();
    assert!(current.wins_over(&historical));
    assert_eq!(current.node_id, "new-writer");
    node.shutdown().await.unwrap();
}

async fn staged_case(old: &[&str], desired: &[&str], epoch: [u8; 32]) {
    let directory = tempfile::tempdir().unwrap();
    let map = |ids: &[&str]| {
        HashMap::from([(
            FragmentId(0),
            ids.iter().map(|id| (*id).into()).collect::<Vec<_>>(),
        )])
    };
    let old_map = map(old);
    let desired_map = map(desired);
    let controller = Arc::new(ManualPlacement::new(old_map.clone()));
    let path = directory.path().join("transition");
    let mut transition =
        MembershipTransition::create(&path, epoch, old_map, desired_map.clone(), desired.len())
            .unwrap();
    let mut stores = HashMap::new();
    let mut bundles = HashMap::new();
    for id in old {
        let store = Arc::new(RocksDbStore::open(directory.path().join(id)).unwrap());
        populate(
            &store,
            id,
            &[(
                physical("KV", id),
                VersionedValue::new(id.as_bytes().to_vec(), 1, format!("origin-{id}")),
            )],
        );
        let bundle = directory.path().join(format!("bundle-{id}"));
        let manifest = export(store.clone(), id, &bundle);
        transition.record_source(manifest).unwrap();
        stores.insert((*id).to_owned(), store);
        bundles.insert((*id).to_owned(), bundle);
    }
    transition.publish_joint(&controller).unwrap();
    assert!(transition.check_retirement().is_err());
    for target in desired {
        let store = stores
            .entry((*target).to_owned())
            .or_insert_with(|| Arc::new(RocksDbStore::open(directory.path().join(target)).unwrap()))
            .clone();
        for source in old {
            let receipt =
                CheckpointImporter::open(store.clone(), identity(target), &bundles[*source])
                    .unwrap()
                    .finish()
                    .unwrap();
            transition.record_import(receipt).unwrap();
        }
    }
    transition
        .record_authority_fence(authority(epoch, &transition.joint(), 99))
        .unwrap();
    drop(transition);
    let mut transition = MembershipTransition::reopen(&path).unwrap();
    transition.publish_joint(&controller).unwrap();
    let mut nodes = HashMap::new();
    for (id, store) in &stores {
        let config = NodeConfig::builder()
            .fragment_count(1)
            .store(store.clone())
            .placement_controller(controller.clone())
            .placement_poll_interval(Duration::from_millis(10))
            .recovery(RecoveryOptions::new(epoch).unwrap())
            .cluster_secret(vec![42; 32])
            .build();
        nodes.insert(
            id.clone(),
            Node::start(id.clone(), "127.0.0.1:0".parse().unwrap(), config)
                .await
                .unwrap(),
        );
    }
    let ids = nodes.keys().cloned().collect::<Vec<_>>();
    for (index, id) in ids.iter().enumerate() {
        for peer in ids.iter().skip(index + 1) {
            nodes[id].connect(nodes[peer].addr()).await.unwrap();
        }
    }
    for target in desired {
        common::wait_for(
            || async { nodes[*target].certify_membership_ready(&transition).is_ok() },
            Duration::from_secs(10),
        )
        .await;
        transition
            .record_ready(
                nodes[*target]
                    .certify_membership_ready(&transition)
                    .unwrap(),
            )
            .unwrap();
    }
    assert!(transition.publish_final(&controller).is_err());
    transition
        .record_final_authority_fence(authority(epoch, transition.desired(), 100))
        .unwrap();
    assert_eq!(transition.stage(), MembershipStage::Joint);
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
    assert_eq!(controller.desired_placement(), desired_map);
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
    let mut resumed = MembershipTransition::reopen(&path).unwrap();
    assert_eq!(resumed.stage(), MembershipStage::Complete);
    let restarted_controller = ManualPlacement::new(resumed.current().clone());
    resumed.publish_final(&restarted_controller).unwrap();
    assert_eq!(restarted_controller.desired_placement(), desired_map);
    for node in nodes.values() {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn fixed_count_replacement_is_staged_and_resumable() {
    staged_case(&["a", "b"], &["b", "c"], [3; 32]).await;
}

#[tokio::test]
async fn staged_shrink_requires_all_old_floors_and_retains_their_union() {
    staged_case(&["a", "b", "c"], &["c"], [4; 32]).await;
}

#[test]
fn missing_source_and_dangerous_reduction_require_separate_explicit_decisions() {
    let directory = tempfile::tempdir().unwrap();
    let old = HashMap::from([(FragmentId(0), vec!["a".into(), "b".into()])]);
    let desired = HashMap::from([(FragmentId(0), vec!["a".into()])]);
    assert!(MembershipTransition::create(
        directory.path().join("bad"),
        [1; 32],
        old.clone(),
        desired.clone(),
        2
    )
    .is_err());
    let mut transition = MembershipTransition::create(
        directory.path().join("good"),
        [1; 32],
        old.clone(),
        desired,
        1,
    )
    .unwrap();
    let controller = ManualPlacement::new(old);
    assert!(transition.publish_final(&controller).is_err());
    let loss = PermanentSourceLoss::authorize([1; 32], vec!["b".into()], [7; 32]).unwrap();
    transition.authorize_permanent_loss(loss).unwrap();
    assert!(transition.publish_joint(&controller).is_err());
    transition
        .record_authority_fence(authority([1; 32], &transition.joint(), 8))
        .unwrap();
    transition.publish_joint(&controller).unwrap();
    assert!(transition.publish_final(&controller).is_err());
    assert_eq!(
        transition
            .missing_source_authorization()
            .unwrap()
            .missing_sources()
            .len(),
        1
    );
}

#[tokio::test]
async fn explicit_permanent_source_loss_recovers_only_qualified_floor_without_fake_handoff() {
    let directory = tempfile::tempdir().unwrap();
    let old = HashMap::from([(FragmentId(0), vec!["survivor".into(), "failed".into()])]);
    let desired = HashMap::from([(FragmentId(0), vec!["survivor".into()])]);
    let controller = Arc::new(ManualPlacement::new(old.clone()));
    let epoch = [6; 32];
    let mut transition =
        MembershipTransition::create(directory.path().join("transition"), epoch, old, desired, 1)
            .unwrap();
    let store = Arc::new(RocksDbStore::open(directory.path().join("survivor")).unwrap());
    populate(
        &store,
        "survivor",
        &[(
            physical("KV", "surviving-floor"),
            VersionedValue::new(vec![8], 10, "historical-origin".into()),
        )],
    );
    let bundle = directory.path().join("bundle");
    transition
        .record_source(export(store.clone(), "survivor", &bundle))
        .unwrap();
    transition
        .authorize_permanent_loss(
            PermanentSourceLoss::authorize(epoch, vec!["failed".into()], [17; 32]).unwrap(),
        )
        .unwrap();
    transition
        .record_authority_fence(authority(epoch, &transition.joint(), 18))
        .unwrap();
    transition.publish_joint(&controller).unwrap();
    transition
        .record_import(
            CheckpointImporter::open(store.clone(), identity("survivor"), &bundle)
                .unwrap()
                .finish()
                .unwrap(),
        )
        .unwrap();
    let node = Node::start(
        "survivor".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .store(store)
            .placement_controller(controller.clone())
            .recovery(RecoveryOptions::new(epoch).unwrap())
            .build(),
    )
    .await
    .unwrap();
    common::wait_for(
        || async { node.certify_membership_ready(&transition).is_ok() },
        Duration::from_secs(5),
    )
    .await;
    transition
        .record_ready(node.certify_membership_ready(&transition).unwrap())
        .unwrap();
    assert!(transition.publish_final(&controller).is_err());
    transition
        .record_final_authority_fence(authority(epoch, transition.desired(), 19))
        .unwrap();
    transition
        .record_ready(node.certify_membership_ready(&transition).unwrap())
        .unwrap();
    transition.publish_final(&controller).unwrap();
    assert_eq!(transition.import_receipts().count(), 1);
    assert!(transition
        .import_receipts()
        .all(|receipt| receipt.source().node_id() != "failed"));
    assert_eq!(
        node.get("surviving-floor".into())
            .await
            .unwrap()
            .unwrap()
            .node_id,
        "historical-origin"
    );
    node.shutdown().await.unwrap();
}

#[test]
fn retained_floor_evidence_is_invalidated_by_prefix_retirement_and_reimport_restores_it() {
    let directory = tempfile::tempdir().unwrap();
    let source = Arc::new(RocksDbStore::open(directory.path().join("source")).unwrap());
    populate(
        &source,
        "source",
        &[(
            physical("KV", "floor"),
            VersionedValue::new(vec![1], 10, "historical".into()),
        )],
    );
    let bundle = directory.path().join("bundle");
    export(source, "source", &bundle);
    let target = Arc::new(RocksDbStore::open(directory.path().join("target")).unwrap());
    let receipt = CheckpointImporter::open(target.clone(), identity("target"), &bundle)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(
        target
            .checkpoint_backend()
            .unwrap()
            .import_evidence(receipt.manifest_digest())
            .unwrap()
            .as_ref(),
        Some(&receipt)
    );
    let binding = target.bind(&identity("target")).unwrap();
    target.delete_prefix(&[0; 4]).unwrap();
    drop(binding);
    assert!(target
        .checkpoint_backend()
        .unwrap()
        .import_evidence(receipt.manifest_digest())
        .unwrap()
        .is_none());
    let restored = CheckpointImporter::open(target.clone(), identity("target"), &bundle)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(restored, receipt);
    assert_eq!(
        target
            .checkpoint_backend()
            .unwrap()
            .import_evidence(receipt.manifest_digest())
            .unwrap()
            .as_ref(),
        Some(&receipt)
    );
}

#[test]
fn oversized_records_and_changed_export_limits_are_rejected_without_oversized_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let source = Arc::new(RocksDbStore::open(directory.path().join("source")).unwrap());
    populate(
        &source,
        "source",
        &[(
            physical("KV", "large"),
            VersionedValue::new(vec![1; 4096], 10, "writer".into()),
        )],
    );
    let bundle = directory.path().join("small");
    let mut exporter = CheckpointExporter::open(
        source.clone(),
        identity("source"),
        &bundle,
        CheckpointOptions::new(2048, 1).unwrap(),
    )
    .unwrap();
    assert!(exporter.next_chunk().is_err());
    drop(exporter);
    assert!(!bundle.join("manifest").exists());
    assert!(CheckpointExporter::open(
        source.clone(),
        identity("source"),
        &bundle,
        CheckpointOptions::new(8192, 1).unwrap()
    )
    .is_err());
    let result = CheckpointExporter::open(
        source,
        identity("source"),
        directory.path().join("large"),
        CheckpointOptions::new(8192, 1).unwrap(),
    )
    .unwrap()
    .finish()
    .unwrap();
    assert_eq!(result.records(), 1);
}

#[tokio::test]
async fn export_distinguishes_local_stable_copy_from_exact_remote_ack_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let map = HashMap::from([(FragmentId(0), vec!["source".into(), "replica".into()])]);
    let source_store = Arc::new(RocksDbStore::open(directory.path().join("source")).unwrap());
    let replica_store = Arc::new(RocksDbStore::open(directory.path().join("replica")).unwrap());
    let source = Node::start(
        "source".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .fragment_map(map.clone())
            .store(source_store)
            .build(),
    )
    .await
    .unwrap();
    let replica = Node::start(
        "replica".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_count(1)
            .fragment_map(map)
            .store(replica_store)
            .build(),
    )
    .await
    .unwrap();
    source.connect(replica.addr()).await.unwrap();
    common::wait_for(
        || async { source.peer_count().await == 1 && replica.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    source.put("fenced".into(), vec![0xff, 0]).await.unwrap();
    let report = source
        .shutdown_with_replication_drain(
            ReplicationDrainOptions::default().with_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    let bundle = directory.path().join("bundle");
    let manifest = source
        .export_checkpoint(&bundle, CheckpointOptions::default())
        .await
        .unwrap();
    assert_eq!(
        manifest.local_durability(),
        actordb::store::StoreCommitDurability::Stable
    );
    let remote = manifest.remote_replication().unwrap();
    assert_eq!(remote.durability(), report.durability());
    assert_eq!(remote.source_id(), source.id());
    assert_eq!(remote.targets().len(), 1);
    assert_eq!(remote.targets()[0].1, replica.id());
    assert_eq!(CheckpointManifest::read(&bundle).unwrap(), manifest);
    replica.shutdown().await.unwrap();
}
