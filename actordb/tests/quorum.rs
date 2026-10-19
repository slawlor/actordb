#![cfg(feature = "quorum")]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::network::{
    AcceptFuture, Acceptor, BindFuture, ConnectFuture, Network, NetworkError, TcpNetwork,
};
use actordb::quorum::{
    ChangeKind, Expectation, FileQuorumStore, Mutation, QuorumConfig, QuorumError, QuorumNode,
    QuorumStore,
};
use actordb::shard::FragmentId;
use prost::Message;

fn now() -> u64 {
    actordb::actor::now_ms()
}

fn mutation(id: &str, key: &str, value: Option<Vec<u8>>, expectation: Expectation) -> Mutation {
    Mutation::builder()
        .request_id(id)
        .valid_until_ms(now() + 30_000)
        .key(key)
        .expectation(expectation)
        .maybe_value(value)
        .build()
}

async fn mutate_retry(
    node: &QuorumNode,
    request: Mutation,
) -> Result<actordb::quorum::MutationResult, QuorumError> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match node.mutate(request.clone()).await {
                Err(QuorumError::UnknownOutcome | QuorumError::Unavailable) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => return result,
            }
        }
    })
    .await
    .unwrap()
}

#[derive(Default)]
struct PartitionNetwork {
    blocked: Arc<AtomicBool>,
}

struct PartitionAcceptor {
    inner: Box<dyn Acceptor>,
    blocked: Arc<AtomicBool>,
}
impl Acceptor for PartitionAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            loop {
                let stream = self.inner.accept().await?;
                if !self.blocked.load(Ordering::Acquire) {
                    return Ok(stream);
                }
                drop(stream);
            }
        })
    }
}
impl Network for PartitionNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let (addr, inner) = TcpNetwork.bind(addr).await?;
            Ok((
                addr,
                Box::new(PartitionAcceptor {
                    inner,
                    blocked: self.blocked.clone(),
                }) as Box<dyn Acceptor>,
            ))
        })
    }
    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            if self.blocked.load(Ordering::Acquire) {
                return Err(NetworkError("partition".into()));
            }
            TcpNetwork.connect(addr).await
        })
    }
}

struct Cluster {
    nodes: Vec<QuorumNode>,
    stores: Vec<Arc<FileQuorumStore>>,
    directories: Vec<tempfile::TempDir>,
    configs: Vec<QuorumConfig>,
    networks: Vec<Arc<PartitionNetwork>>,
}

impl Cluster {
    async fn start(count: usize, voters: usize) -> Self {
        Self::start_with_policy(count, voters, |_| {}).await
    }

    async fn start_with_policy(
        count: usize,
        voters: usize,
        configure: impl Fn(&mut QuorumConfig),
    ) -> Self {
        let listeners: Vec<_> = (0..count)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let members: HashMap<_, _> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (format!("n{i}"), l.local_addr().unwrap()))
            .collect();
        let placement = HashMap::from([(
            FragmentId(0),
            (0..voters).map(|i| format!("n{i}")).collect(),
        )]);
        let directories: Vec<_> = (0..count).map(|_| tempfile::tempdir().unwrap()).collect();
        let stores: Vec<_> = directories
            .iter()
            .map(|d| Arc::new(FileQuorumStore::open(d.path()).unwrap()))
            .collect();
        let networks: Vec<_> = (0..count)
            .map(|_| Arc::new(PartitionNetwork::default()))
            .collect();
        let mut configs: Vec<_> = (0..count)
            .map(|i| {
                QuorumConfig::builder()
                    .cluster_id("integration")
                    .store(stores[i].clone())
                    .fragment_map(placement.clone())
                    .members(members.clone())
                    .network(networks[i].clone())
                    .cluster_secret(vec![7; 32])
                    .rpc_timeout(Duration::from_millis(500))
                    .operation_timeout(Duration::from_secs(10))
                    .maintenance_interval(Duration::from_millis(25))
                    .build()
            })
            .collect();
        for config in &mut configs {
            configure(config);
        }
        drop(listeners);
        let mut nodes = Vec::new();
        for (i, config) in configs.iter().enumerate() {
            nodes.push(
                QuorumNode::start(format!("n{i}"), members[&format!("n{i}")], config.clone())
                    .await
                    .unwrap(),
            );
        }
        Self {
            nodes,
            stores,
            directories,
            configs,
            networks,
        }
    }

    async fn shutdown(&self) {
        for node in &self.nodes {
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_writes_serialize_across_nodes_and_replay_exact_results() {
    let cluster = Cluster::start(3, 3).await;
    let initial = cluster.nodes[0]
        .mutate(mutation(
            "initial",
            "key",
            Some(vec![0]),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    let revision = initial.current.unwrap().revision;
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..6 {
        let node = cluster.nodes[i % 3].clone();
        tasks.spawn(async move {
            mutate_retry(
                &node,
                mutation(
                    &format!("contender-{i}"),
                    "key",
                    Some(vec![i as u8 + 1]),
                    Expectation::Revision(revision),
                ),
            )
            .await
            .unwrap()
        });
    }
    let mut winners = 0;
    while let Some(result) = tasks.join_next().await {
        winners += usize::from(result.unwrap().applied);
    }
    assert_eq!(winners, 1);

    let request = mutation(
        "replayed",
        "other",
        Some(b"original".to_vec()),
        Expectation::Absent,
    );
    let first = cluster.nodes[1].mutate(request.clone()).await.unwrap();
    cluster.nodes[2]
        .mutate(mutation(
            "overwrite",
            "other",
            Some(b"later".to_vec()),
            Expectation::Any,
        ))
        .await
        .unwrap();
    let replay = cluster.nodes[2].mutate(request.clone()).await.unwrap();
    assert_eq!(replay.revision, first.revision);
    assert_eq!(replay.current, first.current);
    assert!(replay.applied);
    let mut conflicting = request;
    conflicting.value = Some(b"different".to_vec());
    assert_eq!(
        cluster.nodes[0].mutate(conflicting).await.unwrap_err(),
        QuorumError::RequestConflict
    );
    assert_eq!(
        cluster.nodes[1]
            .get("other".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        b"later"
    );
    let changes = cluster.nodes[0]
        .changes(FragmentId(0), 0, 100)
        .await
        .unwrap();
    assert_eq!(
        changes.changes.iter().filter(|c| c.key == "other").count(),
        2
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_writer_cannot_commit_and_failover_preserves_acknowledged_state() {
    let cluster = Cluster::start(3, 3).await;
    let first = cluster.nodes[0]
        .mutate(mutation(
            "first",
            "key",
            Some(b"first".to_vec()),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    cluster.networks[0].blocked.store(true, Ordering::Release);
    let second = cluster.nodes[1]
        .mutate(mutation(
            "second",
            "key",
            Some(b"second".to_vec()),
            Expectation::Revision(first.current.unwrap().revision),
        ))
        .await
        .unwrap();
    assert!(second.applied);
    assert!(second.commit.acknowledgements.len() >= 2);
    let stale = cluster.nodes[0]
        .mutate(mutation(
            "stale",
            "key",
            Some(b"stale".to_vec()),
            Expectation::Any,
        ))
        .await;
    assert!(matches!(
        stale,
        Err(QuorumError::Unavailable | QuorumError::UnknownOutcome)
    ));
    cluster.networks[0].blocked.store(false, Ordering::Release);
    assert_eq!(
        cluster.nodes[0]
            .get("key".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        b"second"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn certified_deletion_reclaims_keys_and_rejects_restored_stale_state() {
    let mut cluster = Cluster::start(3, 3).await;
    let created = cluster.nodes[0]
        .mutate(mutation(
            "create",
            "key",
            Some(b"old".to_vec()),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    let old_file = std::fs::read(cluster.directories[0].path().join("fragment-0")).unwrap();
    cluster.nodes[0].shutdown().await.unwrap();
    cluster.nodes[1]
        .mutate(mutation(
            "delete",
            "key",
            None,
            Expectation::Revision(created.current.unwrap().revision),
        ))
        .await
        .unwrap();
    let persisted = actordb_protocol::quorum::ReplicaState::decode(
        cluster.stores[1]
            .load(FragmentId(0))
            .unwrap()
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    assert!(persisted
        .committed
        .unwrap()
        .snapshot
        .unwrap()
        .entries
        .is_empty());
    std::fs::write(cluster.directories[0].path().join("fragment-0"), old_file).unwrap();
    cluster.nodes[0] = QuorumNode::start(
        "n0".into(),
        cluster.nodes[0].addr(),
        cluster.configs[0].clone(),
    )
    .await
    .unwrap();
    assert!(cluster.nodes[0].get("key".into()).await.unwrap().is_none());
    let recreated = cluster.nodes[2]
        .mutate(mutation(
            "recreate",
            "key",
            Some(b"new".to_vec()),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    let old_revision = created.revision;
    assert_ne!(recreated.current.unwrap().revision, old_revision);
    let stale_cas = cluster.nodes[0]
        .mutate(mutation(
            "old-cas",
            "key",
            Some(b"bad".to_vec()),
            Expectation::Revision(old_revision),
        ))
        .await
        .unwrap();
    assert!(!stale_cas.applied);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joint_reconfiguration_transfers_authority_to_disjoint_voters() {
    let cluster = Cluster::start(6, 3).await;
    cluster.nodes[0]
        .mutate(mutation(
            "before",
            "key",
            Some(b"before".to_vec()),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    let commit = cluster.nodes[0]
        .reconfigure(FragmentId(0), vec!["n3".into(), "n4".into(), "n5".into()])
        .await
        .unwrap();
    // A retried final step may return a later barrier under the new set.
    if commit.voting_membership.voters == vec!["n0", "n1", "n2"] {
        assert_eq!(commit.voting_membership.next_voters, vec!["n3", "n4", "n5"]);
    } else {
        assert_eq!(commit.voting_membership.voters, vec!["n3", "n4", "n5"]);
        assert!(commit.voting_membership.next_voters.is_empty());
    }
    for i in 0..3 {
        cluster.nodes[i].shutdown().await.unwrap();
    }
    assert_eq!(
        cluster.nodes[3]
            .get("key".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        b"before"
    );
    let after = cluster.nodes[4]
        .mutate(mutation(
            "after",
            "key",
            Some(b"after".to_vec()),
            Expectation::Any,
        ))
        .await
        .unwrap();
    assert_eq!(
        after.commit.voting_membership.voters,
        vec!["n3", "n4", "n5"]
    );
    assert!(after.commit.voting_membership.next_voters.is_empty());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_expiry_is_durable_and_change_cursors_report_retention_gaps() {
    let cluster = Cluster::start(3, 3).await;
    let mut request = mutation(
        "expiring",
        "key",
        Some(b"value".to_vec()),
        Expectation::Absent,
    );
    request.expires_at_ms = Some(now() + 1_500);
    cluster.nodes[0].mutate(request).await.unwrap();
    // Observe storage only: no read/mutation request drives the expiry.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted = actordb_protocol::quorum::ReplicaState::decode(
                cluster.stores[1]
                    .load(FragmentId(0))
                    .unwrap()
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let snapshot = persisted.committed.unwrap().snapshot.unwrap();
            if snapshot
                .changes
                .iter()
                .any(|c| c.kind == ChangeKind::Expire as i32)
            {
                assert!(snapshot.entries.is_empty());
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let page = cluster.nodes[2]
        .changes(FragmentId(0), 0, 10)
        .await
        .unwrap();
    assert_eq!(
        page.changes.iter().map(|c| c.kind).collect::<Vec<_>>(),
        vec![ChangeKind::Put as i32, ChangeKind::Expire as i32]
    );
    cluster.shutdown().await;
}

struct FailLearnStore {
    inner: Arc<FileQuorumStore>,
    fail: AtomicBool,
    final_only: bool,
}

impl QuorumStore for FailLearnStore {
    fn bind(
        &self,
        manifest: &actordb::quorum::Manifest,
    ) -> Result<actordb::store::StoreBinding, QuorumError> {
        self.inner.bind(manifest)
    }
    fn load(&self, fragment: FragmentId) -> Result<Option<Vec<u8>>, QuorumError> {
        self.inner.load(fragment)
    }
    fn save(&self, fragment: FragmentId, bytes: &[u8]) -> Result<(), QuorumError> {
        let state = actordb_protocol::quorum::ReplicaState::decode(bytes).unwrap();
        let membership = state
            .committed
            .as_ref()
            .unwrap()
            .snapshot
            .as_ref()
            .unwrap()
            .membership
            .as_ref()
            .unwrap();
        let eligible = !self.final_only
            || (membership.voters == vec!["n3", "n4", "n5"] && membership.next_voters.is_empty());
        if eligible
            && state.accepted.is_none()
            && state
                .committed
                .as_ref()
                .unwrap()
                .snapshot
                .as_ref()
                .unwrap()
                .slot
                > 0
            && self.fail.swap(false, Ordering::AcqRel)
        {
            return Err(QuorumError::Storage(
                "injected crash before learning chosen state".into(),
            ));
        }
        self.inner.save(fragment, bytes)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_adopts_a_quorum_accepted_operation_after_the_writer_loses_its_result() {
    let mut cluster = Cluster::start(3, 3).await;
    cluster.nodes[0].shutdown().await.unwrap();
    cluster.configs[0].store = Arc::new(FailLearnStore {
        inner: cluster.stores[0].clone(),
        fail: AtomicBool::new(true),
        final_only: false,
    });
    cluster.nodes[0] = QuorumNode::start(
        "n0".into(),
        cluster.nodes[0].addr(),
        cluster.configs[0].clone(),
    )
    .await
    .unwrap();
    let request = mutation(
        "interrupted",
        "key",
        Some(b"once".to_vec()),
        Expectation::Absent,
    );
    assert!(matches!(
        cluster.nodes[0].mutate(request.clone()).await,
        Err(QuorumError::Storage(_))
    ));
    let recovered = cluster.nodes[1].mutate(request.clone()).await.unwrap();
    assert!(recovered.applied);
    assert_eq!(recovered.revision, 1);
    assert_eq!(recovered.current.as_ref().unwrap().value, b"once");
    cluster.nodes[0].shutdown().await.unwrap();
    cluster.nodes[0] = QuorumNode::start(
        "n0".into(),
        cluster.nodes[0].addr(),
        cluster.configs[0].clone(),
    )
    .await
    .unwrap();
    let replay = cluster.nodes[0].mutate(request).await.unwrap();
    assert_eq!(replay.revision, recovered.revision);
    assert_eq!(replay.current, recovered.current);
    assert_eq!(
        cluster.nodes[2]
            .changes(FragmentId(0), 0, 10)
            .await
            .unwrap()
            .changes
            .len(),
        1
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_limits_expired_retries_and_event_retention_are_explicit() {
    let cluster = Cluster::start_with_policy(1, 1, |config| {
        config.max_receipts = 2;
        config.max_changes = 1;
        config.max_records = 2;
    })
    .await;
    let deadline = now() + 2_000;
    let mut first = mutation("first", "one", Some(vec![1]), Expectation::Absent);
    first.valid_until_ms = deadline;
    let mut second = mutation("second", "two", Some(vec![2]), Expectation::Absent);
    second.valid_until_ms = deadline;
    cluster.nodes[0].mutate(first.clone()).await.unwrap();
    cluster.nodes[0].mutate(second).await.unwrap();
    let view = cluster.nodes[0].snapshot(FragmentId(0)).await.unwrap();
    assert_eq!(
        view.records
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "two"]
    );
    assert_eq!(view.last_change_id, 2);
    assert!(cluster.nodes[0]
        .changes(FragmentId(0), view.last_change_id, 1)
        .await
        .unwrap()
        .changes
        .is_empty());
    assert_eq!(
        cluster.nodes[0]
            .changes(FragmentId(0), 0, 1)
            .await
            .unwrap_err(),
        QuorumError::CursorExpired
    );
    assert!(matches!(
        cluster.nodes[0]
            .mutate(mutation("third", "one", Some(vec![3]), Expectation::Any))
            .await,
        Err(QuorumError::ResourceLimit(_))
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted = actordb_protocol::quorum::ReplicaState::decode(
                cluster.stores[0]
                    .load(FragmentId(0))
                    .unwrap()
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            if persisted
                .committed
                .unwrap()
                .snapshot
                .unwrap()
                .receipts
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        cluster.nodes[0].mutate(first).await.unwrap_err(),
        QuorumError::RequestExpired
    );
    assert!(
        cluster.nodes[0]
            .mutate(mutation("fourth", "one", Some(vec![4]), Expectation::Any))
            .await
            .unwrap()
            .applied
    );
    assert!(matches!(
        cluster.nodes[0]
            .mutate(mutation(
                "fifth",
                "three",
                Some(vec![5]),
                Expectation::Absent
            ))
            .await,
        Err(QuorumError::ResourceLimit(_))
    ));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconfiguration_requires_the_new_quorum_to_persist_the_decision_certificate() {
    let mut cluster = Cluster::start(6, 3).await;
    for index in [3, 4] {
        cluster.nodes[index].shutdown().await.unwrap();
        cluster.configs[index].store = Arc::new(FailLearnStore {
            inner: cluster.stores[index].clone(),
            fail: AtomicBool::new(true),
            final_only: true,
        });
        cluster.nodes[index] = QuorumNode::start(
            format!("n{index}"),
            cluster.nodes[index].addr(),
            cluster.configs[index].clone(),
        )
        .await
        .unwrap();
    }
    let target = vec!["n3".into(), "n4".into(), "n5".into()];
    assert!(cluster.nodes[0]
        .reconfigure(FragmentId(0), target.clone())
        .await
        .is_err());
    for index in [3, 4] {
        cluster.nodes[index].shutdown().await.unwrap();
        cluster.nodes[index] = QuorumNode::start(
            format!("n{index}"),
            cluster.nodes[index].addr(),
            cluster.configs[index].clone(),
        )
        .await
        .unwrap();
    }
    cluster.nodes[0]
        .reconfigure(FragmentId(0), target)
        .await
        .unwrap();
    for index in 0..3 {
        cluster.nodes[index].shutdown().await.unwrap();
    }
    assert!(
        cluster.nodes[3]
            .mutate(mutation(
                "after-transition",
                "key",
                Some(vec![1]),
                Expectation::Absent
            ))
            .await
            .unwrap()
            .applied
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn missing_initialized_state_fails_before_binding_a_listener() {
    let cluster = Cluster::start(1, 1).await;
    let address = cluster.nodes[0].addr();
    cluster.nodes[0].shutdown().await.unwrap();
    std::fs::remove_file(cluster.directories[0].path().join("fragment-0")).unwrap();
    let result = QuorumNode::start("n0".into(), address, cluster.configs[0].clone()).await;
    assert!(matches!(result, Err(QuorumError::Storage(_))));
    let _listener = TcpListener::bind(address).unwrap();
}

#[tokio::test]
async fn snapshot_byte_pressure_preserves_reads_and_background_reclamation() {
    let cluster = Cluster::start_with_policy(1, 1, |config| config.max_state_bytes = 4096).await;
    let expires = now() + 2_000;
    let mut inserted = 0;
    for index in 0..20 {
        let mut request = mutation(
            &format!("sized-{index}"),
            &format!("key-{index}"),
            Some(vec![1; 200]),
            Expectation::Absent,
        );
        request.expires_at_ms = Some(expires);
        match cluster.nodes[0].mutate(request).await {
            Ok(result) => {
                assert!(result.applied);
                inserted += 1;
            }
            Err(QuorumError::ResourceLimit(_)) => break,
            result => panic!("unexpected size outcome: {result:?}"),
        }
    }
    assert!(inserted > 0 && inserted < 20);
    assert_eq!(
        cluster.nodes[0]
            .get("key-0".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        vec![1; 200]
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted = actordb_protocol::quorum::ReplicaState::decode(
                cluster.stores[0]
                    .load(FragmentId(0))
                    .unwrap()
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            if persisted
                .committed
                .unwrap()
                .snapshot
                .unwrap()
                .entries
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(cluster.nodes[0]
        .get("key-0".into())
        .await
        .unwrap()
        .is_none());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fragments_isolate_request_ids_and_reject_files_from_other_namespaces() {
    let cluster = Cluster::start_with_policy(3, 3, |config| {
        config
            .fragment_map
            .insert(FragmentId(1), vec!["n0".into(), "n1".into(), "n2".into()]);
    })
    .await;
    let keys: Vec<_> = (0..2)
        .map(|fragment| {
            (0..100)
                .map(|index| format!("key-{index}"))
                .find(|key| actordb::shard::fragment_of(key, 2).0 == fragment)
                .unwrap()
        })
        .collect();
    for (index, key) in keys.iter().enumerate() {
        assert!(
            cluster.nodes[2]
                .mutate(mutation(
                    "same-id",
                    key,
                    Some(vec![index as u8]),
                    Expectation::Absent
                ))
                .await
                .unwrap()
                .applied
        );
    }
    for (index, key) in keys.iter().enumerate() {
        let snapshot = cluster.nodes[1]
            .snapshot(FragmentId(index as u32))
            .await
            .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].0, *key);
        assert_eq!(snapshot.records[0].1.value, vec![index as u8]);
    }
    let zero = std::fs::read(cluster.directories[0].path().join("fragment-0")).unwrap();
    cluster.nodes[0].shutdown().await.unwrap();
    std::fs::write(cluster.directories[0].path().join("fragment-1"), zero).unwrap();
    assert!(matches!(
        QuorumNode::start(
            "n0".into(),
            cluster.nodes[0].addr(),
            cluster.configs[0].clone()
        )
        .await,
        Err(QuorumError::Storage(_))
    ));
    cluster.shutdown().await;

    let local = Cluster::start(1, 1).await;
    let foreign =
        Cluster::start_with_policy(1, 1, |config| config.cluster_id = "foreign".into()).await;
    foreign.nodes[0]
        .mutate(mutation(
            "foreign",
            "key",
            Some(vec![1]),
            Expectation::Absent,
        ))
        .await
        .unwrap();
    let bytes = std::fs::read(foreign.directories[0].path().join("fragment-0")).unwrap();
    local.nodes[0].shutdown().await.unwrap();
    std::fs::write(local.directories[0].path().join("fragment-0"), bytes).unwrap();
    assert!(matches!(
        QuorumNode::start("n0".into(), local.nodes[0].addr(), local.configs[0].clone()).await,
        Err(QuorumError::Storage(_))
    ));
    foreign.shutdown().await;
    local.shutdown().await;
}
