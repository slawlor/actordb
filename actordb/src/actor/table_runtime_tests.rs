use super::*;
use ractor::{rpc::CallResult, Actor, ActorRef, ActorStatus};
use std::sync::Arc;
use std::time::Duration;

fn key(table: &str, key: &str) -> String {
    crate::table_key::encode_table_key(table, key)
}
pub(super) async fn actor(
    capacity: usize,
    label: &str,
) -> (Arc<FragmentState>, ActorRef<KvMessage>) {
    let shared = Arc::new(
        FragmentState::new(Default::default(), None, 100, Duration::from_secs(3600))
            .with_table_pool_capacity(capacity)
            .unwrap()
            .enable_partitions()
            .unwrap(),
    );
    let pool = PoolConfig::builder().workers(1).queue_cap(10).build();
    let (actor, _) = KvActor::spawn(
        None,
        KvActor,
        KvArgs::builder()
            .shared(shared.clone())
            .fragment(crate::shard::FragmentId(0))
            .worker_prefix(label)
            .default_pool(pool.clone())
            .high_pool(pool.clone())
            .background_pool(pool)
            .build(),
    )
    .await
    .unwrap();
    (shared, actor)
}
pub(super) async fn apply(
    actor: &ActorRef<KvMessage>,
    table: &str,
    version: u64,
    replication: bool,
    writer: &str,
) -> Result<ApplyOutcome, ApplyError> {
    let op = ReplicatedOp {
        key: key(table, "one"),
        value: VersionedValue::new(vec![version as u8], version, writer.into()),
        seq: version,
        boot_id: 1,
    };
    match actor
        .call(
            |reply| KvMessage::ApplyOp {
                op,
                priority: OpPriority::Default,
                is_replication: replication,
                reply: Some(reply),
            },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    {
        CallResult::Success(result) => result,
        _ => panic!("apply did not reply"),
    }
}
async fn apply_ready(actor: &ActorRef<KvMessage>, table: &str, version: u64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match apply(actor, table, version, false, "writer").await {
                Ok(_) => break,
                Err(ApplyError::Backpressure) => tokio::task::yield_now().await,
                other => panic!("unexpected apply {other:?}"),
            }
        }
    })
    .await
    .unwrap();
}
fn query(table: &str) -> crate::query::QueryPageWork {
    crate::query::QueryPageWork::new(
        Arc::new(actordb_query::parse(&format!("SELECT COUNT(*) FROM {table}")).unwrap()),
        crate::proto::QueryPageRequest {
            max_rows: 256,
            max_visits: 256,
            max_bytes: 4096,
            ..Default::default()
        },
    )
}
async fn page(
    actor: &ActorRef<KvMessage>,
    work: crate::query::QueryPageWork,
) -> crate::proto::QueryPageResponse {
    match actor
        .call(
            |reply| KvMessage::QueryPage { work, reply },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    {
        CallResult::Success(page) => page,
        _ => panic!("query did not reply"),
    }
}
pub(super) async fn stop(actor: ActorRef<KvMessage>) {
    actor.stop(None);
    actor.wait(None).await.unwrap();
}

#[tokio::test]
async fn empty_scalar_batch_scan_query_and_versioned_reads_allocate_no_named_runtime() {
    let (shared, actor) = actor(2, "empty-table-cache").await;
    for index in 0..32 {
        let name = format!("MISSING_{index}");
        let k = key(&name, "one");
        assert!(matches!(
            actor
                .call(
                    |reply| KvMessage::Get {
                        key: k.clone(),
                        priority: OpPriority::Default,
                        reply
                    },
                    None
                )
                .await
                .unwrap(),
            CallResult::Success(Ok(None))
        ));
        let batch = actor
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec![k.clone(), k.clone()],
                    priority: OpPriority::Default,
                    response_base_payload_len: 0,
                    response_soft_limit: 65536,
                    reply,
                },
                None,
            )
            .await
            .unwrap();
        match batch {
            CallResult::Success(batch) => assert!(batch
                .entries
                .iter()
                .all(|entry| matches!(entry.result, Ok(None)))),
            _ => panic!("batch did not reply"),
        }
        let spec = ScanSpec {
            matcher: KeyMatcher::Prefix(crate::table_key::encode_table_prefix(&name, b"")),
            transform: ValueTransform::KeyOnly,
            limit: 10,
            offset: 0,
        };
        assert!(
            matches!(actor.call(|reply| KvMessage::Scan {spec, reply}, None).await.unwrap(), CallResult::Success(Ok(rows)) if rows.is_empty())
        );
        let page = page(&actor, query(&name)).await;
        assert_eq!(page.status, crate::proto::QueryPageStatus::Ok as i32);
        assert_eq!(
            crate::query::cell_to_value(page.rows[0].cells[0].clone()).unwrap(),
            actordb_query::QueryValue::Integer(0)
        );
        assert!(shared.get_versioned(&k, None).await.is_none());
    }
    assert_eq!(
        shared.cached_table_count().await,
        1,
        "only built-in KV remains"
    );
    assert_eq!(
        actor.get_children().len(),
        6,
        "fixed base + KV workers only"
    );
    stop(actor).await;
}

#[tokio::test]
async fn absence_snapshots_ignore_other_tables_but_detect_first_record() {
    let (shared, actor) = actor(1, "absence-identities").await;
    let first = page(&actor, query("MISSING")).await;
    let mut verify = query("MISSING");
    verify.request.continuation = true;
    verify.request.verify_only = true;
    verify.request.revision = first.revision;
    verify.request.incarnation = first.incarnation;
    verify.request.snapshot_time_ms = first.snapshot_time_ms;
    apply_ready(&actor, "OTHER", 1).await;
    let unchanged = shared.query_page(&verify).await;
    assert_eq!(unchanged.status, crate::proto::QueryPageStatus::Ok as i32);
    apply_ready(&actor, "MISSING", 2).await;
    assert_eq!(
        shared.query_page(&verify).await.status,
        crate::proto::QueryPageStatus::Changed as i32
    );
    stop(actor).await;
}

#[tokio::test]
async fn worker_lru_preserves_data_query_incarnations_and_has_no_resurrection() {
    let (shared, actor) = actor(1, "table-lru-identities").await;
    apply_ready(&actor, "FIRST", 1).await;
    let first = page(&actor, query("FIRST")).await;
    let mut verify = query("FIRST");
    verify.request.continuation = true;
    verify.request.verify_only = true;
    verify.request.revision = first.revision;
    verify.request.incarnation = first.incarnation;
    verify.request.snapshot_time_ms = first.snapshot_time_ms;
    for index in 0..24 {
        apply_ready(&actor, &format!("TABLE_{index}"), 1).await;
        assert_eq!(actor.get_children().len(), 6);
    }
    assert_eq!(
        shared.query_page(&verify).await.status,
        crate::proto::QueryPageStatus::Ok as i32
    );
    let reopened = page(&actor, verify).await;
    assert_eq!(reopened.status, crate::proto::QueryPageStatus::Ok as i32);
    assert_eq!(reopened.incarnation, first.incarnation);
    assert_eq!(shared.get(&key("FIRST", "one")).await.unwrap().value, [1]);
    assert_eq!(actor.get_children().len(), 6);
    stop(actor).await;
}

struct Release(Arc<tokio::sync::Notify>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[tokio::test]
async fn busy_and_cancelled_work_is_not_evicted_and_capacity_sheds_are_nonfatal() {
    let (shared, actor) = actor(1, "busy-table-capacity").await;
    apply_ready(&actor, "SECOND", 1).await;
    apply_ready(&actor, "FIRST", 1).await;
    let gate = super::pool_worker::install_worker_get_gate(key("FIRST", "one"));
    let _release = Release(gate.release.clone());
    let reading = actor.clone();
    let blocked = tokio::spawn(async move {
        reading
            .call(
                |reply| KvMessage::Get {
                    key: key("FIRST", "one"),
                    priority: OpPriority::Default,
                    reply,
                },
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let reading = actor.clone();
    let queued = tokio::spawn(async move {
        reading
            .call(
                |reply| KvMessage::Get {
                    key: key("FIRST", "two"),
                    priority: OpPriority::Default,
                    reply,
                },
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let request = super::observations::PoolObservationRequest {
                tables: vec!["FIRST".into()],
                max_pools: 3,
                max_workers: 3,
                include_internal: false,
                retention: Arc::new(()),
            };
            if let CallResult::Success(page) = actor
                .call(|reply| KvMessage::ObservePools { request, reply }, None)
                .await
                .unwrap()
            {
                if page.pools.iter().any(|pool| pool.pending == 2) {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    blocked.abort();
    let _ = blocked.await;
    assert!(matches!(
        apply(&actor, "SECOND", 2, true, "writer").await,
        Err(ApplyError::Backpressure)
    ));
    assert!(
        matches!(actor.call(|reply| KvMessage::Get {key: key("SECOND", "one"), priority: OpPriority::Default, reply}, None).await.unwrap(), CallResult::Success(Err(error)) if error.starts_with("shed:"))
    );
    assert_eq!(
        page(&actor, query("SECOND")).await.status,
        crate::proto::QueryPageStatus::Backpressure as i32
    );
    assert_eq!(shared.get(&key("SECOND", "one")).await.unwrap().value, [1]);
    assert_eq!(shared.cached_table_count().await, 3);
    assert_eq!(actor.get_status(), ActorStatus::Running);
    gate.release.notify_one();
    queued.await.unwrap().unwrap();
    apply_ready(&actor, "SECOND", 2).await;
    assert_eq!(shared.get(&key("SECOND", "one")).await.unwrap().value, [2]);
    assert_eq!(actor.get_children().len(), 6);
    stop(actor).await;
}

#[tokio::test]
async fn rejected_cold_writes_do_not_accumulate_empty_children() {
    let (shared, actor) = actor(2, "cold-rejected-tables").await;
    for index in 0..32 {
        assert!(matches!(
            apply(&actor, &format!("REJECTED_{index}"), 1, false, "").await,
            Err(ApplyError::InvalidInput(_))
        ));
        assert_eq!(shared.cached_table_count().await, 1);
        assert!(actor.get_children().len() <= 9);
    }
    stop(actor).await;
}

#[tokio::test]
async fn cancelled_cold_write_retains_actor_execution_without_speculative_child() {
    let label = "cancelled-cold-table";
    let (shared, actor) = actor(1, label).await;
    let gate = super::pool_worker::install_worker_startup_gate(format!(
        "{label}-table-COLD-high-worker-0"
    ));
    let _release = Release(gate.release.clone());
    let writing = actor.clone();
    let request = tokio::spawn(async move { apply(&writing, "COLD", 1, false, "writer").await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    request.abort();
    let _ = request.await;
    assert_eq!(
        shared.cached_table_count().await,
        1,
        "no speculative COLD child while workers start"
    );
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while shared.get(&key("COLD", "one")).await.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(shared.cached_table_count().await, 2);
    assert_eq!(actor.get_status(), ActorStatus::Running);
    stop(actor).await;
}

#[tokio::test]
async fn standalone_unpartitioned_actor_resolves_children_after_eviction_without_changing_base_view(
) {
    let shared = Arc::new(
        FragmentState::new(
            std::collections::HashMap::from([
                (
                    "plain".into(),
                    VersionedValue::new(vec![9], 1, "writer".into()),
                ),
                (
                    key("KV", "preloaded"),
                    VersionedValue::new(vec![7], 1, "writer".into()),
                ),
                (
                    key("CUSTOM", "preloaded"),
                    VersionedValue::new(vec![8], 1, "writer".into()),
                ),
            ]),
            None,
            100,
            Duration::from_secs(3600),
        )
        .with_table_pool_capacity(1)
        .unwrap(),
    );
    let (actor, _) = KvActor::spawn(
        None,
        KvActor,
        KvArgs::builder()
            .shared(shared.clone())
            .fragment(crate::shard::FragmentId(0))
            .worker_prefix("unpartitioned-table-cache")
            .build(),
    )
    .await
    .unwrap();
    let base = shared.query_page(&query("KV")).await;
    for (table, expected) in [("KV", 7), ("CUSTOM", 8), ("KV", 7)] {
        assert!(
            matches!(actor.call(|reply| KvMessage::Get {key: key(table, "preloaded"), priority: OpPriority::Default, reply}, None).await.unwrap(), CallResult::Success(Ok(Some(value))) if value.value == [expected])
        );
    }
    let mut verify = query("KV");
    verify.request.continuation = true;
    verify.request.verify_only = true;
    verify.request.revision = base.revision;
    verify.request.incarnation = base.incarnation;
    verify.request.snapshot_time_ms = base.snapshot_time_ms;
    for table in ["KV", "CUSTOM", "OTHER", "KV", "CUSTOM"] {
        apply_ready(&actor, table, 1).await;
        assert!(
            matches!(actor.call(|reply| KvMessage::Get {key: key(table, "one"), priority: OpPriority::Default, reply}, None).await.unwrap(), CallResult::Success(Ok(Some(value))) if value.value == [1])
        );
        assert_eq!(
            page(&actor, query(table)).await.status,
            crate::proto::QueryPageStatus::Ok as i32
        );
    }
    assert_eq!(
        shared.query_page(&verify).await.status,
        crate::proto::QueryPageStatus::Ok as i32
    );
    assert_eq!(shared.get("plain").await.unwrap().value, [9]);
    assert!(
        matches!(actor.call(|reply| KvMessage::Get {key: "plain".into(), priority: OpPriority::Default, reply}, None).await.unwrap(), CallResult::Success(Ok(Some(value))) if value.value == [9])
    );
    stop(actor).await;
}
