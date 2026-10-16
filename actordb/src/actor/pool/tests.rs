// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Managed-pool admission, routing, and worker lifecycle regressions.

use super::*;
use crate::actor::pool_worker::take_worker_get_gate;
use crate::actor::{
    BatchReadEntry, KvActor, KvArgs, OpPriority, ReplicatedOp, RetainedBatchRead, VersionedValue,
};
use crate::shard::FragmentId;
use ractor::{registry::where_is, rpc::CallResult, ActorStatus, SupervisionEvent};
use std::collections::HashMap;
use std::time::Duration;
#[test]
fn pool_builder_preserves_defaults_and_optional_overrides() {
    let built = PoolConfig::builder().build();
    let default = PoolConfig::default();
    assert_eq!(
        (built.workers, built.queue_cap, built.rate_limit_per_sec),
        (
            default.workers,
            default.queue_cap,
            default.rate_limit_per_sec
        )
    );
    assert_eq!((built.workers, built.queue_cap), (4, 10_000));
    assert_eq!(built.rate_limit_per_sec, None);
    let limited = PoolConfig::builder()
        .workers(3)
        .queue_cap(17)
        .rate_limit_per_sec(9)
        .build();
    assert_eq!(
        (
            limited.workers,
            limited.queue_cap,
            limited.rate_limit_per_sec
        ),
        (3, 17, Some(9))
    );
    assert_eq!(
        PoolConfig::builder()
            .maybe_rate_limit_per_sec(None)
            .build()
            .rate_limit_per_sec,
        None
    );
}
static NEXT_PREFIX: AtomicU64 = AtomicU64::new(0);

struct SupervisionProbe;

impl Actor for SupervisionProbe {
    type Msg = ();
    type State = tokio::sync::mpsc::UnboundedSender<SupervisionEvent>;
    type Arguments = tokio::sync::mpsc::UnboundedSender<SupervisionEvent>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        events: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(events)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        events: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = events.send(message);
        Ok(())
    }
}

fn unique_prefix(label: &str) -> String {
    format!("{label}-{}", NEXT_PREFIX.fetch_add(1, Ordering::Relaxed))
}

fn pool_config(queue_cap: usize, rate_limit_per_sec: Option<u32>) -> PoolConfig {
    PoolConfig {
        workers: 1,
        queue_cap,
        rate_limit_per_sec,
    }
}

fn op(key: &str, timestamp_ms: u64) -> ReplicatedOp {
    ReplicatedOp {
        key: key.to_string(),
        value: VersionedValue::new(
            timestamp_ms.to_be_bytes().to_vec(),
            timestamp_ms,
            "test-node".to_string(),
        ),
        seq: timestamp_ms,
        boot_id: 1,
    }
}

async fn wait_for(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition did not become true");
}

#[tokio::test]
async fn standalone_worker_pool_releases_capacity_and_honors_rate_limit() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = WorkerPool::spawn(pool_config(1, None), shared.clone())
        .await
        .unwrap();

    pool.try_reserve("first")
        .await
        .expect("first job should be admitted")
        .send(KvMessage::ApplyOp {
            op: op("first", 1),
            priority: OpPriority::Default,
            is_replication: false,
            reply: None,
        });
    wait_for(|| pool.pending.load(Ordering::Relaxed) == 0).await;
    assert!(shared.get("first").await.is_some());

    let second = pool
        .try_reserve("second")
        .await
        .expect("completed work did not release capacity");
    drop(second);
    assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
    assert_eq!(pool.workers[0].pending.load(Ordering::Relaxed), 0);
    pool.shutdown().await;

    let rate_limited = WorkerPool::spawn(pool_config(1, Some(1)), shared)
        .await
        .unwrap();
    drop(
        rate_limited
            .try_reserve("token")
            .await
            .expect("initial rate-limit token should be available"),
    );
    assert!(
        rate_limited.try_reserve("limited").await.is_none(),
        "standalone pool ignored its configured rate limit"
    );
    assert_eq!(rate_limited.shed_count(), 1);
    rate_limited.shutdown().await;
}

#[tokio::test]
async fn batch_read_releases_each_reservation_before_routing_the_next_key() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(1, None);
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: unique_prefix("batch-cap-one"),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();

    let page = dispatcher
        .call(
            |reply| KvMessage::GetBatch {
                keys: vec!["first".to_string(), "second".to_string()],
                response_base_payload_len: 0,
                response_soft_limit: usize::MAX,
                reply,
                priority: OpPriority::Default,
            },
            None,
        )
        .await
        .unwrap();
    let CallResult::Success(page) = page else {
        panic!("batch read did not complete");
    };
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().all(|entry| entry.result == Ok(None)));
    let counts = dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    assert_eq!(counts, CallResult::Success((0, 0, 0)));

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test]
async fn batch_read_stops_before_admitting_an_omitted_suffix() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    shared.apply(&op("first", 1)).await.unwrap();
    shared.apply(&op("second", 2)).await.unwrap();
    let pool = pool_config(8, Some(1));
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: unique_prefix("batch-lazy-admission"),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();

    let page = dispatcher
        .call(
            |reply| KvMessage::GetBatch {
                keys: vec!["first".to_string(), "second".to_string()],
                response_base_payload_len: 0,
                response_soft_limit: 0,
                reply,
                priority: OpPriority::Default,
            },
            None,
        )
        .await
        .unwrap();
    let CallResult::Success(page) = page else {
        panic!("batch read did not complete");
    };
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, "first");
    assert!(matches!(page.entries[0].result, Ok(Some(_))));
    let counts = dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    assert_eq!(
        counts,
        CallResult::Success((0, 0, 0)),
        "omitted suffix consumed a rate-limit token"
    );

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test]
async fn batch_read_rate_limit_is_charged_and_reported_per_key() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(8, Some(1));
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: unique_prefix("batch-rate-limit"),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();

    let page = dispatcher
        .call(
            |reply| KvMessage::GetBatch {
                keys: vec!["first".to_string(), "second".to_string()],
                response_base_payload_len: 0,
                response_soft_limit: usize::MAX,
                reply,
                priority: OpPriority::Default,
            },
            None,
        )
        .await
        .unwrap();
    let CallResult::Success(page) = page else {
        panic!("batch read did not complete");
    };
    assert_eq!(page.entries[0].result, Ok(None));
    assert!(page.entries[1].result.is_err());
    let counts = dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    assert_eq!(counts, CallResult::Success((0, 1, 0)));

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_background_batch_read_does_not_stall_high_priority_or_health() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(8, None);
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: unique_prefix("batch-priority-isolation"),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let gate = install_worker_get_gate("blocked-batch".to_string());
    let batch_dispatcher = dispatcher.clone();
    let batch = tokio::spawn(async move {
        batch_dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["blocked-batch".to_string(), "later".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply,
                    priority: OpPriority::Background,
                },
                None,
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("background batch did not reach its worker");

    let high = tokio::time::timeout(
        Duration::from_secs(2),
        dispatcher.call(
            |reply| KvMessage::Get {
                key: "high".to_string(),
                reply,
                priority: OpPriority::High,
            },
            None,
        ),
    )
    .await
    .expect("background batch blocked high-priority dispatch")
    .unwrap();
    assert_eq!(high, CallResult::Success(Ok(None)));
    let counts = dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    assert_eq!(counts, CallResult::Success((0, 0, 0)));

    gate.release.notify_one();
    let CallResult::Success(page) = batch.await.unwrap() else {
        panic!("background batch did not complete");
    };
    assert_eq!(page.entries.len(), 2);

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_batch_lease_survives_cancellation_until_sticky_worker_drops_keys() {
    for kill in [false, true] {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(1, None);
        let prefix = unique_prefix("retained-batch");
        let key = format!("{prefix}-blocked");
        let suffix = format!("{prefix}-suffix");
        let gate = install_worker_get_gate(key.clone());
        let suffix_gate = install_worker_get_gate(suffix.clone());
        let (dispatcher, _) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix.clone(),
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let retention = Arc::new(());
        let retained = Arc::downgrade(&retention);
        let batch_dispatcher = dispatcher.clone();
        let batch = tokio::spawn(async move {
            batch_dispatcher
                .call(
                    move |reply| {
                        KvMessage::GetBatchRetained(RetainedBatchRead::new(
                            vec![key, suffix],
                            0,
                            usize::MAX,
                            reply,
                            OpPriority::Default,
                            retention,
                        ))
                    },
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        assert!(
            retained.upgrade().is_some(),
            "cancellation freed actor-owned admission"
        );
        if kill {
            where_is(format!("{prefix}-default-worker-0"))
                .unwrap()
                .kill();
        } else {
            gate.release.notify_one();
        }
        wait_for(|| retained.upgrade().is_none()).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), suffix_gate.entered.notified())
                .await
                .is_err(),
            "cancelled request admitted its untouched suffix"
        );
        take_worker_get_gate(&format!("{prefix}-suffix"));
        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_read_worker_loss_closes_reply_and_reclaims_its_reservation() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(1, None);
    let prefix = unique_prefix("batch-worker-loss");
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: prefix.clone(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let gate = install_worker_get_gate("blocked-batch-loss".to_string());
    let batch_dispatcher = dispatcher.clone();
    let batch = tokio::spawn(async move {
        batch_dispatcher
            .call(
                |reply| KvMessage::GetBatch {
                    keys: vec!["blocked-batch-loss".to_string(), "later".to_string()],
                    response_base_payload_len: 0,
                    response_soft_limit: usize::MAX,
                    reply,
                    priority: OpPriority::Default,
                },
                None,
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("batch did not reach the worker");

    let worker_name = format!("{prefix}-default-worker-0");
    let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
    let old_id = old_worker.get_id();
    old_worker.kill();
    wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
        .await;
    assert!(matches!(batch.await.unwrap(), CallResult::SenderError));

    let recovered = dispatcher
        .call(
            |reply| KvMessage::GetBatch {
                keys: vec!["recovered".to_string()],
                response_base_payload_len: 0,
                response_soft_limit: usize::MAX,
                reply,
                priority: OpPriority::Default,
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(recovered, CallResult::Success(_)));
    let counts = dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    assert_eq!(counts, CallResult::Success((0, 0, 0)));

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_read_orderly_drain_returns_the_admitted_prefix() {
    for key_count in [1, 2] {
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            None,
            100,
            Duration::from_secs(3600),
        ));
        let pool = pool_config(1, None);
        let prefix = unique_prefix("batch-orderly-drain");
        let first = format!("{prefix}-first");
        let keys = vec![first.clone(), format!("{prefix}-suffix")];
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix,
                default_pool: pool.clone(),
                high_pool: pool.clone(),
                background_pool: pool,
            },
        )
        .await
        .unwrap();
        let gate = install_worker_get_gate(first.clone());
        let (reply, response) = tokio::sync::oneshot::channel();
        dispatcher
            .send_message(KvMessage::GetBatch {
                keys: keys.into_iter().take(key_count).collect(),
                response_base_payload_len: 0,
                response_soft_limit: usize::MAX,
                reply: reply.into(),
                priority: OpPriority::Default,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("batch did not enter its worker");

        dispatcher.drain().unwrap();
        gate.release.notify_one();
        let page = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .expect("admitted read did not finish during drain")
            .expect("orderly drain dropped the completed prefix");
        assert_eq!(
            page.entries,
            vec![BatchReadEntry {
                key: first,
                result: Ok(None),
            }]
        );
        tokio::time::timeout(Duration::from_secs(2), dispatcher.wait(None))
            .await
            .expect("batch prevented orderly worker drain")
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_read_cancellation_retains_admission_until_worker_completion() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(1, None);
    let prefix = unique_prefix("batch-cancelled");
    let first = format!("{prefix}-first");
    let suffix = format!("{prefix}-suffix");
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: prefix,
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let gate = install_worker_get_gate(first.clone());
    let suffix_gate = install_worker_get_gate(suffix.clone());
    let (reply, response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::GetBatch {
            keys: vec![first, suffix.clone()],
            response_base_payload_len: 0,
            response_soft_limit: usize::MAX,
            reply: reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("batch did not enter its worker");
    drop(response);

    let read = || {
        dispatcher.call(
            |reply| KvMessage::Get {
                key: "probe".to_string(),
                reply,
                priority: OpPriority::Default,
            },
            None,
        )
    };
    assert!(matches!(read().await.unwrap(), CallResult::Success(Err(_))));
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(read().await.unwrap(), CallResult::Success(Ok(None))) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled batch did not release its completed reservation");
    assert!(
        take_worker_get_gate(&suffix).is_some(),
        "cancelled batch admitted its untouched suffix"
    );
    drop(suffix_gate);
    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test]
async fn immediate_worker_send_failure_rolls_back_both_counters() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = WorkerPool::spawn(pool_config(1, None), shared)
        .await
        .unwrap();
    let reservation = pool
        .try_reserve("dead")
        .await
        .expect("job should reserve the empty pool");
    let slot = reservation.slot;
    let worker = pool.workers[slot].worker.clone();
    worker.stop(None);
    worker.wait(None).await.unwrap();

    let (reply, response) = tokio::sync::oneshot::channel();
    reservation.send(KvMessage::Get {
        key: "dead".to_string(),
        reply: reply.into(),
        priority: OpPriority::Default,
    });

    assert!(response.await.is_err(), "failed send fabricated a reply");
    assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
    assert_eq!(pool.workers[slot].pending.load(Ordering::Relaxed), 0);
    assert_eq!(pool.shed_count(), 0, "unavailability was counted as shed");
    pool.shutdown().await;
}

#[tokio::test]
async fn cancelled_get_holds_admission_until_worker_terminal() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = WorkerPool::spawn(pool_config(1, None), shared.clone())
        .await
        .unwrap();
    let cache_guard = shared.lock_cache_for_test().await;
    let (reply, response) = tokio::sync::oneshot::channel();
    pool.try_reserve("cancelled")
        .await
        .expect("first read should be admitted")
        .send(KvMessage::Get {
            key: "cancelled".to_string(),
            reply: reply.into(),
            priority: OpPriority::Default,
        });
    drop(response);

    assert_eq!(pool.pending.load(Ordering::Relaxed), 1);
    assert!(
        pool.try_reserve("next").await.is_none(),
        "caller cancellation released capacity before worker completion"
    );

    drop(cache_guard);
    wait_for(|| pool.pending.load(Ordering::Relaxed) == 0).await;
    drop(
        pool.try_reserve("next")
            .await
            .expect("worker completion did not release cancelled read"),
    );
    assert_eq!(pool.pending.load(Ordering::Relaxed), 0);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_kill_reclaims_active_and_queued_admissions() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let prefix = unique_prefix("reclaim-worker");
    let pool = pool_config(2, None);
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared: shared.clone(),
            fragment: FragmentId(0),
            worker_prefix: prefix.clone(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();

    // Hold the cache so one read is active and the next remains queued in
    // the same sticky worker. A third read proves both capacity slots are
    // reserved before the kill.
    let cache_guard = shared.lock_cache_for_test().await;
    let (first_reply, first_response) = tokio::sync::oneshot::channel();
    let (second_reply, second_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: "same-slot".to_string(),
            reply: first_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    dispatcher
        .send_message(KvMessage::Get {
            key: "same-slot".to_string(),
            reply: second_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    let shed = tokio::time::timeout(
        Duration::from_secs(1),
        dispatcher.call(
            |reply| KvMessage::Get {
                key: "same-slot".to_string(),
                reply,
                priority: OpPriority::Default,
            },
            None,
        ),
    )
    .await
    .expect("dispatcher blocked behind a worker read")
    .unwrap();
    assert!(matches!(shed, CallResult::Success(Err(_))));

    let worker_name = format!("{prefix}-default-worker-0");
    let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
    let old_id = old_worker.get_id();
    old_worker.kill();
    wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
        .await;
    assert!(first_response.await.is_err());
    assert!(second_response.await.is_err());

    // Keep the replacement blocked and fill the whole pool again. Both
    // replies must succeed after release; one leaked slot would shed one.
    let (fresh_one_reply, fresh_one_response) = tokio::sync::oneshot::channel();
    let (fresh_two_reply, fresh_two_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: "same-slot".to_string(),
            reply: fresh_one_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    dispatcher
        .send_message(KvMessage::Get {
            key: "same-slot".to_string(),
            reply: fresh_two_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();
    drop(cache_guard);
    assert!(fresh_one_response.await.unwrap().is_ok());
    assert!(fresh_two_response.await.unwrap().is_ok());

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_kill_reclaims_only_the_dead_slots_admissions() {
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let prefix = unique_prefix("reclaim-one-slot");
    let pool = PoolConfig {
        workers: 2,
        queue_cap: 2,
        rate_limit_per_sec: None,
    };
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared: shared.clone(),
            fragment: FragmentId(0),
            worker_prefix: prefix.clone(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let key_for_slot = |wanted| {
        (0..1_000)
            .map(|candidate| format!("slot-key-{candidate}"))
            .find(|key| worker_idx(key, 2) == wanted)
            .expect("test keys should cover both worker slots")
    };
    let dead_key = key_for_slot(0);
    let healthy_key = key_for_slot(1);

    // Block one admitted read in each worker. The dispatcher health call
    // is a mailbox fence proving both reservations were handed off.
    let cache_guard = shared.lock_cache_for_test().await;
    let (dead_reply, dead_response) = tokio::sync::oneshot::channel();
    let (healthy_reply, healthy_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: dead_key.clone(),
            reply: dead_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    dispatcher
        .send_message(KvMessage::Get {
            key: healthy_key,
            reply: healthy_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    dispatcher
        .call(|reply| KvMessage::GetShedCounts { reply }, None)
        .await
        .unwrap();

    let worker_name = format!("{prefix}-default-worker-0");
    let old_worker = where_is(worker_name.clone()).expect("worker should be registered");
    let old_id = old_worker.get_id();
    old_worker.kill();
    wait_for(|| where_is(worker_name.clone()).is_some_and(|worker| worker.get_id() != old_id))
        .await;
    assert!(dead_response.await.is_err());

    // Exactly one slot is now available. A fresh read for the replacement
    // consumes it; another read must shed while the healthy sibling still
    // owns its original reservation.
    let (fresh_reply, fresh_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: dead_key.clone(),
            reply: fresh_reply.into(),
            priority: OpPriority::Default,
        })
        .unwrap();
    let overflow = tokio::time::timeout(
        Duration::from_secs(2),
        dispatcher.call(
            |reply| KvMessage::Get {
                key: dead_key,
                reply,
                priority: OpPriority::Default,
            },
            None,
        ),
    )
    .await;

    drop(cache_guard);
    assert!(healthy_response.await.unwrap().is_ok());
    assert!(fresh_response.await.unwrap().is_ok());
    assert!(matches!(
        overflow
            .expect("healthy sibling reservation was reclaimed")
            .unwrap(),
        CallResult::Success(Err(_))
    ));

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_racing_worker_restart_still_drains_accepted_sibling_work() {
    let prefix = unique_prefix("restart-drain-race");
    let accepted_key = format!("{prefix}-accepted-before-drain");
    let shared = Arc::new(FragmentState::new(
        HashMap::from([(
            accepted_key.clone(),
            VersionedValue::new(b"value".to_vec(), 1, "test-node".to_string()),
        )]),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(8, None);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (probe, probe_handle) = SupervisionProbe::spawn(None, SupervisionProbe, event_tx)
        .await
        .unwrap();
    let (dispatcher, _handle) = KvActor::spawn_linked(
        None,
        KvActor,
        KvArgs {
            shared: shared.clone(),
            fragment: FragmentId(0),
            worker_prefix: prefix.clone(),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
        probe.get_cell(),
    )
    .await
    .unwrap();
    let dispatcher_id = dispatcher.get_id();

    // Keep accepted work active in a sibling pool. If the replacement
    // race turns into a dispatcher handler failure, ractor skips
    // `post_stop` and this worker is killed before it can reply.
    let accepted_gate = install_worker_get_gate(accepted_key.clone());
    let (accepted_reply, accepted_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: accepted_key.clone(),
            priority: OpPriority::High,
            reply: accepted_reply.into(),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), accepted_gate.entered.notified())
        .await
        .expect("accepted sibling work did not enter its worker");

    // Pause a default-worker replacement in pre_start. Draining the
    // dispatcher before releasing it makes ractor reject the new link,
    // deterministically exercising the status-check/spawn race.
    let worker_name = format!("{prefix}-default-worker-0");
    let startup_gate = install_worker_startup_gate(worker_name.clone());
    where_is(worker_name)
        .expect("default worker should be registered")
        .kill();
    tokio::time::timeout(Duration::from_secs(2), startup_gate.entered.notified())
        .await
        .expect("replacement did not enter pre_start");

    dispatcher.drain().unwrap();
    assert!(dispatcher.get_status() >= ActorStatus::Draining);
    startup_gate.release.notify_one();
    accepted_gate.release.notify_one();

    let accepted = tokio::time::timeout(Duration::from_secs(2), accepted_response)
        .await
        .expect("accepted sibling work did not finish during drain")
        .unwrap();
    assert!(matches!(accepted, Ok(Some(_))));
    tokio::time::timeout(Duration::from_secs(2), dispatcher.wait(None))
        .await
        .expect("dispatcher did not complete its orderly drain")
        .unwrap();
    let terminated_cleanly = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match event_rx.recv().await {
                Some(SupervisionEvent::ActorTerminated(cell, _, _))
                    if cell.get_id() == dispatcher_id =>
                {
                    break true;
                }
                Some(SupervisionEvent::ActorFailed(cell, _)) if cell.get_id() == dispatcher_id => {
                    break false;
                }
                Some(_) => {}
                None => panic!("supervision probe stopped before dispatcher exit"),
            }
        }
    })
    .await
    .expect("supervisor did not observe dispatcher exit");
    assert!(
        terminated_cleanly,
        "replacement/drain race failed the dispatcher instead of terminating cleanly"
    );
    assert!(shared.get(&accepted_key).await.is_some());
    probe.stop(None);
    tokio::time::timeout(Duration::from_secs(2), probe_handle)
        .await
        .expect("supervision probe did not stop")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_background_get_does_not_stall_high_priority_or_health() {
    let prefix = unique_prefix("get-hol");
    let blocked_key = format!("{prefix}-blocked");
    let shared = Arc::new(FragmentState::new(
        HashMap::from([(
            "hot".to_string(),
            VersionedValue::new(b"value".to_vec(), 1, "test-node".to_string()),
        )]),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let pool = pool_config(8, None);
    let (dispatcher, _handle) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared,
            fragment: FragmentId(0),
            worker_prefix: prefix,
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();

    let worker_gate = install_worker_get_gate(blocked_key.clone());
    let (background_reply, background_response) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Get {
            key: blocked_key,
            reply: background_reply.into(),
            priority: OpPriority::Background,
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker_gate.entered.notified())
        .await
        .expect("background Get did not enter its worker");

    let health = tokio::time::timeout(
        Duration::from_secs(2),
        dispatcher.call(|reply| KvMessage::GetShedCounts { reply }, None),
    )
    .await;
    let high = tokio::time::timeout(
        Duration::from_secs(2),
        dispatcher.call(
            |reply| KvMessage::Get {
                key: "hot".to_string(),
                reply,
                priority: OpPriority::High,
            },
            None,
        ),
    )
    .await;

    // Always release the worker before asserting so the old nested-RPC
    // behavior cannot strand the dispatcher after the timeout.
    worker_gate.release.notify_one();
    assert!(matches!(background_response.await.unwrap(), Ok(None)));
    assert!(matches!(
        health
            .expect("health query stalled behind a background worker")
            .unwrap(),
        CallResult::Success(_)
    ));
    assert!(matches!(
        high.expect("high-priority read stalled").unwrap(),
        CallResult::Success(Ok(Some(_)))
    ));

    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
}
