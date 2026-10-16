// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Exact worker ownership and bounded local health evidence.

use super::*;

enum HealthDispatcherMode {
    Reply,
    DropReply,
    Block(Arc<tokio::sync::Notify>),
}

struct HealthDispatcher;

impl Actor for HealthDispatcher {
    type Msg = KvMessage;
    type State = HealthDispatcherMode;
    type Arguments = HealthDispatcherMode;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let KvMessage::GetShedCounts { reply } = message {
            match state {
                HealthDispatcherMode::Reply => {}
                HealthDispatcherMode::DropReply => return Ok(()),
                HealthDispatcherMode::Block(release) => release.notified().await,
            }
            let _ = reply.send((7, 11, 13));
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: SupervisionEvent,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Keep a stopped child absent; this probe fixture intentionally
        // does not restart workers or terminate itself on child exit.
        Ok(())
    }
}

fn health_args(prefix: &str) -> SupervisorArgs {
    let pool = PoolConfig {
        workers: 1,
        queue_cap: 16,
        rate_limit_per_sec: None,
    };
    SupervisorArgs {
        fragment: FragmentId(0),
        worker_prefix: prefix.to_string(),
        store: None,
        hlc: Arc::new(Mutex::new(HlcClock::new())),
        default_pool: pool.clone(),
        high_pool: pool.clone(),
        background_pool: pool,
        flush_batch_size: 100,
        flush_interval: Duration::from_secs(3600),
        clean_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        tombstone_gc_age: None,
        reset_senders: Arc::new(Mutex::new(HashMap::new())),
    }
}

#[tokio::test]
async fn health_counts_only_exact_owned_running_worker_names() {
    let args = health_args("health-owned-workers");
    let (dispatcher, _) =
        HealthDispatcher::spawn(None, HealthDispatcher, HealthDispatcherMode::Reply)
            .await
            .unwrap();
    let shared = Arc::new(FragmentState::new(
        HashMap::new(),
        None,
        100,
        Duration::from_secs(3600),
    ));
    let mut children = Vec::new();
    for suffix in ["high", "default", "bg"] {
        let worker_args = KvWorkerArgs {
            shared: shared.clone(),
            pending: None,
        };
        let name = Some(format!("{}-{suffix}-worker-0", args.worker_prefix));
        let (child, _) = if suffix == "default" {
            // Same configured name, but not owned by this dispatcher.
            KvWorker::spawn(name, KvWorker, worker_args).await.unwrap()
        } else {
            KvWorker::spawn_linked(name, KvWorker, worker_args, dispatcher.get_cell())
                .await
                .unwrap()
        };
        children.push(child);
    }
    children[2].stop(None);
    children[2].wait(None).await.unwrap();
    let restarts = HashMap::from([("dispatcher".to_string(), 3)]);
    let health = probe_health(&dispatcher, &args, &restarts, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(health.worker_count, 1);
    assert_eq!(
        health.worker_names,
        vec!["health-owned-workers-high-worker-0"]
    );
    assert_eq!(health.shed_counts, (7, 11, 13));
    assert_eq!(health.restarts, restarts);
    for child in children {
        child.stop(None);
        child.wait(None).await.unwrap();
    }
    dispatcher.stop(None);
    dispatcher.wait(None).await.unwrap();
}

#[tokio::test]
async fn health_dispatcher_missing_reply_stopped_and_blocked_are_unavailable() {
    let args = health_args("health-unavailable");
    let restarts = HashMap::new();
    let (missing_reply, _) =
        HealthDispatcher::spawn(None, HealthDispatcher, HealthDispatcherMode::DropReply)
            .await
            .unwrap();
    assert!(
        probe_health(&missing_reply, &args, &restarts, Duration::from_secs(1))
            .await
            .is_none()
    );
    missing_reply.stop(None);
    missing_reply.wait(None).await.unwrap();
    assert!(
        probe_health(&missing_reply, &args, &restarts, Duration::from_secs(1))
            .await
            .is_none()
    );

    let release = Arc::new(tokio::sync::Notify::new());
    let (blocked, _) = HealthDispatcher::spawn(
        None,
        HealthDispatcher,
        HealthDispatcherMode::Block(release.clone()),
    )
    .await
    .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        probe_health(&blocked, &args, &restarts, Duration::from_millis(20)),
    )
    .await
    .unwrap()
    .is_none());
    release.notify_one();
    blocked.stop(None);
    blocked.wait(None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_closed_ports_skip_and_blocked_dispatcher_does_not_pin_supervisor() {
    let store = Arc::new(GatedStore::default());
    let _release = ReleaseStore(store.clone());
    let mut args = health_args("health-blocked-real-dispatcher");
    args.store = Some(store.clone());
    args.flush_batch_size = 1;
    let (supervisor, _) = FragmentSupervisor::spawn(None, FragmentSupervisor, args)
        .await
        .unwrap();
    let dispatcher = match supervisor
        .call(|reply| SupervisorMessage::GetDispatcher { reply }, None)
        .await
        .unwrap()
    {
        CallResult::Success(dispatcher) => dispatcher,
        other => panic!("dispatcher lookup failed: {other:?}"),
    };
    let (applied, mut apply_result) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::ApplyOp {
            op: ReplicatedOp {
                key: "blocked".to_string(),
                value: VersionedValue::new(b"value".to_vec(), 1, "writer".to_string()),
                seq: 1,
                boot_id: 1,
            },
            priority: crate::actor::OpPriority::Default,
            is_replication: false,
            reply: Some(applied.into()),
        })
        .unwrap();
    crate::common_test::periodic_async_check(
        || async { store.entered.load(Ordering::SeqCst) },
        Duration::from_secs(2),
    )
    .await;
    let (flushed, flush_result) = tokio::sync::oneshot::channel();
    dispatcher
        .send_message(KvMessage::Flush {
            reply: flushed.into(),
        })
        .unwrap();

    // The dispatcher is held behind the store fence. Abandoned health
    // requests must not start an unnecessary five-second nested probe.
    let (abandoned, receiver) = tokio::sync::oneshot::channel();
    drop(receiver);
    supervisor
        .send_message(SupervisorMessage::GetHealth {
            reply: abandoned.into(),
        })
        .unwrap();
    assert!(matches!(
        supervisor
            .call(
                |reply| SupervisorMessage::GetDispatcher { reply },
                Some(Duration::from_millis(200)),
            )
            .await
            .unwrap(),
        CallResult::Success(_)
    ));
    assert!(matches!(
        supervisor
            .call(
                |reply| SupervisorMessage::GetHealth { reply },
                Some(Duration::from_millis(20)),
            )
            .await
            .unwrap(),
        CallResult::Timeout | CallResult::SenderError
    ));
    // Inner health timeout uses the caller's budget; the supervisor can
    // answer again while the actual dispatcher/store is still blocked.
    assert!(matches!(
        supervisor
            .call(
                |reply| SupervisorMessage::GetDispatcher { reply },
                Some(Duration::from_millis(200)),
            )
            .await
            .unwrap(),
        CallResult::Success(_)
    ));
    assert!(matches!(
        apply_result.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    store.release();
    assert_eq!(apply_result.await.unwrap().unwrap(), ApplyOutcome::Applied);
    assert!(flush_result.await.unwrap().is_ok());
    match supervisor
        .call(|reply| SupervisorMessage::Quiesce { reply }, None)
        .await
        .unwrap()
    {
        CallResult::Success(Ok(())) => {}
        other => panic!("quiesce failed: {other:?}"),
    }
    supervisor.stop(None);
    supervisor.wait(None).await.unwrap();
}
