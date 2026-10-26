// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

use super::super::test_support::test_peer_channel;
use super::*;
use crate::actor::{SnapshotSummary, SummaryPage};
use std::sync::atomic::AtomicBool;

struct RetentionProbe(Arc<AtomicBool>);
impl Drop for RetentionProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
struct SummaryActorState {
    started: Arc<Notify>,
    release: Arc<Notify>,
    completed: Arc<Notify>,
    blocked: bool,
}

struct SummaryActor;
impl Actor for SummaryActor {
    type Msg = KvMessage;
    type State = SummaryActorState;
    type Arguments = SummaryActorState;

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        let (message, retention) = message.into_retained_parts();
        if let KvMessage::GetSummaryPage { reply, .. } = message {
            state.started.notify_one();
            if state.blocked {
                state.release.notified().await;
            }
            let _ = reply.send(SummaryPage {
                summaries: vec![SnapshotSummary {
                    content_digest: [7; 32],
                    key: "key".into(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: "node".into(),
                    is_tombstone: false,
                }],
                revision: 5,
                snapshot_time_ms: 1,
                done: true,
                restart: false,
            });
            drop(retention);
            state.completed.notify_one();
        }
        Ok(())
    }
}

async fn fixture(
    blocked: bool,
) -> (
    ReconcileResponderContext,
    ActorRef<KvMessage>,
    SummaryActorState,
    InboundSessions,
    watch::Sender<u64>,
    watch::Receiver<u64>,
) {
    let state = SummaryActorState {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        completed: Arc::new(Notify::new()),
        blocked,
    };
    let (actor, _) = Actor::spawn(None, SummaryActor, state.clone())
        .await
        .unwrap();
    let router = FragmentRouter::new(1, 0);
    router.insert(FragmentId(0), actor.clone());
    let context = ReconcileResponderContext {
        served: Arc::new(StdMutex::new(HashMap::new())),
        router,
        serving: Arc::new(RwLock::new(HashSet::from([FragmentId(0)]))),
        placement: Arc::new(RwLock::new(HashMap::new())),
        recovery: None,
        dynamic_placement: false,
    };
    let sessions = Arc::new(Mutex::new(HashMap::from([(
        ("peer".into(), FragmentId(0)),
        InboundSession {
            session_id: 7,
            generation: 1,
        },
    )])));
    let (changes, changes_rx) = watch::channel(0);
    (context, actor, state, sessions, changes, changes_rx)
}

fn summary_job(
    context: &ReconcileResponderContext,
    actor: ActorRef<KvMessage>,
    timeout: Duration,
    retention: Option<Arc<dyn Send + Sync>>,
) -> ReconcileResponderJob {
    ReconcileResponderJob::Summary(ReconcileSummaryJob {
        retention,
        peer: "peer".into(),
        session: InboundSession {
            session_id: 7,
            generation: 1,
        },
        route_generation: context
            .router
            .route_with_generation(FragmentId(0))
            .unwrap()
            .0,
        actor,
        request: crate::proto::ReconcileRequest {
            fragment: 0,
            session_id: 7,
            round_id: 9,
            ..crate::proto::ReconcileRequest::default()
        },
        deadline: Instant::now() + timeout,
    })
}

#[tokio::test]
async fn summary_deadline_preserves_admitted_actor_retention_and_healthy_connection() {
    let (context, actor, state, sessions, _changes, changes_rx) = fixture(true).await;
    let served = context.served.clone();
    let (peer, _frames, mut responses) = test_peer_channel(2);
    let channel = peer.clone();
    let (fetches, fetch_rx) = mpsc::channel(1);
    let (summaries, summary_rx) = mpsc::channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let job = summary_job(
        &context,
        actor.clone(),
        Duration::from_millis(20),
        Some(Arc::new(RetentionProbe(dropped.clone()))),
    );
    let responder = tokio::spawn(run_reconcile_responder(
        fetch_rx,
        summary_rx,
        peer,
        sessions,
        changes_rx,
        Duration::from_secs(1),
        context,
    ));
    summaries.send(job).await.unwrap();
    state.started.notified().await;
    drop(fetches);
    drop(summaries);
    tokio::time::timeout(Duration::from_secs(1), responder)
        .await
        .unwrap()
        .unwrap();
    assert!(!*channel.cancel.borrow());
    assert!(
        !dropped.load(Ordering::Acquire),
        "deadline dropped accepted actor ownership"
    );
    assert!(served.lock().unwrap().is_empty());
    assert!(responses.try_recv().is_err());
    state.release.notify_one();
    state.completed.notified().await;
    assert!(dropped.load(Ordering::Acquire));
    actor.stop_and_wait(None, None).await.unwrap();
}

#[tokio::test]
async fn session_rotation_cancels_active_summary_without_publishing_stale_authorization() {
    let (context, actor, state, sessions, changes, changes_rx) = fixture(true).await;
    let served = context.served.clone();
    let (peer, _frames, mut responses) = test_peer_channel(2);
    let (fetches, fetch_rx) = mpsc::channel(1);
    let (summaries, summary_rx) = mpsc::channel(1);
    let job = summary_job(&context, actor.clone(), Duration::from_secs(30), None);
    let responder = tokio::spawn(run_reconcile_responder(
        fetch_rx,
        summary_rx,
        peer,
        sessions.clone(),
        changes_rx,
        Duration::from_secs(1),
        context,
    ));
    summaries.send(job).await.unwrap();
    state.started.notified().await;
    sessions.lock().await.insert(
        ("peer".into(), FragmentId(0)),
        InboundSession {
            session_id: 8,
            generation: 2,
        },
    );
    changes.send_modify(|generation| *generation += 1);
    drop(fetches);
    drop(summaries);
    tokio::time::timeout(Duration::from_millis(100), responder)
        .await
        .unwrap()
        .unwrap();
    state.release.notify_one();
    state.completed.notified().await;
    assert!(served.lock().unwrap().is_empty());
    assert!(responses.try_recv().is_err());
    actor.stop_and_wait(None, None).await.unwrap();
}

#[tokio::test]
async fn route_aba_or_authority_revocation_after_summary_read_never_advertises_keys() {
    for route_aba in [false, true] {
        let (context, actor, state, sessions, _changes, changes_rx) = fixture(true).await;
        let served = context.served.clone();
        let router = context.router.clone();
        let serving = context.serving.clone();
        let (peer, _frames, mut responses) = test_peer_channel(2);
        let (fetches, fetch_rx) = mpsc::channel(1);
        let (summaries, summary_rx) = mpsc::channel(1);
        let job = summary_job(&context, actor.clone(), Duration::from_secs(30), None);
        let responder = tokio::spawn(run_reconcile_responder(
            fetch_rx,
            summary_rx,
            peer,
            sessions,
            changes_rx,
            Duration::from_secs(1),
            context,
        ));
        summaries.send(job).await.unwrap();
        state.started.notified().await;
        if route_aba {
            router.remove(FragmentId(0));
            router.insert(FragmentId(0), actor.clone());
        } else {
            serving.write().unwrap().clear();
        }
        state.release.notify_one();
        drop(fetches);
        drop(summaries);
        responder.await.unwrap();
        assert!(served.lock().unwrap().is_empty());
        assert!(responses.try_recv().is_err());
        actor.stop_and_wait(None, None).await.unwrap();
    }
}

#[tokio::test]
async fn successful_summary_publishes_authorization_before_response_and_retains_frame() {
    let (context, actor, _state, sessions, _changes, changes_rx) = fixture(false).await;
    let served = context.served.clone();
    let (peer, _frames, mut responses) = test_peer_channel(2);
    let (fetches, fetch_rx) = mpsc::channel(1);
    let (summaries, summary_rx) = mpsc::channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let job = summary_job(
        &context,
        actor.clone(),
        Duration::from_secs(30),
        Some(Arc::new(RetentionProbe(dropped.clone()))),
    );
    let responder = tokio::spawn(run_reconcile_responder(
        fetch_rx,
        summary_rx,
        peer,
        sessions,
        changes_rx,
        Duration::from_secs(1),
        context,
    ));
    summaries.send(job).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(1), responses.recv())
        .await
        .unwrap()
        .unwrap();
    {
        let authorization = served.lock().unwrap();
        let state = authorization.get(&("peer".into(), FragmentId(0))).unwrap();
        assert_eq!(state.session_id, 7);
        assert_eq!(state.round_id, 9);
        assert!(state.advertised_keys.contains("key"));
    }
    drop(fetches);
    drop(summaries);
    responder.await.unwrap();
    assert!(
        !dropped.load(Ordering::Acquire),
        "queued response lost peer authority retention"
    );
    drop(frame);
    assert!(dropped.load(Ordering::Acquire));
    actor.stop_and_wait(None, None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_serving_writer_cannot_make_fetch_hold_the_served_round_registry() {
    use std::sync::Condvar;
    struct ReleaseWriter(Arc<(StdMutex<bool>, Condvar)>);
    impl Drop for ReleaseWriter {
        fn drop(&mut self) {
            *self.0 .0.lock().unwrap() = true;
            self.0 .1.notify_all();
        }
    }
    let (context, actor, _state, sessions, _changes, changes_rx) = fixture(false).await;
    let generation = context
        .router
        .route_with_generation(FragmentId(0))
        .unwrap()
        .0;
    context.served.lock().unwrap().insert(
        ("peer".into(), FragmentId(0)),
        ServedReconcileState {
            session_id: 7,
            round_id: 9,
            next_page_seq: 1,
            next_after_key: "key".into(),
            snapshot_revision: 5,
            snapshot_time_ms: 1,
            next_fetch_chunk_seq: 1,
            advertised_keys: HashSet::new(),
            route_generation: generation,
            recovery_epoch: Vec::new(),
            recovery_replica_digest: Vec::new(),
        },
    );
    let served = context.served.clone();
    let serving = context.serving.clone();
    let released = Arc::new((StdMutex::new(false), Condvar::new()));
    let release = ReleaseWriter(released.clone());
    let (ready, writer_ready) = oneshot::channel();
    let writer = std::thread::spawn(move || {
        let _writer = serving.write().unwrap();
        let _ = ready.send(());
        let mut released_guard = released.0.lock().unwrap();
        while !*released_guard {
            released_guard = released.1.wait(released_guard).unwrap();
        }
    });
    writer_ready.await.unwrap();
    let (peer, _frames, _responses) = test_peer_channel(2);
    let channel = peer.clone();
    let (fetches, fetch_rx) = mpsc::channel(1);
    let (summaries, summary_rx) = mpsc::channel(1);
    let responder = tokio::spawn(run_reconcile_responder(
        fetch_rx,
        summary_rx,
        peer,
        sessions,
        changes_rx,
        Duration::from_secs(1),
        context,
    ));
    fetches
        .send(ReconcileResponderJob::Fetch(ReconcileResponseJob {
            retention: None,
            peer: "peer".into(),
            session: InboundSession {
                session_id: 7,
                generation: 1,
            },
            actor: actor.clone(),
            fetch: ReconcileFetch {
                fragment: 0,
                session_id: 7,
                round_id: 9,
                chunk_seq: 0,
                keys: vec!["key".into()],
            },
            snapshot_time_ms: 1,
            route_generation: generation,
            recovery_epoch: Vec::new(),
            recovery_replica_digest: Vec::new(),
        }))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while fetches.capacity() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The worker reaches a blocking Serving read while the independent writer
    // holds it. The served-round guard must already be dropped; retaining it
    // here recreates the reader/worker/Serving-writer lock-order cycle.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(
            served.try_lock().is_ok(),
            "fetch retained served while waiting for Serving"
        );
        assert!(!responder.is_finished());
    }
    channel.cancel();
    drop(fetches);
    drop(summaries);
    drop(release);
    writer.join().unwrap();
    tokio::time::timeout(Duration::from_secs(1), responder)
        .await
        .unwrap()
        .unwrap();
    actor.stop_and_wait(None, None).await.unwrap();
}
