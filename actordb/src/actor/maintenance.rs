// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Shared single-job ticker admission and opaque maintenance ownership.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

use ractor::{ActorRef, ActorStatus};

/// Opaque ownership of one admitted automatic maintenance job.
///
/// Public only because maintenance messages are public. The unique lease
/// moves through cleaner continuations; manual messages cannot release it.
#[doc(hidden)]
pub struct MaintenanceTrigger {
    pending: Arc<AtomicBool>,
}

impl Drop for MaintenanceTrigger {
    fn drop(&mut self) {
        self.pending.store(false, Ordering::Release);
    }
}

pub(super) struct TickerTask(Option<JoinHandle<()>>);

impl TickerTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    pub(super) async fn stop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for TickerTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// At most one automatic job is queued or executing, including every page
/// of a cleaner pass. Skip alone would still enqueue ticks behind a stall.
pub(super) fn coalesced_ticker<M: ractor::Message>(
    myself: ActorRef<M>,
    interval: Duration,
    immediate: bool,
    message: fn(MaintenanceTrigger) -> M,
) -> TickerTask {
    let start = tokio::time::Instant::now();
    let mut ticker =
        tokio::time::interval_at(if immediate { start } else { start + interval }, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let pending = Arc::new(AtomicBool::new(false));
    TickerTask::new(tokio::spawn(async move {
        loop {
            ticker.tick().await;
            if myself.get_status() >= ActorStatus::Draining {
                break;
            }
            if pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let trigger = MaintenanceTrigger {
                pending: pending.clone(),
            };
            if myself.send_message(message(trigger)).is_err() {
                break;
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    //! Coalesced ticks, active cancellation and rejected-message lease release.

    use super::*;
    use std::sync::Mutex as StdMutex;

    use ractor::rpc::CallResult;
    use ractor::{Actor, ActorProcessingErr, RpcReplyPort};

    enum TickTestMessage {
        Automatic(MaintenanceTrigger),
        Barrier(RpcReplyPort<()>),
    }

    struct TickTestActor;

    struct TickTestState {
        entered: Arc<std::sync::atomic::AtomicUsize>,
        gate: Arc<tokio::sync::Notify>,
        pending: Arc<StdMutex<Option<Arc<AtomicBool>>>>,
    }

    impl Actor for TickTestActor {
        type Msg = TickTestMessage;
        type State = TickTestState;
        type Arguments = TickTestState;

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                TickTestMessage::Automatic(trigger) => {
                    *state.pending.lock().unwrap() = Some(trigger.pending.clone());
                    state.entered.fetch_add(1, Ordering::SeqCst);
                    state.gate.notified().await;
                    drop(trigger);
                }
                TickTestMessage::Barrier(reply) => {
                    let _ = reply.send(());
                }
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn automatic_maintenance_ticks_coalesce_stalls_and_release_on_terminal_paths() {
        for immediate in [true, false] {
            let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Notify::new());
            let pending = Arc::new(StdMutex::new(None));
            let (actor, handle) = TickTestActor::spawn(
                None,
                TickTestActor,
                TickTestState {
                    entered: entered.clone(),
                    gate: gate.clone(),
                    pending: pending.clone(),
                },
            )
            .await
            .unwrap();
            let mut ticker = coalesced_ticker(
                actor.clone(),
                Duration::from_millis(2),
                immediate,
                TickTestMessage::Automatic,
            );
            tokio::time::advance(Duration::from_millis(2)).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while entered.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let lease = pending.lock().unwrap().clone().unwrap();
            assert!(lease.load(Ordering::Acquire));
            // Deterministic interval ticks while the actor handler is stalled. Stopping
            // the producer before release makes the following FIFO barrier
            // prove there is no hidden queued automatic backlog.
            for _ in 0..20 {
                tokio::time::advance(Duration::from_millis(2)).await;
                tokio::task::yield_now().await;
            }
            ticker.stop().await;
            gate.notify_one();
            assert!(matches!(
                actor
                    .call(TickTestMessage::Barrier, Some(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                CallResult::Success(())
            ));
            assert_eq!(entered.load(Ordering::SeqCst), 1);
            assert!(!lease.load(Ordering::Acquire));

            // Cancellation of an active handler retains no abandoned latch.
            lease.store(true, Ordering::Release);
            actor
                .send_message(TickTestMessage::Automatic(MaintenanceTrigger {
                    pending: lease.clone(),
                }))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while entered.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            actor.kill();
            handle.await.unwrap();
            assert!(!lease.load(Ordering::Acquire));
            lease.store(true, Ordering::Release);
            let result = actor.send_message(TickTestMessage::Automatic(MaintenanceTrigger {
                pending: lease.clone(),
            }));
            assert!(result.is_err());
            drop(result); // failed typed send owns the rejected message
            assert!(!lease.load(Ordering::Acquire));
        }
    }
}
