// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Periodic and explicit flushing of the fragment write buffer.

use std::sync::Arc;
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef};

use super::maintenance::{coalesced_ticker, TickerTask};
use super::{MaintenanceTrigger, DEFAULT_SNAPSHOT_INTERVAL};
use crate::actor::FragmentState;

/// Messages for the [`Snapshotter`].
#[non_exhaustive]
pub enum SnapshotterMessage {
    /// Flush the write buffer to the store now. Explicit manual messages
    /// are not covered by the automatic ticker's admission bound.
    Snap,
    /// Internal, coalesced automatic flush. Direct callers use `Snap`.
    #[doc(hidden)]
    AutomaticSnap(MaintenanceTrigger),
    #[cfg(test)]
    CompletionBarrier(ractor::RpcReplyPort<()>),
}

/// Background actor: periodically flushes the fragment's write buffer,
/// so the durable store holds a fresh snapshot even under a trickle of
/// writes that never fills the batch.
pub struct Snapshotter;

/// Shared state is required; the default periodic flush interval is 60 seconds.
#[derive(Clone, bon::Builder)]
pub struct SnapshotterArgs {
    pub shared: Arc<FragmentState>,
    #[builder(default = DEFAULT_SNAPSHOT_INTERVAL)]
    pub interval: Duration,
}

pub struct SnapshotterState {
    shared: Arc<FragmentState>,
    ticker: TickerTask,
}

impl Actor for Snapshotter {
    type Msg = SnapshotterMessage;
    type State = SnapshotterState;
    type Arguments = SnapshotterArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // `interval()` yields its first tick immediately. Start at one full
        // interval so a write arriving just after startup is not flushed by a
        // delayed "initial" tick.
        let ticker = coalesced_ticker(
            myself,
            args.interval,
            false,
            SnapshotterMessage::AutomaticSnap,
        );
        Ok(SnapshotterState {
            shared: args.shared,
            ticker,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        state.ticker.stop().await;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SnapshotterMessage::Snap => state.shared.flush().await,
            SnapshotterMessage::AutomaticSnap(_trigger) => state.shared.flush().await,
            #[cfg(test)]
            SnapshotterMessage::CompletionBarrier(reply) => {
                let _ = reply.send(());
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Automatic flush persistence and lease rearming under a paused timer.

    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    use crate::actor::{ApplyOutcome, ReplicatedOp, VersionedValue};
    use crate::store::{KvStore, StoreError};

    type SnapshotWrites = Vec<(Vec<u8>, VersionedValue)>;

    #[derive(Default)]
    struct RecordingSnapshotStore {
        batches: StdMutex<Vec<SnapshotWrites>>,
        completed: tokio::sync::Notify,
    }

    impl KvStore for RecordingSnapshotStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }

        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.batches.lock().unwrap().push(writes.to_vec());
            self.completed.notify_one();
            Ok(())
        }
    }

    impl RecordingSnapshotStore {
        async fn wait_for_batches(&self, expected: usize) {
            loop {
                let completed = self.completed.notified();
                if self.batches.lock().unwrap().len() >= expected {
                    return;
                }
                completed.await;
            }
        }
    }

    async fn completed_automatic_snap(actor: &ActorRef<SnapshotterMessage>) {
        // FIFO processing fences the completed flush and its trigger drop.
        // A paused-clock timeout cannot represent blocking-thread progress.
        assert!(matches!(
            actor
                .call(SnapshotterMessage::CompletionBarrier, None)
                .await
                .unwrap(),
            ractor::rpc::CallResult::Success(())
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn automatic_snapshot_flushes_buffered_values_and_rearms_after_completion() {
        let store = Arc::new(RecordingSnapshotStore::default());
        let shared = Arc::new(FragmentState::new(
            HashMap::new(),
            Some(store.clone()),
            usize::MAX,
            Duration::from_secs(3600),
        ));
        let first = VersionedValue::new(b"first".to_vec(), 10, "writer".into());
        assert_eq!(
            shared
                .apply(&ReplicatedOp {
                    key: "first".into(),
                    value: first.clone(),
                    seq: 1,
                    boot_id: 1,
                })
                .await
                .unwrap(),
            ApplyOutcome::Applied
        );
        let interval = Duration::from_millis(10);
        let (snapshotter, handle) = Snapshotter::spawn(
            None,
            Snapshotter,
            SnapshotterArgs {
                shared: shared.clone(),
                interval,
            },
        )
        .await
        .unwrap();
        // Neither buffered apply nor the ticker's startup may flush. Let the
        // actual actor/ticker run without advancing to their first deadline.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(store.batches.lock().unwrap().is_empty());
        tokio::time::advance(interval).await;
        store.wait_for_batches(1).await;
        completed_automatic_snap(&snapshotter).await;
        assert_eq!(
            store.batches.lock().unwrap().clone(),
            vec![vec![(b"first".to_vec(), first.clone())]]
        );

        // A second buffered expiry floor needs a later automatic trigger:
        // persisting the first batch must have released the coalescing lease.
        let mut expired = VersionedValue::new(b"expired".to_vec(), 20, "writer".into());
        expired.expires_at_ms = Some(1);
        let floor = expired.expiry_tombstone();
        assert_eq!(
            shared
                .apply(&ReplicatedOp {
                    key: "second".into(),
                    value: expired,
                    seq: 2,
                    boot_id: 1,
                })
                .await
                .unwrap(),
            ApplyOutcome::Expired
        );
        assert_eq!(store.batches.lock().unwrap().len(), 1);
        tokio::time::advance(interval).await;
        store.wait_for_batches(2).await;
        completed_automatic_snap(&snapshotter).await;
        assert_eq!(
            store.batches.lock().unwrap().clone(),
            vec![
                vec![(b"first".to_vec(), first.clone())],
                vec![(b"second".to_vec(), floor.clone())],
            ]
        );
        assert_eq!(shared.get("first").await, Some(first));
        assert_eq!(shared.get("second").await, None);
        assert_eq!(
            shared.snapshot_with_revision().await.0.get("second"),
            Some(&floor)
        );
        snapshotter.drain().unwrap();
        handle.await.unwrap();
        tokio::time::advance(interval * 2).await;
        assert_eq!(store.batches.lock().unwrap().len(), 2);
    }
}
