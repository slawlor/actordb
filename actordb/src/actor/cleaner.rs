// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Automatic paged TTL materialization and explicit manual sweeps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use ractor::{Actor, ActorProcessingErr, ActorRef, ActorStatus};

use super::maintenance::{coalesced_ticker, TickerTask};
use super::{MaintenanceTrigger, DEFAULT_CLEAN_INTERVAL};
use crate::actor::{now_ms, ExpiryPass, FragmentState};
use crate::shard::FragmentId;

/// Messages for the [`Cleaner`].
#[non_exhaustive]
pub enum CleanerMessage {
    /// Perform a full sweep for expired keys now. Explicit manual messages
    /// are not covered by the automatic ticker's admission bound.
    Sweep,
    /// Internal, coalesced automatic pass. Direct callers use `Sweep`.
    #[doc(hidden)]
    AutomaticSweep(MaintenanceTrigger),
}

/// Background actor: periodically converts TTL-expired values into
/// versioned tombstones. Reads already hide expired values, but retaining the
/// version floor prevents older replicas from resurrecting them.
pub struct Cleaner;

/// The fragment and shared state are required; the default sweep interval is
/// 30 seconds. Legacy compatibility fields may be omitted.
#[derive(Clone, bon::Builder)]
pub struct CleanerArgs {
    /// Legacy compatibility field. The cleaner no longer uses fragment ids.
    pub fragment: FragmentId,
    pub shared: Arc<FragmentState>,
    #[builder(default = DEFAULT_CLEAN_INTERVAL)]
    pub interval: Duration,
    /// Legacy compatibility field. Ignored; tombstones are retained.
    pub tombstone_gc_age: Option<Duration>,
    /// Legacy compatibility field. Ignored because time-based collection was
    /// removed rather than guarded by transient sender state.
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
}

pub struct CleanerState {
    shared: Arc<FragmentState>,
    ticker: TickerTask,
    pass: Option<ExpiryPass>,
}

impl Actor for Cleaner {
    type Msg = CleanerMessage;
    type State = CleanerState;
    type Arguments = CleanerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let ticker = coalesced_ticker(myself, args.interval, true, CleanerMessage::AutomaticSweep);
        Ok(CleanerState {
            shared: args.shared,
            ticker,
            pass: None,
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
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CleanerMessage::Sweep => {
                let now = now_ms();
                state.shared.expire_values(now).await?;
                Ok(())
            }
            CleanerMessage::AutomaticSweep(trigger) => {
                if myself.get_status() >= ActorStatus::Draining {
                    state.pass = None;
                    return Ok(());
                }
                let pass = state.pass.get_or_insert_with(|| ExpiryPass::new(now_ms()));
                let page = state.shared.expire_values_page(pass).await?;
                tracing::trace!(
                    visited = page.visited,
                    converted = page.converted,
                    work_bytes = page.work_bytes,
                    done = page.done,
                    "automatic expiry page"
                );
                if page.done || myself.get_status() >= ActorStatus::Draining {
                    state.pass = None;
                } else {
                    // A ready ractor mailbox can otherwise consume several
                    // pages before Tokio's cooperative budget expires. Yield
                    // with no data locks held and retain the unique admission
                    // lease; this is an opportunity, not a priority guarantee.
                    tokio::task::yield_now().await;
                    if myself.get_status() >= ActorStatus::Draining {
                        state.pass = None;
                    } else {
                        // Move, rather than opening a new slot between pages.
                        // Failed typed sends drop the lease with the message.
                        let _ = myself.send_message(CleanerMessage::AutomaticSweep(trigger));
                    }
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Paged automatic expiry and explicit snapshot persistence.

    use super::*;
    use crate::actor::maintenance_test_support::{GatedStore, ReleaseMaintenanceStore};
    use crate::actor::VersionedValue;
    use crate::actor::{Snapshotter, SnapshotterArgs, SnapshotterMessage};

    #[tokio::test]
    async fn automatic_cleaner_completes_multiple_pages_and_manual_snap_flushes_markers() {
        let store = Arc::new(GatedStore::default());
        let _release = ReleaseMaintenanceStore(store.clone());
        store.release();
        let values = (0..600)
            .map(|i| {
                let mut value = VersionedValue::new(vec![1], 10, "writer".into());
                value.expires_at_ms = Some(1);
                (format!("k{i:04}"), value)
            })
            .collect();
        let shared = Arc::new(FragmentState::new(
            values,
            Some(store.clone()),
            usize::MAX,
            Duration::from_secs(3600),
        ));
        let (cleaner, _) = Cleaner::spawn(
            None,
            Cleaner,
            CleanerArgs {
                fragment: FragmentId(0),
                shared: shared.clone(),
                interval: Duration::from_secs(3600),
                tombstone_gc_age: None,
                reset_senders: Arc::new(Mutex::new(HashMap::new())),
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while shared.snapshot_with_revision().await.1 != 600 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            store.entries.lock().unwrap().is_empty(),
            "idle buffer flushed before explicit snapshot"
        );
        assert_eq!(shared.expire_values(now_ms()).await.unwrap(), 0);
        cleaner.send_message(CleanerMessage::Sweep).unwrap();
        cleaner.drain().unwrap();
        cleaner.wait(None).await.unwrap();
        let (snapshotter, _) = Snapshotter::spawn(
            None,
            Snapshotter,
            SnapshotterArgs {
                shared,
                interval: Duration::from_secs(3600),
            },
        )
        .await
        .unwrap();
        snapshotter.send_message(SnapshotterMessage::Snap).unwrap();
        snapshotter.drain().unwrap();
        snapshotter.wait(None).await.unwrap();
        let entries = store.entries.lock().unwrap();
        assert_eq!(entries.len(), 600);
        assert!(entries
            .iter()
            .all(|(_, value)| value.is_tombstone && value.expires_at_ms == Some(1)));
    }
}
