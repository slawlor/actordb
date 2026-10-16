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
        }
    }
}

#[cfg(test)]
#[path = "tests/snapshotter.rs"]
mod tests;
