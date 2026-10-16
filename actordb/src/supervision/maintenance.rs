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
#[path = "tests/maintenance.rs"]
mod tests;
