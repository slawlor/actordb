//! Regional sender selection and fair node-wide concurrency ownership.

use super::{SenderConfig, SenderRuntimeOptions};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// Opt-in regional profiles and node-wide cross-region concurrency limits.
/// Profiles replace ordinary sender tuning; the Node's required ACK durability
/// remains a minimum. Missing local/peer labels classify the peer cross-region.
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
pub struct RegionalReplicationOptions {
    /// Higher concurrency and shorter batching for known same-region peers.
    #[builder(default = SenderConfig::builder().max_in_flight(16).batch_interval(Duration::from_millis(10)).build())]
    pub in_region: SenderConfig,
    /// Lower concurrency and larger batches for cross-region/unlabeled peers.
    #[builder(default = SenderConfig::builder().max_in_flight(2).batch_size(256).build())]
    pub cross_region: SenderConfig,
    /// Shared outstanding ordinary batches across all outgoing WAN senders.
    #[builder(default = 16)]
    pub cross_region_max_in_flight: usize,
    /// Shared active outgoing reconciliation rounds, separate from batches so
    /// saturation of write traffic cannot consume all recovery capacity.
    #[builder(default = 2)]
    pub cross_region_max_reconcile_rounds: usize,
}

impl Default for RegionalReplicationOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RegionalReplicationOptions {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.in_region
            .validate()
            .map_err(|error| format!("in-region replication: {error}"))?;
        self.cross_region
            .validate()
            .map_err(|error| format!("cross-region replication: {error}"))?;
        if self.cross_region_max_in_flight == 0 || self.cross_region_max_reconcile_rounds == 0 {
            return Err("regional replication concurrency limits must be nonzero".into());
        }
        Ok(())
    }
}

/// Actual node-wide cross-region ownership at observation time. The batch and
/// round snapshots are independently synchronized, not one atomic global view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionalReplicationUsage {
    pub in_flight_batches: usize,
    pub max_in_flight_batches: usize,
    pub waiting_batch_senders: usize,
    pub active_reconcile_rounds: usize,
    pub max_reconcile_rounds: usize,
    pub waiting_reconcile_senders: usize,
}

#[derive(Debug)]
pub(crate) struct RegionalReplicationRuntime {
    local_node_id: String,
    regions: Arc<HashMap<String, String>>,
    options: RegionalReplicationOptions,
    streaming: Arc<TrafficBudget>,
    reconcile: Arc<TrafficBudget>,
}

impl RegionalReplicationRuntime {
    pub(crate) fn new(
        local_node_id: String,
        regions: Arc<HashMap<String, String>>,
        options: RegionalReplicationOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            local_node_id,
            regions,
            streaming: TrafficBudget::new(options.cross_region_max_in_flight),
            reconcile: TrafficBudget::new(options.cross_region_max_reconcile_rounds),
            options,
        })
    }

    pub(crate) fn select(
        &self,
        peer: &str,
        base: &SenderConfig,
        runtime: &mut SenderRuntimeOptions,
    ) -> SenderConfig {
        let same_region = self
            .regions
            .get(&self.local_node_id)
            .is_some_and(|region| self.regions.get(peer) == Some(region));
        let mut selected = if same_region {
            self.options.in_region.clone()
        } else {
            runtime.streaming_budget = Some(self.streaming.clone());
            runtime.reconcile_budget = Some(self.reconcile.clone());
            self.options.cross_region.clone()
        };
        selected.required_ack_durability = selected
            .required_ack_durability
            .max(base.required_ack_durability);
        selected
    }

    pub(crate) fn usage(&self) -> RegionalReplicationUsage {
        let streaming = self.streaming.state.lock().unwrap();
        let reconcile = self.reconcile.state.lock().unwrap();
        RegionalReplicationUsage {
            in_flight_batches: streaming.in_use,
            max_in_flight_batches: streaming.capacity,
            waiting_batch_senders: streaming.waiters.len(),
            active_reconcile_rounds: reconcile.in_use,
            max_reconcile_rounds: reconcile.capacity,
            waiting_reconcile_senders: reconcile.waiters.len(),
        }
    }
}

/// One FIFO ticket per eligible sender. Acquisition is nonblocking so sender
/// ACK/control/session processing continues while ordinary capacity is busy.
#[derive(Debug)]
pub(super) struct TrafficBudget {
    state: Mutex<TrafficState>,
    changes: watch::Sender<u64>,
}
#[derive(Debug)]
struct TrafficState {
    capacity: usize,
    in_use: usize,
    next_ticket: u64,
    waiters: VecDeque<u64>,
}
impl TrafficBudget {
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0);
        Arc::new(Self {
            state: Mutex::new(TrafficState {
                capacity,
                in_use: 0,
                next_ticket: 0,
                waiters: VecDeque::new(),
            }),
            changes: watch::channel(0).0,
        })
    }
    fn notify(&self) {
        self.changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }
    pub(super) fn ticket(self: &Arc<Self>) -> TrafficTicket {
        let mut state = self.state.lock().unwrap();
        // A wrapped counter skips IDs still queued; no two owners can share a
        // live ticket even across the theoretical u64 wrap boundary.
        loop {
            state.next_ticket = state.next_ticket.wrapping_add(1);
            if state.next_ticket != 0 && !state.waiters.contains(&state.next_ticket) {
                break;
            }
        }
        let id = state.next_ticket;
        state.waiters.push_back(id);
        TrafficTicket {
            budget: self.clone(),
            id,
            queued: true,
        }
    }
}

pub(super) struct TrafficTicket {
    budget: Arc<TrafficBudget>,
    id: u64,
    queued: bool,
}
impl TrafficTicket {
    pub(super) fn try_acquire(&mut self) -> Option<TrafficPermit> {
        let mut state = self.budget.state.lock().unwrap();
        if state.in_use == state.capacity || state.waiters.front() != Some(&self.id) {
            return None;
        }
        state.waiters.pop_front();
        state.in_use += 1;
        self.queued = false;
        drop(state);
        self.budget.notify();
        Some(TrafficPermit {
            budget: self.budget.clone(),
        })
    }
}
impl Drop for TrafficTicket {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }
        let mut state = self.budget.state.lock().unwrap();
        if let Some(position) = state.waiters.iter().position(|id| *id == self.id) {
            state.waiters.remove(position);
        }
        drop(state);
        self.budget.notify();
    }
}

/// Lives with the actual in-flight batch or active round. RAII releases on ACK,
/// NACK/requeue, timeout, session replacement, send failure and task shutdown.
pub(super) struct TrafficPermit {
    budget: Arc<TrafficBudget>,
}
impl Drop for TrafficPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        debug_assert!(state.in_use > 0);
        state.in_use -= 1;
        drop(state);
        self.budget.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationAckDurability;

    #[tokio::test]
    async fn fifo_acquisition_cancellation_and_drop_preserve_credit() {
        let budget = TrafficBudget::new(1);
        let mut changes = budget.subscribe();
        let first = budget.ticket().try_acquire().unwrap();
        let waiting = budget.ticket();
        let mut later = budget.ticket();
        assert!(later.try_acquire().is_none());
        drop(waiting);
        assert!(later.try_acquire().is_none());
        drop(first);
        changes.changed().await.unwrap();
        let permit = later.try_acquire().unwrap();
        assert_eq!(budget.state.lock().unwrap().in_use, 1);
        drop(permit);
        assert_eq!(budget.state.lock().unwrap().in_use, 0);
        assert!(budget.state.lock().unwrap().waiters.is_empty());
    }

    #[test]
    fn unknown_labels_use_wan_limits_and_ack_floor_is_preserved() {
        let options = RegionalReplicationOptions::default();
        let runtime = RegionalReplicationRuntime::new(
            "a".into(),
            Arc::new(HashMap::from([
                ("a".into(), "west".into()),
                ("b".into(), "west".into()),
                ("c".into(), "east".into()),
            ])),
            options.clone(),
        );
        let base = SenderConfig::builder()
            .required_ack_durability(ReplicationAckDurability::StableCommitted)
            .build();
        let mut local = SenderRuntimeOptions::default();
        assert_eq!(runtime.select("b", &base, &mut local).max_in_flight, 16);
        assert!(local.streaming_budget.is_none());
        for peer in ["c", "unlabeled"] {
            let mut remote = SenderRuntimeOptions::default();
            let selected = runtime.select(peer, &base, &mut remote);
            assert_eq!(selected.max_in_flight, 2);
            assert_eq!(
                selected.required_ack_durability,
                ReplicationAckDurability::StableCommitted
            );
            assert!(Arc::ptr_eq(
                remote.streaming_budget.as_ref().unwrap(),
                &runtime.streaming
            ));
        }
        assert!(RegionalReplicationOptions::builder()
            .cross_region_max_in_flight(0)
            .build()
            .validate()
            .is_err());
    }
}
