// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Phase 8: dynamic placement controller.
//!
//! Static placement (`NodeConfig::fragment_map`) fixes every fragment's
//! replica set at startup. Dynamic placement replaces the fixed map
//! with a [`PlacementController`](crate::placement::PlacementController):
//! a source of truth for "which nodes
//! hold which fragments" that each node's placement driver polls and
//! converges toward at runtime.
//!
//! The migration protocol is controller-sequenced with overlap:
//!
//! ```text
//!   1. Controller adds the new owner:  {F: [A]} -> {F: [A, B]}.
//!      B's driver spawns the fragment subtree and bootstraps state
//!      from A via the Phase 5 reconciler (chunked, bounded).
//!   2. Controller waits until B reports F as Serving, then removes
//!      the old owner: {F: [A, B]} -> {F: [B]}.
//!      A's driver drains its replication senders, then stops the
//!      subtree.
//! ```
//!
//! At every step at least one node serves the fragment, and last-
//! writer-wins merge keeps the overlap window consistent, so no write
//! is lost. Nodes that don't serve a fragment forward to a current
//! replica holder; a node that receives a forwarded op for a fragment
//! it no longer serves re-forwards it (hop-limited) instead of
//! dropping it, which covers the window where peers' placement maps
//! disagree.
//!
//! Dynamic placement is opt-in: set
//! [`NodeConfig::placement_controller`](crate::node::NodeConfig::placement_controller).
//! With no controller configured, placement is static; established replica
//! sessions still use the same correlated reconciliation engine.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::RwLock;

use crate::shard::FragmentId;
use crate::wire::MAX_WIRE_NODE_ID_BYTES;

/// Source of truth for fragment placement.
///
/// Implement this to drive placement from outside the node: a cluster
/// manager, an operator CLI, or a test script. The node's placement
/// driver polls [`desired_placement`](PlacementController::desired_placement)
/// and converges the node's local state (spawn/drain fragment
/// subtrees, update routing) toward it.
///
/// Sequencing rule for migrations: move through an overlap state. To
/// move fragment `F` from `A` to `B`, first publish `{F: [A, B]}`,
/// wait until `B` reports `F` as
/// [`Serving`](FragmentPlacementState::Serving) (via
/// [`Node::placement_report`](crate::node::Node::placement_report) or
/// the [`on_fragment_ready`](PlacementController::on_fragment_ready)
/// callback), then publish `{F: [B]}`. Never publish a placement where
/// a fragment has no replicas, and never remove the old owner before
/// the new one is serving — the driver treats the controller's map as
/// authoritative and will tear down local state for revoked fragments.
pub trait PlacementController: Debug + Send + Sync {
    /// Desired placement: fragment -> replica node ids. Must cover
    /// every fragment `0..fragment_count` with a non-empty, duplicate-free
    /// replica list of valid 1-to-255-byte node ids. The driver validates this
    /// each poll and keeps the current placement on invalid output (fail-safe).
    fn desired_placement(&self) -> HashMap<FragmentId, Vec<String>>;

    /// Called when the node's observed peer set changes (a peer
    /// connects or disconnects). Default: no-op. External controllers
    /// can use this to trigger re-evaluation instead of polling.
    fn on_membership_change(&self, _members: &[String]) {}

    /// Called when this node finishes bootstrapping a newly assigned
    /// fragment and starts serving it. Default: no-op. In-process
    /// controllers can use this to sequence migrations (e.g. revoke
    /// the old owner once the new one is ready).
    fn on_fragment_ready(&self, _fragment: FragmentId) {}
}

/// Static placement as a controller: always returns the configured
/// map. Used internally so the dynamic code path can share validation
/// logic; nodes without an explicit controller keep the legacy static
/// startup path untouched.
#[derive(Debug, Clone)]
pub struct StaticPlacement {
    map: HashMap<FragmentId, Vec<String>>,
}

impl StaticPlacement {
    /// Build from a fragment map (same shape as
    /// `NodeConfig::fragment_map`).
    pub fn new(map: HashMap<FragmentId, Vec<String>>) -> Self {
        Self { map }
    }
}

impl PlacementController for StaticPlacement {
    fn desired_placement(&self) -> HashMap<FragmentId, Vec<String>> {
        self.map.clone()
    }
}

/// Manually driven placement for tests and operators. The map is
/// interior-mutable so a test (or an external control loop holding the
/// `Arc`) can sequence migrations: publish overlap, wait for the new
/// owner to report serving, then revoke the old owner.
#[derive(Debug, Default)]
pub struct ManualPlacement {
    map: RwLock<HashMap<FragmentId, Vec<String>>>,
}

impl ManualPlacement {
    /// Create with an initial placement.
    pub fn new(map: HashMap<FragmentId, Vec<String>>) -> Self {
        Self {
            map: RwLock::new(map),
        }
    }

    /// Replace the desired placement. Takes effect on the driver's next
    /// poll (see `NodeConfig::placement_poll_interval`).
    pub fn set_placement(&self, map: HashMap<FragmentId, Vec<String>>) {
        *self.map.write().unwrap() = map;
    }

    /// Current desired placement (for assertions).
    pub fn placement(&self) -> HashMap<FragmentId, Vec<String>> {
        self.map.read().unwrap().clone()
    }
}

impl PlacementController for ManualPlacement {
    fn desired_placement(&self) -> HashMap<FragmentId, Vec<String>> {
        self.placement()
    }
}

/// Runtime lifecycle state of a fragment on this node, for
/// [`PlacementReport`] and the migration protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentPlacementState {
    /// Subtree spawned, bootstrapping state from a replica holder via
    /// the Phase 5 reconciler. Not yet authoritative for reads/writes;
    /// local ops are forwarded to the previous owner.
    Bootstrapping,
    /// Authoritative: reads and writes are served locally.
    Serving,
    /// Placement revoked: new operations relay to the current owner while
    /// replication senders drain, then the subtree is stopped.
    Draining,
    /// Subtree stopped; the fragment is no longer served here.
    Stopped,
}

/// A placement health snapshot for [`Node::placement_report`](crate::node::Node::placement_report).
#[derive(Debug, Clone)]
pub struct PlacementReport {
    /// This node's authoritative placement map (what it routes by).
    pub placement: HashMap<FragmentId, Vec<String>>,
    /// Per-fragment lifecycle state for fragments this node serves or
    /// recently served.
    pub states: HashMap<FragmentId, FragmentPlacementState>,
    /// Whether dynamic placement is enabled on this node.
    pub dynamic: bool,
}

/// Validate a desired placement: exactly the fragment range
/// `0..fragment_count`, each with a non-empty, duplicate-free list of node ids
/// that can be represented by the peer handshake.
pub fn validate_placement(
    map: &HashMap<FragmentId, Vec<String>>,
    fragment_count: usize,
) -> Result<(), String> {
    if let Some(fragment) = map
        .keys()
        .find(|fragment| fragment.0 as usize >= fragment_count)
    {
        return Err(format!("placement contains out-of-range {fragment}"));
    }
    for i in 0..fragment_count {
        let frag = FragmentId(i as u32);
        let Some(replicas) = map.get(&frag).filter(|replicas| !replicas.is_empty()) else {
            return Err(format!("placement leaves {frag} with no replicas"));
        };
        let mut unique = HashSet::with_capacity(replicas.len());
        for replica in replicas {
            if replica.is_empty() || replica.len() > MAX_WIRE_NODE_ID_BYTES {
                return Err(format!(
                    "placement {frag} has a replica id outside 1 to {MAX_WIRE_NODE_ID_BYTES} bytes"
                ));
            }
            if !unique.insert(replica) {
                return Err(format!(
                    "placement {frag} contains duplicate replica {replica:?}"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_placement_returns_map() {
        let mut map = HashMap::new();
        map.insert(FragmentId(0), vec!["a".to_string()]);
        let c = StaticPlacement::new(map.clone());
        assert_eq!(c.desired_placement(), map);
    }

    #[test]
    fn manual_placement_is_mutable() {
        let mut map = HashMap::new();
        map.insert(FragmentId(0), vec!["a".to_string()]);
        let c = ManualPlacement::new(map);
        assert_eq!(c.desired_placement()[&FragmentId(0)], vec!["a".to_string()]);
        let mut map2 = HashMap::new();
        map2.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
        c.set_placement(map2.clone());
        assert_eq!(c.desired_placement(), map2);
    }

    #[test]
    fn validate_rejects_missing_fragment() {
        let map = HashMap::new();
        assert!(validate_placement(&map, 2).is_err());
    }

    #[test]
    fn validate_rejects_empty_replicas() {
        let mut map = HashMap::new();
        map.insert(FragmentId(0), vec!["a".to_string()]);
        map.insert(FragmentId(1), vec![]);
        assert!(validate_placement(&map, 2).is_err());
    }

    #[test]
    fn validate_rejects_unrepresentable_duplicate_and_out_of_range_replicas() {
        for map in [
            HashMap::from([(FragmentId(0), vec![String::new()])]),
            HashMap::from([(FragmentId(0), vec!["x".repeat(256)])]),
            HashMap::from([(FragmentId(0), vec!["a".to_string(), "a".to_string()])]),
            HashMap::from([
                (FragmentId(0), vec!["a".to_string()]),
                (FragmentId(1), vec!["b".to_string()]),
            ]),
        ] {
            assert!(validate_placement(&map, 1).is_err());
        }
    }

    #[test]
    fn validate_accepts_full_map() {
        let mut map = HashMap::new();
        map.insert(FragmentId(0), vec!["a".to_string()]);
        map.insert(FragmentId(1), vec!["b".to_string(), "a".to_string()]);
        assert!(validate_placement(&map, 2).is_ok());
    }
}
