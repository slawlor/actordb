//! Deterministic, offline capacity and regional placement planning.
//!
//! Estimates are only as good as the host profile and workload supplied. The
//! default profile is a conservative planning assumption, not a benchmark or
//! an SLO. See the crate README for the accounting model and calibration.
//!
//! Existing deployments keep their fragment count. A recommendation is a
//! target, never permission to revoke a serving replica. Use
//! [`PlacementTransition`] with cluster-wide readiness evidence to sequence
//! overlap before publishing removals through your placement controller.

mod engine;
mod model;
mod transition;

pub use engine::{evaluate, recommend};
pub use model::*;
pub use transition::PlacementTransition;
