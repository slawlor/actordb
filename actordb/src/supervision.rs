// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Per-fragment supervision.
//!
//! Every fragment runs under a [`FragmentSupervisor`](crate::supervision::FragmentSupervisor),
//! a ractor supervisor owning the fragment's background subsystem:
//!
//! - the [`KvActor`](crate::actor::KvActor) dispatcher and its worker pool
//!   (Phase 3), which self-supervises worker restarts;
//! - the [`Cleaner`](crate::supervision::Cleaner), which materializes TTL-expiry tombstones on an interval;
//! - the [`Snapshotter`](crate::supervision::Snapshotter), which periodically flushes the write
//!   buffer so the durable store holds a fresh snapshot.
//!
//! A dead child is restarted in place without touching sibling fragments.
//! The supervisor also answers [`FragmentHealth`](crate::supervision::FragmentHealth) queries, feeding the
//! observability layer and (later) the Phase 8 placement controller.

mod cleaner;
mod maintenance;
mod snapshotter;
mod supervisor;

pub use cleaner::{Cleaner, CleanerArgs, CleanerArgsBuilder, CleanerMessage, CleanerState};
pub use maintenance::MaintenanceTrigger;
pub use snapshotter::{
    Snapshotter, SnapshotterArgs, SnapshotterArgsBuilder, SnapshotterMessage, SnapshotterState,
};
pub use supervisor::{
    FragmentHealth, FragmentShutdownError, FragmentSupervisor, SupervisorArgs,
    SupervisorArgsBuilder, SupervisorMessage, SupervisorState,
};

pub(crate) use supervisor::{
    FragmentStartupStorageError, DEFAULT_CLEAN_INTERVAL, DEFAULT_FLUSH_BATCH_SIZE,
    DEFAULT_FLUSH_INTERVAL, DEFAULT_SNAPSHOT_INTERVAL,
};

#[cfg(test)]
mod test_support;
