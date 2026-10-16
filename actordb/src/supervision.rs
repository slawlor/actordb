// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Compatibility imports for actors canonically exported by [`crate::actor`].
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

pub use crate::actor::{
    Cleaner, CleanerArgs, CleanerArgsBuilder, CleanerMessage, CleanerState, FragmentHealth,
    FragmentShutdownError, FragmentSupervisor, MaintenanceTrigger, Snapshotter, SnapshotterArgs,
    SnapshotterArgsBuilder, SnapshotterMessage, SnapshotterState, SupervisorArgs,
    SupervisorArgsBuilder, SupervisorMessage, SupervisorState,
};
