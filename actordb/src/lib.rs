// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

#![deny(warnings)]
#![warn(unused_imports)]
#![warn(unsafe_code)]
// #![warn(missing_docs)]
// #![warn(unused_crate_dependencies)]
// #![cfg_attr(docsrs, feature(doc_cfg))]

/// The actor-first heart of the database: per-fragment dispatchers
/// routing to worker pools over shared fragment state, with
/// last-writer-wins merge, TTLs, and tombstones.
pub mod actor;

/// A database node: per-fragment supervision subtrees plus the peer mesh
/// that replicates writes between nodes. Supports static sharding with
/// one-hop routing.
pub mod node;

/// Pluggable network transport for the peer mesh. The default is plain
/// TCP; implement `Network` for TLS or other transports.
pub mod network;

/// Protobuf wire protocol (generated via [prost])
pub mod proto;

/// Pluggable durable storage for a node's local copy. The default is
/// an in-memory no-op; enable the `rocksdb` feature for RocksDB.
pub mod store;

/// Sharding: deterministic key → fragment mapping.
pub mod shard;

/// Per-fragment supervision: dispatcher pool, TTL cleaner, snapshotter.
pub mod supervision;

/// Phase 5: async sender/receiver replication (per-fragment, per-peer
/// senders with batching, acks, and reconciliation).
pub mod replication;

#[cfg(test)]
pub(crate) mod common_test;
