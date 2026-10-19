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
/// routing to worker pools, supervisors, and maintenance actors over shared fragment state, with
/// last-writer-wins merge, TTLs, and tombstones.
pub mod actor;

/// A database node: per-fragment supervision subtrees plus the peer mesh
/// that replicates writes between nodes. Supports static sharding with
/// one-hop routing.
pub mod node;

/// Application listener for remote SDKs, independent of the peer mesh.
pub mod client_server;

/// Bounded SQL query coordination over fragment views.
pub mod query;

/// Pluggable network transport for the peer mesh. The default is plain
/// TCP; implement `Network` for TLS or other transports.
pub mod network;

/// Protobuf wire protocol (generated via [prost])
pub mod proto;

/// Pluggable durable storage for a node's local copy. The default crate ships
/// no storage backend; its Node still uses a volatile actor cache. Enable
/// `rocksdb` for the offered durable backend or the independent `memory`
/// feature for the no-op store test double. Storage traits and the fragment
/// namespace wrapper are always available for caller-supplied stores.
pub mod store;

/// Sharding: deterministic key → fragment mapping.
pub mod shard;

/// Compatibility reexports of the supervisor and maintenance actors in [`actor`].
pub mod supervision;

/// Phase 5: async sender/receiver replication (per-fragment, per-peer
/// senders with batching, acks, and reconciliation).
pub mod replication;

/// Phase 8: dynamic placement controller — runtime fragment lifecycle,
/// chunked bootstrap, and controller-sequenced migration.
pub mod placement;

/// Opt-in, stable quorum commits, conditional mutations, and durable changes.
#[cfg(feature = "quorum")]
pub mod quorum;

mod runtime_tasks;

// Shared protobuf framing limits and encoding helpers. Node and replication
// consume this module as part of the v3 paginated anti-entropy protocol.
pub(crate) mod wire;

#[cfg(test)]
pub(crate) mod common_test {
    #![allow(dead_code)]

    use std::future::Future;

    use ractor::concurrency::sleep;
    use ractor::concurrency::Duration;
    use ractor::concurrency::Instant;

    pub async fn periodic_check<F>(check: F, timeout: Duration)
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if check() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(check());
    }

    pub async fn periodic_async_check<F, Fut>(check: F, timeout: Duration)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = bool>,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if check().await {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(check().await);
    }
}
