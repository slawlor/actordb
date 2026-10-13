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

/// The actor-first heart of the database: a `KvActor` owns each node's
/// local copy of the keyspace. All state transitions happen inside the
/// actor's message handler.
pub mod actor;

/// A database node: a `KvActor` plus the Tokio TCP mesh that replicates
/// writes between nodes with epidemic broadcast + last-writer-wins merge.
pub mod node;

/// Protobuf wire protocol (generated via [prost])
pub mod proto;

#[cfg(test)]
pub(crate) mod common_test;
