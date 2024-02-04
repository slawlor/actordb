// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Protobuf specifications for over-the-wire intercommuncation
//! between nodes. Generated via [prost]

/// Meta types which include all base network protocol message types
pub mod meta {
    include!(concat!(env!("OUT_DIR"), "/meta.rs"));
}

pub use meta::Hello;
