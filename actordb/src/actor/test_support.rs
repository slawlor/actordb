// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Small fixtures shared by actor-owned tests.

use std::collections::HashMap;
use std::time::Duration;

use super::{FragmentState, ReplicatedOp, VersionedValue};
pub(super) fn test_state(cache: HashMap<String, VersionedValue>) -> FragmentState {
    FragmentState::new(cache, None, 100, Duration::from_secs(3600))
}

pub(super) fn test_op(key: &str, value: VersionedValue) -> ReplicatedOp {
    ReplicatedOp {
        key: key.to_string(),
        value,
        seq: 0,
        boot_id: 0,
    }
}
