// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Supervisor regression groups retain access to their private runtime owner.

use super::*;
use crate::actor::maintenance_test_support::{GatedStore, ReleaseMaintenanceStore, ReleaseStore};
use crate::actor::{ApplyOutcome, KvWorker, KvWorkerArgs, ReplicatedOp};
use crate::store::StoreError;
use ractor::registry::where_is;
use std::sync::atomic::Ordering;
use std::sync::Mutex as StdMutex;

#[path = "tests/builders.rs"]
mod builders;
#[path = "tests/health.rs"]
mod health;
#[path = "tests/lifecycle.rs"]
mod lifecycle;
