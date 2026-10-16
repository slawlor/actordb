// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Dispatcher argument defaults and caller-owned dependencies.

use super::*;
use crate::actor::test_support::test_state;
use crate::actor::KvWorkerArgs;
use std::collections::HashMap;
#[test]
fn actor_args_builders_retain_dependencies_and_priority_defaults() {
    let shared = Arc::new(test_state(HashMap::new()));
    let args = KvArgs::builder()
        .shared(shared.clone())
        .fragment(FragmentId(7))
        .worker_prefix("builder-defaults")
        .build();
    assert!(Arc::ptr_eq(&args.shared, &shared));
    assert_eq!(args.fragment, FragmentId(7));
    assert_eq!(args.worker_prefix, "builder-defaults");
    for (pool, expected) in [
        (&args.default_pool, (4, 10_000, None)),
        (&args.high_pool, (2, 5_000, None)),
        (&args.background_pool, (2, 5_000, Some(1000))),
    ] {
        assert_eq!(
            (pool.workers, pool.queue_cap, pool.rate_limit_per_sec),
            expected
        );
    }
    let custom = || PoolConfig::builder().workers(1).queue_cap(2).build();
    let overridden = KvArgs::builder()
        .shared(shared.clone())
        .fragment(FragmentId(8))
        .worker_prefix(String::from("builder-overrides"))
        .high_pool(custom())
        .default_pool(custom())
        .background_pool(custom())
        .build();
    for pool in [
        overridden.high_pool,
        overridden.default_pool,
        overridden.background_pool,
    ] {
        assert_eq!(
            (pool.workers, pool.queue_cap, pool.rate_limit_per_sec),
            (1, 2, None)
        );
    }
    let worker = KvWorkerArgs::builder().shared(shared.clone()).build();
    assert!(Arc::ptr_eq(&worker.shared, &shared));
    assert!(worker.pending.is_none());
    let pending = Arc::new(AtomicU64::new(0));
    let worker = KvWorkerArgs::builder()
        .shared(shared.clone())
        .pending(pending.clone())
        .build();
    assert!(Arc::ptr_eq(worker.pending.as_ref().unwrap(), &pending));
    assert!(KvWorkerArgs::builder()
        .shared(shared)
        .maybe_pending(None)
        .build()
        .pending
        .is_none());
}
