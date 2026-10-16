// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! HLC ordering, future timestamp rejection, and persisted seeding.

use super::*;
#[test]
fn hlc_is_monotonic_and_observes_remote() {
    let mut hlc = HlcClock::new();
    let t1 = hlc.tick();
    let t2 = hlc.tick();
    assert!(t2 >= t1, "ticks must be monotonic");
    // Observing a far-future remote timestamp moves the clock past it
    // (unbounded by default, preserving legacy behavior).
    hlc.observe(t2 + 100_000).unwrap();
    let t3 = hlc.tick();
    assert!(t3 > t2 + 100_000, "clock must move past observed remote");
    // Observing a stale timestamp never moves the clock backward.
    hlc.observe(1).unwrap();
    let t4 = hlc.tick();
    assert!(t4 >= t3, "clock must never go backward");
}

#[test]
fn hlc_rejects_far_future_remote_timestamps() {
    // Adversarial review #5: a far-future timestamp must not
    // permanently poison the clock.
    let mut hlc = HlcClock::new().with_max_skew(300_000); // 5 min
    let t1 = hlc.tick();
    // u64::MAX would previously drag the clock to u64::MAX forever.
    let error = hlc.observe(u64::MAX).unwrap_err();
    assert_eq!(error.timestamp_ms, u64::MAX);
    let t2 = hlc.tick();
    // Rejection leaves the clock near wall time instead of storing or
    // advancing to the poisonous timestamp.
    let wall = hlc.wall();
    assert!(
        t2 <= wall.saturating_add(10),
        "far-future remote must be rejected, got t2={t2} wall={wall}"
    );
    assert!(t2 >= t1, "clock must still be monotonic");
    // A moderately-ahead (within skew) remote still moves the clock.
    hlc.observe(wall + 60_000).unwrap();
    let t3 = hlc.tick();
    assert!(t3 > wall + 60_000, "in-skew remote must be observed");
}

#[test]
fn hlc_seeds_from_persisted_versions() {
    let mut hlc = HlcClock::new().with_max_skew(300_000);
    let persisted = hlc.wall() + 60_000;
    hlc.seed_persisted(persisted).unwrap();
    assert!(
        hlc.tick() > persisted,
        "the first post-restart write must beat durable state"
    );
}
