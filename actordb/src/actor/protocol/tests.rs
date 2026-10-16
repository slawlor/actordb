// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! LWW versions and tombstone ordering.

use super::*;
#[test]
fn lww_orders_by_timestamp_then_node_id() {
    let a = VersionedValue::new(vec![1], 100, "node-a".to_string());
    let b = VersionedValue::new(vec![2], 200, "node-a".to_string());
    let c = VersionedValue::new(vec![3], 200, "node-b".to_string());

    assert!(b.wins_over(&a));
    assert!(!a.wins_over(&b));
    // same timestamp: higher node id wins, deterministically
    assert!(c.wins_over(&b));
    assert!(!b.wins_over(&c));
    // identical versions never "win" — merge is a no-op
    assert!(!a.wins_over(&a));
}
#[test]
fn tombstone_wins_exact_tie() {
    let ts = 1_000u64;
    let live = VersionedValue::new(b"v".to_vec(), ts, "node-a".to_string());
    let tomb = VersionedValue::tombstone(ts, "node-a".to_string());
    // Same (timestamp, node_id): tombstone wins the tie.
    assert!(tomb.wins_over(&live));
    assert!(!live.wins_over(&tomb));
    // Higher timestamp still beats the tombstone.
    let newer = VersionedValue::new(b"v2".to_vec(), ts + 1, "node-a".to_string());
    assert!(newer.wins_over(&tomb));
    assert!(!tomb.wins_over(&newer));
}
