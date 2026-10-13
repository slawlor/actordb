// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Sharding: key → fragment mapping.
//!
//! The keyspace is partitioned into a fixed number of fragments. Every
//! node computes the mapping locally and deterministically, so all
//! nodes agree on placement without coordination.
//!
//! The hash **must** be deterministic across processes and machines:
//! `std`'s `RandomState` (and anything seeded per-process) would make
//! two nodes disagree on where a key lives. We use `seahash`, which is
//! deterministic by design.

use std::fmt;

/// A shard of the keyspace. `0..fragment_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FragmentId(pub u32);

impl fmt::Display for FragmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "frag-{}", self.0)
    }
}

/// Deterministic 64-bit hash of a key. Stable across processes,
/// machines, and restarts.
pub fn hash_key(key: &[u8]) -> u64 {
    seahash::hash(key)
}

/// The fragment owning `key`. `frag_count` must match the cluster's
/// configured fragment count (see `NodeConfig.fragment_count`).
///
/// # Panics
///
/// Panics if `frag_count` is 0.
pub fn fragment_of(key: &str, frag_count: usize) -> FragmentId {
    assert!(frag_count > 0, "fragment count must be non-zero");
    FragmentId((hash_key(key.as_bytes()) % frag_count as u64) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn fragment_ids_are_in_range() {
        for i in 0..1000 {
            let key = format!("key-{i}");
            let frag = fragment_of(&key, 16);
            assert!(frag.0 < 16, "out of range: {frag}");
        }
    }

    #[test]
    fn mapping_is_deterministic() {
        // Same key, same fragment, every time (across calls = across
        // processes, since seahash has no per-process seed).
        for _ in 0..100 {
            assert_eq!(fragment_of("hello", 8), fragment_of("hello", 8));
        }
    }

    #[test]
    fn distribution_is_roughly_uniform() {
        // 10k keys over 16 fragments: every fragment should get a
        // healthy share (chi-square would be overkill; just check no
        // fragment is starved or gorged).
        let mut counts = [0usize; 16];
        for i in 0..10_000 {
            counts[fragment_of(&format!("key-{i}"), 16).0 as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(
                (400..=900).contains(c),
                "fragment {i} got {c}/10000 keys, expected ~625"
            );
        }
    }

    #[test]
    fn all_fragments_reachable() {
        let mut seen = HashSet::new();
        for i in 0..10_000 {
            seen.insert(fragment_of(&format!("k{i}"), 8));
        }
        assert_eq!(seen.len(), 8, "some fragments never hit");
    }
}
