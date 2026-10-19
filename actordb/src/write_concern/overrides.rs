// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

use std::num::NonZeroU32;

/// Apply an explicit operational cap without raising the requested threshold.
pub(super) fn effective_acknowledgement_count(requested: u32, cap: Option<NonZeroU32>) -> u32 {
    cap.map_or(requested, |cap| requested.min(cap.get()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_cap_only_lowers_acknowledgement_counts() {
        assert_eq!(effective_acknowledgement_count(3, NonZeroU32::new(2)), 2);
        assert_eq!(effective_acknowledgement_count(2, NonZeroU32::new(3)), 2);
        assert_eq!(effective_acknowledgement_count(1, NonZeroU32::new(2)), 1);
        assert_eq!(effective_acknowledgement_count(3, None), 3);
    }
}
