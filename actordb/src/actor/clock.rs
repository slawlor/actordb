// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Wall-clock access and the node's hybrid logical clock.

/// Current wall-clock time in milliseconds since the unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A remote or persisted timestamp exceeds the configured future-skew bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockSkewError {
    pub timestamp_ms: u64,
    pub max_allowed_ms: u64,
}

impl std::fmt::Display for ClockSkewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "timestamp {} exceeds maximum allowed future time {}",
            self.timestamp_ms, self.max_allowed_ms
        )
    }
}

impl std::error::Error for ClockSkewError {}

/// Hybrid logical clock (simplified): monotonic per node, tracks the
/// max of wall-clock and observed remote timestamps.
///
/// Pure wall-clock LWW breaks under clock skew: a fast-clocked node's
/// writes shadow newer data from slow-clocked nodes, silently. HLC
/// keeps the `(timestamp, node_id)` tiebreak shape so the merge stays
/// idempotent and commutative, but guarantees per-node monotonicity and
/// moves forward when it sees a higher remote timestamp.
///
/// Timestamps are still wall-clock-meaningful when clocks agree; under
/// skew they degrade to logical ordering, which is the correct
/// tradeoff for LWW.
#[derive(Debug, Clone)]
pub struct HlcClock {
    last_ms: u64,
    /// Wall-clock offset in ms. Zero in production; tests use it to
    /// simulate clock skew between nodes.
    offset_ms: i64,
    /// Maximum a remote or persisted timestamp may exceed our wall clock
    /// before it is rejected. Bounds `observe()` so a single far-future
    /// timestamp cannot permanently poison stored LWW versions.
    /// `u64::MAX` = unbounded (legacy behavior).
    max_skew_ms: u64,
}

impl HlcClock {
    pub fn new() -> Self {
        Self::with_offset(0)
    }

    /// Create a clock whose wall-clock reads are shifted by `offset_ms`.
    /// Used to simulate clock skew in tests.
    pub fn with_offset(offset_ms: i64) -> Self {
        Self {
            last_ms: 0,
            offset_ms,
            max_skew_ms: u64::MAX,
        }
    }

    /// Set the maximum accepted future skew for remote and persisted
    /// timestamps. Values beyond the bound are rejected, not clamped: storing
    /// the original value after clamping only the clock would still poison
    /// LWW ordering.
    pub fn with_max_skew(mut self, max_skew_ms: u64) -> Self {
        self.max_skew_ms = max_skew_ms;
        self
    }

    fn wall(&self) -> u64 {
        let now = now_ms();
        if self.offset_ms >= 0 {
            now.saturating_add(self.offset_ms as u64)
        } else {
            now.saturating_sub(self.offset_ms.unsigned_abs())
        }
    }

    /// Timestamp for a local event: `max(wall, last + 1)`.
    pub fn tick(&mut self) -> u64 {
        let wall = self.wall();
        self.last_ms = wall.max(self.last_ms.saturating_add(1));
        self.last_ms
    }

    /// Observe an accepted remote timestamp:
    /// `max(wall, last + 1, remote + 1)`.
    pub fn observe(&mut self, remote_ms: u64) -> Result<(), ClockSkewError> {
        let wall = self.wall();
        self.validate(remote_ms, wall)?;
        self.last_ms = wall
            .max(self.last_ms.saturating_add(1))
            .max(remote_ms.saturating_add(1));
        Ok(())
    }

    /// Seed the clock from a durable version before serving writes. This does
    /// not create an event; the next [`Self::tick`] advances beyond it.
    pub fn seed_persisted(&mut self, timestamp_ms: u64) -> Result<(), ClockSkewError> {
        let wall = self.wall();
        self.validate(timestamp_ms, wall)?;
        self.last_ms = self.last_ms.max(timestamp_ms);
        Ok(())
    }

    fn validate(&self, timestamp_ms: u64, wall_ms: u64) -> Result<(), ClockSkewError> {
        let max_allowed_ms = wall_ms.saturating_add(self.max_skew_ms);
        if timestamp_ms > max_allowed_ms {
            return Err(ClockSkewError {
                timestamp_ms,
                max_allowed_ms,
            });
        }
        Ok(())
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
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
}
