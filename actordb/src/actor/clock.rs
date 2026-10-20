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

/// A timestamp could not be generated or accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClockError {
    Skew(ClockSkewError),
    /// Both the physical and logical components have exhausted their range.
    Exhausted,
}
impl std::fmt::Display for ClockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Skew(error) => error.fmt(f),
            Self::Exhausted => f.write_str("hybrid clock version range exhausted"),
        }
    }
}
impl std::error::Error for ClockError {}

use actordb_protocol::RecordVersion;

/// Hybrid logical clock with independent physical time and logical counters.
///
/// Event rate advances the logical component, never the physical component.
/// Remote and persisted versions are skew-checked against physical wall time.
/// Counter exhaustion carries to the next physical millisecond only after
/// checking that bound; complete version exhaustion returns an explicit error.
#[derive(Debug, Clone)]
pub struct HlcClock {
    last: RecordVersion,
    offset_ms: i64,
    max_skew_ms: u64,
}

impl HlcClock {
    pub fn new() -> Self {
        Self::with_offset(0)
    }
    /// Shift physical wall time for clock-skew tests.
    pub fn with_offset(offset_ms: i64) -> Self {
        Self {
            last: RecordVersion::default(),
            offset_ms,
            max_skew_ms: u64::MAX,
        }
    }
    /// Maximum accepted physical future skew. Logical counters are not skew.
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
    /// Generate one monotonic local physical/logical timestamp.
    pub fn tick(&mut self) -> Result<RecordVersion, ClockError> {
        self.tick_at(self.wall())
    }
    fn successor(&self, version: RecordVersion, wall: u64) -> Result<RecordVersion, ClockError> {
        if let Some(logical) = version.logical_counter.checked_add(1) {
            return Ok(RecordVersion::new(version.timestamp_ms, logical));
        }
        let physical = version
            .timestamp_ms
            .checked_add(1)
            .ok_or(ClockError::Exhausted)?;
        self.validate(physical, wall).map_err(ClockError::Skew)?;
        Ok(RecordVersion::new(physical, 0))
    }
    fn tick_at(&mut self, wall: u64) -> Result<RecordVersion, ClockError> {
        let next = if wall > self.last.timestamp_ms {
            RecordVersion::from(wall)
        } else {
            self.successor(self.last, wall)?
        };
        self.validate(next.timestamp_ms, wall)
            .map_err(ClockError::Skew)?;
        self.last = next;
        Ok(next)
    }
    /// Observe a legacy timestamp with logical counter zero.
    pub fn observe(&mut self, timestamp_ms: u64) -> Result<(), ClockError> {
        self.observe_version(RecordVersion::from(timestamp_ms))
    }
    /// Observe a remote timestamp after validating its physical component.
    pub fn observe_version(&mut self, remote: RecordVersion) -> Result<(), ClockError> {
        self.observe_at(remote, self.wall())
    }
    fn observe_at(&mut self, remote: RecordVersion, wall: u64) -> Result<(), ClockError> {
        self.validate(remote.timestamp_ms, wall)
            .map_err(ClockError::Skew)?;
        let floor = self.last.max(remote);
        self.last = if wall > floor.timestamp_ms {
            RecordVersion::from(wall)
        } else {
            self.successor(floor, wall)?
        };
        Ok(())
    }
    /// Seed a legacy durable timestamp without creating an event.
    pub fn seed_persisted(&mut self, timestamp_ms: u64) -> Result<(), ClockSkewError> {
        self.seed_version(RecordVersion::from(timestamp_ms))
    }
    /// Seed the highest durable physical/logical timestamp before serving.
    pub fn seed_version(&mut self, version: RecordVersion) -> Result<(), ClockSkewError> {
        self.validate(version.timestamp_ms, self.wall())?;
        self.last = self.last.max(version);
        Ok(())
    }
    fn validate(&self, timestamp_ms: u64, wall: u64) -> Result<(), ClockSkewError> {
        let max_allowed_ms = wall.saturating_add(self.max_skew_ms);
        if timestamp_ms > max_allowed_ms {
            Err(ClockSkewError {
                timestamp_ms,
                max_allowed_ms,
            })
        } else {
            Ok(())
        }
    }
}
impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn millions_of_events_do_not_create_physical_skew() {
        let mut clock = HlcClock::new().with_max_skew(300_000);
        let wall = 1_000_000;
        for _ in 0..1_000_000 {
            clock.tick_at(wall).unwrap();
        }
        assert_eq!(clock.last, RecordVersion::new(wall, 999_999));
        let mut receiver = HlcClock::new().with_max_skew(300_000);
        receiver.observe_at(clock.last, wall).unwrap();
        assert!(receiver.last > clock.last);
    }
    #[test]
    fn observations_order_both_components_and_reject_skew_without_changes() {
        let mut clock = HlcClock::new().with_max_skew(100);
        clock
            .observe_at(RecordVersion::new(1_010, 500), 1_000)
            .unwrap();
        assert_eq!(clock.last, RecordVersion::new(1_010, 501));
        assert_eq!(
            clock.tick_at(1_000).unwrap(),
            RecordVersion::new(1_010, 502)
        );
        let before = clock.last;
        assert!(matches!(
            clock.observe_at(RecordVersion::new(1_101, 0), 1_000),
            Err(ClockError::Skew(_))
        ));
        assert_eq!(clock.last, before);
        assert_eq!(clock.tick_at(1_020).unwrap(), RecordVersion::new(1_020, 0));
    }
    #[test]
    fn exhaustion_is_explicit_and_does_not_reuse_versions() {
        let mut clock = HlcClock::new();
        clock.last = RecordVersion::new(u64::MAX, u64::MAX);
        assert_eq!(clock.tick_at(u64::MAX), Err(ClockError::Exhausted));
        clock.last = RecordVersion::new(100, u64::MAX);
        assert_eq!(clock.tick_at(100).unwrap(), RecordVersion::new(101, 0));
        clock.last = RecordVersion::new(100, u64::MAX);
        clock.max_skew_ms = 0;
        assert!(matches!(clock.tick_at(100), Err(ClockError::Skew(_))));
    }
    #[test]
    fn durable_logical_high_water_survives_restart() {
        let mut clock = HlcClock::new().with_max_skew(300_000);
        let version = RecordVersion::new(clock.wall() + 60_000, 42);
        clock.seed_version(version).unwrap();
        assert!(clock.tick().unwrap() > version);
        assert!(clock.seed_persisted(u64::MAX).is_err());
    }
}
