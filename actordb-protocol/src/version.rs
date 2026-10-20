//! Portable physical/logical record timestamps.

/// A totally ordered hybrid-clock timestamp.
///
/// `timestamp_ms` is physical Unix time; `logical_counter` distinguishes events
/// at that time without making a busy node's physical clock run ahead. Record
/// ordering additionally uses writer identity and the tombstone tie-break bit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordVersion {
    pub timestamp_ms: u64,
    pub logical_counter: u64,
}

impl RecordVersion {
    /// Construct a timestamp from its physical and logical components.
    pub const fn new(timestamp_ms: u64, logical_counter: u64) -> Self {
        Self {
            timestamp_ms,
            logical_counter,
        }
    }
}

impl From<u64> for RecordVersion {
    /// Legacy timestamps have a zero logical component.
    fn from(timestamp_ms: u64) -> Self {
        Self::new(timestamp_ms, 0)
    }
}
