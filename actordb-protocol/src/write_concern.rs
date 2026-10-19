//! Acknowledgement thresholds do not establish a global mutation order.

use std::collections::HashSet;
use std::fmt;
use std::time::{Duration, Instant};

use prost::Message;

use crate::meta;

pub const MAX_WRITE_CONCERN_REPLICAS: usize = 4096;
pub const MAX_WRITE_CONCERN_POLICY_BYTES: usize = 1024;
const MAX_RESULT_BYTES: usize = MAX_WRITE_CONCERN_REPLICAS * 258 + 8192;

/// Total acknowledgements, including the receiving node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum WriteConcern {
    #[default]
    Local,
    Count(u32),
    Majority,
}

impl WriteConcern {
    pub fn required_acknowledgements(
        self,
        replica_count: usize,
    ) -> Result<u32, WriteConcernPolicyError> {
        let majority = replica_count / 2 + 1;
        match self {
            Self::Local | Self::Count(1) => Ok(1),
            Self::Count(0) => Err(invalid("acknowledgement count must be positive")),
            Self::Count(count) if replica_count != 0 && count as usize <= majority => Ok(count),
            Self::Count(_) if replica_count == 0 => Err(invalid(
                "nonlocal concern requires explicit replica placement",
            )),
            Self::Count(_) => Err(invalid("acknowledgement count exceeds majority")),
            Self::Majority if replica_count == 0 => Err(invalid(
                "majority concern requires explicit replica placement",
            )),
            Self::Majority => u32::try_from(majority)
                .map_err(|_| invalid("replica count exceeds supported range")),
        }
    }
}

/// Per-write wait policy. Timeout starts when the receiving node applies the
/// update. Transport and actor admission have their own deadlines.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
pub struct WriteConcernOptions {
    #[builder(default)]
    concern: WriteConcern,
    #[builder(default = Duration::from_secs(5))]
    timeout: Duration,
}

impl Default for WriteConcernOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl WriteConcernOptions {
    pub fn concern(&self) -> WriteConcern {
        self.concern
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    pub fn validate(&self) -> Result<(), WriteConcernPolicyError> {
        let ms = self.timeout.as_millis();
        if ms == 0 || ms > u64::MAX as u128 || Instant::now().checked_add(self.timeout).is_none() {
            return Err(invalid(
                "write concern timeout must be representable and at least one millisecond",
            ));
        }
        if self.concern == WriteConcern::Count(0) {
            return Err(invalid("acknowledgement count must be positive"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, WriteConcernPolicyError> {
        self.validate()?;
        let (level, acknowledgements) = match self.concern {
            WriteConcern::Local => (meta::WriteConcernLevel::WriteConcernLocal, 0),
            WriteConcern::Count(n) => (meta::WriteConcernLevel::WriteConcernCount, n),
            WriteConcern::Majority => (meta::WriteConcernLevel::WriteConcernMajority, 0),
        };
        Ok(meta::WriteConcernPolicy {
            level: level as i32,
            acknowledgements,
            timeout_ms: self.timeout.as_millis() as u64,
        }
        .encode_to_vec())
    }

    /// Unknown policy fields are rejected, so an older implementation cannot
    /// silently ignore a new required acknowledgement constraint.
    pub fn decode(bytes: &[u8]) -> Result<Self, WriteConcernPolicyError> {
        preflight_policy(bytes)?;
        let policy = meta::WriteConcernPolicy::decode(bytes).map_err(|e| invalid(e.to_string()))?;
        let concern = match meta::WriteConcernLevel::try_from(policy.level) {
            Ok(meta::WriteConcernLevel::WriteConcernLocal) if policy.acknowledgements == 0 => {
                WriteConcern::Local
            }
            Ok(meta::WriteConcernLevel::WriteConcernCount) => {
                WriteConcern::Count(policy.acknowledgements)
            }
            Ok(meta::WriteConcernLevel::WriteConcernMajority) if policy.acknowledgements == 0 => {
                WriteConcern::Majority
            }
            _ => return Err(invalid("invalid acknowledgement policy")),
        };
        let options = Self {
            concern,
            timeout: Duration::from_millis(policy.timeout_ms),
        };
        options.validate()?;
        Ok(options)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteConcernPolicyError {
    Invalid(String),
    Unsupported(String),
}

impl fmt::Display for WriteConcernPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) | Self::Unsupported(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for WriteConcernPolicyError {}
fn invalid(s: impl Into<String>) -> WriteConcernPolicyError {
    WriteConcernPolicyError::Invalid(s.into())
}

/// Actual acknowledgement progress. The receiving node always counts as one.
/// An unknown replica count is represented by `None` for local-only writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConcernReceipt {
    receiving_node: String,
    replica_count: u32,
    requested: u32,
    required: u32,
    acknowledged_nodes: Vec<String>,
}

impl WriteConcernReceipt {
    pub fn new(
        receiving_node: String,
        replica_count: u32,
        requested: u32,
        required: u32,
        acknowledged_nodes: Vec<String>,
    ) -> Result<Self, WriteConcernPolicyError> {
        let receipt = Self {
            receiving_node,
            replica_count,
            requested,
            required,
            acknowledged_nodes,
        };
        receipt.validate()?;
        Ok(receipt)
    }
    pub fn receiving_node(&self) -> &str {
        &self.receiving_node
    }
    pub fn replica_count(&self) -> Option<u32> {
        (self.replica_count != 0).then_some(self.replica_count)
    }
    pub fn requested_acknowledgements(&self) -> u32 {
        self.requested
    }
    pub fn required_acknowledgements(&self) -> u32 {
        self.required
    }
    pub fn acknowledged_nodes(&self) -> &[String] {
        &self.acknowledged_nodes
    }
    pub fn was_overridden(&self) -> bool {
        self.required < self.requested
    }

    fn validate(&self) -> Result<(), WriteConcernPolicyError> {
        let valid_id = |id: &str| !id.is_empty() && id.len() <= 255;
        if !valid_id(&self.receiving_node)
            || self.requested == 0
            || self.required == 0
            || self.required > self.requested
            || self.replica_count as usize > MAX_WRITE_CONCERN_REPLICAS
            || (self.replica_count == 0 && self.requested != 1)
            || (self.replica_count > 0 && self.requested > self.replica_count / 2 + 1)
            || self.acknowledged_nodes.len() > MAX_WRITE_CONCERN_REPLICAS
            || (self.replica_count == 0 && self.acknowledged_nodes.len() != 1)
            || (self.replica_count > 0
                && self.acknowledged_nodes.len() > self.replica_count as usize)
            || self.acknowledged_nodes.iter().any(|id| !valid_id(id))
            || self.acknowledged_nodes.iter().collect::<HashSet<_>>().len()
                != self.acknowledged_nodes.len()
            || !self
                .acknowledged_nodes
                .iter()
                .any(|id| id == &self.receiving_node)
        {
            return Err(invalid("invalid write concern receipt"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteConcernOutcome {
    Satisfied(WriteConcernReceipt),
    TimedOut(WriteConcernReceipt),
    InvalidPolicy(String),
    Unsupported(String),
    Backpressure,
}

impl WriteConcernOutcome {
    pub fn encode(&self) -> Vec<u8> {
        let mut result = meta::WriteConcernResult::default();
        let (status, receipt) = match self {
            Self::Satisfied(receipt) => (
                meta::WriteConcernStatus::WriteConcernSatisfied,
                Some(receipt),
            ),
            Self::TimedOut(receipt) => (
                meta::WriteConcernStatus::WriteConcernTimedOut,
                Some(receipt),
            ),
            Self::InvalidPolicy(error) => {
                result.error = error.clone();
                (meta::WriteConcernStatus::WriteConcernInvalid, None)
            }
            Self::Unsupported(error) => {
                result.error = error.clone();
                (meta::WriteConcernStatus::WriteConcernUnsupported, None)
            }
            Self::Backpressure => (meta::WriteConcernStatus::WriteConcernBackpressure, None),
        };
        result.status = status as i32;
        if let Some(receipt) = receipt {
            result.receiving_node = receipt.receiving_node.clone();
            result.replica_count = receipt.replica_count;
            result.requested_acknowledgements = receipt.requested;
            result.required_acknowledgements = receipt.required;
            result.acknowledged_nodes = receipt.acknowledged_nodes.clone();
        }
        result.encode_to_vec()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, WriteConcernPolicyError> {
        preflight_result(bytes)?;
        let result = meta::WriteConcernResult::decode(bytes).map_err(|e| invalid(e.to_string()))?;
        match meta::WriteConcernStatus::try_from(result.status) {
            Ok(
                meta::WriteConcernStatus::WriteConcernSatisfied
                | meta::WriteConcernStatus::WriteConcernTimedOut,
            ) => {
                let receipt = WriteConcernReceipt::new(
                    result.receiving_node,
                    result.replica_count,
                    result.requested_acknowledgements,
                    result.required_acknowledgements,
                    result.acknowledged_nodes,
                )?;
                if result.status == meta::WriteConcernStatus::WriteConcernSatisfied as i32 {
                    if receipt.acknowledged_nodes.len() < receipt.required as usize {
                        return Err(invalid("receipt lacks required acknowledgements"));
                    }
                    Ok(Self::Satisfied(receipt))
                } else {
                    Ok(Self::TimedOut(receipt))
                }
            }
            Ok(meta::WriteConcernStatus::WriteConcernInvalid) => {
                Ok(Self::InvalidPolicy(result.error))
            }
            Ok(meta::WriteConcernStatus::WriteConcernUnsupported) => {
                Ok(Self::Unsupported(result.error))
            }
            Ok(meta::WriteConcernStatus::WriteConcernBackpressure) => Ok(Self::Backpressure),
            _ => Err(invalid("unknown write concern outcome")),
        }
    }
}

fn varint(bytes: &[u8], pos: &mut usize) -> Result<u64, WriteConcernPolicyError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*pos).ok_or_else(|| invalid("truncated policy"))?;
        *pos += 1;
        if shift == 63 && byte > 1 {
            return Err(invalid("overflowing policy varint"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(invalid("overflowing policy varint"))
}

pub fn preflight_policy(bytes: &[u8]) -> Result<(), WriteConcernPolicyError> {
    if bytes.len() > MAX_WRITE_CONCERN_POLICY_BYTES {
        return Err(invalid("write concern policy exceeds byte limit"));
    }
    let mut pos = 0;
    let mut seen = [false; 4];
    while pos < bytes.len() {
        let tag = varint(bytes, &mut pos)?;
        let field = tag >> 3;
        if !(1..=3).contains(&field) {
            return Err(WriteConcernPolicyError::Unsupported(
                "unsupported write concern policy field".into(),
            ));
        }
        if tag & 7 != 0 || seen[field as usize] {
            return Err(invalid("invalid or duplicate policy field"));
        }
        seen[field as usize] = true;
        varint(bytes, &mut pos)?;
    }
    Ok(())
}

pub fn preflight_result(bytes: &[u8]) -> Result<(), WriteConcernPolicyError> {
    if bytes.len() > MAX_RESULT_BYTES {
        return Err(invalid("write concern result exceeds byte limit"));
    }
    let mut pos = 0;
    let mut nodes = 0usize;
    while pos < bytes.len() {
        let tag = varint(bytes, &mut pos)?;
        let field = tag >> 3;
        if field == 0 {
            return Err(invalid("invalid receipt field"));
        }
        match tag & 7 {
            0 => {
                varint(bytes, &mut pos)?;
            }
            1 => {
                pos = pos
                    .checked_add(8)
                    .ok_or_else(|| invalid("receipt size overflow"))?;
            }
            5 => {
                pos = pos
                    .checked_add(4)
                    .ok_or_else(|| invalid("receipt size overflow"))?;
            }
            2 => {
                let len = usize::try_from(varint(bytes, &mut pos)?)
                    .map_err(|_| invalid("receipt size overflow"))?;
                if field == 6 {
                    nodes += 1;
                    if nodes > MAX_WRITE_CONCERN_REPLICAS || len > 255 {
                        return Err(invalid("receipt acknowledgement limit"));
                    }
                }
                if field == 2 && len > 255 || field == 7 && len > 4096 {
                    return Err(invalid("receipt string limit"));
                }
                pos = pos
                    .checked_add(len)
                    .ok_or_else(|| invalid("receipt size overflow"))?;
            }
            _ => return Err(invalid("invalid receipt wire type")),
        }
        if pos > bytes.len() {
            return Err(invalid("truncated receipt"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn count_is_total_and_bounded_by_majority() {
        assert_eq!(
            WriteConcern::Count(3).required_acknowledgements(5).unwrap(),
            3
        );
        assert_eq!(
            WriteConcern::Majority.required_acknowledgements(4).unwrap(),
            3
        );
        assert!(WriteConcern::Count(4).required_acknowledgements(5).is_err());
        assert!(WriteConcern::Majority.required_acknowledgements(0).is_err());
        assert_eq!(WriteConcern::Local.required_acknowledgements(0).unwrap(), 1);
    }
    #[test]
    fn policy_roundtrip_rejects_unknown_and_duplicate_requirements() {
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(2))
            .build();
        let bytes = options.encode().unwrap();
        assert_eq!(WriteConcernOptions::decode(&bytes).unwrap(), options);
        assert!(WriteConcernOptions::decode(&[bytes.clone(), bytes].concat()).is_err());
        assert!(matches!(
            WriteConcernOptions::decode(&[40, 1]),
            Err(WriteConcernPolicyError::Unsupported(_))
        ));
    }
    #[test]
    fn receipts_require_distinct_acknowledgements_and_receiver() {
        assert!(
            WriteConcernReceipt::new("a".into(), 3, 2, 2, vec!["a".into(), "a".into()]).is_err()
        );
        let receipt =
            WriteConcernReceipt::new("a".into(), 3, 2, 2, vec!["a".into(), "b".into()]).unwrap();
        let result = WriteConcernOutcome::Satisfied(receipt);
        assert_eq!(
            WriteConcernOutcome::decode(&result.encode()).unwrap(),
            result
        );
    }
}
