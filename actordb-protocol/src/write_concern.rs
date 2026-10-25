//! Acknowledgement thresholds do not establish a global mutation order.
//!
//! The receiving node freezes replica eligibility, region labels, and the
//! effective count before apply. A processing acknowledgement means that the
//! replica processed this mutation and retains its equal-or-newer LWW winner;
//! it does not promise that the submitted value remains the winner. The local
//! acknowledgement follows ordinary apply completion, which may only enqueue
//! persistence and return before a store flush. Remote acknowledgements
//! satisfy the sender's configured replication durability fence. Neither the
//! count nor strict count enforcement requests stable storage: memory apply,
//! buffered store commit, and stable store commit depend on node configuration,
//! and receipts currently do not classify that evidence per node.

use std::collections::HashSet;
use std::fmt;
use std::time::{Duration, Instant};

use prost::Message;

use crate::meta;

pub const MAX_WRITE_CONCERN_REPLICAS: usize = 4096;
pub const MAX_WRITE_CONCERN_POLICY_BYTES: usize = 1024;
const MAX_RESULT_BYTES: usize = 2 * MAX_WRITE_CONCERN_REPLICAS * 258 + 8192;

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
    /// Additionally require a processed acknowledgement outside the receiver's
    /// configured region, including for local and count-one policies.
    #[builder(default)]
    require_other_region: bool,
    /// Reject before apply if an operational cap would reduce the requested
    /// acknowledgement count. Defaults to allowing a disclosed reduction.
    #[builder(default)]
    strict_acknowledgements: bool,
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
    /// Whether success additionally requires a processed outside-region replica.
    pub fn require_other_region(&self) -> bool {
        self.require_other_region
    }
    /// Add or clear the independent region acknowledgement requirement.
    pub fn with_require_other_region(mut self, required: bool) -> Self {
        self.require_other_region = required;
        self
    }
    /// Whether an operational cap must preserve the requested count. This does
    /// not change the configured storage durability or LWW winner semantics.
    pub fn strict_acknowledgements(&self) -> bool {
        self.strict_acknowledgements
    }
    /// Require or relax preservation of the requested acknowledgement count.
    /// Older peers reject an enabled constraint before applying the mutation.
    pub fn with_strict_acknowledgements(mut self, strict: bool) -> Self {
        self.strict_acknowledgements = strict;
        self
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
            require_other_region: self.require_other_region,
            strict_acknowledgements: self.strict_acknowledgements,
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
            require_other_region: policy.require_other_region,
            strict_acknowledgements: policy.strict_acknowledgements,
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

/// Actual processing acknowledgement progress. The receiving node always
/// counts as one after ordinary apply completion. Remote acknowledgements meet
/// configured sender durability; this receipt does not classify crash durability.
/// An unknown replica count is represented by `None` for local-only writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConcernReceipt {
    receiving_node: String,
    replica_count: u32,
    requested: u32,
    required: u32,
    acknowledged_nodes: Vec<String>,
    receiving_region: Option<String>,
    outside_region_acknowledged_nodes: Vec<String>,
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
            receiving_region: None,
            outside_region_acknowledged_nodes: Vec::new(),
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

    /// Attach progress against the receiving node's frozen region requirement.
    pub fn with_region_progress(
        mut self,
        receiving_region: String,
        outside_region_acknowledged_nodes: Vec<String>,
    ) -> Result<Self, WriteConcernPolicyError> {
        self.receiving_region = Some(receiving_region);
        self.outside_region_acknowledged_nodes = outside_region_acknowledged_nodes;
        self.validate()?;
        Ok(self)
    }
    /// Frozen receiving-region label, present when a region requirement was used.
    pub fn receiving_region(&self) -> Option<&str> {
        self.receiving_region.as_deref()
    }
    /// Eligible acknowledged identities classified outside the receiving region
    /// by the receiving node's frozen configuration.
    pub fn outside_region_acknowledged_nodes(&self) -> &[String] {
        &self.outside_region_acknowledged_nodes
    }
    /// Whether an eligible outside-region processing acknowledgement was observed.
    pub fn other_region_satisfied(&self) -> bool {
        self.receiving_region.is_some() && !self.outside_region_acknowledged_nodes.is_empty()
    }
    /// Validate receipt correlation with the requested policy. A timed-out
    /// receipt may lack the required progress but must preserve policy metadata.
    pub fn validate_for(
        &self,
        options: &WriteConcernOptions,
        satisfied: bool,
    ) -> Result<(), WriteConcernPolicyError> {
        self.validate()?;
        let expected = options
            .concern()
            .required_acknowledgements(self.replica_count as usize)?;
        if self.requested != expected {
            return Err(invalid(
                "write concern reply changed the requested threshold",
            ));
        }
        if options.strict_acknowledgements() && self.required != self.requested {
            return Err(invalid(
                "write concern reply lowered a strict acknowledgement threshold",
            ));
        }
        if options.require_other_region()
            && (self.receiving_region.is_none() || satisfied && !self.other_region_satisfied())
        {
            return Err(invalid(
                "write concern reply lacks required region progress",
            ));
        }
        if satisfied && self.acknowledged_nodes.len() < self.required as usize {
            return Err(invalid("receipt lacks required acknowledgements"));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), WriteConcernPolicyError> {
        let valid_id = |id: &str| !id.is_empty() && id.len() <= 255;
        let acknowledged: HashSet<_> = self.acknowledged_nodes.iter().collect();
        let outside: HashSet<_> = self.outside_region_acknowledged_nodes.iter().collect();
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
            || acknowledged.len() != self.acknowledged_nodes.len()
            || !self
                .acknowledged_nodes
                .iter()
                .any(|id| id == &self.receiving_node)
            || self
                .receiving_region
                .as_ref()
                .is_some_and(|region| !valid_id(region))
            || self.receiving_region.is_some() && self.replica_count == 0
            || self.receiving_region.is_none() && !self.outside_region_acknowledged_nodes.is_empty()
            || self.outside_region_acknowledged_nodes.len() > self.acknowledged_nodes.len()
            || outside.len() != self.outside_region_acknowledged_nodes.len()
            || self
                .outside_region_acknowledged_nodes
                .iter()
                .any(|id| id == &self.receiving_node || !acknowledged.contains(id))
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
            result.receiving_region = receipt.receiving_region.clone();
            result.outside_region_acknowledged_nodes =
                receipt.outside_region_acknowledged_nodes.clone();
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
                let mut receipt = WriteConcernReceipt::new(
                    result.receiving_node,
                    result.replica_count,
                    result.requested_acknowledgements,
                    result.required_acknowledgements,
                    result.acknowledged_nodes,
                )?;
                if let Some(region) = result.receiving_region {
                    receipt = receipt
                        .with_region_progress(region, result.outside_region_acknowledged_nodes)?;
                } else if !result.outside_region_acknowledged_nodes.is_empty() {
                    return Err(invalid("region progress lacks receiving region"));
                }
                if result.status == meta::WriteConcernStatus::WriteConcernSatisfied as i32 {
                    if receipt.acknowledged_nodes.len() < receipt.required as usize {
                        return Err(invalid("receipt lacks required acknowledgements"));
                    }
                    if receipt.receiving_region.is_some() && !receipt.other_region_satisfied() {
                        return Err(invalid("receipt lacks required region acknowledgement"));
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
    let mut seen = [false; 6];
    while pos < bytes.len() {
        let tag = varint(bytes, &mut pos)?;
        let field = tag >> 3;
        if !(1..=5).contains(&field) {
            return Err(WriteConcernPolicyError::Unsupported(
                "unsupported write concern policy field".into(),
            ));
        }
        if tag & 7 != 0 || seen[field as usize] {
            return Err(invalid("invalid or duplicate policy field"));
        }
        seen[field as usize] = true;
        let value = varint(bytes, &mut pos)?;
        if field == 4 && value > 1 {
            return Err(invalid("invalid region requirement boolean"));
        }
        if field == 5 && value > 1 {
            return Err(invalid("invalid strict acknowledgement boolean"));
        }
    }
    Ok(())
}

pub fn preflight_result(bytes: &[u8]) -> Result<(), WriteConcernPolicyError> {
    if bytes.len() > MAX_RESULT_BYTES {
        return Err(invalid("write concern result exceeds byte limit"));
    }
    let mut pos = 0;
    let mut nodes = 0usize;
    let mut outside_nodes = 0usize;
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
                if field == 9 {
                    outside_nodes += 1;
                    if outside_nodes > MAX_WRITE_CONCERN_REPLICAS || len > 255 {
                        return Err(invalid("receipt region acknowledgement limit"));
                    }
                }
                if (field == 2 || field == 8) && len > 255 || field == 7 && len > 4096 {
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
            WriteConcernOptions::decode(&[48, 1]),
            Err(WriteConcernPolicyError::Unsupported(_))
        ));
    }
    #[test]
    fn strict_policy_roundtrips_and_uses_a_required_extension_field() {
        let legacy = WriteConcernOptions::builder()
            .concern(WriteConcern::Count(2))
            .build();
        assert!(!legacy.strict_acknowledgements());
        let strict = legacy.clone().with_strict_acknowledgements(true);
        let mut expected = legacy.encode().unwrap();
        // Field 5 is omitted when false. Pre-extension peers reject this tag
        // as unknown instead of silently applying a weaker request.
        expected.extend([40, 1]);
        assert_eq!(strict.encode().unwrap(), expected);
        assert_eq!(WriteConcernOptions::decode(&expected).unwrap(), strict);
        assert_eq!(strict.with_strict_acknowledgements(false), legacy);
        assert!(WriteConcernOptions::decode(&[expected, vec![40, 1]].concat()).is_err());
        assert!(WriteConcernOptions::decode(&[40, 2]).is_err());
    }
    #[test]
    fn strict_receipts_reject_lowered_counts_even_for_timeout() {
        let options = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .strict_acknowledgements(true)
            .build();
        let lowered = WriteConcernReceipt::new("a".into(), 3, 2, 1, vec!["a".into()]).unwrap();
        assert!(lowered.validate_for(&options, true).is_err());
        assert!(lowered.validate_for(&options, false).is_err());
        let preserved = WriteConcernReceipt::new("a".into(), 3, 2, 2, vec!["a".into()]).unwrap();
        assert!(preserved.validate_for(&options, false).is_ok());
        assert!(preserved.validate_for(&options, true).is_err());
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
    #[test]
    fn region_policy_roundtrips_and_requires_canonical_boolean() {
        let options = WriteConcernOptions::builder()
            .require_other_region(true)
            .build();
        let encoded = options.encode().unwrap();
        assert_eq!(WriteConcernOptions::decode(&encoded).unwrap(), options);
        assert!(WriteConcernOptions::decode(&[encoded, vec![32, 1]].concat()).is_err());
        assert!(WriteConcernOptions::decode(&[32, 2]).is_err());
    }
    #[test]
    fn region_receipts_require_acknowledged_distinct_outside_nodes() {
        let base =
            WriteConcernReceipt::new("a".into(), 3, 1, 1, vec!["a".into(), "b".into()]).unwrap();
        for nodes in [
            vec!["a".into()],
            vec!["outside".into()],
            vec!["b".into(), "b".into()],
        ] {
            assert!(base
                .clone()
                .with_region_progress("west".into(), nodes)
                .is_err());
        }
        let receipt = base
            .with_region_progress("west".into(), vec!["b".into()])
            .unwrap();
        let options = WriteConcernOptions::default().with_require_other_region(true);
        assert!(receipt.validate_for(&options, true).is_ok());
        let outcome = WriteConcernOutcome::Satisfied(receipt);
        assert_eq!(
            WriteConcernOutcome::decode(&outcome.encode()).unwrap(),
            outcome
        );
        let timed_out = WriteConcernReceipt::new("a".into(), 3, 1, 1, vec!["a".into()])
            .unwrap()
            .with_region_progress("west".into(), Vec::new())
            .unwrap();
        assert!(timed_out.validate_for(&options, false).is_ok());
        assert!(timed_out.validate_for(&options, true).is_err());
        assert!(WriteConcernOutcome::decode(
            &WriteConcernOutcome::Satisfied(timed_out.clone()).encode()
        )
        .is_err());
        let outcome = WriteConcernOutcome::TimedOut(timed_out);
        assert_eq!(
            WriteConcernOutcome::decode(&outcome.encode()).unwrap(),
            outcome
        );
    }
    #[test]
    fn maximum_region_receipt_fits_and_preflight_bounds_region_fields() {
        let ids: Vec<_> = (0..MAX_WRITE_CONCERN_REPLICAS)
            .map(|i| format!("{i:04}{}", "x".repeat(251)))
            .collect();
        let receipt = WriteConcernReceipt::new(
            ids[0].clone(),
            MAX_WRITE_CONCERN_REPLICAS as u32,
            1,
            1,
            ids.clone(),
        )
        .unwrap()
        .with_region_progress("west".into(), ids[1..].to_vec())
        .unwrap();
        let outcome = WriteConcernOutcome::Satisfied(receipt);
        assert_eq!(
            WriteConcernOutcome::decode(&outcome.encode()).unwrap(),
            outcome
        );
        // Field 9, 4097 one-byte strings.
        let excessive = [74, 1, b'n'].repeat(MAX_WRITE_CONCERN_REPLICAS + 1);
        assert!(preflight_result(&excessive).is_err());
        let mut oversized_region = vec![66, 128, 2];
        oversized_region.extend([b'x'; 256]);
        assert!(preflight_result(&oversized_region).is_err());
    }
}
