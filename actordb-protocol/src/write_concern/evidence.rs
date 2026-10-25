//! Bounded, independently validated certificates of a processing version floor.
use super::{
    invalid, meta, varint, WriteConcernPolicyError, WriteConcernReceipt, MAX_WRITE_CONCERN_REPLICAS,
};
use crate::RecordVersion;
use prost::Message;
use sha2::{Digest, Sha256};

/// Actual minimum fence reached. Buffered store commit does not imply fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[non_exhaustive]
#[repr(i32)]
pub enum WriteDurability {
    #[default]
    MemoryApplied = 0,
    StoreCommitted = 1,
    StableCommitted = 2,
}
impl WriteDurability {
    pub(super) fn decode(value: i32) -> Result<Self, WriteConcernPolicyError> {
        match value {
            0 => Ok(Self::MemoryApplied),
            1 => Ok(Self::StoreCommitted),
            2 => Ok(Self::StableCommitted),
            _ => Err(invalid("unknown durability evidence")),
        }
    }
}

/// Submitted operation identity. ACKs certify an equal or newer LWW winner,
/// not immutable payload/log presence or a distributed atomic snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOperationEvidence {
    wire: meta::WriteOperationEvidence,
}
impl WriteOperationEvidence {
    /// SDK correlation digests without retaining a second request payload.
    pub fn application_write_digests(table: &str, key: &str, value: &[u8]) -> ([u8; 32], [u8; 32]) {
        let mut digest = Sha256::new();
        digest.update(b"\x1eactordb.table.v1:");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in table.bytes().map(|byte| byte.to_ascii_uppercase()) {
            digest.update([HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]);
        }
        digest.update(b":");
        for byte in key.as_bytes() {
            digest.update([HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]);
        }
        (digest.finalize().into(), Sha256::digest(value).into())
    }
    pub fn validate_application_digests(
        &self,
        key_digest: &[u8],
        value_digest: &[u8],
        is_tombstone: bool,
    ) -> Result<(), WriteConcernPolicyError> {
        if self.wire.key_digest != key_digest
            || self.wire.value_digest != value_digest
            || self.wire.is_tombstone != is_tombstone
        {
            return Err(invalid("receipt certifies a different application write"));
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fragment: u32,
        key: &str,
        value: &[u8],
        version: RecordVersion,
        node_id: String,
        is_tombstone: bool,
        expires_at_ms: Option<u64>,
    ) -> Result<Self, WriteConcernPolicyError> {
        let mut wire = meta::WriteOperationEvidence {
            fragment,
            key_digest: Sha256::digest(key.as_bytes()).to_vec(),
            value_digest: Sha256::digest(value).to_vec(),
            timestamp_ms: version.timestamp_ms,
            logical_counter: version.logical_counter,
            node_id,
            is_tombstone,
            expires_at_ms,
            operation_digest: Vec::new(),
        };
        wire.operation_digest = Sha256::digest(wire.encode_to_vec()).to_vec();
        let evidence = Self { wire };
        evidence.validate()?;
        Ok(evidence)
    }
    pub fn fragment(&self) -> u32 {
        self.wire.fragment
    }
    pub fn version(&self) -> RecordVersion {
        RecordVersion::new(self.wire.timestamp_ms, self.wire.logical_counter)
    }
    pub fn originating_node(&self) -> &str {
        &self.wire.node_id
    }
    pub fn is_tombstone(&self) -> bool {
        self.wire.is_tombstone
    }
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.wire.expires_at_ms
    }
    pub fn key_digest(&self) -> &[u8] {
        &self.wire.key_digest
    }
    pub fn value_digest(&self) -> &[u8] {
        &self.wire.value_digest
    }
    pub fn operation_digest(&self) -> &[u8] {
        &self.wire.operation_digest
    }
    /// Correlate SDK evidence with the selected table, user key and payload.
    /// Version/absolute TTL are minted by the receiving node; callers can save
    /// the returned certificate for later comparison.
    pub fn validate_application_write(
        &self,
        table: &str,
        key: &str,
        value: &[u8],
        is_tombstone: bool,
    ) -> Result<(), WriteConcernPolicyError> {
        let (key_digest, value_digest) = Self::application_write_digests(table, key, value);
        self.validate_application_digests(&key_digest, &value_digest, is_tombstone)
    }
    fn validate(&self) -> Result<(), WriteConcernPolicyError> {
        let mut unsigned = self.wire.clone();
        unsigned.operation_digest.clear();
        if self.wire.node_id.is_empty()
            || self.wire.node_id.len() > 255
            || self.wire.key_digest.len() != 32
            || self.wire.value_digest.len() != 32
            || self.wire.operation_digest != Sha256::digest(unsigned.encode_to_vec()).as_slice()
            || self.wire.expires_at_ms == Some(0)
        {
            return Err(invalid("invalid operation certificate"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAcknowledgement {
    node: String,
    durability: WriteDurability,
}
impl WriteAcknowledgement {
    pub fn new(node: String, durability: WriteDurability) -> Self {
        Self { node, durability }
    }
    pub fn node(&self) -> &str {
        &self.node
    }
    pub fn durability(&self) -> WriteDurability {
        self.durability
    }
}

/// Exact frozen eligible identities and reached fences for one submitted floor.
/// The deterministic digest identifies the set, not a placement epoch or lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConcernEvidence {
    operation: WriteOperationEvidence,
    eligible_nodes: Vec<String>,
    replica_set_digest: Vec<u8>,
    acknowledgements: Vec<WriteAcknowledgement>,
}
fn set_digest(nodes: &[String]) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(b"actordb.write-concern.replica-set.v1");
    hash.update((nodes.len() as u64).to_be_bytes());
    for node in nodes {
        hash.update((node.len() as u64).to_be_bytes());
        hash.update(node.as_bytes());
    }
    hash.finalize().to_vec()
}
impl WriteConcernEvidence {
    pub fn new(
        operation: WriteOperationEvidence,
        mut eligible_nodes: Vec<String>,
        acknowledgements: Vec<WriteAcknowledgement>,
    ) -> Result<Self, WriteConcernPolicyError> {
        eligible_nodes.sort();
        let evidence = Self {
            operation,
            replica_set_digest: set_digest(&eligible_nodes),
            eligible_nodes,
            acknowledgements,
        };
        evidence.validate()?;
        Ok(evidence)
    }
    pub fn operation(&self) -> &WriteOperationEvidence {
        &self.operation
    }
    pub fn eligible_nodes(&self) -> &[String] {
        &self.eligible_nodes
    }
    pub fn replica_set_digest(&self) -> &[u8] {
        &self.replica_set_digest
    }
    pub fn acknowledgements(&self) -> &[WriteAcknowledgement] {
        &self.acknowledgements
    }
    pub fn durability_at(&self, node: &str) -> Option<WriteDurability> {
        self.acknowledgements
            .iter()
            .find(|ack| ack.node == node)
            .map(|ack| ack.durability)
    }
    fn validate(&self) -> Result<(), WriteConcernPolicyError> {
        self.operation.validate()?;
        if self.eligible_nodes.is_empty()
            || self.eligible_nodes.len() > MAX_WRITE_CONCERN_REPLICAS
            || self
                .eligible_nodes
                .iter()
                .any(|node| node.is_empty() || node.len() > 255)
            || self
                .eligible_nodes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self.replica_set_digest != set_digest(&self.eligible_nodes)
            || self.acknowledgements.len() > self.eligible_nodes.len()
        {
            return Err(invalid("invalid replica-set evidence"));
        }
        let mut seen = std::collections::HashSet::new();
        for ack in &self.acknowledgements {
            if self.eligible_nodes.binary_search(&ack.node).is_err() || !seen.insert(&ack.node) {
                return Err(invalid("ineligible or duplicate durability evidence"));
            }
        }
        Ok(())
    }
    pub(super) fn validate_receipt(
        &self,
        receipt: &WriteConcernReceipt,
    ) -> Result<(), WriteConcernPolicyError> {
        self.validate()?;
        let count = receipt.replica_count().map_or(1, |n| n as usize);
        if count != self.eligible_nodes.len()
            || self.acknowledgements.len() != receipt.acknowledged_nodes().len()
            || receipt
                .acknowledged_nodes()
                .iter()
                .any(|node| self.durability_at(node).is_none())
        {
            return Err(invalid("evidence disagrees with acknowledgement progress"));
        }
        Ok(())
    }
    pub(super) fn encode(&self) -> meta::WriteConcernEvidence {
        meta::WriteConcernEvidence {
            operation: Some(self.operation.wire.clone()),
            eligible_nodes: self.eligible_nodes.clone(),
            replica_set_digest: self.replica_set_digest.clone(),
            acknowledgements: self
                .acknowledgements
                .iter()
                .map(|ack| meta::WriteAcknowledgement {
                    replica_index: self.eligible_nodes.binary_search(&ack.node).unwrap() as u32,
                    durability: ack.durability as i32,
                })
                .collect(),
        }
    }
    pub(super) fn decode(
        wire: meta::WriteConcernEvidence,
    ) -> Result<Self, WriteConcernPolicyError> {
        let acknowledgements = wire
            .acknowledgements
            .iter()
            .map(|ack| {
                let node = wire
                    .eligible_nodes
                    .get(ack.replica_index as usize)
                    .ok_or_else(|| invalid("invalid replica evidence index"))?
                    .clone();
                Ok(WriteAcknowledgement::new(
                    node,
                    WriteDurability::decode(ack.durability)?,
                ))
            })
            .collect::<Result<Vec<_>, WriteConcernPolicyError>>()?;
        let evidence = Self {
            operation: WriteOperationEvidence {
                wire: wire
                    .operation
                    .ok_or_else(|| invalid("missing operation certificate"))?,
            },
            eligible_nodes: wire.eligible_nodes,
            replica_set_digest: wire.replica_set_digest,
            acknowledgements,
        };
        evidence.validate()?;
        Ok(evidence)
    }
}

/// Bound repeated allocation amplification before Prost allocates evidence.
pub(super) fn preflight(bytes: &[u8]) -> Result<(), WriteConcernPolicyError> {
    let mut pos = 0;
    let mut counts = [0usize; 5];
    while pos < bytes.len() {
        let tag = varint(bytes, &mut pos)?;
        let field = (tag >> 3) as usize;
        if !(1..=4).contains(&field) || tag & 7 != 2 {
            return Err(invalid("invalid evidence field"));
        }
        counts[field] += 1;
        if counts[field]
            > if field == 2 || field == 4 {
                MAX_WRITE_CONCERN_REPLICAS
            } else {
                1
            }
        {
            return Err(invalid("duplicate or excessive evidence field"));
        }
        let len = usize::try_from(varint(bytes, &mut pos)?)
            .map_err(|_| invalid("evidence length overflow"))?;
        let limit = match field {
            1 => 1024,
            2 => 255,
            3 => 32,
            _ => 16,
        };
        if len > limit {
            return Err(invalid("evidence field exceeds limit"));
        }
        pos = pos
            .checked_add(len)
            .filter(|pos| *pos <= bytes.len())
            .ok_or_else(|| invalid("truncated evidence"))?;
    }
    Ok(())
}
