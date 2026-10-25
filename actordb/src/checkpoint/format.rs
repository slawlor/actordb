//! Canonical bounded checkpoint codec. Integers use big-endian encoding.

use sha2::{Digest, Sha256};

use super::{CheckpointError, CheckpointRecord};
use crate::actor::VersionedValue;
use crate::store::StoreIdentity;

pub(super) fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Default)]
pub(crate) struct Encoder(pub(crate) Vec<u8>);
impl Encoder {
    pub(crate) fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    pub(crate) fn u32(&mut self, value: u32) {
        self.0.extend(value.to_be_bytes());
    }
    pub(crate) fn u64(&mut self, value: u64) {
        self.0.extend(value.to_be_bytes());
    }
    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.u32(value.len() as u32);
        self.0.extend(value);
    }
    pub(crate) fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }
    pub(crate) fn digest(&mut self, value: &[u8; 32]) {
        self.0.extend(value);
    }
    pub(crate) fn identity(&mut self, value: &StoreIdentity) {
        self.string(value.node_id());
        self.u32(value.fragment_count());
    }
    pub(crate) fn record(&mut self, record: &CheckpointRecord) {
        self.bytes(&record.key);
        let value = &record.value;
        self.u64(value.timestamp_ms);
        self.u64(value.logical_counter);
        self.string(&value.node_id);
        match value.expires_at_ms {
            Some(expiry) => {
                self.u8(1);
                self.u64(expiry);
            }
            None => self.u8(0),
        }
        self.u8(u8::from(value.is_tombstone));
        self.bytes(&value.value);
    }
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], CheckpointError> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| CheckpointError::Invalid("codec length overflow".into()))?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| CheckpointError::Invalid("truncated checkpoint encoding".into()))?;
        self.position = end;
        Ok(result)
    }
    pub(crate) fn u8(&mut self) -> Result<u8, CheckpointError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32, CheckpointError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, CheckpointError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>, CheckpointError> {
        let length = self.u32()? as usize;
        if length > maximum {
            return Err(CheckpointError::Invalid(
                "checkpoint field exceeds its limit".into(),
            ));
        }
        Ok(self.take(length)?.to_vec())
    }
    pub(crate) fn string(&mut self, maximum: usize) -> Result<String, CheckpointError> {
        String::from_utf8(self.bytes(maximum)?)
            .map_err(|_| CheckpointError::Invalid("checkpoint string is not UTF-8".into()))
    }
    pub(crate) fn digest(&mut self) -> Result<[u8; 32], CheckpointError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    pub(crate) fn identity(&mut self) -> Result<StoreIdentity, CheckpointError> {
        let identity = StoreIdentity::new(self.string(255)?, self.u32()?);
        super::validate_identity(&identity)?;
        Ok(identity)
    }
    pub(crate) fn record(&mut self) -> Result<CheckpointRecord, CheckpointError> {
        let key = self.bytes(super::MAX_CHUNK_BYTES)?;
        let timestamp_ms = self.u64()?;
        let logical_counter = self.u64()?;
        let node_id = self.string(255)?;
        let expires_at_ms = match self.u8()? {
            0 => None,
            1 => Some(self.u64()?),
            _ => return Err(CheckpointError::Invalid("invalid expiry flag".into())),
        };
        let is_tombstone = match self.u8()? {
            0 => false,
            1 => true,
            _ => return Err(CheckpointError::Invalid("invalid tombstone flag".into())),
        };
        let value = self.bytes(super::MAX_CHUNK_BYTES)?;
        Ok(CheckpointRecord {
            key,
            value: VersionedValue {
                value,
                timestamp_ms,
                logical_counter,
                node_id,
                expires_at_ms,
                is_tombstone,
            },
        })
    }
    pub(crate) fn finish(self) -> Result<(), CheckpointError> {
        if self.position != self.bytes.len() {
            Err(CheckpointError::Invalid("trailing checkpoint bytes".into()))
        } else {
            Ok(())
        }
    }
}
