//! Content digests advertised by raw protocol-peer fixtures.
use sha2::{Digest, Sha256};

pub fn content_digest(payload: &[u8], expires_at_ms: u64) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"actordb-record-content-v1\0");
    digest.update((payload.len() as u64).to_be_bytes());
    digest.update(payload);
    if expires_at_ms == 0 {
        digest.update([0]);
    } else {
        digest.update([1]);
        digest.update(expires_at_ms.to_be_bytes());
    }
    digest.finalize().to_vec()
}
