//! Committed Prost bindings and stable limits for peer and application protocols.
//!
//! Regenerate with `cargo run -p actordb-protocol --features codegen --bin generate`.
//! Append `-- --check` to verify schema/binding agreement without writing files.
pub mod meta {
    include!("generated/meta.rs");
}
#[allow(clippy::large_enum_variant)]
pub mod client {
    include!("generated/client.rs");
}

mod version;
/// Per-write acknowledgement policy and progress receipts.
pub mod write_concern;
pub use version::RecordVersion;

impl client::Value {
    /// Physical/logical timestamp; compare writer identity after this pair.
    pub fn version(&self) -> RecordVersion {
        RecordVersion::new(self.timestamp_ms, self.logical_counter)
    }
}

pub const CLIENT_PROTOCOL_VERSION: u32 = 2;
pub const CLIENT_CAPABILITY_QUERY: u64 = 1;
pub const CLIENT_CAPABILITY_SCAN: u64 = 2;
pub const CLIENT_CAPABILITY_WRITE_CONCERNS: u64 = 4;
pub const CLIENT_CAPABILITIES: u64 =
    CLIENT_CAPABILITY_QUERY | CLIENT_CAPABILITY_SCAN | CLIENT_CAPABILITY_WRITE_CONCERNS;
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_ITEMS: usize = 4096;
pub const MAX_HANDSHAKE_BYTES: usize = 4096;

/// Canonical challenge proof. Client credentials use their own domain and never
/// authenticate a peer. Both identities, offers, and fresh challenges are bound.
pub fn client_auth_proof(
    secret: &[u8],
    dialer: bool,
    client: &client::Hello,
    server: &client::Hello,
) -> Vec<u8> {
    use hmac::{KeyInit, Mac};
    let mut mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(b"actordb-client-proof-v1\0");
    mac.update(if dialer { b"client" } else { b"server" });
    for hello in [client, server] {
        mac.update(&(hello.identity.len() as u64).to_be_bytes());
        mac.update(hello.identity.as_bytes());
        mac.update(&hello.min_version.to_be_bytes());
        mac.update(&hello.max_version.to_be_bytes());
        mac.update(&hello.capabilities.to_be_bytes());
        mac.update(&(hello.challenge.len() as u64).to_be_bytes());
        mac.update(&hello.challenge);
    }
    mac.finalize().into_bytes().to_vec()
}

pub fn verify_client_auth(
    secret: Option<&[u8]>,
    dialer: bool,
    client: &client::Hello,
    server: &client::Hello,
    proof: &[u8],
) -> bool {
    match secret {
        None => proof.is_empty(),
        Some(secret) => {
            // Compare using a MAC verification operation, avoiding data-dependent equality.
            use hmac::{KeyInit, Mac};
            let expected = client_auth_proof(secret, dialer, client, server);
            let mut expected_mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret)
                .expect("HMAC accepts any key size");
            expected_mac.update(&expected);
            let tag = expected_mac.finalize().into_bytes();
            let mut supplied_mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret)
                .expect("HMAC accepts any key size");
            supplied_mac.update(proof);
            supplied_mac.verify_slice(&tag).is_ok() && proof.len() == 32
        }
    }
}

pub fn valid_client_hello(hello: &client::Hello) -> bool {
    !hello.identity.is_empty()
        && hello.identity.len() <= 255
        && hello.challenge.len() == 32
        && hello.challenge.iter().any(|byte| *byte != 0)
        && hello.min_version != 0
        && hello.min_version <= CLIENT_PROTOCOL_VERSION
        && hello.max_version >= CLIENT_PROTOCOL_VERSION
}

/// Allocation-free protobuf preflight. Bounds repeated fields before Prost
/// materializes empty nested messages. Rejects duplicate envelope/operation
/// variants rather than accepting protobuf's last-one-wins interpretation.
pub fn preflight_client_frame(bytes: &[u8]) -> Result<(), &'static str> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Context {
        Frame,
        Request,
        BatchGet,
        BatchPut,
        Query,
        Response,
        QueryResult,
        Row,
        Inspection,
        Health,
        Placement,
        Cell,
        Put,
        Delete,
        MutationResult,
    }
    fn varint(bytes: &[u8], pos: &mut usize) -> Result<u64, &'static str> {
        let mut result = 0;
        for shift in (0..70).step_by(7) {
            let byte = *bytes.get(*pos).ok_or("truncated varint")?;
            *pos += 1;
            if shift == 63 && byte > 1 {
                return Err("overflowing varint");
            }
            result |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(result);
            }
        }
        Err("overflowing varint")
    }
    fn walk(bytes: &[u8], context: Context) -> Result<(), &'static str> {
        use Context::*;
        let mut pos = 0;
        let mut counts = [0usize; 10];
        let mut variants = 0;
        while pos < bytes.len() {
            let tag = varint(bytes, &mut pos)?;
            let field = tag >> 3;
            if field == 0 {
                return Err("invalid field");
            }
            if (context == Frame && (1..=4).contains(&field))
                || (context == Request && (2..=9).contains(&field))
                || (context == Cell && (1..=4).contains(&field))
            {
                variants += 1;
                if variants > 1 || (context != Cell && tag & 7 != 2) {
                    return Err("duplicate or invalid message");
                }
            }
            let limit = match (context, field) {
                (BatchGet | BatchPut, 1) | (Query, 2) | (Response, 3 | 4) => Some(MAX_BATCH_ITEMS),
                (Response, 7) | (QueryResult, 2) | (Inspection, 4 | 5) => Some(100_000),
                (QueryResult, 1) | (Row, 2) => Some(32),
                (Health, 3 | 4) | (Placement, 2) => Some(MAX_BATCH_ITEMS),
                _ => None,
            };
            if let Some(limit) = limit {
                let counter = counts
                    .get_mut(field as usize)
                    .ok_or("invalid repeated field")?;
                *counter += 1;
                if *counter > limit || tag & 7 != 2 {
                    return Err("invalid or excessive repeated field");
                }
            }
            if context == Response && [5, 6].contains(&field) {
                counts[field as usize] += 1;
                if counts[field as usize] > 1 {
                    return Err("duplicate structured response");
                }
            }
            match tag & 7 {
                0 => {
                    varint(bytes, &mut pos)?;
                }
                1 => {
                    pos = pos.checked_add(8).ok_or("overflow")?;
                }
                5 => {
                    pos = pos.checked_add(4).ok_or("overflow")?;
                }
                2 => {
                    let length =
                        usize::try_from(varint(bytes, &mut pos)?).map_err(|_| "overflow")?;
                    let end = pos.checked_add(length).ok_or("overflow")?;
                    let inner = bytes.get(pos..end).ok_or("truncated field")?;
                    if matches!((context, field), (Put, 5) | (Delete, 3))
                        && inner.len() > write_concern::MAX_WRITE_CONCERN_POLICY_BYTES
                    {
                        return Err("write concern policy exceeds byte limit");
                    }
                    if matches!((context, field), (MutationResult, 3)) {
                        write_concern::preflight_result(inner)
                            .map_err(|_| "invalid write concern receipt")?;
                    }
                    let nested = match (context, field) {
                        (Frame, 3) => Some(Request),
                        (Frame, 4) => Some(Response),
                        (Request, 5) => Some(BatchGet),
                        (Request, 3) | (BatchPut, 1) => Some(Put),
                        (Request, 4) => Some(Delete),
                        (Response, 4) => Some(MutationResult),
                        (Request, 6) => Some(BatchPut),
                        (Request, 7) => Some(Query),
                        (Query, 2) | (Row, 2) => Some(Cell),
                        (Response, 5) => Some(QueryResult),
                        (Response, 6) => Some(Inspection),
                        (QueryResult, 2) => Some(Row),
                        (Inspection, 4) => Some(Health),
                        (Inspection, 5) => Some(Placement),
                        _ => None,
                    };
                    if let Some(nested) = nested {
                        walk(inner, nested)?;
                    }
                    pos = end;
                }
                _ => return Err("invalid wire type"),
            }
            if pos > bytes.len() {
                return Err("truncated field");
            }
        }
        if context == Frame && variants != 1 {
            return Err("missing message");
        }
        Ok(())
    }
    walk(bytes, Context::Frame)
}
