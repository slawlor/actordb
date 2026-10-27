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

/// Peer framing limit; independent of the application RPC adapter.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RPC_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_ITEMS: usize = 4096;
pub const RPC_SERVICE_NAME: &str = "actordb.rpc.v1.ActorDb";
pub const RPC_CALL_PATH: &str = "/actordb.rpc.v1.ActorDb/Call";

mod client_decode;
pub use client_decode::{
    preflight_rpc_request, preflight_rpc_response, preflight_rpc_response_unchecked_shape,
    RpcDecodeReport, RpcResponseShape, DEFAULT_MAX_DECODED_RESPONSE_BYTES,
    PORTABLE_CELL_HEADER_CHARGE,
};

#[cfg(feature = "grpc")]
pub mod grpc;
/// Framework-neutral client/service traits, ownership, and request envelopes.
pub mod rpc;
