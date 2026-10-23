// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Raw generated protobuf specifications for over-the-wire communication.
//!
//! This compatibility module is intentionally hidden from generated
//! documentation. The supported schema surface is the `actordb-protocol`
//! crate; application code should use the high-level `actordb` APIs.

/// Meta types which include all base network protocol message types
pub mod meta {
    pub use actordb_protocol::meta::*;
}

pub use meta::batch_get_response::KeyValue as BatchKeyValue;
pub use meta::envelope::Msg as EnvelopeMsg;
pub use meta::query_cell;
pub use meta::{
    BatchAck, BatchGet, BatchGetResponse, BatchNack, BatchPut, Envelope, ForwardedGet,
    ForwardedPut, GetResponse, Hello, HelloAuth, KeySummary, MutationResponse, MutationResult,
    MutationStatus, Priority, QueryCell, QueryPageRequest, QueryPageResponse, QueryPageRow,
    QueryPageStatus, ReconcileData, ReconcileFetch, ReconcileRequest, ReconcileResponse,
    ReplicateBatch, ReplicateOp, ReplicationAckLevel, ScanMatch as ScanMatchProto, ScanRequest,
    ScanResponse, SessionStart, SyncEntry, SyncRequest, SyncResponse,
};
