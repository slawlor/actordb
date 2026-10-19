//! Conditional mutations with stable majority commits, independent of the LWW
//! [`crate::node::Node`]. Enable the `quorum` feature. See `docs/quorum.md` for
//! recovery, membership, retry, change retention, and storage contracts.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actordb_protocol::quorum as wire;

use crate::network::{Network, TcpNetwork};
use crate::shard::FragmentId;

mod engine;
mod state;
mod store;
mod transport;

pub use engine::{ChangeSubscription, QuorumNode};
pub use store::{FileQuorumStore, QuorumStore};
pub use wire::{Ballot, Change, ChangeKind, Manifest, Membership, Record};

/// A condition checked against the committed record after expiry processing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Expectation {
    #[default]
    Any,
    Absent,
    /// Match the exact record incarnation, including across delete/recreate.
    Revision(u64),
}

/// A retriable single-key mutation. Preserve every field when retrying.
///
/// `valid_until_ms` is an absolute Unix millisecond deadline. Once it expires,
/// the request is rejected even if its receipt has been reclaimed. A request
/// ID must be unique within its fragment until that deadline.
#[derive(Debug, Clone, bon::Builder)]
pub struct Mutation {
    #[builder(into)]
    pub request_id: String,
    pub valid_until_ms: u64,
    #[builder(into)]
    pub key: String,
    #[builder(default)]
    pub expectation: Expectation,
    /// `Some` writes bytes; `None` deletes the record.
    pub value: Option<Vec<u8>>,
    /// An absolute expiry deadline; only valid for a write.
    pub expires_at_ms: Option<u64>,
}

impl Mutation {
    fn into_wire(self) -> wire::Mutation {
        let (condition, expected_revision) = match self.expectation {
            Expectation::Any => (wire::Condition::Any, 0),
            Expectation::Absent => (wire::Condition::Absent, 0),
            Expectation::Revision(revision) => (wire::Condition::Revision, revision),
        };
        wire::Mutation {
            request_id: self.request_id,
            valid_until_ms: self.valid_until_ms,
            key: self.key,
            condition: condition as i32,
            expected_revision,
            value: self.value,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

/// Exact stable quorum boundary reached by an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    pub revision: u64,
    pub term: Ballot,
    /// Distinct stable acceptors. Joint transitions require both majorities.
    pub acknowledgements: Vec<String>,
    pub voting_membership: Membership,
}

/// Conditional mismatch is an ordinary committed result, with `applied=false`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationResult {
    pub applied: bool,
    pub current: Option<Record>,
    /// The original mutation's revision, preserved on replay.
    pub revision: u64,
    pub commit: CommitReceipt,
}

/// One bounded, durable changefeed page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePage {
    pub changes: Vec<Change>,
    /// Resume after this ID. Advances only through returned changes.
    pub cursor: u64,
    pub last_change_id: u64,
    pub commit: CommitReceipt,
}

/// A consistent fragment view and the matching changefeed resume position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentSnapshot {
    pub records: Vec<(String, Record)>,
    pub last_change_id: u64,
    pub time_ms: u64,
    pub commit: CommitReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumError {
    Invalid(String),
    Storage(String),
    Unavailable,
    /// An attempt may have committed. Retry the identical mutation.
    UnknownOutcome,
    RequestExpired,
    RequestConflict,
    CursorExpired,
    ResourceLimit(String),
    ShuttingDown,
}

impl fmt::Display for QuorumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => write!(f, "invalid quorum operation: {s}"),
            Self::Storage(s) => write!(f, "quorum storage error: {s}"),
            Self::Unavailable => f.write_str("no quorum is available"),
            Self::UnknownOutcome => {
                f.write_str("operation may have committed; retry the same request")
            }
            Self::RequestExpired => f.write_str("request deadline expired"),
            Self::RequestConflict => f.write_str("request ID reused with different contents"),
            Self::CursorExpired => f.write_str("change cursor is outside retained history"),
            Self::ResourceLimit(s) => write!(f, "quorum resource limit: {s}"),
            Self::ShuttingDown => f.write_str("quorum node is shutting down"),
        }
    }
}

impl std::error::Error for QuorumError {}

/// Every member must agree on genesis placement and state limits. Addresses
/// may change on restart; node identity and genesis placement may not.
#[derive(Clone, bon::Builder)]
pub struct QuorumConfig {
    #[builder(into)]
    pub cluster_id: String,
    pub store: Arc<dyn QuorumStore>,
    pub fragment_map: HashMap<FragmentId, Vec<String>>,
    /// Address book including every initial and prospective voter.
    pub members: HashMap<String, SocketAddr>,
    #[builder(default = Arc::new(TcpNetwork))]
    pub network: Arc<dyn Network>,
    /// Independent credentials for the quorum endpoint. At least 32 bytes.
    pub cluster_secret: Option<Vec<u8>>,
    #[builder(default = Duration::from_secs(5))]
    pub operation_timeout: Duration,
    #[builder(default = Duration::from_secs(1))]
    pub rpc_timeout: Duration,
    #[builder(default = Duration::from_millis(100))]
    pub maintenance_interval: Duration,
    #[builder(default = Duration::from_secs(3600))]
    pub max_request_lifetime: Duration,
    #[builder(default = Duration::from_secs(3600))]
    pub change_retention: Duration,
    #[builder(default = 10_000)]
    pub max_records: usize,
    #[builder(default = 10_000)]
    pub max_receipts: usize,
    #[builder(default = 10_000)]
    pub max_changes: usize,
    /// Bounds complete protocol states, including an accepted successor.
    #[builder(default = 4 * 1024 * 1024)]
    pub max_state_bytes: usize,
    #[builder(default = 128)]
    pub max_inflight: usize,
}
