// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Node-level distributed scan: [`Node::scan`](super::Node::scan) is a
//! server-side coordinator, exactly like [`Node::get`](super::Node::get).
//!
//! The client talks to any one node; that node coordinates the whole scan.
//! For each fragment the coordinator scans in-process when it holds the
//! fragment locally (via `KvMessage::Scan` on the fragment's background
//! pool), and otherwise forwards a `ScanRequest` to one replica holder from
//! `replica_order(frag)` — the same pattern as `ForwardedGet`. Holders are
//! retried in order on failure; exactly one holder is contacted per
//! fragment, so replicas (which converge via LWW) never double-count.
//!
//! If no holder is reachable for a fragment, the whole scan fails naming
//! the fragment ([`ScanError::FragmentsUnavailable`]). v1 never returns
//! silent partial results.
//!
//! The default configuration is **full replication** (no `fragment_map`),
//! in which every node serves every fragment and a scan never touches the
//! network.
//!
//! ## Wire size
//!
//! Embedded scans use bounded query pages with a 16 MiB aggregate result
//! budget and a one-million-record work budget. Compatibility peer requests
//! also execute bounded pages before encoding their single response. Wide
//! replies fail completely; prefer `KeyOnly`/`PrefixBytes` transforms.
//!
//! ## Consistency
//!
//! Each fragment is scanned once, at its own instant, from a single
//! replica. There is no snapshot isolation across fragments: concurrent
//! writes may land between two fragments' scans, and a forwarded scan may
//! read a replica that has not yet converged. This matches the system's
//! AP/eventual-consistency contract. See [`crate::actor`] for the
//! per-fragment semantics (visibility, ordering, backpressure).

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use ractor::rpc::CallResult;

use super::{deadline_after, try_send_envelope_until, Node, NodeError, PeerChannel};
use crate::actor::{KvMessage, ScanMatch, ScanSpec};
use crate::proto::{self, Envelope, EnvelopeMsg, ScanResponse};
use crate::shard::FragmentId;

/// Cap on total merged matches returned by [`Node::scan`](super::Node::scan).
/// Load-bearing: bounds the merged reply independent of fragment count.
/// The per-fragment `ScanSpec::limit` applies first; this is the
/// coordinator-side safety net.
pub const MAX_SCAN_TOTAL_MATCHES: usize = 100_000;

/// Timeout for a single fragment scan, local or remote. The local scan is
/// bounded by the spec's effective limit; remote scans add one network
/// round trip. Scans are bulk work, so this is deliberately longer than
/// the 5s point-read timeout.
const SCAN_FRAGMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Error type for [`Node::scan`](super::Node::scan).
#[derive(Debug)]
pub enum ScanError {
    /// No reachable holder for these fragments: not served locally and no
    /// replica holder answered (or no replica is configured). v1 fails the
    /// whole scan rather than returning silent partial results.
    FragmentsUnavailable(Vec<FragmentId>),
    /// A fragment's scan was shed by the background pool's admission
    /// control. Retry with backoff or reduce load.
    Backpressure(String),
    /// A fragment's scan could not complete (actor unavailable, timed
    /// out, or call failed).
    Actor(String),
    /// The node has begun shutdown; no new operations are admitted.
    ShuttingDown,
}

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanError::FragmentsUnavailable(fragments) => {
                write!(f, "no reachable holder for fragments: {fragments:?}")
            }
            ScanError::Backpressure(reason) => write!(f, "scan shed by backpressure: {reason}"),
            ScanError::Actor(reason) => write!(f, "scan actor error: {reason}"),
            ScanError::ShuttingDown => write!(f, "node is shutting down"),
        }
    }
}

impl std::error::Error for ScanError {}

impl Node {
    /// Scan through bounded query pages while retaining the legacy per-fragment
    /// offset/limit and fragment-order merge. Replies fail completely at a
    /// 16 MiB aggregate storage budget or one million visited records.
    /// Fragment work is sequential, keeping active page copies bounded.
    pub async fn scan_bounded(
        &self,
        spec: &ScanSpec,
    ) -> Result<Vec<ScanMatch>, crate::query::QueryExecutionError> {
        use crate::query::{QueryExecutionError, QueryOptions};
        use actordb_query::QueryValue;
        let _operation = self
            .admit_operation()
            .map_err(|_| QueryExecutionError::ShuttingDown)?;
        // Covers previously merged rows while each fragment owns its own
        // page reservation; holding both is deliberately conservative.
        let request_bytes = match &spec.matcher {
            crate::actor::KeyMatcher::All => 1024,
            crate::actor::KeyMatcher::Prefix(prefix) => {
                prefix.capacity().saturating_mul(2).saturating_add(1024)
            }
        };
        let _runtime = self.query_admission.reserve_work(
            request_bytes,
            QueryOptions::default().max_result_bytes + QueryOptions::default().max_buffer_bytes,
        )?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        tokio::time::timeout_at(deadline, async {
            let mut merged = Vec::new();
            let mut retained = 0usize;
            let mut retained_payload = 0usize;
            let mut visited = 0u64;
            for fragment in 0..self.router.fragment_count() {
                let mut plan = spec.query_plan(Some(fragment as u32));
                plan.limit = Some(spec.effective_limit().min(MAX_SCAN_TOTAL_MATCHES as u64));
                // Even after the aggregate count cap, validate later fragments
                // so an unavailable holder never produces silent partial success.
                if merged.len() == MAX_SCAN_TOTAL_MATCHES {
                    plan.projections = vec![actordb_query::Projection::Key];
                }
                let remaining = if merged.len() == MAX_SCAN_TOTAL_MATCHES {
                    16 * 1024 * 1024usize
                } else {
                    (16 * 1024 * 1024usize).saturating_sub(retained)
                };
                let options = QueryOptions {
                    max_result_bytes: remaining,
                    max_scanned_rows: 1_000_000u64.saturating_sub(visited),
                    timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                    ..Default::default()
                };
                if options.max_scanned_rows == 0 {
                    return Err(QueryExecutionError::BudgetExceeded(
                        "scan visited record limit".into(),
                    ));
                }
                let (result, work) = self
                    .query_plan_with_options_and_visits(&plan, options)
                    .await?;
                visited += work;
                for row in result.rows {
                    if merged.len() == MAX_SCAN_TOTAL_MATCHES {
                        break;
                    }
                    let mut cells = row.into_iter();
                    let Some(QueryValue::Text(key)) = cells.next() else {
                        return Err(QueryExecutionError::Protocol("scan key projection".into()));
                    };
                    let value = match cells.next() {
                        Some(QueryValue::Bytes(bytes)) => bytes,
                        None => Vec::new(),
                        _ => {
                            return Err(QueryExecutionError::Protocol(
                                "scan value projection".into(),
                            ))
                        }
                    };
                    retained_payload = retained_payload
                        .saturating_add(key.capacity())
                        .saturating_add(value.capacity());
                    let capacity = if merged.len() == merged.capacity() {
                        merged.capacity().saturating_mul(2).max(4)
                    } else {
                        merged.capacity()
                    };
                    retained = retained_payload
                        .saturating_add(capacity.saturating_mul(std::mem::size_of::<ScanMatch>()));
                    if retained > 16 * 1024 * 1024 {
                        return Err(QueryExecutionError::BudgetExceeded(
                            "scan result storage".into(),
                        ));
                    }
                    merged.push(ScanMatch { key, value });
                }
            }
            Ok(merged)
        })
        .await
        .map_err(|_| QueryExecutionError::Timeout)?
    }

    /// Distributed scan: apply `spec` to every fragment in the keyspace
    /// and merge the transformed matches.
    ///
    /// This node coordinates: each fragment is scanned once, from a single
    /// holder — in-process through bounded query pages when held locally,
    /// otherwise via forwarded query pages to the first reachable
    /// holder in `replica_order` (same pattern as `ForwardedGet`). Remote
    /// holders are retried in order; if none answers, the whole scan fails
    /// with [`ScanError::FragmentsUnavailable`]. v1 never returns partial
    /// results.
    ///
    /// Results merge in fragment order (each fragment's matches in
    /// lexicographic key order), truncated at [`MAX_SCAN_TOTAL_MATCHES`].
    /// In the default full-replication configuration every fragment is
    /// local and no network traffic occurs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use actordb::actor::{KeyMatcher, ScanSpec, ValueTransform};
    /// use actordb::node::{Node, NodeConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let node = Node::start(
    ///     "n1".to_string(),
    ///     "127.0.0.1:0".parse()?,
    ///     NodeConfig::default(),
    /// ).await?;
    /// node.put("user:1".to_string(), b"alice".to_vec()).await?;
    /// node.put("user:2".to_string(), b"bob".to_vec()).await?;
    ///
    /// let matches = node.scan(&ScanSpec {
    ///     matcher: KeyMatcher::Prefix(b"user:".to_vec()),
    ///     transform: ValueTransform::Identity,
    ///     limit: 1000,
    ///     offset: 0,
    /// }).await?;
    /// assert_eq!(matches.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scan(&self, spec: &ScanSpec) -> Result<Vec<ScanMatch>, ScanError> {
        use crate::query::QueryExecutionError;
        let mut scoped = spec.clone();
        scoped.matcher = match &spec.matcher {
            crate::actor::KeyMatcher::All => {
                crate::actor::KeyMatcher::Prefix(crate::table_key::table_prefix("KV").into_bytes())
            }
            crate::actor::KeyMatcher::Prefix(prefix)
                if std::str::from_utf8(prefix)
                    .ok()
                    .and_then(crate::table_key::table_name)
                    .is_some() =>
            {
                crate::actor::KeyMatcher::Prefix(prefix.clone())
            }
            crate::actor::KeyMatcher::Prefix(prefix) => crate::actor::KeyMatcher::Prefix(
                crate::table_key::encode_table_prefix("KV", prefix),
            ),
        };
        let mut rows = self
            .scan_bounded(&scoped)
            .await
            .map_err(|error| match error {
                QueryExecutionError::FragmentsUnavailable(ids) => {
                    ScanError::FragmentsUnavailable(ids.into_iter().map(FragmentId).collect())
                }
                QueryExecutionError::Backpressure(reason) => ScanError::Backpressure(reason),
                QueryExecutionError::ShuttingDown => ScanError::ShuttingDown,
                other => ScanError::Actor(other.to_string()),
            })?;
        // The unqualified Node API is the named `KV` table view. Keep its
        // public scan keys in user-key form; other named tables remain
        // encoded until their `Table` handle decodes them.
        for row in &mut rows {
            if crate::table_key::table_name(&row.key).as_deref() == Some("KV") {
                row.key = crate::table_key::decode_table_key("KV", &row.key)
                    .ok_or_else(|| ScanError::Actor("invalid KV table key namespace".into()))?;
            }
        }
        Ok(rows)
    }
}

/// Serve an inbound `ScanRequest`: run the local scan and reply with a
/// single bounded `ScanResponse`. Spawned per request so the connection
/// read loop never blocks on scan work; the scan itself runs in the
/// fragment's background pool under admission control.
pub(super) async fn serve_scan_request(
    tx: PeerChannel,
    serving: Arc<StdRwLock<HashSet<FragmentId>>>,
    router: super::FragmentRouter,
    req: proto::ScanRequest,
    retention: Arc<crate::query::QueryRetention>,
) {
    let fragment = FragmentId(req.fragment);
    let req_id = req.req_id;

    if !serving.read().unwrap().contains(&fragment) {
        send_scan_response(
            &tx,
            req_id,
            Vec::new(),
            format!("not serving fragment {fragment}"),
        )
        .await;
        return;
    }
    let spec = match ScanSpec::from_scan_request(&req) {
        Ok(spec) => spec,
        Err(error) => {
            send_scan_response(
                &tx,
                req_id,
                Vec::new(),
                format!("invalid scan spec: {error}"),
            )
            .await;
            return;
        }
    };
    let Some(actor) = router.route_for_fragment(fragment) else {
        send_scan_response(
            &tx,
            req_id,
            Vec::new(),
            format!("no actor for fragment {fragment}"),
        )
        .await;
        return;
    };

    let matches = match actor
        .call(
            |reply| KvMessage::ScanRetained {
                spec,
                retention: retention.clone(),
                reply,
            },
            Some(SCAN_FRAGMENT_TIMEOUT),
        )
        .await
    {
        Ok(CallResult::Success(Ok(matches))) => matches,
        Ok(CallResult::Success(Err(shed))) => {
            send_scan_response(&tx, req_id, Vec::new(), format!("scan shed: {shed}")).await;
            return;
        }
        _ => {
            send_scan_response(
                &tx,
                req_id,
                Vec::new(),
                "scan actor call failed".to_string(),
            )
            .await;
            return;
        }
    };

    let proto_matches: Vec<proto::ScanMatchProto> = matches
        .into_iter()
        .map(|m| proto::ScanMatchProto {
            key: m.key,
            value: m.value,
        })
        .collect();
    send_scan_response(&tx, req_id, proto_matches, String::new()).await;
}

pub(super) async fn send_scan_response(
    tx: &PeerChannel,
    req_id: u64,
    matches: Vec<proto::ScanMatchProto>,
    error: String,
) {
    let _ = try_send_envelope_until(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::ScanResponse(ScanResponse {
                req_id,
                matches,
                error,
            })),
        },
        deadline_after(Duration::from_secs(10)),
    )
    .await;
}

impl From<ScanError> for NodeError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::FragmentsUnavailable(fragments) => {
                NodeError::Actor(format!("no reachable holder for fragments: {fragments:?}"))
            }
            ScanError::Backpressure(reason) => NodeError::Backpressure(reason),
            ScanError::Actor(reason) => NodeError::Actor(reason),
            ScanError::ShuttingDown => NodeError::ShuttingDown,
        }
    }
}
