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
//! The scan response is a single envelope per fragment. The per-fragment
//! `ScanSpec::limit` bounds it: `limit` × average value size must stay
//! comfortably under the 16 MiB frame cap. Keep limits tight and prefer
//! `KeyOnly`/`PrefixBytes` transforms for wide scans.
//!
//! ## Consistency
//!
//! Each fragment is scanned once, at its own instant, from a single
//! replica. There is no snapshot isolation across fragments: concurrent
//! writes may land between two fragments' scans, and a forwarded scan may
//! read a replica that has not yet converged. This matches the system's
//! AP/eventual-consistency contract. See [`crate::actor::scan`] for the
//! per-fragment semantics (visibility, ordering, backpressure).

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use futures::future::join_all;
use ractor::rpc::CallResult;
use tokio::sync::oneshot;

use super::{
    deadline_after, try_send_envelope_until, Node, NodeError, PeerChannel, PendingScan,
    PendingScanGuard,
};
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
    /// Distributed scan: apply `spec` to every fragment in the keyspace
    /// and merge the transformed matches.
    ///
    /// This node coordinates: each fragment is scanned once, from a single
    /// holder — in-process via `KvMessage::Scan` when held locally,
    /// otherwise via a forwarded `ScanRequest` to the first reachable
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
        let _operation = self
            .admit_operation()
            .map_err(|_| ScanError::ShuttingDown)?;

        let fragment_count = self.router.fragment_count();
        let has_placement = !self.fragment_map.read().unwrap().is_empty();

        // Fan out per fragment concurrently; latency is max(fragment),
        // not sum.
        let futures = (0..fragment_count).map(|index| {
            let fragment = FragmentId(index as u32);
            self.scan_one_fragment(fragment, spec.clone(), has_placement)
        });
        let results = join_all(futures).await;

        // Merge. Unavailable fragments fail the whole scan; other errors
        // propagate immediately. v1 never returns partial results.
        let mut merged = Vec::new();
        let mut unavailable = Vec::new();
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(matches) => {
                    let remaining = MAX_SCAN_TOTAL_MATCHES.saturating_sub(merged.len());
                    merged.extend(matches.into_iter().take(remaining));
                }
                Err(ScanError::FragmentsUnavailable(_)) => {
                    unavailable.push(FragmentId(index as u32));
                }
                Err(error) => return Err(error),
            }
        }
        if !unavailable.is_empty() {
            return Err(ScanError::FragmentsUnavailable(unavailable));
        }
        Ok(merged)
    }

    /// Scan one fragment: in-process when held locally, otherwise forward
    /// to a replica holder with failover.
    async fn scan_one_fragment(
        &self,
        fragment: FragmentId,
        spec: ScanSpec,
        has_placement: bool,
    ) -> Result<Vec<ScanMatch>, ScanError> {
        if self.serving.read().unwrap().contains(&fragment) {
            return self.scan_fragment_local(fragment, &spec).await;
        }
        if !has_placement {
            // No placement map means full replication: every fragment is
            // local. Not serving here is a misconfiguration.
            return Err(ScanError::Actor(format!(
                "fragment {fragment} not served locally and no placement map configured"
            )));
        }
        self.scan_fragment_remote(fragment, &spec).await
    }

    /// Run the scan against this node's own fragment actor.
    async fn scan_fragment_local(
        &self,
        fragment: FragmentId,
        spec: &ScanSpec,
    ) -> Result<Vec<ScanMatch>, ScanError> {
        let Some(actor) = self.router.route_for_fragment(fragment) else {
            return Err(ScanError::Actor(format!(
                "fragment {fragment} is marked serving but has no actor"
            )));
        };
        match actor
            .call(
                |reply| KvMessage::Scan {
                    spec: spec.clone(),
                    reply,
                },
                Some(SCAN_FRAGMENT_TIMEOUT),
            )
            .await
        {
            Ok(CallResult::Success(Ok(matches))) => Ok(matches),
            Ok(CallResult::Success(Err(shed))) => Err(ScanError::Backpressure(format!(
                "fragment {fragment} shed scan: {shed}"
            ))),
            Ok(CallResult::Timeout) => Err(ScanError::Actor(format!(
                "fragment {fragment} scan timed out"
            ))),
            Ok(CallResult::SenderError) => Err(ScanError::Actor(format!(
                "fragment {fragment} actor unavailable"
            ))),
            Err(_) => Err(ScanError::Actor(format!(
                "fragment {fragment} scan call failed"
            ))),
        }
    }

    /// Forward the scan to a replica holder, failing over across
    /// `replica_order`. Mirrors the `ForwardedGet` remote path: transport,
    /// timeout, and holder-refusal all retry the next holder; only a
    /// successful response is terminal.
    async fn scan_fragment_remote(
        &self,
        fragment: FragmentId,
        spec: &ScanSpec,
    ) -> Result<Vec<ScanMatch>, ScanError> {
        let replicas = self.replica_order(fragment);
        if replicas.is_empty() {
            return Err(ScanError::FragmentsUnavailable(vec![fragment]));
        }
        let mut tried: Vec<String> = Vec::with_capacity(replicas.len());
        for owner in replicas {
            tried.push(owner.clone());
            match self.scan_fragment_from_owner(&owner, fragment, spec).await {
                Ok(matches) => return Ok(matches),
                Err(error) => {
                    tracing::warn!(
                        %fragment,
                        owner = %owner,
                        %error,
                        "scan of remote fragment failed, failing over"
                    );
                }
            }
        }
        Err(ScanError::FragmentsUnavailable(vec![fragment]))
    }

    /// One scan attempt against one holder.
    async fn scan_fragment_from_owner(
        &self,
        owner: &str,
        fragment: FragmentId,
        spec: &ScanSpec,
    ) -> Result<Vec<ScanMatch>, ScanError> {
        let Some(tx) = self.peer_sender(owner).await else {
            return Err(ScanError::Actor(format!(
                "not connected to scan holder {owner}"
            )));
        };
        let req_id = self
            .next_req_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending_scans.lock().unwrap().insert(
            req_id,
            PendingScan {
                expected_peer: owner.to_string(),
                reply: resp_tx,
            },
        );
        let _guard = PendingScanGuard {
            req_id,
            pending: self.pending_scans.clone(),
        };

        let deadline = deadline_after(SCAN_FRAGMENT_TIMEOUT);
        let request = spec.to_scan_request(req_id, fragment.0);
        if !try_send_envelope_until(
            &tx,
            Envelope {
                msg: Some(EnvelopeMsg::ScanRequest(request)),
            },
            deadline,
        )
        .await
        {
            return Err(ScanError::Actor(format!(
                "could not send scan request to {owner}"
            )));
        }

        match tokio::time::timeout_at(deadline, resp_rx).await {
            Ok(Ok(Ok(proto_matches))) => {
                Ok(proto_matches.iter().map(ScanMatch::from_proto).collect())
            }
            Ok(Ok(Err(error))) => Err(ScanError::Actor(format!(
                "holder {owner} refused scan of fragment {fragment}: {error}"
            ))),
            _ => Err(ScanError::Actor(format!(
                "scan of fragment {fragment} via {owner} timed out"
            ))),
        }
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
            |reply| KvMessage::Scan { spec, reply },
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

    let proto_matches: Vec<proto::ScanMatchProto> =
        matches.into_iter().map(|m| m.to_proto()).collect();
    send_scan_response(&tx, req_id, proto_matches, String::new()).await;
}

async fn send_scan_response(
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
