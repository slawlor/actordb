// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Phase 5: async sender/receiver replication.
//!
//! Replication moves off the write path. Per fragment, per replication
//! peer, a `ReplicationSender` (the task spawned by `spawn_sender`) task owns a state machine:
//!
//! ```text
//!   Disconnected ──PeerConnected──▶ Connected ⇄ (batching, acks)
//!        │                              │
//!        │                         buffer overflow
//!        │                              ▼
//!        └─────────────────────▶ BufferReset ──PeerConnected──▶ reconciling ──▶ Connected
//! ```
//!
//! - **Connected**: mutations (writes + tombstoned deletes) accumulate
//!   in a bounded local buffer. The sender ships batches on a timer,
//!   keeps a bounded number of batches in flight, and drops acked
//!   records. Each sender↔receiver pairing gets a fresh `session_id`;
//!   the receiver discards batches from stale sessions.
//! - **Disconnected**: the peer is down; the buffer is retained for
//!   resume on reconnect.
//! - **BufferReset**: the buffer overflowed while the peer was down.
//!   The buffer is shed; on reconnect the sender reconciles via
//!   timestamp/tombstone comparison instead of replaying. While any
//!   sender is in this state, tombstone GC is blocked (see below).
//!
//! Tombstone GC blocking: if a tombstone were GC'd locally while a
//! peer was down, reconciliation could resurrect the deleted key (the
//! peer still holds the live value, we no longer know it was deleted).
//! Senders register themselves in `reset_senders` while in
//! `BufferReset`; the fragment's cleaner skips tombstone GC while its
//! fragment appears in the set.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use ractor::rpc::CallResult;
use tokio::sync::{mpsc, Mutex};

use crate::actor::{KvMessage, VersionedValue};
use crate::node::FragmentRouter;
use crate::proto::{
    Envelope, EnvelopeMsg, KeySummary, ReconcileFetch, ReconcileRequest, ReplicateBatch,
    ReplicateOp as ProtoReplicateOp, SessionStart, SyncEntry,
};
use crate::shard::FragmentId;

/// Tuning knobs for the replication sender. All fields are public so
/// embedders can tune; `Default` is sane for tests and small clusters.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Max ops per batch.
    pub batch_size: usize,
    /// Ship a batch at least this often when the buffer is non-empty.
    pub batch_interval: Duration,
    /// Max batches in flight awaiting acks.
    pub max_in_flight: usize,
    /// Max buffered (unshipped) ops per sender. Overflow sheds the
    /// buffer and enters `BufferReset`.
    pub buffer_capacity: usize,
    /// How often a connected sender runs anti-entropy reconciliation
    /// to repair divergence.
    pub consistency_interval: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_interval: Duration::from_millis(100),
            max_in_flight: 4,
            buffer_capacity: 10_000,
            consistency_interval: Duration::from_secs(300),
        }
    }
}

/// Messages driving a `ReplicationSender` (the task spawned by `spawn_sender`).
pub enum SenderMsg {
    /// A locally-applied mutation to replicate.
    Enqueue(crate::actor::ReplicatedOp),
    /// The peer (re)connected; start a new session on this channel.
    PeerConnected { peer_tx: mpsc::Sender<Vec<u8>> },
    /// The peer connection dropped; retain the buffer.
    PeerDisconnected,
    /// The peer acked a batch.
    Ack { session_id: u64, batch_seq: u64 },
    /// Response to our reconcile request.
    ReconcileResponse { summaries: Vec<KeySummary> },
    /// Full values for keys we asked to fetch.
    ReconcileData { entries: Vec<SyncEntry> },
}

/// A batch awaiting its ack.
struct InflightBatch {
    batch_seq: u64,
    ops: Vec<crate::actor::ReplicatedOp>,
}

enum SenderState {
    Disconnected,
    Connected {
        session_id: u64,
        next_batch_seq: u64,
        in_flight: VecDeque<InflightBatch>,
    },
    BufferReset,
}

/// A snapshot of a sender's state, for health checks and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SenderStateReport {
    /// "disconnected", "connected", or "buffer_reset".
    pub state: String,
    /// Current session id (0 if none).
    pub session_id: u64,
    /// Ops currently buffered.
    pub buffered: usize,
    /// Batches in flight.
    pub in_flight: usize,
}

/// Shared dependencies for a sender task.
pub struct SenderDeps {
    pub router: FragmentRouter,
    /// Senders currently in `BufferReset`, keyed by (fragment, peer).
    /// The cleaner consults this to block tombstone GC.
    pub reset_senders: Arc<Mutex<HashSet<(FragmentId, String)>>>,
    /// Where the sender publishes its state.
    pub state_report: Arc<Mutex<SenderStateReport>>,
}

/// Spawn a replication sender for (`fragment`, `peer_id`).
pub fn spawn_sender(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
) -> mpsc::UnboundedSender<SenderMsg> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(sender_task(fragment, peer_id, config, deps, rx));
    tx
}

fn proto_op(op: &crate::actor::ReplicatedOp) -> ProtoReplicateOp {
    ProtoReplicateOp {
        key: op.key.clone(),
        value: op.value.value.clone(),
        timestamp_ms: op.value.timestamp_ms,
        node_id: op.value.node_id.clone(),
        seq: op.seq,
        boot_id: op.boot_id,
        expires_at_ms: op.value.expires_at_ms.unwrap_or(0),
        is_tombstone: op.value.is_tombstone,
    }
}

async fn send_envelope(peer_tx: &mpsc::Sender<Vec<u8>>, msg: EnvelopeMsg) -> bool {
    let mut buf = Vec::new();
    {
        use prost::Message;
        let env = Envelope { msg: Some(msg) };
        if env.encode(&mut buf).is_err() {
            return false;
        }
    }
    peer_tx.send(buf).await.is_ok()
}

async fn sender_task(
    fragment: FragmentId,
    peer_id: String,
    config: SenderConfig,
    deps: SenderDeps,
    mut rx: mpsc::UnboundedReceiver<SenderMsg>,
) {
    let mut state = SenderState::Disconnected;
    let mut buffer: VecDeque<crate::actor::ReplicatedOp> = VecDeque::new();
    let mut peer_tx: Option<mpsc::Sender<Vec<u8>>> = None;
    let mut batch_timer = tokio::time::interval(config.batch_interval);
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consistency_timer = tokio::time::interval(config.consistency_interval);
    consistency_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // The sender only learns its session id on PeerConnected.
    let mut session_id: u64 = 0;

    loop {
        // Publish state for health checks and tests.
        {
            let (state_str, in_flight) = match &state {
                SenderState::Disconnected => ("disconnected".to_string(), 0),
                SenderState::Connected { in_flight, .. } => {
                    ("connected".to_string(), in_flight.len())
                }
                SenderState::BufferReset => ("buffer_reset".to_string(), 0),
            };
            *deps.state_report.lock().await = SenderStateReport {
                state: state_str,
                session_id,
                buffered: buffer.len(),
                in_flight,
            };
        }
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    SenderMsg::Enqueue(op) => {
                        if buffer.len() >= config.buffer_capacity {
                            // Shed the buffer and enter BufferReset. The op
                            // itself is applied locally regardless; it will
                            // be picked up by reconciliation.
                            buffer.clear();
                            if !matches!(state, SenderState::BufferReset) {
                                state = SenderState::BufferReset;
                                deps.reset_senders
                                    .lock()
                                    .await
                                    .insert((fragment, peer_id.clone()));
                            }
                        }
                        buffer.push_back(op);
                    }
                    SenderMsg::PeerConnected { peer_tx: tx } => {
                        session_id = rand_session_id();
                        peer_tx = Some(tx.clone());
                        // Requeue unacked ops at the front: the new session
                        // must not lose them, and redelivery is idempotent.
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            while let Some(b) = in_flight.pop_back() {
                                for op in b.ops.into_iter().rev() {
                                    buffer.push_front(op);
                                }
                            }
                        }
                        match state {
                            SenderState::BufferReset => {
                                // Don't go Connected yet: reconcile first.
                                send_envelope(
                                    &tx,
                                    EnvelopeMsg::SessionStart(SessionStart {
                                        session_id,
                                        fragment: fragment.0,
                                    }),
                                )
                                .await;
                                send_envelope(
                                    &tx,
                                    EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                                        fragment: fragment.0,
                                    }),
                                )
                                .await;
                            }
                            _ => {
                                state = SenderState::Connected {
                                    session_id,
                                    next_batch_seq: 0,
                                    in_flight: VecDeque::new(),
                                };
                                send_envelope(
                                    &tx,
                                    EnvelopeMsg::SessionStart(SessionStart {
                                        session_id,
                                        fragment: fragment.0,
                                    }),
                                )
                                .await;
                            }
                        }
                    }
                    SenderMsg::PeerDisconnected => {
                        peer_tx = None;
                        if !matches!(state, SenderState::BufferReset) {
                            state = SenderState::Disconnected;
                        }
                        // BufferReset survives disconnects: we still need
                        // to reconcile when the peer returns.
                    }
                    SenderMsg::Ack {
                        session_id: ack_session,
                        batch_seq,
                    } => {
                        if ack_session != session_id {
                            continue; // stale session ack
                        }
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            if let Some(pos) =
                                in_flight.iter().position(|b| b.batch_seq == batch_seq)
                            {
                                in_flight.remove(pos);
                            }
                        }
                    }
                    SenderMsg::ReconcileResponse { summaries } => {
                        if !matches!(state, SenderState::BufferReset) {
                            continue;
                        }
                        let Some(tx) = peer_tx.clone() else { continue };
                        let needs_fetch = reconcile(
                            fragment,
                            &deps.router,
                            &tx,
                            session_id,
                            summaries,
                        )
                        .await;
                        if !needs_fetch {
                            finish_reconcile(
                                fragment,
                                &peer_id,
                                &deps.reset_senders,
                                &mut state,
                                session_id,
                                peer_tx.clone(),
                            )
                            .await;
                        }
                        // Else: ReconcileData completes the round.
                    }
                    SenderMsg::ReconcileData { entries } => {
                        // Merge fetched values locally via LWW.
                        if let Some(actor) = deps.router.route_for_fragment(fragment) {
                            for e in entries {
                                let mut v = VersionedValue::new(
                                    e.value,
                                    e.timestamp_ms,
                                    e.node_id,
                                );
                                v.expires_at_ms = if e.expires_at_ms == 0 {
                                    None
                                } else {
                                    Some(e.expires_at_ms)
                                };
                                v.is_tombstone = e.is_tombstone;
                                let op = crate::actor::ReplicatedOp {
                                    key: e.key,
                                    value: v,
                                    seq: 0,
                                    boot_id: 0,
                                };
                                let _ = actor.send_message(KvMessage::ApplyOp(op));
                            }
                        }
                        // Reconciliation is complete once the data lands:
                        // leave BufferReset and go Connected.
                        finish_reconcile(
                            fragment,
                            &peer_id,
                            &deps.reset_senders,
                            &mut state,
                            session_id,
                            peer_tx.clone(),
                        )
                        .await;
                    }
                }
            }
            _ = batch_timer.tick() => {
                let (sid, tx) = match (&state, peer_tx.clone()) {
                    (SenderState::Connected { session_id, in_flight, .. }, Some(tx))
                        if !buffer.is_empty() && in_flight.len() < config.max_in_flight =>
                    {
                        (*session_id, tx)
                    }
                    _ => continue,
                };
                let batch_seq = match &mut state {
                    SenderState::Connected { next_batch_seq, .. } => {
                        let s = *next_batch_seq;
                        *next_batch_seq += 1;
                        s
                    }
                    _ => continue,
                };
                let ops: Vec<_> = buffer
                    .drain(..config.batch_size.min(buffer.len()))
                    .collect();
                let proto_ops = ops.iter().map(proto_op).collect();
                let ok = send_envelope(
                    &tx,
                    EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                        session_id: sid,
                        batch_seq,
                        fragment: fragment.0,
                        ops: proto_ops,
                    }),
                )
                .await;
                if ok {
                    if let SenderState::Connected { in_flight, .. } = &mut state {
                        in_flight.push_back(InflightBatch { batch_seq, ops });
                    }
                } else {
                    // Send failed: the peer is gone. Requeue and wait for
                    // the disconnect notice.
                    for op in ops.into_iter().rev() {
                        buffer.push_front(op);
                    }
                    peer_tx = None;
                    if !matches!(state, SenderState::BufferReset) {
                        state = SenderState::Disconnected;
                    }
                }
            }
            _ = consistency_timer.tick() => {
                // Periodic anti-entropy: if connected, run a reconciliation
                // pass to repair any divergence the streaming path missed.
                if matches!(state, SenderState::Connected { .. }) {
                    if let Some(tx) = peer_tx.clone() {
                        send_envelope(
                            &tx,
                            EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                                fragment: fragment.0,
                            }),
                        )
                        .await;
                    }
                }
            }
        }
    }
}

/// Run one reconciliation round: compare the peer's summaries against
/// local state, fetch keys where the peer wins, and ship keys where we
/// win. Returns `true` if we sent a `ReconcileFetch` and are waiting
/// for `ReconcileData`; `false` if the round is complete.
///
/// LWW makes the whole exchange idempotent: if a shipped op no longer
/// wins by the time the peer applies it, the peer's merge rejects it.
async fn reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
    peer_tx: &mpsc::Sender<Vec<u8>>,
    session_id: u64,
    summaries: Vec<KeySummary>,
) -> bool {
    let Some(actor) = router.route_for_fragment(fragment) else {
        return false;
    };
    let local: HashMap<String, VersionedValue> =
        match actor.call(|reply| KvMessage::GetAll { reply }, None).await {
            Ok(CallResult::Success(map)) => map,
            _ => return false,
        };

    let mut fetch_keys = Vec::new();
    let mut send_ops: Vec<crate::actor::ReplicatedOp> = Vec::new();
    let mut seen_remote = HashSet::new();

    for s in &summaries {
        seen_remote.insert(s.key.clone());
        let remote_v = VersionedValue {
            value: Vec::new(),
            timestamp_ms: s.timestamp_ms,
            node_id: s.node_id.clone(),
            expires_at_ms: None,
            is_tombstone: s.is_tombstone,
        };
        match local.get(&s.key) {
            Some(local_v) => {
                if remote_v.wins_over(local_v) {
                    fetch_keys.push(s.key.clone());
                } else if local_v.wins_over(&remote_v) {
                    send_ops.push(crate::actor::ReplicatedOp {
                        key: s.key.clone(),
                        value: local_v.clone(),
                        seq: 0,
                        boot_id: 0,
                    });
                }
            }
            None => {
                // Peer has it, we don't.
                fetch_keys.push(s.key.clone());
            }
        }
    }
    // Keys we have that the peer lacks.
    for (key, v) in &local {
        if !seen_remote.contains(key) {
            send_ops.push(crate::actor::ReplicatedOp {
                key: key.clone(),
                value: v.clone(),
                seq: 0,
                boot_id: 0,
            });
        }
    }

    // Ship our winning ops as (non-ack-tracked) reconciliation batches.
    // The receiver applies them via LWW under the current session.
    for chunk in send_ops.chunks(100) {
        let proto_ops = chunk.iter().map(proto_op).collect();
        send_envelope(
            peer_tx,
            EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id,
                batch_seq: u64::MAX, // reconciliation batch; not ack-tracked
                fragment: fragment.0,
                ops: proto_ops,
            }),
        )
        .await;
    }

    if fetch_keys.is_empty() {
        return false;
    }
    // Ask for the values; ReconcileData completes the round.
    send_envelope(
        peer_tx,
        EnvelopeMsg::ReconcileFetch(ReconcileFetch {
            fragment: fragment.0,
            keys: fetch_keys,
        }),
    )
    .await;
    true
}

/// Leave `BufferReset` after reconciliation: clear the reset flag and
/// go `Connected` with a fresh session.
async fn finish_reconcile(
    fragment: FragmentId,
    peer_id: &str,
    reset_senders: &Arc<Mutex<HashSet<(FragmentId, String)>>>,
    state: &mut SenderState,
    session_id: u64,
    peer_tx: Option<mpsc::Sender<Vec<u8>>>,
) {
    reset_senders
        .lock()
        .await
        .remove(&(fragment, peer_id.to_string()));
    if peer_tx.is_some() {
        *state = SenderState::Connected {
            session_id,
            next_batch_seq: 0,
            in_flight: VecDeque::new(),
        };
    } else {
        *state = SenderState::Disconnected;
    }
}

fn rand_session_id() -> u64 {
    // Not cryptographic; just needs to differ across pairings.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    // Mix in a counter so rapid reconnects don't collide.
    static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .hash(&mut h);
    let id = h.finish();
    if id == 0 {
        1
    } else {
        id
    }
}
