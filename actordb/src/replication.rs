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
use std::time::{Duration, Instant};

use ractor::rpc::CallResult;
use tokio::sync::{mpsc, Mutex};

use crate::actor::{KvMessage, OpPriority, VersionedValue};
use crate::node::FragmentRouter;
use crate::proto::{
    Envelope, EnvelopeMsg, KeySummary, ReconcileFetch, ReconcileRequest, ReplicateBatch,
    ReplicateOp as ProtoReplicateOp, ReplicationAckLevel, SessionStart, SyncEntry,
};
use crate::shard::FragmentId;

/// Prevent a permanently failing receiver store from becoming a tight
/// NACK/write/log loop. Full retry policy belongs with ACK timeouts and bounded
/// ingress; this floor makes the current explicit retry operationally safe.
const NACK_RETRY_DELAY: Duration = Duration::from_secs(1);

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
    /// Minimum receiver fence that may retire an in-flight batch.
    /// `MemoryApplied` accepts either acknowledgement level;
    /// `StoreCommitted` requires a receiver with a configured store.
    pub required_ack_durability: ReplicationAckDurability,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_interval: Duration::from_millis(100),
            max_in_flight: 4,
            buffer_capacity: 10_000,
            consistency_interval: Duration::from_secs(300),
            required_ack_durability: ReplicationAckDurability::MemoryApplied,
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
    /// The peer acked a batch after reaching the reported durability fence.
    Ack {
        session_id: u64,
        batch_seq: u64,
        durability: ReplicationAckDurability,
    },
    /// The peer could not apply or flush a batch. Requeue the exact stamped
    /// operations; LWW application makes retries idempotent.
    Nack {
        session_id: u64,
        batch_seq: u64,
        error: String,
    },
    /// Response to our reconcile request.
    ReconcileResponse { summaries: Vec<KeySummary> },
    /// Full values for keys we asked to fetch.
    ReconcileData { entries: Vec<SyncEntry> },
    /// Trigger an anti-entropy reconcile round now (if connected).
    /// Used by the Phase 8 placement driver to bootstrap a newly
    /// assigned fragment without waiting for the periodic timer.
    ReconcileNow,
}

/// Receiver guarantee attached to the most recent accepted batch ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReplicationAckDurability {
    /// Every operation is visible in the receiver's in-memory cache. No store
    /// flush is required, even when the receiver has a configured store.
    MemoryApplied,
    /// The receiver applied every op and its configured store accepted a
    /// flush. This is not an fsync guarantee unless that store says so.
    StoreCommitted,
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

fn requeue_in_flight(
    in_flight: &mut VecDeque<InflightBatch>,
    buffer: &mut VecDeque<crate::actor::ReplicatedOp>,
) {
    // Pop newest batches first and push each batch in reverse so the final
    // buffer preserves original batch and operation order.
    while let Some(batch) = in_flight.pop_back() {
        for op in batch.ops.into_iter().rev() {
            buffer.push_front(op);
        }
    }
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
    /// Durability fence reported by the most recent well-formed ACK. This may
    /// be below `required_ack_durability`, in which case the batch remains in
    /// flight and the mismatch is observable here.
    pub last_ack_durability: Option<ReplicationAckDurability>,
    /// The most recent matching ACK was below the configured minimum and did
    /// not retire its batch.
    pub ack_durability_mismatch: bool,
    /// Completed anti-entropy reconcile rounds. The Phase 8 placement
    /// driver polls this to detect bootstrap completion.
    pub reconcile_rounds: u64,
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
    // Remaining fetch chunks for the in-flight reconcile round.
    // Chunking bounds the memory of a single ReconcileData message
    // during bootstrap / anti-entropy on large fragments.
    let mut fetch_queue: VecDeque<Vec<String>> = VecDeque::new();
    // Completed reconcile rounds, published for health/tests (the
    // Phase 8 placement driver polls this for bootstrap completion).
    let mut reconcile_rounds: u64 = 0;
    let mut last_ack_durability = None;
    let mut ack_durability_mismatch = false;
    let mut retry_not_before: Option<Instant> = None;

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
                last_ack_durability,
                ack_durability_mismatch,
                reconcile_rounds,
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
                        retry_not_before = None;
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                        // Requeue unacked ops at the front: the new session
                        // must not lose them, and redelivery is idempotent.
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            requeue_in_flight(in_flight, &mut buffer);
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
                        session_id = 0;
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            // Preserve every unacknowledged operation before
                            // discarding the dead session.
                            requeue_in_flight(in_flight, &mut buffer);
                            state = SenderState::Disconnected;
                        }
                        // BufferReset survives disconnects: we still need
                        // to reconcile when the peer returns.
                    }
                    SenderMsg::Ack {
                        session_id: ack_session,
                        batch_seq,
                        durability,
                    } => {
                        if ack_session != session_id {
                            continue; // stale session ack
                        }
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            if let Some(pos) =
                                in_flight.iter().position(|b| b.batch_seq == batch_seq)
                            {
                                last_ack_durability = Some(durability);
                                ack_durability_mismatch =
                                    durability < config.required_ack_durability;
                                if ack_durability_mismatch {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        batch_seq,
                                        ?durability,
                                        required = ?config.required_ack_durability,
                                        "replication ack did not meet the configured durability fence"
                                    );
                                } else {
                                    in_flight.remove(pos);
                                }
                            }
                        }
                    }
                    SenderMsg::Nack {
                        session_id: nack_session,
                        batch_seq,
                        error,
                    } => {
                        if nack_session != session_id {
                            continue; // stale session nack
                        }
                        if let SenderState::Connected { in_flight, .. } = &mut state {
                            if let Some(pos) =
                                in_flight.iter().position(|b| b.batch_seq == batch_seq)
                            {
                                if let Some(batch) = in_flight.remove(pos) {
                                    for op in batch.ops.into_iter().rev() {
                                        buffer.push_front(op);
                                    }
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        batch_seq,
                                        %error,
                                        "replication batch rejected; queued for retry"
                                    );
                                    retry_not_before = Some(Instant::now() + NACK_RETRY_DELAY);
                                }
                            }
                        }
                    }
                    SenderMsg::ReconcileNow => {
                        // On-demand anti-entropy (Phase 8 bootstrap).
                        // A disconnected sender reconciles on the next
                        // PeerConnected; a BufferReset sender already
                        // has a round in flight.
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
                    SenderMsg::ReconcileResponse { summaries } => {
                        // Anti-entropy applies in BufferReset (recovery)
                        // and in Connected (periodic repair): a response
                        // received while Disconnected belongs to a dead
                        // session and is ignored.
                        let in_reset = matches!(state, SenderState::BufferReset);
                        if !in_reset && !matches!(state, SenderState::Connected { .. }) {
                            continue;
                        }
                        let Some(tx) = peer_tx.clone() else { continue };
                        // A newer round supersedes an older one still
                        // fetching: LWW makes the exchange idempotent.
                        fetch_queue.clear();
                        match reconcile(fragment, &deps.router, &tx, session_id, summaries).await
                        {
                            // No fetch needed: the round is complete.
                            None => {
                                reconcile_rounds += 1;
                                if in_reset {
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
                            // First chunk sent; the rest wait for
                            // ReconcileData.
                            Some(remaining) => {
                                fetch_queue = remaining;
                            }
                        }
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
                                let _ = actor.send_message(KvMessage::ApplyOp {
                                    op,
                                    priority: OpPriority::Default,
                                    is_replication: true,
                                    reply: None,
                                });
                            }
                        }
                        // More chunks? Send the next fetch; otherwise the
                        // round is complete.
                        let mut next_chunk = fetch_queue.pop_front();
                        // A queued chunk with no peer is useless; drop it
                        // and finish the round instead of stalling.
                        if next_chunk.is_some() && peer_tx.is_none() {
                            next_chunk = None;
                            fetch_queue.clear();
                        }
                        if let Some(chunk) = next_chunk {
                            if let Some(tx) = peer_tx.clone() {
                                send_envelope(
                                    &tx,
                                    EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                                        fragment: fragment.0,
                                        keys: chunk,
                                    }),
                                )
                                .await;
                                continue;
                            }
                        }
                        reconcile_rounds += 1;
                        // Reconciliation is complete once the data lands:
                        // leave BufferReset and go Connected. A
                        // Connected sender simply stays connected.
                        if matches!(state, SenderState::BufferReset) {
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
            }
            _ = batch_timer.tick() => {
                let (sid, tx) = match (&state, peer_tx.clone()) {
                    (SenderState::Connected { session_id, in_flight, .. }, Some(tx))
                        if !buffer.is_empty()
                            && in_flight.len() < config.max_in_flight
                            && retry_not_before
                                .map(|deadline| Instant::now() >= deadline)
                                .unwrap_or(true) =>
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
                        required_ack_level: match config.required_ack_durability {
                            ReplicationAckDurability::MemoryApplied => {
                                ReplicationAckLevel::Applied as i32
                            }
                            ReplicationAckDurability::StoreCommitted => {
                                ReplicationAckLevel::StoreCommitted as i32
                            }
                        },
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
                    session_id = 0;
                    last_ack_durability = None;
                    ack_durability_mismatch = false;
                    if let SenderState::Connected { in_flight, .. } = &mut state {
                        requeue_in_flight(in_flight, &mut buffer);
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

/// Keys per ReconcileFetch chunk. Bounds the memory of a single
/// ReconcileData message during bootstrap / anti-entropy on large
/// fragments (Phase 8 chunked bootstrap).
const RECONCILE_FETCH_CHUNK: usize = 500;

/// Run one reconciliation round: compare the peer's summaries against
/// local state, fetch keys where the peer wins, and ship keys where we
/// win.
///
/// Returns `None` when no fetch was needed (the round is complete).
/// Otherwise sends the first fetch chunk and returns the remaining
/// chunks; the caller sends them as each `ReconcileData` arrives.
///
/// LWW makes the whole exchange idempotent: if a shipped op no longer
/// wins by the time the peer applies it, the peer's merge rejects it.
async fn reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
    peer_tx: &mpsc::Sender<Vec<u8>>,
    session_id: u64,
    summaries: Vec<KeySummary>,
) -> Option<VecDeque<Vec<String>>> {
    let actor = router.route_for_fragment(fragment)?;
    let local: HashMap<String, VersionedValue> =
        match actor.call(|reply| KvMessage::GetAll { reply }, None).await {
            Ok(CallResult::Success(map)) => map,
            _ => return None,
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
                required_ack_level: ReplicationAckLevel::Applied as i32,
            }),
        )
        .await;
    }

    if fetch_keys.is_empty() {
        return None;
    }
    // Chunk the fetch so a large fragment doesn't materialize as one
    // giant ReconcileData message: send the first chunk now, queue the
    // rest for the caller to send as each chunk's data arrives.
    let mut chunks: VecDeque<Vec<String>> = fetch_keys
        .chunks(RECONCILE_FETCH_CHUNK)
        .map(|c| c.to_vec())
        .collect();
    let first = chunks.pop_front().unwrap_or_default();
    send_envelope(
        peer_tx,
        EnvelopeMsg::ReconcileFetch(ReconcileFetch {
            fragment: fragment.0,
            keys: first,
        }),
    )
    .await;
    Some(chunks)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn op(key: &str) -> crate::actor::ReplicatedOp {
        crate::actor::ReplicatedOp {
            key: key.to_string(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 0,
            boot_id: 0,
        }
    }

    #[test]
    fn requeue_in_flight_preserves_batches_ahead_of_newer_buffered_ops() {
        let mut in_flight = VecDeque::from([
            InflightBatch {
                batch_seq: 0,
                ops: vec![op("a"), op("b")],
            },
            InflightBatch {
                batch_seq: 1,
                ops: vec![op("c")],
            },
        ]);
        let mut buffer = VecDeque::from([op("d")]);
        requeue_in_flight(&mut in_flight, &mut buffer);
        assert!(in_flight.is_empty());
        assert_eq!(
            buffer.into_iter().map(|op| op.key).collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
    }
}
