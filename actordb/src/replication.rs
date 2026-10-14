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
//!        └─────────────────────▶ BufferReset ──reconcile + ACK fence──▶ Connected
//! ```
//!
//! - **Connected**: mutations (writes + tombstoned deletes) accumulate
//!   in a bounded local buffer. The sender ships batches on a timer,
//!   keeps a bounded number of batches in flight, and drops acked
//!   records. Each sender↔receiver pairing gets a fresh `session_id`;
//!   the receiver discards batches from stale sessions.
//! - **Disconnected**: the peer is down; the buffer is retained for
//!   resume on reconnect.
//! - **BufferReset**: the bounded buffer overflowed. The buffer is shed and,
//!   if the transport is live, reconciliation starts immediately. Every
//!   round is correlated to its session and round id, fetched values are
//!   awaited locally, and a tracked empty batch fences outbound repairs
//!   before the reset completes. While any sender is in this state,
//!   tombstone GC is blocked (see below).
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
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::actor::{ApplyDurability, HlcClock, KvMessage, OpPriority, VersionedValue};
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
    /// Maximum time to wait for one network reconciliation phase
    /// (summary response, fetched-data chunk, or durability barrier ACK).
    /// A timeout rotates the session and retries the round, so a connected
    /// but silent peer cannot strand bootstrap or overflow recovery forever.
    pub reconcile_timeout: Duration,
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
            reconcile_timeout: Duration::from_secs(30),
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
    /// One peer connection dropped. Ignore it if a newer channel has already
    /// replaced it; otherwise retain the buffer for reconnect.
    PeerDisconnected { peer_tx: mpsc::Sender<Vec<u8>> },
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
    ReconcileResponse {
        session_id: u64,
        round_id: u64,
        summaries: Vec<KeySummary>,
        /// Tells the connection task whether the correlated frame passed
        /// timestamp validation. Stale frames return `true` without touching
        /// the HLC; an invalid current frame returns `false` so the connection
        /// is closed.
        connection_valid: oneshot::Sender<bool>,
    },
    /// Full values for keys we asked to fetch.
    ReconcileData {
        session_id: u64,
        round_id: u64,
        chunk_seq: u64,
        entries: Vec<SyncEntry>,
        connection_valid: oneshot::Sender<bool>,
    },
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

struct ActiveConnection {
    peer_tx: mpsc::Sender<Vec<u8>>,
    session_id: u64,
    next_batch_seq: u64,
    in_flight: VecDeque<InflightBatch>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryState {
    Streaming,
    Reset { epoch: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconcilePurpose {
    Routine,
    Reset { epoch: u64 },
}

enum ReconcilePhase {
    Response,
    Data {
        remaining_chunks: VecDeque<Vec<String>>,
        next_chunk_seq: u64,
        expected_keys: HashSet<String>,
    },
    Barrier {
        batch_seq: u64,
    },
}

struct ReconcileRound {
    session_id: u64,
    round_id: u64,
    purpose: ReconcilePurpose,
    phase: ReconcilePhase,
    phase_deadline: Instant,
    snapshot_revision: Option<u64>,
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

fn disconnect_transport(
    connection: &mut Option<ActiveConnection>,
    delivery: DeliveryState,
    buffer: &mut VecDeque<crate::actor::ReplicatedOp>,
) {
    if let Some(mut connection) = connection.take() {
        if matches!(delivery, DeliveryState::Streaming) {
            requeue_in_flight(&mut connection.in_flight, buffer);
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
    /// Node-wide HLC. Reconcile frames are observed here only after the sender
    /// has authenticated their exact session/round/phase correlation.
    pub hlc: Arc<Mutex<HlcClock>>,
    /// Senders currently in `BufferReset`, keyed by (fragment, peer).
    /// The cleaner consults this to block tombstone GC.
    pub reset_senders: Arc<Mutex<HashMap<(FragmentId, String), u64>>>,
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
    let mut delivery = DeliveryState::Streaming;
    let mut connection: Option<ActiveConnection> = None;
    let mut buffer: VecDeque<crate::actor::ReplicatedOp> = VecDeque::new();
    let mut batch_timer = tokio::time::interval(config.batch_interval);
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consistency_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + config.consistency_interval,
        config.consistency_interval,
    );
    consistency_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut active_reconcile: Option<ReconcileRound> = None;
    let reset_generation = rand_session_id();
    let mut next_round_id = 0_u64;
    let mut next_reset_epoch = 0_u64;
    let mut reconcile_rounds: u64 = 0;
    let mut last_ack_durability = None;
    let mut ack_durability_mismatch = false;
    let mut retry_not_before: Option<Instant> = None;
    let mut reconcile_retry: Option<(Instant, ReconcilePurpose)> = None;
    // A fetched winner may have entered a configured local store's write
    // buffer before a flush failed. Carry that obligation across retry
    // rounds; ordinary no-fetch reconciliation on a storeless sender must not
    // acquire a new local durability requirement.
    let mut local_reconcile_flush_pending = false;

    loop {
        // Publish state for health checks and tests.
        {
            let state = match delivery {
                DeliveryState::Reset { .. } => "buffer_reset",
                DeliveryState::Streaming if connection.is_some() => "connected",
                DeliveryState::Streaming => "disconnected",
            };
            *deps.state_report.lock().await = SenderStateReport {
                state: state.to_string(),
                session_id: connection.as_ref().map_or(0, |c| c.session_id),
                buffered: buffer.len(),
                in_flight: connection.as_ref().map_or(0, |c| c.in_flight.len()),
                last_ack_durability,
                ack_durability_mismatch,
                reconcile_rounds,
            };
        }
        let reconcile_wakeup = active_reconcile
            .as_ref()
            .map(|round| round.phase_deadline)
            .into_iter()
            .chain(
                reconcile_retry
                    .filter(|_| connection.is_some())
                    .map(|(deadline, _)| deadline),
            )
            .min();
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    SenderMsg::Enqueue(op) => {
                        if let DeliveryState::Reset { epoch } = &mut delivery {
                            // Preserve post-snapshot local deltas so a busy
                            // fragment can finish reset without requiring a
                            // completely quiet network round. A second
                            // overflow invalidates the snapshot and falls back
                            // to another full round.
                            if buffer.len() >= config.buffer_capacity {
                                buffer.clear();
                                *epoch = next_nonzero(&mut next_reset_epoch);
                            } else {
                                buffer.push_back(op);
                            }
                            continue;
                        }
                        if buffer.len() >= config.buffer_capacity {
                            // Install the tombstone-GC guard before shedding
                            // the only queued copies. The overflowing op is
                            // already in local state and is recovered by the
                            // correlated reconciliation snapshot.
                            deps.reset_senders
                                .lock()
                                .await
                                .insert((fragment, peer_id.clone()), reset_generation);
                            buffer.clear();
                            let epoch = next_nonzero(&mut next_reset_epoch);
                            delivery = DeliveryState::Reset { epoch };
                            active_reconcile = None;
                            reconcile_retry = None;

                            // Invalidate old streaming batches before taking
                            // the reset snapshot. Delayed ACKs and reconcile
                            // responses from that session are then harmless.
                            let session_started = if let Some(conn) = connection.as_mut() {
                                conn.in_flight.clear();
                                conn.session_id = rand_session_id();
                                conn.next_batch_seq = 0;
                                send_session_start(fragment, conn).await
                            } else {
                                false
                            };
                            if session_started {
                                let conn = connection.as_ref().expect("checked above");
                                match begin_reconcile(
                                    fragment,
                                    ReconcilePurpose::Reset { epoch },
                                    conn,
                                    &mut next_round_id,
                                    config.reconcile_timeout,
                                )
                                .await
                                {
                                    Ok(round) => active_reconcile = Some(round),
                                    Err(()) => disconnect_transport(&mut connection, delivery, &mut buffer),
                                }
                            } else if connection.is_some() {
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                            continue;
                        }
                        buffer.push_back(op);
                    }
                    SenderMsg::PeerConnected { peer_tx: tx } => {
                        if connection
                            .as_ref()
                            .map_or(false, |current| current.peer_tx.same_channel(&tx))
                        {
                            // Dynamic placement polls and duplicate
                            // notifications must not rotate live sessions.
                            continue;
                        }
                        if let Some(round) = active_reconcile.take() {
                            reconcile_retry = Some((Instant::now(), round.purpose));
                        }
                        if let Some(mut old) = connection.take() {
                            if matches!(delivery, DeliveryState::Streaming) {
                                requeue_in_flight(&mut old.in_flight, &mut buffer);
                            }
                        }
                        retry_not_before = None;
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                        let mut conn = ActiveConnection {
                            peer_tx: tx,
                            session_id: rand_session_id(),
                            next_batch_seq: 0,
                            in_flight: VecDeque::new(),
                        };
                        if !send_session_start(fragment, &conn).await {
                            continue;
                        }
                        let reconcile_purpose = match delivery {
                            DeliveryState::Reset { epoch } => {
                                Some(ReconcilePurpose::Reset { epoch })
                            }
                            DeliveryState::Streaming => {
                                reconcile_retry.map(|(_, purpose)| purpose)
                            }
                        };
                        if let Some(purpose) = reconcile_purpose {
                            match begin_reconcile(
                                fragment,
                                purpose,
                                &conn,
                                &mut next_round_id,
                                config.reconcile_timeout,
                            )
                            .await
                            {
                                Ok(round) => {
                                    active_reconcile = Some(round);
                                    reconcile_retry = None;
                                }
                                Err(()) => {
                                    conn.in_flight.clear();
                                    continue;
                                }
                            }
                        }
                        connection = Some(conn);
                    }
                    SenderMsg::PeerDisconnected { peer_tx } => {
                        if connection
                            .as_ref()
                            .map_or(true, |current| !current.peer_tx.same_channel(&peer_tx))
                        {
                            continue;
                        }
                        if let Some(round) = active_reconcile.take() {
                            reconcile_retry = Some((Instant::now(), round.purpose));
                        }
                        if let Some(mut conn) = connection.take() {
                            if matches!(delivery, DeliveryState::Streaming) {
                                requeue_in_flight(&mut conn.in_flight, &mut buffer);
                            }
                        }
                        last_ack_durability = None;
                        ack_durability_mismatch = false;
                    }
                    SenderMsg::Ack {
                        session_id: ack_session,
                        batch_seq,
                        durability,
                    } => {
                        let mut completed_round = None;
                        if let Some(conn) = connection.as_mut() {
                            if ack_session != conn.session_id {
                                continue;
                            }
                            if let Some(pos) = conn
                                .in_flight
                                .iter()
                                .position(|b| b.batch_seq == batch_seq)
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
                                    ack_durability_mismatch = false;
                                    conn.in_flight.remove(pos);
                                    let is_barrier = active_reconcile.as_ref().map_or(false, |round| {
                                        round.session_id == ack_session
                                            && matches!(
                                                round.phase,
                                                ReconcilePhase::Barrier { batch_seq: seq }
                                                    if seq == batch_seq
                                            )
                                    });
                                    if is_barrier {
                                        completed_round = active_reconcile.take();
                                    }
                                }
                            }
                        }
                        if let Some(mut round) = completed_round {
                            reconcile_retry = None;
                            match round.purpose {
                                ReconcilePurpose::Routine => {
                                    if round.snapshot_revision.is_some() {
                                        reconcile_rounds = reconcile_rounds.saturating_add(1);
                                    } else if let Some(conn) = connection.as_ref() {
                                        // Fetched winners (and reset deltas)
                                        // invalidate the snapshot that chose
                                        // them. The barrier proves those
                                        // writes were applied, but not that
                                        // the two sides now agree. Require a
                                        // fresh no-fetch round before
                                        // reporting routine/bootstrap
                                        // completion.
                                        match begin_reconcile(
                                            fragment,
                                            ReconcilePurpose::Routine,
                                            conn,
                                            &mut next_round_id,
                                            config.reconcile_timeout,
                                        )
                                        .await
                                        {
                                            Ok(next_round) => {
                                                active_reconcile = Some(next_round)
                                            }
                                            Err(()) => {
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    ReconcilePurpose::Routine,
                                                ));
                                                disconnect_transport(
                                                    &mut connection,
                                                    delivery,
                                                    &mut buffer,
                                                );
                                            }
                                        }
                                    } else {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            ReconcilePurpose::Routine,
                                        ));
                                    }
                                }
                                ReconcilePurpose::Reset {
                                    epoch: completed_epoch,
                                } => {
                                    if !buffer.is_empty() {
                                        let deltas: Vec<_> = buffer.drain(..).collect();
                                        let send_result = match connection.as_ref() {
                                            Some(conn) => {
                                                send_repair_ops(fragment, conn, &deltas).await
                                            }
                                            None => Err(ReconcileError::Transport),
                                        };
                                        if send_result.is_err() {
                                            for op in deltas.into_iter().rev() {
                                                buffer.push_front(op);
                                            }
                                            disconnect_transport(
                                                &mut connection,
                                                delivery,
                                                &mut buffer,
                                            );
                                            reconcile_retry = Some((
                                                Instant::now() + NACK_RETRY_DELAY,
                                                round.purpose,
                                            ));
                                            continue;
                                        }
                                        // These deltas are now fenced, but a
                                        // bare revision cannot distinguish
                                        // them from unrelated concurrent
                                        // mutations. Require one fresh
                                        // no-change snapshot before clearing
                                        // reset.
                                        round.snapshot_revision = None;
                                        let Some(conn) = connection.as_mut() else {
                                            for op in deltas.into_iter().rev() {
                                                buffer.push_front(op);
                                            }
                                            continue;
                                        };
                                        match send_reconcile_barrier(fragment, &config, conn).await {
                                            Ok(batch_seq) => {
                                                round.phase =
                                                    ReconcilePhase::Barrier { batch_seq };
                                                round.phase_deadline =
                                                    Instant::now() + config.reconcile_timeout;
                                                active_reconcile = Some(round);
                                            }
                                            Err(()) => {
                                                for op in deltas.into_iter().rev() {
                                                    buffer.push_front(op);
                                                }
                                                disconnect_transport(
                                                    &mut connection,
                                                    delivery,
                                                    &mut buffer,
                                                );
                                                reconcile_retry = Some((
                                                    Instant::now() + NACK_RETRY_DELAY,
                                                    round.purpose,
                                                ));
                                            }
                                        }
                                        continue;
                                    }

                                    let snapshot_is_current = match (
                                        round.snapshot_revision,
                                        fragment_revision(fragment, &deps.router).await,
                                    ) {
                                        (Some(expected), Ok(current)) => expected == current,
                                        _ => false,
                                    };
                                    match delivery {
                                        DeliveryState::Reset { epoch }
                                            if epoch == completed_epoch && snapshot_is_current =>
                                        {
                                        let key = (fragment, peer_id.clone());
                                        let mut reset_senders = deps.reset_senders.lock().await;
                                        if reset_senders.get(&key) == Some(&reset_generation) {
                                            reset_senders.remove(&key);
                                        }
                                        delivery = DeliveryState::Streaming;
                                        reconcile_rounds = reconcile_rounds.saturating_add(1);
                                        }
                                        DeliveryState::Reset { epoch } => {
                                            if let Some(conn) = connection.as_ref() {
                                                match begin_reconcile(
                                                    fragment,
                                                    ReconcilePurpose::Reset { epoch },
                                                    conn,
                                                    &mut next_round_id,
                                                    config.reconcile_timeout,
                                                )
                                                .await
                                                {
                                                    Ok(round) => active_reconcile = Some(round),
                                                    Err(()) => {
                                                        reconcile_retry = Some((
                                                            Instant::now() + NACK_RETRY_DELAY,
                                                            ReconcilePurpose::Reset { epoch },
                                                        ));
                                                        disconnect_transport(
                                                            &mut connection,
                                                            delivery,
                                                            &mut buffer,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        DeliveryState::Streaming => {}
                                    }
                                }
                            }
                        }
                    }
                    SenderMsg::Nack {
                        session_id: nack_session,
                        batch_seq,
                        error,
                    } => {
                        let mut restart_session = false;
                        if let Some(conn) = connection.as_mut() {
                            if nack_session != conn.session_id {
                                continue;
                            }
                            if let Some(pos) = conn
                                .in_flight
                                .iter()
                                .position(|b| b.batch_seq == batch_seq)
                            {
                                if let Some(batch) = conn.in_flight.remove(pos) {
                                    let is_barrier = active_reconcile.as_ref().map_or(false, |round| {
                                        round.session_id == nack_session
                                            && matches!(
                                                round.phase,
                                                ReconcilePhase::Barrier { batch_seq: seq }
                                                    if seq == batch_seq
                                            )
                                    });
                                    if is_barrier {
                                        let purpose = active_reconcile
                                            .take()
                                            .map(|round| round.purpose)
                                            .unwrap_or(ReconcilePurpose::Routine);
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            purpose,
                                        ));
                                        // A reconciliation repair failure
                                        // poisons the receiver's current
                                        // session. Preserve unrelated
                                        // streaming batches, then mint a new
                                        // session before retrying the round.
                                        if matches!(delivery, DeliveryState::Streaming) {
                                            requeue_in_flight(
                                                &mut conn.in_flight,
                                                &mut buffer,
                                            );
                                        } else {
                                            conn.in_flight.clear();
                                        }
                                        conn.session_id = rand_session_id();
                                        conn.next_batch_seq = 0;
                                        restart_session = true;
                                    }
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
                                    if !is_barrier {
                                        retry_not_before = Some(Instant::now() + NACK_RETRY_DELAY);
                                    }
                                }
                            }
                        }
                        if restart_session {
                            let restarted = match connection.as_ref() {
                                Some(conn) => send_session_start(fragment, conn).await,
                                None => false,
                            };
                            if !restarted {
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                    SenderMsg::ReconcileNow => {
                        if matches!(delivery, DeliveryState::Streaming)
                            && active_reconcile.is_none()
                        {
                            if let Some(conn) = connection.as_ref() {
                                match begin_reconcile(
                                    fragment,
                                    ReconcilePurpose::Routine,
                                    conn,
                                    &mut next_round_id,
                                    config.reconcile_timeout,
                                )
                                .await
                                {
                                    Ok(round) => {
                                        active_reconcile = Some(round);
                                        reconcile_retry = None;
                                    }
                                    Err(()) => {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            ReconcilePurpose::Routine,
                                        ));
                                        disconnect_transport(&mut connection, delivery, &mut buffer);
                                    }
                                }
                            } else {
                                // Preserve on-demand bootstrap across a
                                // connection race instead of silently dropping
                                // the request.
                                reconcile_retry =
                                    Some((Instant::now(), ReconcilePurpose::Routine));
                            }
                        }
                    }
                    SenderMsg::ReconcileResponse {
                        session_id,
                        round_id,
                        summaries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().map_or(false, |round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(round.phase, ReconcilePhase::Response)
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        if reconcile_summaries_are_well_formed(
                            fragment,
                            &deps.router,
                            &summaries,
                        ) {
                            if let Some(timestamp_ms) =
                                summaries.iter().map(|summary| summary.timestamp_ms).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe(timestamp_ms) {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        %error,
                                        "rejecting correlated reconcile response timestamp"
                                    );
                                    let _ = connection_valid.send(false);
                                    continue;
                                }
                            }
                        }
                        let _ = connection_valid.send(true);
                        let Some(mut round) = active_reconcile.take() else { continue };
                        let Some(conn) = connection.as_mut() else { continue };
                        match reconcile(
                            fragment,
                            &deps.router,
                            conn,
                            round.round_id,
                            summaries,
                        )
                        .await
                        {
                            Ok((
                                Some((
                                    remaining_chunks,
                                    expected_chunk_seq,
                                    expected_keys,
                                )),
                                snapshot_revision,
                            )) => {
                                round.snapshot_revision = Some(snapshot_revision);
                                round.phase = ReconcilePhase::Data {
                                    remaining_chunks,
                                    next_chunk_seq: expected_chunk_seq,
                                    expected_keys,
                                };
                                round.phase_deadline =
                                    Instant::now() + config.reconcile_timeout;
                                active_reconcile = Some(round);
                            }
                            Ok((None, snapshot_revision)) => {
                                round.snapshot_revision = Some(snapshot_revision);
                                // A prior attempt may already have applied
                                // fetched winners to the cache and then failed
                                // its local store flush. The retry will see
                                // equal summaries and need no fetch, so fence
                                // local durability on every StoreCommitted
                                // completion path, not only after fresh data.
                                if local_reconcile_flush_pending {
                                    if flush_reconcile(fragment, &deps.router).await.is_err() {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            round.purpose,
                                        ));
                                        continue;
                                    }
                                    local_reconcile_flush_pending = false;
                                }
                                match send_reconcile_barrier(fragment, &config, conn).await {
                                    Ok(batch_seq) => {
                                        round.phase = ReconcilePhase::Barrier { batch_seq };
                                        round.phase_deadline =
                                            Instant::now() + config.reconcile_timeout;
                                        active_reconcile = Some(round);
                                    }
                                    Err(()) => {
                                        reconcile_retry = Some((
                                            Instant::now() + NACK_RETRY_DELAY,
                                            round.purpose,
                                        ));
                                        disconnect_transport(&mut connection, delivery, &mut buffer);
                                    }
                                }
                            }
                            Err(ReconcileError::Local) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                            }
                            Err(ReconcileError::Transport) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                    SenderMsg::ReconcileData {
                        session_id,
                        round_id,
                        chunk_seq,
                        entries,
                        connection_valid,
                    } => {
                        let expected = active_reconcile.as_ref().map_or(false, |round| {
                            round.session_id == session_id
                                && round.round_id == round_id
                                && matches!(
                                    round.phase,
                                    ReconcilePhase::Data { next_chunk_seq, .. }
                                        if next_chunk_seq == chunk_seq
                                )
                        });
                        if !expected {
                            let _ = connection_valid.send(true);
                            continue;
                        }
                        let expected_keys = match active_reconcile.as_ref().map(|round| &round.phase)
                        {
                            Some(ReconcilePhase::Data { expected_keys, .. }) => {
                                expected_keys
                            }
                            _ => {
                                let _ = connection_valid.send(true);
                                continue;
                            }
                        };
                        if reconcile_entries_are_exact(
                            fragment,
                            &deps.router,
                            &entries,
                            expected_keys,
                        ) {
                            if let Some(timestamp_ms) =
                                entries.iter().map(|entry| entry.timestamp_ms).max()
                            {
                                if let Err(error) = deps.hlc.lock().await.observe(timestamp_ms) {
                                    tracing::warn!(
                                        peer = %peer_id,
                                        fragment = fragment.0,
                                        %error,
                                        "rejecting correlated reconcile data timestamp"
                                    );
                                    let _ = connection_valid.send(false);
                                    continue;
                                }
                            }
                        }
                        let _ = connection_valid.send(true);
                        let Some(mut round) = active_reconcile.take() else { continue };
                        let ReconcilePhase::Data {
                            mut remaining_chunks,
                            next_chunk_seq,
                            expected_keys,
                        } = round.phase
                        else {
                            continue;
                        };
                        if config.required_ack_durability
                            == ReplicationAckDurability::StoreCommitted
                        {
                            // Apply is not transactional: an error or lost RPC
                            // reply may follow a successful prefix. Record the
                            // flush obligation before the first mutation.
                            local_reconcile_flush_pending = true;
                        }
                        if apply_reconcile_entries(
                            fragment,
                            &deps.router,
                            entries,
                            &expected_keys,
                        )
                            .await
                            .is_err()
                        {
                            reconcile_retry = Some((
                                Instant::now() + NACK_RETRY_DELAY,
                                round.purpose,
                            ));
                            continue;
                        }
                        let Some(conn) = connection.as_mut() else { continue };
                        if let Some(keys) = remaining_chunks.pop_front() {
                            let next_seq = next_chunk_seq.saturating_add(1);
                            let expected_keys = keys.iter().cloned().collect();
                            let ok = send_envelope(
                                &conn.peer_tx,
                                EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                                    fragment: fragment.0,
                                    keys,
                                    session_id,
                                    round_id,
                                    chunk_seq: next_seq,
                                }),
                            )
                            .await;
                            if !ok {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                                continue;
                            }
                            round.phase = ReconcilePhase::Data {
                                remaining_chunks,
                                next_chunk_seq: next_seq,
                                expected_keys,
                            };
                            round.phase_deadline =
                                Instant::now() + config.reconcile_timeout;
                            active_reconcile = Some(round);
                            continue;
                        }
                        if local_reconcile_flush_pending {
                            if flush_reconcile(fragment, &deps.router).await.is_err() {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                continue;
                            }
                            local_reconcile_flush_pending = false;
                        }
                        // A fetched apply changes the fragment revision, but
                        // unrelated mutations may have raced with it. Force a
                        // no-fetch verification round after this barrier
                        // instead of rebasing to an ambiguous revision.
                        round.snapshot_revision = None;
                        match send_reconcile_barrier(fragment, &config, conn).await {
                            Ok(batch_seq) => {
                                round.phase = ReconcilePhase::Barrier { batch_seq };
                                round.phase_deadline =
                                    Instant::now() + config.reconcile_timeout;
                                active_reconcile = Some(round);
                            }
                            Err(()) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    round.purpose,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
                    }
                }
            }
            _ = batch_timer.tick() => {
                if matches!(delivery, DeliveryState::Reset { .. }) {
                    continue;
                }
                let (sid, tx) = match connection.as_ref() {
                    Some(conn)
                        if !buffer.is_empty()
                            && conn.in_flight.len() < config.max_in_flight
                            && retry_not_before
                                .map(|deadline| Instant::now() >= deadline)
                                .unwrap_or(true) =>
                    {
                        (conn.session_id, conn.peer_tx.clone())
                    }
                    _ => continue,
                };
                let batch_seq = match connection.as_mut() {
                    Some(conn) => {
                        let seq = conn.next_batch_seq;
                        conn.next_batch_seq = conn.next_batch_seq.wrapping_add(1);
                        seq
                    }
                    None => continue,
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
                    if let Some(conn) = connection.as_mut() {
                        conn.in_flight.push_back(InflightBatch { batch_seq, ops });
                    }
                } else {
                    for op in ops.into_iter().rev() {
                        buffer.push_front(op);
                    }
                    last_ack_durability = None;
                    ack_durability_mismatch = false;
                    if let Some(mut conn) = connection.take() {
                        requeue_in_flight(&mut conn.in_flight, &mut buffer);
                    }
                    if let Some(round) = active_reconcile.take() {
                        reconcile_retry = Some((Instant::now(), round.purpose));
                    }
                }
            }
            _ = async {
                match reconcile_wakeup {
                    Some(deadline) => {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                let now = Instant::now();
                if active_reconcile
                    .as_ref()
                    .map_or(false, |round| now >= round.phase_deadline)
                {
                    let round = active_reconcile.take().expect("checked above");
                    tracing::warn!(
                        peer = %peer_id,
                        fragment = fragment.0,
                        session_id = round.session_id,
                        round_id = round.round_id,
                        "reconciliation phase timed out; rotating session"
                    );
                    reconcile_retry = Some((now + NACK_RETRY_DELAY, round.purpose));
                    let restarted = if let Some(conn) = connection.as_mut() {
                        if matches!(delivery, DeliveryState::Streaming) {
                            requeue_in_flight(&mut conn.in_flight, &mut buffer);
                        } else {
                            conn.in_flight.clear();
                        }
                        conn.session_id = rand_session_id();
                        conn.next_batch_seq = 0;
                        send_session_start(fragment, conn).await
                    } else {
                        true
                    };
                    if !restarted {
                        disconnect_transport(&mut connection, delivery, &mut buffer);
                    }
                    continue;
                }

                let Some((deadline, requested_purpose)) = reconcile_retry else {
                    continue;
                };
                if now < deadline {
                    continue;
                }
                let purpose = match delivery {
                    DeliveryState::Reset { epoch } => ReconcilePurpose::Reset { epoch },
                    DeliveryState::Streaming => requested_purpose,
                };
                if let Some(conn) = connection.as_ref() {
                    match begin_reconcile(
                        fragment,
                        purpose,
                        conn,
                        &mut next_round_id,
                        config.reconcile_timeout,
                    )
                    .await
                    {
                        Ok(round) => {
                            active_reconcile = Some(round);
                            reconcile_retry = None;
                        }
                        Err(()) => {
                            reconcile_retry = Some((now + NACK_RETRY_DELAY, purpose));
                            disconnect_transport(&mut connection, delivery, &mut buffer);
                        }
                    }
                }
            }
            _ = consistency_timer.tick() => {
                if matches!(delivery, DeliveryState::Streaming)
                    && active_reconcile.is_none()
                {
                    if let Some(conn) = connection.as_ref() {
                        match begin_reconcile(
                            fragment,
                            ReconcilePurpose::Routine,
                            conn,
                            &mut next_round_id,
                            config.reconcile_timeout,
                        )
                        .await
                        {
                            Ok(round) => {
                                active_reconcile = Some(round);
                                reconcile_retry = None;
                            }
                            Err(()) => {
                                reconcile_retry = Some((
                                    Instant::now() + NACK_RETRY_DELAY,
                                    ReconcilePurpose::Routine,
                                ));
                                disconnect_transport(&mut connection, delivery, &mut buffer);
                            }
                        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileError {
    Local,
    Transport,
}

fn next_nonzero(counter: &mut u64) -> u64 {
    *counter = counter.wrapping_add(1);
    if *counter == 0 {
        *counter = 1;
    }
    *counter
}

fn allocate_batch_seq(connection: &mut ActiveConnection) -> u64 {
    let seq = connection.next_batch_seq;
    connection.next_batch_seq = if seq == u64::MAX - 1 { 0 } else { seq + 1 };
    seq
}

async fn send_session_start(fragment: FragmentId, connection: &ActiveConnection) -> bool {
    send_envelope(
        &connection.peer_tx,
        EnvelopeMsg::SessionStart(SessionStart {
            session_id: connection.session_id,
            fragment: fragment.0,
        }),
    )
    .await
}

async fn begin_reconcile(
    fragment: FragmentId,
    purpose: ReconcilePurpose,
    connection: &ActiveConnection,
    next_round_id: &mut u64,
    phase_timeout: Duration,
) -> Result<ReconcileRound, ()> {
    let round_id = next_nonzero(next_round_id);
    let ok = send_envelope(
        &connection.peer_tx,
        EnvelopeMsg::ReconcileRequest(ReconcileRequest {
            fragment: fragment.0,
            session_id: connection.session_id,
            round_id,
        }),
    )
    .await;
    if !ok {
        return Err(());
    }
    Ok(ReconcileRound {
        session_id: connection.session_id,
        round_id,
        purpose,
        phase: ReconcilePhase::Response,
        phase_deadline: Instant::now() + phase_timeout,
        snapshot_revision: None,
    })
}

async fn send_reconcile_barrier(
    fragment: FragmentId,
    config: &SenderConfig,
    connection: &mut ActiveConnection,
) -> Result<u64, ()> {
    let batch_seq = allocate_batch_seq(connection);
    let ok = send_envelope(
        &connection.peer_tx,
        EnvelopeMsg::ReplicateBatch(ReplicateBatch {
            session_id: connection.session_id,
            batch_seq,
            fragment: fragment.0,
            ops: Vec::new(),
            required_ack_level: match config.required_ack_durability {
                ReplicationAckDurability::MemoryApplied => ReplicationAckLevel::Applied as i32,
                ReplicationAckDurability::StoreCommitted => {
                    ReplicationAckLevel::StoreCommitted as i32
                }
            },
        }),
    )
    .await;
    if !ok {
        return Err(());
    }
    connection.in_flight.push_back(InflightBatch {
        batch_seq,
        ops: Vec::new(),
    });
    Ok(batch_seq)
}

async fn send_repair_ops(
    fragment: FragmentId,
    connection: &ActiveConnection,
    ops: &[crate::actor::ReplicatedOp],
) -> Result<(), ReconcileError> {
    for chunk in ops.chunks(100) {
        let proto_ops = chunk.iter().map(proto_op).collect();
        if !send_envelope(
            &connection.peer_tx,
            EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: connection.session_id,
                batch_seq: u64::MAX,
                fragment: fragment.0,
                ops: proto_ops,
                // Untracked repairs are apply-only. The following tracked
                // empty barrier requests and reports the actual fence.
                required_ack_level: ReplicationAckLevel::Applied as i32,
            }),
        )
        .await
        {
            return Err(ReconcileError::Transport);
        }
    }
    Ok(())
}

fn reconcile_summaries_are_well_formed(
    fragment: FragmentId,
    router: &FragmentRouter,
    summaries: &[KeySummary],
) -> bool {
    let mut seen = HashSet::with_capacity(summaries.len());
    summaries.iter().all(|summary| {
        router.key_belongs_to(fragment, &summary.key) && seen.insert(summary.key.as_str())
    })
}

fn reconcile_entries_are_exact(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: &[SyncEntry],
    expected_keys: &HashSet<String>,
) -> bool {
    let received_keys: HashSet<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
    entries.len() == expected_keys.len()
        && received_keys.len() == entries.len()
        && entries.iter().all(|entry| {
            expected_keys.contains(&entry.key) && router.key_belongs_to(fragment, &entry.key)
        })
}

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
    connection: &ActiveConnection,
    round_id: u64,
    summaries: Vec<KeySummary>,
) -> Result<(Option<(VecDeque<Vec<String>>, u64, HashSet<String>)>, u64), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    let (local, snapshot_revision): (HashMap<String, VersionedValue>, u64) = match actor
        .call(|reply| KvMessage::GetSnapshot { reply }, None)
        .await
    {
        Ok(CallResult::Success(snapshot)) => snapshot,
        _ => return Err(ReconcileError::Local),
    };

    let mut fetch_keys = Vec::new();
    let mut send_ops: Vec<crate::actor::ReplicatedOp> = Vec::new();
    let mut seen_remote = HashSet::new();

    for s in &summaries {
        if !router.key_belongs_to(fragment, &s.key) || !seen_remote.insert(s.key.clone()) {
            tracing::warn!(
                fragment = fragment.0,
                key = %s.key,
                "rejecting reconcile summary with wrong-fragment or duplicate key"
            );
            return Err(ReconcileError::Local);
        }
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

    send_repair_ops(fragment, connection, &send_ops).await?;

    if fetch_keys.is_empty() {
        return Ok((None, snapshot_revision));
    }
    // Chunk the fetch so a large fragment doesn't materialize as one
    // giant ReconcileData message: send the first chunk now, queue the
    // rest for the caller to send as each chunk's data arrives.
    let mut chunks: VecDeque<Vec<String>> = fetch_keys
        .chunks(RECONCILE_FETCH_CHUNK)
        .map(|c| c.to_vec())
        .collect();
    let first = chunks.pop_front().unwrap_or_default();
    let expected_keys = first.iter().cloned().collect();
    if !send_envelope(
        &connection.peer_tx,
        EnvelopeMsg::ReconcileFetch(ReconcileFetch {
            fragment: fragment.0,
            keys: first,
            session_id: connection.session_id,
            round_id,
            chunk_seq: 0,
        }),
    )
    .await
    {
        return Err(ReconcileError::Transport);
    }
    Ok((Some((chunks, 0, expected_keys)), snapshot_revision))
}

async fn apply_reconcile_entries(
    fragment: FragmentId,
    router: &FragmentRouter,
    entries: Vec<SyncEntry>,
    expected_keys: &HashSet<String>,
) -> Result<(), ReconcileError> {
    if !reconcile_entries_are_exact(fragment, router, &entries, expected_keys) {
        tracing::warn!(
            fragment = fragment.0,
            expected = expected_keys.len(),
            received = entries.len(),
            "rejecting incomplete, duplicate, unrequested, or wrong-fragment reconcile data"
        );
        return Err(ReconcileError::Local);
    }
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    for entry in entries {
        let mut value = VersionedValue::new(entry.value, entry.timestamp_ms, entry.node_id);
        value.expires_at_ms = (entry.expires_at_ms != 0).then_some(entry.expires_at_ms);
        value.is_tombstone = entry.is_tombstone;
        let op = crate::actor::ReplicatedOp {
            key: entry.key,
            value,
            seq: 0,
            boot_id: 0,
        };
        match actor
            .call(
                |reply| KvMessage::ApplyOp {
                    op,
                    priority: OpPriority::Default,
                    is_replication: true,
                    reply: Some(reply),
                },
                None,
            )
            .await
        {
            Ok(CallResult::Success(Ok(_))) => {}
            Ok(CallResult::Success(Err(error))) => {
                tracing::warn!(fragment = fragment.0, %error, "reconcile apply failed");
                return Err(ReconcileError::Local);
            }
            Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => {
                return Err(ReconcileError::Local);
            }
        }
    }
    Ok(())
}

async fn fragment_revision(
    fragment: FragmentId,
    router: &FragmentRouter,
) -> Result<u64, ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor
        .call(|reply| KvMessage::GetRevision { reply }, None)
        .await
    {
        Ok(CallResult::Success(revision)) => Ok(revision),
        Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => Err(ReconcileError::Local),
    }
}

async fn flush_reconcile(
    fragment: FragmentId,
    router: &FragmentRouter,
) -> Result<(), ReconcileError> {
    let actor = router
        .route_for_fragment(fragment)
        .ok_or(ReconcileError::Local)?;
    match actor.call(|reply| KvMessage::Flush { reply }, None).await {
        Ok(CallResult::Success(Ok(
            ApplyDurability::StoreCommitted | ApplyDurability::MemoryApplied,
        ))) => Ok(()),
        Ok(CallResult::Success(Err(error))) => {
            tracing::warn!(fragment = fragment.0, %error, "reconcile flush failed");
            Err(ReconcileError::Local)
        }
        Ok(CallResult::Timeout | CallResult::SenderError) | Err(_) => Err(ReconcileError::Local),
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
