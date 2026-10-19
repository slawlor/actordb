// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Shared framing limits and protobuf encoding for the peer wire protocol.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex as StdMutex};

use prost::Message;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};

use crate::actor::{ReplicatedOp, VersionedValue};
use crate::proto::Envelope;

/// Hard limit for one length-prefixed protobuf envelope body.
pub(crate) const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Soft target for one anti-entropy page. A single individually valid entry
/// may exceed this target, but no encoded envelope may exceed
/// [`MAX_FRAME_BYTES`].
pub(crate) const ANTI_ENTROPY_PAGE_BYTES: usize = 1024 * 1024;

/// Maximum number of keys in one reconciliation fetch request. The responder
/// reads them incrementally and emits exact-byte-bounded data pages through
/// the byte-budgeted writer, avoiding both N-RTT catch-up and value bursts in
/// memory.
pub(crate) const MAX_RECONCILE_FETCH_KEYS: usize = 256;

/// Defensive count ceiling for repeated request/response and replication
/// fields. Exact byte limits remain authoritative; this prevents a legal-size
/// frame containing millions of zero-length protobuf elements from amplifying
/// into unbounded decode allocations.
pub(crate) const MAX_WIRE_BATCH_ITEMS: usize = 4096;
/// Sanity ceiling on ScanResponse.matches repeated-field count. The
/// per-fragment scan limit and the 16 MiB frame cap are the real bounds;
/// this only rejects absurd counts during preflight decode.
pub(crate) const MAX_SCAN_RESPONSE_MATCHES: usize = 1_000_000;
/// Provenance ids are carried in result metadata and must match the handshake
/// and placement identity bound.
pub(crate) const MAX_WIRE_NODE_ID_BYTES: usize = 255;
/// Peer-visible diagnostic text is capped so exact mutation response bounds do
/// not depend on backend-provided error lengths.
pub(crate) const MAX_WIRE_ERROR_BYTES: usize = 1024;

/// Summary and data page cardinality emitted by protocol v3.
pub(crate) const MAX_RECONCILE_PAGE_ITEMS: usize = 256;

/// One encoded body admitted to a Node-owned peer writer. Keeping the owned
/// byte permit in the queue item makes every drop path (timeouts, task aborts,
/// connection cancellation, and socket errors) release capacity by RAII.
pub(crate) struct BudgetedFrame {
    bytes: Vec<u8>,
    _byte_permit: OwnedSemaphorePermit,
    _producer_byte_permit: Option<OwnedSemaphorePermit>,
}

impl BudgetedFrame {
    pub(crate) fn new_producer(
        bytes: Vec<u8>,
        byte_permit: OwnedSemaphorePermit,
        producer_byte_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            bytes,
            _byte_permit: byte_permit,
            _producer_byte_permit: Some(producer_byte_permit),
        }
    }

    pub(crate) fn new_response(bytes: Vec<u8>, byte_permit: OwnedSemaphorePermit) -> Self {
        Self {
            bytes,
            _byte_permit: byte_permit,
            _producer_byte_permit: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Shared producer gate for one physical Node connection. All generic RPC and
/// per-fragment replication producers clone this exact wrapper, so neither the
/// frame-count nor encoded-byte budget can be bypassed or mismatched.
#[derive(Clone)]
pub(crate) struct BudgetedPeerTx {
    /// Ordinary producer traffic. Reader-generated protocol responses have a
    /// separate priority lane so a full data queue cannot make both peers
    /// stop reading while they wait to acknowledge one another.
    pub(crate) tx: mpsc::Sender<BudgetedFrame>,
    pub(crate) response_tx: mpsc::Sender<BudgetedFrame>,
    pub(crate) byte_budget: Arc<Semaphore>,
    /// Producer-only share of `byte_budget`. Keeping this smaller than the
    /// total leaves room for one ordinary anti-entropy/control response even
    /// when data producers have consumed their entire allowance.
    pub(crate) producer_byte_budget: Arc<Semaphore>,
    /// One anti-entropy round at a time per physical connection. This bounds
    /// responder work independently of the configured fragment count.
    pub(crate) reconcile_gate: ReconcileGate,
    pub(crate) identity: mpsc::Sender<Vec<u8>>,
}

#[derive(Default)]
struct ReconcileGateState {
    closed: bool,
    held: bool,
    next_ticket: u64,
    waiters: VecDeque<u64>,
}

/// A ticketed one-slot gate. Unlike `Semaphore::try_acquire`, registration is
/// retained between retries, so a fragment completing one round rejoins at the
/// tail instead of synchronously stealing the credit from woken waiters.
#[derive(Clone)]
pub(crate) struct ReconcileGate {
    state: Arc<StdMutex<ReconcileGateState>>,
    changes: watch::Sender<u64>,
}

impl ReconcileGate {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(StdMutex::new(ReconcileGateState::default())),
            changes: watch::channel(0).0,
        }
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.notify();
    }

    fn notify(&self) {
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn ticket(&self) -> Result<ReconcileTicket, ()> {
        let id = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(());
            }
            state.next_ticket = state.next_ticket.wrapping_add(1);
            if state.next_ticket == 0 {
                state.next_ticket = 1;
            }
            let id = state.next_ticket;
            state.waiters.push_back(id);
            id
        };
        Ok(ReconcileTicket {
            id,
            state: self.state.clone(),
            changes: self.changes.clone(),
            queued: true,
        })
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        usize::from(!self.state.lock().unwrap().held)
    }
}

pub(crate) struct ReconcileTicket {
    id: u64,
    state: Arc<StdMutex<ReconcileGateState>>,
    changes: watch::Sender<u64>,
    queued: bool,
}

impl ReconcileTicket {
    fn try_acquire(&mut self) -> Result<Option<ReconcilePermit>, ()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(());
        }
        if state.held || state.waiters.front() != Some(&self.id) {
            return Ok(None);
        }
        let popped = state.waiters.pop_front();
        debug_assert_eq!(popped, Some(self.id));
        state.held = true;
        self.queued = false;
        Ok(Some(ReconcilePermit {
            state: self.state.clone(),
            changes: self.changes.clone(),
        }))
    }
}

impl Drop for ReconcileTicket {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }
        let removed = {
            let mut state = self.state.lock().unwrap();
            let Some(index) = state.waiters.iter().position(|id| *id == self.id) else {
                return;
            };
            state.waiters.remove(index);
            true
        };
        if removed {
            self.changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });
        }
    }
}

pub(crate) struct ReconcilePermit {
    state: Arc<StdMutex<ReconcileGateState>>,
    changes: watch::Sender<u64>,
}

impl Drop for ReconcilePermit {
    fn drop(&mut self) {
        // Publish only after making the credit available. Otherwise a woken
        // sender can lose the sole notification while this guard still owns
        // the slot.
        self.state.lock().unwrap().held = false;
        self.changes.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

impl BudgetedPeerTx {
    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }

    pub(crate) fn close(&self) {
        self.byte_budget.close();
        self.producer_byte_budget.close();
        self.reconcile_gate.close();
    }

    pub(crate) fn try_acquire_reconcile(
        &self,
        ticket: &mut Option<ReconcileTicket>,
    ) -> Result<Option<ReconcilePermit>, ()> {
        if ticket.is_none() {
            *ticket = Some(self.reconcile_gate.ticket()?);
        }
        let permit = ticket.as_mut().expect("installed above").try_acquire()?;
        if permit.is_some() {
            drop(ticket.take());
        }
        Ok(permit)
    }

    pub(crate) fn reconcile_change_receiver(&self) -> watch::Receiver<u64> {
        self.reconcile_gate.subscribe()
    }
}

/// Failure to encode an envelope within a requested wire budget.
#[derive(Debug)]
pub(crate) enum WireEncodeError {
    /// The protobuf body would exceed the caller's byte budget.
    FrameTooLarge { encoded_len: usize, budget: usize },
    /// Record provenance is absent or exceeds the cluster identity bound.
    InvalidNodeIdLength { node_len: usize },
    /// Prost rejected the message while writing into the pre-sized buffer.
    Encode(prost::EncodeError),
}

impl fmt::Display for WireEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge {
                encoded_len,
                budget,
            } => write!(
                f,
                "encoded envelope is {encoded_len} bytes, exceeding the {budget}-byte budget"
            ),
            Self::InvalidNodeIdLength { node_len } => write!(
                f,
                "record node id is {node_len} bytes; expected 1 to {MAX_WIRE_NODE_ID_BYTES}"
            ),
            Self::Encode(error) => write!(f, "failed to encode envelope: {error}"),
        }
    }
}

impl std::error::Error for WireEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FrameTooLarge { .. } | Self::InvalidNodeIdLength { .. } => None,
            Self::Encode(error) => Some(error),
        }
    }
}

/// Allocation-free rejection reason for an inbound protobuf frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireDecodeError {
    Malformed(&'static str),
    MultipleMessages,
    MissingMessage,
    RetiredMessage,
    TooManyItems { message: &'static str, limit: usize },
}

impl fmt::Display for WireDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(reason) => write!(f, "malformed protobuf envelope: {reason}"),
            Self::MultipleMessages => write!(f, "protobuf envelope contains multiple messages"),
            Self::MissingMessage => write!(f, "protobuf envelope contains no supported message"),
            Self::RetiredMessage => write!(f, "protobuf envelope contains a retired message"),
            Self::TooManyItems { message, limit } => {
                write!(f, "{message} exceeds the {limit}-item wire limit")
            }
        }
    }
}

impl std::error::Error for WireDecodeError {}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, WireDecodeError> {
    let mut value = 0u64;
    for index in 0..10 {
        let byte = *bytes
            .get(*cursor)
            .ok_or(WireDecodeError::Malformed("truncated varint"))?;
        *cursor += 1;
        if index == 9 && byte > 1 {
            return Err(WireDecodeError::Malformed("varint overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(WireDecodeError::Malformed("unterminated varint"))
}

fn take_length_delimited<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
) -> Result<&'a [u8], WireDecodeError> {
    let len = usize::try_from(read_varint(bytes, cursor)?)
        .map_err(|_| WireDecodeError::Malformed("length does not fit usize"))?;
    let end = cursor
        .checked_add(len)
        .ok_or(WireDecodeError::Malformed("length overflow"))?;
    let payload = bytes.get(*cursor..end).ok_or(WireDecodeError::Malformed(
        "truncated length-delimited field",
    ))?;
    *cursor = end;
    Ok(payload)
}

fn skip_field(bytes: &[u8], cursor: &mut usize, wire_type: u8) -> Result<(), WireDecodeError> {
    match wire_type {
        0 => {
            let _ = read_varint(bytes, cursor)?;
        }
        1 => {
            *cursor = cursor
                .checked_add(8)
                .filter(|end| *end <= bytes.len())
                .ok_or(WireDecodeError::Malformed("truncated fixed64 field"))?;
        }
        2 => {
            let _ = take_length_delimited(bytes, cursor)?;
        }
        5 => {
            *cursor = cursor
                .checked_add(4)
                .filter(|end| *end <= bytes.len())
                .ok_or(WireDecodeError::Malformed("truncated fixed32 field"))?;
        }
        _ => return Err(WireDecodeError::Malformed("unsupported wire type")),
    }
    Ok(())
}

fn preflight_repeated_field(
    message: &[u8],
    repeated_field: u32,
    limit: usize,
    message_name: &'static str,
) -> Result<(), WireDecodeError> {
    let mut cursor = 0usize;
    let mut count = 0usize;
    while cursor < message.len() {
        let key = read_varint(message, &mut cursor)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| WireDecodeError::Malformed("field number overflows u32"))?;
        if field == 0 {
            return Err(WireDecodeError::Malformed("field number is zero"));
        }
        let wire_type = (key & 7) as u8;
        if field == repeated_field {
            if wire_type != 2 {
                return Err(WireDecodeError::Malformed(
                    "repeated field has the wrong wire type",
                ));
            }
            count = count.saturating_add(1);
            if count > limit {
                return Err(WireDecodeError::TooManyItems {
                    message: message_name,
                    limit,
                });
            }
        }
        skip_field(message, &mut cursor, wire_type)?;
    }
    Ok(())
}

fn preflight_concern_field(
    message: &[u8],
    concern_field: u32,
    receipt: bool,
) -> Result<(), WireDecodeError> {
    let mut cursor = 0;
    let mut seen = false;
    while cursor < message.len() {
        let tag = read_varint(message, &mut cursor)?;
        if tag >> 3 == u64::from(concern_field) {
            if tag & 7 != 2 || seen {
                return Err(WireDecodeError::Malformed(
                    "invalid or duplicate write concern",
                ));
            }
            seen = true;
            let bytes = take_length_delimited(message, &mut cursor)?;
            if receipt {
                actordb_protocol::write_concern::preflight_result(bytes)
                    .map_err(|_| WireDecodeError::Malformed("invalid write concern receipt"))?;
            } else if bytes.len() > actordb_protocol::write_concern::MAX_WRITE_CONCERN_POLICY_BYTES
            {
                return Err(WireDecodeError::Malformed(
                    "write concern policy exceeds byte limit",
                ));
            }
        } else {
            skip_field(message, &mut cursor, (tag & 7) as u8)?;
        }
    }
    Ok(())
}

fn preflight_concerns_in_entries(
    message: &[u8],
    entry_field: u32,
    concern_field: u32,
    receipt: bool,
) -> Result<(), WireDecodeError> {
    let mut cursor = 0;
    while cursor < message.len() {
        let tag = read_varint(message, &mut cursor)?;
        if tag >> 3 == u64::from(entry_field) && tag & 7 == 2 {
            preflight_concern_field(
                take_length_delimited(message, &mut cursor)?,
                concern_field,
                receipt,
            )?;
        } else {
            skip_field(message, &mut cursor, (tag & 7) as u8)?;
        }
    }
    Ok(())
}

/// Inspect the envelope and dangerous repeated fields before Prost allocates.
/// This intentionally understands only the outer message and immediate
/// repeated fields; byte payloads stay opaque and are bounded by the frame.
pub(crate) fn preflight_envelope_decode(frame: &[u8]) -> Result<(), WireDecodeError> {
    let mut cursor = 0usize;
    let mut messages = 0usize;
    while cursor < frame.len() {
        let key = read_varint(frame, &mut cursor)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| WireDecodeError::Malformed("field number overflows u32"))?;
        if field == 0 {
            return Err(WireDecodeError::Malformed("field number is zero"));
        }
        let wire_type = (key & 7) as u8;
        if (1..=24).contains(&field) {
            messages += 1;
            if messages > 1 {
                return Err(WireDecodeError::MultipleMessages);
            }
            if wire_type != 2 {
                return Err(WireDecodeError::Malformed(
                    "envelope message has the wrong wire type",
                ));
            }
            let payload = take_length_delimited(frame, &mut cursor)?;
            match field {
                5 => preflight_concern_field(payload, 12, false)?,
                // Retired ReplicateOp, SyncRequest, and SyncResponse.
                2..=4 => return Err(WireDecodeError::RetiredMessage),
                9 => preflight_repeated_field(
                    payload,
                    4,
                    MAX_WIRE_BATCH_ITEMS,
                    "ReplicateBatch.ops",
                )?,
                12 => preflight_repeated_field(
                    payload,
                    2,
                    MAX_RECONCILE_PAGE_ITEMS,
                    "ReconcileResponse.summaries",
                )?,
                13 => preflight_repeated_field(
                    payload,
                    2,
                    MAX_RECONCILE_FETCH_KEYS,
                    "ReconcileFetch.keys",
                )?,
                14 => preflight_repeated_field(
                    payload,
                    2,
                    MAX_RECONCILE_PAGE_ITEMS,
                    "ReconcileData.entries",
                )?,
                15 => {
                    preflight_repeated_field(payload, 1, MAX_WIRE_BATCH_ITEMS, "BatchPut.puts")?;
                    preflight_concerns_in_entries(payload, 1, 12, false)?;
                }
                16 => preflight_repeated_field(payload, 2, MAX_WIRE_BATCH_ITEMS, "BatchGet.keys")?,
                17 => preflight_repeated_field(
                    payload,
                    2,
                    MAX_WIRE_BATCH_ITEMS,
                    "BatchGetResponse.results",
                )?,
                18 => {
                    preflight_repeated_field(
                        payload,
                        2,
                        MAX_WIRE_BATCH_ITEMS,
                        "MutationResponse.results",
                    )?;
                    preflight_concerns_in_entries(payload, 2, 8, true)?;
                }
                // ScanResponse.matches: bounded by the per-fragment scan
                // limit; the 16 MiB frame cap is the hard bound. This
                // preflight count is a sanity ceiling against absurd
                // repeated-field counts.
                22 => preflight_repeated_field(
                    payload,
                    2,
                    MAX_SCAN_RESPONSE_MATCHES,
                    "ScanResponse.matches",
                )?,
                24 => {
                    preflight_repeated_field(payload, 3, 256, "QueryPageResponse.rows")?;
                    let mut row_cursor = 0usize;
                    while row_cursor < payload.len() {
                        let key = read_varint(payload, &mut row_cursor)?;
                        if key >> 3 == 3 && key & 7 == 2 {
                            let row = take_length_delimited(payload, &mut row_cursor)?;
                            preflight_repeated_field(row, 2, 64, "QueryPageRow.cells")?;
                        } else {
                            skip_field(payload, &mut row_cursor, (key & 7) as u8)?;
                        }
                    }
                }
                _ => {}
            }
        } else {
            skip_field(frame, &mut cursor, wire_type)?;
        }
    }
    if messages == 0 {
        Err(WireDecodeError::MissingMessage)
    } else {
        Ok(())
    }
}

/// Exact protobuf body length, excluding the transport's four-byte prefix.
pub(crate) fn envelope_encoded_len(envelope: &Envelope) -> usize {
    envelope.encoded_len()
}

/// Check an envelope against an exact byte budget without allocating its
/// encoded body. The returned length can be used to pre-size the output.
pub(crate) fn check_envelope_budget(
    envelope: &Envelope,
    budget: usize,
) -> Result<usize, WireEncodeError> {
    let encoded_len = envelope_encoded_len(envelope);
    if encoded_len > budget {
        return Err(WireEncodeError::FrameTooLarge {
            encoded_len,
            budget,
        });
    }
    Ok(encoded_len)
}

/// Whether an envelope fits an exact byte budget.
#[cfg(test)]
pub(crate) fn envelope_fits_budget(envelope: &Envelope, budget: usize) -> bool {
    check_envelope_budget(envelope, budget).is_ok()
}

/// Encode one envelope after preflighting the hard frame limit.
pub(crate) fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>, WireEncodeError> {
    let encoded_len = check_envelope_budget(envelope, MAX_FRAME_BYTES)?;
    let mut frame = Vec::with_capacity(encoded_len);
    envelope
        .encode(&mut frame)
        .map_err(WireEncodeError::Encode)?;
    debug_assert_eq!(frame.len(), encoded_len);
    Ok(frame)
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn tag_len(field: u32, wire_type: u8) -> usize {
    varint_len(((field as u64) << 3) | u64::from(wire_type))
}

/// Exact encoded length of a singular protobuf varint field. Proto3 omits a
/// zero scalar, matching Prost's generated encoder.
pub(crate) fn varint_field_len(field: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        tag_len(field, 0).saturating_add(varint_len(value))
    }
}

pub(crate) fn bytes_field_len(field: u32, payload_len: usize) -> usize {
    if payload_len == 0 {
        0
    } else {
        tag_len(field, 2)
            .saturating_add(varint_len(payload_len as u64))
            .saturating_add(payload_len)
    }
}

pub(crate) fn message_field_len(field: u32, payload_len: usize) -> usize {
    tag_len(field, 2)
        .saturating_add(varint_len(payload_len as u64))
        .saturating_add(payload_len)
}

/// Exact encoded length of one `BatchGetResponse.KeyValue` payload without
/// cloning its value. The actor-side batch reader uses this to stop after one
/// ordered soft-byte-bounded prefix; the network layer still performs the
/// authoritative whole-envelope preflight before sending.
pub(crate) fn batch_get_result_encoded_len(
    key: &str,
    value: Option<&VersionedValue>,
    shed: bool,
) -> usize {
    let mut encoded_len = bytes_field_len(1, key.len());
    if shed {
        return encoded_len.saturating_add(varint_field_len(8, 1));
    }
    let Some(value) = value else {
        return encoded_len;
    };
    encoded_len = encoded_len
        .saturating_add(varint_field_len(2, 1))
        .saturating_add(bytes_field_len(3, value.value.len()))
        .saturating_add(varint_field_len(4, value.timestamp_ms))
        .saturating_add(bytes_field_len(5, value.node_id.len()))
        .saturating_add(varint_field_len(6, value.expires_at_ms.unwrap_or(0)))
        .saturating_add(varint_field_len(7, u64::from(value.is_tombstone)));
    encoded_len
}

/// Exact size of one envelope message whose payload contains a repeated
/// length-delimited field. The fixed payload is measured once; each string,
/// bytes, or nested-message item then contributes only its own field length.
/// This keeps page construction linear without changing Prost's wire size.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RepeatedFieldSizer {
    envelope_field: u32,
    item_field: u32,
    payload_len: usize,
}

impl RepeatedFieldSizer {
    pub(crate) fn new(envelope_field: u32, item_field: u32, base_payload_len: usize) -> Self {
        Self {
            envelope_field,
            item_field,
            payload_len: base_payload_len,
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        message_field_len(self.envelope_field, self.payload_len)
    }

    pub(crate) fn encoded_len_with(&self, item_payload_len: usize) -> usize {
        let payload_len = self
            .payload_len
            .saturating_add(message_field_len(self.item_field, item_payload_len));
        message_field_len(self.envelope_field, payload_len)
    }

    pub(crate) fn push(&mut self, item_payload_len: usize) -> usize {
        self.payload_len = self
            .payload_len
            .saturating_add(message_field_len(self.item_field, item_payload_len));
        self.encoded_len()
    }

    pub(crate) fn reset(&mut self, base_payload_len: usize) {
        self.payload_len = base_payload_len;
    }
}

fn max_u64_field_len(field: u32) -> usize {
    tag_len(field, 0) + 10
}

fn max_u32_field_len(field: u32) -> usize {
    tag_len(field, 0) + 5
}

fn small_varint_field_len(field: u32) -> usize {
    tag_len(field, 0) + 1
}

fn bool_field_len(field: u32, present: bool) -> usize {
    if present {
        tag_len(field, 0) + 1
    } else {
        0
    }
}

fn replicated_record_max_encoded_len(
    key_len: usize,
    value_len: usize,
    node_len: usize,
    tombstone: bool,
) -> usize {
    let replicate_op = bytes_field_len(1, key_len)
        + bytes_field_len(2, value_len)
        + max_u64_field_len(3)
        + bytes_field_len(4, node_len)
        + max_u64_field_len(5)
        + max_u64_field_len(6)
        + max_u64_field_len(7)
        + bool_field_len(8, tombstone);
    let replicate_batch = max_u64_field_len(1)
        + max_u64_field_len(2)
        + max_u32_field_len(3)
        + message_field_len(4, replicate_op)
        + small_varint_field_len(5);
    let replicate_envelope = message_field_len(9, replicate_batch);

    let sync_entry = bytes_field_len(1, key_len)
        + bytes_field_len(2, value_len)
        + max_u64_field_len(3)
        + bytes_field_len(4, node_len)
        + max_u64_field_len(5)
        + bool_field_len(6, tombstone);
    let reconcile_data = max_u32_field_len(1)
        + message_field_len(2, sync_entry)
        + max_u64_field_len(3)
        + max_u64_field_len(4)
        + max_u64_field_len(5)
        + max_u64_field_len(6)
        + bool_field_len(7, true);
    let reconcile_envelope = message_field_len(14, reconcile_data);

    // A summary page repeats its final key as the continuation cursor. Keep
    // this shape in the admission bound so a large key that fits every value
    // frame cannot become permanently unreconcilable.
    let key_summary = bytes_field_len(1, key_len)
        + max_u64_field_len(2)
        + bytes_field_len(3, node_len)
        + bool_field_len(4, tombstone);
    let reconcile_response = max_u32_field_len(1)
        + message_field_len(2, key_summary)
        + max_u64_field_len(3)
        + max_u64_field_len(4)
        + max_u64_field_len(5)
        + bytes_field_len(6, key_len)
        + max_u64_field_len(7)
        + max_u64_field_len(8)
        + bool_field_len(9, true);
    let reconcile_summary_envelope = message_field_len(12, reconcile_response);

    let forwarded_put = bytes_field_len(1, key_len)
        + bytes_field_len(2, value_len)
        + max_u64_field_len(3)
        + bytes_field_len(4, node_len)
        + max_u64_field_len(5)
        + max_u64_field_len(6)
        + max_u64_field_len(7)
        + bool_field_len(8, tombstone)
        + small_varint_field_len(9)
        + max_u32_field_len(10)
        + max_u64_field_len(11);
    let batch_put =
        message_field_len(1, forwarded_put) + small_varint_field_len(2) + max_u64_field_len(3);
    let batch_put_envelope = message_field_len(15, batch_put);

    let batch_key_value = bytes_field_len(1, key_len)
        + bool_field_len(2, true)
        + bytes_field_len(3, value_len)
        + max_u64_field_len(4)
        + bytes_field_len(5, node_len)
        + max_u64_field_len(6)
        + bool_field_len(7, tombstone);
    let batch_get_response = max_u64_field_len(1) + message_field_len(2, batch_key_value);
    let batch_get_envelope = message_field_len(17, batch_get_response);

    // ForwardedPut and BatchPut both return MutationResponse. Count mutually
    // exclusive error and superseded-winner fields together so every exact
    // outcome for one admitted record remains encodable.
    let mutation_result = bytes_field_len(1, key_len)
        + small_varint_field_len(2)
        + bytes_field_len(3, MAX_WIRE_ERROR_BYTES)
        + max_u64_field_len(4)
        + bytes_field_len(5, MAX_WIRE_NODE_ID_BYTES)
        + bool_field_len(6, true)
        + max_u32_field_len(7);
    let mutation_response = max_u64_field_len(1) + message_field_len(2, mutation_result);
    let mutation_response_envelope = message_field_len(18, mutation_response);

    replicate_envelope
        .max(reconcile_envelope)
        .max(reconcile_summary_envelope)
        .max(batch_put_envelope)
        .max(batch_get_envelope)
        .max(mutation_response_envelope)
}

#[cfg(test)]
fn replicated_op_max_encoded_len(op: &ReplicatedOp) -> usize {
    replicated_record_max_encoded_len(
        op.key.len(),
        op.value.value.len(),
        op.value.node_id.len(),
        op.value.is_tombstone,
    )
}

pub(crate) fn check_record_budget(
    key_len: usize,
    value_len: usize,
    node_len: usize,
) -> Result<(), WireEncodeError> {
    check_node_id_budget(node_len)?;
    // Every admitted key must remain encodable after an explicit delete or
    // TTL conversion. Size the bool fields as present even for a live value.
    let encoded_len = replicated_record_max_encoded_len(key_len, value_len, node_len, true);
    if encoded_len > MAX_FRAME_BYTES {
        Err(WireEncodeError::FrameTooLarge {
            encoded_len,
            budget: MAX_FRAME_BYTES,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn check_node_id_budget(node_len: usize) -> Result<(), WireEncodeError> {
    if node_len == 0 || node_len > MAX_WIRE_NODE_ID_BYTES {
        return Err(WireEncodeError::InvalidNodeIdLength { node_len });
    }
    Ok(())
}

pub(crate) fn check_versioned_value_budget(
    key: &str,
    value: &VersionedValue,
) -> Result<(), WireEncodeError> {
    check_record_budget(key.len(), value.value.len(), value.node_id.len())
}

/// Verify that one record can traverse every single-value protocol envelope.
/// Repeated-message builders still enforce their exact aggregate byte budget;
/// this rejects the irreducible case before a local mutation can be accepted.
pub(crate) fn check_replicated_op_budget(op: &ReplicatedOp) -> Result<(), WireEncodeError> {
    check_versioned_value_budget(&op.key, &op.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::VersionedValue;
    use crate::proto::{
        BatchGet, BatchGetResponse, BatchKeyValue, BatchPut, EnvelopeMsg, ForwardedPut, KeySummary,
        MutationResponse, MutationResult, MutationStatus, ReconcileData, ReconcileFetch,
        ReconcileResponse, ReplicateBatch, ReplicateOp as ProtoReplicateOp, ReplicationAckLevel,
        SyncEntry,
    };

    fn data_envelope(value_len: usize) -> Envelope {
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 7,
                entries: vec![SyncEntry {
                    key: "key".to_string(),
                    value: vec![0; value_len],
                    timestamp_ms: 11,
                    node_id: "node".to_string(),
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
                session_id: 13,
                round_id: 17,
                chunk_seq: 19,
                page_seq: 23,
                done: true,
            })),
        }
    }

    fn push_varint(mut value: u64, out: &mut Vec<u8>) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn raw_envelope(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        push_varint(u64::from((field << 3) | 2), &mut frame);
        push_varint(payload.len() as u64, &mut frame);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn exact_budget_boundary_is_inclusive() {
        let envelope = data_envelope(32);
        let encoded_len = envelope_encoded_len(&envelope);

        assert_eq!(
            check_envelope_budget(&envelope, encoded_len).unwrap(),
            encoded_len
        );
        assert!(envelope_fits_budget(&envelope, encoded_len));
        assert!(!envelope_fits_budget(&envelope, encoded_len - 1));
    }

    #[test]
    fn soft_page_budget_is_distinct_from_hard_frame_limit() {
        let envelope = data_envelope(ANTI_ENTROPY_PAGE_BYTES);

        assert!(!envelope_fits_budget(&envelope, ANTI_ENTROPY_PAGE_BYTES));
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(encoded.len(), envelope_encoded_len(&envelope));
        assert!(encoded.len() < MAX_FRAME_BYTES);
    }

    #[test]
    fn hard_limit_is_checked_before_encoding() {
        let envelope = data_envelope(MAX_FRAME_BYTES);
        let encoded_len = envelope_encoded_len(&envelope);
        assert!(encoded_len > MAX_FRAME_BYTES);

        assert!(matches!(
            encode_envelope(&envelope),
            Err(WireEncodeError::FrameTooLarge {
                encoded_len: actual,
                budget: MAX_FRAME_BYTES,
            }) if actual == encoded_len
        ));
    }

    #[test]
    fn shared_encoder_matches_prost_output() {
        let envelope = data_envelope(128);
        let mut direct = Vec::new();
        envelope.encode(&mut direct).unwrap();

        assert_eq!(encode_envelope(&envelope).unwrap(), direct);
        assert!(preflight_envelope_decode(&direct).is_ok());
    }

    fn assert_single_repeated_item_size(
        envelope_field: u32,
        item_field: u32,
        base_payload_len: usize,
        item_payload_len: usize,
        envelope: Envelope,
    ) {
        let mut sizing = RepeatedFieldSizer::new(envelope_field, item_field, base_payload_len);
        assert_eq!(
            sizing.encoded_len_with(item_payload_len),
            envelope.encoded_len()
        );
        assert_eq!(sizing.push(item_payload_len), envelope.encoded_len());
        sizing.reset(base_payload_len);
        assert_eq!(
            sizing.encoded_len_with(item_payload_len),
            envelope.encoded_len()
        );
    }

    #[test]
    fn repeated_field_sizer_matches_every_batched_envelope_tag() {
        let op = ProtoReplicateOp::default();
        assert_single_repeated_item_size(
            9,
            4,
            ReplicateBatch::default().encoded_len(),
            op.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                    ops: vec![op],
                    ..ReplicateBatch::default()
                })),
            },
        );

        let summary = KeySummary::default();
        assert_single_repeated_item_size(
            12,
            2,
            ReconcileResponse::default().encoded_len(),
            summary.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    summaries: vec![summary],
                    ..ReconcileResponse::default()
                })),
            },
        );

        assert_single_repeated_item_size(
            13,
            2,
            ReconcileFetch::default().encoded_len(),
            0,
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                    keys: vec![String::new()],
                    ..ReconcileFetch::default()
                })),
            },
        );

        let entry = SyncEntry::default();
        assert_single_repeated_item_size(
            14,
            2,
            ReconcileData::default().encoded_len(),
            entry.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                    entries: vec![entry],
                    ..ReconcileData::default()
                })),
            },
        );

        let put = ForwardedPut::default();
        assert_single_repeated_item_size(
            15,
            1,
            BatchPut::default().encoded_len(),
            put.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    puts: vec![put],
                    ..BatchPut::default()
                })),
            },
        );

        assert_single_repeated_item_size(
            16,
            2,
            BatchGet::default().encoded_len(),
            0,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    keys: vec![String::new()],
                    ..BatchGet::default()
                })),
            },
        );

        let value = BatchKeyValue::default();
        assert_single_repeated_item_size(
            17,
            2,
            BatchGetResponse::default().encoded_len(),
            value.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                    results: vec![value],
                    ..BatchGetResponse::default()
                })),
            },
        );

        let result = MutationResult::default();
        assert_single_repeated_item_size(
            18,
            2,
            MutationResponse::default().encoded_len(),
            result.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                    results: vec![result],
                    ..MutationResponse::default()
                })),
            },
        );
    }

    #[test]
    fn batch_get_result_sizer_matches_prost() {
        let missing = BatchKeyValue {
            key: "missing".to_string(),
            ..BatchKeyValue::default()
        };
        assert_eq!(
            batch_get_result_encoded_len(&missing.key, None, false),
            missing.encoded_len()
        );

        let shed = BatchKeyValue {
            key: String::new(),
            shed: true,
            ..BatchKeyValue::default()
        };
        assert_eq!(
            batch_get_result_encoded_len(&shed.key, None, true),
            shed.encoded_len()
        );

        let mut value = VersionedValue::new(vec![7; 128], u64::MAX, "writer".to_string());
        value.expires_at_ms = Some(u64::MAX);
        value.is_tombstone = true;
        let found = BatchKeyValue {
            key: "found".to_string(),
            found: true,
            value: value.value.clone(),
            timestamp_ms: value.timestamp_ms,
            node_id: value.node_id.clone(),
            expires_at_ms: value.expires_at_ms.unwrap_or(0),
            is_tombstone: value.is_tombstone,
            shed: false,
        };
        assert_eq!(
            batch_get_result_encoded_len(&found.key, Some(&value), false),
            found.encoded_len()
        );
    }

    #[test]
    fn repeated_string_sizer_matches_prost_at_varint_boundaries() {
        let mut request = ReconcileFetch {
            fragment: u32::MAX,
            keys: Vec::new(),
            session_id: u64::MAX,
            round_id: u64::MAX,
            chunk_seq: u64::MAX,
        };
        let base_payload_len = request.encoded_len();
        let mut sizing = RepeatedFieldSizer::new(13, 2, base_payload_len);
        assert_eq!(
            sizing.encoded_len(),
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileFetch(request.clone()))
            }
            .encoded_len()
        );

        for item_len in [0, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152] {
            let key = "k".repeat(item_len);
            let predicted = sizing.encoded_len_with(item_len);
            request.keys.push(key);
            let actual = Envelope {
                msg: Some(EnvelopeMsg::ReconcileFetch(request.clone())),
            }
            .encoded_len();
            assert_eq!(predicted, actual, "item length {item_len}");
            assert_eq!(sizing.push(item_len), actual, "item length {item_len}");
        }

        sizing.reset(usize::MAX);
        assert_eq!(sizing.encoded_len(), usize::MAX);
        assert_eq!(sizing.encoded_len_with(1), usize::MAX);
        assert_eq!(sizing.push(1), usize::MAX);
    }

    #[test]
    fn decode_preflight_rejects_repeated_field_amplification() {
        let mut response = Vec::new();
        for _ in 0..=MAX_RECONCILE_PAGE_ITEMS {
            // ReconcileResponse.summaries: empty nested KeySummary.
            response.extend_from_slice(&[0x12, 0x00]);
        }
        let frame = raw_envelope(12, &response);

        assert!(matches!(
            preflight_envelope_decode(&frame),
            Err(WireDecodeError::TooManyItems {
                message: "ReconcileResponse.summaries",
                limit: MAX_RECONCILE_PAGE_ITEMS,
            })
        ));
    }

    #[test]
    fn decode_preflight_rejects_retired_and_ambiguous_envelopes() {
        assert_eq!(
            preflight_envelope_decode(&raw_envelope(3, &[])),
            Err(WireDecodeError::RetiredMessage)
        );

        let mut duplicate = raw_envelope(1, &[]);
        duplicate.extend_from_slice(&raw_envelope(20, &[]));
        assert_eq!(
            preflight_envelope_decode(&duplicate),
            Err(WireDecodeError::MultipleMessages)
        );
        assert_eq!(
            preflight_envelope_decode(&[]),
            Err(WireDecodeError::MissingMessage)
        );
    }

    #[test]
    fn irreducible_oversized_record_is_rejected() {
        let small = ReplicatedOp {
            key: "key".to_string(),
            value: VersionedValue::new(vec![0; 32], 1, "node".to_string()),
            seq: 1,
            boot_id: 1,
        };
        assert!(check_replicated_op_budget(&small).is_ok());

        let oversized = ReplicatedOp {
            key: "key".to_string(),
            value: VersionedValue::new(vec![0; MAX_FRAME_BYTES], 1, "node".to_string()),
            seq: 1,
            boot_id: 1,
        };
        assert!(matches!(
            check_replicated_op_budget(&oversized),
            Err(WireEncodeError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn record_provenance_matches_cluster_identity_bound() {
        assert!(matches!(
            check_record_budget(1, 1, 0),
            Err(WireEncodeError::InvalidNodeIdLength { node_len: 0 })
        ));
        assert!(check_record_budget(1, 1, MAX_WIRE_NODE_ID_BYTES).is_ok());
        assert!(matches!(
            check_record_budget(1, 1, MAX_WIRE_NODE_ID_BYTES + 1),
            Err(WireEncodeError::InvalidNodeIdLength { node_len })
                if node_len == MAX_WIRE_NODE_ID_BYTES + 1
        ));
    }

    #[test]
    fn allocation_free_record_bound_covers_every_value_envelope() {
        let op = ReplicatedOp {
            key: "key".to_string(),
            value: VersionedValue::new(vec![1; 127], 1, "node".to_string()),
            seq: 1,
            boot_id: 1,
        };
        let proto = ProtoReplicateOp {
            key: op.key.clone(),
            value: op.value.value.clone(),
            timestamp_ms: u64::MAX,
            node_id: op.value.node_id.clone(),
            seq: u64::MAX,
            boot_id: u64::MAX,
            expires_at_ms: u64::MAX,
            is_tombstone: false,
        };
        let entry = SyncEntry {
            key: op.key.clone(),
            value: op.value.value.clone(),
            timestamp_ms: u64::MAX,
            node_id: op.value.node_id.clone(),
            expires_at_ms: u64::MAX,
            is_tombstone: false,
        };
        let forwarded = ForwardedPut {
            key: op.key.clone(),
            value: op.value.value.clone(),
            timestamp_ms: u64::MAX,
            node_id: op.value.node_id.clone(),
            seq: u64::MAX,
            boot_id: u64::MAX,
            expires_at_ms: u64::MAX,
            is_tombstone: false,
            priority: 2,
            hops: u32::MAX,
            req_id: u64::MAX,

            write_concern: None,
        };
        let envelopes = [
            Envelope {
                msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                    session_id: u64::MAX,
                    batch_seq: u64::MAX,
                    fragment: u32::MAX,
                    ops: vec![proto],
                    required_ack_level: ReplicationAckLevel::StoreCommitted as i32,
                })),
            },
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileData(ReconcileData {
                    fragment: u32::MAX,
                    entries: vec![entry],
                    session_id: u64::MAX,
                    round_id: u64::MAX,
                    chunk_seq: u64::MAX,
                    page_seq: u64::MAX,
                    done: true,
                })),
            },
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                    fragment: u32::MAX,
                    summaries: vec![KeySummary {
                        key: op.key.clone(),
                        timestamp_ms: u64::MAX,
                        node_id: op.value.node_id.clone(),
                        is_tombstone: false,
                    }],
                    session_id: u64::MAX,
                    round_id: u64::MAX,
                    page_seq: u64::MAX,
                    next_after_key: op.key.clone(),
                    snapshot_revision: u64::MAX,
                    snapshot_time_ms: u64::MAX,
                    done: true,
                    restart: false,
                })),
            },
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                    puts: vec![forwarded],
                    priority: 2,
                    req_id: u64::MAX,
                })),
            },
            Envelope {
                msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                    req_id: u64::MAX,
                    results: vec![BatchKeyValue {
                        key: op.key.clone(),
                        found: true,
                        value: op.value.value.clone(),
                        timestamp_ms: u64::MAX,
                        node_id: op.value.node_id.clone(),
                        expires_at_ms: u64::MAX,
                        is_tombstone: false,
                        shed: false,
                    }],
                })),
            },
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                    req_id: u64::MAX,
                    results: vec![MutationResult {
                        key: op.key.clone(),
                        status: MutationStatus::Unavailable as i32,
                        error: "e".repeat(MAX_WIRE_ERROR_BYTES),
                        winner_timestamp_ms: u64::MAX,
                        winner_node_id: "n".repeat(MAX_WIRE_NODE_ID_BYTES),
                        winner_is_tombstone: true,
                        request_index: u32::MAX,

                        write_concern: None,
                    }],
                })),
            },
        ];
        let actual_max = envelopes.iter().map(Message::encoded_len).max().unwrap();
        assert_eq!(replicated_op_max_encoded_len(&op), actual_max);
    }

    #[test]
    fn key_must_fit_both_summary_and_continuation_cursor() {
        let key = "k".repeat(MAX_FRAME_BYTES / 2);
        let op = ReplicatedOp {
            key: key.clone(),
            value: VersionedValue::new(Vec::new(), 1, "node".to_string()),
            seq: 1,
            boot_id: 1,
        };
        let value_envelope = Envelope {
            msg: Some(EnvelopeMsg::ReplicateBatch(ReplicateBatch {
                session_id: u64::MAX,
                batch_seq: u64::MAX,
                fragment: u32::MAX,
                ops: vec![ProtoReplicateOp {
                    key,
                    value: Vec::new(),
                    timestamp_ms: u64::MAX,
                    node_id: "node".to_string(),
                    seq: u64::MAX,
                    boot_id: u64::MAX,
                    expires_at_ms: u64::MAX,
                    is_tombstone: false,
                }],
                required_ack_level: ReplicationAckLevel::StoreCommitted as i32,
            })),
        };

        assert!(envelope_fits_budget(&value_envelope, MAX_FRAME_BYTES));
        assert!(matches!(
            check_replicated_op_budget(&op),
            Err(WireEncodeError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn live_record_admission_reserves_future_tombstone_bits() {
        let mut low = 0usize;
        let mut high = MAX_FRAME_BYTES;
        while low < high {
            let midpoint = low + (high - low).div_ceil(2);
            if replicated_record_max_encoded_len(midpoint, 0, 4, false) <= MAX_FRAME_BYTES {
                low = midpoint;
            } else {
                high = midpoint - 1;
            }
        }

        assert!(replicated_record_max_encoded_len(low, 0, 4, false) <= MAX_FRAME_BYTES);
        assert!(replicated_record_max_encoded_len(low, 0, 4, true) > MAX_FRAME_BYTES);
        assert!(matches!(
            check_record_budget(low, 0, 4),
            Err(WireEncodeError::FrameTooLarge { .. })
        ));
    }
}
