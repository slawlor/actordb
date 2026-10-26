//! Decoded-object admission, separate from encoded framing.
//!
//! The budget, header/payload and pending-shape walk is allocation-free and
//! precedes all message/collection materialization. The existing typed receipt
//! validator can allocate bounded error metadata on malformed receipts; that
//! rejection does not materialize their decoded messages or payloads.

use std::mem::size_of;

use crate::{client, meta, write_concern, MAX_BATCH_ITEMS, MAX_FRAME_BYTES};

/// Default accounted response allowance, including raw framing, decoded
/// collections/payloads and portable conversion work. Not an allocator/RSS cap.
pub const DEFAULT_MAX_DECODED_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
/// Conservative portable QueryValue header allowance. SDK tests check that
/// their actual layout fits this charge without coupling the wire crate to SQL.
pub const PORTABLE_CELL_HEADER_CHARGE: usize = 64;
const MAP_SLOT_CHARGE: usize = 128;
const VALIDATION_SLOT_CHARGE: usize = 64;
const MAX_WRITER_BYTES: usize = 255;

/// Work admitted before Prost or SDK receipt/result conversion allocates.
/// Header charges include Vec geometric growth/minimum capacity and conservative
/// map/portable-conversion storage. Payloads are charged twice for decoded and
/// temporary validation copies. Caller-retained returned results, allocator
/// slack, transport buffers and outgoing request storage are outside this scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientDecodeReport {
    pub accounted_bytes: usize,
    pub raw_bytes: usize,
    pub header_bytes: usize,
    pub payload_bytes: usize,
    pub query_cells: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read(usize),
    Mutation(usize),
    Query,
    Inspect,
    Scan,
    Unsupported,
}

/// Expectations derived from the actual pending application request, never
/// from data echoed by the server. Use optional query shape from a trusted
/// locally parsed plan; the protocol preflight does not parse SQL.
#[derive(Debug, Clone, Copy)]
pub struct ClientResponseShape {
    id: u64,
    operation: Operation,
    columns: Option<usize>,
    rows: usize,
    aggregate: bool,
}
impl ClientResponseShape {
    pub fn from_request(request: &client::Request) -> Self {
        use client::request::Operation as Request;
        let operation = match &request.operation {
            Some(Request::Get(_)) => Operation::Read(1),
            Some(Request::BatchGet(batch)) => Operation::Read(batch.keys.len()),
            Some(Request::Put(_) | Request::Delete(_)) => Operation::Mutation(1),
            Some(Request::BatchPut(batch)) => Operation::Mutation(batch.puts.len()),
            Some(Request::Query(_)) => Operation::Query,
            Some(Request::Inspect(_)) => Operation::Inspect,
            Some(Request::Scan(_)) => Operation::Scan,
            None => Operation::Unsupported,
        };
        Self {
            id: request.id,
            operation,
            columns: None,
            rows: 100_000,
            aggregate: false,
        }
    }

    /// Refine a query using its validated pending plan. Invalid refinements
    /// fail instead of becoming a less restrictive expectation.
    pub fn with_query_shape(
        mut self,
        columns: usize,
        limit: Option<u64>,
        aggregate: bool,
    ) -> Result<Self, &'static str> {
        if self.operation != Operation::Query
            || !(1..=32).contains(&columns)
            || limit.is_some_and(|limit| limit > 100_000)
        {
            return Err("invalid expected query shape");
        }
        self.columns = Some(columns);
        self.rows = if aggregate {
            1
        } else {
            limit.unwrap_or(100_000) as usize
        };
        self.aggregate = aggregate;
        Ok(self)
    }
}

/// Validate before message/collection materialization using the default budget.
/// The accounting/shape pass allocates no owned data. Malformed typed receipt
/// validation can allocate a bounded error string before returning rejection.
pub fn preflight_client_frame(bytes: &[u8]) -> Result<(), &'static str> {
    preflight_client_frame_with_budget(bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES).map(|_| ())
}

/// As [`preflight_client_frame`], with an explicit accounted allowance and report.
/// Successful preflight does not materialize messages, collections or payloads.
pub fn preflight_client_frame_with_budget(
    bytes: &[u8],
    max_accounted_bytes: usize,
) -> Result<ClientDecodeReport, &'static str> {
    preflight(bytes, max_accounted_bytes, None, None)
}

/// Exact message expected by the current application handshake/exchange phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientFrameKind {
    Hello,
    Auth,
    Request,
    Response,
}

pub fn preflight_client_frame_kind(
    bytes: &[u8],
    max_accounted_bytes: usize,
    kind: ClientFrameKind,
) -> Result<ClientDecodeReport, &'static str> {
    preflight(bytes, max_accounted_bytes, None, Some(kind))
}

/// Additionally validate the actual pending request before materialization.
/// The same bounded malformed-receipt error-metadata caveat applies.
pub fn preflight_client_response(
    bytes: &[u8],
    max_accounted_bytes: usize,
    shape: &ClientResponseShape,
) -> Result<ClientDecodeReport, &'static str> {
    preflight(
        bytes,
        max_accounted_bytes,
        Some(shape),
        Some(ClientFrameKind::Response),
    )
}

fn preflight(
    bytes: &[u8],
    maximum: usize,
    shape: Option<&ClientResponseShape>,
    kind: Option<ClientFrameKind>,
) -> Result<ClientDecodeReport, &'static str> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
        return Err("invalid application frame size");
    }
    if bytes.len() > maximum {
        return Err("decoded application response exceeds admission allowance");
    }
    if let Some(expected) = kind {
        if envelope_kind(bytes)? != expected {
            return Err("unexpected application message kind");
        }
    }
    let mut budget = Budget {
        maximum,
        report: ClientDecodeReport {
            accounted_bytes: bytes.len(),
            raw_bytes: bytes.len(),
            header_bytes: 0,
            payload_bytes: 0,
            query_cells: 0,
        },
        columns: 0,
        rows: 0,
        wrong_row_width: false,
    };
    budget.charge(0, false)?;
    walk(bytes, Context::Frame, &mut budget, shape)?;
    // Preserve every original envelope/oneof/receipt structural constraint.
    // Admission runs first so dense rejected frames need not traverse every
    // repeated nested message before the aggregate budget can stop the work.
    crate::validate_client_frame_structure(bytes)?;
    Ok(budget.report)
}

struct Budget {
    maximum: usize,
    report: ClientDecodeReport,
    columns: usize,
    rows: usize,
    wrong_row_width: bool,
}
impl Budget {
    fn charge(&mut self, bytes: usize, payload: bool) -> Result<(), &'static str> {
        self.report.accounted_bytes = self
            .report
            .accounted_bytes
            .checked_add(bytes)
            .ok_or("decoded application accounting overflow")?;
        if self.report.accounted_bytes > self.maximum {
            return Err("decoded application response exceeds admission allowance");
        }
        let counter = if payload {
            &mut self.report.payload_bytes
        } else {
            &mut self.report.header_bytes
        };
        *counter = counter
            .checked_add(bytes)
            .ok_or("decoded application accounting overflow")?;
        Ok(())
    }
    fn payload(&mut self, bytes: usize) -> Result<(), &'static str> {
        self.charge(
            bytes
                .max(if bytes == 0 { 0 } else { 8 })
                .checked_mul(2)
                .ok_or("decoded payload accounting overflow")?,
            true,
        )
    }
    fn repeated(&mut self, count: usize, element: usize) -> Result<(), &'static str> {
        // Current Vec push growth: a non-byte collection starts at four slots,
        // then doubles. Charge its capacity, not only its populated elements.
        let capacity = count
            .checked_next_power_of_two()
            .ok_or("decoded collection overflow")?
            .max(4);
        let previous = if count == 1 {
            0
        } else {
            (count - 1)
                .checked_next_power_of_two()
                .ok_or("decoded collection overflow")?
                .max(4)
        };
        self.charge(
            (capacity - previous)
                .checked_mul(element)
                .ok_or("decoded collection overflow")?,
            false,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Frame,
    Hello,
    Auth,
    Request,
    Get,
    Put,
    Delete,
    BatchGet,
    BatchPut,
    Query,
    Inspect,
    Scan,
    Response,
    Error,
    Value,
    Read,
    Mutation,
    QueryResult,
    Row,
    Cell,
    Inspection,
    Health,
    Placement,
    MapEntry,
    ScanMatch,
    Receipt,
    Evidence,
    OperationEvidence,
    Acknowledgement,
}
impl Context {
    fn size(self) -> usize {
        use Context::*;
        match self {
            Frame => size_of::<client::Frame>(),
            Hello => size_of::<client::Hello>(),
            Auth => size_of::<client::Auth>(),
            Request => size_of::<client::Request>(),
            Get => size_of::<client::Get>(),
            Put => size_of::<client::Put>(),
            Delete => size_of::<client::Delete>(),
            BatchGet => size_of::<client::BatchGet>(),
            BatchPut => size_of::<client::BatchPut>(),
            Query => size_of::<client::Query>(),
            Inspect => size_of::<client::Inspect>(),
            Scan => size_of::<client::Scan>(),
            Response => size_of::<client::Response>(),
            Error => size_of::<client::Error>(),
            Value => size_of::<client::Value>(),
            Read => size_of::<client::ReadResult>(),
            Mutation => size_of::<client::MutationResult>(),
            QueryResult => size_of::<client::QueryResult>(),
            Row => size_of::<meta::QueryPageRow>(),
            Cell => size_of::<meta::QueryCell>(),
            Inspection => size_of::<client::Inspection>(),
            Health => size_of::<client::FragmentHealth>(),
            Placement => size_of::<client::Placement>(),
            MapEntry => size_of::<(String, u64)>(),
            ScanMatch => size_of::<meta::ScanMatch>(),
            Receipt => size_of::<meta::WriteConcernResult>(),
            Evidence => size_of::<meta::WriteConcernEvidence>(),
            OperationEvidence => size_of::<meta::WriteOperationEvidence>(),
            Acknowledgement => size_of::<meta::WriteAcknowledgement>(),
        }
    }
}

#[derive(Clone, Copy)]
enum Field {
    Scalar,
    Text,
    Bytes,
    Message(Context),
    Texts {
        limit: usize,
        extra: usize,
    },
    Messages {
        context: Context,
        limit: usize,
        extra: usize,
    },
    Map {
        limit: usize,
    },
    ReceiptBytes,
}
fn schema(context: Context, field: u64) -> Option<Field> {
    use Context::*;
    use Field::{Bytes, Map, Message, Messages, ReceiptBytes, Scalar, Text, Texts};
    let strings = |limit| Texts { limit, extra: 0 };
    let messages = |context, limit| Messages {
        context,
        limit,
        extra: 0,
    };
    Some(match (context, field) {
        (Frame, 1) => Message(Hello),
        (Frame, 2) => Message(Auth),
        (Frame, 3) => Message(Request),
        (Frame, 4) => Message(Response),
        (Hello, 1) => Text,
        (Hello, 2) | (Auth, 1) => Bytes,
        (Hello, 3) => Scalar,
        (Request, 1) => Scalar,
        (Request, 10) => Text,
        (Request, 2) => Message(Get),
        (Request, 3) => Message(Put),
        (Request, 4) => Message(Delete),
        (Request, 5) => Message(BatchGet),
        (Request, 6) => Message(BatchPut),
        (Request, 7) => Message(Query),
        (Request, 8) => Message(Inspect),
        (Request, 9) => Message(Scan),
        (Get, 1) | (Put, 1) | (Delete, 1) => Text,
        (Get, 2) | (Put, 3 | 4) | (Delete, 2) => Scalar,
        (Put, 2 | 5) | (Delete, 3) => Bytes,
        (BatchGet, 1) => strings(MAX_BATCH_ITEMS),
        (BatchGet | BatchPut, 2) => Scalar,
        (BatchPut, 1) => messages(Put, MAX_BATCH_ITEMS),
        (Query, 1) => Text,
        (Query, 2) => Messages {
            context: Cell,
            limit: MAX_BATCH_ITEMS,
            extra: PORTABLE_CELL_HEADER_CHARGE,
        },
        (Scan, 1 | 3 | 4 | 5 | 6) => Scalar,
        (Scan, 2) => Bytes,
        (Response, 1) => Scalar,
        (Response, 2) => Message(Error),
        (Response, 3) => messages(Read, MAX_BATCH_ITEMS),
        (Response, 4) => messages(Mutation, MAX_BATCH_ITEMS),
        (Response, 5) => Message(QueryResult),
        (Response, 6) => Message(Inspection),
        (Response, 7) => messages(ScanMatch, 100_000),
        (Error, 1 | 3 | 5 | 6) => Scalar,
        (Error, 2 | 4) => Text,
        (Value, 1) => Bytes,
        (Value, 2 | 4 | 5) => Scalar,
        (Value, 3) => Text,
        (Read, 1) | (Mutation, 1) => Text,
        (Read, 2) => Message(Value),
        (Read, 3) | (Mutation, 2) => Message(Error),
        (Mutation, 3) => ReceiptBytes,
        (QueryResult, 1) => strings(32),
        (QueryResult, 2) => Messages {
            context: Row,
            limit: 100_000,
            extra: size_of::<Vec<()>>(),
        },
        (Row, 1) => Text,
        (Row, 2) => Messages {
            context: Cell,
            limit: 32,
            extra: PORTABLE_CELL_HEADER_CHARGE,
        },
        (Cell, 1) => Scalar,
        (Cell, 2 | 4) => Text,
        (Cell, 3) => Bytes,
        (Inspection, 1 | 2) => Text,
        (Inspection, 3 | 6) => Scalar,
        (Inspection, 4) => messages(Health, 100_000),
        (Inspection, 5) => messages(Placement, 100_000),
        (Health, 1 | 2 | 6 | 7 | 8) => Scalar,
        (Health, 3) => strings(MAX_BATCH_ITEMS),
        (Health, 4) => Map {
            limit: MAX_BATCH_ITEMS,
        },
        (Placement, 1) => Scalar,
        (Placement, 2) => strings(MAX_BATCH_ITEMS),
        (Placement, 3) => Text,
        (MapEntry, 1) => Text,
        (MapEntry, 2) => Scalar,
        (ScanMatch, 1) => Text,
        (ScanMatch, 2) => Bytes,
        (Receipt, 1 | 3 | 4 | 5 | 11) => Scalar,
        (Receipt, 2 | 7 | 8) => Text,
        (Receipt, 6 | 9) => Texts {
            limit: write_concern::MAX_WRITE_CONCERN_REPLICAS,
            extra: VALIDATION_SLOT_CHARGE,
        },
        (Receipt, 10) => Message(Evidence),
        (Evidence, 1) => Message(OperationEvidence),
        (Evidence, 3) => Bytes,
        (Evidence, 2) => Texts {
            limit: write_concern::MAX_WRITE_CONCERN_REPLICAS,
            extra: VALIDATION_SLOT_CHARGE,
        },
        (Evidence, 4) => Messages {
            context: Acknowledgement,
            limit: write_concern::MAX_WRITE_CONCERN_REPLICAS,
            extra: size_of::<write_concern::WriteAcknowledgement>() + MAX_WRITER_BYTES,
        },
        (OperationEvidence, 1 | 4 | 5 | 7 | 8) | (Acknowledgement, 1 | 2) => Scalar,
        (OperationEvidence, 2 | 3 | 9) => Bytes,
        (OperationEvidence, 6) => Text,
        _ => return None,
    })
}

fn varint(bytes: &[u8], position: &mut usize) -> Result<u64, &'static str> {
    let mut result = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*position).ok_or("truncated varint")?;
        *position += 1;
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

fn skip(bytes: &[u8], position: &mut usize, wire: u64) -> Result<(), &'static str> {
    let length = match wire {
        0 => {
            varint(bytes, position)?;
            return Ok(());
        }
        1 => 8,
        5 => 4,
        2 => usize::try_from(varint(bytes, position)?).map_err(|_| "overflow")?,
        _ => return Err("invalid wire type"),
    };
    *position = position.checked_add(length).ok_or("overflow")?;
    if *position > bytes.len() {
        return Err("truncated field");
    }
    Ok(())
}
fn envelope_kind(bytes: &[u8]) -> Result<ClientFrameKind, &'static str> {
    let mut position = 0;
    let mut kind = None;
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        let next = match tag >> 3 {
            1 => Some(ClientFrameKind::Hello),
            2 => Some(ClientFrameKind::Auth),
            3 => Some(ClientFrameKind::Request),
            4 => Some(ClientFrameKind::Response),
            0 => return Err("invalid field"),
            _ => None,
        };
        if let Some(next) = next {
            if kind.replace(next).is_some() || tag & 7 != 2 {
                return Err("duplicate or invalid message");
            }
        }
        skip(bytes, &mut position, tag & 7)?;
    }
    kind.ok_or("missing message")
}
fn response_id(bytes: &[u8]) -> Result<u64, &'static str> {
    let mut position = 0;
    let mut id = 0;
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        if tag >> 3 == 1 {
            if tag & 7 != 0 {
                return Err("invalid response id wire type");
            }
            id = varint(bytes, &mut position)?;
        } else {
            skip(bytes, &mut position, tag & 7)?;
        }
    }
    Ok(id)
}

fn walk(
    bytes: &[u8],
    context: Context,
    budget: &mut Budget,
    shape: Option<&ClientResponseShape>,
) -> Result<(), &'static str> {
    if context == Context::Response {
        if let Some(shape) = shape {
            if response_id(bytes)? != shape.id {
                return Err("response id does not match pending request");
            }
        }
    }
    budget.charge(context.size(), false)?;
    let mut position = 0;
    let mut counts = [0usize; 12];
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        let field = tag >> 3;
        if field == 0 {
            return Err("invalid field");
        }
        let kind = schema(context, field);
        if let Some(kind) = kind {
            let expected_wire = if matches!(kind, Field::Scalar) { 0 } else { 2 };
            if tag & 7 != expected_wire {
                return Err("invalid known application field wire type");
            }
            let count = counts
                .get_mut(field as usize)
                .ok_or("invalid known field")?;
            *count = count.checked_add(1).ok_or("decoded collection overflow")?;
            let repeated = match kind {
                Field::Texts { limit, extra } => Some((limit, size_of::<String>() + extra)),
                Field::Messages {
                    context,
                    limit,
                    extra,
                } => Some((limit, context.size() + extra)),
                Field::Map { limit } => Some((limit, MAP_SLOT_CHARGE)),
                _ => None,
            };
            if let Some((limit, element)) = repeated {
                if *count > limit {
                    return Err("invalid or excessive repeated field");
                }
                budget.repeated(*count, element)?;
            }
            if context == Context::Row && field == 2 {
                budget.report.query_cells = budget
                    .report
                    .query_cells
                    .checked_add(1)
                    .ok_or("decoded cell overflow")?;
            }
        }
        match tag & 7 {
            0 => {
                varint(bytes, &mut position)?;
            }
            1 => {
                position = position.checked_add(8).ok_or("overflow")?;
            }
            5 => {
                position = position.checked_add(4).ok_or("overflow")?;
            }
            2 => {
                let length =
                    usize::try_from(varint(bytes, &mut position)?).map_err(|_| "overflow")?;
                let end = position.checked_add(length).ok_or("overflow")?;
                let inner = bytes.get(position..end).ok_or("truncated field")?;
                match kind {
                    Some(Field::Text | Field::Texts { .. }) => {
                        std::str::from_utf8(inner).map_err(|_| "invalid application UTF-8")?;
                        budget.payload(inner.len())?;
                    }
                    Some(Field::Bytes) => budget.payload(inner.len())?,
                    Some(
                        Field::Message(nested)
                        | Field::Messages {
                            context: nested, ..
                        },
                    ) => {
                        walk(inner, nested, budget, shape)?;
                    }
                    Some(Field::Map { .. }) => walk(inner, Context::MapEntry, budget, shape)?,
                    Some(Field::ReceiptBytes) => {
                        budget.payload(inner.len())?;
                        // Opaque application receipt bytes are subsequently decoded
                        // by the SDK. Admit that nested materialization as well.
                        walk(inner, Context::Receipt, budget, shape)?;
                    }
                    _ => {}
                }
                position = end;
            }
            _ => return Err("invalid wire type"),
        }
        if position > bytes.len() {
            return Err("truncated field");
        }
    }
    if context == Context::Row {
        budget.wrong_row_width |= shape
            .and_then(|shape| shape.columns)
            .is_some_and(|columns| counts[2] != columns);
    }
    if context == Context::QueryResult {
        budget.columns = counts[1];
        budget.rows = counts[2];
    }
    if context == Context::Frame && shape.is_some() && counts[4] != 1 {
        return Err("expected pending application response");
    }
    if context == Context::Response && counts[2] == 0 {
        if let Some(shape) = shape {
            let (reads, mutations, query, inspection, scans) = match shape.operation {
                Operation::Read(reads) => (reads, 0, 0, 0, false),
                Operation::Mutation(mutations) => (0, mutations, 0, 0, false),
                Operation::Query => (0, 0, 1, 0, false),
                Operation::Inspect => (0, 0, 0, 1, false),
                Operation::Scan => (0, 0, 0, 0, true),
                Operation::Unsupported => (0, 0, 0, 0, false),
            };
            if counts[3] != reads
                || counts[4] != mutations
                || counts[5] != query
                || counts[6] != inspection
                || (!scans && counts[7] != 0)
                || (query == 1
                    && (shape
                        .columns
                        .is_some_and(|columns| columns != budget.columns)
                        || budget.rows > shape.rows
                        || (shape.aggregate && budget.rows != 1)
                        || budget.wrong_row_width))
            {
                return Err("decoded response does not match pending request shape");
            }
        }
    }
    Ok(())
}
