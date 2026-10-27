//! Decoded-object admission, separate from encoded framing.
//!
//! The budget, header/payload and pending-shape walk is allocation-free and
//! precedes all message/collection materialization. The existing typed receipt
//! validator can allocate bounded error metadata on malformed receipts; that
//! rejection does not materialize their decoded messages or payloads.

use std::mem::size_of;

use crate::{client, meta, write_concern, MAX_BATCH_ITEMS, MAX_RPC_MESSAGE_BYTES};

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
pub struct RpcDecodeReport {
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
pub struct RpcResponseShape {
    operation: Operation,
    columns: Option<usize>,
    rows: usize,
    aggregate: bool,
}
impl RpcResponseShape {
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

/// Allocation-free request preflight before an RPC adapter materializes Prost.
pub fn preflight_rpc_request(
    bytes: &[u8],
    maximum: usize,
) -> Result<RpcDecodeReport, &'static str> {
    preflight(bytes, maximum, Context::Request, None)
}

/// Allocation-free response admission, pinned to the actual pending request.
pub fn preflight_rpc_response(
    bytes: &[u8],
    maximum: usize,
    shape: &RpcResponseShape,
) -> Result<RpcDecodeReport, &'static str> {
    preflight(bytes, maximum, Context::Response, Some(shape))
}

/// Structural response admission for adapters which validate the pending shape separately.
pub fn preflight_rpc_response_unchecked_shape(
    bytes: &[u8],
    maximum: usize,
) -> Result<RpcDecodeReport, &'static str> {
    preflight(bytes, maximum, Context::Response, None)
}

fn preflight(
    bytes: &[u8],
    maximum: usize,
    context: Context,
    shape: Option<&RpcResponseShape>,
) -> Result<RpcDecodeReport, &'static str> {
    if bytes.len() > MAX_RPC_MESSAGE_BYTES || bytes.len() > maximum {
        return Err("RPC message exceeds admission allowance");
    }
    let mut budget = Budget {
        maximum,
        report: RpcDecodeReport {
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
    walk(bytes, context, &mut budget, shape)?;
    Ok(budget.report)
}

struct Budget {
    maximum: usize,
    report: RpcDecodeReport,
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
            Row => size_of::<client::QueryRow>(),
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
        (Request, 1) => Text,
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
        (Response, 1) => Message(Error),
        (Response, 2) => messages(Read, MAX_BATCH_ITEMS),
        (Response, 3) => messages(Mutation, MAX_BATCH_ITEMS),
        (Response, 4) => Message(QueryResult),
        (Response, 5) => Message(Inspection),
        (Response, 6) => messages(ScanMatch, 100_000),
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
        (Row, 1) => Messages {
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

fn walk(
    bytes: &[u8],
    context: Context,
    budget: &mut Budget,
    shape: Option<&RpcResponseShape>,
) -> Result<(), &'static str> {
    budget.charge(context.size(), false)?;
    let mut position = 0;
    let mut counts = [0usize; 12];
    let mut variants = 0usize;
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        let field = tag >> 3;
        if field == 0 {
            return Err("invalid field");
        }
        if (context == Context::Request && (2..=9).contains(&field))
            || (context == Context::Cell && (1..=4).contains(&field))
        {
            variants += 1;
            if variants > 1 {
                return Err("duplicate RPC oneof");
            }
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
            if repeated.is_none() && *count > 1 {
                return Err("duplicate RPC field");
            }
            if let Some((limit, element)) = repeated {
                if *count > limit {
                    return Err("invalid or excessive repeated field");
                }
                budget.repeated(*count, element)?;
            }
            if context == Context::Row && field == 1 {
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
                    Some(Field::Bytes) => {
                        if matches!((context, field), (Context::Put, 5) | (Context::Delete, 3))
                            && inner.len() > write_concern::MAX_WRITE_CONCERN_POLICY_BYTES
                        {
                            return Err("write concern policy exceeds byte limit");
                        }
                        budget.payload(inner.len())?;
                    }
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
                        write_concern::preflight_result(inner)
                            .map_err(|_| "invalid write concern receipt")?;
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
            .is_some_and(|columns| counts[1] != columns);
    }
    if context == Context::QueryResult {
        budget.columns = counts[1];
        budget.rows = counts[2];
    }
    if context == Context::Request && (variants != 1 || counts[1] != 1) {
        return Err("RPC request requires a table and exactly one operation");
    }
    if context == Context::Response && counts[1] == 0 {
        if let Some(shape) = shape {
            let (reads, mutations, query, inspection, scans) = match shape.operation {
                Operation::Read(reads) => (reads, 0, 0, 0, false),
                Operation::Mutation(mutations) => (0, mutations, 0, 0, false),
                Operation::Query => (0, 0, 1, 0, false),
                Operation::Inspect => (0, 0, 0, 1, false),
                Operation::Scan => (0, 0, 0, 0, true),
                Operation::Unsupported => (0, 0, 0, 0, false),
            };
            if counts[2] != reads
                || counts[3] != mutations
                || counts[4] != query
                || counts[5] != inspection
                || (!scans && counts[6] != 0)
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
