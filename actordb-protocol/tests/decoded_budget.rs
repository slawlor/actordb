//! Raw framing and aggregate decoded admission are intentionally different.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::size_of;
use std::time::Instant;

use actordb_protocol::{
    client as wire, meta, preflight_client_frame, preflight_client_frame_kind,
    preflight_client_frame_with_budget, preflight_client_response, ClientFrameKind,
    ClientResponseShape, DEFAULT_MAX_DECODED_RESPONSE_BYTES, MAX_FRAME_BYTES,
};
use prost::Message;

#[derive(Clone, Copy, Default)]
struct Allocations {
    calls: usize,
    live: usize,
    peak: usize,
}
thread_local! { static TRACKING: Cell<Option<Allocations>> = const { Cell::new(None) }; }
struct Allocator;
fn track(update: impl FnOnce(&mut Allocations)) {
    let _ = TRACKING.try_with(|tracking| {
        if let Some(mut counts) = tracking.get() {
            update(&mut counts);
            tracking.set(Some(counts));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            track(|counts| {
                counts.calls += 1;
                counts.live += layout.size();
                counts.peak = counts.peak.max(counts.live);
            });
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        track(|counts| counts.live = counts.live.saturating_sub(layout.size()));
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, new_size) };
        if !result.is_null() {
            track(|counts| {
                counts.calls += 1;
                counts.live = counts.live.saturating_sub(layout.size()) + new_size;
                counts.peak = counts.peak.max(counts.live);
            });
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
struct AllocationScope(Option<Allocations>);
impl AllocationScope {
    fn start() -> Self {
        Self(TRACKING.with(|tracking| tracking.replace(Some(Allocations::default()))))
    }
    fn finish(mut self) -> Allocations {
        TRACKING.with(|tracking| {
            let counts = tracking.get().unwrap();
            tracking.set(self.0.take());
            counts
        })
    }
}
impl Drop for AllocationScope {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(self.0.take()));
    }
}
fn field(out: &mut Vec<u8>, tag: u64, inner: &[u8]) {
    prost::encoding::encode_varint(tag << 3 | 2, out);
    prost::encoding::encode_varint(inner.len() as u64, out);
    out.extend_from_slice(inner);
}
fn response(inner: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    field(&mut out, 4, inner);
    out
}
fn query(rows: usize, columns: usize, null: bool) -> Vec<u8> {
    let mut row = Vec::new();
    for _ in 0..columns {
        field(&mut row, 2, if null { &[8, 1] } else { &[34, 1, b'0'] });
    }
    let mut result = Vec::new();
    for _ in 0..columns {
        field(&mut result, 1, b"column");
    }
    for _ in 0..rows {
        field(&mut result, 2, &row);
    }
    let mut body = vec![8, 1]; // Actual response id.
    field(&mut body, 5, &result);
    response(&body)
}
fn query_shape(columns: usize, limit: Option<u64>) -> ClientResponseShape {
    ClientResponseShape::from_request(&wire::Request {
        id: 1,
        operation: Some(wire::request::Operation::Query(wire::Query::default())),
        ..Default::default()
    })
    .with_query_shape(columns, limit, false)
    .unwrap()
}

#[test]
fn tiny_cells_are_rejected_without_materialization_and_explicit_override_remains_available() {
    let bytes = query(100_000, 32, true);
    assert!(bytes.len() < MAX_FRAME_BYTES);
    let scope = AllocationScope::start();
    let started = Instant::now();
    let rejected = preflight_client_frame(&bytes);
    let elapsed = started.elapsed();
    let allocation = scope.finish();
    assert!(rejected.is_err());
    assert_eq!(
        allocation.calls, 0,
        "budget rejection must precede all collection materialization"
    );
    let scope = AllocationScope::start();
    let report = preflight_client_frame_with_budget(&bytes, 512 * 1024 * 1024).unwrap();
    let admission_allocation = scope.finish();
    assert_eq!(admission_allocation.calls, 0);
    assert_eq!(report.query_cells, 3_200_000);
    assert!(report.accounted_bytes > DEFAULT_MAX_DECODED_RESPONSE_BYTES);
    let scope = AllocationScope::start();
    let decoded = wire::Frame::decode(bytes.as_slice()).unwrap();
    let measured = scope.finish();
    let Some(wire::frame::Message::Response(decoded)) = decoded.message else {
        panic!("response")
    };
    let result = decoded.query.unwrap();
    let owned_headers = result.rows.capacity() * size_of::<meta::QueryPageRow>()
        + result
            .rows
            .iter()
            .map(|row| row.cells.capacity() * size_of::<meta::QueryCell>())
            .sum::<usize>();
    assert!(owned_headers > 100 * 1024 * 1024);
    assert!(report.accounted_bytes >= owned_headers + bytes.len());
    assert!(measured.peak >= owned_headers);
    eprintln!("tiny-cell probe: raw={}, cells={}, decoded_vec_headers={}, accounted={}, Rust-allocation-request peak={} (not RSS), rejected preflight={elapsed:?}", bytes.len(), report.query_cells, owned_headers, report.accounted_bytes, measured.peak);
}

#[test]
fn maximum_narrow_rows_and_supported_wide_shapes_have_explicit_admission() {
    for (rows, columns) in [(100_000, 1), (10_000, 32)] {
        let bytes = query(rows, columns, false);
        let shape = query_shape(columns, Some(rows as u64));
        let report =
            preflight_client_response(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &shape).unwrap();
        assert!(report.accounted_bytes <= DEFAULT_MAX_DECODED_RESPONSE_BYTES);
        assert_eq!(report.query_cells, rows * columns);
    }
    let widest = query(100_000, 32, false);
    assert!(widest.len() <= MAX_FRAME_BYTES);
    let shape = query_shape(32, Some(100_000));
    assert!(
        preflight_client_response(&widest, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &shape).is_err()
    );
    assert!(preflight_client_response(&widest, 512 * 1024 * 1024, &shape).is_ok());
}

#[test]
fn actual_pending_request_shape_id_and_control_kind_are_checked_before_decode() {
    let bytes = query(2, 2, true);
    let read = ClientResponseShape::from_request(&wire::Request {
        id: 1,
        operation: Some(wire::request::Operation::Get(wire::Get::default())),
        ..Default::default()
    });
    assert!(preflight_client_response(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &read).is_err());
    assert!(preflight_client_response(
        &bytes,
        DEFAULT_MAX_DECODED_RESPONSE_BYTES,
        &query_shape(1, None)
    )
    .is_err());
    assert!(preflight_client_response(
        &bytes,
        DEFAULT_MAX_DECODED_RESPONSE_BYTES,
        &query_shape(2, Some(1))
    )
    .is_err());
    let wrong_id = ClientResponseShape::from_request(&wire::Request {
        id: 2,
        operation: Some(wire::request::Operation::Query(wire::Query::default())),
        ..Default::default()
    });
    assert!(
        preflight_client_response(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &wrong_id).is_err()
    );
    let scope = AllocationScope::start();
    assert!(
        preflight_client_frame_kind(&bytes, 512 * 1024 * 1024, ClientFrameKind::Hello).is_err()
    );
    assert!(preflight_client_frame_kind(&bytes, 512 * 1024 * 1024, ClientFrameKind::Auth).is_err());
    assert_eq!(scope.finish().calls, 0);
    for (message, kind) in [
        (
            wire::frame::Message::Hello(wire::Hello {
                identity: "peer".into(),
                challenge: vec![1; 32],
                protocol_version: 4,
            }),
            ClientFrameKind::Hello,
        ),
        (
            wire::frame::Message::Auth(wire::Auth { proof: vec![1; 32] }),
            ClientFrameKind::Auth,
        ),
    ] {
        assert!(preflight_client_frame_kind(
            &wire::Frame {
                message: Some(message)
            }
            .encode_to_vec(),
            4096,
            kind
        )
        .is_ok());
    }
}

#[test]
fn inspection_headers_maps_and_strings_share_one_aggregate_allowance() {
    let mut health = Vec::new();
    for _ in 0..32 {
        field(&mut health, 3, b"");
    }
    let mut inspection = Vec::new();
    for _ in 0..100_000 {
        field(&mut inspection, 4, &health);
    }
    let mut body = vec![8, 1];
    field(&mut body, 6, &inspection);
    let bytes = response(&body);
    assert!(bytes.len() < MAX_FRAME_BYTES);
    assert!(preflight_client_frame(&bytes).is_err());
    assert!(preflight_client_frame_with_budget(&bytes, 512 * 1024 * 1024).is_ok());
    let mut health = Vec::new();
    for _ in 0..4096 {
        field(&mut health, 4, b"");
    }
    let mut inspection = Vec::new();
    for _ in 0..128 {
        field(&mut inspection, 4, &health);
    }
    let mut body = vec![8, 1];
    field(&mut body, 6, &inspection);
    assert!(preflight_client_frame(&response(&body)).is_err());
    let mut invalid_entry = Vec::new();
    field(&mut invalid_entry, 1, &[255]);
    let mut health = Vec::new();
    field(&mut health, 4, &invalid_entry);
    let mut inspection = Vec::new();
    field(&mut inspection, 4, &health);
    let mut body = vec![8, 1];
    field(&mut body, 6, &inspection);
    assert!(
        preflight_client_frame(&response(&body)).is_err(),
        "nested map UTF-8 must be checked before map allocation"
    );
}

#[test]
fn nested_receipts_and_evidence_are_charged_even_in_otherwise_ignored_server_errors() {
    let mut evidence = Vec::new();
    for _ in 0..4096 {
        field(&mut evidence, 4, b"");
    }
    let mut receipt = Vec::new();
    field(&mut receipt, 10, &evidence);
    let mut mutation = Vec::new();
    field(&mut mutation, 3, &receipt);
    let mut body = vec![8, 1];
    for _ in 0..128 {
        field(&mut body, 4, &mutation);
    }
    field(&mut body, 2, b""); // Error does not waive decoded admission.
    let bytes = response(&body);
    assert!(bytes.len() < MAX_FRAME_BYTES);
    assert!(preflight_client_frame(&bytes).is_err());
    assert!(preflight_client_frame_with_budget(&bytes, 512 * 1024 * 1024).is_ok());
    let request = ClientResponseShape::from_request(&wire::Request {
        id: 1,
        operation: Some(wire::request::Operation::Get(wire::Get::default())),
        ..Default::default()
    });
    assert!(
        preflight_client_response(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &request).is_err()
    );
}

#[test]
fn all_known_nested_payloads_and_raw_bytes_are_accounted_and_malformed_shapes_fail() {
    let bytes = wire::Frame {
        message: Some(wire::frame::Message::Response(wire::Response {
            id: 1,
            reads: vec![wire::ReadResult {
                key: "key".into(),
                value: Some(wire::Value {
                    bytes: vec![1; 1_000_000],
                    node_id: "writer".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        })),
    }
    .encode_to_vec();
    let report =
        preflight_client_frame_with_budget(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES).unwrap();
    assert_eq!(report.raw_bytes, bytes.len());
    assert!(report.payload_bytes >= 2_000_000);
    assert_eq!(
        report.accounted_bytes,
        report.raw_bytes + report.header_bytes + report.payload_bytes
    );
    assert!(preflight_client_frame_with_budget(&bytes, report.accounted_bytes - 1).is_err());
    assert!(preflight_client_frame_with_budget(&bytes, report.accounted_bytes).is_ok());
    for malformed in [
        vec![34, 2, 42, 255],
        vec![34, 2, 42, 0, 34, 0],
        vec![34, 2, 29, 0],
        vec![34, 1, 8],
    ] {
        assert!(preflight_client_frame(&malformed).is_err());
    }
}
