use actordb_protocol::{client as wire, *};
use prost::Message;

#[test]
fn unary_request_golden_contract_has_no_envelope_handshake_or_id() {
    let request = wire::Request {
        table: "KV".into(),
        operation: Some(wire::request::Operation::Get(wire::Get {
            key: "x".into(),
            priority: 0,
        })),
    };
    let mut bytes = request.encode_to_vec();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        "0a024b5612030a0178"
    );
    preflight_rpc_request(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES).unwrap();
    bytes.extend_from_slice(&[0xa0, 0x06, 1]);
    preflight_rpc_request(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES).unwrap();
    assert_eq!(wire::Request::decode(bytes.as_slice()).unwrap(), request);
    assert_eq!(RPC_CALL_PATH, "/actordb.rpc.v1.ActorDb/Call");
}

#[test]
fn preflight_rejects_duplicate_operations_structures_and_amplification() {
    let request = wire::Request {
        table: "KV".into(),
        operation: Some(wire::request::Operation::BatchGet(wire::BatchGet {
            keys: vec![String::new(); MAX_BATCH_ITEMS + 1],
            priority: 0,
        })),
    };
    assert!(
        preflight_rpc_request(&request.encode_to_vec(), DEFAULT_MAX_DECODED_RESPONSE_BYTES)
            .is_err()
    );
    assert!(preflight_rpc_request(&[10, 2, b'K', b'V', 18, 0, 18, 0], 4096).is_err());
    assert!(preflight_rpc_response_unchecked_shape(&[34, 0, 34, 0], 4096).is_err());
    assert!(preflight_rpc_request(&[], 4096).is_err());
}

#[test]
fn empty_batch_response_is_valid_only_for_the_actual_empty_batch() {
    let empty = RpcResponseShape::from_request(&wire::Request {
        table: "KV".into(),
        operation: Some(wire::request::Operation::BatchGet(wire::BatchGet::default())),
    });
    assert!(preflight_rpc_response(&[], 4096, &empty).is_ok());
    let get = RpcResponseShape::from_request(&wire::Request {
        table: "KV".into(),
        operation: Some(wire::request::Operation::Get(wire::Get::default())),
    });
    assert!(preflight_rpc_response(&[], 4096, &get).is_err());
}

#[test]
fn query_and_scan_results_have_independent_catalog_limits() {
    let query = wire::Response {
        query: Some(wire::QueryResult {
            columns: vec!["key".into()],
            rows: (0..MAX_BATCH_ITEMS + 1)
                .map(|i| wire::QueryRow {
                    cells: vec![meta::QueryCell {
                        value: Some(meta::query_cell::Value::Text(i.to_string())),
                    }],
                })
                .collect(),
        }),
        ..Default::default()
    };
    assert!(preflight_rpc_response_unchecked_shape(
        &query.encode_to_vec(),
        DEFAULT_MAX_DECODED_RESPONSE_BYTES
    )
    .is_ok());
    let scan = wire::Response {
        scans: (0..10_000)
            .map(|i| meta::ScanMatch {
                key: format!("k{i}"),
                value: Vec::new(),
            })
            .collect(),
        ..Default::default()
    };
    assert!(preflight_rpc_response_unchecked_shape(
        &scan.encode_to_vec(),
        DEFAULT_MAX_DECODED_RESPONSE_BYTES
    )
    .is_ok());
}

#[test]
fn inspection_collections_are_bounded_without_treating_text_as_messages() {
    let mut response = wire::Response {
        inspection: Some(wire::Inspection {
            node_id: "node-a".into(),
            peer_addr: "127.0.0.1:7447".into(),
            health: vec![wire::FragmentHealth {
                worker_names: vec!["worker".into()],
                restarts: std::collections::HashMap::from([("worker".into(), 1)]),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(preflight_rpc_response_unchecked_shape(
        &response.encode_to_vec(),
        DEFAULT_MAX_DECODED_RESPONSE_BYTES
    )
    .is_ok());
    response.inspection.as_mut().unwrap().health[0].worker_names =
        vec![String::new(); MAX_BATCH_ITEMS + 1];
    assert!(preflight_rpc_response_unchecked_shape(
        &response.encode_to_vec(),
        DEFAULT_MAX_DECODED_RESPONSE_BYTES
    )
    .is_err());
}
