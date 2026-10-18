use actordb_protocol::{client as wire, *};
use prost::Message;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn hello(identity: &str, byte: u8) -> wire::Hello {
    wire::Hello {
        identity: identity.into(),
        challenge: vec![byte; 32],
        min_version: 1,
        max_version: 1,
        capabilities: 3,
    }
}
#[test]
fn application_auth_golden_vectors_bind_roles_identity_version_and_challenges() {
    let client = hello("c", 1);
    let server = hello("s", 2);
    let secret = vec![42; 32];
    let proof = client_auth_proof(&secret, true, &client, &server);
    assert_eq!(
        hex(&proof),
        "9366c3660b9ae79b4370fa7d2eb7483afa6c5e6417f61ef837f95b516b5bcc3f"
    );
    assert_eq!(
        hex(&client_auth_proof(&secret, false, &client, &server)),
        "ae8a58f928ad17db176af16d217c47f59138e57fde760480c778f61ddcb46075"
    );
    assert!(verify_client_auth(
        Some(&secret),
        true,
        &client,
        &server,
        &proof
    ));
    assert!(!verify_client_auth(
        Some(&secret),
        false,
        &client,
        &server,
        &proof
    ));
    assert!(!verify_client_auth(
        Some(&secret),
        true,
        &client,
        &hello("s", 3),
        &proof
    ));
    assert!(!verify_client_auth(
        Some(&secret),
        true,
        &hello("other", 1),
        &server,
        &proof
    ));
    assert!(!verify_client_auth(
        Some(&secret),
        true,
        &client,
        &server,
        &proof[..31]
    ));
}
#[test]
fn application_get_golden_frame_and_unknown_fields() {
    let frame = wire::Frame {
        message: Some(wire::frame::Message::Request(wire::Request {
            id: 1,
            operation: Some(wire::request::Operation::Get(wire::Get {
                key: "x".into(),
                priority: 0,
            })),
        })),
    };
    let mut bytes = frame.encode_to_vec();
    assert_eq!(hex(&bytes), "1a07080112030a0178");
    assert!(preflight_client_frame(&bytes).is_ok());
    bytes.extend_from_slice(&[0xa0, 0x06, 1]);
    assert!(preflight_client_frame(&bytes).is_ok());
    assert_eq!(wire::Frame::decode(bytes.as_slice()).unwrap(), frame);
}
#[test]
fn preflight_rejects_repeated_amplification_and_duplicate_structured_messages() {
    let request = wire::Frame {
        message: Some(wire::frame::Message::Request(wire::Request {
            id: 1,
            operation: Some(wire::request::Operation::BatchGet(wire::BatchGet {
                keys: vec![String::new(); MAX_BATCH_ITEMS + 1],
                priority: 0,
            })),
        })),
    };
    assert!(preflight_client_frame(&request.encode_to_vec()).is_err());
    // Frame.response { query {}, query {} }. Prost would merge them.
    assert!(preflight_client_frame(&[0x22, 4, 0x2a, 0, 0x2a, 0]).is_err());
    let inspection = wire::Frame {
        message: Some(wire::frame::Message::Response(wire::Response {
            id: 1,
            inspection: Some(wire::Inspection {
                node_id: "n".into(),
                peer_addr: "127.0.0.1:123".into(),
                ..Default::default()
            }),
            ..Default::default()
        })),
    };
    assert!(preflight_client_frame(&inspection.encode_to_vec()).is_ok());
}

#[test]
fn preflight_accepts_query_and_scan_results_larger_than_request_batches() {
    // Results have a different catalog limit from 4096-item mutation batches.
    let response = wire::Frame {
        message: Some(wire::frame::Message::Response(wire::Response {
            id: 1,
            query: Some(wire::QueryResult {
                columns: vec!["key".into()],
                rows: (0..MAX_BATCH_ITEMS + 1)
                    .map(|index| meta::QueryPageRow {
                        key: String::new(),
                        cells: vec![meta::QueryCell {
                            value: Some(meta::query_cell::Value::Text(index.to_string())),
                        }],
                    })
                    .collect(),
            }),
            ..Default::default()
        })),
    };
    assert!(preflight_client_frame(&response.encode_to_vec()).is_ok());
    let response = wire::Frame {
        message: Some(wire::frame::Message::Response(wire::Response {
            id: 2,
            scans: (0..10_000)
                .map(|index| meta::ScanMatch {
                    key: format!("key:{index}"),
                    value: Vec::new(),
                })
                .collect(),
            ..Default::default()
        })),
    };
    assert!(preflight_client_frame(&response.encode_to_vec()).is_ok());
}

#[test]
fn inspection_nested_collections_are_checked_without_parsing_strings_as_messages() {
    let health = wire::FragmentHealth {
        worker_names: vec!["fragment-0-high-0".into()],
        restarts: std::collections::HashMap::from([("fragment-0".into(), 1)]),
        ..Default::default()
    };
    let mut response = wire::Response {
        id: 1,
        inspection: Some(wire::Inspection {
            node_id: "node-a".into(),
            peer_addr: "127.0.0.1:7447".into(),
            health: vec![health],
            placement: vec![wire::Placement {
                fragment: 0,
                replica_ids: vec!["node-a".into(), "node-b".into()],
                state: "Serving".into(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let encode = |response| {
        wire::Frame {
            message: Some(wire::frame::Message::Response(response)),
        }
        .encode_to_vec()
    };
    assert!(preflight_client_frame(&encode(response.clone())).is_ok());
    response.inspection.as_mut().unwrap().health[0].worker_names =
        vec![String::new(); MAX_BATCH_ITEMS + 1];
    assert!(preflight_client_frame(&encode(response)).is_err());
}
