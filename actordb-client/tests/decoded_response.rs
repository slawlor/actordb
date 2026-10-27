#![cfg(feature = "grpc")]
#[path = "common/raw_grpc.rs"]
mod raw;
use actordb_client::{Client, ClientOptions, QueryValue};
use actordb_protocol::{
    client as wire, preflight_rpc_response, RpcResponseShape, DEFAULT_MAX_DECODED_RESPONSE_BYTES,
    MAX_RPC_MESSAGE_BYTES,
};
use prost::Message;

fn field(out: &mut Vec<u8>, tag: u64, inner: &[u8]) {
    prost::encoding::encode_varint(tag << 3 | 2, out);
    prost::encoding::encode_varint(inner.len() as u64, out);
    out.extend_from_slice(inner);
}
fn query_response(rows: usize, columns: &[String]) -> Vec<u8> {
    let mut row = Vec::new();
    for _ in columns {
        field(&mut row, 1, &[34, 1, b'0']);
    }
    let mut result = Vec::new();
    for column in columns {
        field(&mut result, 1, column.as_bytes());
    }
    for _ in 0..rows {
        field(&mut result, 2, &row);
    }
    let mut response = Vec::new();
    field(&mut response, 4, &result);
    response
}
#[tokio::test]
async fn dense_raw_grpc_responses_require_explicit_materialization_allowance() {
    let sql = format!(
        "SELECT {} FROM KV LIMIT 100000",
        ["OCTET_LENGTH(value)"; 32].join(",")
    );
    let columns = actordb_query::parse(&sql).unwrap().columns();
    let shape = RpcResponseShape::from_request(&wire::Request {
        table: "KV".into(),
        operation: Some(wire::request::Operation::Query(wire::Query {
            sql: sql.clone(),
            params: Vec::new(),
        })),
    })
    .with_query_shape(32, Some(100_000), false)
    .unwrap();
    let bytes = query_response(100_000, &columns);
    assert!(bytes.len() < MAX_RPC_MESSAGE_BYTES);
    assert!(preflight_rpc_response(&bytes, DEFAULT_MAX_DECODED_RESPONSE_BYTES, &shape).is_err());
    assert!(preflight_rpc_response(&bytes, 512 * 1024 * 1024, &shape).is_ok());
    let server = raw::RawServer::start(move |request| match &request.operation {
        Some(wire::request::Operation::Query(_)) => bytes.clone(),
        Some(wire::request::Operation::Get(get)) => wire::Response {
            reads: vec![wire::ReadResult {
                key: get.key.clone(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        _ => panic!("catalog"),
    })
    .await;
    for allowance in [DEFAULT_MAX_DECODED_RESPONSE_BYTES, 512 * 1024 * 1024] {
        let client = Client::connect(
            format!("http://{}", server.addr),
            ClientOptions::builder()
                .max_decoded_response_bytes(allowance)
                .max_response_memory_bytes(allowance * 2)
                .build(),
        )
        .await
        .unwrap();
        let result = client.query(&sql).await;
        if allowance == DEFAULT_MAX_DECODED_RESPONSE_BYTES {
            assert!(result.is_err());
        } else {
            let rows = result.unwrap().rows;
            assert_eq!(rows.len(), 100_000);
            assert_eq!(rows[0].len(), 32);
            assert_eq!(rows[0][0], QueryValue::Integer(0));
        }
        // Rejecting one HTTP/2 response does not retire unrelated RPC calls.
        assert!(client.get("next").await.unwrap().is_none());
        client.close().await;
    }
    server.stop().await;
}
#[tokio::test]
async fn raw_wrong_shapes_and_malformed_fields_fail_without_false_misses() {
    for bytes in [
        vec![34, 0, 34, 0],
        vec![29, 0],
        vec![8],
        vec![18, 2, 10, 255],
    ] {
        let server = raw::RawServer::start(move |_| bytes.clone()).await;
        let client = Client::connect(format!("http://{}", server.addr), ClientOptions::default())
            .await
            .unwrap();
        assert!(client.get("not-a-miss").await.is_err());
        assert!(client.put("not-success", vec![1]).await.is_err());
        client.close().await;
        server.stop().await;
    }
}
