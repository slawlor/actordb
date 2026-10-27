use actordb_client::{
    Client, ClientError, ClientOptions, QueryValue, RpcCallOptions, RpcClient, RpcError, RpcFuture,
    RpcReply,
};
use actordb_protocol::{
    client as wire,
    meta::{query_cell::Value as Cell, QueryCell},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct QueryRpc {
    columns: Vec<String>,
    rows: Vec<Vec<Option<Cell>>>,
}
impl RpcClient for QueryRpc {
    fn call(
        &self,
        request: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        Box::pin(async move {
            let response = if let Some(wire::request::Operation::Get(get)) = &request.operation {
                wire::Response {
                    reads: vec![wire::ReadResult {
                        key: get.key.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            } else {
                wire::Response {
                    query: Some(wire::QueryResult {
                        columns: self.columns.clone(),
                        rows: self
                            .rows
                            .iter()
                            .map(|row| wire::QueryRow {
                                cells: row
                                    .iter()
                                    .cloned()
                                    .map(|value| QueryCell { value })
                                    .collect(),
                            })
                            .collect(),
                    }),
                    ..Default::default()
                }
            };
            Ok(RpcReply::new(response))
        })
    }
}
#[tokio::test]
async fn malformed_metadata_fails_this_call_without_retiring_the_rpc_adapter() {
    for (projection, cell) in [
        ("timestamp_ms", Some(Cell::Integer("-1".into()))),
        (
            "timestamp_ms",
            Some(Cell::Integer((u64::MAX as i128 + 1).to_string())),
        ),
        ("timestamp_ms", Some(Cell::Integer("01".into()))),
        ("timestamp_ms", Some(Cell::Integer("+1".into()))),
        ("timestamp_ms", Some(Cell::Null(true))),
        ("logical_counter", Some(Cell::Text("1".into()))),
        (
            "logical_counter",
            Some(Cell::Integer((u64::MAX as i128 + 1).to_string())),
        ),
        ("node_id", Some(Cell::Bytes(vec![1]))),
        ("node_id", Some(Cell::Text(String::new()))),
        ("node_id", Some(Cell::Text("w".repeat(256)))),
        ("expires_at_ms", Some(Cell::Null(false))),
        ("expires_at_ms", Some(Cell::Integer("-1".into()))),
        (
            "expires_at_ms",
            Some(Cell::Integer((u64::MAX as i128 + 1).to_string())),
        ),
        ("is_tombstone", Some(Cell::Integer("2".into()))),
        ("is_tombstone", Some(Cell::Null(true))),
        ("is_tombstone", None),
    ] {
        let client = Client::from_rpc(
            Arc::new(QueryRpc {
                columns: vec![projection.into()],
                rows: vec![vec![cell]],
            }),
            ClientOptions::default(),
        )
        .unwrap();
        assert!(matches!(
            client.query(&format!("SELECT {projection} FROM KV")).await,
            Err(ClientError::Protocol(_))
        ));
        assert!(client.get("next").await.unwrap().is_none());
    }
}
#[tokio::test]
async fn canonical_unsigned_metadata_ranges_and_null_expiry_are_preserved() {
    let columns = [
        "timestamp_ms",
        "logical_counter",
        "node_id",
        "expires_at_ms",
        "is_tombstone",
    ];
    let rpc = QueryRpc {
        columns: columns.iter().map(|s| s.to_string()).collect(),
        rows: vec![
            vec![
                Some(Cell::Integer("0".into())),
                Some(Cell::Integer("0".into())),
                Some(Cell::Text("writer".into())),
                Some(Cell::Null(true)),
                Some(Cell::Integer("0".into())),
            ],
            vec![
                Some(Cell::Integer(u64::MAX.to_string())),
                Some(Cell::Integer(u64::MAX.to_string())),
                Some(Cell::Text("w".repeat(255))),
                Some(Cell::Integer(u64::MAX.to_string())),
                Some(Cell::Integer("1".into())),
            ],
        ],
    };
    let client = Client::from_rpc(Arc::new(rpc), ClientOptions::default()).unwrap();
    let rows = client
        .query("SELECT timestamp_ms,logical_counter,node_id,expires_at_ms,is_tombstone FROM KV")
        .await
        .unwrap()
        .rows;
    assert_eq!(rows[0][3], QueryValue::Null);
    assert_eq!(rows[1][0], QueryValue::Integer(u64::MAX as i128));
    assert_eq!(rows[1][4], QueryValue::Integer(1));
}
struct LostReply(Arc<AtomicUsize>);
impl RpcClient for LostReply {
    fn call(
        &self,
        _: Arc<wire::Request>,
        _: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(RpcError::Failure("reply lost after dispatch".into()))
        })
    }
}
#[tokio::test]
async fn a_lost_mutation_reply_remains_unknown_and_is_not_retried() {
    let calls = Arc::new(AtomicUsize::new(0));
    let client =
        Client::from_rpc(Arc::new(LostReply(calls.clone())), ClientOptions::default()).unwrap();
    assert!(matches!(
        client.put("once", vec![1]).await,
        Err(ClientError::UnknownOutcome(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[test]
fn impossible_rpc_limits_fail_before_framework_dispatch() {
    let rpc = Arc::new(LostReply(Arc::new(AtomicUsize::new(0))));
    for options in [
        ClientOptions::builder()
            .max_queued_requests(usize::MAX)
            .build(),
        ClientOptions::builder()
            .max_response_memory_bytes(1)
            .build(),
        ClientOptions::builder()
            .timeout(std::time::Duration::MAX)
            .build(),
    ] {
        assert!(matches!(
            Client::from_rpc(rpc.clone(), options),
            Err(ClientError::Invalid(_))
        ));
    }
}
