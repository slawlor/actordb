use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientError, ClientOptions, Priority, QueryValue};
use actordb_protocol::{
    client as wire, preflight_client_frame_with_budget, DEFAULT_MAX_DECODED_RESPONSE_BYTES,
    MAX_FRAME_BYTES,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// Whole-test-process Rust allocation requests, not allocator slack or RSS.
// Run the concurrent probe alone for an uncontaminated baseline.
struct Allocator;
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);
fn peak(value: usize) {
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let mut old = PEAK_BYTES.load(Ordering::Relaxed);
    while value > old {
        match PEAK_BYTES.compare_exchange_weak(old, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => old = actual,
        }
    }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            peak(LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
            peak(LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

async fn receive(stream: &mut TcpStream) -> wire::Frame {
    let size = stream.read_u32().await.unwrap();
    let mut bytes = vec![0; size as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    wire::Frame::decode(bytes.as_slice()).unwrap()
}
async fn send(stream: &mut TcpStream, frame: wire::Frame) {
    let bytes = frame.encode_to_vec();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}
async fn handshake(stream: &mut TcpStream) {
    assert!(matches!(
        receive(stream).await.message,
        Some(wire::frame::Message::Hello(_))
    ));
    send(
        stream,
        wire::Frame {
            message: Some(wire::frame::Message::Hello(wire::Hello {
                identity: "budget-peer".into(),
                challenge: vec![1; 32],
                protocol_version: actordb_protocol::CLIENT_PROTOCOL_VERSION,
            })),
        },
    )
    .await;
    assert!(matches!(
        receive(stream).await.message,
        Some(wire::frame::Message::Auth(_))
    ));
    send(
        stream,
        wire::Frame {
            message: Some(wire::frame::Message::Auth(wire::Auth::default())),
        },
    )
    .await;
}
fn field(out: &mut Vec<u8>, tag: u64, inner: &[u8]) {
    prost::encoding::encode_varint(tag << 3 | 2, out);
    prost::encoding::encode_varint(inner.len() as u64, out);
    out.extend_from_slice(inner);
}
fn query_response(id: u64, rows: usize, columns: &[String]) -> Vec<u8> {
    let mut row = Vec::new();
    for _ in columns {
        field(&mut row, 2, &[34, 1, b'0']);
    }
    let mut result = Vec::new();
    for column in columns {
        field(&mut result, 1, column.as_bytes());
    }
    for _ in 0..rows {
        field(&mut result, 2, &row);
    }
    let mut body = vec![8];
    prost::encoding::encode_varint(id, &mut body);
    field(&mut body, 5, &result);
    let mut frame = Vec::new();
    field(&mut frame, 4, &body);
    frame
}

#[tokio::test]
async fn legitimate_wide_rows_require_explicit_override_and_then_leave_connection_usable() {
    let sql = format!(
        "SELECT {} FROM kv LIMIT 100000",
        ["OCTET_LENGTH(value)"; 32].join(",")
    );
    let columns = actordb_query::parse(&sql).unwrap().columns();
    for allowance in [DEFAULT_MAX_DECODED_RESPONSE_BYTES, 512 * 1024 * 1024] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let columns = columns.clone();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let Some(wire::frame::Message::Request(request)) = receive(&mut stream).await.message
            else {
                panic!("request")
            };
            let bytes = query_response(request.id, 100_000, &columns);
            assert!(bytes.len() < MAX_FRAME_BYTES);
            let report = preflight_client_frame_with_budget(&bytes, 512 * 1024 * 1024).unwrap();
            assert_eq!(report.query_cells, 3_200_000);
            stream.write_u32(bytes.len() as u32).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            if allowance > DEFAULT_MAX_DECODED_RESPONSE_BYTES {
                let Some(wire::frame::Message::Request(request)) =
                    receive(&mut stream).await.message
                else {
                    panic!("follow-up")
                };
                send(
                    &mut stream,
                    wire::Frame {
                        message: Some(wire::frame::Message::Response(wire::Response {
                            id: request.id,
                            reads: vec![wire::ReadResult {
                                key: "next".into(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                    },
                )
                .await;
            }
        });
        let client = Client::connect(
            addr,
            ClientOptions::builder()
                .timeout(Duration::from_secs(60))
                .max_decoded_response_bytes(allowance)
                .build(),
        )
        .await
        .unwrap();
        let result = client.query(&sql).await;
        if allowance == DEFAULT_MAX_DECODED_RESPONSE_BYTES {
            assert!(matches!(result, Err(ClientError::Protocol(_))));
            assert!(matches!(client.get("next").await, Err(ClientError::Closed)));
        } else {
            let result = result.unwrap();
            assert_eq!(result.rows.len(), 100_000);
            assert!(result.rows.iter().all(
                |row| row.len() == 32 && row.iter().all(|cell| *cell == QueryValue::Integer(0))
            ));
            assert!(client.get("next").await.unwrap().is_none());
            client.close().await;
        }
        peer.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_server_maximum_narrow_query_and_wide_default_envelope_pass_default_admission() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let node = Arc::new(
            Node::start(
                "decode-budget".into(),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::default(),
            )
            .await
            .unwrap(),
        );
        let mut server = ClientServer::bind(
            node.clone(),
            "127.0.0.1:0".parse().unwrap(),
            ClientServerOptions {
                timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let client = Client::connect(
            server.addr(),
            ClientOptions::builder()
                .timeout(Duration::from_secs(60))
                .build(),
        )
        .await
        .unwrap();
        for start in (0..100_000).step_by(4096) {
            let values = (start..(start + 4096).min(100_000))
                .map(|index| (format!("row:{index:06}"), Vec::new()))
                .collect();
            assert!(client
                .put_multi_with_priority(values, Priority::Default)
                .await
                .unwrap()
                .into_iter()
                .all(|result| result.is_ok()));
        }
        let narrow = client
            .query("SELECT OCTET_LENGTH(value) FROM kv LIMIT 100000")
            .await
            .unwrap();
        assert_eq!(narrow.rows.len(), 100_000);
        let wide_sql = format!(
            "SELECT {} FROM kv LIMIT 5000",
            ["OCTET_LENGTH(value)"; 32].join(",")
        );
        let wide = client.query(&wide_sql).await.unwrap();
        assert_eq!(wide.rows.len(), 5000);
        assert!(wide.rows.iter().all(|row| row.len() == 32));
        assert!(client.get("row:000000").await.unwrap().is_some());
        assert!(!client.inspect().await.unwrap().health.is_empty());
        client.close().await;
        server.shutdown().await;
        node.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independently_connected_clients_and_shared_clones_obey_request_shapes_concurrently() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            tasks.push(tokio::spawn(async move {
                handshake(&mut stream).await;
                for _ in 0..4 {
                    let Some(wire::frame::Message::Request(request)) =
                        receive(&mut stream).await.message
                    else {
                        panic!("request")
                    };
                    let bytes =
                        query_response(request.id, 2000, &vec!["OCTET_LENGTH(value)".into(); 8]);
                    stream.write_u32(bytes.len() as u32).await.unwrap();
                    stream.write_all(&bytes).await.unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });
    let sql = format!(
        "SELECT {} FROM kv LIMIT 2000",
        ["OCTET_LENGTH(value)"; 8].join(",")
    );
    let mut clients = Vec::new();
    for _ in 0..4 {
        clients.push(
            Client::connect(addr, ClientOptions::default())
                .await
                .unwrap(),
        );
    }
    let mut tasks = Vec::new();
    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    for client in &clients {
        for _ in 0..4 {
            let client = client.clone();
            let sql = sql.clone();
            tasks.push(tokio::spawn(
                async move { client.query(&sql).await.unwrap() },
            ));
        }
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().rows.len(), 2000);
    }
    for client in clients {
        client.close().await;
    }
    peer.await.unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    eprintln!("4 clients ×4 shared clones: Rust allocation-request peak delta={} bytes, including mock send buffers and caller-owned task results; not SDK-only/RSS", PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline));
}

#[tokio::test]
async fn invalid_decoded_allowance_releases_transferred_stream_without_panicking() {
    for allowance in [0, 4095] {
        let (local, mut remote) = tokio::io::duplex(16);
        assert!(matches!(
            Client::from_stream(
                local,
                ClientOptions::builder()
                    .max_decoded_response_bytes(allowance)
                    .build()
            )
            .await,
            Err(ClientError::Invalid(_))
        ));
        assert_eq!(remote.read(&mut [0]).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn oversized_prefix_is_rejected_before_raw_allocation_or_body_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake(&mut stream).await;
        receive(&mut stream).await;
        // Do not provide a body: rejection must happen immediately from the
        // prefix, rather than allocating 16 MiB and waiting until timeout.
        stream.write_u32(MAX_FRAME_BYTES as u32).await.unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(closed, Ok(0))
                || matches!(closed, Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset)
        );
    });
    let client = Client::connect(
        addr,
        ClientOptions::builder()
            .timeout(Duration::from_secs(5))
            .max_decoded_response_bytes(4096)
            .build(),
    )
    .await
    .unwrap();
    assert!(matches!(
        client.get("large").await,
        Err(ClientError::Protocol(_))
    ));
    assert!(matches!(client.get("next").await, Err(ClientError::Closed)));
    peer.await.unwrap();
}
