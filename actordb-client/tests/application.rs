use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientError, ClientOptions, Priority, PutOptions, QueryValue};
use actordb_protocol::{client as wire, CLIENT_PROTOCOL_VERSION};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn server(options: ClientServerOptions) -> (Arc<Node>, ClientServer) {
    let node = Arc::new(
        Node::start(
            "sdk-node".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    );
    let server = ClientServer::bind(node.clone(), "127.0.0.1:0".parse().unwrap(), options)
        .await
        .unwrap();
    (node, server)
}

#[tokio::test]
async fn full_application_catalog_preserves_peer_membership_and_node_lifecycle() {
    let (node, mut server) = server(ClientServerOptions::default()).await;
    let client = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap();
    assert_eq!(node.peer_count().await, 0);
    client
        .put_with_options(
            "item:1",
            b"123".to_vec(),
            PutOptions::builder()
                .maybe_ttl(Some(Duration::from_secs(60)))
                .priority(Priority::High)
                .build(),
        )
        .await
        .unwrap();
    let value = client.get("item:1").await.unwrap().unwrap();
    assert_eq!(value.bytes, b"123");
    assert_eq!(value.node_id, "sdk-node");
    assert!(value.timestamp_ms > 0);
    assert!(value.expires_at_ms.is_some());
    let writes = client
        .put_multi(vec![
            ("dup".into(), b"old".to_vec()),
            ("dup".into(), b"new".to_vec()),
            ("item:2".into(), b"456".to_vec()),
        ])
        .await
        .unwrap();
    assert_eq!(writes.len(), 3);
    assert!(writes.into_iter().all(|result| result.is_ok()));
    let reads = client
        .get_multi(vec!["dup".into(), "missing".into(), "dup".into()])
        .await
        .unwrap();
    assert_eq!(reads.len(), 3);
    assert_eq!(reads[0].as_ref().unwrap().as_ref().unwrap().bytes, b"new");
    assert!(reads[1].as_ref().unwrap().is_none());
    assert_eq!(reads[2].as_ref().unwrap().as_ref().unwrap().bytes, b"new");
    assert!(client.get_multi(Vec::new()).await.unwrap().is_empty());
    assert!(client.put_multi(Vec::new()).await.unwrap().is_empty());
    let query = client
        .query_with_params(
            "SELECT key, value FROM kv WHERE key = ?",
            &[QueryValue::Text("dup".into())],
        )
        .await
        .unwrap();
    assert_eq!(
        query.rows,
        vec![vec![
            QueryValue::Text("dup".into()),
            QueryValue::Bytes(b"new".to_vec())
        ]]
    );
    assert_eq!(
        client.query("SELECT COUNT(*) FROM kv").await.unwrap().rows,
        vec![vec![QueryValue::Integer(3)]]
    );
    let scan = client
        .scan(wire::Scan {
            matcher_kind: 1,
            prefix: b"item:".to_vec(),
            transform_kind: 2,
            prefix_bytes: 1,
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(scan.len(), 2);
    assert_eq!(scan[0].value, b"1");
    client
        .delete_with_priority("dup", Priority::High)
        .await
        .unwrap();
    assert!(client.get("dup").await.unwrap().is_none());
    assert!(
        matches!(client.put_with_ttl("expired", b"x".to_vec(), Some(Duration::ZERO)).await, Err(ClientError::Server(error)) if error.code == wire::ErrorCode::Expired as i32)
    );
    assert!(client.get("expired").await.unwrap().is_none());
    let inspection = client.inspect().await.unwrap();
    assert_eq!(inspection.node_id, "sdk-node");
    assert_eq!(inspection.peer_count, 0);
    assert_eq!(inspection.health.len(), 1);

    // Table views share the authenticated connection; no second socket is
    // opened for the users table.
    let users = client.table("users").unwrap();
    users.put("item:1", b"table-value".to_vec()).await.unwrap();
    assert_eq!(
        users.get("item:1").await.unwrap().unwrap().bytes,
        b"table-value"
    );
    assert_eq!(client.get("item:1").await.unwrap().unwrap().bytes, b"123");
    assert_eq!(
        users
            .query("SELECT COUNT(*) FROM users")
            .await
            .unwrap()
            .rows,
        vec![vec![QueryValue::Integer(1)]]
    );
    client.close().await;
    assert_eq!(node.peer_count().await, 0);
    server.shutdown().await;
    node.put("still-running".into(), b"ok".to_vec())
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn application_credentials_are_mutual_and_separate_from_cluster_auth() {
    let secret = vec![42; 32];
    let (node, mut server) = server(ClientServerOptions {
        credential: Some(secret.clone()),
        ..Default::default()
    })
    .await;
    assert!(Client::connect(server.addr(), ClientOptions::default())
        .await
        .is_err());
    assert!(Client::connect(
        server.addr(),
        ClientOptions::builder()
            .maybe_credential(Some(vec![43; 32]))
            .build()
    )
    .await
    .is_err());
    let client = Client::connect(
        server.addr(),
        ClientOptions::builder()
            .maybe_credential(Some(secret))
            .build(),
    )
    .await
    .unwrap();
    client
        .put("authenticated", b"value".to_vec())
        .await
        .unwrap();
    assert_eq!(node.peer_count().await, 0);
    client.close().await;
    server.shutdown().await;
    node.shutdown().await.unwrap();
}

async fn send(stream: &mut TcpStream, frame: wire::Frame) {
    let bytes = frame.encode_to_vec();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}
async fn receive(stream: &mut TcpStream) -> wire::Frame {
    let length = stream.read_u32().await.unwrap();
    let mut bytes = vec![0; length as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    wire::Frame::decode(bytes.as_slice()).unwrap()
}
async fn mock_handshake(stream: &mut TcpStream) {
    assert!(matches!(
        receive(stream).await.message,
        Some(wire::frame::Message::Hello(_))
    ));
    send(
        stream,
        wire::Frame {
            message: Some(wire::frame::Message::Hello(wire::Hello {
                identity: "mock".into(),
                challenge: vec![7; 32],
                protocol_version: CLIENT_PROTOCOL_VERSION,
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
            message: Some(wire::frame::Message::Auth(wire::Auth { proof: Vec::new() })),
        },
    )
    .await;
}

#[tokio::test]
async fn application_listener_and_sdk_require_exact_current_versions() {
    let (node, mut listener) = server(ClientServerOptions::default()).await;
    for version in (0..CLIENT_PROTOCOL_VERSION).chain([CLIENT_PROTOCOL_VERSION + 1]) {
        let mut stream = TcpStream::connect(listener.addr()).await.unwrap();
        send(
            &mut stream,
            wire::Frame {
                message: Some(wire::frame::Message::Hello(wire::Hello {
                    identity: "wrong-version".into(),
                    challenge: vec![9; 32],
                    protocol_version: version,
                })),
            },
        )
        .await;
        let mut byte = [0];
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(result, Ok(0))
                || matches!(result, Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset)
        );

        let mock = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock.local_addr().unwrap();
        let serving = tokio::spawn(async move {
            let (mut stream, _) = mock.accept().await.unwrap();
            assert!(
                matches!(receive(&mut stream).await.message, Some(wire::frame::Message::Hello(hello)) if hello.protocol_version == CLIENT_PROTOCOL_VERSION)
            );
            send(
                &mut stream,
                wire::Frame {
                    message: Some(wire::frame::Message::Hello(wire::Hello {
                        identity: "mock".into(),
                        challenge: vec![7; 32],
                        protocol_version: version,
                    })),
                },
            )
            .await;
        });
        assert!(matches!(
            Client::connect(addr, ClientOptions::default()).await,
            Err(ClientError::Authentication)
        ));
        serving.await.unwrap();
    }
    listener.shutdown().await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_application_operation_is_explicitly_unsupported_without_closing_current_client() {
    let (node, mut listener) = server(ClientServerOptions::default()).await;
    let mut stream = TcpStream::connect(listener.addr()).await.unwrap();
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Hello(wire::Hello {
                identity: "unknown-operation".into(),
                challenge: vec![8; 32],
                protocol_version: CLIENT_PROTOCOL_VERSION,
            })),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut stream).await.message,
        Some(wire::frame::Message::Hello(_))
    ));
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Auth(wire::Auth { proof: Vec::new() })),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut stream).await.message,
        Some(wire::frame::Message::Auth(_))
    ));
    let mut request = wire::Request {
        id: 10,
        table: "KV".into(),
        operation: None,
    }
    .encode_to_vec();
    // Unknown request-operation field 11, empty length-delimited payload.
    // Prost discards the field, but the server must still return Unsupported.
    request.extend_from_slice(&[0x5a, 0]);
    let mut frame = vec![0x1a]; // Frame.request
    prost::encoding::encode_varint(request.len() as u64, &mut frame);
    frame.extend_from_slice(&request);
    stream.write_u32(frame.len() as u32).await.unwrap();
    stream.write_all(&frame).await.unwrap();
    assert!(
        matches!(receive(&mut stream).await.message, Some(wire::frame::Message::Response(wire::Response { id: 10, error: Some(error), .. })) if error.code == wire::ErrorCode::Unsupported as i32)
    );
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Request(wire::Request {
                id: 11,
                table: "KV".into(),
                operation: Some(wire::request::Operation::Get(wire::Get {
                    key: "missing".into(),
                    priority: 0,
                })),
            })),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut stream).await.message,
        Some(wire::frame::Message::Response(wire::Response {
            id: 11,
            error: None,
            ..
        }))
    ));
    drop(stream);
    listener.shutdown().await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_partial_response_closes_connection_and_releases_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (partial_tx, partial_rx) = tokio::sync::oneshot::channel();
    let mock = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        mock_handshake(&mut stream).await;
        receive(&mut stream).await;
        stream.write_u32(100).await.unwrap();
        stream.write_all(&[1, 2]).await.unwrap();
        partial_tx.send(()).unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(closed, Ok(0))
                || matches!(closed, Err(ref error) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe))
        );
    });
    let client = Client::connect(
        addr,
        ClientOptions::builder().max_queued_requests(1).build(),
    )
    .await
    .unwrap();
    let active = client.clone();
    let task = tokio::spawn(async move { active.get("partial").await });
    partial_rx.await.unwrap();
    assert!(matches!(
        client.get("queue-full").await,
        Err(ClientError::Overloaded)
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(client.get("next").await, Err(ClientError::Closed)));
    mock.await.unwrap();
}

#[tokio::test]
async fn disconnected_mutation_returns_unknown_outcome_without_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        mock_handshake(&mut stream).await;
        assert!(matches!(
            receive(&mut stream).await.message,
            Some(wire::frame::Message::Request(wire::Request {
                operation: Some(wire::request::Operation::Put(_)),
                ..
            }))
        ));
        drop(stream);
        // A disconnected write must not trigger a second dial.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client = Client::connect(addr, ClientOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        client.put("uncertain", b"x".to_vec()).await,
        Err(ClientError::UnknownOutcome(_))
    ));
    assert!(matches!(
        client.get("uncertain").await,
        Err(ClientError::Closed)
    ));
    mock.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_shutdown_drains_canceled_client_mutation_before_returning() {
    use actordb::actor::VersionedValue;
    use actordb::store::{KvStore, StoreError};
    struct BlockingStore {
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        gate: std::sync::Condvar,
    }
    impl KvStore for BlockingStore {
        fn bind(
            &self,
            _identity: &actordb::store::StoreIdentity,
        ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
            Ok(actordb::store::StoreBinding::unmanaged())
        }

        fn load_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, actordb::actor::VersionedValue)>, actordb::store::StoreError>
        {
            // This fixture owns a bounded in-memory dataset, not a production prefix scan.
            Ok(self
                .load()?
                .into_iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .collect())
        }

        fn commit_durability(&self) -> actordb::store::StoreCommitDurability {
            actordb::store::StoreCommitDurability::Buffered
        }

        fn stable_fence(
            &self,
        ) -> Result<actordb::store::StoreCommitDurability, actordb::store::StoreError> {
            Ok(self.commit_durability())
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            if !writes.is_empty() {
                self.entered.notify_one();
                let mut released = self.released.lock().unwrap();
                while !*released {
                    released = self.gate.wait(released).unwrap();
                }
            }
            Ok(())
        }
    }
    let store = Arc::new(BlockingStore {
        entered: tokio::sync::Notify::new(),
        released: std::sync::Mutex::new(false),
        gate: std::sync::Condvar::new(),
    });
    let config = NodeConfig::builder()
        .store(store.clone())
        .flush_batch_size(1)
        .build();
    let node = Arc::new(
        Node::start(
            "drain-origin".into(),
            "127.0.0.1:0".parse().unwrap(),
            config,
        )
        .await
        .unwrap(),
    );
    let replica = Node::start(
        "drain-replica".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    node.connect(replica.addr()).await.unwrap();
    let mut server = ClientServer::bind(
        node.clone(),
        "127.0.0.1:0".parse().unwrap(),
        ClientServerOptions::default(),
    )
    .await
    .unwrap();
    let client = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap();
    let active = client.clone();
    let write = tokio::spawn(async move { active.put("retained", b"accepted".to_vec()).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    write.abort();
    let _ = write.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), server.shutdown())
            .await
            .is_err()
    );
    // Canceling the waiter retains the join handle for a subsequent retry.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), server.shutdown())
            .await
            .is_err()
    );
    *store.released.lock().unwrap() = true;
    store.gate.notify_all();
    tokio::time::timeout(Duration::from_secs(5), server.shutdown())
        .await
        .unwrap();
    assert_eq!(
        node.get("retained".into()).await.unwrap().unwrap().value,
        b"accepted"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if replica.get("retained".into()).await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.shutdown().await.unwrap();
    replica.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_ordered_results_and_query_rows_retire_connection() {
    for case in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mock = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            mock_handshake(&mut stream).await;
            let request = match receive(&mut stream).await.message {
                Some(wire::frame::Message::Request(request)) => request,
                _ => panic!("expected request"),
            };
            let mut response = wire::Response {
                id: request.id,
                ..Default::default()
            };
            match case {
                0 => {
                    response.reads = vec![
                        wire::ReadResult {
                            key: "b".into(),
                            ..Default::default()
                        },
                        wire::ReadResult {
                            key: "a".into(),
                            ..Default::default()
                        },
                    ]
                }
                1 => {
                    response.mutations = vec![wire::MutationResult {
                        key: "wrong".into(),
                        error: None,

                        write_concern: None,
                    }]
                }
                _ => {
                    response.query = Some(wire::QueryResult {
                        columns: vec!["key".into()],
                        rows: vec![actordb_protocol::meta::QueryPageRow {
                            key: String::new(),
                            cells: Vec::new(),
                        }],
                    })
                }
            }
            send(
                &mut stream,
                wire::Frame {
                    message: Some(wire::frame::Message::Response(response)),
                },
            )
            .await;
        });
        let client = Client::connect(addr, ClientOptions::default())
            .await
            .unwrap();
        match case {
            0 => assert!(matches!(
                client.get_multi(vec!["a".into(), "b".into()]).await,
                Err(ClientError::Protocol(_))
            )),
            1 => assert!(matches!(
                client.put("a", b"x".to_vec()).await,
                Err(ClientError::UnknownOutcome(_))
            )),
            _ => assert!(matches!(
                client.query("SELECT key FROM kv").await,
                Err(ClientError::Protocol(_))
            )),
        }
        assert!(matches!(client.get("next").await, Err(ClientError::Closed)));
        mock.await.unwrap();
    }
}

#[tokio::test]
async fn incomplete_application_prefix_is_timed_out_and_idle_client_survives() {
    let (node, mut server) = server(ClientServerOptions {
        timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    let mut stream = TcpStream::connect(server.addr()).await.unwrap();
    let hello = wire::Hello {
        identity: "raw".into(),
        challenge: vec![8; 32],
        protocol_version: CLIENT_PROTOCOL_VERSION,
    };
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Hello(hello)),
        },
    )
    .await;
    receive(&mut stream).await;
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Auth(wire::Auth { proof: Vec::new() })),
        },
    )
    .await;
    receive(&mut stream).await;
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Request(wire::Request {
                id: 1,
                operation: Some(wire::request::Operation::Query(wire::Query {
                    sql: "SELECT COUNT(*) FROM kv".into(),
                    params: Vec::new(),
                })),
                table: String::new(),
            })),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut stream).await.message, Some(wire::frame::Message::Response(wire::Response { error: Some(error), .. })) if error.code == wire::ErrorCode::Invalid as i32)
    );
    send(
        &mut stream,
        wire::Frame {
            message: Some(wire::frame::Message::Request(wire::Request {
                id: 2,
                operation: Some(wire::request::Operation::Get(wire::Get {
                    key: "missing".into(),
                    priority: 0,
                })),
                table: String::new(),
            })),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut stream).await.message,
        Some(wire::frame::Message::Response(wire::Response {
            error: Some(error),
            ..
        })) if error.code == wire::ErrorCode::Invalid as i32
    ));
    let idle = Client::connect(server.addr(), ClientOptions::default())
        .await
        .unwrap();
    stream.write_all(&[0]).await.unwrap();
    let mut byte = [0];
    let closed = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || matches!(closed, Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset)
    );
    assert!(idle.get("missing").await.unwrap().is_none());
    idle.close().await;
    server.shutdown().await;
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn excessive_public_queue_limits_return_errors_without_panicking() {
    let result = Client::connect(
        "127.0.0.1:1".parse().unwrap(),
        ClientOptions::builder()
            .max_queued_requests(usize::MAX)
            .build(),
    )
    .await;
    assert!(matches!(result, Err(ClientError::Invalid(_))));
    let node = Arc::new(
        Node::start(
            "invalid-limits".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
        )
        .await
        .unwrap(),
    );
    for options in [
        ClientServerOptions {
            max_connections: usize::MAX,
            ..Default::default()
        },
        ClientServerOptions {
            max_in_flight_requests: usize::MAX,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            ClientServer::bind(node.clone(), "127.0.0.1:0".parse().unwrap(), options).await,
            Err(actordb::node::NodeError::Codec(_))
        ));
    }
    node.shutdown().await.unwrap();
}
