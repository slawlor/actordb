// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Real dispatcher flush, valid peer correlation, and same-socket progress.
use super::*;
use actordb::shard::{fragment_of, FragmentId};
use actordb::write_concern::{WriteConcernOptions, WriteDurability};
use actordb_protocol::meta::{
    BatchAck, ForwardedGet, ReconcileFetch, ReconcileRequest, ReplicationAckLevel, SessionStart,
};

fn key_for_fragment(fragment: u32) -> String {
    (0..1000)
        .map(|index| format!("isolation-{fragment}-{index}"))
        .find(|key| fragment_of(&kv_key(key), 2) == FragmentId(fragment))
        .unwrap()
}

struct FragmentZeroGatedStore(Arc<GatedStore>);
impl KvStore for FragmentZeroGatedStore {
    fn bind(
        &self,
        identity: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, StoreError> {
        self.0.bind(identity)
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.0.load()
    }
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.0.load_prefix(prefix)
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        self.0.commit_durability()
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.0.stable_fence()
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if writes
            .iter()
            .any(|(key, _)| key.starts_with(&0u32.to_be_bytes()))
        {
            self.0.write_batch(writes)
        } else {
            self.0
                .entries
                .lock()
                .unwrap()
                .extend(writes.iter().cloned());
            Ok(())
        }
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn dispatcher_flush_does_not_block_other_fragment_get_and_ack_on_same_socket() {
    let store = Arc::new(GatedStore::default());
    let release = store.release_on_drop();
    let blocked_key = key_for_fragment(0);
    let readable_key = key_for_fragment(1);
    store.entries.lock().unwrap().insert(
        kv_storage_key(1, &readable_key),
        VersionedValue::new(vec![8], now_ms(), "summary-source".into()),
    );
    let mut config = NodeConfig::default();
    config.fragment_count = 2;
    config.store = Some(Arc::new(FragmentZeroGatedStore(store.clone())));
    config.fragment_map = HashMap::from([
        (FragmentId(0), vec!["summary-source".into()]),
        (
            FragmentId(1),
            vec!["summary-source".into(), "summary-peer".into()],
        ),
    ]);
    config.snapshot_interval = Duration::from_secs(3600);
    config.clean_interval = Duration::from_secs(3600);
    config.replication = actordb::replication::SenderConfig::builder()
        .consistency_interval(Duration::from_secs(3600))
        .build();
    let node = Arc::new(
        Node::start_with_options(
            "summary-source".into(),
            "127.0.0.1:0".parse().unwrap(),
            config,
            NodeRuntimeOptions::builder()
                .local_work_timeout(Duration::from_millis(20))
                .build(),
        )
        .await
        .unwrap(),
    );
    let mut stream = tokio::net::TcpStream::connect(node.addr()).await.unwrap();
    complete_no_secret_handshake(&mut stream, "summary-peer").await;
    common::wait_for(
        || async { node.peer_count().await == 1 },
        Duration::from_secs(5),
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                fragment: 1,
                session_id: 78,
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 1,
                session_id: 78,
                round_id: 8,
                ..ReconcileRequest::default()
            })),
        },
    )
    .await;
    let advertised = loop {
        match recv_frame(&mut stream).await.msg {
            Some(EnvelopeMsg::ReconcileResponse(response))
                if response.session_id == 78 && response.round_id == 8 =>
            {
                break response
            }
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                send_reconcile_response(&mut stream, &request, Vec::new()).await;
            }
            _ => {}
        }
    };
    assert_eq!(advertised.summaries.len(), 1);
    node.put(readable_key.clone(), vec![8]).await.unwrap();
    let awaiting_ack = loop {
        match recv_frame(&mut stream).await.msg {
            Some(EnvelopeMsg::ReplicateBatch(batch))
                if batch.fragment == 1 && batch.batch_seq != u64::MAX =>
            {
                break batch
            }
            Some(EnvelopeMsg::ReconcileRequest(request)) => {
                send_reconcile_response(&mut stream, &request, Vec::new()).await;
            }
            _ => {}
        }
    };

    // A normal explicit local write concern reaches KvActor::Flush, rather
    // than merely blocking a periodic Snapshotter on the same store.
    let write_node = node.clone();
    let write = tokio::spawn(async move {
        write_node
            .put_with_write_concern(
                blocked_key,
                vec![7],
                WriteConcernOptions::default()
                    .with_minimum_durability(WriteDurability::StoreCommitted),
            )
            .await
    });
    common::wait_for(
        || async { store.entered.load(Ordering::SeqCst) },
        Duration::from_secs(5),
    )
    .await;
    for (fragment, session_id) in [(0, 77), (1, 78)] {
        send_frame(
            &mut stream,
            Envelope {
                msg: Some(EnvelopeMsg::SessionStart(SessionStart {
                    fragment,
                    session_id,
                })),
            },
        )
        .await;
    }
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                fragment: 0,
                session_id: 77,
                round_id: 9,
                ..ReconcileRequest::default()
            })),
        },
    )
    .await;
    // More summaries than the bounded responder can retain must be harmless
    // retryable misses, not a reason to disconnect unrelated operations.
    for round_id in 10..15 {
        send_frame(
            &mut stream,
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileRequest(ReconcileRequest {
                    fragment: 0,
                    session_id: 77,
                    round_id,
                    ..ReconcileRequest::default()
                })),
            },
        )
        .await;
    }
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileFetch(ReconcileFetch {
                fragment: 1,
                session_id: 78,
                round_id: 8,
                chunk_seq: 0,
                keys: vec![advertised.summaries[0].key.clone()],
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                req_id: 111,
                key: kv_key(&readable_key),
                hops: 0,
                ..ForwardedGet::default()
            })),
        },
    )
    .await;
    send_frame(
        &mut stream,
        Envelope {
            msg: Some(EnvelopeMsg::BatchAck(BatchAck {
                session_id: awaiting_ack.session_id,
                batch_seq: awaiting_ack.batch_seq,
                fragment: 1,
                level: ReplicationAckLevel::Applied as i32,
            })),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_millis(500), async {
        let mut saw_get = false;
        let mut saw_data = false;
        while !saw_get || !saw_data {
            match recv_frame(&mut stream).await.msg {
                Some(EnvelopeMsg::GetResponse(response)) if response.req_id == 111 => {
                    assert!(response.found);
                    assert_eq!(response.value, [8]);
                    saw_get = true;
                }
                Some(EnvelopeMsg::ReconcileData(data))
                    if data.session_id == 78 && data.round_id == 8 =>
                {
                    assert_eq!(data.fragment, 1);
                    assert_eq!(data.entries.len(), 1);
                    assert_eq!(data.entries[0].value, [8]);
                    saw_data = true;
                }
                Some(EnvelopeMsg::ReconcileResponse(response)) if response.session_id == 77 => {
                    panic!(
                        "blocked dispatcher unexpectedly served summary round {}",
                        response.round_id
                    );
                }
                _ => {}
            }
        }
    })
    .await
    .expect("fragment-0 dispatcher flush blocked unrelated Get/ACK parsing");
    common::wait_for(
        || async {
            node.sender_report(FragmentId(1), "summary-peer")
                .await
                .is_some_and(|report| {
                    report.last_ack_durability
                        == Some(actordb::replication::ReplicationAckDurability::MemoryApplied)
                })
        },
        Duration::from_millis(500),
    )
    .await;
    assert!(store.entered.load(Ordering::SeqCst));
    assert!(!*store.release.0.lock().unwrap());
    drop(release);
    let _ = write.await.unwrap();
    node.shutdown().await.unwrap();
}
