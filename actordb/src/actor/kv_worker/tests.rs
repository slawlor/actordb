// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Low-level worker APIs, retained reads, and input rejection.

use super::*;
use crate::actor::test_support::{test_op, test_state};
use crate::actor::{ApplyOutcome, OpPriority, ReplicatedOp, RetainedBatchRead, VersionedValue};
use ractor::rpc::CallResult;
use std::collections::HashMap;
use std::time::Duration;
#[tokio::test]
async fn worker_builder_with_omitted_pending_serves_real_actor_operations() {
    let shared = Arc::new(test_state(HashMap::new()));
    let (worker, handle) = KvWorker::spawn(
        None,
        KvWorker,
        KvWorkerArgs::builder().shared(shared.clone()).build(),
    )
    .await
    .unwrap();
    let value = VersionedValue::new(b"built".to_vec(), 10, "writer".into());
    assert_eq!(
        worker
            .call(
                |reply| KvMessage::ApplyOp {
                    op: ReplicatedOp {
                        key: "built".into(),
                        value: value.clone(),
                        seq: 1,
                        boot_id: 1
                    },
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                Some(Duration::from_secs(2))
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(ApplyOutcome::Applied))
    );
    assert_eq!(shared.get("built").await, Some(value));
    worker.drain().unwrap();
    handle.await.unwrap();
}
#[tokio::test]
async fn direct_worker_batch_read_preserves_positions_and_exact_response_boundary() {
    use crate::proto::{BatchGetResponse, BatchKeyValue, Envelope, EnvelopeMsg};
    use prost::Message;

    let value = VersionedValue::new(vec![7; 128], 128, "n".to_string());
    let shared = Arc::new(test_state(HashMap::from([(
        "present".to_string(),
        value.clone(),
    )])));
    let (worker, handle) = KvWorker::spawn(
        None,
        KvWorker,
        KvWorkerArgs {
            shared,
            pending: None,
        },
    )
    .await
    .unwrap();

    // Measure with Prost, including the nonzero request id and nested
    // envelope, rather than the actor's incremental sizing helper.
    let response = BatchGetResponse {
        req_id: u64::MAX,
        results: Vec::new(),
    };
    let response_base_payload_len = response.encoded_len();
    let mut singleton = response;
    singleton.results.push(BatchKeyValue {
        key: "present".to_string(),
        found: true,
        value: value.value.clone(),
        timestamp_ms: value.timestamp_ms,
        node_id: value.node_id.clone(),
        ..BatchKeyValue::default()
    });
    let singleton_response_len = Envelope {
        msg: Some(EnvelopeMsg::BatchGetResponse(singleton)),
    }
    .encoded_len();
    let keys = vec![
        "present".to_string(),
        "m".to_string(),
        "present".to_string(),
        "m".to_string(),
    ];
    let read = |keys: Vec<String>, response_soft_limit| {
        worker.call(
            move |reply| KvMessage::GetBatch {
                keys,
                response_base_payload_len,
                response_soft_limit,
                reply,
                priority: OpPriority::Background,
            },
            Some(Duration::from_secs(2)),
        )
    };

    let present = BatchReadEntry {
        key: "present".to_string(),
        result: Ok(Some(value)),
    };
    let missing = BatchReadEntry {
        key: "m".to_string(),
        result: Ok(None),
    };
    let CallResult::Success(below_boundary) = read(keys.clone(), singleton_response_len - 1)
        .await
        .unwrap()
    else {
        panic!("direct worker did not return the crossing singleton");
    };
    assert_eq!(below_boundary.entries, vec![present.clone()]);

    // Equality does not end the page: the next miss crosses the target
    // and is included once. The duplicate positions remain in the suffix.
    let CallResult::Success(first) = read(keys.clone(), singleton_response_len).await.unwrap()
    else {
        panic!("direct worker did not return the first prefix");
    };
    assert_eq!(first.entries, vec![present.clone(), missing.clone()]);
    let suffix = keys.into_iter().skip(first.entries.len()).collect();
    let CallResult::Success(second) = read(suffix, singleton_response_len).await.unwrap() else {
        panic!("direct worker did not return the resumed suffix");
    };
    assert_eq!(second.entries, vec![present.clone(), missing]);

    let retention = Arc::new(());
    let released = Arc::downgrade(&retention);
    let retained = worker
        .call(
            move |reply| {
                KvMessage::GetBatchRetained(RetainedBatchRead::new(
                    vec!["present".to_string(), "m".to_string()],
                    response_base_payload_len,
                    singleton_response_len - 1,
                    reply,
                    OpPriority::Background,
                    retention,
                ))
            },
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(
        retained,
        CallResult::Success(BatchReadPage {
            entries: vec![present]
        })
    );

    let empty = read(Vec::new(), 0).await.unwrap();
    assert_eq!(
        empty,
        CallResult::Success(BatchReadPage {
            entries: Vec::new()
        })
    );
    assert!(
        released.upgrade().is_none(),
        "low-level worker retained a completed request lease"
    );
    worker.stop(None);
    handle.await.unwrap();
}

#[tokio::test]
async fn invalid_wire_record_apply_is_nonfatal_to_worker() {
    let shared = Arc::new(test_state(HashMap::new()));
    let (worker, handle) = KvWorker::spawn(
        None,
        KvWorker,
        KvWorkerArgs {
            shared: shared.clone(),
            pending: None,
        },
    )
    .await
    .unwrap();

    let oversized_key = "k".repeat(crate::wire::MAX_FRAME_BYTES / 2);
    let invalid = worker
        .call(
            |reply| KvMessage::ApplyOp {
                op: test_op(
                    &oversized_key,
                    VersionedValue::new(Vec::new(), 1, "node".to_string()),
                ),
                priority: OpPriority::Default,
                is_replication: false,
                reply: Some(reply),
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        invalid,
        CallResult::Success(Err(ApplyError::InvalidInput(_)))
    ));

    for node_id in [
        String::new(),
        "n".repeat(crate::wire::MAX_WIRE_NODE_ID_BYTES + 1),
    ] {
        let invalid = worker
            .call(
                move |reply| KvMessage::ApplyOp {
                    op: test_op(
                        "invalid-provenance",
                        VersionedValue::new(Vec::new(), 2, node_id),
                    ),
                    priority: OpPriority::Default,
                    is_replication: false,
                    reply: Some(reply),
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            invalid,
            CallResult::Success(Err(ApplyError::InvalidInput(_)))
        ));
        assert!(shared.get("invalid-provenance").await.is_none());
    }

    let valid = worker
        .call(
            |reply| KvMessage::ApplyOp {
                op: test_op(
                    "healthy",
                    VersionedValue::new(b"value".to_vec(), 3, "node".to_string()),
                ),
                priority: OpPriority::Default,
                is_replication: false,
                reply: Some(reply),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(valid, CallResult::Success(Ok(ApplyOutcome::Applied)));
    assert_eq!(
        shared.get("healthy").await.unwrap().value,
        b"value".to_vec()
    );

    worker.stop(None);
    handle.await.unwrap();
}
