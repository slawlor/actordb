//! Public registration, durable startup normalization, and Node lifecycle.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig, NodeError};
use actordb::shard::FragmentId;
use actordb::store::{KvStore, PrefixedStore, StoreError};
use actordb::ttl_expiry::{expiry_handler_fn, ExpiryBatch, ExpiryCallbackError, TtlExpiryOptions};
use tokio::sync::mpsc;

#[derive(Default)]
struct Store {
    entries: Mutex<BTreeMap<Vec<u8>, VersionedValue>>,
    fail_writes: AtomicBool,
    loads: AtomicUsize,
}
impl KvStore for Store {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(StoreError("unavailable".into()));
        }
        self.entries.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
}
fn live(version: u64) -> VersionedValue {
    let mut value = VersionedValue::new(vec![0xff, 0, 0x80], version, "original-writer".into());
    value.expires_at_ms = Some(1);
    value
}
fn populate(store: Arc<Store>, records: Vec<(&str, VersionedValue)>) {
    PrefixedStore::new(store, FragmentId(0))
        .write_batch(
            &records
                .into_iter()
                .map(|(key, value)| (key.as_bytes().to_vec(), value))
                .collect::<Vec<_>>(),
        )
        .unwrap();
}

#[tokio::test]
async fn startup_reports_expired_live_values_once_and_retains_original_versions() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let store = Arc::new(Store::default());
        let a = live(10);
        let b = live(11);
        let mut future = live(12);
        future.expires_at_ms = Some(u64::MAX);
        populate(
            store.clone(),
            vec![
                ("a", a.clone()),
                ("b", b.clone()),
                (
                    "deleted",
                    VersionedValue::tombstone(13, "original-writer".into()),
                ),
                ("future", future.clone()),
            ],
        );
        let (sender, mut output) = mpsc::channel(8);
        let handler = Arc::new(expiry_handler_fn(move |batch| {
            let sender = sender.clone();
            async move {
                sender
                    .try_send(batch)
                    .map_err(|_| ExpiryCallbackError::new("receiver full"))
            }
        }));
        let config = NodeConfig::builder()
            .store(store.clone())
            .ttl_expiry_handler(handler)
            .ttl_expiry_options(
                TtlExpiryOptions::builder()
                    .batch_size(8)
                    .max_batch_delay(Duration::from_secs(60))
                    .build(),
            )
            .build();
        let node = Node::start(
            "startup-node".into(),
            "127.0.0.1:0".parse().unwrap(),
            config.clone(),
        )
        .await
        .unwrap();
        assert!(node.get("a".into()).await.unwrap().is_none());
        assert_eq!(
            node.get("future".into()).await.unwrap(),
            Some(future.clone())
        );
        node.shutdown().await.unwrap();
        let batch = output.try_recv().unwrap();
        assert_eq!(batch.local_node_id, "startup-node");
        assert_eq!(
            batch
                .events
                .iter()
                .map(|event| event.key.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(batch.events[0].previous_value, a);
        assert_eq!(batch.events[1].previous_value, b);
        assert!(batch
            .events
            .iter()
            .all(|e| e.fragment == FragmentId(0) && e.expires_at_ms == 1 && e.observed_at_ms >= 1));
        assert_eq!(node.ttl_expiry_diagnostics().unwrap().delivered_events, 2);
        let stored = PrefixedStore::new(store.clone(), FragmentId(0))
            .load()
            .unwrap();
        let stored: BTreeMap<_, _> = stored.into_iter().collect();
        assert_eq!(stored[b"a".as_slice()], a.expiry_tombstone());
        assert_eq!(stored[b"b".as_slice()], b.expiry_tombstone());
        assert_eq!(stored[b"future".as_slice()], future);
        // Persisted tombstones no longer have the original payload. Restart is
        // not notification replay; the future live record also stays silent.
        let restarted = Node::start(
            "startup-node".into(),
            "127.0.0.1:0".parse().unwrap(),
            config,
        )
        .await
        .unwrap();
        restarted.shutdown().await.unwrap();
        assert_eq!(
            restarted.ttl_expiry_diagnostics().unwrap().enqueued_events,
            0
        );
        assert!(output.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn startup_validation_and_storage_failure_do_not_publish_staged_events() {
    for fail_store in [true, false] {
        let store = Arc::new(Store::default());
        populate(
            store.clone(),
            vec![("expired", live(if fail_store { 10 } else { u64::MAX }))],
        );
        store.fail_writes.store(fail_store, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_handler = calls.clone();
        let handler = expiry_handler_fn(move |_batch| {
            calls_handler.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        let result = Node::start(
            "invalid-start".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .store(store)
                .ttl_expiry_handler(Arc::new(handler))
                .build(),
        )
        .await;
        assert!(matches!(result, Err(NodeError::Storage(_))));
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn disabled_registration_and_already_expired_application_writes_are_silent() {
    let disabled = Node::start(
        "disabled".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::default(),
    )
    .await
    .unwrap();
    assert!(disabled.ttl_expiry_diagnostics().is_none());
    disabled.shutdown().await.unwrap();
    let (sender, mut output) = mpsc::channel(4);
    let handler = expiry_handler_fn(move |batch| {
        let sender = sender.clone();
        async move {
            sender
                .try_send(batch)
                .map_err(|_| ExpiryCallbackError::new("full"))
        }
    });
    let enabled = Node::start(
        "enabled".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .ttl_expiry_handler(Arc::new(handler))
            .build(),
    )
    .await
    .unwrap();
    assert!(matches!(
        enabled
            .put_with_ttl("expired".into(), vec![0xff], Some(Duration::ZERO))
            .await,
        Err(NodeError::Expired)
    ));
    assert!(enabled.get("expired".into()).await.unwrap().is_none());
    enabled.shutdown().await.unwrap();
    assert_eq!(enabled.ttl_expiry_diagnostics().unwrap().enqueued_events, 0);
    assert!(output.try_recv().is_err());
}

#[tokio::test]
async fn invalid_callback_configuration_is_rejected_before_loading_storage() {
    let store = Arc::new(Store::default());
    let handler = expiry_handler_fn(|_batch| async { Ok(()) });
    let options = TtlExpiryOptions {
        queue_event_capacity: 0,
        ..Default::default()
    };
    let result = Node::start(
        "invalid-options".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .store(store.clone())
            .ttl_expiry_handler(Arc::new(handler))
            .ttl_expiry_options(options)
            .build(),
    )
    .await;
    assert!(matches!(result, Err(NodeError::Actor(_))));
    assert_eq!(store.loads.load(Ordering::SeqCst), 0);
}

struct OnDrop(Arc<AtomicUsize>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn node_shutdown_drains_callbacks_with_a_bound_and_reports_abandonment() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let store = Arc::new(Store::default());
        populate(store.clone(), vec![("key", live(10))]);
        let dropped = Arc::new(AtomicUsize::new(0));
        let drop_handler = dropped.clone();
        let (entered, mut entered_rx) = mpsc::channel(1);
        let handler = expiry_handler_fn(move |batch: ExpiryBatch| {
            let entered = entered.clone();
            let dropped = drop_handler.clone();
            async move {
                let _batch = batch;
                let _drop = OnDrop(dropped);
                entered.try_send(()).unwrap();
                std::future::pending().await
            }
        });
        let options = TtlExpiryOptions::builder()
            .batch_size(1)
            .handler_timeout(Duration::from_secs(60))
            .shutdown_drain_timeout(Duration::from_millis(10))
            .build();
        let node = Node::start(
            "drain-node".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::builder()
                .store(store)
                .ttl_expiry_handler(Arc::new(handler))
                .ttl_expiry_options(options)
                .build(),
        )
        .await
        .unwrap();
        entered_rx.recv().await.unwrap();
        node.shutdown().await.unwrap();
        let diagnostics = node.ttl_expiry_diagnostics().unwrap();
        assert_eq!(diagnostics.abandoned_events, 1);
        assert_eq!(diagnostics.handler_timeouts, 0);
        assert_eq!(diagnostics.retained_events, 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
