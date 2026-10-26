//! Handled backend failures preserve the actual managed worker mailbox.

use super::*;
use crate::actor::observations::{PoolObservation, PoolObservationRequest};
use crate::actor::{
    ApplyError, ApplyOutcome, KvActor, KvArgs, OpPriority, PoolConfig, ReplicatedOp, VersionedValue,
};
use crate::shard::FragmentId;
use crate::store::{
    KvStore, PersistenceConfig, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity,
};
use ractor::rpc::CallResult;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

struct GatedStore {
    entered: Notify,
    released: StdMutex<bool>,
    wake: Condvar,
    calls: AtomicUsize,
    panic: bool,
    writes: StdMutex<HashMap<Vec<u8>, VersionedValue>>,
}

impl GatedStore {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

impl KvStore for GatedStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(self.commit_durability())
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
            drop(released);
            if self.panic {
                panic!("controlled backend panic");
            }
            return Err(StoreError("controlled transient storage error".into()));
        }
        self.writes.lock().unwrap().extend(writes.iter().cloned());
        Ok(())
    }
}

struct Fixture {
    store: Arc<GatedStore>,
    shared: Arc<FragmentState>,
    actor: ActorRef<KvMessage>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.store.release();
        self.actor.stop(None);
    }
}
impl Fixture {
    async fn new(panic: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let store = Arc::new(GatedStore {
            entered: Notify::new(),
            released: StdMutex::new(false),
            wake: Condvar::new(),
            calls: AtomicUsize::new(0),
            panic,
            writes: StdMutex::new(HashMap::new()),
        });
        let shared = Arc::new(
            FragmentState::new(
                HashMap::from([(
                    key("present"),
                    VersionedValue::new(vec![9], 1, "writer".into()),
                )]),
                Some(store.clone()),
                1,
                Duration::from_secs(3600),
            )
            .with_persistence_config(PersistenceConfig::default())
            .unwrap()
            .enable_partitions()
            .unwrap(),
        );
        let pool = PoolConfig::builder().workers(1).queue_cap(8).build();
        let (actor, _) = KvActor::spawn(
            None,
            KvActor,
            KvArgs::builder()
                .shared(shared.clone())
                .fragment(FragmentId(0))
                .worker_prefix(format!(
                    "handled-storage-error-{}",
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ))
                .high_pool(pool.clone())
                .default_pool(pool.clone())
                .background_pool(pool)
                .build(),
        )
        .await
        .unwrap();
        Self {
            store,
            shared,
            actor,
        }
    }
    async fn observation(&self, priority: OpPriority) -> PoolObservation {
        let request = PoolObservationRequest {
            tables: vec!["KV".into()],
            include_internal: false,
            max_pools: 3,
            max_workers: 3,
            retention: Arc::new(()),
        };
        let CallResult::Success(page) = self
            .actor
            .call(
                |reply| KvMessage::ObservePools { request, reply },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap()
        else {
            panic!("pool observation failed")
        };
        page.pools
            .into_iter()
            .find(|pool| pool.priority == priority)
            .unwrap()
    }
    async fn settled(&self, priority: OpPriority) -> PoolObservation {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pool = self.observation(priority).await;
                if pool.pending == 0 && pool.active == 0 && pool.queued == 0 {
                    return pool;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
    async fn stop(&self) {
        self.store.release();
        self.actor.drain().unwrap();
        tokio::time::timeout(Duration::from_secs(2), self.actor.wait(None))
            .await
            .unwrap()
            .unwrap();
    }
}

fn key(key: &str) -> String {
    crate::table_key::encode_table_key("KV", key)
}
fn op(key_name: &str, version: u64) -> ReplicatedOp {
    ReplicatedOp {
        key: key(key_name),
        value: VersionedValue::new(vec![version as u8], version, "writer".into()),
        seq: version,
        boot_id: 1,
    }
}
async fn receive<T>(response: oneshot::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .unwrap()
        .unwrap()
}

async fn queued_work_survives(panic: bool, priority: OpPriority) {
    let fixture = Fixture::new(panic).await;
    let before = fixture.settled(priority).await;
    let (write, wrote) = oneshot::channel();
    fixture
        .actor
        .cast(KvMessage::ApplyOp {
            op: op("first", 2),
            priority,
            is_replication: false,
            reply: Some(write.into()),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), fixture.store.entered.notified())
        .await
        .unwrap();
    let (read, read_result) = oneshot::channel();
    fixture
        .actor
        .cast(KvMessage::Get {
            key: key("present"),
            priority,
            reply: read.into(),
        })
        .unwrap();
    let (second, second_result) = oneshot::channel();
    fixture
        .actor
        .cast(KvMessage::ApplyOp {
            op: op("second", 3),
            priority,
            is_replication: false,
            reply: Some(second.into()),
        })
        .unwrap();
    let queued = fixture.observation(priority).await;
    assert_eq!(queued.pending, 3);
    assert_eq!(queued.active, 1);
    assert_eq!(queued.queued, 2);
    assert_eq!(queued.shed_total, 0);
    fixture.store.release();
    assert!(matches!(receive(wrote).await, Err(ApplyError::Storage(_))));
    assert!(matches!(receive(read_result).await, Ok(Some(value)) if value.value == [9]));
    assert_eq!(receive(second_result).await, Ok(ApplyOutcome::Applied));
    let after = fixture.settled(priority).await;
    assert_eq!(after.workers[0].actor_id, before.workers[0].actor_id);
    assert_eq!(after.workers[0].incarnation, before.workers[0].incarnation);
    assert_eq!(after.completed - before.completed, 3);
    assert_eq!(after.abandoned, 0);
    assert_eq!(after.shed_total, 0);
    assert_eq!(fixture.shared.persistence_backlog().await.0, 2);
    assert!(matches!(
        fixture
            .actor
            .call(
                |reply| KvMessage::Flush { reply },
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap(),
        CallResult::Success(Ok(_))
    ));
    assert_eq!(fixture.shared.persistence_backlog().await, (0, 0));
    assert_eq!(fixture.store.writes.lock().unwrap().len(), 2);
    fixture.stop().await;
}

#[tokio::test]
async fn storage_errors_keep_admitted_reads_writes_and_worker_identity() {
    for priority in [
        OpPriority::High,
        OpPriority::Default,
        OpPriority::Background,
    ] {
        queued_work_survives(false, priority).await;
    }
}

#[tokio::test]
async fn caught_backend_panics_keep_admitted_mailboxes_and_retry_winners() {
    for priority in [
        OpPriority::High,
        OpPriority::Default,
        OpPriority::Background,
    ] {
        queued_work_survives(true, priority).await;
    }
}

struct Ownership(Arc<AtomicUsize>);
impl Drop for Ownership {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_write_reply_keeps_actual_ownership_and_queued_reads() {
    let fixture = Fixture::new(false).await;
    let dropped = Arc::new(AtomicUsize::new(0));
    let retention: Arc<dyn Send + Sync> = Arc::new(Ownership(dropped.clone()));
    let (write, wrote) = oneshot::channel();
    fixture
        .actor
        .cast(
            KvMessage::ApplyOp {
                op: op("cancelled", 2),
                priority: OpPriority::Default,
                is_replication: false,
                reply: Some(write.into()),
            }
            .with_retention(retention),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), fixture.store.entered.notified())
        .await
        .unwrap();
    drop(wrote);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    let (read, read_result) = oneshot::channel();
    fixture
        .actor
        .cast(KvMessage::Get {
            key: key("present"),
            priority: OpPriority::Default,
            reply: read.into(),
        })
        .unwrap();
    assert_eq!(fixture.observation(OpPriority::Default).await.pending, 2);
    fixture.store.release();
    assert!(matches!(receive(read_result).await, Ok(Some(value)) if value.value == [9]));
    let after = fixture.settled(OpPriority::Default).await;
    assert_eq!(after.abandoned, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.shared.persistence_backlog().await.0, 1);
    fixture.stop().await;
    assert_eq!(fixture.shared.persistence_backlog().await, (0, 0));
    assert_eq!(fixture.store.writes.lock().unwrap().len(), 1);
}
