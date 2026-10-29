// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Actor-routed write throughput on one shared fragment/cache. Input payload
//! allocation is excluded; dispatcher admission, worker apply, hashing, cache
//! mutation and persistence enqueue are included. Fixed keys bound memory.
//! Depth 24 spreads writes across all three unlimited priority pools (four
//! workers each); depth 1 measures an uncontended Default write.
//!
//! `buffered` uses a no-op store and defers flushing until teardown. This
//! measures ordered persistence enqueue, not disk throughput or durability.

use std::collections::HashMap;
use std::time::Duration;
use std::{cell::Cell, sync::Arc};

use actordb::actor::{
    worker_idx, ApplyOutcome, FragmentState, KvActor, KvArgs, KvMessage, OpPriority, PoolConfig,
    ReplicatedOp, VersionedValue,
};
use actordb::shard::FragmentId;
use actordb::store::{
    KvStore, PersistenceConfig, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity,
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use futures::future::join_all;
use ractor::Actor;

struct NoopStore;

impl KvStore for NoopStore {
    fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        Ok(StoreBinding::unmanaged())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn load_prefix(&self, _: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(Vec::new())
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        StoreCommitDurability::Buffered
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        Ok(StoreCommitDurability::Buffered)
    }
}

fn bench_fragment_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    for buffered in [false, true] {
        let mode = if buffered { "buffered" } else { "storeless" };
        let mut group = c.benchmark_group(format!("fragment_writes/{mode}"));
        group.sample_size(20);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(3));
        for size in [100usize, 10_000, 1_000_000] {
            for depth in [1usize, 24] {
                let store = buffered.then(|| Arc::new(NoopStore) as Arc<dyn KvStore>);
                let shared = Arc::new(
                    FragmentState::new(
                        HashMap::new(),
                        store,
                        usize::MAX,
                        Duration::from_secs(24 * 3600),
                    )
                    .with_persistence_config(
                        PersistenceConfig::builder()
                            .max_bytes(128 * 1024 * 1024)
                            .build(),
                    )
                    .unwrap(),
                );
                let pool = || PoolConfig::builder().workers(4).queue_cap(1024).build();
                let (actor, handle) = rt
                    .block_on(KvActor::spawn(
                        None,
                        KvActor,
                        KvArgs::builder()
                            .shared(shared.clone())
                            .fragment(FragmentId(0))
                            .worker_prefix(format!("writes-{mode}-{size}-{depth}"))
                            .high_pool(pool())
                            .default_pool(pool())
                            .background_pool(pool())
                            .build(),
                    ))
                    .unwrap();
                let keys: Vec<String> = (0..depth)
                    .map(|index| {
                        let slot = index / 3 % 4;
                        (0u64..)
                            .map(|nonce| format!("write-{index}-{nonce}"))
                            .find(|key| worker_idx(key, 4) == slot)
                            .unwrap()
                    })
                    .collect();
                let version = Cell::new(0u64);
                group.throughput(Throughput::Elements(depth as u64));
                group.bench_function(BenchmarkId::new(format!("depth_{depth}"), size), |b| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let next = version.get() + 1;
                            version.set(next);
                            keys.iter()
                                .map(|key| ReplicatedOp {
                                    key: key.clone(),
                                    value: VersionedValue::new(
                                        vec![0xAB; size],
                                        next,
                                        "writer".into(),
                                    ),
                                    seq: next,
                                    boot_id: 1,
                                })
                                .collect::<Vec<_>>()
                        },
                        |ops| {
                            let actor = &actor;
                            async move {
                                let writes =
                                    ops.into_iter().enumerate().map(|(index, op)| async move {
                                        let priority = if depth == 1 {
                                            OpPriority::Default
                                        } else {
                                            match index % 3 {
                                                0 => OpPriority::High,
                                                1 => OpPriority::Default,
                                                _ => OpPriority::Background,
                                            }
                                        };
                                        ractor::call!(actor, |reply| KvMessage::ApplyOp {
                                            op,
                                            priority,
                                            is_replication: false,
                                            reply: Some(reply),
                                        })
                                    });
                                for outcome in join_all(writes).await {
                                    assert_eq!(outcome.unwrap().unwrap(), ApplyOutcome::Applied);
                                }
                            }
                        },
                        BatchSize::PerIteration,
                    );
                });
                rt.block_on(async {
                    actor.drain_and_wait(None).await.unwrap();
                    handle.await.unwrap();
                    shared.flush().await.unwrap();
                    if version.get() > 0 {
                        for key in &keys {
                            let value = shared.get(key).await.unwrap();
                            assert_eq!(value.timestamp_ms, version.get());
                            assert_eq!(value.value.len(), size);
                        }
                    }
                });
            }
        }
        group.finish();
    }
}

criterion_group!(benches, bench_fragment_writes);
criterion_main!(benches);
