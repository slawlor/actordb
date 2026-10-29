// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Compare cache lock implementations with identical actor workloads.
//! Input allocation is excluded. Runtime threads, read/write mix, payload size,
//! concurrency, and buffered persistence vary; fixed independent keys bound
//! memory. All three priority pools have four unlimited workers.
//!
//! A 1 ms timer records wakeup delay for intervals overlapping benchmark batches
//! (including warmup). Epochs include batches completed before the timer wakes,
//! so blocking an entire batch cannot hide its delay. These intervals can also
//! include excluded input setup. Timer samples are separate from Criterion's
//! throughput samples. Set ACTORDB_CACHE_LOCK_RESULTS to save JSON lines.

use std::cell::Cell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::actor::{
    worker_idx, ApplyOutcome, FragmentState, KvActor, KvArgs, KvMessage, OpPriority, PoolConfig,
    ReplicatedOp, VersionedValue,
};
use actordb::shard::FragmentId;
use actordb::store::{
    KvStore, PersistenceConfig, StoreBinding, StoreCommitDurability, StoreError, StoreIdentity,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
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

struct Case {
    id: &'static str,
    threads: usize,
    depth: usize,
    size: usize,
    /// 0 = write only; 1 = read only; 4 = one write then three reads.
    read_mix: usize,
    buffered: bool,
}
impl Case {
    fn is_write(&self, index: usize) -> bool {
        self.read_mix == 0 || (self.read_mix > 1 && index.is_multiple_of(self.read_mix))
    }
    fn writes(&self) -> usize {
        (0..self.depth)
            .filter(|&index| self.is_write(index))
            .count()
    }
}

enum Request {
    Put(ReplicatedOp),
    Get(String),
}

fn bench_fragment_locks(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragment_locks");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    let profiles = [
        ("write_t1_c1_100b", 1, 1, 100, 0, false),
        ("write_t4_c1_100b", 4, 1, 100, 0, false),
        ("write_t1_c24_100b", 1, 24, 100, 0, false),
        ("write_t4_c24_100b", 4, 24, 100, 0, false),
        ("write_t4_c24_10kb", 4, 24, 10_000, 0, false),
        ("write_t4_c24_1mb", 4, 24, 1_000_000, 0, false),
        ("buffered_t4_c24_100b", 4, 24, 100, 0, true),
        ("buffered_t4_c24_1mb", 4, 24, 1_000_000, 0, true),
        ("read75_t1_c24_100b", 1, 24, 100, 4, false),
        ("read75_t4_c24_100b", 4, 24, 100, 4, false),
        ("read75_t4_c24_1mb", 4, 24, 1_000_000, 4, false),
        ("read75_t4_c24_4mb", 4, 24, 4_000_000, 4, false),
        ("read100_t4_c24_1mb", 4, 24, 1_000_000, 1, false),
    ];
    for (id, threads, depth, size, read_mix, buffered) in profiles {
        let case = Case {
            id,
            threads,
            depth,
            size,
            read_mix,
            buffered,
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(case.threads)
            .enable_all()
            .build()
            .unwrap();
        let keys: Vec<String> = (0..case.depth)
            .map(|index| {
                (0u64..)
                    .map(|nonce| format!("lock-{index}-{nonce}"))
                    .find(|key| worker_idx(key, 4) == index / 3 % 4)
                    .unwrap()
            })
            .collect();
        let values = keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    VersionedValue::new(vec![0xAB; case.size], 0, "writer".into()),
                )
            })
            .collect::<HashMap<_, _>>();
        let store = case
            .buffered
            .then(|| Arc::new(NoopStore) as Arc<dyn KvStore>);
        let shared = Arc::new(
            FragmentState::new(values, store, usize::MAX, Duration::from_secs(24 * 3600))
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
                    .worker_prefix(format!("locks-{}", case.id))
                    .high_pool(pool())
                    .default_pool(pool())
                    .background_pool(pool())
                    .build(),
            ))
            .unwrap();
        let epoch = Arc::new(AtomicU64::new(0));
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let heartbeat_epoch = epoch.clone();
        let heartbeat = rt.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut previous = 0;
            let mut delays = Vec::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    scheduled = ticker.tick() => {
                        let current = heartbeat_epoch.load(Ordering::Relaxed);
                        if current != previous || current % 2 == 1 {
                            delays.push(tokio::time::Instant::now().saturating_duration_since(scheduled));
                        }
                        previous = current;
                    }
                }
            }
            delays
        });
        let version = Cell::new(0u64);
        group.throughput(Throughput::Elements(case.depth as u64));
        group.bench_function(case.id, |b| {
            b.to_async(&rt).iter_batched(
                || {
                    let next = version.get() + 1;
                    version.set(next);
                    keys.iter()
                        .enumerate()
                        .map(|(index, key)| {
                            if case.is_write(index) {
                                Request::Put(ReplicatedOp {
                                    key: key.clone(),
                                    value: VersionedValue::new(
                                        vec![0xAB; case.size],
                                        next,
                                        "writer".into(),
                                    ),
                                    seq: next,
                                    boot_id: 1,
                                })
                            } else {
                                Request::Get(key.clone())
                            }
                        })
                        .collect::<Vec<_>>()
                },
                |requests| {
                    let actor = &actor;
                    let epoch = &epoch;
                    let size = case.size;
                    async move {
                        epoch.fetch_add(1, Ordering::Relaxed);
                        let requests =
                            requests
                                .into_iter()
                                .enumerate()
                                .map(|(index, request)| async move {
                                    let priority = if depth == 1 {
                                        OpPriority::Default
                                    } else {
                                        match index % 3 {
                                            0 => OpPriority::High,
                                            1 => OpPriority::Default,
                                            _ => OpPriority::Background,
                                        }
                                    };
                                    match request {
                                        Request::Put(op) => {
                                            let result =
                                                ractor::call!(actor, |reply| KvMessage::ApplyOp {
                                                    op,
                                                    priority,
                                                    is_replication: false,
                                                    reply: Some(reply),
                                                })
                                                .unwrap()
                                                .unwrap();
                                            assert_eq!(result, ApplyOutcome::Applied);
                                        }
                                        Request::Get(key) => {
                                            let value =
                                                ractor::call!(actor, |reply| KvMessage::Get {
                                                    key,
                                                    priority,
                                                    reply
                                                })
                                                .unwrap()
                                                .unwrap()
                                                .unwrap();
                                            assert_eq!(value.value.len(), size);
                                            std::hint::black_box(value);
                                        }
                                    }
                                });
                        join_all(requests).await;
                        epoch.fetch_add(1, Ordering::Relaxed);
                    }
                },
                BatchSize::PerIteration,
            );
        });
        let mut delays = rt.block_on(async {
            let _ = stop.send(());
            let delays = heartbeat.await.unwrap();
            actor.drain_and_wait(None).await.unwrap();
            handle.await.unwrap();
            shared.flush().await.unwrap();
            for (index, key) in keys.iter().enumerate() {
                let value = shared.get(key).await.unwrap();
                assert_eq!(value.value.len(), case.size);
                assert_eq!(
                    value.timestamp_ms,
                    if case.is_write(index) {
                        version.get()
                    } else {
                        0
                    }
                );
            }
            delays
        });
        if version.get() > 0 {
            delays.sort_unstable();
            let percentile = |p: usize| {
                delays
                    .get((delays.len().saturating_sub(1) * p) / 100)
                    .copied()
                    .unwrap_or_default()
                    .as_secs_f64()
                    * 1e6
            };
            let row = format!(
                "{{\"case\":\"{}\",\"threads\":{},\"depth\":{},\"size_bytes\":{},\"writes_per_batch\":{},\"buffered\":{},\"timer_samples\":{},\"timer_p50_us\":{:.3},\"timer_p99_us\":{:.3},\"timer_max_us\":{:.3}}}",
                case.id, case.threads, case.depth, case.size, case.writes(), case.buffered,
                delays.len(), percentile(50), percentile(99), percentile(100),
            );
            eprintln!("Timer delay: {row}");
            if let Ok(path) = std::env::var("ACTORDB_CACHE_LOCK_RESULTS") {
                writeln!(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .unwrap(),
                    "{row}"
                )
                .unwrap();
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_fragment_locks);
criterion_main!(benches);
