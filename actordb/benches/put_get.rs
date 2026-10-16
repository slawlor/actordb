// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Single-node `put`/`get` latency baselines (in-memory store, no network).
//!
//! What it measures
//! ----------------
//! * `put/<size>`: end-to-end latency of one `Node::put`, including the
//!   serving worker's exact apply outcome.
//! * `get/<size>`: end-to-end latency of one `Node::get` cache hit.
//! * `concurrent_get/{sticky,spread}/<depth>`: completion time and aggregate
//!   throughput for 1, 8, or 64 concurrent cache hits on one fragment. Sticky
//!   requests share one worker; spread requests cycle across its workers.
//!
//! Sequential cases run with 100 B, 10 KB and 1 MB values and report payload
//! bytes/s. Concurrent cases use 100 B values and report completed reads/s.
//!
//! How to read regressions
//! -----------------------
//! * A `put` regression with flat `get` points at dispatcher admission or
//!   worker apply.
//! * A sequential `get` regression points at dispatcher admission, sticky
//!   routing, worker execution, or the direct worker-to-caller reply.
//! * A concurrent sticky-only regression points at one worker's FIFO; a
//!   sticky-and-spread regression points at shared dispatcher/cache work.
//! * A size-proportional regression (100 B fine, 1 MB bad) points at
//!   per-byte copies in cache reads/insertion or protocol encoding.
//!
//! The node uses `NodeConfig::builder().build()` (in-memory, default worker pools,
//! no persistence). Nothing here touches the network or RocksDB.

use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use actordb::actor::worker_idx;
use actordb::node::{Node, NodeConfig};
use actordb::shard::{fragment_of, FragmentId};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::future::join_all;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn bench_put_get(c: &mut Criterion) {
    let rt = runtime();
    let config = NodeConfig::builder().build();
    let fragment_count = config.fragment_count;
    let default_workers = config.default_pool.workers.max(1);
    let node = rt.block_on(async {
        Node::start(
            "bench".to_string(),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            config,
        )
        .await
        .expect("node failed to start")
    });

    let mut group = c.benchmark_group("put_get");
    for &size in &[100usize, 10_000, 1_000_000] {
        let value = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));

        // --- put: cycle through a small key set (unbounded fresh keys would
        // OOM the bench at 1 MB values; overwrites exercise the same path).
        const PUT_KEYS: u64 = 100;
        let put_counter = Arc::new(AtomicU64::new(0));
        group.bench_with_input(BenchmarkId::new("put", size), &size, |b, _| {
            b.to_async(&rt).iter(|| {
                let node = &node;
                let value = value.clone();
                let put_counter = put_counter.clone();
                async move {
                    let i = put_counter.fetch_add(1, Ordering::Relaxed) % PUT_KEYS;
                    node.put(format!("bench-put-{size}-{i}"), value)
                        .await
                        .unwrap();
                }
            })
        });

        // --- get: pre-populate, then cycle reads over the key set.
        const N_KEYS: u64 = 1_000;
        rt.block_on(async {
            for i in 0..N_KEYS {
                node.put(format!("bench-get-{size}-{i}"), value.clone())
                    .await
                    .unwrap();
            }
        });
        let get_counter = Arc::new(AtomicU64::new(0));
        group.bench_with_input(BenchmarkId::new("get", size), &size, |b, _| {
            b.to_async(&rt).iter(|| {
                let node = &node;
                let get_counter = get_counter.clone();
                async move {
                    let i = get_counter.fetch_add(1, Ordering::Relaxed) % N_KEYS;
                    let got = node.get(format!("bench-get-{size}-{i}")).await.unwrap();
                    black_box(got);
                }
            })
        });
    }
    group.finish();

    let mut keys_by_worker = vec![None; default_workers];
    for candidate in 0u64.. {
        let key = format!("bench-concurrent-get-{candidate}");
        if fragment_of(&key, fragment_count) != FragmentId(0) {
            continue;
        }
        let slot = worker_idx(&key, default_workers);
        keys_by_worker[slot].get_or_insert(key);
        if keys_by_worker.iter().all(Option::is_some) {
            break;
        }
    }
    let spread_keys: Vec<String> = keys_by_worker.into_iter().flatten().collect();
    rt.block_on(async {
        for key in &spread_keys {
            node.put(key.clone(), vec![0xCD; 100]).await.unwrap();
        }
    });

    let mut concurrent = c.benchmark_group("concurrent_get");
    for (mode, keys) in [
        ("sticky", vec![spread_keys[0].clone()]),
        ("spread", spread_keys.clone()),
    ] {
        for depth in [1usize, 8, 64] {
            concurrent.throughput(Throughput::Elements(depth as u64));
            concurrent.bench_with_input(BenchmarkId::new(mode, depth), &depth, |b, &depth| {
                b.to_async(&rt).iter(|| {
                    let node = &node;
                    let keys = &keys;
                    async move {
                        let reads =
                            (0..depth).map(|index| node.get(keys[index % keys.len()].clone()));
                        for result in join_all(reads).await {
                            black_box(result.unwrap().expect("benchmark key disappeared"));
                        }
                    }
                })
            });
        }
    }
    concurrent.finish();

    rt.block_on(node.shutdown()).unwrap();
}

criterion_group!(benches, bench_put_get);
criterion_main!(benches);
