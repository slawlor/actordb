// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Single-node `put`/`get` latency baselines (in-memory store, no network).
//!
//! What it measures
//! ----------------
//! * `put/<size>`: end-to-end latency of one `Node::put`, **including** the
//!   read-your-write barrier (the extra `Get` round-trip through the
//!   dispatcher). This is the cost every acknowledged write pays.
//! * `get/<size>`: end-to-end latency of one `Node::get` cache hit.
//!
//! Each case runs with 100 B, 10 KB and 1 MB values. Throughput is reported
//! as bytes/s of value payload.
//!
//! How to read regressions
//! -----------------------
//! * A `put` regression with flat `get` points at the write path: the
//!   barrier, dispatcher admission, or worker apply. See adversarial
//!   finding #14 (dispatcher as serial funnel; barrier doubles its load).
//! * A `get` regression points at the read path: dispatcher → worker
//!   round-trip (finding #36: Gets serialize inside the dispatcher loop).
//! * A size-proportional regression (100 B fine, 1 MB bad) points at
//!   per-byte copies: value clones on the barrier read, proto encode,
//!   or cache insertion.
//!
//! The node uses `NodeConfig::default()` (in-memory, default worker pools,
//! no persistence). Nothing here touches the network or RocksDB.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use actordb::node::{Node, NodeConfig};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn bench_put_get(c: &mut Criterion) {
    let rt = runtime();
    let node = rt.block_on(async {
        Node::start(
            "bench".to_string(),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            NodeConfig::default(),
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
                    criterion::black_box(got);
                }
            })
        });
    }
    group.finish();

    rt.block_on(node.shutdown());
}

criterion_group!(benches, bench_put_get);
criterion_main!(benches);
