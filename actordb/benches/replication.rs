// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Replication lag baselines: 2 in-process nodes, one shared fragment.
//!
//! Topology
//! --------
//! Nodes `a` (primary) and `b` (replica) both serve fragment 0
//! (`fragment_map = {0: [a, b]}`). Writes go to `a` via `put_multi`
//! (exact source apply completed); the async sender replicates to `b`.
//!
//! What it measures
//! ----------------
//! `replication_lag/<n>`: wall time from the moment the batch of `n` puts
//! completes its source apply on `a` until all `n` keys are visible on `b` via
//! `get_multi`. This does not observe the replication ACK itself. Sender
//! batching (`batch_interval`, `batch_size`), network encode, receiver apply,
//! and `b`'s read are all in the measured path.
//!
//! How to read regressions
//! -----------------------
//! * Lag growing with `n` faster than linearly points at per-op receiver
//!   costs (e.g. per-op `hlc.observe()` acquisitions, finding #23) or
//!   `max_in_flight` throttling (finding #25: 4 in-flight batches cap
//!   throughput at ~400 ops/sender batch quantum).
//! * A flat floor independent of `n` is the sender `batch_interval`
//!   timer (default 100 ms): even a full batch waits for the tick.
//!   Shipping full batches immediately should lower this floor.
//! * This bench polls `b` every 10 ms, so measurements have ~10 ms of
//!   quantization noise — compare runs, not absolute values, below ~50 ms.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actordb::actor::PoolConfig;
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn bench_replication(c: &mut Criterion) {
    let rt = runtime();
    let (node_a, node_b) = rt.block_on(async {
        let mut fragment_map = HashMap::new();
        fragment_map.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
        // High queue caps: criterion hammers iterations back-to-back; we want
        // to measure the op path, not the backpressure shed path.
        let big_pool = || PoolConfig {
            workers: 4,
            queue_cap: 1_000_000,
            rate_limit_per_sec: None,
        };
        let mk = || NodeConfig {
            default_pool: big_pool(),
            high_pool: big_pool(),
            background_pool: big_pool(),
            fragment_count: 1,
            fragment_map: fragment_map.clone(),
            ..NodeConfig::default()
        };
        let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
            .await
            .expect("node a");
        let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
            .await
            .expect("node b");
        a.connect(b.addr()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if a.peer_count().await == 1 && b.peer_count().await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(a.peer_count().await, 1, "peers did not connect");
        // Probe that replication actually flows before benchmarking.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let kvs = vec![("repl-probe".to_string(), vec![0u8; 8])];
            let keys = vec!["repl-probe".to_string()];
            if a.put_multi(kvs).await.iter().all(|r| r.is_ok()) {
                let res = b.get_multi(keys).await;
                if res.iter().all(|r| matches!(r, Ok(Some(_)))) {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("replication not flowing");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (a, b)
    });

    let mut group = c.benchmark_group("replication");
    // Fewer samples: each iteration does real network replication.
    group.sample_size(20);
    for &n in &[10usize, 100, 1000] {
        let value = vec![0xEFu8; 100];
        let counter = Arc::new(AtomicU64::new(0));
        group.bench_with_input(BenchmarkId::new("replication_lag", n), &n, |b, _| {
            b.to_async(&rt).iter_batched(
                || {
                    let base = counter.fetch_add(n as u64, Ordering::Relaxed);
                    (0..n)
                        .map(|i| (format!("repl-{base}-{i}"), value.clone()))
                        .collect::<Vec<_>>()
                },
                |kvs| async {
                    let keys: Vec<String> = kvs.iter().map(|(k, _)| k.clone()).collect();
                    let results = node_a.put_multi(kvs).await;
                    assert!(results.iter().all(|r| r.is_ok()));
                    // Batch acknowledged on `a`; now measure visibility on `b`.
                    let start = Instant::now();
                    loop {
                        let res = node_b.get_multi(keys.clone()).await;
                        let visible = res.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
                        if visible == n {
                            break;
                        }
                        if start.elapsed() > Duration::from_secs(30) {
                            let n_err = res.iter().filter(|r| r.is_err()).count();
                            panic!("replication timed out for batch of {n}: visible={visible}/{n} errs={n_err}");
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    start.elapsed()
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    rt.block_on(node_a.shutdown());
    rt.block_on(node_b.shutdown());
}

criterion_group!(benches, bench_replication);
criterion_main!(benches);
