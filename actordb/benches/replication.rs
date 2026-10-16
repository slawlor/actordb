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
//! `buffered_catchup/<n>`: a fresh source buffers `n` compact writes while its
//! replica is disconnected. Timing starts immediately before connect and ends
//! when the source observes the replication ACK. A 4,096-op batch therefore
//! exercises the production batch builder without the default 100 ms timer or
//! the visibility polling in `replication_lag` dominating the result.
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
//! * `buffered_catchup` uses fresh nodes per sample and a 1 ms batch interval.
//!   Setup, preload, verification, and shutdown are outside its reported
//!   duration; connection establishment and the receiver apply remain inside.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actordb::actor::PoolConfig;
use actordb::node::{Node, NodeConfig};
use actordb::replication::SenderConfig;
use actordb::shard::FragmentId;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn large_pool() -> PoolConfig {
    PoolConfig {
        workers: 4,
        queue_cap: 1_000_000,
        rate_limit_per_sec: None,
    }
}

fn catchup_config(replicas: Vec<String>) -> NodeConfig {
    let mut fragment_map = HashMap::new();
    fragment_map.insert(FragmentId(0), replicas);
    NodeConfig {
        default_pool: large_pool(),
        high_pool: large_pool(),
        background_pool: large_pool(),
        fragment_count: 1,
        fragment_map,
        replication: SenderConfig {
            batch_size: 4096,
            batch_interval: Duration::from_millis(1),
            max_in_flight: 8,
            buffer_capacity: 8192,
            consistency_interval: Duration::from_secs(3600),
            ..SenderConfig::default()
        },
        ..NodeConfig::default()
    }
}

async fn buffered_catchup_sample(n: usize) -> Duration {
    let source = Node::start(
        "catchup-a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        catchup_config(vec!["catchup-a".to_string(), "catchup-b".to_string()]),
    )
    .await
    .expect("catch-up source");
    let expected = (0..n)
        .map(|index| {
            (
                format!("catchup-{index:04}"),
                (index as u64).to_le_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let results = source.put_multi(expected.clone()).await;
    assert!(results.iter().all(Result::is_ok));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if source
            .sender_report(FragmentId(0), "catchup-b")
            .await
            .is_some_and(|report| {
                report.state == "disconnected" && report.buffered == n && report.in_flight == 0
            })
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "source did not buffer {n} disconnected writes"
        );
        tokio::task::yield_now().await;
    }

    // The target-only placement prevents a reverse sender/reconcile workload
    // from contaminating this one-direction catch-up measurement.
    let target = Node::start(
        "catchup-b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        catchup_config(vec!["catchup-b".to_string()]),
    )
    .await
    .expect("catch-up target");

    let started = Instant::now();
    source.connect(target.addr()).await.expect("connect target");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if source
            .sender_report(FragmentId(0), "catchup-b")
            .await
            .is_some_and(|report| {
                report.session_id != 0
                    && report.buffered == 0
                    && report.in_flight == 0
                    && report.last_ack_durability.is_some()
            })
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "source did not receive the catch-up ACK for {n} writes"
        );
        tokio::task::yield_now().await;
    }
    let elapsed = started.elapsed();

    let keys = expected
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let values = target.get_multi(keys).await;
    assert_eq!(values.len(), expected.len());
    for (result, (_, expected_value)) in values.into_iter().zip(&expected) {
        let actual = result.unwrap().expect("replicated value");
        assert_eq!(actual.value.as_slice(), expected_value.as_slice());
    }

    source.shutdown().await.expect("source shutdown");
    target.shutdown().await.expect("target shutdown");
    elapsed
}

fn bench_replication(c: &mut Criterion) {
    let rt = runtime();
    let (node_a, node_b) = rt.block_on(async {
        let mut fragment_map = HashMap::new();
        fragment_map.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
        // High queue caps: criterion hammers iterations back-to-back; we want
        // to measure the op path, not the backpressure shed path.
        let mk = || NodeConfig {
            default_pool: large_pool(),
            high_pool: large_pool(),
            background_pool: large_pool(),
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

    rt.block_on(node_a.shutdown()).unwrap();
    rt.block_on(node_b.shutdown()).unwrap();

    let mut catchup = c.benchmark_group("replication_catchup");
    catchup.sample_size(10);
    for &n in &[64usize, 256, 1024, 4096] {
        catchup.bench_with_input(BenchmarkId::new("buffered_catchup", n), &n, |b, &n| {
            b.to_async(&rt).iter_custom(|iterations| async move {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    measured += buffered_catchup_sample(n).await;
                }
                measured
            });
        });
    }
    catchup.finish();
}

criterion_group!(benches, bench_replication);
criterion_main!(benches);
