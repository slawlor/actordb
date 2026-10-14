// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Multi-key fan-out baselines across 3 in-process nodes.
//!
//! Topology
//! --------
//! Three nodes (`a`, `b`, `c`) on 127.0.0.1, fully meshed. Three fragments;
//! each node owns exactly one (`a`→0, `b`→1, `c`→2). Every `put_multi` /
//! `get_multi` issued on `a` therefore fans out to all three nodes: one
//! local fragment plus two remote batches.
//!
//! What it measures
//! ----------------
//! * `put_multi/<n>`: latency of one `put_multi` call with `n` keys
//!   (n = 10, 100, 1000), 100 B values. Includes grouping by destination,
//!   one message per destination, and per-key result collection.
//! * `get_multi/<n>`: latency of one `get_multi` call with `n` keys.
//!
//! Throughput is reported as elements/s (keys per second).
//!
//! How to read regressions
//! -----------------------
//! * A `put_multi` regression with flat single-`put` (see put_get bench)
//!   points at fan-out overhead: destination grouping, per-destination
//!   message encode, or the failover `replica_order` scan.
//! * A `get_multi` regression points at the remote batch path: note that
//!   `BatchGet` currently serves keys with sequential actor round-trips
//!   (adversarial finding #24) — batching that path should show up here
//!   as a large win, especially at n = 1000.
//! * Sub-linear scaling with `n` is expected (per-batch fixed costs);
//!   per-op latency *increasing* with `n` points at O(n²) behavior such
//!   as the per-key senders-map scan (finding #22).
//!
//! Known baseline anomaly (2026-10-14): `get_multi` has a ~100 ms floor
//! independent of `n`, even on an empty database. `put_multi` does not
//! (it is fire-and-forget for remote fragments) while `get_multi` waits
//! for `BatchGetResponse`. This matches adversarial finding #30 —
//! `TCP_NODELAY` is not set, so small request/response exchanges pay
//! Nagle + delayed-ACK (~40 ms per remote fragment round-trip). Fixing
//! #30 should drop this floor dramatically; this bench will show it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actordb::actor::PoolConfig;
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Start 3 meshed nodes, one fragment each. Returns (a, b, c).
async fn mesh() -> (Node, Node, Node) {
    let mut fragment_map = HashMap::new();
    fragment_map.insert(FragmentId(0), vec!["a".to_string()]);
    fragment_map.insert(FragmentId(1), vec!["b".to_string()]);
    fragment_map.insert(FragmentId(2), vec!["c".to_string()]);
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
        fragment_count: 3,
        fragment_map: fragment_map.clone(),
        ..NodeConfig::default()
    };
    let a = Node::start("a".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .expect("node a");
    let b = Node::start("b".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .expect("node b");
    let c = Node::start("c".to_string(), "127.0.0.1:0".parse().unwrap(), mk())
        .await
        .expect("node c");
    a.connect(b.addr()).await.unwrap();
    a.connect(c.addr()).await.unwrap();
    b.connect(c.addr()).await.unwrap();
    // Wait for the mesh to converge (each node sees 2 peers).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if a.peer_count().await == 2 && b.peer_count().await == 2 && c.peer_count().await == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(a.peer_count().await, 2, "mesh did not converge");
    // peer_count only means the handshake finished; probe that a real
    // cross-fragment put_multi succeeds before benchmarking.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let kvs = vec![("mesh-probe".to_string(), vec![0u8; 8])];
        if a.put_multi(kvs).await.iter().all(|r| r.is_ok()) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("mesh not ready for put_multi");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (a, b, c)
}

fn bench_fanout(c: &mut Criterion) {
    let rt = runtime();
    let (node_a, node_b, node_c) = rt.block_on(mesh());
    // All traffic is issued on `a`; b/c stay alive for the whole group.

    let mut group = c.benchmark_group("fanout");
    for &n in &[10usize, 100, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        let value = vec![0xCDu8; 100];

        // --- put_multi: fresh keys per iteration.
        let put_counter = Arc::new(AtomicU64::new(0));
        group.bench_with_input(BenchmarkId::new("put_multi", n), &n, |b, _| {
            b.to_async(&rt).iter_batched(
                || {
                    let base = put_counter.fetch_add(n as u64, Ordering::Relaxed);
                    (0..n)
                        .map(|i| (format!("fanout-{base}-{i}"), value.clone()))
                        .collect::<Vec<_>>()
                },
                |kvs| async {
                    let results = node_a.put_multi(kvs).await;
                    for (i, r) in results.iter().enumerate() {
                        if let Err(e) = r {
                            panic!("put_multi key {i} failed: {e:?}");
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });

        // --- get_multi: pre-populate n keys, then read the same set back.
        // (Cycle through a few pre-populated batches to keep it realistic.)
        const SETS: usize = 4;
        let key_sets: Vec<Vec<String>> = rt.block_on(async {
            let mut sets = Vec::with_capacity(SETS);
            for s in 0..SETS {
                let kvs: Vec<(String, Vec<u8>)> = (0..n)
                    .map(|i| (format!("fanout-get-{s}-{i}"), value.clone()))
                    .collect();
                let results = node_a.put_multi(kvs.clone()).await;
                assert!(results.iter().all(|r| r.is_ok()));
                sets.push(kvs.into_iter().map(|(k, _)| k).collect());
            }
            sets
        });
        let get_counter = Arc::new(AtomicU64::new(0));
        group.bench_with_input(BenchmarkId::new("get_multi", n), &n, |b, _| {
            b.to_async(&rt).iter_batched(
                || {
                    let s = get_counter.fetch_add(1, Ordering::Relaxed) as usize % SETS;
                    key_sets[s].clone()
                },
                |keys| async {
                    let results = node_a.get_multi(keys).await;
                    assert!(results.iter().all(|r| matches!(r, Ok(Some(_)))));
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    rt.block_on(node_a.shutdown());
    rt.block_on(node_b.shutdown());
    rt.block_on(node_c.shutdown());
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);
