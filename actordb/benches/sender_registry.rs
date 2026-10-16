// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Replication sender-registry contention baselines.
//!
//! Two in-process nodes own every fragment, so each source mutation on `a`
//! performs local apply plus fan-out to one sender. Cases compare a one-entry
//! registry with 64 entries, use 1/32/64 concurrent writes (spread across the
//! available fragments), and cover 100 B and 1 MiB values. The measured
//! duration ends when every source apply has completed; exact-generation
//! replica visibility is awaited outside that duration before the next
//! iteration so sender overflow/reset does not contaminate the result. Each
//! case reuses a fixed key set, keeping retained cache memory bounded.
//!
//! Before sender-handle snapshotting, every write cloned its value and entered
//! the per-sender bounded mailbox while holding the node-global registry. A
//! regression that grows sharply with value size, registry cardinality, or
//! cross-fragment concurrency points back at work leaking into that critical
//! section.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actordb::actor::PoolConfig;
use actordb::node::{Node, NodeConfig, NodeRuntimeOptions};
use actordb::replication::SenderConfig;
use actordb::shard::{fragment_of, FragmentId};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::future::join_all;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn key_for_fragment(fragment_count: usize, target: usize, seed: u64) -> String {
    for nonce in 0u64.. {
        let key = format!("sender-registry-{seed}-{target}-{nonce}");
        if fragment_of(&key, fragment_count) == FragmentId(target as u32) {
            return key;
        }
    }
    unreachable!()
}

async fn replicated_pair(fragment_count: usize) -> (Node, Node) {
    let replicas = vec!["a".to_string(), "b".to_string()];
    let fragment_map = (0..fragment_count)
        .map(|fragment| (FragmentId(fragment as u32), replicas.clone()))
        .collect::<HashMap<_, _>>();
    let workers = if fragment_count == 1 { 64 } else { 1 };
    let pool = || {
        PoolConfig::builder()
            .workers(workers)
            .queue_cap(100_000)
            .build()
    };
    let config = || {
        NodeConfig::builder()
            .default_pool(pool())
            .high_pool(pool())
            .background_pool(pool())
            .fragment_count(fragment_count)
            .fragment_map(fragment_map.clone())
            .replication(
                SenderConfig::builder()
                    .batch_interval(Duration::from_millis(1))
                    .max_in_flight(64)
                    .buffer_capacity(100_000)
                    .build(),
            )
            .build()
    };
    let runtime_options = NodeRuntimeOptions::builder()
        .peer_queue_frame_capacity(4096)
        .peer_queue_byte_capacity(256 * 1024 * 1024)
        .build();
    let a = Node::start_with_options(
        "a".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        runtime_options.clone(),
    )
    .await
    .expect("node a");
    let b = Node::start_with_options(
        "b".to_string(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        runtime_options,
    )
    .await
    .expect("node b");
    a.connect(b.addr()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while a.peer_count().await != 1 || b.peer_count().await != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "peers did not connect"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let probe = key_for_fragment(fragment_count, 0, 0);
    a.put(probe.clone(), vec![0; 8]).await.unwrap();
    loop {
        if b.get(probe.clone()).await.unwrap().is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replication did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (a, b)
}

fn bench_sender_registry(c: &mut Criterion) {
    let rt = runtime();
    for fragment_count in [1usize, 64] {
        let (source, replica) = rt.block_on(replicated_pair(fragment_count));
        let mut group = c.benchmark_group(format!("sender_registry/{fragment_count}_fragments"));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));
        let counter = Arc::new(AtomicU64::new(1));
        for value_bytes in [100usize, 1024 * 1024] {
            for concurrency in [1usize, 32, 64] {
                group.throughput(Throughput::Bytes(
                    (value_bytes.saturating_mul(concurrency)) as u64,
                ));
                let keys: Arc<Vec<String>> = Arc::new(
                    (0..concurrency)
                        .map(|index| {
                            key_for_fragment(fragment_count, index % fragment_count, index as u64)
                        })
                        .collect(),
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("{value_bytes}_bytes"), concurrency),
                    &concurrency,
                    |b, &concurrency| {
                        let source = &source;
                        let replica = &replica;
                        let counter = counter.clone();
                        let keys = keys.clone();
                        b.to_async(&rt).iter_custom(move |iterations| {
                            let counter = counter.clone();
                            let keys = keys.clone();
                            async move {
                                let mut measured = Duration::ZERO;
                                for _ in 0..iterations {
                                    let generation = counter.fetch_add(1, Ordering::Relaxed);
                                    let mut value = vec![0x5a; value_bytes];
                                    let generation_bytes = generation.to_be_bytes();
                                    let marker_bytes = generation_bytes.len().min(value.len());
                                    value[..marker_bytes]
                                        .copy_from_slice(&generation_bytes[..marker_bytes]);
                                    let writes: Vec<(String, Vec<u8>)> = keys
                                        .iter()
                                        .map(|key| (key.clone(), value.clone()))
                                        .collect();

                                    let started = Instant::now();
                                    let results = join_all(
                                        writes
                                            .into_iter()
                                            .map(|(key, value)| source.put(key, value)),
                                    )
                                    .await;
                                    measured += started.elapsed();
                                    assert!(results.iter().all(Result::is_ok));

                                    let deadline = Instant::now() + Duration::from_secs(30);
                                    loop {
                                        let visible = replica.get_multi(keys.as_ref().clone()).await;
                                        if visible.iter().all(|result| {
                                            matches!(result, Ok(Some(versioned)) if versioned.value == value)
                                        })
                                        {
                                            break;
                                        }
                                        assert!(
                                            Instant::now() < deadline,
                                            "replica did not catch up between samples"
                                        );
                                        tokio::time::sleep(Duration::from_millis(1)).await;
                                    }
                                    for target in 0..concurrency.min(fragment_count) {
                                        let report = source
                                            .sender_report(FragmentId(target as u32), "b")
                                            .await
                                            .expect("replication sender disappeared");
                                        assert_ne!(
                                            report.state, "buffer_reset",
                                            "benchmark overflowed sender ingress"
                                        );
                                    }
                                }
                                measured
                            }
                        });
                    },
                );
            }
        }
        group.finish();
        rt.block_on(source.shutdown()).unwrap();
        rt.block_on(replica.shutdown()).unwrap();
    }
}

criterion_group!(benches, bench_sender_registry);
criterion_main!(benches);
