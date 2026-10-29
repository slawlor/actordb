// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Manual release diagnostic for #190. It exercises the production snapshot
//! helper without exposing private Node internals to an external benchmark.
//! Lookup cases use real sender handles cloned into synthetic unrelated slots;
//! Node cases use real fragment actors, senders, TCP replication and ACKs.

use super::*;
use futures::future::join_all;
use std::hint::black_box;
use std::time::Instant;

fn bench_option(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("invalid benchmark option")
    })
}

async fn sender_setup_state(source: &Node, replica: &Node) -> (usize, usize) {
    let mut complete = 0;
    let mut total = 0;
    for node in [source, replica] {
        let reports: Vec<_> = node.sender_reports.lock().await.values().cloned().collect();
        for report in reports {
            total += 1;
            let report = report.lock().await;
            complete += usize::from(
                report.state == "connected"
                    && report.reconcile_rounds > 0
                    && report.progress.reconciliation_stage.is_none()
                    && report.buffered == 0
                    && report.in_flight == 0,
            );
        }
    }
    (complete, total)
}

fn key_for_fragment(fragment_count: usize, target: usize, seed: usize) -> String {
    for nonce in 0u64.. {
        let key = format!("sender-index-{target}-{seed}-{nonce}");
        if fragment_of(&key, fragment_count) == FragmentId(target as u32) {
            return key;
        }
    }
    unreachable!()
}

async fn lookup_sweep() {
    let node = Node::start(
        "lookup".into(),
        "127.0.0.1:0".parse().unwrap(),
        NodeConfig::builder()
            .fragment_map(HashMap::from([(
                FragmentId(0),
                vec!["lookup".into(), "b".into(), "c".into()],
            )]))
            .build(),
    )
    .await
    .unwrap();
    let handles = snapshot_senders_for_fragment(&node.senders, FragmentId(0)).await;
    assert_eq!(handles.len(), 2);
    for fragment_count in [1usize, 64, 1024, 8192] {
        let registry: SenderMap = Arc::new(Mutex::new(Default::default()));
        for fragment in 0..fragment_count {
            for (peer, sender) in handles.iter().enumerate() {
                registry.lock().await.insert(
                    (FragmentId(fragment as u32), format!("peer-{peer}")),
                    sender.clone(),
                );
            }
        }
        for concurrency in [1usize, 32] {
            let iterations = 1_000_000 / concurrency;
            let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
            let tasks: Vec<_> = (0..concurrency)
                .map(|writer| {
                    let registry = registry.clone();
                    let barrier = barrier.clone();
                    tokio::spawn(async move {
                        barrier.wait().await;
                        for iteration in 0..iterations {
                            let fragment =
                                FragmentId(((writer + iteration) % fragment_count) as u32);
                            let targets = snapshot_senders_for_fragment(&registry, fragment).await;
                            assert_eq!(targets.len(), 2);
                            black_box(targets);
                        }
                    })
                })
                .collect();
            let started = Instant::now();
            barrier.wait().await;
            for task in tasks {
                task.await.unwrap();
            }
            let nanos = started.elapsed().as_nanos() as f64;
            let operations = (iterations * concurrency) as f64;
            println!(
                "SENDER_BENCH,lookup,{fragment_count},2,{concurrency},0,{:.3},{:.3},0,0",
                operations * 1e9 / nanos,
                nanos / operations,
            );
        }
    }
    node.shutdown().await.unwrap();
}

async fn replicated_pair(fragment_count: usize) -> (Node, Node) {
    let fragment_map = (0..fragment_count)
        .map(|fragment| (FragmentId(fragment as u32), vec!["a".into(), "b".into()]))
        .collect::<HashMap<_, _>>();
    // Fixed workers, runtime threads, fan-out and active-fragment count.
    let pool = || PoolConfig::builder().workers(1).queue_cap(100_000).build();
    let config = || {
        NodeConfig::builder()
            .default_pool(pool())
            .high_pool(pool())
            .background_pool(pool())
            .fragment_count(fragment_count)
            .fragment_map(fragment_map.clone())
            .replication(
                SenderConfig::builder()
                    .batch_interval(Duration::from_millis(bench_option(
                        "ACTORDB_SENDER_BENCH_TICK_MS",
                        1,
                    ) as u64))
                    .consistency_interval(Duration::from_secs(3600))
                    .max_in_flight(64)
                    .buffer_capacity(100_000)
                    .build(),
            )
            .build()
    };
    let options = NodeRuntimeOptions::builder()
        .peer_queue_frame_capacity(4096)
        .peer_queue_byte_capacity(256 * 1024 * 1024)
        .build();
    let a = Node::start_with_options(
        "a".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        options.clone(),
    )
    .await
    .unwrap();
    let b = Node::start_with_options(
        "b".into(),
        "127.0.0.1:0".parse().unwrap(),
        config(),
        options,
    )
    .await
    .unwrap();
    a.connect(b.addr()).await.unwrap();
    (a, b)
}

async fn node_sweep(isolated_registry: bool) {
    let mut counts = if isolated_registry {
        vec![8usize, 64, 1024, 8192]
    } else {
        vec![1, 64, 512]
    };
    if let Ok(value) = std::env::var("ACTORDB_SENDER_BENCH_FRAGMENTS") {
        counts = vec![value.parse().expect("invalid fragment count")];
    }
    for registry_fragment_count in counts {
        let fragment_count = if isolated_registry {
            8
        } else {
            registry_fragment_count
        };
        let setup_started = Instant::now();
        let (source, replica) = replicated_pair(fragment_count).await;
        if bench_option("ACTORDB_SENDER_BENCH_SETTLE", 0) != 0 {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let (complete, total) = sender_setup_state(&source, &replica).await;
                    if complete == total && total == 2 * fragment_count {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("initial reconciliation did not finish");
        }
        println!(
            "SENDER_SETUP,{fragment_count},ready_seconds,{:.6}",
            setup_started.elapsed().as_secs_f64()
        );
        if isolated_registry {
            // Finish both Hello handlers before injecting aliases: connection
            // setup enumerates all peer senders, which would otherwise notify
            // the same task repeatedly and contaminate this lookup experiment.
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let mut ready = true;
                    for fragment in 0..fragment_count {
                        for (node, peer) in [(&source, "b"), (&replica, "a")] {
                            ready &= node
                                .sender_report(FragmentId(fragment as u32), peer)
                                .await
                                .unwrap()
                                .state
                                == "connected";
                        }
                    }
                    if ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("replication sessions did not become ready");
            // Synthetic unrelated slots isolate the map scan from actor count
            // and idle sender timers. Only the eight real fragments receive
            // mutations. Aliased handles are never offered unrelated writes.
            let handle = snapshot_senders_for_fragment(&source.senders, FragmentId(0))
                .await
                .pop()
                .unwrap();
            let mut registry = source.senders.lock().await;
            for fragment in fragment_count..registry_fragment_count {
                registry.insert((FragmentId(fragment as u32), "b".into()), handle.clone());
            }
        }
        for (value_bytes, concurrency) in [(100usize, 1usize), (100, 32), (16_384, 32)] {
            if std::env::var("ACTORDB_SENDER_BENCH_CONCURRENCY").is_ok()
                && concurrency != bench_option("ACTORDB_SENDER_BENCH_CONCURRENCY", 32)
            {
                continue;
            }
            if std::env::var("ACTORDB_SENDER_BENCH_BYTES").is_ok()
                && value_bytes != bench_option("ACTORDB_SENDER_BENCH_BYTES", 100)
            {
                continue;
            }
            if value_bytes != 100
                && if isolated_registry {
                    registry_fragment_count != 8 && registry_fragment_count != 8192
                } else {
                    fragment_count == 64
                }
            {
                continue;
            }
            let keys: Vec<_> = (0..concurrency)
                .map(|index| key_for_fragment(fragment_count, index % fragment_count.min(8), index))
                .collect();
            let sample_count = bench_option("ACTORDB_SENDER_BENCH_SAMPLES", 400);
            let warmup = bench_option("ACTORDB_SENDER_BENCH_WARMUP", 50);
            let mut samples = Vec::with_capacity(sample_count);
            for generation in 0u64..(sample_count + warmup) as u64 {
                if generation == warmup as u64 {
                    let (complete, total) = sender_setup_state(&source, &replica).await;
                    println!("SENDER_SETUP,{fragment_count},{value_bytes},{concurrency},reconciled,{complete},{total}");
                }
                let mut value = vec![0x5a; value_bytes];
                value[..8].copy_from_slice(&generation.to_be_bytes());
                let writes: Vec<_> = keys
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
                let elapsed = started.elapsed().as_nanos() as f64;
                assert!(results.iter().all(Result::is_ok));
                if generation >= warmup as u64 {
                    samples.push(elapsed);
                }
                // Exact-generation replica visibility is outside the timer.
                // Drain before the next batch so bounded-ingress overflow and
                // reset cannot inflate measured source throughput.
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let visible = replica.get_multi(keys.clone()).await;
                    if visible.iter().all(
                        |result| matches!(result, Ok(Some(versioned)) if versioned.value == value),
                    ) {
                        break;
                    }
                    assert!(Instant::now() < deadline, "replication did not catch up");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            for fragment in 0..concurrency.min(fragment_count.min(8)) {
                let report = source
                    .sender_report(FragmentId(fragment as u32), "b")
                    .await
                    .unwrap();
                assert_ne!(
                    report.state, "buffer_reset",
                    "benchmark overflowed sender ingress"
                );
            }
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;
            if let Ok(path) = std::env::var("ACTORDB_SENDER_BENCH_SAMPLES_FILE") {
                std::fs::write(
                    path,
                    samples
                        .iter()
                        .map(|sample| format!("{sample:.0}\n"))
                        .collect::<String>(),
                )
                .unwrap();
            }
            samples.sort_by(f64::total_cmp);
            let kind = if isolated_registry {
                "node-isolated"
            } else {
                "node"
            };
            println!(
                "SENDER_BENCH,{kind},{registry_fragment_count},1,{concurrency},{value_bytes},{:.3},{:.3},{:.3},{:.3}",
                concurrency as f64 * 1e9 / mean, mean,
                samples[samples.len()/2], samples[samples.len()*95/100],
            );
        }
        source.shutdown().await.unwrap();
        replica.shutdown().await.unwrap();
    }
}

async fn write_chunk(
    source: &Node,
    replica: &Node,
    keys: &[String],
    value_bytes: usize,
    generation: &mut u64,
    batches: usize,
) -> Vec<f64> {
    let mut prepared = Vec::with_capacity(batches);
    let mut expected = Vec::new();
    for _ in 0..batches {
        *generation += 1;
        let mut value = vec![0x5a; value_bytes];
        value[..8].copy_from_slice(&generation.to_be_bytes());
        prepared.push(
            keys.iter()
                .map(|key| (key.clone(), value.clone()))
                .collect::<Vec<_>>(),
        );
        expected = value;
    }
    let mut samples = Vec::with_capacity(batches);
    for writes in prepared {
        let started = Instant::now();
        let results = join_all(
            writes
                .into_iter()
                .map(|(key, value)| source.put(key, value)),
        )
        .await;
        samples.push(started.elapsed().as_nanos() as f64);
        assert!(results.iter().all(Result::is_ok));
    }
    // Catch up once per block. The next timed batch is not coupled to every
    // preceding replication timer/ACK wave. Exact visibility plus idle sender
    // ingress/deques and zero cumulative overflow certify this workload.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let visible = replica.get_multi(keys.to_vec()).await;
            let mut idle = visible
                .iter()
                .all(|result| matches!(result, Ok(Some(versioned)) if versioned.value == expected));
            // All active fragments must have drained before the next mode.
            let active: HashSet<_> = keys
                .iter()
                .map(|key| fragment_of(key, source.router.fragment_count))
                .collect();
            for fragment in active {
                let handles = snapshot_senders_for_fragment(&source.senders, fragment).await;
                for sender in handles {
                    assert_eq!(sender.ingress_overflow_counts_for_test(), (0, 0));
                    idle &= sender.ingress_is_idle();
                }
                let report = source.sender_report(fragment, "b").await.unwrap();
                idle &= report.buffered == 0 && report.in_flight == 0;
                assert_ne!(report.state, "buffer_reset");
            }
            if idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("replication did not drain after continuous writes");
    samples
}

/// Switch only registry lookup inside one running node pair. Both layouts
/// retain exactly the same task handles; actor/session/timer state is fixed.
async fn paired_comparison() {
    let fragment_count = bench_option("ACTORDB_SENDER_BENCH_FRAGMENTS", 512);
    let concurrency = bench_option("ACTORDB_SENDER_BENCH_CONCURRENCY", 32);
    let value_bytes = bench_option("ACTORDB_SENDER_BENCH_BYTES", 100);
    let samples_per_block = bench_option("ACTORDB_SENDER_BENCH_SAMPLES", 128);
    let warmup = bench_option("ACTORDB_SENDER_BENCH_WARMUP", 16);
    let control = bench_option("ACTORDB_SENDER_BENCH_CONTROL", 0) != 0;
    let continuous = bench_option("ACTORDB_SENDER_BENCH_CONTINUOUS", 1) != 0;
    for trial in 0..bench_option("ACTORDB_SENDER_BENCH_TRIALS", 4) {
        let (source, replica) = replicated_pair(fragment_count).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let (complete, total) = sender_setup_state(&source, &replica).await;
                if complete == total && total == 2 * fragment_count {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("initial reconciliation did not finish");
        let keys: Vec<_> = (0..concurrency)
            .map(|index| key_for_fragment(fragment_count, index % fragment_count.min(8), index))
            .collect();
        let mut generation = 0u64;
        let warmup_started = Instant::now();
        let mut warmup_chunks = 0usize;
        while warmup_started.elapsed()
            < Duration::from_millis(bench_option("ACTORDB_SENDER_BENCH_PREROLL_MS", 2000) as u64)
        {
            for node in [&source, &replica] {
                node.senders
                    .lock()
                    .await
                    .benchmark_flat_lookup(warmup_chunks.is_multiple_of(2) && !control);
            }
            write_chunk(
                &source,
                &replica,
                &keys,
                value_bytes,
                &mut generation,
                warmup + samples_per_block,
            )
            .await;
            warmup_chunks += 1;
        }
        // ABBA then BAAB: balance order both within and across each trial.
        for (block, flat_label) in [true, false, false, true, false, true, true, false]
            .into_iter()
            .enumerate()
        {
            let flat_label = if trial.is_multiple_of(2) {
                flat_label
            } else {
                !flat_label
            };
            for node in [&source, &replica] {
                node.senders
                    .lock()
                    .await
                    .benchmark_flat_lookup(flat_label && !control);
            }
            let mut samples = Vec::with_capacity(samples_per_block);
            if continuous {
                samples = write_chunk(
                    &source,
                    &replica,
                    &keys,
                    value_bytes,
                    &mut generation,
                    warmup + samples_per_block,
                )
                .await;
                samples.drain(..warmup);
            } else {
                for iteration in 0..(warmup + samples_per_block) {
                    generation += 1;
                    let mut value = vec![0x5a; value_bytes];
                    value[..8].copy_from_slice(&generation.to_be_bytes());
                    let writes: Vec<_> = keys
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
                    let nanos = started.elapsed().as_nanos() as f64;
                    assert!(results.iter().all(Result::is_ok));
                    if iteration >= warmup {
                        samples.push(nanos);
                    }
                    let deadline = Instant::now() + Duration::from_secs(30);
                    loop {
                        let visible = replica.get_multi(keys.clone()).await;
                        if visible.iter().all(
                        |result| matches!(result, Ok(Some(versioned)) if versioned.value == value),
                    ) {
                        break;
                    }
                        assert!(Instant::now() < deadline, "replication did not catch up");
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;
            let layout = if flat_label { "flat" } else { "index" };
            let kind = if control { "control" } else { "paired" };
            println!("SENDER_PAIR,{kind},{trial},{block},{layout},{fragment_count},{concurrency},{value_bytes},{:.3},{:.3}", concurrency as f64 * 1e9 / mean, mean);
            if let Ok(directory) = std::env::var("ACTORDB_SENDER_BENCH_SAMPLES_DIR") {
                let path = std::path::Path::new(&directory)
                    .join(format!("{kind}-{trial}-{block}-{layout}.samples"));
                std::fs::write(
                    path,
                    samples
                        .iter()
                        .map(|sample| format!("{sample:.0}\n"))
                        .collect::<String>(),
                )
                .unwrap();
            }
        }
        for node in [&source, &replica] {
            for fragment in 0..fragment_count.min(8) {
                assert_ne!(
                    node.sender_report(
                        FragmentId(fragment as u32),
                        if std::ptr::eq(node, &source) {
                            "b"
                        } else {
                            "a"
                        }
                    )
                    .await
                    .unwrap()
                    .state,
                    "buffer_reset"
                );
            }
            node.shutdown().await.unwrap();
        }
    }
}

#[test]
#[ignore = "manual release performance diagnostic; see benches/README.md"]
fn sender_registry_scaling() {
    if cfg!(debug_assertions) {
        panic!("run this diagnostic with --release");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(bench_option("ACTORDB_SENDER_BENCH_RT_THREADS", 4))
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        println!("SENDER_BENCH,kind,fragments,fanout,concurrency,value_bytes,ops_per_sec,mean_ns,p50_batch_ns,p95_batch_ns");
        match std::env::var("ACTORDB_SENDER_BENCH_MODE").as_deref() {
            Ok("lookup") => lookup_sweep().await,
            Ok("node") => node_sweep(false).await,
            Ok("isolated") => node_sweep(true).await,
            Ok("paired") => paired_comparison().await,
            _ => {
                lookup_sweep().await;
                node_sweep(false).await;
                node_sweep(true).await;
            }
        }
    });
}
