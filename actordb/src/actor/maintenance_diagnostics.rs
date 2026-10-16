// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Opt-in, descriptive latency samples, not a CI performance assertion.
//! Full-sweep baseline uses the retained public primitive; paged mode runs the
//! actual Cleaner. Both contend with actual dispatcher/worker foreground RPCs.

use super::*;
use crate::supervision::{Cleaner, CleanerArgs};
use ractor::rpc::CallResult;
use tokio::sync::Barrier;

const MAX_SAMPLES_PER_CLASS: usize = 20_000;

#[derive(Clone, Copy, Debug)]
enum Mode {
    FullSweep,
    AutomaticCleaner,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual release diagnostic; no shared-runner latency threshold"]
async fn maintenance_foreground_latency_distribution() {
    eprintln!("mode,keys,map_capacity,ttl_percent,materialization_us,total_through_drain_us,class,operations,stored_samples,p50_us,p95_us,p99_us,max_us");
    for cardinality in [10_000usize, 100_000] {
        for capacity_multiple in [1usize, 8] {
            for ttl_percent in [0usize, 1, 100] {
                for mode in [Mode::FullSweep, Mode::AutomaticCleaner] {
                    measure_case(cardinality, capacity_multiple, ttl_percent, mode).await;
                }
            }
        }
    }
}

async fn measure_case(
    cardinality: usize,
    capacity_multiple: usize,
    ttl_percent: usize,
    mode: Mode,
) {
    let mut values = HashMap::with_capacity(cardinality * capacity_multiple + 4);
    for i in 0..cardinality {
        let mut value = VersionedValue::new(vec![1; 32], 10, "seed".into());
        if i < cardinality * ttl_percent / 100 {
            value.expires_at_ms = Some(1);
        }
        values.insert(format!("k{i:08}"), value);
    }
    // A last ordered witness makes even the near-zero-expiry pass observable.
    // This is not a literal zero-expiry dataset: one witness always expires.
    let mut witness = VersionedValue::new(vec![1; 32], 10, "seed".into());
    witness.expires_at_ms = Some(1);
    values.insert("zz-witness".into(), witness);
    let classes = [
        ("high-hit", OpPriority::High, false, "foreground-high"),
        (
            "default-hit",
            OpPriority::Default,
            false,
            "foreground-default",
        ),
        (
            "overwrite",
            OpPriority::Default,
            true,
            "foreground-overwrite",
        ),
    ];
    for (_, _, _, key) in classes {
        values.insert(
            key.into(),
            VersionedValue::new(vec![1; 32], 10, "seed".into()),
        );
    }
    let map_capacity = values.capacity();
    let shared = Arc::new(FragmentState::new(
        values,
        None,
        usize::MAX,
        Duration::from_secs(3600),
    ));
    let pool = PoolConfig {
        workers: 2,
        queue_cap: 128,
        rate_limit_per_sec: None,
    };
    let (dispatcher, _) = KvActor::spawn(
        None,
        KvActor,
        KvArgs {
            shared: shared.clone(),
            fragment: FragmentId(0),
            worker_prefix: format!(
                "maintenance-diag-{cardinality}-{capacity_multiple}-{ttl_percent}-{mode:?}"
            ),
            default_pool: pool.clone(),
            high_pool: pool.clone(),
            background_pool: pool,
        },
    )
    .await
    .unwrap();
    let start_gate = Arc::new(Barrier::new(4));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut foreground = Vec::new();
    for (label, priority, write, key) in classes {
        let dispatcher = dispatcher.clone();
        let start_gate = start_gate.clone();
        let stop = stop.clone();
        foreground.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(MAX_SAMPLES_PER_CLASS);
            let mut operations = 0usize;
            start_gate.wait().await;
            while !stop.load(Ordering::Acquire) {
                let started = Instant::now();
                if write {
                    let outcome = dispatcher
                        .call(
                            |reply| KvMessage::ApplyOp {
                                op: ReplicatedOp {
                                    key: key.into(),
                                    value: VersionedValue::new(
                                        vec![2; 32],
                                        100 + operations as u64,
                                        "foreground".into(),
                                    ),
                                    seq: 0,
                                    boot_id: 0,
                                },
                                priority,
                                is_replication: false,
                                reply: Some(reply),
                            },
                            Some(Duration::from_secs(5)),
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        outcome,
                        CallResult::Success(Ok(ApplyOutcome::Applied))
                    ));
                } else {
                    let outcome = dispatcher
                        .call(
                            |reply| KvMessage::Get {
                                key: key.into(),
                                priority,
                                reply,
                            },
                            Some(Duration::from_secs(5)),
                        )
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CallResult::Success(Ok(Some(_)))));
                }
                if samples.len() < MAX_SAMPLES_PER_CLASS {
                    samples.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                }
                operations += 1;
            }
            (label, operations, samples)
        }));
    }
    start_gate.wait().await;
    let started = Instant::now();
    let mut full_sweep = None;
    let mut cleaner = None;
    match mode {
        Mode::FullSweep => {
            let shared = shared.clone();
            full_sweep = Some(tokio::spawn(async move {
                shared.expire_values(now_ms()).await.unwrap()
            }));
        }
        Mode::AutomaticCleaner => {
            cleaner = Some(
                Cleaner::spawn(
                    None,
                    Cleaner,
                    CleanerArgs {
                        fragment: FragmentId(0),
                        shared: shared.clone(),
                        interval: Duration::from_secs(3600),
                        tombstone_gc_age: None,
                        reset_senders: Arc::new(Mutex::new(HashMap::new())),
                    },
                )
                .await
                .unwrap()
                .0,
            );
        }
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if shared.cache.read().await.values["zz-witness"].is_tombstone {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let materialization = started.elapsed();
    stop.store(true, Ordering::Release);
    if let Some(full_sweep) = full_sweep {
        assert_eq!(
            full_sweep.await.unwrap(),
            cardinality * ttl_percent / 100 + 1
        );
    }
    if let Some(cleaner) = cleaner {
        cleaner.drain().unwrap();
        cleaner.wait(None).await.unwrap();
    }
    let mut results = Vec::new();
    for task in foreground {
        results.push(task.await.unwrap());
    }
    dispatcher.drain().unwrap();
    dispatcher.wait(None).await.unwrap();
    let total = started.elapsed();
    for (label, operations, mut samples) in results {
        samples.sort_unstable();
        let quantile = |percent: usize| {
            // One blocked RPC is a useful maximum observation, not evidence
            // for a meaningful tail distribution. Report sparse tails as NA.
            if (percent == 99 && samples.len() < 100) || (percent == 95 && samples.len() < 20) {
                return "na".into();
            }
            samples
                .get(samples.len().saturating_sub(1) * percent / 100)
                .map(|ns| format!("{:.3}", *ns as f64 / 1000.0))
                .unwrap_or_else(|| "na".into())
        };
        eprintln!("{mode:?},{cardinality},{map_capacity},{ttl_percent},{},{},{label},{operations},{},{},{},{},{}",
            materialization.as_micros(), total.as_micros(), samples.len(), quantile(50), quantile(95), quantile(99), quantile(100));
    }
}
