//! Process-isolated sizing measurements. Run one case per fresh process; see
//! docs/benchmarks/laptop-sizing.md for the experiment matrix and limitations.

use actordb::actor::{KeyMatcher, OpPriority, PoolConfig, ScanSpec, ValueTransform};
use actordb::node::{Node, NodeConfig};
use actordb::shard::fragment_of_literal;
use actordb::write_concern::{WriteConcernOptions, WriteDurability};
use serde_json::{json, Value};
use std::error::Error;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

fn argument(name: &str, default: &str) -> String {
    std::env::args()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| default.into())
}

fn rss() -> Result<u64, Box<dyn Error>> {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !out.status.success() {
        return Err("process RSS unavailable".into());
    }
    Ok(String::from_utf8(out.stdout)?.trim().parse::<u64>()? * 1024)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn cpu_seconds() -> f64 {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, &mut usage), 0);
        usage.ru_utime.tv_sec as f64
            + usage.ru_utime.tv_usec as f64 / 1e6
            + usage.ru_stime.tv_sec as f64
            + usage.ru_stime.tv_usec as f64 / 1e6
    }
}
#[cfg(not(unix))]
fn cpu_seconds() -> f64 {
    0.0
}

fn key(index: usize, bytes: usize) -> String {
    let prefix = format!("key{index:010}");
    format!("{prefix}{}", "x".repeat(bytes - prefix.len()))
}

// A small bank of deterministic high-entropy values prevents benchmarks of
// repetitive zero bytes from masquerading as uncompressed storage capacity.
fn payloads(bytes: usize) -> Arc<Vec<Vec<u8>>> {
    Arc::new(
        (0..32u64)
            .map(|index| {
                let mut state = index + 0x9e3779b97f4a7c15;
                (0..bytes)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state as u8
                    })
                    .collect()
            })
            .collect(),
    )
}

fn percentile(samples: &mut [f64], fraction: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[((samples.len() - 1) as f64 * fraction).ceil() as usize]
}

#[derive(Clone)]
struct OperationConfig {
    records: usize,
    key_bytes: usize,
    concurrency: usize,
    duration: Duration,
}

async fn operations(
    node: Arc<Node>,
    bank: Arc<Vec<Vec<u8>>>,
    kind: &'static str,
    config: &OperationConfig,
    idle_cpu: f64,
) -> Result<Value, Box<dyn Error>> {
    let barrier = Arc::new(Barrier::new(config.concurrency + 1));
    let started = Arc::new(OnceLock::<Instant>::new());
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..config.concurrency {
        let node = node.clone();
        let bank = bank.clone();
        let barrier = barrier.clone();
        let started = started.clone();
        let config = config.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let end = *started.get().expect("start published before barrier") + config.duration;
            let policy = match kind {
                "store_write" => Some(
                    WriteConcernOptions::builder()
                        .minimum_durability(WriteDurability::StoreCommitted)
                        .build(),
                ),
                "stable_write" => Some(
                    WriteConcernOptions::builder()
                        .minimum_durability(WriteDurability::StableCommitted)
                        .build(),
                ),
                _ => None,
            };
            let mut count = 0usize;
            let mut attempts = 0usize;
            let mut rejected = 0usize;
            let mut latencies = Vec::new();
            while Instant::now() < end {
                let index = worker
                    + (attempts % (config.records / config.concurrency)) * config.concurrency;
                let key = key(index, config.key_bytes);
                let start = Instant::now();
                let result = if kind == "read" {
                    node.get(key)
                        .await
                        .map(|value| value.is_some())
                        .map_err(|e| e.to_string())
                } else if let Some(policy) = &policy {
                    node.put_with_write_concern(
                        key,
                        bank[index % bank.len()].clone(),
                        policy.clone(),
                    )
                    .await
                    .map(|_| true)
                    .map_err(|e| e.to_string())
                } else {
                    node.put(key, bank[index % bank.len()].clone())
                        .await
                        .map(|_| true)
                        .map_err(|e| e.to_string())
                };
                attempts += 1;
                match result {
                    Ok(true) => {}
                    Ok(false) => return Err("missing loaded key".to_string()),
                    Err(error) if error.contains("backpressure") => {
                        rejected += 1;
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                if latencies.len() < 4096 {
                    latencies.push(start.elapsed().as_secs_f64() * 1e6);
                }
                count += 1;
            }
            Ok::<_, String>((count, latencies, rejected, attempts))
        });
    }
    let before_cpu = cpu_seconds();
    let before = Instant::now();
    started.set(before).expect("one start");
    barrier.wait().await;
    let mut count = 0;
    let mut rejected = 0;
    let mut attempts = 0;
    let mut latencies = Vec::new();
    while let Some(task) = tasks.join_next().await {
        let (operations, samples, shed, total) = task?.map_err(std::io::Error::other)?;
        count += operations;
        rejected += shed;
        attempts += total;
        latencies.extend(samples);
    }
    let seconds = before.elapsed().as_secs_f64();
    let raw_cpu = cpu_seconds() - before_cpu;
    if count == 0 {
        return Err("no completed operations".into());
    }
    Ok(json!({
        "kind":kind,"operations":count,"attempts":attempts,"rejected":rejected,"seconds":seconds,"operations_per_second":count as f64/seconds,
        "raw_cpu_seconds":raw_cpu,"cpu_us_per_operation":(raw_cpu-idle_cpu*seconds).max(0.0)*1e6/count as f64,
        "p50_us":percentile(&mut latencies,0.50),"p99_us":percentile(&mut latencies,0.99),
        "latency_samples":latencies.len(),"concurrency":config.concurrency,
    }))
}

#[cfg(feature = "rocksdb")]
fn disk_bytes(path: &std::path::Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

async fn run() -> Result<Value, Box<dyn Error>> {
    let mode = argument("--mode", "memory");
    let backend = argument("--backend", "none");
    let fragments: usize = argument("--fragments", "32").parse()?;
    let records: usize = argument("--records", "20000").parse()?;
    let key_bytes: usize = argument("--key-bytes", "32").parse()?;
    let value_bytes: usize = argument("--value-bytes", "1024").parse()?;
    let tables: usize = argument("--tables", "1").parse()?;
    let duration_ms: u64 = argument("--duration-ms", "2000").parse()?;
    let idle_ms: u64 = argument("--idle-ms", "1000").parse()?;
    let concurrency: usize = argument("--concurrency", "8").parse()?;
    let query_samples: usize = argument("--query-samples", "10").parse()?;
    if !["memory", "ops", "query"].contains(&mode.as_str())
        || !["none", "sync", "async"].contains(&backend.as_str())
        || !(1..=512).contains(&fragments)
        || !(1..=16).contains(&tables)
        || !(16..=256).contains(&key_bytes)
        || value_bytes > 1024 * 1024
        || records > 250_000
        || records.saturating_mul(value_bytes) > 1024 * 1024 * 1024
        || idle_ms == 0
        || duration_ms == 0
        || !(1..=64).contains(&concurrency)
        || query_samples == 0
        || (mode == "ops" && records == 0)
        || (mode == "ops" && records < concurrency)
    {
        return Err("invalid or excessive sizing-probe parameters".into());
    }
    let baseline_rss = rss()?;
    #[cfg(feature = "rocksdb")]
    let directory = tempfile::tempdir()?;
    let mut config = match backend.as_str() {
        "none" => NodeConfig::default(),
        #[cfg(feature = "rocksdb")]
        name => NodeConfig::with_rocksdb_options(
            directory.path(),
            actordb::store::RocksDbOptions::default().with_durability(if name == "sync" {
                actordb::store::RocksDbDurability::SyncWal
            } else {
                actordb::store::RocksDbDurability::AsyncWal
            }),
        )?,
        #[cfg(not(feature = "rocksdb"))]
        _ => return Err("build with --features rocksdb for a storage probe".into()),
    };
    config.fragment_count = fragments;
    config.table_pool_capacity = tables;
    // CPU calibration measures service cost without the configurable 1000/s
    // background admission limit. This is recorded explicitly in the report.
    config.background_pool = PoolConfig::builder().workers(2).queue_cap(5000).build();
    let node = Arc::new(Node::start("sizing-probe".into(), "127.0.0.1:0".parse()?, config).await?);
    let names: Vec<String> = (0..tables)
        .map(|i| {
            if i == 0 {
                "KV".into()
            } else {
                format!("TABLE{i}")
            }
        })
        .collect();
    for fragment in 0..fragments {
        let probe = (0usize..)
            .find(|i| fragment_of_literal(&key(*i, key_bytes), fragments).0 as usize == fragment)
            .expect("hash reaches fragment");
        for name in names.iter().skip(1) {
            node.table(name)?
                .put(key(probe, key_bytes), Vec::new())
                .await?;
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let idle_rss = rss()?;
    let before = Instant::now();
    let before_cpu = cpu_seconds();
    tokio::time::sleep(Duration::from_millis(idle_ms)).await;
    let idle_cpu = (cpu_seconds() - before_cpu) / before.elapsed().as_secs_f64();
    let bank = payloads(value_bytes);
    let mut last = vec![None; fragments];
    let started = Instant::now();
    let load_cpu = cpu_seconds();
    for index in 0..records {
        let key = key(index, key_bytes);
        last[fragment_of_literal(&key, fragments).0 as usize] = Some(index);
        node.table(&names[index % tables])?
            .put(&key, bank[index % bank.len()].clone())
            .await?;
    }
    if backend != "none" {
        let policy = WriteConcernOptions::builder()
            .minimum_durability(WriteDurability::StableCommitted)
            .build();
        for index in last.into_iter().flatten() {
            node.table(&names[index % tables])?
                .put_with_write_concern_options(
                    key(index, key_bytes),
                    bank[index % bank.len()].clone(),
                    None,
                    OpPriority::Default,
                    policy.clone(),
                )
                .await?;
        }
    }
    let load_seconds = started.elapsed().as_secs_f64();
    let load_cpu_seconds = cpu_seconds() - load_cpu;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let loaded_rss = rss()?;
    #[cfg(feature = "rocksdb")]
    let disk_before = if backend == "none" {
        None
    } else {
        Some(disk_bytes(directory.path())?)
    };
    #[cfg(not(feature = "rocksdb"))]
    let disk_before: Option<u64> = None;
    let mut phases = Vec::new();
    if mode == "ops" {
        if tables != 1 {
            return Err("operation calibration currently uses one KV table".into());
        }
        let operation = OperationConfig {
            records,
            key_bytes,
            concurrency,
            duration: Duration::from_millis(duration_ms),
        };
        for kind in ["read", "write"] {
            phases.push(operations(node.clone(), bank.clone(), kind, &operation, idle_cpu).await?);
        }
        if backend != "none" {
            for kind in ["store_write", "stable_write"] {
                phases.push(
                    operations(node.clone(), bank.clone(), kind, &operation, idle_cpu).await?,
                );
            }
        }
    }
    if mode == "query" {
        let begin = key(records / 2, key_bytes);
        let end = key((records / 2 + 128).min(records), key_bytes);
        for (kind, sql) in [
            ("count", "SELECT COUNT(*) FROM kv".into()),
            (
                "range",
                format!("SELECT key FROM kv WHERE key >= '{begin}' AND key < '{end}'"),
            ),
        ] {
            let started = Instant::now();
            let cpu = cpu_seconds();
            let mut latency = Vec::new();
            let mut rows = 0;
            for _ in 0..query_samples {
                let before = Instant::now();
                let result = node.query(&sql).await?;
                rows = result.rows.len();
                latency.push(before.elapsed().as_secs_f64() * 1e6);
            }
            let seconds = started.elapsed().as_secs_f64();
            phases.push(json!({"kind":kind,"queries":query_samples,"cpu_us_per_query":(cpu_seconds()-cpu-idle_cpu*seconds).max(0.0)*1e6/query_samples as f64,
                "p50_us":percentile(&mut latency,0.5),"p99_us":percentile(&mut latency,0.99),"rows_returned":rows,
                "records_examined":if kind=="count" { records.div_ceil(tables) } else { 128.min(records-records/2).div_ceil(tables) }}));
        }
        let started = Instant::now();
        let cpu = cpu_seconds();
        let mut latency = Vec::new();
        let mut rows = 0;
        for _ in 0..query_samples {
            let before = Instant::now();
            rows = node
                .scan(&ScanSpec {
                    matcher: KeyMatcher::All,
                    transform: ValueTransform::PrefixBytes(8),
                    limit: 128,
                    offset: 0,
                })
                .await?
                .len();
            latency.push(before.elapsed().as_secs_f64() * 1e6);
        }
        let seconds = started.elapsed().as_secs_f64();
        phases.push(json!({"kind":"scan","queries":query_samples,"cpu_us_per_query":(cpu_seconds()-cpu-idle_cpu*seconds).max(0.0)*1e6/query_samples as f64,
            "p50_us":percentile(&mut latency,0.5),"p99_us":percentile(&mut latency,0.99),"rows_returned":rows,"records_examined":rows}));
    }
    node.shutdown().await?;
    #[cfg(feature = "rocksdb")]
    let disk_after = if backend == "none" {
        None
    } else {
        Some(disk_bytes(directory.path())?)
    };
    #[cfg(not(feature = "rocksdb"))]
    let disk_after: Option<u64> = None;
    Ok(
        json!({"mode":mode,"backend":backend,"fragments":fragments,"records":records,"key_bytes":key_bytes,"value_bytes":value_bytes,"resident_tables":tables,
        "seeded_table_records":fragments*(tables-1),"runtime_threads":4,"background_rate_limit":null,"entropy":"32 high-entropy values; independently compressed blocks",
        "baseline_rss_bytes":baseline_rss,"idle_rss_bytes":idle_rss,"loaded_rss_bytes":loaded_rss,"payload_bank_bytes":32*value_bytes,
        "idle_cpu_cores":idle_cpu,"load_seconds":load_seconds,"load_cpu_seconds":load_cpu_seconds,
        "disk_bytes_before_shutdown":disk_before,"disk_bytes_after_shutdown":disk_after,"phases":phases}),
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    println!("{}", serde_json::to_string(&runtime.block_on(run())?)?);
    Ok(())
}
