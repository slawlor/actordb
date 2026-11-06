//! Manual, process-isolated fragment capacity calibration. Each case starts
//! real Node defaults and TCP-connected replicas; it measures resident cache
//! growth, idle CPU, point-operation CPU and latency, aggregate CPU and catchup.
//! No storage backend is attached: disk/stable-write costs remain assumptions.
//! Run optimized on representative hardware; see docs/placement-planning.md.

use actordb::node::{Node, NodeConfig};
use actordb::placement::planner::HostProfile;
use actordb::shard::FragmentId;
use actordb::write_concern::{WriteConcern, WriteConcernOptions};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use std::time::{Duration, Instant};

fn argument(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| default.into())
}
fn rss() -> Result<u64, Box<dyn Error>> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !output.status.success() {
        return Err("ps could not read process RSS".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().parse::<u64>()? * 1024)
}
#[cfg(unix)]
#[allow(unsafe_code)]
fn cpu_seconds() -> f64 {
    // getrusage reads this process's aggregate thread CPU, including Tokio and
    // all in-process replicas. The initialized struct is filled on success.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, &mut usage), 0);
        usage.ru_utime.tv_sec as f64
            + usage.ru_utime.tv_usec as f64 / 1_000_000.0
            + usage.ru_stime.tv_sec as f64
            + usage.ru_stime.tv_usec as f64 / 1_000_000.0
    }
}
#[cfg(not(unix))]
fn cpu_seconds() -> f64 {
    0.0
}

fn percentile(values: &mut [f64], fraction: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * fraction).ceil() as usize]
}

async fn case(
    fragments: u32,
    records: u32,
    value_bytes: usize,
    replicas: usize,
    samples: u32,
) -> Result<Value, Box<dyn Error>> {
    let baseline = rss()?;
    let ids: Vec<String> = (0..replicas).map(|i| format!("capacity-{i}")).collect();
    let map: HashMap<FragmentId, Vec<String>> = (0..fragments)
        .map(|f| (FragmentId(f), ids.clone()))
        .collect();
    let mut nodes = Vec::new();
    for id in &ids {
        nodes.push(
            Node::start(
                id.clone(),
                "127.0.0.1:0".parse()?,
                NodeConfig::builder()
                    .fragment_count(fragments as usize)
                    .fragment_map(map.clone())
                    .build(),
            )
            .await?,
        );
    }
    // Star connections suffice for source-to-all replication. These replicas
    // share one process; network traffic traverses actual loopback TCP.
    for node in nodes.iter().skip(1) {
        node.connect(nodes[0].addr()).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while nodes[0].peer_count().await != replicas - 1 {
        if Instant::now() >= deadline {
            return Err("peers did not connect".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Empty startup reconciliation is real work, not idle maintenance. Wait
    // for both directions to complete before sampling the idle baseline.
    loop {
        let mut ready = true;
        for fragment in 0..fragments {
            for (position, node) in nodes.iter().enumerate().skip(1) {
                ready &= nodes[0]
                    .sender_report(FragmentId(fragment), &ids[position])
                    .await
                    .is_some_and(|report| report.reconcile_rounds > 0);
                ready &= node
                    .sender_report(FragmentId(fragment), &ids[0])
                    .await
                    .is_some_and(|report| report.reconcile_rounds > 0);
            }
        }
        if ready {
            break;
        }
        if Instant::now() >= deadline {
            return Err("initial reconciliation did not settle".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let idle_rss = rss()?;
    let start = Instant::now();
    let cpu = cpu_seconds();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle_cpu = (cpu_seconds() - cpu) / start.elapsed().as_secs_f64();
    let policy = WriteConcernOptions::builder()
        .concern(WriteConcern::Count(replicas as u32))
        .build();
    for i in 0..records {
        nodes[0]
            .put(format!("capacity-key-{i:010}"), vec![1; value_bytes])
            .await?;
    }
    // Count with revision checks; retry transient QueryChanged during apply.
    let recovery_start = Instant::now();
    loop {
        let mut complete = true;
        for node in &nodes {
            let result = node.query("SELECT COUNT(*) FROM kv").await;
            complete &= result.is_ok_and(|r| {
                r.rows == vec![vec![actordb_query::QueryValue::Integer(records as i128)]]
            });
        }
        if complete {
            break;
        }
        if recovery_start.elapsed() > Duration::from_secs(30) {
            return Err("replicas did not catch up".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let loaded_rss = rss()?;
    let catchup_seconds = recovery_start.elapsed().as_secs_f64();
    let mut read_latency = Vec::new();
    let start = Instant::now();
    let cpu = cpu_seconds();
    for i in 0..samples {
        let operation = Instant::now();
        let value = nodes[0]
            .get(format!("capacity-key-{:010}", i % records))
            .await?;
        assert!(value.is_some());
        read_latency.push(operation.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let read_seconds = start.elapsed().as_secs_f64();
    let read_cpu_us = ((cpu_seconds() - cpu - idle_cpu * read_seconds).max(0.0) / samples as f64
        * 1_000_000.0)
        .max(1.0);
    let mut write_latency = Vec::new();
    let start = Instant::now();
    let cpu = cpu_seconds();
    for i in 0..samples {
        let operation = Instant::now();
        nodes[0]
            .put_with_write_concern(
                format!("capacity-key-{:010}", i % records),
                vec![2; value_bytes],
                policy.clone(),
            )
            .await?;
        write_latency.push(operation.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let write_seconds = start.elapsed().as_secs_f64();
    let write_cpu_us = ((cpu_seconds() - cpu - idle_cpu * write_seconds).max(0.0)
        / (samples as f64 * replicas as f64)
        * 1_000_000.0)
        .max(1.0);
    let start = Instant::now();
    let cpu = cpu_seconds();
    for _ in 0..10 {
        nodes[0].query("SELECT COUNT(*) FROM kv").await?;
    }
    let query_seconds = start.elapsed().as_secs_f64();
    let query_cpu_us =
        (cpu_seconds() - cpu - idle_cpu * query_seconds).max(0.0) * 1_000_000.0 / 10.0;
    let mut mixed_reads = Vec::new();
    let mut mixed_writes = Vec::new();
    let mut mixed_query_failures = 0;
    for i in 0..samples {
        let read = async {
            let start = Instant::now();
            nodes[0]
                .get(format!("capacity-key-{:010}", i % records))
                .await?;
            Ok::<f64, actordb::node::NodeError>(start.elapsed().as_secs_f64() * 1_000_000.0)
        };
        let write = async {
            let start = Instant::now();
            nodes[0]
                .put_with_write_concern(
                    format!("capacity-key-{:010}", i % records),
                    vec![3; value_bytes],
                    policy.clone(),
                )
                .await?;
            Ok::<f64, actordb::write_concern::WriteConcernError>(
                start.elapsed().as_secs_f64() * 1_000_000.0,
            )
        };
        let (read, write, query) =
            tokio::join!(read, write, nodes[0].query("SELECT COUNT(*) FROM kv"));
        mixed_reads.push(read?);
        mixed_writes.push(write?);
        if query.is_err() {
            mixed_query_failures += 1;
        }
    }
    let mut visibility_lag = Vec::new();
    let mut recovery_seconds = None;
    let mut recovery_cpu_us = None;
    if replicas > 1 {
        for i in 0..samples {
            let key = format!("capacity-key-{:010}", i % records);
            nodes[0].put(key.clone(), vec![4; value_bytes]).await?;
            let local_done = Instant::now();
            let expected = nodes[0]
                .get(key.clone())
                .await?
                .expect("applied source value");
            loop {
                let mut visible = true;
                for node in nodes.iter().skip(1) {
                    visible &= node
                        .get(key.clone())
                        .await?
                        .is_some_and(|value| value == expected);
                }
                if visible {
                    break;
                }
                if local_done.elapsed() > Duration::from_secs(30) {
                    return Err("replica visibility timed out".into());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            visibility_lag.push(local_done.elapsed().as_secs_f64() * 1_000_000.0);
        }
        // Restart one storeless replica with the same identity. Recovery must
        // reconstruct the entire copy from the still-serving source, then
        // finish a verified reconciliation pass for every fragment.
        let target = nodes.pop().expect("replica exists");
        target.shutdown().await?;
        let disconnected = Instant::now();
        while nodes[0].peer_count().await != replicas - 2 {
            if disconnected.elapsed() > Duration::from_secs(30) {
                return Err("old replica connection did not retire".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = Instant::now();
        let cpu = cpu_seconds();
        let replacement = Node::start(
            ids[replicas - 1].clone(),
            "127.0.0.1:0".parse()?,
            NodeConfig::builder()
                .fragment_count(fragments as usize)
                .fragment_map(map.clone())
                .build(),
        )
        .await?;
        replacement.connect(nodes[0].addr()).await?;
        loop {
            let count_ready = replacement
                .query("SELECT COUNT(*) FROM kv")
                .await
                .is_ok_and(|result| {
                    result.rows == vec![vec![actordb_query::QueryValue::Integer(records as i128)]]
                });
            let mut rounds_ready = true;
            for fragment in 0..fragments {
                rounds_ready &= replacement
                    .sender_report(FragmentId(fragment), &ids[0])
                    .await
                    .is_some_and(|report| report.reconcile_rounds > 0);
            }
            if count_ready && rounds_ready {
                break;
            }
            if started.elapsed() > Duration::from_secs(30) {
                return Err("restarted replica did not recover every fragment".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        recovery_seconds = Some(started.elapsed().as_secs_f64());
        recovery_cpu_us = Some((cpu_seconds() - cpu) * 1_000_000.0);
        // Verify payload/version equality at representative endpoints as well
        // as record count and completed revision-stable rounds.
        for index in [0, records - 1] {
            let key = format!("capacity-key-{index:010}");
            if replacement.get(key.clone()).await? != nodes[0].get(key).await? {
                return Err("recovered replica differs from the source version".into());
            }
        }
        nodes.push(replacement);
    }
    let result = json!({
        "fragments_per_host":fragments,"replicas":replicas,"records_per_copy":records,"value_bytes":value_bytes,
        "rss_baseline_bytes":baseline,"rss_idle_bytes":idle_rss,"rss_loaded_bytes":loaded_rss,
        "idle_cpu_cores_per_host":idle_cpu/replicas as f64,
        "point_read_cpu_us":read_cpu_us,"write_cpu_us_per_replica":write_cpu_us,
        "point_read_qps_single_caller":samples as f64/read_seconds,
        "write_qps_single_caller_all_replica_ack":samples as f64/write_seconds,
        "read_p50_us":percentile(&mut read_latency,0.50),"read_p99_us":percentile(&mut read_latency,0.99),
        "write_p50_us":percentile(&mut write_latency,0.50),"write_p99_us":percentile(&mut write_latency,0.99),
        "aggregate_cpu_us":query_cpu_us,"aggregate_latency_us":query_seconds*100_000.0,
        "initial_catchup_seconds_after_source_apply":catchup_seconds,
        "mixed_read_p99_us":percentile(&mut mixed_reads,0.99),
        "mixed_write_p99_us":percentile(&mut mixed_writes,0.99),
        "mixed_query_failures":mixed_query_failures,
        "replica_visibility_p50_us":if visibility_lag.is_empty() { None } else { Some(percentile(&mut visibility_lag,0.50)) },
        "replica_visibility_p99_us":if visibility_lag.is_empty() { None } else { Some(percentile(&mut visibility_lag,0.99)) },
        "restarted_replica_recovery_seconds":recovery_seconds,
        "restarted_replica_recovery_cpu_us":recovery_cpu_us,
    });
    for node in nodes {
        node.shutdown().await?;
    }
    Ok(result)
}

fn main() -> Result<(), Box<dyn Error>> {
    let records: u32 = argument("--records", "10000").parse()?;
    let value_bytes: usize = argument("--value-bytes", "1024").parse()?;
    let replicas: usize = argument("--replicas", "2").parse()?;
    let samples: u32 = argument("--samples", "100").parse()?;
    if records == 0 || value_bytes == 0 || !(1..=16).contains(&replicas) || samples == 0 {
        return Err(
            "records, value size and samples must be nonzero; replicas must be 1..=16".into(),
        );
    }
    let single = argument("--case", "");
    if !single.is_empty() {
        let fragments: u32 = single.parse()?;
        if !(1..=4096).contains(&fragments) {
            return Err("fragments must be 1..=4096".into());
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        println!(
            "{}",
            rt.block_on(case(fragments, records, value_bytes, replicas, samples))?
        );
        return Ok(());
    }
    let mut measurements = Vec::new();
    for fragments in argument("--fragments", "1,8,32,128").split(',') {
        eprintln!("Measuring {fragments} fragments per host...");
        let output = Command::new(std::env::current_exe()?)
            .args([
                "--case",
                fragments,
                "--records",
                &records.to_string(),
                "--value-bytes",
                &value_bytes.to_string(),
                "--replicas",
                &replicas.to_string(),
                "--samples",
                &samples.to_string(),
            ])
            .output()?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
        }
        measurements.push(serde_json::from_slice::<Value>(&output.stdout)?);
    }
    let max = |field: &str| {
        measurements
            .iter()
            .map(|m| m[field].as_f64().unwrap())
            .fold(0.0, f64::max)
    };
    let mut profile = HostProfile {
        calibration:format!("partial local calibration: {} / {}; {} replicas, {} records, {} B values, {} samples; disk/stable costs and unmeasured overhead remain assumptions",
            std::env::consts::OS,std::env::consts::ARCH,replicas,records,value_bytes,samples),
        point_read_cpu_us:max("point_read_cpu_us").max(HostProfile::default().point_read_cpu_us),
        write_cpu_us:max("write_cpu_us_per_replica").max(HostProfile::default().write_cpu_us),
        ..HostProfile::default()
    };
    // Conservative upper envelopes avoid treating allocator/noise floors as
    // proof of zero runtime or metadata costs. Shared-process RSS is divided
    // across hosts; use isolated real hosts to validate deployment ceilings.
    for m in &measurements {
        let fragments = m["fragments_per_host"].as_u64().unwrap();
        let idle_bytes = m["rss_idle_bytes"]
            .as_u64()
            .unwrap()
            .saturating_sub(m["rss_baseline_bytes"].as_u64().unwrap());
        let loaded_bytes = m["rss_loaded_bytes"]
            .as_u64()
            .unwrap()
            .saturating_sub(m["rss_idle_bytes"].as_u64().unwrap());
        profile.fragment_overhead_bytes = profile
            .fragment_overhead_bytes
            .max(idle_bytes / (fragments * replicas as u64));
        profile.fragment_idle_cpu_cores = profile
            .fragment_idle_cpu_cores
            .max(m["idle_cpu_cores_per_host"].as_f64().unwrap() / fragments as f64);
        let record_bytes = 23.0 + value_bytes as f64 + profile.record_metadata_bytes as f64;
        profile.resident_data_multiplier = profile
            .resident_data_multiplier
            .max(loaded_bytes as f64 / (records as f64 * replicas as f64 * record_bytes));
        // Aggregate includes per-fragment coordination; charging all of it to
        // examined records is a conservative partial CPU calibration.
        profile.examined_record_cpu_us = profile
            .examined_record_cpu_us
            .max(m["aggregate_cpu_us"].as_f64().unwrap() / records as f64);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"host_profile":profile,"measurements":measurements}))?
    );
    Ok(())
}
