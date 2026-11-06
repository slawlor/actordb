//! Plaintext loopback RPC CPU upper bound: client and server share one process.
use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientOptions};
use serde_json::json;
use std::error::Error;
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
fn key(index: usize) -> String {
    format!("key{index:010}{}", "x".repeat(19))
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
fn percentile(samples: &mut [f64], fraction: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[((samples.len() - 1) as f64 * fraction).ceil() as usize]
}

async fn run() -> Result<serde_json::Value, Box<dyn Error>> {
    let records: usize = argument("--records", "20000").parse()?;
    let value_bytes: usize = argument("--value-bytes", "1024").parse()?;
    let concurrency: usize = argument("--concurrency", "16").parse()?;
    let duration_ms: u64 = argument("--duration-ms", "2000").parse()?;
    if records < concurrency
        || records > 100000
        || !(1..=32).contains(&concurrency)
        || value_bytes > 16384
        || duration_ms == 0
    {
        return Err("invalid RPC probe parameters".into());
    }
    let node = Arc::new(
        Node::start(
            "rpc-sizing".into(),
            "127.0.0.1:0".parse()?,
            NodeConfig::builder().fragment_count(32).build(),
        )
        .await?,
    );
    for index in 0..records {
        node.put(key(index), vec![7; value_bytes]).await?;
    }
    let mut server = GrpcServer::bind(
        node.clone(),
        "127.0.0.1:0".parse()?,
        GrpcServerOptions {
            max_decoded_request_bytes: 16 * 1024 * 1024,
            receive_memory_bytes: 512 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await?;
    let client = Arc::new(
        Client::connect(
            format!("http://{}", server.addr()),
            ClientOptions::builder()
                .max_decoded_response_bytes(16 * 1024 * 1024)
                .max_response_memory_bytes(512 * 1024 * 1024)
                .build(),
        )
        .await?,
    );
    client.get(key(0)).await?;
    let idle_start = Instant::now();
    let cpu = cpu_seconds();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle_cpu = (cpu_seconds() - cpu) / idle_start.elapsed().as_secs_f64();
    let mut phases = Vec::new();
    for kind in ["read", "write"] {
        let barrier = Arc::new(Barrier::new(concurrency + 1));
        let started = Arc::new(OnceLock::<Instant>::new());
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..concurrency {
            let client = client.clone();
            let barrier = barrier.clone();
            let started = started.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                let end = *started.get().unwrap() + Duration::from_millis(duration_ms);
                let mut count = 0;
                let mut latency = Vec::new();
                while Instant::now() < end {
                    let index = worker + (count % (records / concurrency)) * concurrency;
                    let start = Instant::now();
                    if kind == "read" {
                        client
                            .get(key(index))
                            .await
                            .map_err(|e| e.to_string())?
                            .ok_or("missing key")?;
                    } else {
                        client
                            .put(key(index), vec![7; value_bytes])
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    if latency.len() < 4096 {
                        latency.push(start.elapsed().as_secs_f64() * 1e6);
                    }
                    count += 1;
                }
                Ok::<_, String>((count, latency))
            });
        }
        let cpu = cpu_seconds();
        let start = Instant::now();
        started.set(start).unwrap();
        barrier.wait().await;
        let mut operations = 0;
        let mut latency = Vec::new();
        while let Some(task) = tasks.join_next().await {
            let (count, samples) = task?.map_err(std::io::Error::other)?;
            operations += count;
            latency.extend(samples);
        }
        let seconds = start.elapsed().as_secs_f64();
        phases.push(json!({"kind":kind,"operations":operations,"operations_per_second":operations as f64/seconds,
            "combined_cpu_us_per_operation":(cpu_seconds()-cpu-idle_cpu*seconds).max(0.0)*1e6/operations as f64,
            "p50_us":percentile(&mut latency,0.5),"p99_us":percentile(&mut latency,0.99)}));
    }
    client.close().await;
    server.shutdown().await?;
    node.shutdown().await?;
    Ok(
        json!({"records":records,"key_bytes":32,"value_bytes":value_bytes,"concurrency":concurrency,"runtime_threads":4,
        "transport":"plaintext loopback gRPC, reused channel, client and server CPU combined","client_response_reservation_bytes":16*1024*1024,"server_request_reservation_bytes":16*1024*1024,"server_receive_allowance_bytes":512*1024*1024,
        "idle_cpu_cores":idle_cpu,"phases":phases}),
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
