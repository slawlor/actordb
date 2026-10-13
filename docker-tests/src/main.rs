//! Dockerized multi-node integration tests for actordb, modeled on
//! ractor's `ractor_cluster_integration_tests`.
//!
//! Each container runs one role: it starts a real [`Node`], dials its
//! peers over the compose network (DNS names like `node-a:7447`), runs a
//! scripted scenario with generous timeouts, and exits 0 on success.
//!
//! Roles:
//! - `replicate-writer` / `replicate-reader`: 3-node convergence.
//! - `restart-crashing` / `restart-survivor`: SIGKILL a node mid-cluster,
//!   restart it, assert it reconverges (exercises redial + `boot_id`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use actordb::node::Node;
use clap::{Parser, Subcommand};

/// generous per-step timeouts: CI runners and cold docker builds are slow
const DIAL_TIMEOUT: Duration = Duration::from_secs(60);
const PEER_TIMEOUT: Duration = Duration::from_secs(90);
const VALUE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Parser, Debug)]
#[command(
    name = "actordb-docker-test",
    about = "Runs one node's role in a dockerized actordb integration scenario"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Wait for peers, write one key, exit 0.
    ReplicateWriter {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        expect_peers: usize,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
    },
    /// Dial peers, wait for one key to converge, exit 0.
    ReplicateReader {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        peer: Vec<String>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
        #[arg(long, default_value_t = 90)]
        timeout_secs: u64,
    },
    /// Write `put_value`, wait for SIGKILL; on reboot assert `expect_value`.
    RestartCrashing {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        peer: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        put_value: String,
        #[arg(long)]
        expect_value: String,
        /// sentinel file proving this container already ran once
        #[arg(long)]
        sentinel: PathBuf,
    },
    /// Wait for `put_value`, write `expect_value`, watch the peer die and return.
    RestartSurvivor {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        key: String,
        #[arg(long)]
        put_value: String,
        #[arg(long)]
        expect_value: String,
    },
}

type TestResult = Result<(), String>;

fn fail(msg: impl Into<String>) -> TestResult {
    Err(msg.into())
}

async fn wait_for_peers(node: &Node, n: usize) -> TestResult {
    tokio::time::timeout(PEER_TIMEOUT, async {
        while node.peer_count().await < n {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| format!("timed out waiting for {n} peers"))?;
    println!("peers: {}", node.peer_count().await);
    Ok(())
}

/// Dial `peer` (`host:port`, DNS allowed), retrying until it answers.
async fn dial_with_retry(node: &Node, peer: &str) -> TestResult {
    let start = Instant::now();
    loop {
        match tokio::net::lookup_host(peer).await {
            Ok(addrs) => {
                for addr in addrs {
                    match node.connect(addr).await {
                        Ok(()) => {
                            println!("dialed {addr}");
                            return Ok(());
                        }
                        Err(e) => eprintln!("dial {addr} failed: {e}"),
                    }
                }
            }
            Err(e) => eprintln!("DNS lookup for {peer} failed: {e}"),
        }
        if start.elapsed() > DIAL_TIMEOUT {
            return fail(format!("timed out dialing {peer}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_value(node: &Node, key: &str, value: &[u8], timeout: Duration) -> TestResult {
    tokio::time::timeout(timeout, async {
        loop {
            match node.get(key.to_string()).await {
                Ok(Some(v)) if v.value == value => break,
                Ok(_) => {}
                Err(e) => eprintln!("get failed: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| format!("timed out waiting for key {key:?}"))?;
    println!("converged: {key:?}");
    Ok(())
}

fn bind(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}

async fn replicate_writer(
    node_id: String,
    port: u16,
    expect_peers: usize,
    key: String,
    value: String,
) -> TestResult {
    let node = Node::start(node_id, bind(port))
        .await
        .map_err(|e| format!("start failed: {e}"))?;
    wait_for_peers(&node, expect_peers).await?;
    node.put(key.clone(), value.clone().into_bytes())
        .await
        .map_err(|e| format!("put failed: {e}"))?;
    println!("WRITER_DONE key={key:?}");
    // let gossip settle before this container exits
    tokio::time::sleep(Duration::from_secs(5)).await;
    node.shutdown().await;
    Ok(())
}

async fn replicate_reader(
    node_id: String,
    port: u16,
    peers: Vec<String>,
    key: String,
    value: String,
    timeout_secs: u64,
) -> TestResult {
    let node = Node::start(node_id, bind(port))
        .await
        .map_err(|e| format!("start failed: {e}"))?;
    for peer in &peers {
        dial_with_retry(&node, peer).await?;
    }
    wait_for_value(
        &node,
        &key,
        value.as_bytes(),
        Duration::from_secs(timeout_secs),
    )
    .await?;
    println!("READER_OK key={key:?}");
    node.shutdown().await;
    Ok(())
}

async fn restart_crashing(
    node_id: String,
    port: u16,
    peer: String,
    key: String,
    put_value: String,
    expect_value: String,
    sentinel: PathBuf,
) -> TestResult {
    let node = Node::start(node_id, bind(port))
        .await
        .map_err(|e| format!("start failed: {e}"))?;
    dial_with_retry(&node, &peer).await?;
    wait_for_peers(&node, 1).await?;

    if sentinel.exists() {
        // rebooted after the SIGKILL: the survivor should have written
        // expect_value while we were gone; sync on handshake delivers it
        println!("REBOOTED, waiting for convergence");
        wait_for_value(&node, &key, expect_value.as_bytes(), VALUE_TIMEOUT).await?;
        println!("CRASH_TEST_OK key={key:?}");
        node.shutdown().await;
        return Ok(());
    }

    node.put(key.clone(), put_value.into_bytes())
        .await
        .map_err(|e| format!("put failed: {e}"))?;
    // make sure the survivor has it before we die
    tokio::time::sleep(Duration::from_secs(3)).await;
    std::fs::write(&sentinel, b"1").map_err(|e| format!("sentinel write failed: {e}"))?;
    println!("READY");
    // the driver SIGKILLs this container now; wait forever if run by hand
    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    Ok(())
}

async fn restart_survivor(
    node_id: String,
    port: u16,
    key: String,
    put_value: String,
    expect_value: String,
) -> TestResult {
    let node = Node::start(node_id, bind(port))
        .await
        .map_err(|e| format!("start failed: {e}"))?;
    // the crasher dials us; its write arrives via gossip
    wait_for_value(&node, &key, put_value.as_bytes(), VALUE_TIMEOUT).await?;
    node.put(key.clone(), expect_value.into_bytes())
        .await
        .map_err(|e| format!("put failed: {e}"))?;
    println!("wrote {key:?}, waiting for peer to die");
    tokio::time::timeout(PEER_TIMEOUT, async {
        while node.peer_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for peer to die".to_string())?;
    println!("PEER_DIED, waiting for it to return");
    wait_for_peers(&node, 1).await?;
    println!("PEER_RETURNED");
    // let the rebooted node's handshake sync settle before we exit
    tokio::time::sleep(Duration::from_secs(10)).await;
    node.shutdown().await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let result = match args.command {
        Command::ReplicateWriter {
            node_id,
            port,
            expect_peers,
            key,
            value,
        } => replicate_writer(node_id, port, expect_peers, key, value).await,
        Command::ReplicateReader {
            node_id,
            port,
            peer,
            key,
            value,
            timeout_secs,
        } => replicate_reader(node_id, port, peer, key, value, timeout_secs).await,
        Command::RestartCrashing {
            node_id,
            port,
            peer,
            key,
            put_value,
            expect_value,
            sentinel,
        } => restart_crashing(node_id, port, peer, key, put_value, expect_value, sentinel).await,
        Command::RestartSurvivor {
            node_id,
            port,
            key,
            put_value,
            expect_value,
        } => restart_survivor(node_id, port, key, put_value, expect_value).await,
    };
    if let Err(e) = result {
        eprintln!("SCENARIO_FAILED: {e}");
        std::process::exit(1);
    }
    println!("SCENARIO_OK");
}
