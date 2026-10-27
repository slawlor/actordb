//! Dockerized multi-node integration tests for actordb, modeled on
//! ractor's `ractor_cluster_integration_tests`.
//!
//! Each container runs one role: it starts a real [`Node`], dials its
//! peers over the compose network (DNS names like `node-a:7447`), runs a
//! scripted scenario with generous timeouts, and exits 0 on success.
//!
//! Roles:
//! - `replicate-writer` / `replicate-reader`: 3-node convergence.
//! - `client-node`: a database/application server for external SDK scenarios.
//! - `restart-crashing` / `restart-survivor`: SIGKILL a node mid-cluster,
//!   restart it, assert it reconverges (exercises redial + `boot_id`).
//!
//! Observability: every line is `[<elapsed>s <node-id>] <message>` on
//! stdout, so `docker compose logs` shows an interleaved, attributable
//! timeline across containers. The bare marker lines `READY`,
//! `SCENARIO_OK` and `SCENARIO_FAILED` are intentionally unprefixed —
//! the driver scripts grep for them.

mod client_node;
mod cluster_node;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actordb::node::{Node, NodeConfig};
use actordb::store::{KvStore, RocksDbStore};
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
    /// Durable, dynamically placed server with a PRIVATE test-control socket.
    ClusterNode {
        #[arg(long, value_parser = ["node-a", "node-b", "node-c", "node-d"])]
        node_id: String,
        #[arg(long, default_value_t = 7447)]
        peer_port: u16,
        #[arg(long, default_value_t = 7449)]
        client_port: u16,
        #[arg(long, default_value_t = 7450)]
        control_port: u16,
        #[arg(long, required = true)]
        peer: Vec<String>,
        #[arg(long)]
        db_path: PathBuf,
    },
    /// Serve a sharded database node and its separate application listener.
    ClientNode {
        #[arg(long, value_parser = ["node-a", "node-b", "node-c"])]
        node_id: String,
        #[arg(long, default_value_t = 7447)]
        peer_port: u16,
        #[arg(long, default_value_t = 7449)]
        client_port: u16,
        /// Repeat for the other two database nodes; DNS names are supported.
        #[arg(long, required = true)]
        peer: Vec<String>,
        #[arg(long)]
        db_path: Option<PathBuf>,
        /// Optional test-only gRPC bearer token; not a peer secret.
        #[arg(long)]
        bearer_token: Option<String>,
    },
    /// Wait for peers, write one key, exit 0.
    ReplicateWriter {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        port: u16,
        /// optional RocksDB path for the node's local copy
        #[arg(long)]
        db_path: Option<PathBuf>,
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
        /// optional RocksDB path for the node's local copy
        #[arg(long)]
        db_path: Option<PathBuf>,
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
        /// optional RocksDB path for the node's local copy
        #[arg(long)]
        db_path: Option<PathBuf>,
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
        /// optional RocksDB path for the node's local copy
        #[arg(long)]
        db_path: Option<PathBuf>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        put_value: String,
        #[arg(long)]
        expect_value: String,
    },
}

type TestResult = Result<(), String>;

/// Per-role context: the node, its id for log prefixes, and the role's
/// start time for elapsed timestamps.
struct Ctx {
    id: String,
    node: Node,
    t0: Instant,
}

impl Ctx {
    async fn start(node_id: String, port: u16, db_path: Option<PathBuf>) -> Result<Self, String> {
        let store = db_path
            .map(|path| RocksDbStore::open(path).map(|store| Arc::new(store) as Arc<dyn KvStore>))
            .transpose()
            .map_err(|e| format!("open db: {e}"))?;
        let config = NodeConfig::builder().maybe_store(store).build();
        let node = Node::start(node_id.clone(), bind(port), config)
            .await
            .map_err(|e| format!("start failed: {e}"))?;
        Ok(Self {
            id: node_id,
            node,
            t0: Instant::now(),
        })
    }

    fn elapsed(&self) -> f64 {
        self.t0.elapsed().as_secs_f64()
    }

    fn log(&self, msg: &str) {
        println!("[{:>7.1}s {}] {}", self.elapsed(), self.id, msg);
    }

    fn warn(&self, msg: &str) {
        eprintln!("[{:>7.1}s {}] WARN: {}", self.elapsed(), self.id, msg);
    }

    /// One-line snapshot of replica state, for failure diagnostics.
    async fn describe_state(&self) -> String {
        let peers = self.node.peer_count().await;
        let keys = match self.node.get_all().await {
            Ok(m) => {
                let mut ks: Vec<_> = m.keys().cloned().collect();
                ks.sort();
                format!("{ks:?}")
            }
            Err(e) => format!("<get_all failed: {e}>"),
        };
        format!("peers={peers} keys={keys}")
    }
}

/// Run one named phase of a scenario, logging start/end with its duration.
async fn phase<T>(
    ctx: &Ctx,
    name: &str,
    f: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    ctx.log(&format!("phase start: {name}"));
    let t = Instant::now();
    let r = f.await;
    let dt = t.elapsed().as_secs_f64();
    match &r {
        Ok(_) => ctx.log(&format!("phase done: {name} ({dt:.1}s)")),
        Err(e) => ctx.warn(&format!("phase FAILED: {name} ({dt:.1}s): {e}")),
    }
    r
}

async fn wait_for_peers(ctx: &Ctx, n: usize) -> TestResult {
    let r = tokio::time::timeout(PEER_TIMEOUT, async {
        while ctx.node.peer_count().await < n {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if r.is_err() {
        return Err(format!(
            "timed out waiting for {n} peers [{}]",
            ctx.describe_state().await
        ));
    }
    ctx.log(&format!("peers: {}", ctx.node.peer_count().await));
    Ok(())
}

/// Dial `peer` (`host:port`, DNS allowed), retrying until it answers.
async fn dial_with_retry(ctx: &Ctx, peer: &str) -> TestResult {
    let start = Instant::now();
    loop {
        match tokio::net::lookup_host(peer).await {
            Ok(addrs) => {
                for addr in addrs {
                    match ctx.node.connect(addr).await {
                        Ok(()) => {
                            ctx.log(&format!("dialed {addr}"));
                            return Ok(());
                        }
                        Err(e) => ctx.warn(&format!("dial {addr} failed: {e}")),
                    }
                }
            }
            Err(e) => ctx.warn(&format!("DNS lookup for {peer} failed: {e}")),
        }
        if start.elapsed() > DIAL_TIMEOUT {
            return Err(format!("timed out dialing {peer}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_value(ctx: &Ctx, key: &str, value: &[u8], timeout: Duration) -> TestResult {
    let r = tokio::time::timeout(timeout, async {
        loop {
            match ctx.node.get(key.to_string()).await {
                Ok(Some(v)) if v.value == value => break,
                Ok(_) => {}
                Err(e) => ctx.warn(&format!("get failed: {e}")),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if r.is_err() {
        return Err(format!(
            "timed out waiting for key {key:?} [{}]",
            ctx.describe_state().await
        ));
    }
    ctx.log(&format!("converged: {key:?}"));
    Ok(())
}

/// Wait for a value, re-dialing peers if the connection drops.
/// Used by replicate-reader to survive peer restarts.
async fn wait_for_value_with_redial(
    ctx: &Ctx,
    key: &str,
    value: &[u8],
    timeout: Duration,
    peers: &[String],
) -> TestResult {
    let r = tokio::time::timeout(timeout, async {
        loop {
            // Re-dial if we lost all peers (peer crashed/restarted).
            if ctx.node.peer_count().await == 0 {
                ctx.log("lost all peers, re-dialing");
                for peer in peers {
                    let _ = dial_with_retry(ctx, peer).await;
                }
            }
            match ctx.node.get(key.to_string()).await {
                Ok(Some(v)) if v.value == value => break,
                Ok(_) => {}
                Err(e) => ctx.warn(&format!("get failed: {e}")),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if r.is_err() {
        return Err(format!(
            "timed out waiting for key {key:?} [{}]",
            ctx.describe_state().await
        ));
    }
    ctx.log(&format!("converged: {key:?}"));
    Ok(())
}

fn bind(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}

async fn replicate_writer(
    node_id: String,
    port: u16,
    db_path: Option<PathBuf>,
    expect_peers: usize,
    key: String,
    value: String,
) -> TestResult {
    let ctx = Ctx::start(node_id, port, db_path).await?;
    ctx.log(&format!(
        "role=replicate-writer port={port} expect_peers={expect_peers} key={key:?}"
    ));
    phase(&ctx, "wait-peers", wait_for_peers(&ctx, expect_peers)).await?;
    phase(
        &ctx,
        "put",
        put_key(&ctx, key.clone(), value.clone().into_bytes()),
    )
    .await?;
    ctx.log("WRITER_DONE: letting gossip settle before exit");
    // let gossip settle before this container exits
    tokio::time::sleep(Duration::from_secs(5)).await;
    ctx.node.shutdown().await.unwrap();
    ctx.log(&format!("done in {:.1}s", ctx.elapsed()));
    Ok(())
}

async fn put_key(ctx: &Ctx, key: String, value: Vec<u8>) -> TestResult {
    ctx.node
        .put(key.clone(), value)
        .await
        .map_err(|e| format!("put failed: {e}"))?;
    ctx.log(&format!("put {key:?}"));
    Ok(())
}

async fn replicate_reader(
    node_id: String,
    port: u16,
    db_path: Option<PathBuf>,
    peers: Vec<String>,
    key: String,
    value: String,
    timeout_secs: u64,
) -> TestResult {
    let ctx = Ctx::start(node_id, port, db_path).await?;
    ctx.log(&format!(
        "role=replicate-reader port={port} peers={peers:?} key={key:?} timeout={timeout_secs}s"
    ));
    for peer in &peers {
        phase(&ctx, &format!("dial {peer}"), dial_with_retry(&ctx, peer)).await?;
    }
    // Phase 5: if peers drop (e.g., crash/restart), re-dial. The wait
    // loop checks peer count and reconnects as needed.
    phase(
        &ctx,
        "wait-value",
        wait_for_value_with_redial(
            &ctx,
            &key,
            value.as_bytes(),
            Duration::from_secs(timeout_secs),
            &peers,
        ),
    )
    .await?;
    ctx.log("READER_OK");
    ctx.node.shutdown().await.unwrap();
    ctx.log(&format!("done in {:.1}s", ctx.elapsed()));
    Ok(())
}

// Role handlers take one arg per CLI flag; allow the arity lint here —
// this is a scripted test driver, not library API.
#[allow(clippy::too_many_arguments)]
async fn restart_crashing(
    node_id: String,
    port: u16,
    db_path: Option<PathBuf>,
    peer: String,
    key: String,
    put_value: String,
    expect_value: String,
    sentinel: PathBuf,
) -> TestResult {
    let ctx = Ctx::start(node_id, port, db_path).await?;
    ctx.log(&format!(
        "role=restart-crashing port={port} peer={peer:?} key={key:?} sentinel={sentinel:?}"
    ));
    phase(&ctx, &format!("dial {peer}"), dial_with_retry(&ctx, &peer)).await?;
    phase(&ctx, "wait-peers", wait_for_peers(&ctx, 1)).await?;

    if sentinel.exists() {
        // rebooted after the SIGKILL: the survivor should have written
        // expect_value while we were gone; sync on handshake delivers it
        ctx.log("REBOOTED: this container was SIGKILLed and restarted");
        phase(
            &ctx,
            "wait-reconverge",
            wait_for_value(&ctx, &key, expect_value.as_bytes(), VALUE_TIMEOUT),
        )
        .await?;
        ctx.log("CRASH_TEST_OK");
        // Stay up to let readers (node-c) finish via re-dial.
        // Sleep long enough for the scenario timeout, then exit.
        ctx.log("staying up for readers (60s)");
        tokio::time::sleep(Duration::from_secs(60)).await;
        ctx.node.shutdown().await.unwrap();
        ctx.log(&format!("done in {:.1}s", ctx.elapsed()));
        return Ok(());
    }

    phase(
        &ctx,
        "put",
        put_key(&ctx, key.clone(), put_value.into_bytes()),
    )
    .await?;
    // make sure the survivor has it before we die
    tokio::time::sleep(Duration::from_secs(3)).await;
    std::fs::write(&sentinel, b"1").map_err(|e| format!("sentinel write failed: {e}"))?;
    // bare marker line for the driver script (grep ^READY$)
    println!("READY");
    ctx.log("waiting for SIGKILL from the driver (sleeping)");
    // the driver SIGKILLs this container now; wait forever if run by hand
    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    Ok(())
}

async fn restart_survivor(
    node_id: String,
    port: u16,
    db_path: Option<PathBuf>,
    key: String,
    put_value: String,
    expect_value: String,
) -> TestResult {
    let ctx = Ctx::start(node_id, port, db_path).await?;
    ctx.log(&format!(
        "role=restart-survivor port={port} key={key:?} (waits to be dialed)"
    ));
    // the crasher dials us; its write arrives via gossip
    phase(
        &ctx,
        "wait-value",
        wait_for_value(&ctx, &key, put_value.as_bytes(), VALUE_TIMEOUT),
    )
    .await?;
    phase(
        &ctx,
        "put",
        put_key(&ctx, key.clone(), expect_value.into_bytes()),
    )
    .await?;
    println!("SURVIVOR_READY");
    ctx.log("waiting for peer to die (SIGKILL)");
    phase(&ctx, "wait-peer-death", wait_for_peer_death(&ctx)).await?;
    println!("PEER_DIED");
    ctx.log("PEER_DIED: waiting for it to return");
    phase(&ctx, "wait-peer-return", wait_for_peers(&ctx, 1)).await?;
    ctx.log("PEER_RETURNED: letting post-restart sync settle");
    // let the rebooted node's handshake sync settle before we exit
    tokio::time::sleep(Duration::from_secs(10)).await;
    ctx.node.shutdown().await.unwrap();
    ctx.log(&format!("done in {:.1}s", ctx.elapsed()));
    Ok(())
}

async fn wait_for_peer_death(ctx: &Ctx) -> TestResult {
    let r = tokio::time::timeout(PEER_TIMEOUT, async {
        while ctx.node.peer_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if r.is_err() {
        return Err(format!(
            "timed out waiting for peer to die [{}]",
            ctx.describe_state().await
        ));
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let result = match args.command {
        Command::ClusterNode {
            node_id,
            peer_port,
            client_port,
            control_port,
            peer,
            db_path,
        } => cluster_node::run(node_id, peer_port, client_port, control_port, peer, db_path).await,
        Command::ClientNode {
            node_id,
            peer_port,
            client_port,
            peer,
            db_path,
            bearer_token,
        } => client_node::run(node_id, peer_port, client_port, peer, db_path, bearer_token).await,
        Command::ReplicateWriter {
            node_id,
            port,
            db_path,
            expect_peers,
            key,
            value,
        } => replicate_writer(node_id, port, db_path, expect_peers, key, value).await,
        Command::ReplicateReader {
            node_id,
            port,
            db_path,
            peer,
            key,
            value,
            timeout_secs,
        } => replicate_reader(node_id, port, db_path, peer, key, value, timeout_secs).await,
        Command::RestartCrashing {
            node_id,
            port,
            db_path,
            peer,
            key,
            put_value,
            expect_value,
            sentinel,
        } => {
            restart_crashing(
                node_id,
                port,
                db_path,
                peer,
                key,
                put_value,
                expect_value,
                sentinel,
            )
            .await
        }
        Command::RestartSurvivor {
            node_id,
            port,
            db_path,
            key,
            put_value,
            expect_value,
        } => restart_survivor(node_id, port, db_path, key, put_value, expect_value).await,
    };
    match result {
        Ok(()) => println!("SCENARIO_OK"),
        Err(e) => {
            // bare marker line for CI log grepping
            println!("SCENARIO_FAILED: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod client_node_cli_tests {
    use super::*;

    #[test]
    fn client_node_accepts_repeated_peers_and_separate_port_defaults() {
        let args = Args::try_parse_from([
            "actordb-docker-test",
            "client-node",
            "--node-id",
            "node-a",
            "--peer",
            "node-b:7447",
            "--peer",
            "node-c:7447",
        ])
        .unwrap();
        let Command::ClientNode {
            node_id,
            peer_port,
            client_port,
            peer,
            ..
        } = args.command
        else {
            panic!("wrong role");
        };
        assert_eq!(node_id, "node-a");
        assert_eq!((peer_port, client_port), (7447, 7449));
        assert_eq!(peer, ["node-b:7447", "node-c:7447"]);
        assert!(Args::try_parse_from([
            "actordb-docker-test",
            "client-node",
            "--node-id",
            "unplaced",
            "--peer",
            "node-b:7447"
        ])
        .is_err());
    }
}
