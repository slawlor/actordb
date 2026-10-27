//! Database-only server role for the separate external SDK process.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb::shard::FragmentId;
use actordb::store::{KvStore, RocksDbStore};

use super::{bind, TestResult, DIAL_TIMEOUT, PEER_TIMEOUT};

fn placement() -> HashMap<FragmentId, Vec<String>> {
    [
        (0, ["node-a", "node-b"]),
        (1, ["node-a", "node-b"]),
        (2, ["node-b", "node-c"]),
        (3, ["node-b", "node-c"]),
        (4, ["node-c", "node-a"]),
        (5, ["node-c", "node-a"]),
    ]
    .into_iter()
    .map(|(fragment, nodes)| (FragmentId(fragment), nodes.map(str::to_owned).to_vec()))
    .collect()
}

pub(super) async fn run(
    node_id: String,
    peer_port: u16,
    client_port: u16,
    peers: Vec<String>,
    db_path: Option<PathBuf>,
    bearer_token: Option<String>,
) -> TestResult {
    if peers.len() != 2 || peers[0] == peers[1] {
        return Err("client-node requires two distinct --peer addresses".into());
    }
    if !["node-a", "node-b", "node-c"].contains(&node_id.as_str()) {
        return Err("client-node identity must be node-a, node-b, or node-c".into());
    }
    if peer_port != 0 && peer_port == client_port {
        return Err("peer and application ports must be different".into());
    }
    let authorize: Option<actordb::grpc::GrpcAuthorizer> = bearer_token.map(|token| {
        let expected = format!("Bearer {token}");
        let callback: actordb::grpc::GrpcAuthorizer = Arc::new(move |metadata, _, _| {
            if metadata.get("authorization").and_then(|v| v.to_str().ok())
                == Some(expected.as_str())
            {
                Ok(())
            } else {
                Err(actordb::grpc::Status::unauthenticated("test bearer token"))
            }
        });
        callback
    });
    let mut shutdown_signal = ShutdownSignal::install()?;
    let started = Instant::now();
    let map = placement();
    let expected_fragments: HashSet<_> = map
        .iter()
        .filter(|(_, owners)| owners.contains(&node_id))
        .map(|(fragment, _)| *fragment)
        .collect();
    let store = db_path
        .map(|path| RocksDbStore::open(path).map(|store| Arc::new(store) as Arc<dyn KvStore>))
        .transpose()
        .map_err(|error| format!("open client-node store: {error}"))?;
    let node = Arc::new(
        Node::start(
            node_id.clone(),
            bind(peer_port),
            NodeConfig::builder()
                .fragment_count(6)
                .fragment_map(map)
                .maybe_store(store)
                .build(),
        )
        .await
        .map_err(|error| format!("start client-node: {error}"))?,
    );
    let mut listener = match GrpcServer::bind(
        node.clone(),
        bind(client_port),
        GrpcServerOptions {
            authorize,
            ..Default::default()
        },
    )
    .await
    {
        Ok(listener) => listener,
        Err(error) => {
            let shutdown = node.shutdown().await;
            return Err(format!(
                "bind application listener: {error}; local shutdown: {shutdown:?}"
            ));
        }
    };
    println!(
        "[{:>7.1}s {node_id}] peer={} application={}",
        started.elapsed().as_secs_f64(),
        node.addr(),
        listener.addr()
    );
    // Bind both listeners before dialing: simultaneous role startup and all
    // three nodes dialing one another cannot deadlock on a readiness barrier.
    let readiness = async {
        tokio::try_join!(
            dial(&node, &peers[0], started),
            dial(&node, &peers[1], started)
        )?;
        tokio::time::timeout(PEER_TIMEOUT, async {
            loop {
                if node.peer_count().await == 2 {
                    if let Ok(health) = node.health().await {
                        if health.len() == expected_fragments.len()
                            && expected_fragments.iter().all(|fragment| {
                                health
                                    .get(fragment)
                                    .is_some_and(|health| health.worker_count > 0)
                            })
                        {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| {
            "timed out waiting for exactly two database peers and live owned fragments".to_string()
        })?;
        Ok::<(), String>(())
    };
    let result = tokio::select! {
        ready = readiness => {
            match ready {
                Ok(()) => {
                    println!("[{:>7.1}s {node_id}] ready: database peers=2 owned fragments={}", started.elapsed().as_secs_f64(), expected_fragments.len());
                    println!("READY");
                    shutdown_signal.wait().await
                }
                Err(error) => Err(error),
            }
        }
        signal = shutdown_signal.wait() => signal,
    };
    println!(
        "[{:>7.1}s {node_id}] closing application listener and database",
        started.elapsed().as_secs_f64()
    );
    listener
        .shutdown()
        .await
        .map_err(|e| format!("gRPC listener shutdown: {e}"))?;
    let shutdown = node
        .shutdown()
        .await
        .map_err(|error| format!("client-node shutdown: {error}"));
    match (result, shutdown) {
        (Err(error), Err(shutdown)) => Err(format!("{error}; {shutdown}")),
        (Err(error), _) | (_, Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn dial(node: &Node, peer: &str, started: Instant) -> TestResult {
    tokio::time::timeout(DIAL_TIMEOUT, async {
        loop {
            if let Ok(addresses) = tokio::net::lookup_host(peer).await {
                for address in addresses {
                    if node.connect(address).await.is_ok() {
                        println!(
                            "[{:>7.1}s {}] dial supervisor owns {peer} at {address}",
                            started.elapsed().as_secs_f64(),
                            node.id()
                        );
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| format!("timed out resolving or dialing {peer}"))
}

struct ShutdownSignal {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}
impl ShutdownSignal {
    fn install() -> Result<Self, String> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())
                    .map_err(|error| format!("install SIGINT: {error}"))?,
                terminate: signal(SignalKind::terminate())
                    .map_err(|error| format!("install SIGTERM: {error}"))?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }
    async fn wait(&mut self) -> TestResult {
        #[cfg(unix)]
        {
            tokio::select! {
                signal = self.interrupt.recv() => signal.ok_or_else(|| "SIGINT stream closed".into()),
                signal = self.terminate.recv() => signal.ok_or_else(|| "SIGTERM stream closed".into()),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| format!("wait for Ctrl-C: {error}"))
        }
    }
}
