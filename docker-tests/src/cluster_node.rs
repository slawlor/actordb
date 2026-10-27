//! PRIVATE Docker harness control, fault injection and durable dynamic server.
//! This socket is not an ActorDB API; never expose it outside the test network.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use actordb::actor::VersionedValue;
use actordb::checkpoint::CheckpointBackend;
use actordb::grpc::{GrpcServer, GrpcServerOptions};
use actordb::network::{
    AcceptFuture, Acceptor, BindFuture, BoxStream, ConnectFuture, Network, NetworkError, TcpNetwork,
};
use actordb::node::status::SnapshotLimits;
use actordb::node::{
    DrainDurability, DrainOptions, Node, NodeConfig, NodeRuntimeOptions, RecoveryOptions,
};
use actordb::placement::{validate_placement, ManualPlacement};
use actordb::replication::{ReplicationAckDurability, SenderConfig};
use actordb::shard::FragmentId;
use actordb::store::{
    KvStore, PrefixDeleteOutcome, PrefixDeleteSupport, RocksDbStore, StoreBinding,
    StoreCommitDurability, StoreError, StoreIdentity,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;

use super::{bind, TestResult};

pub(super) const FRAGMENTS: usize = 6;
const MAX_CONTROL: usize = 16 * 1024;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Configuration {
    revision: u64,
    owners: Vec<String>,
    recovery_epoch: Option<String>,
}
impl Default for Configuration {
    fn default() -> Self {
        Self {
            revision: 0,
            owners: vec!["node-a".into()],
            recovery_epoch: None,
        }
    }
}
impl Configuration {
    fn map(&self) -> std::collections::HashMap<FragmentId, Vec<String>> {
        (0..FRAGMENTS)
            .map(|id| (FragmentId(id as u32), self.owners.clone()))
            .collect()
    }
    fn validate(&self) -> Result<Option<RecoveryOptions>, String> {
        if self
            .owners
            .iter()
            .any(|id| !["node-a", "node-b", "node-c", "node-d"].contains(&id.as_str()))
        {
            return Err("unknown harness node identity".into());
        }
        validate_placement(&self.map(), FRAGMENTS)?;
        self.recovery_epoch.as_deref().map(epoch).transpose()
    }
}
fn epoch(hex: &str) -> Result<RecoveryOptions, String> {
    if hex.len() != 64 {
        return Err("recovery epoch must contain exactly 64 hex digits".into());
    }
    let mut bytes = [0; 32];
    for (index, pair) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let part = std::str::from_utf8(pair).map_err(|e| e.to_string())?;
        bytes[index] = u8::from_str_radix(part, 16).map_err(|e| e.to_string())?;
    }
    RecoveryOptions::new(bytes).map_err(str::to_owned)
}
fn persist(path: &Path, configuration: &Configuration) -> Result<(), String> {
    let bytes = serde_json::to_vec(configuration).map_err(|e| e.to_string())?;
    let pending = path.with_extension("pending");
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&pending, path)?;
        File::open(
            path.parent()
                .ok_or_else(|| std::io::Error::other("config lacks parent"))?,
        )?
        .sync_all()
    })();
    result.map_err(|e| e.to_string())
}

#[derive(Default)]
struct Faults {
    isolated: AtomicBool,
    fail_store: AtomicBool,
    delay_ms: AtomicU64,
    changed: Arc<Notify>,
}
impl Faults {
    fn before_store(&self) -> Result<(), StoreError> {
        let delay = self.delay_ms.load(Ordering::Acquire);
        if delay != 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        if self.fail_store.load(Ordering::Acquire) {
            Err(StoreError("injected harness storage outage".into()))
        } else {
            Ok(())
        }
    }
}
struct FaultStore {
    inner: Arc<RocksDbStore>,
    faults: Arc<Faults>,
}
impl KvStore for FaultStore {
    fn checkpoint_backend(&self) -> Option<&dyn CheckpointBackend> {
        self.inner.checkpoint_backend()
    }
    fn bind(&self, identity: &StoreIdentity) -> Result<StoreBinding, StoreError> {
        self.inner.bind(identity)
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.faults.before_store()?;
        self.inner.load()
    }
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        self.faults.before_store()?;
        self.inner.load_prefix(prefix)
    }
    fn commit_durability(&self) -> StoreCommitDurability {
        self.inner.commit_durability()
    }
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError> {
        self.faults.before_store()?;
        self.inner.stable_fence()
    }
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        self.faults.before_store()?;
        self.inner.write_batch(writes)
    }
    fn prefix_delete_support(&self) -> PrefixDeleteSupport {
        self.inner.prefix_delete_support()
    }
    fn delete_prefix(&self, prefix: &[u8]) -> Result<PrefixDeleteOutcome, StoreError> {
        self.faults.before_store()?;
        self.inner.delete_prefix(prefix)
    }
}

// Process-mode equivalent of a disconnected peer network. Application/control
// sockets do not use this adapter. Docker mode disconnects the actual bridge.
struct FaultNetwork(Arc<Faults>);
struct FaultAcceptor {
    inner: Box<dyn Acceptor>,
    faults: Arc<Faults>,
}
struct FaultStream {
    inner: BoxStream,
    faults: Arc<Faults>,
    read_changed: Pin<Box<OwnedNotified>>,
    write_changed: Pin<Box<OwnedNotified>>,
}
impl FaultStream {
    fn boxed(inner: BoxStream, faults: Arc<Faults>) -> BoxStream {
        let read_changed = Box::pin(faults.changed.clone().notified_owned());
        let write_changed = Box::pin(faults.changed.clone().notified_owned());
        Box::pin(Self {
            inner,
            faults,
            read_changed,
            write_changed,
        })
    }
    fn check(&mut self, cx: &mut Context<'_>, reading: bool) -> std::io::Result<()> {
        // Split readers/writers have different wakers. A successful flush must
        // not replace the idle reader's notification and hide an outage.
        let changed = if reading {
            &mut self.read_changed
        } else {
            &mut self.write_changed
        };
        while std::future::Future::poll(changed.as_mut(), cx).is_ready() {
            *changed = Box::pin(self.faults.changed.clone().notified_owned());
        }
        if self.faults.isolated.load(Ordering::Acquire) {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected peer partition",
            ))
        } else {
            Ok(())
        }
    }
}
impl AsyncRead for FaultStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(e) = self.check(cx, true) {
            return Poll::Ready(Err(e));
        }
        self.inner.as_mut().poll_read(cx, buf)
    }
}
impl AsyncWrite for FaultStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Err(e) = self.check(cx, false) {
            return Poll::Ready(Err(e));
        }
        self.inner.as_mut().poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Err(e) = self.check(cx, false) {
            return Poll::Ready(Err(e));
        }
        self.inner.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}
impl Acceptor for FaultAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            let stream = self.inner.accept().await?;
            if self.faults.isolated.load(Ordering::Acquire) {
                return Err(NetworkError("injected peer partition".into()));
            }
            Ok(FaultStream::boxed(stream, self.faults.clone()))
        })
    }
}
impl Network for FaultNetwork {
    fn bind(&self, addr: std::net::SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let (addr, inner) = TcpNetwork.bind(addr).await?;
            Ok((
                addr,
                Box::new(FaultAcceptor {
                    inner,
                    faults: self.0.clone(),
                }) as Box<dyn Acceptor>,
            ))
        })
    }
    fn connect(&self, addr: std::net::SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            if self.0.isolated.load(Ordering::Acquire) {
                return Err(NetworkError("injected peer partition".into()));
            }
            Ok(FaultStream::boxed(
                TcpNetwork.connect(addr).await?,
                self.0.clone(),
            ))
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Status {},
    Configure {
        revision: u64,
        owners: Vec<String>,
        recovery_epoch: Option<String>,
        #[serde(default)]
        next_boot_only: bool,
    },
    Fault {
        isolated: Option<bool>,
        fail_store: Option<bool>,
        delay_ms: Option<u64>,
    },
    StoreCounts {},
}
struct Control {
    node: Arc<Node>,
    faults: Arc<Faults>,
    store: Arc<RocksDbStore>,
    controller: Arc<ManualPlacement>,
    path: PathBuf,
    configuration: tokio::sync::Mutex<Configuration>,
    active_epoch: Option<String>,
}
impl Control {
    async fn request(&self, request: Request) -> Result<Value, String> {
        match request {
            Request::Status {} => {
                let status = self
                    .node
                    .status_snapshot_with_limits(SnapshotLimits::new(64, 128));
                Ok(json!({
                    "node": self.node.id(), "boot_id": status.boot_id,
                    "readiness": format!("{:?}", status.readiness), "lifecycle": format!("{:?}", status.lifecycle),
                    "complete": status.observation.is_complete(),
                    "placement_complete": !status.observation.placement_unavailable && !status.observation.placement_changed
                        && !status.observation.fragments_truncated && !status.observation.replicas_truncated,
                    "peers": self.node.peer_count().await,
                    "active_storage": status.storage.active_jobs,
                    "active_requests": status.drain.active_requests(),
                    "receive_applies": status.drain.active_replication_applies(),
                    "active_epoch": self.active_epoch,
                    "configuration": self.configuration.lock().await.clone(),
                    "fragments": status.fragments.iter().map(|f| json!({
                        "id": f.fragment.0, "state": f.placement_state.map(|v|format!("{v:?}")),
                        "serving": f.serving, "route": f.route_available, "assigned": f.assigned_locally,
                        "replicas": f.replicas,
                    })).collect::<Vec<_>>(),
                    "senders": status.replication.iter().map(|s| json!({
                        "fragment": s.fragment.0, "peer": s.peer_id, "state": format!("{:?}",s.connection),
                        "buffered": s.buffered_operations, "in_flight": s.in_flight_batches,
                        "rounds": s.completed_reconcile_rounds, "session": s.session_id,
                        "activity": format!("{:?}", s.activity),
                        "reconciliation_stage": s.reconciliation_stage.map(|stage| format!("{stage:?}")),
                    })).collect::<Vec<_>>(),
                }))
            }
            Request::Configure {
                revision,
                owners,
                recovery_epoch,
                next_boot_only,
            } => {
                let next = Configuration {
                    revision,
                    owners,
                    recovery_epoch,
                };
                next.validate()?;
                let mut current = self.configuration.lock().await;
                if next.revision < current.revision
                    || next.revision == current.revision && next != *current
                {
                    return Err("stale or conflicting controller revision".into());
                }
                let path = self.path.clone();
                let candidate = next.clone();
                tokio::task::spawn_blocking(move || persist(&path, &candidate))
                    .await
                    .map_err(|e| e.to_string())??;
                if !next_boot_only {
                    self.controller.set_placement(next.map());
                }
                *current = next;
                Ok(json!({"configured": revision}))
            }
            Request::Fault {
                isolated,
                fail_store,
                delay_ms,
            } => {
                if delay_ms.is_some_and(|ms| ms > 2000) {
                    return Err("storage delay exceeds harness limit".into());
                }
                if let Some(ms) = delay_ms {
                    self.faults.delay_ms.store(ms, Ordering::Release);
                }
                if let Some(fail) = fail_store {
                    self.faults.fail_store.store(fail, Ordering::Release);
                }
                if let Some(isolated) = isolated {
                    self.faults.isolated.store(isolated, Ordering::Release);
                    self.faults.changed.notify_waiters();
                }
                Ok(json!({"fault_updated":true}))
            }
            Request::StoreCounts {} => {
                let store = self.store.clone();
                let counts = tokio::task::spawn_blocking(move || {
                    let mut counts = BTreeMap::<u32, usize>::new();
                    for (key, _) in store.load().map_err(|e| e.to_string())? {
                        let prefix: [u8; 4] = key
                            .get(..4)
                            .ok_or("missing fragment prefix")?
                            .try_into()
                            .map_err(|_| "invalid fragment prefix")?;
                        *counts.entry(u32::from_be_bytes(prefix)).or_default() += 1;
                    }
                    Ok::<_, String>(counts)
                })
                .await
                .map_err(|e| e.to_string())??;
                Ok(json!({"counts":counts}))
            }
        }
    }
}
async fn connection(mut stream: TcpStream, control: Arc<Control>) {
    let exchange = async {
        let mut bytes = Vec::new();
        loop {
            let byte = stream.read_u8().await.map_err(|e| e.to_string())?;
            if byte == b'\n' {
                break;
            }
            if bytes.len() == MAX_CONTROL {
                return Err("control request too large".into());
            }
            bytes.push(byte);
        }
        let request: Request = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        control.request(request).await
    };
    let result = tokio::time::timeout(CONTROL_TIMEOUT, exchange)
        .await
        .map_err(|_| "control request timed out".to_owned())
        .and_then(|v| v);
    let response = match result {
        Ok(value) => json!({"ok":value}),
        Err(error) => json!({"error":error}),
    };
    let send = async {
        let mut bytes = serde_json::to_vec(&response).map_err(std::io::Error::other)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await
    };
    let _ = tokio::time::timeout(CONTROL_TIMEOUT, send).await;
}

pub(super) async fn run(
    id: String,
    peer_port: u16,
    app_port: u16,
    control_port: u16,
    peers: Vec<String>,
    directory: PathBuf,
) -> TestResult {
    if peers.len() != 3
        || peer_port == app_port
        || peer_port == control_port
        || app_port == control_port
    {
        return Err("cluster-node needs three peers and distinct ports".into());
    }
    fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let path = directory.join("controller.json");
    let configuration: Configuration = if path.exists() {
        serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?
    } else {
        let configuration = Configuration::default();
        persist(&path, &configuration)?;
        configuration
    };
    let recovery = configuration.validate()?;
    let controller = Arc::new(ManualPlacement::new(configuration.map()));
    let faults = Arc::new(Faults::default());
    let store = Arc::new(RocksDbStore::open(directory.join("db")).map_err(|e| e.to_string())?);
    let adapter: Arc<dyn KvStore> = Arc::new(FaultStore {
        inner: store.clone(),
        faults: faults.clone(),
    });
    let config = NodeConfig::builder()
        .fragment_count(FRAGMENTS)
        .store(adapter)
        .placement_controller(controller.clone())
        .placement_poll_interval(Duration::from_millis(50))
        .maybe_recovery(recovery)
        .flush_batch_size(1)
        .clean_interval(Duration::from_millis(200))
        .snapshot_interval(Duration::from_secs(1))
        .replication(
            SenderConfig::builder()
                .batch_size(4)
                .buffer_capacity(8)
                .max_in_flight(2)
                .batch_interval(Duration::from_millis(50))
                .consistency_interval(Duration::from_secs(2))
                .required_ack_durability(ReplicationAckDurability::StableCommitted)
                .build(),
        )
        .network(Arc::new(FaultNetwork(faults.clone())) as Arc<dyn Network>)
        .build();
    let options = NodeRuntimeOptions::builder()
        .reconciliation_retained_byte_capacity(256 * 1024 * 1024)
        .build()
        .with_socket_write_timeout(Duration::from_secs(2))
        .with_connect_timeout(Duration::from_secs(2))
        .with_replication_ack_timeout(Duration::from_secs(3))
        .with_forwarded_attempt_timeout(Duration::from_secs(2));
    let node = Arc::new(
        Node::start_with_options(id.clone(), bind(peer_port), config, options)
            .await
            .map_err(|e| e.to_string())?,
    );
    let mut app = GrpcServer::bind(node.clone(), bind(app_port), GrpcServerOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    let listener = TcpListener::bind(bind(control_port))
        .await
        .map_err(|e| e.to_string())?;
    let control = Arc::new(Control {
        node: node.clone(),
        faults,
        store,
        controller,
        path,
        configuration: tokio::sync::Mutex::new(configuration.clone()),
        active_epoch: configuration.recovery_epoch,
    });
    let (stop, mut stopped) = watch::channel(false);
    let serving = control.clone();
    let control_task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept(), if connections.len() < 16 => {
                    match accepted {
                        Ok((stream,_)) => { connections.spawn(connection(stream,serving.clone())); }
                        Err(_) => break,
                    }
                }
            }
        }
        connections.shutdown().await;
    });
    let mut dials = JoinSet::new();
    for peer in peers {
        let node = node.clone();
        dials.spawn(async move {
            loop {
                if let Ok(addresses) = tokio::net::lookup_host(&peer).await {
                    for address in addresses {
                        // Re-resolve peer aliases after bridge re-attachment.
                        // Node owns and deduplicates every dial supervisor.
                        let _ = node.connect(address).await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    println!(
        "READY node={id} control={control_port} boot_id={}",
        node.status_snapshot().boot_id
    );
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
        let mut interrupt = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    dials.shutdown().await;
    app.shutdown().await.map_err(|e| e.to_string())?;
    let result = node
        .finish_shutdown(
            DrainOptions::default()
                .with_durability(DrainDurability::StableCommitted)
                .with_timeout(Duration::from_secs(60)),
        )
        .await;
    let _ = stop.send(true);
    let _ = control_task.await;
    result
        .map(|report| {
            println!(
                "SHUTDOWN_OK node={id} requests={} applies={} storage={} stable_fences={}",
                report.status().active_requests(),
                report.status().active_replication_applies(),
                report.status().active_storage_jobs(),
                report.local_fences().len()
            );
        })
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn peer_fault_wakes_idle_readers_after_writer_flush() {
        struct CountWake(AtomicU64);
        impl std::task::Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let faults = Arc::new(Faults::default());
        let (local, _remote) = tokio::io::duplex(64);
        let mut stream = FaultStream::boxed(Box::pin(local), faults.clone());
        let read = Arc::new(CountWake(AtomicU64::new(0)));
        let write = Arc::new(CountWake(AtomicU64::new(0)));
        let read_waker = std::task::Waker::from(read.clone());
        let write_waker = std::task::Waker::from(write.clone());
        let mut input = [0u8; 1];
        let mut buffer = ReadBuf::new(&mut input);
        assert!(stream
            .as_mut()
            .poll_read(&mut Context::from_waker(&read_waker), &mut buffer)
            .is_pending());
        assert!(stream
            .as_mut()
            .poll_flush(&mut Context::from_waker(&write_waker))
            .is_ready());
        faults.isolated.store(true, Ordering::Release);
        faults.changed.notify_waiters();
        assert!(
            read.0.load(Ordering::SeqCst) > 0,
            "flush stole the reader wakeup"
        );
        assert!(write.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            stream
                .as_mut()
                .poll_read(&mut Context::from_waker(&read_waker), &mut buffer),
            Poll::Ready(Err(_))
        ));
    }
    #[test]
    fn controller_shape_and_recovery_epoch_are_strict() {
        assert!(Configuration::default().validate().is_ok());
        for owners in [
            vec![],
            vec!["node-a".into(), "node-a".into()],
            vec!["unknown".into()],
        ] {
            assert!(Configuration {
                owners,
                ..Configuration::default()
            }
            .validate()
            .is_err());
        }
        assert!(epoch(&"00".repeat(32)).is_err());
        assert!(epoch(&"2a".repeat(32)).is_ok());
        assert!(epoch("not-hex").is_err());
        assert!(serde_json::from_str::<Request>(r#"{"command":"status","typo":1}"#).is_err());
    }
}
