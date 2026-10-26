//! Separate SDK-only process: durable oracles and PRIVATE harness control.
//! Never retry a mutation after an unknown outcome. Reads may poll convergence.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actordb_client::{
    Client, ClientError, ClientOptions, PutOptions, QueryValue, WriteConcern,
    WriteConcernClientError, WriteConcernOptions, WriteConcernReceipt, WriteDurability,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const FRAGMENTS: u32 = 6;
const TABLES: [&str; 3] = ["KV", "USERS", "ORDERS"];
const WAIT: Duration = Duration::from_secs(180);
type Result<T> = std::result::Result<T, String>;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    phase: String,
    /// Repeated id@application-host:port@PRIVATE-control-host:port.
    #[arg(long, required = true)]
    node: Vec<String>,
    #[arg(long, default_value = "/work/ledger.json")]
    ledger: PathBuf,
    #[arg(long, default_value = "node-a")]
    target: String,
    /// A deliberately stopped node whose next boot config was staged first.
    #[arg(long)]
    absent: Option<String>,
    #[arg(long, default_value_t = 0)]
    round: u32,
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(1..=256))]
    records_per_fragment: u32,
    #[arg(long)]
    epoch: Option<String>,
    #[arg(long)]
    isolated: Option<bool>,
    #[arg(long)]
    fail_store: Option<bool>,
    #[arg(long)]
    delay_ms: Option<u64>,
}
struct Endpoint {
    id: String,
    app: String,
    control: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Record {
    table: String,
    key: String,
    value: Vec<u8>,
    timestamp: u64,
    logical: u64,
    node: String,
    tombstone: bool,
    expires: Option<u64>,
}
impl Record {
    fn identity(&self) -> String {
        format!("{}\0{}", self.table, self.key)
    }
    fn version(&self) -> (u64, u64, &str, bool) {
        (self.timestamp, self.logical, &self.node, self.tombstone)
    }
    fn at(&self, now: u64) -> Self {
        let mut record = self.clone();
        if !record.tombstone && record.expires.is_some_and(|time| time <= now) {
            record.tombstone = true;
            record.value.clear();
        }
        record
    }
}
#[derive(Default, Serialize, Deserialize)]
struct Ledger {
    revision: u64,
    owners: Vec<String>,
    recovery_epoch: Option<String>,
    records: BTreeMap<String, Record>,
}
impl Ledger {
    fn add(&mut self, record: Record) {
        let identity = record.identity();
        if self
            .records
            .get(&identity)
            .is_none_or(|old| record.version() >= old.version())
        {
            self.records.insert(identity, record);
        }
    }
    fn save(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path.parent().ok_or("ledger path lacks parent")?)
            .map_err(|e| e.to_string())?;
        let pending = path.with_extension("pending");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)
            .map_err(|e| e.to_string())?;
        let mut file = BufWriter::new(file);
        serde_json::to_writer(&mut file, self).map_err(|e| e.to_string())?;
        file.flush().map_err(|e| e.to_string())?;
        file.get_ref().sync_all().map_err(|e| e.to_string())?;
        fs::rename(pending, path).map_err(|e| e.to_string())?;
        File::open(path.parent().unwrap())
            .and_then(|file| file.sync_all())
            .map_err(|e| e.to_string())
    }
}
struct Harness {
    args: Args,
    endpoints: BTreeMap<String, Endpoint>,
    ledger: Ledger,
}
fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn key_for(fragment: u32, prefix: &str) -> String {
    (0u64..)
        .map(|i| format!("{prefix}:{fragment}:{i}"))
        .find(|key| seahash::hash(key.as_bytes()) % u64::from(FRAGMENTS) == u64::from(fragment))
        .unwrap()
}
async fn retry(name: &str, mut check: impl AsyncFnMut() -> Result<()>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match check().await {
            Ok(()) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(format!("{name}: {error}"))
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}
async fn control(endpoint: &Endpoint, request: Value) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut stream = TcpStream::connect(&endpoint.control)
            .await
            .map_err(|e| format!("{} control: {e}", endpoint.id))?;
        let mut bytes = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await.map_err(|e| e.to_string())?;
        let mut bytes = Vec::new();
        loop {
            let byte = stream.read_u8().await.map_err(|e| e.to_string())?;
            if byte == b'\n' {
                break;
            }
            if bytes.len() == 128 * 1024 {
                return Err("oversized harness response".into());
            }
            bytes.push(byte);
        }
        let response: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        response
            .get("ok")
            .cloned()
            .ok_or_else(|| format!("{}: {response}", endpoint.id))
    })
    .await
    .map_err(|_| format!("{} control deadline", endpoint.id))?
}
async fn client(endpoint: &Endpoint, timeout: Duration) -> Result<Client> {
    let address = tokio::net::lookup_host(&endpoint.app)
        .await
        .map_err(|e| e.to_string())?
        .next()
        .ok_or("no application address")?;
    Client::connect(
        address,
        ClientOptions::builder()
            .identity("cluster-hardening")
            .timeout(timeout)
            .build(),
    )
    .await
    .map_err(|e| format!("{} application: {e}", endpoint.id))
}
fn concern(majority: bool) -> WriteConcernOptions {
    WriteConcernOptions::builder()
        .concern(if majority {
            WriteConcern::Majority
        } else {
            WriteConcern::Local
        })
        .minimum_durability(WriteDurability::StableCommitted)
        .require_evidence(true)
        .strict_acknowledgements(true)
        .timeout(Duration::from_secs(10))
        .build()
}
fn certified(
    table: &str,
    key: &str,
    value: Vec<u8>,
    receipt: &WriteConcernReceipt,
) -> Result<Record> {
    let evidence = receipt
        .evidence()
        .ok_or("missing stable operation evidence")?;
    require(
        evidence
            .acknowledgements()
            .iter()
            .all(|ack| ack.durability() >= WriteDurability::StableCommitted),
        "weak acknowledgement counted",
    )?;
    let op = evidence.operation();
    op.validate_application_write(table, key, &value, op.is_tombstone())
        .map_err(|e| e.to_string())?;
    Ok(Record {
        table: table.into(),
        key: key.into(),
        value,
        timestamp: op.version().timestamp_ms,
        logical: op.version().logical_counter,
        node: op.originating_node().into(),
        tombstone: op.is_tombstone(),
        expires: op.expires_at_ms(),
    })
}
fn integer(value: &QueryValue) -> Result<u64> {
    match value {
        QueryValue::Integer(value) => u64::try_from(*value).map_err(|e| e.to_string()),
        _ => Err("expected integer metadata".into()),
    }
}
fn text(value: &QueryValue) -> Result<String> {
    match value {
        QueryValue::Text(value) => Ok(value.clone()),
        _ => Err("expected text metadata".into()),
    }
}
async fn snapshot(client: &Client) -> Result<BTreeMap<String, Record>> {
    let mut records = BTreeMap::new();
    for table in TABLES {
        let result=client.table(table).map_err(|e|e.to_string())?.query(&format!(
            "SELECT key,value,timestamp_ms,logical_counter,node_id,is_tombstone,expires_at_ms FROM {table} INCLUDING TOMBSTONES ORDER BY key"
        )).await.map_err(|e|e.to_string())?;
        for row in result.rows {
            require(row.len() == 7, "metadata row width")?;
            let QueryValue::Bytes(value) = &row[1] else {
                return Err("expected opaque value".into());
            };
            let record = Record {
                table: table.into(),
                key: text(&row[0])?,
                value: value.clone(),
                timestamp: integer(&row[2])?,
                logical: integer(&row[3])?,
                node: text(&row[4])?,
                tombstone: integer(&row[5])? == 1,
                expires: if row[6] == QueryValue::Null {
                    None
                } else {
                    Some(integer(&row[6])?)
                },
            };
            let identity = record.identity();
            require(
                records.insert(identity, record).is_none(),
                "duplicate metadata row",
            )?;
        }
    }
    Ok(records)
}
fn compare_records(
    id: &str,
    expected: &BTreeMap<String, Record>,
    observed: &BTreeMap<String, Record>,
) -> Result<()> {
    require(
        observed.len() == expected.len(),
        format!("{id}: rows={}, expected={}", observed.len(), expected.len()),
    )?;
    if let Some((key, value)) = expected
        .iter()
        .find(|(key, value)| observed.get(*key) != Some(*value))
    {
        return Err(format!(
            "{id}: divergent/missing {} key {:?} version {:?}; observed version {:?}",
            value.table,
            value.key,
            value.version(),
            observed.get(key).map(Record::version)
        ));
    }
    Ok(())
}
impl Harness {
    fn new(args: Args) -> Result<Self> {
        let mut endpoints = BTreeMap::new();
        for spec in &args.node {
            let parts: Vec<_> = spec.split('@').collect();
            require(parts.len() == 3, "node must be id@app@control")?;
            require(
                ["node-a", "node-b", "node-c", "node-d"].contains(&parts[0]),
                "unknown node",
            )?;
            require(
                endpoints
                    .insert(
                        parts[0].into(),
                        Endpoint {
                            id: parts[0].into(),
                            app: parts[1].into(),
                            control: parts[2].into(),
                        },
                    )
                    .is_none(),
                "duplicate node",
            )?;
        }
        let needs_ledger = !["status", "fault", "await-bootstrap", "shrink-blocked"]
            .contains(&args.phase.as_str());
        let ledger = if needs_ledger && args.ledger.exists() {
            serde_json::from_reader(BufReader::new(
                File::open(&args.ledger).map_err(|e| e.to_string())?,
            ))
            .map_err(|e| e.to_string())?
        } else {
            Ledger::default()
        };
        Ok(Self {
            args,
            endpoints,
            ledger,
        })
    }
    fn endpoint(&self, id: &str) -> Result<&Endpoint> {
        self.endpoints
            .get(id)
            .ok_or_else(|| format!("missing node {id}"))
    }
    async fn configure(&mut self, owners: Vec<String>, epoch: Option<String>) -> Result<()> {
        self.ledger.revision += 1;
        for endpoint in self.endpoints.values() {
            if self.args.absent.as_ref() == Some(&endpoint.id) {
                continue;
            }
            control(endpoint,json!({"command":"configure","revision":self.ledger.revision,"owners":owners,"recovery_epoch":epoch})).await?;
        }
        self.ledger.owners = owners;
        self.ledger.recovery_epoch = epoch;
        self.ledger.save(&self.args.ledger)
    }
    async fn topology(&self) -> Result<()> {
        for endpoint in self.endpoints.values() {
            let status = control(endpoint, json!({"command":"status"})).await?;
            require(
                status["placement_complete"] == true,
                format!("{} incomplete placement", endpoint.id),
            )?;
            let assigned = self.ledger.owners.contains(&endpoint.id);
            let fragments = status["fragments"]
                .as_array()
                .ok_or("missing fragment status")?;
            require(
                fragments.len() == FRAGMENTS as usize,
                "incomplete fragment range",
            )?;
            for fragment in fragments {
                require(
                    fragment["replicas"] == json!(self.ledger.owners),
                    "stale published replica set",
                )?;
                if assigned {
                    require(
                        fragment["serving"] == true && fragment["state"] == "Serving",
                        format!("{} not serving: {fragment}", endpoint.id),
                    )?;
                } else {
                    require(
                        fragment["serving"] == false
                            && (fragment["state"].is_null() || fragment["state"] == "Stopped"),
                        format!("{} still holds revoked authority: {fragment}", endpoint.id),
                    )?;
                }
            }
            require(
                status["readiness"] == "Ready",
                format!("{} not ready: {status}", endpoint.id),
            )?;
        }
        Ok(())
    }
    async fn verify(&self) -> Result<()> {
        require(
            !self.ledger.records.is_empty(),
            "empty oracle cannot verify recovery",
        )?;
        let expected: BTreeMap<_, _> = self
            .ledger
            .records
            .iter()
            .map(|(id, r)| (id.clone(), r.at(now())))
            .collect();
        for id in &self.ledger.owners {
            let c = client(self.endpoint(id)?, Duration::from_secs(15)).await?;
            let observed = snapshot(&c).await;
            c.close().await;
            let observed = observed?;
            compare_records(id, &expected, &observed)?;
        }
        Ok(())
    }
    async fn write(
        &mut self,
        id: &str,
        table: &str,
        key: String,
        value: Vec<u8>,
        majority: bool,
    ) -> Result<()> {
        let c = client(self.endpoint(id)?, Duration::from_secs(15))
            .await?
            .table(table)
            .map_err(|e| e.to_string())?;
        let result = c
            .put_with_write_concern(&key, value.clone(), concern(majority))
            .await;
        c.close().await;
        let receipt = result.map_err(|e| e.to_string())?;
        self.ledger.add(certified(table, &key, value, &receipt)?);
        Ok(())
    }
    async fn barrier(&mut self, id: &str, phase: &str, majority: bool) -> Result<()> {
        for fragment in 0..FRAGMENTS {
            let key = key_for(fragment, &format!("fence:{phase}:{}", self.args.round));
            self.write(
                id,
                "KV",
                key,
                format!("stable:{phase}:{fragment}").into_bytes(),
                majority,
            )
            .await?;
        }
        self.ledger.save(&self.args.ledger)
    }
    async fn seed(&mut self) -> Result<()> {
        retry("genesis ready", async || self.topology().await).await?;
        let c = client(self.endpoint("node-a")?, Duration::from_secs(15)).await?;
        let mut generated = BTreeMap::new();
        for table in TABLES {
            let view = c.table(table).map_err(|e| e.to_string())?;
            for fragment in 0..FRAGMENTS {
                let values: Vec<_> = (0..self.args.records_per_fragment)
                    .map(|index| {
                        let key = key_for(fragment, &format!("seed:{index}:雪"));
                        let value = format!("{table}:{fragment}:{index}\0").into_bytes();
                        (key, value)
                    })
                    .collect();
                for chunk in values.chunks(32) {
                    let results = view
                        .put_multi(chunk.to_vec())
                        .await
                        .map_err(|e| e.to_string())?;
                    require(
                        results.len() == chunk.len()
                            && results.iter().all(std::result::Result::is_ok),
                        "seed batch not fully acknowledged",
                    )?;
                }
                for (key, value) in values {
                    generated.insert(format!("{table}\0{key}"), value);
                }
            }
        }
        let kv = c.table("KV").map_err(|e| e.to_string())?;
        for fragment in 0..FRAGMENTS {
            for index in 0..8 {
                let key = key_for(fragment, &format!("wide:{index}"));
                let value = vec![(fragment * 8 + index) as u8; 128 * 1024];
                kv.put(&key, value.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                generated.insert(format!("KV\0{key}"), value);
            }
        }
        let literal = "\u{001e}actordb.table.v1:5553455253:6b";
        kv.put(literal, vec![255, 0, 1])
            .await
            .map_err(|e| e.to_string())?;
        generated.insert(format!("KV\0{literal}"), vec![255, 0, 1]);
        let initial = snapshot(&c).await?;
        c.close().await;
        require(
            initial.len() == generated.len(),
            "seed cardinality mismatch",
        )?;
        for (id, value) in generated {
            require(
                initial
                    .get(&id)
                    .is_some_and(|r| r.value == value && !r.tombstone),
                "seed content mismatch",
            )?;
        }
        self.ledger.records = initial;
        self.barrier("node-a", "seed", true).await?;
        retry("seed oracle", async || self.verify().await).await
    }
    async fn partition(&mut self) -> Result<()> {
        // Local durable AP writes on the isolated side; majority writes on AB.
        for table in TABLES {
            for fragment in 0..FRAGMENTS {
                self.write(
                    "node-c",
                    table,
                    key_for(fragment, "partition:C"),
                    vec![3, 0, 255],
                    false,
                )
                .await?;
                self.write(
                    "node-a",
                    table,
                    key_for(fragment, "partition:AB"),
                    vec![1, 2, 255],
                    true,
                )
                .await?;
            }
        }
        for table in TABLES {
            let key = key_for(0, "same-key-conflict");
            self.write("node-a", table, key.clone(), vec![1], true)
                .await?;
            self.write("node-c", table, key.clone(), vec![2], false)
                .await?;
            let c = client(self.endpoint("node-c")?, Duration::from_secs(15))
                .await?
                .table(table)
                .map_err(|e| e.to_string())?;
            let receipt = c
                .delete_with_write_concern(&key, concern(false))
                .await
                .map_err(|e| e.to_string())?;
            self.ledger
                .add(certified(table, &key, Vec::new(), &receipt)?);
            c.close().await;
        }
        let c = client(self.endpoint("node-c")?, Duration::from_secs(15)).await?;
        let key = key_for(1, "missing-quorum");
        let unavailable_policy = WriteConcernOptions::builder()
            .concern(WriteConcern::Majority)
            .minimum_durability(WriteDurability::StableCommitted)
            .require_evidence(true)
            .strict_acknowledgements(true)
            .timeout(Duration::from_secs(2))
            .build();
        match c
            .put_with_write_concern(&key, vec![8], unavailable_policy)
            .await
        {
            Err(WriteConcernClientError::TimedOut(receipt)) => {
                require(
                    receipt.acknowledged_nodes().len()
                        < receipt.required_acknowledgements() as usize,
                    "false quorum success",
                )?;
                self.ledger.add(certified("KV", &key, vec![8], &receipt)?);
            }
            other => {
                return Err(format!(
                    "isolated majority must time out with evidence, got {other:?}"
                ))
            }
        }
        let ttl_key = key_for(2, "ttl-during-partition");
        let receipt = c
            .put_with_write_concern_options(
                &ttl_key,
                vec![7],
                PutOptions::builder().ttl(Duration::from_secs(1)).build(),
                concern(false),
            )
            .await
            .map_err(|e| e.to_string())?;
        self.ledger
            .add(certified("KV", &ttl_key, vec![7], &receipt)?);
        // Force volatile delta history overflow; durable cache recovery must repair it.
        let burst: Vec<_> = (0..512)
            .map(|i| {
                (
                    key_for(i % FRAGMENTS, &format!("overflow:{i}")),
                    vec![i as u8],
                )
            })
            .collect();
        for chunk in burst.chunks(32) {
            let result = c
                .put_multi(chunk.to_vec())
                .await
                .map_err(|e| e.to_string())?;
            require(
                result.len() == chunk.len() && result.iter().all(std::result::Result::is_ok),
                "partition burst not acknowledged",
            )?;
        }
        let isolated = snapshot(&c).await?;
        c.close().await;
        for (key, value) in &burst {
            let record = isolated
                .get(&format!("KV\0{key}"))
                .ok_or("acknowledged overflow key missing before recovery")?;
            require(
                record.value == *value && !record.tombstone && record.expires.is_none(),
                "overflow content differs from independently generated write",
            )?;
            self.ledger.add(record.clone());
        }
        require(
            self.ledger
                .records
                .values()
                .filter(|r| r.key.starts_with("overflow:"))
                .count()
                == 512,
            "overflow oracle incomplete",
        )?;
        self.barrier("node-c", "partition-C", false).await?;
        retry("visible sender reset", async || {
            let status = control(self.endpoint("node-c")?, json!({"command":"status"})).await?;
            require(
                status["senders"]
                    .as_array()
                    .ok_or("missing senders")?
                    .iter()
                    .any(|s| s["state"] == "ResetRequired"),
                "overflow reset not observed",
            )
        })
        .await?;
        self.ledger.save(&self.args.ledger)
    }
    async fn unavailable(&self) -> Result<()> {
        let c = client(self.endpoint(&self.args.target)?, Duration::from_secs(15)).await?;
        let key = self
            .ledger
            .records
            .values()
            .find(|r| r.table == "KV")
            .ok_or("no oracle key")?
            .key
            .clone();
        require(
            c.get(key).await.is_err(),
            "unavailable fragment must not report a miss/value",
        )?;
        require(
            c.query("SELECT COUNT(*) FROM KV").await.is_err(),
            "unavailable query must not return empty/partial success",
        )?;
        c.close().await;
        Ok(())
    }
    async fn storage_fault(&mut self) -> Result<()> {
        let id = self.args.target.clone();
        control(
            self.endpoint(&id)?,
            json!({"command":"fault","fail_store":true}),
        )
        .await?;
        let key = key_for(3, &format!("storage-fault:{}", self.args.round));
        let c = client(self.endpoint(&id)?, Duration::from_secs(15)).await?;
        let result = c
            .put_with_write_concern(&key, vec![9, 8, 7], concern(false))
            .await;
        require(
            result.is_err(),
            "failed backend must not certify stable storage",
        )?;
        let value = c
            .get(&key)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("failed store mutation was not observed locally")?;
        require(value.bytes == [9, 8, 7], "failed store read payload")?;
        self.ledger.add(Record {
            table: "KV".into(),
            key,
            value: value.bytes,
            timestamp: value.timestamp_ms,
            logical: value.logical_counter,
            node: value.node_id,
            tombstone: false,
            expires: value.expires_at_ms,
        });
        c.close().await;
        control(
            self.endpoint(&id)?,
            json!({"command":"fault","fail_store":false}),
        )
        .await?;
        self.barrier(&id, "storage-recovered", true).await?;
        retry("storage recovery convergence", async || self.verify().await).await
    }
    async fn timeout(&mut self) -> Result<()> {
        let id = self.args.target.clone();
        control(
            self.endpoint(&id)?,
            json!({"command":"fault","delay_ms":1200}),
        )
        .await?;
        let c = client(self.endpoint(&id)?, Duration::from_millis(300)).await?;
        let writing = c.clone();
        let key = key_for(4, &format!("unknown-outcome:{}", self.args.round));
        let submitted = key.clone();
        let task = tokio::spawn(async move { writing.put(submitted, vec![4, 3, 2]).await });
        retry("actual delayed storage job", async || {
            let status = control(self.endpoint(&id)?, json!({"command":"status"})).await?;
            require(
                status["active_storage"].as_u64().unwrap_or(0) > 0,
                "mutation never entered backend",
            )
        })
        .await?;
        let result = task.await.map_err(|e| e.to_string())?;
        require(
            matches!(result, Err(ClientError::UnknownOutcome(_))),
            "dispatched timeout must remain unknown",
        )?;
        c.close().await;
        control(self.endpoint(&id)?, json!({"command":"fault","delay_ms":0})).await?;
        retry("completion owns accepted mutation", async || {
            let c = client(self.endpoint(&id)?, Duration::from_secs(15)).await?;
            let result = c.get(&key).await;
            c.close().await;
            require(
                matches!(result,Ok(Some(ref value)) if value.bytes==[4,3,2]),
                "accepted mutation did not complete",
            )
        })
        .await?;
        let c = client(self.endpoint(&id)?, Duration::from_secs(15)).await?;
        let value = c
            .get(&key)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing resolved write")?;
        self.ledger.add(Record {
            table: "KV".into(),
            key,
            value: value.bytes,
            timestamp: value.timestamp_ms,
            logical: value.logical_counter,
            node: value.node_id,
            tombstone: false,
            expires: value.expires_at_ms,
        });
        c.close().await;
        self.barrier(&id, "timeout-resolved", true).await?;
        retry("timeout completion convergence", async || {
            self.verify().await
        })
        .await
    }
    async fn run(&mut self) -> Result<()> {
        let phase = self.args.phase.clone();
        match phase.as_str() {
            "genesis" => {
                self.ledger.owners = vec!["node-a".into()];
                retry("genesis topology", async || self.topology().await).await?;
            }
            "seed" => self.seed().await?,
            "grow-two" | "grow-three" | "grow-four" | "shrink" => {
                let owners = match phase.as_str() {
                    "grow-two" => vec!["node-a", "node-b"],
                    "grow-three" => vec!["node-a", "node-b", "node-c"],
                    "grow-four" => vec!["node-a", "node-b", "node-c", "node-d"],
                    _ => vec!["node-b", "node-c", "node-d"],
                }
                .into_iter()
                .map(str::to_owned)
                .collect();
                self.configure(owners, None).await?;
                if phase != "grow-four" && phase != "shrink" {
                    retry("placement ready", async || self.topology().await).await?;
                    retry("growth convergence", async || self.verify().await).await?;
                }
            }
            "verify" => {
                retry("placement ready", async || self.topology().await).await?;
                retry("exact durable oracle", async || self.verify().await).await?;
            }
            "partition" => self.partition().await?,
            "heal" => {
                self.barrier("node-a", "healed", true).await?;
                retry("partition heal", async || self.verify().await).await?;
            }
            "rolling" => {
                let id = self.args.target.clone();
                for table in TABLES {
                    for fragment in 0..FRAGMENTS {
                        self.write(
                            &id,
                            table,
                            key_for(fragment, &format!("rolling:{}", self.args.round)),
                            vec![self.args.round as u8, 5],
                            true,
                        )
                        .await?;
                    }
                }
                self.barrier(&id, "rolling", true).await?;
            }
            "unavailable" => self.unavailable().await?,
            "await-bootstrap" => {
                retry("new owner bootstrapping", async || {
                    let status = control(
                        self.endpoint(&self.args.target)?,
                        json!({"command":"status"}),
                    )
                    .await?;
                    require(
                        status["active_storage"].as_u64().unwrap_or(0) > 0
                            && status["fragments"]
                                .as_array()
                                .ok_or("missing fragment status")?
                                .iter()
                                .any(|f| f["state"] == "Bootstrapping" && f["serving"] == false),
                        "did not observe bootstrap under gated storage",
                    )
                })
                .await?
            }
            "shrink-blocked" => {
                retry("retirement waits for missing target", async || {
                    let endpoint = self.endpoint("node-a")?;
                    let status = control(endpoint, json!({"command":"status"})).await?;
                    require(
                        status["fragments"]
                            .as_array()
                            .ok_or("missing fragments")?
                            .iter()
                            .all(|f| f["state"] == "Draining" && f["serving"] == false),
                        "old owner not draining",
                    )?;
                    let counts = control(endpoint, json!({"command":"store_counts"})).await?;
                    require(
                        (0..FRAGMENTS)
                            .all(|id| counts["counts"][id.to_string()].as_u64().unwrap_or(0) > 0),
                        "old durable prefix deleted before handoff",
                    )
                })
                .await?;
                // Keep the target absent through a full reconciliation timeout,
                // checking every prefix rather than just one surviving table.
                let until = tokio::time::Instant::now() + Duration::from_secs(35);
                while tokio::time::Instant::now() < until {
                    let endpoint = self.endpoint("node-a")?;
                    let status = control(endpoint, json!({"command":"status"})).await?;
                    if status["placement_complete"] == true {
                        require(
                            status["fragments"]
                                .as_array()
                                .ok_or("missing fragments")?
                                .iter()
                                .all(|f| f["state"] == "Draining" && f["serving"] == false),
                            "premature retirement while target missing",
                        )?;
                    }
                    let counts = control(endpoint, json!({"command":"store_counts"})).await?;
                    require(
                        (0..FRAGMENTS)
                            .all(|id| counts["counts"][id.to_string()].as_u64().unwrap_or(0) > 0),
                        "a fragment prefix disappeared before full handoff",
                    )?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            "retired" => {
                retry("shrink ready", async || self.topology().await).await?;
                retry("shrunk oracle", async || self.verify().await).await?;
                let counts =
                    control(self.endpoint("node-a")?, json!({"command":"store_counts"})).await?;
                require(
                    counts["counts"].as_object().is_some_and(|v| v.is_empty()),
                    "retired namespace still durable",
                )?;
            }
            "arm-recovery" => {
                self.configure(
                    self.ledger.owners.clone(),
                    Some(self.args.epoch.clone().ok_or("missing recovery epoch")?),
                )
                .await?
            }
            "prepare-shrink" => {
                control(
                    self.endpoint("node-d")?,
                    json!({
                        "command":"configure","revision":self.ledger.revision+1,
                        "owners":["node-b","node-c","node-d"],"recovery_epoch":null,
                        "next_boot_only":true,
                    }),
                )
                .await?;
            }
            "blocked" => {
                let until = tokio::time::Instant::now() + Duration::from_secs(40);
                while tokio::time::Instant::now() < until {
                    let status = control(
                        self.endpoint(&self.args.target)?,
                        json!({"command":"status"}),
                    )
                    .await?;
                    require(
                        status["active_epoch"] == json!(self.ledger.recovery_epoch),
                        "wrong explicit recovery authority",
                    )?;
                    require(
                        status["fragments"]
                            .as_array()
                            .ok_or("missing fragments")?
                            .iter()
                            .all(|f| f["serving"] == false),
                        "recovery served before missing candidates",
                    )?;
                    self.unavailable().await?;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            "storage-fault" => self.storage_fault().await?,
            "timeout" => self.timeout().await?,
            "fault" => {
                control(self.endpoint(&self.args.target)?,json!({"command":"fault","isolated":self.args.isolated,"fail_store":self.args.fail_store,"delay_ms":self.args.delay_ms})).await?;
            }
            "status" => {
                let status = control(
                    self.endpoint(&self.args.target)?,
                    json!({"command":"status"}),
                )
                .await?;
                println!(
                    "RESULT {}",
                    serde_json::to_string(&status).map_err(|e| e.to_string())?
                );
            }
            other => return Err(format!("unknown phase {other}")),
        }
        if ![
            "status",
            "fault",
            "unavailable",
            "await-bootstrap",
            "blocked",
            "shrink-blocked",
        ]
        .contains(&phase.as_str())
        {
            self.ledger.save(&self.args.ledger)?;
        }
        println!(
            "SCENARIO_OK phase={phase} oracle_records={}",
            self.ledger.records.len()
        );
        Ok(())
    }
}
#[tokio::main]
async fn main() {
    let result = async { Harness::new(Args::parse())?.run().await }.await;
    if let Err(error) = result {
        eprintln!("SCENARIO_FAILED: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_oracle_rejects_missing_extra_and_divergent_metadata() {
        let record = Record {
            table: "KV".into(),
            key: "k".into(),
            value: vec![1, 0, 255],
            timestamp: 1,
            logical: 2,
            node: "node-a".into(),
            tombstone: false,
            expires: Some(10),
        };
        let expected = BTreeMap::from([(record.identity(), record.clone())]);
        assert!(compare_records("owner", &expected, &expected).is_ok());
        assert!(compare_records("owner", &expected, &BTreeMap::new()).is_err());
        let mut extra = expected.clone();
        let mut changed = record.clone();
        changed.key = "extra".into();
        extra.insert(changed.identity(), changed);
        assert!(compare_records("owner", &expected, &extra).is_err());
        for field in 0..7 {
            let mut changed = record.clone();
            match field {
                0 => changed.value.push(2),
                1 => changed.timestamp += 1,
                2 => changed.logical += 1,
                3 => changed.node = "node-b".into(),
                4 => changed.tombstone = true,
                5 => changed.expires = None,
                _ => changed.table = "USERS".into(),
            }
            let observed = BTreeMap::from([(record.identity(), changed)]);
            assert!(compare_records("owner", &expected, &observed).is_err());
        }
    }
    #[test]
    fn literal_sharding_oracle_and_lww_tombstones_are_deterministic() {
        for fragment in 0..FRAGMENTS {
            require(
                seahash::hash(key_for(fragment, "雪").as_bytes()) % 6 == u64::from(fragment),
                "routing",
            )
            .unwrap();
        }
        let a = Record {
            table: "KV".into(),
            key: "k".into(),
            value: vec![1],
            timestamp: 1,
            logical: 2,
            node: "a".into(),
            tombstone: false,
            expires: Some(10),
        };
        let mut ledger = Ledger::default();
        ledger.add(a.clone());
        ledger.add(a.at(10));
        assert!(ledger.records[&a.identity()].tombstone);
        ledger.add(a);
        assert!(ledger.records.values().next().unwrap().tombstone);
    }
}
