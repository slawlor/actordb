//! Standalone external SDK scenario. No database implementation is linked.
use actordb_client::{
    Client, ClientError, ClientOptions, Priority, PutOptions, QueryResult, QueryValue, ScanOptions,
};
use clap::Parser;
use std::error::Error;
use std::io::{self, Write};
use std::time::Duration;

#[derive(Parser)]
struct Arguments {
    /// Application endpoints, in node-a, node-b, node-c order; accepts DNS names.
    #[arg(long, required = true, num_args = 1)]
    endpoint: Vec<String>,
    /// Public test credential; independently configured on every listener.
    #[arg(long)]
    credential: Option<String>,
    #[arg(long, default_value_t = 180, value_parser = clap::value_parser!(u64).range(1..=180))]
    timeout_secs: u64,
}

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const PREFIX: &str = "client-scenario:";
const POLL: Duration = Duration::from_millis(100);

fn check(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string().into())
    }
}
fn marker(message: &str) -> Result<()> {
    println!("{message}");
    io::stdout().flush()?;
    Ok(())
}
fn options(credential: Option<&str>) -> ClientOptions {
    ClientOptions::builder()
        .identity("external-sdk-scenario")
        .maybe_credential(credential.map(|secret| secret.as_bytes().to_vec()))
        .timeout(Duration::from_secs(5))
        .build()
}
async fn connect(endpoint: &str, credential: Option<&str>) -> Result<Client> {
    let addr = tokio::net::lookup_host(endpoint)
        .await?
        .next()
        .ok_or("endpoint resolved to no addresses")?;
    Ok(Client::connect(addr, options(credential)).await?)
}
async fn wait_connect(endpoint: &str, credential: Option<&str>) -> Client {
    loop {
        match connect(endpoint, credential).await {
            Ok(client) => return client,
            Err(_) => tokio::time::sleep(POLL).await,
        }
    }
}
fn key_for(fragment: u32, prefix: &str) -> String {
    (0u64..)
        .map(|index| format!("{prefix}f{fragment}:{index}"))
        .find(|key| seahash::hash(key.as_bytes()) % 6 == u64::from(fragment))
        .unwrap()
}
fn expected() -> Vec<(String, Vec<u8>, u32)> {
    let mut records: Vec<_> = (0..6)
        .map(|fragment| {
            (
                key_for(fragment, PREFIX),
                (10 + fragment).to_string().into_bytes(),
                fragment,
            )
        })
        .collect();
    records.sort_by(|a, b| a.0.cmp(&b.0));
    records
}
fn expected_rows(records: &[(String, Vec<u8>, u32)]) -> Vec<Vec<QueryValue>> {
    records
        .iter()
        .map(|(key, value, fragment)| {
            vec![
                QueryValue::Text(key.clone()),
                QueryValue::Bytes(value.clone()),
                QueryValue::Integer(i128::from(*fragment)),
            ]
        })
        .collect()
}
fn query_text() -> &'static str {
    "SELECT key, value, frag FROM kv WHERE STARTS_WITH(key, ?) ORDER BY key ASC"
}
async fn full_rows(client: &Client) -> std::result::Result<QueryResult, ClientError> {
    client
        .query_with_params(query_text(), &[QueryValue::Text(PREFIX.into())])
        .await
}
async fn wait_replica_rows(client: &Client, records: &[(String, Vec<u8>, u32)]) {
    let wanted = expected_rows(records);
    loop {
        if matches!(full_rows(client).await, Ok(result) if result.rows == wanted) {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}
async fn wait_value(client: &Client, key: &str, value: Option<&[u8]>) {
    loop {
        let visible = client.get(key).await;
        let matches = match (&visible, value) {
            (Ok(None), None) => true,
            (Ok(Some(actual)), Some(expected)) => actual.bytes == expected,
            _ => false,
        };
        if matches {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}
async fn healthy(clients: &[Client], records: &[(String, Vec<u8>, u32)]) -> Result<()> {
    for (index, client) in clients.iter().enumerate() {
        loop {
            let inspection = client.inspect().await?;
            check(
                inspection.node_id == ["node-a", "node-b", "node-c"][index],
                "endpoint identity mismatch",
            )?;
            if inspection.peer_count == 2 && inspection.health.len() == 4 {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
    }
    let client = &clients[0];
    // Two keys (fragments2/3) are routed by node-a to remote holders.
    for (index, (key, value, _)) in records.iter().enumerate() {
        client
            .put_with_priority(
                key.clone(),
                value.clone(),
                [Priority::High, Priority::Default, Priority::Background][index % 3],
            )
            .await?;
    }
    for peer in clients {
        wait_replica_rows(peer, records).await;
    }
    let keys = vec![
        records[2].0.clone(),
        records[1].0.clone(),
        "client-extra:missing".into(),
        records[2].0.clone(),
    ];
    let values = client.get_multi_with_priority(keys, Priority::High).await?;
    check(values.len() == 4, "batch read cardinality")?;
    for (index, expected) in [(0, &records[2].1), (1, &records[1].1), (3, &records[2].1)] {
        let value = values[index]
            .as_ref()
            .map_err(|error| error.to_string())?
            .as_ref()
            .ok_or("ordered batch value absent")?;
        check(
            &value.bytes == expected,
            "batch read position/duplicate mismatch",
        )?;
        check(
            value.timestamp_ms > 0 && !value.node_id.is_empty(),
            "missing value version metadata",
        )?;
    }
    check(
        values[2]
            .as_ref()
            .map_err(|error| error.to_string())?
            .is_none(),
        "missing result conflated with error",
    )?;
    let batch_local = key_for(0, "client-extra:batch:");
    let batch_remote = key_for(2, "client-extra:batch:");
    let batch_remote_other = key_for(3, "client-extra:batch:");
    let duplicates = client
        .put_multi_with_priority(
            vec![
                (batch_local.clone(), b"local".to_vec()),
                (batch_remote.clone(), b"first".to_vec()),
                (batch_remote_other.clone(), b"other".to_vec()),
                (batch_remote.clone(), b"second".to_vec()),
            ],
            Priority::Background,
        )
        .await?;
    check(
        duplicates.len() == 4 && duplicates.iter().all(|result| result.is_ok()),
        "multifragment duplicate batch write outcomes",
    )?;
    for peer in clients {
        wait_value(peer, &batch_local, Some(b"local")).await;
        wait_value(peer, &batch_remote, Some(b"second")).await;
        wait_value(peer, &batch_remote_other, Some(b"other")).await;
    }
    for key in [&batch_local, &batch_remote, &batch_remote_other] {
        client
            .delete_with_priority(key, Priority::Background)
            .await?;
        for peer in clients {
            wait_value(peer, key, None).await;
        }
    }
    let ttl_key = key_for(4, "client-extra:ttl:");
    client
        .put_with_options(
            &ttl_key,
            b"expires".to_vec(),
            PutOptions::builder()
                .ttl(Duration::from_secs(5))
                .priority(Priority::High)
                .build(),
        )
        .await?;
    let ttl = client
        .get(&ttl_key)
        .await?
        .ok_or("TTL value absent before expiry")?;
    check(ttl.expires_at_ms.is_some(), "TTL metadata missing")?;
    for peer in clients {
        wait_value(peer, &ttl_key, Some(b"expires")).await;
    }
    for peer in clients {
        wait_value(peer, &ttl_key, None).await;
    }
    let row_query = client.query_with_params("SELECT key, PREFIX_BYTES(value, 1), OCTET_LENGTH(value) FROM kv WHERE STARTS_WITH(key, ?) ORDER BY key ASC LIMIT 3 OFFSET 2", &[QueryValue::Text(PREFIX.into())]).await?;
    let wanted: Vec<_> = records[2..5]
        .iter()
        .map(|(key, value, _)| {
            vec![
                QueryValue::Text(key.clone()),
                QueryValue::Bytes(value[..1].to_vec()),
                QueryValue::Integer(value.len() as i128),
            ]
        })
        .collect();
    check(
        row_query.rows == wanted,
        "global order/offset/limit/projection mismatch",
    )?;
    let aggregate = client.query_with_params("SELECT COUNT(*), SUM(CAST(value AS BIGINT)), MIN(CAST(value AS BIGINT)), MAX(CAST(value AS BIGINT)), SUM(OCTET_LENGTH(value)) FROM kv WHERE STARTS_WITH(key, ?)", &[QueryValue::Text(PREFIX.into())]).await?;
    check(
        aggregate.rows
            == vec![vec![
                QueryValue::Integer(6),
                QueryValue::Integer(75),
                QueryValue::Integer(10),
                QueryValue::Integer(15),
                QueryValue::Integer(12),
            ]],
        "exact aggregate mismatch",
    )?;
    let scoped = client.query_with_params("SELECT key, value, frag FROM kv WHERE STARTS_WITH(key, ?) AND frag IN (0, 2) ORDER BY key ASC", &[QueryValue::Text(PREFIX.into())]).await?;
    let selected: Vec<_> = records
        .iter()
        .filter(|(_, _, fragment)| [0, 2].contains(fragment))
        .cloned()
        .collect();
    check(
        scoped.rows == expected_rows(&selected),
        "fragment-scoped query mismatch",
    )?;
    check(
        client
            .query("SELECT key FROM kv LIMIT 0")
            .await?
            .rows
            .is_empty(),
        "LIMIT0 returned records",
    )?;
    // Scans use the public SQL/scan surface without any server type dependency.
    let scan = client
        .scan(ScanOptions {
            matcher_kind: 1,
            prefix: PREFIX.as_bytes().to_vec(),
            transform_kind: 2,
            prefix_bytes: 1,
            limit: 10,
            offset: 0,
        })
        .await?;
    let mut scan: Vec<_> = scan
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect();
    scan.sort_by(|a, b| a.0.cmp(&b.0));
    check(
        scan == records
            .iter()
            .map(|(key, value, _)| (key.clone(), value[..1].to_vec()))
            .collect::<Vec<_>>(),
        "bounded scan projection mismatch",
    )?;
    for peer in clients {
        wait_replica_rows(peer, records).await;
        check(
            peer.inspect().await?.peer_count == 2,
            "SDK changed database peer membership",
        )?;
    }
    Ok(())
}
async fn scenario(arguments: Arguments) -> Result<()> {
    check(
        arguments.endpoint.len() == 3,
        "supply exactly3 --endpoint values in node-a,b,c order",
    )?;
    let credential = arguments.credential.as_deref();
    check(
        credential.is_none_or(|value| value.len() >= 32),
        "test credential must contain at least32bytes",
    )?;
    let mut clients = Vec::new();
    for endpoint in &arguments.endpoint {
        clients.push(wait_connect(endpoint, credential).await);
    }
    let mut wrong = options(credential);
    let mut wrong_key = credential
        .map(|secret| secret.as_bytes().to_vec())
        .unwrap_or_else(|| vec![b'!'; 32]);
    wrong_key[0] ^= 1;
    wrong.credential = Some(wrong_key);
    let address = tokio::net::lookup_host(arguments.endpoint[0].as_str())
        .await?
        .next()
        .ok_or("empty endpoint")?;
    check(
        Client::connect(address, wrong).await.is_err(),
        "wrong application credential was accepted",
    )?;
    let records = expected();
    healthy(&clients, &records).await?;
    marker("CLIENT_HEALTHY_OK")?;
    marker("CLIENT_WAITING_FOR_NODE_A_FAILURE")?;
    loop {
        match clients[0].get(&records[0].0).await {
            Err(
                ClientError::Transport(_)
                | ClientError::Closed
                | ClientError::Timeout
                | ClientError::Protocol(_),
            ) => break,
            Ok(_) => {}
            Err(ClientError::Server(_)) => {}
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(POLL).await;
    }
    check(
        matches!(
            clients[0].get(&records[0].0).await,
            Err(ClientError::Closed)
        ),
        "dead handle did not close after transport failure",
    )?;
    loop {
        if clients[1].inspect().await?.peer_count == 1
            && clients[2].inspect().await?.peer_count == 1
        {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    // Reconnection is explicit. Never replay a possibly-dispatched mutation.
    let survivor = connect(&arguments.endpoint[1], credential).await?;
    check(
        full_rows(&survivor).await?.rows == expected_rows(&records),
        "surviving replicas lost baseline rows",
    )?;
    check(
        survivor
            .query_with_params(
                "SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv WHERE STARTS_WITH(key, ?)",
                &[QueryValue::Text(PREFIX.into())],
            )
            .await?
            .rows
            == vec![vec![QueryValue::Integer(6), QueryValue::Integer(75)]],
        "surviving replicas aggregate mismatch",
    )?;
    check(
        survivor.inspect().await?.peer_count == 1,
        "new SDK connection joined peer membership",
    )?;
    let recovery_key = key_for(4, "client-failover:");
    survivor
        .put_with_priority(&recovery_key, b"77".to_vec(), Priority::High)
        .await?;
    wait_value(&survivor, &recovery_key, Some(b"77")).await;
    wait_value(&clients[2], &recovery_key, Some(b"77")).await;
    let recovery_version = survivor
        .get(&recovery_key)
        .await?
        .ok_or("failover write absent")?;
    marker("CLIENT_FAILOVER_OK")?;
    // The driver restarts node-a only after the failover marker.
    loop {
        if let Ok(recovered) = connect(&arguments.endpoint[0], credential).await {
            let ready = recovered.inspect().await.is_ok_and(|state| {
                state.peer_count == 2
                    && state.health.len() == 4
                    && state
                        .placement
                        .iter()
                        .any(|row| row.fragment == 4 && row.state == "Serving")
            }) && recovered.get(&recovery_key).await.is_ok_and(|value| {
                value.is_some_and(|value| {
                    value.bytes == recovery_version.bytes
                        && value.timestamp_ms == recovery_version.timestamp_ms
                        && value.node_id == recovery_version.node_id
                })
            }) && full_rows(&recovered)
                .await
                .is_ok_and(|result| result.rows == expected_rows(&records))
                && recovered
                    .query_with_params(
                        "SELECT key, value, frag FROM kv WHERE key = ? AND frag = 4",
                        &[QueryValue::Text(recovery_key.clone())],
                    )
                    .await
                    .is_ok_and(|result| {
                        result.rows
                            == vec![vec![
                                QueryValue::Text(recovery_key.clone()),
                                QueryValue::Bytes(b"77".to_vec()),
                                QueryValue::Integer(4),
                            ]]
                    });
            let ready = ready && recovered.query_with_params("SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv WHERE STARTS_WITH(key, ?) AND frag = 4", &[QueryValue::Text("client-failover:".into())]).await.is_ok_and(|result| result.rows == vec![vec![QueryValue::Integer(1), QueryValue::Integer(77)]]);
            if ready {
                check(
                    recovered.inspect().await?.peer_count == 2,
                    "recovery SDK joined peer membership",
                )?;
                recovered.close().await;
                break;
            }
            recovered.close().await;
        }
        tokio::time::sleep(POLL).await;
    }
    marker("CLIENT_RECOVERY_OK")?;
    survivor.close().await;
    for client in clients {
        client.close().await;
    }
    marker("SCENARIO_OK")?;
    Ok(())
}
#[tokio::main]
async fn main() {
    let arguments = Arguments::parse();
    let deadline = Duration::from_secs(arguments.timeout_secs);
    let result = tokio::time::timeout(deadline, scenario(arguments)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("CLIENT_SCENARIO_ERROR: {error}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("CLIENT_SCENARIO_ERROR: overall deadline exceeded");
            std::process::exit(1);
        }
    }
}
