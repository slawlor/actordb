//! Run with `cargo run -p actordb-client --example query_demo`.

use std::sync::Arc;

use actordb::client_server::{ClientServer, ClientServerOptions};
use actordb::node::{Node, NodeConfig};
use actordb_client::{Client, ClientOptions, QueryValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Arc::new(
        Node::start(
            "query-demo".into(),
            "127.0.0.1:0".parse()?,
            NodeConfig::builder().fragment_count(4).build(),
        )
        .await?,
    );
    let mut server = ClientServer::bind(
        node.clone(),
        "127.0.0.1:0".parse()?,
        ClientServerOptions::default(),
    )
    .await?;
    let client = Client::connect(server.addr(), ClientOptions::default()).await?;
    for index in 0..30 {
        client
            .put(format!("user:{index:03}"), index.to_string().into_bytes())
            .await?;
    }
    let scoped = client
        .query_with_params(
            "SELECT key, frag FROM kv WHERE STARTS_WITH(key, ?) AND frag IN (?, ?) LIMIT 5",
            &[
                QueryValue::Text("user:".into()),
                QueryValue::Integer(0),
                QueryValue::Integer(1),
            ],
        )
        .await?;
    println!("Rows from fragments 0 and 1: {:?}", scoped.rows);
    let aggregate = client
        .query("SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv")
        .await?;
    println!("Count and sum across all fragments: {:?}", aggregate.rows);
    client.close().await;
    server.shutdown().await;
    node.shutdown().await?;
    Ok(())
}
