// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Run with `cargo run -p actordb --example peer_fence`.
//! Generations are operational IDs; production transport authentication and
//! credential revocation remain the application's responsibility.

use actordb::node::{Node, NodeConfig, NodeRuntimeOptions, PeerAuthorityOptions, PeerFenceOptions};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut secret = vec![0; 32];
    getrandom::fill(&mut secret)?;
    let a_policy = PeerAuthorityOptions::new(1, HashMap::from([("b".into(), 1)]));
    let b_policy = PeerAuthorityOptions::new(1, HashMap::from([("a".into(), 1)]));
    let a = Node::start_with_options(
        "a".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::builder()
            .maybe_cluster_secret(Some(secret.clone()))
            .build(),
        NodeRuntimeOptions::default().with_peer_authority(a_policy.clone()),
    )
    .await?;
    let b = Node::start_with_options(
        "b".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::builder()
            .maybe_cluster_secret(Some(secret))
            .build(),
        NodeRuntimeOptions::default().with_peer_authority(b_policy),
    )
    .await?;
    a.connect(b.addr()).await?;
    a.put("example".into(), b"opaque value".to_vec()).await?;
    // This local receiver revokes b. Application writes remain admitted;
    // other receivers need their own externally coordinated authority update.
    let next = a_policy.without_peer("b");
    a.begin_peer_fence(next.clone())?;
    let report = a
        .advance_peer_authority(next, PeerFenceOptions::default())
        .await?;
    println!(
        "Receiver revision {} fenced old peer work at {:?}",
        report.revision(),
        report.local_durability()
    );
    b.shutdown().await?;
    a.shutdown().await?;
    Ok(())
}
