// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Run with `cargo run -p actordb --example peer_fence`.
//! Production provisioning distributes public identities independently and
//! gives each process only its own fresh signing seed.

use actordb::node::{
    Node, NodeConfig, NodeRuntimeOptions, PeerAuthorityOptions, PeerCredential, PeerFenceOptions,
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut a_seed = [0; 32];
    getrandom::fill(&mut a_seed)?;
    let mut b_seed = [0; 32];
    getrandom::fill(&mut b_seed)?;
    let a_key = PeerCredential::new(1, a_seed);
    let b_key = PeerCredential::new(1, b_seed);
    let a_policy = PeerAuthorityOptions::new(
        a_key.clone(),
        HashMap::from([("b".into(), b_key.public_identity())]),
    );
    let b_policy = PeerAuthorityOptions::new(
        b_key,
        HashMap::from([("a".into(), a_key.public_identity())]),
    );
    let a = Node::start_with_options(
        "a".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::default(),
        NodeRuntimeOptions::default().with_peer_authority(a_policy.clone()),
    )
    .await?;
    let b = Node::start_with_options(
        "b".into(),
        "127.0.0.1:0".parse()?,
        NodeConfig::default(),
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
