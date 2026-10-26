# Peer generation fencing

Peer authority is an operational allowlist and a local retained-work barrier.
The application allocates monotonically increasing generations and supplies
the permitted generation for each logical node ID. It is not a membership
allocator, consensus protocol, writer lease, or proof of process ownership.

Transport authentication belongs to the application: use TLS with appropriate
certificate verification, an externally authenticated network, or the optional
cluster shared-secret handshake. Plain TCP and a shared-secret handshake do not
provide post-handshake confidentiality or frame integrity. A process that still
has valid transport credentials can claim a permitted ID and generation.
Excluding such a process requires external certificate or shared-secret
revocation; changing the generation allowlist alone is not a security boundary.

## Configure the current generation cohort

Use the exact current peer protocol. All current RPC, query, scan, table,
reconciliation, and durability semantics are mandatory; no capability or
version-range negotiation takes place.

The following example assumes the application provides `shared_secret` as
`Option<Vec<u8>>` and configures its trusted network/TLS independently:

```rust
use actordb::node::{
    Node, NodeConfig, NodeRuntimeOptions, PeerAuthorityOptions, PeerFenceOptions,
};
use std::collections::HashMap;

let authority = PeerAuthorityOptions::new(
    7,
    HashMap::from([("node-b".to_owned(), 9)]),
);
let node = Node::start_with_options(
    "node-a".into(),
    "127.0.0.1:7447".parse()?,
    NodeConfig::builder().maybe_cluster_secret(shared_secret).build(),
    NodeRuntimeOptions::default().with_peer_authority(authority.clone()),
).await?;

let next = authority.with_peer("node-b", 10);
let report = node.advance_peer_authority(next, PeerFenceOptions::default()).await?;
assert_eq!(report.public_authority().peer_generations()["node-b"], 10);
node.shutdown().await?;
```

Configured generations must be nonzero, IDs contain 1–255 bytes, and at most
1024 peers may be permitted. An unconfigured allowlist uses generation zero and
still requires the exact current wire protocol. Authentication choice is local
configuration, not a negotiated mode. Shared-secret mismatch fails the handshake.

An unchanged still-present ID may retain its generation. Replacing or readding
an ID requires a generation above every previously admitted generation for that
ID during this receiver's lifetime. Historical ID admission is bounded at 65,536
entries; exhaustion rejects additions before closing admission and never evicts
generation history. The application must persist its own generation-allocation
and credential-revocation policy across restarts.

## Advance a receiver fence

`begin_peer_fence(next)` validates the entire policy before mutation, atomically
closes old admission, and cancels old physical sessions. No handshake or frame
may enter while the transition is pending. Every accepted frame retains its
authority lease through queued actor/storage work and queued responses, even
when its caller times out or disappears.

`advance_peer_authority(next, options)` joins those old leases, waits retained
persistence work, reaches the configured local store fence, and opens the exact
next generation cohort. Timeout or cancellation leaves the pending policy owned
and admission closed. Retry with the identical policy; a different pending policy
is rejected. A completed identical retry returns the same report. Shutdown
permanently closes authority and joins outstanding leases before releasing storage.

Application request admission is independent of this peer fence. Local completion
does not certify remote durability, cache equality, checkpoint completeness, or
the retirement of a remote process. Membership certification separately qualifies
the receiver's current generation map and boot identity.

Historical record writer IDs are provenance, never session credentials. A current
permitted peer may reconcile records originally written by a retired node without
making that writer eligible to reconnect.

## Observe and integrate

`peer_fence_status()` reports current/pending generations, revision, missing
peers, retained frame/persistence counts, progress, and cumulative admissions and
rejections. `peer_authority_snapshot()` captures the exact generation policy,
receiver ID, receiver boot ID, revision, and phase without an actor/store RPC.

Reports are receiver-local. Validate them against the live Node and current
boot/revision before activation; they are not reusable distributed authorization
tokens. The fresh challenge proof binds the exact current Hello fields, including
generation zero, but authenticates only possession of the shared secret.

## Current peer framing and handshake

Every frame is a four-byte big-endian body length followed by one protobuf
Envelope, with a 16 MiB maximum body. There is no per-frame identity header.

Hello contains node ID, exact protocol version, a fresh 32-byte challenge, and
process generation. HelloAuth is empty when no shared secret is configured,
otherwise HMAC-SHA256 under the configured cluster secret.

The proof input is ASCII `actordb-handshake-proof-v6` plus NUL, the proving role
(`dialer` or `listener`), then the canonical ordered Hello transcript. The
transcript begins with ASCII `actordb-handshake-transcript-v6` plus NUL. For the
dialer then listener, append role length (u64) and bytes, node-ID length (u64) and
UTF-8 bytes, exact protocol version (u32), challenge length (u64) and bytes, and
process generation (u64), including zero. All integers are big-endian.

Both independent fresh challenges, roles, identities, versions, and generations
are bound. A proof from another session or role is rejected. Post-handshake
traffic integrity remains the responsibility of the configured transport.
