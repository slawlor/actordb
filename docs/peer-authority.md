# Authenticated peer generations and hot fencing

Configure `NodeRuntimeOptions::with_peer_authority` to authenticate each peer
with an independent Ed25519 process key and externally allocated generation.
This mode uses signed handshake proofs in place of the legacy shared cluster
secret. A node stores its own signing seed and its authorized peers' public
keys; a verifier's public key cannot sign on the peer's behalf.

```rust
use actordb::node::{NodeRuntimeOptions, PeerAuthorityOptions, PeerCredential};
use std::collections::HashMap;

// Obtain these independently generated seeds through trusted provisioning.
// Only the corresponding process receives a signing seed. Other processes
// receive the public_identity(), together with its expected node ID/generation.
# let local_seed = [1; 32];
# let remote_seed = [2; 32];
let local = PeerCredential::new(41, local_seed);
let remote_public = PeerCredential::new(73, remote_seed).public_identity();
let authority = PeerAuthorityOptions::new(
    local,
    HashMap::from([("replica-b".to_string(), remote_public)]),
);
let runtime = NodeRuntimeOptions::default().with_peer_authority(authority);
```

A runnable two-node example is in
[`actordb/examples/peer_fence.rs`](../actordb/examples/peer_fence.rs).

The application supplies identity/key provisioning and generation allocation.
Use 32 cryptographically random seed bytes. A generation claim or node ID
alone grants no authority: the receiver checks its exact configured identity
and generation, then verifies a signature binding both peers' IDs, generations,
fresh challenges, protocol offers, negotiated capabilities and proving role.
Generation zero denotes legacy cluster authentication and is rejected in this
mode. A peer that lacks the generation capability is also rejected.

## Retiring a process while keeping the receiver alive

`begin_peer_fence(next_authority)` validates the entire next policy, atomically
closes current peer frame/handshake admission and cancels its registered
connections. It does not change embedded/application request admission.
`advance_peer_authority(next_authority, PeerFenceOptions)` starts that same
transition if necessary, waits for old accepted work and local store completion,
and only then opens the exact new policy.

```rust
use actordb::node::{Node, PeerAuthorityOptions, PeerFenceError, PeerFenceOptions};
use std::time::Duration;

async fn replace_peer(node: &Node, next: PeerAuthorityOptions) -> Result<(), PeerFenceError> {
    node.begin_peer_fence(next.clone())?;
    let report = node.advance_peer_authority(
        next, PeerFenceOptions::default().with_timeout(Duration::from_secs(30)),
    ).await?;
    println!("receiver authority revision {} completed", report.revision());
    Ok(())
}
```

Remove a revoked ID from the next authorized set, or replace it with a strictly
newer generation and a fresh public key belonging to its replacement process.
Unchanged authorized peers may retain their identity/key; their old physical
sessions are still retired and they reconnect under the new receiver policy.
Changing the local signing key also requires a newer local generation.
Readding a removed identity requires a newer generation and fresh key. The
receiver remembers retired identities and every admitted public key during its
process lifetime. A historical key can never be recycled, even after several
rotations or under a different node ID. Only an exactly unchanged current
identity/generation can retain its current key. Persist the desired authority
and never-reuse key allocation history externally across receiver restarts;
a process-local revision is not a durable membership epoch.

The authority allows at most 1024 current peers and remembers at most 65536
distinct historical identities and 65536 admitted public keys. Advancing an
existing identity consumes a fresh key slot. Exhaustion rejects new keys before
closing admission, never evicts tombstones, and still permits revocation with
unchanged surviving keys. Policy validation occurs
before the admission cutoff. An unchanged initial policy is rejected because
it would revoke nothing. Repeating an already completed transition returns
that transition's original report.

Timeout, cancellation or local persistence failure leaves the explicit
`Fencing` state and exact pending policy owned by the node. New and old
handshakes/frames remain excluded. Retry with the same policy and a fresh
timeout; a different pending policy is rejected. Zero/unrepresentable timeouts
are rejected before starting a transition. There is no force-open or downgrade
to unauthenticated peers. Successful node shutdown permanently closes peer
authority and joins its old frame/persistence ownership before store release.

## What the completion report proves

Every authenticated decoded frame acquires a lease before it can observe an
HLC timestamp, forward a request, affect a sender ACK, reconcile, or dispatch
actor work. Cutoff and lease acquisition are coordinated. Work admitted before
the cutoff may complete during the fence; the new policy cannot open until its
leases finish. Leases follow actor messages and queued sender/responder work,
including cancelled/timed-out reads, SQL, forwarded mutations and replication.
`connection_valid` means validation and cannot release a reconciliation page's
lease before its actual apply finishes.

After old work completes, the fence flushes every current fragment through its
configured local store contract and joins storage execution. A cancelled
flush RPC stays counted until its actor/store work ends; retry waits for that
same work before issuing an idempotent flush again. `MemoryApplied` remains
volatile. `StoreCommitted` means the backend accepted the flush and does not
imply stable disk storage. This is local evidence, independent of remote ACKs.

Transport retirement may reject an unissued batch suffix or a stale queued
page, with no successful ACK for an incomplete batch. A rejected frame is not
evidence that all its records applied. Issued actor work reaches a terminal
outcome before the fence succeeds. No old session can admit another frame
after the cutoff, including a session whose reader already decoded a frame
while reconfiguration began. Buffered response bytes may still be lost.

Application writes and TTL/maintenance can continue during a peer fence.
Use request draining or the application's writer/admission policy when a
replacement also requires a fixed data floor or exclusive writer boundary.
The fence does not prove remote convergence, joint membership, host death,
latest-writer authority, or a successful handoff by an unavailable instance.
Each receiver must apply the application's intended authority transition.
An authorized intermediary can relay pre-fence work to another node: a local
fence is not proof that all downstream receivers retired that work.

Historical writer provenance is independent of sender authority. An authorized
current peer can import a record or tombstone stamped with a retired writer ID.
The SDK fences the authenticated process/session, not the record's origin.
Checkpoint import never grants the historical writer permission to connect.

## Observation and transport requirements

`peer_fence_status()` reads a bounded snapshot of current/pending receiver
revisions, local/peer generations, outstanding old frames and persistence,
retiring registered sessions and missing configured peers. Revisions reset at
node startup and are scoped to this receiver process. During `Fencing`, no
current session is eligible; missing peers do not mean an application writer
lease is unavailable. Legacy `Unconfigured` authority has no declared expected
peer set, so an empty missing list does not certify complete membership.

The snapshot also exposes cumulative admitted/completed/rejected frame counts
and authority/signature-rejected handshakes. Completed frames count terminal
ownership release, including errors, rather than successful writes. Counters
reset at node startup and survive hot authority transitions. Collections do not
consume them. Fence stage and a retained local blocking error distinguish
waiting actor/persistence work from a failed store fence.

`peer_authority_snapshot()` and `PeerFenceReport::public_authority()` expose the
exact current/completed public key and generation policy for qualification
against an externally persisted membership/checkpoint decision. They contain
the receiver ID, local public identity and expected peer public identities.
Require the current snapshot's `Ready` phase and exact identity/key/generation
matches before activation. A completed report is evidence for that past local
transition; later policy changes or shutdown must be checked separately. An
external membership epoch/decision ID remains application authority, not the
receiver's process-local revision.

Signing seed Debug output is redacted. Status contains no seed, signature,
public key, request payload or per-key labels. Peer keys are verified during
the handshake; post-handshake plaintext TCP still needs a protected
authenticated `Network` transport to resist channel injection or tampering.
Provision new signing seeds so the retired instance cannot obtain them.

`Hello::process_generation` is a new public protobuf struct field. Exhaustive
Rust `Hello` literals must add it (zero for legacy mode), requiring the 0.9
release. Legacy generation-zero handshakes retain their existing transcript
and protocol compatibility; process-signature mode requires both endpoints to
support the authenticated-generation capability.
