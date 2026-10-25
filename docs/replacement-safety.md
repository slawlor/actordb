# Replacement and membership safety

An embedding application owns its serving admission, authenticated peer
authority and membership transition policy. ActorDB's local readiness,
replication acknowledgements and recovery source qualification provide
evidence for that policy; they do not elect a writer or prove that an old
process is dead.

## Retiring old sessions

Changing placement or closing a connection does not revoke a process that
still holds valid peer credentials. Already decoded replication or
reconciliation can also be applying after its stream closes. Successful
full node shutdown is a supported receive/apply completion barrier. The
authenticated generation API now also provides a hot receiver barrier:
configure per-process signing keys/public identities and use
`begin_peer_fence` / `advance_peer_authority` to retire old sessions and join
their actual work before opening the next authority. See
[peer-authority.md](peer-authority.md). Application request admission is
independent; use request draining when the replacement also needs to stop
application writes. An application-only drain keeps peer replication alive.

For a cohort replacement that must exclude an unreachable old instance:

1. Close external serving admission and freeze the transition's source set.
2. Complete shutdown of surviving old SDK instances, handling errors and
   retries. A missing instance cannot provide a successful handoff receipt.
3. Establish authenticated authority for the new cohort that excludes old
   instances. Provision a fresh signing seed and higher generation for every
   replacement, and update each receiver's exact authorized public identities
   through its completed hot fence (or configure them at startup after full
   shutdown). A retired instance cannot sign as a replacement using its old
   key or the replacement's public verification key. Process sessions sign
   every frame with fresh-session and sequence binding, including on plain
   TCP; use a protected `Network` when payload confidentiality is required. Externally coordinated
   legacy protected transport/cluster-secret replacement remains possible.
4. Recover from qualified durable sources, validate required data and writer
   authority, and then reopen external admission. Local `Serving` does not
   prove either latest-writer authority or a stable remote quorum.

A claimed node ID, recovery epoch or larger integer is not authentication.
Historical rows legitimately retain retired writer IDs in their LWW stamps.
Exclude stale senders through current authenticated authority without
discarding those historical versions or their tombstones.

## Replacing or shrinking the replica set

A majority acknowledgement is evaluated against the captured replica set for
that write. Independent old and new majorities can be disjoint. Merely
changing placement and requesting `Majority` in each set is not a joint
membership transition, and a large count reduction can remove every holder
of a previously acknowledged version.

Keep old data holders until the application has established the required
version floor in the new set. For planned retirement, use explicit
replication drain evidence for the required fragment/peer targets and inspect
its durability level. Local shutdown proves only the local store contract;
`MemoryApplied` is volatile, `StoreCommitted` is the backend's ordinary flush
contract, and `StableCommitted` additionally requires its stable fence.
If a receiver can accept later writes after a handoff fence, the application
must also control writer authority and admission throughout the transition.

For unavailable holders, choose qualified recovery sources using external
evidence. Existing `RecoveryOptions` requires the complete configured source
set and stable reconciliation; removing a missing source to clear a timeout
is not an authorized recovery decision. If every durable source of an update
is gone, the SDK cannot reconstruct it.

Use [stable checkpoints](stable-checkpoints.md) for bounded export/import with
hashes, exact retry/resume and preserved table/version/tombstone/expiry metadata.
The [staged membership workflow](staged-membership.md) requires actual stable
source/target floors, exact joint/final peer authority and live serving evidence
before removal. Keep an unavailable member's disposition explicitly separate;
source selection never fabricates its handoff or grants writer authority. Do not use a
live `get_all`, ordinary scan or SQL projection as a recovery checkpoint:
those APIs do not certify a complete stable record image. Preserve values as
opaque data; application journals and paired-table consistency need their own
validation above the SDK.
