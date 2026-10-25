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
full node shutdown is the supported receive/apply completion barrier; use the
draining API to close application admission and wait for accepted work first.
An application-only drain keeps peer replication alive and is not a hot
membership fence. Resume and hot epoch advancement are tracked in
[#133](https://github.com/slawlor/actordb/issues/133).

For a cohort replacement that must exclude an unreachable old instance:

1. Close external serving admission and freeze the transition's source set.
2. Complete shutdown of surviving old SDK instances, handling errors and
   retries. A missing instance cannot provide a successful handoff receipt.
3. Establish authenticated authority for the new cohort that excludes old
   instances. A new protected transport credential or fresh cluster secret
   can exclude holders of old credentials, provided the retired instance
   cannot obtain the new credential. The shared secret handshake alone does
   not encrypt or integrity-protect subsequent frames; use a protected,
   authenticated `Network` when that is required.
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

Portable stable export/import with hashes, chunk retry/resume and preserved
table/version/tombstone/expiry metadata remains tracked in
[#134](https://github.com/slawlor/actordb/issues/134). Until supported, arrange
the required backend checkpoint before removal, under the backend's stable
snapshot contract and after completed shutdown when necessary. Do not use a
live `get_all`, ordinary scan or SQL projection as a recovery checkpoint:
those APIs do not certify a complete stable record image. Preserve values as
opaque data; application journals and paired-table consistency need their own
validation above the SDK.
