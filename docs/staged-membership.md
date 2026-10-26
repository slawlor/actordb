# Supported staged replacement and shrink

`MembershipTransition` provides durable proposal/progress validation around a
caller-owned `ManualPlacement` controller. It supports fixed-fragment-count
replacement and replica-count reduction while preserving the union of qualified
old local floors. It allocates no machines, selects no failed member to discard,
and implements no consensus. The application owns serialized control-plane
decisions and authentication of supplied evidence.

Authentication belongs to TLS and the optional shared PSK. Receiver generation
gates are cooperative: an old process retaining valid transport credentials
can claim an allowed generation. Changing a counter does not revoke those
credentials; retire/rotate them externally. Checkpoint and receiver-fence
evidence certifies observed local work and stable floors, not cryptographic
identity or consensus.

Only ACTMEM04 journals are read or written. Older journal headers are rejected
without conversion or modification. The format stores process IDs/generations,
receiver boot/revision observations and explicit application decisions, with
no signing keys or credential payloads.

## Workflow

Run the native SDK workflow with a fresh directory:

```sh
cargo run -p actordb --features rocksdb --example staged_membership -- /tmp/actordb-replacement replace
cargo run -p actordb --features rocksdb --example staged_membership -- /tmp/actordb-shrink shrink
```

The example uses real hot peer fences, checkpoint import receipts and separate
full shutdown reports for retired SDK nodes. Its application chooses the epoch
and distributed decision IDs; those are not inferred from local revisions.

1. Capture the complete old and desired maps, a fresh nonzero transition epoch,
   and an explicit minimum replica count. `MembershipTransition::create` validates
   full coverage, IDs, the unchanged fragment layout and bounded membership.
   Changes that violate the minimum are rejected before controller publication.
2. Fence application writers and obtain stable checkpoints from every old holder
   of each changed fragment, including holders retained in the final map. Record
   each immutable `CheckpointManifest` with `record_source`. The joint map keeps
   the old holder set and adds the new targets.
3. Complete the operational receiver-generation transition at every
   affected node. Record the externally certified `MembershipAuthority` with
   `record_authority_fence`: it binds this epoch, the exact joint node IDs, their
   positive process generations and a nonzero durable external decision ID. A process-local revision is not distributed evidence.
4. `publish_joint(&controller)` persists the overlap intent before publishing it.
   A controller changed outside the captured old/joint map is rejected. An exact
   retry can repair publication after controller restart.
5. Import **every** qualified old source into **every** final target, including
   importing a retained holder's own checkpoint to register its stable floor.
   `record_import` accepts only receipts matching the captured source digest,
   physical layout and intended target. Interruptions resume through the durable
   backend import marker; incomplete targets cannot start serving.
6. Start/reconcile the joint set using its appropriate explicit recovery authority
   and current receiver generations. `Node::certify_membership_ready` checks actual
   joint serving routes, placement generation, and currently retained stable
   import evidence. Record every final target's returned live evidence. Checkpoint
   qualification alone does not certify readiness or elect a writer.
7. Drain retiring processes and complete the **final** authority fences at every
   affected node. `record_final_authority_fence` must contain exactly the desired
   process IDs/generations, excluding retirees. A retained process can retain
   or advance its generation; rollback is rejected. This operational gate does
   not authenticate a generation claim or revoke transport credentials.
   Authority changes invalidate cached readiness; reacquire every target's
   `Node::certify_membership_ready` evidence under the final policy before retiring.
8. `publish_final(&controller)` requires both authority decisions, all required
   source/target evidence and live readiness. It persists final intent before
   publishing the desired map. Normal placement handoff then stops retired
   fragments. Resume application writes under the final authenticated policy.

Applications should use the native receiver-fence report to certify
the corresponding external decision after all affected nodes have completed
their fences. A caller's `MembershipAuthority::certify_external` call explicitly
asserts that this external decision/evidence exists; ActorDB does not turn that
declaration into consensus, verify a control-plane signature, or automatically
distribute process credentials. Historical imported writer IDs are independent
of current receiver-generation policies.

`MembershipAuthority::certify_native_joint` requires a `MembershipFenceObservation`
for every exact joint receiver. Each genuine `PeerFenceReport::public_authority()`
must equal that live Node's current `Ready` snapshot, including receiver boot,
local generation, peer generation registry and process-local revision. Receivers
must agree on the whole receiver generation registry. The final collector also requires a
`MembershipShutdownObservation` for every retiring SDK process; full receive/apply
shutdown and cutoff boot are verified separately from surviving receivers' peer
revocation. The transition checks retired boots against its captured joint cohort.

`Node::certify_membership_ready` independently matches configured native policy
against the selected joint/final certificate, verifies every assigned joint
fragment is `Serving`, and rechecks placement generation, public authority and
retained import receipts. A fencing phase, changed generation, old report/boot, or
incomplete assigned gain fails certification even when ordinary local readiness
remains usable for established fragments. Native boot/revision evidence survives
coordinator restart; fresh process authority needs a new transition decision.
Receivers without optional generation gates can use only the explicitly external route; they
cannot contribute a native SDK fence certificate.

Membership eligibility checks complete placement and lifecycle observations,
not optional metric-table or sender-report coverage. Registering more metric
scopes or truncating diagnostic sender output cannot block a fully validated
cutover. External-only readiness is rejected while native authority is being
enabled: an absent old-era public snapshot does not mean the pending fence is
inactive. Peer phase, placement generation and application admission are checked
again after retained floor verification.

## Restarts and bounds

The transition file uses a canonical checksummed encoding with atomic write,
file sync and directory sync. Reopen the existing proposal with
`MembershipTransition::reopen`; do not construct a fresh proposal over its path.
Source digests and import receipts survive controller/caller restart, and changes
to a source checkpoint within one transition are rejected. Live serving readiness
is deliberately reacquired after restart. A completed final intent remains
idempotently publishable and cannot accept changed evidence.
Ready evidence binds the exact transition epoch and authority digest, as well as
node incarnation and placement generation; a token from another epoch or a
previous joint/final process policy cannot satisfy the new decision.
Each joint/final authority decision is immutable once recorded; identical retries
are allowed, but further process-authority changes require a fresh transition
epoch.

The supported envelope is at most 4,096 fragments, 64 joint holders per fragment,
256 distinct node IDs, and 32 MiB of proposal/evidence metadata. Adding evidence
checks that bound before extending its owned state. Record transfer is separately
bounded by [stable-checkpoints.md](stable-checkpoints.md).

Readiness is a live observation, not a lease against unrelated controller
changes. Keep this captured proposal fixed and serialize external decisions
through publication. An importer receipt is rechecked against the target backend:
physical namespace retirement invalidates old floor evidence. An external actor
that can overwrite backend data or forge operator authorization is outside the
normal library ownership contract.

## Permanently unavailable sources

Missing old holders keep ordinary retirement blocked. Never shorten a source
list to make recovery converge. Where the application has separately decided to
accept permanent source loss, call `PermanentSourceLoss::authorize` with the exact
missing retiring IDs, the proposal epoch and its nonzero external decision ID,
then `authorize_permanent_loss` while the proposal is prepared.

This explicit decision records an unavailable floor; it is **not** a checkpoint
or successful handoff receipt. A completed operational joint fence is required
before that decision can remove failed processes from the joint set. All surviving
qualified floors, stable target imports, live readiness and final receiver cutoff are
still required before publishing final membership. The missing member contributes
no fabricated receipt, and its latest writes may remain unrecoverable. Source-set
qualification by itself never authorizes discard or automatic writer election.

The existing [full-outage recovery procedure](dynamic-recovery.md) remains the
conservative complete-source route. Staged transition validation supplies the
explicit checkpoint/import and external-decision workflow needed when identities
or holder counts must change.
