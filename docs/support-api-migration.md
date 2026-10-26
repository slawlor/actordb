# ActorDB 0.10.0 deployment

ActorDB `0.10.0` preserves the wire and data formats of the published `0.9.0`.
Current-format deployments require no data conversion. The SDK's default 64 MiB
decoded-response allowance is a breaking admission-policy change; configure
`ClientOptions::max_decoded_response_bytes` explicitly for larger supported
responses. The 16 MiB encoded-frame bound is unchanged.

Non-current formats still require a new deployment with fresh stores, current
configuration and matching database/protocol/query/SDK crates. There are no
compatibility aliases, old-data adoption switches, wire-version fallbacks or
automatic conversions.

## Deployment contract

- Every peer uses exactly peer protocol **6**.
- Every application client/listener uses exactly application protocol **4**.
- RocksDB uses record/manifest format **3** only.
- Checkpoint bundles use **ACTCP003** and membership journals **ACTMEM04** only.
- Non-current stores, journals, bundles and wire versions are rejected.
  Rejection is not permission to remove markers or rewrite old data.
- The envelope/record maximum remains **16 MiB**. There is no signed-frame
  header or Ed25519 dependency.

For non-current formats, use new data directories. If business data must be retained, export it
through the source application's supported tools and ingest it as new
application writes; preserving it is a separate operator-owned project, not an
in-place ActorDB upgrade. Nothing in this release deletes an existing deployment.

## Transport and authentication

Provision TLS certificates and verification policy outside ActorDB. The
`Network` / `NetStream` interfaces and optional rustls transport accept that
policy; the SDK can use `Client::from_stream`, and the application listener can
use `ClientServer::bind_with_network` or `from_acceptor`.

An optional high-entropy shared secret provides challenge-bound authentication.
It does not identify individual secret holders or encrypt subsequent traffic.
Use mutually authenticated TLS for peer identity, integrity and confidentiality.
There is no capability or version-range negotiation: both sides must use the
current protocol.

Generation allowlists and `begin_peer_fence` / `advance_peer_authority` close
old sessions and wait for accepted local work and persistence. They do not prove
that an old process lost its TLS/shared-secret credentials. A holder of valid
credentials can claim an allowed identity/generation; external credential
revocation and writer authority remain deployment responsibilities.

## Canonical APIs

Use `actordb::actor` for actors and maintenance types, and
`actordb_protocol::meta` for wire types. No public `actordb::proto` or
`actordb::supervision` alias is provided. Standalone replication uses the bounded
sender API; unbounded compatibility constructors are removed.

No-op tombstone collection methods, unused full-sync messages and always-zero
health/sync counters are removed. Tombstones remain retained until a supported
durable whole-namespace retirement establishes its separate boundary.

`NodeStatus.lifecycle` is the typed `NodeLifecycle`; `NodeStatus.drain`
contains request ownership and cutoff status. `shutdown_stage` is a concrete
stage. Unimplemented lag/percentage fields are absent rather than permanent
`None` placeholders. Actual queue age, fetch counts, ACK/reconciliation times,
staleness and omitted observations remain available.

## Drain and persistence boundaries

`begin_drain()` closes application admission synchronously. Accepted work stays
owned through execution and queued response delivery even when its waiter
cancels. `drain_requests(DrainOptions)` defaults to configured local persistence;
select `Applied` for execution/response completion or `StableCommitted` for an
explicit stable backend fence. Retry a timed-out/cancelled drain with the same
durability selection.

`finish_shutdown` and `shutdown` join receive/apply ownership before releasing
the Node's store attachment. A failed backend remains owned until a successful
retry. Local shutdown is not a remote durability certificate.

Post-shutdown checkpoint export requires a caller-retained backend `Arc`.
The Node retains only a weak checkpoint handle; otherwise export reports
`BackendReleased`. An accepted export owns its backend and snapshot work through
actual completion even if its waiter leaves.

## Recovery and write evidence

Checkpoint chunks are authenticated before apply; records and import cursors
commit atomically. An unfinished import blocks ordinary store use until the
matching import completes. Staged membership requires qualified source floors,
stable target copies, actual readiness and the appropriate receiver policy/drain
evidence. No allocator, consensus or writer lease is introduced.

Strict write policies reject acknowledgement-cap downgrades before apply.
Minimum durability and evidence requirements report actual memory/store/stable
boundaries, frozen logical replica identities and the submitted LWW version
floor. `DurabilityFailed` and `UnknownOutcome` retain partial or uncertain
progress; neither means an operation was safely unexecuted.

See [checkpoint recovery](stable-checkpoints.md), [staged membership](staged-membership.md),
[write evidence](write-concerns.md) and [complete observations](observability.md).
