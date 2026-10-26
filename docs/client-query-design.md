# Rust client and query companion design

This describes the current ActorDB `0.9.0` architecture. See
[SQL queries](query.md) and [the Rust SDK](../actordb-client/README.md) for usage.

## Crate and endpoint boundaries

The embedded `Node` API and the application listener are separate from the
peer mesh. Peers replicate stamped operations; application clients submit
intent so the server owns clock, expiry, routing and admission decisions.
Application connections never join peer membership.

- `actordb-protocol` contains public `meta` / `client` schemas, full record
  versions, typed write policies and evidence.
- `actordb-query` contains portable SQL parsing/planning and exact reducer
  contracts, without actors or native storage.
- `actordb-client` contains the standalone Rust application SDK, without a
  normal database/native/backend dependency.
- `actordb` owns application serving, distributed query execution and peer
  replication.

Use `actordb_protocol` directly for schema consumers. There is no public
database-crate wire alias. Exact peer protocol **6** and application protocol
**4** include all current operations without capability negotiation.

## Current operation coverage

| Operation | Contract |
| --- | --- |
| Point put/delete/read | Server-stamped LWW versions, TTL/priority, typed errors and explicit uncertain mutation outcomes |
| Ordered batches | Input positions and duplicates preserved; independent per-key outcomes, not transactions |
| Write concerns | Count/majority/region constraints, strict caps, actual durability and operation-floor evidence |
| Logical tables | Named namespaces share one authenticated application connection and Node |
| Scans | Bounded filter/transform work with explicitly per-fragment limit/offset semantics |
| SQL | Named relations, typed parameters, fragment filters, bounded pages, projections and exact aggregates |
| Inspection | Read-only local health and placement, not authority or a remote durability proof |
| Lifecycle/admin | Embedded Node APIs; no remote mutation of hosting/placement/shutdown authority |

The peer protocol has one streamed anti-entropy engine using
`ReconcileRequest`, `ReconcileResponse`, `ReconcileFetch` and `ReconcileData`.
It does not retain an alternative full-sync engine or obsolete sync messages.

## Query execution

Server execution is bounded by request admission, fragment fan-out, actual
actor/pool ownership, response bytes and a total deadline. Aggregate reducers
consume the complete selected input within their budgets, not a capped scan
sample. Queries and scans retain ownership through actor completion and queued
response delivery after waiter cancellation.

Table views isolate records, SQL relation selection and durable partitions.
Equal user keys co-partition across tables; this is not a transaction or an
implemented join planner. Pagination cursors bind table/query/fragment scope and
observed revisions, so a changed source is not silently treated as the same
snapshot. A distributed query is not a globally simultaneous read transaction.

## Application connection behavior

SDK clones share one serialized connection and bounded count/encoded-byte
admission. The request deadline includes connection-lock waiting. Use additional
connections for parallelism; multiplexing, topology discovery, automatic
mutation replay and remote operator control are not provided.

The SDK does not automatically retry a mutation after uncertain dispatch.
Connection loss or timeout after possible application returns
`UnknownOutcome`. A cancelled in-flight exchange closes its connection; later
requests observe `Closed`. Closing the SDK cancels queued/active exchanges but
does not shut down the database.

## Transport and authentication

All frames use four-byte big-endian lengths and current protobuf schemas.
Preflight bounds repeated fields before decoding; exact encoded byte limits
apply to requests and responses. A non-current protocol is rejected.

Host TLS policy belongs to the supplied transport. Optional shared-secret
challenge/response establishes fresh possession but not an individual holder.
There are no process signing keys, signed-frame headers or capability/version
range negotiation. Generation allowlists are operational admission/drain
controls; external credential revocation establishes exclusion.

## Verification

Portable protocol/query tests validate schemas, preflight, exact ordering,
metadata, aggregates and malformed responses. SDK integration uses the real
application listener, including shared tables, TLS/custom transports,
cancellation, routing, write evidence and shutdown ownership. Docker exercises
a separate SDK-only process across sharded nodes and ingress restart.
