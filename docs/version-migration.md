# Current record and deployment versions

ActorDB `0.11.0` replaces the application TCP protocol with typed unary RPC.
The default service is `actordb.rpc.v1.ActorDb/Call`; SDK and server both enable
`grpc` by default, and vendors may disable it and supply another RPC framework.

The former application handshake, identity/credential fields, frame wrapper and
ActorDB request IDs are removed. There is no compatibility listener or fallback.
Authentication/TLS/ACLs belong to the RPC adapter, not the Node service or SDK core.
Migrate clients and servers together; see [RPC configuration](transports.md).

Peer protocol **6**, RocksDB record/manifest format **3**, checkpoint
**ACTCP003**, and membership journal **ACTMEM04** are unchanged by this API work.
Current-format data needs no conversion for this RPC migration; unsupported
formats still require fresh directories. Per-response decode allowance is
64 MiB and aggregate reservation allowance 256 MiB by default; wide responses
require explicit compatible per-call/aggregate settings.

Non-current data and wire versions fail closed. Existing source data must be
handled through a separately planned application export/re-ingestion workflow;
do not remove validation metadata or overwrite a source deployment.

## Record ordering

Records are ordered by `(timestamp_ms, logical_counter, node_id, is_tombstone)`.
Physical time is Unix milliseconds; the logical counter orders events at the
same physical time without turning write rate into future clock skew. TTL uses
`expires_at_ms` against wall time.

Compare complete `RecordVersion` values followed by writer identity and the
tombstone tie-breaker. Physical milliseconds alone do not establish order.
`HlcClock::tick()` is fallible. Construct versioned values from explicit complete
versions; producers are responsible for uniqueness per writer.

## Tables and recovery

Each operation names a logical table. Tables share a Node listener and backend,
but their cache, flush and iteration namespaces are isolated. Routing hashes the
unqualified key so equal keys co-partition across tables.

Current checkpoint import preserves full versions, writer provenance, table
namespaces, expiry and tombstones. Stable target floors and readiness evidence
are distinct from writer authority. See [fresh deployment](support-api-migration.md)
and [stable checkpoints](stable-checkpoints.md).
