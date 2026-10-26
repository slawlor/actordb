# Current record and deployment versions

ActorDB `0.9.0` supports fresh deployments only: exact peer protocol **6**,
application protocol **4**, RocksDB record/manifest format **3**, checkpoint
**ACTCP003** and membership journal **ACTMEM04**. Deploy matching participants
and use fresh data directories. There is no old-format reader, manifest adoption,
wire fallback or automatic record conversion.

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
