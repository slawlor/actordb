# Physical/logical version migration

The 0.8.0 release uses peer protocol **5**, application protocol **3**, and
RocksDB data format **2**. Update every peer and application client together;
older binaries are rejected during negotiation. The crate version is 0.8.0.

Logical table views are namespaced within one node and reuse its peer listener.
The default node API remains the legacy unnamed keyspace; named tables use
their own deterministic key namespace. Table-aware client and peer framing is
being added incrementally in the 0.8.0 rollout. SQL must use `FROM table_name`
for a named table; `FROM kv` selects only the legacy unnamed keyspace.

Record order is `(timestamp_ms, logical_counter, node_id, is_tombstone)`.
`timestamp_ms` is physical Unix time. The counter orders same-time events without
turning event rate into future skew. TTL continues to use `expires_at_ms` against
wall time. Compare `VersionedValue::version()` or SDK `Value::version()`, followed
by writer identity; comparing physical milliseconds alone is insufficient.

`HlcClock::tick()` returns `Result<RecordVersion, ClockError>`. New records can use
`VersionedValue::new_version` and `tombstone_version`; existing constructors
accept physical timestamps and set counter zero. Custom producers must ensure
unique versions per writer. Include `logical_counter` in full record literals.

RocksDB opens valid format-1 manifests, validates their records, and synchronously
upgrades the manifest before any format-2 writes. Legacy records decode with
counter zero and coexist with new records. New records use a versioned flag byte
and an eight-byte logical counter before the writer ID. Older binaries reject
the upgraded manifest. Back up the database before upgrading; rollback requires
restoring that backup. Corrupt or unknown formats are rejected without repair.

Expiry event IDs use the v2 hash domain and include both timestamp components.
Applications must migrate persisted deduplication state and compare complete
versions before clearing newer state. Callback delivery remains best effort.
