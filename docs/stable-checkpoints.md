# Stable recovery checkpoints

Fresh deployments use ACTCP003 bundles and RocksDB format3 only. Older bundle
headers and store formats are rejected; import never converts them. Historical
record stamps in a valid current-format bundle remain unchanged.

The `checkpoint` module exports bounded, immutable local store copies and
imports them into an explicitly chosen node identity without rewriting historical
LWW origins. The offered RocksDB backend implements the required capability.
Other stores return `CheckpointError::Unsupported` until they implement
`CheckpointBackend`; ActorDB never emulates streaming with an unbounded `load()`.

## Export

For a running node, finish full shutdown first and then call
`Node::export_checkpoint(directory, options)`. Export rejects a node that has not
completed receive/apply shutdown. The root store is exclusively bound while its
stable fence and native immutable snapshot are created. A positive stable fence
is mandatory even when ordinary store commits are buffered. The exporter releases
that binding after the durable immutable snapshot exists.

Node keeps only a weak checkpoint handle so `shutdown()` releases its owned
physical database and permits reopening the path while the stopped Node is still
alive. Retain the caller's original `Arc<dyn KvStore>` through post-shutdown
`Node::export_checkpoint`. Without a surviving store owner it returns explicit
`CheckpointError::BackendReleased`; reopen the store and use the offline exporter
instead. Once export starts, its blocking job owns the upgraded Arc and exclusive
binding through snapshot creation even if its caller is cancelled.

```rust
use actordb::checkpoint::CheckpointOptions;

# async fn copy(node: &actordb::node::Node) -> Result<(), Box<dyn std::error::Error>> {
node.shutdown().await?;
let manifest = node.export_checkpoint("./checkpoint-a", CheckpointOptions::default()).await?;
println!("records={}, chunks={}, root={:?}", manifest.records(), manifest.chunks(), manifest.root_hash());
# Ok(()) }
```

An offline application can use `CheckpointExporter::open(store, identity, path,
options)`, then call `next_chunk` one bounded chunk at a time or `finish` to run
the stream to completion. `CheckpointExporter::resume` reopens exact persisted
limits, evidence, cursor and frozen snapshot after a process restart. Reopening
`open` with changed limits/source/evidence is rejected. Later source writes do not
change an already frozen export. Snapshot creation uses a separate staging
directory and atomic publication; incomplete staging files are never accepted as
a usable snapshot. Abandoned staging directories may be retained after a crash.
Once the immutable snapshot exists, resumption needs no new live-source binding.
Torn uncommitted hash-index suffixes are regenerated only after verifying the
committed index/chunk prefix against the durable cursor.

The default chunk limits are 256 records and one MiB of encoded bytes. Callers
can choose up to 4,096 records and 32 MiB. An individual record that cannot fit
the selected limit produces `BudgetExceeded`, never an oversized page. Start a
new export path with a larger permitted limit if needed. The record's own wire
budget still applies independently.

## Import and restart

`CheckpointImporter::open(store, target_identity, bundle_path)` first validates
the manifest-committed `chunk-hashes` index with bounded memory. Its temporary
immutable copy retains one SHA-256 digest per chunk, so each received data chunk
is authenticated before its individual apply. Transfer `manifest` and
`chunk-hashes` first, then deliver `chunk-*` data files incrementally. A missing
later chunk leaves the exact committed import cursor available for retry; a
corrupt chunk contributes no writes. The export's native snapshot and export-state
files are not needed for import.

`next_chunk` merges one bounded page into the target and synchronously commits
its exact progress in the same RocksDB write batch. Drop/cancellation releases
in-process import ownership while retaining the durable incomplete marker.
Backend apply accepts only an opaque `VerifiedCheckpointChunk` produced after
digest authentication; a direct capability caller cannot manufacture it from
arbitrary records. Ordinary store loads, writes, prefix deletion and fences are
also excluded during active import, including through a shared store handle.
Ordinary store binding and Node startup reject that marker, including after a
database/process restart. Reopen the importer with the same manifest and target
identity to resume the exact committed chunk. A different import or identity is
rejected while one remains incomplete.

`finish` verifies the final record count and hash commitment, establishes stable
target data/provenance, and atomically removes the incomplete marker. It returns
an `ImportReceipt`. Retrying a completed import returns the same evidence.
Importing multiple qualified bundles into one target preserves their LWW union.
Every full `(physical timestamp, logical counter, writer ID, tombstone bit)`
tuple must retain the same payload and expiry. Equal-version divergent contents
fail the transaction and leave the import unavailable; they are not resolved by
load order. A conflicting import is not permission to overwrite data with a
reused stamp. Preserve that target for diagnosis and use an explicitly repaired
or fresh target for recovery.

Import can restore historical records into a **new** node identity on a fresh
store. It cannot rebind an existing different node manifest or change the
fragment count/partitioner. Retained named-table physical prefixes, binary values,
timestamps, logical counters, origins, tombstones and TTL deadlines are copied
without application decoding. The existing reserved `Some(0)` expiry is rejected.
Checkpoint record validation uses the same logical wire-key allowance as actor
and RocksDB admission. Physical durable namespace bytes remain preserved in the
bundle, but are excluded from that logical record limit only after the physical
and logical table identities and encoded UTF-8 key have been validated together.
Actor startup still observes the imported clock floor and applies normal expiry
normalization before serving. Old record origins do not become trusted peers.

Checkpoint methods are synchronous backend/filesystem operations; use a blocking
executor from async applications. `Node::export_checkpoint` already does so and
retains blocking work ownership if its caller is cancelled.

## Evidence boundaries

Every completed manifest certifies a stable **local** copy. Optional
`remote_replication()` evidence is attached only when that same Node actually
completed `shutdown_with_replication_drain`; its exact target set and ACK
durability remain separate historical evidence. It does not assert that later
offline edits or all subsequently copied records were acknowledged, prove
cluster convergence, authorize a writer, or invent a handoff for a failed member.

`CheckpointReplicaCertificate::collect` provides separate **actual stable target
floor evidence**: pass the complete expected target identity set and live
`CheckpointReplicaObservation`s with their returned import receipts. The
collector verifies manifest/root/count identity and rereads each target backend's
retained import evidence before returning. Missing, changed, or physically
retired target floors fail certification. Its scope is the imported checkpoint
floor, not exact latest payload equality or writer authority.

The SHA-256 chain detects corruption and changed chunks. It is not a signature:
the application must authenticate the manifest/source through its control plane.
Storage operators capable of changing native metadata or externally certified
decisions remain responsible for their authority. Prefix retirement atomically
advances a provenance generation, invalidating prior full-floor import evidence
without scanning or collecting all old receipts.

`CheckpointReplicaCertificate::collect` validates actual target backends and
their stable `ImportReceipt`s against the exact checkpoint digest, source,
target identities, record count and root, then rechecks retained evidence. Every
requested target must be present; a missing or retired floor fails collection.
This certifies explicit stable checkpoint copies, independently of a manifest's
historical pre-checkpoint replication ACK report. It does not grant process
authority or claim that later application writes reached those targets.

Use [staged-membership.md](staged-membership.md) to require qualified source
floors and target evidence before replacement or shrink. Checkpoints do not
recover writes absent from every source or make permanent source loss safe by
themselves.
