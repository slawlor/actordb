# Reconciliation summaries and snapshot revisions

Reconciliation reads ordered metadata pages, not full cache snapshots. Each
fragment maintains an ordered index of physical keys and cached live/expired
content digests, versions, writer identities, tombstone flags and TTL deadlines.
Table children share that index and one mutation revision. Physical-key order
is used directly: table-name order differs when one hexadecimal namespace
prefix is shorter than another.

A summary request seeks its exclusive key cursor in O(log N), then visits only
its count/byte-bounded metadata prefix, plus an end-of-page witness. It does not
clone payloads, sort all keys, rehash values, walk the table registry, or await
child cache/backend locks. A revision mismatch returns an empty restart page
before allocating the result vector. The existing soft metadata byte target
allows one individually valid large metadata row; existing wire limits remain
in force. Current revision reads likewise do not visit tables.

The resident index retains one key, writer identity, version/expiry fields and
cached digest pair per cached winner. This is dataset-sized state, not an idle
table-worker cache and not a total-cache/RSS limit. It replaces repeated
dataset-sized temporary allocations with bounded page allocations; it does not
claim that the resident dataset itself is bounded.

## Publication and TTL

Winning apply and expiry changes publish their cache record, metadata and
revision inside the same cache mutation critical section, before releasing the
cache lock and before any auto-flush/backend await. An equal already-present
record or superseded write does not advance the revision. A subsequent storage
error or cancelled flush waiter cannot undo or delay the revision of an already
visible cache mutation. Revision numbers are opaque, process-local observation
tokens, not durable transaction IDs or a distributed commit sequence.

Summary and exact fetch TTL selection use the same caller-pinned time against
the retained record. Crossing its wall-clock deadline without cleanup does not
silently change a continuation's live digest. Cleaner materialization advances
the revision and retains the deterministic expiry tombstone; after the original
payload is discarded, both summary and exact fetch return that same tombstone,
including for a formerly-live pinned time. Pinned TTL visibility is not MVCC
history for overwritten or materialized records.

## Explicit full snapshots

`FragmentState::snapshot_with_revision` still returns the explicitly full
diagnostic dataset. In a partitioned live fragment, a shared snapshot fence
excludes winning cache publication across every table while that dataset and
its revision are captured, using one TTL time. The operation may allocate the
entire dataset and delay writes; do not substitute it for bounded summary
pagination. The fence is not retained through storage I/O.

A standalone unpartitioned public state preserves its direct base-cache view:
preloaded encoded child records and managed actor child mutations do not enter
that base snapshot or change its local revision. Base snapshot values and the
local revision are read under the same cache guard. Per-table query revisions
and incarnations remain local to their own data state.

No wire/storage encoding or public method signature changes. These are local
snapshot guarantees; reconciliation still does not establish an atomic
cross-node snapshot or strong consistency.

## Regression evidence

`summary_revision_tests` covers physical prefix ordering, startup-loaded
children, empty/plain/Unicode keys, cross-table mutations, pinned TTL before
and after cleanup, deterministic paused snapshot cloning, two-table snapshot
fencing, concurrent exact mutation-count pairing, and failed/cancelled
auto-flush publication. Its allocation probe requests one summary from both a
512-record ×128 KiB-value fragment (both KV-only and distinct-table variants)
and a 4,096-table narrow-value fragment while
a child cache is blocked. The bounded metadata call allocates less than 32 KiB;
revision mismatch/current-revision calls allocate no data. The probe counts
requested Rust allocator bytes, not RSS, and has no shared-runner latency/SLO
assertion.

On the 2026-10-26 release-mode probe, the KV-only 512×128 KiB fixture requested
153 allocation bytes for one metadata row; the earlier review probe on
`1be81db8badc72dadb9d94d51d497ff4bed8760a` requested 67,412,769 bytes. The
distinct-table variants requested 145 bytes. These are requested allocation
sizes for that call, not a statistical latency comparison or an RSS estimate.
