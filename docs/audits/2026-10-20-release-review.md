# Release review — 2026-10-20

Reviewed commit: `9dd78dce56d600b994700af0075f845919b3d163` on `main`.
[PR #111](https://github.com/slawlor/actordb/pull/111) was squash-merged after
all ten checks passed. Its remote and local feature branches were deleted.

## Recommendation

Address R1–R4 before a release intended for sustained production load. R1 can
prevent replication and restart; R2 and R3 can exhaust memory during an outage;
R4 can stall async execution. R5–R7 are smaller, targeted fixes worth including.
R8 and R9 need explicit deployment limits or further design work.

These are findings and proposed work, not implemented fixes. Several limits
are already acknowledged in the documentation and the
[hardening backlog #49](https://github.com/slawlor/actordb/issues/49).
This review supplies concrete consequences and reproductions for prioritizing them.

| ID | Priority | Category | Finding | Evidence |
| --- | --- | --- | --- | --- |
| R1 | P1 | Correctness / scalability | Logical clock drift causes valid versions to fail skew admission | Reproduced |
| R2 | P1 | Bug / scalability | Failed flushes retain every overwritten payload version | Reproduced |
| R3 | P1 | Scalability | Replication backlogs have no retained-byte limit | Traced source and calculated capacity |
| R4 | P1 | Correctness / scalability | Synchronous storage blocks async execution; local actor waits lack deadlines | Traced execution paths |
| R5 | P2 | Scalability | Exact-key and range SQL queries scan unrelated keys | Reproduced |
| R6 | P2 | Correctness | TTL normalization uses time captured before mutation locks | Reproduced |
| R7 | P2 | Bug / lifecycle | Client close can wait indefinitely on a custom stream | Reproduced with paused Tokio time |
| R8 | P2 | Scalability | Legacy scans allocate full fragment replies before aggregate/wire bounds | Traced execution paths |
| R9 | P2 | Recovery | Dynamic placement needs a serving source after a full cluster restart | Traced lifecycle; documented genesis limitation |

P1 means high-impact work to prioritize before production use. P2 means a
conditional failure or a significant limitation to fix or explicitly support.

## Findings

### R1 — Separate physical time from logical clock increments

Sources: [clock.rs:94](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/clock.rs#L94),
[clock.rs:102](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/clock.rs#L102),
[clock.rs:113](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/clock.rs#L113),
[NodeConfig::max_clock_skew](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L3454).

Every local tick advances `timestamp_ms` by at least one millisecond. Remote
observations also increment it. Sustained event rates above 1,000/s therefore
create future drift even when physical clocks agree. The producer does not
apply the configured skew check to its own ticks; peers and durable startup do.

The probe generated one million ticks in approximately 50 ms. The resulting
version was about 999,947 ms ahead of wall time. A clock using the default
300,000 ms limit rejected both `observe(version)` and `seed_persisted(version)`.
Replication can disconnect repeatedly, and restart can fail until wall time
catches up. The existing consistency guide mentions scalar-clock drift, but
its interaction with skew rejection makes this a release risk.

Proposed fix: use an explicitly ordered physical/logical version, with skew
validation applied to the physical component. Plan wire and persisted-record
migration together. Include the callback event identity and version comparison
APIs in that migration. Raising the skew limit only postpones the failure.

Verification: sustained writes and observations well above 1,000/s, bounded
physical skew, monotonic versions, replication, and immediate durable restart.

### R2 — Bound and coalesce the authoritative persistence backlog

Sources: [apply:246](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L246),
[flush:303](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L303),
[begin_batch:1036](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L1036).

Failed flushes retain their authoritative batch, then append subsequent pending
writes before retrying. That retained batch is a vector of all versions; it is
coalesced only in the temporary store-write representation. Each retry also
clones the complete retained vector.

With an always-failing store and a flush threshold of one, 100 failed writes
to the same key left 100 complete payload versions retained while the cache
contained one key. Further writes continue growing that vector. Even a fixed
working set can exhaust memory during a prolonged outage, with increasing
copy/merge work on each retry. Storage errors also restart managed workers,
adding churn without relieving the backlog.

Proposed fix: coalesce retained winners under the existing ordered flush
protocol, and impose count/byte admission before another mutation exceeds the
persistence budget. Preserve any batch still owned by an active store call and
the exact retry semantics. Make retry pacing account for a failed retained
batch; emit backlog and storage-failure diagnostics.

Verification: long failing-store runs with one hot key and many distinct keys,
bounded retained bytes, pre-apply rejection at capacity, recovery to the latest
winners, and cancellation/final-flush regressions.

### R3 — Add byte budgets to replication ingress, buffers, and in-flight work

Sources: [SenderConfig:282](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/replication.rs#L282),
[send_data:568](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/replication.rs#L568),
[sender_task:1432](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/replication.rs#L1432),
[fanout:1386](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L1386).

Each sender retains owning `ReplicatedOp` payloads in its count-bounded ingress,
deque, and in-flight batches. Fanout clones the value for each replica. The
32 MiB connection budget applies later, to encoded frames, and cannot bound
these earlier copies.

A single default 10,000-entry stage could retain roughly 156 GiB of values
near the 16 MiB record limit, before ingress/deque overlap, metadata, other
senders, cache, and persistence copies. Even 1 MiB values imply approximately
9.8 GiB in that stage. Reset occurs on count saturation, potentially long after
the process has exhausted memory. The public standalone sender also keeps its
legacy unbounded input channel.

Proposed fix: retain byte permits throughout ingress, deque, and in-flight
ownership, preferably with a node-wide allowance as well as sender limits.
Byte saturation should trigger the existing visible reset/reconciliation path
without blocking cleanup or retroactively failing an applied write. Provide
a bounded replacement for standalone sender ingress.

Verification: partition/non-reading-peer tests with wide values, replica
fanout, byte saturation before count saturation, reconnect, and permit release
after ACK, reset, cancellation, and shutdown.

### R4 — Define storage execution and local-wait liveness policies

Sources: [flush:313](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L313),
[stable_fence:331](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L331),
[RocksDB write:616](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/store/rocksdb.rs#L616),
[local reconcile read:3253](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/replication.rs#L3253),
[local get:4491](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L4491).

`KvStore` operations execute synchronously inside async startup, worker,
maintenance, and placement paths. RocksDB writes and WAL fences can block on
storage. On a current-thread runtime, one such call blocks every async task;
enough concurrent fragment calls can occupy all workers of a multithreaded
runtime. Cooperative deadlines cannot interrupt those calls.

Separately, local reconciliation summary/value/apply/flush actor RPCs use
`None` deadlines. Some are awaited inside a selected sender branch, so the
sender cannot process its timers or control messages while awaiting them.
The advertised reconciliation timeout covers network phases, not these local
stalls. Embedded point operations also have unbounded local actor waits.

Proposed fix: use a bounded, owned storage executor, and propagate explicit
local-work deadlines through read/reconcile paths. Preserve completion ownership
for submitted writes: timing out a waiter must not detach a store operation,
release its ordering fence prematurely, or misreport a definite rejection.
Document the distinction between a bounded waiter and a storage operation that
must finish before safe teardown.

Verification: deliberately blocked storage and actor replies, current-thread
and small multithreaded runtimes, continued unrelated work, timeout outcomes,
and cancellation/shutdown ownership.

### R5 — Seek query key bounds in the ordered index

Sources: [query_page:529](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L529),
[exact-key fragment routing:145](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node/query.rs#L145),
[merge:271](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node/query.rs#L271).

The first query page always starts at `Unbounded`. Exact-key, prefix, and key
range predicates filter after visiting records. Exact equality chooses a
fragment but still scans that fragment's preceding keys. Keyset pagination
also repeatedly visits unrelated prefixes.

The probe inserted 100 ordered keys. `get("key:099")` succeeded, but SQL
equality for that key exhausted a ten-record work budget. With the default
million-record budget, a sufficiently large fragment can reject a one-row
point query for the same reason.

Proposed fix: derive safe lower/upper index bounds from conjunctive predicates;
use an exact-key lookup with the same snapshot/version checks. Stop after the
upper bound. Also replace the coordinator's per-row linear search over all
fragment heads with a heap when fragment counts justify it, and consider
bounded concurrent head fetches instead of serial network round trips.

Verification: exact/range/prefix queries over large nonmatching prefixes,
contradictory bounds, empty/unicode/binary prefixes, continuations, and work
accounting proportional to the selected range.

### R6 — Evaluate apply-time TTL after acquiring mutation synchronization

Source: [apply:217](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L217).

Apply decides whether an incoming record is expired before awaiting the flush
and cache locks. A record can cross its deadline during that wait, then be
installed as a live cache entry with `ApplyOutcome::Applied`. Reads immediately
hide it; cleanup later performs the tombstone transition and may notify it.
The choice between immediate expiry normalization and later cleanup therefore
depends on stale pre-lock time.

The probe held the cache lock, polled apply while its TTL was live, let the
deadline pass, and released the lock. Apply returned `Applied`, installed a
non-tombstone, and an immediate get returned `None`.

Proposed fix: normalize the incoming value against wall time under the existing
mutation synchronization immediately before merge. Preserve the rule that
an already-expired incoming write is not itself a TTL-cleanup callback. This
finding does not assert linearizable reads or a callback-delivery guarantee.

Verification: controlled lock contention crossing expiry in both storeless
and persisted paths, exact outcome/fanout, and callback distinction.

### R7 — Bound Client::close and specify cancellation of active requests

Source: [Client::close:343](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb-client/src/lib.rs#L343).

Close shuts request admission, then awaits the connection mutex and the
stream's `shutdown()` with no deadline. A custom buffered/TLS stream can require
peer progress to complete shutdown. An active request also holds the mutex for
its exchange; closing admission does not cancel that exchange.

A conforming injected stream whose shutdown remained pending completed the
normal handshake. With a one-second client timeout, close still had not
finished after ten seconds of paused-clock advancement. Cancellation of that
close released the stream, but callers currently need their own wrapper.

Proposed fix: provide a bounded close policy and an explicit active-request
cancellation signal. Graceful stream shutdown should fall back to dropping the
owned stream at its deadline. Active mutation cancellation must retain an
unknown-outcome classification. Add public close rustdoc.

### R8 — Route compatibility scans through byte-bounded pages

Sources: [fragment scan:391](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/actor/fragment_state.rs#L391),
[Node::scan:223](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node/scan.rs#L223),
[peer scan responder:417](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node/scan.rs#L417).

Legacy fragment scans hold one cache read lock for the whole scan and clone
matching values under a count limit only. `Node::scan` concurrently materializes
all fragment replies with `join_all`, then truncates the merged count. Thus the
100,000-result aggregate cap is not a peak-memory cap. The peer responder also
constructs the full result before the encoded-frame check.

The application listener already uses `scan_bounded`, so that safer path should
be reused. Preserve legacy per-fragment limits/offsets and whole-query failure
semantics while bounding fragment concurrency, visited work, and retained bytes.
Reject oversize output before cloning an entire fragment reply.

### R9 — Provide a full-restart recovery procedure for dynamic placement

Sources: [startup serving set](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L3744),
[gain convergence:6117](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L6117),
[serving-only reconcile requests](https://github.com/slawlor/actordb/blob/9dd78dce56d600b994700af0075f845919b3d163/actordb/src/node.rs#L8339),
[operations guide](../operations.md).

Every dynamically placed startup subtree starts non-authoritative, including
durable preloads. A multi-replica gain requires a completed round from an
already-serving replica, while non-serving replicas refuse authoritative
summary/fetch requests. After all replicas of a fragment restart, no source can
certify the others under the unchanged placement. Waiting or reconnecting does
not create one.

Cold genesis with one seed owner is documented; full outage recovery of an
existing multi-replica durable cluster needs a similarly explicit procedure.
This is an intentional fail-closed availability limit, not evidence of data
corruption. Single-owner fragments and rolling restarts with a surviving
serving replica are different cases.

Proposed work: define operator-authorized recovery authority and reconcile
surviving durable candidates before serving. Avoid automatically promoting an
arbitrary unverified copy. Add a full-cluster restart test and a recovery runbook;
otherwise explicitly limit dynamic placement to surviving-source recovery.

## Further scalability and cleanliness work

1. **Account for total retained work.** Query admission's 32 MiB allowance
   charges request storage; each of 32 admitted queries separately allows
   16 MiB results and 32 MiB merge buffers. Peer writer allowances likewise
   multiply by connection count. Dispatcher mailboxes remain unbounded ahead
   of worker admission. Existing docs acknowledge these limits, but operators
   need aggregate queue/result/backlog byte metrics and deployment budgets.
2. **Measure lifetime key growth.** All stored values remain in the fragment
   cache and tombstones are retained indefinitely. TTL frees the payload, not
   the key/version floor. Cleaner passes visit retained non-TTL and tombstone
   keys too. Consider a deadline index for expiry and benchmark long-lived TTL
   workloads. Working-set eviction and causally safe tombstone reclamation need
   separate designs; restoring age-based GC would break correctness.
3. **Document unique-version invariants.** A sixth probe applied different
   payloads with identical `(timestamp_ms, node_id, is_tombstone)` to two states
   in opposite order; they remained different after exchanging those writes.
   Low-level APIs admit this input, and anti-entropy summaries cannot detect
   it. Ordinary Node-generated versions are intended to be unique. Specify
   that invariant for custom callers, duplicate node identities, and restore
   operations; consider conflict detection instead of silently treating equal
   metadata as convergence. Include exhaustion handling in R1.
4. **Reduce public implementation surface.** Use the next breaking release
   to review full-literal public config structs, hidden internal publisher and
   maintenance types, ignored compatibility fields, and unbounded legacy
   sender APIs. Bon builders already exist; private/non-exhaustive option
   surfaces can avoid another break for each optional setting. Remove obsolete
   compatibility knobs with explicit migration notes.
5. **Split runtime responsibilities.** `node.rs` has about 17,300 lines and
   `replication.rs` about 4,900, including tests. Extract connection/session,
   relay, placement, lifecycle, and sender-state responsibilities with explicit
   lock order and transition ownership. Standardize task-owner shutdown
   patterns where their contracts match. A factory migration should remain
   benchmark-gated work from #49, not a prerequisite for these bug fixes.
6. **Make measurements stable.** `fanout.rs` writes fresh keys repeatedly into
   long-lived Nodes, so retained dataset size grows throughout Criterion
   measurement. Use fixed key sets or bounded/scoped samples. Add load cases
   for query selectivity, fragment count, wide values during partitions, slow
   storage, and retained tombstones. CI currently compiles benches; it does not
   establish throughput or memory ceilings.
7. **Polish diagnostics and release tooling.** Placement failures still use
   `eprintln!` alongside tracing. Standardize structured diagnostics, document
   public lifecycle methods, and remove the duplicate `--package actordb` in
   the package-artifact workflow. This review found no additional release
   provenance failure; all 49 release-guard tests passed.

## Scope and verification

Static review covered the four public crates and their surrounding tooling:

| Area | Paths examined |
| --- | --- |
| Mutation, cache, expiry, workers, supervision | `actordb/src/actor/`, `ttl_expiry.rs` |
| Storage and persistence contracts | `store.rs`, `store/{memory,prefixed,rocksdb}.rs` |
| Routing, placement, startup, shutdown | `node.rs`, `placement.rs`, `runtime_tasks.rs` |
| Replication, reconciliation, ACK fences | `replication.rs`, connection/session handlers |
| Framing, authentication, transport | `wire.rs`, `network.rs`, protocol schemas/preflight |
| SQL and distributed scans | `actordb-query`, `query.rs`, `node/{query,scan}.rs` |
| Application server and SDK | `client_server.rs`, `actordb-client` |
| Write policies and receipt validation | database, SDK, and protocol write-concern modules |
| Release, tests, examples, benchmarks | workflows/scripts, Docker roles, xtask, docs, benches |

The review traced critical paths and checked implementation against documented
contracts; it is not an exhaustive proof or a production soak test. No new
unsafe block was found in the hand-written sources scanned. Frame preflight,
exact mutation outcomes, tombstone tie-breaking, ordered flush ownership,
placement handoff fences, and optional callback admission are substantial
existing safeguards and should be preserved by fixes.

Local audit verification in an isolated checkout:

- Six targeted probes passed while asserting the problematic behaviors.
- `cargo test --workspace --exclude docker-tests`: **520 passed, 0 failed,
  1 ignored**, including those probes.
- Release helpers: **49 passed**.
- PR #111's ten green checks included the backend-free, memory, full-feature,
  Rust 1.88, Docker, Clippy, formatting, docs, package and benchmark gates.
  Native and Docker checks were not repeated for this read-only audit.

The [probe patch](2026-10-20-release-review-probes.patch) applies to the reviewed
commit and contains test-only additions. Reproduce in an isolated checkout:

```bash
git worktree add --detach /tmp/actordb-audit-probes 9dd78dce56d600b994700af0075f845919b3d163
cd /tmp/actordb-audit-probes
git apply /path/to/2026-10-20-release-review-probes.patch
cargo test -p actordb -p actordb-client --test release_audit_probes -- --nocapture
cargo test -p actordb --lib release_audit_ -- --nocapture
```

The close probe pauses Tokio time after a real TCP handshake. The TTL probe
explicitly controls cache-lock acquisition and uses a short wall-clock wait,
because TTL currently reads system time rather than Tokio's paused clock.
No heap-exhaustion or destructive recovery experiment was run.
