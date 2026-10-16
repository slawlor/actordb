# actordb benchmarks

Criterion benchmarks for operation performance. These are **manual/local only**
— deliberately not in CI (too heavy, too noisy for shared runners).

Configurations use Bon builders without changing benchmark tuning. The
replication/fan-out fixtures deliberately replace all priority profiles with
large unlimited pools; a standalone `PoolConfig` builder defaults to four
workers and no rate limit, unlike the Node's rate-limited Background profile.
The single-node fixture omits overrides and keeps the Node defaults.

## Running them

```bash
# Full run (backend-free defaults; storeless fixtures need no libclang)
cargo bench -p actordb

# Optional RocksDB-enabled build (requires libclang; fixtures remain storeless)
cargo bench -p actordb --features rocksdb

# Quick smoke run (~10x faster, coarser numbers)
cargo bench -p actordb -- --quick

# One bench file only
cargo bench -p actordb --bench put_get
cargo bench -p actordb --bench fanout
cargo bench -p actordb --bench replication
cargo bench -p actordb --bench sender_registry
```

Criterion saves results under `target/criterion/` and compares against the
previous run automatically — the second run of the same bench shows
regressions/improvements vs the first.

Neither backend feature is needed for these cache/replication benchmarks.
`memory` only exposes a no-op store test double; it is not the fragment cache
and enabling it would not turn these fixtures into persistence benchmarks.
`--no-default-features` is currently equivalent to the backend-free default.
All configurations still compile the bundled protobuf toolchain from source.

## What each bench measures

| Bench file        | Setup                              | Cases                                              |
|-------------------|------------------------------------|----------------------------------------------------|
| `put_get.rs`      | 1 node, in-memory, no network      | Sequential `put` / `get` latency at 100 B, 10 KB, 1 MB; concurrent sticky-vs-spread Gets at depths 1/8/64 |
| `fanout.rs`       | 3 nodes, 3 fragments (1 each)      | `put_multi` / `get_multi` at 10, 100, 1000 keys    |
| `replication.rs`  | 2 nodes, 1 shared fragment         | timer-dominated visibility lag for 10/100/1000 keys; 1 ms-tick disconnected catch-up for 64/256/1024/4096 compact records |
| `sender_registry.rs` | 2 nodes, 1 or 64 shared fragments | source apply/fan-out at concurrency 1/32/64 with 100 B or 1 MiB values; replica catch-up excluded from timing |

Each file has a doc comment explaining what it measures and how to read a
regression — start there before tuning.

Automatic expiry maintenance also has an ignored internal release diagnostic,
separate from Criterion. It compares the full-sweep primitive with the actual
Cleaner under concurrent actor-routed High/default reads and overwrites, with
dense/sparse keysets and several TTL fractions. It reports bounded foreground
samples separately from materialization/drain times; no release baseline or
p99 improvement has been recorded. See [test strategy](../../docs/testing.md)
for the witness, sampling and storeless-scope caveats.

```bash
cargo test -p actordb --release \
  maintenance_foreground_latency_distribution -- --ignored --nocapture
```

## Baselines

Recorded 2026-10-14 on a shared dev VM (multi-thread tokio, storeless fragment cache,
`--no-default-features`, release profile). Your numbers will differ; what
matters is the trend on the *same* machine.

### put_get (mean latency)

| case | 100 B   | 10 KB   | 1 MB    |
|------|---------|---------|---------|
| put  | 20.0 µs | 23.1 µs | 243.9 µs|
| get  | 14.0 µs | 15.1 µs | 74.7 µs |

(Re-baselined 2026-10-14 after TCP_NODELAY; single-op path unaffected.)

### fanout (mean per-batch latency)

| case      | 10 keys | 100 keys | 1000 keys |
|-----------|---------|----------|-----------|
| put_multi | 190 µs  | 371 µs   | 2.05 ms   |
| get_multi | 186 µs  | 308 µs   | 1.69 ms   |

(Re-baselined on `main` on 2026-10-16 after the sender-registry and incremental
protobuf-sizing work. The actor-routed `BatchGet` continuation remained near
1.7 ms at 1000 keys in a focused run; its benefit is bounded admission and
result memory, not speculative parallel reads.)

### replication (mean source-apply-completion-to-replica-visible lag)

| batch size | 10 keys | 100 keys | 1000 keys |
|------------|---------|----------|-----------|
| lag        | 100.1 ms| 100.1 ms | 1001.8 ms |

Note: lag is dominated by the sender `batch_interval` (100 ms): 10 keys and
100 keys both take one batch (~100 ms); 1000 keys take 10 batches (~1 s).
This is finding #25 — shipping full batches immediately would cut this ~10x.

### replication buffered catch-up (mean connect-to-ACK latency)

Recorded 2026-10-16 on the maintainer's dev VM with fresh in-memory node pairs,
`batch_size = 4096`, a 1 ms interval, and `--quick`. Preload, verification, and
shutdown are excluded from the reported duration; connection establishment,
encoding, receiver apply, and ACK remain included.

| batch size | 64 | 256 | 1024 | 4096 |
|------------|----|-----|------|------|
| catch-up   | 2.84 ms | 3.43 ms | 5.99 ms | 15.93 ms |

The 4,096-record case was 45.71 ms before incremental protobuf sizing and
15.93 ms after it on the same machine (65% lower). Unlike `replication_lag`,
this group reduces the sender tick to 1 ms instead of hiding batch construction
behind the default 100 ms timer.

## Convention for perf-relevant changes

1. Run the affected bench **before** your change; note the numbers.
2. Make the change; run the same bench **after**.
3. If the delta is real (not noise — re-run if unsure), paste the
   before/after numbers into the PR description.
4. If the change moves a baseline permanently (e.g. a deliberate
   throughput/latency tradeoff), update the table above in the same PR.

## Known perf-relevant findings (from the 2026-10-14 adversarial review)

These are the first things to measure when they get fixed:

- ~~#30 — no TCP_NODELAY~~ **fixed 2026-10-14** (`set_nodelay(true)` on
  accept + connect in `network.rs`): `get_multi` 99.9/105.6/120.8 ms →
  0.22/0.79/8.31 ms.
- #14 — dispatcher ingress remains a shared serial funnel. Worker-bound Gets
  no longer make it await a nested RPC; watch sequential `put`/`get` plus
  `concurrent_get/{sticky,spread}` in `put_get`.
- ~~#24 — one actor RPC channel per `BatchGet` key~~ **replaced 2026-10-16**
  with one ordered aggregate continuation. It deliberately keeps one value
  clone and one per-key admission in flight, so this is a boundedness/fairness
  fix rather than eager parallel read fan-out. Keep watching `get_multi` at
  1000 keys in `fanout`; a same-machine focused run remained around
  1.7 ms (within noise of the pre-change result).
- #23 — per-op `hlc.observe()` acquisitions in the replication receiver loop
  → watch `replication_lag` scaling with batch size.
- Sender-registry critical sections → watch `sender_registry` scale across
  value size, registry cardinality, and cross-fragment concurrency. Payload
  cloning and per-sender mailbox admission belong outside the global registry
  lock.
- #25 — sender `batch_interval` timer-gates full batches; `max_in_flight=4`
  → watch the `replication_lag` floor independent of batch size.
- Incremental ordinary replication batch sizing is protected by
  `replication_catchup/buffered_catchup`; watch 4,096-record scaling for a
  return of repeated whole-batch scans.
- #18 — anti-entropy clones full values to build summaries
  → not covered by these benches yet; needs a reconcile bench.
