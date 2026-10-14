# actordb benchmarks

Criterion benchmarks for operation performance. These are **manual/local only**
— deliberately not in CI (too heavy, too noisy for shared runners).

## Running them

```bash
# Full run (default features; needs libclang for RocksDB — CI has it)
cargo bench -p actordb

# Lean run (in-memory store only; works without libclang)
cargo bench -p actordb --no-default-features

# Quick smoke run (~10x faster, coarser numbers)
cargo bench -p actordb -- --quick

# One bench file only
cargo bench -p actordb --bench put_get
cargo bench -p actordb --bench fanout
cargo bench -p actordb --bench replication
```

Criterion saves results under `target/criterion/` and compares against the
previous run automatically — the second run of the same bench shows
regressions/improvements vs the first.

## What each bench measures

| Bench file        | Setup                              | Cases                                              |
|-------------------|------------------------------------|----------------------------------------------------|
| `put_get.rs`      | 1 node, in-memory, no network      | `put` / `get` latency at 100 B, 10 KB, 1 MB values |
| `fanout.rs`       | 3 nodes, 3 fragments (1 each)      | `put_multi` / `get_multi` at 10, 100, 1000 keys    |
| `replication.rs`  | 2 nodes, 1 shared fragment         | source-apply-completion-to-replica-visible lag for 10, 100, 1000-key batches |

Each file has a doc comment explaining what it measures and how to read a
regression — start there before tuning.

## Baselines

Recorded 2026-10-14 on the maintainer's dev VM (multi-thread tokio, in-memory
store, `--no-default-features`). Your numbers will differ; what matters is the
trend on the *same* machine.

Recorded 2026-10-14 on a shared dev VM (multi-thread tokio, in-memory store,
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
| put_multi | 175 µs  | 1.41 ms  | 12.87 ms  |
| get_multi | 225 µs  | 790 µs   | 8.31 ms   |

(Re-baselined 2026-10-14: `TCP_NODELAY` fixed the old ~100 ms floor —
finding #30 closed. `get_multi` went from 99.9/105.6/120.8 ms to
0.22/0.79/8.31 ms, a 93–99% reduction.)

### replication (mean source-apply-completion-to-replica-visible lag)

| batch size | 10 keys | 100 keys | 1000 keys |
|------------|---------|----------|-----------|
| lag        | 100.1 ms| 100.1 ms | 1001.8 ms |

Note: lag is dominated by the sender `batch_interval` (100 ms): 10 keys and
100 keys both take one batch (~100 ms); 1000 keys take 10 batches (~1 s).
This is finding #25 — shipping full batches immediately would cut this ~10x.

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
- #14 — dispatcher is a serial funnel; the per-put barrier doubles its load
  → watch `put` in `put_get`.
- #24 — `BatchGet` serves keys with sequential actor round-trips
  → watch `get_multi` at 1000 keys in `fanout`.
- #23 — per-op `hlc.observe()` acquisitions in the replication receiver loop
  → watch `replication_lag` scaling with batch size.
- #25 — sender `batch_interval` timer-gates full batches; `max_in_flight=4`
  → watch the `replication_lag` floor independent of batch size.
- #18 — anti-entropy clones full values to build summaries
  → not covered by these benches yet; needs a reconcile bench.
