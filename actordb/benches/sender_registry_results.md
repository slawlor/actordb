# Fragment-indexed sender registry — 2026-10-29

## Change and conclusions

Issue #190's mutation fanout scanned the node-wide `(fragment, peer)` sender
map under a shared Tokio mutex. The new authoritative registry is
`HashMap<FragmentId, HashMap<String, NodeSender>>`. Lookup and handle cloning
visit only the target fragment's bucket; payload cloning and mailbox admission
remain outside the mutex. Replacement, exact-task checks and retirement seals
retain the same locking order. Removing the last peer removes its bucket.

The index removes the registry-size cost, but these measurements show a
tradeoff:

- At 8,192 registry fragments and two targets, lookup throughput improved
  **94.4× with one caller and 61.6× with 32 callers**.
- The controlled write case with eight real fragments and 8,192 registry entries
  improved **3.58×** for 100 B values / 32 callers and **2.71×** for
  16 KiB values / 32 callers. These are synthetic unrelated entries, not
  thousands of real fragment actors.
- A one-fragment lookup was **16.5–19.9% slower**, adding roughly 22–43 ns per
  lookup. Hashing and bucket indirection add costs to this representation;
  the measurements do not isolate their individual contributions.
- Real Node throughput was mixed. At 512 real fragments, the node-only repeat
  measured **18.5% fewer writes/s** for 100 B / 32 callers, and **11.4% fewer**
  for 16 KiB / 32 callers. The earlier sweep also showed a slowdown in the
  100 B / 32-caller case. Its cause is not established by these measurements.
  Treat this as a performance risk, not as evidence of a universal Node win.

## Method

Baseline: `4872790ecdd229e5e28236105f2545a8f04b6401` (merged std cache locks,
0.11.1). Only the production sender lookup was extracted into a private helper
for measurement before implementing the index. Both versions use the same
benchmark source, dependencies and release profile, with the existing std
cache locks and Tokio sender-registry mutex.

Machine: Apple M4 Max, macOS arm64, 16 cores; rustc 1.97.0
`2d8144b78` (2026-07-07), Cargo 1.97.0. Runtime: four threads. The diagnostic
is a release-profile library test binary with default features and no store.
It includes the test-only shared cache pause gate identically in both versions;
absolute actor latencies should not be treated as production guarantees.

Both executables were built before timing. Each sweep ran **A/B/B/A**, with no
builds or tests in parallel. Two runs per version are reported. Rates below pool
the measured duration across both equal-sized runs, rather than averaging the
rates. Parentheses show the minimum and maximum per-run rates.

Lookup: 1,000,000 acquisitions per case, split over 1/32 Tokio tasks. Every
lookup calls the production snapshot helper, clones two real sender handles,
and drops them after unlocking. Unrelated slots alias those handles to isolate
registry cardinality; targets rotate across fragment IDs. It measures the mutex,
lookup, vector allocation, handle clones and drops, not actor writes or offers.

Node: two real TCP-connected nodes, one remote target per fragment, one unlimited
worker per priority pool per fragment. Source writes use 1/32 callers, fixed
keys and at most eight active fragments; 50 warmup and 400 measured batches.
Each value carries a generation marker. Timing covers completed `Node::put`
local application and replication admission; input preparation and exact-value
replica visibility are outside timing. Every batch drains before the next and
sender reports are checked for buffer reset. This measures source completion,
not replicated durability or sustained disk throughput.

Controlled Node: exactly eight real fragments, actors and sender timers stay
fixed while the source registry grows to 8/64/1,024/8,192 entries. Extra entries
alias a real handle and are never targeted. Both Hello handlers finish before
injection: an earlier experiment injected before readiness and was discarded
because setup could repeatedly notify the aliased task.

The real Node sweep was repeated separately with `ACTORDB_SENDER_BENCH_MODE=node`
to check the 512-fragment slowdown without the preceding lookup workload. That
node-only sweep supplies the real Node table below. Background sender timers
and actor scheduling grow with real fragment count, so it is not a pure map
benchmark. There was substantial variation between runs; no p99 or deployment
throughput claim is made.

## Lookup results

| Registry fragments | Callers | Before ns/lookup | After ns/lookup | Throughput change |
|---:|---:|---:|---:|---:|
| 1 | 1 | 109.4 | 131.1 | -16.5% |
| 1 | 32 | 172.3 | 215.2 | -19.9% |
| 64 | 1 | 173.7 | 118.8 | +46.2% |
| 64 | 32 | 264.2 | 222.2 | +18.9% |
| 1,024 | 1 | 1,580.7 | 127.8 | +1136.4% |
| 1,024 | 32 | 2,028.8 | 224.6 | +803.4% |
| 8,192 | 1 | 12,879.9 | 136.4 | +9339.3% |
| 8,192 | 32 | 14,094.4 | 228.7 | +6061.9% |

## Controlled Node write results

Rates are thousands of completed writes/s; parentheses give the two-run range.

| Registry entries | Callers | Value | Before k writes/s (range) | After k writes/s (range) | Change |
|---:|---:|---:|---:|---:|---:|
| 8 | 1 | 100 B | 17.3 (12.2–29.6) | 18.9 (18.8–19.1) | +9.4% |
| 8 | 32 | 100 B | 113.2 (89.8–153.2) | 126.6 (122.3–131.2) | +11.8% |
| 8 | 32 | 16 KiB | 69.3 (59.2–83.6) | 85.2 (84.2–86.3) | +22.9% |
| 64 | 1 | 100 B | 15.7 (12.7–20.6) | 16.2 (14.5–18.4) | +3.3% |
| 64 | 32 | 100 B | 103.5 (83.6–135.8) | 126.3 (120.7–132.5) | +22.1% |
| 1,024 | 1 | 100 B | 13.1 (10.7–16.9) | 16.7 (16.4–17.2) | +27.7% |
| 1,024 | 32 | 100 B | 89.2 (82.1–97.5) | 103.3 (84.0–134.1) | +15.8% |
| 8,192 | 1 | 100 B | 7.4 (5.9–9.8) | 15.5 (13.6–18.0) | +110.2% |
| 8,192 | 32 | 100 B | 27.6 (21.1–39.9) | 98.8 (83.3–121.5) | +258.4% |
| 8,192 | 32 | 16 KiB | 28.1 (24.1–33.8) | 76.2 (64.1–94.0) | +171.0% |

## Real Node write results

Rates are thousands of completed writes/s; parentheses give the two-run range.

| Real fragments | Callers | Value | Before k writes/s (range) | After k writes/s (range) | Change |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 100 B | 16.4 (14.0–19.6) | 16.2 (12.7–22.2) | -1.1% |
| 1 | 32 | 100 B | 109.7 (84.9–154.9) | 117.4 (102.4–137.6) | +7.0% |
| 1 | 32 | 16 KiB | 50.6 (46.3–55.9) | 47.5 (41.6–55.4) | -6.2% |
| 64 | 1 | 100 B | 4.7 (3.7–6.6) | 5.0 (4.7–5.4) | +6.7% |
| 64 | 32 | 100 B | 97.2 (84.0–115.4) | 98.7 (92.1–106.2) | +1.5% |
| 512 | 1 | 100 B | 2.7 (2.2–3.8) | 2.2 (1.9–2.6) | -20.1% |
| 512 | 32 | 100 B | 76.3 (72.0–81.1) | 62.2 (61.5–62.8) | -18.5% |
| 512 | 32 | 16 KiB | 66.4 (59.6–74.8) | 58.8 (58.2–59.3) | -11.4% |

## Verification and reproduction

- Default `cargo test --locked -p actordb`: **703 passed, 2 ignored**.
- `memory` library, cluster, table and version-conflict tests: **616 passed,
  2 ignored**, including durable handoff and retirement coverage.
- Clippy on the library, tests and benches with all warnings denied: passed.
- The new regression witnesses fragment-scoped admission, stale-task rejection,
  empty-bucket retirement/recreation and preservation of an unrelated fragment.
  Existing cancellation, exact sender replacement, namespace and handoff tests
  remain intact.

Run the manual diagnostic:

```bash
cargo test --locked -p actordb --release --lib sender_registry_scaling -- --ignored --nocapture
ACTORDB_SENDER_BENCH_MODE=node cargo test --locked -p actordb --release --lib sender_registry_scaling -- --ignored --nocapture
ACTORDB_SENDER_BENCH_MODE=isolated cargo test --locked -p actordb --release --lib sender_registry_scaling -- --ignored --nocapture
```

For a comparison, build and save the two executables before running either,
then invoke each saved executable with `sender_registry_scaling --ignored
--nocapture` in A/B/B/A order. The baseline uses the same diagnostic and the
original flat-map snapshot helper. Do not include compilation in the timing.

Local artifacts are retained under `target/sender-index-comparison/`: baseline
helper/source patches, both optimized executables, the exact Cargo.lock,
`diagnostic-ready.rs`, all run logs, and `comparison.json`. Reported lookup runs
are `final-{before-a1,after-b1,after-b2,before-a2}`; reported real Node runs are
`node-{before-a1,after-b1,after-b2,before-a2}`; corrected controlled runs are
`ready-{before-a1,after-b1,after-b2,before-a2}`. Earlier exploratory results are
retained but not pooled into these tables. For the real Node and corrected
controlled sweeps, executables were:

- Before SHA-256: `079a5dd327758408a99a2d8e8c875339e0617dc5154278e42e793cbd8fa1fd15`
- After SHA-256: `bd221a136c9cf3d9a39daf3580931b29132914eb134f2a5e0d2561c168729a99`

0.11.1 remains the shared unpublished patch release; no extra version bump or
release publication is part of #190.
