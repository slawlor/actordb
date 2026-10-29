# Parking_lot, std and Tokio fragment cache locks

## Production adoption update

PR #195 now adopts the std cache lock. The async unit-test hooks have
been migrated: cache execution pauses use a test-only async gate,
snapshot values/revision are captured before the paused-clone await,
and a dedicated thread retains the real cache guard for the nonblocking
summary witness. Validation passed 702 default tests, 615 selected memory-
feature tests, and Clippy with warnings denied. The measurements and build limitations below describe
the earlier experiment. Replay its patches from commit `b573c94`,
where the cache was still Tokio; they do not target the adopted code.

Measured locally on 2026-10-29 against the shortened cache write path at commit
`f0ff14e55ac2d9fccb889abf1437bc6ce30f641a`.

## Findings

**Parking_lot is faster in the uncontended direct cache methods measured here.**
Relative to std, revision reads took 44.2% less time, 100 B Gets took 6.0% less
time, and 100 B applies took 3.9% less time. Both Parking_lot repetitions were
faster than both std repetitions in all three direct cases.

This does not translate into a universal actor throughput improvement. A single
actor write still takes roughly 10 microseconds with all three implementations,
so a few nanoseconds saved in acquisition are a small part of that cost.
Parking_lot and std were close in concurrent write-only cases: Parking_lot was
3.8% slower at 100 B, 1.3% slower at 10 KB, and 0.4% faster at 1 MB in the
mean rates. Both beat Tokio in those contended write-only cases. Other actor
workloads were mixed and sometimes varied substantially between repetitions.

The result supports Parking_lot's fast-path claim, with modest benefits for
actual small Gets/applies beyond std. It does not establish Parking_lot as the
best implementation for every ActorDB workload.

## Method

- Apple M4 Max, arm64, 16 cores; Rust 1.97.0, optimized benchmark profile,
  default crate features. Parking_lot **0.12.5**, default features only. This
  version supports the project's Rust 1.88 minimum; available 0.13.0 requires
  Rust 1.95. No `send_guard`, deadlock detection or other optional feature was
  enabled.
- Only the fragment cache lock changes. Table routing, snapshot fences and
  flush locks retain their existing types; the summary index already uses a
  std lock. The Parking_lot change also adapts cache try-read checks to its
  Option-returning API and adds its dependency.
- Build all three executables first against the same measured Rust harness,
  generated lockfile and experimental manifest (Parking_lot included as a
  dependency even in the Tokio/std builds). Run **Tokio → Std → Parking_lot →
  Parking_lot → Std → Tokio**, without concurrent builds or tests.
- The existing 13 actor workloads use 20 Criterion samples, 500 ms warmup and
  three seconds measurement. The three added direct cases use 30 samples with
  the same warmup/measurement durations. Every implementation has two runs,
  for 96 measured case executions in total.
- Report arithmetic means of the two per-run point estimates. Each point uses
  Criterion's slope when available, otherwise its mean. Actor percentage
  changes compare mean rates; direct percentage changes compare mean times.
  Repeat ranges below are separate from within-run confidence intervals.

## Direct uncontended cache methods

One caller on a current-thread Tokio runtime accesses one populated, standalone
FragmentState. No actor dispatch, network, disk or concurrent cache access is
present. These are real public cache methods, not empty lock/unlock loops:
revision reads include an atomic revision load, Gets include lookup/TTL checks
and payload cloning, and applies include record checks, hashing, cache/index
mutation and metadata publication. Apply input preparation is excluded.

| Direct uncontended operation | Tokio ns/op | Std ns/op | Parking_lot ns/op | Parking_lot time vs Std |
|---|---:|---:|---:|---:|
| Revision read (lock + atomic load) | 113.98 | 7.64 | 4.26 | -44.2% |
| Cached Get, 100 B | 183.82 | 65.77 | 61.82 | -6.0% |
| Apply, 100 B | 523.61 | 423.35 | 406.85 | -3.9% |

Negative percentages above mean less time per operation. A revision read is the
closest case to acquisition overhead; the Get/apply cases show how that saving
changes when useful cache work is included.

## Actor throughput

The actor workloads retain the prior comparison's setup: one cache/dispatcher,
four workers in each of three unlimited priority pools, fixed independent keys,
explicit runtime threads and depths, and optional no-op buffered persistence.
Input allocation and flushing are excluded. Dispatch, cache work, hashing,
enqueue and result consumption are timed. Every Put must report Applied, every
Get must return the expected payload size, and teardown verifies final versions.

Ops/s counts completed requests. In write-only rows it is writes/s; in 75%-read
rows writes/s is one quarter of ops/s. KB/MB are decimal payload sizes.

| Actor workload | Tokio ops/s | Std ops/s | Parking_lot ops/s | Parking_lot vs Std |
|---|---:|---:|---:|---:|
| Writes; 1 thread; depth 1; 100 B | 113,879 | 117,657 | 116,047 | -1.4% |
| Writes; 4 threads; depth 1; 100 B | 97,548 | 98,615 | 96,882 | -1.8% |
| Writes; 1 thread; depth 24; 100 B | 568,461 | 561,596 | 564,940 | +0.6% |
| Writes; 4 threads; depth 24; 100 B | 515,342 | 729,042 | 701,478 | -3.8% |
| Writes; 4 threads; depth 24; 10 KB | 260,577 | 368,047 | 363,360 | -1.3% |
| Writes; 4 threads; depth 24; 1 MB | 7,991 | 9,699 | 9,737 | +0.4% |
| Buffered writes; 4 threads; depth 24; 100 B | 482,144 | 469,722 | 438,757 | -6.6% |
| Buffered writes; 4 threads; depth 24; 1 MB | 7,540 | 6,997 | 7,156 | +2.3% |
| 75% reads; 1 thread; depth 24; 100 B | 680,381 | 613,842 | 555,824 | -9.5% |
| 75% reads; 4 threads; depth 24; 100 B | 631,057 | 757,086 | 658,957 | -13.0% |
| 75% reads; 4 threads; depth 24; 1 MB | 25,129 | 24,572 | 24,660 | +0.4% |
| 75% reads; 4 threads; depth 24; 4 MB | 6,521 | 6,511 | 6,425 | -1.3% |
| Read only; 4 threads; depth 24; 1 MB | 105,373 | 109,332 | 106,674 | -2.4% |

## Repeat ranges

| Case | Tokio range | Std range | Parking_lot range |
|---|---:|---:|---:|
| `write_t1_c1_100b` | 111,361–116,397 ops/s | 117,005–118,310 ops/s | 115,645–116,449 ops/s |
| `write_t4_c1_100b` | 95,390–99,705 ops/s | 98,116–99,114 ops/s | 95,676–98,087 ops/s |
| `write_t1_c24_100b` | 563,752–573,170 ops/s | 555,289–567,904 ops/s | 562,807–567,073 ops/s |
| `write_t4_c24_100b` | 501,920–528,765 ops/s | 720,456–737,628 ops/s | 693,798–709,158 ops/s |
| `write_t4_c24_10kb` | 257,893–263,261 ops/s | 367,608–368,485 ops/s | 356,703–370,018 ops/s |
| `write_t4_c24_1mb` | 7,989–7,993 ops/s | 9,663–9,736 ops/s | 9,609–9,865 ops/s |
| `buffered_t4_c24_100b` | 480,865–483,424 ops/s | 459,363–480,081 ops/s | 401,256–476,257 ops/s |
| `buffered_t4_c24_1mb` | 7,488–7,593 ops/s | 6,583–7,412 ops/s | 6,740–7,573 ops/s |
| `read75_t1_c24_100b` | 673,897–686,865 ops/s | 541,352–686,331 ops/s | 539,891–571,756 ops/s |
| `read75_t4_c24_100b` | 626,994–635,119 ops/s | 670,739–843,432 ops/s | 658,257–659,657 ops/s |
| `read75_t4_c24_1mb` | 25,053–25,204 ops/s | 24,168–24,975 ops/s | 24,199–25,122 ops/s |
| `read75_t4_c24_4mb` | 6,496–6,547 ops/s | 6,440–6,583 ops/s | 6,231–6,619 ops/s |
| `read100_t4_c24_1mb` | 104,662–106,084 ops/s | 108,306–110,358 ops/s | 106,445–106,902 ops/s |
| `direct_revision_read` | 110.65–117.32 ns | 7.64–7.64 ns | 4.23–4.29 ns |
| `direct_get_100b` | 183.81–183.82 ns | 64.91–66.62 ns | 61.13–62.51 ns |
| `direct_apply_100b` | 517.30–529.92 ns | 417.08–429.63 ns | 402.33–411.36 ns |

Several actor rows overlap or show substantial variation, including buffered
traffic and the single-thread mixed workload. The 13% mean Parking_lot deficit
in mixed small four-thread traffic is also affected by a std repeat ranging
from 671k to 843k ops/s; it should not be treated as a precisely established
workload-independent difference. The direct-case ranges are much tighter and
do not overlap between std and Parking_lot in this experiment.

The actor cases also collected the same 1 ms timer probe as the previous
comparison. Raw p50/p99/max wakeup delays and sample counts are retained in
`results.json`. They are scheduler wakeup delays, not RPC tail latency; with two
runs per lock and noisy scheduling, no production latency conclusion is claimed.

## Validation and adoption boundary

All three optimized benchmark builds passed. The Parking_lot production library
passed **128 integration tests** across cluster, version conflicts and tables.
The checked dependency feature tree showed Parking_lot's default features only.
The expanded benchmark passed Clippy with warnings denied and formatting checks.

As with the std experiment, existing unit fixtures assume async cache guards,
including a test-only snapshot pause that awaits under a read guard. Parking_lot
uses synchronous acquisition and its default guards are not Send; these fixtures
need migration before adopting a synchronous cache in production. This benchmark
does not claim the full unit suite passed for the experimental Parking_lot build.
Parking_lot also does not poison its locks; fairness and poisoning differences
are separate from the throughput measurements here.

At the end of the experiment, the original library and manifest were restored. The replayable
[Parking_lot patch](fragment_cache_parking_lot.patch) contains the experimental
dependency/cache change. The [std patch](fragment_cache_std.patch) reproduces the
other synchronous variant.

## Reproduction and artifacts

To compare the current Tokio implementation with Parking_lot:

```sh
ACTORDB_CACHE_LOCK_RESULTS=target/tokio-cache-timers.jsonl \
  cargo bench --locked -p actordb --bench fragment_locks -- --save-baseline tokio-cache

git apply --unidiff-zero actordb/benches/fragment_cache_parking_lot.patch
ACTORDB_CACHE_LOCK_RESULTS=target/parking-cache-timers.jsonl \
  cargo bench -p actordb --bench fragment_locks -- --baseline tokio-cache

git apply -R --unidiff-zero actordb/benches/fragment_cache_parking_lot.patch
```

The patch uses zero-context hunks against the matching source base. The first
Parking_lot build may update the ignored lockfile for the added dependency.
For an exact six-run repetition, keep its dependency present in all three
build manifests, save each executable, then alternate the saved executables in
A/B/C/C/B/A order.

All raw Criterion estimates/samples, per-case timer observations, source and
manifest snapshots, the common generated lockfile, saved executables/hashes,
logs and the six-run driver are in `target/parking-lot-comparison/`.
`results.json` holds the full records and `summary.json` the mean/range
calculations. Criterion retains the six uniquely named baselines under
`target/criterion/fragment_locks/` and `target/criterion/cache_fast_path/`.
