# Tokio vs synchronous fragment cache locks

## Production adoption update

PR #195 now adopts the std cache lock. The async unit-test hooks have
been migrated: cache execution pauses use a test-only async gate,
snapshot values/revision are captured before the paused-clone await,
and a dedicated thread retains the real cache guard for the nonblocking
summary witness. Validation passed 702 default tests, 615 selected memory-
feature tests, and Clippy with warnings denied. The measurements and build limitations below describe
the earlier experiment. Replay its patches from commit `f0ff14e`,
where the cache was still Tokio; they do not target the adopted code.

Local measurements on 2026-10-29, based on the shortened write critical
sections at commit `97e41592a4b4feb3807010e73add3be3e701ea1e` (0.11.1 PR).

## Findings

The standard library `std::sync::RwLock` improved the write-heavy cache path
with four runtime threads and 24 concurrent requests: **41.7% at 100 B,
47.8% at 10 KB, and 19.4% at 1 MB**. Both synchronous repeats beat both Tokio
repeats in these three cases. Mixed traffic with small values also improved;
one-thread, buffered and large-read workloads had small or inconsistent changes.

These are actor-request throughput measurements on one shared cache, not
network RPC or disk throughput. The synchronous cache is a promising candidate
for write-heavy workloads; adopting it also requires migration of the existing
async unit-test contention hooks and a full validation pass.

## Experiment

- Apple M4 Max, arm64, 16 cores; Rust 1.97.0, optimized bench profile, default
  crate features and the same generated lockfile for both variants.
- Replace only `FragmentState.cache` and its read/write acquisitions with
  `std::sync::RwLock`. Flush-buffer mutexes, flush-ordering mutexes, table routing
  and snapshot fences retain Tokio locks because their guards can span async
  acquisition or I/O. Summary metadata already uses a synchronous lock.
- Build and save both executables before timing. Run all 13 cases in
  **Tokio → Std → Std → Tokio** order, with no concurrent tests or compilation.
- Each case has 20 Criterion samples, 500 ms warmup and three seconds of
  measurement. Report arithmetic means of the two run rate estimates;
  `Std change = Std mean / Tokio mean - 1`. Within a run, use Criterion's
  slope estimate when present, otherwise its mean estimate.
- One dispatcher, one shared cache, four workers in each of three unlimited
  priority pools, a 1,024-job admission cap per pool and fixed independent keys.
  Depth 24 distributes requests over all priorities and workers; depth 1 uses
  Default. Runtime thread count is explicitly one or four.
- Workloads are write only, 75% reads/25% writes, and read only. Buffered cases
  use a no-op store and defer flushing to teardown. Input preparation, fixture
  setup and flushing are excluded; actor dispatch, hashing, cache work,
  persistence enqueue and result consumption are timed.
- Every Put must return Applied, every Get must return a value of the expected
  size, and teardown verifies final versions for written and unwritten keys.

## Throughput

**Ops/s counts completed requests.** In write-only rows it is writes/s. For
75%-read rows, writes/s is one quarter of ops/s. Read-only rows contain no writes.
MB/KB are decimal payload sizes.

| Workload | Threads | Concurrency | Value | Tokio ops/s | Std ops/s | Std change |
|---|---:|---:|---:|---:|---:|---:|
| Write only | 1 | 1 | 100 B | 114,487 | 112,025 | -2.2% |
| Write only | 4 | 1 | 100 B | 96,532 | 95,621 | -0.9% |
| Write only | 1 | 24 | 100 B | 559,740 | 580,468 | +3.7% |
| Write only | 4 | 24 | 100 B | 527,364 | 747,226 | +41.7% |
| Write only | 4 | 24 | 10 KB | 257,478 | 380,616 | +47.8% |
| Write only | 4 | 24 | 1 MB | 8,279 | 9,889 | +19.4% |
| Buffered writes | 4 | 24 | 100 B | 481,688 | 488,238 | +1.4% |
| Buffered writes | 4 | 24 | 1 MB | 7,761 | 7,248 | -6.6% |
| 75% reads | 1 | 24 | 100 B | 666,716 | 588,340 | -11.8% |
| 75% reads | 4 | 24 | 100 B | 614,959 | 719,224 | +17.0% |
| 75% reads | 4 | 24 | 1 MB | 25,080 | 24,647 | -1.7% |
| 75% reads | 4 | 24 | 4 MB | 6,476 | 6,380 | -1.5% |
| Read only | 4 | 24 | 1 MB | 108,817 | 110,319 | +1.4% |

## Repeat ranges and timer responsiveness

A 1 ms Tokio timer runs on the same runtime. Its epoch counter records any
interval that overlaps a timed batch, including a batch completed before the
timer can wake; this avoids discarding delays when a synchronous wait stalls
an entire batch. Samples include warmup and can span excluded input setup.
They do not include Criterion's idle analysis phase when no batch epoch advances.
Timer p99 measures scheduler wakeup delay, not application-request p99 latency.

| Case | Tokio ops/s range | Std ops/s range | Tokio timer p99 range (ms) | Std timer p99 range (ms) |
|---|---:|---:|---:|---:|
| `write_t1_c1_100b` | 114,241–114,733 | 108,776–115,275 | 0.220–0.877 | 0.423–0.883 |
| `write_t4_c1_100b` | 96,434–96,630 | 94,718–96,523 | 1.900–1.924 | 1.921–1.929 |
| `write_t1_c24_100b` | 559,502–559,978 | 579,121–581,815 | 0.964–0.974 | 0.885–0.953 |
| `write_t4_c24_100b` | 522,780–531,947 | 743,288–751,164 | 0.951–0.964 | 0.901–0.920 |
| `write_t4_c24_10kb` | 255,992–258,965 | 380,084–381,148 | 0.861–0.875 | 0.821–0.838 |
| `write_t4_c24_1mb` | 8,187–8,371 | 9,803–9,976 | 2.822–2.889 | 2.907–2.953 |
| `buffered_t4_c24_100b` | 480,691–482,685 | 478,471–498,006 | 0.940–0.952 | 0.904–0.948 |
| `buffered_t4_c24_1mb` | 7,613–7,909 | 6,704–7,792 | 2.831–2.963 | 2.811–3.013 |
| `read75_t1_c24_100b` | 666,557–666,875 | 508,016–668,664 | 0.933–0.945 | 0.965–1.300 |
| `read75_t4_c24_100b` | 606,438–623,479 | 666,568–771,880 | 0.939–0.971 | 0.902–0.942 |
| `read75_t4_c24_1mb` | 25,032–25,128 | 24,251–25,043 | 1.208–1.246 | 0.947–1.060 |
| `read75_t4_c24_4mb` | 6,474–6,478 | 6,247–6,512 | 1.522–1.771 | 1.129–1.805 |
| `read100_t4_c24_1mb` | 105,453–112,181 | 106,825–113,812 | 0.406–0.739 | 0.468–1.081 |

Timer ranges generally overlap, with some individually slower synchronous
repeats. This small synthetic probe does not establish a production tail-latency
improvement or regression. Read cloning still runs synchronously under the cache
read guard in both variants, and synchronous acquisition can block runtime
threads under contention. Standard-library fairness/poisoning behavior also
differs from Tokio; these platform-specific measurements do not establish
results for other hosts or lock implementations.

## Investigating the mixed single-thread outlier

The first Std mixed single-thread run was 508k ops/s, while its second run was
669k; the two full Tokio runs were both about 667k. To check whether the observed
11.8% mean reduction was stable, rerun only that case in A/B/B/A order:

| Isolated single-thread mixed run | Ops/s | Timer p99 (ms) |
|---|---:|---:|
| tokio `a1` | 672,180 | 0.955 |
| std `b1` | 710,476 | 0.955 |
| std `b2` | 632,935 | 1.023 |
| tokio `a2` | 678,155 | 0.987 |

The isolated means were **675,168 Tokio ops/s** and **671,706 Std ops/s**
(-0.5%). The synchronous repeat range straddles the Tokio range.
A stable single-thread mixed-workload improvement or regression is not established.

## Validation and adoption boundary

Both release benchmark builds passed, the new benchmark passed Clippy with
warnings denied, and the synchronous production library
passed **128 integration tests** across `cluster`, `version_conflicts` and
`tables`. These exercise real actor requests, priority routing, replication,
TTL, conflict handling, shared persistence admission and table namespaces.

The synchronous library **unit-test build did not pass**. Existing unit fixtures
still call async cache acquisitions, and the test-only snapshot pause in
`FragmentState::snapshot_local` awaits a notification while retaining its cache
read guard. A std guard is not Send, so that hook also makes actor futures
non-Send in test builds. These test hooks need redesign before shipping the
synchronous implementation; production builds contain no await under a cache
guard. This experiment does not claim the full unit suite passed for Std.

At the end of the experiment, the source was restored to Tokio. The measured
synchronous change is saved in [fragment_cache_std.patch](fragment_cache_std.patch)
for replay. It deliberately contains the cache experiment rather than an async
unit-fixture migration.

## Reproduction

From the source containing this benchmark and the Tokio cache. The experiment
patch uses zero-context hunks against the matching base; use `--unidiff-zero`:

```sh
ACTORDB_CACHE_LOCK_RESULTS=target/tokio-cache-timers.jsonl \
  cargo bench --locked -p actordb --bench fragment_locks -- --save-baseline tokio-cache

git apply --unidiff-zero actordb/benches/fragment_cache_std.patch
ACTORDB_CACHE_LOCK_RESULTS=target/std-cache-timers.jsonl \
  cargo bench --locked -p actordb --bench fragment_locks -- --baseline tokio-cache

git apply -R --unidiff-zero actordb/benches/fragment_cache_std.patch
```

For A/B/B/A runs, build and save both executables first, then alternate those
saved binaries. Criterion filters can select one case, for example
`fragment_locks/read75_t1_c24_100b`.

Local raw samples/estimates, timer samples, source snapshots, the measured
harness, executable hashes, logs and saved executables are under
`target/cache-lock-comparison/`. `results.json` includes both full and isolated
runs; `summary.json` contains the mean/range calculations. Criterion also retains
all eight uniquely named result sets under `target/criterion/fragment_locks/`.
The final harness uses Clippy's equivalent `is_multiple_of` spelling in untimed
request setup; its measured source snapshot is archived alongside the results.
