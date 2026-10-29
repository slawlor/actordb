# Fragment write critical sections

Measured locally on 2026-10-29 against main commit
`ff5fc80a21f82be22f5b6a23a55d8a37fb0aa0cc`.

## Changes

Workers transfer their owned message value into the cache. Borrowed
`FragmentState::apply` calls copy their candidate before acquiring mutation
locks. Buffered writes prepare their persistence copy and summary metadata
before acquiring the flush-buffer mutex and cache write lock. Replaced cache
and pending payloads are destroyed after those guards are released.

The buffered winner check now compares LWW versions directly; a winning version
already proves inequality, so comparing full payloads first was redundant.
TTL selection, version-conflict checks, persistence admission, ordered enqueue,
cache mutation and revision publication remain together under their existing
locks. Expiry markers are prepared only for writes with a TTL.

## Workload and method

Apple M4 Max (arm64, 16 cores), Rust 1.97.0, optimized bench profile, default
crate features, the same generated Cargo.lock for all runs. Four Tokio runtime
threads; 20 Criterion samples per case, one second warmup, three seconds
measurement. Rates use Criterion's slope estimate when available, otherwise
its mean estimate; throughput change is `after / before - 1`.

One fragment dispatcher shares one cache among three priority pools with four
workers each. Every pool has an unlimited token bucket and a 1,024-job cap.
Depth 24 uses eight writes per priority, with independent fixed keys distributed
across each pool's four workers. Depth 1 uses Default priority. Every operation
must return Applied, and final cache versions and payload lengths are checked.

Input payload allocation is excluded. Dispatcher admission, actor execution,
payload hashing, cache insertion/replacement and persistence enqueue are timed.
There is no network in this fixture. Buffered mode uses a no-op store and defers
all flushing to teardown: these figures measure cache/persistence enqueue
throughput, not disk throughput or crash durability. Fixed keys keep resident
memory bounded. Selected benchmark filters can skip unmeasured fixtures.

The baseline executable was saved before changing the implementation. Runs
were sequential; tests and builds did not run concurrently with measurements.

## Final comparison

The large-value concurrent cases improved by 59.6% without persistence and
55.7% with buffered persistence. Smaller-value and single-writer cases were
mixed across runs; these measurements do not establish a universal improvement.

| Mode | Concurrent writes | Value | Before writes/sec | After writes/sec | Change |
|---|---:|---:|---:|---:|---:|
| storeless | 1 | 100 B | 102,931 | 101,005 | -1.9% |
| storeless | 1 | 10 KB | 69,632 | 57,020 | -18.1% |
| storeless | 1 | 1 MB | 2,150 | 2,223 | +3.4% |
| storeless | 24 | 100 B | 483,549 | 499,764 | +3.4% |
| storeless | 24 | 10 KB | 196,713 | 221,644 | +12.7% |
| storeless | 24 | 1 MB | 4,900 | 7,821 | +59.6% |
| buffered | 1 | 100 B | 98,380 | 98,444 | +0.1% |
| buffered | 1 | 10 KB | 77,690 | 79,167 | +1.9% |
| buffered | 1 | 1 MB | 2,328 | 2,392 | +2.7% |
| buffered | 24 | 100 B | 465,806 | 446,973 | -4.0% |
| buffered | 24 | 10 KB | 202,087 | 221,357 | +9.5% |
| buffered | 24 | 1 MB | 4,496 | 6,999 | +55.7% |

## Initial comparison

This first candidate prepared an expiry marker for every write. The final
candidate avoids that work for writes without TTLs. The baseline was also rerun
because several cases varied noticeably between sessions.

| Mode | Concurrent writes | Value | Before writes/sec | After writes/sec | Change |
|---|---:|---:|---:|---:|---:|
| storeless | 1 | 100 B | 105,018 | 100,111 | -4.7% |
| storeless | 1 | 10 KB | 81,609 | 80,646 | -1.2% |
| storeless | 1 | 1 MB | 2,516 | 2,598 | +3.2% |
| storeless | 24 | 100 B | 513,705 | 510,705 | -0.6% |
| storeless | 24 | 10 KB | 231,274 | 254,974 | +10.2% |
| storeless | 24 | 1 MB | 5,078 | 8,282 | +63.1% |
| buffered | 1 | 100 B | 101,184 | 96,594 | -4.5% |
| buffered | 1 | 10 KB | 62,489 | 76,872 | +23.0% |
| buffered | 1 | 1 MB | 2,471 | 2,155 | -12.8% |
| buffered | 24 | 100 B | 410,512 | 454,540 | +10.7% |
| buffered | 24 | 10 KB | 203,637 | 200,021 | -1.8% |
| buffered | 24 | 1 MB | 4,603 | 7,866 | +70.9% |

Within-run confidence intervals do not account for variation between sessions.
For example, the unchanged storeless single-writer 10 KB baseline ranged from
69.6k to 81.6k writes/sec across the two runs. The large concurrent gains appear
in both comparisons; smaller deltas should be interpreted with that variation
in mind. The first buffered single-writer 1 MB comparison regressed; the final
comparison slightly improved, so no stable speedup is claimed for that case.

## Correctness and remaining costs

`cargo test --locked -p actordb --lib --test cluster --test version_conflicts
--test tables` passed 613 tests, with one existing ignored library diagnostic.
This includes TTL crossing a cache-lock wait, conflicting versions, persistence
resize rollback, shared admission, cross-priority requests, replication and table
isolation. Formatting and whitespace checks also passed.

Copies prepared for buffered admission are transient per active worker;
duplicate, losing or rejected writes can now copy a payload that is never
persisted. That workload is not measured here. Cache writes still serialize,
and read payload cloning and query-page processing still hold cache read locks.

## Reproduction and artifacts

Add the benchmark to the original implementation, then save a baseline:

```sh
cargo bench --locked -p actordb --bench fragment_writes -- --save-baseline before-critical-sections
```

After applying the change, compare with that baseline:

```sh
cargo bench --locked -p actordb --bench fragment_writes -- --baseline before-critical-sections
```

The confirmation runs used separately saved `before-confirm` and `after-confirm`
baselines; the tables above calculate throughput changes from their estimates.

Local raw estimates, samples, the generated lockfile, source snapshots and saved
benchmark executables are in `target/critical-section-benchmark/`. Criterion
also retains all four named result sets under `target/criterion/fragment_writes_*`.
These generated artifacts are ignored by Git.
