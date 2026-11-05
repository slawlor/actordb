# Fragment capacity smoke measurements

Recorded 2026-11-05 on an Apple M4 Max / 64 GiB RAM / macOS arm64,
optimized build. Raw measurements and the partial calibrated profile are in
[fragment-capacity-smoke.json](fragment-capacity-smoke.json).

```sh
cargo run --release -p actordb --example fragment_capacity -- \
  --fragments 1,8,32,128 --records 2000 --samples 20
```

Two real Nodes per fresh process, loopback TCP, default pools, one KV table,
2,000 records per copy, 1,024-byte values and no storage backend. Memory below
is **whole process idle RSS divided by two**, including shared runtime and
process overhead. CPU is aggregate process idle CPU divided by two.

| Fragments/host | Worker/maintenance actors/host | Idle RSS/host | Idle CPU cores/host | Aggregate latency | Read p99 | Mixed read p99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 20 | 3.80 MiB | 0.0008 | 1.64 ms | 69.9 us | 819 us |
| 8 | 160 | 6.16 MiB | 0.0016 | 1.60 ms | 67.9 us | 393 us |
| 32 | 640 | 14.05 MiB | 0.0153 | 2.96 ms | 33.8 us | 163 us |
| 128 | 2,560 | 44.95 MiB | 0.2073 | 6.08 ms | 28.8 us | 364 us |

Each host also runs one sender task per fragment in this two-copy configuration.
Loaded RSS adds about 6.6–7.4 MiB per host across these cases. This small dataset
leaves fixed runtime overhead unusually visible; larger datasets need independent
measurement. Increasing from 32 to 128 fragments adds about 31 MiB idle RSS per
host and raises timer/maintenance CPU and aggregate coordination work.

Measured writes await every replica ACK. Their roughly 100 ms latency follows
the default 100 ms batch timer; the resulting roughly 10 writes/s from one serial
caller is not an overall throughput limit. The mixed phase joins point reads,
writes and an aggregate concurrently. Three aggregates failed under concurrent
mutation in the one-fragment case; all other mixed aggregates completed. Query
revision checks can reject a query when its fragment changes between pages.

Twenty samples make p99 effectively a maximum observed sample. Idle CPU is noisy
and subtracting its baseline can exceed short operation CPU measurements; the
partial profile uses conservative upper envelopes across cases. It does not
derive a universal safe host ceiling, a sustained-QPS SLO or disk/stable-write
capacity. The default planner ceiling of 128 fragments/host remains an editable
policy assumption. Calibrate with larger samples, the actual tables/pools,
workload, persistence and recovery requirements before choosing a deployment limit.
