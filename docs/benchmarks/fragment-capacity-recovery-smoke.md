# Replica visibility and restart recovery smoke measurements

Recorded 2026-11-05 on Apple M4 Max / 64 GiB RAM / macOS arm64,
optimized build. Each case uses two real Nodes in a fresh process, loopback TCP,
2,000 records with 1,024-byte values, default pools and 20 samples per operation.
[Raw measurements and partial profile](fragment-capacity-recovery-smoke.json).

```sh
cargo run --release -p actordb --example fragment_capacity -- \
  --fragments 1,8,32,128 --records 2000 --samples 20
```

The idle baseline waits until initial empty reconciliation completes in both
peer directions. Visibility samples start after a source mutation applies and
poll every replica until the exact source version appears. Recovery restarts one
storeless replica with the same identity and waits for the expected record count
and a completed verified reconciliation round on every fragment. Representative
first/last records also match the source payload and version.

| Fragments/host | Visibility p50 | Visibility p99 | Replica recovery | Recovery process CPU |
|---|---:|---:|---:|---:|
| 1 | 99.99 ms | 101.78 ms | 47.03 ms | 0.060 s |
| 8 | 100.00 ms | 101.98 ms | 49.93 ms | 0.057 s |
| 32 | 100.25 ms | 102.77 ms | 94.68 ms | 0.109 s |
| 128 | 98.85 ms | 103.38 ms | 339.99 ms | 0.896 s |

Visibility follows the default 100 ms ordinary batching interval. Restart
recovery reconstructs the lost in-memory copy using existing anti-entropy;
recovery includes new Node startup, reconnection, data transfer and verification,
while old-node shutdown and disconnect retirement are excluded. CPU is aggregate
process CPU across the source and replacement, including runtime work, not elapsed
wall time or a dedicated-core estimate.

Twenty samples make p99 close to the maximum sample. These are small local smoke
measurements, not production throughput, a universal fragment ceiling, disk
recovery or a region-failure simulation. The exported partial profile retains
conservative default CPU floors when shorter/noisier samples fall below them.
Disk/stable-write costs and host resource budgets remain assumptions.
