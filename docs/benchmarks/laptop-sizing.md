# Laptop sizing calibration

Measured on a 16-core Apple M4 Max (12 performance + 4 efficiency cores), 64 GiB
RAM, macOS arm64, Rust 1.97, optimized builds. The probes use four runtime
threads and run sequentially in fresh processes. Raw data is in
[laptop-sizing](laptop-sizing/), with fitted JSON profiles in
[profiles](laptop-sizing/profiles/).

## Recommended starting estimates

For the measured 32-byte keys and 1 KiB values:

| Cost | Measured/fitted | Conservative profile |
|---|---:|---:|
| Fixed process/Node memory | 9.46 MiB | 18 MiB, including data-activation margin |
| Base fragment overhead | 41.5 KiB | 48 KiB |
| Each resident table worker set per fragment | 129.8 KiB | 144 KiB |
| Encoded key/index amplification | 9.09 × logical key bytes | 9 ×, inside 1.1 memory margin |
| Fixed resident metadata per record | 819 bytes | 832 bytes, inside 1.1 memory margin |
| Value allocation slope | 1.017 × value bytes | 1.1 memory margin |
| Embedded point read CPU | about 3–4 us | 10 us |
| Embedded point mutation CPU | about 6–13 us | 20 us |
| COUNT CPU per examined record | at most 0.23 us in these runs | 0.3 us workload override |
| General range/scan record cost | at most 4.44 us after empty-page cost | 6 us |
| Query coordination per touched fragment | about 35 us | 45 us |

The resident record formula is:

```text
1.1 × (value_bytes + 9 × logical_key_bytes + 832) bytes
```

This gives about **2.2 GiB per million live records per copy** at 32-byte keys
and 1 KiB values, before growth/skew allowances, tombstones, fragment/pool
overhead, replication backlog and durable-backend reserves. Longer 128-byte keys
increase this to about 3.08 GiB. The key factor accounts for namespace encoding
and multiple resident key/index copies; physical storage uses a separate factor.

The two 200,000-record holdouts were excluded from fitting. The conservative
profile predicted 478 MiB versus 427 MiB measured at 32-byte keys, and 660 MiB
versus 611 MiB at 128-byte keys: **12% and 8% over measured RAM**. These are sanity
checks on this allocator/software stack, not universal confidence bounds.

With the default base and KV worker sets, one empty fragment uses
`48 KiB + 2 × 144 KiB = 336 KiB` in the profile. Each extra resident named table
adds 144 KiB per fragment. A first absence-read probe did not instantiate worker
pools; four such raw samples are explicitly excluded. The corrected probe seeds
one empty live value in each named table/fragment before measuring.

## RPC costs

Loopback gRPC adds substantial work relative to direct embedded calls. With a
reused channel, 1 KiB values and 16 concurrent callers, observed throughput was
about 55,000 requests/s and combined client/server CPU was about 59–61 us per
request. Serial callers cost about 100–107 us and completed 11,000–13,000/s.
At 16 KiB, concurrent throughput was about 44,000–47,000/s.

`m4-max-grpc.json` uses 125 us reads and 135 us writes as conservative **combined
client/server CPU upper bounds**. They are not measurements of server-only CPU.
The transport is plaintext loopback; TLS, authentication, real NIC and WAN
throughput were not measured. To measure service cost at 16 concurrent streams,
the probe uses a 16 MiB per-request receive allowance and 512 MiB aggregate
receive reservation. The default 64 MiB/256 MiB receive settings rejected that
concurrency with `gRPC receive admission full`; these reservations are logical
limits, not eagerly allocated buffers. Production admission settings remain
unchanged.

## Durable writes and backend reserves

The probes exercise the offered RocksDB backend, real WAL synchronization and
local StoreCommitted/StableCommitted policies. They preserve the four-job storage
executor and test 16 callers on disjoint keys. Key sets are disjoint so LWW
supersession does not masquerade as an I/O failure.

| Backend/value size | Store completion/s, repeats | Stable completion/s, repeats | Profile limits |
|---|---|---|---|
| Sync WAL / 1 KiB | 25,751–37,654 | 28,187–28,990 | 25,000 store; 28,000 stable |
| Async WAL / 1 KiB | 63,241–78,079 | 31,676–33,195 | 63,000 store; 31,000 stable |
| Sync WAL / 16 KiB | 19,637–25,046 | 12,068–15,318 | 19,000 store; 12,000 stable |
| Async WAL / 16 KiB | 66,320–66,978 | 14,043–14,936 | 66,000 store; 14,000 stable |

Stable writes obey both store and stable limits. Rates are rounded down from the
lower repeat; the planner's 30% resource headroom applies afterwards. CPU time
alone misses time waiting for these fences. Equivalent committed-write demand
is local writes plus peer writes divided by the **observed mean** replicated
durability batch size. The default batch assumption is one; configured maximum
batch sizes do not imply full batches. Buffered MemoryApplied writes do not use
the immediate-fence limit.

The supplied RocksDB 11.8.1 public source confirms:

- `include/rocksdb/options.h`: default write buffer 64 MiB.
- `include/rocksdb/advanced_options.h`: two write buffers per column family.
- `include/rocksdb/table.h`: a 32 MiB internal block cache when no cache is supplied.
- `db/db_impl/db_impl_write.cc`, `DBImpl::GetMaxTotalWalSize`: default WAL-pressure
  threshold is four times the configured maximum memtable state.

ActorDB opens data and metadata column families with these defaults. The RocksDB
profiles reserve **320 MiB RAM** for two column families' potential buffers/caches
and **1 GiB disk** for WAL pressure, in addition to the fitted Node/data costs.
These are configured allowances, not measured steady resident usage or hard
maximums. Table readers, flush/compaction overlap and OS cache can add overhead.
The 100,000-record RocksDB run used about 312–314 MiB total process RSS; native
overhead was well below the reserved allowance at this workload.

Physical storage uses two times logical key bytes and 128 bytes of encoded
metadata, independently of resident indexes. The 2× storage/traffic amplification
factor remains an assumption for WAL/LSM overlap. Effective write bandwidth is
derived from unique-record durable ingestion, not advertised SSD bandwidth or
measured device-level write amplification. API fences on this laptop do not
constitute a power-loss durability test.

## Using the profiles

The templates reserve a 12-performance-core CPU budget, 32 GiB for the database
on this 64 GiB laptop, and 100 GiB disk. NIC throughput remains an explicit
1 Gbit/s assumption. Adjust these budgets for your deployment. Portable defaults
remain unchanged; M4 measurements do not establish cloud-host performance.

```sh
cargo run -p actordb-placement --features cli --bin actordb-plan -- \
  recommend workload.json \
  --profile docs/benchmarks/laptop-sizing/profiles/m4-max-grpc.json
```

Choose `m4-max-embedded.json` for colocated direct Node calls. RocksDB templates
name the WAL policy and tested value size, such as `m4-max-rocksdb-sync-1024.json`.
The `m4-max-grpc-rocksdb-...` templates compose the separate gRPC CPU upper
bound with backend measurements; they are not combined durable-RPC benchmarks.
Set `durable_storage = true` and the requested write durability in the workload.
For a matching count-only workload, `queries[].examined_record_cpu_us = 0.3`
uses the measured cheaper record cost; generic scans keep the conservative
profile cost. An override is specific to the measured hardware/query shape.

## Reproduce

```sh
cargo build --release --no-default-features -p actordb --features rocksdb \
  --example sizing_probe -j 4
python3 docs/benchmarks/probe_laptop.py --output measurements.json --repeats 2
cargo build --release -p actordb-client --example rpc_sizing_probe -j 4
python3 docs/benchmarks/probe_supplement.py extra --output extra.json
python3 docs/benchmarks/probe_supplement.py rpc --output rpc.json
python3 docs/benchmarks/probe_supplement.py holdout --output holdout.json
python3 docs/benchmarks/fit_laptop_profiles.py --measurements measurements.json \
  --extra extra.json --rpc rpc.json --output profiles
```

The matrix covers 1/8/32/128/256 fragments; 1/4/8 resident tables; 64 B, 1 KiB,
16 KiB values; 32/128-byte keys; 10,000–100,000 records; direct/RPC concurrency;
COUNT, range and prefix-transformed scans; and both WAL policies. Each case runs
alone. The 16 KiB storage CPU probes use a smaller hot set to separate value-size
cost from the configured persistence buffer's burst limit. Backpressure is
recorded as rejection, not silently counted as completed throughput.

The profile generator uses ordinary least squares plus conservative rounding
and margins. Run `fit_laptop_profiles.py --help` for input files. The raw matrix,
corrected table/empty-query measurements, RPC measurements, fit and holdouts are
retained alongside the profiles. Sustained load, larger datasets, other table
names, compaction history, actual network behavior and background scheduling can
change these numbers. The 128-fragment template limit remains an operating policy,
not a newly established software maximum.
