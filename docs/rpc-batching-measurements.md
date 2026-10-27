# RPC batching and reply-sizing measurements

The application service now uses bounded actor/peer read pages and independent-key
write waves. Read replies reserve node-wide memory through actor cancellation and
reply encoding. Repeated writes to one key remain sequential with independent
exact outcomes and TTL stamps. Reply accounting sizes only newly inserted results.

## Reproduction

```sh
cargo run -p actordb --no-default-features --release --example rpc_batching_benchmark -- after
```

The identical benchmark example was copied into baseline
`3af08d84cac9ded99b85bf1fb4170590b5dd64cb` and run with label `before`.
Measurements used Apple M4 Max, aarch64, stable Rust, four Tokio threads and one
real TCP peer, with one fragment and fixed 16-byte values. This measures the
framework-neutral RPC service and peer transport, not additional HTTP/2 or TLS
cost. Each operation had one untimed warmup and three measured trials; request
construction, dataset setup and shutdown were outside timing. Every trial checked
all result positions and values/errors. Background rate limiting was disabled
for the fixture; production priorities, admission and byte limits are unchanged.

Other builds, including Docker compilation, were not globally suspended. These
are local elapsed-time samples, not throughput/SLO or statistical-confidence claims.

## Median elapsed microseconds

| Rows | Operation | Before µs | After µs | Before/after |
| --- | --- | ---: | ---: | ---: |
| 64 | Remote read | 5753 | 277 | 20.77× |
| 64 | Remote write | 3405 | 363 | 9.38× |
| 64 | Local read | 192 | 135 | 1.42× |
| 256 | Remote read | 13493 | 628 | 21.49× |
| 256 | Remote write | 13530 | 1219 | 11.10× |
| 256 | Local read | 810 | 485 | 1.67× |
| 1024 | Remote read | 54083 | 2608 | 20.74× |
| 1024 | Remote write | 57904 | 4523 | 12.80× |
| 1024 | Local read | 4904 | 1832 | 2.68× |
| 4096 | Remote read | 234022 | 10220 | 22.90× |
| 4096 | Remote write | 214470 | 17698 | 12.12× |
| 4096 | Local read | 39760 | 7318 | 5.43× |

## Peer frame and sizing work

Successful socket-write polls were two per peer frame in this fixture: one
length prefix and one body. At 64 rows, remote reads and writes dropped from
128 polls (64 frames) to two polls (one frame). At 4096 rows, reads dropped from
8192 polls to 32 (16 request pages); writes dropped to 64 (32 independent-key
waves). The latter wave size also bounds local concurrency and request storage.

The sizing regression wraps 4096 results with an encoded-length counter and
completes them in a sparse permutation. The new accumulator invokes each result's
encoded-length method exactly once (4096 calls), then reconstructs the original
order. The previous append loop visited approximately 8.4 million result positions.
Property checks compare accumulated bytes with actual encoded read/mutation
responses and verify the exact 16 MiB boundary without storing an overflowing row.

Wide-value local/remote tests verify complete resource-limit errors rather than
partial replies or full-dataset materialization. Cancellation tests retain page
memory and Node admission until the actor relinquishes ownership; accepted
mutation waves continue after their caller disappears. These are safeguards,
not assertions that the memory allowance equals process RSS.
