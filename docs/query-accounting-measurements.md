# Incremental query-buffer accounting measurements

The query coordinator maintains per-cursor row-storage totals and one aggregate
cursor/SQL total. Page adoption measures incoming rows once; consumption removes
only the consumed row's charge. Refill admission runs only when a cursor needs a
page. The conservative accounting still includes cursor metadata, continuation
String capacity, deque backing capacity, projected cell/payload capacities, the
heap's duplicated head key, and separate heap-header reservations. Aggregate page
draining and sparse continuations update the same total. Result limits, page
validation, cancellation ownership, and final revision/incarnation checks remain
unchanged.

A consumed row can expose a longer next key, increasing the conservative
heap-copy charge. Consumption therefore also performs an O(1) buffer-limit
check before that next head is cloned, even when no new page is needed. The
after trials below include this check; no all-cursor or buffered-row walk is
restored.

## Reproduction

Run the same fixture on the baseline and incremental implementations:

```sh
cargo run --release -p actordb --example query_accounting_benchmark -- before
cargo run --release -p actordb --example query_accounting_benchmark -- after
```

The baseline was commit `422721a060700241f9c90f425abb295284097cdc` with the
identical new benchmark example copied into that checkout; the after implementation
is the commit introducing this document. Measurements were taken on 2026-10-26,
Apple M4 Max, aarch64-apple-darwin, rustc 1.97.0, using four Tokio worker threads.
Both used the same release target directory, fixed buffered in-memory store,
current physical KV table encoding, 8,192/16,384 resident records with one-byte
values, and repeated key projections of width 1/8/32. Node startup and admission
lease settling were outside timing. Each shape had one unreported warmup followed
by three measured complete queries, including final identity verification. Every
query asserted its exact row count and projection width.

These are local elapsed-time samples, not throughput/SLO or RSS claims. Other
workspace builds were not globally disabled, and three trials are not a statistical
confidence interval. Wide projections still incur legitimate materialization and
validation work. The existing Criterion selectivity fixture now also uses current
physical table encoding and asserts expected results before timing, preventing
accidental benchmarks of empty tables.

## Median elapsed microseconds

| Rows | Fragments | Columns | Before µs | After µs | Before/after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8192 | 1 | 1 | 12000 | 8578 | 1.40× |
| 8192 | 1 | 8 | 57853 | 32105 | 1.80× |
| 8192 | 1 | 32 | 162689 | 120975 | 1.34× |
| 8192 | 8 | 1 | 28771 | 8836 | 3.26× |
| 8192 | 8 | 8 | 142429 | 32476 | 4.39× |
| 8192 | 8 | 32 | 359899 | 121534 | 2.96× |
| 8192 | 32 | 1 | 90061 | 9011 | 9.99× |
| 8192 | 32 | 8 | 438500 | 32058 | 13.68× |
| 8192 | 32 | 32 | 1266817 | 119562 | 10.60× |
| 8192 | 64 | 1 | 105474 | 7833 | 13.47× |
| 8192 | 64 | 8 | 452354 | 25208 | 17.94× |
| 8192 | 64 | 32 | 1170056 | 90767 | 12.89× |
| 16384 | 1 | 1 | 24287 | 18122 | 1.34× |
| 16384 | 1 | 8 | 103313 | 65877 | 1.57× |
| 16384 | 1 | 32 | 340421 | 260089 | 1.31× |
| 16384 | 8 | 1 | 59240 | 18379 | 3.22× |
| 16384 | 8 | 8 | 291975 | 65174 | 4.48× |
| 16384 | 8 | 32 | 753865 | 278564 | 2.71× |
| 16384 | 32 | 1 | 200847 | 27395 | 7.33× |
| 16384 | 32 | 8 | 898379 | 69703 | 12.89× |
| 16384 | 32 | 32 | 2170238 | 252081 | 8.61× |
| 16384 | 64 | 1 | 350307 | 19374 | 18.08× |
| 16384 | 64 | 8 | 1692815 | 65962 | 25.66× |
| 16384 | 64 | 32 | 4373514 | 254449 | 17.19× |

## Raw measured trials

```csv
label,rows,fragments,columns,trial,micros
before,8192,1,1,1,11769
before,8192,1,1,2,12000
before,8192,1,1,3,12032
before,8192,1,8,1,58541
before,8192,1,8,2,57853
before,8192,1,8,3,56529
before,8192,1,32,1,171384
before,8192,1,32,2,162689
before,8192,1,32,3,160808
before,8192,8,1,1,28771
before,8192,8,1,2,28444
before,8192,8,1,3,29207
before,8192,8,8,1,141682
before,8192,8,8,2,142429
before,8192,8,8,3,142976
before,8192,8,32,1,360431
before,8192,8,32,2,359899
before,8192,8,32,3,359354
before,8192,32,1,1,90061
before,8192,32,1,2,90876
before,8192,32,1,3,89808
before,8192,32,8,1,438500
before,8192,32,8,2,438125
before,8192,32,8,3,446293
before,8192,32,32,1,1266817
before,8192,32,32,2,1506263
before,8192,32,32,3,1186342
before,8192,64,1,1,106742
before,8192,64,1,2,105474
before,8192,64,1,3,105277
before,8192,64,8,1,452354
before,8192,64,8,2,452695
before,8192,64,8,3,450085
before,8192,64,32,1,1170056
before,8192,64,32,2,1211706
before,8192,64,32,3,1110816
before,16384,1,1,1,23984
before,16384,1,1,2,24287
before,16384,1,1,3,24652
before,16384,1,8,1,103313
before,16384,1,8,2,101982
before,16384,1,8,3,103589
before,16384,1,32,1,340421
before,16384,1,32,2,337696
before,16384,1,32,3,342493
before,16384,8,1,1,57740
before,16384,8,1,2,60025
before,16384,8,1,3,59240
before,16384,8,8,1,291975
before,16384,8,8,2,294544
before,16384,8,8,3,291847
before,16384,8,32,1,753865
before,16384,8,32,2,770141
before,16384,8,32,3,750960
before,16384,32,1,1,200065
before,16384,32,1,2,200847
before,16384,32,1,3,204372
before,16384,32,8,1,906982
before,16384,32,8,2,898379
before,16384,32,8,3,897148
before,16384,32,32,1,2170238
before,16384,32,32,2,2179077
before,16384,32,32,3,2158530
before,16384,64,1,1,347173
before,16384,64,1,2,350307
before,16384,64,1,3,351594
before,16384,64,8,1,1707020
before,16384,64,8,2,1692815
before,16384,64,8,3,1681061
before,16384,64,32,1,4343378
before,16384,64,32,2,4373514
before,16384,64,32,3,4457584
after,8192,1,1,1,9159
after,8192,1,1,2,8578
after,8192,1,1,3,8230
after,8192,1,8,1,32021
after,8192,1,8,2,32874
after,8192,1,8,3,32105
after,8192,1,32,1,122120
after,8192,1,32,2,120975
after,8192,1,32,3,120156
after,8192,8,1,1,8836
after,8192,8,1,2,8675
after,8192,8,1,3,9553
after,8192,8,8,1,32074
after,8192,8,8,2,32476
after,8192,8,8,3,32868
after,8192,8,32,1,121534
after,8192,8,32,2,121818
after,8192,8,32,3,120734
after,8192,32,1,1,9127
after,8192,32,1,2,9011
after,8192,32,1,3,8963
after,8192,32,8,1,31641
after,8192,32,8,2,32058
after,8192,32,8,3,32218
after,8192,32,32,1,119682
after,8192,32,32,2,119562
after,8192,32,32,3,117595
after,8192,64,1,1,7688
after,8192,64,1,2,7833
after,8192,64,1,3,8012
after,8192,64,8,1,25065
after,8192,64,8,2,25208
after,8192,64,8,3,25325
after,8192,64,32,1,89038
after,8192,64,32,2,91289
after,8192,64,32,3,90767
after,16384,1,1,1,18022
after,16384,1,1,2,18181
after,16384,1,1,3,18122
after,16384,1,8,1,65824
after,16384,1,8,2,65877
after,16384,1,8,3,65893
after,16384,1,32,1,260831
after,16384,1,32,2,257719
after,16384,1,32,3,260089
after,16384,8,1,1,18032
after,16384,8,1,2,18696
after,16384,8,1,3,18379
after,16384,8,8,1,64706
after,16384,8,8,2,65174
after,16384,8,8,3,65499
after,16384,8,32,1,254633
after,16384,8,32,2,278564
after,16384,8,32,3,334034
after,16384,32,1,1,34185
after,16384,32,1,2,27395
after,16384,32,1,3,24065
after,16384,32,8,1,76248
after,16384,32,8,2,69703
after,16384,32,8,3,66664
after,16384,32,32,1,258178
after,16384,32,32,2,252081
after,16384,32,32,3,248227
after,16384,64,1,1,19496
after,16384,64,1,2,18763
after,16384,64,1,3,19374
after,16384,64,8,1,65962
after,16384,64,8,2,65819
after,16384,64,8,3,66424
after,16384,64,32,1,248481
after,16384,64,32,2,254735
after,16384,64,32,3,254449
```

