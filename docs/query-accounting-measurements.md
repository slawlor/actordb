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
| 8192 | 1 | 1 | 12000 | 9218 | 1.30× |
| 8192 | 1 | 8 | 57853 | 34889 | 1.66× |
| 8192 | 1 | 32 | 162689 | 128193 | 1.27× |
| 8192 | 8 | 1 | 28771 | 9368 | 3.07× |
| 8192 | 8 | 8 | 142429 | 32689 | 4.36× |
| 8192 | 8 | 32 | 359899 | 123247 | 2.92× |
| 8192 | 32 | 1 | 90061 | 9712 | 9.27× |
| 8192 | 32 | 8 | 438500 | 34230 | 12.81× |
| 8192 | 32 | 32 | 1266817 | 124230 | 10.20× |
| 8192 | 64 | 1 | 105474 | 8145 | 12.95× |
| 8192 | 64 | 8 | 452354 | 26064 | 17.36× |
| 8192 | 64 | 32 | 1170056 | 91323 | 12.81× |
| 16384 | 1 | 1 | 24287 | 18435 | 1.32× |
| 16384 | 1 | 8 | 103313 | 67325 | 1.53× |
| 16384 | 1 | 32 | 340421 | 265419 | 1.28× |
| 16384 | 8 | 1 | 59240 | 19640 | 3.02× |
| 16384 | 8 | 8 | 291975 | 67928 | 4.30× |
| 16384 | 8 | 32 | 753865 | 265987 | 2.83× |
| 16384 | 32 | 1 | 200847 | 19107 | 10.51× |
| 16384 | 32 | 8 | 898379 | 68036 | 13.20× |
| 16384 | 32 | 32 | 2170238 | 261289 | 8.31× |
| 16384 | 64 | 1 | 350307 | 19339 | 18.11× |
| 16384 | 64 | 8 | 1692815 | 68383 | 24.75× |
| 16384 | 64 | 32 | 4373514 | 262059 | 16.69× |

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
after,8192,1,1,1,10048
after,8192,1,1,2,9002
after,8192,1,1,3,9218
after,8192,1,8,1,35113
after,8192,1,8,2,34889
after,8192,1,8,3,33482
after,8192,1,32,1,125891
after,8192,1,32,2,131954
after,8192,1,32,3,128193
after,8192,8,1,1,9368
after,8192,8,1,2,9219
after,8192,8,1,3,9700
after,8192,8,8,1,31662
after,8192,8,8,2,32929
after,8192,8,8,3,32689
after,8192,8,32,1,125096
after,8192,8,32,2,123247
after,8192,8,32,3,122759
after,8192,32,1,1,10301
after,8192,32,1,2,9712
after,8192,32,1,3,9544
after,8192,32,8,1,34230
after,8192,32,8,2,33064
after,8192,32,8,3,34274
after,8192,32,32,1,124230
after,8192,32,32,2,120784
after,8192,32,32,3,124376
after,8192,64,1,1,8145
after,8192,64,1,2,8133
after,8192,64,1,3,8408
after,8192,64,8,1,26064
after,8192,64,8,2,26090
after,8192,64,8,3,25801
after,8192,64,32,1,92169
after,8192,64,32,2,89547
after,8192,64,32,3,91323
after,16384,1,1,1,18635
after,16384,1,1,2,18373
after,16384,1,1,3,18435
after,16384,1,8,1,67175
after,16384,1,8,2,67325
after,16384,1,8,3,67399
after,16384,1,32,1,265419
after,16384,1,32,2,268106
after,16384,1,32,3,263795
after,16384,8,1,1,19640
after,16384,8,1,2,20738
after,16384,8,1,3,19493
after,16384,8,8,1,69102
after,16384,8,8,2,67724
after,16384,8,8,3,67928
after,16384,8,32,1,260199
after,16384,8,32,2,265987
after,16384,8,32,3,267925
after,16384,32,1,1,19107
after,16384,32,1,2,18548
after,16384,32,1,3,19374
after,16384,32,8,1,68460
after,16384,32,8,2,68036
after,16384,32,8,3,67418
after,16384,32,32,1,261289
after,16384,32,32,2,260600
after,16384,32,32,3,262009
after,16384,64,1,1,19198
after,16384,64,1,2,19745
after,16384,64,1,3,19339
after,16384,64,8,1,69387
after,16384,64,8,2,67575
after,16384,64,8,3,68383
after,16384,64,32,1,262059
after,16384,64,32,2,257722
after,16384,64,32,3,263879
```

