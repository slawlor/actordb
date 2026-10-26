# Bounded table runtime

Named tables retain independent cache, TTL, tombstone and persistence state.
They use the existing actor dispatcher and managed priority workers; the
worker cache is bounded independently of the number of stored tables.

`NodeConfig::table_pool_capacity` defaults to **16 pool sets per fragment**,
including the built-in KV set. Each set contains the configured high/default/
background workers. Fixed base pools are additional. With default worker
counts, a fragment has at most `(16 + 1) * (2 + 4 + 2) = 136` workers.
The allowed capacity range is 1–4096 and startup validates it before binding.
The TOML provider accepts the same `table_pool_capacity` key. Direct `KvActor`
embedders configure `FragmentState::with_table_pool_capacity` before sharing it;
existing `KvArgs` construction remains unchanged.

Missing-table scalar/batch reads, queries and scans do not create named worker
sets or empty child caches. They execute through the existing bounded base
workers. KV is a fixed startup namespace rather than an arbitrary missing name.
An absence query pins a stable empty identity and detects a later first record.

A cold populated table may reuse space from the least recently used idle set.
Reclamation requires zero admitted, queued and active worker ownership across
all three priorities. Workers are closed and joined before a replacement set
starts. Stale supervision events cannot restart an evicted generation. Busy
sets are never evicted: when every cached set is busy, requests return explicit,
nonfatal backpressure. Replication retries instead of acknowledging a rejected
apply. Cold reopening has actor-startup cost.

Worker eviction never evicts records, deletion floors, TTL deadlines, pending
persistence or the data child's query incarnation. Stored data remains
dataset-sized and is outside this worker-cache limit. Worker-local metrics and
token buckets restart with a new pool incarnation; replacing one failed worker
does not reset the whole pool's rate state. Use a separate deployment-level
limiter for a hard persistent table/node QPS policy.

Cold writes have a separate bounded pending-empty namespace allowance using
the same capacity; fixed KV is exempt. Failed or cancelled cold work cannot
accumulate untracked empty children. A child is reclaimed only when no snapshot
or operation retains it, every record/index map is empty, persistence has no
pending/in-flight records, and no flush ordering fence is active. Retained
owners postpone reclamation, not its retry: unrelated cold admission and normal
snapshot/flush/expiry maintenance revisit only this bounded candidate set.
If retained candidates fill that allowance, new cold writes backpressure before
another child is allocated. This bounds empty metadata, not total process RSS.

Detailed observations can report a populated table's pool as not created after
idle eviction. That does not mean its stored records are absent; the next
admitted operation recreates the worker set over the same data state.
