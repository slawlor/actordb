# SQL queries

ActorDB queries use a virtual key/value table with UTF-8 `key`, opaque byte
`value`, numeric `frag`, and durable-record metadata columns. `FROM kv` selects
the legacy unnamed keyspace; named logical tables use `FROM table_name`. No
application schema registration or secondary index is required. Queries read
visible records: deletes and expired values are hidden.

```sql
SELECT key, PREFIX_BYTES(value, 32) FROM kv
WHERE STARTS_WITH(key, 'user:') ORDER BY key ASC LIMIT 100;
SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM kv WHERE frag IN (0, 3);
SELECT SUM(CAST(value AS BIGINT)) FROM kv WHERE frag = 2;
SELECT key, timestamp_ms, node_id FROM kv INCLUDING TOMBSTONES
WHERE is_tombstone = 1;
SELECT key, value FROM users WHERE STARTS_WITH(key, 'user:');
```

## Supported subset

Use one `SELECT` statement with:

- Projections: `key`, `value`, `frag`, `PREFIX_BYTES(value, n)`, and
  `OCTET_LENGTH(value)`, plus `timestamp_ms`, `logical_counter`, `node_id`,
  `expires_at_ms`, and `is_tombstone` from ActorDB's durable record envelope.
- Predicates: key comparisons (`=`, `<`, `<=`, `>`, `>=`),
  `STARTS_WITH(key, prefix)`, and `frag = n` or
  `frag IN (...)`, plus durable metadata comparisons on
  `timestamp_ms`, `logical_counter`, `node_id`, `expires_at_ms`, and
  `is_tombstone`, combined with `AND`.
- Visibility modifier: `INCLUDING TOMBSTONES` includes retained tombstone
  records. Without it, tombstones remain hidden.
- Row pagination: `ORDER BY key ASC`, `LIMIT`, and `OFFSET`.
- Aggregates: `COUNT(*)` and `SUM`, `MIN`, or `MAX` over
  `OCTET_LENGTH(value)` or `CAST(value AS BIGINT)`.

Joins, subqueries, unions, multiple statements, grouping, and user-defined
functions are unsupported. Named tables are co-partitioned by their user key,
which is the storage prerequisite for a future local key-join implementation,
but SQL join planning is not enabled yet. Aggregate and ordinary row projections cannot be
mixed. Aggregate queries reject row pagination clauses. `LIMIT 0` returns
zero rows, unlike the legacy scan default-limit convention.

Numeric conversion is explicit. `CAST(value AS BIGINT)` accepts signed decimal
64-bit integers; invalid bytes fail the query. Aggregate arithmetic uses
checked wide integers. Empty `COUNT(*)` is zero; empty `SUM`, `MIN`, and `MAX`
are null. Query cells preserve text, bytes, integer, and null types.

Durable metadata is returned without decoding the opaque application payload.
`timestamp_ms`, `logical_counter`, and `expires_at_ms` are unsigned integer
values; `expires_at_ms` is `NULL` when no TTL is set. `node_id` is text and
`is_tombstone` is an integer flag (`0` or `1`). Without
`INCLUDING TOMBSTONES`, `is_tombstone` is always `0`; with the modifier it can
identify retained tombstones.

Metadata comparisons use typed literals: timestamp, logical-counter, and TTL
comparisons require unsigned integers; `node_id` requires text; and
`is_tombstone` accepts `0` or `1`. `expires_at_ms IS NULL` and
`expires_at_ms IS NOT NULL` test whether a TTL is present. Metadata predicates
do not create a secondary index, so they scan the selected key ranges.

## Fragment filtering and unindexed work

`WHERE frag IN (...)` selects those fragments before execution. Unavailable
fragments outside that scope do not fail the query. IDs must be in
`0..fragment_count`. Key equality automatically selects its hash fragment;
explicit fragment restrictions are intersected with that fragment.

Key and prefix predicates filter an ordered traversal; the current executor
does not seek directly to a prefix range. Sparse predicates can still consume
the full examined-record budget. Fragment scopes prune traversal and row limits
can stop it early; predicates reduce returned/projected data. There is no
secondary value index or inferred JSON/relational schema for byte values.

## Parameters

Bind positional `?` parameters as typed values:

```rust,ignore
use actordb_query::QueryValue;

let result = node.query_with_params(
    "SELECT key FROM kv WHERE STARTS_WITH(key, ?) AND frag = ? LIMIT 20",
    &[QueryValue::Text("user:".into()), QueryValue::Integer(2)],
).await?;
```

Parameters are data, not interpolated SQL. Wrong types, missing or unused
parameters, and unsupported syntax produce errors. The SDK exposes the same
SQL and typed parameter surface.

## Completion, consistency, and budgets

Rows are ordered by key across fragments; offset and limit apply once to the
global order. Each fragment contributes from one holder, avoiding replica
double counting. Aggregates traverse complete qualifying input rather than
reducing the legacy scan API's capped vectors.

Execution uses byte/work-bounded background pages, exclusive key cursors,
pinned TTL visibility, and local revision/incarnation checks. Changes or
holder loss after traversal begins fail explicitly. There is no global
transactional snapshot; results describe per-fragment views under ActorDB's
eventual consistency contract.

`QueryOptions` controls deadline, result bytes, coordinator buffering,
examined records, page bytes, and page work. Resource exhaustion fails the
query rather than silently truncating aggregates. Background scheduling keeps
worker capacity separate; shared fragment locks still cause contention while
a page executes.

Use `Node::query_plan_with_options` for explicit budgets. Keep `Node::scan`
for its existing per-fragment limit/offset semantics; SQL has its own global
pagination and completion contract.
