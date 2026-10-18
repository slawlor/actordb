# SQL queries

ActorDB queries use one virtual table, `kv`, with UTF-8 `key`, opaque byte
`value`, and numeric `frag` columns. No schema registration or secondary
index is required. Queries read visible records: deletes and expired values
are hidden.

```sql
SELECT key, PREFIX_BYTES(value, 32) FROM kv
WHERE STARTS_WITH(key, 'user:') ORDER BY key ASC LIMIT 100;
SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM kv WHERE frag IN (0, 3);
SELECT SUM(CAST(value AS BIGINT)) FROM kv WHERE frag = 2;
```

## Supported subset

Use one `SELECT` statement with:

- Projections: `key`, `value`, `frag`, `PREFIX_BYTES(value, n)`, and
  `OCTET_LENGTH(value)`.
- Predicates: key comparisons (`=`, `<`, `<=`, `>`, `>=`),
  `STARTS_WITH(key, prefix)`, and `frag = n` or
  `frag IN (...)`, combined with `AND`.
- Row pagination: `ORDER BY key ASC`, `LIMIT`, and `OFFSET`.
- Aggregates: `COUNT(*)` and `SUM`, `MIN`, or `MAX` over
  `OCTET_LENGTH(value)` or `CAST(value AS BIGINT)`.

Joins, subqueries, unions, multiple statements, grouping, and user-defined
functions are unsupported. Aggregate and ordinary row projections cannot be
mixed. Aggregate queries reject row pagination clauses. `LIMIT 0` returns
zero rows, unlike the legacy scan default-limit convention.

Numeric conversion is explicit. `CAST(value AS BIGINT)` accepts signed decimal
64-bit integers; invalid bytes fail the query. Aggregate arithmetic uses
checked wide integers. Empty `COUNT(*)` is zero; empty `SUM`, `MIN`, and `MAX`
are null. Query cells preserve text, bytes, integer, and null types.

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
