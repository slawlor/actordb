# actordb-query

<p align="center">
  <img src="https://raw.githubusercontent.com/slawlor/actordb/main/docs/assets/actordb-logo-draft.svg" alt="ActorDB distributed Ferris graph" width="360">
</p>

Portable query plans, a bounded SQL subset, and exact integer reduction for
[actordb](https://github.com/slawlor/actordb). This crate has no runtime or
storage dependencies. Database and client crates execute the plans.

```rust
use actordb_query::{parse_with_params, QueryValue};

let plan = parse_with_params(
    "SELECT key, PREFIX_BYTES(value, 32) FROM kv \
     WHERE STARTS_WITH(key, ?) AND frag IN (0, 2) \
     ORDER BY key ASC LIMIT 100",
    &[QueryValue::Text("user:".into())],
)?;
let portable_sql = plan.to_sql();
# Ok::<(), actordb_query::QueryError>(())
```

## Supported syntax

- One `SELECT ... FROM table_name` statement with an optional terminal
  semicolon. The relation name is required; `kv` is simply the conventional
  default table used by low-level examples.
- Columns: `key`, `value`, `frag`, `PREFIX_BYTES(value, N)`,
  `OCTET_LENGTH(value)`, and durable-record metadata `timestamp_ms`,
  `logical_counter`, `node_id`, `expires_at_ms`, and `is_tombstone`.
- Aggregates: `COUNT(*)`; `SUM`, `MIN`, and `MAX` of
  `OCTET_LENGTH(value)` or `CAST(value AS BIGINT)`.
- Conjunctive predicates: `key =`, `<`, `<=`, `>`, `>=` text;
  `STARTS_WITH(key, prefix)`; `frag = N`; `frag IN (N, ...)`.
- Durable metadata predicates compare `timestamp_ms`, `logical_counter`,
  `node_id`, `expires_at_ms`, and `is_tombstone`; TTL presence checks use
  `IS NULL`/`IS NOT NULL`. Add `INCLUDING TOMBSTONES` after `FROM table_name` to
  include retained tombstones.
- Prefixes can use text or byte literals, for example `X'e99b'`.
- Global row ordering is ascending by key. `ORDER BY key ASC` is optional.
  Row queries support `LIMIT N` and `OFFSET N`. `LIMIT 0` returns no rows.
- Positional `?` parameters bind typed values through `parse_with_params`.
  SQL keywords are case insensitive; string literals escape quotes as `''`.

Fragment scopes intersect when combined with `AND`; `frag IN ()` denotes an
empty scope. Scopes select the fragments to execute. Aggregates process every
qualifying row and reject pagination or ordering clauses. Joins, subqueries,
unions, grouping, aliases, comments, descending order, and user functions are
rejected.

## Values and completion

Keys are UTF-8 text; values are opaque bytes. Durable metadata describes the
ActorDB record envelope and does not impose a schema on application values.
BIGINT conversion accepts up to
20 ASCII decimal bytes with an optional sign, checked against the signed
64-bit range. Numeric aggregation uses checked signed 128-bit arithmetic.
`QueryValue::Integer` must use decimal strings on language-neutral wires to
preserve precision. Empty input returns COUNT zero and SUM/MIN/MAX null.

`QueryAccumulator` projects one aggregate row and can merge complete partial
rows without transmitting values. Executors must enforce one merge per input
range and explicit completion from every selected fragment. Omitted LIMIT
requests complete results; resource exhaustion must produce an error, never
silent truncation. Row projection leaves filtering, global ordering, and
pagination to the executor.

## Bounds and conformance

SQL is capped at 64 KiB and 4,096 tokens; plans have at most 32 projections and
32 predicates, 1,024 fragment IDs, and 16 KiB per key/prefix literal. Explicit
LIMIT/OFFSET cannot exceed 100,000. Grammar nesting is fixed and does not use
recursive expressions. Bound plans must fit the same canonical SQL byte cap.

Run `cargo test -p actordb-query`. Tests cover full syntax rejection, typed
binding, canonical round trips, Unicode and binary prefixes, fragment scope
intersection, strict conversion, empty aggregates, atomic merge errors, and
integer overflow. Portable implementations should preserve these semantics.
