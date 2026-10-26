# ActorDB Rust client

<p align="center">
  <img src="https://raw.githubusercontent.com/slawlor/actordb/main/docs/assets/actordb-logo-draft.svg" alt="ActorDB distributed Ferris graph" width="360">
</p>

Connect to the **application listener**, using `Client::connect(address,
ClientOptions::default())`. A server explicitly binds
`actordb::client_server::ClientServer` around an existing `Arc<Node>`.

Use `actordb-client = "0.10.0"` with a current application listener. The SDK
uses exact application protocol 4, without version-range or capability
negotiation. Construct `ClientOptions` and `PutOptions` with their Bon builders,
or start with `Default` and modify public fields; these structs are
`non_exhaustive`. See the [option guide](../docs/client-options.md).

Run the complete TCP and SQL example with
`cargo run -p actordb-client --example query_demo`. It starts a local listener,
writes records through the SDK, queries selected fragments, and reduces all
fragments. For the SQL grammar and budgets, see [the query guide](../docs/query.md).

The client supports point and ordered batch reads/writes, delete tombstones,
TTL and priority options, value version/expiry metadata, bounded distributed
scans, SQL queries with typed positional parameters, and local node health and
placement inspection. SQL is the ActorDB key/value dialect, not arbitrary SQL.
`ScanOptions` and `ScanMatch` are reexported for callers using the scan API.
Set `ClientOptions::table` (or use the Bon builder) to bind requests to a
named logical table. Table clients reuse the same application listener and
isolate keys and SQL results by table.
Inspection describes the contacted node; it is not a readiness or durability
check. Batches are individual operations, not transactions.

Clones share one connection and serialize exchanges, decoding and conversion. Count and encoded-byte
admission are nonwaiting and return `ClientError::Overloaded` at capacity. The
request deadline includes waiting for the connection. For parallel connections,
create several clients. Incoming frames retain their fixed 16 MiB wire cap.
Before decoding, aggregate headers, cells, map entries, payloads and nested
write-concern evidence must also fit `ClientOptions::max_decoded_response_bytes`
(64 MiB by default). This stricter default can reject formerly accepted wide
responses. For a trusted workload needing wider supported results, opt in with
`.max_decoded_response_bytes(512 * 1024 * 1024)`; increasing that allowance does
not increase the wire limit. Exceeding decoded admission retires the connection
with `Protocol` (or `UnknownOutcome` for a possibly dispatched mutation).
Server-side complete-result exhaustion remains a resource-limit error; reduce
batch/query size or explicitly configure the receiving budget as appropriate.
Scan limits/offsets retain per-fragment
semantics and scans share the query engine's byte bounds.

The allowance is conservative allocation accounting, not exact heap usage or
RSS. It includes the raw body, decoded collection capacity/payloads, temporary
validation copies and portable result/receipt conversion. A shared owner stays
held until conversion returns, so the next clone cannot allocate a raw response
while the previous response is being converted. Caller-retained returned results,
outgoing request storage, transport buffers and allocator slack are outside this
scope; independently connected clients each have their own allowance.

No write retries happen automatically. Connection loss or timeout after possible
dispatch returns `UnknownOutcome`: the write might have committed. Canceling an
in-flight request closes that connection; later requests return `Closed`.
`Client::close` stops admission for all clones and cancels active and queued
requests. An active mutation whose write has started returns `UnknownOutcome`;
queued operations and cancelled reads return `Closed`. Graceful transport
shutdown has a total budget of `ClientOptions.timeout`, then the stream is
dropped. `close_with_timeout` supplies a different budget; zero skips graceful
shutdown. Transport adapters must release resources on Drop. Closing the client
never shuts down the database.

Optional credentials are distinct from cluster secrets and must contain at
least 32 random bytes. Mutual challenge authentication prevents replay but plain
TCP does not encrypt traffic. Supply a caller-validated TLS stream with
`Client::from_stream(stream, options)` and use `ClientServer::bind_with_network`
or `ClientServer::from_acceptor` on the server. The caller owns TLS configuration
and transport handshake deadlines; ActorDB retains application authentication,
request limits, and cancellation semantics. See the [transport guide](../docs/transports.md).
Run `cargo run -p actordb-client --example tls_demo` for a complete mTLS mesh and
application example.
Application connections never become replication peers. Administrative shutdown,
placement mutation, local debug dumps, and peer replication/reconciliation RPCs
are deliberately not application operations.

This crate's normal dependency tree contains no database actors, native storage
engine, or protobuf compiler. Integration tests depend on `actordb` only for the
real application listener.

Per-write acknowledgement counts and region requirements are available through
`put_with_write_concern` and `delete_with_write_concern`. Install
`actordb-client = "0.10.0"` for region, strict-policy and durability evidence support; the compatible dependency minimums
also select the required protocol and query revisions. See the
[region guide](../docs/write-concern-regions.md) for configuration and receipt
semantics. Local acknowledgement remains the default.
