# Rust RPC client options

Construct `ClientOptions` and `PutOptions` with Bon builders or `Default`.
They contain database/request policy, not RPC credentials or connection identity.

```rust,ignore
let options = ClientOptions::builder()
    .table("USERS")
    .timeout(Duration::from_secs(5))
    .max_queued_requests(64)
    .max_decoded_response_bytes(64 * 1024 * 1024)
    .max_response_memory_bytes(256 * 1024 * 1024)
    .build();
let put = PutOptions::builder().ttl(Duration::from_secs(60))
    .priority(Priority::High).build();
```

A table view shares the RPC adapter and budgets:

```rust,ignore
let client = Client::connect("http://db.example:7449", options).await?;
let users = client.table("USERS")?;
users.put("u:1", value).await?;
```

For a vendor framework use `Client::from_rpc(Arc::new(adapter), options)` with
default features disabled. It validates options before dispatch, without dialing
or performing authentication. The gRPC convenience factory validates before dial.

## Response admission

Each call reserves its whole per-response allowance before invoking the RPC
adapter. Aggregate reservations are shared across clones/table views until
public conversion finishes. The defaults allow four concurrent 64 MiB calls;
the request-count limit also bounds callers waiting for a reservation.

The RPC message cap is 16 MiB. Allocation-free Prost preflight additionally
accounts for raw bytes, collection capacity/headers, payloads, temporary validation
copies and portable result/receipt conversion. `RpcDecodeReport` exposes these
charges and query cell counts. Errors cannot bypass admission, and decoded
responses are pinned to the actual pending operation/query shape, not echoed IDs.

For an explicitly trusted wider result:

```rust,ignore
let options = ClientOptions::builder()
    .max_decoded_response_bytes(512 * 1024 * 1024)
    .max_response_memory_bytes(1024 * 1024 * 1024)
    .build();
```

Per-call allowance must be at least 16 MiB and fit the aggregate budget. Raising
it does not change row/column/receipt or RPC message limits. The accounting is
not an allocator/RSS or caller-retained-result cap. Custom frameworks must
provide equivalent bounded decoding before constructing canonical responses.

## Deadlines and close

Deadlines include admission wait, RPC, validation and conversion. Write-concern
policy wait is explicitly added to the normal request budget. No mutation is
replayed after uncertainty. Losing one RPC does not retire unrelated channel calls.

`close` stops admission and cancels local waiters across this client's views.
Unsent work and active reads return `Closed`; possibly dispatched writes return
`UnknownOutcome`. Admitted server mutations still finish and retain ownership.
The Node and independently shared RPC adapters remain running.

Authentication/TLS/ACL configuration belongs only to the adapter:
`GrpcClientOptions`, a configured tonic channel and `GrpcServerOptions`.
