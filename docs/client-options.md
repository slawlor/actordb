# Rust client options

Construct `ClientOptions` and `PutOptions` through their Bon builders, or start
with `Default` and modify public fields. These types are `non_exhaustive`.

```rust,ignore
let options = ClientOptions::builder()
    .table("users")
    .timeout(Duration::from_secs(5))
    .credential(credential)
    .build();
let put = PutOptions::builder()
    .ttl(Duration::from_secs(60))
    .priority(Priority::High)
    .build();
```

Optional fields provide `maybe_credential(Option<Vec<u8>>)` and
`maybe_ttl(Option<Duration>)` setters. The initial table defaults to `KV`;
`table("users")` explicitly selects another validated namespace.

Additional table handles share one authenticated connection:

```rust,ignore
let client = Client::connect(addr, ClientOptions::default()).await?;
let users = client.table("users")?;
let events = client.table("events")?;
users.put("u:1", value).await?;
events.put("e:1", other_value).await?;
```

Connection validation happens at `connect` / `from_stream`, including
credentials, timeouts and queue capacities. The SDK uses exact application
protocol **4**; there is no negotiation with non-current listeners.

## Decoded response admission

The fixed wire-body limit remains 16 MiB. `max_decoded_response_bytes` defaults
to a separate **64 MiB** accounted allowance, including raw bytes, collection
capacity/message headers, payloads and portable result/receipt conversion.
Cells and nested inspection/acknowledgement entries count toward one aggregate
budget; server errors cannot waive it. This stricter default can reject large
responses that encoded-framing-only clients accepted.
It can also reject a near-limit text result produced under the server's default
result policy: encoded size and server-retained size do not account for the same
decoded/conversion capacities. It is not a promise that every server-accepted
result fits the receiving client's default allowance.

```rust,ignore
let options = ClientOptions::builder()
    .max_decoded_response_bytes(512 * 1024 * 1024)
    .build();
```

Choose a larger allowance explicitly for trusted wide or near-server-limit narrow
text results. Increasing it
does not increase the wire limit or existing row/column/receipt limits. Values
below 4 KiB are invalid. Exceeding admission fails before decoded collections
are constructed and closes that connection; a possibly dispatched write has
an unknown outcome, never an automatic retry. Invalid request/response shape
and handshake message kinds are also checked before materialization.

The protocol's `ClientDecodeReport` exposes conservative accounted raw/header/
payload bytes and aggregate query cells. Vector charges include minimum and
geometric growth capacity; byte/string charges include small-buffer capacity,
validation copies and nested receipt expansion. This is not an allocator or RSS
bound. Caller-retained results, outgoing request storage, transport buffers and
allocator slack are outside this allowance. Separate client connections have
separate allowances.

The budget/header/payload and shape walk is allocation-free. Successful preflight
does not materialize decoded messages or payloads. Malformed typed receipt
validation may allocate bounded error metadata before returning rejection; this
does not waive or bypass admission.

All clones/table views share one decode/conversion owner. It is acquired before
socket transfer or raw-body allocation and retained until public conversion
returns. A clone waiting there can time out or be cancelled without taking or
retiring the aligned transport. Closing wakes all waiting clones. Cancellation,
errors and Drop release ownership; no detached response-conversion task remains.

The request timeout is the default total graceful close budget.
`close_with_timeout` selects another budget. Closing cancels queued and active
exchanges across clones; a mutation whose write started has an unknown outcome.
Closing the SDK never shuts down the database.

See [the client guide](../actordb-client/README.md) and
[transport configuration](transports.md).
