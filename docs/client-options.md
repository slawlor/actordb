# Rust client options in 0.9.0

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

The request timeout is the default total graceful close budget.
`close_with_timeout` selects another budget. Closing cancels queued and active
exchanges across clones; a mutation whose write started has an unknown outcome.
Closing the SDK never shuts down the database.

See [the client guide](../actordb-client/README.md) and
[transport configuration](transports.md).
