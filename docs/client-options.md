# Rust client option construction in 0.7.0

`ClientOptions` and `PutOptions` now expose Bon builders and are
`non_exhaustive`. This changes external struct-literal construction in the
unpublished 0.7.0 release; adding optional settings in later releases can keep
existing builder calls source compatible. Defaults and public field access are
preserved.

Replace this construction:

```rust,ignore
let options = ClientOptions {
    timeout: Duration::from_secs(5),
    credential: Some(credential),
    ..ClientOptions::default()
};
```

with:

```rust,ignore
let options = ClientOptions::builder()
    .timeout(Duration::from_secs(5))
    .credential(credential)
    .build();
let put = PutOptions::builder()
    .ttl(Duration::from_secs(60))
    .priority(Priority::High)
    .build();
```

Optional fields also provide `maybe_credential(Option<Vec<u8>>)` and
`maybe_ttl(Option<Duration>)` setters. For incremental configuration, start with
`ClientOptions::default()` or `PutOptions::default()` and assign public fields.

Connection validation still occurs at `connect`/`from_stream`: identities,
credentials, timeouts, and queue capacities retain their existing constraints.
The request timeout remains the default total graceful close budget;
`close_with_timeout` selects an explicit budget. Closing cancels queued and
active exchanges across clones; mutations whose write has started retain an
unknown outcome.
