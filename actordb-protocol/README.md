# ActorDB protocol

`proto/meta.proto` is the canonical peer schema. Peer protocol 6 and application
protocol 4 require an exact current version; there are no negotiated capability
bits, downgrade paths, or compatibility message bindings. The standalone
`actordb-protocol` crate provides the public Rust bindings. Bindings are committed
so normal consumers do not compile protobuf or RocksDB.

Regenerate or check the packaged schemas:

```sh
cargo run -p actordb-protocol --features codegen --bin generate
cargo run -p actordb-protocol --features codegen --bin generate -- --check
```

The optional `codegen` feature builds bundled protoc and requires CMake and a
C/C++ toolchain. Other language SDKs should generate from these schemas.

Framing is a four-byte big-endian body length followed by a single protobuf
frame, capped at 16 MiB. Values are arbitrary bytes; keys are UTF-8. Query integer
cells use canonical decimal strings to preserve i128 precision. Paginated
reconciliation, bounded batches, scalar RPCs, tables, scans, queries, and
durability acknowledgements are current mandatory contracts.

Application preflight also admits aggregate decoded storage separately from
wire size. The default accounted allowance is 64 MiB; the SDK can explicitly
configure a larger one. Raw bytes, geometric/minimum vector capacities, message/
cell/map headers, string/byte buffers and nested receipt/evidence conversion are
charged before Prost materializes them. `preflight_client_frame_with_budget`
returns the `ClientDecodeReport`; `preflight_client_response` additionally checks
the actual pending request ID, operation counts and trusted query width/limit.
Handshake phases can require an exact `ClientFrameKind` before materialization.
The budget/header/payload and shape walk itself is allocation-free. Successful
preflight constructs no decoded messages or collections; malformed typed receipt
validation can allocate bounded error metadata before returning rejection.
Returned caller-owned data, allocator slack and transport buffers are not an RSS
guarantee. This admission policy changes no protocol tags or value encodings.

Application requests require a logical `table` name. Empty tables and unknown
operations are rejected explicitly. Never reuse reserved field numbers or
silently alter enum/absence meanings.

Application Hello contains identity, fresh challenge, and exact version.
The optional shared-secret proof uses HMAC-SHA256 over
`actordb-client-proof-v4\0`, proving role (`client` or `server`), and both
ordered Hellos: identity length (u64), identity bytes, exact version (u32),
challenge length (u64), and challenge bytes. Integers are big-endian.
Application and peer proofs use distinct domains. See
[peer generation fencing](../docs/peer-authority.md) for the peer transcript and
the operational-versus-transport authentication boundary.

Transport trust, TLS verification, and credential revocation belong to the
application. A shared-secret handshake alone does not protect post-handshake
plaintext frames.
