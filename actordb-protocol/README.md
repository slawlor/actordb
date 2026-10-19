# ActorDB protocol

`proto/meta.proto` is the canonical peer schema, preserving all existing message
numbers and `actordb::proto` reexports. Peer protocol v3 remains separate from
`proto/client.proto`, application protocol v1. Rust bindings are committed so
normal client builds do not compile protobuf or RocksDB.

Regenerate bindings:

```
cargo run -p actordb-protocol --features codegen --bin generate
cargo run -p actordb-protocol --features codegen --bin generate -- --check
```

The optional `codegen` feature builds bundled protoc through `protobuf-src` 2.1
and requires CMake and a C/C++ toolchain. It is unnecessary for consumers.
Other language SDKs should generate bindings from these packaged schemas.
Integers in query cells are canonical signed decimal
strings, preserving i128 precision. Keys are UTF-8 strings; values are arbitrary
bytes. Framing is a four-byte big-endian body length, capped at 16 MiB.

Application Hello/Auth negotiate version and optional capabilities before requests.
Challenge proofs use `actordb-client-proof-v1\0`, proving role, and both ordered
Hello offers; they are independent of peer credentials and membership. Byte
lengths are u64 big-endian, versions u32 big-endian, capabilities u64 big-endian.
Unknown operations must fail explicitly. Never recycle retired field numbers or
silently change enum/absence meanings. New peer scan/query messages require their
negotiated capability bits before sending to old v3 nodes.
