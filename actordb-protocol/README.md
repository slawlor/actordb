# ActorDB protocol and RPC contracts

`proto/meta.proto` carries the unchanged peer protocol 6. The application
contract is `proto/client.proto`: typed unary `actordb.rpc.v1.ActorDb/Call`
with no ActorDB hello/auth exchange, connection identity, frame envelope or
request ID. Other language gRPC clients generate from both schemas normally.

Committed Prost bindings are used by normal builds. Regenerate/check with:

```sh
cargo run -p actordb-protocol --features codegen --bin generate
cargo run -p actordb-protocol --features codegen --bin generate -- --check
```

Codegen uses a vendored protoc binary; it is optional and does not enter normal
SDK/runtime dependency trees. No protobuf compiler or C++ codegen build is
required by ordinary consumers.

The always-available `rpc` module supplies object-safe client/service traits,
canonical typed envelopes and retained request/response ownership. The optional
`grpc` feature supplies the bounded Prost codec, not generic authentication.
gRPC metadata, TLS and authorization policy belong to the actual adapter.

RPC messages are capped at 16 MiB. Allocation-free `preflight_rpc_request` and
`preflight_rpc_response` additionally account for collection growth, payloads,
raw data, maps, opaque receipts and portable conversion before materialization.
`RpcResponseShape` is derived from the actual pending request/query plan;
`RpcDecodeReport` exposes conservative charges. Dense result limits differ
from request batch limits. Malformed receipt rejection may allocate bounded
error metadata, but never bypasses object admission.

Values remain opaque bytes; keys/table names are UTF-8; query integer cells use
canonical decimal strings for i128 precision. Peers retain their existing
length-prefixed protocol; RPC frameworks own their own message framing.
Authentication and credentials are absent from the canonical application model.
