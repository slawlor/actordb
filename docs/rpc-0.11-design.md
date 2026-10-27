# Application RPC migration (0.11.0)

The SDK and Node application service communicate through typed `RpcClient` and
`RpcService` traits. No sockets, credentials, authentication handshake, connection
identity, or correlation IDs belong to that core boundary. An alternate framework
maps its own messages into the canonical request/response types and owns its ACLs.

The default-enabled `grpc` feature supplies unary Prost-based gRPC for both crates.
Vendors disable default features and implement their framework adapters. gRPC
reuses HTTP/2 channels; request/response RPC does not mean dialing per operation.
Peer replication, peer authentication, and actor execution remain unchanged.

The old application byte-stream protocol is removed, not preserved behind a
compatibility feature. Authentication, TLS, metadata, and authorization hooks
exist only in the gRPC adapter. The gRPC service path versions its RPC contract.

Admission accounts for requests and response materialization. Conservative
per-call response reservations permit concurrent RPCs without losing aggregate
memory bounds. Codecs preflight repeated fields, opaque receipts, byte budgets,
and pending response shapes before Prost collection allocation. Custom adapters
must provide equivalent bounded decoding, cancellation, and non-retry semantics.

Once admitted, mutation application and fan-out stay owned by the service even
if the caller disappears. A lost reply remains an unknown outcome; no adapter
may transparently retry a mutation. Responses retain admission/drain ownership
through their adapter's encoding/body lifecycle. Adapter shutdown joins its
network work before Node shutdown.

Verification includes gRPC-disabled custom adapters; concurrent calls; API and
table/query/receipt parity; malformed and oversized messages; deadlines and
shutdown; adapter-only authorization and TLS; and all Docker cluster scenarios.
All published crates advance together to 0.11.0; publishing is a separate step.
