# ActorDB Rust client

The SDK is a typed, transport-neutral unary RPC client. gRPC is enabled by
default; servers bind `actordb::grpc::GrpcServer` around an existing `Arc<Node>`.
Peer replication remains separate and application RPCs never join its membership.

```toml
actordb-client = "0.11.0"
```

```rust,ignore
let client = Client::connect("http://db.example:7449", ClientOptions::default()).await?;
client.put("greeting", b"hello".to_vec()).await?;
let value = client.get("greeting").await?;
client.close().await;
```

For another framework, disable default features in both SDK and database crates,
implement `RpcClient`, and construct the SDK using `Client::from_rpc`.
The server-side `RpcService` trait and `NodeRpcService` have no gRPC, sockets,
credentials, or authentication state. A Thrift or other adapter maps its messages
into the canonical typed requests and owns its own TLS, authentication and ACLs.
See [the RPC design](../docs/rpc-0.11-design.md) and [transports](../docs/transports.md).

The gRPC adapter uses unary `actordb.rpc.v1.ActorDb/Call` requests over reusable
HTTP/2 channels, not a fresh TCP connection for each operation. The former
application byte-stream protocol is removed: there is no compatibility listener,
application hello/auth exchange, connection identity, or ActorDB request ID.

The catalog includes point and ordered batch reads/writes, delete tombstones,
TTL/priorities, version/expiry metadata, table views, scans, distributed SQL,
write-concern receipts and read-only local inspection. Batches are not
transactions; inspection is not readiness or durability evidence. Logical table
views share the adapter and admission limits but isolate their data and queries.

## Bounds and cancellation

Calls can run concurrently. `max_queued_requests` and `max_queued_bytes` bound
admitted waiting/active calls. Each RPC reserves its full configured
`max_decoded_response_bytes` before framework buffering/decoding; the aggregate
`max_response_memory_bytes` budget bounds concurrent reservations. Defaults are
64 MiB per response and 256 MiB aggregate, allowing four concurrent RPCs.
Queued callers retain their request budget and total deadline.

The fixed RPC message cap is 16 MiB. The Prost codec additionally preflights
headers, collection growth, nested receipts, payloads and portable conversion
before materializing collections. Dense wide results can require an explicit
larger per-response **and aggregate** allowance. These are conservative accounting
limits, not allocator/RSS or caller-retained result bounds.

No mutation is transparently retried. A timeout/lost reply after possible
dispatch returns `UnknownOutcome`, even if the server later finishes the write.
Cancellation affects that RPC, not unrelated calls on the shared channel.
`close` stops this client's admission and cancels its clones/table views;
accepted server mutations retain their apply/fan-out ownership. Closing the SDK
does not close a separately shared custom adapter or shut down the Node.

## gRPC authentication and TLS

There are no credentials or identity fields in `ClientOptions`.
Use `grpc::GrpcClientOptions.metadata` for framework-native authorization
metadata, and `GrpcClient::from_channel` for a caller-configured tonic channel
with trusted roots/client certificates. Server TLS configuration and its
`GrpcAuthorizer` hook are gRPC-specific and run before Node admission. ACL hooks
can inspect actual table/operation, metadata and authenticated TLS connect info.

`cargo run -p actordb-client --example query_demo` demonstrates the catalog.
`tls_demo` demonstrates caller-configured mTLS on both the independent peer
transport and gRPC adapter. Production hosts provision their own identities
and trust policy; ActorDB does not manage credentials.

The SDK's normal dependency tree has no Node/actors, native storage or protobuf
compiler. Tests use a database only to verify RPC behavior; gRPC-disabled custom
adapters are tested separately. Normal builds use committed Prost bindings.
