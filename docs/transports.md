# Peer transports and application RPC

ActorDB 0.11.0 separates two interfaces. Persistent peer replication uses
`Network` byte streams and unchanged exact peer protocol 6. Application APIs
use typed unary RPC traits; default gRPC is a separate framework adapter.
Neither the generic SDK nor Node RPC service authenticates clients.

## Peer mesh

Implement `actordb::network::Network` and `Acceptor`, then configure
`NodeConfig::builder().network(Arc::new(your_network)).build()`.

- `Network::bind(SocketAddr)` returns the actual listener address and an owned
  acceptor. Port zero selects an ephemeral port for the TCP/TLS transports.
- `Acceptor::accept()` returns an owned `BoxStream` for each inbound connection.
- `Network::connect(SocketAddr)` returns an owned `BoxStream` for an outbound
  connection, including any transport authentication needed before use.

A stream implements `AsyncRead + AsyncWrite + Send + Unpin + 'static`.
`PeerStream` is blanket-implemented, so TLS streams do not require a separate
implementation. Framing, ActorDB handshakes, gossip, replication, and Node-owned
reconnects continue to run above the transport.

### Built-in rustls transport

The optional `actordb/tls` feature provides `actordb::network::TlsNetwork`.
It accepts caller-owned rustls `ServerConfig` and `ClientConfig` values, an
outbound `ServerName`, and a nonzero handshake timeout:

```rust,ignore
use actordb::network::TlsNetwork;
use tokio_rustls::rustls::pki_types::ServerName;

let network = TlsNetwork::new(
    server_config,
    client_config,
    ServerName::try_from("db.example.test")?,
    std::time::Duration::from_secs(5),
)?;
let config = actordb::node::NodeConfig::builder()
    .network(std::sync::Arc::new(network))
    .build();
```

Certificate roots, client authentication, protocol versions, and cipher policy
remain under the supplied rustls configurations. Inbound handshakes are lazy
per accepted stream, so one stalled client does not block the listener; the
configured timeout still bounds each handshake. A single `TlsNetwork` uses one
outbound server name for all destinations. Deployments that need a different
name per address can implement `Network` and select names in their adapter.

## Application RPC

The default `grpc` feature is enabled in both `actordb` and `actordb-client`.
It provides unary `actordb.rpc.v1.ActorDb/Call` RPCs with committed Prost bindings.
HTTP/2 channels are reusable; RPC does not require dialing for every operation.

```rust,ignore
let mut server = GrpcServer::bind(node, addr, GrpcServerOptions::default()).await?;
let client = Client::connect(format!("http://{}", server.addr()), ClientOptions::default()).await?;
client.put("greeting", b"hello".to_vec()).await?;
client.close().await;
server.shutdown().await?;
```

Authentication, authorization and TLS belong to this adapter. Supply provisioned
certificates and CA policy through tonic `ServerTlsConfig`/`ClientTlsConfig`;
construct `GrpcClient` from the configured channel and provide native metadata
using `GrpcClientOptions`. `GrpcServerOptions.authorize` runs before Node admission
and can inspect metadata, TLS certificate/connect info, and actual table/operation.
Normalize table names appropriately in an ACL policy; the Node canonicalizes them.

For another RPC framework:

```toml
actordb = { version = "0.11.0", default-features = false }
actordb-client = { version = "0.11.0", default-features = false }
```

Implement `actordb_protocol::rpc::RpcClient` on the client side and expose
`actordb::rpc::NodeRpcService` through that framework on the server.
`RpcRequest` and `RpcReply` carry typed canonical data plus retained ownership.
The traits do not contain gRPC, sockets, metadata credentials, or auth callbacks.
A Thrift adapter owns its authentication/ACLs and maps messages into these types.
It must preserve bounded materialization, deadlines, cancellation and no-retry
semantics. Dropping a waiter never authorizes abandoning an admitted mutation.

The old application length-prefixed listener/hello/auth protocol is removed.
There is no stream-based compatibility adapter, identity/credential field,
or ActorDB request ID. See [the RPC migration](rpc-0.11-design.md).

## Peer transport contract

Peer frames remain four-byte big-endian lengths plus bounded protobuf bodies.
`Network` owns TLS configuration, reliable ordered bidirectional bytes,
backpressure, handshake deadlines and cancellation/resource release.
An optional high-entropy cluster secret proves shared possession, not individual
identity or post-handshake payload protection. TLS provides peer authentication,
integrity and confidentiality. These peer mechanisms are unchanged.

## Lifecycle and verification

The Node service owns accepted mutation apply/fan-out and response leases even
after a caller disappears. Adapter shutdown joins its transport lifecycle before
Node shutdown; timed-out service shutdown retains owned work for retry.
RPC loss produces unknown mutation outcomes, never transparent replay.

Run `cargo run -p actordb-client --example tls_demo` for separate peer mTLS
and gRPC mTLS. The demo generates throwaway self-signed credentials only for
localhost; real deployments provision their own certificates and trust policy.
Tests cover adapter ACL/mTLS, custom gRPC-disabled adapters, concurrency,
bounded raw decoding, lifecycle and independent Docker processes.
