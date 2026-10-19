# Custom transports and TLS

ActorDB accepts caller-supplied async byte streams for both peer connections and
application connections. Plain TCP remains the default. TLS libraries are only
development dependencies for the example and tests; applications choose their
own TLS library, certificates, trust roots, and validation policy.

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

## Application listener

Use `ClientServer::bind_with_network(node, addr, network, options)` to bind through
the same transport interface. `ClientServer::bind(node, addr, options)` remains
the plain TCP convenience API.

If the caller already owns a listener, transfer it with
`ClientServer::from_acceptor(node, bound_addr, acceptor, options)`. The server owns
the acceptor and accepted streams, applies its normal connection/request/byte
limits, and reports `bound_addr` through `addr()`. Invalid options drop the supplied
acceptor; `bind_with_network` validates options before binding. Accept errors retry
with a short delay, allowing transient transport failures to recover.

Shutdown cancels pending accepts, drops the listener, and joins application
sessions. Accepted mutations retain their existing completion semantics. The
underlying Node continues running, and application connections never enter peer
membership.

## Rust application client

Establish and validate the transport yourself, then call:

```rust,ignore
let stream = connector.connect(server_name, tcp_stream).await?;
let client = Client::from_stream(stream, client_options).await?;
client.put("greeting", b"hello".to_vec()).await?;
client.close().await;
```

`Client::from_stream` owns the supplied stream and performs the normal application
handshake. `ClientOptions::timeout` bounds that handshake; the caller sets deadlines
for dialing and TLS negotiation before supplying the stream. Validation failure,
handshake failure, and cancellation drop the stream. `Client::connect` still uses
plain TCP with one deadline covering its dial and application handshake.

All operation methods, serialized request admission, framing limits, credential
checks, and unknown mutation outcomes remain the same. The SDK does not gain a
normal dependency on `actordb` or a TLS library.

## Transport contract

- Deliver reliable, ordered bytes and support concurrent reads and writes.
  Propagate backpressure and connection errors. ActorDB flushes each complete
  frame, supporting buffered streams as well as sockets.
- Dropping a pending bind, connect, or accept future must cancel that operation
  and release partial resources. Dropping an acceptor releases its listener;
  dropping a stream releases its connection and owned transport work. Do not
  detach unowned tasks or replay bytes or operations across reconnects.
- Bound TLS negotiation and other transport work. Inbound negotiation should
  run inside admitted sessions, such as through a stream that negotiates on
  first I/O. Awaiting every TLS handshake serially inside `accept()` can let one
  slow connection block subsequent accepts and does work before session limits.
- TLS certificate/name validation belongs to the transport. Application
  credentials and cluster-secret proofs keep their existing roles; they do not
  provide transport encryption. The current interface does not expose certificate
  identities to authorize individual ActorDB node IDs.

Destinations remain `SocketAddr`. ActorDB still uses its length-prefixed protobuf
peer/application protocols. A custom RPC framework can supply a compatible
stream adapter; native framework RPC methods and non-socket endpoint addressing
would require a larger API change.

## Runnable mTLS example

```bash
cargo run -p actordb-client --example tls_demo
```

The [example](../actordb-client/examples/tls_demo.rs) starts two TLS-connected
nodes, a TLS application listener, and a client created with `from_stream`, then
verifies replication of a client write. Its
[transport adapter](../actordb-client/examples/common/tls.rs) accepts caller-created
rustls client/server configurations and runs inbound TLS negotiation on first
I/O with a deadline. There are no detached handshake tasks.

For the localhost demonstration only, fresh self-signed credentials are generated
and shared by its participants. Real applications should provision their own
identities and trust policy. For multiple peer names, the adapter should select
the expected TLS name per destination instead of using the example's single
`localhost` name.

Transport integration tests exercise buffered non-TCP streams, partial I/O,
replication/reconnects, authentication failures, validation, cancellation,
listener/stream release, certificate/name validation, and mTLS traffic.
