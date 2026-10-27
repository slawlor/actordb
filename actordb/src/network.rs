// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Pluggable byte-stream transport for the peer mesh and application listener.
//!
//! A [`Network`](crate::network::Network) binds a listen address and dials connections,
//! producing byte streams for the peer mesh. Application RPC frameworks own
//! their own transports. Peer framing, handshake, and gossip
//! operates on those streams generically. The default is [`TcpNetwork`](crate::network::TcpNetwork)
//! (plain TCP). Enable the crate's `tls` feature for the built-in
//! [`TlsNetwork`](crate::network::TlsNetwork), or implement [`Network`](crate::network::Network)
//! to run the mesh over another TLS stack, a Meta-internal transport, or anything else that moves
//! bytes.
//!
//! # Cancellation and resource ownership
//!
//! Node startup, deadlines, and shutdown may drop a pending
//! [`Network::bind`](crate::network::Network::bind),
//! [`Network::connect`](crate::network::Network::connect), or
//! [`Acceptor::accept`](crate::network::Acceptor::accept) future.
//! Implementations must be cancellation-safe: dropping one of these futures
//! must cancel that operation and release any partially acquired transport
//! resources rather than detach background work. Dropping an
//! [`Acceptor`](crate::network::Acceptor) must release its listener, and
//! dropping a [`BoxStream`](crate::network::BoxStream) must release the
//! connection. This is how
//! [`Node::shutdown`](crate::node::Node::shutdown) can join all Node-owned
//! transport work without knowing the concrete transport.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

mod connection_id;
pub use connection_id::PeerConnectionId;

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub use tls::TlsNetwork;

/// A peer or application connection: any async bidirectional byte stream.
///
/// Streams must deliver reliable, ordered bytes, support concurrent reading and
/// writing, and propagate backpressure and connection errors. ActorDB flushes
/// after each frame, so buffered streams are supported. Transport adapters must
/// not replay bytes across connections or transparently retry database operations.
///
/// Blanket-implemented for all suitable types, so a `TcpStream`, a
/// TLS stream, or any future transport plugs in unchanged. Dropping the stream
/// must close or otherwise release the underlying connection; Node shutdown
/// relies on this ownership boundary.
pub trait PeerStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> PeerStream for T {}

/// Boxed peer stream.
pub type BoxStream = Pin<Box<dyn PeerStream>>;

/// Boxed future returned by [`Acceptor::accept`].
pub type AcceptFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BoxStream, NetworkError>> + Send + 'a>>;

/// Boxed future returned by [`Network::bind`].
pub type BindFuture<'a> = Pin<
    Box<dyn Future<Output = Result<(SocketAddr, Box<dyn Acceptor>), NetworkError>> + Send + 'a>,
>;

/// Boxed future returned by [`Network::connect`].
pub type ConnectFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BoxStream, NetworkError>> + Send + 'a>>;

/// Error from a [`Network`] operation.
#[derive(Debug)]
pub struct NetworkError(pub String);

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "network error: {}", self.0)
    }
}

impl std::error::Error for NetworkError {}

impl From<std::io::Error> for NetworkError {
    fn from(e: std::io::Error) -> Self {
        Self(e.to_string())
    }
}

/// Accepts inbound peer or application connections. Returned by [`Network::bind`].
///
/// Dropping the acceptor must release the listening resource. A pending
/// [`accept`](Acceptor::accept) future may be dropped during Node shutdown and
/// must not leave a detached accept operation behind.
pub trait Acceptor: Send + 'static {
    /// Accept the next inbound connection.
    ///
    /// This future must be cancellation-safe: dropping it cancels the pending
    /// accept without leaking a socket, task, or other transport resource.
    fn accept(&mut self) -> AcceptFuture<'_>;
}

/// Pluggable byte-stream transport for peers and application connections.
///
/// Implementors only move bytes: [`Network::bind`] listens for peers
/// and [`Network::connect`] dials one. Framing, the handshake, and
/// gossip all live one layer up and are transport-agnostic.
pub trait Network: Send + Sync + 'static {
    /// Bind `addr` (port 0 picks an ephemeral port). Returns the bound
    /// address and an acceptor for inbound connections.
    ///
    /// This future may be dropped when Node startup is cancelled. Dropping it
    /// must release any partially created listener or background work.
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_>;

    /// Dial a peer.
    ///
    /// Node dial deadlines and shutdown drop this future to cancel the attempt.
    /// An implementation must therefore release partial connections and must
    /// not leave a detached dial task running after the future is dropped.
    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_>;
}

/// Plain TCP transport. The default [`Network`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TcpNetwork;

struct TcpAcceptor {
    listener: TcpListener,
}

impl Acceptor for TcpAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            let (stream, _) = self.listener.accept().await?;
            // Disable Nagle: the mesh is small request/response RPCs
            // (acks, forwarded gets, hellos). Buffering them for
            // coalescing interacts badly with delayed ACKs and adds
            // tens of ms per exchange. Best-effort: a failure here
            // must not fail the connection.
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY on accepted peer: {e}");
            }
            let boxed: Box<dyn PeerStream> = Box::new(stream);
            Ok(Pin::new(boxed))
        })
    }
}

impl Network for TcpNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move { Ok(bind_tcp(addr).await?) })
    }

    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            // See accept(): disable Nagle on dialed connections too.
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY on dialed peer: {e}");
            }
            let boxed: Box<dyn PeerStream> = Box::new(stream);
            Ok(Pin::new(boxed))
        })
    }
}

// Keep the concrete I/O error for the existing application TCP convenience API.
pub(crate) async fn bind_tcp(addr: SocketAddr) -> std::io::Result<(SocketAddr, Box<dyn Acceptor>)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    Ok((bound, Box::new(TcpAcceptor { listener })))
}
