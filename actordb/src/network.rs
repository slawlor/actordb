// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Pluggable network transport for the peer mesh.
//!
//! A [`Network`](crate::network::Network) binds a listen address and dials peers, producing
//! byte streams. Everything above — framing, handshake, gossip —
//! operates on those streams generically. The default is [`TcpNetwork`](crate::network::TcpNetwork)
//! (plain TCP); implement [`Network`](crate::network::Network) to run the mesh over TLS, a
//! Meta-internal transport, or anything else that moves bytes.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// A connected peer: any async bidirectional byte stream.
///
/// Blanket-implemented for all suitable types, so a `TcpStream`, a
/// TLS stream, or any future transport plugs in unchanged.
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

/// Accepts inbound peer connections. Returned by [`Network::bind`].
pub trait Acceptor: Send + 'static {
    /// Accept the next inbound connection.
    fn accept(&mut self) -> AcceptFuture<'_>;
}

/// Pluggable network transport for the peer mesh.
///
/// Implementors only move bytes: [`Network::bind`] listens for peers
/// and [`Network::connect`] dials one. Framing, the handshake, and
/// gossip all live one layer up and are transport-agnostic.
pub trait Network: Send + Sync + 'static {
    /// Bind `addr` (port 0 picks an ephemeral port). Returns the bound
    /// address and an acceptor for inbound connections.
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_>;

    /// Dial a peer.
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
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await?;
            let bound = listener.local_addr()?;
            Ok((
                bound,
                Box::new(TcpAcceptor { listener }) as Box<dyn Acceptor>,
            ))
        })
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
