//! Optional rustls transport for [`super::Network`].

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::{AcceptFuture, Acceptor, BindFuture, BoxStream, ConnectFuture, Network, NetworkError};

/// A TCP transport secured with caller-configured rustls client and server policies.
///
/// The supplied configurations determine certificate verification, client
/// authentication, cipher suites, and protocol versions. `server_name` is used
/// for outbound certificate/name validation. The same transport can be shared
/// by a node peer mesh and an application listener because each accepted stream
/// performs its own inbound handshake.
///
/// This type intentionally does not generate certificates or choose trust roots.
/// Production callers should provision those policies for their deployment.
#[derive(Clone)]
pub struct TlsNetwork {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for TlsNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsNetwork")
            .field("server_name", &self.server_name)
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl TlsNetwork {
    /// Creates a TLS transport from caller-owned rustls policies.
    ///
    /// `handshake_timeout` must be nonzero. The timeout covers the complete
    /// outbound TCP connect and TLS handshake, and each inbound TLS handshake.
    pub fn new(
        server: Arc<ServerConfig>,
        client: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        handshake_timeout: Duration,
    ) -> Result<Self, NetworkError> {
        if handshake_timeout.is_zero() {
            return Err(NetworkError("TLS handshake timeout must be nonzero".into()));
        }
        Ok(Self {
            server,
            client,
            server_name,
            handshake_timeout,
        })
    }

    /// Returns the configured outbound certificate name.
    pub fn server_name(&self) -> &ServerName<'static> {
        &self.server_name
    }

    /// Returns the configured handshake timeout.
    pub fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }
}

struct TlsAcceptorState {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    timeout: Duration,
}

impl Acceptor for TlsAcceptorState {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            let (stream, _) = self.listener.accept().await?;
            if let Err(error) = stream.set_nodelay(true) {
                tracing::debug!(%error, "failed to set TCP_NODELAY on accepted TLS connection");
            }
            let acceptor = self.acceptor.clone();
            let timeout = self.timeout;
            let handshake = Box::pin(async move {
                tokio::time::timeout(timeout, acceptor.accept(stream))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS accept timed out"))?
            });
            Ok(Box::pin(InboundTlsStream {
                state: InboundState::Handshaking(handshake),
                waiters: Vec::new(),
            }) as BoxStream)
        })
    }
}

impl Network for TlsNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await?;
            let bound = listener.local_addr()?;
            Ok((
                bound,
                Box::new(TlsAcceptorState {
                    listener,
                    acceptor: TlsAcceptor::from(self.server.clone()),
                    timeout: self.handshake_timeout,
                }) as Box<dyn Acceptor>,
            ))
        })
    }

    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            let stream = tokio::time::timeout(self.handshake_timeout, async {
                let tcp = TcpStream::connect(addr).await?;
                if let Err(error) = tcp.set_nodelay(true) {
                    tracing::debug!(%error, "failed to set TCP_NODELAY on outbound TLS connection");
                }
                TlsConnector::from(self.client.clone())
                    .connect(self.server_name.clone(), tcp)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|_| NetworkError("TLS connect timed out".into()))??;
            Ok(Box::pin(stream) as BoxStream)
        })
    }
}

type ServerStream = tokio_rustls::server::TlsStream<TcpStream>;
type HandshakeFuture = Pin<Box<dyn Future<Output = io::Result<ServerStream>> + Send>>;

enum InboundState {
    Handshaking(HandshakeFuture),
    Ready(Box<ServerStream>),
    Failed,
}

struct InboundTlsStream {
    state: InboundState,
    waiters: Vec<Waker>,
}

impl InboundTlsStream {
    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut ServerStream>> {
        if let InboundState::Handshaking(handshake) = &mut self.state {
            let result = match handshake.as_mut().poll(cx) {
                Poll::Pending => {
                    if !self.waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                        self.waiters.push(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            };
            self.wake_waiters();
            self.state = match result {
                Ok(stream) => InboundState::Ready(Box::new(stream)),
                Err(error) => {
                    self.state = InboundState::Failed;
                    return Poll::Ready(Err(error));
                }
            };
        }
        match &mut self.state {
            InboundState::Ready(stream) => Poll::Ready(Ok(stream)),
            InboundState::Failed | InboundState::Handshaking(_) => Poll::Ready(Err(
                io::Error::new(io::ErrorKind::BrokenPipe, "TLS stream closed"),
            )),
        }
    }

    fn wake_waiters(&mut self) {
        for waiter in self.waiters.drain(..) {
            waiter.wake();
        }
    }
}

impl AsyncRead for InboundTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(ready!(self.poll_stream(cx))?).poll_read(cx, buf)
    }
}

impl AsyncWrite for InboundTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(ready!(self.poll_stream(cx))?).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(ready!(self.poll_stream(cx))?).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            InboundState::Ready(stream) => Pin::new(stream).poll_shutdown(cx),
            InboundState::Handshaking(_) | InboundState::Failed => {
                self.state = InboundState::Failed;
                self.wake_waiters();
                Poll::Ready(Ok(()))
            }
        }
    }
}
