//! Example caller-owned TLS transport. No TLS dependency is needed by ActorDB.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};
use std::time::Duration;

use actordb::network::{
    AcceptFuture, Acceptor, BindFuture, BoxStream, ConnectFuture, Network, NetworkError,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Clone)]
pub struct TlsNetwork {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    timeout: Duration,
}

impl TlsNetwork {
    /// Real applications supply their own certificates, roots and name policy.
    pub fn new(
        server: Arc<ServerConfig>,
        client: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        timeout: Duration,
    ) -> Self {
        Self {
            server,
            client,
            server_name,
            timeout,
        }
    }
}

struct Listener {
    tcp: TcpListener,
    tls: TlsAcceptor,
    timeout: Duration,
}

impl Acceptor for Listener {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            let (stream, _) = self.tcp.accept().await?;
            stream.set_nodelay(true)?;
            let tls = self.tls.clone();
            let timeout = self.timeout;
            // Handshake on first I/O, inside the Node/application session's
            // admission and cancellation boundary. A slow or invalid TLS client
            // cannot block the listener or tear down another client's session.
            let handshake = Box::pin(async move {
                tokio::time::timeout(timeout, tls.accept(stream))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS accept timed out"))?
            });
            Ok(Box::pin(Inbound {
                state: State::Handshake(handshake),
                waiters: Vec::new(),
            }) as BoxStream)
        })
    }
}

impl Network for TlsNetwork {
    fn bind(&self, addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let tcp = TcpListener::bind(addr).await?;
            Ok((
                tcp.local_addr()?,
                Box::new(Listener {
                    tcp,
                    tls: TlsAcceptor::from(self.server.clone()),
                    timeout: self.timeout,
                }) as Box<dyn Acceptor>,
            ))
        })
    }

    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            let stream = tokio::time::timeout(self.timeout, async {
                let tcp = TcpStream::connect(addr).await?;
                tcp.set_nodelay(true)?;
                TlsConnector::from(self.client.clone())
                    .connect(self.server_name.clone(), tcp)
                    .await
            })
            .await
            .map_err(|_| NetworkError("TLS connect timed out".into()))??;
            Ok(Box::pin(stream) as BoxStream)
        })
    }
}

type ServerStream = tokio_rustls::server::TlsStream<TcpStream>;
enum State {
    Handshake(Pin<Box<dyn Future<Output = io::Result<ServerStream>> + Send>>),
    Ready(Box<ServerStream>),
    Failed,
}

struct Inbound {
    state: State,
    waiters: Vec<Waker>,
}

impl Inbound {
    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut ServerStream>> {
        if let State::Handshake(handshake) = &mut self.state {
            let result = match handshake.as_mut().poll(cx) {
                Poll::Pending => {
                    if !self.waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                        self.waiters.push(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            };
            // Node splits the stream into independently polled reader/writer
            // tasks. Finishing negotiation in one half must wake the other too.
            self.wake_waiters();
            self.state = match result {
                Ok(stream) => State::Ready(Box::new(stream)),
                Err(error) => {
                    self.state = State::Failed;
                    return Poll::Ready(Err(error));
                }
            };
        }
        match &mut self.state {
            State::Ready(stream) => Poll::Ready(Ok(stream)),
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TLS stream closed",
            ))),
        }
    }

    fn wake_waiters(&mut self) {
        for waker in self.waiters.drain(..) {
            waker.wake();
        }
    }
}

impl AsyncRead for Inbound {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(ready!(self.poll_stream(cx))?).poll_read(cx, buf)
    }
}

impl AsyncWrite for Inbound {
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
            State::Ready(stream) => Pin::new(stream).poll_shutdown(cx),
            _ => {
                self.state = State::Failed; // Drop any partially established connection.
                self.wake_waiters();
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Fresh credentials for a localhost demonstration, never persistent identities.
/// All participants in this example trust the generated certificate and use it
/// for both server and client authentication. Deployments should provision
/// distinct identities and an appropriate CA/name policy.
pub fn localhost_configs(
) -> Result<(Arc<ServerConfig>, Arc<ClientConfig>), Box<dyn std::error::Error>> {
    use tokio_rustls::rustls::{
        pki_types::PrivatePkcs8KeyDer, server::WebPkiClientVerifier, RootCertStore,
    };

    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = generated.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone())).build()?;
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert.clone()], key.clone_key().into())?;
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![cert], key.into())?;
    Ok((Arc::new(server), Arc::new(client)))
}
