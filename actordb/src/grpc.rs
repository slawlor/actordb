//! Optional gRPC application adapter. TLS, credentials, and ACLs live here;
//! the Node service and generic RPC traits never authenticate clients.

use crate::node::{Node, NodeError};
use crate::rpc::{NodeRpcService, RpcServiceOptions};
use actordb_protocol::{
    client as wire,
    grpc::{BoundedCodec, Payload},
    rpc::{RpcError, RpcService},
    DEFAULT_MAX_DECODED_RESPONSE_BYTES, MAX_RPC_MESSAGE_BYTES, RPC_CALL_PATH, RPC_SERVICE_NAME,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tonic::codegen::http;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::transport::{Server, ServerTlsConfig};
pub use tonic::Status;

/// Framework-native authorization hook. Metadata, TLS connect info and the
/// actual requested table/operation are available before any Node admission.
pub type GrpcAuthorizer = Arc<
    dyn Fn(&tonic::metadata::MetadataMap, &http::Extensions, &wire::Request) -> Result<(), Status>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct GrpcServerOptions {
    pub service: RpcServiceOptions,
    pub tls: Option<ServerTlsConfig>,
    pub authorize: Option<GrpcAuthorizer>,
    pub max_connections: usize,
    pub max_decoded_request_bytes: usize,
    pub receive_memory_bytes: usize,
    pub shutdown_timeout: Duration,
}
impl Default for GrpcServerOptions {
    fn default() -> Self {
        Self {
            service: RpcServiceOptions::default(),
            tls: None,
            authorize: None,
            max_connections: 64,
            max_decoded_request_bytes: DEFAULT_MAX_DECODED_RESPONSE_BYTES,
            receive_memory_bytes: 256 * 1024 * 1024,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}
impl std::fmt::Debug for GrpcServerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcServerOptions")
            .field("service", &self.service)
            .field("tls", &self.tls.as_ref().map(|_| "[CONFIGURED]"))
            .field(
                "authorize",
                &self.authorize.as_ref().map(|_| "[CONFIGURED]"),
            )
            .field("max_connections", &self.max_connections)
            .field("max_decoded_request_bytes", &self.max_decoded_request_bytes)
            .field("receive_memory_bytes", &self.receive_memory_bytes)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

pub struct GrpcServer {
    addr: SocketAddr,
    cancel: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    node_service: Option<NodeRpcService>,
    shutdown_timeout: Duration,
}
impl GrpcServer {
    pub async fn bind(
        node: Arc<Node>,
        addr: SocketAddr,
        options: GrpcServerOptions,
    ) -> Result<Self, NodeError> {
        let service = NodeRpcService::new(node, options.service.clone())?;
        let mut server = Self::bind_service(Arc::new(service.clone()), addr, options).await?;
        server.node_service = Some(service);
        Ok(server)
    }
    /// Bind the optional adapter around any typed service. Custom framework
    /// providers need only RpcService and never depend on this module.
    pub async fn bind_service(
        service: Arc<dyn RpcService>,
        addr: SocketAddr,
        options: GrpcServerOptions,
    ) -> Result<Self, NodeError> {
        if options.max_connections == 0
            || options.max_connections > Semaphore::MAX_PERMITS
            || options.max_decoded_request_bytes < MAX_RPC_MESSAGE_BYTES
            || options.max_decoded_request_bytes > u32::MAX as usize
            || options.receive_memory_bytes < options.max_decoded_request_bytes
            || options.receive_memory_bytes > Semaphore::MAX_PERMITS
            || options.shutdown_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(options.shutdown_timeout)
                .is_none()
        {
            return Err(NodeError::Codec("invalid gRPC admission bounds".into()));
        }
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let connections = Arc::new(Semaphore::new(options.max_connections));
        let incoming =
            TcpListenerStream::new(listener).filter_map(move |accepted| match accepted {
                Ok(stream) => connections.clone().try_acquire_owned().ok().map(|permit| {
                    Ok(LimitedTcp {
                        stream,
                        _permit: permit,
                    })
                }),
                Err(error) => Some(Err(error)),
            });
        let adapter = GrpcAdapter {
            service,
            authorize: options.authorize,
            receive: Arc::new(Semaphore::new(options.receive_memory_bytes)),
            decoded_limit: options.max_decoded_request_bytes,
        };
        let mut builder = Server::builder().max_concurrent_streams(64u32);
        if let Some(tls) = options.tls {
            builder = builder
                .tls_config(tls)
                .map_err(|e| NodeError::Network(e.to_string()))?;
        }
        let (cancel, mut stopped) = watch::channel(false);
        let task = tokio::spawn(builder.add_service(adapter).serve_with_incoming_shutdown(
            incoming,
            async move {
                if !*stopped.borrow_and_update() {
                    let _ = stopped.changed().await;
                }
            },
        ));
        Ok(Self {
            addr,
            cancel,
            task: Some(task),
            node_service: None,
            shutdown_timeout: options.shutdown_timeout,
        })
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub async fn shutdown(&mut self) -> Result<(), NodeError> {
        self.cancel.send_replace(true);
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        let mut failure = None;
        if let Some(task) = self.task.as_mut() {
            match tokio::time::timeout_at(deadline, &mut *task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(result) => {
                    failure = Some(NodeError::Network(format!("gRPC server exit: {result:?}")))
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failure = Some(NodeError::Network(
                        "gRPC network shutdown deadline elapsed".into(),
                    ));
                }
            }
        }
        self.task.take();
        if let Some(service) = &self.node_service {
            service
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await?;
        }
        if let Some(failure) = failure {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
impl Drop for GrpcServer {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

#[derive(Clone)]
struct GrpcAdapter {
    service: Arc<dyn RpcService>,
    authorize: Option<GrpcAuthorizer>,
    receive: Arc<Semaphore>,
    decoded_limit: usize,
}
impl tonic::server::NamedService for GrpcAdapter {
    const NAME: &'static str = RPC_SERVICE_NAME;
}
impl<B> tonic::codegen::Service<http::Request<B>> for GrpcAdapter
where
    B: http_body::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != RPC_CALL_PATH {
            return Box::pin(async { Ok(Status::unimplemented("unknown RPC method").into_http()) });
        }
        // Reserve before tonic receives/materializes the entire message. The
        // lease transfers into RpcRequest, then the owned Node execution task.
        let permit = match self
            .receive
            .clone()
            .try_acquire_many_owned(self.decoded_limit as u32)
        {
            Ok(permit) => permit,
            Err(_) => {
                return Box::pin(async {
                    Ok(Status::resource_exhausted("gRPC receive admission full").into_http())
                })
            }
        };
        let codec = BoundedCodec::<wire::Response, wire::Request>::new(
            self.decoded_limit,
            None,
            vec![Box::new(permit)],
        );
        let method = Unary {
            service: self.service.clone(),
            authorize: self.authorize.clone(),
        };
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(codec)
                .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES);
            Ok(grpc.unary(method, request).await)
        })
    }
}
struct Unary {
    service: Arc<dyn RpcService>,
    authorize: Option<GrpcAuthorizer>,
}
impl tonic::server::UnaryService<Payload<wire::Request>> for Unary {
    type Response = Payload<Arc<wire::Response>>;
    type Future = Pin<
        Box<
            dyn std::future::Future<Output = Result<tonic::Response<Self::Response>, Status>>
                + Send,
        >,
    >;
    fn call(&mut self, request: tonic::Request<Payload<wire::Request>>) -> Self::Future {
        let service = self.service.clone();
        let authorize = self.authorize.clone();
        Box::pin(async move {
            if let Some(authorize) = authorize {
                authorize(
                    request.metadata(),
                    request.extensions(),
                    &request.get_ref().message,
                )?;
            }
            let reply = service
                .call(request.into_inner().into_request())
                .await
                .map_err(|e| match e {
                    RpcError::NotDispatched(e) => Status::unavailable(e),
                    error => Status::unknown(error.to_string()),
                })?;
            Ok(tonic::Response::new(Payload::from_reply(reply)))
        })
    }
}

struct LimitedTcp {
    stream: TcpStream,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Connected for LimitedTcp {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}
impl AsyncRead for LimitedTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for LimitedTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
