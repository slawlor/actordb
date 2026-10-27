//! An independent raw-response gRPC fixture: no decoded producer collections,
//! database node, application handshake, or request-correlation state.
use actordb_protocol::{
    client as wire,
    grpc::{BoundedCodec, BoundedDecoder, Payload},
    RPC_CALL_PATH, RPC_SERVICE_NAME,
};
use bytes::BufMut;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::codec::{Codec, EncodeBuf, Encoder};
use tonic::codegen::http;
use tonic::{Request, Response, Status};

type Handler = Arc<dyn Fn(&wire::Request) -> Vec<u8> + Send + Sync>;
pub struct RawServer {
    pub addr: std::net::SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}
impl RawServer {
    pub async fn start(
        handler: impl Fn(&wire::Request) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(tcp);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RawService(Arc::new(handler)))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            addr,
            stop: Some(stop),
            task,
        }
    }
    pub async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.await.unwrap();
    }
}
#[derive(Clone)]
struct RawService(Handler);
impl tonic::server::NamedService for RawService {
    const NAME: &'static str = RPC_SERVICE_NAME;
}
impl<B> tonic::codegen::Service<http::Request<B>> for RawService
where
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        assert_eq!(request.uri().path(), RPC_CALL_PATH);
        let method = Method(self.0.clone());
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(RawCodec)
                .max_encoding_message_size(actordb_protocol::MAX_RPC_MESSAGE_BYTES)
                .unary(method, request)
                .await)
        })
    }
}
struct Method(Handler);
impl tonic::server::UnaryService<Payload<wire::Request>> for Method {
    type Response = Vec<u8>;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Response<Vec<u8>>, Status>> + Send>>;
    fn call(&mut self, request: Request<Payload<wire::Request>>) -> Self::Future {
        let bytes = (self.0)(&request.into_inner().message);
        Box::pin(async move { Ok(Response::new(bytes)) })
    }
}
struct RawCodec;
impl Codec for RawCodec {
    type Encode = Vec<u8>;
    type Decode = Payload<wire::Request>;
    type Encoder = RawEncoder;
    type Decoder = BoundedDecoder<wire::Request>;
    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        BoundedCodec::<wire::Response, wire::Request>::default().decoder()
    }
}
struct RawEncoder;
impl Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = Status;
    fn encode(&mut self, bytes: Vec<u8>, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&bytes);
        Ok(())
    }
}
