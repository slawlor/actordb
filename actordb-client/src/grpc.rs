//! Optional gRPC client adapter. Metadata/authentication and TLS configuration
//! belong here, not in the transport-neutral SDK or RPC trait.

use super::{
    Client, ClientError, ClientOptions, RpcCallOptions, RpcClient, RpcError, RpcFuture, RpcReply,
};
use actordb_protocol::{
    client as wire,
    grpc::{BoundedCodec, Payload},
    RPC_CALL_PATH,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone, Default)]
pub struct GrpcClientOptions {
    /// Framework-native metadata, including caller-provided bearer credentials.
    pub metadata: MetadataMap,
}
impl std::fmt::Debug for GrpcClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClientOptions")
            .field("metadata", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    channel: Channel,
    options: GrpcClientOptions,
}
impl GrpcClient {
    /// Use a caller-configured gRPC channel (TLS roots, client certificates,
    /// connection policy). No separate ActorDB authentication is performed.
    pub fn from_channel(channel: Channel, options: GrpcClientOptions) -> Self {
        Self { channel, options }
    }
    pub async fn connect(
        endpoint: impl Into<String>,
        options: GrpcClientOptions,
        timeout: Duration,
    ) -> Result<Self, RpcError> {
        if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(RpcError::NotDispatched(
                "invalid gRPC connect deadline".into(),
            ));
        }
        let endpoint = Endpoint::from_shared(endpoint.into())
            .map_err(|e| RpcError::NotDispatched(e.to_string()))?;
        let channel = endpoint
            .connect_timeout(timeout)
            .connect()
            .await
            .map_err(|e| RpcError::NotDispatched(e.to_string()))?;
        Ok(Self::from_channel(channel, options))
    }
}
impl RpcClient for GrpcClient {
    fn call(
        &self,
        request: Arc<wire::Request>,
        options: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>> {
        let channel = self.channel.clone();
        let metadata = self.options.metadata.clone();
        Box::pin(async move {
            if options.deadline <= std::time::Instant::now() {
                return Err(RpcError::NotDispatched(
                    "gRPC deadline elapsed before dispatch".into(),
                ));
            }
            let mut grpc = tonic::client::Grpc::new(channel)
                .max_decoding_message_size(actordb_protocol::MAX_RPC_MESSAGE_BYTES)
                .max_encoding_message_size(actordb_protocol::MAX_RPC_MESSAGE_BYTES);
            grpc.ready()
                .await
                .map_err(|e| RpcError::NotDispatched(e.to_string()))?;
            let remaining = options
                .deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or_else(|| {
                    RpcError::NotDispatched("gRPC deadline elapsed before dispatch".into())
                })?;
            let mut call = tonic::Request::new(Payload::new(request));
            *call.metadata_mut() = metadata;
            call.set_timeout(remaining);
            let codec = BoundedCodec::<wire::Request, wire::Response>::new(
                options.max_decoded_response_bytes,
                Some(options.response_shape),
                Vec::new(),
            );
            let path = tonic::codegen::http::uri::PathAndQuery::from_static(RPC_CALL_PATH);
            let response = grpc.unary(call, path, codec).await.map_err(|status| {
                // These statuses originate in the adapter before Node admission.
                // Other statuses, including decoder/resource failures, can follow
                // a committed mutation and must never be called "not sent".
                if matches!(
                    status.code(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                ) {
                    RpcError::NotDispatched(status.to_string())
                } else {
                    RpcError::Failure(status.to_string())
                }
            })?;
            Ok(response.into_inner().into_reply())
        })
    }
}

impl Client {
    /// Convenience factory for default gRPC. Supply a full http(s) endpoint;
    /// configure TLS/metadata explicitly via GrpcClient for authenticated use.
    pub async fn connect(
        endpoint: impl Into<String>,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        Self::validate_options(&options)?;
        let rpc = tokio::time::timeout(
            options.timeout,
            GrpcClient::connect(endpoint, GrpcClientOptions::default(), options.timeout),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::Transport(e.to_string()))?;
        Self::from_rpc(Arc::new(rpc), options)
    }
}
