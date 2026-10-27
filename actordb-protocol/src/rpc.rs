//! Framework-neutral unary RPC boundary. Adapters own authentication and I/O.
//! No adapter may transparently replay a mutation. Decoding must be bounded
//! before allocating message collections, including opaque receipt payloads.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::{client, RpcResponseShape};

pub type RpcFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Adapter/service ownership carried until decoding, apply, or response encoding
/// finishes. Dropping a caller must not discard an already admitted mutation.
pub type RpcOwnership = Vec<Box<dyn Send + Sync + 'static>>;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RpcError {
    /// The adapter can prove that the operation was not dispatched.
    NotDispatched(String),
    /// Dispatch may have occurred; mutation callers must report uncertainty.
    Failure(String),
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDispatched(s) | Self::Failure(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for RpcError {}

#[derive(Debug, Clone)]
pub struct RpcCallOptions {
    pub deadline: Instant,
    pub max_decoded_response_bytes: usize,
    pub response_shape: RpcResponseShape,
}

/// A typed request plus the adapter's retained receive/admission ownership.
pub struct RpcRequest {
    request: client::Request,
    ownership: RpcOwnership,
}
impl RpcRequest {
    pub fn new(request: client::Request) -> Self {
        Self {
            request,
            ownership: Vec::new(),
        }
    }
    pub fn retain<T: Send + Sync + 'static>(&mut self, owner: T) {
        self.ownership.push(Box::new(owner));
    }
    pub fn into_parts(self) -> (client::Request, RpcOwnership) {
        (self.request, self.ownership)
    }
    pub fn from_parts(request: client::Request, ownership: RpcOwnership) -> Self {
        Self { request, ownership }
    }
}
impl std::ops::Deref for RpcRequest {
    type Target = client::Request;
    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

/// A response retains server drain or client materialization ownership until
/// the adapter/body or SDK conversion completes. Returned user values are then
/// caller-owned and outside the RPC materialization allowance.
pub struct RpcReply {
    pub response: client::Response,
    ownership: RpcOwnership,
}
impl RpcReply {
    pub fn new(response: client::Response) -> Self {
        Self {
            response,
            ownership: Vec::new(),
        }
    }
    pub fn retain<T: Send + Sync + 'static>(&mut self, owner: T) {
        self.ownership.push(Box::new(owner));
    }
    pub fn into_parts(self) -> (client::Response, RpcOwnership) {
        (self.response, self.ownership)
    }
    pub fn from_parts(response: client::Response, ownership: RpcOwnership) -> Self {
        Self {
            response,
            ownership,
        }
    }
}
impl std::fmt::Debug for RpcReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcReply").finish_non_exhaustive()
    }
}
impl std::ops::Deref for RpcReply {
    type Target = client::Response;
    fn deref(&self) -> &Self::Target {
        &self.response
    }
}
impl std::ops::DerefMut for RpcReply {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

/// Object-safe client adapter: gRPC, Thrift, or another unary RPC framework.
pub trait RpcClient: Send + Sync + 'static {
    fn call(
        &self,
        request: Arc<client::Request>,
        options: RpcCallOptions,
    ) -> RpcFuture<'_, Result<RpcReply, RpcError>>;
}

/// Object-safe server adapter target. Authentication/ACL decisions happen in
/// the framework adapter before it calls this service, never in this trait.
pub trait RpcService: Send + Sync + 'static {
    fn call(&self, request: RpcRequest) -> RpcFuture<'_, Result<RpcReply, RpcError>>;
}
