//! Bounded Prost codec for the optional unary gRPC adapter.

use crate::{
    client,
    rpc::{RpcOwnership, RpcReply, RpcRequest},
    RpcResponseShape, DEFAULT_MAX_DECODED_RESPONSE_BYTES, MAX_RPC_MESSAGE_BYTES,
};
use bytes::Buf;
use prost::Message;
use std::marker::PhantomData;
use std::sync::Arc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

pub struct Payload<T> {
    pub message: T,
    ownership: RpcOwnership,
}
impl<T> Payload<T> {
    pub fn new(message: T) -> Self {
        Self {
            message,
            ownership: Vec::new(),
        }
    }
}
impl Payload<client::Request> {
    pub fn into_request(self) -> RpcRequest {
        RpcRequest::from_parts(self.message, self.ownership)
    }
}
impl Payload<client::Response> {
    pub fn into_reply(self) -> RpcReply {
        RpcReply::from_parts(self.message, self.ownership)
    }
}
impl Payload<Arc<client::Response>> {
    pub fn from_reply(reply: RpcReply) -> Self {
        let (message, ownership) = reply.into_parts();
        Self {
            message: Arc::new(message),
            ownership,
        }
    }
}

pub trait BoundedMessage: Message + Default + Send + 'static {
    fn preflight(
        bytes: &[u8],
        maximum: usize,
        shape: Option<&RpcResponseShape>,
    ) -> Result<(), &'static str>;
}
impl BoundedMessage for client::Request {
    fn preflight(
        bytes: &[u8],
        maximum: usize,
        _: Option<&RpcResponseShape>,
    ) -> Result<(), &'static str> {
        crate::preflight_rpc_request(bytes, maximum).map(|_| ())
    }
}
impl BoundedMessage for client::Response {
    fn preflight(
        bytes: &[u8],
        maximum: usize,
        shape: Option<&RpcResponseShape>,
    ) -> Result<(), &'static str> {
        match shape {
            Some(shape) => crate::preflight_rpc_response(bytes, maximum, shape),
            None => crate::preflight_rpc_response_unchecked_shape(bytes, maximum),
        }
        .map(|_| ())
    }
}

pub struct BoundedCodec<E, D> {
    maximum: usize,
    shape: Option<RpcResponseShape>,
    receive_ownership: RpcOwnership,
    marker: PhantomData<(E, D)>,
}
impl<E, D> Default for BoundedCodec<E, D> {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_DECODED_RESPONSE_BYTES, None, Vec::new())
    }
}
impl<E, D> BoundedCodec<E, D> {
    pub fn new(
        maximum: usize,
        shape: Option<RpcResponseShape>,
        receive_ownership: RpcOwnership,
    ) -> Self {
        Self {
            maximum,
            shape,
            receive_ownership,
            marker: PhantomData,
        }
    }
}
impl<E: Message + Send + 'static, D: BoundedMessage> Codec for BoundedCodec<E, D> {
    type Encode = Payload<Arc<E>>;
    type Decode = Payload<D>;
    type Encoder = BoundedEncoder<E>;
    type Decoder = BoundedDecoder<D>;
    fn encoder(&mut self) -> Self::Encoder {
        BoundedEncoder {
            ownership: Vec::new(),
            marker: PhantomData,
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        BoundedDecoder {
            maximum: self.maximum,
            shape: self.shape,
            ownership: std::mem::take(&mut self.receive_ownership),
            decoded: false,
            marker: PhantomData,
        }
    }
}
pub struct BoundedEncoder<T> {
    ownership: RpcOwnership,
    marker: PhantomData<T>,
}
impl<T: Message> Encoder for BoundedEncoder<T> {
    type Item = Payload<Arc<T>>;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        if item.message.encoded_len() > MAX_RPC_MESSAGE_BYTES {
            return Err(Status::resource_exhausted(
                "RPC response exceeds byte limit",
            ));
        }
        // The encoder belongs to the body: retain drain ownership through its
        // full lifecycle rather than dropping it immediately after encoding.
        self.ownership.extend(item.ownership);
        item.message
            .encode(dst)
            .map_err(|e| Status::internal(e.to_string()))
    }
}
pub struct BoundedDecoder<T> {
    maximum: usize,
    shape: Option<RpcResponseShape>,
    ownership: RpcOwnership,
    decoded: bool,
    marker: PhantomData<T>,
}
impl<T: BoundedMessage> Decoder for BoundedDecoder<T> {
    type Item = Payload<T>;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Status> {
        if self.decoded {
            return Err(Status::invalid_argument(
                "unary RPC accepts exactly one message",
            ));
        }
        T::preflight(src.chunk(), self.maximum, self.shape.as_ref())
            .map_err(Status::resource_exhausted)?;
        let message = T::decode(src).map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.decoded = true;
        Ok(Some(Payload {
            message,
            ownership: std::mem::take(&mut self.ownership),
        }))
    }
}
