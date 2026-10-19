//! Explicit per-write acknowledgement policy; no automatic mutation retries.
use super::*;
use actordb_protocol::write_concern::{WriteConcernOutcome, WriteConcernPolicyError};

#[derive(Debug)]
#[non_exhaustive]
pub enum WriteConcernClientError {
    Operation(ClientError),
    InvalidPolicy(String),
    Unsupported(String),
    TimedOut(WriteConcernReceipt),
    Backpressure,
    UnknownOutcome(String),
}
impl fmt::Display for WriteConcernClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(e) => fmt::Display::fmt(e, f),
            Self::InvalidPolicy(e) | Self::Unsupported(e) | Self::UnknownOutcome(e) => {
                f.write_str(e)
            }
            Self::TimedOut(receipt) => write!(
                f,
                "write applied at {}; {} of {} acknowledgements received",
                receipt.receiving_node(),
                receipt.acknowledged_nodes().len(),
                receipt.required_acknowledgements()
            ),
            Self::Backpressure => f.write_str("write concern waiter capacity exhausted"),
        }
    }
}
impl std::error::Error for WriteConcernClientError {}
impl From<ClientError> for WriteConcernClientError {
    fn from(e: ClientError) -> Self {
        Self::Operation(e)
    }
}
impl From<WriteConcernPolicyError> for WriteConcernClientError {
    fn from(e: WriteConcernPolicyError) -> Self {
        match e {
            WriteConcernPolicyError::Invalid(e) => Self::InvalidPolicy(e),
            WriteConcernPolicyError::Unsupported(e) => Self::Unsupported(e),
            _ => Self::Unsupported(e.to_string()),
        }
    }
}

impl Client {
    pub async fn put_with_write_concern(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        concern: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernClientError> {
        self.put_with_write_concern_options(key, value, PutOptions::default(), concern)
            .await
    }
    pub async fn put_with_write_concern_options(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        options: PutOptions,
        concern: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernClientError> {
        let ttl_ms = options
            .ttl
            .map(|ttl| {
                u64::try_from(ttl.as_millis())
                    .map_err(|_| ClientError::Invalid("TTL exceeds milliseconds".into()))
            })
            .transpose()?;
        let encoded = concern.encode()?;
        let op = wire::request::Operation::Put(wire::Put {
            key: key.into(),
            value,
            ttl_ms,
            priority: options.priority as i32,
            write_concern: Some(encoded),
        });
        self.concern_request(op, concern).await
    }
    pub async fn delete_with_write_concern(
        &self,
        key: impl Into<String>,
        concern: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernClientError> {
        self.delete_with_write_concern_options(key, Priority::Default, concern)
            .await
    }
    pub async fn delete_with_write_concern_options(
        &self,
        key: impl Into<String>,
        priority: Priority,
        concern: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernClientError> {
        let encoded = concern.encode()?;
        let op = wire::request::Operation::Delete(wire::Delete {
            key: key.into(),
            priority: priority as i32,
            write_concern: Some(encoded),
        });
        self.concern_request(op, concern).await
    }
    async fn concern_request(
        &self,
        operation: wire::request::Operation,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernClientError> {
        if self.inner.capabilities & actordb_protocol::CLIENT_CAPABILITY_WRITE_CONCERNS == 0 {
            return Err(WriteConcernClientError::Unsupported(
                "server did not negotiate write concerns".into(),
            ));
        }
        let mut response = self
            .request_with_timeout(operation, true, options.timeout())
            .await?;
        if response.mutations.len() != 1 {
            return Err(WriteConcernClientError::UnknownOutcome(
                "missing mutation acknowledgement".into(),
            ));
        }
        let result = response.mutations.remove(0);
        let Some(bytes) = result.write_concern.as_deref() else {
            if let Some(error) = result.error {
                return Err(server_error(error).into());
            }
            return Err(WriteConcernClientError::UnknownOutcome(
                "server omitted write concern receipt".into(),
            ));
        };
        match WriteConcernOutcome::decode(bytes).map_err(|e| {
            WriteConcernClientError::UnknownOutcome(format!("invalid concern reply: {e}"))
        })? {
            WriteConcernOutcome::Satisfied(receipt) => {
                if result.error.is_some() {
                    return Err(WriteConcernClientError::UnknownOutcome(
                        "success receipt conflicts with mutation error".into(),
                    ));
                }
                let expected = options
                    .concern()
                    .required_acknowledgements(receipt.replica_count().unwrap_or(0) as usize)?;
                if receipt.requested_acknowledgements() != expected {
                    return Err(WriteConcernClientError::UnknownOutcome(
                        "server changed requested acknowledgement count".into(),
                    ));
                }
                Ok(receipt)
            }
            WriteConcernOutcome::TimedOut(receipt) => {
                Err(WriteConcernClientError::TimedOut(receipt))
            }
            WriteConcernOutcome::InvalidPolicy(e) => Err(WriteConcernClientError::InvalidPolicy(e)),
            WriteConcernOutcome::Unsupported(e) => Err(WriteConcernClientError::Unsupported(e)),
            WriteConcernOutcome::Backpressure => Err(WriteConcernClientError::Backpressure),
            _ => Err(WriteConcernClientError::UnknownOutcome(
                "unsupported concern reply".into(),
            )),
        }
    }
}
