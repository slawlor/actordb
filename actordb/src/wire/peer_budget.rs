//! Aggregate encoded-body admission shared by every physical peer connection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::{timeout_at, Instant};

use super::MAX_FRAME_BYTES;

pub(crate) const NODE_PEER_QUEUE_BYTE_CAPACITY: usize = 256 * 1024 * 1024;

/// Approximate node-wide encoded peer-writer admission usage.
///
/// Counts include reservations before encoding and bodies queued or currently
/// being written. They exclude protobuf objects, frame headers, allocator and
/// task overhead, cache/storage/replication payloads, and kernel socket buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerWriterDiagnostics {
    capacity_bytes: usize,
    admitted_bytes: usize,
    producer_admitted_bytes: usize,
    saturation_events: u64,
}
impl PeerWriterDiagnostics {
    /// Total encoded-body allowance across all physical peer connections.
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }
    /// Encoded bytes reserved, queued, or being written across the Node.
    pub fn admitted_bytes(&self) -> usize {
        self.admitted_bytes
    }
    /// Bytes reserved by ordinary producers, excluding priority responses.
    pub fn producer_admitted_bytes(&self) -> usize {
        self.producer_admitted_bytes
    }
    /// Failed immediate aggregate-byte acquisitions, including attempts that
    /// subsequently wait for capacity within their original deadline.
    pub fn saturation_events(&self) -> u64 {
        self.saturation_events
    }
}

pub(crate) struct GlobalBytePermits {
    pub(super) total: OwnedSemaphorePermit,
    pub(super) producer: Option<OwnedSemaphorePermit>,
}

pub(crate) struct NodePeerByteBudget {
    capacity: usize,
    total: Arc<Semaphore>,
    producers: Arc<Semaphore>,
    saturations: AtomicU64,
}
impl NodePeerByteBudget {
    /// Called only after Node runtime option validation; two maximum bodies
    /// fit so one full response remains reserved for readers.
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity >= 2 * MAX_FRAME_BYTES);
        Arc::new(Self {
            capacity,
            total: Arc::new(Semaphore::new(capacity)),
            producers: Arc::new(Semaphore::new(capacity - MAX_FRAME_BYTES)),
            saturations: AtomicU64::new(0),
        })
    }
    fn saturated(&self) {
        let _ = self
            .saturations
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            });
    }
    pub(crate) fn diagnostics(&self) -> PeerWriterDiagnostics {
        PeerWriterDiagnostics {
            capacity_bytes: self.capacity,
            admitted_bytes: self.capacity.saturating_sub(self.total.available_permits()),
            producer_admitted_bytes: (self.capacity - MAX_FRAME_BYTES)
                .saturating_sub(self.producers.available_permits()),
            saturation_events: self.saturations.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn try_response(&self, bytes: u32) -> Result<GlobalBytePermits, ()> {
        match self.total.clone().try_acquire_many_owned(bytes) {
            Ok(total) => Ok(GlobalBytePermits {
                total,
                producer: None,
            }),
            Err(TryAcquireError::NoPermits) => {
                self.saturated();
                Err(())
            }
            Err(TryAcquireError::Closed) => Err(()),
        }
    }
    async fn acquire(
        &self,
        semaphore: &Arc<Semaphore>,
        bytes: u32,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, ()> {
        if Instant::now() >= deadline {
            return Err(());
        }
        match semaphore.clone().try_acquire_many_owned(bytes) {
            Ok(permit) => Ok(permit),
            Err(TryAcquireError::Closed) => Err(()),
            Err(TryAcquireError::NoPermits) => {
                self.saturated();
                timeout_at(deadline, semaphore.clone().acquire_many_owned(bytes))
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())
            }
        }
    }
    pub(crate) async fn acquire_producer(
        &self,
        bytes: u32,
        deadline: Instant,
    ) -> Result<GlobalBytePermits, ()> {
        // Producers cannot consume the priority-response reserve. Acquire both
        // leases before allocating an encoded body, and keep them until Drop.
        let producer = self.acquire(&self.producers, bytes, deadline).await?;
        let total = self.acquire(&self.total, bytes, deadline).await?;
        Ok(GlobalBytePermits {
            total,
            producer: Some(producer),
        })
    }
}
