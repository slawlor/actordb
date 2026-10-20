// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Shared retained-memory accounting. Charges include owned capacities and
//! conservative container overhead; they are admission bounds, not RSS meters.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ByteBudget {
    limit: usize,
    used: AtomicUsize,
}
impl ByteBudget {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    fn acquire(&self, bytes: usize) -> bool {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes).filter(|next| *next <= self.limit) else {
                return false;
            };
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(current) => used = current,
            }
        }
    }
    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Option<BytePermit> {
        self.acquire(bytes).then(|| BytePermit {
            budget: self.clone(),
            bytes,
        })
    }
}
#[derive(Debug)]
pub(crate) struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}
impl BytePermit {
    pub(crate) fn resize(&mut self, bytes: usize) -> bool {
        if bytes > self.bytes && !self.budget.acquire(bytes - self.bytes) {
            return false;
        }
        if bytes < self.bytes {
            self.budget
                .used
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        }
        self.bytes = bytes;
        true
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
