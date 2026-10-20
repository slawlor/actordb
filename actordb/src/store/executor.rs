// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Bounded ownership of synchronous backend operations.
use super::StoreError;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(crate) struct StorageExecutor {
    permits: Arc<Semaphore>,
    concurrency: usize,
}
impl StorageExecutor {
    pub(crate) fn new(concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(concurrency)),
            concurrency,
        })
    }
    /// Admission precedes spawn_blocking. Cancellation of the async waiter
    /// cannot release the permit or captures of a submitted synchronous call.
    pub(crate) async fn run<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, StoreError> + Send + 'static,
    {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| StoreError(error.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(|error| StoreError(format!("storage job failed: {error}")))?
    }
    /// Lifecycle calls this only after users have quiesced. Every submitted
    /// backend job must finish before an exclusive store binding is released.
    pub(crate) async fn drain(&self) {
        let _all = self
            .permits
            .clone()
            .acquire_many_owned(self.concurrency as u32)
            .await;
    }
    pub(crate) fn active(&self) -> usize {
        self.concurrency - self.permits.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};

    #[tokio::test]
    async fn cancelled_waiter_keeps_slot_until_backend_finishes_and_drain_waits() {
        let executor = StorageExecutor::new(1);
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_executor = executor.clone();
        let worker_entered = entered.clone();
        let worker_release = release.clone();
        let worker = tokio::spawn(async move {
            worker_executor
                .run(move || {
                    worker_entered.store(true, Ordering::Release);
                    let (lock, wake) = &*worker_release;
                    let mut done = lock.lock().unwrap();
                    while !*done {
                        done = wake.wait(done).unwrap();
                    }
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        worker.abort();
        let _ = worker.await;
        assert_eq!(executor.active(), 1);
        let draining = executor.clone();
        let drain = tokio::spawn(async move {
            draining.drain().await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!drain.is_finished());
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(executor.active(), 0);
    }
}
