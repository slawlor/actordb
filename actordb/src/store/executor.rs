// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Bounded ownership of synchronous backend operations and their completion tasks.
use super::StoreError;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{oneshot, Mutex, Notify, Semaphore};
use tokio::task::JoinSet;

pub(crate) struct StorageExecutor {
    permits: Arc<Semaphore>,
    concurrency: usize,
    owners: StdMutex<JoinSet<()>>,
    owner_changes: Notify,
    drain_lock: Mutex<()>,
}
impl std::fmt::Debug for StorageExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageExecutor")
            .field("concurrency", &self.concurrency)
            .field("active", &self.active())
            .field("owned_tasks", &self.owned_tasks())
            .finish()
    }
}
impl StorageExecutor {
    pub(crate) fn new(concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(concurrency)),
            concurrency,
            owners: StdMutex::new(JoinSet::new()),
            owner_changes: Notify::new(),
            drain_lock: Mutex::new(()),
        })
    }

    /// Register synchronously before a submitted caller can yield. Each task
    /// keeps the executor and registry alive through its actual completion.
    /// Completed handles are reaped on subsequent admission or explicit drain.
    pub(crate) fn spawn_owner<F>(self: &Arc<Self>, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let keep_alive = self.clone();
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while let Some(completed) = owners.try_join_next() {
            Self::report_completion(completed);
        }
        owners.spawn(async move {
            let _keep_alive = keep_alive;
            task.await;
        });
        drop(owners);
        // A concurrent reaper can replace JoinSet's stored poll waker. Wake
        // drain on every registration so it registers its current waker again.
        self.owner_changes.notify_one();
    }

    fn report_completion(completed: Result<(), tokio::task::JoinError>) {
        if let Err(error) = completed {
            tracing::error!(%error, "storage completion owner failed");
        }
    }

    /// Admission precedes spawn_blocking. The registry owns completion even
    /// when this result waiter is canceled; the blocking call owns its permit.
    pub(crate) async fn run<T, F>(self: &Arc<Self>, operation: F) -> Result<T, StoreError>
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
        let (reply, completion) = oneshot::channel();
        self.spawn_owner(async move {
            let result = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                operation()
            })
            .await
            .map_err(|error| StoreError(format!("storage job failed: {error}")))
            .and_then(|result| result);
            let _ = reply.send(result);
        });
        completion
            .await
            .map_err(|error| StoreError(format!("storage completion owner stopped: {error}")))?
    }

    /// Lifecycle calls this only after users quiesce. Polls borrow the registry
    /// only briefly; nested owned jobs can register while drain is waiting.
    /// Canceling a drain leaves every remaining handle registered for a retry.
    pub(crate) async fn drain(&self) {
        let _single_drain = self.drain_lock.lock().await;
        loop {
            let changed = self.owner_changes.notified();
            let completed = tokio::select! {
                _ = changed => continue,
                completed = std::future::poll_fn(|cx| {
                    self.owners.lock().unwrap_or_else(|poison| poison.into_inner())
                        .poll_join_next(cx)
                }) => completed,
            };
            match completed {
                Some(completed) => Self::report_completion(completed),
                None => break,
            }
        }
        let _all = self
            .permits
            .clone()
            .acquire_many_owned(self.concurrency as u32)
            .await;
    }
    pub(crate) fn active(&self) -> usize {
        self.concurrency - self.permits.available_permits()
    }
    pub(crate) fn owned_tasks(&self) -> usize {
        self.owners
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
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
        drain.abort();
        let _ = drain.await;
        assert_eq!(executor.active(), 1);
        assert!(executor.owned_tasks() >= 1);
        let resumed = executor.clone();
        let drain = tokio::spawn(async move {
            resumed.drain().await;
        });
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(executor.active(), 0);
        assert_eq!(executor.owned_tasks(), 0);
    }
}
