// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Structured ownership for background Tokio tasks.
//!
//! A single manager owns the [`JoinSet`] so completed children are reaped
//! promptly. Registration is acknowledged, shutdown closes registration
//! before aborting and joining every child, and dropping the owner aborts the
//! manager as a final safety net.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};

type BoxTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

enum TaskCommand {
    Spawn {
        task: BoxTask,
        registered: oneshot::Sender<bool>,
    },
}

/// Node-owned task set with race-free registration and shutdown.
pub(crate) struct RuntimeTasks {
    accepting: AtomicBool,
    commands: mpsc::Sender<TaskCommand>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    manager: StdMutex<Option<JoinHandle<()>>>,
    shutdown_lock: Mutex<()>,
}

impl RuntimeTasks {
    /// Start a task manager. `command_capacity` bounds concurrent internal
    /// registration requests; callers await space rather than detaching work.
    pub(crate) fn new(command_capacity: usize) -> Arc<Self> {
        assert!(
            command_capacity > 0,
            "task command capacity must be non-zero"
        );
        let (commands, receiver) = mpsc::channel(command_capacity);
        let (stop, stop_rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(false);
        let manager = tokio::spawn(run_task_manager(receiver, stop_rx, done_tx));
        Arc::new(Self {
            accepting: AtomicBool::new(true),
            commands,
            stop,
            done,
            manager: StdMutex::new(Some(manager)),
            shutdown_lock: Mutex::new(()),
        })
    }

    pub(crate) fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    pub(crate) fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }

    /// Synchronous, idempotent transition that closes future admission and
    /// wakes the manager. Useful from Drop paths that cannot await.
    pub(crate) fn request_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        let _ = self.stop.send(true);
    }

    /// Register a child. The result is true only after the manager has put the
    /// future in its JoinSet; a concurrent shutdown either owns and joins it or
    /// rejects it.
    pub(crate) async fn spawn<F>(&self, task: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if !self.is_accepting() {
            return false;
        }
        let (registered, registered_rx) = oneshot::channel();
        if self
            .commands
            .send(TaskCommand::Spawn {
                task: Box::pin(task),
                registered,
            })
            .await
            .is_err()
        {
            return false;
        }
        registered_rx.await.unwrap_or(false)
    }

    /// Stop accepting work, then abort and join every registered child.
    /// Serialized so repeated `Node::shutdown` calls wait for the same drain.
    pub(crate) async fn shutdown(&self) {
        let _shutdown_guard = self.shutdown_lock.lock().await;
        self.request_shutdown();

        let mut done = self.done.clone();
        while !*done.borrow() {
            if done.changed().await.is_err() {
                break;
            }
        }

        // Take the JoinHandle only after the manager has declared every child
        // stopped. Cancellation before this point leaves the handle available
        // to a later shutdown call rather than detaching live work.
        let manager = self.manager.lock().unwrap().take();
        if let Some(manager) = manager {
            if let Err(error) = manager.await {
                if !error.is_cancelled() {
                    tracing::warn!(%error, "runtime task manager failed");
                }
            }
        }
    }
}

impl Drop for RuntimeTasks {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Some(manager) = self.manager.lock().unwrap().take() {
            manager.abort();
        }
    }
}

async fn run_task_manager(
    mut commands: mpsc::Receiver<TaskCommand>,
    mut stop: watch::Receiver<bool>,
    done: watch::Sender<bool>,
) {
    struct PublishDone(watch::Sender<bool>);
    impl Drop for PublishDone {
        fn drop(&mut self) {
            let _ = self.0.send(true);
        }
    }

    let _publish_done = PublishDone(done.clone());
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            changed = stop.changed() => {
                let _ = changed;
                commands.close();
                tasks.shutdown().await;
                let _ = done.send(true);
                return;
            }
            command = commands.recv() => {
                match command {
                    Some(TaskCommand::Spawn { task, registered }) => {
                        tasks.spawn(task);
                        let _ = registered.send(true);
                    }
                    None => {
                        tasks.shutdown().await;
                        let _ = done.send(true);
                        return;
                    }
                }
            }
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    if !error.is_cancelled() {
                        tracing::warn!(%error, "runtime child task failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Condvar;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn shutdown_rejects_racing_registration_and_joins_children() {
        let tasks = RuntimeTasks::new(1);
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));

        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let child_started = started.clone();
        let child_dropped = dropped.clone();
        assert!(
            tasks
                .spawn(async move {
                    let _guard = DropCount(child_dropped);
                    child_started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        );
        started.notified().await;

        tasks.shutdown().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(!tasks.spawn(async {}).await);
    }

    #[tokio::test]
    async fn completed_children_do_not_block_later_registration() {
        let tasks = RuntimeTasks::new(1);
        let completed = Arc::new(AtomicUsize::new(0));
        for expected in 1..=32 {
            let task_completed = completed.clone();
            assert!(
                tasks
                    .spawn(async move {
                        task_completed.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            );
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while completed.load(Ordering::SeqCst) < expected {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        tasks.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_shutdown_waiter_does_not_detach_the_manager() {
        struct BlockingDrop {
            entered: Arc<AtomicBool>,
            release: Arc<(StdMutex<bool>, Condvar)>,
        }

        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                self.entered.store(true, Ordering::SeqCst);
                let (lock, ready) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
        }

        let tasks = RuntimeTasks::new(1);
        let started = Arc::new(Notify::new());
        let drop_entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let child_started = started.clone();
        let child_drop_entered = drop_entered.clone();
        let child_release = release.clone();
        assert!(
            tasks
                .spawn(async move {
                    let _guard = BlockingDrop {
                        entered: child_drop_entered,
                        release: child_release,
                    };
                    child_started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        );
        started.notified().await;

        let first_tasks = tasks.clone();
        let first_waiter = tokio::spawn(async move {
            first_tasks.shutdown().await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !drop_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("task manager did not begin joining its child");

        first_waiter.abort();
        assert!(first_waiter.await.unwrap_err().is_cancelled());
        {
            let (lock, ready) = &*release;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("replacement shutdown waiter did not join the manager");
        assert!(!tasks.spawn(async {}).await);
    }
}
