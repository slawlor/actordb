// Shared helpers for actordb integration tests.

use std::future::Future;
use std::time::{Duration, Instant};

use tokio::time::sleep;

/// Poll `check` until it returns true or `timeout` elapses (panics on timeout).
pub async fn wait_for<F, Fut>(check: F, timeout: Duration)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(check().await, "condition not met within {timeout:?}");
}
