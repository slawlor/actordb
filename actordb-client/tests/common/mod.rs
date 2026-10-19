// Shared helpers for actordb-client integration tests.

use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;

/// Poll `check` until it returns true or `timeout` elapses (panics on timeout).
pub async fn wait_for<F, Fut>(check: F, timeout: Duration)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    assert!(
        tokio::time::timeout(timeout, async {
            loop {
                if check().await {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .is_ok(),
        "condition not met within {timeout:?}"
    );
}
