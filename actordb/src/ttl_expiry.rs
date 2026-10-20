// Copyright (c) Sean Lawlor
//
// This source code is licensed under the MIT license in the repository root.

//! Optional, best-effort notifications of local TTL cleanup transitions.
//!
//! Register a handler through [`NodeConfig`](crate::node::NodeConfig). Events
//! contain owned, opaque value bytes and the original LWW version. A notification
//! means a local live version became its expiry tombstone; it establishes neither
//! quorum commitment nor durable callback delivery. Replicas can report the same
//! version, so deduplicate by [`ExpiryEvent::id`](crate::ttl_expiry::ExpiryEvent::id) and compare record versions
//! before clearing application state: a new value can supersede an expired version
//! before its callback executes. Cleanup may run after the nominal expiry time.
//!
//! Delivery is bounded and ordered within each fragment, with no automatic retry,
//! exactly-once guarantee, restart replay, or cluster-wide order. Recipient
//! selection, ownership filtering, networking, and application semantics belong
//! to the caller. Stronger delivery requires a separate durable outbox/replay design.

pub use futures::future::BoxFuture;
use futures::FutureExt;
use sha2::{Digest, Sha256};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::actor::VersionedValue;
use crate::shard::FragmentId;

/// Stable SHA-256 identity of a fragment, UTF-8 key and original LWW version.
/// Local node identity and callback timing are deliberately excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExpiryEventId([u8; 32]);

impl ExpiryEventId {
    /// Bytes suitable for an application's deduplication storage or protocol.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn for_record(fragment: FragmentId, key: &str, value: &VersionedValue) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"actordb-ttl-expiry-v1\0");
        hash.update(fragment.0.to_be_bytes());
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(key.as_bytes());
        hash.update(value.timestamp_ms.to_be_bytes());
        hash.update((value.node_id.len() as u64).to_be_bytes());
        hash.update(value.node_id.as_bytes());
        Self(hash.finalize().into())
    }
}

impl fmt::Display for ExpiryEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One successful local conversion of the current expired live version.
/// The previous value owns its opaque bytes; ActorDB imposes no payload format.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExpiryEvent {
    /// Stable identity shared by replicas expiring the same original version.
    pub id: ExpiryEventId,
    /// Fragment of this local record copy.
    pub fragment: FragmentId,
    /// Owned key in the existing UTF-8 namespace.
    pub key: String,
    /// Original live record, including payload, timestamp and writer identity.
    pub previous_value: VersionedValue,
    /// Scheduled wall-clock expiry in milliseconds since the Unix epoch.
    pub expires_at_ms: u64,
    /// Wall clock observed at materialization, independently of a pass's pinned cutoff.
    pub observed_at_ms: u64,
}

impl ExpiryEvent {
    /// Original `(timestamp_ms, node_id)` LWW version, using the existing types.
    pub fn original_version(&self) -> (u64, &str) {
        (
            self.previous_value.timestamp_ms,
            &self.previous_value.node_id,
        )
    }
}

// Avoid accidentally printing opaque values or keys in diagnostic/debug output.
impl fmt::Debug for ExpiryEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpiryEvent")
            .field("id", &self.id)
            .field("fragment", &self.fragment)
            .field("key_bytes", &self.key.len())
            .field("value_bytes", &self.previous_value.value.len())
            .field("original_version", &self.original_version())
            .field("expires_at_ms", &self.expires_at_ms)
            .field("observed_at_ms", &self.observed_at_ms)
            .finish()
    }
}

/// Owned callback context. Batches may contain several fragments; each fragment's
/// events retain their local materialization order. Handler calls never overlap.
#[derive(Debug)]
#[non_exhaustive]
pub struct ExpiryBatch {
    /// Identity of the Node performing cleanup, which can differ from the writer.
    pub local_node_id: String,
    /// Wall clock observed immediately before invoking the handler.
    pub dispatched_at_ms: u64,
    /// Events in materialization order within each fragment.
    pub events: Vec<ExpiryEvent>,
}

/// Caller-reported callback failure. Its contents are never logged by ActorDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryCallbackError(pub String);

impl ExpiryCallbackError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ExpiryCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ExpiryCallbackError {}

/// Application-owned asynchronous batch handler.
///
/// Invoked outside database locks and mutation workers. Errors and unwind panics
/// are isolated; ActorDB drops timed-out futures and never retries them. A handler
/// may have partially acted before error, panic or cancellation. Handlers must
/// cooperate with async cancellation and must not block a runtime thread or
/// detach work: Rust cannot preempt synchronous blocking code or abort-mode panics.
/// Data retained or cloned by the application is outside ActorDB's memory budget.
pub trait TtlExpiryHandler: Send + Sync + 'static {
    fn on_expired(&self, batch: ExpiryBatch) -> BoxFuture<'_, Result<(), ExpiryCallbackError>>;
}

/// Closure adapter returned by [`expiry_handler_fn`].
pub struct ClosureTtlExpiryHandler<F>(F);

/// Register an async closure without implementing [`TtlExpiryHandler`] manually.
/// Clone captured application handles before entering an `async move` block.
///
/// ```
/// use actordb::ttl_expiry::{expiry_handler_fn, ExpiryBatch, ExpiryCallbackError};
/// let handler = expiry_handler_fn(|batch: ExpiryBatch| async move {
///     for event in batch.events {
///         let _identity = event.id;
///         let _opaque_payload = event.previous_value.value;
///     }
///     Ok::<(), ExpiryCallbackError>(())
/// });
/// ```
pub fn expiry_handler_fn<F, Fut>(handler: F) -> ClosureTtlExpiryHandler<F>
where
    F: Fn(ExpiryBatch) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ExpiryCallbackError>> + Send + 'static,
{
    ClosureTtlExpiryHandler(handler)
}

impl<F, Fut> TtlExpiryHandler for ClosureTtlExpiryHandler<F>
where
    F: Fn(ExpiryBatch) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ExpiryCallbackError>> + Send + 'static,
{
    fn on_expired(&self, batch: ExpiryBatch) -> BoxFuture<'_, Result<(), ExpiryCallbackError>> {
        Box::pin((self.0)(batch))
    }
}

/// Limits for the optional Node-owned dispatcher. Queue budgets include collected
/// batches, in-flight callbacks and staged startup events, until delivery finishes.
/// Bytes conservatively charge event metadata and owned allocation capacities;
/// allocator overhead and application-retained data are not an exact heap bound.
#[derive(Debug, Clone, bon::Builder)]
pub struct TtlExpiryOptions {
    /// Maximum retained notifications, including in-flight and startup staging.
    /// Defaults to 1024.
    #[builder(default = 1024)]
    pub queue_event_capacity: usize,
    /// Maximum charged retained bytes. Defaults to 8 MiB.
    #[builder(default = 8 * 1024 * 1024)]
    pub queue_byte_capacity: usize,
    /// Maximum events in one call. Defaults to 64.
    #[builder(default = 64)]
    pub batch_size: usize,
    /// Maximum collection delay from the first event's enqueue time. Backlog or
    /// an executing handler can delay dispatch further; this is not a delivery SLA.
    #[builder(default = Duration::from_millis(20))]
    pub max_batch_delay: Duration,
    /// Per-call cooperative execution deadline. Defaults to five seconds.
    #[builder(default = Duration::from_secs(5))]
    pub handler_timeout: Duration,
    /// One overall deadline after final fragment cleanup, including active handlers.
    /// Defaults to five seconds; this does not bound database persistence shutdown.
    #[builder(default = Duration::from_secs(5))]
    pub shutdown_drain_timeout: Duration,
}

impl Default for TtlExpiryOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl TtlExpiryOptions {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.queue_event_capacity == 0
            || self.queue_event_capacity > Semaphore::MAX_PERMITS
            || self.queue_byte_capacity == 0
            || self.queue_byte_capacity > Semaphore::MAX_PERMITS.min(u32::MAX as usize)
            || self.batch_size == 0
            || self.batch_size > self.queue_event_capacity
        {
            return Err("invalid TTL expiry queue capacities or batch size".into());
        }
        let now = Instant::now();
        for duration in [
            self.max_batch_delay,
            self.handler_timeout,
            self.shutdown_drain_timeout,
        ] {
            if duration.is_zero() || now.checked_add(duration).is_none() {
                return Err("TTL expiry deadlines must be nonzero and representable".into());
            }
        }
        Ok(())
    }
}

/// Local, approximate diagnostic snapshot. Outcome counters are cumulative.
/// Failed events had an error/panic; abandoned events were canceled by timeout,
/// drain expiry, owner Drop, or failed startup. Neither excludes partial side effects.
/// Dropped events were rejected before dispatch. No keys or payloads are logged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExpiryDiagnostics {
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub in_flight_events: usize,
    pub in_flight_bytes: usize,
    pub retained_events: usize,
    pub retained_bytes: usize,
    pub enqueued_events: u64,
    pub dispatched_batches: u64,
    /// Events in calls that returned `Ok(())`; not recipient acknowledgements.
    pub delivered_events: u64,
    pub failed_events: u64,
    pub abandoned_events: u64,
    pub dropped_events: u64,
    pub dropped_event_capacity: u64,
    pub dropped_byte_capacity: u64,
    pub dropped_closed: u64,
    pub handler_errors: u64,
    pub handler_panics: u64,
    pub handler_timeouts: u64,
    pub latency_samples: u64,
    /// Wall-clock differences saturate at zero if clocks move backwards.
    pub expiry_to_dispatch_total_ms: u64,
    pub expiry_to_dispatch_max_ms: u64,
    /// Monotonic residence from admission, including startup staging, batch
    /// collection and preceding calls.
    pub queue_to_dispatch_total_ms: u64,
    pub queue_to_dispatch_max_ms: u64,
}

#[derive(Default)]
struct Metrics {
    queued_events: AtomicUsize,
    queued_bytes: AtomicUsize,
    in_flight_events: AtomicUsize,
    in_flight_bytes: AtomicUsize,
    enqueued_events: AtomicU64,
    dispatched_batches: AtomicU64,
    delivered_events: AtomicU64,
    failed_events: AtomicU64,
    abandoned_events: AtomicU64,
    dropped_events: AtomicU64,
    dropped_event_capacity: AtomicU64,
    dropped_byte_capacity: AtomicU64,
    dropped_closed: AtomicU64,
    handler_errors: AtomicU64,
    handler_panics: AtomicU64,
    handler_timeouts: AtomicU64,
    latency_samples: AtomicU64,
    expiry_to_dispatch_total_ms: AtomicU64,
    expiry_to_dispatch_max_ms: AtomicU64,
    queue_to_dispatch_total_ms: AtomicU64,
    queue_to_dispatch_max_ms: AtomicU64,
}

fn add(counter: &AtomicU64, value: u64) -> u64 {
    // fetch_update is deprecated on current stable; its replacement is newer
    // than our MSRV. Keep the same saturating operation using the portable CAS.
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(value);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

type WallClock = Arc<dyn Fn() -> u64 + Send + Sync>;

struct Control {
    accepting: AtomicBool,
    event_permits: Arc<Semaphore>,
    byte_permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    options: TtlExpiryOptions,
    clock: WallClock,
}

impl Control {
    fn dropped(&self, reason: &'static str) {
        let counter = match reason {
            "event_capacity" => &self.metrics.dropped_event_capacity,
            "byte_capacity" => &self.metrics.dropped_byte_capacity,
            _ => &self.metrics.dropped_closed,
        };
        add(counter, 1);
        let count = add(&self.metrics.dropped_events, 1);
        // Logarithmic diagnostics keep a saturated queue from flooding cleanup logs.
        if count.is_power_of_two() {
            tracing::warn!(
                dropped_events = count,
                reason,
                "TTL expiry notifications dropped"
            );
        }
    }
}

/// Opaque transport into the Node-owned dispatcher, threaded through public actor
/// arguments. Applications register handlers with NodeConfig rather than minting events.
#[doc(hidden)]
#[derive(Clone)]
pub struct ExpiryPublisher {
    sender: mpsc::Sender<QueuedExpiry>,
    control: Arc<Control>,
}

pub(crate) struct PreparedExpiry {
    event: QueuedExpiry,
    publisher: ExpiryPublisher,
}

impl PreparedExpiry {
    pub(crate) fn publish(mut self) {
        if !self.publisher.control.accepting.load(Ordering::Acquire) {
            self.publisher.control.dropped("closed");
            self.event.budget.outcome = Some(Outcome::Dropped);
            return;
        }
        self.event.budget.queued();
        match self.publisher.sender.try_send(self.event) {
            Ok(()) => {
                add(&self.publisher.control.metrics.enqueued_events, 1);
            }
            Err(error) => {
                let reason = if matches!(&error, mpsc::error::TrySendError::Full(_)) {
                    "event_capacity"
                } else {
                    "closed"
                };
                self.publisher.control.dropped(reason);
                error.into_inner().budget.outcome = Some(Outcome::Dropped);
            }
        }
    }
}

impl ExpiryPublisher {
    pub(crate) fn prepare(
        &self,
        fragment: FragmentId,
        key: &str,
        previous_value: VersionedValue,
    ) -> Option<PreparedExpiry> {
        self.prepare_with_metadata(fragment, key, previous_value, 0)
    }

    pub(crate) fn prepare_startup(
        &self,
        fragment: FragmentId,
        key: &str,
        previous_value: VersionedValue,
    ) -> Option<PreparedExpiry> {
        // Startup stages only the final current row per key, before publishing
        // after its storage fence. Charge the extra staging index/key as well.
        self.prepare_with_metadata(
            fragment,
            key,
            previous_value,
            key.len()
                .saturating_add(3 * std::mem::size_of::<PreparedExpiry>())
                .saturating_add(std::mem::size_of::<String>()),
        )
    }

    fn prepare_with_metadata(
        &self,
        fragment: FragmentId,
        key: &str,
        previous_value: VersionedValue,
        extra_bytes: usize,
    ) -> Option<PreparedExpiry> {
        let expires_at_ms = previous_value.expires_at_ms?;
        debug_assert!(!previous_value.is_tombstone);
        if !self.control.accepting.load(Ordering::Acquire) {
            self.control.dropped("closed");
            return None;
        }
        let bytes = std::mem::size_of::<QueuedExpiry>()
            .saturating_add(64)
            .saturating_add(extra_bytes)
            .saturating_add(key.len())
            .saturating_add(previous_value.value.capacity())
            .saturating_add(previous_value.node_id.capacity());
        let Ok(events) = self.control.event_permits.clone().try_acquire_owned() else {
            self.control.dropped("event_capacity");
            return None;
        };
        let byte_count = match u32::try_from(bytes) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.control.dropped("byte_capacity");
                return None;
            }
        };
        let Ok(byte_permit) = self
            .control
            .byte_permits
            .clone()
            .try_acquire_many_owned(byte_count)
        else {
            self.control.dropped("byte_capacity");
            return None;
        };
        // Admission precedes key copying and hashing. The payload itself moves.
        let event = ExpiryEvent {
            id: ExpiryEventId::for_record(fragment, key, &previous_value),
            fragment,
            key: key.to_owned(),
            previous_value,
            expires_at_ms,
            observed_at_ms: (self.control.clock)(),
        };
        Some(PreparedExpiry {
            event: QueuedExpiry {
                event,
                queued_at: Instant::now(),
                budget: Budget {
                    _events: events,
                    _bytes: byte_permit,
                    bytes,
                    metrics: self.control.metrics.clone(),
                    state: BudgetState::Staged,
                    outcome: None,
                },
            },
            publisher: self.clone(),
        })
    }

    pub(crate) fn offer(&self, fragment: FragmentId, key: &str, previous_value: VersionedValue) {
        if let Some(event) = self.prepare(fragment, key, previous_value) {
            event.publish();
        }
    }
}

struct QueuedExpiry {
    event: ExpiryEvent,
    queued_at: Instant,
    budget: Budget,
}

#[derive(Clone, Copy)]
enum BudgetState {
    Staged,
    Queued,
    InFlight,
}
#[derive(Clone, Copy)]
enum Outcome {
    Delivered,
    Failed,
    Abandoned,
    Dropped,
}

struct Budget {
    _events: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    bytes: usize,
    metrics: Arc<Metrics>,
    state: BudgetState,
    outcome: Option<Outcome>,
}

impl Budget {
    fn queued(&mut self) {
        self.state = BudgetState::Queued;
        self.metrics.queued_events.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .queued_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
    }

    fn dispatched(&mut self, event: &ExpiryEvent, queued_at: Instant, wall_now: u64) {
        self.metrics.queued_events.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.state = BudgetState::InFlight;
        self.metrics
            .in_flight_events
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .in_flight_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        let expiry_latency = wall_now.saturating_sub(event.expires_at_ms);
        let queue_latency = u64::try_from(queued_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        add(&self.metrics.latency_samples, 1);
        add(&self.metrics.expiry_to_dispatch_total_ms, expiry_latency);
        self.metrics
            .expiry_to_dispatch_max_ms
            .fetch_max(expiry_latency, Ordering::Relaxed);
        add(&self.metrics.queue_to_dispatch_total_ms, queue_latency);
        self.metrics
            .queue_to_dispatch_max_ms
            .fetch_max(queue_latency, Ordering::Relaxed);
    }
}

impl Drop for Budget {
    fn drop(&mut self) {
        match self.state {
            BudgetState::Staged => {}
            BudgetState::Queued => {
                self.metrics.queued_events.fetch_sub(1, Ordering::Relaxed);
                self.metrics
                    .queued_bytes
                    .fetch_sub(self.bytes, Ordering::Relaxed);
            }
            BudgetState::InFlight => {
                self.metrics
                    .in_flight_events
                    .fetch_sub(1, Ordering::Relaxed);
                self.metrics
                    .in_flight_bytes
                    .fetch_sub(self.bytes, Ordering::Relaxed);
            }
        }
        let counter = match self.outcome.unwrap_or(Outcome::Abandoned) {
            Outcome::Delivered => &self.metrics.delivered_events,
            Outcome::Failed => &self.metrics.failed_events,
            Outcome::Abandoned => &self.metrics.abandoned_events,
            Outcome::Dropped => return,
        };
        add(counter, 1);
    }
}

#[derive(Clone, Copy)]
enum Stop {
    Running,
    Drain(Instant),
    Abort,
}

fn deadline_after(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration).unwrap_or(now)
}

async fn drain_deadline(mode: Stop) {
    match mode {
        Stop::Drain(deadline) => tokio::time::sleep_until(deadline).await,
        _ => std::future::pending::<()>().await,
    }
}

fn stopping(mode: Stop) -> bool {
    matches!(mode, Stop::Abort)
        || matches!(mode, Stop::Drain(deadline) if Instant::now() >= deadline)
}

/// Sole task owner. Producers hold control/queue handles, never this owner.
pub(crate) struct ExpiryRuntime {
    publisher: ExpiryPublisher,
    stop: watch::Sender<Stop>,
    done: watch::Receiver<bool>,
    manager: StdMutex<Option<JoinHandle<()>>>,
    shutdown_lock: Mutex<()>,
}

impl ExpiryRuntime {
    pub(crate) fn start(
        node_id: String,
        handler: Arc<dyn TtlExpiryHandler>,
        options: TtlExpiryOptions,
    ) -> Self {
        Self::start_with_clock(node_id, handler, options, Arc::new(crate::actor::now_ms))
    }

    pub(crate) fn start_with_clock(
        node_id: String,
        handler: Arc<dyn TtlExpiryHandler>,
        options: TtlExpiryOptions,
        clock: WallClock,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(options.queue_event_capacity);
        let control = Arc::new(Control {
            accepting: AtomicBool::new(true),
            event_permits: Arc::new(Semaphore::new(options.queue_event_capacity)),
            byte_permits: Arc::new(Semaphore::new(options.queue_byte_capacity)),
            metrics: Arc::new(Metrics::default()),
            options,
            clock,
        });
        let (stop, stop_rx) = watch::channel(Stop::Running);
        let (done_tx, done) = watch::channel(false);
        let manager = tokio::spawn(run_dispatcher(
            node_id,
            handler,
            control.clone(),
            receiver,
            stop_rx,
            done_tx,
        ));
        Self {
            publisher: ExpiryPublisher { sender, control },
            stop,
            done,
            manager: StdMutex::new(Some(manager)),
            shutdown_lock: Mutex::new(()),
        }
    }

    pub(crate) fn publisher(&self) -> ExpiryPublisher {
        self.publisher.clone()
    }

    pub(crate) fn diagnostics(&self) -> ExpiryDiagnostics {
        let c = &self.publisher.control;
        let m = &c.metrics;
        macro_rules! u {
            ($field:ident) => {
                m.$field.load(Ordering::Relaxed)
            };
        }
        ExpiryDiagnostics {
            queued_events: u!(queued_events),
            queued_bytes: u!(queued_bytes),
            in_flight_events: u!(in_flight_events),
            in_flight_bytes: u!(in_flight_bytes),
            retained_events: c.options.queue_event_capacity - c.event_permits.available_permits(),
            retained_bytes: c.options.queue_byte_capacity - c.byte_permits.available_permits(),
            enqueued_events: u!(enqueued_events),
            dispatched_batches: u!(dispatched_batches),
            delivered_events: u!(delivered_events),
            failed_events: u!(failed_events),
            abandoned_events: u!(abandoned_events),
            dropped_events: u!(dropped_events),
            dropped_event_capacity: u!(dropped_event_capacity),
            dropped_byte_capacity: u!(dropped_byte_capacity),
            dropped_closed: u!(dropped_closed),
            handler_errors: u!(handler_errors),
            handler_panics: u!(handler_panics),
            handler_timeouts: u!(handler_timeouts),
            latency_samples: u!(latency_samples),
            expiry_to_dispatch_total_ms: u!(expiry_to_dispatch_total_ms),
            expiry_to_dispatch_max_ms: u!(expiry_to_dispatch_max_ms),
            queue_to_dispatch_total_ms: u!(queue_to_dispatch_total_ms),
            queue_to_dispatch_max_ms: u!(queue_to_dispatch_max_ms),
        }
    }

    fn request_drain(&self) {
        if self
            .publisher
            .control
            .accepting
            .swap(false, Ordering::AcqRel)
        {
            self.stop.send_replace(Stop::Drain(deadline_after(
                self.publisher.control.options.shutdown_drain_timeout,
            )));
        }
    }

    pub(crate) async fn shutdown(&self) {
        // The deadline lives in the manager, independent of this waiter's cancellation.
        self.request_drain();
        let _lock = self.shutdown_lock.lock().await;
        let mut done = self.done.clone();
        while !*done.borrow() {
            if done.changed().await.is_err() {
                break;
            }
        }
        let manager = self.manager.lock().unwrap().take();
        if let Some(manager) = manager {
            if let Err(error) = manager.await {
                tracing::warn!(
                    panicked = error.is_panic(),
                    "TTL expiry dispatcher task stopped unexpectedly"
                );
            }
        }
    }

    pub(crate) fn request_abort(&self) {
        self.publisher
            .control
            .accepting
            .store(false, Ordering::Release);
        self.stop.send_replace(Stop::Abort);
        if let Some(manager) = self.manager.lock().unwrap().as_ref() {
            manager.abort();
        }
    }
}

impl Drop for ExpiryRuntime {
    fn drop(&mut self) {
        if !*self.done.borrow() && self.diagnostics().retained_events != 0 {
            tracing::warn!(
                retained_events = self.diagnostics().retained_events,
                "TTL expiry owner dropped; notifications canceled"
            );
        }
        self.request_abort();
        if let Some(manager) = self.manager.lock().unwrap().take() {
            manager.abort();
        }
    }
}

struct Done {
    control: Arc<Control>,
    signal: watch::Sender<bool>,
}

impl Drop for Done {
    fn drop(&mut self) {
        self.control.accepting.store(false, Ordering::Release);
        let abandoned = self
            .control
            .metrics
            .abandoned_events
            .load(Ordering::Relaxed);
        if abandoned != 0 {
            tracing::warn!(
                abandoned_events = abandoned,
                "TTL expiry dispatcher abandoned notifications"
            );
        }
        self.signal.send_replace(true);
    }
}

enum Attempt {
    Success,
    Error,
    Timeout,
    Abandoned,
}

#[derive(Clone, Copy)]
enum Cancellation {
    Timeout,
    Shutdown,
}

async fn invoke(
    handler: &dyn TtlExpiryHandler,
    batch: ExpiryBatch,
    timeout: Duration,
    stop: &mut watch::Receiver<Stop>,
    mode: &mut Stop,
    cancellation: &mut Option<Cancellation>,
) -> Attempt {
    let deadline = deadline_after(timeout);
    // Construction, polling and ordinary cancellation/drop all happen inside
    // the caller's catch_unwind future, without holding database locks.
    let mut invocation = Box::pin(async { handler.on_expired(batch).await });
    loop {
        *mode = *stop.borrow_and_update();
        if stopping(*mode) {
            *cancellation = Some(Cancellation::Shutdown);
            return Attempt::Abandoned;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                *cancellation = Some(Cancellation::Timeout);
                return Attempt::Timeout;
            },
            _ = drain_deadline(*mode) => {
                *cancellation = Some(Cancellation::Shutdown);
                return Attempt::Abandoned;
            },
            changed = stop.changed() => {
                if changed.is_err() {
                    *cancellation = Some(Cancellation::Shutdown);
                    return Attempt::Abandoned;
                }
            }
            result = &mut invocation => return if result.is_ok() { Attempt::Success } else { Attempt::Error },
        }
    }
}

async fn run_dispatcher(
    node_id: String,
    handler: Arc<dyn TtlExpiryHandler>,
    control: Arc<Control>,
    receiver: mpsc::Receiver<QueuedExpiry>,
    mut stop: watch::Receiver<Stop>,
    done: watch::Sender<bool>,
) {
    let _done = Done {
        control: control.clone(),
        signal: done,
    };
    let mut receiver = receiver;
    loop {
        let mut mode = *stop.borrow_and_update();
        if !matches!(mode, Stop::Running) {
            receiver.close();
        }
        if stopping(mode) {
            return;
        }
        let first = tokio::select! {
            biased;
            _ = drain_deadline(mode) => return,
            _ = stop.changed() => continue,
            first = receiver.recv() => match first { Some(event) => event, None => return },
        };
        let collection_deadline = first
            .queued_at
            .checked_add(control.options.max_batch_delay)
            .unwrap_or(first.queued_at);
        let mut batch = vec![first];
        while batch.len() < control.options.batch_size {
            mode = *stop.borrow_and_update();
            if !matches!(mode, Stop::Running) {
                receiver.close();
            }
            if stopping(mode) {
                return;
            }
            if matches!(mode, Stop::Running) && Instant::now() >= collection_deadline {
                break;
            }
            // Consume immediately available entries before waiting for a batch.
            match receiver.try_recv() {
                Ok(event) => {
                    batch.push(event);
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            if !matches!(mode, Stop::Running) || Instant::now() >= collection_deadline {
                break;
            }
            tokio::select! {
                biased;
                _ = drain_deadline(mode) => return,
                _ = stop.changed() => {},
                _ = tokio::time::sleep_until(collection_deadline) => break,
                event = receiver.recv() => match event { Some(event) => batch.push(event), None => break },
            }
        }
        mode = *stop.borrow_and_update();
        if stopping(mode) {
            return;
        }
        let wall_now = (control.clock)();
        let mut events = Vec::with_capacity(batch.len());
        let mut budgets = Vec::with_capacity(batch.len());
        for mut queued in batch {
            queued
                .budget
                .dispatched(&queued.event, queued.queued_at, wall_now);
            events.push(queued.event);
            budgets.push(queued.budget);
        }
        add(&control.metrics.dispatched_batches, 1);
        let batch = ExpiryBatch {
            local_node_id: node_id.clone(),
            dispatched_at_ms: wall_now,
            events,
        };
        let mut cancellation = None;
        let result = AssertUnwindSafe(invoke(
            handler.as_ref(),
            batch,
            control.options.handler_timeout,
            &mut stop,
            &mut mode,
            &mut cancellation,
        ))
        .catch_unwind()
        .await;
        if matches!(cancellation, Some(Cancellation::Timeout)) {
            let count = add(&control.metrics.handler_timeouts, 1);
            if count.is_power_of_two() {
                tracing::warn!(handler_timeouts = count, "TTL expiry handler timed out");
            }
        }
        let outcome = match result {
            Ok(Attempt::Success) => Outcome::Delivered,
            Ok(Attempt::Error) => {
                let count = add(&control.metrics.handler_errors, 1);
                if count.is_power_of_two() {
                    tracing::warn!(
                        handler_errors = count,
                        "TTL expiry handler returned an error"
                    );
                }
                Outcome::Failed
            }
            Ok(Attempt::Timeout) => Outcome::Abandoned,
            Ok(Attempt::Abandoned) => Outcome::Abandoned,
            Err(_) => {
                let count = add(&control.metrics.handler_panics, 1);
                if count.is_power_of_two() {
                    tracing::warn!(handler_panics = count, "TTL expiry handler panicked");
                }
                if cancellation.is_some() {
                    Outcome::Abandoned
                } else {
                    Outcome::Failed
                }
            }
        };
        for budget in &mut budgets {
            budget.outcome = Some(outcome);
        }
        drop(budgets);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    fn value(version: u64) -> VersionedValue {
        let mut value = VersionedValue::new(vec![0xff, 0, 1], version, "writer".into());
        value.expires_at_ms = Some(100);
        value
    }

    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    fn collector(
        options: TtlExpiryOptions,
    ) -> (ExpiryRuntime, mpsc::Receiver<ExpiryBatch>, Arc<AtomicU64>) {
        let (sender, receiver) = mpsc::channel(16);
        let clock = Arc::new(AtomicU64::new(1000));
        let read_clock = clock.clone();
        let handler = expiry_handler_fn(move |batch| {
            let sender = sender.clone();
            async move {
                sender
                    .try_send(batch)
                    .map_err(|_| ExpiryCallbackError::new("collector full"))
            }
        });
        let runtime = ExpiryRuntime::start_with_clock(
            "local-node".into(),
            Arc::new(handler),
            options,
            Arc::new(move || read_clock.load(Ordering::SeqCst)),
        );
        (runtime, receiver, clock)
    }

    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn identity_is_stable_version_based_unambiguous_and_debug_redacts_values() {
        let mut original = value(123);
        let id = ExpiryEventId::for_record(FragmentId(7), "key\0x", &original);
        assert_eq!(
            id.to_string(),
            "15def029ebc6affe08fed6e4cd8413c21752d704bb258c45ec0edfa60d5746c8"
        );
        original.value = b"another opaque format".to_vec();
        original.expires_at_ms = Some(200);
        assert_eq!(
            id,
            ExpiryEventId::for_record(FragmentId(7), "key\0x", &original)
        );
        assert_ne!(
            id,
            ExpiryEventId::for_record(FragmentId(8), "key\0x", &original)
        );
        assert_ne!(
            id,
            ExpiryEventId::for_record(FragmentId(7), "another key", &original)
        );
        original.timestamp_ms += 1;
        assert_ne!(
            id,
            ExpiryEventId::for_record(FragmentId(7), "key\0x", &original)
        );
        let mut left = value(1);
        left.node_id = "bc".into();
        let mut right = value(1);
        right.node_id = "c".into();
        assert_ne!(
            ExpiryEventId::for_record(FragmentId(0), "a", &left),
            ExpiryEventId::for_record(FragmentId(0), "ab", &right)
        );
        let event = ExpiryEvent {
            id,
            fragment: FragmentId(7),
            key: "private-recipient".into(),
            previous_value: original,
            expires_at_ms: 200,
            observed_at_ms: 300,
        };
        let debug = format!("{event:?}");
        assert!(!debug.contains("private-recipient"));
        assert!(!debug.contains("another opaque format"));
    }

    #[tokio::test(start_paused = true)]
    async fn batches_by_size_and_delay_in_fifo_order_with_latency_diagnostics() {
        let (runtime, mut output, clock) = collector(
            TtlExpiryOptions::builder()
                .batch_size(2)
                .max_batch_delay(Duration::from_millis(10))
                .build(),
        );
        let publisher = runtime.publisher();
        for (fragment, key, version) in [(0, "a", 1), (1, "b", 2), (0, "c", 3)] {
            publisher.offer(FragmentId(fragment), key, value(version));
        }
        settle().await;
        let first = output.try_recv().unwrap();
        assert_eq!(first.local_node_id, "local-node");
        assert_eq!(
            first
                .events
                .iter()
                .map(|e| e.key.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(runtime.diagnostics().queued_events, 1);
        assert!(output.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(9)).await;
        settle().await;
        assert!(output.try_recv().is_err());
        clock.store(1010, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        let last = output.try_recv().unwrap();
        assert_eq!(last.events[0].key, "c");
        assert_eq!(last.events[0].original_version(), (3, "writer"));
        assert_eq!(last.events[0].previous_value.value, [0xff, 0, 1]);
        assert_eq!(last.events[0].observed_at_ms, 1000);
        assert_eq!(last.dispatched_at_ms, 1010);
        runtime.shutdown().await;
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.delivered_events, 3);
        assert_eq!(diagnostics.dispatched_batches, 2);
        assert_eq!(diagnostics.latency_samples, 3);
        assert_eq!(diagnostics.expiry_to_dispatch_total_ms, 2710);
        assert_eq!(diagnostics.expiry_to_dispatch_max_ms, 910);
        assert_eq!(diagnostics.queue_to_dispatch_total_ms, 10);
        assert_eq!(diagnostics.queue_to_dispatch_max_ms, 10);
        assert_eq!(diagnostics.retained_events, 0);
        assert_eq!(diagnostics.retained_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn admission_includes_in_flight_payloads_and_drops_without_waiting() {
        let gate = Arc::new(Notify::new());
        let gate_handler = gate.clone();
        let (entered, mut receive) = mpsc::channel(4);
        let handler = expiry_handler_fn(move |batch: ExpiryBatch| {
            let gate = gate_handler.clone();
            let entered = entered.clone();
            async move {
                entered.try_send(batch.events[0].key.clone()).unwrap();
                gate.notified().await;
                Ok(())
            }
        });
        let options = TtlExpiryOptions::builder()
            .queue_event_capacity(2)
            .batch_size(1)
            .build();
        let runtime = ExpiryRuntime::start_with_clock(
            "n".into(),
            Arc::new(handler),
            options.clone(),
            Arc::new(|| 1000),
        );
        let publisher = runtime.publisher();
        publisher.offer(FragmentId(0), "first", value(1));
        settle().await;
        assert_eq!(receive.try_recv().unwrap(), "first");
        publisher.offer(FragmentId(0), "second", value(2));
        publisher.offer(FragmentId(0), "dropped", value(3));
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.in_flight_events, 1);
        assert_eq!(diagnostics.queued_events, 1);
        assert_eq!(diagnostics.retained_events, 2);
        assert!(diagnostics.retained_bytes <= options.queue_byte_capacity);
        assert_eq!(diagnostics.dropped_event_capacity, 1);
        gate.notify_one();
        settle().await;
        assert_eq!(receive.try_recv().unwrap(), "second");
        gate.notify_one();
        runtime.shutdown().await;
        assert_eq!(runtime.diagnostics().delivered_events, 2);
        assert_eq!(runtime.diagnostics().retained_events, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn byte_limit_charges_capacity_and_payloads_move_instead_of_copying() {
        let (runtime, mut output, _) = collector(
            TtlExpiryOptions::builder()
                .batch_size(1)
                .queue_byte_capacity(1024)
                .build(),
        );
        let mut oversized = value(1);
        oversized.value = Vec::with_capacity(4096);
        oversized.value.push(1);
        runtime.publisher().offer(FragmentId(0), "large", oversized);
        assert_eq!(runtime.diagnostics().dropped_byte_capacity, 1);
        let mut original = value(2);
        original.value.reserve(64);
        let pointer = original.value.as_ptr() as usize;
        runtime.publisher().offer(FragmentId(0), "small", original);
        runtime.shutdown().await;
        let batch = output.try_recv().unwrap();
        assert_eq!(
            batch.events[0].previous_value.value.as_ptr() as usize,
            pointer
        );
        assert_eq!(runtime.diagnostics().delivered_events, 1);
        assert_eq!(runtime.diagnostics().retained_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn errors_and_constructor_or_poll_panics_do_not_close_dispatcher() {
        for mode in 0..3 {
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_handler = calls.clone();
            let (sender, mut output) = mpsc::channel(4);
            let handler = expiry_handler_fn(move |batch: ExpiryBatch| {
                let call = calls_handler.fetch_add(1, Ordering::SeqCst);
                if call == 0 && mode == 1 {
                    panic!("constructor panic");
                }
                let sender = sender.clone();
                async move {
                    if call == 0 {
                        if mode == 2 {
                            panic!("poll panic");
                        }
                        return Err(ExpiryCallbackError::new("private application failure"));
                    }
                    sender.try_send(batch).unwrap();
                    Ok(())
                }
            });
            let runtime = ExpiryRuntime::start_with_clock(
                "n".into(),
                Arc::new(handler),
                TtlExpiryOptions::builder().batch_size(1).build(),
                Arc::new(|| 1000),
            );
            runtime.publisher().offer(FragmentId(0), "fails", value(1));
            runtime
                .publisher()
                .offer(FragmentId(0), "succeeds", value(2));
            runtime.shutdown().await;
            assert_eq!(output.try_recv().unwrap().events[0].key, "succeeds");
            let diagnostics = runtime.diagnostics();
            assert_eq!(diagnostics.failed_events, 1);
            assert_eq!(diagnostics.delivered_events, 1);
            assert_eq!(diagnostics.handler_errors, u64::from(mode == 0));
            assert_eq!(diagnostics.handler_panics, u64::from(mode != 0));
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_cancels_the_future_and_dispatches_later_events_without_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let call_handler = calls.clone();
        let drop_handler = dropped.clone();
        let (sender, mut output) = mpsc::channel(4);
        let handler = expiry_handler_fn(move |batch: ExpiryBatch| {
            let call = call_handler.fetch_add(1, Ordering::SeqCst);
            let sender = sender.clone();
            let dropped = drop_handler.clone();
            async move {
                if call == 0 {
                    let _guard = CountDrop(dropped);
                    std::future::pending::<()>().await;
                }
                sender.try_send(batch).unwrap();
                Ok(())
            }
        });
        let runtime = ExpiryRuntime::start_with_clock(
            "n".into(),
            Arc::new(handler),
            TtlExpiryOptions::builder()
                .batch_size(1)
                .handler_timeout(Duration::from_millis(20))
                .build(),
            Arc::new(|| 1000),
        );
        runtime
            .publisher()
            .offer(FragmentId(0), "timeout", value(1));
        runtime.publisher().offer(FragmentId(0), "later", value(2));
        settle().await;
        assert_eq!(runtime.diagnostics().in_flight_events, 1);
        tokio::time::advance(Duration::from_millis(20)).await;
        settle().await;
        assert_eq!(output.try_recv().unwrap().events[0].key, "later");
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        runtime.shutdown().await;
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.handler_timeouts, 1);
        assert_eq!(diagnostics.abandoned_events, 1);
        assert_eq!(diagnostics.delivered_events, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancellation_keeps_one_bounded_drain_and_accounts_for_abandoned_events() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let drop_handler = dropped.clone();
        let handler = expiry_handler_fn(move |_batch| {
            let dropped = drop_handler.clone();
            async move {
                let _guard = CountDrop(dropped);
                std::future::pending().await
            }
        });
        let options = TtlExpiryOptions::builder()
            .queue_event_capacity(3)
            .batch_size(1)
            .handler_timeout(Duration::from_secs(1))
            .shutdown_drain_timeout(Duration::from_millis(10))
            .build();
        let runtime = ExpiryRuntime::start_with_clock(
            "n".into(),
            Arc::new(handler),
            options,
            Arc::new(|| 1000),
        );
        for index in 0..3 {
            runtime
                .publisher()
                .offer(FragmentId(0), &index.to_string(), value(index));
        }
        settle().await;
        let mut shutdown = Box::pin(runtime.shutdown());
        assert!(futures::poll!(&mut shutdown).is_pending());
        drop(shutdown);
        runtime.publisher().offer(FragmentId(0), "closed", value(4));
        tokio::time::advance(Duration::from_millis(9)).await;
        settle().await;
        assert_eq!(runtime.diagnostics().abandoned_events, 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        runtime.shutdown().await;
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.abandoned_events, 3);
        assert_eq!(diagnostics.handler_timeouts, 0);
        assert_eq!(diagnostics.dropped_closed, 1);
        assert_eq!(diagnostics.queued_events, 0);
        assert_eq!(diagnostics.in_flight_events, 0);
        assert_eq!(diagnostics.retained_events, 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn successful_shutdown_flushes_partial_batches_without_waiting_for_delay() {
        let (runtime, mut output, _) = collector(
            TtlExpiryOptions::builder()
                .batch_size(8)
                .max_batch_delay(Duration::from_secs(60))
                .build(),
        );
        runtime.publisher().offer(FragmentId(0), "a", value(1));
        runtime.publisher().offer(FragmentId(0), "b", value(2));
        settle().await;
        let now = Instant::now();
        runtime.shutdown().await;
        assert_eq!(Instant::now(), now);
        assert_eq!(output.try_recv().unwrap().events.len(), 2);
        assert_eq!(runtime.diagnostics().delivered_events, 2);
        assert_eq!(runtime.diagnostics().abandoned_events, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn panic_during_timeout_drop_is_isolated_and_cancellation_is_still_accounted() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("callback destructor panic");
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_handler = calls.clone();
        let (sender, mut output) = mpsc::channel(4);
        let handler = expiry_handler_fn(move |batch| {
            let first = calls_handler.fetch_add(1, Ordering::SeqCst) == 0;
            let sender = sender.clone();
            async move {
                if first {
                    let _guard = PanicOnDrop;
                    std::future::pending::<()>().await;
                }
                sender.try_send(batch).unwrap();
                Ok(())
            }
        });
        let runtime = ExpiryRuntime::start_with_clock(
            "n".into(),
            Arc::new(handler),
            TtlExpiryOptions::builder()
                .batch_size(1)
                .handler_timeout(Duration::from_millis(20))
                .build(),
            Arc::new(|| 1000),
        );
        runtime
            .publisher()
            .offer(FragmentId(0), "panics_on_drop", value(1));
        runtime.publisher().offer(FragmentId(0), "later", value(2));
        settle().await;
        tokio::time::advance(Duration::from_millis(20)).await;
        settle().await;
        runtime.shutdown().await;
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.handler_timeouts, 1);
        assert_eq!(diagnostics.handler_panics, 1);
        assert_eq!(diagnostics.abandoned_events, 1);
        assert_eq!(diagnostics.failed_events, 0);
        assert_eq!(diagnostics.delivered_events, 1);
        assert_eq!(output.try_recv().unwrap().events[0].key, "later");
    }

    #[tokio::test(start_paused = true)]
    async fn owner_drop_aborts_owned_work_and_releases_queued_and_in_flight_budgets() {
        for start_handler in [false, true] {
            let dropped = Arc::new(AtomicUsize::new(0));
            let drop_handler = dropped.clone();
            let handler = expiry_handler_fn(move |_batch| {
                let dropped = drop_handler.clone();
                async move {
                    let _guard = CountDrop(dropped);
                    std::future::pending().await
                }
            });
            let runtime = ExpiryRuntime::start_with_clock(
                "n".into(),
                Arc::new(handler),
                TtlExpiryOptions::builder().batch_size(1).build(),
                Arc::new(|| 1000),
            );
            let publisher = runtime.publisher();
            publisher.offer(FragmentId(0), "a", value(1));
            publisher.offer(FragmentId(0), "b", value(2));
            if start_handler {
                settle().await;
            }
            drop(runtime);
            settle().await;
            let control = &publisher.control;
            assert_eq!(control.metrics.abandoned_events.load(Ordering::SeqCst), 2);
            assert_eq!(
                control.event_permits.available_permits(),
                control.options.queue_event_capacity
            );
            assert_eq!(
                control.byte_permits.available_permits(),
                control.options.queue_byte_capacity
            );
            assert_eq!(control.metrics.queued_events.load(Ordering::SeqCst), 0);
            assert_eq!(control.metrics.in_flight_events.load(Ordering::SeqCst), 0);
            assert_eq!(dropped.load(Ordering::SeqCst), usize::from(start_handler));
            publisher.offer(FragmentId(0), "late", value(3));
            assert_eq!(control.metrics.dropped_closed.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn staged_startup_events_consume_bounded_capacity_and_release_on_failure() {
        let (runtime, mut output, _) = collector(
            TtlExpiryOptions::builder()
                .queue_event_capacity(1)
                .batch_size(1)
                .build(),
        );
        let staged = runtime
            .publisher()
            .prepare_startup(FragmentId(0), "startup", value(1))
            .unwrap();
        assert_eq!(runtime.diagnostics().queued_events, 0);
        assert_eq!(runtime.diagnostics().retained_events, 1);
        runtime.publisher().offer(FragmentId(0), "full", value(2));
        assert_eq!(runtime.diagnostics().dropped_event_capacity, 1);
        drop(staged);
        assert_eq!(runtime.diagnostics().abandoned_events, 1);
        runtime
            .publisher()
            .offer(FragmentId(0), "recovered", value(3));
        runtime.shutdown().await;
        assert_eq!(output.try_recv().unwrap().events[0].key, "recovered");
    }

    #[test]
    fn validates_nonzero_capacities_batch_sizes_and_representable_deadlines() {
        assert!(TtlExpiryOptions::default().validate().is_ok());
        for options in [
            TtlExpiryOptions {
                queue_event_capacity: 0,
                ..Default::default()
            },
            TtlExpiryOptions {
                queue_byte_capacity: 0,
                ..Default::default()
            },
            TtlExpiryOptions {
                batch_size: 0,
                ..Default::default()
            },
            TtlExpiryOptions {
                batch_size: 1025,
                ..Default::default()
            },
            TtlExpiryOptions {
                max_batch_delay: Duration::ZERO,
                ..Default::default()
            },
            TtlExpiryOptions {
                handler_timeout: Duration::ZERO,
                ..Default::default()
            },
            TtlExpiryOptions {
                shutdown_drain_timeout: Duration::ZERO,
                ..Default::default()
            },
            TtlExpiryOptions {
                handler_timeout: Duration::MAX,
                ..Default::default()
            },
        ] {
            assert!(options.validate().is_err());
        }
    }

    #[test]
    fn diagnostic_counters_saturate_without_wrapping() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(add(&counter, 1), u64::MAX);
        assert_eq!(add(&counter, 100), u64::MAX);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }
}
