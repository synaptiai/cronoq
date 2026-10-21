//! Executors run task payloads.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use cronoq_core::{LeaseId, Tags, TaskId};
use tokio_util::sync::CancellationToken;

use crate::error::TaskError;

/// A boxed, `Send` future (what [`Executor::execute`] returns).
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Runs one attempt of a task.
///
/// Both methods are called on the scheduler's driver task. A panic in either (or in the
/// returned future) fails only that attempt, permanently, with a `panic: ...` message.
pub trait Executor<P, O>: Send + Sync + 'static {
    /// Start an attempt. Do the work in the returned future; keep the synchronous part cheap.
    fn execute(&self, ctx: TaskContext, payload: Arc<P>) -> BoxFuture<Result<O, TaskError>>;

    /// Called synchronously when the engine abandons an attempt (timeout, cancellation, lease
    /// expiry), before any new dispatch. The attempt's future is cancelled afterwards; use this
    /// to release resources tied to the lease immediately (e.g. a remote worker's slot).
    fn abort(&self, _lease: LeaseId) {}
}

/// Executor from an async closure.
pub struct FnExecutor<F>(F);

impl<F> FnExecutor<F> {
    /// Wrap a closure `|ctx, payload| async move { ... }`.
    pub fn new(f: F) -> Self {
        FnExecutor(f)
    }
}

impl<P, O, F, Fut> Executor<P, O> for FnExecutor<F>
where
    F: Fn(TaskContext, Arc<P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, TaskError>> + Send + 'static,
{
    fn execute(&self, ctx: TaskContext, payload: Arc<P>) -> BoxFuture<Result<O, TaskError>> {
        Box::pin((self.0)(ctx, payload))
    }
}

/// Sends one heartbeat request for a lease; the flag is cleared by the driver once handled.
pub(crate) type HeartbeatFn = Arc<dyn Fn(LeaseId, Arc<AtomicBool>) + Send + Sync>;

/// Information and controls for a running attempt.
#[derive(Clone)]
pub struct TaskContext {
    /// The task.
    pub id: TaskId,
    /// This attempt's lease.
    pub lease: LeaseId,
    /// Attempt number, starting at 1.
    pub attempt: u32,
    /// Pool the attempt runs in.
    pub pool: Arc<str>,
    /// The task's kind.
    pub kind: Arc<str>,
    /// The task's key, if it has one.
    pub key: Option<Arc<str>>,
    /// The task's tags.
    pub tags: Tags,
    /// The task's deadline, if any (wall-clock).
    pub deadline: Option<SystemTime>,
    /// Per-attempt timeout, if any. The engine aborts the attempt when it elapses.
    pub timeout: Option<Duration>,
    pub(crate) cancel: CancellationToken,
    pub(crate) heartbeat: HeartbeatFn,
    /// Set while a heartbeat for this lease is queued, so heartbeats coalesce.
    pub(crate) heartbeat_pending: Arc<AtomicBool>,
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("id", &self.id)
            .field("attempt", &self.attempt)
            .field("pool", &self.pool)
            .field("kind", &self.kind)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl TaskContext {
    /// A detached context for unit-testing executors: attempt 1 of task `id` in pool and kind
    /// `"default"`, with no deadline, timeout or tags (all fields are public, so adjust as
    /// needed). Heartbeats are no-ops; cancel it via [`TaskContext::cancellation_token`].
    pub fn for_test(id: TaskId) -> TaskContext {
        TaskContext {
            id,
            lease: LeaseId(id.0),
            attempt: 1,
            pool: cronoq_core::default_name(),
            kind: cronoq_core::default_name(),
            key: None,
            tags: Arc::from(Vec::new()),
            deadline: None,
            timeout: None,
            cancel: CancellationToken::new(),
            heartbeat: Arc::new(|_, _| {}),
            heartbeat_pending: Arc::new(AtomicBool::new(false)),
        }
    }

    /// True once the task was cancelled or its attempt aborted.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves when the task is cancelled or its attempt aborted.
    ///
    /// When the engine abandons an attempt (cancellation, timeout, lease expiry) it cancels
    /// this token and then immediately drops the attempt's future. Code awaiting
    /// `cancelled()` inside that future therefore does not get to run async cleanup: use
    /// `Drop` guards for cleanup, and this token for work you spawned elsewhere (which keeps
    /// running until it observes the cancellation).
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }

    /// The attempt's cancellation token (see [`TaskContext::cancelled`]).
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Extend the lease (needed for pools with a lease TTL). Cheap to call often: requests
    /// are coalesced, so at most one per lease is queued at a time.
    pub fn heartbeat(&self) {
        if !self.heartbeat_pending.swap(true, Ordering::AcqRel) {
            (self.heartbeat)(self.lease, self.heartbeat_pending.clone());
        }
    }

    /// Value of tag `key`, if present.
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| &**v)
    }

    /// W3C trace context of the submitter, if any.
    pub fn traceparent(&self) -> Option<&str> {
        self.tag("traceparent")
    }
}
