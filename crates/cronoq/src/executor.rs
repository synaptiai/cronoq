//! Executors run task payloads.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cronoq_core::{LeaseId, Tags, TaskId};
use tokio_util::sync::CancellationToken;

use crate::error::TaskError;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Runs one attempt of a task.
pub trait Executor<P, O>: Send + Sync + 'static {
    fn execute(&self, ctx: TaskContext, payload: Arc<P>) -> BoxFuture<Result<O, TaskError>>;
}

/// Executor from an async closure.
pub struct FnExecutor<F>(F);

impl<F> FnExecutor<F> {
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

pub(crate) type HeartbeatFn = Arc<dyn Fn(LeaseId) + Send + Sync>;

/// Information and controls for a running attempt.
#[derive(Clone)]
pub struct TaskContext {
    pub id: TaskId,
    pub lease: LeaseId,
    pub attempt: u32,
    pub pool: Arc<str>,
    pub kind: Arc<str>,
    pub key: Option<Arc<str>>,
    pub tags: Tags,
    pub deadline: Option<SystemTime>,
    pub timeout: Option<Duration>,
    pub(crate) cancel: CancellationToken,
    pub(crate) heartbeat: HeartbeatFn,
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
    /// True once the task was cancelled or its attempt aborted.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    /// Resolves when the task is cancelled or its attempt aborted.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
    /// Extend the lease (needed for pools with a lease TTL).
    pub fn heartbeat(&self) {
        (self.heartbeat)(self.lease)
    }
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
