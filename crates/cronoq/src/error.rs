//! Error types.

use std::sync::Arc;
use std::time::Duration;

use cronoq_core::{SubmitError, TaskId, TaskStatus};

/// Error returned by an executor for one attempt.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TaskError {
    /// Worth retrying (subject to the retry policy).
    #[error("{message}")]
    Retryable {
        /// Human-readable cause, recorded with the task.
        message: Arc<str>,
        /// Do not retry before this much time has passed (e.g. an HTTP `Retry-After`).
        retry_after: Option<Duration>,
    },
    /// Never retried.
    #[error("{message}")]
    Permanent {
        /// Human-readable cause, recorded with the task.
        message: Arc<str>,
    },
    /// The executor observed cancellation.
    #[error("cancelled")]
    Cancelled,
}

impl TaskError {
    /// A retryable failure.
    pub fn retryable(message: impl Into<Arc<str>>) -> Self {
        TaskError::Retryable {
            message: message.into(),
            retry_after: None,
        }
    }
    /// Retryable, but not before `after` (e.g. an HTTP 429 `Retry-After`).
    pub fn retry_after(message: impl Into<Arc<str>>, after: Duration) -> Self {
        TaskError::Retryable {
            message: message.into(),
            retry_after: Some(after),
        }
    }
    /// A failure that must not be retried.
    pub fn permanent(message: impl Into<Arc<str>>) -> Self {
        TaskError::Permanent {
            message: message.into(),
        }
    }
    /// Wrap any error as retryable.
    pub fn from_err(e: impl std::fmt::Display) -> Self {
        TaskError::retryable(e.to_string())
    }
}

/// Why a [`TaskHandle`](crate::TaskHandle) did not produce an output.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TaskFailure {
    /// The task reached a terminal status other than `Succeeded`.
    #[error("task ended as {status}: {message}")]
    Ended {
        /// Terminal status.
        status: TaskStatus,
        /// Last error message, or the status name.
        message: Arc<str>,
    },
    /// The task succeeded, but its output is no longer held in memory or in the store
    /// (e.g. a duplicate submission long after the original finished).
    #[error("task {id} succeeded, but its output is no longer available")]
    OutputUnavailable {
        /// The task that succeeded.
        id: TaskId,
    },
    /// The scheduler shut down before the task finished.
    #[error("scheduler shut down")]
    Shutdown,
}

impl TaskFailure {
    /// The task's terminal status, when known.
    pub fn status(&self) -> Option<TaskStatus> {
        match self {
            TaskFailure::Ended { status, .. } => Some(*status),
            TaskFailure::OutputUnavailable { .. } => Some(TaskStatus::Succeeded),
            TaskFailure::Shutdown => None,
        }
    }
}

/// Scheduler-level error.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine rejected a submission.
    #[error(transparent)]
    Submit(#[from] SubmitError),
    /// The scheduler is shut down (or shutting down).
    #[error("scheduler is shut down")]
    Shutdown,
    /// Invalid configuration or API misuse.
    #[error("configuration error: {0}")]
    Config(String),
    /// The store failed fatally (e.g. leadership lost, or an outage outlasted
    /// [`Builder::max_store_outage`](crate::Builder::max_store_outage)).
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    /// An internal failure (e.g. the scheduler task panicked).
    #[error("internal error: {0}")]
    Internal(String),
}

/// Failure reported by a [`Store`](crate::Store).
///
/// The driver retries [`StoreError::Unavailable`] with capped backoff for up to
/// [`Builder::max_store_outage`](crate::Builder::max_store_outage) while the engine keeps
/// running; [`StoreError::Other`] is retried a few times; [`StoreError::Fenced`] stops the
/// scheduler immediately.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Another instance holds leadership; this one must stop writing.
    #[error("fenced: leadership lost")]
    Fenced,
    /// Transient failure (connection lost, timeout, database restarting); safe to retry.
    #[error("store unavailable: {0}")]
    Unavailable(String),
    /// Any other failure.
    #[error("{0}")]
    Other(String),
}

impl StoreError {
    /// Whether retrying the same operation may succeed.
    pub fn is_transient(&self) -> bool {
        matches!(self, StoreError::Unavailable(_))
    }
}

/// Failure reported by a [`Scaler`](crate::Scaler).
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScaleError {
    /// Capacity is managed elsewhere and will be reported via `Scheduler::set_capacity`.
    #[error("capacity is managed externally")]
    External,
    /// The scale request failed.
    #[error("{0}")]
    Other(String),
}
