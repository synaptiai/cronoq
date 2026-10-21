//! Error types.

use std::sync::Arc;
use std::time::Duration;

use cronoq_core::{SubmitError, TaskStatus};

/// Error returned by an executor for one attempt.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TaskError {
    /// Worth retrying (subject to the retry policy).
    #[error("{message}")]
    Retryable {
        message: Arc<str>,
        retry_after: Option<Duration>,
    },
    /// Never retried.
    #[error("{message}")]
    Permanent { message: Arc<str> },
    /// The executor observed cancellation.
    #[error("cancelled")]
    Cancelled,
}

impl TaskError {
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

/// Why a task did not produce an output.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TaskFailure {
    #[error("task ended as {status}: {message}")]
    Ended {
        status: TaskStatus,
        message: Arc<str>,
    },
    /// The scheduler shut down before the task finished.
    #[error("scheduler shut down")]
    Shutdown,
}

impl TaskFailure {
    pub fn status(&self) -> Option<TaskStatus> {
        match self {
            TaskFailure::Ended { status, .. } => Some(*status),
            TaskFailure::Shutdown => None,
        }
    }
}

/// Scheduler-level error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Submit(#[from] SubmitError),
    #[error("scheduler is shut down")]
    Shutdown,
    #[error("configuration error: {0}")]
    Config(String),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StoreError {
    /// Another instance holds leadership; this one must stop writing.
    #[error("fenced: leadership lost")]
    Fenced,
    #[error("{0}")]
    Other(String),
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ScaleError {
    /// Capacity is managed elsewhere and will be reported via `Scheduler::set_capacity`.
    #[error("capacity is managed externally")]
    External,
    #[error("{0}")]
    Other(String),
}
