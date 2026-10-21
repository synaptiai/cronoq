//! Mapping between Rust errors and the Python exception classes in `cronoq.errors`.

use std::time::Duration;

use cronoq::{Error, SubmitError, TaskError, TaskFailure};
use pyo3::call::PyCallArgs;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Instantiate `cronoq.errors.<class>(*args)` as a `PyErr`.
pub(crate) fn py_error<'py, A>(py: Python<'py>, class: &str, args: A) -> PyErr
where
    A: PyCallArgs<'py>,
{
    let made = py
        .import("cronoq.errors")
        .and_then(|m| m.getattr(class))
        .and_then(|cls| cls.call1(args));
    match made {
        Ok(v) => PyErr::from_value(v),
        Err(e) => e,
    }
}

fn submit_code(e: &SubmitError) -> &'static str {
    match e {
        SubmitError::UnknownPool(_) => "unknown_pool",
        SubmitError::UnknownResource(_) => "unknown_resource",
        SubmitError::ResourceExceedsCapacity(_) => "resource_exceeds_capacity",
        SubmitError::ReservationConflict => "reservation_conflict",
        SubmitError::Cycle => "cycle",
        SubmitError::UnknownDependency(_) => "unknown_dependency",
        SubmitError::Full => "full",
        SubmitError::Overloaded => "overloaded",
        SubmitError::DuplicateKeyInBatch(_) => "duplicate_key_in_batch",
        SubmitError::Invalid(_) => "invalid",
    }
}

/// Scheduler errors become `SubmitError` (with a `.code`) or `SchedulerError`.
pub(crate) fn to_pyerr(e: Error) -> PyErr {
    Python::attach(|py| match &e {
        Error::Submit(s) => py_error(py, "SubmitError", (submit_code(s), e.to_string())),
        _ => py_error(py, "SchedulerError", (e.to_string(),)),
    })
}

/// A task without an output raises `TaskFailed(status, message, task_id)`.
pub(crate) fn failure_to_pyerr(py: Python<'_>, f: &TaskFailure, id: u64) -> PyErr {
    match f {
        TaskFailure::Ended { status, message } => {
            py_error(py, "TaskFailed", (status.name(), message.to_string(), id))
        }
        TaskFailure::Shutdown => {
            py_error(py, "TaskFailed", ("shutdown", "scheduler shut down", id))
        }
        TaskFailure::OutputUnavailable { .. } => {
            py_error(py, "TaskFailed", ("succeeded", f.to_string(), id))
        }
        other => py_error(py, "TaskFailed", ("unknown", other.to_string(), id)),
    }
}

pub(crate) fn not_started() -> PyErr {
    PyRuntimeError::new_err("scheduler not started; call `await scheduler.start()` first")
}

/// Classify an exception raised by a Python executor.
///
/// `cronoq.Retryable` / `cronoq.Permanent` carry an explicit kind; `asyncio.CancelledError`
/// means the attempt observed cancellation; anything else is retryable.
pub(crate) fn classify(e: PyErr) -> TaskError {
    Python::attach(|py| {
        let cancelled = py
            .import("asyncio")
            .and_then(|m| m.getattr("CancelledError"))
            .map(|c| e.matches(py, c).unwrap_or(false))
            .unwrap_or(false);
        if cancelled {
            return TaskError::Cancelled;
        }
        let value = e.value(py);
        let message = value
            .str()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<unprintable exception>".into());
        let kind: Option<String> = value.getattr("_cronoq_kind").and_then(|k| k.extract()).ok();
        match kind.as_deref() {
            Some("permanent") => TaskError::permanent(message),
            Some("retryable") => {
                let after: Option<f64> = value
                    .getattr("retry_after")
                    .and_then(|v| v.extract())
                    .ok()
                    .flatten();
                match after {
                    Some(s) if s > 0.0 => {
                        TaskError::retry_after(message, Duration::from_secs_f64(s))
                    }
                    _ => TaskError::retryable(message),
                }
            }
            _ => {
                let ty = value
                    .get_type()
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|_| "Exception".into());
                TaskError::retryable(format!("{ty}: {message}"))
            }
        }
    })
}
