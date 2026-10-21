//! Running Python coroutines as cronoq executors.

use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use cronoq::{BoxFuture, Executor, TaskError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::{PyTraverseError, PyVisit};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::oneshot;

use crate::errors::classify;
use crate::types::PyObj;

/// `(event loop, asyncio.Task)` of a running attempt, set from Python by `_invoke`.
type TaskSlot = Arc<Mutex<Option<(PyObj, PyObj)>>>;

/// Called (without the GIL held by the caller's stack) when the captured event loop is closed.
pub(crate) type LoopClosed = Arc<dyn Fn() + Send + Sync>;

/// Executes a Python coroutine function on the event loop captured at `Scheduler.start()`.
pub(crate) struct PyExecutor(Arc<Inner>);

struct Inner {
    func: PyObj,
    /// `cronoq._runtime._invoke`: registers the asyncio task, then awaits `func(ctx, payload)`.
    invoke: PyObj,
    locals: TaskLocals,
    loop_closed: LoopClosed,
}

impl PyExecutor {
    pub fn new(func: PyObj, invoke: PyObj, locals: TaskLocals, loop_closed: LoopClosed) -> Self {
        PyExecutor(Arc::new(Inner {
            func,
            invoke,
            locals,
            loop_closed,
        }))
    }
}

impl Executor<PyObj, PyObj> for PyExecutor {
    fn execute(
        &self,
        ctx: cronoq::TaskContext,
        payload: Arc<PyObj>,
    ) -> BoxFuture<Result<PyObj, TaskError>> {
        // No GIL here: this runs on the engine task. All Python work happens inside the future.
        let this = self.0.clone();
        Box::pin(async move {
            let token = ctx.cancellation_token();
            let slot: TaskSlot = Arc::new(Mutex::new(None));
            let fut = Python::attach(|py| {
                // A closed loop can never run the coroutine; stop the scheduler instead of
                // failing every task with "Event loop is closed".
                let closed = this
                    .locals
                    .event_loop(py)
                    .call_method0("is_closed")
                    .and_then(|v| v.extract::<bool>())
                    .unwrap_or(true);
                if closed {
                    return Ok(None);
                }
                let pyctx = Py::new(
                    py,
                    TaskContext {
                        inner: ctx,
                        slot: slot.clone(),
                    },
                )?;
                let coro = this.invoke.bind(py).call1((
                    this.func.bind(py),
                    pyctx,
                    payload.clone_ref(py),
                ))?;
                pyo3_async_runtimes::into_future_with_locals(&this.locals, coro).map(Some)
            });
            let fut = match fut {
                Ok(Some(f)) => f,
                Ok(None) => {
                    (this.loop_closed)();
                    return Err(TaskError::retryable(
                        "the scheduler's asyncio event loop is closed",
                    ));
                }
                Err(e) => return Err(classify(e, token.is_cancelled())),
            };
            // Propagate engine-side cancellation (cancel, timeout, lease loss) to the asyncio task.
            // `done` is dropped when this future completes or is aborted, ending the watcher.
            let (done, done_rx) = oneshot::channel::<()>();
            let watch_slot = slot.clone();
            let watch_token = token.clone();
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = watch_token.cancelled() => cancel_python_task(&watch_slot),
                    _ = done_rx => {}
                }
            });
            let r = fut.await;
            drop(done);
            // Release the asyncio task: after a failure it references the traceback, whose frames
            // reference the context (and this slot), forming a cycle the GC cannot see into.
            let held = slot.lock().unwrap().take();
            if held.is_some() {
                Python::attach(|_| drop(held));
            }
            r.map_err(|e| classify(e, token.is_cancelled()))
        })
    }
}

fn cancel_python_task(slot: &TaskSlot) {
    let Some((lp, task)) = slot.lock().unwrap().take() else {
        return;
    };
    Python::attach(|py| {
        if let Ok(cancel) = task.bind(py).getattr("cancel") {
            let _ = lp.bind(py).call_method1("call_soon_threadsafe", (cancel,));
        }
    });
}

/// Information and controls for a running attempt, passed as the executor's first argument.
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct TaskContext {
    inner: cronoq::TaskContext,
    slot: TaskSlot,
}

#[pymethods]
impl TaskContext {
    #[getter]
    fn id(&self) -> u64 {
        self.inner.id.0
    }
    #[getter]
    fn attempt(&self) -> u32 {
        self.inner.attempt
    }
    #[getter]
    fn kind(&self) -> &str {
        &self.inner.kind
    }
    #[getter]
    fn key(&self) -> Option<&str> {
        self.inner.key.as_deref()
    }
    #[getter]
    fn pool(&self) -> &str {
        &self.inner.pool
    }
    #[getter]
    fn tags<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for (k, v) in self.inner.tags.iter() {
            d.set_item(&**k, &**v)?;
        }
        Ok(d)
    }
    /// Seconds allowed for this attempt, if a timeout was set.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.inner.timeout.map(|d| d.as_secs_f64())
    }
    /// The task's deadline as Unix seconds (like `time.time()`), if one was set.
    #[getter]
    fn deadline(&self) -> Option<f64> {
        self.inner
            .deadline
            .map(|t| match t.duration_since(UNIX_EPOCH) {
                Ok(d) => d.as_secs_f64(),
                Err(e) => -e.duration().as_secs_f64(),
            })
    }
    /// True once the task was cancelled or this attempt aborted (timeout, lease loss).
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
    /// Extend the lease (required for pools created with `lease_ttl`).
    fn heartbeat(&self) {
        self.inner.heartbeat()
    }
    /// Internal: register the asyncio task running this attempt.
    fn _set_task(&self, py: Python<'_>, event_loop: PyObj, task: PyObj) {
        // Check under the lock so a concurrent cancellation either sees the task or we see it.
        let mut slot = self.slot.lock().unwrap();
        if self.inner.is_cancelled() {
            if let Ok(cancel) = task.bind(py).getattr("cancel") {
                let _ = event_loop
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (cancel,));
            }
            return;
        }
        *slot = Some((event_loop, task));
    }
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Ok(slot) = self.slot.try_lock() {
            if let Some((lp, task)) = &*slot {
                visit.call(lp)?;
                visit.call(task)?;
            }
        }
        Ok(())
    }
    fn __clear__(&self) {
        if let Ok(mut slot) = self.slot.try_lock() {
            slot.take();
        }
    }
    fn __repr__(&self) -> String {
        format!(
            "TaskContext(id={}, attempt={}, pool={:?}, kind={:?}, key={:?})",
            self.inner.id.0,
            self.inner.attempt,
            &*self.inner.pool,
            &*self.inner.kind,
            self.inner.key
        )
    }
}
