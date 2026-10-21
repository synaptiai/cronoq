//! The asyncio-facing `Scheduler`, `TaskHandle` and event stream.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use cronoq::{Recurring, ResourceConfig, StatusEvent as CoreEvent, TaskFailure, TaskId, TaskRef};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::{Notify, broadcast, watch};

use crate::errors::{failure_to_pyerr, not_started, py_error, stopped, to_pyerr};
use crate::executor::{LoopClosed, PyExecutor};
use crate::types::{
    Dep, Opts, Pool, PyObj, Resource, RetryPolicy, Task, extract_ref, parse_miss, parse_overlap,
    secs,
};

type Sched = cronoq::Scheduler<PyObj, PyObj>;

/// Python `None` as a return value of an awaitable (instead of `()`, which becomes `()`).
const NONE: Option<bool> = None;

/// Shutdown grace used for `float("inf")` ("wait for running work as long as it takes").
const FOREVER: Duration = Duration::from_secs(100 * 365 * 86_400);

fn spawn_py<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    pyo3_async_runtimes::tokio::future_into_py(py, fut)
}

fn runtime() -> &'static tokio::runtime::Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

fn grace_duration(grace: f64) -> PyResult<Duration> {
    if grace == f64::INFINITY {
        return Ok(FOREVER);
    }
    Ok(secs(grace, "grace")?.min(FOREVER))
}

/// Lifecycle of the engine behind a `Scheduler`.
enum Life {
    Idle,
    /// `start()` is building the engine (the id tells a stale start from the current one).
    Starting(u64),
    Running(Sched),
    /// `shutdown()` is in progress; the API keeps working until it completes.
    Stopping(Sched),
}

struct Shared {
    life: Mutex<Life>,
    /// Signalled on every lifecycle change.
    changed: Notify,
    starts: AtomicU64,
    /// The event loop captured by `start()` (checked for being closed).
    event_loop: Mutex<Option<PyObj>>,
    /// Why the engine stopped on its own, reported by later calls.
    stopped_reason: Mutex<Option<String>>,
}

impl Shared {
    /// Stop the engine because it can no longer run executors (e.g. its loop was closed).
    fn stop_unexpectedly(self: &Arc<Self>, reason: &str) {
        let s = {
            let mut life = self.life.lock().unwrap();
            match &*life {
                Life::Running(s) => {
                    let s = s.clone();
                    *life = Life::Stopping(s.clone());
                    s
                }
                _ => return,
            }
        };
        *self.stopped_reason.lock().unwrap() = Some(reason.to_string());
        let shared = self.clone();
        runtime().spawn(async move {
            let _ = s.shutdown(Duration::ZERO).await;
            let mut life = shared.life.lock().unwrap();
            if matches!(&*life, Life::Stopping(_)) {
                *life = Life::Idle;
            }
            drop(life);
            shared.changed.notify_waiters();
        });
    }
}

/// Elastic priority scheduler. Create, `await start()`, submit work, `await shutdown()` (or use
/// `async with`).
///
/// Executors run on the asyncio event loop that called `start()`; that loop must outlive the
/// scheduler. If it is closed first, the scheduler stops and later calls raise `SchedulerError`.
#[pyclass(module = "cronoq._cronoq")]
pub struct Scheduler {
    pools: Vec<Py<Pool>>,
    resources: Vec<ResourceConfig>,
    emit_transitions: bool,
    shutdown_grace: f64,
    event_capacity: usize,
    default_pool: Arc<str>,
    pool_names: Arc<[Arc<str>]>,
    shared: Arc<Shared>,
}

impl Scheduler {
    /// The running engine (also while shutting down).
    fn sched(&self, py: Python<'_>) -> PyResult<Sched> {
        let s = match &*self.shared.life.lock().unwrap() {
            Life::Running(s) | Life::Stopping(s) => Some(s.clone()),
            _ => None,
        };
        let Some(s) = s else {
            return Err(match &*self.shared.stopped_reason.lock().unwrap() {
                Some(reason) => stopped(reason),
                None => not_started(),
            });
        };
        let closed = self
            .shared
            .event_loop
            .lock()
            .unwrap()
            .as_ref()
            .map(|lp| {
                lp.bind(py)
                    .call_method0("is_closed")
                    .and_then(|v| v.extract::<bool>())
                    .unwrap_or(true)
            })
            .unwrap_or(false);
        if closed {
            let reason = "its asyncio event loop was closed (call start() again on a running loop)";
            self.shared.stop_unexpectedly(reason);
            return Err(stopped(reason));
        }
        Ok(s)
    }

    fn task(&self, opts: &Opts, payload: PyObj) -> cronoq::Task<PyObj> {
        opts.to_task(payload, &self.default_pool)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        // Garbage-collected without `shutdown()`: stop the engine rather than leaking it.
        let s = match &*self.shared.life.lock().unwrap() {
            Life::Running(s) => Some(s.clone()),
            _ => None,
        };
        if let Some(s) = s {
            runtime().spawn(async move {
                let _ = s.shutdown(Duration::ZERO).await;
            });
        }
    }
}

#[pymethods]
impl Scheduler {
    /// `pools` must have unique names. Tasks submitted without `pool=` go to the pool named
    /// `"default"`, or to the first pool if none has that name.
    ///
    /// `shutdown_grace` is the grace period used by `async with`; `event_capacity` bounds the
    /// buffer of each `events()` stream.
    #[new]
    #[pyo3(signature = (pools, resources=None, emit_transitions=false, *, shutdown_grace=5.0, event_capacity=1024))]
    fn new(
        pools: Vec<Py<Pool>>,
        resources: Option<Vec<Resource>>,
        emit_transitions: bool,
        shutdown_grace: f64,
        event_capacity: usize,
    ) -> PyResult<Self> {
        if pools.is_empty() {
            return Err(PyValueError::new_err("at least one pool is required"));
        }
        let names: Vec<Arc<str>> = pools.iter().map(|p| p.get().cfg.name.clone()).collect();
        let mut seen = HashSet::new();
        for n in &names {
            if !seen.insert(n.clone()) {
                return Err(PyValueError::new_err(format!("duplicate pool name '{n}'")));
            }
        }
        grace_duration(shutdown_grace)?;
        if event_capacity == 0 {
            return Err(PyValueError::new_err("event_capacity must be >= 1"));
        }
        let default_pool = names
            .iter()
            .find(|n| &***n == "default")
            .unwrap_or(&names[0])
            .clone();
        Ok(Scheduler {
            pools,
            resources: resources
                .unwrap_or_default()
                .into_iter()
                .map(|r| r.inner)
                .collect(),
            emit_transitions,
            shutdown_grace,
            event_capacity,
            default_pool,
            pool_names: names.into(),
            shared: Arc::new(Shared {
                life: Mutex::new(Life::Idle),
                changed: Notify::new(),
                starts: AtomicU64::new(0),
                event_loop: Mutex::new(None),
                stopped_reason: Mutex::new(None),
            }),
        })
    }

    /// Start the engine. Executors will run on the current event loop.
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        let id = {
            let mut life = self.shared.life.lock().unwrap();
            match &*life {
                Life::Idle => {}
                Life::Starting(_) => {
                    return Err(PyRuntimeError::new_err("scheduler is already starting"));
                }
                Life::Running(_) | Life::Stopping(_) => {
                    return Err(PyRuntimeError::new_err("scheduler already started"));
                }
            }
            let id = self.shared.starts.fetch_add(1, Ordering::SeqCst) + 1;
            *life = Life::Starting(id);
            id
        };
        *self.shared.stopped_reason.lock().unwrap() = None;
        *self.shared.event_loop.lock().unwrap() = Some(locals.event_loop(py).unbind());
        let invoke = py.import("cronoq._runtime")?.getattr("_invoke")?.unbind();
        let weak = Arc::downgrade(&self.shared);
        let loop_closed: LoopClosed = Arc::new(move || {
            if let Some(shared) = weak.upgrade() {
                shared.stop_unexpectedly("its asyncio event loop was closed");
            }
        });
        let mut b = Sched::builder().emit_transitions(self.emit_transitions);
        for p in &self.pools {
            let p = p.get();
            let exec = PyExecutor::new(
                p.executor.clone_ref(py),
                invoke.clone_ref(py),
                locals.clone(),
                loop_closed.clone(),
            );
            let cfg = p.cfg.clone();
            b = b.pool(
                cronoq::Pool::new(cfg.name.clone())
                    .configure(move |c| *c = cfg)
                    .executor(exec),
            );
        }
        for r in &self.resources {
            b = b.resource(r.clone());
        }
        let shared = self.shared.clone();
        spawn_py(py, async move {
            let built = b.build().await;
            let current = {
                let mut life = shared.life.lock().unwrap();
                let current = matches!(&*life, Life::Starting(i) if *i == id);
                if current {
                    *life = match &built {
                        Ok(s) => Life::Running(s.clone()),
                        Err(_) => Life::Idle,
                    };
                }
                current
            };
            shared.changed.notify_waiters();
            let s = built.map_err(to_pyerr)?;
            if !current {
                // `shutdown()` was called while starting: discard the engine we just built.
                let _ = s.shutdown(Duration::ZERO).await;
                return Err(Python::attach(|py| {
                    py_error(
                        py,
                        "SchedulerError",
                        ("scheduler was shut down while starting",),
                    )
                }));
            }
            Ok(NONE)
        })
    }

    /// Submit a task; resolves to a `TaskHandle` (await it for the result).
    #[pyo3(signature = (
        payload, *, priority=None, pool=None, kind=None, key=None, deadline_in=None,
        deadline_at=None, start_in=None, start_at=None, tolerance=None, on_miss=None,
        duration_hint=None, timeout=None, retry=None, after=None, deps=None, resources=None,
        tags=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn submit<'py>(
        &self,
        py: Python<'py>,
        payload: PyObj,
        priority: Option<&Bound<'py, PyAny>>,
        pool: Option<&str>,
        kind: Option<&str>,
        key: Option<&str>,
        deadline_in: Option<f64>,
        deadline_at: Option<&Bound<'py, PyAny>>,
        start_in: Option<f64>,
        start_at: Option<&Bound<'py, PyAny>>,
        tolerance: Option<f64>,
        on_miss: Option<&str>,
        duration_hint: Option<f64>,
        timeout: Option<f64>,
        retry: Option<RetryPolicy>,
        after: Option<&Bound<'py, PyAny>>,
        deps: Option<Vec<Dep>>,
        resources: Option<&Bound<'py, PyDict>>,
        tags: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let opts = Opts::parse(
            priority,
            pool,
            kind,
            key,
            deadline_in,
            deadline_at,
            start_in,
            start_at,
            tolerance,
            on_miss,
            duration_hint,
            timeout,
            retry,
            after,
            deps,
            resources,
            tags,
        )?;
        let s = self.sched(py)?;
        let task = self.task(&opts, payload);
        spawn_py(py, async move {
            let h = s.submit(task).await.map_err(to_pyerr)?;
            Ok(TaskHandle::new(h, s))
        })
    }

    /// Submit without a result handle (lowest overhead); resolves to the task id.
    #[pyo3(signature = (
        payload, *, priority=None, pool=None, kind=None, key=None, deadline_in=None,
        deadline_at=None, start_in=None, start_at=None, tolerance=None, on_miss=None,
        duration_hint=None, timeout=None, retry=None, after=None, deps=None, resources=None,
        tags=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue<'py>(
        &self,
        py: Python<'py>,
        payload: PyObj,
        priority: Option<&Bound<'py, PyAny>>,
        pool: Option<&str>,
        kind: Option<&str>,
        key: Option<&str>,
        deadline_in: Option<f64>,
        deadline_at: Option<&Bound<'py, PyAny>>,
        start_in: Option<f64>,
        start_at: Option<&Bound<'py, PyAny>>,
        tolerance: Option<f64>,
        on_miss: Option<&str>,
        duration_hint: Option<f64>,
        timeout: Option<f64>,
        retry: Option<RetryPolicy>,
        after: Option<&Bound<'py, PyAny>>,
        deps: Option<Vec<Dep>>,
        resources: Option<&Bound<'py, PyDict>>,
        tags: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let opts = Opts::parse(
            priority,
            pool,
            kind,
            key,
            deadline_in,
            deadline_at,
            start_in,
            start_at,
            tolerance,
            on_miss,
            duration_hint,
            timeout,
            retry,
            after,
            deps,
            resources,
            tags,
        )?;
        let s = self.sched(py)?;
        let task = self.task(&opts, payload);
        spawn_py(py, async move {
            let sub = s.enqueue(task).await.map_err(to_pyerr)?;
            Ok(sub.id.0)
        })
    }

    /// Submit `Task`s atomically (dependencies may reference keys in the batch).
    fn submit_batch<'py>(
        &self,
        py: Python<'py>,
        tasks: Vec<Py<Task>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched(py)?;
        let tasks: Vec<_> = tasks
            .iter()
            .map(|t| {
                let t = t.get();
                self.task(&t.opts, t.payload.clone_ref(py))
            })
            .collect();
        spawn_py(py, async move {
            let hs = s.submit_batch(tasks).await.map_err(to_pyerr)?;
            Ok(hs
                .into_iter()
                .map(|h| TaskHandle::new(h, s.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// Cancel by key (str) or id (int); resolves to whether the task was live.
    fn cancel<'py>(
        &self,
        py: Python<'py>,
        target: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (s, target) = (self.sched(py)?, extract_ref(target)?);
        spawn_py(py, async move { s.cancel(target).await.map_err(to_pyerr) })
    }

    /// Status name (e.g. "running", "succeeded") or None if unknown.
    fn status<'py>(
        &self,
        py: Python<'py>,
        target: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (s, target) = (self.sched(py)?, extract_ref(target)?);
        spawn_py(py, async move {
            Ok(s.status(target)
                .await
                .map_err(to_pyerr)?
                .map(|st| st.name()))
        })
    }

    /// A handle for an existing task (by key or id), or None.
    fn handle<'py>(
        &self,
        py: Python<'py>,
        target: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (s, target) = (self.sched(py)?, extract_ref(target)?);
        spawn_py(py, async move {
            let h = s.handle(target).await.map_err(to_pyerr)?;
            Ok(h.map(|h| TaskHandle::new(h, s)))
        })
    }

    /// Quality metrics per pool over the metrics window.
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched(py)?;
        spawn_py(py, async move {
            Ok(Snapshot::from(s.snapshot().await.map_err(to_pyerr)?))
        })
    }

    /// Report capacity for pools whose workers are managed externally.
    fn set_capacity<'py>(
        &self,
        py: Python<'py>,
        pool: String,
        capacity: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched(py)?;
        spawn_py(py, async move {
            s.set_capacity(&pool, capacity).await.map_err(to_pyerr)?;
            Ok(NONE)
        })
    }

    /// Run `payload` on a cron schedule (5 fields, or 6 with leading seconds).
    #[pyo3(signature = (
        name, cron, payload, *, tz=None, overlap="skip", tolerance=1.0, on_miss="run_late",
        priority=None, pool=None, kind=None, duration_hint=None, timeout=None, retry=None,
        resources=None, tags=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn add_recurring<'py>(
        &self,
        py: Python<'py>,
        name: String,
        cron: String,
        payload: PyObj,
        tz: Option<String>,
        overlap: &str,
        tolerance: f64,
        on_miss: &str,
        priority: Option<&Bound<'py, PyAny>>,
        pool: Option<&str>,
        kind: Option<&str>,
        duration_hint: Option<f64>,
        timeout: Option<f64>,
        retry: Option<RetryPolicy>,
        resources: Option<&Bound<'py, PyDict>>,
        tags: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let opts = Opts::parse(
            priority,
            pool,
            kind,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            duration_hint,
            timeout,
            retry,
            None,
            None,
            resources,
            tags,
        )?;
        let mut r = Recurring::new(name, cron, self.task(&opts, payload))
            .overlap(parse_overlap(overlap)?)
            .on_miss(parse_miss(on_miss)?)
            .tolerance(secs(tolerance, "tolerance")?);
        if let Some(tz) = tz {
            r = r.tz(tz);
        }
        let s = self.sched(py)?;
        spawn_py(py, async move {
            s.add_recurring(r).await.map_err(to_pyerr)?;
            Ok(NONE)
        })
    }

    /// Remove a cron schedule; resolves to whether it existed.
    fn remove_recurring<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched(py)?;
        spawn_py(py, async move {
            s.remove_recurring(&name).await.map_err(to_pyerr)
        })
    }

    /// Stream of task status events (`async for event in scheduler.events(): ...`).
    ///
    /// Terminal events are always delivered; every transition is delivered when the scheduler was
    /// created with `emit_transitions=True`. Events are buffered per stream (up to
    /// `event_capacity`); when a slow consumer falls behind, the oldest events are dropped and
    /// counted in `stream.lagged`, or, with `on_lag="raise"`, the next `__anext__` raises
    /// `EventsLagged` (iteration can continue afterwards). The stream ends when the scheduler
    /// stops or `close()` is called.
    #[pyo3(signature = (on_lag="skip"))]
    fn events(&self, py: Python<'_>, on_lag: &str) -> PyResult<EventStream> {
        let raise = match on_lag {
            "skip" => false,
            "raise" => true,
            _ => return Err(PyValueError::new_err("on_lag must be 'skip' or 'raise'")),
        };
        let s = self.sched(py)?;
        Ok(EventStream::spawn(
            s,
            self.pool_names.clone(),
            self.event_capacity,
            raise,
        ))
    }

    /// Stop accepting work, let running attempts finish for up to `grace` seconds
    /// (`float("inf")` waits as long as they take), then abort them. The API keeps working
    /// (for example from running executors) until shutdown completes.
    #[pyo3(signature = (grace=5.0))]
    fn shutdown<'py>(&self, py: Python<'py>, grace: f64) -> PyResult<Bound<'py, PyAny>> {
        let grace = grace_duration(grace)?;
        let shared = self.shared.clone();
        // Cancel a pending `start()` synchronously, at call time (as `start()` claims its state
        // at call time). Deciding inside the spawned future would race with the build.
        {
            let mut life = shared.life.lock().unwrap();
            if matches!(&*life, Life::Starting(_)) {
                *life = Life::Idle;
                drop(life);
                shared.changed.notify_waiters();
            }
        }
        spawn_py(py, async move {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let s = {
                    let mut life = shared.life.lock().unwrap();
                    match &*life {
                        Life::Idle => return Ok(NONE),
                        Life::Starting(_) => {
                            // The pending `start()` sees this and discards its engine.
                            *life = Life::Idle;
                            drop(life);
                            shared.changed.notify_waiters();
                            return Ok(NONE);
                        }
                        Life::Running(s) => {
                            let s = s.clone();
                            *life = Life::Stopping(s.clone());
                            Some(s)
                        }
                        // Another shutdown is in progress: wait for it.
                        Life::Stopping(_) => None,
                    }
                };
                match s {
                    Some(s) => {
                        let r = s.shutdown(grace).await;
                        {
                            let mut life = shared.life.lock().unwrap();
                            if matches!(&*life, Life::Stopping(_)) {
                                *life = Life::Idle;
                            }
                        }
                        shared.changed.notify_waiters();
                        r.map_err(to_pyerr)?;
                        return Ok(NONE);
                    }
                    None => changed.await,
                }
            }
        })
    }

    /// True from a successful `start()` until `shutdown()` completes.
    #[getter]
    fn started(&self) -> bool {
        matches!(
            &*self.shared.life.lock().unwrap(),
            Life::Running(_) | Life::Stopping(_)
        )
    }

    #[getter]
    fn pools(&self) -> Vec<String> {
        self.pool_names.iter().map(|n| n.to_string()).collect()
    }

    /// The pool used when `submit` is called without `pool=`.
    #[getter]
    fn default_pool(&self) -> &str {
        &self.default_pool
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let start = slf.borrow().start(py)?;
        // Await start(), then return self.
        py.import("cronoq._runtime")?
            .getattr("_then_return")?
            .call1((start, slf.clone()))
    }

    #[pyo3(signature = (*_args))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.shutdown(py, self.shutdown_grace)
    }
}

type Outcome = Result<Arc<PyObj>, TaskFailure>;

/// Awaitable handle to a submitted task. `await handle` returns the executor's result or
/// raises `TaskFailed`. May be awaited any number of times.
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct TaskHandle {
    id: u64,
    duplicate: bool,
    result: watch::Receiver<Option<Outcome>>,
    sched: Sched,
}

impl TaskHandle {
    fn new(h: cronoq::TaskHandle<PyObj>, sched: Sched) -> Self {
        let (tx, rx) = watch::channel(None);
        let (id, duplicate) = (h.id().0, h.is_duplicate());
        // Resolve in the background so `done()` reflects completion without being awaited.
        runtime().spawn(async move {
            let r = h.await;
            let _ = tx.send(Some(r));
        });
        TaskHandle {
            id,
            duplicate,
            result: rx,
            sched,
        }
    }
}

#[pymethods]
impl TaskHandle {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }
    /// True if the submission matched an existing key.
    #[getter]
    fn duplicate(&self) -> bool {
        self.duplicate
    }
    /// Coroutine resolving to the result (same as awaiting the handle).
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (mut rx, id) = (self.result.clone(), self.id);
        spawn_py(py, async move {
            let r = match rx.wait_for(Option::is_some).await {
                Ok(v) => v.clone().expect("checked"),
                Err(_) => Err(TaskFailure::Shutdown),
            };
            Python::attach(|py| match r {
                Ok(o) => Ok(o.clone_ref(py)),
                Err(f) => Err(failure_to_pyerr(py, &f, id)),
            })
        })
    }
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.result(py)?.call_method0("__await__")
    }
    /// True once the task has finished (whether or not the handle was awaited).
    fn done(&self) -> bool {
        self.result.borrow().is_some()
    }
    /// Request cancellation (fire and forget).
    fn cancel(&self) {
        let (s, id) = (self.sched.clone(), self.id);
        runtime().spawn(async move {
            let _ = s.cancel(TaskRef::Id(TaskId(id))).await;
        });
    }
    fn __repr__(&self) -> String {
        format!(
            "TaskHandle(id={}, duplicate={}, done={})",
            self.id,
            if self.duplicate { "True" } else { "False" },
            if self.done() { "True" } else { "False" }
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Events

/// A task status transition.
#[pyclass(frozen, get_all, module = "cronoq._cronoq")]
#[derive(Clone, Debug)]
pub struct StatusEvent {
    /// Task id.
    id: u64,
    /// Pool name.
    pool: String,
    /// Task key, if it has one.
    key: Option<String>,
    /// Status name, e.g. `"running"` or `"succeeded"`.
    status: String,
    /// Attempt number (0 before the first dispatch).
    attempt: u32,
    /// When it happened, as Unix seconds (like `time.time()`).
    at: f64,
    /// Failure or skip reason, if any.
    message: Option<String>,
}

#[pymethods]
impl StatusEvent {
    fn __repr__(&self) -> String {
        format!(
            "StatusEvent(id={}, pool={:?}, key={:?}, status={:?}, attempt={})",
            self.id, self.pool, self.key, self.status, self.attempt
        )
    }
}

struct StreamState {
    queue: Mutex<VecDeque<StatusEvent>>,
    capacity: usize,
    /// Dropped since the last `on_lag="raise"` report.
    unreported: AtomicU64,
    /// Dropped in total.
    lagged: AtomicU64,
    /// No more events will arrive (scheduler stopped or stream closed).
    ended: AtomicBool,
    notify: Notify,
}

impl StreamState {
    fn push(&self, e: StatusEvent) {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= self.capacity {
            q.pop_front();
            self.lagged.fetch_add(1, Ordering::Relaxed);
            self.unreported.fetch_add(1, Ordering::Relaxed);
        }
        q.push_back(e);
        drop(q);
        self.notify.notify_one();
    }
    fn lag(&self, n: u64) {
        self.lagged.fetch_add(n, Ordering::Relaxed);
        self.unreported.fetch_add(n, Ordering::Relaxed);
        self.notify.notify_one();
    }
    fn end(&self) {
        self.ended.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }
}

enum Next {
    Event(StatusEvent),
    Lagged(u64),
    End,
}

/// Async iterator of `StatusEvent`s returned by `Scheduler.events()`.
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct EventStream {
    state: Arc<StreamState>,
    raise_on_lag: bool,
    relay: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl EventStream {
    fn spawn(s: Sched, pools: Arc<[Arc<str>]>, capacity: usize, raise_on_lag: bool) -> Self {
        let state = Arc::new(StreamState {
            queue: Mutex::new(VecDeque::new()),
            capacity,
            unreported: AtomicU64::new(0),
            lagged: AtomicU64::new(0),
            ended: AtomicBool::new(false),
            notify: Notify::new(),
        });
        let mut rx = s.events();
        let clock = *s.clock();
        let st = state.clone();
        let relay = runtime().spawn(async move {
            // Task handles keep scheduler clones (and so the event sender) alive after
            // shutdown, so end on `stopped()` rather than waiting for the channel to close.
            let stopped = s.stopped();
            tokio::pin!(stopped);
            loop {
                tokio::select! {
                    r = rx.recv() => match r {
                        Ok(e) => st.push(convert(&e, &pools, &clock)),
                        Err(broadcast::error::RecvError::Lagged(n)) => st.lag(n),
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = &mut stopped => {
                        while let Ok(e) = rx.try_recv() {
                            st.push(convert(&e, &pools, &clock));
                        }
                        break;
                    }
                }
            }
            st.end();
        });
        EventStream {
            state,
            raise_on_lag,
            relay: Mutex::new(Some(relay)),
        }
    }

    fn take_next(&self) -> Option<Next> {
        if self.raise_on_lag {
            let n = self.state.unreported.swap(0, Ordering::Relaxed);
            if n > 0 {
                return Some(Next::Lagged(n));
            }
        }
        if let Some(e) = self.state.queue.lock().unwrap().pop_front() {
            return Some(Next::Event(e));
        }
        self.state.ended.load(Ordering::SeqCst).then_some(Next::End)
    }
}

fn convert(e: &CoreEvent, pools: &[Arc<str>], clock: &cronoq::Clock) -> StatusEvent {
    let at = match clock.to_system(e.at).duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(err) => -err.duration().as_secs_f64(),
    };
    StatusEvent {
        id: e.id.0,
        pool: pools
            .get(e.pool.0 as usize)
            .map(|p| p.to_string())
            .unwrap_or_default(),
        key: e.key.as_deref().map(str::to_string),
        status: e.status.name().to_string(),
        attempt: e.attempt,
        at,
        message: e.message.as_deref().map(str::to_string),
    }
}

#[pymethods]
impl EventStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        spawn_py(py, async move {
            let state = slf.get().state.clone();
            loop {
                let notified = state.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let next = slf.get().take_next();
                match next {
                    Some(Next::Event(e)) => return Ok(e),
                    Some(Next::Lagged(n)) => {
                        return Err(Python::attach(|py| py_error(py, "EventsLagged", (n,))));
                    }
                    Some(Next::End) => return Err(PyStopAsyncIteration::new_err(())),
                    None => notified.await,
                }
            }
        })
    }

    /// Stop receiving events. Buffered events are discarded and iteration ends.
    fn close(&self) {
        if let Some(r) = self.relay.lock().unwrap().take() {
            r.abort();
        }
        self.state.queue.lock().unwrap().clear();
        self.state.end();
    }

    /// Coroutine form of `close()` (for `contextlib.aclosing`).
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.close();
        spawn_py(py, async { Ok(NONE) })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        spawn_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (*_args))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.aclose(py)
    }

    /// Number of events dropped because this stream fell behind.
    #[getter]
    fn lagged(&self) -> u64 {
        self.state.lagged.load(Ordering::Relaxed)
    }

    /// True once no more events will be delivered.
    #[getter]
    fn closed(&self) -> bool {
        self.state.ended.load(Ordering::SeqCst) && self.state.queue.lock().unwrap().is_empty()
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if let Some(r) = self.relay.lock().unwrap().take() {
            r.abort();
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Snapshots

/// Metrics for one pool (durations in seconds).
#[pyclass(frozen, get_all, module = "cronoq._cronoq")]
pub struct PoolSnapshot {
    name: String,
    capacity: u32,
    busy: u32,
    held: u32,
    desired: u32,
    pending: usize,
    pending_by_class: Vec<usize>,
    ready: usize,
    reservations: usize,
    window: f64,
    dispatched: u64,
    succeeded: u64,
    failed: u64,
    retried: u64,
    priority_adherence: f64,
    fixed_start_hit_rate: f64,
    utilization: f64,
    schedule_efficiency: f64,
    wait_mean: f64,
    wait_p50: f64,
    wait_p90: f64,
    wait_p99: f64,
    run_p50: f64,
    run_p99: f64,
    worker_seconds: f64,
    cost: f64,
}

#[pymethods]
impl PoolSnapshot {
    fn __repr__(&self) -> String {
        format!(
            "PoolSnapshot(name={:?}, capacity={}, busy={}, pending={}, utilization={:.3}, priority_adherence={:.3})",
            self.name,
            self.capacity,
            self.busy,
            self.pending,
            self.utilization,
            self.priority_adherence
        )
    }
}

/// Point-in-time scheduler metrics.
#[pyclass(frozen, get_all, module = "cronoq._cronoq")]
pub struct Snapshot {
    pools: Vec<Py<PoolSnapshot>>,
    live_tasks: usize,
    running: usize,
    pending: usize,
    totals: std::collections::BTreeMap<String, u64>,
}

#[pymethods]
impl Snapshot {
    /// The snapshot for pool `name`, or None.
    fn pool(&self, py: Python<'_>, name: &str) -> Option<Py<PoolSnapshot>> {
        self.pools
            .iter()
            .find(|p| p.get().name == name)
            .map(|p| p.clone_ref(py))
    }
    fn __repr__(&self) -> String {
        format!(
            "Snapshot(live_tasks={}, running={}, pending={})",
            self.live_tasks, self.running, self.pending
        )
    }
}

impl From<cronoq::Snapshot> for Snapshot {
    fn from(s: cronoq::Snapshot) -> Self {
        let t = &s.totals;
        let totals = [
            ("submitted", t.submitted),
            ("dispatched", t.dispatched),
            ("succeeded", t.succeeded),
            ("failed", t.failed),
            ("dead_lettered", t.dead_lettered),
            ("cancelled", t.cancelled),
            ("skipped", t.skipped),
            ("missed", t.missed),
            ("shed", t.shed),
            ("retries", t.retries),
            ("timeouts", t.timeouts),
            ("lease_expiries", t.lease_expiries),
            ("recurring_conflicts", t.recurring_conflicts),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let pools = Python::attach(|py| {
            s.pools
                .iter()
                .map(|p| {
                    Py::new(
                        py,
                        PoolSnapshot {
                            name: p.name.to_string(),
                            capacity: p.capacity,
                            busy: p.busy,
                            held: p.held,
                            desired: p.desired,
                            pending: p.pending(),
                            pending_by_class: p.pending_by_class.to_vec(),
                            ready: p.ready,
                            reservations: p.reservations,
                            window: p.window.as_secs_f64(),
                            dispatched: p.dispatched,
                            succeeded: p.succeeded,
                            failed: p.failed,
                            retried: p.retried,
                            priority_adherence: p.priority_adherence,
                            fixed_start_hit_rate: p.fixed_start_hit_rate,
                            utilization: p.utilization,
                            schedule_efficiency: p.schedule_efficiency,
                            wait_mean: p.wait_mean.as_secs_f64(),
                            wait_p50: p.wait_p50.as_secs_f64(),
                            wait_p90: p.wait_p90.as_secs_f64(),
                            wait_p99: p.wait_p99.as_secs_f64(),
                            run_p50: p.run_p50.as_secs_f64(),
                            run_p99: p.run_p99.as_secs_f64(),
                            worker_seconds: p.worker_seconds,
                            cost: p.cost,
                        },
                    )
                    .expect("allocate PoolSnapshot")
                })
                .collect()
        });
        Snapshot {
            pools,
            live_tasks: s.live_tasks,
            running: s.running,
            pending: s.pending,
            totals,
        }
    }
}
