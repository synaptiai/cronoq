//! The asyncio-facing `Scheduler` and `TaskHandle`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cronoq::{Recurring, ResourceConfig, TaskFailure, TaskId, TaskRef};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::errors::{failure_to_pyerr, not_started, to_pyerr};
use crate::executor::PyExecutor;
use crate::types::{
    Dep, Opts, Pool, PyObj, Resource, RetryPolicy, Task, extract_ref, parse_miss, parse_overlap,
};

type Sched = cronoq::Scheduler<PyObj, PyObj>;

fn spawn_py<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    pyo3_async_runtimes::tokio::future_into_py(py, fut)
}

/// Elastic priority scheduler. Create, `await start()`, submit work, `await shutdown()`.
///
/// Executors run on the asyncio event loop that called `start()`.
#[pyclass(module = "cronoq._cronoq")]
pub struct Scheduler {
    pools: Vec<Py<Pool>>,
    resources: Vec<ResourceConfig>,
    emit_transitions: bool,
    inner: Arc<Mutex<Option<Sched>>>,
}

impl Scheduler {
    fn sched(&self) -> PyResult<Sched> {
        self.inner.lock().unwrap().clone().ok_or_else(not_started)
    }
}

#[pymethods]
impl Scheduler {
    #[new]
    #[pyo3(signature = (pools, resources=None, emit_transitions=false))]
    fn new(
        pools: Vec<Py<Pool>>,
        resources: Option<Vec<Resource>>,
        emit_transitions: bool,
    ) -> PyResult<Self> {
        if pools.is_empty() {
            return Err(PyValueError::new_err("at least one pool is required"));
        }
        Ok(Scheduler {
            pools,
            resources: resources
                .unwrap_or_default()
                .into_iter()
                .map(|r| r.inner)
                .collect(),
            emit_transitions,
            inner: Arc::new(Mutex::new(None)),
        })
    }

    /// Start the engine. Executors will run on the current event loop.
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.inner.lock().unwrap().is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "scheduler already started",
            ));
        }
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        let invoke = py.import("cronoq._runtime")?.getattr("_invoke")?.unbind();
        let mut b = Sched::builder().emit_transitions(self.emit_transitions);
        for p in &self.pools {
            let p = p.get();
            let exec = PyExecutor::new(
                p.executor.clone_ref(py),
                invoke.clone_ref(py),
                locals.clone(),
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
        let inner = self.inner.clone();
        spawn_py(py, async move {
            let s = b.build().await.map_err(to_pyerr)?;
            *inner.lock().unwrap() = Some(s);
            Ok(())
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
        let s = self.sched()?;
        let task = opts.to_task(payload);
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
        let s = self.sched()?;
        let task = opts.to_task(payload);
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
        let s = self.sched()?;
        let tasks: Vec<_> = tasks
            .iter()
            .map(|t| {
                let t = t.get();
                t.opts.to_task(t.payload.clone_ref(py))
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
        let (s, target) = (self.sched()?, extract_ref(target)?);
        spawn_py(py, async move { s.cancel(target).await.map_err(to_pyerr) })
    }

    /// Status name (e.g. "running", "succeeded") or None if unknown.
    fn status<'py>(
        &self,
        py: Python<'py>,
        target: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (s, target) = (self.sched()?, extract_ref(target)?);
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
        let (s, target) = (self.sched()?, extract_ref(target)?);
        spawn_py(py, async move {
            let h = s.handle(target).await.map_err(to_pyerr)?;
            Ok(h.map(|h| TaskHandle::new(h, s)))
        })
    }

    /// Quality metrics per pool over the metrics window.
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched()?;
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
        let s = self.sched()?;
        spawn_py(py, async move {
            s.set_capacity(&pool, capacity).await.map_err(to_pyerr)
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
        let mut r = Recurring::new(name, cron, opts.to_task(payload))
            .overlap(parse_overlap(overlap)?)
            .on_miss(parse_miss(on_miss)?)
            .tolerance(Duration::from_secs_f64(tolerance.max(0.0)));
        if let Some(tz) = tz {
            r = r.tz(tz);
        }
        let s = self.sched()?;
        spawn_py(
            py,
            async move { s.add_recurring(r).await.map_err(to_pyerr) },
        )
    }

    fn remove_recurring<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let s = self.sched()?;
        spawn_py(py, async move {
            s.remove_recurring(&name).await.map_err(to_pyerr)
        })
    }

    /// Stop accepting work, let running attempts finish for up to `grace` seconds, then abort.
    #[pyo3(signature = (grace=5.0))]
    fn shutdown<'py>(&self, py: Python<'py>, grace: f64) -> PyResult<Bound<'py, PyAny>> {
        let s = self.inner.lock().unwrap().take();
        spawn_py(py, async move {
            if let Some(s) = s {
                s.shutdown(Duration::from_secs_f64(grace.max(0.0)))
                    .await
                    .map_err(to_pyerr)?;
            }
            Ok(())
        })
    }

    #[getter]
    fn started(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }

    #[getter]
    fn pools(&self, py: Python<'_>) -> Vec<String> {
        self.pools
            .iter()
            .map(|p| p.bind(py).get().cfg.name.to_string())
            .collect()
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
        self.shutdown(py, 5.0)
    }
}

enum HState {
    Pending(cronoq::TaskHandle<PyObj>),
    Done(Result<Arc<PyObj>, TaskFailure>),
}

/// Awaitable handle to a submitted task. `await handle` returns the executor's result or
/// raises `TaskFailed`. May be awaited any number of times.
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct TaskHandle {
    id: u64,
    duplicate: bool,
    state: Arc<tokio::sync::Mutex<HState>>,
    sched: Sched,
}

impl TaskHandle {
    fn new(h: cronoq::TaskHandle<PyObj>, sched: Sched) -> Self {
        TaskHandle {
            id: h.id().0,
            duplicate: h.is_duplicate(),
            state: Arc::new(tokio::sync::Mutex::new(HState::Pending(h))),
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
        let (state, id) = (self.state.clone(), self.id);
        spawn_py(py, async move {
            let mut g = state.lock().await;
            if let HState::Pending(h) = &mut *g {
                let r = h.await;
                *g = HState::Done(r);
            }
            let HState::Done(r) = &*g else { unreachable!() };
            Python::attach(|py| match r {
                Ok(o) => Ok(o.clone_ref(py)),
                Err(f) => Err(failure_to_pyerr(py, f, id)),
            })
        })
    }
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.result(py)?.call_method0("__await__")
    }
    /// True once the result is available.
    fn done(&self) -> bool {
        matches!(self.state.try_lock().as_deref(), Ok(HState::Done(_)))
    }
    /// Request cancellation (fire and forget).
    fn cancel(&self) {
        let (s, id) = (self.sched.clone(), self.id);
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let _ = s.cancel(TaskRef::Id(TaskId(id))).await;
        });
    }
    fn __repr__(&self) -> String {
        format!(
            "TaskHandle(id={}, duplicate={})",
            self.id,
            if self.duplicate { "True" } else { "False" }
        )
    }
}

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
