//! Configuration and value types exposed to Python.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cronoq::{
    Dep as CoreDep, EdgeCond, MissPolicy, OverflowPolicy, OverlapPolicy, PoolConfig,
    Priority as CorePriority, ResourceConfig, RetryPolicy as CoreRetry, TaskId, TaskRef,
    UnknownDependency,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyString};

pub(crate) type PyObj = Py<PyAny>;

/// A finite, non-negative number of seconds that fits in a `Duration`.
pub(crate) fn secs(v: f64, what: &str) -> PyResult<Duration> {
    if !v.is_finite() || v < 0.0 {
        return Err(PyValueError::new_err(format!(
            "{what} must be a finite, non-negative number of seconds (got {v})"
        )));
    }
    Duration::try_from_secs_f64(v)
        .map_err(|_| PyValueError::new_err(format!("{what} is too large (got {v} seconds)")))
}

/// Like [`secs`], but strictly positive.
fn positive_secs(v: f64, what: &str) -> PyResult<Duration> {
    let d = secs(v, what)?;
    if d.is_zero() {
        return Err(PyValueError::new_err(format!("{what} must be > 0")));
    }
    Ok(d)
}

/// `v` in `(lo, hi]` (or `[lo, hi]` with `lo_inclusive`).
fn in_range(v: f64, lo: f64, hi: f64, lo_inclusive: bool, what: &str) -> PyResult<f64> {
    let above = if lo_inclusive { v >= lo } else { v > lo };
    if v.is_finite() && above && v <= hi {
        return Ok(v);
    }
    let open = if lo_inclusive { "[" } else { "(" };
    Err(PyValueError::new_err(format!(
        "{what} must be in {open}{lo}, {hi}] (got {v})"
    )))
}

/// Task priority class (lower value = more important).
#[pyclass(
    eq,
    eq_int,
    ord,
    hash,
    frozen,
    from_py_object,
    module = "cronoq._cronoq"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    #[pyo3(name = "CRITICAL")]
    Critical = 0,
    #[pyo3(name = "HIGH")]
    High = 1,
    #[pyo3(name = "NORMAL")]
    Normal = 2,
    #[pyo3(name = "LOW")]
    Low = 3,
}

#[pymethods]
impl Priority {
    /// The member name, e.g. `"HIGH"`.
    #[getter]
    fn name(&self) -> &'static str {
        match self {
            Priority::Critical => "CRITICAL",
            Priority::High => "HIGH",
            Priority::Normal => "NORMAL",
            Priority::Low => "LOW",
        }
    }
    /// The class index: 0 (critical) to 3 (low).
    #[getter]
    fn value(&self) -> u8 {
        *self as u8
    }
}

impl From<Priority> for CorePriority {
    fn from(p: Priority) -> Self {
        CorePriority::from_index(p as usize).expect("valid priority")
    }
}

/// Accepts `Priority`, an int 0..=3 (e.g. an `IntEnum`), or a name like `"high"`.
pub(crate) fn extract_priority(obj: &Bound<'_, PyAny>) -> PyResult<CorePriority> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "priority must be a Priority, int or str, not bool",
        ));
    }
    if let Ok(p) = obj.extract::<Priority>() {
        return Ok(p.into());
    }
    if let Ok(i) = obj.extract::<usize>() {
        return CorePriority::from_index(i)
            .ok_or_else(|| PyValueError::new_err("priority must be 0..=3"));
    }
    if let Ok(s) = obj.extract::<String>() {
        return CorePriority::ALL
            .into_iter()
            .find(|p| p.name().eq_ignore_ascii_case(&s))
            .ok_or_else(|| PyValueError::new_err(format!("unknown priority '{s}'")));
    }
    Err(PyTypeError::new_err(
        "priority must be a Priority, int or str",
    ))
}

/// Retry behaviour for retryable failures.
#[pyclass(frozen, get_all, from_py_object, module = "cronoq._cronoq")]
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: f64,
    pub multiplier: f64,
    pub max_backoff: f64,
    pub jitter: bool,
}

#[pymethods]
impl RetryPolicy {
    /// `multiplier` must be finite and >= 1; backoffs are seconds.
    #[new]
    #[pyo3(signature = (max_attempts=3, initial_backoff=1.0, multiplier=2.0, max_backoff=60.0, jitter=true))]
    fn new(
        max_attempts: u32,
        initial_backoff: f64,
        multiplier: f64,
        max_backoff: f64,
        jitter: bool,
    ) -> PyResult<Self> {
        if max_attempts == 0 {
            return Err(PyValueError::new_err("max_attempts must be >= 1"));
        }
        secs(initial_backoff, "initial_backoff")?;
        secs(max_backoff, "max_backoff")?;
        if !multiplier.is_finite() || multiplier < 1.0 {
            return Err(PyValueError::new_err(format!(
                "multiplier must be a finite number >= 1 (got {multiplier})"
            )));
        }
        Ok(RetryPolicy {
            max_attempts,
            initial_backoff,
            multiplier,
            max_backoff,
            jitter,
        })
    }
    /// A single attempt, never retried.
    #[staticmethod]
    fn none() -> Self {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff: 1.0,
            multiplier: 2.0,
            max_backoff: 60.0,
            jitter: true,
        }
    }
    fn __repr__(&self) -> String {
        format!(
            "RetryPolicy(max_attempts={}, initial_backoff={}, multiplier={}, max_backoff={}, jitter={})",
            self.max_attempts,
            self.initial_backoff,
            self.multiplier,
            self.max_backoff,
            if self.jitter { "True" } else { "False" }
        )
    }
}

impl RetryPolicy {
    pub(crate) fn to_core(&self) -> CoreRetry {
        // Values were validated in `new`.
        CoreRetry {
            max_attempts: self.max_attempts,
            initial_backoff: Duration::from_secs_f64(self.initial_backoff),
            multiplier: self.multiplier,
            max_backoff: Duration::from_secs_f64(self.max_backoff),
            jitter: self.jitter,
        }
    }
}

/// Queue-pressure autoscaling parameters (seconds as floats).
#[pyclass(frozen, get_all, from_py_object, module = "cronoq._cronoq")]
#[derive(Clone, Debug)]
pub struct AutoscaleConfig {
    pub interval: f64,
    pub target_utilization: f64,
    pub drain_time: f64,
    /// Maximum acceptable queue wait in seconds, by lower-case priority name.
    pub slo: BTreeMap<String, f64>,
    pub reservation_horizon: f64,
    pub stabilization_window: f64,
    pub cooldown: f64,
    pub max_step_up_abs: u32,
    pub max_step_down: u32,
    pub cost_per_worker_second: f64,
}

#[pymethods]
impl AutoscaleConfig {
    /// `slo` maps a priority (`Priority`, its name or int) to the maximum acceptable queue wait
    /// in seconds.
    #[new]
    #[pyo3(signature = (
        interval=1.0, target_utilization=0.75, drain_time=30.0, slo=None, reservation_horizon=60.0,
        stabilization_window=60.0, cooldown=30.0, max_step_up_abs=4, max_step_down=1,
        cost_per_worker_second=0.0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        interval: f64,
        target_utilization: f64,
        drain_time: f64,
        slo: Option<&Bound<'_, PyDict>>,
        reservation_horizon: f64,
        stabilization_window: f64,
        cooldown: f64,
        max_step_up_abs: u32,
        max_step_down: u32,
        cost_per_worker_second: f64,
    ) -> PyResult<Self> {
        positive_secs(interval, "interval")?;
        for (v, n) in [
            (drain_time, "drain_time"),
            (reservation_horizon, "reservation_horizon"),
            (stabilization_window, "stabilization_window"),
            (cooldown, "cooldown"),
        ] {
            secs(v, n)?;
        }
        in_range(target_utilization, 0.0, 1.0, false, "target_utilization")?;
        if !cost_per_worker_second.is_finite() || cost_per_worker_second < 0.0 {
            return Err(PyValueError::new_err(
                "cost_per_worker_second must be a finite, non-negative number",
            ));
        }
        let mut slo_map = BTreeMap::new();
        if let Some(d) = slo {
            for (k, s) in d.iter() {
                let p = extract_priority(&k)?;
                let v: f64 = s.extract()?;
                secs(v, "slo")?;
                slo_map.insert(p.name().to_string(), v);
            }
        }
        Ok(AutoscaleConfig {
            interval,
            target_utilization,
            drain_time,
            slo: slo_map,
            reservation_horizon,
            stabilization_window,
            cooldown,
            max_step_up_abs,
            max_step_down,
            cost_per_worker_second,
        })
    }
}

impl AutoscaleConfig {
    pub(crate) fn to_core(&self) -> cronoq::AutoscaleConfig {
        // Values were validated in `new`.
        let mut slo = [None; 4];
        for (name, s) in &self.slo {
            if let Some(p) = CorePriority::ALL.into_iter().find(|p| p.name() == name) {
                slo[p.index()] = Some(Duration::from_secs_f64(*s));
            }
        }
        cronoq::AutoscaleConfig {
            interval: Duration::from_secs_f64(self.interval),
            target_utilization: self.target_utilization,
            drain_time: Duration::from_secs_f64(self.drain_time),
            slo,
            reservation_horizon: Duration::from_secs_f64(self.reservation_horizon),
            stabilization_window: Duration::from_secs_f64(self.stabilization_window),
            cooldown: Duration::from_secs_f64(self.cooldown),
            max_step_up_abs: self.max_step_up_abs,
            max_step_down: self.max_step_down,
            cost_per_worker_second: self.cost_per_worker_second,
            ..Default::default()
        }
    }
}

/// A dependency edge: run after `target` ends with the given condition.
#[pyclass(frozen, from_py_object, module = "cronoq._cronoq")]
#[derive(Clone, Debug)]
pub struct Dep {
    pub(crate) inner: CoreDep,
}

#[pymethods]
impl Dep {
    /// `target` is a task key (str) or id (int); `on` is `"success"`, `"failure"` or
    /// `"complete"` (the `on_`-prefixed spellings used by `cronoq.client` are accepted too).
    #[new]
    #[pyo3(signature = (target, on="success"))]
    fn new(target: &Bound<'_, PyAny>, on: &str) -> PyResult<Self> {
        let cond = match on {
            "success" | "on_success" => EdgeCond::OnSuccess,
            "failure" | "on_failure" => EdgeCond::OnFailure,
            "complete" | "on_complete" => EdgeCond::OnComplete,
            _ => {
                return Err(PyValueError::new_err(
                    "on must be 'success', 'failure' or 'complete'",
                ));
            }
        };
        Ok(Dep {
            inner: CoreDep {
                target: extract_ref(target)?,
                cond,
            },
        })
    }
    fn __repr__(&self) -> String {
        format!("Dep({:?}, on={:?})", self.inner.target, self.inner.cond)
    }
}

/// A task key (str) or id (int).
pub(crate) fn extract_ref(obj: &Bound<'_, PyAny>) -> PyResult<TaskRef> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "task reference must be a key (str) or id (int), not bool",
        ));
    }
    if let Ok(i) = obj.extract::<u64>() {
        return Ok(TaskRef::Id(TaskId(i)));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(TaskRef::Key(s.into()));
    }
    Err(PyTypeError::new_err(
        "task reference must be a key (str) or id (int)",
    ))
}

/// A shared resource (concurrency limit or rate limit).
#[pyclass(frozen, from_py_object, module = "cronoq._cronoq")]
#[derive(Clone, Debug)]
pub struct Resource {
    pub(crate) inner: ResourceConfig,
}

#[pymethods]
impl Resource {
    /// At most `permits` (>= 1) units held at once (released when an attempt ends).
    #[staticmethod]
    fn semaphore(name: &str, permits: u32) -> PyResult<Self> {
        if permits == 0 {
            return Err(PyValueError::new_err("permits must be >= 1"));
        }
        Ok(Resource {
            inner: ResourceConfig::semaphore(name, permits),
        })
    }
    /// Units refill at `rate` (> 0) per second up to `burst` (>= 1), e.g. API requests per
    /// second.
    #[staticmethod]
    fn token_bucket(name: &str, rate: f64, burst: u32) -> PyResult<Self> {
        if !rate.is_finite() || rate <= 0.0 {
            return Err(PyValueError::new_err(format!(
                "rate must be a finite number > 0 (got {rate})"
            )));
        }
        if burst == 0 {
            return Err(PyValueError::new_err("burst must be >= 1"));
        }
        Ok(Resource {
            inner: ResourceConfig::token_bucket(name, rate, burst),
        })
    }
    fn __repr__(&self) -> String {
        format!("Resource({:?})", self.inner)
    }
}

/// A worker pool and the Python callable that executes its tasks.
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct Pool {
    pub(crate) cfg: PoolConfig,
    /// Always a coroutine function (sync callables are wrapped at construction).
    pub(crate) executor: PyObj,
}

#[pymethods]
impl Pool {
    /// `executor` is `async def run(ctx, payload)` or a plain function (run in a thread).
    /// `autoscale=False` keeps exactly `initial` workers.
    #[new]
    #[pyo3(signature = (
        name, executor, *, min=1, max=10, initial=None, autoscale=None, lease_ttl=None,
        max_pending=None, overflow="wait", retry=None, unknown_dependency="reject",
        await_timeout=None, default_duration=1.0, reservable_fraction=0.5
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        name: &str,
        executor: &Bound<'_, PyAny>,
        min: u32,
        max: u32,
        initial: Option<u32>,
        autoscale: Option<&Bound<'_, PyAny>>,
        lease_ttl: Option<f64>,
        max_pending: Option<usize>,
        overflow: &str,
        retry: Option<RetryPolicy>,
        unknown_dependency: &str,
        await_timeout: Option<f64>,
        default_duration: f64,
        reservable_fraction: f64,
    ) -> PyResult<Self> {
        if !executor.is_callable() {
            return Err(PyTypeError::new_err("executor must be callable"));
        }
        if max == 0 {
            return Err(PyValueError::new_err("max must be >= 1"));
        }
        if min > max {
            return Err(PyValueError::new_err("min must be <= max"));
        }
        in_range(reservable_fraction, 0.0, 1.0, true, "reservable_fraction")?;
        let mut cfg = PoolConfig::new(name);
        cfg.min_workers = min;
        cfg.max_workers = max;
        cfg.initial_workers = initial.unwrap_or(min).clamp(min, max);
        cfg.autoscale = match autoscale {
            None => Some(cronoq::AutoscaleConfig::default()),
            Some(a) => {
                if let Ok(b) = a.extract::<bool>() {
                    b.then(cronoq::AutoscaleConfig::default)
                } else {
                    Some(a.extract::<AutoscaleConfig>()?.to_core())
                }
            }
        };
        if cfg.autoscale.is_none() && initial.is_none() {
            cfg.initial_workers = max;
            cfg.min_workers = max;
        }
        cfg.lease_ttl = lease_ttl
            .map(|s| positive_secs(s, "lease_ttl"))
            .transpose()?;
        cfg.bounds.max_pending = max_pending;
        cfg.overflow = match overflow {
            "wait" => OverflowPolicy::Wait,
            "reject" => OverflowPolicy::Reject,
            "shed_lowest" => OverflowPolicy::ShedLowest,
            "shed_oldest" => OverflowPolicy::ShedOldest,
            _ => {
                return Err(PyValueError::new_err(
                    "overflow must be wait, reject, shed_lowest or shed_oldest",
                ));
            }
        };
        if let Some(r) = retry {
            cfg.retry = r.to_core();
        }
        cfg.unknown_dependency = match unknown_dependency {
            "reject" => UnknownDependency::Reject,
            "await" => UnknownDependency::Await {
                timeout: await_timeout
                    .map(|s| secs(s, "await_timeout"))
                    .transpose()?,
            },
            _ => {
                return Err(PyValueError::new_err(
                    "unknown_dependency must be 'reject' or 'await'",
                ));
            }
        };
        cfg.default_duration = secs(default_duration, "default_duration")?;
        cfg.reservable_fraction = reservable_fraction;
        let is_async: bool = py
            .import("inspect")?
            .call_method1("iscoroutinefunction", (executor,))?
            .extract()?;
        let executor = if is_async {
            executor.clone().unbind()
        } else {
            py.import("cronoq._runtime")?
                .call_method1("wrap_sync", (executor,))?
                .unbind()
        };
        Ok(Pool { cfg, executor })
    }

    #[getter]
    fn name(&self) -> &str {
        &self.cfg.name
    }

    fn __repr__(&self) -> String {
        format!(
            "Pool({:?}, min={}, max={}, initial={}, autoscale={})",
            &*self.cfg.name,
            self.cfg.min_workers,
            self.cfg.max_workers,
            self.cfg.initial_workers,
            if self.cfg.autoscale.is_some() {
                "True"
            } else {
                "False"
            }
        )
    }
}

fn to_system_time(obj: &Bound<'_, PyAny>) -> PyResult<SystemTime> {
    let ts: f64 = if obj.hasattr("timestamp")? {
        obj.call_method0("timestamp")?.extract()?
    } else {
        obj.extract()?
    };
    if !ts.is_finite() || ts < 0.0 {
        return Err(PyValueError::new_err(
            "time must be a datetime or a non-negative epoch timestamp",
        ));
    }
    Duration::try_from_secs_f64(ts)
        .ok()
        .and_then(|d| UNIX_EPOCH.checked_add(d))
        .ok_or_else(|| PyValueError::new_err("time is too far in the future"))
}

/// Parsed task options (pure Rust, so they can cross threads freely).
#[derive(Clone, Debug, Default)]
pub(crate) struct Opts {
    priority: Option<CorePriority>,
    pool: Option<Arc<str>>,
    kind: Option<Arc<str>>,
    key: Option<Arc<str>>,
    deadline_in: Option<Duration>,
    deadline_at: Option<SystemTime>,
    start_in: Option<Duration>,
    start_at: Option<SystemTime>,
    tolerance: Option<Duration>,
    on_miss: Option<MissPolicy>,
    duration_hint: Option<Duration>,
    timeout: Option<Duration>,
    retry: Option<CoreRetry>,
    deps: Vec<CoreDep>,
    resources: Vec<(Arc<str>, u32)>,
    tags: Vec<(Arc<str>, Arc<str>)>,
    labels: Vec<(Arc<str>, Arc<str>)>,
}

pub(crate) fn parse_miss(s: &str) -> PyResult<MissPolicy> {
    match s {
        "run_late" => Ok(MissPolicy::RunLate),
        "skip" => Ok(MissPolicy::Skip),
        "fail" => Ok(MissPolicy::Fail),
        _ => Err(PyValueError::new_err(
            "on_miss must be 'run_late', 'skip' or 'fail'",
        )),
    }
}

pub(crate) fn parse_overlap(s: &str) -> PyResult<OverlapPolicy> {
    match s {
        "skip" => Ok(OverlapPolicy::Skip),
        "allow" => Ok(OverlapPolicy::Allow),
        "queue" => Ok(OverlapPolicy::Queue),
        "queue_on_success" => Ok(OverlapPolicy::QueueOnSuccess),
        _ => Err(PyValueError::new_err(
            "overlap must be 'skip', 'allow', 'queue' or 'queue_on_success'",
        )),
    }
}

impl Opts {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse(
        priority: Option<&Bound<'_, PyAny>>,
        pool: Option<&str>,
        kind: Option<&str>,
        key: Option<&str>,
        deadline_in: Option<f64>,
        deadline_at: Option<&Bound<'_, PyAny>>,
        start_in: Option<f64>,
        start_at: Option<&Bound<'_, PyAny>>,
        tolerance: Option<f64>,
        on_miss: Option<&str>,
        duration_hint: Option<f64>,
        timeout: Option<f64>,
        retry: Option<RetryPolicy>,
        after: Option<&Bound<'_, PyAny>>,
        deps: Option<Vec<Dep>>,
        resources: Option<&Bound<'_, PyDict>>,
        tags: Option<&Bound<'_, PyDict>>,
        labels: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Opts> {
        let mut o = Opts {
            priority: priority.map(extract_priority).transpose()?,
            pool: pool.map(Arc::from),
            kind: kind.map(Arc::from),
            key: key.map(Arc::from),
            deadline_in: deadline_in.map(|s| secs(s, "deadline_in")).transpose()?,
            deadline_at: deadline_at.map(to_system_time).transpose()?,
            start_in: start_in.map(|s| secs(s, "start_in")).transpose()?,
            start_at: start_at.map(to_system_time).transpose()?,
            tolerance: tolerance.map(|s| secs(s, "tolerance")).transpose()?,
            on_miss: on_miss.map(parse_miss).transpose()?,
            duration_hint: duration_hint
                .map(|s| secs(s, "duration_hint"))
                .transpose()?,
            timeout: timeout.map(|s| secs(s, "timeout")).transpose()?,
            retry: retry.map(|r| r.to_core()),
            deps: deps
                .unwrap_or_default()
                .into_iter()
                .map(|d| d.inner)
                .collect(),
            ..Default::default()
        };
        if let Some(a) = after {
            if a.is_instance_of::<PyString>() || a.extract::<u64>().is_ok() {
                o.deps.push(CoreDep::on_success(extract_ref(a)?));
            } else {
                for item in a.try_iter()? {
                    o.deps.push(CoreDep::on_success(extract_ref(&item?)?));
                }
            }
        }
        if let Some(r) = resources {
            for (k, v) in r.iter() {
                o.resources
                    .push((k.extract::<String>()?.into(), v.extract()?));
            }
        }
        if let Some(t) = tags {
            for (k, v) in t.iter() {
                o.tags
                    .push((k.str()?.to_string().into(), v.str()?.to_string().into()));
            }
        }
        if let Some(l) = labels {
            for (k, v) in l.iter() {
                o.labels
                    .push((k.extract::<String>()?.into(), v.extract::<String>()?.into()));
            }
        }
        if o.start_in.is_some() && o.start_at.is_some() {
            return Err(PyValueError::new_err("use either start_in or start_at"));
        }
        if o.deadline_in.is_some() && o.deadline_at.is_some() {
            return Err(PyValueError::new_err(
                "use either deadline_in or deadline_at",
            ));
        }
        Ok(o)
    }

    /// Build the runtime task; `default_pool` is used when no pool was given.
    pub(crate) fn to_task(&self, payload: PyObj, default_pool: &Arc<str>) -> cronoq::Task<PyObj> {
        let mut t = cronoq::Task::new(payload)
            .pool(self.pool.clone().unwrap_or_else(|| default_pool.clone()));
        if let Some(p) = self.priority {
            t = t.priority(p);
        }
        if let Some(k) = &self.kind {
            t = t.kind(k.clone());
        }
        if let Some(k) = &self.key {
            t = t.key(k.clone());
        }
        if let Some(d) = self.deadline_in {
            t = t.deadline_in(d);
        }
        if let Some(d) = self.deadline_at {
            t = t.deadline_at(d);
        }
        if let Some(d) = self.start_in {
            t = t.start_in(d);
        }
        if let Some(d) = self.start_at {
            t = t.start_at(d);
        }
        if let Some(d) = self.tolerance {
            t = t.start_tolerance(d);
        }
        if let Some(m) = self.on_miss {
            t = t.on_miss(m);
        }
        if let Some(d) = self.duration_hint {
            t = t.duration_hint(d);
        }
        if let Some(d) = self.timeout {
            t = t.timeout(d);
        }
        if let Some(r) = self.retry {
            t = t.retry(r);
        }
        for d in &self.deps {
            t = t.dep(d.clone());
        }
        for (n, a) in &self.resources {
            t = t.resource(n.clone(), *a);
        }
        for (k, v) in &self.tags {
            t = t.tag(k.clone(), v.clone());
        }
        for (k, v) in &self.labels {
            t = t.label(k.clone(), v.clone());
        }
        t
    }
}

/// A task description for `Scheduler.submit_batch` (same options as `submit`).
#[pyclass(frozen, module = "cronoq._cronoq")]
pub struct Task {
    pub(crate) payload: PyObj,
    pub(crate) opts: Opts,
}

#[pymethods]
impl Task {
    #[new]
    #[pyo3(signature = (
        payload, *, priority=None, pool=None, kind=None, key=None, deadline_in=None,
        deadline_at=None, start_in=None, start_at=None, tolerance=None, on_miss=None,
        duration_hint=None, timeout=None, retry=None, after=None, deps=None, resources=None,
        tags=None, labels=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        payload: PyObj,
        priority: Option<&Bound<'_, PyAny>>,
        pool: Option<&str>,
        kind: Option<&str>,
        key: Option<&str>,
        deadline_in: Option<f64>,
        deadline_at: Option<&Bound<'_, PyAny>>,
        start_in: Option<f64>,
        start_at: Option<&Bound<'_, PyAny>>,
        tolerance: Option<f64>,
        on_miss: Option<&str>,
        duration_hint: Option<f64>,
        timeout: Option<f64>,
        retry: Option<RetryPolicy>,
        after: Option<&Bound<'_, PyAny>>,
        deps: Option<Vec<Dep>>,
        resources: Option<&Bound<'_, PyDict>>,
        tags: Option<&Bound<'_, PyDict>>,
        labels: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
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
            labels,
        )?;
        Ok(Task { payload, opts })
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> PyObj {
        self.payload.clone_ref(py)
    }

    #[getter]
    fn key(&self) -> Option<&str> {
        self.opts.key.as_deref()
    }

    fn __repr__(&self) -> String {
        format!(
            "Task(key={:?}, priority={:?})",
            self.opts.key, self.opts.priority
        )
    }
}
