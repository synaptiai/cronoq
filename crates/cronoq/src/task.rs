//! Task builder with wall-clock and relative times.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cronoq_core::{
    Dep, FixedStart, MissPolicy, OverlapPolicy, Priority, RecurringSpec, RetryPolicy, TaskRef,
    TaskSpec,
};

use crate::clock::Clock;

/// A point in time, relative to submission or absolute.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum When {
    In(Duration),
    At(SystemTime),
}

/// A task to submit. Wraps [`TaskSpec`] and resolves times at submission.
#[derive(Debug)]
pub struct Task<P> {
    pub(crate) spec: TaskSpec<P>,
    deadline: Option<When>,
    start: Option<(When, Duration, MissPolicy)>,
}

impl<P> Clone for Task<P> {
    fn clone(&self) -> Self {
        Task {
            spec: self.spec.clone(),
            deadline: self.deadline,
            start: self.start,
        }
    }
}

impl<P> From<TaskSpec<P>> for Task<P> {
    fn from(spec: TaskSpec<P>) -> Self {
        Task {
            spec,
            deadline: None,
            start: None,
        }
    }
}

impl<P> Task<P> {
    pub fn new(payload: P) -> Self {
        TaskSpec::new(payload).into()
    }
    pub fn from_arc(payload: Arc<P>) -> Self {
        TaskSpec::from_arc(payload).into()
    }
    pub fn spec(&self) -> &TaskSpec<P> {
        &self.spec
    }
    pub fn kind(mut self, k: impl Into<Arc<str>>) -> Self {
        self.spec.kind = k.into();
        self
    }
    pub fn pool(mut self, p: impl Into<Arc<str>>) -> Self {
        self.spec.pool = p.into();
        self
    }
    pub fn priority(mut self, p: Priority) -> Self {
        self.spec.priority = p;
        self
    }
    /// Unique key: deduplicates submissions and can be referenced by dependents.
    pub fn key(mut self, k: impl Into<Arc<str>>) -> Self {
        self.spec.key = Some(k.into());
        self
    }
    pub fn deadline_in(mut self, d: Duration) -> Self {
        self.deadline = Some(When::In(d));
        self
    }
    pub fn deadline_at(mut self, t: SystemTime) -> Self {
        self.deadline = Some(When::At(t));
        self
    }
    /// Start at a fixed time (default tolerance 1s, run late if missed).
    pub fn start_at(mut self, t: SystemTime) -> Self {
        self.start = Some((When::At(t), Duration::from_secs(1), MissPolicy::RunLate));
        self
    }
    pub fn start_in(mut self, d: Duration) -> Self {
        self.start = Some((When::In(d), Duration::from_secs(1), MissPolicy::RunLate));
        self
    }
    pub fn start_tolerance(mut self, tol: Duration) -> Self {
        if let Some(s) = &mut self.start {
            s.1 = tol;
        }
        self
    }
    pub fn on_miss(mut self, p: MissPolicy) -> Self {
        if let Some(s) = &mut self.start {
            s.2 = p;
        }
        self
    }
    pub fn duration_hint(mut self, d: Duration) -> Self {
        self.spec.duration_hint = Some(d);
        self
    }
    pub fn timeout(mut self, d: Duration) -> Self {
        self.spec.timeout = Some(d);
        self
    }
    pub fn retry(mut self, r: RetryPolicy) -> Self {
        self.spec.retry = Some(r);
        self
    }
    pub fn no_retry(self) -> Self {
        self.retry(RetryPolicy::none())
    }
    /// Run after `target` succeeds.
    pub fn after(mut self, target: impl Into<TaskRef>) -> Self {
        self.spec.deps.push(Dep::on_success(target));
        self
    }
    pub fn dep(mut self, d: Dep) -> Self {
        self.spec.deps.push(d);
        self
    }
    pub fn resource(mut self, name: impl Into<Arc<str>>, amount: u32) -> Self {
        self.spec.resources.push((name.into(), amount));
        self
    }
    pub fn tag(mut self, k: impl Into<Arc<str>>, v: impl Into<Arc<str>>) -> Self {
        self.spec.tags.push((k.into(), v.into()));
        self
    }

    pub(crate) fn resolve(mut self, clock: &Clock) -> TaskSpec<P> {
        let now = clock.now();
        let at = |w: When| match w {
            When::In(d) => now + d,
            When::At(t) => clock.from_system(t),
        };
        if let Some(d) = self.deadline {
            self.spec.deadline = Some(at(d));
        }
        if let Some((w, tolerance, on_miss)) = self.start {
            self.spec.fixed_start = Some(FixedStart {
                at: at(w),
                tolerance,
                on_miss,
            });
        }
        if self.spec.tags.iter().all(|(k, _)| &**k != "traceparent") {
            if let Some(tp) = crate::telemetry::current_traceparent() {
                self.spec.tags.push(("traceparent".into(), tp.into()));
            }
        }
        self.spec
    }
}

/// A recurring (cron) schedule.
#[derive(Debug)]
pub struct Recurring<P> {
    pub(crate) spec: RecurringSpec<P>,
}

impl<P> Recurring<P> {
    /// `cron` accepts 5 fields, or 6 with leading seconds.
    pub fn new(name: impl Into<Arc<str>>, cron: impl Into<Arc<str>>, template: Task<P>) -> Self {
        Recurring {
            spec: RecurringSpec {
                name: name.into(),
                cron: cron.into(),
                tz: None,
                template: template.spec,
                overlap: OverlapPolicy::Skip,
                tolerance: Duration::from_secs(1),
                on_miss: MissPolicy::RunLate,
            },
        }
    }
    /// IANA time zone, e.g. `Europe/Lisbon` (default UTC).
    pub fn tz(mut self, tz: impl Into<Arc<str>>) -> Self {
        self.spec.tz = Some(tz.into());
        self
    }
    pub fn overlap(mut self, o: OverlapPolicy) -> Self {
        self.spec.overlap = o;
        self
    }
    pub fn tolerance(mut self, d: Duration) -> Self {
        self.spec.tolerance = d;
        self
    }
    pub fn on_miss(mut self, p: MissPolicy) -> Self {
        self.spec.on_miss = p;
        self
    }
}
