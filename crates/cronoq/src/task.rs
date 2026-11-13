//! Task builder with wall-clock and relative times.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cronoq_core::{
    Dep, FixedStart, MissPolicy, OverlapPolicy, Priority, RecurringSpec, RetryPolicy, SubmitError,
    TaskRef, TaskSpec,
};

use crate::clock::Clock;
use crate::error::Error;

/// A point in time, relative to submission or absolute.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum When {
    /// This long after the task is submitted.
    In(Duration),
    /// At this wall-clock time.
    At(SystemTime),
}

/// A task to submit. Wraps [`TaskSpec`] and resolves times at submission.
#[derive(Debug)]
pub struct Task<P> {
    pub(crate) spec: TaskSpec<P>,
    deadline: Option<When>,
    start: Option<When>,
    tolerance: Option<Duration>,
    on_miss: Option<MissPolicy>,
    pub(crate) opts: cronoq_core::SubmitOptions,
}

impl<P> Clone for Task<P> {
    fn clone(&self) -> Self {
        Task {
            spec: self.spec.clone(),
            deadline: self.deadline,
            start: self.start,
            tolerance: self.tolerance,
            on_miss: self.on_miss,
            opts: self.opts,
        }
    }
}

impl<P> From<TaskSpec<P>> for Task<P> {
    fn from(spec: TaskSpec<P>) -> Self {
        Task {
            spec,
            deadline: None,
            start: None,
            tolerance: None,
            on_miss: None,
            opts: cronoq_core::SubmitOptions::default(),
        }
    }
}

/// Default tolerance of a fixed start.
const DEFAULT_TOLERANCE: Duration = Duration::from_secs(1);

impl<P> Task<P> {
    /// A task with default settings: pool and kind `"default"`, priority `Normal`.
    pub fn new(payload: P) -> Self {
        TaskSpec::new(payload).into()
    }
    /// Like [`Task::new`], sharing an existing `Arc` payload.
    pub fn from_arc(payload: Arc<P>) -> Self {
        TaskSpec::from_arc(payload).into()
    }
    /// The underlying engine spec (times given relatively are resolved at submission).
    pub fn spec(&self) -> &TaskSpec<P> {
        &self.spec
    }
    /// Task kind: groups tasks for duration learning, metrics and tracing.
    pub fn kind(mut self, k: impl Into<Arc<str>>) -> Self {
        self.spec.kind = k.into();
        self
    }
    /// Pool to run in (default `"default"`).
    pub fn pool(mut self, p: impl Into<Arc<str>>) -> Self {
        self.spec.pool = p.into();
        self
    }
    /// Priority class (default `Normal`).
    pub fn priority(mut self, p: Priority) -> Self {
        self.spec.priority = p;
        self
    }
    /// Unique key: deduplicates submissions and can be referenced by dependents.
    pub fn key(mut self, k: impl Into<Arc<str>>) -> Self {
        self.spec.key = Some(k.into());
        self
    }
    /// Soft deadline, relative to submission: urgency rises as it approaches.
    pub fn deadline_in(mut self, d: Duration) -> Self {
        self.deadline = Some(When::In(d));
        self
    }
    /// Soft deadline at a wall-clock time.
    pub fn deadline_at(mut self, t: SystemTime) -> Self {
        self.deadline = Some(When::At(t));
        self
    }
    /// Start at a fixed time (default tolerance 1s, run late if missed).
    pub fn start_at(mut self, t: SystemTime) -> Self {
        self.start = Some(When::At(t));
        self
    }
    /// Start this long after submission (default tolerance 1s, run late if missed).
    pub fn start_in(mut self, d: Duration) -> Self {
        self.start = Some(When::In(d));
        self
    }
    /// How late a fixed start may begin and still count as on time (any call order).
    pub fn start_tolerance(mut self, tol: Duration) -> Self {
        self.tolerance = Some(tol);
        self
    }
    /// What to do if the fixed start is missed (any call order).
    pub fn on_miss(mut self, p: MissPolicy) -> Self {
        self.on_miss = Some(p);
        self
    }
    /// Expected run time, used before the engine has learned the kind's durations.
    pub fn duration_hint(mut self, d: Duration) -> Self {
        self.spec.duration_hint = Some(d);
        self
    }
    /// Per-attempt timeout: the attempt is aborted and retried (if allowed) when it elapses.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.spec.timeout = Some(d);
        self
    }
    /// Retry policy (default: the pool's).
    pub fn retry(mut self, r: RetryPolicy) -> Self {
        self.spec.retry = Some(r);
        self
    }
    /// Run at most once.
    pub fn no_retry(self) -> Self {
        self.retry(RetryPolicy::none())
    }
    /// Run after `target` succeeds.
    pub fn after(mut self, target: impl Into<TaskRef>) -> Self {
        self.spec.deps.push(Dep::on_success(target));
        self
    }
    /// Add a dependency with an explicit edge condition.
    pub fn dep(mut self, d: Dep) -> Self {
        self.spec.deps.push(d);
        self
    }
    /// Require `amount` units of a configured resource for each attempt.
    pub fn resource(mut self, name: impl Into<Arc<str>>, amount: u32) -> Self {
        self.spec.resources.push((name.into(), amount));
        self
    }
    /// Attach a key/value label (visible to executors and in traces).
    pub fn tag(mut self, k: impl Into<Arc<str>>, v: impl Into<Arc<str>>) -> Self {
        self.spec.tags.push((k.into(), v.into()));
        self
    }
    /// The tenant this task belongs to, for weighted fair sharing of pools with
    /// [`PoolConfig::fairness`](crate::PoolConfig::fairness).
    pub fn tenant(mut self, tenant: impl Into<Arc<str>>) -> Self {
        self.spec.tenant = Some(tenant.into());
        self
    }
    /// Run only on workers labeled `name = value` (see
    /// [`Scheduler::set_group_capacity`](crate::Scheduler::set_group_capacity)).
    pub fn label(mut self, name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        self.spec.labels.push((name.into(), value.into()));
        self
    }

    /// The newest submission of this task's key wins ("latest wins"). Needs a [`Task::key`].
    ///
    /// If the task holding the key has not started, it is replaced in place: same id,
    /// dependents and place in line, with this task's payload, kind, priority, deadline, tags,
    /// timeout and retry policy. If an attempt of it has started, its payload is frozen, and
    /// this task is stored to run as a new task with the same key once that one succeeds,
    /// fails or is dead-lettered (a newer submission replaces the stored one). If no live task
    /// holds the key, this task is submitted as usual.
    ///
    /// The task must name the same pool, resources, worker labels and dependencies as the one
    /// it replaces, and neither may have a fixed start. Not available in
    /// [`Scheduler::submit_batch`](crate::Scheduler::submit_batch),
    /// [`Scheduler::enqueue_many`](crate::Scheduler::enqueue_many) or schedules.
    /// [`Submitted::outcome`](crate::Submitted::outcome) tells which of these happened.
    pub fn replace_queued(mut self) -> Self {
        self.opts.on_key_conflict = cronoq_core::KeyConflict::ReplaceQueued;
        self
    }
    /// What this submission does when its key is already in use, instead of the scheduler's
    /// default ([`Builder::on_key_conflict`](crate::Builder::on_key_conflict)).
    /// [`KeyConflict::Reject`](cronoq_core::KeyConflict::Reject) reports a conflict when the
    /// task holding the key was submitted with a different [`Task::fingerprint`].
    pub fn on_key_conflict(mut self, mode: cronoq_core::KeyConflict) -> Self {
        self.opts.on_key_conflict = mode;
        self
    }
    /// A fingerprint of what this submission asks for, chosen by the caller: for example a
    /// hash of the payload. It is stored with the task. A later submission of the same key
    /// with [`KeyConflict::Reject`](cronoq_core::KeyConflict::Reject) is answered as a
    /// duplicate when its fingerprint is the same, and refused with
    /// [`SubmitError::KeyConflict`](cronoq_core::SubmitError::KeyConflict) when it differs,
    /// instead of being dropped silently. A task or submission without one matches anything.
    pub fn fingerprint(mut self, fingerprint: u128) -> Self {
        self.spec.fingerprint = Some(fingerprint);
        self
    }
    /// With [`Task::replace_queued`]: take this submission's place in line, and start its
    /// waiting time now, instead of keeping the place of the task it replaces.
    pub fn renew_position(mut self) -> Self {
        self.opts.renew_position = true;
        self
    }

    /// Fields that only make sense for a single task: absolute deadlines, fixed starts and
    /// dependencies. (A relative deadline and a key work per occurrence.)
    fn one_shot_fields(&self) -> Option<&'static str> {
        if self.opts != cronoq_core::SubmitOptions::default() {
            Some("on_key_conflict (occurrences are separate tasks with their own keys)")
        } else if matches!(self.deadline, Some(When::At(_))) || self.spec.deadline.is_some() {
            Some("an absolute deadline (use deadline_in, relative to each occurrence)")
        } else if self.start.is_some() || self.spec.fixed_start.is_some() {
            Some("a fixed start")
        } else if !self.spec.deps.is_empty() {
            Some("a dependency (use OverlapPolicy::Queue to chain occurrences)")
        } else {
            None
        }
    }

    pub(crate) fn resolve(mut self, clock: &Clock) -> Result<TaskSpec<P>, Error> {
        validate(&self.spec)?;
        let now = clock.now();
        let at = |w: When| match w {
            When::In(d) => now + d,
            When::At(t) => clock.from_system(t),
        };
        if let Some(d) = self.deadline {
            self.spec.deadline = Some(at(d));
        }
        if let Some(w) = self.start {
            self.spec.fixed_start = Some(FixedStart {
                at: at(w),
                tolerance: self.tolerance.unwrap_or(DEFAULT_TOLERANCE),
                on_miss: self.on_miss.unwrap_or_default(),
            });
        } else if let Some(f) = &mut self.spec.fixed_start {
            if let Some(t) = self.tolerance {
                f.tolerance = t;
            }
            if let Some(p) = self.on_miss {
                f.on_miss = p;
            }
        }
        if self.spec.tags.iter().all(|(k, _)| &**k != "traceparent")
            && let Some(tp) = crate::telemetry::current_traceparent()
        {
            self.spec.tags.push(("traceparent".into(), tp.into()));
        }
        Ok(self.spec)
    }
}

fn invalid(what: &'static str) -> Error {
    Error::Submit(SubmitError::Invalid(what))
}

/// Changes to a task that has not finished, for
/// [`Scheduler::update`](crate::Scheduler::update). What is not named keeps its value.
///
/// ```
/// use cronoq::{Priority, Update};
/// use std::time::Duration;
///
/// let promote = Update::new().priority(Priority::High);
/// let sooner = Update::new().deadline_in(Duration::from_secs(30));
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Update {
    priority: Option<Priority>,
    deadline: Option<Option<When>>,
    tags: Option<Vec<(Arc<str>, Arc<str>)>>,
}

impl Update {
    /// An update that changes nothing yet.
    pub fn new() -> Self {
        Update::default()
    }
    /// Move the task to this priority class. It keeps its place by submission order there,
    /// and its waiting time for aging still counts from its submission.
    pub fn priority(mut self, p: Priority) -> Self {
        self.priority = Some(p);
        self
    }
    /// Set the deadline to this long from now.
    pub fn deadline_in(mut self, d: Duration) -> Self {
        self.deadline = Some(Some(When::In(d)));
        self
    }
    /// Set the deadline to this wall-clock time.
    pub fn deadline_at(mut self, t: SystemTime) -> Self {
        self.deadline = Some(Some(When::At(t)));
        self
    }
    /// Remove the deadline.
    pub fn no_deadline(mut self) -> Self {
        self.deadline = Some(None);
        self
    }
    /// Replace the task's tags. Names starting with `cronoq.` are reserved.
    pub fn tags<K: Into<Arc<str>>, V: Into<Arc<str>>>(
        mut self,
        tags: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.tags = Some(
            tags.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// The priority class this update moves the task to, if it names one.
    pub fn new_priority(&self) -> Option<Priority> {
        self.priority
    }
    pub(crate) fn resolve(self, clock: &Clock) -> Result<cronoq_core::TaskPatch, Error> {
        if let Some(tags) = &self.tags
            && tags
                .iter()
                .any(|(k, v)| k.contains('\0') || v.contains('\0'))
        {
            return Err(Error::Update(cronoq_core::UpdateError::Invalid(
                "tags must not contain NUL characters",
            )));
        }
        let now = clock.now();
        Ok(cronoq_core::TaskPatch {
            priority: self.priority,
            deadline: self.deadline.map(|d| {
                d.map(|w| match w {
                    When::In(d) => now + d,
                    When::At(t) => clock.from_system(t),
                })
            }),
            tags: self.tags,
        })
    }
}

/// Reject values that cannot be stored or would misbehave later.
pub(crate) fn validate<P>(s: &TaskSpec<P>) -> Result<(), Error> {
    let nul = |v: &str| v.contains('\0');
    if nul(&s.kind)
        || nul(&s.pool)
        || s.key.as_deref().is_some_and(nul)
        || s.tenant.as_deref().is_some_and(nul)
    {
        return Err(invalid(
            "kind, pool, key and tenant must not contain NUL characters",
        ));
    }
    if s.tags.iter().any(|(k, v)| nul(k) || nul(v)) {
        return Err(invalid("tags must not contain NUL characters"));
    }
    if s.labels.iter().any(|(k, v)| nul(k) || nul(v)) {
        return Err(invalid("labels must not contain NUL characters"));
    }
    if s.resources.iter().any(|(n, _)| nul(n)) {
        return Err(invalid("resource names must not contain NUL characters"));
    }
    if s.deps
        .iter()
        .any(|d| matches!(&d.target, TaskRef::Key(k) if nul(k)))
    {
        return Err(invalid("dependency keys must not contain NUL characters"));
    }
    if let Some(r) = &s.retry
        && (!r.multiplier.is_finite() || r.multiplier < 0.0)
    {
        return Err(invalid("retry multiplier must be finite and non-negative"));
    }
    Ok(())
}

/// A recurring (cron) schedule.
#[derive(Debug)]
pub struct Recurring<P> {
    pub(crate) spec: RecurringSpec<P>,
    /// Reason the template cannot be used, reported by `add_recurring`.
    pub(crate) invalid: Option<String>,
}

impl<P> Recurring<P> {
    /// `cron` accepts 5 fields, or 6 with leading seconds.
    ///
    /// The template supplies payload, pool, kind, priority, resources, retry, timeout and tags
    /// for every occurrence, and optionally:
    ///
    /// - a relative deadline ([`Task::deadline_in`]): each occurrence's deadline is that long
    ///   after its start;
    /// - a start tolerance and miss policy ([`Task::start_tolerance`], [`Task::on_miss`]), as
    ///   defaults for [`Recurring::tolerance`] and [`Recurring::on_miss`];
    /// - a key: a prefix; each occurrence gets `"<key>@<start in UTC, RFC 3339>"`, e.g.
    ///   `nightly@2026-10-22T03:00:00Z`, so occurrences can be looked up and are not created
    ///   twice.
    ///
    /// Absolute deadlines, fixed starts and dependencies describe a single task and are
    /// rejected by [`Scheduler::add_recurring`](crate::Scheduler::add_recurring); to run
    /// occurrences one after another, use [`OverlapPolicy::Queue`]. Each occurrence starts at
    /// its cron time with [`Recurring::tolerance`] and [`Recurring::on_miss`].
    pub fn new(name: impl Into<Arc<str>>, cron: impl Into<Arc<str>>, template: Task<P>) -> Self {
        let invalid = template
            .one_shot_fields()
            .map(|f| format!("recurring templates cannot set {f}"))
            .or_else(|| validate(&template.spec).err().map(|e| e.to_string()));
        let name = name.into();
        let invalid = invalid.or_else(|| {
            name.contains('\0')
                .then(|| "recurring names must not contain NUL characters".into())
        });
        Recurring {
            spec: RecurringSpec {
                name,
                cron: cron.into(),
                tz: None,
                deadline_after: match template.deadline {
                    Some(When::In(d)) => Some(d),
                    _ => None,
                },
                template: template.spec,
                overlap: OverlapPolicy::Skip,
                tolerance: template.tolerance.unwrap_or(DEFAULT_TOLERANCE),
                on_miss: template.on_miss.unwrap_or_default(),
            },
            invalid,
        }
    }
    /// IANA time zone, e.g. `Europe/Lisbon` (default UTC).
    pub fn tz(mut self, tz: impl Into<Arc<str>>) -> Self {
        self.spec.tz = Some(tz.into());
        self
    }
    /// What happens while the previous occurrence is still live: skip (default), allow overlap,
    /// or queue behind it ([`OverlapPolicy::Queue`], [`OverlapPolicy::QueueOnSuccess`]).
    pub fn overlap(mut self, o: OverlapPolicy) -> Self {
        self.spec.overlap = o;
        self
    }
    /// How late an occurrence may start and still count as on time.
    pub fn tolerance(mut self, d: Duration) -> Self {
        self.spec.tolerance = d;
        self
    }
    /// What to do with an occurrence that missed its start.
    pub fn on_miss(mut self, p: MissPolicy) -> Self {
        self.spec.on_miss = p;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_options_are_order_independent() {
        let clock = Clock::start();
        let a = Task::new(1u64)
            .start_tolerance(Duration::from_secs(60))
            .on_miss(MissPolicy::Skip)
            .start_in(Duration::from_secs(1));
        let b = Task::new(1u64)
            .start_in(Duration::from_secs(1))
            .start_tolerance(Duration::from_secs(60))
            .on_miss(MissPolicy::Skip);
        let (fa, fb) = (
            a.resolve(&clock).unwrap().fixed_start.unwrap(),
            b.resolve(&clock).unwrap().fixed_start.unwrap(),
        );
        assert_eq!((fa.tolerance, fa.on_miss), (fb.tolerance, fb.on_miss));
        assert_eq!(fa.tolerance, Duration::from_secs(60));
    }

    #[test]
    fn rejects_unstorable_values() {
        let clock = Clock::start();
        assert!(Task::new(1).key("a\0b").resolve(&clock).is_err());
        assert!(Task::new(1).tag("k", "v\0").resolve(&clock).is_err());
        let r = RetryPolicy {
            multiplier: f64::INFINITY,
            ..Default::default()
        };
        assert!(Task::new(1).retry(r).resolve(&clock).is_err());
        assert!(Task::new(1).kind("ok").resolve(&clock).is_ok());
    }

    #[test]
    fn recurring_templates_take_relative_deadlines_and_key_prefixes() {
        let r = Recurring::new(
            "r",
            "* * * * *",
            Task::new(1).deadline_in(Duration::from_secs(5)).key("k"),
        );
        assert!(r.invalid.is_none(), "{:?}", r.invalid);
        assert_eq!(r.spec.deadline_after, Some(Duration::from_secs(5)));
        assert_eq!(r.spec.template.key.as_deref(), Some("k"));
        let at = SystemTime::now() + Duration::from_secs(60);
        for (t, what) in [
            (Task::new(1).deadline_at(at), "absolute deadline"),
            (Task::new(1).start_in(Duration::from_secs(1)), "fixed start"),
            (Task::new(1).after("other"), "dependency"),
        ] {
            let r = Recurring::new("r", "* * * * *", t);
            assert!(r.invalid.unwrap().contains(what), "{what}");
        }
    }
}
