//! Task specification and identifiers.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::time::Time;

/// Engine-assigned task identifier (monotonic; the high 16 bits carry the leader epoch).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub u64);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies one dispatch (attempt) of a task.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// Caller-chosen correlation id for submissions.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Ticket(pub u64);

/// Index of a pool in the engine configuration.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolId(pub u16);

/// Index of a resource in the engine configuration.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceId(pub u16);

/// Priority class. Lower discriminant = more important.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Priority {
    Critical = 0,
    High = 1,
    #[default]
    Normal = 2,
    Low = 3,
}

impl Priority {
    pub const ALL: [Priority; 4] = [
        Priority::Critical,
        Priority::High,
        Priority::Normal,
        Priority::Low,
    ];
    pub const COUNT: usize = 4;

    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }
    pub fn from_index(i: usize) -> Option<Priority> {
        Self::ALL.get(i).copied()
    }
    pub fn name(self) -> &'static str {
        match self {
            Priority::Critical => "critical",
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
        }
    }
}

/// Reference to another task, used for dependencies and cancellation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TaskRef {
    Id(TaskId),
    Key(Arc<str>),
}

impl From<TaskId> for TaskRef {
    fn from(id: TaskId) -> Self {
        TaskRef::Id(id)
    }
}
impl From<&str> for TaskRef {
    fn from(k: &str) -> Self {
        TaskRef::Key(k.into())
    }
}
impl From<String> for TaskRef {
    fn from(k: String) -> Self {
        TaskRef::Key(k.into())
    }
}

/// When a dependency edge is satisfied by its parent's terminal status.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum EdgeCond {
    /// Parent succeeded.
    #[default]
    OnSuccess,
    /// Parent reached any terminal status.
    OnComplete,
    /// Parent failed or was dead-lettered.
    OnFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Dep {
    pub target: TaskRef,
    pub cond: EdgeCond,
}

impl Dep {
    pub fn on_success(target: impl Into<TaskRef>) -> Dep {
        Dep {
            target: target.into(),
            cond: EdgeCond::OnSuccess,
        }
    }
    pub fn on_complete(target: impl Into<TaskRef>) -> Dep {
        Dep {
            target: target.into(),
            cond: EdgeCond::OnComplete,
        }
    }
    pub fn on_failure(target: impl Into<TaskRef>) -> Dep {
        Dep {
            target: target.into(),
            cond: EdgeCond::OnFailure,
        }
    }
}

/// What to do when a fixed-start task could not start within its tolerance.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum MissPolicy {
    /// Start as soon as possible, ahead of all other work.
    #[default]
    RunLate,
    /// Give up; the task ends as `Missed`.
    Skip,
    /// The task ends as `Failed`.
    Fail,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct FixedStart {
    pub at: Time,
    pub tolerance: Duration,
    pub on_miss: MissPolicy,
}

impl FixedStart {
    pub fn at(at: Time) -> FixedStart {
        FixedStart {
            at,
            tolerance: Duration::from_secs(1),
            on_miss: MissPolicy::RunLate,
        }
    }
}

/// Retry behaviour for retryable failures.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts including the first one (>= 1).
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
    /// Full jitter: the delay is uniformly drawn from `[0, backoff]`.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(60),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    pub fn none() -> Self {
        RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        }
    }
    /// Backoff before attempt `next_attempt` (2 = first retry), before jitter.
    pub fn backoff(&self, next_attempt: u32) -> Duration {
        let exp = next_attempt.saturating_sub(2).min(63) as i32;
        let d = self.initial_backoff.as_secs_f64() * self.multiplier.powi(exp);
        Duration::from_secs_f64(d.min(self.max_backoff.as_secs_f64()).max(0.0))
    }
}

/// Key/value labels attached to a task.
pub type Tags = Arc<[(Arc<str>, Arc<str>)]>;

/// Shared `"default"` name, so building a spec does not allocate for kind and pool.
pub fn default_name() -> Arc<str> {
    static DEFAULT: std::sync::LazyLock<Arc<str>> =
        std::sync::LazyLock::new(|| Arc::from("default"));
    DEFAULT.clone()
}

/// Everything the engine needs to schedule one task.
#[derive(Debug)]
pub struct TaskSpec<P> {
    pub payload: Arc<P>,
    pub kind: Arc<str>,
    pub pool: Arc<str>,
    pub priority: Priority,
    pub key: Option<Arc<str>>,
    pub deadline: Option<Time>,
    pub fixed_start: Option<FixedStart>,
    pub duration_hint: Option<Duration>,
    pub timeout: Option<Duration>,
    /// `None` uses the pool default.
    pub retry: Option<RetryPolicy>,
    pub deps: Vec<Dep>,
    pub resources: SmallVec<[(Arc<str>, u32); 2]>,
    pub tags: Vec<(Arc<str>, Arc<str>)>,
}

impl<P> Clone for TaskSpec<P> {
    fn clone(&self) -> Self {
        TaskSpec {
            payload: self.payload.clone(),
            kind: self.kind.clone(),
            pool: self.pool.clone(),
            priority: self.priority,
            key: self.key.clone(),
            deadline: self.deadline,
            fixed_start: self.fixed_start,
            duration_hint: self.duration_hint,
            timeout: self.timeout,
            retry: self.retry,
            deps: self.deps.clone(),
            resources: self.resources.clone(),
            tags: self.tags.clone(),
        }
    }
}

impl<P> TaskSpec<P> {
    /// Same task description with a different payload type.
    pub fn map_payload<Q>(self, payload: Arc<Q>) -> TaskSpec<Q> {
        TaskSpec {
            payload,
            kind: self.kind,
            pool: self.pool,
            priority: self.priority,
            key: self.key,
            deadline: self.deadline,
            fixed_start: self.fixed_start,
            duration_hint: self.duration_hint,
            timeout: self.timeout,
            retry: self.retry,
            deps: self.deps,
            resources: self.resources,
            tags: self.tags,
        }
    }

    pub fn new(payload: P) -> Self {
        Self::from_arc(Arc::new(payload))
    }
    pub fn from_arc(payload: Arc<P>) -> Self {
        TaskSpec {
            payload,
            kind: default_name(),
            pool: default_name(),
            priority: Priority::Normal,
            key: None,
            deadline: None,
            fixed_start: None,
            duration_hint: None,
            timeout: None,
            retry: None,
            deps: Vec::new(),
            resources: SmallVec::new(),
            tags: Vec::new(),
        }
    }
    pub fn kind(mut self, k: impl Into<Arc<str>>) -> Self {
        self.kind = k.into();
        self
    }
    pub fn pool(mut self, p: impl Into<Arc<str>>) -> Self {
        self.pool = p.into();
        self
    }
    pub fn priority(mut self, p: Priority) -> Self {
        self.priority = p;
        self
    }
    pub fn key(mut self, k: impl Into<Arc<str>>) -> Self {
        self.key = Some(k.into());
        self
    }
    pub fn deadline(mut self, t: Time) -> Self {
        self.deadline = Some(t);
        self
    }
    pub fn fixed_start(mut self, f: FixedStart) -> Self {
        self.fixed_start = Some(f);
        self
    }
    pub fn duration_hint(mut self, d: Duration) -> Self {
        self.duration_hint = Some(d);
        self
    }
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }
    pub fn retry(mut self, r: RetryPolicy) -> Self {
        self.retry = Some(r);
        self
    }
    pub fn dep(mut self, d: Dep) -> Self {
        self.deps.push(d);
        self
    }
    pub fn after(self, target: impl Into<TaskRef>) -> Self {
        self.dep(Dep::on_success(target))
    }
    pub fn resource(mut self, name: impl Into<Arc<str>>, amount: u32) -> Self {
        self.resources.push((name.into(), amount));
        self
    }
    pub fn tag(mut self, k: impl Into<Arc<str>>, v: impl Into<Arc<str>>) -> Self {
        self.tags.push((k.into(), v.into()));
        self
    }
}
