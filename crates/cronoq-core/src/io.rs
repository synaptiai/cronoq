//! Engine outputs (actions), outcomes, statuses, errors and persistence mutations.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use crate::spec::{LeaseId, MissPolicy, PoolId, TaskId, TaskSpec};
use crate::time::Time;

/// Lifecycle status of a task.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    /// Waiting for dependencies.
    Blocked,
    /// Waiting for its fixed start time.
    Scheduled,
    /// Eligible and queued.
    Ready,
    Running,
    /// Waiting for a retry backoff to elapse.
    Retrying,
    Succeeded,
    /// Permanent failure (or a fixed-start miss with `MissPolicy::Fail`).
    Failed,
    /// Retryable failures exhausted all attempts.
    DeadLettered,
    Cancelled,
    /// A dependency edge condition can no longer be met.
    Skipped,
    /// Fixed start missed with `MissPolicy::Skip`.
    Missed,
    /// Evicted under overload.
    Shed,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            TaskStatus::Blocked
                | TaskStatus::Scheduled
                | TaskStatus::Ready
                | TaskStatus::Running
                | TaskStatus::Retrying
        )
    }
    pub fn name(self) -> &'static str {
        match self {
            TaskStatus::Blocked => "blocked",
            TaskStatus::Scheduled => "scheduled",
            TaskStatus::Ready => "ready",
            TaskStatus::Running => "running",
            TaskStatus::Retrying => "retrying",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
            TaskStatus::DeadLettered => "dead_lettered",
            TaskStatus::Cancelled => "cancelled",
            TaskStatus::Skipped => "skipped",
            TaskStatus::Missed => "missed",
            TaskStatus::Shed => "shed",
        }
    }
    pub fn from_name(s: &str) -> Option<TaskStatus> {
        use TaskStatus::*;
        Some(match s {
            "blocked" => Blocked,
            "scheduled" => Scheduled,
            "ready" => Ready,
            "running" => Running,
            "retrying" => Retrying,
            "succeeded" => Succeeded,
            "failed" => Failed,
            "dead_lettered" => DeadLettered,
            "cancelled" => Cancelled,
            "skipped" => Skipped,
            "missed" => Missed,
            "shed" => Shed,
            _ => return None,
        })
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Result of one attempt, reported by the executor.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Success,
    Failure {
        retryable: bool,
        retry_after: Option<Duration>,
        message: Arc<str>,
    },
    /// The executor observed cancellation.
    Cancelled,
}

impl Outcome {
    pub fn retryable(msg: impl Into<Arc<str>>) -> Outcome {
        Outcome::Failure {
            retryable: true,
            retry_after: None,
            message: msg.into(),
        }
    }
    pub fn permanent(msg: impl Into<Arc<str>>) -> Outcome {
        Outcome::Failure {
            retryable: false,
            retry_after: None,
            message: msg.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitError {
    UnknownPool(Arc<str>),
    UnknownResource(Arc<str>),
    /// Requested amount can never be satisfied by the resource.
    ResourceExceedsCapacity(Arc<str>),
    ReservationConflict,
    Cycle,
    UnknownDependency(Arc<str>),
    /// Bounds reached and the pool's policy is `Wait`: retry once `Action::SpaceAvailable` is seen.
    Full,
    /// Bounds reached and the policy is `Reject` (or shedding found no victim).
    Overloaded,
    /// The key already exists and the batch tried to define it twice.
    DuplicateKeyInBatch(Arc<str>),
    Invalid(&'static str),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmitError::UnknownPool(p) => write!(f, "unknown pool '{p}'"),
            SubmitError::UnknownResource(r) => write!(f, "unknown resource '{r}'"),
            SubmitError::ResourceExceedsCapacity(r) => {
                write!(f, "requested amount exceeds capacity of resource '{r}'")
            }
            SubmitError::ReservationConflict => f.write_str("fixed-start reservation conflict"),
            SubmitError::Cycle => f.write_str("dependency cycle"),
            SubmitError::UnknownDependency(k) => write!(f, "unknown dependency '{k}'"),
            SubmitError::Full => f.write_str("queue full"),
            SubmitError::Overloaded => f.write_str("overloaded"),
            SubmitError::DuplicateKeyInBatch(k) => write!(f, "duplicate key '{k}' in batch"),
            SubmitError::Invalid(m) => write!(f, "invalid task: {m}"),
        }
    }
}

impl std::error::Error for SubmitError {}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Submitted {
    pub id: TaskId,
    /// The key was already live; `id` is the existing task.
    pub duplicate: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AbortReason {
    Timeout,
    Cancelled,
    LeaseExpired,
}

/// A task handed to an executor.
#[derive(Debug)]
pub struct Dispatch<P> {
    pub lease: LeaseId,
    pub id: TaskId,
    pub pool: PoolId,
    pub attempt: u32,
    pub payload: Arc<P>,
    pub kind: Arc<str>,
    pub key: Option<Arc<str>>,
    pub timeout: Option<Duration>,
    pub deadline: Option<Time>,
    pub tags: crate::spec::Tags,
}

impl<P> Clone for Dispatch<P> {
    fn clone(&self) -> Self {
        Dispatch {
            lease: self.lease,
            id: self.id,
            pool: self.pool,
            attempt: self.attempt,
            payload: self.payload.clone(),
            kind: self.kind.clone(),
            key: self.key.clone(),
            timeout: self.timeout,
            deadline: self.deadline,
            tags: self.tags.clone(),
        }
    }
}

/// Status transition event.
#[derive(Clone, Debug, PartialEq)]
pub struct StatusEvent {
    pub id: TaskId,
    pub key: Option<Arc<str>>,
    pub status: TaskStatus,
    pub attempt: u32,
    pub at: Time,
    pub message: Option<Arc<str>>,
}

/// Overlap behaviour for recurring schedules.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OverlapPolicy {
    /// Skip an occurrence while the previous one is still live.
    #[default]
    Skip,
    /// Start occurrences regardless of the previous one.
    Allow,
}

#[derive(Debug)]
pub struct RecurringSpec<P> {
    pub name: Arc<str>,
    /// Cron expression (5 or 6 fields, seconds optional).
    pub cron: Arc<str>,
    /// IANA time zone (default UTC).
    pub tz: Option<Arc<str>>,
    pub template: TaskSpec<P>,
    pub overlap: OverlapPolicy,
    pub tolerance: Duration,
    pub on_miss: MissPolicy,
}

impl<P> Clone for RecurringSpec<P> {
    fn clone(&self) -> Self {
        RecurringSpec {
            name: self.name.clone(),
            cron: self.cron.clone(),
            tz: self.tz.clone(),
            template: self.template.clone(),
            overlap: self.overlap,
            tolerance: self.tolerance,
            on_miss: self.on_miss,
        }
    }
}

/// Durable state change (emitted only with `CoreConfig::persist`).
#[derive(Debug)]
pub enum Mutation<P> {
    Submitted {
        id: TaskId,
        seq: u64,
        arrival: Time,
        spec: TaskSpec<P>,
    },
    Status {
        id: TaskId,
        status: TaskStatus,
        attempt: u32,
        at: Time,
        message: Option<Arc<str>>,
    },
    RecurringUpsert {
        spec: RecurringSpec<P>,
    },
    RecurringRemove {
        name: Arc<str>,
    },
    /// Next occurrence time of a recurring schedule.
    RecurringNext {
        name: Arc<str>,
        next: Option<Time>,
    },
}

/// Something the driver must do.
#[derive(Debug)]
pub enum Action<P> {
    Dispatch(Dispatch<P>),
    /// Stop the attempt (the engine already treats it as finished).
    Abort {
        lease: LeaseId,
        id: TaskId,
        reason: AbortReason,
    },
    /// Ask the pool's scaler for `desired` workers; report back with `set_capacity`.
    ScaleTo {
        pool: PoolId,
        desired: u32,
    },
    Status(StatusEvent),
    Persist(Box<Mutation<P>>),
    /// Space freed after a `SubmitError::Full`; waiting submitters may retry.
    SpaceAvailable,
}

/// A non-terminal task restored from durable storage.
#[derive(Debug)]
pub struct RecoveredTask<P> {
    pub id: TaskId,
    pub seq: u64,
    pub arrival: Time,
    pub spec: TaskSpec<P>,
    /// Attempts already made (a task that was running counts its lost attempt).
    pub attempt: u32,
}

#[derive(Clone, Debug)]
pub struct RecoveredTerminal {
    pub id: TaskId,
    pub key: Option<Arc<str>>,
    pub status: TaskStatus,
    pub at: Time,
}

#[derive(Debug)]
pub struct Recovered<P> {
    pub tasks: Vec<RecoveredTask<P>>,
    pub terminal: Vec<RecoveredTerminal>,
    pub recurring: Vec<(RecurringSpec<P>, Option<Time>)>,
    /// Lowest task id the engine may hand out next.
    pub next_id: u64,
}

impl<P> Default for Recovered<P> {
    fn default() -> Self {
        Recovered {
            tasks: Vec::new(),
            terminal: Vec::new(),
            recurring: Vec::new(),
            next_id: 0,
        }
    }
}
