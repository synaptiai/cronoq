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
    /// An attempt is running on a worker.
    Running,
    /// Waiting for a retry backoff to elapse.
    Retrying,
    /// Completed successfully.
    Succeeded,
    /// Permanent failure (or a fixed-start miss with `MissPolicy::Fail`).
    Failed,
    /// Retryable failures exhausted all attempts.
    DeadLettered,
    /// Cancelled by a caller.
    Cancelled,
    /// A dependency edge condition can no longer be met.
    Skipped,
    /// Fixed start missed with `MissPolicy::Skip`.
    Missed,
    /// Evicted under overload.
    Shed,
}

impl TaskStatus {
    /// Whether the task has finished for good.
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
    /// Stable lower-case name (used in storage and APIs).
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
    /// Inverse of [`TaskStatus::name`].
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
    /// The attempt succeeded.
    Success,
    /// The attempt failed.
    Failure {
        /// Whether the failure may be retried under the task's retry policy.
        retryable: bool,
        /// Minimum delay before the retry (overrides a shorter backoff).
        retry_after: Option<Duration>,
        /// Error description.
        message: Arc<str>,
    },
    /// The executor observed cancellation.
    Cancelled,
}

impl Outcome {
    /// A retryable failure.
    pub fn retryable(msg: impl Into<Arc<str>>) -> Outcome {
        Outcome::Failure {
            retryable: true,
            retry_after: None,
            message: msg.into(),
        }
    }
    /// A permanent (non-retryable) failure.
    pub fn permanent(msg: impl Into<Arc<str>>) -> Outcome {
        Outcome::Failure {
            retryable: false,
            retry_after: None,
            message: msg.into(),
        }
    }
}

/// Why a submission was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitError {
    /// No pool with this name.
    UnknownPool(Arc<str>),
    /// No resource with this name.
    UnknownResource(Arc<str>),
    /// Requested amount can never be satisfied by the resource.
    ResourceExceedsCapacity(Arc<str>),
    /// The pool's fixed-start reservation limit would be exceeded at that time.
    ReservationConflict,
    /// The dependencies would form a cycle.
    Cycle,
    /// A dependency names an unknown key and the pool rejects unknown dependencies.
    UnknownDependency(Arc<str>),
    /// Bounds reached and the pool's policy is `Wait`: retry once `Action::SpaceAvailable` is seen.
    Full,
    /// Bounds reached and the policy is `Reject` (or shedding found no victim).
    Overloaded,
    /// The key already exists and the batch tried to define it twice.
    DuplicateKeyInBatch(Arc<str>),
    /// The spec is malformed.
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

/// Result of an accepted submission.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Submitted {
    /// Id of the task.
    pub id: TaskId,
    /// The key was already live; `id` is the existing task.
    pub duplicate: bool,
}

/// Why the engine aborted an attempt.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AbortReason {
    /// The attempt exceeded the task's timeout.
    Timeout,
    /// The task was cancelled.
    Cancelled,
    /// The worker stopped heartbeating its lease.
    LeaseExpired,
}

/// A task handed to an executor.
#[derive(Debug)]
pub struct Dispatch<P> {
    /// Lease identifying this attempt; pass it back to `complete` and `heartbeat`.
    pub lease: LeaseId,
    /// Task id.
    pub id: TaskId,
    /// Pool the task runs in.
    pub pool: PoolId,
    /// Attempt number, starting at 1.
    pub attempt: u32,
    /// Opaque task payload.
    pub payload: Arc<P>,
    /// Task kind (selects the handler).
    pub kind: Arc<str>,
    /// Idempotency key, if any.
    pub key: Option<Arc<str>>,
    /// Per-attempt timeout, if any.
    pub timeout: Option<Duration>,
    /// Completion deadline, if any.
    pub deadline: Option<Time>,
    /// Task tags.
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
    /// Task id.
    pub id: TaskId,
    /// Pool of the task.
    pub pool: PoolId,
    /// Idempotency key, if any.
    pub key: Option<Arc<str>>,
    /// New status.
    pub status: TaskStatus,
    /// Attempts made so far.
    pub attempt: u32,
    /// Engine time of the transition.
    pub at: Time,
    /// Error or reason, if any.
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

/// A cron schedule that submits a fixed-start task from `template` at each occurrence.
#[derive(Debug)]
pub struct RecurringSpec<P> {
    /// Unique schedule name (occurrences are tagged `cronoq.recurring=<name>`).
    pub name: Arc<str>,
    /// Cron expression (5 or 6 fields, seconds optional).
    pub cron: Arc<str>,
    /// IANA time zone (default UTC).
    pub tz: Option<Arc<str>>,
    /// Spec of each occurrence (its `fixed_start` is overwritten).
    pub template: TaskSpec<P>,
    /// What to do while the previous occurrence is still live.
    pub overlap: OverlapPolicy,
    /// How late an occurrence may start.
    pub tolerance: Duration,
    /// What to do when an occurrence misses its tolerance.
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
    /// A task was accepted.
    Submitted {
        /// Task id.
        id: TaskId,
        /// Submission sequence number (FIFO tie-break).
        seq: u64,
        /// Submission time.
        arrival: Time,
        /// The task spec.
        spec: TaskSpec<P>,
    },
    /// A task changed status.
    Status {
        /// Task id.
        id: TaskId,
        /// New status.
        status: TaskStatus,
        /// Attempts made so far.
        attempt: u32,
        /// Time of the transition.
        at: Time,
        /// Error or reason, if any.
        message: Option<Arc<str>>,
    },
    /// A recurring schedule was added or replaced.
    RecurringUpsert {
        /// The schedule.
        spec: RecurringSpec<P>,
    },
    /// A recurring schedule was removed.
    RecurringRemove {
        /// Schedule name.
        name: Arc<str>,
    },
    /// Next occurrence time of a recurring schedule.
    RecurringNext {
        /// Schedule name.
        name: Arc<str>,
        /// Next occurrence (`None` = the schedule has ended).
        next: Option<Time>,
    },
}

impl<P> Clone for Mutation<P> {
    fn clone(&self) -> Self {
        match self {
            Mutation::Submitted {
                id,
                seq,
                arrival,
                spec,
            } => Mutation::Submitted {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                spec: spec.clone(),
            },
            Mutation::Status {
                id,
                status,
                attempt,
                at,
                message,
            } => Mutation::Status {
                id: *id,
                status: *status,
                attempt: *attempt,
                at: *at,
                message: message.clone(),
            },
            Mutation::RecurringUpsert { spec } => Mutation::RecurringUpsert { spec: spec.clone() },
            Mutation::RecurringRemove { name } => Mutation::RecurringRemove { name: name.clone() },
            Mutation::RecurringNext { name, next } => Mutation::RecurringNext {
                name: name.clone(),
                next: *next,
            },
        }
    }
}

/// Something the driver must do.
#[derive(Debug)]
pub enum Action<P> {
    /// Run an attempt on a free worker of its pool.
    Dispatch(Dispatch<P>),
    /// Stop the attempt (the engine already treats it as finished).
    Abort {
        /// Lease of the attempt.
        lease: LeaseId,
        /// Task id.
        id: TaskId,
        /// Why it was aborted.
        reason: AbortReason,
    },
    /// Ask the pool's scaler for `desired` workers; report back with `set_capacity`.
    ScaleTo {
        /// Pool to scale.
        pool: PoolId,
        /// Requested worker count.
        desired: u32,
    },
    /// A task changed status.
    Status(StatusEvent),
    /// Write this mutation to durable storage (only with `CoreConfig::persist`).
    Persist(Box<Mutation<P>>),
    /// Space freed after a `SubmitError::Full`; waiting submitters may retry.
    SpaceAvailable,
}

/// A non-terminal task restored from durable storage.
#[derive(Debug)]
pub struct RecoveredTask<P> {
    /// Task id.
    pub id: TaskId,
    /// Original submission sequence number.
    pub seq: u64,
    /// Original submission time.
    pub arrival: Time,
    /// The task spec.
    pub spec: TaskSpec<P>,
    /// Attempts already made (a task that was running counts its lost attempt).
    pub attempt: u32,
}

/// A recently finished task restored into the retention table (for late dependents).
#[derive(Clone, Debug)]
pub struct RecoveredTerminal {
    /// Task id.
    pub id: TaskId,
    /// Idempotency key, if any.
    pub key: Option<Arc<str>>,
    /// Terminal status.
    pub status: TaskStatus,
    /// When it finished.
    pub at: Time,
}

/// Engine state restored from durable storage, passed to `Core::recover`.
#[derive(Debug)]
pub struct Recovered<P> {
    /// Live (non-terminal) tasks.
    pub tasks: Vec<RecoveredTask<P>>,
    /// Recently finished tasks.
    pub terminal: Vec<RecoveredTerminal>,
    /// Recurring schedules with their stored next occurrence.
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
