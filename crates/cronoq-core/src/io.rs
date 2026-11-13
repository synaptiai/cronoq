//! Engine outputs (actions), outcomes, statuses, errors and persistence mutations.

use core::fmt;
use core::time::Duration;
use std::sync::Arc;

use crate::spec::{LeaseId, MissPolicy, PoolId, Priority, TaskId, TaskSpec};
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
    /// A submission asked to replace the task holding its key, but differs from it in
    /// something a replacement cannot change (named here).
    ReplaceMismatch(&'static str),
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
            SubmitError::ReplaceMismatch(what) => {
                write!(f, "cannot replace the task with this key: {what}")
            }
            SubmitError::Invalid(m) => write!(f, "invalid task: {m}"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// What a submission whose key is already in use does.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum KeyConflict {
    /// Return the existing task and drop the new submission.
    #[default]
    ReturnExisting,
    /// The newest submission wins: replace the existing task if no attempt of it has started,
    /// and otherwise run the new submission after it (see [`Core::submit_with`](crate::Core::submit_with)).
    ReplaceQueued,
}

/// Options of one submission that are not part of the task itself.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SubmitOptions {
    /// What to do when the task's key is already in use.
    pub on_key_conflict: KeyConflict,
    /// With [`KeyConflict::ReplaceQueued`]: the task takes the place in line of this
    /// submission, not the place of the task it replaces, and its waiting time starts now.
    pub renew_position: bool,
}

/// What an accepted submission did.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum SubmitOutcome {
    /// A new task was created.
    #[default]
    Created,
    /// The key was already in use and the existing task is returned unchanged.
    Existing,
    /// The existing task had not started: it now carries this submission.
    Replaced,
    /// The existing task has started, so its payload is frozen: this submission is kept and
    /// runs as a new task with the same key once that task succeeds, fails or is
    /// dead-lettered. The id returned is the existing task's.
    Pending,
}

/// Result of an accepted submission.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Submitted {
    /// Id of the task.
    pub id: TaskId,
    /// The key was already in use; `id` is the existing task.
    pub duplicate: bool,
    /// What the submission did.
    pub outcome: SubmitOutcome,
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
    /// Labels of the worker group chosen for this attempt (empty for the unlabeled group);
    /// a driver with several groups must run the attempt on a worker of this group.
    pub group: Labels,
}

/// Sorted `(name, value)` labels of a worker group.
pub type Labels = Arc<[(Arc<str>, Arc<str>)]>;

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
            group: self.group.clone(),
        }
    }
}

/// What a [`StatusEvent`] reports.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// The task changed status.
    #[default]
    Transition,
    /// The task was changed by [`Core::update`](crate::Core::update); its status is the same.
    Updated,
    /// A submission with the task's key replaced it, or was stored to run after it
    /// ([`SubmitOutcome::Replaced`] or [`SubmitOutcome::Pending`]); its status is the same.
    Replaced,
    /// The task was created from a stored replacement when the task that held its key ended.
    /// `message` names that task's id.
    Successor,
}

/// Status transition event, or a change to a task that keeps its status (see
/// [`StatusEvent::kind`]).
#[derive(Clone, Debug, PartialEq)]
pub struct StatusEvent {
    /// Task id.
    pub id: TaskId,
    /// Pool of the task.
    pub pool: PoolId,
    /// Idempotency key, if any.
    pub key: Option<Arc<str>>,
    /// New status (the current one for [`EventKind::Updated`]).
    pub status: TaskStatus,
    /// Attempts made so far.
    pub attempt: u32,
    /// Engine time of the transition.
    pub at: Time,
    /// Error or reason, if any.
    pub message: Option<Arc<str>>,
    /// The task's priority class at the time of the event.
    pub priority: Priority,
    /// What the event reports.
    pub kind: EventKind,
}

/// Changes to a task that has not finished, for [`Core::update`](crate::Core::update). Fields
/// left `None` keep their value.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskPatch {
    /// New priority class.
    pub priority: Option<Priority>,
    /// New deadline; `Some(None)` removes it.
    pub deadline: Option<Option<Time>>,
    /// New tags, replacing the task's own. Tags whose name starts with `cronoq.` belong to
    /// the engine: they cannot be set here and are kept as they are.
    pub tags: Option<Vec<(Arc<str>, Arc<str>)>>,
}

impl TaskPatch {
    /// Whether the patch names no field at all.
    pub fn is_empty(&self) -> bool {
        self.priority.is_none() && self.deadline.is_none() && self.tags.is_none()
    }
    /// Set the priority class.
    pub fn priority(mut self, p: Priority) -> Self {
        self.priority = Some(p);
        self
    }
    /// Set the deadline.
    pub fn deadline(mut self, t: Time) -> Self {
        self.deadline = Some(Some(t));
        self
    }
    /// Remove the deadline.
    pub fn no_deadline(mut self) -> Self {
        self.deadline = Some(None);
        self
    }
    /// Replace the tags.
    pub fn tags(mut self, tags: Vec<(Arc<str>, Arc<str>)>) -> Self {
        self.tags = Some(tags);
        self
    }
}

/// Result of [`Core::update`](crate::Core::update).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Updated {
    /// Id of the task.
    pub id: TaskId,
    /// Its status, which an update never changes.
    pub status: TaskStatus,
    /// An attempt is running: it is not interrupted, and the new values apply to what
    /// happens after it (a retry, and what the task reports).
    pub deferred: bool,
    /// Whether anything changed (`false` when every value given was already in place).
    pub changed: bool,
}

/// Why an update was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateError {
    /// No live task with this id or key (unknown, or already finished).
    NotFound,
    /// The task has a fixed start: its reservation was sized at admission and cannot change.
    FixedStart,
    /// The patch is malformed.
    Invalid(&'static str),
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdateError::NotFound => f.write_str("no live task with this id or key"),
            UpdateError::FixedStart => f.write_str("a task with a fixed start cannot be updated"),
            UpdateError::Invalid(m) => write!(f, "invalid update: {m}"),
        }
    }
}

impl std::error::Error for UpdateError {}

/// Overlap behaviour for recurring schedules.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OverlapPolicy {
    /// Skip an occurrence while the previous one is still live.
    #[default]
    Skip,
    /// Start occurrences regardless of the previous one.
    Allow,
    /// Queue each occurrence behind the previous one: it starts once the previous one has
    /// finished, whatever the outcome.
    Queue,
    /// Queue each occurrence behind the previous one and run it only if the previous one
    /// succeeded; otherwise it ends as `Skipped`, and so do the following ones, until the
    /// schedule is added again.
    QueueOnSuccess,
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
    /// Spec of each occurrence. Its `fixed_start` is set to the occurrence; a `key` is a
    /// prefix that each occurrence extends with `@` and its UTC start time (RFC 3339).
    pub template: TaskSpec<P>,
    /// Deadline of each occurrence, relative to its start.
    pub deadline_after: Option<Duration>,
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
            deadline_after: self.deadline_after,
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
    /// A live task was changed by [`Core::update`](crate::Core::update). The fields hold the
    /// task's values after the change, whether or not each one changed.
    Updated {
        /// Task id.
        id: TaskId,
        /// Priority class.
        priority: Priority,
        /// Deadline, if any.
        deadline: Option<Time>,
        /// All tags of the task.
        tags: crate::spec::Tags,
        /// Time of the change.
        at: Time,
    },
    /// A live task that had not started was replaced by a new submission with its key: `spec`
    /// replaces everything stored for the task except its id, status and attempt count.
    Replaced {
        /// Task id.
        id: TaskId,
        /// Submission sequence number of the task from now on.
        seq: u64,
        /// Submission time of the task from now on.
        arrival: Time,
        /// The new task spec.
        spec: TaskSpec<P>,
    },
    /// A submission was stored to run after live task `id`, which has started. It replaces
    /// any replacement stored for that task before. A store drops it when `id` reaches a
    /// terminal status: the engine then submits the successor itself, in the same batch.
    PendingReplacement {
        /// The live task the submission waits for.
        id: TaskId,
        /// Sequence number of the replacing submission.
        seq: u64,
        /// Time of the replacing submission.
        arrival: Time,
        /// The successor takes this submission's place in line instead of the task's.
        renew: bool,
        /// The spec of the successor.
        spec: TaskSpec<P>,
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
        /// Task of the latest occurrence a queued schedule chains to (`None` = no chain yet).
        last: Option<TaskId>,
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
            Mutation::Updated {
                id,
                priority,
                deadline,
                tags,
                at,
            } => Mutation::Updated {
                id: *id,
                priority: *priority,
                deadline: *deadline,
                tags: tags.clone(),
                at: *at,
            },
            Mutation::Replaced {
                id,
                seq,
                arrival,
                spec,
            } => Mutation::Replaced {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                spec: spec.clone(),
            },
            Mutation::PendingReplacement {
                id,
                seq,
                arrival,
                renew,
                spec,
            } => Mutation::PendingReplacement {
                id: *id,
                seq: *seq,
                arrival: *arrival,
                renew: *renew,
                spec: spec.clone(),
            },
            Mutation::RecurringUpsert { spec } => Mutation::RecurringUpsert { spec: spec.clone() },
            Mutation::RecurringRemove { name } => Mutation::RecurringRemove { name: name.clone() },
            Mutation::RecurringNext { name, next, last } => Mutation::RecurringNext {
                name: name.clone(),
                next: *next,
                last: *last,
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

/// A replacement stored for a live task (see [`Mutation::PendingReplacement`]).
#[derive(Clone, Debug)]
pub struct RecoveredPending<P> {
    /// The live task the replacement waits for.
    pub id: TaskId,
    /// Sequence number of the replacing submission.
    pub seq: u64,
    /// Time of the replacing submission.
    pub arrival: Time,
    /// The successor takes the replacing submission's place in line.
    pub renew: bool,
    /// The spec of the successor.
    pub spec: TaskSpec<P>,
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

/// A recurring schedule restored from durable storage.
#[derive(Debug)]
pub struct RecoveredRecurring<P> {
    /// The schedule.
    pub spec: RecurringSpec<P>,
    /// Its stored next occurrence.
    pub next: Option<Time>,
    /// Task of the latest occurrence a queued schedule chains to.
    pub last: Option<TaskId>,
}

/// Engine state restored from durable storage, passed to `Core::recover`.
#[derive(Debug)]
pub struct Recovered<P> {
    /// Live (non-terminal) tasks.
    pub tasks: Vec<RecoveredTask<P>>,
    /// Recently finished tasks.
    pub terminal: Vec<RecoveredTerminal>,
    /// Recurring schedules.
    pub recurring: Vec<RecoveredRecurring<P>>,
    /// Replacements stored for live tasks.
    pub pending: Vec<RecoveredPending<P>>,
    /// Lowest task id the engine may hand out next.
    pub next_id: u64,
}

impl<P> Recovered<P> {
    /// The first recovered task id that does not belong to `shard`, if any: the store holds
    /// tasks of another shard. Ids of an unsharded server are caught only when their top byte
    /// differs from `shard` (early epochs look like shard 0's), so give shards new stores.
    pub fn outside_shard(&self, shard: u8) -> Option<TaskId> {
        let last = (self.next_id > 0).then(|| TaskId(self.next_id - 1));
        last.into_iter()
            .chain(self.tasks.iter().map(|t| t.id))
            .chain(self.terminal.iter().map(|t| t.id))
            .find(|id| id.shard() != shard)
    }
}

impl<P> Default for Recovered<P> {
    fn default() -> Self {
        Recovered {
            tasks: Vec::new(),
            terminal: Vec::new(),
            recurring: Vec::new(),
            pending: Vec::new(),
            next_id: 0,
        }
    }
}

/// A live task, as listed by [`Core::list_tasks`](crate::Core::list_tasks).
#[derive(Clone, Debug, PartialEq)]
pub struct TaskSummary {
    /// Task id.
    pub id: TaskId,
    /// Key, if any.
    pub key: Option<Arc<str>>,
    /// Pool.
    pub pool: Arc<str>,
    /// Kind.
    pub kind: Arc<str>,
    /// Priority class.
    pub priority: Priority,
    /// Current status.
    pub status: TaskStatus,
    /// Attempts started so far.
    pub attempt: u32,
    /// Tenant (pools with fairness).
    pub tenant: Option<Arc<str>>,
}

/// A recurring schedule, as listed by [`Core::list_recurring`](crate::Core::list_recurring).
#[derive(Clone, Debug, PartialEq)]
pub struct RecurringInfo {
    /// Schedule name.
    pub name: Arc<str>,
    /// Cron expression.
    pub cron: Arc<str>,
    /// Time zone (UTC if `None`).
    pub tz: Option<Arc<str>>,
    /// Pool of its occurrences.
    pub pool: Arc<str>,
    /// Kind of its occurrences.
    pub kind: Arc<str>,
    /// Overlap policy.
    pub overlap: OverlapPolicy,
    /// Next occurrence (`None` when the schedule has ended).
    pub next: Option<Time>,
    /// Task of the latest occurrence.
    pub last_task: Option<TaskId>,
}
