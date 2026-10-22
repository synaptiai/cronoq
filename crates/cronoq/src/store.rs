//! Durable storage for scheduler state.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use cronoq_core::{Mutation, Priority, Recovered, TaskId, TaskRef, TaskStatus};

use crate::error::StoreError;

/// Wall-clock anchor of engine time, for converting persisted timestamps:
/// `unix_nanos = unix_nanos_at_zero + engine_nanos`.
///
/// The runtime uses Unix-epoch engine time, so it always passes `unix_nanos_at_zero = 0`;
/// stores should still apply it so they work with other drivers.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TimeBase {
    /// Unix time, in nanoseconds, of engine time zero.
    pub unix_nanos_at_zero: i64,
}

/// Persistence backend. The engine is the system of record while running; the store is a
/// write-ahead log of its state transitions, read back on startup or failover.
///
/// The scheduler acknowledges submissions, cancellations and schedule changes only after the
/// batch containing them has been committed, and dispatches work only after it is durable.
/// Return [`StoreError::Unavailable`] for transient failures (the driver keeps retrying
/// while the engine keeps running), [`StoreError::Fenced`] when another instance took over.
#[async_trait]
pub trait Store<P, O>: Send + Sync + 'static {
    /// Whether mutations should be produced at all.
    fn persists(&self) -> bool {
        true
    }
    /// Block until this instance is the leader; returns the new fencing epoch.
    async fn acquire_leadership(&self) -> Result<u16, StoreError>;
    /// Give up leadership (e.g. when startup fails after it was acquired).
    async fn release_leadership(&self) {}
    /// Durably apply a batch, in order. Must fail with `StoreError::Fenced` if `epoch` is
    /// stale. `outputs` holds the outputs of tasks that succeed in this batch.
    async fn commit(
        &self,
        epoch: u16,
        base: TimeBase,
        batch: &[Mutation<P>],
        outputs: &[(TaskId, &O)],
    ) -> Result<(), StoreError>;
    /// Load non-terminal tasks, recent terminal statuses and schedules.
    async fn recover(&self, base: TimeBase) -> Result<Recovered<P>, StoreError>;
    /// Output of a task that succeeded earlier, if the store still has it (used for handles
    /// to tasks that finished before they were requested, e.g. duplicate submissions).
    async fn load_output(&self, _id: TaskId) -> Result<Option<O>, StoreError> {
        Ok(None)
    }
}

/// A task as a durable store last recorded it, for lookups after it left the engine.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskRecord<O> {
    /// Task id.
    pub id: TaskId,
    /// Deduplication key, if any.
    pub key: Option<String>,
    /// Pool the task runs in.
    pub pool: String,
    /// Task kind.
    pub kind: String,
    /// Priority.
    pub priority: Priority,
    /// Tenant the task was submitted for, if any.
    pub tenant: Option<String>,
    /// `None` while queued: stores do not track blocked/scheduled/ready separately.
    pub status: Option<TaskStatus>,
    /// Number of attempts started.
    pub attempt: u32,
    /// Last message recorded for the task (e.g. the latest error).
    pub message: Option<String>,
    /// Output, once the task succeeded.
    pub output: Option<O>,
    /// Time of the last recorded transition.
    pub updated: SystemTime,
}

/// Lookups and housekeeping a server needs from a durable store, beyond what the scheduler
/// itself uses through [`Store`].
#[async_trait]
pub trait StoreQueries<O>: Send + Sync + 'static {
    /// The task `target` refers to: by id, or the newest task with that key.
    async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord<O>>, StoreError>;
    /// Delete finished tasks older than `older_than` that no live task depends on; returns
    /// how many were deleted.
    async fn prune(&self, older_than: Duration) -> Result<u64, StoreError>;
    /// Finished tasks in id order: those in one of `pools` (any if empty) with one of
    /// `statuses` (any finished status if empty) and ids above `after`, at most `limit`.
    async fn list_finished(
        &self,
        pools: &[&str],
        statuses: &[TaskStatus],
        after: Option<TaskId>,
        limit: usize,
    ) -> Result<Vec<TaskRecord<O>>, StoreError>;
    /// Whether this instance currently holds leadership.
    fn is_leader(&self) -> bool;
    /// Resolves once leadership held by this instance is lost.
    async fn leadership_lost(&self);
}

/// No persistence.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryStore;

#[async_trait]
impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Store<P, O> for MemoryStore {
    fn persists(&self) -> bool {
        false
    }
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        Ok(0)
    }
    async fn commit(
        &self,
        _: u16,
        _: TimeBase,
        _: &[Mutation<P>],
        _: &[(TaskId, &O)],
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn recover(&self, _: TimeBase) -> Result<Recovered<P>, StoreError> {
        Ok(Recovered::default())
    }
}
