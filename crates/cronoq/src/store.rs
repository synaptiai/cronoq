//! Durable storage for scheduler state.

use async_trait::async_trait;
use cronoq_core::{Mutation, Recovered, TaskId};

use crate::error::StoreError;

/// Wall-clock anchor of engine time, for converting persisted timestamps.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TimeBase {
    pub unix_nanos_at_zero: i64,
}

/// Persistence backend. The engine is the system of record while running; the store is a
/// write-ahead log of its state transitions, read back on startup or failover.
#[async_trait]
pub trait Store<P, O>: Send + Sync + 'static {
    /// Whether mutations should be produced at all.
    fn persists(&self) -> bool {
        true
    }
    /// Block until this instance is the leader; returns the new fencing epoch.
    async fn acquire_leadership(&self) -> Result<u16, StoreError>;
    /// Durably apply a batch. Must fail with `StoreError::Fenced` if `epoch` is stale.
    async fn commit(
        &self,
        epoch: u16,
        base: TimeBase,
        batch: &[Mutation<P>],
        outputs: &[(TaskId, &O)],
    ) -> Result<(), StoreError>;
    /// Load non-terminal tasks, recent terminal statuses and schedules.
    async fn recover(&self, base: TimeBase) -> Result<Recovered<P>, StoreError>;
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
