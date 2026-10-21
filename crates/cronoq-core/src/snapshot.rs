//! Point-in-time metrics (the prototype's quality metrics, computed over a sliding window).

use core::time::Duration;
use std::sync::Arc;

use crate::engine::Totals;
use crate::spec::Priority;
use crate::time::Time;

#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    pub name: Arc<str>,
    pub capacity: u32,
    pub busy: u32,
    pub held: u32,
    /// Last autoscaler target (equals capacity without autoscaling).
    pub desired: u32,
    pub pending_by_class: [usize; Priority::COUNT],
    pub ready: usize,
    pub reservations: usize,
    pub window: Duration,
    pub dispatched: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub retried: u64,
    /// Fraction of dispatches where no strictly higher class was waiting in the pool.
    pub priority_adherence: f64,
    /// Fraction of fixed-start tasks started within tolerance.
    pub fixed_start_hit_rate: f64,
    /// Busy slot-seconds / capacity slot-seconds.
    pub utilization: f64,
    /// Completed work seconds / capacity slot-seconds.
    pub schedule_efficiency: f64,
    pub wait_mean: Duration,
    pub wait_p50: Duration,
    pub wait_p90: Duration,
    pub wait_p99: Duration,
    pub run_p50: Duration,
    pub run_p99: Duration,
    /// Since start.
    pub worker_seconds: f64,
    pub cost: f64,
}

impl PoolSnapshot {
    pub fn pending(&self) -> usize {
        self.pending_by_class.iter().sum()
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub at: Time,
    pub pools: Vec<PoolSnapshot>,
    pub live_tasks: usize,
    pub running: usize,
    pub pending: usize,
    pub totals: Totals,
}

impl Snapshot {
    pub fn pool(&self, name: &str) -> Option<&PoolSnapshot> {
        self.pools.iter().find(|p| &*p.name == name)
    }
}
