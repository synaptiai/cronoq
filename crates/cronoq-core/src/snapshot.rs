//! Point-in-time metrics: pool state plus quality signals computed over a sliding window.

use core::time::Duration;
use std::sync::Arc;

use crate::engine::Totals;
use crate::spec::Priority;
use crate::time::Time;

/// State and windowed quality metrics of one pool.
///
/// Counters and ratios cover the last [`PoolSnapshot::window`]
/// (`CoreConfig::metrics_window`); `worker_seconds` and `cost` accumulate since start.
#[derive(Clone, Debug)]
pub struct PoolSnapshot {
    /// Pool name.
    pub name: Arc<str>,
    /// Workers currently available to the pool.
    pub capacity: u32,
    /// Workers currently running a task.
    pub busy: u32,
    /// Workers held free for upcoming fixed-start tasks.
    pub held: u32,
    /// Last autoscaler target (equals capacity without autoscaling).
    pub desired: u32,
    /// Tasks not yet running (blocked, scheduled, ready or retrying), by priority class.
    pub pending_by_class: [usize; Priority::COUNT],
    /// Tasks ready to run right now (queued or due).
    pub ready: usize,
    /// Active fixed-start reservations.
    pub reservations: usize,
    /// Length of the metrics window.
    pub window: Duration,
    /// Attempts started in the window.
    pub dispatched: u64,
    /// Tasks that succeeded in the window.
    pub succeeded: u64,
    /// Attempts that failed in the window (including ones that will be retried).
    pub failed: u64,
    /// Retries scheduled in the window.
    pub retried: u64,
    /// Fraction of dispatches where no strictly higher class was waiting in the pool.
    pub priority_adherence: f64,
    /// Fraction of fixed-start tasks started within tolerance (first attempts only).
    pub fixed_start_hit_rate: f64,
    /// Busy slot-seconds / capacity slot-seconds.
    pub utilization: f64,
    /// Completed work seconds / capacity slot-seconds.
    pub schedule_efficiency: f64,
    /// Mean time from submission to first start.
    pub wait_mean: Duration,
    /// Median time from submission to first start.
    pub wait_p50: Duration,
    /// 90th-percentile time from submission to first start.
    pub wait_p90: Duration,
    /// 99th-percentile time from submission to first start.
    pub wait_p99: Duration,
    /// Median runtime of successful attempts.
    pub run_p50: Duration,
    /// 99th-percentile runtime of successful attempts.
    pub run_p99: Duration,
    /// Capacity slot-seconds provisioned since start.
    pub worker_seconds: f64,
    /// `worker_seconds × AutoscaleConfig::cost_per_worker_second` since start.
    pub cost: f64,
    /// Worker groups by label set (the unlabeled group first).
    pub groups: Vec<GroupSnapshot>,
}

impl PoolSnapshot {
    /// Total tasks not yet running.
    pub fn pending(&self) -> usize {
        self.pending_by_class.iter().sum()
    }
}

/// Engine-wide snapshot returned by [`Core::snapshot`](crate::Core::snapshot).
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Engine time of the snapshot.
    pub at: Time,
    /// Per-pool state, in configuration order.
    pub pools: Vec<PoolSnapshot>,
    /// Tasks not yet in a terminal status.
    pub live_tasks: usize,
    /// Attempts currently running.
    pub running: usize,
    /// Tasks not yet running.
    pub pending: usize,
    /// Counters since start.
    pub totals: Totals,
}

impl Snapshot {
    /// The snapshot of the pool named `name`.
    pub fn pool(&self, name: &str) -> Option<&PoolSnapshot> {
        self.pools.iter().find(|p| &*p.name == name)
    }
}

/// Capacity of the workers of a pool that share one label set.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GroupSnapshot {
    /// The group's labels, sorted (empty for unlabeled workers).
    pub labels: Vec<(Arc<str>, Arc<str>)>,
    /// Slots of the group's workers.
    pub capacity: u32,
    /// Slots running a task.
    pub busy: u32,
}
