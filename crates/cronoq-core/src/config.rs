//! Engine configuration.

use core::time::Duration;
use std::sync::Arc;

use crate::spec::{Priority, RetryPolicy};

/// Per-class score parameters: `base + min(aging_rate * wait, age_cap) + urgency`.
#[derive(Clone, Debug, PartialEq)]
pub struct PriorityConfig {
    pub base: [f64; Priority::COUNT],
    /// Score points per second of waiting.
    pub aging_rate: [f64; Priority::COUNT],
    pub age_cap: [f64; Priority::COUNT],
}

impl Default for PriorityConfig {
    fn default() -> Self {
        PriorityConfig {
            base: [10_000.0, 1_000.0, 100.0, 10.0],
            aging_rate: [2.0; 4],
            age_cap: [800.0; 4],
        }
    }
}

/// Deadline urgency curve (monotonically increasing as slack shrinks).
#[derive(Clone, Debug, PartialEq)]
pub struct UrgencyConfig {
    /// Urgency reached when slack hits zero.
    pub max: f64,
    /// Minimum horizon before the latest start time where urgency starts rising.
    pub window: Duration,
    /// Extra urgency per second past the latest start time.
    pub overdue_rate: f64,
    pub overdue_cap: f64,
}

impl Default for UrgencyConfig {
    fn default() -> Self {
        UrgencyConfig {
            max: 5_000.0,
            window: Duration::from_secs(5),
            overdue_rate: 100.0,
            overdue_cap: 3_000.0,
        }
    }
}

/// Behaviour when a submission would exceed queue bounds.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject with a retryable `Full` error; the runtime parks the caller until space frees up.
    #[default]
    Wait,
    /// Reject with `Overloaded`.
    Reject,
    /// Evict the newest pending task of the lowest class strictly below the incoming one.
    ShedLowest,
    /// Evict the oldest pending `Low` task.
    ShedOldest,
}

/// Maximum number of not-yet-running tasks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bounds {
    pub max_pending: Option<usize>,
    pub max_pending_per_class: [Option<usize>; Priority::COUNT],
}

/// How to treat a dependency on a key the engine has never seen.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum UnknownDependency {
    #[default]
    Reject,
    /// Wait for a task with that key to be submitted; skip the dependent after `timeout`.
    Await { timeout: Option<Duration> },
}

/// Queue-pressure autoscaling parameters (see SPEC §10).
#[derive(Clone, Debug, PartialEq)]
pub struct AutoscaleConfig {
    pub interval: Duration,
    pub target_utilization: f64,
    pub drain_time: Duration,
    /// Maximum acceptable predicted queue wait per class (`None` = no SLO).
    pub slo: [Option<Duration>; Priority::COUNT],
    /// Reservations starting within this horizon count towards desired capacity.
    pub reservation_horizon: Duration,
    pub stabilization_window: Duration,
    pub cooldown: Duration,
    /// Max relative growth per step (1.0 = +100%).
    pub max_step_up_ratio: f64,
    pub max_step_up_abs: u32,
    pub max_step_down: u32,
    /// Smoothing factor for rate estimates (0..1, weight of the newest sample).
    pub ewma_alpha: f64,
    pub cost_per_worker_second: f64,
}

impl Default for AutoscaleConfig {
    fn default() -> Self {
        AutoscaleConfig {
            interval: Duration::from_secs(1),
            target_utilization: 0.75,
            drain_time: Duration::from_secs(30),
            slo: [None; 4],
            reservation_horizon: Duration::from_secs(60),
            stabilization_window: Duration::from_secs(60),
            cooldown: Duration::from_secs(30),
            max_step_up_ratio: 1.0,
            max_step_up_abs: 4,
            max_step_down: 1,
            ewma_alpha: 0.3,
            cost_per_worker_second: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub name: Arc<str>,
    pub min_workers: u32,
    pub max_workers: u32,
    pub initial_workers: u32,
    /// `None` disables autoscaling (capacity changes only via `CapacityChanged`).
    pub autoscale: Option<AutoscaleConfig>,
    pub retry: RetryPolicy,
    pub bounds: Bounds,
    pub overflow: OverflowPolicy,
    /// Fraction of `max_workers` that fixed-start reservations may occupy at once (at least 1 slot).
    pub reservable_fraction: f64,
    /// Duration estimate when nothing else is known.
    pub default_duration: Duration,
    pub max_hold: Duration,
    /// Lease time-to-live; `Some` for remote workers that must heartbeat.
    pub lease_ttl: Option<Duration>,
    pub unknown_dependency: UnknownDependency,
}

impl PoolConfig {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        PoolConfig {
            name: name.into(),
            min_workers: 1,
            max_workers: 10,
            initial_workers: 1,
            autoscale: Some(AutoscaleConfig::default()),
            retry: RetryPolicy::default(),
            bounds: Bounds::default(),
            overflow: OverflowPolicy::Wait,
            reservable_fraction: 0.5,
            default_duration: Duration::from_secs(1),
            max_hold: Duration::from_secs(300),
            lease_ttl: None,
            unknown_dependency: UnknownDependency::Reject,
        }
    }
    /// Fixed worker count, no autoscaling.
    pub fn fixed(name: impl Into<Arc<str>>, workers: u32) -> Self {
        let mut p = Self::new(name);
        p.min_workers = workers;
        p.max_workers = workers;
        p.initial_workers = workers;
        p.autoscale = None;
        p
    }
    pub fn workers(mut self, min: u32, initial: u32, max: u32) -> Self {
        self.min_workers = min;
        self.initial_workers = initial;
        self.max_workers = max;
        self
    }
    pub(crate) fn reservation_limit(&self) -> u32 {
        ((self.max_workers as f64 * self.reservable_fraction).floor() as u32).max(1)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResourceKind {
    /// At most `permits` units held concurrently; released at the end of each attempt.
    Semaphore { permits: u32 },
    /// Consumed units refill at `rate` per second up to `burst`.
    TokenBucket { rate: f64, burst: u32 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResourceConfig {
    pub name: Arc<str>,
    pub kind: ResourceKind,
}

impl ResourceConfig {
    pub fn semaphore(name: impl Into<Arc<str>>, permits: u32) -> Self {
        ResourceConfig {
            name: name.into(),
            kind: ResourceKind::Semaphore { permits },
        }
    }
    pub fn token_bucket(name: impl Into<Arc<str>>, rate: f64, burst: u32) -> Self {
        ResourceConfig {
            name: name.into(),
            kind: ResourceKind::TokenBucket { rate, burst },
        }
    }
}

#[derive(Clone, Debug)]
pub struct CoreConfig {
    pub pools: Vec<PoolConfig>,
    pub resources: Vec<ResourceConfig>,
    pub priority: PriorityConfig,
    pub urgency: UrgencyConfig,
    pub global_bounds: Bounds,
    /// How long terminal statuses are remembered (for late dependents and lookups).
    pub retention: Duration,
    pub retention_max: usize,
    /// Emit `Action::Persist` mutations.
    pub persist: bool,
    /// Unix time (ns) corresponding to `Time::ZERO`; used for cron schedules.
    pub epoch_unix_nanos: i64,
    /// High bits mixed into task ids (leader epoch in HA mode).
    pub id_epoch: u16,
    /// Window for snapshot metrics.
    pub metrics_window: Duration,
    pub seed: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        CoreConfig {
            pools: vec![PoolConfig::new("default")],
            resources: Vec::new(),
            priority: PriorityConfig::default(),
            urgency: UrgencyConfig::default(),
            global_bounds: Bounds::default(),
            retention: Duration::from_secs(3600),
            retention_max: 1_000_000,
            persist: false,
            epoch_unix_nanos: 0,
            id_epoch: 0,
            metrics_window: Duration::from_secs(60),
            seed: 0x5eed_c0de,
        }
    }
}
