//! Engine configuration.

use core::time::Duration;
use std::sync::Arc;

use crate::spec::{Priority, RetryPolicy};

/// Per-class score parameters: `base + min(aging_rate * wait, age_cap) + urgency`.
#[derive(Clone, Debug, PartialEq)]
pub struct PriorityConfig {
    /// Starting score per class.
    pub base: [f64; Priority::COUNT],
    /// Score points per second of waiting.
    pub aging_rate: [f64; Priority::COUNT],
    /// Maximum score points gained from waiting.
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
    /// Maximum extra urgency from being overdue.
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
    /// Limit across all classes (`None` = unbounded).
    pub max_pending: Option<usize>,
    /// Limit per class (`None` = unbounded).
    pub max_pending_per_class: [Option<usize>; Priority::COUNT],
}

/// How to treat a dependency on a key the engine has never seen.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum UnknownDependency {
    /// Reject the submission with `UnknownDependency`.
    #[default]
    Reject,
    /// Wait for a task with that key to be submitted; skip the dependent after `timeout`.
    Await {
        /// How long to wait for the key (`None` = forever).
        timeout: Option<Duration>,
    },
}

/// Queue-pressure autoscaling parameters (see SPEC §10).
#[derive(Clone, Debug, PartialEq)]
pub struct AutoscaleConfig {
    /// Time between evaluations.
    pub interval: Duration,
    /// Utilization the arrival-rate term aims for (0..1].
    pub target_utilization: f64,
    /// Time in which the current backlog should be drained.
    pub drain_time: Duration,
    /// Maximum acceptable predicted queue wait per class (`None` = no SLO).
    pub slo: [Option<Duration>; Priority::COUNT],
    /// Reservations starting within this horizon count towards desired capacity.
    pub reservation_horizon: Duration,
    /// Scale-down uses the highest desired value seen within this window.
    pub stabilization_window: Duration,
    /// Minimum time between two scale-down steps.
    pub cooldown: Duration,
    /// Max relative growth per step (1.0 = +100%).
    pub max_step_up_ratio: f64,
    /// Minimum absolute growth allowed per step (the step is the larger of ratio and this).
    pub max_step_up_abs: u32,
    /// Max workers removed per scale-down step.
    pub max_step_down: u32,
    /// Smoothing factor for rate estimates (0..1, weight of the newest sample).
    pub ewma_alpha: f64,
    /// Cost of one worker-second, reported in snapshots.
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

/// A worker pool: capacity limits, scaling, retries, queue bounds and fixed-start behaviour.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Unique pool name.
    pub name: Arc<str>,
    /// Lower capacity bound for the autoscaler.
    pub min_workers: u32,
    /// Upper capacity bound for the autoscaler (also sizes the reservation limit).
    pub max_workers: u32,
    /// Capacity at start.
    pub initial_workers: u32,
    /// `None` disables autoscaling (capacity changes only via `CapacityChanged`).
    pub autoscale: Option<AutoscaleConfig>,
    /// Default retry policy for tasks without their own.
    pub retry: RetryPolicy,
    /// Pool queue bounds.
    pub bounds: Bounds,
    /// What to do when a bound is hit.
    pub overflow: OverflowPolicy,
    /// Fraction of `max_workers` that fixed-start reservations may occupy at once (at least 1 slot).
    pub reservable_fraction: f64,
    /// Duration estimate when nothing else is known.
    pub default_duration: Duration,
    /// Earliest a worker may be held free ahead of a fixed-start task.
    pub max_hold: Duration,
    /// Lease time-to-live; `Some` for remote workers that must heartbeat.
    pub lease_ttl: Option<Duration>,
    /// Handling of dependencies on unknown keys.
    pub unknown_dependency: UnknownDependency,
}

impl PoolConfig {
    /// Autoscaled pool with defaults: 1..=10 workers, starting at 1.
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
    /// Set min, initial and max worker counts.
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

/// Kind of a shared resource.
#[derive(Clone, Debug, PartialEq)]
pub enum ResourceKind {
    /// At most `permits` units held concurrently; released at the end of each attempt.
    Semaphore {
        /// Units available.
        permits: u32,
    },
    /// Consumed units refill at `rate` per second up to `burst`.
    TokenBucket {
        /// Units added per second.
        rate: f64,
        /// Bucket size (also the initial fill).
        burst: u32,
    },
}

/// A named resource tasks can require (concurrency or rate limit), shared across pools.
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceConfig {
    /// Unique resource name.
    pub name: Arc<str>,
    /// Semaphore or token bucket.
    pub kind: ResourceKind,
}

impl ResourceConfig {
    /// A semaphore with `permits` units.
    pub fn semaphore(name: impl Into<Arc<str>>, permits: u32) -> Self {
        ResourceConfig {
            name: name.into(),
            kind: ResourceKind::Semaphore { permits },
        }
    }
    /// A token bucket refilling `rate` units per second up to `burst`.
    pub fn token_bucket(name: impl Into<Arc<str>>, rate: f64, burst: u32) -> Self {
        ResourceConfig {
            name: name.into(),
            kind: ResourceKind::TokenBucket { rate, burst },
        }
    }
}

/// Complete engine configuration.
#[derive(Clone, Debug)]
pub struct CoreConfig {
    /// Worker pools (at least one; the first is the default).
    pub pools: Vec<PoolConfig>,
    /// Shared resources.
    pub resources: Vec<ResourceConfig>,
    /// Class scores and aging.
    pub priority: PriorityConfig,
    /// Deadline urgency curve.
    pub urgency: UrgencyConfig,
    /// Queue bounds across all pools.
    pub global_bounds: Bounds,
    /// How long terminal statuses are remembered (for late dependents and lookups).
    pub retention: Duration,
    /// Maximum number of remembered terminal statuses.
    pub retention_max: usize,
    /// Emit `Action::Persist` mutations.
    pub persist: bool,
    /// Unix time (ns) corresponding to `Time::ZERO`; used for cron schedules.
    pub epoch_unix_nanos: i64,
    /// High bits mixed into task and lease ids (leader epoch in HA mode), so ids issued by
    /// different leaders never collide.
    pub id_epoch: u16,
    /// Window for snapshot metrics.
    pub metrics_window: Duration,
    /// Seed for retry jitter.
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

impl CoreConfig {
    /// Check the configuration for errors that [`Core::new`](crate::Core::new) would otherwise
    /// reject with a panic (structural problems) or silently clamp (out-of-range numbers).
    pub fn validate(&self) -> Result<(), String> {
        if self.pools.is_empty() {
            return Err("at least one pool is required".into());
        }
        let mut names = std::collections::HashSet::new();
        for p in &self.pools {
            if !names.insert(&p.name) {
                return Err(format!("duplicate pool '{}'", p.name));
            }
            if p.min_workers > p.max_workers {
                return Err(format!("pool '{}': min_workers > max_workers", p.name));
            }
            if !(p.reservable_fraction.is_finite() && p.reservable_fraction > 0.0) {
                return Err(format!(
                    "pool '{}': reservable_fraction must be > 0",
                    p.name
                ));
            }
            if p.retry.max_attempts == 0 {
                return Err(format!(
                    "pool '{}': retry.max_attempts must be >= 1",
                    p.name
                ));
            }
            if let Some(a) = &p.autoscale {
                if a.interval.is_zero() {
                    return Err(format!("pool '{}': autoscale interval must be > 0", p.name));
                }
                if !(a.target_utilization > 0.0 && a.target_utilization <= 1.0) {
                    return Err(format!(
                        "pool '{}': target_utilization must be in (0, 1]",
                        p.name
                    ));
                }
                if !(a.ewma_alpha > 0.0 && a.ewma_alpha <= 1.0) {
                    return Err(format!("pool '{}': ewma_alpha must be in (0, 1]", p.name));
                }
                if !(a.max_step_up_ratio.is_finite() && a.max_step_up_ratio >= 0.0) {
                    return Err(format!("pool '{}': max_step_up_ratio must be >= 0", p.name));
                }
                if !(a.cost_per_worker_second.is_finite() && a.cost_per_worker_second >= 0.0) {
                    return Err(format!(
                        "pool '{}': cost_per_worker_second must be >= 0",
                        p.name
                    ));
                }
            }
        }
        let mut names = std::collections::HashSet::new();
        for r in &self.resources {
            if !names.insert(&r.name) {
                return Err(format!("duplicate resource '{}'", r.name));
            }
            match r.kind {
                ResourceKind::Semaphore { permits: 0 } => {
                    return Err(format!("resource '{}': permits must be >= 1", r.name));
                }
                ResourceKind::TokenBucket { rate, burst } => {
                    if rate.is_nan() || rate < 0.0 {
                        return Err(format!("resource '{}': rate must be >= 0", r.name));
                    }
                    if burst == 0 {
                        return Err(format!("resource '{}': burst must be >= 1", r.name));
                    }
                }
                ResourceKind::Semaphore { .. } => {}
            }
        }
        let finite = |v: &[f64]| v.iter().all(|x| x.is_finite());
        if !finite(&self.priority.base)
            || !finite(&self.priority.aging_rate)
            || !finite(&self.priority.age_cap)
        {
            return Err("priority weights must be finite".into());
        }
        let u = &self.urgency;
        if !finite(&[u.max, u.overdue_rate, u.overdue_cap]) {
            return Err("urgency settings must be finite".into());
        }
        if self.metrics_window.is_zero() {
            return Err("metrics_window must be > 0".into());
        }
        Ok(())
    }

    /// A copy with out-of-range numeric settings clamped to safe values, so the engine never
    /// hangs (zero intervals) or panics (non-finite rates) on bad input.
    pub(crate) fn sanitized(mut self) -> Self {
        const MIN_INTERVAL: Duration = Duration::from_millis(1);
        for p in &mut self.pools {
            if p.reservable_fraction.is_nan() || p.reservable_fraction <= 0.0 {
                p.reservable_fraction = 0.5;
            }
            if p.retry.max_attempts == 0 {
                p.retry.max_attempts = 1;
            }
            if let Some(a) = &mut p.autoscale {
                a.interval = a.interval.max(MIN_INTERVAL);
                if !(a.target_utilization > 0.0 && a.target_utilization <= 1.0) {
                    a.target_utilization = 0.75;
                }
                if !(a.ewma_alpha > 0.0 && a.ewma_alpha <= 1.0) {
                    a.ewma_alpha = 0.3;
                }
                if !(a.max_step_up_ratio.is_finite() && a.max_step_up_ratio >= 0.0) {
                    a.max_step_up_ratio = 1.0;
                }
                if !(a.cost_per_worker_second.is_finite() && a.cost_per_worker_second >= 0.0) {
                    a.cost_per_worker_second = 0.0;
                }
            }
        }
        for r in &mut self.resources {
            if let ResourceKind::TokenBucket { rate, .. } = &mut r.kind
                && (rate.is_nan() || *rate < 0.0)
            {
                *rate = 0.0;
            }
        }
        self.metrics_window = self.metrics_window.max(MIN_INTERVAL);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_reports_bad_settings() {
        assert!(CoreConfig::default().validate().is_ok());
        let mut c = CoreConfig::default();
        c.pools[0].autoscale.as_mut().unwrap().interval = Duration::ZERO;
        assert!(c.validate().unwrap_err().contains("interval"));
        let c = CoreConfig {
            resources: vec![ResourceConfig::token_bucket("x", f64::NAN, 1)],
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().contains("rate"));
        let c = CoreConfig {
            pools: vec![PoolConfig::new("a"), PoolConfig::new("a")],
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().contains("duplicate"));
        let c = CoreConfig {
            pools: vec![PoolConfig::new("a").workers(5, 1, 2)],
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
}
