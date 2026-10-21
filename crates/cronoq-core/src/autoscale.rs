//! Queue-pressure autoscaling policy (SPEC §10).

use std::collections::VecDeque;

use crate::config::AutoscaleConfig;
use crate::spec::Priority;
use crate::stats::Ewma;
use crate::time::Time;

/// Signals sampled at each evaluation.
#[derive(Clone, Debug, Default)]
pub struct Signals {
    /// Tasks that became eligible since the previous evaluation.
    pub arrivals: u64,
    /// Seconds since the previous evaluation.
    pub dt: f64,
    /// Mean service time in seconds.
    pub service_secs: f64,
    /// Waiting tasks per class.
    pub backlog: [u64; Priority::COUNT],
    pub busy: u32,
    pub capacity: u32,
    /// Peak concurrent reservations within the horizon.
    pub reserved: u32,
    pub min: u32,
    pub max: u32,
}

#[derive(Clone, Debug, Default)]
pub struct AutoscaleState {
    pub arrival_rate: Ewma,
    /// Last requested target.
    pub target: Option<u32>,
    pub last_desired: u32,
    history: VecDeque<(Time, u32)>,
    last_down: Option<Time>,
}

impl AutoscaleState {
    /// Unclamped-by-step desired capacity from the signals.
    pub fn desired(&mut self, cfg: &AutoscaleConfig, s: &Signals) -> u32 {
        if s.dt > 0.0 {
            self.arrival_rate
                .update(s.arrivals as f64 / s.dt, cfg.ewma_alpha);
        }
        let lambda = self.arrival_rate.get().unwrap_or(0.0);
        let svc = s.service_secs.max(1e-9);
        let base = (lambda * svc / cfg.target_utilization.clamp(0.01, 1.0)).ceil();
        let backlog: u64 = s.backlog.iter().sum();
        let drain = (backlog as f64 * svc / cfg.drain_time.as_secs_f64().max(1e-3)).ceil();
        let mut slo_need = 0.0f64;
        let mut ahead = 0u64;
        for c in 0..Priority::COUNT {
            ahead += s.backlog[c];
            if let Some(slo) = cfg.slo[c] {
                if s.backlog[c] > 0 {
                    slo_need =
                        slo_need.max((ahead as f64 * svc / slo.as_secs_f64().max(1e-3)).ceil());
                }
            }
        }
        let need = (base + drain).max(slo_need).max(s.busy as f64) + s.reserved as f64;
        let d = (need.min(u32::MAX as f64) as u32).clamp(s.min, s.max.max(s.min));
        self.last_desired = d;
        d
    }

    /// Evaluates the policy; returns a new target when it changes.
    pub fn evaluate(&mut self, cfg: &AutoscaleConfig, s: &Signals, now: Time) -> Option<u32> {
        let desired = self.desired(cfg, s);
        self.history.push_back((now, desired));
        while let Some(&(t, _)) = self.history.front() {
            if now.saturating_sub(t) > cfg.stabilization_window {
                self.history.pop_front();
            } else {
                break;
            }
        }
        let current = self.target.unwrap_or(s.capacity);
        let next = if desired > current {
            let step = ((current as f64 * cfg.max_step_up_ratio).ceil() as u32)
                .max(cfg.max_step_up_abs)
                .max(1);
            desired.min(current.saturating_add(step))
        } else if desired < current {
            let recommended = self
                .history
                .iter()
                .map(|&(_, d)| d)
                .max()
                .unwrap_or(desired);
            let cooled = self
                .last_down
                .is_none_or(|t| now.saturating_sub(t) >= cfg.cooldown);
            if recommended < current && cooled {
                self.last_down = Some(now);
                recommended.max(current.saturating_sub(cfg.max_step_down.max(1)))
            } else {
                current
            }
        } else {
            current
        };
        let next = next.clamp(s.min, s.max.max(s.min));
        if Some(next) != self.target && (self.target.is_some() || next != s.capacity) {
            self.target = Some(next);
            return Some(next);
        }
        self.target = Some(next);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;

    fn sig(backlog: u64, cap: u32) -> Signals {
        Signals {
            arrivals: 0,
            dt: 1.0,
            service_secs: 1.0,
            backlog: [0, 0, backlog, 0],
            busy: cap,
            capacity: cap,
            reserved: 0,
            min: 1,
            max: 10,
        }
    }

    #[test]
    fn scales_up_on_backlog_and_down_after_stabilization() {
        let cfg = AutoscaleConfig {
            stabilization_window: Duration::from_secs(5),
            cooldown: Duration::from_secs(2),
            ..Default::default()
        };
        let mut st = AutoscaleState::default();
        // 300 queued 1s tasks, drain in 30s => 10 workers wanted, step up limited to max(1*1, 4) = +4.
        assert_eq!(
            st.evaluate(&cfg, &sig(300, 1), Time::from_secs_f64(1.0)),
            Some(5)
        );
        assert_eq!(
            st.evaluate(&cfg, &sig(300, 5), Time::from_secs_f64(2.0)),
            Some(10)
        );
        // Idle: stays up during stabilization window.
        let idle = Signals {
            busy: 0,
            ..sig(0, 10)
        };
        assert_eq!(st.evaluate(&cfg, &idle, Time::from_secs_f64(3.0)), None);
        assert_eq!(st.evaluate(&cfg, &idle, Time::from_secs_f64(6.0)), None);
        // After the window the high samples expire; scale down one step at a time with cooldown.
        assert_eq!(st.evaluate(&cfg, &idle, Time::from_secs_f64(8.5)), Some(9));
        assert_eq!(st.evaluate(&cfg, &idle, Time::from_secs_f64(9.0)), None);
        assert_eq!(st.evaluate(&cfg, &idle, Time::from_secs_f64(10.6)), Some(8));
    }

    #[test]
    fn slo_drives_capacity() {
        let mut cfg = AutoscaleConfig::default();
        cfg.slo[0] = Some(Duration::from_secs(1));
        let mut st = AutoscaleState::default();
        let s = Signals {
            backlog: [6, 0, 0, 0],
            busy: 0,
            capacity: 1,
            max: 100,
            ..sig(0, 1)
        };
        // 6 critical tasks, 1s each, must wait <= 1s => 6 workers.
        assert_eq!(st.desired(&cfg, &s), 6);
    }

    #[test]
    fn reservations_add_capacity() {
        let cfg = AutoscaleConfig::default();
        let mut st = AutoscaleState::default();
        let s = Signals {
            reserved: 3,
            busy: 0,
            ..sig(0, 1)
        };
        assert_eq!(st.desired(&cfg, &s), 3);
    }
}
