//! Deterministic simulation harness: drives `Core` under virtual time.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use cronoq_core::*;

/// Simulated work: runs for `dur` seconds; the first `fail` attempts fail.
#[derive(Clone, Debug)]
pub struct Job {
    pub dur: f64,
    pub fail: u32,
    pub permanent: bool,
}

impl Job {
    pub fn ok(dur: f64) -> Job {
        Job {
            dur,
            fail: 0,
            permanent: false,
        }
    }
    pub fn flaky(dur: f64, fail: u32) -> Job {
        Job {
            dur,
            fail,
            permanent: false,
        }
    }
    pub fn broken(dur: f64) -> Job {
        Job {
            dur,
            fail: u32::MAX,
            permanent: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Started {
    pub id: TaskId,
    pub lease: LeaseId,
    pub at: Time,
    pub group: Vec<(std::sync::Arc<str>, std::sync::Arc<str>)>,
    pub pool: PoolId,
    pub attempt: u32,
    pub kind: String,
    pub tags: Vec<(String, String)>,
    /// `dur` of the payload the attempt was handed.
    pub dur: f64,
}

pub struct Sim {
    pub core: Core<Job>,
    pub now: Time,
    /// (end, lease) -> dispatch
    running: BTreeMap<(Time, LeaseId), Dispatch<Job>>,
    pub started: Vec<Started>,
    pub events: Vec<StatusEvent>,
    pub scales: Vec<(Time, PoolId, u32)>,
    pub aborts: Vec<(LeaseId, AbortReason)>,
    /// When each attempt that ended did so (completed or aborted).
    pub attempt_end: std::collections::HashMap<LeaseId, Time>,
    pub persisted: Vec<Mutation<Job>>,
    pub space_available: usize,
    /// Apply `ScaleTo` immediately (in-process slot scaler).
    pub auto_capacity: bool,
    /// Never complete dispatched tasks (lets timeouts and leases expire).
    pub hang: bool,
}

pub fn t(s: f64) -> Time {
    Time::from_secs_f64(s)
}

pub fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}

impl Sim {
    pub fn new(cfg: CoreConfig) -> Sim {
        let core = Core::new(cfg);
        core.check_invariants();
        let mut s = Sim {
            core,
            now: Time::ZERO,
            running: BTreeMap::new(),
            started: Vec::new(),
            events: Vec::new(),
            scales: Vec::new(),
            aborts: Vec::new(),
            attempt_end: Default::default(),
            persisted: Vec::new(),
            space_available: 0,
            auto_capacity: true,
            hang: false,
        };
        s.drain();
        s
    }

    pub fn with_pool(pool: PoolConfig) -> Sim {
        Sim::new(CoreConfig {
            pools: vec![pool],
            ..Default::default()
        })
    }

    pub fn submit(&mut self, spec: TaskSpec<Job>) -> TaskId {
        let r = self.core.submit(self.now, spec).expect("submit");
        self.drain();
        r.id
    }

    /// Update a task by id at the current time.
    pub fn update(&mut self, id: TaskId, patch: TaskPatch) -> Result<Updated, UpdateError> {
        let r = self.core.update(self.now, &TaskRef::Id(id), patch);
        self.drain();
        r
    }

    /// Submit with `replace_queued` (and `renew_position` if `renew`).
    pub fn replace(&mut self, spec: TaskSpec<Job>, renew: bool) -> Result<Submitted, SubmitError> {
        let opts = SubmitOptions {
            on_key_conflict: KeyConflict::ReplaceQueued,
            renew_position: renew,
        };
        let r = self.core.submit_with(self.now, spec, opts);
        self.drain();
        r
    }

    pub fn try_submit(&mut self, spec: TaskSpec<Job>) -> Result<Submitted, SubmitError> {
        let r = self.core.submit(self.now, spec);
        self.drain();
        r
    }

    pub fn drain(&mut self) {
        while let Some(a) = self.core.poll_action() {
            match a {
                Action::Dispatch(d) => {
                    self.started.push(Started {
                        id: d.id,
                        lease: d.lease,
                        at: self.now,
                        group: d.group.to_vec(),
                        pool: d.pool,
                        attempt: d.attempt,
                        kind: d.kind.to_string(),
                        dur: d.payload.dur,
                        tags: d
                            .tags
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                    });
                    if !self.hang {
                        let end = self.now + secs(d.payload.dur);
                        self.running.insert((end, d.lease), d);
                    }
                }
                Action::Abort { lease, reason, .. } => {
                    self.running.retain(|(_, l), _| *l != lease);
                    self.attempt_end.entry(lease).or_insert(self.now);
                    self.aborts.push((lease, reason));
                }
                Action::ScaleTo { pool, desired } => {
                    self.scales.push((self.now, pool, desired));
                    if self.auto_capacity {
                        self.core.set_capacity(self.now, pool, desired);
                    }
                }
                Action::Status(e) => self.events.push(e),
                Action::Persist(m) => self.persisted.push(*m),
                Action::SpaceAvailable => self.space_available += 1,
            }
        }
        self.core.check_invariants();
    }

    /// Advance virtual time to `until`, processing completions and timers in order.
    pub fn run_until(&mut self, until: Time) {
        loop {
            let next_done = self.running.keys().next().map(|k| k.0);
            let next_timer = self.core.next_wakeup();
            let next = match (next_done, next_timer) {
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => break,
            };
            if next > until {
                break;
            }
            self.now = self.now.max(next);
            if next_done == Some(next) {
                let (k, d) = self.running.pop_first().unwrap();
                let job = &d.payload;
                let outcome = if d.attempt <= job.fail {
                    if job.permanent {
                        Outcome::permanent("boom")
                    } else {
                        Outcome::retryable("flaky")
                    }
                } else {
                    Outcome::Success
                };
                self.attempt_end.entry(k.1).or_insert(self.now);
                self.core.complete(self.now, k.1, outcome);
            } else {
                self.core.tick(self.now);
            }
            self.drain();
        }
        self.now = self.now.max(until);
        self.core.tick(self.now);
        self.drain();
    }

    /// Run until no work is pending or running (or `limit` seconds pass).
    pub fn run_to_completion(&mut self, limit: f64) {
        let end = self.now + secs(limit);
        while self.now < end {
            let next = self.now + secs(1.0);
            self.run_until(next);
            if self.core.live_tasks() == 0 && self.running.is_empty() {
                break;
            }
        }
    }

    pub fn status(&self, id: TaskId) -> Option<TaskStatus> {
        self.core.status(&TaskRef::Id(id))
    }

    pub fn start_of(&self, id: TaskId) -> Option<Time> {
        self.started.iter().find(|s| s.id == id).map(|s| s.at)
    }

    pub fn order(&self) -> Vec<TaskId> {
        self.started.iter().map(|s| s.id).collect()
    }

    pub fn running_count(&self) -> usize {
        self.running.len()
    }

    /// Worker group of the latest start of `id`.
    pub fn dispatched_group(
        &self,
        id: TaskId,
    ) -> Option<Vec<(std::sync::Arc<str>, std::sync::Arc<str>)>> {
        self.started
            .iter()
            .rev()
            .find(|s| s.id == id)
            .map(|s| s.group.clone())
    }

    /// Resume everything that is halted or paused. Returns how many there were.
    pub fn resume_halted(&mut self) -> usize {
        let mut n = 0;
        for p in 0..self.core.snapshot(self.now).pools.len() {
            let pool = PoolId(p as u16);
            for key in self.core.halted_orderings(pool, usize::MAX) {
                n += self.core.resume_ordering(self.now, pool, &key) as usize;
            }
            // Pauses too: of ordering keys, of classes and of the pool.
            for (key, _) in self.core.paused_orderings(pool, usize::MAX) {
                n += self.core.resume_ordering(self.now, pool, &key) as usize;
            }
            let targets = Priority::ALL
                .into_iter()
                .map(PauseTarget::Class)
                .chain([PauseTarget::Pool]);
            for target in targets {
                n += self.core.resume(self.now, pool, &target).unwrap() as usize;
            }
        }
        self.drain();
        n
    }

    pub fn running_leases(&self) -> Vec<LeaseId> {
        self.running.keys().map(|(_, l)| *l).collect()
    }

    pub fn complete_all_running_now(&mut self, outcome: Outcome) {
        let all: Vec<_> = std::mem::take(&mut self.running).into_iter().collect();
        for ((_, lease), _) in all {
            self.attempt_end.entry(lease).or_insert(self.now);
            self.core.complete(self.now, lease, outcome.clone());
        }
        self.drain();
    }
}
