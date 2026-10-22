//! The sans-IO scheduling engine.
//!
//! All methods take `now`; the engine never reads a clock or performs I/O. After each call the
//! driver drains [`Core::poll_action`] and arranges to call [`Core::tick`] at
//! [`Core::next_wakeup`].

use core::cmp::Reverse;
use core::time::Duration;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::autoscale::{AutoscaleState, Signals};
use crate::config::{CoreConfig, OverflowPolicy, PoolConfig, ResourceKind, UnknownDependency};
use crate::io::*;
use crate::lane::Lane;
use crate::reservations::Reservations;
use crate::rng::Rng;
use crate::snapshot::{PoolSnapshot, Snapshot};
use crate::spec::*;
use crate::stats::{DurStats, P90Stats, Window};
use crate::time::{Time, secs};

// ---------------------------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Key {
    idx: u32,
    generation: u32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Loc {
    None,
    Lane,
    Edf,
    Due,
    Parked(ResourceId),
}

#[derive(Clone, Debug)]
struct FixedInfo {
    at: Time,
    tolerance: Duration,
    on_miss: MissPolicy,
    /// Reservation interval start (for removal).
    res_start: Time,
    held: bool,
    hold_pending: bool,
    missed: bool,
    /// Recurring overlap guard: skip if this task is still live at start.
    skip_if_live: Option<TaskId>,
}

struct Entry<P> {
    id: TaskId,
    seq: u64,
    arrival: Time,
    payload: Arc<P>,
    kind: u32,
    pool: u16,
    class: u8,
    status: TaskStatus,
    loc: Loc,
    key: Option<Arc<str>>,
    deadline: Time,
    lst: Time,
    hint_ns: u64,
    timeout_ns: u64,
    retry: Option<Box<RetryPolicy>>,
    attempt: u32,
    unmet: u32,
    awaiting: u32,
    /// Registered awaits on unknown keys (listed in `Core::awaited_by`, cleaned up at the end).
    has_awaits: bool,
    /// Counted in the pending bounds.
    pending: bool,
    lease: Option<LeaseId>,
    retry_at: Time,
    dependents: Vec<(TaskId, EdgeCond)>,
    resources: Option<Box<[(ResourceId, u32)]>>,
    tags: Option<Tags>,
    fixed: Option<Box<FixedInfo>>,
}

struct Slot<P> {
    generation: u32,
    entry: Option<Entry<P>>,
}

#[derive(Clone, Debug)]
struct Lease {
    key: Key,
    id: TaskId,
    pool: u16,
    started: Time,
    expires: Time,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Timer {
    Retry(TaskId),
    HoldAt(TaskId),
    StartAt(TaskId),
    MissAt(TaskId),
    AwaitTimeout(TaskId),
    AttemptTimeout(LeaseId),
    LeaseExpiry(LeaseId),
    Refill(u16),
    Autoscale(u16),
    Recurring(Arc<str>, Time),
}

struct Resource {
    name: Arc<str>,
    kind: ResourceKind,
    available: f64,
    capacity: f64,
    last_refill: Time,
    waiters: VecDeque<Key>,
    refill_at: Option<Time>,
}

impl Resource {
    fn refill(&mut self, now: Time) {
        if let ResourceKind::TokenBucket { rate, .. } = self.kind {
            let dt = now.diff_secs(self.last_refill);
            if dt > 0.0 && rate > 0.0 {
                self.available = (self.available + rate * dt).min(self.capacity);
            }
        }
        self.last_refill = self.last_refill.max(now);
    }
}

struct Pool {
    cfg: PoolConfig,
    capacity: u32,
    busy: u32,
    held: u32,
    lanes: [Lane<Key>; Priority::COUNT],
    edf: [BTreeMap<(Time, u64), Key>; Priority::COUNT],
    due: BTreeMap<(Time, u64), Key>,
    pending_holds: BTreeMap<(Time, u64), Key>,
    /// Fixed-start tasks currently holding a slot (at most the reservation limit).
    held_tasks: BTreeMap<(Time, u64), Key>,
    reservations: Reservations,
    /// Reservations of fixed-start tasks that have not started yet (autoscaling signal).
    unstarted: Reservations,
    /// Tasks waiting for a retry backoff, per class.
    retrying: [usize; Priority::COUNT],
    pending: usize,
    pending_class: [usize; Priority::COUNT],
    autoscale: Option<AutoscaleState>,
    arrivals: u64,
    last_eval: Time,
    stats: DurStats,
    /// Runtimes of non-fixed work only (drives the hold lead for fixed starts).
    adhoc_stats: P90Stats,
    window: Window,
    last_acct: Time,
    worker_secs: f64,
    cost: f64,
    dirty: bool,
}

impl Pool {
    #[inline]
    fn free(&self) -> u32 {
        self.capacity.saturating_sub(self.busy + self.held)
    }
    fn account(&mut self, now: Time) {
        if self.last_acct == Time::ZERO {
            // First observation: engine time may start anywhere (e.g. Unix nanoseconds).
            self.last_acct = now;
            return;
        }
        let dt = now.diff_secs(self.last_acct);
        if dt <= 0.0 {
            return;
        }
        self.last_acct = now;
        let cap = self.capacity as f64;
        let busy = self.busy as f64;
        let b = self.window.at(now);
        b.busy_secs += busy * dt;
        b.capacity_secs += cap * dt;
        self.worker_secs += cap * dt;
        if let Some(a) = &self.cfg.autoscale {
            self.cost += cap * dt * a.cost_per_worker_second;
        }
    }
    fn ready_len(&self) -> usize {
        self.lanes.iter().map(|l| l.len()).sum::<usize>()
            + self.edf.iter().map(|l| l.len()).sum::<usize>()
            + self.due.len()
    }
}

struct Recurring<P> {
    spec: RecurringSpec<P>,
    cron: croner::Cron,
    tz: chrono_tz::Tz,
    next: Option<Time>,
    last_task: Option<TaskId>,
    /// Latest occurrence already turned into a task.
    materialized: Option<Time>,
}

enum Parent {
    Live(TaskId),
    Done(TaskStatus),
    Await(Arc<str>),
}

/// Engine-wide totals since start.
#[derive(Clone, Debug, Default)]
pub struct Totals {
    /// Tasks accepted.
    pub submitted: u64,
    /// Attempts started.
    pub dispatched: u64,
    /// Tasks that succeeded.
    pub succeeded: u64,
    /// Tasks that failed permanently.
    pub failed: u64,
    /// Tasks that exhausted their retries.
    pub dead_lettered: u64,
    /// Tasks cancelled.
    pub cancelled: u64,
    /// Tasks skipped because a dependency condition could not be met.
    pub skipped: u64,
    /// Fixed-start tasks that missed their tolerance.
    pub missed: u64,
    /// Tasks evicted under overload.
    pub shed: u64,
    /// Retries scheduled.
    pub retries: u64,
    /// Attempts aborted for exceeding their timeout.
    pub timeouts: u64,
    /// Attempts aborted because their lease expired.
    pub lease_expiries: u64,
    /// Recurring occurrences not created because the reservation limit was reached.
    pub recurring_conflicts: u64,
    /// Recovered recurring schedules not armed because the configuration no longer supports
    /// their template (a removed pool or resource).
    pub recurring_invalid: u64,
}

// ---------------------------------------------------------------------------------------------
// Core
// ---------------------------------------------------------------------------------------------

/// The scheduling engine. See the crate docs.
pub struct Core<P> {
    cfg: CoreConfig,
    pools: Vec<Pool>,
    pool_index: FxHashMap<Arc<str>, u16>,
    resources: Vec<Resource>,
    resource_index: FxHashMap<Arc<str>, u16>,
    slots: Vec<Slot<P>>,
    free_slots: Vec<u32>,
    index: FxHashMap<TaskId, Key>,
    keys: FxHashMap<Arc<str>, TaskId>,
    awaits: FxHashMap<Arc<str>, Vec<(TaskId, EdgeCond)>>,
    retained: FxHashMap<TaskId, (TaskStatus, Time)>,
    retained_keys: FxHashMap<Arc<str>, TaskId>,
    retention_q: VecDeque<(Time, TaskId, Option<Arc<str>>)>,
    leases: FxHashMap<LeaseId, Lease>,
    timers: BinaryHeap<Reverse<(Time, u64, Timer)>>,
    timer_seq: u64,
    kinds: Vec<Arc<str>>,
    kind_index: FxHashMap<Arc<str>, u32>,
    dur: FxHashMap<(u16, u32), DurStats>,
    recurring: BTreeMap<Arc<str>, Recurring<P>>,
    actions: VecDeque<Action<P>>,
    rng: Rng,
    next_id: u64,
    next_seq: u64,
    next_lease: u64,
    now: Time,
    dirty: Vec<u16>,
    total_pending: usize,
    pending_class: [usize; Priority::COUNT],
    space_waiters: bool,
    emit_transitions: bool,
    totals: Totals,
    empty_tags: Tags,
    /// Keys of tasks being recovered (only set while `recover` runs).
    recovering_keys: Option<FxHashSet<Arc<str>>>,
    /// Overlap guard and materialization state of removed recurring schedules, so re-adding
    /// one does not duplicate an occurrence that already exists.
    recurring_history: FxHashMap<Arc<str>, (Option<TaskId>, Option<Time>)>,
    /// Whether time-based accounting has been anchored to the first observed `now`.
    started: bool,
    /// Unknown keys each awaiting task registered for, so they can be cleaned up when it ends.
    awaited_by: FxHashMap<TaskId, Box<[Arc<str>]>>,
}

impl<P> Core<P> {
    /// Create an engine. Structural errors (no pools, duplicate names, `min_workers >
    /// max_workers`) panic; use [`CoreConfig::validate`] first to report them gracefully.
    /// Out-of-range numeric settings are clamped to safe values.
    pub fn new(cfg: CoreConfig) -> Self {
        let cfg = cfg.sanitized();
        assert!(!cfg.pools.is_empty(), "at least one pool is required");
        assert!(cfg.pools.len() <= u16::MAX as usize && cfg.resources.len() <= u16::MAX as usize);
        let mut pool_index = FxHashMap::default();
        let mut pools = Vec::new();
        let mut timers = BinaryHeap::new();
        for (i, pc) in cfg.pools.iter().enumerate() {
            assert!(
                pool_index.insert(pc.name.clone(), i as u16).is_none(),
                "duplicate pool name"
            );
            assert!(
                pc.min_workers <= pc.max_workers,
                "min_workers > max_workers"
            );
            let initial = pc.initial_workers.clamp(pc.min_workers, pc.max_workers);
            if let Some(a) = &pc.autoscale {
                timers.push(Reverse((
                    Time::ZERO + a.interval,
                    i as u64,
                    Timer::Autoscale(i as u16),
                )));
            }
            pools.push(Pool {
                cfg: pc.clone(),
                capacity: initial,
                busy: 0,
                held: 0,
                lanes: Default::default(),
                edf: Default::default(),
                due: BTreeMap::new(),
                pending_holds: BTreeMap::new(),
                held_tasks: BTreeMap::new(),
                reservations: Reservations::default(),
                unstarted: Reservations::default(),
                retrying: [0; Priority::COUNT],
                pending: 0,
                pending_class: [0; Priority::COUNT],
                autoscale: pc.autoscale.as_ref().map(|_| AutoscaleState::default()),
                arrivals: 0,
                last_eval: Time::ZERO,
                stats: DurStats::default(),
                adhoc_stats: P90Stats::default(),
                window: Window::new(cfg.metrics_window),
                last_acct: Time::ZERO,
                worker_secs: 0.0,
                cost: 0.0,
                dirty: false,
            });
        }
        let mut resource_index = FxHashMap::default();
        let resources = cfg
            .resources
            .iter()
            .enumerate()
            .map(|(i, rc)| {
                assert!(
                    resource_index.insert(rc.name.clone(), i as u16).is_none(),
                    "duplicate resource name"
                );
                let cap = match rc.kind {
                    ResourceKind::Semaphore { permits } => permits as f64,
                    ResourceKind::TokenBucket { burst, .. } => burst as f64,
                };
                Resource {
                    name: rc.name.clone(),
                    kind: rc.kind.clone(),
                    available: cap,
                    capacity: cap,
                    last_refill: Time::ZERO,
                    waiters: VecDeque::new(),
                    refill_at: None,
                }
            })
            .collect();
        let timer_seq = pools.len() as u64;
        Core {
            rng: Rng::new(cfg.seed),
            next_id: (cfg.id_epoch as u64) << 48 | 1,
            cfg,
            pools,
            pool_index,
            resources,
            resource_index,
            slots: Vec::new(),
            free_slots: Vec::new(),
            index: FxHashMap::default(),
            keys: FxHashMap::default(),
            awaits: FxHashMap::default(),
            retained: FxHashMap::default(),
            retained_keys: FxHashMap::default(),
            retention_q: VecDeque::new(),
            leases: FxHashMap::default(),
            timers,
            timer_seq,
            kinds: Vec::new(),
            kind_index: FxHashMap::default(),
            dur: FxHashMap::default(),
            recurring: BTreeMap::new(),
            actions: VecDeque::new(),
            next_seq: 0,
            next_lease: 1,
            now: Time::ZERO,
            dirty: Vec::new(),
            total_pending: 0,
            pending_class: [0; Priority::COUNT],
            space_waiters: false,
            emit_transitions: false,
            totals: Totals::default(),
            empty_tags: Arc::from(Vec::new()),
            recovering_keys: None,
            recurring_history: FxHashMap::default(),
            started: false,
            awaited_by: FxHashMap::default(),
        }
    }

    /// Emit `Action::Status` for every transition, not only terminal ones.
    pub fn set_emit_transitions(&mut self, on: bool) {
        self.emit_transitions = on;
    }

    /// The (sanitized) configuration the engine runs with.
    pub fn config(&self) -> &CoreConfig {
        &self.cfg
    }

    /// Id of the pool named `name`.
    pub fn pool_id(&self, name: &str) -> Option<PoolId> {
        self.pool_index.get(name).map(|&i| PoolId(i))
    }

    /// Name of `pool`. Panics if `pool` is not from this engine.
    pub fn pool_name(&self, pool: PoolId) -> &Arc<str> {
        &self.pools[pool.0 as usize].cfg.name
    }

    /// Current worker capacity of `pool`. Panics if `pool` is not from this engine.
    pub fn pool_capacity(&self, pool: PoolId) -> u32 {
        self.pools[pool.0 as usize].capacity
    }

    /// Counters since start.
    pub fn totals(&self) -> &Totals {
        &self.totals
    }

    /// Next action for the driver.
    #[inline]
    pub fn poll_action(&mut self) -> Option<Action<P>> {
        self.actions.pop_front()
    }

    /// Move all pending actions into `out`.
    pub fn drain_actions(&mut self, out: &mut Vec<Action<P>>) {
        out.extend(self.actions.drain(..));
    }

    /// Whether actions are waiting to be polled.
    pub fn has_actions(&self) -> bool {
        !self.actions.is_empty()
    }

    /// Earliest time at which `tick` has work to do.
    pub fn next_wakeup(&self) -> Option<Time> {
        self.timers.peek().map(|Reverse((t, _, _))| *t)
    }

    /// Advance time: fire due timers and dispatch.
    pub fn tick(&mut self, now: Time) {
        self.advance(now);
        self.pump();
    }

    // -----------------------------------------------------------------------------------------
    // Submission
    // -----------------------------------------------------------------------------------------

    /// Submit one task.
    pub fn submit(&mut self, now: Time, spec: TaskSpec<P>) -> Result<Submitted, SubmitError> {
        self.try_submit(now, spec).map_err(|(e, _)| e)
    }

    /// Like [`Core::submit`], but hands the spec back on rejection (e.g. to retry after
    /// `SubmitError::Full`).
    #[allow(clippy::result_large_err)] // returning the spec by value avoids an allocation
    pub fn try_submit(
        &mut self,
        now: Time,
        spec: TaskSpec<P>,
    ) -> Result<Submitted, (SubmitError, TaskSpec<P>)> {
        self.advance(now);
        if let Some(id) = self.existing_key(spec.key.as_deref()) {
            return Ok(Submitted {
                id,
                duplicate: true,
            });
        }
        let pool = match self.validate(&spec, &FxHashSet::default()) {
            Ok(p) => p,
            Err(e) => return Err((e, spec)),
        };
        // Reservations are checked before bounds, so a submission that will be rejected anyway
        // never sheds other work first.
        if let Some(f) = &spec.fixed_start {
            let (s, e) = self.reservation_interval(pool, &spec, f);
            if self.pools[pool as usize].reservations.max_overlap(s, e)
                >= self.pools[pool as usize].cfg.reservation_limit()
            {
                return Err((SubmitError::ReservationConflict, spec));
            }
        }
        if let Err(e) = self.check_bounds(pool, spec.priority, 1, true) {
            return Err((e, spec));
        }
        let fixed = spec.fixed_start.is_some();
        let id = self.alloc_id();
        let seq = self.alloc_seq();
        self.insert(now, id, seq, now, spec, pool, 0, None);
        if fixed {
            // A fixed start already past its window applies its miss policy before dispatch.
            self.advance(now);
        }
        self.pump();
        Ok(Submitted {
            id,
            duplicate: false,
        })
    }

    /// Submit several tasks atomically. Dependencies may reference keys defined in the batch
    /// (in any order). Either all are admitted or none.
    pub fn submit_batch(
        &mut self,
        now: Time,
        specs: Vec<TaskSpec<P>>,
    ) -> Result<Vec<Submitted>, (usize, SubmitError)> {
        self.advance(now);
        // Keys defined in the batch.
        let mut batch_keys: FxHashMap<Arc<str>, usize> = FxHashMap::default();
        for (i, s) in specs.iter().enumerate() {
            if let Some(k) = &s.key
                && self.existing_key(Some(k)).is_none()
                && batch_keys.insert(k.clone(), i).is_some()
            {
                return Err((i, SubmitError::DuplicateKeyInBatch(k.clone())));
            }
        }
        let key_set: FxHashSet<Arc<str>> = batch_keys.keys().cloned().collect();
        let mut pools = Vec::with_capacity(specs.len());
        for (i, s) in specs.iter().enumerate() {
            if self.existing_key(s.key.as_deref()).is_some() {
                pools.push(u16::MAX);
                continue;
            }
            pools.push(self.validate(s, &key_set).map_err(|e| (i, e))?);
        }
        // Topological order over in-batch key dependencies (Kahn).
        let n = specs.len();
        let mut indeg = vec![0u32; n];
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, s) in specs.iter().enumerate() {
            if pools[i] == u16::MAX {
                continue;
            }
            for d in &s.deps {
                if let TaskRef::Key(k) = &d.target
                    && let Some(&j) = batch_keys.get(k)
                {
                    if self.keys.contains_key(k) || self.retained_keys.contains_key(k) {
                        continue;
                    }
                    indeg[i] += 1;
                    children[j].push(i);
                }
            }
        }
        let mut order: Vec<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();
        let mut h = 0;
        while h < order.len() {
            let j = order[h];
            h += 1;
            for &c in &children[j] {
                indeg[c] -= 1;
                if indeg[c] == 0 {
                    order.push(c);
                }
            }
        }
        if order.len() != n {
            let bad = (0..n).find(|&i| indeg[i] > 0).unwrap_or(0);
            return Err((bad, SubmitError::Cycle));
        }
        // Bounds per pool/class.
        let mut counts: FxHashMap<(u16, Priority), usize> = FxHashMap::default();
        for (i, s) in specs.iter().enumerate() {
            if pools[i] != u16::MAX {
                *counts.entry((pools[i], s.priority)).or_default() += 1;
            }
        }
        if let Some(i) = self.batch_cycle(&specs, &pools, &batch_keys) {
            return Err((i, SubmitError::Cycle));
        }
        self.check_batch_bounds(&counts).map_err(|e| (0, e))?;
        // Reservations: tentative insertion so batch members see each other.
        let mut tentative = Vec::new();
        let mut conflict = None;
        for (i, s) in specs.iter().enumerate() {
            if pools[i] == u16::MAX {
                continue;
            }
            if let Some(f) = &s.fixed_start {
                let (st, en) = self.reservation_interval(pools[i], s, f);
                let pool = &mut self.pools[pools[i] as usize];
                if pool.reservations.max_overlap(st, en) >= pool.cfg.reservation_limit() {
                    conflict = Some(i);
                    break;
                }
                let tid = TaskId(u64::MAX - i as u64);
                pool.reservations.insert(tid, st, en);
                tentative.push((pools[i], tid, st));
            }
        }
        for (p, tid, st) in tentative {
            self.pools[p as usize].reservations.remove(tid, st);
        }
        if let Some(i) = conflict {
            return Err((i, SubmitError::ReservationConflict));
        }
        // Commit in topological order.
        let mut out = vec![
            Submitted {
                id: TaskId(0),
                duplicate: false
            };
            n
        ];
        let mut specs: Vec<Option<TaskSpec<P>>> = specs.into_iter().map(Some).collect();
        for i in order {
            let spec = specs[i].take().expect("each index once");
            if pools[i] == u16::MAX {
                let id = self.existing_key(spec.key.as_deref()).expect("checked");
                out[i] = Submitted {
                    id,
                    duplicate: true,
                };
                continue;
            }
            let id = self.alloc_id();
            let seq = self.alloc_seq();
            self.insert(now, id, seq, now, spec, pools[i], 0, None);
            out[i] = Submitted {
                id,
                duplicate: false,
            };
        }
        self.advance(now);
        self.pump();
        Ok(out)
    }

    /// Find a dependency cycle through batch members, including cycles closed by existing
    /// tasks that await a key the batch defines. Returns a batch index on the cycle.
    fn batch_cycle(
        &self,
        specs: &[TaskSpec<P>],
        pools: &[u16],
        batch_keys: &FxHashMap<Arc<str>, usize>,
    ) -> Option<usize> {
        #[derive(Copy, Clone, PartialEq, Eq, Hash)]
        enum N {
            B(usize),
            L(TaskId),
        }
        let n = specs.len();
        let mut batch_children: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut live_children: FxHashMap<TaskId, Vec<usize>> = FxHashMap::default();
        for (i, s) in specs.iter().enumerate() {
            if pools[i] == u16::MAX {
                continue;
            }
            for d in &s.deps {
                match &d.target {
                    TaskRef::Key(k) => {
                        if let Some(&id) = self.keys.get(k) {
                            live_children.entry(id).or_default().push(i);
                        } else if let Some(&j) = batch_keys.get(k) {
                            batch_children[j].push(i);
                        }
                    }
                    TaskRef::Id(id) => {
                        if self.index.contains_key(id) {
                            live_children.entry(*id).or_default().push(i);
                        }
                    }
                }
            }
        }
        let succ = |node: N| -> Vec<N> {
            let mut out = Vec::new();
            match node {
                N::B(j) => {
                    out.extend(batch_children[j].iter().map(|&i| N::B(i)));
                    if let Some(k) = &specs[j].key
                        && let Some(w) = self.awaits.get(k)
                    {
                        out.extend(
                            w.iter()
                                .filter(|(id, _)| self.index.contains_key(id))
                                .map(|(id, _)| N::L(*id)),
                        );
                    }
                }
                N::L(id) => {
                    if let Some(e) = self.entry_by_id(id) {
                        out.extend(e.dependents.iter().map(|d| N::L(d.0)));
                        if let Some(k) = &e.key
                            && let Some(w) = self.awaits.get(k)
                        {
                            out.extend(w.iter().map(|(id, _)| N::L(*id)));
                        }
                    }
                    if let Some(c) = live_children.get(&id) {
                        out.extend(c.iter().map(|&i| N::B(i)));
                    }
                }
            }
            out
        };
        // Iterative three-colour DFS.
        let mut color: FxHashMap<N, u8> = FxHashMap::default(); // 1 = on stack, 2 = done
        for start in (0..n).filter(|&i| pools[i] != u16::MAX) {
            if color.contains_key(&N::B(start)) {
                continue;
            }
            let mut stack: Vec<(N, Vec<N>)> = vec![(N::B(start), succ(N::B(start)))];
            color.insert(N::B(start), 1);
            while let Some((node, children)) = stack.last_mut() {
                if let Some(next) = children.pop() {
                    match color.get(&next) {
                        Some(1) => {
                            return Some(match next {
                                N::B(i) => i,
                                N::L(_) => match *node {
                                    N::B(i) => i,
                                    N::L(_) => start,
                                },
                            });
                        }
                        Some(_) => {}
                        None => {
                            color.insert(next, 1);
                            let c = succ(next);
                            stack.push((next, c));
                        }
                    }
                } else {
                    color.insert(*node, 2);
                    stack.pop();
                }
            }
        }
        None
    }

    fn existing_key(&self, key: Option<&str>) -> Option<TaskId> {
        let k = key?;
        self.keys
            .get(k)
            .or_else(|| self.retained_keys.get(k))
            .copied()
    }

    fn alloc_id(&mut self) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        id
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Static validation; returns the pool index.
    fn validate(
        &self,
        spec: &TaskSpec<P>,
        batch_keys: &FxHashSet<Arc<str>>,
    ) -> Result<u16, SubmitError> {
        let pool = self.check_config(spec)?;
        self.check_deps(spec, pool, batch_keys)?;
        Ok(pool)
    }

    /// The checks that depend only on the configuration (pool, resources, retry policy);
    /// returns the pool index. `insert` relies on them, so recurring templates pass them too.
    fn check_config(&self, spec: &TaskSpec<P>) -> Result<u16, SubmitError> {
        let pool = *self
            .pool_index
            .get(&spec.pool)
            .ok_or_else(|| SubmitError::UnknownPool(spec.pool.clone()))?;
        for (name, amount) in &spec.resources {
            let r = *self
                .resource_index
                .get(name)
                .ok_or_else(|| SubmitError::UnknownResource(name.clone()))?;
            if *amount as f64 > self.resources[r as usize].capacity {
                return Err(SubmitError::ResourceExceedsCapacity(name.clone()));
            }
            if *amount == 0 {
                return Err(SubmitError::Invalid("resource amount must be > 0"));
            }
        }
        if let Some(r) = &spec.retry
            && r.max_attempts == 0
        {
            return Err(SubmitError::Invalid("retry.max_attempts must be >= 1"));
        }
        Ok(pool)
    }

    fn check_deps(
        &self,
        spec: &TaskSpec<P>,
        pool: u16,
        batch_keys: &FxHashSet<Arc<str>>,
    ) -> Result<(), SubmitError> {
        let await_ok = matches!(
            self.pools[pool as usize].cfg.unknown_dependency,
            UnknownDependency::Await { .. }
        );
        let mut live_parents: FxHashSet<TaskId> = FxHashSet::default();
        for d in &spec.deps {
            match &d.target {
                TaskRef::Id(id) => {
                    if self.index.contains_key(id) {
                        live_parents.insert(*id);
                    } else if !self.retained.contains_key(id) {
                        return Err(SubmitError::UnknownDependency(id.to_string().into()));
                    }
                }
                TaskRef::Key(k) => {
                    if spec.key.as_ref() == Some(k) {
                        return Err(SubmitError::Cycle);
                    }
                    if let Some(id) = self.keys.get(k) {
                        live_parents.insert(*id);
                    } else if !self.retained_keys.contains_key(k)
                        && !batch_keys.contains(k)
                        && !await_ok
                    {
                        return Err(SubmitError::UnknownDependency(k.clone()));
                    }
                }
            }
        }
        // A cycle needs this task's key to be awaited by a descendant of one of its parents.
        if let Some(k) = &spec.key
            && let Some(waiting) = self.awaits.get(k)
            && !live_parents.is_empty()
            && self.reaches_any(waiting.iter().map(|w| w.0), &live_parents)
        {
            return Err(SubmitError::Cycle);
        }
        Ok(())
    }

    fn reaches_any(
        &self,
        start: impl Iterator<Item = TaskId>,
        targets: &FxHashSet<TaskId>,
    ) -> bool {
        let mut stack: Vec<TaskId> = start.collect();
        let mut seen: FxHashSet<TaskId> = FxHashSet::default();
        while let Some(id) = stack.pop() {
            if targets.contains(&id) {
                return true;
            }
            if !seen.insert(id) {
                continue;
            }
            if let Some(e) = self.entry_by_id(id) {
                stack.extend(e.dependents.iter().map(|d| d.0));
                if let Some(k) = &e.key
                    && let Some(w) = self.awaits.get(k)
                {
                    stack.extend(w.iter().map(|d| d.0));
                }
            }
        }
        false
    }

    fn check_bounds(
        &mut self,
        pool: u16,
        class: Priority,
        n: usize,
        allow_shed: bool,
    ) -> Result<(), SubmitError> {
        let c = class.index();
        loop {
            let p = &self.pools[pool as usize];
            let over = |cur: usize, lim: Option<usize>| lim.is_some_and(|l| cur + n > l);
            // Bounds on the incoming task's own class can only be relieved by evicting tasks of
            // that same class; totals can be relieved by evicting any task of this pool.
            let class_full = over(
                self.pending_class[c],
                self.cfg.global_bounds.max_pending_per_class[c],
            ) || over(p.pending_class[c], p.cfg.bounds.max_pending_per_class[c]);
            let total_full = over(self.total_pending, self.cfg.global_bounds.max_pending)
                || over(p.pending, p.cfg.bounds.max_pending);
            if !class_full && !total_full {
                return Ok(());
            }
            let victim = match (p.cfg.overflow, allow_shed) {
                (OverflowPolicy::Wait, _) => {
                    self.space_waiters = true;
                    return Err(SubmitError::Full);
                }
                (OverflowPolicy::Reject, _) | (_, false) => return Err(SubmitError::Overloaded),
                // Victims are strictly lower classes, so they never relieve a class bound.
                (OverflowPolicy::ShedLowest, true) if class_full => None,
                (OverflowPolicy::ShedLowest, true) => (c + 1..Priority::COUNT)
                    .rev()
                    .find_map(|vc| self.shed_victim(pool, vc, true)),
                (OverflowPolicy::ShedOldest, true) => {
                    let low = Priority::Low.index();
                    if class_full && c != low {
                        None
                    } else {
                        self.shed_victim(pool, low, false)
                    }
                }
            };
            match victim {
                Some(k) => self.finish(k, TaskStatus::Shed, Some("shed under overload".into())),
                None => return Err(SubmitError::Overloaded),
            }
        }
    }

    /// The newest (or oldest) waiting task of `class` in `pool` that may be shed: ready tasks
    /// in lanes and deadline queues, and tasks parked on resources. Fixed-start tasks are never
    /// shed: they hold capacity reservations that were promised at admission.
    fn shed_victim(&self, pool: u16, class: usize, newest: bool) -> Option<Key> {
        let p = &self.pools[pool as usize];
        let seq = |k: &Key| self.entry(*k).seq;
        let lane = if newest {
            p.lanes[class].last()
        } else {
            p.lanes[class].first()
        };
        let edf = p.edf[class].values();
        let edf = if newest {
            edf.max_by_key(|k| seq(k))
        } else {
            edf.min_by_key(|k| seq(k))
        };
        let parked = self
            .resources
            .iter()
            .flat_map(|r| r.waiters.iter())
            .filter(|k| {
                self.slot_entry(**k).is_some_and(|e| {
                    matches!(e.loc, Loc::Parked(_)) && e.pool == pool && e.class as usize == class
                })
            });
        let parked = if newest {
            parked.max_by_key(|k| seq(k))
        } else {
            parked.min_by_key(|k| seq(k))
        };
        let candidates = [lane, edf.copied(), parked.copied()].into_iter().flatten();
        if newest {
            candidates.max_by_key(|k| seq(k))
        } else {
            candidates.min_by_key(|k| seq(k))
        }
    }

    /// Bounds for a whole batch: per pool and class, then summed globally.
    fn check_batch_bounds(
        &mut self,
        counts: &FxHashMap<(u16, Priority), usize>,
    ) -> Result<(), SubmitError> {
        for (&(p, c), &k) in counts {
            self.check_bounds(p, c, k, false)?;
        }
        let over = |cur: usize, n: usize, lim: Option<usize>| lim.is_some_and(|l| cur + n > l);
        let total: usize = counts.values().sum();
        let mut full = over(
            self.total_pending,
            total,
            self.cfg.global_bounds.max_pending,
        );
        let mut per_pool: FxHashMap<u16, usize> = FxHashMap::default();
        for c in Priority::ALL {
            let n: usize = counts
                .iter()
                .filter(|((_, pc), _)| *pc == c)
                .map(|(_, k)| k)
                .sum();
            full |= over(
                self.pending_class[c.index()],
                n,
                self.cfg.global_bounds.max_pending_per_class[c.index()],
            );
        }
        for (&(p, _), &k) in counts {
            *per_pool.entry(p).or_default() += k;
        }
        for (&p, &k) in &per_pool {
            let pool = &self.pools[p as usize];
            full |= over(pool.pending, k, pool.cfg.bounds.max_pending);
        }
        if !full {
            return Ok(());
        }
        let wait = per_pool
            .keys()
            .any(|p| self.pools[*p as usize].cfg.overflow == OverflowPolicy::Wait);
        if wait {
            self.space_waiters = true;
            Err(SubmitError::Full)
        } else {
            Err(SubmitError::Overloaded)
        }
    }

    fn reservation_interval(&self, pool: u16, spec: &TaskSpec<P>, f: &FixedStart) -> (Time, Time) {
        let kind = self.kind_index.get(&spec.kind).copied();
        let d = self.estimate_raw(pool, kind, spec.duration_hint.map(nanos).unwrap_or(0));
        (f.at - f.tolerance, f.at + d + f.tolerance)
    }

    fn intern_kind(&mut self, k: &Arc<str>) -> u32 {
        if let Some(&i) = self.kind_index.get(k) {
            return i;
        }
        let i = self.kinds.len() as u32;
        self.kinds.push(k.clone());
        self.kind_index.insert(k.clone(), i);
        i
    }

    fn estimate_raw(&self, pool: u16, kind: Option<u32>, hint_ns: u64) -> Duration {
        if hint_ns > 0 {
            return Duration::from_nanos(hint_ns);
        }
        kind.and_then(|k| self.dur.get(&(pool, k)))
            .and_then(|s| s.estimate())
            .or_else(|| self.pools[pool as usize].stats.estimate())
            .unwrap_or(self.pools[pool as usize].cfg.default_duration)
    }

    /// How long before a fixed start a worker is held: long enough for the ad-hoc work that
    /// might otherwise take the slot (p90 of non-fixed runtimes) to finish.
    fn hold_lead(&self, pool: u16, tolerance: Duration) -> Duration {
        let p = &self.pools[pool as usize];
        let p90 = p.adhoc_stats.p90().unwrap_or(p.cfg.default_duration);
        tolerance.max(p90).min(p.cfg.max_hold)
    }

    /// Insert a validated task. Infallible.
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &mut self,
        now: Time,
        id: TaskId,
        seq: u64,
        arrival: Time,
        spec: TaskSpec<P>,
        pool: u16,
        attempt: u32,
        skip_if_live: Option<TaskId>,
    ) {
        self.totals.submitted += 1;
        if self.cfg.persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::Submitted {
                    id,
                    seq,
                    arrival,
                    spec: spec.clone(),
                })));
        }
        let kind = self.intern_kind(&spec.kind);
        let hint_ns = spec.duration_hint.map(nanos).unwrap_or(0);
        let resources = if spec.resources.is_empty() {
            None
        } else {
            Some(
                spec.resources
                    .iter()
                    .map(|(n, a)| (ResourceId(self.resource_index[n]), *a))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        };
        let fixed = spec.fixed_start.map(|f| {
            let (s, e) = self.reservation_interval(pool, &spec, &f);
            let p = &mut self.pools[pool as usize];
            p.reservations.insert(id, s, e);
            if attempt == 0 {
                p.unstarted.insert(id, s, e);
            }
            Box::new(FixedInfo {
                at: f.at,
                tolerance: f.tolerance,
                on_miss: f.on_miss,
                res_start: s,
                held: false,
                hold_pending: false,
                missed: false,
                skip_if_live,
            })
        });
        // Resolve parents.
        let await_timeout = match self.pools[pool as usize].cfg.unknown_dependency {
            UnknownDependency::Await { timeout } => timeout,
            UnknownDependency::Reject => None,
        };
        let mut parents = Vec::with_capacity(spec.deps.len());
        for d in spec.deps.iter() {
            let recovering = self.recovering_keys.is_some();
            let p = match &d.target {
                TaskRef::Id(pid) => match self.index.get(pid) {
                    Some(_) => Parent::Live(*pid),
                    None => match self.retained.get(pid) {
                        Some(r) => Parent::Done(r.0),
                        // Recovering a live task whose parent left retention: the parent was
                        // terminal and the child is still live, so its edge was satisfied.
                        None if recovering => continue,
                        // Unreachable for validated submissions.
                        None => Parent::Done(TaskStatus::Succeeded),
                    },
                },
                TaskRef::Key(k) => {
                    if let Some(&pid) = self.keys.get(k) {
                        Parent::Live(pid)
                    } else if let Some(r) = self
                        .retained_keys
                        .get(k)
                        .and_then(|pid| self.retained.get(pid))
                    {
                        Parent::Done(r.0)
                    } else if self
                        .recovering_keys
                        .as_ref()
                        .is_some_and(|ks| !ks.contains(k))
                    {
                        // As above: a key that is neither live, retained, nor being recovered
                        // belonged to a parent that finished long ago.
                        continue;
                    } else {
                        Parent::Await(k.clone())
                    }
                }
            };
            parents.push((p, d.cond));
        }
        let entry = Entry {
            id,
            seq,
            arrival,
            payload: spec.payload,
            kind,
            pool,
            class: spec.priority as u8,
            status: TaskStatus::Blocked,
            loc: Loc::None,
            key: spec.key.clone(),
            deadline: spec.deadline.unwrap_or(Time::MAX),
            lst: Time::MAX,
            hint_ns,
            timeout_ns: spec.timeout.map(nanos).unwrap_or(0),
            retry: spec.retry.map(Box::new),
            attempt,
            unmet: 0,
            awaiting: 0,
            has_awaits: false,
            pending: true,
            lease: None,
            retry_at: Time::ZERO,
            dependents: Vec::new(),
            resources,
            tags: if spec.tags.is_empty() {
                None
            } else {
                Some(spec.tags.into())
            },
            fixed,
        };
        let key = self.slab_insert(entry);
        self.index.insert(id, key);
        if let Some(k) = &spec.key {
            self.keys.insert(k.clone(), id);
        }
        {
            let p = &mut self.pools[pool as usize];
            p.pending += 1;
            p.pending_class[spec.priority.index()] += 1;
        }
        self.total_pending += 1;
        self.pending_class[spec.priority.index()] += 1;

        // Wire dependency edges.
        let mut unsatisfied: Option<TaskStatus> = None;
        let (mut unmet, mut awaiting) = (0u32, 0u32);
        let mut awaited: Option<Vec<Arc<str>>> = None;
        for (p, cond) in parents {
            match p {
                Parent::Live(pid) => {
                    let pk = self.index[&pid];
                    self.entry_mut(pk).dependents.push((id, cond));
                    unmet += 1;
                }
                Parent::Done(st) => {
                    if !edge_satisfied(cond, st) {
                        unsatisfied = Some(st);
                    }
                }
                Parent::Await(k) => {
                    self.awaits.entry(k.clone()).or_default().push((id, cond));
                    awaited.get_or_insert_with(Vec::new).push(k);
                    unmet += 1;
                    awaiting += 1;
                }
            }
        }
        // Tasks awaiting this key become our dependents.
        if let Some(k) = &spec.key
            && let Some(waiting) = self.awaits.remove(k)
        {
            for (cid, _) in &waiting {
                if let Some(ck) = self.index.get(cid).copied() {
                    let c = self.entry_mut(ck);
                    c.awaiting = c.awaiting.saturating_sub(1);
                }
            }
            self.entry_mut(key).dependents.extend(waiting);
        }
        {
            let e = self.entry_mut(key);
            e.unmet = unmet;
            e.awaiting = awaiting;
            if let Some(keys) = awaited {
                e.has_awaits = true;
                self.awaited_by.insert(id, keys.into_boxed_slice());
            }
        }
        if awaiting > 0
            && let Some(t) = await_timeout
        {
            self.schedule(now + t, Timer::AwaitTimeout(id));
        }
        if let Some(f) = spec.fixed_start {
            // The hold lead is re-evaluated when this timer fires (see `Timer::HoldAt`), so it
            // tracks duration statistics learned after submission.
            let max_hold = self.pools[pool as usize].cfg.max_hold;
            self.schedule((f.at - max_hold).max(now), Timer::HoldAt(id));
            self.schedule(f.at, Timer::StartAt(id));
            // Missed only when strictly past `at + tolerance`.
            self.schedule(
                f.at + f.tolerance + Duration::from_nanos(1),
                Timer::MissAt(id),
            );
        }
        if let Some(st) = unsatisfied {
            self.finish(
                key,
                TaskStatus::Skipped,
                Some(format!("dependency ended as {st}").into()),
            );
            return;
        }
        if attempt > 0 && unmet == 0 {
            // Recovered task whose last attempt was lost: it counts against the retry budget.
            let max = {
                let e = self.entry(key);
                e.retry
                    .as_deref()
                    .copied()
                    .unwrap_or(self.pools[pool as usize].cfg.retry)
                    .max_attempts
            };
            if attempt >= max {
                self.finish(
                    key,
                    TaskStatus::DeadLettered,
                    Some("attempt lost in failover; retries exhausted".into()),
                );
                return;
            }
            if self.entry(key).fixed.is_none() {
                self.schedule_retry(now, key, None);
                return;
            }
            // Fixed-start tasks fall through: they wait for their slot (and miss policy) again.
        }
        self.emit(key, TaskStatus::Blocked, None);
        if unmet == 0 {
            self.make_eligible(key);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Execution feedback
    // -----------------------------------------------------------------------------------------

    /// Report the outcome of an attempt. Returns `false` for unknown or stale leases.
    pub fn complete(&mut self, now: Time, lease: LeaseId, outcome: Outcome) -> bool {
        self.advance(now);
        let Some(l) = self.leases.remove(&lease) else {
            return false;
        };
        self.end_attempt(&l);
        let ran = now.saturating_sub(l.started);
        match outcome {
            Outcome::Success => {
                let (pool, kind, fixed) = {
                    let e = self.entry(l.key);
                    (e.pool, e.kind, e.fixed.is_some())
                };
                let s = ran.as_secs_f64();
                self.dur.entry((pool, kind)).or_default().observe(s);
                let p = &mut self.pools[pool as usize];
                p.stats.observe(s);
                if !fixed {
                    p.adhoc_stats.observe(s);
                }
                let b = p.window.at(now);
                b.succeeded += 1;
                b.work_secs += s;
                b.run.record(ran);
                self.finish(l.key, TaskStatus::Succeeded, None);
            }
            Outcome::Cancelled => self.finish(l.key, TaskStatus::Cancelled, None),
            Outcome::Failure {
                retryable,
                retry_after,
                message,
            } => {
                self.fail_attempt(now, l.key, retryable, retry_after, message);
            }
        }
        self.pump();
        true
    }

    /// Extend a lease (remote workers). Returns `false` if the lease is unknown.
    pub fn heartbeat(&mut self, now: Time, lease: LeaseId) -> bool {
        self.advance(now);
        let known = match self.leases.get_mut(&lease) {
            Some(l) => {
                if let Some(ttl) = self.pools[l.pool as usize].cfg.lease_ttl {
                    l.expires = now + ttl;
                }
                true
            }
            None => false,
        };
        // Timers fired by `advance` may have made work ready.
        self.pump();
        known
    }

    fn end_attempt(&mut self, l: &Lease) {
        let now = self.now;
        let pool = &mut self.pools[l.pool as usize];
        pool.account(now);
        pool.busy -= 1;
        pool.dirty = true;
        self.dirty.push(l.pool);
        let e = self.entry_mut(l.key);
        e.lease = None;
        let res = e.resources.clone();
        if let Some(res) = res {
            for (r, amount) in res.iter() {
                let rs = &mut self.resources[r.0 as usize];
                if let ResourceKind::Semaphore { .. } = rs.kind {
                    rs.available += *amount as f64;
                    self.wake_waiters(r.0);
                }
            }
            self.dirty_due_pools();
        }
    }

    fn dirty_due_pools(&mut self) {
        for i in 0..self.pools.len() {
            if !self.pools[i].due.is_empty() {
                self.mark_dirty(i as u16);
            }
        }
    }

    fn fail_attempt(
        &mut self,
        now: Time,
        key: Key,
        retryable: bool,
        retry_after: Option<Duration>,
        message: Arc<str>,
    ) {
        let (attempt, max) = {
            let e = self.entry(key);
            let policy = e
                .retry
                .as_deref()
                .copied()
                .unwrap_or(self.pools[e.pool as usize].cfg.retry);
            (e.attempt, policy.max_attempts)
        };
        let pool = self.entry(key).pool;
        self.pools[pool as usize].window.at(now).failed += 1;
        if retryable && attempt < max {
            self.schedule_retry(now, key, retry_after);
            let e = self.entry(key);
            let (id, pool, k, at) = (e.id, e.pool, e.key.clone(), e.attempt);
            self.push_status(id, pool, k, TaskStatus::Retrying, at, Some(message), true);
        } else if retryable {
            self.finish(key, TaskStatus::DeadLettered, Some(message));
        } else {
            self.finish(key, TaskStatus::Failed, Some(message));
        }
    }

    fn schedule_retry(&mut self, now: Time, key: Key, retry_after: Option<Duration>) {
        self.totals.retries += 1;
        let (pool, attempt, policy) = {
            let e = self.entry(key);
            (
                e.pool,
                e.attempt,
                e.retry
                    .as_deref()
                    .copied()
                    .unwrap_or(self.pools[e.pool as usize].cfg.retry),
            )
        };
        let mut delay = policy.backoff(attempt + 1);
        if policy.jitter {
            delay = delay.mul_f64(self.rng.next_f64());
        }
        if let Some(ra) = retry_after {
            delay = delay.max(ra);
        }
        let at = now + delay;
        let (id, class) = {
            let e = self.entry_mut(key);
            e.status = TaskStatus::Retrying;
            e.retry_at = at;
            (e.id, e.class as usize)
        };
        self.pools[pool as usize].retrying[class] += 1;
        self.pools[pool as usize].window.at(now).retried += 1;
        self.add_pending(key, pool, class);
        self.schedule(at, Timer::Retry(id));
    }

    fn add_pending(&mut self, key: Key, pool: u16, class: usize) {
        let e = self.entry_mut(key);
        if !e.pending {
            e.pending = true;
            let p = &mut self.pools[pool as usize];
            p.pending += 1;
            p.pending_class[class] += 1;
            self.total_pending += 1;
            self.pending_class[class] += 1;
        }
    }

    fn remove_pending(&mut self, key: Key) {
        let e = self.entry_mut(key);
        if e.pending {
            e.pending = false;
            let (pool, class) = (e.pool as usize, e.class as usize);
            let p = &mut self.pools[pool];
            p.pending -= 1;
            p.pending_class[class] -= 1;
            self.total_pending -= 1;
            self.pending_class[class] -= 1;
            if self.space_waiters {
                self.space_waiters = false;
                self.actions.push_back(Action::SpaceAvailable);
            }
        }
    }

    /// Cancel a task. Running attempts are aborted. Returns `false` if not live.
    pub fn cancel(&mut self, now: Time, target: &TaskRef) -> bool {
        self.advance(now);
        let Some(key) = self.lookup(target) else {
            return false;
        };
        if let Some(lease) = self.entry(key).lease
            && let Some(l) = self.leases.remove(&lease)
        {
            self.end_attempt(&l);
            self.actions.push_back(Action::Abort {
                lease,
                id: l.id,
                reason: AbortReason::Cancelled,
            });
        }
        self.finish(key, TaskStatus::Cancelled, Some("cancelled".into()));
        self.pump();
        true
    }

    /// Report the capacity actually available in a pool.
    pub fn set_capacity(&mut self, now: Time, pool: PoolId, capacity: u32) {
        self.advance(now);
        let p = &mut self.pools[pool.0 as usize];
        p.account(now);
        p.capacity = capacity;
        p.dirty = true;
        self.dirty.push(pool.0);
        self.pump();
    }

    /// Current status of a task (live or retained).
    pub fn status(&self, target: &TaskRef) -> Option<TaskStatus> {
        if let Some(k) = self.lookup(target) {
            return Some(self.entry(k).status);
        }
        let id = match target {
            TaskRef::Id(id) => *id,
            TaskRef::Key(k) => *self.retained_keys.get(k)?,
        };
        self.retained.get(&id).map(|r| r.0)
    }

    /// Id of a live or retained task by id or key.
    pub fn resolve(&self, target: &TaskRef) -> Option<TaskId> {
        match target {
            TaskRef::Id(id) => {
                (self.index.contains_key(id) || self.retained.contains_key(id)).then_some(*id)
            }
            TaskRef::Key(k) => self
                .keys
                .get(k)
                .or_else(|| self.retained_keys.get(k))
                .copied(),
        }
    }

    /// Number of live (non-terminal) tasks.
    pub fn live_tasks(&self) -> usize {
        self.index.len()
    }

    /// Number of distinct unknown keys that live tasks are waiting for.
    #[doc(hidden)]
    pub fn awaited_keys(&self) -> usize {
        self.awaits.len()
    }

    fn lookup(&self, target: &TaskRef) -> Option<Key> {
        let id = match target {
            TaskRef::Id(id) => *id,
            TaskRef::Key(k) => *self.keys.get(k)?,
        };
        self.index.get(&id).copied()
    }

    // -----------------------------------------------------------------------------------------
    // Recurring schedules
    // -----------------------------------------------------------------------------------------

    /// Add or replace a recurring (cron) schedule.
    pub fn add_recurring(&mut self, now: Time, spec: RecurringSpec<P>) -> Result<(), String> {
        self.advance(now);
        self.add_recurring_inner(now, spec, None)?;
        self.pump();
        Ok(())
    }

    fn add_recurring_inner(
        &mut self,
        now: Time,
        spec: RecurringSpec<P>,
        next: Option<Time>,
    ) -> Result<(), String> {
        let cron = croner::parser::CronParser::builder()
            .seconds(croner::parser::Seconds::Optional)
            .build()
            .parse(&spec.cron)
            .map_err(|e| format!("invalid cron '{}': {e}", spec.cron))?;
        let tz: chrono_tz::Tz = match &spec.tz {
            Some(t) => t.parse().map_err(|_| format!("unknown time zone '{t}'"))?,
            None => chrono_tz::UTC,
        };
        self.check_config(&spec.template)
            .map_err(|e| format!("invalid template: {e}"))?;
        if self.cfg.persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::RecurringUpsert {
                    spec: spec.clone(),
                })));
        }
        let name = spec.name.clone();
        // Replacing a schedule keeps its history, so an occurrence that was already
        // materialized is neither duplicated nor allowed to overlap.
        let (last_task, materialized) = self
            .recurring
            .get(&name)
            .map(|old| (old.last_task, old.materialized))
            .or_else(|| self.recurring_history.remove(&name))
            .unwrap_or((None, None));
        let mut r = Recurring {
            spec,
            cron,
            tz,
            next: None,
            last_task,
            materialized,
        };
        let mut next = match next {
            Some(n) => Some(n),
            None => self.next_occurrence(&r, now),
        };
        if let (Some(n), Some(m)) = (next, materialized)
            && n <= m
        {
            next = self.next_occurrence(&r, m);
        }
        r.next = next;
        self.recurring.insert(name.clone(), r);
        self.arm_recurring(now, &name);
        Ok(())
    }

    /// Remove a recurring schedule. An occurrence already materialized still runs.
    pub fn remove_recurring(&mut self, now: Time, name: &str) -> bool {
        self.advance(now);
        let removed = self.recurring.remove(name);
        if let Some(r) = &removed {
            // Remembered so re-adding the schedule does not duplicate that occurrence.
            self.recurring_history
                .insert(name.into(), (r.last_task, r.materialized));
            if self.cfg.persist {
                self.actions
                    .push_back(Action::Persist(Box::new(Mutation::RecurringRemove {
                        name: name.into(),
                    })));
            }
        }
        self.pump();
        removed.is_some()
    }

    /// Names of all recurring schedules.
    pub fn recurring_names(&self) -> Vec<Arc<str>> {
        self.recurring.keys().cloned().collect()
    }

    fn next_occurrence(&self, r: &Recurring<P>, after: Time) -> Option<Time> {
        use chrono::TimeZone;
        let wall = self.cfg.epoch_unix_nanos as i128 + after.0 as i128;
        // Cron has whole-second resolution: drop the sub-second part so occurrences land
        // exactly on the second instead of inheriting the offset of `after`.
        let secs = i64::try_from(wall.div_euclid(1_000_000_000)).ok()?;
        let dt = r.tz.timestamp_opt(secs, 0).single()?;
        let next = r.cron.find_next_occurrence(&dt, false).ok()?;
        let ns = next.timestamp_nanos_opt()? as i128 - self.cfg.epoch_unix_nanos as i128;
        (ns >= 0).then_some(Time(ns as u64))
    }

    fn recurring_lead(&self, pool: u16, tolerance: Duration) -> Duration {
        (self.hold_lead(pool, tolerance) + tolerance).min(self.pools[pool as usize].cfg.max_hold)
    }

    /// Arm the materialization timer for the next occurrence, never before `not_before`.
    fn arm_recurring(&mut self, not_before: Time, name: &Arc<str>) {
        let Some(r) = self.recurring.get(name) else {
            return;
        };
        let Some(next) = r.next else { return };
        let pool = self.pool_index[&r.spec.template.pool];
        let lead = self.recurring_lead(pool, r.spec.tolerance);
        let at = (next - lead).max(not_before);
        self.schedule(at, Timer::Recurring(name.clone(), next));
    }

    fn fire_recurring(&mut self, now: Time, name: Arc<str>, occurrence: Time) {
        let Some(r) = self.recurring.get(&name) else {
            return;
        };
        if r.next != Some(occurrence) {
            return; // stale timer from a replaced schedule
        }
        let at = occurrence;
        // Far past its start (e.g. after downtime): run at most this one occurrence late,
        // then resume from the present instead of replaying every missed occurrence.
        let late = at + r.spec.tolerance < now;
        if late && r.spec.on_miss != MissPolicy::RunLate {
            self.totals.missed += 1;
            let next = self.next_occurrence(r, now);
            let r = self.recurring.get_mut(&name).unwrap();
            r.next = next;
            if self.cfg.persist {
                self.actions
                    .push_back(Action::Persist(Box::new(Mutation::RecurringNext {
                        name: name.clone(),
                        next,
                    })));
            }
            self.arm_recurring(now, &name);
            return;
        }
        let pool = self.pool_index[&r.spec.template.pool];
        let mut spec = r.spec.template.clone();
        spec.fixed_start = Some(FixedStart {
            at,
            tolerance: r.spec.tolerance,
            on_miss: r.spec.on_miss,
        });
        spec.key = None;
        spec.deps.clear();
        let prev = match r.spec.overlap {
            OverlapPolicy::Skip => r.last_task,
            OverlapPolicy::Allow => None,
        };
        let (s, e) = self.reservation_interval(pool, &spec, spec.fixed_start.as_ref().unwrap());
        let fits = self.pools[pool as usize].reservations.max_overlap(s, e)
            < self.pools[pool as usize].cfg.reservation_limit();
        let mut task = None;
        if fits {
            let id = self.alloc_id();
            let seq = self.alloc_seq();
            spec.tags.push(("cronoq.recurring".into(), name.clone()));
            self.insert(now, id, seq, now, spec, pool, 0, prev);
            task = Some(id);
        } else {
            self.totals.recurring_conflicts += 1;
        }
        let from = if late { now } else { at };
        let next = self.next_occurrence(self.recurring.get(&name).unwrap(), from);
        let r = self.recurring.get_mut(&name).unwrap();
        if task.is_some() {
            r.last_task = task;
        }
        r.materialized = Some(at);
        r.next = next;
        if self.cfg.persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::RecurringNext {
                    name: name.clone(),
                    next,
                })));
        }
        // At most one materialized occurrence ahead: the next one waits until this one is due.
        self.arm_recurring(at.max(now), &name);
    }

    // -----------------------------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------------------------

    /// Restore state from durable storage. Call once, before any submission.
    pub fn recover(&mut self, now: Time, rec: Recovered<P>) {
        self.advance(now);
        self.next_id = self.next_id.max(rec.next_id);
        for t in rec.terminal {
            self.retain(t.id, t.key, t.status, t.at);
        }
        let mut tasks = rec.tasks;
        tasks.sort_by_key(|t| t.id);
        // Keys that will exist once recovery completes; dependencies on any other unknown key
        // belong to parents that finished outside the retention window.
        self.recovering_keys = Some(tasks.iter().filter_map(|t| t.spec.key.clone()).collect());
        for t in tasks {
            self.next_id = self.next_id.max(t.id.0 + 1);
            self.next_seq = self.next_seq.max(t.seq + 1);
            let pool = self.pool_index.get(&t.spec.pool).copied();
            let missing = match pool {
                None => Some(format!("pool '{}' no longer exists", t.spec.pool)),
                Some(_) => t.spec.resources.iter().find_map(|(n, a)| {
                    match self.resource_index.get(n) {
                        None => Some(format!("resource '{n}' no longer exists")),
                        Some(&r) if *a as f64 > self.resources[r as usize].capacity => Some(
                            format!("resource '{n}' no longer has capacity for this task"),
                        ),
                        Some(_) => None,
                    }
                }),
            };
            if let Some(reason) = missing {
                // Close it out durably so its key is released and it is not recovered again.
                self.totals.failed += 1;
                self.retain(t.id, t.spec.key.clone(), TaskStatus::Failed, now);
                self.push_status(
                    t.id,
                    0,
                    t.spec.key,
                    TaskStatus::Failed,
                    t.attempt,
                    Some(reason.into()),
                    true,
                );
                continue;
            }
            self.insert(
                now,
                t.id,
                t.seq,
                t.arrival,
                t.spec,
                pool.unwrap(),
                t.attempt,
                None,
            );
        }
        self.recovering_keys = None;
        // The latest live occurrence of each schedule restores its overlap guard.
        let mut latest: FxHashMap<Arc<str>, (TaskId, Time)> = FxHashMap::default();
        for k in self.index.values() {
            let e = self.entry(*k);
            let (Some(tags), Some(f)) = (&e.tags, &e.fixed) else {
                continue;
            };
            if let Some((_, name)) = tags.iter().find(|(t, _)| &**t == "cronoq.recurring") {
                let slot = latest.entry(name.clone()).or_insert((e.id, f.at));
                if f.at > slot.1 {
                    *slot = (e.id, f.at);
                }
            }
        }
        for (spec, next) in rec.recurring {
            let name = spec.name.clone();
            if let Some(&(id, at)) = latest.get(&name) {
                self.recurring_history.insert(name, (Some(id), Some(at)));
            }
            // A schedule the configuration no longer supports (removed pool or resource) is not
            // armed; it stays stored and comes back once the configuration supports it again.
            if self.add_recurring_inner(now, spec, next).is_err() {
                self.totals.recurring_invalid += 1;
            }
        }
        // Timers already in the past (missed fixed starts, expired awaits) apply before
        // anything is dispatched.
        self.advance(now);
        self.pump();
    }

    // -----------------------------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------------------------

    fn schedule(&mut self, at: Time, t: Timer) {
        self.timer_seq += 1;
        self.timers.push(Reverse((at, self.timer_seq, t)));
    }

    fn advance(&mut self, now: Time) {
        let now = now.max(self.now);
        self.now = now;
        if !self.started {
            // Engine time may be anything (e.g. Unix nanoseconds): start accounting here
            // rather than integrating capacity over the whole span since `Time::ZERO`.
            self.started = true;
            for p in &mut self.pools {
                p.last_acct = now;
                p.last_eval = now;
            }
            for r in &mut self.resources {
                r.last_refill = now;
            }
        }
        while let Some(Reverse((t, _, _))) = self.timers.peek() {
            if *t > now {
                break;
            }
            let Reverse((t, _, timer)) = self.timers.pop().unwrap();
            self.fire(t.max(Time::ZERO), now, timer);
        }
        self.evict_retained(now);
    }

    fn fire(&mut self, _at: Time, now: Time, timer: Timer) {
        match timer {
            Timer::Retry(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    let e = self.entry(k);
                    if e.status == TaskStatus::Retrying && e.retry_at <= now {
                        let (pool, class) = (e.pool as usize, e.class as usize);
                        self.pools[pool].retrying[class] -= 1;
                        if self.entry(k).unmet == 0 {
                            self.make_eligible(k);
                        } else {
                            self.entry_mut(k).status = TaskStatus::Blocked;
                        }
                    }
                }
            }
            Timer::HoldAt(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    let e = self.entry(k);
                    let ok = matches!(
                        e.status,
                        TaskStatus::Blocked | TaskStatus::Scheduled | TaskStatus::Ready
                    ) && e.fixed.as_ref().is_some_and(|f| !f.held && !f.hold_pending);
                    if ok {
                        let pool = e.pool;
                        let f = e.fixed.as_ref().unwrap();
                        let (at, tolerance, seq) = (f.at, f.tolerance, e.seq);
                        // Recompute the lead from current statistics. If it is not time yet,
                        // check again halfway there, so a lead that grows meanwhile is caught.
                        let target = at - self.hold_lead(pool, tolerance);
                        if now < target {
                            let gap = target.saturating_sub(now);
                            let next = if gap > Duration::from_secs(1) {
                                now + gap / 2
                            } else {
                                target
                            };
                            self.schedule(next, Timer::HoldAt(id));
                            return;
                        }
                        self.entry_mut(k).fixed.as_mut().unwrap().hold_pending = true;
                        self.pools[pool as usize].pending_holds.insert((at, seq), k);
                        self.mark_dirty(pool);
                    }
                }
            }
            Timer::StartAt(id) => {
                if let Some(k) = self.index.get(&id).copied()
                    && self.entry(k).status == TaskStatus::Scheduled
                {
                    self.make_eligible(k);
                }
            }
            Timer::MissAt(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    self.on_miss(k);
                }
            }
            Timer::AwaitTimeout(id) => {
                if let Some(k) = self.index.get(&id).copied()
                    && self.entry(k).awaiting > 0
                    && self.entry(k).status == TaskStatus::Blocked
                {
                    self.finish(
                        k,
                        TaskStatus::Skipped,
                        Some("timed out awaiting unknown dependency".into()),
                    );
                }
            }
            Timer::AttemptTimeout(lease) => {
                if let Some(l) = self.leases.remove(&lease) {
                    self.totals.timeouts += 1;
                    self.end_attempt(&l);
                    self.actions.push_back(Action::Abort {
                        lease,
                        id: l.id,
                        reason: AbortReason::Timeout,
                    });
                    self.fail_attempt(now, l.key, true, None, "attempt timed out".into());
                }
            }
            Timer::LeaseExpiry(lease) => {
                let Some(l) = self.leases.get(&lease) else {
                    return;
                };
                if l.expires > now {
                    let at = l.expires;
                    self.schedule(at, Timer::LeaseExpiry(lease));
                    return;
                }
                let l = self.leases.remove(&lease).unwrap();
                self.totals.lease_expiries += 1;
                self.end_attempt(&l);
                self.actions.push_back(Action::Abort {
                    lease,
                    id: l.id,
                    reason: AbortReason::LeaseExpired,
                });
                self.fail_attempt(now, l.key, true, None, "lease expired".into());
            }
            Timer::Refill(r) => {
                let rs = &mut self.resources[r as usize];
                rs.refill_at = None;
                rs.refill(now);
                self.wake_waiters(r);
                // Fixed tasks blocked on resources live in `due`.
                self.dirty_due_pools();
            }
            Timer::Autoscale(pool) => self.autoscale(now, pool),
            Timer::Recurring(name, at) => self.fire_recurring(now, name, at),
        }
    }

    fn on_miss(&mut self, k: Key) {
        let (status, pool, policy, attempt, late) = {
            let e = self.entry(k);
            let Some(f) = e.fixed.as_ref() else { return };
            (
                e.status,
                e.pool,
                f.on_miss,
                e.attempt,
                self.now > f.at + f.tolerance,
            )
        };
        // A task that already started once (now retrying or waiting for a slot to retry)
        // made its fixed start; only a start that never happened can be missed.
        if attempt > 0
            || !late
            || matches!(status, TaskStatus::Running)
            || status.is_terminal()
            || status == TaskStatus::Retrying
        {
            return;
        }
        self.entry_mut(k).fixed.as_mut().unwrap().missed = true;
        match policy {
            MissPolicy::RunLate => {
                if status == TaskStatus::Blocked {
                    self.release_hold(k);
                }
                self.mark_dirty(pool);
            }
            MissPolicy::Skip => {
                self.totals.missed += 1;
                self.finish(k, TaskStatus::Missed, Some("fixed start missed".into()));
            }
            MissPolicy::Fail => {
                self.finish(k, TaskStatus::Failed, Some("fixed start missed".into()))
            }
        }
    }

    fn release_hold(&mut self, k: Key) {
        let pool = self.entry(k).pool as usize;
        let (seq, f) = {
            let e = self.entry_mut(k);
            (e.seq, e.fixed.as_mut().unwrap())
        };
        let at = f.at;
        let was_held = core::mem::replace(&mut f.held, false);
        let was_pending = core::mem::replace(&mut f.hold_pending, false);
        let p = &mut self.pools[pool];
        if was_held {
            p.held -= 1;
            p.held_tasks.remove(&(at, seq));
            p.dirty = true;
            self.dirty.push(pool as u16);
        }
        if was_pending {
            p.pending_holds.remove(&(at, seq));
        }
    }

    // -----------------------------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------------------------

    /// Unmet dependencies are zero: queue the task (or wait for its fixed start).
    fn make_eligible(&mut self, k: Key) {
        let now = self.now;
        let (pool, class, seq, fixed_at, has_deadline) = {
            let e = self.entry(k);
            (
                e.pool,
                e.class as usize,
                e.seq,
                e.fixed.as_ref().map(|f| f.at),
                e.deadline != Time::MAX,
            )
        };
        match fixed_at {
            Some(at) if now < at => {
                self.set_status(k, TaskStatus::Scheduled);
            }
            Some(at) => {
                self.entry_mut(k).loc = Loc::Due;
                self.pools[pool as usize].due.insert((at, seq), k);
                self.set_status(k, TaskStatus::Ready);
                self.mark_dirty(pool);
            }
            None => {
                if has_deadline {
                    let lst = {
                        let e = self.entry(k);
                        e.deadline - self.estimate_raw(e.pool, Some(e.kind), e.hint_ns)
                    };
                    let e = self.entry_mut(k);
                    e.lst = lst;
                    e.loc = Loc::Edf;
                    self.pools[pool as usize].edf[class].insert((lst, seq), k);
                } else {
                    self.entry_mut(k).loc = Loc::Lane;
                    self.pools[pool as usize].lanes[class].insert(seq, k);
                }
                self.pools[pool as usize].arrivals += 1;
                self.set_status(k, TaskStatus::Ready);
                self.mark_dirty(pool);
            }
        }
    }

    fn unqueue(&mut self, k: Key) {
        let (loc, pool, class, seq, lst, at) = {
            let e = self.entry(k);
            (
                e.loc,
                e.pool as usize,
                e.class as usize,
                e.seq,
                e.lst,
                e.fixed.as_ref().map(|f| f.at),
            )
        };
        let p = &mut self.pools[pool];
        match loc {
            Loc::None | Loc::Parked(_) => {}
            Loc::Lane => {
                p.lanes[class].remove(seq);
            }
            Loc::Edf => {
                p.edf[class].remove(&(lst, seq));
            }
            Loc::Due => {
                p.due.remove(&(at.unwrap(), seq));
            }
        }
        self.entry_mut(k).loc = Loc::None;
    }

    /// Terminal transition with dependency propagation.
    fn finish(&mut self, k: Key, status: TaskStatus, message: Option<Arc<str>>) {
        debug_assert!(status.is_terminal());
        let mut work = vec![(k, status, message)];
        while let Some((k, status, message)) = work.pop() {
            if self.slot_entry(k).is_none() {
                continue;
            }
            let parked_on = match self.entry(k).loc {
                Loc::Parked(r) => Some(r),
                _ => None,
            };
            if self.entry(k).status == TaskStatus::Retrying {
                let e = self.entry(k);
                let (pool, class) = (e.pool as usize, e.class as usize);
                self.pools[pool].retrying[class] -= 1;
            }
            self.unqueue(k);
            self.remove_pending(k);
            if self.entry(k).fixed.is_some() {
                self.release_hold(k);
                let e = self.entry(k);
                let (pool, id, rs) = (e.pool as usize, e.id, e.fixed.as_ref().unwrap().res_start);
                self.pools[pool].reservations.remove(id, rs);
                self.pools[pool].unstarted.remove(id, rs);
            }
            match status {
                TaskStatus::Succeeded => self.totals.succeeded += 1,
                TaskStatus::Failed => self.totals.failed += 1,
                TaskStatus::DeadLettered => self.totals.dead_lettered += 1,
                TaskStatus::Cancelled => self.totals.cancelled += 1,
                TaskStatus::Skipped => self.totals.skipped += 1,
                TaskStatus::Shed => self.totals.shed += 1,
                _ => {}
            }
            let e = self.slab_remove(k);
            if e.has_awaits
                && let Some(keys) = self.awaited_by.remove(&e.id)
            {
                for key in keys.iter() {
                    if let Some(list) = self.awaits.get_mut(key) {
                        list.retain(|(cid, _)| *cid != e.id);
                        if list.is_empty() {
                            self.awaits.remove(key);
                        }
                    }
                }
            }
            if let Some(r) = parked_on {
                // This task may have been the head waiter blocking others that now fit.
                self.wake_waiters(r.0);
            }
            self.index.remove(&e.id);
            if let Some(key) = &e.key
                && self.keys.get(key) == Some(&e.id)
            {
                self.keys.remove(key);
            }
            self.retain(e.id, e.key.clone(), status, self.now);
            self.push_status(
                e.id,
                e.pool,
                e.key.clone(),
                status,
                e.attempt,
                message,
                true,
            );
            for (cid, cond) in e.dependents {
                let Some(ck) = self.index.get(&cid).copied() else {
                    continue;
                };
                if edge_satisfied(cond, status) {
                    let c = self.entry_mut(ck);
                    c.unmet = c.unmet.saturating_sub(1);
                    if c.unmet == 0 && c.status == TaskStatus::Blocked {
                        self.make_eligible(ck);
                    }
                } else {
                    work.push((
                        ck,
                        TaskStatus::Skipped,
                        Some(format!("dependency {} ended as {status}", e.id).into()),
                    ));
                }
            }
        }
    }

    fn retain(&mut self, id: TaskId, key: Option<Arc<str>>, status: TaskStatus, at: Time) {
        self.retained.insert(id, (status, at));
        if let Some(k) = &key {
            self.retained_keys.insert(k.clone(), id);
        }
        self.retention_q.push_back((at, id, key));
    }

    fn evict_retained(&mut self, now: Time) {
        while let Some((t, _, _)) = self.retention_q.front() {
            if now.saturating_sub(*t) <= self.cfg.retention
                && self.retention_q.len() <= self.cfg.retention_max
            {
                break;
            }
            let (_, id, key) = self.retention_q.pop_front().unwrap();
            self.retained.remove(&id);
            if let Some(k) = key
                && self.retained_keys.get(&k) == Some(&id)
            {
                self.retained_keys.remove(&k);
            }
        }
    }

    fn set_status(&mut self, k: Key, status: TaskStatus) {
        let emit = self.emit_transitions;
        let e = self.entry_mut(k);
        e.status = status;
        if emit {
            let (id, pool, key, at) = (e.id, e.pool, e.key.clone(), e.attempt);
            self.push_status(id, pool, key, status, at, None, false);
        }
    }

    fn emit(&mut self, k: Key, status: TaskStatus, message: Option<Arc<str>>) {
        let emit = self.emit_transitions;
        let e = self.entry_mut(k);
        e.status = status;
        let (id, pool, key, at) = (e.id, e.pool, e.key.clone(), e.attempt);
        if emit {
            self.push_status(id, pool, key, status, at, message, false);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_status(
        &mut self,
        id: TaskId,
        pool: u16,
        key: Option<Arc<str>>,
        status: TaskStatus,
        attempt: u32,
        message: Option<Arc<str>>,
        important: bool,
    ) {
        let persist = self.cfg.persist
            && (status.is_terminal()
                || matches!(status, TaskStatus::Running | TaskStatus::Retrying));
        if persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::Status {
                    id,
                    status,
                    attempt,
                    at: self.now,
                    message: message.clone(),
                })));
        }
        if important || self.emit_transitions {
            self.actions.push_back(Action::Status(StatusEvent {
                id,
                pool: PoolId(pool),
                key,
                status,
                attempt,
                at: self.now,
                message,
            }));
        }
    }

    // -----------------------------------------------------------------------------------------
    // Dispatch
    // -----------------------------------------------------------------------------------------

    fn mark_dirty(&mut self, pool: u16) {
        let p = &mut self.pools[pool as usize];
        if !p.dirty {
            p.dirty = true;
            self.dirty.push(pool);
        }
    }

    fn pump(&mut self) {
        while let Some(pool) = self.dirty.pop() {
            if self.pools[pool as usize].dirty {
                self.pools[pool as usize].dirty = false;
                self.pump_pool(pool);
            }
        }
    }

    fn pump_pool(&mut self, pool: u16) {
        let now = self.now;
        // 1. Holds for upcoming fixed starts take free slots first.
        loop {
            let p = &mut self.pools[pool as usize];
            if p.free() == 0 {
                break;
            }
            let Some((hk, k)) = p.pending_holds.pop_first() else {
                break;
            };
            p.held += 1;
            p.held_tasks.insert(hk, k);
            let f = self.entry_mut(k).fixed.as_mut().unwrap();
            f.hold_pending = false;
            f.held = true;
        }
        if self.pools[pool as usize].due.is_empty() {
            self.pump_regular(now, pool);
            return;
        }
        // 2a. Due fixed-start tasks that hold a slot start first (there are few of them).
        if !self.pools[pool as usize].held_tasks.is_empty() {
            let held: Vec<Key> = self.pools[pool as usize]
                .held_tasks
                .values()
                .copied()
                .filter(|k| self.entry(*k).loc == Loc::Due)
                .collect();
            for k in held {
                self.start_due(now, pool, k);
            }
        }
        // 2b. Other due fixed-start tasks, earliest first, while slots are free. Walking the
        // map in order (instead of copying it) keeps a large run-late backlog linear.
        let mut cursor: Option<(Time, u64)> = None;
        while self.pools[pool as usize].free() > 0 {
            let p = &self.pools[pool as usize];
            let next = match cursor {
                None => p.due.iter().next(),
                Some(c) => p
                    .due
                    .range((core::ops::Bound::Excluded(c), core::ops::Bound::Unbounded))
                    .next(),
            };
            let Some((&dk, &k)) = next else {
                break;
            };
            cursor = Some(dk);
            self.start_due(now, pool, k);
        }
        self.pump_regular(now, pool);
    }

    /// 3. Regular work by effective score.
    fn pump_regular(&mut self, now: Time, pool: u16) {
        while self.pools[pool as usize].free() > 0 {
            let Some(k) = self.best_candidate(pool, now) else {
                break;
            };
            self.unqueue(k);
            if !self.try_acquire(k) {
                self.park(k);
                continue;
            }
            self.dispatch(now, pool, k);
        }
    }

    /// Try to start a due fixed-start task (in a held slot or a free one).
    fn start_due(&mut self, now: Time, pool: u16, k: Key) {
        let (held, skip_if, at, seq) = {
            let e = self.entry(k);
            let f = e.fixed.as_ref().unwrap();
            (f.held, f.skip_if_live, f.at, e.seq)
        };
        if let Some(prev) = skip_if
            && self.index.contains_key(&prev)
        {
            // The schedule's guard moves back to the occurrence that is still running, so
            // the next occurrence is checked against it rather than this skipped one.
            let e = self.entry(k);
            let id = e.id;
            let name = e
                .tags
                .as_ref()
                .and_then(|t| t.iter().find(|(k, _)| &**k == "cronoq.recurring"))
                .map(|(_, v)| v.clone());
            if let Some(r) = name.and_then(|n| self.recurring.get_mut(&n))
                && r.last_task == Some(id)
            {
                r.last_task = Some(prev);
            }
            self.finish(
                k,
                TaskStatus::Skipped,
                Some("previous occurrence still running".into()),
            );
            return;
        }
        if !held && self.pools[pool as usize].free() == 0 {
            return;
        }
        if !self.try_acquire(k) {
            self.schedule_refills(k);
            return;
        }
        self.pools[pool as usize].due.remove(&(at, seq));
        self.entry_mut(k).loc = Loc::None;
        // Frees the held slot for this dispatch and drops any hold still pending.
        self.release_hold(k);
        self.dispatch(now, pool, k);
    }

    fn score(&self, e: &Entry<P>, now: Time) -> f64 {
        let pc = &self.cfg.priority;
        let c = e.class as usize;
        let wait = now.diff_secs(e.arrival).max(0.0);
        let mut s = pc.base[c] + (pc.aging_rate[c] * wait).min(pc.age_cap[c]);
        if e.deadline != Time::MAX {
            let u = &self.cfg.urgency;
            let d = e.deadline.diff_secs(e.lst).max(0.0);
            let h = d.max(secs(u.window)).max(1e-9);
            let slack = e.lst.diff_secs(now);
            s += if slack >= h {
                0.0
            } else if slack >= 0.0 {
                u.max * (1.0 - slack / h)
            } else {
                u.max + (u.overdue_rate * -slack).min(u.overdue_cap)
            };
        }
        s
    }

    fn best_candidate(&self, pool: u16, now: Time) -> Option<Key> {
        let p = &self.pools[pool as usize];
        let mut best: Option<(f64, u8, u64, Key)> = None;
        for c in 0..Priority::COUNT {
            let heads = [
                p.lanes[c].first(),
                p.edf[c].first_key_value().map(|(_, k)| *k),
            ];
            for k in heads.into_iter().flatten() {
                let e = self.entry(k);
                let s = self.score(e, now);
                let better = match best {
                    None => true,
                    Some((bs, bc, bseq, _)) => s > bs || (s == bs && (e.class, e.seq) < (bc, bseq)),
                };
                if better {
                    best = Some((s, e.class, e.seq, k));
                }
            }
        }
        best.map(|b| b.3)
    }

    fn try_acquire(&mut self, k: Key) -> bool {
        let Some(res) = self.entry(k).resources.clone() else {
            return true;
        };
        let now = self.now;
        for (r, amount) in res.iter() {
            let rs = &mut self.resources[r.0 as usize];
            rs.refill(now);
            if rs.available < *amount as f64 {
                return false;
            }
        }
        for (r, amount) in res.iter() {
            self.resources[r.0 as usize].available -= *amount as f64;
        }
        true
    }

    fn park(&mut self, k: Key) {
        let res = self.entry(k).resources.clone().unwrap();
        let now = self.now;
        let (r, amount) = *res
            .iter()
            .find(|(r, a)| {
                let rs = &mut self.resources[r.0 as usize];
                rs.refill(now);
                rs.available < *a as f64
            })
            .unwrap_or(&res[0]);
        self.entry_mut(k).loc = Loc::Parked(r);
        self.resources[r.0 as usize].waiters.push_back(k);
        self.schedule_refill(r.0, amount);
    }

    fn schedule_refills(&mut self, k: Key) {
        if let Some(res) = self.entry(k).resources.clone() {
            for (r, a) in res.iter() {
                self.schedule_refill(r.0, *a);
            }
        }
    }

    fn schedule_refill(&mut self, r: u16, need: u32) {
        let now = self.now;
        let rs = &mut self.resources[r as usize];
        if let ResourceKind::TokenBucket { rate, .. } = rs.kind {
            if rate <= 0.0 {
                return;
            }
            let missing = (need as f64 - rs.available).max(0.0);
            let wait = Duration::try_from_secs_f64(missing / rate).unwrap_or(Duration::MAX);
            let at = now + wait.max(Duration::from_micros(1));
            if rs.refill_at.is_none_or(|t| at < t) {
                rs.refill_at = Some(at);
                self.schedule(at, Timer::Refill(r));
            }
        }
    }

    /// Move parked tasks back to their queues, in FIFO order, while the resource can satisfy them.
    fn wake_waiters(&mut self, r: u16) {
        let now = self.now;
        self.resources[r as usize].refill(now);
        let mut budget = self.resources[r as usize].available;
        while let Some(&k) = self.resources[r as usize].waiters.front() {
            let need = match self.slot_entry(k) {
                Some(e) if e.loc == Loc::Parked(ResourceId(r)) => e
                    .resources
                    .as_ref()
                    .and_then(|v| v.iter().find(|(x, _)| x.0 == r))
                    .map(|x| x.1)
                    .unwrap_or(1),
                _ => {
                    self.resources[r as usize].waiters.pop_front();
                    continue;
                }
            };
            if (need as f64) > budget + 1e-9 {
                self.schedule_refill(r, need);
                break;
            }
            self.resources[r as usize].waiters.pop_front();
            budget -= need as f64;
            let (pool, class, seq, has_deadline, lst) = {
                let e = self.entry(k);
                (
                    e.pool,
                    e.class as usize,
                    e.seq,
                    e.deadline != Time::MAX,
                    e.lst,
                )
            };
            let p = &mut self.pools[pool as usize];
            if has_deadline {
                p.edf[class].insert((lst, seq), k);
                self.entry_mut(k).loc = Loc::Edf;
            } else {
                p.lanes[class].insert(seq, k);
                self.entry_mut(k).loc = Loc::Lane;
            }
            self.mark_dirty(pool);
        }
    }

    fn dispatch(&mut self, now: Time, pool: u16, k: Key) {
        let lease = LeaseId(self.next_lease);
        self.next_lease += 1;
        self.remove_pending(k);
        let higher_waiting = {
            let p = &self.pools[pool as usize];
            let c = self.entry(k).class as usize;
            (0..c).any(|h| !p.lanes[h].is_empty() || !p.edf[h].is_empty())
        };
        let (id, attempt, timeout_ns, first, arrival, fixed) = {
            let e = self.entry_mut(k);
            e.attempt += 1;
            e.status = TaskStatus::Running;
            e.lease = Some(lease);
            (
                e.id,
                e.attempt,
                e.timeout_ns,
                e.attempt == 1,
                e.arrival,
                e.fixed.as_ref().map(|f| (f.at, f.tolerance, f.res_start)),
            )
        };
        if let Some((_, _, res_start)) = fixed {
            self.pools[pool as usize].unstarted.remove(id, res_start);
        }
        let lease_ttl = {
            let p = &mut self.pools[pool as usize];
            p.account(now);
            p.busy += 1;
            let b = p.window.at(now);
            b.dispatched += 1;
            if higher_waiting {
                b.inversions += 1;
            }
            if first {
                b.wait.record(now.saturating_sub(arrival));
            }
            if let (Some((at, tol, _)), true) = (fixed, first) {
                // Only the first start counts: retries are not late fixed starts.
                b.fixed_total += 1;
                if now.diff_secs(at).abs() <= tol.as_secs_f64() {
                    b.fixed_hits += 1;
                }
            }
            p.cfg.lease_ttl
        };
        self.totals.dispatched += 1;
        let expires = lease_ttl.map(|t| now + t).unwrap_or(Time::MAX);
        self.leases.insert(
            lease,
            Lease {
                key: k,
                id,
                pool,
                started: now,
                expires,
            },
        );
        if timeout_ns > 0 {
            self.schedule(
                now + Duration::from_nanos(timeout_ns),
                Timer::AttemptTimeout(lease),
            );
        }
        if expires != Time::MAX {
            self.schedule(expires, Timer::LeaseExpiry(lease));
        }
        let e = self.entry(k);
        let d = Dispatch {
            lease,
            id,
            pool: PoolId(pool),
            attempt,
            payload: e.payload.clone(),
            kind: self.kinds[e.kind as usize].clone(),
            key: e.key.clone(),
            timeout: (timeout_ns > 0).then(|| Duration::from_nanos(timeout_ns)),
            deadline: (e.deadline != Time::MAX).then_some(e.deadline),
            tags: e.tags.clone().unwrap_or_else(|| self.empty_tags.clone()),
        };
        let key = e.key.clone();
        self.actions.push_back(Action::Dispatch(d));
        self.push_status(id, pool, key, TaskStatus::Running, attempt, None, false);
    }

    // -----------------------------------------------------------------------------------------
    // Autoscaling
    // -----------------------------------------------------------------------------------------

    fn autoscale(&mut self, now: Time, pool: u16) {
        let Some(cfg) = self.pools[pool as usize].cfg.autoscale.clone() else {
            return;
        };
        self.schedule(now + cfg.interval, Timer::Autoscale(pool));
        let p = &mut self.pools[pool as usize];
        p.account(now);
        let dt = if p.last_eval == Time::ZERO {
            0.0
        } else {
            now.diff_secs(p.last_eval)
        };
        p.last_eval = now;
        let mut backlog = [0u64; Priority::COUNT];
        for (c, b) in backlog.iter_mut().enumerate() {
            *b = (p.lanes[c].len() + p.edf[c].len() + p.retrying[c]) as u64;
        }
        backlog[0] += p.due.len() as u64;
        let signals = Signals {
            arrivals: core::mem::take(&mut p.arrivals),
            dt,
            service_secs: p
                .stats
                .mean
                .get()
                .unwrap_or(p.cfg.default_duration.as_secs_f64()),
            backlog,
            busy: p.busy,
            capacity: p.capacity,
            // Running fixed tasks are already counted in `busy`.
            reserved: p.unstarted.max_overlap(now, now + cfg.reservation_horizon),
            min: p.cfg.min_workers,
            max: p.cfg.max_workers,
        };
        if let Some(desired) = p.autoscale.as_mut().unwrap().evaluate(&cfg, &signals, now) {
            self.actions.push_back(Action::ScaleTo {
                pool: PoolId(pool),
                desired,
            });
        }
    }

    // -----------------------------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------------------------

    /// Advance to `now` (firing due timers) and return current state and metrics.
    pub fn snapshot(&mut self, now: Time) -> Snapshot {
        self.advance(now);
        // Timers fired by `advance` may have made work ready.
        self.pump();
        let mut pools = Vec::with_capacity(self.pools.len());
        for p in &mut self.pools {
            p.account(now);
            let w = p.window.total(now);
            let mut pending = [0usize; Priority::COUNT];
            pending.copy_from_slice(&p.pending_class);
            pools.push(PoolSnapshot {
                name: p.cfg.name.clone(),
                capacity: p.capacity,
                busy: p.busy,
                held: p.held,
                desired: p
                    .autoscale
                    .as_ref()
                    .and_then(|a| a.target)
                    .unwrap_or(p.capacity),
                pending_by_class: pending,
                ready: p.ready_len(),
                reservations: p.reservations.len(),
                window: p.window.span(),
                dispatched: w.dispatched,
                succeeded: w.succeeded,
                failed: w.failed,
                retried: w.retried,
                priority_adherence: if w.dispatched == 0 {
                    1.0
                } else {
                    1.0 - w.inversions as f64 / w.dispatched as f64
                },
                fixed_start_hit_rate: if w.fixed_total == 0 {
                    1.0
                } else {
                    w.fixed_hits as f64 / w.fixed_total as f64
                },
                utilization: if w.capacity_secs > 0.0 {
                    (w.busy_secs / w.capacity_secs).min(1.0)
                } else {
                    0.0
                },
                schedule_efficiency: if w.capacity_secs > 0.0 {
                    (w.work_secs / w.capacity_secs).min(1.0)
                } else {
                    0.0
                },
                wait_mean: w.wait.mean(),
                wait_p50: w.wait.quantile(0.5),
                wait_p90: w.wait.quantile(0.9),
                wait_p99: w.wait.quantile(0.99),
                run_p50: w.run.quantile(0.5),
                run_p99: w.run.quantile(0.99),
                worker_seconds: p.worker_secs,
                cost: p.cost,
            });
        }
        Snapshot {
            at: now,
            pools,
            live_tasks: self.index.len(),
            running: self.leases.len(),
            pending: self.total_pending,
            totals: self.totals.clone(),
        }
    }

    // -----------------------------------------------------------------------------------------
    // Slab
    // -----------------------------------------------------------------------------------------

    fn slab_insert(&mut self, e: Entry<P>) -> Key {
        if let Some(idx) = self.free_slots.pop() {
            let s = &mut self.slots[idx as usize];
            s.entry = Some(e);
            Key {
                idx,
                generation: s.generation,
            }
        } else {
            let idx = self.slots.len() as u32;
            self.slots.push(Slot {
                generation: 0,
                entry: Some(e),
            });
            Key { idx, generation: 0 }
        }
    }

    fn slab_remove(&mut self, k: Key) -> Entry<P> {
        let s = &mut self.slots[k.idx as usize];
        debug_assert_eq!(s.generation, k.generation);
        let e = s.entry.take().expect("live entry");
        s.generation = s.generation.wrapping_add(1);
        self.free_slots.push(k.idx);
        e
    }

    #[inline]
    fn slot_entry(&self, k: Key) -> Option<&Entry<P>> {
        let s = self.slots.get(k.idx as usize)?;
        if s.generation != k.generation {
            return None;
        }
        s.entry.as_ref()
    }

    #[inline]
    fn entry(&self, k: Key) -> &Entry<P> {
        self.slot_entry(k).expect("stale task key")
    }

    #[inline]
    fn entry_mut(&mut self, k: Key) -> &mut Entry<P> {
        let s = &mut self.slots[k.idx as usize];
        debug_assert_eq!(s.generation, k.generation);
        s.entry.as_mut().expect("stale task key")
    }

    fn entry_by_id(&self, id: TaskId) -> Option<&Entry<P>> {
        self.index.get(&id).and_then(|k| self.slot_entry(*k))
    }

    // -----------------------------------------------------------------------------------------
    // Invariants (for tests and simulation)
    // -----------------------------------------------------------------------------------------

    /// Panics if internal invariants are violated. Intended for tests.
    #[doc(hidden)]
    pub fn check_invariants(&self) {
        let mut busy = vec![0u32; self.pools.len()];
        for (lid, l) in &self.leases {
            let e = self.slot_entry(l.key).expect("lease points to live task");
            assert_eq!(e.status, TaskStatus::Running);
            assert_eq!(e.lease, Some(*lid));
            busy[l.pool as usize] += 1;
        }
        let mut held = vec![0u32; self.pools.len()];
        let mut pending = vec![0usize; self.pools.len()];
        let mut queued = vec![0usize; self.pools.len()];
        let mut pending_class = vec![[0usize; Priority::COUNT]; self.pools.len()];
        let mut retrying = vec![[0usize; Priority::COUNT]; self.pools.len()];
        let mut reservations = vec![0usize; self.pools.len()];
        let mut live_parents: FxHashMap<TaskId, usize> = FxHashMap::default();
        for k in self.index.values() {
            for (child, _) in &self.entry(*k).dependents {
                *live_parents.entry(*child).or_default() += 1;
            }
        }
        let mut awaiting: FxHashMap<TaskId, usize> = FxHashMap::default();
        for list in self.awaits.values() {
            for (id, _) in list {
                if self.index.contains_key(id) {
                    *awaiting.entry(*id).or_default() += 1;
                }
            }
        }
        for (id, k) in &self.index {
            let e = self.slot_entry(*k).expect("index points to live slot");
            assert_eq!(e.id, *id);
            assert!(!e.status.is_terminal());
            if e.status == TaskStatus::Running {
                assert!(e.lease.is_some() && !e.pending);
            } else {
                assert!(e.pending, "non-running task {id} not counted as pending");
                pending[e.pool as usize] += 1;
            }
            if let Some(f) = &e.fixed
                && f.held
            {
                held[e.pool as usize] += 1;
            }
            let p = &self.pools[e.pool as usize];
            match e.loc {
                Loc::Lane => {
                    assert_eq!(p.lanes[e.class as usize].get(e.seq), Some(*k));
                    queued[e.pool as usize] += 1;
                }
                Loc::Edf => {
                    assert_eq!(p.edf[e.class as usize].get(&(e.lst, e.seq)), Some(k));
                    queued[e.pool as usize] += 1;
                }
                Loc::Due => {
                    assert_eq!(p.due.get(&(e.fixed.as_ref().unwrap().at, e.seq)), Some(k));
                    queued[e.pool as usize] += 1;
                }
                Loc::Parked(_) | Loc::None => {}
            }
            if matches!(e.loc, Loc::Lane | Loc::Edf | Loc::Due | Loc::Parked(_)) {
                assert_eq!(e.status, TaskStatus::Ready);
            }
            if e.status == TaskStatus::Blocked {
                assert!(
                    e.unmet > 0 || e.fixed.is_some(),
                    "blocked task {id} without unmet deps"
                );
            }
            if e.pending {
                pending_class[e.pool as usize][e.class as usize] += 1;
            }
            if e.status == TaskStatus::Retrying {
                retrying[e.pool as usize][e.class as usize] += 1;
            }
            if let Loc::Parked(r) = e.loc {
                assert!(
                    self.resources[r.0 as usize].waiters.contains(k),
                    "parked task {id} missing from its waiter queue"
                );
            }
            if let Some(f) = &e.fixed {
                assert!(
                    !(f.held && f.hold_pending),
                    "task {id} both held and pending a hold"
                );
                assert_eq!(
                    p.pending_holds.get(&(f.at, e.seq)) == Some(k),
                    f.hold_pending,
                    "task {id} pending-hold mismatch"
                );
                assert_eq!(
                    p.held_tasks.get(&(f.at, e.seq)) == Some(k),
                    f.held,
                    "task {id} held mismatch"
                );
                assert!(
                    p.reservations.contains(e.id, f.res_start),
                    "fixed task {id} lost its reservation"
                );
                assert_eq!(
                    p.unstarted.contains(e.id, f.res_start),
                    e.attempt == 0 && e.status != TaskStatus::Running,
                    "task {id} unstarted-reservation mismatch"
                );
                reservations[e.pool as usize] += 1;
            }
            // Every unmet dependency is a live parent or an awaited key.
            let parents = live_parents.get(id).copied().unwrap_or(0);
            let awaited = awaiting.get(id).copied().unwrap_or(0);
            assert_eq!(e.awaiting as usize, awaited, "task {id} awaiting mismatch");
            if e.status == TaskStatus::Blocked && e.fixed.is_none() {
                assert_eq!(
                    e.unmet as usize,
                    parents + awaited,
                    "task {id} unmet mismatch"
                );
            }
        }
        for (i, p) in self.pools.iter().enumerate() {
            assert_eq!(p.busy, busy[i], "pool {} busy mismatch", p.cfg.name);
            assert_eq!(p.held, held[i], "pool {} held mismatch", p.cfg.name);
            assert_eq!(
                p.pending, pending[i],
                "pool {} pending mismatch",
                p.cfg.name
            );
            assert_eq!(
                p.ready_len(),
                queued[i],
                "pool {} queue mismatch",
                p.cfg.name
            );
            assert_eq!(
                p.pending_class, pending_class[i],
                "pool {} per-class pending",
                p.cfg.name
            );
            assert_eq!(
                p.retrying, retrying[i],
                "pool {} retrying mismatch",
                p.cfg.name
            );
            assert_eq!(
                p.reservations.len(),
                reservations[i],
                "pool {} stray reservations",
                p.cfg.name
            );
            assert_eq!(
                p.held_tasks.len() as u32,
                p.held,
                "pool {} held_tasks mismatch",
                p.cfg.name
            );
        }
        assert_eq!(self.total_pending, pending.iter().sum::<usize>());
        for c in 0..Priority::COUNT {
            let sum: usize = pending_class.iter().map(|pc| pc[c]).sum();
            assert_eq!(
                self.pending_class[c], sum,
                "global per-class pending mismatch"
            );
        }
        // Semaphore permits in use equal what running attempts hold.
        let mut in_use = vec![0u64; self.resources.len()];
        for l in self.leases.values() {
            if let Some(res) = &self.entry(l.key).resources {
                for (r, a) in res.iter() {
                    in_use[r.0 as usize] += *a as u64;
                }
            }
        }
        for (i, r) in self.resources.iter().enumerate() {
            if let ResourceKind::Semaphore { permits } = r.kind {
                let used = permits as f64 - r.available;
                assert!(
                    (used - in_use[i] as f64).abs() < 1e-6,
                    "semaphore {} leaks permits: {used} in use, {} held",
                    r.name,
                    in_use[i]
                );
            }
        }
        for r in &self.resources {
            assert!(
                r.available <= r.capacity + 1e-9,
                "resource {} over capacity",
                r.name
            );
            assert!(r.available >= -1e-9, "resource {} negative", r.name);
        }
    }
}

/// Duration in nanoseconds, at least 1 and saturating at `u64::MAX`.
#[inline]
fn nanos(d: Duration) -> u64 {
    d.as_nanos().clamp(1, u64::MAX as u128) as u64
}

#[inline]
fn edge_satisfied(cond: EdgeCond, parent: TaskStatus) -> bool {
    match cond {
        EdgeCond::OnSuccess => parent == TaskStatus::Succeeded,
        EdgeCond::OnComplete => parent.is_terminal(),
        EdgeCond::OnFailure => matches!(parent, TaskStatus::Failed | TaskStatus::DeadLettered),
    }
}
