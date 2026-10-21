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
use crate::stats::{DurStats, Window};
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
            let dt = now.diff_secs(self.last_refill).max(0.0);
            self.available = (self.available + rate * dt).min(self.capacity);
        }
        self.last_refill = now;
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
    reservations: Reservations,
    pending: usize,
    pending_class: [usize; Priority::COUNT],
    autoscale: Option<AutoscaleState>,
    arrivals: u64,
    last_eval: Time,
    stats: DurStats,
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
}

enum Parent {
    Live(TaskId),
    Done(TaskStatus),
    Await(Arc<str>),
}

/// Engine-wide totals since start.
#[derive(Clone, Debug, Default)]
pub struct Totals {
    pub submitted: u64,
    pub dispatched: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub dead_lettered: u64,
    pub cancelled: u64,
    pub skipped: u64,
    pub missed: u64,
    pub shed: u64,
    pub retries: u64,
    pub timeouts: u64,
    pub lease_expiries: u64,
    pub recurring_conflicts: u64,
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
}

impl<P> Core<P> {
    pub fn new(cfg: CoreConfig) -> Self {
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
                reservations: Reservations::default(),
                pending: 0,
                pending_class: [0; Priority::COUNT],
                autoscale: pc.autoscale.as_ref().map(|_| AutoscaleState::default()),
                arrivals: 0,
                last_eval: Time::ZERO,
                stats: DurStats::default(),
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
        }
    }

    /// Emit `Action::Status` for every transition, not only terminal ones.
    pub fn set_emit_transitions(&mut self, on: bool) {
        self.emit_transitions = on;
    }

    pub fn config(&self) -> &CoreConfig {
        &self.cfg
    }

    pub fn pool_id(&self, name: &str) -> Option<PoolId> {
        self.pool_index.get(name).map(|&i| PoolId(i))
    }

    pub fn pool_name(&self, pool: PoolId) -> &Arc<str> {
        &self.pools[pool.0 as usize].cfg.name
    }

    pub fn pool_capacity(&self, pool: PoolId) -> u32 {
        self.pools[pool.0 as usize].capacity
    }

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
        self.advance(now);
        if let Some(id) = self.existing_key(spec.key.as_deref()) {
            return Ok(Submitted {
                id,
                duplicate: true,
            });
        }
        let pool = self.validate(&spec, &FxHashSet::default())?;
        self.check_bounds(pool, spec.priority, 1, true)?;
        if let Some(f) = &spec.fixed_start {
            let (s, e) = self.reservation_interval(pool, &spec, f);
            if self.pools[pool as usize].reservations.max_overlap(s, e)
                >= self.pools[pool as usize].cfg.reservation_limit()
            {
                return Err(SubmitError::ReservationConflict);
            }
        }
        let id = self.alloc_id();
        let seq = self.alloc_seq();
        self.insert(now, id, seq, now, spec, pool, 0, None);
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
            if let Some(k) = &s.key {
                if self.existing_key(Some(k)).is_none() && batch_keys.insert(k.clone(), i).is_some()
                {
                    return Err((i, SubmitError::DuplicateKeyInBatch(k.clone())));
                }
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
                if let TaskRef::Key(k) = &d.target {
                    if let Some(&j) = batch_keys.get(k) {
                        if self.keys.contains_key(k) || self.retained_keys.contains_key(k) {
                            continue;
                        }
                        indeg[i] += 1;
                        children[j].push(i);
                    }
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
        for (&(p, c), &k) in &counts {
            self.check_bounds(p, c, k, false).map_err(|e| (0, e))?;
        }
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
        self.pump();
        Ok(out)
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
        if let Some(r) = &spec.retry {
            if r.max_attempts == 0 {
                return Err(SubmitError::Invalid("retry.max_attempts must be >= 1"));
            }
        }
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
        if let Some(k) = &spec.key {
            if let Some(waiting) = self.awaits.get(k) {
                if !live_parents.is_empty()
                    && self.reaches_any(waiting.iter().map(|w| w.0), &live_parents)
                {
                    return Err(SubmitError::Cycle);
                }
            }
        }
        Ok(pool)
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
                if let Some(k) = &e.key {
                    if let Some(w) = self.awaits.get(k) {
                        stack.extend(w.iter().map(|d| d.0));
                    }
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
        loop {
            let p = &self.pools[pool as usize];
            let c = class.index();
            let over = |cur: usize, lim: Option<usize>| lim.is_some_and(|l| cur + n > l);
            let full = over(self.total_pending, self.cfg.global_bounds.max_pending)
                || over(
                    self.pending_class[c],
                    self.cfg.global_bounds.max_pending_per_class[c],
                )
                || over(p.pending, p.cfg.bounds.max_pending)
                || over(p.pending_class[c], p.cfg.bounds.max_pending_per_class[c]);
            if !full {
                return Ok(());
            }
            let victim = match (p.cfg.overflow, allow_shed) {
                (OverflowPolicy::Wait, _) => {
                    self.space_waiters = true;
                    return Err(SubmitError::Full);
                }
                (OverflowPolicy::Reject, _) | (_, false) => return Err(SubmitError::Overloaded),
                (OverflowPolicy::ShedLowest, true) => {
                    (c + 1..Priority::COUNT).rev().find_map(|vc| {
                        p.lanes[vc]
                            .last()
                            .or_else(|| p.edf[vc].last_key_value().map(|(_, k)| *k))
                    })
                }
                (OverflowPolicy::ShedOldest, true) => {
                    let low = Priority::Low.index();
                    p.lanes[low].first()
                }
            };
            match victim {
                Some(k) => self.finish(k, TaskStatus::Shed, Some("shed under overload".into())),
                None => return Err(SubmitError::Overloaded),
            }
        }
    }

    fn reservation_interval(&self, pool: u16, spec: &TaskSpec<P>, f: &FixedStart) -> (Time, Time) {
        let kind = self.kind_index.get(&spec.kind).copied();
        let d = self.estimate_raw(
            pool,
            kind,
            spec.duration_hint.map(|d| d.as_nanos() as u64).unwrap_or(0),
        );
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

    fn hold_lead(&self, pool: u16, tolerance: Duration) -> Duration {
        let p = &self.pools[pool as usize];
        let p90 = p.stats.p90().unwrap_or(p.cfg.default_duration);
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
        let hint_ns = spec
            .duration_hint
            .map(|d| d.as_nanos().max(1) as u64)
            .unwrap_or(0);
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
            self.pools[pool as usize].reservations.insert(id, s, e);
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
        for d in &spec.deps {
            let p = match &d.target {
                TaskRef::Id(pid) => match self.index.get(pid) {
                    Some(_) => Parent::Live(*pid),
                    None => Parent::Done(
                        self.retained
                            .get(pid)
                            .map(|r| r.0)
                            .unwrap_or(TaskStatus::Succeeded),
                    ),
                },
                TaskRef::Key(k) => {
                    if let Some(&pid) = self.keys.get(k) {
                        Parent::Live(pid)
                    } else if let Some(pid) = self.retained_keys.get(k) {
                        Parent::Done(self.retained[pid].0)
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
            timeout_ns: spec
                .timeout
                .map(|d| d.as_nanos().max(1) as u64)
                .unwrap_or(0),
            retry: spec.retry.map(Box::new),
            attempt,
            unmet: 0,
            awaiting: 0,
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
                    self.awaits.entry(k).or_default().push((id, cond));
                    unmet += 1;
                    awaiting += 1;
                }
            }
        }
        // Tasks awaiting this key become our dependents.
        if let Some(k) = &spec.key {
            if let Some(waiting) = self.awaits.remove(k) {
                for (cid, _) in &waiting {
                    if let Some(ck) = self.index.get(cid).copied() {
                        let c = self.entry_mut(ck);
                        c.awaiting = c.awaiting.saturating_sub(1);
                    }
                }
                self.entry_mut(key).dependents.extend(waiting);
            }
        }
        {
            let e = self.entry_mut(key);
            e.unmet = unmet;
            e.awaiting = awaiting;
        }
        if awaiting > 0 {
            if let Some(t) = await_timeout {
                self.schedule(now + t, Timer::AwaitTimeout(id));
            }
        }
        if let Some(f) = spec.fixed_start {
            let lead = self.hold_lead(pool, f.tolerance);
            self.schedule(f.at - lead, Timer::HoldAt(id));
            self.schedule(f.at, Timer::StartAt(id));
            self.schedule(f.at + f.tolerance, Timer::MissAt(id));
        }
        if let Some(st) = unsatisfied {
            self.finish(
                key,
                TaskStatus::Skipped,
                Some(format!("dependency ended as {st}").into()),
            );
            return;
        }
        if attempt > 0 && unmet == 0 && self.entry(key).fixed.is_none() {
            // Recovered task that already ran: back off like a retry.
            self.schedule_retry(now, key, None);
            return;
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
                let (pool, kind) = {
                    let e = self.entry(l.key);
                    (e.pool, e.kind)
                };
                let s = ran.as_secs_f64();
                self.dur.entry((pool, kind)).or_default().observe(s);
                let p = &mut self.pools[pool as usize];
                p.stats.observe(s);
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
        let Some(l) = self.leases.get_mut(&lease) else {
            return false;
        };
        if let Some(ttl) = self.pools[l.pool as usize].cfg.lease_ttl {
            l.expires = now + ttl;
        }
        true
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
            let (id, k, at) = (e.id, e.key.clone(), e.attempt);
            self.push_status(id, k, TaskStatus::Retrying, at, Some(message), true);
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
            delay = Duration::from_secs_f64(delay.as_secs_f64() * self.rng.next_f64());
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
        if let Some(lease) = self.entry(key).lease {
            if let Some(l) = self.leases.remove(&lease) {
                self.end_attempt(&l);
                self.actions.push_back(Action::Abort {
                    lease,
                    id: l.id,
                    reason: AbortReason::Cancelled,
                });
            }
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

    pub fn live_tasks(&self) -> usize {
        self.index.len()
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
        if !self.pool_index.contains_key(&spec.template.pool) {
            return Err(format!("unknown pool '{}'", spec.template.pool));
        }
        if self.cfg.persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::RecurringUpsert {
                    spec: spec.clone(),
                })));
        }
        let name = spec.name.clone();
        let mut r = Recurring {
            spec,
            cron,
            tz,
            next: None,
            last_task: None,
        };
        let next = match next {
            Some(n) => Some(n),
            None => self.next_occurrence(&r, now),
        };
        r.next = next;
        self.recurring.insert(name.clone(), r);
        self.arm_recurring(now, &name);
        Ok(())
    }

    pub fn remove_recurring(&mut self, now: Time, name: &str) -> bool {
        self.advance(now);
        let removed = self.recurring.remove(name).is_some();
        if removed && self.cfg.persist {
            self.actions
                .push_back(Action::Persist(Box::new(Mutation::RecurringRemove {
                    name: name.into(),
                })));
        }
        removed
    }

    pub fn recurring_names(&self) -> Vec<Arc<str>> {
        self.recurring.keys().cloned().collect()
    }

    fn next_occurrence(&self, r: &Recurring<P>, after: Time) -> Option<Time> {
        use chrono::TimeZone;
        let wall = self.cfg.epoch_unix_nanos as i128 + after.0 as i128;
        let secs = wall.div_euclid(1_000_000_000) as i64;
        let nanos = wall.rem_euclid(1_000_000_000) as u32;
        let dt = r.tz.timestamp_opt(secs, nanos).single()?;
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
        let next = self.next_occurrence(self.recurring.get(&name).unwrap(), at);
        let r = self.recurring.get_mut(&name).unwrap();
        if task.is_some() {
            r.last_task = task;
        }
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
        for t in tasks {
            self.next_id = self.next_id.max(t.id.0 + 1);
            self.next_seq = self.next_seq.max(t.seq + 1);
            let Some(&pool) = self.pool_index.get(&t.spec.pool) else {
                continue;
            };
            if t.spec
                .resources
                .iter()
                .any(|(n, _)| !self.resource_index.contains_key(n))
            {
                continue;
            }
            self.insert(now, t.id, t.seq, t.arrival, t.spec, pool, t.attempt, None);
        }
        for (spec, next) in rec.recurring {
            let _ = self.add_recurring_inner(now, spec, next);
        }
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
                        if e.unmet == 0 {
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
                        let (at, seq) = (e.fixed.as_ref().unwrap().at, e.seq);
                        self.entry_mut(k).fixed.as_mut().unwrap().hold_pending = true;
                        self.pools[pool as usize].pending_holds.insert((at, seq), k);
                        self.mark_dirty(pool);
                    }
                }
            }
            Timer::StartAt(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    if self.entry(k).status == TaskStatus::Scheduled {
                        self.make_eligible(k);
                    }
                }
            }
            Timer::MissAt(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    self.on_miss(k);
                }
            }
            Timer::AwaitTimeout(id) => {
                if let Some(k) = self.index.get(&id).copied() {
                    if self.entry(k).awaiting > 0 && self.entry(k).status == TaskStatus::Blocked {
                        self.finish(
                            k,
                            TaskStatus::Skipped,
                            Some("timed out awaiting unknown dependency".into()),
                        );
                    }
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
        let (status, pool, policy) = {
            let e = self.entry(k);
            let Some(f) = e.fixed.as_ref() else { return };
            (e.status, e.pool, f.on_miss)
        };
        if matches!(status, TaskStatus::Running)
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
            self.unqueue(k);
            self.remove_pending(k);
            if self.entry(k).fixed.is_some() {
                self.release_hold(k);
                let e = self.entry(k);
                let (pool, id, rs) = (e.pool as usize, e.id, e.fixed.as_ref().unwrap().res_start);
                self.pools[pool].reservations.remove(id, rs);
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
            self.index.remove(&e.id);
            if let Some(key) = &e.key {
                if self.keys.get(key) == Some(&e.id) {
                    self.keys.remove(key);
                }
            }
            self.retain(e.id, e.key.clone(), status, self.now);
            self.push_status(e.id, e.key.clone(), status, e.attempt, message, true);
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
            if let Some(k) = key {
                if self.retained_keys.get(&k) == Some(&id) {
                    self.retained_keys.remove(&k);
                }
            }
        }
    }

    fn set_status(&mut self, k: Key, status: TaskStatus) {
        let emit = self.emit_transitions;
        let e = self.entry_mut(k);
        e.status = status;
        if emit {
            let (id, key, at) = (e.id, e.key.clone(), e.attempt);
            self.push_status(id, key, status, at, None, false);
        }
    }

    fn emit(&mut self, k: Key, status: TaskStatus, message: Option<Arc<str>>) {
        let emit = self.emit_transitions;
        let e = self.entry_mut(k);
        e.status = status;
        let (id, key, at) = (e.id, e.key.clone(), e.attempt);
        if emit {
            self.push_status(id, key, status, at, message, false);
        }
    }

    fn push_status(
        &mut self,
        id: TaskId,
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
            let Some((_, k)) = p.pending_holds.pop_first() else {
                break;
            };
            p.held += 1;
            let f = self.entry_mut(k).fixed.as_mut().unwrap();
            f.hold_pending = false;
            f.held = true;
        }
        // 2. Due fixed-start tasks (earliest first).
        let due: Vec<((Time, u64), Key)> = self.pools[pool as usize]
            .due
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect();
        for (dk, k) in due {
            let (held, skip_if) = {
                let f = self.entry(k).fixed.as_ref().unwrap();
                (f.held, f.skip_if_live)
            };
            if let Some(prev) = skip_if {
                if self.index.contains_key(&prev) {
                    self.finish(
                        k,
                        TaskStatus::Skipped,
                        Some("previous occurrence still running".into()),
                    );
                    continue;
                }
            }
            let p = &self.pools[pool as usize];
            if !held && p.free() == 0 {
                continue;
            }
            if !self.try_acquire(k) {
                self.schedule_refills(k);
                continue;
            }
            self.pools[pool as usize].due.remove(&dk);
            self.entry_mut(k).loc = Loc::None;
            if held {
                let p = &mut self.pools[pool as usize];
                p.held -= 1;
                self.entry_mut(k).fixed.as_mut().unwrap().held = false;
            }
            self.dispatch(now, pool, k);
        }
        // 3. Regular work by effective score.
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
            let at = now + Duration::from_secs_f64(missing / rate).max(Duration::from_micros(1));
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
                e.fixed.as_ref().map(|f| (f.at, f.tolerance)),
            )
        };
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
            if let Some((at, tol)) = fixed {
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
        self.push_status(id, key, TaskStatus::Running, attempt, None, false);
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
        let dt = now.diff_secs(p.last_eval);
        p.last_eval = now;
        let mut backlog = [0u64; Priority::COUNT];
        for (c, b) in backlog.iter_mut().enumerate() {
            *b = (p.lanes[c].len() + p.edf[c].len()) as u64;
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
            reserved: p
                .reservations
                .max_overlap(now, now + cfg.reservation_horizon),
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

    pub fn snapshot(&mut self, now: Time) -> Snapshot {
        self.advance(now);
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
            if let Some(f) = &e.fixed {
                if f.held {
                    held[e.pool as usize] += 1;
                }
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
                    e.unmet > 0 || e.fixed.is_some() || e.attempt > 0,
                    "blocked task {id} without unmet deps"
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
        }
        assert_eq!(self.total_pending, pending.iter().sum::<usize>());
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

#[inline]
fn edge_satisfied(cond: EdgeCond, parent: TaskStatus) -> bool {
    match cond {
        EdgeCond::OnSuccess => parent == TaskStatus::Succeeded,
        EdgeCond::OnComplete => parent.is_terminal(),
        EdgeCond::OnFailure => matches!(parent, TaskStatus::Failed | TaskStatus::DeadLettered),
    }
}
