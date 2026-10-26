//! The engine task: owns the core, executes its actions, feeds back completions.
//!
//! With a persistent store, actions flow through a commit pipeline: the mutations produced by
//! one engine step are committed as a batch *in the background* while the engine keeps
//! processing feedback, heartbeats and commands. Everything that depends on durability —
//! replies to submitters, dispatches, terminal status events — is held with its batch and
//! released, in order, once that batch has committed. Aborts and scale requests run
//! immediately.

use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cronoq_core::{
    Action, Core, CoreConfig, LeaseId, Mutation, Outcome, PoolId, Priority, Recovered,
    RecurringSpec, Snapshot, StatusEvent, SubmitError, Submitted, TaskId, TaskRef, TaskSpec,
    TaskStatus,
};
use futures_util::FutureExt;
use rustc_hash::FxHashMap;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::clock::Clock;
use crate::error::{Error, ScaleError, StoreError, TaskError, TaskFailure};
use crate::executor::{BoxFuture, Executor, HeartbeatFn, TaskContext};
use crate::scaler::Scaler;
use crate::scheduler::ResultRx;
use crate::store::{Store, TimeBase};
use crate::telemetry::{self, Metrics};

pub(crate) type Reply<T> = oneshot::Sender<T>;
type ResultTx<O> = oneshot::Sender<Result<Arc<O>, TaskFailure>>;
type SubmitReply<O> = Reply<Result<(Submitted, Option<ResultRx<O>>), Error>>;
type BatchReply<O> = Reply<Result<Vec<(Submitted, ResultRx<O>)>, Error>>;
type ManyReply = Reply<Vec<Result<Submitted, Error>>>;
/// Reply released once its batch is durable (`None`), or failed with the fatal error.
type Deferred = Box<dyn FnOnce(Option<&Error>) + Send>;

pub(crate) enum Cmd<P, O> {
    Submit {
        spec: TaskSpec<P>,
        reply: SubmitReply<O>,
        handle: bool,
    },
    SubmitBatch {
        specs: Vec<TaskSpec<P>>,
        reply: BatchReply<O>,
    },
    EnqueueMany {
        specs: Vec<TaskSpec<P>>,
        reply: ManyReply,
    },
    Attach {
        target: TaskRef,
        reply: Reply<Option<(TaskId, ResultRx<O>)>>,
    },
    Cancel {
        target: TaskRef,
        reply: Reply<Result<bool, Error>>,
    },
    Status {
        target: TaskRef,
        reply: Reply<Option<TaskStatus>>,
    },
    ListTasks {
        pools: Vec<Arc<str>>,
        statuses: Vec<TaskStatus>,
        after: Option<TaskId>,
        limit: usize,
        reply: Reply<Vec<cronoq_core::TaskSummary>>,
    },
    ListRecurring {
        reply: Reply<Vec<cronoq_core::RecurringInfo>>,
    },
    Snapshot {
        reply: Reply<Snapshot>,
    },
    SetCapacity {
        pool: PoolId,
        capacity: u32,
    },
    SetGroupCapacity {
        pool: PoolId,
        labels: Vec<(Arc<str>, Arc<str>)>,
        capacity: u32,
    },
    AddRecurring {
        spec: RecurringSpec<P>,
        reply: Reply<Result<(), Error>>,
    },
    RemoveRecurring {
        name: Arc<str>,
        reply: Reply<Result<bool, Error>>,
    },
    Shutdown {
        grace: Duration,
        reply: Reply<()>,
    },
}

enum Feedback<O> {
    Done {
        lease: LeaseId,
        result: Result<O, TaskError>,
    },
    Heartbeat(LeaseId, Arc<AtomicBool>),
    Capacity {
        pool: PoolId,
        capacity: u32,
        /// Responses older than the newest applied one are ignored.
        seq: u64,
        /// The answer to the request made at start.
        initial: bool,
    },
}

pub(crate) struct PoolRt<P, O> {
    pub name: Arc<str>,
    pub executor: Arc<dyn Executor<P, O>>,
    pub scaler: Arc<dyn Scaler>,
}

struct Running {
    id: TaskId,
    pool: u16,
    kind: Arc<str>,
    started: Instant,
    cancel: CancellationToken,
    abort: Option<AbortHandle>,
}

/// Results of an `enqueue_many` whose items may be admitted at different times.
struct ManyGroup {
    results: Vec<Option<Result<Submitted, Error>>>,
    remaining: usize,
    reply: Option<ManyReply>,
}

/// A submission waiting for queue space (`OverflowPolicy::Wait`).
enum Blocked<P, O> {
    One {
        spec: TaskSpec<P>,
        reply: SubmitReply<O>,
        handle: bool,
    },
    Many {
        spec: TaskSpec<P>,
        slot: usize,
        group: Arc<Mutex<ManyGroup>>,
    },
    Batch {
        specs: Vec<TaskSpec<P>>,
        reply: BatchReply<O>,
    },
}

impl<P, O> Blocked<P, O> {
    /// FIFO is kept per (pool, class): a waiter only queues behind waiters it competes with.
    fn queues_behind(&self, pool: &str, class: Priority) -> bool {
        let same = |s: &TaskSpec<P>| &*s.pool == pool && s.priority == class;
        match self {
            Blocked::One { spec, .. } | Blocked::Many { spec, .. } => same(spec),
            Blocked::Batch { specs, .. } => specs.iter().any(same),
        }
    }
    fn abandoned(&self) -> bool {
        match self {
            Blocked::One { reply, .. } => reply.is_closed(),
            Blocked::Many { group, .. } => group
                .lock()
                .unwrap()
                .reply
                .as_ref()
                .is_none_or(|r| r.is_closed()),
            Blocked::Batch { reply, .. } => reply.is_closed(),
        }
    }
    fn fail(self, e: Error) {
        match self {
            Blocked::One { reply, .. } => {
                let _ = reply.send(Err(e));
            }
            Blocked::Many { slot, group, .. } => {
                if let Some((reply, results)) = ManyGroup::fill(&group, slot, Err(e)) {
                    let _ = reply.send(results);
                }
            }
            Blocked::Batch { reply, .. } => {
                let _ = reply.send(Err(e));
            }
        }
    }
}

impl ManyGroup {
    /// Record one result; once every slot is filled, returns the reply and all results.
    #[allow(clippy::type_complexity)]
    fn fill(
        group: &Arc<Mutex<ManyGroup>>,
        slot: usize,
        r: Result<Submitted, Error>,
    ) -> Option<(ManyReply, Vec<Result<Submitted, Error>>)> {
        let mut g = group.lock().unwrap();
        if g.results[slot].is_none() {
            g.results[slot] = Some(r);
            g.remaining -= 1;
        }
        if g.remaining > 0 {
            return None;
        }
        let reply = g.reply.take()?;
        let results = g.results.iter_mut().map(|r| r.take().unwrap()).collect();
        Some((reply, results))
    }
}

/// Non-persist actions and replies of one engine step, released once its batch is durable.
struct Held<P> {
    actions: Vec<Action<P>>,
    deferred: Vec<Deferred>,
}

/// A commit batch (one or more engine steps), possibly being retried.
struct Batch<P, O> {
    muts: Arc<Vec<Mutation<P>>>,
    outputs: Arc<Vec<(TaskId, Arc<O>)>>,
    held: Vec<Held<P>>,
}

type CommitFuture = Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send>>;

/// Commit pipeline tuning.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PipelineConfig {
    pub max_store_outage: Duration,
    pub drop_grace: Duration,
    pub output_cache: usize,
}

/// Retries of `StoreError::Other` before giving up.
const MAX_OTHER_FAILURES: u32 = 5;
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Feedback items handled per loop iteration (keeps commands and timers responsive).
const FEEDBACK_BATCH: usize = 1024;

enum Event {
    Feedback,
    Commands(usize),
    Cancel(TaskId),
    Committed(Result<(), StoreError>),
    Timer,
}

pub(crate) struct Driver<P, O> {
    core: Core<P>,
    clock: Clock,
    pools: Vec<PoolRt<P, O>>,
    store: Arc<dyn Store<P, O>>,
    persists: bool,
    epoch: u16,
    base: TimeBase,
    cfg: PipelineConfig,
    events: broadcast::Sender<StatusEvent>,
    fb_tx: mpsc::UnboundedSender<Feedback<O>>,
    fb_rx: Option<mpsc::UnboundedReceiver<Feedback<O>>>,
    heartbeat: HeartbeatFn,
    running: FxHashMap<LeaseId, Running>,
    waiters: FxHashMap<TaskId, Vec<ResultTx<O>>>,
    /// Outputs of tasks that succeeded in the engine but whose terminal event is not yet
    /// released (held for durability).
    outputs: FxHashMap<TaskId, Arc<O>>,
    /// Recently released outputs, and the messages of tasks that ended otherwise, for handles
    /// attached after completion (bounded by `output_cache`).
    recent: FxHashMap<TaskId, Result<Arc<O>, Arc<str>>>,
    recent_order: VecDeque<TaskId>,
    blocked: VecDeque<Blocked<P, O>>,
    deferred: Vec<Deferred>,
    actions: Vec<Action<P>>,
    /// Batches waiting to be committed, in order.
    queued: VecDeque<Batch<P, O>>,
    /// The batch being committed (or waiting to be retried).
    inflight: Option<Batch<P, O>>,
    commit: Option<CommitFuture>,
    retry_at: Option<Instant>,
    outage_since: Option<Instant>,
    other_failures: u32,
    scale_seq: Vec<u64>,
    scale_applied: Vec<u64>,
    /// Pools whose unlabeled capacity was reported (`set_capacity`) since start.
    reported: Vec<bool>,
    metrics: Metrics,
    next_gauge: Instant,
    shutdown: Option<Instant>,
    shutdown_replies: Vec<Reply<()>>,
}

fn panic_message(p: Box<dyn Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".into())
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Driver<P, O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: CoreConfig,
        clock: Clock,
        pools: Vec<PoolRt<P, O>>,
        store: Arc<dyn Store<P, O>>,
        epoch: u16,
        events: broadcast::Sender<StatusEvent>,
        emit_transitions: bool,
        pipeline: PipelineConfig,
    ) -> Self {
        let persists = cfg.persist;
        let base = TimeBase {
            unix_nanos_at_zero: cfg.epoch_unix_nanos,
        };
        let mut core = Core::new(cfg);
        core.set_emit_transitions(emit_transitions);
        let (fb_tx, fb_rx) = mpsc::unbounded_channel();
        let hb_tx = fb_tx.clone();
        let n = pools.len();
        Driver {
            core,
            clock,
            pools,
            store,
            persists,
            epoch,
            base,
            cfg: pipeline,
            events,
            heartbeat: Arc::new(move |lease, pending| {
                let _ = hb_tx.send(Feedback::Heartbeat(lease, pending));
            }),
            fb_tx,
            fb_rx: Some(fb_rx),
            running: FxHashMap::default(),
            waiters: FxHashMap::default(),
            outputs: FxHashMap::default(),
            recent: FxHashMap::default(),
            recent_order: VecDeque::new(),
            blocked: VecDeque::new(),
            deferred: Vec::new(),
            actions: Vec::new(),
            queued: VecDeque::new(),
            inflight: None,
            commit: None,
            retry_at: None,
            outage_since: None,
            other_failures: 0,
            scale_seq: vec![0; n],
            scale_applied: vec![0; n],
            reported: vec![false; n],
            metrics: Metrics::new(),
            next_gauge: Instant::now(),
            shutdown: None,
            shutdown_replies: Vec::new(),
        }
    }

    pub fn recover(&mut self, rec: Recovered<P>) {
        let now = self.clock.now();
        self.core.recover(now, rec);
    }

    /// Ask every pool's scaler for its initial capacity.
    pub fn start_pools(&mut self) {
        for i in 0..self.pools.len() {
            let pool = PoolId(i as u16);
            let initial = self.core.pool_capacity(pool);
            self.request_scale(pool, initial, true);
        }
    }

    /// Send a scale request; the response is applied only if no newer one was applied first.
    fn request_scale(&mut self, pool: PoolId, desired: u32, initial: bool) {
        let i = pool.0 as usize;
        self.scale_seq[i] += 1;
        let seq = self.scale_seq[i];
        let p = &self.pools[i];
        let (scaler, name, fb) = (p.scaler.clone(), p.name.clone(), self.fb_tx.clone());
        tokio::spawn(async move {
            let r = AssertUnwindSafe(scaler.scale_to(&name, desired))
                .catch_unwind()
                .await;
            let capacity = match r {
                Ok(Ok(c)) => c,
                // Externally managed pools start empty until capacity is reported.
                Ok(Err(ScaleError::External)) if initial => 0,
                Ok(Err(ScaleError::External)) => return,
                Ok(Err(e)) => {
                    tracing::warn!(pool = %name, desired, error = %e, "scale request failed");
                    return;
                }
                Err(p) => {
                    tracing::error!(pool = %name, desired, panic = %panic_message(p), "scaler panicked");
                    return;
                }
            };
            let _ = fb.send(Feedback::Capacity {
                pool,
                capacity,
                seq,
                initial,
            });
        });
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<Cmd<P, O>>,
        mut cancel_rx: mpsc::UnboundedReceiver<TaskId>,
    ) -> Result<(), Error> {
        let mut fb_rx = self.fb_rx.take().expect("run once");
        let mut cmds = Vec::with_capacity(256);
        let mut fbs = Vec::with_capacity(FEEDBACK_BATCH);
        let mut cmds_open = true;
        let result = loop {
            self.process();
            if let Err(e) = self.start_commit() {
                break Err(e);
            }
            if let Some(deadline) = self.shutdown {
                let drained = self.running.is_empty() && !self.commit_pending();
                if drained || Instant::now() >= deadline {
                    break Ok(());
                }
            }
            let mut wake = self.next_gauge;
            if let Some(t) = self.core.next_wakeup() {
                wake = wake.min(self.clock.instant(t));
            }
            if let Some(d) = self.shutdown {
                wake = wake.min(d);
            }
            if let (Some(t), None) = (self.retry_at, &self.commit) {
                wake = wake.min(t);
            }
            let mut commit = self.commit.take();
            let event = tokio::select! {
                n = fb_rx.recv_many(&mut fbs, FEEDBACK_BATCH) => {
                    if n == 0 { Event::Timer } else { Event::Feedback }
                }
                n = rx.recv_many(&mut cmds, 256), if cmds_open => Event::Commands(n),
                Some(id) = cancel_rx.recv() => Event::Cancel(id),
                r = async { commit.as_mut().expect("guarded").await }, if commit.is_some() => {
                    Event::Committed(r)
                }
                _ = tokio::time::sleep_until(wake) => Event::Timer,
            };
            if !matches!(event, Event::Committed(_)) {
                self.commit = commit;
            }
            let now = self.clock.now();
            match event {
                Event::Feedback => {
                    for f in fbs.drain(..) {
                        self.on_feedback(now, f);
                    }
                }
                Event::Commands(0) => {
                    // Every Scheduler handle was dropped: stop accepting work, let running
                    // attempts finish within the drop grace period.
                    cmds_open = false;
                    self.begin_shutdown(self.cfg.drop_grace);
                }
                Event::Commands(_) => {
                    // Cancels issued before these commands (e.g. `TaskHandle::cancel`, which
                    // uses its own channel) take effect first.
                    while let Ok(id) = cancel_rx.try_recv() {
                        self.core.cancel(now, &TaskRef::Id(id));
                    }
                    for c in cmds.drain(..) {
                        self.on_cmd(c);
                    }
                }
                Event::Cancel(id) => {
                    self.core.cancel(now, &TaskRef::Id(id));
                }
                Event::Committed(r) => {
                    if let Err(e) = self.on_committed(r) {
                        break Err(e);
                    }
                }
                Event::Timer => {
                    self.core.tick(now);
                }
            }
            if Instant::now() >= self.next_gauge {
                self.housekeeping();
            }
        };
        self.finish(result)
    }

    /// Periodic work: gauges, and pruning of abandoned waiters and submitters.
    fn housekeeping(&mut self) {
        let snap = self.core.snapshot(self.clock.now());
        self.metrics.update(&snap);
        self.next_gauge = Instant::now() + Duration::from_secs(1);
        self.waiters.retain(|_, ws| {
            ws.retain(|w| !w.is_closed());
            !ws.is_empty()
        });
        self.blocked.retain(|b| !b.abandoned());
    }

    fn finish(mut self, result: Result<(), Error>) -> Result<(), Error> {
        for (_, r) in self.running.drain() {
            r.cancel.cancel();
            if let Some(a) = r.abort {
                a.abort();
            }
        }
        // Replies whose mutations never became durable.
        let pending_err = match &result {
            Err(e) => e.clone(),
            Ok(()) => Error::Shutdown,
        };
        let mut held: Vec<Held<P>> = Vec::new();
        if let Some(b) = self.inflight.take() {
            held.extend(b.held);
        }
        for b in self.queued.drain(..) {
            held.extend(b.held);
        }
        for h in held {
            for d in h.deferred {
                d(Some(&pending_err));
            }
        }
        for d in self.deferred.drain(..) {
            d(Some(&pending_err));
        }
        self.waiters.clear();
        for b in self.blocked.drain(..) {
            b.fail(match &result {
                Err(e) => e.clone(),
                Ok(()) => Error::Shutdown,
            });
        }
        for r in self.shutdown_replies.drain(..) {
            let _ = r.send(());
        }
        if let Err(e) = &result {
            tracing::error!(error = %e, "cronoq scheduler stopped");
        }
        result
    }

    fn begin_shutdown(&mut self, grace: Duration) {
        let now = self.clock.now();
        let deadline = Instant::now() + grace;
        self.shutdown = Some(self.shutdown.map_or(deadline, |d| d.min(deadline)));
        // Drain: running attempts may finish, but nothing new starts.
        for i in 0..self.pools.len() {
            self.core.set_capacity(now, PoolId(i as u16), 0);
        }
        for b in self.blocked.drain(..) {
            b.fail(Error::Shutdown);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Commands
    // -----------------------------------------------------------------------------------------

    fn waits_behind(&self, spec: &TaskSpec<P>) -> bool {
        self.blocked
            .iter()
            .any(|b| b.queues_behind(&spec.pool, spec.priority))
    }

    fn on_cmd(&mut self, cmd: Cmd<P, O>) {
        let now = self.clock.now();
        let closed = self.shutdown.is_some();
        match cmd {
            Cmd::Submit {
                spec,
                reply,
                handle,
            } => {
                if closed {
                    let _ = reply.send(Err(Error::Shutdown));
                } else if self.waits_behind(&spec) {
                    self.blocked.push_back(Blocked::One {
                        spec,
                        reply,
                        handle,
                    });
                } else {
                    self.submit_one(now, spec, reply, handle);
                }
            }
            Cmd::SubmitBatch { specs, reply } => {
                if closed {
                    let _ = reply.send(Err(Error::Shutdown));
                } else if specs.iter().any(|s| self.waits_behind(s)) {
                    self.blocked.push_back(Blocked::Batch { specs, reply });
                } else if let Some(b) = self.submit_batch(now, specs, reply) {
                    self.blocked.push_back(b);
                }
            }
            Cmd::EnqueueMany { specs, reply } => {
                if closed {
                    let _ = reply.send(specs.iter().map(|_| Err(Error::Shutdown)).collect());
                    return;
                }
                let n = specs.len();
                let group = Arc::new(Mutex::new(ManyGroup {
                    results: (0..n).map(|_| None).collect(),
                    remaining: n,
                    reply: Some(reply),
                }));
                if n == 0 {
                    if let Some(reply) = group.lock().unwrap().reply.take() {
                        let _ = reply.send(Vec::new());
                    }
                    return;
                }
                for (slot, spec) in specs.into_iter().enumerate() {
                    if self.waits_behind(&spec) {
                        self.blocked.push_back(Blocked::Many {
                            spec,
                            slot,
                            group: group.clone(),
                        });
                    } else {
                        self.submit_many(now, spec, slot, &group);
                    }
                }
            }
            Cmd::Attach { target, reply } => {
                let r = self.core.resolve(&target).map(|id| (id, self.attach(id)));
                let _ = reply.send(r);
            }
            Cmd::Cancel { target, reply } => {
                let r = self.core.cancel(now, &target);
                self.defer(move |e| {
                    let _ = reply.send(match e {
                        None => Ok(r),
                        Some(e) => Err(e.clone()),
                    });
                });
            }
            Cmd::Status { target, reply } => {
                let _ = reply.send(self.core.status(&target));
            }
            Cmd::ListTasks {
                pools,
                statuses,
                after,
                limit,
                reply,
            } => {
                let _ = reply.send(self.core.list_tasks(&pools, &statuses, after, limit));
            }
            Cmd::ListRecurring { reply } => {
                let _ = reply.send(self.core.list_recurring());
            }
            Cmd::Snapshot { reply } => {
                let _ = reply.send(self.core.snapshot(now));
            }
            Cmd::SetCapacity { pool, capacity } => {
                if !closed {
                    self.reported[pool.0 as usize] = true;
                    self.core.set_capacity(now, pool, capacity);
                }
            }
            Cmd::SetGroupCapacity {
                pool,
                labels,
                capacity,
            } => {
                if !closed {
                    // Scaler answers set the unlabeled group only.
                    if labels.is_empty() {
                        self.reported[pool.0 as usize] = true;
                    }
                    self.core.set_group_capacity(now, pool, &labels, capacity);
                }
            }
            Cmd::AddRecurring { spec, reply } => match self.core.add_recurring(now, spec) {
                Ok(()) => self.defer(move |e| {
                    let _ = reply.send(match e {
                        None => Ok(()),
                        Some(e) => Err(e.clone()),
                    });
                }),
                Err(m) => {
                    let _ = reply.send(Err(Error::Config(m)));
                }
            },
            Cmd::RemoveRecurring { name, reply } => {
                let r = self.core.remove_recurring(now, &name);
                self.defer(move |e| {
                    let _ = reply.send(match e {
                        None => Ok(r),
                        Some(e) => Err(e.clone()),
                    });
                });
            }
            Cmd::Shutdown { grace, reply } => {
                self.begin_shutdown(grace);
                self.shutdown_replies.push(reply);
            }
        }
    }

    fn submit_one(
        &mut self,
        now: cronoq_core::Time,
        spec: TaskSpec<P>,
        reply: SubmitReply<O>,
        handle: bool,
    ) {
        let pool = spec.pool.clone();
        match self.core.try_submit(now, spec) {
            Ok(sub) => {
                if !sub.duplicate {
                    self.metrics.submitted(&pool);
                }
                let rx = handle.then(|| self.attach(sub.id));
                self.defer(move |e| {
                    let _ = reply.send(match e {
                        None => Ok((sub, rx)),
                        Some(e) => Err(e.clone()),
                    });
                });
            }
            Err((SubmitError::Full, spec)) if self.fits_when_empty(std::slice::from_ref(&spec)) => {
                self.blocked.push_back(Blocked::One {
                    spec,
                    reply,
                    handle,
                });
            }
            Err((SubmitError::Full, _)) => {
                let _ = reply.send(Err(Error::Submit(SubmitError::Overloaded)));
            }
            Err((e, _)) => {
                let _ = reply.send(Err(Error::Submit(e)));
            }
        }
    }

    /// Submit one item of an `enqueue_many`; returns `false` if it had to wait.
    fn submit_many(
        &mut self,
        now: cronoq_core::Time,
        spec: TaskSpec<P>,
        slot: usize,
        group: &Arc<Mutex<ManyGroup>>,
    ) -> bool {
        let pool = spec.pool.clone();
        let r = match self.core.try_submit(now, spec) {
            Ok(sub) => {
                if !sub.duplicate {
                    self.metrics.submitted(&pool);
                }
                Ok(sub)
            }
            Err((SubmitError::Full, spec)) if self.fits_when_empty(std::slice::from_ref(&spec)) => {
                self.blocked.push_back(Blocked::Many {
                    spec,
                    slot,
                    group: group.clone(),
                });
                return false;
            }
            Err((SubmitError::Full, _)) => Err(Error::Submit(SubmitError::Overloaded)),
            Err((e, _)) => Err(Error::Submit(e)),
        };
        if let Some((reply, results)) = ManyGroup::fill(group, slot, r) {
            // Released with the latest admitted item's batch, so all of them are durable.
            self.defer(move |e| {
                let results = match e {
                    None => results,
                    Some(e) => results.into_iter().map(|r| r.and(Err(e.clone()))).collect(),
                };
                let _ = reply.send(results);
            });
        }
        true
    }

    /// Submit an atomic batch; returns it back if it has to wait for space.
    fn submit_batch(
        &mut self,
        now: cronoq_core::Time,
        specs: Vec<TaskSpec<P>>,
        reply: BatchReply<O>,
    ) -> Option<Blocked<P, O>> {
        let pools: Vec<_> = specs.iter().map(|s| s.pool.clone()).collect();
        match self.core.submit_batch(now, specs.clone()) {
            Ok(subs) => {
                let mut out = Vec::with_capacity(subs.len());
                for (sub, pool) in subs.into_iter().zip(pools) {
                    if !sub.duplicate {
                        self.metrics.submitted(&pool);
                    }
                    out.push((sub, self.attach(sub.id)));
                }
                self.defer(move |e| {
                    let _ = reply.send(match e {
                        None => Ok(out),
                        Some(e) => Err(e.clone()),
                    });
                });
                None
            }
            Err((_, SubmitError::Full)) if self.fits_when_empty(&specs) => {
                Some(Blocked::Batch { specs, reply })
            }
            Err((_, SubmitError::Full)) => {
                // Larger than the queue bounds: waiting would never succeed.
                let _ = reply.send(Err(Error::Submit(SubmitError::Overloaded)));
                None
            }
            Err((_, e)) => {
                let _ = reply.send(Err(Error::Submit(e)));
                None
            }
        }
    }

    /// Whether `specs` could be admitted into empty queues (so waiting can succeed).
    fn fits_when_empty(&self, specs: &[TaskSpec<P>]) -> bool {
        let cfg = self.core.config();
        let within = |n: usize, lim: Option<usize>| lim.is_none_or(|l| n <= l);
        if !within(specs.len(), cfg.global_bounds.max_pending) {
            return false;
        }
        for c in Priority::ALL {
            let in_class = specs.iter().filter(|s| s.priority == c).count();
            if !within(in_class, cfg.global_bounds.max_pending_per_class[c.index()]) {
                return false;
            }
        }
        cfg.pools.iter().all(|p| {
            let mine: Vec<_> = specs.iter().filter(|s| s.pool == p.name).collect();
            within(mine.len(), p.bounds.max_pending)
                && Priority::ALL.iter().all(|c| {
                    let n = mine.iter().filter(|s| s.priority == *c).count();
                    within(n, p.bounds.max_pending_per_class[c.index()])
                })
        })
    }

    /// Admit waiting submitters in order while there is space.
    fn retry_blocked(&mut self) {
        let now = self.clock.now();
        let pending = std::mem::take(&mut self.blocked);
        let mut still: VecDeque<Blocked<P, O>> = VecDeque::new();
        for b in pending {
            if b.abandoned() {
                continue; // the caller gave up
            }
            // Keep FIFO with waiters still blocked on the same (pool, class).
            let behind = |still: &VecDeque<Blocked<P, O>>, s: &TaskSpec<P>| {
                still.iter().any(|w| w.queues_behind(&s.pool, s.priority))
            };
            let wait = match &b {
                Blocked::One { spec, .. } | Blocked::Many { spec, .. } => behind(&still, spec),
                Blocked::Batch { specs, .. } => specs.iter().any(|s| behind(&still, s)),
            };
            if wait {
                still.push_back(b);
                continue;
            }
            match b {
                Blocked::One {
                    spec,
                    reply,
                    handle,
                } => self.submit_one(now, spec, reply, handle),
                Blocked::Many { spec, slot, group } => {
                    self.submit_many(now, spec, slot, &group);
                }
                Blocked::Batch { specs, reply } => {
                    if let Some(b) = self.submit_batch(now, specs, reply) {
                        self.blocked.push_back(b);
                    }
                }
            }
            // Whatever blocked again landed in `self.blocked`; keep it in order.
            still.extend(self.blocked.drain(..));
        }
        self.blocked = still;
    }

    /// Register a result waiter; resolves immediately for already-terminal tasks.
    fn attach(&mut self, id: TaskId) -> ResultRx<O> {
        let (tx, rx) = oneshot::channel();
        match self.core.status(&TaskRef::Id(id)) {
            Some(TaskStatus::Succeeded) if !self.outputs.contains_key(&id) => {
                if let Some(Ok(o)) = self.recent.get(&id) {
                    let _ = tx.send(Ok(o.clone()));
                } else if self.persists {
                    let store = self.store.clone();
                    tokio::spawn(async move {
                        let r = AssertUnwindSafe(store.load_output(id)).catch_unwind().await;
                        let _ = tx.send(match r {
                            Ok(Ok(Some(o))) => Ok(Arc::new(o)),
                            _ => Err(TaskFailure::OutputUnavailable { id }),
                        });
                    });
                } else {
                    let _ = tx.send(Err(TaskFailure::OutputUnavailable { id }));
                }
            }
            Some(st) if st.is_terminal() && st != TaskStatus::Succeeded => {
                let message = match self.recent.get(&id) {
                    Some(Err(m)) => m.clone(),
                    _ => "task already finished".into(),
                };
                let _ = tx.send(Err(TaskFailure::Ended {
                    status: st,
                    message,
                }));
            }
            _ => {
                // Live, or succeeded with its terminal event still held for durability.
                let ws = self.waiters.entry(id).or_default();
                // Drop waiters whose handles were abandoned (e.g. a timed-out `Wait` RPC).
                ws.retain(|w| !w.is_closed());
                ws.push(tx);
            }
        }
        rx
    }

    fn defer(&mut self, f: impl FnOnce(Option<&Error>) + Send + 'static) {
        if self.persists {
            self.deferred.push(Box::new(f));
        } else {
            f(None);
        }
    }

    fn on_feedback(&mut self, now: cronoq_core::Time, f: Feedback<O>) {
        match f {
            Feedback::Done { lease, result } => {
                let Some(r) = self.running.remove(&lease) else {
                    return;
                };
                let secs = r.started.elapsed().as_secs_f64();
                let pool_name = self.pools[r.pool as usize].name.clone();
                self.metrics
                    .attempt_finished(&pool_name, &r.kind, secs, result.is_ok());
                let outcome = match result {
                    Ok(o) => {
                        self.outputs.insert(r.id, Arc::new(o));
                        Outcome::Success
                    }
                    Err(TaskError::Retryable {
                        message,
                        retry_after,
                    }) => Outcome::Failure {
                        retryable: true,
                        retry_after,
                        message,
                    },
                    Err(TaskError::Permanent { message }) => Outcome::Failure {
                        retryable: false,
                        retry_after: None,
                        message,
                    },
                    Err(TaskError::Cancelled) => Outcome::Cancelled,
                };
                if !self.core.complete(now, lease, outcome) {
                    self.outputs.remove(&r.id);
                }
            }
            Feedback::Heartbeat(lease, pending) => {
                pending.store(false, Ordering::Release);
                self.core.heartbeat(now, lease);
            }
            Feedback::Capacity {
                pool,
                capacity,
                seq,
                initial,
            } => {
                let i = pool.0 as usize;
                // The start-up answer (an external pool starts empty) can arrive after workers
                // reported their capacity; it describes the pool before them, so it is dropped
                // instead of erasing theirs.
                let superseded = initial && self.reported[i];
                if self.shutdown.is_none() && seq > self.scale_applied[i] && !superseded {
                    self.scale_applied[i] = seq;
                    self.core.set_capacity(now, pool, capacity);
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Actions and the commit pipeline
    // -----------------------------------------------------------------------------------------

    fn commit_pending(&self) -> bool {
        self.inflight.is_some() || !self.queued.is_empty()
    }

    /// Drain engine actions; run immediate ones, group durable ones into batches.
    fn process(&mut self) {
        loop {
            self.core.drain_actions(&mut self.actions);
            if self.actions.is_empty() && self.deferred.is_empty() {
                return;
            }
            let actions = std::mem::take(&mut self.actions);
            if !self.persists {
                for a in actions {
                    self.execute(a);
                }
                continue;
            }
            let mut muts = Vec::new();
            let mut held = Vec::new();
            let mut now_actions = Vec::new();
            for a in actions {
                match a {
                    Action::Persist(m) => muts.push(*m),
                    a
                    @ (Action::Abort { .. } | Action::ScaleTo { .. } | Action::SpaceAvailable) => {
                        now_actions.push(a)
                    }
                    a => held.push(a),
                }
            }
            let deferred = std::mem::take(&mut self.deferred);
            if muts.is_empty() && !self.commit_pending() {
                // Nothing to make durable and nothing ahead of it: release right away.
                for a in held {
                    self.execute(a);
                }
                for d in deferred {
                    d(None);
                }
            } else {
                let outputs: Vec<(TaskId, Arc<O>)> = muts
                    .iter()
                    .filter_map(|m| match m {
                        Mutation::Status {
                            id,
                            status: TaskStatus::Succeeded,
                            ..
                        } => self.outputs.get(id).map(|o| (*id, o.clone())),
                        _ => None,
                    })
                    .collect();
                self.queued.push_back(Batch {
                    muts: Arc::new(muts),
                    outputs: Arc::new(outputs),
                    held: vec![Held {
                        actions: held,
                        deferred,
                    }],
                });
            }
            for a in now_actions {
                self.execute(a);
            }
        }
    }

    /// Start committing the queued batches (merged into one), unless a commit is running
    /// or a retry is pending.
    fn start_commit(&mut self) -> Result<(), Error> {
        if self.commit.is_some() {
            return Ok(());
        }
        if let Some(t) = self.retry_at {
            if Instant::now() < t {
                return Ok(());
            }
            self.retry_at = None;
        }
        if self.inflight.is_none() {
            let Some(mut merged) = self.queued.pop_front() else {
                return Ok(());
            };
            // Group commit: everything queued so far goes in one transaction.
            if !self.queued.is_empty() {
                let mut muts = Arc::try_unwrap(merged.muts).unwrap_or_else(|a| (*a).clone());
                let mut outs = Arc::try_unwrap(merged.outputs).unwrap_or_else(|a| (*a).clone());
                for b in self.queued.drain(..) {
                    muts.extend(Arc::try_unwrap(b.muts).unwrap_or_else(|a| (*a).clone()));
                    outs.extend(Arc::try_unwrap(b.outputs).unwrap_or_else(|a| (*a).clone()));
                    merged.held.extend(b.held);
                }
                merged.muts = Arc::new(muts);
                merged.outputs = Arc::new(outs);
            }
            self.inflight = Some(merged);
        }
        let b = self.inflight.as_ref().expect("set above");
        let (store, epoch, base) = (self.store.clone(), self.epoch, self.base);
        let (muts, outputs) = (b.muts.clone(), b.outputs.clone());
        let fut: BoxFuture<Result<(), StoreError>> = Box::pin(async move {
            let outs: Vec<(TaskId, &O)> = outputs.iter().map(|(id, o)| (*id, &**o)).collect();
            AssertUnwindSafe(store.commit(epoch, base, &muts, &outs))
                .catch_unwind()
                .await
                .unwrap_or_else(|p| {
                    Err(StoreError::Other(format!(
                        "store panicked: {}",
                        panic_message(p)
                    )))
                })
        });
        self.commit = Some(fut);
        Ok(())
    }

    fn on_committed(&mut self, r: Result<(), StoreError>) -> Result<(), Error> {
        match r {
            Ok(()) => {
                self.outage_since = None;
                self.other_failures = 0;
                let b = self.inflight.take().expect("commit without batch");
                for h in b.held {
                    for a in h.actions {
                        self.execute(a);
                    }
                    for d in h.deferred {
                        d(None);
                    }
                }
                Ok(())
            }
            Err(StoreError::Fenced) => Err(Error::Store(StoreError::Fenced)),
            Err(e) => {
                let now = Instant::now();
                let since = *self.outage_since.get_or_insert(now);
                let transient = e.is_transient();
                if !transient {
                    self.other_failures += 1;
                }
                if (transient && now.duration_since(since) >= self.cfg.max_store_outage)
                    || (!transient && self.other_failures >= MAX_OTHER_FAILURES)
                {
                    return Err(Error::Store(e));
                }
                let n = if transient {
                    (now.duration_since(since).as_millis() / 100).min(10) as u32
                } else {
                    self.other_failures
                };
                let delay = (Duration::from_millis(50) * 2u32.saturating_pow(n)).min(MAX_BACKOFF);
                tracing::warn!(error = %e, retry_in = ?delay, "store commit failed; retrying");
                self.retry_at = Some(now + delay);
                Ok(())
            }
        }
    }

    fn execute(&mut self, a: Action<P>) {
        match a {
            Action::Dispatch(d) => {
                let pool = &self.pools[d.pool.0 as usize];
                let cancel = CancellationToken::new();
                let ctx = TaskContext {
                    id: d.id,
                    lease: d.lease,
                    attempt: d.attempt,
                    pool: pool.name.clone(),
                    kind: d.kind.clone(),
                    key: d.key.clone(),
                    deadline: d.deadline.map(|t| self.clock.to_system(t)),
                    timeout: d.timeout,
                    tags: d.tags.clone(),
                    group: d.group.clone(),
                    cancel: cancel.clone(),
                    heartbeat: self.heartbeat.clone(),
                    heartbeat_pending: Arc::new(AtomicBool::new(false)),
                };
                let span = telemetry::task_span(
                    d.id.0,
                    &pool.name,
                    &d.kind,
                    d.attempt,
                    d.tags
                        .iter()
                        .find(|(k, _)| &**k == "traceparent")
                        .map(|(_, v)| &**v),
                );
                let executor = pool.executor.clone();
                let payload = d.payload;
                // The synchronous part of `execute` may panic too: isolate it.
                let fut =
                    std::panic::catch_unwind(AssertUnwindSafe(|| executor.execute(ctx, payload)))
                        .unwrap_or_else(|p| {
                            let msg = format!("panic: {}", panic_message(p));
                            Box::pin(async move { Err(TaskError::permanent(msg)) })
                        });
                let fb = self.fb_tx.clone();
                let lease = d.lease;
                let jh = tokio::spawn(
                    async move {
                        let result =
                            AssertUnwindSafe(fut)
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|p| {
                                    Err(TaskError::permanent(format!(
                                        "panic: {}",
                                        panic_message(p)
                                    )))
                                });
                        let _ = fb.send(Feedback::Done { lease, result });
                    }
                    .instrument(span),
                );
                self.running.insert(
                    lease,
                    Running {
                        id: d.id,
                        pool: d.pool.0,
                        kind: d.kind,
                        started: Instant::now(),
                        cancel,
                        abort: Some(jh.abort_handle()),
                    },
                );
            }
            Action::Abort { lease, reason, .. } => {
                if let Some(r) = self.running.remove(&lease) {
                    let executor = self.pools[r.pool as usize].executor.clone();
                    if let Err(p) =
                        std::panic::catch_unwind(AssertUnwindSafe(|| executor.abort(lease)))
                    {
                        tracing::error!(panic = %panic_message(p), "Executor::abort panicked");
                    }
                    r.cancel.cancel();
                    if let Some(a) = r.abort {
                        a.abort();
                    }
                    let pool_name = self.pools[r.pool as usize].name.clone();
                    self.metrics.attempt_finished(
                        &pool_name,
                        &r.kind,
                        r.started.elapsed().as_secs_f64(),
                        false,
                    );
                    tracing::debug!(task = %r.id, ?reason, "attempt aborted");
                }
            }
            Action::ScaleTo { pool, desired } => self.request_scale(pool, desired, false),
            Action::Status(e) => {
                if e.status.is_terminal() {
                    let output = self.outputs.remove(&e.id);
                    self.metrics
                        .task_finished(&self.pools[e.pool.0 as usize].name, e.status);
                    match (&output, e.status) {
                        (Some(o), TaskStatus::Succeeded) => self.remember(e.id, Ok(o.clone())),
                        (_, TaskStatus::Succeeded) => {}
                        _ => {
                            if let Some(m) = &e.message {
                                self.remember(e.id, Err(m.clone()));
                            }
                        }
                    }
                    if let Some(ws) = self.waiters.remove(&e.id) {
                        for w in ws {
                            let r = match (&output, e.status) {
                                (Some(o), TaskStatus::Succeeded) => Ok(o.clone()),
                                (None, TaskStatus::Succeeded) => {
                                    Err(TaskFailure::OutputUnavailable { id: e.id })
                                }
                                _ => Err(TaskFailure::Ended {
                                    status: e.status,
                                    message: e
                                        .message
                                        .clone()
                                        .unwrap_or_else(|| e.status.name().into()),
                                }),
                            };
                            let _ = w.send(r);
                        }
                    }
                }
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(e);
                }
            }
            Action::SpaceAvailable => self.retry_blocked(),
            Action::Persist(_) => {}
        }
    }

    fn remember(&mut self, id: TaskId, r: Result<Arc<O>, Arc<str>>) {
        if self.cfg.output_cache == 0 {
            return;
        }
        if self.recent.insert(id, r).is_none() {
            self.recent_order.push_back(id);
        }
        while self.recent_order.len() > self.cfg.output_cache {
            if let Some(old) = self.recent_order.pop_front() {
                self.recent.remove(&old);
            }
        }
    }
}
