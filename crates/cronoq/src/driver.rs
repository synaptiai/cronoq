//! The engine task: owns the core, executes its actions, feeds back completions.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use cronoq_core::{
    Action, Core, CoreConfig, LeaseId, Mutation, Outcome, PoolId, Recovered, RecurringSpec,
    Snapshot, StatusEvent, SubmitError, Submitted, TaskId, TaskRef, TaskSpec, TaskStatus,
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
use crate::executor::{Executor, HeartbeatFn, TaskContext};
use crate::scaler::Scaler;
use crate::scheduler::ResultRx;
use crate::store::{Store, TimeBase};
use crate::telemetry::{self, Metrics};

pub(crate) type Reply<T> = oneshot::Sender<T>;
type ResultTx<O> = oneshot::Sender<Result<Arc<O>, TaskFailure>>;
type SubmitReply<O> = Reply<Result<(Submitted, Option<ResultRx<O>>), Error>>;
type BatchReply<O> = Reply<Result<Vec<(Submitted, ResultRx<O>)>, Error>>;

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
        reply: Reply<Vec<Result<Submitted, Error>>>,
    },
    Attach {
        target: TaskRef,
        reply: Reply<Option<(TaskId, ResultRx<O>)>>,
    },
    Cancel {
        target: TaskRef,
        reply: Reply<bool>,
    },
    Status {
        target: TaskRef,
        reply: Reply<Option<TaskStatus>>,
    },
    Snapshot {
        reply: Reply<Snapshot>,
    },
    SetCapacity {
        pool: PoolId,
        capacity: u32,
    },
    AddRecurring {
        spec: RecurringSpec<P>,
        reply: Reply<Result<(), String>>,
    },
    RemoveRecurring {
        name: Arc<str>,
        reply: Reply<bool>,
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
    Heartbeat(LeaseId),
    Capacity {
        pool: PoolId,
        capacity: u32,
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

struct Blocked<P, O> {
    spec: TaskSpec<P>,
    reply: SubmitReply<O>,
    handle: bool,
}

pub(crate) struct Driver<P, O> {
    core: Core<P>,
    clock: Clock,
    pools: Vec<PoolRt<P, O>>,
    store: Arc<dyn Store<P, O>>,
    persists: bool,
    epoch: u16,
    base: TimeBase,
    events: broadcast::Sender<StatusEvent>,
    fb_tx: mpsc::UnboundedSender<Feedback<O>>,
    fb_rx: Option<mpsc::UnboundedReceiver<Feedback<O>>>,
    heartbeat: HeartbeatFn,
    running: FxHashMap<LeaseId, Running>,
    waiters: FxHashMap<TaskId, Vec<ResultTx<O>>>,
    outputs: FxHashMap<TaskId, Arc<O>>,
    blocked: VecDeque<Blocked<P, O>>,
    deferred: Vec<Box<dyn FnOnce() + Send>>,
    actions: Vec<Action<P>>,
    metrics: Metrics,
    next_gauge: Instant,
    shutdown: Option<Instant>,
    shutdown_replies: Vec<Reply<()>>,
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Driver<P, O> {
    pub fn new(
        cfg: CoreConfig,
        clock: Clock,
        pools: Vec<PoolRt<P, O>>,
        store: Arc<dyn Store<P, O>>,
        epoch: u16,
        events: broadcast::Sender<StatusEvent>,
        emit_transitions: bool,
    ) -> Self {
        let persists = cfg.persist;
        let base = TimeBase {
            unix_nanos_at_zero: cfg.epoch_unix_nanos,
        };
        let mut core = Core::new(cfg);
        core.set_emit_transitions(emit_transitions);
        let (fb_tx, fb_rx) = mpsc::unbounded_channel();
        let hb_tx = fb_tx.clone();
        Driver {
            core,
            clock,
            pools,
            store,
            persists,
            epoch,
            base,
            events,
            heartbeat: Arc::new(move |lease| {
                let _ = hb_tx.send(Feedback::Heartbeat(lease));
            }),
            fb_tx,
            fb_rx: Some(fb_rx),
            running: FxHashMap::default(),
            waiters: FxHashMap::default(),
            outputs: FxHashMap::default(),
            blocked: VecDeque::new(),
            deferred: Vec::new(),
            actions: Vec::new(),
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
        for (i, p) in self.pools.iter().enumerate() {
            let pool = PoolId(i as u16);
            let initial = self.core.pool_capacity(pool);
            let scaler = p.scaler.clone();
            let name = p.name.clone();
            let fb = self.fb_tx.clone();
            tokio::spawn(async move {
                let capacity = match scaler.scale_to(&name, initial).await {
                    Ok(c) => c,
                    // Externally managed pools start empty until capacity is reported.
                    Err(ScaleError::External) => 0,
                    Err(e) => {
                        tracing::warn!(pool = %name, error = %e, "initial scale request failed");
                        return;
                    }
                };
                let _ = fb.send(Feedback::Capacity { pool, capacity });
            });
        }
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<Cmd<P, O>>,
        mut cancel_rx: mpsc::UnboundedReceiver<TaskId>,
    ) -> Result<(), Error> {
        let mut fb_rx = self.fb_rx.take().expect("run once");
        let mut cmds = Vec::with_capacity(256);
        let mut fbs = Vec::with_capacity(1024);
        let mut cmds_open = true;
        let result = loop {
            if let Err(e) = self.process().await {
                break Err(e);
            }
            if let Some(deadline) = self.shutdown {
                if self.running.is_empty() || Instant::now() >= deadline {
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
            tokio::select! {
                biased;
                n = fb_rx.recv_many(&mut fbs, 1024) => {
                    if n > 0 {
                        let now = self.clock.now();
                        for f in fbs.drain(..) {
                            self.on_feedback(now, f);
                        }
                    }
                }
                n = rx.recv_many(&mut cmds, 256), if cmds_open => {
                    if n == 0 {
                        // Every Scheduler handle was dropped: stop now.
                        cmds_open = false;
                        self.shutdown = Some(Instant::now());
                    }
                    for c in cmds.drain(..) {
                        self.on_cmd(c);
                    }
                }
                Some(id) = cancel_rx.recv() => {
                    self.core.cancel(self.clock.now(), &TaskRef::Id(id));
                }
                _ = tokio::time::sleep_until(wake) => {
                    self.core.tick(self.clock.now());
                    if Instant::now() >= self.next_gauge {
                        let snap = self.core.snapshot(self.clock.now());
                        self.metrics.update(&snap);
                        self.next_gauge = Instant::now() + Duration::from_secs(1);
                        self.waiters.retain(|_, ws| {
                            ws.retain(|w| !w.is_closed());
                            !ws.is_empty()
                        });
                    }
                }
            }
        };
        for (_, r) in self.running.drain() {
            r.cancel.cancel();
            if let Some(a) = r.abort {
                a.abort();
            }
        }
        self.waiters.clear();
        for b in self.blocked.drain(..) {
            let _ = b.reply.send(Err(Error::Shutdown));
        }
        for r in self.shutdown_replies.drain(..) {
            let _ = r.send(());
        }
        if let Err(e) = &result {
            tracing::error!(error = %e, "cronoq scheduler stopped");
        }
        result
    }

    // -----------------------------------------------------------------------------------------

    fn on_cmd(&mut self, cmd: Cmd<P, O>) {
        let now = self.clock.now();
        match cmd {
            Cmd::Submit {
                spec,
                reply,
                handle,
            } => {
                if self.shutdown.is_some() {
                    let _ = reply.send(Err(Error::Shutdown));
                } else if !self.blocked.is_empty() {
                    // Preserve FIFO admission behind submitters waiting for space.
                    self.blocked.push_back(Blocked {
                        spec,
                        reply,
                        handle,
                    });
                } else {
                    self.submit(now, spec, reply, handle);
                }
            }
            Cmd::SubmitBatch { specs, reply } => {
                if self.shutdown.is_some() {
                    let _ = reply.send(Err(Error::Shutdown));
                    return;
                }
                let pools: Vec<_> = specs.iter().map(|s| s.pool.clone()).collect();
                match self.core.submit_batch(now, specs) {
                    Ok(subs) => {
                        let mut out = Vec::with_capacity(subs.len());
                        for (sub, pool) in subs.into_iter().zip(pools) {
                            if !sub.duplicate {
                                self.metrics.submitted(&pool);
                            }
                            out.push((sub, self.attach(sub.id)));
                        }
                        self.defer(move || {
                            let _ = reply.send(Ok(out));
                        });
                    }
                    Err((_, e)) => {
                        let _ = reply.send(Err(Error::Submit(e)));
                    }
                }
            }
            Cmd::EnqueueMany { specs, reply } => {
                let mut out = Vec::with_capacity(specs.len());
                for spec in specs {
                    if self.shutdown.is_some() {
                        out.push(Err(Error::Shutdown));
                        continue;
                    }
                    let pool = spec.pool.clone();
                    out.push(match self.core.submit(now, spec) {
                        Ok(sub) => {
                            if !sub.duplicate {
                                self.metrics.submitted(&pool);
                            }
                            Ok(sub)
                        }
                        Err(e) => Err(Error::Submit(e)),
                    });
                }
                self.defer(move || {
                    let _ = reply.send(out);
                });
            }
            Cmd::Attach { target, reply } => {
                let r = self.core.resolve(&target).map(|id| (id, self.attach(id)));
                let _ = reply.send(r);
            }
            Cmd::Cancel { target, reply } => {
                let _ = reply.send(self.core.cancel(now, &target));
            }
            Cmd::Status { target, reply } => {
                let _ = reply.send(self.core.status(&target));
            }
            Cmd::Snapshot { reply } => {
                let _ = reply.send(self.core.snapshot(now));
            }
            Cmd::SetCapacity { pool, capacity } => {
                if self.shutdown.is_none() {
                    self.core.set_capacity(now, pool, capacity);
                }
            }
            Cmd::AddRecurring { spec, reply } => {
                let _ = reply.send(self.core.add_recurring(now, spec));
            }
            Cmd::RemoveRecurring { name, reply } => {
                let _ = reply.send(self.core.remove_recurring(now, &name));
            }
            Cmd::Shutdown { grace, reply } => {
                let deadline = Instant::now() + grace;
                self.shutdown = Some(self.shutdown.map_or(deadline, |d| d.min(deadline)));
                // Drain: running attempts may finish, but nothing new starts.
                for i in 0..self.pools.len() {
                    self.core.set_capacity(now, PoolId(i as u16), 0);
                }
                self.shutdown_replies.push(reply);
                for b in self.blocked.drain(..) {
                    let _ = b.reply.send(Err(Error::Shutdown));
                }
            }
        }
    }

    fn submit(
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
                self.defer(move || {
                    let _ = reply.send(Ok((sub, rx)));
                });
            }
            Err((SubmitError::Full, spec)) => self.blocked.push_back(Blocked {
                spec,
                reply,
                handle,
            }),
            Err((e, _)) => {
                let _ = reply.send(Err(Error::Submit(e)));
            }
        }
    }

    /// Admit parked submitters in order while there is space.
    fn retry_blocked(&mut self) {
        let now = self.clock.now();
        while let Some(b) = self.blocked.pop_front() {
            if b.reply.is_closed() {
                continue; // the caller gave up
            }
            let pool = b.spec.pool.clone();
            match self.core.try_submit(now, b.spec) {
                Ok(sub) => {
                    if !sub.duplicate {
                        self.metrics.submitted(&pool);
                    }
                    let rx = b.handle.then(|| self.attach(sub.id));
                    let reply = b.reply;
                    self.defer(move || {
                        let _ = reply.send(Ok((sub, rx)));
                    });
                }
                Err((SubmitError::Full, spec)) => {
                    self.blocked.push_front(Blocked {
                        spec,
                        reply: b.reply,
                        handle: b.handle,
                    });
                    break;
                }
                Err((e, _)) => {
                    let _ = b.reply.send(Err(Error::Submit(e)));
                }
            }
        }
    }

    /// Register a result waiter; resolves immediately for already-terminal tasks.
    fn attach(&mut self, id: TaskId) -> ResultRx<O> {
        let (tx, rx) = oneshot::channel();
        match self.core.status(&TaskRef::Id(id)) {
            Some(st) if st.is_terminal() => {
                let message: Arc<str> = if st == TaskStatus::Succeeded {
                    "output no longer available".into()
                } else {
                    "task already finished".into()
                };
                let _ = tx.send(Err(TaskFailure::Ended {
                    status: st,
                    message,
                }));
            }
            _ => {
                let ws = self.waiters.entry(id).or_default();
                // Drop waiters whose handles were abandoned (e.g. a timed-out `Wait` RPC).
                ws.retain(|w| !w.is_closed());
                ws.push(tx);
            }
        }
        rx
    }

    fn defer(&mut self, f: impl FnOnce() + Send + 'static) {
        if self.persists {
            self.deferred.push(Box::new(f));
        } else {
            f();
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
            Feedback::Heartbeat(lease) => {
                self.core.heartbeat(now, lease);
            }
            Feedback::Capacity { pool, capacity } => {
                if self.shutdown.is_none() {
                    self.core.set_capacity(now, pool, capacity);
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------

    async fn process(&mut self) -> Result<(), Error> {
        loop {
            self.core.drain_actions(&mut self.actions);
            if self.actions.is_empty() && self.deferred.is_empty() {
                return Ok(());
            }
            let mut actions = std::mem::take(&mut self.actions);
            if self.persists {
                self.commit(&mut actions).await?;
            }
            for d in self.deferred.drain(..) {
                d();
            }
            for a in actions.drain(..) {
                self.execute(a);
            }
            self.actions = actions;
        }
    }

    async fn commit(&mut self, actions: &mut Vec<Action<P>>) -> Result<(), Error> {
        let mut muts: Vec<Mutation<P>> = Vec::new();
        let mut rest = Vec::with_capacity(actions.len());
        for a in actions.drain(..) {
            match a {
                Action::Persist(m) => muts.push(*m),
                other => rest.push(other),
            }
        }
        *actions = rest;
        if muts.is_empty() {
            return Ok(());
        }
        let outputs: Vec<(TaskId, &O)> = muts
            .iter()
            .filter_map(|m| match m {
                Mutation::Status {
                    id,
                    status: TaskStatus::Succeeded,
                    ..
                } => self.outputs.get(id).map(|o| (*id, &**o)),
                _ => None,
            })
            .collect();
        let mut delay = Duration::from_millis(50);
        for attempt in 0..5 {
            match self
                .store
                .commit(self.epoch, self.base, &muts, &outputs)
                .await
            {
                Ok(()) => return Ok(()),
                Err(StoreError::Fenced) => return Err(Error::Store(StoreError::Fenced)),
                Err(e) if attempt == 4 => return Err(Error::Store(e)),
                Err(e) => {
                    tracing::warn!(error = %e, attempt, "store commit failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
        unreachable!()
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
                    cancel: cancel.clone(),
                    heartbeat: self.heartbeat.clone(),
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
                let fut = pool.executor.execute(ctx, d.payload);
                let fb = self.fb_tx.clone();
                let lease = d.lease;
                let jh = tokio::spawn(
                    async move {
                        let result =
                            AssertUnwindSafe(fut)
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|p| {
                                    let msg = p
                                        .downcast_ref::<&str>()
                                        .map(|s| s.to_string())
                                        .or_else(|| p.downcast_ref::<String>().cloned())
                                        .unwrap_or_else(|| "executor panicked".into());
                                    Err(TaskError::permanent(format!("panic: {msg}")))
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
                    self.pools[r.pool as usize].executor.abort(lease);
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
            Action::ScaleTo { pool, desired } => {
                let p = &self.pools[pool.0 as usize];
                let (scaler, name, fb) = (p.scaler.clone(), p.name.clone(), self.fb_tx.clone());
                tokio::spawn(async move {
                    match scaler.scale_to(&name, desired).await {
                        Ok(capacity) => {
                            let _ = fb.send(Feedback::Capacity { pool, capacity });
                        }
                        Err(ScaleError::External) => {}
                        Err(e) => {
                            tracing::warn!(pool = %name, desired, error = %e, "scale request failed")
                        }
                    }
                });
            }
            Action::Status(e) => {
                if e.status.is_terminal() {
                    let output = self.outputs.remove(&e.id);
                    self.metrics
                        .task_finished(&self.pools[e.pool.0 as usize].name, e.status);
                    if let Some(ws) = self.waiters.remove(&e.id) {
                        for w in ws {
                            let r = match (&output, e.status) {
                                (Some(o), TaskStatus::Succeeded) => Ok(o.clone()),
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
}
