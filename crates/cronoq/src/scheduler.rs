//! Public scheduler API: builder, handle and task handles.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use cronoq_core::{
    AutoscaleConfig, CoreConfig, PoolConfig, PoolId, PriorityConfig, ResourceConfig, Snapshot,
    StatusEvent, Submitted, TaskId, TaskRef, TaskStatus, UrgencyConfig,
};
use tokio::sync::{OnceCell, broadcast, mpsc, oneshot};

use crate::clock::Clock;
use crate::driver::{Cmd, Driver, PipelineConfig, PoolRt, Reply};
use crate::error::{Error, TaskFailure};
use crate::executor::{Executor, FnExecutor, TaskContext};
use crate::scaler::{Scaler, SlotScaler};
use crate::store::{MemoryStore, Store, TimeBase};
use crate::task::{Recurring, Task};

/// A worker pool: engine configuration plus its executor and scaler.
pub struct Pool<P, O> {
    pub(crate) cfg: PoolConfig,
    pub(crate) executor: Option<Arc<dyn Executor<P, O>>>,
    pub(crate) scaler: Arc<dyn Scaler>,
}

impl<P, O> Pool<P, O> {
    /// Autoscaled pool (1..=10 workers by default).
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Pool {
            cfg: PoolConfig::new(name),
            executor: None,
            scaler: Arc::new(SlotScaler),
        }
    }
    /// Fixed number of workers, no autoscaling.
    pub fn fixed(name: impl Into<Arc<str>>, workers: u32) -> Self {
        Pool {
            cfg: PoolConfig::fixed(name, workers),
            executor: None,
            scaler: Arc::new(SlotScaler),
        }
    }
    /// Worker bounds: minimum, initial and maximum capacity.
    pub fn workers(mut self, min: u32, initial: u32, max: u32) -> Self {
        self.cfg = self.cfg.workers(min, initial, max);
        self
    }
    /// The executor that runs this pool's tasks (required).
    pub fn executor(mut self, e: impl Executor<P, O>) -> Self {
        self.executor = Some(Arc::new(e));
        self
    }
    /// Like [`Pool::executor`], with a shared executor.
    pub fn executor_arc(mut self, e: Arc<dyn Executor<P, O>>) -> Self {
        self.executor = Some(e);
        self
    }
    /// Executor from an async closure `|ctx, payload| async { ... }`.
    pub fn executor_fn<F, Fut>(self, f: F) -> Self
    where
        F: Fn(TaskContext, Arc<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, crate::TaskError>> + Send + 'static,
        P: 'static,
        O: 'static,
    {
        self.executor(FnExecutor::new(f))
    }
    /// How capacity changes are carried out (default: in-process slots).
    pub fn scaler(mut self, s: impl Scaler) -> Self {
        self.scaler = Arc::new(s);
        self
    }
    /// Like [`Pool::scaler`], with a shared scaler.
    pub fn scaler_arc(mut self, s: Arc<dyn Scaler>) -> Self {
        self.scaler = s;
        self
    }
    /// Enable queue-pressure autoscaling with these parameters.
    pub fn autoscale(mut self, a: AutoscaleConfig) -> Self {
        self.cfg.autoscale = Some(a);
        self
    }
    /// Disable autoscaling (capacity changes only via [`Scheduler::set_capacity`]).
    pub fn no_autoscale(mut self) -> Self {
        self.cfg.autoscale = None;
        self
    }
    /// Direct access to all engine settings for this pool.
    pub fn configure(mut self, f: impl FnOnce(&mut PoolConfig)) -> Self {
        f(&mut self.cfg);
        self
    }
}

/// Configures and starts a [`Scheduler`].
pub struct Builder<P, O> {
    core: CoreConfig,
    pools: Vec<Pool<P, O>>,
    store: Arc<dyn Store<P, O>>,
    emit_transitions: bool,
    queue: usize,
    events: usize,
    pipeline: PipelineConfig,
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Default for Builder<P, O> {
    fn default() -> Self {
        Builder {
            core: CoreConfig {
                pools: Vec::new(),
                ..Default::default()
            },
            pools: Vec::new(),
            store: Arc::new(MemoryStore),
            emit_transitions: false,
            queue: 8192,
            events: 4096,
            pipeline: PipelineConfig {
                max_store_outage: Duration::from_secs(300),
                drop_grace: Duration::from_secs(5),
                output_cache: 1024,
            },
        }
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Builder<P, O> {
    /// Add a pool (at least one is required; names must be unique).
    pub fn pool(mut self, pool: Pool<P, O>) -> Self {
        self.pools.push(pool);
        self
    }
    /// Add a shared resource (semaphore or token bucket) that tasks can require.
    pub fn resource(mut self, r: ResourceConfig) -> Self {
        self.core.resources.push(r);
        self
    }
    /// Per-class base scores and aging.
    pub fn priority(mut self, p: PriorityConfig) -> Self {
        self.core.priority = p;
        self
    }
    /// Deadline urgency curve.
    pub fn urgency(mut self, u: UrgencyConfig) -> Self {
        self.core.urgency = u;
        self
    }
    /// Persist state in `s` (default: memory only).
    pub fn store(mut self, s: impl Store<P, O>) -> Self {
        self.store = Arc::new(s);
        self
    }
    /// Like [`Builder::store`], with a shared store.
    pub fn store_arc(mut self, s: Arc<dyn Store<P, O>>) -> Self {
        self.store = s;
        self
    }
    /// Broadcast every status transition on [`Scheduler::events`], not only terminal ones.
    pub fn emit_transitions(mut self, on: bool) -> Self {
        self.emit_transitions = on;
        self
    }
    /// Capacity of the submission channel (default 8192).
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.queue = n.max(1);
        self
    }
    /// Capacity of the [`Scheduler::events`] broadcast buffer (default 4096). Slow
    /// subscribers lose the oldest events beyond it.
    pub fn events_capacity(mut self, n: usize) -> Self {
        self.events = n.max(1);
        self
    }
    /// How long a store may stay unavailable ([`StoreError::Unavailable`]) before the
    /// scheduler gives up (default 5 minutes). The engine keeps running meanwhile; replies
    /// and dispatches that need durability wait.
    ///
    /// [`StoreError::Unavailable`]: crate::StoreError::Unavailable
    pub fn max_store_outage(mut self, d: Duration) -> Self {
        self.pipeline.max_store_outage = d;
        self
    }
    /// Grace period for running attempts when every [`Scheduler`] handle is dropped without
    /// calling [`Scheduler::shutdown`] (default 5s). Queued tasks are not started.
    pub fn drop_grace(mut self, d: Duration) -> Self {
        self.pipeline.drop_grace = d;
        self
    }
    /// How many recent outputs are kept for handles attached after a task succeeded, e.g.
    /// duplicate submissions (default 1024; 0 disables). Stores can also serve older ones.
    pub fn output_cache(mut self, n: usize) -> Self {
        self.pipeline.output_cache = n;
        self
    }
    /// Direct access to engine-wide settings (bounds, retention, metrics window, ...).
    pub fn configure(mut self, f: impl FnOnce(&mut CoreConfig)) -> Self {
        f(&mut self.core);
        self
    }

    /// Start the scheduler on the current tokio runtime. With a persistent store this waits
    /// until the instance becomes leader, then recovers state.
    pub async fn build(self) -> Result<Scheduler<P, O>, Error> {
        let Builder {
            mut core,
            pools,
            store,
            emit_transitions,
            queue,
            events,
            pipeline,
        } = self;
        if pools.is_empty() {
            return Err(Error::Config("at least one pool is required".into()));
        }
        let clock = Clock::start();
        let base = TimeBase {
            unix_nanos_at_zero: clock.unix_nanos_at_zero(),
        };
        let mut rts = Vec::new();
        core.pools.clear();
        for p in pools {
            let executor = p
                .executor
                .ok_or_else(|| Error::Config(format!("pool '{}' has no executor", p.cfg.name)))?;
            if p.cfg.min_workers > p.cfg.max_workers {
                return Err(Error::Config(format!(
                    "pool '{}': min_workers > max_workers",
                    p.cfg.name
                )));
            }
            if p.cfg.name.contains('\0') {
                return Err(Error::Config("pool names must not contain NUL".into()));
            }
            rts.push(PoolRt {
                name: p.cfg.name.clone(),
                executor,
                scaler: p.scaler,
            });
            core.pools.push(p.cfg);
        }
        let mut names = std::collections::HashSet::new();
        for p in &core.pools {
            if !names.insert(p.name.clone()) {
                return Err(Error::Config(format!("duplicate pool '{}'", p.name)));
            }
        }
        core.epoch_unix_nanos = base.unix_nanos_at_zero;
        let persists = store.persists();
        let mut epoch = 0;
        if persists {
            epoch = store.acquire_leadership().await?;
            core.persist = true;
            core.id_epoch = epoch;
        }
        let (tx, rx) = mpsc::channel(queue);
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(events.max(1));
        let pool_names: Arc<[Arc<str>]> = core.pools.iter().map(|p| p.name.clone()).collect();
        let mut driver = Driver::new(
            core,
            clock,
            rts,
            store.clone(),
            epoch,
            events_tx.clone(),
            emit_transitions,
            pipeline,
        );
        if persists {
            match store.recover(base).await {
                Ok(rec) => driver.recover(rec),
                Err(e) => {
                    // Let a standby take over instead of holding leadership while broken.
                    store.release_leadership().await;
                    return Err(Error::Store(e));
                }
            }
        }
        driver.start_pools();
        let join = tokio::spawn(driver.run(rx, cancel_rx));
        Ok(Scheduler {
            inner: Arc::new(Inner {
                tx,
                cancel: cancel_tx,
                events: events_tx,
                clock,
                pool_names,
                join: std::sync::Mutex::new(Some(join)),
                result: OnceCell::new(),
            }),
        })
    }
}

struct Inner<P, O> {
    tx: mpsc::Sender<Cmd<P, O>>,
    cancel: mpsc::UnboundedSender<TaskId>,
    events: broadcast::Sender<StatusEvent>,
    clock: Clock,
    pool_names: Arc<[Arc<str>]>,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), Error>>>>,
    /// The driver's exit result, shared by every `shutdown` caller.
    result: OnceCell<Result<(), Error>>,
}

/// Handle to a running scheduler. Cheap to clone; all methods are thread-safe.
///
/// The scheduler runs until [`Scheduler::shutdown`] is called or every clone is dropped.
/// Dropping the last clone stops accepting work and gives running attempts
/// [`Builder::drop_grace`] to finish; [`TaskHandle`]s do not keep the scheduler alive.
pub struct Scheduler<P, O> {
    inner: Arc<Inner<P, O>>,
}

impl<P, O> Clone for Scheduler<P, O> {
    fn clone(&self) -> Self {
        Scheduler {
            inner: self.inner.clone(),
        }
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Scheduler<P, O> {
    /// Start configuring a scheduler.
    pub fn builder() -> Builder<P, O> {
        Builder::default()
    }

    async fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Cmd<P, O>) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(make(tx))
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)
    }

    fn task_handle(&self, sub: Submitted, rx: crate::scheduler::ResultRx<O>) -> TaskHandle<O> {
        TaskHandle {
            id: sub.id,
            duplicate: sub.duplicate,
            rx,
            cancel: self.inner.cancel.clone(),
        }
    }

    /// Submit a task and get a handle that resolves with its output. With a persistent
    /// store, returns once the submission is durable.
    pub async fn submit(&self, task: impl Into<Task<P>>) -> Result<TaskHandle<O>, Error> {
        let spec = task.into().resolve(&self.inner.clock)?;
        let (sub, rx) = self
            .call(|reply| Cmd::Submit {
                spec,
                reply,
                handle: true,
            })
            .await??;
        Ok(self.task_handle(sub, rx.expect("handle requested")))
    }

    /// Submit without a result handle (lowest overhead).
    pub async fn enqueue(&self, task: impl Into<Task<P>>) -> Result<Submitted, Error> {
        let spec = task.into().resolve(&self.inner.clock)?;
        let (sub, _) = self
            .call(|reply| Cmd::Submit {
                spec,
                reply,
                handle: false,
            })
            .await??;
        Ok(sub)
    }

    /// Submit many tasks in one round trip, each admitted independently and in order.
    /// Items for pools that are full with [`OverflowPolicy::Wait`] wait for space, like
    /// [`Scheduler::submit`]; the call returns once every item has been decided.
    ///
    /// [`OverflowPolicy::Wait`]: cronoq_core::OverflowPolicy::Wait
    pub async fn enqueue_many(
        &self,
        tasks: Vec<Task<P>>,
    ) -> Result<Vec<Result<Submitted, Error>>, Error> {
        let mut specs = Vec::with_capacity(tasks.len());
        let mut early: Vec<Option<Error>> = Vec::with_capacity(tasks.len());
        for t in tasks {
            match t.resolve(&self.inner.clock) {
                Ok(s) => {
                    specs.push(s);
                    early.push(None);
                }
                Err(e) => early.push(Some(e)),
            }
        }
        let mut admitted = self
            .call(|reply| Cmd::EnqueueMany { specs, reply })
            .await?
            .into_iter();
        Ok(early
            .into_iter()
            .map(|e| match e {
                Some(e) => Err(e),
                None => admitted.next().unwrap_or(Err(Error::Shutdown)),
            })
            .collect())
    }

    /// Submit tasks atomically; dependencies may reference keys within the batch. When a pool
    /// is full with [`OverflowPolicy::Wait`], the whole batch waits for space.
    ///
    /// [`OverflowPolicy::Wait`]: cronoq_core::OverflowPolicy::Wait
    pub async fn submit_batch(&self, tasks: Vec<Task<P>>) -> Result<Vec<TaskHandle<O>>, Error> {
        let specs = tasks
            .into_iter()
            .map(|t| t.resolve(&self.inner.clock))
            .collect::<Result<Vec<_>, _>>()?;
        let subs = self
            .call(|reply| Cmd::SubmitBatch { specs, reply })
            .await??;
        Ok(subs
            .into_iter()
            .map(|(sub, rx)| self.task_handle(sub, rx))
            .collect())
    }

    /// Handle for an existing task (e.g. submitted elsewhere, or by key), if the scheduler
    /// knows it (live or recently finished).
    pub async fn handle(&self, target: impl Into<TaskRef>) -> Result<Option<TaskHandle<O>>, Error> {
        let target = target.into();
        let r = self.call(|reply| Cmd::Attach { target, reply }).await?;
        Ok(r.map(|(id, rx)| {
            self.task_handle(
                Submitted {
                    id,
                    duplicate: false,
                },
                rx,
            )
        }))
    }

    /// Cancel a task; running attempts are aborted. Returns `false` if it was not live.
    /// With a persistent store, returns once the cancellation is durable.
    pub async fn cancel(&self, target: impl Into<TaskRef>) -> Result<bool, Error> {
        let target = target.into();
        self.call(|reply| Cmd::Cancel { target, reply }).await?
    }

    /// Current status of a task (live or recently finished).
    pub async fn status(&self, target: impl Into<TaskRef>) -> Result<Option<TaskStatus>, Error> {
        let target = target.into();
        self.call(|reply| Cmd::Status { target, reply }).await
    }

    /// Quality metrics over the metrics window, per pool.
    pub async fn snapshot(&self) -> Result<Snapshot, Error> {
        self.call(|reply| Cmd::Snapshot { reply }).await
    }

    /// Report capacity for pools whose workers are managed externally.
    pub async fn set_capacity(&self, pool: &str, capacity: u32) -> Result<(), Error> {
        let id = self.pool_id(pool)?;
        self.inner
            .tx
            .send(Cmd::SetCapacity { pool: id, capacity })
            .await
            .map_err(|_| Error::Shutdown)
    }

    /// Add or replace a recurring (cron) schedule. With a persistent store, returns once the
    /// schedule is durable.
    pub async fn add_recurring(&self, r: Recurring<P>) -> Result<(), Error> {
        if let Some(m) = r.invalid {
            return Err(Error::Config(m));
        }
        let spec = r.spec;
        self.call(|reply| Cmd::AddRecurring { spec, reply }).await?
    }

    /// Remove a recurring schedule. Returns `false` if it did not exist.
    pub async fn remove_recurring(&self, name: &str) -> Result<bool, Error> {
        let name = name.into();
        self.call(|reply| Cmd::RemoveRecurring { name, reply })
            .await?
    }

    /// Status events (terminal ones always; all with `emit_transitions`).
    pub fn events(&self) -> broadcast::Receiver<StatusEvent> {
        self.inner.events.subscribe()
    }

    /// Names of the configured pools, in order.
    pub fn pools(&self) -> &[Arc<str>] {
        &self.inner.pool_names
    }

    /// The scheduler's clock (engine time is Unix nanoseconds).
    pub fn clock(&self) -> &Clock {
        &self.inner.clock
    }

    /// Resolves when the scheduler has stopped (shutdown, or a fatal store error such as
    /// lost leadership).
    pub async fn stopped(&self) {
        self.inner.tx.closed().await
    }

    /// Stop accepting work, let running attempts finish for up to `grace`, then abort them.
    /// Queued tasks remain in the store (if any) for the next leader. Every caller gets the
    /// scheduler's exit result (e.g. a fatal store error).
    pub async fn shutdown(&self, grace: Duration) -> Result<(), Error> {
        let _ = self.call(|reply| Cmd::Shutdown { grace, reply }).await;
        self.inner
            .result
            .get_or_init(|| async {
                let join = self.inner.join.lock().unwrap().take();
                match join {
                    Some(j) => j
                        .await
                        .unwrap_or_else(|e| Err(Error::Internal(format!("driver panicked: {e}")))),
                    None => Ok(()),
                }
            })
            .await
            .clone()
    }

    fn pool_id(&self, pool: &str) -> Result<PoolId, Error> {
        self.inner
            .pool_names
            .iter()
            .position(|p| &**p == pool)
            .map(|i| PoolId(i as u16))
            .ok_or_else(|| Error::Submit(cronoq_core::SubmitError::UnknownPool(pool.into())))
    }
}

pub(crate) type ResultRx<O> = oneshot::Receiver<Result<Arc<O>, TaskFailure>>;

/// Resolves with the task's output once it reaches a terminal status.
///
/// Await it once; use [`Scheduler::handle`] to get further handles for the same task.
/// Dropping it does not cancel the task; use [`TaskHandle::cancel`].
pub struct TaskHandle<O> {
    id: TaskId,
    duplicate: bool,
    rx: ResultRx<O>,
    cancel: mpsc::UnboundedSender<TaskId>,
}

impl<O> std::fmt::Debug for TaskHandle<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("id", &self.id)
            .field("duplicate", &self.duplicate)
            .finish()
    }
}

impl<O> TaskHandle<O> {
    /// The task's id.
    pub fn id(&self) -> TaskId {
        self.id
    }
    /// The submission matched an existing key; this handle tracks that task.
    pub fn is_duplicate(&self) -> bool {
        self.duplicate
    }
    /// Request cancellation (fire and forget; the handle then resolves as cancelled). It
    /// takes effect before any scheduler call made after it from the same task or thread.
    pub fn cancel(&self) {
        let _ = self.cancel.send(self.id);
    }
}

impl<O> Future for TaskHandle<O> {
    type Output = Result<Arc<O>, TaskFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(TaskFailure::Shutdown)))
    }
}
