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
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::clock::Clock;
use crate::driver::{Cmd, Driver, PoolRt, Reply};
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
    pub fn workers(mut self, min: u32, initial: u32, max: u32) -> Self {
        self.cfg = self.cfg.workers(min, initial, max);
        self
    }
    pub fn executor(mut self, e: impl Executor<P, O>) -> Self {
        self.executor = Some(Arc::new(e));
        self
    }
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
    pub fn scaler(mut self, s: impl Scaler) -> Self {
        self.scaler = Arc::new(s);
        self
    }
    pub fn scaler_arc(mut self, s: Arc<dyn Scaler>) -> Self {
        self.scaler = s;
        self
    }
    pub fn autoscale(mut self, a: AutoscaleConfig) -> Self {
        self.cfg.autoscale = Some(a);
        self
    }
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
        }
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Builder<P, O> {
    pub fn pool(mut self, pool: Pool<P, O>) -> Self {
        self.pools.push(pool);
        self
    }
    pub fn resource(mut self, r: ResourceConfig) -> Self {
        self.core.resources.push(r);
        self
    }
    pub fn priority(mut self, p: PriorityConfig) -> Self {
        self.core.priority = p;
        self
    }
    pub fn urgency(mut self, u: UrgencyConfig) -> Self {
        self.core.urgency = u;
        self
    }
    pub fn store(mut self, s: impl Store<P, O>) -> Self {
        self.store = Arc::new(s);
        self
    }
    pub fn store_arc(mut self, s: Arc<dyn Store<P, O>>) -> Self {
        self.store = s;
        self
    }
    /// Broadcast every status transition on [`Scheduler::events`], not only terminal ones.
    pub fn emit_transitions(mut self, on: bool) -> Self {
        self.emit_transitions = on;
        self
    }
    /// Capacity of the submission channel.
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.queue = n.max(1);
        self
    }
    /// Direct access to engine-wide settings (bounds, retention, metrics window, ...).
    pub fn configure(mut self, f: impl FnOnce(&mut CoreConfig)) -> Self {
        f(&mut self.core);
        self
    }

    /// Start the scheduler on the current tokio runtime.
    pub async fn build(self) -> Result<Scheduler<P, O>, Error> {
        let Builder {
            mut core,
            pools,
            store,
            emit_transitions,
            queue,
            events,
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
        );
        if persists {
            let rec = store.recover(base).await?;
            driver.recover(rec);
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
}

/// Handle to a running scheduler. Cheap to clone; all methods are thread-safe.
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

    /// Submit a task and get a handle that resolves with its output.
    pub async fn submit(&self, task: impl Into<Task<P>>) -> Result<TaskHandle<O>, Error> {
        let spec = task.into().resolve(&self.inner.clock);
        let (sub, rx) = self
            .call(|reply| Cmd::Submit {
                spec,
                reply,
                handle: true,
            })
            .await??;
        Ok(TaskHandle {
            id: sub.id,
            duplicate: sub.duplicate,
            rx: rx.expect("handle requested"),
            cancel: self.inner.cancel.clone(),
        })
    }

    /// Submit without a result handle (lowest overhead).
    pub async fn enqueue(&self, task: impl Into<Task<P>>) -> Result<Submitted, Error> {
        let spec = task.into().resolve(&self.inner.clock);
        let (sub, _) = self
            .call(|reply| Cmd::Submit {
                spec,
                reply,
                handle: false,
            })
            .await??;
        Ok(sub)
    }

    /// Submit many tasks in one round trip (each admitted independently, in order).
    pub async fn enqueue_many(
        &self,
        tasks: Vec<Task<P>>,
    ) -> Result<Vec<Result<Submitted, Error>>, Error> {
        let specs = tasks
            .into_iter()
            .map(|t| t.resolve(&self.inner.clock))
            .collect();
        self.call(|reply| Cmd::EnqueueMany { specs, reply }).await
    }

    /// Submit tasks atomically; dependencies may reference keys within the batch.
    pub async fn submit_batch(&self, tasks: Vec<Task<P>>) -> Result<Vec<TaskHandle<O>>, Error> {
        let specs = tasks
            .into_iter()
            .map(|t| t.resolve(&self.inner.clock))
            .collect();
        let subs = self
            .call(|reply| Cmd::SubmitBatch { specs, reply })
            .await??;
        Ok(subs
            .into_iter()
            .map(|(sub, rx)| TaskHandle {
                id: sub.id,
                duplicate: sub.duplicate,
                rx,
                cancel: self.inner.cancel.clone(),
            })
            .collect())
    }

    /// Handle for an existing task (e.g. submitted elsewhere, or by key).
    pub async fn handle(&self, target: impl Into<TaskRef>) -> Result<Option<TaskHandle<O>>, Error> {
        let target = target.into();
        let r = self.call(|reply| Cmd::Attach { target, reply }).await?;
        Ok(r.map(|(id, rx)| TaskHandle {
            id,
            duplicate: true,
            rx,
            cancel: self.inner.cancel.clone(),
        }))
    }

    pub async fn cancel(&self, target: impl Into<TaskRef>) -> Result<bool, Error> {
        let target = target.into();
        self.call(|reply| Cmd::Cancel { target, reply }).await
    }

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

    pub async fn add_recurring(&self, r: Recurring<P>) -> Result<(), Error> {
        let spec = r.spec;
        self.call(|reply| Cmd::AddRecurring { spec, reply })
            .await?
            .map_err(Error::Config)
    }

    pub async fn remove_recurring(&self, name: &str) -> Result<bool, Error> {
        let name = name.into();
        self.call(|reply| Cmd::RemoveRecurring { name, reply })
            .await
    }

    /// Status events (terminal ones always; all with `emit_transitions`).
    pub fn events(&self) -> broadcast::Receiver<StatusEvent> {
        self.inner.events.subscribe()
    }

    pub fn pools(&self) -> &[Arc<str>] {
        &self.inner.pool_names
    }

    pub fn clock(&self) -> &Clock {
        &self.inner.clock
    }

    /// Stop accepting work, let running attempts finish for up to `grace`, then abort them.
    /// Queued tasks remain in the store (if any) for the next leader.
    pub async fn shutdown(&self, grace: Duration) -> Result<(), Error> {
        let _ = self.call(|reply| Cmd::Shutdown { grace, reply }).await;
        let join = self.inner.join.lock().unwrap().take();
        match join {
            Some(j) => j
                .await
                .map_err(|e| Error::Config(format!("driver panicked: {e}")))?,
            None => Ok(()),
        }
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
    pub fn id(&self) -> TaskId {
        self.id
    }
    /// The submission matched an existing key; this handle tracks that task.
    pub fn is_duplicate(&self) -> bool {
        self.duplicate
    }
    /// Request cancellation (fire and forget; the handle then resolves as cancelled).
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
