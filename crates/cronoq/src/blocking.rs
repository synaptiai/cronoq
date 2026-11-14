//! Synchronous facade for code that does not run on tokio.
//!
//! [`BlockingScheduler`] owns a private tokio runtime: multi-threaded with the
//! `rt-multi-thread` feature (on by default), otherwise a single-threaded one on a thread of
//! its own. Its methods block the
//! calling thread; they are safe to call from inside another tokio runtime too (the wait then
//! happens on a helper thread), although async code should prefer [`Scheduler`] directly.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cronoq_core::{Snapshot, StatusEvent, Submitted, TaskId, TaskRef, TaskStatus};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::broadcast;

use crate::error::{Error, TaskFailure};
use crate::scheduler::{Builder, Scheduler, TaskHandle};
use crate::task::{Recurring, Task};

/// Run `f` to completion on `rt`, from any context (including inside a tokio runtime, where
/// blocking the current thread with `block_on` would panic).
fn block_on<F>(rt: &Handle, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if Handle::try_current().is_ok() {
        std::thread::scope(|s| {
            s.spawn(|| rt.block_on(f))
                .join()
                .unwrap_or_else(|p| std::panic::resume_unwind(p))
        })
    } else {
        rt.block_on(f)
    }
}

/// Wakes the thread that polls a future in [`park_until`].
struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Poll `f` on this thread until it is ready or `deadline` passes. A task handle only waits on
/// a channel, so this needs no runtime: it works from inside a runtime, and after the
/// scheduler's runtime has shut down (whose timer would panic).
fn park_until<F: Future + Unpin>(mut f: F, deadline: Option<Instant>) -> Option<F::Output> {
    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    loop {
        if let std::task::Poll::Ready(v) = std::pin::Pin::new(&mut f).poll(&mut cx) {
            return Some(v);
        }
        match deadline {
            None => std::thread::park(),
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return None;
                }
                std::thread::park_timeout(d - now);
            }
        }
    }
}

/// The private runtime of a [`BlockingScheduler`].
struct Private {
    handle: Handle,
    /// The runtime itself (multi-threaded), or the thread that drives a single-threaded one.
    #[cfg(feature = "rt-multi-thread")]
    rt: Runtime,
    #[cfg(not(feature = "rt-multi-thread"))]
    stop: tokio::sync::oneshot::Sender<()>,
    #[cfg(not(feature = "rt-multi-thread"))]
    thread: std::thread::JoinHandle<()>,
}

impl Private {
    #[cfg(feature = "rt-multi-thread")]
    fn start() -> std::io::Result<Private> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("cronoq")
            .build()?;
        Ok(Private {
            handle: rt.handle().clone(),
            rt,
        })
    }

    /// One thread that drives a single-threaded runtime until it is told to stop.
    #[cfg(not(feature = "rt-multi-thread"))]
    fn start() -> std::io::Result<Private> {
        let rt: Runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let handle = rt.handle().clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("cronoq".into())
            .spawn(move || {
                let _ = rt.block_on(stopped);
                // Dropping the runtime here drops what is still spawned on it.
            })?;
        Ok(Private {
            handle,
            stop,
            thread,
        })
    }

    /// Stop the runtime. With `wait`, give its tasks a moment to end (not possible from
    /// inside another runtime, where blocking is not allowed).
    fn stop(self, wait: bool) {
        #[cfg(feature = "rt-multi-thread")]
        if wait {
            self.rt.shutdown_timeout(Duration::from_secs(1));
        } else {
            self.rt.shutdown_background();
        }
        #[cfg(not(feature = "rt-multi-thread"))]
        {
            let _ = self.stop.send(());
            if wait {
                let _ = self.thread.join();
            }
        }
    }
}

/// A scheduler that owns its own tokio runtime and exposes blocking methods.
pub struct BlockingScheduler<P, O> {
    rt: Option<Private>,
    inner: Scheduler<P, O>,
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Builder<P, O> {
    /// Build on a private runtime: multi-threaded with the `rt-multi-thread` feature (the
    /// default), otherwise single-threaded on one thread of its own, which is enough for the
    /// scheduler itself and for executors that do not block.
    pub fn build_blocking(self) -> Result<BlockingScheduler<P, O>, Error> {
        let rt = Private::start().map_err(|e| Error::Config(format!("runtime: {e}")))?;
        let inner = match block_on(&rt.handle, self.build()) {
            Ok(inner) => inner,
            Err(e) => {
                rt.stop(false);
                return Err(e);
            }
        };
        Ok(BlockingScheduler {
            rt: Some(rt),
            inner,
        })
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> BlockingScheduler<P, O> {
    /// Like [`Builder::build_blocking`], for a builder that has to be made on the runtime
    /// the scheduler will run on: `make` runs there. A store that opens connections, for
    /// example, must be opened on the runtime that will use them.
    pub fn start<F, Fut>(make: F) -> Result<BlockingScheduler<P, O>, Error>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Builder<P, O>, Error>> + Send,
    {
        let rt = Private::start().map_err(|e| Error::Config(format!("runtime: {e}")))?;
        let built = block_on(&rt.handle, async move { make().await?.build().await });
        match built {
            Ok(inner) => Ok(BlockingScheduler {
                rt: Some(rt),
                inner,
            }),
            Err(e) => {
                rt.stop(false);
                Err(e)
            }
        }
    }

    fn handle(&self) -> &Handle {
        &self.rt.as_ref().expect("runtime alive").handle
    }
    fn wrap(&self, h: TaskHandle<O>) -> BlockingHandle<O> {
        BlockingHandle { handle: Some(h) }
    }
    /// Blocking [`Scheduler::submit`].
    pub fn submit(&self, task: impl Into<Task<P>>) -> Result<BlockingHandle<O>, Error> {
        let task = task.into();
        let h = block_on(self.handle(), self.inner.submit(task))?;
        Ok(self.wrap(h))
    }
    /// Blocking [`Scheduler::enqueue`].
    pub fn enqueue(&self, task: impl Into<Task<P>>) -> Result<Submitted, Error> {
        let task = task.into();
        block_on(self.handle(), self.inner.enqueue(task))
    }
    /// Blocking [`Scheduler::enqueue_many`].
    pub fn enqueue_many(
        &self,
        tasks: Vec<Task<P>>,
    ) -> Result<Vec<Result<Submitted, Error>>, Error> {
        block_on(self.handle(), self.inner.enqueue_many(tasks))
    }
    /// Blocking [`Scheduler::submit_batch`].
    pub fn submit_batch(&self, tasks: Vec<Task<P>>) -> Result<Vec<BlockingHandle<O>>, Error> {
        let hs = block_on(self.handle(), self.inner.submit_batch(tasks))?;
        Ok(hs.into_iter().map(|h| self.wrap(h)).collect())
    }
    /// Blocking [`Scheduler::handle`].
    pub fn task_handle(
        &self,
        target: impl Into<TaskRef>,
    ) -> Result<Option<BlockingHandle<O>>, Error> {
        let target = target.into();
        let h = block_on(self.handle(), self.inner.handle(target))?;
        Ok(h.map(|h| self.wrap(h)))
    }
    /// Blocking [`Scheduler::cancel`].
    pub fn cancel(&self, target: impl Into<TaskRef>) -> Result<bool, Error> {
        let target = target.into();
        block_on(self.handle(), self.inner.cancel(target))
    }
    /// Blocking [`Scheduler::update`].
    pub fn update(
        &self,
        target: impl Into<TaskRef>,
        update: crate::Update,
    ) -> Result<crate::Updated, Error> {
        let target = target.into();
        block_on(self.handle(), self.inner.update(target, update))
    }
    /// Blocking [`Scheduler::status`].
    pub fn status(&self, target: impl Into<TaskRef>) -> Result<Option<TaskStatus>, Error> {
        let target = target.into();
        block_on(self.handle(), self.inner.status(target))
    }
    /// Blocking [`Scheduler::snapshot`].
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        block_on(self.handle(), self.inner.snapshot())
    }
    /// Blocking [`Scheduler::set_capacity`].
    pub fn set_capacity(&self, pool: &str, capacity: u32) -> Result<(), Error> {
        block_on(self.handle(), self.inner.set_capacity(pool, capacity))
    }
    /// Blocking [`Scheduler::add_recurring`].
    pub fn add_recurring(&self, r: Recurring<P>) -> Result<(), Error> {
        block_on(self.handle(), self.inner.add_recurring(r))
    }
    /// Blocking [`Scheduler::remove_recurring`].
    pub fn remove_recurring(&self, name: &str) -> Result<bool, Error> {
        block_on(self.handle(), self.inner.remove_recurring(name))
    }
    /// Status events; use `blocking_recv` on the receiver.
    pub fn events(&self) -> broadcast::Receiver<StatusEvent> {
        self.inner.events()
    }
    /// The async scheduler (usable from other runtimes too).
    pub fn scheduler(&self) -> &Scheduler<P, O> {
        &self.inner
    }
    /// Run a future to completion on the private runtime, blocking this thread: for the
    /// methods of [`scheduler`](Self::scheduler) that have no blocking counterpart here.
    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on(self.handle(), f)
    }
    /// Blocking [`Scheduler::shutdown`]; also stops the private runtime.
    pub fn shutdown(mut self, grace: Duration) -> Result<(), Error> {
        let r = block_on(self.handle(), self.inner.shutdown(grace));
        if let Some(rt) = self.rt.take() {
            // Waiting blocks, which is not allowed inside another runtime.
            rt.stop(Handle::try_current().is_err());
        }
        r
    }
}

impl<P, O> Drop for BlockingScheduler<P, O> {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.stop(false);
        }
    }
}

/// Blocking task handle.
pub struct BlockingHandle<O> {
    handle: Option<TaskHandle<O>>,
}

impl<O: Send + Sync + 'static> BlockingHandle<O> {
    /// The task's id.
    pub fn id(&self) -> TaskId {
        self.handle.as_ref().expect("not yet awaited").id()
    }
    /// Request cancellation.
    pub fn cancel(&self) {
        if let Some(h) = &self.handle {
            h.cancel();
        }
    }
    /// Block until the task finishes. Resolves as [`TaskFailure::Shutdown`] if the scheduler
    /// stopped first.
    pub fn wait(mut self) -> Result<Arc<O>, TaskFailure> {
        let h = self.handle.take().expect("not yet awaited");
        park_until(h, None).expect("no deadline")
    }
    /// Block up to `timeout`; returns `None` (and keeps the handle) if still running.
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<Result<Arc<O>, TaskFailure>> {
        let h = self.handle.as_mut()?;
        let r = park_until(h, Some(Instant::now() + timeout));
        if r.is_some() {
            self.handle = None;
        }
        r
    }
}
