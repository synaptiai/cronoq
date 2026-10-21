//! Synchronous facade for code that does not run on tokio.
//!
//! [`BlockingScheduler`] owns a private multi-threaded tokio runtime. Its methods block the
//! calling thread; they are safe to call from inside another tokio runtime too (the wait then
//! happens on a helper thread), although async code should prefer [`Scheduler`] directly.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

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

/// A scheduler that owns its own tokio runtime and exposes blocking methods.
pub struct BlockingScheduler<P, O> {
    rt: Option<Runtime>,
    inner: Scheduler<P, O>,
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> Builder<P, O> {
    /// Build on a private multi-threaded runtime.
    pub fn build_blocking(self) -> Result<BlockingScheduler<P, O>, Error> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("cronoq")
            .build()
            .map_err(|e| Error::Config(format!("runtime: {e}")))?;
        let inner = block_on(rt.handle(), self.build())?;
        Ok(BlockingScheduler {
            rt: Some(rt),
            inner,
        })
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> BlockingScheduler<P, O> {
    fn handle(&self) -> &Handle {
        self.rt.as_ref().expect("runtime alive").handle()
    }
    fn wrap(&self, h: TaskHandle<O>) -> BlockingHandle<O> {
        BlockingHandle {
            handle: Some(h),
            rt: self.handle().clone(),
        }
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
    /// Blocking [`Scheduler::shutdown`]; also stops the private runtime.
    pub fn shutdown(mut self, grace: Duration) -> Result<(), Error> {
        let r = block_on(self.handle(), self.inner.shutdown(grace));
        if let Some(rt) = self.rt.take() {
            // `shutdown_timeout` blocks, which is not allowed inside another runtime.
            if Handle::try_current().is_ok() {
                rt.shutdown_background();
            } else {
                rt.shutdown_timeout(Duration::from_secs(1));
            }
        }
        r
    }
}

impl<P, O> Drop for BlockingScheduler<P, O> {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// Blocking task handle.
pub struct BlockingHandle<O> {
    handle: Option<TaskHandle<O>>,
    rt: Handle,
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
        block_on(&self.rt, h)
    }
    /// Block up to `timeout`; returns `None` (and keeps the handle) if still running.
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<Result<Arc<O>, TaskFailure>> {
        let h = self.handle.as_mut()?;
        let r = block_on(&self.rt, async {
            tokio::time::timeout(timeout, h).await.ok()
        });
        if r.is_some() {
            self.handle = None;
        }
        r
    }
}
