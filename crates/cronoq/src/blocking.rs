//! Synchronous facade for code that does not run on tokio.

use std::sync::Arc;
use std::time::Duration;

use cronoq_core::{Snapshot, Submitted, TaskRef, TaskStatus};
use tokio::runtime::Runtime;

use crate::error::{Error, TaskFailure};
use crate::scheduler::{Builder, Scheduler, TaskHandle};
use crate::task::{Recurring, Task};

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
        let inner = rt.block_on(self.build())?;
        Ok(BlockingScheduler {
            rt: Some(rt),
            inner,
        })
    }
}

impl<P: Send + Sync + 'static, O: Send + Sync + 'static> BlockingScheduler<P, O> {
    fn rt(&self) -> &Runtime {
        self.rt.as_ref().expect("runtime alive")
    }
    pub fn submit(&self, task: impl Into<Task<P>>) -> Result<BlockingHandle<O>, Error> {
        let h = self.rt().block_on(self.inner.submit(task))?;
        Ok(BlockingHandle {
            handle: Some(h),
            rt: self.rt().handle().clone(),
        })
    }
    pub fn enqueue(&self, task: impl Into<Task<P>>) -> Result<Submitted, Error> {
        self.rt().block_on(self.inner.enqueue(task))
    }
    pub fn submit_batch(&self, tasks: Vec<Task<P>>) -> Result<Vec<BlockingHandle<O>>, Error> {
        let hs = self.rt().block_on(self.inner.submit_batch(tasks))?;
        Ok(hs
            .into_iter()
            .map(|h| BlockingHandle {
                handle: Some(h),
                rt: self.rt().handle().clone(),
            })
            .collect())
    }
    pub fn cancel(&self, target: impl Into<TaskRef>) -> Result<bool, Error> {
        self.rt().block_on(self.inner.cancel(target))
    }
    pub fn status(&self, target: impl Into<TaskRef>) -> Result<Option<TaskStatus>, Error> {
        self.rt().block_on(self.inner.status(target))
    }
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        self.rt().block_on(self.inner.snapshot())
    }
    pub fn add_recurring(&self, r: Recurring<P>) -> Result<(), Error> {
        self.rt().block_on(self.inner.add_recurring(r))
    }
    pub fn remove_recurring(&self, name: &str) -> Result<bool, Error> {
        self.rt().block_on(self.inner.remove_recurring(name))
    }
    /// The async scheduler (usable from other runtimes too).
    pub fn scheduler(&self) -> &Scheduler<P, O> {
        &self.inner
    }
    pub fn shutdown(mut self, grace: Duration) -> Result<(), Error> {
        let r = self.rt().block_on(self.inner.shutdown(grace));
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
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
    rt: tokio::runtime::Handle,
}

impl<O> BlockingHandle<O> {
    pub fn id(&self) -> cronoq_core::TaskId {
        self.handle.as_ref().expect("not yet awaited").id()
    }
    pub fn cancel(&self) {
        if let Some(h) = &self.handle {
            h.cancel();
        }
    }
    /// Block until the task finishes.
    pub fn wait(mut self) -> Result<Arc<O>, TaskFailure> {
        let h = self.handle.take().expect("not yet awaited");
        self.rt.block_on(h)
    }
    /// Block up to `timeout`; returns `None` (and keeps the handle) if still running.
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<Result<Arc<O>, TaskFailure>> {
        let h = self.handle.as_mut()?;
        let r = self
            .rt
            .block_on(async { tokio::time::timeout(timeout, h).await.ok() });
        if r.is_some() {
            self.handle = None;
        }
        r
    }
}
