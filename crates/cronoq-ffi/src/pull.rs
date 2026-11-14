//! The pull-based executor: the scheduler hands attempts to a hub, and host threads take
//! them from it with a blocking call, as remote workers take leases from a server.
//!
//! A pool's capacity follows the host: it is the number of leases the host holds plus the
//! number of threads waiting for one. A task is therefore chosen when a thread asks for it,
//! not before, and the order of the queue is decided as late as possible.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cronoq::{BoxFuture, Executor, LeaseId, TaskContext, TaskError};
use tokio::sync::oneshot;

type Done = oneshot::Sender<Result<Bytes, TaskError>>;
type Resync = Box<dyn Fn(Arc<str>) -> BoxFuture<()> + Send + Sync>;

/// An attempt the scheduler has started and a host thread may take.
struct Offer {
    ctx: TaskContext,
    payload: Arc<Bytes>,
    done: Done,
}

/// An attempt handed to a host thread.
pub(crate) struct Leased {
    pub(crate) ctx: TaskContext,
    pub(crate) payload: Arc<Bytes>,
}

/// An attempt a host thread has taken.
struct Held {
    pool: Arc<str>,
    ctx: TaskContext,
    done: Done,
}

#[derive(Default)]
struct PoolQueue {
    ready: VecDeque<Offer>,
    /// Threads blocked in `next` for this pool.
    waiting: u32,
    /// Leases of this pool the host holds.
    held: u32,
}

impl PoolQueue {
    /// One slot per lease the host holds and one per thread that waits for a lease. An
    /// attempt in `ready` is on its way to a waiting thread, which is already counted.
    fn capacity(&self) -> u32 {
        self.held + self.waiting
    }
}

#[derive(Default)]
struct State {
    pools: HashMap<Arc<str>, PoolQueue>,
    held: HashMap<u64, Held>,
    closed: bool,
}

/// Where the scheduler and the host threads meet.
#[derive(Default)]
pub(crate) struct Hub {
    state: Mutex<State>,
    changed: Condvar,
    /// Brings the scheduler's capacity of a pool in line with [`Hub::capacity`], from the
    /// scheduler's own thread, where blocking is not possible.
    resync: OnceLock<Resync>,
}

/// What a wait for a lease ended with.
pub(crate) enum Next {
    Lease(Leased),
    Timeout,
    Closed,
}

impl Hub {
    pub(crate) fn new(pools: impl IntoIterator<Item = Arc<str>>) -> Arc<Hub> {
        let hub = Hub::default();
        {
            let mut s = hub.lock();
            for p in pools {
                s.pools.insert(p, PoolQueue::default());
            }
        }
        Arc::new(hub)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while the lock is held leaves the state consistent: every change is a
        // single insert or remove.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn has_pool(&self, pool: &str) -> bool {
        self.lock().pools.contains_key(pool)
    }

    /// Set how the capacity of a pool is brought in line from the scheduler's thread.
    pub(crate) fn resync_with(
        &self,
        f: impl Fn(Arc<str>) -> BoxFuture<()> + Send + Sync + 'static,
    ) {
        let _ = self.resync.set(Box::new(f));
    }

    /// The capacity `pool` should have now: the leases the host holds plus the threads
    /// waiting for one.
    pub(crate) fn capacity(&self, pool: &str) -> u32 {
        self.lock().pools.get(pool).map_or(0, PoolQueue::capacity)
    }

    /// Wait up to `timeout` for an attempt of `pool`. `sync` is called, without the lock
    /// held, whenever the capacity the pool should have has changed: it has to bring the
    /// scheduler's capacity in line with [`capacity`](Self::capacity).
    pub(crate) fn next(&self, pool: &str, timeout: Duration, sync: &dyn Fn()) -> Next {
        let deadline = Instant::now().checked_add(timeout);
        let mut s = self.lock();
        if s.closed {
            return Next::Closed;
        }
        if let Some(o) = Self::take(&mut s, pool) {
            return Next::Lease(o);
        }
        // Nothing is ready: ask the scheduler for one more attempt.
        s.pools.get_mut(pool).expect("pool was checked").waiting += 1;
        drop(s);
        sync();
        let mut s = self.lock();
        let got = loop {
            if s.closed {
                break None;
            }
            if let Some(o) = Self::take(&mut s, pool) {
                break Some(o);
            }
            let left = match deadline {
                Some(d) => d.saturating_duration_since(Instant::now()),
                None => Duration::from_secs(3600),
            };
            if left.is_zero() {
                break None;
            }
            s = self
                .changed
                .wait_timeout(s, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        };
        let closed = s.closed;
        s.pools.get_mut(pool).expect("pool was checked").waiting -= 1;
        drop(s);
        if let Some(o) = got {
            // The lease took the place of the waiting thread: the capacity is unchanged.
            return Next::Lease(o);
        }
        if closed {
            return Next::Closed;
        }
        sync();
        // An attempt may have been started for this thread in the meantime: take it. One
        // that arrives later still finds no waiting thread and is given back (`execute`).
        let mut s = self.lock();
        match Self::take(&mut s, pool) {
            Some(o) => Next::Lease(o),
            None => Next::Timeout,
        }
    }

    /// Move the oldest ready attempt of `pool` to the held ones.
    fn take(s: &mut State, pool: &str) -> Option<Leased> {
        let name = s.pools.get_key_value(pool)?.0.clone();
        let q = s.pools.get_mut(pool)?;
        let o = q.ready.pop_front()?;
        q.held += 1;
        // The host gets the context and the payload; the hub keeps the way to answer.
        let offer = Leased {
            ctx: o.ctx.clone(),
            payload: o.payload,
        };
        s.held.insert(
            o.ctx.lease.0,
            Held {
                pool: name,
                ctx: o.ctx,
                done: o.done,
            },
        );
        Some(offer)
    }

    /// End a held lease with `result`. `sync` is called for the lease's pool before the
    /// scheduler learns of the end, so that it does not start another attempt for a slot
    /// nobody waits for. Returns false if the lease is not held (it ended, or was never
    /// handed out).
    pub(crate) fn finish(
        &self,
        lease: u64,
        result: Result<Bytes, TaskError>,
        sync: &dyn Fn(&str),
    ) -> bool {
        let mut s = self.lock();
        let Some(h) = s.held.remove(&lease) else {
            return false;
        };
        s.pools.get_mut(&h.pool).expect("pool of a held lease").held -= 1;
        drop(s);
        sync(&h.pool);
        let _ = h.done.send(result);
        true
    }

    /// Extend a held lease. Returns false if it is not held any more.
    pub(crate) fn heartbeat(&self, lease: u64) -> bool {
        let s = self.lock();
        match s.held.get(&lease) {
            Some(h) => {
                h.ctx.heartbeat();
                true
            }
            None => false,
        }
    }

    /// The scheduler ended the attempt itself (timeout, cancel, expiry, shutdown).
    fn forget(&self, pool: &str, lease: u64) {
        let mut s = self.lock();
        let Some((name, q)) = s.pools.get_key_value(pool) else {
            return;
        };
        let (name, before) = (name.clone(), q.capacity());
        if s.held.remove(&lease).is_some() {
            s.pools.get_mut(pool).expect("checked").held -= 1;
        } else {
            let q = s.pools.get_mut(pool).expect("checked");
            q.ready.retain(|o| o.ctx.lease.0 != lease);
        }
        let changed = s.pools[pool].capacity() != before && !s.closed;
        drop(s);
        // The scheduler ended a lease the host held: the pool has one slot too many.
        if changed
            && let Some(resync) = self.resync.get()
            && let Ok(rt) = tokio::runtime::Handle::try_current()
        {
            rt.spawn(resync(name));
        }
    }

    /// Stop handing out leases and wake every waiting thread.
    pub(crate) fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        for q in s.pools.values_mut() {
            q.ready.clear();
        }
        // Dropping the senders ends the attempts.
        s.held.clear();
        for q in s.pools.values_mut() {
            q.held = 0;
        }
        drop(s);
        self.changed.notify_all();
    }
}

/// The executor of one pool: it puts each attempt into the hub and waits for the host.
pub(crate) struct PullExecutor {
    pub(crate) pool: Arc<str>,
    pub(crate) hub: Arc<Hub>,
}

/// Removes the attempt from the hub when the scheduler drops it without an abort.
struct Forget {
    hub: Arc<Hub>,
    pool: Arc<str>,
    lease: u64,
}

impl Drop for Forget {
    fn drop(&mut self) {
        self.hub.forget(&self.pool, self.lease);
    }
}

impl Executor<Bytes, Bytes> for PullExecutor {
    fn execute(
        &self,
        ctx: TaskContext,
        payload: Arc<Bytes>,
    ) -> BoxFuture<Result<Bytes, TaskError>> {
        let (done, result) = oneshot::channel();
        let guard = Forget {
            hub: self.hub.clone(),
            pool: self.pool.clone(),
            lease: ctx.lease.0,
        };
        {
            let mut s = self.hub.lock();
            if s.closed {
                return Box::pin(async { Err(TaskError::retryable("the scheduler is closed")) });
            }
            let Some(q) = s.pools.get_mut(&self.pool) else {
                return Box::pin(async { Err(TaskError::permanent("unknown pool")) });
            };
            if q.ready.len() as u32 >= q.waiting {
                // No thread waits for this attempt: it was started for a slot that has gone
                // since (the capacity is lowered by a message, and the scheduler may start
                // an attempt before it arrives). Lower the capacity first, then give the
                // task back, so that it keeps its place and is not started again at once.
                drop(s);
                let resync = self.hub.resync.get().map(|f| f(self.pool.clone()));
                return Box::pin(async move {
                    drop(guard);
                    if let Some(r) = resync {
                        r.await;
                    }
                    Err(TaskError::release(Duration::ZERO))
                });
            }
            q.ready.push_back(Offer { ctx, payload, done });
        }
        self.hub.changed.notify_all();
        Box::pin(async move {
            let _guard = guard;
            match result.await {
                Ok(r) => r,
                Err(_) => Err(TaskError::retryable("the scheduler was closed")),
            }
        })
    }

    fn abort(&self, lease: LeaseId) {
        self.hub.forget(&self.pool, lease.0);
    }
}
