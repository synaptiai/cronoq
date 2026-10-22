//! Remote execution: workers connect over gRPC streams and receive leases.
//!
//! Each pool's capacity is the sum of the slots its connected workers registered. The
//! [`RemoteExecutor`] turns an engine dispatch into a `Lease` message for a worker with a free
//! slot and resolves when that worker reports an outcome, disconnects, or the attempt is aborted
//! by the engine (timeout, lease expiry, cancellation).
//!
//! Every worker gets a bounded outbound channel sized for its slots (at most one lease and one
//! abort per slot, plus control messages). A worker whose channel is full is not reading its
//! stream: it is disconnected rather than allowed to swallow leases or miss aborts.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use cronoq::{BoxFuture, Executor, Scheduler, TaskContext, TaskError, TaskId};
use cronoq_proto::v1 as pb;
use rustc_hash::FxHashMap;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::convert::{CONTENT_TYPE_TAG, unix_ns};

/// Outbound message channel of one worker stream.
pub type WorkerTx = mpsc::Sender<Result<pb::ServerMessage, tonic::Status>>;

/// Channel capacity for a worker allowed `slot_cap` slots.
pub fn channel_capacity(slot_cap: u32) -> usize {
    (slot_cap as usize).saturating_mul(2).saturating_add(16)
}

struct WorkerConn {
    pool: Arc<str>,
    name: String,
    tx: WorkerTx,
    slots: u32,
    busy: u32,
    /// Cancelled to cut the stream immediately (unhealthy worker, forced stop).
    cut: CancellationToken,
}

struct PendingLease {
    worker: u64,
    task: TaskId,
    done: oneshot::Sender<Result<Bytes, TaskError>>,
    ctx: TaskContext,
    /// Kept to offer the lease to another worker after a Nack.
    msg: pb::Lease,
    /// Workers that refused this lease.
    refused: Vec<u64>,
}

#[derive(Default)]
struct State {
    workers: BTreeMap<u64, WorkerConn>,
    pending: FxHashMap<u64, PendingLease>,
    next_worker: u64,
    /// Round-robin cursor per pool.
    cursor: FxHashMap<Arc<str>, u64>,
    closed: bool,
}

/// Connected workers and their leases, shared by all remote pools.
#[derive(Default)]
pub struct WorkerRegistry {
    state: Mutex<State>,
    /// Capacity totals, applied to the scheduler in the order they were computed.
    capacity_tx: OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
    /// Reports for leases the reporting worker does not hold (late, duplicate, aborted).
    stale_reports: std::sync::atomic::AtomicU64,
}

/// Outcome of trying to send a lease to one worker.
enum Offer {
    Sent(u64),
    NoWorker,
}

impl WorkerRegistry {
    /// An empty registry.
    pub fn new() -> Arc<Self> {
        Arc::new(WorkerRegistry::default())
    }

    /// Number of reports for leases the reporting worker did not hold, which were ignored.
    pub fn stale_reports(&self) -> u64 {
        self.stale_reports
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Start publishing capacity changes to `sched`.
    pub fn attach(&self, sched: Scheduler<Bytes, Bytes>) {
        let (tx, mut rx) = mpsc::unbounded_channel::<(Arc<str>, u32)>();
        if self.capacity_tx.set(tx).is_err() {
            return;
        }
        tokio::spawn(async move {
            while let Some((pool, total)) = rx.recv().await {
                let _ = sched.set_capacity(&pool, total).await;
            }
        });
    }

    /// Reserve a worker id. Send `Registered` before calling [`Self::register`], so the worker
    /// never receives a lease before its registration ack.
    pub fn next_worker_id(&self) -> u64 {
        let mut st = self.state.lock().unwrap();
        st.next_worker += 1;
        st.next_worker
    }

    /// Add a worker. Returns `false` if the registry is already closed (shutting down).
    pub fn register(
        &self,
        id: u64,
        pool: Arc<str>,
        name: String,
        slots: u32,
        tx: WorkerTx,
        cut: CancellationToken,
    ) -> bool {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return false;
        }
        st.workers.insert(
            id,
            WorkerConn {
                pool: pool.clone(),
                name,
                tx,
                slots,
                busy: 0,
                cut,
            },
        );
        Self::publish_capacity(&self.capacity_tx, &st, &pool);
        true
    }

    /// Change a worker's slots (already capped by the caller).
    pub fn set_slots(&self, worker: u64, slots: u32) {
        let mut st = self.state.lock().unwrap();
        let Some(w) = st.workers.get_mut(&worker) else {
            return;
        };
        w.slots = slots;
        let pool = w.pool.clone();
        Self::publish_capacity(&self.capacity_tx, &st, &pool);
    }

    /// Remove a worker; its in-flight leases fail as retryable.
    pub fn unregister(&self, worker: u64) {
        let lost = {
            let mut st = self.state.lock().unwrap();
            Self::remove_locked(&self.capacity_tx, &mut st, worker)
        };
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
    }

    /// Cut a worker's stream with `status` (e.g. a protocol violation) and remove it.
    pub fn reject(&self, worker: u64, status: tonic::Status) {
        let lost = {
            let mut st = self.state.lock().unwrap();
            if let Some(w) = st.workers.get(&worker) {
                // Best effort: the stream ends with this status if the worker is reading.
                let _ = w.tx.try_send(Err(status));
            }
            Self::remove_locked(&self.capacity_tx, &mut st, worker)
        };
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
    }

    fn remove_locked(
        capacity_tx: &OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
        st: &mut State,
        worker: u64,
    ) -> Vec<PendingLease> {
        let Some(w) = st.workers.remove(&worker) else {
            return Vec::new();
        };
        let leases: Vec<u64> = st
            .pending
            .iter()
            .filter(|(_, p)| p.worker == worker)
            .map(|(l, _)| *l)
            .collect();
        let lost = leases
            .into_iter()
            .filter_map(|l| st.pending.remove(&l))
            .collect();
        Self::publish_capacity(capacity_tx, st, &w.pool);
        lost
    }

    /// Disconnect a worker that stopped reading its stream.
    fn evict_locked(
        capacity_tx: &OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
        st: &mut State,
        worker: u64,
        lost: &mut Vec<PendingLease>,
    ) {
        if let Some(w) = st.workers.get(&worker) {
            tracing::warn!(worker, name = ?w.name, pool = %w.pool, "disconnecting worker: its stream is not being read");
            w.cut.cancel();
        }
        lost.extend(Self::remove_locked(capacity_tx, st, worker));
    }

    /// Queue the pool's new total. Called with the state lock held, so updates are applied in
    /// the same order as the membership changes that caused them.
    fn publish_capacity(
        capacity_tx: &OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
        st: &State,
        pool: &Arc<str>,
    ) {
        let total: u64 = st
            .workers
            .values()
            .filter(|w| &w.pool == pool)
            .map(|w| w.slots as u64)
            .sum();
        if let Some(tx) = capacity_tx.get() {
            let _ = tx.send((pool.clone(), total.min(u32::MAX as u64) as u32));
        }
    }

    /// Extend a lease owned by `worker`.
    pub fn heartbeat(&self, worker: u64, lease: u64) {
        let st = self.state.lock().unwrap();
        if let Some(p) = st.pending.get(&lease)
            && p.worker == worker
        {
            p.ctx.heartbeat();
        }
    }

    /// Resolve a lease reported by `worker`. Reports for leases the worker does not hold are
    /// ignored.
    pub fn finish(
        &self,
        worker: u64,
        lease: u64,
        result: Result<Bytes, TaskError>,
    ) -> Option<TaskId> {
        let p = {
            let mut st = self.state.lock().unwrap();
            match st.pending.get(&lease) {
                Some(p) if p.worker == worker => {}
                _ => {
                    self.stale_reports
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return None;
                }
            }
            let p = st.pending.remove(&lease).unwrap();
            if let Some(w) = st.workers.get_mut(&worker) {
                w.busy = w.busy.saturating_sub(1);
            }
            p
        };
        let task = p.task;
        let _ = p.done.send(result);
        Some(task)
    }

    /// The worker refused a lease: offer it to another worker of the pool. If none can take it,
    /// the attempt fails as retryable.
    pub fn nack(&self, worker: u64, lease: u64, reason: &str) {
        let mut lost = Vec::new();
        let failed = {
            let mut st = self.state.lock().unwrap();
            match st.pending.get(&lease) {
                Some(p) if p.worker == worker => {}
                _ => return,
            }
            let mut p = st.pending.remove(&lease).unwrap();
            if let Some(w) = st.workers.get_mut(&worker) {
                w.busy = w.busy.saturating_sub(1);
            }
            p.refused.push(worker);
            let pool = st
                .workers
                .get(&worker)
                .map(|w| w.pool.clone())
                .unwrap_or_else(|| p.ctx.pool.clone());
            match Self::offer_locked(
                &self.capacity_tx,
                &mut st,
                &pool,
                &p.msg,
                &p.refused,
                &mut lost,
            ) {
                Offer::Sent(w) => {
                    p.worker = w;
                    st.pending.insert(lease, p);
                    None
                }
                Offer::NoWorker => Some(p),
            }
        };
        if let Some(p) = failed {
            let _ = p.done.send(Err(TaskError::retryable(format!(
                "lease refused by workers: {reason}"
            ))));
        }
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
    }

    /// Send `msg` to a worker of `pool` with a free slot, skipping `exclude`. Workers whose
    /// channel is full are disconnected and the next worker is tried.
    fn offer_locked(
        capacity_tx: &OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
        st: &mut State,
        pool: &Arc<str>,
        msg: &pb::Lease,
        exclude: &[u64],
        lost: &mut Vec<PendingLease>,
    ) -> Offer {
        loop {
            let start = st.cursor.get(pool).copied().unwrap_or(0);
            let pick = st
                .workers
                .range(start + 1..)
                .chain(st.workers.range(..=start))
                .find(|(id, w)| &w.pool == pool && w.busy < w.slots && !exclude.contains(id))
                .map(|(id, w)| (*id, w.tx.clone()));
            let Some((worker, tx)) = pick else {
                return Offer::NoWorker;
            };
            let out = pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Lease(msg.clone())),
            };
            match tx.try_send(Ok(out)) {
                Ok(()) => {
                    st.cursor.insert(pool.clone(), worker);
                    st.workers.get_mut(&worker).unwrap().busy += 1;
                    return Offer::Sent(worker);
                }
                Err(TrySendError::Full(_)) => Self::evict_locked(capacity_tx, st, worker, lost),
                Err(TrySendError::Closed(_)) => {
                    lost.extend(Self::remove_locked(capacity_tx, st, worker))
                }
            }
        }
    }

    /// Pick a worker with a free slot and send it the lease.
    fn assign(
        &self,
        pool: &Arc<str>,
        msg: pb::Lease,
        ctx: TaskContext,
        done: oneshot::Sender<Result<Bytes, TaskError>>,
    ) -> Result<(), TaskError> {
        let lease = msg.lease;
        let mut lost = Vec::new();
        let r = {
            let mut st = self.state.lock().unwrap();
            if st.closed {
                return Err(TaskError::retryable("server is shutting down"));
            }
            match Self::offer_locked(&self.capacity_tx, &mut st, pool, &msg, &[], &mut lost) {
                Offer::Sent(worker) => {
                    st.pending.insert(
                        lease,
                        PendingLease {
                            worker,
                            task: ctx.id,
                            done,
                            ctx,
                            msg,
                            refused: Vec::new(),
                        },
                    );
                    Ok(())
                }
                Offer::NoWorker => Err(TaskError::retryable("no worker with a free slot")),
            }
        };
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
        r
    }

    /// Tell the worker to stop an attempt the engine no longer wants, freeing its slot. The
    /// Abort is never dropped: a worker that cannot receive it is disconnected.
    pub fn abort(&self, lease: u64, reason: &str) {
        let mut lost = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            let Some(p) = st.pending.remove(&lease) else {
                return;
            };
            let worker = p.worker;
            let Some(w) = st.workers.get_mut(&worker) else {
                return;
            };
            w.busy = w.busy.saturating_sub(1);
            let msg = pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Abort(pb::Abort {
                    lease,
                    reason: reason.into(),
                })),
            };
            match w.tx.try_send(Ok(msg)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    Self::evict_locked(&self.capacity_tx, &mut st, worker, &mut lost)
                }
                Err(TrySendError::Closed(_)) => {
                    lost.extend(Self::remove_locked(&self.capacity_tx, &mut st, worker))
                }
            }
        }
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
    }

    /// Stop accepting workers, tell every worker to go away and drop their streams. Streams that
    /// are not drained within `grace` are cut.
    pub fn close_all(&self, reason: &str, not_leader: bool, grace: Duration) {
        let (conns, lost) = {
            let mut st = self.state.lock().unwrap();
            st.closed = true;
            let conns: Vec<WorkerConn> = std::mem::take(&mut st.workers).into_values().collect();
            let lost: Vec<PendingLease> = st.pending.drain().map(|(_, p)| p).collect();
            (conns, lost)
        };
        for p in lost {
            let _ = p.done.send(Err(TaskError::retryable(reason.to_string())));
        }
        let mut cuts = Vec::new();
        for c in conns {
            let msg = pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Shutdown(pb::Shutdown {
                    reason: reason.into(),
                    not_leader,
                })),
            };
            let _ = c.tx.try_send(Ok(msg));
            cuts.push(c.cut);
            // Dropping the sender ends the stream once the queued messages are read.
        }
        if !cuts.is_empty() {
            tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                for c in cuts {
                    c.cancel();
                }
            });
        }
    }

    /// Connected workers.
    pub fn worker_count(&self) -> usize {
        self.state.lock().unwrap().workers.len()
    }

    /// Connected workers of `pool`.
    pub fn pool_worker_count(&self, pool: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .workers
            .values()
            .filter(|w| &*w.pool == pool)
            .count()
    }
}

/// Executor for pools served by remote workers.
pub struct RemoteExecutor {
    pool: Arc<str>,
    lease_ttl: Duration,
    registry: Arc<WorkerRegistry>,
    outputs: Arc<OutputCache>,
}

impl RemoteExecutor {
    /// Executor for `pool`.
    pub fn new(
        pool: Arc<str>,
        lease_ttl: Duration,
        registry: Arc<WorkerRegistry>,
        outputs: Arc<OutputCache>,
    ) -> Self {
        RemoteExecutor {
            pool,
            lease_ttl,
            registry,
            outputs,
        }
    }
}

/// Aborts the remote attempt if the executor future is dropped before the worker finished.
struct LeaseGuard {
    registry: Arc<WorkerRegistry>,
    lease: u64,
    armed: bool,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.armed {
            self.registry.abort(self.lease, "aborted");
        }
    }
}

impl Executor<Bytes, Bytes> for RemoteExecutor {
    fn abort(&self, lease: cronoq::LeaseId) {
        // Free the worker slot now: the engine may dispatch into it in the same batch.
        self.registry.abort(lease.0, "aborted");
    }

    fn execute(
        &self,
        ctx: TaskContext,
        payload: Arc<Bytes>,
    ) -> BoxFuture<Result<Bytes, TaskError>> {
        let registry = self.registry.clone();
        let outputs = self.outputs.clone();
        let pool = self.pool.clone();
        let ttl = self.lease_ttl;
        Box::pin(async move {
            let msg = pb::Lease {
                lease: ctx.lease.0,
                task_id: ctx.id.0,
                attempt: ctx.attempt,
                payload: payload.to_vec(),
                content_type: ctx.tag(CONTENT_TYPE_TAG).unwrap_or_default().to_string(),
                kind: ctx.kind.to_string(),
                key: ctx.key.as_deref().unwrap_or_default().to_string(),
                tags: ctx
                    .tags
                    .iter()
                    .filter(|(k, _)| &**k != CONTENT_TYPE_TAG)
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                timeout_s: ctx.timeout.map(|d| d.as_secs_f64()),
                deadline_unix_ns: ctx.deadline.map(unix_ns),
                pool: pool.to_string(),
                // The runtime does not expose the task's priority to executors yet.
                priority: pb::Priority::Unspecified as i32,
                lease_ttl_s: Some(ttl.as_secs_f64()),
            };
            let (tx, rx) = oneshot::channel();
            let id = ctx.id;
            let cancel = ctx.cancellation_token();
            registry.assign(&pool, msg, ctx.clone(), tx)?;
            let mut guard = LeaseGuard {
                registry: registry.clone(),
                lease: ctx.lease.0,
                armed: true,
            };
            let result = tokio::select! {
                r = rx => {
                    guard.armed = false;
                    r.unwrap_or_else(|_| Err(TaskError::retryable("worker lost")))
                }
                _ = cancel.cancelled() => Err(TaskError::Cancelled),
            };
            if let Ok(out) = &result {
                outputs.put(id, out.clone());
            }
            result
        })
    }
}

/// Bounded cache of recent outputs (by count and bytes), for `Wait`/`Get` after completion.
pub struct OutputCache {
    inner: Mutex<CacheState>,
    max_entries: usize,
    max_bytes: usize,
}

#[derive(Default)]
struct CacheState {
    map: FxHashMap<TaskId, Bytes>,
    order: VecDeque<TaskId>,
    bytes: usize,
}

impl OutputCache {
    /// Cache bounded by entries and bytes.
    pub fn new(max_entries: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(OutputCache {
            inner: Mutex::new(CacheState::default()),
            max_entries,
            max_bytes,
        })
    }

    /// Remember an output (outputs larger than the byte budget are not cached).
    pub fn put(&self, id: TaskId, out: Bytes) {
        if self.max_entries == 0 || out.len() > self.max_bytes {
            return;
        }
        let mut st = self.inner.lock().unwrap();
        st.bytes += out.len();
        if let Some(old) = st.map.insert(id, out) {
            st.bytes -= old.len();
        } else {
            st.order.push_back(id);
        }
        while st.map.len() > self.max_entries || st.bytes > self.max_bytes {
            let Some(victim) = st.order.pop_front() else {
                break;
            };
            if let Some(v) = st.map.remove(&victim) {
                st.bytes -= v.len();
            }
        }
    }

    /// A cached output.
    pub fn get(&self, id: TaskId) -> Option<Bytes> {
        self.inner.lock().unwrap().map.get(&id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_cache_is_bounded() {
        let c = OutputCache::new(2, 10);
        c.put(TaskId(1), Bytes::from_static(b"aaaa"));
        c.put(TaskId(2), Bytes::from_static(b"bbbb"));
        c.put(TaskId(3), Bytes::from_static(b"cc"));
        assert!(c.get(TaskId(1)).is_none());
        assert_eq!(c.get(TaskId(3)).unwrap(), Bytes::from_static(b"cc"));
        c.put(TaskId(4), Bytes::from_static(b"dddddddd"));
        assert!(c.get(TaskId(2)).is_none(), "byte budget evicts oldest");
        c.put(TaskId(5), Bytes::from(vec![0u8; 11]));
        assert!(
            c.get(TaskId(5)).is_none(),
            "oversized outputs are not cached"
        );
    }
}
