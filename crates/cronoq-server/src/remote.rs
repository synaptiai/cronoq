//! Remote execution: workers connect over gRPC streams and receive leases.
//!
//! Each pool's capacity is the sum of the slots its connected workers registered. The
//! [`RemoteExecutor`] turns an engine dispatch into a `Lease` message for a worker with a free
//! slot and resolves when that worker reports `Complete`/`Fail`, disconnects, or the attempt is
//! aborted by the engine (timeout, lease expiry, cancellation).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use cronoq::{BoxFuture, Executor, Scheduler, TaskContext, TaskError, TaskId};
use cronoq_proto::v1 as pb;
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};

use crate::convert::{CONTENT_TYPE_TAG, unix_ns};

pub type WorkerTx = mpsc::Sender<Result<pb::ServerMessage, tonic::Status>>;

struct WorkerConn {
    pool: Arc<str>,
    tx: WorkerTx,
    slots: u32,
    busy: u32,
}

struct PendingLease {
    worker: u64,
    task: TaskId,
    done: oneshot::Sender<Result<Bytes, TaskError>>,
    ctx: TaskContext,
}

#[derive(Default)]
struct State {
    workers: BTreeMap<u64, WorkerConn>,
    pending: FxHashMap<u64, PendingLease>,
    next_worker: u64,
    /// Round-robin cursor per pool.
    cursor: FxHashMap<Arc<str>, u64>,
}

/// Connected workers and their leases, shared by all remote pools.
#[derive(Default)]
pub struct WorkerRegistry {
    state: Mutex<State>,
    /// Capacity totals, applied to the scheduler in the order they were computed.
    capacity_tx: OnceLock<mpsc::UnboundedSender<(Arc<str>, u32)>>,
}

impl WorkerRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(WorkerRegistry::default())
    }

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

    pub fn register(&self, id: u64, pool: Arc<str>, slots: u32, tx: WorkerTx) {
        let mut st = self.state.lock().unwrap();
        st.workers.insert(
            id,
            WorkerConn {
                pool: pool.clone(),
                tx,
                slots,
                busy: 0,
            },
        );
        self.publish_capacity(&st, &pool);
    }

    pub fn set_slots(&self, worker: u64, slots: u32) {
        let mut st = self.state.lock().unwrap();
        let Some(w) = st.workers.get_mut(&worker) else {
            return;
        };
        w.slots = slots;
        let pool = w.pool.clone();
        self.publish_capacity(&st, &pool);
    }

    /// Remove a worker; its in-flight leases fail as retryable.
    pub fn unregister(&self, worker: u64) {
        let lost = {
            let mut st = self.state.lock().unwrap();
            let Some(w) = st.workers.remove(&worker) else {
                return;
            };
            let leases: Vec<u64> = st
                .pending
                .iter()
                .filter(|(_, p)| p.worker == worker)
                .map(|(l, _)| *l)
                .collect();
            let lost: Vec<PendingLease> = leases
                .into_iter()
                .filter_map(|l| st.pending.remove(&l))
                .collect();
            self.publish_capacity(&st, &w.pool);
            lost
        };
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
    }

    /// Queue the pool's new total. Called with the state lock held, so updates are applied in
    /// the same order as the membership changes that caused them.
    fn publish_capacity(&self, st: &State, pool: &Arc<str>) {
        let total: u64 = st
            .workers
            .values()
            .filter(|w| &w.pool == pool)
            .map(|w| w.slots as u64)
            .sum();
        if let Some(tx) = self.capacity_tx.get() {
            let _ = tx.send((pool.clone(), total.min(u32::MAX as u64) as u32));
        }
    }

    pub fn heartbeat(&self, worker: u64, lease: u64) {
        let st = self.state.lock().unwrap();
        if let Some(p) = st.pending.get(&lease) {
            if p.worker == worker {
                p.ctx.heartbeat();
            }
        }
    }

    /// Resolve a lease reported by `worker`.
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
                _ => return None,
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

    /// Pick a worker with a free slot and send it the lease.
    fn assign(
        &self,
        pool: &Arc<str>,
        msg: pb::Lease,
        ctx: TaskContext,
        done: oneshot::Sender<Result<Bytes, TaskError>>,
    ) -> Result<(), TaskError> {
        let lease = msg.lease;
        let mut st = self.state.lock().unwrap();
        let start = st.cursor.get(pool).copied().unwrap_or(0);
        let pick = st
            .workers
            .range(start + 1..)
            .chain(st.workers.range(..=start))
            .find(|(_, w)| &w.pool == pool && w.busy < w.slots)
            .map(|(id, w)| (*id, w.tx.clone()));
        let Some((worker, tx)) = pick else {
            return Err(TaskError::retryable("no worker with a free slot"));
        };
        let msg = pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Lease(msg)),
        };
        if tx.try_send(Ok(msg)).is_err() {
            return Err(TaskError::retryable("worker stream is congested or closed"));
        }
        st.cursor.insert(pool.clone(), worker);
        st.workers.get_mut(&worker).unwrap().busy += 1;
        st.pending.insert(
            lease,
            PendingLease {
                worker,
                task: ctx.id,
                done,
                ctx,
            },
        );
        Ok(())
    }

    /// Tell the worker to stop an attempt the engine no longer wants, freeing its slot.
    pub fn abort(&self, lease: u64, reason: &str) {
        let tx = {
            let mut st = self.state.lock().unwrap();
            let Some(p) = st.pending.remove(&lease) else {
                return;
            };
            let Some(w) = st.workers.get_mut(&p.worker) else {
                return;
            };
            w.busy = w.busy.saturating_sub(1);
            w.tx.clone()
        };
        let msg = pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Abort(pb::Abort {
                lease,
                reason: reason.into(),
            })),
        };
        let _ = tx.try_send(Ok(msg));
    }

    pub fn worker_count(&self) -> usize {
        self.state.lock().unwrap().workers.len()
    }
}

/// Executor for pools served by remote workers.
pub struct RemoteExecutor {
    pool: Arc<str>,
    registry: Arc<WorkerRegistry>,
    outputs: Arc<OutputCache>,
}

impl RemoteExecutor {
    pub fn new(pool: Arc<str>, registry: Arc<WorkerRegistry>, outputs: Arc<OutputCache>) -> Self {
        RemoteExecutor {
            pool,
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
    pub fn new(max_entries: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(OutputCache {
            inner: Mutex::new(CacheState::default()),
            max_entries,
            max_bytes,
        })
    }
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
