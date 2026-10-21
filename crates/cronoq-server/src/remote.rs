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
    sched: OnceLock<Scheduler<Bytes, Bytes>>,
}

impl WorkerRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(WorkerRegistry::default())
    }

    pub fn attach(&self, sched: Scheduler<Bytes, Bytes>) {
        let _ = self.sched.set(sched);
    }

    pub fn register(&self, pool: Arc<str>, slots: u32, tx: WorkerTx) -> u64 {
        let id = {
            let mut st = self.state.lock().unwrap();
            st.next_worker += 1;
            let id = st.next_worker;
            st.workers.insert(
                id,
                WorkerConn {
                    pool: pool.clone(),
                    tx,
                    slots,
                    busy: 0,
                },
            );
            id
        };
        self.publish_capacity(&pool);
        id
    }

    pub fn set_slots(&self, worker: u64, slots: u32) {
        let pool = {
            let mut st = self.state.lock().unwrap();
            let Some(w) = st.workers.get_mut(&worker) else {
                return;
            };
            w.slots = slots;
            w.pool.clone()
        };
        self.publish_capacity(&pool);
    }

    /// Remove a worker; its in-flight leases fail as retryable.
    pub fn unregister(&self, worker: u64) {
        let (pool, lost) = {
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
            (w.pool, lost)
        };
        for p in lost {
            let _ = p
                .done
                .send(Err(TaskError::retryable("worker disconnected")));
        }
        self.publish_capacity(&pool);
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

    fn publish_capacity(&self, pool: &Arc<str>) {
        let total: u32 = {
            let st = self.state.lock().unwrap();
            st.workers
                .values()
                .filter(|w| &w.pool == pool)
                .map(|w| w.slots)
                .sum()
        };
        if let Some(s) = self.sched.get() {
            let s = s.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                let _ = s.set_capacity(&pool, total).await;
            });
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

    /// Tell the worker to stop an attempt the engine no longer wants.
    fn abort(&self, lease: u64, reason: &str) {
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
