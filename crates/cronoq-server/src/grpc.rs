//! gRPC services (`cronoq.v1.Tasks`, `cronoq.v1.Workers`, `cronoq.v1.Admin`) and the request
//! logic shared with the HTTP gateway.
//!
//! Every call authenticates first, then checks that this instance is the serving leader
//! (`UNAVAILABLE` otherwise). Callers whose credentials are restricted to some pools only see
//! tasks of those pools: other tasks are reported as `NOT_FOUND`.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use cronoq::{OverlapPolicy, Recurring, TaskError, TaskFailure, TaskId, TaskRef, TaskStatus};
use cronoq_proto::v1 as pb;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{Principal, check_grpc};
use crate::config::{LimitsConfig, Role};
use crate::convert;
use crate::info::TaskMeta;
use crate::remote::channel_capacity;
use crate::state::{AppState, Shared};

/// How long a worker may take to send `Register` after opening its stream.
const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest recurring-schedule name or cron expression.
const MAX_SCHEDULE_FIELD: usize = 256;

/// The effective wait bound: the requested timeout capped by `limits.max_wait_s`.
pub fn wait_bound(requested_s: Option<f64>, limits: &LimitsConfig) -> Result<Duration, Status> {
    let max = limits.max_wait();
    Ok(match requested_s {
        Some(s) => convert::secs(s)?.min(max),
        None => max,
    })
}

fn not_found() -> Status {
    Status::not_found("unknown task")
}

/// Refuse a pool the caller may not use.
pub fn check_pool(principal: &Principal, pool: &str) -> Result<(), Status> {
    if principal.may_use_pool(pool) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "credentials do not allow pool '{pool}'"
        )))
    }
}

/// Resolve a reference to a task id (live in the engine, cached, or persisted).
async fn resolve_id(app: &AppState, target: &TaskRef) -> Result<Option<TaskId>, Status> {
    match target {
        TaskRef::Id(id) => Ok(Some(*id)),
        TaskRef::Key(k) => {
            if let Some(h) = app
                .sched
                .handle(target.clone())
                .await
                .map_err(convert::error)?
            {
                return Ok(Some(h.id()));
            }
            if let Some(id) = app.info.id_for_key(k) {
                return Ok(Some(TaskId(id)));
            }
            match &app.pg {
                Some(pg) => Ok(pg
                    .get_task(target)
                    .await
                    .map_err(store_error)?
                    .map(|r| r.id)),
                None => Ok(None),
            }
        }
    }
}

fn store_error(e: cronoq::StoreError) -> Status {
    convert::error(cronoq::Error::Store(e))
}

/// Check that `principal` may see the task `target` refers to. Unrestricted principals pass
/// without a lookup; restricted ones get `NOT_FOUND` for tasks of other (or unknown) pools.
pub async fn authorize_task(
    app: &AppState,
    principal: &Principal,
    target: &TaskRef,
) -> Result<(), Status> {
    if principal.pools.is_none() {
        return Ok(());
    }
    let id = resolve_id(app, target).await?.ok_or_else(not_found)?;
    match app.pool_of(id).await {
        Some(pool) if principal.may_use_pool(&pool) => Ok(()),
        _ => Err(not_found()),
    }
}

/// Wait for a task and build its result. Waits at most `timeout` (`DEADLINE_EXCEEDED`), and
/// ends with `UNAVAILABLE` if the server stops serving first.
pub async fn wait_result(
    app: &AppState,
    target: TaskRef,
    timeout: Duration,
    stop: &CancellationToken,
) -> Result<pb::TaskResult, Status> {
    let handle = app
        .sched
        .handle(target.clone())
        .await
        .map_err(convert::error)?;
    match handle {
        Some(h) => await_handle(app, h, timeout, stop).await,
        // Not live in the engine any more: answer from the persisted record if it finished.
        None => finished_result(app, &target).await?.ok_or_else(not_found),
    }
}

async fn finished_result(
    app: &AppState,
    target: &TaskRef,
) -> Result<Option<pb::TaskResult>, Status> {
    let Some(pg) = &app.pg else { return Ok(None) };
    let Some(r) = pg.get_task(target).await.map_err(store_error)? else {
        return Ok(None);
    };
    let Some(status) = r.status.filter(|s| s.is_terminal()) else {
        return Err(Status::unavailable(
            "task is queued but not loaded by this leader; retry",
        ));
    };
    let output_available = status == TaskStatus::Succeeded && r.output.is_some();
    Ok(Some(pb::TaskResult {
        id: r.id.0,
        status: convert::status(status) as i32,
        output: r.output.map(|b| b.to_vec()),
        message: r.message.unwrap_or_default(),
        output_available,
    }))
}

/// Await a task handle and build its result (see [`wait_result`]).
pub async fn await_handle(
    app: &AppState,
    handle: cronoq::TaskHandle<Bytes>,
    timeout: Duration,
    stop: &CancellationToken,
) -> Result<pb::TaskResult, Status> {
    let id = handle.id();
    let outcome = tokio::select! {
        r = handle => r,
        _ = tokio::time::sleep(timeout) => {
            return Err(Status::deadline_exceeded("task is still running; wait again or poll"));
        }
        _ = stop.cancelled() => {
            return Err(Status::unavailable("server is shutting down; wait again on the new leader"));
        }
    };
    Ok(match outcome {
        Ok(out) => pb::TaskResult {
            id: id.0,
            status: pb::TaskStatus::Succeeded as i32,
            output: Some(out.to_vec()),
            message: String::new(),
            output_available: true,
        },
        Err(TaskFailure::Ended {
            status: TaskStatus::Succeeded,
            ..
        }) => {
            let output = app.output(id).await;
            pb::TaskResult {
                id: id.0,
                status: pb::TaskStatus::Succeeded as i32,
                output_available: output.is_some(),
                output: output.map(|b| b.to_vec()),
                message: String::new(),
            }
        }
        Err(TaskFailure::Ended { status, message }) => {
            // Attaching to an already finished task yields a generic message; the cache keeps
            // the real one from the terminal event.
            let cached = app
                .info
                .get(id.0)
                .filter(|m| m.status.is_some_and(|s| s.is_terminal()))
                .and_then(|m| m.message)
                .filter(|m| !m.is_empty());
            let message = cached
                .map(|m| m.to_string())
                .unwrap_or_else(|| message.to_string());
            pb::TaskResult {
                id: id.0,
                status: convert::status(status) as i32,
                output: None,
                message,
                output_available: false,
            }
        }
        Err(TaskFailure::Shutdown) => {
            return Err(Status::unavailable(
                "scheduler shut down before the task finished; wait again on the new leader",
            ));
        }
    })
}

fn record_submit(app: &AppState, id: TaskId, spec: &pb::TaskSpec) {
    let pool = convert::spec_pool(spec);
    let max_attempts = match &spec.retry {
        Some(r) => r.max_attempts.max(1),
        None => app.pool(pool).map(|p| p.max_attempts).unwrap_or(0),
    };
    app.info.on_submit(
        id.0,
        TaskMeta {
            pool: Arc::from(pool),
            key: (!spec.key.is_empty()).then(|| Arc::from(spec.key.as_str())),
            kind: Some(Arc::from(if spec.kind.is_empty() {
                "default"
            } else {
                spec.kind.as_str()
            })),
            max_attempts,
            created_ns: Some(convert::unix_ns(SystemTime::now())),
            ..Default::default()
        },
    );
}

/// Validate and submit one task, optionally waiting (bounded by `wait_timeout`) for its result.
pub async fn submit_one(
    app: &AppState,
    principal: &Principal,
    spec: pb::TaskSpec,
    wait: Option<Duration>,
    stop: &CancellationToken,
) -> Result<pb::SubmitResponse, Status> {
    check_pool(principal, convert::spec_pool(&spec))?;
    let meta = pb::TaskSpec {
        payload: Vec::new(),
        ..spec.clone()
    };
    let task = convert::task(spec, &app.limits)?;
    match wait {
        Some(timeout) => {
            let h = app.sched.submit(task).await.map_err(convert::error)?;
            let (id, duplicate) = (h.id(), h.is_duplicate());
            if !duplicate {
                record_submit(app, id, &meta);
            }
            let result = await_handle(app, h, timeout, stop).await?;
            Ok(pb::SubmitResponse {
                id: id.0,
                duplicate,
                result: Some(result),
            })
        }
        None => {
            let s = app.sched.enqueue(task).await.map_err(convert::error)?;
            if !s.duplicate {
                record_submit(app, s.id, &meta);
            }
            Ok(pb::SubmitResponse {
                id: s.id.0,
                duplicate: s.duplicate,
                result: None,
            })
        }
    }
}

/// Validate and submit tasks atomically.
pub async fn submit_batch(
    app: &AppState,
    principal: &Principal,
    specs: Vec<pb::TaskSpec>,
) -> Result<pb::SubmitBatchResponse, Status> {
    let max = app.limits.max_tasks_per_batch;
    if specs.len() > max {
        return Err(Status::invalid_argument(format!(
            "at most {max} tasks per batch"
        )));
    }
    let mut metas = Vec::with_capacity(specs.len());
    let mut tasks = Vec::with_capacity(specs.len());
    for spec in specs {
        check_pool(principal, convert::spec_pool(&spec))?;
        metas.push(pb::TaskSpec {
            payload: Vec::new(),
            ..spec.clone()
        });
        tasks.push(convert::task(spec, &app.limits)?);
    }
    let hs = app
        .sched
        .submit_batch(tasks)
        .await
        .map_err(convert::error)?;
    for (h, m) in hs.iter().zip(&metas) {
        if !h.is_duplicate() {
            record_submit(app, h.id(), m);
        }
    }
    Ok(pb::SubmitBatchResponse {
        tasks: hs
            .iter()
            .map(|h| pb::SubmitResponse {
                id: h.id().0,
                duplicate: h.is_duplicate(),
                result: None,
            })
            .collect(),
    })
}

/// Details of a task (`NOT_FOUND` if it is unknown).
pub async fn task_info(app: &AppState, target: TaskRef) -> Result<pb::TaskInfo, Status> {
    let id = resolve_id(app, &target).await?.ok_or_else(not_found)?;
    let live = app
        .sched
        .status(TaskRef::Id(id))
        .await
        .map_err(convert::error)?;
    let meta = app.info.get(id.0);
    let rec = match (&app.pg, live.is_some() && meta.is_some()) {
        (Some(pg), false) => pg.get_task(&TaskRef::Id(id)).await.map_err(store_error)?,
        _ => None,
    };
    if live.is_none() && meta.as_ref().and_then(|m| m.status).is_none() && rec.is_none() {
        return Err(not_found());
    }
    let status = live
        .or_else(|| meta.as_ref().and_then(|m| m.status))
        .or_else(|| rec.as_ref().and_then(|r| r.status))
        // Persisted without a status = queued.
        .unwrap_or(TaskStatus::Ready);
    let meta = meta.unwrap_or_default();
    let output_available = status == TaskStatus::Succeeded
        && (app.outputs.get(id).is_some() || rec.as_ref().is_some_and(|r| r.output.is_some()));
    let pool = if meta.pool.is_empty() {
        rec.as_ref().map(|r| r.pool.clone()).unwrap_or_default()
    } else {
        meta.pool.to_string()
    };
    Ok(pb::TaskInfo {
        id: id.0,
        status: convert::status(status) as i32,
        pool,
        key: meta
            .key
            .as_deref()
            .map(str::to_string)
            .or_else(|| rec.as_ref().and_then(|r| r.key.clone()))
            .unwrap_or_default(),
        kind: meta
            .kind
            .as_deref()
            .map(str::to_string)
            .or_else(|| rec.as_ref().map(|r| r.kind.clone()))
            .unwrap_or_default(),
        attempt: meta
            .attempt
            .max(rec.as_ref().map(|r| r.attempt).unwrap_or(0)),
        max_attempts: meta.max_attempts,
        created_unix_ns: meta.created_ns,
        started_unix_ns: meta.started_ns,
        finished_unix_ns: meta.finished_ns,
        message: meta
            .message
            .as_deref()
            .map(str::to_string)
            .or_else(|| rec.and_then(|r| r.message))
            .unwrap_or_default(),
        output_available,
    })
}

/// Cancel a task (`false` if it already finished).
pub async fn cancel(app: &AppState, target: TaskRef) -> Result<bool, Status> {
    app.sched.cancel(target).await.map_err(convert::error)
}

/// Which status events a Watch stream delivers.
pub struct WatchFilter {
    /// Pools (empty = all the caller may see).
    pub pools: Vec<String>,
    /// Task ids (empty = any).
    pub ids: Vec<u64>,
    /// Task keys (empty = any).
    pub keys: Vec<String>,
    /// Deliver non-terminal transitions too.
    pub all_transitions: bool,
}

/// Start streaming status events. The stream ends with `UNAVAILABLE` when the server stops
/// serving and with `DATA_LOSS` if the client falls behind.
pub fn watch(
    app: &Arc<AppState>,
    principal: &Principal,
    filter: WatchFilter,
    stop: CancellationToken,
) -> Result<mpsc::Receiver<Result<pb::StatusEvent, Status>>, Status> {
    for p in &filter.pools {
        convert::check_str("pool", p, 64)?;
        if app.pool(p).is_none() {
            return Err(Status::not_found(format!("unknown pool '{p}'")));
        }
        check_pool(principal, p)?;
    }
    if filter.ids.len() + filter.keys.len() > app.limits.max_tasks_per_batch {
        return Err(Status::invalid_argument("too many ids/keys in the filter"));
    }
    for k in &filter.keys {
        convert::check_str("key", k, app.limits.max_key_len)?;
    }
    let allowed: Option<Vec<String>> = match (&principal.pools, filter.pools.is_empty()) {
        (_, false) => Some(filter.pools.clone()),
        (Some(ps), true) => Some(ps.iter().map(|p| p.to_string()).collect()),
        (None, true) => None,
    };
    let names: Arc<[Arc<str>]> = app.sched.pools().into();
    let clock = *app.sched.clock();
    let mut events = app.sched.events();
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        loop {
            let ev = tokio::select! {
                e = events.recv() => e,
                _ = stop.cancelled() => {
                    let _ = tx.send(Err(Status::unavailable("server is shutting down; reconnect to the leader"))).await;
                    return;
                }
                _ = tx.closed() => return,
            };
            let e = match ev {
                Ok(e) => e,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let _ = tx
                        .send(Err(Status::data_loss(format!(
                            "watch fell behind; {n} events dropped"
                        ))))
                        .await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    let _ = tx.send(Err(Status::unavailable("scheduler stopped"))).await;
                    return;
                }
            };
            if !filter.all_transitions && !e.status.is_terminal() {
                continue;
            }
            if !filter.ids.is_empty() && !filter.ids.contains(&e.id.0) {
                continue;
            }
            if !filter.keys.is_empty()
                && !e
                    .key
                    .as_deref()
                    .is_some_and(|k| filter.keys.iter().any(|f| f == k))
            {
                continue;
            }
            let ev = convert::event(&e, &names, &clock);
            if allowed.as_ref().is_some_and(|ps| !ps.contains(&ev.pool)) {
                continue;
            }
            if tx.send(Ok(ev)).await.is_err() {
                return;
            }
        }
    });
    Ok(rx)
}

/// `cronoq.v1.Tasks`.
pub struct TasksService {
    /// Server state.
    pub shared: Arc<Shared>,
}

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::StatusEvent, Status>> + Send>>;

#[tonic::async_trait]
impl pb::tasks_server::Tasks for TasksService {
    async fn submit(
        &self,
        req: Request<pb::SubmitRequest>,
    ) -> Result<Response<pb::SubmitResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let spec = r
            .task
            .ok_or_else(|| Status::invalid_argument("missing task"))?;
        let wait = if r.wait {
            Some(wait_bound(r.wait_timeout_s, &app.limits)?)
        } else {
            None
        };
        Ok(Response::new(
            submit_one(app, &p, spec, wait, &self.shared.stop).await?,
        ))
    }

    async fn submit_batch(
        &self,
        req: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(
            submit_batch(app, &p, req.into_inner().tasks).await?,
        ))
    }

    async fn cancel(
        &self,
        req: Request<pb::CancelRequest>,
    ) -> Result<Response<pb::CancelResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let target = convert::task_ref(req.into_inner().task, &app.limits)?;
        authorize_task(app, &p, &target).await?;
        Ok(Response::new(pb::CancelResponse {
            cancelled: cancel(app, target).await?,
        }))
    }

    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::TaskInfo>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let target = convert::task_ref(req.into_inner().task, &app.limits)?;
        authorize_task(app, &p, &target).await?;
        Ok(Response::new(task_info(app, target).await?))
    }

    async fn wait(
        &self,
        req: Request<pb::WaitRequest>,
    ) -> Result<Response<pb::TaskResult>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let target = convert::task_ref(r.task, &app.limits)?;
        let timeout = wait_bound(r.timeout_s, &app.limits)?;
        authorize_task(app, &p, &target).await?;
        Ok(Response::new(
            wait_result(app, target, timeout, &self.shared.stop).await?,
        ))
    }

    type WatchStream = EventStream;

    async fn watch(
        &self,
        req: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let filter = WatchFilter {
            pools: r.pools,
            ids: r.ids,
            keys: r.keys,
            all_transitions: r.all_transitions,
        };
        let rx = watch(app, &p, filter, self.shared.stop.clone())?;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// `cronoq.v1.Workers`.
pub struct WorkersService {
    /// Server state.
    pub shared: Arc<Shared>,
}

type ServerStream = Pin<Box<dyn Stream<Item = Result<pb::ServerMessage, Status>> + Send>>;

#[tonic::async_trait]
impl pb::workers_server::Workers for WorkersService {
    type WorkStream = ServerStream;

    async fn work(
        &self,
        req: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::WorkStream>, Status> {
        let principal = check_grpc(&self.shared.auth, &req, Role::Worker)?;
        let app = self.shared.app_or_unavailable()?.clone();
        let mut inbound = req.into_inner();
        let first = tokio::time::timeout(REGISTER_TIMEOUT, inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("expected Register within 10s"))??
            .and_then(|m| m.msg);
        let Some(pb::worker_message::Msg::Register(reg)) = first else {
            return Err(Status::invalid_argument(
                "expected Register as first message",
            ));
        };
        convert::check_str("pool", &reg.pool, 64)?;
        let Some(info) = app.pool(&reg.pool).cloned() else {
            return Err(Status::not_found(format!("unknown pool '{}'", reg.pool)));
        };
        check_pool(&principal, &reg.pool)?;
        let pool: Arc<str> = app
            .pools
            .get_key_value(reg.pool.as_str())
            .map(|(k, _)| k.clone())
            .unwrap();
        let name = convert::sanitize_name(&reg.worker_name);
        let slots = reg.slots.min(info.slot_cap);
        let (tx, rx) = mpsc::channel(channel_capacity(info.slot_cap));
        let registry = app.registry.clone();
        let worker = registry.next_worker_id();
        // The ack is queued before the worker becomes eligible for leases.
        let _ = tx.try_send(Ok(pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Registered(pb::Registered {
                worker_id: worker,
                lease_ttl_s: Some(info.lease_ttl.as_secs_f64()),
                slots,
                protocol_version: cronoq_proto::PROTOCOL_VERSION,
                max_message_bytes: app.limits.max_message_bytes as u64,
            })),
        }));
        let cut = CancellationToken::new();
        if !registry.register(worker, pool.clone(), name.clone(), slots, tx, cut.clone()) {
            return Err(Status::unavailable("server is shutting down"));
        }
        if slots < reg.slots {
            tracing::info!(worker, pool = %pool, requested = reg.slots, granted = slots, "worker slots capped");
        }
        tracing::info!(worker, pool = %pool, slots, name = ?name, "worker registered");
        let slot_cap = info.slot_cap;
        let inbound_cut = cut.clone();
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    m = inbound.message() => m,
                    _ = inbound_cut.cancelled() => break,
                };
                let msg = match msg {
                    Ok(Some(m)) => m,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(worker, error = %e, "worker stream error");
                        break;
                    }
                };
                use pb::worker_message::Msg;
                match msg.msg {
                    Some(Msg::Heartbeat(h)) => registry.heartbeat(worker, h.lease),
                    Some(Msg::Complete(c)) => {
                        registry.finish(worker, c.lease, Ok(Bytes::from(c.output)));
                    }
                    Some(Msg::Fail(f)) => {
                        let message = convert::sanitize_message(&f.message);
                        let err = if f.permanent {
                            TaskError::permanent(message)
                        } else {
                            TaskError::Retryable {
                                message: message.into(),
                                retry_after: f.retry_after_s.and_then(|s| convert::secs(s).ok()),
                            }
                        };
                        registry.finish(worker, f.lease, Err(err));
                    }
                    Some(Msg::SetSlots(s)) => registry.set_slots(worker, s.slots.min(slot_cap)),
                    Some(Msg::Nack(n)) => {
                        registry.nack(worker, n.lease, &convert::sanitize_message(&n.reason))
                    }
                    Some(Msg::Cancelled(c)) => {
                        registry.finish(worker, c.lease, Err(TaskError::Cancelled));
                    }
                    Some(Msg::Register(_)) => {
                        registry.reject(
                            worker,
                            Status::invalid_argument("duplicate Register on one stream"),
                        );
                        break;
                    }
                    None => {}
                }
            }
            registry.unregister(worker);
            tracing::info!(worker, "worker disconnected");
        });
        let out = ReceiverStream::new(rx).take_until(cut.cancelled_owned());
        Ok(Response::new(Box::pin(out)))
    }
}

/// `cronoq.v1.Admin`.
pub struct AdminService {
    /// Server state.
    pub shared: Arc<Shared>,
}

/// The scheduler snapshot as a protobuf message.
pub async fn snapshot(app: &AppState) -> Result<pb::SnapshotResponse, Status> {
    let s = app.sched.snapshot().await.map_err(convert::error)?;
    let registry = app.registry.clone();
    Ok(convert::snapshot(&s, &|p| registry.pool_worker_count(p)))
}

#[tonic::async_trait]
impl pb::admin_server::Admin for AdminService {
    async fn snapshot(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(snapshot(app).await?))
    }

    async fn add_recurring(
        &self,
        req: Request<pb::RecurringRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        convert::check_str("name", &r.name, MAX_SCHEDULE_FIELD)?;
        convert::check_str("cron", &r.cron, MAX_SCHEDULE_FIELD)?;
        convert::check_str("tz", &r.tz, 64)?;
        if r.name.is_empty() {
            return Err(Status::invalid_argument("recurring schedule needs a name"));
        }
        let template = convert::task(r.template.unwrap_or_default(), &app.limits)?;
        let mut rec = Recurring::new(r.name, r.cron, template).overlap(if r.allow_overlap {
            OverlapPolicy::Allow
        } else {
            OverlapPolicy::Skip
        });
        if !r.tz.is_empty() {
            rec = rec.tz(r.tz);
        }
        app.sched.add_recurring(rec).await.map_err(convert::error)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn remove_recurring(
        &self,
        req: Request<pb::RemoveRecurringRequest>,
    ) -> Result<Response<pb::RemoveRecurringResponse>, Status> {
        check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let name = req.into_inner().name;
        convert::check_str("name", &name, MAX_SCHEDULE_FIELD)?;
        let removed = app
            .sched
            .remove_recurring(&name)
            .await
            .map_err(convert::error)?;
        Ok(Response::new(pb::RemoveRecurringResponse { removed }))
    }
}
