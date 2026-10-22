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
use cronoq::{
    OverlapPolicy, Recurring, TaskError, TaskFailure, TaskId, TaskRecord, TaskRef, TaskStatus,
};
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
            match &app.queries {
                Some(q) => Ok(q.get_task(target).await.map_err(store_error)?.map(|r| r.id)),
                None => Ok(None),
            }
        }
    }
}

fn store_error(e: cronoq::StoreError) -> Status {
    convert::error(cronoq::Error::Store(e))
}

/// Check that `principal` may see the task `target` refers to, and return the reference to act
/// on. Unrestricted principals pass without a lookup; restricted ones get `NOT_FOUND` for tasks
/// of other (or unknown) pools, and the id that was checked, so a key cannot resolve to a
/// different task between the check and the action.
pub async fn authorize_task(
    app: &AppState,
    principal: &Principal,
    target: TaskRef,
) -> Result<TaskRef, Status> {
    if principal.pools.is_none() {
        return Ok(target);
    }
    let id = resolve_id(app, &target).await?.ok_or_else(not_found)?;
    match app.pool_of(id).await {
        Some(pool) if principal.may_use_pool(&pool) => Ok(TaskRef::Id(id)),
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
    let Some(q) = &app.queries else {
        return Ok(None);
    };
    let Some(r) = q.get_task(target).await.map_err(store_error)? else {
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
        // The task succeeded earlier but its output is no longer held by the runtime; fall
        // back to the server's output cache and the store.
        Err(TaskFailure::OutputUnavailable { .. }) => {
            let output = app.output(id).await;
            pb::TaskResult {
                id: id.0,
                status: pb::TaskStatus::Succeeded as i32,
                output_available: output.is_some(),
                output: output.map(|b| b.to_vec()),
                message: String::new(),
            }
        }
        Err(TaskFailure::Shutdown) => {
            return Err(Status::unavailable(
                "scheduler shut down before the task finished; wait again on the new leader",
            ));
        }
        Err(e) => return Err(Status::internal(e.to_string())),
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
            tenant: (!spec.tenant.is_empty()).then(|| Arc::from(spec.tenant.as_str())),
            priority: convert::priority(spec.priority).ok(),
            ..Default::default()
        },
    );
}

/// For pool-restricted callers, refuse task keys and dependencies that belong to tasks of
/// other pools: a duplicate key would otherwise hand back that task's id and result, and a
/// dependency would expose its outcome. Keys the same batch introduces (`batch_keys`, which
/// excludes keys of existing tasks) and dependencies on keys that do not exist yet are allowed.
async fn check_cross_pool(
    app: &AppState,
    principal: &Principal,
    spec: &pb::TaskSpec,
    batch_keys: &[&str],
) -> Result<(), Status> {
    if principal.pools.is_none() {
        return Ok(());
    }
    if !spec.key.is_empty() && !batch_keys.contains(&spec.key.as_str()) {
        let key = TaskRef::Key(spec.key.as_str().into());
        if let Some(id) = resolve_id(app, &key).await? {
            check_duplicate(app, principal, id).await?;
        }
    }
    for d in &spec.deps {
        let target = convert::task_ref(d.target.clone(), &app.limits)?;
        if let TaskRef::Key(k) = &target
            && batch_keys.contains(&&**k)
        {
            continue;
        }
        if let Some(id) = resolve_id(app, &target).await? {
            match app.pool_of(id).await {
                Some(pool) if principal.may_use_pool(&pool) => {}
                _ => return Err(Status::not_found("unknown dependency")),
            }
        }
    }
    Ok(())
}

/// Credentials with a tenant submit for that tenant only: fill it in, or refuse another one.
fn pin_tenant(principal: &Principal, spec: &mut pb::TaskSpec) -> Result<(), Status> {
    let Some(t) = &principal.tenant else {
        return Ok(());
    };
    if spec.tenant.is_empty() {
        spec.tenant = t.to_string();
        Ok(())
    } else if spec.tenant == **t {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "credentials do not allow tenant '{}'",
            spec.tenant
        )))
    }
}

/// A duplicate submission must not reveal a task of a pool the caller may not use.
async fn check_duplicate(app: &AppState, principal: &Principal, id: TaskId) -> Result<(), Status> {
    if principal.pools.is_none() {
        return Ok(());
    }
    match app.pool_of(id).await {
        Some(pool) if principal.may_use_pool(&pool) => Ok(()),
        _ => Err(Status::already_exists("task key is already in use")),
    }
}

/// Validate and submit one task, optionally waiting (bounded by `wait_timeout`) for its result.
pub async fn submit_one(
    app: &AppState,
    principal: &Principal,
    mut spec: pb::TaskSpec,
    wait: Option<Duration>,
    stop: &CancellationToken,
) -> Result<pb::SubmitResponse, Status> {
    pin_tenant(principal, &mut spec)?;
    check_pool(principal, convert::spec_pool(&spec))?;
    check_cross_pool(app, principal, &spec, &[]).await?;
    let meta = pb::TaskSpec {
        payload: Vec::new(),
        ..spec.clone()
    };
    let task = convert::task(spec, &app.limits)?;
    match wait {
        Some(timeout) => {
            let h = app.sched.submit(task).await.map_err(convert::error)?;
            let (id, duplicate) = (h.id(), h.is_duplicate());
            if duplicate {
                // The key may have been taken in another pool since the check above.
                check_duplicate(app, principal, id).await?;
            } else {
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
            if s.duplicate {
                check_duplicate(app, principal, s.id).await?;
            } else {
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
    mut specs: Vec<pb::TaskSpec>,
) -> Result<pb::SubmitBatchResponse, Status> {
    let max = app.limits.max_tasks_per_batch;
    if specs.len() > max {
        return Err(Status::invalid_argument(format!(
            "at most {max} tasks per batch"
        )));
    }
    for s in &mut specs {
        pin_tenant(principal, s)?;
    }
    // Only keys the batch introduces count as batch-local: a key that already names a task
    // (possibly of another pool) is checked like any other duplicate or dependency, before
    // anything is submitted.
    let mut batch_keys: Vec<&str> = Vec::new();
    if principal.pools.is_some() {
        for s in specs.iter().filter(|s| !s.key.is_empty()) {
            let key = TaskRef::Key(s.key.as_str().into());
            if resolve_id(app, &key).await?.is_none() {
                batch_keys.push(&s.key);
            }
        }
    }
    for spec in &specs {
        check_pool(principal, convert::spec_pool(spec))?;
        check_cross_pool(app, principal, spec, &batch_keys).await?;
    }
    let mut metas = Vec::with_capacity(specs.len());
    let mut tasks = Vec::with_capacity(specs.len());
    for spec in specs {
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
        if h.is_duplicate() {
            check_duplicate(app, principal, h.id()).await?;
        } else {
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
    let rec = match (&app.queries, live.is_some() && meta.is_some()) {
        (Some(q), false) => q.get_task(&TaskRef::Id(id)).await.map_err(store_error)?,
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
    Ok(info_message(app, id, status, meta.unwrap_or_default(), rec))
}

/// Build a `TaskInfo` from what the cache (`meta`) and the store (`rec`) know; the cache wins.
fn info_message(
    app: &AppState,
    id: TaskId,
    status: TaskStatus,
    meta: TaskMeta,
    rec: Option<TaskRecord<Bytes>>,
) -> pb::TaskInfo {
    let output_available = status == TaskStatus::Succeeded
        && (app.outputs.get(id).is_some() || rec.as_ref().is_some_and(|r| r.output.is_some()));
    let pool = if meta.pool.is_empty() {
        rec.as_ref().map(|r| r.pool.clone()).unwrap_or_default()
    } else {
        meta.pool.to_string()
    };
    let text = |m: Option<Arc<str>>, r: Option<String>| {
        m.as_deref().map(str::to_string).or(r).unwrap_or_default()
    };
    let rec_key = rec.as_ref().and_then(|r| r.key.clone());
    let rec_kind = rec.as_ref().map(|r| r.kind.clone());
    let rec_tenant = rec.as_ref().and_then(|r| r.tenant.clone());
    let priority = meta.priority.or_else(|| rec.as_ref().map(|r| r.priority));
    let attempt = meta
        .attempt
        .max(rec.as_ref().map(|r| r.attempt).unwrap_or(0));
    pb::TaskInfo {
        id: id.0,
        status: convert::status(status) as i32,
        pool,
        key: text(meta.key, rec_key),
        kind: text(meta.kind, rec_kind),
        attempt,
        max_attempts: meta.max_attempts,
        created_unix_ns: meta.created_ns,
        started_unix_ns: meta.started_ns,
        finished_unix_ns: meta.finished_ns,
        message: text(meta.message, rec.and_then(|r| r.message)),
        output_available,
        tenant: text(meta.tenant, rec_tenant),
        priority: priority.map_or(pb::Priority::Unspecified, convert::priority_to_pb) as i32,
    }
}

/// Default `ListTasks` page size.
const DEFAULT_LIST: usize = 100;

/// The pools a listing covers: the requested ones (each checked), else all the caller may see.
/// `None` means every pool.
fn listing_pools(
    app: &AppState,
    principal: &Principal,
    requested: Vec<String>,
) -> Result<Option<Vec<String>>, Status> {
    if requested.len() > app.limits.max_tasks_per_batch {
        return Err(Status::invalid_argument("too many pools in the filter"));
    }
    for p in &requested {
        convert::check_str("pool", p, 64)?;
        if app.pool(p).is_none() {
            return Err(Status::not_found(format!("unknown pool '{p}'")));
        }
        check_pool(principal, p)?;
    }
    Ok(match (&principal.pools, requested.is_empty()) {
        (_, false) => Some(requested),
        (Some(ps), true) => Some(ps.iter().map(|p| p.to_string()).collect()),
        (None, true) => None,
    })
}

/// One page of tasks, live and finished, in id order.
pub async fn list_tasks(
    app: &AppState,
    principal: &Principal,
    r: pb::ListTasksRequest,
) -> Result<pb::ListTasksResponse, Status> {
    let pools = listing_pools(app, principal, r.pools)?;
    let pools: Vec<&str> = match &pools {
        // Credentials allowing no pool see nothing (an empty list would mean every pool).
        Some(v) if v.is_empty() => return Ok(pb::ListTasksResponse::default()),
        Some(v) => v.iter().map(String::as_str).collect(),
        None => Vec::new(),
    };
    let mut statuses = Vec::new();
    for s in r.statuses {
        let s = convert::status_from_pb(s)?;
        if !statuses.contains(&s) {
            statuses.push(s);
        }
    }
    let (finished, live): (Vec<TaskStatus>, Vec<TaskStatus>) =
        statuses.iter().partition(|s| s.is_terminal());
    let limit = match r.limit {
        0 => DEFAULT_LIST,
        n => n as usize,
    }
    .min(app.limits.max_list);
    let after = (r.after > 0).then_some(TaskId(r.after));
    // One more than a page from each source tells whether another page follows.
    let mut page: std::collections::BTreeMap<u64, pb::TaskInfo> = Default::default();
    if statuses.is_empty() || !live.is_empty() {
        for t in app
            .sched
            .list_tasks(&pools, &live, after, limit + 1)
            .await
            .map_err(convert::error)?
        {
            let meta = app.info.get(t.id.0).unwrap_or_default();
            let rec = TaskRecord {
                id: t.id,
                key: t.key.as_deref().map(str::to_string),
                pool: t.pool.to_string(),
                kind: t.kind.to_string(),
                priority: t.priority,
                tenant: t.tenant.as_deref().map(str::to_string),
                status: Some(t.status),
                attempt: t.attempt,
                message: None,
                output: None,
                updated: SystemTime::UNIX_EPOCH,
            };
            page.insert(t.id.0, info_message(app, t.id, t.status, meta, Some(rec)));
        }
    }
    if statuses.is_empty() || !finished.is_empty() {
        // A task that finished while the live page was read is listed as finished.
        match &app.queries {
            Some(q) => {
                for rec in q
                    .list_finished(&pools, &finished, after, limit + 1)
                    .await
                    .map_err(store_error)?
                {
                    let (id, status) = (rec.id, rec.status.unwrap_or(TaskStatus::Ready));
                    let meta = app.info.get(id.0).unwrap_or_default();
                    page.insert(id.0, info_message(app, id, status, meta, Some(rec)));
                }
            }
            None => {
                for (id, meta) in app
                    .info
                    .list_finished(&pools, &finished, r.after, limit + 1)
                {
                    let status = meta.status.unwrap_or(TaskStatus::Ready);
                    page.insert(id, info_message(app, TaskId(id), status, meta, None));
                }
            }
        }
    }
    let more = page.len() > limit;
    let tasks: Vec<pb::TaskInfo> = page.into_values().take(limit).collect();
    let next_after = if more {
        tasks.last().map_or(0, |t| t.id)
    } else {
        0
    };
    Ok(pb::ListTasksResponse { tasks, next_after })
}

/// Connected workers of the pools the caller may see.
pub fn list_workers(app: &AppState, principal: &Principal) -> pb::ListWorkersResponse {
    let workers = app
        .registry
        .list()
        .into_iter()
        .filter(|w| principal.may_use_pool(&w.pool))
        .map(|w| pb::WorkerInfo {
            id: w.id,
            name: w.name,
            pool: w.pool.to_string(),
            labels: w
                .labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            slots: w.slots,
            busy: w.busy,
            connected_unix_ns: convert::unix_ns(w.connected),
        })
        .collect();
    pb::ListWorkersResponse { workers }
}

/// Recurring schedules of the pools the caller may see, by name.
pub async fn list_recurring(
    app: &AppState,
    principal: &Principal,
) -> Result<pb::ListRecurringResponse, Status> {
    let clock = *app.sched.clock();
    let mut schedules: Vec<pb::RecurringInfo> = app
        .sched
        .list_recurring()
        .await
        .map_err(convert::error)?
        .into_iter()
        .filter(|r| principal.may_use_pool(&r.pool))
        .map(|r| pb::RecurringInfo {
            name: r.name.to_string(),
            cron: r.cron.to_string(),
            tz: r.tz.as_deref().unwrap_or_default().to_string(),
            pool: r.pool.to_string(),
            kind: r.kind.to_string(),
            overlap: convert::overlap_to_pb(r.overlap) as i32,
            next_unix_ns: r.next.map(|t| convert::unix_ns(clock.to_system(t))),
            last_task_id: r.last_task.map(|t| t.0),
        })
        .collect();
    schedules.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(pb::ListRecurringResponse { schedules })
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
        let target = authorize_task(app, &p, target).await?;
        Ok(Response::new(pb::CancelResponse {
            cancelled: cancel(app, target).await?,
        }))
    }

    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::TaskInfo>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let target = convert::task_ref(req.into_inner().task, &app.limits)?;
        let target = authorize_task(app, &p, target).await?;
        Ok(Response::new(task_info(app, target).await?))
    }

    async fn list_tasks(
        &self,
        req: Request<pb::ListTasksRequest>,
    ) -> Result<Response<pb::ListTasksResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(list_tasks(app, &p, req.into_inner()).await?))
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
        let target = authorize_task(app, &p, target).await?;
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
        if reg.protocol_version.max(1) < cronoq_proto::PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "worker protocol version {} is not supported: this server needs version {} \
                 (upgrade the worker)",
                reg.protocol_version.max(1),
                cronoq_proto::PROTOCOL_VERSION
            )));
        }
        convert::check_str("pool", &reg.pool, 64)?;
        let labels = convert::worker_labels(reg.labels, &app.limits)?;
        if !principal.may_claim_labels(&labels) {
            return Err(Status::permission_denied(
                "credentials do not allow this worker's labels",
            ));
        }
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
        if !registry.register(
            worker,
            pool.clone(),
            labels.clone(),
            name.clone(),
            slots,
            tx,
            cut.clone(),
        ) {
            return Err(Status::unavailable("server is shutting down"));
        }
        if slots < reg.slots {
            tracing::info!(worker, pool = %pool, requested = reg.slots, granted = slots, "worker slots capped");
        }
        tracing::info!(worker, pool = %pool, slots, name = ?name, labels = ?labels, "worker registered");
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
    let mut r = convert::snapshot(&s, &|p| registry.pool_worker_count(p));
    // Remote workers' stale reports are dropped before they reach the engine.
    if let Some(t) = &mut r.totals {
        t.stale_completions += registry.stale_reports();
    }
    Ok(r)
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
        let overlap = match pb::Overlap::try_from(r.overlap) {
            Ok(pb::Overlap::Unspecified | pb::Overlap::Skip) => OverlapPolicy::Skip,
            Ok(pb::Overlap::Allow) => OverlapPolicy::Allow,
            Ok(pb::Overlap::Queue) => OverlapPolicy::Queue,
            Ok(pb::Overlap::QueueOnSuccess) => OverlapPolicy::QueueOnSuccess,
            Err(_) => return Err(Status::invalid_argument("unknown overlap policy")),
        };
        let mut rec = Recurring::new(r.name, r.cron, template).overlap(overlap);
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

    async fn list_workers(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::ListWorkersResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(list_workers(app, &p)))
    }

    async fn list_recurring(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::ListRecurringResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(list_recurring(app, &p).await?))
    }
}
