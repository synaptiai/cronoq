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

/// Refuse a pool that another shard serves, naming that shard.
pub fn check_pool_shard(app: &AppState, pool: &str) -> Result<(), Status> {
    match app.shards.as_ref().and_then(|m| m.other_for_pool(pool)) {
        Some(s) => Err(crate::shard::elsewhere(s, &format!("pool '{pool}'"))),
        None => Ok(()),
    }
}

/// Refuse a task id that another shard issued, naming that shard.
fn check_id_shard(app: &AppState, target: &TaskRef) -> Result<(), Status> {
    let TaskRef::Id(id) = target else {
        return Ok(());
    };
    match app.shards.as_ref().and_then(|m| m.other_for_id(*id)) {
        Some(s) => Err(crate::shard::elsewhere(s, &format!("task {id}"))),
        None => Ok(()),
    }
}

/// Refuse a task for another shard's pool, or one that depends on another shard's task:
/// dependencies across shards are not supported.
fn check_spec_shard(app: &AppState, spec: &pb::TaskSpec) -> Result<(), Status> {
    let Some(map) = &app.shards else {
        return Ok(());
    };
    check_pool_shard(app, convert::spec_pool(spec))?;
    for r in spec.resources.keys() {
        if let Some(s) = map.other_for_resource(r) {
            return Err(Status::invalid_argument(format!(
                "resource '{r}' belongs to shard '{}': a task can only use its own shard's \
                 resources",
                s.name
            )));
        }
    }
    for d in &spec.deps {
        if let Some(pb::task_ref::Target::Id(id)) =
            d.target.as_ref().and_then(|t| t.target.as_ref())
            && let Some(s) = map.other_for_id(TaskId(*id))
        {
            return Err(Status::invalid_argument(format!(
                "task {id} is in shard '{}': dependencies across shards are not supported",
                s.name
            )));
        }
    }
    Ok(())
}

/// Resolve a reference to a task id (live in the engine, cached, or persisted).
pub(crate) async fn resolve_id(app: &AppState, target: &TaskRef) -> Result<Option<TaskId>, Status> {
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
    check_id_shard(app, &target)?;
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

/// What the task cache holds for a task submitted as `spec`.
fn submit_meta(app: &AppState, spec: &pb::TaskSpec) -> TaskMeta {
    let pool = convert::spec_pool(spec);
    let max_attempts = match &spec.retry {
        Some(r) => r.max_attempts.max(1),
        None => app.pool(pool).map(|p| p.max_attempts).unwrap_or(0),
    };
    {
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
        }
    }
}

fn record_submit(app: &AppState, id: TaskId, spec: &pb::TaskSpec) {
    app.info.on_submit(id.0, submit_meta(app, spec));
}

/// Record what a submission did in the task cache, and keep a duplicate from revealing a
/// task of a pool the caller may not use.
async fn record_outcome(
    app: &AppState,
    principal: &Principal,
    sub: cronoq::Submitted,
    spec: &pb::TaskSpec,
    renewed: bool,
) -> Result<(), Status> {
    match sub.outcome {
        cronoq::SubmitOutcome::Created => record_submit(app, sub.id, spec),
        // The key may have been taken in another pool since the check before submitting.
        cronoq::SubmitOutcome::Existing => check_duplicate(app, principal, sub.id).await?,
        cronoq::SubmitOutcome::Replaced => {
            check_duplicate(app, principal, sub.id).await?;
            app.info
                .on_replace(sub.id.0, submit_meta(app, spec), renewed);
        }
        cronoq::SubmitOutcome::Pending => {
            check_duplicate(app, principal, sub.id).await?;
            app.info.on_pending(sub.id.0, submit_meta(app, spec));
        }
    }
    Ok(())
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
    } else if spec.tenant != **t {
        return Err(Status::permission_denied(format!(
            "credentials do not allow tenant '{}'",
            spec.tenant
        )));
    }
    // Its ordering keys are its own: it cannot join, block or halt a key of anyone else.
    if !spec.ordering_key.is_empty() {
        spec.ordering_key = tenant_key(principal, &spec.ordering_key);
    }
    Ok(())
}

/// What precedes the ordering keys of tenant `t` in their stored names: the tenant, with `%`
/// and `:` escaped so that no two tenants share a prefix, and a colon.
pub fn tenant_prefix(t: &str) -> String {
    format!("{}:", t.replace('%', "%25").replace(':', "%3A"))
}

/// The stored name of ordering key `key` for these credentials. Credentials pinned to a
/// tenant have their own namespace: `<tenant>:<key>`. Others name keys as they are stored.
pub fn tenant_key(p: &Principal, key: &str) -> String {
    match &p.tenant {
        Some(t) => format!("{}{key}", tenant_prefix(t)),
        None => key.to_string(),
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

/// The status for a submission the scheduler refused. A key conflict names the task that holds
/// the key: for a caller limited to certain pools that must not be a task of another pool
/// (the key may have been taken there after the check made before submitting), so such a
/// conflict is answered like any key that is in use elsewhere.
async fn submit_refused(app: &AppState, principal: &Principal, e: cronoq::Error) -> Status {
    let conflict = match &e {
        cronoq::Error::Submit(cronoq::SubmitError::KeyConflict { id, .. })
        | cronoq::Error::Batch {
            error: cronoq::SubmitError::KeyConflict { id, .. },
            ..
        } => Some(*id),
        _ => None,
    };
    if let Some(id) = conflict
        && let Err(hidden) = check_duplicate(app, principal, id).await
    {
        return hidden;
    }
    convert::error(e)
}

/// `status`, naming the task of a batch it is about.
fn at_index(index: usize, mut status: Status) -> Status {
    if let Ok(v) = index.to_string().parse() {
        status.metadata_mut().insert(convert::BATCH_INDEX, v);
    }
    status
}

/// Validate and submit one task, optionally waiting (bounded by `wait_timeout`) for its result.
pub async fn submit_one(
    app: &AppState,
    principal: &Principal,
    mut spec: pb::TaskSpec,
    opts: cronoq::SubmitOptions,
    wait: Option<Duration>,
    stop: &CancellationToken,
) -> Result<pb::SubmitResponse, Status> {
    let replace = opts.on_key_conflict == cronoq::KeyConflict::ReplaceQueued;
    if replace && spec.key.is_empty() {
        return Err(Status::invalid_argument(
            "on_key_conflict = replace_queued needs a key",
        ));
    }
    pin_tenant(principal, &mut spec)?;
    check_pool(principal, convert::spec_pool(&spec))?;
    check_spec_shard(app, &spec)?;
    check_cross_pool(app, principal, &spec, &[]).await?;
    let meta = pb::TaskSpec {
        payload: Vec::new(),
        ..spec.clone()
    };
    let mut task = convert::task(spec, &app.limits)?;
    if replace {
        task = task.replace_queued();
    }
    if opts.renew_position {
        task = task.renew_position();
    }
    if !replace {
        task = task.on_key_conflict(opts.on_key_conflict);
    }
    match wait {
        Some(timeout) => {
            let h = match app.sched.submit(task).await {
                Ok(h) => h,
                Err(e) => return Err(submit_refused(app, principal, e).await),
            };
            let sub = cronoq::Submitted {
                id: h.id(),
                duplicate: h.is_duplicate(),
                outcome: h.outcome(),
            };
            record_outcome(app, principal, sub, &meta, opts.renew_position).await?;
            let result = await_handle(app, h, timeout, stop).await?;
            Ok(pb::SubmitResponse {
                id: sub.id.0,
                duplicate: sub.duplicate,
                result: Some(result),
                outcome: convert::outcome(sub.outcome) as i32,
            })
        }
        None => {
            let s = match app.sched.enqueue(task).await {
                Ok(s) => s,
                Err(e) => return Err(submit_refused(app, principal, e).await),
            };
            record_outcome(app, principal, s, &meta, opts.renew_position).await?;
            Ok(pb::SubmitResponse {
                id: s.id.0,
                duplicate: s.duplicate,
                result: None,
                outcome: convert::outcome(s.outcome) as i32,
            })
        }
    }
}

/// Validate and submit tasks atomically.
pub async fn submit_batch(
    app: &AppState,
    principal: &Principal,
    mut specs: Vec<pb::TaskSpec>,
    mode: cronoq::KeyConflict,
) -> Result<pb::SubmitBatchResponse, Status> {
    if mode == cronoq::KeyConflict::ReplaceQueued {
        return Err(Status::invalid_argument(
            "on_key_conflict = replace_queued is not supported in a batch",
        ));
    }
    let max = app.limits.max_tasks_per_batch;
    if specs.len() > max {
        return Err(Status::invalid_argument(format!(
            "at most {max} tasks per batch"
        )));
    }
    for (i, s) in specs.iter_mut().enumerate() {
        pin_tenant(principal, s).map_err(|e| at_index(i, e))?;
        check_pool(principal, convert::spec_pool(s)).map_err(|e| at_index(i, e))?;
        // A batch is atomic in one shard's store, so every task must be this shard's.
        check_spec_shard(app, s).map_err(|e| at_index(i, e))?;
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
    for (i, spec) in specs.iter().enumerate() {
        check_cross_pool(app, principal, spec, &batch_keys)
            .await
            .map_err(|e| at_index(i, e))?;
    }
    let mut metas = Vec::with_capacity(specs.len());
    let mut tasks = Vec::with_capacity(specs.len());
    for (i, spec) in specs.into_iter().enumerate() {
        metas.push(pb::TaskSpec {
            payload: Vec::new(),
            ..spec.clone()
        });
        let task = convert::task(spec, &app.limits).map_err(|e| at_index(i, e))?;
        tasks.push(task.on_key_conflict(mode));
    }
    let hs = match app.sched.submit_batch(tasks).await {
        Ok(hs) => hs,
        Err(e) => return Err(submit_refused(app, principal, e).await),
    };
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
                outcome: convert::outcome(h.outcome()) as i32,
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

/// The page size for a requested `limit`: 0 means the default, and `limits.max_list` caps it.
pub(crate) fn list_limit(app: &AppState, requested: u32) -> usize {
    match requested {
        0 => DEFAULT_LIST,
        n => n as usize,
    }
    .min(app.limits.max_list)
}

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
            check_pool(principal, p)?;
            check_pool_shard(app, p)?;
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
    let limit = list_limit(app, r.limit);
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
    Ok(pb::ListTasksResponse {
        tasks,
        next_after,
        unavailable_shards: Vec::new(),
    })
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
    pb::ListWorkersResponse {
        workers,
        unavailable_shards: Vec::new(),
    }
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
    Ok(pb::ListRecurringResponse {
        schedules,
        unavailable_shards: Vec::new(),
    })
}

/// Cancel a task (`false` if it already finished).
pub async fn cancel(app: &AppState, target: TaskRef) -> Result<bool, Status> {
    app.sched.cancel(target).await.map_err(convert::error)
}

/// Change a live task's priority, deadline or tags.
pub async fn update(
    app: &AppState,
    target: TaskRef,
    update: cronoq::Update,
) -> Result<pb::UpdateResponse, Status> {
    let priority = update.new_priority();
    let u = app
        .sched
        .update(target, update)
        .await
        .map_err(convert::error)?;
    if let Some(p) = priority {
        // Lookups read the priority from this cache, which the event stream fills later.
        app.info.set_priority(u.id.0, p);
    }
    Ok(pb::UpdateResponse {
        id: u.id.0,
        status: convert::status(u.status) as i32,
        deferred: u.deferred,
        changed: u.changed,
    })
}

/// Like [`authorize_task`], and for credentials pinned to a tenant also requires the task to
/// belong to that tenant. A task of another tenant is reported as not found.
pub async fn authorize_task_of_tenant(
    app: &AppState,
    principal: &Principal,
    target: TaskRef,
) -> Result<TaskRef, Status> {
    let target = authorize_task(app, principal, target).await?;
    let Some(pinned) = &principal.tenant else {
        return Ok(target);
    };
    let id = resolve_id(app, &target).await?.ok_or_else(not_found)?;
    // The scheduler knows the tenant of every task that has not finished. For a finished one
    // (a cancel that will answer `false`), the task cache or the store does; a finished task
    // whose tenant neither holds any more is not reachable.
    let live = app.sched.task(id).await.map_err(convert::error)?;
    let mut tenant = match &live {
        Some(t) => t.tenant.clone(),
        None => app.info.get(id.0).and_then(|m| m.tenant),
    };
    if live.is_none()
        && tenant.is_none()
        && let Some(q) = &app.queries
    {
        tenant = q
            .get_task(&TaskRef::Id(id))
            .await
            .map_err(store_error)?
            .and_then(|r| r.tenant.map(|t| Arc::from(t.as_str())));
    }
    match tenant {
        Some(t) if t == *pinned => Ok(TaskRef::Id(id)),
        _ => Err(not_found()),
    }
}

/// Which status events a Watch stream delivers.
#[derive(Clone, Debug)]
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
            check_pool(principal, p)?;
            check_pool_shard(app, p)?;
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
        let opts = convert::submit_options(r.on_key_conflict, r.renew_position)?;
        Ok(Response::new(
            crate::route::submit(app, &p, spec, opts, wait, &self.shared.stop).await?,
        ))
    }

    async fn submit_batch(
        &self,
        req: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let mode = convert::key_conflict(r.on_key_conflict)?;
        Ok(Response::new(
            crate::route::submit_batch(app, &p, r.tasks, mode).await?,
        ))
    }

    async fn cancel(
        &self,
        req: Request<pb::CancelRequest>,
    ) -> Result<Response<pb::CancelResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let target = convert::task_ref(req.into_inner().task, &app.limits)?;
        Ok(Response::new(pb::CancelResponse {
            cancelled: crate::route::cancel(app, &p, target).await?,
        }))
    }

    async fn update(
        &self,
        req: Request<pb::UpdateRequest>,
    ) -> Result<Response<pb::UpdateResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let mut req = req.into_inner();
        let target = convert::task_ref(req.task.take(), &app.limits)?;
        Ok(Response::new(
            crate::route::update(app, &p, target, req).await?,
        ))
    }

    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::TaskInfo>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        let target = convert::task_ref(req.into_inner().task, &app.limits)?;
        let (info, missing) = crate::route::get(app, &p, target).await?;
        Ok(crate::route::with_missing(info, &missing))
    }

    async fn list_tasks(
        &self,
        req: Request<pb::ListTasksRequest>,
    ) -> Result<Response<pb::ListTasksResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Client)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(
            crate::route::list_tasks(app, &p, req.into_inner()).await?,
        ))
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
        let (result, missing) =
            crate::route::wait(app, &p, target, timeout, &self.shared.stop).await?;
        Ok(crate::route::with_missing(result, &missing))
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
        let rx = crate::route::watch(app, &p, filter, self.shared.stop.clone())?;
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
            check_pool(&principal, &reg.pool)?;
            // Workers of another shard's pool are sent to that shard's leader.
            check_pool_shard(&app, &reg.pool)?;
            return Err(Status::not_found(format!("unknown pool '{}'", reg.pool)));
        };
        check_pool(&principal, &reg.pool)?;
        let pool: Arc<str> = app
            .pools
            .get_key_value(reg.pool.as_str())
            .map(|(k, _)| k.clone())
            .unwrap();
        let name = convert::sanitize_name(&reg.worker_name);
        // A worker of a paused pool is told so, and why: it will get no leases for now.
        let pool_paused = app
            .sched
            .pauses(&pool)
            .await
            .ok()
            .and_then(|p| p.pool)
            .map(|r| r.to_string());
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
                pool_paused,
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

/// Checks of a schedule's own fields, before it is routed to its shard.
fn check_recurring(r: &pb::RecurringRequest) -> Result<(), Status> {
    convert::check_str("name", &r.name, MAX_SCHEDULE_FIELD)?;
    convert::check_str("cron", &r.cron, MAX_SCHEDULE_FIELD)?;
    convert::check_str("tz", &r.tz, 64)?;
    if r.name.is_empty() {
        return Err(Status::invalid_argument("recurring schedule needs a name"));
    }
    Ok(())
}

/// Add (or replace) a schedule on this shard.
pub async fn add_recurring(app: &AppState, r: pb::RecurringRequest) -> Result<(), Status> {
    check_recurring(&r)?;
    let template = r.template.unwrap_or_default();
    check_spec_shard(app, &template)?;
    let template = convert::task(template, &app.limits)?;
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
    app.sched.add_recurring(rec).await.map_err(convert::error)
}

/// Remove a schedule from this shard.
pub async fn remove_recurring(app: &AppState, name: &str) -> Result<bool, Status> {
    app.sched
        .remove_recurring(name)
        .await
        .map_err(convert::error)
}

/// The pool and key of a call about an ordering key, checked: the default pool if none is
/// named, a pool the caller may use, and a key within the limits.
pub fn ordering_target(
    p: &Principal,
    limits: &LimitsConfig,
    pool: &str,
    key: &str,
) -> Result<String, Status> {
    let pool = if pool.is_empty() { "default" } else { pool };
    convert::check_str("pool", pool, 64)?;
    check_pool(p, pool)?;
    convert::check_str("ordering key", key, limits.max_key_len)?;
    Ok(pool.to_string())
}

/// Like [`ordering_target`] for a call that changes the key, which must be named.
/// (Credentials pinned to a tenant reach the keys of their own namespace only: see
/// [`tenant_key`].)
pub fn ordering_change(
    p: &Principal,
    limits: &LimitsConfig,
    pool: &str,
    key: &str,
) -> Result<String, Status> {
    let pool = ordering_target(p, limits, pool, key)?;
    if key.is_empty() {
        return Err(Status::invalid_argument("an ordering key is required"));
    }
    Ok(pool)
}

/// An ordering key of a pool of this shard.
pub async fn get_ordering(
    app: &AppState,
    pool: &str,
    key: &str,
) -> Result<pb::OrderingInfo, Status> {
    check_pool_shard(app, pool)?;
    let info = app
        .sched
        .ordering(pool, key)
        .await
        .map_err(convert::error)?;
    let class = |c: Option<cronoq::Priority>| c.map(|c| convert::priority_to_pb(c) as i32);
    Ok(match info {
        Some(i) => pb::OrderingInfo {
            pool: pool.to_string(),
            key: key.to_string(),
            tasks: i.tasks as u64,
            head: i.head.map(|id| id.0),
            effective: class(i.effective).unwrap_or_default(),
            floor: class(i.floor),
            persist: i.persist,
            halted: i.halted,
            paused: i.paused.map(|r| r.to_string()),
        },
        None => pb::OrderingInfo {
            pool: pool.to_string(),
            key: key.to_string(),
            ..Default::default()
        },
    })
}

/// Most halted ordering keys one call lists.
pub const MAX_HALTED_LIST: u32 = 1000;

/// The halted ordering keys of a pool of this shard.
pub async fn list_halted_orderings(
    app: &AppState,
    pool: &str,
    limit: u32,
) -> Result<Vec<String>, Status> {
    check_pool_shard(app, pool)?;
    let limit = match limit {
        0 => 100,
        n => n.min(MAX_HALTED_LIST),
    };
    let keys = app
        .sched
        .halted_orderings(pool, limit as usize)
        .await
        .map_err(convert::error)?;
    Ok(keys.iter().map(|k| k.to_string()).collect())
}

/// Every halted ordering key of a pool of this shard (for callers that filter them).
pub async fn list_all_halted_orderings(app: &AppState, pool: &str) -> Result<Vec<String>, Status> {
    check_pool_shard(app, pool)?;
    let keys = app
        .sched
        .halted_orderings(pool, usize::MAX)
        .await
        .map_err(convert::error)?;
    Ok(keys.iter().map(|k| k.to_string()).collect())
}

/// Set or remove the floor of an ordering key of a pool of this shard.
pub async fn set_ordering_floor(
    app: &AppState,
    pool: &str,
    key: &str,
    floor: Option<cronoq::Priority>,
    persist: bool,
) -> Result<(), Status> {
    check_pool_shard(app, pool)?;
    app.sched
        .set_ordering_floor(pool, key, floor, persist)
        .await
        .map_err(convert::error)
}

/// What a pause or resume call names, checked: the pool (the default one if unnamed, and one
/// the caller may use), and the pool itself, one of its classes or one of its ordering keys.
/// Credentials pinned to a tenant reach their own ordering keys only.
pub fn pause_target(
    p: &Principal,
    limits: &LimitsConfig,
    pool: &str,
    class: Option<i32>,
    key: &str,
) -> Result<(String, cronoq::PauseTarget), Status> {
    let pool = ordering_target(p, limits, pool, key)?;
    let target = match (class, key.is_empty()) {
        (Some(_), false) => {
            return Err(Status::invalid_argument(
                "name a class or an ordering key, not both",
            ));
        }
        (Some(c), true) => {
            if c == pb::Priority::Unspecified as i32 {
                return Err(Status::invalid_argument("class must name a class"));
            }
            cronoq::PauseTarget::Class(convert::priority(c)?)
        }
        (None, false) => cronoq::PauseTarget::Ordering(key.into()),
        (None, true) => cronoq::PauseTarget::Pool,
    };
    if p.tenant.is_some() && !matches!(target, cronoq::PauseTarget::Ordering(_)) {
        return Err(Status::permission_denied(
            "credentials pinned to a tenant pause and resume their own ordering keys only",
        ));
    }
    Ok((pool, target))
}

/// `target` as it is stored: an ordering key of pinned credentials has their tenant's prefix.
pub fn tenant_target(p: &Principal, target: cronoq::PauseTarget) -> cronoq::PauseTarget {
    match target {
        cronoq::PauseTarget::Ordering(key) => {
            cronoq::PauseTarget::Ordering(tenant_key(p, &key).into())
        }
        other => other,
    }
}

/// Pause (`reason` set) or resume something of a pool of this shard.
pub async fn pause(
    app: &AppState,
    pool: &str,
    target: cronoq::PauseTarget,
    reason: Option<&str>,
) -> Result<bool, Status> {
    check_pool_shard(app, pool)?;
    match reason {
        Some(reason) => app.sched.pause(pool, target, reason).await,
        None => app.sched.resume(pool, target).await,
    }
    .map_err(convert::error)
}

/// The paused ordering keys of a pool of this shard, with their reasons.
pub async fn list_paused_orderings(
    app: &AppState,
    pool: &str,
    limit: usize,
) -> Result<Vec<(String, String)>, Status> {
    check_pool_shard(app, pool)?;
    let keys = app
        .sched
        .paused_orderings(pool, limit)
        .await
        .map_err(convert::error)?;
    Ok(keys
        .iter()
        .map(|(k, r)| (k.to_string(), r.to_string()))
        .collect())
}

#[tonic::async_trait]
impl pb::admin_server::Admin for AdminService {
    async fn get_ordering(
        &self,
        req: Request<pb::OrderingRequest>,
    ) -> Result<Response<pb::OrderingInfo>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let pool = ordering_target(&p, &app.limits, &r.pool, &r.key)?;
        Ok(Response::new(
            crate::route::get_ordering(app, &p, &pool, &r.key).await?,
        ))
    }

    async fn list_halted_orderings(
        &self,
        req: Request<pb::ListHaltedOrderingsRequest>,
    ) -> Result<Response<pb::ListHaltedOrderingsResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let pool = ordering_target(&p, &app.limits, &r.pool, "")?;
        let keys = crate::route::list_halted_orderings(app, &p, &pool, r.limit).await?;
        Ok(Response::new(pb::ListHaltedOrderingsResponse { keys }))
    }

    async fn set_ordering_floor(
        &self,
        req: Request<pb::SetOrderingFloorRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let pool = ordering_change(&p, &app.limits, &r.pool, &r.key)?;
        if r.floor == Some(pb::Priority::Unspecified as i32) {
            return Err(Status::invalid_argument(
                "floor must name a class; leave it unset to remove the floor",
            ));
        }
        let floor = r.floor.map(convert::priority).transpose()?;
        if r.persist && floor.is_none() {
            return Err(Status::invalid_argument("persist needs a floor"));
        }
        crate::route::set_ordering_floor(app, &p, &pool, &r.key, floor, r.persist).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn list_paused_orderings(
        &self,
        req: Request<pb::ListHaltedOrderingsRequest>,
    ) -> Result<Response<pb::ListPausedOrderingsResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let pool = ordering_target(&p, &app.limits, &r.pool, "")?;
        let keys = crate::route::list_paused_orderings(app, &p, &pool, r.limit).await?;
        Ok(Response::new(pb::ListPausedOrderingsResponse {
            keys: keys
                .into_iter()
                .map(|(key, reason)| pb::PausedOrdering { key, reason })
                .collect(),
        }))
    }

    async fn pause(
        &self,
        req: Request<pb::PauseRequest>,
    ) -> Result<Response<pb::PauseResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        convert::check_str("reason", &r.reason, app.limits.max_tag_len)?;
        let (pool, target) =
            pause_target(&p, &app.limits, &r.pool, r.priority_class, &r.ordering_key)?;
        let paused = crate::route::pause(app, &p, &pool, target, Some(&r.reason)).await?;
        Ok(Response::new(pb::PauseResponse { paused }))
    }

    async fn resume(
        &self,
        req: Request<pb::ResumeRequest>,
    ) -> Result<Response<pb::ResumeResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        let (pool, target) =
            pause_target(&p, &app.limits, &r.pool, r.priority_class, &r.ordering_key)?;
        let resumed = crate::route::pause(app, &p, &pool, target, None).await?;
        Ok(Response::new(pb::ResumeResponse { resumed }))
    }

    async fn snapshot(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(crate::route::snapshot(app, &p).await?))
    }

    async fn add_recurring(
        &self,
        req: Request<pb::RecurringRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let r = req.into_inner();
        check_recurring(&r)?;
        crate::route::add_recurring(app, &p, r).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn remove_recurring(
        &self,
        req: Request<pb::RemoveRecurringRequest>,
    ) -> Result<Response<pb::RemoveRecurringResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Admin)?;
        let app = self.shared.app_or_unavailable()?;
        let name = req.into_inner().name;
        convert::check_str("name", &name, MAX_SCHEDULE_FIELD)?;
        let removed = crate::route::remove_recurring(app, &p, name).await?;
        Ok(Response::new(pb::RemoveRecurringResponse { removed }))
    }

    async fn list_workers(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::ListWorkersResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(crate::route::list_workers(app, &p).await?))
    }

    async fn list_recurring(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::ListRecurringResponse>, Status> {
        let p = check_grpc(&self.shared.auth, &req, Role::Metrics)?;
        let app = self.shared.app_or_unavailable()?;
        Ok(Response::new(crate::route::list_recurring(app, &p).await?))
    }
}
