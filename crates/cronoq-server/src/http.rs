//! HTTP/JSON gateway, health checks and Prometheus metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cronoq::{TaskId, TaskRef};
use cronoq_proto::v1 as pb;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::Role;
use crate::grpc;
use crate::state::AppState;

type St = Arc<AppState>;

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<tonic::Status> for ApiError {
    fn from(s: tonic::Status) -> Self {
        use tonic::Code::*;
        let code = match s.code() {
            InvalidArgument => StatusCode::BAD_REQUEST,
            NotFound => StatusCode::NOT_FOUND,
            FailedPrecondition => StatusCode::CONFLICT,
            ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Unauthenticated => StatusCode::UNAUTHORIZED,
            PermissionDenied => StatusCode::FORBIDDEN,
            DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
            Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, s.message().to_string())
    }
}

fn authorize(state: &AppState, headers: &HeaderMap, role: Role) -> Result<(), ApiError> {
    let h = headers.get("authorization").and_then(|v| v.to_str().ok());
    state
        .auth
        .check(h, role)
        .map_err(|e| ApiError::from(e.to_status()))
}

/// Routes that are always available (health), plus the API when `state` is ready.
pub fn health_router(ready: Arc<std::sync::atomic::AtomicBool>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(move || {
                let ready = ready.clone();
                async move {
                    if ready.load(Ordering::SeqCst) {
                        (StatusCode::OK, "ready")
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "standby")
                    }
                }
            }),
        )
}

pub fn api_router(state: St) -> Router {
    Router::new()
        .route("/v1/tasks", post(submit))
        .route("/v1/tasks:batch", post(submit_batch))
        .route("/v1/tasks/{task}", get(get_task).delete(cancel))
        .route("/v1/tasks/{task}/result", get(result))
        .route("/v1/snapshot", get(snapshot))
        .route("/metrics", get(metrics))
        .with_state(state)
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TaskJson {
    /// JSON payload (stored as `application/json`).
    payload: Option<Value>,
    /// Binary payload, base64-encoded.
    payload_b64: Option<String>,
    content_type: Option<String>,
    kind: Option<String>,
    pool: Option<String>,
    priority: Option<String>,
    key: Option<String>,
    deadline_in_s: Option<f64>,
    deadline_unix_ns: Option<i64>,
    start_in_s: Option<f64>,
    start_unix_ns: Option<i64>,
    tolerance_s: Option<f64>,
    on_miss: Option<String>,
    duration_hint_s: Option<f64>,
    timeout_s: Option<f64>,
    retry: Option<RetryJson>,
    /// Keys (strings) or ids (numbers) that must succeed first.
    #[serde(default)]
    after: Vec<Value>,
    #[serde(default)]
    deps: Vec<DepJson>,
    #[serde(default)]
    resources: HashMap<String, u32>,
    #[serde(default)]
    tags: HashMap<String, String>,
    #[serde(default)]
    wait: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryJson {
    max_attempts: u32,
    #[serde(default)]
    initial_backoff_s: f64,
    #[serde(default)]
    multiplier: f64,
    #[serde(default)]
    max_backoff_s: f64,
    #[serde(default = "yes")]
    jitter: bool,
}

fn yes() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DepJson {
    id: Option<u64>,
    key: Option<String>,
    #[serde(default)]
    cond: Option<String>,
}

fn bad(m: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, m.into())
}

fn json_ref(v: &Value) -> Result<pb::TaskRef, ApiError> {
    use pb::task_ref::Target;
    match v {
        Value::Number(n) => n
            .as_u64()
            .map(|id| pb::TaskRef {
                target: Some(Target::Id(id)),
            })
            .ok_or_else(|| bad("invalid id")),
        Value::String(s) => Ok(pb::TaskRef {
            target: Some(Target::Key(s.clone())),
        }),
        _ => Err(bad("dependency must be a key (string) or id (number)")),
    }
}

impl TaskJson {
    fn into_proto(self) -> Result<(pb::TaskSpec, bool), ApiError> {
        let (payload, default_ct) = match (self.payload, self.payload_b64) {
            (Some(_), Some(_)) => return Err(bad("use either payload or payload_b64")),
            (Some(v), None) => (serde_json::to_vec(&v).unwrap(), "application/json"),
            (None, Some(b)) => (
                B64.decode(b)
                    .map_err(|e| bad(format!("payload_b64: {e}")))?,
                "application/octet-stream",
            ),
            (None, None) => (Vec::new(), ""),
        };
        let priority = match self.priority.as_deref() {
            None | Some("normal") => pb::Priority::Normal,
            Some("critical") => pb::Priority::Critical,
            Some("high") => pb::Priority::High,
            Some("low") => pb::Priority::Low,
            Some(p) => return Err(bad(format!("unknown priority '{p}'"))),
        };
        let on_miss = match self.on_miss.as_deref() {
            None | Some("run_late") => pb::MissPolicy::RunLate,
            Some("skip") => pb::MissPolicy::Skip,
            Some("fail") => pb::MissPolicy::Fail,
            Some(p) => return Err(bad(format!("unknown on_miss '{p}'"))),
        };
        let mut deps = Vec::new();
        for a in &self.after {
            deps.push(pb::Dep {
                target: Some(json_ref(a)?),
                cond: pb::EdgeCond::OnSuccess as i32,
            });
        }
        for d in self.deps {
            let target = match (d.id, d.key) {
                (Some(id), None) => json_ref(&Value::from(id))?,
                (None, Some(k)) => json_ref(&Value::String(k))?,
                _ => return Err(bad("dependency needs exactly one of id or key")),
            };
            let cond = match d.cond.as_deref() {
                None | Some("on_success") => pb::EdgeCond::OnSuccess,
                Some("on_complete") => pb::EdgeCond::OnComplete,
                Some("on_failure") => pb::EdgeCond::OnFailure,
                Some(c) => return Err(bad(format!("unknown cond '{c}'"))),
            };
            deps.push(pb::Dep {
                target: Some(target),
                cond: cond as i32,
            });
        }
        let spec = pb::TaskSpec {
            payload,
            content_type: self.content_type.unwrap_or_else(|| default_ct.into()),
            kind: self.kind.unwrap_or_default(),
            pool: self.pool.unwrap_or_default(),
            priority: priority as i32,
            key: self.key.unwrap_or_default(),
            deadline_in_s: self.deadline_in_s,
            deadline_unix_ns: self.deadline_unix_ns,
            start_in_s: self.start_in_s,
            start_unix_ns: self.start_unix_ns,
            tolerance_s: self.tolerance_s,
            on_miss: on_miss as i32,
            duration_hint_s: self.duration_hint_s,
            timeout_s: self.timeout_s,
            retry: self.retry.map(|r| pb::RetryPolicy {
                max_attempts: r.max_attempts,
                initial_backoff_s: r.initial_backoff_s,
                multiplier: r.multiplier,
                max_backoff_s: r.max_backoff_s,
                jitter: r.jitter,
            }),
            deps,
            resources: self.resources,
            tags: self.tags,
        };
        Ok((spec, self.wait))
    }
}

fn status_name(s: i32) -> &'static str {
    match pb::TaskStatus::try_from(s).unwrap_or(pb::TaskStatus::Unspecified) {
        pb::TaskStatus::Unspecified => "unknown",
        pb::TaskStatus::Blocked => "blocked",
        pb::TaskStatus::Scheduled => "scheduled",
        pb::TaskStatus::Ready => "ready",
        pb::TaskStatus::Running => "running",
        pb::TaskStatus::Retrying => "retrying",
        pb::TaskStatus::Succeeded => "succeeded",
        pb::TaskStatus::Failed => "failed",
        pb::TaskStatus::DeadLettered => "dead_lettered",
        pb::TaskStatus::Cancelled => "cancelled",
        pb::TaskStatus::Skipped => "skipped",
        pb::TaskStatus::Missed => "missed",
        pb::TaskStatus::Shed => "shed",
    }
}

fn result_json(r: &pb::TaskResult) -> Value {
    let mut v = json!({ "id": r.id, "status": status_name(r.status) });
    if !r.message.is_empty() {
        v["message"] = r.message.clone().into();
    }
    if let Some(out) = &r.output {
        v["output_b64"] = B64.encode(out).into();
        if let Ok(j) = serde_json::from_slice::<Value>(out) {
            v["output"] = j;
        }
    }
    v
}

fn parse_ref(s: &str) -> TaskRef {
    match s.parse::<u64>() {
        Ok(id) => TaskRef::Id(TaskId(id)),
        Err(_) => TaskRef::Key(s.into()),
    }
}

async fn submit(
    State(st): State<St>,
    headers: HeaderMap,
    Json(body): Json<TaskJson>,
) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let (spec, wait) = body.into_proto()?;
    let r = grpc::submit_one(&st, spec, wait).await?;
    let mut v = json!({ "id": r.id, "duplicate": r.duplicate });
    if let Some(res) = &r.result {
        v["result"] = result_json(res);
    }
    Ok(Json(v))
}

#[derive(Deserialize)]
struct BatchJson {
    tasks: Vec<TaskJson>,
}

async fn submit_batch(
    State(st): State<St>,
    headers: HeaderMap,
    Json(body): Json<BatchJson>,
) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let specs = body
        .tasks
        .into_iter()
        .map(|t| t.into_proto().map(|x| x.0))
        .collect::<Result<Vec<_>, _>>()?;
    let r = grpc::submit_batch(&st, specs).await?;
    Ok(Json(
        json!({ "tasks": r.tasks.iter().map(|t| json!({"id": t.id, "duplicate": t.duplicate})).collect::<Vec<_>>() }),
    ))
}

async fn get_task(
    State(st): State<St>,
    headers: HeaderMap,
    Path(task): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let info = grpc::task_info(&st, parse_ref(&task)).await?;
    if !info.found {
        return Err(ApiError(StatusCode::NOT_FOUND, "unknown task".into()));
    }
    Ok(Json(
        json!({ "id": info.id, "status": status_name(info.status) }),
    ))
}

async fn cancel(
    State(st): State<St>,
    headers: HeaderMap,
    Path(task): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let cancelled = st
        .sched
        .cancel(parse_ref(&task))
        .await
        .map_err(crate::convert::error)?;
    Ok(Json(json!({ "cancelled": cancelled })))
}

#[derive(Deserialize)]
struct WaitQuery {
    timeout_s: Option<f64>,
}

async fn result(
    State(st): State<St>,
    headers: HeaderMap,
    Path(task): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let t = q.timeout_s.unwrap_or(30.0);
    if !t.is_finite() {
        return Err(bad("timeout_s must be a finite number"));
    }
    let timeout = Some(Duration::from_secs_f64(t.clamp(0.0, 3600.0)));
    let r = grpc::wait_result(&st, parse_ref(&task), timeout).await?;
    Ok(Json(result_json(&r)))
}

async fn snapshot(State(st): State<St>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
    authorize(&st, &headers, Role::Client)?;
    let s = st.sched.snapshot().await.map_err(crate::convert::error)?;
    let pools: Vec<Value> = s
        .pools
        .iter()
        .map(|p| {
            json!({
                "name": &*p.name, "capacity": p.capacity, "busy": p.busy, "held": p.held,
                "desired": p.desired, "pending": p.pending(), "pending_by_class": p.pending_by_class,
                "reservations": p.reservations, "dispatched": p.dispatched, "succeeded": p.succeeded,
                "failed": p.failed, "priority_adherence": p.priority_adherence,
                "fixed_start_hit_rate": p.fixed_start_hit_rate, "utilization": p.utilization,
                "schedule_efficiency": p.schedule_efficiency,
                "wait_p50_s": p.wait_p50.as_secs_f64(), "wait_p90_s": p.wait_p90.as_secs_f64(),
                "wait_p99_s": p.wait_p99.as_secs_f64(), "cost": p.cost,
            })
        })
        .collect();
    Ok(Json(
        json!({ "pools": pools, "live_tasks": s.live_tasks, "running": s.running, "pending": s.pending }),
    ))
}

async fn metrics(State(st): State<St>) -> Result<Response, ApiError> {
    let s = st.sched.snapshot().await.map_err(crate::convert::error)?;
    let body = crate::metrics::render(&s, st.registry.worker_count());
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
        .into_response())
}
