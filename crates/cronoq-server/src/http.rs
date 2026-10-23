//! HTTP/JSON gateway, health checks and Prometheus metrics.
//!
//! The router is created once and serves for the whole process lifetime: while standing by
//! (or shutting down) the API answers `503` with a JSON error, `/healthz` stays `200` and
//! `/readyz` reports `503`. Requests are authenticated before their body is read.
//!
//! Task ids are JSON strings (they exceed the integer precision of JavaScript numbers).

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cronoq::{TaskId, TaskRef, TaskStatus};
use cronoq_proto::v1 as pb;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::auth::{AuthError, Principal};
use crate::config::Role;
use crate::grpc;
use crate::state::{AppState, Shared};

/// Per-connection facts made available to handlers.
#[derive(Clone, Debug, Default)]
pub struct ConnInfo {
    /// Principal mapped from the client certificate (mTLS), if any.
    pub cert: Option<Principal>,
}

/// An error response: `{"error": "...", "code": "..."}`.
#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self
            .0
            .canonical_reason()
            .unwrap_or("error")
            .to_ascii_lowercase()
            .replace(' ', "_");
        (self.0, Json(json!({ "error": self.1, "code": code }))).into_response()
    }
}

impl From<tonic::Status> for ApiError {
    fn from(s: tonic::Status) -> Self {
        use tonic::Code::*;
        let code = match s.code() {
            InvalidArgument | OutOfRange => StatusCode::BAD_REQUEST,
            NotFound => StatusCode::NOT_FOUND,
            AlreadyExists | FailedPrecondition | Aborted => StatusCode::CONFLICT,
            ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Unauthenticated => StatusCode::UNAUTHORIZED,
            PermissionDenied => StatusCode::FORBIDDEN,
            DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Unimplemented => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, s.message().to_string())
    }
}

impl From<AuthError> for ApiError {
    fn from(e: AuthError) -> Self {
        let mut r = ApiError::from(e.to_status());
        if e != AuthError::Forbidden {
            r.0 = StatusCode::UNAUTHORIZED;
        }
        r
    }
}

fn bad(m: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, m.into())
}

fn unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "not the leader (standby or shutting down)".into(),
    )
}

/// JSON body extractor whose rejections are JSON errors.
pub struct ApiJson<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for ApiJson<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(ApiJson(v)),
            Err(e) => {
                let status = match &e {
                    JsonRejection::MissingJsonContentType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    JsonRejection::BytesRejection(b) => b.status(),
                    _ => StatusCode::BAD_REQUEST,
                };
                Err(ApiError(status, e.body_text()))
            }
        }
    }
}

type St = Arc<Shared>;

fn app(st: &Shared) -> Result<&Arc<AppState>, ApiError> {
    st.app().ok_or_else(unavailable)
}

/// Authenticates (before the body is read), checks readiness, bounds the request time and
/// hands the [`Principal`] to the handler.
async fn gate(State((st, role)): State<(St, Role)>, mut req: Request, next: Next) -> Response {
    if st.loopback_hosts_only && !loopback_host(&req) {
        return ApiError(
            StatusCode::FORBIDDEN,
            "without auth the API only answers requests for a loopback host".into(),
        )
        .into_response();
    }
    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let cert = req
        .extensions()
        .get::<ConnInfo>()
        .and_then(|c| c.cert.clone());
    let mut principal = match st.auth.authorize(header, cert.as_ref(), role) {
        Ok(p) => p,
        Err(e) => {
            let mut r = ApiError::from(e).into_response();
            if r.status() == StatusCode::UNAUTHORIZED {
                r.headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            }
            return r;
        }
    };
    if st.app().is_none() {
        return unavailable().into_response();
    }
    // Waiting routes may block for up to `max_wait_s`.
    let waits = req.uri().path().ends_with("/result") || req.uri().path() == "/v1/tasks";
    let limit = st.limits.request_timeout()
        + if waits {
            st.limits.max_wait()
        } else {
            Duration::ZERO
        };
    principal.forwarded = req.headers().contains_key(crate::auth::FORWARDED_HEADER);
    req.extensions_mut().insert(principal);
    match tokio::time::timeout(limit, next.run(req)).await {
        Ok(r) => r,
        Err(_) => ApiError(StatusCode::GATEWAY_TIMEOUT, "request timed out".into()).into_response(),
    }
}

/// Whether the request's host (the `Host` header, or the HTTP/2 authority) is `localhost`, a
/// `*.localhost` name or a loopback address.
pub(crate) fn loopback_host<B>(req: &::http::Request<B>) -> bool {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.uri().authority().map(|a| a.as_str()));
    let Some(host) = host else { return false };
    let name = match host.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((ip, _)) => ip,
            None => return false,
        },
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    name == "localhost"
        || name.ends_with(".localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn gated(router: Router<St>, st: &St, role: Role) -> Router<St> {
    router.route_layer(from_fn_with_state((st.clone(), role), gate))
}

/// The full HTTP router (health, API, metrics).
pub fn router(st: St) -> Router {
    let api = Router::new()
        .route("/v1/tasks", post(submit).get(list_tasks))
        .route("/v1/tasks:batch", post(submit_batch))
        .route("/v1/tasks/{id}", get(get_by_id).delete(cancel_by_id))
        .route("/v1/tasks/{id}/result", get(result_by_id))
        .route(
            "/v1/tasks/by-key/{key}",
            get(get_by_key).delete(cancel_by_key),
        )
        .route("/v1/tasks/by-key/{key}/result", get(result_by_key));
    let metrics = Router::new()
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/workers", get(list_workers))
        .route("/v1/recurring", get(list_recurring))
        .route("/metrics", get(metrics));
    let body_limit = st.limits.max_http_body_bytes;
    gated(api, &st, Role::Client)
        .merge(gated(metrics, &st, Role::Metrics))
        .merge(health_routes())
        .fallback(|| async { ApiError(StatusCode::NOT_FOUND, "no such route".into()) })
        .method_not_allowed_fallback(|| async {
            ApiError(StatusCode::METHOD_NOT_ALLOWED, "method not allowed".into())
        })
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(st)
}

/// Router for the optional unauthenticated metrics listener (`/metrics`, `/healthz`, `/readyz`).
pub fn metrics_router(st: St) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .merge(health_routes())
        .with_state(st)
}

fn health_routes() -> Router<St> {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(|State(st): State<St>| async move {
                if st.app().is_some() {
                    (StatusCode::OK, "ready")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "standby")
                }
            }),
        )
}

/// Serve `router` on `listener` (optionally over TLS) until `stop` is cancelled, then drain
/// connections for up to `drain`.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    tls: Option<tokio_rustls::TlsAcceptor>,
    st: St,
    stop: CancellationToken,
    drain: Duration,
) {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10));
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(st.limits.max_concurrent_streams)
        .keep_alive_interval(Some(st.limits.keepalive_interval()))
        .keep_alive_timeout(st.limits.keepalive_timeout());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let conns = Arc::new(tokio::sync::Semaphore::new(
        st.limits.max_concurrent_requests.max(1),
    ));
    loop {
        let (tcp, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = stop.cancelled() => break,
        };
        let Ok(permit) = conns.clone().try_acquire_owned() else {
            tracing::warn!(%peer, "too many HTTP connections; refusing");
            continue;
        };
        let _ = tcp.set_nodelay(true);
        let (router, tls, st, builder) = (router.clone(), tls.clone(), st.clone(), builder.clone());
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let _permit = permit;
            match tls {
                None => {
                    let svc = service(router, ConnInfo::default());
                    let conn = builder
                        .serve_connection_with_upgrades(TokioIo::new(tcp), svc)
                        .into_owned();
                    let _ = watcher.watch(conn).await;
                }
                Some(acceptor) => {
                    let stream =
                        match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp))
                            .await
                        {
                            Ok(Ok(s)) => s,
                            Ok(Err(e)) => {
                                tracing::debug!(%peer, error = %e, "TLS handshake failed");
                                return;
                            }
                            Err(_) => return,
                        };
                    let cert = stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|c| c.first())
                        .and_then(|c| st.auth.principal_for_cert(c.as_ref()));
                    let svc = service(router, ConnInfo { cert });
                    let conn = builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), svc)
                        .into_owned();
                    let _ = watcher.watch(conn).await;
                }
            }
        });
    }
    drop(listener);
    if tokio::time::timeout(drain, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::debug!("HTTP connections did not drain in time");
    }
}

fn service(
    router: Router,
    info: ConnInfo,
) -> TowerToHyperService<
    impl tower::Service<
        hyper::Request<hyper::body::Incoming>,
        Response = Response,
        Error = Infallible,
        Future = impl Send + 'static,
    > + Clone
    + Send
    + 'static,
> {
    TowerToHyperService::new(router.map_request(
        move |mut r: hyper::Request<hyper::body::Incoming>| {
            r.extensions_mut().insert(info.clone());
            r.map(axum::body::Body::new)
        },
    ))
}

// ---------------------------------------------------------------------------------------------
// Request and response bodies.

/// A task in JSON form.
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
    /// Keys (strings) or ids (numbers, or `{"id": "..."}`) that must succeed first.
    #[serde(default)]
    after: Vec<Value>,
    #[serde(default)]
    deps: Vec<DepJson>,
    #[serde(default)]
    resources: HashMap<String, u32>,
    #[serde(default)]
    tags: HashMap<String, String>,
    /// Worker labels the task requires.
    #[serde(default)]
    labels: HashMap<String, String>,
    /// Tenant, for pools that share capacity fairly between tenants.
    tenant: Option<String>,
    /// Wait for the result (single submissions only).
    #[serde(default)]
    wait: bool,
    /// Upper bound for `wait` (capped by `limits.max_wait_s`).
    wait_timeout_s: Option<f64>,
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
    /// Task id (string or number).
    id: Option<Value>,
    key: Option<String>,
    #[serde(default)]
    cond: Option<String>,
}

fn json_id(v: &Value) -> Result<u64, ApiError> {
    match v {
        Value::Number(n) => n.as_u64().ok_or_else(|| bad("invalid id")),
        Value::String(s) => s.parse().map_err(|_| bad(format!("invalid id '{s}'"))),
        _ => Err(bad("id must be a string or number")),
    }
}

fn id_ref(id: u64) -> pb::TaskRef {
    pb::TaskRef {
        target: Some(pb::task_ref::Target::Id(id)),
    }
}

fn key_ref(k: String) -> pb::TaskRef {
    pb::TaskRef {
        target: Some(pb::task_ref::Target::Key(k)),
    }
}

impl TaskJson {
    fn into_proto(self) -> Result<(pb::TaskSpec, bool, Option<f64>), ApiError> {
        let (payload, default_ct) = match (self.payload, self.payload_b64) {
            (Some(_), Some(_)) => return Err(bad("use either payload or payload_b64")),
            (Some(v), None) => (
                serde_json::to_vec(&v).map_err(|e| bad(e.to_string()))?,
                "application/json",
            ),
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
            Some(p) => {
                return Err(bad(format!(
                    "unknown priority '{p}' (critical, high, normal, low)"
                )));
            }
        };
        let on_miss = match self.on_miss.as_deref() {
            None | Some("run_late") => pb::MissPolicy::RunLate,
            Some("skip") => pb::MissPolicy::Skip,
            Some("fail") => pb::MissPolicy::Fail,
            Some(p) => return Err(bad(format!("unknown on_miss '{p}' (run_late, skip, fail)"))),
        };
        let mut deps = Vec::new();
        for a in &self.after {
            let target = match a {
                Value::String(k) => key_ref(k.clone()),
                Value::Number(_) => id_ref(json_id(a)?),
                Value::Object(o) => match o.get("id") {
                    Some(id) if o.len() == 1 => id_ref(json_id(id)?),
                    _ => return Err(bad("`after` objects must be {\"id\": \"...\"}")),
                },
                _ => return Err(bad("`after` entries must be keys (strings) or ids")),
            };
            deps.push(pb::Dep {
                target: Some(target),
                cond: pb::EdgeCond::OnSuccess as i32,
            });
        }
        for d in self.deps {
            let target = match (d.id, d.key) {
                (Some(id), None) => id_ref(json_id(&id)?),
                (None, Some(k)) => key_ref(k),
                _ => return Err(bad("dependency needs exactly one of id or key")),
            };
            let cond = match d.cond.as_deref() {
                None | Some("on_success") => pb::EdgeCond::OnSuccess,
                Some("on_complete") => pb::EdgeCond::OnComplete,
                Some("on_failure") => pb::EdgeCond::OnFailure,
                Some(c) => {
                    return Err(bad(format!(
                        "unknown cond '{c}' (on_success, on_complete, on_failure)"
                    )));
                }
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
            labels: self.labels,
            tenant: self.tenant.unwrap_or_default(),
        };
        Ok((spec, self.wait, self.wait_timeout_s))
    }
}

/// Lower-case name of a wire status.
pub fn status_name(s: i32) -> &'static str {
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
    let mut v = json!({
        "id": r.id.to_string(),
        "status": status_name(r.status),
        "output_available": r.output_available,
    });
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

fn info_json(i: &pb::TaskInfo) -> Value {
    let mut v = json!({
        "id": i.id.to_string(),
        "status": status_name(i.status),
        "pool": i.pool,
        "attempt": i.attempt,
        "output_available": i.output_available,
    });
    if !i.key.is_empty() {
        v["key"] = i.key.clone().into();
    }
    if !i.kind.is_empty() {
        v["kind"] = i.kind.clone().into();
    }
    if i.max_attempts > 0 {
        v["max_attempts"] = i.max_attempts.into();
    }
    for (name, t) in [
        ("created_unix_ns", i.created_unix_ns),
        ("started_unix_ns", i.started_unix_ns),
        ("finished_unix_ns", i.finished_unix_ns),
    ] {
        if let Some(t) = t {
            v[name] = t.to_string().into();
        }
    }
    if !i.message.is_empty() {
        v["message"] = i.message.clone().into();
    }
    if !i.tenant.is_empty() {
        v["tenant"] = i.tenant.clone().into();
    }
    if let Some(p) = priority_name(i.priority) {
        v["priority"] = p.into();
    }
    v
}

/// Lower-case name of a wire priority (`None` if unspecified).
fn priority_name(p: i32) -> Option<&'static str> {
    match pb::Priority::try_from(p).ok()? {
        pb::Priority::Unspecified => None,
        pb::Priority::Critical => Some("critical"),
        pb::Priority::High => Some("high"),
        pb::Priority::Normal => Some("normal"),
        pb::Priority::Low => Some("low"),
    }
}

fn overlap_name(o: i32) -> &'static str {
    match pb::Overlap::try_from(o).unwrap_or(pb::Overlap::Unspecified) {
        pb::Overlap::Unspecified | pb::Overlap::Skip => "skip",
        pb::Overlap::Allow => "allow",
        pb::Overlap::Queue => "queue",
        pb::Overlap::QueueOnSuccess => "queue_on_success",
    }
}

fn parse_id(s: &str) -> Result<TaskRef, ApiError> {
    s.parse::<u64>()
        .map(|id| TaskRef::Id(TaskId(id)))
        .map_err(|_| {
            bad(format!(
                "invalid task id '{s}' (use /v1/tasks/by-key/{{key}} for keys)"
            ))
        })
}

fn key(app: &AppState, k: String) -> Result<TaskRef, ApiError> {
    Ok(crate::convert::task_ref(Some(key_ref(k)), &app.limits)?)
}

// ---------------------------------------------------------------------------------------------
// Handlers.

async fn submit(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    ApiJson(body): ApiJson<TaskJson>,
) -> Result<Json<Value>, ApiError> {
    let app = app(&st)?;
    let (spec, wait, wait_timeout) = body.into_proto()?;
    let wait = if wait {
        Some(grpc::wait_bound(wait_timeout, &app.limits)?)
    } else {
        None
    };
    let r = crate::route::submit(app, &p, spec, wait, &st.stop).await?;
    let mut v = json!({ "id": r.id.to_string(), "duplicate": r.duplicate });
    if let Some(res) = &r.result {
        v["result"] = result_json(res);
    }
    Ok(Json(v))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchJson {
    tasks: Vec<TaskJson>,
}

async fn submit_batch(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    ApiJson(body): ApiJson<BatchJson>,
) -> Result<Json<Value>, ApiError> {
    let app = app(&st)?;
    let max = app.limits.max_tasks_per_batch;
    if body.tasks.len() > max {
        return Err(bad(format!("at most {max} tasks per batch")));
    }
    let specs = body
        .tasks
        .into_iter()
        .enumerate()
        .map(|(i, t)| {
            let (spec, wait, _) = t.into_proto()?;
            if wait {
                return Err(bad(format!(
                    "tasks[{i}]: `wait` is not supported in batches; wait on each id"
                )));
            }
            Ok(spec)
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let r = crate::route::submit_batch(app, &p, specs).await?;
    Ok(Json(json!({
        "tasks": r.tasks.iter().map(|t| json!({"id": t.id.to_string(), "duplicate": t.duplicate})).collect::<Vec<_>>()
    })))
}

async fn get_task(st: &Shared, p: &Principal, target: TaskRef) -> Result<Json<Value>, ApiError> {
    let app = app(st)?;
    let (info, missing) = crate::route::get(app, p, target).await?;
    let mut v = info_json(&info);
    note_missing(&mut v, &missing);
    Ok(Json(v))
}

async fn get_by_id(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    get_task(&st, &p, parse_id(&id)?).await
}

async fn get_by_key(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(k): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let target = key(app(&st)?, k)?;
    get_task(&st, &p, target).await
}

async fn cancel_task(st: &Shared, p: &Principal, target: TaskRef) -> Result<Json<Value>, ApiError> {
    let app = app(st)?;
    Ok(Json(
        json!({ "cancelled": crate::route::cancel(app, p, target).await? }),
    ))
}

async fn cancel_by_id(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    cancel_task(&st, &p, parse_id(&id)?).await
}

async fn cancel_by_key(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(k): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let target = key(app(&st)?, k)?;
    cancel_task(&st, &p, target).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitQuery {
    timeout_s: Option<f64>,
}

async fn wait_task(
    st: &Shared,
    p: &Principal,
    target: TaskRef,
    q: WaitQuery,
) -> Result<Json<Value>, ApiError> {
    let app = app(st)?;
    let timeout = grpc::wait_bound(q.timeout_s, &app.limits)?;
    let (result, missing) = crate::route::wait(app, p, target, timeout, &st.stop).await?;
    let mut v = result_json(&result);
    note_missing(&mut v, &missing);
    Ok(Json(v))
}

fn query(
    q: Result<Query<WaitQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<WaitQuery, ApiError> {
    q.map(|Query(q)| q).map_err(|e| bad(e.body_text()))
}

async fn result_by_id(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    q: Result<Query<WaitQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    wait_task(&st, &p, parse_id(&id)?, query(q)?).await
}

async fn result_by_key(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    Path(k): Path<String>,
    q: Result<Query<WaitQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let target = key(app(&st)?, k)?;
    wait_task(&st, &p, target, query(q)?).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    /// Comma-separated pool names.
    pool: Option<String>,
    /// Comma-separated status names.
    status: Option<String>,
    after: Option<String>,
    limit: Option<u32>,
}

fn csv(s: Option<&str>) -> impl Iterator<Item = &str> {
    s.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
}

async fn list_tasks(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
    q: Result<Query<ListQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(|e| bad(e.body_text()))?;
    let mut statuses = Vec::new();
    for name in csv(q.status.as_deref()) {
        let s =
            TaskStatus::from_name(name).ok_or_else(|| bad(format!("unknown status '{name}'")))?;
        statuses.push(crate::convert::status(s) as i32);
    }
    let after = match q.after.as_deref() {
        None | Some("") => 0,
        Some(a) => a
            .parse::<u64>()
            .map_err(|_| bad(format!("invalid task id '{a}' in after")))?,
    };
    let req = pb::ListTasksRequest {
        pools: csv(q.pool.as_deref()).map(str::to_string).collect(),
        statuses,
        after,
        limit: q.limit.unwrap_or(0),
    };
    let r = crate::route::list_tasks(app(&st)?, &p, req).await?;
    let mut v = json!({ "tasks": r.tasks.iter().map(info_json).collect::<Vec<_>>() });
    note_missing(&mut v, &r.unavailable_shards);
    if r.next_after > 0 {
        v["next_after"] = r.next_after.to_string().into();
    }
    Ok(Json(v))
}

async fn list_workers(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    let r = crate::route::list_workers(app(&st)?, &p).await?;
    let workers: Vec<Value> = r
        .workers
        .iter()
        .map(|w| {
            json!({
                "id": w.id.to_string(), "name": w.name, "pool": w.pool, "labels": w.labels,
                "slots": w.slots, "busy": w.busy,
                "connected_unix_ns": w.connected_unix_ns.to_string(),
            })
        })
        .collect();
    let mut v = json!({ "workers": workers });
    note_missing(&mut v, &r.unavailable_shards);
    Ok(Json(v))
}

async fn list_recurring(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    let r = crate::route::list_recurring(app(&st)?, &p).await?;
    let schedules: Vec<Value> = r
        .schedules
        .iter()
        .map(|s| {
            let mut v = json!({
                "name": s.name, "cron": s.cron, "pool": s.pool, "kind": s.kind,
                "overlap": overlap_name(s.overlap),
            });
            if !s.tz.is_empty() {
                v["tz"] = s.tz.clone().into();
            }
            if let Some(t) = s.next_unix_ns {
                v["next_unix_ns"] = t.to_string().into();
            }
            if let Some(id) = s.last_task_id {
                v["last_task_id"] = id.to_string().into();
            }
            v
        })
        .collect();
    let mut v = json!({ "schedules": schedules });
    note_missing(&mut v, &r.unavailable_shards);
    Ok(Json(v))
}

/// Name the shards a combined answer is missing, if any.
fn note_missing(v: &mut Value, shards: &[String]) {
    if !shards.is_empty() {
        v["unavailable_shards"] = shards.into();
    }
}

async fn snapshot(
    State(st): State<St>,
    Extension(p): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    let s = crate::route::snapshot(app(&st)?, &p).await?;
    let pools: Vec<Value> = s
        .pools
        .iter()
        .map(|p| {
            json!({
                "name": p.name, "capacity": p.capacity, "busy": p.busy, "held": p.held,
                "desired": p.desired, "pending": p.pending_by_class.iter().sum::<u64>(),
                "pending_by_class": p.pending_by_class, "reservations": p.reservations,
                "dispatched": p.dispatched, "succeeded": p.succeeded, "failed": p.failed,
                "priority_adherence": p.priority_adherence, "fixed_start_hit_rate": p.fixed_start_hit_rate,
                "utilization": p.utilization, "schedule_efficiency": p.schedule_efficiency,
                "wait_p50_s": p.wait_p50_s, "wait_p90_s": p.wait_p90_s, "wait_p99_s": p.wait_p99_s,
                "cost": p.cost, "workers_connected": p.workers_connected,
                "groups": p.groups.iter().map(|g| json!({
                    "labels": g.labels, "capacity": g.capacity, "busy": g.busy,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    let t = s.totals.unwrap_or_default();
    Ok(Json(json!({
        "pools": pools, "live_tasks": s.live_tasks, "running": s.running, "pending": s.pending,
        "workers_connected": s.workers_connected, "unavailable_shards": s.unavailable_shards,
        "totals": {
            "submitted": t.submitted, "dispatched": t.dispatched, "succeeded": t.succeeded,
            "failed": t.failed, "dead_lettered": t.dead_lettered, "cancelled": t.cancelled,
            "skipped": t.skipped, "missed": t.missed, "shed": t.shed, "retries": t.retries,
            "timeouts": t.timeouts, "lease_expiries": t.lease_expiries,
            "stale_completions": t.stale_completions,
        },
    })))
}

async fn metrics(State(st): State<St>) -> Result<Response, ApiError> {
    let s = grpc::snapshot(app(&st)?).await?;
    Ok((
        [(header::CONTENT_TYPE, crate::metrics::CONTENT_TYPE)],
        crate::metrics::render(&s),
    )
        .into_response())
}
