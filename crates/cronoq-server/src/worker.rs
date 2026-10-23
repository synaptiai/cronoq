//! Rust client for remote workers.
//!
//! [`run_worker`] connects to a cronoq server, registers for a pool and runs a handler for each
//! lease, with at most `slots` handlers at a time. It heartbeats running leases, reports
//! outcomes, reconnects with backoff when the connection drops or the server steps down, and
//! drains running handlers when `shutdown` resolves.
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use cronoq_server::worker::{WorkerOptions, run_worker};
//! let opts = WorkerOptions::new("http://127.0.0.1:7070", "default").slots(8);
//! run_worker(opts, |lease| async move { Ok(lease.payload) }, std::future::pending()).await
//! # }
//! ```
//!
//! A handler that panics fails its attempt as retryable; an output larger than the server
//! accepts fails it permanently.
//!
//! When the pool belongs to another shard, the server refuses the worker and names that
//! shard's endpoint; the worker then connects there instead (see [`MAX_REDIRECTS`]). It never
//! follows a redirect from `https://` to `http://`, and never sends its token over plain http
//! to another host unless [`WorkerOptions::insecure_redirect_http`] allows it.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::TaskError;
use cronoq_proto::v1 as pb;
use futures_util::FutureExt;
use pb::worker_message::Msg;
use pb::workers_client::WorkersClient;
use rustc_hash::FxHashMap;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

/// TLS settings for the worker connection.
#[derive(Clone, Default)]
pub struct WorkerTls {
    /// PEM CA certificate(s) to trust (default: the system roots).
    pub ca_pem: Option<Vec<u8>>,
    /// PEM client certificate and key, for mTLS.
    pub identity: Option<(Vec<u8>, Vec<u8>)>,
    /// Server name to verify (default: the endpoint host).
    pub domain: Option<String>,
}

impl fmt::Debug for WorkerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerTls")
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<pem>"))
            .field("identity", &self.identity.as_ref().map(|_| "<redacted>"))
            .field("domain", &self.domain)
            .finish()
    }
}

/// Worker settings.
#[derive(Clone)]
pub struct WorkerOptions {
    /// Server gRPC endpoint (`http://` or `https://`).
    pub endpoint: String,
    /// Pool to serve.
    pub pool: String,
    /// Leases run concurrently (the server may grant fewer).
    pub slots: u32,
    /// Bearer token (role `worker`).
    pub token: Option<String>,
    /// Name shown in server logs.
    pub name: String,
    /// TLS settings; they need an `https://` endpoint (an `https://` endpoint without them uses
    /// the system roots).
    pub tls: Option<WorkerTls>,
    /// Reconnect after the connection drops or the server steps down (default `true`).
    pub reconnect: bool,
    /// First reconnect delay (doubles up to `max_backoff`).
    pub initial_backoff: Duration,
    /// Longest reconnect delay.
    pub max_backoff: Duration,
    /// How long running handlers may take to finish after `shutdown` resolves.
    pub drain_timeout: Duration,
    /// Largest lease message accepted from the server.
    pub max_message_bytes: usize,
    /// Labels of this worker: tasks that require labels are leased only to workers that have
    /// all of them.
    pub labels: Vec<(String, String)>,
    /// Follow redirects to plain `http://` endpoints on other hosts with the token (default
    /// `false`: only loopback); for trusted networks without TLS.
    pub insecure_redirect_http: bool,
}

impl fmt::Debug for WorkerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerOptions")
            .field("endpoint", &self.endpoint)
            .field("pool", &self.pool)
            .field("slots", &self.slots)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("name", &self.name)
            .field("tls", &self.tls)
            .field("reconnect", &self.reconnect)
            .field("drain_timeout", &self.drain_timeout)
            .field("labels", &self.labels)
            .field("insecure_redirect_http", &self.insecure_redirect_http)
            .finish_non_exhaustive()
    }
}

impl WorkerOptions {
    /// Options for serving `pool` on the server at `endpoint`, with one slot.
    pub fn new(endpoint: impl Into<String>, pool: impl Into<String>) -> Self {
        WorkerOptions {
            endpoint: endpoint.into(),
            pool: pool.into(),
            slots: 1,
            token: None,
            name: String::new(),
            tls: None,
            reconnect: true,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(10),
            drain_timeout: Duration::from_secs(30),
            max_message_bytes: 64 << 20,
            labels: Vec::new(),
            insecure_redirect_http: false,
        }
    }
    /// Allow redirects to plain `http://` endpoints on other hosts with the token.
    pub fn insecure_redirect_http(mut self, on: bool) -> Self {
        self.insecure_redirect_http = on;
        self
    }
    /// Add a label (`name = value`) to this worker.
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }
    /// Concurrent leases.
    pub fn slots(mut self, n: u32) -> Self {
        self.slots = n;
        self
    }
    /// Bearer token.
    pub fn token(mut self, t: impl Into<String>) -> Self {
        self.token = Some(t.into());
        self
    }
    /// Name shown in server logs.
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        self
    }
    /// TLS settings.
    pub fn tls(mut self, tls: WorkerTls) -> Self {
        self.tls = Some(tls);
        self
    }
    /// Whether to reconnect after the connection drops.
    pub fn reconnect(mut self, on: bool) -> Self {
        self.reconnect = on;
        self
    }
    /// Time running handlers get to finish on shutdown.
    pub fn drain_timeout(mut self, d: Duration) -> Self {
        self.drain_timeout = d;
        self
    }
}

/// One leased attempt.
#[derive(Clone, Debug)]
pub struct LeaseInfo {
    /// Lease id (unique per attempt).
    pub lease: u64,
    /// Task id.
    pub task_id: u64,
    /// Attempt number (1-based).
    pub attempt: u32,
    /// Pool the task runs in.
    pub pool: String,
    /// Task payload.
    pub payload: Bytes,
    /// Payload content type (may be empty).
    pub content_type: String,
    /// Task kind.
    pub kind: String,
    /// Task key (may be empty).
    pub key: String,
    /// Task tags.
    pub tags: HashMap<String, String>,
    /// Attempt timeout, if any.
    pub timeout: Option<Duration>,
    /// Task deadline, if any.
    pub deadline: Option<SystemTime>,
    /// Cancelled when the server aborts this attempt or the worker stops.
    pub cancel: CancellationToken,
}

fn duration(s: Option<f64>) -> Option<Duration> {
    s.and_then(|s| Duration::try_from_secs_f64(s).ok())
}

/// Redirects to other shards followed in a row before the worker gives up.
pub const MAX_REDIRECTS: u32 = 3;

/// Metadata naming the endpoint of the shard that serves a refused pool.
const ENDPOINT_HEADER: &str = "cronoq-shard-endpoint";

/// The endpoint a refusal sends the worker to, if it is a shard redirect: `Ok(None)` when it is
/// not one, an error when the endpoint may not be followed from `current`.
fn redirect(
    opts: &WorkerOptions,
    current: &str,
    s: &tonic::Status,
) -> anyhow::Result<Option<String>> {
    if s.code() != Code::FailedPrecondition {
        return Ok(None);
    }
    let Some(target) = s
        .metadata()
        .get(ENDPOINT_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(None);
    };
    let https = target.starts_with("https://");
    anyhow::ensure!(
        https || target.starts_with("http://"),
        "the server redirected the worker to '{target}', which is not an http(s) endpoint"
    );
    anyhow::ensure!(
        https || !current.starts_with("https://"),
        "the server redirected the worker from https to plain http ('{target}'); refusing"
    );
    let uri = target.parse::<tonic::transport::Uri>()?;
    anyhow::ensure!(
        uri.authority().is_some_and(|a| !a.as_str().contains('@')),
        "the server redirected the worker to '{target}', which is not a plain host and port"
    );
    let plain_remote = target.starts_with("http://") && !is_loopback_endpoint(target);
    if opts.token.is_some() && plain_remote {
        anyhow::ensure!(
            opts.insecure_redirect_http,
            "the server redirected the worker to '{target}', which would send its token over \
             plain http to another host; refusing (insecure_redirect_http allows it)"
        );
        tracing::warn!(endpoint = %target, "following a redirect that sends the worker token over plain http");
    }
    Ok(Some(target.to_string()))
}

/// Errors that retrying cannot fix.
fn fatal(code: Code) -> bool {
    matches!(
        code,
        Code::Unauthenticated
            | Code::PermissionDenied
            | Code::NotFound
            | Code::InvalidArgument
            | Code::FailedPrecondition
            | Code::Unimplemented
    )
}

/// Running handlers, across connections.
#[derive(Default)]
struct Running {
    leases: Mutex<FxHashMap<u64, Lease>>,
    count: AtomicUsize,
    idle: Notify,
}

struct Lease {
    cancel: CancellationToken,
    /// Set when the server aborted the attempt: no outcome is reported.
    aborted: Arc<std::sync::atomic::AtomicBool>,
}

/// Decrements the running count when a handler task ends, however it ends.
struct Slot {
    running: Arc<Running>,
    lease: u64,
}

impl Running {
    /// Free a lease's slot (once): when its handler ends, or as soon as the server aborts it,
    /// so the server may reuse the slot right away.
    fn release(&self, lease: u64) -> Option<Lease> {
        let l = self.leases.lock().unwrap().remove(&lease)?;
        if self.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.idle.notify_waiters();
        }
        Some(l)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.running.release(self.lease);
    }
}

/// Aborts a spawned task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum SessionEnd {
    /// The caller asked to stop.
    Shutdown,
    /// The server closed the stream or asked us to go away.
    Ended(String),
    /// A gRPC error.
    Failed(tonic::Status),
}

/// Connect, register and process leases until `shutdown` resolves (then drain running
/// handlers), reconnecting with backoff when the connection is lost. Returns an error for
/// failures that retrying cannot fix (bad credentials, unknown pool, ...), or for any failure
/// when reconnecting is disabled.
pub async fn run_worker<F, Fut>(
    opts: WorkerOptions,
    handler: F,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()>
where
    F: Fn(LeaseInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, TaskError>> + Send + 'static,
{
    endpoint(&opts, &opts.endpoint)?;
    let auth: Option<MetadataValue<_>> = match &opts.token {
        Some(t) => {
            let mut v: MetadataValue<_> = format!("Bearer {t}").parse()?;
            v.set_sensitive(true);
            Some(v)
        }
        None => None,
    };
    let handler = Arc::new(handler);
    let running = Arc::new(Running::default());
    let stop = CancellationToken::new();
    // Handlers are cancelled if this future is dropped, too.
    let _stop_on_drop = stop.clone().drop_guard();
    tokio::pin!(shutdown);
    let mut backoff = opts.initial_backoff;
    // The endpoint connected to: the configured one, or the shard it redirected the worker to.
    let mut current = opts.endpoint.clone();
    let mut redirects = 0;
    let result = loop {
        let mut connect_failed = false;
        let session = async {
            let channel = endpoint(&opts, &current)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
                .connect()
                .await
                .map_err(|e| {
                    connect_failed = true;
                    tonic::Status::unavailable(format!("connect: {e}"))
                })?;
            let auth = auth.clone();
            let client =
                WorkersClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                    if let Some(a) = &auth {
                        req.metadata_mut().insert("authorization", a.clone());
                    }
                    Ok(req)
                })
                .max_decoding_message_size(opts.max_message_bytes);
            Ok::<_, tonic::Status>(client)
        };
        let end = tokio::select! {
            c = session => match c {
                Ok(client) => serve(&opts, client, handler.clone(), running.clone(), &stop, &mut shutdown, &mut backoff).await,
                Err(s) => SessionEnd::Failed(s),
            },
            _ = &mut shutdown => SessionEnd::Shutdown,
        };
        if let SessionEnd::Failed(s) = &end {
            match redirect(&opts, &current, s) {
                Ok(Some(target)) => {
                    redirects += 1;
                    if redirects > MAX_REDIRECTS {
                        break Err(anyhow::anyhow!(
                            "worker redirected {redirects} times in a row, more than the {MAX_REDIRECTS} it follows: {s}"
                        ));
                    }
                    tracing::info!(from = %current, to = %target, "pool is served by another shard; reconnecting there");
                    current = target;
                    continue;
                }
                Ok(None) => {}
                Err(e) => break Err(e),
            }
        }
        redirects = 0;
        if connect_failed && current != opts.endpoint {
            // The shard is unreachable: ask the configured endpoint again where the pool is.
            tracing::warn!(endpoint = %current, "shard unreachable; returning to the configured endpoint");
            current = opts.endpoint.clone();
        }
        match end {
            SessionEnd::Shutdown => break Ok(()),
            SessionEnd::Failed(s) if fatal(s.code()) => {
                break Err(anyhow::anyhow!("worker rejected: {s}"));
            }
            SessionEnd::Failed(s) if !opts.reconnect => {
                break Err(anyhow::anyhow!("worker stream failed: {s}"));
            }
            SessionEnd::Ended(why) if !opts.reconnect => {
                break Err(anyhow::anyhow!("worker stream ended: {why}"));
            }
            SessionEnd::Failed(s) => {
                tracing::warn!(error = %s, retry_in = ?backoff, "worker connection failed")
            }
            SessionEnd::Ended(why) => {
                tracing::info!(reason = %why, retry_in = ?backoff, "worker stream ended; reconnecting")
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = &mut shutdown => break Ok(()),
        }
        backoff = (backoff * 2).min(opts.max_backoff);
    };
    // Whatever is still running after the drain (or after a fatal error) is cancelled.
    stop.cancel();
    for (_, l) in running.leases.lock().unwrap().drain() {
        l.cancel.cancel();
    }
    result
}

/// The endpoint at `url`: the configured one, or a shard the server redirected the worker to
/// (which keeps the TLS settings except a fixed server name).
fn endpoint(opts: &WorkerOptions, url: &str) -> anyhow::Result<Endpoint> {
    let mut ep = Endpoint::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(20)))
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    let https = url.starts_with("https://");
    // tonic only applies TLS to https:// endpoints; anything else would silently be plaintext.
    anyhow::ensure!(
        https || opts.tls.is_none(),
        "worker TLS settings need an https:// endpoint, not '{url}'"
    );
    if !https && opts.token.is_some() && !is_loopback_endpoint(url) {
        tracing::warn!(endpoint = %url, "the worker token is sent over plain http; use https outside a trusted network");
    }
    if https {
        let tls = opts.tls.clone().unwrap_or_default();
        let mut cfg = ClientTlsConfig::new();
        cfg = match &tls.ca_pem {
            Some(ca) => cfg.ca_certificate(Certificate::from_pem(ca)),
            None => cfg.with_native_roots(),
        };
        if let Some((cert, key)) = &tls.identity {
            cfg = cfg.identity(Identity::from_pem(cert, key));
        }
        if let Some(d) = &tls.domain
            && url == opts.endpoint
        {
            cfg = cfg.domain_name(d.clone());
        }
        ep = ep.tls_config(cfg)?;
    }
    Ok(ep)
}

fn is_loopback_endpoint(endpoint: &str) -> bool {
    // Parsed as tonic connects, so userinfo (`http://127.0.0.1:1@host`) cannot hide the host.
    let Ok(uri) = endpoint.parse::<tonic::transport::Uri>() else {
        return false;
    };
    let host = uri.host().unwrap_or("");
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[allow(clippy::too_many_arguments)]
async fn serve<F, Fut, I>(
    opts: &WorkerOptions,
    mut client: WorkersClient<
        tonic::service::interceptor::InterceptedService<tonic::transport::Channel, I>,
    >,
    handler: Arc<F>,
    running: Arc<Running>,
    stop: &CancellationToken,
    shutdown: &mut std::pin::Pin<&mut impl Future<Output = ()>>,
    backoff: &mut Duration,
) -> SessionEnd
where
    F: Fn(LeaseInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, TaskError>> + Send + 'static,
    I: tonic::service::Interceptor,
{
    let cap = (opts.slots as usize).saturating_mul(4).clamp(64, 65_536);
    let (tx, rx) = mpsc::channel::<pb::WorkerMessage>(cap);
    let register = pb::WorkerMessage {
        msg: Some(Msg::Register(pb::Register {
            pool: opts.pool.clone(),
            slots: opts.slots,
            worker_name: opts.name.clone(),
            protocol_version: cronoq_proto::PROTOCOL_VERSION,
            labels: opts.labels.iter().cloned().collect(),
        })),
    };
    if tx.send(register).await.is_err() {
        return SessionEnd::Ended("local channel closed".into());
    }
    let mut inbound = match client.work(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(s) => return SessionEnd::Failed(s),
    };
    let registered = match inbound.message().await {
        Ok(Some(pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Registered(r)),
        })) => r,
        Ok(Some(pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Shutdown(s)),
        })) => {
            return SessionEnd::Ended(format!("server shutting down: {}", s.reason));
        }
        Ok(other) => {
            return SessionEnd::Failed(tonic::Status::internal(format!(
                "unexpected first server message: {other:?}"
            )));
        }
        Err(s) => return SessionEnd::Failed(s),
    };
    *backoff = opts.initial_backoff;
    let ttl = duration(registered.lease_ttl_s);
    let slots = registered.slots as usize;
    // Leave room for the message envelope.
    let max_output = (registered.max_message_bytes as usize).saturating_sub(1024);
    tracing::info!(worker = registered.worker_id, pool = %opts.pool, slots, "worker registered");
    let mut draining: Option<tokio::time::Instant> = None;
    loop {
        let drain_done = async {
            match draining {
                Some(deadline) => loop {
                    let idle = running.idle.notified();
                    if running.count.load(Ordering::SeqCst) == 0 {
                        return;
                    }
                    tokio::select! {
                        _ = idle => {}
                        _ = tokio::time::sleep_until(deadline) => return,
                    }
                },
                None => std::future::pending().await,
            }
        };
        let msg = tokio::select! {
            m = inbound.message() => m,
            _ = shutdown.as_mut(), if draining.is_none() => {
                tracing::info!(running = running.count.load(Ordering::SeqCst), "worker draining");
                draining = Some(tokio::time::Instant::now() + opts.drain_timeout);
                continue;
            }
            _ = drain_done => {
                // Handlers still running after the drain timeout report a retryable failure.
                for l in running.leases.lock().unwrap().values() {
                    l.cancel.cancel();
                }
                // Half-close: once every outcome is sent, the server ends the stream.
                drop(tx);
                let flush = async { while let Ok(Some(_)) = inbound.message().await {} };
                let _ = tokio::time::timeout(Duration::from_secs(5), flush).await;
                return SessionEnd::Shutdown;
            }
        };
        let msg = match msg {
            Ok(Some(m)) => m.msg,
            Ok(None) => {
                return if draining.is_some() {
                    SessionEnd::Shutdown
                } else {
                    SessionEnd::Ended("server closed the stream".into())
                };
            }
            Err(s) => {
                return if draining.is_some() {
                    SessionEnd::Shutdown
                } else {
                    SessionEnd::Failed(s)
                };
            }
        };
        match msg {
            Some(pb::server_message::Msg::Lease(l)) => {
                let busy = running.count.load(Ordering::SeqCst);
                if draining.is_some() || busy >= slots {
                    let reason = if draining.is_some() {
                        "worker is draining"
                    } else {
                        "no free slot"
                    };
                    let _ = tx.try_send(pb::WorkerMessage {
                        msg: Some(Msg::Nack(pb::Nack {
                            lease: l.lease,
                            reason: reason.into(),
                        })),
                    });
                    continue;
                }
                start_lease(
                    l,
                    handler.clone(),
                    running.clone(),
                    tx.clone(),
                    ttl,
                    max_output,
                    stop,
                );
            }
            Some(pb::server_message::Msg::Abort(a)) => {
                if let Some(l) = running.release(a.lease) {
                    l.aborted.store(true, Ordering::SeqCst);
                    l.cancel.cancel();
                }
            }
            Some(pb::server_message::Msg::Shutdown(s)) => {
                if draining.is_some() {
                    return SessionEnd::Shutdown;
                }
                let why = if s.not_leader {
                    format!("server is no longer the leader: {}", s.reason)
                } else {
                    s.reason
                };
                return SessionEnd::Ended(why);
            }
            Some(pb::server_message::Msg::Registered(_)) | None => {}
        }
    }
}

fn start_lease<F, Fut>(
    l: pb::Lease,
    handler: Arc<F>,
    running: Arc<Running>,
    tx: mpsc::Sender<pb::WorkerMessage>,
    ttl: Option<Duration>,
    max_output: usize,
    stop: &CancellationToken,
) where
    F: Fn(LeaseInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, TaskError>> + Send + 'static,
{
    let lease = l.lease;
    let cancel = stop.child_token();
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    running.count.fetch_add(1, Ordering::SeqCst);
    running.leases.lock().unwrap().insert(
        lease,
        Lease {
            cancel: cancel.clone(),
            aborted: aborted.clone(),
        },
    );
    let slot = Slot { running, lease };
    let info = LeaseInfo {
        lease,
        task_id: l.task_id,
        attempt: l.attempt,
        pool: l.pool,
        payload: Bytes::from(l.payload),
        content_type: l.content_type,
        kind: l.kind,
        key: l.key,
        tags: l.tags,
        timeout: duration(l.timeout_s),
        deadline: l
            .deadline_unix_ns
            .filter(|ns| *ns >= 0)
            .map(|ns| UNIX_EPOCH + Duration::from_nanos(ns as u64)),
        cancel: cancel.clone(),
    };
    let ttl = duration(l.lease_ttl_s).or(ttl);
    tokio::spawn(async move {
        let _slot = slot;
        let _beat = ttl.map(|t| {
            let tx = tx.clone();
            AbortOnDrop(tokio::spawn(async move {
                let mut iv = tokio::time::interval((t / 3).max(Duration::from_millis(10)));
                iv.tick().await;
                loop {
                    iv.tick().await;
                    if tx
                        .send(pb::WorkerMessage {
                            msg: Some(Msg::Heartbeat(pb::Heartbeat { lease })),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }))
        });
        let run = AssertUnwindSafe(handler(info)).catch_unwind();
        let result = tokio::select! {
            r = run => Some(r),
            _ = cancel.cancelled() => None,
        };
        let msg = match result {
            None if aborted.load(Ordering::SeqCst) => return,
            None => Msg::Fail(pb::Fail {
                lease,
                message: "worker stopped before the attempt finished".into(),
                permanent: false,
                retry_after_s: None,
            }),
            Some(Err(panic)) => {
                let what = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".into());
                tracing::error!(lease, panic = %what, "handler panicked");
                Msg::Fail(pb::Fail {
                    lease,
                    message: format!("handler panicked: {what}"),
                    permanent: false,
                    retry_after_s: None,
                })
            }
            Some(Ok(Ok(output))) if max_output > 0 && output.len() > max_output => {
                Msg::Fail(pb::Fail {
                    lease,
                    message: format!(
                        "output of {} bytes exceeds the server limit of {max_output} bytes",
                        output.len()
                    ),
                    permanent: true,
                    retry_after_s: None,
                })
            }
            Some(Ok(Ok(output))) => Msg::Complete(pb::Complete {
                lease,
                output: output.to_vec(),
                content_type: String::new(),
            }),
            Some(Ok(Err(TaskError::Permanent { message }))) => Msg::Fail(pb::Fail {
                lease,
                message: message.to_string(),
                permanent: true,
                retry_after_s: None,
            }),
            Some(Ok(Err(TaskError::Retryable {
                message,
                retry_after,
            }))) => Msg::Fail(pb::Fail {
                lease,
                message: message.to_string(),
                permanent: false,
                retry_after_s: retry_after.map(|d| d.as_secs_f64()),
            }),
            Some(Ok(Err(TaskError::Cancelled))) => Msg::Cancelled(pb::Cancelled { lease }),
        };
        let _ = tx.send(pb::WorkerMessage { msg: Some(msg) }).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(code: Code, endpoint: Option<&'static str>) -> tonic::Status {
        let mut s = tonic::Status::new(code, "pool 'llm' is served by shard 'gpu'");
        if let Some(e) = endpoint {
            s.metadata_mut()
                .insert(ENDPOINT_HEADER, MetadataValue::from_static(e));
        }
        s
    }

    #[test]
    fn redirects_never_weaken_the_connection() {
        let opts = WorkerOptions::new("https://cpu.internal:7070", "llm").token("t");
        let follow = |current: &str, s| redirect(&opts, current, &s);
        let fp = Code::FailedPrecondition;
        assert_eq!(
            follow(
                current(&opts),
                refusal(fp, Some("https://gpu.internal:7070"))
            )
            .unwrap(),
            Some("https://gpu.internal:7070".into())
        );
        // Not a redirect: another code, or no endpoint.
        assert_eq!(
            follow(current(&opts), refusal(Code::NotFound, Some("https://x:1"))).unwrap(),
            None
        );
        assert_eq!(follow(current(&opts), refusal(fp, None)).unwrap(), None);
        // No downgrade from https, not even to loopback.
        assert!(follow(current(&opts), refusal(fp, Some("http://127.0.0.1:7070"))).is_err());
        assert!(follow(current(&opts), refusal(fp, Some("unix:/tmp/s"))).is_err());
        assert!(
            follow(
                current(&opts),
                refusal(fp, Some("https://u:p@gpu.internal:7070"))
            )
            .is_err()
        );

        // From plain http on loopback, a token never goes over plain http to another host.
        let local = WorkerOptions::new("http://127.0.0.1:7070", "llm").token("t");
        let r = |t| redirect(&local, "http://127.0.0.1:7070", &refusal(fp, Some(t)));
        assert!(r("http://127.0.0.1:7071").unwrap().is_some());
        assert!(r("https://gpu.internal:7070").unwrap().is_some());
        assert!(r("http://gpu.internal:7070").is_err());
        // Userinfo does not make another host look like loopback.
        assert!(r("http://127.0.0.1:1@gpu.internal:7070").is_err());
        assert!(!is_loopback_endpoint(
            "http://127.0.0.1:1@gpu.internal:7070"
        ));
        assert!(is_loopback_endpoint("http://[::1]:7070"));
        // Without a token there is nothing to leak.
        let anon = WorkerOptions::new("http://127.0.0.1:7070", "llm");
        let to_remote = refusal(fp, Some("http://gpu.internal:7070"));
        assert!(
            redirect(&anon, "http://127.0.0.1:7070", &to_remote)
                .unwrap()
                .is_some()
        );
        // A plain-http endpoint on another host does not by itself allow more of them.
        let remote = WorkerOptions::new("http://cpu.internal:7070", "llm").token("t");
        assert!(redirect(&remote, "http://cpu.internal:7070", &to_remote).is_err());
        // Only the opt-in does.
        let trusted = remote.insecure_redirect_http(true);
        assert!(
            redirect(&trusted, "http://cpu.internal:7070", &to_remote)
                .unwrap()
                .is_some()
        );
    }

    fn current(opts: &WorkerOptions) -> &str {
        &opts.endpoint
    }
}
