//! Server assembly: listeners, TLS, store, scheduler and leadership.
//!
//! Listeners are bound and served from the start. Until this instance holds leadership (always
//! immediately with the memory store), the API answers `UNAVAILABLE` / `503` and `/readyz`
//! reports `503`; it switches to serving without rebinding once the scheduler is built.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use cronoq::{ExternalScaler, Pool, Scheduler};
use cronoq_proto::v1 as pb;
use rustc_hash::FxHashMap;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::auth::Auth;
use crate::config::{Config, ScalerSection, StoreConfig, TlsConfig};
use crate::convert::unix_ns;
use crate::grpc::{AdminService, TasksService, WorkersService};
use crate::info::TaskInfoCache;
use crate::remote::{OutputCache, RemoteExecutor, WorkerRegistry};
use crate::scaler::WebhookScaler;
use crate::state::{AppState, PoolInfo, Shared};
use crate::store::Opened;

/// How long listeners may take to drain after the server stops serving.
const DRAIN: Duration = Duration::from_secs(5);

/// How long workers get to read their `Shutdown` message before their streams are cut.
const WORKER_GRACE: Duration = Duration::from_secs(1);

/// Listening addresses, reported once this instance serves as the leader.
#[derive(Clone, Debug)]
pub struct Bound {
    /// gRPC listener.
    pub grpc: SocketAddr,
    /// HTTP listener.
    pub http: SocketAddr,
    /// Unauthenticated metrics listener, if configured.
    pub metrics: Option<SocketAddr>,
}

async fn open_store(cfg: &StoreConfig) -> anyhow::Result<Opened> {
    match cfg {
        StoreConfig::Memory => Ok(Opened::memory()),
        StoreConfig::Postgres {
            url: inline,
            url_env,
            schema,
            ..
        } => {
            let url = match (inline, url_env) {
                (Some(u), _) => u.clone(),
                (None, env) => {
                    let var = env.as_deref().unwrap_or("DATABASE_URL");
                    std::env::var(var).map_err(|_| {
                        anyhow::anyhow!("environment variable {var} (store URL) is not set")
                    })?
                }
            };
            crate::pg::warn_insecure_url(&url, inline.is_some());
            crate::pg::open(&url, schema.as_deref()).await
        }
        StoreConfig::Sqlite { path, .. } => {
            let store = cronoq_store_sqlite::SqliteStore::open(path)
                .await
                .map_err(|e| anyhow::anyhow!("sqlite {}: {e}", path.display()))?;
            Ok(Opened::durable(store))
        }
    }
}

fn read(what: &str, path: &Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("reading TLS {what} {}", path.display()))
}

/// TLS material, loaded once at startup.
struct Tls {
    grpc: tonic::transport::ServerTlsConfig,
    http: tokio_rustls::TlsAcceptor,
}

fn load_tls(cfg: &TlsConfig) -> anyhow::Result<Tls> {
    let cert = read("certificate", &cfg.cert)?;
    let key = read("key", &cfg.key)?;
    let ca = cfg
        .client_ca
        .as_ref()
        .map(|p| read("client CA", p))
        .transpose()?;

    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert)
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing TLS certificate {}", cfg.cert.display()))?;
    anyhow::ensure!(
        !chain.is_empty(),
        "no certificate found in {}",
        cfg.cert.display()
    );
    let private = PrivateKeyDer::from_pem_slice(&key)
        .with_context(|| format!("parsing TLS key {}", cfg.key.display()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = match &ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(ca) {
                let c = c.with_context(|| "parsing TLS client CA")?;
                roots.add(c).context("adding TLS client CA")?;
            }
            anyhow::ensure!(
                !roots.is_empty(),
                "no certificate found in the client CA file"
            );
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .context("building the client certificate verifier")?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut http = builder
        .with_single_cert(chain, private)
        .context("TLS certificate and key do not match")?;
    http.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let mut grpc = tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(cert, key));
    if let Some(ca) = ca {
        grpc = grpc.client_ca_root(tonic::transport::Certificate::from_pem(ca));
    }
    Ok(Tls {
        grpc,
        http: tokio_rustls::TlsAcceptor::from(Arc::new(http)),
    })
}

async fn bind(what: &str, addr: SocketAddr) -> anyhow::Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the {what} listener on {addr}"))
}

/// Run until `shutdown` resolves, or until the scheduler stops (e.g. leadership is lost, which
/// returns an error). `on_bound` is called once this instance serves as the leader.
pub async fn run(
    cfg: Config,
    shutdown: impl Future<Output = ()>,
    on_bound: impl FnOnce(Bound),
) -> anyhow::Result<()> {
    cfg.validate()?;
    let auth = Auth::from_config(&cfg.auth)?;
    cfg.check_auth_exposure(auth.enabled())?;
    if !auth.enabled() {
        tracing::warn!(
            "authentication is DISABLED: configure [[auth.tokens]] for any shared deployment"
        );
    }
    let tls = cfg.server.tls.as_ref().map(load_tls).transpose()?;
    let limits = cfg.limits.clone();

    let grpc_listener = bind("gRPC", cfg.server.grpc_addr).await?;
    let http_listener = bind("HTTP", cfg.server.http_addr).await?;
    let metrics_listener = match cfg.server.metrics_addr {
        Some(a) => Some(bind("metrics", a).await?),
        None => None,
    };
    let bound = Bound {
        grpc: grpc_listener.local_addr()?,
        http: http_listener.local_addr()?,
        metrics: metrics_listener
            .as_ref()
            .map(|l| l.local_addr())
            .transpose()?,
    };

    let shared = Arc::new(Shared {
        loopback_hosts_only: !auth.enabled() && !cfg.server.insecure_no_auth,
        auth,
        limits: limits.clone(),
        ready: Arc::new(AtomicBool::new(false)),
        app: OnceLock::new(),
        stop: CancellationToken::new(),
    });

    // Listeners serve from the start; the API reports UNAVAILABLE until leadership.
    let mut servers: Vec<JoinHandle<()>> = Vec::new();
    {
        let max_msg = limits.max_message_bytes;
        let mut grpc = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(limits.keepalive_interval()))
            .http2_keepalive_timeout(Some(limits.keepalive_timeout()))
            .tcp_keepalive(Some(limits.keepalive_interval()))
            .max_concurrent_streams(limits.max_concurrent_streams)
            .timeout(limits.max_wait() + limits.request_timeout());
        if let Some(t) = &tls {
            grpc = grpc
                .tls_config(t.grpc.clone())
                .context("configuring gRPC TLS")?;
        }
        let semaphore = Arc::new(tokio::sync::Semaphore::new(limits.max_concurrent_requests));
        let router = grpc
            .layer(tower::limit::GlobalConcurrencyLimitLayer::with_semaphore(
                semaphore,
            ))
            .add_service(
                pb::tasks_server::TasksServer::new(TasksService {
                    shared: shared.clone(),
                })
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
            )
            .add_service(
                pb::workers_server::WorkersServer::new(WorkersService {
                    shared: shared.clone(),
                })
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
            )
            .add_service(
                pb::admin_server::AdminServer::new(AdminService {
                    shared: shared.clone(),
                })
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
            );
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);
        let stop = shared.stop.clone();
        servers.push(tokio::spawn(async move {
            if let Err(e) = router
                .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
                .await
            {
                tracing::error!(error = %e, "gRPC server failed");
            }
        }));
    }
    servers.push(tokio::spawn(crate::http::serve(
        http_listener,
        crate::http::router(shared.clone()),
        tls.as_ref().map(|t| t.http.clone()),
        shared.clone(),
        shared.stop.clone(),
        DRAIN,
    )));
    if let Some(l) = metrics_listener {
        servers.push(tokio::spawn(crate::http::serve(
            l,
            crate::http::metrics_router(shared.clone()),
            None,
            shared.clone(),
            shared.stop.clone(),
            DRAIN,
        )));
    }

    let result = lead(&cfg, &shared, shutdown, bound, on_bound).await;
    shared.ready.store(false, Ordering::SeqCst);
    shared.stop.cancel();
    for mut s in servers {
        if tokio::time::timeout(DRAIN + Duration::from_secs(1), &mut s)
            .await
            .is_err()
        {
            tracing::warn!("a listener did not stop in time; aborting it");
            s.abort();
        }
    }
    result
}

/// Acquire leadership, serve, and stop. Returns when `shutdown` resolves or leadership is lost.
async fn lead(
    cfg: &Config,
    shared: &Arc<Shared>,
    shutdown: impl Future<Output = ()>,
    bound: Bound,
    on_bound: impl FnOnce(Bound),
) -> anyhow::Result<()> {
    let limits = &cfg.limits;
    tokio::pin!(shutdown);
    let Opened { store, queries } = tokio::select! {
        s = open_store(&cfg.store) => s?,
        _ = &mut shutdown => return Ok(()),
    };
    let registry = WorkerRegistry::new();
    let outputs = OutputCache::new(cfg.outputs.cache_entries, cfg.outputs.cache_bytes);
    let info = TaskInfoCache::new(cfg.outputs.info_entries);
    let mut builder = Scheduler::<Bytes, Bytes>::builder()
        .store_arc(store.clone())
        // Outputs are cached by the server (bounded by count and bytes) and the store; the
        // runtime's count-only cache would hold up to 1024 maximum-size outputs.
        .output_cache(0)
        .emit_transitions(true)
        .configure(|c| c.global_bounds.max_pending = Some(limits.max_pending_total));
    let mut pools = FxHashMap::default();
    for p in &cfg.pools {
        let pc = p.to_pool_config(limits);
        let name: Arc<str> = pc.name.clone();
        let lease_ttl = pc.lease_ttl.unwrap_or(Duration::from_secs(30));
        pools.insert(
            name.clone(),
            PoolInfo {
                slot_cap: p.slot_cap(limits),
                lease_ttl,
                max_attempts: pc.retry.max_attempts,
            },
        );
        let mut pool = Pool::new(name.clone())
            .configure(|c| *c = pc.clone())
            .executor(RemoteExecutor::new(
                name.clone(),
                lease_ttl,
                registry.clone(),
                outputs.clone(),
            ));
        pool = match &p.scaler {
            ScalerSection::External => pool.scaler(ExternalScaler),
            ScalerSection::Webhook { url, token_env } => {
                let token = match token_env {
                    Some(env) => Some(std::env::var(env).map_err(|_| {
                        anyhow::anyhow!(
                            "pool '{name}': environment variable {env} (webhook token) is not set"
                        )
                    })?),
                    None => None,
                };
                pool.scaler(WebhookScaler::new(url.clone(), token)?)
            }
        };
        builder = builder.pool(pool);
    }
    for r in &cfg.resources {
        builder = builder.resource(r.to_config());
    }
    if queries.is_some() {
        tracing::info!("waiting for leadership");
    }
    let sched = tokio::select! {
        s = builder.build() => s.context("starting the scheduler")?,
        _ = &mut shutdown => {
            tracing::info!("shutdown requested while standing by");
            return Ok(());
        }
    };
    registry.attach(sched.clone());
    let pruner = match (&queries, &cfg.store) {
        (
            Some(q),
            StoreConfig::Postgres {
                keep_finished_s, ..
            }
            | StoreConfig::Sqlite {
                keep_finished_s, ..
            },
        ) => Some(crate::store::spawn_pruner(
            q.clone(),
            Duration::from_secs_f64(keep_finished_s.unwrap_or(7.0 * 86400.0)),
            Duration::from_secs(3600),
        )),
        _ => None,
    };
    let feeder = spawn_info_feeder(&sched, info.clone(), shared.stop.clone());
    let app = Arc::new(AppState {
        sched: sched.clone(),
        registry: registry.clone(),
        outputs,
        info,
        pools,
        limits: limits.clone(),
        queries: queries.clone(),
    });
    let _ = shared.app.set(app);
    shared.ready.store(true, Ordering::SeqCst);
    tracing::info!(grpc = %bound.grpc, http = %bound.http, "leader: serving");
    on_bound(bound);

    let lost_leadership = tokio::select! {
        _ = &mut shutdown => false,
        _ = sched.stopped() => true,
        _ = crate::store::leadership_lost(queries.clone()) => true,
    };
    shared.ready.store(false, Ordering::SeqCst);
    if let Some(p) = pruner {
        p.abort();
    }
    let result = if lost_leadership {
        tracing::error!("leadership lost or scheduler stopped: no longer serving");
        registry.close_all("this server is no longer the leader", true, WORKER_GRACE);
        let _ = sched.shutdown(Duration::ZERO).await;
        Err(anyhow::anyhow!("lost leadership or the scheduler stopped"))
    } else {
        let grace = Duration::from_secs_f64(cfg.server.shutdown_grace_s);
        // Workers keep reporting outcomes while running attempts finish.
        let r = sched.shutdown(grace).await;
        registry.close_all("server shutting down", false, WORKER_GRACE);
        // Hand over at once instead of letting a standby wait for the lease to run out.
        store.release_leadership().await;
        r.context("scheduler shutdown")
    };
    // Ends Watch streams and pending waits before the listeners drain.
    shared.stop.cancel();
    feeder.abort();
    result
}

/// Keep the task-info cache up to date from the scheduler's status events.
fn spawn_info_feeder(
    sched: &Scheduler<Bytes, Bytes>,
    info: Arc<TaskInfoCache>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let mut events = sched.events();
    let names: Arc<[Arc<str>]> = sched.pools().into();
    let clock = *sched.clock();
    tokio::spawn(async move {
        loop {
            let e = tokio::select! {
                e = events.recv() => e,
                _ = stop.cancelled() => return,
            };
            match e {
                Ok(e) => {
                    let Some(pool) = names.get(e.pool.0 as usize) else {
                        continue;
                    };
                    info.on_event(&e, pool, unix_ns(clock.to_system(e.at)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::debug!(dropped = n, "task info cache missed events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}
