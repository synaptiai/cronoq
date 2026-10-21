//! Server assembly: store, scheduler, gRPC and HTTP listeners, leadership.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cronoq::{ExternalScaler, MemoryStore, Pool, Scheduler, Store};
use cronoq_proto::v1 as pb;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::auth::Auth;
use crate::config::{Config, ScalerSection, StoreConfig};
use crate::grpc::{AdminService, TasksService, WorkersService};
use crate::remote::{OutputCache, RemoteExecutor, WorkerRegistry};
use crate::scaler::WebhookScaler;
use crate::state::AppState;

/// Bound addresses, reported once the server is serving.
#[derive(Clone, Debug)]
pub struct Bound {
    pub grpc: std::net::SocketAddr,
    pub http: std::net::SocketAddr,
}

type OpenedStore = (
    Arc<dyn Store<Bytes, Bytes>>,
    Option<cronoq_store_postgres::PgStore>,
);

async fn open_store(cfg: &StoreConfig) -> anyhow::Result<OpenedStore> {
    match cfg {
        StoreConfig::Memory => Ok((Arc::new(MemoryStore), None)),
        StoreConfig::Postgres { url, url_env, .. } => {
            let url = match (url, url_env) {
                (Some(u), _) => u.clone(),
                (None, env) => {
                    let var = env.as_deref().unwrap_or("DATABASE_URL");
                    std::env::var(var).map_err(|_| anyhow::anyhow!("{var} is not set"))?
                }
            };
            let (store, pg) = crate::pg::open(&url).await?;
            Ok((store, Some(pg)))
        }
    }
}

/// Run until `shutdown` resolves or the scheduler stops (e.g. leadership lost).
/// `on_bound` receives the listening addresses (useful with port 0).
pub async fn run(
    cfg: Config,
    shutdown: impl Future<Output = ()>,
    on_bound: impl FnOnce(Bound),
) -> anyhow::Result<()> {
    let auth = Auth::from_config(&cfg.auth)?;
    if !auth.enabled() {
        tracing::warn!(
            "authentication is DISABLED: configure [[auth.tokens]] for any non-local deployment"
        );
    }
    let ready = Arc::new(AtomicBool::new(false));
    let http_listener = TcpListener::bind(cfg.server.http_addr).await?;
    let grpc_listener = TcpListener::bind(cfg.server.grpc_addr).await?;
    let bound = Bound {
        grpc: grpc_listener.local_addr()?,
        http: http_listener.local_addr()?,
    };
    let (stop_tx, stop_rx) = watch::channel(false);

    // Phase 1: health endpoints only, while waiting for leadership.
    let (standby_stop_tx, mut standby_stop_rx) = watch::channel(false);
    let standby = {
        let router = crate::http::health_router(ready.clone());
        tokio::spawn(async move {
            axum::serve(http_listener, router)
                .with_graceful_shutdown(async move {
                    let _ = standby_stop_rx.changed().await;
                })
                .await
                .map(|_| ())
        })
    };

    let (store, pg) = open_store(&cfg.store).await?;
    let registry = WorkerRegistry::new();
    let outputs = OutputCache::new(cfg.outputs.cache_entries, cfg.outputs.cache_bytes);
    let mut builder = Scheduler::<Bytes, Bytes>::builder().store_arc(store);
    let mut remote_pools = FxHashSet::default();
    let mut lease_ttl = FxHashMap::default();
    for p in &cfg.pools {
        let pc = p.to_pool_config();
        let name: Arc<str> = pc.name.clone();
        let mut pool = Pool::new(name.clone())
            .configure(|c| *c = pc.clone())
            .executor(RemoteExecutor::new(
                name.clone(),
                registry.clone(),
                outputs.clone(),
            ));
        pool = match &p.scaler {
            ScalerSection::External => pool.scaler(ExternalScaler),
            ScalerSection::Webhook { url, token_env } => {
                let token = token_env.as_ref().and_then(|e| std::env::var(e).ok());
                pool.scaler(WebhookScaler::new(url.clone(), token))
            }
        };
        if let Some(t) = pc.lease_ttl {
            lease_ttl.insert(name.clone(), t);
        }
        remote_pools.insert(name);
        builder = builder.pool(pool);
    }
    for r in &cfg.resources {
        builder = builder.resource(r.to_config());
    }
    tracing::info!("waiting for leadership");
    let sched = tokio::select! {
        s = builder.build() => s?,
        _ = wait_flag(stop_rx.clone()) => return Ok(()),
    };
    registry.attach(sched.clone());
    tracing::info!("leader: serving");
    let pruner = match (&pg, &cfg.store) {
        (
            Some(pg),
            StoreConfig::Postgres {
                keep_finished_s, ..
            },
        ) => Some(crate::pg::spawn_pruner(
            pg.clone(),
            Duration::from_secs_f64(keep_finished_s.unwrap_or(7.0 * 86400.0)),
            Duration::from_secs(3600),
        )),
        _ => None,
    };

    let state = Arc::new(AppState {
        sched: sched.clone(),
        auth,
        registry,
        outputs,
        remote_pools,
        lease_ttl,
        ready: ready.clone(),
        pg: pg.clone(),
    });

    // Phase 2: full API. Restart the HTTP listener with the API routes.
    let _ = standby_stop_tx.send(true);
    standby.await??;
    let http_listener = TcpListener::bind(bound.http).await?;
    let router =
        crate::http::health_router(ready.clone()).merge(crate::http::api_router(state.clone()));
    let http_stop = stop_rx.clone();
    let http = tokio::spawn(async move {
        axum::serve(http_listener, router)
            .with_graceful_shutdown(wait_flag(http_stop))
            .await
            .map_err(anyhow::Error::from)
    });

    let mut grpc = tonic::transport::Server::builder();
    if let Some(tls) = &cfg.server.tls {
        let cert = std::fs::read(&tls.cert)?;
        let key = std::fs::read(&tls.key)?;
        let mut t = tonic::transport::ServerTlsConfig::new()
            .identity(tonic::transport::Identity::from_pem(cert, key));
        if let Some(ca) = &tls.client_ca {
            t = t.client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(ca)?));
        }
        grpc = grpc.tls_config(t)?;
    }
    let grpc_stop = stop_rx.clone();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);
    let grpc = tokio::spawn(
        grpc.add_service(pb::tasks_server::TasksServer::new(TasksService {
            state: state.clone(),
        }))
        .add_service(pb::workers_server::WorkersServer::new(WorkersService {
            state: state.clone(),
        }))
        .add_service(pb::admin_server::AdminServer::new(AdminService {
            state: state.clone(),
        }))
        .serve_with_incoming_shutdown(incoming, wait_flag(grpc_stop)),
    );
    ready.store(true, Ordering::SeqCst);
    on_bound(bound);

    let lost_leadership = tokio::select! {
        _ = shutdown => false,
        _ = sched.stopped() => true,
        _ = crate::pg::leadership_lost(pg.clone()) => true,
    };
    if let Some(p) = pruner {
        p.abort();
    }
    ready.store(false, Ordering::SeqCst);
    let grace = Duration::from_secs_f64(cfg.server.shutdown_grace_s.max(0.0));
    let result = sched.shutdown(grace).await;
    let _ = stop_tx.send(true);
    let _ = http.await;
    let _ = grpc.await;
    if lost_leadership {
        anyhow::bail!("lost leadership or scheduler failed: {:?}", result.err());
    }
    Ok(())
}

async fn wait_flag(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}
