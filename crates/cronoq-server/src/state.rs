//! Shared server state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::auth::Auth;
use crate::config::LimitsConfig;
use crate::info::TaskInfoCache;
use crate::remote::{OutputCache, WorkerRegistry};

/// State that exists for the whole process lifetime, including while standing by.
pub struct Shared {
    /// Credentials.
    pub auth: Auth,
    /// Request limits.
    pub limits: LimitsConfig,
    /// Running without auth on loopback: HTTP requests must name a loopback host, so a web
    /// page cannot reach the API by rebinding its own name to 127.0.0.1.
    pub loopback_hosts_only: bool,
    /// True while this instance is the leader and serving.
    pub ready: Arc<AtomicBool>,
    /// Set once leadership is acquired and the scheduler is running.
    pub app: OnceLock<Arc<AppState>>,
    /// Cancelled when the server stops serving (ends Watch streams, listeners).
    pub stop: CancellationToken,
}

impl Shared {
    /// The serving state, or `None` while standing by (or after stopping).
    pub fn app(&self) -> Option<&Arc<AppState>> {
        if self.stop.is_cancelled() || !self.ready.load(Ordering::SeqCst) {
            return None;
        }
        self.app.get()
    }

    /// The serving state as a gRPC result.
    pub fn app_or_unavailable(&self) -> Result<&Arc<AppState>, tonic::Status> {
        self.app()
            .ok_or_else(|| tonic::Status::unavailable("not the leader (standby or shutting down)"))
    }
}

/// Per-pool server settings.
#[derive(Clone, Debug)]
pub struct PoolInfo {
    /// Slots a single worker may register.
    pub slot_cap: u32,
    /// Lease TTL workers must heartbeat within.
    pub lease_ttl: Duration,
    /// Attempts allowed by the pool's default retry policy.
    pub max_attempts: u32,
}

/// State available while serving as the leader.
pub struct AppState {
    /// The scheduler.
    pub sched: cronoq::Scheduler<Bytes, Bytes>,
    /// Connected workers.
    pub registry: Arc<WorkerRegistry>,
    /// Recent outputs.
    pub outputs: Arc<OutputCache>,
    /// Recent task details.
    pub info: Arc<TaskInfoCache>,
    /// Pools by name.
    pub pools: FxHashMap<Arc<str>, PoolInfo>,
    /// Request limits.
    pub limits: LimitsConfig,
    /// Durable store, for outputs and details no longer cached.
    pub queries: Option<Arc<dyn cronoq::StoreQueries<Bytes>>>,
}

impl AppState {
    /// Output of a finished task: from the cache, else from the database.
    pub async fn output(&self, id: cronoq::TaskId) -> Option<Bytes> {
        if let Some(o) = self.outputs.get(id) {
            return Some(o);
        }
        let q = self.queries.as_ref()?;
        q.get_task(&cronoq::TaskRef::Id(id))
            .await
            .ok()
            .flatten()
            .and_then(|r| r.output)
    }

    /// Pool of a task, from the info cache or the database.
    pub async fn pool_of(&self, id: cronoq::TaskId) -> Option<Arc<str>> {
        if let Some(m) = self.info.get(id.0) {
            return Some(m.pool);
        }
        let q = self.queries.as_ref()?;
        q.get_task(&cronoq::TaskRef::Id(id))
            .await
            .ok()
            .flatten()
            .map(|r| Arc::from(r.pool.as_str()))
    }

    /// The pool's settings.
    pub fn pool(&self, name: &str) -> Option<&PoolInfo> {
        self.pools.get(name)
    }
}
