//! Shared server state.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::auth::Auth;
use crate::remote::{OutputCache, WorkerRegistry};

pub struct AppState {
    pub sched: cronoq::Scheduler<Bytes, Bytes>,
    pub auth: Auth,
    pub registry: Arc<WorkerRegistry>,
    pub outputs: Arc<OutputCache>,
    /// Pools whose capacity comes from connected remote workers.
    pub remote_pools: FxHashSet<Arc<str>>,
    pub lease_ttl: FxHashMap<Arc<str>, Duration>,
    /// True while this instance is the leader and serving.
    pub ready: Arc<AtomicBool>,
    /// Durable store, for outputs no longer in the cache.
    pub pg: Option<cronoq_store_postgres::PgStore>,
}

impl AppState {
    /// Output of a finished task: from the cache, else from the database.
    pub async fn output(&self, id: cronoq::TaskId) -> Option<Bytes> {
        if let Some(o) = self.outputs.get(id) {
            return Some(o);
        }
        let pg = self.pg.as_ref()?;
        pg.get_task(&cronoq::TaskRef::Id(id))
            .await
            .ok()
            .flatten()
            .and_then(|r| r.output)
    }
}
