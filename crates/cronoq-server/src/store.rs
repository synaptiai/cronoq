//! The scheduler's store, and the lookups and housekeeping the server needs from it.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cronoq::{MemoryStore, Store, StoreQueries};

/// An opened store: what the scheduler writes to, and, for durable stores, what the server
/// queries.
pub struct Opened {
    /// The scheduler's store.
    pub store: Arc<dyn Store<Bytes, Bytes>>,
    /// Lookups, pruning and leadership, for durable stores.
    pub queries: Option<Arc<dyn StoreQueries<Bytes>>>,
}

impl Opened {
    /// State in memory only.
    pub fn memory() -> Opened {
        Opened {
            store: Arc::new(MemoryStore),
            queries: None,
        }
    }

    /// A durable store serving both roles.
    pub fn durable<S: Store<Bytes, Bytes> + StoreQueries<Bytes> + Clone>(s: S) -> Opened {
        Opened {
            store: Arc::new(s.clone()),
            queries: Some(Arc::new(s)),
        }
    }
}

/// Resolves when leadership is lost (never, without a durable store).
pub async fn leadership_lost(queries: Option<Arc<dyn StoreQueries<Bytes>>>) {
    match queries {
        Some(q) => q.leadership_lost().await,
        None => std::future::pending().await,
    }
}

/// Delete finished tasks older than `keep`, every `every`, while this instance leads.
pub fn spawn_pruner(
    queries: Arc<dyn StoreQueries<Bytes>>,
    keep: Duration,
    every: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(every);
        loop {
            iv.tick().await;
            if !queries.is_leader() {
                continue;
            }
            match queries.prune(keep).await {
                Ok(n) if n > 0 => tracing::info!(rows = n, "pruned finished tasks"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "prune failed"),
            }
        }
    })
}
