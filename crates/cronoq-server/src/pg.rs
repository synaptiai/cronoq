//! Postgres store wiring.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cronoq::Store;
use cronoq_store_postgres::PgStore;

/// Connect (applying migrations) and return the store both as a trait object and concretely.
pub async fn open(url: &str) -> anyhow::Result<(Arc<dyn Store<Bytes, Bytes>>, PgStore)> {
    let pg = PgStore::connect(url)
        .await
        .map_err(|e| anyhow::anyhow!("postgres: {e}"))?;
    Ok((Arc::new(pg.clone()), pg))
}

/// Warn about connection settings that are risky outside a trusted network.
pub fn warn_insecure_url(url: &str, inline: bool) {
    if inline {
        tracing::warn!(
            "store.url is set inline in the config file; prefer store.url_env to keep credentials out of files"
        );
    }
    let lower = url.to_ascii_lowercase();
    let local = ["@localhost", "@127.0.0.1", "@[::1]", "host=/", "@/"]
        .iter()
        .any(|h| lower.contains(h));
    if !local && !lower.contains("sslmode=verify-full") && !lower.contains("sslmode=verify-ca") {
        tracing::warn!(
            "the Postgres URL does not set sslmode=verify-full; connections to a remote database may be unencrypted or unauthenticated"
        );
    }
}

/// Resolves when leadership is lost (never, without a store).
pub fn leadership_lost(pg: Option<PgStore>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        match pg {
            Some(pg) => pg.leadership_lost().await,
            None => std::future::pending().await,
        }
    })
}

/// Delete terminal rows older than `keep`, every `every`, while this instance leads.
pub fn spawn_pruner(pg: PgStore, keep: Duration, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(every);
        loop {
            iv.tick().await;
            if !pg.is_leader() {
                continue;
            }
            match pg.prune(keep).await {
                Ok(n) if n > 0 => tracing::info!(rows = n, "pruned finished tasks"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "prune failed"),
            }
        }
    })
}
