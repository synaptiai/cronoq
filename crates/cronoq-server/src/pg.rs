//! Postgres store wiring.

use cronoq_store_postgres::{PgStore, PgStoreConfig};

/// Connect (applying migrations).
pub async fn open(url: &str, schema: Option<&str>) -> anyhow::Result<crate::store::Opened> {
    let mut cfg = PgStoreConfig::default();
    if let Some(s) = schema {
        cfg.schema = s.into();
    }
    let pg = PgStore::connect_with(url, cfg)
        .await
        .map_err(|e| anyhow::anyhow!("postgres: {e}"))?;
    Ok(crate::store::Opened::durable(pg))
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
