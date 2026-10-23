//! Snapshots of the configured servers, cached for a fraction of the reconcile interval so that
//! many scalers of one server share one call.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use cronoq_proto::v1 as pb;
use tokio::sync::Mutex;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::config::{Config, Server};

/// A snapshot, or why it could not be read.
type Fetched = Result<Arc<pb::SnapshotResponse>, String>;

struct Entry {
    server: Server,
    channel: Channel,
    cached: Mutex<Option<(Instant, Fetched)>>,
}

/// The configured servers.
pub struct Servers {
    entries: HashMap<String, Entry>,
    ttl: Duration,
}

impl Servers {
    /// Connections (opened on first use) to every configured server.
    pub fn new(cfg: Config) -> anyhow::Result<Servers> {
        let ttl = cfg.interval() / 2;
        let mut entries = HashMap::new();
        for server in cfg.servers {
            let mut ep = Endpoint::from_shared(server.url.clone())
                .with_context(|| format!("server '{}': bad url", server.name))?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10));
            if server.url.starts_with("https://") {
                let mut tls = ClientTlsConfig::new();
                tls = match &server.ca_file {
                    Some(p) => tls.ca_certificate(Certificate::from_pem(
                        std::fs::read(p).with_context(|| format!("reading {}", p.display()))?,
                    )),
                    None => tls.with_native_roots(),
                };
                ep = ep.tls_config(tls)?;
            } else if server.token_file.is_some() && !is_loopback(&server.url) {
                tracing::warn!(server = %server.name, url = %server.url, "the metrics token is sent over plain http; use https outside a trusted network");
            }
            let channel = ep.connect_lazy();
            entries.insert(
                server.name.clone(),
                Entry {
                    server,
                    channel,
                    cached: Mutex::new(None),
                },
            );
        }
        Ok(Servers { entries, ttl })
    }

    /// The snapshot of server `name`, at most `ttl` old. Errors are cached too, so a down server
    /// is not asked once per scaler.
    pub async fn snapshot(&self, name: &str) -> Fetched {
        let entry = self
            .entries
            .get(name)
            .ok_or_else(|| format!("server '{name}' is not in the operator's configuration"))?;
        let mut cached = entry.cached.lock().await;
        if let Some((at, r)) = cached.as_ref()
            && at.elapsed() < self.ttl
        {
            return r.clone();
        }
        let r = fetch(entry)
            .await
            .map(Arc::new)
            .map_err(|e| format!("{e:#}"));
        *cached = Some((Instant::now(), r.clone()));
        r
    }
}

async fn fetch(entry: &Entry) -> anyhow::Result<pb::SnapshotResponse> {
    // Read the token on every call, so a rotated Secret takes effect without a restart.
    let token: Option<MetadataValue<Ascii>> = match &entry.server.token_file {
        Some(p) => {
            let t = tokio::fs::read_to_string(p)
                .await
                .with_context(|| format!("reading {}", p.display()))?;
            let mut v: MetadataValue<Ascii> = format!("Bearer {}", t.trim())
                .parse()
                .map_err(|_| anyhow!("the token in {} is not a valid header", p.display()))?;
            v.set_sensitive(true);
            Some(v)
        }
        None => None,
    };
    let mut client = pb::admin_client::AdminClient::with_interceptor(
        entry.channel.clone(),
        move |mut req: tonic::Request<()>| {
            if let Some(t) = &token {
                req.metadata_mut().insert("authorization", t.clone());
            }
            Ok(req)
        },
    );
    let r = client
        .snapshot(pb::Empty {})
        .await
        .map_err(|s| anyhow!("snapshot: {}: {}", s.code(), s.message()))?;
    Ok(r.into_inner())
}

/// Whether the url's host is `localhost` or a loopback address.
fn is_loopback(url: &str) -> bool {
    let Ok(uri) = url.parse::<tonic::transport::Uri>() else {
        return false;
    };
    let host = uri.host().unwrap_or("");
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The pool's desired capacity in slots, or why it cannot be read from `snap`.
pub fn desired(snap: &pb::SnapshotResponse, pool: &str) -> Result<u32, String> {
    if let Some(p) = snap.pools.iter().find(|p| p.name == pool) {
        return Ok(p.desired);
    }
    if snap.unavailable_shards.is_empty() {
        Err(format!(
            "the server has no pool '{pool}' (or the metrics token may not see it)"
        ))
    } else {
        Err(format!(
            "pool '{pool}' not found; shards unreachable: {}",
            snap.unavailable_shards.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desired_comes_from_the_pool() {
        let snap = pb::SnapshotResponse {
            pools: vec![pb::PoolSnapshot {
                name: "llm".into(),
                desired: 7,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(desired(&snap, "llm"), Ok(7));
        assert!(desired(&snap, "gpu").unwrap_err().contains("no pool"));
        let partial = pb::SnapshotResponse {
            unavailable_shards: vec!["gpu".into()],
            ..snap
        };
        assert!(
            desired(&partial, "gpu")
                .unwrap_err()
                .contains("unreachable: gpu")
        );
    }

    #[test]
    fn loopback_is_read_from_the_parsed_host() {
        assert!(is_loopback("http://127.0.0.1:7070"));
        assert!(is_loopback("http://localhost:7070"));
        assert!(!is_loopback("http://127.0.0.1:1@cronoq:7070"));
        assert!(!is_loopback("http://cronoq.cronoq:7070"));
    }
}
