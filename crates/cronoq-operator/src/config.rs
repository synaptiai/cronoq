//! The operator's configuration: the servers it may read, with their tokens.
//!
//! Server addresses and tokens live here, not in the custom resources, so a resource can only
//! name a configured server: the operator never sends a token to an address a resource names,
//! and needs no access to Secrets.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;

/// The operator's configuration file (TOML).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// How often each scaler is reconciled, in seconds (default 15).
    #[serde(default = "default_interval")]
    pub interval_s: f64,
    /// The servers scalers may name.
    pub servers: Vec<Server>,
}

/// A cronoq server the operator reads snapshots from.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    /// Name that scalers use in `spec.server`.
    pub name: String,
    /// gRPC endpoint (`http://` or `https://`).
    pub url: String,
    /// File holding a token with the metrics role.
    pub token_file: Option<PathBuf>,
    /// PEM CA to verify an `https://` server (default: the system roots).
    pub ca_file: Option<PathBuf>,
}

fn default_interval() -> f64 {
    15.0
}

impl Config {
    /// Read and check the configuration at `path`.
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.check()?;
        Ok(cfg)
    }

    fn check(&self) -> anyhow::Result<()> {
        if !(self.interval_s.is_finite() && self.interval_s >= 1.0) {
            bail!("interval_s must be at least 1");
        }
        if self.servers.is_empty() {
            bail!("at least one server is required");
        }
        let mut seen = HashMap::new();
        for s in &self.servers {
            if s.name.is_empty() || seen.insert(s.name.as_str(), ()).is_some() {
                bail!("server names must be non-empty and unique ('{}')", s.name);
            }
            let https = s.url.starts_with("https://");
            if !https && !s.url.starts_with("http://") {
                bail!(
                    "server '{}': url must start with http:// or https://",
                    s.name
                );
            }
            if !https && s.ca_file.is_some() {
                bail!("server '{}': ca_file needs an https:// url", s.name);
            }
        }
        Ok(())
    }

    /// Time between reconciliations.
    pub fn interval(&self) -> Duration {
        Duration::from_secs_f64(self.interval_s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> anyhow::Result<Config> {
        let c: Config = toml::from_str(s)?;
        c.check()?;
        Ok(c)
    }

    #[test]
    fn configurations_are_checked() {
        let ok = parse(
            r#"
            [[servers]]
            name = "main"
            url = "https://cronoq.cronoq:7070"
            token_file = "/var/run/cronoq/main/token"
            ca_file = "/var/run/cronoq/main/ca.crt"
            "#,
        )
        .unwrap();
        assert_eq!(ok.interval(), Duration::from_secs(15));
        assert!(parse("servers = []").is_err());
        assert!(parse("[[servers]]\nname = \"a\"\nurl = \"grpc://x:1\"").is_err());
        assert!(
            parse("[[servers]]\nname = \"a\"\nurl = \"http://x:1\"\nca_file = \"/c\"").is_err()
        );
        assert!(
            parse("[[servers]]\nname = \"a\"\nurl = \"http://x:1\"\n[[servers]]\nname = \"a\"\nurl = \"http://y:1\"")
                .is_err()
        );
        assert!(parse("interval_s = 0\n[[servers]]\nname = \"a\"\nurl = \"http://x:1\"").is_err());
        assert!(parse("unknown = 1\n[[servers]]\nname = \"a\"\nurl = \"http://x:1\"").is_err());
    }
}
