//! Server configuration (TOML file, with environment variables for secrets).
//!
//! Every numeric option is validated by [`Config::validate`], which [`crate::run`] also calls,
//! so a bad value is reported as an error at startup instead of panicking later.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use cronoq::{AutoscaleConfig, OverflowPolicy, PoolConfig, ResourceConfig, RetryPolicy};
use serde::Deserialize;

/// Longest duration accepted anywhere in the configuration (one year).
const MAX_CONFIG_SECS: f64 = 365.0 * 86400.0;

/// Top-level server configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listeners, TLS and shutdown behaviour.
    #[serde(default)]
    pub server: ServerConfig,
    /// Bearer tokens and client-certificate role mappings.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Where scheduler state is kept.
    #[serde(default)]
    pub store: StoreConfig,
    /// Request and resource limits.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Worker pools (each served by remote workers).
    #[serde(default = "default_pools")]
    pub pools: Vec<PoolSection>,
    /// Shared resources (semaphores and token buckets).
    #[serde(default)]
    pub resources: Vec<ResourceSection>,
    /// In-memory cache of recent task outputs.
    #[serde(default)]
    pub outputs: OutputsConfig,
    /// Groups of pools served by separate leaders. Every shard's servers read the same list
    /// and pick their own with `server.shard`.
    #[serde(default)]
    pub shards: Vec<ShardSection>,
}

/// A group of pools served by its own leader (and standbys).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSection {
    /// Name: lower-case letters, digits and `_`, starting with a letter (at most 32).
    pub name: String,
    /// Number carried in the shard's task ids (0-255, unique).
    pub number: u8,
    /// The pools this shard serves; every pool belongs to exactly one shard.
    pub pools: Vec<String>,
    /// gRPC endpoint (`https://...` or `http://...`) that reaches the shard's leader, for other
    /// servers and for workers.
    pub endpoint: String,
}

fn valid_shard_name(s: &str) -> bool {
    s.len() <= 32
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The shard this process serves, and where every shard is.
#[derive(Clone, Debug)]
pub struct ShardPlan {
    /// This process's shard.
    pub own: ShardSection,
    /// All shards, including this one.
    pub all: Vec<ShardSection>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig::default(),
            auth: AuthConfig::default(),
            store: StoreConfig::default(),
            limits: LimitsConfig::default(),
            shards: Vec::new(),
            pools: default_pools(),
            resources: Vec::new(),
            outputs: OutputsConfig::default(),
        }
    }
}

fn default_pools() -> Vec<PoolSection> {
    vec![PoolSection::named("default")]
}

fn finite_secs(what: &str, v: f64, min: f64) -> anyhow::Result<()> {
    ensure!(
        v.is_finite() && v >= min && v <= MAX_CONFIG_SECS,
        "{what} must be a number of seconds between {min} and {MAX_CONFIG_SECS} (got {v})"
    );
    Ok(())
}

fn opt_secs(what: &str, v: Option<f64>, min: f64) -> anyhow::Result<()> {
    match v {
        Some(v) => finite_secs(what, v, min),
        None => Ok(()),
    }
}

/// Names of pools and resources appear in metrics labels and logs: keep them simple.
pub(crate) fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
}

impl Config {
    /// Read and validate a TOML file.
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let cfg = Self::read(path)?;
        cfg.validate()
            .with_context(|| format!("in config file {}", path.display()))?;
        Ok(cfg)
    }

    /// Read a TOML file without validating it, to apply command-line overrides first; call
    /// [`Config::validate`] afterwards ([`crate::run`] does).
    pub fn read(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("in config file {}", path.display()))
    }

    /// Parse and validate TOML text.
    pub fn parse(text: &str) -> anyhow::Result<Config> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check every option. Called by [`Config::parse`] and again by [`crate::run`].
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.pools.is_empty(), "at least one pool is required");
        let s = &self.server;
        finite_secs("server.shutdown_grace_s", s.shutdown_grace_s, 0.0)?;
        ensure!(
            self.outputs.cache_entries == 0 || self.outputs.cache_bytes > 0,
            "outputs.cache_bytes must be positive (set outputs.cache_entries = 0 to disable the cache)"
        );
        if let Some(tls) = &s.tls {
            ensure!(
                !tls.cert.as_os_str().is_empty() && !tls.key.as_os_str().is_empty(),
                "server.tls needs cert and key"
            );
        }
        if let Some(w) = &s.grpc_web {
            ensure!(
                (1..=64).contains(&w.allowed_origins.len()),
                "server.grpc_web.allowed_origins needs 1 to 64 origins"
            );
            for o in &w.allowed_origins {
                ensure!(
                    valid_origin(o),
                    "server.grpc_web.allowed_origins: {o:?} is not an origin \
                     (scheme://host[:port], no path, no wildcard)"
                );
            }
        }
        let mut names = HashSet::new();
        for p in &self.pools {
            ensure!(
                valid_name(&p.name),
                "pool name {:?} is invalid: use 1-64 characters from [A-Za-z0-9_.:-]",
                p.name
            );
            ensure!(names.insert(p.name.as_str()), "duplicate pool '{}'", p.name);
            ensure!(p.max >= 1, "pool '{}': max must be at least 1", p.name);
            ensure!(p.min <= p.max, "pool '{}': min > max", p.name);
            let at = |k: &str| format!("pool '{}': {k}", p.name);
            finite_secs(&at("lease_ttl_s"), p.lease_ttl_s, 0.1)?;
            opt_secs(&at("default_duration_s"), p.default_duration_s, 0.0)?;
            opt_secs(
                &at("await_unknown_dependencies_s"),
                p.await_unknown_dependencies_s,
                0.0,
            )?;
            opt_secs(&at("drain_time_s"), p.drain_time_s, 0.001)?;
            opt_secs(&at("cooldown_s"), p.cooldown_s, 0.0)?;
            if let Some(f) = p.reservable_fraction {
                ensure!(
                    f.is_finite() && f > 0.0 && f <= 1.0,
                    "{} must be in (0, 1]",
                    at("reservable_fraction")
                );
            }
            for (t, w) in &p.tenant_weights {
                ensure!(
                    w.is_finite() && *w > 0.0,
                    "{} must be finite and > 0",
                    at(&format!("tenant_weights.{t}"))
                );
            }
            if let Some(u) = p.target_utilization {
                ensure!(
                    u.is_finite() && u > 0.0 && u <= 1.0,
                    "{} must be in (0, 1]",
                    at("target_utilization")
                );
            }
            if let Some(c) = p.cost_per_worker_second {
                ensure!(
                    c.is_finite() && c >= 0.0,
                    "{} must be a non-negative number",
                    at("cost_per_worker_second")
                );
            }
            if let Some(m) = p.max_pending {
                ensure!(m >= 1, "{} must be at least 1", at("max_pending"));
            }
            if let Some(m) = p.max_slots_per_worker {
                ensure!(m >= 1, "{} must be at least 1", at("max_slots_per_worker"));
            }
            if let Some(r) = &p.retry {
                r.validate(&at("retry"))?;
            }
            if let ScalerSection::Webhook { url, .. } = &p.scaler {
                ensure!(
                    url.starts_with("https://") || url.starts_with("http://"),
                    "{} must be an http(s) URL",
                    at("scaler.url")
                );
            }
        }
        let mut rnames = HashSet::new();
        for r in &self.resources {
            let name = r.name();
            ensure!(
                valid_name(name),
                "resource name {name:?} is invalid: use 1-64 characters from [A-Za-z0-9_.:-]"
            );
            ensure!(rnames.insert(name), "duplicate resource '{name}'");
            match r {
                ResourceSection::Semaphore { permits, .. } => {
                    ensure!(
                        *permits >= 1,
                        "resource '{name}': permits must be at least 1"
                    )
                }
                ResourceSection::TokenBucket { rate, burst, .. } => {
                    ensure!(
                        rate.is_finite() && *rate > 0.0,
                        "resource '{name}': rate must be a positive number"
                    );
                    ensure!(*burst >= 1, "resource '{name}': burst must be at least 1");
                }
            }
        }
        let mut literal_tokens = HashSet::new();
        for t in &self.auth.tokens {
            ensure!(
                t.token.is_some() != t.token_env.is_some(),
                "each [[auth.tokens]] entry needs exactly one of `token` or `token_env`"
            );
            if let Some(v) = &t.token {
                ensure!(
                    literal_tokens.insert(v.as_str()),
                    "duplicate auth token (each token must be unique)"
                );
            }
            for pool in t.pools.iter().flatten() {
                ensure!(
                    names.contains(pool.as_str()),
                    "auth token restricted to unknown pool '{pool}'"
                );
            }
        }
        for c in &self.auth.certs {
            ensure!(
                !c.dns_name.is_empty(),
                "[[auth.certs]] entries need a dns_name"
            );
            for pool in c.pools.iter().flatten() {
                ensure!(
                    names.contains(pool.as_str()),
                    "certificate mapping restricted to unknown pool '{pool}'"
                );
            }
        }
        if !self.auth.certs.is_empty() {
            ensure!(
                s.tls.as_ref().is_some_and(|t| t.client_ca.is_some()),
                "[[auth.certs]] requires server.tls.client_ca (mTLS)"
            );
        }
        self.limits.validate()?;
        match &self.store {
            StoreConfig::Memory => {}
            StoreConfig::Postgres {
                keep_finished_s,
                url,
                url_env,
                ..
            } => {
                opt_secs("store.keep_finished_s", *keep_finished_s, 60.0)?;
                ensure!(
                    !(url.is_some() && url_env.is_some()),
                    "store: set either url or url_env, not both"
                );
            }
            StoreConfig::Sqlite {
                path,
                keep_finished_s,
            } => {
                opt_secs("store.keep_finished_s", *keep_finished_s, 60.0)?;
                ensure!(
                    !path.as_os_str().is_empty(),
                    "store.path must name the database file"
                );
            }
        }
        self.validate_shards()?;
        Ok(())
    }

    fn validate_shards(&self) -> anyhow::Result<()> {
        if self.shards.is_empty() {
            ensure!(
                self.server.shard.is_none(),
                "server.shard is set but no [[shards]] are configured"
            );
            return Ok(());
        }
        let mut names = HashSet::new();
        let mut numbers = HashSet::new();
        let mut endpoints = HashSet::new();
        let mut owner: HashMap<&str, &str> = HashMap::new();
        for sh in &self.shards {
            ensure!(
                valid_shard_name(&sh.name),
                "shard name {:?} is invalid: use 1-32 lower-case letters, digits and '_', \
                 starting with a letter",
                sh.name
            );
            ensure!(
                names.insert(sh.name.as_str()),
                "duplicate shard '{}'",
                sh.name
            );
            ensure!(
                numbers.insert(sh.number),
                "shards '{}' and another share number {}",
                sh.name,
                sh.number
            );
            ensure!(
                (sh.endpoint.starts_with("https://") || sh.endpoint.starts_with("http://"))
                    && sh.endpoint.len() <= 256
                    && !sh
                        .endpoint
                        .chars()
                        .any(|c| c.is_whitespace() || c.is_control())
                    && http::Uri::try_from(sh.endpoint.as_str()).is_ok_and(|u| u.host().is_some()),
                "shard '{}': endpoint {:?} must be an http:// or https:// URL",
                sh.name,
                sh.endpoint
            );
            ensure!(
                endpoints.insert(sh.endpoint.as_str()),
                "shard '{}' repeats the endpoint {}",
                sh.name,
                sh.endpoint
            );
            ensure!(!sh.pools.is_empty(), "shard '{}' has no pools", sh.name);
            for p in &sh.pools {
                ensure!(
                    self.pools.iter().any(|q| q.name == *p),
                    "shard '{}' names unknown pool '{p}'",
                    sh.name
                );
                if let Some(other) = owner.insert(p.as_str(), sh.name.as_str()) {
                    bail!("pool '{p}' is in shards '{other}' and '{}'", sh.name);
                }
            }
        }
        for p in &self.pools {
            ensure!(
                owner.contains_key(p.name.as_str()),
                "pool '{}' is in no shard: with [[shards]], every pool belongs to one",
                p.name
            );
        }
        let Some(own) = &self.server.shard else {
            bail!(
                "[[shards]] are configured: choose this server's shard with server.shard or --shard"
            );
        };
        ensure!(
            names.contains(own.as_str()),
            "server.shard '{own}' is not one of the configured [[shards]]"
        );
        match &self.store {
            StoreConfig::Memory => {}
            StoreConfig::Postgres { schema, .. } => ensure!(
                schema.as_deref().is_none_or(|s| s.contains("{shard}")),
                "store.schema must contain {{shard}} with [[shards]], so that each shard has \
                 its own tables (the default is cronoq_{{shard}})"
            ),
            StoreConfig::Sqlite { path, .. } => ensure!(
                path.to_string_lossy().contains("{shard}"),
                "store.path must contain {{shard}} with [[shards]], so that each shard has its \
                 own file"
            ),
        }
        Ok(())
    }

    /// This server's shard and the full shard list, if the configuration has shards.
    pub fn shard_plan(&self) -> Option<ShardPlan> {
        let own = self.server.shard.as_ref()?;
        Some(ShardPlan {
            own: self.shards.iter().find(|s| &s.name == own)?.clone(),
            all: self.shards.clone(),
        })
    }

    /// The configuration this server runs: only its shard's pools, and the store namespace
    /// (`{shard}` in the schema or path) filled in. Unchanged without shards.
    pub fn for_own_shard(&self) -> Config {
        let Some(plan) = self.shard_plan() else {
            return self.clone();
        };
        let mut cfg = self.clone();
        cfg.pools.retain(|p| plan.own.pools.contains(&p.name));
        let fill = |s: &str| s.replace("{shard}", &plan.own.name);
        cfg.store = match &self.store {
            StoreConfig::Memory => StoreConfig::Memory,
            StoreConfig::Postgres {
                url,
                url_env,
                keep_finished_s,
                schema,
            } => StoreConfig::Postgres {
                url: url.clone(),
                url_env: url_env.clone(),
                keep_finished_s: *keep_finished_s,
                schema: Some(fill(schema.as_deref().unwrap_or("cronoq_{shard}"))),
            },
            StoreConfig::Sqlite {
                path,
                keep_finished_s,
            } => StoreConfig::Sqlite {
                path: fill(&path.to_string_lossy()).into(),
                keep_finished_s: *keep_finished_s,
            },
        };
        cfg
    }

    /// Validation that depends on the environment (token values) and the bind addresses.
    pub(crate) fn check_auth_exposure(&self, auth_enabled: bool) -> anyhow::Result<()> {
        if auth_enabled || self.server.insecure_no_auth {
            return Ok(());
        }
        let public = |a: &SocketAddr| !a.ip().is_loopback();
        let s = &self.server;
        if public(&s.grpc_addr)
            || public(&s.http_addr)
            || s.metrics_addr.as_ref().is_some_and(public)
        {
            bail!(
                "no auth tokens are configured but the server listens on a non-loopback address; \
                 configure [[auth.tokens]] (or pass --insecure-no-auth for a trusted network)"
            );
        }
        Ok(())
    }
}

/// Listeners, TLS and shutdown behaviour.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// gRPC listener (default `127.0.0.1:7070`).
    #[serde(default = "default_grpc")]
    pub grpc_addr: SocketAddr,
    /// HTTP/JSON listener (default `127.0.0.1:7071`).
    #[serde(default = "default_http")]
    pub http_addr: SocketAddr,
    /// Optional unauthenticated listener serving only `/metrics` (bind it to an internal
    /// network). Without it, `/metrics` on `http_addr` requires a metrics or admin token.
    pub metrics_addr: Option<SocketAddr>,
    /// TLS for both the gRPC and the HTTP listener (and mTLS with `client_ca`).
    pub tls: Option<TlsConfig>,
    /// Allow running without any auth token on non-loopback addresses.
    #[serde(default)]
    pub insecure_no_auth: bool,
    /// Grace period for running attempts on shutdown.
    #[serde(default = "default_grace")]
    pub shutdown_grace_s: f64,
    /// Serve gRPC-Web for browsers on the gRPC listener (off unless set).
    pub grpc_web: Option<GrpcWebConfig>,
    /// The shard this server serves, one of `[[shards]]` (also `--shard`).
    pub shard: Option<String>,
}

/// gRPC-Web for browser clients.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcWebConfig {
    /// Origins (`https://dash.example.com`, scheme, host and optional port) whose pages may
    /// call the API. Credentials are still required as for any other call.
    pub allowed_origins: Vec<String>,
}

/// Whether `o` is an exact origin: `http` or `https`, a host, an optional port, nothing else.
fn valid_origin(o: &str) -> bool {
    let Some((scheme, rest)) = o.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https") || rest.is_empty() || o.len() > 256 {
        return false;
    }
    let (host, port) = match rest.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((ip, port)) => {
                if ip.parse::<std::net::Ipv6Addr>().is_err() {
                    return false;
                }
                (ip, port)
            }
            None => return false,
        },
        None => match rest.find(':') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        },
    };
    let port_ok = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| !p.is_empty() && p.parse::<u16>().is_ok());
    let host_ok = !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'));
    port_ok && host_ok
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            grpc_addr: default_grpc(),
            http_addr: default_http(),
            metrics_addr: None,
            tls: None,
            insecure_no_auth: false,
            shutdown_grace_s: default_grace(),
            grpc_web: None,
            shard: None,
        }
    }
}

fn default_grpc() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}
fn default_http() -> SocketAddr {
    "127.0.0.1:7071".parse().unwrap()
}
fn default_grace() -> f64 {
    10.0
}

/// TLS material, read once at startup.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain.
    pub cert: PathBuf,
    /// PEM private key.
    pub key: PathBuf,
    /// Require client certificates signed by this CA (mTLS) on both listeners.
    pub client_ca: Option<PathBuf>,
}

/// Permission level of a token or client certificate. `Admin` may do everything.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Submit, inspect, wait for, cancel and watch tasks.
    Client,
    /// Serve leases on the `Workers` stream.
    Worker,
    /// Read snapshots and metrics.
    Metrics,
    /// Everything, including recurring schedules.
    Admin,
}

impl Role {
    /// Name used in configuration and errors.
    pub fn name(self) -> &'static str {
        match self {
            Role::Client => "client",
            Role::Worker => "worker",
            Role::Metrics => "metrics",
            Role::Admin => "admin",
        }
    }
}

/// Authentication settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Bearer tokens.
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
    /// Roles for client certificates (mTLS), matched on a DNS subject alternative name.
    #[serde(default)]
    pub certs: Vec<CertRoleConfig>,
}

/// One bearer token.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    /// The token (prefer `token_env`; at least 16 characters, 32+ recommended).
    pub token: Option<String>,
    /// Read the token from this environment variable instead.
    pub token_env: Option<String>,
    /// What the token may do.
    pub role: Role,
    /// Restrict a client or worker token to these pools.
    pub pools: Option<Vec<String>>,
    /// Worker labels (`"name=value"`) a worker with this token may claim (default: any).
    pub labels: Option<Vec<String>>,
    /// Tenant of every task submitted with this token; submitting for another tenant is refused.
    pub tenant: Option<String>,
}

impl fmt::Debug for TokenConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenConfig")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("token_env", &self.token_env)
            .field("role", &self.role)
            .field("pools", &self.pools)
            .finish()
    }
}

/// Role for client certificates presenting a DNS subject alternative name.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertRoleConfig {
    /// DNS SAN to match exactly (e.g. `worker-1.internal`).
    pub dns_name: String,
    /// Role granted.
    pub role: Role,
    /// Optional pool allow-list.
    pub pools: Option<Vec<String>>,
    /// Worker labels (`"name=value"`) a worker with this certificate may claim (default: any).
    pub labels: Option<Vec<String>>,
    /// Tenant of every task submitted with this certificate.
    pub tenant: Option<String>,
}

/// Scheduler state storage.
#[derive(Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreConfig {
    /// In memory only (state is lost on restart).
    #[default]
    Memory,
    /// PostgreSQL: durable state and active-passive HA.
    Postgres {
        /// Connection URL (prefer `url_env`; use `sslmode=verify-full` across networks).
        url: Option<String>,
        /// Read the URL from this environment variable (default `DATABASE_URL`).
        url_env: Option<String>,
        /// Delete finished tasks older than this (default 7 days).
        keep_finished_s: Option<f64>,
        /// PostgreSQL schema for cronoq's tables (default `public`). Instances sharing a schema
        /// form one cluster. A dedicated schema that only cronoq's role can create objects in
        /// keeps other database roles away from its tables.
        schema: Option<String>,
    },
    /// SQLite: durable state in a local file; server processes sharing the file elect one
    /// leader and the others stand by.
    Sqlite {
        /// Database file (created if missing). Keep it on a local filesystem.
        path: std::path::PathBuf,
        /// Delete finished tasks older than this (default 7 days).
        keep_finished_s: Option<f64>,
    },
}

impl fmt::Debug for StoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreConfig::Memory => f.write_str("Memory"),
            StoreConfig::Postgres {
                url,
                url_env,
                keep_finished_s,
                schema,
            } => f
                .debug_struct("Postgres")
                .field("url", &url.as_ref().map(|_| "<redacted>"))
                .field("url_env", url_env)
                .field("keep_finished_s", keep_finished_s)
                .field("schema", schema)
                .finish(),
            StoreConfig::Sqlite {
                path,
                keep_finished_s,
            } => f
                .debug_struct("Sqlite")
                .field("path", path)
                .field("keep_finished_s", keep_finished_s)
                .finish(),
        }
    }
}

/// Request and resource limits (defaults are safe for a shared deployment).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// Tasks per `SubmitBatch` call.
    pub max_tasks_per_batch: usize,
    /// Pending tasks across all pools.
    pub max_pending_total: usize,
    /// Default per-pool pending bound (overridable per pool with `max_pending`).
    pub max_pending_per_pool: usize,
    /// Largest gRPC message accepted or sent (payloads and outputs ride in these).
    pub max_message_bytes: usize,
    /// Largest HTTP request body.
    pub max_http_body_bytes: usize,
    /// Longest task key.
    pub max_key_len: usize,
    /// Longest task kind.
    pub max_kind_len: usize,
    /// Most tags per task.
    pub max_tags: usize,
    /// Longest tag key or value.
    pub max_tag_len: usize,
    /// Most dependencies per task.
    pub max_deps: usize,
    /// Most resource entries per task.
    pub max_resources: usize,
    /// Longest server-side wait for a task result (`Wait`, `Submit{wait}`).
    pub max_wait_s: f64,
    /// Timeout for any other request.
    pub request_timeout_s: f64,
    /// Requests processed concurrently per listener (excess requests queue).
    pub max_concurrent_requests: usize,
    /// HTTP/2 streams per connection.
    pub max_concurrent_streams: u32,
    /// Slots a single worker may register (also capped by the pool's `max`).
    pub max_slots_per_worker: u32,
    /// HTTP/2 keepalive ping interval (evicts half-open workers and clients).
    pub keepalive_interval_s: f64,
    /// HTTP/2 keepalive ping timeout.
    pub keepalive_timeout_s: f64,
    /// How long a finished task's key keeps deduplicating submissions: resubmitting it returns
    /// the finished task (and its stored output). Also how far back a new leader loads
    /// finished tasks from the store.
    pub key_retention_s: f64,
    /// Most finished tasks kept for key deduplication (bounds memory).
    pub key_retention_max: usize,
    /// Most tasks one `ListTasks` page returns.
    pub max_list: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        LimitsConfig {
            max_tasks_per_batch: 1000,
            max_pending_total: 1_000_000,
            max_pending_per_pool: 100_000,
            max_message_bytes: 16 << 20,
            max_http_body_bytes: 16 << 20,
            max_key_len: 256,
            max_kind_len: 128,
            max_tags: 32,
            max_tag_len: 1024,
            max_deps: 256,
            max_resources: 16,
            max_wait_s: 300.0,
            request_timeout_s: 30.0,
            max_concurrent_requests: 4096,
            max_concurrent_streams: 1024,
            max_slots_per_worker: 1024,
            keepalive_interval_s: 20.0,
            keepalive_timeout_s: 20.0,
            key_retention_s: 3600.0,
            key_retention_max: 1_000_000,
            max_list: 1000,
        }
    }
}

impl LimitsConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.max_tasks_per_batch >= 1,
            "limits.max_tasks_per_batch must be at least 1"
        );
        ensure!(
            self.max_pending_total >= 1,
            "limits.max_pending_total must be at least 1"
        );
        ensure!(
            self.max_pending_per_pool >= 1,
            "limits.max_pending_per_pool must be at least 1"
        );
        ensure!(
            self.max_message_bytes >= 4096,
            "limits.max_message_bytes must be at least 4096"
        );
        ensure!(
            self.max_http_body_bytes >= 4096,
            "limits.max_http_body_bytes must be at least 4096"
        );
        ensure!(
            self.max_key_len >= 1 && self.max_kind_len >= 1,
            "key and kind limits must be at least 1"
        );
        ensure!(
            self.max_tag_len >= 1,
            "limits.max_tag_len must be at least 1"
        );
        ensure!(
            self.max_concurrent_requests >= 1,
            "limits.max_concurrent_requests must be at least 1"
        );
        ensure!(
            self.max_concurrent_streams >= 1,
            "limits.max_concurrent_streams must be at least 1"
        );
        ensure!(
            self.max_slots_per_worker >= 1,
            "limits.max_slots_per_worker must be at least 1"
        );
        ensure!(
            (1..=100_000).contains(&self.max_list),
            "limits.max_list must be between 1 and 100000"
        );
        finite_secs("limits.max_wait_s", self.max_wait_s, 0.0)?;
        finite_secs("limits.request_timeout_s", self.request_timeout_s, 0.1)?;
        finite_secs(
            "limits.keepalive_interval_s",
            self.keepalive_interval_s,
            0.1,
        )?;
        finite_secs("limits.keepalive_timeout_s", self.keepalive_timeout_s, 0.1)?;
        finite_secs("limits.key_retention_s", self.key_retention_s, 0.0)?;
        Ok(())
    }

    /// `key_retention_s` as a duration.
    pub fn key_retention(&self) -> Duration {
        Duration::from_secs_f64(self.key_retention_s)
    }

    /// `max_wait_s` as a duration.
    pub fn max_wait(&self) -> Duration {
        Duration::from_secs_f64(self.max_wait_s)
    }

    /// `request_timeout_s` as a duration.
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs_f64(self.request_timeout_s)
    }

    /// `keepalive_interval_s` as a duration.
    pub fn keepalive_interval(&self) -> Duration {
        Duration::from_secs_f64(self.keepalive_interval_s)
    }

    /// `keepalive_timeout_s` as a duration.
    pub fn keepalive_timeout(&self) -> Duration {
        Duration::from_secs_f64(self.keepalive_timeout_s)
    }
}

/// In-memory cache of recent outputs, for `Wait`/`Get` after completion.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputsConfig {
    /// Most outputs kept.
    #[serde(default = "default_cache_entries")]
    pub cache_entries: usize,
    /// Most bytes kept.
    #[serde(default = "default_cache_bytes")]
    pub cache_bytes: usize,
    /// Most task records kept for `Get` (pool, kind, timestamps, last error).
    #[serde(default = "default_info_entries")]
    pub info_entries: usize,
}

impl Default for OutputsConfig {
    fn default() -> Self {
        OutputsConfig {
            cache_entries: default_cache_entries(),
            cache_bytes: default_cache_bytes(),
            info_entries: default_info_entries(),
        }
    }
}

fn default_cache_entries() -> usize {
    10_000
}
fn default_cache_bytes() -> usize {
    64 << 20
}
fn default_info_entries() -> usize {
    100_000
}

/// How a pool's desired capacity reaches workers.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalerSection {
    /// Capacity = slots of connected workers; the desired count is exported as a metric.
    #[default]
    External,
    /// POST `{"pool": ..., "desired": n}` to `url` when the target changes.
    Webhook {
        /// Endpoint (https recommended).
        url: String,
        /// Bearer token for the webhook, from this environment variable (must be set).
        token_env: Option<String>,
    },
}

/// One pool served by remote workers.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSection {
    /// Pool name (1-64 characters from `[A-Za-z0-9_.:-]`).
    pub name: String,
    /// Autoscaler lower bound.
    #[serde(default)]
    pub min: u32,
    /// Autoscaler upper bound, reservation capacity basis, and per-worker slot cap.
    #[serde(default = "default_max")]
    pub max: u32,
    /// Compute a desired capacity from queue pressure.
    #[serde(default = "default_true")]
    pub autoscale: bool,
    /// How the desired capacity is communicated.
    #[serde(default)]
    pub scaler: ScalerSection,
    /// Remote workers must heartbeat each lease within this many seconds.
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_s: f64,
    /// Pending-task bound (default `limits.max_pending_per_pool`).
    pub max_pending: Option<usize>,
    /// Behaviour when the bound is reached.
    #[serde(default = "default_overflow")]
    pub overflow: Overflow,
    /// Default retry policy.
    pub retry: Option<RetrySection>,
    /// Duration estimate when nothing is known.
    pub default_duration_s: Option<f64>,
    /// Share of `max` that fixed-start reservations may occupy, in (0, 1].
    pub reservable_fraction: Option<f64>,
    /// Allow dependencies on keys that do not exist yet (wait up to this many seconds).
    pub await_unknown_dependencies_s: Option<f64>,
    /// Autoscaler utilization target, in (0, 1].
    pub target_utilization: Option<f64>,
    /// Autoscaler backlog drain target.
    pub drain_time_s: Option<f64>,
    /// Autoscaler scale-down cooldown.
    pub cooldown_s: Option<f64>,
    /// Cost per worker-second, for the cost metric.
    pub cost_per_worker_second: Option<f64>,
    /// Slots a single worker may register in this pool (default `limits.max_slots_per_worker`,
    /// never above `max`).
    pub max_slots_per_worker: Option<u32>,
    /// Share the pool fairly between tenants (implied by `tenant_weights`).
    #[serde(default)]
    pub fair_share: bool,
    /// Weight of each tenant in the fair share (others weigh 1).
    #[serde(default)]
    pub tenant_weights: std::collections::BTreeMap<String, f64>,
}

fn default_max() -> u32 {
    10
}
fn default_true() -> bool {
    true
}
fn default_lease_ttl() -> f64 {
    30.0
}
fn default_overflow() -> Overflow {
    Overflow::Reject
}

impl PoolSection {
    /// A pool with default settings.
    pub fn named(name: &str) -> Self {
        PoolSection {
            name: name.into(),
            min: 0,
            max: default_max(),
            autoscale: true,
            scaler: ScalerSection::External,
            lease_ttl_s: default_lease_ttl(),
            max_pending: None,
            overflow: Overflow::Reject,
            retry: None,
            default_duration_s: None,
            reservable_fraction: None,
            await_unknown_dependencies_s: None,
            target_utilization: None,
            drain_time_s: None,
            cooldown_s: None,
            cost_per_worker_second: None,
            max_slots_per_worker: None,
            fair_share: false,
            tenant_weights: Default::default(),
        }
    }

    /// Slots one worker may register in this pool.
    pub fn slot_cap(&self, limits: &LimitsConfig) -> u32 {
        self.max_slots_per_worker
            .unwrap_or(limits.max_slots_per_worker)
            .min(limits.max_slots_per_worker)
            .min(self.max)
            .max(1)
    }

    /// Engine configuration. Assumes [`Config::validate`] passed.
    pub fn to_pool_config(&self, limits: &LimitsConfig) -> PoolConfig {
        let mut c = PoolConfig::new(self.name.as_str()).workers(self.min, 0, self.max);
        c.min_workers = 0; // capacity comes from connected workers
        c.initial_workers = 0;
        c.lease_ttl = Some(Duration::from_secs_f64(self.lease_ttl_s));
        c.bounds.max_pending = Some(self.max_pending.unwrap_or(limits.max_pending_per_pool));
        c.overflow = self.overflow.into();
        if let Some(r) = &self.retry {
            c.retry = r.to_policy();
        }
        if let Some(d) = self.default_duration_s {
            c.default_duration = Duration::from_secs_f64(d);
        }
        if let Some(f) = self.reservable_fraction {
            c.reservable_fraction = f;
        }
        if let Some(t) = self.await_unknown_dependencies_s {
            c.unknown_dependency = cronoq::UnknownDependency::Await {
                timeout: Some(Duration::from_secs_f64(t)),
            };
        }
        if self.fair_share || !self.tenant_weights.is_empty() {
            c.fairness = Some(cronoq::Fairness::weights(
                self.tenant_weights.iter().map(|(t, w)| (t.as_str(), *w)),
            ));
        }
        c.autoscale = self.autoscale.then(|| {
            let mut a = AutoscaleConfig::default();
            if let Some(v) = self.target_utilization {
                a.target_utilization = v;
            }
            if let Some(v) = self.drain_time_s {
                a.drain_time = Duration::from_secs_f64(v);
            }
            if let Some(v) = self.cooldown_s {
                a.cooldown = Duration::from_secs_f64(v);
            }
            if let Some(v) = self.cost_per_worker_second {
                a.cost_per_worker_second = v;
            }
            a
        });
        c
    }
}

/// Behaviour when a pool's pending bound is reached.
#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overflow {
    /// Hold the request until there is room (bounded by the request timeout).
    Wait,
    /// Reject with `RESOURCE_EXHAUSTED` / HTTP 429.
    Reject,
    /// Evict the newest pending task of the lowest lower class.
    ShedLowest,
    /// Evict the oldest pending low-priority task.
    ShedOldest,
}

impl From<Overflow> for OverflowPolicy {
    fn from(o: Overflow) -> Self {
        match o {
            Overflow::Wait => OverflowPolicy::Wait,
            Overflow::Reject => OverflowPolicy::Reject,
            Overflow::ShedLowest => OverflowPolicy::ShedLowest,
            Overflow::ShedOldest => OverflowPolicy::ShedOldest,
        }
    }
}

/// Retry policy in seconds.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySection {
    /// Attempts including the first.
    pub max_attempts: u32,
    /// First backoff.
    #[serde(default = "one")]
    pub initial_backoff_s: f64,
    /// Backoff growth factor (at least 1).
    #[serde(default = "two")]
    pub multiplier: f64,
    /// Backoff ceiling.
    #[serde(default = "sixty")]
    pub max_backoff_s: f64,
    /// Full jitter.
    #[serde(default = "default_true")]
    pub jitter: bool,
}

fn one() -> f64 {
    1.0
}
fn two() -> f64 {
    2.0
}
fn sixty() -> f64 {
    60.0
}

impl RetrySection {
    fn validate(&self, at: &str) -> anyhow::Result<()> {
        ensure!(
            self.max_attempts >= 1,
            "{at}.max_attempts must be at least 1"
        );
        finite_secs(
            &format!("{at}.initial_backoff_s"),
            self.initial_backoff_s,
            0.0,
        )?;
        finite_secs(&format!("{at}.max_backoff_s"), self.max_backoff_s, 0.0)?;
        ensure!(
            self.multiplier.is_finite() && (1.0..=1000.0).contains(&self.multiplier),
            "{at}.multiplier must be between 1 and 1000"
        );
        Ok(())
    }

    /// Engine retry policy. Assumes validation passed.
    pub fn to_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts.max(1),
            initial_backoff: Duration::from_secs_f64(self.initial_backoff_s),
            multiplier: self.multiplier,
            max_backoff: Duration::from_secs_f64(self.max_backoff_s),
            jitter: self.jitter,
        }
    }
}

/// A shared resource.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSection {
    /// At most `permits` units held at once.
    Semaphore {
        /// Resource name.
        name: String,
        /// Concurrent units.
        permits: u32,
    },
    /// Units refill at `rate` per second up to `burst`.
    TokenBucket {
        /// Resource name.
        name: String,
        /// Refill rate per second.
        rate: f64,
        /// Bucket size.
        burst: u32,
    },
}

impl ResourceSection {
    /// The resource name.
    pub fn name(&self) -> &str {
        match self {
            ResourceSection::Semaphore { name, .. } | ResourceSection::TokenBucket { name, .. } => {
                name
            }
        }
    }

    /// Engine configuration.
    pub fn to_config(&self) -> ResourceConfig {
        match self {
            ResourceSection::Semaphore { name, permits } => {
                ResourceConfig::semaphore(name.as_str(), *permits)
            }
            ResourceSection::TokenBucket { name, rate, burst } => {
                ResourceConfig::token_bucket(name.as_str(), *rate, *burst)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_layouts_are_checked() {
        const POOLS: &str = "[[pools]]\nname = \"default\"\n[[pools]]\nname = \"llm\"\n";
        let shards = |a: &str, b: &str| {
            format!(
                "[[shards]]\nname = \"cpu\"\nnumber = 1\npools = [{a}]\nendpoint = \"http://cpu:7070\"\n\
                 [[shards]]\nname = \"gpu\"\nnumber = 2\npools = [{b}]\nendpoint = \"http://gpu:7070\"\n"
            )
        };
        let ok = shards("\"default\"", "\"llm\"");
        let cfg = |server: &str, store: &str, sh: &str| {
            let mut c: Config = toml::from_str(&format!("{server}{store}{POOLS}{sh}")).unwrap();
            if c.server.shard.is_none() {
                c.server.shard = Some("gpu".into());
            }
            c
        };
        let good = cfg("", "", &ok);
        good.validate().unwrap();
        let own = good.for_own_shard();
        assert_eq!(own.pools.len(), 1);
        assert_eq!(own.pools[0].name, "llm");
        assert_eq!(good.shard_plan().unwrap().own.number, 2);

        // Store namespaces are filled in per shard.
        let pg = cfg("", "[store]\nkind = \"postgres\"\nurl_env = \"X\"\n", &ok).for_own_shard();
        assert!(
            matches!(pg.store, StoreConfig::Postgres { schema: Some(ref s), .. } if s == "cronoq_gpu")
        );
        let pg = cfg(
            "",
            "[store]\nkind = \"postgres\"\nurl_env = \"X\"\nschema = \"q_{shard}\"\n",
            &ok,
        )
        .for_own_shard();
        assert!(
            matches!(pg.store, StoreConfig::Postgres { schema: Some(ref s), .. } if s == "q_gpu")
        );
        let lite = cfg(
            "",
            "[store]\nkind = \"sqlite\"\npath = \"/d/{shard}.db\"\n",
            &ok,
        )
        .for_own_shard();
        assert!(
            matches!(lite.store, StoreConfig::Sqlite { ref path, .. } if path.to_str() == Some("/d/gpu.db"))
        );

        let extra_pool = "[[pools]]\nname = \"extra\"\n";
        for (c, want) in [
            (
                cfg("", "", &shards("\"default\"", "\"llm\", \"nope\"")),
                "unknown pool 'nope'",
            ),
            (
                cfg("", "", &shards("\"default\", \"llm\"", "\"llm\"")),
                "is in shards",
            ),
            (cfg("", extra_pool, &ok), "pool 'extra' is in no shard"),
            (
                cfg("", "", &shards("\"default\", \"llm\"", "")),
                "has no pools",
            ),
            (
                cfg("", "", &ok.replace("number = 2", "number = 1")),
                "share number",
            ),
            (
                cfg("", "", &ok.replace("http://gpu:7070", "http://cpu:7070")),
                "repeats the endpoint",
            ),
            (
                cfg("", "", &ok.replace("http://gpu:7070", "gpu:7070")),
                "must be an http",
            ),
            (cfg("", "", &ok.replace("\"gpu\"", "\"GPU\"")), "is invalid"),
            (
                cfg("[server]\nshard = \"tpu\"\n", "", &ok),
                "not one of the configured",
            ),
            (
                cfg(
                    "",
                    "[store]\nkind = \"postgres\"\nurl_env = \"X\"\nschema = \"one\"\n",
                    &ok,
                ),
                "store.schema must contain {shard}",
            ),
            (
                cfg(
                    "",
                    "[store]\nkind = \"sqlite\"\npath = \"/d/one.db\"\n",
                    &ok,
                ),
                "store.path must contain {shard}",
            ),
        ] {
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(want), "{e:?} lacks {want:?}");
        }
        let mut no_shard = cfg("", "", &ok);
        no_shard.server.shard = None;
        assert!(
            no_shard
                .validate()
                .unwrap_err()
                .to_string()
                .contains("--shard")
        );
        let mut stray: Config =
            toml::from_str(&format!("[server]\nshard = \"x\"\n{POOLS}")).unwrap();
        assert!(stray.validate().is_err());
        stray.server.shard = None;
        stray.validate().unwrap();
        assert_eq!(stray.for_own_shard().pools.len(), 2);
    }

    #[test]
    fn grpc_web_origins_must_be_exact() {
        for ok in [
            "https://dash.example.com",
            "http://localhost:3000",
            "https://[::1]:8443",
            "http://127.0.0.1",
        ] {
            assert!(valid_origin(ok), "{ok}");
        }
        for bad in [
            "*",
            "https://*.example.com",
            "https://dash.example.com/",
            "https://dash.example.com/app",
            "dash.example.com",
            "ftp://dash.example.com",
            "https://",
            "https://host:",
            "https://host:99999",
            "https://[nope]:1",
            "https://a b",
        ] {
            assert!(!valid_origin(bad), "{bad}");
        }
        let base = "[[pools]]\nname = \"default\"\n[server.grpc_web]\n";
        assert!(Config::parse(&format!("{base}allowed_origins = []\n")).is_err());
        assert!(Config::parse(&format!("{base}allowed_origins = [\"*\"]\n")).is_err());
        let c = Config::parse(&format!(
            "{base}allowed_origins = [\"https://x.example\"]\n"
        ))
        .unwrap();
        assert_eq!(
            c.server.grpc_web.unwrap().allowed_origins,
            ["https://x.example"]
        );
    }

    #[test]
    fn parses_full_config() {
        let cfg = Config::parse(
            r#"
            [server]
            grpc_addr = "127.0.0.1:9000"
            [[auth.tokens]]
            token = "abcdefghijklmnopqrstuvwxyz012345"
            role = "client"
            pools = ["llm"]
            [[auth.tokens]]
            token_env = "WORKER_TOKEN"
            role = "worker"
            [store]
            kind = "postgres"
            url_env = "DB"
            [limits]
            max_tasks_per_batch = 50
            [[pools]]
            name = "llm"
            max = 20
            lease_ttl_s = 10
            overflow = "shed_lowest"
            scaler = { kind = "webhook", url = "http://scaler/scale" }
            retry = { max_attempts = 5 }
            [[resources]]
            kind = "token_bucket"
            name = "openai"
            rate = 50
            burst = 100
            "#,
        )
        .unwrap();
        assert_eq!(cfg.pools[0].name, "llm");
        assert!(matches!(cfg.pools[0].scaler, ScalerSection::Webhook { .. }));
        assert_eq!(
            cfg.pools[0].to_pool_config(&cfg.limits).retry.max_attempts,
            5
        );
        assert_eq!(
            cfg.pools[0].to_pool_config(&cfg.limits).bounds.max_pending,
            Some(100_000)
        );
        assert!(matches!(cfg.store, StoreConfig::Postgres { .. }));
        assert_eq!(cfg.auth.tokens.len(), 2);
        assert_eq!(cfg.limits.max_tasks_per_batch, 50);
        assert!(Config::parse("[[pools]]\nname='x'\nmin=5\nmax=1").is_err());
        assert!(Config::parse("bogus = 1").is_err());
    }

    #[test]
    fn defaults_are_secure() {
        let c = Config::default();
        assert!(c.server.grpc_addr.ip().is_loopback());
        assert!(c.server.http_addr.ip().is_loopback());
        assert!(
            c.check_auth_exposure(false).is_ok(),
            "loopback without auth is fine"
        );
        let mut public = c.clone();
        public.server.http_addr = "0.0.0.0:7071".parse().unwrap();
        assert!(public.check_auth_exposure(false).is_err());
        assert!(public.check_auth_exposure(true).is_ok());
        public.server.insecure_no_auth = true;
        assert!(public.check_auth_exposure(false).is_ok());
    }

    /// Every one of these used to parse and then panic (or misbehave) at runtime.
    #[test]
    fn rejects_invalid_values() {
        let cases = [
            (
                "negative backoff",
                "[[pools]]\nname='a'\nretry = { max_attempts = 3, initial_backoff_s = -1 }",
            ),
            (
                "huge multiplier",
                "[[pools]]\nname='a'\nretry = { max_attempts = 3, multiplier = 1e10 }",
            ),
            (
                "negative default_duration",
                "[[pools]]\nname='a'\ndefault_duration_s = -1",
            ),
            ("nan drain", "[[pools]]\nname='a'\ndrain_time_s = nan"),
            (
                "negative await deps",
                "[[pools]]\nname='a'\nawait_unknown_dependencies_s = -5",
            ),
            ("inf lease ttl", "[[pools]]\nname='a'\nlease_ttl_s = inf"),
            ("dup pools", "[[pools]]\nname='a'\n[[pools]]\nname='a'"),
            ("empty pool name", "[[pools]]\nname=''"),
            ("odd pool name", "[[pools]]\nname='we\"ird'"),
            ("max 0", "[[pools]]\nname='a'\nmax = 0"),
            (
                "reservable 7",
                "[[pools]]\nname='a'\nreservable_fraction = 7.0",
            ),
            (
                "dup resource",
                "[[resources]]\nkind='semaphore'\nname='r'\npermits=1\n[[resources]]\nkind='semaphore'\nname='r'\npermits=2",
            ),
            (
                "zero-rate bucket",
                "[[resources]]\nkind='token_bucket'\nname='r'\nrate=0\nburst=0",
            ),
            (
                "dup token",
                "[[auth.tokens]]\ntoken='tttttttttttttttttt'\nrole='admin'\n[[auth.tokens]]\ntoken='tttttttttttttttttt'\nrole='client'",
            ),
            (
                "token pool unknown",
                "[[auth.tokens]]\ntoken='tttttttttttttttttt'\nrole='client'\npools=['nope']",
            ),
            ("grace 1e30", "[server]\nshutdown_grace_s = 1e30"),
            ("max wait nan", "[limits]\nmax_wait_s = nan"),
            (
                "certs without mtls",
                "[[auth.certs]]\ndns_name='w'\nrole='worker'",
            ),
        ];
        for (name, toml) in cases {
            assert!(Config::parse(toml).is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn debug_redacts_secrets() {
        let c = Config::parse(
            "[[auth.tokens]]\ntoken='supersecretsupersecret'\nrole='admin'\n[store]\nkind='postgres'\nurl='postgres://u:pw@h/db'",
        )
        .unwrap();
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret") && !s.contains("pw@h"), "{s}");
    }
}
