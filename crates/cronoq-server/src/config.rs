//! Server configuration (TOML file, with environment overrides for secrets).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cronoq::{AutoscaleConfig, OverflowPolicy, PoolConfig, ResourceConfig, RetryPolicy};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub store: StoreConfig,
    #[serde(default = "default_pools")]
    pub pools: Vec<PoolSection>,
    #[serde(default)]
    pub resources: Vec<ResourceSection>,
    #[serde(default)]
    pub outputs: OutputsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig::default(),
            auth: AuthConfig::default(),
            store: StoreConfig::default(),
            pools: default_pools(),
            resources: Vec::new(),
            outputs: OutputsConfig::default(),
        }
    }
}

fn default_pools() -> Vec<PoolSection> {
    vec![PoolSection::named("default")]
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)?;
        Self::parse(&text)
    }
    pub fn parse(text: &str) -> anyhow::Result<Config> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.pools.is_empty(), "at least one pool is required");
        for p in &self.pools {
            anyhow::ensure!(p.min <= p.max, "pool '{}': min > max", p.name);
        }
        for t in &self.auth.tokens {
            anyhow::ensure!(
                t.token.is_some() || t.token_env.is_some(),
                "auth token needs `token` or `token_env`"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_grpc")]
    pub grpc_addr: SocketAddr,
    #[serde(default = "default_http")]
    pub http_addr: SocketAddr,
    pub tls: Option<TlsConfig>,
    /// Grace period for running attempts on shutdown.
    #[serde(default = "default_grace")]
    pub shutdown_grace_s: f64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            grpc_addr: default_grpc(),
            http_addr: default_http(),
            tls: None,
            shutdown_grace_s: default_grace(),
        }
    }
}

fn default_grpc() -> SocketAddr {
    "0.0.0.0:7070".parse().unwrap()
}
fn default_http() -> SocketAddr {
    "0.0.0.0:7071".parse().unwrap()
}
fn default_grace() -> f64 {
    10.0
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// Require client certificates signed by this CA (mTLS).
    pub client_ca: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Client,
    Worker,
    Admin,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub token: Option<String>,
    /// Read the token from this environment variable instead.
    pub token_env: Option<String>,
    pub role: Role,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreConfig {
    #[default]
    Memory,
    Postgres {
        url: Option<String>,
        /// Read the URL from this environment variable (default `DATABASE_URL`).
        url_env: Option<String>,
        /// Delete finished tasks older than this (default 7 days).
        keep_finished_s: Option<f64>,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputsConfig {
    /// Recent task outputs kept in memory for `Wait`/`Get` after completion.
    #[serde(default = "default_cache_entries")]
    pub cache_entries: usize,
    #[serde(default = "default_cache_bytes")]
    pub cache_bytes: usize,
}

impl Default for OutputsConfig {
    fn default() -> Self {
        OutputsConfig {
            cache_entries: default_cache_entries(),
            cache_bytes: default_cache_bytes(),
        }
    }
}

fn default_cache_entries() -> usize {
    10_000
}
fn default_cache_bytes() -> usize {
    64 << 20
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalerSection {
    /// Capacity = slots of connected workers; the desired count is exported as a metric.
    #[default]
    External,
    /// POST `{"pool": ..., "desired": n}` to `url` when the target changes.
    Webhook {
        url: String,
        token_env: Option<String>,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSection {
    pub name: String,
    #[serde(default)]
    pub min: u32,
    #[serde(default = "default_max")]
    pub max: u32,
    #[serde(default = "default_true")]
    pub autoscale: bool,
    #[serde(default)]
    pub scaler: ScalerSection,
    /// Remote workers must heartbeat each lease within this many seconds.
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_s: f64,
    pub max_pending: Option<usize>,
    #[serde(default = "default_overflow")]
    pub overflow: Overflow,
    pub retry: Option<RetrySection>,
    pub default_duration_s: Option<f64>,
    pub reservable_fraction: Option<f64>,
    /// Allow dependencies on keys that do not exist yet (wait up to this many seconds).
    pub await_unknown_dependencies_s: Option<f64>,
    pub target_utilization: Option<f64>,
    pub drain_time_s: Option<f64>,
    pub cooldown_s: Option<f64>,
    pub cost_per_worker_second: Option<f64>,
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
        }
    }

    pub fn to_pool_config(&self) -> PoolConfig {
        let mut c = PoolConfig::new(self.name.as_str()).workers(self.min, 0, self.max);
        c.min_workers = 0; // capacity comes from connected workers
        c.initial_workers = 0;
        c.lease_ttl = Some(Duration::from_secs_f64(self.lease_ttl_s.max(0.1)));
        c.bounds.max_pending = self.max_pending;
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

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overflow {
    Wait,
    Reject,
    ShedLowest,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySection {
    pub max_attempts: u32,
    #[serde(default = "one")]
    pub initial_backoff_s: f64,
    #[serde(default = "two")]
    pub multiplier: f64,
    #[serde(default = "sixty")]
    pub max_backoff_s: f64,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSection {
    Semaphore { name: String, permits: u32 },
    TokenBucket { name: String, rate: f64, burst: u32 },
}

impl ResourceSection {
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
    fn parses_full_config() {
        let cfg = Config::parse(
            r#"
            [server]
            grpc_addr = "127.0.0.1:9000"
            [[auth.tokens]]
            token = "abc"
            role = "client"
            [[auth.tokens]]
            token_env = "WORKER_TOKEN"
            role = "worker"
            [store]
            kind = "postgres"
            url_env = "DB"
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
        assert_eq!(cfg.pools[0].to_pool_config().retry.max_attempts, 5);
        assert!(matches!(cfg.store, StoreConfig::Postgres { .. }));
        assert_eq!(cfg.auth.tokens.len(), 2);
        assert!(Config::parse("[[pools]]\nname='x'\nmin=5\nmax=1").is_err());
        assert!(Config::parse("bogus = 1").is_err());
    }
}
