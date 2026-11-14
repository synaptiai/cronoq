//! The JSON a host passes in: the configuration of a scheduler, the options of a task and
//! of an update. Unknown fields are refused, so a misspelt option is an error, not a default.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use cronoq::{KeyConflict, Pool, Priority, ResourceConfig, RetryPolicy, Task, Update};
use cronoq_core::{OverflowPolicy, PriorityConfig, UrgencyConfig};
use serde::Deserialize;

/// Most leases of one pool the host may hold at a time, unless the pool says otherwise.
const DEFAULT_MAX_LEASES: u32 = 256;

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

fn secs(field: &str, v: f64) -> Result<Duration, String> {
    Duration::try_from_secs_f64(v).map_err(|_| format!("{field}: {v} is not a duration"))
}

/// A scheduler's configuration.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default)]
    pub(crate) pools: Vec<PoolJson>,
    #[serde(default)]
    resources: Vec<ResourceJson>,
    priority: Option<PriorityJson>,
    urgency: Option<UrgencyJson>,
    pub(crate) store: Option<StoreJson>,
    events: Option<EventsJson>,
}

/// Which status events subscriptions get.
#[derive(Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
enum EventsJson {
    /// One event when a task ends (the default).
    Final,
    /// Every change of status, and updates and replacements.
    All,
}

/// Where tasks are kept. Without it they are kept in memory.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreJson {
    /// Path of a SQLite file (needs the `sqlite` feature of the library).
    pub(crate) sqlite: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolJson {
    pub(crate) name: String,
    max_leases: Option<u32>,
    max_pending: Option<usize>,
    overflow: Option<OverflowJson>,
    retry: Option<RetryJson>,
    max_releases: Option<u32>,
    lease_ttl_ms: Option<u64>,
    priority: Option<PriorityJson>,
    urgency: Option<UrgencyJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum OverflowJson {
    Reject,
    ShedLowest,
    ShedOldest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryJson {
    max_attempts: Option<u32>,
    initial_backoff_ms: Option<u64>,
    multiplier: Option<f64>,
    max_backoff_ms: Option<u64>,
    jitter: Option<bool>,
}

impl RetryJson {
    fn over(&self, mut r: RetryPolicy) -> RetryPolicy {
        if let Some(v) = self.max_attempts {
            r.max_attempts = v;
        }
        if let Some(v) = self.initial_backoff_ms {
            r.initial_backoff = ms(v);
        }
        if let Some(v) = self.multiplier {
            r.multiplier = v;
        }
        if let Some(v) = self.max_backoff_ms {
            r.max_backoff = ms(v);
        }
        if let Some(v) = self.jitter {
            r.jitter = v;
        }
        r
    }
}

/// Values per class, by name; classes left out keep their default.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PerClass {
    critical: Option<f64>,
    high: Option<f64>,
    normal: Option<f64>,
    low: Option<f64>,
}

impl PerClass {
    fn over(&self, into: &mut [f64; Priority::COUNT]) {
        for (i, v) in [self.critical, self.high, self.normal, self.low]
            .into_iter()
            .enumerate()
        {
            if let Some(v) = v {
                into[i] = v;
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriorityJson {
    base: Option<PerClass>,
    aging_rate: Option<PerClass>,
    age_cap: Option<PerClass>,
    allow_overtaking_high: Option<bool>,
}

impl PriorityJson {
    fn over(&self, mut p: PriorityConfig) -> PriorityConfig {
        if let Some(v) = &self.base {
            v.over(&mut p.base);
        }
        if let Some(v) = &self.aging_rate {
            v.over(&mut p.aging_rate);
        }
        if let Some(v) = &self.age_cap {
            v.over(&mut p.age_cap);
        }
        if let Some(v) = self.allow_overtaking_high {
            p.allow_overtaking_high = v;
        }
        p
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UrgencyJson {
    max: Option<f64>,
    window_s: Option<f64>,
    overdue_rate: Option<f64>,
    overdue_cap: Option<f64>,
}

impl UrgencyJson {
    fn over(&self, mut u: UrgencyConfig) -> Result<UrgencyConfig, String> {
        if let Some(v) = self.max {
            u.max = v;
        }
        if let Some(v) = self.window_s {
            u.window = secs("urgency.window_s", v)?;
        }
        if let Some(v) = self.overdue_rate {
            u.overdue_rate = v;
        }
        if let Some(v) = self.overdue_cap {
            u.overdue_cap = v;
        }
        Ok(u)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ResourceJson {
    /// At most `permits` in use at a time.
    Semaphore { name: String, permits: u32 },
    /// `rate` per second, with bursts of up to `burst`.
    Rate { name: String, rate: f64, burst: u32 },
}

impl Config {
    pub(crate) fn parse(json: &str) -> Result<Config, String> {
        let mut c: Config = if json.trim().is_empty() {
            Config::default()
        } else {
            serde_json::from_str(json).map_err(|e| format!("configuration: {e}"))?
        };
        if c.pools.is_empty() {
            c.pools.push(PoolJson {
                name: "default".into(),
                max_leases: None,
                max_pending: None,
                overflow: None,
                retry: None,
                max_releases: None,
                lease_ttl_ms: None,
                priority: None,
                urgency: None,
            });
        }
        Ok(c)
    }

    /// Whether subscriptions get every change of status, not only the last one.
    pub(crate) fn all_events(&self) -> bool {
        self.events == Some(EventsJson::All)
    }

    pub(crate) fn resources(&self) -> Vec<ResourceConfig> {
        self.resources
            .iter()
            .map(|r| match r {
                ResourceJson::Semaphore { name, permits } => {
                    ResourceConfig::semaphore(name.as_str(), *permits)
                }
                ResourceJson::Rate { name, rate, burst } => {
                    ResourceConfig::token_bucket(name.as_str(), *rate, *burst)
                }
            })
            .collect()
    }

    pub(crate) fn priority(&self) -> Option<PriorityConfig> {
        self.priority
            .as_ref()
            .map(|p| p.over(PriorityConfig::default()))
    }

    pub(crate) fn urgency(&self) -> Result<Option<UrgencyConfig>, String> {
        self.urgency
            .as_ref()
            .map(|u| u.over(UrgencyConfig::default()))
            .transpose()
    }
}

impl PoolJson {
    /// The pool, without its executor. Its capacity starts at zero and follows the host.
    /// Most attempts of the pool the host may hold at a time.
    pub(crate) fn max_leases(&self) -> u32 {
        self.max_leases.unwrap_or(DEFAULT_MAX_LEASES)
    }

    pub(crate) fn pool(
        &self,
        priority: &Option<PriorityConfig>,
        urgency: &Option<UrgencyConfig>,
    ) -> Result<Pool<Bytes, Bytes>, String> {
        let max = self.max_leases();
        if max == 0 {
            return Err(format!(
                "pool '{}': max_leases must be at least 1",
                self.name
            ));
        }
        let pool_priority = self
            .priority
            .as_ref()
            .map(|p| p.over(priority.clone().unwrap_or_default()));
        let pool_urgency = self
            .urgency
            .as_ref()
            .map(|u| u.over(urgency.clone().unwrap_or_default()))
            .transpose()?;
        let pool = Pool::new(self.name.as_str())
            .workers(0, 0, max)
            .no_autoscale()
            .scaler(cronoq::ExternalScaler)
            .configure(|c| {
                c.bounds.max_pending = self.max_pending;
                // A full pool never blocks the calling thread: without a policy it rejects.
                c.overflow = match self.overflow {
                    None | Some(OverflowJson::Reject) => OverflowPolicy::Reject,
                    Some(OverflowJson::ShedLowest) => OverflowPolicy::ShedLowest,
                    Some(OverflowJson::ShedOldest) => OverflowPolicy::ShedOldest,
                };
                if let Some(r) = &self.retry {
                    c.retry = r.over(c.retry);
                }
                if let Some(n) = self.max_releases {
                    c.max_releases = n;
                }
                c.lease_ttl = self.lease_ttl_ms.map(ms);
                if pool_priority.is_some() {
                    c.priority = pool_priority;
                }
                if pool_urgency.is_some() {
                    c.urgency = pool_urgency;
                }
            });
        Ok(pool)
    }
}

/// A class by its name.
pub(crate) fn priority(name: &str) -> Result<Priority, String> {
    match name {
        "critical" => Ok(Priority::Critical),
        "high" => Ok(Priority::High),
        "normal" => Ok(Priority::Normal),
        "low" => Ok(Priority::Low),
        other => Err(format!(
            "unknown priority '{other}' (critical, high, normal or low)"
        )),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConflictJson {
    ReturnExisting,
    Reject,
    ReplaceQueued,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnFailureJson {
    Continue,
    Halt,
}

/// The options of one task. Everything is optional.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskJson {
    pool: Option<String>,
    kind: Option<String>,
    key: Option<String>,
    priority: Option<String>,
    deadline_ms: Option<u64>,
    timeout_ms: Option<u64>,
    retry: Option<RetryJson>,
    #[serde(default)]
    resources: BTreeMap<String, u32>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    ordering_key: Option<String>,
    on_failure: Option<OnFailureJson>,
    on_key_conflict: Option<ConflictJson>,
    /// Up to 32 hexadecimal digits: tasks with one key and different fingerprints conflict.
    fingerprint: Option<String>,
    #[serde(default)]
    renew_position: bool,
    max_releases: Option<u32>,
}

impl TaskJson {
    pub(crate) fn parse(json: Option<&str>) -> Result<TaskJson, String> {
        match json {
            Some(j) if !j.trim().is_empty() => {
                serde_json::from_str(j).map_err(|e| format!("task options: {e}"))
            }
            _ => Ok(TaskJson::default()),
        }
    }

    pub(crate) fn task(self, payload: Bytes) -> Result<Task<Bytes>, String> {
        let mut t = Task::new(payload);
        if let Some(v) = self.pool {
            t = t.pool(v);
        }
        if let Some(v) = self.kind {
            t = t.kind(v);
        }
        if let Some(v) = self.key {
            t = t.key(v);
        }
        if let Some(v) = &self.priority {
            t = t.priority(priority(v)?);
        }
        if let Some(v) = self.deadline_ms {
            t = t.deadline_in(ms(v));
        }
        if let Some(v) = self.timeout_ms {
            t = t.timeout(ms(v));
        }
        if let Some(r) = &self.retry {
            // Fields left out come from the defaults, not from the pool: a task's policy
            // replaces the pool's as a whole.
            t = t.retry(r.over(RetryPolicy::default()));
        }
        for (name, amount) in self.resources {
            t = t.resource(name, amount);
        }
        for (k, v) in self.tags {
            t = t.tag(k, v);
        }
        if let Some(v) = self.ordering_key {
            t = t.ordering_key(v);
        }
        if let Some(OnFailureJson::Halt) = self.on_failure {
            t = t.halt_on_failure();
        }
        if let Some(c) = self.on_key_conflict {
            t = t.on_key_conflict(match c {
                ConflictJson::ReturnExisting => KeyConflict::ReturnExisting,
                ConflictJson::Reject => KeyConflict::Reject,
                ConflictJson::ReplaceQueued => KeyConflict::ReplaceQueued,
            });
        }
        if let Some(f) = &self.fingerprint {
            let ok = !f.is_empty() && f.len() <= 32;
            let v = u128::from_str_radix(f, 16)
                .ok()
                .filter(|_| ok)
                .ok_or_else(|| format!("fingerprint '{f}' is not 1 to 32 hexadecimal digits"))?;
            t = t.fingerprint(v);
        }
        if self.renew_position {
            t = t.renew_position();
        }
        if let Some(n) = self.max_releases {
            t = t.max_releases(n);
        }
        Ok(t)
    }
}

/// What to change on a queued task.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateJson {
    priority: Option<String>,
    deadline_ms: Option<u64>,
    #[serde(default)]
    no_deadline: bool,
    tags: Option<BTreeMap<String, String>>,
}

impl UpdateJson {
    pub(crate) fn parse(json: &str) -> Result<Update, String> {
        let u: UpdateJson = serde_json::from_str(json).map_err(|e| format!("update: {e}"))?;
        if u.no_deadline && u.deadline_ms.is_some() {
            return Err("update: deadline_ms and no_deadline exclude each other".into());
        }
        let mut out = Update::new();
        if let Some(p) = &u.priority {
            out = out.priority(priority(p)?);
        }
        if let Some(d) = u.deadline_ms {
            out = out.deadline_in(ms(d));
        }
        if u.no_deadline {
            out = out.no_deadline();
        }
        if let Some(tags) = u.tags {
            out = out.tags(tags);
        }
        Ok(out)
    }
}
