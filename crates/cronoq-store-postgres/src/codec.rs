//! Conversions between engine types and their stored representation.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cronoq::TimeBase;
use cronoq_core::{
    Dep, EdgeCond, FixedStart, MissPolicy, OverlapPolicy, Priority, RecurringSpec, RetryPolicy,
    TaskId, TaskRef, TaskSpec, Time,
};
use serde::{Deserialize, Serialize};

pub(crate) type Tag = (Arc<str>, Arc<str>);

/// Engine time to wall-clock Unix nanoseconds.
#[inline]
pub(crate) fn to_unix(base: TimeBase, t: Time) -> i64 {
    base.unix_nanos_at_zero
        .saturating_add(t.0.min(i64::MAX as u64) as i64)
}

/// Wall-clock Unix nanoseconds to engine time (clamped at the engine epoch).
#[inline]
pub(crate) fn from_unix(base: TimeBase, unix: i64) -> Time {
    Time(unix.saturating_sub(base.unix_nanos_at_zero).max(0) as u64)
}

#[inline]
pub(crate) fn dur_ns(d: Duration) -> i64 {
    d.as_nanos().min(i64::MAX as u128) as i64
}

#[inline]
pub(crate) fn ns_dur(ns: i64) -> Duration {
    Duration::from_nanos(ns.max(0) as u64)
}

pub(crate) fn miss_name(p: MissPolicy) -> &'static str {
    match p {
        MissPolicy::RunLate => "run_late",
        MissPolicy::Skip => "skip",
        MissPolicy::Fail => "fail",
    }
}

pub(crate) fn miss_from(s: &str) -> MissPolicy {
    match s {
        "skip" => MissPolicy::Skip,
        "fail" => MissPolicy::Fail,
        _ => MissPolicy::RunLate,
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct DepJ {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    cond: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct RetryJ {
    max_attempts: u32,
    initial_backoff_ns: u64,
    multiplier: f64,
    max_backoff_ns: u64,
    jitter: bool,
}

impl From<&RetryPolicy> for RetryJ {
    fn from(r: &RetryPolicy) -> Self {
        RetryJ {
            max_attempts: r.max_attempts,
            initial_backoff_ns: r.initial_backoff.as_nanos() as u64,
            multiplier: r.multiplier,
            max_backoff_ns: r.max_backoff.as_nanos() as u64,
            jitter: r.jitter,
        }
    }
}

impl From<RetryJ> for RetryPolicy {
    fn from(r: RetryJ) -> Self {
        RetryPolicy {
            max_attempts: r.max_attempts.max(1),
            initial_backoff: Duration::from_nanos(r.initial_backoff_ns),
            multiplier: r.multiplier,
            max_backoff: Duration::from_nanos(r.max_backoff_ns),
            jitter: r.jitter,
        }
    }
}

pub(crate) fn deps_json(deps: &[Dep]) -> String {
    let v: Vec<DepJ> = deps
        .iter()
        .map(|d| {
            let (id, key) = match &d.target {
                TaskRef::Id(id) => (Some(id.0), None),
                TaskRef::Key(k) => (None, Some(k.to_string())),
            };
            let cond = match d.cond {
                EdgeCond::OnSuccess => "on_success",
                EdgeCond::OnComplete => "on_complete",
                EdgeCond::OnFailure => "on_failure",
            };
            DepJ {
                id,
                key,
                cond: cond.into(),
            }
        })
        .collect();
    serde_json::to_string(&v).expect("serializable")
}

pub(crate) fn deps_from(v: serde_json::Value) -> Result<Vec<Dep>, String> {
    let deps: Vec<DepJ> = serde_json::from_value(v).map_err(|e| e.to_string())?;
    deps.into_iter()
        .map(|d| {
            let target = match (d.id, d.key) {
                (Some(id), _) => TaskRef::Id(TaskId(id)),
                (None, Some(k)) => TaskRef::Key(k.into()),
                (None, None) => return Err("dependency without target".to_string()),
            };
            let cond = match d.cond.as_str() {
                "on_complete" => EdgeCond::OnComplete,
                "on_failure" => EdgeCond::OnFailure,
                _ => EdgeCond::OnSuccess,
            };
            Ok(Dep { target, cond })
        })
        .collect()
}

pub(crate) fn retry_json(r: &Option<RetryPolicy>) -> Option<String> {
    r.as_ref()
        .map(|r| serde_json::to_string(&RetryJ::from(r)).expect("serializable"))
}

pub(crate) fn retry_from(v: Option<serde_json::Value>) -> Result<Option<RetryPolicy>, String> {
    v.map(|v| {
        serde_json::from_value::<RetryJ>(v)
            .map(Into::into)
            .map_err(|e| e.to_string())
    })
    .transpose()
}

pub(crate) fn resources_json<P>(spec: &TaskSpec<P>) -> String {
    let v: Vec<(&str, u32)> = spec.resources.iter().map(|(n, a)| (&**n, *a)).collect();
    serde_json::to_string(&v).expect("serializable")
}

pub(crate) fn resources_from(v: serde_json::Value) -> Result<Vec<(Arc<str>, u32)>, String> {
    let r: Vec<(String, u32)> = serde_json::from_value(v).map_err(|e| e.to_string())?;
    Ok(r.into_iter().map(|(n, a)| (n.into(), a)).collect())
}

pub(crate) fn tags_json<P>(spec: &TaskSpec<P>) -> String {
    let v: Vec<(&str, &str)> = spec.tags.iter().map(|(k, v)| (&**k, &**v)).collect();
    serde_json::to_string(&v).expect("serializable")
}

pub(crate) fn tags_from(v: serde_json::Value) -> Result<Vec<Tag>, String> {
    let t: Vec<(String, String)> = serde_json::from_value(v).map_err(|e| e.to_string())?;
    Ok(t.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
}

/// Recurring schedule definition; the template payload is stored separately as bytes.
#[derive(Serialize, Deserialize)]
pub(crate) struct RecurringJ {
    cron: String,
    tz: Option<String>,
    overlap: String,
    tolerance_ns: u64,
    on_miss: String,
    template: TemplateJ,
}

#[derive(Serialize, Deserialize)]
struct TemplateJ {
    kind: String,
    pool: String,
    priority: u8,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    deadline_unix_ns: Option<i64>,
    #[serde(default)]
    duration_hint_ns: Option<u64>,
    #[serde(default)]
    timeout_ns: Option<u64>,
    #[serde(default)]
    retry: Option<RetryJ>,
    deps: serde_json::Value,
    resources: Vec<(String, u32)>,
    tags: Vec<(String, String)>,
}

pub(crate) fn recurring_json(base: TimeBase, r: &RecurringSpec<Bytes>) -> String {
    let t = &r.template;
    let j = RecurringJ {
        cron: r.cron.to_string(),
        tz: r.tz.as_ref().map(|s| s.to_string()),
        overlap: match r.overlap {
            OverlapPolicy::Skip => "skip".into(),
            OverlapPolicy::Allow => "allow".into(),
        },
        tolerance_ns: r.tolerance.as_nanos() as u64,
        on_miss: miss_name(r.on_miss).into(),
        template: TemplateJ {
            kind: t.kind.to_string(),
            pool: t.pool.to_string(),
            priority: t.priority as u8,
            key: t.key.as_ref().map(|k| k.to_string()),
            deadline_unix_ns: t.deadline.map(|d| to_unix(base, d)),
            duration_hint_ns: t.duration_hint.map(|d| d.as_nanos() as u64),
            timeout_ns: t.timeout.map(|d| d.as_nanos() as u64),
            retry: t.retry.as_ref().map(RetryJ::from),
            deps: serde_json::from_str(&deps_json(&t.deps)).expect("valid json"),
            resources: t
                .resources
                .iter()
                .map(|(n, a)| (n.to_string(), *a))
                .collect(),
            tags: t
                .tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        },
    };
    serde_json::to_string(&j).expect("serializable")
}

pub(crate) fn recurring_from(
    base: TimeBase,
    name: String,
    spec: serde_json::Value,
    payload: Vec<u8>,
) -> Result<RecurringSpec<Bytes>, String> {
    let j: RecurringJ = serde_json::from_value(spec).map_err(|e| e.to_string())?;
    let t = j.template;
    let mut template = TaskSpec::new(Bytes::from(payload))
        .kind(t.kind)
        .pool(t.pool)
        .priority(Priority::from_index(t.priority as usize).unwrap_or_default());
    template.key = t.key.map(Into::into);
    template.deadline = t.deadline_unix_ns.map(|u| from_unix(base, u));
    template.duration_hint = t.duration_hint_ns.map(Duration::from_nanos);
    template.timeout = t.timeout_ns.map(Duration::from_nanos);
    template.retry = t.retry.map(Into::into);
    template.deps = deps_from(t.deps)?;
    template.resources = t
        .resources
        .into_iter()
        .map(|(n, a)| (n.into(), a))
        .collect();
    template.tags = t
        .tags
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
    Ok(RecurringSpec {
        name: name.into(),
        cron: j.cron.into(),
        tz: j.tz.map(Into::into),
        template,
        overlap: if j.overlap == "allow" {
            OverlapPolicy::Allow
        } else {
            OverlapPolicy::Skip
        },
        tolerance: Duration::from_nanos(j.tolerance_ns),
        on_miss: miss_from(&j.on_miss),
    })
}

/// Fixed-start columns: (at, tolerance, on_miss).
pub(crate) fn fixed_cols(
    base: TimeBase,
    f: &Option<FixedStart>,
) -> (Option<i64>, Option<i64>, Option<String>) {
    match f {
        Some(f) => (
            Some(to_unix(base, f.at)),
            Some(dur_ns(f.tolerance)),
            Some(miss_name(f.on_miss).into()),
        ),
        None => (None, None, None),
    }
}
