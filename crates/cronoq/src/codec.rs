//! Conversions between engine types and the representation durable stores keep, shared by
//! the PostgreSQL and SQLite stores so both store tasks the same way.
//!
//! Times are Unix nanoseconds in 64-bit integers (saturating in the year 2262); durations inside
//! JSON documents are exact (`u128` nanoseconds). Strings are stored with NUL characters
//! replaced by U+FFFD, which every backend can hold.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use crate::TimeBase;
use bytes::Bytes;
use cronoq_core::{
    Dep, EdgeCond, FixedStart, MissPolicy, OverlapPolicy, Priority, RecurringSpec, RetryPolicy,
    TaskId, TaskRef, TaskSpec, Time,
};
use serde::{Deserialize, Serialize};

/// A task tag (key, value).
pub type Tag = (Arc<str>, Arc<str>);

/// `s` with NUL characters replaced by U+FFFD.
#[inline]
pub fn clean(s: &str) -> Cow<'_, str> {
    if s.contains('\0') {
        Cow::Owned(s.replace('\0', "\u{FFFD}"))
    } else {
        Cow::Borrowed(s)
    }
}

/// [`clean`], owned.
#[inline]
pub fn cleaned(s: &str) -> String {
    clean(s).into_owned()
}

/// Engine time to wall-clock Unix nanoseconds (saturating).
#[inline]
pub fn to_unix(base: TimeBase, t: Time) -> i64 {
    base.unix_nanos_at_zero
        .saturating_add(i64::try_from(t.0).unwrap_or(i64::MAX))
}

/// Wall-clock Unix nanoseconds to engine time (times before engine zero map to zero).
#[inline]
pub fn from_unix(base: TimeBase, unix: i64) -> Time {
    Time(u64::try_from(unix.saturating_sub(base.unix_nanos_at_zero)).unwrap_or(0))
}

/// Duration as 64-bit nanoseconds (saturating at ~292 years).
#[inline]
pub fn dur_ns(d: Duration) -> i64 {
    d.as_nanos().min(i64::MAX as u128) as i64
}

#[inline]
/// Nanoseconds to a duration (negative values map to zero).
pub fn ns_dur(ns: i64) -> Duration {
    Duration::from_nanos(ns.max(0) as u64)
}

/// Exact nanoseconds of `d`, for JSON documents.
#[inline]
fn dur_ns128(d: Duration) -> u128 {
    d.as_nanos()
}

#[inline]
fn ns128_dur(ns: u128) -> Duration {
    const NS: u128 = 1_000_000_000;
    Duration::new((ns / NS).min(u64::MAX as u128) as u64, (ns % NS) as u32)
}

/// Stored name of a miss policy.
pub fn miss_name(p: MissPolicy) -> &'static str {
    match p {
        MissPolicy::RunLate => "run_late",
        MissPolicy::Skip => "skip",
        MissPolicy::Fail => "fail",
    }
}

/// Stored name of an overlap policy.
pub fn overlap_name(p: OverlapPolicy) -> &'static str {
    match p {
        OverlapPolicy::Skip => "skip",
        OverlapPolicy::Allow => "allow",
        OverlapPolicy::Queue => "queue",
        OverlapPolicy::QueueOnSuccess => "queue_on_success",
    }
}

/// Overlap policy from its stored name (unknown names skip).
pub fn overlap_from(s: &str) -> OverlapPolicy {
    match s {
        "allow" => OverlapPolicy::Allow,
        "queue" => OverlapPolicy::Queue,
        "queue_on_success" => OverlapPolicy::QueueOnSuccess,
        _ => OverlapPolicy::Skip,
    }
}

/// Miss policy from its stored name (unknown names run late).
pub fn miss_from(s: &str) -> MissPolicy {
    match s {
        "skip" => MissPolicy::Skip,
        "fail" => MissPolicy::Fail,
        _ => MissPolicy::RunLate,
    }
}

#[derive(Serialize, Deserialize)]
struct DepJ {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    cond: String,
}

#[derive(Serialize, Deserialize)]
struct RetryJ {
    max_attempts: u32,
    initial_backoff_ns: u128,
    /// `None` (JSON `null`) for a multiplier that JSON cannot represent (non-finite).
    multiplier: Option<f64>,
    max_backoff_ns: u128,
    jitter: bool,
}

/// Multiplier used when the stored one is missing or unusable.
const DEFAULT_MULTIPLIER: f64 = 2.0;

impl From<&RetryPolicy> for RetryJ {
    fn from(r: &RetryPolicy) -> Self {
        RetryJ {
            max_attempts: r.max_attempts,
            initial_backoff_ns: dur_ns128(r.initial_backoff),
            multiplier: Some(r.multiplier).filter(|m| m.is_finite() && *m >= 0.0),
            max_backoff_ns: dur_ns128(r.max_backoff),
            jitter: r.jitter,
        }
    }
}

impl From<RetryJ> for RetryPolicy {
    fn from(r: RetryJ) -> Self {
        RetryPolicy {
            max_attempts: r.max_attempts.max(1),
            initial_backoff: ns128_dur(r.initial_backoff_ns),
            multiplier: r
                .multiplier
                .filter(|m| m.is_finite() && *m >= 0.0)
                .unwrap_or(DEFAULT_MULTIPLIER),
            max_backoff: ns128_dur(r.max_backoff_ns),
            jitter: r.jitter,
        }
    }
}

/// Dependencies as a JSON array.
pub fn deps_json(deps: &[Dep]) -> String {
    let v: Vec<DepJ> = deps
        .iter()
        .map(|d| {
            let (id, key) = match &d.target {
                TaskRef::Id(id) => (Some(id.0), None),
                TaskRef::Key(k) => (None, Some(cleaned(k))),
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

/// Dependencies from their JSON array.
pub fn deps_from(v: &str) -> Result<Vec<Dep>, String> {
    let deps: Vec<DepJ> = serde_json::from_str(v).map_err(|e| e.to_string())?;
    deps_from_j(deps)
}

fn deps_from_j(deps: Vec<DepJ>) -> Result<Vec<Dep>, String> {
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

/// Retry policy as a JSON object.
pub fn retry_json(r: &Option<RetryPolicy>) -> Option<String> {
    r.as_ref()
        .map(|r| serde_json::to_string(&RetryJ::from(r)).expect("serializable"))
}

/// Retry policy from its JSON object.
pub fn retry_from(v: Option<&str>) -> Result<Option<RetryPolicy>, String> {
    v.map(|v| {
        serde_json::from_str::<RetryJ>(v)
            .map(Into::into)
            .map_err(|e| e.to_string())
    })
    .transpose()
}

/// Resource requests as a JSON array of `[name, amount]`.
pub fn resources_json<P>(spec: &TaskSpec<P>) -> String {
    let v: Vec<(Cow<'_, str>, u32)> = spec.resources.iter().map(|(n, a)| (clean(n), *a)).collect();
    serde_json::to_string(&v).expect("serializable")
}

/// Resource requests from their JSON array.
pub fn resources_from(v: &str) -> Result<Vec<(Arc<str>, u32)>, String> {
    let r: Vec<(String, u32)> = serde_json::from_str(v).map_err(|e| e.to_string())?;
    Ok(r.into_iter().map(|(n, a)| (n.into(), a)).collect())
}

/// Tags as a JSON array of `[key, value]`.
pub fn tags_json<P>(spec: &TaskSpec<P>) -> String {
    let v: Vec<(Cow<'_, str>, Cow<'_, str>)> = spec
        .tags
        .iter()
        .map(|(k, v)| (clean(k), clean(v)))
        .collect();
    serde_json::to_string(&v).expect("serializable")
}

/// Tags from their JSON array.
pub fn tags_from(v: &str) -> Result<Vec<Tag>, String> {
    let t: Vec<(String, String)> = serde_json::from_str(v).map_err(|e| e.to_string())?;
    Ok(t.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
}

/// Recurring schedule definition; the template payload is stored separately as bytes.
#[derive(Serialize, Deserialize)]
struct RecurringJ {
    cron: String,
    tz: Option<String>,
    overlap: String,
    tolerance_ns: u128,
    on_miss: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deadline_after_ns: Option<u128>,
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
    duration_hint_ns: Option<u128>,
    #[serde(default)]
    timeout_ns: Option<u128>,
    #[serde(default)]
    retry: Option<RetryJ>,
    deps: Vec<DepJ>,
    resources: Vec<(String, u32)>,
    tags: Vec<(String, String)>,
}

/// A recurring schedule (without its template payload) as a JSON document.
pub fn recurring_json(base: TimeBase, r: &RecurringSpec<Bytes>) -> String {
    let t = &r.template;
    let j = RecurringJ {
        cron: cleaned(&r.cron),
        tz: r.tz.as_deref().map(cleaned),
        overlap: overlap_name(r.overlap).into(),
        tolerance_ns: dur_ns128(r.tolerance),
        on_miss: miss_name(r.on_miss).into(),
        deadline_after_ns: r.deadline_after.map(dur_ns128),
        template: TemplateJ {
            kind: cleaned(&t.kind),
            pool: cleaned(&t.pool),
            priority: t.priority as u8,
            key: t.key.as_deref().map(cleaned),
            deadline_unix_ns: t.deadline.map(|d| to_unix(base, d)),
            duration_hint_ns: t.duration_hint.map(dur_ns128),
            timeout_ns: t.timeout.map(dur_ns128),
            retry: t.retry.as_ref().map(RetryJ::from),
            deps: serde_json::from_str(&deps_json(&t.deps)).expect("valid json"),
            resources: t.resources.iter().map(|(n, a)| (cleaned(n), *a)).collect(),
            tags: t
                .tags
                .iter()
                .map(|(k, v)| (cleaned(k), cleaned(v)))
                .collect(),
        },
    };
    serde_json::to_string(&j).expect("serializable")
}

/// A recurring schedule from its JSON document and template payload.
pub fn recurring_from(
    base: TimeBase,
    name: String,
    spec: &str,
    payload: Vec<u8>,
) -> Result<RecurringSpec<Bytes>, String> {
    let j: RecurringJ = serde_json::from_str(spec).map_err(|e| e.to_string())?;
    let t = j.template;
    let mut template = TaskSpec::new(Bytes::from(payload))
        .kind(t.kind)
        .pool(t.pool)
        .priority(Priority::from_index(t.priority as usize).unwrap_or_default());
    template.key = t.key.map(Into::into);
    template.deadline = t.deadline_unix_ns.map(|u| from_unix(base, u));
    template.duration_hint = t.duration_hint_ns.map(ns128_dur);
    template.timeout = t.timeout_ns.map(ns128_dur);
    template.retry = t.retry.map(Into::into);
    template.deps = deps_from_j(t.deps)?;
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
        overlap: overlap_from(&j.overlap),
        tolerance: ns128_dur(j.tolerance_ns),
        on_miss: miss_from(&j.on_miss),
        deadline_after: j.deadline_after_ns.map(ns128_dur),
    })
}

/// Fixed-start columns: (at, tolerance, on_miss).
pub fn fixed_cols(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_lose_only_nul() {
        assert_eq!(clean("a\0b"), "a\u{FFFD}b");
        assert!(matches!(clean("plain"), Cow::Borrowed(_)));
    }

    #[test]
    fn times_are_identity_and_saturate() {
        let b = TimeBase {
            unix_nanos_at_zero: 0,
        };
        assert_eq!(to_unix(b, Time(123)), 123);
        assert_eq!(from_unix(b, 123), Time(123));
        assert_eq!(to_unix(b, Time(u64::MAX)), i64::MAX);
        assert_eq!(from_unix(b, -5), Time::ZERO);
    }

    #[test]
    fn retry_round_trips_exactly_and_tolerates_bad_multipliers() {
        let big = Duration::from_secs(1 << 40);
        let r = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_nanos(1),
            multiplier: f64::INFINITY,
            max_backoff: big,
            jitter: true,
        };
        let j = retry_json(&Some(r)).unwrap();
        let back = retry_from(Some(&j)).unwrap().unwrap();
        assert_eq!(back.max_backoff, big);
        assert_eq!(back.multiplier, DEFAULT_MULTIPLIER);
        // Rows written before multipliers were sanitized hold `null`.
        let old = r#"{"max_attempts":3,"initial_backoff_ns":1,"multiplier":null,"max_backoff_ns":2,"jitter":false}"#;
        assert_eq!(retry_from(Some(old)).unwrap().unwrap().multiplier, 2.0);
    }
}
