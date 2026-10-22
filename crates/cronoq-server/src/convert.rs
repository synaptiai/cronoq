//! Conversions between protobuf messages and scheduler types, with input validation.
//!
//! Everything a client or worker sends is validated here before it reaches the scheduler:
//! strings must not contain NUL bytes (PostgreSQL cannot store them) and must respect the
//! configured length and count limits.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{
    Clock, Dep, EdgeCond, Error, MissPolicy, Priority, RetryPolicy, Snapshot, StatusEvent,
    SubmitError, Task, TaskId, TaskRef, TaskStatus, When,
};
use cronoq_proto::v1 as pb;
use tonic::Status;

use crate::config::LimitsConfig;

/// Tag carrying the payload content type.
pub const CONTENT_TYPE_TAG: &str = "cronoq.content_type";

/// Longest duration accepted from clients (100 years).
pub const MAX_SECS: f64 = 100.0 * 365.0 * 86400.0;

/// Longest error message accepted from a worker (longer ones are truncated).
pub const MAX_MESSAGE_LEN: usize = 8192;

/// Longest content type.
const MAX_CONTENT_TYPE_LEN: usize = 128;

/// Validate a client-supplied duration in seconds.
pub fn secs(s: f64) -> Result<Duration, Status> {
    if !s.is_finite() || !(0.0..=MAX_SECS).contains(&s) {
        return Err(Status::invalid_argument(format!("invalid duration {s}")));
    }
    Ok(Duration::from_secs_f64(s))
}

/// Validate a client-supplied Unix timestamp in nanoseconds.
pub fn unix(field: &str, ns: i64) -> Result<SystemTime, Status> {
    if ns < 0 {
        return Err(Status::invalid_argument(format!(
            "{field} must not be negative"
        )));
    }
    Ok(UNIX_EPOCH + Duration::from_nanos(ns as u64))
}

/// A wall-clock time as Unix nanoseconds (saturating).
pub fn unix_ns(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Validate a client- or worker-supplied string.
pub fn check_str(field: &str, s: &str, max_len: usize) -> Result<(), Status> {
    if s.len() > max_len {
        return Err(Status::invalid_argument(format!(
            "{field} is longer than {max_len} bytes"
        )));
    }
    if s.contains('\0') {
        return Err(Status::invalid_argument(format!(
            "{field} must not contain NUL characters"
        )));
    }
    Ok(())
}

/// Make a worker-supplied message safe to store and log: NUL bytes are replaced and the text is
/// truncated (on a character boundary) to [`MAX_MESSAGE_LEN`].
pub fn sanitize_message(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| if c == '\0' { '\u{FFFD}' } else { c })
        .collect();
    if out.len() > MAX_MESSAGE_LEN {
        let mut end = MAX_MESSAGE_LEN;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

/// Make a worker-supplied name safe to log: control characters are dropped, length capped.
pub fn sanitize_name(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).take(64).collect()
}

/// Map a wire priority.
pub fn priority(p: i32) -> Result<Priority, Status> {
    Ok(
        match pb::Priority::try_from(p).map_err(|_| Status::invalid_argument("unknown priority"))? {
            pb::Priority::Critical => Priority::Critical,
            pb::Priority::High => Priority::High,
            pb::Priority::Unspecified | pb::Priority::Normal => Priority::Normal,
            pb::Priority::Low => Priority::Low,
        },
    )
}

/// Map an engine priority to the wire.
pub fn priority_to_pb(p: Priority) -> pb::Priority {
    match p {
        Priority::Critical => pb::Priority::Critical,
        Priority::High => pb::Priority::High,
        Priority::Normal => pb::Priority::Normal,
        Priority::Low => pb::Priority::Low,
    }
}

/// Map a wire task reference.
pub fn task_ref(r: Option<pb::TaskRef>, limits: &LimitsConfig) -> Result<TaskRef, Status> {
    match r.and_then(|r| r.target) {
        Some(pb::task_ref::Target::Id(id)) => Ok(TaskRef::Id(TaskId(id))),
        Some(pb::task_ref::Target::Key(k)) if !k.is_empty() => {
            check_str("key", &k, limits.max_key_len)?;
            Ok(TaskRef::Key(k.into()))
        }
        _ => Err(Status::invalid_argument(
            "task reference requires an id or key",
        )),
    }
}

/// Map a wire retry policy (zero fields take the defaults).
pub fn retry(r: &pb::RetryPolicy) -> Result<RetryPolicy, Status> {
    let d = RetryPolicy::default();
    if !r.multiplier.is_finite() || r.multiplier < 0.0 || r.multiplier > 1000.0 {
        return Err(Status::invalid_argument(
            "retry.multiplier must be between 1 and 1000",
        ));
    }
    Ok(RetryPolicy {
        max_attempts: r.max_attempts.max(1),
        initial_backoff: if r.initial_backoff_s != 0.0 {
            secs(r.initial_backoff_s)?
        } else {
            d.initial_backoff
        },
        multiplier: if r.multiplier > 0.0 {
            r.multiplier.max(1.0)
        } else {
            d.multiplier
        },
        max_backoff: if r.max_backoff_s != 0.0 {
            secs(r.max_backoff_s)?
        } else {
            d.max_backoff
        },
        jitter: r.jitter,
    })
}

fn edge_cond(c: i32) -> Result<EdgeCond, Status> {
    Ok(
        match pb::EdgeCond::try_from(c)
            .map_err(|_| Status::invalid_argument("unknown dependency condition"))?
        {
            pb::EdgeCond::Unspecified | pb::EdgeCond::OnSuccess => EdgeCond::OnSuccess,
            pb::EdgeCond::OnComplete => EdgeCond::OnComplete,
            pb::EdgeCond::OnFailure => EdgeCond::OnFailure,
        },
    )
}

fn miss_policy(m: i32) -> Result<MissPolicy, Status> {
    Ok(
        match pb::MissPolicy::try_from(m)
            .map_err(|_| Status::invalid_argument("unknown miss policy"))?
        {
            pb::MissPolicy::Unspecified | pb::MissPolicy::RunLate => MissPolicy::RunLate,
            pb::MissPolicy::Skip => MissPolicy::Skip,
            pb::MissPolicy::Fail => MissPolicy::Fail,
        },
    )
}

/// The pool a wire spec targets (`default` when empty).
pub fn spec_pool(spec: &pb::TaskSpec) -> &str {
    if spec.pool.is_empty() {
        "default"
    } else {
        &spec.pool
    }
}

/// Whether `key` has the form of a recurring occurrence's key, `<prefix>@<UTC RFC 3339 time>`.
pub fn is_occurrence_key(key: &str) -> bool {
    let Some((_, ts)) = key.rsplit_once('@') else {
        return false;
    };
    let b = ts.as_bytes();
    b.len() == 20
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            13 | 16 => *c == b':',
            19 => *c == b'Z',
            _ => c.is_ascii_digit(),
        })
}

/// Build a task from its wire form, enforcing `limits`.
pub fn task(spec: pb::TaskSpec, limits: &LimitsConfig) -> Result<Task<Bytes>, Status> {
    check_str("kind", &spec.kind, limits.max_kind_len)?;
    check_str("pool", &spec.pool, 64)?;
    check_str("key", &spec.key, limits.max_key_len)?;
    if is_occurrence_key(&spec.key) {
        // Reserved, so a client cannot take a future occurrence's key in advance.
        return Err(Status::invalid_argument(
            "keys ending in '@<UTC time>' are reserved for recurring occurrences",
        ));
    }
    check_str("content_type", &spec.content_type, MAX_CONTENT_TYPE_LEN)?;
    if spec.tags.len() > limits.max_tags {
        return Err(Status::invalid_argument(format!(
            "at most {} tags are allowed",
            limits.max_tags
        )));
    }
    if spec.labels.len() > limits.max_tags {
        return Err(Status::invalid_argument(format!(
            "at most {} labels are allowed",
            limits.max_tags
        )));
    }
    if spec.deps.len() > limits.max_deps {
        return Err(Status::invalid_argument(format!(
            "at most {} dependencies are allowed",
            limits.max_deps
        )));
    }
    if spec.resources.len() > limits.max_resources {
        return Err(Status::invalid_argument(format!(
            "at most {} resources are allowed",
            limits.max_resources
        )));
    }
    let mut t = Task::new(Bytes::from(spec.payload)).priority(priority(spec.priority)?);
    if !spec.kind.is_empty() {
        t = t.kind(spec.kind);
    }
    if !spec.pool.is_empty() {
        t = t.pool(spec.pool);
    }
    if !spec.key.is_empty() {
        t = t.key(spec.key);
    }
    match (spec.deadline_in_s, spec.deadline_unix_ns) {
        (Some(_), Some(_)) => {
            return Err(Status::invalid_argument(
                "set deadline_in_s or deadline_unix_ns, not both",
            ));
        }
        (Some(s), None) => t = t.deadline_in(secs(s)?),
        (None, Some(ns)) => t = t.deadline_at(unix("deadline_unix_ns", ns)?),
        (None, None) => {}
    }
    let start = match (spec.start_in_s, spec.start_unix_ns) {
        (Some(_), Some(_)) => {
            return Err(Status::invalid_argument(
                "set start_in_s or start_unix_ns, not both",
            ));
        }
        (Some(s), None) => Some(When::In(secs(s)?)),
        (None, Some(ns)) => Some(When::At(unix("start_unix_ns", ns)?)),
        (None, None) => None,
    };
    let on_miss = miss_policy(spec.on_miss)?;
    if let Some(w) = start {
        t = match w {
            When::In(d) => t.start_in(d),
            When::At(at) => t.start_at(at),
        };
        if let Some(tol) = spec.tolerance_s {
            t = t.start_tolerance(secs(tol)?);
        }
        t = t.on_miss(on_miss);
    } else if spec.tolerance_s.is_some() {
        return Err(Status::invalid_argument(
            "tolerance_s requires a start time",
        ));
    }
    if let Some(d) = spec.duration_hint_s {
        t = t.duration_hint(secs(d)?);
    }
    if let Some(d) = spec.timeout_s {
        t = t.timeout(secs(d)?);
    }
    if let Some(r) = &spec.retry {
        t = t.retry(retry(r)?);
    }
    for d in spec.deps {
        t = t.dep(Dep {
            target: task_ref(d.target, limits)?,
            cond: edge_cond(d.cond)?,
        });
    }
    let mut resources: Vec<_> = spec.resources.into_iter().collect();
    resources.sort();
    for (name, amount) in resources {
        check_str("resource name", &name, 64)?;
        t = t.resource(name, amount);
    }
    let mut tags: Vec<_> = spec.tags.into_iter().collect();
    tags.sort();
    for (k, v) in tags {
        check_str("tag key", &k, limits.max_tag_len)?;
        check_str("tag value", &v, limits.max_tag_len)?;
        if k.starts_with("cronoq.") {
            return Err(Status::invalid_argument(
                "tag keys starting with 'cronoq.' are reserved",
            ));
        }
        t = t.tag(k, v);
    }
    let mut labels: Vec<_> = spec.labels.into_iter().collect();
    labels.sort();
    for (k, v) in labels {
        check_label(&k, &v, limits)?;
        t = t.label(k, v);
    }
    if !spec.content_type.is_empty() {
        t = t.tag(CONTENT_TYPE_TAG, spec.content_type);
    }
    Ok(t)
}

/// Check one worker label: a non-empty name and a value within the tag length limits.
pub fn check_label(name: &str, value: &str, limits: &LimitsConfig) -> Result<(), Status> {
    check_str("label name", name, limits.max_tag_len)?;
    check_str("label value", value, limits.max_tag_len)?;
    if name.is_empty() {
        return Err(Status::invalid_argument("label names must not be empty"));
    }
    Ok(())
}

/// Worker labels from the wire, checked and sorted.
pub fn worker_labels(
    labels: std::collections::HashMap<String, String>,
    limits: &LimitsConfig,
) -> Result<cronoq::Labels, Status> {
    if labels.len() > limits.max_tags {
        return Err(Status::invalid_argument(format!(
            "at most {} labels are allowed",
            limits.max_tags
        )));
    }
    let mut v = Vec::with_capacity(labels.len());
    for (k, val) in labels {
        check_label(&k, &val, limits)?;
        v.push((
            std::sync::Arc::<str>::from(k),
            std::sync::Arc::<str>::from(val),
        ));
    }
    v.sort();
    Ok(v.into())
}

/// Map an engine status to the wire.
pub fn status(s: TaskStatus) -> pb::TaskStatus {
    match s {
        TaskStatus::Blocked => pb::TaskStatus::Blocked,
        TaskStatus::Scheduled => pb::TaskStatus::Scheduled,
        TaskStatus::Ready => pb::TaskStatus::Ready,
        TaskStatus::Running => pb::TaskStatus::Running,
        TaskStatus::Retrying => pb::TaskStatus::Retrying,
        TaskStatus::Succeeded => pb::TaskStatus::Succeeded,
        TaskStatus::Failed => pb::TaskStatus::Failed,
        TaskStatus::DeadLettered => pb::TaskStatus::DeadLettered,
        TaskStatus::Cancelled => pb::TaskStatus::Cancelled,
        TaskStatus::Skipped => pb::TaskStatus::Skipped,
        TaskStatus::Missed => pb::TaskStatus::Missed,
        TaskStatus::Shed => pb::TaskStatus::Shed,
    }
}

/// Map an engine status event to the wire.
pub fn event(e: &StatusEvent, pools: &[Arc<str>], clock: &Clock) -> pb::StatusEvent {
    pb::StatusEvent {
        id: e.id.0,
        pool: pools
            .get(e.pool.0 as usize)
            .map(|p| p.to_string())
            .unwrap_or_default(),
        key: e.key.as_deref().unwrap_or_default().to_string(),
        status: status(e.status) as i32,
        attempt: e.attempt,
        at_unix_ns: unix_ns(clock.to_system(e.at)),
        message: e.message.as_deref().unwrap_or_default().to_string(),
    }
}

/// Map a scheduler snapshot to the wire.
pub fn snapshot(s: &Snapshot, workers: &dyn Fn(&str) -> usize) -> pb::SnapshotResponse {
    let t = &s.totals;
    let pools: Vec<pb::PoolSnapshot> = s
        .pools
        .iter()
        .map(|p| pb::PoolSnapshot {
            name: p.name.to_string(),
            capacity: p.capacity,
            busy: p.busy,
            held: p.held,
            desired: p.desired,
            pending_by_class: p.pending_by_class.iter().map(|v| *v as u64).collect(),
            reservations: p.reservations as u64,
            dispatched: p.dispatched,
            succeeded: p.succeeded,
            failed: p.failed,
            priority_adherence: p.priority_adherence,
            fixed_start_hit_rate: p.fixed_start_hit_rate,
            utilization: p.utilization,
            schedule_efficiency: p.schedule_efficiency,
            wait_p50_s: p.wait_p50.as_secs_f64(),
            wait_p90_s: p.wait_p90.as_secs_f64(),
            wait_p99_s: p.wait_p99.as_secs_f64(),
            cost: p.cost,
            workers_connected: workers(&p.name) as u64,
            groups: p
                .groups
                .iter()
                .map(|g| pb::WorkerGroup {
                    labels: g
                        .labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    capacity: g.capacity,
                    busy: g.busy,
                })
                .collect(),
        })
        .collect();
    pb::SnapshotResponse {
        workers_connected: pools.iter().map(|p| p.workers_connected).sum(),
        pools,
        live_tasks: s.live_tasks as u64,
        running: s.running as u64,
        pending: s.pending as u64,
        totals: Some(pb::Totals {
            submitted: t.submitted,
            dispatched: t.dispatched,
            succeeded: t.succeeded,
            failed: t.failed,
            dead_lettered: t.dead_lettered,
            cancelled: t.cancelled,
            skipped: t.skipped,
            missed: t.missed,
            shed: t.shed,
            retries: t.retries,
            timeouts: t.timeouts,
            lease_expiries: t.lease_expiries,
            stale_completions: t.stale_completions,
        }),
    }
}

/// Map a scheduler error to a gRPC status. Store details are logged, not returned.
pub fn error(e: Error) -> Status {
    match e {
        Error::Submit(e) => submit_error(e),
        Error::Shutdown => Status::unavailable("scheduler is shutting down"),
        Error::Config(m) => Status::invalid_argument(m),
        Error::Store(e) => {
            tracing::warn!(error = %e, "store error");
            Status::unavailable("store unavailable")
        }
        e => Status::internal(e.to_string()),
    }
}

/// Map a submission error to a gRPC status.
pub fn submit_error(e: SubmitError) -> Status {
    let msg = e.to_string();
    match e {
        SubmitError::Full | SubmitError::Overloaded => Status::resource_exhausted(msg),
        SubmitError::ReservationConflict => Status::failed_precondition(msg),
        SubmitError::UnknownPool(_)
        | SubmitError::UnknownResource(_)
        | SubmitError::UnknownDependency(_) => Status::not_found(msg),
        _ => Status::invalid_argument(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim() -> LimitsConfig {
        LimitsConfig::default()
    }

    #[test]
    fn durations_are_validated() {
        for bad in [-1.0, f64::NAN, f64::INFINITY, 1e300] {
            assert!(secs(bad).is_err(), "{bad}");
        }
        assert_eq!(secs(1.5).unwrap(), Duration::from_millis(1500));
    }

    #[test]
    fn task_from_proto() {
        let spec = pb::TaskSpec {
            payload: b"hi".to_vec(),
            content_type: "text/plain".into(),
            kind: "k".into(),
            pool: "p".into(),
            priority: pb::Priority::High as i32,
            key: "key".into(),
            deadline_in_s: Some(5.0),
            start_in_s: Some(10.0),
            tolerance_s: Some(2.0),
            timeout_s: Some(1.5),
            deps: vec![pb::Dep {
                target: Some(pb::TaskRef {
                    target: Some(pb::task_ref::Target::Key("a".into())),
                }),
                cond: pb::EdgeCond::OnFailure as i32,
            }],
            resources: [("gpu".to_string(), 1)].into_iter().collect(),
            ..Default::default()
        };
        let t = task(spec, &lim()).unwrap();
        let s = t.spec();
        assert_eq!(&*s.kind, "k");
        assert_eq!(&*s.pool, "p");
        assert_eq!(s.priority, Priority::High);
        assert_eq!(s.key.as_deref(), Some("key"));
        assert_eq!(s.timeout, Some(Duration::from_millis(1500)));
        assert_eq!(s.deps[0].cond, EdgeCond::OnFailure);
        assert_eq!(s.resources[0].1, 1);
        assert!(
            s.tags
                .iter()
                .any(|(k, v)| &**k == CONTENT_TYPE_TAG && &**v == "text/plain")
        );
    }

    #[test]
    fn rejects_hostile_specs() {
        let bad = [
            pb::TaskSpec {
                timeout_s: Some(-1.0),
                ..Default::default()
            },
            pb::TaskSpec {
                key: "evil\0".into(),
                ..Default::default()
            },
            pb::TaskSpec {
                kind: "k\0".into(),
                ..Default::default()
            },
            pb::TaskSpec {
                pool: "p\0".into(),
                ..Default::default()
            },
            pb::TaskSpec {
                tags: [("a".to_string(), "\0".to_string())].into_iter().collect(),
                ..Default::default()
            },
            pb::TaskSpec {
                resources: [("r\0".to_string(), 1)].into_iter().collect(),
                ..Default::default()
            },
            pb::TaskSpec {
                key: "k".repeat(1000),
                ..Default::default()
            },
            pb::TaskSpec {
                deadline_unix_ns: Some(-5),
                ..Default::default()
            },
            pb::TaskSpec {
                on_miss: 99,
                start_in_s: Some(1.0),
                ..Default::default()
            },
            pb::TaskSpec {
                deps: vec![pb::Dep {
                    target: Some(pb::TaskRef {
                        target: Some(pb::task_ref::Target::Id(1)),
                    }),
                    cond: 42,
                }],
                ..Default::default()
            },
            pb::TaskSpec {
                tags: (0..100).map(|i| (format!("t{i}"), String::new())).collect(),
                ..Default::default()
            },
            pb::TaskSpec {
                tags: [("cronoq.content_type".to_string(), "x".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
            pb::TaskSpec {
                retry: Some(pb::RetryPolicy {
                    max_attempts: 1,
                    multiplier: f64::NAN,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        for (i, s) in bad.into_iter().enumerate() {
            assert!(task(s, &lim()).is_err(), "case {i} should be rejected");
        }
    }

    #[test]
    fn sanitizes_worker_text() {
        assert_eq!(sanitize_message("a\0b"), "a\u{FFFD}b");
        assert!(sanitize_message(&"é".repeat(10_000)).len() <= MAX_MESSAGE_LEN);
        assert_eq!(
            sanitize_name("w\n\x1b[31mERROR admin token leaked"),
            "w[31mERROR admin token leaked"
        );
    }
}
