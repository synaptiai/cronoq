//! Conversions between protobuf messages and scheduler types.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{
    Clock, Dep, EdgeCond, Error, MissPolicy, Priority, RetryPolicy, Snapshot, StatusEvent,
    SubmitError, Task, TaskId, TaskRef, TaskStatus, When,
};
use cronoq_proto::v1 as pb;
use tonic::Status;

/// Tag carrying the payload content type.
pub const CONTENT_TYPE_TAG: &str = "cronoq.content_type";

fn secs(s: f64) -> Result<Duration, Status> {
    if !s.is_finite() || s < 0.0 {
        return Err(Status::invalid_argument(format!("invalid duration {s}")));
    }
    Ok(Duration::from_secs_f64(s))
}

fn unix(ns: i64) -> SystemTime {
    if ns >= 0 {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    } else {
        UNIX_EPOCH
    }
}

pub fn unix_ns(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

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

pub fn task_ref(r: Option<pb::TaskRef>) -> Result<TaskRef, Status> {
    match r.and_then(|r| r.target) {
        Some(pb::task_ref::Target::Id(id)) => Ok(TaskRef::Id(TaskId(id))),
        Some(pb::task_ref::Target::Key(k)) if !k.is_empty() => Ok(TaskRef::Key(k.into())),
        _ => Err(Status::invalid_argument(
            "task reference requires an id or key",
        )),
    }
}

pub fn retry(r: &pb::RetryPolicy) -> Result<RetryPolicy, Status> {
    let d = RetryPolicy::default();
    Ok(RetryPolicy {
        max_attempts: r.max_attempts.max(1),
        initial_backoff: if r.initial_backoff_s > 0.0 {
            secs(r.initial_backoff_s)?
        } else {
            d.initial_backoff
        },
        multiplier: if r.multiplier > 0.0 {
            r.multiplier
        } else {
            d.multiplier
        },
        max_backoff: if r.max_backoff_s > 0.0 {
            secs(r.max_backoff_s)?
        } else {
            d.max_backoff
        },
        jitter: r.jitter,
    })
}

/// Build a task from its wire form.
pub fn task(spec: pb::TaskSpec) -> Result<Task<Bytes>, Status> {
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
        (Some(s), _) => t = t.deadline_in(secs(s)?),
        (None, Some(ns)) => t = t.deadline_at(unix(ns)),
        _ => {}
    }
    let start = match (spec.start_in_s, spec.start_unix_ns) {
        (Some(s), _) => Some(When::In(secs(s)?)),
        (None, Some(ns)) => Some(When::At(unix(ns))),
        _ => None,
    };
    if let Some(w) = start {
        t = match w {
            When::In(d) => t.start_in(d),
            When::At(at) => t.start_at(at),
        };
        if let Some(tol) = spec.tolerance_s {
            t = t.start_tolerance(secs(tol)?);
        }
        t = t.on_miss(
            match pb::MissPolicy::try_from(spec.on_miss).unwrap_or(pb::MissPolicy::RunLate) {
                pb::MissPolicy::RunLate => MissPolicy::RunLate,
                pb::MissPolicy::Skip => MissPolicy::Skip,
                pb::MissPolicy::Fail => MissPolicy::Fail,
            },
        );
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
        let cond = match pb::EdgeCond::try_from(d.cond).unwrap_or(pb::EdgeCond::OnSuccess) {
            pb::EdgeCond::OnSuccess => EdgeCond::OnSuccess,
            pb::EdgeCond::OnComplete => EdgeCond::OnComplete,
            pb::EdgeCond::OnFailure => EdgeCond::OnFailure,
        };
        t = t.dep(Dep {
            target: task_ref(d.target)?,
            cond,
        });
    }
    let mut resources: Vec<_> = spec.resources.into_iter().collect();
    resources.sort();
    for (name, amount) in resources {
        t = t.resource(name, amount);
    }
    let mut tags: Vec<_> = spec.tags.into_iter().collect();
    tags.sort();
    for (k, v) in tags {
        t = t.tag(k, v);
    }
    if !spec.content_type.is_empty() {
        t = t.tag(CONTENT_TYPE_TAG, spec.content_type);
    }
    Ok(t)
}

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

pub fn snapshot(s: &Snapshot) -> pb::SnapshotResponse {
    pb::SnapshotResponse {
        pools: s
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
            })
            .collect(),
        live_tasks: s.live_tasks as u64,
        running: s.running as u64,
        pending: s.pending as u64,
    }
}

pub fn error(e: Error) -> Status {
    match e {
        Error::Submit(e) => submit_error(e),
        Error::Shutdown => Status::unavailable("scheduler is shutting down"),
        Error::Config(m) => Status::invalid_argument(m),
        Error::Store(e) => Status::unavailable(format!("store: {e}")),
    }
}

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
        let t = task(spec).unwrap();
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
        assert!(
            task(pb::TaskSpec {
                timeout_s: Some(-1.0),
                ..Default::default()
            })
            .is_err()
        );
    }
}
