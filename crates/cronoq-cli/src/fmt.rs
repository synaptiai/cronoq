//! Text and JSON renderings of server messages.
//!
//! JSON output uses the field names of the HTTP API, with ids and nanosecond times as strings
//! so that tools reading numbers as doubles (such as `jq`) do not round them.

use chrono::{DateTime, Utc};
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

/// Lower-case name of a wire status (`"unknown"` if unspecified).
pub fn status(s: i32) -> &'static str {
    match pb::TaskStatus::try_from(s).unwrap_or(pb::TaskStatus::Unspecified) {
        pb::TaskStatus::Unspecified => "unknown",
        pb::TaskStatus::Blocked => "blocked",
        pb::TaskStatus::Scheduled => "scheduled",
        pb::TaskStatus::Ready => "ready",
        pb::TaskStatus::Running => "running",
        pb::TaskStatus::Retrying => "retrying",
        pb::TaskStatus::Succeeded => "succeeded",
        pb::TaskStatus::Failed => "failed",
        pb::TaskStatus::DeadLettered => "dead_lettered",
        pb::TaskStatus::Cancelled => "cancelled",
        pb::TaskStatus::Skipped => "skipped",
        pb::TaskStatus::Missed => "missed",
        pb::TaskStatus::Shed => "shed",
    }
}

/// The wire status for a lower-case name.
pub fn status_value(name: &str) -> Option<pb::TaskStatus> {
    let s = pb::TaskStatus::from_str_name(&format!("TASK_STATUS_{}", name.to_ascii_uppercase()))?;
    (s != pb::TaskStatus::Unspecified).then_some(s)
}

/// Lower-case name of a wire priority (`None` if unspecified).
pub fn priority(p: i32) -> Option<&'static str> {
    match pb::Priority::try_from(p).ok()? {
        pb::Priority::Unspecified => None,
        pb::Priority::Critical => Some("critical"),
        pb::Priority::High => Some("high"),
        pb::Priority::Normal => Some("normal"),
        pb::Priority::Low => Some("low"),
    }
}

/// Lower-case name of a wire overlap policy.
pub fn overlap(o: i32) -> &'static str {
    match pb::Overlap::try_from(o).unwrap_or(pb::Overlap::Unspecified) {
        pb::Overlap::Unspecified | pb::Overlap::Skip => "skip",
        pb::Overlap::Allow => "allow",
        pb::Overlap::Queue => "queue",
        pb::Overlap::QueueOnSuccess => "queue_on_success",
    }
}

/// A Unix-nanosecond time in UTC, to the second.
pub fn time(ns: i64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(ns)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// How long ago (or how far ahead) `ns` is from `now_ns`, in the largest whole unit.
pub fn ago(ns: i64, now_ns: i64) -> String {
    let d = (now_ns - ns) / 1_000_000_000;
    let (n, future) = (d.unsigned_abs(), d < 0);
    let span = match n {
        0..60 => format!("{n}s"),
        60..3600 => format!("{}m", n / 60),
        3600..86_400 => format!("{}h", n / 3600),
        _ => format!("{}d", n / 86_400),
    };
    if future {
        format!("in {span}")
    } else {
        format!("{span} ago")
    }
}

/// Current time as Unix nanoseconds.
pub fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}

/// Unicode bidirectional controls, which can make text read differently from what it is.
fn is_bidi_control(c: char) -> bool {
    matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

/// `s` with control characters (escape sequences, carriage returns, ...) and bidirectional
/// controls shown as `\u{1b}` escapes, so that text from other clients cannot drive the
/// terminal or disguise itself. `keep_lines` keeps newlines and tabs.
pub fn clean(s: &str, keep_lines: bool) -> std::borrow::Cow<'_, str> {
    let bad = |c: char| {
        (c.is_control() && !(keep_lines && (c == '\n' || c == '\t'))) || is_bidi_control(c)
    };
    if !s.chars().any(bad) {
        return s.into();
    }
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        if bad(c) {
            out.push_str(&format!("\\u{{{:x}}}", c as u32));
        } else {
            out.push(c);
        }
    }
    out.into()
}

/// Labels as `k=v,k=v`, sorted.
pub fn labels(l: &std::collections::HashMap<String, String>) -> String {
    let mut v: Vec<String> = l.iter().map(|(k, v)| format!("{k}={v}")).collect();
    v.sort();
    v.join(",")
}

/// Left-aligned columns separated by two spaces; the last column is not padded.
pub fn table(header: &[&str], rows: &[Vec<String>]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|c| clean(c, false).into_owned()).collect())
        .collect();
    let rows = &rows;
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for r in rows {
        for (i, c) in r.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut out = String::new();
        for (i, c) in cells.enumerate() {
            if i > 0 {
                out.push_str("  ");
            }
            out.push_str(c);
            if i + 1 < widths.len() {
                out.extend(std::iter::repeat_n(' ', widths[i] - c.chars().count()));
            }
        }
        out.trim_end().to_string()
    };
    let mut out = line(&mut header.iter().copied());
    for r in rows {
        out.push('\n');
        out.push_str(&line(&mut r.iter().map(String::as_str)));
    }
    out
}

pub fn task_json(i: &pb::TaskInfo) -> Value {
    let mut v = json!({
        "id": i.id.to_string(),
        "status": status(i.status),
        "pool": i.pool,
        "attempt": i.attempt,
        "output_available": i.output_available,
    });
    for (name, s) in [
        ("key", &i.key),
        ("kind", &i.kind),
        ("tenant", &i.tenant),
        ("message", &i.message),
    ] {
        if !s.is_empty() {
            v[name] = s.clone().into();
        }
    }
    if let Some(p) = priority(i.priority) {
        v["priority"] = p.into();
    }
    if i.max_attempts > 0 {
        v["max_attempts"] = i.max_attempts.into();
    }
    for (name, t) in [
        ("created_unix_ns", i.created_unix_ns),
        ("started_unix_ns", i.started_unix_ns),
        ("finished_unix_ns", i.finished_unix_ns),
    ] {
        if let Some(t) = t {
            v[name] = t.to_string().into();
        }
    }
    v
}

pub const TASK_HEADER: [&str; 8] = [
    "ID", "STATUS", "POOL", "KIND", "KEY", "PRIORITY", "ATTEMPT", "AGE",
];

pub fn task_row(i: &pb::TaskInfo, now: i64) -> Vec<String> {
    vec![
        i.id.to_string(),
        status(i.status).to_string(),
        i.pool.clone(),
        i.kind.clone(),
        i.key.clone(),
        priority(i.priority).unwrap_or("").to_string(),
        match i.max_attempts {
            0 => i.attempt.to_string(),
            m => format!("{}/{m}", i.attempt),
        },
        i.created_unix_ns.map(|t| ago(t, now)).unwrap_or_default(),
    ]
}

/// Task details as `field: value` lines.
pub fn task_details(i: &pb::TaskInfo) -> String {
    let mut lines = vec![
        format!("id:        {}", i.id),
        format!("status:    {}", status(i.status)),
        format!("pool:      {}", clean(&i.pool, false)),
    ];
    let mut opt = |name: &str, v: String| {
        if !v.is_empty() {
            lines.push(format!("{:<10} {}", format!("{name}:"), clean(&v, false)));
        }
    };
    opt("key", i.key.clone());
    opt("kind", i.kind.clone());
    opt("tenant", i.tenant.clone());
    opt("priority", priority(i.priority).unwrap_or("").to_string());
    opt(
        "attempt",
        match i.max_attempts {
            0 => i.attempt.to_string(),
            m => format!("{} of {m}", i.attempt),
        },
    );
    opt("created", i.created_unix_ns.map(time).unwrap_or_default());
    opt("started", i.started_unix_ns.map(time).unwrap_or_default());
    opt("finished", i.finished_unix_ns.map(time).unwrap_or_default());
    opt("message", i.message.clone());
    opt(
        "output",
        if i.output_available {
            "available".into()
        } else {
            String::new()
        },
    );
    lines.join("\n")
}

pub fn result_json(r: &pb::TaskResult) -> Value {
    let mut v = json!({
        "id": r.id.to_string(),
        "status": status(r.status),
        "output_available": r.output_available,
    });
    if !r.message.is_empty() {
        v["message"] = r.message.clone().into();
    }
    if let Some(out) = &r.output {
        match serde_json::from_slice::<Value>(out) {
            Ok(j) => v["output"] = j,
            Err(_) => v["output_text"] = String::from_utf8_lossy(out).into_owned().into(),
        }
    }
    v
}

pub fn worker_json(w: &pb::WorkerInfo) -> Value {
    json!({
        "id": w.id.to_string(), "name": w.name, "pool": w.pool, "labels": w.labels,
        "slots": w.slots, "busy": w.busy, "connected_unix_ns": w.connected_unix_ns.to_string(),
    })
}

pub fn schedule_json(s: &pb::RecurringInfo) -> Value {
    let mut v = json!({
        "name": s.name, "cron": s.cron, "pool": s.pool, "kind": s.kind,
        "overlap": overlap(s.overlap),
    });
    if !s.tz.is_empty() {
        v["tz"] = s.tz.clone().into();
    }
    if let Some(t) = s.next_unix_ns {
        v["next_unix_ns"] = t.to_string().into();
    }
    if let Some(id) = s.last_task_id {
        v["last_task_id"] = id.to_string().into();
    }
    v
}

pub fn snapshot_json(s: &pb::SnapshotResponse) -> Value {
    let pools: Vec<Value> = s
        .pools
        .iter()
        .map(|p| {
            json!({
                "name": p.name, "capacity": p.capacity, "busy": p.busy, "held": p.held,
                "desired": p.desired, "pending": p.pending_by_class.iter().sum::<u64>(),
                "pending_by_class": p.pending_by_class, "reservations": p.reservations,
                "dispatched": p.dispatched, "succeeded": p.succeeded, "failed": p.failed,
                "utilization": p.utilization, "wait_p50_s": p.wait_p50_s,
                "wait_p90_s": p.wait_p90_s, "wait_p99_s": p.wait_p99_s,
                "workers_connected": p.workers_connected,
            })
        })
        .collect();
    let t = s.totals.unwrap_or_default();
    json!({
        "pools": pools, "live_tasks": s.live_tasks, "running": s.running, "pending": s.pending,
        "workers_connected": s.workers_connected,
        "totals": {
            "submitted": t.submitted, "dispatched": t.dispatched, "succeeded": t.succeeded,
            "failed": t.failed, "dead_lettered": t.dead_lettered, "cancelled": t.cancelled,
            "retries": t.retries, "timeouts": t.timeouts,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_round_trip() {
        for s in 1..=12 {
            let name = status(s);
            assert_eq!(status_value(name).map(|v| v as i32), Some(s), "{name}");
        }
        assert_eq!(status_value("unspecified"), None);
        assert_eq!(status_value("nope"), None);
    }

    #[test]
    fn tables_align_and_trim() {
        let t = table(
            &["ID", "NAME"],
            &[vec!["1".into(), "a".into()], vec!["123".into(), "".into()]],
        );
        assert_eq!(t, "ID   NAME\n1    a\n123");
        // Control characters in cells are shown, not sent to the terminal.
        let t = table(&["KEY", "N"], &[vec!["\x1b[2Kx\ny".into(), "1".into()]]);
        assert_eq!(t, "KEY               N\n\\u{1b}[2Kx\\u{a}y  1");
    }

    #[test]
    fn control_characters_are_escaped() {
        assert_eq!(clean("plain", false), "plain");
        assert_eq!(clean("a\x1b]52;c;x\x07b", false), "a\\u{1b}]52;c;x\\u{7}b");
        assert_eq!(clean("a\r\u{9b}b", false), "a\\u{d}\\u{9b}b");
        assert_eq!(clean("line\n\tx\x1b", true), "line\n\tx\\u{1b}");
        assert_eq!(clean("job\u{202e}gnp.exe", false), "job\\u{202e}gnp.exe");
        assert_eq!(clean("ok\u{2066}x\u{2069}", true), "ok\\u{2066}x\\u{2069}");
        let details = task_details(&pb::TaskInfo {
            key: "\x1b[1Afake".into(),
            message: "boom\x1b[0m".into(),
            ..Default::default()
        });
        assert!(!details.contains('\x1b'), "{details}");
    }

    #[test]
    fn relative_times() {
        let s = 1_000_000_000;
        assert_eq!(ago(0, 5 * s), "5s ago");
        assert_eq!(ago(0, 125 * s), "2m ago");
        assert_eq!(ago(7200 * s, 0), "in 2h");
        assert_eq!(ago(0, 3 * 86_400 * s), "3d ago");
        assert_eq!(time(0), "1970-01-01T00:00:00Z");
    }
}
