//! Prometheus text exposition built from the scheduler snapshot.

use std::fmt::Write;

use cronoq_proto::v1 as pb;

/// Content type of [`render`]'s output.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

const CLASSES: [&str; 4] = ["critical", "high", "normal", "low"];

/// Escape a label value (`\`, `"` and newlines).
pub fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// Format a sample value (`+Inf`, `-Inf` and `NaN` as Prometheus expects).
fn value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "+Inf".into()
    } else if v == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        format!("{v}")
    }
}

struct Out(String);

impl Out {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], v: f64) {
        self.0.push_str(name);
        if !labels.is_empty() {
            self.0.push('{');
            for (i, (k, val)) in labels.iter().enumerate() {
                if i > 0 {
                    self.0.push(',');
                }
                let _ = write!(self.0, "{k}=\"{}\"", escape_label(val));
            }
            self.0.push('}');
        }
        let _ = writeln!(self.0, " {}", value(v));
    }

    fn per_pool(
        &mut self,
        s: &pb::SnapshotResponse,
        name: &str,
        kind: &str,
        help: &str,
        f: impl Fn(&pb::PoolSnapshot) -> f64,
    ) {
        self.family(name, kind, help);
        for p in &s.pools {
            self.sample(name, &[("pool", &p.name)], f(p));
        }
    }
}

/// Render the snapshot in the Prometheus text format.
pub fn render(s: &pb::SnapshotResponse) -> String {
    let mut o = Out(String::with_capacity(4096));
    o.per_pool(
        s,
        "cronoq_pool_workers",
        "gauge",
        "Current worker capacity (slots)",
        |p| p.capacity as f64,
    );
    o.per_pool(
        s,
        "cronoq_pool_desired_workers",
        "gauge",
        "Autoscaler target capacity",
        |p| p.desired as f64,
    );
    o.per_pool(
        s,
        "cronoq_pool_busy_workers",
        "gauge",
        "Slots running tasks",
        |p| p.busy as f64,
    );
    o.per_pool(
        s,
        "cronoq_pool_held_workers",
        "gauge",
        "Slots held for fixed-start tasks",
        |p| p.held as f64,
    );
    o.per_pool(
        s,
        "cronoq_pool_workers_connected",
        "gauge",
        "Connected remote workers",
        |p| p.workers_connected as f64,
    );
    o.family("cronoq_pool_pending", "gauge", "Tasks waiting to run");
    for p in &s.pools {
        for (i, class) in CLASSES.iter().enumerate() {
            let v = p.pending_by_class.get(i).copied().unwrap_or(0);
            o.sample(
                "cronoq_pool_pending",
                &[("pool", &p.name), ("class", class)],
                v as f64,
            );
        }
    }
    o.per_pool(
        s,
        "cronoq_pool_reservations",
        "gauge",
        "Fixed-start reservations",
        |p| p.reservations as f64,
    );
    o.per_pool(
        s,
        "cronoq_pool_priority_adherence",
        "gauge",
        "Windowed priority adherence",
        |p| p.priority_adherence,
    );
    o.per_pool(
        s,
        "cronoq_pool_fixed_start_hit_rate",
        "gauge",
        "Windowed fixed-start hit rate",
        |p| p.fixed_start_hit_rate,
    );
    o.per_pool(
        s,
        "cronoq_pool_utilization",
        "gauge",
        "Windowed utilization",
        |p| p.utilization,
    );
    o.per_pool(
        s,
        "cronoq_pool_schedule_efficiency",
        "gauge",
        "Windowed schedule efficiency",
        |p| p.schedule_efficiency,
    );
    o.family(
        "cronoq_pool_wait_seconds",
        "gauge",
        "Windowed queue wait quantiles",
    );
    for p in &s.pools {
        for (q, v) in [
            ("0.5", p.wait_p50_s),
            ("0.9", p.wait_p90_s),
            ("0.99", p.wait_p99_s),
        ] {
            o.sample(
                "cronoq_pool_wait_seconds",
                &[("pool", &p.name), ("quantile", q)],
                v,
            );
        }
    }
    o.per_pool(
        s,
        "cronoq_pool_cost_total",
        "counter",
        "Accumulated worker cost",
        |p| p.cost,
    );
    o.per_pool(
        s,
        "cronoq_pool_dispatched_window",
        "gauge",
        "Attempts dispatched within the metrics window (a windowed count, not a counter)",
        |p| p.dispatched as f64,
    );
    o.family(
        "cronoq_workers_connected",
        "gauge",
        "Connected remote workers",
    );
    o.sample("cronoq_workers_connected", &[], s.workers_connected as f64);
    for (name, help, v) in [
        ("cronoq_live_tasks", "Tasks not yet finished", s.live_tasks),
        ("cronoq_running_tasks", "Attempts running", s.running),
        ("cronoq_pending_tasks", "Tasks waiting to run", s.pending),
    ] {
        o.family(name, "gauge", help);
        o.sample(name, &[], v as f64);
    }
    let t = s.totals.unwrap_or_default();
    o.family("cronoq_tasks_total", "counter", "Tasks by terminal status");
    for (status, v) in [
        ("succeeded", t.succeeded),
        ("failed", t.failed),
        ("dead_lettered", t.dead_lettered),
        ("cancelled", t.cancelled),
        ("skipped", t.skipped),
        ("missed", t.missed),
        ("shed", t.shed),
    ] {
        o.sample("cronoq_tasks_total", &[("status", status)], v as f64);
    }
    for (name, help, v) in [
        (
            "cronoq_tasks_submitted_total",
            "Tasks admitted",
            t.submitted,
        ),
        (
            "cronoq_attempts_dispatched_total",
            "Attempts dispatched",
            t.dispatched,
        ),
        ("cronoq_retries_total", "Retries scheduled", t.retries),
        (
            "cronoq_timeouts_total",
            "Attempts that timed out",
            t.timeouts,
        ),
        (
            "cronoq_lease_expiries_total",
            "Leases that expired without a heartbeat",
            t.lease_expiries,
        ),
        (
            "cronoq_stale_completions_total",
            "Completions for leases that were no longer valid, ignored",
            t.stale_completions,
        ),
    ] {
        o.family(name, "counter", help);
        o.sample(name, &[], v as f64);
    }
    o.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_and_formats() {
        assert_eq!(escape_label("we\"ird\\pool\n"), r#"we\"ird\\pool\n"#);
        assert_eq!(value(f64::INFINITY), "+Inf");
        assert_eq!(value(f64::NEG_INFINITY), "-Inf");
        assert_eq!(value(f64::NAN), "NaN");
        let s = pb::SnapshotResponse {
            pools: vec![pb::PoolSnapshot {
                name: "a\"b".into(),
                pending_by_class: vec![1, 2, 3, 4],
                ..Default::default()
            }],
            ..Default::default()
        };
        let text = render(&s);
        assert!(
            text.contains("cronoq_pool_workers{pool=\"a\\\"b\"} 0\n"),
            "{text}"
        );
        assert!(text.contains("cronoq_pool_pending{pool=\"a\\\"b\",class=\"low\"} 4\n"));
        assert!(text.contains("# TYPE cronoq_pool_cost_total counter"));
    }
}
