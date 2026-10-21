//! Prometheus text exposition built from the scheduler snapshot.

use std::fmt::Write;

use cronoq::{Priority, Snapshot};

pub fn render(s: &Snapshot, workers_connected: usize) -> String {
    let mut out = String::with_capacity(4096);
    let mut gauge = |name: &str, help: &str, rows: Vec<(String, f64)>| {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge");
        for (labels, v) in rows {
            let _ = writeln!(out, "{name}{labels} {v}");
        }
    };
    let per_pool = |f: &dyn Fn(&cronoq::PoolSnapshot) -> f64| {
        s.pools
            .iter()
            .map(|p| (format!("{{pool=\"{}\"}}", p.name), f(p)))
            .collect::<Vec<_>>()
    };
    gauge(
        "cronoq_pool_workers",
        "Current worker capacity",
        per_pool(&|p| p.capacity as f64),
    );
    gauge(
        "cronoq_pool_desired_workers",
        "Autoscaler target capacity",
        per_pool(&|p| p.desired as f64),
    );
    gauge(
        "cronoq_pool_busy_workers",
        "Workers running tasks",
        per_pool(&|p| p.busy as f64),
    );
    gauge(
        "cronoq_pool_held_workers",
        "Workers held for fixed-start tasks",
        per_pool(&|p| p.held as f64),
    );
    gauge(
        "cronoq_pool_pending",
        "Tasks waiting to run",
        s.pools
            .iter()
            .flat_map(|p| {
                Priority::ALL.iter().map(move |c| {
                    (
                        format!("{{pool=\"{}\",class=\"{}\"}}", p.name, c.name()),
                        p.pending_by_class[c.index()] as f64,
                    )
                })
            })
            .collect(),
    );
    gauge(
        "cronoq_pool_priority_adherence",
        "Windowed priority adherence",
        per_pool(&|p| p.priority_adherence),
    );
    gauge(
        "cronoq_pool_fixed_start_hit_rate",
        "Windowed fixed-start hit rate",
        per_pool(&|p| p.fixed_start_hit_rate),
    );
    gauge(
        "cronoq_pool_utilization",
        "Windowed utilization",
        per_pool(&|p| p.utilization),
    );
    gauge(
        "cronoq_pool_wait_p99_seconds",
        "Windowed p99 queue wait",
        per_pool(&|p| p.wait_p99.as_secs_f64()),
    );
    gauge(
        "cronoq_pool_cost",
        "Accumulated worker cost",
        per_pool(&|p| p.cost),
    );
    gauge(
        "cronoq_workers_connected",
        "Connected remote workers",
        vec![(String::new(), workers_connected as f64)],
    );
    let t = &s.totals;
    let _ = writeln!(
        out,
        "# HELP cronoq_tasks_total Tasks by terminal status\n# TYPE cronoq_tasks_total counter"
    );
    for (status, v) in [
        ("succeeded", t.succeeded),
        ("failed", t.failed),
        ("dead_lettered", t.dead_lettered),
        ("cancelled", t.cancelled),
        ("skipped", t.skipped),
        ("missed", t.missed),
        ("shed", t.shed),
    ] {
        let _ = writeln!(out, "cronoq_tasks_total{{status=\"{status}\"}} {v}");
    }
    let _ = writeln!(
        out,
        "# HELP cronoq_tasks_submitted_total Tasks admitted\n# TYPE cronoq_tasks_submitted_total counter"
    );
    let _ = writeln!(out, "cronoq_tasks_submitted_total {}", t.submitted);
    let _ = writeln!(
        out,
        "# HELP cronoq_retries_total Retries scheduled\n# TYPE cronoq_retries_total counter"
    );
    let _ = writeln!(out, "cronoq_retries_total {}", t.retries);
    out
}
