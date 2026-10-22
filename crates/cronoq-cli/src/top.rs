//! `cronoq top`: a live view of pools, workers, live tasks and schedules.
//!
//! Each pane is fetched on its own. The pool, worker and schedule panes need the metrics role
//! and the task pane the client role, so with a token holding only one of them the other panes
//! say which role they need instead of failing the whole view.

use std::time::{Duration, Instant};

use cronoq_proto::v1 as pb;
use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph, Row, Table};

use crate::conn::describe;
use crate::fmt;

type Tasks = pb::tasks_client::TasksClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, crate::conn::Auth>,
>;
type Admin = pb::admin_client::AdminClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, crate::conn::Auth>,
>;

#[derive(clap::Args, Debug)]
pub struct TopArgs {
    /// Seconds between refreshes.
    #[arg(long, default_value_t = 1.0, value_parser = interval)]
    pub interval: f64,
    /// Only tasks of these pools (repeatable).
    #[arg(long = "pool", value_name = "POOL")]
    pub pools: Vec<String>,
    /// Most live tasks to show.
    #[arg(long, default_value_t = 200)]
    pub tasks: u32,
}

fn interval(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(v) if v.is_finite() && v >= 0.1 => Ok(v),
        _ => Err("the interval must be at least 0.1 seconds".into()),
    }
}

/// One pane's latest data, or why it has none.
#[derive(Clone, Debug, Default)]
pub enum Pane<T> {
    #[default]
    Loading,
    Ready(T),
    Error(String),
}

impl<T> Pane<T> {
    fn from(r: Result<tonic::Response<T>, tonic::Status>, role: &str) -> Pane<T> {
        match r {
            Ok(r) => Pane::Ready(r.into_inner()),
            Err(s) if s.code() == tonic::Code::PermissionDenied => {
                Pane::Error(format!("needs a token with the {role} role"))
            }
            Err(s) => Pane::Error(describe(&s)),
        }
    }
}

/// Everything one frame shows.
#[derive(Clone, Debug, Default)]
pub struct State {
    pub server: String,
    pub snapshot: Pane<pb::SnapshotResponse>,
    pub workers: Pane<pb::ListWorkersResponse>,
    pub tasks: Pane<pb::ListTasksResponse>,
    pub schedules: Pane<pb::ListRecurringResponse>,
    /// When the panes were fetched (Unix ns); 0 before the first fetch.
    pub updated_ns: i64,
}

const LIVE: [pb::TaskStatus; 5] = [
    pb::TaskStatus::Running,
    pb::TaskStatus::Retrying,
    pb::TaskStatus::Ready,
    pb::TaskStatus::Blocked,
    pb::TaskStatus::Scheduled,
];

async fn fetch(tasks: &mut Tasks, admin: &mut Admin, a: &TopArgs, st: &mut State) {
    let req = pb::ListTasksRequest {
        pools: a.pools.clone(),
        statuses: LIVE.iter().map(|s| *s as i32).collect(),
        after: 0,
        limit: a.tasks,
    };
    let (mut a1, mut a2, mut a3) = (admin.clone(), admin.clone(), admin.clone());
    let (snap, workers, list, schedules) = tokio::join!(
        a1.snapshot(pb::Empty {}),
        a2.list_workers(pb::Empty {}),
        tasks.list_tasks(req),
        a3.list_recurring(pb::Empty {}),
    );
    st.snapshot = Pane::from(snap, "metrics");
    st.workers = Pane::from(workers, "metrics");
    st.tasks = Pane::from(list, "client");
    st.schedules = Pane::from(schedules, "metrics");
    st.updated_ns = fmt::now_ns();
}

/// Run the live view until the user quits.
pub async fn run(
    mut tasks: Tasks,
    mut admin: Admin,
    server: String,
    a: TopArgs,
) -> anyhow::Result<()> {
    let mut st = State {
        server,
        ..Default::default()
    };
    let mut terminal = ratatui::try_init()?;
    let every = Duration::from_secs_f64(a.interval);
    let mut next = Instant::now();
    let result = loop {
        if Instant::now() >= next {
            fetch(&mut tasks, &mut admin, &a, &mut st).await;
            next = Instant::now() + every;
        }
        if let Err(e) = terminal.draw(|f| render(f, &st, fmt::now_ns())) {
            break Err(e.into());
        }
        let wait = next
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(250));
        let ev = tokio::task::block_in_place(|| -> std::io::Result<Option<Event>> {
            if event::poll(wait)? {
                event::read().map(Some)
            } else {
                Ok(None)
            }
        });
        match ev {
            Ok(Some(Event::Key(k))) if k.kind == KeyEventKind::Press => match k.code {
                KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break Ok(()),
                KeyCode::Char('r') => next = Instant::now(),
                _ => {}
            },
            Ok(_) => {}
            Err(e) => break Err(e.into()),
        }
    };
    ratatui::restore();
    result
}

fn message_pane<T>(p: &Pane<T>) -> Option<String> {
    match p {
        Pane::Loading => Some("loading…".into()),
        Pane::Error(e) => Some(e.clone()),
        Pane::Ready(_) => None,
    }
}

fn header() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

fn secs(s: f64) -> String {
    if s >= 10.0 {
        format!("{s:.0}s")
    } else {
        format!("{s:.2}s")
    }
}

/// Draw one frame.
pub fn render(f: &mut Frame, st: &State, now: i64) {
    let [top, pools, middle, tasks] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(pool_height(st)),
        Constraint::Percentage(35),
        Constraint::Fill(1),
    ])
    .areas(f.area());
    f.render_widget(Paragraph::new(summary(st, now)), top);
    render_pools(f, st, pools);
    let [workers, schedules] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(middle);
    render_workers(f, st, workers, now);
    render_schedules(f, st, schedules, now);
    render_tasks(f, st, tasks, now);
}

fn pool_height(st: &State) -> u16 {
    match &st.snapshot {
        Pane::Ready(s) => (s.pools.len() as u16).saturating_add(3).min(12),
        _ => 3,
    }
}

fn summary(st: &State, now: i64) -> Line<'static> {
    let mut parts = vec!["cronoq top".bold().to_string()];
    if !st.server.is_empty() {
        parts.push(st.server.clone());
    }
    if let Pane::Ready(s) = &st.snapshot {
        let t = s.totals.unwrap_or_default();
        parts.push(format!(
            "live {}  running {}  pending {}  workers {}  done {}  failed {}",
            s.live_tasks, s.running, s.pending, s.workers_connected, t.succeeded, t.failed
        ));
    }
    if st.updated_ns > 0 {
        parts.push(format!("updated {}", fmt::ago(st.updated_ns, now)));
    }
    parts.push("q quit, r refresh".into());
    Line::from(parts.join("   "))
}

fn render_pools(f: &mut Frame, st: &State, area: Rect) {
    let block = Block::bordered().title(" Pools ");
    let s = match &st.snapshot {
        Pane::Ready(s) => s,
        p => {
            f.render_widget(
                Paragraph::new(message_pane(p).unwrap_or_default()).block(block),
                area,
            );
            return;
        }
    };
    let rows = s.pools.iter().map(|p| Row::new(pool_cells(p)));
    let table = Table::new(rows, [Constraint::Fill(1); 10])
        .header(Row::new(POOL_HEADER).style(header()))
        .block(block);
    f.render_widget(table, area);
}

const POOL_HEADER: [&str; 10] = [
    "POOL",
    "CAPACITY",
    "BUSY",
    "PENDING",
    "WAIT P50",
    "WAIT P90",
    "UTIL",
    "DONE/MIN",
    "FAILED/MIN",
    "WORKERS",
];

fn pool_cells(p: &pb::PoolSnapshot) -> Vec<String> {
    vec![
        p.name.clone(),
        p.capacity.to_string(),
        p.busy.to_string(),
        p.pending_by_class.iter().sum::<u64>().to_string(),
        secs(p.wait_p50_s),
        secs(p.wait_p90_s),
        format!("{:.0}%", p.utilization * 100.0),
        p.succeeded.to_string(),
        p.failed.to_string(),
        p.workers_connected.to_string(),
    ]
}

/// The pools of a snapshot as a text table (for `cronoq snapshot`).
pub fn pools_table(s: &pb::SnapshotResponse) -> String {
    let rows: Vec<Vec<String>> = s.pools.iter().map(pool_cells).collect();
    let t = s.totals.unwrap_or_default();
    format!(
        "{}\n\nlive {}  running {}  pending {}  workers {}  submitted {}  succeeded {}  failed {}",
        fmt::table(&POOL_HEADER, &rows),
        s.live_tasks,
        s.running,
        s.pending,
        s.workers_connected,
        t.submitted,
        t.succeeded,
        t.failed
    )
}

fn render_workers(f: &mut Frame, st: &State, area: Rect, now: i64) {
    let block = Block::bordered().title(" Workers ");
    let w = match &st.workers {
        Pane::Ready(w) => w,
        p => {
            f.render_widget(
                Paragraph::new(message_pane(p).unwrap_or_default()).block(block),
                area,
            );
            return;
        }
    };
    let rows = w.workers.iter().map(|w| {
        Row::new(vec![
            w.name.clone(),
            w.pool.clone(),
            format!("{}/{}", w.busy, w.slots),
            fmt::labels(&w.labels),
            fmt::ago(w.connected_unix_ns, now),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Fill(2),
            Constraint::Fill(1),
            Constraint::Length(7),
            Constraint::Fill(2),
            Constraint::Length(9),
        ],
    )
    .header(Row::new(["NAME", "POOL", "BUSY", "LABELS", "CONNECTED"]).style(header()))
    .block(block.title_bottom(format!(" {} connected ", w.workers.len())));
    f.render_widget(table, area);
}

fn render_schedules(f: &mut Frame, st: &State, area: Rect, now: i64) {
    let block = Block::bordered().title(" Schedules ");
    let s = match &st.schedules {
        Pane::Ready(s) => s,
        p => {
            f.render_widget(
                Paragraph::new(message_pane(p).unwrap_or_default()).block(block),
                area,
            );
            return;
        }
    };
    let rows = s.schedules.iter().map(|s| {
        Row::new(vec![
            s.name.clone(),
            s.cron.clone(),
            s.pool.clone(),
            s.next_unix_ns
                .map(|t| fmt::ago(t, now))
                .unwrap_or_else(|| "-".into()),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Fill(2),
            Constraint::Fill(2),
            Constraint::Fill(1),
            Constraint::Length(9),
        ],
    )
    .header(Row::new(["NAME", "CRON", "POOL", "NEXT"]).style(header()))
    .block(block);
    f.render_widget(table, area);
}

fn render_tasks(f: &mut Frame, st: &State, area: Rect, now: i64) {
    let block = Block::bordered().title(" Live tasks ");
    let page = match &st.tasks {
        Pane::Ready(p) => p,
        p => {
            f.render_widget(
                Paragraph::new(message_pane(p).unwrap_or_default()).block(block),
                area,
            );
            return;
        }
    };
    // Running work first, then the rest in submission order.
    let mut list: Vec<&pb::TaskInfo> = page.tasks.iter().collect();
    list.sort_by_key(|t| (t.status != pb::TaskStatus::Running as i32, t.id));
    let rows = list.iter().map(|t| Row::new(fmt::task_row(t, now)));
    let more = if page.next_after > 0 {
        " (more not shown) "
    } else {
        " "
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(20),
            Constraint::Length(10),
            Constraint::Fill(1),
            Constraint::Fill(1),
            Constraint::Fill(2),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(9),
        ],
    )
    .header(Row::new(fmt::TASK_HEADER).style(header()))
    .block(block.title_bottom(format!(" {}{more}", page.tasks.len())));
    f.render_widget(table, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn draw(st: &State) -> String {
        let mut t = Terminal::new(TestBackend::new(140, 40)).unwrap();
        t.draw(|f| render(f, st, 100_000_000_000)).unwrap();
        let buf = t.backend().buffer();
        let mut out = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    fn full() -> State {
        State {
            server: "http://127.0.0.1:7070".into(),
            snapshot: Pane::Ready(pb::SnapshotResponse {
                pools: vec![pb::PoolSnapshot {
                    name: "gpu-pool".into(),
                    capacity: 8,
                    busy: 3,
                    pending_by_class: vec![0, 1, 4, 0],
                    wait_p90_s: 1.5,
                    utilization: 0.375,
                    ..Default::default()
                }],
                live_tasks: 8,
                running: 3,
                pending: 5,
                workers_connected: 1,
                ..Default::default()
            }),
            workers: Pane::Ready(pb::ListWorkersResponse {
                workers: vec![pb::WorkerInfo {
                    name: "worker-eu-1".into(),
                    pool: "gpu-pool".into(),
                    labels: [("gpu".to_string(), "a100".to_string())].into(),
                    slots: 4,
                    busy: 3,
                    connected_unix_ns: 40_000_000_000,
                    ..Default::default()
                }],
            }),
            tasks: Pane::Ready(pb::ListTasksResponse {
                tasks: vec![
                    pb::TaskInfo {
                        id: 41,
                        status: pb::TaskStatus::Ready as i32,
                        pool: "gpu-pool".into(),
                        key: "queued-one".into(),
                        ..Default::default()
                    },
                    pb::TaskInfo {
                        id: 42,
                        status: pb::TaskStatus::Running as i32,
                        pool: "gpu-pool".into(),
                        key: "running-one".into(),
                        attempt: 1,
                        max_attempts: 3,
                        ..Default::default()
                    },
                ],
                next_after: 42,
            }),
            schedules: Pane::Ready(pb::ListRecurringResponse {
                schedules: vec![pb::RecurringInfo {
                    name: "nightly-report".into(),
                    cron: "0 0 3 * * *".into(),
                    pool: "gpu-pool".into(),
                    next_unix_ns: Some(160_000_000_000),
                    ..Default::default()
                }],
            }),
            updated_ns: 98_000_000_000,
        }
    }

    #[test]
    fn every_pane_shows_its_data() {
        let s = draw(&full());
        for want in [
            "live 8  running 3  pending 5",
            "updated 2s ago",
            "gpu-pool",
            "38%",
            "1.50s",
            "worker-eu-1",
            "3/4",
            "gpu=a100",
            "1m ago",
            "nightly-report",
            "in 1m",
            "1/3",
            "(more not shown)",
        ] {
            assert!(s.contains(want), "missing {want:?} in\n{s}");
        }
        // Running tasks come first.
        let (run, queued) = (
            s.find("running-one").unwrap(),
            s.find("queued-one").unwrap(),
        );
        assert!(run < queued, "{s}");
    }

    #[test]
    fn panes_without_access_say_which_role_they_need() {
        let mut st = full();
        fn denied<T>() -> Result<tonic::Response<T>, tonic::Status> {
            Err(tonic::Status::permission_denied(
                "credentials lack the required role",
            ))
        }
        st.snapshot = Pane::from(denied(), "metrics");
        st.workers = Pane::from(denied(), "metrics");
        st.schedules = Pane::Error("unavailable: this instance is a standby".into());
        let s = draw(&st);
        assert!(s.contains("needs a token with the metrics role"), "{s}");
        assert!(s.contains("unavailable: this instance is a standby"), "{s}");
        // The task pane still shows its data.
        assert!(s.contains("running-one"), "{s}");
    }
}
