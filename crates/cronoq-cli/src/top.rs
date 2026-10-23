//! `cronoq top`: a live view of pools, workers, live tasks and schedules.
//!
//! Each pane is fetched on its own. The pool, worker and schedule panes need the metrics role
//! and the task pane the client role, so with a token holding only one of them the other panes
//! say which role they need instead of failing the whole view.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;

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

/// Why a pane has no data: the role it needs, or the server's error.
fn explain(s: &tonic::Status, role: &str) -> String {
    if s.code() == tonic::Code::PermissionDenied {
        format!("needs a token with the {role} role")
    } else {
        describe(s)
    }
}

impl<T> Pane<T> {
    fn from(r: Result<tonic::Response<T>, tonic::Status>, role: &str) -> Pane<T> {
        match r {
            Ok(r) => Pane::Ready(r.into_inner()),
            Err(s) => Pane::Error(explain(&s, role)),
        }
    }
}

/// Most recent status events kept for the events pane.
const RECENT: usize = 200;

/// The latest status events, newest first, and the state of the stream that delivers them.
#[derive(Clone, Debug, Default)]
pub struct Events {
    pub recent: VecDeque<pb::StatusEvent>,
    /// Why no events are arriving (connecting, an error, a missing role); `None` while live.
    pub problem: Option<String>,
}

/// Follow every status transition into `events`, reconnecting with backoff.
async fn follow(mut tasks: Tasks, pools: Vec<String>, events: Arc<Mutex<Events>>) {
    let set_problem = |p: Option<String>| events.lock().unwrap().problem = p;
    let mut backoff = Duration::from_millis(500);
    loop {
        let req = pb::WatchRequest {
            pools: pools.clone(),
            all_transitions: true,
            ..Default::default()
        };
        match tasks.watch(req).await {
            Ok(r) => {
                set_problem(None);
                backoff = Duration::from_millis(500);
                let mut stream = r.into_inner();
                let ended = loop {
                    match stream.next().await {
                        Some(Ok(ev)) => {
                            let mut e = events.lock().unwrap();
                            e.recent.push_front(ev);
                            e.recent.truncate(RECENT);
                        }
                        Some(Err(s)) => break explain(&s, "client"),
                        None => break "the event stream ended".to_string(),
                    }
                };
                set_problem(Some(format!("{ended}; reconnecting")));
            }
            Err(s) => set_problem(Some(explain(&s, "client"))),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
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
    pub events: Events,
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
        events: Events {
            problem: Some("connecting…".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let events = Arc::new(Mutex::new(st.events.clone()));
    let follower = tokio::spawn(follow(tasks.clone(), a.pools.clone(), events.clone()));
    let mut terminal = ratatui::try_init()?;
    let every = Duration::from_secs_f64(a.interval);
    let mut next = Instant::now();
    let result = loop {
        if Instant::now() >= next {
            fetch(&mut tasks, &mut admin, &a, &mut st).await;
            next = Instant::now() + every;
        }
        st.events = events.lock().unwrap().clone();
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
    follower.abort();
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

/// A table row of server-supplied text. ratatui drops control characters itself; this also
/// shows bidirectional controls, which it would keep, as escapes.
fn row(cells: Vec<String>) -> Row<'static> {
    Row::new(
        cells
            .into_iter()
            .map(|c| fmt::clean(&c, false).into_owned()),
    )
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
    let [tasks, events] =
        Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)]).areas(tasks);
    render_tasks(f, st, tasks, now);
    render_events(f, st, events, now);
}

fn render_events(f: &mut Frame, st: &State, area: Rect, now: i64) {
    let mut block = Block::bordered().title(" Recent events ");
    if let Some(p) = &st.events.problem {
        block = block.title_bottom(format!(" {p} "));
    }
    if st.events.recent.is_empty() {
        let text = st
            .events
            .problem
            .clone()
            .unwrap_or_else(|| "no events yet".into());
        f.render_widget(Paragraph::new(text).block(block), area);
        return;
    }
    let rows = st.events.recent.iter().map(|e| {
        row(vec![
            fmt::ago(e.at_unix_ns, now),
            fmt::status(e.status).to_string(),
            e.id.to_string(),
            e.pool.clone(),
            match (e.key.is_empty(), e.message.is_empty()) {
                (_, true) => e.key.clone(),
                (true, false) => e.message.clone(),
                (false, false) => format!("{}: {}", e.key, e.message),
            },
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(13),
            Constraint::Length(20),
            Constraint::Fill(1),
            Constraint::Fill(2),
        ],
    )
    .header(Row::new(["WHEN", "STATUS", "ID", "POOL", "KEY / MESSAGE"]).style(header()));
    f.render_widget(table.block(block), area);
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
        if !s.unavailable_shards.is_empty() {
            parts.push(format!(
                "UNREACHABLE: {}",
                fmt::clean(&s.unavailable_shards.join(", "), false)
            ));
        }
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
    let rows = s.pools.iter().map(|p| row(pool_cells(p)));
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
        row(vec![
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
        row(vec![
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
    let rows = list.iter().map(|t| row(fmt::task_row(t, now)));
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

    const CONFIG: &str = r#"
[server]
grpc_addr = "127.0.0.1:0"
http_addr = "127.0.0.1:0"
shutdown_grace_s = 0.2
[[auth.tokens]]
token = "client-token-0123456789abcdef0123"
role = "client"
[[auth.tokens]]
token = "metrics-token-0123456789abcdef0123"
role = "metrics"
[[pools]]
name = "default"
"#;

    /// Clients for `url` authenticating with `token` (through a token file).
    fn clients(url: &str, token: &str, name: &str) -> crate::conn::Clients {
        let path = std::env::temp_dir().join(format!("cronoq-top-{}-{name}", std::process::id()));
        std::fs::write(&path, token).unwrap();
        crate::conn::ConnArgs {
            server: url.into(),
            token_file: Some(path),
            ca: None,
            cert: None,
            key: None,
            domain: None,
        }
        .clients()
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_event_pane_follows_the_stream_and_reports_missing_roles() {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(cronoq_server::run(
            cronoq_server::Config::parse(CONFIG).unwrap(),
            async {
                let _ = stopped.await;
            },
            move |b| {
                let _ = bound_tx.send(b);
            },
        ));
        let url = format!("http://{}", bound_rx.await.unwrap().grpc);
        let client = clients(&url, "client-token-0123456789abcdef0123", "client");
        let metrics = clients(&url, "metrics-token-0123456789abcdef0123", "metrics");

        let live = Arc::new(Mutex::new(Events::default()));
        let denied = Arc::new(Mutex::new(Events::default()));
        let a = tokio::spawn(follow(client.tasks.clone(), vec![], live.clone()));
        let b = tokio::spawn(follow(metrics.tasks.clone(), vec![], denied.clone()));
        // Submit until the stream (which starts asynchronously) has seen an event.
        let mut tasks = client.tasks.clone();
        for _ in 0..100 {
            tasks
                .submit(pb::SubmitRequest {
                    task: Some(pb::TaskSpec::default()),
                    ..Default::default()
                })
                .await
                .unwrap();
            if !live.lock().unwrap().recent.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        {
            let e = live.lock().unwrap();
            assert!(!e.recent.is_empty());
            assert_eq!(e.problem, None);
        }
        let mut problem = None;
        for _ in 0..200 {
            problem = denied.lock().unwrap().problem.clone();
            if problem.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            problem.as_deref(),
            Some("needs a token with the client role")
        );
        a.abort();
        b.abort();
        let _ = stop.send(());
        let _ = server.await;
    }

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
                unavailable_shards: vec!["tpu".into()],
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
                ..Default::default()
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
                ..Default::default()
            }),
            schedules: Pane::Ready(pb::ListRecurringResponse {
                schedules: vec![pb::RecurringInfo {
                    name: "nightly-report".into(),
                    cron: "0 0 3 * * *".into(),
                    pool: "gpu-pool".into(),
                    next_unix_ns: Some(160_000_000_000),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            events: Events {
                recent: [pb::StatusEvent {
                    id: 40,
                    pool: "gpu-pool".into(),
                    status: pb::TaskStatus::Failed as i32,
                    at_unix_ns: 95_000_000_000,
                    message: "out of memory".into(),
                    ..Default::default()
                }]
                .into(),
                problem: None,
            },
            updated_ns: 98_000_000_000,
        }
    }

    #[test]
    fn every_pane_shows_its_data() {
        let s = draw(&full());
        for want in [
            "live 8  running 3  pending 5",
            "UNREACHABLE: tpu",
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
            "Recent events",
            "out of memory",
            "5s ago",
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
        // Bidirectional controls in another client's text are shown, not applied.
        st.events.recent[0].key = "report\u{202e}fdp.exe".into();
        let s = draw(&st);
        assert!(
            !s.contains('\u{202e}') && s.contains("report\\u{202e}"),
            "{s}"
        );
        // The event stream reports why it stopped and keeps the events it had.
        st.events.problem = Some("needs a token with the client role; reconnecting".into());
        let s = draw(&st);
        assert!(s.contains("needs a token with the client role"), "{s}");
        assert!(s.contains("out of memory"), "{s}");
    }
}
