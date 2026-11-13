//! `cronoq`: command-line client for cronoq-server.
//!
//! Submit, inspect, cancel and wait for tasks, list tasks, workers and schedules, manage
//! schedules, stream status events, and watch the scheduler live with `cronoq top`.

mod conn;
mod fmt;
mod top;

use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use cronoq_proto::v1 as pb;
use futures_util::StreamExt;

use crate::conn::{Clients, ConnArgs, rpc};

#[derive(Parser, Debug)]
#[command(
    name = "cronoq",
    version,
    about = "Command-line client for cronoq-server"
)]
struct Cli {
    #[command(flatten)]
    conn: ConnArgs,
    /// Output format.
    #[arg(short, long, global = true, value_enum, default_value_t = Output::Text)]
    output: Output,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Output {
    Text,
    Json,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Submit a task.
    Submit(Box<SubmitArgs>),
    /// Show a task.
    Get(RefArgs),
    /// Cancel a task.
    Cancel(RefArgs),
    /// Change the priority, deadline or tags of a task that has not finished.
    Update(UpdateArgs),
    /// Wait for a task to finish and print its output.
    Wait {
        #[command(flatten)]
        target: RefArgs,
        /// Give up after this many seconds (capped by the server's limits.max_wait_s).
        #[arg(long, value_name = "SECONDS")]
        timeout: Option<f64>,
    },
    /// List tasks, oldest first.
    Tasks(TasksArgs),
    /// List connected workers (metrics role).
    Workers,
    /// List recurring schedules (metrics role).
    Schedules,
    /// Add or remove a recurring schedule (admin role).
    #[command(subcommand)]
    Schedule(ScheduleCommand),
    /// Show or change an ordering key: tasks of a pool that share one run one at a time, in
    /// submission order.
    #[command(subcommand)]
    Ordering(OrderingCommand),
    /// Resume what a failure halted: an ordering key (admin role).
    Resume {
        /// The halted ordering key.
        #[arg(long, value_name = "KEY")]
        ordering_key: String,
        #[arg(long, default_value = "default")]
        pool: String,
    },
    /// Show pools, quality signals and totals (metrics role).
    Snapshot,
    /// Stream status events until interrupted.
    Watch {
        /// Only these pools (repeatable; default: every pool the token may use).
        #[arg(long = "pool", value_name = "POOL")]
        pools: Vec<String>,
        /// Every transition, not only finished tasks.
        #[arg(long)]
        all: bool,
    },
    /// Live view of pools, workers, tasks and schedules.
    Top(top::TopArgs),
}

/// A task by id, or by key with `--key`.
#[derive(Args, Debug)]
struct RefArgs {
    /// Task id (or key, with --key).
    task: String,
    /// Treat TASK as a key.
    #[arg(long)]
    key: bool,
}

impl RefArgs {
    fn to_pb(&self) -> anyhow::Result<pb::TaskRef> {
        let target = if self.key {
            pb::task_ref::Target::Key(self.task.clone())
        } else {
            match self.task.parse::<u64>() {
                Ok(id) => pb::task_ref::Target::Id(id),
                Err(_) => bail!(
                    "'{}' is not a task id; pass --key to look it up by key",
                    self.task
                ),
            }
        };
        Ok(pb::TaskRef {
            target: Some(target),
        })
    }
}

#[derive(Args, Debug)]
struct SpecArgs {
    /// Payload: a string (sent as UTF-8 text), JSON with --json, or `-` to read stdin.
    payload: Option<String>,
    /// Read the payload from a file (sent as bytes).
    #[arg(long, value_name = "PATH", conflicts_with = "payload")]
    file: Option<PathBuf>,
    /// The payload is JSON (checked, and sent as application/json).
    #[arg(long)]
    json: bool,
    /// Content type passed to the worker (overrides the default for the payload).
    #[arg(long)]
    content_type: Option<String>,
    #[arg(long, default_value = "default")]
    pool: String,
    #[arg(long, default_value = "")]
    kind: String,
    #[arg(long, value_enum, default_value_t = PriorityArg::Normal)]
    priority: PriorityArg,
    /// Tenant, for pools that share capacity fairly.
    #[arg(long, default_value = "")]
    tenant: String,
    /// Required worker label `name=value` (repeatable).
    #[arg(long = "label", value_name = "NAME=VALUE", value_parser = pair)]
    labels: Vec<(String, String)>,
    /// Tag `name=value` passed to the worker (repeatable).
    #[arg(long = "tag", value_name = "NAME=VALUE", value_parser = pair)]
    tags: Vec<(String, String)>,
    /// Resource `name=units` acquired for each attempt (repeatable).
    #[arg(long = "resource", value_name = "NAME=UNITS", value_parser = resource)]
    resources: Vec<(String, u32)>,
    /// Deadline, in seconds from now (for schedules: from each occurrence's start).
    #[arg(long, value_name = "SECONDS")]
    deadline_in: Option<f64>,
    /// Per-attempt timeout, in seconds.
    #[arg(long, value_name = "SECONDS")]
    timeout_s: Option<f64>,
}

#[derive(Args, Debug)]
struct UpdateArgs {
    #[command(flatten)]
    target: RefArgs,
    /// New priority class. A waiting task keeps its place by submission order in that class.
    #[arg(long, value_enum)]
    priority: Option<PriorityArg>,
    /// New deadline, in seconds from now.
    #[arg(long, value_name = "SECONDS", conflicts_with = "no_deadline")]
    deadline_in: Option<f64>,
    /// Remove the deadline.
    #[arg(long)]
    no_deadline: bool,
    /// Replace the task's tags with these `name=value` pairs (repeatable).
    #[arg(long = "tag", value_name = "NAME=VALUE", value_parser = pair, conflicts_with = "no_tags")]
    tags: Vec<(String, String)>,
    /// Remove all tags.
    #[arg(long)]
    no_tags: bool,
}

#[derive(Args, Debug)]
struct SubmitArgs {
    #[command(flatten)]
    spec: SpecArgs,
    /// Unique key: deduplicates submissions.
    #[arg(long)]
    key: Option<String>,
    /// Latest wins: replace the task that holds --key if it has not started; if it has, run
    /// this submission after it as a new task with the same key.
    #[arg(long, requires = "key")]
    replace: bool,
    /// With --replace: take this submission's place in line instead of keeping the place of
    /// the task it replaces.
    #[arg(long, requires = "replace")]
    renew_position: bool,
    /// Fail if --key belongs to a task that was submitted with other content (payload,
    /// content type, pool, kind, priority or dependencies), instead of returning that task.
    #[arg(long, requires = "key", conflicts_with = "replace")]
    reject_conflict: bool,
    /// Return the task that holds --key whatever it was submitted with, also when the
    /// server's default is to report conflicts.
    #[arg(long, requires = "key", conflicts_with_all = ["replace", "reject_conflict"])]
    return_existing: bool,
    /// Ordering key: tasks of one pool that share it run one at a time, in the order they
    /// were submitted. Not a deduplication key (--key).
    #[arg(long, value_name = "KEY", conflicts_with = "start_in")]
    ordering_key: Option<String>,
    /// With --ordering-key: if this task fails, halt the key until `cronoq resume`.
    #[arg(long, requires = "ordering_key")]
    halt_on_failure: bool,
    /// Start at a fixed time, in seconds from now.
    #[arg(long, value_name = "SECONDS")]
    start_in: Option<f64>,
    /// Run only after this task (id, or key) succeeds (repeatable).
    #[arg(long, value_name = "TASK")]
    after: Vec<String>,
    /// Wait for the result and print its output.
    #[arg(long)]
    wait: bool,
    /// Upper bound for --wait, in seconds.
    #[arg(long, value_name = "SECONDS", requires = "wait")]
    wait_timeout: Option<f64>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum PriorityArg {
    Critical,
    High,
    Normal,
    Low,
}

impl PriorityArg {
    fn to_pb(self) -> pb::Priority {
        match self {
            PriorityArg::Critical => pb::Priority::Critical,
            PriorityArg::High => pb::Priority::High,
            PriorityArg::Normal => pb::Priority::Normal,
            PriorityArg::Low => pb::Priority::Low,
        }
    }
}

#[derive(Args, Debug)]
struct TasksArgs {
    /// Only these pools (repeatable; default: every pool the token may use).
    #[arg(long = "pool", value_name = "POOL")]
    pools: Vec<String>,
    /// Only these statuses (repeatable), e.g. running, failed.
    #[arg(long = "status", value_name = "STATUS", value_parser = status_arg)]
    statuses: Vec<pb::TaskStatus>,
    /// Tasks after this id.
    #[arg(long, value_name = "ID", default_value_t = 0)]
    after: u64,
    /// Page size (capped by the server's limits.max_list).
    #[arg(long, default_value_t = 100)]
    limit: u32,
    /// Follow pages until every matching task is listed.
    #[arg(long)]
    all: bool,
}

#[derive(Subcommand, Debug)]
enum OrderingCommand {
    /// Show an ordering key: its tasks, the one at the front, its floor, whether it is halted
    /// (metrics role).
    Get {
        key: String,
        #[arg(long, default_value = "default")]
        pool: String,
    },
    /// List the halted ordering keys of a pool (metrics role).
    Halted {
        #[arg(long, default_value = "default")]
        pool: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Set the class the front of an ordering key never runs below (admin role).
    Floor {
        key: String,
        /// The class; leave out with --clear.
        #[arg(
            value_enum,
            required_unless_present = "clear",
            conflicts_with = "clear"
        )]
        class: Option<PriorityArg>,
        /// Remove the floor.
        #[arg(long)]
        clear: bool,
        /// Keep the floor when the key has no tasks left (otherwise it goes with them).
        #[arg(long, conflicts_with = "clear")]
        persist: bool,
        #[arg(long, default_value = "default")]
        pool: String,
    },
}

#[derive(Subcommand, Debug)]
enum ScheduleCommand {
    /// Add or replace a schedule.
    Add {
        name: String,
        /// Cron expression: six fields (seconds first) or five.
        cron: String,
        /// IANA time zone (default UTC).
        #[arg(long)]
        tz: Option<String>,
        #[arg(long, value_enum, default_value_t = OverlapArg::Skip)]
        overlap: OverlapArg,
        /// Key prefix: each occurrence's key is `<prefix>@<start in UTC>`.
        #[arg(long)]
        key: Option<String>,
        #[command(flatten)]
        spec: Box<SpecArgs>,
    },
    /// Remove a schedule.
    Remove { name: String },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OverlapArg {
    Skip,
    Allow,
    Queue,
    QueueOnSuccess,
}

fn pair(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected NAME=VALUE, got '{s}'")),
    }
}

fn resource(s: &str) -> Result<(String, u32), String> {
    let (k, v) = pair(s)?;
    let n = v
        .parse()
        .map_err(|_| format!("units must be a whole number in '{s}'"))?;
    Ok((k, n))
}

fn status_arg(s: &str) -> Result<pb::TaskStatus, String> {
    fmt::status_value(s).ok_or_else(|| {
        format!(
            "unknown status '{s}' (blocked, scheduled, ready, running, retrying, succeeded, \
             failed, dead_lettered, cancelled, skipped, missed, shed)"
        )
    })
}

impl SpecArgs {
    fn to_pb(&self) -> anyhow::Result<pb::TaskSpec> {
        let (payload, default_type) = match (&self.payload, &self.file) {
            (_, Some(p)) => (
                std::fs::read(p).with_context(|| format!("reading {}", p.display()))?,
                "application/octet-stream",
            ),
            (Some(s), None) if s == "-" => {
                let mut buf = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut buf)
                    .context("reading stdin")?;
                (buf, "application/octet-stream")
            }
            (Some(s), None) => (s.clone().into_bytes(), "text/plain; charset=utf-8"),
            (None, None) => (Vec::new(), ""),
        };
        let default_type = if self.json {
            serde_json::from_slice::<serde_json::Value>(&payload)
                .context("the payload is not valid JSON")?;
            "application/json"
        } else {
            default_type
        };
        Ok(pb::TaskSpec {
            payload,
            content_type: self
                .content_type
                .clone()
                .unwrap_or_else(|| default_type.to_string()),
            kind: self.kind.clone(),
            pool: self.pool.clone(),
            priority: self.priority.to_pb() as i32,
            deadline_in_s: self.deadline_in,
            timeout_s: self.timeout_s,
            resources: self.resources.iter().cloned().collect(),
            tags: self.tags.iter().cloned().collect(),
            labels: self.labels.iter().cloned().collect(),
            tenant: self.tenant.clone(),
            ..Default::default()
        })
    }
}

/// Say on stderr which shards a combined answer is missing.
fn warn_missing(shards: &[String]) {
    if !shards.is_empty() {
        eprintln!(
            "warning: shards {} could not be reached; their part is missing",
            fmt::clean(&shards.join(", "), false)
        );
    }
}

fn print_ordering(out: Output, i: &pb::OrderingInfo) {
    let class = |c: i32| fmt::priority(c).filter(|_| c != pb::Priority::Unspecified as i32);
    match out {
        Output::Json => print_json(&serde_json::json!({
            "pool": i.pool, "key": i.key, "tasks": i.tasks,
            "head": i.head.map(|id| id.to_string()),
            "effective": class(i.effective), "floor": i.floor.and_then(class),
            "persist": i.persist, "halted": i.halted,
        })),
        Output::Text => {
            let or_none = |v: Option<String>| v.unwrap_or_else(|| "-".into());
            println!("key        {}", fmt::clean(&i.key, false));
            println!("pool       {}", fmt::clean(&i.pool, false));
            println!("tasks      {}", i.tasks);
            println!("head       {}", or_none(i.head.map(|id| id.to_string())));
            println!(
                "runs at    {}",
                or_none(class(i.effective).map(|c| c.to_string()))
            );
            let kept = if i.persist { " (kept)" } else { "" };
            println!(
                "floor      {}",
                or_none(i.floor.and_then(class).map(|c| format!("{c}{kept}")))
            );
            println!("halted     {}", if i.halted { "yes" } else { "no" });
        }
    }
}

fn print_json(v: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(v).expect("JSON values serialize")
    );
}

/// Print a finished task's output (raw to stdout) or its JSON; the exit code says whether it
/// succeeded.
fn print_result(r: &pb::TaskResult, out: Output) -> ExitCode {
    let ok = r.status == pb::TaskStatus::Succeeded as i32;
    match out {
        Output::Json => print_json(&fmt::result_json(r)),
        Output::Text => {
            if let Some(o) = &r.output {
                let mut stdout = std::io::stdout().lock();
                if std::io::stdout().is_terminal() {
                    // Shown on a terminal: escape control characters. Redirected: unchanged.
                    let text = String::from_utf8_lossy(o);
                    let _ = stdout.write_all(fmt::clean(&text, true).as_bytes());
                    if !o.ends_with(b"\n") {
                        let _ = stdout.write_all(b"\n");
                    }
                } else {
                    let _ = stdout.write_all(o);
                }
            }
            if !ok {
                let msg = if r.message.is_empty() {
                    String::new()
                } else {
                    format!(": {}", fmt::clean(&r.message, false))
                };
                eprintln!("task {} {}{msg}", r.id, fmt::status(r.status));
            } else if r.output.is_none() && !r.output_available {
                eprintln!("task {} succeeded; its output is no longer retained", r.id);
            }
        }
    }
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    let Clients {
        mut tasks,
        mut admin,
    } = cli.conn.clients()?;
    let out = cli.output;
    match cli.command {
        Command::Submit(a) => {
            let mut spec = a.spec.to_pb()?;
            spec.key = a.key.unwrap_or_default();
            spec.start_in_s = a.start_in;
            for t in &a.after {
                let target = match t.parse::<u64>() {
                    Ok(id) => pb::task_ref::Target::Id(id),
                    Err(_) => pb::task_ref::Target::Key(t.clone()),
                };
                spec.deps.push(pb::Dep {
                    target: Some(pb::TaskRef {
                        target: Some(target),
                    }),
                    cond: pb::EdgeCond::OnSuccess as i32,
                });
            }
            spec.ordering_key = a.ordering_key.clone().unwrap_or_default();
            if a.halt_on_failure {
                spec.on_failure = pb::OrderFailure::Halt as i32;
            }
            let r = rpc(tasks
                .submit(pb::SubmitRequest {
                    task: Some(spec),
                    wait: a.wait,
                    wait_timeout_s: a.wait_timeout,
                    on_key_conflict: if a.replace {
                        pb::KeyConflict::ReplaceQueued
                    } else if a.reject_conflict {
                        pb::KeyConflict::Reject
                    } else if a.return_existing {
                        pb::KeyConflict::ReturnExisting
                    } else {
                        // The server's default.
                        pb::KeyConflict::Unspecified
                    } as i32,
                    renew_position: a.renew_position,
                })
                .await)?;
            if a.replace && r.outcome == pb::SubmitOutcome::Unspecified as i32 {
                // A server older than the option ignored it: with the key in use, this
                // submission was dropped, not stored.
                bail!("the server does not support --replace");
            }
            if let Some(res) = &r.result {
                return Ok(print_result(res, out));
            }
            match out {
                Output::Json => print_json(&serde_json::json!({
                    "id": r.id.to_string(), "duplicate": r.duplicate,
                    "outcome": fmt::outcome(r.outcome, r.duplicate),
                })),
                Output::Text => match fmt::outcome(r.outcome, r.duplicate) {
                    "created" => println!("{}", r.id),
                    "existing" => println!("{} (duplicate)", r.id),
                    // Replaced in place, or stored to run after the task with this id.
                    other => println!("{} ({other})", r.id),
                },
            }
        }
        Command::Get(t) => {
            let info = rpc(tasks
                .get(pb::GetRequest {
                    task: Some(t.to_pb()?),
                })
                .await)?;
            match out {
                Output::Json => print_json(&fmt::task_json(&info)),
                Output::Text => println!("{}", fmt::task_details(&info)),
            }
        }
        Command::Cancel(t) => {
            let r = rpc(tasks
                .cancel(pb::CancelRequest {
                    task: Some(t.to_pb()?),
                })
                .await)?;
            match out {
                Output::Json => print_json(&serde_json::json!({ "cancelled": r.cancelled })),
                Output::Text if r.cancelled => println!("cancelled"),
                Output::Text => println!("already finished"),
            }
        }
        Command::Update(a) => {
            if a.priority.is_none()
                && a.deadline_in.is_none()
                && !a.no_deadline
                && a.tags.is_empty()
                && !a.no_tags
            {
                bail!(
                    "nothing to change: pass --priority, --deadline-in, --no-deadline, --tag or --no-tags"
                );
            }
            let r = rpc(tasks
                .update(pb::UpdateRequest {
                    task: Some(a.target.to_pb()?),
                    priority: a.priority.map_or(pb::Priority::Unspecified, |p| p.to_pb()) as i32,
                    deadline_in_s: a.deadline_in,
                    deadline_unix_ns: None,
                    clear_deadline: a.no_deadline,
                    set_tags: a.no_tags || !a.tags.is_empty(),
                    tags: a.tags.iter().cloned().collect(),
                })
                .await)?;
            match out {
                Output::Json => print_json(&serde_json::json!({
                    "id": r.id.to_string(), "status": fmt::status(r.status),
                    "deferred": r.deferred, "changed": r.changed,
                })),
                Output::Text if !r.changed => println!("unchanged"),
                Output::Text if r.deferred => {
                    println!("updated (applies after the running attempt)")
                }
                Output::Text => println!("updated"),
            }
        }
        Command::Wait { target, timeout } => {
            let r = rpc(tasks
                .wait(pb::WaitRequest {
                    task: Some(target.to_pb()?),
                    timeout_s: timeout,
                })
                .await)?;
            return Ok(print_result(&r, out));
        }
        Command::Tasks(a) => {
            let mut after = a.after;
            let mut all = Vec::new();
            let mut missing = Vec::new();
            loop {
                let page = rpc(tasks
                    .list_tasks(pb::ListTasksRequest {
                        pools: a.pools.clone(),
                        statuses: a.statuses.iter().map(|s| *s as i32).collect(),
                        after,
                        limit: a.limit,
                    })
                    .await)?;
                all.extend(page.tasks);
                for s in page.unavailable_shards {
                    if !missing.contains(&s) {
                        missing.push(s);
                    }
                }
                if !a.all || page.next_after == 0 {
                    after = page.next_after;
                    break;
                }
                after = page.next_after;
            }
            warn_missing(&missing);
            match out {
                Output::Json => {
                    let mut v = serde_json::json!({
                        "tasks": all.iter().map(fmt::task_json).collect::<Vec<_>>(),
                    });
                    if after > 0 {
                        v["next_after"] = after.to_string().into();
                    }
                    print_json(&v);
                }
                Output::Text => {
                    let now = fmt::now_ns();
                    let rows: Vec<Vec<String>> =
                        all.iter().map(|t| fmt::task_row(t, now)).collect();
                    println!("{}", fmt::table(&fmt::TASK_HEADER, &rows));
                    if after > 0 {
                        eprintln!("more tasks follow: --after {after}, or --all");
                    }
                }
            }
        }
        Command::Workers => {
            let r = rpc(admin.list_workers(pb::Empty {}).await)?;
            warn_missing(&r.unavailable_shards);
            match out {
                Output::Json => print_json(&serde_json::json!({
                    "workers": r.workers.iter().map(fmt::worker_json).collect::<Vec<_>>(),
                })),
                Output::Text => {
                    let now = fmt::now_ns();
                    let rows: Vec<Vec<String>> = r
                        .workers
                        .iter()
                        .map(|w| {
                            vec![
                                w.id.to_string(),
                                w.name.clone(),
                                w.pool.clone(),
                                format!("{}/{}", w.busy, w.slots),
                                fmt::labels(&w.labels),
                                fmt::ago(w.connected_unix_ns, now),
                            ]
                        })
                        .collect();
                    println!(
                        "{}",
                        fmt::table(
                            &["ID", "NAME", "POOL", "BUSY", "LABELS", "CONNECTED"],
                            &rows
                        )
                    );
                }
            }
        }
        Command::Schedules => {
            let r = rpc(admin.list_recurring(pb::Empty {}).await)?;
            warn_missing(&r.unavailable_shards);
            match out {
                Output::Json => print_json(&serde_json::json!({
                    "schedules": r.schedules.iter().map(fmt::schedule_json).collect::<Vec<_>>(),
                })),
                Output::Text => {
                    let rows: Vec<Vec<String>> = r
                        .schedules
                        .iter()
                        .map(|s| {
                            vec![
                                s.name.clone(),
                                s.cron.clone(),
                                if s.tz.is_empty() {
                                    "UTC".into()
                                } else {
                                    s.tz.clone()
                                },
                                s.pool.clone(),
                                fmt::overlap(s.overlap).to_string(),
                                s.next_unix_ns.map(fmt::time).unwrap_or_else(|| "-".into()),
                                s.last_task_id.map(|i| i.to_string()).unwrap_or_default(),
                            ]
                        })
                        .collect();
                    println!(
                        "{}",
                        fmt::table(
                            &["NAME", "CRON", "TZ", "POOL", "OVERLAP", "NEXT", "LAST TASK"],
                            &rows
                        )
                    );
                }
            }
        }
        Command::Schedule(ScheduleCommand::Add {
            name,
            cron,
            tz,
            overlap,
            key,
            spec,
        }) => {
            let mut template = spec.to_pb()?;
            template.key = key.unwrap_or_default();
            rpc(admin
                .add_recurring(pb::RecurringRequest {
                    name: name.clone(),
                    cron,
                    tz: tz.unwrap_or_default(),
                    template: Some(template),
                    overlap: match overlap {
                        OverlapArg::Skip => pb::Overlap::Skip,
                        OverlapArg::Allow => pb::Overlap::Allow,
                        OverlapArg::Queue => pb::Overlap::Queue,
                        OverlapArg::QueueOnSuccess => pb::Overlap::QueueOnSuccess,
                    } as i32,
                })
                .await)?;
            if out == Output::Json {
                print_json(&serde_json::json!({ "name": name }));
            }
        }
        Command::Schedule(ScheduleCommand::Remove { name }) => {
            let r = rpc(admin
                .remove_recurring(pb::RemoveRecurringRequest { name })
                .await)?;
            match out {
                Output::Json => print_json(&serde_json::json!({ "removed": r.removed })),
                Output::Text if r.removed => println!("removed"),
                Output::Text => println!("no such schedule"),
            }
        }
        Command::Ordering(OrderingCommand::Get { key, pool }) => {
            let i = rpc(admin.get_ordering(pb::OrderingRequest { pool, key }).await)?;
            print_ordering(out, &i);
        }
        Command::Ordering(OrderingCommand::Halted { pool, limit }) => {
            let r = rpc(admin
                .list_halted_orderings(pb::ListHaltedOrderingsRequest { pool, limit })
                .await)?;
            match out {
                Output::Json => print_json(&serde_json::json!({ "keys": r.keys })),
                Output::Text => {
                    for k in &r.keys {
                        println!("{}", fmt::clean(k, false));
                    }
                }
            }
        }
        Command::Ordering(OrderingCommand::Floor {
            key,
            class,
            clear: _,
            persist,
            pool,
        }) => {
            rpc(admin
                .set_ordering_floor(pb::SetOrderingFloorRequest {
                    pool: pool.clone(),
                    key: key.clone(),
                    floor: class.map(|c| c.to_pb() as i32),
                    persist,
                })
                .await)?;
            let i = rpc(admin.get_ordering(pb::OrderingRequest { pool, key }).await)?;
            print_ordering(out, &i);
        }
        Command::Resume { ordering_key, pool } => {
            let r = rpc(admin.resume(pb::ResumeRequest { pool, ordering_key }).await)?;
            match out {
                Output::Json => print_json(&serde_json::json!({ "resumed": r.resumed })),
                Output::Text if r.resumed => println!("resumed"),
                Output::Text => println!("was not halted"),
            }
        }
        Command::Snapshot => {
            let s = rpc(admin.snapshot(pb::Empty {}).await)?;
            warn_missing(&s.unavailable_shards);
            match out {
                Output::Json => print_json(&fmt::snapshot_json(&s)),
                Output::Text => println!("{}", top::pools_table(&s)),
            }
        }
        Command::Watch { pools, all } => {
            let mut stream = rpc(tasks
                .watch(pb::WatchRequest {
                    pools,
                    all_transitions: all,
                    ..Default::default()
                })
                .await)?;
            loop {
                let ev = tokio::select! {
                    ev = stream.next() => ev,
                    _ = tokio::signal::ctrl_c() => break,
                };
                let Some(ev) = ev else { break };
                let ev = ev.map_err(|s| anyhow::anyhow!("{}", conn::describe(&s)))?;
                match out {
                    Output::Json => println!(
                        "{}",
                        serde_json::json!({
                            "id": ev.id.to_string(), "pool": ev.pool, "key": ev.key,
                            "status": fmt::status(ev.status), "attempt": ev.attempt,
                            "at_unix_ns": ev.at_unix_ns.to_string(), "message": ev.message,
                            "priority": fmt::priority(ev.priority),
                            "updated": ev.kind == pb::EventKind::Updated as i32,
                            "kind": fmt::event_kind(ev.kind).unwrap_or("transition"),
                        })
                    ),
                    Output::Text => {
                        // An update, replacement or successor is not a change of status.
                        let note = fmt::event_kind(ev.kind);
                        let mut line = format!(
                            "{}  {:<13} {}  pool={}",
                            fmt::time(ev.at_unix_ns),
                            note.unwrap_or_else(|| fmt::status(ev.status)),
                            ev.id,
                            ev.pool
                        );
                        if note.is_some() {
                            line.push_str(&format!(
                                " status={} priority={}",
                                fmt::status(ev.status),
                                fmt::priority(ev.priority).unwrap_or("-")
                            ));
                        }
                        if !ev.key.is_empty() {
                            line.push_str(&format!(" key={}", ev.key));
                        }
                        if ev.attempt > 0 {
                            line.push_str(&format!(" attempt={}", ev.attempt));
                        }
                        if !ev.message.is_empty() {
                            line.push_str(&format!("  {}", ev.message));
                        }
                        println!("{}", fmt::clean(&line, false));
                    }
                }
            }
        }
        Command::Top(a) => top::run(tasks, admin, cli.conn.server.clone(), a).await?,
    }
    Ok(ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    match rt.block_on(run(cli)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {}", fmt::clean(&format!("{e:#}"), true));
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_command_line_is_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
        // Global options must not collide with subcommand options of the same name.
        let lines: [&[&str]; 5] = [
            &["get", "--key", "report"],
            &[
                "submit", "--key", "k", "--label", "gpu=a100", "--wait", "payload",
            ],
            &[
                "schedule",
                "add",
                "nightly",
                "0 0 3 * * *",
                "--key",
                "report",
                "--overlap",
                "queue",
            ],
            &[
                "--tls-ca",
                "ca.pem",
                "--tls-cert",
                "c.pem",
                "--tls-key",
                "k.pem",
                "tasks",
                "--all",
            ],
            &["-o", "json", "top", "--interval", "0.5", "--pool", "a"],
        ];
        for line in lines {
            let args = std::iter::once("cronoq").chain(line.iter().copied());
            let cli = Cli::try_parse_from(args).unwrap_or_else(|e| panic!("{line:?}: {e}"));
            // Reading the values is where mismatched definitions panic.
            let _ = format!("{cli:?}");
        }
        assert!(Cli::try_parse_from(["cronoq", "--tls-key", "k.pem", "tasks"]).is_err());
    }

    #[test]
    fn task_refs() {
        let r = |task: &str, key| {
            RefArgs {
                task: task.into(),
                key,
            }
            .to_pb()
        };
        assert_eq!(
            r("17", false).unwrap().target,
            Some(pb::task_ref::Target::Id(17))
        );
        assert_eq!(
            r("17", true).unwrap().target,
            Some(pb::task_ref::Target::Key("17".into()))
        );
        assert!(r("report-1", false).is_err());
    }

    #[test]
    fn json_payloads_are_checked() {
        let spec = |payload: &str, json| SpecArgs {
            payload: Some(payload.into()),
            file: None,
            json,
            content_type: None,
            pool: "default".into(),
            kind: String::new(),
            priority: PriorityArg::Normal,
            tenant: String::new(),
            labels: vec![],
            tags: vec![],
            resources: vec![],
            deadline_in: None,
            timeout_s: None,
        };
        assert_eq!(
            spec("{\"a\":1}", true).to_pb().unwrap().content_type,
            "application/json"
        );
        assert!(spec("{not json", true).to_pb().is_err());
        assert_eq!(
            spec("hi", false).to_pb().unwrap().content_type,
            "text/plain; charset=utf-8"
        );
    }
}
