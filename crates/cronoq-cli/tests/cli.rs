//! The `cronoq` binary against a server started in this process.

use std::process::Output;
use std::time::Duration;

use bytes::Bytes;
use cronoq_server::worker::{WorkerOptions, run_worker};
use cronoq_server::{Bound, Config};
use tokio::sync::oneshot;

const CLIENT: &str = "client-token-0123456789abcdef0123";
const WORKER: &str = "worker-token-0123456789abcdef0123";
const ADMIN: &str = "admin-token-0123456789abcdef012345";
const METRICS: &str = "metrics-token-0123456789abcdef0123";

const CONFIG: &str = r#"
[server]
grpc_addr = "127.0.0.1:0"
http_addr = "127.0.0.1:0"
shutdown_grace_s = 0.5

[[auth.tokens]]
token = "client-token-0123456789abcdef0123"
role = "client"
[[auth.tokens]]
token = "worker-token-0123456789abcdef0123"
role = "worker"
[[auth.tokens]]
token = "admin-token-0123456789abcdef012345"
role = "admin"
[[auth.tokens]]
token = "metrics-token-0123456789abcdef0123"
role = "metrics"

[[pools]]
name = "default"
max = 4
lease_ttl_s = 2
"#;

struct Server {
    bound: Bound,
    stop: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Server {
    async fn start() -> Server {
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (bound_tx, bound_rx) = oneshot::channel();
        let join = tokio::spawn(cronoq_server::run(
            Config::parse(CONFIG).unwrap(),
            async {
                let _ = stop_rx.await;
            },
            move |b| {
                let _ = bound_tx.send(b);
            },
        ));
        let bound = tokio::time::timeout(Duration::from_secs(10), bound_rx)
            .await
            .expect("server started")
            .unwrap();
        Server {
            bound,
            stop: Some(stop_tx),
            join,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.bound.grpc)
    }

    /// Run `cronoq` with `token` and `args`.
    async fn cli(&self, token: &str, args: &[&str]) -> Output {
        let (url, token) = (self.url(), token.to_string());
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_cronoq"))
                .args(&args)
                .env("CRONOQ_SERVER", url)
                .env("CRONOQ_TOKEN", token)
                .output()
                .unwrap()
        })
        .await
        .unwrap()
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut self.join).await;
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn json(o: &Output) -> serde_json::Value {
    assert!(o.status.success(), "{}", stderr(o));
    serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("{e}: {}", stdout(o)))
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_can_be_submitted_inspected_listed_and_cancelled() {
    let s = Server::start().await;
    let o = s
        .cli(
            CLIENT,
            &["submit", "hello", "--key", "greeting", "--tenant", "acme"],
        )
        .await;
    assert!(o.status.success(), "{}", stderr(&o));
    let id = stdout(&o).trim().to_string();
    assert!(id.parse::<u64>().is_ok(), "{id}");
    // No worker is connected: the task stays queued.
    let o = s.cli(CLIENT, &["get", &id]).await;
    assert!(o.status.success(), "{}", stderr(&o));
    let text = stdout(&o);
    assert!(text.contains("status:    ready"), "{text}");
    assert!(text.contains("tenant:    acme"), "{text}");
    let v = json(
        &s.cli(CLIENT, &["-o", "json", "get", "--key", "greeting"])
            .await,
    );
    assert_eq!(v["id"], id);
    assert_eq!(v["key"], "greeting");
    // Submitting the same key again returns the same task.
    let o = s
        .cli(CLIENT, &["submit", "again", "--key", "greeting"])
        .await;
    assert_eq!(stdout(&o).trim(), format!("{id} (duplicate)"));

    let o = s.cli(CLIENT, &["tasks"]).await;
    let text = stdout(&o);
    assert!(text.starts_with("ID"), "{text}");
    assert!(text.contains(&id) && text.contains("greeting"), "{text}");
    let v = json(
        &s.cli(CLIENT, &["-o", "json", "tasks", "--status", "ready"])
            .await,
    );
    assert_eq!(v["tasks"][0]["id"], id);
    let v = json(
        &s.cli(CLIENT, &["-o", "json", "tasks", "--status", "failed"])
            .await,
    );
    assert_eq!(v["tasks"].as_array().unwrap().len(), 0);
    let o = s.cli(CLIENT, &["tasks", "--status", "nope"]).await;
    assert!(!o.status.success());
    assert!(
        stderr(&o).contains("unknown status 'nope'"),
        "{}",
        stderr(&o)
    );

    // Another client's key with an escape sequence is shown, not sent to the terminal.
    let o = s
        .cli(CLIENT, &["submit", "x", "--key", "evil\x1b[2Kkey"])
        .await;
    let evil = stdout(&o).trim().to_string();
    for args in [vec!["tasks"], vec!["get", evil.as_str()]] {
        let o = s.cli(ADMIN, &args).await;
        assert!(!o.stdout.contains(&0x1b), "{args:?}: {}", stdout(&o));
        assert!(
            stdout(&o).contains("evil\\u{1b}[2Kkey"),
            "{args:?}: {}",
            stderr(&o)
        );
    }

    // Latest wins: the queued task takes the new payload and keeps its id.
    let o = s
        .cli(
            CLIENT,
            &[
                "submit",
                "newer",
                "--key",
                "greeting",
                "--tenant",
                "acme",
                "--replace",
            ],
        )
        .await;
    assert_eq!(
        stdout(&o).trim(),
        format!("{id} (replaced)"),
        "{}",
        stderr(&o)
    );
    let v = json(
        &s.cli(
            CLIENT,
            &[
                "-o",
                "json",
                "submit",
                "newest",
                "--key",
                "greeting",
                "--tenant",
                "acme",
                "--replace",
                "--renew-position",
            ],
        )
        .await,
    );
    assert_eq!(v["id"], id);
    assert_eq!(v["outcome"], "replaced");
    let o = s.cli(CLIENT, &["submit", "x", "--replace"]).await;
    assert_eq!(o.status.code(), Some(2), "--replace needs --key");
    // A replacement must belong to the same tenant as the task.
    let o = s
        .cli(CLIENT, &["submit", "x", "--key", "greeting", "--replace"])
        .await;
    assert!(!o.status.success());
    assert!(stderr(&o).contains("the tenant differs"), "{}", stderr(&o));

    // A key reused with other content is reported on request; a retry of the same
    // submission is still a duplicate.
    let same = [
        "submit",
        "newest",
        "--key",
        "greeting",
        "--tenant",
        "acme",
        "--reject-conflict",
    ];
    let o = s.cli(CLIENT, &same).await;
    assert_eq!(
        stdout(&o).trim(),
        format!("{id} (duplicate)"),
        "{}",
        stderr(&o)
    );
    let o = s
        .cli(
            CLIENT,
            &["submit", "other", "--key", "greeting", "--reject-conflict"],
        )
        .await;
    assert!(!o.status.success());
    assert!(
        stderr(&o).contains(&format!("the key belongs to task {id} (ready)")),
        "{}",
        stderr(&o)
    );
    let o = s
        .cli(
            CLIENT,
            &[
                "submit",
                "x",
                "--key",
                "greeting",
                "--replace",
                "--reject-conflict",
            ],
        )
        .await;
    assert_eq!(o.status.code(), Some(2));
    // Asking for the existing task whatever it holds.
    let o = s
        .cli(
            CLIENT,
            &["submit", "other", "--key", "greeting", "--return-existing"],
        )
        .await;
    assert_eq!(
        stdout(&o).trim(),
        format!("{id} (duplicate)"),
        "{}",
        stderr(&o)
    );

    // Update: by id and by key, then nothing left to change, then nothing asked.
    let o = s
        .cli(
            CLIENT,
            &["update", &id, "--priority", "high", "--deadline-in", "60"],
        )
        .await;
    assert_eq!(stdout(&o).trim(), "updated", "{}", stderr(&o));
    let v = json(&s.cli(CLIENT, &["-o", "json", "get", &id]).await);
    assert_eq!(v["priority"], "high");
    let v = json(
        &s.cli(
            CLIENT,
            &[
                "-o",
                "json",
                "update",
                "greeting",
                "--key",
                "--priority",
                "high",
            ],
        )
        .await,
    );
    assert_eq!(v["id"], id);
    assert_eq!(v["status"], "ready");
    assert_eq!(v["changed"], false);
    assert_eq!(v["deferred"], false);
    let o = s
        .cli(CLIENT, &["update", &id, "--tag", "a=1", "--no-tags"])
        .await;
    assert_eq!(o.status.code(), Some(2));
    let o = s.cli(CLIENT, &["update", &id]).await;
    assert!(!o.status.success());
    assert!(stderr(&o).contains("nothing to change"), "{}", stderr(&o));
    let o = s
        .cli(CLIENT, &["update", "999999", "--priority", "low"])
        .await;
    assert!(!o.status.success());

    let o = s.cli(CLIENT, &["cancel", &id]).await;
    assert_eq!(stdout(&o).trim(), "cancelled");
    // Waiting on a task that did not succeed exits with 1.
    let o = s.cli(CLIENT, &["wait", &id]).await;
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("cancelled"), "{}", stderr(&o));
    // A key without --key is refused before anything is sent.
    let o = s.cli(CLIENT, &["get", "greeting"]).await;
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("pass --key"), "{}", stderr(&o));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn results_workers_schedules_and_roles() {
    let s = Server::start().await;
    let (stop_worker, stopped) = oneshot::channel::<()>();
    let worker = tokio::spawn(run_worker(
        WorkerOptions::new(s.url(), "default")
            .token(WORKER)
            .name("cli-test-worker")
            .label("zone", "eu"),
        |lease| async move { Ok(Bytes::from(lease.payload.to_ascii_uppercase())) },
        async {
            let _ = stopped.await;
        },
    ));
    // Wait for the worker to register.
    for _ in 0..200 {
        let v = json(&s.cli(METRICS, &["-o", "json", "workers"]).await);
        if !v["workers"].as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let o = s.cli(CLIENT, &["submit", "shout", "--wait"]).await;
    assert!(o.status.success(), "{}", stderr(&o));
    assert_eq!(stdout(&o), "SHOUT");
    let v = json(
        &s.cli(
            CLIENT,
            &["-o", "json", "submit", "{\"a\":1}", "--json", "--wait"],
        )
        .await,
    );
    assert_eq!(v["status"], "succeeded");
    assert_eq!(v["output"], serde_json::json!({ "A": 1 }));

    let o = s.cli(METRICS, &["workers"]).await;
    let text = stdout(&o);
    assert!(
        text.contains("cli-test-worker") && text.contains("zone=eu"),
        "{text}"
    );
    let o = s.cli(METRICS, &["snapshot"]).await;
    assert!(stdout(&o).contains("default"), "{}", stdout(&o));
    let v = json(&s.cli(METRICS, &["-o", "json", "snapshot"]).await);
    assert_eq!(v["pools"][0]["name"], "default");
    assert_eq!(v["totals"]["succeeded"], 2);

    let o = s
        .cli(
            ADMIN,
            &[
                "schedule",
                "add",
                "hourly",
                "0 0 * * * *",
                "tick",
                "--overlap",
                "queue",
            ],
        )
        .await;
    assert!(o.status.success(), "{}", stderr(&o));
    let o = s.cli(METRICS, &["schedules"]).await;
    let text = stdout(&o);
    assert!(text.contains("hourly") && text.contains("queue"), "{text}");
    let o = s.cli(ADMIN, &["schedule", "remove", "hourly"]).await;
    assert_eq!(stdout(&o).trim(), "removed");

    // Roles: a client token cannot list workers, a metrics token cannot add schedules.
    let o = s.cli(CLIENT, &["workers"]).await;
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("permission_denied"), "{}", stderr(&o));
    let o = s
        .cli(METRICS, &["schedule", "add", "x", "0 0 * * * *"])
        .await;
    assert!(stderr(&o).contains("permission_denied"), "{}", stderr(&o));

    let _ = stop_worker.send(());
    let _ = worker.await;
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_settings_are_checked() {
    let s = Server::start().await;
    let o = s.cli(CLIENT, &["--tls-ca", "ca.pem", "tasks"]).await;
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("need an https://"), "{}", stderr(&o));
    // A wrong token is refused by the server.
    let o = s.cli("not-a-valid-token-at-all-0000000", &["tasks"]).await;
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("unauthenticated"), "{}", stderr(&o));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_keys_are_submitted_shown_halted_and_resumed() {
    let s = Server::start().await;
    // A task that fails and halts its key, and one behind it. No worker yet.
    let bad = json(
        &s.cli(
            CLIENT,
            &[
                "-o",
                "json",
                "submit",
                "fail",
                "--ordering-key",
                "acct",
                "--halt-on-failure",
            ],
        )
        .await,
    );
    let next = json(
        &s.cli(
            CLIENT,
            &["-o", "json", "submit", "next", "--ordering-key", "acct"],
        )
        .await,
    );
    // What cannot be combined is refused before the server is asked.
    let o = s.cli(CLIENT, &["submit", "x", "--halt-on-failure"]).await;
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    let o = s
        .cli(
            CLIENT,
            &["submit", "x", "--ordering-key", "k", "--start-in", "60"],
        )
        .await;
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));

    let v = json(
        &s.cli(METRICS, &["-o", "json", "ordering", "get", "acct"])
            .await,
    );
    assert_eq!(
        (&v["tasks"], &v["head"]),
        (&serde_json::json!(2), &bad["id"])
    );
    assert_eq!(v["halted"], false);
    let v = json(&s.cli(METRICS, &["-o", "json", "snapshot"]).await);
    assert_eq!(v["pools"][0]["ordering"]["waiting"], 1);

    // A floor: shown, kept, removed.
    let o = s
        .cli(ADMIN, &["ordering", "floor", "acct", "high", "--persist"])
        .await;
    assert!(o.status.success(), "{}", stderr(&o));
    let text = stdout(&o);
    assert!(
        text.contains("floor      high (kept)") && text.contains("runs at    high"),
        "{text}"
    );
    let o = s.cli(METRICS, &["ordering", "floor", "acct", "high"]).await;
    assert!(stderr(&o).contains("permission_denied"), "{}", stderr(&o));
    let v = json(
        &s.cli(
            ADMIN,
            &["-o", "json", "ordering", "floor", "acct", "--clear"],
        )
        .await,
    );
    assert_eq!(v["floor"], serde_json::Value::Null);

    // The worker fails the first task: the key is halted and the next task is not run.
    let (stop_worker, stopped) = oneshot::channel::<()>();
    let worker = tokio::spawn(run_worker(
        WorkerOptions::new(s.url(), "default").token(WORKER),
        |lease| async move {
            if &lease.payload[..] == b"fail" {
                return Err(cronoq::TaskError::permanent("boom"));
            }
            Ok(Bytes::from(lease.payload.to_ascii_uppercase()))
        },
        async {
            let _ = stopped.await;
        },
    ));
    let id = bad["id"].as_str().unwrap().to_string();
    let o = s.cli(CLIENT, &["wait", &id, "--timeout", "10"]).await;
    assert!(!o.status.success());
    let mut halted = String::new();
    for _ in 0..200 {
        halted = stdout(&s.cli(METRICS, &["ordering", "halted"]).await);
        if halted.trim() == "acct" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(halted.trim(), "acct");
    let text = stdout(&s.cli(METRICS, &["ordering", "get", "acct"]).await);
    assert!(text.contains("halted     yes"), "{text}");
    let next_id = next["id"].as_str().unwrap().to_string();
    let v = json(&s.cli(CLIENT, &["-o", "json", "get", &next_id]).await);
    assert_eq!(v["status"], "ready");

    let o = s.cli(CLIENT, &["resume", "--ordering-key", "acct"]).await;
    assert!(stderr(&o).contains("permission_denied"), "{}", stderr(&o));
    let o = s.cli(ADMIN, &["resume", "--ordering-key", "acct"]).await;
    assert_eq!(stdout(&o).trim(), "resumed", "{}", stderr(&o));
    let o = s.cli(ADMIN, &["resume", "--ordering-key", "acct"]).await;
    assert_eq!(stdout(&o).trim(), "was not paused or halted");
    let o = s.cli(CLIENT, &["wait", &next_id, "--timeout", "10"]).await;
    assert_eq!(stdout(&o), "NEXT", "{}", stderr(&o));

    let _ = stop_worker.send(());
    let _ = worker.await;
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_a_class_and_an_ordering_key_are_paused_and_resumed() {
    let s = Server::start().await;
    let o = s.cli(CLIENT, &["pause", "--reason", "x"]).await;
    assert!(stderr(&o).contains("permission_denied"), "{}", stderr(&o));
    // A reason is required, and a class and a key exclude each other.
    let o = s.cli(ADMIN, &["pause"]).await;
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    let o = s
        .cli(
            ADMIN,
            &[
                "pause",
                "--reason",
                "x",
                "--class",
                "low",
                "--ordering-key",
                "k",
            ],
        )
        .await;
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));

    let o = s.cli(ADMIN, &["pause", "--reason", "deploy 14:00"]).await;
    assert_eq!(stdout(&o).trim(), "paused", "{}", stderr(&o));
    let o = s.cli(ADMIN, &["pause", "--reason", "deploy 14:00"]).await;
    assert_eq!(stdout(&o).trim(), "was already paused");
    let o = s
        .cli(
            ADMIN,
            &["pause", "--class", "low", "--reason", "backfill off"],
        )
        .await;
    assert_eq!(stdout(&o).trim(), "paused", "{}", stderr(&o));
    let o = s
        .cli(
            ADMIN,
            &[
                "pause",
                "--ordering-key",
                "acct",
                "--reason",
                "customer asked",
            ],
        )
        .await;
    assert_eq!(stdout(&o).trim(), "paused", "{}", stderr(&o));

    let v = json(&s.cli(METRICS, &["-o", "json", "snapshot"]).await);
    assert_eq!(v["pools"][0]["paused"], "deploy 14:00");
    assert_eq!(v["pools"][0]["paused_classes"][0]["class"], "low");
    let text = stdout(&s.cli(METRICS, &["ordering", "paused"]).await);
    assert!(
        text.contains("acct") && text.contains("customer asked"),
        "{text}"
    );
    let text = stdout(&s.cli(METRICS, &["ordering", "get", "acct"]).await);
    assert!(text.contains("paused     yes (customer asked)"), "{text}");

    for args in [
        &["resume"][..],
        &["resume", "--class", "low"],
        &["resume", "--ordering-key", "acct"],
    ] {
        let o = s.cli(ADMIN, args).await;
        assert_eq!(stdout(&o).trim(), "resumed", "{args:?}: {}", stderr(&o));
    }
    let o = s.cli(ADMIN, &["resume"]).await;
    assert_eq!(stdout(&o).trim(), "was not paused or halted");
    let v = json(&s.cli(METRICS, &["-o", "json", "snapshot"]).await);
    assert_eq!(v["pools"][0]["paused"], serde_json::Value::Null);
    s.stop().await;
}
