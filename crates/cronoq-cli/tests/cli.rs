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
