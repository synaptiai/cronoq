//! Active-passive failover with the Postgres store.

#[path = "../../cronoq-store-postgres/tests/common/mod.rs"]
mod pgtest;

use std::time::Duration;

use bytes::Bytes;
use cronoq_proto::v1 as pb;
use cronoq_server::worker::{WorkerOptions, run_worker};
use cronoq_server::{Bound, Config};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Node {
    http: String,
    grpc: String,
    stop: Option<oneshot::Sender<()>>,
    bound: Option<oneshot::Receiver<Bound>>,
    join: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Node {
    fn start(db: &str) -> Node {
        let (gp, hp) = (free_port(), free_port());
        let cfg = Config::parse(&format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:{gp}"
            http_addr = "127.0.0.1:{hp}"
            shutdown_grace_s = 0
            [store]
            kind = "postgres"
            url = "{db}"
            [[pools]]
            name = "default"
            max = 4
            lease_ttl_s = 2
            "#
        ))
        .unwrap();
        // reqwest is built without a bundled crypto provider; the tests use ring like the server.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (bound_tx, bound_rx) = oneshot::channel();
        let join = tokio::spawn(cronoq_server::run(
            cfg,
            async {
                let _ = stop_rx.await;
            },
            move |b| {
                let _ = bound_tx.send(b);
            },
        ));
        Node {
            http: format!("http://127.0.0.1:{hp}"),
            grpc: format!("http://127.0.0.1:{gp}"),
            stop: Some(stop_tx),
            bound: Some(bound_rx),
            join,
        }
    }

    async fn leader(db: &str) -> Node {
        let mut n = Node::start(db);
        if tokio::time::timeout(Duration::from_secs(20), n.bound.take().unwrap())
            .await
            .unwrap()
            .is_err()
        {
            panic!("server failed: {:?}", n.join.await);
        }
        n
    }

    async fn standby(db: &str) -> Node {
        let n = Node::start(db);
        for _ in 0..200 {
            if n.ready().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(n.ready().await, Some(503));
        n
    }

    async fn ready(&self) -> Option<u16> {
        reqwest::get(format!("{}/readyz", self.http))
            .await
            .ok()
            .map(|r| r.status().as_u16())
    }

    async fn tasks(&self) -> pb::tasks_client::TasksClient<tonic::transport::Channel> {
        let ch = tonic::transport::Endpoint::from_shared(self.grpc.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        pb::tasks_client::TasksClient::new(ch)
    }

    /// Stop and require `run()` to return within `t`.
    async fn stop_within(&mut self, t: Duration) -> anyhow::Result<()> {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(t, &mut self.join)
            .await
            .expect("run() did not return in time")
            .unwrap()
    }
}

fn worker(node: &Node) -> oneshot::Sender<()> {
    let (tx, rx) = oneshot::channel::<()>();
    let opts = WorkerOptions::new(node.grpc.clone(), "default").slots(2);
    tokio::spawn(run_worker(
        opts,
        |lease| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Bytes::from(lease.payload.to_ascii_uppercase()))
        },
        async {
            let _ = rx.await;
        },
    ));
    tx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_takes_over_and_finishes_accepted_work() {
    let Some(pg) = pgtest::TestPg::start() else {
        return;
    };
    let mut a = Node::leader(&pg.url).await;
    let mut b = Node::standby(&pg.url).await;
    assert_eq!(a.ready().await, Some(200));

    // Accept work on A with no workers connected, so nothing runs yet.
    let http = reqwest::Client::new();
    let mut ids = Vec::new();
    for i in 0..10 {
        let v: serde_json::Value = http
            .post(format!("{}/v1/tasks", a.http))
            .json(&serde_json::json!({"payload": format!("task-{i}"), "key": format!("k{i}")}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.push(v["id"].as_str().unwrap().parse::<u64>().unwrap());
    }

    // Leader goes away; B takes over.
    a.stop_within(Duration::from_secs(10)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), b.bound.take().unwrap())
        .await
        .expect("B became leader")
        .unwrap();
    assert_eq!(b.ready().await, Some(200));

    let _w = worker(&b);
    let mut c = b.tasks().await;
    for (i, id) in ids.iter().enumerate() {
        let r = c
            .wait(pb::WaitRequest {
                task: Some(pb::TaskRef {
                    target: Some(pb::task_ref::Target::Id(*id)),
                }),
                timeout_s: Some(20.0),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            r.status,
            pb::TaskStatus::Succeeded as i32,
            "task {id}: {r:?}"
        );
        assert_eq!(
            r.output.unwrap(),
            serde_json::to_vec(&format!("TASK-{i}"))
                .unwrap()
                .to_ascii_uppercase()
        );
    }
    // Keys survive the failover: resubmitting is deduplicated.
    let v: serde_json::Value = http
        .post(format!("{}/v1/tasks", b.http))
        .json(&serde_json::json!({"payload": "again", "key": "k0"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["duplicate"], true);
    b.stop_within(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_answers_unavailable_and_honors_shutdown() {
    let Some(pg) = pgtest::TestPg::start() else {
        return;
    };
    let mut a = Node::leader(&pg.url).await;
    let mut b = Node::standby(&pg.url).await;
    let e = b
        .tasks()
        .await
        .submit(pb::SubmitRequest {
            task: Some(pb::TaskSpec::default()),
            wait: false,
            wait_timeout_s: None,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    let r = reqwest::Client::new()
        .post(format!("{}/v1/tasks", b.http))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    // Before the fix, a standby ignored shutdown while waiting for leadership.
    b.stop_within(Duration::from_secs(5)).await.unwrap();
    a.stop_within(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_http_client_does_not_block_promotion() {
    let Some(pg) = pgtest::TestPg::start() else {
        return;
    };
    let mut a = Node::leader(&pg.url).await;
    let mut b = Node::standby(&pg.url).await;
    let mut sock = tokio::net::TcpStream::connect(b.http.trim_start_matches("http://"))
        .await
        .unwrap();
    sock.write_all(b"GET /readyz HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap(); // never finishes headers
    a.stop_within(Duration::from_secs(10)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), b.bound.take().unwrap())
        .await
        .expect("B promoted")
        .unwrap();
    assert_eq!(b.ready().await, Some(200));
    drop(sock);
    b.stop_within(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deposed_leader_stops_and_releases_workers() {
    let Some(pg) = pgtest::TestPg::start() else {
        return;
    };
    let psql = std::path::Path::new("/usr/lib/postgresql/16/bin/psql");
    if !psql.exists() {
        return;
    }
    let mut a = Node::leader(&pg.url).await;
    let opts = WorkerOptions::new(a.grpc.clone(), "default")
        .slots(1)
        .reconnect(false);
    let w = tokio::spawn(run_worker(
        opts,
        |l| async move { Ok(l.payload) },
        std::future::pending(),
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let out = std::process::Command::new(psql)
        .arg(&pg.url)
        .arg("-Atc")
        .arg("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory'")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let r = tokio::time::timeout(Duration::from_secs(15), &mut a.join)
        .await
        .expect("deposed leader returned")
        .unwrap();
    assert!(r.is_err(), "losing leadership is an error");
    assert_eq!(a.ready().await, None, "listeners are closed");
    // The worker was told to go away (and, with reconnect disabled, returns).
    let wr = tokio::time::timeout(Duration::from_secs(5), w)
        .await
        .expect("worker released")
        .unwrap();
    assert!(wr.unwrap_err().to_string().contains("leader"));
}
