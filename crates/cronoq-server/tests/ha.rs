//! Active-passive failover with the Postgres store.

#[path = "../../cronoq-store-postgres/tests/common/mod.rs"]
mod pgtest;

use std::time::Duration;

use bytes::Bytes;
use cronoq_proto::v1 as pb;
use cronoq_server::worker::{WorkerOptions, run_worker};
use cronoq_server::{Bound, Config};
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

async fn ready(node: &Node) -> Option<u16> {
    reqwest::get(format!("{}/readyz", node.http))
        .await
        .ok()
        .map(|r| r.status().as_u16())
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
    let mut a = start(&pg.url);
    if tokio::time::timeout(Duration::from_secs(20), a.bound.take().unwrap())
        .await
        .unwrap()
        .is_err()
    {
        panic!("server A failed: {:?}", a.join.await);
    }
    let mut b = start(&pg.url);
    // B is up but standing by.
    let mut b_status = None;
    for _ in 0..100 {
        b_status = ready(&b).await;
        if b_status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(b_status, Some(503));
    assert_eq!(ready(&a).await, Some(200));

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
        ids.push(v["id"].as_u64().unwrap());
    }

    // Leader goes away; B takes over.
    let _ = a.stop.take().unwrap().send(());
    let _ = tokio::time::timeout(Duration::from_secs(10), a.join).await;
    tokio::time::timeout(Duration::from_secs(20), b.bound.take().unwrap())
        .await
        .expect("B became leader")
        .unwrap();
    assert_eq!(ready(&b).await, Some(200));

    let _w = worker(&b);
    let ch = tonic::transport::Endpoint::from_shared(b.grpc.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = pb::tasks_client::TasksClient::new(ch);
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
    let _ = b.stop.take().unwrap().send(());
    let _ = tokio::time::timeout(Duration::from_secs(10), b.join).await;
}
