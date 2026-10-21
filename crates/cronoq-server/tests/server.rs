use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cronoq::TaskError;
use cronoq_proto::v1 as pb;
use cronoq_server::worker::{WorkerOptions, run_worker};
use cronoq_server::{Bound, Config};
use pb::tasks_client::TasksClient;
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;

const CLIENT: &str = "client-secret";
const WORKER: &str = "worker-secret";
const ADMIN: &str = "admin-secret";

struct TestServer {
    bound: Bound,
    stop: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    async fn start(extra: &str) -> TestServer {
        let cfg = Config::parse(&format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:0"
            http_addr = "127.0.0.1:0"
            shutdown_grace_s = 1
            [[auth.tokens]]
            token = "{CLIENT}"
            role = "client"
            [[auth.tokens]]
            token = "{WORKER}"
            role = "worker"
            [[auth.tokens]]
            token = "{ADMIN}"
            role = "admin"
            [[pools]]
            name = "default"
            max = 8
            lease_ttl_s = 2
            retry = {{ max_attempts = 3, initial_backoff_s = 0.05, jitter = false }}
            {extra}
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
        let bound = tokio::time::timeout(Duration::from_secs(10), bound_rx)
            .await
            .unwrap()
            .unwrap();
        TestServer {
            bound,
            stop: Some(stop_tx),
            join,
        }
    }

    fn grpc_url(&self) -> String {
        format!("http://{}", self.bound.grpc)
    }

    fn http_url(&self, path: &str) -> String {
        format!("http://{}{}", self.bound.http, path)
    }

    async fn client(
        &self,
        token: &str,
    ) -> TasksClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            impl tonic::service::Interceptor + use<>,
        >,
    > {
        let ch = tonic::transport::Endpoint::from_shared(self.grpc_url())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let v: MetadataValue<_> = format!("Bearer {token}").parse().unwrap();
        TasksClient::with_interceptor(ch, move |mut r: tonic::Request<()>| {
            r.metadata_mut().insert("authorization", v.clone());
            Ok(r)
        })
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        let _ = tokio::time::timeout(Duration::from_secs(10), self.join).await;
    }
}

/// Echo worker: upper-cases payloads; "fail" fails permanently; "slow" sleeps 30s.
fn spawn_worker(
    server: &TestServer,
    slots: u32,
    seen: Arc<AtomicU32>,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (tx, rx) = oneshot::channel::<()>();
    let opts = WorkerOptions::new(server.grpc_url(), "default")
        .slots(slots)
        .token(WORKER)
        .name("test");
    let join = tokio::spawn(run_worker(
        opts,
        move |lease| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                match &lease.payload[..] {
                    b"fail" => Err(TaskError::permanent("asked to fail")),
                    b"slow" => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Ok(lease.payload)
                    }
                    p => Ok(Bytes::from(p.to_ascii_uppercase())),
                }
            }
        },
        async {
            let _ = rx.await;
        },
    ));
    (tx, join)
}

fn spec(payload: &str) -> pb::TaskSpec {
    pb::TaskSpec {
        payload: payload.as_bytes().to_vec(),
        content_type: "text/plain".into(),
        ..Default::default()
    }
}

async fn wait_for_capacity(server: &TestServer, n: u32) {
    let http = reqwest::Client::new();
    for _ in 0..200 {
        let v: serde_json::Value = http
            .get(server.http_url("/v1/snapshot"))
            .bearer_auth(CLIENT)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if v["pools"][0]["capacity"].as_u64() == Some(n as u64) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("capacity never reached {n}");
}

#[tokio::test]
async fn grpc_submit_wait_with_remote_worker() {
    let server = TestServer::start("").await;
    let seen = Arc::new(AtomicU32::new(0));
    let (_w, _j) = spawn_worker(&server, 2, seen.clone());
    wait_for_capacity(&server, 2).await;
    let mut c = server.client(CLIENT).await;

    let r = c
        .submit(pb::SubmitRequest {
            task: Some(spec("hello")),
            wait: true,
        })
        .await
        .unwrap()
        .into_inner();
    let res = r.result.unwrap();
    assert_eq!(res.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(res.output.unwrap(), b"HELLO");

    let r = c
        .submit(pb::SubmitRequest {
            task: Some(spec("fail")),
            wait: true,
        })
        .await
        .unwrap()
        .into_inner();
    let res = r.result.unwrap();
    assert_eq!(res.status, pb::TaskStatus::Failed as i32);
    assert!(res.message.contains("asked to fail"));

    // Batch with dependencies by key, then Wait on the last.
    let mut a = spec("a");
    a.key = "a".into();
    let mut b = spec("b");
    b.key = "b".into();
    b.deps.push(pb::Dep {
        target: Some(pb::TaskRef {
            target: Some(pb::task_ref::Target::Key("a".into())),
        }),
        cond: 0,
    });
    let r = c
        .submit_batch(pb::SubmitBatchRequest { tasks: vec![b, a] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.tasks.len(), 2);
    let res = c
        .wait(pb::WaitRequest {
            task: Some(pb::TaskRef {
                target: Some(pb::task_ref::Target::Key("b".into())),
            }),
            timeout_s: Some(10.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.output.unwrap(), b"B");
    // Results stay available from the output cache after completion.
    let res = c
        .wait(pb::WaitRequest {
            task: Some(pb::TaskRef {
                target: Some(pb::task_ref::Target::Key("a".into())),
            }),
            timeout_s: Some(1.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(res.output.unwrap(), b"A");
    server.stop().await;
}

#[tokio::test]
async fn auth_is_enforced_per_role() {
    let server = TestServer::start("").await;
    let mut wrong = server.client(WORKER).await;
    let e = wrong
        .submit(pb::SubmitRequest {
            task: Some(spec("x")),
            wait: false,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    let mut bad = server.client("nope").await;
    let e = bad
        .submit(pb::SubmitRequest {
            task: Some(spec("x")),
            wait: false,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated);
    let mut admin = server.client(ADMIN).await;
    assert!(
        admin
            .submit(pb::SubmitRequest {
                task: Some(spec("x")),
                wait: false
            })
            .await
            .is_ok()
    );

    let http = reqwest::Client::new();
    let r = http
        .post(server.http_url("/v1/tasks"))
        .json(&serde_json::json!({"payload": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = http.get(server.http_url("/readyz")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = http.get(server.http_url("/metrics")).send().await.unwrap();
    let body = r.text().await.unwrap();
    assert!(
        body.contains("cronoq_pool_workers{pool=\"default\"}"),
        "{body}"
    );
    server.stop().await;
}

#[tokio::test]
async fn http_json_api() {
    let server = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&server, 1, Arc::default());
    wait_for_capacity(&server, 1).await;
    let http = reqwest::Client::new();
    let v: serde_json::Value = http
        .post(server.http_url("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "json-string", "priority": "high", "key": "k1", "wait": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["result"]["status"], "succeeded");
    // The worker upper-cased the JSON text, which is still valid JSON.
    assert_eq!(v["result"]["output"], "JSON-STRING");
    let v: serde_json::Value = http
        .get(server.http_url("/v1/tasks/k1"))
        .bearer_auth(CLIENT)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["status"], "succeeded");
    let r = http
        .post(server.http_url("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": 1, "priority": "urgent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = http
        .post(server.http_url("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": 1, "pool": "missing"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    server.stop().await;
}

#[tokio::test]
async fn disconnected_worker_leases_are_retried_elsewhere() {
    let server = TestServer::start("").await;
    let seen_a = Arc::new(AtomicU32::new(0));
    let (stop_a, join_a) = spawn_worker(&server, 1, seen_a.clone());
    wait_for_capacity(&server, 1).await;
    let mut c = server.client(CLIENT).await;
    // "slow" goes to worker A, which then disconnects mid-attempt.
    let id = c
        .submit(pb::SubmitRequest {
            task: Some(spec("slow")),
            wait: false,
        })
        .await
        .unwrap()
        .into_inner()
        .id;
    for _ in 0..100 {
        if seen_a.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = stop_a.send(());
    let _ = join_a.await;
    wait_for_capacity(&server, 0).await;
    let info = c
        .get(pb::TaskRef {
            target: Some(pb::task_ref::Target::Id(id)),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        [
            pb::TaskStatus::Retrying as i32,
            pb::TaskStatus::Ready as i32
        ]
        .contains(&info.status),
        "status {}",
        info.status
    );
    server.stop().await;
}

#[tokio::test]
async fn timeouts_abort_remote_attempts() {
    let server = TestServer::start("").await;
    let seen = Arc::new(AtomicU32::new(0));
    let (_w, _j) = spawn_worker(&server, 1, seen.clone());
    wait_for_capacity(&server, 1).await;
    let mut c = server.client(CLIENT).await;
    let mut s = spec("slow");
    s.timeout_s = Some(0.2);
    s.retry = Some(pb::RetryPolicy {
        max_attempts: 2,
        initial_backoff_s: 0.01,
        ..Default::default()
    });
    let r = c
        .submit(pb::SubmitRequest {
            task: Some(s),
            wait: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        r.result.unwrap().status,
        pb::TaskStatus::DeadLettered as i32
    );
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    // The slot was released by the aborts: new work still runs.
    let r = c
        .submit(pb::SubmitRequest {
            task: Some(spec("ok")),
            wait: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.result.unwrap().output.unwrap(), b"OK");
    server.stop().await;
}

#[tokio::test]
async fn watch_streams_events_and_admin_snapshot() {
    let server = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&server, 1, Arc::default());
    wait_for_capacity(&server, 1).await;
    let mut c = server.client(CLIENT).await;
    let mut events = c
        .watch(pb::WatchRequest { pools: vec![] })
        .await
        .unwrap()
        .into_inner();
    let id = c
        .submit(pb::SubmitRequest {
            task: Some(spec("e")),
            wait: true,
        })
        .await
        .unwrap()
        .into_inner()
        .id;
    let ev = tokio::time::timeout(Duration::from_secs(5), events.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.id, id);
    assert_eq!(ev.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(ev.pool, "default");

    let ch = tonic::transport::Endpoint::from_shared(server.grpc_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let v: MetadataValue<_> = format!("Bearer {ADMIN}").parse().unwrap();
    let mut admin =
        pb::admin_client::AdminClient::with_interceptor(ch, move |mut r: tonic::Request<()>| {
            r.metadata_mut().insert("authorization", v.clone());
            Ok(r)
        });
    let snap = admin.snapshot(pb::Empty {}).await.unwrap().into_inner();
    assert_eq!(snap.pools[0].succeeded, 1);
    server.stop().await;
}
