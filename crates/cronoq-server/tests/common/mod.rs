//! Shared helpers for server integration tests.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cronoq::TaskError;
use cronoq_proto::v1 as pb;
use cronoq_server::worker::{WorkerOptions, run_worker};
use cronoq_server::{Bound, Config};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

pub const CLIENT: &str = "client-token-0123456789abcdef0123";
pub const WORKER: &str = "worker-token-0123456789abcdef0123";
pub const ADMIN: &str = "admin-token-0123456789abcdef012345";
pub const METRICS: &str = "metrics-token-0123456789abcdef0123";

pub const TOKENS: &str = r#"
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
"#;

pub const SERVER: &str = r#"
[server]
grpc_addr = "127.0.0.1:0"
http_addr = "127.0.0.1:0"
shutdown_grace_s = 0.5
"#;

pub const DEFAULT_POOL: &str = r#"
[[pools]]
name = "default"
max = 8
lease_ttl_s = 2
retry = { max_attempts = 3, initial_backoff_s = 0.05, jitter = false }
"#;

pub type Intercepted = InterceptedService<Channel, Auth>;

/// Adds a bearer token to every request.
#[derive(Clone)]
pub struct Auth(pub Option<MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for Auth {
    fn call(&mut self, mut r: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            r.metadata_mut().insert("authorization", v.clone());
        }
        Ok(r)
    }
}

pub struct TestServer {
    pub bound: Bound,
    pub stop: Option<oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    /// Server with the test tokens, the default pool (unless `extra` defines pools) and `extra`.
    pub async fn start(extra: &str) -> TestServer {
        let pools = if extra.contains("[[pools]]") {
            ""
        } else {
            DEFAULT_POOL
        };
        Self::start_toml(&format!("{SERVER}\n{TOKENS}\n{pools}\n{extra}")).await
    }

    /// Server without authentication.
    pub async fn start_open(extra: &str) -> TestServer {
        let pools = if extra.contains("[[pools]]") {
            ""
        } else {
            DEFAULT_POOL
        };
        Self::start_toml(&format!("{SERVER}\n{pools}\n{extra}")).await
    }

    pub async fn start_toml(toml: &str) -> TestServer {
        Self::start_cfg(Config::parse(toml).unwrap()).await
    }

    pub async fn start_cfg(cfg: Config) -> TestServer {
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
        let bound = match tokio::time::timeout(Duration::from_secs(10), bound_rx).await {
            Ok(Ok(b)) => b,
            _ => panic!("server did not start: {:?}", join.await),
        };
        TestServer {
            bound,
            stop: Some(stop_tx),
            join,
        }
    }

    pub fn grpc_url(&self) -> String {
        format!("http://{}", self.bound.grpc)
    }

    pub fn http(&self, path: &str) -> String {
        format!("http://{}{}", self.bound.http, path)
    }

    pub async fn channel(&self) -> Channel {
        tonic::transport::Endpoint::from_shared(self.grpc_url())
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    fn auth(token: &str) -> Auth {
        Auth(Some(format!("Bearer {token}").parse().unwrap()))
    }

    pub async fn tasks(&self, token: &str) -> pb::tasks_client::TasksClient<Intercepted> {
        pb::tasks_client::TasksClient::with_interceptor(self.channel().await, Self::auth(token))
    }

    pub async fn admin(&self, token: &str) -> pb::admin_client::AdminClient<Intercepted> {
        pb::admin_client::AdminClient::with_interceptor(self.channel().await, Self::auth(token))
    }

    pub async fn workers(&self, token: &str) -> pb::workers_client::WorkersClient<Intercepted> {
        pb::workers_client::WorkersClient::with_interceptor(self.channel().await, Self::auth(token))
    }

    pub async fn snapshot(&self) -> serde_json::Value {
        reqwest::Client::new()
            .get(self.http("/v1/snapshot"))
            .bearer_auth(METRICS)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub async fn capacity(&self) -> u64 {
        self.snapshot().await["pools"][0]["capacity"]
            .as_u64()
            .unwrap()
    }

    pub async fn wait_capacity(&self, n: u64) {
        for _ in 0..400 {
            if self.capacity().await == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("capacity never reached {n}, is {}", self.capacity().await);
    }

    /// Stop and require a clean return within `t`.
    pub async fn stop_within(mut self, t: Duration) -> anyhow::Result<()> {
        let _ = self.stop.take().unwrap().send(());
        match tokio::time::timeout(t, &mut self.join).await {
            Ok(r) => r.expect("server task panicked"),
            Err(_) => {
                self.join.abort();
                panic!("run() did not return within {t:?} of shutdown");
            }
        }
    }

    /// Stop and require a clean shutdown.
    pub async fn stop(self) {
        self.stop_within(Duration::from_secs(10))
            .await
            .expect("server returned an error");
    }
}

pub fn spec(payload: &[u8]) -> pb::TaskSpec {
    pb::TaskSpec {
        payload: payload.to_vec(),
        content_type: "text/plain".into(),
        ..Default::default()
    }
}

pub fn by_id(id: u64) -> Option<pb::TaskRef> {
    Some(pb::TaskRef {
        target: Some(pb::task_ref::Target::Id(id)),
    })
}

pub fn by_key(k: &str) -> Option<pb::TaskRef> {
    Some(pb::TaskRef {
        target: Some(pb::task_ref::Target::Key(k.into())),
    })
}

pub fn submit(spec: pb::TaskSpec, wait: bool) -> pb::SubmitRequest {
    pb::SubmitRequest {
        task: Some(spec),
        wait,
        wait_timeout_s: None,
    }
}

/// Echo worker: upper-cases payloads; "fail" fails permanently; "slow" sleeps 30s.
pub fn spawn_worker(
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

/// A hand-driven worker stream.
pub struct RawWorker {
    pub tx: mpsc::Sender<pb::WorkerMessage>,
    pub inbound: tonic::Streaming<pb::ServerMessage>,
}

pub fn register(pool: &str, slots: u32) -> pb::WorkerMessage {
    pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Register(pb::Register {
            pool: pool.into(),
            slots,
            worker_name: "raw".into(),
            protocol_version: cronoq_proto::PROTOCOL_VERSION,
        })),
    }
}

pub async fn raw_worker(
    server: &TestServer,
    token: &str,
    first: pb::WorkerMessage,
) -> Result<RawWorker, tonic::Status> {
    let mut c = server.workers(token).await;
    let (tx, rx) = mpsc::channel(64);
    tx.send(first).await.unwrap();
    let inbound = c.work(ReceiverStream::new(rx)).await?.into_inner();
    Ok(RawWorker { tx, inbound })
}

/// Next server message of a given kind, skipping others.
pub async fn next_lease(w: &mut RawWorker) -> pb::Lease {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(10), w.inbound.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(pb::server_message::Msg::Lease(l)) = m.msg {
            return l;
        }
    }
}
