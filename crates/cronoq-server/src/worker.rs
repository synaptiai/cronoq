//! Rust client for remote workers.
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use cronoq_server::worker::{WorkerOptions, run_worker};
//! let opts = WorkerOptions::new("http://127.0.0.1:7070", "default").slots(8);
//! run_worker(opts, |lease| async move { Ok(lease.payload) }, std::future::pending()).await
//! # }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cronoq::TaskError;
use cronoq_proto::v1 as pb;
use pb::workers_client::WorkersClient;
use rustc_hash::FxHashMap;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub endpoint: String,
    pub pool: String,
    pub slots: u32,
    pub token: Option<String>,
    pub name: String,
}

impl WorkerOptions {
    pub fn new(endpoint: impl Into<String>, pool: impl Into<String>) -> Self {
        WorkerOptions {
            endpoint: endpoint.into(),
            pool: pool.into(),
            slots: 1,
            token: None,
            name: String::new(),
        }
    }
    pub fn slots(mut self, n: u32) -> Self {
        self.slots = n;
        self
    }
    pub fn token(mut self, t: impl Into<String>) -> Self {
        self.token = Some(t.into());
        self
    }
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        self
    }
}

/// One leased attempt.
#[derive(Clone, Debug)]
pub struct LeaseInfo {
    pub lease: u64,
    pub task_id: u64,
    pub attempt: u32,
    pub payload: Bytes,
    pub content_type: String,
    pub kind: String,
    pub key: String,
    pub tags: HashMap<String, String>,
    pub timeout: Option<Duration>,
    /// Cancelled when the server aborts this attempt.
    pub cancel: CancellationToken,
}

/// Connect, register and process leases until `shutdown` resolves or the stream ends.
pub async fn run_worker<F, Fut>(
    opts: WorkerOptions,
    handler: F,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()>
where
    F: Fn(LeaseInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, TaskError>> + Send + 'static,
{
    let channel = tonic::transport::Endpoint::from_shared(opts.endpoint.clone())?
        .connect()
        .await?;
    let auth: Option<MetadataValue<_>> = match &opts.token {
        Some(t) => Some(format!("Bearer {t}").parse()?),
        None => None,
    };
    let mut client = WorkersClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        if let Some(a) = &auth {
            req.metadata_mut().insert("authorization", a.clone());
        }
        Ok(req)
    });
    let (tx, rx) = mpsc::channel::<pb::WorkerMessage>(1024);
    use pb::worker_message::Msg;
    tx.send(pb::WorkerMessage {
        msg: Some(Msg::Register(pb::Register {
            pool: opts.pool.clone(),
            slots: opts.slots,
            worker_name: opts.name.clone(),
        })),
    })
    .await?;
    let mut inbound = client.work(ReceiverStream::new(rx)).await?.into_inner();
    let ttl = match inbound.message().await?.and_then(|m| m.msg) {
        Some(pb::server_message::Msg::Registered(r)) => r.lease_ttl_s.map(Duration::from_secs_f64),
        other => anyhow::bail!("unexpected first server message: {other:?}"),
    };
    let handler = Arc::new(handler);
    let running: Arc<Mutex<FxHashMap<u64, CancellationToken>>> = Arc::default();
    tokio::pin!(shutdown);
    loop {
        let msg = tokio::select! {
            m = inbound.message() => m?,
            _ = &mut shutdown => break,
        };
        let Some(msg) = msg.and_then(|m| m.msg) else {
            break;
        };
        match msg {
            pb::server_message::Msg::Lease(l) => {
                let cancel = CancellationToken::new();
                running.lock().await.insert(l.lease, cancel.clone());
                let info = LeaseInfo {
                    lease: l.lease,
                    task_id: l.task_id,
                    attempt: l.attempt,
                    payload: Bytes::from(l.payload),
                    content_type: l.content_type,
                    kind: l.kind,
                    key: l.key,
                    tags: l.tags,
                    timeout: l.timeout_s.map(Duration::from_secs_f64),
                    cancel: cancel.clone(),
                };
                let (h, tx, running) = (handler.clone(), tx.clone(), running.clone());
                tokio::spawn(async move {
                    let lease = info.lease;
                    let beat = ttl.map(|t| {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let mut iv =
                                tokio::time::interval((t / 3).max(Duration::from_millis(10)));
                            iv.tick().await;
                            loop {
                                iv.tick().await;
                                let hb = pb::WorkerMessage {
                                    msg: Some(Msg::Heartbeat(pb::Heartbeat { lease })),
                                };
                                if tx.send(hb).await.is_err() {
                                    break;
                                }
                            }
                        })
                    });
                    let result = tokio::select! {
                        r = h(info) => Some(r),
                        _ = cancel.cancelled() => None,
                    };
                    if let Some(b) = beat {
                        b.abort();
                    }
                    running.lock().await.remove(&lease);
                    let msg = match result {
                        None => return, // aborted by the server
                        Some(Ok(output)) => Msg::Complete(pb::Complete {
                            lease,
                            output: output.to_vec(),
                        }),
                        Some(Err(TaskError::Permanent { message })) => Msg::Fail(pb::Fail {
                            lease,
                            message: message.to_string(),
                            permanent: true,
                            retry_after_s: None,
                        }),
                        Some(Err(TaskError::Retryable {
                            message,
                            retry_after,
                        })) => Msg::Fail(pb::Fail {
                            lease,
                            message: message.to_string(),
                            permanent: false,
                            retry_after_s: retry_after.map(|d| d.as_secs_f64()),
                        }),
                        Some(Err(TaskError::Cancelled)) => Msg::Fail(pb::Fail {
                            lease,
                            message: "cancelled".into(),
                            permanent: true,
                            retry_after_s: None,
                        }),
                    };
                    let _ = tx.send(pb::WorkerMessage { msg: Some(msg) }).await;
                });
            }
            pb::server_message::Msg::Abort(a) => {
                if let Some(c) = running.lock().await.remove(&a.lease) {
                    c.cancel();
                }
            }
            pb::server_message::Msg::Registered(_) => {}
        }
    }
    for (_, c) in running.lock().await.drain() {
        c.cancel();
    }
    Ok(())
}
