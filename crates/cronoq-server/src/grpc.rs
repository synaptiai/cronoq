//! gRPC services.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cronoq::{OverlapPolicy, Recurring, TaskFailure, TaskRef, TaskStatus};
use cronoq_proto::v1 as pb;
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::auth::check_grpc;
use crate::config::Role;
use crate::convert;
use crate::state::AppState;

type Sched = cronoq::Scheduler<Bytes, Bytes>;

/// Wait for a task and build its wire result (from the handle or the output cache).
pub async fn wait_result(
    state: &AppState,
    target: TaskRef,
    timeout: Option<Duration>,
) -> Result<pb::TaskResult, Status> {
    let sched: &Sched = &state.sched;
    let handle = sched.handle(target.clone()).await.map_err(convert::error)?;
    let Some(handle) = handle else {
        return Err(Status::not_found("unknown task"));
    };
    await_handle(state, handle, timeout).await
}

/// Await a task handle and build its wire result.
pub async fn await_handle(
    state: &AppState,
    handle: cronoq::TaskHandle<Bytes>,
    timeout: Option<Duration>,
) -> Result<pb::TaskResult, Status> {
    let id = handle.id();
    let fut = async {
        match handle.await {
            Ok(out) => pb::TaskResult {
                id: id.0,
                status: pb::TaskStatus::Succeeded as i32,
                output: Some(out.to_vec()),
                message: String::new(),
            },
            Err(TaskFailure::Ended {
                status: TaskStatus::Succeeded,
                ..
            }) => pb::TaskResult {
                id: id.0,
                status: pb::TaskStatus::Succeeded as i32,
                output: state.outputs.get(id).map(|b| b.to_vec()),
                message: String::new(),
            },
            Err(TaskFailure::Ended { status, message }) => pb::TaskResult {
                id: id.0,
                status: convert::status(status) as i32,
                output: None,
                message: message.to_string(),
            },
            Err(TaskFailure::Shutdown) => pb::TaskResult {
                id: id.0,
                status: pb::TaskStatus::Unspecified as i32,
                output: None,
                message: "scheduler shut down".into(),
            },
        }
    };
    match timeout {
        Some(t) => tokio::time::timeout(t, fut)
            .await
            .map_err(|_| Status::deadline_exceeded("task still running")),
        None => Ok(fut.await),
    }
}

pub async fn submit_one(
    state: &AppState,
    spec: pb::TaskSpec,
    wait: bool,
) -> Result<pb::SubmitResponse, Status> {
    let task = convert::task(spec)?;
    if wait {
        let h = state.sched.submit(task).await.map_err(convert::error)?;
        let (id, duplicate) = (h.id(), h.is_duplicate());
        let result = await_handle(state, h, None).await?;
        Ok(pb::SubmitResponse {
            id: id.0,
            duplicate,
            result: Some(result),
        })
    } else {
        let s = state.sched.enqueue(task).await.map_err(convert::error)?;
        Ok(pb::SubmitResponse {
            id: s.id.0,
            duplicate: s.duplicate,
            result: None,
        })
    }
}

pub async fn submit_batch(
    state: &AppState,
    specs: Vec<pb::TaskSpec>,
) -> Result<pb::SubmitBatchResponse, Status> {
    let tasks = specs
        .into_iter()
        .map(convert::task)
        .collect::<Result<Vec<_>, _>>()?;
    let hs = state
        .sched
        .submit_batch(tasks)
        .await
        .map_err(convert::error)?;
    Ok(pb::SubmitBatchResponse {
        tasks: hs
            .iter()
            .map(|h| pb::SubmitResponse {
                id: h.id().0,
                duplicate: h.is_duplicate(),
                result: None,
            })
            .collect(),
    })
}

pub struct TasksService {
    pub state: Arc<AppState>,
}

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::StatusEvent, Status>> + Send>>;

#[tonic::async_trait]
impl pb::tasks_server::Tasks for TasksService {
    async fn submit(
        &self,
        req: Request<pb::SubmitRequest>,
    ) -> Result<Response<pb::SubmitResponse>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        let r = req.into_inner();
        let spec = r
            .task
            .ok_or_else(|| Status::invalid_argument("missing task"))?;
        Ok(Response::new(submit_one(&self.state, spec, r.wait).await?))
    }

    async fn submit_batch(
        &self,
        req: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        Ok(Response::new(
            submit_batch(&self.state, req.into_inner().tasks).await?,
        ))
    }

    async fn cancel(
        &self,
        req: Request<pb::TaskRef>,
    ) -> Result<Response<pb::CancelResponse>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        let target = convert::task_ref(Some(req.into_inner()))?;
        let cancelled = self
            .state
            .sched
            .cancel(target)
            .await
            .map_err(convert::error)?;
        Ok(Response::new(pb::CancelResponse { cancelled }))
    }

    async fn get(&self, req: Request<pb::TaskRef>) -> Result<Response<pb::TaskInfo>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        let target = convert::task_ref(Some(req.into_inner()))?;
        Ok(Response::new(task_info(&self.state, target).await?))
    }

    async fn wait(
        &self,
        req: Request<pb::WaitRequest>,
    ) -> Result<Response<pb::TaskResult>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        let r = req.into_inner();
        let target = convert::task_ref(r.task)?;
        let timeout = r.timeout_s.map(Duration::from_secs_f64);
        Ok(Response::new(
            wait_result(&self.state, target, timeout).await?,
        ))
    }

    type WatchStream = EventStream;

    async fn watch(
        &self,
        req: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        check_grpc(&self.state.auth, &req, Role::Client)?;
        let pools = req.into_inner().pools;
        let state = self.state.clone();
        let names: Arc<[Arc<str>]> = state.sched.pools().into();
        let clock = *state.sched.clock();
        let stream = BroadcastStream::new(state.sched.events()).filter_map(move |e| match e {
            Ok(e) => {
                let ev = convert::event(&e, &names, &clock);
                (pools.is_empty() || pools.contains(&ev.pool)).then_some(Ok(ev))
            }
            Err(_) => Some(Err(Status::data_loss(
                "event stream lagged; some events were dropped",
            ))),
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

pub async fn task_info(state: &AppState, target: TaskRef) -> Result<pb::TaskInfo, Status> {
    let id = match &target {
        TaskRef::Id(id) => Some(*id),
        TaskRef::Key(_) => state
            .sched
            .handle(target.clone())
            .await
            .map_err(convert::error)?
            .map(|h| h.id()),
    };
    let status = state.sched.status(target).await.map_err(convert::error)?;
    Ok(match (id, status) {
        (Some(id), Some(st)) => pb::TaskInfo {
            id: id.0,
            status: convert::status(st) as i32,
            found: true,
        },
        _ => pb::TaskInfo {
            id: id.map(|i| i.0).unwrap_or(0),
            status: 0,
            found: false,
        },
    })
}

pub struct WorkersService {
    pub state: Arc<AppState>,
}

type ServerStream = Pin<Box<dyn Stream<Item = Result<pb::ServerMessage, Status>> + Send>>;

#[tonic::async_trait]
impl pb::workers_server::Workers for WorkersService {
    type WorkStream = ServerStream;

    async fn work(
        &self,
        req: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::WorkStream>, Status> {
        check_grpc(&self.state.auth, &req, Role::Worker)?;
        let mut inbound = req.into_inner();
        // The first message must register the worker.
        let first = inbound
            .message()
            .await?
            .and_then(|m| m.msg)
            .ok_or_else(|| Status::invalid_argument("expected Register as first message"))?;
        let pb::worker_message::Msg::Register(reg) = first else {
            return Err(Status::invalid_argument(
                "expected Register as first message",
            ));
        };
        let Some(pool) = self
            .state
            .sched
            .pools()
            .iter()
            .find(|p| ***p == *reg.pool)
            .cloned()
        else {
            return Err(Status::not_found(format!("unknown pool '{}'", reg.pool)));
        };
        if !self.state.remote_pools.contains(&pool) {
            return Err(Status::failed_precondition(format!(
                "pool '{pool}' is not served by remote workers"
            )));
        }
        let (tx, rx) = mpsc::channel(1024);
        let registry = self.state.registry.clone();
        let worker = registry.register(pool.clone(), reg.slots, tx.clone());
        tracing::info!(worker, pool = %pool, slots = reg.slots, name = %reg.worker_name, "worker registered");
        let ttl = self.state.lease_ttl.get(&pool).copied();
        let _ = tx
            .send(Ok(pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Registered(pb::Registered {
                    worker_id: worker,
                    lease_ttl_s: ttl.map(|d| d.as_secs_f64()),
                })),
            }))
            .await;
        tokio::spawn(async move {
            loop {
                let msg = match inbound.message().await {
                    Ok(Some(m)) => m,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(worker, error = %e, "worker stream error");
                        break;
                    }
                };
                use pb::worker_message::Msg;
                match msg.msg {
                    Some(Msg::Heartbeat(h)) => registry.heartbeat(worker, h.lease),
                    Some(Msg::Complete(c)) => {
                        registry.finish(worker, c.lease, Ok(Bytes::from(c.output)));
                    }
                    Some(Msg::Fail(f)) => {
                        let err = if f.permanent {
                            cronoq::TaskError::permanent(f.message)
                        } else {
                            cronoq::TaskError::Retryable {
                                message: f.message.into(),
                                retry_after: f
                                    .retry_after_s
                                    .filter(|s| s.is_finite() && *s >= 0.0)
                                    .map(Duration::from_secs_f64),
                            }
                        };
                        registry.finish(worker, f.lease, Err(err));
                    }
                    Some(Msg::SetSlots(s)) => registry.set_slots(worker, s.slots),
                    Some(Msg::Register(_)) | None => {}
                }
            }
            registry.unregister(worker);
            tracing::info!(worker, "worker disconnected");
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

pub struct AdminService {
    pub state: Arc<AppState>,
}

#[tonic::async_trait]
impl pb::admin_server::Admin for AdminService {
    async fn snapshot(
        &self,
        req: Request<pb::Empty>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        check_grpc(&self.state.auth, &req, Role::Admin)?;
        let s = self.state.sched.snapshot().await.map_err(convert::error)?;
        Ok(Response::new(convert::snapshot(&s)))
    }

    async fn add_recurring(
        &self,
        req: Request<pb::RecurringRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        check_grpc(&self.state.auth, &req, Role::Admin)?;
        let r = req.into_inner();
        let template = convert::task(r.template.unwrap_or_default())?;
        let mut rec = Recurring::new(r.name, r.cron, template).overlap(if r.allow_overlap {
            OverlapPolicy::Allow
        } else {
            OverlapPolicy::Skip
        });
        if !r.tz.is_empty() {
            rec = rec.tz(r.tz);
        }
        self.state
            .sched
            .add_recurring(rec)
            .await
            .map_err(convert::error)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn remove_recurring(
        &self,
        req: Request<pb::RemoveRecurringRequest>,
    ) -> Result<Response<pb::RemoveRecurringResponse>, Status> {
        check_grpc(&self.state.auth, &req, Role::Admin)?;
        let removed = self
            .state
            .sched
            .remove_recurring(&req.into_inner().name)
            .await
            .map_err(convert::error)?;
        Ok(Response::new(pb::RemoveRecurringResponse { removed }))
    }
}
