//! Calls through any server reach the shard that serves them, and reads cover every shard.

mod common;

use std::time::Duration;

use common::*;
use cronoq::TaskId;
use cronoq_proto::v1 as pb;
use cronoq_server::{Config, Listeners};
use tonic::metadata::MetadataValue;

const LAYOUT: &str = r#"
[[auth.tokens]]
token = "client-cpu-token-0123456789abcdef0"
role = "client"
pools = ["default"]
[[auth.tokens]]
token = "client-gpu-token-0123456789abcdef0"
role = "client"
pools = ["llm"]

[[pools]]
name = "default"
max = 4
lease_ttl_s = 2
[[pools]]
name = "llm"
max = 4
lease_ttl_s = 2
"#;
/// A client token limited to pool `default` (shard `cpu`).
const CPU_CLIENT: &str = "client-cpu-token-0123456789abcdef0";
/// A client token limited to pool `llm` (shard `gpu`).
const GPU_CLIENT: &str = "client-gpu-token-0123456789abcdef0";

/// Listeners for the `cpu` and `gpu` shards, and their gRPC ports.
fn shard_listeners() -> (Listeners, Listeners, (u16, u16)) {
    let (cpu, gpu) = (listeners(), listeners());
    let port = |l: &Listeners| l.grpc.local_addr().unwrap().port();
    let ports = (port(&cpu), port(&gpu));
    (cpu, gpu, ports)
}

/// Two shards: `cpu` (pool `default`) and `gpu` (pool `llm`), on loopback ports.
struct Cluster {
    cpu: TestServer,
    gpu: TestServer,
}

/// The server for `shard`; it listens where [`shard_listeners`] says.
fn config(shard: &str, cpu_port: u16, gpu_endpoint: &str) -> Config {
    let shards = format!(
        "[[shards]]\nname = \"cpu\"\nnumber = 1\npools = [\"default\"]\nendpoint = \"http://127.0.0.1:{}\"\n\
         [[shards]]\nname = \"gpu\"\nnumber = 2\npools = [\"llm\"]\nendpoint = \"{gpu_endpoint}\"\n",
        cpu_port
    );
    let mut cfg: Config =
        toml::from_str(&format!("{SERVER}\n{TOKENS}\n{LAYOUT}\n{shards}")).unwrap();
    cfg.server.shard = Some(shard.into());
    cfg
}

impl Cluster {
    async fn start() -> Cluster {
        let (cpu, gpu, ports) = shard_listeners();
        let gpu_endpoint = format!("http://127.0.0.1:{}", ports.1);
        Cluster {
            cpu: TestServer::start_on(config("cpu", ports.0, &gpu_endpoint), Some(cpu)).await,
            gpu: TestServer::start_on(config("gpu", ports.0, &gpu_endpoint), Some(gpu)).await,
        }
    }

    async fn stop(self) {
        self.cpu.stop().await;
        self.gpu.stop().await;
    }
}

fn in_pool(pool: &str, key: &str) -> pb::TaskSpec {
    let mut s = spec(b"x");
    s.pool = pool.into();
    s.key = key.into();
    s
}

fn ids(tasks: &[pb::TaskInfo]) -> Vec<u64> {
    tasks.iter().map(|t| t.id).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn any_server_reaches_the_shard_of_a_task() {
    let c = Cluster::start().await;
    let mut via_cpu = c.cpu.tasks(CLIENT).await;
    let local = via_cpu
        .submit(submit(in_pool("default", "here"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    // Submitted through cpu, admitted by gpu.
    let remote = via_cpu
        .submit(submit(in_pool("llm", "there"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert_eq!((TaskId(local).shard(), TaskId(remote).shard()), (1, 2));
    let info = c
        .gpu
        .tasks(CLIENT)
        .await
        .get(pb::GetRequest {
            task: by_id(remote),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.pool, "llm");

    // Lookups by id and by key, cancel and wait, all through cpu.
    let got = via_cpu
        .get(pb::GetRequest {
            task: by_id(remote),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.id, remote);
    let got = via_cpu
        .get(pb::GetRequest {
            task: by_key("there"),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.id, remote);
    let e = via_cpu
        .get(pb::GetRequest {
            task: by_key("nowhere"),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    let cancelled = via_cpu
        .cancel(pb::CancelRequest {
            task: by_key("there"),
        })
        .await
        .unwrap()
        .into_inner()
        .cancelled;
    assert!(cancelled);
    let result = via_cpu
        .wait(pb::WaitRequest {
            task: by_id(remote),
            timeout_s: Some(5.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.status, pb::TaskStatus::Cancelled as i32);

    // The same key in two shards is refused wherever it is asked: the answer must not depend
    // on which server a load balancer picks.
    let mut via_gpu = c.gpu.tasks(CLIENT).await;
    via_gpu
        .submit(submit(in_pool("llm", "here"), false))
        .await
        .unwrap();
    for client in [&mut via_cpu, &mut via_gpu] {
        let e = client
            .cancel(pb::CancelRequest {
                task: by_key("here"),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition);
        assert!(e.message().contains("use the task id"), "{}", e.message());
    }
    // Tasks a caller may not see do not count: a token for `llm` asking cpu for "here" finds
    // gpu's task, not an ambiguity with cpu's task in `default`.
    let got = c
        .cpu
        .tasks(GPU_CLIENT)
        .await
        .get(pb::GetRequest {
            task: by_key("here"),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((got.pool.as_str(), TaskId(got.id).shard()), ("llm", 2));
    // The id still reaches the task.
    let got = via_gpu
        .get(pb::GetRequest { task: by_id(local) })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.id, local);

    // Batches go to their one shard; batches spanning shards are refused.
    let r = via_cpu
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![in_pool("llm", "b1"), in_pool("llm", "b2")],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.tasks.iter().all(|s| TaskId(s.id).shard() == 2), "{r:?}");
    let e = via_cpu
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![in_pool("default", "b3"), in_pool("llm", "b4")],
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);

    // HTTP routes the same way.
    let r: serde_json::Value = reqwest::Client::new()
        .post(c.cpu.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({ "pool": "llm", "key": "over-http" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id: u64 = r["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(TaskId(id).shard(), 2);
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_cover_every_shard() {
    let c = Cluster::start().await;
    let mut via_cpu = c.cpu.tasks(CLIENT).await;
    let mut all = Vec::new();
    for (pool, key) in [
        ("llm", "g1"),
        ("default", "c1"),
        ("llm", "g2"),
        ("default", "c2"),
    ] {
        all.push(
            via_cpu
                .submit(submit(in_pool(pool, key), false))
                .await
                .unwrap()
                .into_inner()
                .id,
        );
    }
    all.sort();
    let list = |pools: &[&str], after: u64, limit: u32| pb::ListTasksRequest {
        pools: pools.iter().map(|p| p.to_string()).collect(),
        after,
        limit,
        ..Default::default()
    };
    let r = via_cpu
        .list_tasks(list(&[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&r.tasks), all);
    assert!(r.unavailable_shards.is_empty());
    // Pages run across shards in id order.
    let mut seen = Vec::new();
    let mut after = 0;
    loop {
        let r = via_cpu
            .list_tasks(list(&[], after, 3))
            .await
            .unwrap()
            .into_inner();
        seen.extend(ids(&r.tasks));
        if r.next_after == 0 {
            break;
        }
        after = r.next_after;
    }
    assert_eq!(seen, all);
    let r = via_cpu
        .list_tasks(list(&["llm"], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert!(r.tasks.iter().all(|t| t.pool == "llm") && r.tasks.len() == 2);
    // Credentials scoped to cpu's pool see only its tasks.
    let r = c
        .cpu
        .tasks(CPU_CLIENT)
        .await
        .list_tasks(list(&[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert!(r.tasks.iter().all(|t| t.pool == "default") && r.tasks.len() == 2);

    // Workers, schedules and snapshots of every shard.
    let mut w = raw_worker(&c.gpu, WORKER, register("llm", 2))
        .await
        .unwrap();
    let _ = w.inbound.message().await.unwrap();
    let mut admin = c.cpu.admin(ADMIN).await;
    let mut template = in_pool("llm", "");
    template.key = String::new();
    admin
        .add_recurring(pb::RecurringRequest {
            name: "nightly".into(),
            cron: "0 0 3 * * *".into(),
            template: Some(template),
            ..Default::default()
        })
        .await
        .unwrap();
    let on_gpu = c
        .gpu
        .admin(ADMIN)
        .await
        .list_recurring(pb::Empty {})
        .await
        .unwrap()
        .into_inner()
        .schedules;
    assert_eq!(on_gpu.len(), 1, "the schedule lives on its pool's shard");
    let mut metrics = c.cpu.admin(METRICS).await;
    let schedules = metrics
        .list_recurring(pb::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(schedules.schedules[0].name, "nightly");
    let workers = metrics
        .list_workers(pb::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(workers.workers.len(), 1);
    assert_eq!(workers.workers[0].pool, "llm");
    let snap = metrics.snapshot(pb::Empty {}).await.unwrap().into_inner();
    let mut pools: Vec<&str> = snap.pools.iter().map(|p| p.name.as_str()).collect();
    pools.sort();
    assert_eq!(pools, ["default", "llm"]);
    assert_eq!(snap.totals.unwrap().submitted, 4);
    let removed = admin
        .remove_recurring(pb::RemoveRecurringRequest {
            name: "nightly".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .removed;
    assert!(removed);

    // With gpu down, cpu still answers and says what is missing.
    drop(w);
    c.gpu.stop().await;
    let snap = metrics.snapshot(pb::Empty {}).await.unwrap().into_inner();
    assert_eq!(snap.unavailable_shards, ["gpu"]);
    assert_eq!(snap.pools.len(), 1);
    let r = via_cpu
        .list_tasks(list(&[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.unavailable_shards, ["gpu"]);
    assert_eq!(r.tasks.len(), 2);
    let e = via_cpu
        .submit(submit(in_pool("llm", "late"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert!(e.message().contains("shard 'gpu'"), "{}", e.message());
    // Keys: reads still find a task of a reachable shard and name the missing one; a cancel by
    // key is refused, since gpu might hold the same key; ids keep working.
    let r = via_cpu
        .get(pb::GetRequest { task: by_key("c1") })
        .await
        .unwrap();
    assert_eq!(
        r.metadata().get("cronoq-unavailable-shards").unwrap(),
        "gpu"
    );
    let c1 = r.into_inner().id;
    let e = via_cpu
        .cancel(pb::CancelRequest { task: by_key("c1") })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert!(e.message().contains("use the task id"), "{}", e.message());
    let e = via_cpu
        .get(pb::GetRequest { task: by_key("g1") })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert!(
        via_cpu
            .cancel(pb::CancelRequest { task: by_id(c1) })
            .await
            .unwrap()
            .into_inner()
            .cancelled
    );
    c.cpu.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_merges_every_shard() {
    let c = Cluster::start().await;
    let mut via_cpu = c.cpu.tasks(CLIENT).await;
    let mut events = via_cpu
        .watch(pb::WatchRequest {
            all_transitions: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    // Streams to other shards open asynchronously: submit until an event of gpu arrives.
    let mut want = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for pool in ["default", "llm"] {
                want.push(
                    via_cpu
                        .submit(submit(in_pool(pool, ""), false))
                        .await
                        .unwrap()
                        .into_inner()
                        .id,
                );
            }
            let mut shards = std::collections::HashSet::new();
            while let Ok(Ok(Some(ev))) =
                tokio::time::timeout(Duration::from_millis(200), events.message()).await
            {
                shards.insert(TaskId(ev.id).shard());
            }
            if shards.contains(&1) && shards.contains(&2) {
                return;
            }
        }
    })
    .await;
    assert!(found.is_ok(), "events of both shards within 10 s");
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarding_is_limited_to_safe_cases() {
    let c = Cluster::start().await;
    // A call that says it was forwarded is answered by this shard only, so calls cannot loop.
    let mut forwarded = c.cpu.tasks(CLIENT).await;
    let mut req = tonic::Request::new(submit(in_pool("llm", ""), false));
    req.metadata_mut()
        .insert("cronoq-forwarded", MetadataValue::from_static("1"));
    let e = forwarded.submit(req).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.metadata().get("cronoq-shard-endpoint").is_some());
    // Pool permissions are checked before anything is forwarded.
    let e = c
        .cpu
        .tasks(CPU_CLIENT)
        .await
        .submit(submit(in_pool("llm", ""), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    c.stop().await;

    // Tokens are not forwarded over plain http to another host: the caller is sent there.
    let l = listeners();
    let port = l.grpc.local_addr().unwrap().port();
    let cpu = TestServer::start_on(config("cpu", port, "http://10.255.255.1:7070"), Some(l)).await;
    let e = cpu
        .tasks(CLIENT)
        .await
        .submit(submit(in_pool("llm", ""), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        e.metadata().get("cronoq-shard-endpoint").unwrap(),
        "http://10.255.255.1:7070"
    );
    cpu.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_follow_their_pool_to_its_shard() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use cronoq_server::worker::{WorkerOptions, run_worker};

    let c = Cluster::start().await;
    // A worker for pool `llm` pointed at the cpu shard ends up serving on gpu.
    let seen = Arc::new(AtomicU32::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let opts = WorkerOptions::new(c.cpu.grpc_url(), "llm").token(WORKER);
    let counter = seen.clone();
    let worker = tokio::spawn(run_worker(
        opts,
        move |lease| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(lease.payload) }
        },
        async {
            let _ = stopped.await;
        },
    ));
    let mut tasks = c.cpu.tasks(CLIENT).await;
    let id = tasks
        .submit(submit(in_pool("llm", "redirected"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let done = tokio::time::timeout(
        Duration::from_secs(20),
        tasks.wait(pb::WaitRequest {
            task: by_id(id),
            timeout_s: Some(15.0),
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert_eq!(done.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    let _ = stop.send(());
    worker.await.unwrap().unwrap();
    c.stop().await;

    // Two servers that each say the other serves `llm` send the worker back and forth until
    // it gives up.
    let (cpu_l, gpu_l, ports) = shard_listeners();
    let cpu = TestServer::start_on(
        config("cpu", ports.0, &format!("http://127.0.0.1:{}", ports.1)),
        Some(cpu_l),
    )
    .await;
    let swapped = format!(
        "{LAYOUT}\n[[pools]]\nname = \"spare\"\nmax = 1\n\
         [[shards]]\nname = \"cpu\"\nnumber = 1\npools = [\"default\", \"llm\"]\n\
         endpoint = \"http://127.0.0.1:{}\"\n\
         [[shards]]\nname = \"gpu\"\nnumber = 2\npools = [\"spare\"]\n\
         endpoint = \"http://127.0.0.1:{}\"\n",
        ports.0, ports.1
    );
    let mut cfg: Config = toml::from_str(&format!("{SERVER}\n{TOKENS}\n{swapped}")).unwrap();
    cfg.server.shard = Some("gpu".into());
    let gpu = TestServer::start_on(cfg, Some(gpu_l)).await;
    let opts = WorkerOptions::new(cpu.grpc_url(), "llm").token(WORKER);
    let e = tokio::time::timeout(
        Duration::from_secs(20),
        run_worker(
            opts,
            |l| async move { Ok(l.payload) },
            std::future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(e.to_string().contains("redirected 4 times in a row"), "{e}");
    cpu.stop().await;
    gpu.stop().await;
}
