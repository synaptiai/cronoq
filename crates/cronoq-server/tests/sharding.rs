//! Pools split between shards: each server serves its own shard and names the others.

mod common;

use common::*;
use cronoq::TaskId;
use cronoq_proto::v1 as pb;
use cronoq_server::Config;

const SHARDS: &str = r#"
[[auth.tokens]]
token = "client-cpu-token-0123456789abcdef0"
role = "client"
pools = ["default"]

[[pools]]
name = "default"
max = 4
lease_ttl_s = 2
[[pools]]
name = "llm"
max = 4
lease_ttl_s = 2

[[shards]]
name = "cpu"
number = 1
pools = ["default"]
endpoint = "http://cpu.internal:7070"
[[shards]]
name = "gpu"
number = 2
pools = ["llm"]
endpoint = "https://gpu.internal:7070"
"#;
/// A client token limited to pool `default`.
const CPU_CLIENT: &str = "client-cpu-token-0123456789abcdef0";

/// A server for `shard`, with `store` (a TOML `[store]` table, or "").
async fn shard_server(shard: &str, store: &str) -> TestServer {
    let mut cfg: Config =
        toml::from_str(&format!("{SERVER}\n{TOKENS}\n{SHARDS}\n{store}")).unwrap();
    cfg.server.shard = Some(shard.into());
    TestServer::start_cfg(cfg).await
}

fn in_pool(pool: &str) -> pb::TaskSpec {
    let mut s = spec(b"x");
    s.pool = pool.into();
    s
}

fn assert_elsewhere(e: &tonic::Status, shard: &str, endpoint: &str) {
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
    assert!(
        e.message().contains(&format!("shard '{shard}'")),
        "{}",
        e.message()
    );
    assert_eq!(e.metadata().get("cronoq-shard").unwrap(), shard);
    assert_eq!(e.metadata().get("cronoq-shard-endpoint").unwrap(), endpoint);
}

#[tokio::test(flavor = "multi_thread")]
async fn each_server_serves_its_shard_and_names_the_others() {
    let cpu = shard_server("cpu", "").await;
    let gpu = shard_server("gpu", "").await;
    // Each shard runs only its own pools.
    let snap = cpu.snapshot().await;
    let pools: Vec<&str> = snap["pools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(pools, ["default"]);

    let mut c = cpu.tasks(CLIENT).await;
    let mut g = gpu.tasks(CLIENT).await;
    let local = c
        .submit(submit(in_pool("default"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let remote = g
        .submit(submit(in_pool("llm"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    // Ids carry the shard number.
    assert_eq!((TaskId(local).shard(), TaskId(remote).shard()), (1, 2));

    // Work for the other shard is refused with its name and endpoint.
    let e = c.submit(submit(in_pool("llm"), false)).await.unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    let e = c
        .get(pb::GetRequest {
            task: by_id(remote),
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    let e = c
        .cancel(pb::CancelRequest {
            task: by_id(remote),
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    let e = g
        .get(pb::GetRequest { task: by_id(local) })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "cpu", "http://cpu.internal:7070");
    let e = c
        .watch(pb::WatchRequest {
            pools: vec!["llm".into()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    let e = c
        .list_tasks(pb::ListTasksRequest {
            pools: vec!["llm".into()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    // A batch is atomic in one shard, so it may not mix shards.
    let e = c
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![in_pool("default"), in_pool("llm")],
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");
    // Dependencies across shards are refused.
    let mut dependent = in_pool("default");
    dependent.deps.push(pb::Dep {
        target: by_id(remote),
        cond: pb::EdgeCond::OnSuccess as i32,
    });
    let e = c.submit(submit(dependent, false)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert!(e.message().contains("across shards"), "{}", e.message());
    // Schedules belong to the shard of their pool.
    let e = cpu
        .admin(ADMIN)
        .await
        .add_recurring(pb::RecurringRequest {
            name: "nightly".into(),
            cron: "0 0 3 * * *".into(),
            template: Some(in_pool("llm")),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");

    // Workers of the other shard's pool are told where to go.
    let e = match raw_worker(&cpu, WORKER, register("llm", 1)).await {
        Err(e) => e,
        Ok(mut w) => w.inbound.message().await.unwrap_err(),
    };
    assert_elsewhere(&e, "gpu", "https://gpu.internal:7070");

    // Credentials that may not use a pool learn nothing about where it is.
    let e = cpu
        .tasks(CPU_CLIENT)
        .await
        .submit(submit(in_pool("llm"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    assert!(e.metadata().get("cronoq-shard-endpoint").is_none());

    // HTTP answers the same way.
    let r = reqwest::Client::new()
        .post(cpu.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({ "pool": "llm" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(
        body["error"].as_str().unwrap().contains("shard 'gpu'"),
        "{body}"
    );

    // Losing one shard leaves the other serving.
    gpu.stop().await;
    assert!(c.submit(submit(in_pool("default"), false)).await.is_ok());
    cpu.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shards_keep_separate_stores_from_one_configuration() {
    // Shards sharing a store would wait for each other's leadership: fail instead of hanging.
    tokio::time::timeout(std::time::Duration::from_secs(60), separate_stores())
        .await
        .expect("both shards serve within 60 s");
}

async fn separate_stores() {
    let dir = tempfile::tempdir().unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.path().join("{shard}.db").display()
    );
    let cpu = shard_server("cpu", &store).await;
    let gpu = shard_server("gpu", &store).await;
    let id = cpu
        .tasks(CLIENT)
        .await
        .submit(submit(in_pool("default"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert!(dir.path().join("cpu.db").exists() && dir.path().join("gpu.db").exists());
    cpu.stop().await;
    gpu.stop().await;
    // After a restart the shard still has its task, with an id in its range.
    let cpu = shard_server("cpu", &store).await;
    let info = cpu
        .tasks(CLIENT)
        .await
        .get(pb::GetRequest { task: by_id(id) })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((info.id, TaskId(info.id).shard()), (id, 1));
    cpu.stop().await;
}
