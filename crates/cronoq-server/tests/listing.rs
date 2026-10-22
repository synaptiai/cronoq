//! Listing tasks, workers and recurring schedules.

mod common;

use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;

const SCOPED: &str = r#"
[[auth.tokens]]
token = "client-a-token-0123456789abcdef0123"
role = "client"
pools = ["a"]
[[auth.tokens]]
token = "metrics-a-token-0123456789abcdef012"
role = "metrics"
pools = ["a"]
[[auth.tokens]]
token = "client-none-token-0123456789abcdef0"
role = "client"
pools = []
[[pools]]
name = "a"
max = 4
lease_ttl_s = 2
[[pools]]
name = "b"
max = 4
"#;
const CLIENT_A: &str = "client-a-token-0123456789abcdef0123";
const METRICS_A: &str = "metrics-a-token-0123456789abcdef012";
const CLIENT_NONE: &str = "client-none-token-0123456789abcdef0";

fn in_pool(pool: &str, payload: &[u8]) -> pb::TaskSpec {
    let mut s = spec(payload);
    s.pool = pool.into();
    s
}

fn list(
    pools: &[&str],
    statuses: &[pb::TaskStatus],
    after: u64,
    limit: u32,
) -> pb::ListTasksRequest {
    pb::ListTasksRequest {
        pools: pools.iter().map(|p| p.to_string()).collect(),
        statuses: statuses.iter().map(|s| *s as i32).collect(),
        after,
        limit,
    }
}

fn ids(r: &pb::ListTasksResponse) -> Vec<u64> {
    r.tasks.iter().map(|t| t.id).collect()
}

/// Pool `a` has one finished, one running and one queued task; pool `b` one queued task.
async fn listing_works(extra: &str) {
    let s = TestServer::start(&format!("{SCOPED}{extra}")).await;
    let mut w = raw_worker(&s, WORKER, register("a", 1)).await.unwrap();
    let _ = w.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let mut c = s.tasks(ADMIN).await;
    let mut first = in_pool("a", b"1");
    first.key = "first".into();
    first.tenant = "acme".into();
    first.priority = pb::Priority::High as i32;
    let mut ids_by_order = Vec::new();
    for t in [
        first,
        in_pool("a", b"2"),
        in_pool("a", b"3"),
        in_pool("b", b"4"),
    ] {
        ids_by_order.push(c.submit(submit(t, false)).await.unwrap().into_inner().id);
    }
    let [done, running, queued, other] = ids_by_order[..] else {
        unreachable!()
    };
    let l = next_lease(&mut w).await;
    assert_eq!(l.task_id, done);
    w.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease: l.lease,
            output: b"out".to_vec(),
            content_type: String::new(),
        })),
    })
    .await
    .unwrap();
    assert_eq!(next_lease(&mut w).await.task_id, running);
    // The finished task reaches the store (or the cache) shortly after it completes.
    let mut all = Default::default();
    for _ in 0..100 {
        all = c
            .list_tasks(list(&[], &[], 0, 0))
            .await
            .unwrap()
            .into_inner();
        if all
            .tasks
            .first()
            .is_some_and(|t| t.status == pb::TaskStatus::Succeeded as i32)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(ids(&all), [done, running, queued, other]);
    assert_eq!(all.next_after, 0);
    let t = &all.tasks[0];
    assert_eq!(t.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!((t.pool.as_str(), t.key.as_str()), ("a", "first"));
    assert_eq!(t.tenant, "acme");
    assert_eq!(t.priority, pb::Priority::High as i32);
    assert!(t.output_available);
    assert_eq!(all.tasks[1].status, pb::TaskStatus::Running as i32);
    assert_eq!(all.tasks[1].attempt, 1);
    assert_eq!(all.tasks[2].status, pb::TaskStatus::Ready as i32);

    // Filters and pages.
    let r = c
        .list_tasks(list(
            &[],
            &[pb::TaskStatus::Succeeded, pb::TaskStatus::Ready],
            0,
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&r), [done, queued, other]);
    let r = c
        .list_tasks(list(&["b"], &[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&r), [other]);
    let page1 = c
        .list_tasks(list(&[], &[], 0, 2))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&page1), [done, running]);
    assert_eq!(page1.next_after, running);
    let page2 = c
        .list_tasks(list(&[], &[], page1.next_after, 2))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&page2), [queued, other]);
    assert_eq!(page2.next_after, 0);

    // Credentials scoped to pool `a` list only its tasks, and may not ask for `b`.
    let mut scoped = s.tasks(CLIENT_A).await;
    let r = scoped
        .list_tasks(list(&[], &[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&r), [done, running, queued]);
    let e = scoped
        .list_tasks(list(&["b"], &[], 0, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    // Credentials allowing no pool list nothing.
    let r = s
        .tasks(CLIENT_NONE)
        .await
        .list_tasks(list(&[], &[], 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert!(r.tasks.is_empty(), "{r:?}");
    let e = c
        .list_tasks(list(&[], &[pb::TaskStatus::Unspecified], 0, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);

    // HTTP: comma-separated filters, ids as strings.
    let v: serde_json::Value = reqwest::Client::new()
        .get(s.http("/v1/tasks?status=succeeded,running&limit=1"))
        .bearer_auth(CLIENT_A)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["tasks"][0]["id"], done.to_string());
    assert_eq!(v["tasks"][0]["tenant"], "acme");
    assert_eq!(v["tasks"][0]["priority"], "high");
    assert_eq!(v["next_after"], done.to_string());
    let r = reqwest::Client::new()
        .get(s.http("/v1/tasks?status=nope"))
        .bearer_auth(CLIENT_A)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_are_listed_without_a_store() {
    listing_works("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_are_listed_from_a_sqlite_store() {
    let dir = tempfile::tempdir().unwrap();
    listing_works(&format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.path().join("state.db").display()
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_and_schedules_are_listed_for_the_callers_pools() {
    let s = TestServer::start(SCOPED).await;
    let mut reg = register("a", 2);
    if let Some(pb::worker_message::Msg::Register(r)) = &mut reg.msg {
        r.worker_name = "w-a".into();
        r.labels.insert("zone".into(), "eu".into());
    }
    let mut wa = raw_worker(&s, WORKER, reg).await.unwrap();
    let _ = wa.inbound.message().await.unwrap();
    let mut wb = raw_worker(&s, WORKER, register("b", 1)).await.unwrap();
    let _ = wb.inbound.message().await.unwrap();
    let mut admin = s.admin(ADMIN).await;
    for (name, pool) in [("nightly-b", "b"), ("hourly-a", "a")] {
        admin
            .add_recurring(pb::RecurringRequest {
                name: name.into(),
                cron: "0 0 * * * *".into(),
                tz: String::new(),
                template: Some(in_pool(pool, b"x")),
                overlap: pb::Overlap::Queue as i32,
            })
            .await
            .unwrap();
    }

    let mut m = s.admin(METRICS).await;
    let workers = m
        .list_workers(pb::Empty {})
        .await
        .unwrap()
        .into_inner()
        .workers;
    assert_eq!(workers.len(), 2, "{workers:?}");
    let a = workers.iter().find(|w| w.pool == "a").unwrap();
    assert_eq!((a.name.as_str(), a.slots, a.busy), ("w-a", 2, 0));
    assert_eq!(a.labels["zone"], "eu");
    assert!(a.connected_unix_ns > 0);
    let schedules = m
        .list_recurring(pb::Empty {})
        .await
        .unwrap()
        .into_inner()
        .schedules;
    let names: Vec<&str> = schedules.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["hourly-a", "nightly-b"]);
    assert_eq!(schedules[0].overlap, pb::Overlap::Queue as i32);
    assert!(schedules[0].next_unix_ns.is_some());

    let mut scoped = s.admin(METRICS_A).await;
    let w = scoped
        .list_workers(pb::Empty {})
        .await
        .unwrap()
        .into_inner()
        .workers;
    assert_eq!(w.len(), 1);
    assert_eq!(w[0].pool, "a");
    let r = scoped
        .list_recurring(pb::Empty {})
        .await
        .unwrap()
        .into_inner()
        .schedules;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].name, "hourly-a");
    // Clients lack the metrics role.
    let e = s
        .admin(CLIENT)
        .await
        .list_workers(pb::Empty {})
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);

    let http = reqwest::Client::new();
    let v: serde_json::Value = http
        .get(s.http("/v1/workers"))
        .bearer_auth(METRICS_A)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["workers"][0]["name"], "w-a");
    assert_eq!(v["workers"][0]["labels"]["zone"], "eu");
    let v: serde_json::Value = http
        .get(s.http("/v1/recurring"))
        .bearer_auth(METRICS)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["schedules"][1]["name"], "nightly-b");
    assert_eq!(v["schedules"][1]["overlap"], "queue");
    let r = http
        .get(s.http("/v1/workers"))
        .bearer_auth(CLIENT)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    s.stop().await;
}
