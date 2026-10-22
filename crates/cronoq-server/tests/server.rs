//! End-to-end behaviour of the server with remote workers.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;

#[tokio::test]
async fn grpc_submit_wait_with_remote_worker() {
    let server = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&server, 2, Arc::default());
    server.wait_capacity(2).await;
    let mut c = server.tasks(CLIENT).await;

    let res = c
        .submit(submit(spec(b"hello"), true))
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(res.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(res.output.unwrap(), b"HELLO");
    assert!(res.output_available);

    let res = c
        .submit(submit(spec(b"fail"), true))
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(res.status, pb::TaskStatus::Failed as i32);
    assert!(res.message.contains("asked to fail"));

    // Batch with dependencies by key, then Wait on the last.
    let mut a = spec(b"a");
    a.key = "a".into();
    let mut b = spec(b"b");
    b.key = "b".into();
    b.deps.push(pb::Dep {
        target: by_key("a"),
        cond: pb::EdgeCond::OnSuccess as i32,
    });
    let r = c
        .submit_batch(pb::SubmitBatchRequest { tasks: vec![b, a] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.tasks.len(), 2);
    let res = c
        .wait(pb::WaitRequest {
            task: by_key("b"),
            timeout_s: Some(10.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.output.unwrap(), b"B");
    // Results stay available from the output cache after completion.
    let res = c
        .wait(pb::WaitRequest {
            task: by_key("a"),
            timeout_s: Some(1.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(res.output.unwrap(), b"A");

    // Rich task details.
    let info = c
        .get(pb::GetRequest { task: by_key("a") })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(
        (info.pool.as_str(), info.key.as_str(), info.kind.as_str()),
        ("default", "a", "default")
    );
    assert_eq!((info.attempt, info.max_attempts), (1, 3));
    assert!(
        info.created_unix_ns.is_some()
            && info.started_unix_ns.is_some()
            && info.finished_unix_ns.is_some()
    );
    assert!(info.output_available);
    server.stop().await;
}

#[tokio::test]
async fn auth_is_enforced_per_role() {
    let server = TestServer::start("").await;
    let e = server
        .tasks(WORKER)
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    let e = server
        .tasks("nope-nope-nope-nope-nope")
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated);
    assert!(
        server
            .tasks(ADMIN)
            .await
            .submit(submit(spec(b"x"), false))
            .await
            .is_ok()
    );
    // Snapshot needs the metrics role, which clients do not have.
    let e = server
        .admin(CLIENT)
        .await
        .snapshot(pb::Empty {})
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    assert!(
        server
            .admin(METRICS)
            .await
            .snapshot(pb::Empty {})
            .await
            .is_ok()
    );
    let e = server
        .admin(METRICS)
        .await
        .remove_recurring(pb::RemoveRecurringRequest { name: "x".into() })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);

    let http = reqwest::Client::new();
    let r = http
        .post(server.http("/v1/tasks"))
        .json(&serde_json::json!({"payload": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(r.headers()["www-authenticate"], "Bearer");
    let r = http.get(server.http("/readyz")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = http.get(server.http("/metrics")).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let r = http
        .get(server.http("/metrics"))
        .bearer_auth(CLIENT)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = http
        .get(server.http("/metrics"))
        .bearer_auth(METRICS)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
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
    server.wait_capacity(1).await;
    let http = reqwest::Client::new();
    let v: serde_json::Value = http
        .post(server.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "json-string", "priority": "high", "key": "k1", "wait": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["result"]["status"], "succeeded");
    assert!(v["id"].is_string(), "ids are strings: {v}");
    // The worker upper-cased the JSON text, which is still valid JSON.
    assert_eq!(v["result"]["output"], "JSON-STRING");
    let id = v["id"].as_str().unwrap().to_string();
    for path in [format!("/v1/tasks/{id}"), "/v1/tasks/by-key/k1".to_string()] {
        let v: serde_json::Value = http
            .get(server.http(&path))
            .bearer_auth(CLIENT)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(v["status"], "succeeded", "{path}: {v}");
        assert_eq!(v["id"], id.as_str());
        assert_eq!(v["key"], "k1");
    }
    let r = http
        .post(server.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": 1, "priority": "urgent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = http
        .post(server.http("/v1/tasks"))
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
    server.wait_capacity(1).await;
    let mut c = server.tasks(CLIENT).await;
    // "slow" goes to worker A, which then stops mid-attempt.
    let id = c
        .submit(submit(spec(b"slow"), false))
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
    // The drain waits for the slow handler; the worker gives up after its drain timeout, but
    // the server sees the disconnect when the task is aborted.
    join_a.abort();
    server.wait_capacity(0).await;
    let info = c
        .get(pb::GetRequest { task: by_id(id) })
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
    server.wait_capacity(1).await;
    let mut c = server.tasks(CLIENT).await;
    let mut s = spec(b"slow");
    s.timeout_s = Some(0.2);
    s.retry = Some(pb::RetryPolicy {
        max_attempts: 2,
        initial_backoff_s: 0.01,
        ..Default::default()
    });
    let r = c.submit(submit(s, true)).await.unwrap().into_inner();
    assert_eq!(
        r.result.unwrap().status,
        pb::TaskStatus::DeadLettered as i32
    );
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    // The slot was released by the aborts: new work still runs.
    let r = c
        .submit(submit(spec(b"ok"), true))
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
    server.wait_capacity(1).await;
    let mut c = server.tasks(CLIENT).await;
    let mut events = c
        .watch(pb::WatchRequest::default())
        .await
        .unwrap()
        .into_inner();
    let mut all = c
        .watch(pb::WatchRequest {
            all_transitions: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let id = c
        .submit(submit(spec(b"e"), true))
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
    let mut statuses = Vec::new();
    while statuses.last() != Some(&(pb::TaskStatus::Succeeded as i32)) {
        let ev = tokio::time::timeout(Duration::from_secs(5), all.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        statuses.push(ev.status);
    }
    assert!(
        statuses.contains(&(pb::TaskStatus::Running as i32)),
        "{statuses:?}"
    );

    let snap = server
        .admin(ADMIN)
        .await
        .snapshot(pb::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snap.pools[0].succeeded, 1);
    assert_eq!(snap.workers_connected, 1);
    assert_eq!(snap.totals.unwrap().succeeded, 1);
    server.stop().await;
}

#[tokio::test]
async fn aborted_slot_is_reusable_immediately() {
    // One slot: when the slow task times out, the queued task is dispatched into the same
    // slot in the same engine step. It must not fail for lack of a free worker.
    let server = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&server, 1, Arc::default());
    server.wait_capacity(1).await;
    let mut c = server.tasks(CLIENT).await;
    let no_retry = Some(pb::RetryPolicy {
        max_attempts: 1,
        ..Default::default()
    });
    let mut slow = spec(b"slow");
    slow.timeout_s = Some(0.3);
    slow.retry = no_retry;
    let mut next = spec(b"next");
    next.retry = no_retry;
    c.submit(submit(slow, false)).await.unwrap();
    let res = c
        .submit(submit(next, true))
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(
        res.status,
        pb::TaskStatus::Succeeded as i32,
        "{}",
        res.message
    );
    server.stop().await;
}

#[tokio::test]
async fn recurring_schedules_via_admin() {
    let server = TestServer::start("").await;
    let mut admin = server.admin(ADMIN).await;
    admin
        .add_recurring(pb::RecurringRequest {
            name: "nightly".into(),
            cron: "0 0 3 * * *".into(),
            tz: "UTC".into(),
            template: Some(spec(b"x")),
            overlap: pb::Overlap::Queue as i32,
        })
        .await
        .unwrap();
    // A relative deadline and a key prefix are per occurrence; absolute times and
    // dependencies describe a single task and are refused.
    let mut per_occurrence = spec(b"x");
    per_occurrence.key = "nightly".into();
    per_occurrence.deadline_in_s = Some(60.0);
    let mut absolute = spec(b"x");
    absolute.deadline_unix_ns = Some(1);
    let mut with_dep = spec(b"x");
    with_dep.deps.push(pb::Dep {
        target: by_key("other"),
        cond: pb::EdgeCond::OnSuccess as i32,
    });
    for (template, ok) in [(per_occurrence, true), (absolute, false), (with_dep, false)] {
        let r = admin
            .add_recurring(pb::RecurringRequest {
                name: "nightly".into(),
                cron: "0 0 3 * * *".into(),
                tz: "UTC".into(),
                template: Some(template),
                overlap: pb::Overlap::Skip as i32,
            })
            .await;
        assert_eq!(r.is_ok(), ok, "{r:?}");
    }
    let r = admin
        .remove_recurring(pb::RemoveRecurringRequest {
            name: "nightly".into(),
        })
        .await
        .unwrap();
    assert!(r.into_inner().removed);
    server.stop().await;
}

// Keys of the form `<prefix>@<UTC time>` belong to recurring occurrences; a client taking one in
// advance could stand in for a scheduled run.
#[tokio::test]
async fn occurrence_shaped_keys_are_reserved() {
    let server = TestServer::start("").await;
    let mut c = server.tasks(CLIENT).await;
    let mut squat = spec(b"x");
    squat.key = "nightly@2026-10-23T03:00:00Z".into();
    let e = c.submit(submit(squat, false)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let mut ok = spec(b"x");
    ok.key = "user@example.com".into();
    assert!(c.submit(submit(ok, false)).await.is_ok());
    server.stop().await;
}
