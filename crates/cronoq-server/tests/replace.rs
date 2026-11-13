//! `replace_queued` ("latest wins") over gRPC and HTTP: a task that has not started is
//! replaced in place, a started one is followed by a successor, across restarts.

mod common;

use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

const ACME: &str = "acme-client-token-0123456789abcdef0";
const BATCH_ONLY: &str = "batch-client-token-0123456789abcdef";

type Tasks = pb::tasks_client::TasksClient<Intercepted>;

fn keyed(payload: &[u8], key: &str) -> pb::TaskSpec {
    pb::TaskSpec {
        key: key.into(),
        ..spec(payload)
    }
}

async fn info(c: &mut Tasks, target: Option<pb::TaskRef>) -> pb::TaskInfo {
    c.get(pb::GetRequest { task: target })
        .await
        .unwrap()
        .into_inner()
}

fn complete(lease: u64) -> pb::WorkerMessage {
    pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease,
            output: Vec::new(),
            content_type: String::new(),
        })),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_task_is_replaced_in_place_and_runs_once_with_the_last_payload() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let first = c
        .submit(submit(keyed(b"v1", "title"), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.outcome, pb::SubmitOutcome::Created as i32);
    for v in [&b"v2"[..], b"v3"] {
        let mut newer = keyed(v, "title");
        newer.kind = "rename".into();
        newer.priority = pb::Priority::High as i32;
        newer.content_type = "text/x-title".into();
        let r = c.submit(replace(newer, false)).await.unwrap().into_inner();
        assert_eq!((r.id, r.duplicate), (first.id, true));
        assert_eq!(r.outcome, pb::SubmitOutcome::Replaced as i32);
    }
    // A plain submission of the key still returns the task, unchanged.
    let plain = c
        .submit(submit(keyed(b"v4", "title"), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (plain.id, plain.outcome),
        (first.id, pb::SubmitOutcome::Existing as i32)
    );
    let i = info(&mut c, by_id(first.id)).await;
    assert_eq!(
        (i.kind.as_str(), i.priority, i.status),
        (
            "rename",
            pb::Priority::High as i32,
            pb::TaskStatus::Ready as i32
        )
    );

    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(
        (
            lease.task_id,
            &lease.payload[..],
            lease.content_type.as_str()
        ),
        (first.id, &b"v3"[..], "text/x-title")
    );
    w.tx.send(complete(lease.lease)).await.unwrap();
    let snap = s.snapshot().await;
    assert_eq!(snap["totals"]["superseded"], 2);
    assert_eq!(snap["totals"]["submitted"], 1);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_started_task_keeps_its_payload_and_a_successor_follows_it() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut events = c
        .watch(pb::WatchRequest {
            all_transitions: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    s.wait_capacity(1).await;
    let first = c
        .submit(submit(keyed(b"v1", "title"), false))
        .await
        .unwrap()
        .into_inner();
    let lease = next_lease(&mut w).await;
    assert_eq!((lease.task_id, &lease.payload[..]), (first.id, &b"v1"[..]));

    for v in [&b"v2"[..], b"v3"] {
        let mut newer = keyed(v, "title");
        newer.kind = "rename".into();
        let r = c.submit(replace(newer, false)).await.unwrap().into_inner();
        assert_eq!((r.id, r.duplicate), (first.id, true));
        assert_eq!(r.outcome, pb::SubmitOutcome::Pending as i32);
    }
    // The key still names the running task.
    assert_eq!(info(&mut c, by_key("title")).await.id, first.id);

    w.tx.send(complete(lease.lease)).await.unwrap();
    let next = next_lease(&mut w).await;
    assert_ne!(next.task_id, first.id);
    assert_eq!(&next.payload[..], b"v3");
    assert_eq!(next.key, "title");
    // Now it names the successor, with the details of the submission that became it.
    let i = info(&mut c, by_key("title")).await;
    assert_eq!((i.id, i.kind.as_str()), (next.task_id, "rename"));
    assert_eq!(
        info(&mut c, by_id(first.id)).await.status,
        pb::TaskStatus::Succeeded as i32
    );

    // The stream reported both stored submissions on the running task, and the successor.
    let (mut replaced, mut successor) = (0, None);
    while successor.is_none() {
        let e = tokio::time::timeout(Duration::from_secs(5), events.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if e.kind == pb::EventKind::Replaced as i32 {
            assert_eq!((e.id, e.status), (first.id, pb::TaskStatus::Running as i32));
            replaced += 1;
        } else if e.kind == pb::EventKind::Successor as i32 {
            successor = Some(e);
        }
    }
    let e = successor.unwrap();
    assert_eq!(replaced, 2);
    assert_eq!((e.id, e.key.as_str()), (next.task_id, "title"));
    assert_eq!(e.message, format!("follows task {}", first.id));

    w.tx.send(complete(next.lease)).await.unwrap();
    let snap = s.snapshot().await;
    assert_eq!(snap["totals"]["superseded"], 1);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replacements_that_cannot_be_made_are_refused() {
    let s = TestServer::start(&format!(
        "[[pools]]\nname = \"default\"\n[[pools]]\nname = \"batch\"\n\
         [[auth.tokens]]\ntoken = \"{ACME}\"\nrole = \"client\"\ntenant = \"acme\"\n\
         [[auth.tokens]]\ntoken = \"{BATCH_ONLY}\"\nrole = \"client\"\npools = [\"batch\"]\n"
    ))
    .await;
    let mut any = s.tasks(CLIENT).await;
    let mut acme = s.tasks(ACME).await;
    let mut batch = s.tasks(BATCH_ONLY).await;
    let code =
        |r: Result<tonic::Response<pb::SubmitResponse>, tonic::Status>| r.unwrap_err().code();

    // Without a key there is nothing to replace; renewing needs the replace option.
    assert_eq!(
        code(any.submit(replace(spec(b"x"), false)).await),
        tonic::Code::InvalidArgument
    );
    let renew_only = pb::SubmitRequest {
        renew_position: true,
        ..submit(keyed(b"x", "k"), false)
    };
    assert_eq!(
        code(any.submit(renew_only).await),
        tonic::Code::InvalidArgument
    );
    let unknown = pb::SubmitRequest {
        on_key_conflict: 99,
        ..submit(keyed(b"x", "k"), false)
    };
    assert_eq!(
        code(any.submit(unknown).await),
        tonic::Code::InvalidArgument
    );

    // A replacement must fit where the task is: same pool, labels, dependencies, no fixed start.
    let id = any
        .submit(submit(keyed(b"v1", "k"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let mut other_pool = keyed(b"v2", "k");
    other_pool.pool = "batch".into();
    let mut labelled = keyed(b"v2", "k");
    labelled.labels = [("gpu".to_string(), "a100".to_string())].into();
    let mut fixed = keyed(b"v2", "k");
    fixed.start_in_s = Some(60.0);
    for bad in [other_pool, labelled, fixed] {
        let e = any.submit(replace(bad, false)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
        assert!(e.message().contains("cannot replace"), "{e}");
    }

    // A token pinned to a tenant cannot replace another tenant's task, or one without a tenant.
    let e = acme
        .submit(replace(keyed(b"mine", "k"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("the tenant differs"), "{e}");
    // Its own it can.
    let own = acme
        .submit(submit(keyed(b"a1", "acme-k"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let r = acme
        .submit(replace(keyed(b"a2", "acme-k"), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((r.id, r.outcome), (own, pb::SubmitOutcome::Replaced as i32));
    // A token limited to a pool cannot touch, or learn about, a key held in another pool.
    let mut in_batch = keyed(b"b", "k");
    in_batch.pool = "batch".into();
    let e = batch.submit(replace(in_batch, false)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists);

    // Nothing above changed the task.
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!((lease.task_id, &lease.payload[..]), (id, &b"v1"[..]));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_submissions_can_replace_by_key() {
    let s = TestServer::start("").await;
    let http = reqwest::Client::new();
    let auth = format!("Bearer {CLIENT}");
    let post = |path: &str, body: Value| {
        let (http, auth, url) = (http.clone(), auth.clone(), s.http(path));
        async move {
            let r = http
                .post(url)
                .header("authorization", auth)
                .json(&body)
                .send()
                .await
                .unwrap();
            (r.status().as_u16(), r.json::<Value>().await.unwrap())
        }
    };
    let (code, created) = post("/v1/tasks", json!({"payload": {"title": "a"}, "key": "t"})).await;
    assert_eq!(code, 200, "{created}");
    assert_eq!(created["outcome"], "created");
    let id = created["id"].clone();

    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": {"title": "b"}, "key": "t", "on_key_conflict": "replace_queued",
               "renew_position": true}),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(
        body,
        json!({"id": id, "duplicate": true, "outcome": "replaced"})
    );
    let (_, body) = post("/v1/tasks", json!({"payload": 1, "key": "t"})).await;
    assert_eq!(body["outcome"], "existing");

    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": 1, "key": "t", "on_key_conflict": "newest"}),
    )
    .await;
    assert_eq!(code, 400, "{body}");
    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": 1, "key": "t", "pool": "nope", "on_key_conflict": "replace_queued"}),
    )
    .await;
    assert_eq!(code, 404, "{body}");
    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": 1, "key": "t", "start_in_s": 60, "on_key_conflict": "replace_queued"}),
    )
    .await;
    assert_eq!(code, 409, "{body}");
    // Batches admit tasks as written.
    let (code, body) = post(
        "/v1/tasks:batch",
        json!({"tasks": [{"payload": 1, "key": "t", "on_key_conflict": "replace_queued"}]}),
    )
    .await;
    assert_eq!(code, 400, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("not supported in batches"),
        "{body}"
    );

    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(&lease.payload[..], br#"{"title":"b"}"#);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_replacement_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-replace-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );

    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    s.wait_capacity(1).await;
    let running = c
        .submit(submit(keyed(b"r1", "running"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert_eq!(next_lease(&mut w).await.task_id, running);
    let queued = c
        .submit(submit(keyed(b"q1", "queued"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    for (payload, key, outcome) in [
        (&b"r2"[..], "running", pb::SubmitOutcome::Pending),
        (b"q2", "queued", pb::SubmitOutcome::Replaced),
    ] {
        let r = c
            .submit(replace(keyed(payload, key), false))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.outcome, outcome as i32);
    }
    drop(w);
    s.stop().await;

    // A new process on the same file. The attempt that was running is lost and retried
    // (after its backoff) with the payload it had; the replaced task has its new payload; the
    // stored submission runs once the first task has ended.
    let s = TestServer::start(&store).await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let lease = tokio::time::timeout(Duration::from_secs(20), next_lease(&mut w))
            .await
            .expect("three attempts run after the restart");
        seen.push((lease.task_id, lease.payload.clone(), lease.key.clone()));
        w.tx.send(complete(lease.lease)).await.unwrap();
    }
    let of = |id: u64| seen.iter().position(|(i, ..)| *i == id).unwrap();
    assert_eq!(&seen[of(running)].1[..], b"r1");
    assert_eq!(&seen[of(queued)].1[..], b"q2");
    let successor = seen
        .iter()
        .position(|(id, _, key)| key == "running" && *id != running)
        .expect("the stored submission ran");
    assert_eq!(&seen[successor].1[..], b"r2");
    assert!(successor > of(running), "after the task it follows");
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
