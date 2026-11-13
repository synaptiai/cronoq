//! `on_key_conflict = reject`: a duplicate key that asks for something else is an error with
//! the existing task's id and status, over gRPC and HTTP, in batches and after a restart.

mod common;

use common::*;
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

const BATCH_ONLY: &str = "batch-client-token-0123456789abcdef";

fn keyed(payload: &[u8], key: &str) -> pb::TaskSpec {
    pb::TaskSpec {
        key: key.into(),
        ..spec(payload)
    }
}

fn reject(spec: pb::TaskSpec) -> pb::SubmitRequest {
    pb::SubmitRequest {
        on_key_conflict: pb::KeyConflict::Reject as i32,
        ..submit(spec, false)
    }
}

/// The task and status a conflict names in its metadata.
fn conflict(e: &tonic::Status) -> (String, String) {
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    let get = |name: &str| {
        e.metadata()
            .get(name)
            .unwrap_or_else(|| panic!("no {name} in {e}"))
            .to_str()
            .unwrap()
            .to_string()
    };
    (get("cronoq-conflict-task"), get("cronoq-conflict-status"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_is_a_duplicate_and_other_content_is_a_conflict() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    // Submitted without the option: the fingerprint is stored all the same.
    let first = c
        .submit(submit(keyed(b"edit-1", "op-7"), false))
        .await
        .unwrap()
        .into_inner();

    let again = c
        .submit(reject(keyed(b"edit-1", "op-7")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((again.id, again.duplicate), (first.id, true));
    assert_eq!(again.outcome, pb::SubmitOutcome::Existing as i32);

    // What may differ between a submission and its retry does not make a conflict.
    let mut retried = keyed(b"edit-1", "op-7");
    retried.deadline_in_s = Some(30.0);
    retried.timeout_s = Some(5.0);
    retried.tags = [("traceparent".to_string(), "00-abc-def-01".to_string())].into();
    assert_eq!(
        c.submit(reject(retried)).await.unwrap().into_inner().id,
        first.id
    );

    // Other content does: payload, content type, kind, priority or dependencies.
    let mut other_kind = keyed(b"edit-1", "op-7");
    other_kind.kind = "rename".into();
    let mut other_class = keyed(b"edit-1", "op-7");
    other_class.priority = pb::Priority::High as i32;
    let mut other_type = keyed(b"edit-1", "op-7");
    other_type.content_type = "application/json".into();
    let mut other_deps = keyed(b"edit-1", "op-7");
    other_deps.deps = vec![pb::Dep {
        target: by_id(first.id),
        cond: pb::EdgeCond::OnSuccess as i32,
    }];
    for different in [
        keyed(b"edit-2", "op-7"),
        other_kind,
        other_class,
        other_type,
        other_deps,
    ] {
        let e = c.submit(reject(different.clone())).await.unwrap_err();
        assert_eq!(conflict(&e), (first.id.to_string(), "ready".to_string()));
        // Without the option the same submission is still answered as a duplicate.
        let plain = c
            .submit(submit(different, false))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((plain.id, plain.duplicate), (first.id, true));
    }

    // The task is unchanged, and still answers for its key once it has finished.
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(
        (lease.task_id, &lease.payload[..]),
        (first.id, &b"edit-1"[..])
    );
    w.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease: lease.lease,
            output: Vec::new(),
            content_type: String::new(),
        })),
    })
    .await
    .unwrap();
    c.wait(pb::WaitRequest {
        task: by_id(first.id),
        timeout_s: Some(10.0),
    })
    .await
    .unwrap();
    let e = c
        .submit(reject(keyed(b"edit-2", "op-7")))
        .await
        .unwrap_err();
    assert_eq!(
        conflict(&e),
        (first.id.to_string(), "succeeded".to_string())
    );
    assert_eq!(
        c.submit(reject(keyed(b"edit-1", "op-7")))
            .await
            .unwrap()
            .into_inner()
            .id,
        first.id
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replacement_stores_its_own_fingerprint() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let first = c
        .submit(submit(keyed(b"v1", "title"), false))
        .await
        .unwrap()
        .into_inner();
    c.submit(replace(keyed(b"v2", "title"), false))
        .await
        .unwrap();
    assert_eq!(
        c.submit(reject(keyed(b"v2", "title")))
            .await
            .unwrap()
            .into_inner()
            .id,
        first.id
    );
    let e = c.submit(reject(keyed(b"v1", "title"))).await.unwrap_err();
    assert_eq!(conflict(&e).0, first.id.to_string());
    // An update of the task leaves the fingerprint: a retry of the submission still matches.
    c.update(pb::UpdateRequest {
        task: by_id(first.id),
        priority: pb::Priority::High as i32,
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(c.submit(reject(keyed(b"v2", "title"))).await.is_ok());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_default_can_be_reject_and_a_submission_can_opt_out() {
    let s = TestServer::start_toml(&format!(
        "{TOKENS}\n{DEFAULT_POOL}\n[server]\ngrpc_addr = \"127.0.0.1:0\"\n\
         http_addr = \"127.0.0.1:0\"\non_key_conflict = \"reject\"\n"
    ))
    .await;
    let mut c = s.tasks(CLIENT).await;
    let first = c
        .submit(submit(keyed(b"a", "k"), false))
        .await
        .unwrap()
        .into_inner();
    let e = c.submit(submit(keyed(b"b", "k"), false)).await.unwrap_err();
    assert_eq!(conflict(&e).0, first.id.to_string());
    let opt_out = pb::SubmitRequest {
        on_key_conflict: pb::KeyConflict::ReturnExisting as i32,
        ..submit(keyed(b"b", "k"), false)
    };
    assert_eq!(c.submit(opt_out).await.unwrap().into_inner().id, first.id);
    // Batches follow the default too.
    let e = c
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![keyed(b"new", "n"), keyed(b"b", "k")],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(conflict(&e).0, first.id.to_string());
    assert_eq!(
        e.metadata()
            .get("cronoq-batch-index")
            .unwrap()
            .to_str()
            .unwrap(),
        "1"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_with_a_conflict_admits_nothing_and_names_the_task() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let first = c
        .submit(submit(keyed(b"a", "k"), false))
        .await
        .unwrap()
        .into_inner();
    let batch = |mode: pb::KeyConflict| pb::SubmitBatchRequest {
        tasks: vec![
            keyed(b"x", "new-x"),
            keyed(b"a", "k"),
            keyed(b"b", "k"),
            keyed(b"y", "new-y"),
        ],
        on_key_conflict: mode as i32,
    };
    let e = c
        .submit_batch(batch(pb::KeyConflict::Reject))
        .await
        .unwrap_err();
    assert_eq!(conflict(&e), (first.id.to_string(), "ready".to_string()));
    assert_eq!(
        e.metadata()
            .get("cronoq-batch-index")
            .unwrap()
            .to_str()
            .unwrap(),
        "2"
    );
    assert!(e.message().starts_with("tasks[2]:"), "{e}");
    // Nothing was admitted: the new keys are still unknown.
    let e = c
        .get(pb::GetRequest {
            task: by_key("new-x"),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    // Replacing by key is for single submissions.
    let e = c
        .submit_batch(batch(pb::KeyConflict::ReplaceQueued))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    // Without the option the batch goes in.
    let r = c
        .submit_batch(batch(pb::KeyConflict::Unspecified))
        .await
        .unwrap()
        .into_inner();
    let outcomes: Vec<i32> = r.tasks.iter().map(|t| t.outcome).collect();
    let (created, existing) = (
        pb::SubmitOutcome::Created as i32,
        pb::SubmitOutcome::Existing as i32,
    );
    assert_eq!(outcomes, [created, existing, existing, created]);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_answers_a_conflict_with_422_and_the_existing_task() {
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
    let (_, created) = post("/v1/tasks", json!({"payload": {"n": 1}, "key": "op"})).await;
    let id = created["id"].clone();
    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": {"n": 1}, "key": "op", "on_key_conflict": "reject"}),
    )
    .await;
    assert_eq!(
        (code, &body["outcome"]),
        (200, &json!("existing")),
        "{body}"
    );
    let (code, body) = post(
        "/v1/tasks",
        json!({"payload": {"n": 2}, "key": "op", "on_key_conflict": "reject"}),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert_eq!(body["code"], "unprocessable_entity");
    assert_eq!(
        (&body["task_id"], &body["task_status"]),
        (&id, &json!("ready"))
    );
    // The other error about a key, a fixed-start replacement, stays a 409.
    let (code, _) = post(
        "/v1/tasks",
        json!({"payload": 1, "key": "op", "start_in_s": 60, "on_key_conflict": "replace_queued"}),
    )
    .await;
    assert_eq!(code, 409);

    let (code, body) = post(
        "/v1/tasks:batch",
        json!({"on_key_conflict": "reject",
               "tasks": [{"payload": 1, "key": "fresh"}, {"payload": {"n": 2}, "key": "op"}]}),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert_eq!((&body["index"], &body["task_id"]), (&json!(1), &id));
    let (code, body) = post(
        "/v1/tasks:batch",
        json!({"on_key_conflict": "replace_queued", "tasks": [{"payload": 1, "key": "op"}]}),
    )
    .await;
    assert_eq!(code, 400, "{body}");
    let (code, body) = post(
        "/v1/tasks:batch",
        json!({"tasks": [{"payload": 1, "key": "fresh"}, {"payload": {"n": 2}, "key": "op"}]}),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["tasks"][1]["outcome"], "existing");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_held_in_another_pool_is_not_described_to_a_scoped_token() {
    let s = TestServer::start(&format!(
        "[[pools]]\nname = \"default\"\n[[pools]]\nname = \"batch\"\n\
         [[auth.tokens]]\ntoken = \"{BATCH_ONLY}\"\nrole = \"client\"\npools = [\"batch\"]\n"
    ))
    .await;
    let mut any = s.tasks(CLIENT).await;
    let mut scoped = s.tasks(BATCH_ONLY).await;
    any.submit(submit(keyed(b"a", "k"), false)).await.unwrap();
    let mut in_batch = keyed(b"b", "k");
    in_batch.pool = "batch".into();
    let e = scoped.submit(reject(in_batch)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists);
    assert!(e.metadata().get("cronoq-conflict-task").is_none());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicts_are_detected_after_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-conflict-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );
    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let live = c
        .submit(submit(keyed(b"live-1", "live"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let done = c
        .submit(submit(keyed(b"done-1", "done"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    c.cancel(pb::CancelRequest { task: by_id(done) })
        .await
        .unwrap();
    s.stop().await;

    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    for (payload, key, id, status) in [
        (&b"live-2"[..], "live", live, "ready"),
        (b"done-2", "done", done, "cancelled"),
    ] {
        let e = c.submit(reject(keyed(payload, key))).await.unwrap_err();
        assert_eq!(conflict(&e), (id.to_string(), status.to_string()));
    }
    for (payload, key, id) in [(&b"live-1"[..], "live", live), (b"done-1", "done", done)] {
        let r = c
            .submit(reject(keyed(payload, key)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.id, r.duplicate), (id, true));
    }
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
