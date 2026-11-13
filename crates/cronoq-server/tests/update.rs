//! Updating a task that has not finished: gRPC, HTTP, events, permissions and durability.

mod common;

use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

const ACME: &str = "acme-client-token-0123456789abcdef0";
const BATCH_ONLY: &str = "batch-client-token-0123456789abcdef";

fn update(task: Option<pb::TaskRef>) -> pb::UpdateRequest {
    pb::UpdateRequest {
        task,
        ..Default::default()
    }
}

fn priority(task: Option<pb::TaskRef>, p: pb::Priority) -> pb::UpdateRequest {
    pb::UpdateRequest {
        priority: p as i32,
        ..update(task)
    }
}

async fn info(c: &mut pb::tasks_client::TasksClient<Intercepted>, id: u64) -> pb::TaskInfo {
    c.get(pb::GetRequest { task: by_id(id) })
        .await
        .unwrap()
        .into_inner()
}

async fn queued(
    c: &mut pb::tasks_client::TasksClient<Intercepted>,
    mut s: pb::TaskSpec,
    p: pb::Priority,
) -> u64 {
    s.priority = p as i32;
    c.submit(submit(s, false)).await.unwrap().into_inner().id
}

#[tokio::test(flavor = "multi_thread")]
async fn update_changes_a_queued_task_and_reports_what_happened() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut keyed = spec(b"x");
    keyed.key = "k".into();
    let id = queued(&mut c, keyed, pb::Priority::Low).await;
    assert_eq!(info(&mut c, id).await.priority, pb::Priority::Low as i32);

    let r = c
        .update(priority(by_id(id), pb::Priority::High))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((r.id, r.changed, r.deferred), (id, true, false));
    assert_eq!(r.status, pb::TaskStatus::Ready as i32);
    assert_eq!(info(&mut c, id).await.priority, pb::Priority::High as i32);

    // The same values again: success, nothing changed. By key this time.
    let r = c
        .update(priority(by_key("k"), pb::Priority::High))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((r.id, r.changed), (id, false));

    // Nothing named: also a no-op.
    assert!(
        !c.update(update(by_id(id)))
            .await
            .unwrap()
            .into_inner()
            .changed
    );

    // Deadline and tags in one call.
    let r = c
        .update(pb::UpdateRequest {
            deadline_in_s: Some(60.0),
            set_tags: true,
            tags: [("team".to_string(), "search".to_string())].into(),
            ..update(by_id(id))
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.changed);
    // Removing both again.
    let r = c
        .update(pb::UpdateRequest {
            clear_deadline: true,
            set_tags: true,
            ..update(by_id(id))
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.changed);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_refuses_bad_requests_and_tasks_it_cannot_change() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let id = queued(&mut c, spec(b"x"), pb::Priority::Normal).await;
    let code =
        |r: Result<tonic::Response<pb::UpdateResponse>, tonic::Status>| r.unwrap_err().code();

    assert_eq!(
        code(c.update(priority(by_id(999_999), pb::Priority::High)).await),
        tonic::Code::NotFound
    );
    assert_eq!(
        code(c.update(priority(by_key("nope"), pb::Priority::High)).await),
        tonic::Code::NotFound
    );
    assert_eq!(
        code(c.update(priority(None, pb::Priority::High)).await),
        tonic::Code::InvalidArgument
    );
    for bad in [
        pb::UpdateRequest {
            deadline_in_s: Some(1.0),
            deadline_unix_ns: Some(1),
            ..update(by_id(id))
        },
        pb::UpdateRequest {
            deadline_in_s: Some(1.0),
            clear_deadline: true,
            ..update(by_id(id))
        },
        pb::UpdateRequest {
            deadline_in_s: Some(-1.0),
            ..update(by_id(id))
        },
        pb::UpdateRequest {
            priority: 99,
            ..update(by_id(id))
        },
        // Tags without saying they replace the task's.
        pb::UpdateRequest {
            tags: [("a".to_string(), "b".to_string())].into(),
            ..update(by_id(id))
        },
        pb::UpdateRequest {
            set_tags: true,
            tags: [("cronoq.content_type".to_string(), "x".to_string())].into(),
            ..update(by_id(id))
        },
    ] {
        let e = c.update(bad.clone()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{bad:?}: {e}");
    }

    // A task with a fixed start holds a reservation sized at admission.
    let mut fixed = spec(b"f");
    fixed.start_in_s = Some(60.0);
    let fixed = queued(&mut c, fixed, pb::Priority::Normal).await;
    assert_eq!(
        code(c.update(priority(by_id(fixed), pb::Priority::High)).await),
        tonic::Code::FailedPrecondition
    );

    // A finished task.
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (stop, _join) = spawn_worker(&s, 2, seen);
    let done = c
        .submit(submit(spec(b"done"), true))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert_eq!(
        code(c.update(priority(by_id(done), pb::Priority::High)).await),
        tonic::Code::NotFound
    );
    let _ = stop.send(());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_promoted_task_is_leased_first_and_a_running_one_is_left_alone() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    s.wait_capacity(1).await;
    let running = queued(&mut c, spec(b"running"), pb::Priority::Normal).await;
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, running);

    let normal = queued(&mut c, spec(b"normal"), pb::Priority::Normal).await;
    let low = queued(&mut c, spec(b"low"), pb::Priority::Low).await;
    assert!(
        c.update(priority(by_id(low), pb::Priority::High))
            .await
            .unwrap()
            .into_inner()
            .changed
    );

    // The running attempt is not interrupted: the change is deferred.
    let r = c
        .update(priority(by_id(running), pb::Priority::Critical))
        .await
        .unwrap()
        .into_inner();
    assert!(r.deferred && r.changed);
    assert_eq!(r.status, pb::TaskStatus::Running as i32);
    // An update is not a transition: the attempt's start time stands. Wait for the update's
    // event to pass through the server before reading it.
    let started = info(&mut c, running).await.started_unix_ns;
    assert!(started.is_some());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = info(&mut c, running).await;
    assert_eq!(after.started_unix_ns, started);
    assert_eq!(after.status, pb::TaskStatus::Running as i32);
    assert_eq!(after.priority, pb::Priority::Critical as i32);

    let complete = |lease: u64| pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease,
            output: Vec::new(),
            content_type: String::new(),
        })),
    };
    w.tx.send(complete(lease.lease)).await.unwrap();
    let next = next_lease(&mut w).await;
    assert_eq!(next.task_id, low, "the promoted task goes first");
    w.tx.send(complete(next.lease)).await.unwrap();
    assert_eq!(next_lease(&mut w).await.task_id, normal);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_reports_updates_with_the_new_priority() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut terminal_only = c
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
    let id = queued(&mut c, spec(b"x"), pb::Priority::Low).await;
    c.update(priority(by_id(id), pb::Priority::High))
        .await
        .unwrap();

    let updated = loop {
        let e = tokio::time::timeout(Duration::from_secs(5), all.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if e.kind == pb::EventKind::Updated as i32 {
            break e;
        }
        // Transitions before the update carry the class the task had then.
        assert_eq!(e.kind, pb::EventKind::Transition as i32);
        assert_eq!(e.priority, pb::Priority::Low as i32);
    };
    assert_eq!(updated.id, id);
    assert_eq!(updated.priority, pb::Priority::High as i32);
    assert_eq!(updated.status, pb::TaskStatus::Ready as i32);

    // A stream of terminal events does not see the update, only the end of the task.
    c.cancel(pb::CancelRequest { task: by_id(id) })
        .await
        .unwrap();
    let e = tokio::time::timeout(Duration::from_secs(5), terminal_only.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(e.status, pb::TaskStatus::Cancelled as i32);
    assert_eq!(e.kind, pb::EventKind::Transition as i32);
    assert_eq!(e.priority, pb::Priority::High as i32);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_patch_updates_by_id_and_by_key() {
    let s = TestServer::start("").await;
    let http = reqwest::Client::new();
    let auth = format!("Bearer {CLIENT}");
    let created: Value = http
        .post(s.http("/v1/tasks"))
        .header("authorization", &auth)
        .json(&json!({"payload": "x", "key": "job/1", "priority": "low"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();

    let patch = |path: String, body: Value| {
        let (http, auth, url) = (http.clone(), auth.clone(), s.http(&path));
        async move {
            let r = http
                .patch(url)
                .header("authorization", auth)
                .json(&body)
                .send()
                .await
                .unwrap();
            (r.status().as_u16(), r.json::<Value>().await.unwrap())
        }
    };
    let (code, body) = patch(format!("/v1/tasks/{id}"), json!({"priority": "high"})).await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(
        body,
        json!({"id": id, "status": "ready", "deferred": false, "changed": true})
    );
    let (code, body) = patch(
        "/v1/tasks/by-key/job%2F1".into(),
        json!({"priority": "high", "deadline_in_s": 30, "tags": {"team": "search"}}),
    )
    .await;
    assert_eq!(
        (code, body["changed"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    let (code, body) = patch(
        format!("/v1/tasks/{id}"),
        json!({"clear_deadline": true, "tags": {}}),
    )
    .await;
    assert_eq!(
        (code, body["changed"].as_bool()),
        (200, Some(true)),
        "{body}"
    );

    let got: Value = http
        .get(s.http(&format!("/v1/tasks/{id}")))
        .header("authorization", &auth)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["priority"], "high");

    for (path, body, want) in [
        (
            format!("/v1/tasks/{id}"),
            json!({"priority": "urgent"}),
            400,
        ),
        (format!("/v1/tasks/{id}"), json!({"prio": "high"}), 400),
        (
            format!("/v1/tasks/{id}"),
            json!({"deadline_in_s": 1, "clear_deadline": true}),
            400,
        ),
        (
            "/v1/tasks/999999".to_string(),
            json!({"priority": "high"}),
            404,
        ),
        (
            "/v1/tasks/by-key/nope".to_string(),
            json!({"priority": "high"}),
            404,
        ),
    ] {
        let (code, out) = patch(path.clone(), body.clone()).await;
        assert_eq!(code, want, "{path} {body}: {out}");
        assert!(out["error"].is_string());
    }
    // Without credentials.
    let r = http
        .patch(s.http(&format!("/v1/tasks/{id}")))
        .json(&json!({"priority": "low"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_limit_which_tasks_can_be_updated() {
    let s = TestServer::start(&format!(
        "[[pools]]\nname = \"default\"\n[[pools]]\nname = \"batch\"\n\
         [[auth.tokens]]\ntoken = \"{ACME}\"\nrole = \"client\"\ntenant = \"acme\"\n\
         [[auth.tokens]]\ntoken = \"{BATCH_ONLY}\"\nrole = \"client\"\npools = [\"batch\"]\n"
    ))
    .await;
    let mut any = s.tasks(CLIENT).await;
    let mut acme = s.tasks(ACME).await;
    let mut batch = s.tasks(BATCH_ONLY).await;

    let mut theirs = spec(b"globex");
    theirs.tenant = "globex".into();
    let theirs = queued(&mut any, theirs, pb::Priority::Low).await;
    let untagged = queued(&mut any, spec(b"none"), pb::Priority::Low).await;
    let own = queued(&mut acme, spec(b"acme"), pb::Priority::Low).await;

    // Credentials pinned to a tenant reach that tenant's tasks only; others look absent.
    for id in [theirs, untagged] {
        let e = acme
            .update(priority(by_id(id), pb::Priority::High))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::NotFound);
        assert_eq!(info(&mut any, id).await.priority, pb::Priority::Low as i32);
    }
    assert!(
        acme.update(priority(by_id(own), pb::Priority::High))
            .await
            .unwrap()
            .into_inner()
            .changed
    );

    // By key as well.
    let mut keyed = spec(b"globex-keyed");
    keyed.tenant = "globex".into();
    keyed.key = "globex-key".into();
    queued(&mut any, keyed, pb::Priority::Low).await;
    let e = acme
        .update(priority(by_key("globex-key"), pb::Priority::High))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);

    // Cancel follows the same rule: a pinned token cancels its own tenant's tasks only, and
    // can still read the others.
    let e = acme
        .cancel(pb::CancelRequest {
            task: by_id(theirs),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    let e = acme
        .cancel(pb::CancelRequest {
            task: by_key("globex-key"),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    assert_eq!(
        info(&mut acme, theirs).await.status,
        pb::TaskStatus::Ready as i32
    );
    let mut mine = spec(b"acme-cancel");
    mine.key = "acme-key".into();
    let mine = queued(&mut acme, mine, pb::Priority::Low).await;
    // The second call finds the task already cancelled, and still as its own.
    for (target, cancelled) in [(by_key("acme-key"), true), (by_id(mine), false)] {
        let r = acme
            .cancel(pb::CancelRequest { task: target })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.cancelled, cancelled);
    }
    assert_eq!(
        info(&mut acme, mine).await.status,
        pb::TaskStatus::Cancelled as i32
    );
    assert!(
        any.cancel(pb::CancelRequest {
            task: by_id(theirs),
        })
        .await
        .unwrap()
        .into_inner()
        .cancelled
    );

    // Credentials limited to a pool reach that pool's tasks only.
    let e = batch
        .update(priority(by_id(own), pb::Priority::Critical))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    let mut in_batch = spec(b"b");
    in_batch.pool = "batch".into();
    let in_batch = queued(&mut batch, in_batch, pb::Priority::Low).await;
    assert!(
        batch
            .update(priority(by_id(in_batch), pb::Priority::High))
            .await
            .unwrap()
            .into_inner()
            .changed
    );

    // Workers and metrics readers cannot update at all.
    for token in [WORKER, METRICS] {
        let e = s
            .tasks(token)
            .await
            .update(priority(by_id(own), pb::Priority::Low))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied);
    }
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sending_the_tags_a_task_has_changes_nothing() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut tagged = spec(b"x");
    tagged.content_type = "text/plain".into();
    tagged.tags = [("team".to_string(), "search".to_string())].into();
    let id = queued(&mut c, tagged, pb::Priority::Normal).await;
    let tags = |v: &str| pb::UpdateRequest {
        set_tags: true,
        tags: [("team".to_string(), v.to_string())].into(),
        ..update(by_id(id))
    };
    assert!(!c.update(tags("search")).await.unwrap().into_inner().changed);
    assert!(c.update(tags("ads")).await.unwrap().into_inner().changed);
    assert!(!c.update(tags("ads")).await.unwrap().into_inner().changed);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_credentials_reach_their_tasks_after_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-update-tenant-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n\
         [[auth.tokens]]\ntoken = \"{ACME}\"\nrole = \"client\"\ntenant = \"acme\"\n",
        dir.join("state.db").display()
    );
    let s = TestServer::start(&cfg).await;
    let mut acme = s.tasks(ACME).await;
    let mut any = s.tasks(CLIENT).await;
    let a = queued(&mut acme, spec(b"a"), pb::Priority::Low).await;
    let b = queued(&mut acme, spec(b"b"), pb::Priority::Low).await;
    let mut theirs = spec(b"globex");
    theirs.tenant = "globex".into();
    let theirs = queued(&mut any, theirs, pb::Priority::Low).await;
    s.stop().await;

    // The new process knows these tasks from its store and from events only.
    let s = TestServer::start(&cfg).await;
    let mut acme = s.tasks(ACME).await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let leased = next_lease(&mut w).await.task_id;
    for id in [a, b] {
        let r = acme
            .update(priority(by_id(id), pb::Priority::High))
            .await
            .unwrap_or_else(|e| panic!("task {id} (leased: {leased}): {e}"))
            .into_inner();
        assert!(r.changed);
    }
    let e = acme
        .update(priority(by_id(theirs), pb::Priority::High))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-update-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );

    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let mut keyed = spec(b"x");
    keyed.key = "survivor".into();
    let id = queued(&mut c, keyed, pb::Priority::Low).await;
    let other = queued(&mut c, spec(b"y"), pb::Priority::Normal).await;
    assert!(
        c.update(pb::UpdateRequest {
            priority: pb::Priority::High as i32,
            deadline_in_s: Some(3600.0),
            ..update(by_id(id))
        })
        .await
        .unwrap()
        .into_inner()
        .changed
    );
    s.stop().await;

    // A new process on the same file: the task comes back with what the update gave it, and is
    // leased ahead of the normal task submitted after it.
    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let first = next_lease(&mut w).await;
    assert_eq!(first.task_id, id);
    assert!(first.deadline_unix_ns.is_some());
    assert_eq!(info(&mut c, id).await.priority, pb::Priority::High as i32);
    let r = c
        .update(priority(by_key("survivor"), pb::Priority::High))
        .await
        .unwrap()
        .into_inner();
    assert!(!r.changed && r.deferred);
    let _ = other;
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
