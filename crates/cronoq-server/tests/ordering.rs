//! Ordering keys over gRPC and HTTP: one task of a key at a time in submission order, a key
//! halted by a failed task and resumed, a class floor, and all of it after a restart.

mod common;

use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

const ACME_ADMIN: &str = "acme-admin-token-0123456789abcdef";
const BATCH_ADMIN: &str = "batch-admin-token-0123456789abcdef";

fn ordered(payload: &[u8], key: &str) -> pb::TaskSpec {
    pb::TaskSpec {
        ordering_key: key.into(),
        ..spec(payload)
    }
}

fn halting(payload: &[u8], key: &str) -> pb::TaskSpec {
    pb::TaskSpec {
        on_failure: pb::OrderFailure::Halt as i32,
        retry: Some(pb::RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        }),
        ..ordered(payload, key)
    }
}

fn target(key: &str) -> pb::OrderingRequest {
    pb::OrderingRequest {
        pool: String::new(),
        key: key.into(),
    }
}

async fn finish(w: &RawWorker, lease: u64, ok: bool) {
    let msg = if ok {
        pb::worker_message::Msg::Complete(pb::Complete {
            lease,
            output: Vec::new(),
            content_type: String::new(),
        })
    } else {
        pb::worker_message::Msg::Fail(pb::Fail {
            lease,
            message: "boom".into(),
            permanent: true,
            retry_after_s: None,
        })
    };
    w.tx.send(pb::WorkerMessage { msg: Some(msg) })
        .await
        .unwrap();
}

/// No lease arrives for a while.
async fn no_lease(w: &mut RawWorker) {
    let r = tokio::time::timeout(Duration::from_millis(300), next_lease(w)).await;
    assert!(r.is_err(), "a task was offered: {:?}", r.unwrap().payload);
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_of_a_key_are_leased_one_at_a_time_in_order() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut admin = s.admin(METRICS).await;
    let mut ids = Vec::new();
    // A low task first, then a critical one: the order inside a key is submission order.
    for (payload, class) in [
        (&b"one"[..], pb::Priority::Low),
        (b"two", pb::Priority::Critical),
        (b"three", pb::Priority::Normal),
    ] {
        let mut t = ordered(payload, "account-1");
        t.priority = class as i32;
        ids.push(c.submit(submit(t, false)).await.unwrap().into_inner().id);
    }
    let other = c
        .submit(submit(ordered(b"other", "account-2"), false))
        .await
        .unwrap()
        .into_inner()
        .id;

    let info = admin
        .get_ordering(target("account-1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((info.tasks, info.head), (3, Some(ids[0])));
    // The head runs at the highest class waiting in its key.
    assert_eq!(info.effective, pb::Priority::Critical as i32);
    assert!(!info.halted && info.floor.is_none());
    let snap = s.snapshot().await;
    assert_eq!(snap["pools"][0]["ordering"]["keys"], 2);
    assert_eq!(snap["pools"][0]["ordering"]["waiting"], 2);

    // Four free slots: one task of each key is leased, the rest wait.
    let mut w = raw_worker(&s, WORKER, register("default", 4))
        .await
        .unwrap();
    let a = next_lease(&mut w).await;
    let b = next_lease(&mut w).await;
    let mut first: Vec<u64> = vec![a.task_id, b.task_id];
    first.sort();
    let mut want = vec![ids[0], other];
    want.sort();
    assert_eq!(first, want);
    no_lease(&mut w).await;
    let head = if a.task_id == ids[0] {
        a.lease
    } else {
        b.lease
    };
    finish(&w, head, true).await;
    let two = next_lease(&mut w).await;
    assert_eq!((two.task_id, &two.payload[..]), (ids[1], &b"two"[..]));
    no_lease(&mut w).await;
    finish(&w, two.lease, true).await;
    let three = next_lease(&mut w).await;
    assert_eq!(three.task_id, ids[2]);
    finish(&w, three.lease, true).await;
    c.wait(pb::WaitRequest {
        task: by_id(ids[2]),
        timeout_s: Some(10.0),
    })
    .await
    .unwrap();
    // A key with no tasks left is forgotten.
    let info = admin
        .get_ordering(target("account-1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (info.tasks, info.head, info.key.as_str()),
        (0, None, "account-1")
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_task_halts_its_key_until_an_admin_resumes_it() {
    let tokens = format!(
        "{DEFAULT_POOL}\n[[pools]]\nname = \"batch\"\nmax = 2\n\
         [[auth.tokens]]\ntoken = \"{ACME_ADMIN}\"\nrole = \"admin\"\ntenant = \"acme\"\n\
         [[auth.tokens]]\ntoken = \"{BATCH_ADMIN}\"\nrole = \"admin\"\npools = [\"batch\"]\n"
    );
    let s = TestServer::start(&tokens).await;
    let mut c = s.tasks(CLIENT).await;
    let bad = c
        .submit(submit(halting(b"bad", "acct"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let next = c
        .submit(submit(ordered(b"next", "acct"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let mut w = raw_worker(&s, WORKER, register("default", 2))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, bad);
    finish(&w, lease.lease, false).await;
    no_lease(&mut w).await;

    let mut metrics = s.admin(METRICS).await;
    let info = metrics
        .get_ordering(target("acct"))
        .await
        .unwrap()
        .into_inner();
    assert!(info.halted);
    assert_eq!((info.tasks, info.head), (1, Some(next)));
    let halted = metrics
        .list_halted_orderings(pb::ListHaltedOrderingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(halted.keys, ["acct"]);
    assert_eq!(s.snapshot().await["pools"][0]["ordering"]["halted"], 1);
    // Submissions are still accepted and wait behind.
    let late = c
        .submit(submit(ordered(b"late", "acct"), false))
        .await
        .unwrap()
        .into_inner()
        .id;

    // Who may resume: the admin role, for a pool it may use, not pinned to a tenant.
    let resume = || pb::ResumeRequest {
        pool: String::new(),
        ordering_key: "acct".into(),
    };
    for token in [METRICS, CLIENT, ACME_ADMIN, BATCH_ADMIN] {
        let e = s.admin(token).await.resume(resume()).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied, "{token}: {e}");
    }
    let e = metrics
        .set_ordering_floor(pb::SetOrderingFloorRequest {
            key: "acct".into(),
            floor: Some(pb::Priority::High as i32),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    no_lease(&mut w).await;

    let mut admin = s.admin(ADMIN).await;
    let e = admin
        .resume(pb::ResumeRequest::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let e = admin
        .resume(pb::ResumeRequest {
            pool: "nope".into(),
            ordering_key: "acct".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e}");
    assert!(admin.resume(resume()).await.unwrap().into_inner().resumed);
    assert!(!admin.resume(resume()).await.unwrap().into_inner().resumed);
    for id in [next, late] {
        let lease = next_lease(&mut w).await;
        assert_eq!(lease.task_id, id);
        finish(&w, lease.lease, true).await;
    }
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_floor_lifts_a_key_and_the_number_of_kept_floors_is_limited() {
    let s = TestServer::start("[limits]\nmax_ordering_settings = 1\n").await;
    let mut c = s.tasks(CLIENT).await;
    let mut admin = s.admin(ADMIN).await;
    let floor = |key: &str, floor: Option<pb::Priority>, persist| pb::SetOrderingFloorRequest {
        pool: String::new(),
        key: key.into(),
        floor: floor.map(|f| f as i32),
        persist,
    };
    // A normal task, then a low one in a key with a high floor: the low one is leased first.
    let normal = c
        .submit(submit(spec(b"normal"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let mut low = ordered(b"low", "vip");
    low.priority = pb::Priority::Low as i32;
    let low = c.submit(submit(low, false)).await.unwrap().into_inner().id;
    admin
        .set_ordering_floor(floor("vip", Some(pb::Priority::High), true))
        .await
        .unwrap();
    let info = admin
        .get_ordering(target("vip"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (info.floor, info.effective, info.persist),
        (
            Some(pb::Priority::High as i32),
            pb::Priority::High as i32,
            true
        )
    );
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let first = next_lease(&mut w).await;
    assert_eq!(first.task_id, low);
    finish(&w, first.lease, true).await;
    let second = next_lease(&mut w).await;
    assert_eq!(second.task_id, normal);
    finish(&w, second.lease, true).await;
    c.wait(pb::WaitRequest {
        task: by_id(normal),
        timeout_s: Some(10.0),
    })
    .await
    .unwrap();
    // Kept with no tasks; a second kept floor is over the limit, one that is not kept is not.
    let info = admin
        .get_ordering(target("vip"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (info.tasks, info.floor),
        (0, Some(pb::Priority::High as i32))
    );
    let e = admin
        .set_ordering_floor(floor("other", Some(pb::Priority::High), true))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e}");
    admin
        .set_ordering_floor(floor("other", Some(pb::Priority::High), false))
        .await
        .unwrap();
    // What does not make sense is refused.
    for bad in [
        floor("vip", None, true),
        floor("", Some(pb::Priority::High), false),
        pb::SetOrderingFloorRequest {
            floor: Some(99),
            ..floor("vip", None, false)
        },
        // "Unspecified" is not a class: it must not become a floor of normal.
        pb::SetOrderingFloorRequest {
            floor: Some(pb::Priority::Unspecified as i32),
            ..floor("vip", None, false)
        },
    ] {
        let e = admin.set_ordering_floor(bad).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    }
    admin
        .set_ordering_floor(floor("vip", None, false))
        .await
        .unwrap();
    let info = admin
        .get_ordering(target("vip"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((info.floor, info.persist), (None, false));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn what_cannot_have_an_ordering_key_is_refused() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut fixed = ordered(b"x", "k");
    fixed.start_in_s = Some(60.0);
    let mut policy_only = spec(b"x");
    policy_only.on_failure = pb::OrderFailure::Halt as i32;
    let mut unknown = ordered(b"x", "k");
    unknown.on_failure = 9;
    let long = ordered(b"x", &"k".repeat(300));
    for bad in [fixed, policy_only, unknown, long] {
        let e = c.submit(submit(bad, false)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    }
    let e = s
        .admin(ADMIN)
        .await
        .add_recurring(pb::RecurringRequest {
            name: "nightly".into(),
            cron: "0 3 * * *".into(),
            template: Some(ordered(b"x", "k")),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    // With `reject`, a key held by a task of another ordering key is a conflict: the caller
    // would believe its submission is in a key it is not in.
    let mut first = ordered(b"same", "a");
    first.key = "once".into();
    let id = c
        .submit(submit(first.clone(), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let reject = |spec: pb::TaskSpec| pb::SubmitRequest {
        on_key_conflict: pb::KeyConflict::Reject as i32,
        ..submit(spec, false)
    };
    let again = c.submit(reject(first.clone())).await.unwrap().into_inner();
    assert_eq!(again.id, id);
    let mut moved = first.clone();
    moved.ordering_key = "b".into();
    let e = c.submit(reject(moved)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    let named = e.metadata().get("cronoq-conflict-task").unwrap();
    assert_eq!(named.to_str().unwrap(), id.to_string());
    // A dependency on a task behind it in the key can never be met.
    let mut waits = ordered(b"x", "k");
    waits.key = "first".into();
    waits.deps = vec![pb::Dep {
        target: by_key("first"),
        cond: pb::EdgeCond::OnSuccess as i32,
    }];
    assert!(c.submit(submit(waits, false)).await.is_err());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_submits_reads_and_changes_ordering_keys() {
    let s = TestServer::start("").await;
    let http = reqwest::Client::new();
    let call = |method: reqwest::Method, token: &str, path: &str, body: Option<Value>| {
        let mut r = http
            .request(method, s.http(path))
            .header("authorization", format!("Bearer {token}"));
        if let Some(b) = body {
            r = r.json(&b);
        }
        async move {
            let r = r.send().await.unwrap();
            (r.status().as_u16(), r.json::<Value>().await.unwrap())
        }
    };
    use reqwest::Method as M;
    let (code, bad) = call(
        M::POST,
        CLIENT,
        "/v1/tasks",
        Some(
            json!({"payload": 1, "ordering_key": "acct", "on_failure": "halt",
                    "retry": {"max_attempts": 1}}),
        ),
    )
    .await;
    assert_eq!(code, 200, "{bad}");
    let (_, next) = call(
        M::POST,
        CLIENT,
        "/v1/tasks",
        Some(json!({"payload": 2, "ordering_key": "acct", "priority": "low"})),
    )
    .await;
    for (body, what) in [
        (
            json!({"payload": 1, "on_failure": "halt"}),
            "on_failure requires",
        ),
        (
            json!({"payload": 1, "ordering_key": "k", "on_failure": "stop"}),
            "unknown on_failure",
        ),
        (
            json!({"payload": 1, "ordering_key": "k", "start_in_s": 60}),
            "fixed start",
        ),
    ] {
        let (code, e) = call(M::POST, CLIENT, "/v1/tasks", Some(body)).await;
        assert_eq!(code, 400, "{e}");
        assert!(e["error"].as_str().unwrap().contains(what), "{e}");
    }

    let key = "/v1/pools/default/ordering/acct";
    let (code, info) = call(M::GET, METRICS, key, None).await;
    assert_eq!(code, 200, "{info}");
    assert_eq!(info["tasks"], 2);
    assert_eq!(info["head"], bad["id"]);
    assert_eq!(info["effective"], "normal");
    assert_eq!(info["halted"], false);

    // The first task fails: the key is halted.
    let mut w = raw_worker(&s, WORKER, register("default", 2))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    finish(&w, lease.lease, false).await;
    no_lease(&mut w).await;
    let (_, info) = call(M::GET, METRICS, key, None).await;
    assert_eq!(info["halted"], true);
    assert_eq!(info["head"], next["id"]);
    let (code, list) = call(
        M::GET,
        METRICS,
        "/v1/pools/default/ordering?halted=true",
        None,
    )
    .await;
    assert_eq!((code, &list["keys"]), (200, &json!(["acct"])));
    let (code, _) = call(M::GET, METRICS, "/v1/pools/default/ordering", None).await;
    assert_eq!(code, 400);
    let snap = s.snapshot().await;
    assert_eq!(snap["pools"][0]["ordering"]["halted"], 1);

    // A floor: admin only.
    let floor = format!("{key}/floor");
    let body = json!({"floor": "high", "persist": true});
    let (code, _) = call(M::PUT, METRICS, &floor, Some(body.clone())).await;
    assert_eq!(code, 403);
    let (code, info) = call(M::PUT, ADMIN, &floor, Some(body)).await;
    assert_eq!(code, 200, "{info}");
    assert_eq!(
        (&info["floor"], &info["effective"]),
        (&json!("high"), &json!("high"))
    );
    assert_eq!(info["persist"], true);
    let (code, e) = call(M::PUT, ADMIN, &floor, Some(json!({"floor": "urgent"}))).await;
    assert_eq!(code, 400, "{e}");
    let (code, info) = call(M::DELETE, ADMIN, &floor, None).await;
    assert_eq!(code, 200, "{info}");
    assert_eq!(
        (&info["floor"], &info["effective"]),
        (&Value::Null, &json!("low"))
    );

    // Resume: admin only, and idempotent.
    let resume = format!("{key}/resume");
    let (code, _) = call(M::POST, CLIENT, &resume, None).await;
    assert_eq!(code, 403);
    let (code, r) = call(M::POST, ADMIN, &resume, None).await;
    assert_eq!((code, &r["resumed"]), (200, &json!(true)));
    let (_, r) = call(M::POST, ADMIN, &resume, None).await;
    assert_eq!(r["resumed"], false);
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id.to_string(), next["id"].as_str().unwrap());
    let (code, e) = call(M::POST, ADMIN, "/v1/pools/nope/ordering/acct/resume", None).await;
    assert_eq!(code, 404, "{e}");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_keys_survive_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-ordering-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );
    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let bad = c
        .submit(submit(halting(b"bad", "acct"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let mut ids = Vec::new();
    for payload in [&b"b"[..], b"c", b"d"] {
        ids.push(
            c.submit(submit(ordered(payload, "acct"), false))
                .await
                .unwrap()
                .into_inner()
                .id,
        );
    }
    s.admin(ADMIN)
        .await
        .set_ordering_floor(pb::SetOrderingFloorRequest {
            key: "vip".into(),
            floor: Some(pb::Priority::Critical as i32),
            persist: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut w = raw_worker(&s, WORKER, register("default", 4))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, bad);
    finish(&w, lease.lease, false).await;
    c.wait(pb::WaitRequest {
        task: by_id(bad),
        timeout_s: Some(10.0),
    })
    .await
    .unwrap();
    drop(w);
    s.stop().await;

    let s = TestServer::start(&store).await;
    let mut admin = s.admin(ADMIN).await;
    let info = admin
        .get_ordering(target("acct"))
        .await
        .unwrap()
        .into_inner();
    assert!(info.halted);
    assert_eq!((info.tasks, info.head), (3, Some(ids[0])));
    let vip = admin
        .get_ordering(target("vip"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (vip.floor, vip.persist),
        (Some(pb::Priority::Critical as i32), true)
    );
    let mut w = raw_worker(&s, WORKER, register("default", 4))
        .await
        .unwrap();
    no_lease(&mut w).await;
    assert!(
        admin
            .resume(pb::ResumeRequest {
                pool: String::new(),
                ordering_key: "acct".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .resumed
    );
    // The tasks come in the order they were submitted before the restart, one at a time.
    for id in &ids {
        let lease = next_lease(&mut w).await;
        assert_eq!(lease.task_id, *id);
        no_lease(&mut w).await;
        finish(&w, lease.lease, true).await;
    }
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
