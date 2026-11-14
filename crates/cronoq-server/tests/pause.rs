//! Pause and resume over gRPC and HTTP: a pool, one of its classes or one of its ordering
//! keys stops being leased while its tasks stay queued, and the pause survives a restart.

mod common;

use std::time::Duration;

use common::*;
use cronoq_proto::v1 as pb;
use serde_json::{Value, json};

const ACME_ADMIN: &str = "acme-admin-token-0123456789abcdef";
const BATCH_ADMIN: &str = "batch-admin-token-0123456789abcdef";

fn pause(class: Option<pb::Priority>, key: &str, reason: &str) -> pb::PauseRequest {
    pb::PauseRequest {
        pool: String::new(),
        priority_class: class.map(|c| c as i32),
        ordering_key: (!key.is_empty()).then(|| key.into()),
        reason: reason.into(),
    }
}

fn resume(class: Option<pb::Priority>, key: &str) -> pb::ResumeRequest {
    pb::ResumeRequest {
        pool: String::new(),
        ordering_key: (!key.is_empty()).then(|| key.into()),
        priority_class: class.map(|c| c as i32),
    }
}

async fn id_of(c: &mut pb::tasks_client::TasksClient<Intercepted>, t: pb::TaskSpec) -> u64 {
    c.submit(submit(t, false)).await.unwrap().into_inner().id
}

async fn complete(w: &RawWorker, lease: u64) {
    w.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease,
            output: Vec::new(),
            content_type: String::new(),
        })),
    })
    .await
    .unwrap();
}

async fn no_lease(w: &mut RawWorker) {
    let r = tokio::time::timeout(Duration::from_millis(300), next_lease(w)).await;
    assert!(r.is_err(), "a task was leased: {:?}", r.unwrap().payload);
}

/// The `Registered` message a new worker of the default pool gets.
async fn registered(s: &TestServer) -> (RawWorker, pb::Registered) {
    let mut w = raw_worker(s, WORKER, register("default", 2)).await.unwrap();
    loop {
        let m = w.inbound.message().await.unwrap().unwrap();
        if let Some(pb::server_message::Msg::Registered(r)) = m.msg {
            return (w, r);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_pool_leases_nothing_and_says_why() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut admin = s.admin(ADMIN).await;
    let r = admin.pause(pause(None, "", "deploy 14:00")).await.unwrap();
    assert!(r.into_inner().paused);
    let again = admin.pause(pause(None, "", "deploy 14:00")).await.unwrap();
    assert!(!again.into_inner().paused);
    // Submissions are accepted and wait.
    let a = id_of(&mut c, spec(b"a")).await;
    let (mut w, reg) = registered(&s).await;
    assert_eq!(reg.pool_paused.as_deref(), Some("deploy 14:00"));
    no_lease(&mut w).await;
    let snap = s.snapshot().await;
    let pool = &snap["pools"][0];
    assert_eq!(pool["paused"], "deploy 14:00");
    assert_eq!(pool["paused_tasks"], 1);
    let info = c.get(pb::GetRequest { task: by_id(a) }).await.unwrap();
    assert_eq!(info.into_inner().status, pb::TaskStatus::Ready as i32);

    assert!(
        admin
            .resume(resume(None, ""))
            .await
            .unwrap()
            .into_inner()
            .resumed
    );
    assert!(
        !admin
            .resume(resume(None, ""))
            .await
            .unwrap()
            .into_inner()
            .resumed
    );
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, a);
    complete(&w, lease.lease).await;
    assert_eq!(s.snapshot().await["pools"][0]["paused"], Value::Null);
    // A worker that registers now is not told of a pause.
    let (_w2, reg) = registered(&s).await;
    assert_eq!(reg.pool_paused, None);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_class_and_an_ordering_key_are_paused_on_their_own() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    let mut admin = s.admin(ADMIN).await;
    admin
        .pause(pause(Some(pb::Priority::Low), "", "backfill off"))
        .await
        .unwrap();
    admin
        .pause(pause(None, "acct", "customer asked"))
        .await
        .unwrap();
    let mut low = spec(b"low");
    low.priority = pb::Priority::Low as i32;
    let low = id_of(&mut c, low).await;
    let mut keyed = spec(b"keyed");
    keyed.ordering_key = "acct".into();
    let keyed = id_of(&mut c, keyed).await;
    let normal = id_of(&mut c, spec(b"normal")).await;

    let mut w = raw_worker(&s, WORKER, register("default", 4))
        .await
        .unwrap();
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, normal);
    complete(&w, lease.lease).await;
    no_lease(&mut w).await;

    let snap = s.snapshot().await;
    let pool = &snap["pools"][0];
    assert_eq!(pool["paused"], Value::Null);
    assert_eq!(
        pool["paused_classes"],
        json!([{"class": "low", "reason": "backfill off"}])
    );
    assert_eq!(
        (&pool["paused_tasks"], &pool["ordering"]["paused"]),
        (&json!(2), &json!(1))
    );
    let mut metrics = s.admin(METRICS).await;
    let key = metrics
        .get_ordering(pb::OrderingRequest {
            pool: String::new(),
            key: "acct".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(key.paused.as_deref(), Some("customer asked"));
    assert!(!key.halted);
    let listed = metrics
        .list_paused_orderings(pb::ListHaltedOrderingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.keys.len(), 1);
    assert_eq!(
        (listed.keys[0].key.as_str(), listed.keys[0].reason.as_str()),
        ("acct", "customer asked")
    );

    assert!(
        admin
            .resume(resume(None, "acct"))
            .await
            .unwrap()
            .into_inner()
            .resumed
    );
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, keyed);
    complete(&w, lease.lease).await;
    no_lease(&mut w).await;
    let r = admin
        .resume(resume(Some(pb::Priority::Low), ""))
        .await
        .unwrap();
    assert!(r.into_inner().resumed);
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, low);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn who_may_pause_and_what_is_refused() {
    let tokens = format!(
        "{DEFAULT_POOL}\n[[pools]]\nname = \"batch\"\nmax = 2\n\
         [[auth.tokens]]\ntoken = \"{ACME_ADMIN}\"\nrole = \"admin\"\ntenant = \"acme\"\n\
         [[auth.tokens]]\ntoken = \"{BATCH_ADMIN}\"\nrole = \"admin\"\npools = [\"batch\"]\n"
    );
    let s = TestServer::start(&tokens).await;
    // The admin role, for a pool it may use.
    for token in [CLIENT, METRICS, BATCH_ADMIN] {
        let mut a = s.admin(token).await;
        let e = a.pause(pause(None, "", "x")).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied, "{token}: {e}");
        let e = a.resume(resume(None, "")).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied, "{token}: {e}");
    }
    // Pinned to a tenant: its own ordering keys only, not the pool or a class.
    let mut acme = s.admin(ACME_ADMIN).await;
    for req in [
        pause(None, "", "x"),
        pause(Some(pb::Priority::Low), "", "x"),
    ] {
        let e = acme.pause(req).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e}");
    }
    assert!(
        acme.pause(pause(None, "k", "mine"))
            .await
            .unwrap()
            .into_inner()
            .paused
    );
    let mut admin = s.admin(ADMIN).await;
    let listed = admin
        .list_paused_orderings(pb::ListHaltedOrderingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.keys[0].key, "acme:k");
    let own = acme
        .list_paused_orderings(pb::ListHaltedOrderingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(own.keys[0].key, "k");
    assert!(
        acme.resume(resume(None, "k"))
            .await
            .unwrap()
            .into_inner()
            .resumed
    );

    // What does not make sense.
    for bad in [
        pause(Some(pb::Priority::Low), "k", "x"),
        pause(Some(pb::Priority::Unspecified), "", "x"),
        pb::PauseRequest {
            priority_class: Some(42),
            ..pause(None, "", "x")
        },
        pause(None, "", &"r".repeat(5000)),
        // A reason is required, on one line.
        pause(None, "", ""),
        pause(None, "", "  "),
        pause(None, "", "two\nlines"),
        // A key that is set but empty is not a way to name the pool.
        pb::PauseRequest {
            ordering_key: Some(String::new()),
            ..pause(None, "", "x")
        },
    ] {
        let e = admin.pause(bad).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    }
    let e = admin
        .resume(pb::ResumeRequest {
            ordering_key: Some(String::new()),
            ..resume(None, "")
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    let e = admin
        .pause(pb::PauseRequest {
            pool: "nope".into(),
            ..pause(None, "", "x")
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e}");
    // The pool-limited admin pauses its own pool.
    let mut batch = s.admin(BATCH_ADMIN).await;
    let r = batch
        .pause(pb::PauseRequest {
            pool: "batch".into(),
            ..pause(None, "", "x")
        })
        .await
        .unwrap();
    assert!(r.into_inner().paused);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_pauses_and_resumes() {
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
    let reason = |r: &str| Some(json!({ "reason": r }));
    let (code, _) = call(M::POST, METRICS, "/v1/pools/default/pause", reason("x")).await;
    assert_eq!(code, 403);
    let (code, r) = call(M::POST, ADMIN, "/v1/pools/default/pause", reason("deploy")).await;
    assert_eq!((code, &r["paused"]), (200, &json!(true)));
    let (_, r) = call(M::POST, ADMIN, "/v1/pools/default/pause", reason("deploy")).await;
    assert_eq!(r["paused"], false);
    let (code, r) = call(
        M::POST,
        ADMIN,
        "/v1/pools/default/pause",
        Some(json!({"reason": "backfill off", "class": "low"})),
    )
    .await;
    assert_eq!((code, &r["paused"]), (200, &json!(true)));
    let key = "/v1/pools/default/ordering/acct";
    let (code, r) = call(
        M::POST,
        ADMIN,
        &format!("{key}/pause"),
        reason("customer asked"),
    )
    .await;
    assert_eq!((code, &r["paused"]), (200, &json!(true)));
    for (path, body) in [
        ("/v1/pools/default/pause", json!({})),
        (
            "/v1/pools/default/pause",
            json!({"reason": "x", "class": "urgent"}),
        ),
        (
            "/v1/pools/default/ordering/acct/pause",
            json!({"reason": "x", "class": "low"}),
        ),
    ] {
        let (code, e) = call(M::POST, ADMIN, path, Some(body)).await;
        assert_eq!(code, 400, "{e}");
    }
    let (code, e) = call(M::POST, ADMIN, "/v1/pools/nope/pause", reason("x")).await;
    assert_eq!(code, 404, "{e}");
    // An empty pool segment is not the default pool, and an empty reason is not one.
    for path in ["/v1/pools//pause", "/v1/pools//resume"] {
        let (code, e) = call(M::POST, ADMIN, path, reason("x")).await;
        assert!(code == 400 || code == 404, "{path}: {code} {e}");
    }
    let (code, e) = call(M::POST, ADMIN, "/v1/pools/default/pause", reason(" ")).await;
    assert_eq!(code, 400, "{e}");

    let snap = s.snapshot().await;
    let pool = &snap["pools"][0];
    assert_eq!(pool["paused"], "deploy");
    assert_eq!(pool["paused_classes"][0]["class"], "low");
    let (_, info) = call(M::GET, METRICS, key, None).await;
    assert_eq!(info["paused"], "customer asked");
    let (code, list) = call(
        M::GET,
        METRICS,
        "/v1/pools/default/ordering?paused=true",
        None,
    )
    .await;
    assert_eq!(code, 200, "{list}");
    assert_eq!(
        list["paused"],
        json!([{"key": "acct", "reason": "customer asked"}])
    );

    let (code, _) = call(M::POST, CLIENT, "/v1/pools/default/resume", None).await;
    assert_eq!(code, 403);
    let (code, r) = call(M::POST, ADMIN, "/v1/pools/default/resume", None).await;
    assert_eq!((code, &r["resumed"]), (200, &json!(true)));
    // The class may be named in the body, as for a pause; it is not ignored.
    let low = Some(json!({"class": "low"}));
    let (code, r) = call(M::POST, ADMIN, "/v1/pools/default/resume", low.clone()).await;
    assert_eq!((code, &r["resumed"]), (200, &json!(true)));
    let (code, e) = call(M::POST, ADMIN, "/v1/pools/default/resume?class=high", low).await;
    assert_eq!(code, 400, "{e}");
    let (_, r) = call(M::POST, ADMIN, "/v1/pools/default/resume?class=low", None).await;
    assert_eq!(r["resumed"], false);
    call(
        M::POST,
        ADMIN,
        "/v1/pools/default/pause",
        Some(json!({"reason": "again", "class": "low"})),
    )
    .await;
    let (_, r) = call(M::POST, ADMIN, "/v1/pools/default/resume?class=low", None).await;
    assert_eq!(r["resumed"], true);
    let (_, r) = call(M::POST, ADMIN, "/v1/pools/default/resume?class=low", None).await;
    assert_eq!(r["resumed"], false);
    let (code, e) = call(M::POST, ADMIN, "/v1/pools/default/resume?class=x", None).await;
    assert_eq!(code, 400, "{e}");
    let (_, r) = call(M::POST, ADMIN, &format!("{key}/resume"), None).await;
    assert_eq!(r["resumed"], true);
    let snap = s.snapshot().await;
    assert_eq!(snap["pools"][0]["paused"], Value::Null);
    assert_eq!(snap["pools"][0]["paused_classes"], json!([]));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pauses_survive_a_restart() {
    let dir = std::env::temp_dir().join(format!("cronoq-pause-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = format!(
        "[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );
    let s = TestServer::start(&store).await;
    let mut c = s.tasks(CLIENT).await;
    let mut admin = s.admin(ADMIN).await;
    admin.pause(pause(None, "", "maintenance")).await.unwrap();
    admin
        .pause(pause(None, "acct", "customer asked"))
        .await
        .unwrap();
    let a = id_of(&mut c, spec(b"a")).await;
    s.stop().await;

    let s = TestServer::start(&store).await;
    let mut admin = s.admin(ADMIN).await;
    let (mut w, reg) = registered(&s).await;
    assert_eq!(reg.pool_paused.as_deref(), Some("maintenance"));
    no_lease(&mut w).await;
    let listed = admin
        .list_paused_orderings(pb::ListHaltedOrderingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.keys[0].key, "acct");
    assert!(
        admin
            .resume(resume(None, ""))
            .await
            .unwrap()
            .into_inner()
            .resumed
    );
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, a);
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
