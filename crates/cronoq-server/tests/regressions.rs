//! Regression tests for issues found in reviews (shutdown, workers, results, HTTP, limits,
//! authorization). Each test failed before its fix.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use cronoq::TaskError;
use cronoq_proto::v1 as pb;
use cronoq_server::Config;
use cronoq_server::worker::{WorkerOptions, run_worker};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SHORT_TTL: &str = r#"
[[pools]]
name = "default"
max = 8
lease_ttl_s = 0.5
retry = { max_attempts = 2, initial_backoff_s = 0.05, jitter = false }
"#;

async fn get(s: &TestServer, id: u64) -> pb::TaskInfo {
    s.tasks(CLIENT)
        .await
        .get(pb::GetRequest { task: by_id(id) })
        .await
        .unwrap()
        .into_inner()
}

async fn wait(s: &TestServer, id: u64) -> pb::TaskResult {
    s.tasks(CLIENT)
        .await
        .wait(pb::WaitRequest {
            task: by_id(id),
            timeout_s: Some(20.0),
        })
        .await
        .unwrap()
        .into_inner()
}

// ---------------------------------------------------------------------------------------------
// Shutdown

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_connected_worker_returns() {
    let s = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&s, 1, Arc::default());
    s.wait_capacity(1).await;
    s.stop_within(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_watch_streams_with_unavailable() {
    let s = TestServer::start("").await;
    let mut stream = s
        .tasks(CLIENT)
        .await
        .watch(pb::WatchRequest::default())
        .await
        .unwrap()
        .into_inner();
    s.stop_within(Duration::from_secs(5)).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap();
    assert_eq!(end.unwrap_err().code(), tonic::Code::Unavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn waits_across_shutdown_are_unavailable() {
    let s = TestServer::start("").await;
    let url = s.http("/v1/tasks");
    let http = tokio::spawn(async move {
        let r = reqwest::Client::new()
            .post(url)
            .bearer_auth(CLIENT)
            .json(&serde_json::json!({"payload": 1, "wait": true}))
            .send()
            .await
            .unwrap();
        r.status().as_u16()
    });
    let mut c = s.tasks(CLIENT).await;
    let grpc = tokio::spawn(async move {
        c.submit(submit(spec(b"x"), true))
            .await
            .map(|r| r.into_inner())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    s.stop_within(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), http)
            .await
            .unwrap()
            .unwrap(),
        503
    );
    let g = tokio::time::timeout(Duration::from_secs(2), grpc)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(g.unwrap_err().code(), tonic::Code::Unavailable);
}

// ---------------------------------------------------------------------------------------------
// Workers

#[tokio::test(flavor = "multi_thread")]
async fn handler_panic_fails_the_attempt_and_frees_the_slot() {
    let s = TestServer::start(SHORT_TTL).await;
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .slots(1)
        .token(WORKER);
    tokio::spawn(run_worker(
        opts,
        |l| async move {
            if &l.payload[..] == b"boom" {
                panic!("handler bug");
            }
            Ok(l.payload)
        },
        std::future::pending(),
    ));
    s.wait_capacity(1).await;
    let mut c = s.tasks(CLIENT).await;
    let id = c
        .submit(submit(spec(b"boom"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let r = wait(&s, id).await;
    assert_eq!(r.status, pb::TaskStatus::DeadLettered as i32);
    assert!(r.message.contains("handler panicked"), "{}", r.message);
    let r = c
        .submit(submit(spec(b"ok"), true))
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(r.status, pb::TaskStatus::Succeeded as i32);
    assert_eq!(s.capacity().await, 1, "the worker survived the panic");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_register_is_rejected() {
    let s = TestServer::start("").await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = w.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    w.tx.send(register("default", 5)).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), w.inbound.message())
        .await
        .unwrap();
    assert_eq!(end.unwrap_err().code(), tonic::Code::InvalidArgument);
    s.wait_capacity(0).await;
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_slots_are_capped() {
    let s = TestServer::start("[limits]\nmax_slots_per_worker = 4\n").await;
    let mut w = raw_worker(&s, WORKER, register("default", u32::MAX))
        .await
        .unwrap();
    let m = w.inbound.message().await.unwrap().unwrap();
    let Some(pb::server_message::Msg::Registered(r)) = m.msg else {
        panic!("{m:?}")
    };
    assert_eq!(r.slots, 4);
    assert_eq!(r.protocol_version, cronoq_proto::PROTOCOL_VERSION);
    assert!(r.max_message_bytes > 0);
    s.wait_capacity(4).await;
    w.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::SetSlots(pb::SetSlots {
            slots: 1 << 30,
        })),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(s.capacity().await, 4);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_outputs_within_the_limit_succeed() {
    let s = TestServer::start("").await;
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .slots(2)
        .token(WORKER);
    tokio::spawn(run_worker(
        opts,
        |_| async move { Ok(Bytes::from(vec![b'x'; 5 << 20])) },
        std::future::pending(),
    ));
    s.wait_capacity(2).await;
    // Before the fix, a 5 MiB output exceeded the server's 4 MiB decode limit and the worker's
    // connection was dropped, failing every lease it held.
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"big"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let r = reqwest::Client::new()
        .get(s.http(&format!("/v1/tasks/{id}/result?timeout_s=20")))
        .bearer_auth(CLIENT)
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["status"], "succeeded");
    assert_eq!(
        v["output_b64"].as_str().unwrap().len(),
        (5 << 20) / 3 * 4 + 4
    );
    assert_eq!(s.capacity().await, 2);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_outputs_fail_permanently_without_dropping_the_worker() {
    let s = TestServer::start(&format!(
        "[limits]\nmax_message_bytes = 1048576\n{DEFAULT_POOL}"
    ))
    .await;
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .slots(2)
        .token(WORKER);
    tokio::spawn(run_worker(
        opts,
        |l| async move {
            if &l.payload[..] == b"big" {
                return Ok(Bytes::from(vec![b'x'; 2 << 20]));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(l.payload)
        },
        std::future::pending(),
    ));
    s.wait_capacity(2).await;
    let mut c = s.tasks(CLIENT).await;
    let other = c
        .submit(submit(spec(b"innocent"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let big = c
        .submit(submit(spec(b"big"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let r = wait(&s, big).await;
    assert_eq!(r.status, pb::TaskStatus::Failed as i32, "{r:?}");
    assert!(r.message.contains("exceeds"), "{}", r.message);
    assert_eq!(
        wait(&s, other).await.status,
        pb::TaskStatus::Succeeded as i32
    );
    assert_eq!(s.capacity().await, 2);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unread_worker_is_evicted_and_work_completes_elsewhere() {
    let s = TestServer::start(
        "[[pools]]\nname = \"default\"\nmax = 64\nlease_ttl_s = 0.3\nmax_slots_per_worker = 4\n\
         retry = { max_attempts = 20, initial_backoff_s = 0.01, max_backoff_s = 0.05, jitter = false }\n",
    )
    .await;
    // A worker that registers a huge slot count and never reads its stream.
    let mut evil = raw_worker(&s, WORKER, register("default", u32::MAX))
        .await
        .unwrap();
    let _ = evil.inbound.message().await.unwrap();
    s.wait_capacity(4).await;
    let mut c = s.tasks(CLIENT).await;
    let mut ids = Vec::new();
    for _ in 0..4 {
        let specs: Vec<_> = (0..25).map(|_| spec(&[b'p'; 256 << 10])).collect();
        let r = c
            .submit_batch(pb::SubmitBatchRequest { tasks: specs })
            .await
            .unwrap()
            .into_inner();
        ids.extend(r.tasks.iter().map(|t| t.id));
    }
    // Its leases expire and are re-offered until its stream backs up; then it is disconnected
    // instead of swallowing leases forever.
    s.wait_capacity(0).await;
    let (_w, _j) = spawn_worker(&s, 4, Arc::default());
    for id in ids {
        assert_eq!(wait(&s, id).await.status, pb::TaskStatus::Succeeded as i32);
    }
    drop(evil);
    s.stop().await;
}

/// TCP proxy that forwards until `cut` is set, then silently drops bytes (a half-open peer).
async fn proxy(target: std::net::SocketAddr, cut: Arc<AtomicBool>) -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (a, _) = l.accept().await.unwrap();
            let b = TcpStream::connect(target).await.unwrap();
            let (mut ar, mut aw) = a.into_split();
            let (mut br, mut bw) = b.into_split();
            let (c1, c2) = (cut.clone(), cut.clone());
            tokio::spawn(async move {
                let mut buf = [0u8; 65536];
                while let Ok(n @ 1..) = ar.read(&mut buf).await {
                    if !c1.load(Ordering::SeqCst) {
                        let _ = bw.write_all(&buf[..n]).await;
                    }
                }
                std::future::pending::<()>().await
            });
            tokio::spawn(async move {
                let mut buf = [0u8; 65536];
                while let Ok(n @ 1..) = br.read(&mut buf).await {
                    if !c2.load(Ordering::SeqCst) {
                        let _ = aw.write_all(&buf[..n]).await;
                    }
                }
                std::future::pending::<()>().await
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn half_open_worker_is_detected_by_keepalive() {
    let s = TestServer::start(&format!(
        "[limits]\nkeepalive_interval_s = 0.5\nkeepalive_timeout_s = 0.5\n{SHORT_TTL}"
    ))
    .await;
    let cut = Arc::new(AtomicBool::new(false));
    let p = proxy(s.bound.grpc, cut.clone()).await;
    let opts = WorkerOptions::new(format!("http://{p}"), "default")
        .slots(1)
        .token(WORKER)
        .reconnect(false);
    tokio::spawn(run_worker(
        opts,
        |l| async move { Ok(l.payload) },
        std::future::pending(),
    ));
    s.wait_capacity(1).await;
    cut.store(true, Ordering::SeqCst);
    for _ in 0..200 {
        if s.capacity().await == 0 {
            s.stop().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("half-open worker still counted after 10s");
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_offers_the_lease_to_another_worker() {
    let s = TestServer::start("").await;
    let mut a = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = a.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let l = next_lease(&mut a).await;
    assert_eq!((l.pool.as_str(), l.lease_ttl_s), ("default", Some(2.0)));
    let mut b = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = b.inbound.message().await.unwrap();
    s.wait_capacity(2).await;
    a.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Nack(pb::Nack {
            lease: l.lease,
            reason: "draining".into(),
        })),
    })
    .await
    .unwrap();
    let l2 = next_lease(&mut b).await;
    assert_eq!((l2.task_id, l2.lease, l2.attempt), (id, l.lease, l.attempt));
    b.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease: l2.lease,
            output: b"done".to_vec(),
            content_type: String::new(),
        })),
    })
    .await
    .unwrap();
    let r = wait(&s, id).await;
    assert_eq!(r.output.as_deref(), Some(&b"done"[..]));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_lease_owner_can_report() {
    let s = TestServer::start("").await;
    let mut a = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = a.inbound.message().await.unwrap();
    let mut b = raw_worker(&s, WORKER, register("default", 0))
        .await
        .unwrap();
    let _ = b.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let l = next_lease(&mut a).await;
    let complete = |out: &[u8]| pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
            lease: l.lease,
            output: out.to_vec(),
            content_type: String::new(),
        })),
    };
    b.tx.send(complete(b"forged")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.tx.send(complete(b"real")).await.unwrap();
    assert_eq!(wait(&s, id).await.output.as_deref(), Some(&b"real"[..]));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_errors_are_fatal_or_retried() {
    let s = TestServer::start("").await;
    // Unknown pool: fatal, returns promptly.
    let opts = WorkerOptions::new(s.grpc_url(), "nope").token(WORKER);
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        run_worker(
            opts,
            |l| async move { Ok(l.payload) },
            std::future::pending(),
        ),
    )
    .await;
    assert!(r.unwrap().is_err());
    // Bad token: fatal.
    let opts = WorkerOptions::new(s.grpc_url(), "default").token("wrong-wrong-wrong-wrong");
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        run_worker(
            opts,
            |l| async move { Ok(l.payload) },
            std::future::pending(),
        ),
    )
    .await;
    assert!(r.unwrap().is_err());
    // Server goes away with reconnect disabled: an error, not Ok.
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .token(WORKER)
        .reconnect(false);
    let w = tokio::spawn(run_worker(
        opts,
        |l| async move { Ok(l.payload) },
        std::future::pending(),
    ));
    s.wait_capacity(1).await;
    s.stop().await;
    let r = tokio::time::timeout(Duration::from_secs(5), w)
        .await
        .unwrap()
        .unwrap();
    assert!(r.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_drains_running_leases_on_shutdown() {
    let s = TestServer::start("").await;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let started = Arc::new(AtomicU32::new(0));
    let st = started.clone();
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .token(WORKER)
        .slots(1);
    let w = tokio::spawn(run_worker(
        opts,
        move |l| {
            let st = st.clone();
            async move {
                st.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(l.payload)
            }
        },
        async {
            let _ = stop_rx.await;
        },
    ));
    s.wait_capacity(1).await;
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    while started.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), w)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(wait(&s, id).await.status, pb::TaskStatus::Succeeded as i32);
    s.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Results

#[tokio::test(flavor = "multi_thread")]
async fn results_keep_messages_and_report_evicted_outputs() {
    let s = TestServer::start(
        "[outputs]\ncache_entries = 1\n[[pools]]\nname = \"default\"\nretry = { max_attempts = 1 }\n",
    )
    .await;
    let opts = WorkerOptions::new(s.grpc_url(), "default")
        .slots(1)
        .token(WORKER);
    tokio::spawn(run_worker(
        opts,
        |l| async move {
            if &l.payload[..] == b"fail" {
                return Err(TaskError::permanent("the real reason"));
            }
            Ok(Bytes::from(l.payload.to_ascii_uppercase()))
        },
        std::future::pending(),
    ));
    s.wait_capacity(1).await;
    let mut c = s.tasks(CLIENT).await;
    let a = c
        .submit(submit(spec(b"a"), true))
        .await
        .unwrap()
        .into_inner()
        .id;
    let f = c
        .submit(submit(spec(b"fail"), true))
        .await
        .unwrap()
        .into_inner()
        .id;
    c.submit(submit(spec(b"b"), true)).await.unwrap();
    let ra = wait(&s, a).await;
    assert_eq!(ra.status, pb::TaskStatus::Succeeded as i32);
    assert!(
        !ra.output_available && ra.output.is_none(),
        "evicted output is reported as unavailable"
    );
    let rf = wait(&s, f).await;
    assert_eq!(rf.status, pb::TaskStatus::Failed as i32);
    assert!(rf.message.contains("the real reason"), "{rf:?}");
    let info = get(&s, f).await;
    assert!(info.message.contains("the real reason"), "{info:?}");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_get_of_unknown_task_is_not_found() {
    let s = TestServer::start("").await;
    let mut c = s.tasks(CLIENT).await;
    assert_eq!(
        c.get(pb::GetRequest {
            task: by_key("nope")
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        c.get(pb::GetRequest { task: by_id(12345) })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let w = c
        .wait(pb::WaitRequest {
            task: by_key("nope"),
            timeout_s: Some(0.1),
        })
        .await;
    assert_eq!(w.unwrap_err().code(), tonic::Code::NotFound);
    let w = c
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let r = c
        .wait(pb::WaitRequest {
            task: by_id(w),
            timeout_s: Some(0.1),
        })
        .await;
    assert_eq!(r.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    s.stop().await;
}

// ---------------------------------------------------------------------------------------------
// HTTP

#[tokio::test(flavor = "multi_thread")]
async fn numeric_keys_are_addressable_by_key() {
    let s = TestServer::start("").await;
    let http = reqwest::Client::new();
    let post = |body: serde_json::Value| {
        http.post(s.http("/v1/tasks"))
            .bearer_auth(CLIENT)
            .json(&body)
            .send()
    };
    let a: serde_json::Value = post(serde_json::json!({"payload": "a"}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key = a["id"].as_str().unwrap().to_string();
    let b: serde_json::Value = post(serde_json::json!({"payload": "b", "key": key}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let v: serde_json::Value = http
        .delete(s.http(&format!("/v1/tasks/by-key/{key}")))
        .bearer_auth(CLIENT)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["cancelled"], true);
    let ia = get(&s, key.parse().unwrap()).await;
    let ib = get(&s, b["id"].as_str().unwrap().parse().unwrap()).await;
    assert_eq!(
        ib.status,
        pb::TaskStatus::Cancelled as i32,
        "DELETE by key cancels the keyed task"
    );
    assert_ne!(
        ia.status,
        pb::TaskStatus::Cancelled as i32,
        "and not the task whose id equals the key"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_errors_are_json_with_proper_status_codes() {
    let s = TestServer::start("").await;
    let http = reqwest::Client::new();
    let pending: serde_json::Value = http
        .post(s.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "x"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pid = pending["id"].as_str().unwrap().to_string();
    let json = |v: serde_json::Value| http.post(s.http("/v1/tasks")).bearer_auth(CLIENT).json(&v);
    let cases: Vec<(&str, reqwest::RequestBuilder, u16)> = vec![
        (
            "pending result",
            http.get(s.http(&format!("/v1/tasks/{pid}/result?timeout_s=0.1")))
                .bearer_auth(CLIENT),
            504,
        ),
        (
            "unknown field",
            json(serde_json::json!({"payload": "x", "bogus": 1})),
            400,
        ),
        (
            "malformed",
            http.post(s.http("/v1/tasks"))
                .bearer_auth(CLIENT)
                .header("content-type", "application/json")
                .body("{"),
            400,
        ),
        (
            "missing content type",
            http.post(s.http("/v1/tasks"))
                .bearer_auth(CLIENT)
                .body("{}"),
            415,
        ),
        (
            "unknown pool",
            json(serde_json::json!({"payload": "x", "pool": "nope"})),
            404,
        ),
        (
            "unknown resource",
            json(serde_json::json!({"payload": "x", "resources": {"gpu": 1}})),
            404,
        ),
        (
            "negative deadline",
            json(serde_json::json!({"payload": "x", "deadline_unix_ns": -5})),
            400,
        ),
        (
            "NUL in key",
            json(serde_json::json!({"payload": "x", "key": "a\u{0}b"})),
            400,
        ),
        (
            "unknown key",
            http.get(s.http("/v1/tasks/by-key/does-not-exist/result?timeout_s=0.1"))
                .bearer_auth(CLIENT),
            404,
        ),
        (
            "non-numeric id",
            http.get(s.http("/v1/tasks/abc")).bearer_auth(CLIENT),
            400,
        ),
        (
            "bad query",
            http.get(s.http(&format!("/v1/tasks/{pid}/result?timeout_s=soon")))
                .bearer_auth(CLIENT),
            400,
        ),
        (
            "batch with wait",
            http.post(s.http("/v1/tasks:batch"))
                .bearer_auth(CLIENT)
                .json(&serde_json::json!({"tasks": [{"payload": "x", "wait": true}]})),
            400,
        ),
        (
            "unknown route",
            http.get(s.http("/v2/nothing")).bearer_auth(CLIENT),
            404,
        ),
        (
            "wrong method",
            http.put(s.http("/v1/tasks")).bearer_auth(CLIENT),
            405,
        ),
    ];
    for (name, req, status) in cases {
        let r = req.send().await.unwrap();
        assert_eq!(r.status().as_u16(), status, "{name}");
        assert_eq!(r.headers()["content-type"], "application/json", "{name}");
        let v: serde_json::Value = r.json().await.unwrap();
        assert!(v["error"].is_string(), "{name}: {v}");
    }
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_authenticates_before_reading_the_body() {
    let s = TestServer::start("[limits]\nmax_http_body_bytes = 65536\n").await;
    let http = reqwest::Client::new();
    let r = http
        .post(s.http("/v1/tasks"))
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // The server answers before reading the body, so the client may see the connection close
    // while still sending; any response it does get must be the 401.
    let r = http
        .post(s.http("/v1/tasks"))
        .header("content-type", "application/json")
        .body(vec![b' '; 1 << 20])
        .send()
        .await;
    if let Ok(r) = r {
        assert_eq!(r.status(), 401);
    }
    // Authenticated but too large. The body is only slightly over the limit, so the client has
    // sent all of it before the server answers and always reads the 413.
    let r = http
        .post(s.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .header("content-type", "application/json")
        .body(vec![b' '; 65536 + 4096])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_dependencies_accept_string_ids() {
    let s = TestServer::start("").await;
    let (_w, _j) = spawn_worker(&s, 1, Arc::default());
    s.wait_capacity(1).await;
    let http = reqwest::Client::new();
    let a: serde_json::Value = http
        .post(s.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "a"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b: serde_json::Value = http
        .post(s.http("/v1/tasks"))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "b", "after": [{"id": a["id"]}], "deps": [{"id": a["id"], "cond": "on_complete"}], "wait": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(b["result"]["status"], "succeeded", "{b}");
    s.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Limits and hostile input

#[tokio::test(flavor = "multi_thread")]
async fn hostile_input_is_rejected() {
    let s = TestServer::start("[[pools]]\nname = \"default\"\nmax_pending = 5\n").await;
    let mut c = s.tasks(CLIENT).await;
    let mut nul = spec(b"x");
    nul.key = "evil\0".into();
    assert_eq!(
        c.submit(submit(nul, false)).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let batch = pb::SubmitBatchRequest {
        tasks: (0..1001).map(|_| spec(b"x")).collect(),
    };
    assert_eq!(
        c.submit_batch(batch).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    for _ in 0..5 {
        c.submit(submit(spec(b"x"), false)).await.unwrap();
    }
    assert_eq!(
        c.submit(submit(spec(b"x"), false))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let w = c
        .wait(pb::WaitRequest {
            task: by_id(1),
            timeout_s: Some(f64::NAN),
        })
        .await;
    assert_eq!(w.unwrap_err().code(), tonic::Code::InvalidArgument);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn waits_are_capped_by_max_wait() {
    let s = TestServer::start("[limits]\nmax_wait_s = 0.2\n").await;
    let mut c = s.tasks(CLIENT).await;
    let started = std::time::Instant::now();
    let r = c
        .submit(pb::SubmitRequest {
            task: Some(spec(b"x")),
            wait: true,
            wait_timeout_s: Some(3600.0),
        })
        .await;
    assert_eq!(r.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert!(started.elapsed() < Duration::from_secs(5));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_failure_messages_are_sanitized() {
    let s =
        TestServer::start("[[pools]]\nname = \"default\"\nretry = { max_attempts = 1 }\n").await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = w.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"x"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let l = next_lease(&mut w).await;
    let msg = format!("bad byte \0 in input {}", "x".repeat(20_000));
    w.tx.send(pb::WorkerMessage {
        msg: Some(pb::worker_message::Msg::Fail(pb::Fail {
            lease: l.lease,
            message: msg,
            permanent: true,
            retry_after_s: None,
        })),
    })
    .await
    .unwrap();
    let r = wait(&s, id).await;
    assert_eq!(r.status, pb::TaskStatus::Failed as i32);
    assert!(
        r.message.contains("bad byte \u{FFFD} in input"),
        "{}",
        &r.message[..40]
    );
    assert!(r.message.len() <= 8192);
    s.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Authorization

const SCOPED: &str = r#"
[[auth.tokens]]
token = "client-a-token-0123456789abcdef0123"
role = "client"
pools = ["a"]
[[auth.tokens]]
token = "worker-a-token-0123456789abcdef0123"
role = "worker"
pools = ["a"]
[[pools]]
name = "a"
[[pools]]
name = "b"
"#;
const CLIENT_A: &str = "client-a-token-0123456789abcdef0123";
const WORKER_A: &str = "worker-a-token-0123456789abcdef0123";

#[tokio::test(flavor = "multi_thread")]
async fn tokens_are_scoped_to_pools() {
    let s = TestServer::start(SCOPED).await;
    let mut admin = s.tasks(ADMIN).await;
    let mut in_b = spec(b"x");
    in_b.pool = "b".into();
    in_b.key = "kb".into();
    let b_id = admin
        .submit(submit(in_b.clone(), false))
        .await
        .unwrap()
        .into_inner()
        .id;

    let mut a = s.tasks(CLIENT_A).await;
    assert_eq!(
        a.submit(submit(in_b, false)).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    for target in [by_id(b_id), by_key("kb")] {
        assert_eq!(
            a.get(pb::GetRequest {
                task: target.clone()
            })
            .await
            .unwrap_err()
            .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            a.cancel(pb::CancelRequest {
                task: target.clone()
            })
            .await
            .unwrap_err()
            .code(),
            tonic::Code::NotFound
        );
        let w = a
            .wait(pb::WaitRequest {
                task: target,
                timeout_s: Some(0.1),
            })
            .await;
        assert_eq!(w.unwrap_err().code(), tonic::Code::NotFound);
    }
    let w = a
        .watch(pb::WatchRequest {
            pools: vec!["b".into()],
            ..Default::default()
        })
        .await;
    assert_eq!(w.unwrap_err().code(), tonic::Code::PermissionDenied);
    let r = reqwest::Client::new()
        .get(s.http(&format!("/v1/tasks/{b_id}")))
        .bearer_auth(CLIENT_A)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // Watch without a pool filter only shows allowed pools.
    let mut events = a
        .watch(pb::WatchRequest {
            all_transitions: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut other = spec(b"y");
    other.pool = "b".into();
    admin.submit(submit(other, false)).await.unwrap();
    let mut mine = spec(b"z");
    mine.pool = "a".into();
    let mine_id = a.submit(submit(mine, false)).await.unwrap().into_inner().id;
    let ev = tokio::time::timeout(Duration::from_secs(5), events.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((ev.id, ev.pool.as_str()), (mine_id, "a"));

    // Worker tokens are scoped too.
    let e = raw_worker(&s, WORKER_A, register("b", 1)).await;
    let code = match e {
        Err(e) => e.code(),
        Ok(mut w) => w.inbound.message().await.unwrap_err().code(),
    };
    assert_eq!(code, tonic::Code::PermissionDenied);
    let mut ok = raw_worker(&s, WORKER_A, register("a", 1)).await.unwrap();
    assert!(ok.inbound.message().await.unwrap().is_some());
    s.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Configuration

#[test]
fn invalid_configurations_are_rejected() {
    let cases = [
        (
            "negative backoff",
            "[[pools]]\nname='a'\nretry = { max_attempts = 3, initial_backoff_s = -1 }",
        ),
        (
            "negative default_duration",
            "[[pools]]\nname='a'\ndefault_duration_s = -1",
        ),
        ("nan drain", "[[pools]]\nname='a'\ndrain_time_s = nan"),
        (
            "negative await deps",
            "[[pools]]\nname='a'\nawait_unknown_dependencies_s = -5",
        ),
        ("inf lease ttl", "[[pools]]\nname='a'\nlease_ttl_s = inf"),
        ("dup pools", "[[pools]]\nname='a'\n[[pools]]\nname='a'"),
        ("empty pool name", "[[pools]]\nname=''"),
        ("odd pool name", "[[pools]]\nname='we\"ird'"),
        ("max 0", "[[pools]]\nname='a'\nmax = 0"),
        (
            "reservable 7",
            "[[pools]]\nname='a'\nreservable_fraction = 7.0",
        ),
        (
            "dup resource",
            "[[resources]]\nkind='semaphore'\nname='r'\npermits=1\n[[resources]]\nkind='semaphore'\nname='r'\npermits=2",
        ),
        (
            "zero-rate bucket",
            "[[resources]]\nkind='token_bucket'\nname='r'\nrate=0\nburst=0",
        ),
        (
            "dup token",
            "[[auth.tokens]]\ntoken='tttttttttttttttttttttttttttttttt'\nrole='admin'\n[[auth.tokens]]\ntoken='tttttttttttttttttttttttttttttttt'\nrole='client'",
        ),
        (
            "token pool unknown",
            "[[auth.tokens]]\ntoken='tttttttttttttttttttttttttttttttt'\nrole='client'\npools=['zzz']",
        ),
        ("grace 1e30", "[server]\nshutdown_grace_s = 1e30"),
        ("cache 0 bytes", "[outputs]\ncache_bytes = 0"),
        ("zero batch", "[limits]\nmax_tasks_per_batch = 0"),
        (
            "certs without CA",
            "[[auth.certs]]\ndns_name='w'\nrole='worker'",
        ),
    ];
    for (name, toml) in cases {
        assert!(Config::parse(toml).is_err(), "{name} should be rejected");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn run_returns_errors_instead_of_panicking() {
    let mut cfg = Config::parse(&format!("{SERVER}\n[[pools]]\nname='default'")).unwrap();
    cfg.pools[0].retry = Some(cronoq_server::config::RetrySection {
        max_attempts: 3,
        initial_backoff_s: -1.0,
        multiplier: 2.0,
        max_backoff_s: 60.0,
        jitter: true,
    });
    let r = tokio::spawn(cronoq_server::run(cfg, std::future::pending(), |_| {})).await;
    assert!(
        matches!(r, Ok(Err(_))),
        "expected a config error, got {r:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_public_listeners_without_auth() {
    let cfg = Config::parse("[server]\ngrpc_addr='0.0.0.0:0'\nhttp_addr='127.0.0.1:0'").unwrap();
    let r = cronoq_server::run(cfg, std::future::pending(), |_| {}).await;
    let e = r.unwrap_err().to_string();
    assert!(e.contains("--insecure-no-auth"), "{e}");
    let mut cfg =
        Config::parse("[server]\ngrpc_addr='0.0.0.0:0'\nhttp_addr='127.0.0.1:0'").unwrap();
    cfg.server.insecure_no_auth = true;
    TestServer::start_cfg(cfg).await.stop().await;
    // Short tokens are refused at startup.
    let cfg = Config::parse(&format!(
        "{SERVER}\n[[auth.tokens]]\ntoken='short'\nrole='admin'"
    ))
    .unwrap();
    assert!(
        cronoq_server::run(cfg, std::future::pending(), |_| {})
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_listener_is_separate_and_unauthenticated() {
    let mut cfg = Config::parse(&format!("{SERVER}\n{TOKENS}\n{DEFAULT_POOL}")).unwrap();
    cfg.server.metrics_addr = Some("127.0.0.1:0".parse().unwrap());
    let s = TestServer::start_cfg(cfg).await;
    let addr = s.bound.metrics.expect("metrics listener");
    let r = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("cronoq_workers_connected 0")
    );
    let r = reqwest::get(format!("http://{addr}/v1/snapshot"))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        404,
        "only metrics and health on the metrics listener"
    );
    s.stop().await;
}

#[test]
fn example_config_is_valid() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/cronoq.example.toml"
    );
    let cfg = Config::parse(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(cfg.pools.len(), 2);
}

#[tokio::test]
async fn pool_scoped_tokens_cannot_reach_other_pools_through_keys_or_deps() {
    let s = TestServer::start(SCOPED).await;
    let mut admin = s.tasks(ADMIN).await;
    let mut in_b = spec(b"secret");
    in_b.pool = "b".into();
    in_b.key = "kb".into();
    let b_id = admin
        .submit(submit(in_b, false))
        .await
        .unwrap()
        .into_inner()
        .id;

    let mut a = s.tasks(CLIENT_A).await;
    // Reusing another pool's key must not return its id or result.
    for wait in [false, true] {
        let mut reuse = spec(b"mine");
        reuse.pool = "a".into();
        reuse.key = "kb".into();
        let e = a.submit(submit(reuse, wait)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::AlreadyExists, "wait={wait}");
        assert!(!e.message().contains(&b_id.to_string()));
    }
    let mut reuse = spec(b"mine");
    reuse.pool = "a".into();
    reuse.key = "kb".into();
    let e = a
        .submit_batch(pb::SubmitBatchRequest { tasks: vec![reuse] })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists);

    // Depending on another pool's task (by key or id) would expose its outcome.
    for target in [by_key("kb"), by_id(b_id)] {
        let mut child = spec(b"child");
        child.pool = "a".into();
        child.deps.push(pb::Dep {
            target,
            cond: pb::EdgeCond::OnComplete as i32,
        });
        let e = a.submit(submit(child, false)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::NotFound);
    }

    // Dependencies inside the caller's own batch and pool still work.
    let mut first = spec(b"1");
    first.pool = "a".into();
    first.key = "a-first".into();
    let mut second = spec(b"2");
    second.pool = "a".into();
    second.deps.push(pb::Dep {
        target: by_key("a-first"),
        cond: pb::EdgeCond::OnSuccess as i32,
    });
    let r = a
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![first, second],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.tasks.len(), 2);
    s.stop().await;
}
