//! A worker releases a lease: the task is leased again after the delay, the attempt number
//! goes up, and the attempt does not use up the retry policy.

mod common;

use std::time::{Duration, Instant};

use common::*;
use cronoq_proto::v1 as pb;

const POOL: &str = r#"
[[pools]]
name = "default"
max = 8
lease_ttl_s = 5
max_releases = 2
retry = { max_attempts = 1, initial_backoff_s = 0.05, jitter = false }
"#;

async fn send(w: &RawWorker, msg: pb::worker_message::Msg) {
    w.tx.send(pb::WorkerMessage { msg: Some(msg) })
        .await
        .unwrap();
}

async fn release(w: &RawWorker, lease: u64, delay_s: f64) {
    send(
        w,
        pb::worker_message::Msg::Release(pb::Release { lease, delay_s }),
    )
    .await;
}

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
async fn a_released_task_is_leased_again_without_using_a_retry() {
    let s = TestServer::start(POOL).await;
    let mut c = s.tasks(CLIENT).await;
    let id = c
        .submit(submit(spec(b"work"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let (mut w, reg) = registered(&s).await;
    assert!(reg.release_supported);

    // Released with a delay: the same task comes back as the next attempt, not before.
    let first = next_lease(&mut w).await;
    assert_eq!((first.task_id, first.attempt), (id, 1));
    let at = Instant::now();
    release(&w, first.lease, 0.4).await;
    let second = next_lease(&mut w).await;
    assert_eq!((second.task_id, second.attempt), (id, 2));
    assert!(
        at.elapsed() >= Duration::from_millis(350),
        "{:?}",
        at.elapsed()
    );
    let info = c.get(pb::GetRequest { task: by_id(id) }).await.unwrap();
    assert_eq!(info.into_inner().status, pb::TaskStatus::Running as i32);

    // The pool allows two releases; the retry policy allows one attempt, and none is used.
    release(&w, second.lease, 0.0).await;
    let third = next_lease(&mut w).await;
    assert_eq!(third.attempt, 3);
    // A third release counts as a failure: with one attempt allowed, the task is done.
    release(&w, third.lease, 0.0).await;
    let r = c
        .wait(pb::WaitRequest {
            task: by_id(id),
            timeout_s: Some(10.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.status, pb::TaskStatus::DeadLettered as i32);
    assert!(
        r.message.contains("released more than 2 times"),
        "{}",
        r.message
    );

    // A task's own limit; a release of a lease that is over changes nothing.
    let mut own = spec(b"own");
    own.max_releases = Some(0);
    let own = c.submit(submit(own, false)).await.unwrap().into_inner().id;
    let lease = next_lease(&mut w).await;
    assert_eq!(lease.task_id, own);
    release(&w, lease.lease, 0.0).await;
    release(&w, third.lease, 0.0).await;
    let r = c
        .wait(pb::WaitRequest {
            task: by_id(own),
            timeout_s: Some(10.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.status, pb::TaskStatus::DeadLettered as i32);

    // A task may not ask for more releases than the server allows.
    let mut greedy = spec(b"greedy");
    greedy.max_releases = Some(1001);
    let e = c.submit(submit(greedy, false)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert!(e.message().contains("max_releases"), "{}", e.message());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_task_goes_to_any_worker_and_keeps_its_place() {
    let s = TestServer::start(POOL).await;
    let mut c = s.tasks(CLIENT).await;
    let mut ids = Vec::new();
    for payload in [&b"a"[..], b"b", b"c"] {
        ids.push(
            c.submit(submit(spec(payload), false))
                .await
                .unwrap()
                .into_inner()
                .id,
        );
    }
    // One slot: the first task is leased and released without a delay. It is ahead of the
    // tasks submitted after it, so it comes back first.
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let first = next_lease(&mut w).await;
    assert_eq!(first.task_id, ids[0]);
    release(&w, first.lease, 0.0).await;
    let again = next_lease(&mut w).await;
    assert_eq!((again.task_id, again.attempt), (ids[0], 2));
    // Released with a delay, it lets the next task run; a second worker may take it later.
    release(&w, again.lease, 0.3).await;
    let next = next_lease(&mut w).await;
    assert_eq!(next.task_id, ids[1]);
    // Once the delay is over it is ahead of the third task again, for whoever is free.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut other = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let back = next_lease(&mut other).await;
    assert_eq!((back.task_id, back.attempt), (ids[0], 3));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rust_worker_releases_and_the_count_survives_a_restart() {
    use bytes::Bytes;
    use cronoq_server::worker::{WorkerOptions, run_worker};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let dir = std::env::temp_dir().join(format!("cronoq-release-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = format!(
        "{POOL}\n[store]\nkind = \"sqlite\"\npath = \"{}\"\n",
        dir.join("state.db").display()
    );
    let s = TestServer::start(&cfg).await;
    let mut c = s.tasks(CLIENT).await;
    let id = c
        .submit(submit(spec(b"work"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    // The handler gives the task back twice, then stays busy until the server stops.
    let calls = Arc::new(AtomicU32::new(0));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let seen = calls.clone();
    let worker = tokio::spawn(run_worker(
        WorkerOptions::new(s.grpc_url(), "default").token(WORKER),
        move |_lease| {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    return Err(cronoq::TaskError::release(Duration::from_millis(50)));
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(Bytes::new())
            }
        },
        async {
            let _ = stop_rx.await;
        },
    ));
    for _ in 0..200 {
        if calls.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let _ = stop_tx.send(());
    s.stop().await;
    let _ = worker.await;

    // After the restart the third attempt, which was running, is lost and counts. The retry
    // policy allows one attempt, so the task is done, with all three attempts on record.
    // (That the two released attempts do not count after recovery is tested in the engine
    // and in the store conformance suite.)
    let s = TestServer::start(&cfg).await;
    let mut c = s.tasks(CLIENT).await;
    let r = c
        .wait(pb::WaitRequest {
            task: by_id(id),
            timeout_s: Some(10.0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.status, pb::TaskStatus::DeadLettered as i32);
    assert!(r.message.contains("lost in failover"), "{}", r.message);
    let info = c
        .get(pb::GetRequest { task: by_id(id) })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.attempt, 3);
    s.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
