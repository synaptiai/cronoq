mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{Pool, Scheduler, Store, StoreError, Task, TaskError, TimeBase};
use cronoq_core::*;
use cronoq_store_postgres::{PgStore, PgStoreConfig};

const S: u64 = 1_000_000_000;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn base_at(unix: i64) -> TimeBase {
    TimeBase {
        unix_nanos_at_zero: unix,
    }
}

fn fast_cfg() -> PgStoreConfig {
    PgStoreConfig {
        lock_poll: Duration::from_millis(50),
        keepalive: Duration::from_millis(100),
        ..Default::default()
    }
}

fn full_spec() -> TaskSpec<Bytes> {
    TaskSpec::new(Bytes::from_static(b"\x00payload\xff"))
        .kind("llm")
        .pool("gpu")
        .priority(Priority::High)
        .key("job-1")
        .deadline(Time(50 * S))
        .fixed_start(FixedStart {
            at: Time(30 * S),
            tolerance: Duration::from_millis(1500),
            on_miss: MissPolicy::Skip,
        })
        .duration_hint(Duration::from_millis(2500))
        .timeout(Duration::from_secs(9))
        .retry(RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(200),
            multiplier: 1.5,
            max_backoff: Duration::from_secs(7),
            jitter: false,
        })
        .dep(Dep::on_success(TaskId(7)))
        .dep(Dep::on_failure("parent"))
        .dep(Dep::on_complete("other"))
        .resource("api", 2)
        .resource("gpu_mem", 8)
        .tag("tenant", "acme")
        .tag("traceparent", "00-abc-def-01")
}

fn submitted(id: u64, seq: u64, spec: TaskSpec<Bytes>) -> Mutation<Bytes> {
    Mutation::Submitted {
        id: TaskId(id),
        seq,
        arrival: Time(S),
        spec,
    }
}

fn status(id: u64, st: TaskStatus, attempt: u32, at: u64) -> Mutation<Bytes> {
    Mutation::Status {
        id: TaskId(id),
        status: st,
        attempt,
        at: Time(at),
        message: Some(format!("{st}").into()),
    }
}

#[tokio::test]
async fn round_trips_every_mutation_and_field() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let epoch = store.acquire_leadership().await.unwrap();
    assert_eq!(epoch, 1);
    let base = base_at(now_unix());

    let recurring = RecurringSpec {
        name: "nightly".into(),
        cron: "0 3 * * *".into(),
        tz: Some("Europe/Lisbon".into()),
        template: TaskSpec::new(Bytes::from_static(b"tpl"))
            .kind("report")
            .pool("gpu")
            .retry(RetryPolicy::none())
            .tag("a", "b"),
        overlap: OverlapPolicy::Allow,
        tolerance: Duration::from_secs(5),
        on_miss: MissPolicy::Fail,
    };
    let gone = RecurringSpec {
        name: "gone".into(),
        ..recurring.clone()
    };
    let batch = vec![
        submitted(10, 0, full_spec()),
        submitted(11, 1, TaskSpec::new(Bytes::from_static(b"b"))),
        submitted(12, 2, TaskSpec::new(Bytes::from_static(b"c")).key("done")),
        status(11, TaskStatus::Running, 1, 2 * S),
        status(11, TaskStatus::Retrying, 1, 3 * S),
        status(12, TaskStatus::Running, 1, 2 * S),
        status(12, TaskStatus::Succeeded, 1, 4 * S),
        Mutation::RecurringUpsert {
            spec: recurring.clone(),
        },
        Mutation::RecurringUpsert { spec: gone },
        Mutation::RecurringNext {
            name: "nightly".into(),
            next: Some(Time(3600 * S)),
        },
        Mutation::RecurringRemove {
            name: "gone".into(),
        },
    ];
    let out = Bytes::from_static(b"result");
    store
        .commit(epoch, base, &batch, &[(TaskId(12), &out)])
        .await
        .unwrap();

    let rec = store.recover(base).await.unwrap();
    assert_eq!(rec.next_id, 13);
    assert_eq!(rec.tasks.len(), 2);
    let t = &rec.tasks[0];
    assert_eq!(
        (t.id, t.seq, t.arrival, t.attempt),
        (TaskId(10), 0, Time(S), 0)
    );
    let (a, b) = (&t.spec, full_spec());
    assert_eq!(*a.payload, *b.payload);
    assert_eq!(
        (&*a.kind, &*a.pool, a.priority, a.key.as_deref()),
        ("llm", "gpu", Priority::High, Some("job-1"))
    );
    assert_eq!(a.deadline, b.deadline);
    assert_eq!(a.fixed_start, b.fixed_start);
    assert_eq!(
        (a.duration_hint, a.timeout, a.retry),
        (b.duration_hint, b.timeout, b.retry)
    );
    assert_eq!(a.deps, b.deps);
    assert_eq!(a.resources, b.resources);
    assert_eq!(a.tags, b.tags);
    let retrying = &rec.tasks[1];
    assert_eq!((retrying.id, retrying.attempt), (TaskId(11), 1));
    assert_eq!(rec.terminal.len(), 1);
    let done = &rec.terminal[0];
    assert_eq!(
        (done.id, done.key.as_deref(), done.status, done.at),
        (TaskId(12), Some("done"), TaskStatus::Succeeded, Time(4 * S))
    );

    assert_eq!(rec.recurring.len(), 1);
    let (r, next) = &rec.recurring[0];
    assert_eq!(*next, Some(Time(3600 * S)));
    assert_eq!(
        (&*r.name, &*r.cron, r.tz.as_deref()),
        ("nightly", "0 3 * * *", Some("Europe/Lisbon"))
    );
    assert_eq!(
        (r.overlap, r.tolerance, r.on_miss),
        (
            OverlapPolicy::Allow,
            Duration::from_secs(5),
            MissPolicy::Fail
        )
    );
    assert_eq!(*r.template.payload, Bytes::from_static(b"tpl"));
    assert_eq!(
        (&*r.template.kind, &*r.template.pool, r.template.retry),
        ("report", "gpu", Some(RetryPolicy::none()))
    );
    assert_eq!(r.template.tags, recurring.template.tags);

    // Lookups.
    let rec12 = store
        .get_task(&TaskRef::Key("done".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (rec12.id, rec12.status, rec12.attempt),
        (TaskId(12), Some(TaskStatus::Succeeded), 1)
    );
    assert_eq!(rec12.output, Some(out));
    assert_eq!(rec12.message.as_deref(), Some("succeeded"));
    let rec10 = store
        .get_task(&TaskRef::Id(TaskId(10)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((rec10.status, rec10.pool.as_str()), (None, "gpu"));
    assert!(
        store
            .get_task(&TaskRef::Id(TaskId(99)))
            .await
            .unwrap()
            .is_none()
    );

    // Re-emitted submissions after recovery are idempotent.
    store
        .commit(epoch, base, &[submitted(10, 0, full_spec())], &[])
        .await
        .unwrap();
    assert_eq!(store.recover(base).await.unwrap().tasks.len(), 2);
}

#[tokio::test]
async fn times_are_rebased_and_old_terminals_pruned() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let epoch = store.acquire_leadership().await.unwrap();
    let now = now_unix();
    let old_base = base_at(now - 2 * 3600 * S as i64); // engine started two hours ago
    let batch = vec![
        submitted(
            1,
            0,
            TaskSpec::new(Bytes::new()).deadline(Time(2 * 3600 * S + 60 * S)),
        ),
        submitted(2, 1, TaskSpec::new(Bytes::new()).key("ancient")),
        status(2, TaskStatus::Failed, 1, 10 * S), // finished ~2h ago
        submitted(3, 2, TaskSpec::new(Bytes::new())),
        status(3, TaskStatus::Running, 1, 2 * 3600 * S - S),
    ];
    store.commit(epoch, old_base, &batch, &[]).await.unwrap();

    let new_base = base_at(now);
    let rec = store.recover(new_base).await.unwrap();
    // Deadline 60s after "now" maps to engine time ~60s; arrival before the new epoch clamps to 0.
    let t1 = rec.tasks.iter().find(|t| t.id == TaskId(1)).unwrap();
    assert_eq!(t1.arrival, Time::ZERO);
    assert_eq!(t1.spec.deadline, Some(Time(60 * S)));
    // A task persisted as running counts its lost attempt.
    assert_eq!(
        rec.tasks
            .iter()
            .find(|t| t.id == TaskId(3))
            .unwrap()
            .attempt,
        1
    );
    // Terminal records older than the retention window are not loaded...
    assert!(rec.terminal.is_empty());
    // ...and can be pruned.
    assert_eq!(store.prune(Duration::from_secs(3600)).await.unwrap(), 1);
    assert!(
        store
            .get_task(&TaskRef::Key("ancient".into()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn second_instance_waits_for_lock_and_fences_the_first() {
    let pg = pg_or_skip!();
    let a = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let b = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let ea = a.acquire_leadership().await.unwrap();
    let base = base_at(now_unix());
    a.commit(
        ea,
        base,
        &[submitted(1, 0, TaskSpec::new(Bytes::new()))],
        &[],
    )
    .await
    .unwrap();

    let b2 = b.clone();
    let waiting = tokio::spawn(async move { b2.acquire_leadership().await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "lock is held by the first instance");

    a.release_leadership().await;
    let eb = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(eb, ea + 1);
    assert!(b.is_leader() && !a.is_leader());
    let r = a
        .commit(
            ea,
            base,
            &[submitted(2, 1, TaskSpec::new(Bytes::new()))],
            &[],
        )
        .await;
    assert!(matches!(r, Err(StoreError::Fenced)), "{r:?}");
    b.commit(
        eb,
        base,
        &[submitted(2, 1, TaskSpec::new(Bytes::new()))],
        &[],
    )
    .await
    .unwrap();
    // A stale epoch is fenced by the database even if the instance believes it is leader.
    let c = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let r = c.commit(ea, base, &[], &[]).await;
    assert!(matches!(r, Err(StoreError::Fenced)));
}

async fn terminate_lock_holder(url: &str, key: i64) -> usize {
    let admin = PgStore::connect(url).await.unwrap();
    let pids: Vec<i32> = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .fetch_all(admin.pool())
    .await
    .unwrap();
    for pid in &pids {
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(*pid)
            .execute(admin.pool())
            .await
            .unwrap();
    }
    pids.len()
}

#[tokio::test]
async fn terminated_lock_session_triggers_failover() {
    let pg = pg_or_skip!();
    let a = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let b = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let ea = a.acquire_leadership().await.unwrap();
    let b2 = b.clone();
    let waiting = tokio::spawn(async move { b2.acquire_leadership().await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(terminate_lock_holder(&pg.url, fast_cfg().lock_key).await, 1);
    tokio::time::timeout(Duration::from_secs(5), a.leadership_lost())
        .await
        .expect("loss detected");
    let eb = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(eb > ea);
    let r = a.commit(ea, base_at(now_unix()), &[], &[]).await;
    assert!(matches!(r, Err(StoreError::Fenced)));
}

// ------------------------------------------------------------------------------------------
// End to end with the scheduler
// ------------------------------------------------------------------------------------------

type Runs = Arc<Mutex<HashMap<String, u32>>>;

fn pool(runs: Runs, delay: Duration) -> Pool<Bytes, Bytes> {
    Pool::fixed("default", 1).executor_fn(move |_ctx, p: Arc<Bytes>| {
        let runs = runs.clone();
        async move {
            tokio::time::sleep(delay).await;
            *runs
                .lock()
                .unwrap()
                .entry(String::from_utf8_lossy(&p).into_owned())
                .or_default() += 1;
            Ok::<_, TaskError>(Bytes::from(format!("out:{}", String::from_utf8_lossy(&p))))
        }
    })
}

async fn wait_succeeded(s: &Scheduler<Bytes, Bytes>, n: u64) {
    for _ in 0..200 {
        if s.snapshot().await.unwrap().totals.succeeded >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {n} successes: {:?}",
        s.snapshot().await.unwrap().totals
    );
}

#[tokio::test]
async fn scheduler_resumes_after_restart_without_rerunning_finished_work() {
    let pg = pg_or_skip!();
    let runs: Runs = Default::default();
    let store1 = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let s1 = Scheduler::<Bytes, Bytes>::builder()
        .pool(pool(runs.clone(), Duration::from_millis(100)))
        .store(store1.clone())
        .build()
        .await
        .unwrap();
    let a = s1
        .submit(Task::new(Bytes::from("a")).key("a"))
        .await
        .unwrap();
    for p in ["b", "c", "d"] {
        s1.enqueue(Task::new(Bytes::from(p)).key(p).after("a"))
            .await
            .unwrap();
    }
    assert_eq!(*a.await.unwrap(), Bytes::from("out:a"));
    tokio::time::sleep(Duration::from_millis(30)).await; // b is now running
    s1.shutdown(Duration::ZERO).await.unwrap(); // abrupt: b aborted, c and d queued
    drop(s1);
    store1.release_leadership().await;

    let store2 = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let s2 = Scheduler::<Bytes, Bytes>::builder()
        .pool(pool(runs.clone(), Duration::from_millis(10)))
        .store(store2.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(s2.status("a").await.unwrap(), Some(TaskStatus::Succeeded));
    wait_succeeded(&s2, 3).await;
    let runs = runs.lock().unwrap().clone();
    assert_eq!(runs["a"], 1, "finished work is not re-run");
    for p in ["b", "c", "d"] {
        assert_eq!(runs[p], 1, "{p} completed exactly once ({runs:?})");
        let r = store2
            .get_task(&TaskRef::Key(p.into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.status, Some(TaskStatus::Succeeded));
        assert_eq!(r.output, Some(Bytes::from(format!("out:{p}"))));
    }
    // b's aborted first attempt was persisted as running, so its successful run is attempt 2.
    assert_eq!(
        store2
            .get_task(&TaskRef::Key("b".into()))
            .await
            .unwrap()
            .unwrap()
            .attempt,
        2
    );
    // New ids carry the new epoch.
    let fresh = s2.enqueue(Task::new(Bytes::from("e"))).await.unwrap();
    assert_eq!(fresh.id.0 >> 48, 2);
    s2.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn standby_takes_over_when_leader_session_dies() {
    let pg = pg_or_skip!();
    let runs: Runs = Default::default();
    let store1 = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let s1 = Scheduler::<Bytes, Bytes>::builder()
        .pool(pool(runs.clone(), Duration::from_millis(50)))
        .store(store1.clone())
        .build()
        .await
        .unwrap();
    for i in 0..5 {
        s1.enqueue(Task::new(Bytes::from(format!("t{i}"))).key(format!("t{i}")))
            .await
            .unwrap();
    }
    // Standby blocks in build() until it becomes leader.
    let store2 = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let r2 = runs.clone();
    let st2 = store2.clone();
    let standby = tokio::spawn(async move {
        Scheduler::<Bytes, Bytes>::builder()
            .pool(pool(r2, Duration::from_millis(10)))
            .store(st2)
            .build()
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!standby.is_finished());

    assert_eq!(terminate_lock_holder(&pg.url, fast_cfg().lock_key).await, 1);
    tokio::time::timeout(Duration::from_secs(5), store1.leadership_lost())
        .await
        .expect("old leader notices");
    let s2 = tokio::time::timeout(Duration::from_secs(10), standby)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The old leader is fenced on its next write and stops.
    let _ = s1.enqueue(Task::new(Bytes::from("late"))).await;
    let r = s1.shutdown(Duration::ZERO).await;
    assert!(
        matches!(r, Err(cronoq::Error::Store(StoreError::Fenced)) | Ok(())),
        "{r:?}"
    );

    // Everything the old leader accepted completes under the new one.
    for i in 0..5 {
        let key = format!("t{i}");
        for _ in 0..200 {
            if s2.status(key.as_str()).await.unwrap() == Some(TaskStatus::Succeeded) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            s2.status(key.as_str()).await.unwrap(),
            Some(TaskStatus::Succeeded),
            "{key}"
        );
    }
    assert!(
        store2
            .get_task(&TaskRef::Key("late".into()))
            .await
            .unwrap()
            .is_none(),
        "fenced write was rejected"
    );
    s2.shutdown(Duration::from_secs(1)).await.unwrap();
}
