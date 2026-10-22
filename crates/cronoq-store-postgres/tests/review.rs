//! Regression tests for the second review of the PostgreSQL store. Each test reproduces a
//! reported defect; all of them failed before the corresponding fix.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{Pool, Scheduler, Store, TaskError, TimeBase};
use cronoq_core::*;
use cronoq_store_postgres::{PgStore, PgStoreConfig};

const S: u64 = 1_000_000_000;
/// The runtime's time base: engine time is Unix time.
const BASE: TimeBase = TimeBase {
    unix_nanos_at_zero: 0,
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
fn hours_ago(h: u64) -> u64 {
    now() - h * 3600 * S
}
fn fast_cfg() -> PgStoreConfig {
    PgStoreConfig {
        lock_poll: Duration::from_millis(50),
        keepalive: Duration::from_millis(100),
        ..Default::default()
    }
}
fn in_schema(schema: &str) -> PgStoreConfig {
    PgStoreConfig {
        schema: schema.into(),
        ..fast_cfg()
    }
}
fn submitted_at(id: u64, seq: u64, at: u64, spec: TaskSpec<Bytes>) -> Mutation<Bytes> {
    Mutation::Submitted {
        id: TaskId(id),
        seq,
        arrival: Time(at),
        spec,
    }
}
fn submitted(id: u64, seq: u64, spec: TaskSpec<Bytes>) -> Mutation<Bytes> {
    submitted_at(id, seq, now(), spec)
}
fn status(id: u64, st: TaskStatus, attempt: u32, at: u64, msg: Option<&str>) -> Mutation<Bytes> {
    Mutation::Status {
        id: TaskId(id),
        status: st,
        attempt,
        at: Time(at),
        message: msg.map(Into::into),
    }
}

type Runs = Arc<Mutex<HashMap<String, u32>>>;
fn pool(runs: Runs) -> Pool<Bytes, Bytes> {
    Pool::fixed("default", 2).executor_fn(move |_ctx, p: Arc<Bytes>| {
        let runs = runs.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            *runs
                .lock()
                .unwrap()
                .entry(String::from_utf8_lossy(&p).into_owned())
                .or_default() += 1;
            Ok::<_, TaskError>(Bytes::new())
        }
    })
}
async fn scheduler(store: PgStore, runs: Runs) -> Scheduler<Bytes, Bytes> {
    Scheduler::<Bytes, Bytes>::builder()
        .pool(pool(runs))
        .store(store)
        .build()
        .await
        .unwrap()
}

// --- NUL characters (rejected by PostgreSQL text/JSONB) are stored as U+FFFD instead of
//     failing the whole commit.
#[tokio::test]
async fn nul_in_message_and_tags_is_stored() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let e = store.acquire_leadership().await.unwrap();
    let spec = TaskSpec::new(Bytes::new()).tag("k", "v\0").key("a\0b");
    store
        .commit(e, BASE, &[submitted(1, 0, spec)], &[])
        .await
        .unwrap();
    store
        .commit(
            e,
            BASE,
            &[status(1, TaskStatus::Failed, 1, now(), Some("bad\0byte"))],
            &[],
        )
        .await
        .unwrap();
    let t = store
        .get_task(&TaskRef::Key("a\0b".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.message.as_deref(), Some("bad\u{FFFD}byte"));
    assert_eq!(t.key.as_deref(), Some("a\u{FFFD}b"));
}

// --- End to end: an executor error containing NUL used to stop the scheduler.
#[tokio::test]
async fn executor_error_with_nul_does_not_stop_the_scheduler() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let s = Scheduler::<Bytes, Bytes>::builder()
        .pool(
            Pool::fixed("default", 1).executor_fn(|_ctx, _p: Arc<Bytes>| async move {
                Err::<Bytes, _>(TaskError::permanent("utf16 decode failed: \0\0"))
            }),
        )
        .store(store.clone())
        .build()
        .await
        .unwrap();
    let h = s
        .submit(cronoq::Task::new(Bytes::from("x")).key("x"))
        .await
        .unwrap();
    assert!(h.await.is_err());
    assert!(s.enqueue(cronoq::Task::new(Bytes::from("y"))).await.is_ok());
    assert!(s.shutdown(Duration::ZERO).await.is_ok());
}

// --- A non-finite retry multiplier (stored as JSON null) used to make every recovery fail.
#[tokio::test]
async fn non_finite_retry_multiplier_does_not_break_recovery() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let e = store.acquire_leadership().await.unwrap();
    let spec = TaskSpec::new(Bytes::new()).retry(RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        multiplier: f64::INFINITY,
        max_backoff: Duration::from_secs(1),
        jitter: false,
    });
    store
        .commit(e, BASE, &[submitted(1, 0, spec)], &[])
        .await
        .unwrap();
    // A row written by an older version.
    sqlx::query("UPDATE cronoq_tasks SET retry = jsonb_set(retry, '{multiplier}', 'null')")
        .execute(store.pool())
        .await
        .unwrap();
    let rec = store.recover(BASE).await.unwrap();
    let retry = rec.tasks[0].spec.retry.unwrap();
    assert!(retry.multiplier.is_finite());
}

// --- A row that cannot be decoded is quarantined (marked failed), not fatal to recovery.
#[tokio::test]
async fn corrupt_rows_are_quarantined() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let e = store.acquire_leadership().await.unwrap();
    let batch = [
        submitted(1, 0, TaskSpec::new(Bytes::new()).key("bad")),
        submitted(2, 1, TaskSpec::new(Bytes::new())),
    ];
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    sqlx::query("UPDATE cronoq_tasks SET deps = '{\"not\": \"a list\"}' WHERE id = 1")
        .execute(store.pool())
        .await
        .unwrap();
    let rec = store.recover(BASE).await.unwrap();
    assert_eq!(rec.tasks.len(), 1);
    assert_eq!(rec.tasks[0].id, TaskId(2));
    let q = rec.terminal.iter().find(|t| t.id == TaskId(1)).unwrap();
    assert_eq!(
        (q.status, q.key.as_deref()),
        (TaskStatus::Failed, Some("bad"))
    );
    let row = store
        .get_task(&TaskRef::Id(TaskId(1)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, Some(TaskStatus::Failed));
    assert!(row.message.unwrap().contains("corrupt"));
}

// --- Different clusters (schemas) are independent: separate epochs, no shared locks.
#[tokio::test]
async fn clusters_in_different_schemas_are_isolated() {
    let pg = pg_or_skip!();
    let a = PgStore::connect_with(&pg.url, in_schema("cluster_a"))
        .await
        .unwrap();
    let b = PgStore::connect_with(&pg.url, in_schema("cluster_b"))
        .await
        .unwrap();
    let ea = a.acquire_leadership().await.unwrap();
    let eb = tokio::time::timeout(Duration::from_secs(2), b.acquire_leadership())
        .await
        .expect("cluster B is not blocked by cluster A")
        .unwrap();
    assert_eq!((ea, eb), (1, 1));
    a.commit(
        ea,
        BASE,
        &[submitted(1, 0, TaskSpec::new(Bytes::from("a")))],
        &[],
    )
    .await
    .unwrap();
    b.commit(eb, BASE, &[], &[]).await.unwrap();
    assert!(a.is_leader() && b.is_leader());
    assert_eq!(a.recover(BASE).await.unwrap().tasks.len(), 1);
    assert!(b.recover(BASE).await.unwrap().tasks.is_empty());
    // A third instance of cluster A waits for A's leader.
    let a2 = PgStore::connect_with(&pg.url, in_schema("cluster_a"))
        .await
        .expect("migrations do not wait for the leader lock");
    let waiting = tokio::time::timeout(Duration::from_millis(300), a2.acquire_leadership()).await;
    assert!(waiting.is_err(), "two leaders in one cluster");
}

#[tokio::test]
async fn invalid_schema_names_are_rejected() {
    let pg = pg_or_skip!();
    let r = PgStore::connect_with(&pg.url, in_schema("x\"; DROP TABLE y; --")).await;
    assert!(r.is_err());
}

// --- Migrations are versioned and idempotent.
#[tokio::test]
async fn schema_version_is_recorded() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    store.migrate().await.unwrap();
    let v: i32 = sqlx::query_scalar("SELECT version FROM cronoq_schema_version")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(v, 3);
    sqlx::query("UPDATE cronoq_schema_version SET version = 99")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(
        store.migrate().await.is_err(),
        "newer schema must be refused"
    );
}

// --- After an epoch wrap, ids continued from MAX(id)+1; pruning the newest rows made the
//     next leader hand out ids that were already used.
#[tokio::test]
async fn ids_are_not_reused_after_epoch_wrap_and_prune() {
    let pg = pg_or_skip!();
    let store = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    sqlx::query("UPDATE cronoq_leader SET epoch = 32766")
        .execute(store.pool())
        .await
        .unwrap();
    let e = store.acquire_leadership().await.unwrap();
    assert_eq!(e, 32767);
    let p = 32767u64 << 48;
    let old = hours_ago(3);
    let mut batch = vec![submitted_at(
        p | 1,
        0,
        old,
        TaskSpec::new(Bytes::from("live")).fixed_start(FixedStart {
            at: Time(now() + 10 * 3600 * S),
            tolerance: Duration::from_secs(1),
            on_miss: MissPolicy::Skip,
        }),
    )];
    for i in 2..=5 {
        batch.push(submitted_at(
            p | i,
            i,
            old,
            TaskSpec::new(Bytes::from(format!("old{i}"))),
        ));
        batch.push(status(p | i, TaskStatus::Succeeded, 1, old + S, None));
    }
    store.commit(e, BASE, &batch, &[]).await.unwrap();
    store.release_leadership().await;
    assert_eq!(store.prune(Duration::from_secs(3600)).await.unwrap(), 4);

    let store2 = PgStore::connect_with(&pg.url, fast_cfg()).await.unwrap();
    let s = scheduler(store2.clone(), Default::default()).await;
    assert_eq!(store2.epoch(), Some(1));
    let fresh = s
        .enqueue(cronoq::Task::new(Bytes::from("new")))
        .await
        .unwrap();
    assert!(
        fresh.id.0 > (p | 5),
        "id {:#x} was already issued",
        fresh.id.0
    );
}

// --- Existing cronoq tables were adopted whatever their owner, so another database role could
//     plant them (with triggers that then ran as cronoq's role) before the first start.
#[tokio::test]
async fn tables_owned_by_another_role_are_refused() {
    let pg = pg_or_skip!();
    let admin = sqlx::PgPool::connect(&pg.url).await.unwrap();
    for stmt in [
        "CREATE ROLE mallory LOGIN",
        "CREATE ROLE app LOGIN",
        "GRANT CREATE ON DATABASE postgres TO app",
        "CREATE SCHEMA planted",
        "GRANT ALL ON SCHEMA planted TO mallory, app",
        "CREATE TABLE planted.cronoq_schema_version \
         (id SMALLINT PRIMARY KEY CHECK (id = 1), version INT NOT NULL)",
        "INSERT INTO planted.cronoq_schema_version VALUES (1, 2)",
        "GRANT ALL ON planted.cronoq_schema_version TO PUBLIC",
        "ALTER TABLE planted.cronoq_schema_version OWNER TO mallory",
    ] {
        sqlx::query(stmt).execute(&admin).await.unwrap();
    }
    let app_url = pg.url.replace("postgres@", "app@");
    for url in [&app_url, &pg.url] {
        let err = PgStore::connect_with(url, in_schema("planted"))
            .await
            .err()
            .expect("planted tables must be refused");
        assert!(err.to_string().contains("owned by another role"), "{err}");
    }
    // The role's own tables, created on first connect, are accepted on every later one.
    PgStore::connect_with(&app_url, in_schema("own"))
        .await
        .unwrap();
    PgStore::connect_with(&app_url, in_schema("own"))
        .await
        .unwrap();
}
