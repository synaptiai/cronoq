mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cronoq::{Pool, Scheduler, Store, StoreError, Task, TaskError, TimeBase};
use cronoq_core::*;
use cronoq_store_postgres::{PgStore, PgStoreConfig};

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

async fn terminate_lock_holder(url: &str, (k1, k2): (i32, i32)) -> usize {
    let admin = PgStore::connect(url).await.unwrap();
    let pids: Vec<i32> = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 2 \
         AND classid = ($1::bigint & 4294967295)::oid AND objid = ($2::bigint & 4294967295)::oid",
    )
    .bind(k1 as i64)
    .bind(k2 as i64)
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

    assert_eq!(terminate_lock_holder(&pg.url, a.leader_lock()).await, 1);
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

    assert_eq!(
        terminate_lock_holder(&pg.url, store1.leader_lock()).await,
        1
    );
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
