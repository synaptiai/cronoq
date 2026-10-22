//! Behaviour specific to the SQLite store: lease expiry, takeover, fencing and files.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::TestDb;
use cronoq::{Store, StoreError, TimeBase};
use cronoq_core::*;
use cronoq_store_sqlite::{SqliteStore, SqliteStoreConfig};

const BASE: TimeBase = TimeBase {
    unix_nanos_at_zero: 0,
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn submitted(id: u64) -> Mutation<Bytes> {
    Mutation::Submitted {
        id: TaskId(id),
        seq: id,
        arrival: Time(now()),
        spec: TaskSpec::new(Bytes::from_static(b"x")),
    }
}

// A leader that stops renewing (crashed, or dropped without releasing) loses its lease after
// the TTL, and a standby takes over.
#[tokio::test(flavor = "multi_thread")]
async fn standby_takes_over_after_the_lease_runs_out() {
    let db = TestDb::new();
    let a = db.open().await;
    let b = db.open().await;
    let ea = a.acquire_leadership().await.unwrap();
    drop(a); // its renewal task ends; the lease is never released
    let started = std::time::Instant::now();
    let eb = tokio::time::timeout(Duration::from_secs(5), b.acquire_leadership())
        .await
        .expect("standby took over")
        .unwrap();
    assert!(eb > ea);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "waited for the lease to run out"
    );
}

// A leader that was paused past its lease (simulated by expiring the lease in the file) is
// fenced on its next commit and notices the loss.
#[tokio::test(flavor = "multi_thread")]
async fn paused_leader_is_fenced_after_a_takeover() {
    let db = TestDb::new();
    let a = db.open().await;
    let b = db.open().await;
    let ea = a.acquire_leadership().await.unwrap();
    a.commit(ea, BASE, &[submitted(1)], &[]).await.unwrap();
    sqlx::query("UPDATE cronoq_leader SET expires_unix_ns = 0")
        .execute(b.pool())
        .await
        .unwrap();
    let eb = tokio::time::timeout(Duration::from_secs(5), b.acquire_leadership())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(eb, ea + 1);
    let r = a.commit(ea, BASE, &[submitted(2)], &[]).await;
    assert!(matches!(r, Err(StoreError::Fenced)), "{r:?}");
    assert!(!a.is_leader());
    tokio::time::timeout(Duration::from_secs(5), a.leadership_lost())
        .await
        .expect("old leader notices");
    b.commit(eb, BASE, &[submitted(2)], &[]).await.unwrap();
    assert!(b.get_task(&TaskRef::Id(TaskId(2))).await.unwrap().is_some());
}

// The renewal task notices a takeover by itself, without a commit.
#[tokio::test(flavor = "multi_thread")]
async fn renewal_notices_a_takeover() {
    let db = TestDb::new();
    let a = db.open().await;
    a.acquire_leadership().await.unwrap();
    sqlx::query("UPDATE cronoq_leader SET holder = 'someone-else', epoch = epoch + 1")
        .execute(a.pool())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), a.leadership_lost())
        .await
        .expect("loss detected by renewal");
    assert!(!a.is_leader());
}

// The lease is renewed while the leader is alive, so a standby keeps waiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_leader_keeps_its_lease() {
    let db = TestDb::new();
    let a = db.open().await;
    let b = db.open().await;
    a.acquire_leadership().await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(4), b.acquire_leadership()).await;
    assert!(r.is_err(), "standby must not take over a renewed lease");
    assert!(a.is_leader());
}

#[cfg(unix)]
#[tokio::test]
async fn database_files_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let db = TestDb::new();
    let s = db.open().await;
    let e = s.acquire_leadership().await.unwrap();
    s.commit(e, BASE, &[submitted(1)], &[]).await.unwrap();
    for f in ["state.db", "state.db-wal", "state.db-shm"] {
        let p = db.dir.join(f);
        if p.exists() {
            let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "{f} has mode {mode:o}");
        }
    }
}

#[tokio::test]
async fn in_memory_and_empty_paths_are_refused() {
    for p in [":memory:", ""] {
        let r = SqliteStore::open_with(p, SqliteStoreConfig::default()).await;
        assert!(r.is_err(), "{p:?}");
    }
}

// NUL characters are stored as U+FFFD, as in the PostgreSQL store.
#[tokio::test]
async fn nul_characters_are_replaced() {
    let db = TestDb::new();
    let s = db.open().await;
    let e = s.acquire_leadership().await.unwrap();
    let batch = [
        Mutation::Submitted {
            id: TaskId(1),
            seq: 0,
            arrival: Time(now()),
            spec: TaskSpec::new(Bytes::new()).key("a\0b").tag("k\0", "v\0"),
        },
        Mutation::Status {
            id: TaskId(1),
            status: TaskStatus::Failed,
            attempt: 1,
            at: Time(now()),
            message: Some("bad\0byte".into()),
        },
    ];
    s.commit(e, BASE, &batch, &[]).await.unwrap();
    let t = s
        .get_task(&TaskRef::Key("a\0b".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.key.as_deref(), Some("a\u{FFFD}b"));
    assert_eq!(t.message.as_deref(), Some("bad\u{FFFD}byte"));
}
