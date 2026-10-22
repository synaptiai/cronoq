//! # cronoq-store-sqlite
//!
//! A SQLite [`Store`] for cronoq: durable state in one local file, no database server.
//!
//! - **Write-ahead state**: every engine state transition is committed in batches, one
//!   transaction per batch, before submitters are acknowledged. The file uses SQLite's WAL
//!   journal and, by default, syncs it on every commit.
//! - **Leader and standbys**: several processes on the same host may open the same file. One
//!   leads through a lease row in `cronoq_leader` that it renews; the others wait and take over
//!   once the lease runs out. Taking over bumps a fencing epoch.
//! - **Fencing**: each commit re-reads the epoch and lease holder inside its write transaction
//!   (SQLite serializes writers) and fails with [`StoreError::Fenced`] if another process took
//!   over, so a paused old leader can never write after a standby replaced it.
//! - **Recovery**: a new leader loads live tasks, recently finished ones (for key
//!   deduplication), every finished task a live task still depends on, and recurring
//!   schedules. Rows that cannot be decoded are quarantined (marked failed).
//! - **Files**: the database is created readable and writable by its owner only (on Unix). Keep
//!   it on a local filesystem: SQLite's WAL mode does not work over network filesystems.
//!
//! Times are stored as wall-clock Unix nanoseconds; strings are stored with NUL replaced by
//! U+FFFD, as in the PostgreSQL store. Lease expiry uses the wall clock, which processes on one
//! host share.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use bytes::Bytes;
//! use cronoq::{Pool, Scheduler};
//! use cronoq_store_sqlite::SqliteStore;
//!
//! let store = SqliteStore::open("/var/lib/cronoq/state.db").await?;
//! let sched = Scheduler::<Bytes, Bytes>::builder()
//!     .pool(Pool::new("default").executor_fn(|_ctx, p: std::sync::Arc<Bytes>| async move {
//!         Ok::<_, cronoq::TaskError>((*p).clone())
//!     }))
//!     .store(store.clone())
//!     .build() // blocks until this process is the leader
//!     .await?;
//! tokio::select! {
//!     _ = store.leadership_lost() => { /* step down: stop serving, exit or rejoin */ }
//!     _ = tokio::signal::ctrl_c() => {}
//! }
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cronoq::codec::*;
use cronoq::{Store, StoreError, StoreQueries, TimeBase};
use cronoq_core::{
    Mutation, Priority, Recovered, RecoveredTask, RecoveredTerminal, TaskId, TaskRef, TaskSpec,
    TaskStatus, Time,
};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{AssertSqlSafe, Row, Sqlite, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Ordered schema migrations; `cronoq_schema_version` records how many were applied.
const MIGRATIONS: &[&str] = &[include_str!("schema.sql")];
/// Status stored for tasks that are live but have not started an attempt yet.
const QUEUED: &str = "queued";
/// Fencing epochs stay within 1..=32767 so epoch-prefixed task ids fit in a signed integer.
const MAX_EPOCH: i64 = 32767;
/// Rows deleted per statement by [`SqliteStore::prune`].
const PRUNE_BATCH: i64 = 1000;
/// Longest wait between attempts to reach the database while acquiring leadership.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Persisted view of a task, for lookups by id or key.
pub type TaskRecord = cronoq::TaskRecord<Bytes>;

/// Store configuration.
#[derive(Clone, Debug)]
pub struct SqliteStoreConfig {
    /// How long finished tasks are loaded on recovery, for key deduplication. Finished tasks
    /// that live tasks depend on are always loaded.
    pub retention: Duration,
    /// How long a leader's lease lasts without renewal; a standby takes over once it runs out.
    pub lease_ttl: Duration,
    /// How often the leader renews its lease (well below `lease_ttl`).
    pub lease_renew: Duration,
    /// How often a standby checks whether the lease has run out.
    pub lock_poll: Duration,
    /// How long a statement waits for another process's write to finish.
    pub busy_timeout: Duration,
    /// Upper bound for one commit.
    pub commit_timeout: Duration,
    /// Sync the journal to disk on every commit (default). Without it commits are faster, but a
    /// power loss can drop the most recent ones (the file stays consistent).
    pub full_sync: bool,
    /// Connections in the pool.
    pub max_connections: u32,
}

impl Default for SqliteStoreConfig {
    fn default() -> Self {
        SqliteStoreConfig {
            retention: Duration::from_secs(3600),
            lease_ttl: Duration::from_secs(10),
            lease_renew: Duration::from_secs(2),
            lock_poll: Duration::from_millis(500),
            busy_timeout: Duration::from_secs(5),
            commit_timeout: Duration::from_secs(30),
            full_sync: true,
            max_connections: 4,
        }
    }
}

struct Leader {
    epoch: u16,
    keeper: JoinHandle<()>,
}

struct Inner {
    pool: SqlitePool,
    cfg: SqliteStoreConfig,
    /// Identifies this store instance as the lease holder.
    holder: String,
    leader: Mutex<Option<Leader>>,
    lost: watch::Sender<bool>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(l) = self.leader.get_mut().unwrap().take() {
            l.keeper.abort();
        }
    }
}

/// SQLite-backed [`Store`]. Cheap to clone.
#[derive(Clone)]
pub struct SqliteStore {
    inner: Arc<Inner>,
}

/// Map a database error, classifying failures that are worth retrying as `Unavailable`.
fn db(e: sqlx::Error) -> StoreError {
    use sqlx::Error as E;
    let transient = match &e {
        E::Io(_) | E::PoolTimedOut | E::WorkerCrashed => true,
        // Primary result codes 5 (SQLITE_BUSY) and 6 (SQLITE_LOCKED): another process holds
        // the write lock longer than the busy timeout.
        E::Database(d) => d
            .code()
            .and_then(|c| c.parse::<i32>().ok())
            .is_some_and(|c| matches!(c & 0xff, 5 | 6)),
        _ => false,
    };
    if transient {
        StoreError::Unavailable(e.to_string())
    } else {
        StoreError::Other(e.to_string())
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

/// A migration split into statements. Comment lines are dropped first; migrations have no
/// string literals containing semicolons.
fn statements(sql: &str) -> Vec<String> {
    let code: String = sql
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    code.split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// A random identifier for this store instance.
fn new_holder() -> String {
    use std::hash::{BuildHasher, RandomState};
    let (a, b) = (RandomState::new(), RandomState::new());
    format!(
        "{:x}-{:016x}{:016x}",
        std::process::id(),
        a.hash_one(now_unix()),
        b.hash_one(std::thread::current().id())
    )
}

/// Create the database file readable and writable by its owner only, if it does not exist.
/// SQLite gives its journal files the same permissions.
fn create_private(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

impl SqliteStore {
    /// Open (creating if needed) the database at `path` with default settings.
    pub async fn open(path: impl AsRef<Path>) -> Result<SqliteStore, StoreError> {
        Self::open_with(path, SqliteStoreConfig::default()).await
    }

    /// Open (creating if needed) the database at `path` with `cfg` and apply the schema.
    pub async fn open_with(
        path: impl AsRef<Path>,
        cfg: SqliteStoreConfig,
    ) -> Result<SqliteStore, StoreError> {
        let path = path.as_ref();
        // SQLite treats ":..." names and "file:" URIs specially (in-memory databases, options
        // that turn off the locking fencing relies on); only plain file paths are accepted.
        let text = path.to_string_lossy();
        if text.is_empty()
            || text.starts_with(':')
            || text
                .get(..5)
                .is_some_and(|p| p.eq_ignore_ascii_case("file:"))
        {
            return Err(StoreError::Other(format!(
                "invalid SQLite path {path:?}: use a plain file path on a local filesystem"
            )));
        }
        create_private(path)
            .map_err(|e| StoreError::Other(format!("creating {}: {e}", path.display())))?;
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(if cfg.full_sync {
                SqliteSynchronous::Full
            } else {
                SqliteSynchronous::Normal
            })
            .busy_timeout(cfg.busy_timeout)
            .foreign_keys(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(cfg.max_connections.max(1))
            .connect_with(opts)
            .await
            .map_err(db)?;
        Self::migrate_pool(&pool).await?;
        Ok(SqliteStore {
            inner: Arc::new(Inner {
                pool,
                cfg,
                holder: new_holder(),
                leader: Mutex::new(None),
                lost: watch::channel(false).0,
            }),
        })
    }

    /// Apply pending schema migrations (idempotent; serialized across processes).
    /// [`SqliteStore::open`] already does this.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        Self::migrate_pool(&self.inner.pool).await
    }

    async fn migrate_pool(pool: &SqlitePool) -> Result<(), StoreError> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cronoq_schema_version \
             (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)",
        )
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "INSERT INTO cronoq_schema_version (id, version) VALUES (1, 0) \
             ON CONFLICT (id) DO NOTHING",
        )
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let version: i64 = sqlx::query_scalar("SELECT version FROM cronoq_schema_version")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let version = usize::try_from(version).unwrap_or(0);
        if version > MIGRATIONS.len() {
            return Err(StoreError::Other(format!(
                "database schema version {version} is newer than this build supports ({})",
                MIGRATIONS.len()
            )));
        }
        for m in &MIGRATIONS[version..] {
            for stmt in statements(m) {
                // Migrations are compile-time constants of this crate.
                sqlx::query(AssertSqlSafe(stmt))
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
            }
        }
        if version < MIGRATIONS.len() {
            sqlx::query("UPDATE cronoq_schema_version SET version = ?1 WHERE id = 1")
                .bind(MIGRATIONS.len() as i64)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// The connection pool, for monitoring queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }

    /// The configuration this store was opened with.
    pub fn config(&self) -> &SqliteStoreConfig {
        &self.inner.cfg
    }

    /// Current fencing epoch, if this instance holds leadership.
    pub fn epoch(&self) -> Option<u16> {
        if *self.inner.lost.borrow() {
            return None;
        }
        self.inner.leader.lock().unwrap().as_ref().map(|l| l.epoch)
    }

    /// Whether this instance currently holds leadership.
    pub fn is_leader(&self) -> bool {
        self.epoch().is_some()
    }

    /// Resolves once leadership held by this instance is lost (lease taken over or not
    /// renewable).
    pub async fn leadership_lost(&self) {
        let mut rx = self.inner.lost.subscribe();
        let _ = rx.wait_for(|lost| *lost).await;
    }

    /// Give up leadership, letting a standby take over at once.
    pub async fn release_leadership(&self) {
        let old = self.inner.leader.lock().unwrap().take();
        if let Some(l) = old {
            l.keeper.abort();
            let _ = l.keeper.await;
            let r = sqlx::query(
                "UPDATE cronoq_leader SET holder = NULL, expires_unix_ns = 0 \
                 WHERE id = 1 AND holder = ?1",
            )
            .bind(&self.inner.holder)
            .execute(&self.inner.pool)
            .await;
            if let Err(e) = r {
                tracing::warn!(error = %e, "could not clear the leader lease; it will expire");
            }
        }
        self.inner.lost.send_replace(true);
    }

    /// Look up a task by id, or the newest task with a key.
    pub async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord>, StoreError> {
        macro_rules! select {
            ($rest:literal) => {
                concat!(
                    "SELECT id, key, pool, kind, priority, status, attempt, message, output, \
                     updated_unix_ns FROM cronoq_tasks ",
                    $rest
                )
            };
        }
        let row = match target {
            TaskRef::Id(id) => sqlx::query(select!("WHERE id = ?1"))
                .bind(id.0 as i64)
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(db)?,
            TaskRef::Key(k) => sqlx::query(select!("WHERE key = ?1 ORDER BY id DESC LIMIT 1"))
                .bind(clean(k))
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(db)?,
        };
        let Some(r) = row else { return Ok(None) };
        let status: String = r.get("status");
        let updated: i64 = r.get("updated_unix_ns");
        Ok(Some(TaskRecord {
            id: TaskId(r.get::<i64, _>("id") as u64),
            key: r.get("key"),
            pool: r.get("pool"),
            kind: r.get("kind"),
            priority: Priority::from_index(r.get::<i64, _>("priority").max(0) as usize)
                .unwrap_or_default(),
            status: TaskStatus::from_name(&status),
            attempt: r.get::<i64, _>("attempt").clamp(0, u32::MAX as i64) as u32,
            message: r.get("message"),
            output: r.get::<Option<Vec<u8>>, _>("output").map(Bytes::from),
            updated: UNIX_EPOCH + Duration::from_nanos(updated.max(0) as u64),
        }))
    }

    /// Delete finished tasks last updated more than `older_than` ago, except those that live
    /// tasks still depend on. Deletes in bounded batches; returns the number of rows removed.
    pub async fn prune(&self, older_than: Duration) -> Result<u64, StoreError> {
        let cutoff = now_unix().saturating_sub(dur_ns(older_than));
        let mut total = 0;
        loop {
            let r = sqlx::query(
                "WITH refs AS ( \
                   SELECT json_extract(d.value, '$.id') AS id, json_extract(d.value, '$.key') AS key \
                   FROM cronoq_tasks l, json_each(l.deps) d \
                   WHERE l.terminal = 0 AND l.deps <> '[]'), \
                 victims AS ( \
                   SELECT t.id FROM cronoq_tasks t \
                   WHERE t.terminal = 1 AND t.updated_unix_ns < ?1 \
                   AND t.id NOT IN (SELECT id FROM refs WHERE id IS NOT NULL) \
                   AND (t.key IS NULL OR t.key NOT IN (SELECT key FROM refs WHERE key IS NOT NULL)) \
                   ORDER BY t.updated_unix_ns LIMIT ?2) \
                 DELETE FROM cronoq_tasks WHERE id IN (SELECT id FROM victims)",
            )
            .bind(cutoff)
            .bind(PRUNE_BATCH)
            .execute(&self.inner.pool)
            .await
            .map_err(db)?;
            total += r.rows_affected();
            if r.rows_affected() < PRUNE_BATCH as u64 {
                return Ok(total);
            }
        }
    }

    /// Take the lease if it is free or expired; returns the new epoch, or `None` while another
    /// instance holds it.
    async fn try_lead(&self) -> Result<Option<u16>, StoreError> {
        let inner = &self.inner;
        let mut tx = inner.pool.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let row =
            sqlx::query("SELECT epoch, holder, expires_unix_ns FROM cronoq_leader WHERE id = 1")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        let epoch: i64 = row.get("epoch");
        let holder: Option<String> = row.get("holder");
        let expires: i64 = row.get("expires_unix_ns");
        let now = now_unix();
        let free = holder.is_none() || holder.as_deref() == Some(&inner.holder) || expires <= now;
        if !free {
            return Ok(None); // dropping the transaction rolls it back
        }
        let next = if epoch >= MAX_EPOCH { 1 } else { epoch + 1 };
        sqlx::query(
            "UPDATE cronoq_leader SET epoch = ?1, holder = ?2, expires_unix_ns = ?3 WHERE id = 1",
        )
        .bind(next)
        .bind(&inner.holder)
        .bind(now.saturating_add(dur_ns(inner.cfg.lease_ttl)))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Some(next as u16))
    }

    /// Renew the lease until it is lost: taken over by another instance, or not renewed
    /// before it ran out.
    async fn keeper(store: std::sync::Weak<Inner>, epoch: u16, expires: i64) {
        let mut expires = expires;
        loop {
            let Some(inner) = store.upgrade() else { return };
            let every = inner.cfg.lease_renew;
            let ttl = dur_ns(inner.cfg.lease_ttl);
            drop(inner);
            tokio::time::sleep(every).await;
            let Some(inner) = store.upgrade() else { return };
            let now = now_unix();
            let r = sqlx::query(
                "UPDATE cronoq_leader SET expires_unix_ns = ?1 \
                 WHERE id = 1 AND epoch = ?2 AND holder = ?3",
            )
            .bind(now.saturating_add(ttl))
            .bind(epoch as i64)
            .bind(&inner.holder)
            .execute(&inner.pool)
            .await;
            match r {
                Ok(r) if r.rows_affected() == 1 => expires = now.saturating_add(ttl),
                Ok(_) => {
                    tracing::error!("cronoq leader lease was taken over; stepping down");
                    inner.lost.send_replace(true);
                    return;
                }
                Err(e) if now_unix() >= expires => {
                    tracing::error!(error = %e, "cronoq leader lease could not be renewed before it ran out; stepping down");
                    inner.lost.send_replace(true);
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "renewing the cronoq leader lease failed; retrying")
                }
            }
        }
    }

    async fn commit_inner(
        &self,
        epoch: u16,
        base: TimeBase,
        batch: &[Mutation<Bytes>],
        outputs: &[(TaskId, &Bytes)],
    ) -> Result<(), StoreError> {
        // Fold the batch: new tasks are inserted with their latest status; tasks inserted
        // earlier get one update each. Later mutations win; a message carries forward until a
        // later transition replaces it, so the result does not depend on batching.
        let now = now_unix();
        let mut inserts: Vec<InsertRow> = Vec::new();
        let mut inserted: HashMap<TaskId, usize> = HashMap::new();
        let mut updates: Vec<(TaskId, StatusRow)> = Vec::new();
        let mut updated: HashMap<TaskId, usize> = HashMap::new();
        for m in batch {
            match m {
                Mutation::Submitted {
                    id,
                    seq,
                    arrival,
                    spec,
                } => {
                    if let Entry::Vacant(e) = inserted.entry(*id) {
                        e.insert(inserts.len());
                        inserts.push(InsertRow::new(base, now, *id, *seq, *arrival, spec));
                    }
                }
                Mutation::Status {
                    id,
                    status: st,
                    attempt,
                    at,
                    message,
                } => {
                    let row = StatusRow {
                        status: *st,
                        attempt: *attempt,
                        at: to_unix(base, *at),
                        message: message.as_deref().map(cleaned),
                    };
                    if let Some(&i) = inserted.get(id) {
                        match &mut inserts[i].status {
                            Some(s) => s.fold(row),
                            None => inserts[i].status = Some(row),
                        }
                    } else {
                        match updated.entry(*id) {
                            Entry::Occupied(e) => updates[*e.get()].1.fold(row),
                            Entry::Vacant(e) => {
                                e.insert(updates.len());
                                updates.push((*id, row));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        let inner = &self.inner;
        let mut tx = inner.pool.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let row = sqlx::query("SELECT epoch, holder FROM cronoq_leader WHERE id = 1")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let (current, holder): (i64, Option<String>) = (row.get("epoch"), row.get("holder"));
        if current != epoch as i64 || holder.as_deref() != Some(&inner.holder) {
            inner.lost.send_replace(true);
            return Err(StoreError::Fenced);
        }
        // Record the id high-water mark and extend the lease while holding the write lock.
        let max_id = inserts.iter().map(|r| r.id).max().unwrap_or(0);
        sqlx::query(
            "UPDATE cronoq_leader SET max_id = MAX(max_id, ?1), expires_unix_ns = ?2 WHERE id = 1",
        )
        .bind(max_id)
        .bind(now.saturating_add(dur_ns(inner.cfg.lease_ttl)))
        .execute(&mut *tx)
        .await
        .map_err(db)?;

        // 1. Transitions of existing tasks first, so keys they release can be reused by tasks
        //    inserted in this batch.
        let mut missing = 0usize;
        for (id, row) in &updates {
            let r = sqlx::query(
                "UPDATE cronoq_tasks SET status = ?2, terminal = ?3, attempt = ?4, \
                 updated_unix_ns = ?5, message = COALESCE(?6, message) WHERE id = ?1",
            )
            .bind(id.0 as i64)
            .bind(row.status.name())
            .bind(row.status.is_terminal())
            .bind(row.attempt as i64)
            .bind(row.at)
            .bind(row.message.as_deref())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            missing += (r.rows_affected() == 0) as usize;
        }
        if missing > 0 {
            tracing::warn!(
                missing,
                "status updates for tasks missing from the store (pruned?)"
            );
        }
        // 2. New tasks.
        for r in &inserts {
            r.execute(&mut tx).await?;
        }
        // 3. Outputs of tasks that succeeded in this batch.
        let mut missing = 0usize;
        for (id, out) in outputs {
            let r = sqlx::query("UPDATE cronoq_tasks SET output = ?2 WHERE id = ?1")
                .bind(id.0 as i64)
                .bind(out.as_ref())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            missing += (r.rows_affected() == 0) as usize;
        }
        if missing > 0 {
            tracing::warn!(missing, "outputs for tasks missing from the store");
        }
        // 4. Recurring schedules, in order.
        for m in batch {
            match m {
                Mutation::RecurringUpsert { spec } => {
                    sqlx::query(
                        "INSERT INTO cronoq_recurring (name, spec, template_payload) VALUES (?1, ?2, ?3) \
                         ON CONFLICT (name) DO UPDATE SET spec = excluded.spec, \
                         template_payload = excluded.template_payload",
                    )
                    .bind(clean(&spec.name))
                    .bind(recurring_json(base, spec))
                    .bind(&spec.template.payload[..])
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                }
                Mutation::RecurringRemove { name } => {
                    sqlx::query("DELETE FROM cronoq_recurring WHERE name = ?1")
                        .bind(clean(name))
                        .execute(&mut *tx)
                        .await
                        .map_err(db)?;
                }
                Mutation::RecurringNext { name, next } => {
                    sqlx::query("UPDATE cronoq_recurring SET next_unix_ns = ?2 WHERE name = ?1")
                        .bind(clean(name))
                        .bind(next.map(|t| to_unix(base, t)))
                        .execute(&mut *tx)
                        .await
                        .map_err(db)?;
                }
                _ => {}
            }
        }
        tx.commit().await.map_err(db)
    }
}

/// Latest transition of a task within a batch.
struct StatusRow {
    status: TaskStatus,
    attempt: u32,
    at: i64,
    message: Option<String>,
}

impl StatusRow {
    /// Apply a later transition, keeping the message unless the later one has its own.
    fn fold(&mut self, later: StatusRow) {
        let message = later.message.or(self.message.take());
        *self = StatusRow { message, ..later };
    }
}

/// One new task row.
struct InsertRow {
    id: i64,
    seq: i64,
    arrival: i64,
    pool: String,
    kind: String,
    priority: i64,
    key: Option<String>,
    payload: Bytes,
    deadline: Option<i64>,
    fixed: (Option<i64>, Option<i64>, Option<String>),
    hint: Option<i64>,
    timeout: Option<i64>,
    retry: Option<String>,
    deps: String,
    resources: String,
    tags: String,
    created: i64,
    /// The latest transition within the batch; `None` while the task has not moved.
    status: Option<StatusRow>,
}

impl InsertRow {
    fn new(
        base: TimeBase,
        now: i64,
        id: TaskId,
        seq: u64,
        arrival: Time,
        s: &TaskSpec<Bytes>,
    ) -> InsertRow {
        InsertRow {
            id: id.0 as i64,
            seq: seq as i64,
            arrival: to_unix(base, arrival),
            pool: cleaned(&s.pool),
            kind: cleaned(&s.kind),
            priority: s.priority as i64,
            key: s.key.as_deref().map(cleaned),
            payload: (*s.payload).clone(),
            deadline: s.deadline.map(|d| to_unix(base, d)),
            fixed: fixed_cols(base, &s.fixed_start),
            hint: s.duration_hint.map(dur_ns),
            timeout: s.timeout.map(dur_ns),
            retry: retry_json(&s.retry),
            deps: deps_json(&s.deps),
            resources: resources_json(s),
            tags: tags_json(s),
            created: now,
            status: None,
        }
    }

    async fn execute(&self, tx: &mut Transaction<'static, Sqlite>) -> Result<(), StoreError> {
        let (status, terminal, attempt, at, message) = match &self.status {
            Some(st) => (
                st.status.name(),
                st.status.is_terminal(),
                st.attempt as i64,
                st.at,
                st.message.as_deref(),
            ),
            None => (QUEUED, false, 0, self.created, None),
        };
        sqlx::query(
            "INSERT INTO cronoq_tasks (id, seq, arrival_unix_ns, pool, kind, priority, key, payload, \
             deadline_unix_ns, fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, \
             timeout_ns, retry, deps, resources, tags, status, terminal, attempt, updated_unix_ns, message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, \
             ?19, ?20, ?21, ?22, ?23) ON CONFLICT (id) DO NOTHING",
        )
        .bind(self.id)
        .bind(self.seq)
        .bind(self.arrival)
        .bind(&self.pool)
        .bind(&self.kind)
        .bind(self.priority)
        .bind(self.key.as_deref())
        .bind(self.payload.as_ref())
        .bind(self.deadline)
        .bind(self.fixed.0)
        .bind(self.fixed.1)
        .bind(self.fixed.2.as_deref())
        .bind(self.hint)
        .bind(self.timeout)
        .bind(self.retry.as_deref())
        .bind(&self.deps)
        .bind(&self.resources)
        .bind(&self.tags)
        .bind(status)
        .bind(terminal)
        .bind(attempt)
        .bind(at)
        .bind(message)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }
}

/// Decode a live task row.
fn decode_task(base: TimeBase, r: &SqliteRow) -> Result<RecoveredTask<Bytes>, String> {
    let mut spec = TaskSpec::new(Bytes::from(r.get::<Vec<u8>, _>("payload")))
        .kind(r.get::<String, _>("kind"))
        .pool(r.get::<String, _>("pool"))
        .priority(
            Priority::from_index(r.get::<i64, _>("priority").max(0) as usize).unwrap_or_default(),
        );
    spec.key = r.get::<Option<String>, _>("key").map(Into::into);
    spec.deadline = r
        .get::<Option<i64>, _>("deadline_unix_ns")
        .map(|u| from_unix(base, u));
    if let Some(at) = r.get::<Option<i64>, _>("fixed_at_unix_ns") {
        spec.fixed_start = Some(cronoq_core::FixedStart {
            at: from_unix(base, at),
            tolerance: ns_dur(
                r.get::<Option<i64>, _>("fixed_tolerance_ns")
                    .unwrap_or(1_000_000_000),
            ),
            on_miss: miss_from(
                r.get::<Option<String>, _>("fixed_on_miss")
                    .as_deref()
                    .unwrap_or(""),
            ),
        });
    }
    spec.duration_hint = r.get::<Option<i64>, _>("duration_hint_ns").map(ns_dur);
    spec.timeout = r.get::<Option<i64>, _>("timeout_ns").map(ns_dur);
    spec.retry = retry_from(r.get::<Option<String>, _>("retry").as_deref())?;
    spec.deps = deps_from(&r.get::<String, _>("deps"))?;
    spec.resources = resources_from(&r.get::<String, _>("resources"))?
        .into_iter()
        .collect();
    spec.tags = tags_from(&r.get::<String, _>("tags"))?;
    Ok(RecoveredTask {
        id: TaskId(r.get::<i64, _>("id") as u64),
        seq: r.get::<i64, _>("seq") as u64,
        arrival: from_unix(base, r.get("arrival_unix_ns")),
        spec,
        attempt: r.get::<i64, _>("attempt").clamp(0, u32::MAX as i64) as u32,
    })
}

fn terminal_from(base: TimeBase, r: &SqliteRow) -> Option<RecoveredTerminal> {
    let status: String = r.get("status");
    Some(RecoveredTerminal {
        id: TaskId(r.get::<i64, _>("id") as u64),
        key: r.get::<Option<String>, _>("key").map(Into::into),
        status: TaskStatus::from_name(&status)?,
        at: from_unix(base, r.get("updated_unix_ns")),
    })
}

#[async_trait]
impl StoreQueries<Bytes> for SqliteStore {
    async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord>, StoreError> {
        SqliteStore::get_task(self, target).await
    }
    async fn prune(&self, older_than: Duration) -> Result<u64, StoreError> {
        SqliteStore::prune(self, older_than).await
    }
    fn is_leader(&self) -> bool {
        SqliteStore::is_leader(self)
    }
    async fn leadership_lost(&self) {
        SqliteStore::leadership_lost(self).await
    }
}

#[async_trait]
impl Store<Bytes, Bytes> for SqliteStore {
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        let inner = &self.inner;
        let old = inner.leader.lock().unwrap().take();
        if let Some(l) = old {
            l.keeper.abort();
            let _ = l.keeper.await;
        }
        let mut delay = inner.cfg.lock_poll.max(Duration::from_millis(50));
        let epoch = loop {
            match self.try_lead().await {
                Ok(Some(epoch)) => break epoch,
                Ok(None) => tokio::time::sleep(inner.cfg.lock_poll).await,
                Err(e @ StoreError::Unavailable(_)) => {
                    tracing::warn!(error = %e, "cannot reach the database to acquire leadership; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
                Err(e) => return Err(e),
            }
        };
        let expires = now_unix().saturating_add(dur_ns(inner.cfg.lease_ttl));
        let keeper = tokio::spawn(Self::keeper(Arc::downgrade(inner), epoch, expires));
        *inner.leader.lock().unwrap() = Some(Leader { epoch, keeper });
        inner.lost.send_replace(false);
        tracing::info!(epoch, "acquired cronoq leadership");
        Ok(epoch)
    }

    async fn release_leadership(&self) {
        SqliteStore::release_leadership(self).await
    }

    async fn commit(
        &self,
        epoch: u16,
        base: TimeBase,
        batch: &[Mutation<Bytes>],
        outputs: &[(TaskId, &Bytes)],
    ) -> Result<(), StoreError> {
        if *self.inner.lost.borrow() {
            return Err(StoreError::Fenced);
        }
        match tokio::time::timeout(
            self.inner.cfg.commit_timeout,
            self.commit_inner(epoch, base, batch, outputs),
        )
        .await
        {
            Ok(r) => r,
            // Commits are idempotent, so retrying one that did land is harmless.
            Err(_) => Err(StoreError::Unavailable(format!(
                "commit timed out after {:?}",
                self.inner.cfg.commit_timeout
            ))),
        }
    }

    async fn recover(&self, base: TimeBase) -> Result<Recovered<Bytes>, StoreError> {
        let pool = &self.inner.pool;
        let mut rec = Recovered::default();
        let max_id: i64 = sqlx::query_scalar(
            "SELECT MAX((SELECT max_id FROM cronoq_leader WHERE id = 1), \
             (SELECT COALESCE(MAX(id), 0) FROM cronoq_tasks))",
        )
        .fetch_one(pool)
        .await
        .map_err(db)?;
        rec.next_id = if max_id > 0 { max_id as u64 + 1 } else { 0 };

        let rows = sqlx::query(
            "SELECT id, seq, arrival_unix_ns, pool, kind, priority, key, payload, deadline_unix_ns, \
             fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, timeout_ns, \
             retry, deps, resources, tags, attempt FROM cronoq_tasks WHERE terminal = 0 ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .map_err(db)?;
        let mut corrupt: Vec<(i64, Option<String>, String)> = Vec::new();
        for r in &rows {
            match decode_task(base, r) {
                Ok(t) => rec.tasks.push(t),
                Err(e) => {
                    let id: i64 = r.get("id");
                    tracing::error!(task = id, error = %e, "quarantining undecodable task row");
                    corrupt.push((id, r.get("key"), format!("corrupt task row: {e}")));
                }
            }
        }
        drop(rows);
        let now = now_unix();
        for (id, _, message) in &corrupt {
            sqlx::query(
                "UPDATE cronoq_tasks SET status = ?2, terminal = 1, updated_unix_ns = ?3, \
                 message = ?4 WHERE id = ?1",
            )
            .bind(id)
            .bind(TaskStatus::Failed.name())
            .bind(now)
            .bind(cleaned(message))
            .execute(pool)
            .await
            .map_err(db)?;
        }

        // Finished tasks: those within retention, plus any that live tasks depend on.
        let cutoff = now.saturating_sub(dur_ns(self.inner.cfg.retention));
        let mut dep_ids = HashSet::new();
        let mut dep_keys = HashSet::new();
        for t in &rec.tasks {
            for d in &t.spec.deps {
                match &d.target {
                    TaskRef::Id(id) => dep_ids.insert(id.0 as i64),
                    TaskRef::Key(k) => dep_keys.insert(cleaned(k)),
                };
            }
        }
        let ids =
            serde_json::to_string(&dep_ids.into_iter().collect::<Vec<_>>()).expect("serializable");
        let keys =
            serde_json::to_string(&dep_keys.into_iter().collect::<Vec<_>>()).expect("serializable");
        let rows = sqlx::query(
            "SELECT id, key, status, updated_unix_ns FROM cronoq_tasks \
             WHERE terminal = 1 AND (updated_unix_ns >= ?1 \
             OR id IN (SELECT value FROM json_each(?2)) \
             OR key IN (SELECT value FROM json_each(?3))) \
             ORDER BY updated_unix_ns, id",
        )
        .bind(cutoff)
        .bind(ids)
        .bind(keys)
        .fetch_all(pool)
        .await
        .map_err(db)?;
        rec.terminal = rows.iter().filter_map(|r| terminal_from(base, r)).collect();
        // Quarantined rows were just updated, so they come last in this order.
        rec.terminal
            .extend(corrupt.into_iter().map(|(id, key, _)| RecoveredTerminal {
                id: TaskId(id as u64),
                key: key.map(Into::into),
                status: TaskStatus::Failed,
                at: from_unix(base, now),
            }));

        let rows = sqlx::query(
            "SELECT name, spec, template_payload, next_unix_ns FROM cronoq_recurring ORDER BY name",
        )
        .fetch_all(pool)
        .await
        .map_err(db)?;
        for r in rows {
            let name: String = r.get("name");
            let spec: String = r.get("spec");
            match recurring_from(base, name.clone(), &spec, r.get("template_payload")) {
                Ok(spec) => {
                    let next = r
                        .get::<Option<i64>, _>("next_unix_ns")
                        .map(|u| from_unix(base, u));
                    rec.recurring.push((spec, next));
                }
                // Left in place (so it can be repaired) but not scheduled.
                Err(e) => tracing::error!(
                    schedule = %name,
                    error = %e,
                    "skipping undecodable recurring schedule"
                ),
            }
        }
        Ok(rec)
    }

    async fn load_output(&self, id: TaskId) -> Result<Option<Bytes>, StoreError> {
        let out: Option<Option<Vec<u8>>> =
            sqlx::query_scalar("SELECT output FROM cronoq_tasks WHERE id = ?1 AND status = ?2")
                .bind(id.0 as i64)
                .bind(TaskStatus::Succeeded.name())
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(db)?;
        Ok(out.flatten().map(Bytes::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_and_locked_are_transient() {
        assert!(matches!(
            db(sqlx::Error::PoolTimedOut),
            StoreError::Unavailable(_)
        ));
        assert!(matches!(db(sqlx::Error::RowNotFound), StoreError::Other(_)));
    }

    #[test]
    fn holders_differ() {
        assert_ne!(new_holder(), new_holder());
    }
}
