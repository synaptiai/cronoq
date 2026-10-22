//! # cronoq-store-postgres
//!
//! A PostgreSQL [`Store`] for cronoq with active-passive high availability.
//!
//! - **Write-ahead state**: every engine state transition is committed in batches, one
//!   transaction per batch, before submitters are acknowledged.
//! - **Isolation**: each scheduler cluster lives in its own PostgreSQL schema
//!   ([`PgStoreConfig::schema`]); clusters in different schemas share nothing, not even locks.
//! - **Leader election**: a dedicated session holds an advisory lock derived from the schema;
//!   acquiring it bumps a fencing epoch in `cronoq_leader`.
//! - **Fencing**: each commit re-reads the epoch in the same transaction and fails with
//!   [`StoreError::Fenced`] if another instance took over.
//! - **Recovery**: a new leader loads live tasks, recently finished ones (for key
//!   deduplication), every finished task a live task still depends on, and recurring
//!   schedules. Rows that cannot be decoded are quarantined (marked failed) instead of
//!   blocking recovery.
//! - **Errors**: connection loss, timeouts, serialization failures and server shutdowns are
//!   reported as [`StoreError::Unavailable`], which the scheduler retries.
//!
//! Times are stored as wall-clock Unix nanoseconds. PostgreSQL cannot store NUL characters in
//! text, so NUL in strings (messages, keys, tags, ...) is stored as U+FFFD.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use bytes::Bytes;
//! use cronoq::{Pool, Scheduler};
//! use cronoq_store_postgres::PgStore;
//!
//! let store = PgStore::connect("postgres://localhost/cronoq").await?;
//! let sched = Scheduler::<Bytes, Bytes>::builder()
//!     .pool(Pool::new("default").executor_fn(|_ctx, p: std::sync::Arc<Bytes>| async move {
//!         Ok::<_, cronoq::TaskError>((*p).clone())
//!     }))
//!     .store(store.clone())
//!     .build() // blocks until this instance is the leader
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cronoq::{Store, StoreError, StoreQueries, TimeBase};
use cronoq_core::{
    Mutation, Priority, Recovered, RecoveredRecurring, RecoveredTask, RecoveredTerminal, TaskId,
    TaskRef, TaskSpec, TaskStatus,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Row};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use cronoq::codec::*;

/// Ordered schema migrations; `cronoq_schema_version` records how many were applied. Each one
/// is idempotent so databases created before versioning upgrade cleanly.
const MIGRATIONS: &[&str] = &[
    include_str!("schema.sql"),
    // v2: high-water mark of issued task ids, so pruning can never cause id reuse.
    "ALTER TABLE cronoq_leader ADD COLUMN IF NOT EXISTS max_id BIGINT NOT NULL DEFAULT 0",
    // v3: the occurrence a queued recurring schedule chains to.
    "ALTER TABLE cronoq_recurring ADD COLUMN IF NOT EXISTS last_task_id BIGINT",
    // v4: worker labels a task requires.
    "ALTER TABLE cronoq_tasks ADD COLUMN IF NOT EXISTS labels JSONB NOT NULL DEFAULT '[]'::jsonb",
    // v5: tenant, for fair sharing.
    "ALTER TABLE cronoq_tasks ADD COLUMN IF NOT EXISTS tenant TEXT",
];
/// Status stored for tasks that are live but have not started an attempt yet.
const QUEUED: &str = "queued";
/// Fencing epochs stay within 1..=32767 so epoch-prefixed task ids fit in a signed BIGINT.
const MAX_EPOCH: i32 = 32767;
/// First key of the two-key advisory locks taken by this crate ("croq" / "crom").
const LOCK_NS_LEADER: i32 = 0x6372_6f71;
const LOCK_NS_MIGRATE: i32 = 0x6372_6f6d;
/// Rows deleted per statement by [`PgStore::prune`].
const PRUNE_BATCH: i64 = 1000;
/// Longest wait between attempts to reach the database while acquiring leadership.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Store configuration.
#[derive(Clone, Debug)]
pub struct PgStoreConfig {
    /// PostgreSQL schema holding this cluster's tables (created if missing). Instances
    /// configured with the same schema form one active-passive cluster; different schemas
    /// are fully independent. Letters, digits and `_` only.
    pub schema: String,
    /// How long finished tasks are loaded on recovery, for key deduplication. Finished tasks
    /// that live tasks depend on are always loaded.
    pub retention: Duration,
    /// Interval between attempts to take the leader lock.
    pub lock_poll: Duration,
    /// Interval of liveness checks on the leader lock session.
    pub keepalive: Duration,
    /// How long the leader lock session may stay unresponsive before this instance steps
    /// down. Also used for the server-side TCP keepalive of that session.
    pub session_timeout: Duration,
    /// Upper bound on one commit; a slower commit fails as [`StoreError::Unavailable`] (and
    /// is retried by the scheduler).
    pub commit_timeout: Duration,
    /// Size of the connection pool used for commits and queries.
    pub max_connections: u32,
}

impl Default for PgStoreConfig {
    fn default() -> Self {
        PgStoreConfig {
            schema: "public".into(),
            retention: Duration::from_secs(3600),
            lock_poll: Duration::from_millis(500),
            keepalive: Duration::from_secs(1),
            session_timeout: Duration::from_secs(10),
            commit_timeout: Duration::from_secs(30),
            max_connections: 8,
        }
    }
}

/// Persisted view of a task, for lookups by id or key.
pub type TaskRecord = cronoq::TaskRecord<Bytes>;

struct Leader {
    epoch: u16,
    keeper: JoinHandle<()>,
}

struct Inner {
    pool: PgPool,
    opts: PgConnectOptions,
    cfg: PgStoreConfig,
    /// Second key of the leader advisory lock: the schema's OID.
    lock_id: i32,
    leader: Mutex<Option<Leader>>,
    /// `true` once leadership was lost (or before it was ever acquired: `false`).
    lost: watch::Sender<bool>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(l) = self.leader.get_mut().unwrap().take() {
            l.keeper.abort();
        }
    }
}

/// PostgreSQL-backed [`Store`]. Cheap to clone.
#[derive(Clone)]
pub struct PgStore {
    inner: Arc<Inner>,
}

/// Map a database error, classifying failures that are worth retrying as `Unavailable`.
fn db(e: sqlx::Error) -> StoreError {
    use sqlx::Error as E;
    let transient = match &e {
        E::Io(_) | E::Tls(_) | E::PoolTimedOut | E::WorkerCrashed => true,
        E::Database(d) => d.code().is_some_and(|c| {
            // 08: connection exception, 53: insufficient resources, 57P: shutdown/crash,
            // 40001/40P01: serialization failure/deadlock, 55P03: lock not available.
            c.starts_with("08")
                || c.starts_with("53")
                || c.starts_with("57P")
                || matches!(&*c, "40001" | "40P01" | "55P03")
        }),
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
/// function bodies or string literals containing semicolons.
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

fn valid_schema(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && !s.starts_with(|c: char| c.is_ascii_digit())
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl PgStore {
    /// Connect with default settings and apply the schema.
    pub async fn connect(url: &str) -> Result<PgStore, StoreError> {
        Self::connect_with(url, PgStoreConfig::default()).await
    }

    /// Connect with `cfg` and apply the schema.
    pub async fn connect_with(url: &str, cfg: PgStoreConfig) -> Result<PgStore, StoreError> {
        if !valid_schema(&cfg.schema) {
            return Err(StoreError::Other(format!(
                "invalid schema name {:?}: use letters, digits and '_'",
                cfg.schema
            )));
        }
        let opts: PgConnectOptions = url.parse().map_err(db)?;
        let opts = opts.options([("search_path", format!("\"{}\"", cfg.schema))]);
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections.max(1))
            .connect_with(opts.clone())
            .await
            .map_err(db)?;
        Self::migrate_pool(&pool, &cfg.schema).await?;
        let oid: i64 =
            sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname = $1")
                .bind(&cfg.schema)
                .fetch_one(&pool)
                .await
                .map_err(db)?;
        Ok(PgStore {
            inner: Arc::new(Inner {
                pool,
                opts,
                cfg,
                lock_id: oid as u32 as i32,
                leader: Mutex::new(None),
                lost: watch::channel(false).0,
            }),
        })
    }

    /// Apply pending schema migrations (idempotent; serialized across instances that use
    /// the same schema). [`PgStore::connect`] already does this.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        Self::migrate_pool(&self.inner.pool, &self.inner.cfg.schema).await
    }

    async fn migrate_pool(pool: &PgPool, schema: &str) -> Result<(), StoreError> {
        let mut tx = pool.begin().await.map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(LOCK_NS_MIGRATE)
            .bind(schema)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
                .bind(schema)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if !exists {
            // `schema` passed `valid_schema` when the store was configured: ASCII letters,
            // digits and underscores only.
            sqlx::query(AssertSqlSafe(format!(
                "CREATE SCHEMA IF NOT EXISTS \"{schema}\""
            )))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        // Tables that already exist are used as they are, and their owner controls their
        // triggers, which run as this role. Refuse ones another role could have planted: only
        // this role or a superuser may own them (`pg_has_role` would accept anything for a
        // superuser connection, the case where a planted trigger gains the most).
        let foreign: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_roles o ON o.oid = c.relowner \
             WHERE n.nspname = $1 AND c.relname LIKE 'cronoq\\_%' \
             AND o.rolname <> current_user AND NOT o.rolsuper \
             ORDER BY 1",
        )
        .bind(schema)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        if !foreign.is_empty() {
            return Err(StoreError::Other(format!(
                "schema \"{schema}\" holds cronoq tables owned by another role ({}); refusing to \
                 use them (they must belong to the connecting role or a superuser)",
                foreign.join(", ")
            )));
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cronoq_schema_version \
             (id SMALLINT PRIMARY KEY CHECK (id = 1), version INT NOT NULL)",
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
        let version: i32 =
            sqlx::query_scalar("SELECT version FROM cronoq_schema_version WHERE id = 1")
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
        // One statement at a time: `raw_sql` on a connection yields a future that callers cannot
        // prove `Send` (sqlx higher-ranked lifetime issue), which breaks `tokio::spawn`.
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
            sqlx::query("UPDATE cronoq_schema_version SET version = $1 WHERE id = 1")
                .bind(MIGRATIONS.len() as i32)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// The connection pool (its sessions use the configured schema).
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    /// The configuration this store was opened with.
    pub fn config(&self) -> &PgStoreConfig {
        &self.inner.cfg
    }

    /// The two keys of the advisory lock held by the leader (`pg_locks.classid`, `objid`),
    /// for monitoring.
    pub fn leader_lock(&self) -> (i32, i32) {
        (LOCK_NS_LEADER, self.inner.lock_id)
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

    /// Resolves once leadership held by this instance is lost (lock session gone).
    pub async fn leadership_lost(&self) {
        let mut rx = self.inner.lost.subscribe();
        let _ = rx.wait_for(|lost| *lost).await;
    }

    /// Give up leadership (releases the advisory lock).
    pub async fn release_leadership(&self) {
        let l = self.inner.leader.lock().unwrap().take();
        if let Some(l) = l {
            l.keeper.abort();
            let _ = l.keeper.await;
        }
        self.inner.lost.send_replace(true);
    }

    /// Look up a task by id or key (the most recent task with that key).
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
            TaskRef::Id(id) => sqlx::query(select!("WHERE id = $1"))
                .bind(id.0 as i64)
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(db)?,
            TaskRef::Key(k) => sqlx::query(select!("WHERE key = $1 ORDER BY id DESC LIMIT 1"))
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
            priority: Priority::from_index(r.get::<i16, _>("priority") as usize)
                .unwrap_or_default(),
            status: TaskStatus::from_name(&status),
            attempt: r.get::<i32, _>("attempt").max(0) as u32,
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
                   SELECT (d->>'id')::bigint AS id, d->>'key' AS key \
                   FROM cronoq_tasks l, jsonb_array_elements(l.deps) d \
                   WHERE NOT l.terminal AND l.deps <> '[]'::jsonb), \
                 victims AS ( \
                   SELECT t.id FROM cronoq_tasks t \
                   WHERE t.terminal AND t.updated_unix_ns < $1 \
                   AND t.id NOT IN (SELECT id FROM refs WHERE id IS NOT NULL) \
                   AND (t.key IS NULL OR t.key NOT IN (SELECT key FROM refs WHERE key IS NOT NULL)) \
                   ORDER BY t.updated_unix_ns LIMIT $2) \
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

    async fn keeper(
        store: std::sync::Weak<Inner>,
        mut conn: PgConnection,
        every: Duration,
        limit: Duration,
    ) {
        loop {
            tokio::time::sleep(every).await;
            let alive = matches!(
                tokio::time::timeout(limit, sqlx::query("SELECT 1").execute(&mut conn)).await,
                Ok(Ok(_))
            );
            let Some(inner) = store.upgrade() else { return };
            if !alive {
                tracing::error!("cronoq leader lock session lost; stepping down");
                inner.lost.send_replace(true);
                // Dropping the connection closes the session, releasing the lock if the
                // server still holds it.
                return;
            }
        }
    }

    /// Open the lock session and wait for the lock; returns the session and the new epoch.
    async fn try_lead(&self) -> Result<(PgConnection, i32), StoreError> {
        let inner = &self.inner;
        let idle = inner.cfg.session_timeout.as_secs().max(1).to_string();
        let opts = inner.opts.clone().options([
            ("tcp_keepalives_idle", idle.as_str()),
            ("tcp_keepalives_interval", "1"),
            ("tcp_keepalives_count", "3"),
        ]);
        let mut conn = PgConnection::connect_with(&opts).await.map_err(db)?;
        loop {
            let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
                .bind(LOCK_NS_LEADER)
                .bind(inner.lock_id)
                .fetch_one(&mut conn)
                .await
                .map_err(db)?;
            if got {
                break;
            }
            tokio::time::sleep(inner.cfg.lock_poll).await;
        }
        let epoch: i32 = sqlx::query_scalar(
            "UPDATE cronoq_leader SET epoch = CASE WHEN epoch >= $1 THEN 1 ELSE epoch + 1 END \
             WHERE id = 1 RETURNING epoch",
        )
        .bind(MAX_EPOCH)
        .fetch_one(&mut conn)
        .await
        .map_err(db)?;
        Ok((conn, epoch))
    }

    async fn release(&self) {
        PgStore::release_leadership(self).await
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
        let mut ins = Inserts::default();
        let mut inserted: HashMap<TaskId, usize> = HashMap::new();
        let mut updates: Vec<(TaskId, StatusRow)> = Vec::new();
        let mut updated: HashMap<TaskId, usize> = HashMap::new();
        let now = now_unix();
        for m in batch {
            match m {
                Mutation::Submitted {
                    id,
                    seq,
                    arrival,
                    spec,
                } => {
                    if let Entry::Vacant(e) = inserted.entry(*id) {
                        e.insert(ins.id.len());
                        ins.push(base, now, *id, *seq, *arrival, spec);
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
                        ins.set_status(i, row);
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

        let mut tx = self.inner.pool.begin().await.map_err(db)?;
        let current: i32 = match ins.id.iter().max() {
            // Record the id high-water mark while checking the epoch.
            Some(max) => sqlx::query_scalar(
                "UPDATE cronoq_leader SET max_id = GREATEST(max_id, $1) WHERE id = 1 \
                 RETURNING epoch",
            )
            .bind(*max)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?,
            None => sqlx::query_scalar("SELECT epoch FROM cronoq_leader WHERE id = 1 FOR SHARE")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?,
        };
        if current != epoch as i32 {
            self.inner.lost.send_replace(true);
            return Err(StoreError::Fenced);
        }

        // 1. Transitions of existing tasks first, so keys they release can be reused by tasks
        //    inserted in this batch.
        if !updates.is_empty() {
            let n = updates.len();
            let mut ids = Vec::with_capacity(n);
            let mut sts = Vec::with_capacity(n);
            let mut terms = Vec::with_capacity(n);
            let mut atts = Vec::with_capacity(n);
            let mut ats = Vec::with_capacity(n);
            let mut msgs: Vec<Option<String>> = Vec::with_capacity(n);
            for (id, row) in updates {
                ids.push(id.0 as i64);
                sts.push(row.status.name());
                terms.push(row.status.is_terminal());
                atts.push(row.attempt as i32);
                ats.push(row.at);
                msgs.push(row.message);
            }
            let r = sqlx::query(
                "UPDATE cronoq_tasks t SET status = u.status, terminal = u.terminal, attempt = u.attempt, \
                 updated_unix_ns = u.at, message = COALESCE(u.message, t.message) \
                 FROM UNNEST($1::bigint[], $2::text[], $3::bool[], $4::int[], $5::bigint[], $6::text[]) \
                 AS u(id, status, terminal, attempt, at, message) WHERE t.id = u.id",
            )
            .bind(ids)
            .bind(sts)
            .bind(terms)
            .bind(atts)
            .bind(ats)
            .bind(msgs)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            if r.rows_affected() != n as u64 {
                tracing::warn!(
                    expected = n,
                    updated = r.rows_affected(),
                    "status updates for tasks missing from the store (pruned?)"
                );
            }
        }
        // 2. New tasks, in one statement.
        if !ins.id.is_empty() {
            ins.execute(&mut tx).await?;
        }
        // 3. Outputs of tasks that succeeded in this batch.
        if !outputs.is_empty() {
            let ids: Vec<i64> = outputs.iter().map(|(id, _)| id.0 as i64).collect();
            let outs: Vec<Vec<u8>> = outputs.iter().map(|(_, o)| o.to_vec()).collect();
            let r = sqlx::query(
                "UPDATE cronoq_tasks t SET output = u.output \
                 FROM UNNEST($1::bigint[], $2::bytea[]) AS u(id, output) WHERE t.id = u.id",
            )
            .bind(ids)
            .bind(outs)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            if r.rows_affected() != outputs.len() as u64 {
                tracing::warn!(
                    expected = outputs.len(),
                    updated = r.rows_affected(),
                    "outputs for tasks missing from the store"
                );
            }
        }
        // 4. Recurring schedules: rare, applied in order.
        for m in batch {
            match m {
                Mutation::RecurringUpsert { spec } => {
                    sqlx::query(
                        "INSERT INTO cronoq_recurring (name, spec, template_payload) VALUES ($1, $2::jsonb, $3) \
                         ON CONFLICT (name) DO UPDATE SET spec = EXCLUDED.spec, template_payload = EXCLUDED.template_payload",
                    )
                    .bind(clean(&spec.name))
                    .bind(recurring_json(base, spec))
                    .bind(spec.template.payload.to_vec())
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                }
                Mutation::RecurringRemove { name } => {
                    sqlx::query("DELETE FROM cronoq_recurring WHERE name = $1")
                        .bind(clean(name))
                        .execute(&mut *tx)
                        .await
                        .map_err(db)?;
                }
                Mutation::RecurringNext { name, next, last } => {
                    sqlx::query(
                        "UPDATE cronoq_recurring SET next_unix_ns = $2, last_task_id = $3 \
                         WHERE name = $1",
                    )
                    .bind(clean(name))
                    .bind(next.map(|t| to_unix(base, t)))
                    .bind(last.map(|id| id.0 as i64))
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

/// Decode a live task row.
fn decode_task(base: TimeBase, r: &sqlx::postgres::PgRow) -> Result<RecoveredTask<Bytes>, String> {
    let mut spec = TaskSpec::new(Bytes::from(r.get::<Vec<u8>, _>("payload")))
        .kind(r.get::<String, _>("kind"))
        .pool(r.get::<String, _>("pool"))
        .priority(Priority::from_index(r.get::<i16, _>("priority") as usize).unwrap_or_default());
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
    spec.labels = tags_from(&r.get::<String, _>("labels"))?;
    spec.tenant = r.get::<Option<String>, _>("tenant").map(Into::into);
    Ok(RecoveredTask {
        id: TaskId(r.get::<i64, _>("id") as u64),
        seq: r.get::<i64, _>("seq") as u64,
        arrival: from_unix(base, r.get("arrival_unix_ns")),
        spec,
        attempt: r.get::<i32, _>("attempt").max(0) as u32,
    })
}

fn terminal_from(base: TimeBase, r: &sqlx::postgres::PgRow) -> Option<RecoveredTerminal> {
    let status: String = r.get("status");
    Some(RecoveredTerminal {
        id: TaskId(r.get::<i64, _>("id") as u64),
        key: r.get::<Option<String>, _>("key").map(Into::into),
        status: TaskStatus::from_name(&status)?,
        at: from_unix(base, r.get("updated_unix_ns")),
    })
}

#[async_trait]
impl StoreQueries<Bytes> for PgStore {
    async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord>, StoreError> {
        PgStore::get_task(self, target).await
    }
    async fn prune(&self, older_than: Duration) -> Result<u64, StoreError> {
        PgStore::prune(self, older_than).await
    }
    fn is_leader(&self) -> bool {
        PgStore::is_leader(self)
    }
    async fn leadership_lost(&self) {
        PgStore::leadership_lost(self).await
    }
}

#[async_trait]
impl Store<Bytes, Bytes> for PgStore {
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        let inner = &self.inner;
        let old = inner.leader.lock().unwrap().take();
        if let Some(l) = old {
            l.keeper.abort();
            let _ = l.keeper.await;
        }
        let mut delay = inner.cfg.lock_poll.max(Duration::from_millis(50));
        let (conn, epoch) = loop {
            match self.try_lead().await {
                Ok(v) => break v,
                Err(e @ StoreError::Unavailable(_)) => {
                    // Dropping the session (if any) releases a lock taken before the error.
                    tracing::warn!(error = %e, "cannot reach the database to acquire leadership; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
                Err(e) => return Err(e),
            }
        };
        let keeper = tokio::spawn(Self::keeper(
            Arc::downgrade(inner),
            conn,
            inner.cfg.keepalive,
            inner.cfg.session_timeout,
        ));
        *inner.leader.lock().unwrap() = Some(Leader {
            epoch: epoch as u16,
            keeper,
        });
        inner.lost.send_replace(false);
        tracing::info!(epoch, "acquired cronoq leadership");
        Ok(epoch as u16)
    }

    async fn release_leadership(&self) {
        self.release().await
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
            "SELECT GREATEST((SELECT max_id FROM cronoq_leader WHERE id = 1), \
             (SELECT COALESCE(MAX(id), 0) FROM cronoq_tasks))",
        )
        .fetch_one(pool)
        .await
        .map_err(db)?;
        rec.next_id = if max_id > 0 { max_id as u64 + 1 } else { 0 };

        let rows = sqlx::query(
            "SELECT id, seq, arrival_unix_ns, pool, kind, priority, key, payload, deadline_unix_ns, \
             fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, timeout_ns, \
             retry::text AS retry, deps::text AS deps, resources::text AS resources, \
             tags::text AS tags, labels::text AS labels, tenant, attempt FROM cronoq_tasks \
             WHERE NOT terminal \
             ORDER BY id",
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
        if !corrupt.is_empty() {
            let ids: Vec<i64> = corrupt.iter().map(|c| c.0).collect();
            let msgs: Vec<String> = corrupt.iter().map(|c| cleaned(&c.2)).collect();
            sqlx::query(
                "UPDATE cronoq_tasks t SET status = $3, terminal = TRUE, updated_unix_ns = $4, \
                 message = u.message FROM UNNEST($1::bigint[], $2::text[]) AS u(id, message) \
                 WHERE t.id = u.id",
            )
            .bind(ids)
            .bind(msgs)
            .bind(TaskStatus::Failed.name())
            .bind(now)
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
        let rows = sqlx::query(
            "SELECT id, key, status, updated_unix_ns FROM cronoq_tasks \
             WHERE terminal AND (updated_unix_ns >= $1 OR id = ANY($2) OR key = ANY($3)) \
             ORDER BY updated_unix_ns, id",
        )
        .bind(cutoff)
        .bind(dep_ids.into_iter().collect::<Vec<_>>())
        .bind(dep_keys.into_iter().collect::<Vec<_>>())
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
            "SELECT name, spec::text AS spec, template_payload, next_unix_ns, last_task_id \
             FROM cronoq_recurring ORDER BY name",
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
                    let last = r
                        .get::<Option<i64>, _>("last_task_id")
                        .map(|id| TaskId(id as u64));
                    rec.recurring.push(RecoveredRecurring { spec, next, last });
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
            sqlx::query_scalar("SELECT output FROM cronoq_tasks WHERE id = $1 AND status = $2")
                .bind(id.0 as i64)
                .bind(TaskStatus::Succeeded.name())
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(db)?;
        Ok(out.flatten().map(Bytes::from))
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

/// Column-wise buffers for a multi-row `INSERT ... SELECT FROM UNNEST`.
#[derive(Default)]
struct Inserts {
    id: Vec<i64>,
    seq: Vec<i64>,
    arrival: Vec<i64>,
    pool: Vec<String>,
    kind: Vec<String>,
    priority: Vec<i16>,
    key: Vec<Option<String>>,
    payload: Vec<Vec<u8>>,
    deadline: Vec<Option<i64>>,
    fixed_at: Vec<Option<i64>>,
    fixed_tol: Vec<Option<i64>>,
    fixed_miss: Vec<Option<String>>,
    hint: Vec<Option<i64>>,
    timeout: Vec<Option<i64>>,
    retry: Vec<Option<String>>,
    deps: Vec<String>,
    resources: Vec<String>,
    tags: Vec<String>,
    labels: Vec<String>,
    tenant: Vec<Option<String>>,
    status: Vec<&'static str>,
    terminal: Vec<bool>,
    attempt: Vec<i32>,
    updated: Vec<i64>,
    message: Vec<Option<String>>,
}

impl Inserts {
    fn push(
        &mut self,
        base: TimeBase,
        now: i64,
        id: TaskId,
        seq: u64,
        arrival: cronoq_core::Time,
        s: &TaskSpec<Bytes>,
    ) {
        let (fa, ft, fm) = fixed_cols(base, &s.fixed_start);
        self.id.push(id.0 as i64);
        self.seq.push(seq as i64);
        self.arrival.push(to_unix(base, arrival));
        self.pool.push(cleaned(&s.pool));
        self.kind.push(cleaned(&s.kind));
        self.priority.push(s.priority as i16);
        self.key.push(s.key.as_deref().map(cleaned));
        self.payload.push(s.payload.to_vec());
        self.deadline.push(s.deadline.map(|d| to_unix(base, d)));
        self.fixed_at.push(fa);
        self.fixed_tol.push(ft);
        self.fixed_miss.push(fm);
        self.hint.push(s.duration_hint.map(dur_ns));
        self.timeout.push(s.timeout.map(dur_ns));
        self.retry.push(retry_json(&s.retry));
        self.deps.push(deps_json(&s.deps));
        self.resources.push(resources_json(s));
        self.tags.push(tags_json(s));
        self.labels.push(labels_json(s));
        self.tenant.push(s.tenant.as_deref().map(cleaned));
        self.status.push(QUEUED);
        self.terminal.push(false);
        self.attempt.push(0);
        self.updated.push(now);
        self.message.push(None);
    }

    /// Apply a transition to the task at row `i`.
    fn set_status(&mut self, i: usize, row: StatusRow) {
        self.status[i] = row.status.name();
        self.terminal[i] = row.status.is_terminal();
        self.attempt[i] = row.attempt as i32;
        self.updated[i] = row.at;
        if row.message.is_some() {
            self.message[i] = row.message;
        }
    }

    async fn execute(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO cronoq_tasks (id, seq, arrival_unix_ns, pool, kind, priority, key, payload, \
             deadline_unix_ns, fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, timeout_ns, \
             retry, deps, resources, tags, status, terminal, attempt, updated_unix_ns, message, labels, tenant) \
             SELECT u.id, u.seq, u.arrival, u.pool, u.kind, u.priority, u.key, u.payload, u.deadline, u.fixed_at, \
             u.fixed_tol, u.fixed_miss, u.hint, u.timeout, u.retry::jsonb, u.deps::jsonb, u.resources::jsonb, \
             u.tags::jsonb, u.status, u.terminal, u.attempt, u.updated, u.message, u.labels::jsonb, u.tenant \
             FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::smallint[], \
             $7::text[], $8::bytea[], $9::bigint[], $10::bigint[], $11::bigint[], $12::text[], $13::bigint[], \
             $14::bigint[], $15::text[], $16::text[], $17::text[], $18::text[], $19::text[], $20::bool[], \
             $21::int[], $22::bigint[], $23::text[], $24::text[], $25::text[]) \
             AS u(id, seq, arrival, pool, kind, priority, key, payload, deadline, fixed_at, fixed_tol, fixed_miss, \
             hint, timeout, retry, deps, resources, tags, status, terminal, attempt, updated, message, labels, \
             tenant) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(self.id)
        .bind(self.seq)
        .bind(self.arrival)
        .bind(self.pool)
        .bind(self.kind)
        .bind(self.priority)
        .bind(self.key)
        .bind(self.payload)
        .bind(self.deadline)
        .bind(self.fixed_at)
        .bind(self.fixed_tol)
        .bind(self.fixed_miss)
        .bind(self.hint)
        .bind(self.timeout)
        .bind(self.retry)
        .bind(self.deps)
        .bind(self.resources)
        .bind(self.tags)
        .bind(self.status)
        .bind(self.terminal)
        .bind(self.attempt)
        .bind(self.updated)
        .bind(self.message)
        .bind(self.labels)
        .bind(self.tenant)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_failures_are_transient() {
        let io = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(db(io).is_transient());
        assert!(db(sqlx::Error::PoolTimedOut).is_transient());
        assert!(!db(sqlx::Error::RowNotFound).is_transient());
    }

    #[test]
    fn schema_names_are_validated() {
        assert!(valid_schema("cronoq_prod2"));
        for bad in ["", "1abc", "a-b", "a\"b", &"x".repeat(64)] {
            assert!(!valid_schema(bad), "{bad}");
        }
    }
}
