//! # cronoq-store-postgres
//!
//! A PostgreSQL [`Store`] for cronoq with active-passive high availability.
//!
//! - **Write-ahead state**: every engine state transition is committed in batches, one
//!   transaction per batch, before submitters are acknowledged.
//! - **Leader election**: a dedicated session holds `pg_advisory_lock`; acquiring it bumps a
//!   fencing epoch in `cronoq_leader`.
//! - **Fencing**: each commit re-reads the epoch (`FOR SHARE`) in the same transaction and
//!   fails with [`StoreError::Fenced`] if another instance took over.
//! - **Recovery**: a new leader loads live tasks, recently finished ones (for dependencies
//!   and key deduplication) and recurring schedules.
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

mod codec;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cronoq::{Store, StoreError, TimeBase};
use cronoq_core::{
    Mutation, Priority, Recovered, RecoveredTask, RecoveredTerminal, TaskId, TaskRef, TaskSpec,
    TaskStatus,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool, Row};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use codec::*;

const SCHEMA: &str = include_str!("schema.sql");
/// Status stored for tasks that are live but have not started an attempt yet.
const QUEUED: &str = "queued";
/// Fencing epochs stay within 1..=32767 so epoch-prefixed task ids fit in a signed BIGINT.
const MAX_EPOCH: i32 = 32767;

/// Store configuration.
#[derive(Clone, Debug)]
pub struct PgStoreConfig {
    /// Advisory lock key identifying this scheduler cluster.
    pub lock_key: i64,
    /// How long finished tasks are loaded on recovery (dependencies and key dedup).
    pub retention: Duration,
    /// Interval between attempts to take the leader lock.
    pub lock_poll: Duration,
    /// Interval of liveness checks on the leader lock session.
    pub keepalive: Duration,
    pub max_connections: u32,
}

impl Default for PgStoreConfig {
    fn default() -> Self {
        PgStoreConfig {
            lock_key: 0x6372_6f6e_6f71, // "cronoq"
            retention: Duration::from_secs(3600),
            lock_poll: Duration::from_millis(500),
            keepalive: Duration::from_secs(1),
            max_connections: 8,
        }
    }
}

/// Persisted view of a task, for lookups by id or key.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskRecord {
    pub id: TaskId,
    pub key: Option<String>,
    pub pool: String,
    pub kind: String,
    pub priority: Priority,
    /// `None` while queued: the store does not track blocked/scheduled/ready separately.
    pub status: Option<TaskStatus>,
    pub attempt: u32,
    pub message: Option<String>,
    pub output: Option<Bytes>,
    pub updated: SystemTime,
}

struct Leader {
    epoch: u16,
    keeper: JoinHandle<()>,
}

struct Inner {
    pool: PgPool,
    opts: PgConnectOptions,
    cfg: PgStoreConfig,
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

fn db(e: sqlx::Error) -> StoreError {
    StoreError::Other(e.to_string())
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

impl PgStore {
    /// Connect with default settings and apply the schema.
    pub async fn connect(url: &str) -> Result<PgStore, StoreError> {
        Self::connect_with(url, PgStoreConfig::default()).await
    }

    pub async fn connect_with(url: &str, cfg: PgStoreConfig) -> Result<PgStore, StoreError> {
        let opts: PgConnectOptions = url.parse().map_err(db)?;
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections.max(1))
            .connect_with(opts.clone())
            .await
            .map_err(db)?;
        let store = PgStore {
            inner: Arc::new(Inner {
                pool,
                opts,
                cfg,
                leader: Mutex::new(None),
                lost: watch::channel(false).0,
            }),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Apply the schema (idempotent; serialized across instances).
    pub async fn migrate(&self) -> Result<(), StoreError> {
        let mut tx = self.inner.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(self.inner.cfg.lock_key.wrapping_add(1))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::raw_sql(SCHEMA).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &PgStoreConfig {
        &self.inner.cfg
    }

    /// Current fencing epoch, if this instance holds leadership.
    pub fn epoch(&self) -> Option<u16> {
        if *self.inner.lost.borrow() {
            return None;
        }
        self.inner.leader.lock().unwrap().as_ref().map(|l| l.epoch)
    }

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
        const COLS: &str =
            "id, key, pool, kind, priority, status, attempt, message, output, updated_unix_ns";
        let row = match target {
            TaskRef::Id(id) => {
                sqlx::query(&format!("SELECT {COLS} FROM cronoq_tasks WHERE id = $1"))
                    .bind(id.0 as i64)
                    .fetch_optional(&self.inner.pool)
                    .await
                    .map_err(db)?
            }
            TaskRef::Key(k) => sqlx::query(&format!(
                "SELECT {COLS} FROM cronoq_tasks WHERE key = $1 ORDER BY id DESC LIMIT 1"
            ))
            .bind(&**k)
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
            attempt: r.get::<i32, _>("attempt") as u32,
            message: r.get("message"),
            output: r.get::<Option<Vec<u8>>, _>("output").map(Bytes::from),
            updated: UNIX_EPOCH + Duration::from_nanos(updated.max(0) as u64),
        }))
    }

    /// Delete finished tasks last updated more than `older_than` ago. Returns rows removed.
    pub async fn prune(&self, older_than: Duration) -> Result<u64, StoreError> {
        let cutoff = now_unix().saturating_sub(dur_ns(older_than));
        let r = sqlx::query("DELETE FROM cronoq_tasks WHERE terminal AND updated_unix_ns < $1")
            .bind(cutoff)
            .execute(&self.inner.pool)
            .await
            .map_err(db)?;
        Ok(r.rows_affected())
    }

    async fn keeper(store: std::sync::Weak<Inner>, mut conn: PgConnection, every: Duration) {
        loop {
            tokio::time::sleep(every).await;
            let alive = sqlx::query("SELECT 1").execute(&mut conn).await.is_ok();
            let Some(inner) = store.upgrade() else { return };
            if !alive {
                tracing::error!("cronoq leader lock session lost; stepping down");
                inner.lost.send_replace(true);
                return;
            }
        }
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
        let mut conn = PgConnection::connect_with(&inner.opts).await.map_err(db)?;
        loop {
            let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(inner.cfg.lock_key)
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
        let keeper = tokio::spawn(Self::keeper(
            Arc::downgrade(inner),
            conn,
            inner.cfg.keepalive,
        ));
        *inner.leader.lock().unwrap() = Some(Leader {
            epoch: epoch as u16,
            keeper,
        });
        inner.lost.send_replace(false);
        tracing::info!(epoch, "acquired cronoq leadership");
        Ok(epoch as u16)
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
        let mut tx = self.inner.pool.begin().await.map_err(db)?;
        let current: i32 =
            sqlx::query_scalar("SELECT epoch FROM cronoq_leader WHERE id = 1 FOR SHARE")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if current != epoch as i32 {
            self.inner.lost.send_replace(true);
            return Err(StoreError::Fenced);
        }

        // 1. New tasks, in one statement.
        let mut ins = Inserts::default();
        // 2. Latest status per task (later mutations in the batch win).
        let mut latest: Vec<StatusRow> = Vec::new();
        let mut index: HashMap<TaskId, usize> = HashMap::new();
        for m in batch {
            match m {
                Mutation::Submitted {
                    id,
                    seq,
                    arrival,
                    spec,
                } => ins.push(base, *id, *seq, *arrival, spec),
                Mutation::Status {
                    id,
                    status: st,
                    attempt,
                    at,
                    message,
                } => {
                    let row = (*id, *st, *attempt, to_unix(base, *at), message.clone());
                    match index.entry(*id) {
                        Entry::Occupied(e) => latest[*e.get()] = row,
                        Entry::Vacant(e) => {
                            e.insert(latest.len());
                            latest.push(row);
                        }
                    }
                }
                _ => {}
            }
        }
        if !ins.id.is_empty() {
            ins.execute(&mut tx).await?;
        }
        if !latest.is_empty() {
            let mut ids = Vec::with_capacity(latest.len());
            let mut sts = Vec::with_capacity(latest.len());
            let mut terms = Vec::with_capacity(latest.len());
            let mut atts = Vec::with_capacity(latest.len());
            let mut ats = Vec::with_capacity(latest.len());
            let mut msgs: Vec<Option<String>> = Vec::with_capacity(latest.len());
            for (id, st, att, at, msg) in &latest {
                ids.push(id.0 as i64);
                sts.push(st.name().to_string());
                terms.push(st.is_terminal());
                atts.push(*att as i32);
                ats.push(*at);
                msgs.push(msg.as_ref().map(|m| m.to_string()));
            }
            sqlx::query(
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
        }
        if !outputs.is_empty() {
            let ids: Vec<i64> = outputs.iter().map(|(id, _)| id.0 as i64).collect();
            let outs: Vec<Vec<u8>> = outputs.iter().map(|(_, o)| o.to_vec()).collect();
            sqlx::query(
                "UPDATE cronoq_tasks t SET output = u.output \
                 FROM UNNEST($1::bigint[], $2::bytea[]) AS u(id, output) WHERE t.id = u.id",
            )
            .bind(ids)
            .bind(outs)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        // 3. Recurring schedules: rare, applied in order.
        for m in batch {
            match m {
                Mutation::RecurringUpsert { spec } => {
                    sqlx::query(
                        "INSERT INTO cronoq_recurring (name, spec, template_payload) VALUES ($1, $2::jsonb, $3) \
                         ON CONFLICT (name) DO UPDATE SET spec = EXCLUDED.spec, template_payload = EXCLUDED.template_payload",
                    )
                    .bind(&*spec.name)
                    .bind(recurring_json(base, spec))
                    .bind(spec.template.payload.to_vec())
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                }
                Mutation::RecurringRemove { name } => {
                    sqlx::query("DELETE FROM cronoq_recurring WHERE name = $1")
                        .bind(&**name)
                        .execute(&mut *tx)
                        .await
                        .map_err(db)?;
                }
                Mutation::RecurringNext { name, next } => {
                    sqlx::query("UPDATE cronoq_recurring SET next_unix_ns = $2 WHERE name = $1")
                        .bind(&**name)
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

    async fn recover(&self, base: TimeBase) -> Result<Recovered<Bytes>, StoreError> {
        let pool = &self.inner.pool;
        let mut rec = Recovered::default();
        let max_id: Option<i64> = sqlx::query_scalar("SELECT MAX(id) FROM cronoq_tasks")
            .fetch_one(pool)
            .await
            .map_err(db)?;
        rec.next_id = max_id.map(|m| m as u64 + 1).unwrap_or(0);

        let rows = sqlx::query(
            "SELECT id, seq, arrival_unix_ns, pool, kind, priority, key, payload, deadline_unix_ns, \
             fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, timeout_ns, retry, deps, \
             resources, tags, attempt FROM cronoq_tasks WHERE NOT terminal ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .map_err(db)?;
        for r in rows {
            let bad = |e: String| StoreError::Other(format!("corrupt task row: {e}"));
            let mut spec = TaskSpec::new(Bytes::from(r.get::<Vec<u8>, _>("payload")))
                .kind(r.get::<String, _>("kind"))
                .pool(r.get::<String, _>("pool"))
                .priority(
                    Priority::from_index(r.get::<i16, _>("priority") as usize).unwrap_or_default(),
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
            spec.retry = retry_from(r.get("retry")).map_err(bad)?;
            spec.deps = deps_from(r.get("deps")).map_err(bad)?;
            spec.resources = resources_from(r.get("resources"))
                .map_err(bad)?
                .into_iter()
                .collect();
            spec.tags = tags_from(r.get("tags")).map_err(bad)?;
            rec.tasks.push(RecoveredTask {
                id: TaskId(r.get::<i64, _>("id") as u64),
                seq: r.get::<i64, _>("seq") as u64,
                arrival: from_unix(base, r.get("arrival_unix_ns")),
                spec,
                attempt: r.get::<i32, _>("attempt").max(0) as u32,
            });
        }

        let cutoff = now_unix().saturating_sub(dur_ns(self.inner.cfg.retention));
        let rows = sqlx::query(
            "SELECT id, key, status, updated_unix_ns FROM cronoq_tasks \
             WHERE terminal AND updated_unix_ns >= $1 ORDER BY updated_unix_ns",
        )
        .bind(cutoff)
        .fetch_all(pool)
        .await
        .map_err(db)?;
        for r in rows {
            let status: String = r.get("status");
            let Some(status) = TaskStatus::from_name(&status) else {
                continue;
            };
            rec.terminal.push(RecoveredTerminal {
                id: TaskId(r.get::<i64, _>("id") as u64),
                key: r.get::<Option<String>, _>("key").map(Into::into),
                status,
                at: from_unix(base, r.get("updated_unix_ns")),
            });
        }

        let rows = sqlx::query(
            "SELECT name, spec, template_payload, next_unix_ns FROM cronoq_recurring ORDER BY name",
        )
        .fetch_all(pool)
        .await
        .map_err(db)?;
        for r in rows {
            let spec = recurring_from(
                base,
                r.get("name"),
                r.get("spec"),
                r.get("template_payload"),
            )
            .map_err(|e| StoreError::Other(format!("corrupt recurring row: {e}")))?;
            let next = r
                .get::<Option<i64>, _>("next_unix_ns")
                .map(|u| from_unix(base, u));
            rec.recurring.push((spec, next));
        }
        Ok(rec)
    }
}

/// Latest status of a task within a batch: (id, status, attempt, at, message).
type StatusRow = (TaskId, TaskStatus, u32, i64, Option<Arc<str>>);

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
}

impl Inserts {
    fn push(
        &mut self,
        base: TimeBase,
        id: TaskId,
        seq: u64,
        arrival: cronoq_core::Time,
        s: &TaskSpec<Bytes>,
    ) {
        let (fa, ft, fm) = fixed_cols(base, &s.fixed_start);
        self.id.push(id.0 as i64);
        self.seq.push(seq as i64);
        self.arrival.push(to_unix(base, arrival));
        self.pool.push(s.pool.to_string());
        self.kind.push(s.kind.to_string());
        self.priority.push(s.priority as i16);
        self.key.push(s.key.as_ref().map(|k| k.to_string()));
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
    }

    async fn execute(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), StoreError> {
        let now = now_unix();
        sqlx::query(
            "INSERT INTO cronoq_tasks (id, seq, arrival_unix_ns, pool, kind, priority, key, payload, \
             deadline_unix_ns, fixed_at_unix_ns, fixed_tolerance_ns, fixed_on_miss, duration_hint_ns, timeout_ns, \
             retry, deps, resources, tags, status, updated_unix_ns) \
             SELECT u.id, u.seq, u.arrival, u.pool, u.kind, u.priority, u.key, u.payload, u.deadline, u.fixed_at, \
             u.fixed_tol, u.fixed_miss, u.hint, u.timeout, u.retry::jsonb, u.deps::jsonb, u.resources::jsonb, \
             u.tags::jsonb, $19, $20 \
             FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::smallint[], \
             $7::text[], $8::bytea[], $9::bigint[], $10::bigint[], $11::bigint[], $12::text[], $13::bigint[], \
             $14::bigint[], $15::text[], $16::text[], $17::text[], $18::text[]) \
             AS u(id, seq, arrival, pool, kind, priority, key, payload, deadline, fixed_at, fixed_tol, fixed_miss, \
             hint, timeout, retry, deps, resources, tags) \
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
        .bind(QUEUED)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }
}
