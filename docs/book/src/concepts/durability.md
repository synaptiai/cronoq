# Durability and high availability

By default the scheduler keeps its state in memory: fast, but a process restart loses queued
work. The `Store` trait makes state durable. Two stores implement it, both with active-passive
high availability:

| Store | Use it for |
|---|---|
| `cronoq-store-postgres` | Replicas on several hosts sharing a PostgreSQL database. |
| `cronoq-store-sqlite` | One host, no database server: state in a local file, with standby processes on the same host. |

Both pass the same conformance suite, so the behaviour below holds for either.

## Model

The engine is the system of record while it runs. The store is a **write-ahead log** of the
engine's state transitions:

- task submissions (full spec and payload), status changes (running, retrying, terminal, with
  attempt number and message), outputs of successful tasks;
- cancellations;
- recurring schedules, their next occurrence and the last task each one created.

Persisted mutations are **group-committed**: everything produced in one engine step is written
in one transaction, so durability costs one round trip per step, not per task.

### Durable acknowledgements

With a persistent store, a call returns only after its effect is committed:

| Operation | Returns after |
|---|---|
| `submit`, `enqueue`, `submit_batch` (gRPC `Submit`, HTTP `POST /v1/tasks`) | The task is stored. |
| `cancel` | The cancellation is stored. |
| `add_recurring`, `remove_recurring` | The schedule change is stored. |

The engine also dispatches an attempt only after the mutations that led to it are durable, so a
worker never runs a task that a crash could "unsubmit".

### Time

Engine time is Unix time in nanoseconds, so persisted timestamps (fixed starts, deadlines,
retry times, next cron occurrences) mean the same thing to every replica and survive restarts
without translation.

## Recovery

When a scheduler starts with a durable store it:

1. becomes leader (see below) and obtains a new **fencing epoch**;
2. loads non-terminal tasks, terminal tasks within the store's retention window (for
   dependencies and key deduplication), terminal tasks that live tasks depend on, and recurring
   schedules;
3. re-inserts tasks with their original ids, submission order and attempt counts.

Tasks that were **running** when the previous leader died count that attempt as used: they are
retried after a backoff, or dead-lettered if no attempts remain. Fixed-start tasks whose start
has already passed apply their miss policy. A dependency on a parent that finished before the
retention window counts as satisfied. Recurring schedules keep their overlap guard: an
occurrence still running from before the failover is not overlapped by the next one. This is
what makes execution **at-least-once**.

## Store failures

The driver distinguishes three kinds of store errors:

| Error | Behaviour |
|---|---|
| `Unavailable` (connection lost, timeout, database restarting) | Retried with capped backoff for up to `max_store_outage` (default 5 minutes). The engine keeps running in memory meanwhile; acknowledgements and dispatches that need durability wait. If the outage lasts longer, the scheduler stops with `Error::Store`. |
| `Other` | Retried 5 times, then the scheduler stops. |
| `Fenced` | Another instance is leader: the scheduler stops immediately. |

A commit that takes longer than `commit_timeout` (default 30 s) counts as `Unavailable`. Callers
whose operation could not be made durable get `Error::Store` (Python `SchedulerError`, server
`UNAVAILABLE` / 503); retry them, ideally with a key so the retry is idempotent.

## Leader election and fencing (Postgres)

Several `cronoq-server` replicas (or embedding processes) can point at the same database:

- Each replica tries to take a Postgres **advisory lock** on a dedicated session, every
  `lock_poll` (0.5 s). The holder is the leader; it increments the epoch and serves. The others
  wait and report `GET /readyz` = 503.
- Every commit checks the epoch in the same transaction. A leader that lost its lock (for
  example after a network partition) can no longer write: its commit fails with `Fenced` and it
  stops.
- The leader checks its lock session every `keepalive` (1 s) and steps down if the session is
  unresponsive for `session_timeout` (10 s); the same timeout configures TCP keepalives on the
  database side, so Postgres releases the lock of a vanished leader promptly.
- When the leader stops, a standby acquires the lock, recovers and starts serving. Task ids
  carry the epoch in their upper bits, and the store keeps a high-water mark of issued ids, so
  ids are never reused, even after old rows are pruned.

Clients and workers reach the leader either through a load balancer that routes on `/readyz`
(only the leader is ready), or through one that sends connections to every replica. In the
second case a standby answers each call with `UNAVAILABLE`, and closes every connection about a
second after accepting it (and a leader that steps down closes its connections the same way),
so a client that retries opens a new connection and the balancer picks a replica again. Workers
reconnect on their own. During a graceful shutdown connections stay open, so running attempts
can still report. The [Helm chart](../guides/deployment.md#kubernetes) routes to
every replica, so that rolling updates can proceed.

## Leader election and fencing (SQLite)

Several processes on one host can open the same database file:

- Leadership is a **lease** row in the file: the holder's identity and an expiry time. A
  process that finds the lease free or expired takes it, increments the epoch and serves. The
  others check every `lock_poll` (0.5 s) and report `GET /readyz` = 503.
- The leader renews the lease every `lease_renew` (2 s); it lasts `lease_ttl` (10 s). If the
  leader dies or hangs, a standby takes over once the lease runs out. A leader that shuts down
  cleanly releases the lease, so the standby takes over at once.
- SQLite lets one writer at a time into the file. Every commit checks the epoch and lease
  holder inside its write transaction, so a leader that was paused past its lease and replaced
  can no longer write: its commit fails with `Fenced` and it stops. Its renewal also notices
  the takeover.
- Lease expiry uses the wall clock, which processes on one host share. Keep the file on a
  local filesystem: SQLite's WAL journal does not work over network filesystems, which is also
  why this store does not span hosts.
- The file is created readable and writable by its owner only (on Unix), and SQLite gives its
  journal files the same permissions. By default every commit syncs the journal to disk
  (`full_sync`).

```text
            ┌──────────────┐   advisory lock    ┌──────────────┐
 clients ──▶│ replica A    │◀──────────────────▶│              │
 workers ──▶│ (leader)     │   commits + epoch  │  PostgreSQL  │
            └──────────────┘                    │              │
            ┌──────────────┐  waiting for lock  │              │
            │ replica B    │◀──────────────────▶│              │
            │ (standby,503)│                    └──────────────┘
            └──────────────┘
```

## Schema and migrations

- PostgreSQL tables live in one schema, `public` by default (`PgStoreConfig::schema` when you
  embed the store, `store.schema` in the server). Instances configured with the same schema form
  one cluster; different schemas are fully independent, so one database can host several
  clusters. With SQLite, each file is one cluster.
- Migrations are versioned in `cronoq_schema_version` and applied automatically on connect,
  under a lock, by whichever instance starts first. An older build refuses to run against a
  newer schema instead of corrupting it.
- PostgreSQL text cannot hold NUL characters. The APIs reject them in keys, kinds, tags and
  names; NUL in messages reported by executors or workers is stored as U+FFFD (in both stores,
  so they behave alike).
- A row that cannot be decoded during recovery (for example after manual edits) is
  quarantined: marked failed with an explanatory message, instead of preventing the leader from
  starting.

## Guarantees and limits

| Property | Guarantee |
|---|---|
| Acknowledged submissions, cancellations, schedule changes | Durable. |
| Execution | At least once; a task may run again if its worker or the leader fails mid-attempt. |
| Order after failover | Submission order and priorities are preserved. |
| Split brain | Prevented by fencing: only the current epoch can commit. |
| Outputs | Persisted for successful tasks; `Wait` returns them after a failover. |
| Ids | Unique across leaders and restarts. |

Retention: the server prunes finished rows after `keep_finished_s` (7 days by default). Recovery
loads finished tasks from the last `retention` (1 hour by default) for dependency resolution and
key deduplication.

## Custom stores

Implement `cronoq::Store<P, O>` (`acquire_leadership`, `commit`, `recover`, optionally
`release_leadership` and `load_output`) to persist to another database. Return
`StoreError::Unavailable` for transient failures and `StoreError::Fenced` from `commit` when the
caller's epoch is stale. Apply the `TimeBase` you are given when converting timestamps; the
runtime always passes Unix-epoch time (`unix_nanos_at_zero = 0`).
