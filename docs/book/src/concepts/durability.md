# Durability and high availability

By default the scheduler keeps its state in memory: fast, but a process restart loses queued
work. The `Store` trait makes state durable, and `cronoq-store-postgres` implements it on
PostgreSQL, including active-passive high availability.

## Model

The engine is the system of record while it runs. The store is a **write-ahead log** of the
engine's state transitions:

- task submissions (full spec and payload), status changes (running, retrying, terminal, with
  attempt number and message), outputs of successful tasks;
- recurring schedules and their next occurrence.

Persisted mutations are **group-committed**: everything produced in one engine step is written
in one transaction. A submission is acknowledged only after its commit succeeds, so an
acknowledged task survives a crash.

## Recovery

When a scheduler starts with a durable store it:

1. becomes leader (see below) and obtains a new **fencing epoch**;
2. loads non-terminal tasks, terminal tasks within the store's retention window (for
   dependencies and key deduplication), and recurring schedules;
3. re-inserts tasks with their original ids, submission order and attempt counts.

Tasks that were **running** when the previous leader died count that attempt as used: they are
retried after a backoff, or dead-lettered if no attempts remain. Fixed-start tasks whose start
has already passed apply their miss policy. This is what makes execution **at-least-once**.

## Leader election and fencing (Postgres)

Several `cronoq-server` replicas can point at the same database:

- Each replica tries to take a Postgres **advisory lock** on a dedicated connection. The holder
  is the leader; it increments the epoch in `cronoq_leader` and serves. The others wait and
  report `GET /readyz` = 503.
- Every commit checks the epoch in the same transaction. A leader that lost its lock (for
  example after a network partition) can no longer write: its commit fails with `Fenced` and it
  stops.
- The leader's lock session is checked every second; on loss the server stops serving
  immediately.
- When the leader stops, a standby acquires the lock, recovers, and starts serving. Task ids
  carry the epoch in their upper bits, so ids from different leaders never collide.

Clients and workers reach the leader through a load balancer that uses `/readyz`, and workers
reconnect automatically (both official clients do).

```text
            ┌─────────────┐   advisory lock   ┌──────────────┐
 clients ──▶│ replica A    │◀────────────────▶│              │
 workers ──▶│ (leader)     │   commits+epoch   │  PostgreSQL  │
            └─────────────┘                    │              │
            ┌─────────────┐   waiting for lock │              │
            │ replica B    │◀────────────────▶│              │
            │ (standby,503)│                   └──────────────┘
            └─────────────┘
```

## Guarantees and limits

| Property | Guarantee |
|---|---|
| Acknowledged submissions | Durable. |
| Execution | At least once; a task may run again if its worker or the leader fails mid-attempt. |
| Order after failover | Submission order and priorities are preserved. |
| Split brain | Prevented by fencing: only the current epoch can commit. |
| Outputs | Persisted for successful tasks; `Wait` returns them after a failover. |

Retention: finished rows are pruned after `keep_finished_s` (7 days by default). Recovery loads
finished tasks from the last hour for dependency resolution and key deduplication.

## Custom stores

Implement `cronoq::Store<P, O>` (`acquire_leadership`, `commit`, `recover`) to persist to
another database. Return `StoreError::Fenced` from `commit` when the caller's epoch is stale.
