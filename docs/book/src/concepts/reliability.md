# Retries, timeouts and leases

cronoq executes tasks **at least once**: an attempt that may have been lost is retried. Make
handlers idempotent, or make their side effects exactly-once with
[fencing tokens](#exactly-once-side-effects), and deduplicate submissions with
[keys](dependencies.md#keys).

## Attempts and outcomes

Each dispatch starts an **attempt** identified by a **lease**. An attempt ends with:

| Outcome | Rust | Python | Next |
|---|---|---|---|
| Success | `Ok(output)` | `return value` | `Succeeded` |
| Retryable failure | `TaskError::retryable(msg)` | `raise cronoq.Retryable(msg)` or any other exception | retry, or `DeadLettered` when attempts are exhausted |
| Retryable with delay | `TaskError::retry_after(msg, d)` | `raise cronoq.Retryable(msg, retry_after=s)` | retry no earlier than `d` |
| Permanent failure | `TaskError::permanent(msg)` | `raise cronoq.Permanent(msg)` | `Failed` |
| Cancelled | `TaskError::Cancelled` | (task cancelled) | `Cancelled` |
| Panic (Rust executor) | | | permanent failure (`Failed`); the scheduler and other attempts keep running |

## Exactly-once side effects

No scheduler can stop a handler from running twice: a worker can lose its connection after
charging a card but before reporting success. What cronoq guarantees is enough to make the
*effect* happen once:

- **Attempt numbers never repeat for a task**, also across failovers: an attempt is dispatched
  only after it is durably recorded, and a new leader continues from the recorded number. So
  `(task id, attempt)` is a fencing token: a later attempt always has a higher number.
- **Stale reports change nothing.** A completion or failure for an attempt that timed out, was
  aborted, was already reported or came from a previous leader is ignored, and counted in
  `stale_completions` (metric `cronoq_stale_completions_total`). Lease ids carry the leader
  epoch, so they never repeat across leaders either.
- **Resubmitting a finished keyed task returns it.** Within the key retention window
  (`limits.key_retention_s`, default 1 hour; `CoreConfig::retention` in the library), a
  submission with the key of a finished task is a duplicate: it returns that task and its
  stored output instead of running again.

Record the token with the side effect, in the same transaction, and let the write succeed only
for the newest attempt:

```sql
INSERT INTO charges (task_id, attempt, amount) VALUES ($1, $2, $3)
ON CONFLICT (task_id) DO UPDATE SET attempt = excluded.attempt, amount = excluded.amount
WHERE charges.attempt < excluded.attempt;   -- an older attempt arriving late changes nothing
```

For effects that must happen once at all (not "last attempt wins"), insert on `task_id` alone
and skip when the row exists. The token is `ctx.id` and `ctx.attempt` in Rust and Python
executors, and `Lease.task_id` and `Lease.attempt` for remote workers.

## Retry policy

```rust,ignore
RetryPolicy {
    max_attempts: 3,                          // total, including the first
    initial_backoff: Duration::from_secs(1),
    multiplier: 2.0,
    max_backoff: Duration::from_secs(60),
    jitter: true,                             // full jitter
}
```

The delay before attempt `n` (n ≥ 2) is `min(initial × multiplier^(n−2), max_backoff)`; with
jitter, a uniformly random fraction of it. A `retry_after` hint raises the delay to at least
that value. The policy is set per pool and can be overridden per task; `RetryPolicy::none()` (or
`Task::no_retry()`) disables retries.

A retried task keeps its original submission time, so it keeps its place in its priority class.

## Timeouts

`timeout` limits each attempt. When it expires the engine aborts the attempt (the executor's
future is cancelled, or the remote worker receives `Abort`) and records a retryable failure.

## Leases and heartbeats

Pools with a **lease TTL** (always the case for server pools; `lease_ttl_s`, default 30 s)
require the executor or worker to heartbeat each lease:

- Rust: `ctx.heartbeat()`; Python bindings: `ctx.heartbeat()`; Python worker client and Rust
  worker client: automatic, every TTL / 3.
- If no heartbeat arrives within the TTL, the lease expires, the attempt is aborted and treated
  as a retryable failure. This catches crashed workers and network partitions.

In-process pools have no TTL by default because the engine knows when the attempt's future
ends.

## Cancellation

`cancel(id or key)` ends a task immediately with status `Cancelled`:

- a queued, blocked or retrying task is removed;
- a running attempt is aborted (the cancellation token fires and the future is dropped; remote
  workers get `Abort`);
- dependents whose edge requires success are `Skipped`.

## Shutdown and dropped schedulers

`shutdown(grace)` stops admitting work, lets running attempts finish for `grace`, then aborts the
rest; queued tasks are not started (with a durable store they stay queued for the next leader).
If every `Scheduler` handle is dropped without `shutdown`, the runtime shuts down by itself with
`drop_grace` (default 5 s). Handles awaiting unfinished tasks then resolve with
`TaskFailure::Shutdown` (Python `TaskFailed(status="shutdown")`).

## Outputs

A `TaskHandle` resolves with the executor's output. Handles attached after the task finished
(`handle(key)`, a duplicate submission) get the output from a bounded cache of recent outputs
(`output_cache`, 1024 by default) or from the store. If neither still holds it, the handle
resolves with `TaskFailure::OutputUnavailable` (Python: `TaskFailed` with status `succeeded`;
server: `output_available = false`): the task did succeed, but its result is gone.

## Dead letters

Tasks that end as `DeadLettered` or `Failed` keep their final message, available through
status events, `Wait` results and (with Postgres) the database. Monitor
`cronoq_tasks_total{status="dead_lettered"}` to catch systematic failures.

## Recovery after a crash

With a durable store, a task that was running when the leader died counts that attempt as
used. If attempts remain, it is retried after a backoff; otherwise it is dead-lettered. See
[Durability and HA](durability.md).
