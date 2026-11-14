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
| Released | `TaskError::release(d)` | `raise cronoq.Release(s)` | offered again after `d`, in its place; [no retry is used](#releasing-an-attempt) |
| Panic (Rust executor) | | | permanent failure (`Failed`); the scheduler and other attempts keep running |

## Releasing an attempt

Sometimes an executor cannot run a task, and nothing is wrong with the task: the database it
writes to is down for a minute, a rate limit of its own is used up, the worker was told to
shut down. Failing the attempt would use up one of the task's retries for a problem that is
not the task's.

A **release** gives the task back without a verdict:

```rust
if !downstream.is_up() {
    return Err(TaskError::release(Duration::from_secs(30)));
}
```

```python
if not downstream.is_up():
    raise cronoq.Release(30)
```

```go
return nil, cronoq.Release(30 * time.Second)
```

```ts
throw new Release(30);
```

| | A released attempt |
|---|---|
| The task | Keeps its place in line (and in its [ordering key](ordering-keys.md)) and is offered again, to any worker, once the delay has passed. Until then its status is `retrying`. |
| The attempt number | Goes up: every attempt has its own number, so logs and [fencing](#exactly-once-side-effects) stay unambiguous. |
| The retry policy | Is not charged. A task with `max_attempts = 3` that was released twice may still fail three times. |
| Resources it held | [Concurrency limits](resources.md) are free at once. A rate-limit token the attempt took is spent: the next attempt takes a new one. |
| [Fair pools](fairness.md) | It keeps its turn among the tenants, and its tenant is not charged a second time. |
| Failure counts | Not a failure: it is counted in the `releases` total (`cronoq_releases_total`), not in `failed` or `retries`. |

A task that is released again and again would never end, so releases are limited:
`max_releases` per pool (10 unless set), or per task. A release beyond the limit is treated as
a retryable failure, with the delay asked for as its minimum backoff, and from then on the
retry policy applies. Releases made while a [pause](pausing.md) holds the task do not count
toward the limit: a worker draining a paused pool does not use it up. (A task with a
[fixed start](fixed-starts.md) is not held by a pause, so its releases always count.)

Over the network, a task may not ask for a `max_releases` above the server's
`limits.max_task_releases` (1000 unless set). A delay that is negative or not a number is no
delay; one that is too long is cut to the longest the server accepts (100 years). A worker
that releases without a delay gets the task back at once, so give a delay unless the worker
is going away.

Three ways to give a task back, and what each costs:

| | Use it when | The attempt |
|---|---|---|
| Release | The executor cannot run the task now. | Ends; a new one starts after the delay. No retry is used, up to `max_releases`. |
| Retryable failure with a delay | The task failed and may work later. | Ends; uses one retry. |
| `Nack` / `Reject` (remote workers) | This worker should not have got the lease (no free slot, shutting down). | Does not end: the server hands the same attempt to another worker at once. If no other worker can take it, it fails as retryable and uses one retry. |

Releases are for remote workers and in-process executors alike. A remote worker needs a
server that knows them: the worker libraries check (`Registered.release_supported`) and, with
an older server, send a retryable failure with the delay instead, which uses a retry.

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
- dependents attached with an on-success or on-failure edge are `Skipped`; for dependents attached
  with an on-complete edge, that edge is satisfied.

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
status events, `Wait` results and (with a SQLite or Postgres store) the store. Monitor
`cronoq_tasks_total{status="dead_lettered"}` to catch systematic failures.

## Recovery after a crash

With a durable store, a task that was running when the leader died counts that attempt as
used. If attempts remain, it is retried after a backoff; otherwise it is dead-lettered. See
[Durability and HA](durability.md).

Released attempts are stored with the task and still do not count after a failover. Two
things are worth knowing:

- A release that the store had not yet recorded when the leader died is the case above: the
  attempt was running as far as the store knows, so it counts as used.
- The delay of a release is not stored. A task that was waiting out a release when the
  leader died is offered again after its retry backoff, which may be sooner or later than
  the delay that was asked for.
- How many of a task's releases count toward `max_releases` is kept in memory only. After a
  failover the limit starts again, so a task may be released up to `max_releases` more
  times.
