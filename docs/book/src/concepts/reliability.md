# Retries, timeouts and leases

cronoq executes tasks **at least once**: an attempt that may have been lost is retried. Make
handlers idempotent, or deduplicate with [keys](dependencies.md#keys).

## Attempts and outcomes

Each dispatch starts an **attempt** identified by a **lease**. An attempt ends with:

| Outcome | Rust | Python | Next |
|---|---|---|---|
| Success | `Ok(output)` | `return value` | `Succeeded` |
| Retryable failure | `TaskError::retryable(msg)` | `raise cronoq.Retryable(msg)` or any other exception | retry, or `DeadLettered` when attempts are exhausted |
| Retryable with delay | `TaskError::retry_after(msg, d)` | `raise cronoq.Retryable(msg, retry_after=s)` | retry no earlier than `d` |
| Permanent failure | `TaskError::permanent(msg)` | `raise cronoq.Permanent(msg)` | `Failed` |
| Cancelled | `TaskError::Cancelled` | (task cancelled) | `Cancelled` |
| Panic (Rust) | | | treated as permanent failure |

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

## Dead letters

Tasks that end as `DeadLettered` or `Failed` keep their final message, available through
status events, `Wait` results and (with Postgres) the database. Monitor
`cronoq_tasks_total{status="dead_lettered"}` to catch systematic failures.

## Recovery after a crash

With a durable store, a task that was running when the leader died counts that attempt as
used. If attempts remain, it is retried after a backoff; otherwise it is dead-lettered. See
[Durability and HA](durability.md).
