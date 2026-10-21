# Errors and status codes

This page maps every error across the Rust API, the Python API and the server's gRPC and HTTP
APIs, and says what a caller should do about it.

## Submission errors

| `SubmitError` | Python `SubmitError.code` | gRPC | HTTP | Cause | What to do |
|---|---|---|---|---|---|
| `UnknownPool(name)` | `unknown_pool` | `NOT_FOUND` | 404 | No pool with that name. | Fix the pool name. |
| `UnknownResource(name)` | `unknown_resource` | `NOT_FOUND` | 404 | No resource with that name. | Fix the resource name. |
| `ResourceExceedsCapacity(name)` | `resource_exceeds_capacity` | `INVALID_ARGUMENT` | 400 | Requested units exceed the resource's permits or burst, so it could never run. | Request fewer units. |
| `ReservationConflict` | `reservation_conflict` | `FAILED_PRECONDITION` | 409 | Fixed-start reservations would exceed the pool's reservable capacity at that time. | Pick another start time, raise `reservable_fraction` or `max`. |
| `Cycle` | `cycle` | `INVALID_ARGUMENT` | 400 | The dependencies form a cycle. | Fix the graph. |
| `UnknownDependency(ref)` | `unknown_dependency` | `NOT_FOUND` | 404 | Dependency on an unknown id, or an unknown key when the pool rejects unknown dependencies. | Submit the parent first, or set `await_unknown_dependencies_s`. |
| `Full` | `full` | `RESOURCE_EXHAUSTED` | 429 | Queue bounds reached with the `wait` overflow policy. In-process callers wait for space instead of seeing this. | Retry with backoff. |
| `Overloaded` | `overloaded` | `RESOURCE_EXHAUSTED` | 429 | Bounds reached with `reject`, or shedding found nothing lower to evict. | Retry with backoff, or shed load upstream. |
| `DuplicateKeyInBatch(key)` | `duplicate_key_in_batch` | `INVALID_ARGUMENT` | 400 | A batch defines the same new key twice. | Deduplicate the batch. |
| `Invalid(reason)` | `invalid` | `INVALID_ARGUMENT` | 400 | Malformed spec: zero resource units, `max_attempts = 0`, NUL in a string, ... | Fix the request. |

The server also returns `INVALID_ARGUMENT` / 400 for requests that fail its own checks
(`[limits]`, unknown JSON fields, both relative and absolute times) and `PERMISSION_DENIED` /
403 for pools outside a token's scope.

## Task outcomes

Awaiting a task (`TaskHandle` in Rust, `await handle` in Python, `Wait` or
`GET .../result` on the server) ends with one of:

| Status | Rust | Python | Meaning |
|---|---|---|---|
| `succeeded` | `Ok(Arc<O>)` | the return value | The executor returned an output. |
| `succeeded` (output gone) | `Err(TaskFailure::OutputUnavailable { id })` | `TaskFailed(status="succeeded")` | Succeeded, but the output is no longer cached or stored (for example a duplicate submission long after the original finished). Server: `output_available = false`. |
| `failed` | `Err(Ended { status: Failed, .. })` | `TaskFailed(status="failed")` | `Permanent` error, or a fixed start missed with `on_miss = fail`. |
| `dead_lettered` | `Ended { status: DeadLettered, .. }` | `"dead_lettered"` | Retryable failures used up every attempt. |
| `cancelled` | `Ended { status: Cancelled, .. }` | `"cancelled"` | Cancelled by a caller. |
| `skipped` | `Ended { status: Skipped, .. }` | `"skipped"` | A dependency edge can no longer be met, an unknown dependency was awaited too long, or a recurring occurrence overlapped the previous one. |
| `missed` | `Ended { status: Missed, .. }` | `"missed"` | Fixed start missed with `on_miss = skip`. |
| `shed` | `Ended { status: Shed, .. }` | `"shed"` | Evicted by the `shed_lowest` / `shed_oldest` overflow policy. |
| – | `Err(TaskFailure::Shutdown)` | `"shutdown"` | The scheduler stopped before the task finished. Server: `UNAVAILABLE` / 503, wait again on the new leader. |

`message` carries the last error message (or the status name).

## Executor errors

What an executor returns decides whether the attempt is retried:

| Rust `TaskError` | Python | Remote worker (`Fail`) | Effect |
|---|---|---|---|
| `Retryable { message, retry_after }` | `raise Retryable(msg, retry_after=s)`, or any other exception | `permanent = false`, `retry_after_s` | Retried per the retry policy, not before `retry_after`. |
| `Permanent { message }` | `raise Permanent(msg)` | `permanent = true` | Fails the task now (`failed`). |
| `Cancelled` | `CancelledError` caused by cronoq | `Cancelled` | The attempt was aborted; the engine already decided what happens next. |
| panic in a Rust executor | – | panic in a `run_worker` handler: retryable | In-process: permanent failure (`Failed`). The scheduler keeps running. |

Timeouts (`timeout`) and lease expiries (no heartbeat within `lease_ttl`) are retryable
failures too.

## Scheduler errors

| Rust `Error` | Python | Meaning |
|---|---|---|
| `Submit(SubmitError)` | `SubmitError` | See above. |
| `Shutdown` | `SchedulerError` | The scheduler has stopped or is stopping. |
| `Config(String)` | `ValueError` (at construction) or `SchedulerError` | Invalid configuration: no pools, missing executor, duplicate pool, `min > max`, bad cron or time zone, recurring template with one-shot fields. |
| `Store(StoreError)` | `SchedulerError` | Persistence failed fatally (see below). |
| `Internal(String)` | `SchedulerError` | Internal failure, for example the scheduler task panicked. |

Python raises `RuntimeError` when a scheduler is used before `start()`, and `SchedulerError`
when its event loop was closed without a shutdown.

### Store errors

| `StoreError` | Driver behaviour |
|---|---|
| `Unavailable(String)` | Transient (connection lost, timeout, database restarting). Retried with capped backoff for up to `max_store_outage` (default 5 min) while the engine keeps running; replies and dispatches that need durability wait. After that the scheduler stops with `Error::Store`. |
| `Other(String)` | Retried a few times (5), then the scheduler stops. |
| `Fenced` | Another instance took leadership. The scheduler stops immediately and writes nothing more. |

The server reports store failures as `UNAVAILABLE` / 503 and logs the details; it does not
return database messages to clients.

## Client errors (Python `cronoq.client`)

| Exception | When |
|---|---|
| `ClientError` | A gRPC call failed. `.code` is the lower-case status name (`unauthenticated`, `permission_denied`, `not_found`, `resource_exhausted`, `failed_precondition`, `invalid_argument`, `unavailable`, `data_loss`, ...). |
| `TimeoutError` | `Client.wait(timeout=...)` or `submit(..., call_timeout=...)` ran out of time. |

## Server status codes

| gRPC | HTTP | Retry? | Cause |
|---|---|---|---|
| `INVALID_ARGUMENT` | 400 | no | Malformed request or spec. |
| `UNAUTHENTICATED` | 401 | no | Missing or unknown credentials. |
| `PERMISSION_DENIED` | 403 | no | Role or pool scope does not allow it. |
| `NOT_FOUND` | 404 | no | Unknown task, pool, resource or dependency. |
| `FAILED_PRECONDITION` | 409 | after changing the request | Fixed-start reservation conflict. |
| – | 413, 415 | no | Body too large, wrong content type. |
| `RESOURCE_EXHAUSTED` | 429 | yes, with backoff | Queue bounds reached. |
| `INTERNAL` | 500 | maybe | Unexpected server error. |
| `UNAVAILABLE` | 503 | yes, with backoff | Not the leader, shutting down, store unavailable. |
| `DEADLINE_EXCEEDED` | 504 | wait again | A wait timed out while the task is still running. |
| `DATA_LOSS` | – | reconnect | A `Watch` stream fell behind. |

Submissions with a `key` are idempotent, so retrying a submit after `UNAVAILABLE` or a lost
connection never creates a second task: the retry returns the original id with
`duplicate = true`.
