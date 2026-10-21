# Tasks and their lifecycle

A **task** is one unit of work: a payload plus the rules for scheduling it. A **pool** is a set
of worker slots that runs tasks. An **executor** (in-process) or a **remote worker** (server
mode) runs one *attempt* of a task at a time; a task may need several attempts.

## Task fields

| Field | Default | Meaning |
|---|---|---|
| `payload` | required | The work. A Rust value, a Python object, or bytes on the wire. |
| `pool` | `"default"` | Which pool runs it. |
| `kind` | `"default"` | A label for metrics and for learning typical durations. |
| `priority` | `Normal` | `Critical`, `High`, `Normal` or `Low`. See [Priorities](priorities.md). |
| `key` | none | Unique name: deduplicates submissions and can be referenced by dependents. |
| `deadline` | none | Soft "done by" time. See [Deadlines](deadlines.md). |
| `fixed_start` | none | Start time, tolerance and miss policy. See [Fixed starts](fixed-starts.md). |
| `duration_hint` | learned | Expected run time; used for deadlines and reservations. |
| `timeout` | none | Maximum duration of each attempt. |
| `retry` | pool default | Retry policy. See [Reliability](reliability.md). |
| `deps` | none | Dependencies with edge conditions. See [Dependencies](dependencies.md). |
| `resources` | none | Units of named resources needed while running. See [Resources](resources.md). |
| `tags` | none | Free-form key/value labels passed to the executor (e.g. `traceparent`). |

Every task gets a numeric **id** assigned by the engine. Ids increase monotonically; in server
mode the top bits carry the leader's fencing epoch, so ids stay unique across failovers.

## Lifecycle

```text
                 ┌── dependencies unmet ──┐
  submit ──▶  Blocked                      │
     │           │ all edges satisfied      │
     ├──────▶ Scheduled ── start time ──┐   │
     │        (fixed start, in future)   ▼   ▼
     └────────────────────────────▶  Ready ◀──────── backoff elapsed ──── Retrying
                                       │ dispatched                        ▲
                                       ▼                                   │ retryable failure,
                                    Running ──────────────────────────────┘ timeout, lease lost
                                       │
      ┌──────────────┬────────────────┼──────────────┬───────────────────────┐
  Succeeded        Failed       DeadLettered     Cancelled        Skipped · Missed · Shed
                 (permanent)   (retries spent)  (by request)   (edge unmet · start missed · overload)
```

| Status | Terminal | Meaning |
|---|---|---|
| `Blocked` | | Waiting for dependencies. |
| `Scheduled` | | Waiting for its fixed start time. |
| `Ready` | | Eligible and queued (or waiting for a resource). |
| `Running` | | An attempt is executing. |
| `Retrying` | | Waiting for a retry backoff to elapse. |
| `Succeeded` | ✔ | The last attempt returned an output. |
| `Failed` | ✔ | Permanent failure, or a missed fixed start with `MissPolicy::Fail`. |
| `DeadLettered` | ✔ | Retryable failures used up every attempt. |
| `Cancelled` | ✔ | Cancelled by a client. |
| `Skipped` | ✔ | A dependency edge can no longer be satisfied, or an awaited key never appeared. |
| `Missed` | ✔ | Fixed start missed with `MissPolicy::Skip`. |
| `Shed` | ✔ | Evicted to make room under overload. |

Terminal statuses are retained for the **retention window** (1 hour, up to 1,000,000 entries by
default). During that window the task can still be looked up, keys stay reserved, and new tasks
may depend on it.

## Results

- **Rust**: `submit` returns a `TaskHandle<O>`; awaiting it yields `Ok(Arc<O>)` on success or
  `Err(TaskFailure)` with the terminal status and message.
- **Python**: awaiting a `TaskHandle` returns the executor's return value or raises
  `cronoq.TaskFailed`.
- **Server**: `Wait` / `GET /v1/tasks/{id}/result` return the output bytes (and JSON, when the
  bytes are JSON). Outputs are kept in a bounded in-memory cache and, with Postgres, in the
  database.

Handles are not required: `enqueue` submits without one, which is cheaper.

## Events

Every terminal transition is published as a `StatusEvent { id, pool, key, status, attempt, at,
message }`. With `emit_transitions` enabled, every intermediate transition is published as well.
Subscribe with `Scheduler::events()` in Rust, `sched.events()` in Python, or `Tasks/Watch` over
gRPC (with `all_transitions` for intermediate ones). Slow subscribers lose the oldest events
rather than slowing the scheduler down.
