# gRPC API

Package `cronoq.v1`, defined in
[`crates/cronoq-proto/proto/cronoq/v1/cronoq.proto`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-proto/proto/cronoq/v1/cronoq.proto).
Get it from a running binary with `cronoq-server --print-proto`, or depend on the
`cronoq-proto` crate in Rust. Authentication uses the metadata entry
`authorization: Bearer <token>`.

Times are relative seconds (`*_in_s`, `*_s`) or absolute Unix nanoseconds (`*_unix_ns`).

## `Tasks` (role: client)

| RPC | Request → Response | Description |
|---|---|---|
| `Submit` | `SubmitRequest{task, wait}` → `SubmitResponse{id, duplicate, result}` | Submit a task; with `wait`, block until it finishes. |
| `SubmitBatch` | `SubmitBatchRequest{tasks}` → `SubmitBatchResponse{tasks}` | Atomic batch. |
| `Cancel` | `TaskRef` → `CancelResponse{cancelled}` | Cancel by id or key. |
| `Get` | `TaskRef` → `TaskInfo{id, status, found}` | Current status. |
| `Wait` | `WaitRequest{task, timeout_s}` → `TaskResult{id, status, output, message}` | Wait for completion (`DEADLINE_EXCEEDED` on timeout). |
| `Watch` | `WatchRequest{pools}` → stream `StatusEvent` | Status events (terminal ones; all transitions if the server enables them). `DATA_LOSS` if the stream fell behind. |

`TaskSpec` mirrors the [HTTP task object](http.md#task-object): `payload`, `content_type`,
`kind`, `pool`, `priority`, `key`, `deadline_in_s` / `deadline_unix_ns`, `start_in_s` /
`start_unix_ns`, `tolerance_s`, `on_miss`, `duration_hint_s`, `timeout_s`, `retry`, `deps`,
`resources`, `tags`.

## `Workers` (role: worker)

| RPC | Stream | Description |
|---|---|---|
| `Work` | `stream WorkerMessage` ↔ `stream ServerMessage` | One stream per worker process. |

| Direction | Message | Meaning |
|---|---|---|
| worker → server | `Register{pool, slots, worker_name}` | Must be first. |
| server → worker | `Registered{worker_id, lease_ttl_s}` | Registration accepted. |
| server → worker | `Lease{lease, task_id, attempt, payload, content_type, kind, key, tags, timeout_s, deadline_unix_ns}` | Run this attempt. |
| worker → server | `Heartbeat{lease}` | Keep a lease alive (within `lease_ttl_s`). |
| worker → server | `Complete{lease, output}` | Success. |
| worker → server | `Fail{lease, message, permanent, retry_after_s}` | Failure. |
| server → worker | `Abort{lease, reason}` | Stop this attempt (timeout, cancel, lease expiry). |
| worker → server | `SetSlots{slots}` | Change concurrency (`0` drains). |

See [Writing executors and workers](../guides/workers.md#any-language-over-grpc) for the
protocol rules.

## `Admin` (role: admin)

| RPC | Request → Response |
|---|---|
| `Snapshot` | `Empty` → `SnapshotResponse{pools, live_tasks, running, pending}` |
| `AddRecurring` | `RecurringRequest{name, cron, tz, template, allow_overlap}` → `Empty` |
| `RemoveRecurring` | `RemoveRecurringRequest{name}` → `RemoveRecurringResponse{removed}` |

## Status codes

| Code | Cause |
|---|---|
| `INVALID_ARGUMENT` | Invalid spec, duration, reference or cycle |
| `NOT_FOUND` | Unknown pool, resource, dependency or task |
| `FAILED_PRECONDITION` | Reservation conflict; registering for a pool that is not remote |
| `RESOURCE_EXHAUSTED` | Queue full (overflow policy `reject`) |
| `UNAUTHENTICATED` / `PERMISSION_DENIED` | Missing/invalid token, or wrong role |
| `DEADLINE_EXCEEDED` | `Wait` timed out |
| `UNAVAILABLE` | Shutting down, or the store failed |
| `DATA_LOSS` | A `Watch` stream lagged and events were dropped |

## Enumerations

- `Priority`: `PRIORITY_CRITICAL`, `PRIORITY_HIGH`, `PRIORITY_NORMAL` (also `UNSPECIFIED`),
  `PRIORITY_LOW`.
- `TaskStatus`: `BLOCKED`, `SCHEDULED`, `READY`, `RUNNING`, `RETRYING`, `SUCCEEDED`, `FAILED`,
  `DEAD_LETTERED`, `CANCELLED`, `SKIPPED`, `MISSED`, `SHED` (prefixed `TASK_STATUS_`).
- `EdgeCond`: `EDGE_COND_ON_SUCCESS`, `EDGE_COND_ON_COMPLETE`, `EDGE_COND_ON_FAILURE`.
- `MissPolicy`: `MISS_POLICY_RUN_LATE`, `MISS_POLICY_SKIP`, `MISS_POLICY_FAIL`.
