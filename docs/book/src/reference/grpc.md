# gRPC API

Package `cronoq.v1`, defined in
[`crates/cronoq-proto/proto/cronoq/v1/cronoq.proto`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-proto/proto/cronoq/v1/cronoq.proto).
Print it from a running binary with `cronoq-server --print-proto`, depend on the `cronoq-proto`
crate in Rust, or use the generated stubs shipped in the Python package (`cronoq._proto`).
The server listens on `server.grpc_addr` (default `127.0.0.1:7070`), with TLS when
`[server.tls]` is configured.

This page describes **worker protocol version 3**.

## Conventions

- **Authentication.** Send the metadata entry `authorization: Bearer <token>`, or present a
  client certificate mapped in `[[auth.certs]]`. Each service requires a role (below); `admin`
  passes every check. A token or certificate scoped to `pools = [...]` may only submit into,
  watch and serve those pools; tasks in other pools are reported as `NOT_FOUND`.
- **Times** are relative seconds (`*_in_s`, `*_s`, `double`) or absolute Unix nanoseconds
  (`*_unix_ns`, `int64`). Durations must be finite and non-negative. Set at most one of the
  relative and absolute forms.
- **Enum zero values** (`*_UNSPECIFIED`) mean "default": `PRIORITY_NORMAL`,
  `EDGE_COND_ON_SUCCESS`, `MISS_POLICY_RUN_LATE`.
- **Limits.** Messages are limited to `limits.max_message_bytes` (default 16 MiB), strings and
  collections to the `[limits]` table in the [configuration reference](configuration.md#limits).
  Strings must not contain NUL characters.
- **Only the leader serves.** A standby or a stopping leader answers every RPC with
  `UNAVAILABLE`; retry with backoff (through a load balancer that routes on `/readyz`).
- **Deadlines.** Unary calls are bounded by `limits.request_timeout_s`; `Submit` with `wait` and
  `Wait` by `limits.max_wait_s` on top.

## `Tasks` (role: client)

| RPC | Request → Response | Description |
|---|---|---|
| `Submit` | `SubmitRequest` → `SubmitResponse` | Submit one task; with `wait`, block until it finishes. |
| `SubmitBatch` | `SubmitBatchRequest` → `SubmitBatchResponse` | Submit up to `max_tasks_per_batch` tasks atomically. |
| `Cancel` | `CancelRequest{task}` → `CancelResponse{cancelled}` | Cancel by id or key; `false` if the task already finished. |
| `Get` | `GetRequest{task}` → `TaskInfo` | Task details; `NOT_FOUND` for unknown tasks. |
| `ListTasks` | `ListTasksRequest` → `ListTasksResponse{tasks, next_after}` | One page of tasks, live and finished, in id order. |
| `Wait` | `WaitRequest{task, timeout_s}` → `TaskResult` | Wait for the task to finish. |
| `Watch` | `WatchRequest` → stream `StatusEvent` | Live status events. |

### `TaskSpec`

| Field | Type | Description |
|---|---|---|
| `payload` | bytes | Passed to the worker unchanged. |
| `content_type` | string | Passed to the worker with the payload. |
| `kind` | string | Label passed to the worker; default `"default"`. |
| `pool` | string | Target pool; default `"default"`. |
| `priority` | `Priority` | Default `PRIORITY_NORMAL`. |
| `key` | string | Optional unique key (deduplication, dependencies). |
| `deadline_in_s` / `deadline_unix_ns` | optional | Deadline. |
| `start_in_s` / `start_unix_ns` | optional | Fixed start time. |
| `tolerance_s` | optional double | Fixed-start tolerance; default 1 s, `0` allowed. |
| `on_miss` | `MissPolicy` | What a missed fixed start does. |
| `duration_hint_s` | optional double | Expected run time; default the pool's `default_duration_s`. |
| `timeout_s` | optional double | Per-attempt timeout. |
| `retry` | `RetryPolicy` | `{max_attempts, initial_backoff_s, multiplier, max_backoff_s, jitter}`; unset = the pool's policy. |
| `deps` | repeated `Dep` | `{target: TaskRef, cond: EdgeCond}`. |
| `resources` | map<string, uint32> | Units of each named resource per attempt. |
| `tags` | map<string, string> | Labels passed to the worker. |
| `labels` | map<string, string> | Worker labels the task requires ([Worker labels](../concepts/labels.md)). |
| `tenant` | string | Tenant, for pools that share capacity fairly ([Fair sharing](../concepts/fairness.md)). |

The fields match the [HTTP task object](http.md#task-object), which documents them in detail.
`TaskRef` is a `oneof` of `id` (uint64) and `key` (string).

### Submit and batches

`SubmitRequest{task, wait, wait_timeout_s}` returns `SubmitResponse{id, duplicate, result}`.
`duplicate` is true when the key already exists (then `id` is the existing task). With `wait`,
`result` is set once the task finished; if `wait_timeout_s` (capped at `limits.max_wait_s`)
expires first the call fails with `DEADLINE_EXCEEDED` and the task keeps running.

`SubmitBatchRequest{tasks}` is admitted atomically: all tasks or none. Dependencies may name
keys defined anywhere in the batch. The response lists one `SubmitResponse` per task, in request
order, without `result`.

With Postgres, `Submit`, `SubmitBatch` and `Cancel` return after the change is durable.

### `TaskInfo`

| Field | Description |
|---|---|
| `id`, `status` | Authoritative. |
| `pool`, `key`, `kind`, `tenant` | As submitted. |
| `priority` | `PRIORITY_UNSPECIFIED` if unknown. |
| `attempt` | Current or last attempt (0 before the first dispatch). |
| `max_attempts` | Attempt budget; 0 if unknown. |
| `created_unix_ns`, `started_unix_ns`, `finished_unix_ns` | Lifecycle timestamps, when known. |
| `message` | Last error or status message. |
| `output_available` | True when the task succeeded and its output is retained. |

Fields other than `id` and `status` are best effort: they come from the serving leader's info
cache (`outputs.info_entries`) or, with a durable store, from the stored task.

### `ListTasks`

`ListTasksRequest` has `pools` (empty: every pool the caller may use), `statuses` (empty: any;
`TASK_STATUS_UNSPECIFIED` is `INVALID_ARGUMENT`), `after` (only larger ids; 0 starts from the
beginning) and `limit` (0 means 100; capped at `limits.max_list`). The response's `next_after`
is the value to pass as `after` for the next page, and 0 on the last page. Pools are checked as
for `Watch`: unknown ones are `NOT_FOUND`, pools the caller may not use `PERMISSION_DENIED`.

Live tasks come from the scheduler. Finished tasks come from the store, or, without one, from
the bounded info cache, which holds only the most recent ones. A page is not an atomic
snapshot: a task that finishes while the page is built can be missing from it.

### `TaskResult`

`{id, status, output, message, output_available}`. `status` is always terminal. `output` is set
when the task succeeded and its output is still retained (`output_available`); a succeeded task
whose output was evicted from the cache and the store has `output_available = false`. `message`
explains failures.

`Wait` without `timeout_s` waits up to `limits.max_wait_s`; on timeout it fails with
`DEADLINE_EXCEEDED` (`task is still running; wait again or poll`). It fails with `UNAVAILABLE`
if the server stops serving while waiting: wait again on the new leader.

### `Watch`

`WatchRequest` fields:

| Field | Description |
|---|---|
| `pools` | Pools to watch; empty = all pools the caller may see. Unknown pools are `NOT_FOUND`; pools outside a scoped token are `PERMISSION_DENIED`. |
| `ids`, `keys` | Optional task filters (at most `max_tasks_per_batch` together). |
| `all_transitions` | Deliver every transition (`BLOCKED`, `READY`, `RUNNING`, `RETRYING`, ...), not only terminal statuses. |

Each `StatusEvent` carries `{id, pool, key, status, attempt, at_unix_ns, message}`. The stream
ends with `UNAVAILABLE` when the server shuts down or loses leadership, and with `DATA_LOSS`
when the client reads too slowly and events were dropped. Events are live only: reconnect and
use `Get` / `Wait` to reconcile anything that happened while disconnected.

## `Workers` (role: worker)

| RPC | Stream | Description |
|---|---|---|
| `Work` | `stream WorkerMessage` ↔ `stream ServerMessage` | One bidirectional stream per worker (or per pool a worker serves). |

### Messages

| Direction | Message | Meaning |
|---|---|---|
| worker → server | `Register{pool, slots, worker_name, protocol_version, labels}` | Must be the first message, within 10 s. Send `protocol_version` 3; older versions are refused with `FAILED_PRECONDITION`. `labels` place the worker in a [group](../concepts/labels.md); a credential with `labels = [...]` may claim only those (`PERMISSION_DENIED` otherwise). |
| server → worker | `Registered{worker_id, lease_ttl_s, slots, protocol_version, max_message_bytes}` | Registration accepted. `slots` is what was granted: at most the pool's `max_slots_per_worker`. |
| server → worker | `Lease{lease, task_id, attempt, payload, content_type, kind, key, tags, timeout_s, deadline_unix_ns, pool, priority, lease_ttl_s}` | Run this attempt. `priority` is currently always `UNSPECIFIED`. |
| worker → server | `Heartbeat{lease}` | Keep a lease alive. |
| worker → server | `Complete{lease, output, content_type}` | Success; `output` becomes the task's result. |
| worker → server | `Fail{lease, message, permanent, retry_after_s}` | Failure: permanent, or retryable with an optional minimum delay. |
| worker → server | `Nack{lease, reason}` | The worker cannot take this lease (for example it is draining). |
| worker → server | `Cancelled{lease}` | The handler stopped because the lease was aborted or cancelled. |
| worker → server | `SetSlots{slots}` | Change concurrency; `0` drains (no new leases). Capped like `Register.slots`. |
| server → worker | `Abort{lease, reason}` | Stop this attempt (timeout, cancellation, lease expiry). |
| server → worker | `Shutdown{reason, not_leader}` | The server is going away; reconnect with backoff. |

### Protocol rules

1. **Register first.** A stream whose first message is not `Register` fails with
   `INVALID_ARGUMENT`; one that sends nothing for 10 s fails with `DEADLINE_EXCEEDED`. A second
   `Register` on the same stream ends it. Unknown pools are `NOT_FOUND`.
2. **Capacity is slots.** A pool's capacity is the sum of the granted slots of its connected
   workers. The server never sends more concurrent leases to a worker than its slots. Use the
   `slots` value from `Registered`, not the one you requested.
3. **Heartbeat each running lease** more often than `lease_ttl_s` (every `lease_ttl_s / 3` is a
   good rule). A lease without a heartbeat for `lease_ttl_s` expires: the attempt fails as
   retryable and the worker receives `Abort`.
4. **Report exactly one outcome** per lease: `Complete`, `Fail`, `Nack` or `Cancelled`.
   Reports for leases the worker does not hold (already aborted, or unknown) are ignored.
5. **`Nack`** offers the lease to another worker of the pool, without counting an attempt. If no
   other worker has a free slot, the attempt fails as retryable.
6. **`Abort`** means the server has already given up the attempt and freed the slot. Stop the
   handler; sending `Cancelled` afterwards is allowed and ignored.
7. **Read the stream promptly.** Each worker has a bounded outbound queue (about one lease and
   one abort per slot). A worker that stops reading is disconnected rather than allowed to hold
   leases it cannot see.
8. **Disconnects fail running leases** as retryable (`worker disconnected`); they are retried
   according to the task's retry policy.
9. **Shutdown.** On a graceful stop the server first gives running attempts
   `server.shutdown_grace_s` to finish (workers keep reporting outcomes), then sends
   `Shutdown{not_leader: false}`. When it loses leadership it sends `Shutdown{not_leader: true}`
   immediately. Either way, leases still running are failed as retryable and the stream ends
   about a second later: stop running work and reconnect with backoff.
10. **Messages** in both directions are limited to `max_message_bytes` from `Registered`.

The Rust (`cronoq_server::worker::run_worker`) and Python (`cronoq.client.Worker`) workers
implement these rules; see [Writing executors and workers](../guides/workers.md).

## `Admin`

| RPC | Role | Request → Response |
|---|---|---|
| `Snapshot` | metrics | `Empty` → `SnapshotResponse` |
| `AddRecurring` | admin | `RecurringRequest{name, cron, tz, template, overlap}` → `Empty` |
| `RemoveRecurring` | admin | `RemoveRecurringRequest{name}` → `RemoveRecurringResponse{removed}` |
| `ListWorkers` | metrics | `Empty` → `ListWorkersResponse{workers}` |
| `ListRecurring` | metrics | `Empty` → `ListRecurringResponse{schedules}` |

`ListWorkers` and `ListRecurring` list only the pools the caller may use. A `WorkerInfo` has
the connection `id` the server assigned, the `name`, `pool` and `labels` the worker registered
with, its `slots` and `busy` slots, and `connected_unix_ns`. A `RecurringInfo` has the
schedule's `name`, `cron`, `tz` (empty for UTC), `pool`, `kind` and `overlap`, `next_unix_ns`
(unset when no further occurrence is due) and `last_task_id` (unset before the first
occurrence); schedules are sorted by name.

`SnapshotResponse` carries `pools` (one `PoolSnapshot` per pool: `name`, `capacity`, `busy`,
`held`, `desired`, `pending_by_class`, `reservations`, `dispatched`, `succeeded`, `failed`,
`priority_adherence`, `fixed_start_hit_rate`, `utilization`, `schedule_efficiency`,
`wait_p50_s`, `wait_p90_s`, `wait_p99_s`, `cost`, `workers_connected`), `live_tasks`,
`running`, `pending`, `workers_connected` and `totals` (`submitted`, `dispatched`,
`succeeded`, `failed`, `dead_lettered`, `cancelled`, `skipped`, `missed`, `shed`, `retries`,
`timeouts`, `lease_expiries`). The [HTTP snapshot](http.md#get-v1snapshot) documents each
field.

`AddRecurring` registers (or replaces) a cron schedule named `name`. `cron` has six fields
(seconds first) or five (seconds = 0); `tz` is an IANA time zone (default UTC). The template is
a `TaskSpec` without `deadline_unix_ns`, `start_*` or `deps` (`INVALID_ARGUMENT` otherwise).
Each occurrence gets its own fixed start at the cron time, with the template's `tolerance_s`
(default 1 s) and `on_miss` (default `RUN_LATE`). The template's `deadline_in_s` is relative to
each occurrence's start, and its `key` is a prefix: each occurrence's key is
`<key>@<start in UTC, RFC 3339>`. `overlap` is `OVERLAP_SKIP` (the default: an occurrence whose
previous run is still going ends as `SKIPPED`), `OVERLAP_ALLOW`, `OVERLAP_QUEUE` (wait for the
previous run) or `OVERLAP_QUEUE_ON_SUCCESS` (wait, and run only if it succeeded). Schedules are
durable with a durable store; `AddRecurring` returns once the schedule is stored. See [Recurring schedules](../concepts/recurring.md).

## Shards

With [`[[shards]]`](configuration.md#shards), any server answers for every shard
([Sharding](../concepts/sharding.md)): calls for another shard are forwarded to it with the
caller's token, and `ListTasks`, `Watch`, `ListWorkers`, `ListRecurring` and `Snapshot` cover all
shards. The listing responses and `SnapshotResponse` name shards that could not be reached in
`unavailable_shards`; `Get` and `Wait` by key name them in the `cronoq-unavailable-shards`
response metadata, and `Cancel` by key is refused while any shard cannot be asked. A call that
is not forwarded (client-certificate callers, plain-HTTP
endpoints on other hosts, workers) gets `FAILED_PRECONDITION` with the shard's name and endpoint
in the `cronoq-shard` and `cronoq-shard-endpoint` metadata. Workers reconnect to that endpoint
(see [Sharding](../concepts/sharding.md)).

## gRPC-Web

With [`[server.grpc_web]`](configuration.md#servergrpc_web), the gRPC listener also answers
gRPC-Web requests (`application/grpc-web` and `application/grpc-web+proto`, binary framing) for
browser clients such as the [TypeScript SDK](typescript.md)'s `cronoq/web`, or those generated by
`protoc-gen-grpc-web` or Connect-ES's gRPC-Web transport, over HTTP/1.1 or HTTP/2.

- **Calls.** Unary calls and the server stream `Watch` work. `Workers.Work` is a bidirectional
  stream, which gRPC-Web cannot carry; workers use native gRPC.
- **Authentication** is the same as for native gRPC: send `authorization: Bearer <token>`.
  Browsers do not attach credentials themselves, and the server does not allow credentialed CORS
  requests, so cookies and browser-held client certificates are not used.
- **CORS.** Preflights from the configured `allowed_origins` are answered for `POST` with the
  headers `authorization`, `content-type`, `x-grpc-web`, `x-user-agent` and `grpc-timeout`; the
  response headers `grpc-status`, `grpc-message` and `grpc-status-details-bin` are exposed.
  Other origins get no CORS headers.
- **HTTP/1.1** carries only gRPC-Web: a plain gRPC request over HTTP/1.1 gets `UNIMPLEMENTED`.
- **Without credentials** (loopback-only mode), every request on the gRPC listener must name a
  loopback host (`localhost`, `*.localhost`, `127.0.0.1`, `[::1]`), or it gets
  `PERMISSION_DENIED`, as on the HTTP API. With `--insecure-no-auth` this rule is off, as it is
  for the HTTP API, so a page on a machine that can reach the server could call every service,
  `Admin` included: do not enable gRPC-Web without credentials outside a trusted network.

## Status codes

| Code | Cause |
|---|---|
| `INVALID_ARGUMENT` | Invalid spec, duration, reference, cron expression or time zone; dependency cycle; oversize string or collection. |
| `NOT_FOUND` | Unknown task, pool, resource or dependency. |
| `FAILED_PRECONDITION` | Fixed-start reservation conflict. |
| `RESOURCE_EXHAUSTED` | Queue bounds reached (`max_pending`, `max_pending_total`); retry later. |
| `UNAUTHENTICATED` | Missing, malformed or unknown credentials. |
| `PERMISSION_DENIED` | The credentials lack the role, or the pool is outside their scope. |
| `DEADLINE_EXCEEDED` | A wait timed out while the task is still running, or no `Register` within 10 s. |
| `UNAVAILABLE` | Not the leader, shutting down, or the store is unavailable. Retry with backoff. |
| `DATA_LOSS` | A `Watch` stream fell behind and events were dropped. |
| `INTERNAL` | Unexpected server error. |

## Enumerations

| Enum | Values (zero value first) |
|---|---|
| `Priority` | `PRIORITY_UNSPECIFIED` (= normal), `PRIORITY_CRITICAL`, `PRIORITY_HIGH`, `PRIORITY_NORMAL`, `PRIORITY_LOW` |
| `TaskStatus` | `TASK_STATUS_UNSPECIFIED`, `BLOCKED`, `SCHEDULED`, `READY`, `RUNNING`, `RETRYING`, `SUCCEEDED`, `FAILED`, `DEAD_LETTERED`, `CANCELLED`, `SKIPPED`, `MISSED`, `SHED` (prefixed `TASK_STATUS_`) |
| `EdgeCond` | `EDGE_COND_UNSPECIFIED` (= on success), `EDGE_COND_ON_SUCCESS`, `EDGE_COND_ON_COMPLETE`, `EDGE_COND_ON_FAILURE` |
| `MissPolicy` | `MISS_POLICY_UNSPECIFIED` (= run late), `MISS_POLICY_RUN_LATE`, `MISS_POLICY_SKIP`, `MISS_POLICY_FAIL` |

Status meanings: [HTTP statuses](http.md#statuses).

## Protocol versions

| Version | Changes |
|---|---|
| 1 | Initial version. |
| 2 | `Nack` and `Cancelled` outcomes, `Shutdown`, lease metadata (`pool`, `priority`, `lease_ttl_s`), `Complete.content_type`, granted slots and `max_message_bytes` in `Registered`. |
| 3 | Worker labels (`Register.labels`); lease ids carry the leader epoch. |

A server refuses workers that speak an older version, with `FAILED_PRECONDITION` and a message
naming the version it needs.
