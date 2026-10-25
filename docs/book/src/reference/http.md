# HTTP API

The HTTP gateway exposes the `Tasks` service of the [gRPC API](grpc.md) as JSON, except the
`Watch` stream, plus the read-only `Admin` calls, metrics and health probes. It listens on `server.http_addr` (default `127.0.0.1:7071`) and serves HTTP/1.1
and HTTP/2 (h2c without TLS, ALPN with TLS). With `[server.tls]` configured it serves HTTPS and
accepts client certificates on the same terms as the gRPC listener.

Remote workers cannot use HTTP; they need the gRPC worker stream. Recurring schedules are
managed over gRPC (`Admin.AddRecurring`) or in the embedding runtime.

## Conventions

- **Authentication.** Send `authorization: Bearer <token>`, or connect with a client
  certificate mapped in `[[auth.certs]]`. The `admin` role passes every check. Without
  any `[[auth.tokens]]` or `[[auth.certs]]`, authentication is off and every request is treated
  as admin; the server only starts that way on loopback addresses unless `--insecure-no-auth`
  is given. In that mode, unless `--insecure-no-auth` is given, a request whose `Host` is not a
  loopback name or address (`localhost`, `*.localhost`, any `127.x.x.x`, `[::1]`) gets 403.
- **Pool scope.** A token or certificate with `pools = [...]` may only submit into those pools.
  Tasks in other pools look like they do not exist (404), so their ids and keys do not leak.
  Submitting a key that is already used by a task in a pool outside the scope returns 409.
- **Tenant.** Credentials with a `tenant` submit for that tenant: tasks without a tenant get it,
  and naming another tenant returns 403.
- **Request bodies** must be JSON with `content-type: application/json` (otherwise 415) and at
  most `limits.max_http_body_bytes` (default 16 MiB, otherwise 413). Unknown fields are
  rejected with 400, so a typo such as `"priorty"` never passes silently.
- **Task ids are strings** in every JSON response (`"id": "17"`), because they are 64-bit and
  JavaScript numbers lose precision above 2^53. Requests accept ids as strings or numbers.
- **Timestamps** (`*_unix_ns`) are Unix nanoseconds, returned as strings for the same reason.
  Durations (`*_s`) are seconds as JSON numbers and must be finite and non-negative.
- **Every request has a time limit** of `limits.request_timeout_s` (default 30 s). Every
  request to `/v1/tasks` (`POST` and `GET`) and to every path ending in `/result` gets
  `limits.max_wait_s` (default 300 s) on top. A request that runs out of time returns 504.
- **Only the leader serves.** A standby (or a leader that is shutting down) answers every `/v1`
  route and `/metrics` with 503 `not the leader (standby or shutting down)`. Put the servers
  behind a load balancer that routes on `/readyz`.

## Routes

| Method and path | Role | Description |
|---|---|---|
| `POST /v1/tasks` | client | Submit one task; optionally wait for its result. |
| `GET /v1/tasks` | client | List tasks, live and finished, one page at a time. |
| `POST /v1/tasks:batch` | client | Submit up to `max_tasks_per_batch` tasks atomically. |
| `GET /v1/tasks/{id}` | client | Task details by numeric id. |
| `DELETE /v1/tasks/{id}` | client | Cancel a task by id. |
| `GET /v1/tasks/{id}/result` | client | Wait for a task's result by id. |
| `GET /v1/tasks/by-key/{key}` | client | Task details by key. |
| `DELETE /v1/tasks/by-key/{key}` | client | Cancel a task by key. |
| `GET /v1/tasks/by-key/{key}/result` | client | Wait for a task's result by key. |
| `GET /v1/snapshot` | metrics | Per-pool state, quality signals and totals. |
| `GET /v1/workers` | metrics | Connected remote workers. |
| `GET /v1/recurring` | metrics | Recurring schedules. |
| `GET /metrics` | metrics | Prometheus text exposition ([metrics reference](metrics.md)). |
| `GET /healthz` | none | `200 ok` while the process runs (liveness). |
| `GET /readyz` | none | `200 ready` on the leader, `503 standby` otherwise (readiness). |

`{id}` must be a decimal task id; anything else returns 400 and points to the `by-key` route.
Keys are percent-decoded path segments: encode `/` as `%2F` (`/v1/tasks/by-key/report%2F42`).

When `server.metrics_addr` is set, a second listener serves `/metrics`, `/healthz` and
`/readyz` **without authentication**, for a Prometheus scraper or kubelet on a private network.
The authenticated `/metrics` on `http_addr` stays available.

## Task object

The body of `POST /v1/tasks` and each element of a batch's `tasks`. Every field is optional;
`{}` submits an empty payload to the `default` pool.

| Field | Type | Default | Description |
|---|---|---|---|
| `payload` | any JSON | – | Payload, sent to the worker as JSON bytes with content type `application/json`. |
| `payload_b64` | string | – | Binary payload, standard base64 (content type `application/octet-stream`). Mutually exclusive with `payload`. |
| `content_type` | string | from the payload | Overrides the content type passed to the worker. |
| `pool` | string | `"default"` | Target pool. Must exist (404) and be allowed for the caller (403). |
| `kind` | string | `"default"` | Free-form label passed to the worker (at most `max_kind_len`). |
| `priority` | `"critical"`, `"high"`, `"normal"`, `"low"` | `"normal"` | Priority class. |
| `key` | string | – | Unique key: deduplicates submissions and names the task for dependencies and the `by-key` routes (at most `max_key_len`). Keys of the form `<prefix>@<UTC time, RFC 3339>` (for example `report@2026-01-01T03:00:00Z`) are reserved for recurring occurrences and rejected with 400. |
| `deadline_in_s` / `deadline_unix_ns` | number / integer | – | Deadline, relative or absolute. Raises urgency as it approaches. |
| `start_in_s` / `start_unix_ns` | number / integer | – | Fixed start time, relative or absolute ([fixed starts](../concepts/fixed-starts.md)). |
| `tolerance_s` | number | `1` | How late a fixed start may begin; `0` is allowed. Requires `start_in_s` or `start_unix_ns`. |
| `on_miss` | `"run_late"`, `"skip"`, `"fail"` | `"run_late"` | What happens when a fixed start misses its window. |
| `duration_hint_s` | number | pool's `default_duration_s` | Expected run time, used for reservations and hold leads. |
| `timeout_s` | number | none | Per-attempt timeout; the worker's lease is aborted when it expires. |
| `retry` | object | pool's `[pools.retry]` | `{"max_attempts": n, "initial_backoff_s", "multiplier", "max_backoff_s", "jitter"}`. `max_attempts` is required; `jitter` defaults to `true`. |
| `after` | array | `[]` | Tasks that must **succeed** first: strings are keys; numbers or `{"id": "..."}` are ids. |
| `deps` | array | `[]` | `{"key": "..."}` or `{"id": "..."}`, with optional `"cond"`: `"on_success"` (default), `"on_complete"`, `"on_failure"`. |
| `resources` | object | `{}` | `{"<resource>": units}` acquired for each attempt. At most `max_resources` entries. |
| `tags` | object | `{}` | String labels passed to the worker. At most `max_tags`; each key and each value at most `max_tag_len`. Keys starting with `cronoq.` are reserved. |
| `tenant` | string | – | Tenant, for pools that share capacity fairly ([Fair sharing](../concepts/fairness.md)). |
| `labels` | object | `{}` | Worker labels the task requires: it runs only on workers that registered with all of them ([Worker labels](../concepts/labels.md)). Same limits as `tags`. |
| `wait` | bool | `false` | `POST /v1/tasks` only: respond when the task has finished. Rejected in batches. |
| `wait_timeout_s` | number | `limits.max_wait_s` | Upper bound for `wait`, capped at `limits.max_wait_s`. |

Use either the relative or the absolute form of a time, not both. Strings must not contain NUL
characters. Dependencies (`after` plus `deps`) are limited to `max_deps`.

## `POST /v1/tasks`

```sh
curl -s localhost:7071/v1/tasks -H "authorization: Bearer $CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"pool": "default", "payload": {"prompt": "hi"}, "priority": "high", "key": "job-1"}'
```

```json
{"id": "17", "duplicate": false}
```

`duplicate` is `true` when a task with the same `key` is already known; `id` is then the
existing task's id and nothing new is enqueued. With a durable store (Postgres or SQLite), the
response is sent after the task is stored.

With `"wait": true` the response also carries the [result](#result-object):

```json
{"id": "17", "duplicate": false,
 "result": {"id": "17", "status": "succeeded", "output_available": true,
            "output": {"answer": 42}, "output_b64": "eyJhbnN3ZXIiOjQyfQ=="}}
```

If the task is still running when `wait_timeout_s` expires, the request fails with 504; the task
keeps running and its result can be fetched with `GET /v1/tasks/{id}/result`.

## `POST /v1/tasks:batch`

```json
{"tasks": [
  {"key": "extract", "payload": {"doc": 1}},
  {"key": "summarise", "payload": {"doc": 1}, "after": ["extract"]}
]}
```

```json
{"tasks": [{"id": "18", "duplicate": false}, {"id": "19", "duplicate": false}]}
```

The batch is admitted atomically: either every task is accepted, or none is and the error names
the offending task. Dependencies may reference keys defined anywhere in the same batch, in any
order. Results are in request order. At most `limits.max_tasks_per_batch` tasks (default 1000).

## `GET /v1/tasks/{id}` and `GET /v1/tasks/by-key/{key}`

```json
{"id": "17", "status": "running", "pool": "default", "key": "job-1", "kind": "default",
 "attempt": 1, "max_attempts": 3, "output_available": false,
 "created_unix_ns": "1760000000000000000", "started_unix_ns": "1760000000120000000"}
```

| Field | Present | Description |
|---|---|---|
| `id` | always | Task id (string). |
| `status` | always | See [statuses](#statuses). |
| `pool` | always | Pool name (empty if unknown). |
| `attempt` | always | Current or last attempt number (0 before the first dispatch). |
| `output_available` | always | `true` when the task succeeded and its output is still retained. |
| `key`, `kind` | when set | Task key and kind. |
| `tenant` | when set | Tenant the task was submitted for. |
| `priority` | when known | `"critical"`, `"high"`, `"normal"` or `"low"`. |
| `max_attempts` | when known | Attempt budget from the task's or the pool's retry policy. |
| `created_unix_ns`, `started_unix_ns`, `finished_unix_ns` | when known | Lifecycle timestamps (strings). |
| `message` | when set | Last error or status message. |

`id` and `status` are authoritative; the other fields are best effort and come from the serving
leader's bounded info cache (`outputs.info_entries`) or, with a durable store, from the stored
task. Unknown tasks, tasks past retention, and tasks in pools the caller may not use return 404.

## `GET /v1/tasks`

Lists tasks in id order, oldest first, as task objects like those of `GET /v1/tasks/{id}`:

```
GET /v1/tasks?pool=default,reports&status=running,failed&limit=50
```

```json
{"tasks": [{"id": "17", "status": "running", "pool": "default", "attempt": 1, ...}],
 "next_after": "17"}
```

| Query parameter | Default | Description |
|---|---|---|
| `pool` | every pool the caller may use | Comma-separated pool names. Unknown pools return 404, pools the caller may not use 403. |
| `status` | any | Comma-separated [statuses](#statuses). |
| `after` | – | Only tasks with a larger id: pass the previous page's `next_after`. |
| `limit` | `100` | Page size, at most `limits.max_list`. |

`next_after` is present when more tasks follow. With shards, the list covers every shard, and
`unavailable_shards` names shards that could not be reached. It is present only when a shard
was missing, on this route and on `/v1/workers`, `/v1/recurring`, the task-detail routes and the
result routes; `/v1/snapshot` always includes it (empty when every shard answered). Live tasks come from the scheduler. Finished
tasks come from the store, or, without one, from the bounded info cache, so only the most
recent ones are listed. A page is not an atomic snapshot: a task that finishes while the page is
built can be missing from it.

## `GET /v1/workers`

```json
{"workers": [{"id": "3", "name": "gpu-1", "pool": "default", "labels": {"gpu": "a100"},
              "slots": 4, "busy": 1, "connected_unix_ns": "1760000000000000000"}]}
```

Connected remote workers of the pools the caller may use, in connection order. `id` is assigned
by the server per connection; `name` is what the worker registered with.

## `GET /v1/recurring`

```json
{"schedules": [{"name": "nightly", "cron": "0 0 3 * * *", "tz": "Europe/Berlin",
                "pool": "default", "kind": "report", "overlap": "queue",
                "next_unix_ns": "1760061600000000000", "last_task_id": "41"}]}
```

Recurring schedules of the pools the caller may use, by name. `tz` is absent for UTC,
`next_unix_ns` when the schedule has no further occurrence, and `last_task_id` before the first
occurrence.

## `DELETE /v1/tasks/{id}` and `DELETE /v1/tasks/by-key/{key}`

```json
{"cancelled": true}
```

`false` when the task already finished. Cancelling a running task aborts the worker's lease and
cancels its handler. Dependents on an `on_success` or `on_failure` edge end as `skipped`;
`on_complete` dependents proceed. With a durable store (Postgres or SQLite), the cancellation
is stored before the response is sent.

## `GET /v1/tasks/{id}/result` and `GET /v1/tasks/by-key/{key}/result`

Waits until the task finishes, at most `timeout_s` seconds (query parameter, default and maximum
`limits.max_wait_s`). `?timeout_s=0` returns immediately.

```sh
curl -s "localhost:7071/v1/tasks/17/result?timeout_s=60" -H "authorization: Bearer $CLIENT_TOKEN"
```

Returns a [result object](#result-object), or 504 `task is still running; wait again or poll`.
Results of finished tasks remain available while the leader's output cache
(`outputs.cache_entries`, `outputs.cache_bytes`) or the durable store (Postgres or SQLite,
`store.keep_finished_s`) still holds them.

### Result object

```json
{"id": "17", "status": "failed", "output_available": false, "message": "asked to fail"}
```

| Field | Present | Description |
|---|---|---|
| `id` | always | Task id (string). |
| `status` | always | A terminal status. |
| `output_available` | always | `false` for a succeeded task whose output is no longer retained. |
| `output_b64` | output available | The output bytes, base64. |
| `output` | output is valid JSON | The output decoded as JSON. |
| `message` | when set | Why the task did not succeed. |

## `GET /v1/snapshot`

```json
{"live_tasks": 12, "running": 4, "pending": 8, "workers_connected": 2, "unavailable_shards": [],
 "pools": [{"name": "default", "capacity": 4, "busy": 4, "held": 0, "desired": 6,
            "pending": 8, "pending_by_class": [0, 2, 6, 0], "reservations": 0,
            "dispatched": 120, "succeeded": 118, "failed": 1,
            "priority_adherence": 1.0, "fixed_start_hit_rate": 1.0,
            "utilization": 0.83, "schedule_efficiency": 0.80,
            "wait_p50_s": 0.2, "wait_p90_s": 1.4, "wait_p99_s": 2.9, "cost": 0.0,
            "workers_connected": 2,
            "groups": [{"labels": {}, "capacity": 3, "busy": 3},
                       {"labels": {"gpu": "a100"}, "capacity": 1, "busy": 1}]}],
 "totals": {"submitted": 130, "dispatched": 124, "succeeded": 118, "failed": 1,
            "dead_lettered": 0, "cancelled": 2, "skipped": 0, "missed": 0, "shed": 0,
            "retries": 4, "timeouts": 1, "lease_expiries": 0,
            "stale_completions": 0}}
```

| Field | Description |
|---|---|
| `live_tasks`, `running`, `pending` | Tasks not yet finished; of those, running and waiting. |
| `workers_connected` | Remote worker streams across all pools. |
| `unavailable_shards` | Shards that could not be reached; their part is missing from the response. |
| `pools[].capacity` | Worker slots (sum of the slots of connected workers). |
| `pools[].busy`, `held`, `desired` | Busy slots; slots held idle for an upcoming fixed start; the autoscaler's target. |
| `pools[].pending`, `pending_by_class` | Waiting tasks, total and by class (critical, high, normal, low). |
| `pools[].reservations` | Fixed-start reservations not yet started. |
| `pools[].dispatched`, `succeeded`, `failed` | Attempts started, tasks succeeded and attempts failed (including retried ones) within the metrics window (the last 60 s). |
| `pools[].priority_adherence`, `fixed_start_hit_rate`, `utilization`, `schedule_efficiency` | Quality signals in `[0, 1]` over the metrics window. |
| `pools[].wait_p50_s`, `wait_p90_s`, `wait_p99_s` | Time from submission to first start, over the metrics window. |
| `pools[].cost` | Cost since start (`cost_per_worker_second` × worker-seconds). |
| `pools[].groups` | Worker groups (workers with the same labels), unlabeled first: labels, slots and busy slots. |
| `totals` | Lifetime counters of this leader since it started. |

Field meanings: [Metrics and quality signals](../concepts/metrics.md).

## Statuses

| Status | Terminal | Meaning |
|---|---|---|
| `blocked` | no | Waiting for dependencies. |
| `scheduled` | no | Waiting for its fixed start time. |
| `ready` | no | Eligible and queued for a worker. |
| `running` | no | An attempt is running on a worker. |
| `retrying` | no | An attempt failed; waiting for the retry backoff to elapse. |
| `succeeded` | yes | Finished successfully. |
| `failed` | yes | Failed permanently, or missed a fixed start with `on_miss = "fail"`. |
| `dead_lettered` | yes | Retryable failures used up every attempt. |
| `cancelled` | yes | Cancelled by a caller. |
| `skipped` | yes | A dependency edge can no longer be met, an unknown dependency was awaited too long, or a recurring run overlapped the previous one. |
| `missed` | yes | Missed its fixed start with `on_miss = "skip"`. |
| `shed` | yes | Evicted by a pool's `shed_lowest` / `shed_oldest` overflow policy. |

## Errors

Errors have the body `{"error": "<message>", "code": "<reason>"}`, where `code` is the
lower-case HTTP reason phrase (for example `too_many_requests`). 401 responses carry
`www-authenticate: Bearer`.

| Status | gRPC equivalent | Cause |
|---|---|---|
| 400 | `INVALID_ARGUMENT` | Malformed JSON, unknown field or enum value, invalid duration, oversize string, dependency cycle, invalid id. |
| 401 | `UNAUTHENTICATED` | Missing, malformed or unknown credentials. |
| 403 | `PERMISSION_DENIED` | The credentials lack the route's role, the pool or tenant is outside their scope, or authentication is off without `--insecure-no-auth` and the `Host` is not a loopback name or address. |
| 404 | `NOT_FOUND` | Unknown task, pool, resource or dependency, or unknown route. |
| 405 | – | Method not allowed on this route. |
| 409 | `FAILED_PRECONDITION`, `ALREADY_EXISTS` | Fixed-start reservation conflict; a key already used by a task in a pool outside the credentials' scope; a key that names tasks in several shards (use the id); a task or pool served by another shard that this server cannot forward to. |
| 413 | – | Body larger than `limits.max_http_body_bytes`. |
| 415 | – | Missing or wrong `content-type`. |
| 429 | `RESOURCE_EXHAUSTED` | Pool (`max_pending`) or server (`max_pending_total`) queue full. Retry with backoff. |
| 500 | `INTERNAL` | Unexpected server error. |
| 503 | `UNAVAILABLE` | Not the leader, shutting down, or the store is unavailable. Retry, preferably against the leader. |
| 504 | `DEADLINE_EXCEEDED` | A wait ended before the task finished, or the request exceeded its time limit. |

See also [Errors](errors.md) for retry guidance.
