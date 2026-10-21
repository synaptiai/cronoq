# HTTP API

Base URL: the server's `http_addr` (default port 7071). Request and response bodies are JSON.
Routes under `/v1` require `authorization: Bearer <token>` with the `client` or `admin` role
when authentication is enabled.

| Method and path | Auth | Description |
|---|---|---|
| `POST /v1/tasks` | client | Submit one task. |
| `POST /v1/tasks:batch` | client | Submit tasks atomically. |
| `GET /v1/tasks/{task}` | client | Status of a task. |
| `DELETE /v1/tasks/{task}` | client | Cancel a task. |
| `GET /v1/tasks/{task}/result` | client | Wait for a task's result. |
| `GET /v1/snapshot` | client | Per-pool metrics. |
| `GET /metrics` | none | Prometheus text exposition. |
| `GET /healthz` | none | `200 ok` while the process runs. |
| `GET /readyz` | none | `200 ready` on the leader, `503 standby` otherwise. |

`{task}` is an id when it consists only of digits, and a key otherwise.

## Task object

Used by `POST /v1/tasks` and in `tasks` of a batch. All fields are optional.

| Field | Type | Description |
|---|---|---|
| `payload` | any JSON | Payload, stored as `application/json`. |
| `payload_b64` | string | Binary payload, base64. Mutually exclusive with `payload`. |
| `content_type` | string | Overrides the payload's content type. |
| `pool` | string | Default `"default"`. |
| `kind` | string | Default `"default"`. |
| `priority` | `"critical"` \| `"high"` \| `"normal"` \| `"low"` | Default `"normal"`. |
| `key` | string | Unique key (deduplication, dependencies). |
| `deadline_in_s` / `deadline_unix_ns` | number | Deadline, relative or absolute. |
| `start_in_s` / `start_unix_ns` | number | Fixed start, relative or absolute. |
| `tolerance_s` | number | Fixed-start tolerance (default 1). |
| `on_miss` | `"run_late"` \| `"skip"` \| `"fail"` | Fixed-start miss policy (default `run_late`). |
| `duration_hint_s` | number | Expected run time. |
| `timeout_s` | number | Per-attempt timeout. |
| `retry` | object | `{max_attempts, initial_backoff_s, multiplier, max_backoff_s, jitter}`. |
| `after` | array of string/number | Keys or ids that must succeed first. |
| `deps` | array | `{"key": "..."}` or `{"id": n}`, with optional `"cond"`: `on_success` (default), `on_complete`, `on_failure`. |
| `resources` | object | `{"name": units}`. |
| `tags` | object | String key/value labels. |
| `wait` | bool | `POST /v1/tasks` only: wait for the result before responding. |

Durations must be finite, non-negative numbers of seconds.

## `POST /v1/tasks`

```json
{"id": 17, "duplicate": false}
```

With `"wait": true`:

```json
{"id": 17, "duplicate": false,
 "result": {"id": 17, "status": "succeeded", "output": {"answer": 42}, "output_b64": "eyJhbnN3ZXIiOjQyfQ=="}}
```

`duplicate` is `true` when the key already existed; `id` is then the existing task.

## `POST /v1/tasks:batch`

Request `{"tasks": [<task>, ...]}`; response `{"tasks": [{"id": n, "duplicate": bool}, ...]}`
in request order. Dependencies may reference keys defined anywhere in the batch.

## `GET /v1/tasks/{task}`

```json
{"id": 17, "status": "running"}
```

`status` is one of `blocked`, `scheduled`, `ready`, `running`, `retrying`, `succeeded`,
`failed`, `dead_lettered`, `cancelled`, `skipped`, `missed`, `shed`. Unknown tasks (or finished
ones past retention) return 404.

## `DELETE /v1/tasks/{task}`

`{"cancelled": true}`, or `false` when the task is not live.

## `GET /v1/tasks/{task}/result?timeout_s=30`

Waits up to `timeout_s` (default 30, maximum 3600) for the task to finish.

```json
{"id": 17, "status": "failed", "message": "asked to fail"}
```

| Field | Present when |
|---|---|
| `output_b64` | The task succeeded and its output is available. |
| `output` | Additionally, the output bytes are valid JSON. |
| `message` | The task did not succeed. |

Returns 408 if the task is still running at the timeout.

## `GET /v1/snapshot`

```json
{"live_tasks": 12, "running": 4, "pending": 8,
 "pools": [{"name": "default", "capacity": 4, "busy": 4, "held": 0, "desired": 6,
            "pending": 8, "pending_by_class": [0, 2, 6, 0], "reservations": 0,
            "dispatched": 120, "succeeded": 118, "failed": 1,
            "priority_adherence": 1.0, "fixed_start_hit_rate": 1.0,
            "utilization": 0.83, "schedule_efficiency": 0.80,
            "wait_p50_s": 0.2, "wait_p90_s": 1.4, "wait_p99_s": 2.9, "cost": 0.0}]}
```

Field meanings: [Metrics and quality signals](../concepts/metrics.md).

## Errors

Error responses have the body `{"error": "<message>"}`.

| Status | Cause |
|---|---|
| 400 | Malformed JSON, unknown enum value, invalid duration, dependency cycle, invalid task |
| 401 | Missing or invalid bearer token |
| 403 | Token role not allowed for this route |
| 404 | Unknown task, pool, resource or dependency |
| 408 | Result not ready within `timeout_s` |
| 409 | Fixed-start reservation conflict |
| 429 | Pool or global queue full |
| 500 | Unexpected server error |
| 503 | Scheduler shutting down or store unavailable |
