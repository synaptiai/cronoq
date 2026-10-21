# Using the HTTP API

The HTTP gateway exposes the client side of the gRPC API as JSON, so any language with an HTTP
client can submit and inspect work. It listens on `server.http_addr` (default
`127.0.0.1:7071`). All `/v1` routes need `authorization: Bearer <token>` with the `client` role
(or `admin`) when authentication is enabled, and request bodies need
`content-type: application/json`.

```sh
export CRONOQ=http://localhost:7071
export AUTH="authorization: Bearer $CRONOQ_CLIENT_TOKEN"
export JSON="content-type: application/json"
```

## Submit

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H "$JSON" -d '{
  "payload": {"prompt": "summarize ..."},
  "pool": "llm",
  "kind": "summarize",
  "priority": "high",
  "key": "summary-42",
  "deadline_in_s": 30,
  "timeout_s": 60,
  "retry": {"max_attempts": 5, "initial_backoff_s": 0.5},
  "resources": {"openai": 1},
  "tags": {"tenant": "acme"}
}'
# {"id":"17","duplicate":false}
```

- `payload` is any JSON value (sent to workers as `application/json`); use `payload_b64` for
  binary data.
- `key` makes the submission idempotent: sending it again returns the same id with
  `"duplicate": true`. Use it for every request you might retry.
- Add `"wait": true` (and optionally `"wait_timeout_s"`) to block until the task finishes and
  include its result.
- Unknown fields are rejected, so typos fail loudly instead of being ignored.

## Dependencies and batches

```sh
curl -s "$CRONOQ/v1/tasks:batch" -H "$AUTH" -H "$JSON" -d '{"tasks": [
  {"payload": "extract", "key": "extract"},
  {"payload": "load", "key": "load", "after": ["extract"]},
  {"payload": "alert", "deps": [{"key": "load", "cond": "on_failure"}]}
]}'
# {"tasks":[{"id":"18","duplicate":false},{"id":"19","duplicate":false},{"id":"20","duplicate":false}]}
```

`after` accepts keys (strings) and ids (`{"id": "18"}` or a number). A batch is all-or-nothing,
and its tasks may reference each other in any order. `alert` runs only if `load` fails; if
`load` succeeds, `alert` ends as `skipped`.

## Status, results, cancellation

Ids and keys have separate routes:

```sh
curl -s $CRONOQ/v1/tasks/17 -H "$AUTH"
curl -s $CRONOQ/v1/tasks/by-key/summary-42 -H "$AUTH"
# {"id":"17","status":"running","pool":"llm","key":"summary-42","kind":"summarize","attempt":1,...}

curl -s "$CRONOQ/v1/tasks/17/result?timeout_s=60" -H "$AUTH"
# {"id":"17","status":"succeeded","output_available":true,"output":{...},"output_b64":"..."}

curl -s -X DELETE $CRONOQ/v1/tasks/by-key/summary-42 -H "$AUTH"
# {"cancelled":true}
```

Percent-encode keys that contain `/` (`report%2F2024`). A result request that times out
returns 504 while the task keeps running: ask again.

## Scheduled work

```sh
# Start in 10 minutes; skip it if it cannot start within 30 s of that time.
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H "$JSON" -d '{
  "payload": {"report": "daily"}, "start_in_s": 600, "tolerance_s": 30, "on_miss": "skip"}'
```

The server reserves capacity for the start time, and holds a worker slot free shortly before
it. Recurring schedules are managed through the gRPC `Admin` service with an admin token, for
example with the Python client's `add_recurring`.

## Operations

```sh
curl -s $CRONOQ/v1/snapshot -H "authorization: Bearer $CRONOQ_METRICS_TOKEN"  # pools, waits, adherence, cost
curl -s $CRONOQ/metrics -H "authorization: Bearer $CRONOQ_METRICS_TOKEN"      # Prometheus text
curl -s $CRONOQ/healthz                  # process is up
curl -s $CRONOQ/readyz                   # 200 on the leader, 503 on standbys
```

Set `server.metrics_addr` to serve `/metrics` without a token on a separate, internal port.

## Errors

Errors are JSON `{"error": "...", "code": "..."}`:

| Status | When | Retry? |
|---|---|---|
| 400 | Invalid JSON, unknown field or enum value, invalid duration, cycle | no |
| 401 | Missing or invalid token | no |
| 403 | Token lacks the role, or the pool is outside its scope | no |
| 404 | Unknown task, pool, resource or dependency | no |
| 409 | Fixed-start reservation conflict | after changing the start |
| 413 / 415 | Body too large / missing `content-type: application/json` | no |
| 429 | Queue full | yes, with backoff |
| 503 | Not the leader, shutting down, or store unavailable | yes, with backoff |
| 504 | A wait ended before the task finished | ask for the result again |

Full details: [HTTP API reference](../reference/http.md).
