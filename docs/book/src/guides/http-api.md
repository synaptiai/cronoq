# Using the HTTP API

The HTTP gateway mirrors the gRPC `Tasks` service with JSON. It listens on `http_addr`
(default `0.0.0.0:7071`). All `/v1` routes need `authorization: Bearer <token>` with the
`client` role (or `admin`) when authentication is enabled.

```sh
export CRONOQ=http://localhost:7071
export AUTH="authorization: Bearer client-token"
```

## Submit

```sh
curl -s -X POST $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "payload": {"prompt": "summarize ..."},
  "pool": "llm",
  "kind": "summarize",
  "priority": "high",
  "key": "summary:42",
  "deadline_in_s": 30,
  "timeout_s": 60,
  "retry": {"max_attempts": 5, "initial_backoff_s": 0.5},
  "resources": {"openai": 1},
  "tags": {"tenant": "acme"}
}'
# {"id":17,"duplicate":false}
```

- `payload` is any JSON value (sent to workers as `application/json`); use `payload_b64` for
  binary data.
- Add `"wait": true` to block until the task finishes and include its result.

## Dependencies and batches

```sh
curl -s -X POST "$CRONOQ/v1/tasks:batch" -H "$AUTH" -d '{"tasks": [
  {"payload": "extract", "key": "extract"},
  {"payload": "load", "key": "load", "after": ["extract"]},
  {"payload": "alert", "deps": [{"key": "load", "cond": "on_failure"}]}
]}'
```

`after` accepts keys (strings) and ids (numbers). A batch is all-or-nothing.

## Status, results, cancellation

```sh
curl -s $CRONOQ/v1/tasks/summary:42 -H "$AUTH"
# {"id":17,"status":"running"}

curl -s "$CRONOQ/v1/tasks/17/result?timeout_s=60" -H "$AUTH"
# {"id":17,"status":"succeeded","output":{...},"output_b64":"..."}

curl -s -X DELETE $CRONOQ/v1/tasks/17 -H "$AUTH"
# {"cancelled":true}
```

A path segment that is all digits is an id; anything else is a key.

## Scheduled work

```sh
# Start in 10 minutes, skip it if it cannot start within 30 s of that time.
curl -s -X POST $CRONOQ/v1/tasks -H "$AUTH" -d '{
  "payload": {"report": "daily"}, "start_in_s": 600, "tolerance_s": 30, "on_miss": "skip"}'
```

Recurring schedules are managed through the gRPC `Admin` service (admin token).

## Operations

```sh
curl -s $CRONOQ/v1/snapshot -H "$AUTH"   # pools, capacity, waits, adherence, cost
curl -s $CRONOQ/metrics                  # Prometheus text (no auth)
curl -s $CRONOQ/healthz                  # process is up
curl -s $CRONOQ/readyz                   # 200 on the leader, 503 on standbys
```

## Errors

Errors are JSON `{"error": "..."}` with these status codes:

| Status | When |
|---|---|
| 400 | Invalid JSON, unknown priority/condition, invalid duration, cycle |
| 401 | Missing or invalid token |
| 403 | Token lacks the required role |
| 404 | Unknown task, pool, resource or dependency |
| 408 | Result wait timed out |
| 409 | Fixed-start reservation conflict |
| 429 | Queue full (overflow policy `reject`) |
| 503 | Shutting down, or store unavailable |

Full details: [HTTP API reference](../reference/http.md).
