# Server configuration

`cronoq-server` reads one TOML file, given with `--config <path>` (or the `CRONOQ_CONFIG`
environment variable). Without a file it starts with defaults: one `default` pool, in-memory
state, no auth tokens and listeners on `127.0.0.1` only. Unknown keys are rejected and every
value is validated at startup, so typos and out-of-range numbers fail fast instead of at
runtime.

```sh
cronoq-server --config cronoq.toml --check   # validate and exit
cronoq-server --config cronoq.toml           # run
cronoq-server --print-proto                  # print the gRPC API definition
```

| Flag | Description |
|---|---|
| `-c, --config <path>` | TOML file (env `CRONOQ_CONFIG`). |
| `--check` | Validate the configuration and exit (`configuration is valid`). |
| `--insecure-no-auth` | Allow running without auth on non-loopback addresses (same as `server.insecure_no_auth`). Trusted networks only. |
| `--print-proto` | Print `cronoq/v1/cronoq.proto` and exit. |

The log level comes from `RUST_LOG` (default `info`).

An annotated example lives in
[`deploy/cronoq.example.toml`](https://github.com/synaptiai/cronoq/blob/main/deploy/cronoq.example.toml);
it is the configuration baked into the container image.

Durations are seconds (floats). They must be finite and between their minimum and 10 years.

## `[server]`

| Key | Type | Default | Description |
|---|---|---|---|
| `grpc_addr` | socket address | `"127.0.0.1:7070"` | gRPC listener (`Tasks`, `Workers`, `Admin`). Port `0` picks a free port. |
| `http_addr` | socket address | `"127.0.0.1:7071"` | HTTP listener (`/v1/*`, `/metrics`, `/healthz`, `/readyz`). |
| `metrics_addr` | socket address, optional | unset | Extra **unauthenticated** listener serving only `/metrics`, `/healthz` and `/readyz`. Bind it to an internal network for scrapers. Without it, `/metrics` on `http_addr` needs a `metrics` or `admin` token. |
| `insecure_no_auth` | bool | `false` | Allow starting with no tokens and no certificate mappings while listening on a non-loopback address. Without it the server refuses to start in that situation. |
| `shutdown_grace_s` | float ≥ 0 | `10` | On shutdown (or loss of leadership), running attempts may finish for this long before they are aborted. |

### `[server.tls]`

Optional. Enables TLS on **both** the gRPC and the HTTP listener (the optional metrics listener
stays plain HTTP). Files are read once at startup; a missing file is reported with its path.

| Key | Type | Description |
|---|---|---|
| `cert` | path | PEM certificate chain. |
| `key` | path | PEM private key. |
| `client_ca` | path, optional | Require client certificates signed by this CA on both listeners (mutual TLS). Needed for `[[auth.certs]]`. |

## `[auth]`

Callers authenticate with `authorization: Bearer <token>` (gRPC metadata or HTTP header), or
with a client certificate mapped to a role. Roles:

| Role | May |
|---|---|
| `client` | Submit, inspect, wait for, cancel and watch tasks (`Tasks` service, `/v1/tasks*`). |
| `worker` | Open the `Workers.Work` stream and serve leases. |
| `metrics` | Read snapshots and metrics (`Admin.Snapshot`, `/v1/snapshot`, `/metrics`). |
| `admin` | Everything, including recurring schedules (`Admin.AddRecurring`, `Admin.RemoveRecurring`). |

`/healthz` and `/readyz` never need credentials.

### `[[auth.tokens]]`

```toml
[[auth.tokens]]
token_env = "CRONOQ_CLIENT_TOKEN"
role = "client"
pools = ["llm"]            # optional
```

| Key | Type | Description |
|---|---|---|
| `token` | string | The bearer token. Prefer `token_env`. |
| `token_env` | string | Read the token from this environment variable (it must be set at startup). |
| `role` | `client` \| `worker` \| `metrics` \| `admin` | What the token may do. |
| `pools` | list of pool names, optional | Restrict a `client` or `worker` token to these pools. Tasks in other pools behave as if they did not exist (`NOT_FOUND`), `Watch` filters them out, and workers may only register for these pools. Submitting a key already used in another pool fails with `ALREADY_EXISTS`, and dependencies on tasks of other pools are refused. |
| `labels` | list of `"name=value"`, optional | Labels a `worker` token may claim when it registers; a worker claiming any other label is refused (`PERMISSION_DENIED`). Without it, a worker may claim any labels. See [Worker labels](../concepts/labels.md). |

Exactly one of `token` / `token_env` is required. Tokens must be at least **16** characters
(the server warns below 32; use 32+ random characters, e.g. `openssl rand -hex 24`) and
unique. Tokens are stored as SHA-256 digests and compared in constant time; `Debug` output and
logs never contain them.

### `[[auth.certs]]`

Map mTLS client certificates to roles by an exact DNS subject alternative name. Requires
`server.tls.client_ca`.

```toml
[[auth.certs]]
dns_name = "worker-1.internal"
role = "worker"
pools = ["gpu"]            # optional
```

| Key | Type | Description |
|---|---|---|
| `dns_name` | string | DNS SAN to match exactly. |
| `role` | role | Role granted to that certificate. |
| `pools` | list, optional | Pool allow-list, as for tokens. |
| `labels` | list, optional | Worker-label allow-list, as for tokens. |

A bearer token, when present, takes precedence over the certificate.

## `[store]`

```toml
[store]
kind = "postgres"
url_env = "DATABASE_URL"
keep_finished_s = 604800
```

| Key | Type | Default | Description |
|---|---|---|---|
| `kind` | `memory` \| `postgres` \| `sqlite` | `memory` | `memory` keeps all state in the process (lost on restart). `postgres` and `sqlite` make state durable and enable active-passive HA (`sqlite`: processes on one host). |
| `url` | string | – | `postgres` only. Connection URL. Prefer `url_env`; the server warns about inline URLs and about URLs without `sslmode=verify-full`/`require`. |
| `url_env` | string | `DATABASE_URL` | `postgres` only. Read the URL from this environment variable. Set `url` or `url_env`, not both. |
| `keep_finished_s` | float ≥ 60 | `604800` (7 days) | Finished tasks older than this are pruned hourly (in batches; tasks that live tasks depend on are kept). |
| `path` | path | – | `sqlite` only, required: the database file (created if missing, owner-only permissions). Keep it on a local filesystem. |
| `schema` | string | `public` | `postgres` only. PostgreSQL schema for the server's tables (letters, digits and `_`; created if missing). Servers sharing a schema form one cluster; different schemas are independent. |

The server refuses to start if the schema already holds cronoq tables owned by a role other than
the one it connects as (or a superuser). Prefer a dedicated schema that only the server's role
can create objects in (see [Security hardening](../guides/security.md)).

## `[limits]`

Request and resource limits. The defaults are safe for a shared deployment; every key is
optional.

| Key | Type | Default | Description |
|---|---|---|---|
| `max_tasks_per_batch` | int ≥ 1 | `1000` | Tasks per `SubmitBatch` / `POST /v1/tasks:batch`. |
| `max_pending_total` | int ≥ 1 | `1000000` | Pending (not yet running) tasks across all pools. |
| `max_pending_per_pool` | int ≥ 1 | `100000` | Default pending bound per pool (override with `pools.max_pending`). |
| `max_message_bytes` | int ≥ 4096 | `16777216` (16 MiB) | Largest gRPC message accepted or sent. Payloads and worker outputs travel in these; a worker output above it fails the attempt permanently instead of dropping the worker. |
| `max_http_body_bytes` | int ≥ 4096 | `16777216` | Largest HTTP request body. |
| `max_key_len` | int ≥ 1 | `256` | Longest task key. |
| `max_kind_len` | int ≥ 1 | `128` | Longest task kind. |
| `max_tags` | int | `32` | Most tags per task. |
| `max_tag_len` | int ≥ 1 | `1024` | Longest tag key or value. |
| `max_deps` | int | `256` | Most dependencies per task. |
| `max_resources` | int | `16` | Most resource entries per task. |
| `max_wait_s` | float ≥ 0 | `300` | Longest server-side wait for a result (`Wait`, `Submit{wait}`, `GET …/result`). Longer requested waits are capped; when it expires the caller gets `DEADLINE_EXCEEDED` / HTTP 504 and can wait again. |
| `request_timeout_s` | float ≥ 0.1 | `30` | Timeout for any other request (waiting requests get `request_timeout_s + max_wait_s`). |
| `max_concurrent_requests` | int ≥ 1 | `4096` | Requests processed concurrently per listener (excess requests queue). Also caps open HTTP connections. |
| `max_concurrent_streams` | int ≥ 1 | `1024` | HTTP/2 streams per connection. |
| `max_slots_per_worker` | int ≥ 1 | `1024` | Slots a single worker may register (also capped by the pool's `max`). |
| `keepalive_interval_s` | float ≥ 0.1 | `20` | HTTP/2 keepalive ping interval; detects half-open workers and clients. |
| `keepalive_timeout_s` | float ≥ 0.1 | `20` | HTTP/2 keepalive ping timeout. |
| `key_retention_s` | float ≥ 0 | `3600` | How long the key of a finished task keeps deduplicating submissions (a resubmission returns the finished task and its output), and how far back a new leader loads finished tasks from the store. |
| `key_retention_max` | int | `1000000` | Most finished tasks kept for key deduplication (bounds memory). |

Strings in task specs (keys, kinds, tags, resource names, worker messages) must not contain
NUL characters; worker failure messages are sanitized (NUL replaced, cut at 8 KiB) instead of
rejected.

## `[[pools]]`

Server pools are served by remote workers: a pool's capacity is the number of slots its
connected workers registered. Defaults to a single pool named `default`.

```toml
[[pools]]
name = "llm"
max = 200
lease_ttl_s = 120
overflow = "reject"
retry = { max_attempts = 5, initial_backoff_s = 2, max_backoff_s = 120 }
```

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | 1–64 characters from `[A-Za-z0-9_.:-]`, unique. |
| `min` | int | `0` | Autoscaler lower bound. |
| `max` | int ≥ 1 | `10` | Autoscaler upper bound, basis for reservation capacity, and a cap on slots per worker. |
| `autoscale` | bool | `true` | Compute a desired capacity from queue pressure (exported as `cronoq_pool_desired_workers` and sent to the scaler). |
| `scaler` | table | `{ kind = "external" }` | How the desired capacity is communicated, see below. |
| `lease_ttl_s` | float ≥ 0.1 | `30` | Workers must heartbeat each lease within this many seconds, or the attempt is treated as lost and retried. |
| `max_pending` | int ≥ 1 | `limits.max_pending_per_pool` | Pending-task bound for this pool. |
| `overflow` | `wait` \| `reject` \| `shed_lowest` \| `shed_oldest` | `reject` | Behaviour when the bound is reached (see [Backpressure](../concepts/backpressure.md)). `reject` answers `RESOURCE_EXHAUSTED` / HTTP 429. |
| `retry` | table | engine default (3 attempts, 1 s ×2 up to 60 s, jitter) | Default retry policy for the pool, see below. |
| `default_duration_s` | float ≥ 0 | `1` | Duration estimate before any run time has been observed. |
| `reservable_fraction` | float in (0, 1] | `0.5` | Share of `max` that overlapping fixed-start reservations may occupy (at least one slot). |
| `await_unknown_dependencies_s` | float ≥ 0 | unset | Allow dependencies on keys that do not exist yet; such tasks wait up to this long, then end `skipped`. Unset: unknown keys are rejected. |
| `target_utilization` | float in (0, 1] | `0.75` | Autoscaler utilization target. |
| `drain_time_s` | float > 0 | `30` | Autoscaler: clear the backlog within this time. |
| `cooldown_s` | float ≥ 0 | `30` | Autoscaler: minimum time between scale-down steps. |
| `cost_per_worker_second` | float ≥ 0 | `0` | Cost per slot-second, for `cronoq_pool_cost_total`. |
| `max_slots_per_worker` | int ≥ 1 | `limits.max_slots_per_worker` | Slots one worker may register in this pool (never above `max`). Workers asking for more are granted the cap. |

### `scaler`

| Form | Description |
|---|---|
| `{ kind = "external" }` | Capacity follows connected workers; use the desired-workers metric with KEDA/HPA or your own controller. |
| `{ kind = "webhook", url = "https://…", token_env = "SCALER_TOKEN" }` | `POST {"pool": "...", "desired": n}` to `url` whenever the target changes. `token_env` (optional) must be set if given; it is sent as a bearer token. HTTPS certificates are verified against the operating system's trust store. The server warns about `http://` URLs. |

### `retry`

| Key | Type | Default | Description |
|---|---|---|---|
| `max_attempts` | int ≥ 1 | required | Attempts including the first. |
| `initial_backoff_s` | float ≥ 0 | `1` | First backoff. |
| `multiplier` | float in [1, 1000] | `2` | Backoff growth factor. |
| `max_backoff_s` | float ≥ 0 | `60` | Backoff ceiling. |
| `jitter` | bool | `true` | Full jitter (uniform in `[0, backoff]`). |

## `[[resources]]`

Shared limits that tasks request by name (`resources = {"openai": 1}`).

```toml
[[resources]]
kind = "token_bucket"
name = "openai"
rate = 50
burst = 100

[[resources]]
kind = "semaphore"
name = "gpu"
permits = 8
```

| Key | Kinds | Description |
|---|---|---|
| `kind` | – | `semaphore` or `token_bucket`. |
| `name` | both | 1–64 characters from `[A-Za-z0-9_.:-]`, unique. |
| `permits` | `semaphore` | Units held concurrently (≥ 1); released when each attempt ends. |
| `rate` | `token_bucket` | Refill per second (> 0). |
| `burst` | `token_bucket` | Bucket size (≥ 1). |

## `[outputs]`

Recent outputs and task records, used by `Wait`/`Get` after a task finished. With Postgres,
outputs that are no longer cached are read from the database.

| Key | Type | Default | Description |
|---|---|---|---|
| `cache_entries` | int | `10000` | Most outputs kept in memory (`0` disables the cache). |
| `cache_bytes` | int | `67108864` (64 MiB) | Most output bytes kept in memory. Must be positive while the cache is enabled. |
| `info_entries` | int | `100000` | Most task records kept for `Get` (pool, kind, attempts, timestamps, last error). |

## Validation summary

`--check` (and every startup) rejects:

- unknown keys, duplicate pool or resource names, invalid names;
- non-finite, negative or out-of-range numbers (durations, fractions, multipliers, rates);
- `[[auth.tokens]]` entries without exactly one of `token`/`token_env`, duplicate literal
  tokens, tokens shorter than 16 characters, pool allow-lists naming unknown pools;
- `[[auth.certs]]` without `server.tls.client_ca`;
- both `store.url` and `store.url_env`;
- non-loopback listeners with no credentials, unless `insecure_no_auth` is set.
