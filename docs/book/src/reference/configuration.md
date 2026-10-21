# Server configuration

`cronoq-server` reads one TOML file, given with `--config <path>` or the `CRONOQ_CONFIG`
environment variable. Without a file it starts with defaults: one `default` pool, in-memory
state, and authentication disabled. Unknown keys are rejected, so typos fail at startup.

An annotated example lives in
[`deploy/cronoq.example.toml`](https://github.com/synaptiai/cronoq/blob/main/deploy/cronoq.example.toml).
Print the gRPC API definition with `cronoq-server --print-proto`.

## `[server]`

| Key | Type | Default | Description |
|---|---|---|---|
| `grpc_addr` | socket address | `"0.0.0.0:7070"` | gRPC listener (`Tasks`, `Workers`, `Admin`). Port `0` picks a free port. |
| `http_addr` | socket address | `"0.0.0.0:7071"` | HTTP listener (`/v1/*`, `/metrics`, `/healthz`, `/readyz`). |
| `shutdown_grace_s` | float | `10` | On shutdown, running attempts may finish for this long before they are aborted. |

### `[server.tls]`

Optional. Enables TLS on the gRPC listener.

| Key | Type | Description |
|---|---|---|
| `cert` | path | PEM certificate chain. |
| `key` | path | PEM private key. |
| `client_ca` | path, optional | Require client certificates signed by this CA (mutual TLS). |

## `[auth]`

```toml
[[auth.tokens]]
token_env = "CRONOQ_CLIENT_TOKEN"
role = "client"
```

| Key | Type | Description |
|---|---|---|
| `token` | string | The bearer token (prefer `token_env`). |
| `token_env` | string | Read the token from this environment variable. |
| `role` | `"client"` \| `"worker"` \| `"admin"` | What the token may do (see [Security](../guides/security.md)). |

Each entry needs `token` or `token_env`. With no tokens configured, authentication is disabled.

## `[store]`

| Key | Type | Default | Description |
|---|---|---|---|
| `kind` | `"memory"` \| `"postgres"` | `"memory"` | Where state lives. |
| `url` | string | — | Postgres connection URL (prefer `url_env`). |
| `url_env` | string | `"DATABASE_URL"` | Environment variable holding the URL. |
| `keep_finished_s` | float | `604800` (7 days) | Finished tasks older than this are pruned (hourly, by the leader). |

With `postgres`, several servers may share the database; one leads and the others stand by. See
[Durability and HA](../concepts/durability.md).

## `[[pools]]`

Defines the pools. Every server pool is served by remote workers; its capacity is the sum of
connected workers' slots. Default: a single pool named `default`.

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Pool name; tasks select it with `pool`. |
| `min` | integer | `0` | Lower bound for the desired capacity. |
| `max` | integer | `10` | Upper bound for the desired capacity; also sizes reservations. |
| `autoscale` | bool | `true` | Compute a desired capacity (see [autoscaling](../concepts/autoscaling.md)). |
| `scaler` | table | `{ kind = "external" }` | How the desired capacity is communicated (below). |
| `lease_ttl_s` | float | `30` | Workers must heartbeat each lease within this many seconds. |
| `max_pending` | integer | unbounded | Maximum pending tasks in the pool. |
| `overflow` | `"wait"` \| `"reject"` \| `"shed_lowest"` \| `"shed_oldest"` | `"reject"` | What to do when `max_pending` is reached ([backpressure](../concepts/backpressure.md)). |
| `retry` | table | 3 attempts | Default retry policy (below). |
| `default_duration_s` | float | `1` | Duration estimate before any task has completed. |
| `reservable_fraction` | float | `0.5` | Fraction of `max` that fixed-start reservations may occupy at once. |
| `await_unknown_dependencies_s` | float | reject | Let tasks depend on keys that do not exist yet, waiting up to this long. |
| `target_utilization` | float | `0.75` | Autoscaler steady-state utilization. |
| `drain_time_s` | float | `30` | Autoscaler backlog drain target. |
| `cooldown_s` | float | `30` | Minimum time between scale-down steps. |
| `cost_per_worker_second` | float | `0` | For cost reporting. |

### `scaler`

| Form | Behaviour |
|---|---|
| `{ kind = "external" }` | Export `cronoq_pool_desired_workers{pool}`; an external system (KEDA, HPA, your controller) starts workers. |
| `{ kind = "webhook", url = "...", token_env = "..." }` | POST `{"pool": "<name>", "desired": <n>}` to `url` (with an optional bearer token) whenever the target changes. Capacity still follows connected workers. |

### `retry`

| Key | Type | Default |
|---|---|---|
| `max_attempts` | integer | required (≥ 1) |
| `initial_backoff_s` | float | `1` |
| `multiplier` | float | `2` |
| `max_backoff_s` | float | `60` |
| `jitter` | bool | `true` |

## `[[resources]]`

```toml
[[resources]]
kind = "token_bucket"
name = "openai"
rate = 50     # units per second
burst = 100

[[resources]]
kind = "semaphore"
name = "gpu"
permits = 8
```

| Kind | Keys |
|---|---|
| `semaphore` | `name`, `permits` |
| `token_bucket` | `name`, `rate` (per second), `burst` |

## `[outputs]`

| Key | Type | Default | Description |
|---|---|---|---|
| `cache_entries` | integer | `10000` | Recent outputs kept in memory for `Wait` / `GET .../result`. |
| `cache_bytes` | integer | `67108864` (64 MiB) | Byte budget for the cache; larger outputs are not cached. |

With the Postgres store, outputs evicted from the cache are read from the database.

## Environment variables

| Variable | Purpose |
|---|---|
| `CRONOQ_CONFIG` | Config file path (same as `--config`). |
| `RUST_LOG` | Log filter, e.g. `info` or `info,cronoq=debug`. |
| `DATABASE_URL` | Default source of the Postgres URL. |
| any `token_env` / `url_env` / scaler `token_env` | Secrets referenced from the config. |
