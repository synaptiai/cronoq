# Sharding

One server leads all pools by default, with standbys taking over when it stops
([Durability and high availability](durability.md)). When one leader is not enough, or pools
should fail independently, split the pools into **shards**: each shard has its own leader,
standbys and store, and shards keep serving when another one is down.

```toml
[[pools]]
name = "default"
[[pools]]
name = "llm"

[[shards]]
name = "cpu"
number = 1
pools = ["default"]
endpoint = "https://cronoq-cpu.internal:7070"

[[shards]]
name = "gpu"
number = 2
pools = ["llm"]
endpoint = "https://cronoq-gpu.internal:7070"

[store]
kind = "postgres"
url_env = "DATABASE_URL"   # each shard uses the schema cronoq_<shard>
```

All servers read the same file and are started for one shard each:

```sh
cronoq-server --config cronoq.toml --shard cpu   # 2+ replicas for failover
cronoq-server --config cronoq.toml --shard gpu
```

## What a shard is

- **Its own pools.** A server runs only its shard's pools.
- **Its own resources.** Shards do not share counts, so each `[[resources]]` entry names the
  shard that owns it (`shard = "gpu"`), and only that shard's tasks may use it; a task of
  another shard asking for it is refused. To share an outside limit between shards, split it
  into one resource per shard with its share: `api_cpu` with 6 calls per second in `cpu`,
  `api_gpu` with 4 in `gpu`. `[limits]`, such as `max_pending_total`, apply to each shard
  separately.
- **Its own store.** Postgres keeps each shard in its own schema (`store.schema` must contain
  `{shard}` and defaults to `cronoq_{shard}`); SQLite in its own file (`store.path` must contain
  `{shard}`). A server refuses to start on a store that holds another shard's tasks. Start a
  sharded deployment on new stores: an unsharded server's tasks are not always recognised, and
  moving them into a shard is not supported.
- **Its own ids.** Task ids carry the shard's `number` in their top 8 bits, so ids never collide
  between shards, and any server can tell which shard a task belongs to.
- **Its own keys.** Keys deduplicate within a shard; two shards can each have a task with the
  same key.

## Calling any server

Clients can call the server of any shard. It answers from every shard that the call concerns:

- **Work for another shard is forwarded** to it: submissions and batches for its pools,
  lookups, waits and cancellations of its tasks, and schedules for its pools. Lookups by key
  ask every shard, so the answer does not depend on which server receives the call: a key that
  names tasks in two shards is refused (`FAILED_PRECONDITION`, use the task id). While a shard
  cannot be asked, `Get` and `Wait` by key still find a task that one reachable shard holds and
  name the missing shards (`cronoq-unavailable-shards` metadata; `unavailable_shards` over
  HTTP), but `Cancel` by key is refused with `UNAVAILABLE`, since the missing shard might hold
  the same key. Keys are unique only within a shard, so prefer ids for anything that changes
  a task, or keep each key's tasks in one shard's pools.
- **Reads cover every shard:** task listings (paged across shards in id order), `Watch`,
  worker and schedule listings, and snapshots. When a shard cannot be reached, the others still
  answer, and the response names the missing shard in `unavailable_shards`. A `Watch` ends when
  any shard's stream ends; reconnect. Prometheus `/metrics` stays per server: scrape each shard.

A forwarded call carries the caller's own bearer token, and the other shard authenticates and
authorizes it again, so forwarding never grants more than the caller's own credentials. Each
shard's server sees the tokens of the callers it forwards; protect every shard's servers as
you would one server. Forwarded calls are marked
(`cronoq-forwarded`) and answered by the receiving shard only, so they never travel further.

Configure how servers reach each other's `https://` endpoints with `[server.peer_tls]`
(`ca`, and `cert` / `key` for shards that require mTLS); the system roots are the default.

### When a call is not forwarded

Some calls are refused instead, with `FAILED_PRECONDITION` (HTTP 409) naming the shard and its
endpoint in the message and in the response metadata (`cronoq-shard`,
`cronoq-shard-endpoint`), so the caller can go there directly:

- **Client certificates.** A caller that authenticated with a certificate has no token to
  forward. Its reads cover only the receiving server's shard (the others are listed as
  unavailable), and its `Watch` streams only that shard's events.
- **Plain `http://` to another host.** Tokens are not sent unencrypted across the network:
  calls for a shard whose endpoint is `http://` on another host are refused with its endpoint,
  unless `server.insecure_peer_http = true` (trusted networks only). Loopback endpoints and
  `https://` endpoints are always used.
- **Workers** register with the shard of their pool. A worker that connects to another
  shard's server is refused with the endpoint to use, and the Rust, Python and Go workers connect
  there on their own (up to three redirects in a row). They keep their TLS settings, never
  follow a redirect from `https://` to `http://`, and send their token over plain `http://`
  only to loopback unless the worker allows it (`insecure_redirect_http`, trusted networks
  only). When that shard cannot be reached, the worker asks its configured endpoint again, so
  a load balancer in front of all shards works as the worker's endpoint.

Credentials are checked first: a caller that may not use a pool learns nothing about where it
is served.

### What cannot span shards

- **Dependencies** on another shard's task (`INVALID_ARGUMENT`): a task can only wait for tasks
  of its own shard. Dependencies by key refer to keys of the task's own shard.
- **Batches** with tasks of several shards (`INVALID_ARGUMENT`): a batch is admitted atomically
  by one shard.
- **Resources** of another shard (`INVALID_ARGUMENT`).
