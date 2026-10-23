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

- **Its own pools.** A server runs only its shard's pools. `[limits]` and `[[resources]]` apply
  per shard.
- **Its own store.** Postgres keeps each shard in its own schema (`store.schema` must contain
  `{shard}` and defaults to `cronoq_{shard}`); SQLite in its own file (`store.path` must contain
  `{shard}`). A server refuses to start on a store that holds another shard's tasks.
- **Its own ids.** Task ids carry the shard's `number` in their top 8 bits, so ids never collide
  between shards, and any server can tell which shard a task belongs to.
- **Its own keys.** Keys deduplicate within a shard; two shards can each have a task with the
  same key.

## Work for another shard

A server refuses requests that belong to another shard with `FAILED_PRECONDITION` (HTTP 409),
naming the shard and its endpoint in the message and in the response metadata
(`cronoq-shard`, `cronoq-shard-endpoint`):

- tasks submitted to, listed or watched in another shard's pool;
- lookups, waits and cancellations by an id of another shard;
- recurring schedules for another shard's pool;
- workers registering for another shard's pool.

Credentials are checked first: a caller that may not use a pool learns nothing about where it
is served. Point clients and workers at the endpoint of their pools' shard.

Some requests cannot span shards and are refused with `INVALID_ARGUMENT` or the error above:

- **Dependencies** on another shard's task: a task can only wait for tasks of its own shard.
  Dependencies by key refer to keys of the task's own shard.
- **Batches** with tasks of several shards: a batch is admitted atomically by one shard.
