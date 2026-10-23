# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, minor versions may
contain breaking changes; they are called out under **Changed**.

## [Unreleased]

## [0.1.0] - Unreleased

First public release.

### Added

#### Scheduling engine (`cronoq-core`)

- Sans-IO, deterministic engine: every input carries the current time, so the whole scheduler
  runs under virtual time in tests and simulations. Engine time is Unix time in nanoseconds.
- Four priority classes (critical, high, normal, low), FIFO within a class, and aging so that no
  class starves. Constant-time dispatch regardless of backlog size.
- Deadlines with a monotonic urgency curve and earliest-deadline-first ordering within a class.
- Fixed-start tasks: capacity reservations at admission (`ReservationConflict` when a pool's
  reservable share is full), worker holds with a lead time learned from the pool's run times
  (re-evaluated as the start approaches), zero-tolerance starts, and miss policies (`RunLate`,
  `Skip`, `Fail`).
- Cron schedules with seconds, time zones and daylight-saving handling, whole-second
  occurrences, overlap policies (`Skip`, `Allow`, and `Queue` / `QueueOnSuccess` to run
  occurrences one after another), deadlines relative to each occurrence, per-occurrence keys
  (`<key>@<start>`), and at most one late run after downtime.
- Dependencies by id or key with `on_success`, `on_failure` and `on_complete` edges, awaiting
  unknown keys with a timeout, cycle detection, atomic batches and idempotent keys.
- Leases with heartbeats, per-attempt timeouts, retries with exponential backoff, jitter and
  `retry_after`, dead-lettering and cancellation.
- What effectively-once side effects need: attempt numbers that never repeat for a task (the
  fencing token, also across failovers), lease ids that carry the leader epoch, stale reports
  ignored and counted (`stale_completions`), and key deduplication of finished tasks within the
  retention window.
- Shared semaphores and token buckets, with tasks waiting for a resource parked without holding
  a worker.
- Weighted fair sharing between tenants (`PoolConfig::fairness`, `TaskSpec::tenant`): within a
  priority class, tenants with waiting work get dispatches in proportion to their weights, with
  no banked credit for idle tenants (start-time fair queuing). Server pools take
  `tenant_weights` / `fair_share`, and tokens can pin a tenant.
- Worker labels: a pool's workers form groups by label set, a task can require labels and runs
  only in a matching group, waiting without blocking the pool while none is free; tasks without
  requirements prefer the least specialized group.
- Bounded queues per pool, per class and globally, with `Wait`, `Reject`, `ShedLowest` and
  `ShedOldest` overflow policies. Shedding considers parked tasks, never sheds fixed-start
  tasks, and only happens for submissions that pass every other admission check.
- Queue-pressure autoscaling per pool (arrival rate, backlog, wait-time objectives, upcoming
  reservations) with stabilization, cooldown, step limits and cost tracking.
- Windowed quality metrics: utilization, schedule efficiency, wait and run percentiles,
  priority adherence and fixed-start hit rate.
- Recovery from a durable store, including dependencies on parents that left retention and the
  recurring overlap guard. `CoreConfig::validate` reports configuration errors up front.

#### Runtime (`cronoq`)

- Tokio driver with an async API (`Scheduler`, `TaskHandle`, `submit`, `enqueue`,
  `enqueue_many`, `submit_batch`, `cancel`, `status`, `snapshot`, `events`) and a blocking
  facade (`BlockingScheduler`).
- Executors from closures or the `Executor` trait, with cancellation tokens, heartbeats and
  `TaskContext::for_test` for unit tests. Executor panics are isolated.
- Pluggable scalers (in-process slots, external) and stores.
- `CoreConfig::id_shard` puts a shard number in the top 8 bits of task ids (`TaskId::shard`), and
  a sharded scheduler refuses a store that holds another shard's tasks.
- Listing: `Scheduler::list_tasks` pages through live tasks by pool and status,
  `Scheduler::list_recurring` shows schedules with their next occurrence, and durable stores
  list finished tasks (`StoreQueries::list_finished`; `TaskRecord` carries the tenant).
- Durable acknowledgements: with a persistent store, submissions, cancellations and schedule
  changes return only after they are committed, and work is dispatched only once durable.
- Store error handling: transient outages are ridden out for up to `max_store_outage`
  (default 5 minutes), other errors are retried, fencing stops the scheduler immediately.
- Graceful shutdown with a grace period, `drop_grace` for dropped schedulers, a bounded output
  cache with `TaskFailure::OutputUnavailable`, and a configurable status-event buffer.
- OpenTelemetry metrics and W3C trace propagation (`otel` feature, on by default).

#### Postgres store (`cronoq-store-postgres`)

- Durable state with group commit, advisory-lock leader election, epoch fencing, session
  liveness checks with a configurable timeout, commit timeouts, output storage, lookups and
  pruning.
- Versioned migrations (`cronoq_schema_version`), a configurable schema per cluster, an id
  high-water mark so ids are never reused after pruning, NUL-safe text storage, and quarantine of
  undecodable rows instead of failed recoveries.

#### SQLite store (`cronoq-store-sqlite`)

- Durable state in one local file, with no database server: the same tables, migrations and
  recovery as the Postgres store, checked by the same conformance tests.
- Leader election through a renewed lease, so standby processes on the same host take over
  when the leader stops; every commit checks the fencing epoch inside its write transaction.
- WAL journal synced on every commit by default; the file is created with owner-only
  permissions.

#### Server (`cronoq-server`)

- gRPC (`Tasks`, `Workers`, `Admin`) and HTTP/JSON APIs. HTTP returns ids as strings, has
  separate routes for ids and keys, and rejects unknown fields.
- Remote workers over bidirectional streams, worker protocol version 3: granted slots, lease
  metadata, `Nack` hand-backs, `Cancelled` reports, `Shutdown` notices for restarts and
  leadership changes, and worker labels. Workers that speak an older version are refused.
- Worker tokens and certificate mappings can restrict which labels a worker may claim
  (`labels = ["name=value", ...]`).
- `Watch` with pool, id and key filters and optional intermediate transitions; richer `Get`
  results; waits bounded by `limits.max_wait_s`.
- Sharding: `[[shards]]` split the pools between shards, each with its own leader, standbys and
  store (a Postgres schema or SQLite file per shard, from one shared configuration), started
  with `--shard`. Task ids carry the shard's number. Work for another shard is refused with its
  name and endpoint; dependencies and batches across shards are refused. Each resource belongs
  to one shard (`[[resources]] shard`), so shards cannot exceed a shared limit together.
  Any server forwards calls for another shard to it with the caller's token (re-checked there;
  never over plain HTTP to another host unless `insecure_peer_http`), and listings, `Watch` and
  snapshots cover every shard, naming unreachable shards in `unavailable_shards`. The Rust and
  Python workers follow a refusal to their pool's shard, never from TLS to plain HTTP, and with
  a token over plain HTTP only to loopback unless `insecure_redirect_http` is set.
- gRPC-Web for browser clients on the gRPC listener, off unless `[server.grpc_web]` lists the
  allowed origins: unary calls and `Watch`, the same authentication, CORS only for those origins,
  and in loopback-only mode only loopback hosts.
- Listing: `ListTasks` / `GET /v1/tasks` pages through live and finished tasks by pool and
  status (pages capped by `limits.max_list`), and `ListWorkers` / `GET /v1/workers` and
  `ListRecurring` / `GET /v1/recurring` show connected workers and schedules. Each lists only
  the pools the caller's credentials allow.
- `limits.key_retention_s` and `key_retention_max` set how long finished keys deduplicate
  submissions; the same window bounds what a new leader recovers.
- Webhook and external scalers, Prometheus metrics, health and readiness endpoints, and
  active-passive HA with Postgres or SQLite.
- Rust worker client (`worker::run_worker`) with slot enforcement, heartbeats, panic isolation,
  reconnect with backoff, TLS/mTLS and graceful drain.
- `--check` to validate configuration, `--print-proto` to print the API.

#### Command-line tool (`cronoq-cli`)

- `cronoq` submits, inspects, cancels and waits for tasks, lists tasks, workers and schedules,
  manages schedules, streams status events, and prints JSON for scripts with `-o json`.
- `cronoq top` shows pools, workers, schedules, live tasks and recent status events, refreshed
  every second; panes the token's role cannot read say which role they need.
- The token comes from `CRONOQ_TOKEN` or `--token-file`, never the command line; TLS options
  are refused with an `http://` server. Control characters in text from the server are shown as
  escapes, so other clients' task keys or messages cannot drive the operator's terminal.

#### Python (`cronoq` on PyPI)

- Asyncio-native bindings for the engine with Python payloads (never serialized), sync and
  async executors, typed stubs and picklable exceptions.
- Status events (`Scheduler.events()`) with a bounded buffer, lag reporting and
  `on_lag="raise"`.
- `cronoq.compat.ElasticPriorityScheduler`, a synchronous drop-in for code written against the
  original API.
- Durable state with `Scheduler(..., store=SqliteStore(path))`: payloads and results are
  encoded with a codec (JSON by default, which never runs code when read back); several
  processes can share the file, with one leading and the others standing by.
- `cronoq.client`: gRPC client and remote worker speaking worker protocol version 3, with
  worker labels, and listing of tasks (`list_tasks`, `iter_tasks`), workers and schedules.
- Wheels for CPython 3.14+ (abi3) on Linux, macOS and Windows (x86_64 and arm64).

#### Project

- Documentation book with concepts, use cases, guides, a benefits and decision guide, and
  complete configuration, HTTP, gRPC, metrics, Python, Rust and error references.
- Examples, a distroless container image, CI with deterministic simulation, fuzzing and
  benchmark gates, wheel tests on every platform, and a release pipeline for crates.io, PyPI,
  GitHub releases and GHCR.

### Security

- Roles `client`, `worker`, `metrics` and `admin` for bearer tokens and for client
  certificates mapped by DNS name (`[[auth.certs]]`), optionally limited to pools. Tasks of other
  pools are reported as not found.
- Tokens must be at least 16 characters and unique; they are stored as SHA-256 digests,
  compared in constant time and never logged.
- Listeners bind to `127.0.0.1` by default; the server refuses to listen elsewhere without
  credentials unless `--insecure-no-auth` is given. Without credentials the HTTP API answers
  only requests for a loopback host, which stops DNS-rebinding web pages.
- Pool-scoped clients cannot reach tasks of other pools through batch submissions that reuse
  their keys, and lookups by key act on the task that was authorized.
- Recurring templates are checked against the configured pools and resources when added, so a
  bad template cannot crash the scheduler when it fires or after a restart.
- Task keys, kinds and schedule names are hashed with a randomly seeded hasher, so crafted keys
  cannot degrade the engine.
- The PostgreSQL store refuses existing cronoq tables owned by another role, and
  `store.schema` puts the server's tables in a dedicated schema.
- Workers refuse TLS settings with an `http://` endpoint instead of connecting in plaintext,
  and mark their bearer token as sensitive.
- TLS on both the gRPC and HTTP listeners, with optional mTLS (`client_ca`).
- `/metrics` and `/v1/snapshot` require the metrics role; an optional unauthenticated
  `metrics_addr` listener serves scrapers on an internal network.
- `[limits]` bound batch sizes, queue lengths, message and body sizes, strings and collections,
  waits, request time, concurrent connections and streams, and worker slots; slow TLS
  handshakes and headers time out.
- Database errors are logged rather than returned to clients; messages from workers are
  sanitized; the server warns about database URLs without `sslmode=verify-full`.

[Unreleased]: https://github.com/synaptiai/cronoq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/synaptiai/cronoq/releases/tag/v0.1.0
