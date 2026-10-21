# Rust API

The complete API documentation is on docs.rs. This page maps the crates and their main types,
so you know where to look.

| Crate | docs.rs | Use it for |
|---|---|---|
| `cronoq` | [docs.rs/cronoq](https://docs.rs/cronoq) | Embedding the scheduler in a tokio application. Start here. |
| `cronoq-core` | [docs.rs/cronoq-core](https://docs.rs/cronoq-core) | The sans-IO engine: custom drivers, simulations, deterministic tests. |
| `cronoq-store-postgres` | [docs.rs/cronoq-store-postgres](https://docs.rs/cronoq-store-postgres) | Durable state and leader election for `Scheduler<Bytes, Bytes>`. |
| `cronoq-proto` | [docs.rs/cronoq-proto](https://docs.rs/cronoq-proto) | Generated gRPC clients and servers (`cronoq_proto::v1`), `PROTOCOL_VERSION`. |
| `cronoq-server` | [docs.rs/cronoq-server](https://docs.rs/cronoq-server) | The server binary, and `worker::run_worker` for Rust workers. |

MSRV: Rust 1.85 (edition 2024) for every crate.

## `cronoq`

### Building a scheduler

`Scheduler::builder()` returns a `Builder<P, O>` (`P` = payload type, `O` = output type).

| `Builder` method | Default | Description |
|---|---|---|
| `pool(Pool)` | – | Add a pool (at least one; the first is the default pool). |
| `resource(ResourceConfig)` | – | Add a shared semaphore or token bucket. |
| `priority(PriorityConfig)` | built-in | Class scores and aging. |
| `urgency(UrgencyConfig)` | built-in | Deadline urgency curve. |
| `store(impl Store)` / `store_arc(Arc<dyn Store>)` | `MemoryStore` | Persistence backend. |
| `emit_transitions(bool)` | `false` | Broadcast every transition on `events()`, not only terminal ones. |
| `queue_capacity(n)` | 8192 | Capacity of the submission channel. |
| `events_capacity(n)` | 4096 | Broadcast buffer of `events()`; slow subscribers lose the oldest events. |
| `max_store_outage(Duration)` | 5 min | How long a store may report `StoreError::Unavailable` before the scheduler stops. |
| `drop_grace(Duration)` | 5 s | Grace for running attempts when every handle is dropped without `shutdown`. |
| `output_cache(n)` | 1024 | Recent outputs kept for handles attached after success (0 disables). |
| `configure(FnOnce(&mut CoreConfig))` | – | Engine-wide settings: bounds, retention, metrics window, seed. |
| `build().await` | – | Start on the current tokio runtime (waits for leadership with a persistent store). |
| `build_blocking()` | – | Start a `BlockingScheduler` on its own runtime thread. |

`Pool<P, O>`:

| Method | Description |
|---|---|
| `Pool::new(name)` | Autoscaled pool (1 to 10 workers by default). |
| `Pool::fixed(name, n)` | Fixed size, no autoscaling. |
| `workers(min, initial, max)` | Capacity bounds. |
| `executor(impl Executor)`, `executor_arc(..)`, `executor_fn(async fn)` | How attempts run. |
| `scaler(impl Scaler)`, `scaler_arc(..)` | Capacity backend (default `SlotScaler`: in-process slots). |
| `autoscale(AutoscaleConfig)`, `no_autoscale()` | Autoscaling policy. |
| `configure(FnOnce(&mut PoolConfig))` | Every other pool setting (bounds, overflow, retry, lease TTL, max hold, ...). |

### `Scheduler<P, O>`

A cheap-to-clone handle. Every method is `async` and returns `Result<_, Error>`.

| Method | Returns | Description |
|---|---|---|
| `submit(task)` | `TaskHandle<O>` | Submit and get an awaitable handle. Waits for queue space with the `Wait` overflow policy. |
| `enqueue(task)` | `Submitted` | Submit without a handle (cheaper; `id`, `duplicate`). |
| `enqueue_many(tasks)` | `Vec<Result<Submitted, Error>>` | Independent submissions in one round trip. |
| `submit_batch(tasks)` | `Vec<TaskHandle<O>>` | Atomic batch; tasks may depend on each other by key. |
| `handle(target)` | `Option<TaskHandle<O>>` | Attach to an existing task by id or key. |
| `cancel(target)` | `bool` | `false` if the task already finished. |
| `status(target)` | `Option<TaskStatus>` | `None` for unknown tasks. |
| `snapshot()` | `Snapshot` | Metrics and quality signals. |
| `set_capacity(pool, n)` | `()` | Report capacity for pools with an `ExternalScaler`. |
| `add_recurring(Recurring)` / `remove_recurring(name)` | `()` / `bool` | Cron schedules. |
| `events()` | `broadcast::Receiver<StatusEvent>` | Status events (not async). |
| `pools()`, `clock()` | `&[Arc<str>]`, `&Clock` | Pool names; the engine clock. |
| `stopped()` | – | Resolves when the scheduler stops for any reason. |
| `shutdown(grace)` | `()` | Stop admitting, let running attempts finish for `grace`, then stop. |

With a persistent store, `submit`, `enqueue`, `submit_batch`, `cancel`, `add_recurring` and
`remove_recurring` return only after the change is durable.

`TaskHandle<O>` is a `Future<Output = Result<Arc<O>, TaskFailure>>` with `id()`,
`is_duplicate()` and `cancel()`.

### Tasks and schedules

`Task<P>`: `Task::new(payload)` (or `from_arc`), then `priority`, `pool`, `kind`, `key`,
`deadline_in` / `deadline_at`, `start_in` / `start_at`, `start_tolerance`, `on_miss`,
`duration_hint`, `timeout`, `retry`, `no_retry`, `after(target)`, `dep(Dep)`,
`resource(name, units)`, `tag(key, value)`. Builder order does not matter. Strings must not
contain NUL characters.

`Recurring<P>`: `Recurring::new(name, cron, template)`, then `tz`, `overlap(OverlapPolicy)`,
`tolerance`, `on_miss`. The template may not set a deadline, fixed start, key or dependencies.

### Executors

| Type | Description |
|---|---|
| `Executor<P, O>` | Trait: `execute(ctx, Arc<P>) -> BoxFuture<Result<O, TaskError>>`, optional `abort(lease)`. |
| `FnExecutor` | Wraps an `async fn(TaskContext, Arc<P>)`; what `Pool::executor_fn` uses. |
| `TaskContext` | `id`, `lease`, `attempt`, `pool`, `kind`, `key`, `tags`, `deadline`, `timeout`; `is_cancelled()`, `cancelled().await`, `cancellation_token()`, `heartbeat()`, `tag(key)`, `traceparent()`; `TaskContext::for_test(id)` for unit tests. |
| `TaskError` | `Retryable { message, retry_after }`, `Permanent { message }`, `Cancelled`; constructors `retryable`, `retry_after`, `permanent`, `from_err`. |

A panicking executor fails its attempt permanently (`panic: ...`); the scheduler and other
attempts keep running.

### Errors

| Type | Variants |
|---|---|
| `Error` | `Submit(SubmitError)`, `Shutdown`, `Config(String)`, `Store(StoreError)`, `Internal(String)`. |
| `TaskFailure` | `Ended { status, message }`, `OutputUnavailable { id }`, `Shutdown`; `status()`. |
| `StoreError` | `Fenced`, `Unavailable(String)`, `Other(String)`; `is_transient()`. |
| `ScaleError` | `External`, `Other(String)`. |

See [Errors and status codes](errors.md).

### Capacity and persistence

| Type | Description |
|---|---|
| `Scaler` | Trait: turn a desired capacity into workers. |
| `SlotScaler` | Default: capacity is a number of in-process concurrent attempts. |
| `ExternalScaler` | Capacity is managed elsewhere and reported with `set_capacity`. |
| `Store<P, O>` | Trait: `acquire_leadership`, `release_leadership`, `commit`, `recover`, `load_output`. |
| `MemoryStore` | No persistence (default). |
| `TimeBase` | Wall-clock anchor of engine time; the runtime always uses Unix-epoch time (`unix_nanos_at_zero = 0`). |

### Blocking facade

`BlockingScheduler<P, O>` (from `Builder::build_blocking`) has the same operations without
`async`: `submit`, `enqueue`, `enqueue_many`, `submit_batch`, `task_handle`, `cancel`, `status`,
`snapshot`, `set_capacity`, `add_recurring`, `remove_recurring`, `events`, `scheduler`,
`shutdown(grace)`. `BlockingHandle<O>` has `id()`, `cancel()`, `wait()` and
`wait_timeout(d)`.

### Re-exports and features

Engine types are re-exported from `cronoq-core`: `AutoscaleConfig`, `Bounds`, `Dep`,
`EdgeCond`, `MissPolicy`, `OverflowPolicy`, `OverlapPolicy`, `PoolConfig`, `PoolSnapshot`,
`Priority`, `PriorityConfig`, `ResourceConfig`, `ResourceKind`, `RetryPolicy`, `Snapshot`,
`StatusEvent`, `SubmitError`, `Submitted`, `TaskId`, `TaskRef`, `TaskStatus`,
`UnknownDependency`, `UrgencyConfig`, and more. The whole engine is `cronoq::core`.

| Feature | Default | Effect |
|---|---|---|
| `otel` | on | OpenTelemetry metrics and W3C trace-context propagation ([metrics reference](metrics.md#opentelemetry-cronoq-crate)). |

## `cronoq-core`

`Core<P>` is a deterministic state machine with no I/O, threads or clocks. Every method takes
the current `Time`, so tests and simulations run thousands of virtual hours per second.

| Method | Description |
|---|---|
| `Core::new(CoreConfig)` | Create; `CoreConfig::validate()` reports problems without panicking. |
| `submit`, `try_submit`, `submit_batch` | Admission. |
| `complete(now, lease, Outcome)`, `heartbeat(now, lease)` | Attempt feedback. |
| `cancel`, `status`, `resolve`, `live_tasks`, `totals` | Lookups and cancellation. |
| `set_capacity(now, pool, n)` | Capacity changes. |
| `add_recurring`, `remove_recurring`, `recurring_names` | Cron schedules. |
| `tick(now)`, `next_wakeup()` | Timers. |
| `poll_action()`, `drain_actions()`, `has_actions()` | `Action::{Dispatch, Abort, ScaleTo, Status, Persist, SpaceAvailable}` for the driver. |
| `snapshot(now)` | Metrics. |
| `recover(now, Recovered)` | Restore state loaded from a store. |
| `check_invariants()` | Panics on internal inconsistency (tests and fuzzing). |

`CoreConfig` fields: `pools`, `resources`, `priority`, `urgency`, `global_bounds`, `retention`,
`retention_max`, `persist`, `epoch_unix_nanos`, `id_epoch`, `metrics_window` (default 60 s),
`seed`.

## `cronoq-store-postgres`

| Item | Description |
|---|---|
| `PgStore::connect(url)` / `connect_with(url, PgStoreConfig)` | Connect and migrate. |
| `PgStoreConfig` | `schema` (default `public`), `retention`, `lock_poll`, `keepalive`, `session_timeout`, `commit_timeout`, `max_connections`. |
| `migrate()` | Apply pending schema migrations (versioned in `cronoq_schema_version`). |
| `get_task(target)` | Read a stored task (`TaskRecord`). |
| `prune(older_than)` | Delete finished tasks older than the given age. |
| `is_leader()`, `epoch()`, `leadership_lost().await`, `release_leadership().await` | Leadership state. |

See [Durability and high availability](../concepts/durability.md).

## `cronoq-server::worker`

```rust,ignore
use cronoq_server::worker::{WorkerOptions, run_worker};

let opts = WorkerOptions::new("http://127.0.0.1:7070", "default").slots(8).token(token);
run_worker(opts, |lease| async move { Ok(lease.payload) }, shutdown_signal).await?;
```

| `WorkerOptions` field | Default | Description |
|---|---|---|
| `endpoint`, `pool` | required | Server URL (`http://` or `https://`) and pool name. |
| `slots` | 1 | Concurrent leases requested (the server may grant fewer). |
| `token` | none | Bearer token with the worker role. |
| `name` | empty | Shown in server logs. |
| `tls` | none | `WorkerTls { ca_pem, identity, domain }` for TLS and mTLS. |
| `reconnect` | `true` | Reconnect with backoff when the connection drops or the server steps down. |
| `initial_backoff`, `max_backoff` | 200 ms, 10 s | Reconnect backoff bounds. |
| `drain_timeout` | 30 s | How long running handlers may finish after `shutdown` resolves. |
| `max_message_bytes` | 64 MiB | Largest message accepted from the server. |

The handler receives a `LeaseInfo` (`lease`, `task_id`, `attempt`, `pool`, `payload`,
`content_type`, `kind`, `key`, `tags`, `timeout`, `deadline`, `cancel`) and returns
`Result<Bytes, TaskError>`. `run_worker` enforces the granted slots, heartbeats each lease,
cancels `lease.cancel` on `Abort`, turns a panicking handler into a retryable failure, and
returns an error only for problems retrying cannot fix (bad credentials, unknown pool).
