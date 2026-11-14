# Rust API

The complete API documentation is on docs.rs. This page maps the crates and their main types,
so you know where to look.

| Crate | docs.rs | Use it for |
|---|---|---|
| `cronoq` | [docs.rs/cronoq](https://docs.rs/cronoq) | Embedding the scheduler in a tokio application. Start here. |
| `cronoq-core` | [docs.rs/cronoq-core](https://docs.rs/cronoq-core) | The sans-IO engine: custom drivers, simulations, deterministic tests. |
| `cronoq-store-postgres` | [docs.rs/cronoq-store-postgres](https://docs.rs/cronoq-store-postgres) | Durable state and leader election for `Scheduler<Bytes, Bytes>`. |
| `cronoq-store-sqlite` | [docs.rs/cronoq-store-sqlite](https://docs.rs/cronoq-store-sqlite) | The same, in a local SQLite file. |
| `cronoq-proto` | [docs.rs/cronoq-proto](https://docs.rs/cronoq-proto) | Generated gRPC clients and servers (`cronoq_proto::v1`), `PROTOCOL_VERSION`. |
| `cronoq-server` | [docs.rs/cronoq-server](https://docs.rs/cronoq-server) | The server binary, and `worker::run_worker` for Rust workers. |

MSRV: Rust 1.94 (edition 2024) for every crate.

## `cronoq`

### Building a scheduler

`Scheduler::builder()` returns a `Builder<P, O>` (`P` = payload type, `O` = output type).

| `Builder` method | Default | Description |
|---|---|---|
| `pool(Pool)` | – | Add a pool (at least one; the first is the default pool). |
| `resource(ResourceConfig)` | – | Add a shared semaphore or token bucket. |
| `priority(PriorityConfig)` | built-in | Class scores and aging, for pools without their own. `build()` returns `Error::Config` for values that fail `PriorityConfig::validate`. |
| `urgency(UrgencyConfig)` | built-in | Deadline urgency curve, for pools without their own. Checked like `priority`. |
| `store(impl Store)` / `store_arc(Arc<dyn Store>)` | `MemoryStore` | Persistence backend. |
| `emit_transitions(bool)` | `false` | Broadcast every transition on `events()`, not only terminal ones. |
| `queue_capacity(n)` | 8192 | Capacity of the submission channel. |
| `events_capacity(n)` | 4096 | Broadcast buffer of `events()`; slow subscribers lose the oldest events. |
| `max_store_outage(Duration)` | 5 min | How long a store may report `StoreError::Unavailable` before the scheduler stops. |
| `drop_grace(Duration)` | 5 s | Grace for running attempts when every handle is dropped without `shutdown`. |
| `output_cache(n)` | 1024 | Recent outputs kept for handles attached after a task succeeded (0 disables). The messages of the last 1024 tasks that did not succeed are kept regardless. |
| `configure(FnOnce(&mut CoreConfig))` | – | Engine-wide settings: bounds, retention, metrics window, seed. |
| `build().await` | – | Start on the current tokio runtime (waits for leadership with a persistent store). |
| `build_blocking()` | – | Start a `BlockingScheduler` on its own runtime thread. |

`Pool<P, O>`:

| Method | Description |
|---|---|
| `Pool::new(name)` | Autoscaled pool (1 to 10 workers by default). |
| `Pool::fixed(name, n)` | Fixed size, no autoscaling. |
| `workers(min, initial, max)` | `min` is the lower bound on the autoscaler's desired capacity; `initial` is the capacity at start (at most `max`, and not raised to `min`); `max` is the upper bound. |
| `executor(impl Executor)`, `executor_arc(..)`, `executor_fn(async fn)` | How attempts run. |
| `scaler(impl Scaler)`, `scaler_arc(..)` | Capacity backend (default `SlotScaler`: in-process slots). |
| `autoscale(AutoscaleConfig)`, `no_autoscale()` | Autoscaling policy. |
| `priority(PriorityConfig)`, `urgency(UrgencyConfig)` | Class scores and deadline urgency for this pool only (default: the scheduler's). |
| `configure(FnOnce(&mut PoolConfig))` | Every other pool setting (bounds, overflow, retry, lease TTL, max hold, tenant `fairness`, ...). `configure(\|p\| p.max_releases = n)` sets how often an attempt of one task may be [released](../concepts/reliability.md#releasing-an-attempt) without counting as a failure (default 10). |

### `Scheduler<P, O>`

A cheap-to-clone handle. Every method is `async` and returns `Result<_, Error>`, except
`events()`, `pools()` and `clock()`, which are synchronous and return their value directly, and
`stopped()`, which is `async` and returns `()`.

| Method | Returns | Description |
|---|---|---|
| `submit(task)` | `TaskHandle<O>` | Submit and get an awaitable handle. Waits for queue space with the `Wait` overflow policy. |
| `enqueue(task)` | `Submitted` | Submit without a handle (cheaper; `id`, `duplicate`). |
| `enqueue_many(tasks)` | `Vec<Result<Submitted, Error>>` | Independent submissions in one round trip. |
| `submit_batch(tasks)` | `Vec<TaskHandle<O>>` | Atomic batch; tasks may depend on each other by key. |
| `handle(target)` | `Option<TaskHandle<O>>` | Attach to an existing task by id or key. |
| `cancel(target)` | `bool` | `false` if the task already finished. |
| `update(target, Update)` | `Updated` | [Change the priority, deadline or tags](../concepts/updating.md) of a task that has not finished. `Update::new()` with `priority(p)`, `deadline_in(d)`, `deadline_at(t)`, `no_deadline()`, `tags(pairs)`. `Updated` has `id`, `status`, `deferred` and `changed`. |
| `ordering(pool, key)` | `Option<OrderingInfo>` | An [ordering key](../concepts/ordering-keys.md) of `pool`: `tasks`, `head`, `effective`, `floor`, `persist`, `halted`, `paused` (the reason, if the key is paused). `None` for a key with no tasks and no kept floor. |
| `halted_orderings(pool, limit)` | `Vec<Arc<str>>` | The halted ordering keys of `pool`, in name order, at most `limit`. |
| `set_ordering_floor(pool, key, floor, persist)` | `()` | Set the class floor of an ordering key (`floor` is an `Option<Priority>`; `None` removes it). With `persist`, the floor stays when the key has no tasks left; beyond `CoreConfig::max_ordering_settings` kept floors the call fails with `Error::Ordering(OrderingError::TooManySettings)`. |
| `resume_ordering(pool, key)` | `bool` | Resume an ordering key that is halted or paused. `false` if the key was neither, which is not an error. |
| `pause(pool, PauseTarget, reason)` | `bool` | [Pause](../concepts/pausing.md) a pool, one of its classes or one of its ordering keys: `PauseTarget::Pool`, `Class(Priority)` or `Ordering(Arc<str>)`. `false` if it was already paused; the new reason replaces the old one. |
| `resume(pool, PauseTarget)` | `bool` | Resume what `pause` paused; for an ordering key, also a halt. `false` if there was nothing to resume, which is not an error. |
| `pauses(pool)` | `PoolPauses` | The pauses of `pool` and of its classes, with their reasons: `pool` (`Option<Arc<str>>`) and `classes` (one `Option<Arc<str>>` per class, most important first); `holds(class)` gives the reason tasks that run at `class` are held, `is_empty()` is true when nothing is paused. Paused ordering keys are not included. |
| `paused_orderings(pool, limit)` | `Vec<(Arc<str>, Arc<str>)>` | The paused ordering keys of `pool` with their reasons, in name order, at most `limit`. |
| `status(target)` | `Option<TaskStatus>` | `None` for unknown tasks. |
| `task(target)` | `Option<TaskSummary>` | One task that has not finished (pool, kind, key, priority, status, attempt, tenant); `None` if unknown or finished. |
| `list_tasks(pools, statuses, after, limit)` | `Vec<TaskSummary>` | Live tasks in id order, filtered by pool (any if empty) and status (any if empty), with ids above `after`. Finished tasks are listed by a durable store (`StoreQueries::list_finished`). |
| `list_recurring()` | `Vec<RecurringInfo>` | Recurring schedules with their next occurrence and latest task. |
| `snapshot()` | `Snapshot` | Metrics and quality signals. `Snapshot::totals.releases` counts [released](../concepts/reliability.md#releasing-an-attempt) attempts. `PoolSnapshot::ordering` is an `OrderingSnapshot` (`keys`, `waiting`, `halted`, `paused`, `settings`) for the pool's [ordering keys](../concepts/ordering-keys.md). `PoolSnapshot::pauses` is the pool's `PoolPauses` and `PoolSnapshot::paused_tasks` the number of ready tasks held by a [pause](../concepts/pausing.md). |
| `set_capacity(pool, n)` | `()` | Report capacity for pools with an `ExternalScaler`. |
| `set_group_capacity(pool, labels, n)` | `()` | Report the slots of the pool's workers with exactly `labels` ([Worker labels](../concepts/labels.md)). |
| `add_recurring(Recurring)` / `remove_recurring(name)` | `()` / `bool` | Cron schedules. |
| `events()` | `broadcast::Receiver<StatusEvent>` | Status events. |
| `pools()`, `clock()` | `&[Arc<str>]`, `&Clock` | Pool names; the engine clock. |
| `stopped()` | `()` | Resolves when the scheduler stops for any reason. |
| `shutdown(grace)` | `()` | Stop admitting, let running attempts finish for `grace`, then stop. |

With a persistent store, `submit`, `enqueue`, `submit_batch`, `cancel`, `update`, `add_recurring`,
`remove_recurring`, `set_ordering_floor`, `resume_ordering`, `pause` and `resume` return only
after the change is durable. The four ordering methods and the four pause methods return
`Error::Submit(SubmitError::UnknownPool)` for a pool the scheduler does not have.

`TaskHandle<O>` is a `Future<Output = Result<Arc<O>, TaskFailure>>` with `id()`,
`is_duplicate()` and `cancel()`.

### Tasks and schedules

`Task<P>`: `Task::new(payload)` (or `from_arc`), then `priority`, `pool`, `kind`, `key`,
`deadline_in` / `deadline_at`, `start_in` / `start_at`, `start_tolerance`, `on_miss`,
`duration_hint`, `timeout`, `retry`, `no_retry`, `after(target)`, `dep(Dep)`,
`resource(name, units)`, `tag(key, value)`, `label(name, value)` (a required
[worker label](../concepts/labels.md)), `tenant(name)` ([fair sharing](../concepts/fairness.md)),
`replace_queued()` and `renew_position()` ([replacing a task by key](../concepts/replacing.md);
single submissions only), `fingerprint(u128)` and `on_key_conflict(KeyConflict)`
([detecting conflicting submissions](../concepts/key-conflicts.md)), `ordering_key(key)` and
`halt_on_failure()` ([ordering keys](../concepts/ordering-keys.md); not with a fixed start),
`max_releases(n)` (how often an attempt of this task may be
[released](../concepts/reliability.md#releasing-an-attempt) without counting as a failure, instead of the pool's `max_releases`).
Builder order does not matter. Strings must not contain NUL characters.

`Submitted` (from `enqueue`) has `id`, `duplicate` and `outcome`: a `SubmitOutcome` of
`Created`, `Existing`, `Replaced` or `Pending`. `TaskHandle::outcome()` gives the same for
`submit`. For `Pending`, the id and the handle are those of the running task; the submission
runs after it as a new task with the same key, which `handle(key)` returns once that task ended.

`Recurring<P>`: `Recurring::new(name, cron, template)`, then `tz`, `overlap(OverlapPolicy)`
(`Skip`, `Allow`, `Queue`, `QueueOnSuccess`), `tolerance`, `on_miss`. The template's
`deadline_in` applies per occurrence and its key is a prefix (`<key>@<start>`); it may not set an
absolute deadline, a fixed start, dependencies or an ordering key.

### Executors

| Type | Description |
|---|---|
| `Executor<P, O>` | Trait: `execute(ctx, Arc<P>) -> BoxFuture<Result<O, TaskError>>`, optional `abort(lease)`. |
| `FnExecutor` | Wraps an `async fn(TaskContext, Arc<P>)`; what `Pool::executor_fn` uses. |
| `TaskContext` | `id`, `lease`, `attempt`, `pool`, `kind`, `key`, `tags`, `deadline`, `timeout`, `group` (labels of the worker group the attempt was assigned to); `is_cancelled()`, `cancelled().await`, `cancellation_token()`, `heartbeat()`, `tag(key)`, `traceparent()`; `TaskContext::for_test(id)` for unit tests. |
| `TaskError` | `Retryable { message, retry_after }`, `Permanent { message }`, `Cancelled`, `Release { after }`; constructors `retryable`, `retry_after`, `permanent`, `release`, `from_err`. |

`TaskError::release(after)` gives the task back without a verdict
([Releasing an attempt](../concepts/reliability.md#releasing-an-attempt)): it keeps its place and is
offered again once `after` has passed. The attempt number goes up, and the attempt does not
count against the retry policy. A task may be released `max_releases` times (the pool's, 10
unless set, or the task's own); a release beyond that is a retryable failure with the message
`released more than N times` and `after` as its minimum backoff.

A panicking executor fails its attempt permanently (`panic: ...`); the scheduler and other
attempts keep running.

### Errors

| Type | Variants |
|---|---|
| `SubmitError` | `UnknownPool`, `UnknownResource`, `ResourceExceedsCapacity`, `ReservationConflict`, `Cycle`, `UnknownDependency`, `Full`, `Overloaded`, `DuplicateKeyInBatch`, `ReplaceMismatch(&'static str)`, `KeyConflict { id, status }`, `Invalid(&'static str)`. |
| `Error` | `Submit(SubmitError)`, `Batch { index, error }` (a batch refused because of the task at `index`), `Update(UpdateError)`, `Ordering(OrderingError)`, `Shutdown`, `Config(String)`, `Store(StoreError)`, `Internal(String)`. |
| `UpdateError` | `NotFound`, `FixedStart`, `Invalid(&'static str)`. |
| `OrderingError` | `UnknownPool`, `TooManySettings` (the limit on ordering keys that keep a class floor is reached). |
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
| `Store<P, O>` | Trait: `persists` (default `true`; `false` means the engine produces no mutations), `acquire_leadership`, `release_leadership`, `commit`, `recover`, `load_output`. |
| `MemoryStore` | No persistence (default). |
| `TimeBase` | Wall-clock anchor of engine time; the runtime always uses Unix-epoch time (`unix_nanos_at_zero = 0`). |

### Blocking facade

`BlockingScheduler<P, O>` (from `Builder::build_blocking`) has the same operations without
`async`: `submit`, `enqueue`, `enqueue_many`, `submit_batch`, `task_handle`, `cancel`, `update`, `status`,
`snapshot`, `set_capacity`, `add_recurring`, `remove_recurring`, `events`, `scheduler`,
`shutdown(grace)`. `BlockingHandle<O>` has `id()`, `cancel()`, `wait()` and
`wait_timeout(d)`.

### Re-exports and features

Engine types are re-exported from `cronoq-core`: `AutoscaleConfig`, `Bounds`, `Dep`,
`EdgeCond`, `MissPolicy`, `OrderFailure`, `OrderingError`, `OrderingInfo`, `OrderingSnapshot`,
`OverflowPolicy`, `OverlapPolicy`, `PauseTarget`, `PoolConfig`, `PoolPauses`, `PoolSnapshot`,
`Priority`, `PriorityConfig`, `ResourceConfig`, `ResourceKind`, `RetryPolicy`, `Snapshot`,
`StatusEvent`, `SubmitError`, `Submitted`, `TaskId`, `TaskRef`, `TaskStatus`,
`UnknownDependency`, `UrgencyConfig`, and more. The whole engine is `cronoq::core`.

| Feature | Default | Effect |
|---|---|---|
| `otel` | on | OpenTelemetry metrics and W3C trace-context propagation ([metrics reference](metrics.md#opentelemetry-cronoq-crate)). |
| `store-codec` | off | The `cronoq::codec` module: conversions between engine types and the form durable stores keep, shared by the Postgres and SQLite stores. Needed only to write a store. |

## `cronoq-core`

`Core<P>` is a deterministic state machine with no I/O, threads or clocks. Every method takes
the current `Time`, so tests and simulations run thousands of virtual hours per second.

| Method | Description |
|---|---|
| `Core::new(CoreConfig)` | Create; `CoreConfig::validate()` reports problems without panicking. |
| `submit`, `try_submit`, `submit_batch` | Admission. |
| `submit_batch_with(now, specs, &[KeyConflict])` | Atomic batch with one mode per task (or none for the configured default). |
| `submit_with`, `try_submit_with` | Admission with `SubmitOptions { on_key_conflict, renew_position }`: `KeyConflict::Reject` compares `TaskSpec::fingerprint` with the one stored for the key and fails with `SubmitError::KeyConflict` when they differ; `KeyConflict::ReplaceQueued` replaces the task holding the key or stores the submission to run after it. Emits `Mutation::Replaced` or `Mutation::PendingReplacement`. |
| `complete(now, lease, Outcome)`, `heartbeat(now, lease)` | Attempt feedback. |
| `release(now, lease, delay) -> bool` | [Release an attempt](../concepts/reliability.md#releasing-an-attempt): the task is `Retrying` and is offered again once `delay` has passed; the attempt does not count against the retry policy. Beyond `PoolConfig::max_releases` (default 10) or `TaskSpec::max_releases`, the release is a retryable failure with `delay` as its minimum backoff; releases made while a pause holds the task do not count toward the limit. `false` for a lease that is unknown, as for `complete`. Emits `Mutation::Released { id, attempt, releases, at }`: `attempt` is the attempts made so far, `releases` how many of them were released, `at` the engine time of the release. `Totals::releases` counts releases. |
| `cancel`, `status`, `resolve`, `live_tasks`, `list_tasks`, `totals` | Lookups and cancellation. |
| `update(now, target, TaskPatch)` | Change a live task's priority, deadline or tags; emits `Mutation::Updated` and, with `emit_transitions`, a `StatusEvent` of `EventKind::Updated`. |
| `set_capacity(now, pool, n)`, `set_group_capacity(now, pool, labels, n)` | Capacity changes, for the whole pool or for the workers with exactly `labels`. |
| `ordering(pool, key)`, `halted_orderings(pool, limit)`, `set_ordering_floor`, `resume_ordering(now, pool, key)` | [Ordering keys](../concepts/ordering-keys.md): read a key, list halted keys, set or remove a class floor, resume a halted or paused key. Floor and halt changes emit `Mutation::Ordering`. |
| `pause(now, pool, &PauseTarget, reason)`, `resume(now, pool, &PauseTarget)`, `pauses(pool)`, `paused_orderings(pool, limit)` | [Pausing](../concepts/pausing.md): pause or resume a pool, a class or an ordering key (`Result<bool, OrderingError>`; `OrderingError::UnknownPool` for a pool the engine does not have), read the pauses of a pool and its classes (`Option<&PoolPauses>`), list paused ordering keys with their reasons. A change emits `Mutation::Paused { pool, target, reason }`; `reason` is `None` for a resume. |
| `add_recurring`, `remove_recurring`, `recurring_names`, `list_recurring` | Cron schedules. |
| `tick(now)`, `next_wakeup()` | Timers. |
| `poll_action()`, `drain_actions()`, `has_actions()` | `Action::{Dispatch, Abort, ScaleTo, Status, Persist, SpaceAvailable}` for the driver. |
| `snapshot(now)` | Metrics. |
| `recover(now, Recovered)` | Restore state loaded from a store. `Recovered::paused` is the pauses in place, each a `RecoveredPause { pool, target, reason }`. `RecoveredTask::releases` is how many of the task's attempts (`attempt`) were released; a store keeps it from `Mutation::Released`, and those attempts do not count against the retry policy after recovery. |
| `check_invariants()` | Panics on internal inconsistency (tests and fuzzing). |

`CoreConfig` fields: `pools`, `resources`, `priority`, `urgency`, `global_bounds`, `retention`,
`retention_max`, `max_ordering_settings` (default 100,000: most ordering keys that keep a
class floor while they have no tasks), `persist`, `epoch_unix_nanos`, `id_epoch`, `id_shard` (default `None`; when
set, task ids carry the shard number in their top 8 bits), `metrics_window` (default 60 s),
`seed`.

## `cronoq-store-postgres`

| Item | Description |
|---|---|
| `PgStore::connect(url)` / `connect_with(url, PgStoreConfig)` | Connect and migrate. |
| `PgStoreConfig` | `schema` (default `public`), `retention`, `lock_poll`, `keepalive`, `session_timeout`, `commit_timeout`, `max_connections`. |
| `migrate()` | Apply pending schema migrations (versioned in `cronoq_schema_version`). |
| `get_task(target)` | Read a stored task (`TaskRecord`). Also available through the `cronoq::StoreQueries` trait, with `prune`, `list_finished`, `is_leader` and `leadership_lost`. |
| `list_finished(pools, statuses, after, limit)` | Finished tasks in id order, filtered by pool and status (any if empty), with ids above `after`. |
| `prune(older_than)` | Delete finished tasks older than the given age. |
| `is_leader()`, `epoch()`, `leadership_lost().await`, `release_leadership().await` | Leadership state. |

## `cronoq-store-sqlite`

| Item | Description |
|---|---|
| `SqliteStore::open(path)` / `open_with(path, SqliteStoreConfig)` | Open (creating if needed) and migrate. |
| `SqliteStoreConfig` | `retention`, `lease_ttl` (10 s), `lease_renew` (2 s), `lock_poll`, `busy_timeout`, `commit_timeout`, `full_sync` (default on), `max_connections`. |
| `get_task`, `prune`, `list_finished`, `is_leader()`, `epoch()`, `leadership_lost().await`, `release_leadership().await` | As for the Postgres store. |

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
| `tls` | none | `WorkerTls { ca_pem, identity, domain }` for TLS and mTLS. Needs an `https://` endpoint; with `http://` the worker returns an error. |
| `labels` | empty | Worker labels, as `(name, value)` pairs; add one with the builder method `label(name, value)`. Tasks that require labels are leased only to workers that have them. |
| `reconnect` | `true` | Reconnect with backoff when the connection drops or the server steps down. |
| `insecure_redirect_http` | `false` | Follow a shard redirect to plain `http://` on another host with the token. Redirects otherwise keep TLS and use plain http only on loopback; see [Sharding](../concepts/sharding.md). |
| `initial_backoff`, `max_backoff` | 200 ms, 10 s | Reconnect backoff bounds. |
| `drain_timeout` | 30 s | How long running handlers may finish after `shutdown` resolves. |
| `max_message_bytes` | 64 MiB | Largest message accepted from the server. |

The handler receives a `LeaseInfo` (`lease`, `task_id`, `attempt`, `pool`, `payload`,
`content_type`, `kind`, `key`, `tags`, `timeout`, `deadline`, `cancel`) and returns
`Result<Bytes, TaskError>`. `run_worker` enforces the granted slots, heartbeats each lease,
cancels `lease.cancel` on `Abort`, turns a panicking handler into a retryable failure, and
returns an error only for problems retrying cannot fix (bad credentials, unknown pool).
