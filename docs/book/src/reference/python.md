# Python API

```sh
pip install cronoq            # embedded scheduler (module: cronoq)
pip install "cronoq[client]"  # + cronoq.client: gRPC client and remote worker for cronoq-server
```

Wheels use the stable ABI (`abi3`, CPython 3.14+) for Linux, macOS and Windows. Type stubs ship
with the package (`py.typed`). Durations are seconds
(`float`); absolute times accept a `datetime` or Unix seconds. Invalid durations or
configuration (negative, NaN, out of range) raise `ValueError`.

## `cronoq.Scheduler`

```python
Scheduler(pools: Sequence[Pool], resources: Sequence[Resource] | None = None,
          emit_transitions: bool = False, *, shutdown_grace: float = 5.0,
          event_capacity: int = 1024, store: SqliteStore | None = None,
          codec: Codec | None = None)
```

| Argument | Description |
|---|---|
| `pools` | At least one `Pool`. |
| `resources` | Shared semaphores and token buckets. |
| `emit_transitions` | Deliver every status transition on `events()`, not only terminal ones. |
| `shutdown_grace` | Seconds running attempts get when leaving `async with`. |
| `event_capacity` | Buffer of each `events()` stream. |
| `store` | Keep state in a SQLite file so it survives restarts; see [`SqliteStore`](#cronoqsqlitestore). |
| `codec` | How payloads and results are turned into bytes for the store (an object with `encode(obj) -> bytes` and `decode(bytes) -> obj`). Default: `JsonCodec`. Needs `store`. |

A scheduler is bound to the event loop that starts it. If that loop closes without a shutdown,
the scheduler stops and further calls raise `SchedulerError`; calls before `start()` raise
`RuntimeError`.

| Member | Returns | Description |
|---|---|---|
| `await start()` | | Start on the running event loop. Executors run on this loop. |
| `await submit(payload, **options)` | `TaskHandle` | Submit; [options](#task-options) below. Waits for queue space with the `wait` overflow policy. |
| `await enqueue(payload, **options)` | `int` | Submit without a handle; returns the id. |
| `await submit_batch(tasks: Sequence[Task])` | `list[TaskHandle]` | Atomic batch; tasks may depend on each other by key. |
| `await cancel(target)` | `bool` | Cancel by id or key; `False` if already finished. |
| `await status(target)` | `str \| None` | Status name, or `None` if unknown. |
| `await handle(target)` | `TaskHandle \| None` | Attach to an existing task. |
| `await snapshot()` | `Snapshot` | Metrics and quality signals. |
| `await set_capacity(pool, n)` | | Report capacity for externally managed pools. |
| `await set_group_capacity(pool, labels, n)` | | Report the slots of the pool's workers with exactly `labels` ([Worker labels](../concepts/labels.md)). |
| `await add_recurring(name, cron, payload, **options)` | | Cron schedule. Options: `tz`, `overlap` (`"skip"`, `"allow"`, `"queue"` or `"queue_on_success"`), `tolerance` (1.0), `on_miss` (`"run_late"`), `priority`, `pool`, `kind`, `key` (a prefix: each occurrence gets `"<key>@<start in UTC>"`), `deadline_in` (relative to each occurrence's start), `duration_hint`, `timeout`, `retry`, `resources`, `tags`. |
| `await remove_recurring(name)` | `bool` | `False` if no such schedule. |
| `events(on_lag="skip")` | `EventStream` | Status events; see [below](#status-events). |
| `await shutdown(grace=5.0)` | | Stop admitting, let running attempts finish for `grace` seconds, then stop. |
| `async with Scheduler(...) as s` | | `start()` on enter, `shutdown(shutdown_grace)` on exit. |
| `started`, `pools`, `default_pool` | properties | Whether started; pool names; the pool used without `pool=` (`"default"`, or the first). |

### Task options

Accepted by `submit`, `enqueue` and `Task(...)`.

| Option | Type | Description |
|---|---|---|
| `priority` | `Priority`, `"critical"`/`"high"`/`"normal"`/`"low"`, or `int` | Default normal. |
| `pool`, `kind` | `str` | Default: the default pool, `"default"`. |
| `key` | `str` | Unique key (deduplication, dependencies). |
| `deadline_in` / `deadline_at` | seconds / time | Deadline. |
| `start_in` / `start_at` | seconds / time | Fixed start. |
| `tolerance` | seconds | Fixed-start tolerance (default 1; 0 allowed). |
| `on_miss` | `"run_late"`, `"skip"`, `"fail"` | Fixed-start miss policy. |
| `duration_hint`, `timeout` | seconds | Expected run time; per-attempt timeout. |
| `retry` | `RetryPolicy` | Overrides the pool's policy. |
| `after` | id/key or a list of them | Must succeed first. |
| `deps` | `list[Dep]` | Dependencies with conditions. |
| `resources` | `dict[str, int]` | Units of named resources per attempt. |
| `tags` | `dict` | Labels (values converted to `str`). |
| `labels` | `dict[str, str]` | Worker labels the task requires; it runs only in a group of workers that has them. |
| `tenant` | `str` | Tenant, for pools with `tenant_weights` or `fair_share` ([Fair sharing](../concepts/fairness.md)). |

Strings must not contain NUL characters.

### Status events

`sched.events()` returns an `EventStream`, an async iterator of `StatusEvent` (`id`, `pool`,
`key`, `status`, `attempt`, `at` in Unix seconds, `message`).

```python
async with sched.events() as events:
    async for e in events:
        print(e.id, e.status, e.message)
```

- Each stream buffers up to `event_capacity` events. When the consumer falls behind, the oldest
  are dropped and counted in `events.lagged`. With `on_lag="raise"` the next iteration raises
  `EventsLagged` (`.skipped`); iterating again continues with the buffered events.
- The stream ends (`StopAsyncIteration`) when the scheduler shuts down or the stream is closed
  (`close()`, `await aclose()`, or leaving `async with`). `closed` tells whether it ended.

## `cronoq.SqliteStore`

```python
SqliteStore(path, *, lease_ttl=10.0, lease_renew=2.0, lock_poll=0.5, retention=3600.0,
            full_sync=True)
```

Durable state in a local SQLite file, created with owner-only permissions. Submissions,
cancellations and schedule changes return only once they are stored. After a restart, queued
and interrupted tasks run again (an attempt that was running counts as used), recurring
schedules resume, and results of finished tasks stay available through `handle()`.

| Argument | Description |
|---|---|
| `path` | Database file; keep it on a local filesystem. |
| `lease_ttl`, `lease_renew`, `lock_poll` | Several processes may share the file: one leads and the others wait in `start()`. The leader renews its lease every `lease_renew` seconds; a standby takes over `lease_ttl` seconds after the leader stopped renewing (at once after a clean shutdown) and checks every `lock_poll` seconds. |
| `retention` | How long finished tasks are kept in memory after a restart, for key deduplication. |
| `full_sync` | Sync to disk on every commit. Off is faster but a power loss can drop the latest commits. |

With a store, payloads and results are encoded by the scheduler's codec:

- `cronoq.JsonCodec` (the default) stores JSON. Payloads it cannot encode (bytes, sets, arbitrary
  objects) are refused at submit with `TypeError`, before anything is stored. Decoding JSON never
  runs code, so a tampered file cannot execute anything in your process.
- Pass your own codec for other types, for example msgpack. A pickle codec works, but anyone who
  can write the file can then run code in your process.
- A result the codec cannot encode is not stored (a warning is logged on the `cronoq` logger);
  the task still succeeds.

## `cronoq.Pool`

```python
Pool(name, executor, *, min=1, max=10, initial=None, autoscale=None, lease_ttl=None,
     max_pending=None, overflow="wait", retry=None, unknown_dependency="reject",
     await_timeout=None, default_duration=1.0, reservable_fraction=0.5,
     tenant_weights=None, fair_share=False)
```

| Argument | Description |
|---|---|
| `executor` | `async def run(ctx, payload)`, or a plain function (runs in the loop's default thread pool). |
| `min`, `max`, `initial` | Concurrency bounds (`initial` defaults to `min`). |
| `autoscale` | `True` / `None` (default policy), `False` (fixed size: `initial`, or `max` when `initial` is not given), or an `AutoscaleConfig`. |
| `tenant_weights`, `fair_share` | Share the pool fairly between tenants: weights by tenant (others weigh 1), or equal shares with `fair_share=True` ([Fair sharing](../concepts/fairness.md)). |
| `lease_ttl` | Attempts that do not call `ctx.heartbeat()` within this many seconds are failed as retryable. |
| `max_pending`, `overflow` | Queue bound and what happens when it is reached: `"wait"`, `"reject"`, `"shed_lowest"`, `"shed_oldest"`. |
| `retry` | Default `RetryPolicy` (3 attempts, exponential backoff). |
| `unknown_dependency`, `await_timeout` | `"reject"` unknown dependency keys, or `"await"` them for up to `await_timeout` seconds. |
| `default_duration` | Duration assumed for tasks without `duration_hint`. |
| `reservable_fraction` | Share of `max` that fixed-start reservations may use, in `(0, 1]`. |

A sync executor cannot be interrupted: on timeout or cancellation the attempt is aborted, but
the function keeps running until it returns. Check `ctx.is_cancelled()` in long-running sync
executors.

## Other types

| Type | Description |
|---|---|
| `Priority` | `CRITICAL`, `HIGH`, `NORMAL`, `LOW`; ordered, hashable, `.name`, `.value`, `int()`. |
| `Task(payload, **options)` | A task for `submit_batch`. |
| `Dep(target, on="success")` | Dependency; `on` is `"success"`, `"failure"` or `"complete"` (`"on_"` prefix accepted). |
| `RetryPolicy(max_attempts=3, initial_backoff=1.0, multiplier=2.0, max_backoff=60.0, jitter=True)` | `RetryPolicy.none()` disables retries. |
| `AutoscaleConfig(interval=1.0, target_utilization=0.75, drain_time=30.0, slo=None, reservation_horizon=60.0, stabilization_window=60.0, cooldown=30.0, max_step_up_abs=4, max_step_down=1, cost_per_worker_second=0.0)` | `slo` maps priority names to wait objectives in seconds. |
| `Resource.semaphore(name, permits)` / `Resource.token_bucket(name, rate, burst)` | Shared resources. |
| `TaskContext` | `id`, `attempt`, `kind`, `key`, `pool`, `tags`, `group` (labels of the worker group the attempt was assigned to), `timeout`, `deadline` (Unix seconds); `is_cancelled()`, `heartbeat()`. |
| `TaskHandle` | Awaitable; `id`, `duplicate`, `await result()`, `done()`, `cancel()`. |
| `Snapshot` | `pools`, `live_tasks`, `running`, `pending`, `totals` (dict), `pool(name)`. |
| `PoolSnapshot` | The [snapshot fields](../concepts/metrics.md) with times in seconds, plus `pending`. |
| `StatusEvent`, `EventStream` | See [status events](#status-events). |

## Exceptions

All live in `cronoq` (and `cronoq.errors`), derive from `CronoqError` and can be pickled.

| Exception | Raised |
|---|---|
| `Retryable(message="", retry_after=None)` | By executors and worker handlers: retry, not before `retry_after` seconds. |
| `Permanent(message="")` | By executors and worker handlers: fail without retrying. |
| `TaskFailed` | Awaiting a task that did not succeed. `.status` (terminal status, `"shutdown"`, or `"succeeded"` when the output is no longer available), `.message`, `.task_id`. |
| `SubmitError` | Submission rejected. `.code`: `unknown_pool`, `unknown_resource`, `resource_exceeds_capacity`, `reservation_conflict`, `cycle`, `unknown_dependency`, `full`, `overloaded`, `duplicate_key_in_batch`, `invalid`. |
| `SchedulerError` | Scheduler stopped, misconfigured, or its store failed. |
| `EventsLagged` | An `events(on_lag="raise")` stream fell behind; `.skipped`. |

Any other exception from an executor is a retryable failure. See
[Errors and status codes](errors.md).

## `cronoq.client`

The gRPC client and remote worker for `cronoq-server`, speaking worker protocol version 3
(`cronoq.client.PROTOCOL_VERSION`). Both can be created outside a running loop; they connect on
first use.

### `Client(target, *, token=None, tls=False, credentials=None, options=None)`

`target` is `host:port`. `tls=True` uses system roots; pass `grpc.ssl_channel_credentials(...)`
as `credentials` for a private CA or mTLS. Use it as `async with Client(...) as c` or call
`await c.close()`.

| Method | Returns | Description |
|---|---|---|
| `await submit(payload=None, *, wait=False, wait_timeout=None, call_timeout=None, **options)` | `Submitted(id, duplicate, result)` | Options as for [`task`](#taskpayloadnone-options). With `wait=True`, `result` is set when the task finished. |
| `await submit_batch(specs)` | `list[Submitted]` | Atomic batch of specs built with `task(...)`. |
| `await cancel(ref)` | `bool` | By id or key. |
| `await get(ref)` | `TaskInfo` | Raises `ClientError` with code `not_found` for unknown tasks. |
| `await list_tasks(*, pools=(), statuses=(), after=0, limit=0)` | `TaskPage(tasks, next_after)` | One page of tasks, live and finished, in id order. `statuses` are names such as `"running"`; `limit` defaults to 100, capped by the server's `limits.max_list`. Pass `next_after` as `after` for the next page (`None` on the last one). |
| `iter_tasks(*, pools=(), statuses=(), page_size=0)` | async iterator of `TaskInfo` | Every matching task, fetched one page at a time. |
| `await wait(ref, timeout=None)` | `Result` | Raises `TimeoutError` if still running after `timeout` (or the server's `max_wait_s`). |
| `watch(pools=None, *, ids=None, keys=None, all_transitions=False)` | async iterator of `Event` | Terminal events by default; every transition with `all_transitions=True`. |
| `await snapshot()` | `Snapshot` | Requires a metrics or admin token. |
| `await add_recurring(name, cron, payload=None, *, tz=None, overlap="skip", **options)` | | Requires an admin token. `payload` may also be a prebuilt `task(...)` spec. `overlap`, `key` and `deadline_in` work as for the native scheduler. |
| `await remove_recurring(name)` | `bool` | Requires an admin token. |
| `await list_workers()` | `list[WorkerInfo]` | Connected remote workers. Requires a metrics or admin token. |
| `await list_recurring()` | `list[RecurringInfo]` | Recurring schedules, by name. Requires a metrics or admin token. |

Result types (frozen dataclasses):

| Type | Fields |
|---|---|
| `Submitted` | `id`, `duplicate`, `result` |
| `Result` | `id`, `status` (terminal), `output` (bytes or `None`), `message`, `output_available`; `.ok`, `.json()`, `.text()` |
| `TaskInfo` | `id`, `status`, `pool`, `key`, `kind`, `attempt`, `max_attempts`, `created_at`, `started_at`, `finished_at` (Unix seconds or `None`), `message`, `output_available`, `tenant`, `priority` (name or `None`); `.done` |
| `TaskPage` | `tasks` (tuple of `TaskInfo`), `next_after` (`int` or `None`), `unavailable_shards` (shards that could not be reached) |
| `WorkerInfo` | `id`, `name`, `pool`, `labels`, `slots`, `busy`, `connected_at` (Unix seconds) |
| `RecurringInfo` | `name`, `cron`, `tz` (empty for UTC), `pool`, `kind`, `overlap`, `next_at` (Unix seconds or `None`), `last_task_id` (or `None`) |
| `Event` | `id`, `pool`, `key`, `status`, `attempt`, `at` (Unix seconds), `message` |
| `Snapshot` | `pools`, `live_tasks`, `running`, `pending`, `totals` (`Totals`), `workers_connected`, `unavailable_shards`; `.pool(name)` |
| `PoolSnapshot` | `name`, `capacity`, `busy`, `held`, `desired`, `pending_by_class`, `reservations`, `dispatched`, `succeeded`, `failed`, `priority_adherence`, `fixed_start_hit_rate`, `utilization`, `schedule_efficiency`, `wait_p50`, `wait_p90`, `wait_p99`, `cost`, `workers_connected`; `.pending` |

Failed calls raise `ClientError`: `.code` is the lower-case gRPC status name and `.retryable`
is true for `unavailable` and `resource_exhausted`. A `watch` stream ends with `ClientError`
`unavailable` when the server stops serving and `data_loss` when the consumer fell behind:
reconnect and reconcile with `get` or `wait`.

### `task(payload=None, **options)`

Builds a `TaskSpec` for `submit_batch` and `add_recurring`. Options: `pool`, `kind`,
`priority`, `key`, `deadline_in` / `deadline_at`, `start_in` / `start_at`, `tolerance`,
`on_miss`, `duration_hint`, `timeout`, `retry` (a `RetryPolicy` or a dict), `after`, `deps`
(`(ref, "on_success")` tuples or dicts), `resources`, `tags`, `labels` (worker labels the
task requires), `tenant`, `content_type`.

Payloads: `bytes` are sent as-is, `str` as UTF-8 text, other values as JSON
(`application/json`). Worker outputs are encoded the same way.

### `Worker(target, pool, handler, *, slots=1, token=None, name="", tls=False, credentials=None, reconnect=True, backoff=(0.1, 5.0), grace=10.0, options=None, labels=None, insecure_redirect_http=False)`

| Member | Description |
|---|---|
| `await run()` | Serve until `stop()`, reconnecting with exponential backoff (`backoff` = initial, maximum seconds). Raises `ClientError` for errors retrying cannot fix (bad token, unknown pool). |
| `await stop(grace=None)` | Stop taking leases (`SetSlots(0)`), let running ones finish for `grace` seconds, cancel the rest, disconnect. Safe to call more than once; a stopped worker can run again. |
| `await wait_registered(timeout=None)` | Wait until registered. |
| `running` | Leases running now. |
| `worker_id`, `lease_ttl`, `granted_slots`, `server_protocol`, `max_message_bytes` | From the server's `Registered` message. |
| `last_shutdown`, `connections` | Reason of the last server `Shutdown`; number of connections made. |

`handler(lease)` is an `async def` receiving a `Lease`: `id`, `task_id`, `attempt`, `payload`,
`content_type`, `kind`, `key`, `tags`, `timeout`, `deadline` (Unix seconds), `pool`,
`priority`, `lease_ttl`, `cancelled` (an `asyncio.Event`), `.json()`, `.text()`. It returns the
output, or raises:

| Raise | Effect |
|---|---|
| `cronoq.Retryable(msg, retry_after=s)` or any other exception | Retryable failure. |
| `cronoq.Permanent(msg)` | Permanent failure. |
| `cronoq.client.Reject(reason)` | Hand the lease back unrun (`Nack`); the server offers it to another worker. |
| `cronoq.client.CancelTask(msg)` | End the task as `cancelled`. |

The worker heartbeats each running lease every `lease_ttl / 3`. When the server aborts an
attempt (timeout, cancellation, lease expiry) the handler's task is cancelled and
`lease.cancelled` is set. A handler that swallows `CancelledError` keeps running and may overlap
with the retry of the same task. When the server announces `Shutdown` (restart or leadership
change) the worker reconnects; attempts in flight are retried by the server. When another
shard serves the pool, the worker connects to that shard's endpoint, keeping its credentials
(`insecure_redirect_http=True` allows plain `http://` to other hosts; see
[Sharding](../concepts/sharding.md)).

## `cronoq.compat`

A synchronous, thread-safe API for code written against the `ElasticPriorityScheduler`
prototype: `ElasticPriorityScheduler(initial_workers=1, max_workers=10,
scale_up_threshold=0.7, scale_down_threshold=0.9, worker_cost_per_second=0.001)` with
`start()`, `stop()`, `submit(Task) -> bool`, `get_metrics() -> QualityMetrics`, `get_cost()`,
`completed_count`, `completed_tasks`, `pending_tasks`, and the types `Task`, `Priority`,
`ScheduledTask`, `QualityMetrics`, `ScalingConfig`. See [Migrating](../use-cases/migrating.md).
