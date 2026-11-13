# Python API

```sh
pip install cronoq            # embedded scheduler (module: cronoq)
pip install "cronoq[client]"  # + cronoq.client: gRPC client and remote worker for cronoq-server
```

Wheels use the stable ABI (`abi3`, CPython 3.14+) for Linux, macOS and Windows, on x86_64 and
arm64. The `client` extra needs `grpcio`, which has no prebuilt wheel for Windows on arm64, so
there pip tries to build it from source. Type stubs ship
with the package (`py.typed`). Durations are seconds
(`float`); absolute times accept a `datetime` or Unix seconds. Invalid durations or
configuration (negative, NaN, out of range) raise `ValueError`.

## `cronoq.Scheduler`

```python
Scheduler(pools: Sequence[Pool], resources: Sequence[Resource] | None = None,
          emit_transitions: bool = False, *, shutdown_grace: float = 5.0,
          event_capacity: int = 1024, store: SqliteStore | None = None,
          codec: Codec | None = None, priority: PriorityConfig | None = None,
          urgency: UrgencyConfig | None = None)
```

| Argument | Description |
|---|---|
| `pools` | At least one `Pool`. |
| `resources` | Shared semaphores and token buckets. |
| `emit_transitions` | Deliver every status transition on `events()`, not only terminal ones. |
| `shutdown_grace` | Seconds running attempts get when leaving `async with`. |
| `event_capacity` | Buffer of each `events()` stream. |
| `store` | Keep state in a SQLite file so it survives restarts; see [`SqliteStore`](#cronoqsqlitestore). |
| `codec` | How payloads and results are turned into bytes for the store: any object matching the `cronoq.Codec` protocol (`encode(obj) -> bytes` and `decode(bytes) -> obj`). Default: `JsonCodec`. Needs `store`. |
| `priority`, `urgency` | A `PriorityConfig` and an `UrgencyConfig` for every pool that does not set its own ([Priorities and aging](../concepts/priorities.md#configuration), [Deadlines](../concepts/deadlines.md#configuration)). Default: the built-in values. |

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
| `await update(target, *, priority=None, deadline_in=None, deadline_at=None, clear_deadline=False, tags=None)` | `Updated` | [Change the priority, deadline or tags](../concepts/updating.md) of a task that has not finished. Arguments left out keep their value; at most one of the three deadline arguments; `tags` replaces the task's tags. Raises `UpdateError`. |
| `await status(target)` | `str \| None` | Status name, or `None` if unknown. |
| `await handle(target)` | `TaskHandle \| None` | Attach to an existing task. |
| `await snapshot()` | `Snapshot` | Metrics and quality signals. |
| `await ordering(key, *, pool=None)` | `OrderingInfo \| None` | An [ordering key](../concepts/ordering-keys.md) of `pool` (default: the default pool); `None` for a key with no tasks and no kept floor. |
| `await halted_orderings(*, pool=None, limit=100)` | `list[str]` | The halted ordering keys of a pool, in name order. |
| `await set_ordering_floor(key, floor, *, pool=None, persist=False)` | | Set the class floor of an ordering key; `floor=None` removes it. With `persist=True` the floor stays when the key has no tasks left; beyond 100,000 kept floors the call raises `SchedulerError`. `persist=True` without a floor raises `ValueError`. |
| `await resume_ordering(key, *, pool=None)` | `bool` | Resume a halted ordering key. `False` if the key was not halted, which is not an error. |
| `await set_capacity(pool, n)` | | Report capacity for externally managed pools. |
| `await set_group_capacity(pool, labels, n)` | | Report the slots of the pool's workers with exactly `labels` ([Worker labels](../concepts/labels.md)). |
| `await add_recurring(name, cron, payload, **options)` | | Cron schedule. Options: `tz`, `overlap` (`"skip"`, `"allow"`, `"queue"` or `"queue_on_success"`), `tolerance` (1.0), `on_miss` (`"run_late"`), `priority`, `pool`, `kind`, `key` (a prefix: each occurrence gets `"<key>@<start in UTC>"`), `deadline_in` (relative to each occurrence's start), `duration_hint`, `timeout`, `retry`, `resources`, `tags`, `labels`, `tenant`. |
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
| `ordering_key` | `str` | Tasks of one pool that share it run one at a time, in submission order ([Ordering keys](../concepts/ordering-keys.md)). Not with `start_in` / `start_at`, and not in `add_recurring`. |
| `halt_on_failure` | `bool` | With `ordering_key`: if this task ends as failed, halt its key until `resume_ordering`. Default `False`. |

`submit`, `enqueue` and `Task(...)` accept `fingerprint` (an `int` below 2**128, or 16 bytes
such as a digest) and `on_key_conflict="reject"`: a submission whose key belongs to a task with
a different fingerprint then raises `SubmitError` with code `key_conflict` instead of returning
that task ([Detecting conflicting submissions](../concepts/key-conflicts.md)).
`Scheduler(on_key_conflict="reject")` makes that the default for submissions that do not say.

`submit` and `enqueue` also accept `on_key_conflict="replace_queued"` and
`renew_position=True`: with `"replace_queued"` the newest submission of
a key wins ([Replacing a task by key](../concepts/replacing.md)). A task that has not started is
replaced in place; one that has started keeps its payload, and the submission runs after it as
a new task with the same key. `TaskHandle.outcome` says which; for `"pending"` the handle tracks
the running task, and `await sched.handle(key)` gives the successor once that task has ended.
`enqueue` returns only an id (the existing task's when one was replaced or is running), so use
`submit` when the outcome matters. Both options need a `key`. Replacing is not accepted by
`Task(...)`, `submit_batch` or `add_recurring`.

Strings must not contain NUL characters.

### Status events

`sched.events()` returns an `EventStream`, an async iterator of `StatusEvent` (`id`, `pool`,
`key`, `status`, `attempt`, `at` in Unix seconds, `message`, `priority`, `kind`). `kind` is
`"transition"` for a status change. With `emit_transitions=True`, three more kinds are
published, each with the task's status at that moment: `"updated"` for an
[update](../concepts/updating.md) (`priority` is the class after the change), `"replaced"` when
a submission [replaces the task by key](../concepts/replacing.md) or is stored to run after it,
and `"successor"` for the task created from such a stored submission (`message` names the task
it follows).

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
     tenant_weights=None, fair_share=False, priority=None, urgency=None)
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
| `reservable_fraction` | Share of `max` that fixed-start reservations may use, in `[0, 1]`; `0` is treated as the default `0.5`. |
| `priority`, `urgency` | A `PriorityConfig` and an `UrgencyConfig` for this pool only. Default: the scheduler's. |

A sync executor cannot be interrupted: on timeout or cancellation the attempt is aborted, but
the function keeps running until it returns. Check `ctx.is_cancelled()` in long-running sync
executors.

## Other types

| Type | Description |
|---|---|
| `Priority` | `CRITICAL`, `HIGH`, `NORMAL`, `LOW`; ordered, hashable, `.name`, `.value`, `int()`. |
| `Codec`, `JsonCodec` | `Codec` is the protocol a store codec implements (`encode(obj) -> bytes`, `decode(bytes) -> obj`); `JsonCodec` is the default implementation. |
| `Task(payload, **options)` | A task for `submit_batch`. |
| `Dep(target, on="success")` | Dependency; `on` is `"success"`, `"failure"` or `"complete"` (`"on_"` prefix accepted). |
| `RetryPolicy(max_attempts=3, initial_backoff=1.0, multiplier=2.0, max_backoff=60.0, jitter=True)` | `RetryPolicy.none()` disables retries. |
| `AutoscaleConfig(interval=1.0, target_utilization=0.75, drain_time=30.0, slo=None, reservation_horizon=60.0, stabilization_window=60.0, cooldown=30.0, max_step_up_abs=4, max_step_down=1, cost_per_worker_second=0.0)` | `slo` maps priority names to wait objectives in seconds. |
| `PriorityConfig(base=None, aging_rate=None, age_cap=None, *, allow_overtaking_high=False)` | [Class scores and aging](../concepts/priorities.md#configuration). Each sequence holds four values (critical, high, normal, low); `None` keeps the default. Raises `ValueError` for values that fail the checks described there. Pass it to `Scheduler(priority=...)` for every pool, or to `Pool(priority=...)` for one. |
| `UrgencyConfig(max=5000.0, window=5.0, overdue_rate=100.0, overdue_cap=3000.0)` | [Deadline urgency curve](../concepts/deadlines.md#configuration), `window` in seconds. Pass it to `Scheduler(urgency=...)` or `Pool(urgency=...)`. |
| `Resource.semaphore(name, permits)` / `Resource.token_bucket(name, rate, burst)` | Shared resources. |
| `TaskContext` | `id`, `attempt`, `kind`, `key`, `pool`, `tags`, `group` (labels of the worker group the attempt was assigned to), `timeout`, `deadline` (Unix seconds); `is_cancelled()`, `heartbeat()`. |
| `TaskHandle` | Awaitable; `id`, `duplicate`, `outcome` (`"created"`, `"existing"`, `"replaced"` or `"pending"`), `await result()`, `done()`, `cancel()`. |
| `Updated` | Result of `update`: `id`, `status` (unchanged by the update), `deferred` (an attempt is running; the values apply to what follows it), `changed` (`False` when the task already had every value). |
| `OrderingInfo` | Result of `ordering`: `tasks` (not finished), `head` (id of the task at the front, or `None`), `effective` (name of the class the front task runs at, or `None`), `floor` (class name or `None`), `persist`, `halted`. |
| `Snapshot` | `pools`, `live_tasks`, `running`, `pending`, `totals` (dict), `pool(name)`. |
| `PoolSnapshot` | The [snapshot fields](../concepts/metrics.md) with times in seconds, plus `pending`, `ordering_keys`, `ordering_waiting` and `ordering_halted` ([Ordering keys](../concepts/ordering-keys.md#metrics)), and the `priority` (`PriorityConfig`) and `urgency` (`UrgencyConfig`) in effect for the pool. |
| `StatusEvent`, `EventStream` | See [status events](#status-events). |

`cronoq.__version__` is the package version as a string.

## Exceptions

All live in `cronoq` (and `cronoq.errors`), derive from `CronoqError` and can be pickled.

| Exception | Raised |
|---|---|
| `Retryable(message="", retry_after=None)` | By executors and worker handlers: retry, not before `retry_after` seconds. |
| `Permanent(message="")` | By executors and worker handlers: fail without retrying. |
| `TaskFailed` | Awaiting a task that did not succeed. `.status` (terminal status, `"shutdown"`, or `"succeeded"` when the output is no longer available), `.message`, `.task_id`. |
| `SubmitError` | Submission rejected. For `key_conflict`, `.task_id` and `.task_status` name the task holding the key; for a refused batch, `.index` is the position of the task that caused it. `.code`: `unknown_pool`, `unknown_resource`, `resource_exceeds_capacity`, `reservation_conflict`, `cycle`, `unknown_dependency`, `full`, `overloaded`, `duplicate_key_in_batch`, `replace_mismatch`, `key_conflict`, `invalid`. |
| `UpdateError` | `update` refused. `.code`: `not_found` (unknown or already finished), `fixed_start`, `invalid` (for example a reserved tag name). |
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
| `await submit(payload=None, *, wait=False, wait_timeout=None, call_timeout=None, on_key_conflict=None, renew_position=False, **options)` | `Submitted(id, duplicate, result, outcome)` | Options as for [`task`](#taskpayloadnone-options). With `wait=True`, `result` is set when the task finished. `on_key_conflict="replace_queued"` lets the newest submission of a key win ([Replacing a task by key](../concepts/replacing.md)); `outcome` is `"created"`, `"existing"`, `"replaced"` or `"pending"`. Raises `ClientError` `unimplemented` against a server that does not know the option. |
| `await submit_batch(specs, *, on_key_conflict=None)` | `list[Submitted]` | Atomic batch of specs built with `task(...)`. `on_key_conflict` is `"return_existing"` or `"reject"`. A refused batch raises `ClientError` with `.index`. |
| `await cancel(ref)` | `bool` | By id or key. |
| `await update(ref, *, priority=None, deadline_in=None, deadline_at=None, clear_deadline=False, tags=None)` | `Updated(id, status, deferred, changed)` | [Change the priority, deadline or tags](../concepts/updating.md) of a task that has not finished. Raises `ClientError` `not_found` for an unknown or finished task and `failed_precondition` for one with a fixed start. |
| `await get(ref)` | `TaskInfo` | Raises `ClientError` with code `not_found` for unknown tasks. |
| `await list_tasks(*, pools=(), statuses=(), after=0, limit=0)` | `TaskPage(tasks, next_after)` | One page of tasks, live and finished, in id order. `statuses` are names such as `"running"`; `limit` defaults to 100, capped by the server's `limits.max_list`. Pass `next_after` as `after` for the next page (`None` on the last one). |
| `iter_tasks(*, pools=(), statuses=(), page_size=0)` | async iterator of `TaskInfo` | Every matching task, fetched one page at a time. |
| `await wait(ref, timeout=None)` | `Result` | Raises `TimeoutError` if still running after `timeout` (or the server's `max_wait_s`). |
| `watch(pools=None, *, ids=None, keys=None, all_transitions=False)` | async iterator of `Event` | Terminal events by default; every transition with `all_transitions=True`. |
| `await snapshot()` | `Snapshot` | Requires a metrics or admin token. |
| `await ordering(key, *, pool=None)` | `OrderingInfo` | An [ordering key](../concepts/ordering-keys.md) of `pool` (default: the default pool). A key with no tasks and no kept floor has `tasks == 0`. Requires a metrics or admin token. |
| `await halted_orderings(*, pool=None, limit=100)` | `list[str]` | The halted ordering keys of a pool, in name order. Requires a metrics or admin token. |
| `await set_ordering_floor(key, floor, *, pool=None, persist=False)` | | Set the class floor of an ordering key; `floor=None` removes it. With `persist=True` the floor stays when the key has no tasks left; the server limits how many keys keep one (`ClientError` `resource_exhausted`). Requires an admin token that is not pinned to a tenant. |
| `await resume(*, ordering_key, pool=None)` | `bool` | Resume a halted ordering key. `False` if the key was not halted, which is not an error. Requires an admin token that is not pinned to a tenant. |
| `await add_recurring(name, cron, payload=None, *, tz=None, overlap="skip", **options)` | | Requires an admin token. `payload` may also be a prebuilt `task(...)` spec. `overlap`, `key` and `deadline_in` work as for the native scheduler. |
| `await remove_recurring(name)` | `bool` | Requires an admin token. |
| `await list_workers()` | `list[WorkerInfo]` | Connected remote workers. Requires a metrics or admin token. |
| `await list_recurring()` | `list[RecurringInfo]` | Recurring schedules, by name. Requires a metrics or admin token. |

Result types (frozen dataclasses):

| Type | Fields |
|---|---|
| `Submitted` | `id`, `duplicate`, `result`, `outcome` |
| `Result` | `id`, `status` (terminal), `output` (bytes or `None`), `message`, `output_available`; `.ok`, `.json()`, `.text()` |
| `TaskInfo` | `id`, `status`, `pool`, `key`, `kind`, `attempt`, `max_attempts`, `created_at`, `started_at`, `finished_at` (Unix seconds or `None`), `message`, `output_available`, `tenant`, `priority` (name or `None`); `.done` |
| `TaskPage` | `tasks` (tuple of `TaskInfo`), `next_after` (`int` or `None`), `unavailable_shards` (shards that could not be reached) |
| `WorkerInfo` | `id`, `name`, `pool`, `labels`, `slots`, `busy`, `connected_at` (Unix seconds) |
| `RecurringInfo` | `name`, `cron`, `tz` (empty for UTC), `pool`, `kind`, `overlap`, `next_at` (Unix seconds or `None`), `last_task_id` (or `None`) |
| `Updated` | `id`, `status` (unchanged by the update), `deferred`, `changed` |
| `OrderingInfo` | `pool`, `key`, `tasks`, `head` (task id or `None`), `effective` and `floor` (class names or `None`), `persist`, `halted` |
| `Event` | `id`, `pool`, `key`, `status`, `attempt`, `at` (Unix seconds), `message`, `priority` (name, or `None` from an older server), `updated` (true for an event that reports an update, delivered with `all_transitions=True`), `kind` (`"transition"`, `"updated"`, `"replaced"` or `"successor"`) |
| `Snapshot` | `pools`, `live_tasks`, `running`, `pending`, `totals` (`Totals`), `workers_connected`, `unavailable_shards`; `.pool(name)` |
| `PoolSnapshot` | `name`, `capacity`, `busy`, `held`, `desired`, `pending_by_class`, `reservations`, `dispatched`, `succeeded`, `failed`, `priority_adherence`, `fixed_start_hit_rate`, `utilization`, `schedule_efficiency`, `wait_p50`, `wait_p90`, `wait_p99`, `cost`, `workers_connected`, `groups` (tuple of `WorkerGroup`, unlabeled first), `ordering_keys`, `ordering_waiting`, `ordering_halted`, `priority` (`PrioritySettings`: `base`, `aging_rate`, `age_cap`, `allow_overtaking_high`) and `urgency` (`UrgencySettings`: `max`, `window`, `overdue_rate`, `overdue_cap`), both `None` from a server older than these settings; `.pending` |
| `WorkerGroup` | `labels`, `capacity`, `busy`: the connected workers of a pool that registered the same labels |
| `Totals` | `submitted`, `dispatched`, `succeeded`, `failed`, `dead_lettered`, `cancelled`, `skipped`, `missed`, `shed`, `retries`, `timeouts`, `lease_expiries`, `stale_completions`, `superseded` (counters since the leader started) |

Failed calls raise `ClientError`: `.code` is the lower-case gRPC status name and `.retryable`
is true for `unavailable` and `resource_exhausted`. `on_key_conflict="reject"` on `submit`
reports a key reused with other content as a `ClientError` with `.key_conflict` true and the
existing task in `.task_id` and `.task_status`
([Detecting conflicting submissions](../concepts/key-conflicts.md)). A `watch` stream ends with `ClientError`
`unavailable` when the server stops serving and `data_loss` when the consumer fell behind:
reconnect and reconcile with `get` or `wait`.

### `task(payload=None, **options)`

Builds a `TaskSpec` for `submit_batch` and `add_recurring`. Options: `pool`, `kind`,
`priority`, `key`, `deadline_in` / `deadline_at`, `start_in` / `start_at`, `tolerance`,
`on_miss`, `duration_hint`, `timeout`, `retry` (a `RetryPolicy` or a dict), `after`, `deps`
(`(ref, "on_success")` tuples or dicts), `resources`, `tags`, `labels` (worker labels the
task requires), `tenant`, `content_type`, `ordering_key` and `halt_on_failure`
([Ordering keys](../concepts/ordering-keys.md); `halt_on_failure=True` without an
`ordering_key` raises `ValueError`).

Payloads are encoded by type:

| Payload | Bytes sent | Content type |
|---|---|---|
| `bytes`, `bytearray`, `memoryview` | as-is | `application/octet-stream` |
| `str` | UTF-8 | `text/plain; charset=utf-8` |
| `None` | empty | none |
| anything else | JSON | `application/json` |

`content_type=` overrides the content type. Worker outputs are encoded the same way (without
an override). `Result.json()` returns `None` for an empty output.

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
| `cronoq.client.Reject(reason)` | Hand the lease back unrun (`Nack`); the server offers it to another worker of the pool with the same labels that has a free slot and has not already refused it. If there is none, the attempt fails as retryable and counts against the task's retries. |
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
