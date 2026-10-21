# Python API

```sh
pip install cronoq            # embedded scheduler (module: cronoq)
pip install "cronoq[client]"  # + cronoq.client (gRPC client and worker)
```

Type stubs ship with the package (`py.typed`). Times are seconds (floats); absolute times
accept a `datetime` or Unix seconds.

## `cronoq.Scheduler`

```python
Scheduler(pools: Sequence[Pool], resources: Sequence[Resource] = None, emit_transitions: bool = False)
```

| Method | Returns | Description |
|---|---|---|
| `await start()` | | Start on the running event loop. Executors run on this loop. |
| `await submit(payload, **options)` | `TaskHandle` | Submit; options below. |
| `await enqueue(payload, **options)` | `int` | Submit without a handle; returns the id. |
| `await submit_batch(tasks: list[Task])` | `list[TaskHandle]` | Atomic batch. |
| `await cancel(target)` | `bool` | Cancel by id or key. |
| `await status(target)` | `str \| None` | Status name, or `None` if unknown. |
| `await handle(target)` | `TaskHandle \| None` | Handle for an existing task. |
| `await snapshot()` | `Snapshot` | Metrics. |
| `await set_capacity(pool, n)` | | Report capacity for externally managed pools. |
| `await add_recurring(name, cron, payload, **options)` | | Cron schedule; options `tz`, `overlap` (`"skip"`/`"allow"`), `tolerance`, `on_miss`, `priority`, `pool`, `kind`, `duration_hint`, `timeout`, `retry`, `resources`, `tags`. |
| `await remove_recurring(name)` | `bool` | |
| `await shutdown(grace=5.0)` | | Let running attempts finish for `grace` seconds, then stop. |
| `async with Scheduler(...) as s` | | `start()` on enter, `shutdown()` on exit. |

Attributes: `started`, `pools` (names).

### Submit options

| Option | Type | Description |
|---|---|---|
| `priority` | `Priority` \| `"critical"`/`"high"`/`"normal"`/`"low"` \| int | Default normal. |
| `pool`, `kind` | `str` | Default `"default"`. |
| `key` | `str` | Unique key. |
| `deadline_in` / `deadline_at` | seconds / time | Deadline. |
| `start_in` / `start_at` | seconds / time | Fixed start. |
| `tolerance` | seconds | Fixed-start tolerance (default 1). |
| `on_miss` | `"run_late"` \| `"skip"` \| `"fail"` | Fixed-start miss policy. |
| `duration_hint`, `timeout` | seconds | |
| `retry` | `RetryPolicy` | Overrides the pool default. |
| `after` | id/key or list | Must succeed first. |
| `deps` | `list[Dep]` | Dependencies with conditions. |
| `resources` | `dict[str, int]` | Units of named resources. |
| `tags` | `dict` | Labels (values converted to `str`). |

## `cronoq.Pool`

```python
Pool(name, executor, *, min=1, max=10, initial=None, autoscale=None, lease_ttl=None,
     max_pending=None, overflow="wait", retry=None, unknown_dependency="reject",
     await_timeout=None, default_duration=1.0, reservable_fraction=0.5)
```

`executor(ctx, payload)` is an `async def` or a plain function (run in the default thread pool).
`autoscale` is `True` (default policy), `False` (fixed size) or an `AutoscaleConfig`.

## Other types

| Type | Description |
|---|---|
| `Priority` | `CRITICAL`, `HIGH`, `NORMAL`, `LOW`. |
| `Task(payload, **options)` | A task for `submit_batch` (same options as `submit`). |
| `Dep(target, on="success")` | Dependency; `on` is `"success"`, `"failure"` or `"complete"`. |
| `RetryPolicy(max_attempts=3, initial_backoff=1.0, multiplier=2.0, max_backoff=60.0, jitter=True)` | `RetryPolicy.none()` disables retries. |
| `AutoscaleConfig(interval=1.0, target_utilization=0.75, drain_time=30.0, slo=None, reservation_horizon=60.0, stabilization_window=60.0, cooldown=30.0, max_step_up_abs=4, max_step_down=1, cost_per_worker_second=0.0)` | `slo` maps priorities to seconds. |
| `Resource.semaphore(name, permits)` / `Resource.token_bucket(name, rate, burst)` | Shared resources. |
| `TaskContext` | `id`, `attempt`, `kind`, `key`, `pool`, `tags`, `timeout`, `is_cancelled()`, `heartbeat()`. |
| `TaskHandle` | Awaitable; `id`, `duplicate`, `result()`, `done()`, `cancel()`. |
| `Snapshot` / `PoolSnapshot` | See [metrics](../concepts/metrics.md); times in seconds; `Snapshot.pool(name)`. |

## Exceptions

| Exception | Raised |
|---|---|
| `Retryable(message="", retry_after=None)` | By executors: retry (after at least `retry_after` seconds). |
| `Permanent(message="")` | By executors: fail without retrying. |
| `TaskFailed` | When awaiting a task that did not succeed. `.status`, `.message`, `.task_id`. |
| `SubmitError` | Submission rejected. `.code`: `unknown_pool`, `unknown_resource`, `resource_exceeds_capacity`, `reservation_conflict`, `cycle`, `unknown_dependency`, `full`, `overloaded`, `duplicate_key_in_batch`, `invalid`. |
| `SchedulerError` | Scheduler shut down, misconfigured, or store failure. |
| `CronoqError` | Base class of the above (except `Retryable`/`Permanent`). |

## `cronoq.client`

### `Client(target, *, token=None, tls=False, credentials=None, options=None)`

| Method | Returns |
|---|---|
| `await submit(payload=None, *, wait=False, **options)` | `Submitted(id, duplicate, result)` |
| `await submit_batch(specs)` | `list[Submitted]`; build specs with `cronoq.client.task(...)` |
| `await cancel(ref)` | `bool` |
| `await get(ref)` | `TaskInfo(id, status)` or `None` |
| `await wait(ref, timeout=None)` | `Result(id, status, output, message)`; `.ok`, `.json()`, `.text()`; raises `TimeoutError` |
| `watch(pools=None)` | async iterator of `Event(id, pool, key, status, attempt, at, message)` |
| `await snapshot()` | `Snapshot` (admin token) |
| `await add_recurring(name, cron, template, *, tz="", allow_overlap=False)` / `await remove_recurring(name)` | admin token |

Payloads: `bytes` are sent as-is, `str` as UTF-8 text, other values as JSON. Errors raise
`ClientError` with the gRPC status name in `.code`.

### `Worker(target, pool, handler, *, slots=1, token=None, name="", tls=False, credentials=None, reconnect=True, backoff=(0.1, 5.0), grace=10.0, options=None)`

| Member | Description |
|---|---|
| `await run()` | Serve until `stop()`. |
| `await stop(grace=None)` | Stop taking leases, drain running ones. |
| `await wait_registered(timeout=None)` | Wait for registration. |
| `running`, `worker_id`, `lease_ttl`, `connections` | State. |

`handler(lease)` receives a `Lease` (`id`, `task_id`, `attempt`, `payload`, `json()`, `text()`,
`content_type`, `kind`, `key`, `tags`, `timeout`, `deadline`, `cancelled`) and returns the
output.

## `cronoq.compat`

`ElasticPriorityScheduler(initial_workers=1, max_workers=10, scale_up_threshold=0.7,
scale_down_threshold=0.9, worker_cost_per_second=0.001)` with `start()`, `stop()`,
`submit(Task) -> bool`, `get_metrics() -> QualityMetrics`, `get_cost()`, `completed_count`,
`completed_tasks`, `pending_tasks`, and the types `Task`, `Priority`, `ScheduledTask`,
`QualityMetrics`, `ScalingConfig`. See [Migrating](../use-cases/migrating.md).
