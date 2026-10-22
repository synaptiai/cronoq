# Python quickstart

The `cronoq` package embeds the same Rust engine in Python, with an asyncio-native API.
Payloads and results are ordinary Python objects; nothing is serialized unless you keep state on
disk (see [Durable state](#durable-state)).

## Install

```sh
pip install cronoq             # embedded scheduler
pip install "cronoq[client]"   # plus the gRPC client and remote worker for cronoq-server
```

Wheels are published for CPython 3.14+ (abi3) on Linux, macOS and Windows.

## A first scheduler

```python
import asyncio
import cronoq

async def summarize(ctx: cronoq.TaskContext, text: str) -> str:
    # ctx.id, ctx.attempt, ctx.kind, ctx.key, ctx.pool, ctx.tags, ctx.timeout, ctx.deadline
    await asyncio.sleep(0.1)          # call a model, an API, a database...
    return text[:20]

async def main() -> None:
    pools = [cronoq.Pool("default", summarize, min=1, max=8)]
    async with cronoq.Scheduler(pools) as sched:
        handle = await sched.submit("a long document ...", priority="high", deadline_in=5.0)
        print(await handle)           # the executor's return value

asyncio.run(main())
```

- `Pool(name, executor, min=, max=)` defines a pool of concurrency slots that autoscales between
  `min` and `max`. Pass `autoscale=False` for a fixed size.
- Executors are `async def` functions run on the event loop that started the scheduler. Plain
  functions are accepted too; they run in the loop's default thread pool, so blocking calls do
  not stall the loop.
- `async with Scheduler(...)` starts the scheduler and shuts it down on exit (see
  [Lifecycle](#lifecycle)).
- Tasks without `pool=` go to the default pool: the one named `"default"`, or the first.

## Scheduling options

Every option of the engine is a keyword argument on `submit` (and on `cronoq.Task` for batches):

```python
handle = await sched.submit(
    {"doc": 42},
    pool="default",
    kind="summarize",                 # metrics and duration learning
    priority=cronoq.Priority.HIGH,    # or "critical" / "high" / "normal" / "low"
    key="summary:42",                 # idempotent submit; dependency handle
    deadline_in=30.0,                 # seconds
    timeout=60.0,                     # per attempt
    retry=cronoq.RetryPolicy(max_attempts=5, initial_backoff=0.5),
    resources={"llm-api": 1},         # shared rate limit or semaphore
    after=["fetch:42"],               # run after this key succeeds
)
```

## Retries and failures

```python
async def call_api(ctx, payload):
    resp = await http.post(...)
    if resp.status == 429:
        raise cronoq.Retryable("rate limited", retry_after=float(resp.headers["Retry-After"]))
    if resp.status == 400:
        raise cronoq.Permanent("bad request")   # no retry
    return resp.json()
```

Any other exception is treated as retryable. Awaiting a task that did not succeed raises
`cronoq.TaskFailed` with `.status` (`failed`, `dead_lettered`, `cancelled`, `skipped`,
`missed`, `shed`, or `shutdown`), `.message` and `.task_id`. All cronoq exceptions can be
pickled, so they cross `multiprocessing` boundaries.

## Batches and dependencies

```python
handles = await sched.submit_batch([
    cronoq.Task("extract", key="extract"),
    cronoq.Task("transform", key="transform", after="extract"),
    cronoq.Task("load", key="load", after="transform"),
    cronoq.Task("alert", deps=[cronoq.Dep("load", on="failure")]),
])
```

A batch is admitted atomically: if any task is rejected (for example because of a cycle), none
are.

## Status events

`sched.events()` streams status changes, for logs, metrics or notifying users:

```python
async with sched.events() as events:
    async for e in events:            # cronoq.StatusEvent
        print(e.id, e.key, e.status, e.attempt, e.message)
```

By default only terminal statuses (`succeeded`, `failed`, `dead_lettered`, ...) are delivered;
create the scheduler with `emit_transitions=True` to also see `ready`, `running`, `retrying`
and the rest. Each stream buffers `event_capacity` events (default 1024); a consumer that falls
behind loses the oldest ones (counted in `events.lagged`), or gets `cronoq.EventsLagged` with
`sched.events(on_lag="raise")`.

## Lifecycle

- A scheduler belongs to the event loop that starts it. Use `async with cronoq.Scheduler(...)`,
  or `await sched.start()` and `await sched.shutdown()`, inside that loop.
- Leaving `async with` waits up to `shutdown_grace` seconds (default 5) for running attempts,
  then cancels them. Queued tasks are not started.
- If the loop closes without a shutdown, the scheduler stops and later calls raise
  `cronoq.SchedulerError`.
- Cancelling a task, a timeout or a lost lease cancels the executor's asyncio task. An executor
  that swallows `CancelledError` keeps running and may overlap with its retry.

## Durable state

By default the scheduler keeps its state in memory. Give it a store to keep queued work,
schedules and results across restarts:

```python
store = cronoq.SqliteStore("/var/lib/myapp/cronoq.db")
async with cronoq.Scheduler(pools, store=store) as sched:
    await sched.enqueue({"doc": 42}, key="summary:42")
```

- Payloads and results are stored as JSON by default; submitting something JSON cannot hold
  raises `TypeError`. Pass `codec=` (an object with `encode` and `decode`) for other types.
- After a restart, queued and interrupted tasks run again on the new scheduler's pools, and
  `await sched.handle("summary:42")` returns the stored result of a task that already finished.
- Several processes may open the same file. One leads; the others wait in `start()` and take
  over when it stops.

See [`SqliteStore`](../reference/python.md#cronoqsqlitestore) for the options.

## Coming from `ElasticPriorityScheduler`

`cronoq.compat.ElasticPriorityScheduler` is a synchronous, thread-safe drop-in with the original
API (`submit`, `start`, `stop`, `get_metrics`, `get_cost`, `completed_count`). See
[Migrating from ElasticPriorityScheduler](../use-cases/migrating.md).

## Talking to a cronoq server

With the `client` extra, `cronoq.client.Client` submits work to a `cronoq-server` and
`cronoq.client.Worker` serves a pool:

```python
import os
from cronoq.client import Client, Worker

async with Client("localhost:7070", token=os.environ["CRONOQ_CLIENT_TOKEN"]) as c:
    sub = await c.submit({"prompt": "hi"}, pool="llm", priority="high", wait=True)
    print(sub.result.json())

async def handle(lease):
    return {"answer": lease.json()}

await Worker("localhost:7070", "llm", handle, slots=8,
             token=os.environ["CRONOQ_WORKER_TOKEN"]).run()
```

See [Writing executors and workers](workers.md), the [server quickstart](server-quickstart.md)
and the [Python API reference](../reference/python.md).
