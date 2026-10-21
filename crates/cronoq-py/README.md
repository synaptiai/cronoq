# cronoq for Python

[![PyPI](https://img.shields.io/pypi/v/cronoq.svg)](https://pypi.org/project/cronoq/)
[![Python](https://img.shields.io/pypi/pyversions/cronoq.svg)](https://pypi.org/project/cronoq/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](https://github.com/synaptiai/cronoq#license)

**An elastic, priority-aware task scheduler for asyncio**, powered by a Rust engine.
cronoq decides when your coroutines run, in what order and with how much concurrency:
priorities with aging, deadlines, fixed start times and cron, dependencies, retries with
backoff, shared rate limits, and autoscaling, all in-process, without extra infrastructure.

```sh
pip install cronoq
```

## Why

- **Important work first, nothing starves.** Four priority classes with aging; deadlines raise
  urgency as they approach.
- **Respect rate limits.** Token buckets and semaphores shared across pools gate dispatch, so a
  burst never turns into a 429 storm; `Retryable(retry_after=...)` honours `Retry-After`.
- **Scheduled jobs that start on time.** Fixed-start tasks reserve capacity and get a worker held
  for them; cron schedules with time zones.
- **Pipelines without glue code.** Dependencies by key with success/failure/completion edges,
  atomic batches, idempotent keys.
- **Elastic concurrency.** Pools scale between `min` and `max` from queue pressure and wait-time
  objectives.
- **Fast.** The engine makes about a million scheduling decisions per second; payloads stay
  Python objects (no serialization).

## Quickstart

```python
import asyncio
import cronoq

async def call_llm(ctx: cronoq.TaskContext, prompt: str) -> str:
    # raise cronoq.Retryable("429", retry_after=2.0) to back off,
    # raise cronoq.Permanent("bad input") to fail without retrying
    await asyncio.sleep(0.1)
    return f"answer to {prompt!r}"

async def main() -> None:
    pools = [cronoq.Pool("llm", call_llm, min=1, max=16)]
    limits = [cronoq.Resource.token_bucket("openai", rate=50.0, burst=100)]
    async with cronoq.Scheduler(pools, resources=limits) as sched:
        chat = await sched.submit("hi", pool="llm", priority="high", deadline_in=2.0,
                                  resources={"openai": 1})
        batch = [await sched.submit(f"doc {i}", pool="llm", priority="low",
                                    resources={"openai": 1}) for i in range(10)]
        print(await chat)
        print(len([await h for h in batch]), "batch items done")
        print((await sched.snapshot()).pool("llm"))

asyncio.run(main())
```

* A scheduler is bound to the event loop that started it: use `async with cronoq.Scheduler(...)`
  (or `await sched.start()` / `await sched.shutdown()`) inside that loop. If the loop closes
  without a shutdown, the scheduler stops and further calls raise `cronoq.SchedulerError`.
  Leaving `async with` waits up to `shutdown_grace` seconds (default 5) for running attempts.
* Executors are `async def run(ctx, payload)` coroutines and run on that loop. `ctx` has `id`,
  `attempt`, `kind`, `key`, `pool`, `tags`, `timeout`, `deadline` (Unix seconds),
  `is_cancelled()` and `heartbeat()`.
* Plain (sync) functions are accepted too and run in the loop's default thread pool. A thread
  cannot be interrupted: on timeout or cancellation the attempt is aborted, but the function
  keeps running until it returns, and the next attempt may overlap with it. Check
  `ctx.is_cancelled()` in long-running sync executors.
* Payloads and results are arbitrary Python objects (never serialized).
* Tasks without `pool=` go to the scheduler's `default_pool`: the pool named `"default"`, or
  the first pool.
* `cronoq.Retryable` / `cronoq.Permanent` control retries; any other exception is retryable.
  Awaiting a task that did not succeed raises `cronoq.TaskFailed` (`.status`, `.message`).
  All cronoq exceptions can be pickled.
* Cancelling a task, a timeout, or a lost lease cancels the executor's asyncio task. A
  `CancelledError` the executor raises on its own (not caused by cronoq) is a retryable failure.
  An executor that swallows `CancelledError` keeps running and may overlap with the retry.
* Invalid durations or configuration (negative, NaN, out of range) raise `ValueError`.
* `cronoq.compat.ElasticPriorityScheduler` is a synchronous, thread-safe API for code written
  against `ElasticPriorityScheduler`.
* Wheels use the stable ABI (`abi3`, CPython 3.9+). Free-threaded (no-GIL) CPython builds are
  not supported.

### Status events

`sched.events()` is an async iterator of `cronoq.StatusEvent` (`id`, `pool`, `key`, `status`,
`attempt`, `at` in Unix seconds, `message`). It delivers terminal events (`succeeded`,
`failed`, `dead_lettered`, `cancelled`, ...) by default, and every transition (`ready`,
`running`, `retrying`, ...) when the scheduler is created with `emit_transitions=True`.

```python
async with sched.events() as events:          # or: events = sched.events(); ...; events.close()
    async for e in events:
        print(e.id, e.status, e.message)
```

* Each stream buffers up to `event_capacity` events (a `Scheduler` argument, default 1024).
  When the consumer falls behind, the oldest events are dropped and counted in
  `events.lagged`. With `sched.events(on_lag="raise")` the next iteration raises
  `cronoq.EventsLagged` (`.skipped`); iterating again continues with the buffered events.
* A stream ends (`StopAsyncIteration`) when the scheduler shuts down or the stream is closed.

More: [Python quickstart](https://synaptiai.github.io/cronoq/guides/python-quickstart.html) ·
[use cases](https://synaptiai.github.io/cronoq/use-cases/index.html) ·
[API reference](https://synaptiai.github.io/cronoq/reference/python.html).

## Client and remote workers for `cronoq-server`

```sh
pip install "cronoq[client]"
```

```python
import os
from cronoq.client import CancelTask, Client, Reject, Worker, task

# Submit and inspect (client token; the server requires tokens of at least 16 characters).
async with Client("localhost:7070", token=os.environ["CRONOQ_CLIENT_TOKEN"]) as c:
    sub = await c.submit({"prompt": "hi"}, pool="llm", priority="high",
                         deadline_in=30, timeout=60, key="job-1")
    result = await c.wait(sub.id, timeout=120)        # Result(id, status, output, message, ...)
    print(result.status, result.json())

    done = await c.submit("quick", wait=True, wait_timeout=10)   # submit and wait in one call
    info = await c.get("job-1")      # TaskInfo: status, pool, key, kind, attempt, timestamps, ...

    await c.submit_batch([task("a", key="a"), task("b", key="b", after=["a"])])
    async for event in c.watch(pools=["llm"], keys=["job-1"], all_transitions=True):
        ...                          # every transition of job-1 (default: terminal statuses only)

# Serve a pool (worker token).
async def handle(lease):  # lease.payload / .json() / .text(), .attempt, .pool, .tags, .cancelled
    if not can_handle(lease):
        raise Reject("missing model")    # hand the lease back unrun
    if obsolete(lease):
        raise CancelTask("superseded")   # end the task as cancelled
    return {"answer": 42}                # bytes, str, JSON-serialisable value or None

worker = Worker("localhost:7070", "llm", handle, slots=8,
                token=os.environ["CRONOQ_WORKER_TOKEN"])
await worker.run()                  # reconnects with backoff; `await worker.stop()` drains
```

- Payloads: `bytes` are sent as-is, `str` as UTF-8 text, other values as JSON
  (`application/json`). Outputs are encoded the same way and carry their content type.
- Handlers raise `cronoq.Retryable(msg, retry_after=...)` or `cronoq.Permanent(msg)`; any other
  exception is a retryable failure. An output larger than the server's `max_message_bytes` is a
  permanent failure.
- `Reject(reason)` gives the lease back: the server offers it, within the same attempt, to
  another worker of the pool with a free slot; if there is none, the attempt fails as retryable
  and counts against the retry budget. `CancelTask(reason)` ends the task as `cancelled`.
- The worker speaks protocol version 2. The server may grant fewer slots than requested
  (`worker.granted_slots`, capped by `limits.max_slots_per_worker`); leases beyond the grant are
  handed back. When the server announces it is shutting down (`worker.last_shutdown`) the worker
  reconnects with backoff, or immediately when leadership moved to another node.
- Each running lease is heartbeated every `lease.lease_ttl / 3`; when the server aborts an
  attempt (timeout, cancellation, lease expiry) the handler task is cancelled and
  `lease.cancelled` is set.
- Failed calls raise `cronoq.client.ClientError` with the gRPC status name in `.code`
  (e.g. `unauthenticated`, `not_found` from `get`/`wait` on an unknown task,
  `resource_exhausted` when a pool is full, `unavailable` when the server is not the leader or is
  shutting down); `.retryable` is true for the last two. `Client.wait` and
  `Client.submit(..., wait=True)` raise `TimeoutError` when the wait times out (the server caps
  waits at `limits.max_wait_s`), as does `Client.submit(..., call_timeout=...)`.
- `Client.snapshot()` (metrics or admin token) returns per-pool figures, `totals` and
  `workers_connected`.
* `Client` and `Worker` can be created outside a running loop; they connect on first use.
  `Worker.stop()` can be called more than once (callers wait for the same drain), and a stopped
  worker can be run again. A handler that swallows `CancelledError` keeps running after an
  abort and may overlap with the retry of the same task.

## Migrating from `ElasticPriorityScheduler`

```python
from cronoq.compat import ElasticPriorityScheduler, Priority, Task
```

A synchronous, thread-safe drop-in for the original API. See the
[migration guide](https://synaptiai.github.io/cronoq/use-cases/migrating.html).

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install maturin pytest pytest-asyncio grpcio protobuf mypy
.venv/bin/maturin develop -m crates/cronoq-py/Cargo.toml
cd crates/cronoq-py && ../../.venv/bin/python -m pytest
../../.venv/bin/python -m mypy.stubtest cronoq._cronoq   # stubs match the extension
```

`python/tests/test_client.py` starts a real `cronoq-server` (building it with cargo if needed,
or using `CRONOQ_SERVER_BIN`). The stubs in `cronoq/_proto` are generated by
`scripts/gen_proto.sh`. See [CONTRIBUTING.md](https://github.com/synaptiai/cronoq/blob/main/CONTRIBUTING.md).

## License

MIT OR Apache-2.0.
