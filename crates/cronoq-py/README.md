# cronoq for Python

asyncio-native bindings for the cronoq scheduler (`pip install cronoq`, `import cronoq`).

```python
import asyncio, cronoq

async def call_llm(ctx, prompt):
    ...  # raise cronoq.Retryable("429", retry_after=2.0) to back off
    return "answer"

async def main():
    pools = [cronoq.Pool("llm", call_llm, min=1, max=16)]
    limits = [cronoq.Resource.token_bucket("openai", rate=50.0, burst=100)]
    async with cronoq.Scheduler(pools, resources=limits) as sched:
        h = await sched.submit("hi", pool="llm", priority="high", deadline_in=30, resources={"openai": 1})
        print(await h)
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
* `cronoq.compat.ElasticPriorityScheduler` is a synchronous drop-in for the original prototype.
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

## Server client and remote workers

`cronoq.client` talks to a `cronoq-server` over gRPC. It needs the `client` extra
(`pip install "cronoq[client]"`); `import cronoq` works without it.

```python
from cronoq.client import Client, Worker, task

# Submit work (client or admin token).
async with Client("localhost:7070", token="client-token") as c:
    sub = await c.submit({"prompt": "hi"}, pool="llm", priority="high",
                         deadline_in=30, timeout=60, key="job-1")
    result = await c.wait(sub.id, timeout=120)       # Result(id, status, output, message)
    print(result.status, result.json())

    await c.submit_batch([task("a", key="a"), task("b", key="b", after=["a"])])
    async for event in c.watch(pools=["llm"]):          # status events
        ...

# Serve a pool (worker token).
async def handle(lease):            # lease.payload / .json() / .text(), .attempt, .tags, .cancelled
    return {"answer": 42}           # bytes, str, JSON-serialisable value or None

worker = Worker("localhost:7070", "llm", handle, slots=8, token="worker-token")
await worker.run()                  # reconnects with backoff; `await worker.stop()` drains
```

* Payloads: `bytes` are sent as-is, `str` as UTF-8 text, other values as JSON
  (`application/json`). Outputs are encoded the same way.
* Handlers raise `cronoq.Retryable(msg, retry_after=...)` or `cronoq.Permanent(msg)`; any
  other exception is a retryable failure.
* Each running lease is heartbeated every `lease_ttl / 3`; when the server aborts an attempt
  (timeout, cancellation, lease expiry) the handler task is cancelled and `lease.cancelled` is set.
* Failed calls raise `cronoq.client.ClientError` with the gRPC status name in `.code`
  (e.g. `unauthenticated`, `resource_exhausted` when a pool is full); `Client.wait` raises
  `TimeoutError` on timeout, as does `Client.submit(..., call_timeout=...)`.
* `Client` and `Worker` can be created outside a running loop; they connect on first use.
  `Worker.stop()` can be called more than once (callers wait for the same drain), and a stopped
  worker can be run again. A handler that swallows `CancelledError` keeps running after an
  abort and may overlap with the retry of the same task.

The stubs in `cronoq/_proto` are generated by `scripts/gen_proto.sh` (needs `grpcio-tools`).

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install maturin pytest pytest-asyncio grpcio protobuf mypy
.venv/bin/maturin develop -m crates/cronoq-py/Cargo.toml
cd crates/cronoq-py && ../../.venv/bin/python -m pytest
../../.venv/bin/python -m mypy.stubtest cronoq._cronoq   # stubs match the extension
```

`python/tests/test_client.py` starts a real `cronoq-server` (building it with cargo if needed,
or using `CRONOQ_SERVER_BIN`) and is skipped if that is not possible.

`cargo test --workspace` builds this crate as well (it has no Rust tests); the
`extension-module` feature is only enabled by maturin.
