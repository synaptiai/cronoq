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

- Executors are `async def` functions run on the event loop that started the scheduler; plain
  functions run in the loop's default thread pool.
- Awaiting a task that did not succeed raises `cronoq.TaskFailed` (`.status`, `.message`).
- Cancelling a task, a timeout, or a lost lease cancels the executor's asyncio task.

More: [Python quickstart](https://synaptiai.github.io/cronoq/guides/python-quickstart.html) ·
[use cases](https://synaptiai.github.io/cronoq/use-cases/index.html) ·
[API reference](https://synaptiai.github.io/cronoq/reference/python.html).

## Client and remote workers for `cronoq-server`

```sh
pip install "cronoq[client]"
```

```python
from cronoq.client import Client, Worker, task

# Submit and inspect (client token).
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

- Payloads: `bytes` are sent as-is, `str` as UTF-8 text, other values as JSON
  (`application/json`). Outputs are encoded the same way.
- Handlers raise `cronoq.Retryable(msg, retry_after=...)` or `cronoq.Permanent(msg)`; any other
  exception is a retryable failure.
- Each running lease is heartbeated every `lease_ttl / 3`; when the server aborts an attempt
  (timeout, cancellation, lease expiry) the handler task is cancelled and `lease.cancelled` is
  set.
- Failed calls raise `cronoq.client.ClientError` with the gRPC status name in `.code`
  (e.g. `unauthenticated`, `resource_exhausted` when a pool is full); `Client.wait` raises
  `TimeoutError` on timeout.

## Migrating from `ElasticPriorityScheduler`

```python
from cronoq.compat import ElasticPriorityScheduler, Priority, Task
```

A synchronous, thread-safe drop-in for the original API. See the
[migration guide](https://synaptiai.github.io/cronoq/use-cases/migrating.html).

## Development

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install maturin pytest pytest-asyncio grpcio protobuf
maturin develop -m crates/cronoq-py/Cargo.toml
cd crates/cronoq-py && python -m pytest
```

`python/tests/test_client.py` starts a real `cronoq-server` (building it with cargo if needed,
or using `CRONOQ_SERVER_BIN`). The stubs in `cronoq/_proto` are generated by
`scripts/gen_proto.sh`. See [CONTRIBUTING.md](https://github.com/synaptiai/cronoq/blob/main/CONTRIBUTING.md).

## License

MIT OR Apache-2.0.
