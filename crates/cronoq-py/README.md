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

* Executors are `async def run(ctx, payload)` coroutines and run on the event loop that called
  `start()`. Plain functions are accepted too and run in the loop's default thread pool.
* Payloads and results are arbitrary Python objects (never serialized).
* `cronoq.Retryable` / `cronoq.Permanent` control retries; any other exception is retryable.
  Awaiting a task that did not succeed raises `cronoq.TaskFailed` (`.status`, `.message`).
* Cancelling a task, a timeout, or a lost lease cancels the executor's asyncio task.
* `cronoq.compat.ElasticPriorityScheduler` is a synchronous drop-in for the original prototype.

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install maturin pytest pytest-asyncio
.venv/bin/maturin develop -m crates/cronoq-py/Cargo.toml
cd crates/cronoq-py && ../../.venv/bin/python -m pytest
```

`cargo test --workspace` builds this crate as well (it has no Rust tests); the
`extension-module` feature is only enabled by maturin.
