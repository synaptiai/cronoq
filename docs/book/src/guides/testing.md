# Testing your executors

## Rust: paused time

The runtime uses tokio's clock, so tests with paused time run schedules, timeouts and backoffs
instantly and deterministically:

```rust,ignore
#[tokio::test(start_paused = true)]
async fn retries_then_succeeds() {
    let s = Scheduler::<u64, u64>::builder()
        .pool(Pool::fixed("default", 1).executor_fn(|ctx, x: Arc<u64>| async move {
            if ctx.attempt < 3 { Err(TaskError::retryable("flaky")) } else { Ok(*x) }
        }))
        .build()
        .await
        .unwrap();
    let h = s.submit(Task::new(7)).await.unwrap();
    assert_eq!(*h.await.unwrap(), 7);   // backoffs elapse in virtual time
}
```

Fixed starts and cron schedules work the same way: `tokio::time::sleep(Duration::from_secs(60))`
inside a paused test advances the scheduler's clock by a minute without waiting.

## Rust: the engine directly

`cronoq-core` is a pure state machine. Drive it with explicit times and inspect the actions it
emits; this is how cronoq's own deterministic simulation tests work:

```rust,ignore
use cronoq_core::*;

let mut core: Core<&str> = Core::new(CoreConfig::default());
core.submit(Time::ZERO, TaskSpec::new("a").priority(Priority::High)).unwrap();
let Some(Action::Dispatch(d)) = core.poll_action() else { panic!() };
core.complete(Time::from_secs_f64(0.5), d.lease, Outcome::Success);
core.check_invariants();
```

## Python

Run executors through a real scheduler inside an asyncio test (pytest-asyncio):

```python
import cronoq, pytest

@pytest.mark.asyncio
async def test_executor_retries():
    calls = 0
    async def run(ctx, x):
        nonlocal calls
        calls += 1
        if ctx.attempt < 2:
            raise cronoq.Retryable("flaky")
        return x * 2
    retry = cronoq.RetryPolicy(max_attempts=3, initial_backoff=0.01, jitter=False)
    async with cronoq.Scheduler([cronoq.Pool("default", run, retry=retry)]) as s:
        assert await (await s.submit(21)) == 42
    assert calls == 2
```

## Against a server

Start `cronoq-server` with a temporary config on free ports (`grpc_addr = "127.0.0.1:0"` works
when you embed it with `cronoq_server::run`), connect a test worker, and submit through the
client. The repository's `crates/cronoq-server/tests/server.rs` and
`crates/cronoq-py/python/tests/test_client.py` are complete examples.
