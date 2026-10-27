# Rust quickstart

## Install

```toml
[dependencies]
cronoq = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The `otel` feature (on by default) adds OpenTelemetry metrics and trace propagation. Disable it
with `default-features = false` for a minimal dependency tree.

The minimum supported Rust version is **1.94**.

## A first scheduler

```rust,ignore
use std::{sync::Arc, time::Duration};
use cronoq::{Pool, Priority, Scheduler, Task, TaskError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A scheduler with one pool. Payloads are `String`s, outputs are `usize`s.
    let sched = Scheduler::<String, usize>::builder()
        .pool(
            Pool::new("default")
                .workers(1, 2, 8) // min, initial, max concurrent tasks
                .executor_fn(|ctx, text: Arc<String>| async move {
                    println!("task {} attempt {}", ctx.id, ctx.attempt);
                    Ok::<_, TaskError>(text.len())
                }),
        )
        .build()
        .await?;

    // Submit and await the output.
    let handle = sched
        .submit(Task::new("hello".to_string()).priority(Priority::High))
        .await?;
    let len = handle.await?; // Arc<usize>
    assert_eq!(*len, 5);

    sched.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
```

What happened:

1. `Scheduler::builder()` creates a scheduler for payload type `String` and output type `usize`.
2. `Pool::new("default")` is an autoscaled pool of *worker slots*. In-process, a slot is simply a
   concurrently running task, so `workers(1, 2, 8)` means "start with 2 concurrent tasks, scale
   between 1 and 8 based on queue pressure".
3. `executor_fn` turns an async closure into an [`Executor`](../reference/rust.md). It receives a
   [`TaskContext`](../reference/rust.md) (id, attempt, cancellation, heartbeat) and the payload.
4. `submit` returns a `TaskHandle` that resolves with `Result<Arc<O>, TaskFailure>` once the task
   reaches a terminal status.

## Adding scheduling rules

Everything else is set on the task or the pool:

```rust,ignore
use std::time::Duration;
use cronoq::{Dep, Priority, RetryPolicy, Task};

let task = Task::new(payload)
    .pool("default")
    .kind("summarize")                         // used for metrics and duration learning
    .priority(Priority::High)
    .key("summary:doc-42")                     // idempotent submit + dependency handle
    .deadline_in(Duration::from_secs(30))      // urgency rises as the deadline approaches
    .timeout(Duration::from_secs(60))          // per attempt
    .retry(RetryPolicy { max_attempts: 5, ..Default::default() })
    .resource("llm-api", 1)                    // shared rate limit / semaphore
    .after("fetch:doc-42")                     // run after this key succeeds
    .dep(Dep::on_failure("fetch:doc-42"));     // ...or a failure handler edge
```

Tasks that must start at a given time use `start_in` / `start_at`; recurring work uses
`Scheduler::add_recurring` with a cron expression. See [Fixed starts](../concepts/fixed-starts.md)
and [Recurring schedules](../concepts/recurring.md).

## Signalling failures

Return a [`TaskError`](../reference/errors.md) from the executor:

| Return | Effect |
|---|---|
| `Ok(output)` | Task succeeds; handles resolve with the output. |
| `Err(TaskError::retryable(msg))` | Retried with exponential backoff until the retry policy is exhausted, then `DeadLettered`. |
| `Err(TaskError::retry_after(msg, d))` | Retryable, but not before `d` (e.g. HTTP 429 `Retry-After`). |
| `Err(TaskError::permanent(msg))` | Fails immediately with status `Failed`. |
| panic | Treated as a permanent failure with the panic message. |

## Blocking (non-async) code

If your application does not use tokio, build a `BlockingScheduler`. It owns a private runtime
and exposes the same operations as blocking calls:

```rust,ignore
let sched = cronoq::Scheduler::<u64, u64>::builder()
    .pool(cronoq::Pool::fixed("default", 4).executor_fn(|_ctx, x: std::sync::Arc<u64>| async move {
        Ok::<_, cronoq::TaskError>(*x * 2)
    }))
    .build_blocking()?;
let out = sched.submit(cronoq::Task::new(21))?.wait()?;
assert_eq!(*out, 42);
```

Do not call `build_blocking` from inside a tokio runtime; use the async API there.

## Examples in the repository

| Example | Shows |
|---|---|
| `cargo run -p cronoq --example llm_agents` | Rate limits, priorities, deadlines, 429 retries |
| `cargo run -p cronoq --example dag_pipeline` | Fan-out/fan-in with failure and cleanup handlers |
| `cargo run --release -p cronoq --example firehose -- 1000000` | Microtask throughput |
| `cargo run -p cronoq --example prototype_demo` | Mixed workload with a fixed-start task and quality metrics |

Next: [use cases](../use-cases/index.md) for complete setups, or [concepts](../concepts/tasks.md)
for the precise rules.
