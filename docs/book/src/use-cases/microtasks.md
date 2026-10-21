# High-throughput microtasks

## The problem

When tasks take milliseconds (cache refreshes, notifications, small transforms, fan-out of
events), the scheduler's own overhead can dominate. Broker-based queues spend more time on
network round trips than on the work, and many schedulers rescan or re-sort their queues
periodically, so their cost grows with the backlog.

## The setup

Embed the scheduler and submit in bulk:

```rust,ignore
use std::sync::Arc;
use cronoq::{Pool, Priority, Scheduler, Task, TaskError};

let sched = Scheduler::<Event, ()>::builder()
    .pool(Pool::fixed("default", 256).executor_fn(|_ctx, e: Arc<Event>| async move {
        handle(&e).await;
        Ok::<_, TaskError>(())
    }))
    .queue_capacity(65_536)
    .build()
    .await?;

// One round trip for up to thousands of tasks.
let batch: Vec<_> = events.into_iter().map(|e| Task::new(e).priority(Priority::Normal)).collect();
sched.enqueue_many(batch).await?;
```

Runnable: `cargo run --release -p cronoq --example firehose -- 1000000`.

## Why it is fast

- **Constant-time dispatch.** Each priority class keeps a FIFO lane. Because every task in a
  class ages at the same rate, the oldest task is always the best one, so dispatch only compares
  the heads of at most eight queues: no periodic rescoring, no full-queue scans.
- **Single-threaded engine, no locks.** The engine runs on one task and receives commands in
  batches over channels.
- **No serialization in-process.** Payloads are shared via `Arc`.
- **`enqueue` and `enqueue_many`** skip result handles when you do not need outputs.

Measured on a shared 4-vCPU VM:

| | Throughput |
|---|---|
| Engine only (submit → dispatch → complete) | ~1.0–1.2 million cycles/s per core |
| End to end through tokio with no-op tasks | ~250,000 tasks/s |

## What you get

- **Scheduling overhead in the microseconds**, independent of backlog size.
- **All the scheduling rules still apply** (priorities, deadlines, resources, retries) at this
  rate.
- **Backpressure** via bounded queues instead of unbounded memory growth during spikes.

## Tips

- Prefer `enqueue_many` over many single `enqueue` calls; each call is a channel round trip.
- Size `queue_capacity` to your burst size.
- Disable the `otel` feature if you do not export metrics.
- See [Performance tuning](../guides/performance.md).
