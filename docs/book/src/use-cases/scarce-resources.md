# GPUs and other scarce resources

## The problem

Some work needs something there is little of: GPU slots, database connections, a licensed tool,
a partner API with a concurrency cap. Workers usually enforce such limits locally (a semaphore
per process), which over-subscribes the resource as soon as you run more than one worker, or
under-uses it when the limit is split statically.

Worse, a FIFO queue blocks: if the task at the head needs the GPU and the GPU is busy, the CPU
tasks behind it wait too.

## The setup

Declare the resource once on the scheduler; tasks request units of it:

```rust,ignore
use cronoq::{Pool, ResourceConfig, Scheduler, Task};

let sched = Scheduler::<Job, Out>::builder()
    .resource(ResourceConfig::semaphore("gpu", 4))          // 4 GPUs in total
    .resource(ResourceConfig::semaphore("warehouse-db", 10)) // 10 connections
    .pool(Pool::new("ml").workers(1, 4, 16).executor_fn(run_ml))
    .pool(Pool::new("etl").workers(1, 4, 32).executor_fn(run_etl))
    .build().await?;

sched.submit(Task::new(train).pool("ml").resource("gpu", 2)).await?;        // needs 2 GPUs
sched.submit(Task::new(export).pool("etl").resource("warehouse-db", 1)).await?;
sched.submit(Task::new(features).pool("ml").resource("gpu", 1).resource("warehouse-db", 1)).await?;
```

In the server configuration:

```toml
[[resources]]
kind = "semaphore"
name = "gpu"
permits = 4
```

## How it works

- At dispatch, every resource a task requests is acquired **atomically**: all units or none.
- If they are not available, the task is **parked** on that resource and removed from the
  dispatch queue, so tasks behind it (needing other resources or none) keep flowing. There is no
  head-of-line blocking.
- Semaphore units are released at the end of each attempt (success, failure, timeout, cancel).
  Parked tasks are re-offered in FIFO order as units become free, and only when their full
  request fits.
- A request larger than the resource's capacity is rejected at submission
  (`ResourceExceedsCapacity`), instead of waiting forever.

## What you get

- **Global limits** across all pools, workers and machines (with the server), enforced in one
  place.
- **Full utilization of the scarce resource** with no static splitting between workers.
- **No head-of-line blocking** for work that does not need the resource.
- **Fair access** in FIFO order among tasks waiting for the same resource.
- **Multi-resource requests** (for example GPU + database connection) without deadlock, because
  acquisition is all-or-nothing.
