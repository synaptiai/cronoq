# Performance tuning

## What to expect

| Path | Typical cost |
|---|---|
| Engine decision (submit, dispatch or complete) | under 1 µs |
| Engine cycle (submit → dispatch → complete), one core | ~1.0–1.2 M/s |
| End-to-end in-process, no-op tasks, bulk submit | ~250 k tasks/s |
| Sequential `submit().await` then `handle.await` | ~70 µs round trip |

Dispatch cost is independent of backlog size (see [Architecture](../architecture.md)).

## Tuning the library

- **Submit in bulk.** `enqueue_many(Vec<Task>)` admits thousands of tasks in one channel round
  trip; individual `submit` calls each wait for a reply.
- **Skip handles you do not need.** `enqueue` is cheaper than `submit` (no result waiter).
- **Size the command queue.** `Builder::queue_capacity(n)` (default 8192) should cover bursts.
- **Disable telemetry you do not export.** Build with `default-features = false` to remove
  OpenTelemetry instrumentation.
- **Keep executors async.** A blocking executor holds a tokio worker thread; move CPU-heavy work to
  `spawn_blocking` or a dedicated pool.
- **Prefer fewer, larger pools.** Each pool adds queues and an autoscaling timer; resources can
  express most sharing needs.
- **Retention.** Terminal statuses are kept for an hour (up to 1 M entries) for dependencies and
  deduplication. Lower `CoreConfig::retention` / `retention_max` for very high-rate workloads that
  do not need them.

## Tuning the server

- Give workers enough `slots` that per-lease round trips are amortized; heartbeats are cheap.
- With a durable store (SQLite or Postgres), commit latency bounds how fast the engine can
  acknowledge submissions in a single step. Batched submissions (`SubmitBatch`, `/v1/tasks:batch`) share one commit.
- Keep the database close (same region/zone) to the server.

## Measuring

```sh
cargo bench -p cronoq-core --bench core      # engine
cargo bench -p cronoq --bench runtime        # end to end
cargo run --release -p cronoq --example firehose -- 1000000
```

Shared or virtualized machines are noisy; compare instruction counts (`valgrind --tool=callgrind`
on `crates/cronoq-core/examples/prof.rs`) when optimizing.
