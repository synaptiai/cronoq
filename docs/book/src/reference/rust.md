# Rust API

The full API documentation is on docs.rs. This page maps the crates and their main types.

| Crate | docs.rs | Use it for |
|---|---|---|
| `cronoq` | [docs.rs/cronoq](https://docs.rs/cronoq) | Embedding the scheduler in a tokio application. Start here. |
| `cronoq-core` | [docs.rs/cronoq-core](https://docs.rs/cronoq-core) | The sans-IO engine: custom drivers, simulations, tests. |
| `cronoq-store-postgres` | [docs.rs/cronoq-store-postgres](https://docs.rs/cronoq-store-postgres) | Durable state and HA for `Scheduler<Bytes, Bytes>`. |
| `cronoq-proto` | [docs.rs/cronoq-proto](https://docs.rs/cronoq-proto) | Generated gRPC clients and servers. |
| `cronoq-server` | [docs.rs/cronoq-server](https://docs.rs/cronoq-server) | The server binary, plus `worker::run_worker` for Rust workers. |

## `cronoq`

| Type | Role |
|---|---|
| `Scheduler<P, O>` | Handle to a running scheduler (cheap to clone). `submit`, `enqueue`, `enqueue_many`, `submit_batch`, `handle`, `cancel`, `status`, `snapshot`, `set_capacity`, `add_recurring`, `remove_recurring`, `events`, `stopped`, `shutdown`. |
| `Builder<P, O>` | `pool`, `resource`, `priority`, `urgency`, `store`, `emit_transitions`, `queue_capacity`, `configure`, `build`, `build_blocking`. |
| `Pool<P, O>` | `new` (autoscaled), `fixed`, `workers(min, initial, max)`, `executor`, `executor_fn`, `scaler`, `autoscale`, `no_autoscale`, `configure`. |
| `Task<P>` | Task builder: `priority`, `pool`, `kind`, `key`, `deadline_in/at`, `start_in/at`, `start_tolerance`, `on_miss`, `duration_hint`, `timeout`, `retry`, `no_retry`, `after`, `dep`, `resource`, `tag`. |
| `Recurring<P>` | Cron schedule: `new(name, cron, template)`, `tz`, `overlap`, `tolerance`, `on_miss`. |
| `TaskHandle<O>` | `Future<Output = Result<Arc<O>, TaskFailure>>`; `id`, `is_duplicate`, `cancel`. |
| `Executor<P, O>` / `FnExecutor` | Runs attempts; `execute` and optional `abort`. |
| `TaskContext` | Attempt information, cancellation, heartbeat. |
| `TaskError` | `Retryable`, `Permanent`, `Cancelled`; constructors `retryable`, `retry_after`, `permanent`, `from_err`. |
| `TaskFailure` | `Ended { status, message }` or `Shutdown`. |
| `Error` | `Submit(SubmitError)`, `Shutdown`, `Config`, `Store`. |
| `Scaler`, `SlotScaler`, `ExternalScaler` | Capacity backends. |
| `Store<P, O>`, `MemoryStore`, `TimeBase` | Persistence backends. |
| `BlockingScheduler`, `BlockingHandle` | Synchronous facade. |

Engine types (`Priority`, `RetryPolicy`, `AutoscaleConfig`, `PoolConfig`, `ResourceConfig`,
`Snapshot`, `TaskStatus`, `StatusEvent`, ...) are re-exported from `cronoq-core`; the whole
engine is available as `cronoq::core`.

### Feature flags

| Feature | Default | Effect |
|---|---|---|
| `otel` | on | OpenTelemetry metrics and trace propagation. |

### MSRV

Rust 1.85 (edition 2024).

## `cronoq-core`

`Core<P>` is a deterministic state machine. Every method takes the current `Time`:

| Method | Description |
|---|---|
| `submit`, `try_submit`, `submit_batch` | Admission. |
| `complete(now, lease, Outcome)`, `heartbeat(now, lease)` | Attempt feedback. |
| `cancel`, `status`, `resolve` | Lookups and cancellation. |
| `set_capacity(now, pool, n)` | Capacity changes. |
| `add_recurring`, `remove_recurring` | Cron schedules. |
| `tick(now)`, `next_wakeup()` | Timers. |
| `poll_action()` / `drain_actions()` | `Action::{Dispatch, Abort, ScaleTo, Status, Persist, SpaceAvailable}` for the driver. |
| `snapshot(now)` | Metrics. |
| `recover(now, Recovered)` | Restore from a store. |
| `check_invariants()` | Panics on internal inconsistency (tests). |
