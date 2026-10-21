# Writing executors and workers

An **executor** runs attempts in-process (library mode). A **worker** runs attempts in another
process and talks to `cronoq-server` (server mode). The rules are the same: run the payload,
report an outcome, respect cancellation, and heartbeat long attempts when a lease TTL applies.

## Rust executors

Most executors are closures:

```rust,ignore
Pool::new("default").executor_fn(|ctx: TaskContext, job: Arc<Job>| async move {
    tokio::select! {
        r = do_work(&job) => r.map_err(TaskError::from_err),
        _ = ctx.cancelled() => Err(TaskError::Cancelled),
    }
})
```

For more control, implement the trait:

```rust,ignore
use std::sync::Arc;
use cronoq::{BoxFuture, Executor, LeaseId, TaskContext, TaskError};

struct HttpExecutor { client: reqwest::Client }

impl Executor<Request, Response> for HttpExecutor {
    fn execute(&self, ctx: TaskContext, req: Arc<Request>) -> BoxFuture<Result<Response, TaskError>> {
        let client = self.client.clone();
        Box::pin(async move { /* ... */ })
    }

    // Optional: release per-lease resources synchronously when the engine abandons an attempt.
    fn abort(&self, _lease: LeaseId) {}
}
```

`TaskContext` provides `id`, `lease`, `attempt`, `pool`, `kind`, `key`, `tags`, `deadline`,
`timeout`, `is_cancelled()`, `cancelled()`, `cancellation_token()`, `heartbeat()`, `tag(name)`
and `traceparent()`.

Guidelines:

- **Be idempotent.** Execution is at-least-once.
- **Classify errors.** Use `permanent` for errors a retry cannot fix (validation, 4xx), and
  `retry_after` when the remote side says when to come back.
- **Do not block the runtime.** Use `tokio::task::spawn_blocking` for CPU-heavy or blocking work.
- **Check `ctx.deadline`** to pick a faster path when time is short.

## Python executors

```python
async def run(ctx: cronoq.TaskContext, payload):
    if ctx.attempt > 1:
        log.info("retrying %s", ctx.id)
    for chunk in work(payload):
        await process(chunk)
        ctx.heartbeat()                 # only needed with lease_ttl
    return result
```

Plain (non-async) functions run in the event loop's default thread pool. When the engine
cancels an attempt (cancellation, timeout, lease loss) the executor's asyncio task is cancelled,
so `asyncio.CancelledError` is raised at the next `await`.

## Remote workers: Python

```python
from cronoq.client import Worker
import cronoq

async def handle(lease):
    # lease.id, .task_id, .attempt, .payload (bytes), .json(), .text(), .content_type,
    # .kind, .key, .tags, .timeout, .deadline, .cancelled (asyncio.Event)
    try:
        return await process(lease.json())          # bytes, str, JSON-serializable or None
    except TransientError as e:
        raise cronoq.Retryable(str(e), retry_after=5)

worker = Worker("cronoq:7070", "default", handle, slots=8, token="worker-token",
                tls=True, reconnect=True, grace=30)
await worker.run()          # until worker.stop()
```

The worker heartbeats every running lease every `lease_ttl / 3`, cancels the handler on
`Abort`, reconnects with exponential backoff, and on `stop()` stops accepting leases and drains
running ones for up to `grace` seconds.

## Remote workers: Rust

```rust,ignore
use bytes::Bytes;
use cronoq::TaskError;
use cronoq_server::worker::{run_worker, WorkerOptions};

let opts = WorkerOptions::new("https://cronoq:7070", "default").slots(8).token(token).name("w1");
run_worker(opts, |lease| async move {
    // lease.lease, task_id, attempt, payload, content_type, kind, key, tags, timeout, cancel
    let out = process(&lease.payload).await.map_err(TaskError::from_err)?;
    Ok(Bytes::from(out))
}, shutdown_signal()).await?;
```

## Any language over gRPC

Workers in other languages implement one bidirectional stream, `cronoq.v1.Workers/Work`, from
[`cronoq.proto`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-proto/proto/cronoq/v1/cronoq.proto).
Print it with `cronoq-server --print-proto` and generate stubs with your language's protobuf
tooling.

1. Open the stream with metadata `authorization: Bearer <worker token>`.
2. Send `WorkerMessage{register: {pool, slots, worker_name}}` first.
3. Receive `ServerMessage{registered: {worker_id, lease_ttl_s}}`.
4. For each `ServerMessage{lease: Lease}`:
   - run the task (`payload`, `content_type`, `kind`, `key`, `tags`, `timeout_s`,
     `deadline_unix_ns`);
   - send `heartbeat{lease}` at least every `lease_ttl_s` while it runs;
   - send `complete{lease, output}`, or `fail{lease, message, permanent, retry_after_s}`.
5. On `ServerMessage{abort: {lease, reason}}`, stop that attempt and send nothing for it.
6. Send `set_slots{slots}` to change concurrency; `0` drains the worker.
7. Never run more than `slots` leases at once (the server does not send more).

If the stream drops, in-flight leases are retried elsewhere. Reconnect with backoff.
