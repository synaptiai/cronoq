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
- **Panics are contained.** A panicking executor fails its task permanently; the scheduler
  and other attempts keep running.
- **Heartbeat when the pool has a lease TTL.** Long attempts call `ctx.heartbeat()`, or they
  are failed as retryable when the TTL expires.

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
import os
import cronoq
from cronoq.client import CancelTask, Reject, Worker

async def handle(lease):
    # lease.id, .task_id, .attempt, .payload (bytes), .json(), .text(), .content_type, .kind,
    # .key, .tags, .timeout, .deadline, .pool, .lease_ttl, .cancelled (asyncio.Event)
    if overloaded():
        raise Reject("local GPU busy")              # hand it to another worker
    try:
        return await process(lease.json())          # bytes, str, JSON-serialisable or None
    except TransientError as e:
        raise cronoq.Retryable(str(e), retry_after=5)
    except ValidationError as e:
        raise cronoq.Permanent(str(e))

worker = Worker("cronoq:7070", "default", handle, slots=8,
                token=os.environ["CRONOQ_WORKER_TOKEN"], tls=True, grace=30)
await worker.run()          # until worker.stop()
```

The worker:

- runs at most `slots` handlers at once, or fewer if the server grants fewer
  (`worker.granted_slots`);
- heartbeats every running lease every `lease_ttl / 3`;
- cancels the handler's task and sets `lease.cancelled` when the server aborts an attempt;
- reconnects with exponential backoff when the stream drops, and right away when the server
  announces a leadership change;
- on `stop()`, stops taking leases, lets running ones finish for up to `grace` seconds, then
  cancels the rest.

Raise `Reject` to give a lease back unrun (the server offers it to another worker of the pool
without counting an attempt), and `CancelTask` to end the task as `cancelled`. For mTLS, pass
`credentials=grpc.ssl_channel_credentials(root, key, cert)`.

## Remote workers: Rust

```rust,ignore
use bytes::Bytes;
use cronoq::TaskError;
use cronoq_server::worker::{run_worker, WorkerOptions, WorkerTls};

let opts = WorkerOptions::new("https://cronoq.internal:7070", "default")
    .slots(8)
    .token(token)
    .name("w1")
    .tls(WorkerTls { ca_pem: Some(ca), identity: None, domain: None })
    .drain_timeout(Duration::from_secs(60));
run_worker(opts, |lease| async move {
    // lease.lease, task_id, attempt, pool, payload, content_type, kind, key, tags,
    // timeout, deadline, cancel (a CancellationToken)
    tokio::select! {
        out = process(&lease.payload) => out.map(Bytes::from).map_err(TaskError::from_err),
        _ = lease.cancel.cancelled() => Err(TaskError::Cancelled),
    }
}, shutdown_signal()).await?;
```

`run_worker` enforces the granted slots, heartbeats each lease, cancels `lease.cancel` on
`Abort`, turns a panicking handler into a retryable failure, reconnects with backoff
(`initial_backoff` 200 ms up to `max_backoff` 10 s), and when `shutdown` resolves drains running
handlers for up to `drain_timeout` (30 s). It returns an error only for problems retrying cannot
fix, such as a rejected token or an unknown pool. Use `identity: Some((cert_pem, key_pem))` for
mTLS. TLS settings need an `https://` endpoint: with `http://` the worker returns an error
rather than connecting in plaintext.

## Any language over gRPC

Workers in other languages implement one bidirectional stream, `cronoq.v1.Workers/Work`, from
[`cronoq.proto`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-proto/proto/cronoq/v1/cronoq.proto).
Print it with `cronoq-server --print-proto` and generate stubs with your language's protobuf
tooling.

1. Open the stream with metadata `authorization: Bearer <worker token>` (or a client
   certificate).
2. Send `register{pool, slots, worker_name, protocol_version: 2}` first.
3. Receive `registered{worker_id, lease_ttl_s, slots, protocol_version, max_message_bytes}`; run
   at most the granted `slots` leases at once.
4. For each `lease`:
   - run the task (`payload`, `content_type`, `kind`, `key`, `tags`, `timeout_s`,
     `deadline_unix_ns`);
   - send `heartbeat{lease}` well within `lease_ttl_s` while it runs;
   - finish with exactly one of `complete{lease, output}`,
     `fail{lease, message, permanent, retry_after_s}`, `nack{lease, reason}` (give it back) or
     `cancelled{lease}`.
5. On `abort{lease, reason}`, stop that attempt; its slot is already free.
6. Send `set_slots{slots}` to change concurrency; `0` drains the worker.
7. On `shutdown{reason, not_leader}`, or when the stream ends, reconnect with backoff.

In-flight leases of a dropped stream are failed as retryable and retried elsewhere. The full
rules are in the [gRPC reference](../reference/grpc.md#protocol-rules).
