# Go API

The Go SDK (`github.com/synaptiai/cronoq/sdks/go`, package `cronoq`) is a client for a
[cronoq server](../guides/server-quickstart.md): it submits, inspects, waits for and watches
tasks, and runs workers. It needs Go 1.26 or newer.

```sh
go get github.com/synaptiai/cronoq/sdks/go
```

The generated gRPC stubs are in the `cronoqpb` package, for calls the SDK does not wrap (such
as `AddRecurring` or `ListWorkers`); `Client.Conn()` gives the connection to use with them.

## Submitting and waiting

```go
c, err := cronoq.Dial("http://127.0.0.1:7070", cronoq.Options{Token: os.Getenv("CRONOQ_TOKEN")})
if err != nil {
	return err
}
defer c.Close()

sub, err := c.Submit(ctx, cronoq.Task{Payload: []byte(`{"to":"ada"}`), Key: "welcome-ada", Priority: cronoq.High})
res, err := c.Wait(ctx, cronoq.ID(sub.ID), 30*time.Second)
if err == nil && res.OK() {
	fmt.Println(string(res.Output))
}
```

### `Dial(target, Options)`

`target` is `http://host:port`, `https://host:port` or `host:port` (plain). The connection is
opened on the first call.

| Option | Default | Description |
|---|---|---|
| `Token` | none | Bearer token sent with every call. A token over plain `http://` to a host other than loopback logs a warning. |
| `TLS` | system roots | `*tls.Config` for `https://` targets (CA, client certificate for mTLS, server name). It needs an `https://` target. |
| `MaxMessageBytes` | 64 MiB | Largest message received or sent. |
| `DialOptions` | none | Extra `grpc.DialOption`s. |
| `Logger` | `slog.Default()` | Receives warnings. |

### `Client`

| Method | Returns | Description |
|---|---|---|
| `Submit(ctx, Task)` | `Submitted` | Submit one task. `Submitted.Duplicate` is true when a task with the same key existed; its id is returned. |
| `SubmitAndWait(ctx, Task, timeout)` | `Submitted` | Submit and wait up to `timeout` (0: the server's `limits.max_wait_s`); `Submitted.Result` holds the result. |
| `SubmitSpec(ctx, *cronoqpb.TaskSpec, wait, timeout)` | `Submitted` | Submit a raw spec, for fields `Task` does not cover. |
| `SubmitBatch(ctx, []Task)` | `[]Submitted` | Submit atomically; tasks may depend on each other's keys. |
| `Get(ctx, Ref)` | `*TaskInfo` | A task's details. |
| `Wait(ctx, Ref, timeout)` | `*Result` | Wait for a task to finish. |
| `Cancel(ctx, Ref)` | `bool` | Cancel a task that has not finished. |
| `ListTasks(ctx, ListOptions)` | `*TaskPage` | One page of tasks by pool and status, in id order. Pass `NextAfter` as `After` for the next page. |
| `Watch(ctx, WatchOptions)` | `iter.Seq2[Event, error]` | Status events until `ctx` ends or the stream fails (for example when the server loses leadership: watch again). |
| `Snapshot(ctx)` | `*cronoqpb.SnapshotResponse` | Pools, queues and totals (metrics role). |

`Ref` names a task: `cronoq.ID(id)` or `cronoq.Key(key)`.

| `ListOptions` field | Default | Description |
|---|---|---|
| `Pools` | every pool the caller may use | Pools to list. |
| `Statuses` | any | Statuses to list. |
| `After` | 0 | Only tasks with a larger id; pass the previous page's `NextAfter`. |
| `Limit` | 0 (= 100) | Page size, capped at the server's `limits.max_list`. |

`TaskPage.NextAfter` is 0 on the last page.

| `WatchOptions` field | Default | Description |
|---|---|---|
| `Pools` | every pool the caller may use | Pools to watch. |
| `IDs`, `Keys` | none | Only events for these tasks. |
| `AllTransitions` | `false` | Stream every status change; without it, only terminal statuses are streamed. |

`Task` fields: `Payload`, `ContentType`, `Kind`, `Pool`, `Key`, `Tenant`, `Priority`
(`cronoq.Critical`, `High`, `Normal`, `Low`), `Timeout` (per attempt), `Deadline` (relative),
`StartIn`, `After` (`[]Dep{{Ref: cronoq.Key("extract"), Cond: cronoq.OnSuccess}}`),
`Resources`, `Tags`, `Labels` (worker labels the task requires) and `MaxAttempts` (replaces the
pool's retry policy with this many attempts and the server's default backoff).

`TaskInfo`, `Result` and `Event` carry a `Status` (`cronoq.Ready`, `Running`, `Succeeded`, ...;
`Status.Done()` is true for terminal ones). `TaskPage.UnavailableShards` names shards that could
not be reached ([Sharding](../concepts/sharding.md)).

### Errors

Failed calls return a `*cronoq.Error` with the gRPC `Code` and `Message`
([Errors](errors.md)):

```go
var e *cronoq.Error
if errors.As(err, &e) && e.Timeout() { /* the task is still running */ }
```

`Retryable()` is true for `UNAVAILABLE` (not the leader, shutting down) and
`RESOURCE_EXHAUSTED` (queue full); `NotFound()` for unknown tasks, pools and dependencies.

## Workers

```go
err := cronoq.RunWorker(ctx, cronoq.WorkerOptions{
	Target: "http://127.0.0.1:7070",
	Pool:   "default",
	Slots:  8,
	Token:  os.Getenv("CRONOQ_WORKER_TOKEN"),
}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
	return process(ctx, l.Payload)
})
```

`RunWorker` registers for the pool and runs the handler for each lease, at most `Slots` at a
time (or fewer if the server grants fewer). It heartbeats every running lease, and reconnects
with backoff when the connection drops (right away when the server announces a leadership
change). When the pool belongs to another shard, it connects to that shard's endpoint; see
below. When `ctx` ends, it stops taking leases and lets running handlers finish for up to
`DrainTimeout`, then cancels the rest and returns `nil`. A cancelled handler that returns within
5 more seconds still has its outcome reported; after that its outcome is dropped (the server
retries the task). It returns an error only for failures that retrying cannot fix, such as a
rejected token or an unknown pool.

| Option | Default | Description |
|---|---|---|
| `Target`, `Pool` | required | Server and pool. |
| `Slots` | 1 | Leases run at once. |
| `Token` | none | Bearer token with the worker role. |
| `Name` | empty | Shown in server logs and worker listings. |
| `Labels` | none | Worker labels ([Worker labels](../concepts/labels.md)). |
| `TLS` | system roots | `*tls.Config` for `https://` targets. A `ServerName` here applies to `Target` only. |
| `NoReconnect` | `false` | Return when the connection fails instead of reconnecting. |
| `InitialBackoff`, `MaxBackoff` | 200 ms, 10 s | Reconnect delay bounds. |
| `DrainTimeout` | 30 s | How long running handlers may finish after `ctx` ends. |
| `InsecureRedirectHTTP` | `false` | Follow a shard redirect to plain `http://` on another host with the token. For trusted networks only. |
| `MaxMessageBytes` | 64 MiB | Largest lease message accepted from the server. |
| `DialOptions` | none | Extra `grpc.DialOption`s. |
| `Logger` | `slog.Default()` | Receives the worker's log messages. |

### Handlers

The handler gets a `*Lease`: `ID`, `TaskID`, `Attempt`, `Payload`, `ContentType`, `Kind`, `Key`,
`Pool`, `Priority`, `Tags`, `Timeout` and `Deadline`. Its context ends when the server aborts the
attempt (timeout, cancellation, lease expiry), when the connection to the server ends, or when
the drain timeout passes. After an abort the outcome is not reported; after the drain timeout it
is reported if the handler returns within 5 seconds. It returns the output, or an error:

| Return | Effect |
|---|---|
| `output, nil` | The task succeeds. An output larger than the server accepts fails it permanently. |
| `cronoq.Permanent(err)` | The task fails without retrying. |
| `cronoq.RetryAfter(err, d)` | Retryable failure, retried no earlier than `d`. |
| `cronoq.Reject(reason)` | Hand the lease back unrun; the server offers it to another worker. |
| `cronoq.ErrCancelTask` | The task ends as cancelled. |
| any other error, or a panic | Retryable failure. |

`(Lease.TaskID, Lease.Attempt)` is a fencing token: attempt numbers never repeat for a task,
also across failovers. Record the pair with the handler's side effect and let only the newest
attempt write ([Exactly-once side effects](../concepts/reliability.md#exactly-once-side-effects)).

### Shards

A worker that connects to another shard's server is refused with the endpoint of its pool's
shard, and `RunWorker` connects there (up to `cronoq.MaxRedirects` times in a row). It never
follows a redirect from `https://` to `http://`, sends its token over plain `http://` only to
loopback unless `InsecureRedirectHTTP` is set, and refuses endpoints with userinfo or a path.
When that shard cannot be reached, it asks `Target` again. See [Sharding](../concepts/sharding.md).
