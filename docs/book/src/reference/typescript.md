# TypeScript API

The TypeScript SDK (npm package `cronoq`, in `sdks/typescript`) is a client for a
[cronoq server](../guides/server-quickstart.md), built on Connect-ES. It runs in two places:

| Import | Transport | What works |
|---|---|---|
| `cronoq/node` | gRPC over HTTP/2 (Node 22 or newer) | The client and workers. |
| `cronoq/web` | gRPC-Web from a browser | The client: unary calls and `watch`. Workers need a bidirectional stream, which gRPC-Web cannot carry. |

The package is not on npm yet; build it from the repository (`npm ci && npm run build` in
`sdks/typescript`).

## Submitting and waiting

```ts
import { connect, text } from "cronoq/node";

const c = connect("http://127.0.0.1:7070", { token: process.env.CRONOQ_TOKEN });
const sub = await c.submit({ payload: { to: "ada" }, key: "welcome-ada", priority: "high" });
const res = await c.wait(sub.id, 30);
if (res.status === "succeeded") console.log(text(res.output));
```

Task ids are `bigint`s: they use all 64 bits, which a `number` cannot hold. A task reference
is an id (`bigint`) or a key (`string`). Times are in seconds.

### `connect(endpoint, options)` (Node)

`endpoint` is `http://host:port` or `https://host:port`. The connection opens on the first call.

| Option | Default | Description |
|---|---|---|
| `token` | none | Bearer token sent with every call. A token over plain `http://` to a host other than loopback logs a warning. |
| `tls` | Node's roots | `{ ca, cert, key, servername }` (PEM) for `https://` endpoints; `cert` and `key` for mTLS. It needs an `https://` endpoint. |
| `maxMessageBytes` | 64 MiB | Largest message sent or received. |
| `logger` | `console` | Receives warnings. |

### `connectWeb(baseUrl, { token })` (browser)

`baseUrl` is the server's gRPC listener, for example `https://cronoq.example.com:7070`. The
server must list the page's origin in [`[server.grpc_web]`](configuration.md#servergrpc_web)
([gRPC-Web](grpc.md#grpc-web)). A token in a page is visible to whoever uses the page: give
browsers their own token, limited to the pools and role they need.

### `Client`

| Method | Resolves to | Description |
|---|---|---|
| `submit(task)` | `Submitted` | Submit one task. `duplicate` is true when a task with the same key existed; its id is returned. |
| `submitAndWait(task, waitTimeout?)` | `Submitted` | Submit and wait up to `waitTimeout` seconds (default: the server's `limits.max_wait_s`); `result` holds the result. |
| `submitBatch(tasks)` | `Submitted[]` | Submit atomically; tasks may depend on each other's keys. Accepts `Task`s or raw `pb.TaskSpec`s. |
| `get(ref)` | `TaskInfo` | A task's details. |
| `wait(ref, timeout?)` | `Result` | Wait for a task to finish. |
| `cancel(ref)` | `boolean` | Cancel a task that has not finished. |
| `listTasks({ pools, statuses, after, limit })` | `TaskPage` | One page of tasks, in id order. Pass `nextAfter` as `after` for the next page. |
| `watch({ pools, ids, keys, allTransitions }, { signal })` | `AsyncGenerator<Event>` | Status events until `signal` aborts or the stream fails (for example when the server loses leadership: watch again). |
| `snapshot()` | `pb.SnapshotResponse` | Pools, queues and totals (metrics role). |

Every method also takes `{ signal, timeout }` call options. `client.tasks` and `client.admin`
are the generated Connect clients, for calls the class does not wrap (such as `addRecurring` or
`listWorkers`); `pb` exports the generated messages.

`Task` fields: `payload`, `contentType`, `kind`, `pool`, `key`, `tenant`, `priority`
(`"critical"`, `"high"`, `"normal"`, `"low"`), `timeout` (per attempt), `deadline` (relative),
`startIn`, `after` (references, or `{ ref, cond }` with `cond` `"on_success"`, `"on_complete"`
or `"on_failure"`), `resources`, `tags`, `labels` (worker labels the task requires) and
`maxAttempts` (replaces the pool's retry policy with this many attempts and the server's default
backoff).

Payloads and outputs: a `Uint8Array` is sent as is, a string as UTF-8 text, `null` or
`undefined` as empty, and anything else as JSON. `text(bytes)` and `json(bytes)` decode them.

`TaskInfo`, `Result` and `Event` carry a `status` (`"ready"`, `"running"`, `"succeeded"`, ...;
`isDone(status)` is true for terminal ones). `TaskPage.unavailableShards` names shards that
could not be reached ([Sharding](../concepts/sharding.md)).

### Errors

Failed calls reject with a `CronoqError`: `code` is the gRPC status name in lower case
(`"not_found"`, `"unavailable"`, ...), and `metadata` holds the response metadata
([Errors](errors.md)). `retryable` is true for `unavailable` (not the leader, shutting down) and
`resource_exhausted` (queue full); `timeout` for a wait that ended while the task was still
running; `notFound` for unknown tasks, pools and dependencies. In browsers, only the metadata
the server exposes to CORS requests is visible.

## Workers (Node)

```ts
import { runWorker } from "cronoq/node";

const stop = new AbortController();
process.on("SIGTERM", () => stop.abort());
await runWorker(
  { target: "http://127.0.0.1:7070", pool: "default", slots: 8, token: process.env.CRONOQ_WORKER_TOKEN, signal: stop.signal },
  async (lease) => handle(lease.json()),
);
```

`runWorker` registers for the pool and runs the handler for each lease, at most `slots` at a
time (or fewer if the server grants fewer). It heartbeats every running lease, and reconnects
with backoff when the connection drops (right away when the server announces a leadership
change). When the pool belongs to another shard, it connects to that shard's endpoint; see
below. When `signal` aborts, it stops taking leases and lets running handlers finish for up to
`drainTimeout`, then aborts the rest and resolves. It rejects only for failures that retrying
cannot fix, such as a rejected token or an unknown pool.

| Option | Default | Description |
|---|---|---|
| `target`, `pool` | required | Server and pool. |
| `slots` | 1 | Leases run at once. |
| `token` | none | Bearer token with the worker role. |
| `name` | empty | Shown in server logs and worker listings. |
| `labels` | none | Worker labels ([Worker labels](../concepts/labels.md)). |
| `tls` | Node's roots | As for `connect`; `servername` applies to `target` only. TLS settings with an `http://` target are refused. |
| `signal` | none | Stops the worker. |
| `noReconnect` | `false` | Reject when the connection fails instead of reconnecting. |
| `initialBackoff`, `maxBackoff` | 0.2 s, 10 s | Reconnect delay bounds. |
| `drainTimeout` | 30 s | How long running handlers may finish after `signal` aborts. |
| `insecureRedirectHttp` | `false` | Follow a shard redirect to plain `http://` on another host with the token. For trusted networks only. |
| `onRegistered` | none | Called after each registration with the worker id, granted slots and endpoint. |
| `logger` | `console` | Receives the worker's log messages. |

### Handlers

The handler gets a `Lease`: `id`, `taskId`, `attempt`, `payload`, `contentType`, `kind`, `key`,
`pool`, `priority`, `tags`, `timeout`, `deadline`, `signal`, and `text()` and `json()`. `signal`
aborts when the server aborts the attempt (timeout, cancellation, lease expiry) or the drain
timeout passes; the outcome is then not reported. The handler returns the output, or throws:

| Throw | Effect |
|---|---|
| nothing | The task succeeds. An output larger than the server accepts fails it permanently. |
| `new Permanent(msg)` | The task fails without retrying. |
| `new Retryable(msg, retryAfter)` | Retryable failure, retried no earlier than `retryAfter` seconds. |
| `new Reject(reason)` | Hand the lease back unrun; the server offers it to another worker. |
| `new CancelTask(msg)` | The task ends as cancelled. |
| anything else | Retryable failure. |

`(lease.taskId, lease.attempt)` is a fencing token: attempt numbers never repeat for a task,
also across failovers. Record the pair with the handler's side effect and let only the newest
attempt write ([Exactly-once side effects](../concepts/reliability.md#exactly-once-side-effects)).

### Shards

A worker that connects to another shard's server is refused with the endpoint of its pool's
shard, and `runWorker` connects there (up to `MAX_REDIRECTS` times in a row). It never follows
a redirect from `https://` to `http://`, sends its token over plain `http://` only to loopback
unless `insecureRedirectHttp` is set, and refuses endpoints with userinfo or a path. When that
shard cannot be reached, it asks `target` again. See [Sharding](../concepts/sharding.md).
