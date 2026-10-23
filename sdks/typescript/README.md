# cronoq TypeScript SDK

A TypeScript client for the [cronoq](https://github.com/synaptiai/cronoq) server, built on
Connect-ES:

- `cronoq/node`: the client and workers, over gRPC (Node 22 or newer);
- `cronoq/web`: the client in browsers, over gRPC-Web (unary calls and `watch`).

```ts
import { connect, runWorker, text } from "cronoq/node";

const c = connect("http://127.0.0.1:7070", { token: process.env.CRONOQ_TOKEN });
const sub = await c.submitAndWait({ payload: "hello" }, 30);
console.log(sub.result?.status, text(sub.result?.output));

const stop = new AbortController();
await runWorker(
  { target: "http://127.0.0.1:7070", pool: "default", slots: 8, token: process.env.CRONOQ_WORKER_TOKEN, signal: stop.signal },
  (lease) => lease.text().toUpperCase(),
);
```

In a browser, with the page's origin allowed in the server's `[server.grpc_web]`:

```ts
import { connectWeb } from "cronoq/web";

const c = connectWeb("https://cronoq.example.com:7070", { token });
for await (const e of c.watch({ pools: ["default"] })) console.log(e.id, e.status);
```

Task ids are `bigint`s. Throw `Permanent`, `Retryable` (with `retryAfter`), `Reject` or
`CancelTask` from a handler to control the outcome; `(lease.taskId, lease.attempt)` is a
fencing token for exactly-once side effects.

Documentation: the [TypeScript API reference](https://synaptiai.github.io/cronoq/reference/typescript.html).

## Development

```sh
npm ci
npm test               # type-check, build, and test against a real cronoq-server
npm run test:browser   # the gRPC-Web client in headless Chromium (npx playwright install chromium)
```

The tests start the server from the repository's `target/debug` (build it with
`cargo build -p cronoq-server`) or from `CRONOQ_SERVER_BIN`; set `CRONOQ_REQUIRE_SERVER=1` to fail
instead of skipping when it is missing. After changing `cronoq.proto`, run `npm run generate`;
CI checks that `src/gen` is current.

Licensed under either of Apache License 2.0 or MIT, at your option.
