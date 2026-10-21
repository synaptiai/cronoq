# Server quickstart

`cronoq-server` runs the scheduler as a service. Clients submit tasks over gRPC or HTTP/JSON;
remote workers in any language connect over a gRPC stream, register for a pool with a number of
slots, and receive leases. In about ten minutes you will run a server, connect a worker and
push work through it.

```text
 clients (any language)         cronoq-server                  workers (any language)
 ──────────────────────▶  ┌───────────────────────┐  ◀── Workers.Work stream ───
  gRPC :7070 / HTTP :7071 │ engine + remote pools  │  ─── Lease / Abort ────────▶
                          └───────────┬───────────┘  ◀── Heartbeat / Complete ──
                                      │ optional
                                  PostgreSQL
```

What you get compared with a plain queue: priorities with aging, deadlines, fixed start times
with reserved capacity, dependencies, retries with backoff, shared rate limits, idempotent keys
and an autoscaling signal, behind one small binary.

## 1. Create tokens

The server authenticates every call with a bearer token per role. Tokens must be at least 16
characters; use random 32-character values:

```sh
for role in CLIENT WORKER METRICS ADMIN; do
  export CRONOQ_${role}_TOKEN=$(openssl rand -hex 16)
done
```

## 2. Start the server

From source:

```sh
cargo run --release -p cronoq-server -- --config deploy/cronoq.example.toml
```

Or with Docker (the image contains the same example configuration):

```sh
docker build -t cronoq .
docker run --rm -p 7070:7070 -p 7071:7071 \
  -e CRONOQ_CLIENT_TOKEN -e CRONOQ_WORKER_TOKEN -e CRONOQ_METRICS_TOKEN -e CRONOQ_ADMIN_TOKEN \
  cronoq
```

The [example configuration](https://github.com/synaptiai/cronoq/blob/main/deploy/cronoq.example.toml)
defines two pools (`default` and `llm`), two shared resources (`openai`, a token bucket, and
`gpu`, a semaphore) and the four tokens above. It listens on all interfaces, which the server
only allows with authentication configured.

Without `--config` the server starts with a single `default` pool, listens on `127.0.0.1` only
and has authentication disabled: fine for a laptop, refused on any other address unless you pass
`--insecure-no-auth`.

Validate a configuration without starting anything:

```sh
cronoq-server --config cronoq.toml --check     # "configuration is valid"
```

Check the server is up:

```sh
curl -s localhost:7071/readyz        # "ready"
```

## 3. Connect a worker

A pool's capacity is the sum of the slots of its connected workers, so nothing runs until a
worker connects. In Python (`pip install "cronoq[client]"`):

```python
import asyncio, os
from cronoq.client import Worker

async def handle(lease):
    data = lease.json()
    return {"echo": data, "attempt": lease.attempt}

asyncio.run(Worker("localhost:7070", "default", handle, slots=4,
                   token=os.environ["CRONOQ_WORKER_TOKEN"]).run())
```

Or in Rust, with the worker client in the `cronoq-server` crate:

```rust,ignore
use bytes::Bytes;
use cronoq_server::worker::{run_worker, WorkerOptions};

let opts = WorkerOptions::new("http://localhost:7070", "default")
    .slots(4)
    .token(std::env::var("CRONOQ_WORKER_TOKEN")?);
run_worker(opts, |lease| async move { Ok(Bytes::from(lease.payload.to_ascii_uppercase())) },
           std::future::pending()).await?;
```

Both clients heartbeat running leases, reconnect with backoff, and drain on shutdown. Workers
in other languages implement the `cronoq.v1.Workers/Work` stream; see the
[protocol rules](../reference/grpc.md#protocol-rules).

## 4. Submit work

With curl (HTTP/JSON):

```sh
curl -s localhost:7071/v1/tasks \
  -H "authorization: Bearer $CRONOQ_CLIENT_TOKEN" -H 'content-type: application/json' \
  -d '{"payload": {"hello": "world"}, "priority": "high", "wait": true}'
```

```json
{"id":"1","duplicate":false,"result":{"id":"1","status":"succeeded","output_available":true,"output":{"echo":{"hello":"world"},"attempt":1},"output_b64":"..."}}
```

Task ids are strings in JSON, because they are 64-bit.

With the Python client:

```python
import os
from cronoq.client import Client

async with Client("localhost:7070", token=os.environ["CRONOQ_CLIENT_TOKEN"]) as c:
    sub = await c.submit({"hello": "world"}, priority="high", key="hello-1")
    result = await c.wait(sub.id, timeout=30)
    print(result.status, result.json())
```

Submitting again with the same `key` returns the same task (`duplicate = true`), so retries
after a network error are safe.

## 5. Look inside

```sh
curl -s localhost:7071/v1/snapshot -H "authorization: Bearer $CRONOQ_METRICS_TOKEN" | jq
curl -s localhost:7071/metrics -H "authorization: Bearer $CRONOQ_METRICS_TOKEN" | grep cronoq_pool
```

`cronoq_pool_desired_workers` is the number of slots the autoscaler wants for each pool; feed it
to KEDA or an HPA to scale your worker deployment.

## Next steps

- [Using the HTTP API](http-api.md) for dependencies, batches, scheduled work and errors.
- [Writing executors and workers](workers.md) for worker design.
- [Deploying the server](deployment.md) for Kubernetes, KEDA autoscaling and HA with Postgres.
- [Securing the server](security.md) before exposing it beyond a trusted network.
- [Server configuration](../reference/configuration.md) for every option.
