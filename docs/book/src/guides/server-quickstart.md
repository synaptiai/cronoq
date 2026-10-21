# Server quickstart

`cronoq-server` runs the scheduler as a service. Clients submit tasks over gRPC or HTTP/JSON;
remote workers connect over a gRPC stream, register for a pool with a number of slots, and
receive leases.

```text
 clients (any language)        cronoq-server                 workers (any language)
 ──────────────────────▶  ┌──────────────────────┐  ◀─── Workers.Work stream ───
   gRPC :7070 / HTTP :7071 │ engine + remote pools │  ─── Lease / Abort ─────────▶
                           └──────────┬───────────┘  ◀── Heartbeat / Complete ───
                                      │ optional
                                  PostgreSQL
```

## 1. Start the server

From source:

```sh
cargo run --release -p cronoq-server -- --config deploy/cronoq.example.toml
```

Or with Docker:

```sh
docker build -t cronoq .
docker run --rm -p 7070:7070 -p 7071:7071 \
  -e CRONOQ_CLIENT_TOKEN=client-token \
  -e CRONOQ_WORKER_TOKEN=worker-token \
  -e CRONOQ_ADMIN_TOKEN=admin-token \
  cronoq
```

The example configuration defines two pools (`default` and `llm`), two shared resources, and
three bearer tokens read from the environment. Without a config file the server starts with a
single `default` pool and **authentication disabled**, which is only appropriate for local
experiments.

Check it is up:

```sh
curl -s localhost:7071/readyz        # "ready"
```

## 2. Connect a worker

A pool's capacity is the sum of the slots of its connected workers, so nothing runs until a
worker connects. In Python:

```python
import asyncio
from cronoq.client import Worker

async def handle(lease):
    data = lease.json()
    return {"echo": data, "attempt": lease.attempt}

asyncio.run(Worker("localhost:7070", "default", handle, slots=4, token="worker-token").run())
```

Or in Rust, with the client in the `cronoq-server` crate:

```rust,ignore
use bytes::Bytes;
use cronoq_server::worker::{run_worker, WorkerOptions};

let opts = WorkerOptions::new("http://localhost:7070", "default").slots(4).token("worker-token");
run_worker(opts, |lease| async move { Ok(Bytes::from(lease.payload.to_ascii_uppercase())) },
           std::future::pending()).await?;
```

Workers in other languages implement the `cronoq.v1.Workers/Work` stream; see
[Writing executors and workers](workers.md#any-language-over-grpc).

## 3. Submit work

With curl (HTTP/JSON):

```sh
curl -s -X POST localhost:7071/v1/tasks \
  -H 'authorization: Bearer client-token' -H 'content-type: application/json' \
  -d '{"payload": {"hello": "world"}, "priority": "high", "wait": true}'
```

```json
{"id":1,"duplicate":false,"result":{"id":1,"status":"succeeded","output":{"echo":{"hello":"world"},"attempt":1},"output_b64":"..."}}
```

With the Python client:

```python
from cronoq.client import Client

async with Client("localhost:7070", token="client-token") as c:
    sub = await c.submit({"hello": "world"}, priority="high")
    result = await c.wait(sub.id, timeout=30)
    print(result.status, result.json())
```

## 4. Look inside

```sh
curl -s localhost:7071/v1/snapshot -H 'authorization: Bearer client-token' | jq
curl -s localhost:7071/metrics | grep cronoq_pool
```

Next steps:

- [Using the HTTP API](http-api.md) for the full JSON API.
- [Deploying the server](deployment.md) for Kubernetes, autoscaling with KEDA, and HA with Postgres.
- [Server configuration](../reference/configuration.md) for every option.
