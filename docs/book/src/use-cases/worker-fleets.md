# Polyglot worker fleets

## The problem

The work runs somewhere else: Python model servers on GPU machines, a Go service with access to
a private network, a Node.js renderer. Each team reinvents leasing, heartbeats, retries and
autoscaling on top of a broker.

## The setup

Run `cronoq-server` and connect workers to it. Producers submit over gRPC or HTTP; workers
open one bidirectional stream per process:

```text
producer ──HTTP/gRPC──▶ cronoq-server ◀──Workers.Work stream── worker (Python, GPU box)
                              ▲      ◀──Workers.Work stream── worker (Rust)
                              └──────◀──Workers.Work stream── worker (any gRPC language)
```

A Python worker serving the `gpu` pool with two slots:

```python
from cronoq.client import Worker
import cronoq

async def infer(lease):
    if lease.cancelled.is_set():
        return None
    try:
        return run_model(lease.json())
    except OutOfMemory:
        raise cronoq.Retryable("OOM", retry_after=10)

await Worker("cronoq:7070", "gpu", infer, slots=2, token=WORKER_TOKEN).run()
```

A producer in any language, over HTTP:

```sh
curl -X POST https://cronoq.internal/v1/tasks -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"pool":"gpu","payload":{"image":"s3://..."},"priority":"high","timeout_s":120}'
```

## The worker protocol

1. The worker opens `Workers/Work` and sends `Register{pool, slots, protocol_version}`.
2. The server answers `Registered{worker_id, lease_ttl_s, slots}`. The pool's capacity grows
   by the granted `slots`.
3. The server sends `Lease` messages, at most `slots` at a time.
4. For each lease the worker sends `Heartbeat` well within `lease_ttl_s` (the bundled clients
   send one every third of the TTL), then `Complete{output}`, `Fail{message, permanent,
   retry_after_s}`, or `Nack` to hand the lease to another worker.
5. The server may send `Abort{lease}` (timeout, cancellation, lease expiry). The worker stops
   that attempt.
6. When the stream closes, its in-flight leases are retried elsewhere and capacity shrinks.
   When the server restarts or leadership moves, it sends `Shutdown` and workers reconnect.

See [Writing executors and workers](../guides/workers.md#any-language-over-grpc) for a full
description and the [gRPC reference](../reference/grpc.md).

## What you get

- **Language freedom.** Any gRPC-capable language can be a worker; producers can use plain HTTP.
- **At-least-once execution across machines.** Crashed or partitioned workers lose their lease
  (missed heartbeats) and the attempt is retried on another worker.
- **Central policies.** Priorities, deadlines, rate limits and retries are enforced by the
  server, not re-implemented in each worker.
- **Elastic fleets.** The server computes the desired number of workers per pool and exports it
  (`cronoq_pool_desired_workers`) for KEDA or HPA, or calls your webhook. See
  [Cost-aware autoscaling](autoscaling.md).
- **High availability.** With the Postgres store, standby servers take over; workers reconnect
  and accepted work resumes. See [Durability and HA](../concepts/durability.md).
