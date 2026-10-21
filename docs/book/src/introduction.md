# Introduction

**cronoq** is an elastic, priority-aware task scheduler. It decides *when* each piece of work
runs, *in what order*, and *how much capacity* a pool of workers needs. *Where* the work runs is
up to you: async functions in your Rust process, Python coroutines, or remote workers written in
any language and connected over gRPC.

```text
            submit                 dispatch                  execute
 your code ─────────▶  cronoq  ─────────────▶  executor / remote worker
     ▲                (ordering, limits,             │
     └──── result ───  reservations, scaling) ◀──────┘ outcome
```

## What it does

| Capability | What you get |
|---|---|
| **Priorities with aging** | Four classes (critical, high, normal, low), FIFO within a class, and aging so that a stream of normal work cannot starve low-priority work forever. |
| **Deadlines** | Tasks with a deadline gain urgency smoothly as their latest start time approaches, and are ordered earliest-deadline-first within their class. |
| **Fixed starts and cron** | Work that must start at a given time reserves capacity in advance and gets a worker *held* for it, with a policy for what to do if it still misses its slot. |
| **Dependencies** | DAGs by task id or key, with `on_success`, `on_failure` and `on_complete` edges. Batches are admitted atomically. |
| **Reliability** | Leases with heartbeats, per-attempt timeouts, retries with exponential backoff and `retry_after`, dead-lettering, cancellation, idempotent keys. |
| **Shared resources** | Semaphores and token buckets shared across pools: API rate limits, GPU slots, database connections. |
| **Elasticity** | Each pool computes how many workers it needs from arrival rate, backlog, wait-time SLOs and upcoming reservations, and asks a pluggable scaler for them. |
| **Backpressure** | Bounded queues that make submitters wait, reject, or shed lower-priority work. |
| **Durability and HA** | A Postgres store with leader election and fencing; standby servers take over and resume every accepted task. |
| **Observability** | OpenTelemetry metrics and trace propagation, Prometheus `/metrics`, and windowed quality metrics such as priority adherence and fixed-start hit rate. |

## Three ways to use it

1. **Rust library** (`cronoq` crate): embed the scheduler in a tokio application. Payloads are
   plain Rust values; nothing is serialized. See the [Rust quickstart](guides/rust-quickstart.md).
2. **Python package** (`pip install cronoq`): the same engine with asyncio bindings. Payloads are
   Python objects. See the [Python quickstart](guides/python-quickstart.md).
3. **Server** (`cronoq-server`): a standalone service with a gRPC and HTTP/JSON API. Clients in
   any language submit work and remote workers lease it. See the
   [Server quickstart](guides/server-quickstart.md).

All three share one engine, so behaviour, configuration and metrics are the same everywhere.

## How to read this book

- New to cronoq? Read [Why cronoq](why.md) and [Benefits at a glance](benefits.md), then the
  quickstart for your language.
- Evaluating it for a problem? The [use cases](use-cases/index.md) show complete setups and
  the concrete benefit each feature brings.
- Designing a system on it? The [concepts](concepts/tasks.md) chapters explain every scheduling
  rule precisely, including formulas and defaults.
- Operating the server? See [deployment](guides/deployment.md),
  [observability](guides/observability.md) and the [configuration reference](reference/configuration.md).
