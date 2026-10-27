# Benefits at a glance

This page is for engineers deciding whether cronoq fits their system, and where to start. It
lists what changes when you adopt it, how to check that it worked, and what it asks of you in
return.

## What you gain

| Benefit | What changes for you | How to see it | Built on |
|---|---|---|---|
| **Important work is never stuck behind bulk work** | User-facing tasks overtake batch jobs on the same workers; no separate "fast lane" fleet idling most of the day. | `wait_p99` per pool; `priority_adherence` close to 1. | [Priorities](concepts/priorities.md) |
| **Nothing starves** | Low-priority work still progresses during busy periods instead of waiting indefinitely. | Pool wait percentiles stay bounded while `low` tasks keep completing; `pending_by_class` for `low` does not grow without limit. | [Aging](concepts/priorities.md#aging) |
| **Fewer missed SLAs** | Work with a deadline is ordered by how much slack it has left, so capacity that was free earlier is used for it. | Fewer overdue tasks; deadline-driven tasks finish before their deadline. | [Deadlines](concepts/deadlines.md) |
| **Scheduled jobs start on time** | Capacity is booked at submission and a worker is held just before the start, so the 02:00 job starts at 02:00 even on a busy cluster. Over-subscribed schedules are rejected up front. | `fixed_start_hit_rate` = 1. | [Fixed starts](concepts/fixed-starts.md), [cron](concepts/recurring.md) |
| **No more 429 storms** | Rate limits and concurrency caps are enforced before dispatch, across all workers; `Retry-After` hints delay retries. Waiting for a limit does not occupy a worker. | Fewer rate-limit errors and retries (`cronoq_retries_total`). | [Resources](concepts/resources.md) |
| **Right-sized fleets** | Pools grow on queue pressure and wait-time objectives, pre-warm for scheduled work, and shrink without flapping. | `desired` vs `capacity`; `utilization` in the 0.5–0.85 band; `cost` per pool. | [Autoscaling](concepts/autoscaling.md) |
| **Pipelines without glue code** | Dependencies by key with success, failure and completion edges; atomic batches; failure handlers that run only when needed. | Downstream steps never run on failed input; no hand-written state tables. | [Dependencies](concepts/dependencies.md) |
| **Safe retries end to end** | Retries with backoff and jitter, timeouts, leases and heartbeats; idempotent submissions by key. | Dead letters are rare and visible (`cronoq_tasks_total{status="dead_lettered"}`). | [Reliability](concepts/reliability.md) |
| **Graceful overload** | Bounded queues turn overload into backpressure (wait, reject or shed the least important work) instead of memory growth. | `shed` and `429` counts instead of out-of-memory incidents. | [Backpressure](concepts/backpressure.md) |
| **Survives restarts and failover** | With a SQLite or Postgres store, accepted work, cancellations and schedules are durable; a standby takes over in seconds. | Kill the leader: work continues on the standby. | [Durability](concepts/durability.md) |
| **Scheduling quality you can measure** | Every pool reports utilization, wait percentiles, priority adherence, fixed-start hit rate and cost, in OpenTelemetry and Prometheus. | Dashboards and alerts from day one. | [Metrics](concepts/metrics.md) |
| **Negligible overhead** | About a million scheduling decisions per second per core; choosing the next task looks only at queue heads, never the whole backlog. | Scheduler CPU stays a rounding error even for millisecond tasks. | [Performance](guides/performance.md) |

## Pick your starting point

```text
Where does the work run?
│
├─ Inside one Rust service ───────────────▶ cronoq crate (Rust quickstart)
├─ Inside one Python asyncio service ─────▶ pip install cronoq (Python quickstart)
├─ Code written for ElasticPriorityScheduler ▶ cronoq.compat, then the native API (Migrating)
└─ In other processes, machines or languages ▶ cronoq-server + workers (Server quickstart)
                                                 │
     Must queued work survive restarts? ── yes ─▶ add a SQLite or Postgres store (and a standby for HA)
```

| If you have... | Start with | Then add |
|---|---|---|
| An async Rust service calling rate-limited APIs | [`cronoq` crate](guides/rust-quickstart.md), one pool and a token bucket | Deadlines on interactive calls, `otel` metrics |
| A Python service mixing user requests with background jobs | [`cronoq` for Python](guides/python-quickstart.md), priorities | Status events for notifications, autoscaling objectives |
| GPU or model workers on several machines | [`cronoq-server`](guides/server-quickstart.md) with a pool per worker type | KEDA on `cronoq_pool_desired_workers`, Postgres for HA |
| Cron jobs that start late | [Fixed starts and cron](use-cases/scheduled-jobs.md) | Miss policies and `fixed_start_hit_rate` alerts |
| A hand-rolled DAG runner | [Keys and dependencies](use-cases/pipelines.md) | Atomic batches and failure handlers |
| A queue that falls over during incidents | [Bounded pools](concepts/backpressure.md) with `shed_lowest` | Alerts on `shed` and wait percentiles |

## Match your symptom

| Symptom | Read |
|---|---|
| "User requests wait behind batch jobs." | [Interactive and batch work on shared capacity](use-cases/priority-mixing.md) |
| "We keep hitting the LLM provider's rate limit." | [LLM and agent workloads](use-cases/llm-agents.md) |
| "SLAs are missed although the cluster was idle an hour ago." | [Deadline-driven work](use-cases/deadlines.md) |
| "The nightly job started 20 minutes late." | [Scheduled jobs with guaranteed capacity](use-cases/scheduled-jobs.md) |
| "Step 3 ran although step 2 failed." | [Pipelines and DAGs](use-cases/pipelines.md) |
| "Our 8 GPUs are oversubscribed." | [GPUs and other scarce resources](use-cases/scarce-resources.md) |
| "The scheduler is slower than the tasks." | [High-throughput microtasks](use-cases/microtasks.md) |
| "Workers are written in three languages." | [Polyglot worker fleets](use-cases/worker-fleets.md) |
| "We pay for idle workers at night." | [Cost-aware autoscaling](use-cases/autoscaling.md) |

## What it asks of you

cronoq is deliberately small. Adopting it means accepting a few rules:

- **At-least-once execution.** A task can run again if a worker or the leader fails mid-attempt.
  Make handlers idempotent, and give submissions a key so retries are deduplicated.
- **One active scheduler per shard.** HA is active-passive per shard: one leader decides,
  standbys wait. By default all pools are in one shard; [sharding](concepts/sharding.md) splits
  pools across shards, each with its own leader.
- **In memory by default.** Without a store, a restart loses queued work. Add a SQLite or
  Postgres store when that matters.
- **A scheduler, not a workflow engine.** It orders, admits and sizes work. Long-running
  workflows with durable step state, human approvals or replays belong in a workflow engine;
  cronoq can schedule their activities.
- **Payloads are plain Rust values in the library, Python objects in the Python package, and
  opaque bytes on the server.** Large blobs belong in object storage, with a
  reference in the payload (the server limits messages to 16 MiB by default).

See [Why cronoq](why.md) for a comparison with queues, task frameworks, Kubernetes Jobs and
workflow engines.

## A typical adoption path

1. **Embed** the library (Rust or Python) in the service that already runs the work, with one
   pool per kind of worker and the defaults everywhere else.
2. **Classify** work: priorities for user-facing versus background tasks, deadlines where an
   SLA exists, keys on everything that may be retried.
3. **Protect dependencies**: a token bucket per external API, a semaphore per scarce resource.
4. **Observe**: export metrics and alert on wait percentiles, dead letters and fixed-start hit
   rate.
5. **Scale out** when workers outgrow one process: run `cronoq-server`, move executors into
   workers, add Postgres and a standby, and drive the worker fleet from
   `cronoq_pool_desired_workers`.
