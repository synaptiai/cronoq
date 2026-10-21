# cronoq

**An elastic, priority-aware task scheduler** for Rust, Python and any language over gRPC.

cronoq decides *when* work runs, *in what order*, and *how much capacity* it needs. *Where*
work runs is pluggable: in-process async functions, Python coroutines, or remote workers on
other machines.

- **Priorities with aging.** Four classes; FIFO within a class; waiting tasks gain priority so
  low-priority work is never starved.
- **Deadlines.** Urgency rises smoothly as a task approaches its latest start time.
- **Fixed starts and cron.** Capacity is reserved and a worker is held ahead of time, with a
  per-task policy for a missed start (run late, skip or fail).
- **DAGs.** Dependencies by id or key, with `on_success` / `on_failure` / `on_complete`
  conditions. Batches are admitted atomically.
- **Reliability.** Leases with heartbeats, timeouts, retries with backoff and `retry_after`,
  dead-lettering, cancellation, idempotent keys.
- **Shared resources.** Semaphores and token buckets across pools, e.g. LLM API rate limits
  or GPUs.
- **Elasticity.** Queue-pressure autoscaling per pool, based on arrival rate, backlog,
  per-class wait SLOs and upcoming reservations, through pluggable scalers.
- **Backpressure.** Bounded queues that wait, reject, or shed the lowest or oldest work.
- **Durability and HA.** Postgres store, with active-passive replicas using advisory-lock
  leader election and epoch fencing.
- **Observability.** OpenTelemetry metrics and trace propagation, Prometheus `/metrics`, and
  windowed quality metrics: priority adherence, fixed-start hit rate, utilization, wait
  percentiles and cost.

It started as a rewrite of a Python prototype (`docs/prototype/`); the design is in
[`docs/SPEC.md`](docs/SPEC.md).

## Performance

The engine is a single-threaded state machine with no clock, threads or I/O, so the hot
path never rescans the queue: O(classes + log n) per dispatch.

| | measured (4-vCPU shared VM) | target |
|---|---|---|
| Core engine, submit → dispatch → complete | ~1.0–1.2 M cycles/s | ≥ 1 M/s |
| End to end through tokio, no-op tasks | ~250 k tasks/s | ≥ 250 k/s |
| Submit → await round trip (sequential) | ~70 µs | p99 < 1 ms |

Run them yourself with `cargo bench -p cronoq-core` / `cargo bench -p cronoq` and
`cargo run --release -p cronoq --example firehose -- 1000000`.

## Rust

```rust
use std::{sync::Arc, time::Duration};
use cronoq::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sched = Scheduler::<String, usize>::builder()
        .resource(ResourceConfig::token_bucket("llm-api", 20.0, 5))
        .pool(Pool::new("agents").workers(1, 2, 32).executor_fn(|ctx, prompt: Arc<String>| async move {
            // ... call a model; return Err(TaskError::retry_after("429", d)) to back off
            Ok::<_, TaskError>(prompt.len() + ctx.attempt as usize)
        }))
        .build()
        .await?;

    let h = sched
        .submit(Task::new("hello".to_string())
            .pool("agents")
            .priority(Priority::High)
            .deadline_in(Duration::from_secs(5))
            .resource("llm-api", 1)
            .timeout(Duration::from_secs(30)))
        .await?;
    println!("{}", h.await?);
    sched.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
```

More in `crates/cronoq/examples`:

- `llm_agents`: rate limits, deadlines and 429 retries.
- `dag_pipeline`: fan-out/fan-in with failure and cleanup handlers.
- `firehose`: microtask throughput.
- `prototype_demo`: the original prototype's demo.

## Python

```python
import asyncio, cronoq

async def run(ctx, prompt):
    return f"answer to {prompt}"

async def main():
    async with cronoq.Scheduler([cronoq.Pool("default", run, min=1, max=8)]) as sched:
        h = await sched.submit("hi", priority="high", deadline_in=5.0)
        print(await h)

asyncio.run(main())
```

Payloads stay Python objects (no serialization). `cronoq.compat.ElasticPriorityScheduler` is a
drop-in for the prototype. See [`crates/cronoq-py`](crates/cronoq-py/README.md).

## Server

```sh
cargo run --release -p cronoq-server -- -c deploy/cronoq.example.toml
# or: docker build -t cronoq . && docker run -p 7070:7070 -p 7071:7071 \
#       -e CRONOQ_CLIENT_TOKEN -e CRONOQ_WORKER_TOKEN -e CRONOQ_METRICS_TOKEN -e CRONOQ_ADMIN_TOKEN cronoq
```

Without a config file the server listens on 127.0.0.1 only; it refuses to listen on other
addresses without auth tokens (unless started with `--insecure-no-auth`).

- **Clients** submit work over gRPC (`cronoq.v1.Tasks`, defined in
  [`crates/cronoq-proto/proto`](crates/cronoq-proto/proto/cronoq/v1/cronoq.proto)) or HTTP/JSON:

  ```sh
  curl -X POST localhost:7071/v1/tasks -H "authorization: Bearer $TOKEN" \
       -H 'content-type: application/json' \
       -d '{"payload": {"q": "hi"}, "pool": "llm", "priority": "high", "wait": true}'
  ```

- **Workers** connect to `cronoq.v1.Workers/Work`, register a pool and a number of slots, and
  receive leases. A pool's capacity is the total slots of its connected workers; the desired
  count is exported as `cronoq_pool_desired_workers` (for KEDA/HPA) or pushed to a webhook.
  Clients exist for Rust (`cronoq_server::worker::run_worker`) and Python (`cronoq.client`).
- **HA.** Point several replicas at the same Postgres. One leads; the others answer `/readyz`
  with 503 and take over within seconds, resuming every accepted task.
- **Auth.** Bearer tokens (or mTLS client certificates mapped to roles) with `client`,
  `worker`, `metrics` and `admin` roles, optionally limited to some pools. TLS covers both the
  gRPC and HTTP listeners. The server is meant for one tenant (team or trust domain).

## Layout

| crate | |
|---|---|
| `cronoq-core` | sans-IO engine: lanes, deadline queues, reservations, DAG, leases, resources, autoscaling policy, metrics |
| `cronoq` | tokio runtime, public Rust API, executors, scalers, `Store` trait, OpenTelemetry, blocking facade |
| `cronoq-store-postgres` | durable store, leader election, fencing |
| `cronoq-proto` | protobuf API |
| `cronoq-server` | gRPC/HTTP server, remote workers, auth, Prometheus |
| `cronoq-py` | Python bindings (PyO3/maturin) and gRPC client |

## Testing

- The core runs under randomized deterministic simulation (`cronoq-core/tests/sim.rs`):
  hundreds of seeded workloads (microtasks, heavy-tailed LLM calls, batch jobs, DAGs, fixed
  starts, failures, cancellations, capacity changes), with invariants checked after every
  step.
- Runtime, server (real gRPC/HTTP with workers), Postgres (throwaway local servers,
  `pg_terminate_backend` failover) and Python suites cover the rest.

```sh
cargo test --workspace
CRONOQ_SIM_SEEDS=500 cargo test --release -p cronoq-core --test sim
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
