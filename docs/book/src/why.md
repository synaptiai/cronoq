# Why cronoq

Most systems start with a plain queue: push jobs, have workers pop them. That works until the
workload stops being uniform. Then the same problems appear again and again:

| Symptom | Root cause | What cronoq does |
|---|---|---|
| A user-facing request waits behind thousands of batch jobs. | FIFO queues have no notion of importance. | Four [priority classes](concepts/priorities.md); dispatch always picks the most urgent ready task. |
| Low-priority jobs never run during busy periods. | Strict priority starves the bottom. | [Aging](concepts/priorities.md#aging) raises a waiting task's score over time. |
| Jobs miss their SLA even though there was capacity earlier. | The queue does not know about deadlines. | [Deadline urgency](concepts/deadlines.md) and earliest-deadline-first ordering. |
| The 02:00 report starts at 02:07 because every worker was busy. | Cron and the work queue compete for the same workers. | [Fixed starts](concepts/fixed-starts.md) reserve capacity and hold a worker ahead of time. |
| A burst of retries hammers an API into more 429s. | Rate limits live in each worker, not in the scheduler. | Shared [token buckets](concepts/resources.md) gate dispatch; `retry_after` is honoured. |
| Workers sit idle at night and are saturated at noon. | Fixed fleet size, or CPU-based autoscaling that ignores the queue. | [Queue-pressure autoscaling](concepts/autoscaling.md) per pool, with reservations pre-warming capacity. |
| A crashed worker loses its job. | No leases or heartbeats. | [Leases, heartbeats and retries](concepts/reliability.md) give at-least-once execution. |
| A downstream step runs although its input failed. | Dependencies managed by hand. | [DAG edges](concepts/dependencies.md) with success/failure/completion conditions. |
| Memory grows without bound during an incident. | Unbounded queues. | [Bounded queues](concepts/backpressure.md) with wait, reject or shed policies. |

cronoq puts all of these rules in one place, the scheduling engine, so that individual workers
stay simple: they receive a task, run it, and report the outcome.

## Benefits in practice

- **Better latency for important work without separate infrastructure.** Interactive and
  background work can share one pool of workers. Priority and deadlines decide the order, so you
  do not need a dedicated "fast lane" cluster that is idle most of the time.
- **Predictable scheduled jobs.** A job with a fixed start time has its capacity booked when it
  is submitted, and a worker is held free shortly before it starts. Admission fails up front
  (`ReservationConflict`) instead of silently starting late.
- **Fewer wasted retries and rate-limit errors.** Rate limits are enforced before dispatch, and
  a task that hits a limit waits without occupying a worker.
- **Lower cost.** Pools scale on queue pressure: arrival rate, backlog, wait-time objectives and
  upcoming reservations. Scale-down is damped by a stabilization window, and cost per pool is
  reported directly.
- **Operational clarity.** Every pool reports utilization, wait percentiles, priority adherence,
  fixed-start hit rate over a sliding window, and cost accumulated since start, as both
  OpenTelemetry and Prometheus metrics.
- **Speed.** The engine handles over a million scheduling decisions per second on one core, so
  it can sit in the hot path of microtask systems as well as hours-long batch jobs.
- **One engine, three surfaces.** The same scheduling rules apply whether you embed the Rust
  library, use the Python package, or run the server for a polyglot fleet.

## How it compares

cronoq is a *scheduler*: it orders and admits work and sizes capacity. It is not a workflow
engine with durable step state, and not a general message broker. The table is a rough guide;
each of these tools is excellent at what it was built for.

| | cronoq | Plain queue (SQS, Redis lists) | Celery / Sidekiq / RQ | Kubernetes Jobs / CronJobs | Airflow / Dagster | Temporal |
|---|---|---|---|---|---|---|
| Priorities with aging | ✅ | ❌ | Partial (separate queues) | ❌ | Partial (pools, weights) | ❌ |
| Deadlines / EDF | ✅ | ❌ | ❌ | ❌ | SLA alerts only | Timeouts only |
| Fixed start with capacity reservation | ✅ | ❌ | ETA only (no reservation) | Start time only | Schedule only | Timers |
| Shared rate limits across workers | ✅ | ❌ | Per worker | ❌ | Pools | Activity task-queue rate limits |
| Queue-pressure autoscaling | ✅ built in | External | External | HPA/KEDA | External | External |
| DAGs | ✅ simple edges | ❌ | Canvas primitives | ❌ | ✅ rich | ✅ code-defined |
| Durable workflow state / replay | ❌ | ❌ | ❌ | ❌ | Task-level | ✅ |
| Embeddable as a library | ✅ Rust, Python | ❌ | ❌ | ❌ | ❌ | ❌ |
| Scheduling throughput | ~1M decisions/s per core | Broker-bound | Broker-bound | Slow (pods) | Slow (DB-polling) | Moderate |

### When to use cronoq

- Mixed workloads that share capacity and need ordering by importance or deadline.
- Work against rate-limited or capacity-limited resources (LLM APIs, GPUs, licences).
- Jobs that must start at specific times without being starved by everything else.
- Systems that want autoscaling driven by the queue rather than by CPU.
- An embedded scheduler inside a Rust or Python service, without running extra infrastructure.

### When not to use cronoq

- **Long-running business workflows with durable state** (sagas, human-in-the-loop steps, days of
  waiting). Use a workflow engine such as Temporal; cronoq can still schedule its activities.
- **Pure message passing / pub-sub.** Use a broker such as Kafka or NATS.
- **Complex data-pipeline orchestration** with backfills, lineage and a UI for analysts. Use
  Airflow or Dagster; cronoq's DAGs are deliberately simple.
- **Exactly-once side effects.** cronoq executes tasks *at least once*. Make handlers idempotent,
  or use task keys to deduplicate submissions.
