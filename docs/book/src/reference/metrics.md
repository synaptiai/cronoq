# Metrics

cronoq reports three views of the same state:

- **Prometheus** text at `/metrics` (`cronoq-server`).
- **OpenTelemetry** instruments and `tracing` spans (the `cronoq` crate's default `otel`
  feature).
- **Snapshots** (`Scheduler::snapshot()`, `await sched.snapshot()`, `GET /v1/snapshot`,
  `Admin.Snapshot`), described in [Metrics and quality signals](../concepts/metrics.md).

Window-based values cover the engine's sliding metrics window: the last 60 seconds, in 12
buckets.

## Prometheus (`cronoq-server`)

Served at `GET /metrics` on `server.http_addr` (requires the `metrics` or `admin` role) and,
without authentication, on `server.metrics_addr` when it is set. Content type
`text/plain; version=0.0.4; charset=utf-8`. Values are computed from a snapshot at scrape time.
A standby server answers 503: scrape every instance, and the leader's series are the live ones.

Label values are escaped per the exposition format (`\\`, `\"`, `\n`).

### Per pool (label `pool`)

| Metric | Type | Description |
|---|---|---|
| `cronoq_pool_workers` | gauge | Current capacity: the slots of connected workers. |
| `cronoq_pool_desired_workers` | gauge | Autoscaler target capacity. Feed it to KEDA or an HPA. |
| `cronoq_pool_busy_workers` | gauge | Slots running an attempt. |
| `cronoq_pool_held_workers` | gauge | Slots held idle for an upcoming fixed-start task. |
| `cronoq_pool_workers_connected` | gauge | Connected worker streams. |
| `cronoq_pool_pending` | gauge | Tasks waiting to run, by priority (extra label `class`: `critical`, `high`, `normal`, `low`). |
| `cronoq_pool_reservations` | gauge | Fixed-start reservations not yet started. |
| `cronoq_pool_priority_adherence` | gauge | Windowed fraction of dispatches with no strictly higher class waiting (0–1). |
| `cronoq_pool_fixed_start_hit_rate` | gauge | Windowed fraction of fixed starts that began within tolerance (0–1). |
| `cronoq_pool_utilization` | gauge | Windowed busy slot-seconds ÷ capacity slot-seconds (0–1). |
| `cronoq_pool_schedule_efficiency` | gauge | Windowed completed work seconds ÷ capacity slot-seconds (0–1). |
| `cronoq_pool_wait_seconds` | gauge | Windowed time from submission to first start, by `quantile` (`0.5`, `0.9`, `0.99`). |
| `cronoq_pool_cost_total` | counter | Worker cost since the leader started (`cost_per_worker_second` × worker-seconds). |
| `cronoq_pool_dispatched_window` | gauge | Attempts dispatched within the current metrics window (default 60 s). It is a windowed count that can go down; for a lifetime rate use `rate(cronoq_attempts_dispatched_total[5m])`. |

### Server-wide

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cronoq_workers_connected` | gauge | | Connected worker streams, all pools. |
| `cronoq_live_tasks` | gauge | | Tasks not yet finished. |
| `cronoq_running_tasks` | gauge | | Attempts running. |
| `cronoq_pending_tasks` | gauge | | Tasks waiting to run. |
| `cronoq_tasks_total` | counter | `status` | Tasks that reached `succeeded`, `failed`, `dead_lettered`, `cancelled`, `skipped`, `missed` or `shed`. |
| `cronoq_tasks_submitted_total` | counter | | Tasks admitted. |
| `cronoq_attempts_dispatched_total` | counter | | Attempts dispatched. |
| `cronoq_retries_total` | counter | | Retries scheduled. |
| `cronoq_timeouts_total` | counter | | Attempts that hit their timeout. |
| `cronoq_lease_expiries_total` | counter | | Leases that expired without a heartbeat. |
| `cronoq_stale_completions_total` | counter | | Completions and failures reported for leases that were no longer valid (late, duplicate, or from a previous leader); ignored. |

Counters start at zero when a server becomes leader; `rate()` and `increase()` handle the reset
on failover.

### Example alerts

```yaml
groups:
  - name: cronoq
    rules:
      - alert: CronoqQueueWaitHigh
        expr: cronoq_pool_wait_seconds{quantile="0.99"} > 30
        for: 10m
      - alert: CronoqUnderProvisioned
        expr: cronoq_pool_desired_workers > cronoq_pool_workers
        for: 15m
      - alert: CronoqFixedStartsMissed
        expr: cronoq_pool_fixed_start_hit_rate < 0.99
        for: 15m
      - alert: CronoqDeadLetters
        expr: increase(cronoq_tasks_total{status="dead_lettered"}[10m]) > 0
      - alert: CronoqNoWorkers
        expr: sum by (pool) (cronoq_pool_pending) > 0 and on (pool) cronoq_pool_workers_connected == 0
        for: 5m
```

## OpenTelemetry (`cronoq` crate)

With the `otel` feature (on by default), the runtime records into the global meter provider
under the meter name `cronoq`, so it reports into whatever pipeline the application installed.
Without a provider the instruments are no-ops.

| Instrument | Kind | Attributes | Description |
|---|---|---|---|
| `cronoq.tasks.submitted` | counter | `pool` | Tasks admitted. |
| `cronoq.tasks.finished` | counter | `pool`, `status` | Tasks reaching a terminal status. |
| `cronoq.attempts` | counter | `pool`, `kind`, `outcome` (`ok` / `error`) | Finished attempts. |
| `cronoq.attempt.duration` | histogram (s) | `pool`, `kind`, `outcome` | Attempt run time. |
| `cronoq.pool.workers` | observable gauge | `pool` | Current capacity. |
| `cronoq.pool.desired_workers` | observable gauge | `pool` | Autoscaler target. |
| `cronoq.pool.busy_workers` | observable gauge | `pool` | Busy slots. |
| `cronoq.pool.held_workers` | observable gauge | `pool` | Held slots. |
| `cronoq.pool.pending` | observable gauge | `pool`, `class` | Pending tasks by class. |
| `cronoq.pool.cost` | observable gauge | `pool` | Cost since start. |

Observable gauges read values the runtime refreshes once per second.

### Traces

Each attempt runs inside a `tracing` span named `cronoq.task` with the fields `task.id`,
`task.pool`, `task.kind` and `task.attempt`. When a task is submitted inside a span and an
OpenTelemetry text-map propagator is installed, the submitter's W3C trace context is stored in
the task's `traceparent` tag; with `tracing-opentelemetry` installed, each attempt's span
becomes a child of the submitting trace, across retries. Executors can read it with
`ctx.traceparent()` to propagate it further.

## Snapshot

All snapshot fields are described in [Metrics and quality signals](../concepts/metrics.md) and,
for the server's JSON form, in the [HTTP reference](http.md#get-v1snapshot).
