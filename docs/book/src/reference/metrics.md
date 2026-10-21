# Metrics

## Prometheus (`cronoq-server`, `GET /metrics`)

Gauges are derived from the scheduler snapshot at scrape time; window-based values use the
engine's 60-second sliding window.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cronoq_pool_workers` | gauge | `pool` | Current worker capacity (connected slots). |
| `cronoq_pool_desired_workers` | gauge | `pool` | Autoscaler target. Use it for KEDA/HPA. |
| `cronoq_pool_busy_workers` | gauge | `pool` | Slots running an attempt. |
| `cronoq_pool_held_workers` | gauge | `pool` | Slots held for upcoming fixed-start tasks. |
| `cronoq_pool_pending` | gauge | `pool`, `class` | Pending tasks by priority class. |
| `cronoq_pool_priority_adherence` | gauge | `pool` | Windowed priority adherence (0–1). |
| `cronoq_pool_fixed_start_hit_rate` | gauge | `pool` | Windowed fraction of fixed starts within tolerance. |
| `cronoq_pool_utilization` | gauge | `pool` | Windowed busy ÷ capacity. |
| `cronoq_pool_wait_p99_seconds` | gauge | `pool` | Windowed p99 queue wait. |
| `cronoq_pool_cost` | gauge | `pool` | Accumulated cost (`cost_per_worker_second`). |
| `cronoq_workers_connected` | gauge | | Connected remote workers. |
| `cronoq_tasks_total` | counter | `status` | Tasks reaching `succeeded`, `failed`, `dead_lettered`, `cancelled`, `skipped`, `missed`, `shed`. |
| `cronoq_tasks_submitted_total` | counter | | Tasks admitted. |
| `cronoq_retries_total` | counter | | Retries scheduled. |

## OpenTelemetry (`cronoq` crate, `otel` feature)

Instruments are created on the global meter provider under the meter name `cronoq`.

| Instrument | Kind | Attributes | Description |
|---|---|---|---|
| `cronoq.tasks.submitted` | counter | `pool` | Tasks admitted. |
| `cronoq.tasks.finished` | counter | `pool`, `status` | Tasks reaching a terminal status. |
| `cronoq.attempts` | counter | `pool`, `kind`, `outcome` (`ok`/`error`) | Finished attempts. |
| `cronoq.attempt.duration` | histogram (s) | `pool`, `kind`, `outcome` | Attempt run time. |
| `cronoq.pool.workers` | observable gauge | `pool` | Current capacity. |
| `cronoq.pool.desired_workers` | observable gauge | `pool` | Autoscaler target. |
| `cronoq.pool.busy_workers` | observable gauge | `pool` | Busy slots. |
| `cronoq.pool.held_workers` | observable gauge | `pool` | Held slots. |
| `cronoq.pool.pending` | observable gauge | `pool`, `class` | Pending tasks by class. |
| `cronoq.pool.cost` | observable gauge | `pool` | Accumulated cost. |

Observable gauges are refreshed once per second.

## Snapshot API

All snapshot fields are described in [Metrics and quality signals](../concepts/metrics.md).
