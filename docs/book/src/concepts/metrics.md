# Metrics and quality signals

cronoq reports two kinds of telemetry:

1. **Snapshots**: per-pool quality metrics over a sliding window (60 s by default), returned by
   `Scheduler::snapshot()` (Rust), `await sched.snapshot()` (Python), `GET /v1/snapshot` and
   `Admin/Snapshot`.
2. **Exported metrics**: OpenTelemetry instruments (library, `otel` feature) and Prometheus text
   at `/metrics` (server). See the [metrics reference](../reference/metrics.md).

## Pool snapshot fields

| Field | Meaning |
|---|---|
| `capacity` | Current worker slots. |
| `busy` | Slots running an attempt. |
| `held` | Slots held for upcoming fixed-start tasks. |
| `desired` | Autoscaler target. |
| `pending_by_class` | Pending tasks per class (critical, high, normal, low). |
| `ready` | Tasks queued for dispatch now. |
| `reservations` | Fixed-start reservations booked. |
| `dispatched`, `succeeded`, `failed`, `retried` | Counts within the window. |
| `priority_adherence` | Fraction of dispatches made with no strictly higher class waiting. |
| `fixed_start_hit_rate` | Fraction of fixed-start tasks started within their tolerance. |
| `utilization` | Busy slot-seconds ÷ capacity slot-seconds. |
| `schedule_efficiency` | Completed work seconds ÷ capacity slot-seconds. |
| `wait_mean`, `wait_p50/p90/p99` | Queue wait of first attempts (submission to dispatch). |
| `run_p50`, `run_p99` | Run time of successful attempts. |
| `worker_seconds`, `cost` | Since start. |

Snapshots also include totals since start (`submitted`, `succeeded`, `failed`, `dead_lettered`,
`cancelled`, `skipped`, `missed`, `shed`, `retries`, `timeouts`, `lease_expiries`,
`stale_completions`,
`recurring_conflicts`, `recurring_invalid`).

## Reading the signals

| Signal | Healthy | Investigate when |
|---|---|---|
| `wait_p99` | Below your latency objective | Rising while `utilization` is high: capacity-bound; while low: resource-bound (check rate limits). |
| `utilization` | 0.5–0.85 | Near 1.0 with growing backlog (scale out); very low with `desired` at `min` (over-provisioned). |
| `priority_adherence` | Close to 1.0 | Drops: aging or deadlines override priorities often; review classes or caps. |
| `fixed_start_hit_rate` | 1.0 | Below 1.0: reservable capacity too small, or no workers connected around start times. |
| `desired` vs `capacity` | Equal (in-process) or converging (server) | Persistently `desired > capacity` in server mode: your scaler is not adding workers. |
| `dead_lettered` rate | Near zero | Spikes indicate a failing dependency; inspect status messages. |

## Implementation notes

Window metrics use a ring of 12 buckets, and wait/run percentiles come from a log-linear
histogram (about 9% resolution). Utilization integrates busy and capacity over time, so it is
exact for the window, not sampled.
