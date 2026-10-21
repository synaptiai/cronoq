# Migrating from ElasticPriorityScheduler

cronoq ships a compatibility layer for code written against the Python
`ElasticPriorityScheduler` API (`Task`, `Priority`, `submit`, `start`, `stop`, `get_metrics`,
`get_cost`). It runs on the cronoq engine, so existing code gets the engine's behaviour and
performance with a one-line import change.

## Step 1: swap the import

```python
# before
from elastic_scheduler import ElasticPriorityScheduler, Priority, Task
# after
from cronoq.compat import ElasticPriorityScheduler, Priority, Task
```

```python
import time

scheduler = ElasticPriorityScheduler(initial_workers=1, max_workers=4)
scheduler.start()
scheduler.submit(Task(id="report", duration=2.0, priority=Priority.HIGH))
scheduler.submit(Task(id="nightly", duration=0.5, fixed_start=time.time() + 60))
scheduler.submit(Task(id="email", duration=0.2, dependencies=["report"]))
...
print(scheduler.get_metrics())   # QualityMetrics(schedule_quality=..., priority_adherence=...)
scheduler.stop()
```

The shim is synchronous and thread-safe; it runs the native scheduler on a private event loop in
a background thread. As in the original, a task "runs" by sleeping for its `duration`.

## Behavioural differences

| Area | Original | cronoq |
|---|---|---|
| Fixed-start conflicts | Any two overlapping fixed tasks conflict, regardless of worker count. | Overlaps are allowed up to half of `max_workers` (at least one). |
| Recorded start of fixed tasks | `max(now, fixed_start)`, even if dispatched early. | The actual dispatch time. |
| Dependencies | Satisfied only while the parent is among the last 1000 completed tasks. | Tracked exactly; late dependents are resolved from retained statuses. |
| Deadline urgency | Non-monotonic (an overdue task could rank below an almost-due one). | Monotonic; overdue tasks always rank highest within their class. |
| Scaling trigger | `schedule_quality` thresholds. | Queue pressure; `scale_up_threshold` / `scale_down_threshold` are accepted and ignored. |
| `schedule_quality` | Divides by the worker count at call time. | Total work ÷ worker-seconds actually provisioned. |
| Dispatch | Polls every 100 ms; one thread per task. | Event-driven; no polling. |

## Step 2: move to the native API (recommended)

The native API gives you everything the shim hides: real executors instead of sleeps, results,
retries, timeouts, cancellation, resources and per-pool configuration.

| Original | Native equivalent |
|---|---|
| `Task(id=..., duration=d, priority=p)` | `await sched.submit(payload, key=id, priority=p, duration_hint=d)` |
| `Task(..., fixed_start=t)` | `submit(..., start_at=t)` |
| `Task(..., deadline=t)` | `submit(..., deadline_at=t)` |
| `Task(..., dependencies=[a, b])` | `submit(..., after=[a, b])` |
| `ElasticPriorityScheduler(initial_workers, max_workers)` | `Scheduler([Pool("default", run, min=1, initial=i, max=m)])` |
| `get_metrics()` | `await sched.snapshot()` (per pool: `priority_adherence`, `fixed_start_hit_rate`, `utilization`, wait percentiles, `cost`) |
| `get_cost()` | `snapshot().pool("default").cost` (set `cost_per_worker_second` in `AutoscaleConfig`) |
