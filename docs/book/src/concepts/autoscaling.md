# Pools and autoscaling

## Pools

A **pool** is a group of worker slots with one executor (or one set of remote workers), one
autoscaling policy and its own queue bounds. A **slot** runs one attempt at a time.

| Setting | Default (library) | Default (server) |
|---|---|---|
| `min_workers` / `max_workers` | 1 / 10 | 0 / 10 |
| `initial_workers` | 1 | 0 (capacity comes from workers) |
| Autoscaling | on | on |
| Lease TTL | none | 30 s |
| Overflow policy | `Wait` | `Reject` |
| Retry policy | 3 attempts, 1 s ×2 up to 60 s, jitter | same |

Use several pools to isolate workloads with different executors, hardware or scaling needs;
use [resources](resources.md) to share limits between them.

## Capacity

- **In-process** (Rust or Python library): a slot is a concurrently running task. The default
  `SlotScaler` sets capacity to whatever the autoscaler asks for, immediately.
- **Server**: capacity is the sum of `slots` of the workers connected to the pool. The scaler
  only *requests* capacity (`External`: export a metric; `Webhook`: POST the number), and
  capacity changes when workers connect or disconnect.
- You can report capacity yourself with `Scheduler::set_capacity` (Rust) or
  `set_capacity` (Python) for externally managed pools.

## The queue-pressure policy

Every `interval` (1 s) each autoscaled pool computes:

```text
λ        arrival rate (EWMA, α = 0.3) of tasks becoming ready
S        mean service time (EWMA of observed run times; default_duration before any)
B        backlog: ready tasks per class (fixed-start tasks that are due count as critical)

base     = ceil(λ × S / target_utilization)             target_utilization = 0.75
             (0 when below 0.01: the smoothed rate decays toward zero but never reaches it)
drain    = ceil(ΣB × S / drain_time)                    drain_time = 30 s
slo      = max over classes c with backlog and an SLO:
             ceil(B_≤c × S / slo[c])                    B_≤c = backlog in classes c and higher
reserved = peak concurrent reservations in [now, now + reservation_horizon]   horizon = 60 s

desired  = clamp(max(base + drain, slo, busy) + reserved, min_workers, max_workers)
```

Then:

- **Scale up** immediately, by at most `max(ceil(current × max_step_up_ratio), max_step_up_abs)`
  per evaluation (defaults: +100% or +4).
- **Scale down** only to the **highest** desired value seen over the `stabilization_window`
  (60 s), by at most `max_step_down` (1) per `cooldown` (30 s).
- Running tasks are never interrupted: when capacity shrinks below the busy count, slots retire
  as their attempts finish.

## Cost

Set `cost_per_worker_second` and each pool reports accumulated `worker_seconds` and `cost` in
snapshots and metrics.

## Configuration reference

| Field | Default | Meaning |
|---|---|---|
| `interval` | 1 s | Evaluation period. |
| `target_utilization` | 0.75 | Steady-state busy fraction to aim for. |
| `drain_time` | 30 s | Time within which a backlog should be cleared. |
| `slo` | none | Per-class maximum predicted queue wait. |
| `reservation_horizon` | 60 s | Reservations starting within this add capacity. |
| `stabilization_window` | 60 s | Scale-down looks at the maximum desired value over this window. |
| `cooldown` | 30 s | Minimum time between scale-down steps. |
| `max_step_up_ratio` / `max_step_up_abs` | 1.0 / 4 | Largest scale-up step. |
| `max_step_down` | 1 | Largest scale-down step. |
| `ewma_alpha` | 0.3 | Smoothing of the arrival rate. |
| `cost_per_worker_second` | 0 | For cost reporting. |

The policy is implemented in `cronoq-core` and is fully deterministic, which makes it testable
with simulated time.
