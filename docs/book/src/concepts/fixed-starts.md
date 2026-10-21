# Fixed starts and reservations

A task with a **fixed start** should begin at a given time. cronoq treats this as a capacity
commitment: it books capacity when the task is submitted and holds a worker shortly before the
start.

```rust,ignore
Task::new(job)
    .start_at(when)                          // or .start_in(Duration)
    .start_tolerance(Duration::from_secs(5)) // default 1 s
    .on_miss(MissPolicy::Skip)               // default RunLate
```

## 1. Admission: reservations

The task occupies a reservation interval in its pool:

```text
[start − tolerance,  start + estimated_duration + tolerance]
```

At any instant, at most `floor(max_workers × reservable_fraction)` reservations may overlap
(`reservable_fraction` defaults to **0.5**, and at least one reservation is always allowed). If
the new interval would exceed that, submission fails with **`ReservationConflict`**. Keeping the
fraction below 1 guarantees ad-hoc work always has part of the pool.

## 2. Holding a worker

Shortly before the start, the task asks for a **hold**: the next free slot of its pool is kept
idle for it instead of taking regular work. How early depends on the pool's own behaviour:

```text
hold_lead = min(max(tolerance, p90 run time of regular tasks in the pool), max_hold)
```

The idea is to hold a slot just early enough for the regular work that would otherwise take it
to finish. The lead is computed from the run times of the pool's tasks without a fixed start,
and it is re-evaluated as the start approaches (the first check happens at `start − max_hold`,
`max_hold` = 5 minutes by default), so a pool whose tasks get slower starts holding earlier
without any configuration. If no slot is free when the hold is requested, the first one to free
up is held, before any queued task can take it.

Reservations starting within the autoscaler's horizon also add to the pool's desired capacity,
so an autoscaled pool grows before the hold is needed.

## 3. Start

At the start time (and once its dependencies are satisfied) the task runs on its held slot. The
recorded start time is the actual dispatch time; the pool's `fixed_start_hit_rate` counts the
task as a hit if `|actual − start| ≤ tolerance`.

A tolerance of zero is allowed: the task must start exactly at its time (in practice, on the
first scheduling step at or after it), and anything later is a miss.

## 4. Missing the start

If the task has not started by `start + tolerance`, its **miss policy** applies:

| Policy | Effect |
|---|---|
| `RunLate` (default) | Run as soon as a slot is free, ahead of all regular work. |
| `Skip` | End with status `Missed`. |
| `Fail` | End with status `Failed`. |

Typical reasons for a miss: all held capacity is in use by other fixed tasks, the pool has no
workers (server mode with no connected workers), or a dependency finished late.

## Interaction with other features

- **Dependencies**: a fixed-start task with unmet dependencies waits for both its time and its
  dependencies.
- **Resources**: a fixed-start task also needs its resources; if they are not available at the
  start time it waits for them while keeping its slot.
- **Retries**: a retried attempt is dispatched like regular work.
- **Overload**: fixed-start tasks are never shed by the `shed_lowest` / `shed_oldest` overflow
  policies; their capacity was promised at admission.
- **Recurring schedules** create fixed-start tasks; see [Recurring schedules](recurring.md).

## Configuration

| Setting | Default | Where |
|---|---|---|
| Tolerance | 1 s | per task |
| Miss policy | `RunLate` | per task |
| `reservable_fraction` | 0.5 | per pool |
| `max_hold` | 5 min | per pool (`PoolConfig`) |
| `default_duration` | 1 s | per pool; used when no estimate exists |
