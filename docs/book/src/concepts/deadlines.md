# Deadlines and urgency

A **deadline** is a soft "should be done by" time. It does not cancel the task when it passes;
it raises the task's score as the deadline approaches so that it is dispatched in time.

## Latest start time

```text
d   = duration estimate (duration_hint, else learned median for (pool, kind),
      else the pool's learned median, else the pool's default_duration = 1 s)
LST = deadline − d          latest start time
slack = LST − now
H   = max(d, urgency_window)            urgency_window = 5 s
```

## Urgency curve

```text
urgency = 0                                      if slack ≥ H
        = U × (1 − slack / H)                    if 0 ≤ slack < H
        = U + min(overdue_rate × (−slack), cap)  if slack < 0
```

with `U = 5000`, `overdue_rate = 100` per second and `cap = 3000`.

```text
 urgency
 8000 ┤                                  ┌──────────── cap reached
      │                               ╱
 5000 ┤                        ●───╱       overdue: keeps rising
      │                     ╱
      │                  ╱      rising linearly over the horizon H
    0 ┼───────────────╱
      └──────────────┬──────────┬──────────────────▶ time
                  LST − H      LST
```

The curve is **monotonic**: a task that is further past its latest start always has at least the
urgency of one that is less late.

## Ordering

- Within a class, tasks with deadlines are kept in a queue ordered by latest start time
  (earliest-deadline-first), and the head competes with the class's FIFO lane by full score.
- Across classes, urgency is added to the base score, so a `Low` task near its deadline
  (`10 + 5000`) overtakes fresh `High` work (`1000`) but not `Critical` work (`10000`).

## Choosing values

- Give a `duration_hint` when you know it; otherwise the first tasks of a `kind` use the pool's
  learned typical run time (or `default_duration` if nothing in the pool has completed yet) until
  cronoq has observed completions of that kind.
- Deadlines are about *finishing*. If something must *start* at a given time, use a
  [fixed start](fixed-starts.md).
- To get more capacity when deadlines are at risk, set per-class wait SLOs in the pool's
  [autoscaling](autoscaling.md) configuration.

## Configuration

| Setting | Default | Where |
|---|---|---|
| Maximum urgency `U` | 5000 | `UrgencyConfig::max` |
| Minimum horizon | 5 s | `UrgencyConfig::window` |
| Overdue rate | 100 /s | `UrgencyConfig::overdue_rate` |
| Overdue cap | 3000 | `UrgencyConfig::overdue_cap` |
