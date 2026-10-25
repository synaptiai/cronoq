# Priorities and aging

## Classes

| Class | Base score |
|---|---|
| `Critical` | 10,000 |
| `High` | 1,000 |
| `Normal` (default) | 100 |
| `Low` | 10 |

## Effective score

When a worker slot is free, the pool dispatches the ready task with the highest **effective
score**:

```text
score = base[class] + min(aging_rate[class] × wait_seconds, age_cap[class]) + urgency
```

with defaults `aging_rate = 2.0` points per second and `age_cap = 800` for every class.
`urgency` is zero unless the task has a [deadline](deadlines.md). Ties are broken by class, then
by submission order.

## Aging

Aging prevents **starvation**: a task that keeps waiting slowly gains score until it overtakes
newer work of a higher class.

With the defaults:

- A `Low` task reaches `10 + 800 = 810` after 400 seconds. Against a continuous stream of fresh
  `Normal` work (score about `100`), it wins once `10 + 2w > 100`, that is after about
  **45 seconds** of waiting.
- `Normal` can reach `900` and `High` can reach `1800`. Neither crosses the next class's *base*
  score (`1000` and `10000`), so **fresh `High` work always beats aged `Normal` work, and
  `Critical` always beats everything except deadline urgency**.

This is deliberate: aging rescues background work from floods of ordinary work, but never lets
it delay urgent work. If you want stronger mixing, raise `age_cap` (Rust:
`Builder::priority(PriorityConfig { ... })`).

The time is measured from **submission**. A task released by its dependencies, or retried after
a failure, keeps its original place in its class.

## Why dispatch is constant-time

Within one class, every task ages at the same rate from its own submission time, so an older
task always has a score at least as high as a younger one. A FIFO queue per class therefore
always has its best task at the head. Deadline tasks live in a second per-class queue ordered by
latest start time, and in pools with fair sharing, tenants' tasks live in a third per-class queue
ordered by start tag. Dispatch compares at most twelve heads (four classes × three queues),
computing their scores lazily. There is no periodic rescoring and no full-queue scan, so dispatch cost
does not grow with the backlog.

## Measuring adherence

Each pool counts **priority inversions**: dispatches made while a ready task of a strictly
higher class was waiting in the same pool (tasks blocked on resources do not count).
`priority_adherence = 1 − inversions / dispatches` over the metrics window. With aging and
deadlines some inversions are intended; a sudden drop usually means an aging or deadline policy
is overriding priorities more often than expected.

## Configuration

| Setting | Default | Where |
|---|---|---|
| Base scores | 10000 / 1000 / 100 / 10 | `PriorityConfig::base` |
| Aging rate | 2.0 points/s | `PriorityConfig::aging_rate` |
| Age cap | 800 | `PriorityConfig::age_cap` |

All three are arrays indexed by class (critical, high, normal, low).
