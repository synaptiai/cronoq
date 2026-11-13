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
it delay urgent work. If you want stronger mixing, change the settings below: a higher
`aging_rate` makes waiting count sooner, and a higher `age_cap` lets it count for more.

The time is measured from **submission**. A task released by its dependencies, or retried after
a failure, keeps its original place in its class.

## Why dispatch never rescans the backlog

Within one class, every task ages at the same rate from its own submission time, so an older
task always has a score at least as high as a younger one. A FIFO queue per class therefore
always has its best task at the head. Deadline tasks live in a second per-class queue ordered by
latest start time. In pools with fair sharing, tasks without deadlines live in a per-class
fair-share queue ordered by start tag instead of the FIFO lane. Dispatch compares at most eight
heads (four classes × two queues), computing their scores lazily. There is no periodic rescoring
and no full-queue scan, so choosing the next task costs at most the logarithm of the backlog.
Fresh tasks join the back of their lane; a retried or woken task goes back to its place by
submission order, which in a very long lane moves the entries behind it.

## Measuring adherence

Each pool counts **priority inversions**: dispatches made while a ready task of a strictly
higher class was waiting in the same pool (tasks blocked on resources do not count).
`priority_adherence = 1 − inversions / dispatches` over the metrics window. With aging and
deadlines some inversions are intended; a sudden drop usually means an aging or deadline policy
is overriding priorities more often than expected.

## Configuration

| Setting | Default | Key |
|---|---|---|
| Base scores | 10000 / 1000 / 100 / 10 | `base` |
| Aging rate | 2.0 points/s for every class | `aging_rate` |
| Age cap | 800 for every class | `age_cap` |
| Let waiting pass fresh `high` or `critical` work | no | `allow_overtaking_high` |

The first three hold four values, in the order critical, high, normal, low.

Set them for the whole scheduler, and override them for one pool where that pool needs a
different policy. Scores are only ever compared between tasks of the same pool, so a pool's
settings affect that pool alone. A pool that sets only some keys keeps the scheduler-wide
values for the rest.

| Where you run cronoq | Whole scheduler | One pool |
|---|---|---|
| Server | `[priority]` table | `priority = { ... }` in the pool ([reference](../reference/configuration.md#priority-and-urgency)) |
| Rust | `Builder::priority(PriorityConfig { .. })` | `Pool::priority(..)` |
| Python | `Scheduler(..., priority=PriorityConfig(...))` | `Pool(..., priority=PriorityConfig(...))` |

```toml
# Batch work overtakes fresh normal work after 10 s instead of 45 s, everywhere.
[priority]
aging_rate = [2, 2, 2, 9]

# In this pool, background work may also pass fresh high work, after about 2 minutes of waiting
# (10 + 9w > 1000; the pool keeps the rate of 9 set above).
[[pools]]
name = "reports"
priority = { age_cap = [800, 800, 800, 1000], allow_overtaking_high = true }
```

### What is checked

The settings are checked when the server starts (and by `--check`), when a Rust scheduler is
built, and when a Python `PriorityConfig` is created:

- every value is finite and not negative;
- the base scores are strictly ordered: critical above high above normal above low;
- no class can pass freshly submitted `high` or `critical` work by waiting alone, that is
  `base[lower] + age_cap[lower] <= base[higher]` for every class whose `aging_rate` is above
  zero. This is the guarantee described under
  [Aging](#aging), and a cap that breaks it is usually a mistake. Set
  `allow_overtaking_high` to accept it.

`low` passing fresh `normal` work is always allowed; that is what aging is for.

The [snapshot](metrics.md) reports the settings in effect for each pool.
