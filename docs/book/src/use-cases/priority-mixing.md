# Interactive and batch work on shared capacity

## The problem

A service runs two kinds of work on the same workers: requests a user is waiting for, and
background jobs (exports, re-indexing, reports). With one FIFO queue, a large export blocks every
user request behind it. The usual fix is separate queues and separate worker fleets, which means
paying for interactive capacity that is idle most of the day, while the batch fleet cannot borrow
it during quiet hours.

## The setup

One pool, four priority classes:

```python
async with cronoq.Scheduler([cronoq.Pool("app", run, min=2, max=32)]) as sched:
    # A user clicked "export": they are waiting.
    await sched.submit(job, priority="high", kind="export-interactive")

    # Nightly re-index of 50,000 documents.
    for doc in docs:
        await sched.enqueue(doc, priority="low", kind="reindex")

    # A payment webhook that must never wait.
    await sched.submit(event, priority="critical", kind="webhook")
```

Optionally give each class a wait-time objective, so the autoscaler adds capacity when it would
be violated:

```python
autoscale = cronoq.AutoscaleConfig(slo={"critical": 0.5, "high": 2.0})
pool = cronoq.Pool("app", run, min=2, max=32, autoscale=autoscale)
```

## How the order is decided

Every waiting task has a score (see [Priorities and aging](../concepts/priorities.md)):

```text
score = base[class] + min(2 × seconds_waiting, 800)
base  = critical 10000 · high 1000 · normal 100 · low 10
```

Dispatch always picks the highest score. In practice:

- A new `critical` task goes first.
- `high` always beats `normal` and `low`, however long they have waited.
- A `low` task that has waited about 45 seconds overtakes freshly submitted `normal` work, so
  `normal` traffic cannot starve the batch queue indefinitely.
- Within a class, order is strictly FIFO.

## What you get

- **One fleet instead of two.** Interactive work borrows the whole pool when it needs it; batch
  work uses whatever is left. No idle "fast lane" capacity.
- **Bounded latency for users** regardless of how much batch work is queued, because dispatch
  cost does not depend on queue length and higher classes are always considered first.
- **Batch jobs still finish.** Aging prevents starvation by `normal` traffic, and you can tune
  the aging rate and caps per class.
- **Measurable fairness.** `priority_adherence` in each pool snapshot reports how often a task
  was dispatched while a strictly higher class was waiting (with aging, a value slightly below
  1.0 is expected and healthy).

## Variations

- **Protect batch throughput at peak**: put batch work in its own pool with `min` workers and
  interactive work in another; resources can still be shared between pools.
- **Different aging policy**: configure `PriorityConfig` (Rust) per class, for example a higher
  `age_cap` for `low` to let it overtake `high` after a long wait.
