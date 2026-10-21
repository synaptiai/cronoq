# Deadline-driven work

## The problem

Some work has a time by which it must be *done*: a report promised by 09:00, a fraud check that
must finish before a payment settles, a transcoding job due before a broadcast. A priority alone
does not capture this: a "normal" job due in five minutes is more urgent than a "high" job due
tomorrow, but only once its deadline gets close.

## The setup

Give tasks a deadline and, when known, a duration hint:

```rust,ignore
use std::time::{Duration, SystemTime};
use cronoq::{Priority, Task};

// Must be done in 10 minutes; usually takes ~2 minutes.
sched.submit(Task::new(report)
    .priority(Priority::Normal)
    .deadline_in(Duration::from_secs(600))
    .duration_hint(Duration::from_secs(120))).await?;

// Absolute deadline.
sched.submit(Task::new(batch)
    .deadline_at(SystemTime::now() + Duration::from_secs(3600))).await?;
```

```python
await sched.submit(report, deadline_in=600, duration_hint=120)
```

Without a duration hint, cronoq uses the median runtime it has observed for the task's `kind`
in that pool, then the pool's statistics, then the pool's `default_duration` (1 s).

## How urgency works

From the deadline `D` and the estimated duration `d`, cronoq computes the *latest start time*
`LST = D − d`. As the remaining slack `LST − now` shrinks below a horizon
`H = max(d, 5 s)`, the task's score gains urgency, rising linearly to 5000 at the latest start
time. After that it keeps rising (100 points per second, up to +3000) so that an overdue task
always ranks above one that is merely close. Within a priority class, tasks with deadlines are
ordered by latest start time (earliest-deadline-first), which is optimal for meeting deadlines
on a single resource.

Because 5000 exceeds the gap between `high` (1000) and `low` (10), a `low` task near its
deadline overtakes fresh `high` work, but not `critical` work (10000).

## What you get

- **Fewer missed deadlines with the same capacity.** Work is reordered as deadlines approach,
  instead of strictly by priority or arrival.
- **Correct ordering among deadline tasks**: earliest latest-start-time first.
- **Escalation for overdue work** that is monotonic, so overdue tasks never fall behind ones that
  are merely close to their deadline.
- **Learned estimates.** Duration estimates improve as tasks of the same `kind` complete.
- **Visibility.** `TaskContext::deadline` (Rust) gives executors the deadline so they can choose
  a cheaper code path when time is short.

## Related

- If the work must *start* at a specific time rather than *finish* by one, use a
  [fixed start](scheduled-jobs.md) instead.
- To add capacity when deadlines are at risk, combine deadlines with per-class wait SLOs on the
  pool's [autoscaling](../concepts/autoscaling.md) configuration.
