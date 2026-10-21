# Scheduled jobs with guaranteed capacity

## The problem

Cron jobs and "run at 14:00" tasks usually share workers with everything else. When the time
comes, every worker may be busy with long jobs, so the scheduled job starts late, sometimes much
later. Teams compensate with dedicated cron machines that sit idle 99% of the time.

## The setup

Submit work with a fixed start time:

```rust,ignore
use std::time::Duration;
use cronoq::{MissPolicy, Task};

sched.submit(Task::new(close_books)
    .start_at(at_14_00)                        // SystemTime
    .start_tolerance(Duration::from_secs(5))   // "on time" = within ±5 s
    .on_miss(MissPolicy::Skip)                 // if it still can't start, drop it
    .duration_hint(Duration::from_secs(300))).await?;
```

Or a recurring schedule (cron with optional seconds, IANA time zones):

```rust,ignore
use cronoq::{OverlapPolicy, Recurring, Task};

sched.add_recurring(
    Recurring::new("hourly-rollup", "0 0 * * * *", Task::new(rollup).duration_hint(Duration::from_secs(600)))
        .tz("Europe/Lisbon")
        .overlap(OverlapPolicy::Skip),          // skip an occurrence while the previous one runs
).await?;
```

```python
await sched.add_recurring("hourly-rollup", "0 0 * * * *", rollup_payload,
                          tz="Europe/Lisbon", overlap="skip", duration_hint=600)
```

## What happens

1. **Admission.** The task reserves an interval `[start − tolerance, start + duration + tolerance]`
   in its pool. Up to half of the pool's `max_workers` (at least one) may be reserved at any
   moment; if the new reservation would exceed that, the submission fails immediately with
   `ReservationConflict`. You learn about an impossible schedule when you create it, not when it
   runs late.
2. **Pre-warming.** Reservations within the autoscaler's horizon (60 s by default) count towards
   the pool's desired capacity, so workers are added before they are needed.
3. **Holding.** Shortly before the start (by the pool's p90 task duration, at least the
   tolerance, at most 5 minutes), the next free worker is *held* for the task: regular work
   cannot take it.
4. **Start.** At the start time the task runs on its held worker. The recorded start time is the
   actual dispatch time, and the pool's `fixed_start_hit_rate` metric counts it as on time if it
   started within the tolerance.
5. **Miss.** If it still could not start within the tolerance, the miss policy applies:
   `RunLate` (default: run as soon as possible, ahead of everything else), `Skip` (status
   `Missed`), or `Fail`.

## What you get

- **On-time starts without a dedicated fleet.** Capacity is reserved and a worker is held only
  around the start time; the rest of the time it serves regular work.
- **Early feedback.** Over-subscribed schedules are rejected at submission time.
- **Explicit miss semantics** instead of silently late jobs.
- **Measurable punctuality** via `fixed_start_hit_rate`.
- **Durable schedules.** With the Postgres store, recurring definitions and their next
  occurrence survive restarts and failovers. After downtime, at most one missed occurrence runs
  late (with `RunLate`); the schedule then resumes from the present instead of replaying every
  missed occurrence.
