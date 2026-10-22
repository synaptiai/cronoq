# Recurring schedules

A **recurring schedule** turns a cron expression into a sequence of
[fixed-start](fixed-starts.md) tasks.

```rust,ignore
sched.add_recurring(
    Recurring::new("nightly-export", "0 30 2 * * *", Task::new(export).pool("batch"))
        .tz("America/New_York")
        .overlap(OverlapPolicy::Skip)
        .tolerance(Duration::from_secs(60))
        .on_miss(MissPolicy::RunLate),
).await?;
sched.remove_recurring("nightly-export").await?;
```

```python
await sched.add_recurring("nightly-export", "0 30 2 * * *", payload, pool="batch",
                          tz="America/New_York", overlap="queue", tolerance=60,
                          key="nightly-export", deadline_in=3600)
```

Over gRPC, use `Admin.AddRecurring` with an admin token (for example the Python client's
`Client.add_recurring`).

## Cron syntax

Five fields (`minute hour day month weekday`) or six with leading seconds
(`second minute hour day month weekday`). Names (`MON`, `JAN`) and nicknames (`@hourly`,
`@daily`) are accepted. Occurrences fall on whole seconds.

Time zones are IANA names and default to UTC. Daylight-saving transitions follow the zone's
rules:

| Transition | Daily job (e.g. `0 30 2 * * *`) | Hourly job (`0 0 * * * *`) |
|---|---|---|
| Spring forward (a local hour is skipped) | A time inside the gap runs at the first valid time after it (02:30 → 03:00). | Runs every real hour. |
| Fall back (a local hour repeats) | Runs once. | Runs every real hour, including both occurrences of the repeated local hour. |

Use UTC for schedules that must run at exact intervals.

## How occurrences are created

- Each occurrence is created as a task shortly before it starts: `hold_lead + tolerance` ahead
  (capped by the pool's `max_hold`). Only **one** occurrence exists ahead of time; the next one
  is created when the current one is due.
- Each occurrence is a normal [fixed-start](fixed-starts.md) task: it goes through reservation,
  holding and the miss policy, carries the tag `cronoq.recurring = <name>`, and appears in
  metrics and status events like any other task.
- The template is checked when the schedule is added, like a task submission: its pool and
  resources must exist, resource amounts must fit their capacity, and `max_attempts` must be at
  least 1. A schedule recovered from the store whose template the configuration no longer
  supports (a removed pool or resource) is not armed; the `recurring_invalid` counter records it,
  and it runs again once the configuration supports it.
- If an occurrence cannot be reserved (the pool's reservable capacity is full at that time), it
  is not created; the snapshot's `recurring_conflicts` counter records it.

## Overlap

| Policy | Effect |
|---|---|
| `Skip` (default) | If the previous occurrence is still live when the next one is due, the next one ends as `Skipped` ("previous occurrence still running"). |
| `Allow` | Occurrences run independently and may overlap. |
| `Queue` (`"queue"`) | Each occurrence depends on the previous one and starts once it has finished, whatever the outcome. Occurrences run one after another instead of being skipped. |
| `QueueOnSuccess` (`"queue_on_success"`) | Like `Queue`, but an occurrence runs only if the previous one succeeded. After a failure the following occurrences end as `Skipped`, until the schedule is added again, which starts a new chain. |

A queued occurrence that waits past its start tolerance follows the miss policy, so keep
`on_miss` at its default, `RunLate`, unless a late run should be dropped.

The overlap guard and the chain survive failover: with a durable store, the occurrence a queued
schedule depends on is stored with the schedule, and the occurrence that was running is
recognised by its `cronoq.recurring` tag.

## The template

The template's payload, pool, kind, priority, duration hint, timeout, retry policy, resources
and tags are copied to every occurrence. Two options apply per occurrence:

| Template option | Effect on each occurrence |
|---|---|
| Relative deadline (`Task::deadline_in`, Python `deadline_in=`, gRPC `deadline_in_s`) | The deadline is that long after the occurrence's start. |
| Key (`Task::key`, `key=`, `key`) | A prefix: the occurrence's key is `<key>@<start in UTC, RFC 3339>`, for example `nightly-export@2026-10-22T06:30:00Z`. Look occurrences up by it; an occurrence whose key already exists (for example one created before a failover) is not created again. |

A template may **not** set an absolute deadline, a fixed start or dependencies: they describe a
single task, so `add_recurring` rejects them (`Error::Config` in Rust, `SchedulerError` in
Python, `INVALID_ARGUMENT` on the server). Use `Queue` to make occurrences depend on each other,
and the schedule's `tolerance` and `on_miss` (or the template's start tolerance and miss policy)
to control how late an occurrence may start.

## Downtime and failover

With a durable store, schedules and their next occurrence time are persisted. After downtime:

- if the missed occurrence's policy is `RunLate`, **one** late occurrence runs;
- otherwise the missed occurrence is counted as `missed` and not run;
- the schedule then resumes from the present; missed occurrences are not replayed one by one.

Re-adding a schedule with the same name replaces it without duplicating or overlapping an
occurrence that was already created. Removing a schedule does not cancel an occurrence that was
already created.
