# Backpressure

Queues in cronoq can be **bounded**, so that overload turns into explicit backpressure instead
of unbounded memory growth.

## Bounds

A task counts as **pending** from submission until it starts running (and again while it waits
for a retry). Bounds limit pending tasks:

| Bound | Rust | Python | Server |
|---|---|---|---|
| Per pool | `PoolConfig::bounds.max_pending` | `Pool(max_pending=...)` | `max_pending` |
| Per pool and class | `PoolConfig::bounds.max_pending_per_class` | | |
| Global | `CoreConfig::global_bounds` | | |

## Overflow policies

What happens when a submission would exceed a bound is set per pool:

| Policy | Effect | Default for |
|---|---|---|
| `Wait` | The submit call waits until space frees up (FIFO per pool). | library |
| `Reject` | The submit fails with `Overloaded` (HTTP 429, gRPC `RESOURCE_EXHAUSTED`). | server |
| `ShedLowest` | Evict the newest queued task of the lowest class strictly below the incoming one (status `Shed`); reject if there is none. | |
| `ShedOldest` | Evict the oldest queued `Low` task; reject if there is none. | |

Shedding only happens when evicting the victim actually makes room: a bound on the incoming
task's own class is never relieved by evicting tasks of other classes, so such submissions are
rejected without shedding. Fixed-start tasks are never shed. Batch submissions do not shed; a
batch that does not fit is rejected (or waits, with `Wait`).

## Choosing a policy

- **In-process producers** (`Wait`): natural flow control; a fast producer slows to the pool's
  pace.
- **Network clients** (`Reject`): clients should not hold connections open; they retry with
  backoff on 429 / `RESOURCE_EXHAUSTED`.
- **Best-effort background work** (`ShedLowest`): important work is always admitted; low-value
  work is dropped under pressure and you see it in `cronoq_tasks_total{status="shed"}`.

A waiting submitter whose caller gives up (dropped future, timed-out request) is removed from
the wait queue without being admitted.
