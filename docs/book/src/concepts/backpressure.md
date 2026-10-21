# Backpressure

Queues in cronoq can be **bounded**, so that overload turns into explicit backpressure instead
of unbounded memory growth.

## Bounds

A task counts as **pending** from submission until it starts running (and again while it waits
for a retry). Bounds limit pending tasks:

| Bound | Rust | Python | Server |
|---|---|---|---|
| Per pool | `PoolConfig::bounds.max_pending` | `Pool(max_pending=...)` | `[[pools]] max_pending` (default `limits.max_pending_per_pool`, 100 000) |
| Per pool and class | `PoolConfig::bounds.max_pending_per_class` | | |
| Global | `CoreConfig::global_bounds` | | `limits.max_pending_total` (1 000 000) |

The library leaves queues unbounded unless you set a bound; the server always bounds them.

## Overflow policies

What happens when a submission would exceed a bound is set per pool:

| Policy | Effect | Default for |
|---|---|---|
| `Wait` | The submit call waits until space frees up (FIFO per pool). | library |
| `Reject` | The submit fails with `Overloaded` (HTTP 429, gRPC `RESOURCE_EXHAUSTED`). | server |
| `ShedLowest` | Evict the newest queued task of the lowest class strictly below the incoming one (status `Shed`); reject if there is none. | |
| `ShedOldest` | Evict the oldest queued `Low` task; reject if there is none. | |

Shedding rules:

- **Victims** are tasks waiting to run: queued tasks, including those parked waiting for a
  shared resource. Running tasks are never shed.
- **Fixed-start tasks are never shed**: their capacity was promised when they were admitted.
- **Only when it helps.** A bound on the incoming task's own class is never relieved by evicting
  tasks of other classes, so such submissions are rejected without shedding.
- **Only for admissible tasks.** All other admission checks (unknown pool or dependency, cycle,
  reservation conflict) run first, so a submission that would be rejected anyway never evicts
  anything.
- **Not for batches.** A batch that does not fit is rejected (or waits, with `Wait`).
- A shed task ends with status `Shed`; its dependents are resolved like for any other terminal
  status.

## Choosing a policy

- **In-process producers** (`Wait`): natural flow control; a fast producer slows to the pool's
  pace.
- **Network clients** (`Reject`): clients should not hold connections open; they retry with
  backoff on 429 / `RESOURCE_EXHAUSTED`.
- **Best-effort background work** (`ShedLowest`): important work is always admitted; low-value
  work is dropped under pressure and you see it in `cronoq_tasks_total{status="shed"}`.

A waiting submitter whose caller gives up (dropped future, timed-out request) is removed from
the wait queue without being admitted.
