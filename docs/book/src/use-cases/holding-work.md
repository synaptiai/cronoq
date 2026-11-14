# Holding work without losing it

## The problem

Sometimes work must stop being started, but it must not be lost:

- A downstream service is being deployed, or is down.
- A backfill is slowing down interactive traffic.
- The data of one customer is being repaired, and nothing should touch it.
- A client device is offline, or on a metered connection.

The usual tools all lose something. Scaling the workers to zero stops everything and hides the
reason. Cancelling loses the work and its place in line. Letting the tasks run and fail uses
up their retries, and tasks that had nothing wrong with them end as dead letters.

## The setup

cronoq has two tools. One is for an operator who knows that work must wait. The other is for
a worker that finds out while it holds a task.

| Who decides | Tool | Effect |
|---|---|---|
| An operator, or the program itself | [Pause](../concepts/pausing.md) a pool, a class or an ordering key | Nothing of it is started until it is resumed. |
| The worker or executor that holds an attempt | [Release](../concepts/reliability.md#releasing-an-attempt) the attempt | The task is given back and offered again after a delay. No retry is used. |

### Pause

A pause holds work back. What is paused stays queued, exactly as it is. Every pause carries a
**reason**, which is shown wherever the pause is shown.

```sh
cronoq pause --reason "billing API deploy until 14:00"                      # the pool
cronoq pause --class low --reason "backfill is starving interactive work"   # one class
cronoq pause --ordering-key account:8842 --reason "repairing this account"  # one entity

cronoq resume
cronoq resume --class low
cronoq resume --ordering-key account:8842
```

```python
await sched.pause("billing API deploy until 14:00")
await sched.pause("backfill off", priority_class="low")
await sched.pause("repair", ordering_key="account:8842")
await sched.resume()
```

Over HTTP the routes are `POST /v1/pools/{pool}/pause`, `/resume` and
`/ordering/{key}/pause` ([Using the HTTP API](../guides/http-api.md#pausing)).

### Release

A worker that cannot run a task now, although nothing is wrong with the task, gives it back:

```python
if not downstream.is_up():
    raise cronoq.Release(30)
```

```rust,ignore
if !downstream.is_up() {
    return Err(TaskError::release(Duration::from_secs(30)));
}
```

The task keeps its place in line. It is offered again, to any worker, once the delay has
passed. The released attempt does not count against the retry policy.

### Both together

For a maintenance window, pause the pool first. Attempts that are already running are not
interrupted. A worker whose attempt can no longer reach the downstream service releases it.
Releases made while a pause holds the task do not count toward the release limit, so workers
can give back every task of a paused pool. After the window, resume the pool. The tasks run in
the order they would have had.

## What a pause does

| | While paused |
|---|---|
| Tasks that wait | Stay queued. They keep their place and their age. |
| New submissions | Accepted. They wait with the rest. |
| Attempts that are running | Not interrupted. |
| Cancel, update, replace | Work as usual on held tasks. |
| Tasks with a [fixed start](../concepts/fixed-starts.md), and occurrences of a [schedule](../concepts/recurring.md) | **Not held.** They start when they are due. |
| Autoscaling | Work held by a pause of its pool or class is no reason to add workers. |
| A restart or a failover | With a [durable store](../concepts/durability.md), every pause is still in place afterwards. |

## What you get

- **Nothing is lost and nothing is reordered.** Held tasks keep their place, their age and
  their priority.
- **Retries are not wasted.** A released attempt is not a failure. A task with
  `max_attempts = 3` that was released twice may still fail three times.
- **The reason is visible.** The snapshot, `cronoq snapshot`, `cronoq top` and
  `cronoq ordering get` show what is paused and why. A worker that connects to a paused pool
  is told the reason, and the worker libraries log it.
- **The right size of hold.** Hold one entity, one class or a whole pool. The rest keeps
  running.
- **Metrics.** `cronoq_pool_paused`, `cronoq_pool_paused_classes`, `cronoq_pool_paused_tasks`
  and `cronoq_pool_ordering_paused` report pauses. `cronoq_releases_total` counts releases.

## Trade-offs

- **A paused pool fills up.** Held tasks count toward the
  [queue bounds](../concepts/backpressure.md). When the queue is full, the pool applies its
  overflow policy: on the server, new submissions are rejected by default.
- **No timer.** A pause lasts until someone resumes it. Put the expected end in the reason.
- **Not an abort.** A pause does not stop an attempt that is running. Cancel the task to stop
  its attempt.
- **Not per tenant.** No call holds one tenant's tasks in a pool. An ordering key per subject
  is the closest tool.
- **Start times are not held.** To stop a schedule, remove it. To stop a task with a fixed
  start, cancel it.
- **Releases are limited.** `max_releases` is 10 per task unless the pool or the task sets
  another value. A release beyond the limit counts as a retryable failure.
- **A release without a delay comes back at once.** Give a delay, unless the worker is going
  away.
- **Older servers do not know releases.** The worker libraries then send a retryable failure
  with the delay, which uses a retry ([Upgrading](../guides/upgrading.md#workers-and-releases)).
- **Roles.** Over the server, pausing and resuming need the `admin` role. Credentials pinned
  to a tenant pause and resume their own ordering keys only.

For a client app that holds its own queue while the device is offline, see
[Offline-first clients and embedded queues](embedded-queues.md).
