# Reprioritising live work

## The problem

The importance of work changes while it waits. A document was queued for indexing at low
priority, and now a user has opened it and is waiting for the search result. A report is
needed sooner than planned. A customer calls support, and every task of that customer's
account should move ahead.

In a plain queue the only tool is to cancel the task and submit it again. That loses things.
The new task has a new id, so callers that wait on the old one get a cancellation. Tasks that
wait for the old one to succeed are skipped. The new task goes to the back of the queue.

## The setup

cronoq has two tools. Which one fits depends on what you want to move.

| You want to move | Use | What changes |
|---|---|---|
| One task | [Update](../concepts/updating.md) the task | The task's own priority, deadline or tags. |
| Everything of one entity that uses an [ordering key](../concepts/ordering-keys.md) | A [class floor](../concepts/ordering-keys.md#a-floor-for-a-key) on the key | The class the key's front task runs at. No task's own priority changes. |

### Update one task

Name the task by id or by key, and give the new values. Fields that are not named keep their
value.

```python
r = await sched.update("index-doc-42", priority="high", deadline_in=30)
```

```rust,ignore
use cronoq::{Priority, Update};
use std::time::Duration;

let r = sched
    .update("index-doc-42", Update::new().priority(Priority::High).deadline_in(Duration::from_secs(30)))
    .await?;
assert!(r.changed);
```

```sh
curl -s -X PATCH $CRONOQ/v1/tasks/by-key/index-doc-42 -H "$AUTH" \
    -H 'content-type: application/json' -d '{"priority": "high", "deadline_in_s": 30}'
# {"changed":true,"deferred":false,"id":"17","status":"ready"}

cronoq update 17 --priority high --deadline-in 30
```

The task keeps its id, its key, its dependents and its attempt count. In the new class it is
placed by its original submission time, so a promoted task is not sent to the back. Callers
that already wait on it are not disturbed.

There is one exception. In a pool that
[shares capacity between tenants](../concepts/fairness.md), tasks without a deadline are
ordered by tenant share, not by submission order. A task that changes class there joins its
tenant's work in the new class as of the update, behind what the tenant already has waiting
in that class ([details](../concepts/updating.md)).

### Raise one entity

When the work of an entity uses an ordering key, set a floor on the key. The task at the
front of the key then never runs below that class.

```sh
cronoq ordering floor account:8842 high             # until the key has no tasks left
cronoq ordering floor account:8842 high --persist   # kept while the key is empty too
cronoq ordering floor account:8842 --clear
```

The same call exists as `set_ordering_floor` in Rust and Python, `SetOrderingFloor` in Go, and
`PUT /v1/pools/{pool}/ordering/{key}/floor` over HTTP. One call covers the tasks that wait now
and the tasks that are submitted for the key while the floor is set.

## When an update takes effect

| The task is | What happens |
|---|---|
| Waiting to run, waiting for a dependency, or waiting for a retry | The new values apply at once. |
| Waiting for a [resource](../concepts/resources.md) | The values are stored. The task keeps its place among that resource's waiters. The new priority counts once the resource lets the task through. |
| Running | The attempt is not interrupted. The new values apply to a retry, if the attempt fails. The answer says `deferred: true`. |
| Finished, or unknown | Refused: *not found*. |
| Submitted with a [fixed start](../concepts/fixed-starts.md) | Refused: *fixed start*. Cancel and resubmit it to change it. |

## What you get

- **The task stays the same task.** Same id, same key, same dependents, same attempt count.
- **A fair place in the new class.** The task is ordered by its original submission time, and
  its waiting time for [aging](../concepts/priorities.md#aging) still counts from submission.
  In a pool with fair sharing between tenants, a task without a deadline is placed behind its
  tenant's waiting work in the new class instead.
- **Safe to repeat.** An update that names the values a task already has succeeds and changes
  nothing (`changed` is false).
- **Works in both directions.** An update can also lower a priority, or remove a deadline.
- **Durable.** With a durable store the update is stored before the call returns.
- **Visible.** With `emit_transitions`, each update that changed something is published as an
  event of kind `Updated` ([Events](../concepts/updating.md#events)).

## Trade-offs

- **Only priority, deadline and tags can change.** Payload, kind, pool, key, dependencies,
  resources, labels, tenant, retry policy and timeout are fixed at submission. To change the
  payload of a waiting task, use [replace by key](latest-wins.md).
- **An update does not interrupt.** A promoted task that is already running is not made
  faster, and a task that waits for a rate limit still waits for it.
- **Inside an ordering key, priority does not change the order.** A promoted task still runs
  after the older tasks of its key. The promotion raises the class the key's front task runs
  at, so the whole key moves ahead of other work.
- **A floor only raises.** A floor never lowers a key. Without `--persist` it goes away when
  the key has no tasks left. The number of keys that keep a floor while they are empty is
  limited (`limits.max_ordering_settings`, 100,000 by default).
- **A promotion is not checked against queue bounds.** The task was already admitted, so the
  bound of its new class does not refuse it ([Backpressure](../concepts/backpressure.md)).
- **Roles.** Over the server, an update needs the `client` role and a floor needs the `admin`
  role.
