# Updating queued tasks

A task's priority, deadline and tags can be changed after it was submitted, for as long as it
has not finished. Use this when the importance of work changes while it waits: a user opens
the document whose indexing sat in the background queue, a report is needed sooner than
planned, or a deadline is extended because its consumer moved.

Updating a task is not cancelling and resubmitting it. The task keeps its id, its key, its
dependents, its attempt count and its place by submission order, and callers already waiting
on it are not disturbed.

## What can change

| Field | Effect |
|---|---|
| Priority | The task moves to the new class. Among the tasks of that class it is placed by its original submission order, so a promoted task is not sent to the back. Its waiting time for [aging](priorities.md#aging) still counts from submission. |
| Deadline | Set a new deadline (relative or absolute) or remove it. [Urgency](deadlines.md) and deadline ordering follow the new value. |
| Tags | Replace the task's tags. The next attempt's executor or worker sees the new set. Tag names starting with `cronoq.` are reserved and refused. |

Everything else is fixed at submission: payload, kind, pool, key, dependencies, resources,
labels, tenant, retry policy and timeout.

Fields that are not named keep their value. Naming the values a task already has succeeds and
changes nothing (`changed` is false in the answer), so an update can be repeated safely.

```rust
use cronoq::{Priority, Update};
use std::time::Duration;

let r = sched
    .update("index-doc-42", Update::new().priority(Priority::High).deadline_in(Duration::from_secs(30)))
    .await?;
assert!(r.changed);
```

```python
r = await sched.update("index-doc-42", priority="high", deadline_in=30)
```

```sh
curl -s -X PATCH $CRONOQ/v1/tasks/by-key/index-doc-42 -H "$AUTH" \
    -H 'content-type: application/json' -d '{"priority": "high", "deadline_in_s": 30}'
# {"changed":true,"deferred":false,"id":"17","status":"ready"}
```

## When the change takes effect

| The task is | What happens |
|---|---|
| Waiting to run (`Ready`), waiting for a dependency (`Blocked`), or waiting for a retry (`Retrying`) | The new values apply at once. The next dispatch decision uses them. |
| Waiting for a [resource](resources.md) | The values are stored, but the task keeps its place among that resource's waiters, which are served in the order they started waiting whatever their class. The new priority counts once the resource lets the task through. |
| Running | The attempt is not interrupted and keeps the tags it started with. The new values apply to what follows it: a retry, if the attempt fails. The answer says `deferred: true`. |
| Finished, or unknown | Refused: *not found*. A finished task has nothing left to schedule. |
| Scheduled with a [fixed start](fixed-starts.md) | Refused: *fixed start*. Its slot was reserved against the pool's capacity when it was admitted; cancel and resubmit it to change it. |

An update never changes a task's status and never counts as an attempt.

A promotion is not checked against the queue bound of the class the task moves to
([Backpressure](backpressure.md)): the task was already admitted, and the pool's total does
not change.

In a pool that [shares capacity between tenants](fairness.md), tasks without a deadline are
ordered by tenant share and not by submission order. A task that changes class there (or loses
its deadline) joins its tenant's work in the new class as of the update; the tenant's share is
the same as before.

## Events

With `emit_transitions` enabled (`all_transitions` on `Tasks/Watch`), each update that changed
something publishes a `StatusEvent` whose `kind` is `Updated`. Its `status` is the task's
unchanged status and its `priority` is the class after the change. Consumers that only match
on `status` should check `kind` first, so they do not count an update as a second `Ready` or
`Running` transition. Updates are not published to subscribers that only receive terminal
statuses.

## Durability

With a durable store the update is stored before the call returns, and a restart or failover
restores the task with its new priority, deadline and tags. No schema migration is involved.

## Who may update

Over the server, `Update` needs the `client` role, like `Cancel`. Credentials limited to
certain pools or pinned to a tenant can only update tasks in those pools and of that tenant;
any other task is reported as not found. With [shards](sharding.md), an update is forwarded to
the shard that holds the task; an update by key is refused while a shard cannot be asked,
since that shard might hold the same key.
