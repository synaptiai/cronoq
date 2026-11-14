# Pausing and resuming

Sometimes work has to stop being started without being lost: a downstream system is being
deployed, a backfill is hurting interactive traffic, one customer's data is being repaired.
Scaling workers to zero stops everything and hides why; cancelling loses the work and its
place in line.

A **pause** holds work back. What is paused stays queued, exactly as it is, and nothing of it
is handed to a worker until it is **resumed**.

| You pause | What is held |
|---|---|
| a pool | Every task of the pool. |
| a class of a pool | The tasks of the pool that run at that class. |
| an [ordering key](ordering-keys.md) of a pool | The tasks of that key. |

```sh
cronoq pause --reason "billing API deploy until 14:00"
cronoq pause --class low --reason "backfill is starving interactive work"
cronoq pause --ordering-key account:8842 --reason "repairing this account"

cronoq resume
cronoq resume --class low
cronoq resume --ordering-key account:8842
```

```rust
use cronoq::PauseTarget;

sched.pause("default", PauseTarget::Pool, "billing API deploy until 14:00").await?;
sched.pause("default", PauseTarget::Class(Priority::Low), "backfill off").await?;
sched.pause("default", PauseTarget::Ordering("account:8842".into()), "repair").await?;
sched.resume("default", PauseTarget::Pool).await?;
```

```python
await sched.pause("billing API deploy until 14:00")
await sched.pause("backfill off", priority_class="low")
await sched.pause("repair", ordering_key="account:8842")
await sched.resume()
```

```go
admin.Pause(ctx, cronoq.PauseTarget{}, "billing API deploy until 14:00")
admin.Pause(ctx, cronoq.PauseTarget{Class: cronoq.Low}, "backfill off")
admin.Pause(ctx, cronoq.PauseTarget{OrderingKey: "account:8842"}, "repair")
admin.Resume(ctx, cronoq.PauseTarget{})
```

```ts
await admin.pause({}, "billing API deploy until 14:00");
await admin.pause({ class: "low" }, "backfill off");
await admin.pause({ orderingKey: "account:8842" }, "repair");
await admin.resume();
```

Every pause carries a **reason**. It is shown wherever the pause is: in the snapshot, in
`cronoq snapshot` and `cronoq top`, in `cronoq ordering get`, and to workers. Write it for the
person who finds the queue standing still at three in the morning. Over the network a reason
is required: not empty, and one line.

Leaving out the class and the ordering key names the **pool**. An ordering key that is given
but empty is refused rather than read as "the pool", so a script with an unset variable does
not pause or resume more than it meant to.

## What a pause does and does not do

| | While paused |
|---|---|
| Tasks that wait | Stay queued. They keep their place and their age, so they run in the order, and with the priority, they would have had. |
| New submissions | Accepted, and wait with the rest. |
| [Bounds](backpressure.md) | Held tasks count toward them: a paused pool fills up, and then applies its overflow policy. |
| Attempts that are running | Not interrupted. They finish, fail or time out as usual. |
| [Retries](reliability.md) | A task that fails and is retried waits like any other task once its backoff has passed. |
| [Dependencies](dependencies.md) | A task that is held does not finish, so what depends on it waits. |
| [Fixed starts](fixed-starts.md) and [schedules](recurring.md) | **Not held.** A start time is a promise of its own: a task with one, and so every occurrence of a schedule, starts when it is due. Remove the schedule, or cancel the task, to stop it. |
| [Autoscaling](autoscaling.md) | Work held by a pause of its pool or class is no reason to add workers: it is left out of the backlog and of the arrival rate. |
| [Resources](resources.md) and [labels](labels.md) | A held task does not keep a resource, or a labelled worker, from the tasks behind it. It asks again when it is resumed. |
| [Priority adherence](metrics.md) | A held task is not counted as waiting ahead of the work that runs. |
| Cancel, update, replace | Work as usual on held tasks. |

Pausing does not cancel anything, and it is not a way to stop an attempt that is running.

A pause of a pool and a pause of one of its classes are separate: resuming the pool leaves a
paused class paused. Pausing what is already paused is not an error; the new reason replaces
the old one. Resuming what is not paused is not an error either. Both calls report whether
they changed anything.

## Classes and ordering keys

A class pause holds the tasks that **run at** that class. For most tasks that is their own
priority. The task at the front of an [ordering key](ordering-keys.md) runs at the highest
class among its key's tasks: pausing `high` holds a low task that is lifted to high by a high
task behind it, and pausing `low` does not.

Pausing an ordering key holds that key and nothing else. A key can be paused before it has
any tasks, and it stays paused while it has none. Resuming an ordering key also resumes a key
that a failed task [halted](ordering-keys.md#when-a-task-fails): a pause is something an
operator did, a halt is something a failure did, and one call ends both.

## Seeing what is paused

The snapshot shows, per pool: the reason the pool is paused, the classes paused on their own
with their reasons, how many ready tasks a pause is holding, and how many ordering keys are
paused.

```sh
cronoq -o json snapshot      # "paused", "paused_classes", "paused_tasks" per pool
cronoq ordering paused       # the paused ordering keys of a pool, with reasons
curl -s $CRONOQ/v1/pools/default/ordering?paused=true -H "$AUTH"
```

| Metric | Meaning |
|---|---|
| `cronoq_pool_paused` | 1 while the whole pool is paused. |
| `cronoq_pool_paused_classes` | Classes of the pool that are paused on their own. |
| `cronoq_pool_paused_tasks` | Ready tasks held by a pause. |
| `cronoq_pool_ordering_paused` | Paused ordering keys. |

A worker that connects to a pool that is paused as a whole is told so, with the reason, when
it registers; the worker libraries log it as a warning. The worker stays connected and gets
tasks again when the pool is resumed. A worker that was already connected when the pool was
paused is not told: it simply gets no tasks.

## Durability

With a [durable store](durability.md), a pause is stored when it is set and removed when it
is resumed, and the call returns after that. A restarted server, or a new leader after a
failover, starts with every pause in place: nothing that was held slips out during recovery.

## Who may pause

Over the network, pausing and resuming need the `admin` role, for a pool the credentials
allow. Credentials pinned to a [tenant](fairness.md) pause and resume their own ordering keys
only, not a pool or a class, since those hold other tenants' work.

## What a pause is not

- **Not per tenant.** There is no call that holds one tenant's tasks in a pool; an ordering
  key per tenant-owned subject is the closest tool.
- **No timer.** A pause lasts until someone resumes it. Put the expected end in the reason.
- **Not an abort.** Running attempts are left alone; cancel a task to stop its attempt.
