# Ordering keys

Some work must not overlap, and must not overtake, for one subject: the edits of one document,
the ledger entries of one account, the commands sent to one device. Across subjects it should
still run in parallel on every worker there is.

An **ordering key** says which subject a task belongs to. Tasks of one pool that share an
ordering key run **one at a time, in the order they were submitted**, however many workers are
free. Tasks with different keys, and tasks without one, do not wait for each other.

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "ordering_key": "account:8842", "payload": {"op": "debit", "amount": 40}}'
```

```rust
sched.enqueue(Task::new(entry).ordering_key("account:8842")).await?;
```

```python
await sched.submit(entry, ordering_key="account:8842")
```

```go
c.Submit(ctx, cronoq.Task{Payload: entry, OrderingKey: "account:8842"})
```

```ts
await client.submit({ payload: entry, orderingKey: "account:8842" });
```

With eight workers and a thousand keys, up to eight keys are served at any moment, one task of
each. A key whose task is slow, or stuck, holds one worker at most; the other keys are not
slowed by it.

## The order is submission order, not priority

Inside a key, tasks run in the order the scheduler accepted them. Priority does not change
that order: a critical task submitted after a low one runs after it. That is what makes the
key useful, and it is the first thing to know before adding one to work that also uses
priorities.

Priority still decides how the key competes with **other** work. The task at the front of a
key runs at the highest class among the key's tasks that have not finished, so a critical task
is not stuck behind unrelated normal work just because a low task of its key is ahead of it:

| Queue | Without the lift | With it (what cronoq does) |
|---|---|---|
| `A1` low (key A), `N` normal, `A2` critical (key A) | `N`, `A1`, `A2` | `A1`, `A2`, `N` |

Only the class the front task *runs at* is raised. No task's own priority changes: it is what
`Get` shows, what [bounds and load shedding](backpressure.md) count, and what the task's
[aging](priorities.md) starts from. The task's waiting time is its own, counted from its own
submission.

A batch keeps its order: tasks of one key run in the order they appear in the batch.

## What holds a key

The task at the front holds the key until it has **finished**. While it

- runs, or waits for a [retry](reliability.md);
- waits for a [dependency](dependencies.md);
- waits for a [resource](resources.md) or for a worker with its [labels](labels.md),

the tasks behind it wait too. They are `ready`, and they are counted in the pool's
`ordering.waiting` figure, but no worker is offered them, and the autoscaler does not add
workers for them: a worker could not run them.

A task that is cancelled, skipped or shed leaves its key, and the task behind it moves up.

A dependency that can never be met is refused when it is submitted, as a cycle: a task may
not depend, directly or through other tasks, on a task that is behind it in its own key.

## When a task fails

By default a failed task does not stop its key: the next task runs. For work where the next
step makes no sense after a failure, submit with **halt on failure**:

```python
await sched.submit(step, ordering_key="import:42", halt_on_failure=True)
```

If that task ends as failed (or dead-lettered, once its retries are used up), the key is
**halted**. The tasks behind it stay queued, in place and in order. New submissions for the
key are still accepted and wait behind them. Nothing of the key runs until someone resumes it.

The halt belongs to the key, not to the tasks that happened to be waiting: it stays while the
key has no tasks at all, so a step submitted after the failure waits as well. Cancelling what
waits does not end it. A halted key is listed, and counted in the `ordering.halted` figure,
until it is resumed.

The policy belongs to each task, so one key can hold steps that must not be skipped over and
steps that may be. A task that is cancelled, skipped or shed never halts its key.

```sh
cronoq ordering halted                 # the halted keys of a pool
cronoq ordering get import:42          # tasks, the one at the front, floor, halted
cronoq resume --ordering-key import:42
```

| | Rust | Python (in process) | HTTP |
|---|---|---|---|
| Show a key | `sched.ordering(pool, key)` | `sched.ordering(key)` | `GET /v1/pools/{pool}/ordering/{key}` |
| List halted keys | `sched.halted_orderings(pool, limit)` | `sched.halted_orderings()` | `GET /v1/pools/{pool}/ordering?halted=true` |
| Resume | `sched.resume_ordering(pool, key)` | `sched.resume_ordering(key)` | `POST /v1/pools/{pool}/ordering/{key}/resume` |

Resuming a key that is not halted is not an error; the call reports whether it was.

## A floor for a key

A key can be given a **class floor**: its front task never runs below that class, whatever the
classes of its tasks. Use it to move one subject ahead (a customer waiting on the phone) without
resubmitting or updating its tasks.

```sh
cronoq ordering floor account:8842 high             # until the key has no tasks left
cronoq ordering floor account:8842 high --persist   # kept while the key is empty too
cronoq ordering floor account:8842 --clear
```

A floor never lowers a key, and it changes no task's own priority. It goes away when the key
has no tasks left, unless it was set to persist. The number of keys that keep a floor while
they are empty is limited (`limits.max_ordering_settings`, 100,000 by default) so that
forgotten settings cannot grow without bound; beyond the limit a new kept floor is refused. A
submission is never refused because of it.

## With other features

| Feature | With an ordering key |
|---|---|
| [Deduplication key](dependencies.md#keys) | Independent. The ordering key never deduplicates: two submissions with the same ordering key are two tasks. A task may have both. |
| [Conflict detection](key-conflicts.md) | With `on_key_conflict = reject`, a key held by an unfinished task of another ordering key (or of none) is a conflict, whatever the fingerprints: the caller would otherwise believe its submission is in a key it is not in. The ordering key is not part of the fingerprint. |
| [Replace by key](replacing.md) | The replacement keeps the task's place in its ordering key (or goes to the back with `renew_position`). It must name the same ordering key. A successor takes the place of the task it follows. |
| [Updating](updating.md) | Changing a waiting task's priority can raise or lower the class its key's front task runs at. |
| [Bounds and shedding](backpressure.md) | Every task of a key counts toward the bounds, with its own class. Only a task at the front of its key can be shed: one that waits behind another is never picked. |
| [Fair sharing](fairness.md) | The front task is charged to its own tenant, in the class it runs at. |
| [Fixed starts](fixed-starts.md), [schedules](recurring.md) | Not allowed together with an ordering key: a start time and a place in line cannot both be promised. The submission is refused. |
| [Pools](autoscaling.md) | A key belongs to its pool: the same key in two pools is two keys. |
| [Sharding](sharding.md) | A pool lives on one shard, and so do its keys. |

## Older servers

A server from before ordering keys ignores the key and runs the task unordered. The clients
and the command-line tool detect this from the response and fail with `unimplemented`; the
task has then been admitted without a key. Upgrade every server before relying on ordering
keys.

## Durability

With a [durable store](durability.md), each task's ordering key and failure policy are stored
with it, and a key's floor and halt are stored when they are set. A new leader rebuilds every
key from the stored submission order: the same task is at the front, a halted key is still
halted, and a kept floor is still there.

## Roles and tenants

Over the network, reading a key needs the `metrics` role and changing one (floor, resume) the
`admin` role, both for a pool the credentials allow.

A key is shared by everyone who may submit to its pool: any of them can add a task to it, and
so make the others' tasks wait. Credentials pinned to a [tenant](fairness.md) are kept apart.
They have their own ordering keys: key `k` submitted with credentials pinned to tenant `acme`
is stored as `acme:k`, and is a different key from `k` and from every other tenant's. Such
credentials submit to, read, list and change the keys of their own tenant only, under their
own names; they cannot join, block or halt anyone else's. Credentials that are not pinned see
and name those keys with the prefix (`cronoq ordering get acme:k`). In the prefix, `%` and `:`
in a tenant's name are written `%25` and `%3A`.

## Metrics

Per pool, in the snapshot and as Prometheus gauges:

| Figure | Metric | Meaning |
|---|---|---|
| `ordering.keys` | `cronoq_pool_ordering_keys` | Ordering keys that have tasks. |
| `ordering.waiting` | `cronoq_pool_ordering_waiting` | Ready tasks that wait for an older task of their key, or for a halted key. |
| `ordering.halted` | `cronoq_pool_ordering_halted` | Halted keys. Alert on this one. |

The pool's [priority adherence](metrics.md) is measured against the class a task runs at, so a
front task lifted by its key does not count as an inversion.
