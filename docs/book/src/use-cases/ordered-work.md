# Ordered work per entity

## The problem

Events arrive for many entities: accounts, documents, conversations, devices. For one entity
the order matters. A debit must not run before the credit that pays for it. The second edit of
a document must not be applied before the first. For different entities the order does not
matter, and they should use every worker there is.

A plain queue with several workers breaks the order: two workers take two events of one
account and run them at the same time. The usual fixes cost capacity. One worker for
everything is slow. One queue per entity needs a worker per queue. A fixed set of partitions
keeps the order, but one slow entity delays every other entity in its partition.

## The setup

Give each task an [ordering key](../concepts/ordering-keys.md) that names its entity. Tasks of
one pool that share an ordering key run one at a time, in the order they were submitted. Tasks
with different keys, and tasks without a key, do not wait for each other.

```python
async with cronoq.Scheduler([cronoq.Pool("default", apply_entry, min=1, max=8)]) as sched:
    await sched.submit(credit, ordering_key="account:8842")
    await sched.submit(debit, ordering_key="account:8842")    # runs after the credit
    await sched.submit(other, ordering_key="account:17")      # runs in parallel
```

```rust,ignore
sched.enqueue(Task::new(entry).ordering_key("account:8842")).await?;
```

Over HTTP, for producers in any language:

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "ordering_key": "account:8842", "payload": {"op": "debit", "amount": 40}}'
```

Workers and executors need no change. The scheduler decides which task is offered; a worker
never sees two tasks of one key at the same time.

Runnable: `cargo run -p cronoq --example ordering_keys`.

## Stopping an entity after a failure

By default a failed task does not stop its key: the next task of the key runs. For some work
the next step makes no sense after a failure. Submit such a task with **halt on failure**:

```python
await sched.submit(step, ordering_key="import:42", halt_on_failure=True)
```

If that task ends as failed or dead-lettered, its key is **halted**. The tasks behind it stay
queued, in order. New tasks for the key are accepted and wait too. Other keys keep running.
After you have looked into the failure, resume the key:

```sh
cronoq ordering halted                 # the halted keys of a pool
cronoq ordering get import:42          # tasks, the one at the front, floor, halted
cronoq resume --ordering-key import:42
```

Alert on the metric `cronoq_pool_ordering_halted`: a halted key waits for a person.

## How ordering keys work with priorities

| Question | Answer |
|---|---|
| Does priority change the order inside a key? | No. Inside a key the order is submission order. A critical task submitted after a low task of the same key runs after it. |
| Is a critical task then stuck behind unrelated work? | No. The task at the front of a key runs at the highest class among the key's unfinished tasks. So the low task in front is dispatched like a critical task, and the critical task follows it. |
| Does this change a task's own priority? | No. Only the class the front task runs at is raised. |

See [Reprioritising live work](reprioritising.md) for how to move one entity ahead.

## What you get

- **Order per entity, parallelism across entities.** With eight workers and a thousand keys,
  up to eight keys are served at any moment, one task of each.
- **A slow entity does not block the others.** A key whose task is slow or stuck holds one
  worker at most.
- **No partitions to size.** A key exists while it has tasks. There is no partition count to
  choose and no rebalancing.
- **Order survives failover.** With a [durable store](../concepts/durability.md), a new leader
  rebuilds every key from the stored submission order. A halted key is still halted.
- **Visible state.** Each pool reports the number of keys with tasks, of tasks that wait for
  an older task of their key, and of halted keys
  ([metrics](../concepts/ordering-keys.md#metrics)).

## Trade-offs

- **One task of a key at a time.** A key is never faster than one worker. If one entity
  produces more work than one worker can do, an ordering key is the wrong tool for it.
- **The front task holds the key until it has finished.** While it runs, waits for a retry,
  waits for a dependency or waits for a resource, the tasks behind it wait too. Use a
  `timeout` so that a stuck attempt ends.
- **Execution is still at least once.** An attempt can run again after a worker failure. The
  order of tasks is kept, but each handler must be idempotent
  ([Retries, timeouts and leases](../concepts/reliability.md)).
- **No start times.** An ordering key cannot be combined with a
  [fixed start](../concepts/fixed-starts.md) or used in a
  [recurring schedule](../concepts/recurring.md). Such a submission is refused.
- **A key belongs to its pool.** The same key in two pools is two keys.
- **It does not deduplicate.** Two submissions with one ordering key are two tasks. Add a
  [key](../concepts/dependencies.md#keys) to make a submission safe to retry
  ([Idempotent producers](idempotent-producers.md)).
- **Older servers do not know the key.** Over gRPC a 0.2.2 server ignores it. The Python, Go
  and TypeScript clients detect this and fail with `unimplemented`, and the command-line tool
  exits with an error, but the task has then been admitted without an ordering key. Over HTTP
  a 0.2.2 server refuses the unknown field (status 400) and admits nothing. Upgrade every
  server first ([Upgrading](../guides/upgrading.md)).
