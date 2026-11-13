# Replacing a task by key (latest wins)

Some work is only worth doing for its newest value: syncing a document's title, refreshing a
cache entry, rebuilding an index, recomputing a balance. When five updates of the same thing
arrive while the first still waits, running all five wastes capacity and pushes other work out;
running only the first leaves stale data.

A submission with `on_key_conflict = replace_queued` and a [key](dependencies.md#keys) lets the
newest submission of that key win. It is one atomic call: there is no moment at which the key
names no task, and nothing running is aborted, both of which cancel-then-submit cannot promise.

```rust
use cronoq::{SubmitOutcome, Task};

let h = sched.submit(Task::new(title).key("title:doc-42").replace_queued()).await?;
match h.outcome() {
    SubmitOutcome::Created => {}  // no task held the key
    SubmitOutcome::Replaced => {} // the waiting task now carries this payload
    SubmitOutcome::Pending => {}  // a task is running: this payload runs after it
    SubmitOutcome::Existing => unreachable!("only without replace_queued"),
}
```

```python
h = await sched.submit(title, key="title:doc-42", on_key_conflict="replace_queued")
print(h.outcome)  # "created", "replaced" or "pending"
```

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "key": "title:doc-42", "payload": {"title": "Q3 plan (final)"},
  "on_key_conflict": "replace_queued"}'
# {"duplicate":true,"id":"17","outcome":"replaced"}
```

## What happens

It depends on the task that holds the key when the submission arrives.

| The task holding the key | Outcome | What happens |
|---|---|---|
| None, or one that has finished | `created` | A new task is created, as for any submission. The key now names it. |
| Has not started any attempt (waiting to run, or waiting for a dependency) | `replaced` | The task is replaced **in place**. It keeps its id, its dependents, its attempt count and its place in line, and takes the new submission's payload, kind, priority, deadline, tags, timeout, retry policy and duration hint. Callers already waiting on it get the result of the new payload. |
| Has started an attempt (running, or waiting for a retry) | `pending` | **The payload is frozen.** The task finishes with the payload it started with. The new submission is stored, replacing any stored before, and runs afterwards as a new task with the same key: the **successor**. |

Without `replace_queued` nothing changes: a submission whose key is in use returns the existing
task (`existing`) and its payload is dropped, or, with `reject`, is
[reported as a conflict](key-conflicts.md) when it differs from that task.

### The freeze rule

Every attempt of one task sees the same payload. A task that failed its first attempt with one
payload is never retried with another, so its attempt history, its retry budget and its result
all describe one piece of work. That is why a started task is followed, not changed.

### The successor

When a task with a stored submission ends as **succeeded**, **failed** or **dead-lettered**, the
successor is created in the same step: with a durable store, the end of the task and the new
task are committed together, so a crash leaves either both or neither.

- It has the same key and a new id. Lookups, waits and cancels by key name it from then on.
- It does not wait for the dependencies again: the task it follows already passed them. It
  keeps them in its description, so the next replacement of the key names the same ones.
- It carries the tag `cronoq.follows` with the id of the task it follows, which executors and
  workers can read.
- If the task is **cancelled**, or shed under overload, the stored submission is dropped with
  it and there is no successor: a cancel means "stop working on this key".
- A task that depends on the key while a submission is stored waits for the task running at
  that moment, not for the successor. Depend on the key again later, or on an id, to wait for
  a particular one.
- Only the newest stored submission runs. Each one that is dropped is counted in
  `superseded`.

The response to a `pending` submission carries the id of the *running* task, and with `wait`
its result: the successor does not exist yet. Look the key up after that task ends to find it,
or watch for the successor [event](#events).

## Place in line

| | Default | With `renew_position` |
|---|---|---|
| Replaced in place | Keeps the task's place, and the waiting time it has built up for [aging](priorities.md#aging). | Takes the place of the new submission; waiting time starts now. |
| Successor | Takes the place the ended task held, with that task's original submission time. Work submitted while the task ran does not get ahead of it. | Takes the place of the stored submission. Only the *last* replacing submission decides. |

Keeping the place suits state sync: the key has been waiting since its first update. Renewing
suits work where each update is a fresh request that should queue like one. In a pool that
[shares capacity between tenants](fairness.md), place among tenants follows tenant share, as
for any task: a renewed task goes behind its own tenant's waiting work, and a successor joins
its tenant's work when it is created.

## What a replacement may differ in

A replacing submission describes the same work in the same place, so it must match the task in
everything that decides where and when it may run. Otherwise it is refused with *replace
mismatch* (`FAILED_PRECONDITION` / 409) and nothing changes.

| Must be the same | May differ (the new value applies) |
|---|---|
| Pool | Payload and content type |
| Resources | Kind |
| Worker labels | Priority and deadline (as for [an update](updating.md)) |
| Tenant | Tags |
| Dependencies, named the same way (a key stays a key, an id an id) | Timeout, retry policy, duration hint |

Neither the task nor the replacement may have a [fixed start](fixed-starts.md): a fixed start
holds a capacity reservation made at admission. `replace_queued` needs a key, and is refused in
batch submissions and recurring schedules, which admit tasks as written. The clients refuse it
there too, and report a server too old to know the option (`unimplemented`) instead of letting
the submission be dropped silently.

## Queue bounds

A replacement in place needs no room: the task already has its place. A stored submission
holds one place in the [queue bounds](backpressure.md) of its pool and class from the moment it
is stored until its successor takes that place, so the successor is never refused later. If
the queue is full when the submission arrives, it is refused then (`RESOURCE_EXHAUSTED` /
429), and whatever was stored before stays. A newer stored submission reuses the place of the
one it replaces; if it is of another class, that class's bound applies to it.

A stored submission never evicts other work to make room, whatever the pool's overflow policy:
it could evict the very task it waits for. With the `wait` policy an in-process caller waits
for room like any submitter, while a replacement in place goes straight through.

## Events

With `emit_transitions` (`all_transitions` on `Tasks/Watch`), two kinds of
[event](tasks.md#events) report replacements. Neither is a task status.

| Kind | On | Meaning |
|---|---|---|
| `Replaced` | The existing task, with its unchanged status | A submission replaced it in place, or was stored to run after it. |
| `Successor` | The new task | It was created from a stored submission; `message` names the task it follows, and `key` is the shared key. |

Subscribers that only receive terminal statuses see neither: they see the old task end and,
later, the successor end.

## Durability

Replacements and stored submissions are written to the store before the call returns, and
survive a restart or failover: a replaced task comes back with its new payload, and a stored
submission still follows its task. Both stores gain a table for stored submissions through a
schema migration that runs when the server or library opens the database.

After a failover a task whose attempt was running is retried with the payload it had, like any
interrupted task; the stored submission still waits behind it. If that lost attempt was the
task's last, the task ends as dead-lettered during recovery and the successor is created then.

## Who may replace

Over the server, replacing needs the `client` role, like any submission. A replacement must
name the task's tenant, so credentials [pinned to a tenant](fairness.md#tenants-and-credentials)
cannot replace another tenant's task: the call fails with *replace mismatch*. Credentials
limited to certain pools cannot use, or learn about, a key held by a task in another pool
(`ALREADY_EXISTS`).

## Measuring it

`superseded` in the snapshot totals, and `cronoq_tasks_superseded_total` in the server's
metrics, count submissions that were dropped without running because a newer one replaced
them: each replacement in place, and each stored submission overwritten by a newer one. A high
rate against `submitted` shows how much work latest-wins is saving.
