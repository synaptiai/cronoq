# Latest-wins updates

## The problem

Some work is only worth doing for its newest value: syncing a document's title, refreshing a
cache entry, rebuilding a search index, recomputing a balance. A user types, and five updates
of the same title arrive while the first one still waits in the queue.

A plain queue runs all five. Four of them write a value that is already out of date. They use
capacity and delay other work. Dropping every update after the first is no fix: the queue
then runs only the oldest value. Cancelling the old task and submitting a new one is two
calls, so there is a moment at which no task exists for the title, and a cancel aborts an
attempt that is already running.

## The setup

Give the work a [key](../concepts/dependencies.md#keys) that names the thing it updates, and
submit it with `on_key_conflict = replace_queued`
([Replacing a task by key](../concepts/replacing.md)). The newest submission of the key wins,
in one atomic call.

```python
h = await sched.submit(title, key="title:doc-42", on_key_conflict="replace_queued")
print(h.outcome)  # "created", "replaced" or "pending"
```

```rust,ignore
let h = sched.submit(Task::new(title).key("title:doc-42").replace_queued()).await?;
```

```sh
curl -s $CRONOQ/v1/tasks -H "$AUTH" -H 'content-type: application/json' -d '{
  "key": "title:doc-42", "payload": {"title": "Q3 plan (final)"},
  "on_key_conflict": "replace_queued"}'
# {"duplicate":true,"id":"17","outcome":"replaced"}
```

The command-line tool has `cronoq submit ... --key title:doc-42 --replace`.

## What happens to each submission

The answer depends on the task that holds the key when the submission arrives. The response
names the case in `outcome`.

| The task holding the key | Outcome | What happens |
|---|---|---|
| None, or one that has finished | `created` | A new task is created. |
| Has not started any attempt | `replaced` | The task takes the new payload. It keeps its id, its place in line and its dependents. |
| Has started an attempt (running, or waiting for a retry) | `pending` | The running task is not changed and not aborted. The new submission is stored. It runs afterwards as a new task with the same key, the **successor**. |

A task that has started keeps its payload for every attempt. This is the **freeze rule**: a
retry never runs with a payload other than the one the first attempt had.

While a task runs, only the newest stored submission is kept. Ten updates that arrive during
one run lead to one successor, not ten.

## What you get

- **Stale values are not run.** Each replaced submission is counted in `superseded` in the
  snapshot and in the metric `cronoq_tasks_superseded_total`. Compare it with `submitted` to
  see how much work was saved.
- **The newest value is never lost.** A submission that arrives while the task runs is stored
  and runs afterwards.
- **The key keeps its place in line.** By default a replaced task keeps the place and the
  waiting time of the first submission, so a thing that is updated often is not pushed to the
  back each time. Submit with `renew_position` when each update should queue like a new
  request.
- **Nothing is aborted.** A running attempt finishes with the payload it started with.
- **Durable.** With a [durable store](../concepts/durability.md), replacements and stored
  submissions are written before the call returns.

## Trade-offs

- **Intermediate values are dropped on purpose.** Do not use this for work where every
  submission must run, such as ledger entries. Use an
  [ordering key](ordered-work.md) for that.
- **A replacement must describe the same work.** It must name the same pool, resources,
  worker labels, tenant, dependencies and [ordering key](../concepts/ordering-keys.md) as the
  task. Otherwise it is refused with *replace mismatch* (HTTP 409), and nothing changes.
  Payload, kind, priority, deadline, tags, timeout and retry policy may differ.
- **`pending` returns the running task.** The successor does not exist yet. The response
  carries the id of the task that runs now. Look the key up after that task ends to find the
  successor.
- **A cancel drops the stored submission.** If the running task is cancelled, or shed under
  overload, there is no successor.
- **Not in batches, not in schedules, not with a fixed start.** These submissions are refused.
- **A stored submission holds a place in the queue bounds.** If the queue is full when it
  arrives, it is refused (HTTP 429). See
  [Queue bounds](../concepts/replacing.md#queue-bounds).
- **Older servers do not know the option.** Over gRPC a 0.2.2 server ignores it: if the key
  was free, the task is created; if the key was in use, the submission is dropped. The Python,
  Go and TypeScript clients fail with `unimplemented` in both cases, and the command-line tool
  exits with an error. Over HTTP a 0.2.2 server refuses the unknown field (status 400) and
  admits nothing. Upgrade every server first ([Upgrading](../guides/upgrading.md)).
