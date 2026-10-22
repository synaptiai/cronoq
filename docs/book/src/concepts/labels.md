# Worker labels

One pool can hold workers that differ: some have a GPU, some run in another region. **Labels**
describe them, and a task can require labels to run only on matching workers.

```rust,ignore
// The task needs an A100.
sched.submit(Task::new(payload).pool("inference").label("gpu", "a100")).await?;
```

```python
await sched.submit(payload, pool="inference", labels={"gpu": "a100"})
```

## Worker groups

The workers of a pool that carry exactly the same labels form a **group**. The scheduler keeps
the capacity of each group:

- Remote workers send their labels when they register (`Register.labels`; `WorkerOptions::label`
  in Rust, `Worker(labels=...)` in Python). The server adds each worker's slots to its group.
- Embedded schedulers report groups with `Scheduler::set_group_capacity(pool, labels, n)`
  (Python `set_group_capacity`). `set_capacity` sets the unlabeled group.
- A pool's capacity is the sum over its groups; snapshots list each group's capacity and busy
  slots (`PoolSnapshot.groups`, `/v1/snapshot` → `pools[].groups`).

## How tasks are placed

| Task | Runs in |
|---|---|
| No labels | Any group with a free slot, preferring the least specialized one (fewest labels, then the most free slots), so workers with rare labels stay available for tasks that need them. |
| With labels | Only a group whose labels include every required `name = value` pair. A group may carry more labels than the task requires. |

A task whose required labels no free group has **waits without blocking the pool**: other
tasks keep running in the free slots, and the waiting task starts as soon as a matching slot
frees or a matching worker connects, in submission order among the tasks waiting for that
group. A task that requires labels no worker has waits until one connects (or until it is
cancelled; deadlines raise urgency but never cancel).

Each attempt is assigned to a group: remote leases go only to a worker of that group, and
embedded executors see it as `ctx.group`.

## Limits

- Fixed-start reservations are counted per pool, not per group: a fixed-start task that
  requires labels starts when a matching slot is free, so give it enough tolerance if matching
  workers are scarce.
- Autoscaling targets a pool's total capacity. Scale labeled worker fleets separately, for
  example one deployment per label set.

## Security

Workers declare their own labels, so a worker credential could claim labels it should not
have and receive tasks meant for other machines. Restrict what a credential may claim:

```toml
[[auth.tokens]]
token_env = "CRONOQ_GPU_WORKER_TOKEN"
role = "worker"
pools = ["inference"]
labels = ["gpu=a100", "zone=eu"]   # a worker with this token may claim only these
```

A worker that claims a label its credential does not allow is refused with
`PERMISSION_DENIED`.
