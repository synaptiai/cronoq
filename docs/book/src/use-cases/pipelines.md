# Pipelines and DAGs

## The problem

Multi-step work (extract → transform → load, or crawl → embed → index) is often chained by
hand: each step enqueues the next one when it finishes. Failures leave half-finished pipelines,
downstream steps run on missing inputs, and there is nowhere to hang a "notify on failure"
step.

## The setup

Give tasks keys and declare edges. A batch is admitted atomically and may reference keys in any
order:

```rust,ignore
use cronoq::{Dep, Task};

let mut tasks = vec![Task::new(Step::Extract).key("extract")];
for shard in 0..4 {
    tasks.push(Task::new(Step::Transform(shard)).key(format!("t{shard}")).after("extract"));
}
let mut load = Task::new(Step::Load).key("load");
for shard in 0..4 {
    load = load.after(format!("t{shard}"));            // fan-in
}
tasks.push(load);
tasks.push(Task::new(Step::Alert).dep(Dep::on_failure("load")));   // runs only if load fails
tasks.push(Task::new(Step::Cleanup).dep(Dep::on_complete("load"))); // runs either way

let handles = sched.submit_batch(tasks).await?;
```

```python
await sched.submit_batch([
    cronoq.Task("extract", key="extract"),
    *[cronoq.Task(("transform", i), key=f"t{i}", after="extract") for i in range(4)],
    cronoq.Task("load", key="load", after=[f"t{i}" for i in range(4)]),
    cronoq.Task("alert", deps=[cronoq.Dep("load", on="failure")]),
    cronoq.Task("cleanup", deps=[cronoq.Dep("load", on="complete")]),
])
```

Runnable: `cargo run -p cronoq --example dag_pipeline`.

## Edge semantics

| Edge | The child runs when the parent... | Otherwise the child becomes |
|---|---|---|
| `after` / `Dep::on_success` | succeeded | `Skipped` |
| `Dep::on_failure` | failed or was dead-lettered | `Skipped` |
| `Dep::on_complete` | reached any terminal status | (always runs) |

`Skipped` propagates: everything downstream of a skipped task is skipped too, except children
attached with `on_complete`. A task that waits on dependencies has status `Blocked`; it enters the queue only
when every edge is satisfied, so blocked tasks never occupy workers.

Dependencies can reference **task ids** or **keys**. A key that has never been seen is rejected
by default; configure `UnknownDependency::Await { timeout }` on the pool to let a child wait
for a task that will be submitted later (useful when producers are independent).

## What you get

- **Correctness by construction.** Children never start before their parents finish. This is
  checked continuously by cronoq's randomized simulation tests.
- **Failure handling as a first-class step** with `on_failure` and `on_complete` edges.
- **Atomic submission.** A batch with a cycle, an unknown dependency, a reservation conflict or
  a full queue is rejected as a whole, never half-admitted.
- **Idempotent pipelines.** Re-submitting a batch whose keys already exist returns the existing
  tasks (`duplicate = true`) instead of running them twice. Keys stay reserved while the task is
  live and for the retention window after it finishes (1 hour by default).
- **Late dependents still work.** A child submitted after its parent finished is resolved from
  the retained terminal status.

## Limits

cronoq's DAGs are a scheduling primitive, not a workflow engine: there are no data passing
between steps (pass references in payloads), no conditional branching beyond edge conditions,
and no long-term workflow history. For those, pair cronoq with a workflow engine.
