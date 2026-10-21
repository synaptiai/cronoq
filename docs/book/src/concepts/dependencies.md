# Dependencies and keys

## Keys

A **key** is an optional unique name for a task.

- **Deduplication**: submitting a task whose key belongs to a live task, or to a terminal task
  still in the retention window, does not create a new task. It returns the existing id with
  `duplicate = true`. This makes producers safe to retry.
- **References**: dependencies, cancellation, status lookups and result waits accept a key
  wherever they accept an id.

Keys stay reserved while the task is live and for the retention window after it ends (1 hour by
default; the Postgres store keeps finished rows for 7 days by default).

## Edges

A dependency is a reference to a parent (id or key) plus a condition:

| Condition | Rust | Python | Satisfied when the parent... |
|---|---|---|---|
| on success (default) | `.after(x)` / `Dep::on_success(x)` | `after=[x]` / `Dep(x)` | succeeded |
| on failure | `Dep::on_failure(x)` | `Dep(x, on="failure")` | failed or was dead-lettered |
| on complete | `Dep::on_complete(x)` | `Dep(x, on="complete")` | reached any terminal status |

A task with unmet dependencies is `Blocked` and does not enter any queue. When a parent ends:

- every satisfied edge decrements the child's count of unmet dependencies; at zero the child
  becomes eligible;
- an edge that can no longer be satisfied makes the child `Skipped`, and this propagates to its
  own dependents.

If the parent had already finished when the child was submitted, the edge is resolved
immediately from the retained status.

## Unknown parents

A dependency on a key that does not exist yet is handled per pool:

| Setting | Effect |
|---|---|
| `UnknownDependency::Reject` (default) | Submission fails with `UnknownDependency`. |
| `UnknownDependency::Await { timeout }` | The child waits for a task with that key; if none appears within `timeout`, it is `Skipped`. |

A dependency on an **id** that does not exist is always rejected.

## Cycles

Cycles are rejected with `Cycle`: a task depending on its own key, cycles within a batch, and
cycles closed through a task that is awaiting a key the new submission defines.

## Batches

`submit_batch` admits several tasks atomically. Dependencies may reference keys defined anywhere
in the batch, in any order. If any task is invalid, would close a cycle, conflicts with a
reservation or exceeds a bound, nothing is admitted and the error names the offending index.

## Complexity

Dependencies are tracked with in-degree counters and reverse edges, so a parent finishing costs
O(number of its children). Completed parents are never scanned.
