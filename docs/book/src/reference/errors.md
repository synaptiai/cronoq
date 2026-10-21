# Errors and status codes

## Submission errors

| `SubmitError` | Python `code` | gRPC | HTTP | Cause |
|---|---|---|---|---|
| `UnknownPool(name)` | `unknown_pool` | `NOT_FOUND` | 404 | No pool with that name. |
| `UnknownResource(name)` | `unknown_resource` | `NOT_FOUND` | 404 | No resource with that name. |
| `ResourceExceedsCapacity(name)` | `resource_exceeds_capacity` | `INVALID_ARGUMENT` | 400 | Requested units exceed the resource's permits/burst. |
| `ReservationConflict` | `reservation_conflict` | `FAILED_PRECONDITION` | 409 | Fixed-start reservations would exceed the pool's reservable capacity. |
| `Cycle` | `cycle` | `INVALID_ARGUMENT` | 400 | The dependencies form a cycle. |
| `UnknownDependency(ref)` | `unknown_dependency` | `NOT_FOUND` | 404 | Dependency on an unknown id, or unknown key with `UnknownDependency::Reject`. |
| `Full` | `full` | `RESOURCE_EXHAUSTED` | 429 | Bounds reached with the `Wait` policy (library callers wait instead). |
| `Overloaded` | `overloaded` | `RESOURCE_EXHAUSTED` | 429 | Bounds reached with `Reject`, or shedding could not make room. |
| `DuplicateKeyInBatch(key)` | `duplicate_key_in_batch` | `INVALID_ARGUMENT` | 400 | A batch defines the same new key twice. |
| `Invalid(reason)` | `invalid` | `INVALID_ARGUMENT` | 400 | E.g. zero resource units, `max_attempts = 0`. |

## Task outcomes

| Status | Rust `TaskFailure` | Python | Meaning |
|---|---|---|---|
| `succeeded` | — (`Ok`) | value | Output returned. |
| `failed` | `Ended { status: Failed, .. }` | `TaskFailed(status="failed")` | Permanent error or fixed start missed with `Fail`. |
| `dead_lettered` | `Ended { status: DeadLettered, .. }` | `"dead_lettered"` | Retries exhausted. |
| `cancelled` | `Ended { status: Cancelled, .. }` | `"cancelled"` | Cancelled. |
| `skipped` | `Ended { status: Skipped, .. }` | `"skipped"` | Dependency edge unmet, or await timed out, or recurring overlap. |
| `missed` | `Ended { status: Missed, .. }` | `"missed"` | Fixed start missed with `Skip`. |
| `shed` | `Ended { status: Shed, .. }` | `"shed"` | Evicted under overload. |
| — | `Shutdown` | `"shutdown"` | The scheduler stopped before the task finished. |

## Scheduler errors (Rust `Error`)

| Variant | Meaning |
|---|---|
| `Submit(SubmitError)` | See above. |
| `Shutdown` | The scheduler has stopped. |
| `Config(String)` | Invalid builder configuration (no pools, missing executor, duplicate pool, `min > max`). |
| `Store(StoreError)` | Persistence failed; `StoreError::Fenced` means another instance is now leader. |
