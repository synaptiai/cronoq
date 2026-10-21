# Glossary

**Aging**: the increase of a waiting task's score over time, preventing starvation.

**Attempt**: one execution of a task. A task has one or more attempts.

**Backlog**: tasks ready to run but waiting for a slot.

**Class**: a priority level: critical, high, normal or low.

**Deadline**: a soft time by which a task should be done; it raises urgency.

**Dead letter**: terminal status for a task whose retryable failures used every attempt.

**Driver**: the async task that owns the engine, executes its actions and feeds back results.

**Edge condition**: when a dependency is satisfied: on success, on failure or on completion.

**Effective score**: base score of the class plus aging plus deadline urgency; the highest
dispatches first.

**Engine** / **core**: the sans-IO state machine (`cronoq-core`) that makes all scheduling
decisions.

**Epoch**: a counter incremented by each new leader; used for fencing and embedded in task ids.

**Executor**: the code that runs an attempt in-process.

**Fencing**: rejecting writes from a leader whose epoch is no longer current.

**Fixed start**: a time at which a task should begin, backed by a reservation.

**Hold**: a worker slot kept free for a fixed-start task about to begin.

**Key**: an optional unique name of a task, used for deduplication and references.

**Latest start time (LST)**: deadline minus estimated duration.

**Lease**: the identity of one attempt; remote workers heartbeat it to keep it alive.

**Lease TTL**: how long a lease survives without a heartbeat.

**Miss policy**: what happens to a fixed-start task that could not start within its tolerance.

**Parked**: a ready task waiting for a resource, outside the dispatch queue.

**Pending**: submitted but not running (counted against queue bounds).

**Pool**: a group of worker slots with one executor, one autoscaling policy and its own bounds.

**Reservation**: a booked interval of pool capacity for a fixed-start task.

**Resource**: a shared limit (semaphore or token bucket) that tasks request units of.

**Retention window**: how long terminal statuses are remembered.

**Shed**: evicted under overload by a shedding overflow policy.

**Slot**: capacity to run one attempt at a time.

**Snapshot**: per-pool metrics over the sliding window.

**Tolerance**: how far from its fixed start a task may begin and still count as on time.

**Worker**: a process connected to `cronoq-server` that runs leased attempts.
