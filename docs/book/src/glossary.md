# Glossary

**Aging**: the increase of a waiting task's score over time, preventing starvation.

**Attempt**: one execution of a task. A task has one or more attempts, each with its own
number.

**Backlog**: tasks ready to run but waiting for a slot.

**Class**: a priority level: critical, high, normal or low.

**Class floor**: a class set on an ordering key; the task at the front of the key never runs
below it, and no task's own priority changes ([Ordering keys](concepts/ordering-keys.md#a-floor-for-a-key)).

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

**Fingerprint**: 128 bits stored with a task that say what its submission asked for; a later
submission of the same key with `on_key_conflict = reject` is compared against it
([Detecting conflicting submissions](concepts/key-conflicts.md)).

**Freeze rule**: every attempt of one task sees the same payload; a task that has started is
never changed by a [replacement](concepts/replacing.md), only followed.

**Fixed start**: a time at which a task should begin, backed by a reservation.

**Halted (ordering key)**: said of an ordering key after a task submitted with halt on failure
ended as failed or dead-lettered; its tasks stay queued, in order, and none runs until the key
is resumed ([Ordering keys](concepts/ordering-keys.md#when-a-task-fails)). A halt comes from a
failure, a [pause](concepts/pausing.md) from an operator; resuming the key ends both.

**Hold**: a worker slot kept free for a fixed-start task about to begin.

**Key**: an optional unique name of a task, used for deduplication and references.

**Latest start time (LST)**: deadline minus estimated duration.

**Lease**: the identity of one attempt; remote workers heartbeat it to keep it alive.

**Lease TTL**: how long a lease survives without a heartbeat.

**Miss policy**: what happens to a fixed-start task that could not start within its tolerance.

**Ordering key**: a name shared by tasks of one pool that must run one at a time, in the order
they were submitted; it does not deduplicate ([Ordering keys](concepts/ordering-keys.md)).

**Parked**: a ready task waiting for a resource, outside the dispatch queue.

**Pause**: a hold on a pool, on one class of a pool or on one ordering key, set with a reason;
what is paused stays queued, in place, and none of it is handed to a worker until it is
resumed ([Pausing and resuming](concepts/pausing.md)).

**Pending**: submitted but not running (counted against queue bounds).

**Pool**: a group of worker slots with one executor, one autoscaling policy and its own bounds.

**Release**: to give an attempt back without a verdict, because the executor cannot run the
task now; the task keeps its place, is offered again after a delay, and the attempt does not
count against the retry policy, up to `max_releases`
([Releasing an attempt](concepts/reliability.md#releasing-an-attempt)).

**Reservation**: a booked interval of pool capacity for a fixed-start task.

**Resource**: a shared limit (semaphore or token bucket) that tasks request units of.

**Resume**: to end a pause; for an ordering key, also a halt
([Pausing and resuming](concepts/pausing.md)).

**Retention window**: how long terminal statuses are remembered.

**Shed**: evicted under overload by a shedding overflow policy.

**Successor**: the task created, with the same key and a new id, from a submission that was
stored to run after a task that had already started ([Replacing a task by key](concepts/replacing.md)).

**Superseded**: said of a submission that was dropped without running because a newer one with
its key replaced it.

**Slot**: capacity to run one attempt at a time.

**Snapshot**: per-pool state, quality metrics over the sliding window, and counters since start.

**Tolerance**: how far from its fixed start a task may begin and still count as on time.

**Worker**: a process connected to `cronoq-server` that runs leased attempts.
