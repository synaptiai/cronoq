# Glossary

**Aging**: the increase of a waiting task's score over time, preventing starvation.

**Attempt**: one execution of a task. A task has one or more attempts, each with its own
number.

**Backlog**: tasks ready to run but waiting for a slot.

**Class**: a priority level: critical, high, normal or low.

**Class floor**: a class set on an ordering key; the task at the front of the key never runs
below it, and no task's own priority changes ([Ordering keys](concepts/ordering-keys.md#a-floor-for-a-key)).

**Clock resynchronisation**: moving the scheduler's time forward by the time it missed while
the machine slept, so that timers that became due during the sleep fire. Used with the embedded
library ([Time, sleep and phones](guides/embedding.md#time-sleep-and-phones)).

**Deadline**: a soft time by which a task should be done; it raises urgency.

**Dead letter**: terminal status for a task whose retryable failures used every attempt.

**Driver**: the async task that owns the engine, executes its actions and feeds back results.

**Effective class**: the class a task runs at. It is the task's own class, except for the
task at the front of an ordering key, which runs at the highest class among the key's tasks
and the key's floor.

**Embedded library**: the C library `cronoq_ffi`, with its Go and Swift wrappers, that runs
the scheduler inside another program without a server; experimental
([Embedding from other languages](guides/embedding.md)).

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

**Floor**: see *Class floor*.

**Front task**: the oldest unfinished task of an ordering key; the only task of the key that
can be offered to a worker.

**Halt on failure**: an option of a task with an ordering key; if the task ends as failed or
dead-lettered, its key is halted.

**Halted (ordering key)**: said of an ordering key after a task submitted with halt on failure
ended as failed or dead-lettered; its tasks stay queued, in order, and none runs until the key
is resumed ([Ordering keys](concepts/ordering-keys.md#when-a-task-fails)). A halt comes from a
failure, a [pause](concepts/pausing.md) from an operator; resuming the key ends both.

**Hold**: a worker slot kept free for a fixed-start task about to begin.

**Key**: an optional unique name of a task, used for deduplication and references.

**Key conflict**: the error for a submission with `on_key_conflict = reject` whose key belongs
to a task that was submitted with other content; it carries that task's id and status
([Detecting conflicting submissions](concepts/key-conflicts.md)).

**Latest start time (LST)**: deadline minus estimated duration.

**Lease**: the identity of one attempt; remote workers heartbeat it to keep it alive. In the
embedded library, a lease is what a thread of the program gets from "next lease": one attempt
of one task, which ends when the program reports its result or when the scheduler ends it.

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

**Replace by key** / **latest wins**: a submission with `on_key_conflict = replace_queued`;
the newest submission of a key replaces the task that waits, or is stored to run after a task
that has started ([Replacing a task by key](concepts/replacing.md)).

**Reservation**: a booked interval of pool capacity for a fixed-start task.

**Resource**: a shared limit (semaphore or token bucket) that tasks request units of.

**Resume**: to end a pause; for an ordering key, also a halt
([Pausing and resuming](concepts/pausing.md)).

**Retention window**: how long terminal statuses are remembered.

**Shed**: evicted under overload by a shedding overflow policy.

**Strand**: the engine's internal name for an ordering key and its tasks
([Architecture](architecture.md#ordering-keys)).

**Successor**: the task created, with the same key and a new id, from a submission that was
stored to run after a task that had already started ([Replacing a task by key](concepts/replacing.md)).

**Superseded**: said of a submission that was dropped without running because a newer one with
its key replaced it.

**Slot**: capacity to run one attempt at a time.

**Snapshot**: per-pool state, quality metrics over the sliding window, and counters since start.

**Tolerance**: how far from its fixed start a task may begin and still count as on time.

**Update**: a change of the priority, deadline or tags of a task that has not finished; the
task keeps its id, key, dependents and place by submission order. An update of a task with a
fixed start, including an occurrence of a recurring schedule, is refused
([Updating queued tasks](concepts/updating.md)).

**Worker**: a process connected to `cronoq-server` that runs leased attempts.
