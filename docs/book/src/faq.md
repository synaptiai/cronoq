# FAQ

### Is execution exactly-once?

Execution is at-least-once: an attempt can run again if its worker crashes, misses heartbeats, or
the leader fails over mid-attempt. Side effects can still be exactly-once: record the task id and
attempt number (a fencing token) with each effect and let only the newest attempt write. See
[Exactly-once side effects](concepts/reliability.md#exactly-once-side-effects). Task keys make
*submission* idempotent.

### Does a deadline cancel the task when it passes?

No. Deadlines raise urgency; they never cancel. Use `timeout` to bound an attempt, or cancel the
task yourself.

### What is the difference between a deadline and a fixed start?

A deadline says when work should be *finished* and lets cronoq reorder work to meet it. A fixed
start says when work should *begin*; cronoq reserves capacity and holds a worker for it.

### Can low-priority work starve?

Not behind `normal` traffic: aging lets a waiting `low` task overtake fresh `normal` work after
about 45 seconds with the default settings. It can wait behind sustained `high` or `critical`
load, by design. For stronger mixing, change `aging_rate` or `age_cap`, for the whole scheduler
or for one pool, in the server configuration, in Rust or in Python (see
[Priorities and aging](concepts/priorities.md#configuration)).

### How many tasks can the engine hold?

Memory per pending task is a few hundred bytes plus its payload. Millions of pending tasks are
practical; use `max_pending` to bound them.

### Can I use cronoq without tokio?

From Rust, use `BlockingScheduler` (it owns a private tokio runtime), or drive `cronoq-core`
yourself. From Python, the bindings manage their own runtime. From Go, Swift or C, the
experimental [embedded library](guides/embedding.md) runs the scheduler on one thread that
the library owns. Its SQLite store, if used, adds threads of its own.

### Do I need Postgres?

No. Without a store, state is in memory. For durability on one host, use the SQLite store (a
local file, with standby processes on the same host). Use Postgres to run replicas on several
hosts.

### How do workers find the leader in an HA setup?

Through a load balancer or Kubernetes Service that routes only to ready endpoints. Only the
leader reports `/readyz` = 200.

### Can tasks pass data to their dependents?

Not directly. Put references (object-store paths, database ids) in payloads, or read the
parent's output with `Wait` / `TaskHandle` in the producer and submit the child afterwards.

### How do I limit tokens per minute rather than requests?

Create a token bucket whose units are, for example, thousands of tokens, and have each task
request `ceil(estimated_tokens / 1000)` units.

### Why are my server tasks not running?

A server pool's capacity is its connected workers' slots. Check `cronoq_workers_connected` and
`cronoq_pool_workers{pool=...}`; then check that the task's resources exist and are not exhausted.

### Is there a UI?

There is no web UI. `cronoq top` gives a live view in the terminal; for the rest, use
`/v1/snapshot`, the `Admin` service, Prometheus/Grafana and status events.

### What happens when the database is unavailable?

The scheduler keeps running in memory and retries the commit with backoff for up to
`max_store_outage` (5 minutes by default). Meanwhile, calls that need durability (submissions,
cancellations, schedule changes) wait, and no new attempts start. If the outage lasts longer,
the scheduler stops and those callers get a store error (`UNAVAILABLE` / 503 on the server);
another replica takes over once the database is back. See
[Durability and HA](concepts/durability.md#store-failures).

### Can several teams share one server?

Within one trust domain, yes: give each team tokens scoped to its pools
(`pools = [...]`), so it can only submit into, serve, watch and look up its own pools. Keys and
resources are shared across pools, so teams that must not learn about each other's work need
separate servers. See [Security hardening](guides/security.md).

### How do I upgrade a cluster with a durable store?

The new version applies its schema migrations when it connects, and an older build refuses to
start against a newer schema, so upgrades roll forward only. When a release adds no migration,
replace the standbys first and the leader last; accepted work survives the leader change and
workers reconnect on their own. When the changelog lists a migration, follow its upgrade notes
(or stop all replicas, then start the new version). See [Upgrading](guides/upgrading.md).

### How do I keep the events of one account in order?

Give them the same [ordering key](concepts/ordering-keys.md). Tasks of one pool that share an
ordering key run one at a time, in the order they were submitted. Tasks with other keys run in
parallel. See [Ordered work per entity](use-cases/ordered-work.md).

### Does priority work inside an ordering key?

Not for the order inside the key: that is always submission order. Priority decides how the
key competes with other work. The task at the front of a key runs at the highest class among
the key's unfinished tasks, so a critical task lifts the tasks in front of it.

### What is the difference between a key and an ordering key?

A key names one task. A second submission with the key of an existing task returns that task,
so a key makes a submission safe to retry. An ordering key names a group of tasks that must
run one at a time. It never deduplicates: two submissions with one ordering key are two tasks.
A task may have both.

### What is the difference between a halt and a pause?

Both hold the tasks of an ordering key in place. A **halt** comes from a failure: a task that
was submitted with halt on failure ended as failed or dead-lettered. A **pause** comes from an
operator, and can also hold a pool or a class. Resuming an ordering key ends both. See
[Ordering keys](concepts/ordering-keys.md#when-a-task-fails) and
[Pausing and resuming](concepts/pausing.md).

### Can I change a task after I submitted it?

Yes, as long as it has not finished and has no fixed start. An
[update](concepts/updating.md) changes its priority, deadline or tags and keeps its id and
its place by submission order. An update of a task with a
[fixed start](concepts/fixed-starts.md) is refused; this includes the occurrences of a
recurring schedule. To change the payload,
submit again with the same key and `on_key_conflict = replace_queued`
([Replacing a task by key](concepts/replacing.md)). A task that has started an attempt keeps
its payload; the new submission runs after it.

### How do I stop work for a maintenance window?

[Pause](concepts/pausing.md) the pool, one class of it or one ordering key, with a reason.
What is paused stays queued and keeps its place; running attempts are not interrupted. Tasks
with a fixed start and occurrences of schedules are not held. A pause has no timer: resume it
when the window is over. See [Holding work without losing it](use-cases/holding-work.md).

### My worker cannot reach a downstream service. Should the task fail?

No. [Release](concepts/reliability.md#releasing-an-attempt) the attempt with a delay. The task
keeps its place, is offered again after the delay, and the attempt does not count against the
retry policy, up to `max_releases` (10 unless set).

### The same key arrived with different content. What happens?

By default the second submission is answered like a retry: it returns the existing task, and
its content is dropped. With `on_key_conflict = reject` it is refused as a
[key conflict](concepts/key-conflicts.md), with the id and status of the existing task. The
changelog announces that `reject` becomes the default in the next minor release.

### Can I use cronoq inside a mobile or desktop app?

Experimentally, yes. A C library embeds the scheduler in a Go, Swift or C program, without a
server ([Embedding from other languages](guides/embedding.md)). It does not run while the app
is suspended. Call "resync clock" when the app becomes active again, so that timers that
became due during the sleep fire. See
[Offline-first clients and embedded queues](use-cases/embedded-queues.md).
