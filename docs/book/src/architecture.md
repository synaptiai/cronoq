# Architecture

```text
 ┌──────────────── cronoq (Rust API) · cronoq (Python) · cronoq-server (gRPC/HTTP) ───────────────┐
 │                                                                                               │
 │  Scheduler handle ──commands (bounded mpsc, batched)──▶ ┌──────────────── driver task ───────┐  │
 │  TaskHandle ◀──────oneshot results / broadcast events── │  owns   cronoq-core::Core (sans-IO)│  │
 │                                                         │  sleep_until(core.next_wakeup())   │  │
 │  executors ──feedback (completions, heartbeats)───────▶ │  executes Actions:                 │  │
 │                                                         │   Dispatch → spawn executor        │  │
 │                                                         │   Abort    → cancel attempt        │  │
 │                                                         │   ScaleTo  → Scaler                │  │
 │                                                         │   Persist  → Store (group commit)  │  │
 │                                                         └────────────────────────────────────┘  │
 └───────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Responsibility |
|---|---|
| `cronoq-core` | The scheduling engine. No clock, threads, I/O or `unsafe`. |
| `cronoq` | Tokio driver, public API, executors, scalers, `Store` trait, telemetry, blocking facade. |
| `cronoq-store-postgres` | Durable `Store` with leader election and fencing. |
| `cronoq-store-sqlite` | Durable `Store` in a local file, with a lease for leader election and fencing. |
| `cronoq-proto` | Protobuf definitions (vendored `protoc`, no system dependency). |
| `cronoq-server` | gRPC and HTTP services, remote executor, auth, metrics, worker client. |
| `cronoq-py` | PyO3 bindings (abi3), compat shim and gRPC client. |
| `cronoq-ffi` | The C library `cronoq_ffi` and its header: the scheduler embedded in a Go, Swift or C program. Experimental. |
| `cronoq-cli` | The `cronoq` command-line client for the server, with a live `top` view. |
| `cronoq-operator` | Kubernetes operator that scales worker Deployments to their pool's desired capacity. |
| `cronoq-store-tests` | Conformance tests every durable store must pass. |

## A sans-IO engine

`Core<P>` is a deterministic state machine. Each call receives the current time; the engine
updates its state and queues **actions** for the driver (dispatch this task, abort that lease,
request N workers, persist these mutations, publish this status). It never reads a clock or
performs I/O.

Consequences:

- **Determinism.** The same inputs produce the same outputs. Randomness (retry jitter) comes from
  a seeded generator.
- **Testability.** The test suite drives the engine under *virtual* time: hours of simulated
  workload run in milliseconds.
- **Portability.** The same engine powers the Rust runtime, the Python bindings, the server and
  the embedded library; a different runtime (another async executor, a simulator) only needs a
  new driver.

## Data structures

| Structure | Purpose | Cost |
|---|---|---|
| Slab of task entries | Task storage; queues hold 8-byte keys | O(1) insert/remove |
| Per-class FIFO lane (sorted deque by sequence) | Ready tasks without deadlines, in pools without fairness | O(1) push/pop; O(n) reinsertion for retries (binary search, then insert into the deque) |
| Per-class deadline queue (B-tree by latest start time) | Ready tasks with deadlines | O(log n) |
| Per-class fair-share queue (B-tree by start tag) | Ready tasks without deadlines, in pools with fairness | O(log n) |
| Due queue and pending holds | Fixed-start tasks at or near their start | O(log n) |
| Reservation calendar (B-tree by start) | Admission of fixed starts | O(k log k) for k overlapping |
| Resource waiter queues | Tasks parked on a resource, FIFO | O(1) amortized |
| Label waiter queues (one FIFO per label constraint) | Tasks waiting for a free slot on a worker with the labels they require | O(log c) for c constraints, then O(1) |
| Dependency counters + reverse edges | DAG resolution | O(children) per completion |
| Strands (per pool: a slab, a name index, and per strand a B-tree of members by sequence) | [Ordering keys](#ordering-keys): one strand per key that has tasks or stored state | O(log m) for m tasks of the key |
| Pause state (per pool: one reason for the pool and one per class; per strand: one reason) | [Pauses](#pauses) | One check per class in a dispatch decision |
| Timer heap | Retries, holds, starts, misses, timeouts, leases, refills, autoscaling, cron | O(log n), lazy invalidation |
| Retention table | Terminal statuses for late dependents and dedup | O(1) |
| Window ring + log-linear histograms | Metrics | O(1) per event |

**Dispatch** compares the heads of at most eight queues (for each of the four classes: the
deadline queue and either the FIFO lane or, in pools with fairness, the fair-share queue),
computing their effective scores lazily. Every task in a class ages at the same rate, so in a
FIFO lane the oldest task is always the best of its lane. The deadline queue's head is the task
with the earliest latest start time, and the fair-share queue's head is the task with the lowest
start tag. No periodic rescoring is needed.

## Ordering keys

Inside the engine an ordering key is called a **strand**. (The name keeps it apart from a
*lane*, the per-class ready queue.) Each pool has its own strands. A strand holds:

- the key's unfinished tasks, ordered by submission sequence number;
- a count of its tasks per class;
- the key's settings: a class floor, whether the floor persists, whether the key is halted,
  and the reason it is paused, if it is.

Only the oldest task of a strand, the **front task**, can be in a ready queue. The other tasks
of the key are ready but are in no queue, so dispatch never sees them and they do not hold up
the pool. When the front task finishes, the strand is settled: the next task is put in a ready
queue, unless the key is halted or paused.

Each task has two classes: its own, and the class it is **queued and scored at**. For most
tasks they are equal. For the front task of a strand the second one is the highest class among
the strand's tasks and its floor. This is how a critical task lifts the tasks in front of it
without a change to dispatch: the front task simply sits in the queue of a higher class. A
separate index of such lifted tasks, by their own class, lets load shedding go by a task's own
class.

A strand is forgotten when it has no tasks, no kept floor, and is neither halted nor paused.
With a durable store, the floor and the halt of a key are stored as one `Ordering` mutation
that carries the whole state of the key.

## Updates and replacements

An [update](concepts/updating.md) and a [replacement in place](concepts/replacing.md) use one
internal step. It gives a live task a new class, deadline and tags, and moves the task between
the ready queues and the per-class counters as needed. A replacement with `renew_position`
also gives the task a new sequence number. If the task has an ordering key, the strand's
per-class counts change with it, so the class its front task runs at follows.

An update of a running task changes the stored values only; the answer says `deferred`. An
update that changes nothing queues no action: nothing is stored and nothing is published.

A submission that replaces a task that has already started is not applied to the task. It is
kept with the task as its next submission. When the task ends, the engine creates the
successor in the same step.

## Pauses

Pause state is small. A pool has one optional reason for the pool as a whole and one per
class. A strand has one optional reason.

- **Pool and class.** When dispatch looks for the best candidate, it skips the queues of a
  paused class, and every class of a paused pool. The queues themselves are not touched, so
  held tasks keep their place and age.
- **Ordering key.** Pausing a strand withdraws its front task from the ready queue, as a halt
  does. Resuming puts it back.

A class pause looks at the queue a task is in. The front task of a strand is in the queue of
the class it runs at, so a class pause holds it by that class and not by its own priority.

Each pause and each resume is a `Paused` mutation, so a new leader starts with every pause in
place.

## Driver loop

The driver task waits on five sources (executor feedback, commands, handle cancellations, the
store commit in flight and the engine's next timer), applies them to the engine, then executes the
resulting actions. With a durable store, the mutations queued since the last commit are written in
one transaction while the loop keeps running. Replies, dispatches and status updates that depend on
those mutations are held until that commit succeeds, so acknowledgements imply durability.

## Server

The server wraps a `Scheduler<Bytes, Bytes>` whose pools all use a **remote executor**: a
dispatch becomes a `Lease` message to a connected worker with a free slot, and the attempt's
future resolves when that worker reports back, disconnects, or the engine aborts the attempt.
Pool capacity is the sum of connected workers' slots.

## The embedded library

The server is one way to run the engine for programs in other languages. The embedded library,
`cronoq_ffi`, is another: it runs the engine inside the program itself
([Embedding from other languages](guides/embedding.md)). It is experimental.

| | Rust library | Python package | Server | Embedded library |
|---|---|---|---|---|
| Who calls | Rust code in the same process | Python code in the same process | Clients and workers over gRPC or HTTP | Go, Swift or C code in the same process, through a C API |
| Payloads | Rust values | Python objects | Bytes | Bytes |
| Who runs a task | An executor: an async function | A coroutine | A remote worker with a lease | A thread of the program that asked for a lease |
| How capacity is set | Pool size and autoscaling | Pool size and autoscaling | The slots of the connected workers | The leases the program holds plus the threads that wait for one, up to `max_leases` |

Like the server, the library wraps a scheduler whose payloads and outputs are bytes. It uses
the blocking facade of the `cronoq` crate, built without the multi-threaded runtime, so the
driver runs on one thread that the library owns. The SQLite store, if used, adds threads of
its own.

Its executor is a **pull executor**. When the engine dispatches a task, the attempt is placed
in a hub. A thread of the host program takes it from the hub with the blocking call
`cronoq_next_lease`, and later reports the result, which resolves the attempt. Nothing calls
back into the host.

The capacity of a pool follows the host: it is the number of leases the host holds plus the
number of threads that wait in `cronoq_next_lease`, up to the pool's `max_leases`. So the
engine chooses a task when a thread asks for one, not before, and the order of the queue is
decided as late as possible. There is one exception: when a waiting thread gives up, or the
engine ends a lease the host holds, the engine can already have started one more task. That
task counts as running and goes to the next thread that asks.

Every call that can fail returns a status code, and a panic inside the library is
caught and reported as `CRONOQ_INTERNAL`.

Engine time follows the clock of the Rust standard library, which does not count the time a
machine sleeps on Apple platforms and on Linux. `Scheduler::resync_clock` moves engine time
forward by what was missed, measured with a monotonic clock that counts during sleep; engine
time never moves back.

## Testing strategy

| Layer | Approach |
|---|---|
| Engine | Unit tests per feature; property tests for data structures; **randomized deterministic simulation**: hundreds of seeded workloads mixing microtasks, heavy-tailed jobs, deadlines, fixed starts, DAGs, resources, failures, cancellations and capacity changes, with internal invariants checked after every step and end-to-end properties (dependency order, FIFO within class, every task terminates) checked at the end. |
| Runtime | Tests under tokio's paused clock; OpenTelemetry export test. |
| Store | One conformance suite run against every store (throwaway PostgreSQL servers, temporary SQLite files); fencing, `pg_terminate_backend` failover, lease expiry and takeover. |
| Server | Real gRPC/HTTP listeners with real workers; active-passive failover test. |
| Python | pytest suites for the bindings, the compat shim and the client against a real server, run against built wheels on Linux, macOS and Windows. |
| Go | `go test -race` for the SDK against a real server, with the generated stubs checked against the protocol. |
| TypeScript | `node:test` suites for the SDK against a real server on Node 22 and 24, and the gRPC-Web client in headless Chromium; the generated code is checked against the protocol. |
| Embedded library | Tests of the C API in Rust; a C, a Go and a Swift example program that check what they get and run in CI; the Go wrapper's tests in both build modes; on Apple platforms, the engine's simulation tests and the C API tests in the iOS simulator. |
| Kubernetes | The Helm chart rendered with several value sets, each configuration checked by `cronoq-server --check`; on a kind cluster, failover between two replicas on PostgreSQL, a rolling upgrade, a two-shard release, and the operator scaling a worker Deployment. |
| Performance | Criterion benchmarks with throughput gates in CI. |
