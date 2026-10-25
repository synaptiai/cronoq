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
- **Portability.** The same engine powers the Rust runtime, the Python bindings and the server;
  a different runtime (another async executor, a simulator) only needs a new driver.

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
| Timer heap | Retries, holds, starts, misses, timeouts, leases, refills, autoscaling, cron | O(log n), lazy invalidation |
| Retention table | Terminal statuses for late dependents and dedup | O(1) |
| Window ring + log-linear histograms | Metrics | O(1) per event |

**Dispatch** compares the heads of at most eight queues (for each of the four classes: the
deadline queue and either the FIFO lane or, in pools with fairness, the fair-share queue),
computing their effective scores lazily. Every task in a class ages at the same rate, so in a
FIFO lane the oldest task is always the best of its lane. The deadline queue's head is the task
with the earliest latest start time, and the fair-share queue's head is the task with the lowest
start tag. No periodic rescoring is needed.

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
| Kubernetes | The Helm chart rendered with several value sets, each configuration checked by `cronoq-server --check`; on a kind cluster, failover between two replicas on PostgreSQL, a rolling upgrade, a two-shard release, and the operator scaling a worker Deployment. |
| Performance | Criterion benchmarks with throughput gates in CI. |
