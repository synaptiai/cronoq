# cronoq — Specification v0.1

> Status: **Accepted (v0.1 baseline)** · Owner: Daniel Bentes · License: MIT OR Apache-2.0
> Predecessor: the Python `ElasticPriorityScheduler` in `docs/prototype/`, whose API is preserved
> by `cronoq.compat`.

> **This is the design document**: decisions, invariants and rationale for contributors. For
> user documentation (concepts, use cases, guides and reference) see the
> [cronoq book](https://synaptiai.github.io/cronoq/) (source in `docs/book/`).

cronoq is an embeddable, elastic, priority-aware task scheduler written in Rust,
with Python bindings and an optional gRPC/HTTP server backed by Postgres.
It decides **when** and **in what order** work runs and **how much capacity**
is needed; *where* work runs is pluggable.

---

## 1. Goals and non-goals

### Goals
- Serve three workload shapes with one engine:
  - **Microtasks**: ms-scale, 10k–1M tasks/s, in-process.
  - **LLM / agent jobs**: I/O-bound, seconds to minutes, rate-limited, unknown durations.
  - **Batch / compute**: minutes to hours, DAGs, fixed schedules, expensive workers.
- Keep the prototype's semantics (priority classes, aging, deadlines, fixed starts,
  dependencies, elastic scaling, quality metrics) while fixing its defects (§14).
- Make the hot path O(1) / O(log n), with no periodic rescans and no polling.
- Be deterministic and testable: the core is a pure state machine run under a virtual clock.
- Ship as a Rust library, a Python wheel and a server binary.

### Non-goals (v0.1)
- Several leaders for one pool. Each shard (a group of pools) has one leader with standbys
  (§13); shards run side by side, but a pool never has two leaders.
- General constraint solving or bin-packing. We provide pools, counted resources, and worker
  labels: a task can require labels, and runs only in a group of workers that has them.
- Exactly-once execution of arbitrary code. We provide at-least-once with leases, plus what
  effectively-once side effects need: attempt numbers that never repeat for a task (a fencing
  token), lease ids unique across leaders, stale reports ignored and counted, and key
  deduplication of finished tasks within a configurable window.
- Being a workflow engine (no durable step state or replay of user code).

### Performance acceptance targets (CI-gated)
| Metric | Target |
|---|---|
| Core engine submit → dispatch → complete cycles, 1 core | **≥ 1,000,000 /s** |
| End-to-end through tokio runtime, no-op in-process tasks | **≥ 250,000 /s** |
| p99 dispatch latency at 50% load (runtime) | **< 1 ms** |
| Engine memory per pending task (excluding payload) | **< 300 B** |

---

## 2. Decision log

| # | Decision | Choice |
|---|---|---|
| D1 | Consumers | Library + optional server |
| D2 | Worker model | Pluggable `Executor` + `Scaler` backends |
| D3 | Workloads | General purpose: microtasks, LLM/agent, batch |
| D4 | Durability | Pluggable `Store`; in-memory default; Postgres first durable backend |
| D5 | Language | Rust (tokio); PyO3 bindings; tonic server |
| D6 | Priority | Lane-based aging (per-class FIFO + per-class deadline heap) |
| D7 | Autoscaling | Queue-pressure policy (rates, backlog, SLO, reservations), pluggable |
| D8 | Fixed start | Capacity reservations + worker holds + per-task miss policy; cron on top |
| D9 | Delivery | At-least-once with leases, heartbeats, retries, dead-letter |
| D10 | DAG | In-degree counters; per-edge conditions; configurable unknown-dependency policy |
| D11 | Heterogeneity | Named pools + counted resources (semaphores and token buckets) |
| D12 | Duration | Optional hint; learned per `kind` (EWMA + quantiles); timeout separate |
| D13 | Engine | Sans-IO single-threaded core plus tokio driver |
| D14 | Rust API | Async-first with a `blocking` facade |
| D15 | Server | tonic gRPC (streaming worker leases) plus an axum JSON gateway |
| D16 | Backpressure | Bounded queues; policy: Wait / Reject / ShedLowest / ShedOldest |
| D17 | Payload | Generic `P` in the library; `Bytes` + content type on the wire and in the store |
| D18 | Telemetry | OpenTelemetry native (traces + metrics); cargo feature `otel` on by default |
| D19 | Python | asyncio-native PyO3/maturin abi3 wheel, `import cronoq` |
| D20 | HA | Active-passive: Postgres advisory-lock leader election + fencing epoch |
| D21 | Scope | Everything (core, runtime, Python, server, Postgres, HA) in v0.1 |
| D22 | Name / repo | `cronoq` everywhere; `github.com/Synaptiai/cronoq` (private, then public) |
| D23 | Sharding | Shards are groups of pools, one server process per shard with its own store; servers forward client calls with the caller's token and redirect workers; ids carry the shard number |
| D24 | SDKs | Rust and Python in their crates; Go (grpc-go) and TypeScript in `sdks/`, generated from the proto with buf plus a hand-written client and worker loop |
| D25 | Kubernetes | A Helm chart for the server (all shards in one release, readiness on `/healthz` so standbys stay ready) and a separate operator chart that scales worker Deployments from the server's desired capacity |

---

## 3. Architecture

```
            ┌────────────── cronoq (Rust API, Python, gRPC) ──────────────┐
 submit ──▶ │  Handle ──mpsc(batched)──▶  Engine task (tokio, 1 thread)    │
 cancel     │                             ┌──────────────────────────┐    │
 watch  ◀── │  broadcast/oneshot  ◀───────│  cronoq-core (sans-IO)   │    │
            │                             │  pure state machine      │    │
            │   timer: sleep_until(next)──▶│  handle(now, Input)      │    │
            │                             │  poll_action() → Action  │    │
            │                             └───────┬──────────────────┘    │
            │        Actions: Dispatch / Abort / SetCapacity / Persist / Notify
            │                ▼                ▼                ▼          │
            │          Executor(s)         Scaler(s)        Store         │
            │   in-process | remote(gRPC)  noop|webhook   memory|postgres │
            └─────────────────────────────────────────────────────────────┘
```

### Crates (Cargo workspace)
| Crate | Purpose |
|---|---|
| `cronoq-core` | Sans-IO engine: data model, lanes, timelines, DAG, leases, autoscale policy, metrics. No tokio, no clock, no I/O. `#![forbid(unsafe_code)]`. |
| `cronoq` | Public library: tokio runtime driver, `Scheduler<P, O>`, executors, scalers, `Store` trait plus in-memory store, OTel, `blocking` facade. |
| `cronoq-store-postgres` | Postgres `Store` (sqlx), leader election, fencing, recovery. |
| `cronoq-proto` | Protobuf definitions and generated tonic code. |
| `cronoq-server` | `cronoq-server` binary: gRPC, HTTP gateway, remote executor, Prometheus, config. |
| `cronoq-py` | PyO3 bindings (built with maturin), plus a pure-Python gRPC worker client. |

### Time
- The core uses `Time(u64)`: nanoseconds on a monotonic clock since an engine epoch.
  The core never reads a clock; every input carries `now`.
- Wall-clock inputs (fixed starts, cron, deadlines given as timestamps) are converted at
  the API boundary. The store persists wall-clock (UTC); recovery re-maps them.
- The runtime's `Clock` trait has a `SystemClock` and a `MockClock` for tests.

---

## 4. Data model

```rust
pub struct TaskSpec<P> {
    pub payload: P,
    pub kind: Kind,                      // interned string; routing, stats, duration learning
    pub pool: PoolName,                  // default "default"
    pub priority: Priority,              // Critical | High | Normal | Low
    pub key: Option<String>,             // unique while live and retained; dedup and dependency references
    pub deadline: Option<When>,          // soft: drives urgency
    pub fixed_start: Option<FixedStart>, // hard-ish: start at a time (see §6)
    pub duration_hint: Option<Duration>,
    pub timeout: Option<Duration>,       // per attempt
    pub retry: RetryPolicy,              // default: pool default
    pub deps: Vec<Dep>,                  // Dep { target: TaskRef, cond: EdgeCond }
    pub resources: SmallVec<[(ResourceId, u32); 2]>,
    pub tags: SmallVec<[(Str, Str); 2]>, // incl. tenant, W3C traceparent
}
pub enum TaskRef { Id(TaskId), Key(String) }
pub enum EdgeCond { OnSuccess /*default*/, OnComplete, OnFailure }
pub struct FixedStart { at: When, tolerance: Duration /*default 1s*/, on_miss: MissPolicy }
pub enum MissPolicy { RunLate /*default*/, Skip, Fail }
```

- `TaskId(u64)` is engine-assigned and monotonic; in server mode it is prefixed with the
  leader epoch to stay unique across failovers.
- Submitting a `key` that is already live returns the existing `TaskId`
  (`SubmitOutcome::Duplicate`), which gives idempotent submit within the retention window.
- Tasks live in a slab arena (`u32` index + generation). Queues hold 8-byte keys;
  cancellation is O(1) via tombstones that are skipped lazily.

### Lifecycle

```
           ┌──────── deps unmet ───────┐
 Submit ──▶│ Blocked │──deps met──▶ Ready ◀──backoff elapsed── Retrying
           │ Scheduled (fixed/cron) ─hold/at─▶ Ready              ▲
           └───────────────────────────────┘    │ dispatch         │ retryable failure
                                                ▼                  │ / lease lost / timeout
                                              Running(attempt, lease)
                                                │
         ┌────────────┬───────────┬────────────┼──────────────┐
      Succeeded    Failed     DeadLettered   Cancelled     Skipped / Missed / Shed
   (terminal)  (permanent err) (attempts out) (by user or dep) (dep cond / fixed miss / overload)
```

Terminal statuses are retained in a compact table (`TaskId`/key → status, end time)
for `retention` (default 1 h, max 1 M entries, LRU). This serves late-arriving dependents
and `get`. Results (`O`) are retained only while a handle or watcher holds them, or in
the store.

---

## 5. Priority and dispatch (per pool)

### 5.1 Effective score (the prototype's semantics, kept)
```
score = base[class] + min(aging_rate[class] · wait, age_cap[class]) + urgency
defaults: base = {Critical: 10000, High: 1000, Normal: 100, Low: 10}
          aging_rate = 2.0/s, age_cap = 800        (all configurable per class)
```

**Urgency** (fixes prototype bug #4: it must be monotonic). Let `d` be the duration
estimate (§8), `lst = deadline − d` the latest start time, `slack = lst − now`, and
`H = max(d, urgency_window /*default 5s*/)`:
```
urgency = 0                              if slack ≥ H
        = U · (1 − slack/H)              if 0 ≤ slack < H
        = U + min(overdue_rate·(−slack), overdue_cap)  if slack < 0
defaults: U = 5000, overdue_rate = 100/s, overdue_cap = 3000
```

### 5.2 Lanes: why no rescans are needed
Within one class every non-deadline task ages at the same rate from its own arrival, so
**older ⇒ score ≥ younger, at every instant**, including after the cap (ties are broken
by sequence). A plain FIFO per class therefore always has its best task at the head.

Per pool, per class:
- `lane[c]: VecDeque<Key>`: ready tasks with no deadline, in FIFO order.
- `edf[c]: BinaryHeap<(lst, seq, Key)>`: ready tasks with a deadline, ordered by
  latest start time. Within a class, deadline tasks run in LST/EDF order, which is optimal
  for meeting deadlines.

Dispatch, when the pool has an unheld free slot:
1. Candidates are at most 2 × 4 heads. Compute each one's full score lazily at `now`.
2. Take the maximum (ties: class, then seq). If it lacks resource tokens (§9), park it on
   that resource's waiter queue and repeat.
3. Pop the task and emit `Dispatch`.

Cost: O(classes) plus O(log n) for a heap pop. No periodic work.

### 5.3 Opt-in custom scoring (slow path)
A pool may set `ScoringMode::Custom(Box<dyn Fn(&TaskView, Time) -> f64>)`. It uses a
single heap re-scored every `rescore_interval`, and is documented as O(n log n).

---

## 6. Fixed-start and recurring tasks

### 6.1 Admission (fixes prototype bug #2)
A reservation occupies the interval `[at − tolerance, at + d + tolerance]` in its pool.
Admission succeeds iff the maximum overlap with existing reservations stays
`≤ floor(pool.max_workers · reservable_fraction)` (default fraction 0.5, so ad-hoc work
can never be fully starved). The check is a sweep over a `BTreeMap<Time, Δ>` (O(k log n)).
On conflict: `SubmitError::ReservationConflict`.

### 6.2 Holding workers
At `hold_at = at − hold_lead`, where `hold_lead = max(tolerance, p90 duration of the pool's
non-fixed work, capped by max_hold /*default 5 min*/)`, re-evaluated from current
statistics until the hold is taken (§21):
- if a free slot exists, it becomes **held** for this reservation;
- otherwise the next slot that frees up in the pool is held (holds have priority over lanes).

Upcoming reservations also feed the autoscaler (§10), so capacity is pre-warmed.

### 6.3 Start and miss
- At `at`, the task dispatches to its held slot. Its recorded start time is the **actual**
  dispatch time (fixes prototype bug #3).
- If `now > at + tolerance` and it has not started, apply `on_miss`:
  `RunLate` (dispatch with priority over everything else), `Skip` (→ Missed), or `Fail`.
- The fixed-start hit rate is `|actual − at| ≤ tolerance`.

### 6.4 Recurring (cron)
`Scheduler::add_recurring(Recurring::new(name, cron_expr, template).tz(..).overlap(..))`, with
`overlap: Skip | Allow | Queue | QueueOnSuccess`. Each occurrence is materialized as a
fixed-start task when the previous one fires (look-ahead of 1 occurrence). The template's
relative deadline applies from each occurrence's start, and its key is a prefix
(`<key>@<start in UTC, RFC 3339>`). `Queue` and `QueueOnSuccess` make each occurrence depend on
the previous one (`on_complete` / `on_success`); the predecessor's task id is persisted with the
schedule (`RecurringNext.last`), and adding the schedule again starts a new chain. Cron
definitions are persisted.

---

## 7. Dependencies (DAG)

- Dependencies are tracked by in-degree counters (`unmet`) and a reverse adjacency list
  of dependents. A parent's terminal transition decrements or cancels its children in
  O(out-degree). There is no scan of completed tasks (fixes prototype bug #1).
- Edge conditions: when a parent finishes, the edge is **satisfied** if
  `OnSuccess ∧ Succeeded`, `OnFailure ∧ (Failed|DeadLettered)`, or `OnComplete ∧ any terminal`.
  Otherwise the child becomes `Skipped` and this propagates recursively.
- Parent already terminal at submit: resolved immediately from the retention table.
- Unknown `Key` reference: per-pool `UnknownDependency::Reject` (default) or
  `Await { timeout }`, where the child waits for that key to appear.
- Cycles are only possible with `Await`. On submit, a DFS from the new node through its
  awaiting dependents checks whether any of them is among its parents; if so the submit is
  rejected. `submit_batch` admits a whole DAG atomically (topological check).
- Dependents are routed to their own pools and classes when they become ready.

---

## 8. Durations

- `DurationStats` per `(pool, kind)`: EWMA mean plus a streaming quantile sketch (p50/p90),
  updated from observed successful runtimes.
- Estimate used for a task: `duration_hint`, else `stats.p50`, else
  `pool.default_duration /*1s*/`.
- Used by: urgency (§5.1), reservation intervals and hold lead (§6), autoscaling (§10).
  It is **never** used to decide completion; completion is always observed.
- Timeout is separate. A timeout fires `Abort(lease)` and counts as a retryable failure.

---

## 9. Pools, resources, executors, scalers

### 9.1 Pools
```rust
PoolConfig { name, min_workers, max_workers, initial_workers,
             executor: Arc<dyn Executor<P, O>>, scaler: Arc<dyn Scaler>,
             autoscale: AutoscalePolicy, retry_default, bounds: Bounds, ... }
```
A *worker* is one capacity slot. The core tracks `capacity`, `busy`, `held` and
`draining` counts per pool. Scale-down only removes idle slots; busy slots are marked
draining and removed when they finish.

### 9.2 Resources
`ResourceConfig::Semaphore { permits }` or `::TokenBucket { rate, burst }`, shared across
pools (for example `openai_tpm`). A task acquires all its resources atomically at
dispatch; semaphores are released at the end of each attempt. A resource-starved task
waits on that resource's FIFO waiter queue, and is re-offered as a candidate when permits
are released or tokens refill (the core schedules a timer for the next refill).
This avoids head-of-line blocking.

### 9.3 Executor
```rust
#[async_trait]
pub trait Executor<P, O>: Send + Sync + 'static {
    async fn execute(&self, ctx: TaskContext, payload: Arc<P>) -> Result<O, TaskError>;
}
// TaskContext: id, attempt, deadline/timeout, cancellation token, heartbeat(), span
// TaskError { kind: Retryable | Permanent | Cancelled, message, retry_after: Option<Duration> }
```
Built in: `FnExecutor` (wraps an async closure), and in the server `RemoteExecutor`, which
forwards leases over gRPC streams to registered workers. For the in-process executor a
lease is the spawned future; for remote workers a lease has a TTL (default 30 s) that is
extended by heartbeats. Lease expiry counts as a retryable failure.

### 9.4 Scaler
```rust
#[async_trait]
pub trait Scaler: Send + Sync + 'static {
    /// Request capacity; returns capacity actually available now (may lag).
    async fn scale_to(&self, pool: &str, desired: u32) -> Result<u32, ScaleError>;
}
```
Built in: `SlotScaler` (in-process: capacity = desired, immediately), `WebhookScaler`
(POSTs JSON to a URL), `ExternalScaler` (only publishes the `cronoq_pool_desired_workers`
gauge, for use with KEDA/HPA). For remote pools, capacity comes from registered worker
streams; the scaler only requests.

---

## 10. Autoscaling (queue-pressure policy)

Evaluated per pool on a core timer (`interval`, default 1 s). Inputs are windowed
estimates: arrival rate λ (EWMA), mean service time S, backlog B (ready + retrying per
class), reservations R(h) starting within the horizon `h`, and current capacity c.

```
base     = ceil(λ · S / target_utilization)            # steady state (default 0.75)
drain    = ceil(B · S / drain_time)                    # clear backlog within drain_time (default 30s)
slo      = min c' such that predicted wait W_c(c') ≤ slo[class] for every class with backlog
           (W_c ≈ backlog at or above class c · S / c')
desired  = clamp(max(base + drain, slo) + R(h), min, max)
```
- **Scale-up**: immediate, limited to `max_step_up` (default +100% or +4, whichever is larger).
- **Scale-down**: only to the maximum desired value seen over `stabilization_window`
  (default 60 s; same approach as Kubernetes HPA), at most `max_step_down` per
  `cooldown` (default 30 s).
- Cost: `worker_seconds × cost_per_worker_second` per pool, reported in metrics.
- `AutoscalePolicy` is a trait with this implementation as the default.

This replaces the prototype's makespan-based "quality" trigger (prototype bug #5).

---

## 11. Backpressure

`Bounds { max_pending_total, per_pool, per_class }` with `OverflowPolicy`:
- `Wait`: async submit awaits space (default for the Rust API);
- `Reject`: `SubmitError::Overloaded`;
- `ShedLowest`: evict the newest task of the lowest class that is below the incoming
  task's class (→ `Shed`); otherwise reject;
- `ShedOldest`: evict the oldest Low-class task.

Fixed-start tasks are never shed (see §21).

Waiters are woken by the engine when capacity frees up. Bounds are also visible to the
server as gRPC `RESOURCE_EXHAUSTED`.

---

## 12. Core (sans-IO) interface

```rust
impl<P> Core<P> {
    fn new(cfg: CoreConfig) -> Self;
    fn handle(&mut self, now: Time, input: Input<P>);
    fn poll_action(&mut self) -> Option<Action<P>>;   // drain after each handle()
    fn next_wakeup(&self) -> Option<Time>;            // earliest internal timer
    fn snapshot(&self, now: Time) -> Snapshot;
}
enum Input<P> { Submit(Vec<(Ticket, TaskSpec<P>)>), Cancel(TaskRef), Complete { lease, outcome },
                Heartbeat(LeaseId), CapacityChanged { pool, capacity }, Tick,
                Recover(RecoveredState<P>), ... }
enum Action<P> { Accepted { ticket, id } | Rejected { ticket, err },
                 Dispatch { lease, task: TaskId, pool, payload: Arc<P>, ctx },
                 Abort { lease, reason }, ScaleTo { pool, desired },
                 Persist(Mutation), StatusChanged { id, status } }
```
Internal timers live in one `BinaryHeap<(Time, TimerKind)>` (retry backoff, holds,
fixed starts, miss deadlines, lease expiry, token refill, autoscale tick, await timeouts)
with lazy invalidation.

### Runtime driver
- The engine task owns `Core`. Inputs arrive on a bounded mpsc and are drained with
  `recv_many` (batching amortizes wakeups). The task then `sleep_until(next_wakeup)`.
- `Dispatch` → `tokio::spawn(executor.execute(..))` with timeout and cancellation; the
  completion is sent back as an input.
- `Persist` mutations are group-committed to the `Store`. A submit is acknowledged only
  after its mutation is durable (with the in-memory store this is immediate). The engine
  does not wait for the store: actions that depend on durability are buffered until
  their commit succeeds.

---

## 13. Persistence and HA

```rust
#[async_trait] pub trait Store: Send + Sync {
    async fn acquire_leadership(&self) -> Result<Epoch>;   // blocks until leader
    async fn commit(&self, epoch: Epoch, batch: &[Mutation]) -> Result<()>; // fenced
    async fn recover(&self) -> Result<RecoveredState<Bytes>>;
}
// What a server needs beyond the scheduler's use: lookups, pruning, leadership state.
#[async_trait] pub trait StoreQueries<O>: Send + Sync {
    async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord<O>>>;
    async fn prune(&self, older_than: Duration) -> Result<u64>;
    fn is_leader(&self) -> bool;
    async fn leadership_lost(&self);
}
```
Every durable store passes one shared conformance suite (`cronoq-store-tests`): round trips of
every mutation and field, time rebasing, fencing and handover, key reuse within a batch,
dependencies on pruned parents, missed starts after restart, batching-independent messages,
output loading, id high-water marks after pruning, and restart without rerunning finished work.
**Postgres schema** (sqlx migrations): `tasks` (spec, status, attempt, timestamps, result
ref), `deps`, `crons`, `leader(epoch)`.
- **Leader election**: `pg_try_advisory_lock` on a dedicated session; on success,
  `UPDATE leader SET epoch = epoch + 1 RETURNING epoch`.
- **Fencing**: every commit runs `… WHERE (SELECT epoch FROM leader) = $epoch` in the same
  transaction. A stale leader's writes fail, and it steps down.
- **Recovery**: load non-terminal tasks, crons and recent terminal statuses (for
  dependencies). Tasks that were `Running` are treated as lease-lost and retried
  according to policy (at-least-once).
- **Standbys** poll for the lock; on takeover they recover and resume. Remote workers
  reconnect to the new leader (clients get the leader address from any replica, or via
  a load balancer with a health check).

**SQLite store** (`cronoq-store-sqlite`): the same tables in one local file (WAL journal, full
sync by default, owner-only permissions), for a single host.
- **Leader election**: a lease row (`holder`, `expires_unix_ns`) in `cronoq_leader`. A process
  takes the lease when it is free or expired (`BEGIN IMMEDIATE`, then `epoch + 1`); the leader
  renews it every `lease_renew` (2 s) for `lease_ttl` (10 s) and clears it on clean shutdown.
- **Fencing**: every commit reads the epoch and holder inside its write transaction; SQLite
  admits one writer at a time, so the check is exact.

**Sharding** (`[[shards]]`): the pools are split into shards, each served by its own leader,
standbys and store (a Postgres schema or SQLite file per shard, named from `{shard}`). One shared
configuration lists every shard's name, number (0–255), pools and endpoint; a process serves the
shard named by `--shard`.
- **Ids**: a sharded task id is `shard << 56 | epoch << 40 | seq`, so the owning shard is read
  from the id. A shard refuses a store that holds another shard's ids.
- **What cannot span shards**: dependencies, batches with tasks of several shards, and
  resources (each `[[resources]]` entry names its shard). Keys are unique per shard.
- **Clients** may call any server. Calls for another shard are forwarded to its endpoint with
  the caller's own bearer token, and authorized again there; forwarded calls carry
  `cronoq-forwarded` and are never forwarded again. Tokens are not forwarded over plain http to
  another host unless `server.insecure_peer_http`. Listings, `Watch` and snapshots cover every
  shard and name unreachable ones in `unavailable_shards`. Lookups by key ask every shard and
  refuse a key held by two; while a shard is down, get and wait by key still answer (naming the
  missing shard) and cancel by key is refused.
- **Calls that are not forwarded** (client certificates, unsafe endpoints, workers) get
  `FAILED_PRECONDITION` with `cronoq-shard` and `cronoq-shard-endpoint` metadata. Workers
  reconnect to that endpoint: never from https to http, with a token over plain http only to
  loopback unless the worker allows it, at most three times in a row, and back to their
  configured endpoint when the shard is unreachable.

---

## 14. Prototype defects fixed (for traceability)
1. Dependency check against the truncated completed list → in-degree counters plus a retention table.
2. Fixed-start conflicts are global → per-pool capacity reservations plus worker holds.
3. Recorded start is not the actual start → actual dispatch time is recorded.
4. Non-monotonic urgency → piecewise-linear monotonic urgency.
5. Quality computed over all history with the current worker count → windowed
   queue-pressure autoscaling.
6. Polling loops, O(n log n) refresh, O(n) blocked-task churn, O(n²) adherence →
   event-driven timers, lanes, parked waiters, online adherence.
7. No failures, retries, timeouts, cancellation, persistence or backpressure → §§9–13.
8. `remove_worker` ignores `min_workers` → policy clamps.

---

## 15. Observability (OpenTelemetry native, feature `otel`, default on)
- **Traces**: span `cronoq.task` (submit → terminal) with child spans `queue_wait` and
  `attempt{n}`. W3C `traceparent` is stored in the task tags and propagated to executors,
  remote workers (gRPC metadata) and the Python bindings.
- **Metrics** (OTel instruments; the server exports them in Prometheus format):
  `tasks_submitted/completed{pool,kind,class,status}`, `queue_wait_seconds`,
  `run_seconds`, `e2e_seconds` (histograms), `pending{pool,class}`, `workers{pool}`,
  `desired_workers{pool}`, `held_workers{pool}`, `resource_available{resource}`,
  `fixed_start_offset_seconds`, `priority_inversions_total`, `cost_total{pool}`.
- **Snapshot API** (`snapshot()`): the prototype's metrics, computed over a sliding window:
  - *priority adherence*: computed online at each dispatch. An inversion is counted if a
    ready, resource-eligible task of a strictly higher class was waiting in the same pool.
    O(classes).
  - *fixed-start hit rate*, *utilization* (busy slot-seconds / capacity slot-seconds),
    *wait p50/p90/p99* (HDR histogram), *schedule efficiency* (work / (capacity · window)).
- **Event stream**: `scheduler.events()` gives a broadcast of `StatusChanged` events; gRPC `Watch`.

---

## 16. Public APIs

### Rust
```rust
let sched = Scheduler::<Job, Output>::builder()
    .pool(Pool::new("default").workers(1..=8).executor(FnExecutor::new(|ctx, job| async move { … })))
    .resource("openai", Resource::token_bucket(50.0, 100))
    .build().await?;
let h = sched.submit(Task::new(job).priority(Priority::High).deadline_in(secs(30))).await?;
let out = h.await?;      // TaskHandle<O>: Future + cancel() + status() + id()
sched.snapshot();  sched.shutdown(Graceful { timeout }).await;
```
`cronoq::blocking::Scheduler` wraps the same API and owns a private runtime.

### Python (`pip install cronoq`)
```python
import cronoq
async def run(task: cronoq.TaskContext, payload): ...
sched = cronoq.Scheduler(pools=[cronoq.Pool("default", min=1, max=8, executor=run)])
await sched.start()
h = await sched.submit(payload, priority=cronoq.Priority.HIGH, deadline_in=30.0, key="job-1")
result = await h
sched.snapshot().priority_adherence
```
Python executors run on the caller's asyncio loop (pyo3-async-runtimes). Payloads and
results are arbitrary Python objects (`P = Py<PyAny>`). A migration shim
`cronoq.compat.ElasticPriorityScheduler` mirrors the prototype's API.
`cronoq.client` (an optional extra) is a pure-Python gRPC client for submitting tasks and
running remote workers in server mode.

### Go (`github.com/synaptiai/cronoq/sdks/go`)
```go
c, _ := cronoq.Dial("https://cronoq:7070", cronoq.Options{Token: token})
sub, _ := c.SubmitAndWait(ctx, cronoq.Task{Payload: body, Key: "job-1"}, 30*time.Second)
cronoq.RunWorker(ctx, cronoq.WorkerOptions{Target: "https://cronoq:7070", Pool: "default", Slots: 8, Token: wtoken},
	func(ctx context.Context, l *cronoq.Lease) ([]byte, error) { return handle(ctx, l.Payload) })
```
A gRPC client (generated stubs in `cronoqpb`) and a worker loop with slots, heartbeats, reconnects,
draining and shard redirects; the handler's context ends when the server aborts the attempt.

### TypeScript (`cronoq` on npm, `sdks/typescript`)
```ts
const c = connect("https://cronoq:7070", { token });             // cronoq/node; connectWeb in cronoq/web
const sub = await c.submitAndWait({ payload: { to: "ada" }, key: "job-1" }, 30);
await runWorker({ target: "https://cronoq:7070", pool: "default", slots: 8, token, signal }, (lease) => handle(lease.json()));
```
Connect-ES clients over gRPC (Node) or gRPC-Web (browsers; unary calls and `Watch`), and a Node
worker loop with the same behaviour as the Go one; the handler's `lease.signal` aborts when the
server aborts the attempt. Ids are `bigint`s.

### Server (`cronoq-server --config cronoq.toml`)
gRPC (`cronoq.v1`):
- `Tasks`: Submit, SubmitBatch, Cancel, Get, ListTasks (pages of live and finished tasks),
  Wait, Watch (stream).
- `Workers.Work` (bidirectional stream): Register{pool, slots, labels} → Lease ↔
  Heartbeat / Complete / Fail / Nack.
- `Admin`: Snapshot, AddRecurring, RemoveRecurring, ListWorkers, ListRecurring.

HTTP gateway (axum): `POST|GET /v1/tasks`, `POST /v1/tasks:batch`, `GET|DELETE /v1/tasks/{id}`,
`GET /v1/tasks/{id}/result`, the same under `/v1/tasks/by-key/{key}`, `GET /v1/snapshot`,
`GET /v1/workers`, `GET /v1/recurring`, `GET /metrics`, `GET /healthz`, `GET /readyz` (ready
only on the leader). Task ids are JSON
strings; errors are `{"error": ..., "code": ...}` with a matching status (504 for a wait that
timed out, 503 while standing by).
Auth: static bearer tokens (config) and optional mTLS. Config: TOML plus environment
overrides.

---

## 17. Testing and quality
- **Deterministic simulation tests (DST)**: seeded workload generators drive `Core` under
  virtual time (microtask bursts, LLM-like heavy tails, DAG fan-out/fan-in, cron, failures,
  lease loss, failovers). Each run checks invariants after every step:
  - no task is dispatched twice concurrently;
  - capacity ≥ busy + held;
  - dependencies are respected;
  - no starvation beyond the bound implied by the aging cap;
  - every reservation is admitted within its limit;
  - terminal states are final;
  - waiting tasks of the same class are dispatched in FIFO order.
- **proptest**: data structures (lanes, timer heap, reservation sweep, DAG cycle check).
- **The prototype's test suite ported** as acceptance tests (unit and integration scenarios).
- **Benchmarks**: criterion (core) plus a runtime throughput harness; CI fails on a >10%
  regression vs `main` or a missed absolute target.
- **Postgres** integration and failover tests (docker), Python `pytest` (asyncio).
- CI: fmt, clippy `-D warnings`, `cargo deny`, tests, Miri on core, MSRV 1.94,
  maturin wheels (manylinux, macOS, Windows).

---

## 18. Delivery plan (v0.1, built in this order)
1. Workspace skeleton, CI, license, README.
2. `cronoq-core`: model, lanes/EDF, timers, DAG, leases/retries, pools/resources,
   reservations/holds/cron, backpressure, autoscale policy, duration stats, metrics. DST suite.
3. `cronoq` runtime: driver, FnExecutor, SlotScaler, in-memory store, OTel, blocking API,
   benchmarks.
4. Python bindings plus the compat shim and the ported prototype tests.
5. `cronoq-proto` and `cronoq-server` (gRPC, gateway, remote executor, webhook scaler, auth).
6. `cronoq-store-postgres`, recovery, leader election and fencing, failover tests.
7. Documentation, examples (LLM rate-limited agent, DAG batch, microtask firehose), and
   hardening before going public.

## 19. Not yet built
Nothing from the 0.2.0 plan; see the issue tracker for later work.

## 20. Resolved defaults
- Reservable fraction of max workers for fixed tasks: **0.5** (per pool, configurable)
- Default overflow policy: library **Wait**, server **Reject** (gRPC `RESOURCE_EXHAUSTED` / HTTP 429)
- Default retry: **3 attempts**, exponential backoff 1 s ×2 up to 60 s, full jitter; `retry_after` hints honoured
- Server auth: **bearer tokens or mapped client certificates with roles** (client / worker / metrics / admin), optionally limited to pools, + optional **mTLS**
- Terminal status retention: **1 h / 1 M entries**

## 21. Implementation notes (v0.1)
Differences from, and clarifications of, the sections above:
- **Lanes** are sorted `VecDeque`s keyed by submission sequence. Fresh tasks append in O(1);
  retries and resource wake-ups use binary insertion. Deadline queues are `BTreeMap`s keyed
  by latest start time.
- **Aging is measured from submission.** A task released by its dependencies, or retried,
  keeps its original place in its class.
- **Hold lead time** is re-evaluated, not fixed at submission. The hold timer first fires
  at `at − max_hold` (or immediately if that is past). Each time it fires, it recomputes
  the lead from the current p90 of the pool's non-fixed work (or `default_duration`). If it
  is still early, it re-arms itself, halving the remaining gap. Recurring occurrences are
  materialized one at a time, `hold lead + tolerance` (capped by `max_hold`) before they
  start.
- **Shedding** considers queued ready tasks (lanes and deadline queues) and parked tasks
  waiting for a resource. `ShedLowest` evicts the newest such task by submission order, and
  `ShedOldest` evicts the oldest. Fixed-start tasks are never shed: they hold an admitted
  reservation, and their `MissPolicy` already defines what happens when they cannot run. The
  reservation conflict check runs before bounds, so a submission that is rejected for a
  conflict never sheds anything.
- **Abandoned `awaits`.** When a task waiting on an unknown key ends (time-out, cancel or
  skip), it is removed from that key's waiter list, and empty lists are dropped.
- **Recovery and dependencies.** A recovered live task whose parent (by id or key) is no
  longer known treats that edge as satisfied, because a live child implies the edge was
  satisfied before. Recurring schedules restore their overlap guard from the latest live
  recovered occurrence tagged `cronoq.recurring=<name>`.
- **Engine time** makes no assumption about its epoch. Rates and accounting are anchored on
  the first `advance` call, so `Time` may be Unix nanoseconds.
- **Cron occurrences** are computed at whole-second resolution. At a DST fall-back, a job
  at a fixed time inside the repeated hour runs once, and an hourly job runs every real hour,
  including both occurrences of the repeated local hour. Tests in
  `crates/cronoq-core/tests/review2.rs` pin this behaviour.
- **Recovery.** A task that was running when the leader died counts that attempt. If its
  retry budget is then exhausted, it is dead-lettered.
- **Time base.** Engine time is Unix-epoch nanoseconds (`epoch_unix_nanos = 0`), so stored
  timestamps are absolute and need no rebasing on recovery: a fixed start or recurring
  occurrence missed while no leader ran is handled by its miss policy. Instants beyond what
  the platform clock can represent saturate (they never fire "now"). A wall-clock step
  (NTP) shifts timers by the step, since the runtime reads a monotonic clock anchored to
  the system clock at start-up.
- **Durable acknowledgements.** `submit`, `enqueue`, `cancel`, `add_recurring` and
  `remove_recurring` reply only after the batch holding their mutation is committed;
  dispatches and status replies are also released after the commit. Aborts and capacity
  changes take effect immediately.
- **Store errors.** `StoreError::Unavailable` is transient: the commit is retried with
  capped exponential backoff while the engine keeps serving reads, for up to
  `max_store_outage` (default 5 min). `Other` errors are retried up to 5 times; `Fenced` is
  fatal at once. On a fatal error the scheduler stops and every waiting caller receives
  `Error::Store(..)`. Panics in executors, scalers and stores are caught and reported
  instead of killing the driver; an executor panic fails its task permanently (a panic in a
  `run_worker` handler is reported to the server as a retryable failure).
- **Shutdown.** Dropping every `Scheduler` handle shuts down with `drop_grace` (default
  5 s). `shutdown` may be called from several handles; all see the same result.
- **Outputs of earlier tasks.** Handles to tasks that already succeeded (duplicates,
  `Scheduler::handle`) read the output from a bounded in-memory cache (`output_cache`,
  default 1024) and then from `Store::load_output`; if neither has it the handle resolves to
  `TaskFailure::OutputUnavailable`.
- **Postgres store.** Each cluster lives in its own schema (`PgStoreConfig::schema`); the
  leader lock is the two-key advisory lock (namespace, schema OID), migrations take a
  separate lock and are versioned in `cronoq_schema_version`. Commits apply transitions of
  existing tasks before inserting new ones (a key released and reused in one batch is
  valid) and fold a new task's transitions into its insert. A task's message persists until
  a later transition replaces it. Issued ids are tracked by a high-water mark, so pruning
  never causes reuse. Recovery always loads finished tasks that live tasks depend on, and
  pruning keeps them. Undecodable rows are marked failed (quarantined); undecodable
  schedules are skipped with an error log. NUL characters in strings are stored as U+FFFD.
- **Worker RPC.** The worker stream RPC is `Workers.Work` (`Connect` collided with the
  generated client constructor).
- **Server pools** are always served by remote workers. Capacity is the total slots of the
  connected workers. The default scaler is `External` (desired count exported as a metric);
  `webhook` posts `{pool, desired}`.
- **Outputs** are cached in memory (bounded by count and bytes) and persisted in Postgres
  when it is configured. `Wait` on a finished task reads from the cache, then from the
  database.
- **`/metrics`** is rendered from the scheduler snapshot in Prometheus text format. On the
  main HTTP listener it requires the `metrics` (or `admin`) role; `server.metrics_addr` adds
  an unauthenticated listener for scrapers on a private interface. OpenTelemetry instruments
  use the globally installed meter provider.
- **Server security.** Listeners default to 127.0.0.1, and non-loopback listeners require
  credentials unless `--insecure-no-auth` is given. Requests are authenticated before their
  body is read. TLS applies to gRPC and HTTP; with a client CA, certificates are required and
  their DNS names can map to roles. Tokens are stored as SHA-256 digests and compared in
  constant time, and can be limited to pools (submissions, lookups, watches and workers). Tenants
  share capacity, not namespaces: keys and resources share one namespace per shard. Limits (`[limits]`) bound
  batch sizes, pending tasks, message and body sizes, string lengths, waits, concurrent
  requests and worker slots; HTTP/2 keepalive detects dead connections.
- **Worker protocol (v3).** `Register` carries `protocol_version` and the worker's labels;
  older workers are refused with the version they need. `Registered` returns the granted slots
  and the largest accepted message, and lease ids carry the leader epoch. Workers may `Nack` a lease (offered to
  another worker) or report `Cancelled`. On shutdown or loss of leadership the server sends
  `Shutdown` (with `not_leader`) and workers reconnect with backoff. A worker whose stream
  backs up (it stopped reading) is disconnected.
- **Benchmarks.** The wall-clock targets in §1 are checked by `.github/workflows/bench.yml`
  with 30% headroom for CI noise. For profiling, use instruction counts (callgrind) on the
  `prof` example.
- **Python.** Executors run on the event loop that called `start()`. Plain functions run in
  the loop's default executor. `Scheduler.events()` streams status events (terminal ones by
  default, every transition with `emit_transitions=True`).
