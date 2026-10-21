# cronoq — Specification v0.1

> Status: **Accepted (v0.1 baseline)** · Owner: Daniel Bentes · License: MIT OR Apache-2.0
> Origin: rewrite of the Python prototype in `docs/prototype/` (Elastic Priority Scheduler).

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
- Active-active multi-leader scheduling (we provide active-passive HA).
- Label/constraint bin-packing (we provide pools + counted resources).
- Effectively-once execution (we provide at-least-once with leases; idempotency keys come later).
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
non-fixed work, capped by max_hold /*default 5 min*/)`:
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
`Scheduler::schedule_recurring(name, cron_expr, tz, template, overlap: Skip|Allow|Queue)`.
Each occurrence is materialized as a fixed-start task when the previous one fires
(look-ahead of 1 occurrence). Cron definitions are persisted.

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
```
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

### Server (`cronoq-server --config cronoq.toml`)
gRPC (`cronoq.v1`):
- `TaskService`: Submit, SubmitBatch, Cancel, Get, Watch (stream).
- `WorkerService.Connect` (bidirectional stream): Register{pool, slots} → Lease ↔
  Heartbeat / Complete / Fail.
- `AdminService`: ListPools, Snapshot, SetPoolBounds, Pause/Resume pool.

HTTP gateway (axum): `POST /v1/tasks`, `POST /v1/tasks:batch`, `GET|DELETE /v1/tasks/{id}`,
`GET /v1/tasks/{id}/result`, the same under `/v1/tasks/by-key/{key}`, `GET /v1/snapshot`,
`GET /metrics`, `GET /healthz`, `GET /readyz` (ready only on the leader). Task ids are JSON
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
- CI: fmt, clippy `-D warnings`, `cargo deny`, tests, Miri on core, MSRV 1.85,
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

## 19. Deferred (post v0.1)
Effectively-once (idempotent completion), multi-tenant weighted fairness,
label/constraint matching, SQLite store, Go/TS SDKs, active-active sharding,
Kubernetes operator.

## 20. Resolved defaults
- Reservable fraction of max workers for fixed tasks: **0.5** (per pool, configurable)
- Default overflow policy: library **Wait**, server **Reject** (gRPC `RESOURCE_EXHAUSTED` / HTTP 429)
- Default retry: **3 attempts**, exponential backoff 1 s ×2 up to 60 s, full jitter; `retry_after` hints honoured
- Server auth: **bearer tokens with roles** (client / worker / admin) + optional **mTLS**
- Terminal status retention: **1 h / 1 M entries**

## 21. Implementation notes (v0.1)
Differences from, and clarifications of, the sections above:
- **Lanes** are sorted `VecDeque`s keyed by submission sequence. Fresh tasks append in O(1);
  retries and resource wake-ups use binary insertion. Deadline queues are `BTreeMap`s keyed
  by latest start time.
- **Aging is measured from submission.** A task released by its dependencies, or retried,
  keeps its original place in its class.
- **Hold lead time** is computed when a fixed task is submitted, from the duration
  statistics known at that point (pool p90 or `default_duration`). Recurring occurrences are
  materialized one at a time, `hold lead + tolerance` (capped by `max_hold`) before they
  start.
- **Recovery.** A task that was running when the leader died counts that attempt. If its
  retry budget is then exhausted, it is dead-lettered.
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
  constant time, and can be limited to pools (submissions, lookups, watches and workers). The
  server is single-tenant: keys and resources share one namespace. Limits (`[limits]`) bound
  batch sizes, pending tasks, message and body sizes, string lengths, waits, concurrent
  requests and worker slots; HTTP/2 keepalive detects dead connections.
- **Worker protocol (v2).** `Register` carries `protocol_version`; `Registered` returns the
  granted slots and the largest accepted message. Workers may `Nack` a lease (offered to
  another worker) or report `Cancelled`. On shutdown or loss of leadership the server sends
  `Shutdown` (with `not_leader`) and workers reconnect with backoff. A worker whose stream
  backs up (it stopped reading) is disconnected.
- **Benchmarks.** The wall-clock targets in §1 are checked by `.github/workflows/bench.yml`
  with 30% headroom for CI noise. For profiling, use instruction counts (callgrind) on the
  `prof` example.
- **Python.** Executors run on the event loop that called `start()`. Plain functions run in
  the loop's default executor. Event subscription is not yet exposed in Python.

### Known limitations (tracked for v0.2)
- `cancel`, `add_recurring` and `remove_recurring` reply before their mutation is durable.
  Submissions already wait for the commit. After a failover, a cancel acknowledged just before
  the crash may be lost (the task then runs).
- `awaits` entries for keys that never appear are kept after their waiters time out or are
  cancelled. Memory grows only with distinct abandoned keys.
- Shedding considers ready tasks in lanes and deadline queues only. Parked (resource-blocked)
  and due fixed-start tasks are never shed.
- A submission can shed victims and then still be rejected with `ReservationConflict`,
  because bounds are checked before reservations.
- Recurring occurrences recovered after a failover lose their overlap guard for the
  occurrence that was in flight.
- Hold lead time is fixed when a task is submitted (see above). Very long tasks submitted
  before any duration statistics exist may start late.
