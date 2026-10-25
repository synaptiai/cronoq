# cronoq — Specification

> Status: **Accepted; describes 0.2.0** · Owner: Daniel Bentes · License: MIT OR Apache-2.0
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

### Non-goals
- Several leaders for one pool. Each shard (a group of pools) has one leader with standbys
  (§13); shards run side by side, but a pool never has two leaders.
- General constraint solving or bin-packing. We provide pools, counted resources, and worker
  labels: a task can require labels, and runs only in a group of workers that has them.
- Exactly-once execution of arbitrary code. We provide at-least-once with leases, plus what
  effectively-once side effects need: attempt numbers that never repeat for a task (a fencing
  token), lease ids unique across leaders, stale reports ignored and counted, and key
  deduplication of finished tasks within a configurable window.
- Being a workflow engine (no durable step state or replay of user code).

### Performance targets
| Metric | Target | Checked in CI |
|---|---|---|
| Core engine submit → dispatch → complete cycles, 1 core | **≥ 1,000,000 /s** | yes, floor 700,000 /s |
| End-to-end through tokio runtime, no-op in-process tasks | **≥ 250,000 /s** | yes, floor 175,000 /s |
| p99 dispatch latency at 50% load (runtime) | **< 1 ms** | no (design target) |
| Engine memory per pending task (excluding payload) | **< 300 B** | no (design target) |

The two throughput rows are checked by `.github/workflows/bench.yml` on every push to `main` and
on manual runs. The CI floors are 30% below the targets to absorb noise on shared runners. The
latency and memory rows are not measured in CI. For profiling, use instruction counts
(callgrind) on the `prof` example.

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
| D7 | Autoscaling | Queue-pressure policy (rates, backlog, SLO, reservations), configured per pool; the `Scaler` that carries it out is pluggable |
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
| D21 | Scope | One release ships every component: core, runtime, Python, server, Postgres, HA |
| D22 | Name / repo | `cronoq` everywhere; `github.com/Synaptiai/cronoq` |
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
            │   timer: sleep_until(next)──▶│  submit/cancel/tick(now) │    │
            │                             │  poll_action() → Action  │    │
            │                             └───────┬──────────────────┘    │
            │   Actions: Dispatch / Abort / ScaleTo / Status / Persist / SpaceAvailable
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
| `cronoq-store-sqlite` | SQLite `Store` in one local file, with standby processes and fencing. |
| `cronoq-store-tests` | Shared conformance suite that every durable store runs. |
| `cronoq-proto` | Protobuf definitions and generated tonic code. |
| `cronoq-server` | `cronoq-server` binary: gRPC, HTTP gateway, remote executor, Prometheus, config. |
| `cronoq-cli` | `cronoq` command-line client for the server, including the live `cronoq top` view. |
| `cronoq-operator` | Kubernetes operator that scales worker Deployments to their pool's desired capacity. |
| `cronoq-py` | PyO3 bindings (built with maturin), plus a pure-Python gRPC worker client. |

### Time
- The core uses `Time(u64)`: nanoseconds. The core never reads a clock; every call carries
  `now`. It makes no assumption about the epoch: rates and accounting are anchored on the
  first call it receives.
- The runtime's `Clock` (a struct) makes engine time Unix-epoch nanoseconds: it reads the
  wall clock once at start and then advances with tokio's monotonic clock, so engine time never
  goes backwards. A later wall-clock step (NTP) is not followed; timers keep their monotonic
  positions.
- Wall-clock inputs (fixed starts, cron, deadlines given as timestamps) are converted at the
  API boundary. Because engine time is already Unix nanoseconds, stored timestamps are
  absolute and need no re-mapping on recovery: a fixed start or recurring occurrence missed
  while no leader ran is handled by its miss policy. Instants beyond what the platform clock
  can represent saturate (they never fire "now").
- Tests drive the runtime under tokio's paused time; the core is tested under virtual time
  directly.

---

## 4. Data model

```rust
pub struct TaskSpec<P> {
    pub payload: Arc<P>,
    pub kind: Arc<str>,                  // stats and duration learning per (pool, kind)
    pub pool: Arc<str>,                  // default "default"
    pub priority: Priority,              // Critical | High | Normal | Low
    pub key: Option<Arc<str>>,           // unique while live or retained; dedup, dependencies
    pub deadline: Option<Time>,          // soft: drives urgency
    pub fixed_start: Option<FixedStart>, // start at a time (see §6)
    pub duration_hint: Option<Duration>,
    pub timeout: Option<Duration>,       // per attempt
    pub retry: Option<RetryPolicy>,      // None: the pool's default
    pub deps: Vec<Dep>,                  // Dep { target: TaskRef, cond: EdgeCond }
    pub resources: SmallVec<[(Arc<str>, u32); 2]>, // (resource name, amount)
    pub tags: Vec<(Arc<str>, Arc<str>)>, // passed to executors, e.g. W3C traceparent
    pub labels: Vec<(Arc<str>, Arc<str>)>, // worker labels the task requires (§5.4)
    pub tenant: Option<Arc<str>>,        // for weighted fair sharing (§5.3)
}
pub enum TaskRef { Id(TaskId), Key(Arc<str>) }
pub enum EdgeCond { OnSuccess /*default*/, OnComplete, OnFailure }
pub struct FixedStart { pub at: Time, pub tolerance: Duration /*1 s*/, pub on_miss: MissPolicy }
pub enum MissPolicy { RunLate /*default*/, Skip, Fail }
```

- `TaskId(u64)` is engine-assigned and monotonic; in server mode it is prefixed with the
  leader epoch to stay unique across failovers.
- Submitting a `key` that is already live, or still in the retention table, returns
  `Submitted { id, duplicate: true }` with the existing task's id. This gives idempotent
  submit within the retention window.
- Tasks live in a slab arena (`u32` index + generation). Queues hold 8-byte keys.
  Cancelling a queued task removes it from its lane, deadline queue or fair queue directly;
  only resource and label waiter queues are cleaned lazily.

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
   (terminal)  (permanent err) (attempts out) (user/executor) (dep cond / fixed miss / overload)
```

Terminal statuses are retained in a compact table (`TaskId`/key → status, end time)
for `retention` (default 1 h, max 1 M entries). Entries are evicted oldest first by finish
time once they are older than `retention` or the table is over its size limit; lookups do
not refresh them. This serves late-arriving dependents, key deduplication and `get`.

Outputs (`O`) of succeeded tasks are kept in a bounded in-memory cache in the runtime
(`output_cache`, default 1024 entries) and, with a durable store, in the store
(`Store::load_output`). Handles to tasks that already succeeded (duplicate submissions,
`Scheduler::handle`) read the cache first and then the store; if neither has the output the
handle resolves to `TaskFailure::OutputUnavailable`. The same cache keeps the messages of tasks
that ended without succeeding, so a handle attached later resolves to `TaskFailure::Ended` with
the task's own message.

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

Aging is measured from submission. In a FIFO lane, a task released by its dependencies, or
retried, keeps its original place in its class. In a fair queue (§5.3) such a task gets a new
start tag when it becomes ready; only tasks woken from a resource or label waiter queue keep
their tag.

Per pool, per class:
- `lanes[c]`: a `VecDeque<(seq, Key)>` sorted by submission sequence, holding ready tasks
  with no deadline. Fresh tasks append in O(1); retries and resource wake-ups use binary
  insertion.
- `edf[c]: BTreeMap<(lst, seq), Key>`: ready tasks with a deadline, ordered by latest start
  time. Within a class, deadline tasks run in LST/EDF order, which is optimal for meeting
  deadlines.
- In a pool with tenant fairness (§5.3), a start-time fair queue takes the place of the lane.

Dispatch, when the pool has an unheld free slot:
1. Candidates are the heads of each class's queues (lane or fair queue, and deadline queue).
   Compute each one's full score lazily at `now`.
2. Take the maximum (ties: class, then seq) and remove it from its queue.
3. If no worker group with the task's required labels has a free slot, park it on the pool's
   label waiter queue for that label set and repeat (§5.4).
4. If it lacks resource permits or tokens (§9.2), park it on that resource's waiter queue
   and repeat.
5. Emit `Dispatch`.

Cost: O(classes) plus O(log n) for removing the head. No periodic work.

### 5.3 Tenant fairness
A pool with `fairness: Some(Fairness { weights, default_weight })` shares its capacity between
tenants (`TaskSpec::tenant`). Within each class, tasks without a deadline go to a start-time
fair queue instead of the FIFO lane: a task's start tag is the later of the class's virtual
time and its tenant's previous finish tag, and its finish tag adds `1e6 / weight`. Dispatching
in tag order gives each backlogged tenant dispatches in proportion to its weight. A tenant
with no waiting work does not bank credit, because its tags never fall behind the virtual
time. Higher classes still run first, and tasks with deadlines keep their EDF order. Tasks
without a tenant, and tenants not listed, use `default_weight` (1.0).

### 5.4 Worker labels
A pool's capacity is split into worker groups by label set (`set_group_capacity`; group 0 has
no labels). A task with `labels` runs only in a group whose labels include every required
`(name, value)` pair; tasks without labels run in any group, preferring the least specialized.
When the chosen task has no matching group with a free slot, it waits in a label waiter queue
without blocking the rest of the pool, and is moved back to its queue, oldest first, when a
matching group gains free slots. The label check comes before the resource check.

---

## 6. Fixed-start and recurring tasks

### 6.1 Admission (fixes prototype bug #2)
A reservation occupies the interval `[at − tolerance, at + d + tolerance]` in its pool.
Admission succeeds iff the maximum overlap with existing reservations, including the new one,
stays `≤ max(1, floor(pool.max_workers · reservable_fraction))` (default fraction 0.5). One
reservation is always allowed, so a pool with `max_workers = 1` can be fully reserved; with 2
or more workers and a fraction below 1, part of the pool stays free for ad-hoc work.
Reservations are kept in a `BTreeMap<(start, TaskId), end>`; the check sweeps the start and
end events of the reservations that overlap the new interval. On conflict:
`SubmitError::ReservationConflict`. The conflict check runs before bounds (§11), so a
submission rejected for a conflict never sheds anything.

### 6.2 Holding workers
At `hold_at = at − hold_lead`, where `hold_lead = min(max(tolerance, p90 duration of the
pool's non-fixed work), max_hold /*default 5 min*/)`:
- if a free slot exists, it becomes **held** for this reservation;
- otherwise the next slot that frees up in the pool is held (holds have priority over lanes).

The hold lead is re-evaluated, not fixed at submission. The hold timer first fires at
`at − max_hold` (or immediately if that is past). Each time it fires, it recomputes the lead
from the current p90 of the pool's non-fixed work (or `default_duration`). If it is still
early, it re-arms itself, halving the remaining gap.

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
fixed-start task one at a time, at `max(next − min(hold_lead + tolerance, max_hold), previous
occurrence's start)`, so at most one occurrence is materialized ahead. The template's
relative deadline applies from each occurrence's start, and its key is a prefix
(`<key>@<start in UTC, RFC 3339>`). `Queue` and `QueueOnSuccess` make each occurrence depend on
the previous one (`on_complete` / `on_success`); the predecessor's task id is persisted with the
schedule (`RecurringNext.last`), and adding the schedule again starts a new chain. Cron
definitions are persisted.

Cron occurrences are computed at whole-second resolution. At a DST fall-back, a job at a fixed
time inside the repeated hour runs once, and an hourly job runs every real hour, including
both occurrences of the repeated local hour. Tests in `crates/cronoq-core/tests/review2.rs`
pin this behaviour. After downtime, an occurrence already past its start plus tolerance runs
late once with `RunLate` (with `Skip` or `Fail` it is counted as missed), and the schedule
then resumes from the present instead of replaying every missed occurrence.

---

## 7. Dependencies (DAG)

- Dependencies are tracked by in-degree counters (`unmet`) and a reverse adjacency list
  of dependents. A parent's terminal transition decrements or skips its children in
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
- When a task waiting on an unknown key ends (time-out, cancel or skip), it is removed from
  that key's waiter list, and empty lists are dropped.

---

## 8. Durations

- `DurStats` per `(pool, kind)` and per pool: an EWMA mean plus one stochastic-approximation
  estimator per quantile (`QuantileEst`, p50 and p90; each sample moves the estimate up or
  down by a step scaled to the mean that decays to a floor, so it tracks drift), updated from
  observed successful runtimes. The learned estimate is the mean until there are 3 samples,
  then the p50.
- Estimate used for a task: `duration_hint`, else the `(pool, kind)` estimate, else the
  pool-wide learned estimate, else `pool.default_duration /*1s*/`.
- Used by: urgency (§5.1), reservation intervals and hold lead (§6), autoscaling (§10).
  It is **never** used to decide completion; completion is always observed.
- Timeout is separate. A timeout fires `Abort(lease)` and counts as a retryable failure.

---

## 9. Pools, resources, executors, scalers

### 9.1 Pools
```rust
pub struct PoolConfig {          // engine side (cronoq-core)
    pub name: Arc<str>,
    pub min_workers: u32,        // lower bound on the autoscaler's desired capacity
    pub max_workers: u32,        // upper bound; also sizes the reservation limit
    pub initial_workers: u32,    // capacity at start (at most max_workers)
    pub autoscale: Option<AutoscaleConfig>, // None: capacity changes only via set_capacity
    pub retry: RetryPolicy,      // default for tasks without their own
    pub bounds: Bounds,
    pub overflow: OverflowPolicy,
    pub reservable_fraction: f64,
    pub default_duration: Duration,
    pub max_hold: Duration,
    pub lease_ttl: Option<Duration>, // Some for remote workers that heartbeat
    pub unknown_dependency: UnknownDependency,
    pub fairness: Option<Fairness>,
}
// Runtime side (cronoq): Pool<P, O> wraps a PoolConfig with its executor and scaler.
Pool::new("default").workers(min, initial, max).executor(..).scaler(..)
```
A *worker* is one capacity slot. The core tracks `capacity`, `busy` and `held` counts per
pool, and capacity per worker group (§5.4). Capacity starts at
`min(initial_workers, max_workers)`; `min_workers` bounds only the desired capacity the
autoscaler asks for, so capacity may be below it (at start, or when remote workers report
fewer slots). Capacity may also drop below `busy`: running attempts continue, and no new work
is dispatched until busy slots fall below capacity.

### 9.2 Resources
```rust
pub struct ResourceConfig { pub name: Arc<str>, pub kind: ResourceKind }
pub enum ResourceKind { Semaphore { permits: u32 }, TokenBucket { rate: f64, burst: u32 } }
ResourceConfig::semaphore("gpu", 2);  ResourceConfig::token_bucket("openai", 50.0, 100);
```
Resources are shared across pools. A task acquires all its resources atomically at
dispatch; semaphores are released at the end of each attempt. A resource-starved task
waits on that resource's FIFO waiter queue, and is re-offered as a candidate when permits
are released or tokens refill (the core schedules a timer for the next refill).
This avoids head-of-line blocking.

### 9.3 Executor
```rust
pub trait Executor<P, O>: Send + Sync + 'static {
    fn execute(&self, ctx: TaskContext, payload: Arc<P>) -> BoxFuture<Result<O, TaskError>>;
    /// Called synchronously when the engine abandons an attempt, before any new dispatch.
    fn abort(&self, _lease: LeaseId) {}
}
// TaskContext: id, lease, attempt, pool, kind, key, tags, deadline, timeout, group (labels);
//              is_cancelled(), cancellation_token(), heartbeat(), tag(k), traceparent()
pub enum TaskError {
    Retryable { message: Arc<str>, retry_after: Option<Duration> },
    Permanent { message: Arc<str> },
    Cancelled,
}
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
Built in: `SlotScaler` (in-process: capacity = desired, immediately) and `ExternalScaler`
(capacity is managed elsewhere and reported with `Scheduler::set_capacity`; the desired count
is read from the snapshot or the `cronoq_pool_desired_workers` metric, for example by KEDA,
an HPA or the operator). The server adds a `WebhookScaler` that POSTs `{pool, desired}` to a
URL. For remote pools, capacity comes from registered worker streams; the scaler only
requests.

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
desired  = clamp(max(base + drain, slo, busy) + R(h), min_workers, max_workers)
```
- **Scale-up**: immediate, limited to `max_step_up` (default +100% or +4, whichever is larger).
- **Scale-down**: only to the maximum desired value seen over `stabilization_window`
  (default 60 s; same approach as Kubernetes HPA), at most `max_step_down` per
  `cooldown` (default 30 s).
- Cost: `worker_seconds × cost_per_worker_second` per pool, reported in metrics.
- The policy is fixed (`AutoscaleState::evaluate` in the core) and configured per pool by
  `AutoscaleConfig`; the pluggable part is the `Scaler` that carries out its decisions.

This replaces the prototype's makespan-based "quality" trigger (prototype bug #5).

---

## 11. Backpressure

`Bounds { max_pending: Option<usize>, max_pending_per_class: [Option<usize>; 4] }` limits
not-yet-running tasks. It is used for the whole engine (`CoreConfig::global_bounds`) and per
pool (`PoolConfig::bounds`); each pool's `overflow: OverflowPolicy` says what happens when a
submission would exceed them:
- `Wait`: async submit awaits space (default for the Rust API);
- `Reject`: `SubmitError::Overloaded`;
- `ShedLowest`: evict the newest task of the lowest class that is below the incoming
  task's class (→ `Shed`); otherwise reject;
- `ShedOldest`: evict the oldest Low-class task.

Shedding considers queued ready tasks (lanes, fair queues and deadline queues) and tasks
parked on a resource or label waiter queue, by submission order. Fixed-start tasks are never
shed: they hold an admitted reservation, and their `MissPolicy` already defines what happens
when they cannot run.

Waiters are woken by the engine when capacity frees up. Bounds are also visible to the
server as gRPC `RESOURCE_EXHAUSTED`.

---

## 12. Core (sans-IO) interface

```rust
impl<P> Core<P> {
    pub fn new(cfg: CoreConfig) -> Self;
    // One method per input; each takes `now` and queues the actions it causes.
    pub fn submit(&mut self, now: Time, spec: TaskSpec<P>) -> Result<Submitted, SubmitError>;
    pub fn try_submit(&mut self, now: Time, spec: TaskSpec<P>)
        -> Result<Submitted, (SubmitError, TaskSpec<P>)>;   // hands the spec back
    pub fn submit_batch(&mut self, now: Time, specs: Vec<TaskSpec<P>>)
        -> Result<Vec<Submitted>, (usize, SubmitError)>;
    pub fn complete(&mut self, now: Time, lease: LeaseId, outcome: Outcome) -> bool;
    pub fn heartbeat(&mut self, now: Time, lease: LeaseId) -> bool;
    pub fn cancel(&mut self, now: Time, target: &TaskRef) -> bool;
    pub fn set_capacity(&mut self, now: Time, pool: PoolId, capacity: u32);
    pub fn set_group_capacity(&mut self, now: Time, pool: PoolId, labels: &[(Arc<str>, Arc<str>)],
                              capacity: u32);
    pub fn add_recurring(&mut self, now: Time, spec: RecurringSpec<P>) -> Result<(), String>;
    pub fn remove_recurring(&mut self, now: Time, name: &str) -> bool;
    pub fn recover(&mut self, now: Time, rec: Recovered<P>);
    pub fn tick(&mut self, now: Time);                   // fire due timers
    pub fn poll_action(&mut self) -> Option<Action<P>>;  // drain after each call
    pub fn next_wakeup(&self) -> Option<Time>;           // earliest internal timer
    pub fn snapshot(&mut self, now: Time) -> Snapshot;
    // plus lookups: status, resolve, list_tasks, list_recurring, ...
}
pub enum Action<P> {
    Dispatch(Dispatch<P>),            // lease, id, pool, attempt, payload, kind, key, timeout, ...
    Abort { lease: LeaseId, id: TaskId, reason: AbortReason },
    ScaleTo { pool: PoolId, desired: u32 },
    Status(StatusEvent),
    Persist(Box<Mutation<P>>),        // only with CoreConfig::persist
    SpaceAvailable,                   // waiting submitters may retry after SubmitError::Full
}
```
Internal timers live in one binary heap of `(Time, Timer)` with lazy invalidation. `Timer`
covers retry backoff, hold checks, fixed starts, miss deadlines, await timeouts, attempt
timeouts, lease expiry, token refill, the autoscale tick and recurring occurrences.

### Runtime driver
- The engine task owns `Core`. Inputs arrive on a bounded mpsc and are drained with
  `recv_many` (batching amortizes wakeups). The task then `sleep_until(next_wakeup)`.
- `Dispatch` → `tokio::spawn(executor.execute(..))` with timeout and cancellation; the
  completion is sent back as an input.
- `Persist` mutations are group-committed to the `Store`. A submit is acknowledged only
  after its mutation is durable (with the in-memory store this is immediate). The engine
  does not wait for the store: actions that depend on durability are buffered until
  their commit succeeds.
- **Durable acknowledgements.** `submit`, `enqueue`, `cancel`, `add_recurring` and
  `remove_recurring` reply only after the batch holding their mutation is committed;
  dispatches and status replies are also released after the commit. Aborts and capacity
  changes take effect immediately.
- **Store errors.** `StoreError::Unavailable` is transient: the commit is retried with
  capped exponential backoff while the engine keeps serving reads, for up to
  `max_store_outage` (default 5 min). With other errors the scheduler fails on the
  5th commit that fails that way since the last successful commit; `Fenced` is fatal at once. On a fatal error the scheduler stops
  and every waiting caller receives `Error::Store(..)`.
- **Panics** in executors, scalers and stores are caught and reported instead of killing the
  driver; an executor panic fails its task permanently (a panic in a `run_worker` handler is
  reported to the server as a retryable failure).
- **Shutdown.** `shutdown(grace)` may be called from several handles; all see the same
  result. Dropping every `Scheduler` handle shuts down with `drop_grace` (default 5 s).

---

## 13. Persistence and HA

```rust
#[async_trait] pub trait Store<P, O>: Send + Sync + 'static {
    fn persists(&self) -> bool { true }                  // false: no mutations are produced
    async fn acquire_leadership(&self) -> Result<u16, StoreError>; // blocks; returns the epoch
    async fn release_leadership(&self) {}
    async fn commit(&self, epoch: u16, base: TimeBase, batch: &[Mutation<P>],
                    outputs: &[(TaskId, &O)]) -> Result<(), StoreError>; // fenced
    async fn recover(&self, base: TimeBase) -> Result<Recovered<P>, StoreError>;
    async fn load_output(&self, id: TaskId) -> Result<Option<O>, StoreError> { Ok(None) }
}
// What a server needs beyond the scheduler's use: lookups, listing, pruning, leadership.
#[async_trait] pub trait StoreQueries<O>: Send + Sync + 'static {
    async fn get_task(&self, target: &TaskRef) -> Result<Option<TaskRecord<O>>, StoreError>;
    async fn prune(&self, older_than: Duration) -> Result<u64, StoreError>;
    async fn list_finished(&self, pools: &[&str], statuses: &[TaskStatus],
                           after: Option<TaskId>, limit: usize)
        -> Result<Vec<TaskRecord<O>>, StoreError>;
    fn is_leader(&self) -> bool;
    async fn leadership_lost(&self);
}
```
`TimeBase { unix_nanos_at_zero }` converts engine time to wall-clock time; the runtime always
passes 0, since its engine time is Unix nanoseconds. Every durable store passes one shared
conformance suite (`cronoq-store-tests`): round trips of every mutation and field, time
rebasing, fencing and handover, key reuse within a batch, dependencies on pruned parents,
missed starts after restart, batching-independent messages, output loading, listing finished
tasks, id high-water marks after pruning, and restart without rerunning finished work.

**Recovery** (all stores): load non-terminal tasks, schedules, and finished tasks that are
within the store's retention window (default 1 h) or that live tasks depend on. A task that was `Running` when the leader died counts that attempt as
lease-lost and is retried according to its policy (at-least-once); if its retry budget is then
exhausted, it is dead-lettered. A recovered live task whose parent (by id or key) is no longer
known treats that edge as satisfied, because a live child implies the edge was satisfied
before. Recurring schedules restore their overlap guard from the latest live recovered
occurrence tagged `cronoq.recurring=<name>`.

**Postgres store** (`cronoq-store-postgres`): each cluster lives in its own schema
(`PgStoreConfig::schema`). Tables, created from `schema.sql` and upgraded by numbered
migrations recorded in `cronoq_schema_version` (migrations take a separate lock):
`cronoq_tasks` (one row per task: spec columns, `deps`, `resources`, `tags` and `labels` as
JSONB, tenant, status, attempt, timestamps, message, and the output inline), `cronoq_recurring`
(schedules) and `cronoq_leader(id, epoch, max_id)`.
- **Leader election**: `pg_try_advisory_lock` on a dedicated session, with the two-key lock
  (namespace, schema OID); on success the epoch is incremented (`UPDATE cronoq_leader …
  RETURNING epoch`). Epochs stay within 1..=32767 and wrap to 1 after 32767, so epoch-prefixed
  task ids fit in a signed `BIGINT`.
- **Fencing**: every commit reads the leader row in its transaction (`SELECT epoch … FOR
  SHARE`, or `UPDATE … SET max_id = GREATEST(max_id, $1) RETURNING epoch` when it inserts
  tasks) and compares the epoch in the application. On a mismatch the commit returns
  `StoreError::Fenced`, and the stale leader steps down.
- **Commits** apply transitions of existing tasks before inserting new ones (a key released
  and reused in one batch is valid) and fold a new task's transitions into its insert. A
  task's message persists until a later transition replaces it. Issued ids are tracked by the
  `max_id` high-water mark, so pruning never causes reuse, and pruning keeps finished tasks
  that live tasks depend on. Undecodable rows are marked failed (quarantined); undecodable
  schedules are skipped with an error log. NUL characters in strings are stored as U+FFFD.
- **Standbys** poll for the lock; on takeover they recover and resume. A standby server
  answers every call with `UNAVAILABLE`, closes each client connection about a second after
  accepting it, and reports `503` on `/readyz`. Clients reach the leader through a load
  balancer that routes on `/readyz`, or retry another replica; behind a balancer that routes
  to every replica, the closed connections make clients reconnect until they reach the
  leader. Remote workers reconnect with backoff.

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
- **Traces**: one `cronoq.task` span per attempt, with fields `task.id`, `task.pool`,
  `task.kind` and `task.attempt`. W3C `traceparent` travels as a task tag: submissions record
  the current context in the `traceparent` tag, the attempt span takes it as its parent,
  in-process executors read it with `TaskContext::traceparent()`, remote workers receive it
  in `Lease.tags["traceparent"]`, and Python executors see it in the context's tags.
- **OTel instruments** (meter `cronoq`, using the globally installed meter provider):
  counters `cronoq.tasks.submitted{pool}`, `cronoq.tasks.finished{pool,status}` and
  `cronoq.attempts{pool,kind,outcome}`; histogram `cronoq.attempt.duration{pool,kind,outcome}`
  (seconds); gauges `cronoq.pool.workers`, `cronoq.pool.desired_workers`,
  `cronoq.pool.busy_workers`, `cronoq.pool.held_workers`, `cronoq.pool.pending{pool,class}`
  and `cronoq.pool.cost`.
- **Server `/metrics`** is rendered from the scheduler snapshot in Prometheus text format
  (not from the OTel instruments). Per pool: `cronoq_pool_workers`,
  `cronoq_pool_desired_workers`, `cronoq_pool_busy_workers`, `cronoq_pool_held_workers`,
  `cronoq_pool_workers_connected`, `cronoq_pool_pending{class}`, `cronoq_pool_reservations`,
  `cronoq_pool_priority_adherence`, `cronoq_pool_fixed_start_hit_rate`,
  `cronoq_pool_utilization`, `cronoq_pool_schedule_efficiency`, `cronoq_pool_wait_seconds`,
  `cronoq_pool_cost_total` and `cronoq_pool_dispatched_window`. Server-wide:
  `cronoq_workers_connected`, `cronoq_live_tasks`, `cronoq_running_tasks`,
  `cronoq_pending_tasks`, `cronoq_tasks_total{status}`, `cronoq_tasks_submitted_total`,
  `cronoq_attempts_dispatched_total`, `cronoq_retries_total`, `cronoq_timeouts_total`,
  `cronoq_lease_expiries_total` and `cronoq_stale_completions_total`. On the main HTTP
  listener it requires the `metrics` (or `admin`) role; `server.metrics_addr` adds an
  unauthenticated listener for scrapers on a private interface.
- **Snapshot API** (`snapshot()`): the prototype's metrics, computed over a sliding window:
  - *priority adherence*: computed online at each dispatch. An inversion is counted if a
    ready, resource-eligible task of a strictly higher class was waiting in the same pool.
    O(classes).
  - *fixed-start hit rate*, *utilization* (busy slot-seconds / capacity slot-seconds),
    *wait p50/p90/p99* (log-linear histogram, about 9% resolution), *schedule efficiency*
    (work / (capacity · window)).
- **Event stream**: `scheduler.events()` gives a broadcast of `StatusEvent`s (terminal ones by
  default, every transition with `emit_transitions`); gRPC `Watch`.

---

## 16. Public APIs

### Rust
```rust
let sched = Scheduler::<Job, Output>::builder()
    .pool(Pool::new("default").workers(1, 1, 8).executor(FnExecutor::new(|ctx, job| async move { … })))
    .resource(ResourceConfig::token_bucket("openai", 50.0, 100))
    .build().await?;
let task = Task::new(job).priority(Priority::High).deadline_in(Duration::from_secs(30));
let h = sched.submit(task.resource("openai", 1)).await?;
let out = h.await?;      // TaskHandle<O>: Future<Output = Result<Arc<O>, TaskFailure>>
                         //   + id() + cancel() + is_duplicate()
let snap = sched.snapshot().await?;
sched.shutdown(Duration::from_secs(5)).await?;
```
`cronoq::BlockingScheduler`, from `Builder::build_blocking()`, wraps the same API and owns a
private runtime.

### Python (`pip install cronoq`)
```python
import cronoq
async def run(task: cronoq.TaskContext, payload): ...
sched = cronoq.Scheduler(pools=[cronoq.Pool("default", min=1, max=8, executor=run)])
await sched.start()
h = await sched.submit(payload, priority=cronoq.Priority.HIGH, deadline_in=30.0, key="job-1")
result = await h
(await sched.snapshot()).pool("default").priority_adherence
```
Python executors run on the event loop that called `start()` (pyo3-async-runtimes); plain
functions run in the loop's default executor. Payloads and results are arbitrary Python
objects (`P = Py<PyAny>`). `Scheduler.events()` streams status events (terminal ones by
default, every transition with `emit_transitions=True`). A migration shim
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

### TypeScript (`@synaptiai/cronoq` on GitHub Packages, `sdks/typescript`)
```ts
// connect from @synaptiai/cronoq/node; connectWeb from @synaptiai/cronoq/web
const c = connect("https://cronoq:7070", { token });
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
- `Workers.Work` (bidirectional stream): the worker sends `Register{pool, slots, worker_name,
  protocol_version, labels}`, then `Heartbeat`, `Complete`, `Fail`, `Nack`, `Cancelled` and
  `SetSlots`; the server sends `Registered`, `Lease`, `Abort` and `Shutdown`.
- `Admin`: Snapshot, AddRecurring, RemoveRecurring, ListWorkers, ListRecurring.

**Worker protocol** (version 3). Workers that speak an older version are refused with the
version they need. `Registered` returns the granted slots, the lease TTL and the largest
accepted message, and lease ids carry the leader epoch. A worker may `Nack` a lease (it is
offered to another worker without counting an attempt when possible) or report `Cancelled`.
On shutdown or loss of leadership the server sends `Shutdown` (with `not_leader`) and workers
reconnect with backoff. A worker whose stream backs up (it stopped reading) is disconnected.

**Server pools** are always served by remote workers. Capacity is the total slots of the
connected workers, so it is 0 until workers connect. A pool's `min` and `max` bound the
desired capacity that the autoscaler computes, which push scalers, KEDA and the operator act
on. The default scaler is `External` (the desired count is exported as a metric); `webhook`
posts `{pool, desired}`. Remote leases have a TTL (default 30 s) extended by heartbeats.

**Outputs** are cached in memory (bounded by count and bytes) and persisted in the store when
one is configured. `Wait` on a finished task reads from the cache, then from the store.

HTTP gateway (axum): `POST|GET /v1/tasks`, `POST /v1/tasks:batch`, `GET|DELETE /v1/tasks/{id}`,
`GET /v1/tasks/{id}/result`, the same under `/v1/tasks/by-key/{key}`, `GET /v1/snapshot`,
`GET /v1/workers`, `GET /v1/recurring`, `GET /metrics`, `GET /healthz`, `GET /readyz` (ready
only on the leader). Task ids are JSON
strings; errors are `{"error": ..., "code": ...}` with a matching status (504 for a wait that
timed out, 503 while standing by).
Auth: static bearer tokens (config) and optional mTLS. Config: an optional TOML file, named
by `--config` or `CRONOQ_CONFIG` (without one the server runs on built-in defaults); `--shard` or `CRONOQ_SHARD` picks the shard to serve. Secrets
are read from the environment variables that the config names (`token_env`, `url_env`; a
Postgres store with neither `url` nor `url_env` reads `DATABASE_URL`). Apart from `RUST_LOG`
(the log filter), there are no other environment overrides.

**Security.** Listeners default to 127.0.0.1, and non-loopback listeners require credentials
unless `--insecure-no-auth` is given. Requests are authenticated before their body is read.
TLS applies to gRPC and HTTP; with a client CA, certificates are required and their DNS names
can map to roles. Tokens are stored as SHA-256 digests and compared in constant time, and can
be limited to pools (submissions, lookups, watches and workers). Tenants share capacity, not
namespaces: keys and resources share one namespace per shard. Limits (`[limits]`) bound batch
sizes, pending tasks, message and body sizes, string lengths, waits, concurrent requests and
worker slots; HTTP/2 keepalive detects dead connections.

---

## 17. Testing and quality
- **Deterministic simulations** (`crates/cronoq-core/tests/sim.rs`, `fuzz.rs`): seeded
  generators drive `Core` under virtual time.
  - `sim.rs` runs mixed workloads over three pools: microtasks, LLM-like heavy-tailed
    durations and batch jobs, with transient and permanent failures, retries, timeouts,
    deadlines, fixed starts with every miss policy, token-bucket and semaphore resources,
    dependencies by key with every edge condition, awaits on keys defined later, shedding,
    cancellations, capacity changes and autoscaling. At the end it checks that every task
    reached a terminal status exactly once, that no child started before its parents ended,
    and that plain tasks of the same pool and class started in arrival order.
  - `fuzz.rs` interleaves submissions with every option, cancellations, capacity changes and
    snapshots with time, and checks at the end that nothing is stuck: no live tasks, holds,
    reservations, pending counts or awaited keys.
  - Both call `Core::check_invariants` after every step. It checks that internal state is
    consistent: each running task has exactly one lease, busy, held, pending and per-class
    counts match the tasks, every queued or waiting task is in exactly the queue its location
    names, every live fixed-start task keeps its reservation, and unmet dependency counts match
    live parents plus awaited keys.
- **Scenario tests** in `crates/cronoq-core/tests` cover recurring schedules (including DST
  transitions), fairness, worker labels, listing and regressions; the server's tests cover
  failover (`ha.rs`), sharding, routing, TLS and gRPC-Web.
- **proptest**: the lane (against a sorted-map model) and the reservation sweep (against a
  brute-force overlap count).
- **The prototype's test suite ported** as Python acceptance tests (`test_compat.py`).
- **Benchmarks**: criterion benches for the core and the runtime. `bench.yml` fails when either
  throughput falls below its absolute floor (§1); there is no comparison against earlier runs.
- **Postgres** integration and failover tests start throwaway servers from the PostgreSQL 16
  binaries preinstalled on the CI image. Python tests run with `pytest` (asyncio).
- CI: fmt, clippy and rustdoc with `-D warnings`, `cargo deny`, tests, MSRV 1.94, Go and
  TypeScript SDK checks, a Helm end-to-end test on kind, and a wheel built with maturin and
  tested from a clean environment on Linux, macOS (Intel and Apple silicon) and Windows.

---

## 18. Resolved defaults
- Reservable fraction of max workers for fixed tasks: **0.5** (per pool, configurable)
- Default overflow policy: library **Wait**, server **Reject** (gRPC `RESOURCE_EXHAUSTED` / HTTP 429)
- Default retry: **3 attempts**, exponential backoff 1 s ×2 up to 60 s, full jitter; `retry_after` hints honoured
- Server auth: **bearer tokens or mapped client certificates with roles** (client / worker / metrics / admin), optionally limited to pools, + optional **mTLS**
- Terminal status retention: **1 h / 1 M entries**
