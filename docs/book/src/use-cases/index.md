# Use cases

Each page in this section starts from a concrete problem, shows a complete cronoq setup, and
lists the specific benefits you get compared with a plain queue. Not sure which one applies?
Start with [Benefits at a glance](../benefits.md), which maps symptoms to pages.

| Use case | Typical problem | Key cronoq features |
|---|---|---|
| [LLM and agent workloads](llm-agents.md) | Rate limits, 429 storms, unpredictable latency, expensive workers | Token buckets, `retry_after`, priorities, deadlines, autoscaling |
| [Interactive and batch work on shared capacity](priority-mixing.md) | User requests stuck behind batch jobs; batch jobs starved at peak | Priority classes, aging, per-class wait SLOs |
| [Deadline-driven work](deadlines.md) | SLAs missed although capacity existed earlier | Deadline urgency, EDF ordering, overdue escalation |
| [Scheduled jobs with guaranteed capacity](scheduled-jobs.md) | Cron jobs start late because workers are busy | Fixed starts, reservations, worker holds, miss policies, cron |
| [Pipelines and DAGs](pipelines.md) | Hand-rolled dependency tracking; downstream steps run on bad input | Keys, edge conditions, atomic batches, failure handlers |
| [GPUs and other scarce resources](scarce-resources.md) | Oversubscribed GPUs, connection pools or licences | Semaphores shared across pools; no head-of-line blocking |
| [High-throughput microtasks](microtasks.md) | Scheduler overhead dominates millisecond tasks | O(1) dispatch, bulk enqueue, ~1M decisions/s per core |
| [Polyglot worker fleets](worker-fleets.md) | Workers in several languages and machines | gRPC worker protocol, leases, heartbeats, HTTP API |
| [Cost-aware autoscaling](autoscaling.md) | Over-provisioned at night, saturated at noon | Queue-pressure autoscaling, stabilization, cost metrics, KEDA |
| [Ordered work per entity](ordered-work.md) | Events of one account, document or device run out of order or at the same time | Ordering keys, halt on failure |
| [Latest-wins updates](latest-wins.md) | Stale updates of the same thing use capacity; only the newest value matters | Replace by key, successors, the `superseded` count |
| [Idempotent producers](idempotent-producers.md) | Producers retry after timeouts; a key reused for different work goes unnoticed | Keys, fingerprints, `on_key_conflict = reject` |
| [Reprioritising live work](reprioritising.md) | A user now waits for work that was queued as background work | Update of a queued task, class floor of an ordering key |
| [Holding work without losing it](holding-work.md) | Maintenance windows, an unreachable downstream service | Pause and resume with a reason, release of an attempt |
| [Offline-first clients and embedded queues](embedded-queues.md) | A desktop or mobile app needs a send queue with priorities and per-entity order | Embedded C library with Go and Swift wrappers (experimental), clock resynchronisation |
| [Migrating from ElasticPriorityScheduler](migrating.md) | Existing code on the original Python scheduler | `cronoq.compat` drop-in |

Use cases combine freely: an LLM pipeline can use DAGs, rate limits, deadlines and autoscaling
at the same time, because all of them are properties of tasks and pools in a single engine.
There are a few exceptions, and each page names them under its trade-offs. For example, an
ordering key cannot be combined with a fixed start.
