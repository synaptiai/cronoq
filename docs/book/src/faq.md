# FAQ

### Is execution exactly-once?

No, at-least-once. An attempt can run again if its worker crashes, misses heartbeats, or the
leader fails over mid-attempt. Make handlers idempotent; use task keys to make *submission*
idempotent.

### Does a deadline cancel the task when it passes?

No. Deadlines raise urgency; they never cancel. Use `timeout` to bound an attempt, or cancel the
task yourself.

### What is the difference between a deadline and a fixed start?

A deadline says when work should be *finished* and lets cronoq reorder work to meet it. A fixed
start says when work should *begin*; cronoq reserves capacity and holds a worker for it.

### Can low-priority work starve?

Not behind `normal` traffic: aging lets a waiting `low` task overtake fresh `normal` work after
about 45 seconds with the default settings. It can wait behind sustained `high` or `critical`
load, by design. Raise `age_cap` for stronger mixing.

### How many tasks can the engine hold?

Memory per pending task is a few hundred bytes plus its payload. Millions of pending tasks are
practical; use `max_pending` to bound them.

### Can I use cronoq without tokio?

From Rust, use `BlockingScheduler` (it owns a private tokio runtime), or drive `cronoq-core`
yourself. From Python, the bindings manage their own runtime.

### Do I need Postgres?

Only for durability across restarts and for running several server replicas. Embedded use and
single-node servers work in memory.

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

Not yet. Use `/v1/snapshot`, the `Admin` service, Prometheus/Grafana and status events.
