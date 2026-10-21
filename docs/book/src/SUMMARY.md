# Summary

[Introduction](introduction.md)
[Why cronoq](why.md)

# Getting started

- [Rust quickstart](guides/rust-quickstart.md)
- [Python quickstart](guides/python-quickstart.md)
- [Server quickstart](guides/server-quickstart.md)

# Use cases

- [Overview](use-cases/index.md)
  - [LLM and agent workloads](use-cases/llm-agents.md)
  - [Interactive and batch work on shared capacity](use-cases/priority-mixing.md)
  - [Deadline-driven work](use-cases/deadlines.md)
  - [Scheduled jobs with guaranteed capacity](use-cases/scheduled-jobs.md)
  - [Pipelines and DAGs](use-cases/pipelines.md)
  - [GPUs and other scarce resources](use-cases/scarce-resources.md)
  - [High-throughput microtasks](use-cases/microtasks.md)
  - [Polyglot worker fleets](use-cases/worker-fleets.md)
  - [Cost-aware autoscaling](use-cases/autoscaling.md)
  - [Migrating from ElasticPriorityScheduler](use-cases/migrating.md)

# Concepts

- [Tasks and their lifecycle](concepts/tasks.md)
- [Priorities and aging](concepts/priorities.md)
- [Deadlines and urgency](concepts/deadlines.md)
- [Fixed starts and reservations](concepts/fixed-starts.md)
- [Recurring schedules](concepts/recurring.md)
- [Dependencies and keys](concepts/dependencies.md)
- [Retries, timeouts and leases](concepts/reliability.md)
- [Resources: semaphores and rate limits](concepts/resources.md)
- [Pools and autoscaling](concepts/autoscaling.md)
- [Backpressure](concepts/backpressure.md)
- [Metrics and quality signals](concepts/metrics.md)
- [Durability and high availability](concepts/durability.md)

# Guides

- [Writing executors and workers](guides/workers.md)
- [Using the HTTP API](guides/http-api.md)
- [Deploying the server](guides/deployment.md)
- [Observability](guides/observability.md)
- [Security hardening](guides/security.md)
- [Performance tuning](guides/performance.md)
- [Testing your executors](guides/testing.md)

# Reference

- [Server configuration](reference/configuration.md)
- [HTTP API](reference/http.md)
- [gRPC API](reference/grpc.md)
- [Python API](reference/python.md)
- [Rust API](reference/rust.md)
- [Metrics](reference/metrics.md)
- [Errors and status codes](reference/errors.md)

# Internals

- [Architecture](architecture.md)

---

[FAQ](faq.md)
[Glossary](glossary.md)
[Changelog](changelog.md)
