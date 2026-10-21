# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, minor versions may
contain breaking changes; they are called out under **Changed**.

## [Unreleased]

## [0.1.0] - Unreleased

First public release.

### Added

- **Engine (`cronoq-core`)**: sans-IO scheduling engine with four priority classes and aging,
  deadline urgency with earliest-deadline-first ordering, fixed-start reservations with worker
  holds and miss policies, cron schedules with time zones, DAG dependencies with
  success/failure/completion edges, idempotent keys, atomic batches, leases, heartbeats,
  timeouts, retries with backoff and `retry_after`, dead-lettering, cancellation, semaphores and
  token buckets, bounded queues with wait/reject/shed policies, queue-pressure autoscaling with
  stabilization and cost tracking, duration learning, windowed quality metrics, and recovery
  from a durable store.
- **Runtime (`cronoq`)**: tokio driver with an async API (`Scheduler`, `TaskHandle`), executors
  from closures, pluggable scalers and stores, durable acknowledgements, OpenTelemetry metrics
  and trace propagation (`otel` feature), and a blocking facade.
- **Postgres store (`cronoq-store-postgres`)**: durable state, advisory-lock leader election,
  epoch fencing, recovery, output storage and pruning.
- **Server (`cronoq-server`)**: gRPC (`Tasks`, `Workers`, `Admin`) and HTTP/JSON APIs, remote
  workers over bidirectional streams, bearer-token roles, optional mTLS, webhook and external
  scalers, Prometheus metrics, health and readiness endpoints, active-passive HA, and a Rust
  worker client.
- **Python (`cronoq` on PyPI)**: asyncio-native bindings, `cronoq.compat.ElasticPriorityScheduler`
  drop-in, and `cronoq.client` (gRPC client and remote worker).
- Documentation book, examples, Docker image, CI with deterministic simulation and benchmark
  gates, and a release pipeline for crates.io, PyPI, GitHub releases and GHCR.

[Unreleased]: https://github.com/synaptiai/cronoq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/synaptiai/cronoq/releases/tag/v0.1.0
