# Observability

cronoq tells you not only how busy it is, but how well it is scheduling: whether priorities are
respected, whether fixed starts are on time, and how long work waits. This guide shows how to
collect those signals; [Metrics and quality signals](../concepts/metrics.md) explains how to read
them.

## Prometheus (server)

`GET /metrics` serves Prometheus text built from the scheduler snapshot at scrape time. On the
main HTTP port it requires a token with the `metrics` (or `admin`) role:

```yaml
scrape_configs:
  - job_name: cronoq
    authorization: { credentials_file: /etc/prometheus/cronoq-metrics-token }
    static_configs: [{ targets: ["cronoq:7071"] }]
```

If your scraper cannot send a token, set `server.metrics_addr = "0.0.0.0:9090"` and scrape
that port instead; it serves `/metrics`, `/healthz` and `/readyz` without authentication or
TLS, so keep it on an internal network.

Standbys answer 503; scrape every replica and the leader's series are the live ones. Every
metric is listed in the [metrics reference](../reference/metrics.md).

## OpenTelemetry (library)

With the default `otel` feature, the `cronoq` crate records metrics through the **globally
installed** OpenTelemetry meter provider and propagates trace context through the global
text-map propagator. Configure both once at startup:

```rust,ignore
use opentelemetry::global;
use opentelemetry_sdk::{metrics::SdkMeterProvider, propagation::TraceContextPropagator};

global::set_text_map_propagator(TraceContextPropagator::new());
let meter_provider = SdkMeterProvider::builder()
    .with_periodic_exporter(opentelemetry_otlp::MetricExporter::builder().with_tonic().build()?)
    .build();
global::set_meter_provider(meter_provider);
// Build the scheduler after this, so its instruments use the provider.
```

Without a provider the instruments cost almost nothing. Disable the feature
(`default-features = false`) to drop the OpenTelemetry dependencies entirely.

### Traces

Each attempt runs inside a `tracing` span named `cronoq.task` with fields `task.id`,
`task.pool`, `task.kind` and `task.attempt`. When a task is submitted inside a span and a
propagator is installed, its W3C `traceparent` is stored as a tag; each attempt's span is then
parented to the submitter's trace (with `tracing-opentelemetry` installed), so retries show up
under the request that caused them. Remote workers receive the tag in
`lease.tags["traceparent"]` and can continue the trace.

## Snapshots and events

For dashboards inside your application, read snapshots directly:

```python
snap = await sched.snapshot()
llm = snap.pool("llm")
print(llm.capacity, llm.busy, llm.wait_p99, llm.priority_adherence, snap.totals["dead_lettered"])
```

and subscribe to status events for audit logs or user notifications:

```python
async with sched.events() as events:
    async for e in events:
        if e.status == "dead_lettered":
            await alert(e.id, e.message)
```

The server equivalents are `GET /v1/snapshot` / `Admin.Snapshot` and the `Tasks.Watch` stream.

## Key alerts

| Alert | Expression (Prometheus) | Why |
|---|---|---|
| Dead letters | `increase(cronoq_tasks_total{status="dead_lettered"}[10m]) > 0` | Tasks failing every retry. |
| Queue wait | `cronoq_pool_wait_seconds{quantile="0.99"} > 30` | Users or SLAs waiting. |
| Under-capacity | `cronoq_pool_desired_workers > cronoq_pool_workers` for 15 m | The scaler is not delivering workers. |
| No workers | `sum by (pool) (cronoq_pool_pending) > 0 and on (pool) cronoq_pool_workers_connected == 0` for 5 m | Work is queued and nothing can run it. |
| Fixed starts late | `cronoq_pool_fixed_start_hit_rate < 0.95` | Scheduled jobs starting late. |
| Shedding | `increase(cronoq_tasks_total{status="shed"}[5m]) > 0` | Overload is dropping work. |
| Lease expiries | `increase(cronoq_lease_expiries_total[10m]) > 0` | Workers are dying or not heartbeating. |
| No leader | `max(probe_success{job="cronoq-readyz"}) == 0` (blackbox probe of `/readyz` on every replica) | No replica is serving. |

## Logs

The server logs with `tracing` to stdout; set `RUST_LOG` (for example
`RUST_LOG=info,cronoq_server=debug`). Notable events: listening addresses, leadership acquired
or lost, worker registration and disconnection (with granted slots), store outages and retries,
failed scale requests, and configuration warnings such as short tokens or a database URL
without `sslmode=verify-full`.
