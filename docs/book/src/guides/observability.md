# Observability

## Prometheus (server)

`GET /metrics` on the HTTP port serves Prometheus text built from the scheduler snapshot. It is
not authenticated, so expose the port only to your monitoring network.

```yaml
scrape_configs:
  - job_name: cronoq
    static_configs: [{ targets: ["cronoq:7071"] }]
```

Every metric is listed in the [metrics reference](../reference/metrics.md).

## OpenTelemetry (library)

With the default `otel` feature, the `cronoq` crate records metrics through the **globally
installed** OpenTelemetry meter provider and propagates trace context through the global text-map
propagator. Configure both once at startup:

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

### Traces

Each attempt runs inside a `tracing` span named `cronoq.task` with fields `task.id`,
`task.pool`, `task.kind` and `task.attempt`. When a task is submitted inside a span and a
propagator is installed, its W3C `traceparent` is stored as a tag; the attempt's span is then
parented to the submitter's trace (with `tracing-opentelemetry` installed), and remote workers
receive the tag in `lease.tags["traceparent"]` so they can continue the trace.

## Key alerts

| Alert | Expression (Prometheus) | Why |
|---|---|---|
| Dead letters | `increase(cronoq_tasks_total{status="dead_lettered"}[10m]) > 0` | Tasks failing every retry. |
| Queue wait | `cronoq_pool_wait_p99_seconds > 30` | Users or SLAs waiting. |
| Under-capacity | `cronoq_pool_desired_workers > cronoq_pool_workers` for 10 m | Scaler not delivering workers. |
| No workers | `cronoq_workers_connected == 0` | Nothing can run (server mode). |
| Fixed starts late | `cronoq_pool_fixed_start_hit_rate < 0.95` | Scheduled jobs starting late. |
| Shedding | `increase(cronoq_tasks_total{status="shed"}[5m]) > 0` | Overload is dropping work. |
| No leader | `max(probe_success{job="cronoq-readyz"}) == 0` (blackbox probe of `/readyz` on every replica) | No replica is serving. |

## Logs

The server logs with `tracing` to stdout; set `RUST_LOG` (e.g. `RUST_LOG=info,cronoq=debug`).
Important events: worker registration and disconnection, leadership acquired, store commit
retries, scale requests that failed.
