//! Metrics flow into the globally installed OpenTelemetry meter provider.
#![cfg(feature = "otel")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use cronoq::*;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

#[tokio::test]
async fn exports_task_metrics() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let s = Scheduler::<u64, u64>::builder()
        .pool(
            Pool::fixed("default", 2)
                .executor_fn(|c, x: Arc<u64>| async move {
                    if *x == 3 {
                        Err(TaskError::permanent("no"))
                    } else if *x == 4 && c.attempt <= 2 {
                        // Two releases; the pool allows one, so the second is a failure.
                        Err(TaskError::release(Duration::ZERO))
                    } else {
                        Ok(*x)
                    }
                })
                .configure(|p| {
                    p.max_releases = 1;
                    p.retry.initial_backoff = Duration::from_millis(1);
                    p.retry.jitter = false;
                }),
        )
        .build()
        .await
        .unwrap();
    for i in 0..5 {
        let _ = s.submit(Task::new(i)).await.unwrap().await;
    }
    tokio::time::sleep(Duration::from_millis(1100)).await; // gauges refresh every second
    provider.force_flush().unwrap();
    let names: BTreeSet<String> = exporter
        .get_finished_metrics()
        .unwrap()
        .iter()
        .flat_map(|rm| {
            rm.scope_metrics()
                .flat_map(|sm| sm.metrics().map(|m| m.name().to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    for expected in [
        "cronoq.tasks.submitted",
        "cronoq.tasks.finished",
        "cronoq.attempts",
        "cronoq.attempt.duration",
        "cronoq.pool.workers",
        "cronoq.pool.pending",
    ] {
        assert!(names.contains(expected), "missing {expected} in {names:?}");
    }
    // Attempts are counted by how they ended. Task 4 was released once (accepted), released
    // again (over the limit: an error) and then ran.
    let outcomes: BTreeSet<String> = exporter
        .get_finished_metrics()
        .unwrap()
        .iter()
        .flat_map(|rm| {
            rm.scope_metrics()
                .flat_map(|sm| sm.metrics().filter(|m| m.name() == "cronoq.attempts"))
                .map(|m| format!("{m:?}"))
                .collect::<Vec<_>>()
        })
        .flat_map(|dump| {
            ["ok", "error", "released"]
                .into_iter()
                .filter(|o| dump.contains(&format!("\"{o}\"")))
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        outcomes,
        ["error", "ok", "released"]
            .map(String::from)
            .into_iter()
            .collect()
    );
    assert_eq!(s.snapshot().await.unwrap().totals.releases, 1);
    s.shutdown(Duration::from_secs(1)).await.unwrap();
}
