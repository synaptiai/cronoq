//! Tracing spans, trace-context propagation and OpenTelemetry metrics.
//!
//! cronoq uses the globally installed OpenTelemetry meter provider and text-map propagator,
//! so it reports into whatever pipeline the application configured. Task spans are `tracing`
//! spans; with `tracing-opentelemetry` installed they become OTel spans parented to the
//! submitter's trace (carried in the `traceparent` tag).

use std::sync::{Arc, Mutex};

use cronoq_core::{Snapshot, TaskStatus};

/// W3C `traceparent` of the current span, if a propagator is installed.
pub(crate) fn current_traceparent() -> Option<String> {
    #[cfg(feature = "otel")]
    {
        use std::collections::HashMap;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let span = tracing::Span::current();
        if span.is_disabled() {
            // No subscriber or no active span: there is no trace context to propagate.
            return None;
        }
        let cx = span.context();
        let mut carrier: HashMap<String, String> = HashMap::new();
        opentelemetry::global::get_text_map_propagator(|p| p.inject_context(&cx, &mut carrier));
        carrier.remove("traceparent")
    }
    #[cfg(not(feature = "otel"))]
    {
        None
    }
}

pub(crate) fn task_span(
    id: u64,
    pool: &str,
    kind: &str,
    attempt: u32,
    traceparent: Option<&str>,
) -> tracing::Span {
    let span = tracing::info_span!(
        "cronoq.task",
        task.id = id,
        task.pool = pool,
        task.kind = kind,
        task.attempt = attempt
    );
    #[cfg(feature = "otel")]
    if let Some(tp) = traceparent {
        use std::collections::HashMap;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let mut carrier: HashMap<String, String> = HashMap::new();
        carrier.insert("traceparent".into(), tp.into());
        let cx = opentelemetry::global::get_text_map_propagator(|p| p.extract(&carrier));
        let _ = span.set_parent(cx);
    }
    #[cfg(not(feature = "otel"))]
    let _ = traceparent;
    span
}

/// Latest per-pool gauges, read by observable instruments.
#[derive(Clone, Debug, Default)]
pub(crate) struct Gauges {
    pub pools: Vec<(Arc<str>, PoolGauge)>,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) struct PoolGauge {
    pub workers: u64,
    pub desired: u64,
    pub busy: u64,
    pub held: u64,
    pub pending: [u64; 4],
    pub cost: f64,
}

pub(crate) struct Metrics {
    pub gauges: Arc<Mutex<Gauges>>,
    #[cfg(feature = "otel")]
    inner: otel::Instruments,
}

impl Metrics {
    pub fn new() -> Self {
        let gauges = Arc::new(Mutex::new(Gauges::default()));
        Metrics {
            #[cfg(feature = "otel")]
            inner: otel::Instruments::new(gauges.clone()),
            gauges,
        }
    }

    #[inline]
    pub fn task_finished(&self, pool: &Arc<str>, status: TaskStatus) {
        #[cfg(feature = "otel")]
        self.inner.finished(pool, status);
        #[cfg(not(feature = "otel"))]
        let _ = (pool, status);
    }

    #[inline]
    pub fn attempt_finished(&self, pool: &Arc<str>, kind: &Arc<str>, secs: f64, ok: bool) {
        #[cfg(feature = "otel")]
        self.inner.attempt(pool, kind, secs, ok);
        #[cfg(not(feature = "otel"))]
        let _ = (pool, kind, secs, ok);
    }

    #[inline]
    pub fn submitted(&self, pool: &Arc<str>) {
        #[cfg(feature = "otel")]
        self.inner.submitted(pool);
        #[cfg(not(feature = "otel"))]
        let _ = pool;
    }

    pub fn update(&self, snap: &Snapshot) {
        let mut g = self.gauges.lock().unwrap();
        g.pools = snap
            .pools
            .iter()
            .map(|p| {
                let mut pending = [0u64; 4];
                for (i, v) in p.pending_by_class.iter().enumerate() {
                    pending[i] = *v as u64;
                }
                (
                    p.name.clone(),
                    PoolGauge {
                        workers: p.capacity as u64,
                        desired: p.desired as u64,
                        busy: p.busy as u64,
                        held: p.held as u64,
                        pending,
                        cost: p.cost,
                    },
                )
            })
            .collect();
    }
}

#[cfg(feature = "otel")]
mod otel {
    use std::sync::{Arc, Mutex};

    use cronoq_core::{Priority, TaskStatus};
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableGauge};

    use super::Gauges;

    pub struct Instruments {
        submitted: Counter<u64>,
        finished: Counter<u64>,
        attempts: Counter<u64>,
        run: Histogram<f64>,
        _gauges: Vec<ObservableGauge<u64>>,
        _cost: ObservableGauge<f64>,
    }

    fn gauge(
        meter: &Meter,
        name: &'static str,
        desc: &'static str,
        gauges: Arc<Mutex<Gauges>>,
        f: fn(&super::PoolGauge) -> u64,
    ) -> ObservableGauge<u64> {
        meter
            .u64_observable_gauge(name)
            .with_description(desc)
            .with_callback(move |obs| {
                for (pool, g) in &gauges.lock().unwrap().pools {
                    obs.observe(f(g), &[KeyValue::new("pool", pool.clone())]);
                }
            })
            .build()
    }

    impl Instruments {
        pub fn new(gauges: Arc<Mutex<Gauges>>) -> Self {
            let meter = opentelemetry::global::meter("cronoq");
            let pending_g = gauges.clone();
            let pending = meter
                .u64_observable_gauge("cronoq.pool.pending")
                .with_description("Tasks waiting to run, by priority class")
                .with_callback(move |obs| {
                    for (pool, g) in &pending_g.lock().unwrap().pools {
                        for c in Priority::ALL {
                            obs.observe(
                                g.pending[c.index()],
                                &[
                                    KeyValue::new("pool", pool.clone()),
                                    KeyValue::new("class", c.name()),
                                ],
                            );
                        }
                    }
                })
                .build();
            let cost_g = gauges.clone();
            let cost = meter
                .f64_observable_gauge("cronoq.pool.cost")
                .with_description("Accumulated worker cost")
                .with_callback(move |obs| {
                    for (pool, g) in &cost_g.lock().unwrap().pools {
                        obs.observe(g.cost, &[KeyValue::new("pool", pool.clone())]);
                    }
                })
                .build();
            Instruments {
                submitted: meter
                    .u64_counter("cronoq.tasks.submitted")
                    .with_description("Tasks admitted")
                    .build(),
                finished: meter
                    .u64_counter("cronoq.tasks.finished")
                    .with_description("Tasks reaching a terminal status")
                    .build(),
                attempts: meter
                    .u64_counter("cronoq.attempts")
                    .with_description("Finished attempts")
                    .build(),
                run: meter
                    .f64_histogram("cronoq.attempt.duration")
                    .with_unit("s")
                    .with_description("Attempt run time")
                    .build(),
                _gauges: vec![
                    gauge(
                        &meter,
                        "cronoq.pool.workers",
                        "Current worker capacity",
                        gauges.clone(),
                        |g| g.workers,
                    ),
                    gauge(
                        &meter,
                        "cronoq.pool.desired_workers",
                        "Autoscaler target",
                        gauges.clone(),
                        |g| g.desired,
                    ),
                    gauge(
                        &meter,
                        "cronoq.pool.busy_workers",
                        "Workers running tasks",
                        gauges.clone(),
                        |g| g.busy,
                    ),
                    gauge(
                        &meter,
                        "cronoq.pool.held_workers",
                        "Workers held for fixed-start tasks",
                        gauges,
                        |g| g.held,
                    ),
                    pending,
                ],
                _cost: cost,
            }
        }
        pub fn submitted(&self, pool: &Arc<str>) {
            self.submitted
                .add(1, &[KeyValue::new("pool", pool.clone())]);
        }
        pub fn finished(&self, pool: &Arc<str>, status: TaskStatus) {
            self.finished.add(
                1,
                &[
                    KeyValue::new("pool", pool.clone()),
                    KeyValue::new("status", status.name()),
                ],
            );
        }
        pub fn attempt(&self, pool: &Arc<str>, kind: &Arc<str>, secs: f64, ok: bool) {
            let attrs = [
                KeyValue::new("pool", pool.clone()),
                KeyValue::new("kind", kind.clone()),
                KeyValue::new("outcome", if ok { "ok" } else { "error" }),
            ];
            self.attempts.add(1, &attrs);
            self.run.record(secs, &attrs);
        }
    }
}
