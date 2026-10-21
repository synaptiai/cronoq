//! End-to-end throughput through the tokio runtime with no-op in-process tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use cronoq::{Pool, Scheduler, Task, TaskError};
use tokio::sync::Notify;

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut g = c.benchmark_group("runtime");
    g.throughput(Throughput::Elements(1));
    g.sample_size(10);
    g.bench_function("e2e/noop/bulk", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let done = Arc::new(AtomicU64::new(0));
            let notify = Arc::new(Notify::new());
            let (d, n) = (done.clone(), notify.clone());
            let target = iters;
            let s = Scheduler::<u64, ()>::builder()
                .pool(
                    Pool::fixed("default", 256).executor_fn(move |_c, _x: Arc<u64>| {
                        let (d, n) = (d.clone(), n.clone());
                        async move {
                            if d.fetch_add(1, Ordering::Relaxed) + 1 == target {
                                n.notify_one();
                            }
                            Ok::<_, TaskError>(())
                        }
                    }),
                )
                .queue_capacity(65536)
                .build()
                .await
                .unwrap();
            let start = Instant::now();
            let mut sent = 0;
            while sent < iters {
                let n = (iters - sent).min(4096);
                let batch = (0..n).map(Task::new).collect();
                s.enqueue_many(batch).await.unwrap();
                sent += n;
            }
            if done.load(Ordering::Relaxed) < iters {
                notify.notified().await;
            }
            let el = start.elapsed();
            s.shutdown(Duration::from_secs(1)).await.unwrap();
            el
        })
    });
    g.bench_function("e2e/noop/submit_await", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let s = Scheduler::<u64, u64>::builder()
                .pool(
                    Pool::fixed("default", 64)
                        .executor_fn(|_c, x: Arc<u64>| async move { Ok::<_, TaskError>(*x) }),
                )
                .build()
                .await
                .unwrap();
            let start = Instant::now();
            for i in 0..iters {
                s.submit(Task::new(i)).await.unwrap().await.unwrap();
            }
            let el = start.elapsed();
            s.shutdown(Duration::from_secs(1)).await.unwrap();
            el
        })
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(8)).warm_up_time(Duration::from_secs(2));
    targets = bench
}
criterion_main!(benches);
