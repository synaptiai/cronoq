//! Microtask throughput: push N no-op tasks through the runtime and report tasks/s.
//!
//! `cargo run --release -p cronoq --example firehose -- 1000000`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cronoq::*;
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(500_000);
    let done = Arc::new(AtomicU64::new(0));
    let all_done = Arc::new(Notify::new());
    let (d, a) = (done.clone(), all_done.clone());
    let sched = Scheduler::<u64, ()>::builder()
        .pool(
            Pool::fixed("default", 256).executor_fn(move |_ctx, _x: Arc<u64>| {
                let (d, a) = (d.clone(), a.clone());
                async move {
                    if d.fetch_add(1, Ordering::Relaxed) + 1 == n {
                        a.notify_one();
                    }
                    Ok(())
                }
            }),
        )
        .queue_capacity(65_536)
        .build()
        .await?;
    let start = Instant::now();
    let mut sent = 0;
    while sent < n {
        let k = (n - sent).min(4096);
        sched
            .enqueue_many(
                (0..k)
                    .map(|i| Task::new(sent + i).priority(Priority::ALL[(i % 4) as usize]))
                    .collect(),
            )
            .await?;
        sent += k;
    }
    all_done.notified().await;
    let secs = start.elapsed().as_secs_f64();
    println!("{n} tasks in {secs:.2}s = {:.0} tasks/s", n as f64 / secs);
    let p = &sched.snapshot().await?.pools[0];
    println!(
        "wait p50={:?} p99={:?} utilization={:.0}%",
        p.wait_p50,
        p.wait_p99,
        p.utilization * 100.0
    );
    sched.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
