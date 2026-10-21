//! The original Python prototype's demo, on cronoq: a mixed workload with a fixed-schedule task
//! on an elastic pool, reporting the same quality metrics.
//!
//! `cargo run -p cronoq --example prototype_demo`

use std::sync::Arc;
use std::time::Duration;

use cronoq::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sched = Scheduler::<f64, ()>::builder()
        .pool(
            Pool::new("default")
                .workers(1, 1, 4)
                .autoscale(AutoscaleConfig {
                    drain_time: Duration::from_secs(2),
                    cooldown: Duration::from_secs(2),
                    cost_per_worker_second: 0.001,
                    ..Default::default()
                })
                .executor_fn(|_ctx, secs: Arc<f64>| async move {
                    tokio::time::sleep(Duration::from_secs_f64(*secs)).await;
                    Ok(())
                }),
        )
        .build()
        .await?;
    let tasks = [
        ("ALERT-001", 0.5, Priority::Critical),
        ("URGENT-001", 0.8, Priority::High),
        ("URGENT-002", 0.6, Priority::High),
        ("PROCESS-001", 1.0, Priority::Normal),
        ("PROCESS-002", 0.9, Priority::Normal),
        ("PROCESS-003", 1.1, Priority::Normal),
        ("BACKGROUND-001", 0.7, Priority::Low),
        ("BACKGROUND-002", 0.8, Priority::Low),
    ];
    let mut handles = Vec::new();
    for (id, d, p) in tasks {
        handles.push(
            sched
                .submit(
                    Task::new(d)
                        .key(id)
                        .priority(p)
                        .duration_hint(Duration::from_secs_f64(d)),
                )
                .await?,
        );
    }
    handles.push(
        sched
            .submit(
                Task::new(0.5)
                    .key("SCHEDULED-001")
                    .start_in(Duration::from_secs(2))
                    .duration_hint(Duration::from_millis(500)),
            )
            .await?,
    );
    for h in handles {
        h.await?;
    }
    let snap = sched.snapshot().await?;
    let p = &snap.pools[0];
    println!("Tasks completed:     {}/9", snap.totals.succeeded);
    println!("Priority adherence:  {:.1}%", p.priority_adherence * 100.0);
    println!(
        "Fixed-start hits:    {:.1}%",
        p.fixed_start_hit_rate * 100.0
    );
    println!("Utilization:         {:.1}%", p.utilization * 100.0);
    println!("Schedule efficiency: {:.1}%", p.schedule_efficiency * 100.0);
    println!("Average wait:        {:.2?}", p.wait_mean);
    println!("Peak workers:        {}", p.desired.max(p.capacity));
    println!("Cost:                ${:.4}", p.cost);
    sched.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
