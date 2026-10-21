//! Batch pipeline as a DAG: extract -> transform (fan-out) -> load (fan-in), with a failure
//! handler and a cleanup step that always runs.
//!
//! `cargo run -p cronoq --example dag_pipeline`

use std::sync::Arc;
use std::time::Duration;

use cronoq::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sched = Scheduler::<String, String>::builder()
        .pool(
            Pool::fixed("etl", 4).executor_fn(|ctx, step: Arc<String>| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if step.as_str() == "transform-3" && ctx.attempt < 2 {
                    return Err(TaskError::retryable("transient error"));
                }
                println!("ran {step} (attempt {})", ctx.attempt);
                Ok(format!("{step}: ok"))
            }),
        )
        .build()
        .await?;

    let mut tasks = vec![Task::new("extract".to_string()).pool("etl").key("extract")];
    for i in 0..4 {
        tasks.push(
            Task::new(format!("transform-{i}"))
                .pool("etl")
                .key(format!("t{i}"))
                .after("extract"),
        );
    }
    let mut load = Task::new("load".to_string()).pool("etl").key("load");
    for i in 0..4 {
        load = load.after(format!("t{i}"));
    }
    tasks.push(load);
    tasks.push(
        Task::new("alert-on-failure".to_string())
            .pool("etl")
            .dep(Dep::on_failure("load")),
    );
    tasks.push(
        Task::new("cleanup".to_string())
            .pool("etl")
            .dep(Dep::on_complete("load")),
    );

    // Admitted atomically, in any order.
    let handles = sched.submit_batch(tasks).await?;
    for h in handles {
        match h.await {
            Ok(out) => println!("  -> {out}"),
            Err(e) => println!("  -> {e}"),
        }
    }
    sched.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
