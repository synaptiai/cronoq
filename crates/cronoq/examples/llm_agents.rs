//! LLM-style workload: rate-limited API calls with priorities, deadlines and 429 retries.
//!
//! `cargo run -p cronoq --example llm_agents`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use cronoq::*;

#[derive(Debug)]
struct Prompt {
    user: &'static str,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let sched = Scheduler::<Prompt, String>::builder()
        // 20 requests/s with bursts of 5, shared by every pool that uses "llm-api".
        .resource(ResourceConfig::token_bucket("llm-api", 20.0, 5))
        .pool(
            Pool::new("agents")
                .workers(2, 2, 32)
                .executor_fn(move |ctx, p: Arc<Prompt>| {
                    let c = c.clone();
                    async move {
                        // Simulated model call: 50-250ms, and every 7th call is throttled.
                        let n = c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50 + (n as u64 * 37) % 200)).await;
                        if n % 7 == 3 {
                            return Err(TaskError::retry_after(
                                "429 Too Many Requests",
                                Duration::from_millis(300),
                            ));
                        }
                        Ok(format!(
                            "[{}#{}] answer to '{}'",
                            p.user, ctx.attempt, p.text
                        ))
                    }
                }),
        )
        .build()
        .await?;

    let start = Instant::now();
    let mut handles = Vec::new();
    for i in 0..60 {
        let (user, priority) = match i % 10 {
            0 => ("pager", Priority::Critical),
            1..=3 => ("chat", Priority::High),
            _ => ("batch", Priority::Low),
        };
        let task = Task::new(Prompt {
            user,
            text: format!("question {i}"),
        })
        .pool("agents")
        .kind(user)
        .priority(priority)
        .resource("llm-api", 1)
        .timeout(Duration::from_secs(5));
        // Interactive requests should start within 2 seconds.
        let task = if user == "chat" {
            task.deadline_in(Duration::from_secs(2))
        } else {
            task
        };
        handles.push((user, sched.submit(task).await?));
    }
    let mut latest = std::collections::BTreeMap::new();
    for (user, h) in handles {
        let out = h.await?;
        latest.insert(user, (start.elapsed(), out));
    }
    for (user, (at, out)) in latest {
        println!("last {user:6} done at {at:>8.2?}: {out}");
    }
    let snap = sched.snapshot().await?;
    let p = snap.pool("agents").unwrap();
    println!(
        "calls={} retries={} workers={} adherence={:.0}% wait p90={:.2?}",
        calls.load(Ordering::SeqCst),
        snap.totals.retries,
        p.capacity,
        p.priority_adherence * 100.0,
        p.wait_p90
    );
    sched.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
