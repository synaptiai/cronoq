# cronoq

[![crates.io](https://img.shields.io/crates/v/cronoq.svg)](https://crates.io/crates/cronoq)
[![docs.rs](https://img.shields.io/docsrs/cronoq)](https://docs.rs/cronoq)

An elastic, priority-aware task scheduler for tokio applications. cronoq orders and admits work
(priorities with aging, deadlines, fixed starts and cron, DAG dependencies, retries, shared rate
limits) and sizes each pool's concurrency from queue pressure. Your async functions do the work.

```toml
[dependencies]
cronoq = "0.2"
```

```rust,no_run
use std::{sync::Arc, time::Duration};
use cronoq::{Pool, Priority, Scheduler, Task, TaskError};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let sched = Scheduler::<String, usize>::builder()
    .pool(Pool::new("default").workers(1, 2, 8).executor_fn(|_ctx, text: Arc<String>| async move {
        Ok::<_, TaskError>(text.len())
    }))
    .build()
    .await?;

let handle = sched
    .submit(Task::new("hello".to_string()).priority(Priority::High).deadline_in(Duration::from_secs(5)))
    .await?;
assert_eq!(*handle.await?, 5);
sched.shutdown(Duration::from_secs(5)).await?;
# Ok(()) }
```

**Use it when** you want scheduling policies (priority, deadlines, rate limits, autoscaling)
inside a Rust service without running extra infrastructure. For a standalone service with
remote workers, see [`cronoq-server`](https://crates.io/crates/cronoq-server); for Python, see
[`cronoq` on PyPI](https://pypi.org/project/cronoq/).

- 📘 [Documentation book](https://synaptiai.github.io/cronoq/)
- 🦀 [API reference](https://docs.rs/cronoq)
- 💡 [Use cases](https://synaptiai.github.io/cronoq/use-cases/index.html)

Feature `otel` (default) adds OpenTelemetry metrics and trace propagation. MSRV: 1.94.

Licensed under MIT or Apache-2.0, at your option.
