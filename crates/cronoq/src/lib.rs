//! # cronoq
//!
//! An elastic, priority-aware task scheduler for Rust (and Python, and over gRPC).
//!
//! - **Priorities with aging**: four classes, FIFO within a class, starvation-free aging.
//! - **Deadlines**: urgency rises as a task's latest start time approaches.
//! - **Fixed starts and cron**: capacity is reserved and a worker is held ahead of time.
//! - **DAGs**: dependencies with success/failure/completion conditions.
//! - **Reliability**: leases, timeouts, retries with backoff, dead-lettering, cancellation.
//! - **Resources**: semaphores and token buckets shared across pools (e.g. API rate limits).
//! - **Elasticity**: queue-pressure autoscaling per pool through pluggable scalers.
//! - **Observability**: OpenTelemetry metrics and trace propagation; windowed quality metrics.
//!
//! ```no_run
//! use std::time::Duration;
//! use cronoq::{Pool, Priority, Scheduler, Task, TaskError};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let sched = Scheduler::<String, usize>::builder()
//!     .pool(Pool::new("default").workers(1, 2, 8).executor_fn(|_ctx, text: std::sync::Arc<String>| async move {
//!         Ok::<_, TaskError>(text.len())
//!     }))
//!     .build()
//!     .await?;
//! let h = sched.submit(Task::new("hello".to_string()).priority(Priority::High).deadline_in(Duration::from_secs(5))).await?;
//! assert_eq!(*h.await?, 5);
//! sched.shutdown(Duration::from_secs(5)).await?;
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

mod blocking;
mod clock;
mod driver;
mod error;
mod executor;
mod scaler;
mod scheduler;
mod store;
mod task;
mod telemetry;

pub use blocking::{BlockingHandle, BlockingScheduler};
pub use clock::Clock;
pub use error::{Error, ScaleError, StoreError, TaskError, TaskFailure};
pub use executor::{BoxFuture, Executor, FnExecutor, TaskContext};
pub use scaler::{ExternalScaler, Scaler, SlotScaler};
pub use scheduler::{Builder, Pool, Scheduler, TaskHandle};
#[cfg(feature = "store-codec")]
pub mod codec;
pub use store::{MemoryStore, Store, StoreQueries, TaskRecord, TimeBase};
pub use task::{Recurring, Task, When};

pub use cronoq_core as core;
/// Engine types re-exported for convenience.
pub use cronoq_core::{
    AutoscaleConfig, Bounds, Dep, EdgeCond, GroupSnapshot, Labels, LeaseId, MissPolicy, Mutation,
    OverflowPolicy, OverlapPolicy, PoolConfig, PoolId, PoolSnapshot, Priority, PriorityConfig,
    Recovered, RecoveredTask, RecoveredTerminal, RecurringSpec, ResourceConfig, ResourceKind,
    RetryPolicy, Snapshot, StatusEvent, SubmitError, Submitted, TaskId, TaskRef, TaskSpec,
    TaskStatus, Time, UnknownDependency, UrgencyConfig,
};
