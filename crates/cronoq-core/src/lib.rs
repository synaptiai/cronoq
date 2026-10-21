//! # cronoq-core
//!
//! The sans-IO engine of the cronoq elastic priority scheduler. [`Core`] is a deterministic,
//! single-threaded state machine: it never reads a clock, spawns threads or performs I/O. A
//! driver (the `cronoq` crate, or a test harness with virtual time) feeds it submissions,
//! completions and the current [`Time`], and executes the [`Action`]s it emits.
//!
//! ```
//! use cronoq_core::*;
//!
//! let mut core: Core<&str> = Core::new(CoreConfig::default());
//! let t0 = Time::ZERO;
//! core.submit(t0, TaskSpec::new("hello").priority(Priority::High)).unwrap();
//! let Some(Action::Dispatch(d)) = core.poll_action() else { panic!() };
//! assert_eq!(*d.payload, "hello");
//! core.complete(Time::from_secs_f64(0.5), d.lease, Outcome::Success);
//! ```
#![forbid(unsafe_code)]

mod autoscale;
mod config;
mod engine;
mod io;
mod lane;
mod reservations;
mod rng;
mod snapshot;
mod spec;
mod stats;
mod time;

pub use autoscale::{AutoscaleState, Signals};
pub use config::*;
pub use engine::{Core, Totals};
pub use io::*;
pub use snapshot::{PoolSnapshot, Snapshot};
pub use spec::*;
pub use stats::{DurStats, Histogram};
pub use time::Time;
