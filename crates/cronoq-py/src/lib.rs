//! Python bindings for cronoq (`cronoq._cronoq`).
//!
//! The engine runs on a shared tokio runtime; Python executors run as coroutines on the asyncio
//! loop that called `Scheduler.start()`. Payloads and results are arbitrary Python objects and
//! are never serialized. The pure-Python package `cronoq` re-exports these classes alongside the
//! exception types and the `compat` shim.

use pyo3::prelude::*;

mod errors;
mod executor;
mod scheduler;
mod types;

#[pymodule]
fn _cronoq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<types::Priority>()?;
    m.add_class::<types::RetryPolicy>()?;
    m.add_class::<types::AutoscaleConfig>()?;
    m.add_class::<types::Dep>()?;
    m.add_class::<types::Resource>()?;
    m.add_class::<types::Pool>()?;
    m.add_class::<types::Task>()?;
    m.add_class::<executor::TaskContext>()?;
    m.add_class::<scheduler::Scheduler>()?;
    m.add_class::<scheduler::TaskHandle>()?;
    m.add_class::<scheduler::Snapshot>()?;
    m.add_class::<scheduler::PoolSnapshot>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
