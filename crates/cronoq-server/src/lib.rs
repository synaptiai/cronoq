//! # cronoq-server
//!
//! Runs a cronoq scheduler as a service: clients submit tasks over gRPC (`cronoq.v1.Tasks`) or
//! HTTP/JSON, remote workers lease them over a bidirectional stream (`cronoq.v1.Workers`), and
//! operators inspect it via `cronoq.v1.Admin`, `/v1/snapshot` and Prometheus `/metrics`.
//! With the Postgres store, several replicas run active-passive: one leader serves, the
//! others report `/readyz` = 503 and take over on failure.

pub mod auth;
pub mod config;
pub mod convert;
pub mod grpc;
pub mod http;
pub mod metrics;
mod pg;
pub mod remote;
pub mod scaler;
pub mod server;
pub mod state;
pub mod worker;

pub use config::Config;
pub use server::{Bound, run};
