//! # cronoq-server
//!
//! Runs a cronoq scheduler as a service: clients submit tasks over gRPC (`cronoq.v1.Tasks`) or
//! HTTP/JSON, remote workers lease them over a bidirectional stream (`cronoq.v1.Workers`), and
//! operators inspect it via `cronoq.v1.Admin`, `/v1/snapshot` and Prometheus `/metrics`.
//! With the Postgres store, several replicas run active-passive: one leader serves, the
//! others report `/readyz` = 503 and take over on failure.
//!
//! The server is designed for a single tenant (one team or trust domain). Tokens can be
//! restricted to pools, which scopes submission, lookups, watches and worker registration,
//! but task keys and resources share one namespace.

#![warn(missing_docs)]

pub mod auth;
pub mod config;
pub mod convert;
pub mod grpc;
mod grpc_web;
pub mod http;
pub mod info;
pub mod metrics;
mod peer;
mod pg;
pub mod remote;
mod route;
pub mod scaler;
pub mod server;
pub mod shard;
mod standby;
pub mod state;
mod store;
pub mod worker;

pub use config::Config;
pub use server::{Bound, run};
