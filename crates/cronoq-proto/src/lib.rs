//! Generated protobuf and gRPC types for the cronoq server API (`cronoq.v1`).
//!
//! The service definitions live in `proto/cronoq/v1/cronoq.proto` (also available as
//! [`PROTO`]); generate clients for other languages from that file.
#![warn(missing_docs)]

/// Messages and services of the `cronoq.v1` package.
#[allow(clippy::all, missing_docs)]
pub mod v1 {
    tonic::include_proto!("cronoq.v1");
}

/// The `.proto` source, for clients in other languages.
pub const PROTO: &str = include_str!("../proto/cronoq/v1/cronoq.proto");

/// Worker protocol version implemented by this crate (see `Register.protocol_version`).
pub const PROTOCOL_VERSION: u32 = 3;
