# cronoq-proto

[![crates.io](https://img.shields.io/crates/v/cronoq-proto.svg)](https://crates.io/crates/cronoq-proto)
[![docs.rs](https://img.shields.io/docsrs/cronoq-proto)](https://docs.rs/cronoq-proto)

Protocol buffer definitions and generated [tonic](https://crates.io/crates/tonic) clients and
servers for the [cronoq](https://github.com/synaptiai/cronoq) server API (`cronoq.v1`):

- `Tasks`: submit, batch, cancel, get, wait and watch tasks;
- `Workers`: the bidirectional stream remote workers use to lease and complete tasks;
- `Admin`: snapshots and recurring schedules.

```rust,no_run
use cronoq_proto::v1::{tasks_client::TasksClient, SubmitRequest, TaskSpec};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let mut client = TasksClient::connect("http://localhost:7070").await?;
let task = TaskSpec { payload: b"hello".to_vec(), ..Default::default() };
let resp = client.submit(SubmitRequest { task: Some(task), wait: true }).await?;
println!("{:?}", resp.into_inner());
# Ok(()) }
```

The `.proto` source is in `proto/cronoq/v1/cronoq.proto` and available as `cronoq_proto::PROTO`.
Builds use a vendored `protoc`; no system installation is required.

- 📘 [gRPC API reference](https://synaptiai.github.io/cronoq/reference/grpc.html)

Licensed under MIT or Apache-2.0, at your option.
