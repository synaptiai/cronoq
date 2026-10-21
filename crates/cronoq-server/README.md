# cronoq-server

[![crates.io](https://img.shields.io/crates/v/cronoq-server.svg)](https://crates.io/crates/cronoq-server)
[![docs.rs](https://img.shields.io/docsrs/cronoq-server)](https://docs.rs/cronoq-server)

Runs the [cronoq](https://crates.io/crates/cronoq) scheduler as a service:

- clients submit tasks over **gRPC** or **HTTP/JSON**;
- **remote workers** in any language lease tasks over a bidirectional gRPC stream, with
  heartbeats, aborts and automatic retry of lost attempts;
- pools autoscale on queue pressure and export the desired worker count for KEDA/HPA, or call a
  webhook;
- bearer tokens with `client` / `worker` / `admin` roles and optional mTLS;
- Prometheus `/metrics`, `/healthz`, `/readyz`;
- with the Postgres store, active-passive **high availability** with leader election and
  fencing.

```sh
cargo install cronoq-server
cronoq-server --config cronoq.toml
# or: docker run -p 7070:7070 -p 7071:7071 ghcr.io/synaptiai/cronoq
```

The crate also provides a Rust worker client:

```rust,no_run
use bytes::Bytes;
use cronoq_server::worker::{run_worker, WorkerOptions};

# async fn demo() -> anyhow::Result<()> {
let opts = WorkerOptions::new("http://localhost:7070", "default").slots(4).token("worker-token");
run_worker(opts, |lease| async move { Ok(Bytes::from(lease.payload.to_vec())) },
           std::future::pending()).await
# }
```

- 📘 [Server quickstart](https://synaptiai.github.io/cronoq/guides/server-quickstart.html)
- ⚙️ [Configuration reference](https://synaptiai.github.io/cronoq/reference/configuration.html)
- 🚀 [Deployment guide](https://synaptiai.github.io/cronoq/guides/deployment.html)

Licensed under MIT or Apache-2.0, at your option.
