# cronoq-server

[![crates.io](https://img.shields.io/crates/v/cronoq-server.svg)](https://crates.io/crates/cronoq-server)
[![docs.rs](https://img.shields.io/docsrs/cronoq-server)](https://docs.rs/cronoq-server)

Runs the [cronoq](https://crates.io/crates/cronoq) scheduler as a service:

- clients submit tasks over **gRPC** or **HTTP/JSON**;
- **remote workers** in any language lease tasks over a bidirectional gRPC stream (worker
  protocol v2), with heartbeats, aborts, hand-backs and automatic retry of lost attempts;
- pools autoscale on queue pressure and export the desired worker count for KEDA/HPA, or call a
  webhook;
- bearer tokens and client certificates mapped to `client` / `worker` / `metrics` / `admin`
  roles, optionally limited to pools; TLS and mTLS on both listeners;
- request limits on everything (queues, batch and message sizes, waits, connections);
- Prometheus `/metrics` (authenticated, or on a separate internal port), `/healthz`, `/readyz`;
- with the Postgres store, durable state and active-passive **high availability** with leader
  election and fencing.

```sh
cargo install cronoq-server
cronoq-server --config cronoq.toml --check   # validate
cronoq-server --config cronoq.toml           # serve (127.0.0.1:7070 gRPC, 127.0.0.1:7071 HTTP)
# or: docker run -p 7070:7070 -p 7071:7071 -e CRONOQ_CLIENT_TOKEN ... ghcr.io/synaptiai/cronoq
```

Without `--config` the server runs a single `default` pool on loopback with authentication
disabled. Listening on other addresses requires `[[auth.tokens]]` (or `--insecure-no-auth` on a
trusted network). `cronoq-server --print-proto` prints the API definition.

The crate also provides a Rust worker client:

```rust,no_run
use bytes::Bytes;
use cronoq_server::worker::{run_worker, WorkerOptions};

# async fn demo() -> anyhow::Result<()> {
let opts = WorkerOptions::new("http://localhost:7070", "default")
    .slots(4)
    .token(std::env::var("CRONOQ_WORKER_TOKEN")?);
run_worker(opts, |lease| async move { Ok(Bytes::from(lease.payload.to_vec())) },
           std::future::pending()).await
# }
```

`run_worker` enforces the granted slots, heartbeats each lease, cancels handlers on abort,
reconnects with backoff (also when the server steps down), and drains on shutdown.

- 📘 [Server quickstart](https://synaptiai.github.io/cronoq/guides/server-quickstart.html)
- ⚙️ [Configuration reference](https://synaptiai.github.io/cronoq/reference/configuration.html)
- 🌐 [HTTP](https://synaptiai.github.io/cronoq/reference/http.html) and
  [gRPC](https://synaptiai.github.io/cronoq/reference/grpc.html) references
- 🚀 [Deployment guide](https://synaptiai.github.io/cronoq/guides/deployment.html)
- 🔒 [Security hardening](https://synaptiai.github.io/cronoq/guides/security.html)

Licensed under MIT or Apache-2.0, at your option.
