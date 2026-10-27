# cronoq-store-postgres

[![crates.io](https://img.shields.io/crates/v/cronoq-store-postgres.svg)](https://crates.io/crates/cronoq-store-postgres)
[![docs.rs](https://img.shields.io/docsrs/cronoq-store-postgres)](https://docs.rs/cronoq-store-postgres)

A PostgreSQL [`Store`](https://docs.rs/cronoq/latest/cronoq/trait.Store.html) for
[cronoq](https://crates.io/crates/cronoq) schedulers with `Bytes` payloads and outputs:

- **Durability**: every engine step's mutations are committed in one transaction before
  submissions are acknowledged; non-terminal tasks, recent terminal statuses, outputs and
  recurring schedules are recovered on startup.
- **Leader election**: a session-level advisory lock elects one active scheduler; standbys
  wait and take over when its session ends.
- **Fencing**: every commit checks the leader epoch in the same transaction, so a deposed
  leader cannot write.
- **Housekeeping**: `prune(older_than)` removes old finished tasks; `get_task` looks tasks up by
  id or key.

```rust,no_run
use bytes::Bytes;
use cronoq::{Pool, Scheduler};
use cronoq_store_postgres::PgStore;

# async fn demo(executor: impl cronoq::Executor<Bytes, Bytes>) -> Result<(), Box<dyn std::error::Error>> {
let store = PgStore::connect("postgres://cronoq@db/cronoq").await?; // applies the schema
let sched = Scheduler::<Bytes, Bytes>::builder()
    .store(store)
    .pool(Pool::new("default").executor(executor))
    .build()                         // blocks until this instance is the leader
    .await?;
# Ok(()) }
```

`cronoq-server` uses this store when configured with `[store] kind = "postgres"`.

- 📘 [Durability and HA](https://synaptiai.github.io/cronoq/concepts/durability.html)

Licensed under MIT or Apache-2.0, at your option.
