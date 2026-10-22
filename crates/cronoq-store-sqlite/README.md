# cronoq-store-sqlite

A SQLite store for [cronoq](https://github.com/synaptiai/cronoq): durable scheduler state in one
local file, with no database server.

- **Write-ahead state**: every engine state transition is committed before submitters are
  acknowledged, in one transaction per engine step. The WAL journal is synced on every commit by
  default.
- **Leader and standbys**: processes on the same host can share the file. One leads through a
  renewed lease; the others take over when the lease runs out, or at once when the leader shuts
  down cleanly.
- **Fencing**: every commit checks the fencing epoch inside its write transaction, so a paused
  old leader cannot write after a standby replaced it.
- **Housekeeping**: `prune(older_than)` removes old finished tasks; `get_task` looks tasks up by
  id or key.
- **Files**: created readable and writable by the owner only (on Unix). Keep the file on a
  local filesystem; for several hosts, use `cronoq-store-postgres`.

```rust,no_run
use bytes::Bytes;
use cronoq::{Pool, Scheduler};
use cronoq_store_sqlite::SqliteStore;

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let store = SqliteStore::open("/var/lib/cronoq/state.db").await?;
let sched = Scheduler::<Bytes, Bytes>::builder()
    .pool(Pool::new("default").executor_fn(|_ctx, p: std::sync::Arc<Bytes>| async move {
        Ok::<_, cronoq::TaskError>((*p).clone())
    }))
    .store(store.clone())
    .build() // blocks until this process is the leader
    .await?;
# Ok(()) }
```

`cronoq-server` uses this store when configured with `[store] kind = "sqlite"`.

- 📘 [Durability and HA](https://synaptiai.github.io/cronoq/concepts/durability.html)

Licensed under MIT or Apache-2.0, at your option.
