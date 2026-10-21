//! Postgres store wiring (enabled once `cronoq-store-postgres` lands).

use std::sync::Arc;

use bytes::Bytes;
use cronoq::Store;

pub async fn open(_url: &str) -> anyhow::Result<Arc<dyn Store<Bytes, Bytes>>> {
    anyhow::bail!("the postgres store is not available in this build")
}
