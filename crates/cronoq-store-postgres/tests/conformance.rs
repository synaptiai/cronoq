//! The shared store conformance tests, against a throwaway PostgreSQL server.

mod common;

use std::time::Duration;

use async_trait::async_trait;
use cronoq_store_postgres::{PgStore, PgStoreConfig};

struct Pg(common::TestPg);

#[async_trait]
impl cronoq_store_tests::Harness for Pg {
    type Store = PgStore;
    async fn open(&self) -> PgStore {
        let cfg = PgStoreConfig {
            lock_poll: Duration::from_millis(50),
            keepalive: Duration::from_millis(100),
            ..Default::default()
        };
        PgStore::connect_with(&self.0.url, cfg).await.unwrap()
    }
}

cronoq_store_tests::suite!(async { common::TestPg::start().map(Pg) });
