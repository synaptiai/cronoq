//! The shared store conformance tests, against a fresh SQLite file.

mod common;

use async_trait::async_trait;
use cronoq_store_sqlite::SqliteStore;

#[async_trait]
impl cronoq_store_tests::Harness for common::TestDb {
    type Store = SqliteStore;
    async fn open(&self) -> SqliteStore {
        common::TestDb::open(self).await
    }
}

cronoq_store_tests::suite!(async { Some(common::TestDb::new()) });
