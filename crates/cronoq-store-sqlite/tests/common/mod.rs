//! A fresh database directory per test, removed afterwards.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use cronoq_store_sqlite::{SqliteStore, SqliteStoreConfig};

pub struct TestDb {
    pub dir: PathBuf,
}

impl TestDb {
    pub fn new() -> TestDb {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cronoq-sqlite-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TestDb { dir }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join("state.db")
    }

    pub fn cfg() -> SqliteStoreConfig {
        SqliteStoreConfig {
            lease_ttl: Duration::from_millis(1500),
            lease_renew: Duration::from_millis(200),
            lock_poll: Duration::from_millis(50),
            ..Default::default()
        }
    }

    pub async fn open(&self) -> SqliteStore {
        SqliteStore::open_with(self.path(), Self::cfg())
            .await
            .unwrap()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
