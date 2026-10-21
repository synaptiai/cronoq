//! Throwaway PostgreSQL server for tests, using the local PostgreSQL 16 binaries.
#![allow(dead_code)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

const BIN: &str = "/usr/lib/postgresql/16/bin";

pub struct TestPg {
    dir: PathBuf,
    pub url: String,
}

fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .map(|o| o.stdout.starts_with(b"0"))
        .unwrap_or(false)
}

fn pg_cmd(tool: &str) -> Command {
    let path = Path::new(BIN).join(tool);
    if is_root() {
        // initdb and postgres refuse to run as root.
        let mut c = Command::new("runuser");
        c.args(["-u", "postgres", "--"]).arg(path);
        c
    } else {
        Command::new(path)
    }
}

fn run(mut c: Command) {
    let out = c.output().expect("spawn");
    assert!(
        out.status.success(),
        "{c:?} failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

impl TestPg {
    /// Start a fresh server, or `None` (with a message) if PostgreSQL is not installed.
    pub fn start() -> Option<TestPg> {
        if !Path::new(BIN).join("initdb").exists() {
            eprintln!("skipping: PostgreSQL binaries not found in {BIN}");
            return None;
        }
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cronoq-pg-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        if is_root() {
            run({
                let mut c = Command::new("chown");
                c.args(["-R", "postgres"]).arg(&dir);
                c
            });
        }
        let data = dir.join("data");
        run({
            let mut c = pg_cmd("initdb");
            c.arg("-D").arg(&data).args([
                "-A",
                "trust",
                "-U",
                "postgres",
                "--no-sync",
                "-E",
                "UTF8",
            ]);
            c
        });
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        run({
            let mut c = pg_cmd("pg_ctl");
            c.arg("-D").arg(&data).arg("-l").arg(dir.join("log")).arg("-w").arg("-o").arg(format!(
                "-p {port} -c listen_addresses=127.0.0.1 -k {} -c fsync=off -c synchronous_commit=off",
                dir.display()
            ));
            c.arg("start");
            c
        });
        Some(TestPg {
            dir,
            url: format!("postgres://postgres@127.0.0.1:{port}/postgres"),
        })
    }
}

impl Drop for TestPg {
    fn drop(&mut self) {
        let mut c = pg_cmd("pg_ctl");
        c.arg("-D")
            .arg(self.dir.join("data"))
            .args(["-m", "immediate", "stop"]);
        let _ = c.output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Start a server or return early from the test.
#[macro_export]
macro_rules! pg_or_skip {
    () => {
        match common::TestPg::start() {
            Some(pg) => pg,
            None => return,
        }
    };
}
