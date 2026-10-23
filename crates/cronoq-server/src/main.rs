//! `cronoq-server` binary.

use std::path::PathBuf;

use clap::Parser;

/// cronoq scheduler server.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(short, long, env = "CRONOQ_CONFIG")]
    config: Option<PathBuf>,
    /// Allow running without authentication on non-loopback addresses (trusted networks only).
    #[arg(long)]
    insecure_no_auth: bool,
    /// The shard this server serves (overrides `server.shard`).
    #[arg(long, env = "CRONOQ_SHARD")]
    shard: Option<String>,
    /// Validate the configuration and exit.
    #[arg(long)]
    check: bool,
    /// Print the protobuf API definition and exit.
    #[arg(long)]
    print_proto: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    if args.print_proto {
        print!("{}", cronoq_proto::PROTO);
        return Ok(());
    }
    let mut cfg = match &args.config {
        Some(p) => cronoq_server::Config::read(p)?,
        None => cronoq_server::Config::default(),
    };
    if args.insecure_no_auth {
        cfg.server.insecure_no_auth = true;
    }
    if let Some(s) = args.shard {
        cfg.server.shard = Some(s);
    }
    if args.check {
        cfg.validate()?;
        println!("configuration is valid");
        return Ok(());
    }
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut term) => {
                    tokio::select! {
                        _ = ctrl_c => {}
                        _ = term.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = ctrl_c.await;
                }
            }
        }
        #[cfg(not(unix))]
        let _ = ctrl_c.await;
        tracing::info!("shutting down");
    };
    cronoq_server::run(
        cfg,
        shutdown,
        |b| tracing::info!(grpc = %b.grpc, http = %b.http, metrics = ?b.metrics, "listening"),
    )
    .await
}
