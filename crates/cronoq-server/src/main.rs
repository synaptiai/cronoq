use std::path::PathBuf;

use clap::Parser;

/// cronoq scheduler server.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(short, long, env = "CRONOQ_CONFIG")]
    config: Option<PathBuf>,
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
    let cfg = match &args.config {
        Some(p) => cronoq_server::Config::load(p)?,
        None => cronoq_server::Config::default(),
    };
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal handler");
            tokio::select! {
                _ = ctrl_c => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = ctrl_c.await;
        tracing::info!("shutting down");
    };
    cronoq_server::run(
        cfg,
        shutdown,
        |b| tracing::info!(grpc = %b.grpc, http = %b.http, "listening"),
    )
    .await
}
