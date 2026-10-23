//! `cronoq-operator`: scales worker Deployments to their pool's desired capacity.
//!
//! For each `CronoqWorkerScaler`, every `interval_s` the operator reads the pool's desired
//! capacity from a configured cronoq server (`Admin.Snapshot`, metrics role) and sets the
//! Deployment's replicas through its `scale` subresource. When the server cannot be reached or
//! does not report the pool, it leaves the Deployment alone and says why in the scaler's status.

mod config;
mod crd;
mod servers;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, CustomResourceExt, ResourceExt};
use serde_json::json;

use crate::crd::{CronoqWorkerScaler, ScalerStatus};
use crate::servers::Servers;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Configuration file (TOML): the servers scalers may name.
    #[arg(
        long,
        env = "CRONOQ_OPERATOR_CONFIG",
        default_value = "/etc/cronoq-operator/config.toml"
    )]
    config: PathBuf,
    /// Print the CustomResourceDefinition as YAML and exit.
    #[arg(long)]
    print_crd: bool,
}

struct Ctx {
    client: Client,
    servers: Servers,
    interval: Duration,
}

/// An error that makes the controller retry a scaler sooner.
#[derive(Debug)]
struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<kube::Error> for Error {
    fn from(e: kube::Error) -> Self {
        Error(e.to_string())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.print_crd {
        print!("{}", crd_text()?);
        return Ok(());
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cfg = config::Config::load(&args.config)?;
    let interval = cfg.interval();
    let servers = Servers::new(cfg)?;
    let client = Client::try_default().await?;
    let scalers: Api<CronoqWorkerScaler> = Api::all(client.clone());
    tracing::info!(?interval, "cronoq-operator started");
    Controller::new(scalers, watcher::Config::default())
        .shutdown_on_signal()
        .run(
            reconcile,
            error_policy,
            Arc::new(Ctx {
                client,
                servers,
                interval,
            }),
        )
        .for_each(|r| async move {
            if let Err(e) = r {
                tracing::warn!(error = %e, "reconcile failed");
            }
        })
        .await;
    Ok(())
}

/// The CRD as YAML (JSON is valid YAML; this keeps the dependency list short).
fn crd_text() -> anyhow::Result<String> {
    Ok(serde_json::to_string_pretty(&CronoqWorkerScaler::crd())? + "\n")
}

async fn reconcile(scaler: Arc<CronoqWorkerScaler>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = scaler.namespace().unwrap_or_default();
    let name = scaler.name_any();
    let spec = &scaler.spec;
    let mut status = ScalerStatus {
        observed_generation: scaler.metadata.generation,
        ..Default::default()
    };
    let outcome = async {
        crd::check(spec)?;
        let snap = ctx.servers.snapshot(&spec.server).await?;
        let desired = servers::desired(&snap, &spec.pool)?;
        status.desired_slots = Some(desired);
        let want = crd::replicas(spec, desired);
        let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
        let scale = match deployments.get_scale(&spec.deployment).await {
            Ok(s) => s,
            Err(kube::Error::Api(e)) if e.code == 404 => {
                return Err(format!("deployment '{}' not found", spec.deployment));
            }
            Err(e) => return Err(format!("reading the scale of '{}': {e}", spec.deployment)),
        };
        let current = scale.spec.and_then(|s| s.replicas).unwrap_or(0);
        if current != want as i32 {
            deployments
                .patch_scale(
                    &spec.deployment,
                    &PatchParams::default(),
                    &Patch::Merge(json!({ "spec": { "replicas": want } })),
                )
                .await
                .map_err(|e| format!("scaling '{}': {e}", spec.deployment))?;
            tracing::info!(%ns, scaler = %name, deployment = %spec.deployment, from = current, to = want, desired, "scaled");
        }
        status.replicas = Some(want);
        Ok::<_, String>(())
    }
    .await;
    status.message = match outcome {
        Ok(()) => "Scaled".into(),
        Err(why) => {
            tracing::warn!(%ns, scaler = %name, reason = %why, "deployment left alone");
            // Keep the last replicas the operator set, so the status says what is in effect.
            status.replicas = scaler.status.as_ref().and_then(|s| s.replicas);
            why
        }
    };
    if scaler.status.as_ref() != Some(&status) {
        let api: Api<CronoqWorkerScaler> = Api::namespaced(ctx.client.clone(), &ns);
        api.patch_status(
            &name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    }
    Ok(Action::requeue(ctx.interval))
}

fn error_policy(_: Arc<CronoqWorkerScaler>, _: &Error, ctx: Arc<Ctx>) -> Action {
    Action::requeue(ctx.interval.min(Duration::from_secs(5)))
}
