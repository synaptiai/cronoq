//! The `CronoqWorkerScaler` custom resource.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Scales a worker Deployment to its pool's desired capacity.
///
/// The operator reads the pool's desired capacity (in worker slots) from the named server and
/// sets the Deployment's replicas to `ceil(desired / slotsPerReplica)`, clamped to
/// `[minReplicas, maxReplicas]`.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "cronoq.io",
    version = "v1alpha1",
    kind = "CronoqWorkerScaler",
    namespaced,
    status = "ScalerStatus",
    shortname = "cws",
    printcolumn = r#"{"name":"Pool","type":"string","jsonPath":".spec.pool"}"#,
    printcolumn = r#"{"name":"Deployment","type":"string","jsonPath":".spec.deployment"}"#,
    printcolumn = r#"{"name":"Desired slots","type":"integer","jsonPath":".status.desiredSlots"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".status.replicas"}"#,
    printcolumn = r#"{"name":"Message","type":"string","jsonPath":".status.message"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ScalerSpec {
    /// Name of a server in the operator's configuration.
    pub server: String,
    /// The pool whose desired capacity drives the Deployment.
    pub pool: String,
    /// The worker Deployment, in the scaler's namespace.
    pub deployment: String,
    /// Slots each worker replica serves.
    #[schemars(range(min = 1))]
    pub slots_per_replica: u32,
    /// Fewest replicas.
    #[serde(default)]
    pub min_replicas: u32,
    /// Most replicas.
    #[schemars(range(min = 1))]
    pub max_replicas: u32,
}

/// What the operator last did.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScalerStatus {
    /// The pool's desired capacity in slots, as last read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_slots: Option<u32>,
    /// The replicas set on the Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    /// `Scaled`, or why the Deployment was left alone.
    #[serde(default)]
    pub message: String,
    /// The spec generation this status describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// Replicas for `desired` slots: `ceil(desired / slots_per_replica)`, clamped.
pub fn replicas(spec: &ScalerSpec, desired: u32) -> u32 {
    let per = spec.slots_per_replica.max(1);
    let max = spec.max_replicas.max(spec.min_replicas);
    desired.div_ceil(per).clamp(spec.min_replicas, max)
}

/// Problems with a spec that its schema cannot express.
pub fn check(spec: &ScalerSpec) -> Result<(), String> {
    if spec.slots_per_replica == 0 {
        return Err("slotsPerReplica must be at least 1".into());
    }
    if spec.max_replicas == 0 || spec.min_replicas > spec.max_replicas {
        return Err("maxReplicas must be at least 1 and at least minReplicas".into());
    }
    if spec.pool.is_empty() || spec.deployment.is_empty() || spec.server.is_empty() {
        return Err("server, pool and deployment are required".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(per: u32, min: u32, max: u32) -> ScalerSpec {
        ScalerSpec {
            server: "main".into(),
            pool: "default".into(),
            deployment: "workers".into(),
            slots_per_replica: per,
            min_replicas: min,
            max_replicas: max,
        }
    }

    #[test]
    fn replicas_round_up_and_clamp() {
        assert_eq!(replicas(&spec(4, 0, 10), 0), 0);
        assert_eq!(replicas(&spec(4, 0, 10), 1), 1);
        assert_eq!(replicas(&spec(4, 0, 10), 8), 2);
        assert_eq!(replicas(&spec(4, 0, 10), 9), 3);
        assert_eq!(replicas(&spec(4, 0, 10), 1000), 10);
        assert_eq!(replicas(&spec(4, 2, 10), 0), 2);
        assert_eq!(replicas(&spec(1, 0, 3), 3), 3);
    }

    #[test]
    fn specs_are_checked() {
        assert!(check(&spec(4, 0, 10)).is_ok());
        assert!(check(&spec(0, 0, 10)).is_err());
        assert!(check(&spec(4, 5, 3)).is_err());
        assert!(check(&spec(4, 0, 0)).is_err());
    }
}
