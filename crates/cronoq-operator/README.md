# cronoq-operator

A Kubernetes operator that scales [cronoq](https://github.com/synaptiai/cronoq) worker
Deployments to their pool's desired capacity. For each `CronoqWorkerScaler` it reads the pool's
desired slots from a configured server (`Admin.Snapshot`, metrics role) and sets the
Deployment's replicas to `ceil(desired / slotsPerReplica)`, clamped to the scaler's bounds.

Install it with the chart in `deploy/helm/cronoq-operator`; see the
[deployment guide](https://synaptiai.github.io/cronoq/guides/deployment.html#scaling-workers-with-the-operator).
`cronoq-operator --print-crd` prints the CustomResourceDefinition, which the chart ships in
`crds/` (CI checks that they match).

Licensed under either of Apache License 2.0 or MIT, at your option.
