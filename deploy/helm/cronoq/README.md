# cronoq Helm chart

Deploys the [cronoq](https://github.com/synaptiai/cronoq) server: standbys that take over from
the leader, a PostgreSQL or memory store, tokens from an existing Secret, optional TLS and
gRPC-Web, and shards.

```sh
helm install cronoq deploy/helm/cronoq \
  --set auth.existingSecret=cronoq-tokens --set store.postgres.urlSecret.name=cronoq-db
```

See the [deployment guide](https://synaptiai.github.io/cronoq/guides/deployment.html#kubernetes)
for the values, shards and readiness, and `values.yaml` for every option. `deploy/helm/test`
holds the checks CI runs: `template.sh` (rendering and refusals) and `e2e.sh` (failover, upgrade
and shards on a kind cluster).
