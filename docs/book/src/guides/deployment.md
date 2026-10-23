# Deploying the server

## Container image

Releases publish a multi-architecture image (`linux/amd64`, `linux/arm64`) to
`ghcr.io/synaptiai/cronoq`, tagged with the version (`0.1.0`) and the minor series (`0.1`).
The image is distroless: it contains the `cronoq-server` binary and the example configuration,
runs as a non-root user, and has no shell or curl. Build it yourself with:

```sh
docker build -t cronoq .
docker run -p 7070:7070 -p 7071:7071 \
  -v $PWD/cronoq.toml:/etc/cronoq/cronoq.toml:ro \
  -e CRONOQ_CLIENT_TOKEN -e CRONOQ_WORKER_TOKEN -e CRONOQ_METRICS_TOKEN -e CRONOQ_ADMIN_TOKEN \
  cronoq
```

The configuration path comes from `--config` or `CRONOQ_CONFIG` (the image sets
`/etc/cronoq/cronoq.toml`). Every `token_env` the configuration names must be set, or the
server refuses to start. Log verbosity follows `RUST_LOG` (default `info`). The server stops
gracefully on `SIGTERM` or `SIGINT`: it stops admitting work, gives running attempts
`shutdown_grace_s` (default 10 s) to finish, then tells workers to reconnect elsewhere.

Validate configuration changes before rolling them out:

```sh
docker run --rm -v $PWD/cronoq.toml:/c.toml:ro cronoq --config /c.toml --check
```

## Kubernetes

The Helm chart in [`deploy/helm/cronoq`](https://github.com/synaptiai/cronoq/tree/main/deploy/helm/cronoq)
deploys the server with standbys, a store, tokens, probes and, if you use them, shards. Charts are
not published to a repository yet; install from a checkout.

Create the tokens (one key per role) and a Secret with the database URL
(`postgres://USER:PASSWORD@HOST:5432/DB?sslmode=verify-full`, key `url`), then install:

```sh
kubectl create secret generic cronoq-tokens \
  --from-literal=client="$(openssl rand -hex 24)" --from-literal=worker="$(openssl rand -hex 24)" \
  --from-literal=metrics="$(openssl rand -hex 24)" --from-literal=admin="$(openssl rand -hex 24)"
kubectl create secret generic cronoq-db --from-file=url=./database-url
helm install cronoq deploy/helm/cronoq \
  --set auth.existingSecret=cronoq-tokens --set store.postgres.urlSecret.name=cronoq-db
```

Pools, resources and limits go in `config`, as the TOML that follows the chart's own tables:

```yaml
config: |
  [[pools]]
  name = "default"
  max = 50
  lease_ttl_s = 30
  [[pools]]
  name = "llm"
  max = 200
  lease_ttl_s = 120
```

| Value | Default | Description |
|---|---|---|
| `replicas` | 2 | Server pods (per shard). One leads; the others stand by and take over. |
| `auth.existingSecret` | required | Secret with keys `client`, `worker`, `metrics` and `admin`, each at least 16 characters. `auth.insecureNoAuth: true` runs without tokens, on trusted networks only. |
| `store.kind` | `postgres` | `postgres`, or `memory` with `replicas: 1` (each memory-store pod would lead on its own, so the chart refuses more). |
| `store.postgres.urlSecret` | required | `{name, key}` of the connection URL. `schema` (default `cronoq`) holds the tables. |
| `config` | one pool | TOML for `[[pools]]`, `[[resources]]`, `[limits]` and the rest. Start it with a table header. |
| `shards` | none | `[{name, number, pools}]`; see below. |
| `tls.secretName` | none | A `kubernetes.io/tls` Secret (`tls.crt`, `tls.key`, `ca.crt`) for both listeners; `tls.requireClientCerts` turns on mTLS. |
| `grpcWeb.allowedOrigins` | none | Browser origins for [gRPC-Web](../reference/grpc.md#grpc-web). |
| `podDisruptionBudget` | `maxUnavailable: 1` | Per server group. |
| `serviceMonitor.enabled` | `false` | A Prometheus Operator ServiceMonitor scraping `/metrics` with the metrics token. |

Every pod is ready as soon as it is up (readiness uses `/healthz`), so the Service routes to the
leader and the standbys, and rolling updates proceed. A standby answers `UNAVAILABLE` (503 over
HTTP) and closes each connection about a second after accepting it, so a client that retries
reconnects and the Service picks a pod again until it reaches the leader; workers reconnect on
their own. `/readyz` still answers 200 only on the leader, for your own checks. The pods run as
non-root with a read-only root filesystem and no service-account token.

gRPC streams are long-lived. If an ingress or L7 load balancer sits in front, enable HTTP/2
(gRPC) and raise idle timeouts above your longest wait; the server sends HTTP/2 keepalive pings
every 20 s (`limits.keepalive_interval_s`).

### Shards in the chart

```yaml
shards:
  - { name: cpu, number: 1, pools: [default] }
  - { name: gpu, number: 2, pools: [llm] }
store:
  postgres:
    schema: "cronoq_{shard}"   # each shard gets its own tables
```

Each shard gets its own Deployment, Service (`<release>-cronoq-<shard>`, the shard's endpoint)
and disruption budget, from one shared configuration. The main Service reaches every shard's
servers, and any server answers for every shard ([Sharding](../concepts/sharding.md)).

Servers forward callers' tokens to other shards, and workers follow redirects to them, so shard
endpoints must be encrypted or explicitly trusted:

- with `tls.secretName`, the endpoints use `https://` and the servers verify each other with
  `ca.crt`; the certificate must name every shard's Service;
- without TLS, set `insecurePeerHttp: true` (trusted cluster networks only); workers then need
  their own insecure-redirect option (`insecure_redirect_http`, `InsecureRedirectHTTP`,
  `insecureRedirectHttp`). The chart refuses shards with neither.

### Without the chart

Write the configuration yourself (bind to `0.0.0.0` inside the pod, which requires credentials)
and run the image with probes on the HTTP port: `/healthz` for liveness, and for readiness either
`/healthz` (all pods ready, as in the chart) or `/readyz` (only the leader; rolling updates of a
Deployment with standbys then stall, so use `strategy: Recreate`). With `[server.tls]`, set
`scheme: HTTPS` on the probes.

### Durable state on one host with SQLite

For a single machine (a VM, a bare-metal host, a container with a persistent volume), keep state
in a file:

```toml
[store]
kind = "sqlite"
path = "/var/lib/cronoq/state.db"   # a local filesystem, not NFS
keep_finished_s = 604800            # prune finished tasks after 7 days
```

A second server process pointed at the same file stands by and takes over when the first stops
(at once on a clean shutdown, after the 10 s lease otherwise). Back the file up with SQLite's
online backup (`sqlite3 state.db ".backup backup.db"`), not by copying it while it is in use.

### High availability with Postgres

Add a store and run two or more replicas:

```toml
[store]
kind = "postgres"
url_env = "DATABASE_URL"   # postgres://...?sslmode=verify-full
keep_finished_s = 604800   # prune finished tasks after 7 days
```

```yaml
spec:
  replicas: 2
```

Standbys poll for the leader lock every 0.5 s. The leader checks its lock session every second and steps down if the
session stays unresponsive for 10 s; when its session ends (process exit, pod deletion, a
connection terminated by Postgres, or that timeout), a standby takes the lock, recovers all
accepted work and becomes ready. Clients retry on `UNAVAILABLE` / 503; workers receive
`Shutdown{not_leader}` and reconnect. See [Durability and HA](../concepts/durability.md).

The database role needs permission to create tables in the `public` schema on first start.
Migrations are versioned and applied automatically, under a lock, by whichever replica starts
first; replicas of an older version refuse to run against a newer schema.

### Autoscaling workers with KEDA

Run workers as their own Deployment (one worker process per pod, with several slots each) and
scale it from the server's desired-capacity metric:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: cronoq-default-workers }
spec:
  scaleTargetRef: { name: default-workers }
  minReplicaCount: 0
  maxReplicaCount: 100
  pollingInterval: 5
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring:9090
        # desired slots ÷ slots per pod
        query: ceil(max(cronoq_pool_desired_workers{pool="default"}) / 4)
        threshold: "1"
```

`cronoq_pool_desired_workers` already accounts for queue pressure, wait-time objectives and
upcoming fixed-start reservations, and is smoothed against flapping. Alternatively, set
`scaler = { kind = "webhook", url = "...", token_env = "..." }` on the pool and have your own
service create workers when it receives `{"pool": ..., "desired": n}`.

Give worker pods a `terminationGracePeriodSeconds` longer than their drain timeout, so running
leases finish before the pod is killed.

## Sizing

The scheduler itself is light: one core comfortably handles hundreds of thousands of task
transitions per second. Memory is dominated by pending tasks (a few hundred bytes plus the
payload each), the output cache (`[outputs]`, 64 MiB by default) and the task-info cache. With
Postgres, the leader writes one small transaction per engine step, with all mutations of that
step batched together; replies wait for that commit.

## Checklist

- [ ] One token per role and consumer, delivered through secrets (`token_env`), scoped to pools.
- [ ] TLS on both listeners (or a service mesh), and mTLS for workers if required.
- [ ] `/healthz` for liveness; for readiness `/healthz` with rolling updates (standbys answer
      `UNAVAILABLE`, which clients retry) or `/readyz` (only the leader) with `strategy: Recreate`.
- [ ] Prometheus scraping `/metrics` with a metrics token or on `metrics_addr`; alerts on dead
      letters, wait times and missing workers.
- [ ] `max_pending` and `overflow` set on pools fed by untrusted producers.
- [ ] `sslmode=verify-full` in the database URL.
- [ ] Workers drain on shutdown, with a termination grace period to match.
- [ ] `cronoq-server --check` in CI for configuration changes.
