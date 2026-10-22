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

A single replica with in-memory state:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: cronoq }
spec:
  replicas: 1
  selector: { matchLabels: { app: cronoq } }
  template:
    metadata: { labels: { app: cronoq } }
    spec:
      terminationGracePeriodSeconds: 30     # > shutdown_grace_s
      containers:
        - name: cronoq
          image: ghcr.io/synaptiai/cronoq:0.1
          ports:
            - { name: grpc, containerPort: 7070 }
            - { name: http, containerPort: 7071 }
            - { name: metrics, containerPort: 9090 }
          envFrom: [{ secretRef: { name: cronoq-tokens } }]
          volumeMounts: [{ name: config, mountPath: /etc/cronoq }]
          readinessProbe: { httpGet: { path: /readyz, port: http }, periodSeconds: 2 }
          livenessProbe:  { httpGet: { path: /healthz, port: http }, periodSeconds: 10 }
          resources: { requests: { cpu: 250m, memory: 256Mi } }
          securityContext:
            runAsNonRoot: true
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
      volumes: [{ name: config, configMap: { name: cronoq-config } }]
---
apiVersion: v1
kind: Service
metadata: { name: cronoq }
spec:
  selector: { app: cronoq }
  ports:
    - { name: grpc, port: 7070, targetPort: grpc }
    - { name: http, port: 7071, targetPort: http }
```

The configuration must bind to all interfaces inside the pod (`grpc_addr = "0.0.0.0:7070"`,
`http_addr = "0.0.0.0:7071"`), which requires credentials. If HTTPS is enabled
(`[server.tls]`), set `scheme: HTTPS` on the probes.

gRPC streams are long-lived. If an ingress or L7 load balancer sits in front, enable HTTP/2
(gRPC) and raise idle timeouts above your longest wait; the server sends HTTP/2 keepalive pings
every 20 s (`limits.keepalive_interval_s`).

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

Only the leader becomes ready, so the Service routes traffic to it alone. Standbys poll for the
leader lock every 0.5 s. The leader checks its lock session every second and steps down if the
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
- [ ] `/readyz` for readiness, `/healthz` for liveness; only the leader serves.
- [ ] Prometheus scraping `/metrics` with a metrics token or on `metrics_addr`; alerts on dead
      letters, wait times and missing workers.
- [ ] `max_pending` and `overflow` set on pools fed by untrusted producers.
- [ ] `sslmode=verify-full` in the database URL.
- [ ] Workers drain on shutdown, with a termination grace period to match.
- [ ] `cronoq-server --check` in CI for configuration changes.
