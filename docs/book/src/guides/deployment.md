# Deploying the server

## Container image

The repository's `Dockerfile` builds a small Debian-based image running as an unprivileged
user. Release builds publish multi-architecture images to `ghcr.io/synaptiai/cronoq`.

```sh
docker build -t cronoq .
docker run -p 7070:7070 -p 7071:7071 \
  -v $PWD/cronoq.toml:/etc/cronoq/cronoq.toml:ro \
  -e CRONOQ_CLIENT_TOKEN -e CRONOQ_WORKER_TOKEN -e CRONOQ_ADMIN_TOKEN \
  cronoq
```

The configuration path comes from `--config` or `CRONOQ_CONFIG` (the image sets
`/etc/cronoq/cronoq.toml`). Log verbosity follows `RUST_LOG` (default `info`).

## Kubernetes

A single-replica deployment with in-memory state:

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
      containers:
        - name: cronoq
          image: ghcr.io/synaptiai/cronoq:0.1
          ports:
            - { name: grpc, containerPort: 7070 }
            - { name: http, containerPort: 7071 }
          envFrom: [{ secretRef: { name: cronoq-tokens } }]
          volumeMounts: [{ name: config, mountPath: /etc/cronoq }]
          readinessProbe: { httpGet: { path: /readyz, port: http }, periodSeconds: 2 }
          livenessProbe:  { httpGet: { path: /healthz, port: http }, periodSeconds: 10 }
          resources: { requests: { cpu: 250m, memory: 256Mi } }
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

gRPC streams are long-lived; if you put an ingress or L7 load balancer in front, enable HTTP/2
(gRPC) and raise idle timeouts above your longest task.

### High availability with Postgres

Add a store section and run two or more replicas:

```toml
[store]
kind = "postgres"
url_env = "DATABASE_URL"
keep_finished_s = 604800   # prune finished tasks after 7 days
```

```yaml
spec:
  replicas: 2
```

Only the leader becomes ready, so the Service routes traffic only to it. When the leader's
database session ends (process exit, pod deletion, or a connection terminated by Postgres), a
standby acquires the lock within its polling interval (0.5 s), recovers all accepted work and
becomes ready; clients and workers reconnect. A leader that is merely partitioned keeps its
session until TCP keepalives or Postgres time it out; tune `tcp_keepalives_*` (or
`idle_session_timeout`) on the server if you need faster takeover in that case. See
[Durability and HA](../concepts/durability.md).

The database user needs permission to create tables on first start (the schema is applied
automatically and idempotently).

### Autoscaling workers with KEDA

Run workers as their own Deployment (one worker process per pod, or several slots per pod) and
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

Alternatively, set `scaler = { kind = "webhook", url = "..." }` on the pool and have your own
service create workers when it receives `{"pool": ..., "desired": n}`.

## Sizing

The scheduler itself is light: one core comfortably handles hundreds of thousands of task
transitions per second. Memory is dominated by pending tasks (a few hundred bytes plus the
payload each) and by the output cache (`[outputs]`, 64 MiB by default). Database load with
Postgres is roughly one small transaction per engine step, with all mutations of that step
batched together.

## Checklist

- [ ] Tokens for each role, delivered through secrets (`token_env`), not inline in the config.
- [ ] TLS on the gRPC listener (or a service mesh), and mTLS for workers if required.
- [ ] `/readyz` used for readiness; only the leader serves.
- [ ] Prometheus scraping `/metrics`; alerts on dead letters and wait times.
- [ ] `max_pending` set on every pool that receives traffic from untrusted producers.
- [ ] Workers configured with reconnect and a graceful drain on shutdown.
