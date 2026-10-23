#!/usr/bin/env bash
# End-to-end run of the cronoq chart on a disposable cluster (kind in CI). Expects the images
# cronoq-server:e2e and cronoq-e2e:e2e to be loaded into the cluster.
#
# 1. A release with two replicas and PostgreSQL: a task through the Service, served by a
#    worker that reaches the leader through the same Service.
# 2. The leader pod is deleted: the standby takes over and serves the next task.
# 3. helm upgrade --wait with a changed pod template completes (standbys are ready).
# 4. A sharded release (memory stores, plain http allowed): tasks for both shards through the
#    main Service, with workers redirected to their shard.
# 5. The operator (image cronoq-operator:e2e) scales a worker Deployment up while a pool has a
#    backlog, and back to zero once the backlog is gone.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
chart="$here/../cronoq"
ns=cronoq-e2e

kubectl create namespace "$ns"
kubectl config set-context --current --namespace "$ns"

# Random tokens and database credentials, kept in Secrets only.
random() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
kubectl create secret generic e2e-tokens \
  --from-literal=client="$(random)" --from-literal=worker="$(random)" \
  --from-literal=metrics="$(random)" --from-literal=admin="$(random)"
db=$(random)
kubectl create secret generic e2e-db --from-literal=password="$db" \
  --from-literal=url="postgres://cronoq:$db@postgres:5432/cronoq"
kubectl apply -f "$here/postgres.yaml"
kubectl rollout status deploy/postgres --timeout=180s

dump() {
  kubectl get pods -o wide || true
  for p in $(kubectl get pods -l app.kubernetes.io/name=cronoq -o name); do
    echo "--- $p"
    kubectl logs "$p" --tail=50 || true
  done
}

# Runs the checker as a Job against server $1 for pools $2; extra checker flags follow.
check() {
  local name="check-$RANDOM" server=$1 pools=$2 extra=""
  shift 2
  for a in "$@"; do extra="$extra, \"$a\""; done
  kubectl apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 240
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: e2e
          image: cronoq-e2e:e2e
          imagePullPolicy: Never
          args: ["-server", "$server", "-pools", "$pools"$extra]
          env:
            - name: CRONOQ_CLIENT_TOKEN
              valueFrom: {secretKeyRef: {name: e2e-tokens, key: client}}
            - name: CRONOQ_WORKER_TOKEN
              valueFrom: {secretKeyRef: {name: e2e-tokens, key: worker}}
YAML
  if ! kubectl wait --for=condition=complete "job/$name" --timeout=240s; then
    kubectl logs "job/$name" || true
    dump
    return 1
  fi
  kubectl logs "job/$name"
}

# The pod whose /readyz answers 200 (the leader) among the pods matching selector $1.
leader() {
  for p in $(kubectl get pods -l "$1" --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'); do
    if kubectl get --raw "/api/v1/namespaces/$ns/pods/$p:7071/proxy/readyz" >/dev/null 2>&1; then
      echo "$p"
      return
    fi
  done
}

# Waits up to 60 s for a leader among the pods matching selector $1 other than pod $2.
new_leader() {
  for _ in $(seq 60); do
    l=$(leader "$1")
    if [ -n "$l" ] && [ "$l" != "$2" ]; then
      echo "$l"
      return
    fi
    sleep 1
  done
  dump >&2
  return 1
}

common=(--set image.repository=cronoq-server --set image.tag=e2e --set image.pullPolicy=Never
  --set auth.existingSecret=e2e-tokens)

echo "== 1. two replicas with PostgreSQL"
helm install ha "$chart" "${common[@]}" --set store.postgres.urlSecret.name=e2e-db --wait --timeout 180s
check http://ha-cronoq:7070 default

echo "== 2. the leader goes away"
sel=app.kubernetes.io/instance=ha
first=$(new_leader "$sel" "")
echo "leader: $first"
kubectl delete pod "$first" --wait=false
second=$(new_leader "$sel" "$first")
echo "new leader: $second"
check http://ha-cronoq:7070 default

echo "== 3. rolling upgrade"
helm upgrade ha "$chart" "${common[@]}" --set store.postgres.urlSecret.name=e2e-db \
  --set podAnnotations.e2e=upgraded --wait --timeout 180s
check http://ha-cronoq:7070 default

echo "== 4. two shards"
helm install sh "$chart" "${common[@]}" -f "$here/sharded.yaml" --wait --timeout 180s
check http://sh-cronoq:7070 default,llm -insecure-redirect-http
echo "== 5. the operator scales a worker Deployment"
helm install op "$chart" "${common[@]}" -f "$here/operator-server.yaml" --wait --timeout 180s
helm install scaler "$here/../cronoq-operator" --set image.repository=cronoq-operator \
  --set image.tag=e2e --set image.pullPolicy=Never --set intervalSeconds=2 \
  --set 'servers[0].name=main' --set 'servers[0].url=http://op-cronoq:7070' \
  --set 'servers[0].tokenSecret.name=e2e-tokens' --set 'servers[0].tokenSecret.key=metrics' \
  --wait --timeout 180s
kubectl apply -f "$here/workers.yaml"

# Waits up to $3 seconds until the workers Deployment's replicas compare with $2 by test
# operator $1 (for example: replicas -ge 1 90).
replicas() {
  for _ in $(seq "$3"); do
    n=$(kubectl get deploy workers -o jsonpath='{.spec.replicas}')
    if test "${n:-0}" "$1" "$2"; then
      echo "workers: $n replicas"
      return
    fi
    sleep 1
  done
  kubectl get cronoqworkerscalers -o wide || true
  kubectl logs deploy/scaler-cronoq-operator --tail=50 || true
  return 1
}
check http://op-cronoq:7070 batch -backlog 8
replicas -ge 1 90
kubectl get cronoqworkerscalers -o wide
check http://op-cronoq:7070 batch -clear 8
# Scale-down waits out the server's 60 s stabilization window.
replicas -eq 0 180
kubectl get cronoqworkerscalers -o wide
echo "all checks passed"
