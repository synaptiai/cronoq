#!/usr/bin/env bash
# Static checks of the cronoq chart: lint, render several value sets, check every rendered
# cronoq.toml with `cronoq-server --check` (path in $CRONOQ_SERVER_BIN), and check that unsafe
# combinations are refused.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
chart="$here/../cronoq"
server=${CRONOQ_SERVER_BIN:-cronoq-server}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for role in CLIENT WORKER METRICS ADMIN; do export "CRONOQ_${role}_TOKEN=template-check-placeholder-$role"; done
export DATABASE_URL=postgres://check@localhost/check

helm lint "$chart" --set auth.existingSecret=t --set store.postgres.urlSecret.name=db

# Renders the chart with the given args and checks its configuration (for shard $SHARD).
valid() {
  helm template r "$chart" "$@" > "$work/out.yaml"
  python3 - "$work" <<'PY'
import sys, yaml
w = sys.argv[1]
for d in yaml.safe_load_all(open(f"{w}/out.yaml")):
    if d and d["kind"] == "ConfigMap":
        open(f"{w}/cronoq.toml", "w").write(d["data"]["cronoq.toml"])
PY
  "$server" --config "$work/cronoq.toml" ${SHARD:+--shard "$SHARD"} --check
}

# Renders the chart with the given args and expects it to fail with message $1.
refused() {
  local want=$1
  shift
  if out=$(helm template r "$chart" "$@" 2>&1); then
    echo "expected a refusal ($want), but the chart rendered"
    exit 1
  fi
  grep -q "$want" <<<"$out" || { echo "expected '$want', got: $out"; exit 1; }
  echo "refused: $want"
}

valid --set auth.existingSecret=t --set store.postgres.urlSecret.name=db
valid --set auth.insecureNoAuth=true --set store.kind=memory --set replicas=1 \
  --set 'grpcWeb.allowedOrigins={https://dash.example.com}'
SHARD=cpu valid --set auth.existingSecret=t -f "$here/sharded.yaml"
SHARD=gpu valid --set auth.existingSecret=t -f "$here/sharded.yaml" --set insecurePeerHttp=false \
  --set tls.secretName=tls --set tls.requireClientCerts=true

refused "auth.existingSecret is required" --set store.kind=memory --set replicas=1
refused "every replica would lead" --set auth.existingSecret=t --set store.kind=memory
refused "urlSecret.name is required" --set auth.existingSecret=t
refused "over plain http" --set auth.existingSecret=t -f "$here/sharded.yaml" --set insecurePeerHttp=false
refused "must contain {shard}" --set auth.existingSecret=t -f "$here/sharded.yaml" \
  --set store.kind=postgres --set store.postgres.urlSecret.name=db --set replicas=2
echo "template checks passed"
