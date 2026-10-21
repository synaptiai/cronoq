#!/usr/bin/env bash
# Regenerate the Python protobuf/gRPC stubs for cronoq.client.
# Requires: pip install grpcio-tools   (set PYTHON to choose the interpreter)
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
proto_root="$here/../cronoq-proto/proto"
out="$here/python/cronoq/_proto"
python="${PYTHON:-python3}"
mkdir -p "$out"
"$python" -m grpc_tools.protoc -I "$proto_root" \
  --python_out="$out" --pyi_out="$out" --grpc_python_out="$out" \
  "$proto_root/cronoq/v1/cronoq.proto"
# The generated code imports `cronoq.v1.cronoq_pb2`, which would collide with this package;
# flatten the files and use a relative import instead.
mv "$out/cronoq/v1/"cronoq_pb2* "$out/"
rm -rf "$out/cronoq"
sed -i 's/^from cronoq\.v1 import cronoq_pb2 as /from . import cronoq_pb2 as /' "$out/cronoq_pb2_grpc.py"
cat > "$out/__init__.py" <<'PY'
"""Generated protobuf and gRPC stubs for the cronoq server API (cronoq.v1).

Regenerate with ``scripts/gen_proto.sh``; do not edit by hand.
"""
PY
echo "generated stubs in $out"
