#!/usr/bin/env bash
# Install the wheel found in $1 into a fresh virtualenv and run the Python test suite from a
# copy outside the source tree, so the installed package (not the checkout) is what gets tested.
# CRONOQ_SERVER_BIN may point at a cronoq-server binary for the client tests; by default the
# debug build in ./target is used. CRONOQ_WHEEL_EXTRAS names the extras to install (default
# `client`; `none` for none, where grpcio has no wheel and the client tests are skipped).
set -euo pipefail

dist="$1"
repo="$(pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

python -m venv "$work/venv"
if [ -f "$work/venv/bin/activate" ]; then . "$work/venv/bin/activate"; else . "$work/venv/Scripts/activate"; fi
# Tooling versions come from the pinned CI requirements (as constraints).
wheel="$(ls "$dist"/*.whl | head -n 1)"
extras="${CRONOQ_WHEEL_EXTRAS:-client}"
if [ "$extras" = none ]; then spec="$wheel"; else spec="${wheel}[${extras}]"; fi
python -m pip install --quiet -c "$repo/.github/requirements/ci.txt" "$spec" pytest pytest-asyncio

exe=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) exe=".exe" ;; esac
export CRONOQ_SERVER_BIN="${CRONOQ_SERVER_BIN:-$repo/target/debug/cronoq-server$exe}"

cp -r "$repo/crates/cronoq-py/python/tests" "$work/tests"
cd "$work"
python -c "import cronoq, sys; print('testing', cronoq.__file__, sys.version)"
python -m pytest -q -p no:cacheprovider -o asyncio_mode=auto tests
