#!/bin/bash
# Runs a test binary built for the iOS simulator inside the booted simulator. Cargo calls it
# with the binary and its arguments (CARGO_TARGET_AARCH64_APPLE_IOS_SIM_RUNNER).
# Environment variables reach the binary only with the prefix SIMCTL_CHILD_.
set -euo pipefail
exec xcrun simctl spawn booted "$@"
