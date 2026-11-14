#!/bin/bash
# Builds the embedded library for the Apple targets, with and without the SQLite store and
# time-zone data, and writes a size report (stripped dynamic library, in bytes and MiB) as Markdown.
#
#   .github/scripts/apple_sizes.sh > size-report.md
set -euo pipefail

targets=(aarch64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin x86_64-apple-darwin)

size_of() { # target, extra cargo arguments...
  local target=$1
  shift
  cargo build --quiet --release --locked -p cronoq-ffi --target "$target" "$@" >&2
  local lib="target/$target/release/libcronoq_ffi.dylib"
  local copy
  copy=$(mktemp)
  cp "$lib" "$copy"
  strip -x "$copy" >&2
  stat -f %z "$copy"
  rm -f "$copy"
}

mib() { awk -v b="$1" 'BEGIN { printf "%.2f", b / 1048576 }'; }

echo "# Size of the embedded library on Apple platforms"
echo
echo "Stripped dynamic library (\`libcronoq_ffi.dylib\`, \`strip -x\`), release build of"
echo "\`$(git rev-parse --short HEAD)\` with $(rustc --version)."
echo
echo "| Target | Default build | With the SQLite store (\`--features sqlite\`) | With time-zone data (\`--features tz\`) |"
echo "|---|---|---|---|"
for target in "${targets[@]}"; do
  small=$(size_of "$target")
  store=$(size_of "$target" --features sqlite)
  full=$(size_of "$target" --features tz)
  echo "| \`$target\` | $(mib "$small") MiB ($small bytes) | $(mib "$store") MiB ($store bytes) | $(mib "$full") MiB ($full bytes) |"
done
echo
echo "The default build has recurring schedules in UTC, no store and no time-zone database. Each"
echo "release has a second variant of the library and of the framework with the SQLite store."
echo "Time-zone data, for schedules in a time zone other than UTC, is in neither: build the"
echo "library with the feature tz for it. The size a program grows by when it links the static"
echo "library is smaller than these figures, because the linker drops what the program does not use."
