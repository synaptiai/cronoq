#!/bin/bash
# Builds the embedded library for the Apple targets, with and without cron and time-zone
# data, and writes a size report (stripped dynamic library, in bytes and MiB) as Markdown.
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
echo "| Target | Default build | With the SQLite store (\`--features sqlite\`) | With cron and time-zone data (\`--features cron,tz\`) |"
echo "|---|---|---|---|"
for target in "${targets[@]}"; do
  small=$(size_of "$target")
  store=$(size_of "$target" --features sqlite)
  full=$(size_of "$target" --features cron,tz)
  echo "| \`$target\` | $(mib "$small") MiB ($small bytes) | $(mib "$store") MiB ($store bytes) | $(mib "$full") MiB ($full bytes) |"
done
echo
echo "The default build has no store, no cron schedules and no time-zone database. Each release"
echo "has a second variant of the library and of the framework with the SQLite store. Cron and"
echo "time zones cannot be used through the C API; they matter only to a store that holds"
echo "schedules written by another program. The size a program grows by when it links the static"
echo "library is smaller than these figures, because the linker drops what the program does not use."
