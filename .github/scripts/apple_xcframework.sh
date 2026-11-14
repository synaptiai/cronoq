#!/bin/bash
# Builds the binary framework of the embedded library for the Swift package, and checks that
# the package's tests and example pass with it.
#
#   .github/scripts/apple_xcframework.sh <archive name> [cargo build arguments...]
#
# Writes <archive name>.xcframework.zip and its checksum. The framework inside is always
# named CCronoq.xcframework, because that is the module the Swift package imports.
set -euo pipefail

name=$1
shift
targets=(aarch64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin x86_64-apple-darwin)
build="build/$name"

for target in "${targets[@]}"; do
  cargo build --release --locked -p cronoq-ffi --target "$target" "$@"
done

rm -rf "$build"
mkdir -p "$build/headers" "$build/macos"
cp crates/cronoq-ffi/include/cronoq.h "$build/headers/"
printf 'module CCronoq {\n    header "cronoq.h"\n    export *\n}\n' > "$build/headers/module.modulemap"
lipo -create \
  target/aarch64-apple-darwin/release/libcronoq_ffi.a \
  target/x86_64-apple-darwin/release/libcronoq_ffi.a \
  -output "$build/macos/libcronoq_ffi.a"
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libcronoq_ffi.a -headers "$build/headers" \
  -library target/aarch64-apple-ios-sim/release/libcronoq_ffi.a -headers "$build/headers" \
  -library "$build/macos/libcronoq_ffi.a" -headers "$build/headers" \
  -output "$build/CCronoq.xcframework"
(cd "$build" && zip -qry "../../$name.xcframework.zip" CCronoq.xcframework)
# The checksum SwiftPM wants for a binary target is the SHA-256 of the archive.
shasum -a 256 "$name.xcframework.zip" | cut -d' ' -f1 > "$name.xcframework.zip.checksum"
test "$(wc -c < "$name.xcframework.zip.checksum")" -ge 64
echo "$name.xcframework.zip $(cat "$name.xcframework.zip.checksum")"

# The package as a user would set it up: the C target replaced by the framework.
cp -R sdks/swift "$build/package"
rm -rf "$build/package/Sources/CCronoq" "$build/package/.build"
cp -R "$build/CCronoq.xcframework" "$build/package/"
python3 - "$build/package/Package.swift" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p).read()
s, n = re.subn(
    r'\.target\(\s*name: "CCronoq",\s*linkerSettings: \[\.linkedLibrary\("cronoq_ffi"\)\]\s*\)',
    '.binaryTarget(name: "CCronoq", path: "CCronoq.xcframework")',
    s,
)
assert n == 1, "Package.swift has changed: update apple_xcframework.sh"
open(p, "w").write(s)
PY
(cd "$build/package" && swift test && swift run cronoq-example)
