// swift-tools-version:5.9
import PackageDescription

// The cronoq scheduler embedded in a Swift program. EXPERIMENTAL until cronoq 1.0.
//
// The package wraps the cronoq C library. Until a binary framework is published with a
// release, build the static library yourself and tell the linker where it is:
//
//   cargo build --release -p cronoq-ffi
//   swift build -Xlinker -L/path/to/dir/with/libcronoq_ffi.a
let package = Package(
    name: "Cronoq",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [
        .library(name: "Cronoq", targets: ["Cronoq"]),
    ],
    targets: [
        .target(
            name: "CCronoq",
            linkerSettings: [.linkedLibrary("cronoq_ffi")]
        ),
        .target(name: "Cronoq", dependencies: ["CCronoq"]),
        .executableTarget(name: "cronoq-example", dependencies: ["Cronoq"]),
        .testTarget(name: "CronoqTests", dependencies: ["Cronoq"]),
    ]
)
