// swift-tools-version:5.9
import PackageDescription

// The cronoq scheduler embedded in a Swift program. EXPERIMENTAL until cronoq 1.0.
//
// The package wraps the cronoq C library. Build the static library and tell the linker
// where it is:
//
//   cargo build --release -p cronoq-ffi
//   swift build -Xlinker -L/path/to/dir/with/libcronoq_ffi.a
//
// Or use the binary framework of a release (CCronoq.xcframework.zip) in place of the
// CCronoq target; see "Embedding from other languages" in the book.
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
