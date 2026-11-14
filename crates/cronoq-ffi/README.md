# cronoq-ffi

A C API for embedding the [cronoq](https://github.com/synaptiai/cronoq) scheduler in a
program written in Go, Swift, C or any other language that can call C. **Experimental until
cronoq 1.0.**

- Header: [`include/cronoq.h`](include/cronoq.h), generated with cbindgen
  (`cbindgen --config cbindgen.toml --crate cronoq-ffi --output include/cronoq.h`).
- Libraries: `cargo build --release -p cronoq-ffi` builds a dynamic and a static library.
- Features: `sqlite` (a durable store in a SQLite file), `cron` and `tz`.
- Example: [`examples/c/example.c`](examples/c/example.c).
- Wrappers: Go in [`sdks/go-embed`](../../sdks/go-embed), Swift in
  [`sdks/swift`](../../sdks/swift).

Documentation:
[Embedding from other languages](https://synaptiai.github.io/cronoq/guides/embedding.html).
