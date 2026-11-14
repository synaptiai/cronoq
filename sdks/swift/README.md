# Cronoq for Swift (embedded)

The cronoq scheduler inside a Swift program, without a server. **Experimental until cronoq
1.0.**

See [Embedding from other languages](https://synaptiai.github.io/cronoq/guides/embedding.html).

```sh
cargo build --release -p cronoq-ffi          # in the cronoq repository
swift test -Xlinker -L<dir with libcronoq_ffi.a>
```
