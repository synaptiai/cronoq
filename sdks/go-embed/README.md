# cronoq embedded in Go

The cronoq scheduler inside a Go program, without a server. **Experimental until cronoq
1.0.** For the client of a cronoq server, see [`sdks/go`](../go).

```go
import embedded "github.com/synaptiai/cronoq/sdks/go-embed"
```

The package wraps the cronoq C library in two build modes with the same API:

- **Default, no cgo:** loads the dynamic library at run time, from the path in
  `CRONOQ_LIBRARY`, else next to the executable, else by the system's search rules.
- **`-tags cronoq_static`, cgo:** links the static library; set
  `CGO_LDFLAGS="-L<dir with libcronoq_ffi.a>"`.

The libraries are attached to each cronoq release (`cronoq-embed-<version>-<target>`), or
build them with `cargo build --release -p cronoq-ffi`.

Documentation:
[Embedding from other languages](https://synaptiai.github.io/cronoq/guides/embedding.html).
Example: [`example/main.go`](example/main.go).
