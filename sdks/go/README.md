# cronoq Go SDK

A Go client for the [cronoq](https://github.com/synaptiai/cronoq) server: submit, inspect, wait
for and watch tasks, and run workers with slots, heartbeats, reconnects and graceful draining.
Go 1.26 or newer.

```sh
go get github.com/synaptiai/cronoq/sdks/go
```

```go
import cronoq "github.com/synaptiai/cronoq/sdks/go"

c, err := cronoq.Dial("http://127.0.0.1:7070", cronoq.Options{Token: os.Getenv("CRONOQ_TOKEN")})
if err != nil {
	log.Fatal(err)
}
defer c.Close()
sub, err := c.SubmitAndWait(ctx, cronoq.Task{Payload: []byte("hello")}, 30*time.Second)
```

A worker:

```go
err := cronoq.RunWorker(ctx, cronoq.WorkerOptions{
	Target: "http://127.0.0.1:7070",
	Pool:   "default",
	Slots:  8,
	Token:  os.Getenv("CRONOQ_WORKER_TOKEN"),
}, func(ctx context.Context, l *cronoq.Lease) ([]byte, error) {
	return bytes.ToUpper(l.Payload), nil
})
```

`RunWorker` returns when `ctx` ends, after running handlers finish (up to `DrainTimeout`).
Return `cronoq.Permanent(err)` to fail a task without retrying, `cronoq.RetryAfter(err, d)` to
delay the retry, `cronoq.Reject(reason)` to hand a lease back, or `cronoq.ErrCancelTask` to
cancel the task. `(Lease.TaskID, Lease.Attempt)` is a fencing token for exactly-once side
effects.

The generated gRPC stubs are in the `cronoqpb` package.

Documentation: the [Go API reference](https://synaptiai.github.io/cronoq/reference/go.html).

## Development

The tests start a real `cronoq-server`: build it with `cargo build -p cronoq-server` from the
repository root (or point `CRONOQ_SERVER_BIN` at a binary), then run `go test ./...` here. Set
`CRONOQ_REQUIRE_SERVER=1` to fail instead of skipping when the binary is missing.

After changing `cronoq.proto`, regenerate the stubs from this directory with `buf generate`
(buf 1.72, `protoc-gen-go` 1.36.12 and `protoc-gen-go-grpc` 1.6.2 on `PATH`). CI checks that
they are current.

Licensed under either of Apache License 2.0 or MIT, at your option.
