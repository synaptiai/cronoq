// Embedding cronoq from Go: submit tasks, take them in priority order, and use ordering
// keys, replace by key, pause and release.
//
// Run it from the module directory, sdks/go-embed (the repository root has no go.mod). On
// macOS the library is libcronoq_ffi.dylib.
//
//	cargo build --release -p cronoq-ffi
//	cd sdks/go-embed
//	CRONOQ_LIBRARY="$PWD/../../target/release/libcronoq_ffi.so" go run ./example
//
// or, linking the static library with cgo. The directory must hold libcronoq_ffi.a and no
// dynamic library of the same name, because the linker prefers the dynamic one:
//
//	CGO_LDFLAGS=-L<dir with only libcronoq_ffi.a> go run -tags cronoq_static ./example
//
// The program checks what it gets and exits with 1 if anything is not as expected.
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	embedded "github.com/synaptiai/cronoq/sdks/go-embed"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	version, err := embedded.Version()
	must(err)
	fmt.Println("cronoq", version)

	s, err := embedded.Open(embedded.Config{
		Pools: []embedded.Pool{{Name: "default", Retry: &embedded.Retry{MaxAttempts: 1}}},
	})
	must(err)
	defer s.Close()

	submit := func(payload string, opts *embedded.Task) {
		_, err := s.Submit([]byte(payload), opts)
		must(err)
	}
	// take takes the next task and checks that it is the expected one.
	take := func(expected string) *embedded.Lease {
		l, err := s.Next("", 5*time.Second)
		must(err)
		if string(l.Payload) != expected {
			log.Fatalf("expected task %q, got %q", expected, l.Payload)
		}
		fmt.Printf("  %-10s attempt %d\n", l.Payload, l.Attempt)
		return l
	}

	fmt.Println("priority order:")
	submit("low", &embedded.Task{Priority: embedded.Low})
	submit("normal", nil)
	submit("critical", &embedded.Task{Priority: embedded.Critical})
	submit("high", &embedded.Task{Priority: embedded.High})
	for _, want := range []string{"critical", "high", "normal", "low"} {
		must(take(want).Complete())
	}

	fmt.Println("an ordering key keeps its tasks in order:")
	submit("chat-1/a", &embedded.Task{OrderingKey: "chat-1", Priority: embedded.Low})
	submit("chat-1/b", &embedded.Task{OrderingKey: "chat-1", Priority: embedded.High})
	submit("other", nil)
	for _, want := range []string{"chat-1/a", "chat-1/b", "other"} {
		must(take(want).Complete())
	}

	fmt.Println("replace by key:")
	submit("title v1", &embedded.Task{Key: "title"})
	submit("title v2", &embedded.Task{Key: "title", OnKeyConflict: "replace_queued"})
	must(take("title v2").Complete())

	fmt.Println("pause and resume:")
	submit("held", nil)
	_, err = s.Pause(embedded.PauseTarget{}, "relay deploy")
	must(err)
	var paused *embedded.PausedError
	if _, err := s.Next("", 50*time.Millisecond); !errors.As(err, &paused) {
		log.Fatalf("a paused pool answered: %v", err)
	}
	fmt.Println("  paused:", paused.Reason)
	_, err = s.Resume(embedded.PauseTarget{})
	must(err)
	must(take("held").Complete())

	fmt.Println("release does not use up a retry:")
	submit("released", nil)
	must(take("released").Release(0))
	must(take("released").Complete())

	must(s.Shutdown(time.Second))
	fmt.Println("ok")
}
