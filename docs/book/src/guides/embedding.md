# Embedding from other languages

cronoq can run inside a program written in Go, Swift, C, or any other language that can call
C: no server, no port, no second process. This is for programs that need a scheduler of their
own and cannot run a server next to them, such as a desktop or phone app with a local queue
of uploads and messages to send.

> **Experimental until cronoq 1.0.** The C API has a version number
> (`cronoq_abi_version()`), and it will change before 1.0.

There is one C library, `cronoq_ffi`, with a header, `cronoq.h`. On top of it are two
wrappers that are maintained with it and behave alike:

| Language | Package | How it reaches the library |
|---|---|---|
| C and others | `cronoq.h` | Links the static or the dynamic library. |
| Go | `github.com/synaptiai/cronoq/sdks/go-embed` | By default loads the dynamic library at run time, **without cgo**. With the build tag `cronoq_static`, links the static library with cgo. |
| Swift | `sdks/swift` (package `Cronoq`) | Links the static library. |

Rust programs use the [`cronoq` crate](rust-quickstart.md) and Python programs the
[`cronoq` package](python-quickstart.md) directly; they do not need this.

## How it works

The scheduler runs inside the library, on one thread that the library owns. Your program does
two things:

1. It **submits** tasks. A task is a payload of bytes that only your program understands,
   plus options: priority, key, ordering key, deadline, and so on.
2. It **runs** them. Your threads ask for work with a blocking call, `next lease`, do the
   work, and report the result: *complete*, *fail*, or *release*.

Nothing in the library ever calls back into your program.

A **lease** is one attempt of one task. It ends when you report its result, or when the
scheduler ends it: the attempt ran longer than its timeout, or the task was cancelled.

**The task is chosen when a thread asks for it.** A pool hands out as many attempts as there
are threads asking, so a task that is submitted later with a higher priority is still handed
out first, as long as nobody has taken the earlier one.

## Getting the library

Each release has an archive per platform, `cronoq-embed-<version>-<target>`, with the dynamic
library, the static library and the header:

| Platform | Target | Dynamic library |
|---|---|---|
| Linux x86-64, ARM64 | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` | `libcronoq_ffi.so` |
| macOS x86-64, ARM64 | `x86_64-apple-darwin`, `aarch64-apple-darwin` | `libcronoq_ffi.dylib` |
| Windows x86-64 | `x86_64-pc-windows-gnu` | `cronoq_ffi.dll` |

The Windows library is built with the GNU toolchain, because that is what Go's cgo links
with. There is no prebuilt library for Windows on ARM64 yet.

To build it yourself you need a Rust toolchain:

```sh
cargo build --release -p cronoq-ffi
# target/release/libcronoq_ffi.{so,dylib,a}   and   crates/cronoq-ffi/include/cronoq.h
```

| Build feature | Default | Adds |
|---|---|---|
| `sqlite` | off | A durable store in a SQLite file ([below](#keeping-tasks-across-restarts)). |
| `cron`, `tz` | off | Nothing you can call from C. They matter only when a store holds [recurring schedules](../concepts/recurring.md) written by another program. Time-zone data is most of the size of a full build. |

## C

```c
#include "cronoq.h"

cronoq_scheduler *s;
if (cronoq_new(NULL, &s) != CRONOQ_OK) {          /* NULL: one pool, "default" */
  fprintf(stderr, "%s\n", cronoq_last_error());
  return 1;
}

const char *payload = "send event 17";
cronoq_submit(s, (const uint8_t *)payload, strlen(payload),
              "{\"priority\": \"high\", \"ordering_key\": \"chat-1\"}", NULL, NULL);

cronoq_lease lease;
switch (cronoq_next_lease(s, NULL, 5000, &lease)) {    /* waits up to 5 s */
case CRONOQ_OK:
  if (send(lease.payload, lease.payload_len))
    cronoq_complete(s, lease.lease);
  else
    cronoq_fail(s, lease.lease, "relay refused", /*retryable*/ 1, /*retry_after_ms*/ -1);
  cronoq_lease_free(&lease);
  break;
case CRONOQ_TIMEOUT:  break;                           /* nothing to do: ask again */
case CRONOQ_PAUSED:   printf("paused: %s\n", cronoq_last_error()); break;
case CRONOQ_SHUTDOWN: return 0;
default:              fprintf(stderr, "%s\n", cronoq_last_error());
}

cronoq_shutdown(s, 1000);
cronoq_free(s);
```

```sh
cc -I include app.c libcronoq_ffi.a -lpthread -ldl -lm -o app                      # Linux
cc -I include app.c libcronoq_ffi.a -o app                                         # macOS
gcc -I include app.c libcronoq_ffi.a -lws2_32 -luserenv -lntdll -lbcrypt -ladvapi32 -lkernel32 -o app.exe   # Windows (MinGW)
```

A complete program is in
[`crates/cronoq-ffi/examples/c/example.c`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-ffi/examples/c/example.c).

## Go

```go
import embedded "github.com/synaptiai/cronoq/sdks/go-embed"

s, err := embedded.Open(embedded.Config{})
if err != nil { log.Fatal(err) }
defer s.Close()

s.Submit([]byte("send event 17"), &embedded.Task{
    Priority: embedded.High, OrderingKey: "chat-1",
})

for {
    lease, err := s.Next("", 5*time.Second)
    switch {
    case errors.Is(err, embedded.ErrTimeout):
        continue
    case errors.Is(err, embedded.ErrPaused):
        continue // err says why; Next has already waited
    case errors.Is(err, embedded.ErrShutdown):
        return
    case err != nil:
        log.Fatal(err)
    }
    if err := send(lease.Payload); err != nil {
        lease.Fail(err.Error(), true, 0)
    } else {
        lease.Complete()
    }
}
```

The two build modes have the same API and the same behaviour:

| | Default | Build tag `cronoq_static` |
|---|---|---|
| cgo | Not needed (`CGO_ENABLED=0` works). | Needed, with a C toolchain (MinGW on Windows). |
| What ships with your program | Your binary and the dynamic library. | Your binary only. |
| Where the library is found | At run time: the path in `CRONOQ_LIBRARY`, else next to the executable, else by the system's search rules. `embedded.Load(path)` loads it from a path you choose. | At build time: `CGO_LDFLAGS="-L<dir>"`, where `<dir>` holds `libcronoq_ffi.a` and **not** the dynamic library, or the linker prefers that. |
| Cost per call | About three times that of cgo; both are far below a millisecond. | |

Each goroutine inside `Next` holds an operating system thread while it waits.

A complete program is in
[`sdks/go-embed/example`](https://github.com/synaptiai/cronoq/tree/main/sdks/go-embed/example).

## Swift

```swift
import Cronoq

let scheduler = try Scheduler()
try scheduler.submit(Data("send event 17".utf8), TaskOptions(priority: .high, orderingKey: "chat-1"))

while true {
    do {
        let lease = try scheduler.next(timeout: 5)
        do {
            try send(lease.payload)
            try lease.complete()
        } catch {
            try lease.fail("\(error)")
        }
    } catch CronoqError.timeout {
        continue
    } catch CronoqError.paused(let reason) {
        print("paused: \(reason)")
    } catch CronoqError.shutdown {
        break
    }
}
```

`next` blocks its thread: call it from a thread of your own, not from the main thread and not
from Swift's cooperative thread pool.

The package links the static library. Until a binary framework is published with a release,
build the library and tell the linker where it is:

```sh
cargo build --release -p cronoq-ffi
swift build -Xlinker -L<dir with libcronoq_ffi.a>
```

## Configuration

`cronoq_new` takes a JSON object; the wrappers build it from a `Config` value. Every field is
optional, and an unknown field is an error.

```json
{
  "pools": [{
    "name": "default",
    "max_leases": 256,
    "max_pending": 10000,
    "overflow": "reject",
    "retry": {"max_attempts": 3, "initial_backoff_ms": 1000, "multiplier": 2.0,
              "max_backoff_ms": 60000, "jitter": true},
    "max_releases": 10,
    "lease_ttl_ms": 30000,
    "priority": {"aging_rate": {"low": 0.5}},
    "urgency": {"window_s": 60}
  }],
  "resources": [{"kind": "semaphore", "name": "uplink", "permits": 1},
                {"kind": "rate", "name": "relay", "rate": 5.0, "burst": 10}],
  "priority": {"base": {"critical": 1000}, "allow_overtaking_high": false},
  "urgency": {"max": 100, "window_s": 300},
  "store": {"sqlite": "/path/to/queue.db"}
}
```

| Field | Meaning |
|---|---|
| `pools` | The pools. Without it there is one pool, `default`. |
| `pools[].max_leases` | Most attempts your program may hold at a time (default 256). |
| `pools[].max_pending`, `overflow` | A [bound](../concepts/backpressure.md) on the queue and what a full queue does: `reject` (default), `shed_lowest` or `shed_oldest`. A submission never blocks. |
| `pools[].retry` | The [retry policy](../concepts/reliability.md) of the pool's tasks. |
| `pools[].max_releases` | How often an attempt of one task may be [released](../concepts/reliability.md#releasing-an-attempt) without counting as a failure. |
| `pools[].lease_ttl_ms` | Leases expire unless they are extended with a heartbeat within this time. Without it a lease lasts until its result is reported or its task's timeout. |
| `priority`, `urgency` | The [priority](../concepts/priorities.md) and [urgency](../concepts/deadlines.md) settings, for all pools or for one. Classes are named `critical`, `high`, `normal`, `low`. |
| `resources` | [Semaphores and rate limits](../concepts/resources.md) that tasks name in their `resources`. |
| `store.sqlite` | Keep tasks in this SQLite file. Needs a library built with `sqlite`. |

## Task options

`cronoq_submit` takes the payload and a JSON object of options; the wrappers build it from a
value (`Task` in Go, `TaskOptions` in Swift). Every field is optional, and an unknown field is
an error.

| Option | Meaning |
|---|---|
| `pool` | The pool (default `default`). |
| `kind` | A name for the kind of work, for your own use and for statistics. |
| `key` | A [key](../concepts/dependencies.md#keys): a second submission with the key of a live task returns that task. |
| `priority` | `critical`, `high`, `normal` (default) or `low`. See [Priorities and aging](../concepts/priorities.md). |
| `deadline_ms` | A [deadline](../concepts/deadlines.md) that far from now. |
| `timeout_ms` | An attempt that runs longer is ended and counts as a failure. |
| `retry` | A retry policy for this task. |
| `resources` | Amounts of [resources](../concepts/resources.md) the task needs, by name. |
| `tags` | Text you get back with the lease. |
| `ordering_key`, `on_failure` | [Ordering keys](../concepts/ordering-keys.md): tasks with one key run one at a time, in submission order. `on_failure` is `continue` (default) or `halt`. |
| `on_key_conflict` | `return_existing` (default), `reject`, or `replace_queued` to [replace the queued task](../concepts/replacing.md) with this key. |
| `fingerprint`, with `on_key_conflict: reject` | 1 to 32 hexadecimal digits that stand for the content. A submission whose key belongs to a task with another fingerprint is a [conflict](../concepts/key-conflicts.md). |
| `renew_position` | A replacement goes to the back of the queue instead of keeping the place. |
| `max_releases` | Overrides the pool's limit for this task. |

## Operations

| Operation | C | Go | Swift |
|---|---|---|---|
| Start, stop | `cronoq_new`, `cronoq_shutdown`, `cronoq_free` | `Open`, `Shutdown`, `Close` | `Scheduler()`, `shutdown`, release of the last reference |
| Submit | `cronoq_submit` | `Submit` | `submit` |
| Take an attempt | `cronoq_next_lease`, `cronoq_lease_free` | `Next` | `next` |
| Report | `cronoq_complete`, `cronoq_fail`, `cronoq_release` | `Lease.Complete`, `Fail`, `Release` | `Lease.complete`, `fail`, `release` |
| Extend a lease | `cronoq_heartbeat` | `Lease.Heartbeat` | `Lease.heartbeat` |
| Cancel | `cronoq_cancel` | `Cancel` | `cancel` |
| [Update a queued task](../concepts/updating.md) | `cronoq_update` | `Update` | `update` |
| [Floor of an ordering key](../concepts/ordering-keys.md#a-floor-for-a-key) | `cronoq_set_ordering_floor` | `SetOrderingFloor` | `setOrderingFloor` |
| [Pause, resume](../concepts/pausing.md) | `cronoq_pause`, `cronoq_resume` | `Pause`, `Resume` | `pause`, `resume` |
| Look | `cronoq_task`, `cronoq_snapshot` | `Task`, `Snapshot` | `task`, `snapshot` |
| Version | `cronoq_abi_version`, `cronoq_version` | `Version` | `Scheduler.libraryVersion` |

Three results end an attempt:

| | Use it when | What happens |
|---|---|---|
| Complete | The work is done. | The task succeeds. |
| Fail | The work failed. | Retryable: tried again after a backoff until the retry policy is used up. Not retryable: the task fails for good. |
| Release | You cannot run the task now, and nothing is wrong with it (no network, the app is going to the background). | The task keeps its place and is handed out again after the delay. No retry is used, up to `max_releases`. |

## What to know

- **Results are applied a moment later.** `complete`, `fail` and `release` hand the result to
  the scheduler's thread and return. A snapshot taken at once may not show it yet.
- **A cancelled task's lease is over when cancel returns.** The next heartbeat or report for
  it answers "lease gone". A long attempt learns of a cancel by sending heartbeats.
- **A paused pool.** `next lease` waits out its timeout and then reports "paused" with the
  reason, so a loop around it does not spin. A paused class or ordering key is not reported
  this way: the call just finds nothing of it. See the snapshot for those.
- **No output.** A task has a payload but no result value: your program already has the
  result when it reports the attempt.
- **Not in the C API:** recurring schedules, fixed start times, dependencies between tasks,
  fair sharing between tenants, worker labels, status events. Ask for what you need in an
  issue.
- **A task may be started a moment before a thread asks for it.** When a thread stops
  waiting, or the scheduler ends a lease you hold (timeout, cancel), the scheduler can have
  started one more task already. That task counts as running, its timeout runs, and it goes
  to the next thread that asks. Keep asking, or shut down, rather than leaving a pool alone
  with work in it.
- **Stopping.** `shutdown` wakes the threads that wait and hands out nothing more. For the
  grace period you give it, leases you hold can still be reported; it returns as soon as
  all are. A lease that is still held after that is released: its task keeps its place and
  no retry is used.

## Keeping tasks across restarts

By default tasks are kept in memory: when the program ends, the queue is gone. That suits a
program whose own database is the source of truth and that fills the queue again at start.

With a library built with the `sqlite` feature, `"store": {"sqlite": "<path>"}` keeps tasks,
pauses and ordering-key settings in that file. After a restart the queue is as it was; an
attempt that was running is tried again. One program at a time may have the file open.

## Rules of the boundary

These are also at the top of `cronoq.h`.

| | Rule |
|---|---|
| **Errors** | Every function that can fail returns a `cronoq_status`. Nothing panics or aborts across the boundary: a fault inside the library is `CRONOQ_INTERNAL`. `cronoq_last_error()` returns the message of the last failed call **on the calling thread**, valid until that thread's next call. |
| **Text** | UTF-8, zero-terminated. The library copies what it is given before the call returns. Your buffers stay yours. |
| **Payloads** | Opaque bytes. Copied at submit. |
| **What the library returns** | Stays the library's until you hand it back: a scheduler to `cronoq_free`, a lease to `cronoq_lease_free`, a buffer to `cronoq_buf_free`. Never to `free()`. A lease's pointers stay valid until `cronoq_lease_free`, also after the lease was completed. |
| **Threads** | A scheduler may be used from any number of threads at once. The exception is `cronoq_free`: every other thread must be out of the library first. Call `cronoq_shutdown`, which wakes the threads that wait in `cronoq_next_lease`, then join them, then free. |
| **The library's thread** | One, started by `cronoq_new` and stopped by `cronoq_shutdown`. It never calls into your program. |
| **Versions** | `cronoq_abi_version()` must equal the `CRONOQ_ABI_VERSION` of the header you built with. The number changes when an existing function, structure or constant changes; additions do not change it. The wrappers check it for you. |

The Go and Swift wrappers take care of ownership and of the per-thread error message. In Go,
`Close` waits for calls in progress before it frees the scheduler.
