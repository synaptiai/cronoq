# Offline-first clients and embedded queues

## The problem

A desktop or mobile app has work to send: chat messages, edits of documents, uploads, read
receipts. The network comes and goes. The app needs a send queue with rules:

- A message the user just sent goes before a background upload.
- The messages of one conversation are sent in order, one at a time.
- Only the newest draft of a title is worth sending.
- While the device is offline nothing is sent, and nothing is lost or counted as failed.

An app cannot run a server next to itself. Each app therefore writes its own queue, with its
own retry and ordering code, and each one has its own bugs.

## The setup

cronoq can run inside the app as a library
([Embedding from other languages](../guides/embedding.md)). There is no server, no port and no
second process. A C library, `cronoq_ffi`, contains the same engine as the server. Two
wrappers are maintained with it: a Go package and a Swift package.

> **Experimental until cronoq 1.0.** The C API will change before 1.0.

The app does two things:

1. It **submits** tasks. A task is a payload of bytes that only the app understands, plus
   options such as priority, key and ordering key.
2. It **runs** them. A thread of the app asks for the next task with a blocking call, does the
   work, and reports the result: *complete*, *fail* or *release*.

What the app holds between "next" and the result is a **lease**: one attempt of one task. The
library never calls back into the app.

In Swift:

```swift
import Cronoq
import Foundation

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
    } catch {
        print("error: \(error)")
        break
    }
}
```

In Go:

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

Complete programs that run in CI are in
[`sdks/swift/Sources/cronoq-example`](https://github.com/synaptiai/cronoq/tree/main/sdks/swift/Sources/cronoq-example),
[`sdks/go-embed/example`](https://github.com/synaptiai/cronoq/tree/main/sdks/go-embed/example)
and
[`crates/cronoq-ffi/examples/c/example.c`](https://github.com/synaptiai/cronoq/blob/main/crates/cronoq-ffi/examples/c/example.c).

## The rules of a send queue

Each rule from the problem is one option or one call. The examples are Go; Swift has the same
options.

| Rule | Feature | In Go |
|---|---|---|
| User actions before background work | [Priorities](../concepts/priorities.md) | `&embedded.Task{Priority: embedded.High}` |
| One conversation in order | [Ordering keys](../concepts/ordering-keys.md) | `&embedded.Task{OrderingKey: "chat-1"}` |
| Only the newest draft | [Replace by key](../concepts/replacing.md) | `&embedded.Task{Key: "title", OnKeyConflict: "replace_queued"}` |
| The user opens a conversation whose upload waits | [Update](../concepts/updating.md), or a [floor](../concepts/ordering-keys.md#a-floor-for-a-key) on the key | `s.Update(ref, update)`, `s.SetOrderingFloor("", "chat-1", embedded.High, false)` |
| Offline: start nothing | [Pause](../concepts/pausing.md) | `s.Pause(embedded.PauseTarget{}, "offline")`, later `s.Resume(embedded.PauseTarget{})` |
| The network failed during a send | [Release](../concepts/reliability.md#releasing-an-attempt) | `lease.Release(30 * time.Second)` |
| The send was refused | Fail, retryable or not | `lease.Fail(message, true, 0)` |

**Offline.** When the app learns that the network is gone, it pauses the pool. Tasks are still
accepted and stay queued in place. A thread that asks for a task waits out its timeout and
then gets "paused" with the reason, so a loop around the call does not spin. When the network
is back, the app resumes the pool.

**A send that fails for lack of network** is not the task's fault. Release the lease: the task
keeps its place and no retry is used, up to `max_releases` (10 by default).

## Sleep and suspension

The scheduler's clock does not count the time a machine sleeps (on Apple platforms and on
Linux). After waking, every timer is late by the time slept: a retry that was 30 seconds away
is still 30 seconds away.

Call **resync clock** when the app becomes active again. The scheduler's time then moves
forward by the time it missed, and timers that became due during the sleep fire at once. A
wall clock that was changed has no effect.

```swift
NotificationCenter.default.addObserver(
    forName: UIApplication.didBecomeActiveNotification, object: nil, queue: nil
) { _ in
    _ = try? scheduler.resyncClock()
}
```

In Go the call is `ResyncClock`, in C `cronoq_resync_clock`. Calling it more often than needed
is harmless. The scheduler's time never moves back.

## Keeping the queue across restarts

By default tasks are kept in memory. When the app ends, the queue is gone. That suits an app
whose own database is the source of truth and that fills the queue again at start.

A library built with the `sqlite` feature can keep tasks, pauses and ordering-key settings in
a SQLite file (`Store: &embedded.Store{SQLite: path}` in the Go `Config`). After a restart the
queue is as it was, and an attempt that was running is tried again. One program at a time may
have the file open.

Each release after 0.2.2 has the library and the binary framework in a second variant with
the store (`cronoq-embed-sqlite-…`, `CCronoq-sqlite.xcframework.zip`). To build it yourself:
`cargo build --release -p cronoq-ffi --features sqlite`.

## What you get

- **The same scheduling rules as the server.** Priorities with aging, retries with backoff,
  resources, ordering keys, replace, update, pause and release behave as the concept pages
  describe.
- **No infrastructure.** One library in the app. In Go's default mode no cgo is needed.
- **The newest work goes first.** A task is chosen when a thread asks for it, not before. A
  task submitted later with a higher priority is still handed out first, as long as nobody
  has taken the earlier one. There is one exception: when a waiting thread gives up, or the
  scheduler ends a lease the app holds, one task can already be started. It goes to the next
  thread that asks ([details](../guides/embedding.md#what-to-know)).
- **No callbacks.** The app's threads call the library, and the library never calls into the
  app. The scheduler runs on one thread that the library owns. The SQLite store, if used,
  adds threads of its own: one for each database connection it opens.

## Trade-offs

- **Experimental.** The C API has a version number (`cronoq_abi_version()`), and it will
  change before 1.0. The wrappers check the version for you.
- **How you get it today.** Release 0.2.2 does not contain the library. Releases after 0.2.2
  carry an archive per platform with the dynamic library, the static library and the header,
  and a binary framework for Swift. Until then, build it yourself
  (`cargo build --release -p cronoq-ffi`). Each comes in two variants, without a store and
  with the SQLite store. The Go package loads the dynamic library at run time, or links the static
  one with the build tag `cronoq_static`. The Swift package links the static library
  ([details](../guides/embedding.md#getting-the-library)).
- **It does not run in the background.** While the app is suspended, no task is handed out and
  no timer fires. The library does not ask the operating system for background time.
- **A lease held across a sleep can time out or expire.** At the resynchronisation the attempt
  times out if the task has a timeout shorter than the sleep, and the lease expires if the
  pool has a lease TTL shorter than the sleep. Release leases before the app goes to the
  background if you can.
- **Not in the C API:** recurring schedules, fixed start times, dependencies between tasks,
  fair sharing between tenants and worker labels.
- **Results and events are there when you need them.** An attempt can be completed with an
  output, another part of the app can wait for a task to end, and a subscription delivers
  status events, for example to show progress
  ([details](../guides/embedding.md#results-and-events)). A send queue often needs none of
  them: the code that sends already knows the result.
- **A blocking call.** "Next" blocks its thread. In Swift, call it from a thread of your own,
  not from the main thread and not from Swift's cooperative thread pool. In Go, each goroutine
  inside `Next` holds an operating system thread while it waits.
- **A paused class or ordering key is not reported.** Only a pause of the whole pool is
  reported by "next". For the others the call finds nothing; see the snapshot.
- **Platforms.** Linux, macOS and Windows libraries are in each release after 0.2.2. iOS is
  built and tested in the simulator. Android is not built or tested.
