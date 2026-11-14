// Embedding cronoq from Swift: submit tasks, take them in priority order, and use ordering
// keys, replace by key, pause and release.
//
//   cargo build --release -p cronoq-ffi
//   swift run -Xlinker -L<dir with libcronoq_ffi.a> cronoq-example

import Cronoq
import Foundation

let scheduler = try Scheduler(Config(pools: [Pool(name: "default", retry: Retry(maxAttempts: 1))]))
print("cronoq \(Scheduler.libraryVersion)")

func submit(_ payload: String, _ options: TaskOptions? = nil) throws {
    try scheduler.submit(Data(payload.utf8), options)
}

/// Take the next task, check that it is `expected`, and hand back its lease.
func take(_ expected: String) throws -> Lease {
    let lease = try scheduler.next(timeout: 5)
    let payload = String(decoding: lease.payload, as: UTF8.self)
    guard payload == expected else {
        fatalError("expected task '\(expected)', got '\(payload)'")
    }
    print("  \(payload) attempt \(lease.attempt)")
    return lease
}

print("priority order:")
try submit("low", TaskOptions(priority: .low))
try submit("normal")
try submit("critical", TaskOptions(priority: .critical))
try submit("high", TaskOptions(priority: .high))
for expected in ["critical", "high", "normal", "low"] {
    try take(expected).complete()
}

print("an ordering key keeps its tasks in order:")
try submit("chat-1/a", TaskOptions(priority: .low, orderingKey: "chat-1"))
try submit("chat-1/b", TaskOptions(priority: .high, orderingKey: "chat-1"))
try submit("other")
for expected in ["chat-1/a", "chat-1/b", "other"] {
    try take(expected).complete()
}

print("replace by key:")
try submit("title v1", TaskOptions(key: "title"))
try submit("title v2", TaskOptions(key: "title", onKeyConflict: "replace_queued"))
try take("title v2").complete()

print("pause and resume:")
try submit("held")
try scheduler.pause(reason: "relay deploy")
do {
    _ = try scheduler.next(timeout: 0.05)
    fatalError("a paused pool handed out a task")
} catch CronoqError.paused(let reason) {
    print("  paused: \(reason)")
}
try scheduler.resume()
try take("held").complete()

print("release does not use up a retry:")
try submit("released")
try take("released").release()
try take("released").complete()

try scheduler.shutdown(grace: 1)
print("ok")
